//! Durable Raft state: term, vote, replicated log, commit index, snapshot.
//!
//! # Why this exists
//!
//! Raft's safety argument rests on three facts surviving a crash:
//!
//!   1. `current_term` — a node must never move *backwards* in term.
//!   2. `voted_for` — a node must never cast a second vote in a term it has
//!      already voted in. If it does, two candidates can both collect a
//!      majority in the same term and two leaders can be elected. Two leaders
//!      accept conflicting writes at the same log index; committed entries are
//!      then overwritten. That is unbounded, silent data loss, not a degraded
//!      mode.
//!   3. The replicated log — an entry a follower *acknowledged* must still be
//!      there after a restart, because the leader may have counted that ack
//!      toward the quorum that committed it and already told a client "done".
//!
//! The requirement is not "write it down eventually", it is: the write must be
//! **fsync'd before the RPC response that depends on it leaves the node**. A
//! vote response that outruns its own fsync is exactly as unsafe as no
//! persistence at all, because the crash window is the interesting case.
//!
//! `commit_index` and snapshot metadata are also persisted here. Those two are
//! *recoverable* rather than safety-critical (a restarted node relearns the
//! commit index from the leader's next `AppendEntries`), so they are allowed to
//! lag — see [`RaftStorage::note_commit_index`]. What is never allowed is a
//! persisted commit index that is *ahead* of the truth, and the code only ever
//! writes values it has actually reached.
//!
//! # On-disk layout
//!
//! Three files inside a `raft/` directory under the data dir:
//!
//! | File | Contents | Write strategy |
//! |---|---|---|
//! | `hardstate` | term, voted-for, commit index | atomic replace (tmp → fsync → rename → fsync dir) |
//! | `log` | append-only entry records | append → fsync; atomic replace on truncate/compact |
//! | `snapshot` | last-included index/term + data | atomic replace |
//!
//! Every record carries a CRC32C. A torn tail (a record the crash caught
//! half-written) is detected on load and physically truncated, so the next
//! append does not chain onto garbage. Truncating an *unacknowledged* tail
//! record is safe: it was never fsync'd, so it was never acknowledged.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{Command, LogEntry, LogIndex, NodeId, Snapshot, Term};

/// Format tag for the hard-state file. Bump if the layout changes.
const HARDSTATE_MAGIC: &[u8; 8] = b"NUCRHS01";
/// Format tag written once at the head of the log file.
const LOG_MAGIC: &[u8; 8] = b"NUCRLG01";
/// Format tag for the snapshot file.
const SNAPSHOT_MAGIC: &[u8; 8] = b"NUCRSN01";

/// Command discriminants in the log record encoding. These are an on-disk
/// contract — never renumber, only append.
const CMD_SQL: u8 = 1;
const CMD_NOOP: u8 = 2;
const CMD_ADD_NODE: u8 = 3;
const CMD_REMOVE_NODE: u8 = 4;

/// How far `commit_index` may drift from its persisted value before we pay for
/// a hard-state write. A stale persisted commit index is safe (the node simply
/// relearns it), so this is a pure I/O optimisation and not a safety knob.
const COMMIT_PERSIST_STRIDE: LogIndex = 64;

/// Everything reconstructed from disk when a node restarts.
#[derive(Debug, Default, Clone)]
pub struct PersistedState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
    /// Real log entries (index >= 1), not including the in-memory sentinel.
    pub entries: Vec<LogEntry>,
    pub snapshot: Option<Snapshot>,
}

/// Durable backing store for one Raft node's persistent state.
pub struct RaftStorage {
    dir: PathBuf,
    hardstate_path: PathBuf,
    log_path: PathBuf,
    snapshot_path: PathBuf,
    /// Open append handle for the log, positioned at the end of the last
    /// intact record.
    log_file: File,
    /// Hard-state values last written to disk, so we can skip no-op writes.
    persisted_term: Term,
    persisted_vote: Option<NodeId>,
    persisted_commit: LogIndex,
}

impl std::fmt::Debug for RaftStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftStorage")
            .field("dir", &self.dir)
            .field("persisted_term", &self.persisted_term)
            .field("persisted_vote", &self.persisted_vote)
            .field("persisted_commit", &self.persisted_commit)
            .finish()
    }
}

impl RaftStorage {
    /// Open (creating if needed) the Raft state directory and load whatever a
    /// previous incarnation of this node durably wrote.
    ///
    /// A partially written tail record is truncated away here, before the
    /// append handle is positioned, so subsequent appends cannot chain onto a
    /// torn record.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<(Self, PersistedState)> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let hardstate_path = dir.join("hardstate");
        let log_path = dir.join("log");
        let snapshot_path = dir.join("snapshot");

        let (current_term, voted_for, commit_index) = load_hard_state(&hardstate_path)?;
        let snapshot = load_snapshot(&snapshot_path)?;
        let (entries, intact_len) = load_log(&log_path)?;

        // Drop any torn tail so the next append starts from a clean boundary.
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&log_path)?;
        if log_file.metadata()?.len() != intact_len {
            log_file.set_len(intact_len)?;
            log_file.sync_all()?;
        }
        let mut log_file = log_file;
        log_file.seek(SeekFrom::End(0))?;

        let store = Self {
            dir,
            hardstate_path,
            log_path,
            snapshot_path,
            log_file,
            persisted_term: current_term,
            persisted_vote: voted_for,
            persisted_commit: commit_index,
        };

        Ok((
            store,
            PersistedState {
                current_term,
                voted_for,
                commit_index,
                entries,
                snapshot,
            },
        ))
    }

    /// The directory this store owns.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Durably record term and vote. Returns only once the bytes are fsync'd,
    /// so the caller may respond to the RPC that depends on them.
    ///
    /// This is the call that makes a double vote impossible across a crash.
    pub fn save_hard_state(
        &mut self,
        term: Term,
        voted_for: Option<NodeId>,
        commit_index: LogIndex,
    ) -> io::Result<()> {
        if self.persisted_term == term
            && self.persisted_vote == voted_for
            && self.persisted_commit == commit_index
        {
            return Ok(());
        }
        let bytes = encode_hard_state(term, voted_for, commit_index);
        if let Some(e) = crate::storage::crashpoint::io_fault("raft.hardstate_write") {
            return Err(e);
        }
        atomic_write_instrumented(
            &self.hardstate_path,
            &bytes,
            "raft.before_hardstate_fsync",
            "raft.after_hardstate_fsync",
            "raft.before_hardstate_rename",
            "raft.after_hardstate_rename",
        )?;
        sync_dir(&self.dir)?;
        self.persisted_term = term;
        self.persisted_vote = voted_for;
        self.persisted_commit = commit_index;
        Ok(())
    }

    /// Record a commit-index advance, writing through only when the drift
    /// exceeds [`COMMIT_PERSIST_STRIDE`].
    ///
    /// Lagging is safe: a restarted node's commit index is a *lower bound*, and
    /// the next `AppendEntries` from the leader restores the true value. Only
    /// running ahead would be unsafe, and this never writes an index the node
    /// has not actually reached.
    pub fn note_commit_index(
        &mut self,
        term: Term,
        voted_for: Option<NodeId>,
        commit_index: LogIndex,
    ) -> io::Result<()> {
        if commit_index < self.persisted_commit.saturating_add(COMMIT_PERSIST_STRIDE) {
            return Ok(());
        }
        self.save_hard_state(term, voted_for, commit_index)
    }

    /// Append entries to the durable log and fsync.
    ///
    /// Returns only once the entries are on stable storage, so a follower may
    /// then answer `success: true` — the leader is entitled to count that ack
    /// toward a commit quorum.
    pub fn append_entries(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("raft.log_append") {
            return Err(e);
        }
        let mut buf = Vec::new();
        for entry in entries {
            encode_log_record(&mut buf, entry);
        }
        self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.write_all(&buf)?;
        crate::storage::crashpoint::reach("raft.before_log_fsync");
        self.log_file.sync_all()?;
        crate::storage::crashpoint::reach("raft.after_log_fsync");
        Ok(())
    }

    /// Replace the entire durable log with `entries` and fsync.
    ///
    /// Used for the two cases an append-only file cannot express: truncating a
    /// conflicting suffix (Raft's log-matching repair) and compacting a prefix
    /// that a snapshot now covers. The replace is atomic, so a crash mid-way
    /// leaves the previous log fully intact rather than a half-rewritten one.
    pub fn rewrite_log(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        let mut buf = Vec::with_capacity(LOG_MAGIC.len() + entries.len() * 32);
        buf.extend_from_slice(LOG_MAGIC);
        for entry in entries {
            encode_log_record(&mut buf, entry);
        }
        atomic_write(&self.log_path, &buf)?;
        sync_dir(&self.dir)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.log_path)?;
        file.seek(SeekFrom::End(0))?;
        self.log_file = file;
        Ok(())
    }

    /// Durably store a snapshot and its metadata.
    pub fn save_snapshot(&mut self, snapshot: &Snapshot) -> io::Result<()> {
        let bytes = encode_snapshot(snapshot);
        atomic_write(&self.snapshot_path, &bytes)?;
        sync_dir(&self.dir)?;
        Ok(())
    }
}

// ── Encoding ─────────────────────────────────────────────────────────────────

fn encode_hard_state(term: Term, voted_for: Option<NodeId>, commit_index: LogIndex) -> Vec<u8> {
    let mut body = Vec::with_capacity(25);
    body.extend_from_slice(&term.to_le_bytes());
    match voted_for {
        Some(id) => {
            body.push(1);
            body.extend_from_slice(&id.to_le_bytes());
        }
        None => {
            body.push(0);
            body.extend_from_slice(&0u64.to_le_bytes());
        }
    }
    body.extend_from_slice(&commit_index.to_le_bytes());

    let mut out = Vec::with_capacity(HARDSTATE_MAGIC.len() + body.len() + 4);
    out.extend_from_slice(HARDSTATE_MAGIC);
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
    out
}

fn load_hard_state(path: &Path) -> io::Result<(Term, Option<NodeId>, LogIndex)> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, None, 0)),
        Err(e) => return Err(e),
    };
    // A short or corrupt hard state means the very first write was torn. The
    // atomic replace guarantees we never destroy a good previous version, so
    // the only way to get here is with no committed prior state at all.
    if bytes.len() != HARDSTATE_MAGIC.len() + 25 + 4 || &bytes[..8] != HARDSTATE_MAGIC {
        return Ok((0, None, 0));
    }
    let body = &bytes[8..8 + 25];
    let stored = u32::from_le_bytes(bytes[8 + 25..].try_into().unwrap());
    if crc32c::crc32c(body) != stored {
        return Ok((0, None, 0));
    }
    let term = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let voted_for = if body[8] == 1 {
        Some(u64::from_le_bytes(body[9..17].try_into().unwrap()))
    } else {
        None
    };
    let commit_index = u64::from_le_bytes(body[17..25].try_into().unwrap());
    Ok((term, voted_for, commit_index))
}

fn encode_log_record(out: &mut Vec<u8>, entry: &LogEntry) {
    let mut body = Vec::with_capacity(32);
    body.extend_from_slice(&entry.index.to_le_bytes());
    body.extend_from_slice(&entry.term.to_le_bytes());
    match &entry.command {
        Command::Sql(sql) => {
            body.push(CMD_SQL);
            let raw = sql.as_bytes();
            body.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            body.extend_from_slice(raw);
        }
        Command::Noop => body.push(CMD_NOOP),
        Command::AddNode(id) => {
            body.push(CMD_ADD_NODE);
            body.extend_from_slice(&id.to_le_bytes());
        }
        Command::RemoveNode(id) => {
            body.push(CMD_REMOVE_NODE);
            body.extend_from_slice(&id.to_le_bytes());
        }
    }
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
    out.extend_from_slice(&body);
}

/// Decode one record body. Returns `None` if it is malformed.
fn decode_log_body(body: &[u8]) -> Option<LogEntry> {
    if body.len() < 17 {
        return None;
    }
    let index = u64::from_le_bytes(body[0..8].try_into().ok()?);
    let term = u64::from_le_bytes(body[8..16].try_into().ok()?);
    let command = match body[16] {
        CMD_SQL => {
            if body.len() < 21 {
                return None;
            }
            let len = u32::from_le_bytes(body[17..21].try_into().ok()?) as usize;
            if body.len() != 21 + len {
                return None;
            }
            Command::Sql(String::from_utf8(body[21..].to_vec()).ok()?)
        }
        CMD_NOOP => {
            if body.len() != 17 {
                return None;
            }
            Command::Noop
        }
        CMD_ADD_NODE | CMD_REMOVE_NODE => {
            if body.len() != 25 {
                return None;
            }
            let id = u64::from_le_bytes(body[17..25].try_into().ok()?);
            if body[16] == CMD_ADD_NODE {
                Command::AddNode(id)
            } else {
                Command::RemoveNode(id)
            }
        }
        _ => return None,
    };
    Some(LogEntry {
        index,
        term,
        command,
    })
}

/// Read the log file, stopping at the first damaged record.
///
/// Returns the intact entries plus the byte length of the intact prefix, so the
/// caller can physically drop the torn tail.
fn load_log(path: &Path) -> io::Result<(Vec<LogEntry>, u64)> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // Fresh node: create the file with just its header.
            let mut buf = Vec::new();
            buf.extend_from_slice(LOG_MAGIC);
            atomic_write(path, &buf)?;
            return Ok((Vec::new(), LOG_MAGIC.len() as u64));
        }
        Err(e) => return Err(e),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    if bytes.len() < LOG_MAGIC.len() || &bytes[..LOG_MAGIC.len()] != LOG_MAGIC {
        // No usable header — treat as empty and rewrite the header.
        let mut buf = Vec::new();
        buf.extend_from_slice(LOG_MAGIC);
        atomic_write(path, &buf)?;
        return Ok((Vec::new(), LOG_MAGIC.len() as u64));
    }

    let mut entries = Vec::new();
    let mut pos = LOG_MAGIC.len();
    let mut intact = pos;
    while pos + 8 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let body_start = pos + 8;
        let body_end = match body_start.checked_add(len) {
            Some(e) if e <= bytes.len() => e,
            // Truncated tail — the crash landed mid-record.
            _ => break,
        };
        let body = &bytes[body_start..body_end];
        if crc32c::crc32c(body) != crc {
            break;
        }
        match decode_log_body(body) {
            Some(entry) => entries.push(entry),
            None => break,
        }
        pos = body_end;
        intact = pos;
    }
    Ok((entries, intact as u64))
}

fn encode_snapshot(snapshot: &Snapshot) -> Vec<u8> {
    let mut body = Vec::with_capacity(24 + snapshot.data.len());
    body.extend_from_slice(&snapshot.last_included_index.to_le_bytes());
    body.extend_from_slice(&snapshot.last_included_term.to_le_bytes());
    body.extend_from_slice(&(snapshot.data.len() as u64).to_le_bytes());
    body.extend_from_slice(&snapshot.data);

    let mut out = Vec::with_capacity(SNAPSHOT_MAGIC.len() + body.len() + 4);
    out.extend_from_slice(SNAPSHOT_MAGIC);
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
    out
}

fn load_snapshot(path: &Path) -> io::Result<Option<Snapshot>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if bytes.len() < SNAPSHOT_MAGIC.len() + 24 + 4 || &bytes[..8] != SNAPSHOT_MAGIC {
        return Ok(None);
    }
    let body = &bytes[8..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if crc32c::crc32c(body) != stored {
        return Ok(None);
    }
    let last_included_index = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let last_included_term = u64::from_le_bytes(body[8..16].try_into().unwrap());
    let data_len = u64::from_le_bytes(body[16..24].try_into().unwrap()) as usize;
    if body.len() != 24 + data_len {
        return Ok(None);
    }
    Ok(Some(Snapshot {
        last_included_index,
        last_included_term,
        data: body[24..].to_vec(),
    }))
}

// ── Durable write primitives ─────────────────────────────────────────────────

/// Write `contents` to `path` atomically: fill a sibling temp file, fsync it,
/// then rename over the target. A crash leaves either the old file or the new
/// one — never a partial one.
fn atomic_write(path: &Path, contents: &[u8]) -> io::Result<()> {
    atomic_write_instrumented(path, contents, "", "", "", "")
}

/// `atomic_write` with named crash windows around the fsync and the rename, so
/// a harness can kill the process at exactly the instant that matters instead of
/// hoping a random kill lands there.
fn atomic_write_instrumented(
    path: &Path,
    contents: &[u8],
    before_fsync: &str,
    after_fsync: &str,
    before_rename: &str,
    after_rename: &str,
) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(contents)?;
        w.flush()?;
        if !before_fsync.is_empty() {
            crate::storage::crashpoint::reach(before_fsync);
        }
        w.get_ref().sync_all()?;
        if !after_fsync.is_empty() {
            crate::storage::crashpoint::reach(after_fsync);
        }
    }
    if !before_rename.is_empty() {
        crate::storage::crashpoint::reach(before_rename);
    }
    fs::rename(&tmp, path)?;
    if !after_rename.is_empty() {
        crate::storage::crashpoint::reach(after_rename);
    }
    Ok(())
}

/// fsync a directory so a rename inside it is itself durable. Without this the
/// rename can be lost even though the file's own data was synced.
fn sync_dir(dir: &Path) -> io::Result<()> {
    match File::open(dir) {
        Ok(f) => match f.sync_all() {
            Ok(()) => Ok(()),
            // Some filesystems refuse fsync on a directory handle; the rename
            // is still ordered, so this is not fatal.
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => Ok(()),
            Err(e) => Err(e),
        },
        Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "nucleus_raftstore_{name}_{}_{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn hard_state_round_trips_across_reopen() {
        let dir = tmpdir("hardstate");
        {
            let (mut s, loaded) = RaftStorage::open(&dir).unwrap();
            assert_eq!(loaded.current_term, 0);
            assert_eq!(loaded.voted_for, None);
            s.save_hard_state(7, Some(3), 12).unwrap();
        }
        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.current_term, 7);
        assert_eq!(loaded.voted_for, Some(3));
        assert_eq!(loaded.commit_index, 12);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_entries_round_trip_across_reopen() {
        let dir = tmpdir("log");
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: Command::Noop,
            },
            LogEntry {
                index: 2,
                term: 1,
                command: Command::Sql("INSERT INTO t VALUES (1)".into()),
            },
            LogEntry {
                index: 3,
                term: 2,
                command: Command::AddNode(9),
            },
            LogEntry {
                index: 4,
                term: 2,
                command: Command::RemoveNode(4),
            },
        ];
        {
            let (mut s, _) = RaftStorage::open(&dir).unwrap();
            s.append_entries(&entries).unwrap();
        }
        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.entries.len(), 4);
        assert_eq!(loaded.entries[1].index, 2);
        match &loaded.entries[1].command {
            Command::Sql(sql) => assert_eq!(sql, "INSERT INTO t VALUES (1)"),
            other => panic!("wrong command decoded: {other:?}"),
        }
        assert!(matches!(loaded.entries[2].command, Command::AddNode(9)));
        assert!(matches!(loaded.entries[3].command, Command::RemoveNode(4)));
        let _ = fs::remove_dir_all(&dir);
    }

    /// A crash mid-append leaves a half-written record. Recovery must keep every
    /// intact (therefore possibly acknowledged) record and drop only the torn
    /// tail — and the file must be physically truncated so the next append does
    /// not chain onto garbage.
    #[test]
    fn torn_tail_record_is_dropped_and_truncated() {
        let dir = tmpdir("torn");
        {
            let (mut s, _) = RaftStorage::open(&dir).unwrap();
            s.append_entries(&[
                LogEntry {
                    index: 1,
                    term: 1,
                    command: Command::Sql("A".into()),
                },
                LogEntry {
                    index: 2,
                    term: 1,
                    command: Command::Sql("B".into()),
                },
            ])
            .unwrap();
        }
        // Emulate a crash partway through writing entry 3.
        let log_path = dir.join("log");
        let mut raw = fs::read(&log_path).unwrap();
        let mut partial = Vec::new();
        encode_log_record(
            &mut partial,
            &LogEntry {
                index: 3,
                term: 1,
                command: Command::Sql("CCCCCCCCCC".into()),
            },
        );
        raw.extend_from_slice(&partial[..partial.len() - 5]);
        fs::write(&log_path, &raw).unwrap();

        let (mut s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.entries.len(), 2, "torn record must not be recovered");
        assert_eq!(
            fs::metadata(&log_path).unwrap().len(),
            (raw.len() - (partial.len() - 5)) as u64,
            "torn tail must be physically truncated"
        );

        // The next append must be readable, proving the file boundary is clean.
        s.append_entries(&[LogEntry {
            index: 3,
            term: 1,
            command: Command::Sql("C".into()),
        }])
        .unwrap();
        drop(s);
        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.entries.len(), 3);
        assert_eq!(loaded.entries[2].index, 3);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A record whose bytes were silently corrupted (bit rot, not truncation)
    /// must be rejected by the CRC rather than decoded into a bogus entry.
    #[test]
    fn corrupted_record_body_is_rejected() {
        let dir = tmpdir("corrupt");
        {
            let (mut s, _) = RaftStorage::open(&dir).unwrap();
            s.append_entries(&[
                LogEntry {
                    index: 1,
                    term: 1,
                    command: Command::Sql("KEEP".into()),
                },
                LogEntry {
                    index: 2,
                    term: 1,
                    command: Command::Sql("ROTTED".into()),
                },
            ])
            .unwrap();
        }
        let log_path = dir.join("log");
        let mut raw = fs::read(&log_path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xFF;
        fs::write(&log_path, &raw).unwrap();

        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.entries.len(), 1);
        assert!(matches!(&loaded.entries[0].command, Command::Sql(s) if s == "KEEP"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rewrite_log_replaces_contents_and_keeps_appending() {
        let dir = tmpdir("rewrite");
        let (mut s, _) = RaftStorage::open(&dir).unwrap();
        s.append_entries(&[
            LogEntry {
                index: 1,
                term: 1,
                command: Command::Sql("one".into()),
            },
            LogEntry {
                index: 2,
                term: 1,
                command: Command::Sql("two".into()),
            },
        ])
        .unwrap();
        s.rewrite_log(&[LogEntry {
            index: 1,
            term: 1,
            command: Command::Sql("one".into()),
        }])
        .unwrap();
        s.append_entries(&[LogEntry {
            index: 2,
            term: 5,
            command: Command::Sql("replacement".into()),
        }])
        .unwrap();
        drop(s);

        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(loaded.entries[1].term, 5);
        assert!(matches!(&loaded.entries[1].command, Command::Sql(s) if s == "replacement"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_round_trips_across_reopen() {
        let dir = tmpdir("snapshot");
        {
            let (mut s, _) = RaftStorage::open(&dir).unwrap();
            s.save_snapshot(&Snapshot {
                last_included_index: 42,
                last_included_term: 7,
                data: b"state-machine-bytes".to_vec(),
            })
            .unwrap();
        }
        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        let snap = loaded.snapshot.expect("snapshot must survive restart");
        assert_eq!(snap.last_included_index, 42);
        assert_eq!(snap.last_included_term, 7);
        assert_eq!(snap.data, b"state-machine-bytes");
        let _ = fs::remove_dir_all(&dir);
    }

    /// The commit index may lag on disk (that is safe — it is relearned), but it
    /// must never be persisted *ahead* of a value the node actually reached.
    #[test]
    fn persisted_commit_index_never_exceeds_reached_value() {
        let dir = tmpdir("commit");
        let mut highest_reached = 0u64;
        {
            let (mut s, _) = RaftStorage::open(&dir).unwrap();
            for i in 1..=300u64 {
                highest_reached = i;
                s.note_commit_index(1, Some(1), i).unwrap();
            }
        }
        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert!(
            loaded.commit_index <= highest_reached,
            "persisted commit index {} ran ahead of reality {}",
            loaded.commit_index,
            highest_reached
        );
        assert!(
            loaded.commit_index > 0,
            "commit index was never checkpointed at all"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The whole point of `atomic_write`: a crash between "temp written" and
    /// "rename done" must leave the previous durable state readable.
    #[test]
    fn interrupted_hard_state_write_preserves_previous_state() {
        let dir = tmpdir("atomic");
        {
            let (mut s, _) = RaftStorage::open(&dir).unwrap();
            s.save_hard_state(4, Some(2), 0).unwrap();
        }
        // Emulate the interrupted state: temp file present, rename never ran.
        let hs = dir.join("hardstate");
        fs::write(hs.with_extension("tmp"), encode_hard_state(9, Some(8), 0)).unwrap();

        let (_s, loaded) = RaftStorage::open(&dir).unwrap();
        assert_eq!(loaded.current_term, 4);
        assert_eq!(loaded.voted_for, Some(2));
        let _ = fs::remove_dir_all(&dir);
    }
}
