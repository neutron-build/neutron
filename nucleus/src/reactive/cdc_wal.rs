//! Write-ahead log for the CDC (Change Data Capture) log.
//!
//! Provides crash-recovery by recording all CDC mutations to an append-only
//! log file (`cdc.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory CdcLog state.
//!
//! ## Log entry binary format
//! ```text
//! APPEND:   [0x01] [sequence: u64 LE] [table_len: u32 LE] [table: bytes]
//!           [change_type: u8] [timestamp: u64 LE]
//!           [n_fields: u32 LE] [per field: key_len(u32) + key + val_len(u32) + val]
//! CONSUMER: [0x02] [name_len: u32 LE] [name: bytes] [position: u64 LE]
//! SNAPSHOT: [0x03] [next_sequence: u64 LE]
//!           [n_entries: u32 LE] [per entry: same as APPEND payload]
//!           [n_consumers: u32 LE] [per consumer: name_len(u32) + name + position(u64)]
//! ```
//!
//! ## What CDC records, and why it is not transactional (S63 determination)
//!
//! This log records EMITTED CHANGE EVENTS (APPEND) and consumer positions
//! (CONSUMER) — a change feed, not user data. Emission is fire-and-forget by
//! design: `notify_change_rows` appends at STATEMENT time, inside explicit
//! transactions as readily as outside them, is never enlisted in the
//! transaction's write-set, and is never compensated on ROLLBACK (an aborted
//! transaction's events stay in the feed — "best-effort", see the call sites
//! in `dml.rs`). Whether that is the right semantics is NU-107, an open
//! product call; this file does not decide it.
//!
//! The S63 consequence: no CDC record is ever written under a coordinating
//! transaction id, so tagging production records would be inert. The
//! forward-correct plumbing is live anyway (twinned tags below, the
//! committed-set replay filter, the XactId floor feed, and the `xact`
//! parameters on the log functions) so that if NU-107 lands transactional
//! CDC — emission at commit, discard on rollback — the records are already
//! tagged and the filter already discards; an untagged writer would
//! reintroduce exactly the resurrection defect S63 exists to close. Until
//! then every record carries `XACT_AUTOCOMMIT` (0), and a failed CDC
//! checkpoint can never strand a vouching COMMIT record — which is why CDC
//! stays warn-and-continue in the specialty checkpoint pass.
//!
//! A SNAPSHOT resets all state. After `checkpoint()` the file is truncated to
//! a single SNAPSHOT entry so the log stays small.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::executor::enlistment::XACT_AUTOCOMMIT;
use crate::storage::wal_util::WalSync;

use super::{CdcLog, CdcLogEntry, ChangeType};

// ---- Entry type tags --------------------------------------------------------

const ENTRY_APPEND: u8 = 0x01;
const ENTRY_CONSUMER: u8 = 0x02;
const ENTRY_SNAPSHOT: u8 = 0x03;
/// S63: APPEND carrying the coordinating transaction id — inert today (see
/// the module header); live for the day NU-107 lands transactional CDC.
const ENTRY_APPEND_XACT: u8 = 0x04;
/// S63: CONSUMER carrying the coordinating transaction id.
const ENTRY_CONSUMER_XACT: u8 = 0x05;

// ---- Change type encoding ---------------------------------------------------

fn encode_change_type(ct: &ChangeType) -> u8 {
    match ct {
        ChangeType::Insert => 0,
        ChangeType::Update => 1,
        ChangeType::Delete => 2,
    }
}

fn decode_change_type(b: u8) -> Option<ChangeType> {
    match b {
        0 => Some(ChangeType::Insert),
        1 => Some(ChangeType::Update),
        2 => Some(ChangeType::Delete),
        _ => None,
    }
}

// ---- Public types -----------------------------------------------------------

/// Recovered CDC state from WAL replay.
pub struct CdcWalState {
    pub entries: Vec<CdcLogEntry>,
    pub consumers: HashMap<String, u64>,
    pub next_sequence: u64,
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63).
    pub max_xact_id: u64,
}

/// Append-only CDC WAL.
pub struct CdcWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Append/sync bookkeeping for group commit. These appends previously
    /// ended at `BufWriter::flush`, reaching the kernel but never the device --
    /// an acknowledged change event survived `kill -9` but not power loss.
    /// NU-006. Whether CDC should be *transactional* is a separate open
    /// question (NU-107); this only makes the ack honest.
    syncer: WalSync,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
    /// The highest coordinating transaction id recovered at open (S63).
    max_xact_id: u64,
}

impl CdcWal {
    /// Open or create the WAL file in `dir`, replaying with an EMPTY
    /// committed set so every tagged record keeps — the pre-S63 contract.
    /// The executor opens through [`CdcWal::open_with_committed`] instead.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, CdcWalState)> {
        Self::open_with_committed(dir, &HashSet::new())
    }

    /// Open or create the WAL file in `dir` whose replay is filtered by the
    /// S63 committed set: a tagged record whose coordinating transaction id
    /// is neither `XACT_AUTOCOMMIT` nor in `committed` was written inside a
    /// transaction that never committed, and is discarded. (No production
    /// writer carries a non-zero id today — see the module header; the
    /// filter is the forward-correct half of the NU-107 plumbing.)
    pub fn open_with_committed(
        dir: &Path,
        committed: &HashSet<u64>,
    ) -> io::Result<(Self, CdcWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("cdc.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data, committed)
        } else {
            CdcWalState {
                entries: Vec::new(),
                consumers: HashMap::new(),
                next_sequence: 1,
                max_xact_id: 0,
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let max_xact_id = state.max_xact_id;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: WalSync::new(),
                stranded: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
                max_xact_id,
            },
            state,
        ))
    }

    /// The highest coordinating transaction id this log recovered (S63), 0
    /// when it holds none. Seeds the executor's XactId counter so a reopened
    /// process never mints an id a surviving tagged record already carries.
    pub fn max_xact_id(&self) -> u64 {
        self.max_xact_id
    }

    /// Re-point the writer at the live log file after a checkpoint replaced
    /// the file but could not reopen it. While stranded, `writer` holds an
    /// UNLINKED inode — appends to it succeed into a file no future recovery
    /// reads — so this runs before every append: a successful reopen recovers
    /// the writer, and a failed one fails the append loudly instead of
    /// letting it acknowledge a write to a dead inode.
    fn reattach_if_stranded(&self, w: &mut BufWriter<File>) -> io::Result<()> {
        if !self.stranded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("cdc.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "CDC WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Flush + `fsync` the log, capturing (under the writer lock) the highest
    /// append LSN the fsync covers.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.syncer.current();
        w.flush()?;
        w.get_ref().sync_all()?;
        Ok(covered)
    }

    /// Group-commit sync: durable coverage of every append made before this
    /// call; concurrent committers share fsyncs.
    pub fn group_sync(&self) -> io::Result<()> {
        self.syncer.group_sync(|| self.sync_covering())
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.syncer.is_dirty()
    }

    /// Log a CDC append operation (new change event).
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction
    /// (every write today — see the module header), `Some(id)` inside one,
    /// `None` to write the legacy untagged record (kept unconditionally on
    /// replay — the pre-S63 compatibility rule).
    pub fn log_append(&self, xact: Option<u64>, entry: &CdcLogEntry) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_APPEND, ENTRY_APPEND_XACT);

        // sequence
        buf.extend_from_slice(&entry.sequence.to_le_bytes());

        // table
        write_str(&mut buf, &entry.table);

        // change_type
        buf.push(encode_change_type(&entry.change_type));

        // timestamp
        buf.extend_from_slice(&entry.timestamp.to_le_bytes());

        // row_data fields
        buf.extend_from_slice(&(entry.row_data.len() as u32).to_le_bytes());
        for (k, v) in &entry.row_data {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }

        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a consumer position update (acknowledge).
    ///
    /// `xact` mirrors [`CdcWal::log_append`].
    pub fn log_consumer(&self, xact: Option<u64>, name: &str, position: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_CONSUMER, ENTRY_CONSUMER_XACT);
        write_str(&mut buf, name);
        buf.extend_from_slice(&position.to_le_bytes());

        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Write a full snapshot and truncate the log to just that snapshot.
    pub fn checkpoint(&self, cdc_log: &CdcLog) -> io::Result<()> {
        let mut payload = Vec::new();

        // next_sequence: we need to reconstruct this; it's the max sequence + 1
        // Since CdcLog doesn't expose next_sequence directly, we derive it from
        // the last entry's sequence + 1, or 1 if empty.
        let next_seq = if cdc_log.is_empty() {
            1u64
        } else {
            // Use the read_from method to get all entries and find the max sequence
            let all = cdc_log.read_from(0, usize::MAX);
            all.last().map(|e| e.sequence + 1).unwrap_or(1)
        };
        payload.extend_from_slice(&next_seq.to_le_bytes());

        // entries
        let all_entries = cdc_log.read_from(0, usize::MAX);
        payload.extend_from_slice(&(all_entries.len() as u32).to_le_bytes());
        for entry in &all_entries {
            payload.extend_from_slice(&entry.sequence.to_le_bytes());
            write_str(&mut payload, &entry.table);
            payload.push(encode_change_type(&entry.change_type));
            payload.extend_from_slice(&entry.timestamp.to_le_bytes());
            payload.extend_from_slice(&(entry.row_data.len() as u32).to_le_bytes());
            for (k, v) in &entry.row_data {
                write_str(&mut payload, k);
                write_str(&mut payload, v);
            }
        }

        // consumers: serialize the real positions into the snapshot. checkpoint()
        // truncates the file below, so any CONSUMER entries written before this
        // point are discarded — writing 0 here previously LOST every consumer
        // offset on checkpoint (consumers would re-read from their old position
        // or restart). Persist them in the snapshot instead.
        let consumers = cdc_log.consumers();
        payload.extend_from_slice(&(consumers.len() as u32).to_le_bytes());
        for (name, position) in consumers {
            write_str(&mut payload, name);
            payload.extend_from_slice(&position.to_le_bytes());
        }

        // Serialize the complete new log body (SNAPSHOT tag + payload).
        let mut contents = Vec::with_capacity(payload.len() + 1);
        contents.push(ENTRY_SNAPSHOT);
        contents.extend_from_slice(&payload);

        // Hold the writer lock across the whole checkpoint so no append can interleave
        // between the flush and the reopen. Replace atomically — temp file + fsync +
        // rename — so a crash mid-checkpoint leaves the old log or the new snapshot,
        // never an empty file.
        let mut w = self.writer.lock();
        w.flush()?;
        crate::storage::wal_util::atomic_replace_wal(&self.path, &contents)?;
        // The reopen is the hazardous half: the rename above already unlinked
        // the inode `w` holds, so a failure here leaves the writer pointing at
        // a file no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            .then(|| io::Error::other("injected CDC WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("cdc.wal_reopen") {
            Err(e)
        } else {
            OpenOptions::new().append(true).open(&self.path)
        };
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                // The rename already happened, so the handle in `w` is now an
                // unlinked inode. Mark the writer stranded: appends must
                // reattach (or fail loudly), never write through it.
                self.stranded
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(e);
            }
        };
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// Reconstruct a CdcLog from recovered WAL state.
pub fn rebuild_cdc_log(state: &CdcWalState) -> CdcLog {
    let mut log = CdcLog::new();
    // Replay all entries using the internal append_with method to preserve
    // sequence numbers and timestamps. Since CdcLog::append auto-generates
    // these, we use a lower-level reconstruction approach.
    for entry in &state.entries {
        log.append_recovered(
            entry.sequence,
            &entry.table,
            entry.change_type.clone(),
            entry.row_data.clone(),
            entry.timestamp,
        );
    }
    // Restore consumer positions
    for (name, pos) in &state.consumers {
        log.register_consumer(name);
        log.acknowledge(name, *pos);
    }
    log
}

// ---- Binary encoding helpers ------------------------------------------------

/// Emit the tag for one record: the `_XACT` twin plus the id when `xact` is
/// `Some`, the legacy untagged tag when `None`.
fn push_tag(buf: &mut Vec<u8>, xact: Option<u64>, plain: u8, xact_tagged: u8) {
    match xact {
        Some(x) => {
            buf.push(xact_tagged);
            buf.extend_from_slice(&x.to_le_bytes());
        }
        None => buf.push(plain),
    }
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

fn replay(data: &[u8], committed: &HashSet<u64>) -> CdcWalState {
    let mut entries: Vec<CdcLogEntry> = Vec::new();
    let mut consumers: HashMap<String, u64> = HashMap::new();
    let mut next_sequence: u64 = 1;
    let mut max_xact_id: u64 = 0;
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        // S63: the tagged twins carry the coordinating transaction id next;
        // the id feeds the floor whether kept or discarded, and the record is
        // dropped unless it is autocommit or committed. The body is parsed
        // past either way — nothing length-frames these records and the next
        // one must be found.
        let mut keep_tagged = true;
        if matches!(entry_type, ENTRY_APPEND_XACT | ENTRY_CONSUMER_XACT) {
            let Some(id_bytes) = data.get(pos..pos + 8) else {
                break;
            };
            let xact = u64::from_le_bytes(id_bytes.try_into().unwrap());
            pos += 8;
            max_xact_id = max_xact_id.max(xact);
            keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
        }

        match entry_type {
            ENTRY_APPEND | ENTRY_APPEND_XACT => {
                let Some(entry) = replay_append(data, &mut pos) else {
                    break;
                };
                if entry.sequence >= next_sequence {
                    next_sequence = entry.sequence + 1;
                }
                if keep_tagged {
                    entries.push(entry);
                }
            }
            ENTRY_CONSUMER | ENTRY_CONSUMER_XACT => {
                let Some(name) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(position) = read_u64(data, &mut pos) else {
                    break;
                };
                if keep_tagged {
                    consumers.insert(name, position);
                }
            }
            ENTRY_SNAPSHOT => {
                entries.clear();
                consumers.clear();
                let Some(ns) = read_u64(data, &mut pos) else {
                    break;
                };
                next_sequence = ns;
                let Some(n_entries) = read_u32(data, &mut pos) else {
                    break;
                };
                let mut ok = true;
                for _ in 0..n_entries as usize {
                    let Some(entry) = replay_append(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    entries.push(entry);
                }
                if !ok {
                    break;
                }
                let Some(n_consumers) = read_u32(data, &mut pos) else {
                    break;
                };
                for _ in 0..n_consumers as usize {
                    let Some(name) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(position) = read_u64(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    consumers.insert(name, position);
                }
                if !ok {
                    break;
                }
            }
            _ => {
                break;
            }
        }
    }

    CdcWalState {
        entries,
        consumers,
        next_sequence,
        max_xact_id,
    }
}

fn replay_append(data: &[u8], pos: &mut usize) -> Option<CdcLogEntry> {
    let sequence = read_u64(data, pos)?;
    let table = read_string(data, pos)?;
    let ct_byte = *data.get(*pos)?;
    *pos += 1;
    let change_type = decode_change_type(ct_byte)?;
    let timestamp = read_u64(data, pos)?;
    let n_fields = read_u32(data, pos)? as usize;
    let mut row_data = HashMap::new();
    for _ in 0..n_fields {
        let k = read_string(data, pos)?;
        let v = read_string(data, pos)?;
        row_data.insert(k, v);
    }
    Some(CdcLogEntry {
        sequence,
        table,
        change_type,
        row_data,
        timestamp,
    })
}

// ---- Primitive readers ------------------------------------------------------

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── S63: the recovery filter (NU-107 forward plumbing) ───────────────

    /// Tagged records filter on the committed set: autocommit keeps,
    /// committed ids keep, unknown ids discard — and a discarded record in
    /// the MIDDLE does not stop the records after it. No production writer
    /// carries a non-zero id today (see the module header); this proves the
    /// filter that a transactional-CDC future will rely on.
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let entry = |seq: u64, table: &str| CdcLogEntry {
            sequence: seq,
            table: table.to_string(),
            change_type: ChangeType::Insert,
            row_data: make_row(&[("id", "1")]),
            timestamp: seq * 100,
        };
        let mut buf = Vec::new();
        // Legacy untagged APPEND (pre-S63 log): keep unconditionally.
        buf.push(ENTRY_APPEND);
        buf.extend_from_slice(&1u64.to_le_bytes());
        write_str(&mut buf, "legacy");
        buf.push(0);
        buf.extend_from_slice(&100u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        let tagged_append = |buf: &mut Vec<u8>, xact: u64, seq: u64, table: &str| {
            let e = entry(seq, table);
            let mut rec = Vec::new();
            push_tag(&mut rec, Some(xact), ENTRY_APPEND, ENTRY_APPEND_XACT);
            rec.extend_from_slice(&e.sequence.to_le_bytes());
            write_str(&mut rec, &e.table);
            rec.push(encode_change_type(&e.change_type));
            rec.extend_from_slice(&e.timestamp.to_le_bytes());
            rec.extend_from_slice(&0u32.to_le_bytes());
            buf.extend_from_slice(&rec);
        };
        tagged_append(&mut buf, XACT_AUTOCOMMIT, 2, "auto");
        tagged_append(&mut buf, 7, 3, "committed");
        tagged_append(&mut buf, 8, 4, "never_committed"); // discarded, mid-log
        // An uncommitted transaction's consumer ack is discarded too.
        let mut ack = Vec::new();
        push_tag(&mut ack, Some(8), ENTRY_CONSUMER, ENTRY_CONSUMER_XACT);
        write_str(&mut ack, "abandoned_app");
        ack.extend_from_slice(&9u64.to_le_bytes());
        buf.extend_from_slice(&ack);
        tagged_append(&mut buf, 9, 5, "committed_late");

        let committed: HashSet<u64> = [7u64, 9u64].into_iter().collect();
        let state = replay(&buf, &committed);
        assert_eq!(
            state.max_xact_id, 9,
            "discarded records still feed the floor"
        );
        let mut tables: Vec<&str> = state.entries.iter().map(|e| e.table.as_str()).collect();
        tables.sort_unstable();
        assert_eq!(
            tables,
            vec!["auto", "committed", "committed_late", "legacy"],
            "id 8 never committed; its event and its consumer ack must be \
             discarded, not replayed"
        );
        assert!(
            !state.consumers.contains_key("abandoned_app"),
            "the abandoned CONSUMER record must be discarded"
        );
        assert_eq!(
            state.next_sequence, 6,
            "a discarded record's sequence still advances the counter floor"
        );
    }

    fn make_row(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = CdcWal::open(dir.path()).unwrap();
        assert!(state.entries.is_empty());
        assert_eq!(state.next_sequence, 1);

        let entry1 = CdcLogEntry {
            sequence: 1,
            table: "users".to_string(),
            change_type: ChangeType::Insert,
            row_data: make_row(&[("id", "1"), ("name", "Alice")]),
            timestamp: 1000,
        };
        let entry2 = CdcLogEntry {
            sequence: 2,
            table: "orders".to_string(),
            change_type: ChangeType::Update,
            row_data: make_row(&[("id", "5"), ("amount", "200")]),
            timestamp: 2000,
        };
        let entry3 = CdcLogEntry {
            sequence: 3,
            table: "users".to_string(),
            change_type: ChangeType::Delete,
            row_data: make_row(&[("id", "1")]),
            timestamp: 3000,
        };

        wal.log_append(Some(XACT_AUTOCOMMIT), &entry1).unwrap();
        wal.log_append(Some(XACT_AUTOCOMMIT), &entry2).unwrap();
        wal.log_append(Some(XACT_AUTOCOMMIT), &entry3).unwrap();
        drop(wal);

        let (_wal2, state2) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state2.entries.len(), 3);
        assert_eq!(state2.next_sequence, 4);

        assert_eq!(state2.entries[0].table, "users");
        assert_eq!(state2.entries[0].change_type, ChangeType::Insert);
        assert_eq!(state2.entries[0].sequence, 1);
        assert_eq!(state2.entries[0].timestamp, 1000);

        assert_eq!(state2.entries[1].table, "orders");
        assert_eq!(state2.entries[1].change_type, ChangeType::Update);

        assert_eq!(state2.entries[2].table, "users");
        assert_eq!(state2.entries[2].change_type, ChangeType::Delete);
    }

    #[test]
    fn test_consumer_tracking_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = CdcWal::open(dir.path()).unwrap();

        let entry = CdcLogEntry {
            sequence: 1,
            table: "t".to_string(),
            change_type: ChangeType::Insert,
            row_data: make_row(&[("x", "1")]),
            timestamp: 100,
        };
        wal.log_append(Some(XACT_AUTOCOMMIT), &entry).unwrap();
        wal.log_consumer(Some(XACT_AUTOCOMMIT), "app1", 1).unwrap();
        wal.log_consumer(Some(XACT_AUTOCOMMIT), "app2", 0).unwrap();
        drop(wal);

        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state.consumers["app1"], 1);
        assert_eq!(state.consumers["app2"], 0);
    }

    #[test]
    fn test_consumer_offsets_survive_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = CdcWal::open(dir.path()).unwrap();

        // Build a CdcLog with entries and acknowledged consumer positions.
        let mut log = crate::reactive::CdcLog::new();
        log.append("t", ChangeType::Insert, make_row(&[("x", "1")]));
        log.append("t", ChangeType::Insert, make_row(&[("x", "2")]));
        log.acknowledge("app1", 2);
        log.register_consumer("app2"); // position 0

        // checkpoint() truncates the WAL — consumer offsets must be persisted
        // into the snapshot, not lost (they were previously written as 0).
        wal.checkpoint(&log).unwrap();
        drop(wal);

        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state.consumers.get("app1"), Some(&2));
        assert_eq!(state.consumers.get("app2"), Some(&0));
    }

    #[test]
    fn test_empty_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = CdcWal::open(dir.path()).unwrap();
        assert!(state.entries.is_empty());
        assert!(state.consumers.is_empty());
        assert_eq!(state.next_sequence, 1);
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged change event silently vanishes at restart. The
    /// discriminator is durability: the post-failure append must land in the
    /// replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = CdcWal::open(dir.path()).unwrap();
            let mut log = crate::reactive::CdcLog::new();
            log.append("t", ChangeType::Insert, make_row(&[("x", "1")]));
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&log)
                .expect_err("the injected reopen failure must fail the checkpoint");
            log.append("t", ChangeType::Insert, make_row(&[("x", "2")]));
            let entry = CdcLogEntry {
                sequence: log.read_from(0, usize::MAX).last().unwrap().sequence,
                table: "t".to_string(),
                change_type: ChangeType::Insert,
                row_data: make_row(&[("x", "2")]),
                timestamp: 200,
            };
            wal.log_append(Some(XACT_AUTOCOMMIT), &entry)
                .expect("a later append must reattach, not strand");
        }
        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(
            state.entries.len(),
            2,
            "the post-checkpoint-failure append went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("cdc.wal");

        {
            let (wal, _) = CdcWal::open(dir.path()).unwrap();
            let entry = CdcLogEntry {
                sequence: 1,
                table: "good_table".to_string(),
                change_type: ChangeType::Insert,
                row_data: make_row(&[("id", "1")]),
                timestamp: 500,
            };
            wal.log_append(Some(XACT_AUTOCOMMIT), &entry).unwrap();
            drop(wal);
        }

        // Append garbage
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();
            f.flush().unwrap();
        }

        let (_wal, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.entries[0].table, "good_table");
    }

    #[test]
    fn test_rebuild_cdc_log() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = CdcWal::open(dir.path()).unwrap();

        let entry1 = CdcLogEntry {
            sequence: 1,
            table: "users".to_string(),
            change_type: ChangeType::Insert,
            row_data: make_row(&[("id", "1")]),
            timestamp: 100,
        };
        let entry2 = CdcLogEntry {
            sequence: 2,
            table: "users".to_string(),
            change_type: ChangeType::Update,
            row_data: make_row(&[("id", "1"), ("name", "Bob")]),
            timestamp: 200,
        };

        wal.log_append(Some(XACT_AUTOCOMMIT), &entry1).unwrap();
        wal.log_append(Some(XACT_AUTOCOMMIT), &entry2).unwrap();
        wal.log_consumer(Some(XACT_AUTOCOMMIT), "reader1", 1)
            .unwrap();
        drop(wal);

        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        let log = rebuild_cdc_log(&state);

        assert_eq!(log.len(), 2);
        assert_eq!(log.consumer_position("reader1"), 1);

        // Read from position 1 should return entry2 only
        let pending = log.read_from(1, 100);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sequence, 2);
        assert_eq!(pending[0].table, "users");
    }

    #[test]
    fn test_all_change_types_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = CdcWal::open(dir.path()).unwrap();

        for (seq, ct) in [
            (1, ChangeType::Insert),
            (2, ChangeType::Update),
            (3, ChangeType::Delete),
        ] {
            wal.log_append(
                Some(XACT_AUTOCOMMIT),
                &CdcLogEntry {
                    sequence: seq,
                    table: "t".to_string(),
                    change_type: ct,
                    row_data: HashMap::new(),
                    timestamp: seq * 100,
                },
            )
            .unwrap();
        }
        drop(wal);

        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state.entries[0].change_type, ChangeType::Insert);
        assert_eq!(state.entries[1].change_type, ChangeType::Update);
        assert_eq!(state.entries[2].change_type, ChangeType::Delete);
    }
}
