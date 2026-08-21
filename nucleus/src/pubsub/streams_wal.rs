//! Write-ahead log for Streams (Redis-style append-only logs).
//!
//! Provides crash-recovery by recording all stream mutations to an append-only
//! log file (`streams.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory Stream state.
//!
//! ## Log entry binary format
//! ```text
//! XADD:     [0x01] [stream_name_len: u32 LE] [stream_name: bytes]
//!           [ms: u64 LE] [seq: u64 LE]
//!           [n_fields: u32 LE] [per field: key_len(u32) + key + val_len(u32) + val]
//! SNAPSHOT: [0x02] [n_streams: u32 LE]
//!           [per stream: name_len(u32) + name + n_entries(u32)
//!            + per entry: ms(u64) + seq(u64) + n_fields(u32)
//!            + per field: key_len(u32) + key + val_len(u32) + val]
//! ```
//!
//! A SNAPSHOT resets all state. After `checkpoint()` the file is truncated to
//! a single SNAPSHOT entry so the log stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::{Stream, StreamEntryId};

// ---- Entry type tags --------------------------------------------------------

const ENTRY_XADD: u8 = 0x01;
const ENTRY_SNAPSHOT: u8 = 0x02;

// ---- Public types -----------------------------------------------------------

/// One stream entry: its ID plus ordered field/value pairs.
pub type StreamEntry = (StreamEntryId, Vec<(String, String)>);

/// Per-stream recovered entries, keyed by stream name.
pub type StreamsMap = HashMap<String, Vec<StreamEntry>>;

/// Recovered streams state from WAL replay.
pub struct StreamsWalState {
    /// `stream_name -> Vec<(entry_id, fields)>` in order.
    pub streams: StreamsMap,
}

/// Append-only Streams WAL.
pub struct StreamsWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Group-commit fsync coordinator (durability of the un-checkpointed tail).
    syncer: crate::storage::wal_util::WalSync,
}

impl StreamsWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. A torn or corrupt tail ends replay and is truncated
    /// away, so subsequent appends land on a valid boundary (they would
    /// otherwise sit behind garbage and be lost to every future replay — this
    /// log carries no checksum, so replay stopping is the only detection there
    /// is). Same treatment as `blob/wal.rs::open`.
    pub fn open(dir: &Path) -> io::Result<(Self, StreamsWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("streams.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            let (state, valid_end) = replay(&data);
            if valid_end < data.len() {
                eprintln!(
                    "streams WAL: truncating {} torn/corrupt trailing bytes",
                    data.len() - valid_end
                );
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(valid_end as u64)?;
                f.sync_all()?;
            }
            state
        } else {
            StreamsWalState {
                streams: HashMap::new(),
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: crate::storage::wal_util::WalSync::new(),
            },
            state,
        ))
    }

    /// Log an XADD operation (stream append).
    pub fn log_xadd(
        &self,
        stream_name: &str,
        entry_id: &StreamEntryId,
        fields: &[(String, String)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_XADD);

        // stream name
        write_str(&mut buf, stream_name);

        // entry ID (ms + seq)
        buf.extend_from_slice(&entry_id.ms.to_le_bytes());
        buf.extend_from_slice(&entry_id.seq.to_le_bytes());

        // fields
        buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (k, v) in fields {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
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

    /// Write a full snapshot and truncate the log to just that snapshot.
    ///
    /// `streams` maps stream name to its current entries.
    pub fn checkpoint(&self, streams: &HashMap<String, Stream>) -> io::Result<()> {
        let mut payload = Vec::new();

        // n_streams
        payload.extend_from_slice(&(streams.len() as u32).to_le_bytes());

        for (name, stream) in streams {
            // stream name
            write_str(&mut payload, name);

            // n_entries
            payload.extend_from_slice(&(stream.entries.len() as u32).to_le_bytes());

            for entry in &stream.entries {
                // entry ID
                payload.extend_from_slice(&entry.id.ms.to_le_bytes());
                payload.extend_from_slice(&entry.id.seq.to_le_bytes());

                // fields
                payload.extend_from_slice(&(entry.fields.len() as u32).to_le_bytes());
                for (k, v) in &entry.fields {
                    write_str(&mut payload, k);
                    write_str(&mut payload, v);
                }
            }
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
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *w = BufWriter::new(file);
        // The snapshot was fsync'd by `atomic_replace_wal`; count it as covered.
        let mark = self.syncer.on_append();
        self.syncer.mark_synced(mark);
        Ok(())
    }
}

/// Reconstruct in-memory Streams from recovered WAL state.
///
/// Call this after `StreamsWal::open()` to rebuild the `HashMap<String, Stream>`.
pub fn rebuild_streams(state: &StreamsWalState) -> HashMap<String, Stream> {
    let mut result = HashMap::new();
    for (name, entries) in &state.streams {
        let mut stream = Stream::new();
        for (id, fields) in entries {
            stream.xadd_with_id(id.clone(), fields.clone());
        }
        result.insert(name.clone(), stream);
    }
    result
}

// ---- Binary encoding helpers ------------------------------------------------

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

/// Replay all entries in `data`. Returns the recovered state and the byte
/// offset of the first torn/corrupt entry (== `data.len()` when fully valid).
///
/// No arm here half-applies: the XADD arm pushes only after every field parses,
/// and the SNAPSHOT arm builds a temporary map and swaps it in only on success.
/// So the state accumulated when an entry is abandoned already equals a replay
/// of the clean prefix, and `entry_start` is the truncation point.
fn replay(data: &[u8]) -> (StreamsWalState, usize) {
    let mut streams: StreamsMap = HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let entry_start = pos;
        macro_rules! torn {
            () => {{
                return (StreamsWalState { streams }, entry_start);
            }};
        }

        let Some(&entry_type) = data.get(pos) else {
            torn!();
        };
        pos += 1;

        match entry_type {
            ENTRY_XADD => {
                let Some(stream_name) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(ms) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(seq) = read_u64(data, &mut pos) else {
                    torn!();
                };
                let Some(n_fields) = read_u32(data, &mut pos) else {
                    torn!();
                };
                // `n_fields` comes off disk and this WAL carries NO checksum, so
                // nothing rejects a corrupt length before it reaches here. An
                // unbounded `with_capacity` ABORTS the process on Linux (an
                // allocation failure is not an `Err`) and silently succeeds on an
                // overcommitting macOS. Each field costs at least two 4-byte
                // length prefixes, so the bytes remaining are an exact bound.
                let mut fields =
                    Vec::with_capacity(bounded_by_remaining(data, pos, n_fields as usize, 8));
                let mut ok = true;
                for _ in 0..n_fields {
                    let Some(k) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(v) = read_string(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    fields.push((k, v));
                }
                if !ok {
                    torn!();
                }
                streams
                    .entry(stream_name)
                    .or_default()
                    .push((StreamEntryId::new(ms, seq), fields));
            }
            ENTRY_SNAPSHOT => {
                // Parse into a temporary map and only swap it in once the snapshot
                // parses completely. Clearing first meant a corrupt/truncated
                // snapshot wiped all already-recovered state before failing.
                let mut snapshot = StreamsMap::new();
                if replay_snapshot(data, &mut pos, &mut snapshot) {
                    streams = snapshot;
                } else {
                    torn!();
                }
            }
            _ => {
                // Unknown entry type -- corrupt data; keep the clean prefix.
                torn!();
            }
        }
    }

    (StreamsWalState { streams }, pos)
}

fn replay_snapshot(data: &[u8], pos: &mut usize, streams: &mut StreamsMap) -> bool {
    let Some(n_streams) = read_u32(data, pos) else {
        return false;
    };
    for _ in 0..n_streams as usize {
        let Some(name) = read_string(data, pos) else {
            return false;
        };
        let Some(n_entries) = read_u32(data, pos) else {
            return false;
        };
        // Off-disk count, unchecksummed file: bound it by the bytes actually
        // present. An entry costs at least ms(8) + seq(8) + n_fields(4).
        let mut entries =
            Vec::with_capacity(bounded_by_remaining(data, *pos, n_entries as usize, 20));
        for _ in 0..n_entries as usize {
            let Some(ms) = read_u64(data, pos) else {
                return false;
            };
            let Some(seq) = read_u64(data, pos) else {
                return false;
            };
            let Some(n_fields) = read_u32(data, pos) else {
                return false;
            };
            // Same: each field costs at least two 4-byte length prefixes.
            let mut fields =
                Vec::with_capacity(bounded_by_remaining(data, *pos, n_fields as usize, 8));
            for _ in 0..n_fields as usize {
                let Some(k) = read_string(data, pos) else {
                    return false;
                };
                let Some(v) = read_string(data, pos) else {
                    return false;
                };
                fields.push((k, v));
            }
            entries.push((StreamEntryId::new(ms, seq), fields));
        }
        streams.insert(name, entries);
    }
    true
}

// ---- Primitive readers ------------------------------------------------------

/// Bound a declared element count by the bytes actually left in `data`.
///
/// `min_elem_bytes` is the smallest number of bytes one element can possibly
/// occupy, so `remaining / min_elem_bytes` is a hard upper bound on how many
/// elements this buffer can really contain. Reserving that instead of the
/// declared count cannot over-reserve at all, and the caller's loop still stops
/// (and fails) the moment a read runs off the end — so a corrupt count yields a
/// clean failure rather than a `handle_alloc_error` abort.
fn bounded_by_remaining(data: &[u8], pos: usize, declared: usize, min_elem_bytes: usize) -> usize {
    declared.min(data.len().saturating_sub(pos) / min_elem_bytes)
}

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

    // ── Unbounded-preallocation class (NU-385) ──
    //
    // These counts are `u32`s read straight out of the WAL, and this file
    // carries NO checksum, so nothing rejects a bad length before the decoder
    // sees it. Handing one to `Vec::with_capacity` reserves it, and a Rust
    // allocation failure ABORTS the process (SIGABRT, no unwind, no `Err`, no
    // log) on Linux while silently succeeding on an overcommitting macOS — a
    // boot crash-loop from one corrupt file. An instrumented allocator recorded
    // peak single reservations of 206.2 GB / 171.8 GB / 206.2 GB here for
    // `u32::MAX` counts. Round-tripping honest data does NOT cover this, which
    // is why the class survived: each test below hands the replayer a count the
    // bytes cannot back and requires a refusal.

    fn push_str_field(buf: &mut Vec<u8>, s: &str) {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    }

    /// The bound itself: a `u32::MAX` count against a near-empty buffer must
    /// collapse to a handful of elements, never to the declared count.
    #[test]
    fn declared_count_is_bounded_by_bytes_present() {
        let data = [0u8; 16];
        assert_eq!(bounded_by_remaining(&data, 0, u32::MAX as usize, 8), 2);
        assert_eq!(bounded_by_remaining(&data, 0, u32::MAX as usize, 20), 0);
        // Past the end must saturate, not underflow.
        assert_eq!(bounded_by_remaining(&data, 999, u32::MAX as usize, 8), 0);
        // An honest count under the bound is passed through untouched.
        assert_eq!(bounded_by_remaining(&data, 0, 1, 8), 1);
    }

    /// XADD arm of `replay`: `n_fields` = `u32::MAX` with no fields behind it.
    #[test]
    fn xadd_absurd_field_count_is_refused_not_reserved() {
        let mut buf = vec![ENTRY_XADD];
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&1000u64.to_le_bytes()); // ms
        buf.extend_from_slice(&0u64.to_le_bytes()); // seq
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_fields, a lie
        let (state, _) = replay(&buf);
        assert!(
            state.streams.is_empty(),
            "a field count the bytes cannot back must abandon the entry"
        );
    }

    /// `replay_snapshot`: `n_entries` = `u32::MAX` with no entries behind it.
    #[test]
    fn snapshot_absurd_entry_count_is_refused_not_reserved() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_streams
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_entries, a lie
        let mut pos = 0usize;
        let mut map = StreamsMap::new();
        assert!(
            !replay_snapshot(&buf, &mut pos, &mut map),
            "an entry count the bytes cannot back must fail the snapshot"
        );
        assert!(map.is_empty());
    }

    /// `replay_snapshot`, per-entry: `n_fields` = `u32::MAX` inside an
    /// otherwise well-formed entry.
    #[test]
    fn snapshot_absurd_per_entry_field_count_is_refused_not_reserved() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_streams
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_entries, honest
        buf.extend_from_slice(&1000u64.to_le_bytes()); // ms
        buf.extend_from_slice(&0u64.to_le_bytes()); // seq
        buf.extend_from_slice(&u32::MAX.to_le_bytes()); // n_fields, a lie
        let mut pos = 0usize;
        let mut map = StreamsMap::new();
        assert!(
            !replay_snapshot(&buf, &mut pos, &mut map),
            "a per-entry field count the bytes cannot back must fail the snapshot"
        );
        assert!(map.is_empty());
    }

    /// A whole snapshot entry reached through `replay`, so the corrupt count
    /// must not wipe state already recovered from the entries before it.
    #[test]
    fn corrupt_snapshot_count_does_not_discard_recovered_state() {
        let mut buf = vec![ENTRY_XADD];
        push_str_field(&mut buf, "events");
        buf.extend_from_slice(&1000u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        push_str_field(&mut buf, "k");
        push_str_field(&mut buf, "v");
        // Now a snapshot whose stream count is a lie.
        buf.push(ENTRY_SNAPSHOT);
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        let (state, _) = replay(&buf);
        assert_eq!(state.streams.len(), 1);
        assert_eq!(state.streams["events"].len(), 1);
    }

    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        wal.log_xadd("s", &StreamEntryId::new(1, 0), &[("k".into(), "v".into())])
            .unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    #[test]
    fn test_xadd_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = StreamsWal::open(dir.path()).unwrap();
        assert!(state.streams.is_empty());

        wal.log_xadd(
            "events",
            &StreamEntryId::new(1000, 0),
            &[
                ("user".into(), "alice".into()),
                ("action".into(), "login".into()),
            ],
        )
        .unwrap();
        wal.log_xadd(
            "events",
            &StreamEntryId::new(1001, 0),
            &[
                ("user".into(), "bob".into()),
                ("action".into(), "logout".into()),
            ],
        )
        .unwrap();
        wal.log_xadd(
            "logs",
            &StreamEntryId::new(2000, 0),
            &[("level".into(), "info".into())],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state2) = StreamsWal::open(dir.path()).unwrap();
        assert_eq!(state2.streams.len(), 2);
        assert_eq!(state2.streams["events"].len(), 2);
        assert_eq!(state2.streams["logs"].len(), 1);

        let (id, fields) = &state2.streams["events"][0];
        assert_eq!(id.ms, 1000);
        assert_eq!(id.seq, 0);
        assert_eq!(fields[0], ("user".into(), "alice".into()));
    }

    #[test]
    fn test_rebuild_streams() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path()).unwrap();

        wal.log_xadd(
            "mystream",
            &StreamEntryId::new(100, 0),
            &[("k".into(), "v1".into())],
        )
        .unwrap();
        wal.log_xadd(
            "mystream",
            &StreamEntryId::new(200, 0),
            &[("k".into(), "v2".into())],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path()).unwrap();
        let rebuilt = rebuild_streams(&state);
        assert_eq!(rebuilt.len(), 1);
        let stream = &rebuilt["mystream"];
        assert_eq!(stream.xlen(), 2);
        assert_eq!(stream.entries[0].id, StreamEntryId::new(100, 0));
        assert_eq!(stream.entries[1].id, StreamEntryId::new(200, 0));
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path()).unwrap();

        // Add entries to two streams
        wal.log_xadd("s1", &StreamEntryId::new(1, 0), &[("a".into(), "1".into())])
            .unwrap();
        wal.log_xadd("s2", &StreamEntryId::new(2, 0), &[("b".into(), "2".into())])
            .unwrap();

        // Checkpoint with only s1
        let mut checkpoint_streams = HashMap::new();
        let mut s1 = Stream::new();
        s1.xadd_with_id(StreamEntryId::new(1, 0), vec![("a".into(), "1".into())]);
        checkpoint_streams.insert("s1".to_string(), s1);
        wal.checkpoint(&checkpoint_streams).unwrap();

        // Add new entry after checkpoint
        wal.log_xadd("s1", &StreamEntryId::new(3, 0), &[("c".into(), "3".into())])
            .unwrap();
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path()).unwrap();
        // s2 was dropped by checkpoint, s1 has 2 entries (snapshot + post-checkpoint)
        assert_eq!(state.streams.len(), 1);
        assert!(state.streams.contains_key("s1"));
        assert!(!state.streams.contains_key("s2"));
        assert_eq!(state.streams["s1"].len(), 2);
    }

    #[test]
    fn test_empty_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = StreamsWal::open(dir.path()).unwrap();
        assert!(state.streams.is_empty());
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("streams.wal");

        {
            let (wal, _) = StreamsWal::open(dir.path()).unwrap();
            wal.log_xadd(
                "good_stream",
                &StreamEntryId::new(42, 0),
                &[("k".into(), "v".into())],
            )
            .unwrap();
            drop(wal);
        }

        // Append garbage
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();
            f.flush().unwrap();
        }

        let (_wal, state) = StreamsWal::open(dir.path()).unwrap();
        assert_eq!(state.streams.len(), 1);
        assert!(state.streams.contains_key("good_stream"));
        assert_eq!(state.streams["good_stream"].len(), 1);
    }

    /// S31-03: a torn tail must be truncated on open, so that everything
    /// appended afterwards is replayable. Before the fix `open()` reopened in
    /// append mode without truncating, so every later record sat behind the
    /// garbage and was silently lost by every future replay — while `log_xadd`
    /// returned Ok and `group_sync` reported it durable.
    #[test]
    fn torn_tail_is_truncated_and_later_appends_survive() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("streams.wal");

        {
            let (wal, _) = StreamsWal::open(dir.path()).unwrap();
            wal.log_xadd("s", &StreamEntryId::new(1, 0), &[("k".into(), "a".into())])
                .unwrap();
            wal.group_sync().unwrap();
        }
        let clean_len = std::fs::metadata(&wal_path).unwrap().len();

        // Hand-build a torn tail: an XADD tag plus a truncated stream name.
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            let mut torn = vec![ENTRY_XADD];
            torn.extend_from_slice(&64u32.to_le_bytes()); // name_len, unbacked
            torn.extend_from_slice(b"tor");
            f.write_all(&torn).unwrap();
            f.flush().unwrap();
        }
        assert!(std::fs::metadata(&wal_path).unwrap().len() > clean_len);

        // Reopen: the torn bytes must be gone, and the good prefix intact.
        {
            let (wal, state) = StreamsWal::open(dir.path()).unwrap();
            assert_eq!(state.streams["s"].len(), 1);
            assert_eq!(
                std::fs::metadata(&wal_path).unwrap().len(),
                clean_len,
                "the torn tail must be truncated away on open"
            );
            // Append a good record behind where the garbage used to be.
            wal.log_xadd("s", &StreamEntryId::new(2, 0), &[("k".into(), "b".into())])
                .unwrap();
            wal.group_sync().unwrap();
        }

        // The record written after the torn tail must survive a reopen.
        let (_wal, state) = StreamsWal::open(dir.path()).unwrap();
        let entries = &state.streams["s"];
        assert_eq!(
            entries.len(),
            2,
            "a record appended after a torn tail must be recovered"
        );
        assert_eq!(entries[1].0, StreamEntryId::new(2, 0));
        assert_eq!(entries[1].1, vec![("k".to_string(), "b".to_string())]);
    }

    #[test]
    fn test_multiple_streams_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = StreamsWal::open(dir.path()).unwrap();

        for i in 0..5 {
            let name = format!("stream_{}", i);
            for j in 0..3 {
                wal.log_xadd(
                    &name,
                    &StreamEntryId::new(i * 100 + j, 0),
                    &[("idx".into(), format!("{}-{}", i, j))],
                )
                .unwrap();
            }
        }
        drop(wal);

        let (_wal2, state) = StreamsWal::open(dir.path()).unwrap();
        assert_eq!(state.streams.len(), 5);
        for i in 0..5 {
            let name = format!("stream_{}", i);
            assert_eq!(state.streams[&name].len(), 3);
        }
    }
}
