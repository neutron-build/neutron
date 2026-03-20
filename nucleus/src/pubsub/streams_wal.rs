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

/// Recovered streams state from WAL replay.
pub struct StreamsWalState {
    /// `stream_name -> Vec<(entry_id, fields)>` in order.
    pub streams: HashMap<String, Vec<(StreamEntryId, Vec<(String, String)>)>>,
}

/// Append-only Streams WAL.
pub struct StreamsWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl StreamsWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, StreamsWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("streams.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            StreamsWalState {
                streams: HashMap::new(),
            }
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
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
        w.flush()
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

        // Flush existing writer
        { self.writer.lock().flush()?; }

        // Truncate and rewrite as single SNAPSHOT entry
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);
        w.write_all(&[ENTRY_SNAPSHOT])?;
        w.write_all(&payload)?;
        w.flush()?;
        drop(w);

        // Re-open in append mode for future writes
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
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

fn replay(data: &[u8]) -> StreamsWalState {
    let mut streams: HashMap<String, Vec<(StreamEntryId, Vec<(String, String)>)>> = HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        match entry_type {
            ENTRY_XADD => {
                let Some(stream_name) = read_string(data, &mut pos) else { break };
                let Some(ms) = read_u64(data, &mut pos) else { break };
                let Some(seq) = read_u64(data, &mut pos) else { break };
                let Some(n_fields) = read_u32(data, &mut pos) else { break };
                let mut fields = Vec::with_capacity(n_fields as usize);
                let mut ok = true;
                for _ in 0..n_fields {
                    let Some(k) = read_string(data, &mut pos) else { ok = false; break };
                    let Some(v) = read_string(data, &mut pos) else { ok = false; break };
                    fields.push((k, v));
                }
                if !ok { break; }
                streams
                    .entry(stream_name)
                    .or_default()
                    .push((StreamEntryId::new(ms, seq), fields));
            }
            ENTRY_SNAPSHOT => {
                streams.clear();
                if !replay_snapshot(data, &mut pos, &mut streams) {
                    break;
                }
            }
            _ => {
                break;
            }
        }
    }

    StreamsWalState { streams }
}

fn replay_snapshot(
    data: &[u8],
    pos: &mut usize,
    streams: &mut HashMap<String, Vec<(StreamEntryId, Vec<(String, String)>)>>,
) -> bool {
    let Some(n_streams) = read_u32(data, pos) else { return false };
    for _ in 0..n_streams as usize {
        let Some(name) = read_string(data, pos) else { return false };
        let Some(n_entries) = read_u32(data, pos) else { return false };
        let mut entries = Vec::with_capacity(n_entries as usize);
        for _ in 0..n_entries as usize {
            let Some(ms) = read_u64(data, pos) else { return false };
            let Some(seq) = read_u64(data, pos) else { return false };
            let Some(n_fields) = read_u32(data, pos) else { return false };
            let mut fields = Vec::with_capacity(n_fields as usize);
            for _ in 0..n_fields as usize {
                let Some(k) = read_string(data, pos) else { return false };
                let Some(v) = read_string(data, pos) else { return false };
                fields.push((k, v));
            }
            entries.push((StreamEntryId::new(ms, seq), fields));
        }
        streams.insert(name, entries);
    }
    true
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
    let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
    *pos += len;
    Some(s)
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xadd_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = StreamsWal::open(dir.path()).unwrap();
        assert!(state.streams.is_empty());

        wal.log_xadd(
            "events",
            &StreamEntryId::new(1000, 0),
            &[("user".into(), "alice".into()), ("action".into(), "login".into())],
        )
        .unwrap();
        wal.log_xadd(
            "events",
            &StreamEntryId::new(1001, 0),
            &[("user".into(), "bob".into()), ("action".into(), "logout".into())],
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
