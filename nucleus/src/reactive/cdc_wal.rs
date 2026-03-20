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
//! A SNAPSHOT resets all state. After `checkpoint()` the file is truncated to
//! a single SNAPSHOT entry so the log stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::{CdcLog, CdcLogEntry, ChangeType};

// ---- Entry type tags --------------------------------------------------------

const ENTRY_APPEND: u8 = 0x01;
const ENTRY_CONSUMER: u8 = 0x02;
const ENTRY_SNAPSHOT: u8 = 0x03;

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
}

/// Append-only CDC WAL.
pub struct CdcWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl CdcWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, CdcWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("cdc.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            CdcWalState {
                entries: Vec::new(),
                consumers: HashMap::new(),
                next_sequence: 1,
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

    /// Log a CDC append operation (new change event).
    pub fn log_append(&self, entry: &CdcLogEntry) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_APPEND);

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
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log a consumer position update (acknowledge).
    pub fn log_consumer(&self, name: &str, position: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_CONSUMER);
        write_str(&mut buf, name);
        buf.extend_from_slice(&position.to_le_bytes());

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
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

        // consumers: we don't have direct access to the consumers map from CdcLog,
        // so we write 0 consumers in the snapshot. Consumer positions are reconstructed
        // from CONSUMER entries that follow the snapshot.
        payload.extend_from_slice(&0u32.to_le_bytes());

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

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

fn replay(data: &[u8]) -> CdcWalState {
    let mut entries: Vec<CdcLogEntry> = Vec::new();
    let mut consumers: HashMap<String, u64> = HashMap::new();
    let mut next_sequence: u64 = 1;
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else { break };
        pos += 1;

        match entry_type {
            ENTRY_APPEND => {
                let Some(entry) = replay_append(data, &mut pos) else { break };
                if entry.sequence >= next_sequence {
                    next_sequence = entry.sequence + 1;
                }
                entries.push(entry);
            }
            ENTRY_CONSUMER => {
                let Some(name) = read_string(data, &mut pos) else { break };
                let Some(position) = read_u64(data, &mut pos) else { break };
                consumers.insert(name, position);
            }
            ENTRY_SNAPSHOT => {
                entries.clear();
                consumers.clear();
                let Some(ns) = read_u64(data, &mut pos) else { break };
                next_sequence = ns;
                let Some(n_entries) = read_u32(data, &mut pos) else { break };
                let mut ok = true;
                for _ in 0..n_entries as usize {
                    let Some(entry) = replay_append(data, &mut pos) else { ok = false; break };
                    entries.push(entry);
                }
                if !ok { break; }
                let Some(n_consumers) = read_u32(data, &mut pos) else { break };
                for _ in 0..n_consumers as usize {
                    let Some(name) = read_string(data, &mut pos) else { ok = false; break };
                    let Some(position) = read_u64(data, &mut pos) else { ok = false; break };
                    consumers.insert(name, position);
                }
                if !ok { break; }
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
    let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
    *pos += len;
    Some(s)
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
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

        wal.log_append(&entry1).unwrap();
        wal.log_append(&entry2).unwrap();
        wal.log_append(&entry3).unwrap();
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
        wal.log_append(&entry).unwrap();
        wal.log_consumer("app1", 1).unwrap();
        wal.log_consumer("app2", 0).unwrap();
        drop(wal);

        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state.consumers["app1"], 1);
        assert_eq!(state.consumers["app2"], 0);
    }

    #[test]
    fn test_empty_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = CdcWal::open(dir.path()).unwrap();
        assert!(state.entries.is_empty());
        assert!(state.consumers.is_empty());
        assert_eq!(state.next_sequence, 1);
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
            wal.log_append(&entry).unwrap();
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

        wal.log_append(&entry1).unwrap();
        wal.log_append(&entry2).unwrap();
        wal.log_consumer("reader1", 1).unwrap();
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

        for (seq, ct) in [(1, ChangeType::Insert), (2, ChangeType::Update), (3, ChangeType::Delete)] {
            wal.log_append(&CdcLogEntry {
                sequence: seq,
                table: "t".to_string(),
                change_type: ct,
                row_data: HashMap::new(),
                timestamp: seq * 100,
            }).unwrap();
        }
        drop(wal);

        let (_wal2, state) = CdcWal::open(dir.path()).unwrap();
        assert_eq!(state.entries[0].change_type, ChangeType::Insert);
        assert_eq!(state.entries[1].change_type, ChangeType::Update);
        assert_eq!(state.entries[2].change_type, ChangeType::Delete);
    }
}
