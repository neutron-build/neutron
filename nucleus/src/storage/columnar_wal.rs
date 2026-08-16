//! Write-ahead log for the columnar storage engine.
//!
//! Provides crash-recovery by recording all table mutations to an append-only
//! log file (`columnar.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! [entry_type: u8]
//! [name_len: u32 LE]  [name_bytes: name_len]
//! [payload_len: u32 LE] [payload: payload_len]
//! ```
//!
//! ## Entry types
//! | Tag  | Name         | Payload                                        |
//! |------|--------------|------------------------------------------------|
//! | 0x01 | CREATE_TABLE | (empty)                                        |
//! | 0x02 | DROP_TABLE   | (empty)                                        |
//! | 0x03 | INSERT_ROWS  | n_rows(u32) + rows…                            |
//! | 0x04 | SNAPSHOT     | n_tables(u32) + (name_len + name + n_rows + rows…)… |
//!
//! A SNAPSHOT resets all table state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT entry so the log stays small.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::storage::wal::GroupCommitter;
use crate::types::{Row, Value};

// ─── Entry type tags ──────────────────────────────────────────────────────────

const ENTRY_CREATE_TABLE: u8 = 0x01;
const ENTRY_DROP_TABLE: u8 = 0x02;
const ENTRY_INSERT_ROWS: u8 = 0x03;
const ENTRY_SNAPSHOT: u8 = 0x04;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Recovered table state from a WAL replay.
pub struct WalState {
    /// `(table_name, rows)` pairs — order unspecified.
    pub tables: Vec<(String, Vec<Row>)>,
}

/// Append-only columnar WAL.
pub struct ColumnarWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Monotone append counter, incremented under the writer lock — serves as
    /// this WAL's LSN for durable-coverage tracking.
    appends: AtomicU64,
    /// Highest append-counter value covered by a COMPLETED fsync. Updated
    /// only after the fsync returns, so `synced >= mark` is a durable claim.
    synced: AtomicU64,
    /// Group-commit coordinator so concurrent commit-time syncs share fsyncs.
    committer: GroupCommitter,
}

impl ColumnarWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty (no tables). Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, WalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("columnar.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            WalState { tables: Vec::new() }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                appends: AtomicU64::new(0),
                synced: AtomicU64::new(0),
                committer: GroupCommitter::new(),
            },
            state,
        ))
    }

    /// Whether appends exist that no completed fsync covers yet.
    pub fn is_dirty(&self) -> bool {
        self.synced.load(Ordering::Acquire) < self.appends.load(Ordering::Acquire)
    }

    /// Fsync the log and record the append mark the sync covered.
    /// Returns that mark. The mark is captured under the writer lock, where
    /// appends also increment it, so every append at or below it is flushed
    /// and fsynced by this call.
    fn sync_covering(&self) -> io::Result<u64> {
        let mut w = self.writer.lock();
        let covered = self.appends.load(Ordering::Acquire);
        w.flush()?;
        w.get_ref().sync_all()?;
        self.synced.fetch_max(covered, Ordering::AcqRel);
        Ok(covered)
    }

    /// Fsync the log to stable storage. Appends only `write()` into the OS
    /// page cache; a commit ack requires this.
    pub fn sync(&self) -> io::Result<()> {
        self.sync_covering().map(|_| ())
    }

    /// Group-commit sync: concurrent committers share fsyncs, but each caller
    /// only returns once a completed sync covers every append made before
    /// this call.
    pub fn group_sync(&self) -> io::Result<()> {
        let mark = self.appends.load(Ordering::Acquire);
        if self.synced.load(Ordering::Acquire) >= mark {
            return Ok(());
        }
        self.committer.sync_up_to(mark, || self.sync_covering())
    }

    /// Log a CREATE TABLE operation.
    pub fn log_create_table(&self, table: &str) -> io::Result<()> {
        self.append(ENTRY_CREATE_TABLE, table, &[])
    }

    /// Log a DROP TABLE operation.
    pub fn log_drop_table(&self, table: &str) -> io::Result<()> {
        self.append(ENTRY_DROP_TABLE, table, &[])
    }

    /// Log a batch of newly inserted rows.
    pub fn log_insert_rows(&self, table: &str, rows: &[Row]) -> io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::new();
        payload.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for row in rows {
            encode_row(row, &mut payload);
        }
        self.append(ENTRY_INSERT_ROWS, table, &payload)
    }

    /// Write the complete current state of all tables as a single SNAPSHOT
    /// entry and truncate the log to just that entry.
    ///
    /// `tables` is a slice of `(table_name, all_rows)` covering every table
    /// that the engine currently knows about.
    pub fn checkpoint(&self, tables: &[(&str, Vec<Row>)]) -> io::Result<()> {
        // Build snapshot payload.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(tables.len() as u32).to_le_bytes());
        for (name, rows) in tables {
            let nb = name.as_bytes();
            payload.extend_from_slice(&(nb.len() as u32).to_le_bytes());
            payload.extend_from_slice(nb);
            payload.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for row in rows.iter() {
                encode_row(row, &mut payload);
            }
        }

        // Hold the writer lock across truncate + rewrite + swap so no append
        // can interleave: an entry appended between the truncate and the
        // writer swap would be destroyed without being in the snapshot.
        let mut writer = self.writer.lock();
        writer.flush()?;

        // Serialize the complete new log body, then replace atomically (temp file +
        // fsync + rename) so a crash between the truncate and the snapshot rewrite
        // can't leave a truncated or empty file.
        let mut contents: Vec<u8> = Vec::new();
        write_entry(&mut contents, ENTRY_SNAPSHOT, "", &payload)?;
        crate::storage::wal_util::atomic_replace_wal(&self.path, &contents)?;

        // Re-open in append mode for future writes, and count the snapshot
        // as a covered append so coverage marks stay consistent.
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *writer = BufWriter::new(file);
        let mark = self.appends.fetch_add(1, Ordering::AcqRel) + 1;
        self.synced.fetch_max(mark, Ordering::AcqRel);
        Ok(())
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    fn append(&self, entry_type: u8, name: &str, payload: &[u8]) -> io::Result<()> {
        let mut w = self.writer.lock();
        write_entry(&mut *w, entry_type, name, payload)?;
        w.flush()?;
        // Counted under the writer lock so sync_covering's mark is exact.
        self.appends.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

// ─── Binary encoding ──────────────────────────────────────────────────────────

fn write_entry<W: Write>(w: &mut W, entry_type: u8, name: &str, payload: &[u8]) -> io::Result<()> {
    let nb = name.as_bytes();
    w.write_all(&[entry_type])?;
    w.write_all(&(nb.len() as u32).to_le_bytes())?;
    w.write_all(nb)?;
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(payload)
}

fn encode_row(row: &Row, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(row.len() as u32).to_le_bytes());
    for val in row {
        encode_value(val, buf);
    }
}

fn encode_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(0),
        Value::Bool(b) => {
            buf.push(1);
            buf.push(*b as u8);
        }
        Value::Int32(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Int64(n) => {
            buf.push(3);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            buf.push(4);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(5);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Numeric(s) => {
            buf.push(10);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Date(d) => {
            buf.push(7);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(t) => {
            buf.push(8);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::TimestampTz(t) => {
            buf.push(9);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Interval {
            months,
            days,
            microseconds,
        } => {
            buf.push(11);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&microseconds.to_le_bytes());
        }
        other => {
            // Fallback: encode as Text (lossy for exotic types — sufficient for
            // columnar analytical workloads that don't use JSON/UUID/Array).
            let s = format!("{other}");
            let b = s.as_bytes();
            buf.push(5);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

// ─── Replay ───────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct table state.
///
/// SNAPSHOT entries reset all state to their embedded snapshot, so only the
/// *last* SNAPSHOT (and subsequent incremental entries) matter in practice.
fn replay(data: &[u8]) -> WalState {
    let mut tables: std::collections::HashMap<String, Vec<Row>> = std::collections::HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        // entry_type
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        // name
        let Some(name_len) = read_u32(data, &mut pos) else {
            break;
        };
        let name_len = name_len as usize;
        if pos + name_len > data.len() {
            break;
        }
        let name = match std::str::from_utf8(&data[pos..pos + name_len]) {
            Ok(s) => s.to_string(),
            Err(_) => break,
        };
        pos += name_len;

        // payload
        let Some(payload_len) = read_u32(data, &mut pos) else {
            break;
        };
        let payload_len = payload_len as usize;
        if pos + payload_len > data.len() {
            break;
        }
        let payload = &data[pos..pos + payload_len];
        pos += payload_len;

        match entry_type {
            ENTRY_CREATE_TABLE => {
                tables.entry(name).or_default();
            }
            ENTRY_DROP_TABLE => {
                tables.remove(&name);
            }
            ENTRY_INSERT_ROWS => {
                let rows = decode_rows(payload);
                tables.entry(name).or_default().extend(rows);
            }
            ENTRY_SNAPSHOT => {
                tables.clear();
                decode_snapshot_into(payload, &mut tables);
            }
            _ => {} // Unknown entry types are skipped.
        }
    }

    WalState {
        tables: tables.into_iter().collect(),
    }
}

fn decode_rows(data: &[u8]) -> Vec<Row> {
    let mut pos = 0;
    let n = match read_u32(data, &mut pos) {
        Some(n) => n as usize,
        None => return vec![],
    };
    let mut rows = Vec::with_capacity(super::wal_util::bounded_capacity(n));
    for _ in 0..n {
        match decode_row(data, &mut pos) {
            Some(r) => rows.push(r),
            None => break,
        }
    }
    rows
}

fn decode_snapshot_into(data: &[u8], tables: &mut std::collections::HashMap<String, Vec<Row>>) {
    let mut pos = 0;
    let n_tables = match read_u32(data, &mut pos) {
        Some(n) => n as usize,
        None => return,
    };
    for _ in 0..n_tables {
        // table name
        let name_len = match read_u32(data, &mut pos) {
            Some(n) => n as usize,
            None => return,
        };
        if pos + name_len > data.len() {
            return;
        }
        let name = match std::str::from_utf8(&data[pos..pos + name_len]) {
            Ok(s) => s.to_string(),
            Err(_) => return,
        };
        pos += name_len;
        // rows
        let n_rows = match read_u32(data, &mut pos) {
            Some(n) => n as usize,
            None => return,
        };
        let mut rows = Vec::with_capacity(n_rows);
        for _ in 0..n_rows {
            match decode_row(data, &mut pos) {
                Some(r) => rows.push(r),
                None => return,
            }
        }
        tables.insert(name, rows);
    }
}

fn decode_row(data: &[u8], pos: &mut usize) -> Option<Row> {
    let col_count = read_u32(data, pos)? as usize;
    let mut row = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        row.push(decode_value(data, pos)?);
    }
    Some(row)
}

fn decode_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        0 => Some(Value::Null),
        1 => {
            let b = *data.get(*pos)?;
            *pos += 1;
            Some(Value::Bool(b != 0))
        }
        2 => Some(Value::Int32(read_i32(data, pos)?)),
        3 => Some(Value::Int64(read_i64(data, pos)?)),
        4 => Some(Value::Float64(read_f64(data, pos)?)),
        5 => {
            let len = read_u32(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[*pos..*pos + len])
                .ok()?
                .to_string();
            *pos += len;
            Some(Value::Text(s))
        }
        7 => Some(Value::Date(read_i32(data, pos)?)),
        8 => Some(Value::Timestamp(read_i64(data, pos)?)),
        9 => Some(Value::TimestampTz(read_i64(data, pos)?)),
        10 => {
            let len = read_u32(data, pos)? as usize;
            if *pos + len > data.len() {
                return None;
            }
            let value = std::str::from_utf8(&data[*pos..*pos + len])
                .ok()?
                .to_string();
            *pos += len;
            Some(Value::Numeric(value))
        }
        11 => Some(Value::Interval {
            months: read_i32(data, pos)?,
            days: read_i32(data, pos)?,
            microseconds: read_i64(data, pos)?,
        }),
        _ => None, // Unknown tag — stop decoding row.
    }
}

// ─── Primitive readers ────────────────────────────────────────────────────────

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    read_u32(data, pos).map(|u| u as i32)
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(i64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_i64(data, pos).map(|v| f64::from_bits(v as u64))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn int_row(id: i64, v: f64) -> Row {
        vec![Value::Int64(id), Value::Float64(v)]
    }

    #[test]
    fn group_sync_covers_prior_appends_and_clears_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "fresh WAL must start clean");

        wal.log_create_table("t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
        assert!(wal.is_dirty(), "appends must mark the WAL dirty");

        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "a completed sync covers prior appends");

        wal.log_insert_rows("t", &[int_row(2, 2.0)]).unwrap();
        assert!(wal.is_dirty(), "new appends after a sync are uncovered");
    }

    #[test]
    fn checkpoint_counts_as_covered() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _state) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table("t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
        assert!(wal.is_dirty());
        wal.checkpoint(&[("t", vec![int_row(1, 1.0)])]).unwrap();
        assert!(
            !wal.is_dirty(),
            "checkpoint fsyncs the snapshot — nothing left to force"
        );
    }

    #[test]
    fn test_create_insert_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(state.tables.is_empty());

        wal.log_create_table("t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0), int_row(2, 2.0)])
            .unwrap();
        drop(wal);

        // Reopen — should see 2 rows.
        let (_wal2, state2) = ColumnarWal::open(dir.path()).unwrap();
        let t = state2.tables.iter().find(|(n, _)| n == "t").unwrap();
        assert_eq!(t.1.len(), 2);
        assert_eq!(t.1[0][0], Value::Int64(1));
    }

    #[test]
    fn test_drop_table_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table("t").unwrap();
        wal.log_insert_rows("t", &[int_row(1, 1.0)]).unwrap();
        wal.log_drop_table("t").unwrap();
        drop(wal);

        let (_wal2, state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(state.tables.iter().all(|(n, _)| n != "t"));
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table("t").unwrap();
        let five: Vec<Row> = (1..=5).map(|i| int_row(i, i as f64)).collect();
        wal.log_insert_rows("t", &five).unwrap();
        // Checkpoint with 5 rows.
        let rows: Vec<Row> = (1..=5).map(|i| int_row(i, i as f64)).collect();
        wal.checkpoint(&[("t", rows)]).unwrap();
        // Insert 2 more rows after checkpoint.
        wal.log_insert_rows("t", &[int_row(6, 6.0), int_row(7, 7.0)])
            .unwrap();
        drop(wal);

        let (_wal2, state) = ColumnarWal::open(dir.path()).unwrap();
        let t = state.tables.iter().find(|(n, _)| n == "t").unwrap();
        assert_eq!(t.1.len(), 7);
    }

    #[test]
    fn test_empty_wal_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = ColumnarWal::open(dir.path()).unwrap();
        assert!(state.tables.is_empty());
    }

    #[test]
    fn test_multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = ColumnarWal::open(dir.path()).unwrap();
        wal.log_create_table("a").unwrap();
        wal.log_create_table("b").unwrap();
        wal.log_insert_rows("a", &[int_row(1, 1.0), int_row(2, 2.0)])
            .unwrap();
        wal.log_insert_rows("b", &[int_row(10, 10.0)]).unwrap();
        drop(wal);

        let (_w, state) = ColumnarWal::open(dir.path()).unwrap();
        let a = state.tables.iter().find(|(n, _)| n == "a").unwrap();
        let b = state.tables.iter().find(|(n, _)| n == "b").unwrap();
        assert_eq!(a.1.len(), 2);
        assert_eq!(b.1.len(), 1);
    }
}
