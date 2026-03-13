//! Write-ahead log for the MVCC storage adapter.
//!
//! Provides crash-safe durability by logging all mutations (DDL + DML)
//! as logical records.  On recovery, committed transactions are replayed
//! in order while aborted/in-flight transactions are skipped.
//!
//! ## Binary entry format
//! ```text
//! [record_len: u32 LE] [tag: u8] [payload ...] [crc32: u32 LE]
//! ```

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::types::{DataType, Value};

// ── Record tags ──────────────────────────────────────────────────────────────

const TAG_CREATE_TABLE: u8 = 0x01;
const TAG_DROP_TABLE: u8 = 0x02;
const TAG_INSERT: u8 = 0x03;
const TAG_DELETE: u8 = 0x04;
const TAG_UPDATE: u8 = 0x05;
const TAG_BEGIN: u8 = 0x10;
const TAG_COMMIT: u8 = 0x11;
const TAG_ABORT: u8 = 0x12;
const TAG_CHECKPOINT: u8 = 0x20;

// ── Public API ───────────────────────────────────────────────────────────────

/// A logical WAL record for the MVCC engine.
#[derive(Debug, Clone)]
pub enum MvccWalRecord {
    CreateTable {
        name: String,
        columns: Vec<(String, DataType)>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        txn_id: u64,
        row: Vec<Value>,
    },
    Delete {
        table: String,
        txn_id: u64,
        row_idx: u32,
    },
    Update {
        table: String,
        txn_id: u64,
        row_idx: u32,
        new_row: Vec<Value>,
    },
    Begin {
        txn_id: u64,
    },
    Commit {
        txn_id: u64,
    },
    Abort {
        txn_id: u64,
    },
    Checkpoint,
}

/// State recovered from replaying the MVCC WAL.
#[derive(Debug, Default)]
pub struct MvccWalState {
    /// Recovered tables: table_name → (columns, rows).
    pub tables: HashMap<String, RecoveredTable>,
}

/// A recovered table with its schema and committed rows.
#[derive(Debug, Clone)]
pub struct RecoveredTable {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Vec<Value>>,
}

/// Append-only WAL for MVCC durability.
pub struct MvccWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl MvccWal {
    /// Open or create the WAL file.  Returns (wal, recovered_state).
    pub fn open(dir: &Path) -> io::Result<(Self, MvccWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("mvcc.wal");
        let state = if path.exists() {
            let mut data = Vec::new();
            File::open(&path)?.read_to_end(&mut data)?;
            replay(&data)
        } else {
            MvccWalState::default()
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

    /// Log a record and flush to OS buffer.
    pub fn log(&self, record: &MvccWalRecord) -> io::Result<()> {
        let payload = encode_record(record);
        let crc = crc32c(&payload);
        let len = payload.len() as u32;
        let mut w = self.writer.lock();
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&payload)?;
        w.write_all(&crc.to_le_bytes())?;
        w.flush()
    }

    /// Fsync the WAL file to ensure durability.
    pub fn sync(&self) -> io::Result<()> {
        let mut w = self.writer.lock();
        w.flush()?;
        w.get_ref().sync_all()
    }

    /// Log a COMMIT and immediately fsync.
    pub fn log_commit(&self, txn_id: u64) -> io::Result<()> {
        self.log(&MvccWalRecord::Commit { txn_id })?;
        self.sync()
    }

    /// Truncate the WAL (after a full snapshot has been written).
    pub fn truncate(&self) -> io::Result<()> {
        let mut w = self.writer.lock();
        w.flush()?;
        drop(w);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
        // Re-open in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
        Ok(())
    }
}

// ── Encoding ─────────────────────────────────────────────────────────────────

fn encode_record(rec: &MvccWalRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    match rec {
        MvccWalRecord::CreateTable { name, columns } => {
            buf.push(TAG_CREATE_TABLE);
            write_str(&mut buf, name);
            write_u32(&mut buf, columns.len() as u32);
            for (col_name, col_type) in columns {
                write_str(&mut buf, col_name);
                write_u8(&mut buf, datatype_to_u8(col_type));
            }
        }
        MvccWalRecord::DropTable { name } => {
            buf.push(TAG_DROP_TABLE);
            write_str(&mut buf, name);
        }
        MvccWalRecord::Insert { table, txn_id, row } => {
            buf.push(TAG_INSERT);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_row(&mut buf, row);
        }
        MvccWalRecord::Delete { table, txn_id, row_idx } => {
            buf.push(TAG_DELETE);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u32(&mut buf, *row_idx);
        }
        MvccWalRecord::Update { table, txn_id, row_idx, new_row } => {
            buf.push(TAG_UPDATE);
            write_str(&mut buf, table);
            write_u64(&mut buf, *txn_id);
            write_u32(&mut buf, *row_idx);
            write_row(&mut buf, new_row);
        }
        MvccWalRecord::Begin { txn_id } => {
            buf.push(TAG_BEGIN);
            write_u64(&mut buf, *txn_id);
        }
        MvccWalRecord::Commit { txn_id } => {
            buf.push(TAG_COMMIT);
            write_u64(&mut buf, *txn_id);
        }
        MvccWalRecord::Abort { txn_id } => {
            buf.push(TAG_ABORT);
            write_u64(&mut buf, *txn_id);
        }
        MvccWalRecord::Checkpoint => {
            buf.push(TAG_CHECKPOINT);
        }
    }
    buf
}

// ── Value encoding ───────────────────────────────────────────────────────────

const VAL_NULL: u8 = 0;
const VAL_BOOL: u8 = 1;
const VAL_INT32: u8 = 2;
const VAL_INT64: u8 = 3;
const VAL_FLOAT64: u8 = 4;
const VAL_TEXT: u8 = 5;
const VAL_BYTEA: u8 = 6;
const VAL_DATE: u8 = 7;
const VAL_TIMESTAMP: u8 = 8;
const VAL_TIMESTAMPTZ: u8 = 9;
const VAL_NUMERIC: u8 = 10;
const VAL_UUID: u8 = 11;
const VAL_JSONB: u8 = 12;
const VAL_VECTOR: u8 = 13;
const VAL_INTERVAL: u8 = 14;
const VAL_ARRAY: u8 = 15;

fn write_value(buf: &mut Vec<u8>, val: &Value) {
    match val {
        Value::Null => buf.push(VAL_NULL),
        Value::Bool(b) => {
            buf.push(VAL_BOOL);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Int32(n) => {
            buf.push(VAL_INT32);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Int64(n) => {
            buf.push(VAL_INT64);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            buf.push(VAL_FLOAT64);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(VAL_TEXT);
            write_str(buf, s);
        }
        Value::Bytea(b) => {
            buf.push(VAL_BYTEA);
            write_u32(buf, b.len() as u32);
            buf.extend_from_slice(b);
        }
        Value::Date(d) => {
            buf.push(VAL_DATE);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::Timestamp(t) => {
            buf.push(VAL_TIMESTAMP);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::TimestampTz(t) => {
            buf.push(VAL_TIMESTAMPTZ);
            buf.extend_from_slice(&t.to_le_bytes());
        }
        Value::Numeric(s) => {
            buf.push(VAL_NUMERIC);
            write_str(buf, s);
        }
        Value::Uuid(bytes) => {
            buf.push(VAL_UUID);
            buf.extend_from_slice(bytes);
        }
        Value::Jsonb(j) => {
            buf.push(VAL_JSONB);
            write_str(buf, &j.to_string());
        }
        Value::Vector(v) => {
            buf.push(VAL_VECTOR);
            write_u32(buf, v.len() as u32);
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Interval { months, days, microseconds } => {
            buf.push(VAL_INTERVAL);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&microseconds.to_le_bytes());
        }
        Value::Array(arr) => {
            buf.push(VAL_ARRAY);
            write_u32(buf, arr.len() as u32);
            for v in arr {
                write_value(buf, v);
            }
        }
    }
}

fn read_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        VAL_NULL => Some(Value::Null),
        VAL_BOOL => {
            let b = *data.get(*pos)?;
            *pos += 1;
            Some(Value::Bool(b != 0))
        }
        VAL_INT32 => {
            let b = data.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(Value::Int32(i32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        }
        VAL_INT64 => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::Int64(i64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_FLOAT64 => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::Float64(f64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_TEXT => {
            let s = read_str(data, pos)?;
            Some(Value::Text(s))
        }
        VAL_BYTEA => {
            let len = read_u32_val(data, pos)? as usize;
            if *pos + len > data.len() { return None; }
            let b = data[*pos..*pos + len].to_vec();
            *pos += len;
            Some(Value::Bytea(b))
        }
        VAL_DATE => {
            let b = data.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(Value::Date(i32::from_le_bytes([b[0], b[1], b[2], b[3]])))
        }
        VAL_TIMESTAMP => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::Timestamp(i64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_TIMESTAMPTZ => {
            let b = data.get(*pos..*pos + 8)?;
            *pos += 8;
            Some(Value::TimestampTz(i64::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }
        VAL_NUMERIC => {
            let s = read_str(data, pos)?;
            Some(Value::Numeric(s))
        }
        VAL_UUID => {
            let b = data.get(*pos..*pos + 16)?;
            *pos += 16;
            let mut arr = [0u8; 16];
            arr.copy_from_slice(b);
            Some(Value::Uuid(arr))
        }
        VAL_JSONB => {
            let s = read_str(data, pos)?;
            let v: serde_json::Value = serde_json::from_str(&s).ok()?;
            Some(Value::Jsonb(v))
        }
        VAL_VECTOR => {
            let count = read_u32_val(data, pos)? as usize;
            let byte_len = count * 4;
            if *pos + byte_len > data.len() { return None; }
            let mut v = Vec::with_capacity(count);
            for _ in 0..count {
                let b = data.get(*pos..*pos + 4)?;
                *pos += 4;
                v.push(f32::from_le_bytes([b[0], b[1], b[2], b[3]]));
            }
            Some(Value::Vector(v))
        }
        VAL_INTERVAL => {
            let mb = data.get(*pos..*pos + 4)?;
            *pos += 4;
            let months = i32::from_le_bytes([mb[0], mb[1], mb[2], mb[3]]);
            let db = data.get(*pos..*pos + 4)?;
            *pos += 4;
            let days = i32::from_le_bytes([db[0], db[1], db[2], db[3]]);
            let ub = data.get(*pos..*pos + 8)?;
            *pos += 8;
            let microseconds = i64::from_le_bytes([
                ub[0], ub[1], ub[2], ub[3], ub[4], ub[5], ub[6], ub[7],
            ]);
            Some(Value::Interval { months, days, microseconds })
        }
        VAL_ARRAY => {
            let count = read_u32_val(data, pos)? as usize;
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                arr.push(read_value(data, pos)?);
            }
            Some(Value::Array(arr))
        }
        _ => None,
    }
}

fn write_row(buf: &mut Vec<u8>, row: &[Value]) {
    write_u32(buf, row.len() as u32);
    for val in row {
        write_value(buf, val);
    }
}

fn read_row(data: &[u8], pos: &mut usize) -> Option<Vec<Value>> {
    let count = read_u32_val(data, pos)? as usize;
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        row.push(read_value(data, pos)?);
    }
    Some(row)
}

// ── Primitive helpers ────────────────────────────────────────────────────────

fn write_u8(buf: &mut Vec<u8>, v: u8) { buf.push(v); }
fn write_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_u64(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    write_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

fn read_u32_val(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64_val(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32_val(data, pos)? as usize;
    if *pos + len > data.len() { return None; }
    let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
    *pos += len;
    Some(s)
}

fn datatype_to_u8(dt: &DataType) -> u8 {
    match dt {
        DataType::Bool => 0,
        DataType::Int32 => 1,
        DataType::Int64 => 2,
        DataType::Float64 => 3,
        DataType::Text => 4,
        DataType::Bytea => 5,
        DataType::Numeric => 6,
        DataType::Uuid => 7,
        DataType::Date => 8,
        DataType::Timestamp => 9,
        DataType::TimestampTz => 10,
        DataType::Interval => 11,
        DataType::Jsonb => 12,
        DataType::Vector(_) => 13,
        DataType::Array(_) => 14,
        DataType::UserDefined(_) => 15,
    }
}

fn u8_to_datatype(v: u8) -> DataType {
    match v {
        0 => DataType::Bool,
        1 => DataType::Int32,
        2 => DataType::Int64,
        3 => DataType::Float64,
        4 => DataType::Text,
        5 => DataType::Bytea,
        6 => DataType::Numeric,
        7 => DataType::Uuid,
        8 => DataType::Date,
        9 => DataType::Timestamp,
        10 => DataType::TimestampTz,
        11 => DataType::Interval,
        12 => DataType::Jsonb,
        13 => DataType::Vector(0),
        14 => DataType::Array(Box::new(DataType::Text)),
        15 => DataType::UserDefined(String::new()),
        _ => DataType::Text,
    }
}

/// Simple CRC32C (Castagnoli) for WAL record integrity.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ── Replay ───────────────────────────────────────────────────────────────────

/// Replay WAL data to recover committed state.
fn replay(data: &[u8]) -> MvccWalState {
    let mut pos = 0usize;
    let mut records: Vec<MvccWalRecord> = Vec::new();

    // Phase 1: Parse all records
    while pos + 4 <= data.len() {
        let len = u32::from_le_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
        ]) as usize;
        pos += 4;
        if pos + len + 4 > data.len() { break; } // truncated
        let payload = &data[pos..pos + len];
        pos += len;
        let stored_crc = u32::from_le_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
        ]);
        pos += 4;
        let computed_crc = crc32c(payload);
        if stored_crc != computed_crc { break; } // corrupt

        if let Some(rec) = decode_record(payload) {
            records.push(rec);
        } else {
            break;
        }
    }

    // Phase 2: Identify committed transactions
    let mut committed: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut aborted: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for rec in &records {
        match rec {
            MvccWalRecord::Commit { txn_id } => { committed.insert(*txn_id); }
            MvccWalRecord::Abort { txn_id } => { aborted.insert(*txn_id); }
            _ => {}
        }
    }

    // Phase 3: Replay committed operations (and auto-commits where txn_id=0)
    let mut tables: HashMap<String, RecoveredTable> = HashMap::new();

    for rec in &records {
        match rec {
            MvccWalRecord::CreateTable { name, columns } => {
                tables.insert(name.clone(), RecoveredTable {
                    columns: columns.clone(),
                    rows: Vec::new(),
                });
            }
            MvccWalRecord::DropTable { name } => {
                tables.remove(name);
            }
            MvccWalRecord::Insert { table, txn_id, row } => {
                // Auto-commit txns (id 0) or explicitly committed txns
                if (*txn_id == 0 || committed.contains(txn_id))
                    && let Some(tbl) = tables.get_mut(table) {
                        tbl.rows.push(row.clone());
                    }
            }
            MvccWalRecord::Delete { table, txn_id, row_idx } => {
                if (*txn_id == 0 || committed.contains(txn_id))
                    && let Some(tbl) = tables.get_mut(table) {
                        let idx = *row_idx as usize;
                        if idx < tbl.rows.len() {
                            // Mark deleted with tombstone (empty row) instead
                            // of Vec::remove() to avoid shifting indices that
                            // subsequent WAL records reference.
                            tbl.rows[idx] = Vec::new();
                        }
                    }
            }
            MvccWalRecord::Update { table, txn_id, row_idx, new_row } => {
                if (*txn_id == 0 || committed.contains(txn_id))
                    && let Some(tbl) = tables.get_mut(table) {
                        let idx = *row_idx as usize;
                        if idx < tbl.rows.len() {
                            tbl.rows[idx] = new_row.clone();
                        }
                    }
            }
            MvccWalRecord::Checkpoint => {
                // After a checkpoint, previous records can be ignored.
                // In a future version, truncate records before the checkpoint.
            }
            _ => {} // Begin, Commit, Abort handled above
        }
    }

    // Remove tombstone rows (empty Vec) left by DELETE replay.
    for tbl in tables.values_mut() {
        tbl.rows.retain(|row| !row.is_empty());
    }

    MvccWalState { tables }
}

fn decode_record(data: &[u8]) -> Option<MvccWalRecord> {
    let mut pos = 0usize;
    let tag = *data.get(pos)?;
    pos += 1;
    match tag {
        TAG_CREATE_TABLE => {
            let name = read_str(data, &mut pos)?;
            let count = read_u32_val(data, &mut pos)? as usize;
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                let col_name = read_str(data, &mut pos)?;
                let col_type = *data.get(pos)?;
                pos += 1;
                columns.push((col_name, u8_to_datatype(col_type)));
            }
            Some(MvccWalRecord::CreateTable { name, columns })
        }
        TAG_DROP_TABLE => {
            let name = read_str(data, &mut pos)?;
            Some(MvccWalRecord::DropTable { name })
        }
        TAG_INSERT => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let row = read_row(data, &mut pos)?;
            Some(MvccWalRecord::Insert { table, txn_id, row })
        }
        TAG_DELETE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let row_idx = read_u32_val(data, &mut pos)?;
            Some(MvccWalRecord::Delete { table, txn_id, row_idx })
        }
        TAG_UPDATE => {
            let table = read_str(data, &mut pos)?;
            let txn_id = read_u64_val(data, &mut pos)?;
            let row_idx = read_u32_val(data, &mut pos)?;
            let new_row = read_row(data, &mut pos)?;
            Some(MvccWalRecord::Update { table, txn_id, row_idx, new_row })
        }
        TAG_BEGIN => {
            let txn_id = read_u64_val(data, &mut pos)?;
            Some(MvccWalRecord::Begin { txn_id })
        }
        TAG_COMMIT => {
            let txn_id = read_u64_val(data, &mut pos)?;
            Some(MvccWalRecord::Commit { txn_id })
        }
        TAG_ABORT => {
            let txn_id = read_u64_val(data, &mut pos)?;
            Some(MvccWalRecord::Abort { txn_id })
        }
        TAG_CHECKPOINT => Some(MvccWalRecord::Checkpoint),
        _ => None,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_create_insert_commit() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Write records
        {
            let (wal, state) = MvccWal::open(dir.path()).unwrap();
            assert!(state.tables.is_empty());

            wal.log(&MvccWalRecord::CreateTable {
                name: "users".into(),
                columns: vec![
                    ("id".into(), DataType::Int64),
                    ("name".into(), DataType::Text),
                ],
            }).unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "users".into(),
                txn_id: 1,
                row: vec![Value::Int64(1), Value::Text("Alice".into())],
            }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "users".into(),
                txn_id: 1,
                row: vec![Value::Int64(2), Value::Text("Bob".into())],
            }).unwrap();
            wal.log_commit(1).unwrap();
            drop(wal);
        }

        // Phase 2: Recover
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let users = state.tables.get("users").unwrap();
        assert_eq!(users.rows.len(), 2);
        assert_eq!(users.rows[0][1], Value::Text("Alice".into()));
        assert_eq!(users.rows[1][1], Value::Text("Bob".into()));
    }

    #[test]
    fn test_aborted_txn_not_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            }).unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(), txn_id: 1,
                row: vec![Value::Int32(10)],
            }).unwrap();
            wal.log(&MvccWalRecord::Abort { txn_id: 1 }).unwrap();
            drop(wal);
        }

        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 0); // Aborted insert should NOT be present
    }

    #[test]
    fn test_uncommitted_txn_not_recovered() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            }).unwrap();
            // Begin but never commit/abort
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(), txn_id: 1,
                row: vec![Value::Int32(42)],
            }).unwrap();
            drop(wal);
        }

        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        let t = state.tables.get("t").unwrap();
        assert_eq!(t.rows.len(), 0); // In-flight insert should NOT be present
    }

    #[test]
    fn test_crc_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "t".into(),
                columns: vec![("x".into(), DataType::Int32)],
            }).unwrap();
            wal.log(&MvccWalRecord::Begin { txn_id: 1 }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "t".into(), txn_id: 1,
                row: vec![Value::Int32(99)],
            }).unwrap();
            wal.log_commit(1).unwrap();
            drop(wal);
        }

        // Corrupt a byte in the middle of the WAL
        {
            let path = dir.path().join("mvcc.wal");
            let mut data = std::fs::read(&path).unwrap();
            if data.len() > 20 {
                data[20] ^= 0xFF;
            }
            std::fs::write(&path, data).unwrap();
        }

        // Recover — should stop at corrupted record
        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        // Depending on which record was corrupted, table may or may not exist
        // but it should NOT panic
        let _ = state.tables.get("t");
    }

    #[test]
    fn test_drop_table_removes_data() {
        let dir = tempfile::tempdir().unwrap();

        {
            let (wal, _) = MvccWal::open(dir.path()).unwrap();
            wal.log(&MvccWalRecord::CreateTable {
                name: "temp".into(),
                columns: vec![("x".into(), DataType::Int32)],
            }).unwrap();
            wal.log(&MvccWalRecord::Insert {
                table: "temp".into(), txn_id: 0,
                row: vec![Value::Int32(1)],
            }).unwrap();
            wal.log(&MvccWalRecord::DropTable { name: "temp".into() }).unwrap();
            drop(wal);
        }

        let (_wal2, state) = MvccWal::open(dir.path()).unwrap();
        assert!(!state.tables.contains_key("temp"));
    }
}
