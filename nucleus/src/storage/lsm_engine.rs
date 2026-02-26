//! LSM-tree backed storage engine.
//!
//! Implements the StorageEngine trait using LsmTree as the key-value layer.
//! Each table gets its own LsmTree. Rows are stored as:
//!   key = big-endian u64 row-ID
//!   value = custom binary encoding of the row Value vector
//!
//! ## Modes
//! - `LsmStorageEngine::new()` — purely in-memory (SSTables are in-memory).
//! - `LsmStorageEngine::open(dir)` — disk-backed: each table has its own
//!   sub-directory under `dir` where SSTable `.sst` files are persisted.
//!   `flush_all_dirty` force-flushes all memtables, writing their contents
//!   to disk as SSTable files. On `open`, existing SSTable files are loaded.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::storage::lsm::{LsmConfig, LsmTree};
use crate::storage::{StorageEngine, StorageError};
use crate::types::{Row, Value};

const LSM_CONFIG: LsmConfig = LsmConfig {
    memtable_flush_threshold: 1024,
    level_max_sstables: 4,
    max_levels: 4,
    bloom_bits_per_key: 10,
};

struct LsmTable {
    tree: LsmTree,
    next_id: u64,
}

impl LsmTable {
    fn new() -> Self {
        Self { tree: LsmTree::new(LSM_CONFIG), next_id: 0 }
    }

    /// Open (or create) a disk-backed table in `dir`.
    fn open(dir: &Path) -> Result<Self, StorageError> {
        let tree = LsmTree::open(LSM_CONFIG, dir)
            .map_err(|e| StorageError::Io(e.to_string()))?;
        // Recover next_id from the maximum existing key + 1.
        let start = 0u64.to_be_bytes();
        let end = u64::MAX.to_be_bytes();
        let max_key = tree.range(&start, &end).last().map(|(k, _)| {
            let mut arr = [0u8; 8];
            let len = k.len().min(8);
            arr[..len].copy_from_slice(&k[..len]);
            u64::from_be_bytes(arr)
        });
        let next_id = max_key.map_or(0, |k| k + 1);
        Ok(Self { tree, next_id })
    }

    fn all_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = 0u64.to_be_bytes();
        let end = u64::MAX.to_be_bytes();
        self.tree.range(&start, &end)
    }
}

pub struct LsmStorageEngine {
    tables: RwLock<HashMap<String, LsmTable>>,
    /// Root directory for disk-backed mode. None = in-memory.
    disk_dir: Option<PathBuf>,
}

impl LsmStorageEngine {
    /// Create a purely in-memory LSM engine.
    pub fn new() -> Self {
        Self { tables: RwLock::new(HashMap::new()), disk_dir: None }
    }

    /// Open (or create) a disk-backed LSM engine in `dir`.
    ///
    /// Tables are stored in sub-directories: `<dir>/<table_name>/`.
    /// SSTable files in those directories are loaded on startup.
    pub fn open(dir: &Path) -> Result<Self, StorageError> {
        std::fs::create_dir_all(dir).map_err(|e| StorageError::Io(e.to_string()))?;
        let mut tables = HashMap::new();
        // Load any existing table directories.
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let table_name = match entry.file_name().into_string() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };
                    if let Ok(t) = LsmTable::open(&path) {
                        tables.insert(table_name, t);
                    }
                }
            }
        }
        Ok(Self { tables: RwLock::new(tables), disk_dir: Some(dir.to_path_buf()) })
    }

    fn table_dir(&self, table: &str) -> Option<PathBuf> {
        self.disk_dir.as_ref().map(|d| d.join(table))
    }
}

impl Default for LsmStorageEngine {
    fn default() -> Self { Self::new() }
}

fn encode_row(row: &Row) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(row.len() as u32).to_le_bytes());
    for val in row { encode_value(val, &mut buf); }
    buf
}

fn encode_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(0),
        Value::Bool(b) => { buf.push(1); buf.push(*b as u8); }
        Value::Int32(n) => { buf.push(2); buf.extend_from_slice(&n.to_le_bytes()); }
        Value::Int64(n) => { buf.push(3); buf.extend_from_slice(&n.to_le_bytes()); }
        Value::Float64(f) => { buf.push(4); buf.extend_from_slice(&f.to_le_bytes()); }
        Value::Text(s) => {
            buf.push(5);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Jsonb(v) => {
            buf.push(6);
            let s = v.to_string();
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Date(d) => { buf.push(7); buf.extend_from_slice(&d.to_le_bytes()); }
        Value::Timestamp(t) => { buf.push(8); buf.extend_from_slice(&t.to_le_bytes()); }
        Value::TimestampTz(t) => { buf.push(9); buf.extend_from_slice(&t.to_le_bytes()); }
        Value::Numeric(s) => {
            buf.push(10);
            let b = s.as_bytes();
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Uuid(bytes) => { buf.push(11); buf.extend_from_slice(bytes); }
        Value::Bytea(bytes) => {
            buf.push(12);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Array(arr) => {
            buf.push(13);
            buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
            for v in arr { encode_value(v, buf); }
        }
        Value::Vector(floats) => {
            buf.push(14);
            buf.extend_from_slice(&(floats.len() as u32).to_le_bytes());
            for f in floats { buf.extend_from_slice(&f.to_le_bytes()); }
        }
        Value::Interval { months, days, microseconds } => {
            buf.push(15);
            buf.extend_from_slice(&months.to_le_bytes());
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&microseconds.to_le_bytes());
        }
    }
}

fn decode_row(data: &[u8]) -> Option<Row> {
    let mut pos = 0;
    let col_count = read_u32(data, &mut pos)? as usize;
    let mut row = Vec::with_capacity(col_count);
    for _ in 0..col_count { row.push(decode_value(data, &mut pos)?); }
    Some(row)
}

fn decode_value(data: &[u8], pos: &mut usize) -> Option<Value> {
    let tag = *data.get(*pos)?;
    *pos += 1;
    match tag {
        0 => Some(Value::Null),
        1 => { let b = *data.get(*pos)?; *pos += 1; Some(Value::Bool(b != 0)) }
        2 => Some(Value::Int32(read_i32(data, pos)?)),
        3 => Some(Value::Int64(read_i64(data, pos)?)),
        4 => Some(Value::Float64(read_f64(data, pos)?)),
        5 => Some(Value::Text(read_string(data, pos)?)),
        6 => {
            let s = read_string(data, pos)?;
            let v: serde_json::Value = serde_json::from_str(&s).ok()?;
            Some(Value::Jsonb(v))
        }
        7 => Some(Value::Date(read_i32(data, pos)?)),
        8 => Some(Value::Timestamp(read_i64(data, pos)?)),
        9 => Some(Value::TimestampTz(read_i64(data, pos)?)),
        10 => Some(Value::Numeric(read_string(data, pos)?)),
        11 => {
            if *pos + 16 > data.len() { return None; }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data[*pos..*pos + 16]);
            *pos += 16;
            Some(Value::Uuid(bytes))
        }
        12 => {
            let len = read_u32(data, pos)? as usize;
            if *pos + len > data.len() { return None; }
            let bytes = data[*pos..*pos + len].to_vec();
            *pos += len;
            Some(Value::Bytea(bytes))
        }
        13 => {
            let count = read_u32(data, pos)? as usize;
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count { arr.push(decode_value(data, pos)?); }
            Some(Value::Array(arr))
        }
        14 => {
            let count = read_u32(data, pos)? as usize;
            let mut floats = Vec::with_capacity(count);
            for _ in 0..count {
                if *pos + 4 > data.len() { return None; }
                let f = f32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
                *pos += 4;
                floats.push(f);
            }
            Some(Value::Vector(floats))
        }
        15 => {
            let months = read_i32(data, pos)?;
            let days = read_i32(data, pos)?;
            let microseconds = read_i64(data, pos)?;
            Some(Value::Interval { months, days, microseconds })
        }
        _ => None,
    }
}

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 4 > data.len() { return None; }
    let v = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
    *pos += 4; Some(v)
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    if *pos + 4 > data.len() { return None; }
    let v = i32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
    *pos += 4; Some(v)
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    if *pos + 8 > data.len() { return None; }
    let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().ok()?);
    *pos += 8; Some(v)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    if *pos + 8 > data.len() { return None; }
    let v = f64::from_le_bytes(data[*pos..*pos + 8].try_into().ok()?);
    *pos += 8; Some(v)
}

fn read_string(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u32(data, pos)? as usize;
    if *pos + len > data.len() { return None; }
    let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
    *pos += len; Some(s)
}

#[async_trait]
impl StorageEngine for LsmStorageEngine {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        let mut tables = self.tables.write();
        if tables.contains_key(table) {
            return Ok(());
        }
        let lsm_table = if let Some(dir) = self.table_dir(table) {
            // Disk-backed: create table sub-directory.
            std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io(e.to_string()))?;
            LsmTable::open(&dir)?
        } else {
            LsmTable::new()
        };
        tables.insert(table.to_string(), lsm_table);
        Ok(())
    }

    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        self.tables.write().remove(table);
        // Remove disk directory for this table if disk-backed.
        if let Some(dir) = self.table_dir(table) {
            if dir.exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
        Ok(())
    }

    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        let mut tables = self.tables.write();
        let t = tables.get_mut(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        let key = t.next_id.to_be_bytes().to_vec();
        t.next_id += 1;
        t.tree.put(key, encode_row(&row));
        Ok(())
    }

    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        let tables = self.tables.read();
        let t = tables.get(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        let rows = t.all_entries().into_iter()
            .filter_map(|(_, v)| decode_row(&v))
            .collect();
        Ok(rows)
    }

    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        if positions.is_empty() { return Ok(0); }
        let mut tables = self.tables.write();
        let t = tables.get_mut(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        let pos_set: HashSet<usize> = positions.iter().copied().collect();
        let entries = t.all_entries();
        let mut count = 0;
        for (i, (key, _)) in entries.iter().enumerate() {
            if pos_set.contains(&i) {
                t.tree.delete(key.clone());
                count += 1;
            }
        }
        Ok(count)
    }

    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        if updates.is_empty() { return Ok(0); }
        let mut tables = self.tables.write();
        let t = tables.get_mut(table)
            .ok_or_else(|| StorageError::TableNotFound(table.to_string()))?;
        let entries = t.all_entries();
        let mut count = 0;
        for (pos, new_row) in updates {
            if let Some((key, _)) = entries.get(*pos) {
                t.tree.put(key.clone(), encode_row(new_row));
                count += 1;
            }
        }
        Ok(count)
    }

    async fn flush_all_dirty(&self) -> Result<(), StorageError> {
        // Force-flush all memtables to disk (writes pending entries as SSTable files).
        let mut tables = self.tables.write();
        for t in tables.values_mut() { t.tree.force_flush(); }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(id: i64, name: &str) -> Row {
        vec![Value::Int64(id), Value::Text(name.to_string())]
    }

    #[tokio::test]
    async fn test_lsm_create_insert_scan() {
        let engine = LsmStorageEngine::new();
        engine.create_table("t").await.unwrap();
        engine.insert("t", make_row(1, "alice")).await.unwrap();
        engine.insert("t", make_row(2, "bob")).await.unwrap();
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int64(1));
        assert_eq!(rows[1][1], Value::Text("bob".to_string()));
    }

    #[tokio::test]
    async fn test_lsm_delete() {
        let engine = LsmStorageEngine::new();
        engine.create_table("t").await.unwrap();
        engine.insert("t", make_row(1, "alice")).await.unwrap();
        engine.insert("t", make_row(2, "bob")).await.unwrap();
        engine.insert("t", make_row(3, "carol")).await.unwrap();
        let count = engine.delete("t", &[1]).await.unwrap();
        assert_eq!(count, 1);
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r[1] != Value::Text("bob".to_string())));
    }

    #[tokio::test]
    async fn test_lsm_update() {
        let engine = LsmStorageEngine::new();
        engine.create_table("t").await.unwrap();
        engine.insert("t", make_row(1, "alice")).await.unwrap();
        engine.insert("t", make_row(2, "bob")).await.unwrap();
        let count = engine.update("t", &[(0, make_row(99, "updated"))]).await.unwrap();
        assert_eq!(count, 1);
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows[0][0], Value::Int64(99));
        assert_eq!(rows[0][1], Value::Text("updated".to_string()));
    }

    #[tokio::test]
    async fn test_lsm_drop_table() {
        let engine = LsmStorageEngine::new();
        engine.create_table("t").await.unwrap();
        engine.insert("t", make_row(1, "alice")).await.unwrap();
        engine.drop_table("t").await.unwrap();
        let result = engine.scan("t").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lsm_value_roundtrip() {
        let row: Row = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int32(42),
            Value::Int64(i64::MAX),
            Value::Float64(3.14),
            Value::Text("hello".to_string()),
            Value::Date(12345),
            Value::Timestamp(9999999),
            Value::Bytea(vec![1, 2, 3]),
        ];
        let encoded = encode_row(&row);
        let decoded = decode_row(&encoded).unwrap();
        assert_eq!(row, decoded);
    }

    #[tokio::test]
    async fn test_lsm_flush_all_dirty() {
        let engine = LsmStorageEngine::new();
        engine.create_table("t").await.unwrap();
        for i in 0..2000i64 {
            engine.insert("t", make_row(i, "x")).await.unwrap();
        }
        engine.flush_all_dirty().await.unwrap();
        let rows = engine.scan("t").await.unwrap();
        assert_eq!(rows.len(), 2000);
    }

    // ─── Disk-backed LSM tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_lsm_disk_open_recover() {
        let dir = tempfile::tempdir().unwrap();

        // Session 1: insert rows and flush to disk.
        {
            let engine = LsmStorageEngine::open(dir.path()).unwrap();
            engine.create_table("orders").await.unwrap();
            for i in 1..=10i64 {
                engine.insert("orders", make_row(i, "alice")).await.unwrap();
            }
            engine.flush_all_dirty().await.unwrap(); // materialises SSTable files
        }

        // Session 2: reopen and verify rows are present.
        {
            let engine = LsmStorageEngine::open(dir.path()).unwrap();
            let rows = engine.scan("orders").await.unwrap();
            assert_eq!(rows.len(), 10, "expected 10 rows after disk recovery");
        }
    }

    #[tokio::test]
    async fn test_lsm_disk_multiple_tables() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = LsmStorageEngine::open(dir.path()).unwrap();
            engine.create_table("a").await.unwrap();
            engine.create_table("b").await.unwrap();
            engine.insert("a", make_row(1, "x")).await.unwrap();
            engine.insert("a", make_row(2, "y")).await.unwrap();
            engine.insert("b", make_row(10, "z")).await.unwrap();
            engine.flush_all_dirty().await.unwrap();
        }

        {
            let engine = LsmStorageEngine::open(dir.path()).unwrap();
            assert_eq!(engine.scan("a").await.unwrap().len(), 2);
            assert_eq!(engine.scan("b").await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn test_lsm_disk_drop_removes_files() {
        let dir = tempfile::tempdir().unwrap();

        {
            let engine = LsmStorageEngine::open(dir.path()).unwrap();
            engine.create_table("t").await.unwrap();
            engine.insert("t", make_row(1, "a")).await.unwrap();
            engine.flush_all_dirty().await.unwrap();
            engine.drop_table("t").await.unwrap();
        }

        // After drop, the table directory should be gone.
        let table_dir = dir.path().join("t");
        assert!(!table_dir.exists(), "table directory should be removed after drop");

        // Reopening should not see the table.
        {
            let engine = LsmStorageEngine::open(dir.path()).unwrap();
            assert!(engine.scan("t").await.is_err());
        }
    }
}
