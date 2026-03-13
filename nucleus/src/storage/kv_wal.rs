//! Write-ahead log for the KV store.
//!
//! Provides crash-recovery by recording all KV mutations to an append-only
//! log file (`kv.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! SET:    [0x01] [key_len: u32 LE] [key: bytes] [value_len: u32 LE] [value_encoded: bytes]
//! DEL:    [0x02] [key_len: u32 LE] [key: bytes]
//! EXPIRE: [0x03] [key_len: u32 LE] [key: bytes] [ttl_ms: u64 LE]
//! SNAP:   [0x04] [n_items: u32 LE] [per item: key_len + key + value_len + value + has_ttl(u8) + ttl_ms(u64)]
//! ```
//!
//! A SNAPSHOT resets all KV state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT entry so the log stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::types::Value;

// ─── Entry type tags ──────────────────────────────────────────────────────────

const ENTRY_SET: u8 = 0x01;
const ENTRY_DEL: u8 = 0x02;
const ENTRY_EXPIRE: u8 = 0x03;
const ENTRY_SNAPSHOT: u8 = 0x04;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Operation type for batch WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvWalOp {
    Set,
    Delete,
    Expire,
}

/// Recovered KV state from a WAL replay.
///
/// Each item is `(key, value, optional_ttl_absolute_ms)` where the TTL is
/// milliseconds since the Unix epoch (not a duration).
pub struct KvWalState {
    pub items: Vec<(String, Value, Option<u64>)>,
}

/// Append-only KV WAL.
pub struct KvWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl KvWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, KvWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("kv.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            KvWalState { items: Vec::new() }
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok((Self { path, writer: Mutex::new(BufWriter::new(file)) }, state))
    }

    /// Log a SET operation (key + value, no TTL change).
    pub fn log_set(&self, key: &str, val: &Value) -> io::Result<()> {
        let mut buf = Vec::new();
        // entry tag
        buf.push(ENTRY_SET);
        // key
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        // value
        let mut val_buf = Vec::new();
        encode_value(val, &mut val_buf);
        buf.extend_from_slice(&(val_buf.len() as u32).to_le_bytes());
        buf.extend_from_slice(&val_buf);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log a DEL operation.
    pub fn log_delete(&self, key: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_DEL);
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log an EXPIRE operation (absolute TTL in milliseconds since epoch).
    pub fn log_expire(&self, key: &str, ttl_ms: u64) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_EXPIRE);
        let kb = key.as_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&ttl_ms.to_le_bytes());

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log multiple operations in a single `write_all` + `flush` call.
    ///
    /// Each entry is `(op, key, optional_value, optional_ttl_abs_ms)`.
    /// This avoids per-entry syscall overhead for burst workloads like MSET.
    pub fn log_batch(&self, entries: &[(KvWalOp, &str, Option<&Value>, Option<u64>)]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for (op, key, val, ttl_ms) in entries {
            let kb = key.as_bytes();
            match op {
                KvWalOp::Set => {
                    let value = val.expect("log_batch: SET requires a value");
                    buf.push(ENTRY_SET);
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                    let mut val_buf = Vec::new();
                    encode_value(value, &mut val_buf);
                    buf.extend_from_slice(&(val_buf.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&val_buf);
                }
                KvWalOp::Delete => {
                    buf.push(ENTRY_DEL);
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                }
                KvWalOp::Expire => {
                    let ms = ttl_ms.expect("log_batch: EXPIRE requires a ttl_ms");
                    buf.push(ENTRY_EXPIRE);
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(kb);
                    buf.extend_from_slice(&ms.to_le_bytes());
                }
            }
        }
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Write the complete current state as a single SNAPSHOT entry and
    /// truncate the log to just that entry.
    ///
    /// `items` is a slice of `(key, value, optional_ttl_absolute_ms)`.
    pub fn checkpoint(&self, items: &[(String, Value, Option<u64>)]) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(items.len() as u32).to_le_bytes());
        for (key, val, ttl) in items {
            // key
            let kb = key.as_bytes();
            payload.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            payload.extend_from_slice(kb);
            // value
            let mut val_buf = Vec::new();
            encode_value(val, &mut val_buf);
            payload.extend_from_slice(&(val_buf.len() as u32).to_le_bytes());
            payload.extend_from_slice(&val_buf);
            // TTL
            match ttl {
                Some(ms) => {
                    payload.push(1u8);
                    payload.extend_from_slice(&ms.to_le_bytes());
                }
                None => {
                    payload.push(0u8);
                    payload.extend_from_slice(&0u64.to_le_bytes());
                }
            }
        }

        // Flush existing writer, then truncate file and rewrite as one entry.
        { self.writer.lock().flush()?; }

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);
        // SNAPSHOT has no key prefix — write tag + payload directly
        w.write_all(&[ENTRY_SNAPSHOT])?;
        w.write_all(&payload)?;
        w.flush()?;
        drop(w);

        // Re-open in append mode for future writes.
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
        Ok(())
    }
}

// ─── Value encoding ──────────────────────────────────────────────────────────
//
// Tag-based scheme:
//   0=Null, 1=Bool(u8), 2=Int32(i32 LE), 3=Int64(i64 LE),
//   4=Float64(f64 LE), 5=Text(len u32 + bytes)

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
        other => {
            // Fallback: encode as Text (lossy for exotic types — sufficient
            // for typical KV workloads).
            let s = format!("{other}");
            let b = s.as_bytes();
            buf.push(5);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
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
            let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
            *pos += len;
            Some(Value::Text(s))
        }
        _ => None,
    }
}

// ─── Replay ───────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct KV state.
///
/// SNAPSHOT entries reset all state to their embedded snapshot, so only the
/// *last* SNAPSHOT (and subsequent incremental entries) matter in practice.
fn replay(data: &[u8]) -> KvWalState {
    // key -> (value, optional_ttl_absolute_ms)
    let mut store: HashMap<String, (Value, Option<u64>)> = HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else { break };
        pos += 1;

        match entry_type {
            ENTRY_SET => {
                // key
                let Some(key) = read_string(data, &mut pos) else { break };
                // value (length-prefixed)
                let Some(val_len) = read_u32(data, &mut pos) else { break };
                let val_len = val_len as usize;
                if pos + val_len > data.len() { break; }
                let mut vpos = pos;
                let Some(val) = decode_value(data, &mut vpos) else { break; };
                pos += val_len;
                // Preserve existing TTL if key already exists
                let ttl = store.get(&key).and_then(|(_, t)| *t);
                store.insert(key, (val, ttl));
            }
            ENTRY_DEL => {
                let Some(key) = read_string(data, &mut pos) else { break };
                store.remove(&key);
            }
            ENTRY_EXPIRE => {
                let Some(key) = read_string(data, &mut pos) else { break };
                let Some(ttl_ms) = read_u64(data, &mut pos) else { break };
                if let Some(entry) = store.get_mut(&key) {
                    entry.1 = Some(ttl_ms);
                }
            }
            ENTRY_SNAPSHOT => {
                store.clear();
                let Some(n_items) = read_u32(data, &mut pos) else { break };
                let mut ok = true;
                for _ in 0..n_items {
                    // key
                    let Some(key) = read_string(data, &mut pos) else { ok = false; break };
                    // value (length-prefixed)
                    let Some(val_len) = read_u32(data, &mut pos) else { ok = false; break };
                    let val_len = val_len as usize;
                    if pos + val_len > data.len() { ok = false; break; }
                    let mut vpos = pos;
                    let Some(val) = decode_value(data, &mut vpos) else { ok = false; break };
                    pos += val_len;
                    // TTL
                    let Some(&has_ttl) = data.get(pos) else { ok = false; break };
                    pos += 1;
                    let Some(ttl_ms) = read_u64(data, &mut pos) else { ok = false; break };
                    let ttl = if has_ttl != 0 { Some(ttl_ms) } else { None };
                    store.insert(key, (val, ttl));
                }
                if !ok { break; }
            }
            _ => {
                // Unknown entry type — stop replay (can't know how much to skip).
                break;
            }
        }
    }

    KvWalState {
        items: store.into_iter().map(|(k, (v, t))| (k, v, t)).collect(),
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

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

fn read_i64(data: &[u8], pos: &mut usize) -> Option<i64> {
    read_u64(data, pos).map(|v| v as i64)
}

fn read_f64(data: &[u8], pos: &mut usize) -> Option<f64> {
    read_u64(data, pos).map(f64::from_bits)
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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = KvWal::open(dir.path()).unwrap();
        assert!(state.items.is_empty());

        wal.log_set("name", &Value::Text("Nucleus".into())).unwrap();
        wal.log_set("count", &Value::Int64(42)).unwrap();
        drop(wal);

        let (_wal2, state2) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state2.items.len(), 2);
        let name = state2.items.iter().find(|(k, _, _)| k == "name").unwrap();
        assert_eq!(name.1, Value::Text("Nucleus".into()));
        let count = state2.items.iter().find(|(k, _, _)| k == "count").unwrap();
        assert_eq!(count.1, Value::Int64(42));
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("a", &Value::Int32(1)).unwrap();
        wal.log_set("b", &Value::Int32(2)).unwrap();
        wal.log_delete("a").unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert!(state.items.iter().all(|(k, _, _)| k != "a"));
        let b = state.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.1, Value::Int32(2));
    }

    #[test]
    fn test_set_with_ttl_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("temp", &Value::Text("ephemeral".into())).unwrap();
        let ttl_ms = 1_700_000_000_000u64; // some future epoch ms
        wal.log_expire("temp", ttl_ms).unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        let item = &state.items[0];
        assert_eq!(item.0, "temp");
        assert_eq!(item.1, Value::Text("ephemeral".into()));
        assert_eq!(item.2, Some(ttl_ms));
    }

    #[test]
    fn test_checkpoint_truncates() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        // Write many entries
        for i in 0..100 {
            wal.log_set(&format!("k{i}"), &Value::Int64(i)).unwrap();
        }
        let size_before = std::fs::metadata(dir.path().join("kv.wal")).unwrap().len();

        // Checkpoint with only 3 items
        let items = vec![
            ("a".to_string(), Value::Int32(1), None),
            ("b".to_string(), Value::Int32(2), Some(9_999_999_999_999u64)),
            ("c".to_string(), Value::Text("hello".into()), None),
        ];
        wal.checkpoint(&items).unwrap();
        let size_after = std::fs::metadata(dir.path().join("kv.wal")).unwrap().len();
        assert!(size_after < size_before, "checkpoint should shrink WAL");

        // Can still write after checkpoint
        wal.log_set("d", &Value::Int64(4)).unwrap();
        drop(wal);

        // Verify replay
        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 4);
        let b = state.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.2, Some(9_999_999_999_999u64));
        let d = state.items.iter().find(|(k, _, _)| k == "d").unwrap();
        assert_eq!(d.1, Value::Int64(4));
    }

    #[test]
    fn test_corrupt_trailing_bytes_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("good", &Value::Int32(42)).unwrap();
        drop(wal);

        // Append garbage bytes to the WAL file
        let wal_path = dir.path().join("kv.wal");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD, 0x00, 0x01]).unwrap();
        file.flush().unwrap();
        drop(file);

        // Should recover the good entry and skip the corrupt tail
        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert_eq!(state.items[0].0, "good");
        assert_eq!(state.items[0].1, Value::Int32(42));
    }

    #[test]
    fn test_incr_survives_restart() {
        // Simulate INCR by logging successive SET operations (INCR is a
        // read-modify-write that the KvStore turns into a SET at the WAL level).
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("counter", &Value::Int64(1)).unwrap();
        wal.log_set("counter", &Value::Int64(2)).unwrap();
        wal.log_set("counter", &Value::Int64(3)).unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        let counter = state.items.iter().find(|(k, _, _)| k == "counter").unwrap();
        assert_eq!(counter.1, Value::Int64(3));
    }

    #[test]
    fn test_empty_wal_fresh_store() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = KvWal::open(dir.path()).unwrap();
        assert!(state.items.is_empty());
    }

    #[test]
    fn test_value_types_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("null", &Value::Null).unwrap();
        wal.log_set("bool", &Value::Bool(true)).unwrap();
        wal.log_set("i32", &Value::Int32(-42)).unwrap();
        wal.log_set("i64", &Value::Int64(i64::MAX)).unwrap();
        wal.log_set("f64", &Value::Float64(3.14159)).unwrap();
        wal.log_set("text", &Value::Text("hello world".into())).unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 6);

        let find = |k: &str| state.items.iter().find(|(key, _, _)| key == k).unwrap().1.clone();
        assert_eq!(find("null"), Value::Null);
        assert_eq!(find("bool"), Value::Bool(true));
        assert_eq!(find("i32"), Value::Int32(-42));
        assert_eq!(find("i64"), Value::Int64(i64::MAX));
        assert_eq!(find("f64"), Value::Float64(3.14159));
        assert_eq!(find("text"), Value::Text("hello world".into()));
    }

    #[test]
    fn test_overwrite_preserves_ttl() {
        // When a SET replays over an existing key, it should preserve the TTL
        // (the WAL logs SET and EXPIRE separately).
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("k", &Value::Int32(1)).unwrap();
        wal.log_expire("k", 5_000_000_000_000u64).unwrap();
        wal.log_set("k", &Value::Int32(2)).unwrap(); // overwrite value, keep TTL
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        let item = state.items.iter().find(|(k, _, _)| k == "k").unwrap();
        assert_eq!(item.1, Value::Int32(2));
        assert_eq!(item.2, Some(5_000_000_000_000u64));
    }

    #[test]
    fn test_snapshot_then_incremental() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        // Checkpoint with initial data
        let items = vec![
            ("x".to_string(), Value::Int64(10), None),
            ("y".to_string(), Value::Int64(20), Some(8_000_000_000_000u64)),
        ];
        wal.checkpoint(&items).unwrap();

        // Incremental ops after checkpoint
        wal.log_set("z", &Value::Int64(30)).unwrap();
        wal.log_delete("x").unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 2); // y and z remain
        assert!(state.items.iter().all(|(k, _, _)| k != "x"));
        let y = state.items.iter().find(|(k, _, _)| k == "y").unwrap();
        assert_eq!(y.1, Value::Int64(20));
        assert_eq!(y.2, Some(8_000_000_000_000u64));
        let z = state.items.iter().find(|(k, _, _)| k == "z").unwrap();
        assert_eq!(z.1, Value::Int64(30));
    }

    #[test]
    fn test_batch_set_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = KvWal::open(dir.path()).unwrap();
        assert!(state.items.is_empty());

        let v1 = Value::Text("hello".into());
        let v2 = Value::Int64(42);
        let v3 = Value::Float64(3.14);
        wal.log_batch(&[
            (KvWalOp::Set, "a", Some(&v1), None),
            (KvWalOp::Set, "b", Some(&v2), None),
            (KvWalOp::Set, "c", Some(&v3), None),
        ]).unwrap();
        drop(wal);

        let (_wal2, state2) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state2.items.len(), 3);
        let a = state2.items.iter().find(|(k, _, _)| k == "a").unwrap();
        assert_eq!(a.1, Value::Text("hello".into()));
        let b = state2.items.iter().find(|(k, _, _)| k == "b").unwrap();
        assert_eq!(b.1, Value::Int64(42));
        let c = state2.items.iter().find(|(k, _, _)| k == "c").unwrap();
        assert_eq!(c.1, Value::Float64(3.14));
    }

    #[test]
    fn test_batch_mixed_ops_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        // First set some keys individually
        wal.log_set("x", &Value::Int32(1)).unwrap();
        wal.log_set("y", &Value::Int32(2)).unwrap();
        wal.log_set("z", &Value::Int32(3)).unwrap();

        // Now use a batch with mixed ops: delete x, expire y, set w
        let w_val = Value::Int64(99);
        wal.log_batch(&[
            (KvWalOp::Delete, "x", None, None),
            (KvWalOp::Expire, "y", None, Some(9_000_000_000_000u64)),
            (KvWalOp::Set, "w", Some(&w_val), None),
        ]).unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        // x deleted, y still present with TTL, z unchanged, w added
        assert_eq!(state.items.len(), 3);
        assert!(state.items.iter().all(|(k, _, _)| k != "x"));
        let y = state.items.iter().find(|(k, _, _)| k == "y").unwrap();
        assert_eq!(y.1, Value::Int32(2));
        assert_eq!(y.2, Some(9_000_000_000_000u64));
        let z = state.items.iter().find(|(k, _, _)| k == "z").unwrap();
        assert_eq!(z.1, Value::Int32(3));
        let w = state.items.iter().find(|(k, _, _)| k == "w").unwrap();
        assert_eq!(w.1, Value::Int64(99));
    }

    #[test]
    fn test_batch_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = KvWal::open(dir.path()).unwrap();

        wal.log_set("pre", &Value::Int32(1)).unwrap();
        // Empty batch should be a no-op
        wal.log_batch(&[]).unwrap();
        drop(wal);

        let (_wal2, state) = KvWal::open(dir.path()).unwrap();
        assert_eq!(state.items.len(), 1);
        assert_eq!(state.items[0].0, "pre");
    }
}
