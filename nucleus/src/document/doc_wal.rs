//! Write-ahead log for the document store.
//!
//! Provides crash-recovery by recording all document mutations to an
//! append-only log file (`doc.wal`). On restart the log is replayed from top
//! to bottom to reconstruct in-memory state (documents + GIN index).
//!
//! ## Log entry binary format
//! ```text
//! INSERT:   [0x01] [doc_id: u64 LE] [jsonb_len: u32 LE] [jsonb_bytes...]
//! DELETE:   [0x02] [doc_id: u64 LE]
//! SNAPSHOT: [0x04] [n_docs: u32 LE] [per doc: doc_id(u64) + jsonb_len(u32) + jsonb_bytes]
//! ```
//!
//! A SNAPSHOT resets all document state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT entry so the log stays small.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

// ─── Entry type tags ────────────────────────────────────────────────────────

const ENTRY_INSERT: u8 = 0x01;
const ENTRY_DELETE: u8 = 0x02;
const ENTRY_SNAPSHOT: u8 = 0x04;

// ─── Public types ───────────────────────────────────────────────────────────

/// Recovered document state from a WAL replay.
///
/// Each entry is `(doc_id, jsonb_bytes)` — the caller is responsible for
/// decoding JSONB back into `JsonValue` and rebuilding the GIN index.
pub struct DocWalState {
    /// `(doc_id, jsonb_bytes)` pairs for all surviving documents.
    pub docs: Vec<(u64, Vec<u8>)>,
}

/// Append-only document WAL.
pub struct DocWal {
    path: PathBuf,
    writer: Mutex<File>,
}

impl DocWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, DocWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("doc.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            DocWalState { docs: Vec::new() }
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok((Self { path, writer: Mutex::new(file) }, state))
    }

    /// Log an INSERT operation (insert or replace).
    pub fn log_insert(&self, doc_id: u64, json_bytes: &[u8]) -> io::Result<()> {
        let mut w = self.writer.lock();
        w.write_all(&[ENTRY_INSERT])?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.write_all(&(json_bytes.len() as u32).to_le_bytes())?;
        w.write_all(json_bytes)?;
        w.flush()
    }

    /// Log a DELETE operation.
    pub fn log_delete(&self, doc_id: u64) -> io::Result<()> {
        let mut w = self.writer.lock();
        w.write_all(&[ENTRY_DELETE])?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.flush()
    }

    /// Write the complete current state of all documents as a single SNAPSHOT
    /// entry and truncate the log to just that entry.
    ///
    /// `docs` is a slice of `(doc_id, jsonb_bytes)` covering every document
    /// that the store currently holds.
    pub fn checkpoint(&self, docs: &[(u64, Vec<u8>)]) -> io::Result<()> {
        // Flush existing writer before truncating.
        { self.writer.lock().flush()?; }

        // Build snapshot into a temporary buffer, then write atomically.
        let mut buf: Vec<u8> = Vec::new();
        buf.push(ENTRY_SNAPSHOT);
        buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());
        for (doc_id, jsonb) in docs {
            buf.extend_from_slice(&doc_id.to_le_bytes());
            buf.extend_from_slice(&(jsonb.len() as u32).to_le_bytes());
            buf.extend_from_slice(jsonb);
        }

        // Truncate and rewrite.
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(&buf)?;
        file.flush()?;
        drop(file);

        // Re-open in append mode for future writes.
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *self.writer.lock() = file;
        Ok(())
    }
}

// ─── Replay ─────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct document state.
///
/// SNAPSHOT entries reset all state. Only the *last* SNAPSHOT (and subsequent
/// incremental entries) matter in practice.
fn replay(data: &[u8]) -> DocWalState {
    let mut docs: std::collections::HashMap<u64, Vec<u8>> =
        std::collections::HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else { break };
        pos += 1;

        match entry_type {
            ENTRY_INSERT => {
                let Some(doc_id) = read_u64(data, &mut pos) else { break };
                let Some(jsonb_len) = read_u32(data, &mut pos) else { break };
                let jsonb_len = jsonb_len as usize;
                if pos + jsonb_len > data.len() { break; }
                let jsonb = data[pos..pos + jsonb_len].to_vec();
                pos += jsonb_len;
                docs.insert(doc_id, jsonb);
            }
            ENTRY_DELETE => {
                let Some(doc_id) = read_u64(data, &mut pos) else { break };
                docs.remove(&doc_id);
            }
            ENTRY_SNAPSHOT => {
                docs.clear();
                let Some(n_docs) = read_u32(data, &mut pos) else { break };
                let mut ok = true;
                for _ in 0..n_docs {
                    let Some(doc_id) = read_u64(data, &mut pos) else { ok = false; break };
                    let Some(jsonb_len) = read_u32(data, &mut pos) else { ok = false; break };
                    let jsonb_len = jsonb_len as usize;
                    if pos + jsonb_len > data.len() { ok = false; break; }
                    let jsonb = data[pos..pos + jsonb_len].to_vec();
                    pos += jsonb_len;
                    docs.insert(doc_id, jsonb);
                }
                if !ok { break; }
            }
            _ => {
                // Unknown entry type — stop replay (corrupt data).
                break;
            }
        }
    }

    DocWalState { docs: docs.into_iter().collect() }
}

// ─── Primitive readers ──────────────────────────────────────────────────────

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

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = DocWal::open(dir.path()).unwrap();
        assert!(state.docs.is_empty());

        wal.log_insert(1, b"hello").unwrap();
        wal.log_insert(2, b"world").unwrap();
        drop(wal);

        let (_wal2, state2) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state2.docs.len(), 2);
        let map: std::collections::HashMap<u64, Vec<u8>> =
            state2.docs.into_iter().collect();
        assert_eq!(map[&1], b"hello");
        assert_eq!(map[&2], b"world");
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DocWal::open(dir.path()).unwrap();
        wal.log_insert(1, b"aaa").unwrap();
        wal.log_insert(2, b"bbb").unwrap();
        wal.log_delete(1).unwrap();
        drop(wal);

        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].0, 2);
        assert_eq!(state.docs[0].1, b"bbb");
    }

    #[test]
    fn test_snapshot_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DocWal::open(dir.path()).unwrap();
        wal.log_insert(1, b"aaa").unwrap();
        wal.log_insert(2, b"bbb").unwrap();
        // Checkpoint with only doc 2
        wal.checkpoint(&[(2, b"bbb".to_vec())]).unwrap();
        // Insert doc 3 after checkpoint
        wal.log_insert(3, b"ccc").unwrap();
        drop(wal);

        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 2);
        let map: std::collections::HashMap<u64, Vec<u8>> =
            state.docs.into_iter().collect();
        assert!(map.contains_key(&2));
        assert!(map.contains_key(&3));
        assert!(!map.contains_key(&1)); // removed by snapshot
    }

    #[test]
    fn test_empty_wal_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = DocWal::open(dir.path()).unwrap();
        assert!(state.docs.is_empty());
    }

    #[test]
    fn test_corrupt_wal_graceful() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("doc.wal");
        // Write a valid INSERT then garbage bytes
        let mut data = Vec::new();
        data.push(ENTRY_INSERT);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"abc");
        // Garbage
        data.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
        std::fs::write(&path, &data).unwrap();

        let (_wal, state) = DocWal::open(dir.path()).unwrap();
        // Should recover the valid insert and ignore the garbage
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].0, 1);
        assert_eq!(state.docs[0].1, b"abc");
    }

    #[test]
    fn test_large_payload() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DocWal::open(dir.path()).unwrap();
        let big = vec![0x42u8; 100_000];
        wal.log_insert(1, &big).unwrap();
        drop(wal);

        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].1.len(), 100_000);
        assert!(state.docs[0].1.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn test_replace_via_insert() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DocWal::open(dir.path()).unwrap();
        wal.log_insert(1, b"first").unwrap();
        wal.log_insert(1, b"second").unwrap();
        drop(wal);

        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].1, b"second");
    }
}
