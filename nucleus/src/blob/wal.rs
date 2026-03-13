//! Write-ahead log for the blob store.
//!
//! Provides crash-recovery by recording all blob mutations to an append-only
//! log file (`blob.wal`). On restart the log is replayed from top to bottom
//! to reconstruct in-memory state.
//!
//! ## Log entry binary format
//! ```text
//! STORE:    [0x01] [id_len: u32 LE] [id: bytes] [content_type_len: u32 LE] [content_type: bytes]
//!           [total_size: u64 LE] [n_chunks: u32 LE] [per chunk: hash(32 bytes) + chunk_len(u32) + chunk_data]
//! DELETE:   [0x02] [id_len: u32 LE] [id: bytes]
//! TAG:      [0x03] [id_len: u32 LE] [id: bytes] [key_len: u32 LE] [key: bytes] [val_len: u32 LE] [val: bytes]
//! SNAPSHOT: [0x04] [full blob store dump]
//! ```
//!
//! A SNAPSHOT resets all state. After `checkpoint()` the file is truncated to
//! a single SNAPSHOT entry so the log stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

// ---- Entry type tags --------------------------------------------------------

const ENTRY_STORE: u8 = 0x01;
const ENTRY_DELETE: u8 = 0x02;
const ENTRY_TAG: u8 = 0x03;
const ENTRY_SNAPSHOT: u8 = 0x04;

// ---- Public types -----------------------------------------------------------

/// A single blob's recovered state from WAL replay.
#[derive(Debug, Clone)]
pub struct BlobWalEntry {
    pub content_type: Option<String>,
    pub total_size: u64,
    /// `(blake3_hash_bytes, chunk_data)` in order.
    pub chunks: Vec<([u8; 32], Vec<u8>)>,
    pub tags: HashMap<String, String>,
}

/// Full recovered state from WAL replay.
pub struct BlobWalState {
    /// `blob_id -> recovered entry`.
    pub blobs: HashMap<String, BlobWalEntry>,
}

/// Snapshot data passed to `checkpoint()`.
#[allow(clippy::type_complexity)]
pub struct BlobStoreSnapshot<'a> {
    /// `(blob_id, content_type, total_size, chunks: &[(hash, data)], tags)`.
    pub blobs: Vec<(
        &'a str,
        Option<&'a str>,
        u64,
        Vec<(&'a [u8; 32], &'a [u8])>,
        Vec<(&'a str, &'a str)>,
    )>,
}

/// Append-only blob WAL.
pub struct BlobWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl BlobWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored (best-effort
    /// recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, BlobWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("blob.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            BlobWalState {
                blobs: HashMap::new(),
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

    /// Log a STORE operation (blob put).
    pub fn log_store(
        &self,
        id: &str,
        content_type: Option<&str>,
        total_size: u64,
        chunks: &[([u8; 32], Vec<u8>)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();

        // Entry type
        buf.push(ENTRY_STORE);

        // id
        write_str(&mut buf, id);

        // content_type (empty string = None)
        let ct = content_type.unwrap_or("");
        write_str(&mut buf, ct);

        // total_size
        buf.extend_from_slice(&total_size.to_le_bytes());

        // n_chunks
        buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());

        // per-chunk: hash(32) + chunk_len(u32) + chunk_data
        for (hash, data) in chunks {
            buf.extend_from_slice(hash);
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        }

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log a DELETE operation.
    pub fn log_delete(&self, id: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_DELETE);
        write_str(&mut buf, id);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Log a TAG operation.
    pub fn log_tag(&self, id: &str, key: &str, val: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_TAG);
        write_str(&mut buf, id);
        write_str(&mut buf, key);
        write_str(&mut buf, val);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()
    }

    /// Write a full snapshot and truncate the log to just that snapshot.
    pub fn checkpoint(&self, snapshot: &BlobStoreSnapshot<'_>) -> io::Result<()> {
        // Build snapshot payload
        let mut payload = Vec::new();

        // n_blobs
        payload.extend_from_slice(&(snapshot.blobs.len() as u32).to_le_bytes());

        for (id, content_type, total_size, chunks, tags) in &snapshot.blobs {
            // id
            write_str(&mut payload, id);

            // content_type
            let ct = content_type.unwrap_or("");
            write_str(&mut payload, ct);

            // total_size
            payload.extend_from_slice(&total_size.to_le_bytes());

            // n_chunks
            payload.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
            for (hash, data) in chunks {
                payload.extend_from_slice(*hash);
                payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
                payload.extend_from_slice(data);
            }

            // n_tags
            payload.extend_from_slice(&(tags.len() as u32).to_le_bytes());
            for (k, v) in tags {
                write_str(&mut payload, k);
                write_str(&mut payload, v);
            }
        }

        // Flush existing writer
        {
            self.writer.lock().flush()?;
        }

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

// ---- Binary encoding helpers ------------------------------------------------

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

/// Replay all entries in `data` to reconstruct blob state.
fn replay(data: &[u8]) -> BlobWalState {
    let mut blobs: HashMap<String, BlobWalEntry> = HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        match entry_type {
            ENTRY_STORE => {
                let Some(entry) = replay_store(data, &mut pos) else {
                    break;
                };
                blobs.insert(entry.0, entry.1);
            }
            ENTRY_DELETE => {
                let Some(id) = read_string(data, &mut pos) else {
                    break;
                };
                blobs.remove(&id);
            }
            ENTRY_TAG => {
                let Some(id) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(key) = read_string(data, &mut pos) else {
                    break;
                };
                let Some(val) = read_string(data, &mut pos) else {
                    break;
                };
                if let Some(entry) = blobs.get_mut(&id) {
                    entry.tags.insert(key, val);
                }
            }
            ENTRY_SNAPSHOT => {
                blobs.clear();
                if !replay_snapshot(data, &mut pos, &mut blobs) {
                    break;
                }
            }
            _ => {
                // Unknown entry type -- stop replay (corrupt data)
                break;
            }
        }
    }

    BlobWalState { blobs }
}

fn replay_store(data: &[u8], pos: &mut usize) -> Option<(String, BlobWalEntry)> {
    let id = read_string(data, pos)?;
    let ct_str = read_string(data, pos)?;
    let content_type = if ct_str.is_empty() {
        None
    } else {
        Some(ct_str)
    };
    let total_size = read_u64(data, pos)?;
    let n_chunks = read_u32(data, pos)? as usize;
    let mut chunks = Vec::with_capacity(n_chunks);
    for _ in 0..n_chunks {
        let hash = read_hash(data, pos)?;
        let chunk_len = read_u32(data, pos)? as usize;
        if *pos + chunk_len > data.len() {
            return None;
        }
        let chunk_data = data[*pos..*pos + chunk_len].to_vec();
        *pos += chunk_len;
        chunks.push((hash, chunk_data));
    }
    Some((
        id,
        BlobWalEntry {
            content_type,
            total_size,
            chunks,
            tags: HashMap::new(),
        },
    ))
}

fn replay_snapshot(
    data: &[u8],
    pos: &mut usize,
    blobs: &mut HashMap<String, BlobWalEntry>,
) -> bool {
    let Some(n_blobs) = read_u32(data, pos) else {
        return false;
    };
    for _ in 0..n_blobs as usize {
        let Some(id) = read_string(data, pos) else {
            return false;
        };
        let Some(ct_str) = read_string(data, pos) else {
            return false;
        };
        let content_type = if ct_str.is_empty() {
            None
        } else {
            Some(ct_str)
        };
        let Some(total_size) = read_u64(data, pos) else {
            return false;
        };
        let Some(n_chunks) = read_u32(data, pos) else {
            return false;
        };
        let mut chunks = Vec::with_capacity(n_chunks as usize);
        for _ in 0..n_chunks as usize {
            let Some(hash) = read_hash(data, pos) else {
                return false;
            };
            let Some(chunk_len) = read_u32(data, pos) else {
                return false;
            };
            let chunk_len = chunk_len as usize;
            if *pos + chunk_len > data.len() {
                return false;
            }
            let chunk_data = data[*pos..*pos + chunk_len].to_vec();
            *pos += chunk_len;
            chunks.push((hash, chunk_data));
        }
        let Some(n_tags) = read_u32(data, pos) else {
            return false;
        };
        let mut tags = HashMap::new();
        for _ in 0..n_tags as usize {
            let Some(k) = read_string(data, pos) else {
                return false;
            };
            let Some(v) = read_string(data, pos) else {
                return false;
            };
            tags.insert(k, v);
        }
        blobs.insert(
            id,
            BlobWalEntry {
                content_type,
                total_size,
                chunks,
                tags,
            },
        );
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

fn read_hash(data: &[u8], pos: &mut usize) -> Option<[u8; 32]> {
    let b = data.get(*pos..*pos + 32)?;
    *pos += 32;
    let mut hash = [0u8; 32];
    hash.copy_from_slice(b);
    Some(hash)
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
    fn test_store_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs.is_empty());

        let hash1 = blake3::hash(b"chunk1");
        let hash2 = blake3::hash(b"chunk2");
        wal.log_store(
            "blob1",
            Some("text/plain"),
            12,
            &[
                (*hash1.as_bytes(), b"chunk1".to_vec()),
                (*hash2.as_bytes(), b"chunk2".to_vec()),
            ],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state2) = BlobWal::open(dir.path()).unwrap();
        assert_eq!(state2.blobs.len(), 1);
        let entry = state2.blobs.get("blob1").unwrap();
        assert_eq!(entry.content_type.as_deref(), Some("text/plain"));
        assert_eq!(entry.total_size, 12);
        assert_eq!(entry.chunks.len(), 2);
        assert_eq!(entry.chunks[0].1, b"chunk1");
        assert_eq!(entry.chunks[1].1, b"chunk2");
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store(
            "blob1",
            None,
            4,
            &[(*hash.as_bytes(), b"data".to_vec())],
        )
        .unwrap();
        wal.log_delete("blob1").unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs.is_empty());
    }

    #[test]
    fn test_tag_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store(
            "blob1",
            Some("image/png"),
            4,
            &[(*hash.as_bytes(), b"data".to_vec())],
        )
        .unwrap();
        wal.log_tag("blob1", "author", "Alice").unwrap();
        wal.log_tag("blob1", "dept", "Eng").unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        let entry = state.blobs.get("blob1").unwrap();
        assert_eq!(entry.tags["author"], "Alice");
        assert_eq!(entry.tags["dept"], "Eng");
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        // Store two blobs
        let h1 = blake3::hash(b"aaa");
        let h2 = blake3::hash(b"bbb");
        wal.log_store("a", None, 3, &[(*h1.as_bytes(), b"aaa".to_vec())])
            .unwrap();
        wal.log_store("b", None, 3, &[(*h2.as_bytes(), b"bbb".to_vec())])
            .unwrap();

        // Checkpoint with only blob "a"
        let h1_bytes = *h1.as_bytes();
        let snapshot = BlobStoreSnapshot {
            blobs: vec![(
                "a",
                None,
                3,
                vec![(&h1_bytes, b"aaa".as_slice())],
                vec![("tag1", "val1")],
            )],
        };
        wal.checkpoint(&snapshot).unwrap();

        // Store another blob after checkpoint
        let h3 = blake3::hash(b"ccc");
        wal.log_store("c", None, 3, &[(*h3.as_bytes(), b"ccc".to_vec())])
            .unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        // "b" was dropped by checkpoint, "a" and "c" survive
        assert_eq!(state.blobs.len(), 2);
        assert!(state.blobs.contains_key("a"));
        assert!(state.blobs.contains_key("c"));
        assert!(!state.blobs.contains_key("b"));
        // Tag from snapshot
        assert_eq!(state.blobs["a"].tags["tag1"], "val1");
    }

    #[test]
    fn test_empty_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs.is_empty());
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("blob.wal");

        // Write a valid STORE entry followed by garbage bytes
        {
            let (wal, _) = BlobWal::open(dir.path()).unwrap();
            let hash = blake3::hash(b"good");
            wal.log_store(
                "good_blob",
                None,
                4,
                &[(*hash.as_bytes(), b"good".to_vec())],
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

        // Should recover the good blob, ignore the garbage
        let (_wal, state) = BlobWal::open(dir.path()).unwrap();
        assert_eq!(state.blobs.len(), 1);
        assert!(state.blobs.contains_key("good_blob"));
    }

    #[test]
    fn test_none_content_type() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store(
            "blob1",
            None,
            4,
            &[(*hash.as_bytes(), b"data".to_vec())],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs["blob1"].content_type.is_none());
    }

    #[test]
    fn test_overwrite_blob() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let h1 = blake3::hash(b"old");
        wal.log_store("key", Some("text/plain"), 3, &[(*h1.as_bytes(), b"old".to_vec())])
            .unwrap();

        let h2 = blake3::hash(b"new");
        wal.log_store("key", Some("text/html"), 3, &[(*h2.as_bytes(), b"new".to_vec())])
            .unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        assert_eq!(state.blobs.len(), 1);
        let entry = &state.blobs["key"];
        assert_eq!(entry.content_type.as_deref(), Some("text/html"));
        assert_eq!(entry.chunks[0].1, b"new");
    }
}
