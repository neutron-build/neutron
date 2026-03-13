//! Write-ahead log for the FTS inverted index.
//!
//! Provides crash-recovery by recording all index/remove operations to an
//! append-only log file (`fts.wal`). On restart the log is replayed from top
//! to bottom, re-tokenizing and re-indexing every document to reconstruct the
//! in-memory `InvertedIndex`.
//!
//! ## Design
//! The WAL stores **original text** (not tokenized/indexed form). On replay,
//! each document is re-tokenized and re-indexed via `InvertedIndex::add_document`.
//! This ensures consistency even if the tokenizer/stemmer changes between restarts.
//!
//! ## Log entry binary format
//! ```text
//! INDEX_DOC:  [0x01] [doc_id: u64 LE] [text_len: u32 LE] [text: UTF-8 bytes]
//! REMOVE_DOC: [0x02] [doc_id: u64 LE]
//! SNAPSHOT:   [0x04] [n_docs: u32 LE] [per doc: doc_id(u64) + text_len(u32) + text_bytes]
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

// ─── Entry type tags ──────────────────────────────────────────────────────────

const ENTRY_INDEX_DOC: u8 = 0x01;
const ENTRY_REMOVE_DOC: u8 = 0x02;
const ENTRY_SNAPSHOT: u8 = 0x04;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Recovered state from an FTS WAL replay.
///
/// Contains `(doc_id, original_text)` pairs. The caller is responsible for
/// re-indexing each document into an `InvertedIndex`.
pub struct FtsWalState {
    /// `(doc_id, original_text)` pairs for every live document.
    pub docs: Vec<(u64, String)>,
}

/// Append-only FTS WAL.
pub struct FtsWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl std::fmt::Debug for FtsWal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsWal")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl FtsWal {
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, FtsWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("fts.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data)
        } else {
            FtsWalState { docs: Vec::new() }
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

    /// Log an INDEX_DOC operation (store original text).
    pub fn log_index_doc(&self, doc_id: u64, text: &str) -> io::Result<()> {
        let text_bytes = text.as_bytes();
        let mut w = self.writer.lock();
        w.write_all(&[ENTRY_INDEX_DOC])?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.write_all(&(text_bytes.len() as u32).to_le_bytes())?;
        w.write_all(text_bytes)?;
        w.flush()
    }

    /// Log a REMOVE_DOC operation.
    pub fn log_remove_doc(&self, doc_id: u64) -> io::Result<()> {
        let mut w = self.writer.lock();
        w.write_all(&[ENTRY_REMOVE_DOC])?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.flush()
    }

    /// Write the complete current state of all documents as a single SNAPSHOT
    /// entry and truncate the log to just that entry.
    ///
    /// `docs` is a slice of `(doc_id, original_text)` covering every document
    /// currently in the index.
    pub fn checkpoint(&self, docs: &[(u64, String)]) -> io::Result<()> {
        // Build snapshot payload.
        let mut payload = Vec::new();
        payload.extend_from_slice(&(docs.len() as u32).to_le_bytes());
        for (doc_id, text) in docs {
            payload.extend_from_slice(&doc_id.to_le_bytes());
            let tb = text.as_bytes();
            payload.extend_from_slice(&(tb.len() as u32).to_le_bytes());
            payload.extend_from_slice(tb);
        }

        // Flush existing writer, then truncate file and rewrite as one entry.
        {
            self.writer.lock().flush()?;
        }

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);
        // Write SNAPSHOT header: tag + payload
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

// ─── Replay ───────────────────────────────────────────────────────────────────

/// Replay all entries in `data` to reconstruct document state.
///
/// SNAPSHOT entries reset all state. Only the *last* SNAPSHOT (and subsequent
/// incremental entries) matter in practice.
fn replay(data: &[u8]) -> FtsWalState {
    let mut docs: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        match entry_type {
            ENTRY_INDEX_DOC => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(text_len) = read_u32(data, &mut pos) else {
                    break;
                };
                let text_len = text_len as usize;
                if pos + text_len > data.len() {
                    break;
                }
                let text = match std::str::from_utf8(&data[pos..pos + text_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => break,
                };
                pos += text_len;
                docs.insert(doc_id, text);
            }
            ENTRY_REMOVE_DOC => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                docs.remove(&doc_id);
            }
            ENTRY_SNAPSHOT => {
                docs.clear();
                let Some(n_docs) = read_u32(data, &mut pos) else {
                    break;
                };
                let mut ok = true;
                for _ in 0..n_docs {
                    let Some(doc_id) = read_u64(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(text_len) = read_u32(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let text_len = text_len as usize;
                    if pos + text_len > data.len() {
                        ok = false;
                        break;
                    }
                    let text = match std::str::from_utf8(&data[pos..pos + text_len]) {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            ok = false;
                            break;
                        }
                    };
                    pos += text_len;
                    docs.insert(doc_id, text);
                }
                if !ok {
                    break;
                }
            }
            _ => {
                // Unknown entry type — stop replay (corrupt data).
                break;
            }
        }
    }

    FtsWalState {
        docs: docs.into_iter().collect(),
    }
}

// ─── Primitive readers ────────────────────────────────────────────────────────

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

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_doc_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = FtsWal::open(dir.path()).unwrap();
        assert!(state.docs.is_empty());

        wal.log_index_doc(1, "hello world").unwrap();
        wal.log_index_doc(2, "rust programming").unwrap();
        drop(wal);

        let (_wal2, state2) = FtsWal::open(dir.path()).unwrap();
        assert_eq!(state2.docs.len(), 2);
        let map: std::collections::HashMap<u64, String> = state2.docs.into_iter().collect();
        assert_eq!(map[&1], "hello world");
        assert_eq!(map[&2], "rust programming");
    }

    #[test]
    fn test_remove_doc_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = FtsWal::open(dir.path()).unwrap();
        wal.log_index_doc(1, "hello").unwrap();
        wal.log_index_doc(2, "world").unwrap();
        wal.log_remove_doc(1).unwrap();
        drop(wal);

        let (_wal2, state) = FtsWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].0, 2);
        assert_eq!(state.docs[0].1, "world");
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = FtsWal::open(dir.path()).unwrap();
        // Write a bunch of incremental entries.
        for i in 1..=10 {
            wal.log_index_doc(i, &format!("doc {i}")).unwrap();
        }
        // Checkpoint with only 5 docs (simulating some were removed).
        let snapshot: Vec<(u64, String)> = (1..=5).map(|i| (i, format!("doc {i}"))).collect();
        wal.checkpoint(&snapshot).unwrap();
        // Add 2 more after checkpoint.
        wal.log_index_doc(11, "post-checkpoint doc").unwrap();
        wal.log_index_doc(12, "another post-checkpoint").unwrap();
        drop(wal);

        let (_wal2, state) = FtsWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 7); // 5 from snapshot + 2 incremental
    }

    #[test]
    fn test_empty_wal_open() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, state) = FtsWal::open(dir.path()).unwrap();
        assert!(state.docs.is_empty());
    }

    #[test]
    fn test_corrupt_wal_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fts.wal");

        // Write a valid entry followed by garbage.
        {
            let (wal, _) = FtsWal::open(dir.path()).unwrap();
            wal.log_index_doc(1, "valid document").unwrap();
            drop(wal);
        }

        // Append corrupt bytes.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();
            f.flush().unwrap();
        }

        // Should recover the valid entry and ignore the garbage.
        let (_wal, state) = FtsWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].1, "valid document");
    }

    #[test]
    fn test_overwrite_same_doc_id() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = FtsWal::open(dir.path()).unwrap();
        wal.log_index_doc(1, "version one").unwrap();
        wal.log_index_doc(1, "version two").unwrap();
        drop(wal);

        let (_wal2, state) = FtsWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        let map: std::collections::HashMap<u64, String> = state.docs.into_iter().collect();
        assert_eq!(map[&1], "version two");
    }

    #[test]
    fn test_snapshot_resets_state() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = FtsWal::open(dir.path()).unwrap();
        wal.log_index_doc(1, "alpha").unwrap();
        wal.log_index_doc(2, "beta").unwrap();
        // Snapshot only includes doc 2.
        wal.checkpoint(&[(2, "beta".to_string())]).unwrap();
        drop(wal);

        let (_wal2, state) = FtsWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].0, 2);
    }
}
