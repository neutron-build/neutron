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

use crate::storage::wal_util::WalSync;

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
    /// Documents whose LAST event in the log was a removal.
    ///
    /// Needed because the log is now a TAIL applied on top of the
    /// `fts_index.json` checkpoint (NU-014), and a tail has to be able to
    /// express a deletion. Collapsing the log to "live documents" alone loses
    /// that: a doc removed in the tail but still present in the checkpoint
    /// would come back on every boot.
    pub removed: Vec<u64>,
}

/// Append-only FTS WAL.
pub struct FtsWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Append/sync bookkeeping for group commit. These appends previously
    /// ended at `BufWriter::flush`, which only moves bytes into the kernel --
    /// so an acknowledged FTS write survived `kill -9` but not power loss, and
    /// nothing in the code read as missing. NU-006.
    syncer: WalSync,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
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
            FtsWalState {
                docs: Vec::new(),
                removed: Vec::new(),
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: WalSync::new(),
                stranded: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
            },
            state,
        ))
    }

    /// Re-point the writer at the live log file after a checkpoint replaced
    /// the file but could not reopen it. While stranded, `writer` holds an
    /// UNLINKED inode — appends to it succeed into a file no future recovery
    /// reads — so this runs before every append: a successful reopen recovers
    /// the writer, and a failed one fails the append loudly instead of
    /// letting it acknowledge a write to a dead inode.
    fn reattach_if_stranded(&self, w: &mut BufWriter<File>) -> io::Result<()> {
        if !self.stranded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("fts.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "FTS WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
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

    /// Log an INDEX_DOC operation (store original text).
    pub fn log_index_doc(&self, doc_id: u64, text: &str) -> io::Result<()> {
        let text_bytes = text.as_bytes();
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&[ENTRY_INDEX_DOC])?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.write_all(&(text_bytes.len() as u32).to_le_bytes())?;
        w.write_all(text_bytes)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a REMOVE_DOC operation.
    pub fn log_remove_doc(&self, doc_id: u64) -> io::Result<()> {
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&[ENTRY_REMOVE_DOC])?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Re-read the WAL file to get the current (doc_id, text) pairs.
    /// Used by `InvertedIndex::checkpoint_wal` so it does not need to keep
    /// original texts in memory.
    pub fn read_current_docs(&self) -> io::Result<Vec<(u64, String)>> {
        self.writer.lock().flush()?;
        let data = std::fs::read(&self.path)?;
        let state = replay(&data);
        Ok(state.docs)
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
        // The reopen is the hazardous half: the rename above already unlinked
        // the inode `w` holds, so a failure here leaves the writer pointing at
        // a file no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            .then(|| io::Error::other("injected FTS WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("fts.wal_reopen") {
            Err(e)
        } else {
            OpenOptions::new().append(true).open(&self.path)
        };
        let file = match file {
            Ok(f) => f,
            Err(e) => {
                // The rename already happened, so the handle in `w` is now an
                // unlinked inode. Mark the writer stranded: appends must
                // reattach (or fail loudly), never write through it.
                self.stranded
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(e);
            }
        };
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
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
    let mut removed: std::collections::HashSet<u64> = std::collections::HashSet::new();
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
                removed.remove(&doc_id);
                docs.insert(doc_id, text);
            }
            ENTRY_REMOVE_DOC => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                docs.remove(&doc_id);
                removed.insert(doc_id);
            }
            ENTRY_SNAPSHOT => {
                docs.clear();
                removed.clear();
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
        removed: removed.into_iter().collect(),
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

    /// An append is un-fsynced until `group_sync` covers it. NU-006.
    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = FtsWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        wal.log_index_doc(1, "hello world").unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

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

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged document silently vanishes at restart. The discriminator
    /// is durability: the post-failure append must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = FtsWal::open(dir.path()).unwrap();
            wal.log_index_doc(1, "before").unwrap();
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&[(1, "before".to_string())])
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_index_doc(2, "after")
                .expect("a later append must reattach, not strand");
        }
        let (_wal2, state) = FtsWal::open(dir.path()).unwrap();
        let map: std::collections::HashMap<u64, String> = state.docs.into_iter().collect();
        assert_eq!(
            map.len(),
            2,
            "the post-checkpoint-failure append went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
        assert_eq!(map[&2], "after");
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
