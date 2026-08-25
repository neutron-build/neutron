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
//! INSERT_C: [0x05] [doc_id: u64 LE] [coll_len: u32 LE] [coll_bytes...]
//!                  [jsonb_len: u32 LE] [jsonb_bytes...]
//! SNAP_C:   [0x06] [n_docs: u32 LE] [per doc: doc_id(u64) + coll_len(u32) +
//!                  coll_bytes + jsonb_len(u32) + jsonb_bytes]
//! ```
//!
//! A SNAPSHOT resets all document state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT entry so the log stays small.
//!
//! The `_C` variants carry the document's collection. They are separate entry
//! types rather than a change to `0x01`/`0x04` so a log written by an older
//! build still replays: its documents simply belong to the default (unnamed)
//! collection, which is what they were. A document in the default collection is
//! still logged with the original entry types, so a log only grows the new
//! shapes once collections are actually used.
//!
//! ## Transaction-tagged records (S63)
//!
//! Tags `0x07`-`0x09` are the `_XACT` twins of the three mutation records,
//! each carrying the coordinating transaction id (`u64 LE`) between the tag
//! and the twin's body. Replay keeps a tagged record only if its id is
//! `XACT_AUTOCOMMIT` (0 — written outside any explicit transaction, whose
//! durability point is this log's own fsync) or appears in the committed set
//! recovered from the SQL side; everything else was written inside a
//! transaction that never committed and is discarded — absence of a commit
//! record means discard, always. The untagged tags keep their
//! keep-unconditionally meaning, so pre-S63 logs replay unchanged. A SNAPSHOT
//! (either variant) is committed by construction (the S7 checkpoint gate keeps
//! one from folding an open transaction's writes) and always replays.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::executor::enlistment::XACT_AUTOCOMMIT;
use crate::storage::wal_util::WalSync;

// ─── Entry type tags ────────────────────────────────────────────────────────

const ENTRY_INSERT: u8 = 0x01;
const ENTRY_DELETE: u8 = 0x02;
const ENTRY_SNAPSHOT: u8 = 0x04;
const ENTRY_INSERT_COLL: u8 = 0x05;
const ENTRY_SNAPSHOT_COLL: u8 = 0x06;
/// S63: INSERT carrying the coordinating transaction id. Body after the id is
/// byte-identical to [`ENTRY_INSERT`].
const ENTRY_INSERT_XACT: u8 = 0x07;
/// S63: collection-scoped INSERT carrying the coordinating transaction id.
const ENTRY_INSERT_COLL_XACT: u8 = 0x08;
/// S63: DELETE carrying the coordinating transaction id.
const ENTRY_DELETE_XACT: u8 = 0x09;

// ─── Public types ───────────────────────────────────────────────────────────

/// Recovered document state from a WAL replay.
///
/// Each entry is `(doc_id, jsonb_bytes)` — the caller is responsible for
/// decoding JSONB back into `JsonValue` and rebuilding the GIN index.
pub struct DocWalState {
    /// `(doc_id, jsonb_bytes)` pairs for all surviving documents.
    pub docs: Vec<(u64, Vec<u8>)>,
    /// `(doc_id, collection)` for documents in a NAMED collection. Documents in
    /// the default (unnamed) collection are absent, so a log from a build that
    /// predates collections yields an empty map — every document defaults, which
    /// is exactly where they were.
    pub collections: Vec<(u64, String)>,
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63): a reopened process
    /// must never mint an id that a surviving tagged record already carries,
    /// or the recovery filter could resurrect stale records by matching a
    /// fresh transaction against them.
    pub max_xact_id: u64,
}

/// Append-only document WAL.
pub struct DocWal {
    path: PathBuf,
    writer: Mutex<File>,
    /// The highest coordinating transaction id recovered at open (S63).
    max_xact_id: u64,
    /// Append/sync bookkeeping for group commit. Before this existed the
    /// appends below ended in `Write::flush`, which for a bare `std::fs::File`
    /// is documented to do nothing at all -- so an acknowledged document write
    /// lived only in the kernel page cache and did not survive power loss,
    /// while reading exactly like a durable write. NU-006.
    syncer: WalSync,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no future recovery
    /// reads while `group_sync`/`is_dirty` report healthy. Set when a
    /// checkpoint replaced the log but its reopen failed; cleared by the next
    /// successful reattach (or checkpoint reopen). See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint_in`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
}

impl DocWal {
    /// Open or create the WAL file in `dir`, replaying with an EMPTY
    /// committed set so every tagged record keeps — the pre-S63 contract.
    /// The executor opens through [`DocWal::open_with_committed`] instead,
    /// passing the coordinating transaction ids the SQL side durably
    /// committed so the S63 replay filter can discard the rest.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, DocWalState)> {
        Self::open_with_committed(dir, &HashSet::new())
    }

    /// Open or create the WAL file in `dir` whose replay is filtered by the
    /// S63 committed set: a tagged record whose coordinating transaction id
    /// is neither `XACT_AUTOCOMMIT` nor in `committed` was written inside a
    /// transaction that never committed, and is discarded.
    pub fn open_with_committed(
        dir: &Path,
        committed: &HashSet<u64>,
    ) -> io::Result<(Self, DocWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("doc.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay(&data, committed)
        } else {
            DocWalState {
                docs: Vec::new(),
                collections: Vec::new(),
                max_xact_id: 0,
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let max_xact_id = state.max_xact_id;
        Ok((
            Self {
                path,
                writer: Mutex::new(file),
                syncer: WalSync::new(),
                max_xact_id,
                stranded: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
            },
            state,
        ))
    }

    /// The highest coordinating transaction id this log recovered (S63), 0
    /// when it holds none. Seeds the executor's XactId counter so a reopened
    /// process never mints an id a surviving tagged record already carries.
    pub fn max_xact_id(&self) -> u64 {
        self.max_xact_id
    }

    /// Re-point the writer at the live log file after a checkpoint replaced
    /// the file but could not reopen it. While stranded, `writer` holds an
    /// UNLINKED inode — appends to it succeed into a file no future recovery
    /// reads — so this runs before every append: a successful reopen recovers
    /// the writer, and a failed one fails the append loudly instead of
    /// letting it acknowledge a write to a dead inode.
    fn reattach_if_stranded(&self, w: &mut File) -> io::Result<()> {
        if !self.stranded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        if let Some(e) = crate::storage::crashpoint::io_fault("doc.wal_reopen") {
            return Err(e);
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "document WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = file;
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
        w.sync_all()?;
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

    /// Log an INSERT operation (insert or replace).
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction,
    /// `Some(id)` inside one, `None` to write the legacy untagged record
    /// (kept unconditionally on replay — the pre-S63 compatibility rule).
    pub fn log_insert(&self, xact: Option<u64>, doc_id: u64, json_bytes: &[u8]) -> io::Result<()> {
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        push_tag(&mut w, xact, ENTRY_INSERT, ENTRY_INSERT_XACT)?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.write_all(&(json_bytes.len() as u32).to_le_bytes())?;
        w.write_all(json_bytes)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log an INSERT that places the document in a named collection.
    ///
    /// An empty `collection` is the default one and is logged with the plain
    /// [`ENTRY_INSERT`] shape, so nothing about an existing log changes until a
    /// named collection is used.
    ///
    /// `xact` mirrors [`DocWal::log_insert`].
    pub fn log_insert_in(
        &self,
        xact: Option<u64>,
        doc_id: u64,
        collection: &str,
        json_bytes: &[u8],
    ) -> io::Result<()> {
        if collection.is_empty() {
            return self.log_insert(xact, doc_id, json_bytes);
        }
        let coll = collection.as_bytes();
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        push_tag(&mut w, xact, ENTRY_INSERT_COLL, ENTRY_INSERT_COLL_XACT)?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.write_all(&(coll.len() as u32).to_le_bytes())?;
        w.write_all(coll)?;
        w.write_all(&(json_bytes.len() as u32).to_le_bytes())?;
        w.write_all(json_bytes)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a DELETE operation.
    ///
    /// `xact` mirrors [`DocWal::log_insert`].
    pub fn log_delete(&self, xact: Option<u64>, doc_id: u64) -> io::Result<()> {
        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        push_tag(&mut w, xact, ENTRY_DELETE, ENTRY_DELETE_XACT)?;
        w.write_all(&doc_id.to_le_bytes())?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Write the complete current state of all documents as a single SNAPSHOT
    /// entry and truncate the log to just that entry.
    ///
    /// `docs` is a slice of `(doc_id, jsonb_bytes)` covering every document
    /// that the store currently holds.
    pub fn checkpoint(&self, docs: &[(u64, Vec<u8>)]) -> io::Result<()> {
        self.checkpoint_in(docs, &std::collections::HashMap::new())
    }

    /// [`checkpoint`](Self::checkpoint) carrying each document's collection.
    ///
    /// `collections` holds only documents in a NAMED collection. When it is
    /// empty the snapshot is written in the original format, so a database that
    /// never used collections keeps producing logs an older build can read.
    pub fn checkpoint_in(
        &self,
        docs: &[(u64, Vec<u8>)],
        collections: &std::collections::HashMap<u64, String>,
    ) -> io::Result<()> {
        // Build the complete new log body (SNAPSHOT tag + all docs).
        let mut buf: Vec<u8> = Vec::new();
        if collections.is_empty() {
            buf.push(ENTRY_SNAPSHOT);
            buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());
            for (doc_id, jsonb) in docs {
                buf.extend_from_slice(&doc_id.to_le_bytes());
                buf.extend_from_slice(&(jsonb.len() as u32).to_le_bytes());
                buf.extend_from_slice(jsonb);
            }
        } else {
            buf.push(ENTRY_SNAPSHOT_COLL);
            buf.extend_from_slice(&(docs.len() as u32).to_le_bytes());
            for (doc_id, jsonb) in docs {
                let coll = collections.get(doc_id).map(String::as_str).unwrap_or("");
                buf.extend_from_slice(&doc_id.to_le_bytes());
                buf.extend_from_slice(&(coll.len() as u32).to_le_bytes());
                buf.extend_from_slice(coll.as_bytes());
                buf.extend_from_slice(&(jsonb.len() as u32).to_le_bytes());
                buf.extend_from_slice(jsonb);
            }
        }

        // Hold the writer lock across the whole checkpoint so no append can interleave
        // between the flush and the reopen. Replace atomically — temp file + fsync +
        // rename — so a crash mid-checkpoint leaves the old log or the new snapshot,
        // never an empty file.
        let mut w = self.writer.lock();
        w.flush()?;
        crate::storage::wal_util::atomic_replace_wal(&self.path, &buf)?;
        // The reopen is the hazardous half: the rename above already unlinked
        // the inode `w` holds, so a failure here leaves the writer pointing at
        // a file no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            .then(|| io::Error::other("injected document WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = if let Some(e) = injected {
            Err(e)
        } else if let Some(e) = crate::storage::crashpoint::io_fault("doc.wal_reopen") {
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
        *w = file;
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

// ─── Replay ─────────────────────────────────────────────────────────────────

/// Emit the tag for one record directly into the locked writer: the `_XACT`
/// twin plus the id when `xact` is `Some`, the legacy untagged tag when
/// `None`.
fn push_tag(w: &mut File, xact: Option<u64>, plain: u8, xact_tagged: u8) -> io::Result<()> {
    match xact {
        Some(x) => {
            w.write_all(&[xact_tagged])?;
            w.write_all(&x.to_le_bytes())
        }
        None => w.write_all(&[plain]),
    }
}

/// Replay all entries in `data` to reconstruct document state.
///
/// SNAPSHOT entries reset all state. Only the *last* SNAPSHOT (and subsequent
/// incremental entries) matter in practice.
///
/// `committed` is the set of coordinating transaction ids that durably
/// committed on the SQL side (S63). A tagged record whose id is neither
/// `XACT_AUTOCOMMIT` nor in it was written inside a transaction that never
/// committed, and is discarded — its body is still parsed past, because
/// nothing length-frames these records and the next one must be found.
fn replay(data: &[u8], committed: &HashSet<u64>) -> DocWalState {
    let mut docs: std::collections::HashMap<u64, Vec<u8>> = std::collections::HashMap::new();
    let mut collections: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    let mut pos = 0usize;
    let mut max_xact_id: u64 = 0;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        // The tagged records parse their id, then share the body parse with
        // the untagged twin. `keep_tagged` is the S63 filter in one
        // expression: an autocommit record is durable by its own fsync, a
        // committed id was vouched for by a durable COMMIT record, anything
        // else never happened. Parsing continues either way — the record
        // must be fully consumed to find the next one, since nothing
        // length-frames these. Ids feed `max_xact_id` whether kept or
        // discarded, so the caller can seed the XactId high-water mark.
        let mut keep_tagged = true;
        if matches!(
            entry_type,
            ENTRY_INSERT_XACT | ENTRY_INSERT_COLL_XACT | ENTRY_DELETE_XACT
        ) {
            let Some(xact) = read_u64(data, &mut pos) else {
                break;
            };
            max_xact_id = max_xact_id.max(xact);
            keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
        }

        match entry_type {
            ENTRY_INSERT | ENTRY_INSERT_XACT => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(jsonb_len) = read_u32(data, &mut pos) else {
                    break;
                };
                let jsonb_len = jsonb_len as usize;
                if pos + jsonb_len > data.len() {
                    break;
                }
                let jsonb = data[pos..pos + jsonb_len].to_vec();
                pos += jsonb_len;
                if keep_tagged {
                    docs.insert(doc_id, jsonb);
                    // Re-inserting at an id moves it to the default collection.
                    collections.remove(&doc_id);
                }
            }
            ENTRY_INSERT_COLL | ENTRY_INSERT_COLL_XACT => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                let Some(coll) = read_bytes(data, &mut pos) else {
                    break;
                };
                let Ok(coll) = String::from_utf8(coll) else {
                    break;
                };
                let Some(jsonb) = read_bytes(data, &mut pos) else {
                    break;
                };
                if keep_tagged {
                    docs.insert(doc_id, jsonb);
                    if coll.is_empty() {
                        collections.remove(&doc_id);
                    } else {
                        collections.insert(doc_id, coll);
                    }
                }
            }
            ENTRY_DELETE | ENTRY_DELETE_XACT => {
                let Some(doc_id) = read_u64(data, &mut pos) else {
                    break;
                };
                if keep_tagged {
                    docs.remove(&doc_id);
                    collections.remove(&doc_id);
                }
            }
            ENTRY_SNAPSHOT_COLL => {
                docs.clear();
                collections.clear();
                let Some(n_docs) = read_u32(data, &mut pos) else {
                    break;
                };
                let mut ok = true;
                for _ in 0..n_docs {
                    let Some(doc_id) = read_u64(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(coll) = read_bytes(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Ok(coll) = String::from_utf8(coll) else {
                        ok = false;
                        break;
                    };
                    let Some(jsonb) = read_bytes(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    docs.insert(doc_id, jsonb);
                    if !coll.is_empty() {
                        collections.insert(doc_id, coll);
                    }
                }
                if !ok {
                    break;
                }
            }
            ENTRY_SNAPSHOT => {
                docs.clear();
                collections.clear();
                let Some(n_docs) = read_u32(data, &mut pos) else {
                    break;
                };
                let mut ok = true;
                for _ in 0..n_docs {
                    let Some(doc_id) = read_u64(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let Some(jsonb_len) = read_u32(data, &mut pos) else {
                        ok = false;
                        break;
                    };
                    let jsonb_len = jsonb_len as usize;
                    if pos + jsonb_len > data.len() {
                        ok = false;
                        break;
                    }
                    let jsonb = data[pos..pos + jsonb_len].to_vec();
                    pos += jsonb_len;
                    docs.insert(doc_id, jsonb);
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

    DocWalState {
        docs: docs.into_iter().collect(),
        collections: collections.into_iter().collect(),
        max_xact_id,
    }
}

/// Read a `u32`-length-prefixed byte run, bounds-checked.
fn read_bytes(data: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = read_u32(data, pos)? as usize;
    let end = pos.checked_add(len)?;
    let out = data.get(*pos..end)?.to_vec();
    *pos = end;
    Some(out)
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

    // ── S63: the recovery filter ──────────────────────────────────────────

    /// Hand-encode one tagged record, so the filter tests do not depend on
    /// the writers they are auditing.
    fn tagged(tag: u8, xact: u64, body: &[u8]) -> Vec<u8> {
        let mut buf = vec![tag];
        buf.extend_from_slice(&xact.to_le_bytes());
        buf.extend_from_slice(body);
        buf
    }

    fn insert_body(doc_id: u64, jsonb: &[u8]) -> Vec<u8> {
        let mut buf = doc_id.to_le_bytes().to_vec();
        buf.extend_from_slice(&(jsonb.len() as u32).to_le_bytes());
        buf.extend_from_slice(jsonb);
        buf
    }

    /// One log exercising every filter decision at once: legacy and
    /// autocommit records keep, committed ids keep, unknown ids discard —
    /// and a discarded record in the MIDDLE does not stop the records after
    /// it (they are parsed past, not abandoned).
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let mut buf = Vec::new();
        // Legacy untagged INSERT (pre-S63 log): keep unconditionally.
        buf.push(ENTRY_INSERT);
        buf.extend_from_slice(&insert_body(1, b"legacy"));
        // Tagged autocommit (0): keep.
        buf.extend_from_slice(&tagged(
            ENTRY_INSERT_XACT,
            XACT_AUTOCOMMIT,
            &insert_body(2, b"auto"),
        ));
        buf.extend_from_slice(&tagged(ENTRY_INSERT_XACT, 7, &insert_body(3, b"committed")));
        buf.extend_from_slice(&tagged(
            ENTRY_INSERT_XACT,
            8,
            &insert_body(4, b"never-committed"),
        )); // discarded, mid-log
        buf.extend_from_slice(&tagged(
            ENTRY_INSERT_XACT,
            9,
            &insert_body(5, b"committed-late"),
        ));

        let committed: HashSet<u64> = [7u64, 9u64].into_iter().collect();
        let state = replay(&buf, &committed);
        assert_eq!(
            state.max_xact_id, 9,
            "discarded records still feed the floor"
        );
        let mut ids: Vec<u64> = state.docs.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3, 5],
            "id 8 never committed; its record must be discarded, not replayed"
        );
    }

    /// DELETE filters the same way: an uncommitted transaction's delete must
    /// not reach documents that survived it. The collection-scoped insert
    /// twin carries the same decision.
    #[test]
    fn tagged_delete_filters_and_collections_round_trip() {
        let mut buf = Vec::new();
        // Autocommit base state: two documents, one in a named collection.
        buf.extend_from_slice(&tagged(
            ENTRY_INSERT_XACT,
            XACT_AUTOCOMMIT,
            &insert_body(1, b"plain"),
        ));
        let mut coll_body = 2u64.to_le_bytes().to_vec();
        coll_body.extend_from_slice(&(3u32).to_le_bytes());
        coll_body.extend_from_slice(b"ops");
        coll_body.extend_from_slice(&(4u32).to_le_bytes());
        coll_body.extend_from_slice(b"body");
        buf.extend_from_slice(&tagged(ENTRY_INSERT_COLL_XACT, XACT_AUTOCOMMIT, &coll_body));
        // Abandoned transaction (id 5): deletes both. Neither may land.
        for id in [1u64, 2u64] {
            buf.extend_from_slice(&tagged(ENTRY_DELETE_XACT, 5, &id.to_le_bytes()));
        }

        let state = replay(&buf, &HashSet::new());
        assert_eq!(
            state.docs.len(),
            2,
            "the abandoned transaction's deletes must be discarded"
        );
        assert_eq!(state.max_xact_id, 5);
        let colls: std::collections::HashMap<_, _> = state.collections.into_iter().collect();
        assert_eq!(colls.get(&2).map(String::as_str), Some("ops"));
    }

    /// A truncation inside a tagged record is a torn tail exactly as for the
    /// untagged ones: replay keeps the clean prefix and abandons the partial
    /// record at its boundary.
    #[test]
    fn torn_tagged_record_keeps_the_clean_prefix() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&tagged(
            ENTRY_INSERT_XACT,
            XACT_AUTOCOMMIT,
            &insert_body(1, b"ok"),
        ));
        let torn_start = buf.len();
        buf.extend_from_slice(&tagged(ENTRY_INSERT_XACT, 3, &insert_body(2, b"torn")));
        // Cut inside the second record's id/body.
        let state = replay(&buf[..torn_start + 6], &[3u64].into_iter().collect());
        let ids: Vec<u64> = state.docs.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![1], "the partial record must be abandoned");
    }

    /// An append is un-fsynced until `group_sync` covers it. NU-006: these
    /// appends used to end at `Write::flush`, a documented no-op on a bare
    /// `File`, so the write was acked while living only in the page cache.
    #[test]
    fn group_sync_marks_clean() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DocWal::open(dir.path()).unwrap();
        assert!(!wal.is_dirty(), "a fresh WAL has no un-fsynced appends");
        wal.log_insert(None, 1, b"a").unwrap();
        assert!(wal.is_dirty(), "an append is uncovered until fsync");
        wal.group_sync().unwrap();
        assert!(!wal.is_dirty(), "group_sync fsyncs the tail");
    }

    #[test]
    fn test_insert_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = DocWal::open(dir.path()).unwrap();
        assert!(state.docs.is_empty());

        wal.log_insert(None, 1, b"hello").unwrap();
        wal.log_insert(None, 2, b"world").unwrap();
        drop(wal);

        let (_wal2, state2) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state2.docs.len(), 2);
        let map: std::collections::HashMap<u64, Vec<u8>> = state2.docs.into_iter().collect();
        assert_eq!(map[&1], b"hello");
        assert_eq!(map[&2], b"world");
    }

    /// S31-14: a checkpoint whose reopen fails must not leave the writer
    /// appending into the unlinked inode the rename displaced. Those appends
    /// report success while no future recovery can ever read them, so an
    /// acknowledged document silently vanishes at restart. The discriminator
    /// is durability: the post-failure insert must land in the replaced file.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = DocWal::open(dir.path()).unwrap();
            wal.log_insert(None, 1, b"before").unwrap();
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&[(1, b"before".to_vec())])
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_insert(None, 2, b"after")
                .expect("a later append must reattach, not strand");
        }
        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        let map: std::collections::HashMap<u64, Vec<u8>> = state.docs.into_iter().collect();
        assert_eq!(
            map.len(),
            2,
            "the post-checkpoint-failure insert went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
        assert_eq!(map[&2], b"after");
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DocWal::open(dir.path()).unwrap();
        wal.log_insert(None, 1, b"aaa").unwrap();
        wal.log_insert(None, 2, b"bbb").unwrap();
        wal.log_delete(None, 1).unwrap();
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
        wal.log_insert(None, 1, b"aaa").unwrap();
        wal.log_insert(None, 2, b"bbb").unwrap();
        // Checkpoint with only doc 2
        wal.checkpoint(&[(2, b"bbb".to_vec())]).unwrap();
        // Insert doc 3 after checkpoint
        wal.log_insert(None, 3, b"ccc").unwrap();
        drop(wal);

        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 2);
        let map: std::collections::HashMap<u64, Vec<u8>> = state.docs.into_iter().collect();
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
        wal.log_insert(None, 1, &big).unwrap();
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
        wal.log_insert(None, 1, b"first").unwrap();
        wal.log_insert(None, 1, b"second").unwrap();
        drop(wal);

        let (_wal2, state) = DocWal::open(dir.path()).unwrap();
        assert_eq!(state.docs.len(), 1);
        assert_eq!(state.docs[0].1, b"second");
    }
}
