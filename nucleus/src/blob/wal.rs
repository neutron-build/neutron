//! Write-ahead log for the blob store.
//!
//! Provides crash-recovery by recording blob mutations to an append-only log
//! file (`blob.wal`). On restart the log is replayed from top to bottom to
//! reconstruct blob manifests.
//!
//! Chunk *data* lives in disk segment files (see [`super::segment`]); the WAL
//! records only manifests (chunk hashes + lengths). Chunks are flushed to
//! their segment before the manifest entry is logged, so a manifest that
//! survives a crash always references chunk data that also survived.
//!
//! ## Log entry binary format
//! ```text
//! STORE_META:    [0x05] [id] [content_type] [total_size: u64 LE]
//!                [n_chunks: u32 LE] [per chunk: hash(32) + chunk_len(u32 LE)]
//!                [n_tags: u32 LE] [per tag: key + value]
//! DELETE:        [0x02] [id]
//! TAG:           [0x03] [id] [key] [value]
//! SNAPSHOT_META: [0x06] [n_blobs: u32 LE] [per blob: STORE_META body]
//! ```
//! Strings are `[len: u32 LE][bytes]`.
//!
//! Legacy entries that embedded full chunk data — STORE (0x01) and SNAPSHOT
//! (0x04) — are still replayed for migration: recovered chunk data is written
//! into segment files on open, after which a checkpoint rewrites the log in
//! the metadata-only format.
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
//! keep-unconditionally meaning, so pre-S63 logs replay unchanged. The
//! legacy STORE/SNAPSHOT records are replay-only and never tagged. A
//! SNAPSHOT_META is committed by construction (the S7 checkpoint gate keeps
//! one from folding an open transaction's writes) and always replays.
//!
//! A SNAPSHOT_META resets all state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT_META entry so the log stays small.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::executor::enlistment::XACT_AUTOCOMMIT;
use crate::storage::wal_util::WalSync;

// ---- Entry type tags --------------------------------------------------------

/// Legacy STORE with embedded chunk data (replay-only).
const ENTRY_STORE: u8 = 0x01;
const ENTRY_DELETE: u8 = 0x02;
const ENTRY_TAG: u8 = 0x03;
/// Legacy SNAPSHOT with embedded chunk data (replay-only).
const ENTRY_SNAPSHOT: u8 = 0x04;
const ENTRY_STORE_META: u8 = 0x05;
const ENTRY_SNAPSHOT_META: u8 = 0x06;
/// S63: STORE_META carrying the coordinating transaction id. Body after the
/// id is byte-identical to [`ENTRY_STORE_META`]'s record.
const ENTRY_STORE_META_XACT: u8 = 0x07;
/// S63: DELETE carrying the coordinating transaction id.
const ENTRY_DELETE_XACT: u8 = 0x08;
/// S63: TAG carrying the coordinating transaction id.
const ENTRY_TAG_XACT: u8 = 0x09;

// ---- Public types -----------------------------------------------------------

/// One chunk reference recovered from WAL replay. `data` is only present for
/// legacy entries that embedded chunk bytes; metadata-only entries carry just
/// the hash and length (the data lives in segment files).
#[derive(Debug, Clone)]
pub struct BlobWalChunk {
    pub hash: [u8; 32],
    pub len: u32,
    pub data: Option<Vec<u8>>,
}

/// A single blob's recovered state from WAL replay.
#[derive(Debug, Clone)]
pub struct BlobWalEntry {
    pub content_type: Option<String>,
    pub total_size: u64,
    pub chunks: Vec<BlobWalChunk>,
    pub tags: HashMap<String, String>,
}

/// Full recovered state from WAL replay.
pub struct BlobWalState {
    /// `blob_id -> recovered entry`.
    pub blobs: HashMap<String, BlobWalEntry>,
    /// Whether any legacy data-carrying entry was replayed (triggers a
    /// checkpoint after migrating the chunk data into segments).
    pub legacy_entries_seen: bool,
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63): a reopened process
    /// must never mint an id that a surviving tagged record already carries,
    /// or the recovery filter could resurrect stale records by matching a
    /// fresh transaction against them.
    pub max_xact_id: u64,
}

/// Manifest snapshot data passed to `checkpoint()` — no chunk data.
#[allow(clippy::type_complexity)]
pub struct BlobMetaSnapshot<'a> {
    /// `(blob_id, content_type, total_size, chunks: (hash, len), tags)`.
    pub blobs: Vec<(
        &'a str,
        Option<&'a str>,
        u64,
        Vec<([u8; 32], u32)>,
        Vec<(&'a str, &'a str)>,
    )>,
}

/// Append-only blob WAL.
pub struct BlobWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// Append/sync bookkeeping for group commit. These appends previously
    /// ended at `BufWriter::flush`, which only moves bytes into the kernel, so
    /// an acknowledged blob metadata write survived `kill -9` but not power
    /// loss. NU-006.
    syncer: WalSync,
    /// The highest coordinating transaction id recovered at open (S63).
    max_xact_id: u64,
}

fn encode_store_meta_body(
    buf: &mut Vec<u8>,
    id: &str,
    content_type: Option<&str>,
    total_size: u64,
    chunks: &[([u8; 32], u32)],
    tags: &[(&str, &str)],
) {
    write_str(buf, id);
    write_str(buf, content_type.unwrap_or(""));
    buf.extend_from_slice(&total_size.to_le_bytes());
    buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for (hash, len) in chunks {
        buf.extend_from_slice(hash);
        buf.extend_from_slice(&len.to_le_bytes());
    }
    buf.extend_from_slice(&(tags.len() as u32).to_le_bytes());
    for (k, v) in tags {
        write_str(buf, k);
        write_str(buf, v);
    }
}

impl BlobWal {
    /// Open or create the WAL file in `dir`, replaying with an EMPTY
    /// committed set so every tagged record keeps — the pre-S63 contract.
    /// The executor opens through [`BlobWal::open_with_committed`] instead,
    /// passing the coordinating transaction ids the SQL side durably
    /// committed so the S63 replay filter can discard the rest.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. A torn or corrupt tail ends replay and is truncated
    /// away, so subsequent appends land on a valid boundary (they would
    /// otherwise sit behind garbage and be lost to every future replay).
    pub fn open(dir: &Path) -> io::Result<(Self, BlobWalState)> {
        Self::open_with_committed(dir, &HashSet::new())
    }

    /// Open or create the WAL file in `dir` whose replay is filtered by the
    /// S63 committed set: a tagged record whose coordinating transaction id
    /// is neither `XACT_AUTOCOMMIT` nor in `committed` was written inside a
    /// transaction that never committed, and is discarded.
    pub fn open_with_committed(
        dir: &Path,
        committed: &HashSet<u64>,
    ) -> io::Result<(Self, BlobWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("blob.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            let (state, valid_end) = replay(&data, committed);
            if valid_end < data.len() {
                eprintln!(
                    "blob WAL: truncating {} torn/corrupt trailing bytes",
                    data.len() - valid_end
                );
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(valid_end as u64)?;
            }
            state
        } else {
            BlobWalState {
                blobs: HashMap::new(),
                legacy_entries_seen: false,
                max_xact_id: 0,
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let max_xact_id = state.max_xact_id;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: WalSync::new(),
                max_xact_id,
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

    /// Log a STORE_META operation (blob put) — manifest only, no chunk data.
    /// Replaces any previous manifest (and tags) for `id` on replay.
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction,
    /// `Some(id)` inside one, `None` to write the legacy untagged record
    /// (kept unconditionally on replay — the pre-S63 compatibility rule).
    pub fn log_store_meta(
        &self,
        xact: Option<u64>,
        id: &str,
        content_type: Option<&str>,
        total_size: u64,
        chunks: &[([u8; 32], u32)],
        tags: &[(&str, &str)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_STORE_META, ENTRY_STORE_META_XACT);
        encode_store_meta_body(&mut buf, id, content_type, total_size, chunks, tags);
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a DELETE operation.
    ///
    /// `xact` mirrors [`BlobWal::log_store_meta`].
    pub fn log_delete(&self, xact: Option<u64>, id: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_DELETE, ENTRY_DELETE_XACT);
        write_str(&mut buf, id);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a TAG operation.
    ///
    /// `xact` mirrors [`BlobWal::log_store_meta`].
    pub fn log_tag(&self, xact: Option<u64>, id: &str, key: &str, val: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        push_tag(&mut buf, xact, ENTRY_TAG, ENTRY_TAG_XACT);
        write_str(&mut buf, id);
        write_str(&mut buf, key);
        write_str(&mut buf, val);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Write a full manifest snapshot and truncate the log to just that
    /// snapshot.
    pub fn checkpoint(&self, snapshot: &BlobMetaSnapshot<'_>) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(snapshot.blobs.len() as u32).to_le_bytes());
        for (id, content_type, total_size, chunks, tags) in &snapshot.blobs {
            encode_store_meta_body(&mut payload, id, *content_type, *total_size, chunks, tags);
        }

        // Flush existing writer
        {
            self.writer.lock().flush()?;
        }

        // Write the snapshot to a temp file and rename it over the log, so a
        // crash mid-checkpoint can never lose the previous log contents.
        let tmp_path = crate::storage::atomic_write::tmp_sibling(&self.path);
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut w = BufWriter::new(file);
            w.write_all(&[ENTRY_SNAPSHOT_META])?;
            w.write_all(&payload)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;

        // Re-open in append mode for future writes
        let file = OpenOptions::new().append(true).open(&self.path)?;
        *self.writer.lock() = BufWriter::new(file);
        Ok(())
    }

    /// Log a legacy STORE entry with embedded chunk data. Retained only so
    /// tests can exercise the migration path from pre-segment WAL files.
    #[cfg(test)]
    pub fn log_store_legacy(
        &self,
        id: &str,
        content_type: Option<&str>,
        total_size: u64,
        chunks: &[([u8; 32], Vec<u8>)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_STORE);
        write_str(&mut buf, id);
        write_str(&mut buf, content_type.unwrap_or(""));
        buf.extend_from_slice(&total_size.to_le_bytes());
        buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
        for (hash, data) in chunks {
            buf.extend_from_slice(hash);
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        }
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }
}

// ---- Binary encoding helpers ------------------------------------------------

/// Emit the tag for one record: the `_XACT` twin plus the id when `xact` is
/// `Some`, the legacy untagged tag when `None`.
fn push_tag(buf: &mut Vec<u8>, xact: Option<u64>, plain: u8, xact_tagged: u8) {
    match xact {
        Some(x) => {
            buf.push(xact_tagged);
            buf.extend_from_slice(&x.to_le_bytes());
        }
        None => buf.push(plain),
    }
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

/// Replay all entries in `data`, filtered by the S63 committed set.
///
/// `committed` holds the coordinating transaction ids that durably committed
/// on the SQL side. A tagged record whose id is neither `XACT_AUTOCOMMIT`
/// nor in `committed` was written inside a transaction that never committed,
/// and is discarded — its body is still parsed past, because nothing
/// length-frames these records and the next one must be found. Returns the
/// recovered state and the byte offset of the first torn/corrupt entry
/// (== `data.len()` when fully valid).
fn replay(data: &[u8], committed: &HashSet<u64>) -> (BlobWalState, usize) {
    let mut blobs: HashMap<String, BlobWalEntry> = HashMap::new();
    let mut legacy_entries_seen = false;
    let mut max_xact_id: u64 = 0;
    let mut pos = 0usize;

    while pos < data.len() {
        // A torn entry may have half-applied (e.g. a partial snapshot cleared
        // state before failing). Re-replaying the clean prefix keeps the
        // recovered state exactly equal to a replay of the truncated file.
        let entry_start = pos;
        macro_rules! torn {
            () => {{
                let (state, _) = replay(&data[..entry_start], committed);
                return (state, entry_start);
            }};
        }

        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        // The tagged records parse their id, then share the body parse with
        // the untagged twin. `keep_tagged` is the S63 filter: an autocommit
        // record is durable by its own fsync, a committed id was vouched for
        // by a durable COMMIT record, anything else never happened. Parsing
        // continues either way — the record must be fully consumed to find
        // the next one, since nothing length-frames these. Ids feed
        // `max_xact_id` whether kept or discarded, so the caller can seed
        // the XactId high-water mark.
        let mut keep_tagged = true;
        if matches!(
            entry_type,
            ENTRY_STORE_META_XACT | ENTRY_DELETE_XACT | ENTRY_TAG_XACT
        ) {
            let Some(xact) = read_u64(data, &mut pos) else {
                torn!();
            };
            max_xact_id = max_xact_id.max(xact);
            keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
        }

        match entry_type {
            ENTRY_STORE => {
                legacy_entries_seen = true;
                let Some(entry) = replay_store_legacy(data, &mut pos) else {
                    torn!();
                };
                blobs.insert(entry.0, entry.1);
            }
            ENTRY_STORE_META | ENTRY_STORE_META_XACT => {
                let Some(entry) = replay_store_meta(data, &mut pos) else {
                    torn!();
                };
                if keep_tagged {
                    blobs.insert(entry.0, entry.1);
                }
            }
            ENTRY_DELETE | ENTRY_DELETE_XACT => {
                let Some(id) = read_string(data, &mut pos) else {
                    torn!();
                };
                if keep_tagged {
                    blobs.remove(&id);
                }
            }
            ENTRY_TAG | ENTRY_TAG_XACT => {
                let Some(id) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(key) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(val) = read_string(data, &mut pos) else {
                    torn!();
                };
                if keep_tagged && let Some(entry) = blobs.get_mut(&id) {
                    entry.tags.insert(key, val);
                }
            }
            ENTRY_SNAPSHOT => {
                legacy_entries_seen = true;
                blobs.clear();
                if !replay_snapshot_legacy(data, &mut pos, &mut blobs) {
                    torn!();
                }
            }
            ENTRY_SNAPSHOT_META => {
                blobs.clear();
                if !replay_snapshot_meta(data, &mut pos, &mut blobs) {
                    torn!();
                }
            }
            _ => {
                // Unknown entry type -- corrupt data; keep the clean prefix.
                torn!();
            }
        }
    }

    (
        BlobWalState {
            blobs,
            legacy_entries_seen,
            max_xact_id,
        },
        pos,
    )
}

fn replay_store_legacy(data: &[u8], pos: &mut usize) -> Option<(String, BlobWalEntry)> {
    let id = read_string(data, pos)?;
    let ct_str = read_string(data, pos)?;
    let content_type = if ct_str.is_empty() {
        None
    } else {
        Some(ct_str)
    };
    let total_size = read_u64(data, pos)?;
    let n_chunks = read_u32(data, pos)? as usize;
    // Off-disk count: an unbounded `with_capacity` here ABORTS the process on
    // Linux (allocation failure is not an `Err`) and silently succeeds on an
    // overcommitting macOS. A chunk record is at least hash(32) + len(4) bytes,
    // so the bytes remaining are an exact bound on how many can really follow.
    let mut chunks = Vec::with_capacity(bounded_by_remaining(data, *pos, n_chunks, 36));
    for _ in 0..n_chunks {
        let hash = read_hash(data, pos)?;
        let chunk_len = read_u32(data, pos)? as usize;
        if *pos + chunk_len > data.len() {
            return None;
        }
        let chunk_data = data[*pos..*pos + chunk_len].to_vec();
        *pos += chunk_len;
        chunks.push(BlobWalChunk {
            hash,
            len: chunk_len as u32,
            data: Some(chunk_data),
        });
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

/// Parse a STORE_META body: id, content_type, size, (hash, len) chunk refs,
/// and tags. Shared by STORE_META and SNAPSHOT_META replay.
fn replay_store_meta(data: &[u8], pos: &mut usize) -> Option<(String, BlobWalEntry)> {
    let id = read_string(data, pos)?;
    let ct_str = read_string(data, pos)?;
    let content_type = if ct_str.is_empty() {
        None
    } else {
        Some(ct_str)
    };
    let total_size = read_u64(data, pos)?;
    let n_chunks = read_u32(data, pos)? as usize;
    // Off-disk count: an unbounded `with_capacity` here ABORTS the process on
    // Linux (allocation failure is not an `Err`) and silently succeeds on an
    // overcommitting macOS. A chunk record is at least hash(32) + len(4) bytes,
    // so the bytes remaining are an exact bound on how many can really follow.
    let mut chunks = Vec::with_capacity(bounded_by_remaining(data, *pos, n_chunks, 36));
    for _ in 0..n_chunks {
        let hash = read_hash(data, pos)?;
        let len = read_u32(data, pos)?;
        chunks.push(BlobWalChunk {
            hash,
            len,
            data: None,
        });
    }
    let n_tags = read_u32(data, pos)? as usize;
    let mut tags = HashMap::new();
    for _ in 0..n_tags {
        let k = read_string(data, pos)?;
        let v = read_string(data, pos)?;
        tags.insert(k, v);
    }
    Some((
        id,
        BlobWalEntry {
            content_type,
            total_size,
            chunks,
            tags,
        },
    ))
}

fn replay_snapshot_meta(
    data: &[u8],
    pos: &mut usize,
    blobs: &mut HashMap<String, BlobWalEntry>,
) -> bool {
    let Some(n_blobs) = read_u32(data, pos) else {
        return false;
    };
    for _ in 0..n_blobs as usize {
        let Some((id, entry)) = replay_store_meta(data, pos) else {
            return false;
        };
        blobs.insert(id, entry);
    }
    true
}

fn replay_snapshot_legacy(
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
        // Off-disk count — bound by the bytes present, not by the claim.
        let mut chunks =
            Vec::with_capacity(bounded_by_remaining(data, *pos, n_chunks as usize, 36));
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
            chunks.push(BlobWalChunk {
                hash,
                len: chunk_len as u32,
                data: Some(chunk_data),
            });
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

/// Bound a declared element count by the bytes actually left in `data`.
///
/// `min_elem_bytes` is the smallest size one element can occupy, so
/// `remaining / min_elem_bytes` is a hard upper bound on how many elements the
/// buffer can really hold. Reserving that never over-reserves, and the caller's
/// loop still returns `None` the moment a read runs off the end — so a corrupt
/// count fails cleanly instead of aborting the process.
fn bounded_by_remaining(data: &[u8], pos: usize, declared: usize, min_elem_bytes: usize) -> usize {
    declared.min(data.len().saturating_sub(pos) / min_elem_bytes)
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
    let s = std::str::from_utf8(&data[*pos..*pos + len])
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

// ---- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // ── S63: the recovery filter ──────────────────────────────────────────

    /// One log exercising every filter decision at once: legacy and
    /// autocommit records keep, committed ids keep, unknown ids discard —
    /// and a discarded record in the MIDDLE does not stop the records after
    /// it (they are parsed past, not abandoned).
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let hash = *blake3::hash(b"data").as_bytes();
        let mut buf = Vec::new();
        // Legacy untagged STORE_META (pre-S63 log): keep unconditionally.
        buf.push(ENTRY_STORE_META);
        encode_store_meta_body(&mut buf, "legacy", None, 4, &[(hash, 4)], &[]);
        let tagged_store = |buf: &mut Vec<u8>, xact: u64, id: &str| {
            let mut rec = Vec::new();
            push_tag(
                &mut rec,
                Some(xact),
                ENTRY_STORE_META,
                ENTRY_STORE_META_XACT,
            );
            encode_store_meta_body(&mut rec, id, None, 4, &[(hash, 4)], &[]);
            buf.extend_from_slice(&rec);
        };
        tagged_store(&mut buf, XACT_AUTOCOMMIT, "auto");
        tagged_store(&mut buf, 7, "committed");
        tagged_store(&mut buf, 8, "never_committed"); // discarded, mid-log
        // An uncommitted transaction's DELETE of the surviving "committed"
        // blob and TAG on it must be discarded too.
        let mut del = Vec::new();
        push_tag(&mut del, Some(8), ENTRY_DELETE, ENTRY_DELETE_XACT);
        write_str(&mut del, "committed");
        buf.extend_from_slice(&del);
        let mut tag = Vec::new();
        push_tag(&mut tag, Some(8), ENTRY_TAG, ENTRY_TAG_XACT);
        write_str(&mut tag, "committed");
        write_str(&mut tag, "touched");
        write_str(&mut tag, "yes");
        buf.extend_from_slice(&tag);
        tagged_store(&mut buf, 9, "committed_late");

        let committed: HashSet<u64> = [7u64, 9u64].into_iter().collect();
        let (state, end) = replay(&buf, &committed);
        assert_eq!(end, buf.len(), "the whole log is well-formed");
        assert_eq!(
            state.max_xact_id, 9,
            "discarded records still feed the floor"
        );
        let mut ids: Vec<&str> = state.blobs.keys().map(|s| s.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec!["auto", "committed", "committed_late", "legacy"],
            "id 8 never committed; its store, delete and tag records must all \
             be discarded, not replayed"
        );
        assert!(
            !state.blobs["committed"].tags.contains_key("touched"),
            "the abandoned TAG must be discarded"
        );
    }

    /// A truncation inside a tagged record is a torn tail exactly as for the
    /// untagged ones: replay keeps the clean prefix and abandons the partial
    /// record at its boundary.
    #[test]
    fn torn_tagged_record_keeps_the_clean_prefix() {
        let hash = *blake3::hash(b"data").as_bytes();
        let mut buf = Vec::new();
        push_tag(
            &mut buf,
            Some(XACT_AUTOCOMMIT),
            ENTRY_STORE_META,
            ENTRY_STORE_META_XACT,
        );
        encode_store_meta_body(&mut buf, "first", None, 4, &[(hash, 4)], &[]);
        let first_len = buf.len();
        push_tag(&mut buf, Some(3), ENTRY_STORE_META, ENTRY_STORE_META_XACT);
        encode_store_meta_body(&mut buf, "second", None, 4, &[(hash, 4)], &[]);

        let (state, end) = replay(&buf[..first_len + 6], &[3u64].into_iter().collect());
        assert_eq!(end, first_len, "the partial record is the torn tail");
        assert!(
            state.blobs.contains_key("first"),
            "the complete record before the cut must replay"
        );
        assert!(
            !state.blobs.contains_key("second"),
            "the partial record must be abandoned, not half-applied"
        );
    }

    #[test]
    fn test_store_meta_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs.is_empty());
        assert!(!state.legacy_entries_seen);

        let hash1 = blake3::hash(b"chunk1");
        let hash2 = blake3::hash(b"chunk2");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "blob1",
            Some("text/plain"),
            12,
            &[(*hash1.as_bytes(), 6), (*hash2.as_bytes(), 6)],
            &[("author", "Alice")],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state2) = BlobWal::open(dir.path()).unwrap();
        assert!(!state2.legacy_entries_seen);
        assert_eq!(state2.blobs.len(), 1);
        let entry = state2.blobs.get("blob1").unwrap();
        assert_eq!(entry.content_type.as_deref(), Some("text/plain"));
        assert_eq!(entry.total_size, 12);
        assert_eq!(entry.chunks.len(), 2);
        assert_eq!(entry.chunks[0].hash, *hash1.as_bytes());
        assert_eq!(entry.chunks[0].len, 6);
        assert!(entry.chunks[0].data.is_none());
        assert_eq!(entry.tags["author"], "Alice");
    }

    #[test]
    fn test_legacy_store_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash1 = blake3::hash(b"chunk1");
        let hash2 = blake3::hash(b"chunk2");
        wal.log_store_legacy(
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
        assert!(state2.legacy_entries_seen);
        assert_eq!(state2.blobs.len(), 1);
        let entry = state2.blobs.get("blob1").unwrap();
        assert_eq!(entry.content_type.as_deref(), Some("text/plain"));
        assert_eq!(entry.total_size, 12);
        assert_eq!(entry.chunks.len(), 2);
        assert_eq!(entry.chunks[0].data.as_deref(), Some(b"chunk1".as_slice()));
        assert_eq!(entry.chunks[1].data.as_deref(), Some(b"chunk2".as_slice()));
    }

    #[test]
    fn test_delete_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "blob1",
            None,
            4,
            &[(*hash.as_bytes(), 4)],
            &[],
        )
        .unwrap();
        wal.log_delete(Some(XACT_AUTOCOMMIT), "blob1").unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs.is_empty());
    }

    #[test]
    fn test_tag_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "blob1",
            Some("image/png"),
            4,
            &[(*hash.as_bytes(), 4)],
            &[],
        )
        .unwrap();
        wal.log_tag(Some(XACT_AUTOCOMMIT), "blob1", "author", "Alice")
            .unwrap();
        wal.log_tag(Some(XACT_AUTOCOMMIT), "blob1", "dept", "Eng")
            .unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        let entry = state.blobs.get("blob1").unwrap();
        assert_eq!(entry.tags["author"], "Alice");
        assert_eq!(entry.tags["dept"], "Eng");
    }

    #[test]
    fn test_store_meta_replaces_tags() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "blob1",
            None,
            4,
            &[(*hash.as_bytes(), 4)],
            &[],
        )
        .unwrap();
        wal.log_tag(Some(XACT_AUTOCOMMIT), "blob1", "stale", "yes")
            .unwrap();
        // Overwrite resets tags wholesale.
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "blob1",
            None,
            4,
            &[(*hash.as_bytes(), 4)],
            &[("fresh", "yes")],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        let entry = state.blobs.get("blob1").unwrap();
        assert_eq!(entry.tags.len(), 1);
        assert_eq!(entry.tags["fresh"], "yes");
    }

    #[test]
    fn test_checkpoint_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        // Store two blobs
        let h1 = blake3::hash(b"aaa");
        let h2 = blake3::hash(b"bbb");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "a",
            None,
            3,
            &[(*h1.as_bytes(), 3)],
            &[],
        )
        .unwrap();
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "b",
            None,
            3,
            &[(*h2.as_bytes(), 3)],
            &[],
        )
        .unwrap();

        // Checkpoint with only blob "a"
        let snapshot = BlobMetaSnapshot {
            blobs: vec![(
                "a",
                None,
                3,
                vec![(*h1.as_bytes(), 3)],
                vec![("tag1", "val1")],
            )],
        };
        wal.checkpoint(&snapshot).unwrap();

        // Store another blob after checkpoint
        let h3 = blake3::hash(b"ccc");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "c",
            None,
            3,
            &[(*h3.as_bytes(), 3)],
            &[],
        )
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

        // Write a valid STORE_META entry followed by garbage bytes
        {
            let (wal, _) = BlobWal::open(dir.path()).unwrap();
            let hash = blake3::hash(b"good");
            wal.log_store_meta(
                Some(XACT_AUTOCOMMIT),
                "good_blob",
                None,
                4,
                &[(*hash.as_bytes(), 4)],
                &[],
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
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "blob1",
            None,
            4,
            &[(*hash.as_bytes(), 4)],
            &[],
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
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "key",
            Some("text/plain"),
            3,
            &[(*h1.as_bytes(), 3)],
            &[],
        )
        .unwrap();

        let h2 = blake3::hash(b"new");
        wal.log_store_meta(
            Some(XACT_AUTOCOMMIT),
            "key",
            Some("text/html"),
            3,
            &[(*h2.as_bytes(), 3)],
            &[],
        )
        .unwrap();
        drop(wal);

        let (_wal2, state) = BlobWal::open(dir.path()).unwrap();
        assert_eq!(state.blobs.len(), 1);
        let entry = &state.blobs["key"];
        assert_eq!(entry.content_type.as_deref(), Some("text/html"));
        assert_eq!(entry.chunks[0].hash, *h2.as_bytes());
    }

    #[test]
    fn test_legacy_snapshot_replay() {
        // Hand-encode a legacy (0x04) snapshot to prove old WAL files replay.
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("blob.wal");
        let mut buf = Vec::new();
        buf.push(ENTRY_SNAPSHOT);
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_blobs
        write_str(&mut buf, "legacy_blob");
        write_str(&mut buf, "text/plain");
        buf.extend_from_slice(&4u64.to_le_bytes()); // total_size
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_chunks
        let hash = blake3::hash(b"data");
        buf.extend_from_slice(hash.as_bytes());
        buf.extend_from_slice(&4u32.to_le_bytes());
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&1u32.to_le_bytes()); // n_tags
        write_str(&mut buf, "k");
        write_str(&mut buf, "v");
        std::fs::write(&wal_path, &buf).unwrap();

        let (_wal, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.legacy_entries_seen);
        let entry = &state.blobs["legacy_blob"];
        assert_eq!(entry.content_type.as_deref(), Some("text/plain"));
        assert_eq!(entry.chunks[0].data.as_deref(), Some(b"data".as_slice()));
        assert_eq!(entry.tags["k"], "v");
    }
}
