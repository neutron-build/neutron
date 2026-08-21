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
//! A SNAPSHOT_META resets all state. After `checkpoint()` the file is
//! truncated to a single SNAPSHOT_META entry so the log stays small.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

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
    /// Open or create the WAL file in `dir`.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. A torn or corrupt tail ends replay and is truncated
    /// away, so subsequent appends land on a valid boundary (they would
    /// otherwise sit behind garbage and be lost to every future replay).
    pub fn open(dir: &Path) -> io::Result<(Self, BlobWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("blob.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            let (state, valid_end) = replay(&data);
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
            }
        };
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                syncer: WalSync::new(),
            },
            state,
        ))
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
    pub fn log_store_meta(
        &self,
        id: &str,
        content_type: Option<&str>,
        total_size: u64,
        chunks: &[([u8; 32], u32)],
        tags: &[(&str, &str)],
    ) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_STORE_META);
        encode_store_meta_body(&mut buf, id, content_type, total_size, chunks, tags);
        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
    }

    /// Log a DELETE operation.
    pub fn log_delete(&self, id: &str) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.push(ENTRY_DELETE);
        write_str(&mut buf, id);

        let mut w = self.writer.lock();
        w.write_all(&buf)?;
        w.flush()?;
        self.syncer.on_append();
        Ok(())
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
        let tmp_path = self.path.with_extension("wal.tmp");
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

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

// ---- Replay -----------------------------------------------------------------

/// Replay all entries in `data` to reconstruct blob state.
/// Replay all entries in `data`. Returns the recovered state and the byte
/// offset of the first torn/corrupt entry (== `data.len()` when fully valid).
fn replay(data: &[u8]) -> (BlobWalState, usize) {
    let mut blobs: HashMap<String, BlobWalEntry> = HashMap::new();
    let mut legacy_entries_seen = false;
    let mut pos = 0usize;

    while pos < data.len() {
        // A torn entry may have half-applied (e.g. a partial snapshot cleared
        // state before failing). Re-replaying the clean prefix keeps the
        // recovered state exactly equal to a replay of the truncated file.
        let entry_start = pos;
        macro_rules! torn {
            () => {{
                let (state, _) = replay(&data[..entry_start]);
                return (state, entry_start);
            }};
        }

        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        match entry_type {
            ENTRY_STORE => {
                legacy_entries_seen = true;
                let Some(entry) = replay_store_legacy(data, &mut pos) else {
                    torn!();
                };
                blobs.insert(entry.0, entry.1);
            }
            ENTRY_STORE_META => {
                let Some(entry) = replay_store_meta(data, &mut pos) else {
                    torn!();
                };
                blobs.insert(entry.0, entry.1);
            }
            ENTRY_DELETE => {
                let Some(id) = read_string(data, &mut pos) else {
                    torn!();
                };
                blobs.remove(&id);
            }
            ENTRY_TAG => {
                let Some(id) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(key) = read_string(data, &mut pos) else {
                    torn!();
                };
                let Some(val) = read_string(data, &mut pos) else {
                    torn!();
                };
                if let Some(entry) = blobs.get_mut(&id) {
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

    #[test]
    fn test_store_meta_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = BlobWal::open(dir.path()).unwrap();
        assert!(state.blobs.is_empty());
        assert!(!state.legacy_entries_seen);

        let hash1 = blake3::hash(b"chunk1");
        let hash2 = blake3::hash(b"chunk2");
        wal.log_store_meta(
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
        wal.log_store_meta("blob1", None, 4, &[(*hash.as_bytes(), 4)], &[])
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
        wal.log_store_meta("blob1", Some("image/png"), 4, &[(*hash.as_bytes(), 4)], &[])
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
    fn test_store_meta_replaces_tags() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = BlobWal::open(dir.path()).unwrap();

        let hash = blake3::hash(b"data");
        wal.log_store_meta("blob1", None, 4, &[(*hash.as_bytes(), 4)], &[])
            .unwrap();
        wal.log_tag("blob1", "stale", "yes").unwrap();
        // Overwrite resets tags wholesale.
        wal.log_store_meta(
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
        wal.log_store_meta("a", None, 3, &[(*h1.as_bytes(), 3)], &[])
            .unwrap();
        wal.log_store_meta("b", None, 3, &[(*h2.as_bytes(), 3)], &[])
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
        wal.log_store_meta("c", None, 3, &[(*h3.as_bytes(), 3)], &[])
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
            wal.log_store_meta("good_blob", None, 4, &[(*hash.as_bytes(), 4)], &[])
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
        wal.log_store_meta("blob1", None, 4, &[(*hash.as_bytes(), 4)], &[])
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
        wal.log_store_meta("key", Some("text/plain"), 3, &[(*h1.as_bytes(), 3)], &[])
            .unwrap();

        let h2 = blake3::hash(b"new");
        wal.log_store_meta("key", Some("text/html"), 3, &[(*h2.as_bytes(), 3)], &[])
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
