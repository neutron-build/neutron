//! Disk-backed segment store for blob chunks.
//!
//! Chunks are appended to immutable segment files (`segments/seg-NNNNNNNN.seg`)
//! and located through an in-RAM index rebuilt by scanning the segments on
//! open. Records are never modified in place; space held by dead (unreferenced)
//! chunks is reclaimed by compacting segments whose dead ratio crosses a
//! threshold — live records are rewritten to the active segment and the old
//! file is deleted. A crash between the rewrite and the delete leaves duplicate
//! records; the open-time scan deduplicates them (last copy wins), so
//! compaction is crash-safe.
//!
//! ## Record binary format
//! ```text
//! [hash: 32 bytes BLAKE3] [len: u32 LE] [crc32c(data): u32 LE] [data: len bytes]
//! ```
//! A torn tail (partial record, bad CRC) ends the scan of that segment; for the
//! newest segment the tail is truncated so appends resume from a clean offset.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use super::ChunkHash;

/// Per-record framing overhead: hash(32) + len(4) + crc(4).
const RECORD_HEADER: u64 = 40;

/// Roll to a new segment file once the active one reaches this size.
pub const DEFAULT_SEGMENT_ROLL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    segment: u32,
    offset: u64,
    len: u32,
    /// Unreferenced by any blob manifest — space reclaimable by compaction.
    /// The record stays readable (and revivable) until its segment compacts.
    dead: bool,
}

impl IndexEntry {
    fn record_bytes(&self) -> u64 {
        RECORD_HEADER + self.len as u64
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct SegmentStats {
    total_bytes: u64,
    dead_bytes: u64,
}

/// Append-only chunk segment store.
pub struct SegmentStore {
    dir: PathBuf,
    index: HashMap<ChunkHash, IndexEntry>,
    stats: HashMap<u32, SegmentStats>,
    writer: BufWriter<File>,
    write_seg: u32,
    write_off: u64,
    roll_bytes: u64,
    /// Cached read handles per segment (positioned reads, shared across threads).
    readers: Mutex<HashMap<u32, Arc<File>>>,
    /// Segments with bytes flushed to the OS but not yet fsynced. Drained by
    /// [`Self::sync`], which the blob commit boundary calls BEFORE the WAL
    /// group-sync: the log must never vouch for a manifest whose chunk data
    /// is still only in the page cache.
    dirty: Mutex<HashSet<u32>>,
    /// A new segment file was created since the last successful sync — the
    /// directory entry itself needs an fsync to survive power loss.
    dir_dirty: Mutex<bool>,
}

fn segment_path(dir: &Path, seg: u32) -> PathBuf {
    dir.join(format!("seg-{seg:08}.seg"))
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0usize;
    while done < buf.len() {
        let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment read past EOF",
            ));
        }
        done += n;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(_file: &File, _buf: &mut [u8], _offset: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "positioned segment reads unsupported on this platform",
    ))
}

impl SegmentStore {
    /// Open (or create) the segment store under `dir/segments`, rebuilding the
    /// chunk index by scanning every segment file.
    pub fn open(dir: &Path) -> io::Result<Self> {
        let seg_dir = dir.join("segments");
        std::fs::create_dir_all(&seg_dir)?;

        // Collect existing segment ids, sorted ascending so later duplicates
        // (from an interrupted compaction) win over earlier copies.
        let mut seg_ids: Vec<u32> = Vec::new();
        for entry in std::fs::read_dir(&seg_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name
                .strip_prefix("seg-")
                .and_then(|s| s.strip_suffix(".seg"))
                .and_then(|s| s.parse::<u32>().ok())
            {
                seg_ids.push(id);
            }
        }
        seg_ids.sort_unstable();

        let mut index: HashMap<ChunkHash, IndexEntry> = HashMap::new();
        let mut stats: HashMap<u32, SegmentStats> = HashMap::new();

        for (i, &seg) in seg_ids.iter().enumerate() {
            let path = segment_path(&seg_dir, seg);
            let data = std::fs::read(&path)?;
            let valid_end = scan_segment(seg, &data, &mut index, &mut stats);
            if valid_end < data.len() {
                eprintln!(
                    "blob segments: seg-{seg:08} has {} torn/corrupt trailing bytes",
                    data.len() - valid_end
                );
                // Truncate the torn tail of the newest segment so appends
                // resume from a clean offset. Older segments are immutable;
                // their tail is simply ignored.
                if i == seg_ids.len() - 1 {
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(valid_end as u64)?;
                }
            }
        }

        // Continue appending to the newest segment if it has room; otherwise
        // start a fresh one.
        let (write_seg, write_off) = match seg_ids.last() {
            Some(&last) => {
                let size = stats.get(&last).map(|s| s.total_bytes).unwrap_or(0);
                if size < DEFAULT_SEGMENT_ROLL_BYTES {
                    (last, size)
                } else {
                    (last + 1, 0)
                }
            }
            None => (0, 0),
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&seg_dir, write_seg))?;

        Ok(Self {
            dir: seg_dir,
            index,
            stats,
            writer: BufWriter::new(file),
            write_seg,
            write_off,
            roll_bytes: DEFAULT_SEGMENT_ROLL_BYTES,
            readers: Mutex::new(HashMap::new()),
            dirty: Mutex::new(HashSet::new()),
            dir_dirty: Mutex::new(false),
        })
    }

    /// Override the segment roll size (tests use small segments).
    #[cfg(test)]
    pub fn set_roll_bytes(&mut self, bytes: u64) {
        self.roll_bytes = bytes;
    }

    /// Whether a live (non-dead) record exists for `hash`.
    pub fn contains_live(&self, hash: &ChunkHash) -> bool {
        self.index.get(hash).is_some_and(|e| !e.dead)
    }

    /// Append a chunk record and flush it to the OS. The chunk becomes
    /// readable immediately. No-op if a live record already exists.
    pub fn append(&mut self, hash: &ChunkHash, data: &[u8]) -> io::Result<()> {
        if self.contains_live(hash) {
            return Ok(());
        }
        // If a dead copy survives on disk, reviving it is free.
        if self.revive(hash) {
            return Ok(());
        }

        let rec_len = RECORD_HEADER + data.len() as u64;
        if self.write_off > 0 && self.write_off + rec_len > self.roll_bytes {
            self.roll()?;
        }

        self.writer.write_all(hash)?;
        self.writer.write_all(&(data.len() as u32).to_le_bytes())?;
        self.writer.write_all(&crc32c::crc32c(data).to_le_bytes())?;
        self.writer.write_all(data)?;
        self.writer.flush()?;

        let entry = IndexEntry {
            segment: self.write_seg,
            offset: self.write_off,
            len: data.len() as u32,
            dead: false,
        };
        self.write_off += rec_len;
        self.stats.entry(self.write_seg).or_default().total_bytes += rec_len;
        if let Some(old) = self.index.insert(*hash, entry) {
            // Duplicate copy left behind (interrupted compaction): the old
            // record's bytes are now dead weight in its segment.
            let s = self.stats.entry(old.segment).or_default();
            s.dead_bytes += old.record_bytes();
        }
        self.dirty.lock().insert(self.write_seg);
        Ok(())
    }

    fn roll(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.write_seg += 1;
        self.write_off = 0;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&self.dir, self.write_seg))?;
        self.writer = BufWriter::new(file);
        self.dirty.lock().insert(self.write_seg);
        *self.dir_dirty.lock() = true;
        Ok(())
    }

    /// Fsync every segment written since the last successful call, plus the
    /// segments directory when new files appeared. `&self` works because every
    /// write path ends with `flush` — the BufWriter is empty between calls, so
    /// `get_ref()` reaches all appended bytes.
    pub fn sync(&self) -> io::Result<()> {
        let dirty: Vec<u32> = self.dirty.lock().drain().collect();
        let dir_dirty = std::mem::take(&mut *self.dir_dirty.lock());
        let mut first_err: Option<io::Error> = None;
        for &seg in &dirty {
            let res = if seg == self.write_seg {
                self.writer.get_ref().sync_all()
            } else {
                // Sealed segments have no live handle; a fresh fd on the same
                // inode flushes the file's dirty pages (POSIX fsync semantics).
                OpenOptions::new()
                    .write(true)
                    .open(segment_path(&self.dir, seg))
                    .and_then(|f| f.sync_all())
            };
            if let Err(e) = res {
                first_err.get_or_insert(e);
                self.dirty.lock().insert(seg); // retry at the next commit
            }
        }
        #[cfg(unix)]
        if dir_dirty
            && first_err.is_none()
            && let Err(e) = File::open(&self.dir).and_then(|d| d.sync_all())
        {
            *self.dir_dirty.lock() = true;
            first_err.get_or_insert(e);
        }
        #[cfg(not(unix))]
        let _ = dir_dirty;
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Whether any segment has bytes not yet fsynced.
    pub fn is_dirty(&self) -> bool {
        !self.dirty.lock().is_empty() || *self.dir_dirty.lock()
    }

    /// Read a chunk's data, verifying its CRC. Returns `Ok(None)` if the hash
    /// has no record (live or dead).
    pub fn read(&self, hash: &ChunkHash) -> io::Result<Option<Vec<u8>>> {
        let Some(entry) = self.index.get(hash).copied() else {
            return Ok(None);
        };
        let file = {
            let mut readers = self.readers.lock();
            match readers.get(&entry.segment) {
                Some(f) => Arc::clone(f),
                None => {
                    let f = Arc::new(File::open(segment_path(&self.dir, entry.segment))?);
                    readers.insert(entry.segment, Arc::clone(&f));
                    f
                }
            }
        };
        let mut buf = vec![0u8; RECORD_HEADER as usize + entry.len as usize];
        read_exact_at(&file, &mut buf, entry.offset)?;
        if buf[..32] != hash[..] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment record hash mismatch",
            ));
        }
        let stored_crc = u32::from_le_bytes([buf[36], buf[37], buf[38], buf[39]]);
        let data = buf.split_off(RECORD_HEADER as usize);
        if crc32c::crc32c(&data) != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment record CRC mismatch",
            ));
        }
        Ok(Some(data))
    }

    /// Mark a chunk's record as dead (unreferenced). The bytes are reclaimed
    /// when the segment compacts. No-op if absent or already dead.
    pub fn mark_dead(&mut self, hash: &ChunkHash) {
        if let Some(entry) = self.index.get_mut(hash)
            && !entry.dead
        {
            entry.dead = true;
            let bytes = entry.record_bytes();
            let seg = entry.segment;
            self.stats.entry(seg).or_default().dead_bytes += bytes;
        }
    }

    /// Mark every live record whose hash fails `keep` as dead.
    pub fn mark_dead_where<F: Fn(&ChunkHash) -> bool>(&mut self, keep: F) {
        let mut newly_dead: Vec<(u32, u64)> = Vec::new();
        for (hash, entry) in self.index.iter_mut() {
            if !entry.dead && !keep(hash) {
                entry.dead = true;
                newly_dead.push((entry.segment, entry.record_bytes()));
            }
        }
        for (seg, bytes) in newly_dead {
            self.stats.entry(seg).or_default().dead_bytes += bytes;
        }
    }

    /// Bring a dead record back to life (a rolled-back delete, or a re-put of
    /// content that still sits in a segment). Returns `false` if no record
    /// exists for `hash`.
    pub fn revive(&mut self, hash: &ChunkHash) -> bool {
        if let Some(entry) = self.index.get_mut(hash) {
            if entry.dead {
                entry.dead = false;
                let bytes = entry.record_bytes();
                let seg = entry.segment;
                if let Some(s) = self.stats.get_mut(&seg) {
                    s.dead_bytes = s.dead_bytes.saturating_sub(bytes);
                }
            }
            true
        } else {
            false
        }
    }

    /// Compact sealed segments whose dead bytes reach half their size: rewrite
    /// live records into the active segment, then delete the old file.
    pub fn compact(&mut self) -> io::Result<()> {
        let victims: Vec<u32> = self
            .stats
            .iter()
            .filter(|(seg, s)| {
                **seg != self.write_seg && s.dead_bytes > 0 && s.dead_bytes * 2 >= s.total_bytes
            })
            .map(|(seg, _)| *seg)
            .collect();

        for victim in victims {
            let live: Vec<ChunkHash> = self
                .index
                .iter()
                .filter(|(_, e)| e.segment == victim && !e.dead)
                .map(|(h, _)| *h)
                .collect();

            // Rewrite live records first; only then delete the old segment.
            // A crash in between leaves duplicates that the open-time scan
            // deduplicates, so no data is ever lost.
            for hash in live {
                let Some(data) = self.read(&hash)? else {
                    continue;
                };
                let rec_len = RECORD_HEADER + data.len() as u64;
                if self.write_off > 0 && self.write_off + rec_len > self.roll_bytes {
                    self.roll()?;
                }
                self.writer.write_all(&hash)?;
                self.writer.write_all(&(data.len() as u32).to_le_bytes())?;
                self.writer
                    .write_all(&crc32c::crc32c(&data).to_le_bytes())?;
                self.writer.write_all(&data)?;
                let entry = IndexEntry {
                    segment: self.write_seg,
                    offset: self.write_off,
                    len: data.len() as u32,
                    dead: false,
                };
                self.write_off += rec_len;
                self.stats.entry(self.write_seg).or_default().total_bytes += rec_len;
                self.index.insert(hash, entry);
            }
            self.writer.flush()?;
            // Rewritten copies durable before their source file disappears:
            // power loss between the delete and this sync would otherwise
            // lose the chunk entirely. A crash before the delete still leaves
            // duplicates that the open-time scan deduplicates (last copy
            // wins) — unchanged.
            self.dirty.lock().insert(self.write_seg);
            self.sync()?;

            // Drop index entries (all remaining ones are dead) and the file.
            self.index.retain(|_, e| e.segment != victim);
            self.stats.remove(&victim);
            self.readers.lock().remove(&victim);
            self.dirty.lock().remove(&victim);
            let path = segment_path(&self.dir, victim);
            if let Err(e) = std::fs::remove_file(&path) {
                eprintln!("blob segments: failed to remove compacted {path:?}: {e}");
            }
        }
        Ok(())
    }

    /// Total dead (reclaimable-by-compaction) bytes across all segments.
    pub fn dead_bytes(&self) -> u64 {
        self.stats.values().map(|s| s.dead_bytes).sum()
    }

    /// Total bytes across all segment files.
    pub fn total_bytes(&self) -> u64 {
        self.stats.values().map(|s| s.total_bytes).sum()
    }

    /// Number of segment files currently tracked.
    #[cfg(test)]
    pub fn segment_count(&self) -> usize {
        let mut segs: Vec<u32> = self.index.values().map(|e| e.segment).collect();
        segs.push(self.write_seg);
        segs.sort_unstable();
        segs.dedup();
        segs.len()
    }
}

/// Scan one segment's bytes, indexing every valid record (later duplicates of
/// a hash win; the displaced copy's bytes count as dead). Returns the offset
/// of the first invalid byte (== `data.len()` when the file is fully valid).
fn scan_segment(
    seg: u32,
    data: &[u8],
    index: &mut HashMap<ChunkHash, IndexEntry>,
    stats: &mut HashMap<u32, SegmentStats>,
) -> usize {
    let mut pos = 0usize;
    loop {
        if pos + RECORD_HEADER as usize > data.len() {
            break;
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&data[pos..pos + 32]);
        let len = u32::from_le_bytes([
            data[pos + 32],
            data[pos + 33],
            data[pos + 34],
            data[pos + 35],
        ]) as usize;
        let crc = u32::from_le_bytes([
            data[pos + 36],
            data[pos + 37],
            data[pos + 38],
            data[pos + 39],
        ]);
        let data_start = pos + RECORD_HEADER as usize;
        if data_start + len > data.len() {
            break;
        }
        let chunk = &data[data_start..data_start + len];
        if crc32c::crc32c(chunk) != crc {
            break;
        }
        let entry = IndexEntry {
            segment: seg,
            offset: pos as u64,
            len: len as u32,
            dead: false,
        };
        let rec_bytes = entry.record_bytes();
        stats.entry(seg).or_default().total_bytes += rec_bytes;
        if let Some(old) = index.insert(hash, entry) {
            let s = stats.entry(old.segment).or_default();
            s.dead_bytes += old.record_bytes();
        }
        pos = data_start + len;
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::content_hash_blake3;

    fn put(store: &mut SegmentStore, data: &[u8]) -> ChunkHash {
        let hash = content_hash_blake3(data);
        store.append(&hash, data).unwrap();
        hash
    }

    #[test]
    fn append_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SegmentStore::open(dir.path()).unwrap();
        let h1 = put(&mut store, b"hello");
        let h2 = put(&mut store, b"world!!");
        assert_eq!(store.read(&h1).unwrap().unwrap(), b"hello");
        assert_eq!(store.read(&h2).unwrap().unwrap(), b"world!!");
        assert!(store.contains_live(&h1));
        let missing = content_hash_blake3(b"nope");
        assert!(store.read(&missing).unwrap().is_none());
    }

    #[test]
    fn scan_rebuilds_index() {
        let dir = tempfile::tempdir().unwrap();
        let (h1, h2);
        {
            let mut store = SegmentStore::open(dir.path()).unwrap();
            h1 = put(&mut store, b"persist me");
            h2 = put(&mut store, b"me too");
        }
        let store = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(store.read(&h1).unwrap().unwrap(), b"persist me");
        assert_eq!(store.read(&h2).unwrap().unwrap(), b"me too");
    }

    #[test]
    fn torn_tail_truncated_and_appendable() {
        let dir = tempfile::tempdir().unwrap();
        let h1;
        {
            let mut store = SegmentStore::open(dir.path()).unwrap();
            h1 = put(&mut store, b"good record");
        }
        // Simulate a torn write on the newest segment.
        let seg_path = dir.path().join("segments").join("seg-00000000.seg");
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
            f.write_all(&[0xAB; 17]).unwrap();
        }
        let mut store = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(store.read(&h1).unwrap().unwrap(), b"good record");
        // Appends after truncation land on a clean boundary and survive reopen.
        let h2 = put(&mut store, b"after torn tail");
        drop(store);
        let store = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(store.read(&h1).unwrap().unwrap(), b"good record");
        assert_eq!(store.read(&h2).unwrap().unwrap(), b"after torn tail");
    }

    #[test]
    fn corrupted_data_fails_crc() {
        let dir = tempfile::tempdir().unwrap();
        let h1;
        {
            let mut store = SegmentStore::open(dir.path()).unwrap();
            h1 = put(&mut store, b"will corrupt");
        }
        // Flip a data byte in place (offset 40 = first data byte).
        let seg_path = dir.path().join("segments").join("seg-00000000.seg");
        {
            let mut bytes = std::fs::read(&seg_path).unwrap();
            bytes[41] ^= 0xFF;
            std::fs::write(&seg_path, &bytes).unwrap();
        }
        // Scan drops the corrupt record entirely.
        let store = SegmentStore::open(dir.path()).unwrap();
        assert!(store.read(&h1).unwrap().is_none());
    }

    #[test]
    fn rolls_segments_and_compacts() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SegmentStore::open(dir.path()).unwrap();
        store.set_roll_bytes(256);

        let mut hashes = Vec::new();
        for i in 0..20u8 {
            hashes.push(put(&mut store, &[i; 100]));
        }
        assert!(store.segment_count() > 1);
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(store.read(h).unwrap().unwrap(), vec![i as u8; 100]);
        }

        // Kill most chunks, compact, survivors stay readable.
        for h in &hashes[..16] {
            store.mark_dead(h);
        }
        store.compact().unwrap();
        assert_eq!(store.dead_bytes(), 0);
        for (i, h) in hashes.iter().enumerate().skip(16) {
            assert_eq!(store.read(h).unwrap().unwrap(), vec![i as u8; 100]);
        }
        // Dead chunks are gone from the index after their segment compacts.
        assert!(!store.contains_live(&hashes[0]));

        // Reopen: everything still consistent.
        drop(store);
        let store = SegmentStore::open(dir.path()).unwrap();
        for (i, h) in hashes.iter().enumerate().skip(16) {
            assert_eq!(store.read(h).unwrap().unwrap(), vec![i as u8; 100]);
        }
    }

    #[test]
    fn revive_dead_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SegmentStore::open(dir.path()).unwrap();
        let h = put(&mut store, b"flip flop");
        store.mark_dead(&h);
        assert!(!store.contains_live(&h));
        assert!(store.dead_bytes() > 0);
        assert!(store.revive(&h));
        assert!(store.contains_live(&h));
        assert_eq!(store.dead_bytes(), 0);
        assert_eq!(store.read(&h).unwrap().unwrap(), b"flip flop");
    }

    #[test]
    fn empty_chunk_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SegmentStore::open(dir.path()).unwrap();
        let h = put(&mut store, b"");
        assert_eq!(store.read(&h).unwrap().unwrap(), Vec::<u8>::new());
        drop(store);
        let store = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(store.read(&h).unwrap().unwrap(), Vec::<u8>::new());
    }

    /// BLO-1 dirty-state machine: an append leaves the segment dirty until an
    /// explicit sync fsyncs it, and a sync with nothing pending is clean.
    #[test]
    fn append_marks_dirty_sync_cleans() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SegmentStore::open(dir.path()).unwrap();
        assert!(!store.is_dirty(), "a freshly opened store is clean");
        put(&mut store, b"some bytes");
        assert!(
            store.is_dirty(),
            "an appended chunk is flushed to the OS but not yet fsynced"
        );
        store.sync().unwrap();
        assert!(!store.is_dirty(), "sync drains the dirty set");
        // A redundant sync stays clean and errors nowhere.
        store.sync().unwrap();
        assert!(!store.is_dirty());
    }

    /// BLO-1 compaction half: compaction must leave the store clean (the
    /// rewritten copies are synced BEFORE the victim file is deleted — power
    /// loss in between loses the chunk entirely) and the survivors readable.
    #[test]
    fn compact_syncs_rewrites_and_leaves_clean() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SegmentStore::open(dir.path()).unwrap();
        store.set_roll_bytes(256);

        let mut hashes = Vec::new();
        for i in 0..20u8 {
            hashes.push(put(&mut store, &[i; 100]));
        }
        assert!(store.segment_count() > 1);
        store.sync().unwrap();
        assert!(!store.is_dirty());

        for h in &hashes[..16] {
            store.mark_dead(h);
        }
        store.compact().unwrap();
        assert!(
            !store.is_dirty(),
            "compaction syncs its rewrites before deleting the victim files"
        );
        assert_eq!(store.dead_bytes(), 0);
        for (i, h) in hashes.iter().enumerate().skip(16) {
            assert_eq!(
                store.read(h).unwrap().unwrap(),
                vec![i as u8; 100],
                "rewritten survivor must stay readable"
            );
        }
    }
}
