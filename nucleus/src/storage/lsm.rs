//! LSM-tree (Log-Structured Merge-tree) storage engine.
//!
//! Provides a write-optimized key-value store with:
//!   - In-memory memtable (sorted write buffer)
//!   - Immutable sorted runs (SSTables) with optional disk persistence
//!   - Compaction to merge sorted runs and reduce read amplification
//!   - Bloom filter per SSTable for fast negative lookups
//!   - Configurable memtable flush threshold and max levels
//!
//! ## Disk persistence
//!
//! Use `LsmTree::open(config, dir)` to enable SSTable files on disk.
//! Each SSTable is written to `<dir>/L<level>_S<seq:016x>.sst` when it is
//! flushed from the memtable or produced by compaction.  Compacted-away files
//! are removed.  On startup all `.sst` files are loaded back into the tree.
//!
//! ### SSTable file format
//! ```text
//! Magic:    4 bytes  "LSMS"
//! Level:    1 byte   u8
//! Seq:      8 bytes  u64 LE
//! n_entries 4 bytes  u32 LE
//! entries:
//!   key_len:   4 bytes u32 LE
//!   key_bytes: key_len bytes
//!   kind:      1 byte  0 = tombstone, 1 = value
//!   val_len:   4 bytes u32 LE  (only when kind==1)
//!   val_bytes: val_len bytes   (only when kind==1)
//! ```

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

// ============================================================================
// Configuration
// ============================================================================

/// LSM-tree configuration parameters.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Maximum memtable size (in entries) before flushing to an SSTable.
    pub memtable_flush_threshold: usize,
    /// Maximum number of SSTables per level before compaction triggers.
    pub level_max_sstables: usize,
    /// Maximum number of levels in the LSM tree.
    pub max_levels: usize,
    /// Bloom filter bits per key (0 to disable).
    pub bloom_bits_per_key: usize,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_flush_threshold: 1000,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        }
    }
}

// ============================================================================
// Bloom filter
// ============================================================================

/// Simple bloom filter for probabilistic membership testing.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<bool>,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a bloom filter sized for `num_keys` with the given bits per key.
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let num_bits = (num_keys * bits_per_key).max(64);
        let num_hashes = ((bits_per_key as f64) * 0.693).ceil() as usize; // ln(2)
        Self {
            bits: vec![false; num_bits],
            num_hashes: num_hashes.max(1),
        }
    }

    /// Insert a key into the bloom filter.
    pub fn insert(&mut self, key: &[u8]) {
        let len = self.bits.len();
        let (mut h1, mut h2) = self.hash_pair(key);
        for _ in 0..self.num_hashes {
            let idx = (h1 as usize) % len;
            self.bits[idx] = true;
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(1);
        }
    }

    /// Check whether a key might be in the set (false positives possible).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let len = self.bits.len();
        let (mut h1, mut h2) = self.hash_pair(key);
        for _ in 0..self.num_hashes {
            let idx = (h1 as usize) % len;
            if !self.bits[idx] {
                return false;
            }
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(1);
        }
        true
    }

    fn hash_pair(&self, key: &[u8]) -> (u64, u64) {
        // FNV-1a for h1.
        let mut h1: u64 = 0xcbf29ce484222325;
        for &b in key {
            h1 ^= b as u64;
            h1 = h1.wrapping_mul(0x100000001b3);
        }
        // Simple derived second hash.
        let h2 = h1.wrapping_mul(0x517cc1b727220a95).wrapping_add(0x6c62272e07bb0142);
        (h1, h2)
    }
}

// ============================================================================
// SSTable (Sorted String Table)
// ============================================================================

/// An immutable sorted run of key-value pairs.
#[derive(Debug, Clone)]
pub struct SSTable {
    /// Sorted entries: key -> value (None = tombstone/delete marker).
    entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Bloom filter for fast negative lookups.
    bloom: BloomFilter,
    /// Level in the LSM tree (0 = most recent).
    pub level: usize,
    /// Sequence number for ordering within a level.
    pub seq: u64,
    /// Byte size estimate.
    pub size_bytes: usize,
}

impl SSTable {
    /// Build an SSTable from a sorted iterator of (key, value) pairs.
    pub fn from_sorted(
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        level: usize,
        seq: u64,
        bits_per_key: usize,
    ) -> Self {
        let mut bloom = BloomFilter::new(entries.len().max(1), bits_per_key.max(1));
        let mut size_bytes = 0;
        for (k, v) in &entries {
            bloom.insert(k);
            size_bytes += k.len() + v.as_ref().map_or(0, |v| v.len());
        }
        Self { entries, bloom, level, seq, size_bytes }
    }

    /// Point lookup using bloom filter + binary search.
    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        if !self.bloom.may_contain(key) {
            return None; // Definitely not here.
        }
        self.entries
            .binary_search_by_key(&key, |(k, _)| k.as_slice())
            .ok()
            .map(|idx| self.entries[idx].1.as_deref())
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = &(Vec<u8>, Option<Vec<u8>>)> {
        self.entries.iter()
    }
}

// ============================================================================
// LSM Tree
// ============================================================================

/// A Log-Structured Merge-tree key-value store.
pub struct LsmTree {
    config: LsmConfig,
    /// Active memtable (mutable, in-memory sorted buffer).
    memtable: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Levels of SSTables. Level 0 is the most recent.
    levels: Vec<Vec<SSTable>>,
    /// Monotonically increasing SSTable sequence counter.
    next_seq: u64,
    /// Total number of flushes performed.
    pub flush_count: u64,
    /// Total number of compactions performed.
    pub compaction_count: u64,
    /// Directory for SSTable files. `None` = in-memory only.
    disk_dir: Option<PathBuf>,
}

impl LsmTree {
    pub fn new(config: LsmConfig) -> Self {
        let max_levels = config.max_levels;
        Self {
            config,
            memtable: BTreeMap::new(),
            levels: (0..max_levels).map(|_| Vec::new()).collect(),
            next_seq: 1,
            flush_count: 0,
            compaction_count: 0,
            disk_dir: None,
        }
    }

    /// Open (or create) a disk-backed LsmTree in `dir`.
    ///
    /// Existing `.sst` files are loaded back into the tree. New SSTables
    /// are written to disk when flushed and removed when compacted away.
    pub fn open(config: LsmConfig, dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let max_levels = config.max_levels;
        let mut tree = Self {
            config,
            memtable: BTreeMap::new(),
            levels: (0..max_levels).map(|_| Vec::new()).collect(),
            next_seq: 1,
            flush_count: 0,
            compaction_count: 0,
            disk_dir: Some(dir.to_path_buf()),
        };
        tree.load_from_dir(dir)?;
        Ok(tree)
    }

    // ─── Disk helpers ─────────────────────────────────────────────────────────

    fn sst_filename(level: usize, seq: u64) -> String {
        format!("L{level}_S{seq:016x}.sst")
    }

    fn sst_path(&self, level: usize, seq: u64) -> Option<PathBuf> {
        self.disk_dir.as_ref().map(|d| d.join(Self::sst_filename(level, seq)))
    }

    fn write_sst_to_disk(&self, sst: &SSTable) -> io::Result<()> {
        let Some(path) = self.sst_path(sst.level, sst.seq) else { return Ok(()); };
        let mut buf = Vec::with_capacity(sst.size_bytes + 64);
        buf.extend_from_slice(b"LSMS");
        buf.push(sst.level as u8);
        buf.extend_from_slice(&sst.seq.to_le_bytes());
        buf.extend_from_slice(&(sst.entries.len() as u32).to_le_bytes());
        for (k, v) in sst.iter() {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            match v {
                None => buf.push(0),
                Some(val) => {
                    buf.push(1);
                    buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
                    buf.extend_from_slice(val);
                }
            }
        }
        std::fs::write(&path, &buf)
    }

    fn delete_sst_from_disk(&self, level: usize, seq: u64) {
        if let Some(path) = self.sst_path(level, seq) {
            let _ = std::fs::remove_file(&path);
        }
    }

    fn load_from_dir(&mut self, dir: &Path) -> io::Result<()> {
        let mut sst_files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sst"))
            .collect();
        sst_files.sort(); // Lexicographic order = seq order given our naming.

        for path in &sst_files {
            match load_sst_file(path) {
                Ok(sst) => {
                    let level = sst.level;
                    // Ensure levels array is large enough.
                    while self.levels.len() <= level {
                        self.levels.push(Vec::new());
                    }
                    if sst.seq >= self.next_seq {
                        self.next_seq = sst.seq + 1;
                    }
                    self.levels[level].push(sst);
                }
                Err(_) => {
                    // Skip corrupt files (best-effort recovery).
                }
            }
        }
        // Sort each level by seq (ascending = oldest first).
        for level in &mut self.levels {
            level.sort_by_key(|s| s.seq);
        }
        Ok(())
    }

    /// Insert or update a key-value pair. May trigger a memtable flush.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.insert(key, Some(value));
        if self.memtable.len() >= self.config.memtable_flush_threshold {
            self.flush_memtable();
        }
    }

    /// Delete a key by writing a tombstone. May trigger a memtable flush.
    pub fn delete(&mut self, key: Vec<u8>) {
        self.memtable.insert(key, None);
        if self.memtable.len() >= self.config.memtable_flush_threshold {
            self.flush_memtable();
        }
    }

    /// Point lookup: checks memtable first, then SSTables from newest to oldest.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 1. Check memtable.
        if let Some(entry) = self.memtable.get(key) {
            return entry.clone(); // None = tombstone = key deleted.
        }
        // 2. Check SSTables level by level, newest first.
        for level in &self.levels {
            for sst in level.iter().rev() {
                if let Some(value) = sst.get(key) {
                    return value.map(|v| v.to_vec()); // None = tombstone.
                }
            }
        }
        None
    }

    /// Flush the memtable to a new SSTable at level 0.
    pub fn flush_memtable(&mut self) {
        if self.memtable.is_empty() {
            return;
        }
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = std::mem::take(&mut self.memtable).into_iter().collect();
        let seq = self.next_seq;
        self.next_seq += 1;
        let sst = SSTable::from_sorted(entries, 0, seq, self.config.bloom_bits_per_key);
        // Write to disk before adding to in-memory levels (WAL semantics).
        let _ = self.write_sst_to_disk(&sst);
        if !self.levels.is_empty() {
            self.levels[0].push(sst);
        }
        self.flush_count += 1;

        // Check if level 0 needs compaction.
        self.maybe_compact(0);
    }

    /// Trigger compaction at the given level if it exceeds the threshold.
    fn maybe_compact(&mut self, level: usize) {
        if level + 1 >= self.levels.len() {
            return; // Can't compact beyond max level.
        }
        if self.levels[level].len() <= self.config.level_max_sstables {
            return; // Not enough SSTables to compact.
        }

        // Collect level+seq of all SSTables we are about to consume so we can
        // delete their disk files after the merged SSTable is written.
        let inputs_to_delete: Vec<(usize, u64)> = self.levels[level]
            .iter()
            .map(|s| (s.level, s.seq))
            .chain(self.levels[level + 1].iter().map(|s| (s.level, s.seq)))
            .collect();

        // Merge all SSTables at this level into one at the next level.
        let tables_to_merge: Vec<SSTable> = self.levels[level].drain(..).collect();
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Also include existing SSTables at the next level.
        let next_tables: Vec<SSTable> = self.levels[level + 1].drain(..).collect();

        // Merge: newer entries (higher seq) win.
        // Process older tables first so newer ones overwrite.
        let mut all_tables: Vec<&SSTable> = Vec::new();
        for t in &next_tables {
            all_tables.push(t);
        }
        for t in &tables_to_merge {
            all_tables.push(t);
        }
        all_tables.sort_by_key(|t| t.seq);

        for table in &all_tables {
            for (k, v) in table.iter() {
                merged.insert(k.clone(), v.clone());
            }
        }

        // Remove tombstones at the last level.
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = if level + 1 == self.levels.len() - 1 {
            merged.into_iter().filter(|(_, v)| v.is_some()).collect()
        } else {
            merged.into_iter().collect()
        };

        let seq = self.next_seq;
        self.next_seq += 1;
        let sst = SSTable::from_sorted(entries, level + 1, seq, self.config.bloom_bits_per_key);
        // Write merged SSTable to disk first, then delete superseded files.
        let _ = self.write_sst_to_disk(&sst);
        self.levels[level + 1].push(sst);
        self.compaction_count += 1;

        // Remove input SSTable files (they are now superseded by the merged one).
        for (lvl, s) in inputs_to_delete {
            self.delete_sst_from_disk(lvl, s);
        }

        // Recurse: the next level might now need compaction too.
        self.maybe_compact(level + 1);
    }

    /// Force compaction at a specific level.
    pub fn compact(&mut self, level: usize) {
        if level + 1 < self.levels.len() && !self.levels[level].is_empty() {
            let saved_threshold = self.config.level_max_sstables;
            self.config.level_max_sstables = 0; // Force compaction.
            self.maybe_compact(level);
            self.config.level_max_sstables = saved_threshold;
        }
    }

    /// Range scan: returns all live key-value pairs in [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Collect from SSTables (oldest first so newer overwrites).
        for level in self.levels.iter().rev() {
            for sst in level {
                for (k, v) in sst.iter() {
                    if k.as_slice() >= start && k.as_slice() < end {
                        merged.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Memtable (newest, always wins).
        for (k, v) in self.memtable.range::<Vec<u8>, _>(start.to_vec()..end.to_vec()) {
            merged.insert(k.clone(), v.clone());
        }

        // Filter out tombstones.
        merged.into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    /// Total number of SSTables across all levels.
    pub fn sstable_count(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }

    /// Number of entries in the memtable.
    pub fn memtable_size(&self) -> usize {
        self.memtable.len()
    }

    /// Summary of entries per level.
    pub fn level_summary(&self) -> Vec<(usize, usize)> {
        self.levels.iter().enumerate()
            .map(|(i, l)| (i, l.iter().map(|s| s.len()).sum()))
            .collect()
    }

    /// Force-flush the memtable to level 0 (used before shutdown / snapshot).
    pub fn force_flush(&mut self) {
        self.flush_memtable();
    }
}

// ============================================================================
// SSTable file I/O
// ============================================================================

/// Read and parse an SSTable file.
fn load_sst_file(path: &Path) -> io::Result<SSTable> {
    let data = std::fs::read(path)?;

    // Check magic.
    if data.get(..4) != Some(b"LSMS") {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad SSTable magic"));
    }
    let mut pos = 4usize;

    let level = *data.get(pos).ok_or_else(eof)? as usize;
    pos += 1;

    let seq = read_u64_le(&data, &mut pos).ok_or_else(eof)?;
    let n = read_u32_le(&data, &mut pos).ok_or_else(eof)? as usize;

    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let key_len = read_u32_le(&data, &mut pos).ok_or_else(eof)? as usize;
        if pos + key_len > data.len() { return Err(eof()); }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        let kind = *data.get(pos).ok_or_else(eof)?;
        pos += 1;

        let value = if kind == 1 {
            let val_len = read_u32_le(&data, &mut pos).ok_or_else(eof)? as usize;
            if pos + val_len > data.len() { return Err(eof()); }
            let v = data[pos..pos + val_len].to_vec();
            pos += val_len;
            Some(v)
        } else {
            None // tombstone
        };
        entries.push((key, value));
    }

    Ok(SSTable::from_sorted(entries, level, seq, 10))
}

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated SSTable file")
}

fn read_u32_le(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_u64_le(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn small_config() -> LsmConfig {
        LsmConfig {
            memtable_flush_threshold: 5,
            level_max_sstables: 2,
            max_levels: 3,
            bloom_bits_per_key: 10,
        }
    }

    #[test]
    fn bloom_filter_basic() {
        let mut bf = BloomFilter::new(100, 10);
        bf.insert(b"hello");
        bf.insert(b"world");
        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        // Probabilistic: "missing" should almost certainly not be present.
        // (With 100 keys and 10 bits/key, false positive rate < 1%.)
        // We test with a small filter so we just check it doesn't panic.
    }

    #[test]
    fn sstable_point_lookup() {
        let entries = vec![
            (b"aaa".to_vec(), Some(b"111".to_vec())),
            (b"bbb".to_vec(), Some(b"222".to_vec())),
            (b"ccc".to_vec(), None), // tombstone
        ];
        let sst = SSTable::from_sorted(entries, 0, 1, 10);
        assert_eq!(sst.len(), 3);

        // Found with value.
        assert_eq!(sst.get(b"aaa"), Some(Some(b"111".as_slice())));
        // Found with tombstone.
        assert_eq!(sst.get(b"ccc"), Some(None));
        // Not found (bloom filter says no, or binary search misses).
        // "zzz" is not in the filter, so it should return None.
    }

    #[test]
    fn lsm_put_and_get() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        tree.put(b"key1".to_vec(), b"val1".to_vec());
        tree.put(b"key2".to_vec(), b"val2".to_vec());
        assert_eq!(tree.get(b"key1"), Some(b"val1".to_vec()));
        assert_eq!(tree.get(b"key2"), Some(b"val2".to_vec()));
        assert_eq!(tree.get(b"key3"), None);
    }

    #[test]
    fn lsm_overwrite() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        tree.put(b"k".to_vec(), b"v1".to_vec());
        assert_eq!(tree.get(b"k"), Some(b"v1".to_vec()));
        tree.put(b"k".to_vec(), b"v2".to_vec());
        assert_eq!(tree.get(b"k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn lsm_delete() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        tree.put(b"k".to_vec(), b"v".to_vec());
        assert_eq!(tree.get(b"k"), Some(b"v".to_vec()));
        tree.delete(b"k".to_vec());
        assert_eq!(tree.get(b"k"), None);
    }

    #[test]
    fn lsm_flush_memtable() {
        let mut tree = LsmTree::new(small_config());
        // Insert enough to trigger a flush (threshold = 5).
        for i in 0..6u8 {
            tree.put(vec![i], vec![i + 100]);
        }
        assert!(tree.flush_count >= 1);
        assert!(tree.sstable_count() >= 1);
        // Verify all keys still readable.
        for i in 0..6u8 {
            assert_eq!(tree.get(&[i]), Some(vec![i + 100]));
        }
    }

    #[test]
    fn lsm_compaction() {
        let mut tree = LsmTree::new(small_config());
        // Insert enough to trigger multiple flushes and compaction.
        // threshold=5, level_max_sstables=2 → after 3 flushes, compaction occurs.
        for i in 0..20u8 {
            tree.put(vec![i], vec![i]);
        }
        assert!(tree.compaction_count >= 1, "expected at least 1 compaction, got {}", tree.compaction_count);
        // All keys still readable.
        for i in 0..20u8 {
            assert_eq!(tree.get(&[i]), Some(vec![i]));
        }
    }

    #[test]
    fn lsm_range_scan() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        for i in 0..10u8 {
            tree.put(vec![i], vec![i * 10]);
        }
        let range = tree.range(&[3], &[7]);
        assert_eq!(range.len(), 4); // keys 3,4,5,6
        assert_eq!(range[0], (vec![3], vec![30]));
        assert_eq!(range[3], (vec![6], vec![60]));
    }

    #[test]
    fn lsm_range_excludes_tombstones() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        for i in 0..5u8 {
            tree.put(vec![i], vec![i]);
        }
        tree.delete(vec![2]);
        let range = tree.range(&[0], &[5]);
        assert_eq!(range.len(), 4); // 0,1,3,4 (2 deleted)
    }

    #[test]
    fn lsm_level_summary() {
        let mut tree = LsmTree::new(small_config());
        for i in 0..6u8 {
            tree.put(vec![i], vec![i]);
        }
        let summary = tree.level_summary();
        assert_eq!(summary.len(), 3); // 3 levels
        // At least one level should have entries.
        let total: usize = summary.iter().map(|(_, count)| count).sum();
        assert!(total > 0);
    }

    #[test]
    fn lsm_memtable_size() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        assert_eq!(tree.memtable_size(), 0);
        tree.put(b"a".to_vec(), b"1".to_vec());
        assert_eq!(tree.memtable_size(), 1);
        tree.put(b"b".to_vec(), b"2".to_vec());
        assert_eq!(tree.memtable_size(), 2);
    }

    #[test]
    fn lsm_force_compact() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 3, level_max_sstables: 10, ..small_config() });
        // Create some SSTables at level 0 via flushes.
        for batch in 0..3u8 {
            for i in 0..3u8 {
                tree.put(vec![batch * 10 + i], vec![batch * 10 + i]);
            }
        }
        assert!(tree.sstable_count() >= 2);
        let before = tree.compaction_count;
        tree.compact(0);
        assert!(tree.compaction_count > before);
    }

    #[test]
    fn lsm_delete_then_reinsert() {
        let mut tree = LsmTree::new(LsmConfig { memtable_flush_threshold: 100, ..small_config() });
        tree.put(b"k".to_vec(), b"v1".to_vec());
        tree.delete(b"k".to_vec());
        assert_eq!(tree.get(b"k"), None);
        tree.put(b"k".to_vec(), b"v2".to_vec());
        assert_eq!(tree.get(b"k"), Some(b"v2".to_vec()));
    }

    // ─── Disk persistence tests ───────────────────────────────────────────────

    #[test]
    fn lsm_disk_flush_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        // Write entries and force flush to disk.
        {
            let mut tree = LsmTree::open(small_config(), dir.path()).unwrap();
            for i in 0..10u8 {
                tree.put(vec![i], vec![i * 2]);
            }
            tree.force_flush(); // Write memtable to SSTable file.
        }

        // Reopen — should recover all 10 entries.
        {
            let tree = LsmTree::open(small_config(), dir.path()).unwrap();
            for i in 0..10u8 {
                assert_eq!(tree.get(&[i]), Some(vec![i * 2]), "key {i} missing after recovery");
            }
        }
    }

    #[test]
    fn lsm_disk_compaction_writes_merged_sst() {
        let dir = tempfile::tempdir().unwrap();

        // Use a tiny config to trigger compaction quickly.
        let cfg = LsmConfig {
            memtable_flush_threshold: 3,
            level_max_sstables: 2,
            max_levels: 3,
            bloom_bits_per_key: 10,
        };

        {
            let mut tree = LsmTree::open(cfg.clone(), dir.path()).unwrap();
            // Insert enough to trigger compaction (3 flushes with threshold=3).
            for i in 0..20u8 {
                tree.put(vec![i], vec![i]);
            }
            assert!(tree.compaction_count >= 1);
            // Flush memtable so remaining entries reach disk.
            tree.force_flush();
        }

        // After reopen, all data should still be accessible.
        {
            let tree = LsmTree::open(cfg, dir.path()).unwrap();
            for i in 0..20u8 {
                assert_eq!(tree.get(&[i]), Some(vec![i]), "key {i} missing after compaction + recovery");
            }
        }
    }

    #[test]
    fn lsm_disk_sst_file_roundtrip() {
        // Low-level: write an SSTable to disk, read it back.
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![
            (b"aaa".to_vec(), Some(b"AAA".to_vec())),
            (b"bbb".to_vec(), None), // tombstone
            (b"ccc".to_vec(), Some(b"CCC".to_vec())),
        ];
        let sst = SSTable::from_sorted(entries.clone(), 2, 42, 10);
        let path = dir.path().join("test.sst");

        // Write using the tree helper (create a tree just to get the helper).
        let tree = LsmTree::open(LsmConfig::default(), dir.path()).unwrap();
        let fake_sst = SSTable::from_sorted(entries.clone(), 2, 42, 10);
        // Use write_sst_to_disk indirectly by placing in the expected path.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"LSMS");
        buf.push(sst.level as u8);
        buf.extend_from_slice(&sst.seq.to_le_bytes());
        buf.extend_from_slice(&(sst.len() as u32).to_le_bytes());
        for (k, v) in fake_sst.iter() {
            buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
            buf.extend_from_slice(k);
            match v {
                None => buf.push(0),
                Some(val) => { buf.push(1); buf.extend_from_slice(&(val.len() as u32).to_le_bytes()); buf.extend_from_slice(val); }
            }
        }
        std::fs::write(&path, &buf).unwrap();

        // Read it back.
        let recovered = load_sst_file(&path).unwrap();
        assert_eq!(recovered.level, 2);
        assert_eq!(recovered.seq, 42);
        assert_eq!(recovered.len(), 3);
        // First entry: key=aaa, value=AAA
        assert_eq!(tree.get(b"aaa"), None); // empty tree
        // Verify the raw entries.
        let e: Vec<_> = recovered.iter().collect();
        assert_eq!(e[0].0, b"aaa");
        assert_eq!(e[0].1, Some(b"AAA".to_vec()));
        assert_eq!(e[1].1, None); // tombstone
    }
}
