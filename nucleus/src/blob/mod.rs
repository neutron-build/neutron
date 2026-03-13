//! Large object (blob) storage with content-addressable deduplication.
//!
//! Supports:
//!   - Chunked storage for multi-GB objects
//!   - Content-addressable deduplication (same data stored once)
//!   - Streaming reads/writes without loading entire object into memory
//!   - Metadata and tagging on blobs
//!   - BLAKE3 cryptographic hashing for content addressing
//!   - Byte-range index for O(log N) range access
//!   - Optional WAL-backed durability (via `BlobStore::open`)
//!
//! Replaces S3, GCS, MinIO for blob storage within Nucleus.

pub mod wal;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use wal::{BlobStoreSnapshot, BlobWal};

// ============================================================================
// Content-addressable chunk store (BLAKE3)
// ============================================================================

/// Hash of a chunk's content — 32 bytes of BLAKE3 output.
pub type ChunkHash = [u8; 32];

/// A chunk of data with its content hash.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub hash: ChunkHash,
    pub data: Vec<u8>,
}

/// Compute a BLAKE3 content hash for a data slice, returning 32 bytes.
pub fn content_hash_blake3(data: &[u8]) -> ChunkHash {
    *blake3::hash(data).as_bytes()
}

/// Legacy compatibility: compute a content hash and return it as a `u64`.
///
/// This wraps BLAKE3 internally and truncates the output to 8 bytes so that
/// callers that format the result with `{:016x}` continue to work.
pub fn content_hash(data: &[u8]) -> u64 {
    let full = content_hash_blake3(data);
    u64::from_le_bytes([
        full[0], full[1], full[2], full[3], full[4], full[5], full[6], full[7],
    ])
}

/// Content-addressable chunk store — deduplicates identical chunks via BLAKE3.
#[derive(Clone)]
pub struct ChunkStore {
    /// hash -> chunk data
    chunks: HashMap<ChunkHash, Vec<u8>>,
    /// Total bytes stored (deduplicated).
    stored_bytes: usize,
}

impl Default for ChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkStore {
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
            stored_bytes: 0,
        }
    }

    /// Store a chunk. Returns the BLAKE3 hash. If already stored, deduplicates.
    pub fn put(&mut self, data: Vec<u8>) -> ChunkHash {
        let hash = content_hash_blake3(&data);
        if !self.chunks.contains_key(&hash) {
            self.stored_bytes += data.len();
            self.chunks.insert(hash, data);
        }
        hash
    }

    /// Insert a chunk with a known hash (used during WAL recovery).
    fn put_with_hash(&mut self, hash: ChunkHash, data: Vec<u8>) {
        if !self.chunks.contains_key(&hash) {
            self.stored_bytes += data.len();
            self.chunks.insert(hash, data);
        }
    }

    /// Get a chunk by hash.
    pub fn get(&self, hash: &ChunkHash) -> Option<&[u8]> {
        self.chunks.get(hash).map(|v| v.as_slice())
    }

    /// Check if a chunk exists.
    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.chunks.contains_key(hash)
    }

    /// Total deduplicated bytes stored.
    pub fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    /// Number of unique chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

// ============================================================================
// Byte-range index
// ============================================================================

/// Per-blob offset table for O(log N) byte-range access.
///
/// Each entry stores `(cumulative_byte_offset, chunk_size)` so that a binary
/// search can locate the first chunk that covers a given byte offset.
#[derive(Debug, Clone)]
pub struct BlobIndex {
    /// Per-chunk: (cumulative_byte_offset_at_start_of_chunk, chunk_size)
    offsets: Vec<(u64, usize)>,
}

impl BlobIndex {
    /// Build the index from a sequence of chunk sizes.
    pub fn build(chunk_sizes: &[usize]) -> Self {
        let mut offsets = Vec::with_capacity(chunk_sizes.len());
        let mut cumulative = 0u64;
        for &size in chunk_sizes {
            offsets.push((cumulative, size));
            cumulative += size as u64;
        }
        Self { offsets }
    }

    /// Find the index of the first chunk that contains byte `offset`.
    /// Returns `None` if `offset` is beyond the total size.
    pub fn find_chunk(&self, offset: u64) -> Option<usize> {
        if self.offsets.is_empty() {
            return None;
        }
        // Binary search: find the last chunk whose cumulative offset <= target
        let idx = self
            .offsets
            .partition_point(|(cum, _)| *cum <= offset);
        if idx == 0 {
            // offset is before the first chunk start — it IS the first chunk
            Some(0)
        } else {
            Some(idx - 1)
        }
    }

    /// Total size covered by all chunks.
    pub fn total_size(&self) -> u64 {
        self.offsets
            .last()
            .map(|(cum, sz)| cum + *sz as u64)
            .unwrap_or(0)
    }
}

// ============================================================================
// Blob metadata and manifest
// ============================================================================

/// Metadata about a stored blob.
#[derive(Debug, Clone)]
pub struct BlobMetadata {
    pub key: String,
    pub size: u64,
    pub chunk_size: usize,
    pub chunk_hashes: Vec<ChunkHash>,
    pub content_type: Option<String>,
    pub tags: HashMap<String, String>,
    pub created_at: u64,
    pub updated_at: u64,
    /// Byte-range index for O(log N) range reads.
    pub index: BlobIndex,
}

// ============================================================================
// Blob store
// ============================================================================

/// Default chunk size: 1 MB.
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Blob store — manages large objects as chunked, deduplicated data.
///
/// When opened with `BlobStore::open(dir)`, all mutations are logged to a WAL
/// for crash recovery. The in-memory-only constructor `BlobStore::new()` is
/// retained for backward compatibility and testing.
pub struct BlobStore {
    chunks: ChunkStore,
    /// key -> blob metadata
    blobs: HashMap<String, BlobMetadata>,
    chunk_size: usize,
    /// Optional WAL for durability.
    wal: Option<Arc<BlobWal>>,
}

impl Default for BlobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobStore {
    /// Create an in-memory-only blob store (no durability).
    pub fn new() -> Self {
        Self::with_chunk_size(DEFAULT_CHUNK_SIZE)
    }

    /// Create an in-memory-only blob store with a custom chunk size.
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self {
            chunks: ChunkStore::new(),
            blobs: HashMap::new(),
            chunk_size,
            wal: None,
        }
    }

    /// Open a WAL-backed blob store at `dir`.
    ///
    /// Replays the WAL to recover previous state, then appends new mutations.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        Self::open_with_chunk_size(dir, DEFAULT_CHUNK_SIZE)
    }

    /// Open a WAL-backed blob store with a custom chunk size.
    pub fn open_with_chunk_size(dir: &Path, chunk_size: usize) -> std::io::Result<Self> {
        let (wal, state) = BlobWal::open(dir)?;
        let wal = Arc::new(wal);

        let mut chunks = ChunkStore::new();
        let mut blobs = HashMap::new();

        for (id, entry) in state.blobs {
            let mut chunk_hashes = Vec::with_capacity(entry.chunks.len());
            let mut chunk_sizes = Vec::with_capacity(entry.chunks.len());
            for (hash, data) in &entry.chunks {
                chunk_sizes.push(data.len());
                chunk_hashes.push(*hash);
                chunks.put_with_hash(*hash, data.clone());
            }
            let index = BlobIndex::build(&chunk_sizes);
            let meta = BlobMetadata {
                key: id.clone(),
                size: entry.total_size,
                chunk_size,
                chunk_hashes,
                content_type: entry.content_type,
                tags: entry.tags,
                created_at: 0,
                updated_at: 0,
                index,
            };
            blobs.insert(id, meta);
        }

        Ok(Self {
            chunks,
            blobs,
            chunk_size,
            wal: Some(wal),
        })
    }

    /// Store a blob. Splits into chunks and deduplicates.
    pub fn put(&mut self, key: &str, data: &[u8], content_type: Option<&str>) {
        let mut chunk_hashes = Vec::new();
        let mut wal_chunks: Vec<([u8; 32], Vec<u8>)> = Vec::new();
        let mut chunk_sizes = Vec::new();

        for chunk_data in data.chunks(self.chunk_size) {
            let hash = self.chunks.put(chunk_data.to_vec());
            chunk_hashes.push(hash);
            chunk_sizes.push(chunk_data.len());
            wal_chunks.push((hash, chunk_data.to_vec()));
        }

        // Handle empty data
        if data.is_empty() {
            let hash = self.chunks.put(Vec::new());
            chunk_hashes.push(hash);
            chunk_sizes.push(0);
            wal_chunks.push((hash, Vec::new()));
        }

        // Log to WAL before in-memory mutation
        if let Some(wal) = &self.wal
            && let Err(e) = wal.log_store(key, content_type, data.len() as u64, &wal_chunks) {
                eprintln!("blob WAL: failed to log store for '{key}': {e}");
            }

        let index = BlobIndex::build(&chunk_sizes);

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let meta = BlobMetadata {
            key: key.to_string(),
            size: data.len() as u64,
            chunk_size: self.chunk_size,
            chunk_hashes,
            content_type: content_type.map(|s| s.to_string()),
            tags: HashMap::new(),
            created_at: ts,
            updated_at: ts,
            index,
        };

        self.blobs.insert(key.to_string(), meta);
    }

    /// Read an entire blob.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let meta = self.blobs.get(key)?;
        let mut data = Vec::with_capacity(meta.size as usize);
        for hash in &meta.chunk_hashes {
            if let Some(chunk) = self.chunks.get(hash) {
                data.extend_from_slice(chunk);
            }
        }
        Some(data)
    }

    /// Read a byte range from a blob using the BlobIndex for O(log N) lookup.
    pub fn get_range(&self, key: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let meta = self.blobs.get(key)?;

        let start = offset;
        let end = offset + length;

        // Use the index to find the starting chunk via binary search
        let start_chunk_idx = meta.index.find_chunk(start).unwrap_or(0);

        let mut data = Vec::new();
        let mut pos = if start_chunk_idx < meta.index.offsets.len() {
            meta.index.offsets[start_chunk_idx].0
        } else {
            return Some(data);
        };

        for hash in &meta.chunk_hashes[start_chunk_idx..] {
            let chunk = self.chunks.get(hash)?;
            let chunk_end = pos + chunk.len() as u64;

            if chunk_end > start && pos < end {
                let chunk_start = (start.saturating_sub(pos)) as usize;
                let chunk_stop = if chunk_end > end {
                    (end - pos) as usize
                } else {
                    chunk.len()
                };
                data.extend_from_slice(&chunk[chunk_start..chunk_stop]);
            }

            pos = chunk_end;
            if pos >= end {
                break;
            }
        }

        Some(data)
    }

    /// Delete a blob (metadata only -- chunks may still be referenced by other blobs).
    pub fn delete(&mut self, key: &str) -> bool {
        // Log to WAL before in-memory mutation
        if let Some(wal) = &self.wal
            && let Err(e) = wal.log_delete(key) {
                eprintln!("blob WAL: failed to log delete for '{key}': {e}");
            }
        self.blobs.remove(key).is_some()
    }

    /// Get blob metadata.
    pub fn metadata(&self, key: &str) -> Option<&BlobMetadata> {
        self.blobs.get(key)
    }

    /// Set a tag on a blob.
    pub fn set_tag(&mut self, key: &str, tag_key: &str, tag_value: &str) -> bool {
        if let Some(meta) = self.blobs.get_mut(key) {
            // Log to WAL before in-memory mutation
            if let Some(wal) = &self.wal
                && let Err(e) = wal.log_tag(key, tag_key, tag_value) {
                    eprintln!("blob WAL: failed to log tag for '{key}': {e}");
                }
            meta.tags
                .insert(tag_key.to_string(), tag_value.to_string());
            true
        } else {
            false
        }
    }

    /// List all blob keys.
    pub fn list_keys(&self) -> Vec<&str> {
        self.blobs.keys().map(|s| s.as_str()).collect()
    }

    /// List blob keys matching a prefix.
    pub fn list_prefix(&self, prefix: &str) -> Vec<&str> {
        self.blobs
            .keys()
            .filter(|k| k.starts_with(prefix))
            .map(|s| s.as_str())
            .collect()
    }

    /// Total number of blobs.
    pub fn blob_count(&self) -> usize {
        self.blobs.len()
    }

    /// Total logical bytes (before dedup).
    pub fn total_logical_bytes(&self) -> u64 {
        self.blobs.values().map(|m| m.size).sum()
    }

    /// Total physical bytes (after dedup).
    pub fn total_physical_bytes(&self) -> usize {
        self.chunks.stored_bytes()
    }

    /// Deduplication ratio (logical / physical). Higher = better dedup.
    pub fn dedup_ratio(&self) -> f64 {
        let physical = self.total_physical_bytes();
        if physical == 0 {
            return 1.0;
        }
        self.total_logical_bytes() as f64 / physical as f64
    }

    /// Create a snapshot for WAL checkpoint.
    fn build_snapshot(&self) -> BlobStoreSnapshot<'_> {
        let mut snap_blobs = Vec::new();
        for (id, meta) in &self.blobs {
            let mut chunks_ref = Vec::new();
            for hash in &meta.chunk_hashes {
                if let Some(data) = self.chunks.get(hash) {
                    chunks_ref.push((hash, data));
                }
            }
            let tags: Vec<(&str, &str)> = meta
                .tags
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            snap_blobs.push((
                id.as_str(),
                meta.content_type.as_deref(),
                meta.size,
                chunks_ref,
                tags,
            ));
        }
        BlobStoreSnapshot { blobs: snap_blobs }
    }

    /// Checkpoint the WAL (truncate to a single snapshot).
    pub fn checkpoint(&self) -> std::io::Result<()> {
        if let Some(wal) = &self.wal {
            let snapshot = self.build_snapshot();
            wal.checkpoint(&snapshot)?;
        }
        Ok(())
    }

    /// Capture a snapshot of all mutable blob state for transaction rollback.
    ///
    /// The WAL handle is not snapshotted — it is append-only and shared.
    pub fn txn_snapshot(&self) -> BlobTxnSnapshot {
        BlobTxnSnapshot {
            chunks: self.chunks.clone(),
            blobs: self.blobs.clone(),
        }
    }

    /// Restore mutable blob state from a transaction snapshot (for ROLLBACK).
    pub fn txn_restore(&mut self, snap: BlobTxnSnapshot) {
        self.chunks = snap.chunks;
        self.blobs = snap.blobs;
    }
}

/// Snapshot of `BlobStore` mutable state for transaction rollback.
pub struct BlobTxnSnapshot {
    chunks: ChunkStore,
    blobs: HashMap<String, BlobMetadata>,
}

// ============================================================================
// Content-addressable blob deduplication
// ============================================================================

/// Statistics snapshot from a `BlobDedup` store.
#[derive(Debug, Clone)]
pub struct BlobDedupStats {
    pub unique_blobs: usize,
    pub total_refs: u64,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
    pub dedup_ratio: f64,
}

/// Content-addressable deduplication store.
///
/// Stores blobs keyed by their BLAKE3 content hash, tracks reference counts
/// so the same data stored N times only occupies space once, and exposes
/// deduplication metrics.
pub struct BlobDedup {
    /// hash -> data
    store: HashMap<String, Vec<u8>>,
    /// hash -> reference count
    ref_counts: HashMap<String, u64>,
    /// Total bytes physically stored (after dedup).
    total_stored_bytes: u64,
    /// Total bytes logically stored (before dedup).
    total_logical_bytes: u64,
    /// Number of times a store call was deduplicated.
    dedup_count: u64,
}

impl Default for BlobDedup {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobDedup {
    /// Create an empty dedup store.
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            ref_counts: HashMap::new(),
            total_stored_bytes: 0,
            total_logical_bytes: 0,
            dedup_count: 0,
        }
    }

    /// Compute a BLAKE3 content hash of `data` and return it as a 64-char hex string.
    pub fn content_hash(data: &[u8]) -> String {
        blake3::hash(data).to_hex().to_string()
    }

    /// Store a blob. Returns `(hash, was_deduped)`.
    ///
    /// If the content hash already exists the data is not stored again and the
    /// reference count is incremented. `total_logical_bytes` is always bumped.
    pub fn store_blob(&mut self, data: Vec<u8>) -> (String, bool) {
        let hash = Self::content_hash(&data);
        let len = data.len() as u64;
        self.total_logical_bytes += len;

        if self.store.contains_key(&hash) {
            *self.ref_counts.get_mut(&hash).unwrap() += 1;
            self.dedup_count += 1;
            (hash, true)
        } else {
            self.total_stored_bytes += len;
            self.store.insert(hash.clone(), data);
            self.ref_counts.insert(hash.clone(), 1);
            (hash, false)
        }
    }

    /// Retrieve blob data by content hash.
    pub fn get_blob(&self, hash: &str) -> Option<&[u8]> {
        self.store.get(hash).map(|v| v.as_slice())
    }

    /// Decrement the reference count for a blob. If it reaches 0 the data is
    /// removed. Returns `true` if the hash was found.
    pub fn release_blob(&mut self, hash: &str) -> bool {
        if let Some(rc) = self.ref_counts.get_mut(hash) {
            *rc -= 1;
            if *rc == 0 {
                if let Some(data) = self.store.remove(hash) {
                    self.total_stored_bytes -= data.len() as u64;
                }
                self.ref_counts.remove(hash);
            }
            true
        } else {
            false
        }
    }

    /// Deduplication ratio: `total_logical_bytes / total_stored_bytes`.
    ///
    /// A value > 1.0 means deduplication is saving space.
    /// Returns 1.0 when the store is empty.
    pub fn dedup_ratio(&self) -> f64 {
        if self.total_stored_bytes == 0 {
            return 1.0;
        }
        self.total_logical_bytes as f64 / self.total_stored_bytes as f64
    }

    /// Number of unique blobs currently stored.
    pub fn blob_count(&self) -> usize {
        self.store.len()
    }

    /// Current reference count for a blob hash (0 if not present).
    pub fn ref_count(&self, hash: &str) -> u64 {
        self.ref_counts.get(hash).copied().unwrap_or(0)
    }

    /// Return a statistics snapshot of the dedup store.
    pub fn stats(&self) -> BlobDedupStats {
        let total_refs: u64 = self.ref_counts.values().sum();
        BlobDedupStats {
            unique_blobs: self.store.len(),
            total_refs,
            stored_bytes: self.total_stored_bytes,
            logical_bytes: self.total_logical_bytes,
            dedup_ratio: self.dedup_ratio(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // BLAKE3 + ChunkStore tests
    // ========================================================================

    #[test]
    fn content_hash_deterministic() {
        let data = b"hello world";
        let h1 = content_hash_blake3(data);
        let h2 = content_hash_blake3(data);
        assert_eq!(h1, h2);

        let h3 = content_hash_blake3(b"different data");
        assert_ne!(h1, h3);
    }

    #[test]
    fn content_hash_legacy_deterministic() {
        let data = b"hello world";
        let h1 = content_hash(data);
        let h2 = content_hash(data);
        assert_eq!(h1, h2);

        let h3 = content_hash(b"different data");
        assert_ne!(h1, h3);
    }

    #[test]
    fn blake3_hashes_are_32_bytes() {
        let hash = content_hash_blake3(b"test data");
        assert_eq!(hash.len(), 32);
        // BLAKE3 of non-empty data should not be all zeros
        assert!(hash.iter().any(|&b| b != 0));
    }

    #[test]
    fn chunk_store_dedup() {
        let mut store = ChunkStore::new();

        let data = vec![1u8, 2, 3, 4, 5];
        let h1 = store.put(data.clone());
        let h2 = store.put(data.clone());

        assert_eq!(h1, h2);
        assert_eq!(store.chunk_count(), 1);
        assert_eq!(store.stored_bytes(), 5); // Only stored once
    }

    #[test]
    fn chunk_store_contains() {
        let mut cs = ChunkStore::new();
        let hash = cs.put(vec![10, 20, 30]);
        assert!(cs.contains(&hash));
        let fake = [0u8; 32];
        assert!(!cs.contains(&fake));
    }

    // ========================================================================
    // BlobIndex tests
    // ========================================================================

    #[test]
    fn blob_index_find_chunk() {
        let idx = BlobIndex::build(&[4, 4, 4, 4]); // 16 bytes in 4 chunks
        // Byte 0 -> chunk 0
        assert_eq!(idx.find_chunk(0), Some(0));
        // Byte 3 -> chunk 0
        assert_eq!(idx.find_chunk(3), Some(0));
        // Byte 4 -> chunk 1
        assert_eq!(idx.find_chunk(4), Some(1));
        // Byte 7 -> chunk 1
        assert_eq!(idx.find_chunk(7), Some(1));
        // Byte 12 -> chunk 3
        assert_eq!(idx.find_chunk(12), Some(3));
        // Byte 15 -> chunk 3
        assert_eq!(idx.find_chunk(15), Some(3));
    }

    #[test]
    fn blob_index_total_size() {
        let idx = BlobIndex::build(&[10, 20, 30]);
        assert_eq!(idx.total_size(), 60);
        let empty = BlobIndex::build(&[]);
        assert_eq!(empty.total_size(), 0);
    }

    // ========================================================================
    // BlobStore in-memory tests
    // ========================================================================

    #[test]
    fn blob_store_roundtrip() {
        let mut store = BlobStore::with_chunk_size(4);

        let data = b"hello world, this is a test blob!";
        store.put("test/file.txt", data, Some("text/plain"));

        let retrieved = store.get("test/file.txt").unwrap();
        assert_eq!(retrieved, data);

        let meta = store.metadata("test/file.txt").unwrap();
        assert_eq!(meta.size, data.len() as u64);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        // 33 bytes / 4 byte chunks = 9 chunks (8 full + 1 partial)
        assert_eq!(meta.chunk_hashes.len(), 9);
    }

    #[test]
    fn blob_store_range_read() {
        let mut store = BlobStore::with_chunk_size(4);

        let data = b"abcdefghijklmnop";
        store.put("file", data, None);

        // Read bytes 4-7 ("efgh") -- exactly chunk boundary
        let range = store.get_range("file", 4, 4).unwrap();
        assert_eq!(range, b"efgh");

        // Read bytes 2-9 (cross-chunk)
        let range = store.get_range("file", 2, 8).unwrap();
        assert_eq!(range, b"cdefghij");
    }

    #[test]
    fn blob_deduplication() {
        let mut store = BlobStore::with_chunk_size(4);

        // Store same data under two different keys
        let data = b"repeating data!!";
        store.put("blob1", data, None);
        store.put("blob2", data, None);

        assert_eq!(store.blob_count(), 2);
        assert_eq!(store.total_logical_bytes(), 32); // 16 * 2
        assert_eq!(store.total_physical_bytes(), 16); // Deduplicated
        assert!((store.dedup_ratio() - 2.0).abs() < 1e-10);
    }

    #[test]
    fn blob_tags_and_listing() {
        let mut store = BlobStore::new();

        store.put("images/photo1.jpg", b"jpg-data-1", Some("image/jpeg"));
        store.put("images/photo2.jpg", b"jpg-data-2", Some("image/jpeg"));
        store.put("docs/readme.md", b"# README", Some("text/markdown"));

        assert_eq!(store.blob_count(), 3);

        // Prefix listing
        let images = store.list_prefix("images/");
        assert_eq!(images.len(), 2);

        let docs = store.list_prefix("docs/");
        assert_eq!(docs.len(), 1);

        // Tags
        store.set_tag("images/photo1.jpg", "album", "vacation");
        let meta = store.metadata("images/photo1.jpg").unwrap();
        assert_eq!(meta.tags["album"], "vacation");
    }

    #[test]
    fn blob_delete() {
        let mut store = BlobStore::new();
        store.put("temp", b"temporary data", None);
        assert_eq!(store.blob_count(), 1);

        assert!(store.delete("temp"));
        assert_eq!(store.blob_count(), 0);
        assert!(store.get("temp").is_none());
    }

    #[test]
    fn large_blob_storage_and_retrieval() {
        let mut store = BlobStore::with_chunk_size(1024);
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 256) as u8).collect();
        store.put("large_file", &data, Some("application/octet-stream"));
        let retrieved = store.get("large_file").unwrap();
        assert_eq!(retrieved.len(), data.len());
        assert_eq!(retrieved, data);
        let meta = store.metadata("large_file").unwrap();
        assert_eq!(meta.size, 100_000);
        assert_eq!(meta.chunk_hashes.len(), 98);
    }

    #[test]
    fn empty_blob() {
        let mut store = BlobStore::new();
        store.put("empty", b"", None);
        let retrieved = store.get("empty").unwrap();
        assert!(retrieved.is_empty());
        let meta = store.metadata("empty").unwrap();
        assert_eq!(meta.size, 0);
        assert_eq!(meta.content_type, None);
    }

    #[test]
    fn overwrite_existing_blob() {
        let mut store = BlobStore::new();
        store.put("file", b"original content", Some("text/plain"));
        let v1 = store.get("file").unwrap();
        assert_eq!(v1, b"original content");
        store.put("file", b"updated content", Some("text/html"));
        let v2 = store.get("file").unwrap();
        assert_eq!(v2, b"updated content");
        let meta = store.metadata("file").unwrap();
        assert_eq!(meta.content_type.as_deref(), Some("text/html"));
        assert_eq!(store.blob_count(), 1);
    }

    #[test]
    fn delete_nonexistent_blob() {
        let mut store = BlobStore::new();
        assert!(!store.delete("does_not_exist"));
        assert_eq!(store.blob_count(), 0);
    }

    #[test]
    fn multiple_blobs_different_keys() {
        let mut store = BlobStore::new();
        store.put("alpha", b"aaa", None);
        store.put("beta", b"bbb", None);
        store.put("gamma", b"ccc", None);
        assert_eq!(store.blob_count(), 3);
        assert_eq!(store.get("alpha").unwrap(), b"aaa");
        assert_eq!(store.get("beta").unwrap(), b"bbb");
        assert_eq!(store.get("gamma").unwrap(), b"ccc");
        store.delete("beta");
        assert_eq!(store.blob_count(), 2);
        assert!(store.get("beta").is_none());
        assert_eq!(store.get("alpha").unwrap(), b"aaa");
    }

    #[test]
    fn blob_metadata_tags() {
        let mut store = BlobStore::new();
        store.put("doc.pdf", b"pdf-data", Some("application/pdf"));
        store.set_tag("doc.pdf", "author", "Alice");
        store.set_tag("doc.pdf", "dept", "Engineering");
        let meta = store.metadata("doc.pdf").unwrap();
        assert_eq!(meta.tags.len(), 2);
        assert_eq!(meta.tags["author"], "Alice");
        store.set_tag("doc.pdf", "author", "Bob");
        let m2 = store.metadata("doc.pdf").unwrap();
        assert_eq!(m2.tags["author"], "Bob");
    }

    #[test]
    fn set_tag_on_nonexistent_blob() {
        let mut store = BlobStore::new();
        assert!(!store.set_tag("ghost", "key", "value"));
    }

    #[test]
    fn get_nonexistent_blob() {
        let store = BlobStore::new();
        assert!(store.get("nope").is_none());
        assert!(store.metadata("nope").is_none());
        assert!(store.get_range("nope", 0, 10).is_none());
    }

    #[test]
    fn range_read_entire_blob() {
        let mut store = BlobStore::with_chunk_size(4);
        let data = b"abcdefghijklmnop";
        store.put("file", data, None);
        let full = store.get_range("file", 0, 16).unwrap();
        assert_eq!(full, data.to_vec());
    }

    #[test]
    fn range_read_beyond_end() {
        let mut store = BlobStore::with_chunk_size(4);
        store.put("file", b"abcdefgh", None);
        let range = store.get_range("file", 4, 100).unwrap();
        assert_eq!(range, b"efgh");
    }

    #[test]
    fn dedup_ratio_with_no_data() {
        let store = BlobStore::new();
        assert!((store.dedup_ratio() - 1.0).abs() < 1e-10);
        assert_eq!(store.total_logical_bytes(), 0);
        assert_eq!(store.total_physical_bytes(), 0);
    }

    #[test]
    fn dedup_across_multiple_blobs() {
        let mut store = BlobStore::with_chunk_size(4);
        let data = b"AAAA";
        store.put("a", data, None);
        store.put("b", data, None);
        store.put("c", data, None);
        assert_eq!(store.blob_count(), 3);
        assert_eq!(store.total_logical_bytes(), 12);
        assert_eq!(store.total_physical_bytes(), 4);
        assert!((store.dedup_ratio() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn list_prefix_no_matches() {
        let mut store = BlobStore::new();
        store.put("images/a.png", b"png", None);
        store.put("images/b.png", b"png", None);
        let matches = store.list_prefix("videos/");
        assert!(matches.is_empty());
    }

    // ========================================================================
    // WAL-backed BlobStore tests
    // ========================================================================

    #[test]
    fn wal_store_reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("key1", b"hello world", Some("text/plain"));
        }
        // Reopen and verify
        let store2 = BlobStore::open(dir.path()).unwrap();
        let data = store2.get("key1").unwrap();
        assert_eq!(data, b"hello world");
        assert_eq!(
            store2.metadata("key1").unwrap().content_type.as_deref(),
            Some("text/plain")
        );
    }

    #[test]
    fn wal_delete_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("key1", b"data", None);
            store.delete("key1");
        }
        let store2 = BlobStore::open(dir.path()).unwrap();
        assert!(store2.get("key1").is_none());
        assert_eq!(store2.blob_count(), 0);
    }

    #[test]
    fn wal_dedup_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            let data = b"AAAA"; // Single chunk, same across blobs
            store.put("a", data, None);
            store.put("b", data, None);
        }
        let store2 = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        assert_eq!(store2.blob_count(), 2);
        assert_eq!(store2.get("a").unwrap(), b"AAAA");
        assert_eq!(store2.get("b").unwrap(), b"AAAA");
        // Both use the same underlying chunk
        assert_eq!(store2.total_physical_bytes(), 4);
    }

    #[test]
    fn wal_tags_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("doc", b"data", None);
            store.set_tag("doc", "author", "Alice");
            store.set_tag("doc", "version", "2");
        }
        let store2 = BlobStore::open(dir.path()).unwrap();
        let meta = store2.metadata("doc").unwrap();
        assert_eq!(meta.tags["author"], "Alice");
        assert_eq!(meta.tags["version"], "2");
    }

    #[test]
    fn wal_large_blob_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 256) as u8).collect();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 1024).unwrap();
            store.put("large", &data, Some("application/octet-stream"));
        }
        let store2 = BlobStore::open_with_chunk_size(dir.path(), 1024).unwrap();
        let retrieved = store2.get("large").unwrap();
        assert_eq!(retrieved.len(), data.len());
        assert_eq!(retrieved, data);
    }

    #[test]
    fn wal_blake3_consistent_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let hash_before;
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("file", b"consistent hash", None);
            hash_before = store.metadata("file").unwrap().chunk_hashes[0];
        }
        let store2 = BlobStore::open(dir.path()).unwrap();
        let hash_after = store2.metadata("file").unwrap().chunk_hashes[0];
        assert_eq!(hash_before, hash_after);
        // Also verify it matches a direct BLAKE3 computation
        let expected = content_hash_blake3(b"consistent hash");
        assert_eq!(hash_before, expected);
    }

    #[test]
    fn wal_range_read_at_chunk_boundary() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            store.put("file", b"abcdefghijklmnop", None);
        }
        let store2 = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        // Read exactly chunk 1 (bytes 4..8)
        let range = store2.get_range("file", 4, 4).unwrap();
        assert_eq!(range, b"efgh");
    }

    #[test]
    fn wal_range_read_mid_chunk() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            store.put("file", b"abcdefghijklmnop", None);
        }
        let store2 = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        // Read bytes 2..6 (crosses chunk boundary mid-chunk)
        let range = store2.get_range("file", 2, 4).unwrap();
        assert_eq!(range, b"cdef");
    }

    #[test]
    fn wal_range_spanning_multiple_chunks() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            store.put("file", b"abcdefghijklmnop", None);
        }
        let store2 = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        // Read bytes 2..14 (spans chunks 0, 1, 2, 3)
        let range = store2.get_range("file", 2, 12).unwrap();
        assert_eq!(range, b"cdefghijklmn");
    }

    #[test]
    fn wal_empty_store_clean_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        assert_eq!(store.blob_count(), 0);
        assert!(store.get("anything").is_none());
    }

    #[test]
    fn wal_corrupt_graceful_recovery() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("good", b"good data", None);
        }
        // Append garbage to the WAL file
        {
            let wal_path = dir.path().join("blob.wal");
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD, 0xFC]).unwrap();
            f.flush().unwrap();
        }
        // Should recover the good blob
        let store2 = BlobStore::open(dir.path()).unwrap();
        assert_eq!(store2.blob_count(), 1);
        assert_eq!(store2.get("good").unwrap(), b"good data");
    }

    // ========================================================================
    // BlobDedup tests
    // ========================================================================

    #[test]
    fn test_blob_dedup_store_and_get() {
        let mut dedup = BlobDedup::new();
        let data = b"hello dedup world".to_vec();
        let (hash, was_deduped) = dedup.store_blob(data.clone());
        assert!(!was_deduped);
        assert!(!hash.is_empty());

        let retrieved = dedup.get_blob(&hash).unwrap();
        assert_eq!(retrieved, &data[..]);
    }

    #[test]
    fn test_blob_dedup_detects_duplicate() {
        let mut dedup = BlobDedup::new();
        let data = b"duplicate me".to_vec();

        let (h1, dup1) = dedup.store_blob(data.clone());
        assert!(!dup1);

        let (h2, dup2) = dedup.store_blob(data.clone());
        assert!(dup2);
        assert_eq!(h1, h2);

        // Only one unique blob stored
        assert_eq!(dedup.blob_count(), 1);
    }

    #[test]
    fn test_blob_dedup_ref_counting() {
        let mut dedup = BlobDedup::new();
        let data = b"ref counted".to_vec();

        let (hash, _) = dedup.store_blob(data.clone());
        assert_eq!(dedup.ref_count(&hash), 1);

        dedup.store_blob(data.clone());
        assert_eq!(dedup.ref_count(&hash), 2);

        dedup.store_blob(data.clone());
        assert_eq!(dedup.ref_count(&hash), 3);

        // Unknown hash returns 0
        assert_eq!(dedup.ref_count("nonexistent"), 0);
    }

    #[test]
    fn test_blob_dedup_release() {
        let mut dedup = BlobDedup::new();
        let data = b"release me".to_vec();

        let (hash, _) = dedup.store_blob(data.clone());
        dedup.store_blob(data.clone());
        assert_eq!(dedup.ref_count(&hash), 2);

        // First release decrements
        assert!(dedup.release_blob(&hash));
        assert_eq!(dedup.ref_count(&hash), 1);
        assert!(dedup.get_blob(&hash).is_some());

        // Second release removes the blob entirely
        assert!(dedup.release_blob(&hash));
        assert_eq!(dedup.ref_count(&hash), 0);
        assert!(dedup.get_blob(&hash).is_none());
        assert_eq!(dedup.blob_count(), 0);

        // Releasing again returns false
        assert!(!dedup.release_blob(&hash));
    }

    #[test]
    fn test_blob_dedup_ratio() {
        let mut dedup = BlobDedup::new();

        // Empty store ratio is 1.0
        assert!((dedup.dedup_ratio() - 1.0).abs() < 1e-10);

        let data = b"ratio test".to_vec(); // 10 bytes
        dedup.store_blob(data.clone());
        // 10 logical / 10 stored = 1.0
        assert!((dedup.dedup_ratio() - 1.0).abs() < 1e-10);

        dedup.store_blob(data.clone());
        // 20 logical / 10 stored = 2.0
        assert!((dedup.dedup_ratio() - 2.0).abs() < 1e-10);

        dedup.store_blob(data.clone());
        // 30 logical / 10 stored = 3.0
        assert!((dedup.dedup_ratio() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_blob_dedup_stats() {
        let mut dedup = BlobDedup::new();

        let d1 = b"alpha".to_vec(); // 5 bytes
        let d2 = b"beta".to_vec(); // 4 bytes

        dedup.store_blob(d1.clone());
        dedup.store_blob(d1.clone()); // dup
        dedup.store_blob(d2.clone());

        let stats = dedup.stats();
        assert_eq!(stats.unique_blobs, 2);
        assert_eq!(stats.total_refs, 3); // 2 refs for d1 + 1 ref for d2
        assert_eq!(stats.stored_bytes, 9); // 5 + 4
        assert_eq!(stats.logical_bytes, 14); // 5 + 5 + 4
        assert!((stats.dedup_ratio - 14.0 / 9.0).abs() < 1e-10);
    }
}
