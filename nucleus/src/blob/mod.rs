//! Large object (blob) storage with content-addressable deduplication.
//!
//! Supports:
//!   - Chunked storage for multi-GB objects
//!   - Content-addressable deduplication (same data stored once)
//!   - Streaming reads/writes without loading entire object into memory
//!   - Metadata and tagging on blobs
//!   - BLAKE3 cryptographic hashing for content addressing
//!   - Byte-range index for O(log N) range access
//!   - Disk-tiered chunk storage: capacity is disk-bound, not RAM-bound
//!     (via `BlobStore::open`); a byte-bounded LRU cache keeps hot chunks
//!     RAM-fast
//!   - WAL-backed durability for blob manifests (via `BlobStore::open`)
//!
//! Replaces S3, GCS, MinIO for blob storage within Nucleus.
//!
//! ## Storage architecture (disk mode)
//!
//! Chunk data lives in append-only segment files ([`segment::SegmentStore`]);
//! the WAL ([`wal::BlobWal`]) records only blob manifests (chunk hashes +
//! lengths). A put appends chunks to their segment and flushes them *before*
//! logging the manifest, so a manifest that survives a crash always references
//! chunk data that also survived. The RAM cache is write-through: evicting
//! from it can never lose data.
//!
//! Chunks are content-addressed and shared across blobs, so each chunk carries
//! a reference count (one per manifest reference). Chunks that reach zero
//! references are swept — removed from the cache, marked dead in their segment,
//! and reclaimed by segment compaction — but only when no transaction snapshot
//! is outstanding, because a ROLLBACK may restore manifests that still
//! reference them.

pub mod segment;
pub mod wal;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

use segment::SegmentStore;
use wal::{BlobMetaSnapshot, BlobWal};

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

/// Default RAM budget for the hot-chunk cache in disk mode. Override with the
/// `NUCLEUS_BLOB_CACHE_BYTES` environment variable.
pub const DEFAULT_BLOB_CACHE_BYTES: usize = 128 * 1024 * 1024;

/// Byte-bounded LRU cache of chunk data.
struct ChunkCache {
    map: HashMap<ChunkHash, (Vec<u8>, u64)>,
    /// last-use tick -> hash; lowest tick is the eviction candidate.
    order: BTreeMap<u64, ChunkHash>,
    tick: u64,
    bytes: usize,
    limit: usize,
}

impl ChunkCache {
    fn new(limit: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: BTreeMap::new(),
            tick: 0,
            bytes: 0,
            limit,
        }
    }

    fn touch(
        entry_tick: &mut u64,
        order: &mut BTreeMap<u64, ChunkHash>,
        tick: &mut u64,
        hash: ChunkHash,
    ) {
        order.remove(entry_tick);
        *tick += 1;
        *entry_tick = *tick;
        order.insert(*tick, hash);
    }

    /// Append the chunk's data to `out`. Returns false on miss.
    fn read_into(&mut self, hash: &ChunkHash, out: &mut Vec<u8>) -> bool {
        let Some((data, entry_tick)) = self.map.get_mut(hash) else {
            return false;
        };
        out.extend_from_slice(data);
        Self::touch(entry_tick, &mut self.order, &mut self.tick, *hash);
        true
    }

    fn get_clone(&mut self, hash: &ChunkHash) -> Option<Vec<u8>> {
        let (data, entry_tick) = self.map.get_mut(hash)?;
        let out = data.clone();
        Self::touch(entry_tick, &mut self.order, &mut self.tick, *hash);
        Some(out)
    }

    fn insert(&mut self, hash: ChunkHash, data: Vec<u8>) {
        if data.len() > self.limit {
            return; // larger than the whole budget — serve from disk
        }
        if let Some((_, entry_tick)) = self.map.get_mut(&hash) {
            Self::touch(entry_tick, &mut self.order, &mut self.tick, hash);
            return;
        }
        while self.bytes + data.len() > self.limit {
            let Some((&oldest, _)) = self.order.iter().next() else {
                break;
            };
            let victim = self.order.remove(&oldest).unwrap();
            if let Some((old_data, _)) = self.map.remove(&victim) {
                self.bytes -= old_data.len();
            }
        }
        self.tick += 1;
        self.bytes += data.len();
        self.order.insert(self.tick, hash);
        self.map.insert(hash, (data, self.tick));
    }

    fn remove(&mut self, hash: &ChunkHash) {
        if let Some((data, entry_tick)) = self.map.remove(hash) {
            self.bytes -= data.len();
            self.order.remove(&entry_tick);
        }
    }

    fn retain_keys<F: Fn(&ChunkHash) -> bool>(&mut self, keep: F) {
        let victims: Vec<ChunkHash> = self.map.keys().filter(|h| !keep(h)).copied().collect();
        for h in victims {
            self.remove(&h);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ChunkRef {
    /// Number of manifest references (one per chunk occurrence per blob).
    /// Zero means the chunk is garbage pending a sweep.
    count: u64,
    len: u32,
}

/// Content-addressable chunk store — deduplicates identical chunks via BLAKE3
/// and reference-counts them (chunks are shared across blobs).
///
/// Two modes:
///   - RAM-only (`ChunkStore::new`): all chunk data lives in the cache,
///     which is unbounded. Used by `BlobStore::new()` (tests, ephemeral).
///   - Disk-tiered (`BlobStore::open`): chunk data lives in segment files;
///     the cache is a byte-bounded write-through LRU, so capacity is
///     disk-bound and cache eviction can never lose data.
pub struct ChunkStore {
    /// Interior mutability so reads stay `&self` (LRU bookkeeping + fill).
    cache: Mutex<ChunkCache>,
    disk: Option<SegmentStore>,
    refs: HashMap<ChunkHash, ChunkRef>,
    /// Total bytes of live (referenced) unique chunks.
    stored_bytes: usize,
    /// Cloned into every transaction snapshot; garbage is only swept when the
    /// count is back to 1 (no snapshot could restore a reference to it).
    txn_pin: Arc<()>,
    needs_sweep: bool,
}

impl Default for ChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkStore {
    /// RAM-only chunk store (unbounded cache, no disk tier).
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(ChunkCache::new(usize::MAX)),
            disk: None,
            refs: HashMap::new(),
            stored_bytes: 0,
            txn_pin: Arc::new(()),
            needs_sweep: false,
        }
    }

    fn with_disk(
        disk: SegmentStore,
        cache_limit: usize,
        refs: HashMap<ChunkHash, ChunkRef>,
    ) -> Self {
        let stored_bytes = refs
            .values()
            .filter(|r| r.count > 0)
            .map(|r| r.len as usize)
            .sum();
        Self {
            cache: Mutex::new(ChunkCache::new(cache_limit)),
            disk: Some(disk),
            refs,
            stored_bytes,
            txn_pin: Arc::new(()),
            needs_sweep: true, // reconcile any orphaned segment records
        }
    }

    /// Store a chunk (or add a reference to it if already stored). Returns the
    /// BLAKE3 hash. In disk mode the chunk is on disk (flushed) on return.
    pub fn put(&mut self, data: Vec<u8>) -> ChunkHash {
        let hash = content_hash_blake3(&data);
        let len = data.len();

        if let Some(r) = self.refs.get_mut(&hash) {
            if r.count == 0 {
                // Garbage awaiting sweep — revive instead of re-storing.
                self.stored_bytes += len;
                if let Some(disk) = &mut self.disk {
                    disk.revive(&hash);
                }
            }
            r.count += 1;
            return hash;
        }

        if let Some(disk) = &mut self.disk
            && let Err(e) = disk.append(&hash, &data)
        {
            eprintln!("blob segments: failed to persist chunk: {e}");
        }
        self.cache.get_mut().insert(hash, data);
        self.refs.insert(
            hash,
            ChunkRef {
                count: 1,
                len: len as u32,
            },
        );
        self.stored_bytes += len;
        hash
    }

    /// Add a reference to an already-stored chunk (no data needed). Returns
    /// `false` if the chunk does not exist or is garbage awaiting sweep.
    pub fn add_ref(&mut self, hash: &ChunkHash) -> bool {
        match self.refs.get_mut(hash) {
            Some(r) if r.count > 0 => {
                r.count += 1;
                true
            }
            _ => false,
        }
    }

    /// Drop one reference to a chunk. At zero references the chunk becomes
    /// garbage, physically reclaimed by the next unpinned [`Self::sweep`].
    pub fn release(&mut self, hash: &ChunkHash) {
        if let Some(r) = self.refs.get_mut(hash)
            && r.count > 0
        {
            r.count -= 1;
            if r.count == 0 {
                self.stored_bytes -= r.len as usize;
                self.needs_sweep = true;
            }
        }
    }

    /// Re-acquire a reference to an already-stored chunk. Used by a scoped
    /// rollback to restore the reference counts a reverted blob held, without
    /// rewriting the chunk data (the transaction pin kept it readable).
    /// Returns false when the chunk is unknown to the store.
    fn retain(&mut self, hash: &ChunkHash) -> bool {
        let Some(r) = self.refs.get_mut(hash) else {
            return false;
        };
        if r.count == 0 {
            self.stored_bytes += r.len as usize;
            if let Some(disk) = &mut self.disk {
                disk.revive(hash);
            }
        }
        r.count += 1;
        true
    }

    /// Get a chunk by hash. Served from the RAM cache when hot; falls back to
    /// a segment read (and refills the cache) in disk mode.
    pub fn get(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        if let Some(data) = self.cache.lock().get_clone(hash) {
            return Some(data);
        }
        self.read_from_disk(hash)
    }

    /// Append a chunk's data to `out`. Returns false if the chunk is absent
    /// or unreadable.
    pub fn read_into(&self, hash: &ChunkHash, out: &mut Vec<u8>) -> bool {
        if self.cache.lock().read_into(hash, out) {
            return true;
        }
        match self.read_from_disk(hash) {
            Some(data) => {
                out.extend_from_slice(&data);
                true
            }
            None => false,
        }
    }

    fn read_from_disk(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        let disk = self.disk.as_ref()?;
        match disk.read(hash) {
            Ok(Some(data)) => {
                self.cache.lock().insert(*hash, data.clone());
                Some(data)
            }
            Ok(None) => None,
            Err(e) => {
                eprintln!("blob segments: chunk read failed: {e}");
                None
            }
        }
    }

    /// Check if a chunk exists (has at least one reference).
    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.refs.get(hash).is_some_and(|r| r.count > 0)
    }

    /// Total deduplicated bytes of live chunks.
    pub fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    /// Number of unique live chunks.
    pub fn chunk_count(&self) -> usize {
        self.refs.values().filter(|r| r.count > 0).count()
    }

    /// Physically reclaim garbage: drop zero-reference chunks from the cache,
    /// mark their segment records dead, and compact eligible segments.
    ///
    /// Deferred while any transaction snapshot is outstanding — a ROLLBACK may
    /// restore manifests that still reference the garbage.
    pub fn sweep(&mut self) {
        if !self.needs_sweep || Arc::strong_count(&self.txn_pin) > 1 {
            return;
        }
        self.needs_sweep = false;

        self.refs.retain(|_, r| r.count > 0);
        let refs = &self.refs;
        let cache = self.cache.get_mut();
        // Also drops orphans with no refs entry at all (rolled-back puts).
        cache.retain_keys(|h| refs.contains_key(h));
        if let Some(disk) = &mut self.disk {
            disk.mark_dead_where(|h| refs.contains_key(h));
            if let Err(e) = disk.compact() {
                eprintln!("blob segments: compaction failed: {e}");
            }
        }
    }

    fn txn_snapshot(&self) -> ChunkTxnSnapshot {
        ChunkTxnSnapshot {
            refs: self.refs.clone(),
            stored_bytes: self.stored_bytes,
            _pin: Arc::clone(&self.txn_pin),
        }
    }

    fn txn_restore(&mut self, snap: ChunkTxnSnapshot) {
        self.refs = snap.refs;
        self.stored_bytes = snap.stored_bytes;
        // Chunks written since the snapshot are now orphans; deleted chunks
        // may have come back to life. Reconcile on the next sweep.
        self.needs_sweep = true;
    }
}

/// Chunk-store state captured for transaction rollback: reference counts only
/// — no chunk data. The held pin defers garbage sweeps so every chunk the
/// snapshot references stays physically readable until commit or rollback.
struct ChunkTxnSnapshot {
    refs: HashMap<ChunkHash, ChunkRef>,
    stored_bytes: usize,
    _pin: Arc<()>,
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
        let idx = self.offsets.partition_point(|(cum, _)| *cum <= offset);
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

impl BlobMetadata {
    /// Per-chunk sizes recovered from the byte-range index.
    fn chunk_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.index.offsets.iter().map(|(_, sz)| *sz)
    }

    /// WAL manifest form: `(hash, len)` per chunk.
    fn wal_chunks(&self) -> Vec<(ChunkHash, u32)> {
        self.chunk_hashes
            .iter()
            .zip(self.chunk_sizes())
            .map(|(h, sz)| (*h, sz as u32))
            .collect()
    }

    fn wal_tags(&self) -> Vec<(&str, &str)> {
        self.tags
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Whether the durable state (manifest + tags) differs — used to decide
    /// which corrective WAL entries a rollback must log.
    fn durable_state_differs(&self, other: &BlobMetadata) -> bool {
        self.chunk_hashes != other.chunk_hashes
            || self.size != other.size
            || self.content_type != other.content_type
            || self.tags != other.tags
    }
}

// ============================================================================
// Blob store
// ============================================================================

/// Default chunk size: 1 MB.
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

/// Blob store — manages large objects as chunked, deduplicated data.
///
/// When opened with `BlobStore::open(dir)`, chunk data is disk-tiered into
/// segment files with a RAM LRU cache on top, and manifests are logged to a
/// WAL for crash recovery. The in-memory-only constructor `BlobStore::new()`
/// is retained for backward compatibility and testing.
pub struct BlobStore {
    chunks: ChunkStore,
    /// key -> blob metadata
    blobs: HashMap<String, BlobMetadata>,
    chunk_size: usize,
    /// Optional WAL for durability.
    wal: Option<Arc<BlobWal>>,
    /// Blob keys mutated since the last `clear_touched` — the transaction
    /// write-set the executor drains under the same write guard.
    txn_touched: HashSet<String>,
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
            txn_touched: HashSet::new(),
        }
    }

    /// Forget any recorded mutations (called before a mutating operation).
    pub fn clear_touched(&mut self) {
        self.txn_touched.clear();
    }

    /// Take the blob keys mutated since the last `clear_touched`.
    pub fn take_touched(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.txn_touched)
    }

    /// Open a disk-tiered, WAL-backed blob store at `dir`.
    ///
    /// Replays the WAL to recover blob manifests (chunk data is read from
    /// segment files on demand), then appends new mutations. The hot-chunk
    /// cache budget defaults to [`DEFAULT_BLOB_CACHE_BYTES`], overridable via
    /// the `NUCLEUS_BLOB_CACHE_BYTES` environment variable.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        Self::open_with_chunk_size(dir, DEFAULT_CHUNK_SIZE)
    }

    /// Open a disk-tiered, WAL-backed blob store with a custom chunk size.
    pub fn open_with_chunk_size(dir: &Path, chunk_size: usize) -> std::io::Result<Self> {
        let cache_limit = std::env::var("NUCLEUS_BLOB_CACHE_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_BLOB_CACHE_BYTES);
        Self::open_with_options(dir, chunk_size, cache_limit)
    }

    /// Open a disk-tiered, WAL-backed blob store with explicit chunk size and
    /// hot-chunk cache budget (bytes).
    pub fn open_with_options(
        dir: &Path,
        chunk_size: usize,
        cache_limit: usize,
    ) -> std::io::Result<Self> {
        let mut segments = SegmentStore::open(dir)?;
        let (wal, state) = BlobWal::open(dir)?;
        let migrate_legacy = state.legacy_entries_seen;

        let mut refs: HashMap<ChunkHash, ChunkRef> = HashMap::new();
        let mut blobs = HashMap::new();

        'blob: for (id, entry) in state.blobs {
            // Every referenced chunk must be present in the segment files.
            // Legacy WAL entries carry the data inline — migrate it into
            // segments. A metadata entry whose chunk is missing means torn
            // or corrupt storage: drop the blob (best-effort recovery).
            for c in &entry.chunks {
                if segments.revive(&c.hash) {
                    continue; // present (live or revived dead copy)
                }
                if let Some(data) = &c.data {
                    segments.append(&c.hash, data)?;
                } else {
                    eprintln!("blob store: dropping blob '{id}': chunk data missing from segments");
                    continue 'blob;
                }
            }

            let mut chunk_hashes = Vec::with_capacity(entry.chunks.len());
            let mut chunk_sizes = Vec::with_capacity(entry.chunks.len());
            for c in &entry.chunks {
                chunk_hashes.push(c.hash);
                chunk_sizes.push(c.len as usize);
                refs.entry(c.hash)
                    .or_insert(ChunkRef {
                        count: 0,
                        len: c.len,
                    })
                    .count += 1;
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

        // Self-heal: segment records not referenced by any recovered manifest
        // (orphans from crashes mid-put or pre-compaction copies) are dead.
        segments.mark_dead_where(|h| refs.contains_key(h));

        let store = Self {
            chunks: ChunkStore::with_disk(segments, cache_limit, refs),
            blobs,
            chunk_size,
            wal: Some(Arc::new(wal)),
            txn_touched: HashSet::new(),
        };

        // A legacy WAL embedded chunk data; now that it lives in segments,
        // rewrite the log in the metadata-only format to shrink it.
        if migrate_legacy && let Err(e) = store.checkpoint() {
            eprintln!("blob WAL: post-migration checkpoint failed: {e}");
        }

        Ok(store)
    }

    /// Store a blob. Splits into chunks and deduplicates.
    pub fn put(&mut self, key: &str, data: &[u8], content_type: Option<&str>) {
        self.chunks.sweep();

        let old_meta = self.blobs.remove(key);

        let mut chunk_hashes = Vec::new();
        let mut chunk_sizes = Vec::new();
        let mut wal_chunks: Vec<(ChunkHash, u32)> = Vec::new();

        for chunk_data in data.chunks(self.chunk_size) {
            let hash = self.chunks.put(chunk_data.to_vec());
            chunk_hashes.push(hash);
            chunk_sizes.push(chunk_data.len());
            wal_chunks.push((hash, chunk_data.len() as u32));
        }

        // Handle empty data
        if data.is_empty() {
            let hash = self.chunks.put(Vec::new());
            chunk_hashes.push(hash);
            chunk_sizes.push(0);
            wal_chunks.push((hash, 0));
        }

        // Chunks are on disk (flushed) at this point; the manifest may now be
        // logged — a recovered manifest never references unwritten data.
        if let Some(wal) = &self.wal
            && let Err(e) =
                wal.log_store_meta(key, content_type, data.len() as u64, &wal_chunks, &[])
        {
            eprintln!("blob WAL: failed to log store for '{key}': {e}");
        }

        // Release the overwritten manifest's references only after the new
        // ones exist, so chunks shared between versions never hit zero.
        if let Some(old) = old_meta {
            for hash in &old.chunk_hashes {
                self.chunks.release(hash);
            }
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
        self.txn_touched.insert(key.to_string());
    }

    /// Compose a new blob from the chunks of existing blobs, in order —
    /// zero data copy. Chunks are content-addressed and shared, so only
    /// reference counts and a new manifest are written; the source blobs
    /// remain intact. Returns `false` (storing nothing) if any source blob
    /// is missing.
    ///
    /// This is what makes multipart-style assembly O(metadata): the composed
    /// blob references the same physical chunks as its sources.
    pub fn compose(&mut self, key: &str, sources: &[&str], content_type: Option<&str>) -> bool {
        self.chunks.sweep();

        let mut chunk_hashes = Vec::new();
        let mut chunk_sizes = Vec::new();
        let mut total_size: u64 = 0;
        for src in sources {
            let Some(meta) = self.blobs.get(*src) else {
                return false;
            };
            for (hash, size) in meta.chunk_hashes.iter().zip(meta.chunk_sizes()) {
                chunk_hashes.push(*hash);
                chunk_sizes.push(size);
            }
            total_size += meta.size;
        }

        if chunk_hashes.is_empty() {
            // No sources — mirror put()'s empty-blob representation.
            self.put(key, b"", content_type);
            return true;
        }

        let old_meta = self.blobs.remove(key);

        // Sources hold live references to every chunk, so add_ref cannot miss.
        for hash in &chunk_hashes {
            self.chunks.add_ref(hash);
        }

        let wal_chunks: Vec<(ChunkHash, u32)> = chunk_hashes
            .iter()
            .zip(chunk_sizes.iter())
            .map(|(h, sz)| (*h, *sz as u32))
            .collect();
        if let Some(wal) = &self.wal
            && let Err(e) = wal.log_store_meta(key, content_type, total_size, &wal_chunks, &[])
        {
            eprintln!("blob WAL: failed to log compose for '{key}': {e}");
        }

        if let Some(old) = old_meta {
            for hash in &old.chunk_hashes {
                self.chunks.release(hash);
            }
        }

        let index = BlobIndex::build(&chunk_sizes);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let meta = BlobMetadata {
            key: key.to_string(),
            size: total_size,
            chunk_size: self.chunk_size,
            chunk_hashes,
            content_type: content_type.map(|s| s.to_string()),
            tags: HashMap::new(),
            created_at: ts,
            updated_at: ts,
            index,
        };
        self.blobs.insert(key.to_string(), meta);
        self.txn_touched.insert(key.to_string());
        true
    }

    /// Read an entire blob.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let meta = self.blobs.get(key)?;
        let mut data = Vec::with_capacity(meta.size as usize);
        for hash in &meta.chunk_hashes {
            if !self.chunks.read_into(hash, &mut data) {
                eprintln!("blob store: blob '{key}' has an unreadable chunk");
                return None;
            }
        }
        Some(data)
    }

    /// Read a byte range from a blob using the BlobIndex for O(log N) lookup.
    pub fn get_range(&self, key: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let meta = self.blobs.get(key)?;

        let start = offset;
        // saturating: an attacker-supplied offset+length must not overflow/wrap
        // (which would corrupt the range bounds); clamp to u64::MAX = read to end.
        let end = offset.saturating_add(length);

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

    /// Delete a blob. Its chunks lose one reference each; unreferenced chunks
    /// are physically reclaimed by a later sweep/compaction.
    pub fn delete(&mut self, key: &str) -> bool {
        self.chunks.sweep();
        let Some(meta) = self.blobs.remove(key) else {
            return false;
        };
        // Log to WAL before in-memory mutation
        if let Some(wal) = &self.wal
            && let Err(e) = wal.log_delete(key)
        {
            eprintln!("blob WAL: failed to log delete for '{key}': {e}");
        }
        for hash in &meta.chunk_hashes {
            self.chunks.release(hash);
        }
        self.txn_touched.insert(key.to_string());
        true
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
                && let Err(e) = wal.log_tag(key, tag_key, tag_value)
            {
                eprintln!("blob WAL: failed to log tag for '{key}': {e}");
            }
            meta.tags.insert(tag_key.to_string(), tag_value.to_string());
            self.txn_touched.insert(key.to_string());
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

    /// Total physical bytes (after dedup) of live chunks.
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

    /// Physically reclaim unreferenced chunk space now (cache + segments).
    /// Normally this happens automatically on mutations; explicit calls are
    /// useful after bulk deletes.
    pub fn gc(&mut self) {
        self.chunks.sweep();
    }

    /// Checkpoint the WAL (truncate to a single manifest snapshot).
    pub fn checkpoint(&self) -> std::io::Result<()> {
        if let Some(wal) = &self.wal {
            let mut snap_blobs = Vec::with_capacity(self.blobs.len());
            for (id, meta) in &self.blobs {
                snap_blobs.push((
                    id.as_str(),
                    meta.content_type.as_deref(),
                    meta.size,
                    meta.wal_chunks(),
                    meta.wal_tags(),
                ));
            }
            let snapshot = BlobMetaSnapshot { blobs: snap_blobs };
            wal.checkpoint(&snapshot)?;
        }
        Ok(())
    }

    /// Capture a snapshot of all mutable blob state for transaction rollback.
    ///
    /// Cheap: manifests and chunk reference counts only — no chunk data. The
    /// snapshot pins garbage collection so every referenced chunk stays
    /// physically readable until the snapshot is dropped (COMMIT) or consumed
    /// by [`Self::txn_restore`] (ROLLBACK).
    pub fn txn_snapshot(&self) -> BlobTxnSnapshot {
        BlobTxnSnapshot {
            blobs: self.blobs.clone(),
            chunks: self.chunks.txn_snapshot(),
        }
    }

    /// Revert only the blob keys in `touched`, using `snap` as the
    /// before-image. Blobs this transaction never wrote are left alone, so a
    /// ROLLBACK cannot destroy another session's committed blob writes.
    ///
    /// Durable: every reverted key gets a compensating WAL record. Chunk
    /// reference counts are repaired only for the chunks the reverted
    /// manifests reference — the snapshot's transaction pin kept that data
    /// physically readable, so `retain` can re-acquire without rewriting.
    pub fn txn_restore_scoped(&mut self, snap: &BlobTxnSnapshot, touched: &HashSet<String>) {
        for key in touched {
            let current = self.blobs.remove(key);
            if let Some(ref meta) = current {
                for hash in &meta.chunk_hashes {
                    self.chunks.release(hash);
                }
            }
            match snap.blobs.get(key) {
                Some(meta) => {
                    for hash in &meta.chunk_hashes {
                        self.chunks.retain(hash);
                    }
                    if let Some(wal) = &self.wal
                        && let Err(e) = wal.log_store_meta(
                            key,
                            meta.content_type.as_deref(),
                            meta.size,
                            &meta.wal_chunks(),
                            &meta.wal_tags(),
                        )
                    {
                        eprintln!("blob WAL: failed to log rollback restore for '{key}': {e}");
                    }
                    self.blobs.insert(key.clone(), meta.clone());
                }
                None => {
                    if current.is_some()
                        && let Some(wal) = &self.wal
                        && let Err(e) = wal.log_delete(key)
                    {
                        eprintln!("blob WAL: failed to log rollback delete for '{key}': {e}");
                    }
                }
            }
        }
        // The restore itself is not a client mutation.
        self.txn_touched.clear();
    }

    /// Restore mutable blob state from a transaction snapshot (for ROLLBACK).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn txn_restore(&mut self, snap: BlobTxnSnapshot) {
        // The WAL already holds entries for the rolled-back mutations; log
        // corrective entries so a replay reconstructs the restored state.
        if let Some(wal) = &self.wal {
            for key in self.blobs.keys() {
                if !snap.blobs.contains_key(key)
                    && let Err(e) = wal.log_delete(key)
                {
                    eprintln!("blob WAL: failed to log rollback delete for '{key}': {e}");
                }
            }
            for (key, meta) in &snap.blobs {
                let differs = match self.blobs.get(key) {
                    None => true,
                    Some(cur) => cur.durable_state_differs(meta),
                };
                if differs
                    && let Err(e) = wal.log_store_meta(
                        key,
                        meta.content_type.as_deref(),
                        meta.size,
                        &meta.wal_chunks(),
                        &meta.wal_tags(),
                    )
                {
                    eprintln!("blob WAL: failed to log rollback restore for '{key}': {e}");
                }
            }
        }
        self.blobs = snap.blobs;
        self.chunks.txn_restore(snap.chunks);
    }
}

/// Snapshot of `BlobStore` mutable state for transaction rollback.
pub struct BlobTxnSnapshot {
    blobs: HashMap<String, BlobMetadata>,
    chunks: ChunkTxnSnapshot,
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

    #[test]
    fn chunk_store_refcount_release() {
        let mut cs = ChunkStore::new();
        let hash = cs.put(vec![1, 2, 3]);
        cs.put(vec![1, 2, 3]); // second reference
        assert_eq!(cs.chunk_count(), 1);

        cs.release(&hash);
        assert!(cs.contains(&hash)); // still one reference
        assert_eq!(cs.stored_bytes(), 3);

        cs.release(&hash);
        assert!(!cs.contains(&hash)); // garbage now
        assert_eq!(cs.stored_bytes(), 0);
        cs.sweep();
        assert!(cs.get(&hash).is_none()); // physically gone

        // Re-put after full release stores it again.
        let hash2 = cs.put(vec![1, 2, 3]);
        assert_eq!(hash, hash2);
        assert_eq!(cs.get(&hash2).unwrap(), vec![1, 2, 3]);
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
        // Unreferenced chunk space is reclaimed.
        assert_eq!(store.total_physical_bytes(), 0);
    }

    #[test]
    fn delete_keeps_shared_chunks() {
        let mut store = BlobStore::with_chunk_size(4);
        let data = b"AAAABBBB";
        store.put("one", data, None);
        store.put("two", data, None);

        assert!(store.delete("one"));
        store.gc();
        // "two" still reads fine — its chunks were shared with "one".
        assert_eq!(store.get("two").unwrap(), data);
        assert_eq!(store.total_physical_bytes(), 8);

        assert!(store.delete("two"));
        store.gc();
        assert_eq!(store.total_physical_bytes(), 0);
    }

    #[test]
    fn overwrite_releases_old_chunks() {
        let mut store = BlobStore::with_chunk_size(4);
        store.put("file", b"XXXXYYYY", None);
        assert_eq!(store.total_physical_bytes(), 8);
        store.put("file", b"XXXXZZZZ", None);
        store.gc();
        // "YYYY" released, "XXXX" shared between versions, "ZZZZ" added.
        assert_eq!(store.total_physical_bytes(), 8);
        assert_eq!(store.get("file").unwrap(), b"XXXXZZZZ");
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
    // Compose (zero-copy concatenation) tests
    // ========================================================================

    #[test]
    fn compose_concatenates_without_copy() {
        let mut store = BlobStore::with_chunk_size(4);
        store.put("p1", b"aaaabbbb", None);
        store.put("p2", b"ccccdd", None); // trailing partial chunk
        store.put("p3", b"eeee", None);

        assert!(store.compose("joined", &["p1", "p2", "p3"], Some("text/plain")));
        assert_eq!(store.get("joined").unwrap(), b"aaaabbbbccccddeeee");
        assert_eq!(store.metadata("joined").unwrap().size, 18);
        // Zero copy: physical bytes unchanged by the compose.
        assert_eq!(store.total_physical_bytes(), 18);
        // Range reads work across part boundaries (partial chunk in middle).
        assert_eq!(store.get_range("joined", 6, 8).unwrap(), b"bbccccdd");

        // Deleting the sources must not break the composed blob.
        store.delete("p1");
        store.delete("p2");
        store.delete("p3");
        store.gc();
        assert_eq!(store.get("joined").unwrap(), b"aaaabbbbccccddeeee");
    }

    #[test]
    fn compose_missing_source_fails() {
        let mut store = BlobStore::new();
        store.put("a", b"data", None);
        assert!(!store.compose("out", &["a", "ghost"], None));
        assert!(store.get("out").is_none());
    }

    #[test]
    fn compose_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            store.put("p1", b"hello ", None);
            store.put("p2", b"world!", None);
            assert!(store.compose("msg", &["p1", "p2"], Some("text/plain")));
            store.delete("p1");
            store.delete("p2");
        }
        let store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        assert_eq!(store.get("msg").unwrap(), b"hello world!");
        assert_eq!(
            store.metadata("msg").unwrap().content_type.as_deref(),
            Some("text/plain")
        );
    }

    // ========================================================================
    // Transaction snapshot/restore tests
    // ========================================================================

    #[test]
    fn txn_rollback_restores_blobs_ram() {
        let mut store = BlobStore::with_chunk_size(4);
        store.put("keep", b"keep me around", None);
        store.put("victim", b"delete me", None);

        let snap = store.txn_snapshot();

        store.delete("victim");
        store.put("keep", b"clobbered!", None);
        store.put("new", b"added in txn", None);
        store.set_tag("keep", "tainted", "yes");

        store.txn_restore(snap);

        assert_eq!(store.blob_count(), 2);
        assert_eq!(store.get("keep").unwrap(), b"keep me around");
        assert_eq!(store.get("victim").unwrap(), b"delete me");
        assert!(store.get("new").is_none());
        assert!(store.metadata("keep").unwrap().tags.is_empty());

        // Post-rollback the store keeps working, and garbage from the
        // rolled-back writes is reclaimable.
        store.put("after", b"life goes on", None);
        assert_eq!(store.get("after").unwrap(), b"life goes on");
    }

    #[test]
    fn txn_commit_drops_snapshot_and_allows_reclaim() {
        let mut store = BlobStore::with_chunk_size(4);
        store.put("a", b"AAAA", None);
        let snap = store.txn_snapshot();
        store.delete("a");
        // Snapshot outstanding: chunk data must survive for a possible rollback.
        drop(snap); // COMMIT
        store.gc();
        assert_eq!(store.total_physical_bytes(), 0);
    }

    #[test]
    fn txn_snapshot_pins_deleted_chunk_data() {
        let mut store = BlobStore::with_chunk_size(4);
        store.put("a", b"precious data", None);
        let snap = store.txn_snapshot();

        store.delete("a");
        // Force sweep attempts while pinned — data must survive.
        store.gc();
        store.put("unrelated", b"xyz", None);

        store.txn_restore(snap);
        assert_eq!(store.get("a").unwrap(), b"precious data");
    }

    // ========================================================================
    // Disk-tiered (WAL + segments) BlobStore tests
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
    fn wal_empty_blob_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("empty", b"", None);
        }
        let store2 = BlobStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("empty").unwrap(), Vec::<u8>::new());
        assert_eq!(store2.metadata("empty").unwrap().size, 0);
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

    #[test]
    fn disk_capacity_beyond_cache() {
        // Cache holds ~2 chunks; store 50 blobs and read them all back —
        // most reads must come off disk.
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_options(dir.path(), 64, 128).unwrap();
            for i in 0..50u8 {
                store.put(&format!("blob-{i}"), &[i; 100], None);
            }
            for i in 0..50u8 {
                assert_eq!(
                    store.get(&format!("blob-{i}")).unwrap(),
                    vec![i; 100],
                    "blob-{i} readable while store is open"
                );
            }
        }
        // And again after restart (cache starts cold).
        let store = BlobStore::open_with_options(dir.path(), 64, 128).unwrap();
        for i in 0..50u8 {
            assert_eq!(store.get(&format!("blob-{i}")).unwrap(), vec![i; 100]);
        }
    }

    #[test]
    fn disk_delete_reclaims_space_after_gc() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = BlobStore::open_with_options(dir.path(), 64, 1024).unwrap();
        for i in 0..20u8 {
            store.put(&format!("b{i}"), &[i; 200], None);
        }
        let before = store.total_physical_bytes();
        for i in 0..20u8 {
            store.delete(&format!("b{i}"));
        }
        store.gc();
        assert_eq!(store.total_physical_bytes(), 0);
        assert!(before > 0);
        // Store still fully functional after compaction.
        store.put("fresh", b"fresh data", None);
        assert_eq!(store.get("fresh").unwrap(), b"fresh data");
        drop(store);
        let store = BlobStore::open_with_options(dir.path(), 64, 1024).unwrap();
        assert_eq!(store.blob_count(), 1);
        assert_eq!(store.get("fresh").unwrap(), b"fresh data");
    }

    #[test]
    fn txn_rollback_durable_across_restart() {
        // A rolled-back transaction must not resurrect on restart: the WAL
        // gets corrective entries at restore time.
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            store.put("stable", b"stable data", None);
            store.put("victim", b"victim data", None);

            let snap = store.txn_snapshot();
            store.delete("victim");
            store.put("phantom", b"phantom data", None);
            store.put("stable", b"clobbered", None);
            store.txn_restore(snap);

            assert_eq!(store.get("victim").unwrap(), b"victim data");
        }
        let store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        assert_eq!(store.blob_count(), 2);
        assert_eq!(store.get("stable").unwrap(), b"stable data");
        assert_eq!(store.get("victim").unwrap(), b"victim data");
        assert!(store.get("phantom").is_none());
    }

    #[test]
    fn txn_rollback_restores_tags_durably() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open(dir.path()).unwrap();
            store.put("doc", b"data", None);
            store.set_tag("doc", "state", "clean");

            let snap = store.txn_snapshot();
            store.set_tag("doc", "state", "dirty");
            store.set_tag("doc", "extra", "junk");
            store.txn_restore(snap);

            assert_eq!(store.metadata("doc").unwrap().tags["state"], "clean");
            assert!(!store.metadata("doc").unwrap().tags.contains_key("extra"));
        }
        let store = BlobStore::open(dir.path()).unwrap();
        let tags = &store.metadata("doc").unwrap().tags;
        assert_eq!(tags["state"], "clean");
        assert!(!tags.contains_key("extra"));
    }

    #[test]
    fn legacy_wal_migrates_to_segments() {
        // Simulate a pre-segment WAL (chunk data embedded in STORE entries),
        // then open: data must migrate into segment files and the WAL must be
        // rewritten metadata-only.
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = BlobWal::open(dir.path()).unwrap();
            let c1 = b"legacy chunk one".to_vec();
            let c2 = b"legacy chunk two".to_vec();
            let h1 = content_hash_blake3(&c1);
            let h2 = content_hash_blake3(&c2);
            wal.log_store_legacy(
                "old_blob",
                Some("text/plain"),
                (c1.len() + c2.len()) as u64,
                &[(h1, c1), (h2, c2)],
            )
            .unwrap();
        }
        let wal_size_before = std::fs::metadata(dir.path().join("blob.wal"))
            .unwrap()
            .len();
        {
            let store = BlobStore::open(dir.path()).unwrap();
            assert_eq!(store.blob_count(), 1);
            assert_eq!(
                store.get("old_blob").unwrap(),
                b"legacy chunk onelegacy chunk two"
            );
        }
        // Post-migration checkpoint rewrote the WAL without chunk data.
        let wal_size_after = std::fs::metadata(dir.path().join("blob.wal"))
            .unwrap()
            .len();
        assert!(wal_size_after < wal_size_before);
        // And it stays readable on subsequent opens.
        let store = BlobStore::open(dir.path()).unwrap();
        assert_eq!(
            store.get("old_blob").unwrap(),
            b"legacy chunk onelegacy chunk two"
        );
    }

    #[test]
    fn checkpoint_truncates_and_preserves_state() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
            for i in 0..10u8 {
                store.put(&format!("k{i}"), &[i; 10], None);
            }
            store.delete("k3");
            store.set_tag("k5", "keep", "yes");
            store.checkpoint().unwrap();
            store.put("post", b"post-checkpoint", None);
        }
        let store = BlobStore::open_with_chunk_size(dir.path(), 4).unwrap();
        assert_eq!(store.blob_count(), 10); // 10 - k3 + post
        assert!(store.get("k3").is_none());
        assert_eq!(store.get("k5").unwrap(), vec![5u8; 10]);
        assert_eq!(store.metadata("k5").unwrap().tags["keep"], "yes");
        assert_eq!(store.get("post").unwrap(), b"post-checkpoint");
    }

    #[test]
    fn eviction_stress_differential() {
        // Tiny cache, many interleaved puts/overwrites/deletes/reads; every
        // observable read must match a plain HashMap reference model.
        let dir = tempfile::tempdir().unwrap();
        let mut store = BlobStore::open_with_options(dir.path(), 16, 256).unwrap();
        let mut model: HashMap<String, Vec<u8>> = HashMap::new();

        let mut seed = 0xDEADBEEFu64;
        let mut rng = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for round in 0..2000u32 {
            let key = format!("k{}", rng() % 40);
            match rng() % 10 {
                0..=4 => {
                    // put (sizes cross chunk boundaries; contents repeat for dedup)
                    let len = (rng() % 100) as usize;
                    let fill = (rng() % 7) as u8;
                    let data = vec![fill; len];
                    store.put(&key, &data, None);
                    model.insert(key, data);
                }
                5..=6 => {
                    let expected = model.get(&key).cloned();
                    let actual = store.get(&key);
                    assert_eq!(actual, expected, "round {round} get({key})");
                }
                7 => {
                    let deleted = store.delete(&key);
                    assert_eq!(deleted, model.remove(&key).is_some(), "round {round}");
                }
                8 => {
                    if let Some(expected) = model.get(&key) {
                        let off = rng() % 50;
                        let len = rng() % 60;
                        let want: Vec<u8> = expected
                            .iter()
                            .skip(off as usize)
                            .take(len as usize)
                            .copied()
                            .collect();
                        let got = store.get_range(&key, off, len).unwrap();
                        assert_eq!(got, want, "round {round} range({key},{off},{len})");
                    }
                }
                _ => store.gc(),
            }
        }

        // Full verification, then restart and verify again.
        for (key, expected) in &model {
            assert_eq!(store.get(key).unwrap(), *expected, "final get({key})");
        }
        drop(store);
        let store = BlobStore::open_with_options(dir.path(), 16, 256).unwrap();
        assert_eq!(store.blob_count(), model.len());
        for (key, expected) in &model {
            assert_eq!(
                store.get(key).unwrap(),
                *expected,
                "post-restart get({key})"
            );
        }
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
