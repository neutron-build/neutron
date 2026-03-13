//! Disk-backed tiered storage for the vector engine.
//!
//! `DiskBackedVectorStore` keeps raw vector data on disk via an [`LsmTree`] while
//! maintaining a bounded in-memory cache of hot vectors. The HNSW/IVFFlat graph
//! structure stays entirely in RAM (it is small — just node IDs and edges), but
//! the actual f32 vector payloads can spill to disk when the dataset exceeds
//! available memory.
//!
//! ## Key design points
//!
//! - **LsmTree storage**: Each vector is keyed by its `u64` ID (8-byte big-endian)
//!   with the value being the raw little-endian f32 bytes.
//! - **LRU-ish cache**: A `HashMap<u64, Vector>` of bounded size. When the cache
//!   exceeds `max_cached_vectors`, the oldest entries (lowest IDs) are evicted.
//! - **Brute-force search**: Iterates all vectors (cache + disk) to compute
//!   distances. For production HNSW integration the graph would reference vector
//!   IDs and load on demand, but that is a future sprint.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use parking_lot::{Mutex, RwLock};

use crate::storage::lsm::{LsmConfig, LsmTree};
use crate::vector::{distance, DistanceMetric, Vector};

// ============================================================================
// Encoding helpers
// ============================================================================

/// Encode a vector ID as an 8-byte big-endian key for the LsmTree.
fn id_to_key(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

/// Decode an 8-byte big-endian key back to a vector ID.
fn key_to_id(key: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&key[..8]);
    u64::from_be_bytes(buf)
}

/// Encode a `Vector` (f32 slice) to raw little-endian bytes.
pub fn encode_vector(v: &Vector) -> Vec<u8> {
    let mut buf = Vec::with_capacity(v.data.len() * 4);
    for &val in &v.data {
        buf.extend_from_slice(&val.to_le_bytes());
    }
    buf
}

/// Decode raw little-endian bytes back to a `Vector`.
pub fn decode_vector(data: &[u8], dims: usize) -> Vector {
    assert_eq!(
        data.len(),
        dims * 4,
        "decode_vector: expected {} bytes for {} dims, got {}",
        dims * 4,
        dims,
        data.len()
    );
    let mut vals = Vec::with_capacity(dims);
    for i in 0..dims {
        let off = i * 4;
        let bytes = [data[off], data[off + 1], data[off + 2], data[off + 3]];
        vals.push(f32::from_le_bytes(bytes));
    }
    Vector::new(vals)
}

// ============================================================================
// DiskBackedVectorStore
// ============================================================================

/// A vector store that keeps raw vector data on disk (via LsmTree) with an
/// in-memory LRU-ish cache for hot vectors.
pub struct DiskBackedVectorStore {
    /// The LsmTree holding all vector data on disk.
    lsm: Mutex<LsmTree>,
    /// In-memory cache of recently-used vectors.
    vector_cache: RwLock<HashMap<u64, Vector>>,
    /// Maximum number of vectors to keep in the cache.
    max_cached_vectors: usize,
    /// Monotonically increasing vector ID counter.
    next_id: AtomicU64,
    /// Vector dimensionality.
    dimensions: usize,
    /// Count of live vectors (inserted minus deleted).
    count: AtomicU64,
}

impl DiskBackedVectorStore {
    /// Create a new in-memory-only store (no disk persistence). Useful for tests.
    pub fn new(dimensions: usize, max_cached: usize) -> Self {
        let config = LsmConfig {
            memtable_flush_threshold: 500,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        Self {
            lsm: Mutex::new(LsmTree::new(config)),
            vector_cache: RwLock::new(HashMap::new()),
            max_cached_vectors: max_cached,
            next_id: AtomicU64::new(1),
            dimensions,
            count: AtomicU64::new(0),
        }
    }

    /// Open a disk-backed store at the given directory, reloading any existing
    /// vectors from SSTable files.
    pub fn open(dir: &str, dimensions: usize, max_cached: usize) -> Self {
        let config = LsmConfig {
            memtable_flush_threshold: 500,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        let path = Path::new(dir);
        let tree = LsmTree::open(config, path)
            .unwrap_or_else(|e| panic!("failed to open LsmTree at {dir}: {e}"));

        // Scan all existing keys to determine next_id and count.
        // We scan the full key range [0x00..8, 0xFF..8].
        let start = [0u8; 8];
        let end = [0xFFu8; 8];
        let entries = tree.range(&start, &end);

        let mut max_id: u64 = 0;
        let mut count: u64 = 0;

        // Also populate cache with up to max_cached entries.
        let mut cache = HashMap::new();
        for (key, value) in &entries {
            if key.len() == 8 {
                let id = key_to_id(key);
                if id >= max_id {
                    max_id = id;
                }
                count += 1;
                if cache.len() < max_cached {
                    let vec = decode_vector(value, dimensions);
                    cache.insert(id, vec);
                }
            }
        }

        Self {
            lsm: Mutex::new(tree),
            vector_cache: RwLock::new(cache),
            max_cached_vectors: max_cached,
            next_id: AtomicU64::new(max_id + 1),
            dimensions,
            count: AtomicU64::new(count),
        }
    }

    /// Insert a vector, assigning it a unique ID. Returns the assigned ID.
    pub fn insert(&self, vector: Vector) -> u64 {
        assert_eq!(
            vector.dim(),
            self.dimensions,
            "vector dimension mismatch: expected {}, got {}",
            self.dimensions,
            vector.dim()
        );

        let id = self.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let key = id_to_key(id);
        let value = encode_vector(&vector);

        // Write to LsmTree.
        {
            let mut lsm = self.lsm.lock();
            lsm.put(key, value);
        }

        // Cache the vector if there is room, otherwise evict.
        {
            let mut cache = self.vector_cache.write();
            if cache.len() >= self.max_cached_vectors {
                self.evict_cache(&mut cache);
            }
            cache.insert(id, vector);
        }

        self.count.fetch_add(1, AtomicOrdering::Relaxed);
        id
    }

    /// Brute-force search: compute distances over all stored vectors and return
    /// the `k` nearest `(id, distance)` pairs sorted by ascending distance.
    pub fn search(&self, query: &Vector, k: usize, metric: DistanceMetric) -> Vec<(u64, f32)> {
        assert_eq!(
            query.dim(),
            self.dimensions,
            "query dimension mismatch: expected {}, got {}",
            self.dimensions,
            query.dim()
        );

        let all_vectors = self.all_vectors();
        let mut scored: Vec<(u64, f32)> = all_vectors
            .iter()
            .map(|(id, v)| (*id, distance(v, query, metric)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    /// Retrieve a single vector by ID. Checks cache first, falls back to disk.
    pub fn get(&self, id: u64) -> Option<Vector> {
        // Check cache first.
        {
            let cache = self.vector_cache.read();
            if let Some(v) = cache.get(&id) {
                return Some(v.clone());
            }
        }

        // Fall back to disk.
        let key = id_to_key(id);
        let data = {
            let lsm = self.lsm.lock();
            lsm.get(&key)?
        };

        let vec = decode_vector(&data, self.dimensions);

        // Promote to cache.
        {
            let mut cache = self.vector_cache.write();
            if cache.len() >= self.max_cached_vectors {
                self.evict_cache(&mut cache);
            }
            cache.insert(id, vec.clone());
        }

        Some(vec)
    }

    /// Delete a vector by ID. Returns `true` if it existed.
    pub fn delete(&self, id: u64) -> bool {
        let key = id_to_key(id);

        // Check existence first.
        let existed = {
            let lsm = self.lsm.lock();
            lsm.get(&key).is_some()
        };

        if !existed {
            return false;
        }

        // Remove from LsmTree (tombstone).
        {
            let mut lsm = self.lsm.lock();
            lsm.delete(key);
        }

        // Remove from cache.
        {
            let mut cache = self.vector_cache.write();
            cache.remove(&id);
        }

        self.count.fetch_sub(1, AtomicOrdering::Relaxed);
        true
    }

    /// Number of live vectors in the store.
    pub fn len(&self) -> usize {
        self.count.load(AtomicOrdering::Relaxed) as usize
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Flush the LsmTree memtable to disk and ensure all cached vectors are
    /// written.
    pub fn flush_cache(&self) {
        // Ensure LsmTree memtable is flushed to SSTable.
        let mut lsm = self.lsm.lock();
        lsm.force_flush();
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Load all vectors from the LsmTree (scanning full key range).
    fn all_vectors(&self) -> Vec<(u64, Vector)> {
        let start = [0u8; 8];
        let end = [0xFFu8; 8];

        let lsm = self.lsm.lock();
        let entries = lsm.range(&start, &end);

        let cache = self.vector_cache.read();

        let mut result = Vec::with_capacity(entries.len());
        for (key, value) in &entries {
            if key.len() == 8 {
                let id = key_to_id(key);
                // Prefer cache (might be more recent for reads, though both should
                // be identical for correctness).
                if let Some(v) = cache.get(&id) {
                    result.push((id, v.clone()));
                } else {
                    result.push((id, decode_vector(value, self.dimensions)));
                }
            }
        }
        result
    }

    /// Evict entries from the cache to make room. Removes the entries with the
    /// smallest IDs (oldest-inserted heuristic).
    fn evict_cache(&self, cache: &mut HashMap<u64, Vector>) {
        if cache.len() < self.max_cached_vectors {
            return;
        }
        // Evict ~25% of entries (the ones with lowest IDs).
        let evict_count = (self.max_cached_vectors / 4).max(1);
        let mut ids: Vec<u64> = cache.keys().copied().collect();
        ids.sort_unstable();
        for &id in ids.iter().take(evict_count) {
            cache.remove(&id);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vec(dims: usize, val: f32) -> Vector {
        Vector::new(vec![val; dims])
    }

    fn rand_vec(dims: usize) -> Vector {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Vector::new((0..dims).map(|_| rng.r#gen::<f32>()).collect())
    }

    // ── Test 1: insert and get ──────────────────────────────────────────

    #[test]
    fn test_disk_vector_insert_get() {
        let store = DiskBackedVectorStore::new(3, 100);
        let v = Vector::new(vec![1.0, 2.0, 3.0]);
        let id = store.insert(v.clone());
        assert_eq!(id, 1);

        let retrieved = store.get(id).unwrap();
        assert_eq!(retrieved.data, v.data);
    }

    // ── Test 2: brute-force search ──────────────────────────────────────

    #[test]
    fn test_disk_vector_search_basic() {
        let store = DiskBackedVectorStore::new(3, 100);
        store.insert(Vector::new(vec![1.0, 0.0, 0.0])); // id=1
        store.insert(Vector::new(vec![0.0, 1.0, 0.0])); // id=2
        store.insert(Vector::new(vec![0.9, 0.1, 0.0])); // id=3

        let query = Vector::new(vec![1.0, 0.0, 0.0]);
        let results = store.search(&query, 2, DistanceMetric::L2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1); // exact match
        assert_eq!(results[1].0, 3); // closest
    }

    // ── Test 3: delete ──────────────────────────────────────────────────

    #[test]
    fn test_disk_vector_delete() {
        let store = DiskBackedVectorStore::new(3, 100);
        let id = store.insert(Vector::new(vec![1.0, 2.0, 3.0]));
        assert_eq!(store.len(), 1);
        assert!(store.delete(id));
        assert_eq!(store.len(), 0);
        assert!(store.get(id).is_none());
        // Double-delete returns false.
        assert!(!store.delete(id));
    }

    // ── Test 4: cache eviction ──────────────────────────────────────────

    #[test]
    fn test_disk_vector_cache_eviction() {
        let store = DiskBackedVectorStore::new(4, 10); // max 10 cached

        // Insert 20 vectors — should trigger eviction.
        for i in 0..20 {
            store.insert(make_vec(4, i as f32));
        }

        // All 20 should be retrievable (from disk if evicted from cache).
        for id in 1..=20u64 {
            assert!(
                store.get(id).is_some(),
                "vector {id} should be retrievable after eviction"
            );
        }
        assert_eq!(store.len(), 20);

        // Cache size should be bounded.
        let cache = store.vector_cache.read();
        assert!(
            cache.len() <= 10,
            "cache should be bounded to max_cached_vectors, got {}",
            cache.len()
        );
    }

    // ── Test 5: cold load from disk on cache miss ───────────────────────

    #[test]
    fn test_disk_vector_cold_load() {
        let store = DiskBackedVectorStore::new(4, 5); // tiny cache

        // Insert 10 vectors.
        for i in 0..10 {
            store.insert(make_vec(4, i as f32));
        }

        // Clear the cache manually to simulate cold start.
        {
            let mut cache = store.vector_cache.write();
            cache.clear();
        }

        // All vectors should still be loadable from disk.
        for id in 1..=10u64 {
            let v = store.get(id);
            assert!(v.is_some(), "vector {id} should be loadable from disk");
        }
    }

    // ── Test 6: persistence across reopen ───────────────────────────────

    #[test]
    fn test_disk_vector_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap();

        // Insert vectors and flush.
        {
            let store = DiskBackedVectorStore::open(dir_str, 4, 100);
            store.insert(Vector::new(vec![1.0, 2.0, 3.0, 4.0]));
            store.insert(Vector::new(vec![5.0, 6.0, 7.0, 8.0]));
            store.flush_cache();
        }

        // Reopen and verify data survived.
        {
            let store = DiskBackedVectorStore::open(dir_str, 4, 100);
            assert_eq!(store.len(), 2);

            let v1 = store.get(1).unwrap();
            assert_eq!(v1.data, vec![1.0, 2.0, 3.0, 4.0]);

            let v2 = store.get(2).unwrap();
            assert_eq!(v2.data, vec![5.0, 6.0, 7.0, 8.0]);
        }
    }

    // ── Test 7: large dataset ───────────────────────────────────────────

    #[test]
    fn test_disk_vector_large_dataset() {
        let store = DiskBackedVectorStore::new(16, 500); // cache only 500

        // Insert 5000 vectors.
        for _ in 0..5000 {
            store.insert(rand_vec(16));
        }
        assert_eq!(store.len(), 5000);

        // Search should work across all vectors.
        let query = rand_vec(16);
        let results = store.search(&query, 10, DistanceMetric::L2);
        assert_eq!(results.len(), 10);

        // Results should be sorted by distance ascending.
        for i in 1..results.len() {
            assert!(results[i].1 >= results[i - 1].1);
        }
    }

    // ── Test 8: encoding roundtrip ──────────────────────────────────────

    #[test]
    fn test_disk_vector_encoding_roundtrip() {
        let original = Vector::new(vec![
            1.0,
            -2.5,
            3.14159,
            0.0,
            f32::MIN,
            f32::MAX,
            f32::EPSILON,
            -0.0,
        ]);
        let encoded = encode_vector(&original);
        assert_eq!(encoded.len(), 8 * 4);

        let decoded = decode_vector(&encoded, 8);
        assert_eq!(original.data.len(), decoded.data.len());
        for (a, b) in original.data.iter().zip(decoded.data.iter()) {
            assert!(
                a.to_bits() == b.to_bits(),
                "bit-exact roundtrip failed: {a} vs {b}"
            );
        }
    }

    // ── Test 9: multiple distance metrics ───────────────────────────────

    #[test]
    fn test_disk_vector_multiple_metrics() {
        let store = DiskBackedVectorStore::new(4, 100);
        store.insert(Vector::new(vec![1.0, 0.0, 0.0, 0.0])); // id=1
        store.insert(Vector::new(vec![0.0, 1.0, 0.0, 0.0])); // id=2
        store.insert(Vector::new(vec![0.9, 0.1, 0.0, 0.0])); // id=3

        let query = Vector::new(vec![1.0, 0.0, 0.0, 0.0]);

        // L2
        let l2 = store.search(&query, 1, DistanceMetric::L2);
        assert_eq!(l2[0].0, 1);
        assert!(l2[0].1 < 1e-6);

        // Cosine
        let cos = store.search(&query, 1, DistanceMetric::Cosine);
        assert_eq!(cos[0].0, 1);
        assert!(cos[0].1 < 1e-6);

        // InnerProduct (negate: lower is more similar)
        let ip = store.search(&query, 1, DistanceMetric::InnerProduct);
        assert_eq!(ip[0].0, 1);
    }

    // ── Test 10: len accuracy ───────────────────────────────────────────

    #[test]
    fn test_disk_vector_len() {
        let store = DiskBackedVectorStore::new(3, 100);
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());

        store.insert(make_vec(3, 1.0));
        assert_eq!(store.len(), 1);

        store.insert(make_vec(3, 2.0));
        assert_eq!(store.len(), 2);

        let id = 1;
        store.delete(id);
        assert_eq!(store.len(), 1);

        store.delete(2);
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }
}
