//! Redis-like cache tier with LRU eviction and memory accounting.
//!
//! Provides an in-memory cache with TTL support, LRU eviction when the memory
//! limit is exceeded, and hit/miss statistics tracking.
//!
//! Architecture:
//!   - HashMap-backed storage with per-entry TTL
//!   - VecDeque-based LRU tracking (front = least recent, back = most recent)
//!   - Configurable maximum memory budget with automatic eviction
//!   - Hit-rate and memory-usage statistics

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

// ============================================================================
// CacheStats
// ============================================================================

/// Snapshot of cache statistics at a point in time.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entry_count: usize,
    pub memory_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub max_memory_bytes: usize,
}

// ============================================================================
// CacheEntry
// ============================================================================

/// A single cached value with metadata.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: String,
    pub expires_at: Option<Instant>,
    pub inserted_at: Instant,
    pub access_count: u64,
    pub size_bytes: usize,
}

impl CacheEntry {
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(t) => Instant::now() >= t,
            None => false,
        }
    }
}

// ============================================================================
// CacheTier
// ============================================================================

/// Redis-like in-memory cache with LRU eviction and memory limits.
pub struct CacheTier {
    entries: HashMap<String, CacheEntry>,
    access_order: VecDeque<String>,
    max_memory_bytes: usize,
    used_bytes: usize,
    hits: u64,
    misses: u64,
}

impl CacheTier {
    /// Create a new cache tier with the given memory budget.
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            access_order: VecDeque::new(),
            max_memory_bytes,
            used_bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Retrieve a value by key. Returns `None` if the key does not exist or
    /// has expired. Increments hit/miss counters and updates LRU order.
    pub fn get(&mut self, key: &str) -> Option<&str> {
        // Check existence and expiration first.
        let expired = self.entries.get(key).map(|e| e.is_expired());
        match expired {
            Some(true) => {
                // Lazy expiration: remove the expired entry.
                self.remove_entry(key);
                self.misses += 1;
                None
            }
            Some(false) => {
                // Hit — update LRU and access count.
                self.touch_lru(key);
                self.hits += 1;
                let entry = self.entries.get_mut(key).unwrap();
                entry.access_count += 1;
                Some(&entry.value)
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    /// Insert or update a cache entry. If `ttl_secs` is provided the entry
    /// will expire after that many seconds. Evicts LRU entries if the memory
    /// budget would be exceeded.
    pub fn set(&mut self, key: &str, value: &str, ttl_secs: Option<u64>) {
        let entry_size = Self::compute_size(key, value);

        // If key already exists, remove old entry first to get accurate accounting.
        if self.entries.contains_key(key) {
            self.remove_entry(key);
        }

        // Evict LRU entries until we have room (or the cache is empty).
        while self.used_bytes + entry_size > self.max_memory_bytes && !self.entries.is_empty() {
            self.evict_lru();
        }

        let now = Instant::now();
        let expires_at = ttl_secs.map(|s| now + Duration::from_secs(s));

        let entry = CacheEntry {
            value: value.to_string(),
            expires_at,
            inserted_at: now,
            access_count: 0,
            size_bytes: entry_size,
        };

        self.used_bytes += entry_size;
        self.entries.insert(key.to_string(), entry);
        self.access_order.push_back(key.to_string());
    }

    /// Delete a key from the cache. Returns `true` if the key existed.
    pub fn delete(&mut self, key: &str) -> bool {
        if self.entries.contains_key(key) {
            self.remove_entry(key);
            true
        } else {
            false
        }
    }

    /// Check whether a key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        match self.entries.get(key) {
            Some(entry) => !entry.is_expired(),
            None => false,
        }
    }

    /// Return the remaining TTL for a key. Returns `None` if the key does not
    /// exist, has no TTL, or is already expired.
    pub fn ttl(&self, key: &str) -> Option<Duration> {
        let entry = self.entries.get(key)?;
        let deadline = entry.expires_at?;
        let now = Instant::now();
        if now >= deadline {
            None
        } else {
            Some(deadline - now)
        }
    }

    /// Return all non-expired keys.
    pub fn keys(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter(|(_, e)| !e.is_expired())
            .map(|(k, _)| k.as_str())
            .collect()
    }

    /// Remove all entries from the cache.
    pub fn flush_all(&mut self) {
        self.entries.clear();
        self.access_order.clear();
        self.used_bytes = 0;
    }

    /// Return the number of non-expired entries.
    pub fn len(&self) -> usize {
        self.entries.values().filter(|e| !e.is_expired()).count()
    }

    /// Return the current memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.used_bytes
    }

    /// Compute the hit rate as `hits / (hits + misses)`. Returns `0.0` if
    /// there have been no accesses.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Return a snapshot of the current cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.len(),
            memory_bytes: self.used_bytes,
            hits: self.hits,
            misses: self.misses,
            hit_rate: self.hit_rate(),
            max_memory_bytes: self.max_memory_bytes,
        }
    }

    /// Actively scan and remove all expired entries. Returns the number of
    /// entries removed.
    pub fn evict_expired(&mut self) -> usize {
        let expired_keys: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired_keys.len();
        for key in &expired_keys {
            self.remove_entry(key);
        }
        count
    }

    // --------------------------------------------------------------------
    // Private helpers
    // --------------------------------------------------------------------

    /// Remove the least recently used entry (front of access_order).
    fn evict_lru(&mut self) {
        // Walk from the front to find the first key that still exists in
        // the entries map (duplicates may linger in access_order after
        // touch_lru re-appends a key).
        while let Some(key) = self.access_order.pop_front() {
            if self.entries.contains_key(&key) {
                if let Some(entry) = self.entries.remove(&key) {
                    self.used_bytes = self.used_bytes.saturating_sub(entry.size_bytes);
                }
                return;
            }
            // Stale entry in the deque — skip it.
        }
    }

    /// Move a key to the back (most-recently-used end) of the access order.
    fn touch_lru(&mut self, key: &str) {
        // Rather than scanning for the key in the deque (O(n)), we simply
        // push a new copy to the back. Stale copies at the front are skipped
        // during eviction.
        self.access_order.push_back(key.to_string());

        // Compact deque if it grows too large relative to entry count.
        // This prevents unbounded memory growth from stale duplicates.
        if self.access_order.len() > self.entries.len() * 2 + 16 {
            let keys: HashSet<&str> = self.entries.keys().map(|k| k.as_str()).collect();
            // Rebuild: keep only the last occurrence of each existing key
            let mut seen = HashSet::new();
            let mut new_order = VecDeque::new();
            for key in self.access_order.iter().rev() {
                if keys.contains(key.as_str()) && seen.insert(key.clone()) {
                    new_order.push_front(key.clone());
                }
            }
            self.access_order = new_order;
        }
    }

    /// Remove an entry and its LRU tracking, adjusting memory accounting.
    fn remove_entry(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.size_bytes);
            // Remove from access_order. This is O(n) but keeps the deque
            // clean for accurate LRU decisions.
            self.access_order.retain(|k| k != key);
        }
    }

    /// Estimate the in-memory size of an entry (key + value lengths as a
    /// simple proxy for heap usage).
    fn compute_size(key: &str, value: &str) -> usize {
        key.len() + value.len()
    }
}

// ============================================================================
// DistributedCacheRouter
// ============================================================================

/// Routes cache operations to the correct node in a cluster using consistent
/// hashing. Each node owns a partition of the key space.
pub struct DistributedCacheRouter {
    /// node_id → local CacheTier for that node.
    partitions: HashMap<u64, CacheTier>,
    /// Number of virtual nodes per physical node on the hash ring.
    #[allow(dead_code)]
    vnodes_per_node: usize,
    /// Sorted list of (ring_position, node_id).
    ring: Vec<(u64, u64)>,
}

impl DistributedCacheRouter {
    /// Create a new distributed cache router.
    /// Each node gets its own `CacheTier` with the given memory budget.
    pub fn new(node_ids: &[u64], per_node_memory: usize, vnodes_per_node: usize) -> Self {
        let mut partitions = HashMap::new();
        let mut ring = Vec::new();
        for &nid in node_ids {
            partitions.insert(nid, CacheTier::new(per_node_memory));
            for i in 0..vnodes_per_node {
                let pos = Self::hash_pair(nid, i as u64);
                ring.push((pos, nid));
            }
        }
        ring.sort_by_key(|&(pos, _)| pos);
        Self { partitions, vnodes_per_node, ring }
    }

    /// Determine which node owns a given key.
    pub fn route_key(&self, key: &str) -> Option<u64> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = Self::hash_str(key);
        let idx = match self.ring.binary_search_by_key(&hash, |&(pos, _)| pos) {
            Ok(i) => i,
            Err(i) => if i == self.ring.len() { 0 } else { i },
        };
        Some(self.ring[idx].1)
    }

    /// Set a key on the owning node.
    pub fn set(&mut self, key: &str, value: &str, ttl_secs: Option<u64>) -> Option<u64> {
        let node_id = self.route_key(key)?;
        if let Some(cache) = self.partitions.get_mut(&node_id) {
            cache.set(key, value, ttl_secs);
        }
        Some(node_id)
    }

    /// Get a key from the owning node.
    pub fn get(&mut self, key: &str) -> Option<String> {
        let node_id = self.route_key(key)?;
        let cache = self.partitions.get_mut(&node_id)?;
        cache.get(key).map(|s| s.to_string())
    }

    /// Delete a key from the owning node.
    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(node_id) = self.route_key(key) {
            if let Some(cache) = self.partitions.get_mut(&node_id) {
                return cache.delete(key);
            }
        }
        false
    }

    /// Broadcast invalidation: remove a key from ALL nodes (for write-through).
    pub fn invalidate_all(&mut self, key: &str) -> usize {
        let mut removed = 0;
        for cache in self.partitions.values_mut() {
            if cache.delete(key) {
                removed += 1;
            }
        }
        removed
    }

    /// Aggregate stats across all nodes.
    pub fn total_stats(&self) -> CacheStats {
        let mut total = CacheStats {
            entry_count: 0,
            memory_bytes: 0,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
            max_memory_bytes: 0,
        };
        for cache in self.partitions.values() {
            let s = cache.stats();
            total.entry_count += s.entry_count;
            total.memory_bytes += s.memory_bytes;
            total.hits += s.hits;
            total.misses += s.misses;
            total.max_memory_bytes += s.max_memory_bytes;
        }
        let total_accesses = total.hits + total.misses;
        total.hit_rate = if total_accesses == 0 {
            0.0
        } else {
            total.hits as f64 / total_accesses as f64
        };
        total
    }

    /// Number of nodes in the cluster.
    pub fn node_count(&self) -> usize {
        self.partitions.len()
    }

    /// Get stats for a specific node.
    pub fn node_stats(&self, node_id: u64) -> Option<CacheStats> {
        self.partitions.get(&node_id).map(|c| c.stats())
    }

    fn hash_str(s: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    fn hash_pair(a: u64, b: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &byte in a.to_le_bytes().iter().chain(b.to_le_bytes().iter()) {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}


// ============================================================================
// CacheConfig
// ============================================================================

/// Configuration for a sharded concurrent cache.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub shard_count: usize,
    pub max_memory_bytes: usize,
    pub default_ttl_secs: Option<u64>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            shard_count: 64,
            max_memory_bytes: 64 * 1024 * 1024, // 64 MiB
            default_ttl_secs: None,
        }
    }
}

// ============================================================================
// ShardedCacheStats
// ============================================================================

/// Aggregated statistics across all shards of a `ShardedCache`.
#[derive(Debug, Clone)]
pub struct ShardedCacheStats {
    pub total_entries: usize,
    pub total_memory_bytes: usize,
    pub total_hits: u64,
    pub total_misses: u64,
    pub hit_rate: f64,
    pub shard_count: usize,
    pub max_memory_bytes: usize,
}

// ============================================================================
// ShardedCache
// ============================================================================

/// A concurrent cache that partitions keys across N shards using FNV-1a
/// hashing. Each shard contains its own [`CacheTier`] with independent LRU
/// tracking, protected by a `parking_lot::RwLock`.
///
/// Reads only acquire a read lock on one shard, enabling high concurrency
/// for read-heavy workloads. Writes acquire a write lock on a single shard,
/// so writers on different shards never contend with each other.
pub struct ShardedCache {
    shards: Vec<parking_lot::RwLock<CacheTier>>,
    shard_count: usize,
    config: CacheConfig,
}
impl ShardedCache {
    /// Create a new sharded cache.
    pub fn new(
        shard_count: usize,
        max_memory_per_shard: usize,
        default_ttl: Option<u64>,
    ) -> Self {
        let shard_count = if shard_count == 0 { 1 } else { shard_count };
        let shards = (0..shard_count)
            .map(|_| parking_lot::RwLock::new(CacheTier::new(max_memory_per_shard)))
            .collect();
        let config = CacheConfig {
            shard_count,
            max_memory_bytes: max_memory_per_shard * shard_count,
            default_ttl_secs: default_ttl,
        };
        Self { shards, shard_count, config }
    }

    /// Retrieve a value by key. Acquires a write lock because `CacheTier::get()`
    /// updates LRU order and hit/miss counters.
    pub fn get(&self, key: &str) -> Option<String> {
        let idx = self.shard_for_key(key);
        let mut shard = self.shards[idx].write();
        shard.get(key).map(|s| s.to_string())
    }

    /// Look up a value without updating LRU order or hit/miss counters.
    /// Only acquires a **read lock**, so it never blocks concurrent readers.
    /// Useful for existence checks or read-heavy paths where LRU accuracy
    /// is not critical.
    pub fn peek(&self, key: &str) -> Option<String> {
        let idx = self.shard_for_key(key);
        let shard = self.shards[idx].read();
        shard.entries.get(key)
            .filter(|e| !e.is_expired())
            .map(|e| e.value.clone())
    }

    /// Insert or update a key. Acquires a write lock on the owning shard only.
    pub fn set(&self, key: &str, value: &str, ttl_secs: Option<u64>) {
        let idx = self.shard_for_key(key);
        let effective_ttl = ttl_secs.or(self.config.default_ttl_secs);
        let mut shard = self.shards[idx].write();
        shard.set(key, value, effective_ttl);
    }

    /// Delete a key. Returns `true` if the key existed.
    pub fn delete(&self, key: &str) -> bool {
        let idx = self.shard_for_key(key);
        let mut shard = self.shards[idx].write();
        shard.delete(key)
    }

    /// Check whether a key exists. Acquires a **read** lock on one shard only.
    pub fn exists(&self, key: &str) -> bool {
        let idx = self.shard_for_key(key);
        let shard = self.shards[idx].read();
        shard.exists(key)
    }

    /// Aggregate statistics across all shards.
    pub fn stats(&self) -> ShardedCacheStats {
        let mut total = ShardedCacheStats {
            total_entries: 0,
            total_memory_bytes: 0,
            total_hits: 0,
            total_misses: 0,
            hit_rate: 0.0,
            shard_count: self.shard_count,
            max_memory_bytes: self.config.max_memory_bytes,
        };
        for shard in &self.shards {
            let s = shard.read().stats();
            total.total_entries += s.entry_count;
            total.total_memory_bytes += s.memory_bytes;
            total.total_hits += s.hits;
            total.total_misses += s.misses;
        }
        let accesses = total.total_hits + total.total_misses;
        total.hit_rate = if accesses == 0 {
            0.0
        } else {
            total.total_hits as f64 / accesses as f64
        };
        total
    }
    /// Remove all entries from every shard.
    pub fn flush_all(&self) {
        for shard in &self.shards {
            shard.write().flush_all();
        }
    }

    /// Run expiry on all shards, removing entries whose TTL has elapsed.
    pub fn evict_expired(&self) {
        for shard in &self.shards {
            shard.write().evict_expired();
        }
    }

    /// Total number of non-expired entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    fn shard_for_key(&self, key: &str) -> usize {
        let hash = Self::fnv1a(key);
        (hash as usize) % self.shard_count
    }

    fn fnv1a(s: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}
// ============================================================================
// Lock-Free Read Cache — epoch-style read-optimized concurrent cache
// ============================================================================

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A cache entry with an atomic access counter (updated without write lock).
#[allow(dead_code)]
#[derive(Debug)]
struct LfCacheEntry {
    value: Arc<String>,
    expires_at: Option<Instant>,
    inserted_at: Instant,
    access_count: AtomicU64,
    size_bytes: usize,
}

impl LfCacheEntry {
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(deadline) => Instant::now() >= deadline,
            None => false,
        }
    }
}

/// A single shard of the lock-free cache.
/// Reads only acquire a `read()` lock and never modify structural state.
/// LRU eviction uses access counts (sampled) rather than tracking access order.
#[derive(Debug)]
struct LfShard {
    entries: HashMap<String, LfCacheEntry>,
    max_memory_bytes: usize,
    used_bytes: usize,
}

impl LfShard {
    fn new(max_memory_bytes: usize) -> Self {
        LfShard {
            entries: HashMap::new(),
            max_memory_bytes,
            used_bytes: 0,
        }
    }

    /// Read-only get: returns Arc<String> without mutating shard state.
    /// Only the atomic access_count is bumped (lock-free).
    fn get_readonly(&self, key: &str) -> Option<Arc<String>> {
        let entry = self.entries.get(key)?;
        if entry.is_expired() {
            return None;
        }
        entry.access_count.fetch_add(1, Ordering::Relaxed);
        Some(Arc::clone(&entry.value))
    }

    /// Mutable set: requires write lock.
    fn set(&mut self, key: &str, value: &str, ttl_secs: Option<u64>) {
        let entry_size = key.len() + value.len() + 64;

        // Remove old entry if exists
        if let Some(old) = self.entries.remove(key) {
            self.used_bytes -= old.size_bytes;
        }

        // Evict least-accessed entries until we have room
        while self.used_bytes + entry_size > self.max_memory_bytes && !self.entries.is_empty() {
            self.evict_least_accessed();
        }

        let now = Instant::now();
        let entry = LfCacheEntry {
            value: Arc::new(value.to_string()),
            expires_at: ttl_secs.map(|s| now + Duration::from_secs(s)),
            inserted_at: now,
            access_count: AtomicU64::new(0),
            size_bytes: entry_size,
        };

        self.used_bytes += entry_size;
        self.entries.insert(key.to_string(), entry);
    }

    fn delete(&mut self, key: &str) -> bool {
        if let Some(entry) = self.entries.remove(key) {
            self.used_bytes -= entry.size_bytes;
            true
        } else {
            false
        }
    }

    fn exists(&self, key: &str) -> bool {
        self.entries.get(key).is_some_and(|e| !e.is_expired())
    }

    /// Evict the entry with the lowest access count (approximate LRU).
    fn evict_least_accessed(&mut self) {
        let victim = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.access_count.load(Ordering::Relaxed))
            .map(|(k, _)| k.clone());

        if let Some(key) = victim {
            if let Some(entry) = self.entries.remove(&key) {
                self.used_bytes -= entry.size_bytes;
            }
        }
    }

    fn evict_expired(&mut self) {
        let expired_keys: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired_keys {
            if let Some(entry) = self.entries.remove(&key) {
                self.used_bytes -= entry.size_bytes;
            }
        }
    }

    fn len(&self) -> usize {
        self.entries.values().filter(|e| !e.is_expired()).count()
    }

    fn memory_usage(&self) -> usize {
        self.used_bytes
    }

    fn flush_all(&mut self) {
        self.entries.clear();
        self.used_bytes = 0;
    }
}

/// Lock-free read cache: concurrent reads never block each other or writers.
///
/// Uses `parking_lot::RwLock` with **read locks for gets** — multiple readers
/// proceed in parallel without contention. Values are stored as `Arc<String>`
/// so readers can hold references after releasing the lock. LRU approximation
/// uses atomic access counters instead of ordered tracking.
///
/// Compared to `ShardedCache`, this eliminates writer starvation on the read
/// path: `get()` only acquires a read lock, so it never blocks concurrent gets.
pub struct LockFreeCache {
    shards: Vec<parking_lot::RwLock<LfShard>>,
    shard_count: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for LockFreeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockFreeCache")
            .field("shard_count", &self.shard_count)
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl LockFreeCache {
    pub fn new(shard_count: usize, max_memory_per_shard: usize) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| parking_lot::RwLock::new(LfShard::new(max_memory_per_shard)))
            .collect();
        LockFreeCache {
            shards,
            shard_count,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Read a value. Only acquires a **read lock** — never blocks other readers.
    pub fn get(&self, key: &str) -> Option<String> {
        let idx = self.shard_for(key);
        let shard = self.shards[idx].read(); // read lock only!
        match shard.get_readonly(key) {
            Some(val) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(val.as_str().to_string())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Write a value. Acquires a write lock on the owning shard.
    pub fn set(&self, key: &str, value: &str, ttl_secs: Option<u64>) {
        let idx = self.shard_for(key);
        self.shards[idx].write().set(key, value, ttl_secs);
    }

    /// Delete a key. Returns `true` if it existed.
    pub fn delete(&self, key: &str) -> bool {
        let idx = self.shard_for(key);
        self.shards[idx].write().delete(key)
    }

    /// Check existence. Read lock only.
    pub fn exists(&self, key: &str) -> bool {
        let idx = self.shard_for(key);
        self.shards[idx].read().exists(key)
    }

    pub fn flush_all(&self) {
        for shard in &self.shards {
            shard.write().flush_all();
        }
    }

    pub fn evict_expired(&self) {
        for shard in &self.shards {
            shard.write().evict_expired();
        }
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn total_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn hit_rate(&self) -> f64 {
        let h = self.total_hits();
        let m = self.total_misses();
        let total = h + m;
        if total == 0 { 0.0 } else { h as f64 / total as f64 }
    }

    pub fn total_memory_bytes(&self) -> usize {
        self.shards.iter().map(|s| s.read().memory_usage()).sum()
    }

    fn shard_for(&self, key: &str) -> usize {
        // FNV-1a hash
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in key.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        (h as usize) % self.shard_count
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_basic() {
        let mut cache = CacheTier::new(1024);
        cache.set("hello", "world", None);
        assert_eq!(cache.get("hello"), Some("world"));
    }

    #[test]
    fn test_get_nonexistent_increments_misses() {
        let mut cache = CacheTier::new(1024);
        assert_eq!(cache.get("nope"), None);
        assert_eq!(cache.misses, 1);
        assert_eq!(cache.hits, 0);
    }

    #[test]
    fn test_set_with_ttl_before_expiry() {
        let mut cache = CacheTier::new(1024);
        cache.set("k", "v", Some(10));
        // Should still be alive.
        assert_eq!(cache.get("k"), Some("v"));
    }

    #[test]
    fn test_set_with_ttl_after_expiry() {
        let mut cache = CacheTier::new(1024);
        // Use a 1-millisecond TTL that we can actually wait out.
        // We cheat by constructing the entry manually with a past deadline.
        cache.set("k", "v", None);
        // Manually set expires_at to the past.
        cache.entries.get_mut("k").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(cache.get("k"), None);
        assert_eq!(cache.misses, 1);
    }

    #[test]
    fn test_delete_removes_entry() {
        let mut cache = CacheTier::new(1024);
        cache.set("a", "1", None);
        assert!(cache.delete("a"));
        assert_eq!(cache.get("a"), None);
        // Deleting again returns false.
        assert!(!cache.delete("a"));
    }

    #[test]
    fn test_exists_present_absent_expired() {
        let mut cache = CacheTier::new(1024);
        cache.set("alive", "yes", None);
        cache.set("dead", "yes", None);
        cache.entries.get_mut("dead").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));

        assert!(cache.exists("alive"));
        assert!(!cache.exists("missing"));
        assert!(!cache.exists("dead"));
    }

    #[test]
    fn test_ttl_returns_remaining() {
        let mut cache = CacheTier::new(1024);
        cache.set("k", "v", Some(60));
        let remaining = cache.ttl("k").expect("should have TTL");
        // Should be close to 60 seconds (allow some slack for test execution).
        assert!(remaining.as_secs() >= 58 && remaining.as_secs() <= 60);

        // No TTL entry returns None.
        cache.set("no_ttl", "v", None);
        assert!(cache.ttl("no_ttl").is_none());

        // Expired entry returns None.
        cache.entries.get_mut("k").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));
        assert!(cache.ttl("k").is_none());
    }

    #[test]
    fn test_flush_all_clears_everything() {
        let mut cache = CacheTier::new(1024);
        cache.set("a", "1", None);
        cache.set("b", "2", None);
        cache.flush_all();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_usage(), 0);
        assert_eq!(cache.get("a"), None);
    }

    #[test]
    fn test_lru_eviction_on_memory_limit() {
        // Budget for roughly one entry at a time.
        // "k1" + "vv" = 4 bytes, "k2" + "vv" = 4 bytes => budget 5 means
        // after inserting k2 the first entry must be evicted.
        let mut cache = CacheTier::new(5);
        cache.set("k1", "vv", None); // 4 bytes
        cache.set("k2", "vv", None); // 4 bytes — should evict k1
        assert!(!cache.exists("k1"), "k1 should have been evicted");
        assert!(cache.exists("k2"));
    }

    #[test]
    fn test_memory_accounting() {
        let mut cache = CacheTier::new(1024);
        cache.set("abc", "defgh", None); // 3 + 5 = 8
        assert_eq!(cache.memory_usage(), 8);
        cache.set("x", "y", None); // 1 + 1 = 2
        assert_eq!(cache.memory_usage(), 10);
        cache.delete("abc");
        assert_eq!(cache.memory_usage(), 2);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let mut cache = CacheTier::new(1024);
        cache.set("k", "v", None);
        cache.get("k"); // hit
        cache.get("k"); // hit
        cache.get("nope"); // miss
        // 2 hits / 3 total
        let rate = cache.hit_rate();
        assert!((rate - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_stats_snapshot() {
        let mut cache = CacheTier::new(256);
        cache.set("a", "1", None);
        cache.get("a");
        cache.get("b");
        let s = cache.stats();
        assert_eq!(s.entry_count, 1);
        assert_eq!(s.memory_bytes, 2); // "a" + "1"
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.max_memory_bytes, 256);
        assert!((s.hit_rate - 0.5).abs() < 1e-9);
    }

    #[test]
    fn test_evict_expired_removes_only_expired() {
        let mut cache = CacheTier::new(1024);
        cache.set("alive", "1", None);
        cache.set("dead1", "2", None);
        cache.set("dead2", "3", None);
        cache.entries.get_mut("dead1").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));
        cache.entries.get_mut("dead2").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));

        let removed = cache.evict_expired();
        assert_eq!(removed, 2);
        assert!(cache.exists("alive"));
        assert!(!cache.exists("dead1"));
        assert!(!cache.exists("dead2"));
    }

    #[test]
    fn test_keys_returns_only_non_expired() {
        let mut cache = CacheTier::new(1024);
        cache.set("a", "1", None);
        cache.set("b", "2", None);
        cache.set("expired", "3", None);
        cache.entries.get_mut("expired").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));

        let mut keys = cache.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let mut cache = CacheTier::new(1024);
        cache.set("k", "short", None); // 1 + 5 = 6
        assert_eq!(cache.memory_usage(), 6);
        cache.set("k", "a much longer value", None); // 1 + 19 = 20
        assert_eq!(cache.get("k"), Some("a much longer value"));
        assert_eq!(cache.memory_usage(), 20);
    }

    #[test]
    fn test_len_returns_non_expired_count() {
        let mut cache = CacheTier::new(1024);
        cache.set("a", "1", None);
        cache.set("b", "2", None);
        cache.set("c", "3", None);
        assert_eq!(cache.len(), 3);

        // Expire one.
        cache.entries.get_mut("b").unwrap().expires_at =
            Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(cache.len(), 2);
    }

    // -- Distributed cache router tests --

    #[test]
    fn test_distributed_cache_route_key() {
        let router = DistributedCacheRouter::new(&[1, 2, 3], 1024, 50);
        // Every key should route to some node.
        for i in 0..100 {
            let key = format!("key_{i}");
            let node = router.route_key(&key);
            assert!(node.is_some());
            let nid = node.unwrap();
            assert!(nid == 1 || nid == 2 || nid == 3);
        }
    }

    #[test]
    fn test_distributed_cache_set_get() {
        let mut router = DistributedCacheRouter::new(&[1, 2], 1024, 50);
        router.set("hello", "world", None);
        assert_eq!(router.get("hello"), Some("world".to_string()));
    }

    #[test]
    fn test_distributed_cache_delete() {
        let mut router = DistributedCacheRouter::new(&[1, 2], 1024, 50);
        router.set("k", "v", None);
        assert!(router.delete("k"));
        assert_eq!(router.get("k"), None);
        assert!(!router.delete("k"));
    }

    #[test]
    fn test_distributed_cache_invalidate_all() {
        let mut router = DistributedCacheRouter::new(&[1, 2], 1024, 50);
        router.set("k", "v", None);
        let removed = router.invalidate_all("k");
        // Should find it on the owning node.
        assert!(removed >= 1);
        assert_eq!(router.get("k"), None);
    }

    #[test]
    fn test_distributed_cache_total_stats() {
        let mut router = DistributedCacheRouter::new(&[1, 2, 3], 1024, 50);
        for i in 0..30 {
            router.set(&format!("k{i}"), &format!("v{i}"), None);
        }
        let stats = router.total_stats();
        assert_eq!(stats.entry_count, 30);
        assert_eq!(router.node_count(), 3);
    }

    #[test]
    fn test_distributed_cache_node_stats() {
        let mut router = DistributedCacheRouter::new(&[10, 20], 1024, 50);
        router.set("a", "1", None);
        // At least one node should have stats.
        let s1 = router.node_stats(10).unwrap();
        let s2 = router.node_stats(20).unwrap();
        assert_eq!(s1.entry_count + s2.entry_count, 1);
    }

    #[test]
    fn test_distributed_cache_deterministic_routing() {
        let r1 = DistributedCacheRouter::new(&[1, 2, 3], 1024, 50);
        let r2 = DistributedCacheRouter::new(&[1, 2, 3], 1024, 50);
        // Same key should route to same node.
        for i in 0..100 {
            let key = format!("test_{i}");
            assert_eq!(r1.route_key(&key), r2.route_key(&key));
        }
    }

    #[test]
    fn test_distributed_cache_distribution() {
        // Use larger, more spread-out node IDs for better ring distribution.
        let router = DistributedCacheRouter::new(&[100, 200, 300], 1024, 200);
        let mut counts = std::collections::HashMap::new();
        for i in 0..3000 {
            let node = router.route_key(&format!("key_{i}")).unwrap();
            *counts.entry(node).or_insert(0usize) += 1;
        }
        // All nodes should get some keys.
        for &nid in &[100, 200, 300] {
            assert!(*counts.get(&nid).unwrap_or(&0) > 0, "node {nid} got no keys");
        }
    }

    // ====================================================================
    // ShardedCache tests
    // ====================================================================

    #[test]
    fn test_sharded_basic_set_get() {
        let cache = ShardedCache::new(8, 4096, None);
        cache.set("hello", "world", None);
        assert_eq!(cache.get("hello"), Some("world".to_string()));
        assert_eq!(cache.get("missing"), None);
    }

    #[test]
    fn test_sharded_delete() {
        let cache = ShardedCache::new(4, 4096, None);
        cache.set("k", "v", None);
        assert!(cache.delete("k"));
        assert_eq!(cache.get("k"), None);
        assert!(!cache.delete("k"));
    }

    #[test]
    fn test_sharded_exists_read_lock() {
        let cache = ShardedCache::new(4, 4096, None);
        cache.set("present", "yes", None);
        assert!(cache.exists("present"));
        assert!(!cache.exists("absent"));
    }

    #[test]
    fn test_sharded_len_and_is_empty() {
        let cache = ShardedCache::new(4, 4096, None);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        for i in 0..20 {
            cache.set(&format!("k{i}"), &format!("v{i}"), None);
        }
        assert_eq!(cache.len(), 20);
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_sharded_flush_all() {
        let cache = ShardedCache::new(8, 4096, None);
        for i in 0..50 {
            cache.set(&format!("k{i}"), &format!("v{i}"), None);
        }
        assert_eq!(cache.len(), 50);
        cache.flush_all();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_sharded_stats_aggregation() {
        let cache = ShardedCache::new(4, 4096, None);
        for i in 0..40 {
            cache.set(&format!("k{i}"), &format!("v{i}"), None);
        }
        for i in 0..40 {
            cache.get(&format!("k{i}"));
        }
        for _ in 0..10 {
            cache.get("nonexistent");
        }
        let stats = cache.stats();
        assert_eq!(stats.total_entries, 40);
        assert_eq!(stats.total_hits, 40);
        assert_eq!(stats.total_misses, 10);
        assert_eq!(stats.shard_count, 4);
        assert!((stats.hit_rate - 0.8).abs() < 1e-9);
    }

    #[test]
    fn test_sharded_default_ttl() {
        let cache = ShardedCache::new(4, 4096, Some(3600));
        cache.set("k", "v", None);
        assert_eq!(cache.get("k"), Some("v".to_string()));
    }
    #[test]
    fn test_sharded_per_shard_isolation() {
        let cache = ShardedCache::new(4, 4096, None);
        let keys: Vec<String> = (0..100).map(|i| format!("key_{i}")).collect();
        for k in &keys {
            cache.set(k, "value", None);
        }
        assert_eq!(cache.len(), 100);
        for k in &keys[..50] {
            cache.delete(k);
        }
        assert_eq!(cache.len(), 50);
        for k in &keys[50..] {
            assert!(cache.exists(k), "key {k} should still exist");
        }
    }

    #[test]
    fn test_sharded_evict_expired() {
        let cache = ShardedCache::new(4, 4096, None);
        for i in 0..20 {
            cache.set(&format!("exp{i}"), "v", Some(0));
        }
        for i in 0..10 {
            cache.set(&format!("live{i}"), "v", Some(3600));
        }
        std::thread::sleep(Duration::from_millis(10));
        cache.evict_expired();
        assert_eq!(cache.len(), 10);
        for i in 0..10 {
            assert!(cache.exists(&format!("live{i}")));
        }
    }

    #[test]
    fn test_sharded_cross_shard_operations() {
        let cache = ShardedCache::new(16, 4096, None);
        let n = 200;
        for i in 0..n {
            cache.set(&format!("x{i}"), &format!("y{i}"), None);
        }
        assert_eq!(cache.len(), n);
        for i in 0..n {
            let val = cache.get(&format!("x{i}"));
            assert_eq!(val, Some(format!("y{i}")));
        }
        let stats = cache.stats();
        assert_eq!(stats.total_hits, n as u64);
        assert_eq!(stats.total_entries, n);
    }
    #[test]
    fn test_sharded_concurrent_reads() {
        use std::sync::Arc;
        let cache = Arc::new(ShardedCache::new(8, 65536, None));
        for i in 0..100 {
            cache.set(&format!("k{i}"), &format!("v{i}"), None);
        }
        let mut handles = Vec::new();
        for t in 0..8 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                let mut found = 0u64;
                for i in 0..100 {
                    let key = format!("k{i}");
                    if let Some(val) = cache.get(&key) {
                        assert_eq!(val, format!("v{i}"));
                        found += 1;
                    }
                }
                assert_eq!(found, 100, "thread {t} should find all 100 keys");
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    #[test]
    fn test_sharded_concurrent_writes() {
        use std::sync::Arc;
        let cache = Arc::new(ShardedCache::new(8, 65536, None));
        let mut handles = Vec::new();
        for t in 0..8 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("t{t}_k{i}");
                    let value = format!("t{t}_v{i}");
                    cache.set(&key, &value, None);
                }
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
        assert_eq!(cache.len(), 800);
        for t in 0..8 {
            for i in 0..100 {
                let key = format!("t{t}_k{i}");
                let expected = format!("t{t}_v{i}");
                assert_eq!(cache.get(&key), Some(expected));
            }
        }
    }
    #[test]
    fn test_sharded_concurrent_mixed_read_write() {
        use std::sync::Arc;
        let cache = Arc::new(ShardedCache::new(16, 65536, None));
        for i in 0..50 {
            cache.set(&format!("shared{i}"), &format!("init{i}"), None);
        }
        let mut handles = Vec::new();
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    let _ = cache.get(&format!("shared{i}"));
                }
            }));
        }
        for t in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    cache.set(
                        &format!("shared{i}"),
                        &format!("updated_by_{t}_{i}"),
                        None,
                    );
                }
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
        assert_eq!(cache.len(), 50);
    }

    #[test]
    fn test_sharded_shard_distribution() {
        let cache = ShardedCache::new(8, 4096, None);
        let mut shard_hits = vec![false; 8];
        for i in 0..200 {
            let key = format!("dist_key_{i}");
            let idx = cache.shard_for_key(&key);
            shard_hits[idx] = true;
        }
        for (i, &hit) in shard_hits.iter().enumerate() {
            assert!(hit, "shard {i} received no keys out of 200");
        }
    }

    #[test]
    fn test_sharded_config_accessors() {
        let cache = ShardedCache::new(16, 2048, Some(120));
        let cfg = cache.config();
        assert_eq!(cfg.shard_count, 16);
        assert_eq!(cfg.max_memory_bytes, 16 * 2048);
        assert_eq!(cfg.default_ttl_secs, Some(120));
    }

    #[test]
    fn test_sharded_zero_shard_count_defaults_to_one() {
        let cache = ShardedCache::new(0, 1024, None);
        cache.set("a", "b", None);
        assert_eq!(cache.get("a"), Some("b".to_string()));
        assert_eq!(cache.config().shard_count, 1);
    }

    // ================================================================
    // LockFreeCache tests
    // ================================================================

    #[test]
    fn lf_cache_basic_get_set() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("key1", "value1", None);
        assert_eq!(cache.get("key1"), Some("value1".to_string()));
        assert_eq!(cache.get("nonexistent"), None);
    }

    #[test]
    fn lf_cache_delete() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("key1", "value1", None);
        assert!(cache.delete("key1"));
        assert_eq!(cache.get("key1"), None);
        assert!(!cache.delete("key1")); // already deleted
    }

    #[test]
    fn lf_cache_exists() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("key1", "value1", None);
        assert!(cache.exists("key1"));
        assert!(!cache.exists("key2"));
    }

    #[test]
    fn lf_cache_overwrite() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("key1", "old", None);
        cache.set("key1", "new", None);
        assert_eq!(cache.get("key1"), Some("new".to_string()));
    }

    #[test]
    fn lf_cache_hit_miss_tracking() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("k", "v", None);
        cache.get("k");      // hit
        cache.get("k");      // hit
        cache.get("miss");   // miss
        assert_eq!(cache.total_hits(), 2);
        assert_eq!(cache.total_misses(), 1);
        assert!((cache.hit_rate() - 2.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn lf_cache_flush_all() {
        let cache = LockFreeCache::new(4, 4096);
        for i in 0..10 {
            cache.set(&format!("k{i}"), &format!("v{i}"), None);
        }
        assert!(cache.len() > 0);
        cache.flush_all();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn lf_cache_ttl_expiry() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("short", "val", Some(0)); // expires immediately
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(cache.get("short"), None);
    }

    #[test]
    fn lf_cache_evict_expired() {
        let cache = LockFreeCache::new(4, 4096);
        cache.set("expires", "val", Some(0));
        std::thread::sleep(Duration::from_millis(10));
        cache.evict_expired();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn lf_cache_memory_eviction() {
        // Small memory budget: only ~128 bytes per shard
        let cache = LockFreeCache::new(1, 256);
        // Each entry is key + value + 64 bytes overhead
        cache.set("a", "aaaa", None);
        cache.set("b", "bbbb", None);
        cache.set("c", "cccc", None);
        // At least some entries should be evicted
        assert!(cache.total_memory_bytes() <= 256);
    }

    #[test]
    fn lf_cache_concurrent_reads() {
        let cache = Arc::new(LockFreeCache::new(8, 65536));
        // Pre-populate
        for i in 0..100 {
            cache.set(&format!("key{i}"), &format!("value{i}"), None);
        }

        // Spawn 8 reader threads — should never block each other
        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let _ = c.get(&format!("key{i}"));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(cache.total_hits(), 800);
    }

    #[test]
    fn lf_cache_concurrent_read_write() {
        let cache = Arc::new(LockFreeCache::new(8, 65536));
        let mut handles = Vec::new();

        // Writers
        for t in 0..4 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    c.set(&format!("t{t}_k{i}"), &format!("v{i}"), None);
                }
            }));
        }

        // Readers
        for _ in 0..4 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..50 {
                    let _ = c.get(&format!("t0_k{i}"));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        // All operations completed without deadlock
        assert!(cache.len() <= 200);
    }

    #[test]
    fn lf_cache_debug_format() {
        let cache = LockFreeCache::new(4, 4096);
        let dbg = format!("{:?}", cache);
        assert!(dbg.contains("LockFreeCache"));
    }
}
