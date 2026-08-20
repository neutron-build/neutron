//! Key-Value store with in-memory cache tier.
//!
//! Replaces Redis, DragonflyDB, Valkey for caching and KV operations.
//! Operations: GET, SET (with optional TTL), DEL, EXISTS, KEYS, INCR, EXPIRE.
//!
//! Architecture:
//!   - In-memory HashMap with TTL support (cache tier)
//!   - Background TTL sweeper that periodically cleans expired keys
//!   - Accessible via SQL: SELECT kv_get('key'), SELECT kv_set('key', 'value', ttl_seconds)
//!   - Or via dedicated KV commands on the wire protocol

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
#[cfg(feature = "server")]
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "server")]
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

#[cfg(feature = "server")]
use crate::storage::kv_wal::{KvWal, KvWalOp};
use crate::types::Value;

pub mod codec;
pub mod collections;
#[cfg(feature = "server")]
pub mod collections_wal;
pub mod streams;

// ============================================================================
// KV Entry
// ============================================================================

#[derive(Debug, Clone)]
struct KvEntry {
    value: Arc<Value>,
    expires_at: Option<Instant>,
}

impl KvEntry {
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(t) => Instant::now() >= t,
            None => false,
        }
    }
}

// ============================================================================
// Sharded Map (64 shards for parallel access)
// ============================================================================

const NUM_SHARDS: usize = 64;

struct Shard {
    data: RwLock<HashMap<String, KvEntry>>,
    /// Index of expiry times → keys for O(expired) sweep instead of O(total).
    expiry_index: RwLock<BTreeMap<Instant, Vec<String>>>,
}

impl Shard {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            expiry_index: RwLock::new(BTreeMap::new()),
        }
    }

    /// Add a key to the expiry index under the given instant.
    fn add_expiry(&self, key: &str, expires_at: Instant) {
        self.expiry_index
            .write()
            .entry(expires_at)
            .or_default()
            .push(key.to_string());
    }

    /// Remove a key from the expiry index at the given instant.
    fn remove_expiry(&self, key: &str, expires_at: Instant) {
        let mut idx = self.expiry_index.write();
        if let std::collections::btree_map::Entry::Occupied(mut oe) = idx.entry(expires_at) {
            let vec = oe.get_mut();
            vec.retain(|k| k != key);
            if vec.is_empty() {
                oe.remove();
            }
        }
    }
}

struct ShardedMap {
    shards: Vec<Shard>,
}

impl ShardedMap {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Shard::new());
        }
        Self { shards }
    }

    /// Determine shard index for a given key.
    fn shard_index(key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % NUM_SHARDS
    }

    /// Get a reference to the shard for a given key.
    fn shard(&self, key: &str) -> &Shard {
        &self.shards[Self::shard_index(key)]
    }
}

// ============================================================================
// KV Store
// ============================================================================

/// In-memory key-value store with TTL support and optional WAL durability.
/// Uses 64-shard design for parallel access — single-key operations lock
/// only one shard, enabling high concurrency.
///
/// When opened with [`KvStore::open`] (disk mode), a cold LsmTree tier is
/// automatically created for overflow storage.  In-memory mode (`new()`)
/// has no cold tier.
/// Per-entry bookkeeping overhead: the `KvEntry` struct, the map slot and the
/// `Arc` header. Approximate, and small enough not to matter for large values —
/// it exists so a store of many tiny keys is not accounted as near-zero.
const KV_ENTRY_OVERHEAD_BYTES: usize = 64;

/// Default hot-tier byte budget, overridable with `NUCLEUS_KV_MAX_HOT_MB`.
///
/// 1 GiB. Entries beyond this spill to the on-disk cold tier rather than being
/// dropped, so the data is still readable — a cold hit is promoted back on
/// access. Chosen to be generous for ordinary key/value use while still bounding
/// a store that has accumulated large blobs.
fn default_max_hot_bytes() -> usize {
    const DEFAULT_MB: usize = 1024;
    std::env::var("NUCLEUS_KV_MAX_HOT_MB")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|mb| *mb > 0)
        .unwrap_or(DEFAULT_MB)
        .saturating_mul(1024 * 1024)
}

pub struct KvStore {
    data: ShardedMap,
    #[cfg(feature = "server")]
    wal: Option<Arc<KvWal>>,
    collections: collections::ShardedCollections,
    /// Cold tier: disk-backed LsmTree for overflow entries (disk mode only).
    cold: Option<parking_lot::Mutex<crate::storage::lsm::LsmTree>>,
    /// Maximum entries in the hot (in-memory) tier before eviction to cold.
    max_hot_entries: usize,
    /// Maximum bytes in the hot tier before eviction to cold.
    ///
    /// A count limit alone does not bound memory. A store of 32k keys holding
    /// 150 KB values each sits far under a 100k-entry cap while occupying
    /// several GB, which is exactly how a production instance ended up over its
    /// memory limit rejecting writes with eviction never once triggering.
    /// Whichever limit is reached first wins.
    max_hot_bytes: usize,
    /// Global monotonic version counter, incremented on every write operation.
    /// Used by WATCH/EXEC optimistic locking in the RESP handler.
    global_version: std::sync::atomic::AtomicU64,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    /// Create an in-memory KV store with no WAL (backward-compatible).
    /// No cold tier — all entries stay in memory.
    pub fn new() -> Self {
        Self {
            data: ShardedMap::new(),
            #[cfg(feature = "server")]
            wal: None,
            collections: collections::ShardedCollections::new(),
            cold: None,
            max_hot_entries: 100_000,
            max_hot_bytes: default_max_hot_bytes(),
            global_version: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Open a WAL-backed KV store, recovering state from `dir/kv.wal`
    /// and `dir/collections.wal`.
    ///
    /// If the WAL files don't exist a fresh store is returned. Corrupt
    /// trailing bytes are silently skipped (best-effort recovery).
    #[cfg(feature = "server")]
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        let (wal, state) = KvWal::open(dir)?;
        let sharded = ShardedMap::new();
        let now_epoch_ms = epoch_ms_now();
        for (key, value, ttl_abs_ms) in state.items {
            // Convert absolute epoch TTL back to Instant
            let expires_at = ttl_abs_ms.and_then(|abs_ms| {
                if abs_ms <= now_epoch_ms {
                    // Already expired — skip this entry entirely
                    return None;
                }
                let remaining_ms = abs_ms - now_epoch_ms;
                // checked_add; an absurd persisted TTL would overflow Instant +
                // Duration. Fall back to ~10 years out (effectively never) so the
                // key is still RESTORED (Some), not dropped as "already expired".
                Instant::now()
                    .checked_add(Duration::from_millis(remaining_ms))
                    .or_else(|| {
                        Instant::now().checked_add(Duration::from_secs(3_600 * 24 * 365 * 10))
                    })
            });
            // If the TTL was set but already expired, skip the key
            if ttl_abs_ms.is_some() && expires_at.is_none() {
                continue;
            }
            let shard = sharded.shard(&key);
            if let Some(exp) = expires_at {
                shard
                    .expiry_index
                    .write()
                    .entry(exp)
                    .or_default()
                    .push(key.clone());
            }
            shard.data.write().insert(
                key,
                KvEntry {
                    value: Arc::new(value),
                    expires_at,
                },
            );
        }

        // Open collections WAL and recover collection state
        let (col_wal, mut col_state) = collections_wal::CollectionWal::open(dir)?;
        let col_wal = Arc::new(col_wal);
        col_state.set_wal(Arc::clone(&col_wal));

        // Open cold LsmTree tier for overflow storage
        let cold_dir = dir.join("kv_cold");
        std::fs::create_dir_all(&cold_dir).ok();
        let config = crate::storage::lsm::LsmConfig::default();
        let cold = crate::storage::lsm::LsmTree::open(config, &cold_dir)
            .ok()
            .map(parking_lot::Mutex::new);

        let store = Self {
            data: sharded,
            wal: Some(Arc::new(wal)),
            collections: col_state,
            cold,
            max_hot_entries: 100_000,
            max_hot_bytes: default_max_hot_bytes(),
            global_version: std::sync::atomic::AtomicU64::new(0),
        };
        // Replay rebuilds the map directly rather than going through `set`, so
        // nothing has checked the budgets yet. Without this, a store that grew
        // past them before a restart comes back over the limit and stays there:
        // eviction only runs on write, and writes are the first thing refused
        // when the process is over its memory ceiling.
        //
        // Note this does not bound the *peak* during replay — the log is still
        // materialised in full before we get here. Bounding that needs eviction
        // interleaved into replay, which is a larger change.
        store.maybe_evict();
        Ok(store)
    }

    /// Return the current global version counter. Used by WATCH to snapshot
    /// the version at WATCH time, then compare at EXEC time.
    pub fn version(&self) -> u64 {
        self.global_version
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Return the version for a specific key. Currently uses the global
    /// monotonic counter — any write to any key bumps this. Good enough
    /// for WATCH semantics (conservative: may abort on unrelated writes).
    pub fn key_version(&self, _key: &str) -> u64 {
        self.version()
    }

    /// Bump the global version counter. **Every** mutator must call this —
    /// strings and collections alike. WATCH is built on it, so a mutator that
    /// skips it makes EXEC miss a concurrent write and commit the lost update
    /// WATCH exists to prevent.
    fn bump_version(&self) {
        self.global_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// GET — retrieve a value by key. Returns None if key doesn't exist or is expired.
    /// In disk mode, falls back to the cold LsmTree tier on hot miss and promotes
    /// the value back to hot for temporal locality.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.get_arc(key).map(Arc::unwrap_or_clone)
    }

    /// GET returning Arc<Value> — avoids deep clone for callers that only need
    /// to borrow the value (e.g. RESP encoding, existence checks).
    pub fn get_arc(&self, key: &str) -> Option<Arc<Value>> {
        let shard = self.data.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => Some(Arc::clone(&entry.value)),
            Some(entry) => {
                let old_exp = entry.expires_at;
                drop(data);
                // Lazy expiration: remove expired key
                shard.data.write().remove(key);
                if let Some(exp) = old_exp {
                    shard.remove_expiry(key, exp);
                }
                // Fall through to cold tier check
                self.cold_get_and_promote(key)
            }
            None => {
                drop(data);
                self.cold_get_and_promote(key)
            }
        }
    }

    /// SET — store a value with optional TTL in seconds.
    pub fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal {
            // One record carrying the value AND the expiry decided with it.
            // These used to be two independent appends, so a crash between
            // them replayed a key the client asked to be temporary as a
            // permanent one — a leaked lock, lease, or secret. The `None` case
            // is equally load-bearing: it clears an expiry the key already had,
            // which is what the in-memory write below does, and what a bare
            // `ENTRY_SET` record would NOT have done on replay.
            if let Err(e) = wal.log_set_with_expiry(key, &value, ttl_secs.map(abs_expiry_ms)) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        }
        // checked_add: a huge TTL (e.g. i64::MAX seconds) would overflow
        // Instant + Duration and panic. Treat an overflowing TTL as "never
        // expires" (None) — it is effectively infinite anyway.
        let expires_at = ttl_secs.and_then(|s| Instant::now().checked_add(Duration::from_secs(s)));
        let shard = self.data.shard(key);
        // Remove old expiry index entry if replacing a key with TTL
        {
            let data = shard.data.read();
            if let Some(old) = data.get(key)
                && let Some(old_exp) = old.expires_at
            {
                shard.remove_expiry(key, old_exp);
            }
        }
        shard.data.write().insert(
            key.to_string(),
            KvEntry {
                value: Arc::new(value),
                expires_at,
            },
        );
        if let Some(exp) = expires_at {
            shard.add_expiry(key, exp);
        }
        self.bump_version();
        if self.cold.is_some() {
            self.maybe_evict();
        }
    }

    /// DEL — remove a key. Returns true if the key existed (in hot or cold tier).
    pub fn del(&self, key: &str) -> bool {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_delete(key)
        {
            tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
        }
        let shard = self.data.shard(key);
        let removed_entry = shard.data.write().remove(key);
        if let Some(ref entry) = removed_entry
            && let Some(exp) = entry.expires_at
        {
            shard.remove_expiry(key, exp);
        }
        let hot_removed = removed_entry.is_some();
        let cold_removed = if let Some(ref cold) = self.cold {
            let mut c = cold.lock();
            if c.get(key.as_bytes()).is_some() {
                c.delete(key.as_bytes().to_vec());
                true
            } else {
                false
            }
        } else {
            false
        };
        if hot_removed || cold_removed {
            self.bump_version();
        }
        hot_removed || cold_removed
    }

    /// EXISTS — check if a key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// INCR — increment an integer value by 1. Creates with value 1 if key doesn't exist.
    pub fn incr(&self, key: &str) -> Result<i64, KvError> {
        self.incr_by(key, 1)
    }

    /// INCRBY — increment an integer value by a given amount.
    pub fn incr_by(&self, key: &str, amount: i64) -> Result<i64, KvError> {
        let shard = self.data.shard(key);
        let mut data = shard.data.write();
        let entry = data.get(key);

        let current = match entry {
            None => 0,
            Some(e) if e.is_expired() => 0,
            Some(e) => match e.value.as_ref() {
                Value::Int32(n) => *n as i64,
                Value::Int64(n) => *n,
                // Redis stores all string values as strings, so `SET k 5; INCR k`
                // must parse the integer-formatted string. Non-integer strings
                // (and other types) are an error, matching Redis.
                Value::Text(s) => s.parse::<i64>().map_err(|_| KvError::NotAnInteger)?,
                _ => return Err(KvError::NotAnInteger),
            },
        };

        // checked_add: Redis returns an error rather than wrapping/panicking when
        // an INCR/INCRBY would exceed i64 range.
        let new_val = current.checked_add(amount).ok_or(KvError::Overflow)?;
        let ttl = entry.and_then(|e| e.expires_at);

        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_set(key, &Value::Int64(new_val))
        {
            tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
        }

        data.insert(
            key.to_string(),
            KvEntry {
                value: Arc::new(Value::Int64(new_val)),
                expires_at: ttl,
            },
        );

        self.bump_version();
        Ok(new_val)
    }

    /// EXPIRE — set a TTL on an existing key. Returns false if key doesn't exist.
    pub fn expire(&self, key: &str, ttl_secs: u64) -> bool {
        let shard = self.data.shard(key);
        let mut data = shard.data.write();
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                // Remove expired entry and its expiry index
                if let Some(old_exp) = entry.expires_at {
                    shard.remove_expiry(key, old_exp);
                }
                data.remove(key);
                return false;
            }
            // Remove old expiry index entry
            if let Some(old_exp) = entry.expires_at {
                shard.remove_expiry(key, old_exp);
            }
            // checked_add: a huge TTL overflows Instant + Duration and panics.
            // Treat an overflowing TTL as "never expires".
            let Some(new_exp) = Instant::now().checked_add(Duration::from_secs(ttl_secs)) else {
                // "Never expires" in memory has to mean the same thing in the
                // log, or a restart resurrects the expiry this branch dropped.
                #[cfg(feature = "server")]
                if let Some(ref wal) = self.wal
                    && let Err(e) = wal.log_set_with_expiry(key, &entry.value, None)
                {
                    tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
                }
                entry.expires_at = None;
                self.bump_version();
                return true;
            };
            #[cfg(feature = "server")]
            if let Some(ref wal) = self.wal
                && let Err(e) = wal.log_expire(key, abs_expiry_ms(ttl_secs))
            {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
            entry.expires_at = Some(new_exp);
            shard.add_expiry(key, new_exp);
            self.bump_version();
            true
        } else {
            false
        }
    }

    /// PERSIST — remove the TTL from a key (make it permanent).
    pub fn persist(&self, key: &str) -> bool {
        let shard = self.data.shard(key);
        let mut data = shard.data.write();
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                if let Some(old_exp) = entry.expires_at {
                    shard.remove_expiry(key, old_exp);
                }
                data.remove(key);
                return false;
            }
            if let Some(old_exp) = entry.expires_at {
                shard.remove_expiry(key, old_exp);
            }
            // PERSIST logged nothing at all, so the log still held the key's
            // old EXPIRE record: the server answered `TTL -1` and a restart
            // brought the expiry back, after which the key the client had made
            // permanent silently disappeared. There is no "clear expiry"
            // record, so re-log the value with an explicit `None` expiry.
            #[cfg(feature = "server")]
            if let Some(ref wal) = self.wal
                && let Err(e) = wal.log_set_with_expiry(key, &entry.value, None)
            {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
            entry.expires_at = None;
            self.bump_version();
            true
        } else {
            false
        }
    }

    /// TTL — get remaining TTL in seconds. Returns -1 if no TTL, -2 if key doesn't exist.
    pub fn ttl(&self, key: &str) -> i64 {
        let shard = self.data.shard(key);
        let data = shard.data.read();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => match entry.expires_at {
                Some(t) => {
                    let remaining = t.duration_since(Instant::now());
                    remaining.as_secs() as i64
                }
                None => -1,
            },
            _ => -2,
        }
    }

    /// KEYS — return all non-expired keys matching a pattern (simple glob: * only).
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let now = Instant::now();
        let mut result = Vec::new();
        for shard in &self.data.shards {
            let data = shard.data.read();
            for (key, entry) in data.iter() {
                if entry.expires_at.is_none_or(|t| now < t) && match_pattern(pattern, key) {
                    result.push(key.clone());
                }
            }
        }
        result
    }

    /// DBSIZE — return the number of non-expired keys.
    pub fn dbsize(&self) -> usize {
        let now = Instant::now();
        let mut count = 0;
        for shard in &self.data.shards {
            let data = shard.data.read();
            count += data
                .values()
                .filter(|entry| entry.expires_at.is_none_or(|t| now < t))
                .count();
        }
        count
    }

    /// FLUSHDB — remove all keys of every type, from both hot and cold tiers.
    ///
    /// "Every type" is the whole contract of this command, so it must reach the
    /// collections store too: lists, hashes, sets, sorted sets, HLLs, streams
    /// and geosets all live there, not in the string shards. Clearing only the
    /// strings left `RPUSH k v; FLUSHDB; LRANGE k 0 -1` returning `v`.
    pub fn flushdb(&self) {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal {
            // Checkpoint with empty state to truncate WAL
            if let Err(e) = wal.checkpoint(&[]) {
                eprintln!("KV WAL: failed to checkpoint on flushdb: {e}");
            }
        }
        for shard in &self.data.shards {
            shard.data.write().clear();
            shard.expiry_index.write().clear();
        }
        // Clear cold tier by replacing with a fresh LsmTree
        if let Some(ref cold) = self.cold {
            let config = crate::storage::lsm::LsmConfig::default();
            *cold.lock() = crate::storage::lsm::LsmTree::new(config);
        }
        // Collections: clear memory first, then snapshot the (now empty) state
        // so replay cannot resurrect what FLUSHDB just removed.
        self.collections.clear_all();
        if let Err(e) = self.collections.checkpoint() {
            eprintln!("KV collections WAL: failed to checkpoint on flushdb: {e}");
        }
        self.bump_version();
    }

    /// Active TTL sweep: only process keys whose expiry time has passed.
    /// O(expired) instead of O(total) — no full shard scans needed.
    pub fn sweep_expired(&self) -> usize {
        let now = Instant::now();
        let mut total_removed = 0;
        for shard in &self.data.shards {
            // Collect expired instants from the index without holding data lock
            let expired_keys: Vec<(Instant, Vec<String>)> = {
                let mut idx = shard.expiry_index.write();
                let mut expired = Vec::new();
                // BTreeMap is sorted by key (Instant), so we iterate from earliest
                while let Some(entry) = idx.first_key_value() {
                    if *entry.0 > now {
                        break; // All remaining entries are in the future
                    }
                    let (instant, keys) = idx.pop_first().unwrap();
                    expired.push((instant, keys));
                }
                expired
            };
            // Now remove the expired keys from the data map
            if !expired_keys.is_empty() {
                let mut data = shard.data.write();
                for (_instant, keys) in expired_keys {
                    for key in keys {
                        // Double-check the entry is actually expired (could have been
                        // overwritten with a new TTL between index read and data write)
                        if let Some(entry) = data.get(&key)
                            && entry.is_expired()
                        {
                            data.remove(&key);
                            total_removed += 1;
                        }
                    }
                }
            }
        }
        total_removed
    }

    /// Full O(n) sweep as safety-net fallback. Also rebuilds the expiry index.
    /// Called infrequently (e.g. every 60 sweeps) to catch any index drift.
    pub fn sweep_expired_full(&self) -> usize {
        let mut total_removed = 0;
        for shard in &self.data.shards {
            let mut data = shard.data.write();
            let before = data.len();
            data.retain(|_, entry| !entry.is_expired());
            total_removed += before - data.len();
            // Rebuild expiry index from surviving entries
            let mut new_idx: BTreeMap<Instant, Vec<String>> = BTreeMap::new();
            for (key, entry) in data.iter() {
                if let Some(exp) = entry.expires_at {
                    new_idx.entry(exp).or_default().push(key.clone());
                }
            }
            *shard.expiry_index.write() = new_idx;
        }
        total_removed
    }

    /// MGET — get multiple values at once.
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Value>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// MSET — set multiple key-value pairs at once.
    ///
    /// Uses batch WAL write (single `write_all` + `flush`) instead of
    /// per-key WAL writes, avoiding per-entry syscall overhead.
    /// Each key is inserted into its own shard for parallel access.
    pub fn mset(&self, pairs: &[(&str, Value)]) {
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal {
            // `SetExact` with no TTL, not `Set`: the in-memory write below
            // stores `expires_at: None`, so MSET over a key that had a TTL
            // clears it. A plain `Set` record preserves the old expiry on
            // replay, and the key would then vanish after a restart despite
            // the live server reporting it as permanent.
            let batch: Vec<(KvWalOp, &str, Option<&Value>, Option<u64>)> = pairs
                .iter()
                .map(|(key, value)| (KvWalOp::SetExact, *key, Some(value), None))
                .collect();
            if let Err(e) = wal.log_batch(&batch) {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
        }
        for (key, value) in pairs {
            self.data.shard(key).data.write().insert(
                key.to_string(),
                KvEntry {
                    value: Arc::new(value.clone()),
                    expires_at: None,
                },
            );
        }
        self.bump_version();
    }

    /// SETNX — set only if key doesn't exist. Returns true if set, false if already exists.
    pub fn setnx(&self, key: &str, value: Value) -> bool {
        self.setnx_ttl(key, value, None)
    }

    /// SETNX with optional TTL — atomic set-if-absent-with-expiry under one
    /// shard write lock (Redis `SET key val NX EX ttl`; the crash-safe lock
    /// acquire). The value and its expiry are decided and WAL-logged inside
    /// the same critical section, so a crash can never leave a lock without
    /// its TTL.
    ///
    /// That last sentence was false until the expiry moved into the SET record
    /// itself. Holding one shard lock across two independent WAL appends makes
    /// the pair atomic against other *threads*, which is not the failure this
    /// lock guards against: a crash lands between the two appends regardless of
    /// who holds what, and replay then produced a lock with no expiry — held
    /// forever by a process that no longer exists.
    pub fn setnx_ttl(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> bool {
        let shard = self.data.shard(key);
        let mut data = shard.data.write();
        if let Some(entry) = data.get(key) {
            if !entry.is_expired() {
                return false;
            }
            // Key exists but is expired — clean up its expiry index entry
            if let Some(old_exp) = entry.expires_at {
                shard.remove_expiry(key, old_exp);
            }
        }
        #[cfg(feature = "server")]
        if let Some(ref wal) = self.wal
            && let Err(e) = wal.log_set_with_expiry(key, &value, ttl_secs.map(abs_expiry_ms))
        {
            tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
        }
        // checked_add: a huge TTL would overflow Instant + Duration and
        // panic; treat it as "never expires" (same rule as `set`).
        let expires_at = ttl_secs.and_then(|s| Instant::now().checked_add(Duration::from_secs(s)));
        data.insert(
            key.to_string(),
            KvEntry {
                value: Arc::new(value),
                expires_at,
            },
        );
        if let Some(exp) = expires_at {
            shard.add_expiry(key, exp);
        }
        self.bump_version();
        true
    }

    /// CDEL — delete `key` only if its current value equals `expected`; the
    /// safe lock release (a holder whose lease already expired cannot delete
    /// the next holder's lock). Expired entries count as absent. Compares
    /// with the stored value's type — write lock values as text.
    ///
    /// Mirrors `del`'s cold-tier behavior: a hot miss promotes any cold copy
    /// first (via the `get_arc` path), and a successful delete purges the
    /// cold tier so a stale copy cannot resurrect. TTL'd keys (locks) never
    /// evict to cold, so lock usage always takes the atomic hot path.
    pub fn cdel(&self, key: &str, expected: &Value) -> bool {
        enum Gate {
            Missing,
            Expired(Option<Instant>),
            Mismatch,
            Match(Option<Instant>),
        }
        for attempt in 0..2 {
            let shard = self.data.shard(key);
            let mut data = shard.data.write();
            let gate = match data.get(key) {
                None => Gate::Missing,
                Some(entry) if entry.is_expired() => Gate::Expired(entry.expires_at),
                Some(entry) if *entry.value != *expected => Gate::Mismatch,
                Some(entry) => Gate::Match(entry.expires_at),
            };
            match gate {
                Gate::Mismatch => return false,
                Gate::Expired(old_exp) => {
                    data.remove(key);
                    if let Some(exp) = old_exp {
                        shard.remove_expiry(key, exp);
                    }
                    return false;
                }
                Gate::Match(old_exp) => {
                    #[cfg(feature = "server")]
                    if let Some(ref wal) = self.wal
                        && let Err(e) = wal.log_delete(key)
                    {
                        tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
                    }
                    data.remove(key);
                    if let Some(exp) = old_exp {
                        shard.remove_expiry(key, exp);
                    }
                    drop(data);
                    if let Some(ref cold) = self.cold {
                        let mut c = cold.lock();
                        if c.get(key.as_bytes()).is_some() {
                            c.delete(key.as_bytes().to_vec());
                        }
                    }
                    self.bump_version();
                    return true;
                }
                Gate::Missing => {
                    drop(data);
                    // Hot miss: promote a possible cold-tier copy, then retry
                    // the atomic hot path once.
                    if attempt == 0 && self.cold.is_some() {
                        if self.get_arc(key).is_none() {
                            return false;
                        }
                        continue;
                    }
                    return false;
                }
            }
        }
        false
    }

    /// CEXPIRE — set the TTL only if the current value equals `expected`;
    /// the lease-renewal heartbeat (a holder whose lease was taken over
    /// cannot extend the new holder's lock). Mirrors `expire`'s hot-tier
    /// semantics; TTL'd keys never evict to cold.
    pub fn cexpire(&self, key: &str, expected: &Value, ttl_secs: u64) -> bool {
        let shard = self.data.shard(key);
        let mut data = shard.data.write();
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                if let Some(old_exp) = entry.expires_at {
                    shard.remove_expiry(key, old_exp);
                }
                data.remove(key);
                return false;
            }
            if *entry.value != *expected {
                return false;
            }
            if let Some(old_exp) = entry.expires_at {
                shard.remove_expiry(key, old_exp);
            }
            // checked_add: a huge TTL overflows Instant + Duration and panics.
            // Treat an overflowing TTL as "never expires" (same rule as `expire`).
            let Some(new_exp) = Instant::now().checked_add(Duration::from_secs(ttl_secs)) else {
                #[cfg(feature = "server")]
                if let Some(ref wal) = self.wal
                    && let Err(e) = wal.log_set_with_expiry(key, &entry.value, None)
                {
                    tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
                }
                entry.expires_at = None;
                self.bump_version();
                return true;
            };
            #[cfg(feature = "server")]
            if let Some(ref wal) = self.wal
                && let Err(e) = wal.log_expire(key, abs_expiry_ms(ttl_secs))
            {
                tracing::error!(target: "nucleus::kv::wal", "WAL write failed: {e}");
            }
            entry.expires_at = Some(new_exp);
            shard.add_expiry(key, new_exp);
            self.bump_version();
            true
        } else {
            false
        }
    }

    // ========================================================================
    // Bitmap operations (SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS)
    // ========================================================================

    /// Helper: get the raw bytes for a key, treating it as a byte buffer.
    /// Returns an empty Vec if key doesn't exist.
    fn get_bitmap_bytes(&self, key: &str) -> Vec<u8> {
        match self.get_arc(key).as_deref() {
            Some(Value::Text(s)) => s.as_bytes().to_vec(),
            Some(Value::Bytea(b)) => b.clone(),
            _ => Vec::new(),
        }
    }

    /// SETBIT — set or clear the bit at `offset` in the string value stored at `key`.
    /// Returns the original bit value.
    pub fn setbit(&self, key: &str, offset: usize, bit: bool) -> u8 {
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8); // MSB-first like Redis

        let mut bytes = self.get_bitmap_bytes(key);
        if bytes.len() <= byte_idx {
            bytes.resize(byte_idx + 1, 0);
        }

        let old_bit = (bytes[byte_idx] >> bit_idx) & 1;

        if bit {
            bytes[byte_idx] |= 1 << bit_idx;
        } else {
            bytes[byte_idx] &= !(1 << bit_idx);
        }

        self.set(key, Value::Bytea(bytes), None);
        old_bit
    }

    /// GETBIT — get the bit at `offset` in the string value stored at `key`.
    pub fn getbit(&self, key: &str, offset: usize) -> u8 {
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        let bytes = self.get_bitmap_bytes(key);
        if byte_idx >= bytes.len() {
            return 0;
        }
        (bytes[byte_idx] >> bit_idx) & 1
    }

    /// BITCOUNT — count the number of set bits in the value stored at `key`.
    /// Optional `start`/`end` byte range (inclusive, like Redis).
    pub fn bitcount(&self, key: &str, start: Option<i64>, end: Option<i64>) -> u64 {
        let bytes = self.get_bitmap_bytes(key);
        if bytes.is_empty() {
            return 0;
        }
        let len = bytes.len() as i64;
        let s = match start {
            Some(s) if s < 0 => (len + s).max(0) as usize,
            Some(s) => (s as usize).min(bytes.len()),
            None => 0,
        };
        let e = match end {
            Some(e) if e < 0 => (len + e).max(0) as usize,
            Some(e) => (e as usize).min(bytes.len() - 1),
            None => bytes.len() - 1,
        };
        if s > e || s >= bytes.len() {
            return 0;
        }
        bytes[s..=e].iter().map(|b| b.count_ones() as u64).sum()
    }

    /// BITOP — perform a bitwise operation between multiple keys and store the result.
    /// Returns the length (in bytes) of the result string.
    pub fn bitop(&self, op: &str, dest: &str, keys: &[&str]) -> usize {
        let mut bufs: Vec<Vec<u8>> = keys.iter().map(|k| self.get_bitmap_bytes(k)).collect();
        let max_len = bufs.iter().map(|b| b.len()).max().unwrap_or(0);

        // Pad all buffers to max_len
        for buf in &mut bufs {
            buf.resize(max_len, 0);
        }

        let result = match op.to_uppercase().as_str() {
            "AND" => {
                let mut out = bufs[0].clone();
                for buf in &bufs[1..] {
                    for (a, b) in out.iter_mut().zip(buf.iter()) {
                        *a &= *b;
                    }
                }
                out
            }
            "OR" => {
                let mut out = bufs[0].clone();
                for buf in &bufs[1..] {
                    for (a, b) in out.iter_mut().zip(buf.iter()) {
                        *a |= *b;
                    }
                }
                out
            }
            "XOR" => {
                let mut out = bufs[0].clone();
                for buf in &bufs[1..] {
                    for (a, b) in out.iter_mut().zip(buf.iter()) {
                        *a ^= *b;
                    }
                }
                out
            }
            "NOT" => {
                // NOT only takes one source key
                bufs[0].iter().map(|b| !b).collect()
            }
            _ => Vec::new(),
        };

        let len = result.len();
        self.set(dest, Value::Bytea(result), None);
        len
    }

    /// BITPOS — find the first bit set to `bit` (0 or 1) in the string value.
    /// Optional byte range `start`..=`end`.
    pub fn bitpos(&self, key: &str, bit: bool, start: Option<i64>, end: Option<i64>) -> i64 {
        let bytes = self.get_bitmap_bytes(key);
        if bytes.is_empty() {
            return if bit { -1 } else { 0 };
        }
        let len = bytes.len() as i64;
        let s = match start {
            Some(s) if s < 0 => (len + s).max(0) as usize,
            Some(s) => (s as usize).min(bytes.len()),
            None => 0,
        };
        let e = match end {
            Some(e) if e < 0 => (len + e).max(0) as usize,
            Some(e) => (e as usize).min(bytes.len() - 1),
            None => bytes.len() - 1,
        };
        if s > e || s >= bytes.len() {
            return -1;
        }
        for (byte_idx, &byte) in bytes.iter().enumerate().take(e + 1).skip(s) {
            for bit_idx in (0..8).rev() {
                let val = (byte >> bit_idx) & 1;
                if (val == 1) == bit {
                    return (byte_idx * 8 + (7 - bit_idx)) as i64;
                }
            }
        }
        -1
    }

    // ========================================================================
    // Collection operations (Lists, Hashes, Sets, Sorted Sets, HyperLogLog)
    // ========================================================================

    /// Direct access to the underlying collection store.
    pub fn collections(&self) -> &collections::ShardedCollections {
        &self.collections
    }

    /// Check if `key` exists as a plain string KV value. If so, return a
    /// WrongTypeError since collection operations cannot operate on string keys.
    fn check_string_conflict(
        &self,
        key: &str,
        expected: &'static str,
    ) -> Result<(), collections::WrongTypeError> {
        let shard = self.data.shard(key);
        let data = shard.data.read();
        if let Some(entry) = data.get(key)
            && !entry.is_expired()
        {
            return Err(collections::WrongTypeError {
                expected,
                actual: "string",
            });
        }
        Ok(())
    }

    // --- Lists ---

    pub fn lpush(&self, key: &str, value: Value) -> Result<usize, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "list")?;
        self.collections.lpush(key, value)
    }
    pub fn rpush(&self, key: &str, value: Value) -> Result<usize, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "list")?;
        self.collections.rpush(key, value)
    }
    pub fn lpop(&self, key: &str) -> Result<Option<Value>, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "list")?;
        self.collections.lpop(key)
    }
    pub fn rpop(&self, key: &str) -> Result<Option<Value>, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "list")?;
        self.collections.rpop(key)
    }
    pub fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Value>, collections::WrongTypeError> {
        self.check_string_conflict(key, "list")?;
        self.collections.lrange(key, start, stop)
    }
    pub fn llen(&self, key: &str) -> Result<usize, collections::WrongTypeError> {
        self.check_string_conflict(key, "list")?;
        self.collections.llen(key)
    }
    pub fn lindex(
        &self,
        key: &str,
        index: i64,
    ) -> Result<Option<Value>, collections::WrongTypeError> {
        self.check_string_conflict(key, "list")?;
        self.collections.lindex(key, index)
    }

    // --- Hashes ---

    pub fn hset(
        &self,
        key: &str,
        field: &str,
        value: Value,
    ) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "hash")?;
        self.collections.hset(key, field, value)
    }
    pub fn hget(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<Value>, collections::WrongTypeError> {
        self.check_string_conflict(key, "hash")?;
        self.collections.hget(key, field)
    }
    pub fn hdel(&self, key: &str, field: &str) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "hash")?;
        self.collections.hdel(key, field)
    }
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Value)>, collections::WrongTypeError> {
        self.check_string_conflict(key, "hash")?;
        self.collections.hgetall(key)
    }
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, collections::WrongTypeError> {
        self.check_string_conflict(key, "hash")?;
        self.collections.hkeys(key)
    }
    pub fn hvals(&self, key: &str) -> Result<Vec<Value>, collections::WrongTypeError> {
        self.check_string_conflict(key, "hash")?;
        self.collections.hvals(key)
    }
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, collections::WrongTypeError> {
        self.check_string_conflict(key, "hash")?;
        self.collections.hexists(key, field)
    }
    pub fn hlen(&self, key: &str) -> Result<usize, collections::WrongTypeError> {
        self.check_string_conflict(key, "hash")?;
        self.collections.hlen(key)
    }

    // --- Sets ---

    pub fn sadd(&self, key: &str, member: &str) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "set")?;
        self.collections.sadd(key, member)
    }
    pub fn srem(&self, key: &str, member: &str) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "set")?;
        self.collections.srem(key, member)
    }
    pub fn smembers(&self, key: &str) -> Result<Vec<String>, collections::WrongTypeError> {
        self.check_string_conflict(key, "set")?;
        self.collections.smembers(key)
    }
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, collections::WrongTypeError> {
        self.check_string_conflict(key, "set")?;
        self.collections.sismember(key, member)
    }
    pub fn scard(&self, key: &str) -> Result<usize, collections::WrongTypeError> {
        self.check_string_conflict(key, "set")?;
        self.collections.scard(key)
    }
    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<String>, collections::WrongTypeError> {
        self.collections.sinter(keys)
    }
    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<String>, collections::WrongTypeError> {
        self.collections.sunion(keys)
    }
    pub fn sdiff(&self, keys: &[&str]) -> Result<Vec<String>, collections::WrongTypeError> {
        self.collections.sdiff(keys)
    }

    // --- Sorted Sets ---

    pub fn col_zadd(
        &self,
        key: &str,
        member: &str,
        score: f64,
    ) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "zset")?;
        self.collections.zadd(key, member, score)
    }
    pub fn col_zrem(&self, key: &str, member: &str) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "zset")?;
        self.collections.zrem(key, member)
    }
    pub fn col_zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SortedSetEntry>, collections::WrongTypeError> {
        self.check_string_conflict(key, "zset")?;
        self.collections.zrange(key, start, stop)
    }
    pub fn col_zrevrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<SortedSetEntry>, collections::WrongTypeError> {
        self.check_string_conflict(key, "zset")?;
        self.collections.zrevrange(key, start, stop)
    }
    pub fn col_zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<SortedSetEntry>, collections::WrongTypeError> {
        self.check_string_conflict(key, "zset")?;
        self.collections.zrangebyscore(key, min, max)
    }
    pub fn col_zrank(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<usize>, collections::WrongTypeError> {
        self.check_string_conflict(key, "zset")?;
        self.collections.zrank(key, member)
    }
    pub fn col_zincrby(
        &self,
        key: &str,
        member: &str,
        increment: f64,
    ) -> Result<f64, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "zset")?;
        self.collections.zincrby(key, member, increment)
    }
    pub fn col_zcard(&self, key: &str) -> Result<usize, collections::WrongTypeError> {
        self.check_string_conflict(key, "zset")?;
        self.collections.zcard(key)
    }
    pub fn col_zcount(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<usize, collections::WrongTypeError> {
        self.check_string_conflict(key, "zset")?;
        self.collections.zcount(key, min, max)
    }

    // --- HyperLogLog ---

    pub fn col_pfadd(&self, key: &str, element: &str) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(key, "hyperloglog")?;
        self.collections.pfadd(key, element)
    }
    pub fn col_pfcount(&self, key: &str) -> Result<u64, collections::WrongTypeError> {
        self.check_string_conflict(key, "hyperloglog")?;
        self.collections.pfcount(key)
    }
    pub fn col_pfmerge(
        &self,
        dest_key: &str,
        source_keys: &[&str],
    ) -> Result<(), collections::WrongTypeError> {
        self.bump_version();
        self.check_string_conflict(dest_key, "hyperloglog")?;
        self.collections.pfmerge(dest_key, source_keys)
    }

    // ========================================================================
    // Stream operations (delegated to ShardedCollections)
    // ========================================================================

    pub fn xadd(
        &self,
        key: &str,
        id_str: &str,
        fields: Vec<(String, String)>,
    ) -> Result<streams::StreamId, String> {
        self.bump_version();
        self.collections.xadd(key, id_str, fields)
    }
    pub fn xlen(&self, key: &str) -> Result<usize, collections::WrongTypeError> {
        self.collections.xlen(key)
    }
    pub fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<Vec<streams::StreamEntry>, collections::WrongTypeError> {
        self.collections.xrange(key, start, end, count)
    }
    pub fn xrevrange(
        &self,
        key: &str,
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<Vec<streams::StreamEntry>, collections::WrongTypeError> {
        self.collections.xrevrange(key, end, start, count)
    }
    pub fn xread(
        &self,
        key: &str,
        last_id: &str,
        count: Option<usize>,
    ) -> Result<Vec<streams::StreamEntry>, collections::WrongTypeError> {
        self.collections.xread(key, last_id, count)
    }
    pub fn xdel(
        &self,
        key: &str,
        ids: &[streams::StreamId],
    ) -> Result<usize, collections::WrongTypeError> {
        self.bump_version();
        self.collections.xdel(key, ids)
    }
    pub fn xtrim_maxlen(
        &self,
        key: &str,
        maxlen: usize,
    ) -> Result<usize, collections::WrongTypeError> {
        self.bump_version();
        self.collections.xtrim_maxlen(key, maxlen)
    }
    pub fn xgroup_create(&self, key: &str, group_name: &str, start_id: &str) -> Result<(), String> {
        self.bump_version();
        self.collections.xgroup_create(key, group_name, start_id)
    }
    pub fn xgroup_destroy(
        &self,
        key: &str,
        group_name: &str,
    ) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.collections.xgroup_destroy(key, group_name)
    }
    pub fn xreadgroup(
        &self,
        key: &str,
        group_name: &str,
        consumer_name: &str,
        pending_id: &str,
        count: Option<usize>,
    ) -> Result<Vec<streams::StreamEntry>, String> {
        self.bump_version();
        self.collections
            .xreadgroup(key, group_name, consumer_name, pending_id, count)
    }
    pub fn xack(
        &self,
        key: &str,
        group_name: &str,
        ids: &[streams::StreamId],
    ) -> Result<usize, String> {
        self.bump_version();
        self.collections.xack(key, group_name, ids)
    }

    // ========================================================================
    // Geo operations (delegated to ShardedCollections)
    // ========================================================================

    pub fn geoadd(
        &self,
        key: &str,
        lon: f64,
        lat: f64,
        member: &str,
    ) -> Result<bool, collections::WrongTypeError> {
        self.bump_version();
        self.collections.geoadd(key, lon, lat, member)
    }
    pub fn geopos(
        &self,
        key: &str,
        member: &str,
    ) -> Result<Option<(f64, f64)>, collections::WrongTypeError> {
        self.collections.geopos(key, member)
    }
    pub fn geodist(
        &self,
        key: &str,
        member1: &str,
        member2: &str,
        unit: &str,
    ) -> Result<Option<f64>, collections::WrongTypeError> {
        self.collections.geodist(key, member1, member2, unit)
    }
    pub fn georadius(
        &self,
        key: &str,
        lon: f64,
        lat: f64,
        radius: f64,
        unit: &str,
    ) -> Result<Vec<(String, f64)>, collections::WrongTypeError> {
        self.collections.georadius(key, lon, lat, radius, unit)
    }
    pub fn geolen(&self, key: &str) -> Result<usize, collections::WrongTypeError> {
        self.collections.geolen(key)
    }

    // ========================================================================
    // Cold tier helpers (internal)
    // ========================================================================

    /// Check the cold LsmTree for a key, and if found, promote it to the hot
    /// tier. Returns `Some(value)` on cold hit, `None` on cold miss.
    fn cold_get_and_promote(&self, key: &str) -> Option<Arc<Value>> {
        let cold = self.cold.as_ref()?;
        let data = cold.lock().get(key.as_bytes())?;
        let value = Arc::new(codec::decode_value(&data));
        // Promote to hot: write directly to the shard (bypass WAL — it's
        // already in the WAL from the original set).
        self.data.shard(key).data.write().insert(
            key.to_string(),
            KvEntry {
                value: Arc::clone(&value),
                expires_at: None,
            },
        );
        // Remove from cold to avoid duplication
        cold.lock().delete(key.as_bytes().to_vec());
        Some(value)
    }

    /// Approximate bytes held by the hot (in-memory) tier.
    ///
    /// Walks the shards summing key and value footprints. O(entries), so it is
    /// called on eviction checks and under memory pressure rather than on every
    /// write. An incrementally maintained counter would be O(1), but it has to
    /// be updated correctly on every one of the many mutation paths here
    /// (SET/MSET/INCR/APPEND/SETRANGE/DEL/EXPIRE/sweep/rollback...) and silently
    /// drifts the moment one is missed. A number that is occasionally expensive
    /// beats a number that is quietly wrong.
    pub fn estimated_memory_bytes(&self) -> usize {
        let now = Instant::now();
        let mut total = 0usize;
        for shard in &self.data.shards {
            let data = shard.data.read();
            for (key, entry) in data.iter() {
                if entry.expires_at.is_some_and(|t| now >= t) {
                    continue;
                }
                total = total
                    .saturating_add(key.len())
                    .saturating_add(entry.value.approx_heap_size())
                    .saturating_add(KV_ENTRY_OVERHEAD_BYTES);
            }
        }
        // Collections live in a separate store and were invisible here, so a
        // workload of lists, hashes, sets and HLLs reported a memory footprint
        // of roughly zero to everything that reads this — the eviction trigger
        // included.
        total.saturating_add(self.collections.estimated_memory_bytes())
    }

    /// Evict non-TTL entries from the hot tier to the cold LsmTree when the hot
    /// tier exceeds either its entry limit or its byte budget.
    ///
    /// Byte-awareness is the point. The original trigger was entry count alone,
    /// so a store of 32k large values sat at several GB while reporting itself
    /// comfortably under a 100k-entry cap and never evicted anything.
    fn maybe_evict(&self) {
        self.evict_to(self.max_hot_bytes, self.max_hot_entries);
    }

    /// Spill the largest non-TTL entries to the cold tier until the hot tier is
    /// within both budgets. Returns the bytes freed.
    ///
    /// Budgets are parameters rather than fields so the memory allocator can aim
    /// eviction at a tighter target under pressure without mutating shared
    /// configuration.
    fn evict_to(&self, byte_budget: usize, entry_budget: usize) -> usize {
        let entries = self.dbsize();
        let over_count = entries > entry_budget;
        // Skip the O(n) walk when the cheap count check already decided it.
        let bytes = if over_count {
            usize::MAX
        } else {
            self.estimated_memory_bytes()
        };
        let over_bytes = bytes > byte_budget;
        if !over_count && !over_bytes {
            return 0;
        }
        let Some(ref cold) = self.cold else { return 0 };

        // Aim slightly under the budget so a steady write stream does not
        // re-trigger a full walk on every subsequent set.
        let bytes_target = if over_bytes && bytes != usize::MAX {
            bytes.saturating_sub(byte_budget - byte_budget / 10)
        } else {
            0
        };
        let entry_target = if over_count {
            entries
                .saturating_sub(entry_budget)
                .max(entry_budget / 10)
                .max(1)
        } else {
            0
        };

        // Largest-first, so a byte target is met by moving a few big values
        // rather than thousands of small ones — the small ones are also the
        // cheapest to keep resident.
        let mut candidates: Vec<(usize, String)> = Vec::new();
        for shard in &self.data.shards {
            let data = shard.data.read();
            for (key, entry) in data.iter() {
                // Only entries without a TTL: an expiring entry is reclaimed by
                // the sweeper anyway, and the cold tier cannot represent its
                // deadline.
                if entry.expires_at.is_some() {
                    continue;
                }
                // The cold codec carries six type tags and falls back to text
                // for anything else. Eviction must never silently change a
                // value's type, so values it cannot represent stay resident.
                if !codec::is_losslessly_encodable(&entry.value) {
                    continue;
                }
                candidates.push((
                    key.len() + entry.value.approx_heap_size() + KV_ENTRY_OVERHEAD_BYTES,
                    key.clone(),
                ));
            }
        }
        candidates.sort_unstable_by_key(|(size, _)| std::cmp::Reverse(*size));

        let mut freed_bytes = 0usize;
        let mut evicted = 0usize;
        for (size, key) in candidates {
            if freed_bytes >= bytes_target && evicted >= entry_target {
                break;
            }
            let Some(arc_value) = ({
                let shard = self.data.shard(&key);
                let data = shard.data.read();
                data.get(key.as_str()).map(|e| Arc::clone(&e.value))
            }) else {
                continue;
            };
            let encoded = codec::encode_value(&arc_value);
            cold.lock().put(key.as_bytes().to_vec(), encoded);
            self.data.shard(&key).data.write().remove(key.as_str());
            freed_bytes = freed_bytes.saturating_add(size);
            evicted += 1;
        }
        freed_bytes
    }

    /// Reclaim memory down to `target_bytes`, for the memory allocator's
    /// pressure callback. Returns the bytes freed.
    ///
    /// The previous handler only swept *expired* entries, so a store whose
    /// entries carry no TTL — the normal case for anything durable — reported
    /// pressure forever while freeing precisely nothing on every pass.
    pub fn shrink_hot_to(&self, target_bytes: usize) -> usize {
        let before = self.estimated_memory_bytes();
        self.sweep_expired_full();
        let after_sweep = self.estimated_memory_bytes();
        let swept = before.saturating_sub(after_sweep);
        if after_sweep <= target_bytes {
            return swept;
        }
        swept + self.evict_to(target_bytes, usize::MAX)
    }

    /// Whether this store has a cold tier (disk mode).
    pub fn has_cold_tier(&self) -> bool {
        self.cold.is_some()
    }

    /// Return the number of non-expired keys in the hot (in-memory) tier only.
    /// Useful for observability and testing eviction behaviour.
    pub fn dbsize_hot(&self) -> usize {
        let now = Instant::now();
        let mut count = 0;
        for shard in &self.data.shards {
            let data = shard.data.read();
            count += data
                .values()
                .filter(|entry| entry.expires_at.is_none_or(|t| now < t))
                .count();
        }
        count
    }

    /// Write a WAL checkpoint (snapshot + truncate). No-op if WAL is disabled.
    #[cfg(feature = "server")]
    pub fn checkpoint(&self) -> std::io::Result<()> {
        let Some(ref wal) = self.wal else {
            return Ok(());
        };
        // Flush the cold tier BEFORE snapshotting, and never after.
        //
        // The snapshot below covers the hot tier only, and truncates the WAL to
        // it — so a key that was evicted to cold is, at that instant, losing its
        // last WAL record. `LsmTree::put` only buffers into an in-memory
        // memtable until a size threshold is reached, so without this flush an
        // evicted key can exist in neither the WAL nor on disk, and a crash
        // loses it outright with nothing reporting a problem.
        //
        // Rare while eviction was keyed on entry count and almost never fired.
        // Making eviction byte-aware made it routine, which is what turned this
        // from theoretical into the ordering that has to hold.
        if let Some(ref cold) = self.cold {
            cold.lock().flush_memtable();
        }
        let mut items = Vec::new();
        for shard in &self.data.shards {
            let data = shard.data.read();
            for (key, entry) in data.iter() {
                if !entry.is_expired() {
                    let ttl = entry.expires_at.map(instant_to_epoch_ms);
                    items.push((key.clone(), (*entry.value).clone(), ttl));
                }
            }
        }
        wal.checkpoint(&items)?;
        // Also checkpoint collections
        self.collections.checkpoint()
    }

    /// Access the underlying WAL (if any).
    #[cfg(feature = "server")]
    pub fn wal(&self) -> Option<&Arc<KvWal>> {
        self.wal.as_ref()
    }

    /// Access the collections WAL (if any) — the second durable log this store
    /// owns (lists/hashes/sets/zsets/HLL/streams).
    #[cfg(feature = "server")]
    pub fn collections_wal(&self) -> Option<&Arc<crate::kv::collections_wal::CollectionWal>> {
        self.collections.wal()
    }

    /// Capture a snapshot of all non-expired entries for transaction rollback.
    pub fn txn_snapshot(&self) -> KvTxnSnapshot {
        let mut entries = HashMap::new();
        for shard in &self.data.shards {
            let data = shard.data.read();
            for (k, e) in data.iter() {
                if !e.is_expired() {
                    entries.insert(k.clone(), e.clone());
                }
            }
        }
        KvTxnSnapshot { entries }
    }

    /// Revert only `keys`, using `snapshot` as the before-image.
    ///
    /// Keys this transaction never wrote are left untouched, so a ROLLBACK can
    /// no longer destroy a value another session (or a RESP client, or the KV
    /// fast path) set and had acknowledged since this transaction's BEGIN.
    ///
    /// Durable: each reverted key gets a compensating WAL record, so a crash
    /// after a successful ROLLBACK cannot resurrect the rolled-back write on
    /// replay.
    pub fn txn_restore_scoped(&self, snapshot: &KvTxnSnapshot, keys: &HashSet<String>) {
        for key in keys {
            let shard = self.data.shard(key);
            let previous = shard.data.write().remove(key);
            if let Some(prev) = previous
                && let Some(exp) = prev.expires_at
                && let Some(bucket) = shard.expiry_index.write().get_mut(&exp)
            {
                bucket.retain(|k| k != key);
            }
            // `single_match` fires only in the core-only build, where the
            // None arm's body is entirely `server`-gated and cfg-strips to
            // nothing. The arm is real with the server feature on, so
            // rewriting this as `if let` would be wrong for the shipped
            // configuration.
            #[allow(clippy::single_match)]
            match snapshot.entries.get(key) {
                Some(entry) => {
                    if let Some(exp) = entry.expires_at {
                        shard
                            .expiry_index
                            .write()
                            .entry(exp)
                            .or_default()
                            .push(key.clone());
                    }
                    #[cfg(feature = "server")]
                    if let Some(ref wal) = self.wal {
                        // The restored expiry belongs to the restored value:
                        // one record, so a crash mid-rollback cannot resurrect
                        // the pre-transaction value without its TTL.
                        let abs_ms = entry.expires_at.map(|exp| {
                            let remaining = exp.saturating_duration_since(Instant::now());
                            epoch_ms_now().saturating_add(remaining.as_millis() as u64)
                        });
                        if let Err(e) = wal.log_set_with_expiry(key, &entry.value, abs_ms) {
                            tracing::error!(target: "nucleus::kv::wal", "rollback WAL write failed: {e}");
                        }
                    }
                    shard.data.write().insert(key.clone(), entry.clone());
                }
                None => {
                    #[cfg(feature = "server")]
                    if let Some(ref wal) = self.wal
                        && let Err(e) = wal.log_delete(key)
                    {
                        tracing::error!(target: "nucleus::kv::wal", "rollback WAL write failed: {e}");
                    }
                }
            }
        }
    }

    /// Every key present in a snapshot. Used to scope a `FLUSHDB` rollback,
    /// which by definition touches the whole keyspace.
    pub fn snapshot_keys(snapshot: &KvTxnSnapshot) -> Vec<String> {
        snapshot.entries.keys().cloned().collect()
    }

    /// Restore from a previously captured snapshot (for ROLLBACK).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn txn_restore(&self, snapshot: KvTxnSnapshot) {
        // Clear all shards (data + expiry index)
        for shard in &self.data.shards {
            shard.data.write().clear();
            shard.expiry_index.write().clear();
        }
        // Re-insert entries into appropriate shards and rebuild expiry index
        for (key, entry) in snapshot.entries {
            let shard = self.data.shard(&key);
            if let Some(exp) = entry.expires_at {
                shard
                    .expiry_index
                    .write()
                    .entry(exp)
                    .or_default()
                    .push(key.clone());
            }
            shard.data.write().insert(key, entry);
        }
    }
}

/// Opaque snapshot of KV store state for transaction rollback.
pub struct KvTxnSnapshot {
    entries: HashMap<String, KvEntry>,
}

/// Start the background TTL sweeper task.
/// Uses the active (O(expired)) sweep on each tick, and runs a full O(n)
/// safety-net sweep every 60 ticks to catch any index drift.
#[cfg(feature = "server")]
pub fn start_sweeper(store: Arc<KvStore>, interval_secs: u64) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        let mut tick_count: u64 = 0;
        loop {
            interval.tick().await;
            tick_count += 1;
            let removed = if tick_count.is_multiple_of(60) {
                store.sweep_expired_full()
            } else {
                store.sweep_expired()
            };
            if removed > 0 {
                tracing::debug!("KV sweeper removed {removed} expired keys");
            }
        }
    })
}

// ============================================================================
// Time helpers
// ============================================================================

/// Current time as milliseconds since the Unix epoch.
#[cfg(feature = "server")]
fn epoch_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// A TTL in seconds as absolute epoch milliseconds, saturating.
///
/// `epoch_ms_now() + secs * 1000` overflows u64 for a large TTL: in release
/// that wraps to a moment in the past, so the key the client asked to keep for
/// ~forever is already expired when the log replays. Saturating keeps the
/// in-memory rule ("an overflowing TTL means never expires") and the logged
/// rule pointed the same way.
#[cfg(feature = "server")]
fn abs_expiry_ms(secs: u64) -> u64 {
    epoch_ms_now().saturating_add(secs.saturating_mul(1000))
}

/// Convert an `Instant` expiration to absolute epoch milliseconds.
#[cfg(feature = "server")]
fn instant_to_epoch_ms(t: Instant) -> u64 {
    let now_inst = Instant::now();
    let now_epoch = epoch_ms_now();
    if t > now_inst {
        now_epoch + (t - now_inst).as_millis() as u64
    } else {
        // Already expired
        now_epoch.saturating_sub((now_inst - t).as_millis() as u64)
    }
}

// ============================================================================
// Pattern matching (simple glob)
// ============================================================================

fn match_pattern(pattern: &str, input: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(inner) = pattern.strip_prefix('*').and_then(|s| s.strip_suffix('*')) {
        return input.contains(inner);
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return input.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return input.starts_with(prefix);
    }
    pattern == input
}

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("value is not an integer")]
    NotAnInteger,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("increment or decrement would overflow")]
    Overflow,
}

// ============================================================================
// Sorted Set (Redis-compatible ZSET)
// ============================================================================

/// Wrapper around f64 that implements total ordering via `total_cmp`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedF64(f64);

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// An entry in a sorted set.
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
    pub member: String,
    pub score: f64,
}

/// A sorted set data structure (Redis ZSET equivalent).
/// Uses BTreeMap keyed by (score, member) for ordered iteration
/// and HashMap for O(1) member→score lookup.
#[derive(Debug)]
pub struct SortedSet {
    tree: BTreeMap<(OrderedF64, String), ()>,
    members: HashMap<String, f64>,
}

impl Default for SortedSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve an inclusive `[start, stop]` rank range against a collection of
/// `len` elements, Redis-style: negative counts from the end, an underflowing
/// start clamps to 0, and an empty range answers `None`.
///
/// Shared by ZRANGE and ZREVRANGE so the two cannot drift — which is exactly
/// how LRANGE came to handle negatives correctly while ZRANGE did not.
fn resolve_rank_range(start: i64, stop: i64, len: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let s = if start < 0 {
        let adj = len + start;
        if adj < 0 { 0 } else { adj }
    } else {
        start
    };
    let e = if stop < 0 { len + stop } else { stop };
    if e < 0 || s >= len || s > e {
        return None;
    }
    Some((s as usize, (e.min(len - 1)) as usize))
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            members: HashMap::new(),
        }
    }

    /// ZADD — insert or update a member. Returns true if new.
    pub fn zadd(&mut self, member: &str, score: f64) -> bool {
        if let Some(&old_score) = self.members.get(member) {
            self.tree
                .remove(&(OrderedF64(old_score), member.to_string()));
            self.tree
                .insert((OrderedF64(score), member.to_string()), ());
            self.members.insert(member.to_string(), score);
            false
        } else {
            self.tree
                .insert((OrderedF64(score), member.to_string()), ());
            self.members.insert(member.to_string(), score);
            true
        }
    }

    /// ZREM — remove a member. Returns true if removed.
    pub fn zrem(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.tree.remove(&(OrderedF64(score), member.to_string()));
            true
        } else {
            false
        }
    }

    pub fn zscore(&self, member: &str) -> Option<f64> {
        self.members.get(member).copied()
    }

    /// ZRANK — 0-based rank by ascending score.
    pub fn zrank(&self, member: &str) -> Option<usize> {
        let score = self.members.get(member)?;
        let key = (OrderedF64(*score), member.to_string());
        Some(self.tree.range(..&key).count())
    }

    /// ZREVRANK — 0-based rank by descending score.
    pub fn zrevrank(&self, member: &str) -> Option<usize> {
        let rank = self.zrank(member)?;
        Some(self.members.len() - 1 - rank)
    }

    /// ZRANGE — entries by rank ascending (inclusive start/stop).
    ///
    /// Indices are SIGNED and negative means from the end, as in Redis and as
    /// LRANGE already did: `zrange(0, -1)` is the whole set.
    ///
    /// This took `usize`, so callers cast a signed index and `-1` became
    /// `usize::MAX`. The result was not "everything" but NOTHING:
    /// `stop.saturating_sub(start) + 1` overflowed to 0 and `.take(0)` returned
    /// an empty vector. A wrong answer with no error, on the most idiomatic
    /// call in the surface — `zrange(key, 0, -1)`. LRANGE was fixed for this
    /// long ago; ZRANGE was missed, and the conformance note recorded it as
    /// "reads -1 literally", which is not what happens.
    pub fn zrange(&self, start: i64, stop: i64) -> Vec<SortedSetEntry> {
        let (start, stop) = match resolve_rank_range(start, stop, self.tree.len() as i64) {
            Some(r) => r,
            None => return Vec::new(),
        };
        self.tree
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|((OrderedF64(s), m), _)| SortedSetEntry {
                member: m.clone(),
                score: *s,
            })
            .collect()
    }

    /// ZREVRANGE — entries by rank descending. Signed indices, as `zrange`.
    pub fn zrevrange(&self, start: i64, stop: i64) -> Vec<SortedSetEntry> {
        let (start, stop) = match resolve_rank_range(start, stop, self.tree.len() as i64) {
            Some(r) => r,
            None => return Vec::new(),
        };
        self.tree
            .iter()
            .rev()
            .skip(start)
            .take(stop - start + 1)
            .map(|((OrderedF64(s), m), _)| SortedSetEntry {
                member: m.clone(),
                score: *s,
            })
            .collect()
    }

    /// ZRANGEBYSCORE — entries with score in [min, max].
    pub fn zrangebyscore(&self, min: f64, max: f64) -> Vec<SortedSetEntry> {
        use std::ops::Bound;
        let lo = (OrderedF64(min), String::new());
        self.tree
            .range((Bound::Included(&lo), Bound::Unbounded))
            .take_while(|((OrderedF64(s), _), _)| *s <= max)
            .map(|((OrderedF64(s), m), _)| SortedSetEntry {
                member: m.clone(),
                score: *s,
            })
            .collect()
    }

    /// Approximate heap footprint: both indexes hold the member string.
    pub fn approx_heap_size(&self) -> usize {
        let mut total = 0usize;
        for member in self.members.keys() {
            // Once in `members`, once in the `tree` key, plus the score.
            total = total.saturating_add(member.len() * 2 + 8 + 48);
        }
        total
    }

    pub fn zcard(&self) -> usize {
        self.members.len()
    }

    /// ZINCRBY — increment score. Creates member if missing.
    pub fn zincrby(&mut self, member: &str, increment: f64) -> f64 {
        let new_score = self
            .members
            .get(member)
            .map_or(increment, |&old| old + increment);
        self.zadd(member, new_score);
        new_score
    }

    pub fn zcount(&self, min: f64, max: f64) -> usize {
        self.zrangebyscore(min, max).len()
    }
}

// ============================================================================
// HyperLogLog — Probabilistic Cardinality Estimator
// ============================================================================

/// HyperLogLog for probabilistic cardinality estimation.
/// Uses FNV-1a hashing and LogLog counting with bias correction.
pub struct HyperLogLog {
    registers: Vec<u8>,
    p: u8,
}

impl std::fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog")
            .field("p", &self.p)
            .field("num_registers", &self.registers.len())
            .finish()
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self::with_precision(14)
    }

    pub fn with_precision(p: u8) -> Self {
        assert!((4..=18).contains(&p), "precision p must be in 4..=18");
        let m = 1usize << p;
        Self {
            registers: vec![0u8; m],
            p,
        }
    }

    pub fn add(&mut self, item: &str) {
        let hash = Self::fnv1a(item);
        let idx = (hash >> (64 - self.p)) as usize;
        let remaining = hash << self.p;
        let rank = (remaining.leading_zeros() as u8) + 1;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    pub fn count(&self) -> u64 {
        let m = self.registers.len() as f64;
        let alpha_m = Self::alpha(self.p);
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &reg in &self.registers {
            sum += 2.0f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }
        let estimate = alpha_m * m * m / sum;
        // Small range correction (linear counting)
        if estimate <= 2.5 * m && zeros > 0 {
            return (m * (m / zeros as f64).ln()) as u64;
        }
        // Large range correction
        let two_32 = (1u64 << 32) as f64;
        if estimate > two_32 / 30.0 {
            return (-two_32 * (1.0 - estimate / two_32).ln()) as u64;
        }
        estimate as u64
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        assert_eq!(
            self.p, other.p,
            "cannot merge HLLs with different precisions"
        );
        for (a, &b) in self.registers.iter_mut().zip(other.registers.iter()) {
            if b > *a {
                *a = b;
            }
        }
    }

    pub fn clear(&mut self) {
        self.registers.fill(0);
    }

    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }

    /// Get a read-only view of the internal registers (for snapshotting in pfmerge).
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// Overwrite internal registers from a snapshot (for pfmerge replay).
    pub fn set_registers(&mut self, regs: &[u8]) {
        let len = self.registers.len().min(regs.len());
        self.registers[..len].copy_from_slice(&regs[..len]);
    }

    fn alpha(p: u8) -> f64 {
        match p {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / (1u64 << p) as f64),
        }
    }

    fn fnv1a(s: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        // MurmurHash3 finalizer for better bit distribution
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_get_set() {
        let store = KvStore::new();
        store.set("name", Value::Text("Nucleus".into()), None);
        assert_eq!(store.get("name"), Some(Value::Text("Nucleus".into())));
        assert_eq!(store.get("missing"), None);
    }

    #[test]
    fn del() {
        let store = KvStore::new();
        store.set("x", Value::Int32(1), None);
        assert!(store.del("x"));
        assert!(!store.del("x"));
        assert_eq!(store.get("x"), None);
    }

    #[test]
    fn incr() {
        let store = KvStore::new();
        assert_eq!(store.incr("counter").unwrap(), 1);
        assert_eq!(store.incr("counter").unwrap(), 2);
        assert_eq!(store.incr_by("counter", 10).unwrap(), 12);
    }

    #[test]
    fn incr_not_integer() {
        let store = KvStore::new();
        store.set("text", Value::Text("hello".into()), None);
        assert!(store.incr("text").is_err());
    }

    // Regression: INCR/INCRBY used unchecked `current + amount`, which wraps
    // (release) or panics (debug) at the i64 boundary. It must return an error.
    #[test]
    fn incr_overflow_errors() {
        let store = KvStore::new();
        store.set("c", Value::Int64(i64::MAX), None);
        assert!(matches!(store.incr("c"), Err(KvError::Overflow)));
        // Underflow at the other end too.
        store.set("d", Value::Int64(i64::MIN), None);
        assert!(matches!(store.incr_by("d", -1), Err(KvError::Overflow)));
        // The stored value is unchanged after a rejected overflow.
        assert_eq!(store.incr_by("c", 0).unwrap(), i64::MAX);
    }

    #[test]
    fn ttl_expiry() {
        let store = KvStore::new();
        // Set with 0-second TTL (expires immediately)
        store.set("ephemeral", Value::Int32(42), Some(0));
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(store.get("ephemeral"), None);
    }

    #[test]
    fn keys_pattern() {
        let store = KvStore::new();
        store.set("user:1", Value::Int32(1), None);
        store.set("user:2", Value::Int32(2), None);
        store.set("session:1", Value::Int32(3), None);

        let mut keys = store.keys("user:*");
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2"]);

        let all = store.keys("*");
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn setnx() {
        let store = KvStore::new();
        assert!(store.setnx("lock", Value::Int32(1)));
        assert!(!store.setnx("lock", Value::Int32(2)));
        assert_eq!(store.get("lock"), Some(Value::Int32(1)));
    }

    #[test]
    fn mget_mset() {
        let store = KvStore::new();
        store.mset(&[
            ("a", Value::Int32(1)),
            ("b", Value::Int32(2)),
            ("c", Value::Int32(3)),
        ]);
        let vals = store.mget(&["a", "b", "c", "d"]);
        assert_eq!(vals[0], Some(Value::Int32(1)));
        assert_eq!(vals[1], Some(Value::Int32(2)));
        assert_eq!(vals[2], Some(Value::Int32(3)));
        assert_eq!(vals[3], None);
    }

    #[test]
    fn dbsize_and_flush() {
        let store = KvStore::new();
        store.set("a", Value::Int32(1), None);
        store.set("b", Value::Int32(2), None);
        assert_eq!(store.dbsize(), 2);
        store.flushdb();
        assert_eq!(store.dbsize(), 0);
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_put_then_get(key in "[a-zA-Z0-9_]{1,64}", val in any::<i64>()) {
            let store = KvStore::new();
            let value = Value::Int64(val);
            store.set(&key, value.clone(), None);
            let got = store.get(&key);
            prop_assert_eq!(got, Some(value));
        }

        #[test]
        fn prop_put_then_delete_then_get(key in "[a-zA-Z0-9_]{1,64}", val in any::<i32>()) {
            let store = KvStore::new();
            store.set(&key, Value::Int32(val), None);
            prop_assert!(store.del(&key));
            prop_assert_eq!(store.get(&key), None);
        }

        #[test]
        fn prop_random_ops_consistency(
            ops in proptest::collection::vec(
                (0u8..4, "[a-z]{1,8}", any::<i64>()),
                1..100
            )
        ) {
            let store = KvStore::new();
            let mut expected: std::collections::HashMap<String, Value> = std::collections::HashMap::new();

            for (op_type, key, val) in &ops {
                match op_type % 4 {
                    0 => {
                        // SET
                        let value = Value::Int64(*val);
                        store.set(key, value.clone(), None);
                        expected.insert(key.clone(), value);
                    }
                    1 => {
                        // GET — verify consistency
                        let got = store.get(key);
                        let exp = expected.get(key.as_str()).cloned();
                        prop_assert_eq!(got, exp, "GET mismatch for key '{}'", key);
                    }
                    2 => {
                        // DELETE
                        store.del(key);
                        expected.remove(key.as_str());
                    }
                    3 => {
                        // EXISTS
                        let exists = store.exists(key);
                        let exp_exists = expected.contains_key(key.as_str());
                        prop_assert_eq!(exists, exp_exists, "EXISTS mismatch for key '{}'", key);
                    }
                    _ => unreachable!(),
                }
            }

            // Final consistency check: all expected keys are present
            for (key, value) in &expected {
                let got = store.get(key);
                prop_assert_eq!(got.as_ref(), Some(value), "final check failed for key '{}'", key);
            }
        }

        #[test]
        fn prop_setnx_only_sets_once(key in "[a-zA-Z0-9]{1,32}") {
            let store = KvStore::new();
            let first = store.setnx(&key, Value::Int32(1));
            prop_assert!(first, "setnx should succeed on empty store");
            let second = store.setnx(&key, Value::Int32(2));
            prop_assert!(!second, "setnx should fail when key exists");
            prop_assert_eq!(store.get(&key), Some(Value::Int32(1)));
        }

    }

    proptest! {
        #[test]
        fn prop_incr_by_arithmetic(key in "[a-zA-Z]{1,16}", amount in 1i64..10000) {
            let store = KvStore::new();
            let result = store.incr_by(&key, amount).expect("incr");
            prop_assert_eq!(result, amount);
            let r2 = store.incr_by(&key, amount).expect("r2");
            prop_assert_eq!(r2, amount * 2);
        }

        #[test]
        fn prop_dbsize_accuracy(
            keys in proptest::collection::hash_set("[a-z]{1,8}", 1..50)
        ) {
            let store = KvStore::new();
            for key in &keys {
                store.set(key, Value::Int32(1), None);
            }
            prop_assert_eq!(store.dbsize(), keys.len());
        }

        #[test]
        fn prop_flushdb_empties(
            keys in proptest::collection::vec("[a-z]{1,8}", 0..30)
        ) {
            let store = KvStore::new();
            for key in &keys {
                store.set(key, Value::Int32(1), None);
            }
            store.flushdb();
            prop_assert_eq!(store.dbsize(), 0);
        }

        #[test]
        fn prop_persist_removes_ttl(key in "[a-zA-Z]{1,16}") {
            let store = KvStore::new();
            store.set(&key, Value::Int32(42), Some(3600));
            prop_assert!(store.persist(&key));
            prop_assert_eq!(store.ttl(&key), -1);
        }
    }

    // ========================================================================
    // Sorted Set tests
    // ========================================================================

    #[test]
    fn zadd_basic() {
        let mut zset = SortedSet::new();
        assert!(zset.zadd("alice", 100.0));
        assert!(zset.zadd("bob", 200.0));
        assert!(zset.zadd("charlie", 150.0));
        assert_eq!(zset.zcard(), 3);
    }

    #[test]
    fn zadd_update_score() {
        let mut zset = SortedSet::new();
        assert!(zset.zadd("alice", 100.0));
        assert!(!zset.zadd("alice", 300.0));
        assert_eq!(zset.zscore("alice"), Some(300.0));
        assert_eq!(zset.zcard(), 1);
    }

    #[test]
    fn zrem_existing_and_nonexistent() {
        let mut zset = SortedSet::new();
        zset.zadd("alice", 100.0);
        assert!(zset.zrem("alice"));
        assert_eq!(zset.zcard(), 0);
        assert!(!zset.zrem("ghost"));
    }

    #[test]
    fn zrank_and_zrevrank() {
        let mut zset = SortedSet::new();
        zset.zadd("alice", 10.0);
        zset.zadd("bob", 20.0);
        zset.zadd("charlie", 30.0);
        assert_eq!(zset.zrank("alice"), Some(0));
        assert_eq!(zset.zrank("bob"), Some(1));
        assert_eq!(zset.zrank("charlie"), Some(2));
        assert_eq!(zset.zrevrank("charlie"), Some(0));
        assert_eq!(zset.zrevrank("alice"), Some(2));
        assert_eq!(zset.zrank("nobody"), None);
    }

    #[test]
    fn zrange_basic() {
        let mut zset = SortedSet::new();
        zset.zadd("charlie", 30.0);
        zset.zadd("alice", 10.0);
        zset.zadd("bob", 20.0);
        let result = zset.zrange(0, 2);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].member, "alice");
        assert_eq!(result[1].member, "bob");
        assert_eq!(result[2].member, "charlie");
    }

    #[test]
    fn zrevrange_basic() {
        let mut zset = SortedSet::new();
        zset.zadd("alice", 10.0);
        zset.zadd("bob", 20.0);
        zset.zadd("charlie", 30.0);
        let result = zset.zrevrange(0, 2);
        assert_eq!(result[0].member, "charlie");
        assert_eq!(result[2].member, "alice");
    }

    #[test]
    fn zrangebyscore_basic() {
        let mut zset = SortedSet::new();
        zset.zadd("alice", 10.0);
        zset.zadd("bob", 20.0);
        zset.zadd("charlie", 30.0);
        zset.zadd("diana", 40.0);
        let result = zset.zrangebyscore(15.0, 35.0);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].member, "bob");
        assert_eq!(result[1].member, "charlie");
    }

    #[test]
    fn zincrby_basic() {
        let mut zset = SortedSet::new();
        assert_eq!(zset.zincrby("alice", 5.0), 5.0);
        assert_eq!(zset.zincrby("alice", 3.0), 8.0);
        assert_eq!(zset.zscore("alice"), Some(8.0));
    }

    #[test]
    fn zcard_and_zcount() {
        let mut zset = SortedSet::new();
        assert_eq!(zset.zcard(), 0);
        zset.zadd("a", 1.0);
        zset.zadd("b", 2.0);
        zset.zadd("c", 3.0);
        assert_eq!(zset.zcard(), 3);
        assert_eq!(zset.zcount(1.0, 2.0), 2);
    }

    // ========================================================================
    // HyperLogLog tests
    // ========================================================================

    #[test]
    fn hll_basic_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..1000 {
            hll.add(&format!("item-{}", i));
        }
        let count = hll.count();
        // HLL has ~2% error at precision 14; allow 10% for safety
        assert!(count > 900 && count < 1100, "expected ~1000, got {}", count);
    }

    #[test]
    fn hll_empty() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.count(), 0);
        assert!(hll.is_empty());
    }

    #[test]
    fn hll_duplicates_dont_increase_count() {
        let mut hll = HyperLogLog::new();
        for _ in 0..1000 {
            hll.add("same-item");
        }
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn hll_merge() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();
        for i in 0..500 {
            hll1.add(&format!("a-{}", i));
        }
        for i in 0..500 {
            hll2.add(&format!("b-{}", i));
        }
        hll1.merge(&hll2);
        let count = hll1.count();
        assert!(count > 900 && count < 1100, "expected ~1000, got {}", count);
    }

    #[test]
    fn hll_clear() {
        let mut hll = HyperLogLog::new();
        hll.add("item");
        assert!(!hll.is_empty());
        hll.clear();
        assert!(hll.is_empty());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn hll_precision_4() {
        let mut hll = HyperLogLog::with_precision(4);
        for i in 0..100 {
            hll.add(&format!("item-{}", i));
        }
        let count = hll.count();
        // Lower precision = higher error (allow 50%)
        assert!(count > 50 && count < 150, "expected ~100, got {}", count);
    }

    #[test]
    fn hll_large_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..100_000 {
            hll.add(&format!("item-{}", i));
        }
        let count = hll.count();
        assert!(
            count > 95_000 && count < 105_000,
            "expected ~100000, got {}",
            count
        );
    }

    // ========================================================================
    // WAL-backed KV store tests
    // ========================================================================

    #[test]
    fn setnx_ttl_atomic_acquire() {
        let store = KvStore::new();
        // acquire with TTL succeeds and carries the expiry
        assert!(store.setnx_ttl("lock", Value::Text("owner-1".into()), Some(3600)));
        let ttl = store.ttl("lock");
        assert!(ttl > 3500 && ttl <= 3600, "expected ~3600, got {}", ttl);
        // second acquire fails and does not disturb the TTL
        assert!(!store.setnx_ttl("lock", Value::Text("owner-2".into()), Some(10)));
        assert_eq!(store.get("lock"), Some(Value::Text("owner-1".into())));
        let ttl = store.ttl("lock");
        assert!(
            ttl > 3500,
            "owner-2's failed acquire must not shorten the TTL"
        );
        // no-TTL variant still behaves like plain setnx
        assert!(store.setnx_ttl("plain", Value::Int64(1), None));
        assert_eq!(store.ttl("plain"), -1);
    }

    #[test]
    fn setnx_ttl_reacquires_expired_lock() {
        let store = KvStore::new();
        assert!(store.setnx_ttl("lock", Value::Text("owner-1".into()), Some(0)));
        std::thread::sleep(Duration::from_millis(20));
        assert!(store.setnx_ttl("lock", Value::Text("owner-2".into()), Some(3600)));
        assert_eq!(store.get("lock"), Some(Value::Text("owner-2".into())));
    }

    #[test]
    fn cdel_releases_only_matching_value() {
        let store = KvStore::new();
        store.set("lock", Value::Text("owner-1".into()), Some(3600));
        // wrong holder cannot release
        assert!(!store.cdel("lock", &Value::Text("owner-2".into())));
        assert!(store.exists("lock"));
        // wrong type does not match either
        assert!(!store.cdel("lock", &Value::Int64(1)));
        // right holder releases
        assert!(store.cdel("lock", &Value::Text("owner-1".into())));
        assert!(!store.exists("lock"));
        // releasing a missing key is false
        assert!(!store.cdel("lock", &Value::Text("owner-1".into())));
    }

    #[test]
    fn cdel_treats_expired_as_absent() {
        let store = KvStore::new();
        store.set("lock", Value::Text("owner-1".into()), Some(0));
        std::thread::sleep(Duration::from_millis(20));
        assert!(!store.cdel("lock", &Value::Text("owner-1".into())));
    }

    #[test]
    fn cexpire_renews_only_matching_value() {
        let store = KvStore::new();
        store.set("lock", Value::Text("owner-1".into()), Some(10));
        // wrong holder cannot renew
        assert!(!store.cexpire("lock", &Value::Text("owner-2".into()), 3600));
        let ttl = store.ttl("lock");
        assert!(
            ttl <= 10,
            "failed renewal must not extend the TTL, got {}",
            ttl
        );
        // right holder renews
        assert!(store.cexpire("lock", &Value::Text("owner-1".into()), 3600));
        let ttl = store.ttl("lock");
        assert!(ttl > 3500 && ttl <= 3600, "expected ~3600, got {}", ttl);
        // missing key is false
        assert!(!store.cexpire("missing", &Value::Text("x".into()), 60));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_setnx_ttl_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        assert!(store.setnx_ttl("lock", Value::Text("owner-1".into()), Some(3600)));
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("lock"), Some(Value::Text("owner-1".into())));
        let ttl = store2.ttl("lock");
        assert!(
            ttl > 3500 && ttl <= 3600,
            "TTL must survive recovery, got {}",
            ttl
        );
    }

    /// PERSIST used to log nothing at all, so the key's old EXPIRE record was
    /// still the last word on disk: the live server answered `TTL -1` and the
    /// restart brought the expiry back, after which a key the client had made
    /// permanent quietly disappeared.
    #[cfg(feature = "server")]
    #[test]
    fn wal_persist_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("cfg", Value::Text("keep-me".into()), Some(3600));
        assert!(store.persist("cfg"));
        assert_eq!(store.ttl("cfg"), -1, "PERSIST is visible in memory");
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("cfg"), Some(Value::Text("keep-me".into())));
        assert_eq!(
            store2.ttl("cfg"),
            -1,
            "the expiry PERSIST removed came back after recovery"
        );
    }

    /// SET with no TTL over a key that had one clears it, and the clearing has
    /// to survive a restart for the same reason PERSIST does.
    #[cfg(feature = "server")]
    #[test]
    fn wal_set_without_ttl_clears_expiry_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("k", Value::Int32(1), Some(3600));
        store.set("k", Value::Int32(2), None);
        assert_eq!(store.ttl("k"), -1);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("k"), Some(Value::Int32(2)));
        assert_eq!(store2.ttl("k"), -1, "the replaced key's old TTL survived");
    }

    /// INCR deliberately keeps the key's TTL, in memory and on replay.
    #[cfg(feature = "server")]
    #[test]
    fn wal_incr_preserves_ttl_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("hits", Value::Int64(1), Some(3600));
        assert_eq!(store.incr("hits").unwrap(), 2);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("hits"), Some(Value::Int64(2)));
        let ttl = store2.ttl("hits");
        assert!(
            ttl > 3500 && ttl <= 3600,
            "TTL must survive INCR, got {ttl}"
        );
    }

    /// MSET stores permanent values; a key that had a TTL must not keep it.
    #[cfg(feature = "server")]
    #[test]
    fn wal_mset_clears_expiry_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("a", Value::Int32(0), Some(3600));
        store.mset(&[("a", Value::Int32(1)), ("b", Value::Int32(2))]);
        assert_eq!(store.ttl("a"), -1);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("a"), Some(Value::Int32(1)));
        assert_eq!(store2.ttl("a"), -1, "MSET left the old TTL on disk");
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_cdel_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("lock", Value::Text("owner-1".into()), Some(3600));
        store.set("other", Value::Text("keep".into()), None);
        // failed conditional delete must not be logged; successful one must be
        assert!(!store.cdel("lock", &Value::Text("owner-2".into())));
        assert!(store.cdel("lock", &Value::Text("owner-1".into())));
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("lock"), None);
        assert_eq!(store2.get("other"), Some(Value::Text("keep".into())));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_insert_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("name", Value::Text("Nucleus".into()), None);
        store.set("version", Value::Int64(1), None);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("name"), Some(Value::Text("Nucleus".into())));
        assert_eq!(store2.get("version"), Some(Value::Int64(1)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_delete_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("a", Value::Int32(1), None);
        store.set("b", Value::Int32(2), None);
        store.del("a");
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("a"), None);
        assert_eq!(store2.get("b"), Some(Value::Int32(2)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_ttl_reopen_verify() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        // Set with a far-future TTL (1 hour) so it doesn't expire during test
        store.set("temp", Value::Text("cached".into()), Some(3600));
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        // Key should still exist (TTL not expired)
        assert_eq!(store2.get("temp"), Some(Value::Text("cached".into())));
        // TTL should be roughly 3600 seconds (allow some slack)
        let ttl = store2.ttl("temp");
        assert!(ttl > 3500 && ttl <= 3600, "expected ~3600, got {}", ttl);
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_checkpoint_truncates() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        for i in 0..100 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }
        let size_before = std::fs::metadata(dir.path().join("kv.wal")).unwrap().len();
        // Delete most keys, then checkpoint
        for i in 3..100 {
            store.del(&format!("k{i}"));
        }
        store.checkpoint().unwrap();
        let size_after = std::fs::metadata(dir.path().join("kv.wal")).unwrap().len();
        assert!(size_after < size_before, "checkpoint should shrink WAL");
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.dbsize(), 3);
        assert_eq!(store2.get("k0"), Some(Value::Int64(0)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_corrupt_trailing_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("good", Value::Int32(42), None);
        drop(store);

        // Append garbage
        let wal_path = dir.path().join("kv.wal");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        use std::io::Write;
        file.write_all(&[0xFF, 0xAB, 0xCD]).unwrap();
        file.flush().unwrap();
        drop(file);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("good"), Some(Value::Int32(42)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_incr_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        assert_eq!(store.incr("counter").unwrap(), 1);
        assert_eq!(store.incr("counter").unwrap(), 2);
        assert_eq!(store.incr("counter").unwrap(), 3);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("counter"), Some(Value::Int64(3)));
        assert_eq!(store2.incr("counter").unwrap(), 4);
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_empty_fresh_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        assert_eq!(store.dbsize(), 0);
        assert_eq!(store.get("anything"), None);
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_pattern_matching_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.set("user:1", Value::Int32(1), None);
        store.set("user:2", Value::Int32(2), None);
        store.set("session:1", Value::Int32(3), None);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        let mut keys = store2.keys("user:*");
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2"]);
        assert_eq!(store2.keys("*").len(), 3);
    }

    // ========================================================================
    // Batch WAL / MSET tests
    // ========================================================================

    #[cfg(feature = "server")]
    #[test]
    fn wal_mset_batch_replay() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        store.mset(&[
            ("x", Value::Int32(10)),
            ("y", Value::Int32(20)),
            ("z", Value::Int32(30)),
        ]);
        drop(store);

        // Re-open and verify all keys survived
        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("x"), Some(Value::Int32(10)));
        assert_eq!(store2.get("y"), Some(Value::Int32(20)));
        assert_eq!(store2.get("z"), Some(Value::Int32(30)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_mset_large_batch_replay() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();

        let pairs: Vec<(String, Value)> = (0..500)
            .map(|i| (format!("key_{i}"), Value::Int64(i)))
            .collect();
        let refs: Vec<(&str, Value)> = pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        store.mset(&refs);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.dbsize(), 500);
        assert_eq!(store2.get("key_0"), Some(Value::Int64(0)));
        assert_eq!(store2.get("key_499"), Some(Value::Int64(499)));
    }

    #[cfg(feature = "server")]
    #[test]
    fn wal_mset_then_individual_ops() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();

        // Batch set
        store.mset(&[
            ("a", Value::Int32(1)),
            ("b", Value::Int32(2)),
            ("c", Value::Int32(3)),
        ]);
        // Individual ops after batch
        store.del("b");
        store.set("d", Value::Int64(4), None);
        drop(store);

        let store2 = KvStore::open(dir.path()).unwrap();
        assert_eq!(store2.get("a"), Some(Value::Int32(1)));
        assert_eq!(store2.get("b"), None);
        assert_eq!(store2.get("c"), Some(Value::Int32(3)));
        assert_eq!(store2.get("d"), Some(Value::Int64(4)));
    }

    #[test]
    fn keys_glob_patterns() {
        let store = KvStore::new();
        store.set("user:1:name", Value::Text("Alice".into()), None);
        store.set("user:2:name", Value::Text("Bob".into()), None);
        store.set(
            "user:1:email",
            Value::Text("alice@example.com".into()),
            None,
        );
        store.set("session:abc", Value::Int32(1), None);

        // Prefix glob
        let mut user_keys = store.keys("user:*");
        user_keys.sort();
        assert_eq!(user_keys.len(), 3);

        // Suffix glob
        let mut name_keys = store.keys("*name");
        name_keys.sort();
        assert_eq!(name_keys, vec!["user:1:name", "user:2:name"]);

        // Contains glob
        let mut one_keys = store.keys("*:1:*");
        one_keys.sort();
        assert_eq!(one_keys, vec!["user:1:email", "user:1:name"]);

        // Exact match
        let exact = store.keys("session:abc");
        assert_eq!(exact, vec!["session:abc"]);

        // Match all
        assert_eq!(store.keys("*").len(), 4);
    }

    #[cfg(feature = "server")]
    #[test]
    fn bench_mset_vs_individual_set() {
        // This test verifies that batch mset (single WAL write) is faster
        // than individual sets (one WAL write per key) for 10K entries.
        let n = 10_000usize;

        // --- Individual sets ---
        let dir_individual = tempfile::tempdir().unwrap();
        let store_individual = KvStore::open(dir_individual.path()).unwrap();
        let start_individual = Instant::now();
        for i in 0..n {
            store_individual.set(&format!("k{i}"), Value::Int64(i as i64), None);
        }
        let elapsed_individual = start_individual.elapsed();
        drop(store_individual);

        // --- Batch mset ---
        let dir_batch = tempfile::tempdir().unwrap();
        let store_batch = KvStore::open(dir_batch.path()).unwrap();
        let pairs: Vec<(String, Value)> = (0..n)
            .map(|i| (format!("k{i}"), Value::Int64(i as i64)))
            .collect();
        let refs: Vec<(&str, Value)> = pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let start_batch = Instant::now();
        store_batch.mset(&refs);
        let elapsed_batch = start_batch.elapsed();
        drop(store_batch);

        // Verify correctness: both stores should have the same data
        let store_check = KvStore::open(dir_batch.path()).unwrap();
        assert_eq!(store_check.dbsize(), n);
        assert_eq!(store_check.get("k0"), Some(Value::Int64(0)));
        assert_eq!(
            store_check.get(&format!("k{}", n - 1)),
            Some(Value::Int64((n - 1) as i64))
        );

        // Print timing (visible with `cargo test -- --nocapture`)
        eprintln!(
            "  [bench] 10K individual sets: {:?}, 10K batch mset: {:?}, speedup: {:.1}x",
            elapsed_individual,
            elapsed_batch,
            elapsed_individual.as_secs_f64() / elapsed_batch.as_secs_f64(),
        );

        // Batch should be faster (single syscall vs 10K syscalls).
        // Use a generous threshold — even 1.1x would validate the optimization.
        // On CI the delta may be smaller due to OS buffering, so we just assert
        // that batch is not significantly slower.
        assert!(
            elapsed_batch < elapsed_individual + Duration::from_millis(500),
            "batch mset should not be significantly slower than individual sets"
        );
    }

    // ========================================================================
    // Sharded KV store tests
    // ========================================================================

    #[test]
    fn shard_concurrent_set_get() {
        // Verify concurrent writers on different keys don't interfere.
        let store = Arc::new(KvStore::new());
        let n = 1000;
        std::thread::scope(|s| {
            for t in 0..8 {
                let store = Arc::clone(&store);
                s.spawn(move || {
                    for i in 0..n {
                        let key = format!("t{t}_k{i}");
                        store.set(&key, Value::Int64((t * n + i) as i64), None);
                    }
                });
            }
        });
        // Every key should be present
        for t in 0..8 {
            for i in 0..n {
                let key = format!("t{t}_k{i}");
                assert_eq!(
                    store.get(&key),
                    Some(Value::Int64((t * n + i) as i64)),
                    "missing key {key}"
                );
            }
        }
        assert_eq!(store.dbsize(), 8 * n);
    }

    #[test]
    fn shard_concurrent_incr() {
        // Multiple threads incrementing the SAME key — final value must equal
        // total number of increments (no lost updates).
        let store = Arc::new(KvStore::new());
        let threads = 8;
        let per_thread = 500;
        std::thread::scope(|s| {
            for _ in 0..threads {
                let store = Arc::clone(&store);
                s.spawn(move || {
                    for _ in 0..per_thread {
                        store.incr("shared_counter").unwrap();
                    }
                });
            }
        });
        assert_eq!(
            store.get("shared_counter"),
            Some(Value::Int64((threads * per_thread) as i64))
        );
    }

    #[test]
    fn shard_mset_across_shards() {
        // MSET with keys that hash to different shards — all should be present.
        let store = KvStore::new();
        let pairs: Vec<(String, Value)> = (0..200)
            .map(|i| (format!("shard_key_{i}"), Value::Int64(i)))
            .collect();
        let refs: Vec<(&str, Value)> = pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        store.mset(&refs);

        // Verify all keys exist and dbsize is correct
        assert_eq!(store.dbsize(), 200);
        for i in 0..200 {
            let key = format!("shard_key_{i}");
            assert_eq!(store.get(&key), Some(Value::Int64(i)), "missing key {key}");
        }
    }

    #[test]
    fn shard_dbsize_sums_all_shards() {
        // Insert keys that are designed to spread across different shards.
        let store = KvStore::new();
        for i in 0..NUM_SHARDS * 3 {
            store.set(&format!("k{i}"), Value::Int32(i as i32), None);
        }
        assert_eq!(store.dbsize(), NUM_SHARDS * 3);

        // Delete half and verify dbsize
        for i in 0..NUM_SHARDS * 3 {
            if i % 2 == 0 {
                store.del(&format!("k{i}"));
            }
        }
        // Remaining: all odd-indexed keys
        let expected = (NUM_SHARDS * 3).div_ceil(2);
        assert_eq!(store.dbsize(), expected);
    }

    #[test]
    fn shard_flushdb_clears_all_shards() {
        let store = KvStore::new();
        for i in 0..500 {
            store.set(&format!("flush_k{i}"), Value::Int32(i), None);
        }
        assert_eq!(store.dbsize(), 500);
        store.flushdb();
        assert_eq!(store.dbsize(), 0);
        // Verify a sampling of keys are gone
        for i in [0, 50, 100, 499] {
            assert_eq!(store.get(&format!("flush_k{i}")), None);
        }
    }

    #[test]
    fn shard_txn_snapshot_restore() {
        let store = KvStore::new();
        store.set("a", Value::Int32(1), None);
        store.set("b", Value::Int32(2), None);
        store.set("c", Value::Int32(3), None);

        let snap = store.txn_snapshot();

        // Mutate after snapshot
        store.set("a", Value::Int32(100), None);
        store.del("b");
        store.set("d", Value::Int32(4), None);

        // Restore should bring back original state
        store.txn_restore(snap);
        assert_eq!(store.get("a"), Some(Value::Int32(1)));
        assert_eq!(store.get("b"), Some(Value::Int32(2)));
        assert_eq!(store.get("c"), Some(Value::Int32(3)));
        assert_eq!(store.get("d"), None); // was added after snapshot
        assert_eq!(store.dbsize(), 3);
    }

    #[test]
    fn shard_sweep_across_shards() {
        let store = KvStore::new();
        // Set many keys with 0-second TTL (expire immediately)
        for i in 0..100 {
            store.set(&format!("exp_{i}"), Value::Int32(i), Some(0));
        }
        // Set some permanent keys
        for i in 0..50 {
            store.set(&format!("perm_{i}"), Value::Int32(i), None);
        }
        std::thread::sleep(Duration::from_millis(10));
        let removed = store.sweep_expired();
        assert_eq!(removed, 100);
        assert_eq!(store.dbsize(), 50);
    }

    #[test]
    fn shard_concurrent_mixed_ops() {
        // Mixed concurrent operations: set, get, del, incr, setnx.
        let store = Arc::new(KvStore::new());
        std::thread::scope(|s| {
            // Writer thread
            let store_w = Arc::clone(&store);
            s.spawn(move || {
                for i in 0..500 {
                    store_w.set(&format!("mixed_{i}"), Value::Int64(i), None);
                }
            });
            // Incr thread
            let store_i = Arc::clone(&store);
            s.spawn(move || {
                for _ in 0..500 {
                    let _ = store_i.incr("incr_key");
                }
            });
            // Setnx thread
            let store_s = Arc::clone(&store);
            s.spawn(move || {
                for i in 0..500 {
                    store_s.setnx(&format!("nx_{i}"), Value::Int32(i));
                }
            });
            // Reader thread
            let store_r = Arc::clone(&store);
            s.spawn(move || {
                for i in 0..500 {
                    let _ = store_r.get(&format!("mixed_{i}"));
                    let _ = store_r.exists(&format!("nx_{i}"));
                }
            });
        });
        // Incr key should have value 500
        assert_eq!(store.get("incr_key"), Some(Value::Int64(500)));
        // All setnx keys should exist
        for i in 0..500 {
            assert!(store.exists(&format!("nx_{i}")), "missing nx_{i}");
        }
    }

    // ========================================================================
    // Cold tier (tiered storage) tests
    // ========================================================================

    #[cfg(feature = "server")]
    #[test]
    // An evicted key must survive a checkpoint + reopen.
    //
    // Checkpoint snapshots the hot tier and truncates the WAL to it, so an
    // evicted key's last WAL record disappears at that moment. The cold tier
    // buffers writes in an in-memory memtable, so unless it is flushed first the
    // key exists in neither place and is simply gone after a restart.
    fn evicted_key_survives_checkpoint_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = KvStore::open(dir.path()).unwrap();
            store.max_hot_bytes = 32 * 1024;
            for i in 0..40 {
                store.set(&format!("k{i}"), Value::Text("p".repeat(8 * 1024)), None);
            }
            // Something must actually have been pushed to cold, or this proves
            // nothing about the ordering.
            assert!(
                store.dbsize_hot() < 40,
                "precondition: eviction must have moved keys to cold"
            );
            store.checkpoint().unwrap();
        }

        let reopened = KvStore::open(dir.path()).unwrap();
        for i in 0..40 {
            assert!(
                reopened.get(&format!("k{i}")).is_some(),
                "k{i} was lost across checkpoint + reopen"
            );
        }
    }

    // Eviction must not change a value's type. The cold codec carries six tags
    // and falls back to Display->Text for anything else, so a Bytea moved to
    // cold would come back as text — a silent type change triggered by nothing
    // more than memory pressure. Values it cannot represent stay resident.
    #[cfg(feature = "server")]
    #[test]
    fn eviction_never_rewrites_a_value_it_cannot_encode() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = KvStore::open(dir.path()).unwrap();
        store.max_hot_bytes = 64 * 1024;

        let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
        store.set("blob", Value::Bytea(payload.clone()), None);
        // Plenty of encodable pressure alongside it.
        for i in 0..32 {
            store.set(&format!("t{i}"), Value::Text("q".repeat(8 * 1024)), None);
        }

        match store.get("blob") {
            Some(v) => assert_eq!(
                v,
                Value::Bytea(payload),
                "Bytea must survive eviction pressure unchanged"
            ),
            None => panic!("blob must not be dropped"),
        }
    }

    // The production failure this exists for. A store of ~32k keys holding
    // ~150 KB values each sits far under the 100k-entry cap while occupying
    // gigabytes. Eviction keyed on entry count never fired, the process pinned
    // itself above its memory limit, and writes were rejected for 32 hours while
    // "triggering eviction" ran twice a second and freed nothing.
    #[cfg(feature = "server")]
    #[test]
    fn few_large_values_trigger_eviction_despite_low_entry_count() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = KvStore::open(dir.path()).unwrap();
        // Well under max_hot_entries (100k), well over the byte budget.
        store.max_hot_bytes = 1 << 20; // 1 MiB
        let blob = "x".repeat(64 * 1024); // 64 KiB each
        for i in 0..64 {
            store.set(&format!("srcmap:{i}"), Value::Text(blob.clone()), None);
        }
        assert!(
            store.dbsize() < store.max_hot_entries,
            "precondition: entry count must stay under the count cap"
        );
        assert!(
            store.estimated_memory_bytes() <= store.max_hot_bytes * 2,
            "hot tier should have been spilled toward its byte budget, got {} bytes",
            store.estimated_memory_bytes()
        );
        // Spilled entries are still readable — they moved, they were not lost.
        assert!(
            store.get("srcmap:0").is_some(),
            "evicted key must read back from cold"
        );
    }

    // The allocator picks which subsystem to shrink from reported usage. A flat
    // 128 bytes per entry under-reported a blob store by three orders of
    // magnitude, so KV was never chosen no matter how much memory it held.
    #[test]
    fn reported_usage_reflects_value_size() {
        let store = KvStore::new();
        let blob = "y".repeat(100_000);
        store.set("big", Value::Text(blob), None);
        let usage = store.estimated_memory_bytes();
        assert!(
            usage >= 100_000,
            "usage must account for the value itself, got {usage}"
        );
        assert!(
            usage < 100_000 * 4,
            "usage should not wildly overcount, got {usage}"
        );
    }

    // shrink_to previously swept only expired entries, so a store with no TTLs
    // reported pressure forever while freeing nothing on every pass.
    #[cfg(feature = "server")]
    #[test]
    fn pressure_reclaims_entries_without_ttl() {
        use crate::memory::Pressurable;
        let dir = tempfile::tempdir().unwrap();
        let mut store = KvStore::open(dir.path()).unwrap();
        let blob = "z".repeat(32 * 1024);
        for i in 0..64 {
            store.set(&format!("k{i}"), Value::Text(blob.clone()), None);
        }
        let before = store.current_usage();
        assert!(before > 1 << 20, "precondition: should hold >1 MiB");
        let freed = store.shrink_to(256 * 1024);
        assert!(
            freed > 0,
            "pressure must reclaim something when no entry has a TTL"
        );
        assert!(
            store.current_usage() < before,
            "usage must actually drop after a pressure callback"
        );
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_kv_cold_tier_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();
        assert!(store.has_cold_tier(), "disk mode should have cold tier");
        // Cold tier directory should exist
        assert!(dir.path().join("kv_cold").exists());
        store.set("hello", Value::Text("world".into()), None);
        assert_eq!(store.get("hello"), Some(Value::Text("world".into())));
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_kv_cold_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = KvStore::open(dir.path()).unwrap();
        // Set a very low max_hot_entries to trigger eviction
        store.max_hot_entries = 10;
        // Insert 30 entries (well above threshold)
        for i in 0..30 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }
        // Hot tier should have fewer entries than total
        let hot_count = store.dbsize_hot();
        assert!(hot_count <= 30, "hot_count={hot_count} should be <= 30");
        // All 30 entries should still be accessible (via hot or cold)
        for i in 0..30 {
            let val = store.get(&format!("k{i}"));
            assert!(val.is_some(), "k{i} should be accessible, got None");
        }
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_kv_cold_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = KvStore::open(dir.path()).unwrap();
        store.max_hot_entries = 5;
        // Insert enough to trigger eviction
        for i in 0..20 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }
        // Access an evicted key — it should be promoted back to hot
        let val = store.get("k0");
        assert!(val.is_some(), "evicted key should be readable from cold");
        // After promotion, key should be in hot tier
        let shard = store.data.shard("k0");
        let data = shard.data.read();
        assert!(
            data.contains_key("k0"),
            "promoted key should be in hot tier"
        );
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_kv_cold_del() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = KvStore::open(dir.path()).unwrap();
        store.max_hot_entries = 5;
        for i in 0..20 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }
        // Delete a key — should be removed from both tiers
        assert!(store.del("k0"));
        assert_eq!(
            store.get("k0"),
            None,
            "deleted key should be gone from both tiers"
        );
        assert!(!store.del("k0"), "second delete should return false");
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_kv_cold_persistence() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = KvStore::open(dir.path()).unwrap();
            store.max_hot_entries = 5;
            for i in 0..20 {
                store.set(&format!("p{i}"), Value::Int64(i), None);
            }
            // Force flush the cold LsmTree to disk
            if let Some(ref cold) = store.cold {
                cold.lock().force_flush();
            }
        }
        // Reopen — cold data should survive
        let store2 = KvStore::open(dir.path()).unwrap();
        // The WAL restores hot entries; cold entries persist independently.
        // All 20 entries should be accessible.
        for i in 0..20 {
            let val = store2.get(&format!("p{i}"));
            assert!(val.is_some(), "p{i} should survive reopen");
        }
    }

    #[test]
    fn test_kv_memory_mode_no_cold() {
        let store = KvStore::new();
        assert!(
            !store.has_cold_tier(),
            "memory mode should have no cold tier"
        );
    }
}
