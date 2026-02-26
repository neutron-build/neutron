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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::types::Value;

// ============================================================================
// KV Entry
// ============================================================================

#[derive(Debug, Clone)]
struct KvEntry {
    value: Value,
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
// KV Store
// ============================================================================

/// In-memory key-value store with TTL support.
#[derive(Debug)]
pub struct KvStore {
    data: RwLock<HashMap<String, KvEntry>>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// GET — retrieve a value by key. Returns None if key doesn't exist or is expired.
    pub fn get(&self, key: &str) -> Option<Value> {
        let data = self.data.read();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => Some(entry.value.clone()),
            Some(_) => {
                drop(data);
                // Lazy expiration: remove expired key
                self.data.write().remove(key);
                None
            }
            None => None,
        }
    }

    /// SET — store a value with optional TTL in seconds.
    pub fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) {
        let expires_at = ttl_secs.map(|s| Instant::now() + Duration::from_secs(s));
        self.data.write().insert(
            key.to_string(),
            KvEntry { value, expires_at },
        );
    }

    /// DEL — remove a key. Returns true if the key existed.
    pub fn del(&self, key: &str) -> bool {
        self.data.write().remove(key).is_some()
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
        let mut data = self.data.write();
        let entry = data.get(key);

        let current = match entry {
            None => 0,
            Some(e) if e.is_expired() => 0,
            Some(e) => match &e.value {
                Value::Int32(n) => *n as i64,
                Value::Int64(n) => *n,
                _ => return Err(KvError::NotAnInteger),
            },
        };

        let new_val = current + amount;
        let ttl = entry.and_then(|e| e.expires_at);
        data.insert(
            key.to_string(),
            KvEntry {
                value: Value::Int64(new_val),
                expires_at: ttl,
            },
        );

        Ok(new_val)
    }

    /// EXPIRE — set a TTL on an existing key. Returns false if key doesn't exist.
    pub fn expire(&self, key: &str, ttl_secs: u64) -> bool {
        let mut data = self.data.write();
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                data.remove(key);
                return false;
            }
            entry.expires_at = Some(Instant::now() + Duration::from_secs(ttl_secs));
            true
        } else {
            false
        }
    }

    /// PERSIST — remove the TTL from a key (make it permanent).
    pub fn persist(&self, key: &str) -> bool {
        let mut data = self.data.write();
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                data.remove(key);
                return false;
            }
            entry.expires_at = None;
            true
        } else {
            false
        }
    }

    /// TTL — get remaining TTL in seconds. Returns -1 if no TTL, -2 if key doesn't exist.
    pub fn ttl(&self, key: &str) -> i64 {
        let data = self.data.read();
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
        let data = self.data.read();
        let now = Instant::now();
        data.iter()
            .filter(|(_, entry)| entry.expires_at.is_none_or(|t| now < t))
            .filter(|(key, _)| match_pattern(pattern, key))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// DBSIZE — return the number of non-expired keys.
    pub fn dbsize(&self) -> usize {
        let data = self.data.read();
        let now = Instant::now();
        data.values()
            .filter(|entry| entry.expires_at.is_none_or(|t| now < t))
            .count()
    }

    /// FLUSHDB — remove all keys.
    pub fn flushdb(&self) {
        self.data.write().clear();
    }

    /// Background sweep: remove expired entries. Call periodically.
    pub fn sweep_expired(&self) -> usize {
        let mut data = self.data.write();
        let before = data.len();
        data.retain(|_, entry| !entry.is_expired());
        before - data.len()
    }

    /// MGET — get multiple values at once.
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Value>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// MSET — set multiple key-value pairs at once.
    pub fn mset(&self, pairs: &[(&str, Value)]) {
        let mut data = self.data.write();
        for (key, value) in pairs {
            data.insert(
                key.to_string(),
                KvEntry {
                    value: value.clone(),
                    expires_at: None,
                },
            );
        }
    }

    /// SETNX — set only if key doesn't exist. Returns true if set, false if already exists.
    pub fn setnx(&self, key: &str, value: Value) -> bool {
        let mut data = self.data.write();
        if let Some(entry) = data.get(key) {
            if !entry.is_expired() {
                return false;
            }
        }
        data.insert(
            key.to_string(),
            KvEntry {
                value,
                expires_at: None,
            },
        );
        true
    }
}

/// Start the background TTL sweeper task.
pub fn start_sweeper(store: Arc<KvStore>, interval_secs: u64) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            let removed = store.sweep_expired();
            if removed > 0 {
                tracing::debug!("KV sweeper removed {removed} expired keys");
            }
        }
    })
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

impl SortedSet {
    pub fn new() -> Self {
        Self { tree: BTreeMap::new(), members: HashMap::new() }
    }

    /// ZADD — insert or update a member. Returns true if new.
    pub fn zadd(&mut self, member: &str, score: f64) -> bool {
        if let Some(&old_score) = self.members.get(member) {
            self.tree.remove(&(OrderedF64(old_score), member.to_string()));
            self.tree.insert((OrderedF64(score), member.to_string()), ());
            self.members.insert(member.to_string(), score);
            false
        } else {
            self.tree.insert((OrderedF64(score), member.to_string()), ());
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
    pub fn zrange(&self, start: usize, stop: usize) -> Vec<SortedSetEntry> {
        self.tree.iter()
            .skip(start)
            .take(stop.saturating_sub(start) + 1)
            .map(|((OrderedF64(s), m), _)| SortedSetEntry { member: m.clone(), score: *s })
            .collect()
    }

    /// ZREVRANGE — entries by rank descending.
    pub fn zrevrange(&self, start: usize, stop: usize) -> Vec<SortedSetEntry> {
        self.tree.iter().rev()
            .skip(start)
            .take(stop.saturating_sub(start) + 1)
            .map(|((OrderedF64(s), m), _)| SortedSetEntry { member: m.clone(), score: *s })
            .collect()
    }

    /// ZRANGEBYSCORE — entries with score in [min, max].
    pub fn zrangebyscore(&self, min: f64, max: f64) -> Vec<SortedSetEntry> {
        use std::ops::Bound;
        let lo = (OrderedF64(min), String::new());
        self.tree.range((Bound::Included(&lo), Bound::Unbounded))
            .take_while(|((OrderedF64(s), _), _)| *s <= max)
            .map(|((OrderedF64(s), m), _)| SortedSetEntry { member: m.clone(), score: *s })
            .collect()
    }

    pub fn zcard(&self) -> usize { self.members.len() }

    /// ZINCRBY — increment score. Creates member if missing.
    pub fn zincrby(&mut self, member: &str, increment: f64) -> f64 {
        let new_score = self.members.get(member).map_or(increment, |&old| old + increment);
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

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self { Self::with_precision(14) }

    pub fn with_precision(p: u8) -> Self {
        assert!((4..=18).contains(&p), "precision p must be in 4..=18");
        let m = 1usize << p;
        Self { registers: vec![0u8; m], p }
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
            if reg == 0 { zeros += 1; }
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
        assert_eq!(self.p, other.p, "cannot merge HLLs with different precisions");
        for (a, &b) in self.registers.iter_mut().zip(other.registers.iter()) {
            if b > *a { *a = b; }
        }
    }

    pub fn clear(&mut self) { self.registers.fill(0); }

    pub fn is_empty(&self) -> bool { self.registers.iter().all(|&r| r == 0) }

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
        assert!(count > 95_000 && count < 105_000, "expected ~100000, got {}", count);
    }
}
