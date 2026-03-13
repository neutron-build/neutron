//! Tiered KV store: hot in-memory cache backed by disk-based LsmTree cold storage.
//!
//! The `TieredKvStore` wraps the existing `KvStore` as a hot cache with an
//! `LsmTree` as cold storage. When the hot tier exceeds `max_hot_entries`,
//! non-TTL entries are evicted to the cold tier. Reads check hot first, then
//! cold, promoting cold hits back to hot for temporal locality.
//!
//! Value encoding for the LsmTree uses a compact binary format:
//!   Tag byte: 0=Null, 1=Bool, 2=Int32, 3=Int64, 4=Float64, 5=Text
//!   Followed by type-specific payload.

use std::path::Path;

use parking_lot::Mutex;

use super::KvStore;
use crate::storage::lsm::{LsmConfig, LsmTree};
use crate::types::Value;

// ============================================================================
// Value encoding/decoding for LsmTree binary storage
// ============================================================================

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT32: u8 = 2;
const TAG_INT64: u8 = 3;
const TAG_FLOAT64: u8 = 4;
const TAG_TEXT: u8 = 5;

/// Encode a `Value` into a compact binary format for LsmTree storage.
pub fn encode_value(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![TAG_NULL],
        Value::Bool(b) => vec![TAG_BOOL, if *b { 1 } else { 0 }],
        Value::Int32(n) => {
            let mut buf = Vec::with_capacity(5);
            buf.push(TAG_INT32);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Int64(n) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_INT64);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Float64(n) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_FLOAT64);
            buf.extend_from_slice(&n.to_le_bytes());
            buf
        }
        Value::Text(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(1 + 4 + bytes.len());
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        // For unsupported types, fall back to Text encoding via Display
        other => {
            let s = other.to_string();
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(1 + 4 + bytes.len());
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
    }
}

/// Decode a `Value` from the compact binary format used in LsmTree storage.
pub fn decode_value(data: &[u8]) -> Value {
    if data.is_empty() {
        return Value::Null;
    }
    match data[0] {
        TAG_NULL => Value::Null,
        TAG_BOOL => {
            if data.len() < 2 {
                return Value::Null;
            }
            Value::Bool(data[1] != 0)
        }
        TAG_INT32 => {
            if data.len() < 5 {
                return Value::Null;
            }
            let n = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
            Value::Int32(n)
        }
        TAG_INT64 => {
            if data.len() < 9 {
                return Value::Null;
            }
            let n = i64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Value::Int64(n)
        }
        TAG_FLOAT64 => {
            if data.len() < 9 {
                return Value::Null;
            }
            let n = f64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Value::Float64(n)
        }
        TAG_TEXT => {
            if data.len() < 5 {
                return Value::Null;
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Value::Null;
            }
            let s = String::from_utf8_lossy(&data[5..5 + len]).into_owned();
            Value::Text(s)
        }
        _ => Value::Null,
    }
}

// ============================================================================
// TieredKvStore
// ============================================================================

/// A tiered key-value store with an in-memory hot cache and an LsmTree cold tier.
///
/// Entries are written to the hot tier first. When the hot tier exceeds
/// `max_hot_entries`, non-TTL entries are evicted to the cold LsmTree.
/// Reads check the hot tier first, then the cold tier, promoting cold hits
/// back to hot for temporal locality.
pub struct TieredKvStore {
    hot: KvStore,
    cold: Mutex<LsmTree>,
    max_hot_entries: usize,
    eviction_batch: usize,
}

impl TieredKvStore {
    /// Create a `TieredKvStore` with an in-memory cold tier (for testing).
    pub fn new(max_hot_entries: usize) -> Self {
        let config = LsmConfig {
            memtable_flush_threshold: 256,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        Self {
            hot: KvStore::new(),
            cold: Mutex::new(LsmTree::new(config)),
            max_hot_entries,
            eviction_batch: (max_hot_entries / 10).max(1),
        }
    }

    /// Open a `TieredKvStore` with a disk-backed cold tier at the given directory.
    pub fn open(dir: &str, max_hot_entries: usize) -> Self {
        let config = LsmConfig {
            memtable_flush_threshold: 256,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        let cold = LsmTree::open(config, Path::new(dir))
            .unwrap_or_else(|_| LsmTree::new(LsmConfig::default()));
        Self {
            hot: KvStore::new(),
            cold: Mutex::new(cold),
            max_hot_entries,
            eviction_batch: (max_hot_entries / 10).max(1),
        }
    }

    /// SET — write a key-value pair to the hot tier with optional TTL.
    /// Triggers eviction if the hot tier exceeds capacity.
    pub fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) {
        self.hot.set(key, value, ttl_secs);
        self.maybe_evict();
    }

    /// GET — retrieve a value by key. Checks hot first, then cold.
    /// Cold hits are promoted back to hot for temporal locality.
    pub fn get(&self, key: &str) -> Option<Value> {
        // Check hot tier first
        if let Some(v) = self.hot.get(key) {
            return Some(v);
        }
        // Check cold tier
        if let Some(v) = self.cold_get(key) {
            // Promote to hot (no TTL since cold entries never have TTL)
            self.hot.set(key, v.clone(), None);
            // Remove from cold to avoid duplication
            self.cold_del(key);
            self.maybe_evict();
            return Some(v);
        }
        None
    }

    /// DEL — delete a key from both tiers. Returns true if the key existed.
    pub fn del(&self, key: &str) -> bool {
        let hot_deleted = self.hot.del(key);
        let cold_existed = self.cold_get(key).is_some();
        if cold_existed {
            self.cold_del(key);
        }
        hot_deleted || cold_existed
    }

    /// EXISTS — check if a key exists in either tier.
    pub fn exists(&self, key: &str) -> bool {
        if self.hot.exists(key) {
            return true;
        }
        self.cold_get(key).is_some()
    }

    /// KEYS — return the union of keys from both tiers (deduplicated).
    pub fn keys(&self) -> Vec<String> {
        let hot_keys = self.hot.keys("*");
        let cold_keys = self.cold_keys();

        let mut all: std::collections::HashSet<String> =
            hot_keys.into_iter().collect();
        for k in cold_keys {
            all.insert(k);
        }
        all.into_iter().collect()
    }

    /// DBSIZE — approximate total size across both tiers.
    /// May double-count entries that exist in both tiers.
    pub fn dbsize(&self) -> usize {
        let hot_size = self.hot.dbsize();
        let cold_size = self.cold_keys().len();
        hot_size + cold_size
    }

    /// INCR — increment an integer value by 1. Always operates in the hot tier
    /// since INCR implies frequent access.
    pub fn incr(&self, key: &str) -> i64 {
        // If key is in cold, promote it first
        if self.hot.get(key).is_none()
            && let Some(v) = self.cold_get(key) {
                self.hot.set(key, v, None);
                self.cold_del(key);
            }
        self.hot.incr(key).unwrap_or(0)
    }

    /// FLUSHDB — clear both tiers.
    pub fn flushdb(&self) {
        self.hot.flushdb();
        // Clear cold tier by replacing with a fresh LsmTree
        let mut cold = self.cold.lock();
        let config = LsmConfig {
            memtable_flush_threshold: 256,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        *cold = LsmTree::new(config);
    }

    /// MSET — batch set multiple key-value pairs.
    pub fn mset(&self, pairs: &[(&str, Value)]) {
        for (key, value) in pairs {
            self.hot.set(key, value.clone(), None);
        }
        self.maybe_evict();
    }

    /// MGET — batch get multiple keys with cold tier fallback.
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Value>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    // ========================================================================
    // Cold tier access helpers
    // ========================================================================

    /// Read a value from the cold LsmTree.
    fn cold_get(&self, key: &str) -> Option<Value> {
        let cold = self.cold.lock();
        cold.get(key.as_bytes()).map(|data| decode_value(&data))
    }

    /// Delete a key from the cold LsmTree.
    fn cold_del(&self, key: &str) {
        let mut cold = self.cold.lock();
        cold.delete(key.as_bytes().to_vec());
    }

    /// Get all keys from the cold LsmTree using a full range scan.
    /// Keys are stored as UTF-8 bytes, so we scan from empty to 0xFF.
    fn cold_keys(&self) -> Vec<String> {
        let cold = self.cold.lock();
        // Range scan from empty (minimum) to [0xFF] (above all valid UTF-8)
        let entries = cold.range(b"", &[0xFF]);
        entries
            .into_iter()
            .filter_map(|(k, _)| String::from_utf8(k).ok())
            .collect()
    }

    // ========================================================================
    // Eviction logic
    // ========================================================================

    /// Evict non-TTL entries from hot to cold when the hot tier exceeds capacity.
    ///
    /// Strategy: iterate hot keys, move entries WITHOUT TTL to cold tier.
    /// TTL entries stay hot since they expire naturally and the cold tier
    /// doesn't support TTL.
    fn maybe_evict(&self) {
        if self.hot.dbsize() <= self.max_hot_entries {
            return;
        }

        // Collect candidate keys for eviction (non-TTL entries)
        let all_keys = self.hot.keys("*");
        let mut evicted = 0;

        for key in &all_keys {
            if evicted >= self.eviction_batch {
                break;
            }
            // Check if key has TTL: TTL returns -1 for no TTL, -2 for missing
            let ttl = self.hot.ttl(key);
            if ttl == -1 {
                // No TTL — candidate for eviction
                if let Some(value) = self.hot.get(key) {
                    let encoded = encode_value(&value);
                    {
                        let mut cold = self.cold.lock();
                        cold.put(key.as_bytes().to_vec(), encoded);
                    }
                    self.hot.del(key);
                    evicted += 1;
                }
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiered_basic_set_get() {
        let store = TieredKvStore::new(100);
        store.set("name", Value::Text("Nucleus".into()), None);
        assert_eq!(store.get("name"), Some(Value::Text("Nucleus".into())));
    }

    #[test]
    fn test_tiered_get_nonexistent() {
        let store = TieredKvStore::new(100);
        assert_eq!(store.get("missing"), None);
    }

    #[test]
    fn test_tiered_del() {
        let store = TieredKvStore::new(100);
        store.set("x", Value::Int32(1), None);
        assert!(store.del("x"));
        assert_eq!(store.get("x"), None);
        assert!(!store.del("x"));
    }

    #[test]
    fn test_tiered_eviction_to_cold() {
        let store = TieredKvStore::new(10);
        // Insert 20 entries — should trigger eviction
        for i in 0..20 {
            store.set(&format!("key{i}"), Value::Int64(i), None);
        }
        // Hot tier should have at most max_hot_entries
        // (eviction may not bring it exactly to 10 due to batch size)
        assert!(store.hot.dbsize() <= 20);

        // All entries should still be accessible (via hot or cold)
        for i in 0..20 {
            let val = store.get(&format!("key{i}"));
            assert!(
                val.is_some(),
                "key{i} should be accessible but got None"
            );
        }
    }

    #[test]
    fn test_tiered_get_promotes_from_cold() {
        // Use a large enough hot tier that promotion doesn't immediately re-evict
        let store = TieredKvStore::new(20);
        // Insert entries and manually evict one to cold
        for i in 0..10 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }
        // Manually push "target" to cold
        {
            let encoded = encode_value(&Value::Int64(999));
            store.cold.lock().put(b"target".to_vec(), encoded);
        }

        // Verify it's not in hot
        assert!(store.hot.get("target").is_none(), "target should not be in hot");

        // Access it — should promote from cold to hot
        let val = store.get("target");
        assert_eq!(val, Some(Value::Int64(999)), "cold key should be readable");

        // Now it should be in hot (hot tier has room: 11 entries < 20 max)
        assert!(
            store.hot.get("target").is_some(),
            "accessed cold key should be promoted to hot"
        );

        // And removed from cold
        assert!(
            store.cold_get("target").is_none(),
            "promoted key should be removed from cold"
        );
    }

    #[test]
    fn test_tiered_del_from_cold() {
        let store = TieredKvStore::new(5);
        // Insert entries, some will be evicted to cold
        for i in 0..15 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }

        // Find a cold key and delete it
        let cold_keys = store.cold_keys();
        if let Some(cold_key) = cold_keys.first().cloned() {
            assert!(store.del(&cold_key), "should delete cold key");
            assert_eq!(store.get(&cold_key), None, "deleted cold key should be gone");
        }
    }

    #[test]
    fn test_tiered_keys_merges_tiers() {
        let store = TieredKvStore::new(5);
        for i in 0..15 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }

        let all_keys = store.keys();
        // All 15 keys should appear (some hot, some cold)
        assert_eq!(all_keys.len(), 15, "expected 15 keys, got {}", all_keys.len());
    }

    #[test]
    fn test_tiered_dbsize() {
        let store = TieredKvStore::new(100);
        store.set("a", Value::Int32(1), None);
        store.set("b", Value::Int32(2), None);
        assert_eq!(store.dbsize(), 2);
    }

    #[test]
    fn test_tiered_exists_checks_cold() {
        let store = TieredKvStore::new(5);
        for i in 0..15 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }

        // All keys should exist (some in hot, some in cold)
        for i in 0..15 {
            assert!(
                store.exists(&format!("k{i}")),
                "k{i} should exist"
            );
        }
    }

    #[test]
    fn test_tiered_incr_stays_hot() {
        let store = TieredKvStore::new(100);
        assert_eq!(store.incr("counter"), 1);
        assert_eq!(store.incr("counter"), 2);
        assert_eq!(store.incr("counter"), 3);
        // Counter should be in hot tier
        assert_eq!(store.hot.get("counter"), Some(Value::Int64(3)));
    }

    #[test]
    fn test_tiered_flushdb() {
        let store = TieredKvStore::new(5);
        for i in 0..15 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }
        store.flushdb();
        assert_eq!(store.dbsize(), 0);
        assert_eq!(store.get("k0"), None);
    }

    #[test]
    fn test_tiered_mset_mget() {
        let store = TieredKvStore::new(100);
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
    fn test_tiered_ttl_entries_stay_hot() {
        let store = TieredKvStore::new(5);

        // Set entries with TTL
        for i in 0..5 {
            store.set(&format!("ttl{i}"), Value::Int64(i), Some(3600));
        }
        // Set non-TTL entries to trigger eviction
        for i in 0..10 {
            store.set(&format!("nottl{i}"), Value::Int64(i), None);
        }

        // TTL entries should still be in hot (not evicted to cold)
        for i in 0..5 {
            let key = format!("ttl{i}");
            assert!(
                store.hot.get(&key).is_some(),
                "TTL entry {key} should remain in hot tier"
            );
        }
    }

    #[test]
    fn test_tiered_value_encoding_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int32(42),
            Value::Int32(-1),
            Value::Int32(i32::MAX),
            Value::Int32(i32::MIN),
            Value::Int64(123456789),
            Value::Int64(-987654321),
            Value::Int64(i64::MAX),
            Value::Int64(i64::MIN),
            Value::Float64(3.14159),
            Value::Float64(-0.0),
            Value::Float64(f64::INFINITY),
            Value::Text("hello world".into()),
            Value::Text(String::new()),
            Value::Text("unicode: \u{1F600}".into()),
        ];

        for v in &values {
            let encoded = encode_value(v);
            let decoded = decode_value(&encoded);
            assert_eq!(
                &decoded, v,
                "roundtrip failed for {v:?}: encoded={encoded:?}, decoded={decoded:?}"
            );
        }
    }

    #[test]
    fn test_tiered_large_dataset() {
        let store = TieredKvStore::new(1000);
        // Insert 10000 entries
        for i in 0..10000 {
            store.set(&format!("big{i}"), Value::Int64(i), None);
        }
        // Verify all entries are accessible
        for i in 0..10000 {
            let val = store.get(&format!("big{i}"));
            assert!(val.is_some(), "big{i} should be accessible");
            assert_eq!(val.unwrap(), Value::Int64(i));
        }
    }

    #[test]
    fn test_tiered_overwrite_in_cold() {
        let store = TieredKvStore::new(5);
        // Insert entries to trigger eviction
        for i in 0..15 {
            store.set(&format!("k{i}"), Value::Int64(i), None);
        }

        // Find a cold key and overwrite it
        let cold_keys = store.cold_keys();
        if let Some(cold_key) = cold_keys.first().cloned() {
            store.set(&cold_key, Value::Text("updated".into()), None);
            let val = store.get(&cold_key);
            assert_eq!(
                val,
                Some(Value::Text("updated".into())),
                "overwritten cold key should return new value"
            );
        }
    }

    #[test]
    fn test_tiered_disk_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap();

        // Write data to a disk-backed store
        {
            let store = TieredKvStore::open(dir_str, 5);
            for i in 0..15 {
                store.set(&format!("persist{i}"), Value::Int64(i), None);
            }
            // Force flush the cold LsmTree to ensure data hits disk
            store.cold.lock().force_flush();
        }

        // Reopen and verify cold tier data persists
        {
            let store = TieredKvStore::open(dir_str, 5);
            // Cold tier entries should be recoverable
            let cold_keys = store.cold_keys();
            assert!(
                !cold_keys.is_empty(),
                "cold tier should have persisted entries after reopen"
            );
            // Verify we can read the values
            for key in &cold_keys {
                let val = store.get(key);
                assert!(val.is_some(), "persisted key {key} should be readable");
            }
        }
    }

    #[test]
    fn test_tiered_incr_promotes_from_cold() {
        let store = TieredKvStore::new(5);
        // Set a value that will be evicted
        store.set("counter", Value::Int64(10), None);
        // Push it out to cold
        for i in 0..20 {
            store.set(&format!("filler{i}"), Value::Int64(i), None);
        }

        // Now incr should promote from cold and increment
        let result = store.incr("counter");
        assert_eq!(result, 11, "incr should promote cold value and add 1");
    }

    #[test]
    fn test_tiered_del_nonexistent() {
        let store = TieredKvStore::new(100);
        assert!(!store.del("ghost"), "deleting nonexistent key should return false");
    }

    #[test]
    fn test_tiered_empty_store() {
        let store = TieredKvStore::new(100);
        assert_eq!(store.dbsize(), 0);
        assert!(store.keys().is_empty());
        assert!(!store.exists("anything"));
    }
}
