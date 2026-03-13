//! Tiered cache: always-present in-process L1, with optional L2 (Redis) stub.
//!
//! Usage (L1-only):
//!
//! ```rust,no_run
//! use neutron_cache::TieredCache;
//!
//! let cache = TieredCache::l1_only(1024);
//! cache.set("key", b"value".to_vec(), 60);
//! assert_eq!(cache.get("key"), Some(b"value".to_vec()));
//! ```

use crate::l1::L1Cache;

/// Two-tier cache: L1 (in-process HashMap) always present;
/// L2 (Redis) optional and stubbed until the  feature is enabled.
#[derive(Clone)]
pub struct TieredCache {
    l1: L1Cache,
    // l2: Option<RedisClient> -- reserved for the  feature
}

impl TieredCache {
    /// Create a TieredCache backed only by the in-process L1 store.
    pub fn l1_only(max_capacity: usize) -> Self {
        Self {
            l1: L1Cache::new(max_capacity),
        }
    }

    /// Return the raw bytes stored for .
    ///
    /// Checks L1 first.  On a miss, would check L2 and populate L1 (when L2
    /// is enabled).  Currently returns  on miss.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(v) = self.l1.get(key) {
            return Some(v);
        }
        // L2 stub: would query Redis here and call self.l1.set(...)
        None
    }

    /// Store  under  with the given TTL in seconds.
    ///
    /// Writes to L1 (and L2 when enabled).
    pub fn set(&self, key: &str, value: Vec<u8>, ttl_secs: u64) {
        self.l1.set(key, value, ttl_secs);
        // L2 stub: would write to Redis here
    }

    /// Remove the entry for  from all cache tiers.
    pub fn set_with_expiry(&self, key: &str, value: Vec<u8>, expires_at: std::time::Instant) {
        self.l1.set_with_expiry(key, value, expires_at);
    }

    pub fn del(&self, key: &str) {
        self.l1.del(key);
        // L2 stub: would delete from Redis here
    }

    /// Scan L1 and delete every key that starts with .
    ///
    /// Note: L2 invalidation (pattern-delete) would be performed here when L2
    /// is enabled.
    pub fn invalidate_prefix(&self, prefix: &str) {
        for key in self.l1.keys_with_prefix(prefix) {
            self.l1.del(&key);
        }
        // L2 stub: would SCAN + DEL matching Redis keys here
    }

    /// Return the number of entries currently held in L1.
    pub fn l1_len(&self) -> usize {
        self.l1.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn tc() -> TieredCache { TieredCache::l1_only(64) }

    #[test]
    fn get_returns_none_on_empty_cache() {
        let c = tc();
        assert!(c.get("missing").is_none());
    }

    #[test]
    fn set_then_get_returns_value() {
        let c = tc();
        c.set("hello", b"world".to_vec(), 60);
        assert_eq!(c.get("hello"), Some(b"world".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_key() {
        let c = tc();
        c.set("k", b"old".to_vec(), 60);
        c.set("k", b"new".to_vec(), 60);
        assert_eq!(c.get("k"), Some(b"new".to_vec()));
    }

    #[test]
    fn del_removes_key() {
        let c = tc();
        c.set("k", b"v".to_vec(), 60);
        c.del("k");
        assert!(c.get("k").is_none());
    }

    #[test]
    fn del_nonexistent_key_is_noop() {
        let c = tc();
        c.del("ghost");
        assert_eq!(c.l1_len(), 0);
    }

    #[test]
    fn expired_entry_not_returned() {
        let c = tc();
        let past = Instant::now() - Duration::from_secs(2);
        c.set_with_expiry("stale", b"data".to_vec(), past);
        assert!(c.get("stale").is_none());
    }

    #[test]
    fn active_entry_returned_after_set_with_expiry() {
        let c = tc();
        let future = Instant::now() + Duration::from_secs(3600);
        c.set_with_expiry("live", b"fresh".to_vec(), future);
        assert_eq!(c.get("live"), Some(b"fresh".to_vec()));
    }

    #[test]
    fn invalidate_prefix_removes_matching_keys() {
        let c = tc();
        c.set("session:abc", b"1".to_vec(), 60);
        c.set("session:xyz", b"2".to_vec(), 60);
        c.set("user:1",      b"3".to_vec(), 60);
        c.invalidate_prefix("session:");
        assert!(c.get("session:abc").is_none());
        assert!(c.get("session:xyz").is_none());
        assert_eq!(c.get("user:1"), Some(b"3".to_vec()));
    }

    #[test]
    fn invalidate_prefix_no_match_leaves_cache_intact() {
        let c = tc();
        c.set("foo:1", b"a".to_vec(), 60);
        c.invalidate_prefix("bar:");
        assert_eq!(c.get("foo:1"), Some(b"a".to_vec()));
    }

    #[test]
    fn invalidate_prefix_empty_prefix_removes_all() {
        let c = tc();
        c.set("a", b"1".to_vec(), 60);
        c.set("b", b"2".to_vec(), 60);
        c.invalidate_prefix("");
        assert_eq!(c.l1_len(), 0);
    }

    #[test]
    fn l1_len_tracks_entries() {
        let c = tc();
        assert_eq!(c.l1_len(), 0);
        c.set("x", b"1".to_vec(), 60);
        assert_eq!(c.l1_len(), 1);
        c.set("y", b"2".to_vec(), 60);
        assert_eq!(c.l1_len(), 2);
        c.del("x");
        assert_eq!(c.l1_len(), 1);
    }

    #[test]
    fn clone_shares_state() {
        let c1 = tc();
        let c2 = c1.clone();
        c1.set("shared", b"data".to_vec(), 60);
        assert_eq!(c2.get("shared"), Some(b"data".to_vec()));
    }

    #[test]
    fn multiple_data_types_stored_as_bytes() {
        let c = tc();
        let json = br#"{"count":42}"#.to_vec();
        c.set("json_key", json.clone(), 60);
        assert_eq!(c.get("json_key"), Some(json));
    }
}
