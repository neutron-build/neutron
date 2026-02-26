//! In-process TTL cache (L1) backed by a HashMap protected by a Mutex.
//!
//! When the cache reaches max_capacity a single entry is evicted: whichever
//! entry expires_at is the smallest (soonest to expire / oldest by deadline).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone)]
struct Entry {
    value:      Vec<u8>,
    expires_at: Instant,
}

/// Cheap-to-clone, thread-safe in-process TTL cache.
#[derive(Clone)]
pub struct L1Cache {
    inner:        Arc<Mutex<HashMap<String, Entry>>>,
    max_capacity: usize,
}

impl L1Cache {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_capacity: max_capacity.max(1),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap();
        map.get(key).and_then(|e| {
            if Instant::now() < e.expires_at { Some(e.value.clone()) } else { None }
        })
    }

    pub fn set(&self, key: &str, value: Vec<u8>, ttl_secs: u64) {
        let expires_at = Instant::now() + Duration::from_secs(ttl_secs);
        let mut map = self.inner.lock().unwrap();
        if map.len() >= self.max_capacity && !map.contains_key(key) {
            self.evict_one(&mut map);
        }
        map.insert(key.to_string(), Entry { value, expires_at });
    }

    pub fn set_with_expiry(&self, key: &str, value: Vec<u8>, expires_at: Instant) {
        let mut map = self.inner.lock().unwrap();
        if map.len() >= self.max_capacity && !map.contains_key(key) {
            self.evict_one(&mut map);
        }
        map.insert(key.to_string(), Entry { value, expires_at });
    }

    pub fn del(&self, key: &str) {
        self.inner.lock().unwrap().remove(key);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.inner.lock().unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn evict_one(&self, map: &mut HashMap<String, Entry>) {
        let now = Instant::now();
        if let Some(expired_key) = map.iter()
            .find(|(_, e)| e.expires_at <= now)
            .map(|(k, _)| k.clone())
        {
            map.remove(&expired_key);
            return;
        }
        if let Some(oldest_key) = map.iter()
            .min_by_key(|(_, e)| e.expires_at)
            .map(|(k, _)| k.clone())
        {
            map.remove(&oldest_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cache(cap: usize) -> L1Cache { L1Cache::new(cap) }

    #[test]
    fn get_on_empty_cache_returns_none() {
        let c = cache(10);
        assert!(c.get("missing").is_none());
    }

    #[test]
    fn set_then_get_returns_value() {
        let c = cache(10);
        c.set("k", b"hello".to_vec(), 60);
        assert_eq!(c.get("k"), Some(b"hello".to_vec()));
    }

    #[test]
    fn set_overwrites_existing_key() {
        let c = cache(10);
        c.set("k", b"v1".to_vec(), 60);
        c.set("k", b"v2".to_vec(), 60);
        assert_eq!(c.get("k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn del_removes_existing_key() {
        let c = cache(10);
        c.set("k", b"v".to_vec(), 60);
        c.del("k");
        assert!(c.get("k").is_none());
    }

    #[test]
    fn del_nonexistent_key_is_noop() {
        let c = cache(10);
        c.del("ghost");
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn len_reflects_current_entry_count() {
        let c = cache(10);
        assert_eq!(c.len(), 0);
        c.set("a", b"1".to_vec(), 60);
        assert_eq!(c.len(), 1);
        c.set("b", b"2".to_vec(), 60);
        assert_eq!(c.len(), 2);
        c.del("a");
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn is_empty_on_fresh_cache() {
        let c = cache(10);
        assert!(c.is_empty());
        c.set("x", b"y".to_vec(), 60);
        assert!(!c.is_empty());
    }

    #[test]
    fn expired_entry_returns_none() {
        let c = cache(10);
        let past = Instant::now() - Duration::from_secs(1);
        c.set_with_expiry("stale", b"old".to_vec(), past);
        assert!(c.get("stale").is_none());
    }

    #[test]
    fn not_yet_expired_entry_returns_value() {
        let c = cache(10);
        let future = Instant::now() + Duration::from_secs(3600);
        c.set_with_expiry("fresh", b"new".to_vec(), future);
        assert_eq!(c.get("fresh"), Some(b"new".to_vec()));
    }

    #[test]
    fn set_with_expiry_past_does_not_return() {
        let c = cache(10);
        let past = Instant::now() - Duration::from_millis(500);
        c.set_with_expiry("exp", b"data".to_vec(), past);
        assert!(c.get("exp").is_none());
    }

    #[test]
    fn evicts_when_at_capacity() {
        let c = cache(2);
        c.set("a", b"1".to_vec(), 60);
        c.set("b", b"2".to_vec(), 60);
        c.set("c", b"3".to_vec(), 60);
        assert_eq!(c.get("c"), Some(b"3".to_vec()));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn prefers_expired_entry_for_eviction() {
        let c = cache(2);
        let past = Instant::now() - Duration::from_secs(1);
        c.set_with_expiry("old", b"expired".to_vec(), past);
        c.set("live", b"alive".to_vec(), 60);
        c.set("new", b"fresh".to_vec(), 60);
        assert_eq!(c.get("live"), Some(b"alive".to_vec()));
        assert_eq!(c.get("new"), Some(b"fresh".to_vec()));
        assert!(c.get("old").is_none());
    }

    #[test]
    fn overwrite_existing_key_does_not_trigger_eviction() {
        let c = cache(2);
        c.set("a", b"1".to_vec(), 60);
        c.set("b", b"2".to_vec(), 60);
        c.set("a", b"updated".to_vec(), 60);
        assert_eq!(c.len(), 2);
        assert_eq!(c.get("a"), Some(b"updated".to_vec()));
        assert_eq!(c.get("b"), Some(b"2".to_vec()));
    }

    #[test]
    fn capacity_of_one_evicts_on_new_key() {
        let c = cache(1);
        c.set("first", b"1".to_vec(), 60);
        c.set("second", b"2".to_vec(), 60);
        assert_eq!(c.get("second"), Some(b"2".to_vec()));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn keys_with_prefix_returns_matching_keys() {
        let c = cache(10);
        c.set("user:1", b"a".to_vec(), 60);
        c.set("user:2", b"b".to_vec(), 60);
        c.set("post:1", b"c".to_vec(), 60);
        let mut keys = c.keys_with_prefix("user:");
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2"]);
    }

    #[test]
    fn keys_with_prefix_empty_when_no_match() {
        let c = cache(10);
        c.set("foo:1", b"x".to_vec(), 60);
        assert!(c.keys_with_prefix("bar:").is_empty());
    }

    #[test]
    fn clone_shares_same_state() {
        let c1 = cache(10);
        let c2 = c1.clone();
        c1.set("shared", b"value".to_vec(), 60);
        assert_eq!(c2.get("shared"), Some(b"value".to_vec()));
    }
}
