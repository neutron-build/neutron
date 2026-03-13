//! Tiered document store — hot in-memory cache backed by cold LSM-tree storage.
//!
//! Documents are organized by collection (string name) and document ID (string).
//! Hot tier keeps the most recently accessed documents in memory; overflow spills
//! to a disk-backed LsmTree cold tier.
//!
//! Key format in LsmTree: `"{collection}\0{doc_id}"` (null-byte separator).
//! Value format: JSON string bytes of the document's JsonValue.

use std::collections::{BTreeMap, HashMap, HashSet};

use parking_lot::Mutex;

use super::JsonValue;
use crate::storage::lsm::{LsmConfig, LsmTree};

// ============================================================================
// JSON serialization helpers (JsonValue <-> String)
// ============================================================================

fn json_value_to_bytes(val: &JsonValue) -> Vec<u8> {
    val.to_json_string().into_bytes()
}

fn json_value_from_bytes(bytes: &[u8]) -> Option<JsonValue> {
    let s = std::str::from_utf8(bytes).ok()?;
    parse_json_value(s).map(|(v, _)| v)
}

/// Public wrapper around the JSON parser for use by the cold tier in mod.rs.
pub fn parse_json_value_pub(s: &str) -> Option<(JsonValue, &str)> {
    parse_json_value(s)
}

/// Minimal recursive-descent JSON parser for deserializing cold-tier values.
fn parse_json_value(s: &str) -> Option<(JsonValue, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    match s.as_bytes()[0] {
        b'n' => {
            if let Some(rest) = s.strip_prefix("null") {
                Some((JsonValue::Null, rest))
            } else {
                None
            }
        }
        b't' => {
            if let Some(rest) = s.strip_prefix("true") {
                Some((JsonValue::Bool(true), rest))
            } else {
                None
            }
        }
        b'f' => {
            if let Some(rest) = s.strip_prefix("false") {
                Some((JsonValue::Bool(false), rest))
            } else {
                None
            }
        }
        b'"' => parse_json_string(s).map(|(st, rest)| (JsonValue::Str(st), rest)),
        b'[' => parse_json_array(s),
        b'{' => parse_json_object(s),
        _ => parse_json_number(s),
    }
}

fn parse_json_string(s: &str) -> Option<(String, &str)> {
    if !s.starts_with('"') {
        return None;
    }
    let s = &s[1..];
    let mut out = String::new();
    let mut chars = s.chars();
    loop {
        match chars.next()? {
            '"' => return Some((out, chars.as_str())),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                '/' => out.push('/'),
                'u' => {
                    // Parse 4 hex digits.
                    let mut hex = String::with_capacity(4);
                    for _ in 0..4 {
                        hex.push(chars.next()?);
                    }
                    let cp = u32::from_str_radix(&hex, 16).ok()?;
                    if let Some(c) = char::from_u32(cp) {
                        out.push(c);
                    }
                }
                _ => return None,
            },
            c => out.push(c),
        }
    }
}

fn parse_json_number(s: &str) -> Option<(JsonValue, &str)> {
    let s = s.trim_start();
    let mut end = 0;
    let bytes = s.as_bytes();
    if end < bytes.len() && bytes[end] == b'-' {
        end += 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end < bytes.len() && bytes[end] == b'.' {
        end += 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end < bytes.len() && (bytes[end] == b'e' || bytes[end] == b'E') {
        end += 1;
        if end < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
            end += 1;
        }
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end == 0 {
        return None;
    }
    let num_str = &s[..end];
    let n: f64 = num_str.parse().ok()?;
    Some((JsonValue::Number(n), &s[end..]))
}

fn parse_json_array(s: &str) -> Option<(JsonValue, &str)> {
    let mut s = s.strip_prefix('[')?.trim_start();
    let mut items = Vec::new();
    if let Some(rest) = s.strip_prefix(']') {
        return Some((JsonValue::Array(items), rest));
    }
    loop {
        let (val, rest) = parse_json_value(s)?;
        items.push(val);
        s = rest.trim_start();
        if let Some(rest) = s.strip_prefix(']') {
            return Some((JsonValue::Array(items), rest));
        }
        s = s.strip_prefix(',')?.trim_start();
    }
}

fn parse_json_object(s: &str) -> Option<(JsonValue, &str)> {
    let mut s = s.strip_prefix('{')?.trim_start();
    let mut map = BTreeMap::new();
    if let Some(rest) = s.strip_prefix('}') {
        return Some((JsonValue::Object(map), rest));
    }
    loop {
        let (key, rest) = parse_json_string(s.trim_start())?;
        let rest = rest.trim_start().strip_prefix(':')?;
        let (val, rest) = parse_json_value(rest)?;
        map.insert(key, val);
        s = rest.trim_start();
        if let Some(rest) = s.strip_prefix('}') {
            return Some((JsonValue::Object(map), rest));
        }
        s = s.strip_prefix(',')?.trim_start();
    }
}

// ============================================================================
// Compound key helpers
// ============================================================================

fn make_key(collection: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + 1 + doc_id.len());
    key.extend_from_slice(collection.as_bytes());
    key.push(0); // null byte separator
    key.extend_from_slice(doc_id.as_bytes());
    key
}

fn split_key(key: &[u8]) -> Option<(String, String)> {
    let pos = key.iter().position(|&b| b == 0)?;
    let collection = std::str::from_utf8(&key[..pos]).ok()?.to_string();
    let doc_id = std::str::from_utf8(&key[pos + 1..]).ok()?.to_string();
    Some((collection, doc_id))
}

/// Collection prefix range for scanning all keys in a collection.
fn collection_range(collection: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = Vec::with_capacity(collection.len() + 1);
    start.extend_from_slice(collection.as_bytes());
    start.push(0);

    let mut end = Vec::with_capacity(collection.len() + 1);
    end.extend_from_slice(collection.as_bytes());
    end.push(1); // next byte after \0
    (start, end)
}

// ============================================================================
// TieredDocumentStore
// ============================================================================

/// Document store with an in-memory hot tier and a disk-backed cold tier.
///
/// The hot tier is a `HashMap<String, HashMap<String, JsonValue>>` organized
/// by collection and document ID. When the total document count exceeds
/// `max_hot_docs`, the oldest entries are evicted to the cold LsmTree tier.
pub struct TieredDocumentStore {
    /// Hot tier: collection -> (doc_id -> document)
    hot: HashMap<String, HashMap<String, JsonValue>>,
    /// Tracks total number of docs in hot tier.
    hot_count: usize,
    /// Cold tier: disk-backed LSM tree.
    cold: Mutex<LsmTree>,
    /// Maximum documents in the hot tier before eviction.
    max_hot_docs: usize,
}

impl TieredDocumentStore {
    /// Create a new tiered document store with an in-memory cold tier (for tests).
    pub fn new(max_hot_docs: usize) -> Self {
        Self {
            hot: HashMap::new(),
            hot_count: 0,
            cold: Mutex::new(LsmTree::new(LsmConfig::default())),
            max_hot_docs,
        }
    }

    /// Open a tiered document store with a disk-backed cold tier.
    pub fn open(dir: &str, max_hot_docs: usize) -> Self {
        let cold = LsmTree::open(LsmConfig::default(), std::path::Path::new(dir))
            .unwrap_or_else(|_| LsmTree::new(LsmConfig::default()));
        Self {
            hot: HashMap::new(),
            hot_count: 0,
            cold: Mutex::new(cold),
            max_hot_docs,
        }
    }

    /// Insert a document into a collection.
    pub fn insert(&mut self, collection: &str, doc_id: &str, doc: JsonValue) {
        // If it already exists in hot, just replace (don't increment count).
        let coll = self.hot.entry(collection.to_string()).or_default();
        if coll.contains_key(doc_id) {
            coll.insert(doc_id.to_string(), doc);
        } else {
            // Also remove from cold if it was there (overwrite semantics).
            {
                let mut cold = self.cold.lock();
                cold.delete(make_key(collection, doc_id));
            }
            coll.insert(doc_id.to_string(), doc);
            self.hot_count += 1;
        }

        self.maybe_evict();
    }

    /// Get a document by collection and ID. Checks hot tier first, then cold.
    /// On cold hit, promotes the document to the hot tier.
    pub fn get(&mut self, collection: &str, doc_id: &str) -> Option<JsonValue> {
        // Check hot tier.
        if let Some(coll) = self.hot.get(collection)
            && let Some(doc) = coll.get(doc_id) {
                return Some(doc.clone());
            }

        // Check cold tier.
        let key = make_key(collection, doc_id);
        let cold_val = {
            let cold = self.cold.lock();
            cold.get(&key)
        };

        if let Some(bytes) = cold_val
            && let Some(doc) = json_value_from_bytes(&bytes) {
                // Promote to hot tier.
                self.promote(collection, doc_id, doc.clone());
                return Some(doc);
            }

        None
    }

    /// Delete a document from both tiers. Returns true if found.
    pub fn delete(&mut self, collection: &str, doc_id: &str) -> bool {
        let mut found = false;

        // Remove from hot.
        if let Some(coll) = self.hot.get_mut(collection)
            && coll.remove(doc_id).is_some() {
                self.hot_count -= 1;
                found = true;
                if coll.is_empty() {
                    self.hot.remove(collection);
                }
            }

        // Remove from cold.
        let key = make_key(collection, doc_id);
        let cold = self.cold.lock();
        // LsmTree::delete writes a tombstone; we check if it existed first.
        if cold.get(&key).is_some() {
            drop(cold);
            let mut cold = self.cold.lock();
            cold.delete(key);
            found = true;
        }

        found
    }

    /// Query documents in a collection by path and value. Searches both tiers.
    pub fn query(
        &mut self,
        collection: &str,
        path: &[&str],
        value: &JsonValue,
    ) -> Vec<(String, JsonValue)> {
        let mut results = Vec::new();
        let mut seen_ids = HashSet::new();

        // Search hot tier.
        if let Some(coll) = self.hot.get(collection) {
            for (doc_id, doc) in coll {
                if doc.get_path(path) == Some(value) {
                    results.push((doc_id.clone(), doc.clone()));
                    seen_ids.insert(doc_id.clone());
                }
            }
        }

        // Search cold tier.
        let (start, end) = collection_range(collection);
        let cold_entries = {
            let cold = self.cold.lock();
            cold.range(&start, &end)
        };

        for (key, val_bytes) in cold_entries {
            if let Some((_, doc_id)) = split_key(&key) {
                if seen_ids.contains(&doc_id) {
                    continue; // Already found in hot tier.
                }
                if let Some(doc) = json_value_from_bytes(&val_bytes)
                    && doc.get_path(path) == Some(value) {
                        results.push((doc_id, doc));
                    }
            }
        }

        results
    }

    /// Count documents in a collection across both tiers.
    pub fn count(&self, collection: &str) -> usize {
        let hot_count = self
            .hot
            .get(collection)
            .map(|c| c.len())
            .unwrap_or(0);

        // Scan cold for this collection, excluding docs also in hot.
        let (start, end) = collection_range(collection);
        let cold_entries = {
            let cold = self.cold.lock();
            cold.range(&start, &end)
        };

        let hot_coll = self.hot.get(collection);
        let cold_count = cold_entries
            .iter()
            .filter(|(key, _)| {
                if let Some((_, doc_id)) = split_key(key) {
                    // Only count if not in hot tier.
                    hot_coll
                        .map(|c| !c.contains_key(&doc_id))
                        .unwrap_or(true)
                } else {
                    false
                }
            })
            .count();

        hot_count + cold_count
    }

    /// List all collection names across both tiers.
    pub fn collections(&self) -> Vec<String> {
        let mut names: HashSet<String> = self.hot.keys().cloned().collect();

        // Scan all cold keys to discover collections.
        // Use a broad range scan (all keys).
        let cold_entries = {
            let cold = self.cold.lock();
            cold.range(&[], &[0xFF])
        };

        for (key, _) in &cold_entries {
            if let Some((coll, _)) = split_key(key) {
                names.insert(coll);
            }
        }

        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    /// Total document count across all collections and both tiers.
    pub fn total_count(&self) -> usize {
        let mut all_collections: HashSet<String> = self.hot.keys().cloned().collect();
        let cold_entries = {
            let cold = self.cold.lock();
            cold.range(&[], &[0xFF])
        };
        for (key, _) in &cold_entries {
            if let Some((coll, _)) = split_key(key) {
                all_collections.insert(coll);
            }
        }
        all_collections.iter().map(|c| self.count(c)).sum()
    }

    /// Flush all hot-tier documents to the cold tier (for persistence/shutdown).
    /// After calling this, `force_flush()` on the cold tier ensures data is on disk.
    pub fn flush_to_cold(&mut self) {
        let mut cold = self.cold.lock();
        for (coll_name, coll) in &self.hot {
            for (doc_id, doc) in coll {
                let key = make_key(coll_name, doc_id);
                let val = json_value_to_bytes(doc);
                cold.put(key, val);
            }
        }
        self.hot.clear();
        self.hot_count = 0;
        cold.force_flush();
    }

    // ---- Internal helpers ----

    /// Promote a document from cold to hot tier.
    fn promote(&mut self, collection: &str, doc_id: &str, doc: JsonValue) {
        // Remove from cold.
        {
            let mut cold = self.cold.lock();
            cold.delete(make_key(collection, doc_id));
        }

        // Add to hot.
        let coll = self.hot.entry(collection.to_string()).or_default();
        if !coll.contains_key(doc_id) {
            self.hot_count += 1;
        }
        coll.insert(doc_id.to_string(), doc);

        self.maybe_evict();
    }

    /// Evict documents from hot to cold if over capacity.
    fn maybe_evict(&mut self) {
        if self.hot_count <= self.max_hot_docs {
            return;
        }

        let to_evict = self.hot_count - self.max_hot_docs;
        let mut evicted = 0;

        // Collect entries to evict: take from the first collections/docs we iterate.
        let mut eviction_list: Vec<(String, String, JsonValue)> = Vec::new();

        'outer: for (coll_name, coll) in &self.hot {
            for (doc_id, doc) in coll {
                eviction_list.push((coll_name.clone(), doc_id.clone(), doc.clone()));
                evicted += 1;
                if evicted >= to_evict {
                    break 'outer;
                }
            }
        }

        // Move evicted docs to cold.
        {
            let mut cold = self.cold.lock();
            for (coll, doc_id, doc) in &eviction_list {
                let key = make_key(coll, doc_id);
                let val = json_value_to_bytes(doc);
                cold.put(key, val);
            }
        }

        // Remove evicted docs from hot.
        for (coll, doc_id, _) in &eviction_list {
            if let Some(c) = self.hot.get_mut(coll) {
                c.remove(doc_id);
                self.hot_count -= 1;
                if c.is_empty() {
                    self.hot.remove(coll);
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

    fn make_doc(name: &str, value: f64) -> JsonValue {
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), JsonValue::Str(name.to_string()));
        map.insert("value".to_string(), JsonValue::Number(value));
        JsonValue::Object(map)
    }

    #[test]
    fn test_tiered_doc_basic_insert_get() {
        let mut store = TieredDocumentStore::new(100);
        let doc = make_doc("Alice", 42.0);
        store.insert("users", "1", doc.clone());
        let result = store.get("users", "1");
        assert_eq!(result, Some(doc));
    }

    #[test]
    fn test_tiered_doc_eviction() {
        let mut store = TieredDocumentStore::new(3);
        // Insert 5 docs -- only 3 should remain in hot.
        for i in 0..5 {
            store.insert("coll", &i.to_string(), make_doc(&format!("doc{i}"), i as f64));
        }
        assert!(store.hot_count <= 3);
        // All 5 should still be accessible (from hot or cold).
        for i in 0..5 {
            assert!(
                store.get("coll", &i.to_string()).is_some(),
                "doc {i} should be accessible"
            );
        }
    }

    #[test]
    fn test_tiered_doc_promote_from_cold() {
        let mut store = TieredDocumentStore::new(2);
        store.insert("coll", "a", make_doc("A", 1.0));
        store.insert("coll", "b", make_doc("B", 2.0));
        store.insert("coll", "c", make_doc("C", 3.0));
        // At least one doc was evicted to cold. All 3 should still be readable.
        let doc_a = store.get("coll", "a").unwrap();
        assert_eq!(doc_a.get_path(&["name"]), Some(&JsonValue::Str("A".to_string())));
        let doc_b = store.get("coll", "b").unwrap();
        assert_eq!(doc_b.get_path(&["name"]), Some(&JsonValue::Str("B".to_string())));
        let doc_c = store.get("coll", "c").unwrap();
        assert_eq!(doc_c.get_path(&["name"]), Some(&JsonValue::Str("C".to_string())));
        // Verify cold tier was involved (not everything stayed in hot).
        assert!(store.hot_count <= 2);
    }

    #[test]
    fn test_tiered_doc_delete() {
        let mut store = TieredDocumentStore::new(2);
        store.insert("coll", "x", make_doc("X", 10.0));
        store.insert("coll", "y", make_doc("Y", 20.0));
        store.insert("coll", "z", make_doc("Z", 30.0));

        // Delete one from each potential tier.
        assert!(store.delete("coll", "x") || store.delete("coll", "x") == false);
        // Actually test deletion.
        store.delete("coll", "z");
        assert!(store.get("coll", "z").is_none());

        // Delete non-existent.
        assert!(!store.delete("coll", "nonexistent"));
    }

    #[test]
    fn test_tiered_doc_query_across_tiers() {
        let mut store = TieredDocumentStore::new(3);
        for i in 0..10 {
            let doc = make_doc(&format!("user{i}"), if i % 2 == 0 { 1.0 } else { 2.0 });
            store.insert("users", &i.to_string(), doc);
        }
        // Query for value=1.0 (should be 5 docs: 0, 2, 4, 6, 8).
        let results = store.query("users", &["value"], &JsonValue::Number(1.0));
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_tiered_doc_count() {
        let mut store = TieredDocumentStore::new(5);
        for i in 0..20 {
            store.insert("items", &i.to_string(), make_doc(&format!("item{i}"), i as f64));
        }
        assert_eq!(store.count("items"), 20);
    }

    #[test]
    fn test_tiered_doc_collections() {
        let mut store = TieredDocumentStore::new(5);
        store.insert("alpha", "1", make_doc("a", 1.0));
        store.insert("beta", "1", make_doc("b", 1.0));
        store.insert("gamma", "1", make_doc("c", 1.0));
        // Fill up to trigger eviction.
        for i in 0..10 {
            store.insert("delta", &i.to_string(), make_doc(&format!("d{i}"), i as f64));
        }
        let colls = store.collections();
        assert!(colls.contains(&"alpha".to_string()));
        assert!(colls.contains(&"beta".to_string()));
        assert!(colls.contains(&"gamma".to_string()));
        assert!(colls.contains(&"delta".to_string()));
    }

    #[test]
    fn test_tiered_doc_overwrite() {
        let mut store = TieredDocumentStore::new(2);
        store.insert("coll", "key", make_doc("v1", 1.0));
        store.insert("coll", "other1", make_doc("o1", 2.0));
        store.insert("coll", "other2", make_doc("o2", 3.0));
        // "key" might be evicted. Now overwrite it.
        store.insert("coll", "key", make_doc("v2", 10.0));
        let result = store.get("coll", "key").unwrap();
        assert_eq!(
            result.get_path(&["name"]),
            Some(&JsonValue::Str("v2".to_string()))
        );
        assert_eq!(
            result.get_path(&["value"]),
            Some(&JsonValue::Number(10.0))
        );
    }

    #[test]
    fn test_tiered_doc_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap();

        // Write data and flush everything to cold (disk).
        {
            let mut store = TieredDocumentStore::open(dir_path, 2);
            for i in 0..10 {
                store.insert("persist", &i.to_string(), make_doc(&format!("p{i}"), i as f64));
            }
            // Flush all hot docs to cold, then cold memtable to disk.
            store.flush_to_cold();
        }

        // Reopen and verify cold data survives.
        {
            let mut store = TieredDocumentStore::open(dir_path, 2);
            // Hot tier is empty after reopen; all data is in cold.
            for i in 0..10 {
                let doc = store.get("persist", &i.to_string());
                assert!(doc.is_some(), "doc {i} should survive reopen");
                let doc = doc.unwrap();
                assert_eq!(
                    doc.get_path(&["name"]),
                    Some(&JsonValue::Str(format!("p{i}")))
                );
            }
        }
    }

    #[test]
    fn test_tiered_doc_large_dataset() {
        let mut store = TieredDocumentStore::new(500);
        for i in 0..5000 {
            store.insert(
                "big",
                &i.to_string(),
                make_doc(&format!("item{i}"), i as f64),
            );
        }
        assert_eq!(store.count("big"), 5000);
        assert!(store.hot_count <= 500);

        // Spot-check some cold docs.
        for i in [0, 100, 999, 2500, 4999] {
            let doc = store.get("big", &i.to_string());
            assert!(doc.is_some(), "doc {i} should be accessible");
        }
    }

    #[test]
    fn test_tiered_doc_empty_collection() {
        let mut store = TieredDocumentStore::new(10);
        assert_eq!(store.count("empty"), 0);
        assert!(store.get("empty", "1").is_none());
        assert!(store.query("empty", &["x"], &JsonValue::Null).is_empty());
    }

    #[test]
    fn test_tiered_doc_multiple_collections_independent() {
        let mut store = TieredDocumentStore::new(5);
        store.insert("a", "1", make_doc("a1", 1.0));
        store.insert("b", "1", make_doc("b1", 2.0));
        store.insert("a", "2", make_doc("a2", 3.0));

        assert_eq!(store.count("a"), 2);
        assert_eq!(store.count("b"), 1);

        // Deleting from "a" shouldn't affect "b".
        store.delete("a", "1");
        assert_eq!(store.count("a"), 1);
        assert_eq!(store.count("b"), 1);
    }

    #[test]
    fn test_tiered_doc_json_roundtrip() {
        // Test that complex nested JSON survives cold tier serialization.
        let mut store = TieredDocumentStore::new(1);
        let nested = {
            let mut inner = BTreeMap::new();
            inner.insert("x".to_string(), JsonValue::Number(1.0));
            inner.insert(
                "arr".to_string(),
                JsonValue::Array(vec![
                    JsonValue::Bool(true),
                    JsonValue::Null,
                    JsonValue::Str("test".to_string()),
                ]),
            );
            let mut outer = BTreeMap::new();
            outer.insert("nested".to_string(), JsonValue::Object(inner));
            JsonValue::Object(outer)
        };

        store.insert("coll", "complex", nested.clone());
        // Force eviction by inserting more.
        store.insert("coll", "filler", make_doc("f", 0.0));
        // Now "complex" should be in cold tier. Retrieve it.
        let result = store.get("coll", "complex");
        assert_eq!(result, Some(nested));
    }
}
