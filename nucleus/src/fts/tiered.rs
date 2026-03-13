//! Disk-backed tiered storage for the full-text search engine.
//!
//! `DiskBackedFtsStore` keeps the inverted index (term -> posting list) entirely
//! in memory (it is small: just term strings + doc_id integers + positions), but
//! stores the original document content on disk via an [`LsmTree`]. This allows
//! the FTS engine to handle corpora that far exceed available RAM while keeping
//! search performance high — the inverted index is the hot path and stays in RAM,
//! while the cold path (fetching original document text for snippets / highlights)
//! goes through a bounded cache backed by disk.
//!
//! ## Key design points
//!
//! - **LsmTree storage**: Each document is keyed by its `u64` doc_id (8-byte
//!   big-endian) with the value being the original UTF-8 text.
//! - **Bounded cache**: A `HashMap<u64, String>` that holds at most
//!   `max_cached_docs` entries. Eviction removes the lowest-ID entries.
//! - **In-memory inverted index**: Built from the same tokenization pipeline as
//!   `InvertedIndex` in `fts/mod.rs`. Search uses BM25 scoring.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use parking_lot::{Mutex, RwLock};

use crate::fts::tokenize;
use crate::storage::lsm::{LsmConfig, LsmTree};

// ============================================================================
// Encoding helpers
// ============================================================================

/// Encode a document ID as an 8-byte big-endian key for the LsmTree.
fn id_to_key(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

/// Decode an 8-byte big-endian key back to a document ID.
fn key_to_id(key: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&key[..8]);
    u64::from_be_bytes(buf)
}

// ============================================================================
// In-memory inverted index (lightweight)
// ============================================================================

/// A posting entry for the in-memory inverted index.
#[derive(Debug, Clone)]
#[allow(dead_code)] // `positions` stored for future phrase query support
struct Posting {
    doc_id: u64,
    positions: Vec<usize>,
    term_frequency: f64,
}

/// Lightweight in-memory inverted index for BM25 search.
#[derive(Debug, Default)]
struct MemIndex {
    /// term -> posting list (sorted by doc_id for intersection).
    postings: HashMap<String, Vec<Posting>>,
    /// doc_id -> document length (in tokens).
    doc_lengths: HashMap<u64, usize>,
    /// Total number of documents.
    doc_count: u64,
    /// Sum of all document lengths (for avgdl).
    total_length: usize,
}

impl MemIndex {
    fn new() -> Self {
        Self::default()
    }

    /// Index a document's text, building posting list entries in memory.
    fn add_document(&mut self, doc_id: u64, text: &str) {
        // If the document already exists, remove it first.
        if self.doc_lengths.contains_key(&doc_id) {
            self.remove_document(doc_id);
        }

        let tokens = tokenize(text);
        let doc_length = tokens.len();

        self.doc_lengths.insert(doc_id, doc_length);
        self.doc_count += 1;
        self.total_length += doc_length;

        // Group tokens by term.
        let mut term_positions: HashMap<String, Vec<usize>> = HashMap::new();
        for token in &tokens {
            term_positions
                .entry(token.term.clone())
                .or_default()
                .push(token.position);
        }

        for (term, positions) in term_positions {
            let tf = positions.len() as f64 / doc_length.max(1) as f64;
            self.postings.entry(term).or_default().push(Posting {
                doc_id,
                positions,
                term_frequency: tf,
            });
        }
    }

    /// Remove a document from the in-memory inverted index.
    fn remove_document(&mut self, doc_id: u64) {
        if let Some(length) = self.doc_lengths.remove(&doc_id) {
            self.doc_count -= 1;
            self.total_length -= length;

            for postings in self.postings.values_mut() {
                postings.retain(|p| p.doc_id != doc_id);
            }
            self.postings.retain(|_, v| !v.is_empty());
        }
    }

    /// BM25 search: returns `(doc_id, score)` pairs sorted by score descending.
    fn search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return vec![];
        }

        let avgdl = if self.doc_count > 0 {
            self.total_length as f64 / self.doc_count as f64
        } else {
            1.0
        };
        let k1: f64 = 1.2;
        let b: f64 = 0.75;

        let mut scores: HashMap<u64, f64> = HashMap::new();

        for token in &query_tokens {
            if let Some(postings) = self.postings.get(&token.term) {
                let df = postings.len() as f64;
                let idf = ((self.doc_count as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();

                for posting in postings {
                    let dl = *self.doc_lengths.get(&posting.doc_id).unwrap_or(&1) as f64;
                    let tf = posting.term_frequency;
                    let tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avgdl));
                    let score = idf * tf_norm;
                    *scores.entry(posting.doc_id).or_default() += score;
                }
            }
        }

        let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b_entry| {
            b_entry
                .1
                .partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b_entry.0))
        });
        results.truncate(limit);
        results
    }
}

// ============================================================================
// DiskBackedFtsStore
// ============================================================================

/// A full-text search store that keeps document content on disk (via LsmTree)
/// with the inverted index in memory and a bounded document cache.
pub struct DiskBackedFtsStore {
    /// The LsmTree holding original document texts.
    doc_lsm: Mutex<LsmTree>,
    /// In-memory cache of recently-used documents.
    doc_cache: RwLock<HashMap<u64, String>>,
    /// Maximum number of documents to keep in the cache.
    max_cached_docs: usize,
    /// In-memory inverted index for search.
    index: RwLock<MemIndex>,
    /// Monotonically increasing document ID counter.
    next_doc_id: AtomicU64,
    /// Count of live documents.
    count: AtomicU64,
}

impl DiskBackedFtsStore {
    /// Create a new in-memory-only store (no disk persistence). Useful for tests.
    pub fn new(max_cached_docs: usize) -> Self {
        let config = LsmConfig {
            memtable_flush_threshold: 500,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        Self {
            doc_lsm: Mutex::new(LsmTree::new(config)),
            doc_cache: RwLock::new(HashMap::new()),
            max_cached_docs,
            index: RwLock::new(MemIndex::new()),
            next_doc_id: AtomicU64::new(1),
            count: AtomicU64::new(0),
        }
    }

    /// Open a disk-backed store at the given directory, reloading any existing
    /// documents from SSTable files.
    pub fn open(dir: &str, max_cached_docs: usize) -> Self {
        let config = LsmConfig {
            memtable_flush_threshold: 500,
            level_max_sstables: 4,
            max_levels: 4,
            bloom_bits_per_key: 10,
        };
        let path = Path::new(dir);
        let tree = LsmTree::open(config, path)
            .unwrap_or_else(|e| panic!("failed to open LsmTree at {dir}: {e}"));

        // Scan all existing entries.
        let start = [0u8; 8];
        let end = [0xFFu8; 8];
        let entries = tree.range(&start, &end);

        let mut max_id: u64 = 0;
        let mut count: u64 = 0;
        let mut cache = HashMap::new();
        let mut mem_index = MemIndex::new();

        for (key, value) in &entries {
            if key.len() == 8 {
                let id = key_to_id(key);
                if id >= max_id {
                    max_id = id;
                }
                count += 1;

                // Decode document text (UTF-8).
                if let Ok(text) = std::str::from_utf8(value) {
                    // Rebuild inverted index.
                    mem_index.add_document(id, text);

                    // Cache up to limit.
                    if cache.len() < max_cached_docs {
                        cache.insert(id, text.to_string());
                    }
                }
            }
        }

        Self {
            doc_lsm: Mutex::new(tree),
            doc_cache: RwLock::new(cache),
            max_cached_docs,
            index: RwLock::new(mem_index),
            next_doc_id: AtomicU64::new(max_id + 1),
            count: AtomicU64::new(count),
        }
    }

    /// Add a document with a specific doc_id. Stores content on disk, builds
    /// inverted index in memory, and caches if room.
    pub fn add_document(&self, doc_id: u64, content: &str) {
        let key = id_to_key(doc_id);
        let value = content.as_bytes().to_vec();

        // Write to LsmTree.
        {
            let mut lsm = self.doc_lsm.lock();
            lsm.put(key, value);
        }

        // Build inverted index entry.
        {
            let mut idx = self.index.write();
            idx.add_document(doc_id, content);
        }

        // Cache the document.
        {
            let mut cache = self.doc_cache.write();
            if cache.len() >= self.max_cached_docs {
                self.evict_cache(&mut cache);
            }
            cache.insert(doc_id, content.to_string());
        }

        // Update next_doc_id if needed.
        loop {
            let current = self.next_doc_id.load(AtomicOrdering::Relaxed);
            if doc_id >= current {
                if self
                    .next_doc_id
                    .compare_exchange(
                        current,
                        doc_id + 1,
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
            } else {
                break;
            }
        }

        self.count.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Search for documents matching the query. Returns `(doc_id, score)` pairs
    /// sorted by BM25 score descending.
    pub fn search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let idx = self.index.read();
        idx.search(query, limit)
    }

    /// Retrieve a document's original text by ID. Checks cache first, falls
    /// back to disk.
    pub fn get_document(&self, doc_id: u64) -> Option<String> {
        // Check cache first.
        {
            let cache = self.doc_cache.read();
            if let Some(text) = cache.get(&doc_id) {
                return Some(text.clone());
            }
        }

        // Fall back to disk.
        let key = id_to_key(doc_id);
        let data = {
            let lsm = self.doc_lsm.lock();
            lsm.get(&key)?
        };

        let text = String::from_utf8(data).ok()?;

        // Promote to cache.
        {
            let mut cache = self.doc_cache.write();
            if cache.len() >= self.max_cached_docs {
                self.evict_cache(&mut cache);
            }
            cache.insert(doc_id, text.clone());
        }

        Some(text)
    }

    /// Remove a document from the store, disk, cache, and inverted index.
    pub fn remove_document(&self, doc_id: u64) {
        let key = id_to_key(doc_id);

        // Check existence.
        let existed = {
            let lsm = self.doc_lsm.lock();
            lsm.get(&key).is_some()
        };

        if !existed {
            return;
        }

        // Tombstone in LsmTree.
        {
            let mut lsm = self.doc_lsm.lock();
            lsm.delete(key);
        }

        // Remove from inverted index.
        {
            let mut idx = self.index.write();
            idx.remove_document(doc_id);
        }

        // Remove from cache.
        {
            let mut cache = self.doc_cache.write();
            cache.remove(&doc_id);
        }

        self.count.fetch_sub(1, AtomicOrdering::Relaxed);
    }

    /// Number of live documents in the store.
    pub fn document_count(&self) -> usize {
        self.count.load(AtomicOrdering::Relaxed) as usize
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.document_count() == 0
    }

    /// Flush the LsmTree memtable to disk.
    pub fn flush_cache(&self) {
        let mut lsm = self.doc_lsm.lock();
        lsm.force_flush();
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Evict entries from the cache to make room. Removes the entries with the
    /// smallest IDs (oldest-inserted heuristic).
    fn evict_cache(&self, cache: &mut HashMap<u64, String>) {
        if cache.len() < self.max_cached_docs {
            return;
        }
        let evict_count = (self.max_cached_docs / 4).max(1);
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

    // ── Test 1: add and search ──────────────────────────────────────────

    #[test]
    fn test_disk_fts_add_search() {
        let store = DiskBackedFtsStore::new(100);
        store.add_document(1, "The quick brown fox jumps over the lazy dog");
        store.add_document(2, "Rust programming language is fast and safe");
        store.add_document(3, "The fox is quick and brown");

        let results = store.search("quick fox", 10);
        assert!(!results.is_empty());
        // Documents 1 and 3 both contain "quick" and "fox" (after stemming).
        let doc_ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&1));
        assert!(doc_ids.contains(&3));
    }

    // ── Test 2: get document content ────────────────────────────────────

    #[test]
    fn test_disk_fts_get_document() {
        let store = DiskBackedFtsStore::new(100);
        let text = "Hello world, this is a test document.";
        store.add_document(42, text);

        let retrieved = store.get_document(42).unwrap();
        assert_eq!(retrieved, text);

        // Non-existent doc.
        assert!(store.get_document(999).is_none());
    }

    // ── Test 3: remove document ─────────────────────────────────────────

    #[test]
    fn test_disk_fts_remove_document() {
        let store = DiskBackedFtsStore::new(100);
        store.add_document(1, "searchable document with unique terms");
        assert_eq!(store.document_count(), 1);

        store.remove_document(1);
        assert_eq!(store.document_count(), 0);
        assert!(store.get_document(1).is_none());

        // Search should return empty after removal.
        let results = store.search("searchable unique", 10);
        assert!(results.is_empty());
    }

    // ── Test 4: cache eviction ──────────────────────────────────────────

    #[test]
    fn test_disk_fts_cache_eviction() {
        let store = DiskBackedFtsStore::new(10); // max 10 cached

        // Add 20 documents.
        for i in 1..=20u64 {
            store.add_document(i, &format!("document number {i} with some text content"));
        }

        // All 20 should be retrievable.
        for i in 1..=20u64 {
            assert!(
                store.get_document(i).is_some(),
                "document {i} should be retrievable after eviction"
            );
        }

        // Cache size should be bounded.
        let cache = store.doc_cache.read();
        assert!(
            cache.len() <= 10,
            "cache should be bounded to max_cached_docs, got {}",
            cache.len()
        );
    }

    // ── Test 5: cold load from disk ─────────────────────────────────────

    #[test]
    fn test_disk_fts_cold_load() {
        let store = DiskBackedFtsStore::new(5); // tiny cache

        for i in 1..=10u64 {
            store.add_document(i, &format!("document {i} text for cold loading test"));
        }

        // Clear cache to simulate cold start.
        {
            let mut cache = store.doc_cache.write();
            cache.clear();
        }

        // All docs should still be loadable from disk.
        for i in 1..=10u64 {
            let doc = store.get_document(i);
            assert!(doc.is_some(), "document {i} should be loadable from disk");
            assert!(doc.unwrap().contains(&format!("document {i}")));
        }
    }

    // ── Test 6: persistence across reopen ───────────────────────────────

    #[test]
    fn test_disk_fts_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap();

        // Add documents and flush.
        {
            let store = DiskBackedFtsStore::open(dir_str, 100);
            store.add_document(1, "persistent document alpha");
            store.add_document(2, "persistent document beta");
            store.flush_cache();
        }

        // Reopen and verify.
        {
            let store = DiskBackedFtsStore::open(dir_str, 100);
            assert_eq!(store.document_count(), 2);

            let d1 = store.get_document(1).unwrap();
            assert_eq!(d1, "persistent document alpha");

            let d2 = store.get_document(2).unwrap();
            assert_eq!(d2, "persistent document beta");

            // Inverted index should be rebuilt — search should work.
            let results = store.search("persistent alpha", 10);
            assert!(!results.is_empty());
            assert_eq!(results[0].0, 1);
        }
    }

    // ── Test 7: large dataset ───────────────────────────────────────────

    #[test]
    fn test_disk_fts_large_dataset() {
        let store = DiskBackedFtsStore::new(300); // cache only 300

        let topics = [
            "machine learning algorithms",
            "distributed database systems",
            "quantum computing theory",
            "network security protocols",
            "compiler optimization techniques",
        ];

        // Insert 3000 documents.
        for i in 1..=3000u64 {
            let topic = topics[(i as usize) % topics.len()];
            let text = format!("document {i} about {topic} with additional context");
            store.add_document(i, &text);
        }
        assert_eq!(store.document_count(), 3000);

        // Search should work and return relevant results.
        let results = store.search("machine learning", 10);
        assert!(!results.is_empty());
        assert!(results.len() <= 10);
    }

    // ── Test 8: multi-term search ranking ───────────────────────────────

    #[test]
    fn test_disk_fts_multiple_terms() {
        let store = DiskBackedFtsStore::new(100);

        // Doc 1: has both "rust" and "fast" (short doc for high term concentration)
        store.add_document(1, "Rust fast");
        // Doc 2: has "rust" only (longer, dilutes term frequency)
        store.add_document(2, "Rust has a strong type system with many features");
        // Doc 3: has "fast" only (longer, dilutes term frequency)
        store.add_document(3, "Python can be fast with numpy for numerical computing");
        // Doc 4: has neither
        store.add_document(4, "Java virtual machine runs bytecode");

        let results = store.search("rust fast", 10);
        // Doc 1 should rank highest (has both terms and shortest document).
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 1, "doc with both terms should rank first");

        // Doc 4 should not appear (no matching terms).
        let doc_ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(!doc_ids.contains(&4));
    }

    // ── Test 9: document count accuracy ─────────────────────────────────

    #[test]
    fn test_disk_fts_document_count() {
        let store = DiskBackedFtsStore::new(100);
        assert_eq!(store.document_count(), 0);
        assert!(store.is_empty());

        store.add_document(1, "first");
        assert_eq!(store.document_count(), 1);

        store.add_document(2, "second");
        assert_eq!(store.document_count(), 2);

        store.remove_document(1);
        assert_eq!(store.document_count(), 1);

        store.remove_document(2);
        assert_eq!(store.document_count(), 0);
        assert!(store.is_empty());
    }

    // ── Test 10: UTF-8 content round-trip ───────────────────────────────

    #[test]
    fn test_disk_fts_encoding() {
        let store = DiskBackedFtsStore::new(100);

        let texts = [
            "Plain ASCII text",
            "Unicode: cafe\u{0301} \u{2603} \u{1F600}",
            "Japanese: \u{65E5}\u{672C}\u{8A9E}\u{306E}\u{30C6}\u{30AD}\u{30B9}\u{30C8}",
            "Arabic: \u{0645}\u{0631}\u{062D}\u{0628}\u{0627}",
            "Mixed: hello \u{4E16}\u{754C} \u{1F30D}",
        ];

        for (i, text) in texts.iter().enumerate() {
            store.add_document((i + 1) as u64, text);
        }

        // Verify round-trip.
        for (i, text) in texts.iter().enumerate() {
            let id = (i + 1) as u64;
            let retrieved = store.get_document(id).unwrap();
            assert_eq!(
                &retrieved, text,
                "UTF-8 round-trip failed for document {id}"
            );
        }
    }
}
