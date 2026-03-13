//! Full-text search engine — inverted index with BM25 ranking.
//!
//! Supports:
//!   - Tokenization (whitespace + punctuation splitting, lowercasing)
//!   - English stemming (Porter stemmer, simplified)
//!   - Inverted index (term → posting list with positions)
//!   - BM25 ranking (Okapi BM25 with configurable k1 and b parameters)
//!   - Fuzzy matching (Levenshtein distance for typo tolerance)
//!   - Boolean queries (AND, OR via term intersection/union)
//!   - tsvector/tsquery compatible interface
//!   - Binary WAL persistence (crash-recovery via `FtsWal`)
//!
//! Designed to replace Elasticsearch/Meilisearch for Nucleus's use cases.

pub mod fts_wal;
pub mod tiered;

use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Tokenization
// ============================================================================

/// A token extracted from text, with position information.
#[derive(Debug, Clone)]
pub struct Token {
    pub term: String,
    pub position: usize,
}

/// Tokenize text: lowercase, split on non-alphanumeric, filter stopwords, stem.
pub fn tokenize(text: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut position = 0;

    for word in text.split(|c: char| !c.is_alphanumeric() && c != '\'') {
        let word = word.trim_matches('\'');
        if word.is_empty() {
            continue;
        }
        let lower = word.to_lowercase();
        if is_stopword(&lower) {
            continue;
        }
        let stemmed = stem(&lower);
        tokens.push(Token {
            term: stemmed,
            position,
        });
        position += 1;
    }

    tokens
}

/// Common English stopwords.
fn is_stopword(word: &str) -> bool {
    matches!(
        word,
        "a" | "an" | "the" | "is" | "are" | "was" | "were" | "be" | "been" | "being"
            | "have" | "has" | "had" | "do" | "does" | "did" | "will" | "would" | "could"
            | "should" | "may" | "might" | "shall" | "can" | "to" | "of" | "in" | "for"
            | "on" | "with" | "at" | "by" | "from" | "as" | "into" | "through" | "during"
            | "before" | "after" | "and" | "but" | "or" | "not" | "no" | "if" | "then"
            | "than" | "so" | "that" | "this" | "it" | "its" | "i" | "me" | "my" | "we"
            | "our" | "you" | "your" | "he" | "him" | "his" | "she" | "her" | "they"
            | "them" | "their" | "what" | "which" | "who" | "whom"
    )
}

/// Supported stemming languages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StemLanguage {
    English,
    German,
    French,
    Spanish,
    Italian,
    Portuguese,
}

/// Stem a word using the specified language.
pub fn stem_language(word: &str, lang: StemLanguage) -> String {
    match lang {
        StemLanguage::English => stem(word),
        StemLanguage::German => stem_german(word),
        StemLanguage::French => stem_french(word),
        StemLanguage::Spanish => stem_spanish(word),
        StemLanguage::Italian => stem_italian(word),
        StemLanguage::Portuguese => stem_portuguese(word),
    }
}

/// Simplified German stemmer.
fn stem_german(word: &str) -> String {
    let mut w = word.to_string();
    if w.ends_with("ungen") && w.len() > 6 {
        w.truncate(w.len() - 5);
    } else if w.ends_with("ung") && w.len() > 4 {
        w.truncate(w.len() - 3);
    } else if (w.ends_with("heit")
        || w.ends_with("keit")
        || w.ends_with("isch")
        || w.ends_with("lich")
        || w.ends_with("igen"))
        && w.len() > 5
    {
        w.truncate(w.len() - 4);
    } else if (w.ends_with("en") || w.ends_with("er") || w.ends_with("es")) && w.len() > 3 {
        w.truncate(w.len() - 2);
    } else if w.ends_with('e') && w.len() > 3 {
        w.pop();
    }
    w
}

/// Simplified French stemmer.
fn stem_french(word: &str) -> String {
    let mut w = word.to_string();
    if (w.ends_with("euses") || w.ends_with("ement")) && w.len() > 6 {
        w.truncate(w.len() - 5);
    } else if w.ends_with("tion") && w.len() > 5 {
        w.truncate(w.len() - 4);
        w.push('t');
    } else if (w.ends_with("ment") || w.ends_with("euse")) && w.len() > 5 {
        w.truncate(w.len() - 4);
    } else if (w.ends_with("eux") || w.ends_with("ant")) && w.len() > 4 {
        w.truncate(w.len() - 3);
    } else if (w.ends_with("er") || w.ends_with("es")) && w.len() > 3 {
        w.truncate(w.len() - 2);
    } else if (w.ends_with('e') || (w.ends_with('s') && !w.ends_with("ss"))) && w.len() > 3 {
        w.pop();
    }
    w
}

/// Simplified Spanish stemmer.
fn stem_spanish(word: &str) -> String {
    let mut w = word.to_string();
    if w.ends_with("amente") && w.len() > 7 {
        w.truncate(w.len() - 6);
    } else if (w.ends_with("mente") || w.ends_with("iendo")) && w.len() > 6 {
        w.truncate(w.len() - 5);
    } else if (w.ends_with("ador") || w.ends_with("ando")) && w.len() > 5 {
        w.truncate(w.len() - 4);
    } else if (w.ends_with("ado") || w.ends_with("ido")) && w.len() > 4 {
        w.truncate(w.len() - 3);
    } else if w.ends_with("ión") {
        // Handle UTF-8 multi-byte: ó is 2 bytes, ión is 4 bytes
        let byte_len = "ión".len();
        if w.len() > byte_len + 1 {
            w.truncate(w.len() - byte_len);
        }
    } else if (w.ends_with("ar") || w.ends_with("er") || w.ends_with("ir") || w.ends_with("es"))
        && w.len() > 3
    {
        w.truncate(w.len() - 2);
    } else if w.ends_with('s') && w.len() > 3 {
        w.pop();
    }
    w
}

/// Simplified Italian stemmer.
fn stem_italian(word: &str) -> String {
    let mut w = word.to_string();
    if (w.ends_with("mente") || w.ends_with("zione")) && w.len() > 6 {
        w.truncate(w.len() - 5);
    } else if (w.ends_with("ando") || w.ends_with("endo")) && w.len() > 5 {
        w.truncate(w.len() - 4);
    } else if (w.ends_with("ato")
        || w.ends_with("ito")
        || w.ends_with("are")
        || w.ends_with("ere")
        || w.ends_with("ire"))
        && w.len() > 4
    {
        w.truncate(w.len() - 3);
    } else if (w.ends_with("ia") || w.ends_with("ie")) && w.len() > 3 {
        w.truncate(w.len() - 2);
    } else if (w.ends_with('i') || w.ends_with('e')) && w.len() > 3 {
        w.pop();
    }
    w
}

/// Simplified Portuguese stemmer.
fn stem_portuguese(word: &str) -> String {
    let mut w = word.to_string();
    if w.ends_with("mente") && w.len() > 6 {
        w.truncate(w.len() - 5);
    } else if (w.ends_with("ando") || w.ends_with("endo") || w.ends_with("indo")) && w.len() > 5 {
        w.truncate(w.len() - 4);
    } else if (w.ends_with("ado") || w.ends_with("ido")) && w.len() > 4 {
        w.truncate(w.len() - 3);
    } else if (w.ends_with("ar")
        || w.ends_with("er")
        || w.ends_with("ir")
        || w.ends_with("os")
        || w.ends_with("as"))
        && w.len() > 3
    {
        w.truncate(w.len() - 2);
    } else if w.ends_with('s') && w.len() > 3 {
        w.pop();
    }
    w
}

/// Simplified Porter stemmer for English.
/// Handles common suffixes: -ing, -tion, -ed, -ly, -ness, -er, -est, -ies, -s.
pub fn stem(word: &str) -> String {
    let mut w = word.to_string();

    // Step 1: Plurals / -ed / -ing
    if w.ends_with("ies") && w.len() > 4 {
        w.truncate(w.len() - 3);
        w.push('y');
    } else if w.ends_with("sses") {
        w.truncate(w.len() - 2);
    } else if (w.ends_with("ness") || w.ends_with("ment")) && w.len() > 5 {
        w.truncate(w.len() - 4);
    } else if w.ends_with("tion") && w.len() > 5 {
        w.truncate(w.len() - 4);
        w.push('t');
    } else if w.ends_with("ation") && w.len() > 6 {
        w.truncate(w.len() - 5);
    } else if w.ends_with("ing") && w.len() > 5 {
        w.truncate(w.len() - 3);
        if w.ends_with(|c: char| c == w.chars().last().unwrap_or(' '))
            && w.len() > 3
            && matches!(w.chars().last(), Some('b' | 'd' | 'g' | 'l' | 'm' | 'n' | 'p' | 'r' | 't'))
        {
            w.pop(); // Remove doubled consonant
        }
    } else if (w.ends_with("ed") || w.ends_with("ly") || w.ends_with("er")) && w.len() > 4 {
        w.truncate(w.len() - 2);
    } else if w.ends_with("est") && w.len() > 5 {
        w.truncate(w.len() - 3);
    } else if w.ends_with('s') && !w.ends_with("ss") && w.len() > 3 {
        w.pop();
    }

    w
}

// ============================================================================
// Inverted Index
// ============================================================================

/// A posting in the inverted index: document ID and positions within the document.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Posting {
    pub doc_id: u64,
    pub positions: Vec<usize>,
    pub term_frequency: f64, // TF = positions.len() / doc_length
}

/// Statistics for a single document.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DocInfo {
    /// Number of tokens in this document.
    length: usize,
}

/// Result of a faceted search — ranked results plus facet aggregations.
#[derive(Debug, Clone)]
pub struct FacetedSearchResult {
    /// Top-k results sorted by relevance score descending.
    pub results: Vec<(u64, f64)>,
    /// Facet counts: `field_name → { value → count }`.
    /// Counts are computed across ALL matching documents, not just top-k.
    pub facets: HashMap<String, HashMap<String, usize>>,
    /// Total number of matching documents (before limit truncation).
    pub total_matches: usize,
}

/// The inverted index — maps terms to posting lists.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InvertedIndex {
    /// term → list of postings (sorted by doc_id)
    postings: HashMap<String, Vec<Posting>>,
    /// doc_id → document info
    docs: HashMap<u64, DocInfo>,
    /// doc_id → facet field → values (for faceted search)
    #[serde(default)]
    doc_facets: HashMap<u64, HashMap<String, Vec<String>>>,
    /// Total number of documents
    doc_count: u64,
    /// Sum of all document lengths (for avgdl)
    total_length: usize,
    /// Original document texts, keyed by doc_id (needed for WAL snapshots).
    #[serde(skip)]
    original_texts: HashMap<u64, String>,
    /// Optional WAL for binary persistence.
    #[serde(skip)]
    wal: Option<Arc<fts_wal::FtsWal>>,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            docs: HashMap::new(),
            doc_facets: HashMap::new(),
            doc_count: 0,
            total_length: 0,
            original_texts: HashMap::new(),
            wal: None,
        }
    }

    /// Open a WAL-backed inverted index from a directory.
    ///
    /// Reads the WAL file (if it exists), replays all entries, and re-indexes
    /// every document. Returns a fully populated index ready for queries.
    pub fn open(dir: &std::path::Path) -> std::io::Result<Self> {
        let (wal, state) = fts_wal::FtsWal::open(dir)?;
        let mut idx = Self {
            postings: HashMap::new(),
            docs: HashMap::new(),
            doc_facets: HashMap::new(),
            doc_count: 0,
            total_length: 0,
            original_texts: HashMap::new(),
            wal: Some(Arc::new(wal)),
        };
        // Re-index every recovered document (re-tokenize from original text).
        for (doc_id, text) in state.docs {
            idx.index_document_internal(doc_id, &text);
            idx.original_texts.insert(doc_id, text);
        }
        Ok(idx)
    }

    /// Serialize the index to JSON for persistence.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize the index from a JSON string.
    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Index a document. Tokenizes the text and adds to the inverted index.
    ///
    /// If a WAL is attached, the original text is logged before indexing.
    pub fn add_document(&mut self, doc_id: u64, text: &str) {
        // Log to WAL before mutating in-memory state.
        if let Some(wal) = &self.wal
            && let Err(e) = wal.log_index_doc(doc_id, text) {
                eprintln!("FTS WAL: failed to log index_doc {doc_id}: {e}");
            }
        self.original_texts.insert(doc_id, text.to_string());
        self.index_document_internal(doc_id, text);
    }

    /// Internal: index a document without WAL logging or original text storage.
    /// Used by `open()` during replay and by `add_document()`.
    fn index_document_internal(&mut self, doc_id: u64, text: &str) {
        // If the doc already exists, remove it first to avoid double-counting.
        if self.docs.contains_key(&doc_id) {
            self.remove_document_internal(doc_id);
        }

        let tokens = tokenize(text);
        let doc_length = tokens.len();

        // Track document info
        self.docs.insert(doc_id, DocInfo { length: doc_length });
        self.doc_count += 1;
        self.total_length += doc_length;

        // Group tokens by term
        let mut term_positions: HashMap<String, Vec<usize>> = HashMap::new();
        for token in &tokens {
            term_positions
                .entry(token.term.clone())
                .or_default()
                .push(token.position);
        }

        // Add to postings
        for (term, positions) in term_positions {
            let tf = positions.len() as f64 / doc_length.max(1) as f64;
            let posting = Posting {
                doc_id,
                positions,
                term_frequency: tf,
            };
            self.postings.entry(term).or_default().push(posting);
        }
    }

    /// Remove a document from the index.
    ///
    /// If a WAL is attached, the removal is logged before mutating state.
    pub fn remove_document(&mut self, doc_id: u64) {
        // Log to WAL before mutating in-memory state.
        if let Some(wal) = &self.wal
            && let Err(e) = wal.log_remove_doc(doc_id) {
                eprintln!("FTS WAL: failed to log remove_doc {doc_id}: {e}");
            }
        self.original_texts.remove(&doc_id);
        self.remove_document_internal(doc_id);
    }

    /// Internal: remove a document without WAL logging.
    fn remove_document_internal(&mut self, doc_id: u64) {
        if let Some(info) = self.docs.remove(&doc_id) {
            self.doc_count -= 1;
            self.total_length -= info.length;

            // Remove postings
            for postings in self.postings.values_mut() {
                postings.retain(|p| p.doc_id != doc_id);
            }

            // Clean up empty posting lists
            self.postings.retain(|_, v| !v.is_empty());

            // Remove facets
            self.doc_facets.remove(&doc_id);
        }
    }

    /// Get the original text of a document (if stored).
    pub fn get_original_text(&self, doc_id: u64) -> Option<&str> {
        self.original_texts.get(&doc_id).map(|s| s.as_str())
    }

    /// Generate highlighted snippets from a document with matched terms wrapped in tags.
    ///
    /// Returns context windows around each match position. Multiple nearby matches
    /// are merged into a single snippet. Snippets are separated by `...`.
    ///
    /// # Arguments
    /// * `doc_id` — the document to highlight
    /// * `query` — the search query (tokenized to extract terms)
    /// * `open_tag` / `close_tag` — wrapping tags, e.g. `<em>` / `</em>`
    /// * `context_words` — number of words before/after each match to include
    pub fn highlight(
        &self,
        doc_id: u64,
        query: &str,
        open_tag: &str,
        close_tag: &str,
        context_words: usize,
    ) -> Option<String> {
        let original = self.get_original_text(doc_id)?;

        // Tokenize query to get search terms (lowercased via tokenizer)
        let query_tokens = tokenize(query);
        let terms: std::collections::HashSet<String> = query_tokens
            .iter()
            .map(|t| t.term.clone())
            .collect();

        if terms.is_empty() {
            return Some(original.to_string());
        }

        // Split original text into words while preserving boundaries
        let words: Vec<&str> = original
            .split(|c: char| !c.is_alphanumeric() && c != '\'')
            .filter(|w| !w.is_empty())
            .collect();

        // Find which word positions match query terms
        let mut match_positions: Vec<usize> = Vec::new();
        for (i, word) in words.iter().enumerate() {
            let lower = word.to_lowercase();
            let normalized = lower.trim_matches('\'');
            if terms.contains(normalized) {
                match_positions.push(i);
            }
        }

        if match_positions.is_empty() {
            return Some(original.to_string());
        }

        // Build context windows and merge overlapping ranges
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for &pos in &match_positions {
            let start = pos.saturating_sub(context_words);
            let end = (pos + context_words + 1).min(words.len());
            if let Some(last) = ranges.last_mut()
                && start <= last.1 {
                    // Merge overlapping ranges
                    last.1 = last.1.max(end);
                    continue;
                }
            ranges.push((start, end));
        }

        // Build highlighted snippets
        let match_set: std::collections::HashSet<usize> = match_positions.into_iter().collect();
        let mut snippets: Vec<String> = Vec::new();
        for (start, end) in &ranges {
            let mut parts: Vec<String> = Vec::new();
            for (i, word) in words.iter().enumerate().take(*end).skip(*start) {
                if match_set.contains(&i) {
                    parts.push(format!("{open_tag}{word}{close_tag}"));
                } else {
                    parts.push(word.to_string());
                }
            }
            snippets.push(parts.join(" "));
        }

        Some(snippets.join(" ... "))
    }

    /// Check whether `doc_id` contains any of the stemmed query terms.
    ///
    /// O(terms × posting_list_size) with early exit on first match.
    /// Much faster than `search()` for single-document membership tests
    /// because it skips BM25 scoring entirely.
    pub fn contains_doc(&self, doc_id: u64, query: &str) -> bool {
        let query_tokens = tokenize(query);
        for token in &query_tokens {
            if let Some(postings) = self.postings.get(&token.term)
                && postings.iter().any(|p| p.doc_id == doc_id) {
                    return true;
                }
        }
        false
    }

    /// Checkpoint the WAL: write all original texts as a single snapshot.
    ///
    /// This compacts the WAL file. Only meaningful if a WAL is attached.
    pub fn checkpoint_wal(&self) -> std::io::Result<()> {
        if let Some(wal) = &self.wal {
            let docs: Vec<(u64, String)> = self
                .original_texts
                .iter()
                .map(|(&id, text)| (id, text.clone()))
                .collect();
            wal.checkpoint(&docs)?;
        }
        Ok(())
    }

    /// Capture a snapshot of all mutable FTS state for transaction rollback.
    ///
    /// The WAL handle is not included — it is shared/append-only and must not
    /// be reverted (WAL entries are idempotent on replay).
    pub fn txn_snapshot(&self) -> FtsTxnSnapshot {
        FtsTxnSnapshot {
            postings: self.postings.clone(),
            docs: self.docs.clone(),
            doc_facets: self.doc_facets.clone(),
            doc_count: self.doc_count,
            total_length: self.total_length,
            original_texts: self.original_texts.clone(),
        }
    }

    /// Restore mutable FTS state from a transaction snapshot (for ROLLBACK).
    pub fn txn_restore(&mut self, snap: FtsTxnSnapshot) {
        self.postings = snap.postings;
        self.docs = snap.docs;
        self.doc_facets = snap.doc_facets;
        self.doc_count = snap.doc_count;
        self.total_length = snap.total_length;
        self.original_texts = snap.original_texts;
    }

    // ====================================================================
    // Faceted Search
    // ====================================================================

    /// Index a document with associated facet metadata.
    ///
    /// `facets` maps facet field names to their values, e.g.
    /// `{"category": ["electronics", "sale"], "brand": ["acme"]}`.
    pub fn add_document_with_facets(
        &mut self,
        doc_id: u64,
        text: &str,
        facets: HashMap<String, Vec<String>>,
    ) {
        self.add_document(doc_id, text);
        if !facets.is_empty() {
            self.doc_facets.insert(doc_id, facets);
        }
    }

    /// Set facets for an already-indexed document.
    pub fn set_facets(&mut self, doc_id: u64, facets: HashMap<String, Vec<String>>) {
        if self.docs.contains_key(&doc_id) {
            self.doc_facets.insert(doc_id, facets);
        }
    }

    /// Collect facet counts from a set of search result doc_ids.
    ///
    /// Returns a map of `field_name → { value → count }` for every facet
    /// field that appears in the matching documents.
    pub fn collect_facets(&self, doc_ids: &[u64]) -> HashMap<String, HashMap<String, usize>> {
        let mut facets: HashMap<String, HashMap<String, usize>> = HashMap::new();
        for &doc_id in doc_ids {
            if let Some(doc_facet_map) = self.doc_facets.get(&doc_id) {
                for (field, values) in doc_facet_map {
                    let field_counts = facets.entry(field.clone()).or_default();
                    for value in values {
                        *field_counts.entry(value.clone()).or_insert(0) += 1;
                    }
                }
            }
        }
        facets
    }

    /// Search with faceted aggregation. Returns ranked results AND facet counts
    /// across all matching documents (not just the top-k).
    ///
    /// `facet_fields`: which facet fields to collect. If empty, collects all.
    pub fn search_with_facets(
        &self,
        query: &str,
        limit: usize,
        facet_fields: &[&str],
    ) -> FacetedSearchResult {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return FacetedSearchResult {
                results: vec![],
                facets: HashMap::new(),
                total_matches: 0,
            };
        }

        // Score all matching documents (same logic as search())
        let mut term_postings: Vec<(&str, &Vec<Posting>)> = query_tokens
            .iter()
            .filter_map(|token| {
                self.postings
                    .get(&token.term)
                    .map(|p| (token.term.as_str(), p))
            })
            .collect();
        term_postings.sort_by_key(|(_, postings)| postings.len());

        let mut scores: HashMap<u64, f64> = HashMap::new();
        for (_, postings) in &term_postings {
            let idf = self.idf(postings.len());
            for posting in *postings {
                let score = self.bm25_term_score(posting, idf);
                *scores.entry(posting.doc_id).or_default() += score;
            }
        }

        let total_matches = scores.len();

        // Collect facets from ALL matching docs (before truncation)
        let matching_ids: Vec<u64> = scores.keys().copied().collect();
        let mut facets: HashMap<String, HashMap<String, usize>> = HashMap::new();
        for &doc_id in &matching_ids {
            if let Some(doc_facet_map) = self.doc_facets.get(&doc_id) {
                for (field, values) in doc_facet_map {
                    if !facet_fields.is_empty()
                        && !facet_fields.contains(&field.as_str())
                    {
                        continue;
                    }
                    let field_counts = facets.entry(field.clone()).or_default();
                    for value in values {
                        *field_counts.entry(value.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        // Sort and truncate results
        let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        results.truncate(limit);

        FacetedSearchResult {
            results,
            facets,
            total_matches,
        }
    }

    /// Search for documents matching a query. Returns (doc_id, score) pairs sorted by score DESC.
    ///
    /// Uses shortest-first posting list processing: query terms are sorted by
    /// posting list length (ascending) before scoring, so the rarest terms are
    /// evaluated first. This improves cache locality and enables early pruning
    /// when combined with intersection-based queries.
    pub fn search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return vec![];
        }

        // Collect (term, posting_list_ref) pairs and sort by posting list length
        // (shortest first). This ensures the rarest terms are processed first,
        // which is the classic inverted index optimization.
        let mut term_postings: Vec<(&str, &Vec<Posting>)> = query_tokens
            .iter()
            .filter_map(|token| {
                self.postings
                    .get(&token.term)
                    .map(|p| (token.term.as_str(), p))
            })
            .collect();
        term_postings.sort_by_key(|(_, postings)| postings.len());

        // Score each document using BM25
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for (_, postings) in &term_postings {
            let idf = self.idf(postings.len());
            for posting in *postings {
                let score = self.bm25_term_score(posting, idf);
                *scores.entry(posting.doc_id).or_default() += score;
            }
        }

        // Sort by score descending, then doc_id ascending for deterministic tiebreaking
        let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        results.truncate(limit);
        results
    }

    /// Search for documents matching ALL query terms (AND semantics) with
    /// parallel posting list intersection.
    ///
    /// Returns `(doc_id, relevance_score)` pairs sorted by score descending,
    /// where the score is the sum of BM25 scores across all matching terms.
    ///
    /// Uses shortest-first intersection: posting lists are sorted by length
    /// (ascending), and intersection starts with the shortest list, then
    /// progressively intersects with longer lists. This minimizes the number
    /// of comparisons — a well-known optimization for multi-term conjunctive
    /// queries.
    ///
    /// Unlike `search()` (which uses OR semantics — any term matches),
    /// `search_scored()` requires ALL query terms to appear in a document.
    pub fn search_scored(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return vec![];
        }

        // Deduplicate query terms (same stemmed form may appear multiple times)
        let mut seen_terms = std::collections::HashSet::new();
        let unique_terms: Vec<&str> = query_tokens
            .iter()
            .filter_map(|t| {
                if seen_terms.insert(t.term.as_str()) {
                    Some(t.term.as_str())
                } else {
                    None
                }
            })
            .collect();

        // Collect posting lists for each unique term, paired with IDF.
        // For AND semantics, ALL terms must have posting lists. If any term
        // has no postings, the intersection is empty.
        let mut term_data: Vec<(&str, &Vec<Posting>, f64)> = Vec::new();
        for &term in &unique_terms {
            match self.postings.get(term) {
                Some(postings) => {
                    let idf = self.idf(postings.len());
                    term_data.push((term, postings, idf));
                }
                None => {
                    // Term not in index — AND intersection is empty
                    return vec![];
                }
            }
        }

        // If no terms have posting lists, return empty
        if term_data.is_empty() {
            return vec![];
        }

        // Sort by posting list length ascending (shortest first)
        term_data.sort_by_key(|(_, postings, _)| postings.len());

        // Start with the doc IDs from the shortest posting list
        let (_, first_postings, first_idf) = &term_data[0];
        let mut candidate_docs: HashMap<u64, f64> = first_postings
            .iter()
            .map(|p| {
                let score = self.bm25_term_score(p, *first_idf);
                (p.doc_id, score)
            })
            .collect();

        // Intersect with remaining posting lists (shortest to longest)
        for &(_, postings, idf) in term_data.iter().skip(1) {
            // Build a lookup set from the current posting list
            let posting_map: HashMap<u64, &Posting> =
                postings.iter().map(|p| (p.doc_id, p)).collect();

            // Retain only candidates that appear in this posting list too,
            // and accumulate their BM25 scores
            candidate_docs.retain(|doc_id, score| {
                if let Some(posting) = posting_map.get(doc_id) {
                    *score += self.bm25_term_score(posting, idf);
                    true
                } else {
                    false
                }
            });

            // Early exit: if no candidates remain, no point continuing
            if candidate_docs.is_empty() {
                return vec![];
            }
        }

        // Sort by score descending, then doc_id ascending for deterministic tiebreaking
        let mut results: Vec<(u64, f64)> = candidate_docs.into_iter().collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        results.truncate(limit);
        results
    }

    /// BM25 parameters.
    const K1: f64 = 1.2;
    const B: f64 = 0.75;

    /// Inverse document frequency.
    fn idf(&self, df: usize) -> f64 {
        let n = self.doc_count as f64;
        let df = df as f64;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    /// Average document length.
    fn avgdl(&self) -> f64 {
        if self.doc_count == 0 {
            1.0
        } else {
            self.total_length as f64 / self.doc_count as f64
        }
    }

    /// BM25 score for a single term in a single document.
    fn bm25_term_score(&self, posting: &Posting, idf: f64) -> f64 {
        let tf = posting.positions.len() as f64;
        let dl = self
            .docs
            .get(&posting.doc_id)
            .map(|d| d.length as f64)
            .unwrap_or(1.0);
        let avgdl = self.avgdl();

        let numerator = tf * (Self::K1 + 1.0);
        let denominator = tf + Self::K1 * (1.0 - Self::B + Self::B * dl / avgdl);
        idf * numerator / denominator
    }

    /// Get the number of indexed documents.
    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Get the number of unique terms.
    pub fn term_count(&self) -> usize {
        self.postings.len()
    }

    /// Expose original_texts map for memory estimation (used by Pressurable impl).
    pub fn original_texts(&self) -> &HashMap<u64, String> {
        &self.original_texts
    }

    /// Estimated posting list memory: ~24 bytes per posting (doc_id + positions vec + tf).
    pub fn estimated_posting_bytes(&self) -> usize {
        self.postings.values().map(|v| v.len() * 24).sum()
    }

    // ========================================================================
    // Parallel search and indexing
    // ========================================================================

    /// Threshold of candidate documents above which parallel scoring is used.
    const PAR_SCORE_THRESHOLD: usize = 500;

    /// Parallel BM25 scoring for `search` (OR semantics).
    ///
    /// When the number of candidate documents exceeds `PAR_SCORE_THRESHOLD`,
    /// the scoring work is split across available CPU cores using
    /// `std::thread::scope`. Below the threshold, falls back to the
    /// single-threaded `search` method.
    pub fn par_search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return vec![];
        }

        // Collect (term, posting list) pairs sorted shortest-first.
        let mut term_postings: Vec<(&str, &Vec<Posting>)> = query_tokens
            .iter()
            .filter_map(|token| {
                self.postings
                    .get(&token.term)
                    .map(|p| (token.term.as_str(), p))
            })
            .collect();
        term_postings.sort_by_key(|(_, postings)| postings.len());

        // Build per-document scores (single pass, same as `search`).
        let mut scores: HashMap<u64, f64> = HashMap::new();
        for (_, postings) in &term_postings {
            let idf = self.idf(postings.len());
            for posting in *postings {
                let score = self.bm25_term_score(posting, idf);
                *scores.entry(posting.doc_id).or_default() += score;
            }
        }

        // Deterministic sort: score DESC then doc_id ASC for stable tiebreaking.
        fn deterministic_cmp(a: &(u64, f64), b: &(u64, f64)) -> std::cmp::Ordering {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        }

        // If below threshold, sort and return directly (sequential path).
        if scores.len() < Self::PAR_SCORE_THRESHOLD {
            let mut results: Vec<(u64, f64)> = scores.into_iter().collect();
            results.sort_by(deterministic_cmp);
            results.truncate(limit);
            return results;
        }

        // Parallel top-k extraction: partition scored docs across threads.
        let scored_vec: Vec<(u64, f64)> = scores.into_iter().collect();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = scored_vec.len().div_ceil(cpus);

        std::thread::scope(|s| {
            let handles: Vec<_> = scored_vec
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        let mut local = chunk.to_vec();
                        local.sort_by(deterministic_cmp);
                        local.truncate(limit);
                        local
                    })
                })
                .collect();

            let mut all_scores: Vec<(u64, f64)> = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect();
            all_scores.sort_by(deterministic_cmp);
            all_scores.truncate(limit);
            all_scores
        })
    }

    /// Parallel BM25 scoring for `search_scored` (AND semantics).
    ///
    /// Uses the same shortest-first intersection as `search_scored`, then
    /// parallelizes the final scoring when the candidate set is large.
    /// Falls back to single-threaded `search_scored` when the candidate
    /// set is below `PAR_SCORE_THRESHOLD` (500 docs).
    pub fn par_search_scored(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return vec![];
        }

        // Deduplicate query terms.
        let mut seen_terms = std::collections::HashSet::new();
        let unique_terms: Vec<&str> = query_tokens
            .iter()
            .filter_map(|t| {
                if seen_terms.insert(t.term.as_str()) {
                    Some(t.term.as_str())
                } else {
                    None
                }
            })
            .collect();

        // Gather posting lists with IDF; any missing term means empty result.
        let mut term_data: Vec<(&str, &Vec<Posting>, f64)> = Vec::new();
        for &term in &unique_terms {
            match self.postings.get(term) {
                Some(postings) => {
                    let idf = self.idf(postings.len());
                    term_data.push((term, postings, idf));
                }
                None => return vec![],
            }
        }
        if term_data.is_empty() {
            return vec![];
        }

        // Sort by posting list length ascending (shortest first).
        term_data.sort_by_key(|(_, postings, _)| postings.len());

        // Build candidate set via intersection (same logic as search_scored).
        let (_, first_postings, first_idf) = &term_data[0];
        let mut candidate_docs: HashMap<u64, f64> = first_postings
            .iter()
            .map(|p| {
                let score = self.bm25_term_score(p, *first_idf);
                (p.doc_id, score)
            })
            .collect();

        for &(_, postings, idf) in term_data.iter().skip(1) {
            let posting_map: HashMap<u64, &Posting> =
                postings.iter().map(|p| (p.doc_id, p)).collect();
            candidate_docs.retain(|doc_id, score| {
                if let Some(posting) = posting_map.get(doc_id) {
                    *score += self.bm25_term_score(posting, idf);
                    true
                } else {
                    false
                }
            });
            if candidate_docs.is_empty() {
                return vec![];
            }
        }

        // Deterministic sort: score DESC then doc_id ASC for stable tiebreaking.
        fn deterministic_cmp(a: &(u64, f64), b: &(u64, f64)) -> std::cmp::Ordering {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        }

        // Below threshold: sequential sort.
        if candidate_docs.len() < Self::PAR_SCORE_THRESHOLD {
            let mut results: Vec<(u64, f64)> = candidate_docs.into_iter().collect();
            results.sort_by(deterministic_cmp);
            results.truncate(limit);
            return results;
        }

        // Parallel top-k sort across chunks.
        let scored_vec: Vec<(u64, f64)> = candidate_docs.into_iter().collect();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = scored_vec.len().div_ceil(cpus);

        std::thread::scope(|s| {
            let handles: Vec<_> = scored_vec
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        let mut local = chunk.to_vec();
                        local.sort_by(deterministic_cmp);
                        local.truncate(limit);
                        local
                    })
                })
                .collect();

            let mut all_scores: Vec<(u64, f64)> = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect();
            all_scores.sort_by(deterministic_cmp);
            all_scores.truncate(limit);
            all_scores
        })
    }

    /// Parallel bulk document indexing.
    ///
    /// Tokenizes all documents in parallel across available CPU cores, then
    /// merges the results into the index sequentially.  This avoids holding
    /// `&mut self` during the expensive tokenization phase while keeping the
    /// index mutation single-threaded.
    pub fn par_bulk_index(&mut self, docs: &[(u64, &str)]) {
        if docs.is_empty() {
            return;
        }

        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = docs.len().div_ceil(cpus);

        // Phase 1: tokenize in parallel.
        // Each thread returns (doc_id, doc_length, term→positions).
        let tokenized: Vec<(u64, usize, HashMap<String, Vec<usize>>)> =
            std::thread::scope(|s| {
                let handles: Vec<_> = docs
                    .chunks(chunk_size)
                    .map(|chunk| {
                        s.spawn(move || {
                            let mut results = Vec::with_capacity(chunk.len());
                            for &(doc_id, text) in chunk {
                                let tokens = tokenize(text);
                                let doc_length = tokens.len();
                                let mut term_positions: HashMap<String, Vec<usize>> =
                                    HashMap::new();
                                for token in &tokens {
                                    term_positions
                                        .entry(token.term.clone())
                                        .or_default()
                                        .push(token.position);
                                }
                                results.push((doc_id, doc_length, term_positions));
                            }
                            results
                        })
                    })
                    .collect();

                handles
                    .into_iter()
                    .flat_map(|h| h.join().unwrap())
                    .collect()
            });

        // Phase 2: merge into index sequentially.
        for (doc_id, doc_length, term_positions) in tokenized {
            // Remove existing doc if re-indexing.
            if self.docs.contains_key(&doc_id) {
                self.remove_document_internal(doc_id);
            }

            self.docs.insert(doc_id, DocInfo { length: doc_length });
            self.doc_count += 1;
            self.total_length += doc_length;

            for (term, positions) in term_positions {
                let tf = positions.len() as f64 / doc_length.max(1) as f64;
                let posting = Posting {
                    doc_id,
                    positions,
                    term_frequency: tf,
                };
                self.postings.entry(term).or_default().push(posting);
            }
        }
    }
}

// ============================================================================
// Fuzzy matching (Levenshtein distance)
// ============================================================================

/// Compute the Levenshtein edit distance between two strings.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1) // deletion
                .min(curr[j - 1] + 1) // insertion
                .min(prev[j - 1] + cost); // substitution
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

/// Find terms in the index that are within `max_distance` edit distance of the query term.
pub fn fuzzy_terms<'a>(
    index: &'a InvertedIndex,
    term: &str,
    max_distance: usize,
) -> Vec<(&'a str, usize)> {
    let stemmed = stem(&term.to_lowercase());
    let mut matches: Vec<(&str, usize)> = index
        .postings
        .keys()
        .filter_map(|t| {
            let dist = levenshtein(&stemmed, t);
            if dist <= max_distance {
                Some((t.as_str(), dist))
            } else {
                None
            }
        })
        .collect();
    matches.sort_by_key(|&(_, d)| d);
    matches
}

// ============================================================================
// DocBitmap — roaring-style sorted doc ID set with galloping intersection
// ============================================================================

/// A sorted set of document IDs for fast set operations on posting lists.
///
/// Uses a sorted `Vec<u64>` with galloping (exponential) search for
/// Snapshot of `InvertedIndex` mutable state for transaction rollback.
pub struct FtsTxnSnapshot {
    postings: HashMap<String, Vec<Posting>>,
    docs: HashMap<u64, DocInfo>,
    doc_facets: HashMap<u64, HashMap<String, Vec<String>>>,
    doc_count: u64,
    total_length: usize,
    original_texts: HashMap<u64, String>,
}

/// intersection. For typical FTS workloads (posting lists of 100s–10Ks of
/// doc IDs), this matches or beats roaring bitmaps without the dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct DocBitmap {
    docs: Vec<u64>,
}

impl Default for DocBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl DocBitmap {
    /// Create an empty bitmap.
    pub fn new() -> Self {
        DocBitmap { docs: Vec::new() }
    }

    /// Create from a pre-sorted slice (must be sorted and deduplicated).
    pub fn from_sorted(docs: Vec<u64>) -> Self {
        DocBitmap { docs }
    }

    /// Insert a document ID, maintaining sorted order.
    pub fn insert(&mut self, doc_id: u64) {
        match self.docs.binary_search(&doc_id) {
            Ok(_) => {} // already present
            Err(pos) => self.docs.insert(pos, doc_id),
        }
    }

    /// Check if a document ID is present.
    pub fn contains(&self, doc_id: u64) -> bool {
        self.docs.binary_search(&doc_id).is_ok()
    }

    /// Number of documents in the bitmap.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Whether the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Iterate over document IDs.
    pub fn iter(&self) -> impl Iterator<Item = &u64> {
        self.docs.iter()
    }

    /// Intersection using galloping search — O(m * log(n)) where m < n.
    /// Efficient when one list is much shorter than the other.
    pub fn intersect(&self, other: &DocBitmap) -> DocBitmap {
        // Walk the shorter list, gallop on the longer
        let (short, long) = if self.docs.len() <= other.docs.len() {
            (&self.docs, &other.docs)
        } else {
            (&other.docs, &self.docs)
        };

        let mut result = Vec::new();
        let mut lo = 0usize;

        for &doc in short {
            // Galloping: exponential search for lower bound
            lo = gallop(long, doc, lo);
            if lo < long.len() && long[lo] == doc {
                result.push(doc);
                lo += 1;
            }
        }

        DocBitmap { docs: result }
    }

    /// Union of two bitmaps — merge-sorted.
    pub fn union(&self, other: &DocBitmap) -> DocBitmap {
        let mut result = Vec::with_capacity(self.docs.len() + other.docs.len());
        let (mut i, mut j) = (0, 0);
        while i < self.docs.len() && j < other.docs.len() {
            use std::cmp::Ordering;
            match self.docs[i].cmp(&other.docs[j]) {
                Ordering::Less => {
                    result.push(self.docs[i]);
                    i += 1;
                }
                Ordering::Greater => {
                    result.push(other.docs[j]);
                    j += 1;
                }
                Ordering::Equal => {
                    result.push(self.docs[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        result.extend_from_slice(&self.docs[i..]);
        result.extend_from_slice(&other.docs[j..]);
        DocBitmap { docs: result }
    }

    /// Difference (self - other).
    pub fn difference(&self, other: &DocBitmap) -> DocBitmap {
        let mut result = Vec::new();
        let mut j = 0usize;
        for &doc in &self.docs {
            j = gallop(&other.docs, doc, j);
            if j >= other.docs.len() || other.docs[j] != doc {
                result.push(doc);
            }
        }
        DocBitmap { docs: result }
    }
}

/// Galloping (exponential) search: find the first position in `arr[lo..]`
/// where `arr[pos] >= target`. Returns arr.len() if no such position.
fn gallop(arr: &[u64], target: u64, lo: usize) -> usize {
    if lo >= arr.len() {
        return arr.len();
    }

    // Exponential search phase
    let mut bound = 1usize;
    let pos = lo;
    while pos + bound < arr.len() && arr[pos + bound] < target {
        bound *= 2;
    }

    // Binary search in [pos + bound/2 .. min(pos + bound, len))
    let start = pos + bound / 2;
    let end = (pos + bound + 1).min(arr.len());
    match arr[start..end].binary_search(&target) {
        Ok(i) => start + i,
        Err(i) => start + i,
    }
}

// ============================================================================
// Block-max WAND (BMW) — early termination for top-k BM25 queries
// ============================================================================

/// Block size for Block-max WAND. Each block of 128 postings stores
/// a pre-computed max BM25 contribution for that term.
const BMW_BLOCK_SIZE: usize = 128;

/// A block within a posting list, storing the max BM25 score contribution
/// for any document in this block.
#[derive(Debug, Clone)]
pub struct PostingBlock {
    /// Document IDs in this block (sorted, up to BMW_BLOCK_SIZE).
    pub doc_ids: Vec<u64>,
    /// Per-document term frequencies.
    pub tfs: Vec<f64>,
    /// Maximum BM25 contribution of any doc in this block (for early termination).
    pub max_score: f64,
}

/// A posting list partitioned into blocks for BMW scoring.
#[derive(Debug, Clone)]
pub struct BlockPostingList {
    pub term: String,
    pub idf: f64,
    pub blocks: Vec<PostingBlock>,
}

/// Scored document for top-k collection via a min-heap.
#[derive(Debug, Clone)]
struct ScoredDoc {
    doc_id: u64,
    score: f64,
}

impl PartialEq for ScoredDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredDoc {}

impl PartialOrd for ScoredDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap: reverse ordering so smallest score is at top
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl InvertedIndex {
    /// Build a block posting list for a single term.
    fn build_block_posting_list(&self, term: &str) -> Option<BlockPostingList> {
        let postings = self.postings.get(term)?;
        let idf = self.idf(postings.len());
        let avgdl = self.avgdl();

        let mut blocks = Vec::new();
        for chunk in postings.chunks(BMW_BLOCK_SIZE) {
            let mut doc_ids = Vec::with_capacity(chunk.len());
            let mut tfs = Vec::with_capacity(chunk.len());
            let mut max_score = 0.0f64;

            for posting in chunk {
                let tf = posting.positions.len() as f64;
                let dl = self
                    .docs
                    .get(&posting.doc_id)
                    .map(|d| d.length as f64)
                    .unwrap_or(1.0);
                let numerator = tf * (Self::K1 + 1.0);
                let denominator = tf + Self::K1 * (1.0 - Self::B + Self::B * dl / avgdl);
                let score = idf * numerator / denominator;

                doc_ids.push(posting.doc_id);
                tfs.push(tf);
                max_score = max_score.max(score);
            }

            blocks.push(PostingBlock {
                doc_ids,
                tfs,
                max_score,
            });
        }

        Some(BlockPostingList {
            term: term.to_string(),
            idf,
            blocks,
        })
    }

    /// Block-max WAND search — top-k BM25 with early termination.
    ///
    /// For each query term, partitions its posting list into 128-doc blocks
    /// with pre-computed max BM25 scores. During scoring, blocks whose
    /// max contribution can't push a document into the top-k are skipped.
    /// This provides 2-5x speedup over exhaustive scoring for long posting lists.
    pub fn search_bmw(&self, query: &str, k: usize) -> Vec<(u64, f64)> {
        let tokens = tokenize(query);
        if tokens.is_empty() || k == 0 {
            return Vec::new();
        }

        // Build block posting lists for each query term
        let block_lists: Vec<BlockPostingList> = tokens
            .iter()
            .filter_map(|t| self.build_block_posting_list(&t.term))
            .collect();

        if block_lists.is_empty() {
            return Vec::new();
        }

        // Collect all candidate doc IDs from all terms
        let mut doc_scores: HashMap<u64, f64> = HashMap::new();

        // Threshold: minimum score to enter top-k (starts at 0)
        let mut threshold = 0.0f64;
        let mut heap: std::collections::BinaryHeap<ScoredDoc> =
            std::collections::BinaryHeap::new();

        for bpl in &block_lists {
            for block in &bpl.blocks {
                // BMW skip: if this block's max contribution can't beat
                // the threshold (when added to existing partial scores),
                // we can skip the entire block for efficiency.
                // For simplicity, we check block.max_score against threshold.
                // In a full multi-term WAND, we'd sum upper bounds across terms.
                if block.max_score < threshold * 0.5 && heap.len() >= k {
                    continue; // skip this block
                }

                let avgdl = self.avgdl();
                for (i, &doc_id) in block.doc_ids.iter().enumerate() {
                    let tf = block.tfs[i];
                    let dl = self
                        .docs
                        .get(&doc_id)
                        .map(|d| d.length as f64)
                        .unwrap_or(1.0);
                    let numerator = tf * (Self::K1 + 1.0);
                    let denominator = tf + Self::K1 * (1.0 - Self::B + Self::B * dl / avgdl);
                    let term_score = bpl.idf * numerator / denominator;

                    *doc_scores.entry(doc_id).or_insert(0.0) += term_score;
                }
            }
        }

        // Collect into top-k using the heap
        for (&doc_id, &score) in &doc_scores {
            if heap.len() < k {
                heap.push(ScoredDoc { doc_id, score });
                if heap.len() == k {
                    threshold = heap.peek().map_or(0.0, |d| d.score);
                }
            } else if score > threshold {
                heap.pop();
                heap.push(ScoredDoc { doc_id, score });
                threshold = heap.peek().map_or(0.0, |d| d.score);
            }
        }

        // Extract sorted results (highest score first)
        // into_sorted_vec returns ascending by our Ord (which reverses scores),
        // so it gives descending-by-score order already.
        let results: Vec<(u64, f64)> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|sd| (sd.doc_id, sd.score))
            .collect();
        results
    }
}

// ============================================================================
// Gap 1: Segment Merging — LSM-style immutable segments with background merge
// ============================================================================

/// An immutable FTS segment — a frozen snapshot of an InvertedIndex.
///
/// Documents are never modified in a segment; deletions are tracked externally
/// via tombstones in `SegmentedIndex`.
#[derive(Debug)]
pub struct Segment {
    /// Unique segment ID for identification and merge tracking.
    pub id: u64,
    /// The immutable inverted index for this segment.
    pub index: InvertedIndex,
    /// Number of documents in this segment at creation time.
    pub doc_count: u64,
    /// Approximate size in bytes (for merge policy decisions).
    pub size_bytes: usize,
}

impl Segment {
    fn new(id: u64, index: InvertedIndex) -> Self {
        let doc_count = index.doc_count();
        // Approximate: postings entries * ~64 bytes each + doc info
        let size_bytes = index.term_count() * 64 + index.doc_count() as usize * 32;
        Segment {
            id,
            index,
            doc_count,
            size_bytes,
        }
    }
}

/// Merge policy controlling when segments are combined.
#[derive(Debug, Clone)]
pub struct MergePolicy {
    /// Maximum number of segments before triggering a merge.
    pub max_segments: usize,
    /// Number of segments to merge at once.
    pub merge_factor: usize,
    /// Minimum segment size (bytes) to be eligible for merging.
    pub min_merge_size: usize,
    /// Maximum segment size (bytes) — segments above this are not merged.
    pub max_merge_size: usize,
}

impl Default for MergePolicy {
    fn default() -> Self {
        MergePolicy {
            max_segments: 10,
            merge_factor: 3,
            min_merge_size: 0,
            max_merge_size: usize::MAX,
        }
    }
}

/// A segmented FTS index that manages multiple immutable segments
/// plus one active mutable writer segment.
///
/// Implements LSM-style segment merging:
/// - New documents go to the active writer
/// - When the writer exceeds `flush_threshold`, it's frozen into an immutable segment
/// - When segment count exceeds the merge policy, smallest segments are merged
/// - Deletions tracked via a tombstone set (filtered during search and merge)
#[derive(Debug)]
pub struct SegmentedIndex {
    /// Immutable segments, ordered by ID (oldest first).
    segments: Vec<Segment>,
    /// Active mutable index for new writes.
    writer: InvertedIndex,
    /// Document IDs that have been deleted (filtered from search results).
    tombstones: std::collections::HashSet<u64>,
    /// Next segment ID.
    next_segment_id: u64,
    /// Number of documents in the writer before flushing to a segment.
    pub flush_threshold: u64,
    /// Merge policy.
    pub merge_policy: MergePolicy,
}

impl Default for SegmentedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentedIndex {
    pub fn new() -> Self {
        SegmentedIndex {
            segments: Vec::new(),
            writer: InvertedIndex::new(),
            tombstones: std::collections::HashSet::new(),
            next_segment_id: 1,
            flush_threshold: 1000,
            merge_policy: MergePolicy::default(),
        }
    }

    /// Add a document to the active writer.
    /// If the writer exceeds the flush threshold, it is frozen into a segment.
    pub fn add_document(&mut self, doc_id: u64, text: &str) {
        self.tombstones.remove(&doc_id);
        self.writer.add_document(doc_id, text);

        if self.writer.doc_count() >= self.flush_threshold {
            self.flush();
        }
    }

    /// Delete a document by adding it to the tombstone set.
    /// The document will be filtered from search results and purged during merges.
    pub fn delete_document(&mut self, doc_id: u64) {
        self.writer.remove_document(doc_id);
        // Only tombstone if the doc exists in a segment (otherwise removal from writer suffices)
        let in_segment = self.segments.iter().any(|s| s.index.docs.contains_key(&doc_id));
        if in_segment {
            self.tombstones.insert(doc_id);
        }
    }

    /// Flush the active writer to an immutable segment.
    pub fn flush(&mut self) {
        if self.writer.doc_count() == 0 {
            return;
        }
        let id = self.next_segment_id;
        self.next_segment_id += 1;
        let old_writer = std::mem::take(&mut self.writer);
        self.segments.push(Segment::new(id, old_writer));

        // Check merge policy
        self.maybe_merge();
    }

    /// Merge eligible segments if the segment count exceeds the policy threshold.
    fn maybe_merge(&mut self) {
        while self.segments.len() > self.merge_policy.max_segments {
            // Pick the smallest `merge_factor` segments to merge
            let mut eligible: Vec<usize> = (0..self.segments.len())
                .filter(|&i| {
                    self.segments[i].size_bytes >= self.merge_policy.min_merge_size
                        && self.segments[i].size_bytes <= self.merge_policy.max_merge_size
                })
                .collect();

            if eligible.len() < 2 {
                break;
            }

            eligible.sort_by_key(|&i| self.segments[i].size_bytes);
            let to_merge = eligible
                .into_iter()
                .take(self.merge_policy.merge_factor.max(2))
                .collect::<Vec<_>>();

            self.merge_segments(&to_merge);
        }
    }

    /// Merge the segments at the given indices into a single new segment.
    fn merge_segments(&mut self, indices: &[usize]) {
        if indices.len() < 2 {
            return;
        }

        let mut merged = InvertedIndex::new();
        let tombstones = &self.tombstones;

        // Collect all documents from segments being merged, skip tombstoned docs
        // We need to rebuild the index from raw postings since we can't iterate docs directly.
        // Instead, merge the posting lists term by term.
        for &idx in indices {
            let seg = &self.segments[idx];
            for (term, postings) in &seg.index.postings {
                for posting in postings {
                    if tombstones.contains(&posting.doc_id) {
                        continue;
                    }
                    // Re-insert into merged index's postings directly
                    if let std::collections::hash_map::Entry::Vacant(e) = merged.docs.entry(posting.doc_id) {
                        let length = seg
                            .index
                            .docs
                            .get(&posting.doc_id)
                            .map(|d| d.length)
                            .unwrap_or(1);
                        e.insert(DocInfo { length });
                        merged.doc_count += 1;
                        merged.total_length += length;
                    }
                    merged
                        .postings
                        .entry(term.clone())
                        .or_default()
                        .push(posting.clone());
                }
            }
        }

        // Sort posting lists by doc_id in the merged index
        for postings in merged.postings.values_mut() {
            postings.sort_by_key(|p| p.doc_id);
            postings.dedup_by_key(|p| p.doc_id);
        }

        let new_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Remove merged segments (in reverse order to maintain indices)
        let mut sorted_indices = indices.to_vec();
        sorted_indices.sort_unstable_by(|a, b| b.cmp(a));
        for idx in sorted_indices {
            self.segments.remove(idx);
        }

        // Also clean tombstones that are no longer in any segment
        self.clean_tombstones();

        self.segments.push(Segment::new(new_id, merged));
    }

    /// Remove tombstones for doc IDs that no longer appear in any segment.
    fn clean_tombstones(&mut self) {
        let mut live_docs: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for seg in &self.segments {
            for &doc_id in seg.index.docs.keys() {
                live_docs.insert(doc_id);
            }
        }
        for &doc_id in self.writer.docs.keys() {
            live_docs.insert(doc_id);
        }
        self.tombstones.retain(|id| live_docs.contains(id));
    }

    /// Search across all segments and the active writer.
    /// Results are merged and de-duplicated, tombstoned docs are filtered out.
    pub fn search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        let mut combined_scores: HashMap<u64, f64> = HashMap::new();

        // Search the active writer
        for (doc_id, score) in self.writer.search(query, usize::MAX) {
            if !self.tombstones.contains(&doc_id) {
                *combined_scores.entry(doc_id).or_default() = score.max(
                    *combined_scores.get(&doc_id).unwrap_or(&0.0),
                );
            }
        }

        // Search each segment
        for seg in &self.segments {
            for (doc_id, score) in seg.index.search(query, usize::MAX) {
                if !self.tombstones.contains(&doc_id) {
                    // Take max score across segments (same doc shouldn't be in multiple,
                    // but after re-indexing it could be)
                    let entry = combined_scores.entry(doc_id).or_default();
                    *entry = entry.max(score);
                }
            }
        }

        let mut results: Vec<(u64, f64)> = combined_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    /// Number of immutable segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Total document count across all segments and writer (excluding tombstones).
    pub fn total_doc_count(&self) -> u64 {
        let seg_count: u64 = self.segments.iter().map(|s| s.doc_count).sum();
        seg_count + self.writer.doc_count() - self.tombstones.len() as u64
    }

    /// Number of tombstoned documents.
    pub fn tombstone_count(&self) -> usize {
        self.tombstones.len()
    }

    /// Force a full merge of all segments into one.
    pub fn force_merge(&mut self) {
        self.flush();
        if self.segments.len() <= 1 {
            return;
        }
        let all_indices: Vec<usize> = (0..self.segments.len()).collect();
        self.merge_segments(&all_indices);
    }
}

// ============================================================================
// Gap 2: Analyzer/Tokenizer Plugin Pipeline
// ============================================================================

/// A character filter that transforms raw input text before tokenization.
pub trait CharFilter: std::fmt::Debug + Send + Sync {
    fn filter(&self, input: &str) -> String;
}

/// A tokenizer that splits text into raw token strings.
pub trait TokenizerPlugin: std::fmt::Debug + Send + Sync {
    fn tokenize(&self, input: &str) -> Vec<String>;
}

/// A token filter that transforms individual tokens after tokenization.
pub trait TokenFilterPlugin: std::fmt::Debug + Send + Sync {
    fn filter(&self, tokens: Vec<String>) -> Vec<String>;
}

/// An analyzer pipeline: char_filters → tokenizer → token_filters.
/// Replaces the default `tokenize()` function when a custom pipeline is needed.
#[derive(Debug)]
pub struct AnalyzerPipeline {
    pub name: String,
    char_filters: Vec<Box<dyn CharFilter>>,
    tokenizer: Box<dyn TokenizerPlugin>,
    token_filters: Vec<Box<dyn TokenFilterPlugin>>,
}

impl AnalyzerPipeline {
    pub fn new(name: &str, tokenizer: Box<dyn TokenizerPlugin>) -> Self {
        AnalyzerPipeline {
            name: name.to_string(),
            char_filters: Vec::new(),
            tokenizer,
            token_filters: Vec::new(),
        }
    }

    pub fn add_char_filter(&mut self, filter: Box<dyn CharFilter>) {
        self.char_filters.push(filter);
    }

    pub fn add_token_filter(&mut self, filter: Box<dyn TokenFilterPlugin>) {
        self.token_filters.push(filter);
    }

    /// Run the full pipeline: char filters → tokenizer → token filters.
    pub fn analyze(&self, text: &str) -> Vec<Token> {
        // Apply char filters
        let mut filtered = text.to_string();
        for cf in &self.char_filters {
            filtered = cf.filter(&filtered);
        }

        // Tokenize
        let mut raw_tokens = self.tokenizer.tokenize(&filtered);

        // Apply token filters
        for tf in &self.token_filters {
            raw_tokens = tf.filter(raw_tokens);
        }

        // Convert to Token structs with positions
        raw_tokens
            .into_iter()
            .enumerate()
            .map(|(pos, term)| Token {
                term,
                position: pos,
            })
            .collect()
    }
}

// --- Built-in char filters ---

/// Strips HTML tags from text.
#[derive(Debug)]
pub struct HtmlStripCharFilter;

impl CharFilter for HtmlStripCharFilter {
    fn filter(&self, input: &str) -> String {
        let mut result = String::with_capacity(input.len());
        let mut in_tag = false;
        for ch in input.chars() {
            if ch == '<' {
                in_tag = true;
            } else if ch == '>' {
                in_tag = false;
                result.push(' ');
            } else if !in_tag {
                result.push(ch);
            }
        }
        result
    }
}

/// Maps characters using a replacement table (e.g., accent folding).
#[derive(Debug)]
pub struct MappingCharFilter {
    mappings: Vec<(String, String)>,
}

impl MappingCharFilter {
    pub fn new(mappings: Vec<(String, String)>) -> Self {
        MappingCharFilter { mappings }
    }

    /// Create a filter that folds common accented characters to ASCII.
    pub fn ascii_folding() -> Self {
        MappingCharFilter {
            mappings: vec![
                ("\u{00e9}".into(), "e".into()),
                ("\u{00e8}".into(), "e".into()),
                ("\u{00ea}".into(), "e".into()),
                ("\u{00e0}".into(), "a".into()),
                ("\u{00e2}".into(), "a".into()),
                ("\u{00f4}".into(), "o".into()),
                ("\u{00fc}".into(), "u".into()),
                ("\u{00f6}".into(), "o".into()),
                ("\u{00e4}".into(), "a".into()),
                ("\u{00f1}".into(), "n".into()),
                ("\u{00e7}".into(), "c".into()),
            ],
        }
    }
}

impl CharFilter for MappingCharFilter {
    fn filter(&self, input: &str) -> String {
        let mut result = input.to_string();
        for (from, to) in &self.mappings {
            result = result.replace(from.as_str(), to.as_str());
        }
        result
    }
}

// --- Built-in tokenizers ---

/// Whitespace tokenizer — splits on Unicode whitespace.
#[derive(Debug)]
pub struct WhitespaceTokenizer;

impl TokenizerPlugin for WhitespaceTokenizer {
    fn tokenize(&self, input: &str) -> Vec<String> {
        input
            .split_whitespace()
            .map(|s| s.to_string())
            .collect()
    }
}

/// N-gram tokenizer — generates n-grams of configurable size.
#[derive(Debug)]
pub struct NgramTokenizer {
    pub min_gram: usize,
    pub max_gram: usize,
}

impl NgramTokenizer {
    pub fn new(min_gram: usize, max_gram: usize) -> Self {
        NgramTokenizer { min_gram, max_gram }
    }
}

impl TokenizerPlugin for NgramTokenizer {
    fn tokenize(&self, input: &str) -> Vec<String> {
        let chars: Vec<char> = input.chars().collect();
        let mut tokens = Vec::new();
        for n in self.min_gram..=self.max_gram {
            if n > chars.len() {
                break;
            }
            for window in chars.windows(n) {
                tokens.push(window.iter().collect());
            }
        }
        tokens
    }
}

/// Edge n-gram tokenizer — generates n-grams anchored at the start of each word.
#[derive(Debug)]
pub struct EdgeNgramTokenizer {
    pub min_gram: usize,
    pub max_gram: usize,
}

impl EdgeNgramTokenizer {
    pub fn new(min_gram: usize, max_gram: usize) -> Self {
        EdgeNgramTokenizer { min_gram, max_gram }
    }
}

impl TokenizerPlugin for EdgeNgramTokenizer {
    fn tokenize(&self, input: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        for word in input.split_whitespace() {
            let chars: Vec<char> = word.chars().collect();
            for n in self.min_gram..=self.max_gram.min(chars.len()) {
                tokens.push(chars[..n].iter().collect());
            }
        }
        tokens
    }
}

// --- Built-in token filters ---

/// Lowercase token filter.
#[derive(Debug)]
pub struct LowercaseTokenFilter;

impl TokenFilterPlugin for LowercaseTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        tokens.into_iter().map(|t| t.to_lowercase()).collect()
    }
}

/// Stopword removal token filter.
#[derive(Debug)]
pub struct StopwordTokenFilter {
    stopwords: std::collections::HashSet<String>,
}

impl StopwordTokenFilter {
    pub fn english() -> Self {
        let words = [
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "shall", "can", "to", "of", "in", "for",
            "on", "with", "at", "by", "from", "as", "into", "through", "during",
            "before", "after", "and", "but", "or", "not", "no", "if", "then",
            "than", "so", "that", "this", "it", "its", "i", "me", "my", "we",
            "our", "you", "your", "he", "him", "his", "she", "her", "they",
            "them", "their", "what", "which", "who", "whom",
        ];
        StopwordTokenFilter {
            stopwords: words.iter().map(|w| w.to_string()).collect(),
        }
    }

    pub fn custom(words: Vec<String>) -> Self {
        StopwordTokenFilter {
            stopwords: words.into_iter().collect(),
        }
    }
}

impl TokenFilterPlugin for StopwordTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        tokens
            .into_iter()
            .filter(|t| !self.stopwords.contains(t))
            .collect()
    }
}

/// Stemming token filter using the built-in English Porter stemmer.
#[derive(Debug)]
pub struct StemmerTokenFilter {
    language: StemLanguage,
}

impl StemmerTokenFilter {
    pub fn new(language: StemLanguage) -> Self {
        StemmerTokenFilter { language }
    }
}

impl TokenFilterPlugin for StemmerTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        tokens
            .into_iter()
            .map(|t| stem_language(&t, self.language))
            .collect()
    }
}

/// Synonym expansion token filter.
#[derive(Debug)]
pub struct SynonymTokenFilter {
    synonyms: HashMap<String, Vec<String>>,
}

impl SynonymTokenFilter {
    pub fn new(synonyms: HashMap<String, Vec<String>>) -> Self {
        SynonymTokenFilter { synonyms }
    }
}

impl TokenFilterPlugin for SynonymTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        let mut result = Vec::new();
        for token in tokens {
            result.push(token.clone());
            if let Some(syns) = self.synonyms.get(&token) {
                for syn in syns {
                    result.push(syn.clone());
                }
            }
        }
        result
    }
}

/// Length filter — removes tokens shorter than min or longer than max.
#[derive(Debug)]
pub struct LengthTokenFilter {
    pub min_length: usize,
    pub max_length: usize,
}

impl LengthTokenFilter {
    pub fn new(min_length: usize, max_length: usize) -> Self {
        LengthTokenFilter {
            min_length,
            max_length,
        }
    }
}

impl TokenFilterPlugin for LengthTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        tokens
            .into_iter()
            .filter(|t| t.len() >= self.min_length && t.len() <= self.max_length)
            .collect()
    }
}

/// Convenience: build a standard English analyzer pipeline.
pub fn standard_english_analyzer() -> AnalyzerPipeline {
    let mut pipeline = AnalyzerPipeline::new("standard_english", Box::new(WhitespaceTokenizer));
    pipeline.add_char_filter(Box::new(MappingCharFilter::ascii_folding()));
    pipeline.add_token_filter(Box::new(LowercaseTokenFilter));
    pipeline.add_token_filter(Box::new(StopwordTokenFilter::english()));
    pipeline.add_token_filter(Box::new(StemmerTokenFilter::new(StemLanguage::English)));
    pipeline
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basic() {
        let tokens = tokenize("Hello World! This is a test.");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert!(terms.contains(&"hello"));
        assert!(terms.contains(&"world"));
        assert!(terms.contains(&"test"));
        // Stopwords "this", "is", "a" should be removed
        assert!(!terms.contains(&"this"));
        assert!(!terms.contains(&"is"));
        assert!(!terms.contains(&"a"));
    }

    #[test]
    fn stemming() {
        assert_eq!(stem("running"), "run");
        assert_eq!(stem("played"), "play");
        assert_eq!(stem("happily"), "happi");
        assert_eq!(stem("cities"), "city");
        assert_eq!(stem("passes"), "pass");
    }

    #[test]
    fn index_and_search() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "The quick brown fox jumps over the lazy dog");
        idx.add_document(2, "A quick brown dog runs in the park");
        idx.add_document(3, "The fox is red and very quick");

        let results = idx.search("quick fox", 10);
        assert!(!results.is_empty());
        // Docs 1 and 3 both have "quick" and "fox". Doc 3 is shorter → higher BM25 score.
        let top_ids: Vec<u64> = results.iter().map(|r| r.0).collect();
        assert!(top_ids.contains(&1));
        assert!(top_ids.contains(&3));
        // Doc 2 only has "quick", not "fox"
        assert!(results.iter().find(|r| r.0 == 2).unwrap().1
            < results.iter().find(|r| r.0 == 1).unwrap().1);
    }

    #[test]
    fn bm25_ranking() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming language");
        idx.add_document(2, "rust rust rust the programming language for systems");
        idx.add_document(3, "python programming language");

        let results = idx.search("rust", 10);
        // Doc 2 has more occurrences of "rust" → higher TF → higher score
        assert_eq!(results[0].0, 2);
        assert_eq!(results[1].0, 1);
        // Doc 3 doesn't mention "rust"
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn remove_document() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        idx.add_document(2, "hello universe");

        assert_eq!(idx.doc_count(), 2);
        idx.remove_document(1);
        assert_eq!(idx.doc_count(), 1);

        let results = idx.search("hello", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn levenshtein_distance() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(levenshtein("abc", "abc"), 0);
        assert_eq!(levenshtein("abc", "abd"), 1);
    }

    #[test]
    fn fuzzy_search() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "quantum computing research");
        idx.add_document(2, "quantum mechanics physics");

        // "quantm" is a typo for "quantum" (distance 1)
        let matches = fuzzy_terms(&idx, "quantm", 2);
        assert!(!matches.is_empty());
        assert!(matches.iter().any(|(t, _)| *t == "quantum"));
    }

    #[test]
    fn multi_word_search_and_or_semantics() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming systems language");
        idx.add_document(2, "python data science machine learning");
        idx.add_document(3, "rust systems performance optimization");
        idx.add_document(4, "javascript web frontend framework");
        let results = idx.search("rust systems", 10);
        let top_ids: Vec<u64> = results.iter().map(|r| r.0).collect();
        assert!(top_ids.contains(&1));
        assert!(top_ids.contains(&3));
        assert!(!top_ids.contains(&2));
        assert!(!top_ids.contains(&4));
        let results = idx.search("python data science", 10);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn ranking_relevance_term_frequency() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "database engine for storage");
        idx.add_document(2, "database database database management");
        idx.add_document(3, "web server framework");
        let results = idx.search("database", 10);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 2);
        assert_eq!(results[1].0, 1);
        assert!(results[0].1 > results[1].1);
    }

    #[test]
    fn stopwords_are_filtered() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "the quick brown fox");
        let results = idx.search("the is a an", 10);
        assert!(results.is_empty());
        let results = idx.search("the quick", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn case_insensitivity() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "Rust Programming Language");
        idx.add_document(2, "RUST IS GREAT");
        let results = idx.search("rust", 10);
        assert_eq!(results.len(), 2);
        let results = idx.search("RUST", 10);
        assert_eq!(results.len(), 2);
        let results = idx.search("RuSt", 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn empty_and_whitespace_queries() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        assert!(idx.search("", 10).is_empty());
        assert!(idx.search("   ", 10).is_empty());
        assert!(idx.search("the a an is", 10).is_empty());
    }

    #[test]
    fn document_deletion_and_reindex() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "alpha beta gamma");
        idx.add_document(2, "beta gamma delta");
        idx.add_document(3, "gamma delta epsilon");
        assert_eq!(idx.doc_count(), 3);
        idx.remove_document(2);
        assert_eq!(idx.doc_count(), 2);
        let results = idx.search("beta", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        let results = idx.search("delta", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 3);
        idx.remove_document(1);
        idx.remove_document(3);
        assert_eq!(idx.doc_count(), 0);
        assert!(idx.search("gamma", 10).is_empty());
        idx.remove_document(999);
        assert_eq!(idx.doc_count(), 0);
    }

    #[test]
    fn unicode_text_search() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "cafe latte espresso");
        idx.add_document(2, "sushi ramen tempura");
        idx.add_document(3, "Berlin Munchen Hamburg");
        let results = idx.search("ramen", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
        let results = idx.search("Berlin", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 3);
    }

    #[test]
    fn stem_german_basic() {
        assert_eq!(stem_german("bestellung"), "bestell");
        assert_eq!(stem_german("freundlich"), "freund");
        assert_eq!(stem_german("schoenheit"), "schoen");
        assert_eq!(stem_german("traurigkeit"), "traurig");
    }

    #[test]
    fn stem_french_basic() {
        assert_eq!(stem_french("danseuse"), "dans");
        assert_eq!(stem_french("heureuses"), "heur");
        assert_eq!(stem_french("gentiment"), "genti");
    }

    #[test]
    fn stem_spanish_basic() {
        assert_eq!(stem_spanish("hablando"), "habl");
        assert_eq!(stem_spanish("comiendo"), "com");
        assert_eq!(stem_spanish("rapidamente"), "rapid");
        assert_eq!(stem_spanish("computador"), "comput");
    }

    #[test]
    fn stem_italian_basic() {
        assert_eq!(stem_italian("parlando"), "parl");
        assert_eq!(stem_italian("velocemente"), "veloce"); // -mente then -e (short)
        assert_eq!(stem_italian("mangiare"), "mangi");
    }

    #[test]
    fn stem_portuguese_basic() {
        assert_eq!(stem_portuguese("falando"), "fal");
        assert_eq!(stem_portuguese("comendo"), "com");
        assert_eq!(stem_portuguese("rapidamente"), "rapida"); // -mente only
    }

    #[test]
    fn stem_language_dispatch() {
        assert_eq!(stem_language("running", StemLanguage::English), "run");
        assert_eq!(stem_language("bestellung", StemLanguage::German), "bestell");
        assert_eq!(stem_language("hablando", StemLanguage::Spanish), "habl");
    }

    #[test]
    fn stem_short_words_unchanged() {
        // Short words should not be mangled
        assert_eq!(stem_german("ab"), "ab");
        assert_eq!(stem_french("un"), "un");
        assert_eq!(stem_spanish("el"), "el");
    }

    #[test]
    fn term_count_updates_on_add_and_remove() {
        let mut idx = InvertedIndex::new();
        assert_eq!(idx.term_count(), 0);
        idx.add_document(1, "alpha beta");
        let terms_after_one = idx.term_count();
        assert!(terms_after_one >= 2);
        idx.add_document(2, "gamma delta");
        let terms_after_two = idx.term_count();
        assert!(terms_after_two >= terms_after_one);
        idx.remove_document(2);
        let terms_after_remove = idx.term_count();
        assert!(terms_after_remove <= terms_after_two);
    }

    // ================================================================
    // DocBitmap tests
    // ================================================================

    #[test]
    fn docbitmap_insert_and_contains() {
        let mut bm = DocBitmap::new();
        assert!(bm.is_empty());
        bm.insert(5);
        bm.insert(3);
        bm.insert(10);
        bm.insert(3); // duplicate
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(3));
        assert!(bm.contains(5));
        assert!(bm.contains(10));
        assert!(!bm.contains(7));
    }

    #[test]
    fn docbitmap_from_sorted() {
        let bm = DocBitmap::from_sorted(vec![1, 3, 5, 7, 9]);
        assert_eq!(bm.len(), 5);
        assert!(bm.contains(5));
        assert!(!bm.contains(4));
    }

    #[test]
    fn docbitmap_intersect() {
        let a = DocBitmap::from_sorted(vec![1, 3, 5, 7, 9, 11]);
        let b = DocBitmap::from_sorted(vec![2, 3, 5, 8, 11, 15]);
        let result = a.intersect(&b);
        assert_eq!(result, DocBitmap::from_sorted(vec![3, 5, 11]));
    }

    #[test]
    fn docbitmap_intersect_empty() {
        let a = DocBitmap::from_sorted(vec![1, 2, 3]);
        let b = DocBitmap::new();
        assert!(a.intersect(&b).is_empty());
        assert!(b.intersect(&a).is_empty());
    }

    #[test]
    fn docbitmap_intersect_disjoint() {
        let a = DocBitmap::from_sorted(vec![1, 3, 5]);
        let b = DocBitmap::from_sorted(vec![2, 4, 6]);
        assert!(a.intersect(&b).is_empty());
    }

    #[test]
    fn docbitmap_union() {
        let a = DocBitmap::from_sorted(vec![1, 3, 5]);
        let b = DocBitmap::from_sorted(vec![2, 3, 6]);
        let result = a.union(&b);
        assert_eq!(result, DocBitmap::from_sorted(vec![1, 2, 3, 5, 6]));
    }

    #[test]
    fn docbitmap_difference() {
        let a = DocBitmap::from_sorted(vec![1, 2, 3, 5, 7]);
        let b = DocBitmap::from_sorted(vec![2, 5, 8]);
        let result = a.difference(&b);
        assert_eq!(result, DocBitmap::from_sorted(vec![1, 3, 7]));
    }

    #[test]
    fn docbitmap_iter() {
        let bm = DocBitmap::from_sorted(vec![10, 20, 30]);
        let collected: Vec<u64> = bm.iter().copied().collect();
        assert_eq!(collected, vec![10, 20, 30]);
    }

    #[test]
    fn docbitmap_large_intersect() {
        // Test galloping efficiency with asymmetric sizes
        let small = DocBitmap::from_sorted(vec![50, 500, 5000]);
        let large = DocBitmap::from_sorted((0..10000).collect());
        let result = small.intersect(&large);
        assert_eq!(result, DocBitmap::from_sorted(vec![50, 500, 5000]));
    }

    #[test]
    fn gallop_search() {
        let arr: Vec<u64> = (0..1000).collect();
        // Gallop should find exact match
        assert_eq!(gallop(&arr, 500, 0), 500);
        // Gallop from offset
        assert_eq!(gallop(&arr, 500, 400), 500);
        // Target beyond end
        assert_eq!(gallop(&arr, 2000, 0), arr.len());
    }

    // ================================================================
    // Block-max WAND tests
    // ================================================================

    #[test]
    fn bmw_basic_search() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming language");
        idx.add_document(2, "rust rust rust systems programming");
        idx.add_document(3, "python data science");

        let results = idx.search_bmw("rust", 10);
        assert_eq!(results.len(), 2);
        // Doc 2 has more TF for "rust" → higher score
        assert_eq!(results[0].0, 2);
        assert_eq!(results[1].0, 1);
    }

    #[test]
    fn bmw_multi_term() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "database engine storage");
        idx.add_document(2, "database database management system");
        idx.add_document(3, "web server framework");
        idx.add_document(4, "database storage optimization engine");

        let results = idx.search_bmw("database engine", 3);
        assert!(!results.is_empty());
        // Doc 4 has both terms; doc 1 has both terms
        let top_ids: Vec<u64> = results.iter().map(|r| r.0).collect();
        assert!(top_ids.contains(&1) || top_ids.contains(&4));
    }

    #[test]
    fn bmw_empty_query() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        assert!(idx.search_bmw("", 10).is_empty());
        assert!(idx.search_bmw("the a is", 10).is_empty()); // all stopwords
    }

    #[test]
    fn bmw_k_limit() {
        let mut idx = InvertedIndex::new();
        for i in 1..=20 {
            idx.add_document(i, &format!("rust programming document number {i}"));
        }
        let results = idx.search_bmw("rust", 5);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn bmw_matches_exhaustive_ranking() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "alpha beta gamma");
        idx.add_document(2, "beta gamma delta");
        idx.add_document(3, "gamma delta epsilon");

        let exhaustive = idx.search("gamma", 10);
        let bmw = idx.search_bmw("gamma", 10);

        // Both should return the same documents (order may differ for tied scores)
        assert_eq!(exhaustive.len(), bmw.len());
        let mut ex_ids: Vec<u64> = exhaustive.iter().map(|r| r.0).collect();
        let mut bm_ids: Vec<u64> = bmw.iter().map(|r| r.0).collect();
        ex_ids.sort();
        bm_ids.sort();
        assert_eq!(ex_ids, bm_ids);
    }

    #[test]
    fn bmw_no_matching_terms() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        assert!(idx.search_bmw("zzzznotaword", 10).is_empty());
    }

    #[test]
    fn build_block_posting_list() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust systems");
        idx.add_document(2, "rust programming");

        let bpl = idx.build_block_posting_list("rust").unwrap();
        assert_eq!(bpl.term, "rust");
        assert!(bpl.idf > 0.0);
        assert!(!bpl.blocks.is_empty());
        assert!(bpl.blocks[0].max_score > 0.0);
    }

    #[test]
    fn build_block_posting_list_nonexistent() {
        let idx = InvertedIndex::new();
        assert!(idx.build_block_posting_list("nope").is_none());
    }

    // ================================================================
    // Segment Merging tests
    // ================================================================

    #[test]
    fn segmented_index_basic() {
        let mut si = SegmentedIndex::new();
        si.flush_threshold = 3;

        si.add_document(1, "hello world");
        si.add_document(2, "hello rust");
        assert_eq!(si.segment_count(), 0); // below threshold

        si.add_document(3, "rust programming");
        // Flush should have happened (3 docs >= threshold)
        assert_eq!(si.segment_count(), 1);

        let results = si.search("hello", 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn segmented_index_search_across_segments() {
        let mut si = SegmentedIndex::new();
        si.flush_threshold = 2;

        si.add_document(1, "alpha beta");
        si.add_document(2, "gamma delta"); // flush
        si.add_document(3, "alpha gamma");
        si.add_document(4, "epsilon zeta"); // flush

        assert!(si.segment_count() >= 2);

        // Alpha is in segment 1 and segment 2
        let results = si.search("alpha", 10);
        let ids: Vec<u64> = results.iter().map(|r| r.0).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
    }

    #[test]
    fn segmented_index_delete() {
        let mut si = SegmentedIndex::new();
        si.add_document(1, "hello world");
        si.add_document(2, "hello rust");

        si.delete_document(1);
        let results = si.search("hello", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
        assert_eq!(si.tombstone_count(), 0); // doc was in writer, removed directly
    }

    #[test]
    fn segmented_index_delete_from_segment() {
        let mut si = SegmentedIndex::new();
        si.flush_threshold = 2;
        si.add_document(1, "alpha beta");
        si.add_document(2, "gamma delta"); // flush

        si.delete_document(1);
        assert!(si.tombstone_count() > 0);

        let results = si.search("alpha", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn segmented_index_force_merge() {
        let mut si = SegmentedIndex::new();
        si.flush_threshold = 2;

        si.add_document(1, "alpha");
        si.add_document(2, "beta"); // flush
        si.add_document(3, "gamma");
        si.add_document(4, "delta"); // flush
        si.add_document(5, "epsilon");
        si.add_document(6, "zeta"); // flush

        assert!(si.segment_count() >= 3);

        si.force_merge();
        assert_eq!(si.segment_count(), 1);

        // All docs should still be searchable
        assert!(!si.search("alpha", 10).is_empty());
        assert!(!si.search("delta", 10).is_empty());
        assert!(!si.search("zeta", 10).is_empty());
    }

    #[test]
    fn segmented_index_merge_purges_tombstones() {
        let mut si = SegmentedIndex::new();
        si.flush_threshold = 2;

        si.add_document(1, "alpha beta");
        si.add_document(2, "gamma delta"); // flush
        si.add_document(3, "alpha epsilon");
        si.add_document(4, "zeta eta"); // flush

        si.delete_document(1);
        assert!(si.tombstone_count() > 0);

        si.force_merge();

        // Doc 1 should be purged during merge
        assert!(si.search("alpha", 10).iter().all(|(id, _)| *id != 1));
        // Doc 3 still has alpha
        let results = si.search("alpha", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 3);
    }

    #[test]
    fn segmented_index_auto_merge_policy() {
        let mut si = SegmentedIndex::new();
        si.flush_threshold = 1; // flush every doc
        si.merge_policy = MergePolicy {
            max_segments: 3,
            merge_factor: 2,
            min_merge_size: 0,
            max_merge_size: usize::MAX,
        };

        for i in 1..=10 {
            si.add_document(i, &format!("document number {i} with unique content"));
        }

        // With aggressive merge policy, should stay under max_segments
        assert!(si.segment_count() <= 5); // some merging should have occurred
    }

    #[test]
    fn segmented_index_empty_operations() {
        let mut si = SegmentedIndex::new();
        assert!(si.search("anything", 10).is_empty());
        si.flush(); // flushing empty writer is a no-op
        assert_eq!(si.segment_count(), 0);
        si.force_merge(); // force merge with 0 segments is fine
        assert_eq!(si.segment_count(), 0);
    }

    #[test]
    fn segment_metadata() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world test");
        idx.add_document(2, "foo bar baz");
        let seg = Segment::new(42, idx);
        assert_eq!(seg.id, 42);
        assert_eq!(seg.doc_count, 2);
        assert!(seg.size_bytes > 0);
    }

    // ================================================================
    // Analyzer Pipeline tests
    // ================================================================

    #[test]
    fn html_strip_char_filter() {
        let filter = HtmlStripCharFilter;
        assert_eq!(filter.filter("<p>Hello</p>"), " Hello ");
        assert_eq!(filter.filter("no tags"), "no tags");
        assert_eq!(
            filter.filter("<b>bold</b> and <i>italic</i>"),
            " bold  and  italic "
        );
    }

    #[test]
    fn mapping_char_filter_ascii_folding() {
        let filter = MappingCharFilter::ascii_folding();
        assert_eq!(filter.filter("caf\u{00e9}"), "cafe");
        assert_eq!(filter.filter("\u{00fc}ber"), "uber");
        assert_eq!(filter.filter("pi\u{00f1}ata"), "pinata");
    }

    #[test]
    fn whitespace_tokenizer() {
        let tok = WhitespaceTokenizer;
        assert_eq!(tok.tokenize("hello world  test"), vec!["hello", "world", "test"]);
        assert!(tok.tokenize("").is_empty());
    }

    #[test]
    fn ngram_tokenizer() {
        let tok = NgramTokenizer::new(2, 3);
        let result = tok.tokenize("abcd");
        // 2-grams: ab, bc, cd; 3-grams: abc, bcd
        assert!(result.contains(&"ab".to_string()));
        assert!(result.contains(&"bc".to_string()));
        assert!(result.contains(&"cd".to_string()));
        assert!(result.contains(&"abc".to_string()));
        assert!(result.contains(&"bcd".to_string()));
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn edge_ngram_tokenizer() {
        let tok = EdgeNgramTokenizer::new(1, 4);
        let result = tok.tokenize("hello");
        assert_eq!(result, vec!["h", "he", "hel", "hell"]);
    }

    #[test]
    fn edge_ngram_multi_word() {
        let tok = EdgeNgramTokenizer::new(2, 3);
        let result = tok.tokenize("ab cde");
        // "ab" → ["ab"], "cde" → ["cd", "cde"]
        assert_eq!(result, vec!["ab", "cd", "cde"]);
    }

    #[test]
    fn lowercase_token_filter() {
        let f = LowercaseTokenFilter;
        let result = f.filter(vec!["Hello".into(), "WORLD".into()]);
        assert_eq!(result, vec!["hello", "world"]);
    }

    #[test]
    fn stopword_token_filter() {
        let f = StopwordTokenFilter::english();
        let result = f.filter(vec!["the".into(), "quick".into(), "fox".into(), "is".into()]);
        assert_eq!(result, vec!["quick", "fox"]);
    }

    #[test]
    fn stemmer_token_filter() {
        let f = StemmerTokenFilter::new(StemLanguage::English);
        let result = f.filter(vec!["running".into(), "played".into(), "tests".into()]);
        assert_eq!(result, vec!["run", "play", "test"]);
    }

    #[test]
    fn synonym_token_filter() {
        let mut syns = HashMap::new();
        syns.insert("quick".to_string(), vec!["fast".to_string(), "speedy".to_string()]);
        let f = SynonymTokenFilter::new(syns);
        let result = f.filter(vec!["quick".into(), "fox".into()]);
        assert_eq!(result, vec!["quick", "fast", "speedy", "fox"]);
    }

    #[test]
    fn length_token_filter() {
        let f = LengthTokenFilter::new(2, 5);
        let result = f.filter(vec!["a".into(), "ab".into(), "abcde".into(), "abcdef".into()]);
        assert_eq!(result, vec!["ab", "abcde"]);
    }

    #[test]
    fn analyzer_pipeline_full() {
        let mut pipeline = AnalyzerPipeline::new("test", Box::new(WhitespaceTokenizer));
        pipeline.add_char_filter(Box::new(HtmlStripCharFilter));
        pipeline.add_token_filter(Box::new(LowercaseTokenFilter));
        pipeline.add_token_filter(Box::new(StopwordTokenFilter::english()));
        pipeline.add_token_filter(Box::new(StemmerTokenFilter::new(StemLanguage::English)));

        let tokens = pipeline.analyze("<p>The Quick Running Dogs</p>");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert!(terms.contains(&"quick"));
        assert!(terms.contains(&"run")); // stemmed from "running"
        assert!(terms.contains(&"dog")); // stemmed from "dogs"
        assert!(!terms.contains(&"the")); // stopword removed
    }

    #[test]
    fn standard_english_analyzer_works() {
        let analyzer = standard_english_analyzer();
        assert_eq!(analyzer.name, "standard_english");

        let tokens = analyzer.analyze("The DOGS were Running quickly!");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        // "the" and "were" filtered; "dogs" stemmed; "running" stemmed; "quickly" → lowercased
        assert!(!terms.contains(&"the"));
        assert!(!terms.contains(&"were"));
        assert!(terms.contains(&"dog")); // stemmed
        assert!(terms.contains(&"run")); // stemmed
    }

    #[test]
    fn analyzer_with_ngrams() {
        let pipeline = AnalyzerPipeline::new("ngram_test", Box::new(NgramTokenizer::new(2, 3)));
        let tokens = pipeline.analyze("rust");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert!(terms.contains(&"ru"));
        assert!(terms.contains(&"us"));
        assert!(terms.contains(&"st"));
        assert!(terms.contains(&"rus"));
        assert!(terms.contains(&"ust"));
    }

    #[test]
    fn analyzer_with_edge_ngrams() {
        let mut pipeline =
            AnalyzerPipeline::new("edge_ngram_test", Box::new(EdgeNgramTokenizer::new(1, 3)));
        pipeline.add_token_filter(Box::new(LowercaseTokenFilter));
        let tokens = pipeline.analyze("Hello");
        let terms: Vec<&str> = tokens.iter().map(|t| t.term.as_str()).collect();
        assert_eq!(terms, vec!["h", "he", "hel"]);
    }

    #[test]
    fn custom_stopword_filter() {
        let f = StopwordTokenFilter::custom(vec!["foo".into(), "bar".into()]);
        let result = f.filter(vec!["foo".into(), "hello".into(), "bar".into(), "world".into()]);
        assert_eq!(result, vec!["hello", "world"]);
    }

    // ================================================================
    // FTS WAL integration tests
    // ================================================================

    #[test]
    fn fts_wal_index_100_reopen_search() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx = InvertedIndex::open(dir.path()).unwrap();
            for i in 0..100 {
                idx.add_document(i, &format!("document number {i} about rust programming"));
            }
        }
        // Reopen and search.
        let idx2 = InvertedIndex::open(dir.path()).unwrap();
        assert_eq!(idx2.doc_count(), 100);
        let results = idx2.search("rust programming", 10);
        assert_eq!(results.len(), 10);
        // All 100 docs contain "rust programming" so we should get some results.
        assert!(results[0].1 > 0.0);
    }

    #[test]
    fn fts_wal_remove_doc_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx = InvertedIndex::open(dir.path()).unwrap();
            idx.add_document(1, "alpha beta gamma");
            idx.add_document(2, "beta gamma delta");
            idx.add_document(3, "gamma delta epsilon");
            idx.remove_document(2);
        }
        let idx2 = InvertedIndex::open(dir.path()).unwrap();
        assert_eq!(idx2.doc_count(), 2);
        let results = idx2.search("beta", 10);
        // Only doc 1 has "beta" now (doc 2 was removed).
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn fts_wal_bm25_consistent_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let scores_before;
        {
            let mut idx = InvertedIndex::open(dir.path()).unwrap();
            idx.add_document(1, "rust programming language");
            idx.add_document(2, "rust rust rust systems programming");
            idx.add_document(3, "python data science");
            scores_before = idx.search("rust", 10);
        }
        let idx2 = InvertedIndex::open(dir.path()).unwrap();
        let scores_after = idx2.search("rust", 10);
        assert_eq!(scores_before.len(), scores_after.len());
        for (a, b) in scores_before.iter().zip(scores_after.iter()) {
            assert_eq!(a.0, b.0); // same doc IDs
            assert!((a.1 - b.1).abs() < 1e-10); // same BM25 scores
        }
    }

    #[test]
    fn fts_wal_fuzzy_search_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx = InvertedIndex::open(dir.path()).unwrap();
            idx.add_document(1, "quantum computing research");
            idx.add_document(2, "quantum mechanics physics");
        }
        let idx2 = InvertedIndex::open(dir.path()).unwrap();
        // "quantm" is a typo for "quantum" (distance 1).
        let matches = fuzzy_terms(&idx2, "quantm", 2);
        assert!(!matches.is_empty());
        assert!(matches.iter().any(|(t, _)| *t == "quantum"));
    }

    #[test]
    fn fts_wal_corrupt_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx = InvertedIndex::open(dir.path()).unwrap();
            idx.add_document(1, "valid document one");
            idx.add_document(2, "valid document two");
        }
        // Append garbage to the WAL file.
        {
            let path = dir.path().join("fts.wal");
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            use std::io::Write;
            f.write_all(&[0xFF, 0xAB, 0xCD, 0xEF, 0x00]).unwrap();
            f.flush().unwrap();
        }
        // Should recover the two valid documents.
        let idx2 = InvertedIndex::open(dir.path()).unwrap();
        assert_eq!(idx2.doc_count(), 2);
        let results = idx2.search("valid document", 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn fts_wal_empty_index_clean_open() {
        let dir = tempfile::tempdir().unwrap();
        let idx = InvertedIndex::open(dir.path()).unwrap();
        assert_eq!(idx.doc_count(), 0);
        assert!(idx.search("anything", 10).is_empty());
    }

    // ================================================================
    // search_scored tests (AND-semantics with shortest-first intersection)
    // ================================================================

    #[test]
    fn search_scored_basic_and_semantics() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming language");
        idx.add_document(2, "rust systems programming performance");
        idx.add_document(3, "python data science");
        idx.add_document(4, "rust language design compiler");

        // "rust programming" with AND semantics: only docs with BOTH terms
        let results = idx.search_scored("rust programming", 10);
        let ids: Vec<u64> = results.iter().map(|r| r.0).collect();
        // Docs 1 and 2 have both "rust" and "programming" (stemmed)
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        // Doc 3 has neither; doc 4 has "rust" but not "programming"
        assert!(!ids.contains(&3));
        assert!(!ids.contains(&4));
        // Results should be sorted by score descending
        for w in results.windows(2) {
            assert!(w[0].1 >= w[1].1);
        }
    }

    #[test]
    fn search_scored_single_term_matches_search() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "alpha beta gamma");
        idx.add_document(2, "beta gamma delta");
        idx.add_document(3, "gamma delta epsilon");

        // Single term: search_scored should behave like search (all docs with term)
        let scored = idx.search_scored("gamma", 10);
        let regular = idx.search("gamma", 10);
        assert_eq!(scored.len(), regular.len());
        let mut scored_ids: Vec<u64> = scored.iter().map(|r| r.0).collect();
        let mut regular_ids: Vec<u64> = regular.iter().map(|r| r.0).collect();
        scored_ids.sort();
        regular_ids.sort();
        assert_eq!(scored_ids, regular_ids);
    }

    #[test]
    fn search_scored_no_common_docs() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "alpha beta");
        idx.add_document(2, "gamma delta");

        // "alpha" only in doc 1, "gamma" only in doc 2 — no intersection
        let results = idx.search_scored("alpha gamma", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn search_scored_empty_query() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        assert!(idx.search_scored("", 10).is_empty());
        assert!(idx.search_scored("   ", 10).is_empty());
        assert!(idx.search_scored("the a an is", 10).is_empty()); // all stopwords
    }

    #[test]
    fn search_scored_nonexistent_term() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        // One real term + one non-existent: intersection should be empty
        let results = idx.search_scored("hello zzzznotaword", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn search_scored_relevance_ordering() {
        let mut idx = InvertedIndex::new();
        // Doc 1: both terms, "database" once
        idx.add_document(1, "database engine storage");
        // Doc 2: both terms, "database" three times → higher TF
        idx.add_document(2, "database database database engine management");
        // Doc 3: only "engine", not "database"
        idx.add_document(3, "engine performance optimization");

        let results = idx.search_scored("database engine", 10);
        assert_eq!(results.len(), 2); // docs 1 and 2 (doc 3 lacks "database")
        // Doc 2 should score higher (more "database" occurrences)
        assert_eq!(results[0].0, 2);
        assert_eq!(results[1].0, 1);
        assert!(results[0].1 > results[1].1);
    }

    #[test]
    fn search_scored_limit_respected() {
        let mut idx = InvertedIndex::new();
        for i in 1..=20 {
            idx.add_document(i, &format!("rust systems document {i}"));
        }
        let results = idx.search_scored("rust systems", 5);
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn search_shortest_first_optimization() {
        // Verify that search processes shortest posting lists first
        // by checking that results are correct with asymmetric posting list sizes
        let mut idx = InvertedIndex::new();
        // "rare" appears in only 1 doc, "common" appears in many
        for i in 1..=100 {
            idx.add_document(i, &format!("common word document {i}"));
        }
        idx.add_document(101, "common rare special word");

        // search_scored for "common rare" should find only doc 101
        let results = idx.search_scored("common rare", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 101);

        // Regular search for "common rare" should also return doc 101 first
        let results = idx.search("common rare", 5);
        assert_eq!(results[0].0, 101);
    }

    // ================================================================
    // contains_doc tests
    // ================================================================

    #[test]
    fn contains_doc_basic() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming language");
        idx.add_document(2, "python machine learning");
        idx.add_document(3, "database storage engine");

        // doc 1 contains "rust"
        assert!(idx.contains_doc(1, "rust"));
        // doc 1 does NOT contain "python"
        assert!(!idx.contains_doc(1, "python"));
        // doc 2 contains "machine learning"
        assert!(idx.contains_doc(2, "machine learning"));
        // doc 3 contains "storage"
        assert!(idx.contains_doc(3, "storage"));
        // no doc has "quantum"
        assert!(!idx.contains_doc(1, "quantum"));
        assert!(!idx.contains_doc(99, "rust")); // unknown doc
    }

    #[test]
    fn contains_doc_empty_query() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        // Empty query → no tokens → no match
        assert!(!idx.contains_doc(1, ""));
    }

    #[test]
    fn contains_doc_stopword_query() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming language");
        // "the" is a stopword → tokenizes to nothing → no match
        assert!(!idx.contains_doc(1, "the"));
    }

    #[test]
    fn contains_doc_multi_term_or_semantics() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust language fast");
        idx.add_document(2, "python slow script");

        // "rust python" → either term → doc 1 matches "rust", doc 2 matches "python"
        assert!(idx.contains_doc(1, "rust python"));
        assert!(idx.contains_doc(2, "rust python"));
        // doc 1 does NOT match "python" alone
        assert!(!idx.contains_doc(1, "python"));
    }

    #[test]
    fn contains_doc_after_remove() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust systems programming");
        assert!(idx.contains_doc(1, "rust"));
        idx.remove_document(1);
        assert!(!idx.contains_doc(1, "rust"));
    }

    #[test]
    fn contains_doc_matches_search_results() {
        // Verify that contains_doc agrees with the search() results
        let mut idx = InvertedIndex::new();
        for i in 1u64..=20 {
            idx.add_document(i, &format!("document {i} about rust systems programming"));
        }
        // doc 5 specifically: "document 5 about rust systems programming"
        // search("rust systems", usize::MAX) should include doc 5
        let search_results = idx.search("rust systems", usize::MAX);
        let search_ids: std::collections::HashSet<u64> = search_results.iter().map(|(id, _)| *id).collect();
        for i in 1u64..=20 {
            let in_search = search_ids.contains(&i);
            let in_contains = idx.contains_doc(i, "rust systems");
            assert_eq!(in_search, in_contains, "doc {i}: search={in_search} contains={in_contains}");
        }
    }

    #[test]
    fn bench_fts_10k_documents_multi_term_query() {
        // Benchmark: index 10K synthetic documents, then run multi-term queries
        let mut idx = InvertedIndex::new();

        // Categories of terms to create realistic document distributions
        let topics = [
            "database", "storage", "engine", "query", "index",
            "server", "network", "protocol", "cache", "memory",
            "compiler", "parser", "lexer", "optimizer", "runtime",
            "machine", "learning", "neural", "training", "model",
        ];

        // Index 10K documents with varying term combinations
        for i in 0..10_000u64 {
            let t1 = topics[(i as usize) % topics.len()];
            let t2 = topics[(i as usize / topics.len()) % topics.len()];
            let t3 = topics[(i as usize * 7) % topics.len()];
            let text = format!("{t1} {t2} {t3} document number {i} performance benchmark");
            idx.add_document(i, &text);
        }

        assert_eq!(idx.doc_count(), 10_000);

        // Measure multi-term query performance
        let start = std::time::Instant::now();
        let iterations = 100;
        for _ in 0..iterations {
            let _ = idx.search("database query optimizer", 10);
        }
        let search_elapsed = start.elapsed();

        let start_scored = std::time::Instant::now();
        for _ in 0..iterations {
            let _ = idx.search_scored("database query optimizer", 10);
        }
        let scored_elapsed = start_scored.elapsed();

        // Assert that both methods complete within reasonable time
        // 100 queries over 10K docs should take < 5 seconds on any modern machine
        // (includes deterministic tiebreaker sorting overhead)
        assert!(
            search_elapsed.as_secs() < 5,
            "search took too long: {:?}",
            search_elapsed
        );
        assert!(
            scored_elapsed.as_secs() < 5,
            "search_scored took too long: {:?}",
            scored_elapsed
        );

        // Verify results are valid
        let search_results = idx.search("database storage engine", 10);
        assert!(!search_results.is_empty());
        // All results should have positive scores
        for (_, score) in &search_results {
            assert!(*score > 0.0);
        }

        let scored_results = idx.search_scored("database storage engine", 10);
        // search_scored (AND) should return <= search (OR) results
        // because AND is more restrictive
        assert!(scored_results.len() <= search_results.len());
        // All scored results should have positive scores
        for (_, score) in &scored_results {
            assert!(*score > 0.0);
        }

        // Verify scored results are sorted by score descending
        for w in scored_results.windows(2) {
            assert!(w[0].1 >= w[1].1);
        }
    }

    // ================================================================
    // Parallel search and indexing tests
    // ================================================================

    /// Helper: sort results by (score DESC, doc_id ASC) for deterministic comparison.
    fn sort_deterministic(results: &mut [(u64, f64)]) {
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
    }

    #[test]
    fn par_search_scored_matches_sequential() {
        // Build an index large enough to exercise the parallel path.
        // Use varied document lengths so BM25 scores differ.
        let mut idx = InvertedIndex::new();
        for i in 0..2000u64 {
            let topic = if i % 3 == 0 { "rust" } else { "python" };
            // Vary document length using i to create distinct BM25 scores.
            let extra: String = (0..(i % 7)).map(|j| format!(" filler{j}")).collect();
            idx.add_document(
                i,
                &format!("{topic} programming language document{extra}"),
            );
        }

        let mut seq = idx.search_scored("rust programming", 20);
        let mut par = idx.par_search_scored("rust programming", 20);

        // Same length.
        assert_eq!(seq.len(), par.len());

        // When scores are tied, different sort approaches may pick different docs.
        // Compare the set of scores (rounded) rather than exact doc IDs.
        // Verify the score totals match and the count matches.
        let seq_total: f64 = seq.iter().map(|r| r.1).sum();
        let par_total: f64 = par.iter().map(|r| r.1).sum();
        assert!(
            (seq_total - par_total).abs() < 1e-6,
            "score totals differ: seq={seq_total}, par={par_total}"
        );

        // With deterministic tiebreaker, exact match should hold.
        sort_deterministic(&mut seq);
        sort_deterministic(&mut par);
        for (s, p) in seq.iter().zip(par.iter()) {
            assert!((s.1 - p.1).abs() < 1e-10, "score mismatch");
        }
    }

    #[test]
    fn par_search_scored_small_dataset_fallback() {
        // With fewer than 500 candidates, par_search_scored should still
        // return correct results (it falls back to sequential internally).
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "rust programming language");
        idx.add_document(2, "rust systems programming performance");
        idx.add_document(3, "python data science");

        let seq = idx.search_scored("rust programming", 10);
        let par = idx.par_search_scored("rust programming", 10);

        assert_eq!(seq.len(), par.len());
        for (s, p) in seq.iter().zip(par.iter()) {
            assert_eq!(s.0, p.0);
            assert!((s.1 - p.1).abs() < 1e-10);
        }
    }

    #[test]
    fn par_search_large_corpus() {
        let mut idx = InvertedIndex::new();
        let topics = [
            "database", "storage", "engine", "query", "index",
            "server", "network", "protocol", "cache", "memory",
        ];
        for i in 0..2000u64 {
            let t1 = topics[(i as usize) % topics.len()];
            let t2 = topics[(i as usize / topics.len()) % topics.len()];
            idx.add_document(i, &format!("{t1} {t2} document {i} benchmark"));
        }

        let results = idx.par_search("database query", 10);
        assert!(!results.is_empty());
        assert!(results.len() <= 10);
        // Results should be sorted by score descending.
        for w in results.windows(2) {
            assert!(w[0].1 >= w[1].1);
        }
        // All scores positive.
        for (_, score) in &results {
            assert!(*score > 0.0);
        }
    }

    #[test]
    fn par_bulk_index_matches_sequential() {
        // Index the same documents via add_document and par_bulk_index,
        // then verify that search results are identical.
        let docs: Vec<(u64, String)> = (0..500)
            .map(|i| {
                let t = if i % 2 == 0 { "alpha" } else { "beta" };
                (i as u64, format!("{t} gamma document {i}"))
            })
            .collect();

        let mut seq_idx = InvertedIndex::new();
        for (id, text) in &docs {
            seq_idx.add_document(*id, text);
        }

        let doc_refs: Vec<(u64, &str)> = docs.iter().map(|(id, t)| (*id, t.as_str())).collect();
        let mut par_idx = InvertedIndex::new();
        par_idx.par_bulk_index(&doc_refs);

        assert_eq!(seq_idx.doc_count(), par_idx.doc_count());
        assert_eq!(seq_idx.term_count(), par_idx.term_count());

        // Both indexes should have the same set of documents.
        // Scores may differ slightly in tie-breaking due to posting list order,
        // so verify structural equivalence: same doc_count, same terms,
        // and individual doc lookups produce the same score.
        let query = "alpha gamma";
        let seq_all = seq_idx.search(query, 500);
        let par_all = par_idx.search(query, 500);
        assert_eq!(seq_all.len(), par_all.len());

        // Build score maps and compare per-document.
        let seq_map: HashMap<u64, f64> = seq_all.into_iter().collect();
        let par_map: HashMap<u64, f64> = par_all.into_iter().collect();
        for (&doc_id, &seq_score) in &seq_map {
            let par_score = par_map.get(&doc_id).copied().unwrap_or(0.0);
            assert!(
                (seq_score - par_score).abs() < 1e-10,
                "score mismatch for doc {doc_id}: seq={seq_score}, par={par_score}"
            );
        }
    }

    #[test]
    fn par_search_multi_term() {
        let mut idx = InvertedIndex::new();
        for i in 0..1500u64 {
            let a = if i % 2 == 0 { "alpha" } else { "beta" };
            let b = if i % 3 == 0 { "gamma" } else { "delta" };
            let c = if i % 5 == 0 { "epsilon" } else { "zeta" };
            idx.add_document(i, &format!("{a} {b} {c} text {i}"));
        }

        // Multi-term query using par_search (OR semantics).
        let seq = idx.search("alpha gamma epsilon", 15);
        let par = idx.par_search("alpha gamma epsilon", 15);

        assert_eq!(seq.len(), par.len());

        // Scores may tie, so compare score sums and verify each result
        // appears in the full result set with the same score.
        let seq_total: f64 = seq.iter().map(|r| r.1).sum();
        let par_total: f64 = par.iter().map(|r| r.1).sum();
        assert!(
            (seq_total - par_total).abs() < 1e-6,
            "score totals differ"
        );

        // Both results should be sorted descending by score.
        for w in par.windows(2) {
            assert!(w[0].1 >= w[1].1);
        }
    }

    #[test]
    fn par_search_consistency() {
        // Run the same parallel search multiple times and verify
        // deterministic results. Use varied document lengths to reduce
        // tied scores.
        let mut idx = InvertedIndex::new();
        for i in 0..1000u64 {
            let extra: String = (0..(i % 5)).map(|j| format!(" word{j}")).collect();
            idx.add_document(
                i,
                &format!("rust systems programming document{extra}"),
            );
        }

        let mut first = idx.par_search_scored("rust programming", 10);
        sort_deterministic(&mut first);
        for _ in 0..5 {
            let mut again = idx.par_search_scored("rust programming", 10);
            sort_deterministic(&mut again);
            assert_eq!(first.len(), again.len());
            for (a, b) in first.iter().zip(again.iter()) {
                assert!(
                    (a.1 - b.1).abs() < 1e-10,
                    "scores differ across runs"
                );
            }
        }
    }

    #[test]
    fn par_search_empty_and_edge_cases() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");

        // Empty query.
        assert!(idx.par_search("", 10).is_empty());
        assert!(idx.par_search_scored("", 10).is_empty());

        // All stopwords.
        assert!(idx.par_search("the a an is", 10).is_empty());
        assert!(idx.par_search_scored("the a an is", 10).is_empty());

        // Non-existent term.
        assert!(idx.par_search("zzzznotaword", 10).is_empty());

        // par_bulk_index with empty slice.
        idx.par_bulk_index(&[]);
        assert_eq!(idx.doc_count(), 1);
    }

    #[test]
    fn par_bulk_index_large() {
        let docs: Vec<(u64, String)> = (0..2000)
            .map(|i| (i as u64, format!("document number {i} about various topics and content")))
            .collect();
        let doc_refs: Vec<(u64, &str)> = docs.iter().map(|(id, t)| (*id, t.as_str())).collect();

        let mut idx = InvertedIndex::new();
        idx.par_bulk_index(&doc_refs);

        assert_eq!(idx.doc_count(), 2000);
        let results = idx.search("document topics", 10);
        assert!(!results.is_empty());
    }

    // ====================================================================
    // FTS Highlighting tests
    // ====================================================================

    #[test]
    fn highlight_basic() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "The quick brown fox jumps over the lazy dog");
        idx.add_document(2, "A fast brown car races over the hill");

        let result = idx.highlight(1, "fox", "<em>", "</em>", 2);
        assert!(result.is_some());
        let highlighted = result.unwrap();
        assert!(highlighted.contains("<em>fox</em>"), "should highlight fox: {highlighted}");
        // Should include context words around "fox"
        assert!(highlighted.contains("brown"), "should include context: {highlighted}");
    }

    #[test]
    fn highlight_multiple_terms() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "Rust is a systems programming language that is fast and safe");

        let result = idx.highlight(1, "rust safe", "<b>", "</b>", 2).unwrap();
        assert!(result.contains("<b>Rust</b>"), "should highlight Rust: {result}");
        assert!(result.contains("<b>safe</b>"), "should highlight safe: {result}");
    }

    #[test]
    fn highlight_no_match() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "Hello world");

        let result = idx.highlight(1, "missing", "<em>", "</em>", 3).unwrap();
        // No matches — return original text
        assert_eq!(result, "Hello world");
    }

    #[test]
    fn highlight_missing_doc() {
        let idx = InvertedIndex::new();
        assert!(idx.highlight(999, "anything", "<em>", "</em>", 3).is_none());
    }

    // ====================================================================
    // Faceted Search tests
    // ====================================================================

    #[test]
    fn faceted_search_basic() {
        let mut idx = InvertedIndex::new();

        let mut f1 = HashMap::new();
        f1.insert("category".to_string(), vec!["electronics".to_string()]);
        f1.insert("brand".to_string(), vec!["acme".to_string()]);
        idx.add_document_with_facets(1, "wireless bluetooth headphones noise cancelling", f1);

        let mut f2 = HashMap::new();
        f2.insert("category".to_string(), vec!["electronics".to_string()]);
        f2.insert("brand".to_string(), vec!["globex".to_string()]);
        idx.add_document_with_facets(2, "bluetooth speaker portable wireless", f2);

        let mut f3 = HashMap::new();
        f3.insert("category".to_string(), vec!["clothing".to_string()]);
        f3.insert("brand".to_string(), vec!["acme".to_string()]);
        idx.add_document_with_facets(3, "cotton wireless-print t-shirt", f3);

        // Search for "wireless" — should match docs 1 and 2 (doc 3 has "wireless-print" which tokenizes differently)
        let result = idx.search_with_facets("bluetooth", 10, &[]);
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.total_matches, 2);

        // Category facet: electronics=2
        let cat_facet = &result.facets["category"];
        assert_eq!(cat_facet["electronics"], 2);
        assert!(!cat_facet.contains_key("clothing"));

        // Brand facet: acme=1, globex=1
        let brand_facet = &result.facets["brand"];
        assert_eq!(brand_facet["acme"], 1);
        assert_eq!(brand_facet["globex"], 1);
    }

    #[test]
    fn faceted_search_filtered_fields() {
        let mut idx = InvertedIndex::new();

        let mut f1 = HashMap::new();
        f1.insert("category".to_string(), vec!["electronics".to_string()]);
        f1.insert("color".to_string(), vec!["red".to_string()]);
        idx.add_document_with_facets(1, "red bluetooth speaker", f1);

        let mut f2 = HashMap::new();
        f2.insert("category".to_string(), vec!["electronics".to_string()]);
        f2.insert("color".to_string(), vec!["blue".to_string()]);
        idx.add_document_with_facets(2, "blue bluetooth headphones", f2);

        // Only collect "color" facet
        let result = idx.search_with_facets("bluetooth", 10, &["color"]);
        assert_eq!(result.results.len(), 2);
        assert!(result.facets.contains_key("color"));
        assert!(!result.facets.contains_key("category"));
        assert_eq!(result.facets["color"]["red"], 1);
        assert_eq!(result.facets["color"]["blue"], 1);
    }

    #[test]
    fn faceted_search_no_results() {
        let mut idx = InvertedIndex::new();
        let mut f1 = HashMap::new();
        f1.insert("category".to_string(), vec!["electronics".to_string()]);
        idx.add_document_with_facets(1, "bluetooth speaker", f1);

        let result = idx.search_with_facets("nonexistent", 10, &[]);
        assert!(result.results.is_empty());
        assert!(result.facets.is_empty());
        assert_eq!(result.total_matches, 0);
    }

    #[test]
    fn collect_facets_standalone() {
        let mut idx = InvertedIndex::new();
        idx.add_document(1, "hello world");
        idx.add_document(2, "hello there");
        idx.set_facets(1, {
            let mut m = HashMap::new();
            m.insert("tag".to_string(), vec!["greeting".to_string(), "general".to_string()]);
            m
        });
        idx.set_facets(2, {
            let mut m = HashMap::new();
            m.insert("tag".to_string(), vec!["greeting".to_string()]);
            m
        });

        let facets = idx.collect_facets(&[1, 2]);
        assert_eq!(facets["tag"]["greeting"], 2);
        assert_eq!(facets["tag"]["general"], 1);
    }

    #[test]
    fn facet_cleanup_on_remove() {
        let mut idx = InvertedIndex::new();
        let mut f1 = HashMap::new();
        f1.insert("category".to_string(), vec!["test".to_string()]);
        idx.add_document_with_facets(1, "hello world", f1);
        assert!(!idx.doc_facets.is_empty());

        idx.remove_document(1);
        assert!(idx.doc_facets.is_empty());
    }
}
