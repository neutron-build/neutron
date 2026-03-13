//! Document / JSONB engine with GIN indexing and path queries.

pub mod doc_wal;
pub mod tiered;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use doc_wal::DocWal;

// ---------------------------------------------------------------------------
// JsonValue
// ---------------------------------------------------------------------------

/// Represents a JSON value with recursive structure.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    Str(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

impl JsonValue {
    // -- Path queries -------------------------------------------------------

    /// Traverse into nested objects by a sequence of string keys.
    ///
    /// ```text
    /// let v = obj.get_path(&["a", "b", "c"]);
    /// ```
    pub fn get_path(&self, path: &[&str]) -> Option<&JsonValue> {
        if path.is_empty() {
            return Some(self);
        }
        match self {
            JsonValue::Object(map) => map.get(path[0]).and_then(|v| v.get_path(&path[1..])),
            _ => None,
        }
    }

    // -- Operators ----------------------------------------------------------

    /// `->` operator: get a JSON object field by key, returning the JsonValue.
    pub fn arrow(&self, key: &str) -> Option<&JsonValue> {
        match self {
            JsonValue::Object(map) => map.get(key),
            _ => None,
        }
    }

    /// `->>` operator: get a JSON object field as a text string.
    ///
    /// - If the field is a `Str`, the inner string is returned.
    /// - For other types the value is rendered as a JSON-like text
    ///   representation.
    /// - Returns `None` when `self` is not an object or the key is absent.
    pub fn arrow_text(&self, key: &str) -> Option<String> {
        self.arrow(key).map(|v| v.to_json_text())
    }

    /// `@>` operator: does `self` *contain* all key-value pairs present in
    /// `other`?
    ///
    /// Containment is checked recursively:
    /// - Object A contains Object B when every key in B exists in A and
    ///   `A[key] @> B[key]`.
    /// - Array A contains Array B when every element in B has a matching
    ///   element in A (order-independent).
    /// - Scalars are compared for equality.
    pub fn contains(&self, other: &JsonValue) -> bool {
        match (self, other) {
            (JsonValue::Object(a), JsonValue::Object(b)) => {
                b.iter().all(|(k, bv)| {
                    a.get(k).is_some_and(|av| av.contains(bv))
                })
            }
            (JsonValue::Array(a), JsonValue::Array(b)) => {
                b.iter().all(|bv| a.iter().any(|av| av.contains(bv)))
            }
            (a, b) => a == b,
        }
    }

    // -- GIN index extraction -----------------------------------------------

    /// Extract all `(path, leaf_value)` pairs from the document suitable for
    /// a GIN (Generalized Inverted Index).
    ///
    /// Paths are expressed as dot-separated strings for object keys and
    /// bracket-notation for array indices (e.g. `"a.b[0].c"`).
    pub fn gin_extract(&self) -> Vec<(String, JsonValue)> {
        let mut pairs = Vec::new();
        self.gin_extract_inner(String::new(), &mut pairs);
        pairs
    }

    fn gin_extract_inner(&self, prefix: String, out: &mut Vec<(String, JsonValue)>) {
        match self {
            JsonValue::Object(map) => {
                for (k, v) in map {
                    let path = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    v.gin_extract_inner(path, out);
                }
            }
            JsonValue::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let path = format!("{prefix}[{i}]");
                    v.gin_extract_inner(path, out);
                }
            }
            leaf => {
                out.push((prefix, leaf.clone()));
            }
        }
    }

    // -- Display helpers ----------------------------------------------------

    /// Render the value as a JSON-compatible text string (without outer
    /// quotes for `Str`).
    fn to_json_text(&self) -> String {
        match self {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => format_number(*n),
            JsonValue::Str(s) => s.clone(),
            JsonValue::Array(_) | JsonValue::Object(_) => self.to_json_string(),
        }
    }

    /// Full JSON serialisation (with quotes around strings).
    pub fn to_json_string(&self) -> String {
        match self {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => format_number(*n),
            JsonValue::Str(s) => format!("\"{}\"", escape_json_string(s)),
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(|v| v.to_json_string()).collect();
                format!("[{}]", items.join(","))
            }
            JsonValue::Object(map) => {
                let items: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("\"{}\":{}", escape_json_string(k), v.to_json_string()))
                    .collect();
                format!("{{{}}}", items.join(","))
            }
        }
    }
}

fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Binary JSONB encoding / decoding  (tag-length-value)
// ---------------------------------------------------------------------------
//
// Wire format:
//
// | tag (u8) | payload ...                               |
// |----------|-------------------------------------------|
// | 0x00     | Null  (no payload)                        |
// | 0x01     | Bool  — 1 byte: 0 = false, 1 = true      |
// | 0x02     | Number — 8 bytes big-endian f64           |
// | 0x03     | Str — 4-byte BE length + UTF-8 bytes      |
// | 0x04     | Array — 4-byte BE element count + elems   |
// | 0x05     | Object — 4-byte BE entry count + entries  |

const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_NUMBER: u8 = 0x02;
const TAG_STR: u8 = 0x03;
const TAG_ARRAY: u8 = 0x04;
const TAG_OBJECT: u8 = 0x05;

/// Encode a `JsonValue` into a compact binary (JSONB) representation.
pub fn jsonb_encode(value: &JsonValue) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_into(value, &mut buf);
    buf
}

fn encode_into(value: &JsonValue, buf: &mut Vec<u8>) {
    match value {
        JsonValue::Null => {
            buf.push(TAG_NULL);
        }
        JsonValue::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(if *b { 1 } else { 0 });
        }
        JsonValue::Number(n) => {
            buf.push(TAG_NUMBER);
            buf.extend_from_slice(&n.to_be_bytes());
        }
        JsonValue::Str(s) => {
            buf.push(TAG_STR);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        JsonValue::Array(arr) => {
            buf.push(TAG_ARRAY);
            buf.extend_from_slice(&(arr.len() as u32).to_be_bytes());
            for item in arr {
                encode_into(item, buf);
            }
        }
        JsonValue::Object(map) => {
            buf.push(TAG_OBJECT);
            buf.extend_from_slice(&(map.len() as u32).to_be_bytes());
            for (k, v) in map {
                // key: length-prefixed UTF-8
                let kb = k.as_bytes();
                buf.extend_from_slice(&(kb.len() as u32).to_be_bytes());
                buf.extend_from_slice(kb);
                // value
                encode_into(v, buf);
            }
        }
    }
}

/// Decode a `JsonValue` from its binary JSONB representation.
///
/// Returns `None` if the byte stream is malformed.
pub fn jsonb_decode(data: &[u8]) -> Option<JsonValue> {
    let mut cursor = 0;
    let val = decode_from(data, &mut cursor)?;
    Some(val)
}

fn decode_from(data: &[u8], cursor: &mut usize) -> Option<JsonValue> {
    if *cursor >= data.len() {
        return None;
    }
    let tag = data[*cursor];
    *cursor += 1;

    match tag {
        TAG_NULL => Some(JsonValue::Null),

        TAG_BOOL => {
            if *cursor >= data.len() {
                return None;
            }
            let v = data[*cursor] != 0;
            *cursor += 1;
            Some(JsonValue::Bool(v))
        }

        TAG_NUMBER => {
            if *cursor + 8 > data.len() {
                return None;
            }
            let bytes: [u8; 8] = data[*cursor..*cursor + 8].try_into().ok()?;
            *cursor += 8;
            Some(JsonValue::Number(f64::from_be_bytes(bytes)))
        }

        TAG_STR => {
            let len = read_u32(data, cursor)? as usize;
            if *cursor + len > data.len() {
                return None;
            }
            let s = std::str::from_utf8(&data[*cursor..*cursor + len]).ok()?;
            *cursor += len;
            Some(JsonValue::Str(s.to_string()))
        }

        TAG_ARRAY => {
            let count = read_u32(data, cursor)? as usize;
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                arr.push(decode_from(data, cursor)?);
            }
            Some(JsonValue::Array(arr))
        }

        TAG_OBJECT => {
            let count = read_u32(data, cursor)? as usize;
            let mut map = BTreeMap::new();
            for _ in 0..count {
                // key
                let klen = read_u32(data, cursor)? as usize;
                if *cursor + klen > data.len() {
                    return None;
                }
                let key = std::str::from_utf8(&data[*cursor..*cursor + klen]).ok()?.to_string();
                *cursor += klen;
                // value
                let val = decode_from(data, cursor)?;
                map.insert(key, val);
            }
            Some(JsonValue::Object(map))
        }

        _ => None,
    }
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Option<u32> {
    if *cursor + 4 > data.len() {
        return None;
    }
    let bytes: [u8; 4] = data[*cursor..*cursor + 4].try_into().ok()?;
    *cursor += 4;
    Some(u32::from_be_bytes(bytes))
}

// ---------------------------------------------------------------------------
// GIN Index
// ---------------------------------------------------------------------------

/// A Generalized Inverted Index mapping `(path, leaf_value)` pairs back to
/// document IDs, enabling fast containment (`@>`) and equality lookups.
#[derive(Debug, Clone)]
pub struct GinIndex {
    /// Map from `(path, encoded_leaf)` -> set of document IDs.
    entries: HashMap<(String, Vec<u8>), HashSet<u64>>,
}

impl Default for GinIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GinIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Add all `(path, value)` pairs from `doc` under the given `doc_id`.
    pub fn insert(&mut self, doc_id: u64, doc: &JsonValue) {
        for (path, leaf) in doc.gin_extract() {
            let key_bytes = jsonb_encode(&leaf);
            self.entries
                .entry((path, key_bytes))
                .or_default()
                .insert(doc_id);
        }
    }

    /// Remove all entries associated with `doc_id` for the given `doc`.
    pub fn remove(&mut self, doc_id: u64, doc: &JsonValue) {
        for (path, leaf) in doc.gin_extract() {
            let key_bytes = jsonb_encode(&leaf);
            if let Some(set) = self.entries.get_mut(&(path, key_bytes)) {
                set.remove(&doc_id);
            }
        }
    }

    /// Return document IDs whose indexed pairs are a superset of the pairs
    /// extracted from `query`.  This is the GIN-accelerated `@>` check.
    pub fn query_contains(&self, query: &JsonValue) -> HashSet<u64> {
        let pairs = query.gin_extract();
        if pairs.is_empty() {
            return HashSet::new();
        }

        let mut result: Option<HashSet<u64>> = None;
        for (path, leaf) in &pairs {
            let key_bytes = jsonb_encode(leaf);
            let ids = self
                .entries
                .get(&(path.clone(), key_bytes))
                .cloned()
                .unwrap_or_default();

            result = Some(match result {
                Some(acc) => acc.intersection(&ids).copied().collect(),
                None => ids,
            });
        }

        result.unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Cold tier JSON encoding helpers
// ---------------------------------------------------------------------------

/// Encode a `JsonValue` as JSON string bytes for cold LsmTree storage.
fn cold_encode_json(val: &JsonValue) -> Vec<u8> {
    val.to_json_string().into_bytes()
}

/// Decode a `JsonValue` from JSON string bytes stored in the cold LsmTree.
fn cold_decode_json(bytes: &[u8]) -> Option<JsonValue> {
    let s = std::str::from_utf8(bytes).ok()?;
    tiered::parse_json_value_pub(s).map(|(v, _)| v)
}

// ---------------------------------------------------------------------------
// DocumentStore
// ---------------------------------------------------------------------------

/// In-memory document store backed by a `HashMap` of document IDs to
/// `JsonValue` documents with a GIN index for fast containment queries.
///
/// When created with [`DocumentStore::open`], all mutations are logged to a
/// WAL file for crash recovery and a cold LsmTree tier is created for
/// overflow storage. The legacy [`DocumentStore::new`] constructor
/// creates an in-memory-only store (no WAL, no cold tier).
pub struct DocumentStore {
    docs: HashMap<u64, JsonValue>,
    gin: GinIndex,
    next_id: u64,
    wal: Option<Arc<DocWal>>,
    /// Cold tier: disk-backed LsmTree for overflow documents (disk mode only).
    cold: Option<parking_lot::Mutex<crate::storage::lsm::LsmTree>>,
    /// Maximum documents in hot (in-memory) tier before eviction to cold.
    max_hot_docs: usize,
}

impl Default for DocumentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentStore {
    pub fn new() -> Self {
        Self {
            docs: HashMap::new(),
            gin: GinIndex::new(),
            next_id: 1,
            wal: None,
            cold: None,
            max_hot_docs: usize::MAX,
        }
    }

    /// Open a WAL-backed document store rooted at `dir`.
    ///
    /// On first call the WAL file is created. On subsequent calls the WAL is
    /// replayed to restore all documents and rebuild the GIN index. The
    /// `next_id` counter is restored from `max(doc_id) + 1`.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        let (wal, state) = DocWal::open(dir)?;
        let wal = Arc::new(wal);

        // Open cold LsmTree tier for overflow documents
        let cold_dir = dir.join("doc_cold");
        std::fs::create_dir_all(&cold_dir).ok();
        let config = crate::storage::lsm::LsmConfig::default();
        let cold = crate::storage::lsm::LsmTree::open(config, &cold_dir)
            .ok()
            .map(parking_lot::Mutex::new);

        let mut store = Self {
            docs: HashMap::new(),
            gin: GinIndex::new(),
            next_id: 1,
            wal: Some(Arc::clone(&wal)),
            cold,
            max_hot_docs: 50_000,
        };
        // Restore documents from WAL state.
        for (doc_id, jsonb) in state.docs {
            if let Some(jv) = jsonb_decode(&jsonb) {
                store.gin.insert(doc_id, &jv);
                store.docs.insert(doc_id, jv);
                if doc_id >= store.next_id {
                    store.next_id = doc_id + 1;
                }
            }
            // Silently skip documents whose JSONB is corrupt.
        }
        Ok(store)
    }

    /// Insert a document and return its assigned ID.
    ///
    /// If a WAL is attached the mutation is logged before the in-memory update.
    /// In disk mode, triggers eviction to the cold tier if the hot tier exceeds
    /// `max_hot_docs`.
    pub fn insert(&mut self, doc: JsonValue) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        if let Some(ref wal) = self.wal {
            let bytes = jsonb_encode(&doc);
            if let Err(e) = wal.log_insert(id, &bytes) {
                eprintln!("document WAL: failed to log insert {id}: {e}");
            }
        }
        self.gin.insert(id, &doc);
        self.docs.insert(id, doc);
        if self.cold.is_some() {
            self.maybe_evict();
        }
        id
    }

    /// Insert a document with a specific ID. Replaces any existing document
    /// at that ID.
    pub fn insert_with_id(&mut self, id: u64, doc: JsonValue) {
        if let Some(ref wal) = self.wal {
            let bytes = jsonb_encode(&doc);
            if let Err(e) = wal.log_insert(id, &bytes) {
                eprintln!("document WAL: failed to log insert_with_id {id}: {e}");
            }
        }
        if let Some(old) = self.docs.get(&id) {
            self.gin.remove(id, &old.clone());
        }
        self.gin.insert(id, &doc);
        self.docs.insert(id, doc);
        if id >= self.next_id {
            self.next_id = id + 1;
        }
    }

    /// Delete a document by ID.
    ///
    /// Returns `true` if the document existed and was removed (from hot or
    /// cold tier), `false` if the ID was not found.
    pub fn delete(&mut self, id: u64) -> bool {
        if let Some(old) = self.docs.remove(&id) {
            if let Some(ref wal) = self.wal
                && let Err(e) = wal.log_delete(id) {
                    eprintln!("document WAL: failed to log delete {id}: {e}");
                }
            self.gin.remove(id, &old);
            true
        } else {
            // Try cold tier
            if let Some(ref cold) = self.cold {
                let key = id.to_le_bytes();
                let found = cold.lock().get(&key).is_some();
                if found {
                    if let Some(ref wal) = self.wal
                        && let Err(e) = wal.log_delete(id) {
                            eprintln!("document WAL: failed to log delete {id}: {e}");
                        }
                    cold.lock().delete(key.to_vec());
                    return true;
                }
            }
            false
        }
    }

    /// Get a document by ID (hot tier only).
    ///
    /// For cold-tier fallback with promotion, use [`get_promoting`] which
    /// requires `&mut self`.
    pub fn get(&self, id: u64) -> Option<&JsonValue> {
        self.docs.get(&id)
    }

    /// Get a document by ID, with cold-tier fallback and promotion.
    ///
    /// If the document is not in the hot tier but exists in the cold LsmTree,
    /// it is promoted back to the hot tier (and GIN index) and returned as
    /// an owned value.
    pub fn get_promoting(&mut self, id: u64) -> Option<JsonValue> {
        if let Some(jv) = self.docs.get(&id) {
            return Some(jv.clone());
        }
        // Try cold tier
        if let Some(ref cold) = self.cold {
            let key = id.to_le_bytes();
            // Must drop the MutexGuard before re-locking for delete
            let cold_data = cold.lock().get(&key);
            if let Some(data) = cold_data
                && let Some(jv) = cold_decode_json(&data) {
                    // Remove from cold (lock is not held here)
                    cold.lock().delete(key.to_vec());
                    // Insert into hot + GIN
                    self.gin.insert(id, &jv);
                    self.docs.insert(id, jv.clone());
                    if id >= self.next_id {
                        self.next_id = id + 1;
                    }
                    return Some(jv);
                }
        }
        None
    }

    /// Query documents by a path and expected leaf value.
    ///
    /// Returns the IDs of all documents where `doc.get_path(path) == Some(value)`.
    pub fn query_by_path(&self, path: &[&str], value: &JsonValue) -> Vec<u64> {
        self.docs
            .iter()
            .filter_map(|(&id, doc)| {
                if doc.get_path(path) == Some(value) {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Query documents using the `@>` containment operator.
    ///
    /// Returns the IDs of all documents where `doc @> query`.
    pub fn query_contains(&self, query: &JsonValue) -> Vec<u64> {
        // Use the GIN index to get candidates, then verify with full
        // containment check (the GIN index may produce false positives
        // in edge cases with arrays).
        let candidates = self.gin.query_contains(query);
        candidates
            .into_iter()
            .filter(|id| {
                self.docs
                    .get(id)
                    .is_some_and(|doc| doc.contains(query))
            })
            .collect()
    }

    /// Query documents using the GIN index alone (no verification pass).
    pub fn query_gin(&self, query: &JsonValue) -> HashSet<u64> {
        self.gin.query_contains(query)
    }

    /// Return the number of stored documents.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    // -- Parallel operations -------------------------------------------------

    /// Threshold below which parallel operations fall back to sequential.
    const PAR_THRESHOLD: usize = 500;

    /// Parse a simple predicate string of the form `"path.to.field=value"`.
    ///
    /// The path uses dot-separated keys and the value is matched as a string
    /// or, if it parses as `f64`, as a number. Returns `(path_segments, value)`.
    fn parse_predicate(predicate: &str) -> Option<(Vec<String>, JsonValue)> {
        let eq_pos = predicate.find('=')?;
        let path_str = &predicate[..eq_pos];
        let value_str = &predicate[eq_pos + 1..];
        let segments: Vec<String> = path_str.split('.').map(|s| s.to_string()).collect();
        if segments.is_empty() || segments.iter().any(|s| s.is_empty()) {
            return None;
        }
        // Try to interpret the value as a number, bool, or null; fall back to string.
        let value = if value_str == "null" {
            JsonValue::Null
        } else if value_str == "true" {
            JsonValue::Bool(true)
        } else if value_str == "false" {
            JsonValue::Bool(false)
        } else if let Ok(n) = value_str.parse::<f64>() {
            JsonValue::Number(n)
        } else {
            JsonValue::Str(value_str.to_string())
        };
        Some((segments, value))
    }

    /// Check whether a document matches a predicate string.
    fn matches_predicate(doc: &JsonValue, predicate: &str) -> bool {
        if let Some((segments, value)) = Self::parse_predicate(predicate) {
            let refs: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();
            doc.get_path(&refs) == Some(&value)
        } else {
            false
        }
    }

    /// Query documents matching a predicate, using parallel evaluation when
    /// the store contains >= 500 documents.
    ///
    /// The predicate format is `"path.to.field=value"` (dot-separated path,
    /// `=` delimiter, value parsed as number/bool/null/string).
    ///
    /// Returns `(doc_id, document)` pairs for every matching document.
    pub fn par_query(&self, predicate: &str) -> Vec<(u64, JsonValue)> {
        if self.docs.len() < Self::PAR_THRESHOLD {
            return self
                .docs
                .iter()
                .filter(|entry| Self::matches_predicate(entry.1, predicate))
                .map(|entry| (*entry.0, entry.1.clone()))
                .collect();
        }
        let all_docs: Vec<_> = self.docs.iter().collect();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = all_docs.len().div_ceil(cpus);
        std::thread::scope(|s| {
            let handles: Vec<_> = all_docs
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        chunk
                            .iter()
                            .filter(|entry| Self::matches_predicate(entry.1, predicate))
                            .map(|entry| (*entry.0, entry.1.clone()))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        })
    }

    /// Extract a JSON path value from every document, in parallel when the
    /// store contains >= 500 documents.
    ///
    /// Returns `(doc_id, Option<value>)` for every document — `None` when
    /// the path does not exist in a given document.
    pub fn par_path_query(&self, path: &str) -> Vec<(u64, Option<JsonValue>)> {
        let segments: Vec<&str> = path.split('.').collect();
        if self.docs.len() < Self::PAR_THRESHOLD {
            return self
                .docs
                .iter()
                .map(|(id, doc)| (*id, doc.get_path(&segments).cloned()))
                .collect();
        }
        let all_docs: Vec<_> = self.docs.iter().collect();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = all_docs.len().div_ceil(cpus);
        std::thread::scope(|s| {
            let handles: Vec<_> = all_docs
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        chunk
                            .iter()
                            .map(|entry| (*entry.0, entry.1.get_path(&segments).cloned()))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        })
    }

    /// Bulk-insert documents, parallelising validation and GIN-key extraction
    /// across threads while performing the actual insertion sequentially.
    ///
    /// Each input pair is `(string_key, document)`. The string key is stored
    /// as a `"_key"` field inside the document metadata — callers can use it
    /// for deduplication. The assigned `u64` IDs are returned in the same
    /// order as the input slice.
    pub fn par_bulk_insert(&mut self, docs: &[(String, JsonValue)]) -> Vec<u64> {
        // Phase 1: extract GIN pairs in parallel.
        let gin_pairs: Vec<Vec<(String, JsonValue)>> = if docs.len() >= Self::PAR_THRESHOLD {
            let cpus = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let chunk_size = docs.len().div_ceil(cpus);
            std::thread::scope(|s| {
                let handles: Vec<_> = docs
                    .chunks(chunk_size)
                    .map(|chunk| {
                        s.spawn(|| {
                            chunk
                                .iter()
                                .map(|(_, doc)| doc.gin_extract())
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .flat_map(|h| h.join().unwrap())
                    .collect()
            })
        } else {
            docs.iter().map(|(_, doc)| doc.gin_extract()).collect()
        };

        // Phase 2: sequential insert using pre-computed GIN pairs.
        let mut ids = Vec::with_capacity(docs.len());
        for (i, (_key, doc)) in docs.iter().enumerate() {
            let id = self.next_id;
            self.next_id += 1;
            if let Some(ref wal) = self.wal {
                let bytes = jsonb_encode(doc);
                if let Err(e) = wal.log_insert(id, &bytes) {
                    eprintln!("document WAL: failed to log bulk_insert {id}: {e}");
                }
            }
            // Use pre-computed GIN pairs instead of re-extracting.
            for (path, leaf) in &gin_pairs[i] {
                let key_bytes = jsonb_encode(leaf);
                self.gin
                    .entries
                    .entry((path.clone(), key_bytes))
                    .or_default()
                    .insert(id);
            }
            self.docs.insert(id, doc.clone());
            ids.push(id);
        }
        ids
    }

    /// Count documents matching a predicate, using parallel evaluation when
    /// the store contains >= 500 documents.
    ///
    /// This is more efficient than `par_query(...).len()` because it avoids
    /// cloning matching documents.
    pub fn par_count_where(&self, predicate: &str) -> usize {
        if self.docs.len() < Self::PAR_THRESHOLD {
            return self
                .docs
                .values()
                .filter(|doc| Self::matches_predicate(doc, predicate))
                .count();
        }
        let all_docs: Vec<_> = self.docs.iter().collect();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let chunk_size = all_docs.len().div_ceil(cpus);
        std::thread::scope(|s| {
            let handles: Vec<_> = all_docs
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        chunk
                            .iter()
                            .filter(|entry| Self::matches_predicate(entry.1, predicate))
                            .count()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .sum()
        })
    }

    /// Capture a snapshot of all document state for transaction rollback.
    pub fn txn_snapshot(&self) -> DocTxnSnapshot {
        DocTxnSnapshot {
            docs: self.docs.clone(),
            gin: self.gin.clone(),
            next_id: self.next_id,
        }
    }

    /// Restore document state from a transaction snapshot (for ROLLBACK).
    pub fn txn_restore(&mut self, snap: DocTxnSnapshot) {
        self.docs = snap.docs;
        self.gin = snap.gin;
        self.next_id = snap.next_id;
    }

    // ========================================================================
    // Cold tier helpers
    // ========================================================================

    /// Whether this store has a cold tier (disk mode).
    pub fn has_cold_tier(&self) -> bool {
        self.cold.is_some()
    }

    /// Return the count of hot (in-memory) documents only.
    pub fn len_hot(&self) -> usize {
        self.docs.len()
    }

    /// Evict documents from the hot tier to the cold LsmTree when the hot
    /// tier exceeds `max_hot_docs`.
    fn maybe_evict(&mut self) {
        if self.docs.len() <= self.max_hot_docs {
            return;
        }
        let Some(ref cold) = self.cold else { return };
        let to_evict = self.docs.len() - self.max_hot_docs;
        let mut eviction_list: Vec<(u64, JsonValue)> = Vec::with_capacity(to_evict);

        // Collect entries to evict (take first `to_evict` docs we iterate)
        for (&id, doc) in self.docs.iter() {
            eviction_list.push((id, doc.clone()));
            if eviction_list.len() >= to_evict {
                break;
            }
        }

        // Move to cold
        {
            let mut c = cold.lock();
            for (id, doc) in &eviction_list {
                let key = id.to_le_bytes().to_vec();
                let val = cold_encode_json(doc);
                c.put(key, val);
            }
        }

        // Remove from hot + GIN
        for (id, doc) in &eviction_list {
            self.gin.remove(*id, doc);
            self.docs.remove(id);
        }
    }
}

/// Snapshot of document store state for transaction rollback.
pub struct DocTxnSnapshot {
    docs: HashMap<u64, JsonValue>,
    gin: GinIndex,
    next_id: u64,
}

// ---------------------------------------------------------------------------
// Helper: build a JsonValue::Object conveniently
// ---------------------------------------------------------------------------

/// Small helper to construct a `JsonValue::Object` from key-value pairs.
#[cfg(test)]
fn json_obj(pairs: Vec<(&str, JsonValue)>) -> JsonValue {
    let mut map = BTreeMap::new();
    for (k, v) in pairs {
        map.insert(k.to_string(), v);
    }
    JsonValue::Object(map)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- 1. Encode / decode roundtrip ---------------------------------------

    #[test]
    fn test_encode_decode_roundtrip_scalar() {
        let values = vec![
            JsonValue::Null,
            JsonValue::Bool(true),
            JsonValue::Bool(false),
            JsonValue::Number(42.0),
            JsonValue::Number(-3.14),
            JsonValue::Str("hello world".to_string()),
        ];
        for v in &values {
            let encoded = jsonb_encode(v);
            let decoded = jsonb_decode(&encoded).expect("decode failed");
            assert_eq!(&decoded, v, "roundtrip mismatch for {:?}", v);
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_complex() {
        let doc = json_obj(vec![
            ("name", JsonValue::Str("Alice".to_string())),
            ("age", JsonValue::Number(30.0)),
            ("active", JsonValue::Bool(true)),
            (
                "tags",
                JsonValue::Array(vec![
                    JsonValue::Str("admin".to_string()),
                    JsonValue::Str("user".to_string()),
                ]),
            ),
            (
                "address",
                json_obj(vec![
                    ("city", JsonValue::Str("Springfield".to_string())),
                    ("zip", JsonValue::Number(62704.0)),
                ]),
            ),
        ]);

        let encoded = jsonb_encode(&doc);
        let decoded = jsonb_decode(&encoded).expect("decode failed");
        assert_eq!(decoded, doc);
    }

    // -- 2. Path queries ----------------------------------------------------

    #[test]
    fn test_path_queries() {
        let doc = json_obj(vec![
            (
                "user",
                json_obj(vec![
                    ("name", JsonValue::Str("Bob".to_string())),
                    (
                        "profile",
                        json_obj(vec![
                            ("bio", JsonValue::Str("Developer".to_string())),
                        ]),
                    ),
                ]),
            ),
        ]);

        assert_eq!(
            doc.get_path(&["user", "name"]),
            Some(&JsonValue::Str("Bob".to_string()))
        );
        assert_eq!(
            doc.get_path(&["user", "profile", "bio"]),
            Some(&JsonValue::Str("Developer".to_string()))
        );
        assert_eq!(doc.get_path(&["user", "nonexistent"]), None);
        assert_eq!(doc.get_path(&["missing"]), None);
        // Empty path returns self
        assert_eq!(doc.get_path(&[]), Some(&doc));
    }

    // -- 3. Arrow operator (->) ---------------------------------------------

    #[test]
    fn test_arrow_operator() {
        let doc = json_obj(vec![
            ("x", JsonValue::Number(10.0)),
            ("y", JsonValue::Str("hello".to_string())),
            ("nested", json_obj(vec![("a", JsonValue::Number(1.0))])),
        ]);

        assert_eq!(doc.arrow("x"), Some(&JsonValue::Number(10.0)));
        assert_eq!(
            doc.arrow("y"),
            Some(&JsonValue::Str("hello".to_string()))
        );
        assert_eq!(
            doc.arrow("nested"),
            Some(&json_obj(vec![("a", JsonValue::Number(1.0))]))
        );
        assert_eq!(doc.arrow("missing"), None);

        // Arrow on non-object returns None.
        assert_eq!(JsonValue::Number(5.0).arrow("x"), None);
    }

    // -- 4. Arrow-text operator (->>) ---------------------------------------

    #[test]
    fn test_arrow_text_operator() {
        let doc = json_obj(vec![
            ("name", JsonValue::Str("Charlie".to_string())),
            ("age", JsonValue::Number(25.0)),
            ("active", JsonValue::Bool(true)),
            ("nothing", JsonValue::Null),
        ]);

        assert_eq!(doc.arrow_text("name"), Some("Charlie".to_string()));
        assert_eq!(doc.arrow_text("age"), Some("25".to_string()));
        assert_eq!(doc.arrow_text("active"), Some("true".to_string()));
        assert_eq!(doc.arrow_text("nothing"), Some("null".to_string()));
        assert_eq!(doc.arrow_text("nope"), None);
    }

    // -- 5. Containment operator (@>) ---------------------------------------

    #[test]
    fn test_containment_operator() {
        let doc = json_obj(vec![
            ("name", JsonValue::Str("Eve".to_string())),
            ("role", JsonValue::Str("admin".to_string())),
            ("level", JsonValue::Number(5.0)),
            (
                "meta",
                json_obj(vec![
                    ("org", JsonValue::Str("ACME".to_string())),
                    ("dept", JsonValue::Str("engineering".to_string())),
                ]),
            ),
        ]);

        // Subset match
        let query1 = json_obj(vec![("role", JsonValue::Str("admin".to_string()))]);
        assert!(doc.contains(&query1));

        // Multi-key subset
        let query2 = json_obj(vec![
            ("role", JsonValue::Str("admin".to_string())),
            ("level", JsonValue::Number(5.0)),
        ]);
        assert!(doc.contains(&query2));

        // Nested containment
        let query3 = json_obj(vec![(
            "meta",
            json_obj(vec![("org", JsonValue::Str("ACME".to_string()))]),
        )]);
        assert!(doc.contains(&query3));

        // Mismatch
        let query4 = json_obj(vec![("role", JsonValue::Str("user".to_string()))]);
        assert!(!doc.contains(&query4));

        // Extra key not present
        let query5 = json_obj(vec![("nonexistent", JsonValue::Null)]);
        assert!(!doc.contains(&query5));

        // Array containment
        let arr_a = JsonValue::Array(vec![
            JsonValue::Number(1.0),
            JsonValue::Number(2.0),
            JsonValue::Number(3.0),
        ]);
        let arr_b = JsonValue::Array(vec![JsonValue::Number(2.0), JsonValue::Number(3.0)]);
        assert!(arr_a.contains(&arr_b));

        let arr_c = JsonValue::Array(vec![JsonValue::Number(4.0)]);
        assert!(!arr_a.contains(&arr_c));
    }

    // -- 6. GIN index extraction --------------------------------------------

    #[test]
    fn test_gin_extraction() {
        let doc = json_obj(vec![
            ("a", JsonValue::Number(1.0)),
            (
                "b",
                json_obj(vec![
                    ("c", JsonValue::Str("deep".to_string())),
                ]),
            ),
            (
                "d",
                JsonValue::Array(vec![
                    JsonValue::Bool(true),
                    JsonValue::Number(99.0),
                ]),
            ),
        ]);

        let pairs = doc.gin_extract();

        // Should contain:
        //   ("a", Number(1.0))
        //   ("b.c", Str("deep"))
        //   ("d[0]", Bool(true))
        //   ("d[1]", Number(99.0))
        assert_eq!(pairs.len(), 4);

        assert!(pairs.contains(&("a".to_string(), JsonValue::Number(1.0))));
        assert!(pairs.contains(&("b.c".to_string(), JsonValue::Str("deep".to_string()))));
        assert!(pairs.contains(&("d[0]".to_string(), JsonValue::Bool(true))));
        assert!(pairs.contains(&("d[1]".to_string(), JsonValue::Number(99.0))));
    }

    // -- 7. DocumentStore insert / get / query by path ----------------------

    #[test]
    fn test_document_store_insert_and_query() {
        let mut store = DocumentStore::new();

        let doc1 = json_obj(vec![
            ("type", JsonValue::Str("user".to_string())),
            ("name", JsonValue::Str("Alice".to_string())),
        ]);
        let doc2 = json_obj(vec![
            ("type", JsonValue::Str("user".to_string())),
            ("name", JsonValue::Str("Bob".to_string())),
        ]);
        let doc3 = json_obj(vec![
            ("type", JsonValue::Str("post".to_string())),
            ("title", JsonValue::Str("Hello".to_string())),
        ]);

        let id1 = store.insert(doc1.clone());
        let id2 = store.insert(doc2.clone());
        let id3 = store.insert(doc3.clone());

        // Get by ID
        assert_eq!(store.get(id1), Some(&doc1));
        assert_eq!(store.get(id2), Some(&doc2));
        assert_eq!(store.get(id3), Some(&doc3));
        assert_eq!(store.get(999), None);

        // Query by path
        let users = store.query_by_path(&["type"], &JsonValue::Str("user".to_string()));
        assert_eq!(users.len(), 2);
        assert!(users.contains(&id1));
        assert!(users.contains(&id2));

        let posts = store.query_by_path(&["type"], &JsonValue::Str("post".to_string()));
        assert_eq!(posts.len(), 1);
        assert!(posts.contains(&id3));

        assert_eq!(store.len(), 3);
    }

    // -- 8. DocumentStore containment query + nested documents ---------------

    #[test]
    fn test_document_store_containment_and_nested() {
        let mut store = DocumentStore::new();

        let doc1 = json_obj(vec![
            ("kind", JsonValue::Str("event".to_string())),
            (
                "location",
                json_obj(vec![
                    ("city", JsonValue::Str("Portland".to_string())),
                    ("state", JsonValue::Str("OR".to_string())),
                ]),
            ),
            ("capacity", JsonValue::Number(200.0)),
        ]);

        let doc2 = json_obj(vec![
            ("kind", JsonValue::Str("event".to_string())),
            (
                "location",
                json_obj(vec![
                    ("city", JsonValue::Str("Seattle".to_string())),
                    ("state", JsonValue::Str("WA".to_string())),
                ]),
            ),
            ("capacity", JsonValue::Number(500.0)),
        ]);

        let doc3 = json_obj(vec![
            ("kind", JsonValue::Str("meetup".to_string())),
            (
                "location",
                json_obj(vec![
                    ("city", JsonValue::Str("Portland".to_string())),
                    ("state", JsonValue::Str("OR".to_string())),
                ]),
            ),
        ]);

        let id1 = store.insert(doc1);
        let id2 = store.insert(doc2);
        let id3 = store.insert(doc3);

        // Containment: find all events
        let events_query = json_obj(vec![("kind", JsonValue::Str("event".to_string()))]);
        let mut events = store.query_contains(&events_query);
        events.sort();
        assert_eq!(events.len(), 2);
        assert!(events.contains(&id1));
        assert!(events.contains(&id2));

        // Containment: find docs in Portland, OR (nested)
        let portland_query = json_obj(vec![(
            "location",
            json_obj(vec![("city", JsonValue::Str("Portland".to_string()))]),
        )]);
        let mut portland = store.query_contains(&portland_query);
        portland.sort();
        assert_eq!(portland.len(), 2);
        assert!(portland.contains(&id1));
        assert!(portland.contains(&id3));

        // GIN index query should return the same candidates
        let gin_candidates = store.query_gin(&portland_query);
        assert!(gin_candidates.contains(&id1));
        assert!(gin_candidates.contains(&id3));
        assert!(!gin_candidates.contains(&id2));

        // Nested path query
        let seattle_docs = store.query_by_path(
            &["location", "city"],
            &JsonValue::Str("Seattle".to_string()),
        );
        assert_eq!(seattle_docs.len(), 1);
        assert!(seattle_docs.contains(&id2));
    }

    // -- 9. Complex nested JSON document operations -------------------------

    #[test]
    fn test_complex_nested_document_operations() {
        // Build a deeply nested document representing an API response.
        let doc = json_obj(vec![
            ("version", JsonValue::Str("2.0".to_string())),
            (
                "data",
                json_obj(vec![
                    (
                        "users",
                        JsonValue::Array(vec![
                            json_obj(vec![
                                ("id", JsonValue::Number(1.0)),
                                ("name", JsonValue::Str("Alice".to_string())),
                                (
                                    "permissions",
                                    json_obj(vec![
                                        ("read", JsonValue::Bool(true)),
                                        ("write", JsonValue::Bool(false)),
                                        (
                                            "scopes",
                                            JsonValue::Array(vec![
                                                JsonValue::Str("repo".to_string()),
                                                JsonValue::Str("user".to_string()),
                                            ]),
                                        ),
                                    ]),
                                ),
                            ]),
                            json_obj(vec![
                                ("id", JsonValue::Number(2.0)),
                                ("name", JsonValue::Str("Bob".to_string())),
                                (
                                    "permissions",
                                    json_obj(vec![
                                        ("read", JsonValue::Bool(true)),
                                        ("write", JsonValue::Bool(true)),
                                        (
                                            "scopes",
                                            JsonValue::Array(vec![
                                                JsonValue::Str("admin".to_string()),
                                            ]),
                                        ),
                                    ]),
                                ),
                            ]),
                        ]),
                    ),
                    (
                        "metadata",
                        json_obj(vec![
                            ("total", JsonValue::Number(2.0)),
                            ("page", JsonValue::Number(1.0)),
                        ]),
                    ),
                ]),
            ),
        ]);

        // Deep path navigation: data -> metadata -> total
        assert_eq!(
            doc.get_path(&["data", "metadata", "total"]),
            Some(&JsonValue::Number(2.0))
        );

        // Arrow chaining: doc->"data"->"metadata"->"page"
        let data = doc.arrow("data").unwrap();
        let metadata = data.arrow("metadata").unwrap();
        assert_eq!(metadata.arrow("page"), Some(&JsonValue::Number(1.0)));

        // Arrow-text on nested value gives JSON serialisation of the array.
        let users_text = data.arrow_text("users");
        assert!(users_text.is_some());
        let text = users_text.unwrap();
        // Should contain both user names in the serialised output.
        assert!(text.contains("Alice"));
        assert!(text.contains("Bob"));

        // JSONB roundtrip of the entire complex document.
        let encoded = jsonb_encode(&doc);
        let decoded = jsonb_decode(&encoded).expect("roundtrip decode failed");
        assert_eq!(decoded, doc);

        // GIN extraction should produce leaf entries for every scalar.
        let pairs = doc.gin_extract();
        // Verify a few specific deep paths exist.
        assert!(pairs.contains(&(
            "data.metadata.total".to_string(),
            JsonValue::Number(2.0)
        )));
        assert!(pairs.contains(&(
            "data.users[0].name".to_string(),
            JsonValue::Str("Alice".to_string())
        )));
        assert!(pairs.contains(&(
            "data.users[1].permissions.write".to_string(),
            JsonValue::Bool(true)
        )));
        assert!(pairs.contains(&(
            "data.users[0].permissions.scopes[1]".to_string(),
            JsonValue::Str("user".to_string())
        )));
    }

    // -- 10. JSONPath querying edge cases -----------------------------------

    #[test]
    fn test_jsonpath_edge_cases() {
        // Empty object
        let empty_obj = json_obj(vec![]);
        assert_eq!(empty_obj.get_path(&[]), Some(&empty_obj));
        assert_eq!(empty_obj.get_path(&["any"]), None);
        assert_eq!(empty_obj.arrow("x"), None);
        assert_eq!(empty_obj.arrow_text("x"), None);
        assert!(empty_obj.gin_extract().is_empty());

        // Scalar values: get_path with empty path returns the scalar itself.
        let scalar = JsonValue::Number(42.0);
        assert_eq!(scalar.get_path(&[]), Some(&scalar));
        // Non-empty path on scalar returns None.
        assert_eq!(scalar.get_path(&["a"]), None);

        // Null value traversal
        let null_val = JsonValue::Null;
        assert_eq!(null_val.get_path(&[]), Some(&null_val));
        assert_eq!(null_val.get_path(&["key"]), None);
        assert_eq!(null_val.arrow("key"), None);

        // Array: arrow operator on array returns None (not an object).
        let arr = JsonValue::Array(vec![JsonValue::Number(1.0), JsonValue::Number(2.0)]);
        assert_eq!(arr.arrow("0"), None);
        assert_eq!(arr.get_path(&["0"]), None);

        // Keys with special characters (dots, brackets).
        let special = json_obj(vec![
            ("a.b", JsonValue::Number(1.0)),
            ("c[0]", JsonValue::Number(2.0)),
            ("", JsonValue::Str("empty_key".to_string())),
        ]);
        assert_eq!(special.arrow("a.b"), Some(&JsonValue::Number(1.0)));
        assert_eq!(special.arrow("c[0]"), Some(&JsonValue::Number(2.0)));
        assert_eq!(
            special.arrow(""),
            Some(&JsonValue::Str("empty_key".to_string()))
        );
        // get_path uses exact key matching per segment, so "a.b" as a single
        // path segment should match.
        assert_eq!(
            special.get_path(&["a.b"]),
            Some(&JsonValue::Number(1.0))
        );

        // Containment edge cases
        // Empty object is contained in everything.
        let any_doc = json_obj(vec![("x", JsonValue::Number(1.0))]);
        assert!(any_doc.contains(&json_obj(vec![])));

        // Self-containment.
        assert!(any_doc.contains(&any_doc));

        // Empty array contained in any array.
        let full_arr = JsonValue::Array(vec![JsonValue::Number(1.0)]);
        let empty_arr = JsonValue::Array(vec![]);
        assert!(full_arr.contains(&empty_arr));

        // JSONB roundtrip of empty object and empty array.
        let enc_obj = jsonb_encode(&empty_obj);
        assert_eq!(jsonb_decode(&enc_obj), Some(empty_obj));
        let enc_arr = jsonb_encode(&empty_arr);
        assert_eq!(jsonb_decode(&enc_arr), Some(empty_arr));

        // Malformed JSONB data returns None.
        assert_eq!(jsonb_decode(&[]), None);
        assert_eq!(jsonb_decode(&[0xFF]), None);
        // Truncated number (tag present but not enough bytes).
        assert_eq!(jsonb_decode(&[TAG_NUMBER, 0x00, 0x00]), None);
    }

    // -- 11. Document update/patch operations -------------------------------

    #[test]
    fn test_document_update_patch_operations() {
        let mut store = DocumentStore::new();

        // Insert an initial document.
        let original = json_obj(vec![
            ("name", JsonValue::Str("Alice".to_string())),
            ("age", JsonValue::Number(30.0)),
            ("email", JsonValue::Str("alice@example.com".to_string())),
        ]);
        let id = store.insert(original.clone());
        assert_eq!(store.get(id), Some(&original));

        // Simulate an update by replacing the document at the same ID.
        let updated = json_obj(vec![
            ("name", JsonValue::Str("Alice".to_string())),
            ("age", JsonValue::Number(31.0)),
            ("email", JsonValue::Str("alice_new@example.com".to_string())),
            ("verified", JsonValue::Bool(true)),
        ]);
        store.insert_with_id(id, updated.clone());

        // The document should reflect the update.
        assert_eq!(store.get(id), Some(&updated));
        // Store size should remain 1 (replaced, not appended).
        assert_eq!(store.len(), 1);

        // The old path query should no longer match the old value.
        let old_email_results = store.query_by_path(
            &["email"],
            &JsonValue::Str("alice@example.com".to_string()),
        );
        assert!(old_email_results.is_empty());

        // New value should be queryable.
        let new_email_results = store.query_by_path(
            &["email"],
            &JsonValue::Str("alice_new@example.com".to_string()),
        );
        assert_eq!(new_email_results, vec![id]);

        // GIN index should also reflect the update.
        let contains_verified =
            json_obj(vec![("verified", JsonValue::Bool(true))]);
        let results = store.query_contains(&contains_verified);
        assert_eq!(results, vec![id]);

        // Containment query on old age should NOT match.
        let old_age_query = json_obj(vec![("age", JsonValue::Number(30.0))]);
        let old_age_results = store.query_contains(&old_age_query);
        assert!(old_age_results.is_empty());

        // Multiple sequential updates to the same document.
        for i in 0..5 {
            let version = json_obj(vec![
                ("name", JsonValue::Str("Alice".to_string())),
                ("revision", JsonValue::Number(i as f64)),
            ]);
            store.insert_with_id(id, version);
        }
        assert_eq!(store.len(), 1);
        let final_doc = store.get(id).unwrap();
        assert_eq!(
            final_doc.get_path(&["revision"]),
            Some(&JsonValue::Number(4.0))
        );
    }

    // -- 12. Index lookup on document fields --------------------------------

    #[test]
    fn test_gin_index_lookup_on_fields() {
        let mut store = DocumentStore::new();

        // Insert several documents with overlapping and unique field values.
        let doc_a = json_obj(vec![
            ("category", JsonValue::Str("electronics".to_string())),
            ("brand", JsonValue::Str("Acme".to_string())),
            ("price", JsonValue::Number(299.99)),
        ]);
        let doc_b = json_obj(vec![
            ("category", JsonValue::Str("electronics".to_string())),
            ("brand", JsonValue::Str("Globex".to_string())),
            ("price", JsonValue::Number(149.50)),
        ]);
        let doc_c = json_obj(vec![
            ("category", JsonValue::Str("clothing".to_string())),
            ("brand", JsonValue::Str("Acme".to_string())),
            ("price", JsonValue::Number(49.99)),
        ]);
        let doc_d = json_obj(vec![
            ("category", JsonValue::Str("electronics".to_string())),
            ("brand", JsonValue::Str("Acme".to_string())),
            ("price", JsonValue::Number(599.00)),
            (
                "specs",
                json_obj(vec![
                    ("weight", JsonValue::Number(1.5)),
                    ("color", JsonValue::Str("black".to_string())),
                ]),
            ),
        ]);

        let id_a = store.insert(doc_a);
        let id_b = store.insert(doc_b);
        let id_c = store.insert(doc_c);
        let id_d = store.insert(doc_d);

        // GIN query: all electronics
        let electronics_query =
            json_obj(vec![("category", JsonValue::Str("electronics".to_string()))]);
        let mut electronics = store.query_contains(&electronics_query);
        electronics.sort();
        assert_eq!(electronics, {
            let mut v = vec![id_a, id_b, id_d];
            v.sort();
            v
        });

        // GIN query: Acme brand
        let acme_query = json_obj(vec![("brand", JsonValue::Str("Acme".to_string()))]);
        let mut acme = store.query_contains(&acme_query);
        acme.sort();
        assert_eq!(acme, {
            let mut v = vec![id_a, id_c, id_d];
            v.sort();
            v
        });

        // Compound containment: electronics AND Acme
        let compound = json_obj(vec![
            ("category", JsonValue::Str("electronics".to_string())),
            ("brand", JsonValue::Str("Acme".to_string())),
        ]);
        let mut compound_results = store.query_contains(&compound);
        compound_results.sort();
        assert_eq!(compound_results, {
            let mut v = vec![id_a, id_d];
            v.sort();
            v
        });

        // Nested field lookup via GIN: specs.color = "black"
        let nested_query = json_obj(vec![(
            "specs",
            json_obj(vec![("color", JsonValue::Str("black".to_string()))]),
        )]);
        let nested_results = store.query_contains(&nested_query);
        assert_eq!(nested_results, vec![id_d]);

        // Direct GIN index query should match verified containment.
        let gin_electronics = store.query_gin(&electronics_query);
        assert!(gin_electronics.contains(&id_a));
        assert!(gin_electronics.contains(&id_b));
        assert!(gin_electronics.contains(&id_d));
        assert!(!gin_electronics.contains(&id_c));

        // query_by_path for exact field match
        let acme_path = store.query_by_path(&["brand"], &JsonValue::Str("Acme".to_string()));
        assert_eq!(acme_path.len(), 3);
        assert!(acme_path.contains(&id_a));
        assert!(acme_path.contains(&id_c));
        assert!(acme_path.contains(&id_d));
    }

    // -- 13. Collection management (create, drop, list) ---------------------

    #[test]
    fn test_collection_management() {
        // Simulate multiple collections using a HashMap of DocumentStores.
        let mut collections: HashMap<String, DocumentStore> = HashMap::new();

        // Create collections.
        collections.insert("users".to_string(), DocumentStore::new());
        collections.insert("posts".to_string(), DocumentStore::new());
        collections.insert("comments".to_string(), DocumentStore::new());

        // List collections.
        let mut names: Vec<&String> = collections.keys().collect();
        names.sort();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&&"users".to_string()));
        assert!(names.contains(&&"posts".to_string()));
        assert!(names.contains(&&"comments".to_string()));

        // Insert into specific collections.
        let user = json_obj(vec![
            ("name", JsonValue::Str("Dana".to_string())),
            ("role", JsonValue::Str("editor".to_string())),
        ]);
        let user_id = collections.get_mut("users").unwrap().insert(user.clone());

        let post = json_obj(vec![
            ("title", JsonValue::Str("Hello World".to_string())),
            ("author_id", JsonValue::Number(user_id as f64)),
        ]);
        let post_id = collections.get_mut("posts").unwrap().insert(post.clone());

        // Verify isolation between collections.
        assert_eq!(collections["users"].len(), 1);
        assert_eq!(collections["posts"].len(), 1);
        assert_eq!(collections["comments"].len(), 0);

        // Cross-collection query: find the user referenced by the post.
        let fetched_post = collections["posts"].get(post_id).unwrap();
        let author_id_val = fetched_post.get_path(&["author_id"]).unwrap();
        if let JsonValue::Number(aid) = author_id_val {
            let author = collections["users"].get(*aid as u64);
            assert!(author.is_some());
            assert_eq!(
                author.unwrap().get_path(&["name"]),
                Some(&JsonValue::Str("Dana".to_string()))
            );
        } else {
            panic!("author_id should be a Number");
        }

        // Drop a collection.
        collections.remove("comments");
        assert_eq!(collections.len(), 2);
        assert!(!collections.contains_key("comments"));

        // Remaining collections still work.
        assert_eq!(collections["users"].get(user_id), Some(&user));
        assert_eq!(collections["posts"].get(post_id), Some(&post));

        // Re-create a dropped collection; it starts empty.
        collections.insert("comments".to_string(), DocumentStore::new());
        assert!(collections["comments"].is_empty());
        assert_eq!(collections.len(), 3);
    }

    // -- 14. Large document handling ----------------------------------------

    #[test]
    fn test_large_document_handling() {
        // Build a large array with 1000 elements.
        let large_array: Vec<JsonValue> = (0..1000)
            .map(|i| {
                json_obj(vec![
                    ("index", JsonValue::Number(i as f64)),
                    ("label", JsonValue::Str(format!("item_{}", i))),
                    ("active", JsonValue::Bool(i % 2 == 0)),
                ])
            })
            .collect();

        let large_doc = json_obj(vec![
            ("type", JsonValue::Str("bulk".to_string())),
            ("count", JsonValue::Number(1000.0)),
            ("items", JsonValue::Array(large_array)),
        ]);

        // JSONB encode/decode roundtrip of the large document.
        let encoded = jsonb_encode(&large_doc);
        let decoded = jsonb_decode(&encoded).expect("large doc decode failed");
        assert_eq!(decoded, large_doc);

        // The encoded size should be non-trivial.
        assert!(encoded.len() > 10_000);

        // GIN extraction should produce entries for all leaf values.
        let pairs = large_doc.gin_extract();
        // 2 top-level scalars + 1000 * 3 leaf values = 3002
        assert_eq!(pairs.len(), 3002);

        // Verify specific entries in the extraction.
        assert!(pairs.contains(&(
            "items[0].index".to_string(),
            JsonValue::Number(0.0)
        )));
        assert!(pairs.contains(&(
            "items[999].label".to_string(),
            JsonValue::Str("item_999".to_string())
        )));
        assert!(pairs.contains(&(
            "items[500].active".to_string(),
            JsonValue::Bool(true)
        )));
        assert!(pairs.contains(&(
            "items[501].active".to_string(),
            JsonValue::Bool(false)
        )));

        // Insert into a store and query.
        let mut store = DocumentStore::new();
        let large_id = store.insert(large_doc.clone());
        assert_eq!(store.get(large_id), Some(&large_doc));

        // Containment query on the large document.
        let type_query = json_obj(vec![("type", JsonValue::Str("bulk".to_string()))]);
        let results = store.query_contains(&type_query);
        assert_eq!(results, vec![large_id]);

        // Insert many documents and verify store handles volume.
        for i in 0..500 {
            let doc = json_obj(vec![
                ("seq", JsonValue::Number(i as f64)),
                (
                    "category",
                    JsonValue::Str(if i % 3 == 0 {
                        "alpha".to_string()
                    } else if i % 3 == 1 {
                        "beta".to_string()
                    } else {
                        "gamma".to_string()
                    }),
                ),
            ]);
            store.insert(doc);
        }

        // 501 total (1 large + 500 small).
        assert_eq!(store.len(), 501);

        // Query a category.
        let alpha_query = json_obj(vec![("category", JsonValue::Str("alpha".to_string()))]);
        let alpha_results = store.query_contains(&alpha_query);
        // i % 3 == 0 for i in 0..500 => 0, 3, 6, ..., 498 => 167 documents
        assert_eq!(alpha_results.len(), 167);
    }

    // -- 15. WAL-backed durability tests ------------------------------------

    #[test]
    fn test_wal_insert_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.insert(json_obj(vec![
                ("name", JsonValue::Str("Alice".to_string())),
            ]));
            store.insert(json_obj(vec![
                ("name", JsonValue::Str("Bob".to_string())),
            ]));
        }
        // Reopen — documents should survive.
        let store2 = DocumentStore::open(dir.path()).unwrap();
        assert_eq!(store2.len(), 2);
        let doc1 = store2.get(1).unwrap();
        assert_eq!(
            doc1.get_path(&["name"]),
            Some(&JsonValue::Str("Alice".to_string()))
        );
        let doc2 = store2.get(2).unwrap();
        assert_eq!(
            doc2.get_path(&["name"]),
            Some(&JsonValue::Str("Bob".to_string()))
        );
    }

    #[test]
    fn test_wal_delete_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            let id1 = store.insert(json_obj(vec![
                ("x", JsonValue::Number(1.0)),
            ]));
            store.insert(json_obj(vec![
                ("x", JsonValue::Number(2.0)),
            ]));
            assert!(store.delete(id1));
        }
        let store2 = DocumentStore::open(dir.path()).unwrap();
        assert_eq!(store2.len(), 1);
        assert!(store2.get(1).is_none());
        assert!(store2.get(2).is_some());
    }

    #[test]
    fn test_wal_gin_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.insert(json_obj(vec![
                ("role", JsonValue::Str("admin".to_string())),
                ("name", JsonValue::Str("Eve".to_string())),
            ]));
            store.insert(json_obj(vec![
                ("role", JsonValue::Str("user".to_string())),
                ("name", JsonValue::Str("Frank".to_string())),
            ]));
        }
        let store2 = DocumentStore::open(dir.path()).unwrap();
        let admin_q = json_obj(vec![("role", JsonValue::Str("admin".to_string()))]);
        let admins = store2.query_contains(&admin_q);
        assert_eq!(admins.len(), 1);
        assert!(admins.contains(&1));
    }

    #[test]
    fn test_wal_nested_json_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let nested = json_obj(vec![
            ("level1", json_obj(vec![
                ("level2", json_obj(vec![
                    ("value", JsonValue::Number(42.0)),
                    ("tags", JsonValue::Array(vec![
                        JsonValue::Str("a".to_string()),
                        JsonValue::Str("b".to_string()),
                    ])),
                ])),
            ])),
        ]);
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.insert(nested.clone());
        }
        let store2 = DocumentStore::open(dir.path()).unwrap();
        assert_eq!(store2.get(1), Some(&nested));
    }

    #[test]
    fn test_wal_next_id_restored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.insert(json_obj(vec![("a", JsonValue::Number(1.0))]));
            store.insert(json_obj(vec![("b", JsonValue::Number(2.0))]));
            store.insert(json_obj(vec![("c", JsonValue::Number(3.0))]));
            // next_id should now be 4
        }
        let mut store2 = DocumentStore::open(dir.path()).unwrap();
        let id = store2.insert(json_obj(vec![("d", JsonValue::Number(4.0))]));
        assert_eq!(id, 4, "next_id should resume from max(doc_id)+1");
    }

    #[test]
    fn test_wal_corrupt_graceful_recovery() {
        let dir = tempfile::tempdir().unwrap();
        // Write a valid WAL, then corrupt it.
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.insert(json_obj(vec![("ok", JsonValue::Bool(true))]));
        }
        // Append garbage to WAL file.
        let wal_path = dir.path().join("doc.wal");
        let mut data = std::fs::read(&wal_path).unwrap();
        data.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
        std::fs::write(&wal_path, &data).unwrap();

        // Should recover the valid document and ignore trailing garbage.
        let store2 = DocumentStore::open(dir.path()).unwrap();
        assert_eq!(store2.len(), 1);
        assert_eq!(
            store2.get(1).unwrap().get_path(&["ok"]),
            Some(&JsonValue::Bool(true))
        );
    }

    #[test]
    fn test_wal_large_documents() {
        let dir = tempfile::tempdir().unwrap();
        let big_arr: Vec<JsonValue> = (0..500)
            .map(|i| json_obj(vec![
                ("idx", JsonValue::Number(i as f64)),
                ("data", JsonValue::Str(format!("entry_{i}"))),
            ]))
            .collect();
        let big_doc = json_obj(vec![
            ("items", JsonValue::Array(big_arr)),
        ]);
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.insert(big_doc.clone());
        }
        let store2 = DocumentStore::open(dir.path()).unwrap();
        assert_eq!(store2.len(), 1);
        assert_eq!(store2.get(1), Some(&big_doc));
    }

    #[test]
    fn test_wal_empty_store_clean_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = DocumentStore::open(dir.path()).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        // Reopen still empty.
        drop(store);
        let store2 = DocumentStore::open(dir.path()).unwrap();
        assert!(store2.is_empty());
    }

    // -- 16. Parallel query/scan operations -----------------------------------

    /// Helper: build a store with `n` documents, each having `category` and
    /// `seq` fields.  Categories cycle through alpha/beta/gamma.
    fn build_large_store(n: usize) -> DocumentStore {
        let mut store = DocumentStore::new();
        for i in 0..n {
            let cat = match i % 3 {
                0 => "alpha",
                1 => "beta",
                _ => "gamma",
            };
            let doc = json_obj(vec![
                ("category", JsonValue::Str(cat.to_string())),
                ("seq", JsonValue::Number(i as f64)),
                ("active", JsonValue::Bool(i % 2 == 0)),
            ]);
            store.insert(doc);
        }
        store
    }

    #[test]
    fn test_par_query_matches_sequential() {
        let store = build_large_store(1500);
        let par_results = store.par_query("category=alpha");
        // Sequential: count documents where category == alpha
        let seq_results: Vec<(u64, JsonValue)> = store
            .docs
            .iter()
            .filter(|(_, doc)| {
                doc.get_path(&["category"]) == Some(&JsonValue::Str("alpha".to_string()))
            })
            .map(|(&id, doc)| (id, doc.clone()))
            .collect();

        assert_eq!(par_results.len(), seq_results.len());
        // Verify every parallel result is also in the sequential set.
        let seq_ids: HashSet<u64> = seq_results.iter().map(|(id, _)| *id).collect();
        for (id, _) in &par_results {
            assert!(seq_ids.contains(id), "par_query returned unexpected id {id}");
        }
        // alpha: i % 3 == 0 for i in 0..1500 => 500 docs
        assert_eq!(par_results.len(), 500);
    }

    #[test]
    fn test_par_query_small_dataset_fallback() {
        // Below the 500-document threshold, par_query should still work
        // (falls back to sequential internally).
        let store = build_large_store(100);
        let results = store.par_query("category=beta");
        // beta: i % 3 == 1 for i in 0..100 => 33 docs (1,4,7,...,97)
        assert_eq!(results.len(), 33);
        for (_, doc) in &results {
            assert_eq!(
                doc.get_path(&["category"]),
                Some(&JsonValue::Str("beta".to_string()))
            );
        }
    }

    #[test]
    fn test_par_path_query_large() {
        let store = build_large_store(1200);
        let results = store.par_path_query("category");
        assert_eq!(results.len(), 1200);
        // Every document has a "category" field, so all should be Some.
        for (_, val) in &results {
            assert!(val.is_some(), "every doc should have a category");
        }
        // Query a non-existent path.
        let missing = store.par_path_query("nonexistent");
        assert_eq!(missing.len(), 1200);
        for (_, val) in &missing {
            assert!(val.is_none(), "no doc should have 'nonexistent'");
        }
    }

    #[test]
    fn test_par_bulk_insert_matches_sequential() {
        // Build input docs.
        let input: Vec<(String, JsonValue)> = (0..800)
            .map(|i| {
                let doc = json_obj(vec![
                    ("idx", JsonValue::Number(i as f64)),
                    ("tag", JsonValue::Str(format!("t{}", i % 10))),
                ]);
                (format!("key_{i}"), doc)
            })
            .collect();

        // Parallel bulk insert.
        let mut par_store = DocumentStore::new();
        let par_ids = par_store.par_bulk_insert(&input);
        assert_eq!(par_ids.len(), 800);
        assert_eq!(par_store.len(), 800);

        // Sequential insert for comparison.
        let mut seq_store = DocumentStore::new();
        for (_key, doc) in &input {
            seq_store.insert(doc.clone());
        }
        assert_eq!(seq_store.len(), 800);

        // Both stores should have the same documents (by content).
        for id in &par_ids {
            let par_doc = par_store.get(*id).expect("par doc missing");
            let seq_doc = seq_store.get(*id).expect("seq doc missing");
            assert_eq!(par_doc, seq_doc);
        }

        // GIN index should work identically: query by containment.
        let tag_query = json_obj(vec![("tag", JsonValue::Str("t0".to_string()))]);
        let par_gin = par_store.query_contains(&tag_query);
        let seq_gin = seq_store.query_contains(&tag_query);
        assert_eq!(par_gin.len(), seq_gin.len());
        // t0: i % 10 == 0 for i in 0..800 => 80 docs
        assert_eq!(par_gin.len(), 80);
    }

    #[test]
    fn test_par_count_where_matches_sequential() {
        let store = build_large_store(1500);
        let par_count = store.par_count_where("category=gamma");
        // Sequential count.
        let seq_count = store
            .docs
            .values()
            .filter(|doc| {
                doc.get_path(&["category"]) == Some(&JsonValue::Str("gamma".to_string()))
            })
            .count();
        assert_eq!(par_count, seq_count);
        // gamma: i % 3 == 2 for i in 0..1500 => 500 docs
        assert_eq!(par_count, 500);
    }

    #[test]
    fn test_par_query_consistency() {
        // Run the same parallel query multiple times and verify deterministic
        // result counts.
        let store = build_large_store(2000);
        let mut counts = Vec::new();
        for _ in 0..5 {
            let results = store.par_query("active=true");
            counts.push(results.len());
        }
        // All runs should produce the same count.
        assert!(
            counts.iter().all(|&c| c == counts[0]),
            "par_query should be deterministic: counts = {:?}",
            counts
        );
        // active=true when i % 2 == 0 => 1000 docs
        assert_eq!(counts[0], 1000);
    }

    #[test]
    fn test_par_count_where_small_dataset_fallback() {
        let store = build_large_store(50);
        let count = store.par_count_where("category=alpha");
        // alpha: i % 3 == 0 for i in 0..50 => 17 docs (0,3,6,...,48)
        assert_eq!(count, 17);
    }

    #[test]
    fn test_par_path_query_nested() {
        // Test parallel path extraction with nested paths.
        let mut store = DocumentStore::new();
        for i in 0..600 {
            let doc = json_obj(vec![(
                "user",
                json_obj(vec![
                    ("name", JsonValue::Str(format!("user_{i}"))),
                    ("level", JsonValue::Number((i % 5) as f64)),
                ]),
            )]);
            store.insert(doc);
        }
        let results = store.par_path_query("user.level");
        assert_eq!(results.len(), 600);
        // Every doc has user.level.
        for (_, val) in &results {
            assert!(val.is_some());
            match val.as_ref().unwrap() {
                JsonValue::Number(n) => assert!((0.0..5.0).contains(n)),
                other => panic!("expected Number, got {:?}", other),
            }
        }
    }

    // ========================================================================
    // Cold tier (tiered storage) tests
    // ========================================================================

    #[test]
    fn test_doc_cold_tier_basic() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DocumentStore::open(dir.path()).unwrap();
        assert!(store.has_cold_tier(), "disk mode should have cold tier");
        assert!(dir.path().join("doc_cold").exists());
        let doc = json_obj(vec![("name", JsonValue::Str("Alice".into()))]);
        let id = store.insert(doc.clone());
        assert_eq!(store.get(id), Some(&doc));
    }

    #[test]
    fn test_doc_cold_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DocumentStore::open(dir.path()).unwrap();
        store.max_hot_docs = 10;
        // Insert 30 docs — should trigger eviction
        for i in 0..30 {
            let doc = json_obj(vec![("seq", JsonValue::Number(i as f64))]);
            store.insert(doc);
        }
        // Hot tier should have at most max_hot_docs entries
        assert!(store.len_hot() <= 10, "hot should have <= 10, got {}", store.len_hot());
        // All 30 should be accessible via get (hot) or get_promoting (cold)
        for id in 1..=30u64 {
            let doc = store.get_promoting(id);
            assert!(doc.is_some(), "doc {id} should be accessible");
        }
    }

    #[test]
    fn test_doc_cold_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DocumentStore::open(dir.path()).unwrap();
        store.max_hot_docs = 5;
        for i in 0..20 {
            let doc = json_obj(vec![("val", JsonValue::Number(i as f64))]);
            store.insert(doc);
        }
        // Access an evicted doc — should be promoted back to hot
        let doc = store.get_promoting(1);
        assert!(doc.is_some(), "cold doc should be promotable");
        // Now it should be in hot
        assert!(store.get(1).is_some(), "promoted doc should be in hot tier");
    }

    #[test]
    fn test_doc_cold_persistence() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = DocumentStore::open(dir.path()).unwrap();
            store.max_hot_docs = 5;
            for i in 0..20 {
                let doc = json_obj(vec![("val", JsonValue::Number(i as f64))]);
                store.insert(doc);
            }
            // Force flush cold LsmTree
            if let Some(ref cold) = store.cold {
                cold.lock().force_flush();
            }
        }
        // Reopen — WAL restores hot entries, cold persists independently
        let mut store2 = DocumentStore::open(dir.path()).unwrap();
        // All docs should be accessible
        for id in 1..=20u64 {
            let doc = store2.get_promoting(id);
            assert!(doc.is_some(), "doc {id} should survive reopen");
        }
    }

    #[test]
    fn test_doc_memory_mode_no_cold() {
        let store = DocumentStore::new();
        assert!(!store.has_cold_tier(), "memory mode should have no cold tier");
    }
}
