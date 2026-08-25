//! TimeSeries + Document differential fuzzer (two sections in one binary).
//!
//! **Section 1 – TimeSeries:** inserts random (timestamp, value) pairs into a
//! named series and checks TS_COUNT, TS_RANGE_COUNT, TS_RANGE_AVG, and TS_LAST
//! against a plain `Vec<(u64,f64)>` oracle.  Float averages use a relative+
//! absolute tolerance.
//!
//! **Section 2 – Document:** inserts random JSON documents, then checks
//! DOC_COUNT, DOC_GET (round-trip), DOC_PATH (nested field extraction), and
//! DOC_QUERY (containment filter) against a serde_json oracle.
//!
//! Build:
//!   cargo build --release --features server --bin probe_tsdoc
//! Run:
//!   cargo run  --release --features server --bin probe_tsdoc
//!   cargo run  --release --features server --bin probe_tsdoc -- --seed 42
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG (xorshift64) ─────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn u64_below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    /// Returns a float in [-100.0, 100.0] with 1-decimal precision.
    fn small_f64(&mut self) -> f64 {
        let raw = self.next() % 2001; // 0..=2000
        (raw as f64 - 1000.0) / 10.0
    }
}

// ─── Executor runner helpers ──────────────────────────────────────────────────

/// Run a SELECT and return the first cell as a String, or Err(()) on
/// error/empty/panic.
fn run_str(ex: &Executor, sql: &str) -> Result<String, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                let v = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(v.to_string())
            }
            _ => Err(()),
        },
        Ok(Err(_)) => Err(()),
        Err(_) => Err(()), // panic
    }
}

/// Run a SELECT and return the first cell as f64 (Null → None).
fn run_f64(ex: &Executor, sql: &str) -> Result<Option<f64>, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Null) | None => Ok(None),
                Some(Value::Float64(f)) => Ok(Some(*f)),
                Some(Value::Int64(n)) => Ok(Some(*n as f64)),
                Some(Value::Int32(n)) => Ok(Some(*n as f64)),
                _ => Err(()),
            },
            _ => Err(()),
        },
        Ok(Err(_)) => Err(()),
        Err(_) => Err(()),
    }
}

/// Run a SELECT and return the first cell as i64.
fn run_i64(ex: &Executor, sql: &str) -> Result<i64, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Int64(n)) => Ok(*n),
                Some(Value::Int32(n)) => Ok(*n as i64),
                _ => Err(()),
            },
            _ => Err(()),
        },
        Ok(Err(_)) => Err(()),
        Err(_) => Err(()),
    }
}

/// Detect panic (for divergence diagnosis).
fn is_panic(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .is_err()
}

// ─── Float tolerance ─────────────────────────────────────────────────────────

/// True when the absolute difference is within eps OR within relative eps of
/// the expected value.  We use 1e-9 absolute and 1e-6 relative — both are
/// loose enough to absorb IEEE-754 summation order variations.
fn float_near(got: f64, expected: f64) -> bool {
    let abs_diff = (got - expected).abs();
    if abs_diff <= 1e-9 {
        return true;
    }
    let rel = abs_diff / expected.abs().max(1e-12);
    rel <= 1e-6
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 1 — TimeSeries
// ═══════════════════════════════════════════════════════════════════════════════

/// In-memory reference state for one named series.
#[derive(Default)]
struct TsRef {
    /// timestamp -> value, deduplicated last-write-wins (insertion order
    /// decides among equal timestamps) — the TS store's LWW semantics
    /// (DKV-9). Duplicates are retries (OTLP does this constantly) and must
    /// not double-count in aggregates.
    points: std::collections::BTreeMap<u64, f64>,
}

impl TsRef {
    fn insert(&mut self, ts: u64, val: f64) {
        self.points.insert(ts, val);
    }

    fn count(&self) -> i64 {
        self.points.len() as i64
    }

    fn range_count(&self, start: u64, end: u64) -> i64 {
        self.points.range(start..=end).count() as i64
    }

    fn range_avg(&self, start: u64, end: u64) -> Option<f64> {
        let vals: Vec<f64> = self.range(start, end);
        if vals.is_empty() {
            None
        } else {
            Some(vals.iter().sum::<f64>() / vals.len() as f64)
        }
    }

    fn range(&self, start: u64, end: u64) -> Vec<f64> {
        self.points.range(start..=end).map(|(_, v)| *v).collect()
    }

    /// Last value = the one with the maximum timestamp; among writes at that
    /// timestamp, the last write won — matching the TS engine's LWW columnar
    /// store and its last_values cache.
    fn last(&self) -> Option<f64> {
        self.points.values().next_back().copied()
    }
}

const TS_SERIES: &[&str] = &["s0", "s1", "s2"];

fn run_ts_section(
    rng: &mut Rng,
    iter: usize,
    ops: usize,
    max_report: usize,
    divs: &mut usize,
    panics: &mut usize,
    total: &mut usize,
) {
    // Fresh executor per iteration (clean state).
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    // Reference oracles for each series.
    let mut refs: Vec<TsRef> = (0..TS_SERIES.len()).map(|_| TsRef::default()).collect();

    for _ in 0..ops {
        let si = rng.below(TS_SERIES.len());
        let series = TS_SERIES[si];
        let ref_oracle = &mut refs[si];

        let op = rng.below(5);
        match op {
            // TS_INSERT
            0 => {
                let ts = rng.u64_below(1_000_000);
                let val = rng.small_f64();
                let sql = format!("SELECT TS_INSERT('{series}',{ts},{val})");
                *total += 1;
                ref_oracle.insert(ts, val);
                match run_str(&ex, &sql) {
                    Ok(got) if got == "OK" => {}
                    Ok(got) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : OK");
                            println!("  got      : {got}");
                            println!();
                        }
                    }
                    Err(_) => {
                        if is_panic(&ex, &sql) {
                            *panics += 1;
                            if *panics <= max_report {
                                println!("─── TS PANIC #{panics} (iter {iter}) ───");
                                println!("  sql: {sql}\n");
                            }
                        } else {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : OK");
                                println!("  got      : Err");
                                println!();
                            }
                        }
                    }
                }
            }
            // TS_COUNT
            1 => {
                let sql = format!("SELECT TS_COUNT('{series}')");
                *total += 1;
                let expected = ref_oracle.count();
                match run_i64(&ex, &sql) {
                    Ok(got) if got == expected => {}
                    Ok(got) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected}");
                            println!("  got      : {got}");
                            println!();
                        }
                    }
                    Err(_) => {
                        if is_panic(&ex, &sql) {
                            *panics += 1;
                        } else {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : {expected}  got: Err");
                                println!();
                            }
                        }
                    }
                }
            }
            // TS_RANGE_COUNT
            2 => {
                let a = rng.u64_below(1_000_000);
                let b = rng.u64_below(1_000_000);
                let (start, end) = if a <= b { (a, b) } else { (b, a) };
                let sql = format!("SELECT TS_RANGE_COUNT('{series}',{start},{end})");
                *total += 1;
                let expected = ref_oracle.range_count(start, end);
                match run_i64(&ex, &sql) {
                    Ok(got) if got == expected => {}
                    Ok(got) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected}");
                            println!("  got      : {got}");
                            println!();
                        }
                    }
                    Err(_) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected}  got: Err");
                            println!();
                        }
                    }
                }
            }
            // TS_RANGE_AVG
            3 => {
                let a = rng.u64_below(1_000_000);
                let b = rng.u64_below(1_000_000);
                let (start, end) = if a <= b { (a, b) } else { (b, a) };
                let sql = format!("SELECT TS_RANGE_AVG('{series}',{start},{end})");
                *total += 1;
                let expected = ref_oracle.range_avg(start, end);
                match run_f64(&ex, &sql) {
                    Ok(got) => {
                        let ok = match (expected, got) {
                            (None, None) => true,
                            (Some(e), Some(g)) => float_near(g, e),
                            _ => false,
                        };
                        if !ok {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : {expected:?}");
                                println!("  got      : {got:?}");
                                println!();
                            }
                        }
                    }
                    Err(_) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected:?}  got: Err");
                            println!();
                        }
                    }
                }
            }
            // TS_LAST
            _ => {
                let sql = format!("SELECT TS_LAST('{series}')");
                *total += 1;
                let expected = ref_oracle.last();
                match run_f64(&ex, &sql) {
                    Ok(got) => {
                        let ok = match (expected, got) {
                            (None, None) => true,
                            (Some(e), Some(g)) => float_near(g, e),
                            _ => false,
                        };
                        if !ok {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : {expected:?}");
                                println!("  got      : {got:?}");
                                println!();
                            }
                        }
                    }
                    Err(_) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── TS DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected:?}  got: Err");
                            println!();
                        }
                    }
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SECTION 2 — Document
// ═══════════════════════════════════════════════════════════════════════════════

/// Reference document store (maps id → serde_json::Value, 1-based monotonic).
struct DocRef {
    docs: Vec<(u64, serde_json::Value)>, // sorted by id, ids are 1,2,3,...
    next_id: u64,
}

impl DocRef {
    fn new() -> Self {
        DocRef {
            docs: Vec::new(),
            next_id: 1,
        }
    }

    fn insert(&mut self, v: serde_json::Value) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.docs.push((id, v));
        id
    }

    fn get(&self, id: u64) -> Option<&serde_json::Value> {
        self.docs.iter().find(|(i, _)| *i == id).map(|(_, v)| v)
    }

    fn count(&self) -> i64 {
        self.docs.len() as i64
    }

    /// path extraction: returns the leaf as a canonical JSON string, or None.
    fn get_path(&self, id: u64, path: &[&str]) -> Option<String> {
        let doc = self.get(id)?;
        let mut cur = doc;
        for key in path {
            match cur {
                serde_json::Value::Object(m) => {
                    cur = m.get(*key)?;
                }
                _ => return None,
            }
        }
        Some(canonical_json(cur))
    }

    /// @> containment: return sorted IDs of docs that contain `query`.
    fn query_contains(&self, query: &serde_json::Value) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .docs
            .iter()
            .filter(|(_, doc)| json_contains(doc, query))
            .map(|(id, _)| *id)
            .collect();
        ids.sort();
        ids
    }
}

/// Reference containment operator (@>).
fn json_contains(doc: &serde_json::Value, query: &serde_json::Value) -> bool {
    match (doc, query) {
        (serde_json::Value::Object(d), serde_json::Value::Object(q)) => q
            .iter()
            .all(|(k, qv)| d.get(k).is_some_and(|dv| json_contains(dv, qv))),
        (serde_json::Value::Array(d), serde_json::Value::Array(q)) => {
            q.iter().all(|qv| d.iter().any(|dv| json_contains(dv, qv)))
        }
        (a, b) => a == b,
    }
}

/// Produce a canonical JSON string matching Nucleus's to_json_string() output.
/// Numbers that are whole → integer, else floating point with default Display.
fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => {
            let f = n
                .as_f64()
                .or_else(|| n.as_i64().map(|i| i as f64))
                .unwrap_or(0.0);
            if f.fract() == 0.0 && f.is_finite() && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                format!("{}", f as i64)
            } else {
                format!("{f}")
            }
        }
        serde_json::Value::String(s) => format!("\"{}\"", escape_json(s)),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        serde_json::Value::Object(map) => {
            // Nucleus uses BTreeMap → alphabetical key order.
            let mut sorted: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            sorted.sort_by_key(|(k, _)| k.as_str());
            let items: Vec<String> = sorted
                .iter()
                .map(|(k, v)| format!("\"{}\":{}", escape_json(k), canonical_json(v)))
                .collect();
            format!("{{{}}}", items.join(","))
        }
    }
}

fn escape_json(s: &str) -> String {
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

// Small set of documents to keep state manageable.
const DOC_TEMPLATES: &[&str] = &[
    r#"{"name":"alice","age":30,"active":true}"#,
    r#"{"name":"bob","age":25,"active":false}"#,
    r#"{"name":"carol","score":99,"tags":["a","b"]}"#,
    r#"{"x":1,"y":2,"z":{"deep":42}}"#,
    r#"{"k":"v","n":0}"#,
];

// Simple containment queries (subsets of the docs above).
const DOC_QUERIES: &[&str] = &[
    r#"{"active":true}"#,
    r#"{"active":false}"#,
    r#"{"age":30}"#,
    r#"{"age":25}"#,
    r#"{"name":"alice"}"#,
    r#"{"name":"bob"}"#,
    r#"{"n":0}"#,
    r#"{"score":99}"#,
];

// (doc_template_index, path_keys) pairs to exercise DOC_PATH.
const DOC_PATHS: &[(&str, &[&str])] = &[
    ("name", &["name"]),
    ("age", &["age"]),
    ("active", &["active"]),
    ("z.deep", &["z", "deep"]),
    ("k", &["k"]),
    ("score", &["score"]),
    ("missing", &["missing"]),        // should return NULL
    ("z.missing", &["z", "missing"]), // should return NULL
];

/// Escape single quotes for SQL string literals (naive — our values are clean).
fn sq(s: &str) -> String {
    s.replace('\'', "''")
}

fn run_doc_section(
    rng: &mut Rng,
    iter: usize,
    ops: usize,
    max_report: usize,
    divs: &mut usize,
    panics: &mut usize,
    total: &mut usize,
) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));
    let mut oracle = DocRef::new();

    for _ in 0..ops {
        let op = rng.below(5);
        match op {
            // DOC_INSERT
            0 => {
                let tmpl = rng.pick(DOC_TEMPLATES);
                let sql = format!("SELECT DOC_INSERT('{}')", sq(tmpl));
                *total += 1;
                let expected_id = oracle.insert(serde_json::from_str(tmpl).unwrap());
                match run_i64(&ex, &sql) {
                    Ok(got_id) if got_id == expected_id as i64 => {}
                    Ok(got_id) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected id : {expected_id}");
                            println!("  got      : {got_id}");
                            println!();
                        }
                    }
                    Err(_) => {
                        if is_panic(&ex, &sql) {
                            *panics += 1;
                            if *panics <= max_report {
                                println!("─── DOC PANIC #{panics} (iter {iter}) ───");
                                println!("  sql: {sql}\n");
                            }
                        } else {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected id: {expected_id}  got: Err");
                                println!();
                            }
                        }
                    }
                }
            }
            // DOC_COUNT
            1 => {
                let sql = "SELECT DOC_COUNT()".to_string();
                *total += 1;
                let expected = oracle.count();
                match run_i64(&ex, &sql) {
                    Ok(got) if got == expected => {}
                    Ok(got) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected}");
                            println!("  got      : {got}");
                            println!();
                        }
                    }
                    Err(_) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected}  got: Err");
                            println!();
                        }
                    }
                }
            }
            // DOC_GET round-trip
            2 => {
                if oracle.count() == 0 {
                    continue;
                }
                // Pick a valid id (1-indexed).
                let max_id = oracle.next_id - 1;
                let id = rng.u64_below(max_id) + 1;
                let sql = format!("SELECT DOC_GET({id})");
                *total += 1;
                match oracle.get(id) {
                    None => {
                        // id was removed — shouldn't happen in our oracle but
                        // if it does, expect NULL.
                        match run_str(&ex, &sql) {
                            Ok(got) if got == "NULL" => {}
                            Ok(got) => {
                                *divs += 1;
                                if *divs <= max_report {
                                    println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                    println!("  sql      : {sql}");
                                    println!("  expected : NULL  got: {got}");
                                    println!();
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    Some(expected_doc) => {
                        let expected_json = canonical_json(expected_doc);
                        match run_str(&ex, &sql) {
                            Ok(got) if got == expected_json => {}
                            Ok(got) => {
                                *divs += 1;
                                if *divs <= max_report {
                                    println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                    println!("  sql      : {sql}");
                                    println!("  expected : {expected_json}");
                                    println!("  got      : {got}");
                                    println!();
                                }
                            }
                            Err(_) => {
                                if is_panic(&ex, &sql) {
                                    *panics += 1;
                                } else {
                                    *divs += 1;
                                    if *divs <= max_report {
                                        println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                        println!("  sql      : {sql}");
                                        println!("  expected : {expected_json}  got: Err");
                                        println!();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // DOC_PATH
            3 => {
                if oracle.count() == 0 {
                    continue;
                }
                let max_id = oracle.next_id - 1;
                let id = rng.u64_below(max_id) + 1;
                let (_, path) = rng.pick(DOC_PATHS);
                // Build SQL: DOC_PATH(id, 'k1', 'k2', ...)
                let keys_sql: Vec<String> = path.iter().map(|k| format!("'{}'", sq(k))).collect();
                let sql = format!("SELECT DOC_PATH({},{})", id, keys_sql.join(","));
                *total += 1;

                let expected = oracle.get_path(id, path);
                let expected_str = expected.as_deref().unwrap_or("NULL");

                match run_str(&ex, &sql) {
                    Ok(got) if got == expected_str => {}
                    Ok(got) => {
                        *divs += 1;
                        if *divs <= max_report {
                            println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                            println!("  sql      : {sql}");
                            println!("  expected : {expected_str}");
                            println!("  got      : {got}");
                            println!();
                        }
                    }
                    Err(_) => {
                        if is_panic(&ex, &sql) {
                            *panics += 1;
                        } else {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : {expected_str}  got: Err");
                                println!();
                            }
                        }
                    }
                }
            }
            // DOC_QUERY containment filter
            _ => {
                if oracle.count() == 0 {
                    continue;
                }
                let q_str = rng.pick(DOC_QUERIES);
                let q_val: serde_json::Value = serde_json::from_str(q_str).unwrap();
                let sql = format!("SELECT DOC_QUERY('{}')", sq(q_str));
                *total += 1;

                let expected_ids = oracle.query_contains(&q_val);
                let expected_str: String = expected_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                // Nucleus returns "" when empty, not "NULL".

                match run_str(&ex, &sql) {
                    Ok(got) => {
                        // got may be "NULL" if the result set is empty AND the
                        // executor returns Null; normalise both empty cases.
                        let got_norm = if got == "NULL" { String::new() } else { got };
                        if got_norm != expected_str {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : '{expected_str}'");
                                println!("  got      : '{got_norm}'");
                                println!();
                            }
                        }
                    }
                    Err(_) => {
                        if is_panic(&ex, &sql) {
                            *panics += 1;
                        } else {
                            *divs += 1;
                            if *divs <= max_report {
                                println!("─── DOC DIVERGENCE #{divs} (iter {iter}) ───");
                                println!("  sql      : {sql}");
                                println!("  expected : '{expected_str}'  got: Err");
                                println!();
                            }
                        }
                    }
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// main
// ═══════════════════════════════════════════════════════════════════════════════

fn main_impl() {
    let mut seed: u64 = 0x9E37_79B9_4CA8;
    let mut iterations = 3000usize;
    let mut ops_per = 50usize;
    let mut max_report = 20usize;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                let s = &args[i];
                seed = if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                    u64::from_str_radix(hex, 16).unwrap()
                } else {
                    s.parse().unwrap()
                };
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().unwrap();
            }
            "--ops" => {
                i += 1;
                ops_per = args[i].parse().unwrap();
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    std::panic::set_hook(Box::new(|_| {})); // suppress backtraces

    println!("Nucleus TimeSeries + Document differential fuzzer");
    println!("seed={seed}  iterations={iterations}  ops/iter={ops_per}\n");

    let mut ts_total = 0usize;
    let mut ts_divs = 0usize;
    let mut ts_panics = 0usize;
    let mut doc_total = 0usize;
    let mut doc_divs = 0usize;
    let mut doc_panics = 0usize;

    for iter in 0..iterations {
        // Each iteration gets a deterministic seed derived from the global seed.
        let iter_seed = seed
            .wrapping_add(iter as u64)
            .wrapping_mul(0x100000001B3u64)
            .wrapping_add(0xCBF29CE484222325);

        let mut ts_rng = Rng(iter_seed);
        run_ts_section(
            &mut ts_rng,
            iter,
            ops_per,
            max_report,
            &mut ts_divs,
            &mut ts_panics,
            &mut ts_total,
        );

        // Use a different scramble so TS and DOC get independent streams.
        let mut doc_rng = Rng(iter_seed ^ 0xDEADBEEF_CAFEF00D);
        run_doc_section(
            &mut doc_rng,
            iter,
            ops_per,
            max_report,
            &mut doc_divs,
            &mut doc_panics,
            &mut doc_total,
        );

        if ts_panics + doc_panics > max_report {
            println!("Too many panics — aborting early.");
            std::process::exit(1);
        }
    }

    println!("\n════ SUMMARY ════");
    println!("─ TimeSeries ─────────────────────────────");
    println!("  ops run            : {ts_total}");
    println!("  divergences        : {ts_divs}");
    println!("  panics             : {ts_panics}");
    println!("─ Document ───────────────────────────────");
    println!("  ops run            : {doc_total}");
    println!("  divergences        : {doc_divs}");
    println!("  panics             : {doc_panics}");
    println!("──────────────────────────────────────────");

    let all_ok = ts_divs == 0 && ts_panics == 0 && doc_divs == 0 && doc_panics == 0;
    if all_ok {
        println!("No divergences, no panics.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
