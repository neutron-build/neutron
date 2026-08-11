//! FTS-model differential fuzzer: drives Nucleus's FTS functions (FTS_INDEX,
//! FTS_REMOVE, FTS_SEARCH, FTS_MATCH, FTS_DOC_COUNT, FTS_TERM_COUNT) through
//! the SQL surface and checks results against a plain-Rust reference oracle.
//!
//! What we compare (NOT BM25 scores — those are implementation-specific):
//!   - SET MEMBERSHIP: which doc_ids appear in FTS_SEARCH results for a query
//!   - FTS_MATCH: per-doc membership agrees with the search result set
//!   - FTS_DOC_COUNT: matches oracle's indexed-document count
//!   - FTS_TERM_COUNT: matches oracle's unique-term count
//!
//! The reference oracle replicates Nucleus's exact tokenizer pipeline
//! (split on non-alphanumeric, strip leading/trailing apostrophes, lowercase,
//! filter stopwords, Porter-stem). This is necessary to predict which tokens
//! are actually stored so we compare the right thing.
//!
//! Build: `cargo run --release --features server --bin probe_fts`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal fuzz harness

use std::collections::{HashMap, HashSet};
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
        if n == 0 {
            return 0;
        }
        (self.next() % n as u64) as usize
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
}

// ─── Reference oracle: same tokenizer + inverted-index as Nucleus FTS ─────────

// The oracle uses the ENGINE's own stopword list and stemmer rather than a
// copy of them. It used to carry a hand-transcribed duplicate labelled
// "exact copy of Nucleus's stem()", which is a promise nothing enforced: a
// differential fuzzer whose oracle drifts from the engine either stops
// finding real divergences or invents fake ones. Importing removes the
// possibility. A-014.
use nucleus::fts::{is_stopword, stem};

/// Tokenize text: same logic as Nucleus's tokenize().
fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in text.split(|c: char| !c.is_alphanumeric() && c != '\'') {
        let word = word.trim_matches('\'');
        if word.is_empty() {
            continue;
        }
        let lower = word.to_lowercase();
        if is_stopword(&lower) {
            continue;
        }
        tokens.push(stem(&lower));
    }
    tokens
}

/// Plain-Rust reference inverted index.
#[derive(Default)]
struct RefIndex {
    /// term → set of doc_ids
    postings: HashMap<String, HashSet<u64>>,
    /// doc_id → set of terms (for removal)
    doc_terms: HashMap<u64, HashSet<String>>,
}

impl RefIndex {
    fn add(&mut self, doc_id: u64, text: &str) {
        // Re-adding replaces existing entry
        if self.doc_terms.contains_key(&doc_id) {
            self.remove(doc_id);
        }
        let toks = tokenize(text);
        let mut terms: HashSet<String> = HashSet::new();
        for t in toks {
            terms.insert(t.clone());
            self.postings.entry(t).or_default().insert(doc_id);
        }
        self.doc_terms.insert(doc_id, terms);
    }

    fn remove(&mut self, doc_id: u64) {
        if let Some(terms) = self.doc_terms.remove(&doc_id) {
            for term in &terms {
                if let Some(set) = self.postings.get_mut(term) {
                    set.remove(&doc_id);
                    if set.is_empty() {
                        self.postings.remove(term);
                    }
                }
            }
        }
    }

    /// OR-semantics membership: doc_ids that contain at least one query term.
    fn search_members(&self, query: &str) -> HashSet<u64> {
        let mut result = HashSet::new();
        for term in tokenize(query) {
            if let Some(ids) = self.postings.get(&term) {
                result.extend(ids.iter().copied());
            }
        }
        result
    }

    /// Does doc_id match any query term?
    fn match_doc(&self, doc_id: u64, query: &str) -> bool {
        for term in tokenize(query) {
            if let Some(ids) = self.postings.get(&term) {
                if ids.contains(&doc_id) {
                    return true;
                }
            }
        }
        false
    }

    fn doc_count(&self) -> u64 {
        self.doc_terms.len() as u64
    }

    fn term_count(&self) -> usize {
        self.postings.len()
    }
}

// ─── Executor helpers ─────────────────────────────────────────────────────────

fn run_scalar(ex: &Executor, sql: &str) -> Result<Value, ()> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .into_iter()
                .next()
                .and_then(|mut r| r.pop())
                .unwrap_or(Value::Null)),
            _ => Err(()),
        },
        _ => Err(()),
    }
}

fn run_bool(ex: &Executor, sql: &str) -> Option<bool> {
    match run_scalar(ex, sql) {
        Ok(Value::Bool(b)) => Some(b),
        Ok(Value::Int64(n)) => Some(n != 0),
        Ok(Value::Int32(n)) => Some(n != 0),
        _ => None,
    }
}

fn run_i64(ex: &Executor, sql: &str) -> Option<i64> {
    match run_scalar(ex, sql) {
        Ok(Value::Int64(n)) => Some(n),
        Ok(Value::Int32(n)) => Some(n as i64),
        _ => None,
    }
}

/// Parse a FTS_SEARCH JSON result into doc_id set.
/// The format is: [{"doc_id":1,"score":0.123}, ...]
fn parse_search_ids(v: &Value) -> Option<HashSet<u64>> {
    let s = match v {
        Value::Text(s) => s,
        _ => return None,
    };
    // Quick hand-parse without pulling in a JSON dep.
    let mut ids = HashSet::new();
    for chunk in s.split("doc_id\":") {
        // After "doc_id":" comes the number before the next comma or brace
        let rest = chunk.trim_start_matches(|c: char| c == ' ');
        if rest.is_empty() || rest.starts_with('[') {
            continue;
        }
        let num_s: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(id) = num_s.parse::<u64>() {
            ids.insert(id);
        }
    }
    Some(ids)
}

fn run_search_ids(ex: &Executor, query: &str, limit: usize) -> Option<HashSet<u64>> {
    // Escape single quotes in query: replace ' with ''
    let safe_q = query.replace('\'', "''");
    let sql = format!("SELECT FTS_SEARCH('{safe_q}', {limit})");
    match run_scalar(ex, &sql) {
        Ok(v) => parse_search_ids(&v),
        Err(_) => None,
    }
}

// ─── Small corpus ─────────────────────────────────────────────────────────────

/// Words that are NOT stopwords so they actually get indexed.
const WORDS: &[&str] = &[
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "gamma", "hotel", "index", "kilo",
    "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform",
    "victor", "whiskey", "xray", "yankee", "zulu", "rust", "fast", "slow", "quick", "brown", "fox",
    "jump", "lazy", "data", "base", "query", "search", "text",
];

fn gen_doc(rng: &mut Rng) -> String {
    let n = 2 + rng.below(6);
    (0..n)
        .map(|_| *rng.pick(WORDS))
        .collect::<Vec<_>>()
        .join(" ")
}

fn gen_query(rng: &mut Rng) -> String {
    let n = 1 + rng.below(3);
    (0..n)
        .map(|_| *rng.pick(WORDS))
        .collect::<Vec<_>>()
        .join(" ")
}

// ─── Main harness ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Op {
    Index(u64, String),
    Remove(u64),
    Search(String),
    Match(u64, String),
    DocCount,
    TermCount,
}

fn gen_op(rng: &mut Rng, doc_ids: &[u64]) -> Op {
    match rng.below(10) {
        0 | 1 | 2 | 3 => {
            let id = 1 + rng.below(8) as u64;
            Op::Index(id, gen_doc(rng))
        }
        4 | 5 => {
            let id = if !doc_ids.is_empty() && rng.chance(70) {
                *rng.pick(doc_ids)
            } else {
                1 + rng.below(8) as u64
            };
            Op::Remove(id)
        }
        6 | 7 => Op::Search(gen_query(rng)),
        8 => {
            let id = if !doc_ids.is_empty() && rng.chance(70) {
                *rng.pick(doc_ids)
            } else {
                1 + rng.below(8) as u64
            };
            Op::Match(id, gen_query(rng))
        }
        _ => {
            if rng.chance(50) {
                Op::DocCount
            } else {
                Op::TermCount
            }
        }
    }
}

fn main_impl() {
    let mut seed: u64 = 0xF75A_B3C1;
    let mut iterations = 3000usize;
    let mut ops_per = 50usize;
    let mut max_report = 20usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().unwrap();
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
    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus FTS differential fuzzer (membership + counts vs reference oracle)");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));

        let mut oracle = RefIndex::default();
        let mut indexed_ids: Vec<u64> = Vec::new();
        let mut op_log: Vec<String> = Vec::new();

        let mut iter_divs = 0usize;

        for _step in 0..ops_per {
            total += 1;
            let op = gen_op(&mut rng, &indexed_ids);
            op_log.push(format!("{:?}", op));

            match &op {
                Op::Index(doc_id, text) => {
                    // Apply to oracle
                    oracle.add(*doc_id, text);
                    // Keep indexed_ids up to date
                    if !indexed_ids.contains(doc_id) {
                        indexed_ids.push(*doc_id);
                    }
                    // Apply to Nucleus
                    let safe_text = text.replace('\'', "''");
                    let sql = format!("SELECT FTS_INDEX({doc_id}, '{safe_text}')");
                    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let rt = tokio::runtime::Handle::current();
                        tokio::task::block_in_place(|| rt.block_on(ex.execute(&sql)))
                    }));
                    match res {
                        Err(_) => {
                            panics += 1;
                            if panics <= max_report {
                                println!("─── PANIC (iter {iter}) FTS_INDEX ───");
                                println!("  sql: {sql}\n");
                            }
                        }
                        Ok(Err(e)) => {
                            divergences += 1;
                            iter_divs += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) FTS_INDEX errored ───"
                                );
                                println!("  sql: {sql}");
                                println!("  err: {e:?}\n");
                            }
                        }
                        Ok(Ok(_)) => {} // success — expected
                    }
                }

                Op::Remove(doc_id) => {
                    oracle.remove(*doc_id);
                    indexed_ids.retain(|&x| x != *doc_id);
                    let sql = format!("SELECT FTS_REMOVE({doc_id})");
                    // FTS_REMOVE on a non-existent doc is implementation-defined;
                    // we only care it doesn't panic.
                    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let rt = tokio::runtime::Handle::current();
                        tokio::task::block_in_place(|| rt.block_on(ex.execute(&sql)))
                    }));
                    if res.is_err() {
                        panics += 1;
                        if panics <= max_report {
                            println!("─── PANIC (iter {iter}) FTS_REMOVE({doc_id}) ───\n");
                        }
                    }
                }

                Op::Search(query) => {
                    let expected = oracle.search_members(query);
                    let got = run_search_ids(&ex, query, 1000);
                    match got {
                        None => {
                            // If expected is non-empty this is a real problem;
                            // if empty and nucleus returned error, could be
                            // acceptable. Only flag when expected has results.
                            if !expected.is_empty() {
                                divergences += 1;
                                iter_divs += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) FTS_SEARCH errored but expected hits ───"
                                    );
                                    println!("  query   : {query}");
                                    println!("  expected ids: {expected:?}\n");
                                }
                            }
                        }
                        Some(got_ids) => {
                            // Compare membership sets (not order, not scores)
                            if got_ids != expected {
                                divergences += 1;
                                iter_divs += 1;
                                if divergences <= max_report {
                                    let missing: Vec<u64> =
                                        expected.difference(&got_ids).copied().collect();
                                    let extra: Vec<u64> =
                                        got_ids.difference(&expected).copied().collect();
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) FTS_SEARCH membership ───"
                                    );
                                    println!("  query   : {query}");
                                    println!("  expected ids : {expected:?}");
                                    println!("  nucleus  ids : {got_ids:?}");
                                    if !missing.is_empty() {
                                        println!("  missing (nucleus missed): {missing:?}");
                                    }
                                    if !extra.is_empty() {
                                        println!("  extra   (nucleus added): {extra:?}");
                                    }
                                    println!("  ── op log ({} ops) ──", op_log.len());
                                    for l in &op_log {
                                        println!("    {l}");
                                    }
                                    println!();
                                }
                            }
                        }
                    }
                }

                Op::Match(doc_id, query) => {
                    let expected = oracle.match_doc(*doc_id, query);
                    let sql_m = {
                        let safe_q = query.replace('\'', "''");
                        format!("SELECT FTS_MATCH({doc_id}, '{safe_q}')")
                    };
                    match run_bool(&ex, &sql_m) {
                        None => {
                            // If oracle says match, this is a real divergence
                            if expected {
                                divergences += 1;
                                iter_divs += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) FTS_MATCH errored but expected true ───"
                                    );
                                    println!("  {sql_m}\n");
                                }
                            }
                        }
                        Some(got) => {
                            if got != expected {
                                divergences += 1;
                                iter_divs += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) FTS_MATCH ───"
                                    );
                                    println!("  sql      : {sql_m}");
                                    println!("  expected : {expected}");
                                    println!("  nucleus  : {got}\n");
                                }
                            }
                        }
                    }
                }

                Op::DocCount => {
                    let expected = oracle.doc_count() as i64;
                    match run_i64(&ex, "SELECT FTS_DOC_COUNT()") {
                        None => {
                            divergences += 1;
                            iter_divs += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) FTS_DOC_COUNT errored ───"
                                );
                                println!("  expected: {expected}\n");
                            }
                        }
                        Some(got) => {
                            if got != expected {
                                divergences += 1;
                                iter_divs += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) FTS_DOC_COUNT ───"
                                    );
                                    println!("  expected: {expected}");
                                    println!("  nucleus : {got}");
                                    println!("  ── op log ──");
                                    for l in &op_log {
                                        println!("    {l}");
                                    }
                                    println!();
                                }
                            }
                        }
                    }
                }

                Op::TermCount => {
                    let expected = oracle.term_count() as i64;
                    match run_i64(&ex, "SELECT FTS_TERM_COUNT()") {
                        None => {
                            divergences += 1;
                            iter_divs += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) FTS_TERM_COUNT errored ───"
                                );
                                println!("  expected: {expected}\n");
                            }
                        }
                        Some(got) => {
                            if got != expected {
                                divergences += 1;
                                iter_divs += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) FTS_TERM_COUNT ───"
                                    );
                                    println!("  expected: {expected}");
                                    println!("  nucleus : {got}");
                                    println!("  ── op log ──");
                                    for l in &op_log {
                                        println!("    {l}");
                                    }
                                    println!();
                                }
                            }
                        }
                    }
                }
            }

            // Stop burning cycles if this iteration already found problems
            if iter_divs > 0 {
                break;
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("ops run            : {total}");
    println!("FTS divergences    : {divergences}");
    println!("panics             : {panics}");
    if divergences == 0 && panics == 0 {
        println!("\nNo FTS divergences, no panics vs reference oracle.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
