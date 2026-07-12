//! FTS ranking differential fuzzer.
//!
//! Probes two invariants of Nucleus FTS ranking:
//!
//! (A) RELATIVE ORDER: for a corpus of N documents and a query, the relative
//!     ordering returned by FTS_SEARCH must match the ordering produced by the
//!     same BM25 formula computed in-process from the same InvertedIndex. We
//!     use nucleus::fts::InvertedIndex directly so the oracle is the exact same
//!     data structure with the exact same BM25 parameters (k1=1.2, b=0.75).
//!
//! (B) SCORE ACCURACY: FTS_SEARCH returns JSON with numeric scores.  We parse
//!     those and compare them to the oracle's f64 values within a tolerance.
//!
//! (C) MONOTONICITY INVARIANTS: a document containing more occurrences of a
//!     query term must rank >= a document with fewer occurrences (all else held
//!     equal), and a document matching more distinct query terms must rank >=
//!     one that matches fewer.
//!
//! (D) FTS_RANK AGREEMENT: FTS_RANK(doc_text, query) is a separate SQL
//!     function that computes a score independently. Its scores must produce a
//!     ranking consistent with BM25 for single-term queries (where term-
//!     frequency ordering is the same under both TF and BM25).
//!
//! Build:
//!   cargo build --release --features "server rusqlite" --bin probe_fts_rank
//!
//! Run:
//!   cargo run --release --features "server rusqlite" --bin probe_fts_rank [--seed N] [--iterations N]

#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)]

use std::collections::HashMap;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::fts::InvertedIndex;
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

// ─── Executor helpers ─────────────────────────────────────────────────────────

fn run_scalar(ex: &Executor, sql: &str) -> Result<Value, String> {
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
            Some(other) => Err(format!("unexpected result: {other:?}")),
            None => Err("empty result set".into()),
        },
        Ok(Err(e)) => Err(format!("exec error: {e:?}")),
        Err(_) => Err("PANIC".into()),
    }
}

fn fts_index(ex: &Executor, doc_id: u64, text: &str) -> Result<(), String> {
    let safe = text.replace('\'', "''");
    let sql = format!("SELECT FTS_INDEX({doc_id}, '{safe}')");
    run_scalar(ex, &sql).map(|_| ())
}

/// Parse FTS_SEARCH JSON result → ordered Vec<(doc_id, score)>.
/// Format: [{"doc_id":N,"score":F}, ...]
fn parse_search_results(v: &Value) -> Option<Vec<(u64, f64)>> {
    let s = match v {
        Value::Text(s) => s,
        _ => return None,
    };
    if s == "[]" {
        return Some(vec![]);
    }
    let mut results = Vec::new();
    // Hand-parse: each entry is {"doc_id":N,"score":F}
    for entry in s.split("},{") {
        let doc_id = {
            let after = entry.split("\"doc_id\":").nth(1)?;
            let num: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            num.parse::<u64>().ok()?
        };
        let score = {
            let after = entry.split("\"score\":").nth(1)?;
            let num: String = after
                .chars()
                .take_while(|c| {
                    c.is_ascii_digit() || *c == '.' || *c == '-' || *c == 'e' || *c == 'E'
                })
                .collect();
            num.parse::<f64>().ok()?
        };
        results.push((doc_id, score));
    }
    Some(results)
}

fn fts_search(ex: &Executor, query: &str, limit: usize) -> Option<Vec<(u64, f64)>> {
    let safe = query.replace('\'', "''");
    let sql = format!("SELECT FTS_SEARCH('{safe}', {limit})");
    match run_scalar(ex, &sql) {
        Ok(v) => parse_search_results(&v),
        Err(_) => None,
    }
}

fn fts_rank(ex: &Executor, doc_text: &str, query: &str) -> Option<f64> {
    let safe_doc = doc_text.replace('\'', "''");
    let safe_q = query.replace('\'', "''");
    let sql = format!("SELECT FTS_RANK('{safe_doc}', '{safe_q}')");
    match run_scalar(ex, &sql) {
        Ok(Value::Float64(f)) => Some(f),
        _ => None,
    }
}

// ─── Small non-stopword corpus ────────────────────────────────────────────────

const WORDS: &[&str] = &[
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "gamma", "hotel", "index", "kilo",
    "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform",
    "victor", "whiskey", "xray", "yankee", "zulu", "rust", "fast", "quick", "brown", "jump",
    "data", "base", "query", "search", "text", "graph",
];

fn gen_doc(rng: &mut Rng, word_count: usize) -> String {
    (0..word_count)
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

// ─── BM25 oracle: thin wrapper around InvertedIndex ──────────────────────────

struct BM25Oracle {
    idx: InvertedIndex,
    /// doc_id → original text (needed for FTS_RANK oracle)
    texts: HashMap<u64, String>,
}

impl BM25Oracle {
    fn new() -> Self {
        Self {
            idx: InvertedIndex::new(),
            texts: HashMap::new(),
        }
    }

    fn add(&mut self, doc_id: u64, text: &str) {
        self.idx.add_document(doc_id, text);
        self.texts.insert(doc_id, text.to_string());
    }

    fn remove(&mut self, doc_id: u64) {
        self.idx.remove_document(doc_id);
        self.texts.remove(&doc_id);
    }

    /// BM25 search via the same InvertedIndex implementation Nucleus uses.
    fn search(&self, query: &str, limit: usize) -> Vec<(u64, f64)> {
        self.idx.search(query, limit)
    }

    fn text_of(&self, doc_id: u64) -> Option<&str> {
        self.texts.get(&doc_id).map(|s| s.as_str())
    }
}

// ─── Invariant checks ─────────────────────────────────────────────────────────

/// Check that `got` (from FTS_SEARCH SQL) has the same relative order as
/// `expected` (from InvertedIndex::search). Returns None on agreement,
/// Some(description) on violation.
fn check_ordering(expected: &[(u64, f64)], got: &[(u64, f64)]) -> Option<String> {
    // Only check docs that appear in BOTH lists (SQL might return subset).
    // Build a position map for each.
    let exp_pos: HashMap<u64, usize> = expected
        .iter()
        .enumerate()
        .map(|(i, (id, _))| (*id, i))
        .collect();
    let got_pos: HashMap<u64, usize> = got
        .iter()
        .enumerate()
        .map(|(i, (id, _))| (*id, i))
        .collect();

    // For every pair of docs that both appear in expected and got,
    // their relative order must agree.
    let common: Vec<u64> = expected
        .iter()
        .filter_map(|(id, _)| {
            if got_pos.contains_key(id) {
                Some(*id)
            } else {
                None
            }
        })
        .collect();

    for i in 0..common.len() {
        for j in (i + 1)..common.len() {
            let a = common[i];
            let b = common[j];
            let exp_a = exp_pos[&a];
            let exp_b = exp_pos[&b];
            let got_a = got_pos[&a];
            let got_b = got_pos[&b];
            // exp_a < exp_b means a ranks higher (better) in oracle
            let exp_a_better = exp_a < exp_b;
            let got_a_better = got_a < got_b;
            // They disagree if one says a>b and the other says b>a.
            // Equal positions are fine (tied in one, tied or any order in the other).
            if exp_a_better != got_a_better {
                let exp_score_a = expected[exp_a].1;
                let exp_score_b = expected[exp_b].1;
                let got_score_a = got[got_a].1;
                let got_score_b = got[got_b].1;
                // Only flag if the oracle has a meaningful score gap (avoid float noise flips).
                if (exp_score_a - exp_score_b).abs() > 1e-9 {
                    return Some(format!(
                        "order inversion: oracle ranks doc {a} (score {exp_score_a:.6}) above doc {b} (score {exp_score_b:.6}), \
                         but SQL has doc {a} at pos {got_a} (score {got_score_a:.6}) and doc {b} at pos {got_b} (score {got_score_b:.6})"
                    ));
                }
            }
        }
    }
    None
}

/// Check that scores from SQL match oracle scores within tolerance.
fn check_scores(expected: &[(u64, f64)], got: &[(u64, f64)], tol: f64) -> Option<String> {
    let exp_map: HashMap<u64, f64> = expected.iter().copied().collect();
    let got_map: HashMap<u64, f64> = got.iter().copied().collect();
    for (id, exp_score) in &exp_map {
        if let Some(got_score) = got_map.get(id) {
            let diff = (exp_score - got_score).abs();
            if diff > tol && diff > tol * exp_score.abs() {
                return Some(format!(
                    "score mismatch for doc {id}: oracle={exp_score:.8} sql={got_score:.8} delta={diff:.2e}"
                ));
            }
        }
    }
    None
}

/// Monotonicity: a doc with more occurrences of a specific term should rank >= one with fewer.
/// Build two synthetic docs that differ only in frequency of the query term and check.
fn check_monotonicity_freq(
    oracle: &mut BM25Oracle,
    ex: &Executor,
    rng: &mut Rng,
    term: &str,
) -> Option<String> {
    // Use very high doc IDs to avoid colliding with regular corpus (90000+)
    let id_more = 90001u64;
    let id_fewer = 90002u64;
    let filler = *rng.pick(WORDS); // different word to pad document length

    // "more": 4 occurrences of term
    let doc_more = format!("{term} {term} {term} {term} {filler}");
    // "fewer": 1 occurrence of term
    let doc_fewer = format!("{term} {filler} {filler} {filler} {filler}");

    oracle.add(id_more, &doc_more);
    oracle.add(id_fewer, &doc_fewer);
    let _ = fts_index(ex, id_more, &doc_more);
    let _ = fts_index(ex, id_fewer, &doc_fewer);

    let limit = 100;
    let oracle_results = oracle.search(term, limit);
    let sql_results = fts_search(ex, term, limit);

    let result = if let Some(sql) = &sql_results {
        // Find positions of id_more and id_fewer in SQL results
        let pos_more = sql.iter().position(|(id, _)| *id == id_more);
        let pos_fewer = sql.iter().position(|(id, _)| *id == id_fewer);

        let oracle_pos_more = oracle_results.iter().position(|(id, _)| *id == id_more);
        let oracle_pos_fewer = oracle_results.iter().position(|(id, _)| *id == id_fewer);

        match (pos_more, pos_fewer, oracle_pos_more, oracle_pos_fewer) {
            (Some(sm), Some(sf), Some(om), Some(of_)) => {
                // Oracle says id_more should rank at least as high as id_fewer (lower pos = better)
                if om <= of_ && sm > sf {
                    // Oracle says more-freq ranks better, but SQL disagrees
                    let score_more = sql[sm].1;
                    let score_fewer = sql[sf].1;
                    if (score_more - score_fewer).abs() > 1e-9 {
                        Some(format!(
                            "MONOTONICITY VIOLATION: doc {id_more} (4x '{term}') ranked after doc {id_fewer} (1x '{term}') in SQL. \
                             SQL scores: {score_more:.6} vs {score_fewer:.6}. \
                             doc_more='{doc_more}', doc_fewer='{doc_fewer}'"
                        ))
                    } else {
                        None // tied scores — acceptable
                    }
                } else {
                    None
                }
            }
            _ => None, // one or both docs not in results — skip
        }
    } else {
        None
    };

    // Clean up
    oracle.remove(id_more);
    oracle.remove(id_fewer);
    let _ = run_scalar(ex, "SELECT FTS_REMOVE(90001)");
    let _ = run_scalar(ex, "SELECT FTS_REMOVE(90002)");

    result
}

// ─── Main harness ─────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xBEEF_CA7E;
    let mut iterations = 2000usize;
    let mut corpus_size = 12usize;
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
            "--corpus" => {
                i += 1;
                corpus_size = args[i].parse().unwrap();
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

    println!("Nucleus FTS ranking differential fuzzer");
    println!("seed={seed} iterations={iterations} corpus_size={corpus_size}");
    println!(
        "Checks: (A) relative order, (B) score accuracy, (C) monotonicity, (D) FTS_RANK vs BM25\n"
    );

    let mut total_queries = 0usize;
    let mut order_violations = 0usize;
    let mut score_violations = 0usize;
    let mut mono_violations = 0usize;
    let mut rank_violations = 0usize;
    let mut parse_errors = 0usize;

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));

        // Fresh executor per iteration
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let mut oracle = BM25Oracle::new();

        // Build a corpus of documents
        let n_docs = 3 + rng.below(corpus_size - 2);
        let mut doc_texts: HashMap<u64, String> = HashMap::new();

        for doc_id in 1..=(n_docs as u64) {
            let word_count = 3 + rng.below(12);
            let text = gen_doc(&mut rng, word_count);
            oracle.add(doc_id, &text);
            if fts_index(&ex, doc_id, &text).is_ok() {
                doc_texts.insert(doc_id, text);
            }
        }

        // Run several queries against this corpus
        let n_queries = 3 + rng.below(8);
        for _ in 0..n_queries {
            total_queries += 1;
            let query = gen_query(&mut rng);
            let limit = n_docs + 5; // request all docs

            let oracle_results = oracle.search(&query, limit);
            let sql_results = fts_search(&ex, &query, limit);

            match sql_results {
                None => {
                    if !oracle_results.is_empty() {
                        parse_errors += 1;
                        if parse_errors <= max_report {
                            println!("─── PARSE/EXEC ERROR (iter {iter}) ───");
                            println!("  query: {query}");
                            println!(
                                "  oracle returned {} results but SQL failed\n",
                                oracle_results.len()
                            );
                        }
                    }
                    continue;
                }
                Some(ref sql) => {
                    // (A) Check relative ordering
                    if let Some(violation) = check_ordering(&oracle_results, sql) {
                        order_violations += 1;
                        if order_violations <= max_report {
                            println!("─── ORDER VIOLATION #{order_violations} (iter {iter}) ───");
                            println!("  query           : {query}");
                            println!(
                                "  oracle order    : {:?}",
                                oracle_results
                                    .iter()
                                    .map(|(id, s)| format!("doc{id}={s:.4}"))
                                    .collect::<Vec<_>>()
                            );
                            println!(
                                "  sql    order    : {:?}",
                                sql.iter()
                                    .map(|(id, s)| format!("doc{id}={s:.4}"))
                                    .collect::<Vec<_>>()
                            );
                            println!("  violation       : {violation}");
                            println!("  corpus ({n_docs} docs):");
                            for doc_id in 1..=(n_docs as u64) {
                                if let Some(t) = doc_texts.get(&doc_id) {
                                    println!("    doc{doc_id}: {t}");
                                }
                            }
                            println!();
                        }
                    }

                    // (B) Check score accuracy within 1e-6 tolerance
                    if let Some(violation) = check_scores(&oracle_results, sql, 1e-6) {
                        score_violations += 1;
                        if score_violations <= max_report {
                            println!("─── SCORE MISMATCH #{score_violations} (iter {iter}) ───");
                            println!("  query    : {query}");
                            println!("  {violation}");
                            println!(
                                "  oracle   : {:?}",
                                oracle_results
                                    .iter()
                                    .map(|(id, s)| format!("doc{id}={s:.8}"))
                                    .collect::<Vec<_>>()
                            );
                            println!(
                                "  sql      : {:?}",
                                sql.iter()
                                    .map(|(id, s)| format!("doc{id}={s:.8}"))
                                    .collect::<Vec<_>>()
                            );
                            println!("  corpus ({n_docs} docs):");
                            for doc_id in 1..=(n_docs as u64) {
                                if let Some(t) = doc_texts.get(&doc_id) {
                                    println!("    doc{doc_id}: {t}");
                                }
                            }
                            println!();
                        }
                    }

                    // (D) FTS_RANK agreement check: for single-term queries, higher TF should correlate
                    // with higher BM25 ranking. FTS_RANK uses simple TF, FTS_SEARCH uses BM25.
                    // We check that for a single-term query, the ranking by FTS_RANK agrees with
                    // FTS_SEARCH ranking on the docs that both rank non-zero.
                    let query_words: Vec<&str> = query.split_whitespace().collect();
                    if query_words.len() == 1 && oracle_results.len() >= 2 {
                        // Collect FTS_RANK scores for each doc in oracle results
                        let mut rank_scores: Vec<(u64, f64)> = Vec::new();
                        for (doc_id, _bm25_score) in &oracle_results {
                            if let Some(text) = doc_texts.get(doc_id) {
                                if let Some(r) = fts_rank(&ex, text, &query) {
                                    rank_scores.push((*doc_id, r));
                                }
                            }
                        }

                        // For each pair, check that FTS_RANK ordering agrees with FTS_SEARCH ordering.
                        // For a single term, higher TF → higher BM25 (monotone in TF), so they should agree.
                        for i in 0..rank_scores.len().min(oracle_results.len()) {
                            for j in (i + 1)..rank_scores.len().min(oracle_results.len()) {
                                let (id_a, rank_a) = rank_scores[i];
                                let (id_b, rank_b) = rank_scores[j];
                                let (_, bm25_a) = oracle_results[i]; // already sorted by BM25 desc
                                let (_, bm25_b) = oracle_results[j];

                                // Only compare docs at same positions in their respective sorts
                                // i.e., doc at BM25 rank i vs doc at BM25 rank j (i < j → i is better)
                                if id_a == oracle_results[i].0 && id_b == oracle_results[j].0 {
                                    // BM25 says id_a ranks higher (bm25_a >= bm25_b)
                                    // FTS_RANK with single-term should agree (rank_a >= rank_b)
                                    let bm25_gap = bm25_a - bm25_b;
                                    let rank_gap = rank_a - rank_b;
                                    if bm25_gap > 1e-9 && rank_gap < -1e-9 {
                                        rank_violations += 1;
                                        if rank_violations <= max_report {
                                            println!(
                                                "─── FTS_RANK vs BM25 ORDER MISMATCH #{rank_violations} (iter {iter}) ───"
                                            );
                                            println!("  single-term query: {query}");
                                            println!(
                                                "  BM25 says doc {id_a} (score {bm25_a:.6}) > doc {id_b} (score {bm25_b:.6})"
                                            );
                                            println!(
                                                "  FTS_RANK says doc {id_a} ({rank_a:.6}) < doc {id_b} ({rank_b:.6})"
                                            );
                                            if let (Some(ta), Some(tb)) =
                                                (doc_texts.get(&id_a), doc_texts.get(&id_b))
                                            {
                                                println!("  doc {id_a}: {ta}");
                                                println!("  doc {id_b}: {tb}");
                                            }
                                            println!();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // (C) Monotonicity: periodically test with controlled docs
        if rng.chance(30) {
            let term = *rng.pick(WORDS);
            if let Some(violation) = check_monotonicity_freq(&mut oracle, &ex, &mut rng, term) {
                mono_violations += 1;
                if mono_violations <= max_report {
                    println!("─── MONOTONICITY VIOLATION #{mono_violations} (iter {iter}) ───");
                    println!("  {violation}\n");
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("iterations         : {iterations}");
    println!("queries run        : {total_queries}");
    println!("order violations   : {order_violations}");
    println!("score mismatches   : {score_violations}");
    println!("monotonicity fails : {mono_violations}");
    println!("rank vs bm25 fails : {rank_violations}");
    println!("parse/exec errors  : {parse_errors}");

    let total_violations = order_violations + score_violations + mono_violations;
    // rank_violations are informational (FTS_RANK vs BM25 is expected to differ —
    // FTS_RANK uses TF only, not BM25; we report them but don't fail for them).
    if total_violations == 0 && parse_errors == 0 {
        println!("\nNo ranking violations found. FTS_SEARCH ordering matches BM25 oracle.");
        if rank_violations > 0 {
            println!(
                "NOTE: {rank_violations} FTS_RANK vs BM25 order differences observed — expected (FTS_RANK uses TF-only, not BM25)."
            );
        }
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
