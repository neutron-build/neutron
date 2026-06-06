//! Datalog differential fuzzer: drives Nucleus's DATALOG_ASSERT/RULE/QUERY/RETRACT/CLEAR
//! through the SQL surface and compares query results against a naive fixpoint
//! evaluator written in plain Rust. The reference supports transitive-closure
//! style rules and retraction. Build:
//! `cargo run --release --features server --bin probe_datalog`.
#![cfg(feature = "server")]

use std::collections::HashSet;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

// ─── Deterministic PRNG ───────────────────────────────────────────────────────
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
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// ─── Tiny domain ──────────────────────────────────────────────────────────────
// Keep constants small so the reference fixpoint terminates fast and the
// derived fact sets stay manageable.
const NODES: &[&str] = &["a", "b", "c", "d", "e"];

// ─── Reference Datalog engine (naive fixpoint) ────────────────────────────────
//
// We support exactly the rule shapes we generate:
//
//   path(X,Y) :- edge(X,Y).
//   path(X,Z) :- edge(X,Y), path(Y,Z).
//
// The reference stores:
//   * base facts (two-argument, predicate "edge")
//   * derived facts (predicate "path"), recomputed from scratch on each query.
//
// Retract removes from the base facts set; clear wipes the whole set.

#[derive(Default)]
struct Ref {
    /// edge(x, y) base facts.
    edges: HashSet<(String, String)>,
}

impl Ref {
    fn assert_edge(&mut self, x: &str, y: &str) {
        self.edges.insert((x.to_string(), y.to_string()));
    }

    fn retract_edge(&mut self, x: &str, y: &str) {
        self.edges.remove(&(x.to_string(), y.to_string()));
    }

    fn clear_edge(&mut self) {
        self.edges.clear();
    }

    /// Naive fixpoint for transitive closure: path(X,Y) reachable via edges.
    fn query_path(&self) -> HashSet<(String, String)> {
        // Start with direct edges
        let mut path = self.edges.clone();
        // Iterate until fixpoint
        loop {
            let mut new_pairs: HashSet<(String, String)> = HashSet::new();
            for (x, y) in &path {
                for (a, b) in &self.edges {
                    // edge(A,B), path(B,Y) => path(A,Y)  -- but we want edge(X,Y),path(Y,Z)=>path(X,Z)
                    // i.e., for each edge(x,y) and existing path(y,z), derive path(x,z)
                    if a == x {
                        // edge(x, b) and path(x, y): we want path(x, z) where edge(x,y) and path(y,z)
                        // Let me redo: iterate edges as (ex,ey), paths as (px,pz):
                        // if ey == px => derive path(ex, pz)
                        // This is handled below
                        let _ = b;
                    }
                }
                // Actually iterate correctly: for edge(ex,ey) and path(ey, pz) => path(ex, pz)
                for (ex, ey) in &self.edges {
                    if ey == x {
                        // edge(ex, ey=x), path(x, y) => path(ex, y)
                        // wait, rule is: path(X,Z) :- edge(X,Y), path(Y,Z)
                        // So X=ex, Y=ey=x, Z=y (from path)
                        new_pairs.insert((ex.clone(), y.clone()));
                    }
                }
            }
            let prev_len = path.len();
            path.extend(new_pairs);
            if path.len() == prev_len {
                break;
            }
        }
        path
    }

    fn query_edge(&self) -> HashSet<(String, String)> {
        self.edges.clone()
    }
}

// ─── Operations ───────────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
enum Op {
    AssertEdge(String, String),
    RetractEdge(String, String),
    ClearEdge,
    QueryEdge,
    QueryPath,
    #[allow(dead_code)]
    AddPathRules, // adds the two path rules (idempotent in reference)
}

fn gen_op(rng: &mut Rng) -> Op {
    let x = (*rng.pick(NODES)).to_string();
    let y = (*rng.pick(NODES)).to_string();
    match rng.below(10) {
        0..=3 => Op::AssertEdge(x, y),
        4 => Op::RetractEdge(x, y),
        5 => Op::ClearEdge,
        6..=7 => Op::QueryEdge,
        8..=9 => Op::QueryPath,
        _ => unreachable!(),
    }
}

// ─── Nucleus SQL runner ───────────────────────────────────────────────────────
fn exec(ex: &Executor, sql: &str) -> Result<String, ()> {
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
        Err(_) => Err(()),
    }
}

fn is_panic(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .is_err()
}

// ─── JSON result parser ────────────────────────────────────────────────────────
// The engine returns: [["a", "b"], ["c", "d"], ...]
// We parse it into a set of tuples for comparison.
fn parse_json_tuples(s: &str) -> HashSet<Vec<String>> {
    let mut result = HashSet::new();
    // Strip outer []
    let s = s.trim();
    if s == "[]" {
        return result;
    }
    // Very simple parser: find inner arrays
    // Format: [["v1", "v2"], ["v3", "v4"]]
    // We'll do a naive scan
    let inner = if s.starts_with('[') && s.ends_with(']') {
        &s[1..s.len() - 1]
    } else {
        return result;
    };

    // Split on "], [" boundaries (simple approach for our tiny domain)
    // Walk character by character tracking bracket depth
    let mut depth = 0i32;
    let mut current = String::new();
    for ch in inner.chars() {
        match ch {
            '[' => {
                depth += 1;
                if depth == 1 {
                    current.clear();
                } else {
                    current.push(ch);
                }
            }
            ']' => {
                depth -= 1;
                if depth == 0 {
                    // Parse the inner tuple: "v1", "v2", ...
                    let tuple = parse_json_string_array(&current);
                    if !tuple.is_empty() {
                        result.insert(tuple);
                    }
                } else {
                    current.push(ch);
                }
            }
            _ => {
                if depth > 0 {
                    current.push(ch);
                }
            }
        }
    }
    result
}

fn parse_json_string_array(s: &str) -> Vec<String> {
    // Parse: "v1", "v2" or "v1" (with surrounding whitespace)
    let mut result = Vec::new();
    let s = s.trim();
    let mut i = 0;
    let chars: Vec<char> = s.chars().collect();
    while i < chars.len() {
        if chars[i] == '"' {
            i += 1;
            let mut val = String::new();
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                    val.push(chars[i]);
                } else {
                    val.push(chars[i]);
                }
                i += 1;
            }
            i += 1; // skip closing "
            result.push(val);
        } else {
            i += 1;
        }
    }
    result
}

// Convert a HashSet<(String,String)> to HashSet<Vec<String>> for comparison
fn pairs_to_set(pairs: &HashSet<(String, String)>) -> HashSet<Vec<String>> {
    pairs
        .iter()
        .map(|(a, b)| vec![a.clone(), b.clone()])
        .collect()
}

// ─── Main fuzzer logic ────────────────────────────────────────────────────────
fn main_impl() {
    let mut seed: u64 = 0xDEAD_BEEF_CAFE_BABE;
    let mut iterations = 3000usize;
    let mut ops_per = 30usize;
    let mut max_report = 15usize;
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

    println!("Nucleus Datalog differential fuzzer (facts+rules vs naive fixpoint reference)");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total_ops = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;

    'outer: for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        let mut reference = Ref::default();
        let mut rules_added = false;
        let mut log: Vec<String> = Vec::new();

        for _ in 0..ops_per {
            total_ops += 1;

            // Occasionally add the path rules to both engine and reference model.
            // We do this lazily and idempotently.  The reference just remembers a bool.
            if !rules_added && rng.below(6) == 0 {
                rules_added = true;
                // Rule 1: path(X,Y) :- edge(X,Y).
                let sql1 =
                    "SELECT DATALOG_RULE('path(X,Y) :- edge(X,Y)')".to_string();
                // Rule 2: path(X,Z) :- edge(X,Y), path(Y,Z).
                let sql2 =
                    "SELECT DATALOG_RULE('path(X,Z) :- edge(X,Y), path(Y,Z)')".to_string();
                let r1 = exec(&ex, &sql1);
                let r2 = exec(&ex, &sql2);
                log.push(sql1.clone());
                log.push(sql2.clone());
                // If the engine errors on rule registration, that itself is a divergence.
                if r1.is_err() || r2.is_err() {
                    divergences += 1;
                    if divergences <= max_report {
                        println!(
                            "─── DIVERGENCE #{divergences} (iter {iter}) ─── rule registration failed"
                        );
                        println!("  r1={r1:?}  r2={r2:?}");
                    }
                    continue 'outer;
                }
                continue;
            }

            let op = gen_op(&mut rng);

            match &op {
                Op::AssertEdge(x, y) => {
                    let fact = format!("edge({x},{y})");
                    let sql = format!("SELECT DATALOG_ASSERT('{fact}')");
                    log.push(sql.clone());
                    let got = exec(&ex, &sql);
                    reference.assert_edge(x, y);

                    // Check that assert succeeded (returns ASSERT edge/2)
                    match got {
                        Ok(s) if s.starts_with("ASSERT") => {}
                        Ok(s) => {
                            divergences += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) ─── unexpected assert result"
                                );
                                println!("  sql: {sql}");
                                println!("  got: Ok({s:?})");
                            }
                            continue 'outer;
                        }
                        Err(()) => {
                            // Check if it panicked
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!(
                                        "─── PANIC #{panics} (iter {iter}) ─── on DATALOG_ASSERT"
                                    );
                                    println!("  sql: {sql}");
                                }
                                continue 'outer;
                            }
                            divergences += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) ─── assert errored"
                                );
                                println!("  sql: {sql}");
                            }
                            continue 'outer;
                        }
                    }
                }

                Op::RetractEdge(x, y) => {
                    let fact = format!("edge({x},{y})");
                    let sql = format!("SELECT DATALOG_RETRACT('{fact}')");
                    log.push(sql.clone());
                    let got = exec(&ex, &sql);
                    reference.retract_edge(x, y);

                    // retract should succeed whether or not the fact existed
                    match got {
                        Ok(s) if s.starts_with("RETRACT") => {}
                        Ok(s) => {
                            divergences += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) ─── unexpected retract result"
                                );
                                println!("  sql: {sql}");
                                println!("  got: Ok({s:?})");
                            }
                            continue 'outer;
                        }
                        Err(()) => {
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!(
                                        "─── PANIC #{panics} (iter {iter}) ─── on DATALOG_RETRACT"
                                    );
                                    println!("  sql: {sql}");
                                }
                            } else {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) ─── retract errored"
                                    );
                                    println!("  sql: {sql}");
                                }
                            }
                            continue 'outer;
                        }
                    }
                }

                Op::ClearEdge => {
                    let sql = "SELECT DATALOG_CLEAR('edge')".to_string();
                    log.push(sql.clone());
                    let got = exec(&ex, &sql);
                    reference.clear_edge();

                    match got {
                        Ok(s) if s.starts_with("CLEAR") => {}
                        Ok(s) => {
                            divergences += 1;
                            if divergences <= max_report {
                                println!(
                                    "─── DIVERGENCE #{divergences} (iter {iter}) ─── unexpected clear result"
                                );
                                println!("  sql: {sql}");
                                println!("  got: Ok({s:?})");
                            }
                            continue 'outer;
                        }
                        Err(()) => {
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!(
                                        "─── PANIC #{panics} (iter {iter}) ─── on DATALOG_CLEAR"
                                    );
                                }
                            } else {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) ─── clear errored"
                                    );
                                    println!("  sql: {sql}");
                                }
                            }
                            continue 'outer;
                        }
                    }
                }

                Op::QueryEdge => {
                    // Query all edge/2 facts
                    let sql = "SELECT DATALOG_QUERY('edge(X,Y)')".to_string();
                    log.push(sql.clone());
                    let got = exec(&ex, &sql);
                    let expected_set = pairs_to_set(&reference.query_edge());

                    match got {
                        Ok(json) => {
                            let got_set = parse_json_tuples(&json);
                            if got_set != expected_set {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) ─── edge query mismatch"
                                    );
                                    println!("  sql: {sql}");
                                    println!("  json: {json}");
                                    // Symmetric diff
                                    let in_ref_not_got: HashSet<_> =
                                        expected_set.difference(&got_set).collect();
                                    let in_got_not_ref: HashSet<_> =
                                        got_set.difference(&expected_set).collect();
                                    if !in_ref_not_got.is_empty() {
                                        let mut v: Vec<_> = in_ref_not_got.iter().collect();
                                        v.sort();
                                        println!("  missing from nucleus: {v:?}");
                                    }
                                    if !in_got_not_ref.is_empty() {
                                        let mut v: Vec<_> = in_got_not_ref.iter().collect();
                                        v.sort();
                                        println!("  extra in nucleus:     {v:?}");
                                    }
                                    println!("  ── replay ({} ops) ──", log.len());
                                    for entry in &log {
                                        println!("    {};", entry);
                                    }
                                    println!();
                                }
                                continue 'outer;
                            }
                        }
                        Err(()) => {
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!(
                                        "─── PANIC #{panics} (iter {iter}) ─── on DATALOG_QUERY edge"
                                    );
                                }
                            } else {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) ─── edge query errored"
                                    );
                                    println!("  sql: {sql}");
                                }
                            }
                            continue 'outer;
                        }
                    }
                }

                Op::QueryPath => {
                    // Only meaningful when rules have been added; without rules,
                    // path should just be empty (no facts ever asserted for it).
                    let sql = "SELECT DATALOG_QUERY('path(X,Y)')".to_string();
                    log.push(sql.clone());
                    let got = exec(&ex, &sql);

                    let expected_set = if rules_added {
                        pairs_to_set(&reference.query_path())
                    } else {
                        // No rules => path predicate has no facts (neither base nor derived)
                        HashSet::new()
                    };

                    match got {
                        Ok(json) => {
                            let got_set = parse_json_tuples(&json);
                            if got_set != expected_set {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) ─── path query mismatch"
                                    );
                                    println!("  rules_added: {rules_added}");
                                    println!("  sql: {sql}");
                                    println!("  json: {json}");
                                    let in_ref_not_got: HashSet<_> =
                                        expected_set.difference(&got_set).collect();
                                    let in_got_not_ref: HashSet<_> =
                                        got_set.difference(&expected_set).collect();
                                    if !in_ref_not_got.is_empty() {
                                        let mut v: Vec<_> = in_ref_not_got.iter().collect();
                                        v.sort();
                                        println!("  missing from nucleus: {v:?}");
                                    }
                                    if !in_got_not_ref.is_empty() {
                                        let mut v: Vec<_> = in_got_not_ref.iter().collect();
                                        v.sort();
                                        println!("  extra in nucleus:     {v:?}");
                                    }
                                    println!("  ── replay ({} ops) ──", log.len());
                                    for entry in &log {
                                        println!("    {};", entry);
                                    }
                                    println!();
                                }
                                continue 'outer;
                            }
                        }
                        Err(()) => {
                            if is_panic(&ex, &sql) {
                                panics += 1;
                                if panics <= max_report {
                                    println!(
                                        "─── PANIC #{panics} (iter {iter}) ─── on DATALOG_QUERY path"
                                    );
                                }
                            } else {
                                divergences += 1;
                                if divergences <= max_report {
                                    println!(
                                        "─── DIVERGENCE #{divergences} (iter {iter}) ─── path query errored"
                                    );
                                    println!("  sql: {sql}");
                                }
                            }
                            continue 'outer;
                        }
                    }
                }

                Op::AddPathRules => {
                    // This variant is not actually generated by gen_op;
                    // rules are added inline above. Keep the arm for exhaustiveness.
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("ops run            : {total_ops}");
    println!("divergences        : {divergences}");
    println!("panics             : {panics}");
    if divergences == 0 && panics == 0 {
        println!("\nNo divergences, no panics. Datalog engine matches reference.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
