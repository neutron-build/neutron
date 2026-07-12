//! Rich Datalog differential fuzzer.
//!
//! Tests Nucleus DATALOG_ASSERT / DATALOG_RULE / DATALOG_QUERY / DATALOG_RETRACT /
//! DATALOG_CLEAR against a self-contained naive/semi-naive fixpoint reference
//! evaluator. Covers:
//!
//! * Transitive closure (edge/path — two-rule mutually recursive chain)
//! * Same-generation (sg — three-way join using two base predicates)
//! * Multi-way join across three relations (works_in / loc_in / region_of)
//! * Stratified negation: `not_member(X) :- person(X), \+ member(X)`
//!
//! Build:
//!   cargo build --release --features server --bin probe_datalog_rich
//! Run:
//!   cargo run  --release --features server --bin probe_datalog_rich [--seed N] [--iterations N]
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)]

use std::collections::{HashMap, HashSet};
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
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// ─── Tiny domain ──────────────────────────────────────────────────────────────
// Node names used for graph / generation relations.
const NODES: &[&str] = &["a", "b", "c", "d", "e"];
// Locations / departments for multi-way join tests.
const LOCS: &[&str] = &["london", "paris", "berlin"];
const REGIONS: &[&str] = &["emea", "amer"];

// ─── Reference Datalog engine ─────────────────────────────────────────────────
//
// Maintains four independent base relations:
//   edge(X, Y)        — directed edge
//   parent(X, Y)      — parent-child relation for same-generation
//   works_in(P, L)    — person works in location
//   loc_in(L, R)      — location is in region
//   person(X)         — universe predicate for negation test
//   member(X)         — a subset of persons
//
// Derived predicates are recomputed from scratch every time they are queried
// (naive bottom-up fixpoint). This is correct but exponential; the tiny domain
// keeps it cheap.

type Tuples = HashSet<Vec<String>>;

#[derive(Default, Clone)]
struct RefDb {
    edge: HashSet<(String, String)>,
    parent: HashSet<(String, String)>,
    works_in: HashSet<(String, String)>,
    loc_in: HashSet<(String, String)>,
    person: HashSet<String>,
    member: HashSet<String>,
}

impl RefDb {
    // ── Transitive closure ────────────────────────────────────────────────────
    // path(X,Y) :- edge(X,Y).
    // path(X,Z) :- edge(X,Y), path(Y,Z).
    fn path(&self) -> HashSet<(String, String)> {
        let mut p: HashSet<(String, String)> = self.edge.clone();
        loop {
            let mut new_pairs = HashSet::new();
            for (x, y) in &p {
                for (a, b) in &self.edge {
                    // edge(a, b), path(b, y) -> path(a, y)
                    if b == x {
                        new_pairs.insert((a.clone(), y.clone()));
                    }
                }
            }
            let prev = p.len();
            p.extend(new_pairs);
            if p.len() == prev {
                break;
            }
        }
        p
    }

    // ── Same-generation ───────────────────────────────────────────────────────
    // sg(X, Y) :- node(X), node(Y), X = Y.            (base: every node is sg with itself)
    // sg(X, Y) :- parent(Px, X), parent(Py, Y), sg(Px, Py).
    //
    // We treat node universe as NODES constant.
    fn sg(&self) -> HashSet<(String, String)> {
        // Seed: every node is same-generation as itself
        let all_nodes: HashSet<String> = NODES.iter().map(|s| s.to_string()).collect();
        let mut result: HashSet<(String, String)> =
            all_nodes.iter().map(|n| (n.clone(), n.clone())).collect();
        loop {
            let mut new_pairs = HashSet::new();
            // sg(X,Y) :- parent(Px,X), parent(Py,Y), sg(Px,Py)
            for (px, py) in &result {
                for (ppx, cx) in &self.parent {
                    if ppx == px {
                        for (ppy, cy) in &self.parent {
                            if ppy == py {
                                new_pairs.insert((cx.clone(), cy.clone()));
                            }
                        }
                    }
                }
            }
            let prev = result.len();
            result.extend(new_pairs);
            if result.len() == prev {
                break;
            }
        }
        result
    }

    // ── Multi-way join: region_of ──────────────────────────────────────────────
    // region_of(P, R) :- works_in(P, L), loc_in(L, R).
    fn region_of(&self) -> HashSet<(String, String)> {
        let mut result = HashSet::new();
        for (p, l) in &self.works_in {
            for (ll, r) in &self.loc_in {
                if l == ll {
                    result.insert((p.clone(), r.clone()));
                }
            }
        }
        result
    }

    // ── Stratified negation: not_member ───────────────────────────────────────
    // not_member(X) :- person(X), \+ member(X).
    fn not_member(&self) -> HashSet<String> {
        self.person
            .iter()
            .filter(|x| !self.member.contains(*x))
            .cloned()
            .collect()
    }

    // ─── Helpers to turn relation sets into Tuples for comparison ─────────────
    fn path_tuples(&self) -> Tuples {
        self.path().into_iter().map(|(a, b)| vec![a, b]).collect()
    }
    fn sg_tuples(&self) -> Tuples {
        self.sg().into_iter().map(|(a, b)| vec![a, b]).collect()
    }
    fn region_of_tuples(&self) -> Tuples {
        self.region_of()
            .into_iter()
            .map(|(a, b)| vec![a, b])
            .collect()
    }
    fn not_member_tuples(&self) -> Tuples {
        self.not_member().into_iter().map(|x| vec![x]).collect()
    }
    fn edge_tuples(&self) -> Tuples {
        self.edge
            .iter()
            .map(|(a, b)| vec![a.clone(), b.clone()])
            .collect()
    }
    fn parent_tuples(&self) -> Tuples {
        self.parent
            .iter()
            .map(|(a, b)| vec![a.clone(), b.clone()])
            .collect()
    }
    fn works_in_tuples(&self) -> Tuples {
        self.works_in
            .iter()
            .map(|(a, b)| vec![a.clone(), b.clone()])
            .collect()
    }
    fn loc_in_tuples(&self) -> Tuples {
        self.loc_in
            .iter()
            .map(|(a, b)| vec![a.clone(), b.clone()])
            .collect()
    }
    fn person_tuples(&self) -> Tuples {
        self.person.iter().map(|x| vec![x.clone()]).collect()
    }
    fn member_tuples(&self) -> Tuples {
        self.member.iter().map(|x| vec![x.clone()]).collect()
    }
}

// ─── Nucleus SQL helpers ───────────────────────────────────────────────────────

/// Run a single SQL statement and return the first cell of the first row of the
/// last SELECT result, or Err on failure.
fn exec_first(ex: &Executor, sql: &str) -> Result<String, String> {
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
            _ => Ok(String::new()), // non-select (DDL, DML)
        },
        Ok(Err(e)) => Err(format!("ExecError: {e:?}")),
        Err(_) => Err("PANIC".to_string()),
    }
}

/// Execute a DATALOG_QUERY and return the result as a sorted Tuples set.
fn datalog_query(ex: &Executor, pattern: &str) -> Result<Tuples, String> {
    let sql = format!("SELECT DATALOG_QUERY('{pattern}')");
    let raw = exec_first(ex, &sql)?;
    Ok(parse_json_tuples(&raw))
}

/// Execute a DATALOG_ASSERT and verify we get "ASSERT …" back.
fn datalog_assert(ex: &Executor, fact: &str) -> Result<(), String> {
    let sql = format!("SELECT DATALOG_ASSERT('{fact}')");
    match exec_first(ex, &sql) {
        Ok(s) if s.starts_with("ASSERT") => Ok(()),
        Ok(s) => Err(format!("unexpected assert result: {s:?}")),
        Err(e) => Err(e),
    }
}

/// Execute a DATALOG_RETRACT.
fn datalog_retract(ex: &Executor, fact: &str) -> Result<(), String> {
    let sql = format!("SELECT DATALOG_RETRACT('{fact}')");
    match exec_first(ex, &sql) {
        Ok(s) if s.starts_with("RETRACT") => Ok(()),
        Ok(s) => Err(format!("unexpected retract result: {s:?}")),
        Err(e) => Err(e),
    }
}

/// Execute a DATALOG_CLEAR.
fn datalog_clear(ex: &Executor, pred: &str) -> Result<(), String> {
    let sql = format!("SELECT DATALOG_CLEAR('{pred}')");
    match exec_first(ex, &sql) {
        Ok(s) if s.starts_with("CLEAR") => Ok(()),
        Ok(s) => Err(format!("unexpected clear result: {s:?}")),
        Err(e) => Err(e),
    }
}

/// Execute a DATALOG_RULE and verify we get "RULE …" back.
fn datalog_rule(ex: &Executor, rule_str: &str) -> Result<(), String> {
    let sql = format!("SELECT DATALOG_RULE('{rule_str}')");
    match exec_first(ex, &sql) {
        Ok(s) if s.starts_with("RULE") => Ok(()),
        Ok(s) => Err(format!("unexpected rule result: {s:?}")),
        Err(e) => Err(e),
    }
}

// ─── JSON tuple parser ─────────────────────────────────────────────────────────
//
// The engine returns results as JSON: [["v1","v2"], ["v3","v4"], ...]
// We convert to a HashSet<Vec<String>> for set comparison.
fn parse_json_tuples(s: &str) -> Tuples {
    let mut result = Tuples::new();
    let s = s.trim();
    if s == "[]" || s.is_empty() {
        return result;
    }
    let inner = if s.starts_with('[') && s.ends_with(']') {
        &s[1..s.len() - 1]
    } else {
        return result;
    };
    let mut depth = 0i32;
    let mut current = String::new();
    for ch in inner.chars() {
        match ch {
            '[' => {
                depth += 1;
                if depth > 1 {
                    current.push(ch);
                }
            }
            ']' => {
                depth -= 1;
                if depth == 0 {
                    let tuple = parse_string_array(&current);
                    if !tuple.is_empty() {
                        result.insert(tuple);
                    }
                    current.clear();
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

fn parse_string_array(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let chars: Vec<char> = s.trim().chars().collect();
    let mut i = 0;
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
            i += 1;
            result.push(val);
        } else {
            i += 1;
        }
    }
    result
}

// ─── Rule suites ──────────────────────────────────────────────────────────────

/// Install the transitive-closure rules into a fresh Nucleus executor.
fn install_path_rules(ex: &Executor) -> Result<(), String> {
    datalog_rule(ex, "path(X,Y) :- edge(X,Y)")?;
    datalog_rule(ex, "path(X,Z) :- edge(X,Y), path(Y,Z)")
}

/// Install the same-generation rules.
fn install_sg_rules(ex: &Executor) -> Result<(), String> {
    // Seed rule: every node is same-generation as itself.
    // sg(X,X) :- sg_node(X).
    datalog_rule(ex, "sg(X,X) :- sg_node(X)")?;
    // Recursive rule: if Px and Py are same-gen and parent(Px,X) and parent(Py,Y) then X and Y are same-gen.
    datalog_rule(ex, "sg(X,Y) :- parent(Px,X), parent(Py,Y), sg(Px,Py)")
}

/// Install the multi-way join rule: region_of(P,R) :- works_in(P,L), loc_in(L,R).
fn install_region_rules(ex: &Executor) -> Result<(), String> {
    datalog_rule(ex, "region_of(P,R) :- works_in(P,L), loc_in(L,R)")
}

/// Install the stratified-negation rule: not_member(X) :- person(X), \\+ member(X).
fn install_neg_rules(ex: &Executor) -> Result<(), String> {
    datalog_rule(ex, "not_member(X) :- person(X), \\+ member(X)")
}

// ─── Divergence report helper ─────────────────────────────────────────────────

fn report_divergence(
    label: &str,
    iter: usize,
    query: &str,
    got: &Tuples,
    expected: &Tuples,
    log: &[String],
) {
    println!("─── DIVERGENCE (iter {iter}) ─── {label}");
    println!("  query:   {query}");
    let missing: Vec<_> = {
        let mut v: Vec<_> = expected.difference(got).collect();
        v.sort();
        v
    };
    let extra: Vec<_> = {
        let mut v: Vec<_> = got.difference(expected).collect();
        v.sort();
        v
    };
    if !missing.is_empty() {
        println!("  missing from nucleus: {missing:?}");
    }
    if !extra.is_empty() {
        println!("  extra in nucleus:     {extra:?}");
    }
    println!("  ── replay ({} ops) ──", log.len());
    for entry in log {
        println!("    {entry};");
    }
    println!();
}

// ─── Operation enumeration ────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum Op {
    // edge relation
    AssertEdge(String, String),
    RetractEdge(String, String),
    ClearEdge,
    // parent relation
    AssertParent(String, String),
    RetractParent(String, String),
    // works_in / loc_in
    AssertWorksIn(String, String),
    RetractWorksIn(String, String),
    AssertLocIn(String, String),
    RetractLocIn(String, String),
    // person / member (for negation)
    AssertPerson(String),
    AssertMember(String),
    RetractMember(String),
    // queries
    QueryEdge,
    QueryPath,
    QueryParent,
    QuerySg,
    QueryWorksIn,
    QueryLocIn,
    QueryRegionOf,
    QueryPerson,
    QueryMember,
    QueryNotMember,
}

fn gen_op(rng: &mut Rng) -> Op {
    let x = (*rng.pick(NODES)).to_string();
    let y = (*rng.pick(NODES)).to_string();
    let p = (*rng.pick(NODES)).to_string();
    let l = (*rng.pick(LOCS)).to_string();
    let r = (*rng.pick(REGIONS)).to_string();
    match rng.below(30) {
        0..=3 => Op::AssertEdge(x, y),
        4 => Op::RetractEdge(x, y),
        5 => Op::ClearEdge,
        6..=8 => Op::AssertParent(x, y),
        9 => Op::RetractParent(x, y),
        10..=11 => Op::AssertWorksIn(p.clone(), l.clone()),
        12 => Op::RetractWorksIn(p, l.clone()),
        13 => Op::AssertLocIn(l, r),
        14 => Op::RetractLocIn(
            (*rng.pick(LOCS)).to_string(),
            (*rng.pick(REGIONS)).to_string(),
        ),
        15 => Op::AssertPerson(x.clone()),
        16..=17 => Op::AssertMember(x.clone()),
        18 => Op::RetractMember(x),
        19..=20 => Op::QueryEdge,
        21..=22 => Op::QueryPath,
        23 => Op::QueryParent,
        24 => Op::QuerySg,
        25 => Op::QueryWorksIn,
        26 => Op::QueryLocIn,
        27 => Op::QueryRegionOf,
        28 => Op::QueryPerson,
        29 => Op::QueryNotMember,
        _ => Op::QueryMember,
    }
}

// ─── Main fuzzer ──────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xDEAD_BEEF_CAFE_F00D;
    let mut iterations: usize = 2000;
    let mut ops_per: usize = 40;
    let mut max_report: usize = 20;

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

    println!("probe_datalog_rich — Nucleus Datalog rich differential fuzzer");
    println!("seed={seed} iterations={iterations} ops/iter={ops_per}\n");

    let mut total_ops = 0usize;
    let mut divergences = 0usize;
    let mut rule_errors = 0usize;

    // We run multiple seeds for coverage
    for seed_round in 0..4u64 {
        let round_seed = seed.wrapping_add(seed_round.wrapping_mul(0x9E3779B97F4A7C15));

        'outer: for iter in 0..iterations {
            let mut rng = Rng(round_seed
                .wrapping_add(iter as u64)
                .wrapping_mul(0x100000001B3));

            // Fresh engine per iteration
            let catalog = Arc::new(Catalog::new());
            let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
            let ex = Arc::new(Executor::new(catalog, storage));

            // Install all rule suites once at the start of each iteration.
            // Rule registration may fail if the engine has a parser issue — count
            // that as a rule_error (separate tally) and skip the iteration rather
            // than marking it as a query divergence.
            let rule_ok = install_path_rules(&ex).is_ok()
                && install_sg_rules(&ex).is_ok()
                && install_region_rules(&ex).is_ok()
                && install_neg_rules(&ex).is_ok();

            if !rule_ok {
                rule_errors += 1;
                if rule_errors <= max_report {
                    println!("RULE INSTALL FAILURE (iter {iter})");
                }
                continue;
            }

            let mut db = RefDb::default();
            let mut log: Vec<String> = Vec::new();

            // Pre-seed the sg_node facts (the sg base rule uses sg_node(X) to seed the
            // identity pairs; we assert a constant universe so the reference and
            // engine start with the same universe).
            // Actually: the sg rule we installed is "sg(X,Y) :- sg_node(X), sg_node(Y)"
            // meaning *any* pair of nodes is sg at first (before parent rules fire).
            // The reference also seeds all pairs when it has nodes in NODES.
            // We therefore assert sg_node for every node in the universe.
            for n in NODES {
                let fact = format!("sg_node({n})");
                if let Err(e) = datalog_assert(&ex, &fact) {
                    rule_errors += 1;
                    if rule_errors <= max_report {
                        println!("sg_node assert failed: {e}");
                    }
                    continue 'outer;
                }
                log.push(format!("SELECT DATALOG_ASSERT('{fact}')"));
            }

            for _ in 0..ops_per {
                total_ops += 1;
                let op = gen_op(&mut rng);

                macro_rules! do_assert {
                    ($fact:expr) => {{
                        let fact_str = $fact;
                        let sql = format!("SELECT DATALOG_ASSERT('{fact_str}')");
                        log.push(sql.clone());
                        if let Err(e) = datalog_assert(&ex, &fact_str) {
                            if divergences < max_report {
                                println!("─── ASSERT ERROR (iter {iter}) ─── {e}");
                                println!("  fact: {fact_str}");
                            }
                            divergences += 1;
                            continue 'outer;
                        }
                    }};
                }

                macro_rules! do_retract {
                    ($fact:expr) => {{
                        let fact_str = $fact;
                        let sql = format!("SELECT DATALOG_RETRACT('{fact_str}')");
                        log.push(sql.clone());
                        if let Err(e) = datalog_retract(&ex, &fact_str) {
                            if divergences < max_report {
                                println!("─── RETRACT ERROR (iter {iter}) ─── {e}");
                                println!("  fact: {fact_str}");
                            }
                            divergences += 1;
                            continue 'outer;
                        }
                    }};
                }

                macro_rules! check_query {
                    ($pattern:expr, $expected:expr, $label:expr) => {{
                        let pattern = $pattern;
                        let expected: Tuples = $expected;
                        match datalog_query(&ex, &pattern) {
                            Err(e) => {
                                if divergences < max_report {
                                    println!("─── QUERY ERROR (iter {iter}) ─── {e}");
                                    println!("  pattern: {pattern}");
                                }
                                divergences += 1;
                                continue 'outer;
                            }
                            Ok(got) => {
                                if got != expected {
                                    if divergences < max_report {
                                        report_divergence(
                                            $label, iter, &pattern, &got, &expected, &log,
                                        );
                                    }
                                    divergences += 1;
                                    continue 'outer;
                                }
                            }
                        }
                    }};
                }

                match &op {
                    Op::AssertEdge(x, y) => {
                        db.edge.insert((x.clone(), y.clone()));
                        do_assert!(format!("edge({x},{y})"));
                    }
                    Op::RetractEdge(x, y) => {
                        db.edge.remove(&(x.clone(), y.clone()));
                        do_retract!(format!("edge({x},{y})"));
                    }
                    Op::ClearEdge => {
                        db.edge.clear();
                        log.push("SELECT DATALOG_CLEAR('edge')".to_string());
                        if let Err(e) = datalog_clear(&ex, "edge") {
                            if divergences < max_report {
                                println!("─── CLEAR ERROR (iter {iter}) ─── {e}");
                            }
                            divergences += 1;
                            continue 'outer;
                        }
                    }
                    Op::AssertParent(x, y) => {
                        db.parent.insert((x.clone(), y.clone()));
                        do_assert!(format!("parent({x},{y})"));
                    }
                    Op::RetractParent(x, y) => {
                        db.parent.remove(&(x.clone(), y.clone()));
                        do_retract!(format!("parent({x},{y})"));
                    }
                    Op::AssertWorksIn(p, l) => {
                        db.works_in.insert((p.clone(), l.clone()));
                        do_assert!(format!("works_in({p},{l})"));
                    }
                    Op::RetractWorksIn(p, l) => {
                        db.works_in.remove(&(p.clone(), l.clone()));
                        do_retract!(format!("works_in({p},{l})"));
                    }
                    Op::AssertLocIn(l, r) => {
                        db.loc_in.insert((l.clone(), r.clone()));
                        do_assert!(format!("loc_in({l},{r})"));
                    }
                    Op::RetractLocIn(l, r) => {
                        db.loc_in.remove(&(l.clone(), r.clone()));
                        do_retract!(format!("loc_in({l},{r})"));
                    }
                    Op::AssertPerson(x) => {
                        db.person.insert(x.clone());
                        do_assert!(format!("person({x})"));
                    }
                    Op::AssertMember(x) => {
                        db.member.insert(x.clone());
                        do_assert!(format!("member({x})"));
                    }
                    Op::RetractMember(x) => {
                        db.member.remove(x);
                        do_retract!(format!("member({x})"));
                    }

                    // ── Queries ────────────────────────────────────────────────
                    Op::QueryEdge => {
                        check_query!("edge(X,Y)", db.edge_tuples(), "QueryEdge");
                    }
                    Op::QueryPath => {
                        check_query!("path(X,Y)", db.path_tuples(), "QueryPath");
                    }
                    Op::QueryParent => {
                        check_query!("parent(X,Y)", db.parent_tuples(), "QueryParent");
                    }
                    Op::QuerySg => {
                        // The sg reference includes all pairs from same-generation
                        // logic. However, note: the engine's sg rule also seeds
                        // "sg(X,Y) :- sg_node(X), sg_node(Y)" which means initially
                        // all node pairs are sg, and the parent rule adds more.
                        // In the reference, sg() already seeds all NODES×NODES pairs
                        // (plus parent-derived) — so we compare against that.
                        check_query!("sg(X,Y)", db.sg_tuples(), "QuerySg");
                    }
                    Op::QueryWorksIn => {
                        check_query!("works_in(P,L)", db.works_in_tuples(), "QueryWorksIn");
                    }
                    Op::QueryLocIn => {
                        check_query!("loc_in(L,R)", db.loc_in_tuples(), "QueryLocIn");
                    }
                    Op::QueryRegionOf => {
                        check_query!("region_of(P,R)", db.region_of_tuples(), "QueryRegionOf");
                    }
                    Op::QueryPerson => {
                        check_query!("person(X)", db.person_tuples(), "QueryPerson");
                    }
                    Op::QueryMember => {
                        check_query!("member(X)", db.member_tuples(), "QueryMember");
                    }
                    Op::QueryNotMember => {
                        check_query!("not_member(X)", db.not_member_tuples(), "QueryNotMember");
                    }
                }
            }
        }
    }

    let total_iters = iterations * 4;
    println!("\n════ SUMMARY ════");
    println!("seeds run          : 4 (base={seed:#x})");
    println!("iterations/seed    : {iterations}  (total {total_iters})");
    println!("ops/iter           : {ops_per}");
    println!("total ops executed : {total_ops}");
    println!("rule install errors: {rule_errors}");
    println!("divergences        : {divergences}");

    if rule_errors == 0 && divergences == 0 {
        println!("\nAll clear. Datalog rich harness: no divergences.");
    } else {
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
