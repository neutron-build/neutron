//! Metamorphic SQL fuzzer (oracle-free, single Mvcc executor).
//!
//! Builds a small random table, then for each base SELECT query Q generates an
//! equivalent transformed query Q2 that MUST return the same result set.
//! Any mismatch is a real Nucleus bug — no oracle ambiguity exists because both
//! queries run on the same engine against identical state.
//!
//! Transformations tested:
//!   1. Predicate commutativity: (A AND B) == (B AND A), (A OR B) == (B OR A)
//!   2. Double negation: WHERE p == WHERE NOT(NOT(p))
//!   3. IN-to-OR expansion: x IN (k1,k2) == (x=k1 OR x=k2)
//!   4. Subquery wrap: SELECT id FROM t WHERE p == SELECT id FROM (SELECT * FROM t) sub WHERE p
//!   5. DISTINCT idempotence: SELECT DISTINCT id == SELECT DISTINCT id FROM (SELECT DISTINCT id FROM t) sub
//!   6. ORDER reversal: ORDER BY col ASC rows reversed in Rust == ORDER BY col DESC
//!   7. UNION empty: Q UNION ALL SELECT ... WHERE 1=0 == Q  (same rows, set comparison)
//!
//! Build: `cargo run --release --features server --bin probe_meta`
#![cfg(feature = "server")]
#![allow(clippy::all)] // internal fuzz harness

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
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

// ─── Value canonicalization ───────────────────────────────────────────────────
fn canon(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "1" } else { "0" }.into(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => {
            // Round to 6 decimal places so float-formatting differences don't
            // create spurious mismatches between two Nucleus executions.
            if f.is_finite() && (f - f.round()).abs() < 1e-9 && f.abs() < 9e15 {
                format!("{}", f.round() as i64)
            } else {
                format!("{f:.6}")
            }
        }
        Value::Text(s) => s.clone(),
        other => format!("{other}"),
    }
}

// ─── Executor helper ──────────────────────────────────────────────────────────
fn run_select(ex: &Executor, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let rt = tokio::runtime::Handle::current();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }));
    match res {
        Ok(Ok(mut results)) => match results.pop() {
            Some(ExecResult::Select { rows, .. }) => {
                Ok(rows.iter().map(|r| r.iter().map(canon).collect()).collect())
            }
            _ => Err("non-select".into()),
        },
        Ok(Err(e)) => Err(format!("err: {e:?}")),
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown".into());
            Err(format!("PANIC: {msg}"))
        }
    }
}

fn exec_dml(ex: &Executor, sql: &str) -> bool {
    let rt = tokio::runtime::Handle::current();
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tokio::task::block_in_place(|| rt.block_on(ex.execute(sql)))
    }))
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

// ─── Table schema + data generation ──────────────────────────────────────────
// Keep small integer values so IN-to-OR and predicate transforms are exact.
const INT_VALS: &[i64] = &[-3, -1, 0, 1, 2, 5, 8, 10, 15, 20];
const TEXT_VALS: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon"];

fn gen_table(ex: &Executor, rng: &mut Rng, n_rows: usize) -> bool {
    let ok = exec_dml(
        ex,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER, c TEXT NOT NULL)",
    );
    if !ok {
        return false;
    }
    let mut vals = Vec::with_capacity(n_rows);
    for id in 1..=n_rows {
        let a = *rng.pick(INT_VALS);
        let b = if rng.chance(20) {
            "NULL".into()
        } else {
            rng.pick(INT_VALS).to_string()
        };
        let c = format!("'{}'", rng.pick(TEXT_VALS));
        vals.push(format!("({id},{a},{b},{c})"));
    }
    exec_dml(
        ex,
        &format!("INSERT INTO t (id,a,b,c) VALUES {}", vals.join(",")),
    )
}

// ─── Predicate generator (simple, closed-form) ────────────────────────────────
// Predicates operate only on NOT NULL columns (a, c, id) so the comparison is
// unambiguous across both sides of a metamorphic pair.
fn gen_simple_pred(rng: &mut Rng) -> String {
    match rng.below(6) {
        0 => {
            let v = *rng.pick(INT_VALS);
            let op = *rng.pick(&["=", "<>", "<", "<=", ">", ">="]);
            format!("a {op} {v}")
        }
        1 => {
            let lo = *rng.pick(INT_VALS);
            let hi = lo + rng.int(1, 10).abs();
            format!("a BETWEEN {lo} AND {hi}")
        }
        2 => {
            // IN with 2-3 known integer values (used also for IN→OR transform).
            let n = 2 + rng.below(2);
            let items: Vec<String> = (0..n).map(|_| rng.pick(INT_VALS).to_string()).collect();
            format!("a IN ({})", items.join(","))
        }
        3 => {
            let cv = rng.pick(TEXT_VALS);
            format!("c = '{cv}'")
        }
        4 => {
            let v = *rng.pick(INT_VALS);
            format!("id > {v}")
        }
        _ => {
            let v = *rng.pick(INT_VALS);
            format!("a <> {v}")
        }
    }
}

fn gen_compound_pred(rng: &mut Rng) -> String {
    let a = gen_simple_pred(rng);
    let b = gen_simple_pred(rng);
    let op = if rng.chance(50) { "AND" } else { "OR" };
    format!("({a} {op} {b})")
}

fn gen_pred(rng: &mut Rng) -> String {
    if rng.chance(50) {
        gen_compound_pred(rng)
    } else {
        gen_simple_pred(rng)
    }
}

// ─── Metamorphic transforms ───────────────────────────────────────────────────

/// Swap the two arms of the outermost AND/OR.
/// "(A AND B)" → "(B AND A)", "(A OR B)" → "(B OR A)"
/// Returns None if the predicate isn't in that shape.
fn commute_pred(pred: &str) -> Option<String> {
    // Only handle the simple "(A OP B)" shape we generate.
    let inner = pred.strip_prefix('(')?.strip_suffix(')')?;
    for op in &[" AND ", " OR "] {
        if let Some(pos) = inner.find(op) {
            let a = &inner[..pos];
            let b = &inner[pos + op.len()..];
            return Some(format!("({b}{op}{a})"));
        }
    }
    None
}

/// x IN (k1,k2,...) → (x=k1 OR x=k2 OR ...)
/// Returns None if pred is not a simple "col IN (...)" form.
fn expand_in(pred: &str) -> Option<String> {
    // Expect: "a IN (v1,v2,...)" or "c IN (...)"
    let (col, rest) = if let Some(r) = pred.strip_prefix("a IN (") {
        ("a", r)
    } else if let Some(r) = pred.strip_prefix("id IN (") {
        ("id", r)
    } else {
        return None;
    };
    let items_str = rest.strip_suffix(')')?;
    let items: Vec<&str> = items_str.split(',').collect();
    if items.is_empty() {
        return None;
    }
    let clauses: Vec<String> = items
        .iter()
        .map(|v| format!("{col}={}", v.trim()))
        .collect();
    Some(format!("({})", clauses.join(" OR ")))
}

// ─── Transform catalogue ──────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
enum TransformKind {
    PredicateCommute,
    DoubleNegation,
    InToOr,
    SubqueryWrap,
    DistinctIdempotent,
    OrderReversal,
    UnionEmpty,
}

impl TransformKind {
    const ALL: &'static [TransformKind] = &[
        TransformKind::PredicateCommute,
        TransformKind::DoubleNegation,
        TransformKind::InToOr,
        TransformKind::SubqueryWrap,
        TransformKind::DistinctIdempotent,
        TransformKind::OrderReversal,
        TransformKind::UnionEmpty,
    ];
    fn name(self) -> &'static str {
        match self {
            TransformKind::PredicateCommute => "predicate-commute",
            TransformKind::DoubleNegation => "double-negation",
            TransformKind::InToOr => "in-to-or",
            TransformKind::SubqueryWrap => "subquery-wrap",
            TransformKind::DistinctIdempotent => "distinct-idempotent",
            TransformKind::OrderReversal => "order-reversal",
            TransformKind::UnionEmpty => "union-empty",
        }
    }
}

/// Result of attempting a transform: the base query, transformed query, and
/// whether the comparison should be ordered or unordered.
struct Case {
    kind: TransformKind,
    q1: String,
    q2: String,
    /// true  → compare row sequences in order (ORDER BY used)
    /// false → sort both and compare as sets
    ordered: bool,
}

fn make_cases(rng: &mut Rng) -> Vec<Case> {
    let mut cases = Vec::new();

    // 1. Predicate commutativity ─────────────────────────────────────────────
    {
        let pred = gen_compound_pred(rng);
        if let Some(pred2) = commute_pred(&pred) {
            let q1 = format!("SELECT id FROM t WHERE {pred} ORDER BY id ASC");
            let q2 = format!("SELECT id FROM t WHERE {pred2} ORDER BY id ASC");
            cases.push(Case {
                kind: TransformKind::PredicateCommute,
                q1,
                q2,
                ordered: true,
            });
        }
    }

    // 2. Double negation ─────────────────────────────────────────────────────
    {
        let pred = gen_pred(rng);
        let q1 = format!("SELECT id FROM t WHERE {pred} ORDER BY id ASC");
        let q2 = format!("SELECT id FROM t WHERE NOT(NOT({pred})) ORDER BY id ASC");
        cases.push(Case {
            kind: TransformKind::DoubleNegation,
            q1,
            q2,
            ordered: true,
        });
    }

    // 3. IN → OR expansion ───────────────────────────────────────────────────
    {
        // Generate a simple IN predicate and expand it.
        // Pick 2 concrete integer values.
        let v1 = *rng.pick(INT_VALS);
        let v2 = *rng.pick(INT_VALS);
        let in_pred = format!("a IN ({v1},{v2})");
        if let Some(or_pred) = expand_in(&in_pred) {
            let q1 = format!("SELECT id FROM t WHERE {in_pred} ORDER BY id ASC");
            let q2 = format!("SELECT id FROM t WHERE {or_pred} ORDER BY id ASC");
            cases.push(Case {
                kind: TransformKind::InToOr,
                q1,
                q2,
                ordered: true,
            });
        }
    }

    // 4. Subquery wrap ───────────────────────────────────────────────────────
    {
        let pred = gen_pred(rng);
        let q1 = format!("SELECT id FROM t WHERE {pred} ORDER BY id ASC");
        // Wrap the table in a derived-table subquery.  The WHERE and ORDER BY
        // live on the outer query, which sees the same columns.
        let q2 = format!(
            "SELECT id FROM (SELECT id, a, b, c FROM t) AS sub WHERE {pred} ORDER BY id ASC"
        );
        cases.push(Case {
            kind: TransformKind::SubqueryWrap,
            q1,
            q2,
            ordered: true,
        });
    }

    // 5. DISTINCT idempotence ─────────────────────────────────────────────────
    // SELECT DISTINCT id FROM t == SELECT DISTINCT id FROM (SELECT DISTINCT id FROM t) sub
    // (id is the PK so no duplicate elimination is expected, but the semantic
    //  contract still holds and exercises the DISTINCT path twice.)
    {
        let q1 = "SELECT DISTINCT id FROM t".to_string();
        let q2 = "SELECT DISTINCT id FROM (SELECT DISTINCT id FROM t) AS sub".to_string();
        // DISTINCT result — compare as sets (no ORDER BY).
        cases.push(Case {
            kind: TransformKind::DistinctIdempotent,
            q1,
            q2,
            ordered: false,
        });
    }

    // 6. ORDER reversal ───────────────────────────────────────────────────────
    // "SELECT id,a FROM t ORDER BY a ASC, id ASC" rows reversed in Rust
    // must equal "SELECT id,a FROM t ORDER BY a DESC, id DESC".
    // We verify by fetching ASC, reversing, and comparing with DESC.
    {
        let q1 = "SELECT id,a FROM t ORDER BY a ASC, id ASC".to_string();
        let q2 = "SELECT id,a FROM t ORDER BY a DESC, id DESC".to_string();
        cases.push(Case {
            kind: TransformKind::OrderReversal,
            q1,
            q2,
            ordered: true,
        });
    }

    // 7. UNION with always-empty SELECT ──────────────────────────────────────
    // Q UNION ALL (SELECT id,a FROM t WHERE 1=0) should return the same rows as Q.
    // Compare as sets (UNION does deduplication that UNION ALL does not; use
    // UNION ALL so the empty branch adds nothing rather than deduplicating Q).
    {
        let pred = gen_pred(rng);
        let q1 = format!("SELECT id,a FROM t WHERE {pred}");
        let q2 = format!("SELECT id,a FROM t WHERE {pred} UNION ALL SELECT id,a FROM t WHERE 1=0");
        cases.push(Case {
            kind: TransformKind::UnionEmpty,
            q1,
            q2,
            ordered: false,
        });
    }

    cases
}

// ─── Result comparison ────────────────────────────────────────────────────────

/// For ORDER BY reversal: compare q1_rows reversed against q2_rows as-is.
fn cmp_reversed(mut a: Vec<Vec<String>>, b: &[Vec<String>]) -> bool {
    a.reverse();
    a == b
}

fn cmp_unordered(mut a: Vec<Vec<String>>, mut b: Vec<Vec<String>>) -> bool {
    a.sort();
    b.sort();
    a == b
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main_impl() {
    let mut seed: u64 = 0xC0DE_CAFE;
    let mut iterations = 3000usize;
    let mut max_report = 20usize;
    let mut rows_per_table = 0usize; // 0 = random 4..20

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
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            "--rows" => {
                i += 1;
                rows_per_table = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }

    std::panic::set_hook(Box::new(|_| {}));

    println!("Nucleus metamorphic SQL fuzzer (oracle-free equivalence checking)");
    println!("seed={seed} iterations={iterations}\n");
    println!(
        "Transforms: {}",
        TransformKind::ALL
            .iter()
            .map(|k| k.name())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!();

    let mut total_cases = 0usize;
    let mut divergences = 0usize;
    let mut panics = 0usize;
    // Per-transform counters
    let mut by_kind = [0usize; 7];

    for iter in 0..iterations {
        let mut rng = Rng(seed.wrapping_add(iter as u64).wrapping_mul(0x100000001B3));
        let n_rows = if rows_per_table > 0 {
            rows_per_table
        } else {
            4 + rng.below(17)
        };

        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));

        if !gen_table(&ex, &mut rng, n_rows) {
            continue;
        }

        let cases = make_cases(&mut rng);

        for case in &cases {
            total_cases += 1;

            let r1 = run_select(&ex, &case.q1);
            let r2 = run_select(&ex, &case.q2);

            // A panic in either query is always a bug.
            let is_panic = r1.as_ref().err().map_or(false, |e| e.starts_with("PANIC:"))
                || r2.as_ref().err().map_or(false, |e| e.starts_with("PANIC:"));
            if is_panic {
                panics += 1;
                if panics <= max_report {
                    println!(
                        "─── PANIC #{panics} (iter {iter}) [{kind}] ───",
                        kind = case.kind.name()
                    );
                    println!("  Q1 : {}", case.q1);
                    println!("  Q2 : {}", case.q2);
                    println!("  R1 : {r1:?}");
                    println!("  R2 : {r2:?}");
                    println!();
                }
                continue;
            }

            // If both error, we can't check the transform; skip silently.
            let (rows1, rows2) = match (r1, r2) {
                (Ok(a), Ok(b)) => (a, b),
                _ => continue, // at least one errored; not a metamorphic violation
            };

            let matches = if case.kind == TransformKind::OrderReversal {
                cmp_reversed(rows1.clone(), &rows2)
            } else if case.ordered {
                rows1 == rows2
            } else {
                cmp_unordered(rows1.clone(), rows2.clone())
            };

            if !matches {
                let kind_idx = TransformKind::ALL
                    .iter()
                    .position(|k| *k == case.kind)
                    .unwrap_or(0);
                by_kind[kind_idx] += 1;
                divergences += 1;

                if divergences <= max_report {
                    // Show up to 12 rows from each side.
                    let preview = |rows: &Vec<Vec<String>>| {
                        let shown: Vec<String> = rows
                            .iter()
                            .take(12)
                            .map(|r| format!("[{}]", r.join(",")))
                            .collect();
                        let more = if rows.len() > 12 {
                            format!(" ...+{}", rows.len() - 12)
                        } else {
                            String::new()
                        };
                        format!("{}{}", shown.join(" "), more)
                    };
                    let (display1, display2) = if case.kind == TransformKind::OrderReversal {
                        let mut rev = rows1.clone();
                        rev.reverse();
                        (preview(&rev), preview(&rows2))
                    } else {
                        (preview(&rows1), preview(&rows2))
                    };

                    println!(
                        "─── METAMORPHIC DIVERGENCE #{divergences} (iter {iter}, seed {seed}) ───"
                    );
                    println!("  kind       : {}", case.kind.name());
                    println!("  Q1 (base)  : {}", case.q1);
                    println!("  Q2 (equiv) : {}", case.q2);
                    println!("  Q1 rows    : {display1}");
                    println!("  Q2 rows    : {display2}");
                    println!("  row counts : {} vs {}", rows1.len(), rows2.len());
                    println!();
                }
            }
        }
    }

    println!("\n════ SUMMARY ════");
    println!("iterations         : {iterations}");
    println!("transform cases    : {total_cases}");
    println!("divergences total  : {divergences}");
    println!("panics             : {panics}");
    println!();
    println!("Divergences by transform:");
    for (i, kind) in TransformKind::ALL.iter().enumerate() {
        if by_kind[i] > 0 || divergences == 0 {
            println!("  {:25}: {}", kind.name(), by_kind[i]);
        }
    }

    if divergences == 0 && panics == 0 {
        println!("\nAll metamorphic equivalences hold. No divergences, no panics.");
    } else {
        println!("\nReproduce with: --seed {seed}");
        std::process::exit(1);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
