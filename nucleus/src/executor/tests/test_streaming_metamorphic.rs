//! Metamorphic differential for the streaming-execution tier: for a random query
//! over random data, the STREAMING result (SET stream_results = on, tiny budget →
//! the Grace operators / WHERE filter / lazy emitters engage and spill) must equal
//! the MATERIALIZED result (streaming off, unlimited budget) as a multiset.
//!
//! Why this is a strong oracle: the materialized path is itself validated against
//! SQLite by the differential fuzzer, so `streaming ≡ materialized ≡ SQLite`
//! transitively — and comparing the two Nucleus paths sidesteps NULL-ordering and
//! MemoryExceeded reconciliation against an external engine. It targets the shapes
//! the streaming producers actually accept (bare scans, WHERE filters, GROUP BY,
//! DISTINCT, two-table equi-joins, ORDER BY over output) so most iterations
//! exercise real streaming code, not the decline-to-materialized fallback.
//!
//! Aggregates are restricted to COUNT/MIN/MAX (SUM/AVG have a known, orthogonal
//! path-dependent result *type* quirk — Float64 vs Int64 — that would show up as a
//! value-format mismatch here without indicating a streaming bug).

use super::super::{ExecError, ExecResult, Executor};
use crate::types::Row;
use std::sync::Arc;

/// Minimal deterministic LCG — no external rng dependency, fully reproducible from
/// the seed printed on failure.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(0x5851_f42d_4c95_7f2d).wrapping_add(1);
        self.0 >> 17
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

fn arc_executor(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> =
        Arc::new(crate::storage::MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new_with_persistence(catalog, storage, None, Some(dir)));
    ex.install_self_ref();
    ex
}

/// Columns available for projection / grouping / predicates / joins. `a` and `g`
/// are low-cardinality (good group/join keys); `b` is nullable; `s` is text.
const COLS: [&str; 5] = ["id", "a", "b", "g", "s"];
const KEYS: [&str; 3] = ["a", "g", "id"]; // NOT NULL, usable as join/group keys

async fn seed(ex: &Executor, sid: u64, rng: &mut Rng) {
    ex.execute_with_session(sid, "CREATE TABLE t (id BIGINT, a BIGINT, b BIGINT, g BIGINT, s TEXT)")
        .await
        .unwrap();
    let n = 30 + rng.below(50);
    let card = 3 + rng.below(6); // distinct group/join key values
    let pad = "p".repeat(40);
    let mut vals = String::new();
    for i in 0..n {
        if i > 0 {
            vals.push(',');
        }
        let a = rng.below(card);
        let g = rng.below(card);
        // b nullable; s a small padded domain (dedup / group friendly).
        let b = if rng.chance(20) {
            "NULL".to_string()
        } else {
            rng.below(card).to_string()
        };
        let s = format!("'{}{pad}'", rng.below(card));
        vals.push_str(&format!("({i}, {a}, {b}, {g}, {s})"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
}

fn predicate(rng: &mut Rng) -> String {
    let one = |rng: &mut Rng| {
        let col = rng.pick(&["a", "b", "g", "id"]);
        let op = rng.pick(&["=", "<>", "<", ">", "<=", ">="]);
        format!("{col} {op} {}", rng.below(6))
    };
    let mut p = one(rng);
    if rng.chance(40) {
        let conj = if rng.chance(50) { "AND" } else { "OR" };
        p = format!("{p} {conj} {}", one(rng));
    }
    if rng.chance(20) {
        p = format!("NOT ({p})");
    }
    p
}

/// Pick `n` distinct column names from `pool`.
fn pick_cols(rng: &mut Rng, pool: &[&str], n: usize) -> Vec<String> {
    let mut names: Vec<String> = pool.iter().map(|s| s.to_string()).collect();
    let take = n.min(names.len());
    for i in 0..take {
        let j = i + rng.below(names.len() - i);
        names.swap(i, j);
    }
    names.truncate(take);
    names
}

/// A random streaming-eligible query. Returns the SQL.
fn gen_query(rng: &mut Rng) -> String {
    match rng.below(5) {
        // Bare scan (+ optional WHERE / ORDER BY-source / LIMIT).
        0 => {
            let proj = if rng.chance(35) {
                "*".to_string()
            } else {
                { let n = 1 + rng.below(3); pick_cols(rng, &COLS, n) }.join(", ")
            };
            let w = if rng.chance(55) {
                format!(" WHERE {}", predicate(rng))
            } else {
                String::new()
            };
            // Optional ORDER BY over source cols (no LIMIT — a bare `LIMIT` without a
            // TOTAL order returns an unspecified row subset, so streaming and
            // materialized may pick different valid rows; not comparable as a
            // multiset. The streaming scan also declines ORDER BY + LIMIT anyway.)
            let order = if rng.chance(45) {
                let keys = { let n = 1 + rng.below(2); pick_cols(rng, &KEYS, n) };
                let ob: Vec<String> = keys
                    .iter()
                    .map(|k| format!("{k} {}", if rng.chance(50) { "ASC" } else { "DESC" }))
                    .collect();
                format!(" ORDER BY {}, id ASC", ob.join(", "))
            } else {
                String::new()
            };
            format!("SELECT {proj} FROM t{w}{order}")
        }
        // GROUP BY (COUNT/MIN/MAX only) + optional ORDER BY output + LIMIT.
        1 => {
            let gcols = { let n = 1 + rng.below(2); pick_cols(rng, &["a", "g", "s"], n) };
            let g = gcols.join(", ");
            let agg = match rng.below(4) {
                0 => "COUNT(*) AS c".to_string(),
                1 => format!("COUNT({}) AS c", rng.pick(&COLS)),
                2 => format!("MIN({}) AS c", rng.pick(&["a", "b", "g", "id"])),
                _ => format!("MAX({}) AS c", rng.pick(&["a", "b", "g", "id"])),
            };
            let order = if rng.chance(55) {
                // Order by an output name or ordinal (deterministic tiebreak by all
                // group cols keeps ties from making the multiset compare ambiguous —
                // multiset compare is order-independent anyway).
                let dir = if rng.chance(50) { "ASC" } else { "DESC" };
                format!(" ORDER BY c {dir}, {g}")
            } else {
                String::new()
            };
            let lim = if !order.is_empty() && rng.chance(50) {
                format!(" LIMIT {}", 1 + rng.below(8))
            } else {
                String::new()
            };
            format!("SELECT {g}, {agg} FROM t GROUP BY {g}{order}{lim}")
        }
        // DISTINCT + optional ORDER BY output + LIMIT.
        2 => {
            let cols = { let n = 1 + rng.below(3); pick_cols(rng, &COLS, n) };
            let c = cols.join(", ");
            let order = if rng.chance(55) {
                let dir = if rng.chance(50) { "ASC" } else { "DESC" };
                format!(" ORDER BY {c} {dir}")
            } else {
                String::new()
            };
            let lim = if !order.is_empty() && rng.chance(50) {
                format!(" LIMIT {}", 1 + rng.below(8))
            } else {
                String::new()
            };
            format!("SELECT DISTINCT {c} FROM t{order}{lim}")
        }
        // Two-table equi self-join.
        _ => {
            let k = rng.pick(&KEYS);
            let jt = rng.pick(&["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]);
            let proj = match rng.below(3) {
                0 => format!("x1.id, x2.{}", rng.pick(&COLS)),
                1 => "x1.a, x2.g, x1.s".to_string(),
                _ => "x1.*".to_string(),
            };
            // No LIMIT: the streaming join can't stream ORDER BY, so a bare LIMIT
            // would pick an unspecified subset (not multiset-comparable). Compare
            // the full join instead.
            format!("SELECT {proj} FROM t x1 {jt} t x2 ON x1.{k} = x2.{k}")
        }
    }
}

async fn run(ex: &Executor, sid: u64, sql: &str) -> Result<Vec<Row>, ExecError> {
    let mut results = ex.execute_with_session(sid, sql).await?;
    match results.pop() {
        Some(r) => match r.materialize().await? {
            ExecResult::Select { rows, .. } => Ok(rows),
            other => panic!("expected Select for {sql}, got {other:?}"),
        },
        None => Ok(Vec::new()),
    }
}

fn multiset(rows: &[Row]) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
    v.sort();
    v
}

/// Direct regression for the materialized range + ORDER BY bug this metamorphic
/// oracle surfaced: a two-sided range with a STRICT bound whose column is filtered
/// and ordered but NOT projected (`SELECT g … WHERE a > 0 AND a <= 3 ORDER BY a`)
/// used an inclusive coarse-range fast scan whose strict-bound recheck was dropped,
/// leaking the excluded boundary row (`a = 0`). Runs materialized (streaming off).
#[tokio::test]
async fn range_with_strict_bound_and_order_by_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE t (id BIGINT, a BIGINT, g BIGINT)")
        .await
        .unwrap();
    let mut v = String::new();
    for i in 0..70 {
        if i > 0 {
            v.push(',');
        }
        v.push_str(&format!("({i},{},{})", i % 5, i % 4)); // a,g in 0..5 / 0..4
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {v}"))
        .await
        .unwrap();
    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();

    // a in {0,1,2,3,4}, 14 rows each. Each of these selects a ∈ {1,2,3} = 42 rows;
    // the boundary a = 0 must be excluded despite the inclusive coarse scan.
    for sql in [
        "SELECT g FROM t WHERE a > 0 AND a <= 3 ORDER BY a DESC",
        "SELECT g FROM t WHERE a <= 3 AND a > 0 ORDER BY a DESC",
        "SELECT g FROM t WHERE a > 0 AND a < 4 ORDER BY a DESC",
    ] {
        ex.query_cache_invalidate_all();
        let (_, rows) = drain(one_result(&ex, sid, sql).await).await;
        assert_eq!(rows.len(), 42, "strict lower bound must exclude a=0: {sql}");
    }
    // Inclusive lower keeps a = 0 (56 rows) — the fast path stays correct there.
    ex.query_cache_invalidate_all();
    let (_, incl) =
        drain(one_result(&ex, sid, "SELECT g FROM t WHERE a >= 0 AND a <= 3 ORDER BY a DESC").await)
            .await;
    assert_eq!(incl.len(), 56, "inclusive range includes a=0");
}

async fn drain(result: ExecResult) -> (Vec<(String, crate::types::DataType)>, Vec<Row>) {
    match result.materialize().await.unwrap() {
        ExecResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

async fn one_result(ex: &Executor, sid: u64, sql: &str) -> ExecResult {
    ex.execute_with_session(sid, sql).await.unwrap().pop().unwrap()
}

#[tokio::test]
async fn streaming_equals_materialized_over_random_queries() {
    // Scale via env (NUCLEUS_METAMORPHIC_ITERS) for a heavier local/CI sweep.
    let iters: u64 = std::env::var("NUCLEUS_METAMORPHIC_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(250);

    let mut checked = 0usize;
    let mut streamed = 0usize;
    for iter in 0..iters {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ iter.wrapping_mul(0x1000_0001B3));
        let dir = tempfile::tempdir().unwrap();
        let ex = arc_executor(dir.path());
        let sid = ex.create_session();
        seed(&ex, sid, &mut rng).await;

        for _ in 0..6 {
            let sql = gen_query(&mut rng);

            // Materialized ground truth.
            ex.set_query_memory_limit(0);
            ex.execute_with_session(sid, "SET stream_results = off")
                .await
                .unwrap();
            ex.query_cache_invalidate_all();
            let base = match run(&ex, sid, &sql).await {
                Ok(r) => r,
                // A query the materialized path itself rejects is not a streaming
                // concern — skip it (keeps the generator permissive).
                Err(_) => continue,
            };

            // Streaming under a tiny budget → Grace operators engage and spill.
            ex.query_cache_invalidate_all();
            ex.set_query_memory_limit(3 * 1024);
            ex.execute_with_session(sid, "SET stream_results = on")
                .await
                .unwrap();
            let result = ex.execute_with_session(sid, &sql).await;
            let streamed_query = matches!(&result, Ok(rs) if rs.last().is_some_and(|r| r.is_stream()));
            let stream_rows = match result {
                Ok(mut rs) => match rs.pop().unwrap().materialize().await {
                    Ok(ExecResult::Select { rows, .. }) => rows,
                    // Honest ceiling on an unsplittable key — not a divergence.
                    Err(ExecError::MemoryExceeded(_)) => continue,
                    Ok(other) => panic!("expected Select for {sql}, got {other:?}"),
                    Err(e) => panic!("streaming error for `{sql}` (iter {iter}): {e:?}"),
                },
                Err(ExecError::MemoryExceeded(_)) => continue,
                Err(e) => panic!("streaming error for `{sql}` (iter {iter}): {e:?}"),
            };

            checked += 1;
            if streamed_query {
                streamed += 1;
            }
            assert_eq!(
                multiset(&stream_rows),
                multiset(&base),
                "streaming ≠ materialized (iter {iter})\n  query: {sql}"
            );
        }
    }

    // Guard against the generator silently drifting to all-declined shapes (which
    // would make the test vacuous): a healthy fraction must actually stream.
    assert!(checked > 0, "no comparable queries generated");
    assert!(
        streamed * 100 / checked >= 40,
        "only {streamed}/{checked} queries streamed — generator no longer exercises the streaming path"
    );
}
