//! Grace hash join: a memory-limited two-table equi-JOIN over row-store base
//! tables must PARTITION both sides and complete with the correct rows, where the
//! materialized hash-join build would return MemoryExceeded. Output rows are
//! identical (as a multiset) to the materialized path — only row order differs,
//! which is unspecified without ORDER BY. Covers INNER / LEFT / RIGHT / FULL,
//! aliases, qualified and aliased projections, LIMIT, recursion, the single
//! oversized-key ceiling, and the decline (fall-through) shapes.

use super::super::{ExecResult, Executor};
use super::test_executor;
use crate::types::Row;
use std::sync::Arc;

fn persistent_executor(dir: &std::path::Path) -> Executor {
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> =
        Arc::new(crate::storage::MemoryEngine::new());
    Executor::new_with_persistence(catalog, storage, None, Some(dir))
}

async fn drain(result: ExecResult) -> (Vec<(String, crate::types::DataType)>, Vec<Row>) {
    match result.materialize().await.unwrap() {
        ExecResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

async fn one_result(ex: &Executor, sid: u64, sql: &str) -> ExecResult {
    let mut results = ex.execute_with_session(sid, sql).await.unwrap();
    assert_eq!(results.len(), 1, "expected one result for: {sql}");
    results.pop().unwrap()
}

fn as_multiset(rows: &[Row]) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
    v.sort();
    v
}

/// Seed `orders(oid, cid, opay)` with `n` rows whose `cid = i % order_keys`, and
/// `customers(cid, name, cpay)` with one row per `cid` in `cust_lo..cust_hi`.
/// Choosing the key ranges to overlap only partially yields unmatched rows on
/// BOTH sides (so the outer joins are exercised for real), and the padded payloads
/// make the working set dwarf a small budget.
async fn seed(
    ex: &Executor,
    sid: u64,
    n: usize,
    order_keys: i64,
    cust_lo: i64,
    cust_hi: i64,
) {
    ex.execute_with_session(sid, "CREATE TABLE orders (oid BIGINT, cid BIGINT, opay TEXT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "CREATE TABLE customers (cid BIGINT, name TEXT, cpay TEXT)")
        .await
        .unwrap();
    let pad = "x".repeat(180);

    let mut ovals = String::new();
    for i in 0..n {
        if i > 0 {
            ovals.push(',');
        }
        let cid = (i as i64) % order_keys;
        ovals.push_str(&format!("({i}, {cid}, '{pad}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO orders VALUES {ovals}"))
        .await
        .unwrap();

    let pad2 = "y".repeat(180);
    let mut cvals = String::new();
    for (j, cid) in (cust_lo..cust_hi).enumerate() {
        if j > 0 {
            cvals.push(',');
        }
        cvals.push_str(&format!("({cid}, 'c{cid}', '{pad2}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO customers VALUES {cvals}"))
        .await
        .unwrap();
}

/// Run `sql` materialized (unlimited budget, ground truth) then streaming (tiny
/// budget → must partition/spill), and assert identical columns + row multiset.
async fn assert_stream_matches(ex: &Executor, sid: u64, sql: &str, budget: u64) {
    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    ex.query_cache_invalidate_all();
    let (base_cols, base_rows) = drain(one_result(ex, sid, sql).await).await;

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(budget);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(ex, sid, sql).await;
    assert!(streamed.is_stream(), "JOIN should stream: {sql}");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols, "columns mismatch for: {sql}");
    assert_eq!(
        as_multiset(&stream_rows),
        as_multiset(&base_rows),
        "row multiset mismatch for: {sql}"
    );
}

/// The core payoff: every join flavor and projection shape matches the
/// materialized path under a budget the materialized build cannot satisfy.
#[tokio::test]
async fn streaming_join_spills_and_matches_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    // orders cid 0..44; customers cid 5..49. Overlap 5..44 matches; orders 0..4
    // are unmatched-left; customers 45..49 are unmatched-right.
    seed(&ex, sid, 3000, 45, 5, 50).await;

    let cases = [
        // INNER — plain, aliased, qualified projections.
        "SELECT * FROM orders JOIN customers ON orders.cid = customers.cid",
        "SELECT oid, name FROM orders JOIN customers ON orders.cid = customers.cid",
        "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid",
        "SELECT c.* FROM orders o JOIN customers c ON o.cid = c.cid",
        "SELECT o.oid AS order_id, c.name AS who FROM orders o JOIN customers c ON o.cid = c.cid",
        // Outer joins — must emit the unmatched side, NULL-padded.
        "SELECT o.oid, c.cid FROM orders o LEFT JOIN customers c ON o.cid = c.cid",
        "SELECT o.oid, c.cid FROM orders o RIGHT JOIN customers c ON o.cid = c.cid",
        "SELECT o.oid, c.cid FROM orders o FULL JOIN customers c ON o.cid = c.cid",
    ];

    for sql in cases {
        assert_stream_matches(&ex, sid, sql, 48 * 1024).await;
    }

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill files must be reclaimed after the join");
}

/// LIMIT without ORDER BY: the count is fixed, and every kept row must be a real
/// row from the full (unlimited) join — not necessarily the materialized LIMIT's
/// subset, since streamed partition order differs from materialized order.
#[tokio::test]
async fn streaming_join_limit_keeps_real_rows() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 3000, 45, 5, 50).await;

    let full_sql = "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid";
    let limit_sql =
        "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid LIMIT 9";

    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    ex.query_cache_invalidate_all();
    let (_, full_rows) = drain(one_result(&ex, sid, full_sql).await).await;
    let full_set: std::collections::HashSet<String> = as_multiset(&full_rows).into_iter().collect();

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, limit_sql).await;
    assert!(streamed.is_stream());
    let (_, rows) = drain(streamed).await;
    assert_eq!(rows.len(), 9, "LIMIT row count");
    for r in &rows {
        assert!(full_set.contains(&format!("{r:?}")), "spurious joined row under LIMIT");
    }
}

/// High key-cardinality with a very small budget forces recursive re-partitioning
/// of BOTH sides (a first-level partition-pair still exceeds the budget). The
/// result must still equal the materialized join.
#[tokio::test]
async fn streaming_join_high_cardinality_recurses_and_matches() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    // 1000 join keys, one customer each, ~4 orders per key.
    seed(&ex, sid, 4000, 1000, 0, 1000).await;

    let sql = "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid";

    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    let (base_cols, base_rows) = drain(one_result(&ex, sid, sql).await).await;
    assert_eq!(base_rows.len(), 4000, "sanity: every order matches its customer");

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(8 * 1024); // tiny → first-level pairs still overflow
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, sql).await;
    assert!(streamed.is_stream());
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols);
    assert_eq!(stream_rows.len(), 4000, "all joined rows present after recursion");
    assert_eq!(as_multiset(&stream_rows), as_multiset(&base_rows));

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill files reclaimed after recursion");
}

/// A self-join streams too: both sides resolve to the same base table under
/// different aliases and partition independently by their own key column. Many
/// keys with a small per-key fan-out keeps every pair's output within budget.
#[tokio::test]
async fn streaming_self_join_matches_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 1500, 500, 0, 1).await; // 500 keys, 3 orders each → 9 pairs/key

    // Pair each order with every order sharing its cid (a real fan-out per key).
    let sql = "SELECT a.oid, b.oid FROM orders a JOIN orders b ON a.cid = b.cid";
    assert_stream_matches(&ex, sid, sql, 48 * 1024).await;
}

/// A single join key whose fan-out cannot fit the budget is unsplittable by
/// hashing; after the recursion cap the pair is joined in one pass and
/// `execute_join`'s reservation returns a clean MemoryExceeded — the honest
/// ceiling, never a crash. (One cid on both sides ⇒ a large cartesian pair.)
#[tokio::test]
async fn streaming_join_single_oversized_key_errors_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 2000, 1, 0, 1).await; // all orders share cid 0; customer 0 exists

    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let result = ex
        .execute_with_session(
            sid,
            "SELECT a.oid, b.oid FROM orders a JOIN orders b ON a.cid = b.cid",
        )
        .await;
    assert!(
        matches!(result, Err(crate::executor::ExecError::MemoryExceeded(_))),
        "a single oversized key is the honest ceiling, got {result:?}"
    );

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill files reclaimed even on the error path");
}

/// The streaming join must NOT engage without a memory budget, and must decline
/// every shape it does not handle — falling through to the materialized path.
#[tokio::test]
async fn streaming_join_engages_only_when_warranted() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 200, 20, 0, 25).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    // No budget: the join does not stream (materialized keeps its fast paths).
    ex.set_query_memory_limit(0);
    assert!(
        !one_result(
            &ex,
            sid,
            "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid",
        )
        .await
        .is_stream(),
        "JOIN must not stream without a memory budget"
    );

    // With a budget, shapes the streaming join deliberately declines still run on
    // the materialized path (never a stream from this producer).
    ex.set_query_memory_limit(48 * 1024);
    for sql in [
        // Non-equi ON (no partition key).
        "SELECT o.oid FROM orders o JOIN customers c ON o.cid > c.cid",
        // Cross join (no ON key).
        "SELECT o.oid FROM orders o CROSS JOIN customers c",
        // USING / NATURAL (v1 handles ON only).
        "SELECT o.oid FROM orders o JOIN customers c USING (cid)",
        // A WHERE predicate (predicate-free scan only in v1).
        "SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid WHERE o.oid > 5",
        // A computed projection.
        "SELECT o.oid + 1 FROM orders o JOIN customers c ON o.cid = c.cid",
        // Three tables / two joins.
        "SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid JOIN customers d ON c.cid = d.cid",
        // GROUP BY over the join.
        "SELECT c.cid, COUNT(*) FROM orders o JOIN customers c ON o.cid = c.cid GROUP BY c.cid",
        // ORDER BY over the join output.
        "SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid ORDER BY o.oid",
    ] {
        ex.query_cache_invalidate_all();
        if let Ok(mut results) = ex.execute_with_session(sid, sql).await {
            let r = results.pop().unwrap();
            assert!(!r.is_stream(), "shape must not stream via join: {sql}");
        }
    }
}

/// Sanity: with streaming on but the default (unlimited) budget and no spill dir,
/// a plain `test_executor` never routes a JOIN through the streaming path — it
/// stays materialized and correct.
#[tokio::test]
async fn no_spill_dir_keeps_join_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE a (id BIGINT, v BIGINT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "CREATE TABLE b (id BIGINT, w BIGINT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "INSERT INTO a VALUES (1,10),(2,20),(3,30)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "INSERT INTO b VALUES (1,100),(2,200)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    ex.set_query_memory_limit(1024); // budget set, but no spill dir configured
    let r = one_result(&ex, sid, "SELECT a.id, b.w FROM a JOIN b ON a.id = b.id").await;
    assert!(!r.is_stream(), "no spill dir → must not stream the join");
    let (_, rows) = drain(r).await;
    assert_eq!(rows.len(), 2);
}
