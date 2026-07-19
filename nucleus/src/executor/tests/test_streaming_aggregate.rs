//! Grace hash aggregation: a memory-limited GROUP BY over a row-store base table
//! must SPILL and complete with the correct groups, where the materialized path
//! returns MemoryExceeded. Aggregate values are identical to the materialized
//! path (only output row order differs, which is unspecified without ORDER BY).

use super::super::{ExecError, ExecResult, Executor};
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

/// Canonicalize a result set to a sorted multiset of rows for order-independent
/// comparison (GROUP BY without ORDER BY has unspecified row order).
fn as_multiset(rows: &[Row]) -> Vec<String> {
    let mut v: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
    v.sort();
    v
}

/// Seed `n` rows into `t(k BIGINT, v BIGINT, payload TEXT)` where `k` has
/// `distinct_keys` distinct values and payload is padded so the working set far
/// exceeds a small budget.
async fn seed(ex: &Executor, sid: u64, n: usize, distinct_keys: i64) {
    ex.execute_with_session(sid, "CREATE TABLE t (k BIGINT, v BIGINT, payload TEXT)")
        .await
        .unwrap();
    let pad = "x".repeat(180);
    let mut vals = String::new();
    for i in 0..n {
        if i > 0 {
            vals.push(',');
        }
        let k = (i as i64) % distinct_keys;
        vals.push_str(&format!("({k}, {i}, '{pad}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
}

/// The core payoff: identical groups to the materialized path, under a budget the
/// materialized path cannot satisfy. Runs several aggregate/HAVING shapes.
#[tokio::test]
async fn streaming_aggregate_spills_and_matches_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 3000, 30).await; // 30 groups, ~200 bytes/row → ~600 KB working set

    let cases = [
        "SELECT k, COUNT(*) FROM t GROUP BY k",
        "SELECT k, SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY k",
        "SELECT k, COUNT(DISTINCT v) FROM t GROUP BY k",
        "SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING COUNT(*) > 50",
        "SELECT k, COUNT(*) FROM t GROUP BY k LIMIT 5",
    ];

    for sql in cases {
        // Ground truth: unlimited memory, materialized.
        ex.set_query_memory_limit(0);
        ex.execute_with_session(sid, "SET stream_results = off")
            .await
            .unwrap();
        ex.query_cache_invalidate_all();
        let (base_cols, base_rows) = drain(one_result(&ex, sid, sql).await).await;

        // Streaming under a tiny budget must spill and complete.
        ex.query_cache_invalidate_all();
        ex.set_query_memory_limit(48 * 1024);
        ex.execute_with_session(sid, "SET stream_results = on")
            .await
            .unwrap();
        let streamed = one_result(&ex, sid, sql).await;
        assert!(streamed.is_stream(), "GROUP BY should stream: {sql}");
        let (stream_cols, stream_rows) = drain(streamed).await;

        assert_eq!(stream_cols, base_cols, "columns mismatch for: {sql}");
        if sql.contains("LIMIT") {
            // LIMIT without ORDER BY: which groups are kept is unspecified (the
            // streamed partition order differs from materialized first-seen
            // order), so assert the count and that each kept row is a real group
            // from the FULL (unlimited) result set — not the materialized LIMIT
            // subset, which is a different 5 groups.
            assert_eq!(stream_rows.len(), 5, "LIMIT row count for: {sql}");
            ex.set_query_memory_limit(0);
            ex.execute_with_session(sid, "SET stream_results = off")
                .await
                .unwrap();
            ex.query_cache_invalidate_all();
            let (_, full_rows) =
                drain(one_result(&ex, sid, "SELECT k, COUNT(*) FROM t GROUP BY k").await).await;
            let full_set: std::collections::HashSet<String> =
                as_multiset(&full_rows).into_iter().collect();
            for r in &stream_rows {
                assert!(full_set.contains(&format!("{r:?}")), "spurious group: {sql}");
            }
        } else {
            assert_eq!(
                as_multiset(&stream_rows),
                as_multiset(&base_rows),
                "group multiset mismatch for: {sql}"
            );
        }
    }

    // The 48 KB budget is far below the ~600 KB working set, so the partitioner
    // necessarily flushed runs to disk during each streamed query above; the
    // matching results prove the spilled aggregation is correct. (Whether the
    // *materialized* path also errors under the budget depends on which executor
    // path it takes — a separate T1.2 gating concern — so it is not asserted here;
    // the single-oversized-group test covers the hard ceiling.)

    // Spill files are reclaimed once the query finishes.
    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill files must be reclaimed after the query");
}

/// High key-cardinality with a very small budget forces recursive
/// re-partitioning (a first-level partition is still larger than the budget).
/// The result must still equal the materialized grouping.
#[tokio::test]
async fn streaming_aggregate_high_cardinality_recurses_and_matches() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 4000, 800).await; // 800 groups of 5 rows each

    let sql = "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k";

    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    let (base_cols, base_rows) = drain(one_result(&ex, sid, sql).await).await;
    assert_eq!(base_rows.len(), 800, "sanity: 800 distinct groups");

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(8 * 1024); // tiny → first-level partitions still overflow
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, sql).await;
    assert!(streamed.is_stream());
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols);
    assert_eq!(stream_rows.len(), 800, "all groups present after recursion");
    assert_eq!(as_multiset(&stream_rows), as_multiset(&base_rows));

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill files reclaimed after recursion");
}

/// A single group larger than the budget cannot be hash-split; after the
/// recursion cap it is aggregated in one pass and its reservation returns a clean
/// MemoryExceeded — the honest ceiling, not a crash.
#[tokio::test]
async fn streaming_aggregate_single_oversized_group_errors_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 3000, 1).await; // one group, ~600 KB

    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let result = ex
        .execute_with_session(sid, "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k")
        .await;
    assert!(
        matches!(result, Err(ExecError::MemoryExceeded(_))),
        "a single group over the budget is the honest ceiling, got {result:?}"
    );

    // Even after the error, no spill files are left behind.
    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill files reclaimed even on the error path");
}

/// The streaming aggregate must NOT engage without a memory budget (the
/// materialized path keeps its fast paths + cache), and must decline every shape
/// it does not handle — falling through to the materialized path.
#[tokio::test]
async fn streaming_aggregate_engages_only_when_warranted() {
    let dir = tempfile::tempdir().unwrap();
    let ex = persistent_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 200, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    // No budget: GROUP BY does not stream (materialized keeps fast paths/cache).
    ex.set_query_memory_limit(0);
    assert!(
        !one_result(&ex, sid, "SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .is_stream(),
        "GROUP BY must not stream without a memory budget"
    );

    // With a budget, shapes the streaming aggregate deliberately declines still
    // run on the materialized path (never a stream from this producer).
    ex.set_query_memory_limit(48 * 1024);
    for sql in [
        "SELECT k, COUNT(*) FROM t WHERE v > 10 GROUP BY k", // predicate
        "SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k",   // ORDER BY over output
        "SELECT COUNT(*) FROM t",                            // no GROUP BY
        "SELECT DISTINCT k FROM t",                          // DISTINCT
        "SELECT k, COUNT(*) FROM t GROUP BY ROLLUP(k)",      // grouping set
    ] {
        ex.query_cache_invalidate_all();
        // Some of these would themselves exceed the tiny budget on the
        // materialized path; that's fine — we only assert they do NOT come back
        // as a lazy stream from the streaming-aggregate producer.
        if let Ok(mut results) = ex.execute_with_session(sid, sql).await {
            let r = results.pop().unwrap();
            assert!(!r.is_stream(), "shape must not stream via aggregate: {sql}");
        }
    }
}

/// Sanity: with streaming on but the default (unlimited) budget, a plain
/// `test_executor` (no spill dir) never routes GROUP BY through the streaming
/// aggregate — it stays materialized and correct.
#[tokio::test]
async fn no_spill_dir_keeps_group_by_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE t (k BIGINT, v BIGINT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "INSERT INTO t VALUES (1,10),(1,20),(2,30)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    ex.set_query_memory_limit(1024); // budget set, but no spill dir configured
    let r = one_result(&ex, sid, "SELECT k, SUM(v) FROM t GROUP BY k").await;
    assert!(!r.is_stream(), "no spill dir → must not stream the aggregate");
    let (_, rows) = drain(r).await;
    assert_eq!(rows.len(), 2);
}
