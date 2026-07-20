//! Streaming WHERE filter (Phase 1.2 read-side): an opt-in `SELECT ... FROM t
//! WHERE <predicate>` streams the predicate over the FULL scan, keeping only
//! matches, where the materialized path would buffer the whole (large) result.
//! Results are identical to the materialized path (multiset — row order is
//! unspecified without ORDER BY). The full-relation scan keeps the conservative
//! SIREAD read set (validated separately by the SSI census); this file checks
//! result correctness, the pipeline order (WHERE before ORDER BY / LIMIT /
//! projection), and the decline (fall-through) shapes.

use super::super::{ExecResult, Executor};
use crate::types::Row;
use std::sync::Arc;

/// An `Arc<Executor>` WITH the self-reference installed — required for the
/// streaming filter to engage (it holds an owned Arc across the drain boundary).
fn arc_executor(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> =
        Arc::new(crate::storage::MemoryEngine::new());
    let ex = Arc::new(Executor::new_with_persistence(catalog, storage, None, Some(dir)));
    ex.install_self_ref();
    ex
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

/// Seed `n` rows into `t(id BIGINT, cat BIGINT, name TEXT, payload TEXT)` — no
/// index — with a padded payload so the working set dwarfs a small budget.
async fn seed(ex: &Executor, sid: u64, n: usize) {
    ex.execute_with_session(
        sid,
        "CREATE TABLE t (id BIGINT, cat BIGINT, name TEXT, payload TEXT)",
    )
    .await
    .unwrap();
    let pad = "x".repeat(180);
    let mut vals = String::new();
    for i in 0..n {
        if i > 0 {
            vals.push(',');
        }
        let cat = (i as i64) % 5;
        vals.push_str(&format!("({i}, {cat}, 'n{i}', '{pad}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
}

/// Ground truth (materialized, unlimited) vs streaming (budgeted) — identical
/// columns and row multiset.
async fn assert_stream_matches(ex: &Executor, sid: u64, sql: &str) {
    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    ex.query_cache_invalidate_all();
    let (base_cols, base_rows) = drain(one_result(ex, sid, sql).await).await;

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(ex, sid, sql).await;
    assert!(streamed.is_stream(), "WHERE should stream: {sql}");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols, "columns mismatch for: {sql}");
    assert_eq!(
        as_multiset(&stream_rows),
        as_multiset(&base_rows),
        "row multiset mismatch for: {sql}"
    );
}

#[tokio::test]
async fn streaming_filter_matches_materialized() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await;

    let cases = [
        "SELECT * FROM t WHERE id > 1500",
        "SELECT * FROM t WHERE cat = 2",
        "SELECT id, name FROM t WHERE cat = 2 AND id < 1000",
        "SELECT id FROM t WHERE cat IN (1, 3)",
        "SELECT id, cat FROM t WHERE id BETWEEN 100 AND 200",
        "SELECT * FROM t WHERE name LIKE 'n1%'",
        "SELECT * FROM t WHERE cat = 2 OR id = 7",
        "SELECT * FROM t WHERE NOT (cat = 0)",
        "SELECT id FROM t WHERE cat = 999", // empty result
    ];
    for sql in cases {
        assert_stream_matches(&ex, sid, sql).await;
    }

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "no spill files from a plain filter");
}

/// WHERE composes with the streaming pipeline in SQL order: filter, then ORDER BY
/// (full external sort), then projection.
#[tokio::test]
async fn streaming_filter_then_order_by_matches() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 2000).await;

    // ORDER BY (no LIMIT) streams via the external sort; WHERE runs before it.
    let sql = "SELECT id, cat FROM t WHERE cat = 3 ORDER BY id";

    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    ex.query_cache_invalidate_all();
    let (base_cols, base_rows) = drain(one_result(&ex, sid, sql).await).await;

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, sql).await;
    assert!(streamed.is_stream(), "WHERE + ORDER BY should stream");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols);
    // ORDER BY fixes the order: compare exactly (not as a multiset).
    assert_eq!(stream_rows, base_rows, "WHERE+ORDER BY row/order mismatch");
    assert!(!stream_rows.is_empty(), "cat=3 should match some rows");
}

/// WHERE + LIMIT streams and returns matching rows (the count is fixed; which
/// rows, without ORDER BY, is unspecified, so every kept row must be a real match).
#[tokio::test]
async fn streaming_filter_with_limit() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await;

    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    ex.query_cache_invalidate_all();
    let (_, full) = drain(one_result(&ex, sid, "SELECT id FROM t WHERE cat = 1").await).await;
    let full_set: std::collections::HashSet<String> = as_multiset(&full).into_iter().collect();

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, "SELECT id FROM t WHERE cat = 1 LIMIT 8").await;
    assert!(streamed.is_stream());
    let (_, rows) = drain(streamed).await;
    assert_eq!(rows.len(), 8, "LIMIT count");
    for r in &rows {
        assert!(full_set.contains(&format!("{r:?}")), "spurious filtered row under LIMIT");
    }
}

/// The streaming filter declines (falls through to the materialized path) for:
/// no self-ref, a subquery predicate, and a predicate an index could serve.
#[tokio::test]
async fn streaming_filter_declines_when_appropriate() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed(&ex, sid, 200).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    ex.set_query_memory_limit(48 * 1024);

    // Subquery predicate: declines (sync_block_on must not run in the drain).
    ex.query_cache_invalidate_all();
    assert!(
        !one_result(
            &ex,
            sid,
            "SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE cat = 1)",
        )
        .await
        .is_stream(),
        "subquery predicate must not stream"
    );

    // A predicate an index can serve: create an index on `cat`, then a `cat`
    // filter must decline (materialized index scan is preferred).
    ex.execute_with_session(sid, "CREATE INDEX idx_cat ON t (cat)")
        .await
        .unwrap();
    ex.query_cache_invalidate_all();
    assert!(
        !one_result(&ex, sid, "SELECT * FROM t WHERE cat = 2")
            .await
            .is_stream(),
        "an indexed-column predicate must not stream (index scan preferred)"
    );
    // ...but a predicate on a NON-indexed column still streams.
    ex.query_cache_invalidate_all();
    assert!(
        one_result(&ex, sid, "SELECT * FROM t WHERE id > 50")
            .await
            .is_stream(),
        "a non-indexed predicate should still stream"
    );
}

/// Without an installed self-ref (a by-value / embedded executor), a WHERE query
/// cannot hold the owning Arc the filter needs, so it declines and materializes —
/// still correct.
#[tokio::test]
async fn no_self_ref_declines_but_is_correct() {
    let dir = tempfile::tempdir().unwrap();
    // NOTE: deliberately NOT installing the self-ref.
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> =
        Arc::new(crate::storage::MemoryEngine::new());
    let ex = Executor::new_with_persistence(catalog, storage, None, Some(dir.path()));
    let sid = ex.create_session();
    seed(&ex, sid, 100).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    ex.set_query_memory_limit(48 * 1024);

    let r = one_result(&ex, sid, "SELECT id FROM t WHERE id > 50").await;
    assert!(!r.is_stream(), "no self-ref ⇒ WHERE must not stream");
    let (_, rows) = drain(r).await;
    assert_eq!(rows.len(), 49, "materialized fallback is still correct");
}
