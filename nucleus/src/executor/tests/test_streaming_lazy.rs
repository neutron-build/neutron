//! Lazy per-partition/pair output emitters for the three Grace operators
//! (aggregate, DISTINCT, join). When an owning `Arc<Executor>` is installed (the
//! server/embedded path), the streaming producers emit ONE partition (or
//! partition-pair) per batch instead of materializing the whole result, so peak
//! OUTPUT memory is one partition — the payoff being a large-output join that
//! streams to the wire. Results are identical to the materialized path (multiset;
//! order unspecified without ORDER BY). Two behaviours differ from the eager
//! fallback and are checked here: (1) a GROUP BY's columns are derived up front
//! (before the first row), and (2) a per-partition MemoryExceeded surfaces during
//! DRAIN, not at execute() time.

use super::super::{ExecError, ExecResult, Executor};
use crate::types::Row;
use std::sync::Arc;

/// An `Arc<Executor>` WITH the self-reference installed — the condition that
/// selects the lazy emitters over the eager fallback.
fn arc_executor(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let storage: Arc<dyn crate::storage::StorageEngine> =
        Arc::new(crate::storage::MemoryEngine::new());
    let ex = Arc::new(Executor::new_with_persistence(
        catalog,
        storage,
        None,
        Some(dir),
    ));
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

/// Ground truth (materialized) vs lazy streaming — identical columns + multiset.
async fn assert_lazy_matches(ex: &Executor, sid: u64, sql: &str) {
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
    assert!(streamed.is_stream(), "should stream lazily: {sql}");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(
        stream_cols, base_cols,
        "columns mismatch (up-front schema): {sql}"
    );
    assert_eq!(
        as_multiset(&stream_rows),
        as_multiset(&base_rows),
        "row multiset mismatch for: {sql}"
    );
}

async fn seed_groups(ex: &Executor, sid: u64, n: usize, keys: i64) {
    ex.execute_with_session(sid, "CREATE TABLE t (k BIGINT, v BIGINT, payload TEXT)")
        .await
        .unwrap();
    let pad = "x".repeat(180);
    let mut vals = String::new();
    for i in 0..n {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({}, {i}, '{pad}')", (i as i64) % keys));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
}

/// Lazy GROUP BY: streams, columns derived up front (empty-input schema probe),
/// aggregate values match the materialized path across several shapes.
#[tokio::test]
async fn lazy_aggregate_matches_and_streams() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed_groups(&ex, sid, 3000, 30).await;

    for sql in [
        "SELECT k, COUNT(*) FROM t GROUP BY k",
        "SELECT k, SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY k",
        "SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING COUNT(*) > 50",
    ] {
        assert_lazy_matches(&ex, sid, sql).await;
    }

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill reclaimed after lazy aggregate drain");
}

/// Lazy DISTINCT: streams one deduped partition at a time and matches.
#[tokio::test]
async fn lazy_distinct_matches_and_streams() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    // Heavy duplication so DISTINCT is a real reduction.
    ex.execute_with_session(sid, "CREATE TABLE d (k BIGINT, name TEXT, payload TEXT)")
        .await
        .unwrap();
    let pad = "y".repeat(180);
    let mut vals = String::new();
    for i in 0..3000 {
        if i > 0 {
            vals.push(',');
        }
        let k = (i as i64) % 40;
        vals.push_str(&format!("({k}, 'n{k}', '{pad}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO d VALUES {vals}"))
        .await
        .unwrap();

    for sql in ["SELECT DISTINCT k FROM d", "SELECT DISTINCT k, name FROM d"] {
        assert_lazy_matches(&ex, sid, sql).await;
    }
    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill reclaimed after lazy distinct drain");
}

async fn seed_join(ex: &Executor, sid: u64, orders: usize, keys: i64) {
    ex.execute_with_session(
        sid,
        "CREATE TABLE orders (oid BIGINT, cid BIGINT, opay TEXT)",
    )
    .await
    .unwrap();
    ex.execute_with_session(
        sid,
        "CREATE TABLE customers (cid BIGINT, name TEXT, cpay TEXT)",
    )
    .await
    .unwrap();
    let pad = "x".repeat(180);
    let mut ov = String::new();
    for i in 0..orders {
        if i > 0 {
            ov.push(',');
        }
        ov.push_str(&format!("({i}, {}, '{pad}')", (i as i64) % keys));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO orders VALUES {ov}"))
        .await
        .unwrap();
    let pad2 = "y".repeat(180);
    let mut cv = String::new();
    for cid in 0..keys {
        if cid > 0 {
            cv.push(',');
        }
        cv.push_str(&format!("({cid}, 'c{cid}', '{pad2}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO customers VALUES {cv}"))
        .await
        .unwrap();
}

/// Lazy join: INNER/LEFT/FULL stream one partition-pair at a time and match.
#[tokio::test]
async fn lazy_join_matches_and_streams() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed_join(&ex, sid, 2000, 40).await;

    for sql in [
        "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid",
        "SELECT o.oid, c.cid FROM orders o LEFT JOIN customers c ON o.cid = c.cid",
        "SELECT o.oid, c.cid FROM orders o FULL JOIN customers c ON o.cid = c.cid",
    ] {
        assert_lazy_matches(&ex, sid, sql).await;
    }
    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill reclaimed after lazy join drain");
}

/// The lazy join's payoff: a join whose TOTAL output far exceeds the budget (many
/// keys, each a small fan-out) streams to completion — one pair at a time keeps
/// peak output bounded — and matches the materialized result.
#[tokio::test]
async fn lazy_join_large_output_streams() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    // 400 keys × ~10 orders each = 4000 output rows, each ~400 B ⇒ ~1.6 MB total,
    // far past the 48 KB budget; per-pair (one key, ~10 rows) stays well under it.
    seed_join(&ex, sid, 4000, 400).await;

    let sql = "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid";

    ex.set_query_memory_limit(0);
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    let (_, base_rows) = drain(one_result(&ex, sid, sql).await).await;
    assert_eq!(base_rows.len(), 4000, "sanity: every order matches");

    ex.query_cache_invalidate_all();
    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, sql).await;
    assert!(streamed.is_stream());
    let (_, stream_rows) = drain(streamed).await;
    assert_eq!(stream_rows.len(), 4000, "large output streamed in full");
    assert_eq!(as_multiset(&stream_rows), as_multiset(&base_rows));
}

/// ORDER BY over the aggregate output through the LAZY path: the emitted groups
/// feed an external sort, giving a correct top-N that matches the materialized
/// path exactly. (Tie-broken by k so the order is deterministic.)
#[tokio::test]
async fn lazy_aggregate_order_by_top_n() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed_groups(&ex, sid, 3000, 30).await;

    for sql in [
        "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY c DESC, k",
        "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k DESC LIMIT 7",
    ] {
        ex.set_query_memory_limit(0);
        ex.execute_with_session(sid, "SET stream_results = off")
            .await
            .unwrap();
        ex.query_cache_invalidate_all();
        let (_, base_rows) = drain(one_result(&ex, sid, sql).await).await;

        ex.query_cache_invalidate_all();
        ex.set_query_memory_limit(48 * 1024);
        ex.execute_with_session(sid, "SET stream_results = on")
            .await
            .unwrap();
        let streamed = one_result(&ex, sid, sql).await;
        assert!(streamed.is_stream(), "lazy ORDER BY should stream: {sql}");
        let (_, stream_rows) = drain(streamed).await;
        assert_eq!(stream_rows, base_rows, "lazy ordered rows mismatch: {sql}");
    }
}

/// Direct regression for the materialized DISTINCT + ORDER BY + LIMIT ordering bug
/// (found via the streaming work): SQL must dedup BEFORE limiting. Without the fix
/// this returned a single row (the six equal leading `k=0` rows collapsed after
/// the limit). Runs with streaming OFF to pin the materialized path.
#[tokio::test]
async fn materialized_distinct_order_by_limit_dedups_first() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE d (k BIGINT)")
        .await
        .unwrap();
    let mut v = String::new();
    for i in 0..300 {
        if i > 0 {
            v.push(',');
        }
        v.push_str(&format!("({})", i % 40)); // 40 distinct keys, each repeated
    }
    ex.execute_with_session(sid, &format!("INSERT INTO d VALUES {v}"))
        .await
        .unwrap();
    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    ex.set_query_memory_limit(0);

    let (_, rows) =
        drain(one_result(&ex, sid, "SELECT DISTINCT k FROM d ORDER BY k LIMIT 6").await).await;
    let got: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            crate::types::Value::Int64(n) => n,
            _ => panic!("expected Int64"),
        })
        .collect();
    assert_eq!(
        got,
        vec![0, 1, 2, 3, 4, 5],
        "DISTINCT must dedup before LIMIT"
    );

    // DISTINCT + LIMIT with no ORDER BY: still six DISTINCT values, not ≤6 raw rows.
    let (_, rows2) = drain(one_result(&ex, sid, "SELECT DISTINCT k FROM d LIMIT 6").await).await;
    assert_eq!(rows2.len(), 6, "DISTINCT + LIMIT returns 6 distinct rows");
}

/// A single oversized GROUP BY group can't be hash-split, so it must surface a
/// clean MemoryExceeded (the honest ceiling, not a crash). With lazy emission the
/// aggregate probes the first non-empty partition eagerly (to learn the columns),
/// so a single-group query errors at execute() time; a later oversized partition
/// would instead error mid-drain. Accept either — both are clean.
#[tokio::test]
async fn lazy_aggregate_oversized_group_errors_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed_groups(&ex, sid, 3000, 1).await; // one group, ~600 KB

    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    let err = match ex
        .execute_with_session(sid, "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k")
        .await
    {
        Err(e) => e,
        Ok(mut rs) => rs.pop().unwrap().materialize().await.unwrap_err(),
    };
    assert!(
        matches!(err, ExecError::MemoryExceeded(_)),
        "oversized group must surface a clean MemoryExceeded, got {err:?}"
    );

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "spill reclaimed even on the error path");
}

/// Same deferred-error semantics for the join: a single oversized key surfaces
/// MemoryExceeded on drain, not at execute().
#[tokio::test]
async fn lazy_join_oversized_key_errors_on_drain() {
    let dir = tempfile::tempdir().unwrap();
    let ex = arc_executor(dir.path());
    let sid = ex.create_session();
    seed_join(&ex, sid, 2000, 1).await; // all orders share cid 0 ⇒ huge cartesian pair

    ex.set_query_memory_limit(48 * 1024);
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    let r = one_result(
        &ex,
        sid,
        "SELECT a.oid, b.oid FROM orders a JOIN orders b ON a.cid = b.cid",
    )
    .await;
    assert!(r.is_stream(), "execute returns the stream before the error");
    let drained = r.materialize().await;
    assert!(
        matches!(drained, Err(ExecError::MemoryExceeded(_))),
        "oversized join key must surface a clean MemoryExceeded on drain, got {drained:?}"
    );

    let leftover = std::fs::read_dir(dir.path().join("spill"))
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        leftover, 0,
        "spill reclaimed even on the drain-time error path"
    );
}
