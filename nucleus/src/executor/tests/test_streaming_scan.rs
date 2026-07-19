//! Phase 1.1: opt-in streaming scan (`SET stream_results = on`) must produce
//! results identical to the materialized path for the bare-scan shape, and must
//! NOT engage for any other shape.

use super::super::{ExecResult, Executor};
use super::test_executor;
use crate::types::Row;

async fn seed(ex: &Executor, sid: u64, n: usize) {
    ex.execute_with_session(sid, "CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .unwrap();
    // One multi-row INSERT so we cross the streaming batch size (2048) cheaply.
    let mut vals = String::new();
    for i in 0..n {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({i}, 'n{i}')"));
    }
    ex.execute_with_session(sid, &format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
}

async fn one_result(ex: &Executor, sid: u64, sql: &str) -> ExecResult {
    let mut results = ex.execute_with_session(sid, sql).await.unwrap();
    assert_eq!(results.len(), 1, "expected exactly one result for: {sql}");
    results.pop().unwrap()
}

async fn drain(result: ExecResult) -> (Vec<(String, crate::types::DataType)>, Vec<Row>) {
    match result.materialize().await.unwrap() {
        ExecResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn streaming_matches_materialized_rows_and_columns() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await; // > batch size, multiple batches

    // Baseline: streaming OFF (default).
    let baseline = one_result(&ex, sid, "SELECT * FROM t").await;
    assert!(!baseline.is_stream(), "default path must materialize");
    let (base_cols, base_rows) = drain(baseline).await;

    // Streaming ON.
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, "SELECT * FROM t").await;
    assert!(streamed.is_stream(), "SELECT * must stream when opted in");
    let (stream_cols, stream_rows) = drain(streamed).await;

    assert_eq!(stream_cols, base_cols, "columns must match exactly");
    assert_eq!(stream_rows, base_rows, "rows must match exactly");
    assert_eq!(stream_rows.len(), 3000);
}

#[tokio::test]
async fn empty_table_streams_to_empty() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .unwrap();
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    let streamed = one_result(&ex, sid, "SELECT * FROM t").await;
    assert!(streamed.is_stream());
    let (_, rows) = drain(streamed).await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn unsupported_shapes_do_not_stream() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    // Shapes the streaming path deliberately declines (predicate/sort/grouping/
    // distinct/CTE, computed or qualified projections). The materialized path runs.
    for sql in [
        "SELECT * FROM t WHERE id > 5",      // predicate
        "SELECT * FROM t ORDER BY id",       // sort
        "SELECT DISTINCT * FROM t",          // distinct
        "SELECT COUNT(*) FROM t",            // aggregate
        "SELECT * FROM t GROUP BY id, name", // group by
        "SELECT id + 1 FROM t",              // computed projection
        "SELECT t.id FROM t",                // qualified projection
        "SELECT UPPER(name) FROM t",         // function projection
        "WITH c AS (SELECT * FROM t) SELECT * FROM c", // CTE
    ] {
        let r = one_result(&ex, sid, sql).await;
        assert!(!r.is_stream(), "shape must NOT stream but did: {sql}");
    }
}

/// Streamed projection / LIMIT / OFFSET must produce byte-identical columns and
/// rows to the materialized path. The materialized result is the ground truth.
#[tokio::test]
async fn projection_and_limit_match_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 3000).await;

    let cases = [
        "SELECT id FROM t",             // single-column projection
        "SELECT name, id FROM t",       // reordered projection
        "SELECT id AS x, name FROM t",  // aliased projection
        "SELECT * FROM t LIMIT 5",      // limit only
        "SELECT * FROM t LIMIT 0",      // zero limit
        "SELECT * FROM t OFFSET 2990",  // offset near end
        "SELECT * FROM t LIMIT 5 OFFSET 3", // limit + offset
        "SELECT id FROM t LIMIT 4 OFFSET 2500", // projection + limit + offset crossing batches
    ];
    for sql in cases {
        // Ground truth: materialized.
        ex.execute_with_session(sid, "SET stream_results = off")
            .await
            .unwrap();
        let base = one_result(&ex, sid, sql).await;
        assert!(!base.is_stream());
        let (base_cols, base_rows) = drain(base).await;

        // Streamed.
        ex.execute_with_session(sid, "SET stream_results = on")
            .await
            .unwrap();
        let streamed = one_result(&ex, sid, sql).await;
        assert!(streamed.is_stream(), "should stream: {sql}");
        let (stream_cols, stream_rows) = drain(streamed).await;

        assert_eq!(stream_cols, base_cols, "columns mismatch for: {sql}");
        assert_eq!(stream_rows, base_rows, "rows mismatch for: {sql}");
    }
}

#[tokio::test]
async fn setting_off_reverts_to_materialized() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;

    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    assert!(one_result(&ex, sid, "SELECT * FROM t").await.is_stream());

    ex.execute_with_session(sid, "SET stream_results = off")
        .await
        .unwrap();
    assert!(
        !one_result(&ex, sid, "SELECT * FROM t").await.is_stream(),
        "turning the setting off must return to the materialized path"
    );
}

#[tokio::test]
async fn missing_table_still_errors_when_streaming() {
    let ex = test_executor();
    let sid = ex.create_session();
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    // Unknown table: the producer declines, the normal path raises the error.
    assert!(
        ex.execute_with_session(sid, "SELECT * FROM nope")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn multi_statement_batch_does_not_stream() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();
    // Two statements in one batch: neither result may be a lazy stream (avoids
    // concurrent producers on one session).
    let results = ex
        .execute_with_session(sid, "SELECT * FROM t; SELECT * FROM t")
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| !r.is_stream()));
}
