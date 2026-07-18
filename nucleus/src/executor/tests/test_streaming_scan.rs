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
async fn only_bare_select_star_streams() {
    let ex = test_executor();
    let sid = ex.create_session();
    seed(&ex, sid, 10).await;
    ex.execute_with_session(sid, "SET stream_results = on")
        .await
        .unwrap();

    // Every one of these has a clause that changes row/column shape, so the
    // streaming producer must decline and the normal materialized path runs.
    for sql in [
        "SELECT id FROM t",                 // projection subset
        "SELECT * FROM t WHERE id > 5",     // predicate
        "SELECT * FROM t ORDER BY id",      // sort
        "SELECT * FROM t LIMIT 3",          // limit
        "SELECT * FROM t OFFSET 2",         // offset
        "SELECT DISTINCT * FROM t",         // distinct
        "SELECT COUNT(*) FROM t",           // aggregate
        "SELECT * FROM t GROUP BY id, name",// group by
        "WITH c AS (SELECT * FROM t) SELECT * FROM c", // CTE
    ] {
        let r = one_result(&ex, sid, sql).await;
        assert!(
            !r.is_stream(),
            "shape must NOT stream but did: {sql}"
        );
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
