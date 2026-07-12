//! Regression for `window_sum_null`: a window `SUM() OVER (...)` over a frame
//! whose argument values are all NULL must return NULL (SQL-standard,
//! NULL-ignoring SUM), not 0. Once a non-NULL value enters the frame, the
//! window SUM returns the sum of the non-NULL values (NULLs are skipped).
//! This mirrors Nucleus's non-window SUM and the default Mvcc engine.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::*;
use nucleus::types::Value;
use std::sync::Arc;

fn ex(st: Arc<dyn StorageEngine>) -> Executor {
    Executor::new(Arc::new(Catalog::new()), st)
}
async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Null => None,
        Value::Int32(n) => Some(*n as f64),
        Value::Int64(n) => Some(*n as f64),
        Value::Float64(n) => Some(*n),
        other => panic!("unexpected window SUM value: {other:?}"),
    }
}

/// Frame contains only NULL argument values -> window SUM must be NULL.
#[tokio::test]
async fn window_sum_all_null_frame_is_null() {
    let e = ex(Arc::new(MvccStorageAdapter::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .await
        .unwrap();
    // Every row has a NULL value column.
    e.execute("INSERT INTO t (id, v) VALUES (1, NULL), (2, NULL), (3, NULL)")
        .await
        .unwrap();
    let r = rows(
        &e,
        "SELECT id, SUM(v) OVER (ORDER BY id ASC) AS s FROM t ORDER BY id ASC",
    )
    .await;
    assert_eq!(r.len(), 3);
    for row in &r {
        assert_eq!(
            row[1],
            Value::Null,
            "all-NULL frame must yield NULL, got {:?}",
            row[1]
        );
    }
}

/// Mixed frame: running window SUM skips NULLs and is NULL only while the
/// running frame has seen no non-NULL value.
#[tokio::test]
async fn window_sum_mixed_frame_skips_nulls() {
    let e = ex(Arc::new(MvccStorageAdapter::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .await
        .unwrap();
    // Running frame "rows so far" (default RANGE UNBOUNDED PRECEDING):
    //   id=1: {NULL}             -> NULL
    //   id=2: {NULL, 10}         -> 10
    //   id=3: {NULL, 10, NULL}   -> 10
    //   id=4: {NULL, 10, NULL,5} -> 15
    e.execute("INSERT INTO t (id, v) VALUES (1, NULL), (2, 10), (3, NULL), (4, 5)")
        .await
        .unwrap();
    let r = rows(
        &e,
        "SELECT id, SUM(v) OVER (ORDER BY id ASC) AS s FROM t ORDER BY id ASC",
    )
    .await;
    let got: Vec<Option<f64>> = r.iter().map(|row| as_f64(&row[1])).collect();
    assert_eq!(got, vec![None, Some(10.0), Some(10.0), Some(15.0)]);
}
