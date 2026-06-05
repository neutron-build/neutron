//! ALTER TABLE ... ALTER COLUMN ... TYPE must rewrite stored values so the
//! physical representation matches the new declared type (teploy-observe D-7).
//! Otherwise a columnar/MergeTree table reconstructs values from the old
//! physical ColumnData and the catalog/storage types silently diverge.

#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{ColumnarStorageEngine, StorageEngine};
use nucleus::types::Value;

async fn fresh() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}
async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().unwrap()
}
async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn alter_text_to_bigint_rewrites_stored_values() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE t (id INT, v TEXT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO t (id, v) VALUES (1, '10'), (2, '20')").await;

    exec(&ex, "ALTER TABLE t ALTER COLUMN v TYPE BIGINT").await;

    // Values must read back as integers, and numeric aggregates must work.
    let rows = select_rows(&ex, "SELECT v FROM t").await;
    let mut got: Vec<Value> = rows.into_iter().map(|r| r[0].clone()).collect();
    got.sort_by_key(|v| match v {
        Value::Int64(n) => *n,
        _ => panic!("expected Int64 after ALTER, got {v:?}"),
    });
    assert_eq!(got, vec![Value::Int64(10), Value::Int64(20)]);

    let sum = select_rows(&ex, "SELECT SUM(v) FROM t").await;
    assert_eq!(sum[0][0], Value::Int64(30));
}

#[tokio::test]
async fn alter_to_incompatible_type_is_rejected() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE t (id INT, v TEXT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'not_a_number')").await;

    // A value that cannot be cast must abort the ALTER with an error rather than
    // silently diverging the catalog from storage.
    let res = ex.execute("ALTER TABLE t ALTER COLUMN v TYPE BIGINT").await;
    assert!(res.is_err(), "ALTER over an uncastable value must error");

    // The column must still read back as its original TEXT value (unchanged).
    let rows = select_rows(&ex, "SELECT v FROM t").await;
    assert_eq!(rows[0][0], Value::Text("not_a_number".into()));
}
