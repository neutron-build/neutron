//! Regression: the wire-level SQL OLTP fast path (PointUpdate / PointDelete /
//! SimpleInsert) must invalidate the query result cache like the parsed DML
//! path does. Pre-fix it didn't, so a cached SELECT kept serving pre-write
//! rows for up to the cache TTL (30s) after a point-write from another
//! connection — the visibility-staleness family from the teploy-observe
//! dogfood findings (#2 / #27 / #30).

#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{ColumnarStorageEngine, StorageEngine};
use nucleus::types::Value;
use nucleus::wire::kv_fast_path::{SqlFastPathCommand, SqlLiteral};

async fn fresh() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}
async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().expect("a result")
}
async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

#[tokio::test]
async fn point_update_invalidates_cached_select() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE fp_upd (id BIGINT, status TEXT)").await;
    exec(&ex, "INSERT INTO fp_upd VALUES (1, 'active')").await;

    // Prime the query result cache.
    let r = rows(&ex, "SELECT status FROM fp_upd WHERE id = 1").await;
    assert_eq!(r[0][0], Value::Text("active".into()));

    // Point-update via the wire fast path (bypasses the parsed DML path).
    let cmd = SqlFastPathCommand::PointUpdate {
        table: "fp_upd".into(),
        assignments: vec![("status".into(), SqlLiteral::Text("closed".into()))],
        where_col: "id".into(),
        where_val: SqlLiteral::Integer(1),
    };
    ex.execute_sql_fast_path(0, &cmd)
        .await
        .expect("fast path should handle this shape")
        .expect("update should succeed");

    // The same SELECT must observe the new value immediately, not the cache.
    let r = rows(&ex, "SELECT status FROM fp_upd WHERE id = 1").await;
    assert_eq!(
        r[0][0],
        Value::Text("closed".into()),
        "cached SELECT served a stale pre-update row"
    );
}

#[tokio::test]
async fn point_delete_invalidates_cached_select() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE fp_del (id BIGINT, status TEXT)").await;
    exec(&ex, "INSERT INTO fp_del VALUES (1, 'x'), (2, 'y')").await;

    let r = rows(&ex, "SELECT COUNT(*) FROM fp_del").await;
    assert!(matches!(&r[0][0], Value::Int64(2) | Value::Int32(2)));

    let cmd = SqlFastPathCommand::PointDelete {
        table: "fp_del".into(),
        where_col: "id".into(),
        where_val: SqlLiteral::Integer(1),
    };
    ex.execute_sql_fast_path(0, &cmd)
        .await
        .expect("fast path should handle this shape")
        .expect("delete should succeed");

    let r = rows(&ex, "SELECT COUNT(*) FROM fp_del").await;
    assert!(
        matches!(&r[0][0], Value::Int64(1) | Value::Int32(1)),
        "cached SELECT served a stale pre-delete count: {:?}",
        r[0][0]
    );
}

#[tokio::test]
async fn simple_insert_invalidates_cached_select() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE fp_ins (id BIGINT, status TEXT)").await;
    exec(&ex, "INSERT INTO fp_ins VALUES (1, 'a')").await;

    let r = rows(&ex, "SELECT COUNT(*) FROM fp_ins").await;
    assert!(matches!(&r[0][0], Value::Int64(1) | Value::Int32(1)));

    let cmd = SqlFastPathCommand::SimpleInsert {
        table: "fp_ins".into(),
        values: vec![SqlLiteral::Integer(2), SqlLiteral::Text("b".into())],
    };
    ex.execute_sql_fast_path(0, &cmd)
        .await
        .expect("fast path should handle this shape")
        .expect("insert should succeed");

    let r = rows(&ex, "SELECT COUNT(*) FROM fp_ins").await;
    assert!(
        matches!(&r[0][0], Value::Int64(2) | Value::Int32(2)),
        "cached SELECT served a stale pre-insert count: {:?}",
        r[0][0]
    );
}

// ── CAT-12: SimpleInsert coerced each literal with
// `.cast(...).unwrap_or_else(|_| v.to_value())` — a text literal that could
// not be cast into the typed column was stored VERBATIM, so `INSERT INTO t
// VALUES ('abc')` into an INT column durably held Text('abc') behind a
// successful INSERT tag. ─────────────────────────────────────────────────────

#[tokio::test]
async fn simple_insert_uncastable_literal_is_an_error() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE fp_cast (id BIGINT, v TEXT)").await;

    let cmd = SqlFastPathCommand::SimpleInsert {
        table: "fp_cast".into(),
        values: vec![SqlLiteral::Text("abc".into()), SqlLiteral::Text("x".into())],
    };
    let err = ex
        .execute_sql_fast_path(0, &cmd)
        .await
        .expect("fast path should handle this shape")
        .expect_err("an uncastable literal must be an error, not a silent Text store");
    assert!(
        err.to_string().contains("invalid input syntax"),
        "got: {err}"
    );

    let r = rows(&ex, "SELECT COUNT(*) FROM fp_cast").await;
    assert!(
        matches!(&r[0][0], Value::Int64(0) | Value::Int32(0)),
        "the refused insert must leave zero rows: {:?}",
        r[0][0]
    );
}

#[tokio::test]
async fn simple_insert_castable_text_and_null_land_typed() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE fp_cast2 (id BIGINT, v TEXT)").await;

    let cmd = SqlFastPathCommand::SimpleInsert {
        table: "fp_cast2".into(),
        values: vec![SqlLiteral::Text("5".into()), SqlLiteral::Text("x".into())],
    };
    ex.execute_sql_fast_path(0, &cmd)
        .await
        .expect("fast path should handle this shape")
        .expect("insert should succeed");
    // '5' must land as an integer, not Text("5").
    let r = rows(&ex, "SELECT id + 1 FROM fp_cast2").await;
    assert!(matches!(&r[0][0], Value::Int64(6) | Value::Int32(6)));

    let cmd = SqlFastPathCommand::SimpleInsert {
        table: "fp_cast2".into(),
        values: vec![SqlLiteral::Null, SqlLiteral::Text("y".into())],
    };
    ex.execute_sql_fast_path(0, &cmd)
        .await
        .expect("fast path should handle this shape")
        .expect("insert should succeed");
    let r = rows(&ex, "SELECT COUNT(*) FROM fp_cast2 WHERE id IS NULL").await;
    assert!(matches!(&r[0][0], Value::Int64(1) | Value::Int32(1)));
}
