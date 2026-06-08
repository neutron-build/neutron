//! Regression: UPDATE/DELETE by a single-column PK must coerce the WHERE literal
//! to the stored value's type — like the slow path (`compare_values`) and
//! PostgreSQL — instead of using strict `Value` equality.
//!
//! A pgwire simple-protocol client (pgx `QueryExecModeSimpleProtocol`) binds
//! params as text, so a `BIGINT` PK populated via a text param is stored as
//! `Int64`, while a later `UPDATE ... WHERE id = 5` re-parses the literal as
//! `Int32`. The PK fast path (`scan_where_eq_positions`) compared with strict
//! `Value` `PartialEq`, so `Int32(5) != Int64(5)` matched zero rows and the
//! UPDATE silently no-oped. For `api_keys.revoked` that means revoked keys keep
//! authenticating (teploy-observe audit finding #3 — a real security hole).
//!
//! Pins both the UPDATE and DELETE fast paths, and the text-literal flavor.

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
    ex.execute(sql).await.expect(sql).pop().expect("a statement result")
}

async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

/// Numeric-literal UPDATE against a text-bound (Int64-stored) BIGINT PK.
#[tokio::test]
async fn update_by_bigint_pk_matches_text_bound_value() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE api_keys (id BIGINT PRIMARY KEY, revoked TEXT)").await;
    // Simple-protocol clients bind params as text → stored as Int64 via INSERT coercion.
    exec(&ex, "INSERT INTO api_keys (id, revoked) VALUES ('5', 'false')").await;
    // Literal re-parses to Int32; must still match the Int64-stored PK.
    exec(&ex, "UPDATE api_keys SET revoked = 'true' WHERE id = 5").await;

    let rows = select_rows(&ex, "SELECT revoked FROM api_keys").await;
    assert_eq!(rows.len(), 1, "row vanished");
    assert_eq!(
        rows[0][0],
        Value::Text("true".into()),
        "revocation UPDATE silently no-oped — revoked key still authenticates"
    );
}

/// Text-literal UPDATE (pgx simple protocol interpolates `$1` as `'5'`).
#[tokio::test]
async fn update_by_bigint_pk_matches_text_literal_predicate() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE api_keys (id BIGINT PRIMARY KEY, revoked TEXT)").await;
    exec(&ex, "INSERT INTO api_keys (id, revoked) VALUES ('5', 'false')").await;
    // pgx simple protocol: WHERE id = $1 arrives as WHERE id = '5'.
    exec(&ex, "UPDATE api_keys SET revoked = 'true' WHERE id = '5'").await;

    let rows = select_rows(&ex, "SELECT revoked FROM api_keys").await;
    assert_eq!(rows[0][0], Value::Text("true".into()), "text-literal UPDATE no-oped");
}

/// DELETE must coerce too (retention / hard revoke).
#[tokio::test]
async fn delete_by_bigint_pk_matches_text_bound_value() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE api_keys (id BIGINT PRIMARY KEY, revoked TEXT)").await;
    exec(&ex, "INSERT INTO api_keys (id, revoked) VALUES ('7', 'false')").await;
    exec(&ex, "DELETE FROM api_keys WHERE id = 7").await;

    let rows = select_rows(&ex, "SELECT revoked FROM api_keys").await;
    assert_eq!(rows.len(), 0, "DELETE by numeric literal missed the Int64-stored PK");
}
