//! Integration tests for `replacing_mergetree` read-time dedup (#10/#20/#27).
//!
//! These tests pin the contract that `SELECT *` and `SELECT COUNT(*)` on a
//! `replacing_mergetree` table return the deduped logical row set, while
//! `UPDATE`/`DELETE` continue to act on every physical version of a row.
//!
//! The intent is that future regressions in the dedup pipeline (column ↔ row
//! conversion, position bookkeeping, fast-path overrides) get caught here
//! rather than only in downstream services like Observe.
//!
//! Tests are gated on the `server` feature because they exercise the
//! pgwire-style code path through `Executor` + `ColumnarStorageEngine`.

#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{ColumnarStorageEngine, StorageEngine};
use nucleus::types::Value;

/// Spin up a fresh executor backed by a fresh in-memory catalog and the
/// columnar storage engine (the engine that hosts `replacing_mergetree`
/// tables).
async fn fresh_executor() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}

async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    let mut results = ex.execute(sql).await.expect(sql);
    results
        .pop()
        .expect("at least one statement result")
}

async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

#[tokio::test]
async fn replacing_mergetree_collapses_duplicate_pks_on_select() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE issues (issue_id TEXT, group_hash TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (issue_id)",
    )
    .await;
    // Insert 5 versions of the same logical row, version increasing.
    exec(
        &ex,
        "INSERT INTO issues (issue_id, group_hash, version) VALUES \
         ('abc', 'g1', 1), ('abc', 'g2', 2), ('abc', 'g3', 3), \
         ('abc', 'g4', 4), ('abc', 'g5', 5)",
    )
    .await;

    let rows = select_rows(&ex, "SELECT issue_id, group_hash, version FROM issues WHERE issue_id='abc'").await;
    assert_eq!(rows.len(), 1, "dedup should collapse all 5 versions");
    assert_eq!(rows[0][0], Value::Text("abc".into()));
    assert_eq!(rows[0][1], Value::Text("g5".into()), "highest version wins");
}

#[tokio::test]
async fn replacing_mergetree_count_matches_select_after_dedup() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE m (id INT, name TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    // Two distinct PKs, each with two versions = 4 physical rows, 2 logical.
    exec(
        &ex,
        "INSERT INTO m (id, name, version) VALUES \
         (1, 'a', 1), (1, 'a2', 2), (2, 'b', 1), (2, 'b2', 2)",
    )
    .await;

    let select_rows = select_rows(&ex, "SELECT id FROM m").await;
    let count_rows = match exec(&ex, "SELECT COUNT(*) FROM m").await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    };
    let count = match &count_rows[0][0] {
        Value::Int64(n) => *n as usize,
        Value::Int32(n) => *n as usize,
        other => panic!("unexpected count value {other:?}"),
    };
    assert_eq!(
        select_rows.len(),
        count,
        "COUNT(*) and SELECT id row count must agree on dedup"
    );
    assert_eq!(select_rows.len(), 2);
}

#[tokio::test]
async fn replacing_mergetree_dedup_prefers_highest_version_not_last_insert() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE rev (k TEXT, v TEXT, ver INT) \
         WITH (engine='replacing_mergetree', version_column='ver') \
         ORDER BY (k)",
    )
    .await;
    // Insert versions out of order: highest version (10) is the second insert,
    // not the last. Dedup must pick version=10, not the chronologically last.
    exec(
        &ex,
        "INSERT INTO rev (k, v, ver) VALUES ('x', 'first', 1), ('x', 'winner', 10), ('x', 'late', 2)",
    )
    .await;
    let rows = select_rows(&ex, "SELECT k, v, ver FROM rev WHERE k='x'").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("winner".into()));
}

#[tokio::test]
async fn replacing_mergetree_delete_via_pk_removes_all_physical_versions() {
    // DELETE on the PK column must remove every physical version of the
    // matching logical row. Otherwise older versions resurrect on the next
    // SELECT (since dedup keeps the highest remaining version).
    //
    // The DELETE goes through the executor's PK-eq fast path
    // (`extract_pk_eq_value` → `scan_where_eq_positions`), which is now
    // overridden on `ColumnarStorageEngine` to return all *physical*
    // matching rows + their physical positions.
    //
    // The PRIMARY KEY declaration (rather than just ORDER BY) is required to
    // engage that fast path; this matches Observe's schema for the affected
    // tables.
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE d (id TEXT PRIMARY KEY, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    // INSERT 4 physical rows: 1 'keep' + 3 versions of 'rm'.
    exec(&ex, "INSERT INTO d (id, version) VALUES ('keep', 1)").await;
    exec(&ex, "INSERT INTO d (id, version) VALUES ('rm', 1)").await;
    exec(&ex, "INSERT INTO d (id, version) VALUES ('rm', 2)").await;
    exec(&ex, "INSERT INTO d (id, version) VALUES ('rm', 3)").await;

    // Sanity: dedup gives 2 logical rows (keep, rm).
    let pre = select_rows(&ex, "SELECT id FROM d").await;
    assert_eq!(pre.len(), 2);

    // DELETE WHERE id='rm' must wipe every physical version of 'rm'.
    let res = exec(&ex, "DELETE FROM d WHERE id='rm'").await;
    if let ExecResult::Command { rows_affected, .. } = res {
        assert_eq!(rows_affected, 3, "DELETE must remove all 3 physical versions of 'rm'");
    } else {
        panic!("expected Command result for DELETE");
    }

    let after = select_rows(&ex, "SELECT id FROM d WHERE id='rm'").await;
    assert!(after.is_empty(), "no version of 'rm' should resurrect after DELETE");

    let remaining = select_rows(&ex, "SELECT id FROM d").await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0][0], Value::Text("keep".into()));
}

#[tokio::test]
async fn replacing_mergetree_no_version_column_keeps_last_insert() {
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE nv (k TEXT, v TEXT) \
         WITH (engine='replacing_mergetree') \
         ORDER BY (k)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO nv (k, v) VALUES ('a', '1'), ('a', '2'), ('a', '3')",
    )
    .await;
    let rows = select_rows(&ex, "SELECT k, v FROM nv WHERE k='a'").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("3".into()), "no version_column → last insert wins");
}

#[tokio::test]
async fn replacing_mergetree_cross_connection_visibility() {
    // Reproduces the spirit of #27 — the bug as reported is in a Go connection
    // pool, not in Nucleus, but we still want to assert that committed writes
    // are visible to all subsequent SELECTs on the same Executor.
    let ex = fresh_executor().await;
    exec(
        &ex,
        "CREATE TABLE c (id TEXT, version INT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO c (id, version) VALUES ('y', 1)").await;
    let pre = select_rows(&ex, "SELECT id FROM c WHERE id='y'").await;
    assert_eq!(pre.len(), 1);

    exec(&ex, "DELETE FROM c WHERE id='y'").await;
    let after = select_rows(&ex, "SELECT id FROM c WHERE id='y'").await;
    assert!(after.is_empty(), "DELETE must be immediately visible to next SELECT");
}

#[tokio::test]
async fn plain_table_count_unchanged_by_replacing_changes() {
    // Regression guard: plain (non-replacing) tables must not get dedup'd.
    let ex = fresh_executor().await;
    exec(&ex, "CREATE TABLE p (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO p (id, name) VALUES (1, 'a'), (1, 'b'), (1, 'c')").await;
    let rows = select_rows(&ex, "SELECT id, name FROM p").await;
    assert_eq!(rows.len(), 3, "plain table must NOT be deduped");
}
