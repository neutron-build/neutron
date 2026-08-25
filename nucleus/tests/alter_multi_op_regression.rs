//! CAT-1: a single ALTER TABLE carrying multiple operations corrupted
//! catalog and storage. `table_def` was fetched once before the operations
//! loop; every arm cloned that PRE-STATEMENT def and `update_table` replaces
//! the whole TableDef — so op2 silently reverted op1's catalog change while
//! op1's physical rewrite had already happened under op1's shape. A dropped
//! column resurrected under a surviving name, or rows came back shifted
//! (values read under the wrong column names). The corruption is
//! engine-independent; these tests assert content identity across the
//! operation and across a checkpoint+reopen on the served disk stack.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::Value;

async fn fresh() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(nucleus::storage::ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}

async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().unwrap()
}

async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

/// Boot a server-shaped stack (segmented-WAL DiskEngine wrapped in
/// BufferedDiskEngine, executor with catalog persistence), mirroring main.rs.
async fn boot(data: &Path) -> (Arc<Executor>, Arc<BufferedDiskEngine>) {
    std::fs::create_dir_all(data).unwrap();
    let catalog = Arc::new(Catalog::new());
    let catalog_path = data.join("catalog.json");
    let _ = CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;
    let db_path = data.join("nucleus.db");
    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(&db_path, catalog.clone(), 1024, 16, SyncMode::Fsync)
            .unwrap(),
    );
    for table in catalog.table_names().await {
        let _ = engine.create_table(&table).await;
    }
    let buffered = Arc::new(BufferedDiskEngine::new(engine));
    let exec = Arc::new(Executor::new_with_persistence(
        catalog,
        buffered.clone() as Arc<dyn StorageEngine>,
        Some(catalog_path),
        Some(data),
    ));
    exec.restore_table_engines().await;
    (exec, buffered)
}

// ── (a) DROP a, DROP b → exactly the remaining column, values unshifted ────

#[tokio::test]
async fn two_drops_leave_only_remaining_column_with_its_own_data() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (id INT, a TEXT, b TEXT)").await;
    exec(
        &ex,
        "INSERT INTO t VALUES (1, 'va_one', 'vb_one'), (2, 'va_two', 'vb_two')",
    )
    .await;

    exec(&ex, "ALTER TABLE t DROP COLUMN a, DROP COLUMN b").await;

    let rows = select_rows(&ex, "SELECT * FROM t ORDER BY id").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(1)], vec![Value::Int64(2)],],
        "both drops must apply: one column left, holding id values — \
         pre-fix op2 reverted op1's catalog change and b's data survived \
         under a's name"
    );
}

// ── (b) ADD c1 DEFAULT 7, ADD c2 DEFAULT 9 → each column holds its own ─────

#[tokio::test]
async fn two_adds_backfill_each_columns_own_default() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;

    exec(
        &ex,
        "ALTER TABLE t ADD COLUMN c1 INT DEFAULT 7, ADD COLUMN c2 INT DEFAULT 9",
    )
    .await;

    let rows = select_rows(&ex, "SELECT c1, c2 FROM t").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(7), Value::Int64(9)]],
        "c1 must hold 7 and c2 must hold 9 — pre-fix op2 reverted op1's \
         catalog change and c2 read slot 1 (= 7)"
    );
}

// ── (c) ADD x, ADD y (no defaults) → both NULL, 3 catalog columns ──────────

#[tokio::test]
async fn two_adds_without_defaults_backfill_nulls() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;

    exec(&ex, "ALTER TABLE t ADD COLUMN x INT, ADD COLUMN y INT").await;

    let rows = select_rows(&ex, "SELECT * FROM t").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(1), Value::Null, Value::Null]],
        "both new columns must exist and read NULL"
    );
    let count = select_rows(&ex, "SELECT COUNT(*) FROM t").await;
    assert_eq!(count[0][0], Value::Int64(1));
}

// ── (d) ADD x, DROP x in one statement round-trips the shape ───────────────

#[tokio::test]
async fn add_then_drop_in_one_statement_round_trips() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (id INT, v TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'keep')").await;

    exec(&ex, "ALTER TABLE t ADD COLUMN x INT, DROP COLUMN x").await;

    let rows = select_rows(&ex, "SELECT * FROM t").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(1), Value::Text("keep".into())]],
        "ADD then DROP of the same column must round-trip the shape and data"
    );
}

// ── (e) RENAME TO then ADD in one statement addresses the renamed table ────

#[tokio::test]
async fn rename_then_add_addresses_renamed_table() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;

    exec(
        &ex,
        "ALTER TABLE t RENAME TO t2, ADD COLUMN c INT DEFAULT 5",
    )
    .await;

    let rows = select_rows(&ex, "SELECT * FROM t2").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(1), Value::Int64(5)]],
        "the op after RENAME TO must apply to the renamed table"
    );
    let old = ex.execute("SELECT * FROM t").await;
    assert!(old.is_err(), "the old name must be gone after the rename");
}

// ── Disk stack: content identity must survive checkpoint + reopen ──────────

#[tokio::test]
async fn multi_op_alter_content_identity_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE mo_disk (id BIGINT, a TEXT, b TEXT)").await;
        exec(
            &ex,
            "INSERT INTO mo_disk VALUES (1, 'va_one', 'vb_one'), (2, 'va_two', 'vb_two')",
        )
        .await;
        exec(&ex, "ALTER TABLE mo_disk DROP COLUMN a, DROP COLUMN b").await;
        let rows = select_rows(&ex, "SELECT * FROM mo_disk ORDER BY id").await;
        assert_eq!(
            rows,
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
            "pre-reopen: both drops applied, one column left"
        );
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT * FROM mo_disk ORDER BY id").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(1)], vec![Value::Int64(2)]],
        "post-reopen: dropped columns must stay dropped with no data shift"
    );
}

#[tokio::test]
async fn multi_op_adds_content_identity_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE mo_add (id BIGINT)").await;
        exec(&ex, "INSERT INTO mo_add VALUES (41)").await;
        exec(
            &ex,
            "ALTER TABLE mo_add ADD COLUMN c1 INT DEFAULT 7, ADD COLUMN c2 INT DEFAULT 9",
        )
        .await;
        let rows = select_rows(&ex, "SELECT c1, c2 FROM mo_add").await;
        assert_eq!(rows, vec![vec![Value::Int64(7), Value::Int64(9)]]);
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT id, c1, c2 FROM mo_add").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(41), Value::Int64(7), Value::Int64(9)]],
        "post-reopen: both added columns present with their own defaults"
    );
}
