//! `CREATE UNIQUE INDEX` on a table that ALREADY holds rows must enforce
//! uniqueness — at creation time, on later writes, and across a checkpoint +
//! reopen.
//!
//! `IndexDef.unique` was stored in the catalog (and round-tripped through
//! catalog.json) but nothing consulted it on the write path:
//! `check_unique_constraints` enumerated only `TableConstraint::PrimaryKey`
//! and `TableConstraint::Unique`, so a UNIQUE *index* — as opposed to a UNIQUE
//! *constraint* — admitted duplicate keys forever. `execute_create_index`
//! likewise registered the index without validating the rows already in the
//! table, so an existing duplicate was blessed silently. And on reopen,
//! although the flag itself survives in catalog.json, nothing re-asserted it,
//! so a reopened process silently depended on whichever catalogue happened to
//! be loaded.
//!
//! The shape exercised here is the one from the wild: a populated DISK table,
//! `ALTER TABLE ADD COLUMN` with a default backfilling every existing row,
//! then `CREATE UNIQUE INDEX` on the new column.
//!
//! Modelled on `alter_multi_op_regression.rs` / `durable_recovery_constraints.rs`
//! — same served disk stack (segmented-WAL DiskEngine wrapped in
//! BufferedDiskEngine, executor with catalog persistence), the shape `main.rs`
//! boots.

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
/// BufferedDiskEngine, executor with catalog persistence), mirroring main.rs —
/// same helper as `alter_multi_op_regression.rs`.
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
    // Mirror main.rs startup: the ordinary B-tree indexes live only in the
    // engine's in-memory registry, so a reopened directory rebuilds them.
    // This is the path that has to keep the UNIQUE flag honest.
    exec.rebuild_persistent_indexes().await;
    (exec, buffered)
}

/// The error must be a unique violation, not some unrelated failure (e.g. a
/// storage error the check happened to fall over).
fn assert_unique_violation(
    result: &Result<Vec<ExecResult>, nucleus::executor::ExecError>,
    ctx: &str,
) {
    let Err(e) = result else {
        panic!("{ctx}: expected an error, got Ok");
    };
    let msg = e.to_string();
    assert!(
        msg.contains("duplicate key value violates unique constraint"),
        "{ctx}: expected a unique-violation error, got: {msg}"
    );
}

// ── populated disk table, ADD COLUMN with default, CREATE UNIQUE INDEX ─────

/// Backfill every existing row with the SAME default, then declare the new
/// column unique: the rows already in the table are duplicates, so the build
/// must be refused — and the refusal must leave no trace.
#[tokio::test]
async fn unique_index_on_default_backfilled_column_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buffered) = boot(&data).await;

    exec(&ex, "CREATE TABLE ui (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO ui VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
    // Every existing row now holds tag = 7.
    exec(&ex, "ALTER TABLE ui ADD COLUMN tag INT DEFAULT 7").await;

    let refused = ex.execute("CREATE UNIQUE INDEX ui_tag ON ui (tag)").await;
    let Err(e) = &refused else {
        panic!("CREATE UNIQUE INDEX over existing duplicates must be refused");
    };
    let msg = e.to_string();
    assert!(
        msg.contains("duplicate"),
        "refusal must name the duplicate, got: {msg}"
    );

    // A refused build must not have registered the index: the table stays
    // writable under the values it already holds.
    let rows = select_rows(
        &ex,
        "SELECT indexname FROM pg_indexes WHERE indexname = 'ui_tag'",
    )
    .await;
    assert!(
        rows.is_empty(),
        "refused index must not be registered: {rows:?}"
    );
    exec(&ex, "INSERT INTO ui VALUES (4, 'd', 7)").await;
    let count = select_rows(&ex, "SELECT COUNT(*) FROM ui").await;
    assert_eq!(count, vec![vec![Value::Int64(4)]]);
}

/// De-duplicated rows: the build succeeds, and the duplicate insert is refused
/// with a unique-violation error from then on.
#[tokio::test]
async fn unique_index_enforced_after_creation_on_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buffered) = boot(&data).await;

    exec(&ex, "CREATE TABLE ur (id INT PRIMARY KEY, v TEXT)").await;
    exec(&ex, "INSERT INTO ur VALUES (1, 'one'), (2, 'two')").await;
    exec(&ex, "ALTER TABLE ur ADD COLUMN code INT DEFAULT 5").await;
    exec(&ex, "UPDATE ur SET code = id").await;
    exec(&ex, "CREATE UNIQUE INDEX ur_code ON ur (code)").await;

    let dup = ex.execute("INSERT INTO ur VALUES (3, 'three', 1)").await;
    assert_unique_violation(&dup, "duplicate insert after CREATE UNIQUE INDEX");

    // The refused row must not have landed; a non-conflicting one must.
    exec(&ex, "INSERT INTO ur VALUES (3, 'three', 30)").await;
    let rows = select_rows(&ex, "SELECT id, code FROM ur ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1), Value::Int64(1)],
            vec![Value::Int64(2), Value::Int64(2)],
            vec![Value::Int64(3), Value::Int64(30)],
        ],
        "the refused insert must not have landed and the accepted one must have"
    );
}

// ── checkpoint + reopen: content and refusal both survive ──────────────────

#[tokio::test]
async fn unique_index_survives_checkpoint_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE ck (id INT PRIMARY KEY, v TEXT)").await;
        exec(&ex, "INSERT INTO ck VALUES (1, 'one'), (2, 'two')").await;
        exec(&ex, "ALTER TABLE ck ADD COLUMN code INT DEFAULT 5").await;
        exec(&ex, "UPDATE ck SET code = id").await;
        exec(&ex, "CREATE UNIQUE INDEX ck_code ON ck (code)").await;
        let dup = ex.execute("INSERT INTO ck VALUES (3, 'three', 1)").await;
        assert_unique_violation(&dup, "duplicate insert before checkpoint");
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT id, code FROM ck ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1), Value::Int64(1)],
            vec![Value::Int64(2), Value::Int64(2)],
        ],
        "content must survive checkpoint + reopen"
    );
    let dup = ex2.execute("INSERT INTO ck VALUES (3, 'three', 1)").await;
    assert_unique_violation(&dup, "duplicate insert after reopen (unique flag lost?)");
    exec(&ex2, "INSERT INTO ck VALUES (3, 'three', 3)").await;
    let count = select_rows(&ex2, "SELECT COUNT(*) FROM ck").await;
    assert_eq!(count, vec![vec![Value::Int64(3)]]);
}

/// A UNIQUE index refused before the checkpoint must still be absent — and
/// still refused — after a reopen.
#[tokio::test]
async fn refused_unique_index_stays_refused_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE rf (id INT PRIMARY KEY)").await;
        exec(&ex, "INSERT INTO rf VALUES (1), (2)").await;
        exec(&ex, "ALTER TABLE rf ADD COLUMN code INT DEFAULT 9").await;
        let refused = ex.execute("CREATE UNIQUE INDEX rf_code ON rf (code)").await;
        assert!(refused.is_err(), "index over duplicates must be refused");
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(
        &ex2,
        "SELECT indexname FROM pg_indexes WHERE indexname = 'rf_code'",
    )
    .await;
    assert!(
        rows.is_empty(),
        "the refused index must not appear after reopen: {rows:?}"
    );
    // Without the index, duplicates are legal again.
    exec(&ex2, "INSERT INTO rf VALUES (3, 9)").await;
    let count = select_rows(&ex2, "SELECT COUNT(*) FROM rf").await;
    assert_eq!(count, vec![vec![Value::Int64(3)]]);
}

// ── in-memory executor: same enforcement without the disk stack ────────────

/// The write path is engine-independent; assert it on the in-memory executor
/// too, where `rebuild_persistent_indexes` never runs.
#[tokio::test]
async fn unique_index_refuses_duplicate_insert_on_memory_executor() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(nucleus::storage::ColumnarStorageEngine::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    exec(&ex, "CREATE TABLE mem (id INT, email TEXT)").await;
    exec(&ex, "INSERT INTO mem VALUES (1, 'a@x'), (2, 'b@x')").await;
    exec(&ex, "CREATE UNIQUE INDEX mem_email ON mem (email)").await;

    let dup = ex.execute("INSERT INTO mem VALUES (3, 'a@x')").await;
    assert_unique_violation(&dup, "duplicate insert under unique index");
    exec(&ex, "INSERT INTO mem VALUES (3, 'c@x')").await;
    let count = select_rows(&ex, "SELECT COUNT(*) FROM mem").await;
    assert_eq!(count, vec![vec![Value::Int64(3)]]);
}
