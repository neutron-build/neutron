//! Regression: `CREATE UNIQUE INDEX` on a table that already holds rows must
//! actually enforce uniqueness — at creation time, on later writes, and across
//! a checkpoint + reopen.
//!
//! `IndexDef.unique` was stored in the catalog (and round-tripped through
//! catalog.json) but nothing consulted it on the write path:
//! `check_unique_constraints` enumerated only `TableConstraint::PrimaryKey`
//! and `TableConstraint::Unique`, so a UNIQUE *index* — as opposed to a UNIQUE
//! *constraint* — admitted duplicate keys forever. `execute_create_index`
//! likewise registered the index without validating the rows already in the
//! table, so an existing duplicate was accepted silently. And although
//! `IndexDef.unique` survives in catalog.json, nothing restored the unique
//! flag into the executor's index registry on reopen.
//!
//! These tests use the served disk stack (segmented-WAL DiskEngine wrapped in
//! BufferedDiskEngine, executor with catalog persistence), the same shape
//! `main.rs` boots.

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

/// Boot a server-shaped stack, mirroring main.rs.
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

async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().unwrap()
}

async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

/// ALTER TABLE ADD COLUMN with a default, then CREATE UNIQUE INDEX on that
/// column: existing rows all carry the same default, so the index build must
/// be refused — unless the rows were de-duplicated first.
#[tokio::test]
async fn unique_index_on_default_backfilled_column() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buffered) = boot(&data).await;
    exec(
        &ex,
        "CREATE TABLE ui (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO ui VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec(&ex, "ALTER TABLE ui ADD COLUMN tag INT DEFAULT 7").await;

    // Rows now hold (1,'a',7), (2,'b',7), (3,'c',7) — a UNIQUE index on `tag`
    // cannot be built over those duplicates.
    let dup_build = ex.execute("CREATE UNIQUE INDEX ui_tag ON ui (tag)").await;
    assert!(
        dup_build.is_err(),
        "CREATE UNIQUE INDEX over existing duplicates must be refused, got {dup_build:?}"
    );

    // De-duplicate, then the build must succeed and be enforced afterwards.
    exec(&ex, "UPDATE ui SET tag = id").await;
    exec(&ex, "CREATE UNIQUE INDEX ui_tag ON ui (tag)").await;
    let dup = ex.execute("INSERT INTO ui VALUES (4, 'd', 4)").await;
    assert!(
        dup.is_err(),
        "duplicate key must be refused after CREATE UNIQUE INDEX, got {dup:?}"
    );
    exec(&ex, "INSERT INTO ui VALUES (4, 'd', 40)").await;
    let rows = select_rows(&ex, "SELECT tag FROM ui ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)],
            vec![Value::Int64(40)]
        ],
        "the refused insert must not have landed and the accepted one must have"
    );
}

/// Refusal and content must both survive a checkpoint + reopen.
#[tokio::test]
async fn unique_index_enforcement_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(
            &ex,
            "CREATE TABLE ur (id INT PRIMARY KEY, v TEXT)",
        )
        .await;
        exec(&ex, "INSERT INTO ur VALUES (1, 'one'), (2, 'two')").await;
        exec(&ex, "ALTER TABLE ur ADD COLUMN code INT DEFAULT 5").await;
        exec(&ex, "UPDATE ur SET code = id").await;
        exec(&ex, "CREATE UNIQUE INDEX ur_code ON ur (code)").await;
        let dup = ex.execute("INSERT INTO ur VALUES (3, 'three', 1)").await;
        assert!(dup.is_err(), "duplicate must be refused before reopen");
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT id, code FROM ur ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1), Value::Int64(1)],
            vec![Value::Int64(2), Value::Int64(2)]
        ],
        "content must survive checkpoint + reopen"
    );
    let dup = ex2.execute("INSERT INTO ur VALUES (3, 'three', 1)").await;
    assert!(
        dup.is_err(),
        "duplicate must still be refused after reopen (unique flag lost?)"
    );
    exec(&ex2, "INSERT INTO ur VALUES (3, 'three', 3)").await;
    let count = select_rows(&ex2, "SELECT COUNT(*) FROM ur").await;
    assert_eq!(count, vec![vec![Value::Int64(3)]]);
}
