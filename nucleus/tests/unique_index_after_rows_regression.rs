//! Regression: a UNIQUE index created over a table that ALREADY has rows must
//! actually enforce uniqueness — at creation, on later writes, and across a
//! checkpoint + reopen.
//!
//! `IndexDef.unique` existed and was persisted, but nothing read it back:
//! `check_unique_constraints` enumerated only `TableConstraint`s, so a
//! `CREATE UNIQUE INDEX` on a populated table accepted a duplicate insert
//! afterwards; `execute_create_index` registered the index without looking at
//! the rows already in the table, so existing duplicates were blessed; and
//! `rebuild_persistent_indexes` re-created the storage-side B-tree without
//! restoring the flag, so the guarantee (and the error text) differed before
//! and after a restart. The shape here is the one from the wild: a disk table,
//! `ALTER TABLE ADD COLUMN` with a default backfilling every existing row, then
//! `CREATE UNIQUE INDEX` on the new column.

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
/// same helper as alter_multi_op_regression.rs.
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

/// The error must be a unique-violation, not some unrelated failure (e.g. a
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

// ── in-memory: duplicate insert after CREATE UNIQUE INDEX is refused ────────

#[tokio::test]
async fn unique_index_on_added_column_refuses_duplicate_insert() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(nucleus::storage::ColumnarStorageEngine::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1), (2), (3)").await;

    // Backfill every existing row with the SAME value, then declare the column
    // unique — the existing rows are duplicates, so this must be refused.
    let dup = ex
        .execute("ALTER TABLE t ADD COLUMN email TEXT DEFAULT 'shared@example.com'")
        .await;
    assert!(dup.is_ok(), "ADD COLUMN with default: {dup:?}");
    let refused = ex
        .execute("CREATE UNIQUE INDEX uq_email ON t (email)")
        .await;
    assert!(
        refused.is_err(),
        "CREATE UNIQUE INDEX over existing duplicates must be refused"
    );

    // Same shape, but the backfill is distinct per row — the index is legal.
    exec(&ex, "CREATE TABLE u (id INT)").await;
    exec(&ex, "INSERT INTO u VALUES (1), (2)").await;
    exec(&ex, "ALTER TABLE u ADD COLUMN email TEXT DEFAULT 'a@example.com'").await;
    exec(&ex, "UPDATE u SET email = 'b@example.com' WHERE id = 2").await;
    exec(&ex, "CREATE UNIQUE INDEX uq_u_email ON u (email)").await;

    let dup = ex
        .execute("INSERT INTO u (id, email) VALUES (3, 'a@example.com')")
        .await;
    assert_unique_violation(&dup, "duplicate insert under unique index");
    assert!(
        ex.execute("INSERT INTO u (id, email) VALUES (4, 'c@example.com')")
            .await
            .is_ok(),
        "a non-conflicting insert must still be accepted"
    );
    let rows = select_rows(&ex, "SELECT id FROM u ORDER BY id").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(1)], vec![Value::Int64(2)], vec![Value::Int64(4)]],
        "the refused row must not have landed"
    );
}

// ── disk stack: content + refusal survive checkpoint + reopen ───────────────

#[tokio::test]
async fn unique_index_across_checkpoint_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE ui_disk (id BIGINT)").await;
        exec(
            &ex,
            "INSERT INTO ui_disk VALUES (1), (2), (3)",
        )
        .await;
        exec(
            &ex,
            "ALTER TABLE ui_disk ADD COLUMN email TEXT DEFAULT 'seed@example.com'",
        )
        .await;
        // Make the backfilled values distinct so the index is legal to create.
        exec(
            &ex,
            "UPDATE ui_disk SET email = 'user' || id || '@example.com'",
        )
        .await;
        exec(&ex, "CREATE UNIQUE INDEX uq_disk_email ON ui_disk (email)").await;

        let dup = ex
            .execute("INSERT INTO ui_disk (id, email) VALUES (9, 'user1@example.com')")
            .await;
        assert_unique_violation(&dup, "duplicate insert before checkpoint");
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT id, email FROM ui_disk ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1), Value::Text("user1@example.com".into())],
            vec![Value::Int64(2), Value::Text("user2@example.com".into())],
            vec![Value::Int64(3), Value::Text("user3@example.com".into())],
        ],
        "post-reopen: content must survive with the added column's values"
    );
    let dup = ex2
        .execute("INSERT INTO ui_disk (id, email) VALUES (10, 'user2@example.com')")
        .await;
    assert_unique_violation(&dup, "duplicate insert after reopen");
    assert!(
        ex2.execute("INSERT INTO ui_disk (id, email) VALUES (11, 'new@example.com')")
            .await
            .is_ok(),
        "a non-conflicting insert must be accepted after reopen"
    );
    let count = select_rows(&ex2, "SELECT COUNT(*) FROM ui_disk").await;
    assert_eq!(count, vec![vec![Value::Int64(4)]]);
}

// ── disk stack: an index over existing duplicates is refused, and stays refused ──

#[tokio::test]
async fn unique_index_over_existing_duplicates_refused_across_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, _buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE dup_disk (id BIGINT)").await;
        exec(&ex, "INSERT INTO dup_disk VALUES (1), (2)").await;
        // The default backfills BOTH rows with the same value.
        exec(
            &ex,
            "ALTER TABLE dup_disk ADD COLUMN code INT DEFAULT 7",
        )
        .await;
        let refused = ex
            .execute("CREATE UNIQUE INDEX uq_dup_code ON dup_disk (code)")
            .await;
        assert!(
            refused.is_err(),
            "CREATE UNIQUE INDEX over existing duplicates must be refused"
        );
        let err = refused.unwrap_err().to_string();
        assert!(
            err.contains("duplicate key value violates unique constraint")
                || err.to_lowercase().contains("duplicate"),
            "refusal must name the duplicate, got: {err}"
        );
        // The refused index must not have been registered.
        let rows = select_rows(&ex, "SELECT indexname FROM pg_indexes WHERE indexname = 'uq_dup_code'").await;
        assert!(rows.is_empty(), "refused index must not be registered: {rows:?}");
        // And the table is still writable under the (never created) index.
        assert!(
            ex.execute("INSERT INTO dup_disk (id, code) VALUES (3, 7)")
                .await
                .is_ok(),
            "with the index refused, a duplicate insert must be accepted again"
        );
    }
}
