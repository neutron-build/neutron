//! Commit-time durability regression tests.
//!
//! The durability contract under test: once a write statement (autocommit) or
//! COMMIT is acked with `synchronous_commit = on` (the default), the rows
//! survive an immediate kill -9 — not just a kill after the next checkpoint.
//!
//! Crash simulation: the engine flushes on Drop (clean shutdown), so these
//! tests copy the data directory WHILE the first engine is still alive. The
//! copy is exactly what a kill -9 would leave behind: WAL forced at commit
//! time, data pages and table directory unflushed.
//!
//! NOTE: table names are unique per test — the replacing_mergetree dedup
//! registry is process-global (see dogfood_observe_verification.rs).

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

/// Boot a server-shaped stack (segmented WAL DiskEngine wrapped in
/// BufferedDiskEngine, executor with persistence) from a data directory,
/// mirroring main.rs: load catalog, re-register tables, restore engines.
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
    ex.execute(sql).await.expect(sql).pop().expect("a result")
}

async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

fn count_of(rows: &[Vec<Value>]) -> i64 {
    match &rows[0][0] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        Value::Text(s) => s.trim().parse().unwrap(),
        other => panic!("not a count: {other:?}"),
    }
}

/// Recursively copy a directory — the kill -9 snapshot.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

// ── Heap tables (DiskEngine page WAL) ───────────────────────────────────────

#[tokio::test]
async fn autocommit_inserts_survive_simulated_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_heap (id BIGINT, name TEXT)").await;
    for i in 0..50 {
        exec(&ex, &format!("INSERT INTO cd_heap VALUES ({i}, 'row-{i}')")).await;
    }

    // kill -9 view: WAL was forced per statement, nothing else flushed.
    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT COUNT(*) FROM cd_heap").await;
    assert_eq!(
        count_of(&r),
        50,
        "acked autocommit inserts must survive kill -9 immediately"
    );
}

/// A bare `CREATE TABLE` (no inserts) dirties no data page, so pre-fix the
/// on-disk table directory was never forced — only the catalog was fsync'd,
/// leaving storage behind it. The `flush_schema` at DDL commit now forces the
/// directory. We verify at the raw DiskEngine level (NO catalog re-register, the
/// band-aid that would otherwise mask this): the recovered directory itself must
/// carry the table across a kill -9, so a scan returns empty rather than
/// TableNotFound.
#[tokio::test]
async fn bare_create_table_directory_is_crash_durable() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_bare (id BIGINT)").await;

    // kill -9 snapshot taken while the engine is alive (no clean-shutdown flush).
    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    // Reopen the DiskEngine directly — a fresh empty catalog, so nothing
    // re-creates the table from catalog metadata. Only storage's own recovered
    // directory can know it.
    let catalog = Arc::new(Catalog::new());
    let db_path = crash.join("nucleus.db");
    let engine =
        DiskEngine::open_segmented_with_sync(&db_path, catalog, 1024, 16, SyncMode::Fsync).unwrap();
    let scanned = engine.scan("cd_bare").await;
    assert!(
        scanned.is_ok(),
        "a CREATE-only table must be recoverable from the crash-forced storage \
         directory, got {scanned:?}"
    );
    assert_eq!(scanned.unwrap().len(), 0, "the recovered table is empty");
}

#[tokio::test]
async fn synchronous_commit_off_defers_durability() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_async (id BIGINT)").await;
    // With the default (on), the WAL force drains pending work per statement.
    exec(&ex, "INSERT INTO cd_async VALUES (1)").await;
    assert!(
        !buf.durability_pending(),
        "synchronous_commit=on must drain the WAL-pending set at statement end"
    );

    exec(&ex, "SET synchronous_commit = off").await;
    exec(&ex, "INSERT INTO cd_async VALUES (2)").await;
    assert!(
        buf.durability_pending(),
        "synchronous_commit=off must leave the force to a later flush/checkpoint"
    );
}

#[tokio::test]
async fn explicit_transaction_commit_is_durable() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_txn (id BIGINT)").await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO cd_txn VALUES (1)").await;
    exec(&ex, "INSERT INTO cd_txn VALUES (2)").await;
    exec(&ex, "INSERT INTO cd_txn VALUES (3)").await;
    exec(&ex, "COMMIT").await;

    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT COUNT(*) FROM cd_txn").await;
    assert_eq!(
        count_of(&r),
        3,
        "committed transaction must survive kill -9"
    );
}

// ── MergeTree tables (per-table columnar engine + engines.json) ─────────────

#[tokio::test]
async fn replacing_mergetree_survives_crash_with_dedup_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE cd_rt (k TEXT, v TEXT, ver BIGINT) \
         WITH (engine='replacing_mergetree', version_column='ver') ORDER BY (k)",
    )
    .await;
    exec(&ex, "INSERT INTO cd_rt (k, v, ver) VALUES ('a', 'old', 1)").await;
    exec(&ex, "INSERT INTO cd_rt (k, v, ver) VALUES ('a', 'new', 2)").await;

    // kill -9 view. Pre-fix this lost BOTH properties: the rows (per-table
    // engine was pure in-memory) and the dedup config (registry was only
    // populated by the original CREATE TABLE statement).
    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT v FROM cd_rt").await;
    assert_eq!(
        r.len(),
        1,
        "replacing dedup must survive restart (got {} rows)",
        r.len()
    );
    assert_eq!(
        r[0][0],
        Value::Text("new".into()),
        "newest version must win after recovery"
    );
}

#[tokio::test]
async fn mergetree_rows_survive_simulated_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE cd_mt (id BIGINT, val TEXT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    for i in 0..20 {
        exec(&ex, &format!("INSERT INTO cd_mt VALUES ({i}, 'v{i}')")).await;
    }

    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT COUNT(*) FROM cd_mt").await;
    assert_eq!(
        count_of(&r),
        20,
        "mergetree rows must survive kill -9 via the per-table columnar WAL"
    );
}

#[tokio::test]
async fn drop_table_removes_engine_sidecar() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE cd_drop (id BIGINT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    let engines_json = data.join("engines.json");
    let listed = std::fs::read_to_string(&engines_json).unwrap();
    assert!(listed.contains("cd_drop"), "CREATE must record the engine");

    exec(&ex, "DROP TABLE cd_drop").await;
    let listed = std::fs::read_to_string(&engines_json).unwrap();
    assert!(
        !listed.contains("cd_drop"),
        "DROP must remove the engines.json entry"
    );
}
