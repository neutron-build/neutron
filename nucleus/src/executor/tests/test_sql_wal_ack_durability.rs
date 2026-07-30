//! R4: an acked autocommit SQL write must be fsync-durable on the MVCC engine.
//!
//! The KV path has had this guarantee under test for a while
//! (`test_kv_write_is_fsync_durable_on_ack`). The SQL path did not, and it was
//! not holding: `MvccStorageAdapter` implemented neither `durability_pending`
//! nor `make_durable`, so it inherited the trait defaults (`false` / `Ok(())`)
//! and `force_wal_durability` — the executor's entire commit point — skipped
//! the engine. Explicit COMMIT was safe (it fsyncs inline through
//! `MvccWal::log_commit`), so the hole was autocommit-only.
//!
//! The crash probes could not see it: their child calls `db.sync()` after every
//! insert and only then prints `DURABLE`, which proves fsynced writes survive —
//! never in question — rather than that an acked write was fsynced.

use std::sync::Arc;

use super::super::Executor;
use super::exec;
use crate::catalog::Catalog;
use crate::storage::{MvccStorageAdapter, StorageEngine};

/// Open an executor on a WAL-backed MVCC engine, as `Database::durable_mvcc`
/// does, keeping a typed handle to the adapter so the WAL is observable.
fn open(dir: &std::path::Path) -> (Executor, Arc<MvccStorageAdapter>) {
    let catalog = Arc::new(Catalog::new());
    let (adapter, _schemas) = MvccStorageAdapter::with_wal(dir).unwrap();
    let adapter = Arc::new(adapter);
    let storage: Arc<dyn StorageEngine> = adapter.clone();
    let ex = Executor::new_with_persistence(catalog, storage, None, Some(dir));
    (ex, adapter)
}

#[tokio::test]
async fn autocommit_insert_is_fsync_durable_on_ack() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, adapter) = open(dir.path());
    exec(&ex, "CREATE TABLE t (id INT, v INT)").await;

    // Default synchronous_commit = on: the ack must mean fsynced.
    exec(&ex, "INSERT INTO t VALUES (1, 100)").await;
    let wal = adapter.wal().expect("a WAL-backed adapter has a WAL");
    assert!(
        !wal.is_dirty(),
        "an autocommit INSERT must fsync the SQL WAL before acking under \
         synchronous_commit=on — the client was told the row is committed"
    );
}

#[tokio::test]
async fn synchronous_commit_off_defers_the_sql_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, adapter) = open(dir.path());
    exec(&ex, "CREATE TABLE t (id INT, v INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 100)").await;

    // The other half of the contract: opting out must actually opt out, or
    // the setting is a lie in the cheaper direction.
    ex.set_synchronous_commit_default(false);
    exec(&ex, "INSERT INTO t VALUES (2, 200)").await;
    let wal = adapter.wal().expect("a WAL-backed adapter has a WAL");
    assert!(
        wal.is_dirty(),
        "synchronous_commit=off should defer the SQL fsync, leaving the tail dirty"
    );
}

#[tokio::test]
async fn explicit_commit_is_fsync_durable_on_ack() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, adapter) = open(dir.path());
    exec(&ex, "CREATE TABLE t (id INT, v INT)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (1, 100)").await;
    exec(&ex, "COMMIT").await;

    let wal = adapter.wal().expect("a WAL-backed adapter has a WAL");
    assert!(
        !wal.is_dirty(),
        "COMMIT must leave the SQL WAL fsynced before returning"
    );
}

/// A write inside an open transaction defers to COMMIT — forcing per-statement
/// would make every multi-statement transaction pay N fsyncs for one commit.
#[tokio::test]
async fn a_write_inside_a_transaction_defers_its_fsync_to_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, adapter) = open(dir.path());
    exec(&ex, "CREATE TABLE t (id INT, v INT)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO t VALUES (1, 100)").await;
    let wal = adapter.wal().expect("a WAL-backed adapter has a WAL");
    assert!(
        wal.is_dirty(),
        "an uncommitted write should not have been fsynced yet"
    );
    exec(&ex, "COMMIT").await;
    assert!(!wal.is_dirty(), "COMMIT must force it");
}
