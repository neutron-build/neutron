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
/// WAL-recovered schemas are registered in the catalog, mirroring
/// `embedded.rs` (the production reopen path).
fn open(dir: &std::path::Path) -> (Executor, Arc<MvccStorageAdapter>) {
    let catalog = Arc::new(Catalog::new());
    let (adapter, schemas) = MvccStorageAdapter::with_wal(dir).unwrap();
    for (name, columns) in schemas {
        use crate::catalog::{ColumnDef, TableDef};
        let cols: Vec<ColumnDef> = columns
            .into_iter()
            .map(|(col_name, dt)| ColumnDef {
                name: col_name,
                data_type: dt,
                nullable: true,
                default_expr: None,
                id: 0,
                analyzer: None,
            })
            .collect();
        let _ = catalog.create_table_sync(TableDef {
            name,
            columns: cols,
            constraints: Vec::new(),
            append_only: false,
            epoch: 0,
        });
    }
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

/// NU-006: an acked write to a *specialty* model must be fsync-durable too.
///
/// `force_specialty_durability` is the executor's commit point for the
/// non-SQL models, and it covered six of them. The document, FTS, blob, geo
/// and CDC logs were not in it, and their appends ended at a `Write::flush` —
/// which is a documented no-op on a bare `std::fs::File` and only a kernel
/// handoff on a `BufWriter`. So `DOC_INSERT` returned an id, the client
/// treated the document as stored, and a power cut lost it. Columnar already
/// had `group_sync` and was simply never called.
///
/// This asserts the property the models advertise rather than the mechanism:
/// after the ack, nothing is left un-fsynced. It fails against the old code —
/// remove any one block from `force_specialty_durability` and the matching
/// assertion below goes red, which is how each was checked.
#[tokio::test]
async fn acked_specialty_writes_are_fsync_durable() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _adapter) = open(dir.path());

    exec(&ex, "SELECT DOC_INSERT('{\"a\":1}')").await;
    assert!(
        !ex.doc_store().read().wal_is_dirty(),
        "DOC_INSERT acked with un-fsynced appends in the document WAL"
    );

    exec(&ex, "SELECT FTS_INDEX(1, 'hello world')").await;
    assert!(
        !ex.fts_index().read().wal_is_dirty(),
        "FTS_INDEX acked with un-fsynced appends in the FTS WAL"
    );

    ex.blob_store_put("k", b"payload", None);
    exec(&ex, "SELECT 1").await;
    assert!(
        !ex.blob_store().read().wal_is_dirty(),
        "a blob write was left un-fsynced past the next commit"
    );
    assert!(
        !ex.blob_store().read().segments_dirty(),
        "blob payload segments were left un-fsynced past the commit that \
         acked the manifest referencing them (BLO-1)"
    );

    assert!(
        !ex.columnar_store().read().wal_is_dirty(),
        "columnar had group_sync all along and was never called at commit"
    );
    assert!(
        ex.geo_wal.as_ref().is_none_or(|w| !w.is_dirty()),
        "geo WAL left un-fsynced past commit"
    );
    assert!(
        ex.cdc_wal.as_ref().is_none_or(|w| !w.is_dirty()),
        "CDC WAL left un-fsynced past commit"
    );
}

/// Cluster 1 (durable half of NU-01/09/14): version identities survive
/// restarts through the full adapter path. Rows recovered from a v2 log
/// keep their durable ids across reopen cycles, and writes minted after a
/// restart continue above the recovered floor — the identity space never
/// rewinds and never reuses an id for a different row.
#[tokio::test]
async fn version_ids_are_stable_across_restarts_and_never_reused() {
    let dir = tempfile::tempdir().unwrap();

    // Generation 1: three rows, one deleted, so live ids are sparse.
    {
        let (ex, _adapter) = open(dir.path());
        exec(&ex, "CREATE TABLE t (id INT, v INT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 10)").await;
        exec(&ex, "INSERT INTO t VALUES (2, 20)").await;
        exec(&ex, "INSERT INTO t VALUES (3, 30)").await;
        exec(&ex, "DELETE FROM t WHERE id = 2").await;
    }

    // Generation 2: reopen (replay + compact), add one row, close.
    {
        let (ex, _adapter) = open(dir.path());
        let out = exec(&ex, "SELECT id FROM t ORDER BY id").await;
        let ids: Vec<i64> = out
            .iter()
            .flat_map(super::rows)
            .filter_map(|r| match r.first() {
                Some(crate::types::Value::Int64(n)) => Some(*n),
                Some(crate::types::Value::Int32(n)) => Some(i64::from(*n)),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![1, 3], "the delete must survive the restart");
        exec(&ex, "INSERT INTO t VALUES (4, 40)").await;
    }

    // The durable log must now carry stable ids: the baseline preserved the
    // live ids (0, 2 after the delete of 1) and the new row minted at/above
    // the floor — never on top of a previous row's id.
    use crate::storage::mvcc_wal::MvccWal;
    let (wal, state) = MvccWal::open(dir.path()).unwrap();
    let t = state.tables.get("t").expect("table survived two restarts");
    let ids: Vec<u64> = t.rows.iter().map(|(id, _)| *id).collect();
    assert_eq!(t.rows.len(), 3, "two survivors + the new row");
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "ids must be strictly increasing, got {ids:?}"
    );
    assert!(
        t.rows.iter().all(|(id, row)| (*id as usize) < t.next_version_id as usize
            || !row.is_empty()),
        "every live id sits below the floor",
    );
    assert!(t.next_version_id > *ids.iter().max().unwrap());
    drop(wal);

    // Generation 3: one more reopen — ids still stable.
    let (ex, _adapter) = open(dir.path());
    let out = exec(&ex, "SELECT COUNT(*) FROM t").await;
    let count = super::rows(&out[0])[0]
        .first()
        .cloned()
        .unwrap_or(crate::types::Value::Null);
    let n = match count {
        crate::types::Value::Int64(n) => n,
        crate::types::Value::Int32(n) => i64::from(n),
        v => panic!("unexpected count value: {v:?}"),
    };
    assert_eq!(n, 3, "count after the third open");
}
