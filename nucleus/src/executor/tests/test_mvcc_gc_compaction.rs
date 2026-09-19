//! NU-01 tail compaction, durable half: WAL v2's stable ids unblocked
//! in-memory GC compaction; the landed subset reclaims all-dead TAIL
//! version slots while preserving every live id, the position==durable-id
//! invariant, and the minted-space never-rewinds guarantee.
//!
//! Compaction is MEMORY-ONLY by design — no WAL record is required,
//! because replay already reconstructs a superset (it re-seats live rows at
//! their durable ids and pads to a floor that covers every id the log ever
//! contained, dead or aborted). These tests pin that contract end to end:
//! compaction followed by more writes followed by a reopen must produce
//! exactly the logical state the process had, with no reclaimed id ever
//! re-minted — in this run or across restarts.

use std::sync::Arc;

use super::super::Executor;
use super::exec;
use crate::catalog::Catalog;
use crate::storage::{MvccStorageAdapter, StorageEngine};

/// Open an executor on a WAL-backed MVCC engine, registering recovered
/// schemas in the catalog the way `embedded.rs` (the production reopen
/// path) does.
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

/// The ids a session can see, in scan order — the stable identities
/// compaction must never move.
async fn visible_ids(ex: &Executor, table: &str) -> Vec<i64> {
    let result = ex
        .execute(&format!("SELECT id FROM {table}"))
        .await
        .unwrap();
    match &result[0] {
        crate::executor::ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|r| match &r[0] {
                crate::types::Value::Int32(v) => *v as i64,
                crate::types::Value::Int64(v) => *v,
                other => panic!("unexpected cell: {other:?}"),
            })
            .collect(),
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Current WAL file size in bytes.
fn wal_bytes(dir: &std::path::Path) -> u64 {
    std::fs::metadata(dir.join("mvcc.wal")).unwrap().len()
}

/// Compaction reclaims the dead tail, mints above the horizon afterwards,
/// and the whole thing survives a reopen with identity intact.
#[tokio::test]
async fn compaction_reclaims_mints_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, adapter) = open(dir.path());
        exec(&ex, "CREATE TABLE t (id INT)").await;
        for i in 0..10 {
            exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
        }
        exec(&ex, "DELETE FROM t WHERE id >= 5").await;

        // The WAL's byte size across VACUUM proves the durable no-op half:
        // compaction writes nothing (a new WAL record would be required if
        // it had any durable effect).
        let wal_bytes_before = wal_bytes(dir.path());

        let vacuum = exec(&ex, "VACUUM t").await;
        drop(vacuum);

        let wal_bytes_after = wal_bytes(dir.path());
        assert_eq!(
            wal_bytes_before, wal_bytes_after,
            "tail compaction must not touch the WAL"
        );

        // Post-compaction mint: id at or above the pre-truncation horizon.
        exec(&ex, "INSERT INTO t VALUES (99)").await;
        let physical = adapter.scan_physical("t").await.unwrap();
        assert!(
            physical.iter().all(|(idx, _)| *idx < 5 || *idx >= 10),
            "a reclaimed id was re-minted: {physical:?}"
        );
        let ids = visible_ids(&ex, "t").await;
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 99]);
    }

    // Reopen: same logical state, surviving ids stable, no id reuse.
    {
        let (ex, adapter) = open(dir.path());
        let ids = visible_ids(&ex, "t").await;
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 99], "reopen changed the logical state");
        let physical = adapter.scan_physical("t").await.unwrap();
        assert!(
            physical.iter().all(|(idx, _)| *idx < 5 || *idx >= 10),
            "reopen re-seated a row at a reclaimed id: {physical:?}"
        );

        // And the mint horizon still does not rewind after the restart.
        exec(&ex, "INSERT INTO t VALUES (100)").await;
        let physical = adapter.scan_physical("t").await.unwrap();
        assert!(
            physical.iter().all(|(idx, _)| *idx < 5 || *idx >= 10),
            "post-reopen mint reused a reclaimed id: {physical:?}"
        );
        let ids = visible_ids(&ex, "t").await;
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 99, 100]);
    }
}

/// Live ids never move, even when dead versions sit below them: an
/// UPDATE's replacement version appends at a fresh id, the survivor keeps
/// its slot, and a reopen agrees.
#[tokio::test]
async fn compaction_keeps_live_ids_stable_across_updates_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (ex, _adapter) = open(dir.path());
        exec(&ex, "CREATE TABLE t (id INT)").await;
        for i in 0..5 {
            exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
        }
        // id 4 replaces itself: old version 4 dies, new version lands at 5.
        exec(&ex, "UPDATE t SET id = 40 WHERE id = 4").await;
        // id 1 dies in the middle of the vector (below live slots).
        exec(&ex, "DELETE FROM t WHERE id = 1").await;
        exec(&ex, "VACUUM t").await;

        let ids = visible_ids(&ex, "t").await;
        assert_eq!(ids, vec![0, 2, 3, 40]);
        let physical = _adapter.scan_physical("t").await.unwrap();
        // Slots 0, 2, 3 keep their original ids; the replacement lives
        // above the dead one. No live row moved.
        let live: Vec<usize> = physical.iter().map(|(i, _)| *i).collect();
        assert!(live.contains(&0) && live.contains(&2) && live.contains(&3));
        assert!(live.iter().all(|i| *i != 4), "dead slot 4 must be gone or stay dead");
    }
    {
        let (ex, _adapter) = open(dir.path());
        let ids = visible_ids(&ex, "t").await;
        assert_eq!(ids, vec![0, 2, 3, 40]);
    }
}

/// Torn-tail interaction: a WAL torn AFTER compaction + further mints
/// recovers the valid prefix, and the recovered floor keeps ids from ever
/// rewinding — the compaction never weakens torn-tail recovery because it
/// never wrote anything.
#[tokio::test]
async fn torn_tail_after_compaction_recovers_prefix_and_keeps_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (ex, _adapter) = open(dir.path());
        exec(&ex, "CREATE TABLE t (id INT)").await;
        for i in 0..6 {
            exec(&ex, &format!("INSERT INTO t VALUES ({i})")).await;
        }
        exec(&ex, "DELETE FROM t WHERE id >= 3").await;
        exec(&ex, "VACUUM t").await;
        // Post-compaction mints (ids >= 6).
        exec(&ex, "INSERT INTO t VALUES (60)").await;
        exec(&ex, "INSERT INTO t VALUES (61)").await;
    }

    // Tear the last frame's payload (valid header prefix = torn tail).
    let path = dir.path().join("mvcc.wal");
    let full = std::fs::read(&path).unwrap();
    let mut pos = 0usize;
    let mut last_start = 0usize;
    while pos + 14 <= full.len() {
        let len = u32::from_le_bytes([full[pos + 6], full[pos + 7], full[pos + 8], full[pos + 9]])
            as usize;
        last_start = pos;
        pos += 14 + len + 4;
    }
    std::fs::write(&path, &full[..last_start + 7]).unwrap();

    let (ex, adapter) = open(dir.path());
    // The torn frame (INSERT 61) is gone; everything before survives.
    let ids = visible_ids(&ex, "t").await;
    assert_eq!(ids, vec![0, 1, 2, 60]);
    // The recovered floor covers every id the log ever contained — the
    // next mint cannot land on a reclaimed id.
    exec(&ex, "INSERT INTO t VALUES (62)").await;
    let physical = adapter.scan_physical("t").await.unwrap();
    assert!(
        physical.iter().all(|(idx, _)| *idx < 3 || *idx >= 6),
        "post-torn-recovery mint reused a reclaimed id: {physical:?}"
    );
}
