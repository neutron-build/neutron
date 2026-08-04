//! R4: per-table storage engines (`WITH (engine='columnar'|'lsm')`) each keep
//! their own WAL, and — unlike the primary storage WAL and every other
//! specialty store — nothing in the running server called the method that
//! compacts it. Proved here: the WAL grows without bound across ordinary
//! writes, and `Executor::checkpoint_table_engines` (wired into `main.rs`'s
//! recurring `WalCheckpoint` task) is what stops that.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use super::exec;
use crate::catalog::Catalog;
use crate::storage::{DiskEngine, StorageEngine};

async fn open_executor(dir: &Path) -> Executor {
    let catalog = Arc::new(Catalog::new());
    let db_path = dir.join("nucleus.db");
    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);
    Executor::new_with_persistence(catalog, storage, None, Some(dir))
}

fn wal_len(dir: &Path, table: &str) -> u64 {
    let path = dir
        .join("columnar_engines")
        .join(format!("{table}_{:08x}", crc32c::crc32c(table.as_bytes())))
        .join("columnar.wal");
    std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
}

#[tokio::test]
async fn columnar_engine_wal_grows_unbounded_without_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(
        &ex,
        "CREATE TABLE t (id INT, v INT) WITH (engine='columnar')",
    )
    .await;

    let after_create = wal_len(dir.path(), "t");
    for i in 0..300 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i}, {i})")).await;
    }
    let after_writes = wal_len(dir.path(), "t");

    assert!(
        after_writes > after_create,
        "WAL should have grown from 300 inserts: {after_create} -> {after_writes}"
    );
    // Every one of those 300 inserts is still sitting in the log — nothing
    // has ever compacted it down to a snapshot of the 300-row end state.
    assert!(
        after_writes > 300 * 8,
        "WAL at {after_writes} bytes looks already compacted; this test needs \
         the UNCOMPACTED size to make its point about the missing checkpoint"
    );
}

#[tokio::test]
async fn checkpoint_table_engines_compacts_the_columnar_wal() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(
        &ex,
        "CREATE TABLE t (id INT, v INT) WITH (engine='columnar')",
    )
    .await;
    for i in 0..300 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i}, {i})")).await;
    }
    let before = wal_len(dir.path(), "t");

    ex.checkpoint_table_engines().await;

    let after = wal_len(dir.path(), "t");
    assert!(
        after < before,
        "checkpoint_table_engines should compact the per-table WAL to a \
         snapshot: {before} -> {after}"
    );

    // And the data survives exactly — this is a compaction, not a truncation.
    let rows = exec(&ex, "SELECT COUNT(*) FROM t").await;
    let n = match &rows[0] {
        crate::executor::ExecResult::Select { rows, .. } => match &rows[0][0] {
            crate::types::Value::Int64(v) => *v,
            crate::types::Value::Int32(v) => *v as i64,
            other => panic!("unexpected count cell: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    };
    assert_eq!(n, 300, "checkpoint must not lose or duplicate rows");
}

#[tokio::test]
async fn checkpoint_table_engines_recovers_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(
            &ex,
            "CREATE TABLE t (id INT, v INT) WITH (engine='columnar')",
        )
        .await;
        for i in 0..50 {
            exec(&ex, &format!("INSERT INTO t VALUES ({i}, {i})")).await;
        }
        ex.checkpoint_table_engines().await;
    }
    // Reopen from the compacted snapshot exactly as main.rs would.
    let ex2 = open_executor(dir.path()).await;
    let eng = ex2.open_columnar_engine("t");
    let rows = eng.scan("t").await.unwrap();
    assert_eq!(
        rows.len(),
        50,
        "all 50 rows must be recoverable from the checkpointed snapshot"
    );
}
/// `sync_schema` is another `{}` trait default that the per-table engines
/// inherit — the shape that produced R0 and the missing checkpoint above. Here
/// the default is CORRECT (these engines address columns positionally and
/// re-read the catalog, so there is no cached column schema to go stale, unlike
/// the disk engine's meta page), but "correct" is a claim that needs a test
/// standing behind it rather than a reading of the code. If a per-table engine
/// ever starts caching its column shape, ALTER stops agreeing with SELECT and
/// this catches it.
#[tokio::test]
async fn alter_add_column_backfills_on_every_per_table_engine() {
    for eng in ["columnar", "mergetree", "lsm"] {
        let ex = super::test_executor();
        let ddl = if eng == "mergetree" {
            "CREATE TABLE t (id INT, v INT) WITH (engine='mergetree') ORDER BY (id)".to_string()
        } else {
            format!("CREATE TABLE t (id INT, v INT) WITH (engine='{eng}')")
        };
        exec(&ex, &ddl).await;
        exec(&ex, "INSERT INTO t VALUES (1, 100)").await;
        exec(&ex, "INSERT INTO t VALUES (2, 200)").await;
        exec(&ex, "ALTER TABLE t ADD COLUMN w INT DEFAULT 7").await;

        let res = exec(&ex, "SELECT id, v, w FROM t ORDER BY id").await;
        let rows = match &res[0] {
            crate::executor::ExecResult::Select { rows, .. } => rows.clone(),
            other => panic!("{eng}: expected Select, got {other:?}"),
        };
        use crate::types::Value::Int32;
        assert_eq!(
            rows,
            vec![
                vec![Int32(1), Int32(100), Int32(7)],
                vec![Int32(2), Int32(200), Int32(7)],
            ],
            "{eng}: ALTER TABLE ADD COLUMN did not backfill existing rows correctly"
        );
    }
}

/// The LSM engine reaches the same checkpoint call. It has no separate WAL —
/// its durable representation is an SSTable, and `flush_all_dirty` forces the
/// memtables out — so there is no file to shrink here; what this asserts is
/// that driving the shared checkpoint path over a per-table LSM engine is
/// reachable and lossless. A future LSM WAL would compact through the exact
/// same call.
#[tokio::test]
async fn checkpoint_table_engines_is_lossless_for_lsm() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(&ex, "CREATE TABLE t (id INT, v INT) WITH (engine='lsm')").await;
    for i in 0..100 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i}, {i})")).await;
    }

    ex.checkpoint_table_engines().await;

    let res = exec(&ex, "SELECT COUNT(*) FROM t").await;
    let n = match &res[0] {
        crate::executor::ExecResult::Select { rows, .. } => match &rows[0][0] {
            crate::types::Value::Int64(v) => *v,
            crate::types::Value::Int32(v) => *v as i64,
            other => panic!("unexpected count cell: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    };
    assert_eq!(n, 100, "LSM checkpoint must not lose or duplicate rows");
}
