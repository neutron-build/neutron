//! Consumer-2 / teploy-observe F45: the cross-table snapshot lease.
//!
//! A holder (BEGIN + ACQUIRE SNAPSHOT LEASE) freezes the database's
//! logical moment: other sessions' mutations wait at the dispatch gate
//! until release/expiry, so a dump tool reading tables one by one — even
//! on separate autocommit connections — cannot mix logical moments.
//!
//! The 2026-09-18 teploy-observe lease-scope reports (three newest
//! entries in `Teploy/_internal/UPSTREAM_BUGS.md`) closed here:
//!
//! - KV/specialty scalar writes (`SELECT kv_set(...)`) bypassed the writer
//!   gate — the gate only matched relational DDL/DML shapes;
//! - holder reads were not pinned to the acquire moment (a writer whose
//!   statement predated the lease could COMMIT mid-window and the holder's
//!   next statement saw it);
//! - the holder could see another session's in-flight UNCOMMITTED writes
//!   on engines without versioning (per-table override engines write
//!   through immediately).
//!
//! The last two are closed by DRAINING write-bearing foreign transactions
//! at acquisition (bounded by the lease's own TIMEOUT) plus an
//! acquire-time snapshot refresh on versioning engines.

use super::tests::{exec, rows, test_executor};
use super::Executor;

use crate::types::Value;

/// An executor over `BufferedDiskEngine(DiskEngine)` — the storage shape a
/// server builds with a data directory, where two of the three reported
/// defects lived.
fn disk_executor(dir: &std::path::Path) -> Executor {
    use crate::storage::buffered_engine::BufferedDiskEngine;
    use crate::storage::disk_engine::DiskEngine;
    use crate::storage::StorageEngine;
    let catalog = std::sync::Arc::new(crate::catalog::Catalog::new());
    let disk = std::sync::Arc::new(DiskEngine::open(&dir.join("t.db"), catalog.clone()).unwrap());
    let engine: std::sync::Arc<dyn StorageEngine> =
        std::sync::Arc::new(BufferedDiskEngine::new(disk));
    Executor::new(catalog, engine)
}

/// An executor over the in-memory versioning stack (`--memory` server
/// shape, embedded `durable_mvcc`).
fn mvcc_executor() -> Executor {
    use crate::storage::mvcc::MvccStorageAdapter;
    use crate::storage::StorageEngine;
    let catalog = std::sync::Arc::new(crate::catalog::Catalog::new());
    let engine: std::sync::Arc<dyn StorageEngine> =
        std::sync::Arc::new(MvccStorageAdapter::new());
    Executor::new(catalog, engine)
}

fn i64_of(result: &super::ExecResult) -> i64 {
    match result {
        super::ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            other => panic!("unexpected cell: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

async fn count_of(ex: &Executor, session: u64, table: &str) -> i64 {
    let r = ex
        .execute_with_session(session, &format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap();
    i64_of(&r[0])
}

async fn kv_get(ex: &Executor, session: u64, key: &str) -> Value {
    let r = ex
        .execute_with_session(session, &format!("SELECT KV_GET('{key}')"))
        .await
        .unwrap();
    rows(&r[0])[0][0].clone()
}

#[tokio::test]
async fn lease_gives_point_in_time_reads_and_blocks_then_releases_writers() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE a (id INT)").await;
    exec(&ex, "CREATE TABLE b (id INT)").await;
    exec(&ex, "INSERT INTO a VALUES (1)").await;
    exec(&ex, "INSERT INTO b VALUES (1)").await;

    let holder = 10u64;
    let writer = 20u64;

    // Holder: open a transaction and take the lease.
    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 5000")
        .await
        .unwrap();

    // Writer: an INSERT from another session must WAIT, not land.
    let write = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer, "INSERT INTO a VALUES (2)")
                .await
                .expect("blocked writer must proceed after release")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !write.is_finished(),
        "the writer must still be blocked while the lease is held"
    );

    // Holder reads across tables: one logical moment, before the blocked
    // write. A concurrent reader on a THIRD session sees the same frozen
    // state (no writer can commit during the window).
    let a = ex
        .execute_with_session(holder, "SELECT id FROM a ORDER BY id")
        .await
        .unwrap();
    let b = ex
        .execute_with_session(holder, "SELECT id FROM b ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&a[0]).len(), 1, "holder sees the pre-write moment in a");
    assert_eq!(rows(&b[0]).len(), 1, "holder sees the pre-write moment in b");
    let third = ex
        .execute_with_session(30, "SELECT COUNT(*) FROM a")
        .await
        .unwrap();
    let count = match rows(&third[0])[0].first() {
        Some(Value::Int64(n)) => *n,
        Some(Value::Int32(n)) => i64::from(*n),
        v => panic!("unexpected count: {v:?}"),
    };
    assert_eq!(count, 1, "the frozen moment is visible to every session");

    // Release: the blocked writer proceeds and lands.
    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    let result = write.await.expect("writer task panicked");
    drop(result);
    let after = ex
        .execute_with_session(writer, "SELECT COUNT(*) FROM a")
        .await
        .unwrap();
    let count = match rows(&after[0])[0].first() {
        Some(Value::Int64(n)) => *n,
        Some(Value::Int32(n)) => i64::from(*n),
        v => panic!("unexpected count: {v:?}"),
    };
    assert_eq!(count, 2, "the released writer's row must land");
}

#[tokio::test]
async fn holder_writes_are_refused() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    let holder = 11u64;
    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 5000")
        .await
        .unwrap();
    let err = ex
        .execute_with_session(holder, "INSERT INTO t VALUES (1)")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("read-only"),
        "error must name the read-only view: {err}"
    );
    // And the failed write did not consume the lease: RELEASE works.
    ex.execute_with_session(holder, "RELEASE SNAPSHOT LEASE")
        .await
        .unwrap();
}

#[tokio::test]
async fn acquire_requires_a_transaction_and_rejects_second_holder() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;

    // No transaction: refused with the BEGIN hint.
    let err = ex
        .execute_with_session(1, "ACQUIRE SNAPSHOT LEASE")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("requires an active transaction"),
        "{err}"
    );

    // Holder takes it.
    ex.execute_with_session(1, "BEGIN").await.unwrap();
    ex.execute_with_session(1, "ACQUIRE SNAPSHOT LEASE TIMEOUT 60000")
        .await
        .unwrap();
    // Another session cannot.
    ex.execute_with_session(2, "BEGIN").await.unwrap();
    let err = ex
        .execute_with_session(2, "ACQUIRE SNAPSHOT LEASE")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("another session"),
        "error must name the conflict: {err}"
    );
    // SHOW reports the holder.
    let show = ex
        .execute_with_session(2, "SHOW SNAPSHOT LEASE")
        .await
        .unwrap();
    let holder_row = rows(&show[0]);
    assert_eq!(holder_row.len(), 1);
    assert_eq!(holder_row[0][0], Value::Int64(1));
}

#[tokio::test]
async fn rollback_releases_the_lease() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    ex.execute_with_session(5, "BEGIN").await.unwrap();
    ex.execute_with_session(5, "ACQUIRE SNAPSHOT LEASE TIMEOUT 60000")
        .await
        .unwrap();
    ex.execute_with_session(5, "ROLLBACK").await.unwrap();
    // The lease must be free now.
    ex.execute_with_session(6, "BEGIN").await.unwrap();
    ex.execute_with_session(6, "ACQUIRE SNAPSHOT LEASE TIMEOUT 60000")
        .await
        .unwrap();
    let show = ex.execute_with_session(6, "SHOW SNAPSHOT LEASE").await.unwrap();
    assert_eq!(rows(&show[0])[0][0], Value::Int64(6));
}

#[tokio::test]
async fn session_drop_releases_the_lease() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    let sid = ex.create_session();
    let _ = ex.execute_with_session(sid, "BEGIN").await.unwrap();
    let _ = ex
        .execute_with_session(sid, "ACQUIRE SNAPSHOT LEASE TIMEOUT 60000")
        .await
        .unwrap();
    ex.drop_session(sid);
    // Free after the drop: another session acquires immediately.
    ex.create_session();
    let _ = ex.execute_with_session(999, "BEGIN").await.unwrap();
    let _ = ex
        .execute_with_session(999, "ACQUIRE SNAPSHOT LEASE TIMEOUT 60000")
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_lease_lets_blocked_writers_recover() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;

    let holder = 7u64;
    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 100")
        .await
        .unwrap();

    // A writer arrives and blocks; the holder stalls (never commits).
    let write = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(8, "INSERT INTO t VALUES (2)")
                .await
                .expect("writer must recover at expiry")
        })
    };
    tokio::time::advance(std::time::Duration::from_millis(300)).await;
    let result = write.await.expect("writer task panicked");
    drop(result);

    let after = ex.execute("SELECT COUNT(*) FROM t").await.unwrap();
    let count = match rows(&after[0])[0].first() {
        Some(Value::Int64(n)) => *n,
        Some(Value::Int32(n)) => i64::from(*n),
        v => panic!("unexpected count: {v:?}"),
    };
    assert_eq!(count, 2, "the writer must land once the lease expires");

    // The expired lease is gone from SHOW.
    let show = ex.execute("SHOW SNAPSHOT LEASE").await.unwrap();
    assert!(rows(&show[0]).is_empty());
}

#[tokio::test]
async fn ddl_also_waits_for_the_window() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    ex.execute_with_session(1, "BEGIN").await.unwrap();
    ex.execute_with_session(1, "ACQUIRE SNAPSHOT LEASE TIMEOUT 5000")
        .await
        .unwrap();

    let ddl = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(2, "CREATE TABLE u (id INT)")
                .await
                .expect("DDL must proceed after release")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!ddl.is_finished(), "DDL must block behind the lease");
    ex.execute_with_session(1, "COMMIT").await.unwrap();
    ddl.await.expect("ddl task panicked");
    // Reads still work while the lease is held (no gate on SELECT).
}

// ===========================================================================
// 2026-09-18 teploy-observe lease-scope reports
// ===========================================================================

/// Defect (a): a KV scalar write from a non-holder (`SELECT KV_SET(...)`)
/// must WAIT at the writer gate exactly like DML — SQL scalar functions are
/// how every SQL client writes KV. Before the fix the write completed
/// immediately and was readable by the holder mid-window.
#[tokio::test]
async fn kv_scalar_writes_wait_for_the_window_and_holder_reads_stay_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let ex = std::sync::Arc::new(disk_executor(dir.path()));
    let holder = ex.create_session();
    let writer = ex.create_session();
    exec(&ex, "SELECT KV_SET('lease:probe', 'before')").await;

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
        .await
        .unwrap();

    let write = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer, "SELECT KV_SET('lease:probe', 'during-lease')")
                .await
                .expect("kv writer must proceed after release")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !write.is_finished(),
        "SELECT KV_SET from another session must block behind the lease"
    );
    assert_eq!(
        kv_get(&ex, holder, "lease:probe").await,
        Value::Text("before".into()),
        "holder KV read must stay pinned to the acquire moment"
    );

    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    write.await.expect("writer task panicked");
    assert_eq!(
        kv_get(&ex, holder, "lease:probe").await,
        Value::Text("during-lease".into()),
        "the gated write must land after release"
    );
}

/// Defect (a), harder shapes: the mutating call hides in a WHERE clause,
/// carries a `pg_catalog.` qualifier, and arrives through the collection
/// functions (`KV_SADD`) — all must take the gate.
#[tokio::test]
async fn mutating_scalar_calls_in_where_pg_catalog_and_collections_also_gate() {
    let dir = tempfile::tempdir().unwrap();
    let ex = std::sync::Arc::new(disk_executor(dir.path()));
    let holder = ex.create_session();
    let writer = ex.create_session();

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
        .await
        .unwrap();

    let writes = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer, "SELECT 1 WHERE PG_CATALOG.KV_SET('k2','v')")
                .await
                .expect("pg_catalog-qualified kv_set must proceed after release");
            ex.execute_with_session(writer, "SELECT KV_SADD('lease:s','m')")
                .await
                .expect("kv_sadd must proceed after release");
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !writes.is_finished(),
        "WHERE-carried and pg_catalog-qualified scalar writes must block"
    );

    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    writes.await.expect("writer task panicked");
    let member = ex
        .execute("SELECT KV_SCARD('lease:s')")
        .await
        .unwrap();
    assert_eq!(
        i64_of(&member[0]),
        1,
        "the gated collection write must land after release"
    );
}

/// Defect (c): the holder must never see another session's in-flight
/// UNCOMMITTED writes. The reported live shape was an override-engine
/// (mergetree) table on the disk stack — those engines have no transaction
/// buffering, so the parked INSERT was already visible to every reader.
/// The fix drains: acquisition WAITS for the write-bearing transaction to
/// end, so the window can only open on resolved state. Before the fix the
/// acquire returned instantly and the holder counted the uncommitted row.
#[tokio::test]
async fn acquire_drains_parked_writers_so_uncommitted_rows_never_enter_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let ex = std::sync::Arc::new(disk_executor(dir.path()));
    exec(
        &ex,
        "CREATE TABLE ev (id INT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    let holder = ex.create_session();
    let writer = ex.create_session();

    // Park an uncommitted INSERT (the observe shape: an ingest transaction
    // in flight when the backup starts).
    ex.execute_with_session(writer, "BEGIN").await.unwrap();
    ex.execute_with_session(writer, "INSERT INTO ev VALUES (1)").await.unwrap();

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    let acquire = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
                .await
                .expect("acquire must complete once the straddler drains")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !acquire.is_finished(),
        "acquisition must wait behind a parked write-bearing transaction"
    );

    // The straddler resolves by ROLLBACK: the row never existed, and the
    // window opens only now.
    ex.execute_with_session(writer, "ROLLBACK").await.unwrap();
    acquire.await.expect("acquire task panicked");
    assert_eq!(
        count_of(&ex, holder, "ev").await,
        0,
        "the rolled-back row must never be visible to the holder"
    );

    // And the same shape resolving by COMMIT: the row is part of the
    // acquire moment (it committed before the window opened).
    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    assert_eq!(count_of(&ex, writer, "ev").await, 0);
    ex.execute_with_session(writer, "BEGIN").await.unwrap();
    ex.execute_with_session(writer, "INSERT INTO ev VALUES (2)").await.unwrap();
    ex.execute_with_session(writer, "COMMIT").await.unwrap();
    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
        .await
        .unwrap();
    let mid = count_of(&ex, holder, "ev").await;
    let writer2 = ex.create_session();
    let write = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer2, "INSERT INTO ev VALUES (3)")
                .await
                .expect("writer must proceed after release")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!write.is_finished(), "mid-window writer must be gated");
    let after = count_of(&ex, holder, "ev").await;
    assert_eq!(mid, 1, "holder reads the acquire moment");
    assert_eq!(after, 1, "a mid-window COMMIT must not advance the holder");
    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    write.await.expect("writer task panicked");
    assert_eq!(count_of(&ex, writer2, "ev").await, 2);
}

/// Defect (b), heap-table shape on the disk stack: a writer that executed
/// its INSERT before the lease existed used to COMMIT mid-window and the
/// holder's next statement saw the new row. The drain closes the commit
/// path: the window opens only after that transaction ended.
#[tokio::test]
async fn mid_window_commit_cannot_advance_the_holder_moment() {
    let dir = tempfile::tempdir().unwrap();
    let ex = std::sync::Arc::new(disk_executor(dir.path()));
    exec(&ex, "CREATE TABLE t (id INT)").await;
    let holder = ex.create_session();
    let writer = ex.create_session();

    ex.execute_with_session(writer, "BEGIN").await.unwrap();
    ex.execute_with_session(writer, "INSERT INTO t VALUES (1)").await.unwrap();

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    let acquire = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
                .await
                .expect("acquire must complete once the writer commits")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !acquire.is_finished(),
        "acquisition must wait behind the write-bearing transaction"
    );

    ex.execute_with_session(writer, "COMMIT").await.unwrap();
    acquire.await.expect("acquire task panicked");

    // The commit landed BEFORE the window opened, so it is IN the moment;
    // reads across statements and tables agree and stay put.
    assert_eq!(count_of(&ex, holder, "t").await, 1);
    assert_eq!(count_of(&ex, holder, "t").await, 1, "the moment is stable");

    // A NEW writer mid-window is gated.
    let write = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer, "INSERT INTO t VALUES (2)")
                .await
                .expect("writer must proceed after release")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!write.is_finished(), "mid-window writer must be gated");
    assert_eq!(count_of(&ex, holder, "t").await, 1);

    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    write.await.expect("writer task panicked");
    assert_eq!(count_of(&ex, writer, "t").await, 2);
}

/// The acquire pins the holder's snapshot to the ACQUIRE moment, not BEGIN,
/// on versioning engines: a commit that lands between the holder's BEGIN
/// and its ACQUIRE belongs to the view. Before the refresh the MVCC stack
/// kept the BEGIN snapshot and the holder read 0 for a committed row.
#[tokio::test]
async fn acquire_pins_to_the_acquire_moment_not_begin_on_versioning_engines() {
    let ex = std::sync::Arc::new(mvcc_executor());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    let holder = ex.create_session();
    let writer = ex.create_session();

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    // Committed AFTER the holder's BEGIN, BEFORE its ACQUIRE.
    ex.execute_with_session(writer, "INSERT INTO t VALUES (1)").await.unwrap();

    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
        .await
        .unwrap();
    assert_eq!(
        count_of(&ex, holder, "t").await,
        1,
        "the acquire moment includes the pre-window commit"
    );

    // And it stays pinned: a mid-window writer is gated, nothing advances.
    let write = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer, "INSERT INTO t VALUES (2)")
                .await
                .expect("writer must proceed after release")
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!write.is_finished(), "mid-window writer must be gated");
    assert_eq!(count_of(&ex, holder, "t").await, 1);

    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    write.await.expect("writer task panicked");
}

/// The drain is bounded by the lease's own TIMEOUT: a writer that parks
/// forever fails the acquisition with a named error rather than wedging
/// the backup tool.
#[tokio::test]
async fn acquire_times_out_behind_a_writer_that_never_drains() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    let holder = ex.create_session();
    let writer = ex.create_session();

    ex.execute_with_session(writer, "BEGIN").await.unwrap();
    ex.execute_with_session(writer, "INSERT INTO t VALUES (1)").await.unwrap();

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    let err = ex
        .execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 200")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("in-flight writer"),
        "error must name the drain: {err}"
    );
    // The failed acquisition holds no lease.
    let show = ex
        .execute("SHOW SNAPSHOT LEASE")
        .await
        .unwrap();
    assert!(rows(&show[0]).is_empty());
    ex.execute_with_session(writer, "ROLLBACK").await.unwrap();
}

/// A transaction that has already written cannot take the lease: its own
/// uncommitted work would be inside the "frozen" view, and its write-side
/// resources could deadlock the drain.
#[tokio::test]
async fn acquire_refuses_a_write_bearing_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(&ex, "CREATE TABLE t (id INT)").await;
    let holder = ex.create_session();
    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "INSERT INTO t VALUES (1)").await.unwrap();
    let err = ex
        .execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("already written"),
        "error must name the rule: {err}"
    );
    ex.execute_with_session(holder, "ROLLBACK").await.unwrap();
}

/// Engine-side equivalent of teploy-observe's backup consistency tests
/// (internal/backup/lease_live_test.go conceptually depends on (b)/(c)):
/// under a lease, a dump that reads a heap table, an override-engine
/// (mergetree) table, and KV keys one by one sees exactly one moment —
/// stable across repeated reads — and writers from other sessions (SQL DML
/// and KV scalars) cannot land until release.
#[tokio::test]
async fn backup_under_lease_is_one_moment_across_models() {
    let dir = tempfile::tempdir().unwrap();
    let ex = std::sync::Arc::new(disk_executor(dir.path()));
    exec(&ex, "CREATE TABLE heap_t (id INT)").await;
    exec(
        &ex,
        "CREATE TABLE mt_t (id INT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    let holder = ex.create_session();
    let writer = ex.create_session();

    // Pre-window state: one row everywhere.
    exec(&ex, "INSERT INTO heap_t VALUES (1)").await;
    exec(&ex, "INSERT INTO mt_t VALUES (1)").await;
    exec(&ex, "SELECT KV_SET('bk:x', 'one')").await;

    ex.execute_with_session(holder, "BEGIN").await.unwrap();
    ex.execute_with_session(holder, "ACQUIRE SNAPSHOT LEASE TIMEOUT 30000")
        .await
        .unwrap();

    // Mid-window writers on every surface: all gated.
    let writes = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(writer, "INSERT INTO heap_t VALUES (2)")
                .await
                .expect("heap writer must proceed after release");
            ex.execute_with_session(writer, "INSERT INTO mt_t VALUES (2)")
                .await
                .expect("mergetree writer must proceed after release");
            ex.execute_with_session(writer, "SELECT KV_SET('bk:x', 'two')")
                .await
                .expect("kv writer must proceed after release");
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!writes.is_finished(), "all three writers must be gated");

    // The dump: repeated reads across models, one moment.
    for _ in 0..2 {
        assert_eq!(count_of(&ex, holder, "heap_t").await, 1);
        assert_eq!(count_of(&ex, holder, "mt_t").await, 1);
        assert_eq!(
            kv_get(&ex, holder, "bk:x").await,
            Value::Text("one".into())
        );
    }

    ex.execute_with_session(holder, "COMMIT").await.unwrap();
    writes.await.expect("writers task panicked");
    // After release the writes are all visible.
    assert_eq!(count_of(&ex, writer, "heap_t").await, 2);
    assert_eq!(count_of(&ex, writer, "mt_t").await, 2);
    assert_eq!(
        kv_get(&ex, writer, "bk:x").await,
        Value::Text("two".into())
    );
}
