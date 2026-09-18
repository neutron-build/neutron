//! Consumer-2 / teploy-observe F45: the cross-table snapshot lease.
//!
//! A holder (BEGIN + ACQUIRE SNAPSHOT LEASE) freezes the database's
//! logical moment: other sessions' mutations wait at the dispatch gate
//! until release/expiry, so a dump tool reading tables one by one — even
//! on separate autocommit connections — cannot mix logical moments.

use super::tests::{exec, rows, test_executor};
use super::Executor;

use crate::types::Value;

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

    let after = ex.execute(&"SELECT COUNT(*) FROM t".to_string()).await.unwrap();
    let count = match rows(&after[0])[0].first() {
        Some(Value::Int64(n)) => *n,
        Some(Value::Int32(n)) => i64::from(*n),
        v => panic!("unexpected count: {v:?}"),
    };
    assert_eq!(count, 2, "the writer must land once the lease expires");

    // The expired lease is gone from SHOW.
    let show = ex.execute(&"SHOW SNAPSHOT LEASE".to_string()).await.unwrap();
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
