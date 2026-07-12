//! Regression tests for the durable-MVCC WAL recovery bug found by the recovery
//! fuzzer (src/bin/probe_recover.rs): replay was position-based and resurrected
//! deleted rows / dropped or duplicated updated rows, and cross-crash version
//! index reuse lost rows. Fixed by logging stable version indices + compacting
//! the WAL to a clean baseline on open.
#![cfg(feature = "server")]
use nucleus::embedded::Database;
use nucleus::executor::ExecResult;

async fn ids_vals(db: &Database) -> Vec<(i64, i64)> {
    match db
        .execute("SELECT id, v FROM t ORDER BY id")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|r| {
                let g = |v: &nucleus::types::Value| match v {
                    nucleus::types::Value::Int32(n) => *n as i64,
                    nucleus::types::Value::Int64(n) => *n,
                    x => panic!("{x:?}"),
                };
                (g(&r[0]), g(&r[1]))
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}
fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("nucleus_recovery_regression_{tag}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Single crash: committed INSERT/UPDATE/DELETE recover exactly.
#[tokio::test]
async fn single_crash_recovers_committed_state() {
    let dir = tmp("single");
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
            .await
            .unwrap();
        db.execute("UPDATE t SET v=99 WHERE id=2").await.unwrap();
        db.execute("DELETE FROM t WHERE id=3").await.unwrap();
        db.sync().unwrap();
    }
    let db = Database::durable_mvcc(&dir).unwrap();
    assert_eq!(ids_vals(&db).await, vec![(1, 10), (2, 99), (4, 40)]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Multiple crash/recover cycles: a fresh run's writes must not collide with a
/// survivor's old version index (previously lost rows on the 2nd recovery).
#[tokio::test]
async fn multi_crash_no_version_index_collision() {
    let dir = tmp("multi");
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .await
            .unwrap();
        db.execute("DELETE FROM t WHERE id=2").await.unwrap();
        db.sync().unwrap();
    }
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        assert_eq!(ids_vals(&db).await, vec![(1, 10), (3, 30)]);
        db.execute("INSERT INTO t VALUES (4,40)").await.unwrap();
        db.execute("UPDATE t SET v=99 WHERE id=1").await.unwrap();
        db.sync().unwrap();
    }
    let db = Database::durable_mvcc(&dir).unwrap();
    assert_eq!(ids_vals(&db).await, vec![(1, 99), (3, 30), (4, 40)]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// An uncommitted transaction's writes are rolled back on crash.
#[tokio::test]
async fn uncommitted_txn_rolled_back_on_crash() {
    let dir = tmp("rollback");
    {
        let db = Database::durable_mvcc(&dir).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20)")
            .await
            .unwrap();
        db.sync().unwrap();
        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (3,30),(4,40)")
            .await
            .unwrap();
        // no COMMIT — crash
    }
    let db = Database::durable_mvcc(&dir).unwrap();
    assert_eq!(ids_vals(&db).await, vec![(1, 10), (2, 20)]);
    let _ = std::fs::remove_dir_all(&dir);
}
