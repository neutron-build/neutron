//! Regression for #5b: disk-mode recovery durability/atomicity under DML.
//! Covers UPDATE/DELETE persistence, explicit-txn multi-cycle recovery, and —
//! the actual bug — that an ABANDONED transaction (BEGIN + writes, dropped with
//! no COMMIT) must NOT survive a reopen. The disk engine applied txn writes to
//! its in-memory directory immediately and flushed that (uncommitted) directory
//! on Drop; fixed by rolling back any open txn's in-memory state before the
//! Drop-time flush (DiskEngine::rollback_open_txn_in_memory).
#![cfg(feature = "server")]
use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::types::Value;

fn rows_of(r: Vec<ExecResult>) -> Vec<(i64, i64)> {
    match r.into_iter().next_back().unwrap() {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|row| {
                let g = |i: usize| match row.get(i) {
                    Some(Value::Int64(n)) => *n,
                    Some(Value::Int32(n)) => *n as i64,
                    _ => i64::MIN,
                };
                (g(0), g(1))
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

#[tokio::test]
async fn disk_abandoned_txn_does_not_survive_reopen() {
    let path = std::env::temp_dir().join("nucleus_disk_abandoned_txn.ndb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));
    {
        let db = Database::builder().disk(&path).build().unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20)")
            .await
            .unwrap();
        // Uncommitted transaction: BEGIN + inserts, then drop WITHOUT commit.
        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (3,30),(4,40)")
            .await
            .unwrap();
        // no COMMIT — db dropped below
    }
    let db = Database::builder().disk(&path).build().unwrap();
    let recovered = rows_of(
        db.execute("SELECT id,v FROM t ORDER BY id,v")
            .await
            .unwrap(),
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));
    assert_eq!(
        recovered,
        vec![(1, 10), (2, 20)],
        "uncommitted (abandoned-txn) rows must NOT survive a reopen"
    );
}

#[tokio::test]
async fn disk_recovery_explicit_txn_multicycle() {
    let path = std::env::temp_dir().join("nucleus_disk_recover_txn.ndb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));

    {
        let db = Database::builder().disk(&path).build().unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .await
            .unwrap();
        // explicit transaction
        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .await
            .unwrap();
        db.execute("DELETE FROM t WHERE id=2").await.unwrap();
        db.execute("UPDATE t SET v=99 WHERE id=3").await.unwrap();
        db.execute("COMMIT").await.unwrap();
    } // drop = clean flush

    // cycle 1: reopen, mutate more in a txn, drop
    {
        let db = Database::builder().disk(&path).build().unwrap();
        let r = rows_of(
            db.execute("SELECT id,v FROM t ORDER BY id,v")
                .await
                .unwrap(),
        );
        assert_eq!(r, vec![(1, 10), (3, 99)], "cycle1 open state");
        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (4,40)").await.unwrap();
        db.execute("UPDATE t SET v=11 WHERE id=1").await.unwrap();
        db.execute("COMMIT").await.unwrap();
    }

    // cycle 2: reopen, verify final state
    let db = Database::builder().disk(&path).build().unwrap();
    let recovered = rows_of(
        db.execute("SELECT id,v FROM t ORDER BY id,v")
            .await
            .unwrap(),
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));
    assert_eq!(
        recovered,
        vec![(1, 11), (3, 99), (4, 40)],
        "cycle2 recovered state"
    );
}

#[tokio::test]
async fn disk_recovery_reflects_update_delete() {
    let path = std::env::temp_dir().join("nucleus_disk_recover_dml.ndb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));

    let expected;
    {
        let db = Database::builder().disk(&path).build().unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
            .await
            .unwrap();
        db.execute("DELETE FROM t WHERE id=2").await.unwrap();
        db.execute("DELETE FROM t WHERE id=4").await.unwrap();
        db.execute("UPDATE t SET v=999 WHERE id=3").await.unwrap();
        let _ = db.sync();
        expected = rows_of(
            db.execute("SELECT id, v FROM t ORDER BY id, v")
                .await
                .unwrap(),
        );
    }
    // expected = [(1,10),(3,999),(5,50)]
    assert_eq!(
        expected,
        vec![(1, 10), (3, 999), (5, 50)],
        "pre-reopen state wrong"
    );

    let db = Database::builder().disk(&path).build().unwrap();
    let recovered = rows_of(
        db.execute("SELECT id, v FROM t ORDER BY id, v")
            .await
            .unwrap(),
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));

    assert_eq!(
        recovered, expected,
        "recovered state diverged from pre-reopen state"
    );
}
