//! Regression for finding #4: READ COMMITTED must take a FRESH snapshot per
//! statement, so a statement sees rows committed by other transactions since the
//! previous statement. REPEATABLE READ / SNAPSHOT must NOT (snapshot fixed at
//! BEGIN). Previously RC never refreshed and behaved as snapshot (stricter than
//! spec); now execute_statement refreshes the RC snapshot per data statement.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

async fn v(ex: &Executor, sid: u64) -> i64 {
    let mut r = ex
        .execute_with_session(sid, "SELECT v FROM t WHERE id=1")
        .await
        .unwrap();
    match r.pop().unwrap() {
        ExecResult::Select { rows, .. } => match rows[0][0] {
            Value::Int64(n) => n,
            Value::Int32(n) => n as i64,
            _ => panic!("non-int"),
        },
        o => panic!("{o:?}"),
    }
}

async fn setup() -> (Arc<Executor>, u64) {
    let ex = Arc::new(Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    ));
    let s = ex.create_session();
    ex.execute_with_session(
        s,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
    )
    .await
    .unwrap();
    ex.execute_with_session(s, "INSERT INTO t VALUES (1,0)")
        .await
        .unwrap();
    ex.drop_session(s);
    (ex.clone(), 0)
}

#[tokio::test]
async fn read_committed_sees_concurrent_commit_next_statement() {
    let (ex, _) = setup().await;
    let r = ex.create_session();
    let w = ex.create_session();

    ex.execute_with_session(r, "BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .unwrap();
    assert_eq!(v(&ex, r).await, 0, "first read sees 0");

    // Another transaction commits a new value.
    ex.execute_with_session(w, "UPDATE t SET v=5 WHERE id=1")
        .await
        .unwrap();

    // READ COMMITTED: the NEXT statement must see the committed value.
    assert_eq!(
        v(&ex, r).await,
        5,
        "RC must see the concurrent commit on the next statement"
    );
    ex.execute_with_session(r, "COMMIT").await.unwrap();
}

#[tokio::test]
async fn repeatable_read_does_not_see_concurrent_commit() {
    let (ex, _) = setup().await;
    let r = ex.create_session();
    let w = ex.create_session();

    ex.execute_with_session(r, "BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    assert_eq!(v(&ex, r).await, 0, "first read sees 0");

    ex.execute_with_session(w, "UPDATE t SET v=5 WHERE id=1")
        .await
        .unwrap();

    // REPEATABLE READ: snapshot is fixed at BEGIN — must still see 0.
    assert_eq!(
        v(&ex, r).await,
        0,
        "RR must NOT see the concurrent commit (fixed snapshot)"
    );
    ex.execute_with_session(r, "COMMIT").await.unwrap();
}
