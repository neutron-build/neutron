//! Regression for `snapshot_count`: `COUNT(*)` (and other aggregates) issued
//! inside an explicit transaction must respect the executing transaction's MVCC
//! snapshot, exactly like `SELECT *` does. This matches PostgreSQL's snapshot /
//! REPEATABLE READ semantics (Nucleus runs explicit txns at IsolationLevel::Snapshot).
//!
//! Two properties must hold inside a `BEGIN ... <no COMMIT yet>` block:
//!   1. read-your-own-writes — after INSERT/DELETE in the same txn, `COUNT(*)`
//!      reflects the uncommitted changes.
//!   2. repeatable read — `COUNT(*)` must NOT change because another session
//!      COMMITted after this txn took its snapshot.
//!
//! Bug: the executor's O(1) fast-count paths called the snapshot-unaware
//! process-global committed counter (MvccStorageAdapter::fast_count_all), so
//! COUNT(*) disagreed with SELECT * inside a transaction. The fix gates those
//! fast paths off when the session is in an explicit txn on an MVCC engine,
//! falling back to the snapshot-correct SeqScan+Aggregate.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::*;
use nucleus::types::Value;
use std::sync::Arc;

fn make_executor() -> Arc<Executor> {
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    Arc::new(Executor::new(Arc::new(Catalog::new()), storage))
}

/// Run SQL in a session and return the scalar i64 of the first cell.
async fn count_in(ex: &Executor, sid: u64) -> i64 {
    let res = ex
        .execute_with_session(sid, "SELECT COUNT(*) FROM t")
        .await
        .unwrap();
    match res.into_iter().next().unwrap() {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(n) => *n,
            Value::Int32(n) => *n as i64,
            other => panic!("unexpected COUNT(*) value: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

async fn run(ex: &Executor, sid: u64, sql: &str) {
    ex.execute_with_session(sid, sql).await.unwrap();
}

/// read-your-own-writes: COUNT(*) inside a txn must see the txn's own
/// uncommitted INSERT (and DELETE), agreeing with SELECT * cardinality.
#[tokio::test]
async fn count_reflects_uncommitted_dml_in_txn() {
    let ex = make_executor();
    let s = ex.create_session();
    run(&ex, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await;
    run(&ex, s, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)").await;

    run(&ex, s, "BEGIN").await;
    // Two more uncommitted inserts inside the txn.
    run(&ex, s, "INSERT INTO t (id, v) VALUES (3, 30), (4, 40)").await;
    assert_eq!(
        count_in(&ex, s).await,
        4,
        "COUNT(*) must read its own uncommitted inserts"
    );

    // An uncommitted delete inside the same txn.
    run(&ex, s, "DELETE FROM t WHERE id = 1").await;
    assert_eq!(
        count_in(&ex, s).await,
        3,
        "COUNT(*) must read its own uncommitted delete"
    );

    run(&ex, s, "COMMIT").await;
    assert_eq!(
        count_in(&ex, s).await,
        3,
        "post-commit COUNT(*) reflects committed state"
    );
    ex.drop_session(s);
}

/// repeatable read: session A holds an open txn; session B commits an insert
/// after A's snapshot was taken. A's repeated COUNT(*) must be unchanged.
#[tokio::test]
async fn count_repeatable_read_ignores_concurrent_commit() {
    let ex = make_executor();
    let a = ex.create_session();
    let b = ex.create_session();
    run(&ex, a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await;
    run(&ex, a, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)").await;

    // A opens a txn and takes its snapshot via a first read.
    run(&ex, a, "BEGIN").await;
    let before = count_in(&ex, a).await;
    assert_eq!(before, 2);

    // B (autocommit) inserts and commits a new row after A's snapshot.
    run(&ex, b, "INSERT INTO t (id, v) VALUES (3, 30)").await;

    // A re-reads: under snapshot isolation the count must be unchanged.
    let after = count_in(&ex, a).await;
    assert_eq!(
        after, before,
        "A's COUNT(*) must not change due to B's concurrent commit (repeatable read)"
    );

    run(&ex, a, "COMMIT").await;
    // After committing, A is in autocommit and sees B's row.
    assert_eq!(count_in(&ex, a).await, 3);

    ex.drop_session(a);
    ex.drop_session(b);
}

/// Autocommit COUNT(*) (no explicit txn) still works and reflects committed
/// state — the fast path is untouched outside transactions.
#[tokio::test]
async fn count_autocommit_unchanged() {
    let ex = make_executor();
    let s = ex.create_session();
    run(&ex, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await;
    run(
        &ex,
        s,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    assert_eq!(count_in(&ex, s).await, 3);
    ex.drop_session(s);
}
