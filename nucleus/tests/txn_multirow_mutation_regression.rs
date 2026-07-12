//! Regression: inside an explicit transaction, `UPDATE/DELETE ... WHERE
//! indexed_col = X` on a MULTI-ROW table must hit the row matching X, not the
//! first physical row.
//!
//! Root cause (fixed): the MVCC scan methods returned scan-order positions and
//! the adapter's update()/delete() re-mapped them against a fresh scan, which
//! could disagree with what row-finding matched (and a per-session scan cache
//! that would reconcile them was populated only for auto-commit). The fix makes
//! the MVCC engine's positions BE stable version indices end-to-end:
//! scan_where_eq_positions / scan_physical / index_version_lookup return
//! (version_idx, row), and update()/delete() mutate exactly that version (no
//! re-scan, no scan cache). This also removes the index-vs-chain visibility
//! divergence that the earlier cache attempt hit.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

async fn rows(ex: &Executor, sid: u64, sql: &str) -> Vec<(i64, i64)> {
    let mut r = ex.execute_with_session(sid, sql).await.unwrap();
    match r.pop().unwrap() {
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
        _ => vec![],
    }
}

#[tokio::test]
async fn txn_update_where_eq_hits_correct_row() {
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
    ex.execute_with_session(s, "INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .await
        .unwrap();

    // UPDATE in a transaction, targeting the middle and last rows by PK.
    ex.execute_with_session(s, "BEGIN").await.unwrap();
    ex.execute_with_session(s, "UPDATE t SET v=222 WHERE id=2")
        .await
        .unwrap();
    ex.execute_with_session(s, "UPDATE t SET v=333 WHERE id=3")
        .await
        .unwrap();
    ex.execute_with_session(s, "COMMIT").await.unwrap();

    assert_eq!(
        rows(&ex, s, "SELECT id, v FROM t ORDER BY id").await,
        vec![(1, 10), (2, 222), (3, 333)],
        "UPDATE WHERE id=N must change row N only"
    );

    // DELETE in a transaction, targeting a non-first row by PK.
    ex.execute_with_session(s, "BEGIN").await.unwrap();
    ex.execute_with_session(s, "DELETE FROM t WHERE id=2")
        .await
        .unwrap();
    ex.execute_with_session(s, "COMMIT").await.unwrap();

    assert_eq!(
        rows(&ex, s, "SELECT id, v FROM t ORDER BY id").await,
        vec![(1, 10), (3, 333)],
        "DELETE WHERE id=2 must remove only row 2"
    );

    // A read-only point SELECT before a non-PK-eq UPDATE must not poison it.
    ex.execute_with_session(s, "BEGIN").await.unwrap();
    let _ = rows(&ex, s, "SELECT id, v FROM t WHERE id=1").await; // populates scan cache
    ex.execute_with_session(s, "UPDATE t SET v=v+1 WHERE v >= 0")
        .await
        .unwrap(); // non-fast path
    ex.execute_with_session(s, "COMMIT").await.unwrap();

    assert_eq!(
        rows(&ex, s, "SELECT id, v FROM t ORDER BY id").await,
        vec![(1, 11), (3, 334)],
        "non-fast UPDATE after a point SELECT must update all matching rows correctly"
    );
    ex.drop_session(s);
}
