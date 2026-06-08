//! OPEN BUG (pinned, #[ignore]'d): inside an explicit transaction,
//! `UPDATE/DELETE ... WHERE indexed_col = X` on a MULTI-ROW table can hit the
//! wrong row. The MVCC PK/eq fast path (scan_where_eq_positions) returns
//! positions relative to the MATCH list (virtual), but the adapter's
//! update()/delete() map them against the full `engine.scan(snap)`; they only
//! agree when the per-session scan cache holds the matches, which is populated
//! for auto-commit but NOT inside a transaction. So a single match at virtual
//! position 0 lands on physical row 0.
//!
//! The obvious fix — cache the matches for transactions too — was tried and
//! REVERTED: it exposed a deeper latent issue under concurrency. The index path
//! (index_version_lookup, newest-visible) and the chain path (engine.scan) can
//! disagree on which version of a PK is visible to the same snapshot (observed
//! cached_vidx != fresh_vidx), so consuming the cached (index) version in
//! update() followed the conflict-unsafe version and reintroduced concurrent
//! lost updates. Fixing this correctly needs reconciling index vs chain MVCC
//! visibility (only one version per key per snapshot) before the position fix is
//! safe — careful work, not a quick patch. Tracked in task #24.
#![allow(dead_code)]
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
#[ignore = "OPEN BUG: multi-row UPDATE/DELETE WHERE indexed_col=X in a txn can hit the wrong row (see file header, task #24)"]
async fn txn_update_where_eq_hits_correct_row() {
    let ex = Arc::new(Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    ));
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)").await.unwrap();
    ex.execute_with_session(s, "INSERT INTO t VALUES (1,10),(2,20),(3,30)").await.unwrap();

    // UPDATE in a transaction, targeting the middle and last rows by PK.
    ex.execute_with_session(s, "BEGIN").await.unwrap();
    ex.execute_with_session(s, "UPDATE t SET v=222 WHERE id=2").await.unwrap();
    ex.execute_with_session(s, "UPDATE t SET v=333 WHERE id=3").await.unwrap();
    ex.execute_with_session(s, "COMMIT").await.unwrap();

    assert_eq!(
        rows(&ex, s, "SELECT id, v FROM t ORDER BY id").await,
        vec![(1, 10), (2, 222), (3, 333)],
        "UPDATE WHERE id=N must change row N only"
    );

    // DELETE in a transaction, targeting a non-first row by PK.
    ex.execute_with_session(s, "BEGIN").await.unwrap();
    ex.execute_with_session(s, "DELETE FROM t WHERE id=2").await.unwrap();
    ex.execute_with_session(s, "COMMIT").await.unwrap();

    assert_eq!(
        rows(&ex, s, "SELECT id, v FROM t ORDER BY id").await,
        vec![(1, 10), (3, 333)],
        "DELETE WHERE id=2 must remove only row 2"
    );

    // A read-only point SELECT before a non-PK-eq UPDATE must not poison it.
    ex.execute_with_session(s, "BEGIN").await.unwrap();
    let _ = rows(&ex, s, "SELECT id, v FROM t WHERE id=1").await; // populates scan cache
    ex.execute_with_session(s, "UPDATE t SET v=v+1 WHERE v >= 0").await.unwrap(); // non-fast path
    ex.execute_with_session(s, "COMMIT").await.unwrap();

    assert_eq!(
        rows(&ex, s, "SELECT id, v FROM t ORDER BY id").await,
        vec![(1, 11), (3, 334)],
        "non-fast UPDATE after a point SELECT must update all matching rows correctly"
    );
    ex.drop_session(s);
}
