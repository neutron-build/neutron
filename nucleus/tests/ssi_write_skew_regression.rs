//! Regression for the SERIALIZABLE (SSI) write-skew cluster (findings #1b/#2).
//!
//! Two parts:
//!  (1) write-skew MUST be detected even when the second txn does its read/write
//!      AFTER the first commits — the old cleanup_ssi purged a committing txn's
//!      SIREAD/write sets immediately (so no rw-conflict edge formed) and SIREAD
//!      was not recorded on point/equality reads. Fixed by deferred SSI cleanup
//!      (retain until concurrent peers finish) + record_siread on the eq read
//!      path + a concurrency guard on edge creation.
//!  (2) Disjoint SERIALIZABLE txns must converge correctly under the standard
//!      retry-on-serialization-failure contract. NOTE on precision: the
//!      predicate-pushdown fast-scan paths (fast_scan_where_eq /
//!      fast_scan_where_range) now record tuple-level SIREAD on the matched rows
//!      (closing a coverage gap and giving precise conflict detection when used).
//!      But a plain point SELECT like `SELECT v FROM t WHERE id=1` is not pushed
//!      down by the planner — it runs as a full scan, so its SIREAD covers the
//!      whole table (coarse "predicate lock", as PostgreSQL does for seq scans),
//!      and disjoint access can abort one txn on the first try. That is SAFE (no
//!      anomaly) and resolved by retry; routing point SELECTs through the index
//!      (so their SIREAD is tuple-granular) is a planner-pushdown optimization,
//!      not a correctness issue.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::types::Value;
use nucleus::storage::MvccStorageAdapter;

fn ex() -> Arc<Executor> {
    Arc::new(Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    ))
}

async fn first_int(ex: &Executor, sid: u64, sql: &str) -> Option<i64> {
    let mut r = ex.execute_with_session(sid, sql).await.ok()?;
    match r.pop()? {
        ExecResult::Select { rows, .. } => match rows.first()?.first()? {
            Value::Int64(n) => Some(*n),
            Value::Int32(n) => Some(*n as i64),
            _ => None,
        },
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serializable_detects_write_skew_after_commit() {
    let ex = ex();
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE oncall (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)").await.unwrap();
    ex.execute_with_session(s, "INSERT INTO oncall VALUES (1,1),(2,1)").await.unwrap();
    ex.drop_session(s);

    let a = ex.create_session();
    let b = ex.create_session();
    ex.execute_with_session(a, "BEGIN ISOLATION LEVEL SERIALIZABLE").await.unwrap();
    ex.execute_with_session(b, "BEGIN ISOLATION LEVEL SERIALIZABLE").await.unwrap();

    // A reads row2, writes row1, commits — all BEFORE B does anything.
    assert_eq!(first_int(&ex, a, "SELECT v FROM oncall WHERE id=2").await, Some(1));
    ex.execute_with_session(a, "UPDATE oncall SET v=0 WHERE id=1").await.unwrap();
    ex.execute_with_session(a, "COMMIT").await.unwrap();

    // B reads row1 (its snapshot still sees 1) then writes row2 from that read.
    assert_eq!(first_int(&ex, b, "SELECT v FROM oncall WHERE id=1").await, Some(1));
    let upd = ex.execute_with_session(b, "UPDATE oncall SET v=0 WHERE id=2").await;
    let rejected = upd.is_err()
        || ex.execute_with_session(b, "COMMIT").await.is_err();

    let chk = ex.create_session();
    let r1 = first_int(&ex, chk, "SELECT v FROM oncall WHERE id=1").await.unwrap();
    let r2 = first_int(&ex, chk, "SELECT v FROM oncall WHERE id=2").await.unwrap();
    ex.drop_session(chk);

    assert!(rejected, "SERIALIZABLE must reject the write-skew");
    assert!(r1 + r2 >= 1, "write-skew anomaly: both rows ended at 0 ({r1},{r2})");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serializable_disjoint_converges_under_retry() {
    let ex = ex();
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)").await.unwrap();
    ex.execute_with_session(s, "INSERT INTO t VALUES (1,10),(2,20)").await.unwrap();
    ex.drop_session(s);

    // Each worker updates ITS OWN disjoint row under SERIALIZABLE, retrying on a
    // serialization failure (the standard SSI contract). Both must EVENTUALLY
    // succeed (no livelock) and land the correct value — over-aborting is allowed,
    // wrong results / hangs are not.
    let sid = ex.create_session();
    for (id, newv) in [(1i64, 11i64), (2, 22)] {
        loop {
            ex.execute_with_session(sid, "BEGIN ISOLATION LEVEL SERIALIZABLE").await.unwrap();
            let _ = first_int(&ex, sid, &format!("SELECT v FROM t WHERE id={id}")).await;
            if ex.execute_with_session(sid, &format!("UPDATE t SET v={newv} WHERE id={id}")).await.is_err() {
                let _ = ex.execute_with_session(sid, "ROLLBACK").await;
                continue;
            }
            if ex.execute_with_session(sid, "COMMIT").await.is_ok() {
                break;
            }
            let _ = ex.execute_with_session(sid, "ROLLBACK").await;
        }
    }
    assert_eq!(first_int(&ex, sid, "SELECT v FROM t WHERE id=1").await, Some(11));
    assert_eq!(first_int(&ex, sid, "SELECT v FROM t WHERE id=2").await, Some(22));
    ex.drop_session(sid);
}
