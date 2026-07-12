//! Regression for the SERIALIZABLE (SSI) write-skew cluster (findings #1b/#2).
//!
//! Two parts:
//!  (1) write-skew MUST be detected even when the second txn does its read/write
//!      AFTER the first commits — the old cleanup_ssi purged a committing txn's
//!      SIREAD/write sets immediately (so no rw-conflict edge formed) and SIREAD
//!      was not recorded on point/equality reads. Fixed by deferred SSI cleanup
//!      (retain until concurrent peers finish) + record_siread on the eq read
//!      path + a concurrency guard on edge creation.
//!  (2) DISJOINT SERIALIZABLE txns (each touching only its own row by PK) must
//!      BOTH commit — no spurious serialization failure. This requires precise
//!      tuple-level SIREAD: point reads/writes record SIREAD only on the matched
//!      row (fast_scan_where_eq / scan_where_eq_positions), single-table
//!      unqualified predicates are pushed down so point SELECTs use the fast scan,
//!      and internal maintenance scans (zone-map rebuild) use a SIREAD-free scan
//!      so they don't pollute the read set with the whole table.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

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
    ex.execute_with_session(
        s,
        "CREATE TABLE oncall (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
    )
    .await
    .unwrap();
    ex.execute_with_session(s, "INSERT INTO oncall VALUES (1,1),(2,1)")
        .await
        .unwrap();
    ex.drop_session(s);

    let a = ex.create_session();
    let b = ex.create_session();
    ex.execute_with_session(a, "BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();
    ex.execute_with_session(b, "BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();

    // A reads row2, writes row1, commits — all BEFORE B does anything.
    assert_eq!(
        first_int(&ex, a, "SELECT v FROM oncall WHERE id=2").await,
        Some(1)
    );
    ex.execute_with_session(a, "UPDATE oncall SET v=0 WHERE id=1")
        .await
        .unwrap();
    ex.execute_with_session(a, "COMMIT").await.unwrap();

    // B reads row1 (its snapshot still sees 1) then writes row2 from that read.
    assert_eq!(
        first_int(&ex, b, "SELECT v FROM oncall WHERE id=1").await,
        Some(1)
    );
    let upd = ex
        .execute_with_session(b, "UPDATE oncall SET v=0 WHERE id=2")
        .await;
    let rejected = upd.is_err() || ex.execute_with_session(b, "COMMIT").await.is_err();

    let chk = ex.create_session();
    let r1 = first_int(&ex, chk, "SELECT v FROM oncall WHERE id=1")
        .await
        .unwrap();
    let r2 = first_int(&ex, chk, "SELECT v FROM oncall WHERE id=2")
        .await
        .unwrap();
    ex.drop_session(chk);

    assert!(rejected, "SERIALIZABLE must reject the write-skew");
    assert!(
        r1 + r2 >= 1,
        "write-skew anomaly: both rows ended at 0 ({r1},{r2})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serializable_disjoint_both_commit() {
    let ex = ex();
    let s = ex.create_session();
    ex.execute_with_session(
        s,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
    )
    .await
    .unwrap();
    ex.execute_with_session(s, "INSERT INTO t VALUES (1,10),(2,20)")
        .await
        .unwrap();
    ex.drop_session(s);

    let a = ex.create_session();
    let b = ex.create_session();
    ex.execute_with_session(a, "BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();
    ex.execute_with_session(b, "BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .unwrap();

    // Disjoint rows by PK: A touches id=1, B touches id=2 — no rw-antidependency
    // cycle, so BOTH must commit on the first try (precise tuple-level SIREAD).
    assert_eq!(
        first_int(&ex, a, "SELECT v FROM t WHERE id=1").await,
        Some(10)
    );
    assert_eq!(
        first_int(&ex, b, "SELECT v FROM t WHERE id=2").await,
        Some(20)
    );
    ex.execute_with_session(a, "UPDATE t SET v=11 WHERE id=1")
        .await
        .unwrap();
    ex.execute_with_session(b, "UPDATE t SET v=22 WHERE id=2")
        .await
        .unwrap();

    assert!(
        ex.execute_with_session(a, "COMMIT").await.is_ok(),
        "A must commit"
    );
    assert!(
        ex.execute_with_session(b, "COMMIT").await.is_ok(),
        "B must commit — disjoint access must NOT be a spurious serialization failure"
    );

    let chk = ex.create_session();
    assert_eq!(
        first_int(&ex, chk, "SELECT v FROM t WHERE id=1").await,
        Some(11)
    );
    assert_eq!(
        first_int(&ex, chk, "SELECT v FROM t WHERE id=2").await,
        Some(22)
    );
    ex.drop_session(chk);
}
