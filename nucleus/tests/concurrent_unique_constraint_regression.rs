//! OPEN BUG (pinned, #[ignore]'d): PRIMARY KEY / UNIQUE constraints are NOT
//! enforced under concurrency. N transactions that concurrently INSERT the same
//! key all succeed, leaving duplicate rows — found by
//! concurrency_schema_constraints_probe.
//!
//! Root cause: uniqueness is checked in the executor (check_unique_constraints)
//! via a snapshot scan, then the row is inserted — the check and the insert are
//! not atomic across transactions, and each concurrent inserter's snapshot shows
//! no duplicate (the others are uncommitted), so they all pass and all insert.
//!
//! A correct fix needs atomic, MVCC-aware unique enforcement in the storage
//! engine (a unique index that detects a conflicting committed OR concurrently-
//! uncommitted entry at insert time, reclaims keys on abort, and tracks key
//! changes on update/delete) — a real concurrency feature, not a quick patch.
//! Tracked in tier_findings_open.rs.
#![cfg(feature = "server")]
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
#[ignore = "OPEN BUG: PK/UNIQUE not enforced under concurrent inserts (see tier_findings_open.rs)"]
fn concurrent_same_pk_insert_only_one_wins() {
    let mut dup_rounds = 0;
    for _round in 0..40u64 {
        let ex = Arc::new(Executor::new(
            Arc::new(Catalog::new()),
            Arc::new(MvccStorageAdapter::new()),
        ));
        let r0 = rt();
        let s0 = ex.create_session();
        let _ = r0.block_on(ex.execute_with_session(s0, "CREATE TABLE t (id INTEGER PRIMARY KEY, w INTEGER)"));
        ex.drop_session(s0);

        let n = 4;
        let barrier = Arc::new(Barrier::new(n));
        let mut hs = Vec::new();
        for w in 0..n {
            let ex = ex.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                barrier.wait();
                // every worker tries to insert the SAME pk=1 (auto-commit)
                let _ = r.block_on(ex.execute_with_session(sid, &format!("INSERT INTO t (id, w) VALUES (1, {w})")));
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }
        let rc = rt();
        let sc = ex.create_session();
        let rows = match rc.block_on(ex.execute_with_session(sc, "SELECT id FROM t WHERE id=1")).unwrap().pop().unwrap() {
            ExecResult::Select { rows, .. } => rows.len(),
            _ => 0,
        };
        ex.drop_session(sc);
        if rows != 1 {
            dup_rounds += 1;
        }
    }
    assert_eq!(dup_rounds, 0, "concurrent same-PK inserts produced duplicates in {dup_rounds}/40 rounds");
}
