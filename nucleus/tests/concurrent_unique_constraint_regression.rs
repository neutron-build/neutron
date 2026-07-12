//! Regression for finding #7 (FIXED): PRIMARY KEY / UNIQUE constraints must be
//! enforced under concurrency. Previously N transactions concurrently INSERTing
//! the same key all succeeded (snapshot-based check_unique_constraints + insert
//! is not atomic across txns). Fixed with atomic, MVCC-aware unique enforcement
//! in the engine: insert_unique/update_unique check a committed-live chain AND an
//! in-flight reservation map (released on commit/abort) under one lock, so two
//! racing transactions can't both take the same key. See
//! src/storage/mvcc.rs (insert_unique / update_unique / release_unique).
#![cfg(feature = "server")]
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn concurrent_same_pk_insert_only_one_wins() {
    let mut dup_rounds = 0;
    for _round in 0..40u64 {
        let ex = Arc::new(Executor::new(
            Arc::new(Catalog::new()),
            Arc::new(MvccStorageAdapter::new()),
        ));
        let r0 = rt();
        let s0 = ex.create_session();
        let _ = r0.block_on(
            ex.execute_with_session(s0, "CREATE TABLE t (id INTEGER PRIMARY KEY, w INTEGER)"),
        );
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
                let _ = r.block_on(
                    ex.execute_with_session(sid, &format!("INSERT INTO t (id, w) VALUES (1, {w})")),
                );
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }
        let rc = rt();
        let sc = ex.create_session();
        let rows = match rc
            .block_on(ex.execute_with_session(sc, "SELECT id FROM t WHERE id=1"))
            .unwrap()
            .pop()
            .unwrap()
        {
            ExecResult::Select { rows, .. } => rows.len(),
            _ => 0,
        };
        ex.drop_session(sc);
        if rows != 1 {
            dup_rounds += 1;
        }
    }
    assert_eq!(
        dup_rounds, 0,
        "concurrent same-PK inserts produced duplicates in {dup_rounds}/40 rounds"
    );
}
