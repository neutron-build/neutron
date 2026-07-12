//! Regression for the concurrent read-modify-write lost-update cluster
//! (probe_concurrency_threads). N OS threads each do BEGIN; SELECT v; UPDATE
//! v+1; COMMIT (retry on serialization error). After the dust settles the
//! committed value, read three independent ways (point lookup, full scan, SUM),
//! must equal the number of successful commits — no committed increment lost.
//!
//! Guards the three root causes found and fixed:
//!   - index point-lookup returning a stale `idx.map` row copy inside a txn
//!     (index_lookup_sync gated off for explicit txns; index_version_lookup
//!     defers to the chain scan),
//!   - non-atomic id-assignment vs snapshot capture in TransactionManager::begin
//!     (a txn observing an active set inconsistent with its own id),
//!   - non-atomic active→committed handoff in commit() (a committing txn briefly
//!     reported as aborted, flipping its rows' visibility mid-transaction).
#![cfg(feature = "server")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn run(
    ex: &Executor,
    r: &tokio::runtime::Runtime,
    sid: u64,
    sql: &str,
) -> Result<Vec<ExecResult>, String> {
    r.block_on(ex.execute_with_session(sid, sql))
        .map_err(|e| format!("{e}"))
}
fn int_of(res: Result<Vec<ExecResult>, String>) -> Option<i64> {
    let mut r = res.ok()?;
    match r.pop()? {
        ExecResult::Select { rows, .. } => match rows.first()?.first()? {
            Value::Int64(n) => Some(*n),
            Value::Int32(n) => Some(*n as i64),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn index_read_matches_fullscan_after_contention() {
    let mut divergences = 0;
    for round in 0..60 {
        let ex = Arc::new(Executor::new(
            Arc::new(Catalog::new()),
            Arc::new(MvccStorageAdapter::new()),
        ));
        let r0 = rt();
        let s0 = ex.create_session();
        run(
            &ex,
            &r0,
            s0,
            "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        )
        .unwrap();
        run(&ex, &r0, s0, "INSERT INTO counter (id,v) VALUES (1,0)").unwrap();
        ex.drop_session(s0);

        let workers = 4;
        let per = 5;
        let commits = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(workers));
        let mut hs = Vec::new();
        for _ in 0..workers {
            let ex = ex.clone();
            let commits = commits.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                let r = rt();
                let sid = ex.create_session();
                barrier.wait();
                for _ in 0..per {
                    loop {
                        if run(&ex, &r, sid, "BEGIN ISOLATION LEVEL REPEATABLE READ").is_err() {
                            let _ = run(&ex, &r, sid, "ROLLBACK");
                            continue;
                        }
                        let cur =
                            match int_of(run(&ex, &r, sid, "SELECT v FROM counter WHERE id=1")) {
                                Some(n) => n,
                                None => {
                                    let _ = run(&ex, &r, sid, "ROLLBACK");
                                    continue;
                                }
                            };
                        if run(
                            &ex,
                            &r,
                            sid,
                            &format!("UPDATE counter SET v={} WHERE id=1", cur + 1),
                        )
                        .is_err()
                        {
                            let _ = run(&ex, &r, sid, "ROLLBACK");
                            continue;
                        }
                        if run(&ex, &r, sid, "COMMIT").is_ok() {
                            commits.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        let _ = run(&ex, &r, sid, "ROLLBACK");
                    }
                }
                ex.drop_session(sid);
            }));
        }
        for h in hs {
            let _ = h.join();
        }
        let n = commits.load(Ordering::Relaxed) as i64;

        let rc = rt();
        let sc = ex.create_session();
        let point = int_of(run(&ex, &rc, sc, "SELECT v FROM counter WHERE id=1"));
        let full = int_of(run(&ex, &rc, sc, "SELECT v FROM counter"));
        // SUM forces an aggregate full scan, a third independent path.
        let summ = int_of(run(&ex, &rc, sc, "SELECT SUM(v) FROM counter"));
        ex.drop_session(sc);

        if point != Some(n) || full != Some(n) || summ != Some(n) {
            divergences += 1;
            if divergences <= 8 {
                println!("round {round}: commits={n} point={point:?} full={full:?} sum={summ:?}");
            }
        }
    }
    println!("TOTAL divergent rounds: {divergences}/60");
    assert_eq!(divergences, 0, "reads diverged from committed count");
}
