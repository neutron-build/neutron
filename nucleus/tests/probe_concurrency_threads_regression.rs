//! Regression coverage for the MVCC concurrency invariants exercised by the
//! `probe_concurrency_threads` fuzz harness. These tests isolate the two
//! confirmed findings into small deterministic-ish reproductions. They run
//! real OS threads, so they are inherently timing-sensitive; each loops enough
//! to surface the race reliably on a normal machine.
//!
//! Run: `cargo test --release --features "server" --test probe_concurrency_threads_regression`
#![cfg(feature = "server")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

fn ex() -> Arc<Executor> {
    Arc::new(Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new())))
}
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}
fn run(ex: &Executor, rt: &tokio::runtime::Runtime, sid: u64, sql: &str) -> Result<Vec<ExecResult>, String> {
    rt.block_on(ex.execute_with_session(sid, sql)).map_err(|e| format!("{e}"))
}
fn read1(ex: &Executor, rt: &tokio::runtime::Runtime, sid: u64, sql: &str) -> Option<i64> {
    let mut r = run(ex, rt, sid, sql).ok()?;
    if let Some(ExecResult::Select { rows, .. }) = r.pop() {
        match rows.first().and_then(|row| row.first()) {
            Some(Value::Int64(n)) => return Some(*n),
            Some(Value::Int32(n)) => return Some(*n as i64),
            _ => {}
        }
    }
    None
}

/// FINDING 1 (CONFIRMED REAL BUG): SERIALIZABLE read-modify-write via a point
/// predicate (`SELECT v FROM counter WHERE id=1`) loses updates. The optimized
/// equality read paths (`fast_scan_where_eq` / `scan_where_eq_positions`, taken
/// for `WHERE col = val`) NEVER call `maybe_record_siread`; only the full
/// `scan()` path records SIREAD locks. So SERIALIZABLE point reads are invisible
/// to SSI and the read-modify-write rw-antidependency goes undetected. Final
/// counter ends up < number of successful commits.
///
/// `#[ignore]` because it documents a *known failing* invariant; remove the
/// ignore once the bug is fixed to use it as a regression guard.
#[test]
#[ignore = "documents confirmed lost-update bug (point-read SIREAD gap); expected to FAIL until fixed"]
fn serializable_no_lost_update_point_read() {
    let select = "SELECT v FROM counter WHERE id=1"; // eq/index path — SIREAD not recorded
    assert_no_lost_update(select);
}

/// Control: the SAME pattern via a full-table scan (`SELECT v FROM counter`)
/// DOES record SIREAD locks (mvcc.rs scan() → maybe_record_siread) and should
/// NOT lose updates. Contrasting the two pins the bug to the eq read path.
#[test]
#[ignore = "timing-sensitive control for the point-read finding; run manually"]
fn serializable_no_lost_update_full_scan() {
    let select = "SELECT v FROM counter"; // full-scan path — records SIREAD
    assert_no_lost_update(select);
}

/// FINDING 2 (CONFIRMED REAL BUG, deterministic): SSI fails to detect a
/// write-skew/lost-update rw-antidependency against an ALREADY-COMMITTED
/// concurrent transaction. Root cause: when T1 commits, `cleanup_ssi(T1)`
/// removes T1's read AND write sets immediately. If a concurrent T2 then writes
/// the same row T1 had read (and had also written), neither `record_write` nor
/// `record_siread` can find T1's now-deleted sets, so NO rw-conflict edge is
/// created and T2's `commit_serializable` check passes — a classic SI write
/// skew / lost update that SERIALIZABLE must forbid.
///
/// Timeline (no contention needed — fully serialized here):
///   T1: BEGIN; read row (SIREAD[0]); UPDATE row (write[0]); COMMIT  → cleanup wipes T1 sets
///   T2: (started concurrently) UPDATE row (overwrites committed) ; COMMIT → NOT rejected → T1's update lost
#[test]
#[ignore = "documents confirmed SSI cleanup lost-update bug; expected to FAIL until fixed"]
fn serializable_detects_conflict_with_committed_txn() {
    let database = ex();
    let r0 = rt();
    let setup = database.create_session();
    run(&database, &r0, setup, "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)").unwrap();
    run(&database, &r0, setup, "INSERT INTO counter (id, v) VALUES (1, 10)").unwrap();
    database.drop_session(setup);

    let s1 = database.create_session();
    let s2 = database.create_session();
    let r1 = rt();
    let r2 = rt();

    // Both start concurrently and both read v (=10) via FULL scan (SIREAD recorded).
    run(&database, &r1, s1, "BEGIN ISOLATION LEVEL SERIALIZABLE").unwrap();
    run(&database, &r2, s2, "BEGIN ISOLATION LEVEL SERIALIZABLE").unwrap();
    let v1 = read1(&database, &r1, s1, "SELECT v FROM counter").unwrap();
    let v2 = read1(&database, &r2, s2, "SELECT v FROM counter").unwrap();
    assert_eq!(v1, 10);
    assert_eq!(v2, 10);

    // T1 increments and COMMITs first (this triggers cleanup_ssi(T1)).
    run(&database, &r1, s1, &format!("UPDATE counter SET v={} WHERE id=1", v1 + 1)).unwrap();
    run(&database, &r1, s1, "COMMIT").unwrap();

    // T2 (still open) now increments based on its stale read of 10 and COMMITs.
    // Under SERIALIZABLE this MUST fail (write skew / lost update); both txns
    // read 10 and both write 11, so one increment would be lost.
    let upd = run(&database, &r2, s2, &format!("UPDATE counter SET v={} WHERE id=1", v2 + 1));
    let commit = if upd.is_ok() {
        run(&database, &r2, s2, "COMMIT")
    } else {
        Err("update already conflicted".into())
    };

    let r3 = rt();
    let s3 = database.create_session();
    let final_v = read1(&database, &r3, s3, "SELECT v FROM counter WHERE id=1").unwrap();
    database.drop_session(s1);
    database.drop_session(s2);
    database.drop_session(s3);

    // Either T2 was rejected (final stays 11) — correct — or the engine must not
    // have silently committed a lost update. A clean T2 COMMIT with final==11 is
    // the BUG: two +1 increments collapsed into one.
    let t2_committed_cleanly = commit.is_ok();
    assert!(
        !(t2_committed_cleanly && final_v == 11),
        "SSI MISSED conflict with committed txn: both read 10, both wrote 11, T2 committed cleanly, \
         final={final_v} (expected rejection of T2 or a serialized final of 12)"
    );
}

/// FINDING 3 (code-confirmed, hard to trigger at runtime): on a SERIALIZABLE
/// COMMIT that fails the SSI check, `Executor::commit_transaction` propagates
/// the storage error via `?` BEFORE clearing `txn.active`/`txn.snapshot`. The
/// storage layer already aborted and cleared its own session txn, so the
/// executor session is left believing it is still in a transaction while
/// storage is not — a state desync. The next `BEGIN` then no-ops with a
/// warning and subsequent statements run as un-rolled-back auto-commits.
/// (Difficult to exercise deterministically because Finding 2 means COMMIT
/// rarely actually fails; documented here from `src/executor/txn.rs:76`.)
#[test]
#[ignore = "code-level finding (txn.rs:76 early-return before clearing txn.active); see notes"]
fn commit_failure_does_not_desync_session_state() {
    // Placeholder guard: kept ignored. The bug is in the COMMIT error path and
    // is established by inspection; once Finding 2 is fixed this can be made to
    // fire by forcing an SSI abort at COMMIT and asserting !session_in_transaction.
}

fn assert_no_lost_update(select: &str) {
    let workers = 4usize;
    let per = 8usize;
    let database = ex();
    {
        let r = rt();
        let s = database.create_session();
        run(&database, &r, s, "CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)").unwrap();
        run(&database, &r, s, "INSERT INTO counter (id, v) VALUES (1, 0)").unwrap();
        database.drop_session(s);
    }
    let commits = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(workers));
    let mut hs = Vec::new();
    for _ in 0..workers {
        let database = database.clone();
        let commits = commits.clone();
        let barrier = barrier.clone();
        let select = select.to_string();
        hs.push(std::thread::spawn(move || {
            let r = rt();
            let s = database.create_session();
            barrier.wait();
            for _ in 0..per {
                loop {
                    if run(&database, &r, s, "BEGIN ISOLATION LEVEL SERIALIZABLE").is_err() {
                        let _ = run(&database, &r, s, "ROLLBACK");
                        continue;
                    }
                    let cur = match read1(&database, &r, s, &select) {
                        Some(n) => n,
                        None => { let _ = run(&database, &r, s, "ROLLBACK"); continue; }
                    };
                    if run(&database, &r, s, &format!("UPDATE counter SET v={} WHERE id=1", cur + 1)).is_err() {
                        let _ = run(&database, &r, s, "ROLLBACK");
                        continue;
                    }
                    if run(&database, &r, s, "COMMIT").is_ok() {
                        commits.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    let _ = run(&database, &r, s, "ROLLBACK");
                }
            }
            database.drop_session(s);
        }));
    }
    for h in hs { h.join().unwrap(); }
    let n = commits.load(Ordering::Relaxed);
    let r = rt();
    let s = database.create_session();
    let final_v = read1(&database, &r, s, "SELECT v FROM counter WHERE id=1").unwrap();
    database.drop_session(s);
    assert_eq!(final_v, n as i64, "lost update: final={final_v} committed={n} (select via `{select}`)");
}
