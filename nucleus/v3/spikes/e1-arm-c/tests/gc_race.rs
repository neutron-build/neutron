//! Verdict X-3, rework items 1 and 2: `gc()` running concurrently with
//! writers — and with **itself** — must never lose a committed write.
//!
//! The gate's finding: every gc call used one fixed lock-table identity,
//! so one gc's `release_all` freed a key another gc was still deleting
//! under (re-acquire by the same owner is a no-op, L-NOOP), and a writer
//! that then acquired the key had its committed value deleted by the
//! other gc's stale re-check. The same shape also catches a tombstone
//! delete that skips the key lock entirely (the gate's GC-NO-LOCK
//! mutant): without the lock, a writer's apply can land between gc's
//! re-read and its delete, and the delete removes the live value.
//!
//! Shape (the verdict's): writers delete, put and read back their **own**
//! keys while two threads loop `gc()`. A committed put must be readable
//! forever after its commit returns (apply and `visible_ts` advance
//! before `commit` returns, C-COMMIT-ORDER), so the read-back retries
//! with fresh snapshots only until the commit-visibility window closes
//! and then must see the value; a deadline that expires, a wrong value,
//! or an engine error is a violation.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use e1_arm_c::ArmC;
use e1_mvcc::api::{Isolation, TxnEngine, Wait};

/// Writer threads (the verdict's repro used four).
const WRITERS: u64 = 4;

/// Keys each writer owns exclusively (no writer touches another's key,
/// so every conflict the test can see involves gc).
const KEYS_PER_WRITER: usize = 6;

/// How long the race runs before the writers and gc threads stop.
const RUN: Duration = Duration::from_millis(1500);

/// How long one read-back may keep retrying before the committed put is
/// declared lost. Correct code needs one read (the commit already
/// returned, so a fresh snapshot sees it); the retries only absorb
/// scheduler noise, never a loss.
const READ_BACK_TIMEOUT: Duration = Duration::from_secs(2);

/// A fresh scratch directory per test (std only; no new dependencies).
fn tmp_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "e1-arm-c-gc-race-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::create_dir_all(&d);
    d
}

/// Writer `w`'s `i`-th key (two bytes, distinct across writers).
fn key(w: u64, i: usize) -> Vec<u8> {
    vec![b'g', w as u8, i as u8]
}

/// The verdict's race: committed puts survive concurrent `gc()` calls.
#[test]
fn concurrent_gc_never_loses_committed_writes() {
    let dir = tmp_dir("race");
    let e = ArmC::new(1024, 8192, 64 << 20, &dir);
    let stop = AtomicBool::new(false);
    let gcs = AtomicU64::new(0);
    let rounds = AtomicU64::new(0);
    let read_backs = AtomicU64::new(0);
    let violations: Mutex<Vec<String>> = Mutex::new(Vec::new());
    thread::scope(|s| {
        // Two gc threads, looping: the concurrent-gc case needs two,
        // and the writer-vs-gc case is exercised by both.
        for _ in 0..2 {
            let e = &e;
            let stop = &stop;
            let gcs = &gcs;
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = e.gc();
                    gcs.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        for w in 0..WRITERS {
            let e = &e;
            let stop = &stop;
            let rounds = &rounds;
            let read_backs = &read_backs;
            let violations = &violations;
            s.spawn(move || {
                let mut round = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    round += 1;
                    // Phase 1 — tombstones for gc to find: delete every
                    // owned key, one txn each, so each delete commits on
                    // its own ts.
                    for i in 0..KEYS_PER_WRITER {
                        let k = key(w, i);
                        let mut t = e.begin(Isolation::ReadCommitted);
                        let r = e
                            .delete(&mut t, &k, Wait::Block(Duration::from_secs(5)))
                            .and_then(|_existed| e.commit(t));
                        if let Err(err) = r {
                            violations
                                .lock()
                                .unwrap()
                                .push(format!("writer {w} round {round}: delete {i}: {err}"));
                            return;
                        }
                    }
                    // Phase 2 — put every key back and read it back. The
                    // put races gc's deletes of the phase-1 tombstones;
                    // once its commit returns, the value must be durable
                    // against gc forever after.
                    for i in 0..KEYS_PER_WRITER {
                        let k = key(w, i);
                        let payload = format!("{w}-{round}-{i}").into_bytes();
                        let mut t = e.begin(Isolation::ReadCommitted);
                        let r = e
                            .put(&mut t, &k, &payload, Wait::Block(Duration::from_secs(5)))
                            .and_then(|()| e.commit(t));
                        if let Err(err) = r {
                            violations
                                .lock()
                                .unwrap()
                                .push(format!("writer {w} round {round}: put {i}: {err}"));
                            return;
                        }
                        read_backs.fetch_add(1, Ordering::Relaxed);
                        let deadline = Instant::now() + READ_BACK_TIMEOUT;
                        loop {
                            let mut r = e.begin(Isolation::ReadCommitted);
                            let got = e.get(&mut r, &k);
                            e.abort(r);
                            match got {
                                Ok(Some(v)) if v == payload => break,
                                Ok(other) => {
                                    if Instant::now() >= deadline {
                                        violations.lock().unwrap().push(format!(
                                            "writer {w} round {round}: committed put on key \
                                             {i} lost: read {other:?}, want {payload:?}"
                                        ));
                                        break;
                                    }
                                    thread::yield_now();
                                }
                                Err(err) => {
                                    violations.lock().unwrap().push(format!(
                                        "writer {w} round {round}: read-back {i}: {err}"
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                    rounds.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        thread::sleep(RUN);
        stop.store(true, Ordering::Release);
    });
    let found = violations.lock().unwrap().clone();
    assert!(
        found.is_empty(),
        "committed writes lost or engine errors under concurrent gc: {found:#?}"
    );
    // Non-vacuity: the race actually ran, on both sides.
    assert!(gcs.load(Ordering::Relaxed) > 10, "gc loops ran");
    assert!(
        rounds.load(Ordering::Relaxed) > 8,
        "writer rounds completed ({} rounds, {} read-backs)",
        rounds.load(Ordering::Relaxed),
        read_backs.load(Ordering::Relaxed)
    );
    let _ = std::fs::remove_dir_all(dir);
}
