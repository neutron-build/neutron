//! Card X-3 mutation evidence WRITE-BEFORE-UNDO: the binding order of
//! commit apply (Work 5 — push the undo record, **then** write the tree
//! value) probed by a reader loop concurrent with writers.
//!
//! FLAKE-FIX-1: the coordination is a **counted window handshake**, not
//! wall-clock luck. The engine's apply-window gate
//! ([`ArmC::arm_apply_window_gate`], companion to the apply-yield hook)
//! holds each apply window open until the reader has completed
//! `SAMPLES_PER_WINDOW` whole sample passes **inside that window**, so
//! coverage is exact and structural — a function of the handshake, not
//! of which thread the host happened to schedule (the old
//! `n_samples > 100` wall-clock bound is gone). Windows on one key
//! serialise on the key's row lock (`commit_with` holds it through
//! apply) and the reader's `get`/`scan` take no row lock, so the
//! handshake cannot deadlock: the writer spins in the gate inside its
//! own commit, the reader keeps sampling the half-applied window.
//!
//! Under the correct order a reader at an old snapshot always resolves
//! through existing undo records and sees exactly its snapshot's value;
//! under the swapped order (tree value first, undo record after) the
//! reader samples the broken interval in the very first held window and
//! reports it — an engine error or a wrong value surfacing the broken
//! AC-UNDO-FIRST order, never a quiet wrong answer.
//!
//! The same property is what S11's readers exercise for atomic
//! visibility; this test aims the reader squarely at the apply window.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use e1_arm_c::ArmC;
use e1_mvcc::api::{Isolation, TxnEngine, Wait};

/// Commits per writer thread (FLAKE-FIX-1: was 250 — coverage is now
/// guaranteed per window by the handshake, so bulk buys nothing).
const COMMITS: u64 = 40;

/// Sample passes the reader must complete inside each apply window
/// before the window may close (the counted handshake).
const SAMPLES_PER_WINDOW: u64 = 4;

/// A fresh scratch directory per test (std only; no new dependencies).
fn tmp_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "e1-arm-c-order-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::create_dir_all(&d);
    d
}

/// The reader loop must never see a value newer than its snapshot — nor
/// a missing or different one: an RR reader that began before the
/// writers always reads `v0`, through every apply window.
#[test]
fn readers_never_see_past_their_snapshot_during_apply() {
    let dir = tmp_dir("race");
    let e = ArmC::new(256, 8192, 64 << 20, &dir);
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        e.put(&mut seed, b"k", b"v0", Wait::NoWait).unwrap();
        e.commit(seed).unwrap(); // ts 1: the reader's snapshot
    }
    // Arm both hooks only after the seed: the seed's apply would be a
    // window too (and the handshake counts exactly 2·COMMITS of them).
    e.arm_apply_yield();
    let samples = Arc::new(AtomicU64::new(0));
    let windows_opened = Arc::new(AtomicU64::new(0));
    let reader_stopped = Arc::new(AtomicBool::new(false));
    {
        let samples = Arc::clone(&samples);
        let windows_opened = Arc::clone(&windows_opened);
        let reader_stopped = Arc::clone(&reader_stopped);
        e.arm_apply_window_gate(Arc::new(move || {
            // Inside one apply window. The committing txn holds the
            // key's row lock through apply, so windows on `k` serialise
            // and this count is exact: hold the window open until the
            // reader has completed SAMPLES_PER_WINDOW whole passes
            // counted from the window's opening — unless the reader has
            // stopped, which must release every gate.
            let s0 = samples.load(Ordering::Acquire);
            windows_opened.fetch_add(1, Ordering::Release);
            while samples.load(Ordering::Acquire) < s0 + SAMPLES_PER_WINDOW
                && !reader_stopped.load(Ordering::Acquire)
            {
                thread::yield_now();
            }
        }));
    }
    let mut reader = e.begin(Isolation::RepeatableRead);
    assert_eq!(e.get(&mut reader, b"k").unwrap(), Some(b"v0".to_vec()));

    let done = AtomicBool::new(false);
    let pending = AtomicU64::new(2);
    let violations: Mutex<Vec<String>> = Mutex::new(Vec::new());
    thread::scope(|s| {
        // Two writers with distinguishable values: 300_000·w + i. The
        // Block wait is a test argument (FLAKE-FIX-1: 120 s) — a writer
        // only ever waits behind the other writer's held window, which
        // the handshake closes as soon as the reader samples it.
        for w in 1..=2u64 {
            let e = &e;
            let done = &done;
            let pending = &pending;
            let violations = &violations;
            s.spawn(move || {
                for i in 0..COMMITS {
                    let mut t = e.begin(Isolation::ReadCommitted);
                    let r = e.put(
                        &mut t,
                        b"k",
                        &(300_000 * w + i).to_be_bytes(),
                        Wait::Block(Duration::from_secs(120)),
                    );
                    let r = r.and_then(|()| e.commit(t));
                    if let Err(err) = r {
                        violations
                            .lock()
                            .unwrap()
                            .push(format!("writer {w}: {err}"));
                        break;
                    }
                }
                // The reader leaves when both writers are through, error
                // or not.
                if pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                    done.store(true, Ordering::Release);
                }
            });
        }
        // The reader loop: one snapshot (ts 1); every read must be v0 —
        // point reads and single-key scans alternate, so both read paths
        // run through the window. Each completed pass counts toward the
        // gates holding the windows open.
        while !done.load(Ordering::Relaxed) {
            match e.get(&mut reader, b"k") {
                Ok(Some(v)) if v == b"v0" => {}
                Ok(other) => {
                    violations
                        .lock()
                        .unwrap()
                        .push(format!("point read saw {other:?}, want b\"v0\""));
                    break;
                }
                Err(err) => {
                    violations
                        .lock()
                        .unwrap()
                        .push(format!("point read failed: {err}"));
                    break;
                }
            }
            match e.scan(&mut reader, b"k", b"l", 10) {
                Ok(pairs) if pairs == vec![(b"k".to_vec(), b"v0".to_vec())] => {}
                Ok(other) => {
                    violations
                        .lock()
                        .unwrap()
                        .push(format!("scan saw {other:?}, want [(k, v0)]"));
                    break;
                }
                Err(err) => {
                    violations
                        .lock()
                        .unwrap()
                        .push(format!("scan failed: {err}"));
                    break;
                }
            }
            samples.fetch_add(1, Ordering::Relaxed);
            thread::yield_now();
        }
        // Every reader exit path — `done`, or a violation breaking the
        // loop — releases the writers' gates: a stopped reader must
        // never hang a window (and a violation is the finding, not the
        // short sample run it causes).
        reader_stopped.store(true, Ordering::Release);
        let n_samples = samples.load(Ordering::Relaxed);
        let found = violations.lock().unwrap().clone();
        assert!(
            found.is_empty(),
            "violations: {found:#?} (after {n_samples} samples)"
        );
    });
    // The counted-handshake assertions (FLAKE-FIX-1), replacing the old
    // wall-clock `n_samples > 100`: every writer commit found a prior
    // tree value, so every window ran — and the gate held each one open
    // for its counted samples. Exact and structural, never load luck.
    assert_eq!(
        windows_opened.load(Ordering::Acquire),
        2 * COMMITS,
        "every writer commit opened (and held) one apply window"
    );
    assert!(
        samples.load(Ordering::Relaxed)
            >= SAMPLES_PER_WINDOW * windows_opened.load(Ordering::Acquire),
        "each held window admitted {SAMPLES_PER_WINDOW} counted samples ({} samples, {} windows)",
        samples.load(Ordering::Relaxed),
        windows_opened.load(Ordering::Acquire)
    );
    let v = violations.lock().unwrap().clone();
    assert!(v.is_empty(), "violations: {v:#?}");
    e.abort(reader);
    // All writes landed: a fresh reader sees one of the writers' values.
    let mut fresh = e.begin(Isolation::ReadCommitted);
    let last = e.get(&mut fresh, b"k").unwrap();
    e.abort(fresh);
    let got = last.map(|v| u64::from_be_bytes(v[..8].try_into().unwrap()));
    assert!(
        got.is_some_and(|v| (300_000..=600_000 + COMMITS).contains(&v)),
        "final value {got:?} is one of the writers'"
    );
    let _ = std::fs::remove_dir_all(dir);
}
