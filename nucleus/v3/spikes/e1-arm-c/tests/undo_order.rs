//! Card X-3 mutation evidence WRITE-BEFORE-UNDO: the binding order of
//! commit apply (Work 5 — push the undo record, **then** write the tree
//! value) probed by a reader loop concurrent with writers.
//!
//! The engine's apply-yield hook ([`ArmC::arm_apply_yield`]) sleeps
//! between the two steps, so the window is wide enough to sample. Under
//! the correct order a reader at an old snapshot always resolves through
//! existing undo records and sees exactly its snapshot's value; under
//! the swapped order (tree value first, undo record after) the reader
//! observes a tree value whose undo pointer names a record that does not
//! exist yet, and the read fails — an engine error surfacing the broken
//! AC-UNDO-FIRST order, never a quiet wrong answer.
//!
//! The same property is what S11's readers exercise for atomic
//! visibility; this test aims the reader squarely at the apply window.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use e1_arm_c::ArmC;
use e1_mvcc::api::{Isolation, TxnEngine, Wait};

/// Commits per writer thread.
const COMMITS: u64 = 250;

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
    e.arm_apply_yield();
    {
        let mut seed = e.begin(Isolation::ReadCommitted);
        e.put(&mut seed, b"k", b"v0", Wait::NoWait).unwrap();
        e.commit(seed).unwrap(); // ts 1: the reader's snapshot
    }
    let mut reader = e.begin(Isolation::RepeatableRead);
    assert_eq!(e.get(&mut reader, b"k").unwrap(), Some(b"v0".to_vec()));

    let done = AtomicBool::new(false);
    let pending = AtomicU64::new(2);
    let violations: Mutex<Vec<String>> = Mutex::new(Vec::new());
    thread::scope(|s| {
        // Two writers with distinguishable values: 300_000·w + i.
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
                        Wait::Block(Duration::from_secs(30)),
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
        // run through the window.
        let samples = AtomicU64::new(0);
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
        // Report what the reader actually saw before how much it
        // sampled: a violation breaks the loop early, and the violation
        // is the finding, not the short sample run it caused.
        let n_samples = samples.load(Ordering::Relaxed);
        let found = violations.lock().unwrap().clone();
        assert!(
            found.is_empty(),
            "violations: {found:#?} (after {n_samples} samples)"
        );
        assert!(
            n_samples > 100,
            "the reader sampled the window ({n_samples} samples)"
        );
    });
    let v = violations.lock().unwrap().clone();
    assert!(v.is_empty(), "violations: {v:#?}");
    e.abort(reader);
    // All writes landed: a fresh reader sees one of the writers' values.
    let mut fresh = e.begin(Isolation::ReadCommitted);
    let last = e.get(&mut fresh, b"k").unwrap();
    e.abort(fresh);
    let got = last.map(|v| u64::from_be_bytes(v[..8].try_into().unwrap()));
    assert!(
        got.is_some_and(|v| (300_000..600_500).contains(&v)),
        "final value {got:?} is one of the writers'"
    );
    let _ = std::fs::remove_dir_all(dir);
}
