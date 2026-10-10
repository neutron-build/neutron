//! C-T1a tests: latches (§5.0) — the one-latch-per-thread debug assert,
//! including a second latch on the same stripe (which must fail fast, not
//! self-deadlock), and release on drop.

#[cfg(debug_assertions)]
use std::panic::{catch_unwind, AssertUnwindSafe};

use nucleus_txn::latch::Latches;

#[test]
fn a_latch_can_be_retaken_after_release() {
    let l = Latches::with_stripes(1);
    drop(l.lock(b"a"));
    drop(l.lock(b"b"));
    let _g = l.lock(b"a");
}

#[cfg(debug_assertions)]
#[test]
fn second_latch_on_the_same_stripe_panics_instead_of_deadlocking() {
    // One stripe: every key maps to the same mutex.
    let l = Latches::with_stripes(1);
    let r = catch_unwind(AssertUnwindSafe(|| {
        let _a = l.lock(b"a");
        let _b = l.lock(b"b");
    }));
    assert!(
        r.is_err(),
        "taking a second latch must trip the debug assert"
    );
    // The first guard was released during unwinding and the flag cleared.
    let _g = l.lock(b"a");
}

#[cfg(debug_assertions)]
#[test]
fn second_latch_on_another_stripe_panics() {
    let l = Latches::with_stripes(1024);
    let (a, b) = (b"/t/1/a".as_slice(), b"/t/1/b".as_slice());
    let r = catch_unwind(AssertUnwindSafe(|| {
        let _a = l.lock(a);
        let _b = l.lock(b);
    }));
    assert!(r.is_err());
    let _g = l.lock(b);
}
