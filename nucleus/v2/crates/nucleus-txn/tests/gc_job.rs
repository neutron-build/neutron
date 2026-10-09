//! C-T4 job-thread tests: `spawn_gc` runs rounds with a test clock and
//! stops cleanly; a failing KV reports through the fail-stop hook and
//! stops the thread.

mod gc_support;

use std::sync::Arc;
use std::time::Duration;

use gc_support::{
    ok, preload_gc_w, preload_ts_hwm, CountingClock, FailOnKey, PanicWhenArmed, RecFailStop,
    SharedKv,
};
use nucleus_txn::boot::Core;
use nucleus_txn::encoding::sys_gc_w_key;
use nucleus_txn::gc::{spawn_gc, GcConfig, GcJob};

/// `spawn_gc` runs `run_once` rounds on a test clock (one `now_secs` per
/// round) and `stop` joins cleanly with the last result `Ok`.
#[test]
fn spawn_gc_runs_rounds_and_stops_cleanly() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    let clock = CountingClock::new();
    let fail = RecFailStop::new();
    let handle = spawn_gc(job, Duration::from_millis(1), clock.clone(), fail.clone());

    // At least three rounds ran (deterministic handshake: the clock counts
    // one call per round).
    clock.ticks.wait_at_least(3, Duration::from_secs(10));
    ok(handle.stop());
    assert!(
        fail.errors().is_empty(),
        "a clean run never reports: {:?}",
        fail.errors()
    );
    // The round published W = visible_ts = 20.
    assert_eq!(gc_support::sys_gc_w(&core), 20);
}

/// A failing KV (the `/sys/gc_w` write of the first publish) makes the
/// thread report through the fail-stop hook and stop; `stop` returns the
/// error.
#[test]
fn spawn_gc_reports_fail_stop_and_stops() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let failing = FailOnKey::new(kv.clone(), &sys_gc_w_key());
    let core = Arc::new(ok(Core::open(failing.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    let clock = CountingClock::new();
    let fail = RecFailStop::new();
    failing.arm();
    let handle = spawn_gc(job, Duration::from_millis(1), clock, fail.clone());

    // The hook fires exactly once and the thread stops on its own.
    fail.hits.wait_at_least(1, Duration::from_secs(10));
    assert_eq!(fail.errors().len(), 1, "reported once");
    let err = handle
        .stop()
        .expect_err("stop returns the failing round's error");
    assert!(
        matches!(err, nucleus_txn::TxnError::Kv(_)),
        "the injected /sys/gc_w failure, got {err:?}"
    );
    // And the durable W never moved: /sys/gc_w is still the boot 10.
    assert_eq!(gc_support::sys_gc_w(&core), 10);
}

/// Rework 6: a GC thread that panics (a KV wrapper that panics inside the
/// first publish's write) is reported through the fail-stop hook when the
/// handle is dropped without `stop` - there is no caller left to return
/// the error to.
///
/// Mutant: `let _ = t.join()` in `Drop for GcHandle` - the panic is
/// swallowed, the hook never fires and `errors()` stays empty.
#[test]
fn dropped_handle_reports_a_panicked_gc_thread() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let panicking = PanicWhenArmed::new(kv.clone());
    let core = Arc::new(ok(Core::open(panicking.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    let clock = CountingClock::new();
    let fail = RecFailStop::new();
    panicking.arm();
    let handle = spawn_gc(job, Duration::from_millis(1), clock, fail.clone());

    // Wait until the thread is inside the write that panics, then drop
    // without stop: the join in Drop observes the panic and reports it.
    panicking.entered.wait_at_least(1, Duration::from_secs(10));
    drop(handle);
    assert_eq!(
        fail.errors().len(),
        1,
        "the panicked round is reported through the fail-stop hook: {:?}",
        fail.errors()
    );
}
