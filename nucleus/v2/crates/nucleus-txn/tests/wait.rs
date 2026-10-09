//! C-T1b tests: §6 waiting — wake on commit, on abort and on
//! `bump_and_wake`; the gen-recheck rule (no lost wakeup when the target
//! ends between the generation read and the registration); prompt
//! cancellation; the `WaitHook` and `Parker` seams.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ok, place_intent, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::{Parker, WaitHook, WaitOutcome};
use nucleus_txn::{TxnId, TxnStatus};

const SOON: Duration = Duration::from_secs(10);

fn spawn_waiter(
    core: &Arc<Core<RecKv>>,
    waiter: Arc<nucleus_txn::txn::Txn>,
    target: TxnId,
    g: u64,
) -> (std::thread::JoinHandle<WaitOutcome>, Arc<AtomicBool>) {
    let started = Arc::new(AtomicBool::new(false));
    let s2 = Arc::clone(&started);
    let core = Arc::clone(core);
    let h = std::thread::spawn(move || {
        s2.store(true, Ordering::SeqCst);
        core.wait_on(&waiter, target, g)
    });
    (h, started)
}

#[test]
fn wait_returns_promptly_when_target_never_begun() {
    // A missing status means ended and released (§4): no park.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let waiter = core.begin(Isolation::ReadCommitted);
    let ghost = TxnId {
        epoch: core.epoch(),
        n: 9_999,
    };
    let start = Instant::now();
    assert_eq!(
        core.wait_on(&waiter, ghost, 0),
        WaitOutcome::Ended,
        "no status entry: ended"
    );
    assert!(start.elapsed() < Duration::from_millis(500));
    ok(handle.shutdown());
}

#[test]
fn wait_wakes_on_commit() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    let g = core.status.entry(target.id).map(|e| e.gen);
    assert_eq!(g, Some(0));

    let (h, started) = spawn_waiter(&core, waiter, target.id, 0);
    while !started.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(20)); // let it park
    place_intent(&core, &target, b"/t/1/r", b"v");
    let ts = ok(core.commit(&target, SyncCommit::On));

    let start = Instant::now();
    let outcome = h.join().expect("waiter");
    assert!(start.elapsed() < SOON, "woken promptly");
    assert_eq!(outcome, WaitOutcome::Committed(ts));
    assert_eq!(core.status.entry(target.id).map(|e| e.gen), Some(1));
    ok(handle.shutdown());
}

#[test]
fn wait_wakes_on_abort() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));

    let (h, started) = spawn_waiter(&core, waiter, target.id, 0);
    while !started.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(20));
    ok(core.abort(&target));

    let start = Instant::now();
    assert_eq!(h.join().expect("waiter"), WaitOutcome::Aborted);
    assert!(start.elapsed() < SOON);
    assert_eq!(core.status.entry(target.id).map(|e| e.gen), Some(1));
    ok(handle.shutdown());
}

#[test]
fn wait_wakes_on_bump_and_wake() {
    // The ROLLBACK TO wake path (§5.5): bump the generation, wake waiters.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));

    let (h, started) = spawn_waiter(&core, waiter, target.id, 0);
    while !started.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(ok(core.bump_and_wake(target.id)), 1);

    let start = Instant::now();
    assert_eq!(h.join().expect("waiter"), WaitOutcome::GenChanged);
    assert!(start.elapsed() < SOON);
    // A waiter holding the new generation does not return on another bump
    // of the same generation value... it does: gen != g. But a waiter with
    // the *current* gen parks, and the status stays Pending throughout.
    assert_eq!(
        core.status.entry(target.id).map(|e| e.status),
        Some(TxnStatus::Pending)
    );
    ok(handle.shutdown());
}

#[test]
fn gen_recheck_returns_at_once_when_target_ended_before_registration() {
    // The lost-wakeup race: the caller read g under the latch, the target
    // fully ended (gen bumped, waiters woken — none registered yet), and
    // only then does the waiter register. The re-check after registration
    // must return immediately instead of parking forever. The gating
    // WaitHook forces exactly that interleaving.
    struct Gate {
        entered: AtomicBool,
        open: AtomicBool,
    }
    impl WaitHook for Gate {
        fn on_wait_start(&self, _waiter: TxnId, _target: TxnId) {
            self.entered.store(true, Ordering::SeqCst);
            while !self.open.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_micros(100));
            }
        }
        fn on_wait_end(&self, _waiter: TxnId, _target: TxnId) {}
    }

    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let gate = Arc::new(Gate {
        entered: AtomicBool::new(false),
        open: AtomicBool::new(false),
    });
    core.waits.set_hook(Arc::clone(&gate) as Arc<dyn WaitHook>);

    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    let (h, entered) = spawn_waiter(&core, waiter, target.id, 0);
    while !entered.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    // The waiter thread is inside on_wait_start (before registration).
    // End the target completely: commit + resolve + truncate.
    place_intent(&core, &target, b"/t/1/r", b"v");
    let ts = ok(core.commit(&target, SyncCommit::On));
    assert!(ts > nucleus_txn::Ts::ZERO);
    common::wait_released(&core, target.id);
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    assert!(core.status.entry(target.id).is_none(), "fully truncated");
    // Now let the waiter register.
    gate.open.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let outcome = h.join().expect("waiter");
    assert!(start.elapsed() < SOON, "no park on an ended target");
    assert_eq!(
        outcome,
        WaitOutcome::Ended,
        "the truncated target reads as ended (§4)"
    );
    ok(handle.shutdown());
}

#[test]
fn cancel_returns_promptly_from_a_park() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    // Grab the handle before moving the txn into the waiter thread.
    let cancel = waiter.cancel_handle();

    let core2 = Arc::clone(&core);
    let h = std::thread::spawn(move || core2.wait_on(&waiter, target.id, 0));
    std::thread::sleep(Duration::from_millis(50)); // let it park
    let start = Instant::now();
    cancel.cancel();
    let outcome = h.join().expect("waiter");
    assert!(start.elapsed() < SOON, "cancel is prompt");
    assert_eq!(outcome, WaitOutcome::Cancelled);
    // The target was never touched.
    assert_eq!(
        core.status.entry(target.id).map(|e| e.gen),
        Some(0),
        "no bump from a cancel"
    );
    ok(handle.shutdown());
}

#[test]
fn wait_hook_sees_every_wait() {
    #[derive(Default)]
    struct Rec {
        starts: Mutex<Vec<(TxnId, TxnId)>>,
        ends: Mutex<Vec<(TxnId, TxnId)>>,
    }
    impl WaitHook for Rec {
        fn on_wait_start(&self, waiter: TxnId, target: TxnId) {
            self.starts.lock().expect("starts").push((waiter, target));
        }
        fn on_wait_end(&self, waiter: TxnId, target: TxnId) {
            self.ends.lock().expect("ends").push((waiter, target));
        }
    }
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let rec = Arc::new(Rec::default());
    core.waits.set_hook(Arc::clone(&rec) as Arc<dyn WaitHook>);
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    ok(core.abort(&target));
    assert_eq!(core.wait_on(&waiter, target.id, 0), WaitOutcome::Aborted);
    assert_eq!(
        &*rec.starts.lock().expect("starts"),
        &[(waiter.id, target.id)]
    );
    assert_eq!(&*rec.ends.lock().expect("ends"), &[(waiter.id, target.id)]);
    ok(handle.shutdown());
}

#[test]
fn parker_seam_is_replaceable() {
    // The deterministic simulator replaces the parker; a counting maker
    // proves the seam is used (one parker per wait).
    struct CountingParker {
        inner: nucleus_txn::wait::CondvarParker,
    }
    impl Parker for CountingParker {
        fn park(&self, timeout: Duration) -> bool {
            self.inner.park(timeout)
        }
        fn unpark(&self) {
            self.inner.unpark()
        }
    }
    struct Maker {
        made: AtomicUsize,
    }
    impl nucleus_txn::wait::MakeParker for Maker {
        fn make(&self) -> Arc<dyn Parker> {
            self.made.fetch_add(1, Ordering::SeqCst);
            Arc::new(CountingParker {
                inner: nucleus_txn::wait::CondvarParker::new(),
            })
        }
    }

    let core = Arc::new(ok(Core::open(RecKv::new())));
    let maker = Arc::new(Maker {
        made: AtomicUsize::new(0),
    });
    core.waits
        .set_parker_maker(Arc::clone(&maker) as Arc<dyn nucleus_txn::wait::MakeParker>);
    let handle = spawn_commit_thread(Arc::clone(&core));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    // A wait that parks (target Pending, generation matching) goes through
    // the replaced maker.
    let core2 = Arc::clone(&core);
    let h = std::thread::spawn(move || core2.wait_on(&waiter, target.id, 0));
    while maker.made.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    ok(core.abort(&target));
    assert_eq!(h.join().expect("waiter"), WaitOutcome::Aborted);
    assert!(maker.made.load(Ordering::SeqCst) >= 1);
    ok(handle.shutdown());
}
