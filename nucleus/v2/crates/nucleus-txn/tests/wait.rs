//! C-T1b tests: §6 waiting — wake on commit, on abort and on
//! `bump_and_wake`; the gen-recheck rule (no lost wakeup when the target
//! ends between the generation read and the registration); prompt
//! cancellation; the `WaitHook` and `Parker` seams.
//!
//! The wake tests install a **strict** parker: its park timeout is
//! effectively infinite and it records whether any park ever ended by
//! timeout, so a lost wakeup is a failed test, not a 1 s re-check. It also
//! records the target's wake generation at unpark time, which pins the
//! §6 order *bump first, then wake*. Wake latency is bounded to 100 ms.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ok, place_intent, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::{MakeParker, Parker, WaitHook, WaitOutcome};
use nucleus_txn::{TxnId, TxnStatus};

/// The bound a wake (or a cancel) must beat: a lost wakeup parks for
/// [`StrictParker`]'s effectively-infinite timeout and blows it.
const WAKE_BOUND: Duration = Duration::from_millis(100);

/// Shared state of the strict parkers: timeouts seen, parks entered, and
/// the target's generation sampled at every unpark (the bump-before-wake
/// probe).
#[derive(Default)]
struct StrictProbe {
    core: Mutex<Option<Arc<Core<RecKv>>>>,
    target: Mutex<Option<TxnId>>,
    timed_out: AtomicBool,
    park_entries: AtomicUsize,
    unpark_gens: Mutex<Vec<Option<u64>>>,
}

impl StrictProbe {
    /// Arms the generation probe: every unpark of a parker made after this
    /// records the target's current gen.
    fn arm(&self, core: &Arc<Core<RecKv>>, target: TxnId) {
        *self.core.lock().expect("core") = Some(Arc::clone(core));
        *self.target.lock().expect("target") = Some(target);
    }

    fn gen_now(&self) -> Option<u64> {
        let core = self.core.lock().expect("core").clone()?;
        let target = (*self.target.lock().expect("target"))?;
        core.status.entry(target).map(|e| e.gen)
    }

    fn assert_no_timeout_and_gens(&self, want: u64) {
        assert!(
            !self.timed_out.load(Ordering::SeqCst),
            "a park ended by timeout: a wakeup was lost (1 s PARK_SLICE re-check must not rescue a wake test)"
        );
        let gens = self.unpark_gens.lock().expect("gens").clone();
        assert!(
            !gens.is_empty(),
            "the waiter was never unparked (it must park before the wake is triggered)"
        );
        assert!(
            gens.iter().all(|&g| g == Some(want)),
            "unparked before the generation bump: gens {gens:?}, want {want}"
        );
    }
}

struct StrictMaker {
    probe: Arc<StrictProbe>,
}

impl MakeParker for StrictMaker {
    fn make(&self) -> Arc<dyn Parker> {
        Arc::new(StrictParker {
            inner: nucleus_txn::wait::CondvarParker::new(),
            probe: Arc::clone(&self.probe),
        })
    }
}

struct StrictParker {
    inner: nucleus_txn::wait::CondvarParker,
    probe: Arc<StrictProbe>,
}

impl Parker for StrictParker {
    fn park(&self, _timeout: Duration) -> bool {
        self.probe.park_entries.fetch_add(1, Ordering::SeqCst);
        // Effectively infinite: a wake test must not be rescued by the
        // timeout re-check.
        let woken = self.inner.park(Duration::from_secs(3600));
        if !woken {
            self.probe.timed_out.store(true, Ordering::SeqCst);
        }
        woken
    }

    fn unpark(&self) {
        // §6 "Waking": whoever wakes bumps the generation first. Sample it
        // now, at unpark time, so a wake-before-bump is observable.
        if let Some(g) = self.probe.gen_now() {
            self.probe.unpark_gens.lock().expect("gens").push(Some(g));
        }
        self.inner.unpark();
    }
}

/// Installs the strict parker maker and returns its probe.
fn strict_parkers(core: &Arc<Core<RecKv>>) -> Arc<StrictProbe> {
    let probe = Arc::new(StrictProbe::default());
    core.waits.set_parker_maker(Arc::new(StrictMaker {
        probe: Arc::clone(&probe),
    }));
    probe
}

/// Spawns a waiter that reports its outcome through a channel, so the test
/// can bound how long the wake takes.
fn spawn_waiter(
    core: &Arc<Core<RecKv>>,
    waiter: Arc<nucleus_txn::txn::Txn>,
    target: TxnId,
    g: u64,
) -> mpsc::Receiver<WaitOutcome> {
    let (tx, rx) = mpsc::channel();
    let core = Arc::clone(core);
    std::thread::spawn(move || {
        let _ = tx.send(core.wait_on(&waiter, target, g));
    });
    rx
}

/// Waits until the waiter has entered a park (so the wake really must wake
/// it) and gives the scheduler a beat to block.
fn let_it_park(probe: &StrictProbe) {
    while probe.park_entries.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(5));
}

fn recv_bounded(rx: mpsc::Receiver<WaitOutcome>) -> WaitOutcome {
    match rx.recv_timeout(WAKE_BOUND) {
        Ok(o) => o,
        Err(_) => panic!("the waiter was not woken within {WAKE_BOUND:?}"),
    }
}

#[test]
fn wait_returns_promptly_when_target_never_begun() {
    // A missing status means ended and released (§4): no park.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
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
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    let target_id = target.id;
    assert_eq!(core.status.entry(target_id).map(|e| e.gen), Some(0));
    let probe = strict_parkers(&core);
    probe.arm(&core, target_id);

    let rx = spawn_waiter(&core, waiter, target_id, 0);
    let_it_park(&probe);
    place_intent(&core, &target, b"/t/1/r", b"v");
    let start = Instant::now();
    let ts = ok(core.commit(target, SyncCommit::On));

    let outcome = recv_bounded(rx);
    assert!(start.elapsed() < WAKE_BOUND, "woken promptly");
    assert_eq!(outcome, WaitOutcome::Committed(ts));
    assert_eq!(core.status.entry(target_id).map(|e| e.gen), Some(1));
    // Bumped (to 1) before the wake, and no park ever timed out.
    probe.assert_no_timeout_and_gens(1);
    ok(handle.shutdown());
}

#[test]
fn wait_wakes_on_abort() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    let target_id = target.id;
    let probe = strict_parkers(&core);
    probe.arm(&core, target_id);

    let rx = spawn_waiter(&core, waiter, target_id, 0);
    let_it_park(&probe);
    let start = Instant::now();
    ok(core.abort(target));

    assert_eq!(recv_bounded(rx), WaitOutcome::Aborted);
    assert!(start.elapsed() < WAKE_BOUND);
    assert_eq!(core.status.entry(target_id).map(|e| e.gen), Some(1));
    probe.assert_no_timeout_and_gens(1);
    ok(handle.shutdown());
}

#[test]
fn wait_wakes_on_bump_and_wake() {
    // The ROLLBACK TO wake path (§5.5): bump the generation, wake waiters.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    let probe = strict_parkers(&core);
    probe.arm(&core, target.id);

    let rx = spawn_waiter(&core, waiter, target.id, 0);
    let_it_park(&probe);
    let start = Instant::now();
    assert_eq!(ok(core.bump_and_wake(target.id)), 1);

    assert_eq!(recv_bounded(rx), WaitOutcome::GenChanged);
    assert!(start.elapsed() < WAKE_BOUND);
    // The status stays Pending throughout: a bump alone ends no txn.
    assert_eq!(
        core.status.entry(target.id).map(|e| e.status),
        Some(TxnStatus::Pending)
    );
    probe.assert_no_timeout_and_gens(1);
    ok(handle.shutdown());
}

#[test]
fn gen_recheck_returns_at_once_when_target_ended_before_registration() {
    // The lost-wakeup race: the caller read g under the latch, the target
    // fully ended (gen bumped, waiters woken — none registered yet), and
    // only then does the waiter register. The re-check after registration
    // must return immediately instead of parking forever. The gating
    // WaitHook forces exactly that interleaving; the strict parker makes a
    // dropped re-check a hang (bounded by WAKE_BOUND), not a 1 s delay.
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
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let probe = strict_parkers(&core);
    let gate = Arc::new(Gate {
        entered: AtomicBool::new(false),
        open: AtomicBool::new(false),
    });
    core.waits.set_hook(Arc::clone(&gate) as Arc<dyn WaitHook>);

    let target = core.begin(Isolation::ReadCommitted);
    let waiter = Arc::new(core.begin(Isolation::ReadCommitted));
    let target_id = target.id;
    let rx = spawn_waiter(&core, waiter, target_id, 0);
    while !gate.entered.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    // The waiter thread is inside on_wait_start (before registration).
    // End the target completely: commit + resolve + truncate.
    place_intent(&core, &target, b"/t/1/r", b"v");
    let ts = ok(core.commit(target, SyncCommit::On));
    assert!(ts > nucleus_txn::Ts::ZERO);
    let tcore = common::TestCore::from_arc(Arc::clone(&core));
    common::note_committed(&tcore, target_id);
    common::wait_released(&tcore, target_id);
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    assert!(core.status.entry(target_id).is_none(), "fully truncated");
    // Now let the waiter register.
    gate.open.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let outcome = recv_bounded(rx);
    assert!(start.elapsed() < WAKE_BOUND, "no park on an ended target");
    assert_eq!(
        outcome,
        WaitOutcome::Ended,
        "the truncated target reads as ended (§4)"
    );
    assert!(
        !probe.timed_out.load(Ordering::SeqCst),
        "the re-check must return without a park"
    );
    ok(handle.shutdown());
}

#[test]
fn cancel_returns_promptly_from_a_park() {
    // §6: setting the cancel flag also wakes a parked session. With the
    // strict parker there is no timeout rescue: a cancel that fails to
    // unpark the parked waiter hangs past WAKE_BOUND and fails.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let probe = strict_parkers(&core);
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    // Grab the handle before moving the txn into the waiter thread.
    let cancel = waiter.cancel_handle();

    let (tx, rx) = mpsc::channel();
    let core2 = Arc::clone(&core);
    std::thread::spawn(move || {
        let _ = tx.send(core2.wait_on(&waiter, target.id, 0));
    });
    while probe.park_entries.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(5));
    let start = Instant::now();
    cancel.cancel();
    let outcome = recv_bounded(rx);
    assert!(start.elapsed() < WAKE_BOUND, "cancel is prompt");
    assert_eq!(outcome, WaitOutcome::Cancelled);
    // The target was never touched.
    assert_eq!(
        core.status.entry(target.id).map(|e| e.gen),
        Some(0),
        "no bump from a cancel"
    );
    assert!(!probe.timed_out.load(Ordering::SeqCst));
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
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let rec = Arc::new(Rec::default());
    core.waits.set_hook(Arc::clone(&rec) as Arc<dyn WaitHook>);
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    let target_id = target.id;
    ok(core.abort(target));
    assert_eq!(core.wait_on(&waiter, target_id, 0), WaitOutcome::Aborted);
    assert_eq!(
        &*rec.starts.lock().expect("starts"),
        &[(waiter.id, target_id)]
    );
    assert_eq!(&*rec.ends.lock().expect("ends"), &[(waiter.id, target_id)]);
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
    impl MakeParker for Maker {
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
        .set_parker_maker(Arc::clone(&maker) as Arc<dyn MakeParker>);
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let target = core.begin(Isolation::ReadCommitted);
    let waiter = core.begin(Isolation::ReadCommitted);
    // A wait that parks (target Pending, generation matching) goes through
    // the replaced maker.
    let core2 = Arc::clone(&core);
    let h = std::thread::spawn(move || core2.wait_on(&waiter, target.id, 0));
    while maker.made.load(Ordering::SeqCst) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    ok(core.abort(target));
    assert_eq!(h.join().expect("waiter"), WaitOutcome::Aborted);
    assert!(maker.made.load(Ordering::SeqCst) >= 1);
    ok(handle.shutdown());
}
