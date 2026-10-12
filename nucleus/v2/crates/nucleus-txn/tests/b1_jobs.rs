//! C-B1a tests: the background-job framework — wake on enqueue, the idle
//! park, the lost-wake rule, the shutdown order, fail-stop for the set,
//! panic reporting, the GC watermark lag gauge, the §7.4 SD1 split of the
//! `/sys/txn` delete out of the registry mutex, the reserved slots and the
//! spawner seam. Each test's doc names the mutant it kills (card item 11).

mod b1_support;
mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use b1_support::{ok, place_write, RecFailStop, SlotJob};
use common::{place_intent, RecKv};
use nucleus_kv::{Batch, Durability, GcFilter, Key, MemKv, OrderedKv, Result, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, FailStop, SyncCommit};
use nucleus_txn::encoding::intent_key;
use nucleus_txn::gc::{GcConfig, GcJob, GcRunner};
use nucleus_txn::jobs::metrics::{
    Metric, MetricKind, METRIC_BOOT_SWEEP_DONE, METRIC_ERRORS, METRIC_GC_WATERMARK_LAG,
    METRIC_IDLE_WAITS, METRIC_ITEMS, METRIC_PROGRESS_ROUNDS, METRIC_RESOLVE_QUEUE_LEN,
    METRIC_ROUNDS, METRIC_STATUS_ENTRIES, METRIC_WAKEUPS,
};
use nucleus_txn::jobs::runner::{JobSet, ShutdownStep, Spawner, StdThreadSpawner, SHUTDOWN_ORDER};
use nucleus_txn::jobs::{JobCtx, JobKind, JobOutcome, JobPoint, JobProbe};
use nucleus_txn::resolver::ResolverJob;
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnError};

fn idle(sec: u64) -> nucleus_txn::jobs::JobConfig {
    nucleus_txn::jobs::JobConfig {
        idle_interval: Duration::from_secs(sec),
    }
}

fn wait_until(deadline_secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(deadline_secs),
            "{what} did not happen within {deadline_secs}s"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// W1 (dropping the notify after `queue_resolution`'s `mark_released`) and
/// W1b (dropping it in the no-write fast path): with a 3600 s idle
/// interval the resolver only ever runs when woken. Twenty committed
/// writers must each be resolved within 5 s of their ack, and a read-only
/// commit's status entry (a truncation candidate with no queue entry)
/// must be truncated within 5 s — the fast path's own wake is its only
/// chance.
#[test]
fn resolver_wakes_on_enqueue_without_polling() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let mut set = JobSet::new(Arc::clone(&core));
    ok(set.start(Box::new(ResolverJob::new(Arc::clone(&core))), idle(3600)));

    for i in 0..20 {
        let key = format!("/t/1/r{i}");
        let txn = core.begin(Isolation::ReadCommitted);
        place_intent(&core, &txn, key.as_bytes(), b"v");
        ok(core.commit(txn, SyncCommit::On));
        wait_until(5, "the committed writer's intent is resolved", || {
            ok(core.latest_get(&intent_key(key.as_bytes()))).is_none()
        });
    }

    // The read-only fast path: no queue entry, but the released entry is a
    // truncation candidate only the resolver acts on.
    let ro = core.begin(Isolation::ReadCommitted);
    let ro_id = ro.id;
    assert_eq!(ok(core.commit(ro, SyncCommit::Off)), Ts::ZERO);
    wait_until(5, "the read-only commit's status is truncated", || {
        core.status.entry(ro_id).is_none()
    });

    ok(set.shutdown());
    ok(handle.shutdown());
}

/// W2 (`Idle` handled as `sleep(1ms)` + loop): at the default 50 ms idle
/// interval, 500 ms must cost at most `elapsed/50ms + 2` rounds.
#[test]
fn idle_resolver_does_not_poll() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let mut set = JobSet::new(Arc::clone(&core));
    ok(set.start(
        Box::new(ResolverJob::new(Arc::clone(&core))),
        nucleus_txn::jobs::JobConfig::default_for(JobKind::Resolver),
    ));
    let start = Instant::now();
    std::thread::sleep(Duration::from_millis(500));
    let elapsed = start.elapsed();
    let rounds = core.job_stats(JobKind::Resolver).rounds;
    assert!(rounds >= 1, "the first step runs immediately");
    assert!(
        rounds <= elapsed.as_millis() as u64 / 50 + 2,
        "{rounds} rounds in {elapsed:?} at a 50 ms idle interval: polling"
    );
    ok(set.shutdown());
}

/// W3 (`take_pending` after the step): a txn aborted by the probe at the
/// first `ResolverBeforeTruncation` — mid-step — enqueues cleanup and
/// wakes; with the wake taken before the step, the pending flag survives
/// the step and the next round drains it. With the take after the step,
/// the Idle park would eat it and the intent would sit there for the
/// 3600 s interval.
#[test]
fn wake_during_step_is_not_lost() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));

    let holder = core.begin(Isolation::ReadCommitted);
    place_intent(&core, &holder, b"/t/1/hold", b"v");

    // Aborts the held txn at the first `ResolverBeforeTruncation`.
    struct AbortProbe<K: OrderedKv> {
        core: Arc<Core<K>>,
        txn: Mutex<Option<nucleus_txn::txn::Txn>>,
        fired: AtomicBool,
    }
    impl<K: OrderedKv> JobProbe for AbortProbe<K> {
        fn at(&self, point: JobPoint) {
            if point == JobPoint::ResolverBeforeTruncation
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                let txn = self
                    .txn
                    .lock()
                    .expect("abort probe txn")
                    .take()
                    .expect("the probe fires once");
                ok(self.core.abort(txn));
            }
        }
    }
    core.set_job_probe(Arc::new(AbortProbe {
        core: Arc::clone(&core),
        txn: Mutex::new(Some(holder)),
        fired: AtomicBool::new(false),
    }));

    let mut set = JobSet::new(Arc::clone(&core));
    ok(set.start(Box::new(ResolverJob::new(Arc::clone(&core))), idle(3600)));
    // The first step runs immediately: it hits the truncation probe, the
    // probe aborts the holder (enqueue + wake mid-step), and the next
    // round must drain the cleanup without any poll.
    wait_until(5, "the mid-step abort's cleanup intent is gone", || {
        ok(core.latest_get(&intent_key(b"/t/1/hold"))).is_none()
    });
    ok(set.shutdown());
    ok(handle.shutdown());
}

/// S1 (`CommitThread` first in `SHUTDOWN_ORDER`): a Merge-slot job that
/// places an intent, waits until `cx.is_stopping()`, then commits `On`
/// must get its ack — the commit thread is still running while the
/// committing jobs shut down. With the commit thread first, the commit
/// fails as "commit pipeline stopped".
#[test]
fn shutdown_order_lets_committing_jobs_finish() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let commit_handle = ok(spawn_commit_thread(Arc::clone(&core)));

    let committed_once = Arc::new(AtomicBool::new(false));
    let result: Arc<Mutex<Option<std::result::Result<Ts, TxnError>>>> = Arc::new(Mutex::new(None));
    let job = {
        let core = Arc::clone(&core);
        let committed_once = Arc::clone(&committed_once);
        let result = Arc::clone(&result);
        SlotJob::new(JobKind::Merge, move |cx: &JobCtx| {
            if !committed_once.swap(true, Ordering::SeqCst) {
                let txn = core.begin(Isolation::ReadCommitted);
                place_write(&core, &txn, b"/t/1/m", b"v");
                let waited = Instant::now();
                while !cx.is_stopping() {
                    assert!(
                        waited.elapsed() < Duration::from_secs(10),
                        "the Merge job's stop flag was never set"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                *result.lock().expect("result") = Some(core.commit(txn, SyncCommit::On));
            }
            Ok(JobOutcome::Idle)
        })
    };

    let hook = RecFailStop::new();
    let mut set = JobSet::new(Arc::clone(&core))
        .with_fail_stop(Arc::clone(&hook) as Arc<dyn nucleus_txn::commit::FailStop>);
    ok(set.start(Box::new(job), idle(3600)));
    ok(set.start(
        Box::new(ResolverJob::new(Arc::clone(&core))),
        nucleus_txn::jobs::JobConfig::default_for(JobKind::Resolver),
    ));
    set.attach_commit_thread(commit_handle);

    // Wait until the Merge job placed its intent, then shut down.
    wait_until(5, "the Merge job placed its intent", || {
        ok(core.latest_get(&intent_key(b"/t/1/m"))).is_some()
    });
    ok(set.shutdown());
    let r = result.lock().expect("result").clone();
    match r {
        Some(Ok(ts)) => assert!(ts > Ts::ZERO, "the in-flight commit was acked: {ts:?}"),
        other => panic!("the Merge job's commit failed: {other:?}"),
    }
    assert!(
        hook.errors().is_empty(),
        "a clean shutdown reports nothing: {:?}",
        hook.errors()
    );
    // The log is SHUTDOWN_ORDER filtered to what ran: Merge, the commit
    // thread, the resolver.
    let expected: Vec<ShutdownStep> = SHUTDOWN_ORDER
        .iter()
        .copied()
        .filter(|s| match s {
            ShutdownStep::Job(k) => matches!(k, JobKind::Merge | JobKind::Resolver),
            ShutdownStep::CommitThread => true,
        })
        .collect();
    assert_eq!(set.shutdown_log(), expected);
}

/// F1 (on error, the other jobs keep running) and F2 (the error is not
/// reported to the hook): a resolver + GC set over a failing KV. The
/// resolver's first step (immediately at start) hits the armed failure;
/// the hook must see the error, the GC rounds counter must stop moving
/// over 300 ms, and `shutdown` must return it.
#[test]
fn job_error_is_fail_stop_for_the_set() {
    let (kv, fail) = common::FailKv::shared();
    let core = Arc::new(ok(Core::open(kv)));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));

    // One committed txn with an unresolved intent, and the GC's first
    // publish already done, all while the KV is healthy — so the only
    // thing left to fail after arming is the resolver's removal write.
    let txn = core.begin(Isolation::ReadCommitted);
    let txn_id = txn.id;
    place_intent(&core, &txn, b"/t/1/r", b"v");
    ok(core.commit(txn, SyncCommit::On));
    let tcore = common::TestCore::from_arc(Arc::clone(&core));
    common::note_committed(&tcore, txn_id);
    common::wait_released(&tcore, txn_id);
    let gcjob = ok(GcJob::install(&core, GcConfig::default()));
    let hook = RecFailStop::new();
    let mut set =
        JobSet::new(Arc::clone(&core)).with_fail_stop(Arc::clone(&hook) as Arc<dyn FailStop>);
    // A 20 ms idle interval, far below the 300 ms observation window
    // below: a live GC demonstrably ticks, a stopped one demonstrably
    // does not (at the 1 s default the window would prove nothing).
    ok(set.start(
        Box::new(GcRunner::new(
            Arc::new(gcjob),
            Arc::new(nucleus_txn::commit::SystemClock),
        )),
        nucleus_txn::jobs::JobConfig {
            idle_interval: Duration::from_millis(20),
        },
    ));
    wait_until(5, "the GC ran a round", || {
        core.job_stats(JobKind::Gc).rounds >= 1
    });

    // Arm, then start the resolver: its first step fails on the removal
    // write and fails the whole set.
    fail.store(true, Ordering::SeqCst);
    ok(set.start(Box::new(ResolverJob::new(Arc::clone(&core))), idle(3600)));
    wait_until(5, "the fail-stop hook got the resolver's error", || {
        !hook.errors().is_empty()
    });
    assert!(matches!(hook.errors()[0], TxnError::Kv(_)));

    // F1: the GC job stopped too — its rounds counter freezes.
    let rounds = core.job_stats(JobKind::Gc).rounds;
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        core.job_stats(JobKind::Gc).rounds,
        rounds,
        "the GC kept running after a sibling job's error"
    );

    assert!(set.shutdown().is_err(), "the set's first error returns");
    ok(handle.shutdown());
}

/// P1 (a panic swallowed at the join): a panicking slot job is reported as
/// `Invariant("job ... panicked")` to the hook and out of `shutdown`.
#[test]
fn panicking_job_is_reported() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let hook = RecFailStop::new();
    let mut set = JobSet::new(Arc::clone(&core))
        .with_fail_stop(Arc::clone(&hook) as Arc<dyn nucleus_txn::commit::FailStop>);
    ok(set.start(
        Box::new(SlotJob::new(JobKind::Analyze, |_cx: &JobCtx| {
            panic!("injected job panic")
        })),
        idle(3600),
    ));
    std::thread::sleep(Duration::from_millis(200));
    let r = set.shutdown();
    let err = r.expect_err("the panic surfaces at the shutdown join");
    assert!(
        matches!(&err, TxnError::Invariant(m) if m.contains("panicked")),
        "the panic becomes an Invariant mentioning the job: {err:?}"
    );
    assert!(
        hook.errors()
            .iter()
            .any(|e| matches!(e, TxnError::Invariant(m) if m.contains("panicked"))),
        "the panic is reported to the fail-stop hook: {:?}",
        hook.errors()
    );
}

/// G1 (`gc_watermark_lag` returns 0): a snapshot held across a publish
/// pins `W` below `visible_ts` (lag > 0); releasing it and publishing
/// again gives 0. Also checks that `job_metrics` carries every name of
/// card item 2.
#[test]
fn gc_watermark_lag_gauge() {
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv)));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));

    let snap = core.registry.take_snapshot(); // S = current visible_ts
    let txn = core.begin(Isolation::ReadCommitted);
    place_intent(&core, &txn, b"/t/1/r", b"v");
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert!(ts > Ts::ZERO);

    let job = ok(GcJob::install(&core, GcConfig::default()));
    ok(job.publish(0));
    assert!(
        core.gc_watermark_lag() > 0,
        "a held snapshot pins W below visible_ts {} (lag {})",
        core.visible_ts().0,
        core.gc_watermark_lag()
    );
    drop(snap);
    ok(job.publish(0));
    assert_eq!(core.gc_watermark_lag(), 0, "nothing holds W back now");

    // The metric surface of item 2: six counters per kind and four gauges.
    let names: Vec<&'static str> = core.job_metrics().iter().map(|m| m.name).collect();
    for counter in [
        METRIC_ROUNDS,
        METRIC_PROGRESS_ROUNDS,
        METRIC_IDLE_WAITS,
        METRIC_WAKEUPS,
        METRIC_ERRORS,
        METRIC_ITEMS,
    ] {
        assert_eq!(
            names.iter().filter(|n| **n == counter).count(),
            JobKind::ALL.len(),
            "{counter} appears once per job kind"
        );
    }
    for gauge in [
        METRIC_GC_WATERMARK_LAG,
        METRIC_RESOLVE_QUEUE_LEN,
        METRIC_STATUS_ENTRIES,
        METRIC_BOOT_SWEEP_DONE,
    ] {
        assert!(names.contains(&gauge), "{gauge} is missing");
    }
    // Spot-check the metric values end to end: the lag gauge reads 0 (just
    // asserted above) and the resolver counters are present at 0 (no
    // resolver job runs in this test).
    let metrics = core.job_metrics();
    let rounds: Vec<&Metric> = metrics
        .iter()
        .filter(|m| m.name == METRIC_ROUNDS && m.job == Some(JobKind::Resolver))
        .collect();
    assert_eq!(rounds.len(), 1);
    assert_eq!(rounds[0].kind, MetricKind::Counter);
    assert_eq!(rounds[0].value, 0);
    let lag: Vec<&Metric> = metrics
        .iter()
        .filter(|m| m.name == METRIC_GC_WATERMARK_LAG)
        .collect();
    assert_eq!(lag.len(), 1);
    assert_eq!(lag[0].kind, MetricKind::Gauge);
    assert_eq!(lag[0].value, 0);
    ok(handle.shutdown());
}

/// T1 (the `/sys/txn` delete written back inside `with_registry`): a KV
/// wrapper that, while writing a `/sys/txn` delete, asks another thread
/// to call `registry.published_w()` — which takes the registry mutex —
/// and fails the test if it does not return within 2 s. With the delete
/// inside the critical section the two block each other; SD1 writes it
/// outside.
#[test]
fn status_delete_is_written_outside_the_registry_mutex() {
    /// Signals, shared between the wrapper and the helper thread.
    #[derive(Default)]
    struct Handshake {
        entered: Mutex<u64>,
        entered_cv: std::sync::Condvar,
        done: Mutex<u64>,
        done_cv: std::sync::Condvar,
    }

    impl Handshake {
        fn signal_entered(&self) {
            let mut n = self.entered.lock().expect("entered");
            *n += 1;
            self.entered_cv.notify_all();
        }

        fn wait_entered(&self, seen: &mut u64) {
            let mut n = self.entered.lock().expect("entered");
            while *n <= *seen {
                n = self.entered_cv.wait(n).expect("entered wait");
            }
            *seen = *n;
        }

        fn signal_done(&self) {
            let mut n = self.done.lock().expect("done");
            *n += 1;
            self.done_cv.notify_all();
        }

        /// Fails (panics) if a matching `done` does not arrive in 2 s.
        fn wait_done(&self, seen: &mut u64) {
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut n = self.done.lock().expect("done");
            while *n <= *seen {
                let left = deadline.saturating_duration_since(Instant::now());
                let (n2, timeout) = self.done_cv.wait_timeout(n, left).expect("done wait");
                n = n2;
                if timeout.timed_out() && *n <= *seen {
                    panic!("registry.published_w() did not return within 2s while the /sys/txn delete was written: the registry mutex is held (SD1)");
                }
            }
            *seen = *n;
        }
    }

    use nucleus_txn::encoding::sys_txn_prefix;

    /// Records the handshake around every `/sys/txn` delete.
    #[derive(Clone)]
    struct SysTxnDeleteKv {
        inner: Arc<MemKv>,
        hs: Arc<Handshake>,
    }

    impl OrderedKv for SysTxnDeleteKv {
        type Snap = <MemKv as OrderedKv>::Snap;

        fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
            let is_sys_txn_delete = batch.ops.iter().any(|op| match op {
                nucleus_kv::Op::Delete(k) => k.starts_with(&sys_txn_prefix()),
                _ => false,
            });
            if is_sys_txn_delete {
                // Ask the helper thread to take the registry mutex; if the
                // delete ran inside `with_registry`, the helper blocks and
                // the wait below panics after 2 s.
                self.hs.signal_entered();
                let mut seen = 0;
                self.hs.wait_done(&mut seen);
            }
            self.inner.write(batch, sync)
        }

        fn sync_wal(&self) -> Result<()> {
            self.inner.sync_wal()
        }

        fn snapshot(&self) -> Self::Snap {
            self.inner.snapshot()
        }

        fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
            self.inner.get_latest(key)
        }

        fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
            self.inner.ingest_sorted(entries)
        }

        fn checkpoint(&self, dir: &std::path::Path) -> Result<()> {
            self.inner.checkpoint(dir)
        }

        fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
            self.inner.set_gc_filter(filter)
        }

        fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
            self.inner.set_gc_watermark(watermark)
        }
    }

    let hs = Arc::new(Handshake::default());
    let kv = SysTxnDeleteKv {
        inner: Arc::new(MemKv::new()),
        hs: Arc::clone(&hs),
    };
    let core = Arc::new(ok(Core::open(kv.clone())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));

    // The helper: on the (single) entered, take the registry mutex and
    // answer, then exit.
    let helper = {
        let core = Arc::clone(&core);
        let hs = Arc::clone(&hs);
        std::thread::spawn(move || {
            let mut seen = 0;
            hs.wait_entered(&mut seen);
            let _w = core.registry.published_w();
            hs.signal_done();
        })
    };

    let txn = core.begin(Isolation::ReadCommitted);
    let id = txn.id;
    place_intent(&core, &txn, b"/t/1/r", b"v");
    ok(core.commit(txn, SyncCommit::On));
    // The ack (step 4) legally precedes step 5's queueing; wait for the
    // release so the resolver round below is guaranteed its entry.
    let tcore = common::TestCore::from_arc(Arc::clone(&core));
    common::wait_released(&tcore, id);
    ok(nucleus_txn::resolver::Resolver::run_once(&core)); // resolve
                                                          // The truncation pass writes the /sys/txn delete: the wrapper's
                                                          // handshake runs on this thread; with the delete inside the registry
                                                          // critical section, it panics here.
    ok(nucleus_txn::resolver::Resolver::run_once(&core));
    assert!(core.status.entry(id).is_none(), "truncated");
    assert!(
        ok(core.latest_get(&nucleus_txn::encoding::sys_txn_key(id))).is_none(),
        "the /sys/txn delete was written"
    );
    ok(handle.shutdown());
    helper.join().expect("helper");
}

/// A slot job runs on the reserved Analyze slot, its rounds are counted,
/// and `wake_job(Analyze)` re-runs it without waiting the idle interval.
#[test]
fn slot_runs_a_test_job() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let count = Arc::new(AtomicU64::new(0));
    let mut set = JobSet::new(Arc::clone(&core));
    {
        let count = Arc::clone(&count);
        ok(set.start(
            Box::new(SlotJob::new(JobKind::Analyze, move |cx: &JobCtx| {
                count.fetch_add(1, Ordering::SeqCst);
                cx.probe(JobPoint::SlotStep);
                Ok(JobOutcome::Idle)
            })),
            idle(3600),
        ));
    }
    // The first step runs immediately at start.
    wait_until(5, "the slot job's first step ran", || {
        count.load(Ordering::SeqCst) >= 1
    });
    // A wake re-runs it while it is parked.
    core.wake_job(JobKind::Analyze);
    wait_until(5, "the wake re-ran the slot job", || {
        count.load(Ordering::SeqCst) >= 2
    });
    assert!(
        core.job_stats(JobKind::Analyze).rounds >= 2,
        "rounds are counted"
    );
    ok(set.shutdown());
}

/// The spawner seam is used: a recording spawner sees
/// `nucleus-job-resolver` (and delegates to the std spawner so the job
/// actually runs).
#[test]
fn spawner_seam_is_used() {
    struct RecSpawner {
        names: Mutex<Vec<String>>,
    }

    impl Spawner for RecSpawner {
        fn spawn(
            &self,
            name: String,
            body: Box<dyn FnOnce() + Send>,
        ) -> std::result::Result<Box<dyn nucleus_txn::jobs::runner::Joiner>, TxnError> {
            self.names.lock().expect("names").push(name.clone());
            StdThreadSpawner.spawn(name, body)
        }
    }

    let core = Arc::new(ok(Core::open(MemKv::new())));
    let spawner = Arc::new(RecSpawner {
        names: Mutex::new(Vec::new()),
    });
    let mut set =
        JobSet::new(Arc::clone(&core)).with_spawner(Arc::clone(&spawner) as Arc<dyn Spawner>);
    ok(set.start(Box::new(ResolverJob::new(Arc::clone(&core))), idle(3600)));
    wait_until(5, "the resolver ran its first step", || {
        core.job_stats(JobKind::Resolver).rounds >= 1
    });
    let names = spawner.names.lock().expect("names").clone();
    assert_eq!(names, vec!["nucleus-job-resolver".to_string()]);
    ok(set.shutdown());
}
