//! C-B1c §9.3 I/O budget tests: the token bucket paces a job on virtual
//! time (a fake [`JobClock`] whose `pause` advances it — the bucket's
//! refill only ever observes time that way, so the pacing is exact), an
//! unbudgeted job never pauses, a throttled job shuts down promptly
//! (stop is checked between the ≤ 10 ms pause slices), and the resolver's
//! bounded rounds requeue the unfinished work at the **front** in order.
//! Each test's doc names the mutant it kills (card item 6).

mod b1_support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use b1_support::{commit, ok, place_write, SlotJob};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::encoding::intent_key;
use nucleus_txn::jobs::runner::{Joiner, Spawner};
use nucleus_txn::jobs::{IoBudget, JobClock, JobCtx, JobKind, JobOutcome, JobPoint, JobProbe};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::TxnError;

// ---- the fake clock and the inline spawner -----------------------------------

/// A [`JobClock`] on virtual time: `now` is a counter, `pause(d)`
/// advances it by `d` and records the total. Deterministic — no wall
/// time anywhere. The bound below is what turns a stalled throttler
/// (a mutant whose refill ignores the clock, so the pauses never buy any
/// tokens) into a loud failure instead of a hang: no legitimate test
/// here needs more than a few virtual seconds.
struct FakeClock {
    now: Mutex<Duration>,
    paused_total: Mutex<Duration>,
}

/// Past this much virtual pause, `pause` panics: the throttler is stuck.
const VIRTUAL_PAUSE_LIMIT: Duration = Duration::from_secs(10);

impl FakeClock {
    fn new() -> Arc<FakeClock> {
        Arc::new(FakeClock {
            now: Mutex::new(Duration::ZERO),
            paused_total: Mutex::new(Duration::ZERO),
        })
    }

    /// The clock as the set's `Arc<dyn JobClock>`.
    fn as_job_clock(self: &Arc<Self>) -> Arc<dyn JobClock> {
        Arc::clone(self) as Arc<dyn JobClock>
    }

    /// How much virtual time the throttler spent paused.
    fn paused_total(&self) -> Duration {
        *self.paused_total.lock().expect("b1 throttle paused")
    }
}

impl JobClock for FakeClock {
    fn now(&self) -> Duration {
        *self.now.lock().expect("b1 throttle now")
    }

    fn pause(&self, d: Duration) {
        let mut total = self.paused_total.lock().expect("b1 throttle paused");
        *total += d;
        assert!(
            *total < VIRTUAL_PAUSE_LIMIT,
            "the throttler stalled: {total:?} of virtual pause and the balance never recovered \
             (the refill is not following the clock)"
        );
        drop(total);
        *self.now.lock().expect("b1 throttle now") += d;
    }
}

/// A [`Spawner`] that runs the job's whole body synchronously on the
/// caller's thread: with the fake clock, the runner loop (throttle, step,
/// charge) executes inline, so the test observes the pacing with no
/// threads and no wall time. The job must end with `Finished`.
struct InlineSpawner;

impl Spawner for InlineSpawner {
    fn spawn(
        &self,
        _name: String,
        body: Box<dyn FnOnce() + Send>,
    ) -> Result<Box<dyn Joiner>, TxnError> {
        body();
        Ok(Box::new(InlineJoiner))
    }
}

struct InlineJoiner;

impl Joiner for InlineJoiner {
    fn join(self: Box<Self>) -> Result<(), ()> {
        Ok(())
    }
}

/// B-IGN (the configured budget is ignored — no bucket, no waits): a
/// Merge test job charging 50 units per step under a 1000 units/s budget
/// with burst 100 must be paced on the clock's virtual time: 40 steps =
/// 2000 units, of which the starting burst pays 100, so the throttler
/// owes at least ⌈(2000 − 100)/1000⌉ = 1.9 s of pauses, and it takes at
/// least one pause slice to get there.
#[test]
fn budget_paces_a_job() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let clock = FakeClock::new();
    let steps = Arc::new(AtomicU64::new(0));
    let steps_for_job = Arc::clone(&steps);
    let job = SlotJob::new(JobKind::Merge, move |cx: &JobCtx| {
        let n = steps_for_job.fetch_add(1, Ordering::SeqCst);
        if n >= 40 {
            return Ok(JobOutcome::Finished);
        }
        cx.charge(50);
        Ok(JobOutcome::Progress)
    });
    let mut set = nucleus_txn::jobs::JobSet::new(Arc::clone(&core))
        .with_clock(clock.as_job_clock())
        .with_spawner(Arc::new(InlineSpawner))
        .with_budget(
            JobKind::Merge,
            Some(IoBudget {
                units_per_sec: 1000,
                burst: 100,
            }),
        );
    ok(set.start(
        Box::new(job),
        nucleus_txn::jobs::JobConfig {
            idle_interval: Duration::from_secs(3600),
        },
    ));
    // `start` ran the whole loop inline; the set shuts down trivially.
    ok(set.shutdown());
    assert_eq!(steps.load(Ordering::SeqCst), 41, "the job ran all 40 steps");
    let paused = clock.paused_total();
    assert!(
        paused >= Duration::from_millis(1900),
        "the budget did not pace the job: {paused:?} of virtual pause, at least 1.9 s owed"
    );
    assert!(
        core.job_stats(JobKind::Merge).throttle_waits > 0,
        "the throttler never paused"
    );
    assert_eq!(
        core.job_stats(JobKind::Merge).units,
        2000,
        "every charged unit is counted"
    );
}

/// The unbudgeted complement: the same job on an unbudgeted kind runs
/// all 40 steps without a single pause (no bucket, no waits, no units
/// owed).
#[test]
fn unbudgeted_job_never_pauses() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let clock = FakeClock::new();
    let steps = Arc::new(AtomicU64::new(0));
    {
        let steps = Arc::clone(&steps);
        let job = SlotJob::new(JobKind::Merge, move |cx: &JobCtx| {
            let n = steps.fetch_add(1, Ordering::SeqCst);
            if n >= 40 {
                return Ok(JobOutcome::Finished);
            }
            cx.charge(50);
            Ok(JobOutcome::Progress)
        });
        let mut set = nucleus_txn::jobs::JobSet::new(Arc::clone(&core))
            .with_clock(clock.as_job_clock())
            .with_spawner(Arc::new(InlineSpawner))
            .with_budget(JobKind::Merge, None);
        ok(set.start(
            Box::new(job),
            nucleus_txn::jobs::JobConfig {
                idle_interval: Duration::from_secs(3600),
            },
        ));
        ok(set.shutdown());
    }
    assert_eq!(steps.load(Ordering::SeqCst), 41);
    assert_eq!(
        clock.paused_total(),
        Duration::ZERO,
        "an unbudgeted job never pauses"
    );
    assert_eq!(core.job_stats(JobKind::Merge).throttle_waits, 0);
}

/// B-STOP (the pause is not sliced / stop is not checked between slices):
/// a real-clock job at 1 unit/s with burst 1 charges 100 units in its
/// first step; the shutdown that follows must return within 1 s — the
/// healthy throttler wakes between its ≤ 10 ms slices and exits at the
/// stop check, while an unsliced pause (or one that ignores stop) parks
/// for the ~99 s the deficit owes. The handshake (step 1 signals when it
/// ends, i.e. just before the runner charges and throttles) makes the
/// job deterministically throttled-or-about-to-throttle when stop lands.
#[test]
fn stop_interrupts_a_throttled_job() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let stepped = Arc::new((Mutex::new(0u64), Condvar::new()));

    let mut set = nucleus_txn::jobs::JobSet::new(Arc::clone(&core)).with_budget(
        JobKind::Merge,
        Some(IoBudget {
            units_per_sec: 1,
            burst: 1,
        }),
    );
    {
        let stepped = Arc::clone(&stepped);
        let job = SlotJob::new(JobKind::Merge, move |cx: &JobCtx| {
            let (done, cv) = &*stepped;
            let mut n = done.lock().expect("b1 throttle stepped");
            if *n == 0 {
                // Step 1: 100 units at 1/s — a ~99 s deficit. Signal at
                // the END of the step, after the charge, before the
                // runner's throttle.
                cx.charge(100);
                *n = 1;
                cv.notify_all();
                return Ok(JobOutcome::Progress);
            }
            Ok(JobOutcome::Finished)
        });
        ok(set.start(
            Box::new(job),
            nucleus_txn::jobs::JobConfig {
                idle_interval: Duration::from_secs(3600),
            },
        ));
    }

    // Deterministically wait for step 1 to end (no wall clock): the job
    // is now charged 100 units against a 1/s bucket.
    {
        let (done, cv) = &*stepped;
        let mut n = done.lock().expect("b1 throttle stepped");
        while *n == 0 {
            n = cv
                .wait(n)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    let started = Instant::now();
    ok(set.shutdown());
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown of a throttled job took {elapsed:?} (stop is not checked between pause slices)"
    );
}

// ---- the bounded resolver -----------------------------------------------------

/// The bounded-round probe: counts one point's hits (the truncation
/// gate) and, at its third removal, aborts a held txn **mid-round** — its
/// cleanup entry lands at the back of the queue while the round holds the
/// drained entries, which is what makes the requeue order observable.
struct CountProbe {
    point: JobPoint,
    hits: AtomicU64,
    removals: AtomicU64,
    pending: Arc<Mutex<Option<nucleus_txn::txn::Txn>>>,
    core: Arc<Core<MemKv>>,
}

impl JobProbe for CountProbe {
    fn at(&self, point: JobPoint) {
        if point == self.point {
            self.hits.fetch_add(1, Ordering::SeqCst);
        }
        if point == JobPoint::ResolverAfterRemoval
            && self.removals.fetch_add(1, Ordering::SeqCst) == 2
        {
            let txn = self
                .pending
                .lock()
                .expect("b1 throttle pending")
                .take()
                .expect("the probe aborts once");
            ok(self.core.abort(txn));
        }
    }
}

/// R-TAIL (the unfinished entry goes to the **back**) and R-DROP (the
/// remainder is dropped): T1 (10 keys) and T2 (1 key) queue one entry
/// each, and a third txn's cleanup entry is enqueued **mid-round** — the
/// probe aborts it at the round's third removal, so the queue is
/// non-empty when the round requeues its unfinished tail and
/// front-vs-back is observable. A `run_bounded(3)` round removes exactly
/// 3 keys of T1, requeues T1's remaining 7 keys at the **front** with T2
/// and the aborted txn's cleanup behind it, and runs no truncation pass.
/// The next bounded round therefore removes T1's a3/a4/a5 and leaves the
/// later entries' keys untouched (under R-TAIL they jump the queue).
/// Draining to the end resolves every key (under R-DROP the dropped
/// remainder lingers) and only then truncates.
#[test]
fn resolver_steps_are_bounded() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let mut pipeline = ok(nucleus_txn::commit::CommitPipeline::new(Arc::clone(&core)));

    // T1: 10 keys; T2: 1 key. Commit order fixes the queue order.
    let t1 = core.begin(Isolation::ReadCommitted);
    let t1_id = t1.id;
    let mut t1_keys = Vec::new();
    for i in 0..10u64 {
        let k = format!("/t/1/a{i}").into_bytes();
        place_write(&core, &t1, &k, b"v");
        t1_keys.push(k);
    }
    ok(commit(
        &core,
        &mut pipeline,
        t1,
        nucleus_txn::commit::SyncCommit::On,
    ));
    let t2 = core.begin(Isolation::ReadCommitted);
    let t2_id = t2.id;
    place_write(&core, &t2, b"/t/1/b0", b"v");
    ok(commit(
        &core,
        &mut pipeline,
        t2,
        nucleus_txn::commit::SyncCommit::On,
    ));
    // A pending txn holding one intent; the probe aborts it mid-round so
    // its cleanup entry lands at the BACK of the queue while the round
    // holds the drained entries.
    let pending = core.begin(Isolation::ReadCommitted);
    place_write(&core, &pending, b"/t/1/ax", b"doomed");
    let pending = Arc::new(Mutex::new(Some(pending)));
    assert_eq!(core.resolve_queue_len(), 2, "one entry per txn, in order");
    drop(pipeline);

    let probe = Arc::new(CountProbe {
        point: JobPoint::ResolverBeforeTruncation,
        hits: AtomicU64::new(0),
        removals: AtomicU64::new(0),
        pending,
        core: Arc::clone(&core),
    });
    core.set_job_probe(Arc::clone(&probe) as Arc<dyn JobProbe>);

    // Round 1: exactly 3 of T1's keys, no entry finished, no truncation.
    let r1 = ok(Resolver::run_bounded(&core, 3));
    assert_eq!(r1.keys, 3, "three keys removed this round");
    assert_eq!(r1.removed, 3);
    assert_eq!(r1.entries, 0, "no entry was fully processed");
    assert!(!r1.drained, "the queue was cut mid-entry");
    assert_eq!(r1.truncated, 0);
    assert_eq!(
        probe.hits.load(Ordering::SeqCst),
        0,
        "no truncation pass before the queue drains"
    );
    assert_eq!(
        core.resolve_queue_len(),
        3,
        "T1's remainder requeued, then T2 and the aborted txn's cleanup"
    );
    for i in 0..3u64 {
        assert!(
            ok(core.latest_get(&intent_key(format!("/t/1/a{i}").as_bytes()))).is_none(),
            "a{i} resolved"
        );
    }
    for i in 3..10u64 {
        assert!(
            ok(core.latest_get(&intent_key(format!("/t/1/a{i}").as_bytes()))).is_some(),
            "a{i} still queued (the remainder was not dropped)"
        );
    }
    assert!(
        ok(core.latest_get(&intent_key(b"/t/1/b0"))).is_some(),
        "T2's key waits behind T1's remainder"
    );

    // Round 2: the front entry continues first — a3/a4/a5, not the
    // entries queued behind T1's remainder (T2's b0, the aborted ax).
    let r2 = ok(Resolver::run_bounded(&core, 3));
    assert_eq!(r2.keys, 3);
    assert_eq!(probe.hits.load(Ordering::SeqCst), 0, "still not drained");
    assert!(
        ok(core.latest_get(&intent_key(b"/t/1/b0"))).is_some(),
        "R-TAIL: T2 jumped T1's requeued remainder"
    );
    assert!(
        ok(core.latest_get(&intent_key(b"/t/1/ax"))).is_some(),
        "R-TAIL: the mid-round cleanup entry jumped T1's remainder"
    );
    for i in 3..6u64 {
        assert!(
            ok(core.latest_get(&intent_key(format!("/t/1/a{i}").as_bytes()))).is_none(),
            "a{i} resolved in order"
        );
    }
    for i in 6..10u64 {
        assert!(
            ok(core.latest_get(&intent_key(format!("/t/1/a{i}").as_bytes()))).is_some(),
            "a{i} still queued"
        );
    }

    // Drain: only a fully drained round truncates, and every key ends
    // resolved with the statuses truncated.
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds < 20, "the bounded rounds do not drain");
        let r = ok(Resolver::run_bounded(&core, 3));
        if r.drained && core.resolve_queue_len() == 0 {
            break;
        }
    }
    assert!(
        probe.hits.load(Ordering::SeqCst) >= 1,
        "a drained round ran the truncation pass"
    );
    for key in t1_keys
        .iter()
        .chain([b"/t/1/b0".to_vec(), b"/t/1/ax".to_vec()].iter())
    {
        assert!(
            ok(core.latest_get(&intent_key(key))).is_none(),
            "{key:?} resolved in the end"
        );
    }
    assert!(core.status.entry(t1_id).is_none(), "T1 truncated");
    assert!(core.status.entry(t2_id).is_none(), "T2 truncated");
    assert_eq!(
        core.job_stats(JobKind::Resolver).units,
        12,
        "one charged unit per removal"
    );
}
