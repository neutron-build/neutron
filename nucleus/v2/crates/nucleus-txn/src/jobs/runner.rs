//! The job runner (§9.3): the per-job loop, the §9.3 I/O budget's token
//! bucket, the [`Spawner`] seam and the fixed shutdown order.
//!
//! **The loop.** Each job runs on its own thread: check the stop flag →
//! take the wake's pending flag **before** the step (an enqueue during the
//! step re-arms the wake, so it cannot be lost to the park that follows) →
//! count the round → **throttle** (a budgeted job refills its token
//! bucket from the set's [`JobClock`] and, while it cannot afford the
//! next step, pauses in slices of at most 10 ms, checking stop between
//! slices — throttling only delays the step, never changes it) → `step`,
//! then charge the step's units (the delta of the kind's `units` counter)
//! to the bucket. `Progress` loops at once; `Idle` parks on the wake for
//! [`JobConfig`](crate::jobs::JobConfig)'s idle interval (`Woken` counts
//! a wakeup, `TimedOut` an idle wait, `Stopped` exits); `Finished` exits
//! Ok. The first step runs immediately at start (after the bucket starts
//! full at `burst`).
//!
//! **Fail-stop** (§3, §9.3): a step's `Err` counts an error, is recorded
//! as the set's first error if none was recorded yet, sets the stop flag
//! of **every other** job in the set and wakes them, runs the fail-stop
//! hook (default: the core's), and the failing job's own loop exits — it
//! returns at once, so it never spins on a KV error. A panic is caught
//! with `catch_unwind` around the step, recorded as
//! `Invariant("job <kind> panicked")` and routed through the same
//! fail-stop, so siblings stop and the hook runs immediately, not at
//! `shutdown`.
//!
//! A job started into a set that has already failed is refused with the
//! set's first error, and a sibling failing while `start` is still in
//! flight still stops the new job: the slot (its stop flag) is pushed
//! under the `slots` lock **before** the spawn call returns to the
//! caller, holding the same lock `fail_the_set` stops jobs under — the
//! two can never miss each other.
//!
//! **Shutdown order.** [`SHUTDOWN_ORDER`]: the jobs that commit txns
//! (merge, analyze, drop cleanup) stop **before** the commit thread, so a
//! committing job's in-flight commit is acked by the still-running
//! pipeline instead of failing as "commit pipeline stopped"; then the
//! commit thread; then the resolver (its queued work only concerns already
//! committed txns); then the boot sweep, GC and retention, which depend on
//! nobody. Each step signals stop, wakes, joins, and appends to the set's
//! shutdown log. `Drop` of a [`JobSet`] runs the same ordered shutdown,
//! ignoring the result (a panic observed there is still reported to the
//! hook).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::commit::{CommitThreadHandle, FailStop};
use crate::jobs::{wake, Job, JobClock, JobConfig, JobCtx, JobKind, JobOutcome, SystemJobClock};
use crate::TxnError;

/// Where a job's thread comes from (C-R0b's seam): the default is a named
/// std thread; a later card may place jobs on pools without changing any
/// job body.
pub trait Spawner: Send + Sync {
    /// Runs `body` on a thread named `name` (the runner passes
    /// `nucleus-job-<kind>`); `Err` means the thread could not be started
    /// (reported to the set's fail-stop hook by [`JobSet::start`]).
    fn spawn(
        &self,
        name: String,
        body: Box<dyn FnOnce() + Send>,
    ) -> Result<Box<dyn Joiner>, TxnError>;
}

/// The spawning side's join handle. `Err` from `join` means the body
/// panicked.
pub trait Joiner: Send {
    /// `Result<(), ()>` is the card's signature: the only fact a join
    /// observes is panicked-or-not, and there is no error payload to
    /// carry (`(())` would be a custom type for a boolean).
    #[allow(clippy::result_unit_err)]
    fn join(self: Box<Self>) -> Result<(), ()>;
}

/// The [`Spawner`] of this crate: a named `std::thread::Builder` thread
/// whose name is exactly the `name` handed to [`Spawner::spawn`].
pub struct StdThreadSpawner;

impl Spawner for StdThreadSpawner {
    fn spawn(
        &self,
        name: String,
        body: Box<dyn FnOnce() + Send>,
    ) -> Result<Box<dyn Joiner>, TxnError> {
        let thread = std::thread::Builder::new()
            .name(name)
            .spawn(body)
            .map_err(|e| TxnError::Invariant(format!("failed to spawn the job thread: {e}")))?;
        Ok(Box::new(StdJoiner {
            thread: Some(thread),
        }))
    }
}

struct StdJoiner {
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Joiner for StdJoiner {
    fn join(mut self: Box<Self>) -> Result<(), ()> {
        match self.thread.take() {
            Some(t) => match t.join() {
                Ok(()) => Ok(()),
                Err(_) => Err(()),
            },
            // `join` called twice: nothing left to observe; not reported
            // as a panic.
            None => Ok(()),
        }
    }
}

/// One step of [`SHUTDOWN_ORDER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownStep {
    Job(JobKind),
    CommitThread,
}

/// The fixed shutdown order (§9.3): jobs that commit txns first, then the
/// commit thread, then the resolver, then the sweep, GC and retention.
pub const SHUTDOWN_ORDER: [ShutdownStep; 8] = [
    ShutdownStep::Job(JobKind::Merge),
    ShutdownStep::Job(JobKind::Analyze),
    ShutdownStep::Job(JobKind::DropCleanup),
    ShutdownStep::CommitThread,
    ShutdownStep::Job(JobKind::Resolver),
    ShutdownStep::Job(JobKind::BootSweep),
    ShutdownStep::Job(JobKind::Gc),
    ShutdownStep::Job(JobKind::SsiRetention),
];

/// One job thread's §9.3 token bucket. Tokens start at `burst`, refill
/// from the set's [`JobClock`] (elapsed × rate, capped at `burst`, the
/// sub-unit remainder carried), and are charged the step's units. A
/// negative balance is a deficit: the runner waits it out in slices of at
/// most [`TokenBucket::SLICE`], so no pause outruns a shutdown.
///
/// Integer arithmetic throughout: elapsed microseconds × rate, whole
/// units only, the unused fraction of the elapsed time carried to the
/// next refill (a double floor that under-refills by less than a
/// microsecond's worth of units — conservative, and deterministic under
/// a fake clock).
struct TokenBucket {
    rate: u64,
    burst: u64,
    tokens: i64,
    /// Sub-unit refill remainder, in elapsed microseconds.
    rem_micros: u64,
    /// The clock value at the last refill (`None` until the first).
    last: Option<Duration>,
}

impl TokenBucket {
    /// The longest pause one throttle slice takes (§9.3: stop is checked
    /// between slices).
    const SLICE: Duration = Duration::from_millis(10);

    fn new(budget: crate::jobs::IoBudget) -> TokenBucket {
        TokenBucket {
            rate: budget.units_per_sec,
            burst: budget.burst,
            tokens: budget.burst as i64,
            rem_micros: 0,
            last: None,
        }
    }

    /// Refills from `now` (capped at `burst`). A clock that stands still
    /// refills nothing — a fake clock only advances in `pause`.
    fn refill(&mut self, now: Duration) {
        if let Some(last) = self.last {
            let elapsed = now.saturating_sub(last);
            let micros = elapsed.as_micros() + self.rem_micros as u128;
            let add = micros * self.rate as u128 / 1_000_000;
            self.rem_micros = (micros - add * 1_000_000 / self.rate as u128) as u64;
            if add > 0 {
                self.tokens = self
                    .tokens
                    .saturating_add(i64::try_from(add).unwrap_or(i64::MAX))
                    .min(self.burst as i64);
            }
        }
        self.last = Some(now);
    }

    /// Charges `units` (a step's delta of the kind's `units` counter);
    /// the balance may go negative.
    fn charge(&mut self, units: u64) {
        self.tokens = self
            .tokens
            .saturating_sub(i64::try_from(units).unwrap_or(i64::MAX));
    }

    /// How long to pause before re-checking the balance: the time the
    /// current deficit still owes (at least one unit — a bucket at
    /// exactly zero waits for its first), capped at one slice. At least
    /// one microsecond, so a very high rate cannot make the slice zero
    /// and the wait a spin.
    fn wait_slice(&self) -> Duration {
        let deficit = (-self.tokens).max(1) as u128;
        let rate = self.rate as u128;
        let need_micros = (deficit * 1_000_000) / rate;
        let micros = need_micros.clamp(1, Self::SLICE.as_micros());
        Duration::from_micros(micros as u64)
    }
}

/// One job thread's throttle state: the bucket and the clock it refills
/// from (§9.3). Owned by the thread's body, so hand-driven callers never
/// see it — they charge the core's `units` counter, unthrottled.
struct Throttle {
    bucket: TokenBucket,
    clock: Arc<dyn JobClock>,
}

impl Throttle {
    /// Refills, then waits the deficit out in slices of at most
    /// [`TokenBucket::SLICE`], checking `stop` between them (a throttled
    /// shutdown stays prompt). Returns `false` when stop was seen — the
    /// caller exits before running the step.
    fn pace<K: OrderedKv>(&mut self, core: &Core<K>, kind: JobKind, stop: &AtomicBool) -> bool {
        self.bucket.refill(self.clock.now());
        while self.bucket.tokens <= 0 {
            if stop.load(Ordering::SeqCst) {
                return false;
            }
            let slice = self.bucket.wait_slice();
            self.clock.pause(slice);
            core.count_job_throttle(kind, 1, slice.as_micros() as u64);
            self.bucket.refill(self.clock.now());
        }
        true
    }
}

/// One running job of a set.
struct JobSlot {
    kind: JobKind,
    stop: Arc<AtomicBool>,
    joiner: Box<dyn Joiner>,
}

/// The bookkeeping every job thread of one set shares: the slots (for
/// fail-stop's "stop every job"), the first error and the shutdown log.
struct SetShared {
    slots: Mutex<Vec<JobSlot>>,
    first_error: Mutex<Option<TxnError>>,
    log: Mutex<Vec<ShutdownStep>>,
}

/// A set of background jobs over one core (§9.3), plus the optional commit
/// thread whose shutdown the set orders. Create with [`JobSet::new`],
/// install jobs with [`JobSet::start`], and shut everything down with
/// [`JobSet::shutdown`] (or just drop it).
pub struct JobSet<K: OrderedKv> {
    core: Arc<Core<K>>,
    spawner: Arc<dyn Spawner>,
    /// `Some` when built with [`JobSet::with_fail_stop`]; `None` reads the
    /// core's current hook at each error, so a hook installed after the
    /// set (e.g. by `CommitPipeline::with_config`) still reports.
    fail_stop: Option<Arc<dyn FailStop>>,
    /// The clock the set's I/O budgets run on (§9.3; C-B1c). Shared by
    /// every budgeted job of the set.
    clock: Arc<dyn JobClock>,
    commit_thread: Option<CommitThreadHandle>,
    shared: Arc<SetShared>,
}

impl<K: OrderedKv> JobSet<K> {
    /// An empty set over `core`, spawning std threads, reporting errors to
    /// the core's fail-stop hook (read at error time), pacing budgeted
    /// jobs on the monotonic [`SystemJobClock`].
    pub fn new(core: Arc<Core<K>>) -> JobSet<K> {
        JobSet {
            core,
            spawner: Arc::new(StdThreadSpawner),
            fail_stop: None,
            clock: Arc::new(SystemJobClock::new()),
            commit_thread: None,
            shared: Arc::new(SetShared {
                slots: Mutex::new(Vec::new()),
                first_error: Mutex::new(None),
                log: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Runs the set's I/O budgets on this clock (§9.3): a fake clock
    /// advances virtual time only in `pause`, so a test paces a job
    /// deterministically.
    pub fn with_clock(mut self, clock: Arc<dyn JobClock>) -> JobSet<K> {
        self.clock = clock;
        self
    }

    /// Uses this spawner for every job the set starts (C-R0b's seam).
    pub fn with_spawner(mut self, spawner: Arc<dyn Spawner>) -> JobSet<K> {
        self.spawner = spawner;
        self
    }

    /// Reports every error to this hook instead of the core's.
    pub fn with_fail_stop(mut self, fail_stop: Arc<dyn FailStop>) -> JobSet<K> {
        self.fail_stop = Some(fail_stop);
        self
    }

    /// The set shuts the commit thread down after the committing jobs
    /// (see [`SHUTDOWN_ORDER`]).
    pub fn attach_commit_thread(&mut self, handle: CommitThreadHandle) {
        self.commit_thread = Some(handle);
    }

    fn hook(&self) -> Arc<dyn FailStop> {
        match &self.fail_stop {
            Some(h) => Arc::clone(h),
            None => self.core.fail_stop(),
        }
    }

    /// Starts `job` on its own thread. One job per kind per set: a second
    /// is an `Invariant` error, and so is starting into a set that has
    /// already failed (the set's first error is returned — a failed set
    /// accepts no new work). The first step runs immediately. The slot's
    /// stop flag is created before spawning and pushed under the `slots`
    /// lock before the spawn returns, so a sibling's fail-stop during the
    /// start cannot slip between the spawn and the push (M1): the two run
    /// under the same lock. A spawn failure is recorded as the set's first
    /// error and reported to the fail-stop hook before it is returned.
    pub fn start(&mut self, job: Box<dyn Job>, config: JobConfig) -> Result<(), TxnError> {
        let kind = job.kind();
        if let Some(e) = self.first_error() {
            // The set has failed: no new job may join it (§9.3 — every
            // job in the set is told to stop, including one that would
            // start now).
            return Err(e);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let throttle = config
            .budget
            .filter(|b| b.units_per_sec > 0)
            .map(|b| Throttle {
                bucket: TokenBucket::new(b),
                clock: Arc::clone(&self.clock),
            });
        let body = job_thread_body(
            Arc::clone(&self.core),
            job,
            Arc::clone(&stop),
            config,
            throttle,
            Arc::clone(&self.shared),
            self.fail_stop.clone(),
        );
        let name = format!("nucleus-job-{}", kind.name());
        // One critical section covers the duplicate check, the spawn and
        // the push. `fail_the_set` stops jobs under this lock, so it can
        // never run between the spawn and the push and miss the new job.
        {
            let mut slots = self
                .shared
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if slots.iter().any(|s| s.kind == kind) {
                return Err(TxnError::Invariant(format!(
                    "a {} job is already running in this set (C-T0 §9.3)",
                    kind.name()
                )));
            }
            match self.spawner.spawn(name, Box::new(body)) {
                Ok(joiner) => {
                    slots.push(JobSlot { kind, stop, joiner });
                    Ok(())
                }
                Err(e) => {
                    // Released before the report: `record_error` and the
                    // hook take their own locks.
                    drop(slots);
                    self.record_error(&e);
                    self.hook().on_kv_error(&e);
                    Err(e)
                }
            }
        }
    }

    fn record_error(&self, e: &TxnError) {
        record_first(&self.shared, e);
    }

    /// The set's first recorded error (a job error, a panic seen at join,
    /// a spawn failure), if any.
    pub fn first_error(&self) -> Option<TxnError> {
        self.shared
            .first_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// One job's counters, straight from the core.
    pub fn stats(&self, kind: JobKind) -> crate::jobs::JobStats {
        self.core.job_stats(kind)
    }

    /// The shutdown steps that ran, in order (filled by `shutdown`; empty
    /// before it).
    pub fn shutdown_log(&self) -> Vec<ShutdownStep> {
        self.shared
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The ordered shutdown (§9.3), returning the set's first error (a
    /// clean set returns `Ok`). Takes `&mut self` (not `self`) so
    /// [`JobSet::shutdown_log`] and [`JobSet::first_error`] stay readable
    /// after it — the card's `shutdown(self)` would orphan both.
    pub fn shutdown(&mut self) -> Result<(), TxnError> {
        let r = self.shutdown_inner();
        let first = self
            .shared
            .first_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match first {
            Some(e) => Err(e),
            None => r,
        }
    }

    fn shutdown_inner(&mut self) -> Result<(), TxnError> {
        let mut result = Ok(());
        for step in SHUTDOWN_ORDER {
            match step {
                ShutdownStep::Job(kind) => {
                    if let Some(slot) = self.take_slot(kind) {
                        slot.stop.store(true, Ordering::SeqCst);
                        self.core.wake_job(kind);
                        if let Err(()) = slot.joiner.join() {
                            let e = TxnError::Invariant(format!(
                                "job {} panicked (observed at shutdown join)",
                                kind.name()
                            ));
                            record_first(&self.shared, &e);
                            self.hook().on_kv_error(&e);
                            result = Err(e);
                        }
                        self.log_step(ShutdownStep::Job(kind));
                    }
                }
                ShutdownStep::CommitThread => {
                    if let Some(handle) = self.commit_thread.take() {
                        if let Err(e) = handle.shutdown() {
                            record_first(&self.shared, &e);
                            result = Err(e);
                        }
                        self.log_step(ShutdownStep::CommitThread);
                    }
                }
            }
        }
        result
    }

    fn take_slot(&mut self, kind: JobKind) -> Option<JobSlot> {
        let mut slots = self
            .shared
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let idx = slots.iter().position(|s| s.kind == kind)?;
        Some(slots.remove(idx))
    }

    fn log_step(&self, step: ShutdownStep) {
        self.shared
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(step);
    }
}

impl<K: OrderedKv> Drop for JobSet<K> {
    fn drop(&mut self) {
        // The same ordered shutdown, ignoring the result. A panic observed
        // at a join is still recorded and reported to the hook inside
        // `shutdown_inner`.
        let _ = self.shutdown_inner();
    }
}

fn record_first(shared: &Arc<SetShared>, e: &TxnError) {
    let mut first = shared
        .first_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if first.is_none() {
        *first = Some(e.clone());
    }
}

/// The per-job thread body (see the module docs for the loop and the
/// fail-stop rules). A budgeted job throttles before each step (§9.3):
/// refill from the clock, then pause in ≤ 10 ms slices — checking stop
/// between slices — until the balance is positive; the step's units are
/// charged after it returns.
fn job_thread_body<K: OrderedKv>(
    core: Arc<Core<K>>,
    mut job: Box<dyn Job>,
    stop: Arc<AtomicBool>,
    config: JobConfig,
    mut throttle: Option<Throttle>,
    shared: Arc<SetShared>,
    fail_stop: Option<Arc<dyn FailStop>>,
) -> impl FnOnce() {
    move || {
        let kind = job.kind();
        loop {
            // Stop first: a shutdown or a sibling's fail-stop exits Ok.
            if stop.load(Ordering::SeqCst) {
                return;
            }
            // Take the pending flag BEFORE the step: a notify that lands
            // during the step stays pending and forces the next round
            // (§9.3 wake-on-enqueue; the lost-wake mutant is caught by
            // b1_jobs' `wake_during_step_is_not_lost`).
            core.take_job_wake(kind);
            core.count_job_round(kind);
            // §9.3 I/O budget: refill, then wait the deficit out in
            // slices, stop between them. Throttling never changes the
            // step, only delays it; an unbudgeted job skips this wholly.
            if let Some(t) = throttle.as_mut() {
                if !t.pace(&core, kind, &stop) {
                    // Stopped mid-throttle: exit before the step runs.
                    return;
                }
            }
            let units_before = core.job_units_now(kind);
            // M2: a panic inside the step is fail-stop for the set right
            // now — routed through `fail_the_set` — not a silently dead
            // thread observed at join time.
            let outcome = {
                let cx = JobCtx::new(&core.jobs, &stop, kind);
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.step(&cx)))
            };
            // The step's charge (§9.3): the delta of the kind's `units`
            // counter around the step, subtracted from the bucket.
            if let Some(t) = throttle.as_mut() {
                t.bucket
                    .charge(core.job_units_now(kind).saturating_sub(units_before));
            }
            let outcome = match outcome {
                Ok(r) => r,
                Err(_) => {
                    let e = TxnError::Invariant(format!("job {} panicked", kind.name()));
                    core.count_job_error(kind);
                    fail_the_set(&core, &shared, e, fail_stop.as_ref(), Some(kind));
                    return;
                }
            };
            match outcome {
                Ok(JobOutcome::Progress) => core.count_job_progress(kind),
                Ok(JobOutcome::Idle) => {
                    match core.wait_job_wake(kind, config.idle_interval, &stop) {
                        wake::WakeResult::Woken => core.count_job_wakeup(kind),
                        wake::WakeResult::TimedOut => core.count_job_idle_wait(kind),
                        wake::WakeResult::Stopped => return,
                    }
                }
                Ok(JobOutcome::Finished) => return,
                Err(e) => {
                    core.count_job_error(kind);
                    fail_the_set(&core, &shared, e, fail_stop.as_ref(), Some(kind));
                    // The failing job's own loop stops here (L4): it never
                    // spins on a KV error. Its own stop flag is not set —
                    // `fail_the_set` stops every *other* job — so a mutant
                    // that drops this return loops visibly instead of
                    // being masked by the flag.
                    return;
                }
            }
        }
    }
}

/// Fail-stop for the set (§3, §9.3): record the first error, stop every
/// **other** job in the set and wake it, run the hook. The failing job
/// itself exits by returning from its body (the caller does that right
/// after), which is what keeps a failed job from spinning on a KV error.
fn fail_the_set<K: OrderedKv>(
    core: &Arc<Core<K>>,
    shared: &Arc<SetShared>,
    e: TxnError,
    fail_stop: Option<&Arc<dyn FailStop>>,
    own_kind: Option<JobKind>,
) {
    record_first(shared, &e);
    // Stop every other job in the set and wake it: an idle job must leave
    // its park now, not after its interval.
    {
        let slots = shared.slots.lock().unwrap_or_else(PoisonError::into_inner);
        for slot in slots.iter() {
            if Some(slot.kind) == own_kind {
                continue;
            }
            slot.stop.store(true, Ordering::SeqCst);
            core.wake_job(slot.kind);
        }
    }
    let hook = match fail_stop {
        Some(h) => Arc::clone(h),
        None => core.fail_stop(),
    };
    hook.on_kv_error(&e);
}
