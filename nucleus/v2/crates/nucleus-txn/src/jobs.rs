//! C-T0 §9.3 (draft 7.7, card C-B1a): the background-job framework. The
//! resolver, the boot sweep, GC, drop cleanup and SSI retention are
//! background jobs; merge and analyze are reserved slots. Each job runs in
//! bounded steps on a dedicated std thread (never an async executor
//! thread), makes every intent removal through §7.3, and reports a KV
//! error by failing the whole set stop (fail-stop, §3): the job stops,
//! every other job is told to stop, and the fail-stop hook runs.
//!
//! - [`JobKind`] names the seven slots (merge, analyze, drop_cleanup,
//!   resolver, boot_sweep, gc, ssi_retention). Merge and analyze stay
//!   reserved; C-B1b gives `DropCleanup` and `BootSweep` their bodies
//!   (`jobs::sweep`, `jobs::drop_cleanup`), alongside a
//!   [`LatchKeySource`] seam both use for §7.3 latch keys.
//! - [`Job`] is the body: `step` does bounded work and returns
//!   [`JobOutcome::Progress`] (more work is ready), [`JobOutcome::Idle`]
//!   (nothing to do; the runner parks on the job's wake for
//!   [`JobConfig::idle_interval`]) or [`JobOutcome::Finished`]. A job is
//!   not generic over `K`: it holds its own `Arc<Core<K>>`.
//! - [`JobCtx`] is what a step sees: its kind, the set's stop flag
//!   ([`JobCtx::is_stopping`]), the probe seam ([`JobCtx::probe`]) and
//!   the items counter ([`JobCtx::add_items`]).
//! - [`JobPoint`] + [`JobProbe`] are the named crash points inside the
//!   job bodies, modelled on the commit pipeline's
//!   [`CommitProbe`](crate::commit::CommitProbe)/[`ProbePoint`](crate::commit::ProbePoint)
//!   and driven the `Fault`/`CrashAt` way by the tests.
//! - [`runner`](self::runner) holds the [`runner::JobSet`]: the loop
//!   (wake taken **before** the step, so an enqueue during a step re-arms
//!   it), the [`runner::Spawner`] seam (C-R0b may place jobs on pools
//!   without changing job bodies) and the fixed shutdown order.
//! - [`metrics`](self::metrics) is the counter/gauge surface C-O3
//!   exports: [`Core::job_stats`](crate::boot::Core::job_stats) and
//!   [`Core::job_metrics`](crate::boot::Core::job_metrics).
//!
//! Every job's persisted effects are idempotent across a crash (§9.3):
//! resolution restarts from the boot sweep, GC from `/sys/gc_w`, drop
//! cleanup from the retired rows.
//!
//! C-B1c adds the §9.3 I/O budgets: a token bucket per budgeted job,
//! configured per job kind on the [`JobSet`](self::runner::JobSet)
//! (D4's defaults via [`IoBudget::default_for`], override with
//! [`JobSet::with_budget`](self::runner::JobSet::with_budget)), refilled
//! from an injectable [`JobClock`] (the default is the monotonic
//! [`SystemJobClock`]) and charged in the work units the job bodies
//! report through [`JobCtx::charge`] / [`Core::job_units`](crate::boot::Core::job_units)
//! (`nucleus_job_units_total`). Throttling only **delays** a step — it
//! pauses in slices of at most 10 ms, checking stop between slices
//! (`nucleus_job_throttle_waits_total`,
//! `nucleus_job_throttled_micros_total`) — and never changes what the
//! step does. Hand-driven callers (recovery harnesses, the simulator)
//! charge the same counters without throttling: the bucket lives in the
//! runner's thread.

pub mod metrics;
pub mod runner;
mod wake;

mod drop_cleanup;
mod sweep;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::boot::Core;
use crate::TxnError;

pub use drop_cleanup::DropCleanup;
pub use runner::{JobSet, ShutdownStep, SHUTDOWN_ORDER};
pub use sweep::BootSweep;
pub use wake::{Wake, WakeResult};

// ---- I/O budgets (§9.3, C-B1c) ------------------------------------------------

/// A job's I/O budget (§9.3): a token bucket over [`JobClock`] time,
/// charged in the work units the job bodies report. `units_per_sec` is
/// the refill rate, `burst` the bucket capacity (and the starting
/// balance). A `units_per_sec` of `0` disables the budget — a bucket
/// that never refills would park its job forever, which no caller wants.
///
/// Budgets are configured **per job kind on the [`JobSet`](crate::jobs::runner::JobSet)**
/// ([`JobSet::with_budget`](crate::jobs::runner::JobSet::with_budget));
/// [`IoBudget::default_for`] holds D4's defaults (Gc, BootSweep and
/// DropCleanup run on [`DEFAULT_JOB_BUDGET`]; the resolver, SSI
/// retention, merge and analyze carry none). Throttling only delays
/// steps and never changes their outcome (§9.3): the runner pauses in
/// slices of at most 10 ms, checking stop between slices, so shutdown
/// stays prompt however deep the deficit is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoBudget {
    pub units_per_sec: u64,
    pub burst: u64,
}

/// The budget D4 gives the budgeted kinds (Gc, BootSweep, DropCleanup):
/// 50 000 units/s, burst 5 000.
pub const DEFAULT_JOB_BUDGET: IoBudget = IoBudget {
    units_per_sec: 50_000,
    burst: 5_000,
};

impl IoBudget {
    /// D4's per-kind default: Gc, BootSweep and DropCleanup run on
    /// [`DEFAULT_JOB_BUDGET`]; the resolver, SSI retention, merge and
    /// analyze carry no budget. A `JobSet` starts every kind on this and
    /// `with_budget` overrides it.
    pub fn default_for(kind: JobKind) -> Option<IoBudget> {
        match kind {
            JobKind::Gc | JobKind::BootSweep | JobKind::DropCleanup => Some(DEFAULT_JOB_BUDGET),
            JobKind::Resolver | JobKind::SsiRetention | JobKind::Merge | JobKind::Analyze => None,
        }
    }
}

/// The clock the I/O budgets run on (§9.3): `now` is a monotonic
/// timestamp the bucket refills from, `pause` waits. Injectable so
/// tests pace a job on virtual time; the default is the monotonic
/// [`SystemJobClock`].
pub trait JobClock: Send + Sync {
    /// A monotonic timestamp. Implementations must not go backwards.
    fn now(&self) -> Duration;
    /// Waits for `d`. Implementations that keep virtual time advance it
    /// here (a budget's refill only ever observes time this way in the
    /// tests).
    fn pause(&self, d: Duration);
}

/// The default [`JobClock`]: `Instant::now` elapsed since the clock was
/// made, `pause` a plain thread sleep.
#[derive(Debug)]
pub struct SystemJobClock {
    start: Instant,
}

impl SystemJobClock {
    pub fn new() -> SystemJobClock {
        SystemJobClock {
            start: Instant::now(),
        }
    }
}

impl Default for SystemJobClock {
    fn default() -> Self {
        SystemJobClock::new()
    }
}

impl JobClock for SystemJobClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }

    fn pause(&self, d: Duration) {
        std::thread::sleep(d);
    }
}

/// §7.3 SD2b (draft 7.7): the seam the schema-less intent removers — the
/// boot sweep (§7.2) and retired-prefix cleanup (§9.2) — obtain
/// `latch_key(k)`'s deferrable prefix from. They hold only raw keys, so
/// they cannot know which `/i/{idx}/{key}{pk}` entries belong to a
/// deferrable unique constraint; the layer that defines those constraints
/// installs a source that does. Owner decision D2: until any deferrable
/// constraint can exist, the default [`NoDeferrableKeys`] answers `None`
/// and every key latches itself.
///
/// Install it (through [`Core::set_latch_key_source`]) **before starting
/// the boot sweep or the drop-cleanup job**: both read the source while
/// planning each removal. The source must answer for every key that can
/// hold an intent, including the ranges of dropped indexes and intents
/// placed by older epochs (§12 Q8) — an answer of `None` for a
/// deferrable entry would make the removal take a finer latch than the
/// §5.1 placement did.
pub trait LatchKeySource: Send + Sync {
    /// The deferrable-prefix length of `key` (§5.0): `Some(n)` when `key`
    /// is an `/i/{idx}/{key}{pk}` entry of a deferrable unique constraint
    /// (`latch_key(key) = &key[..n]`), `None` when the key latches itself.
    fn deferrable_prefix(&self, key: &[u8]) -> Option<usize>;
}

/// The default [`LatchKeySource`] (D2): no deferrable unique constraints
/// exist, so every key latches itself.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDeferrableKeys;

impl LatchKeySource for NoDeferrableKeys {
    fn deferrable_prefix(&self, _key: &[u8]) -> Option<usize> {
        None
    }
}

/// The seven job slots of §9.3. `DropCleanup` and `BootSweep` got their
/// bodies in C-B1b ([`jobs::drop_cleanup`](self::drop_cleanup),
/// [`jobs::sweep`](self::sweep)); merge and analyze stay reserved — their
/// slots, config defaults and counters exist so a later card only adds
/// the bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobKind {
    Merge,
    Analyze,
    DropCleanup,
    Resolver,
    BootSweep,
    Gc,
    SsiRetention,
}

impl JobKind {
    /// Every kind, in shutdown-relevant order (merge, analyze,
    /// drop_cleanup, resolver, boot_sweep, gc, ssi_retention).
    pub const ALL: [JobKind; 7] = [
        JobKind::Merge,
        JobKind::Analyze,
        JobKind::DropCleanup,
        JobKind::Resolver,
        JobKind::BootSweep,
        JobKind::Gc,
        JobKind::SsiRetention,
    ];

    /// The lowercase name used in thread names, counters and metrics.
    pub fn name(self) -> &'static str {
        match self {
            JobKind::Merge => "merge",
            JobKind::Analyze => "analyze",
            JobKind::DropCleanup => "drop_cleanup",
            JobKind::Resolver => "resolver",
            JobKind::BootSweep => "boot_sweep",
            JobKind::Gc => "gc",
            JobKind::SsiRetention => "ssi_retention",
        }
    }

    /// The index of this kind in [`JobKind::ALL`].
    pub(crate) fn index(self) -> usize {
        match self {
            JobKind::Merge => 0,
            JobKind::Analyze => 1,
            JobKind::DropCleanup => 2,
            JobKind::Resolver => 3,
            JobKind::BootSweep => 4,
            JobKind::Gc => 5,
            JobKind::SsiRetention => 6,
        }
    }
}

/// What one [`Job::step`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    /// Work remains: the runner loops at once, without parking.
    Progress,
    /// Nothing to do: the runner parks on the job's wake for
    /// [`JobConfig::idle_interval`], or until woken or stopped.
    Idle,
    /// The job is done forever: the thread exits Ok.
    Finished,
}

/// A background-job body (§9.3). Not generic over the KV: a job struct
/// holds its own `Arc<Core<K>>`. A step does bounded work and never parks
/// on another txn except through the documented session paths.
pub trait Job: Send {
    fn kind(&self) -> JobKind;
    fn step(&mut self, cx: &JobCtx) -> Result<JobOutcome, TxnError>;
}

/// What a step runs against: the job's kind, the set's stop flag for this
/// job, the probe seam and the items counter. Non-generic; built by the
/// runner (and by hand-driven callers through the job's `run_once`).
pub struct JobCtx<'a> {
    shared: &'a JobsShared,
    stop: &'a std::sync::atomic::AtomicBool,
    kind: JobKind,
}

impl<'a> JobCtx<'a> {
    pub(crate) fn new(
        shared: &'a JobsShared,
        stop: &'a std::sync::atomic::AtomicBool,
        kind: JobKind,
    ) -> JobCtx<'a> {
        JobCtx { shared, stop, kind }
    }

    /// The kind of the job this context runs a step of.
    pub fn kind(&self) -> JobKind {
        self.kind
    }

    /// Whether this job's stop flag is set (shutdown or a sibling job's
    /// fail-stop). A long step polls this between bounded units of work.
    pub fn is_stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Fires the job probe at `point` (forwards to the core's probe).
    pub fn probe(&self, point: JobPoint) {
        self.shared.probe_at(point);
    }

    /// Adds `n` to this job's `items` counter (keys removed, statuses
    /// truncated, ranges written, ...).
    pub fn add_items(&self, n: u64) {
        self.shared.counters[self.kind.index()]
            .items
            .fetch_add(n, Ordering::SeqCst);
    }

    /// Adds `n` to this job's `units` counter (§9.3 I/O budget): the
    /// work a step reports — stored entries scanned, keys written,
    /// `DeleteRange`s — in whatever unit the body chooses. The runner
    /// subtracts the step's delta from the job's token bucket; charging
    /// never blocks and never changes what the step does.
    pub fn charge(&self, n: u64) {
        self.shared.counters[self.kind.index()]
            .units
            .fetch_add(n, Ordering::SeqCst);
    }
}

/// The named crash points inside the job bodies (§9.3; the `Fault`/`CrashAt`
/// analogue of the commit pipeline's `ProbePoint`). Firing one is always
/// the last thing its body-phase did before the point's named effect
/// continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobPoint {
    /// After each successful intent removal in `Resolver::run_once`.
    ResolverAfterRemoval,
    /// Before the truncation loop of `Resolver::run_once`.
    ResolverBeforeTruncation,
    /// After each `truncate_status` that removed an entry.
    ResolverAfterTruncation,
    /// Right after `publish_computed_w` in `GcJob`'s publish.
    GcAfterPublish,
    /// Right after the `/sys/gc_w` `Durability::Yes` write, before the
    /// watermark reaches the KV.
    GcAfterWatermarkSync,
    /// After the tombstone pass's `DeleteRange` batch write.
    GcAfterTombstones,
    /// After one SSI retention pass.
    RetentionAfterPass,
    /// A merge/analyze slot job finished a step (test jobs).
    SlotStep,
    /// After each successful intent removal in the boot sweep (C-B1b).
    SweepAfterRemoval,
    /// When the sweep's walk is exhausted, right before `record_sweep`.
    SweepBeforeRecord,
    /// After each retired range's `DeleteRange` batch write inside
    /// `GcJob::retire_storage` (C-B1b).
    RetireAfterRange,
    /// After every range of a retired-storage row is retired, before the
    /// row's own delete txn begins.
    RetireBeforeMarkerDelete,
    /// After the retired-storage row's delete txn committed.
    RetireAfterMarkerDelete,
}

impl JobPoint {
    /// Every point.
    pub const ALL: [JobPoint; 13] = [
        JobPoint::ResolverAfterRemoval,
        JobPoint::ResolverBeforeTruncation,
        JobPoint::ResolverAfterTruncation,
        JobPoint::GcAfterPublish,
        JobPoint::GcAfterWatermarkSync,
        JobPoint::GcAfterTombstones,
        JobPoint::RetentionAfterPass,
        JobPoint::SlotStep,
        JobPoint::SweepAfterRemoval,
        JobPoint::SweepBeforeRecord,
        JobPoint::RetireAfterRange,
        JobPoint::RetireBeforeMarkerDelete,
        JobPoint::RetireAfterMarkerDelete,
    ];

    /// The job whose body fires this point.
    pub fn kind(self) -> JobKind {
        match self {
            JobPoint::ResolverAfterRemoval
            | JobPoint::ResolverBeforeTruncation
            | JobPoint::ResolverAfterTruncation => JobKind::Resolver,
            JobPoint::GcAfterPublish
            | JobPoint::GcAfterWatermarkSync
            | JobPoint::GcAfterTombstones => JobKind::Gc,
            JobPoint::RetentionAfterPass => JobKind::SsiRetention,
            JobPoint::SlotStep => JobKind::Merge, // either slot; see the tests
            JobPoint::SweepAfterRemoval | JobPoint::SweepBeforeRecord => JobKind::BootSweep,
            JobPoint::RetireAfterRange => JobKind::Gc,
            JobPoint::RetireBeforeMarkerDelete | JobPoint::RetireAfterMarkerDelete => {
                JobKind::DropCleanup
            }
        }
    }
}

/// The probe seam (§9.3): called at each [`JobPoint`] inside a job body,
/// so hand-driven callers (the simulator, tests) hit them too. Contract:
/// the probe runs with **no latch and no registry, SSI or wait-for-graph
/// mutex held** (it may run under the job's own step serialiser, e.g.
/// `GcJob::step`), and it must not call back into the same job.
pub trait JobProbe: Send + Sync {
    fn at(&self, point: JobPoint);
}

/// The no-op [`JobProbe`].
#[derive(Default)]
pub struct NoJobProbe;

impl JobProbe for NoJobProbe {
    fn at(&self, _point: JobPoint) {}
}

/// Per-job runner configuration: the idle park interval. The §9.3 I/O
/// budget is configured per job kind on the [`JobSet`](crate::jobs::runner::JobSet)
/// (D4 defaults via [`IoBudget::default_for`], override with
/// [`JobSet::with_budget`](crate::jobs::runner::JobSet::with_budget)) —
/// a budget only delays steps, never changes what they do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobConfig {
    /// How long the runner parks on the job's wake after an `Idle` step
    /// before running another round.
    pub idle_interval: Duration,
}

impl JobConfig {
    /// The default interval of one job kind: the resolver parks 50 ms
    /// (every release wakes it, so the interval is only a backstop), SSI
    /// retention 100 ms, and the rest 1 s.
    pub fn default_for(kind: JobKind) -> JobConfig {
        let idle_interval = match kind {
            JobKind::Resolver => Duration::from_millis(50),
            JobKind::SsiRetention => Duration::from_millis(100),
            JobKind::Merge
            | JobKind::Analyze
            | JobKind::DropCleanup
            | JobKind::BootSweep
            | JobKind::Gc => Duration::from_secs(1),
        };
        JobConfig { idle_interval }
    }
}

/// One job's counters (atomics). [`JobStats`] is the plain copy tests and
/// C-O3 read.
#[derive(Default)]
struct JobCounters {
    rounds: AtomicU64,
    progress_rounds: AtomicU64,
    idle_waits: AtomicU64,
    wakeups: AtomicU64,
    errors: AtomicU64,
    items: AtomicU64,
    /// §9.3 I/O budget: the work units the body charged (runner-driven or
    /// hand-driven — the counter is the core's, the bucket is the
    /// runner's).
    units: AtomicU64,
    throttle_waits: AtomicU64,
    throttled_micros: AtomicU64,
}

/// A snapshot of one job's counters (diagnostic; the metric surface C-O3
/// exports, one set per [`JobKind`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JobStats {
    pub rounds: u64,
    pub progress_rounds: u64,
    pub idle_waits: u64,
    pub wakeups: u64,
    pub errors: u64,
    pub items: u64,
    /// §9.3: units charged by this job's bodies.
    pub units: u64,
    /// §9.3: throttle pause slices taken (each ≤ 10 ms).
    pub throttle_waits: u64,
    /// §9.3: total time spent paused by throttling, in microseconds.
    pub throttled_micros: u64,
}

/// The per-`Core` shared state of the job framework: one [`Wake`] per
/// kind, one counter block per kind, and the probe. `Core` owns one; the
/// runner threads reach it through their `Arc<Core<K>>`, and hand-driven
/// callers through the job bodies.
pub(crate) struct JobsShared {
    wakes: [Wake; 7],
    counters: [JobCounters; 7],
    probe: Mutex<Arc<dyn JobProbe>>,
    /// The `nucleus_drop_cleanup_pending` gauge's value (§9.2): the
    /// drop-cleanup job's `seen` map size, refreshed after each rescan.
    drop_cleanup_pending: AtomicU64,
}

impl JobsShared {
    pub(crate) fn new() -> JobsShared {
        JobsShared {
            wakes: [
                Wake::new(),
                Wake::new(),
                Wake::new(),
                Wake::new(),
                Wake::new(),
                Wake::new(),
                Wake::new(),
            ],
            counters: Default::default(),
            probe: Mutex::new(Arc::new(NoJobProbe)),
            drop_cleanup_pending: AtomicU64::new(0),
        }
    }

    fn wake(&self, kind: JobKind) -> &Wake {
        &self.wakes[kind.index()]
    }

    /// The drop-cleanup pending gauge (§9.2): how many retired-storage
    /// rows the job has seen and not yet cleaned.
    pub(crate) fn drop_cleanup_pending(&self) -> u64 {
        self.drop_cleanup_pending.load(Ordering::SeqCst)
    }

    /// Fires the probe at `point`: the Arc is cloned, the mutex released,
    /// and only then is the probe called, so a probe body may install a
    /// new probe or touch anything the mutex guards. Also asserts the
    /// §9.3 probe contract (no latch held).
    pub(crate) fn probe_at(&self, point: JobPoint) {
        crate::latch::assert_no_latch_held();
        let probe = Arc::clone(&*self.probe.lock().unwrap_or_else(PoisonError::into_inner));
        probe.at(point);
    }

    fn stats(&self, kind: JobKind) -> JobStats {
        let c = &self.counters[kind.index()];
        JobStats {
            rounds: c.rounds.load(Ordering::SeqCst),
            progress_rounds: c.progress_rounds.load(Ordering::SeqCst),
            idle_waits: c.idle_waits.load(Ordering::SeqCst),
            wakeups: c.wakeups.load(Ordering::SeqCst),
            errors: c.errors.load(Ordering::SeqCst),
            items: c.items.load(Ordering::SeqCst),
            units: c.units.load(Ordering::SeqCst),
            throttle_waits: c.throttle_waits.load(Ordering::SeqCst),
            throttled_micros: c.throttled_micros.load(Ordering::SeqCst),
        }
    }

    /// The `units` counter of one kind (the runner measures a step's
    /// charge as the delta around the step).
    pub(crate) fn job_units_now(&self, kind: JobKind) -> u64 {
        self.counters[kind.index()].units.load(Ordering::SeqCst)
    }
}

impl<K: nucleus_kv::OrderedKv> Core<K> {
    /// Wakes the job of `kind` (§9.3 "wake on enqueue"): sets its wake's
    /// pending flag and notifies. Commit step 5, abort and the no-write
    /// fast path call this for the resolver; C-B1b's sweep and C-R0b's
    /// pools use it the same way. Idempotent and cheap; calling it with no
    /// job running is harmless.
    pub fn wake_job(&self, kind: JobKind) {
        self.jobs.wake(kind).notify();
    }

    /// Takes the job's pending wake flag (the runner does this before
    /// every step).
    pub(crate) fn take_job_wake(&self, kind: JobKind) {
        self.jobs.wake(kind).take_pending();
    }

    /// Parks on the job's wake until `timeout`, a notify, or `stop`.
    pub(crate) fn wait_job_wake(
        &self,
        kind: JobKind,
        timeout: std::time::Duration,
        stop: &std::sync::atomic::AtomicBool,
    ) -> wake::WakeResult {
        self.jobs.wake(kind).wait(timeout, stop)
    }

    pub(crate) fn count_job_round(&self, kind: JobKind) {
        self.jobs.counters[kind.index()]
            .rounds
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn count_job_progress(&self, kind: JobKind) {
        self.jobs.counters[kind.index()]
            .progress_rounds
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn count_job_idle_wait(&self, kind: JobKind) {
        self.jobs.counters[kind.index()]
            .idle_waits
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn count_job_wakeup(&self, kind: JobKind) {
        self.jobs.counters[kind.index()]
            .wakeups
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn count_job_error(&self, kind: JobKind) {
        self.jobs.counters[kind.index()]
            .errors
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Installs the job probe (§9.3). Tests drive crash points through it;
    /// the default is [`NoJobProbe`].
    pub fn set_job_probe(&self, probe: Arc<dyn JobProbe>) {
        *self
            .jobs
            .probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = probe;
    }

    /// Fires the job probe at `point` (the crate-internal path the job
    /// bodies use; `JobCtx::probe` forwards here).
    pub(crate) fn job_probe_at(&self, point: JobPoint) {
        self.jobs.probe_at(point);
    }

    /// Sets the `nucleus_drop_cleanup_pending` gauge (§9.2): the
    /// drop-cleanup job reports its `seen` map size after each rescan.
    pub(crate) fn set_drop_cleanup_pending(&self, n: usize) {
        self.jobs
            .drop_cleanup_pending
            .store(n as u64, Ordering::SeqCst);
    }

    /// One job's counters (the per-kind metric source of C-O3).
    pub fn job_stats(&self, kind: JobKind) -> JobStats {
        self.jobs.stats(kind)
    }

    /// Adds `n` to the `items` counter of `kind`'s job.
    pub(crate) fn job_items(&self, kind: JobKind, n: u64) {
        self.jobs.counters[kind.index()]
            .items
            .fetch_add(n, Ordering::SeqCst);
    }

    /// Adds `n` to the `units` counter of `kind`'s job (§9.3 I/O budget):
    /// the job-body path of [`JobCtx::charge`], for bodies that hold the
    /// core (the sweep, the GC job, drop cleanup) instead of a `JobCtx`.
    pub(crate) fn job_units(&self, kind: JobKind, n: u64) {
        self.jobs.counters[kind.index()]
            .units
            .fetch_add(n, Ordering::SeqCst);
    }

    /// The `units` counter of `kind`'s job (the runner measures a step's
    /// charge as the delta around the step; hand-driven callers charge the
    /// same counter, unthrottled).
    pub(crate) fn job_units_now(&self, kind: JobKind) -> u64 {
        self.jobs.job_units_now(kind)
    }

    /// Counts `waits` throttle pause slices of `micros` microseconds for
    /// `kind`'s job (§9.3; the runner is the only caller).
    pub(crate) fn count_job_throttle(&self, kind: JobKind, waits: u64, micros: u64) {
        let c = &self.jobs.counters[kind.index()];
        c.throttle_waits.fetch_add(waits, Ordering::SeqCst);
        c.throttled_micros.fetch_add(micros, Ordering::SeqCst);
    }
}
