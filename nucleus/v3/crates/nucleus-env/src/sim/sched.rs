// Provenance (card A-1b): nucleus/v3/spikes/e4-recovery/src/sim/sched.rs at 48ac99203f97d9dd5a53b1370ed141a294c4df0c, verbatim except departure S1 (card A-6b-0, A-1b-F1 (a)): await_first_baton, the existing park made a public entry; and the departure below (card FLAKE-FIX-1, 2026-11-21): `handoff_locked`'s deadlock condition gains `&& core.running != Some(HARNESS)`.
// The spike stays E4's evidence record; nothing under spikes/ changes (card A-1b).
// WaitKey's variants still name E4's waits (kept as copied; whether to generalise them is A-8a's question, named in this card's Notes).
// E4_SCHED_LOG is E4's X5-R14 diagnostic, off by default and never a digest input.
// Departure from that copy (card FLAKE-FIX-1, 2026-11-21): `handoff_locked`'s deadlock condition
// gains `&& core.running != Some(HARNESS)` — a world whose baton already sits with the harness
// while every workload task has finished is a completed life (the SIMENV-FLAKE-1 start/finish
// race in `run_life`), not a deadlock. E4's own sched.rs copy keeps the latent race unchanged
// (frozen evidence record).
#![deny(clippy::indexing_slicing)]
//! The baton scheduler (card X-5 Work 1.2).
//!
//! Every task is an OS thread. A baton serialises them so exactly one runs
//! at a time; at every yield point the scheduler picks the next runnable
//! task with the seed's PRNG. A task that cannot proceed marks itself
//! blocked (with a [`WaitKey`]) and yields — it never blocks the OS thread
//! while holding the baton. All tasks blocked is `DEADLOCK`; the crash
//! point is delivered as [`SimFail::Crashed`] at each task's next yield.
//!
//! The world lock: the host `H` (disk + store + harness state) lives in one
//! `Mutex`. The running task holds it between scheduler calls; each
//! scheduler call and each `E4Env` call releases it while parking, so no
//! borrow of host state ever survives a yield point (S-BATON). Because only
//! the baton holder runs, the host mutex is never contended and the logical
//! waits (gate, tree mutex, pin, WAL ack) are all expressed through
//! [`WaitKey`]s, never through OS blocking.
//!
//! **The S-BATON detector (card X-5b Work 11, rule 4).** The run-1 deadlock
//! shape is a task that holds the world lock across a yield: it parks its
//! OS thread inside the lock, and the baton can never pass — a hang. Two
//! additions make that shape loud instead:
//!
//! - store- and harness-side acquisitions go through [`Sched::acquire`],
//!   which `try_lock`s (a brief contention from a handoff in flight is
//!   retried; a holder that never releases reports
//!   [`SimFail::Baton`] naming the call site), and whose guard maintains a
//!   thread-local *held* flag;
//! - every entry point that parks ([`Sched::yield_baton_at`],
//!   [`Sched::wait_for`]) checks that flag first: a task that is about to
//!   park while still holding the world lock is exactly the run-1
//!   violation, and gets [`SimFail::Baton`] naming the yield site instead
//!   of hanging. The harness records it as a PANIC-class per-seed failure.
//!
//! [`Sched::lock`] stays a blocking acquisition for the scheduler's own
//! internals (never called with the flag set by construction) and for
//! X-5a's frozen tests, which never violate the discipline.
//!
//! Wakeup protocol (no lost wakeups): a parked task checks `running` under
//! the world lock and then waits on its condvar, which releases the lock
//! atomically. A handoff sets `running` and signals the picked task's
//! condvar while holding the lock; `wake` only flips a waiter to runnable —
//! every later handoff considers it, and signalling it then is correct.

use std::cell::Cell;
use std::sync::{Condvar, Mutex, MutexGuard};

use crate::rng::Rng;

/// The fixed per-world yield budget (X5-R13 (b), P ruling nv3-X-5c-esc-2,
/// 2026-10-31): 40× the largest world measured over seeds 0..199 (every
/// terminating world used fewer than 25,000 yields). A world whose yield
/// count exceeds it is **livelocked** — some task keeps yielding and never
/// parks, so "all tasks blocked" never holds and the deadlock detector can
/// never fire. The scheduler then fails the world exactly as the deadlock
/// path does and returns [`SimFail::Livelock`]; the harness records it on
/// the round (folded into the `deadlock` flags) — a recorded failure,
/// never a pass, never a hang.
///
/// The constant is fixed (the ruling) and recorded in the crate README.
/// It may change only as the fix for a seed whose root cause is a
/// legitimately long world (an instrumentation defect), followed by a
/// whole-campaign re-run. Yields are counted in sim order (under the
/// world lock, one increment per yield point), so the trip point is a
/// pure function of the schedule (S-DET/W-DET unchanged).
pub const YIELD_BUDGET: u64 = 1_000_000;

/// Identifies a task (an OS thread in the round).
pub type TaskId = usize;

/// The harness task. It parks on [`WaitKey::Join`] while the workload tasks
/// run and owns the baton before they start and after they all end.
pub const HARNESS: TaskId = 0;

/// What a blocked task is waiting for. Waking is keyed: releasing one kind
/// of resource wakes exactly its waiters, which re-check their condition
/// and may park again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitKey {
    /// The epoch gate, shared acquisition pending behind a flip.
    GateShared,
    /// The epoch gate exclusive (a flip waiting for holders).
    GateFlip,
    /// A per-tree sim mutex (card 3.6 replaces latch coupling with this).
    Tree(u64),
    /// A pool frame pin wait (§5.4).
    Pin,
    /// A WAL ack (SyncOn group sync, SyncOff write, or marker sync).
    WalAcks,
    /// A `mark_applied` turn (visible_ts advancing in ts order).
    Applied,
    /// The harness waiting for all workload tasks to end (crash or finish).
    Join,
    /// Bulk-load publication waiting for the forced meta sync (§12.8).
    MetaDurable,
}

/// Scheduling state, carried inside the host behind the world lock.
#[derive(Debug)]
pub struct SchedCore {
    /// Total tasks including the harness.
    pub ntasks: usize,
    /// Finished tasks never run again.
    pub finished: Vec<bool>,
    /// Tasks parked on a [`WaitKey`]; `None` means runnable.
    pub waiting: Vec<Option<WaitKey>>,
    /// Which task currently holds the baton.
    pub running: Option<TaskId>,
    /// The schedule PRNG (seeded by the seed; see S-DET).
    pub rng: Rng,
    /// Total yield points so far this life (the crash counter's domain).
    pub yields: u64,
    /// Crash at this yield number (inclusive), if armed.
    pub crash_at_yield: Option<u64>,
    /// Set when the crash fires or a deadlock is detected; every task sees
    /// it at its next scheduler call.
    pub crashed: bool,
    /// A deadlock (all tasks blocked) was detected.
    pub deadlock: bool,
    /// A livelock was detected (X5-R13 (b)): this world's `yields`
    /// exceeded [`YIELD_BUDGET`]. Set once, with the tripping site,
    /// when the world is failed; the harness folds it into the round's
    /// and seed's `deadlock` flags.
    pub livelock: bool,
    /// **X5-R14 (nv3-X-5c-esc-3): the deterministic unwind turn.** Set
    /// at every world-fail site (the crash trip, the livelock trip, the
    /// deadlock detection and `deliver_crash`) to the smallest
    /// `TaskId ≥ 1` that has not finished. From that moment until the
    /// world is dropped, [`Sched::acquire`] grants a host step only to
    /// this task (or to `HARNESS`), so the unwinding tasks' host
    /// mutations — store error-path cleanup, harness records, accepts —
    /// run in `TaskId` order instead of OS order. The ruling: "the
    /// baton passes through the unwinding tasks in TaskId order".
    /// `finish` advances it. `None` while the world has not failed.
    pub unwind_turn: Option<TaskId>,
    /// The call site of the yield that tripped [`YIELD_BUDGET`].
    pub livelock_site: &'static str,
    /// The park table at the trip (X5-R13 (b) diagnosis): each task's
    /// [`WaitKey`] just before the world was failed — the stuck pattern
    /// that the DEADLOCK detector could not see because some task kept
    /// yielding. Diagnostic only; never a record or a digest input.
    pub livelock_parked: Vec<Option<WaitKey>>,
    /// Trace of `(yield_count, picked_task)` handoffs, for the determinism
    /// self-test and diagnosis; bounded so long rounds stay cheap.
    pub trace: Vec<(u64, TaskId)>,
    /// Per-task yield counts (diagnosis only, X5-R13 (b)): which tasks
    /// were spinning when the budget tripped. Never a record or a digest
    /// input.
    pub yld_per_task: Vec<u64>,
}

impl SchedCore {
    /// New scheduling state for `ntasks` tasks; the harness (task 0) holds
    /// the baton initially and the workload tasks wait to be started.
    /// `seed` seeds the schedule PRNG.
    pub fn new(ntasks: usize, seed: u64) -> Self {
        let mut waiting = vec![Some(WaitKey::Join); ntasks];
        if let Some(w) = waiting.get_mut(HARNESS) {
            *w = None;
        }
        Self {
            ntasks,
            finished: vec![false; ntasks],
            waiting,
            running: Some(HARNESS),
            rng: Rng::new(seed),
            yields: 0,
            crash_at_yield: None,
            crashed: false,
            deadlock: false,
            livelock: false,
            unwind_turn: None,
            livelock_site: "",
            livelock_parked: Vec::new(),
            trace: Vec::new(),
            yld_per_task: vec![0; ntasks],
        }
    }
}

/// The host of a [`Sched`]: all per-seed state (disk, store, harness)
/// behind the world lock, plus its scheduling slice.
pub trait SchedHost {
    /// The scheduling slice of the host.
    fn sched_mut(&mut self) -> &mut SchedCore;
}

/// The failure modes a task can observe from the simulator itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SimFail {
    /// The crash point fired (or the world crashed); unwind.
    #[error("crashed")]
    Crashed,
    /// All tasks were blocked; a per-seed failure (card 1.2).
    #[error("deadlock")]
    Deadlock,
    /// The world exceeded [`YIELD_BUDGET`] (X5-R13 (b), P ruling
    /// nv3-X-5c-esc-2): some task kept yielding and never parked, so the
    /// deadlock detector could never fire. The string names the yield
    /// site that tripped it. The scheduler has already failed the world
    /// exactly as the deadlock path does; the harness records it on the
    /// round, folded into the `deadlock` flags — a recorded failure,
    /// never a pass.
    #[error("livelock at {0}")]
    Livelock(&'static str),
    /// An S-BATON violation (card X-5b Work 11, rule 4): a task tried to
    /// park (or to re-acquire the world) while holding the world lock, or
    /// the world lock was held by a task that stopped making progress.
    /// The string names the call site. Recorded as a PANIC-class per-seed
    /// failure by the harness — never a pass, never a hang.
    #[error("baton violation at {0}")]
    Baton(&'static str),
    /// An internal invariant of the spike was violated (a sizing or length
    /// assumption). Recorded as a PANIC-class per-seed failure by the
    /// harness — never a pass.
    #[error("internal: {0}")]
    Internal(&'static str),
}

// Whether the current OS thread holds the world lock through an
// `Sched::acquire` guard (the S-BATON detector's flag). Plain
// `Sched::lock` guards are not tracked: the scheduler's own internals
// never park while holding one, and the frozen X-5a tests use `lock`
// without violating the discipline.
thread_local! {
    static WORLD_HELD: Cell<bool> = const { Cell::new(false) };
}

/// Read the current thread's held flag (visible to tests for assertions).
pub fn world_held_by_me() -> bool {
    WORLD_HELD.with(|h| h.get())
}

/// The deterministic unwind turn (X5-R14): the smallest `TaskId ≥ 1`
/// that has not finished, or `None` when every workload task has. A
/// pure function of `finished`, so the turn order is a function of the
/// task set, never of OS timing.
fn smallest_unfinished(core: &SchedCore) -> Option<TaskId> {
    (1..core.ntasks).find(|t| !core.finished.get(*t).copied().unwrap_or(true))
}

/// The baton scheduler over host `H`.
pub struct Sched<H: SchedHost> {
    host: Mutex<H>,
    cvs: Vec<Condvar>,
    /// X5-R14 diagnostic: the optional host-mutation log (enabled only
    /// by `E4_SCHED_LOG=<path>`, default **off** — never a digest
    /// input, never committed enabled; the ruling's method names it for
    /// the non-determinism root-cause only).
    mutlog: Option<std::sync::Mutex<std::fs::File>>,
    /// X5-R14 diagnostic: this `Sched`'s world serial number in the log.
    world_no: u64,
}

// X5-R14 diagnostic: the global world counter and log path plumbing.
// Default off; see [`Sched::mutlog`].
fn sched_log_file() -> Option<std::sync::Mutex<std::fs::File>> {
    let path = std::env::var("E4_SCHED_LOG").ok()?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .ok()
        .map(std::sync::Mutex::new)
}

/// X5-R14 diagnostic: the process-wide world serial counter (only
/// touched when the log is enabled).
static WORLD_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl<H: SchedHost> Sched<H> {
    /// Build a scheduler around `host`; task 0 holds the baton.
    pub fn new(mut host: H) -> Self {
        let n = host.sched_mut().ntasks;
        let mut cvs = Vec::with_capacity(n);
        for _ in 0..n {
            cvs.push(Condvar::new());
        }
        let mutlog = sched_log_file();
        let world_no = if mutlog.is_some() {
            WORLD_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
        } else {
            0
        };
        Self {
            host: Mutex::new(host),
            cvs,
            mutlog,
            world_no,
        }
    }

    /// X5-R14 diagnostic: append one line to the host-mutation log when
    /// enabled. The thread id distinguishes tasks that the scheduler
    /// cannot name at this call (an `acquire` before the fix carried no
    /// task id); `detail` carries what the caller already knows under
    /// the lock (yield count, baton holder). Never called while the
    /// world lock is held by this thread.
    fn diag(&self, what: &str, detail: &str) {
        use std::io::Write as _;
        let Some(log) = &self.mutlog else {
            return;
        };
        let tid = format!("{:?}", std::thread::current().id());
        if let Ok(mut f) = log.lock() {
            let _ = writeln!(f, "W{ab} {what} {detail} th={tid}", ab = self.world_no);
        }
    }

    /// Lock the world. Only meaningful for the baton holder; every caller
    /// runs between scheduler calls by construction.
    pub fn lock(&self) -> MutexGuard<'_, H> {
        match self.host.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Acquire the world for one atomic step, S-BATON rule 4 (card X-5b
    /// Work 11): a `try_lock` that reports [`SimFail::Baton`] naming
    /// `site` instead of blocking when the lock cannot be taken. Brief
    /// contention from a baton handoff still in flight (the previous
    /// holder finishing its own park) is retried; a holder that never
    /// releases is a violation and is reported.
    ///
    /// **X5-R14 (P ruling nv3-X-5c-esc-3, the non-reproducing
    /// digests):** the acquisition is also the S-DET gate — a host step
    /// (`Cx::with`, every `E4Env` op, the accept hook) is granted only
    /// to the task the scheduler currently authorises:
    ///
    /// - **a live world** (`!crashed && !deadlock`): only the baton
    ///   holder (`running == Some(me)`). Before this gate, a task that
    ///   had not been handed the baton yet — a freshly spawned service
    ///   thread's loop-entry `Cx::with`, which can `wal_writer:take`
    ///   the WAL buffer or advance the pool clock in `evict:pick` —
    ///   could take host steps in OS start order, so the schedule (and
    ///   with it every digest) was a function of host timing, not of
    ///   `(seed, mutant)`. The refusal is loud ([`SimFail::Baton`]
    ///   naming the site), never a silent fall-through.
    /// - **a failed world** (`crashed || deadlock`, including the
    ///   livelock trip and `deliver_crash`): only [`Self::unwind_turn`]
    ///   — the deterministic unwind. Before it, every unwinding task's
    ///   cleanup (`unpin`, `tree_unlock`, `gate_release`, harness
    ///   records) ran concurrently in OS order with `running = None`.
    ///
    /// `HARNESS` is exempt: in the single-task recovery, extraction and
    /// checkpoint worlds it is the only task, and in a multi-task world
    /// the harness thread never acquires after the world fails (it
    /// joins; its post-join reads use [`Self::lock`], the scheduler's
    /// own blocking acquisition).
    pub fn acquire(&self, me: TaskId, site: &'static str) -> Result<WorldGuard<'_, H>, SimFail> {
        if world_held_by_me() {
            // Re-entrant acquisition by this thread: the run-1 deadlock
            // shape, caught before it hangs.
            return Err(SimFail::Baton(site));
        }
        // The acquisition loop never blocks on the world mutex: the
        // handoff-in-flight window (the task that handed the baton to
        // us may still hold the lock inside its own park for a moment)
        // is retried with a bounded yield loop, and the unwind-turn
        // wait parks on this task's condvar **while holding the lock**
        // (released atomically by the wait), so a genuine violator that
        // never frees the lock is reported ([`SimFail::Baton`]) rather
        // than hung on — the cross-thread discipline the gate's R6
        // correction fixed.
        let mut spins: u64 = 0;
        loop {
            match self.host.try_lock() {
                Ok(mut g) => {
                    loop {
                        // The X5-R14 S-DET gate, under the same lock as
                        // the step it authorises.
                        enum Gate {
                            Step,
                            AwaitTurn,
                            Refuse,
                        }
                        let verdict = {
                            let core = g.sched_mut();
                            if core.crashed || core.deadlock {
                                if me == HARNESS || core.unwind_turn == Some(me) {
                                    Gate::Step
                                } else {
                                    Gate::AwaitTurn
                                }
                            } else if core.running == Some(me) {
                                Gate::Step
                            } else {
                                Gate::Refuse
                            }
                        };
                        match verdict {
                            Gate::Step => {
                                WORLD_HELD.with(|h| h.set(true));
                                {
                                    let core = g.sched_mut();
                                    self.diag(
                                        "ACQ",
                                        &format!(
                                            "site={site} me={me} y={} run={:?} crashed={} turn={:?}",
                                            core.yields,
                                            core.running,
                                            core.crashed,
                                            core.unwind_turn
                                        ),
                                    );
                                }
                                return Ok(WorldGuard { g });
                            }
                            Gate::AwaitTurn => {
                                // Not this task's unwind turn yet:
                                // park on our condvar; `finish` advances
                                // the turn and notifies.
                                let Some(cv) = self.cvs.get(me) else {
                                    return Err(SimFail::Internal("cvs shorter than tasks"));
                                };
                                match cv.wait(g) {
                                    Ok(g2) => g = g2,
                                    Err(p) => {
                                        // Cannot be poisoned (no panic
                                        // crosses this boundary); stay
                                        // fail-stop rather than hang.
                                        drop(p.into_inner());
                                        return Err(SimFail::Baton(site));
                                    }
                                }
                            }
                            Gate::Refuse => {
                                // A host step without the baton (a live
                                // world): the loud refusal.
                                return Err(SimFail::Baton(site));
                            }
                        }
                    }
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(SimFail::Baton(site));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    spins += 1;
                    if spins > 1_000_000 {
                        return Err(SimFail::Baton(site));
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    /// A plain yield point with a call-site name: gives up the baton, gets
    /// it back. Checks the S-BATON held flag first (rule 4): a task that
    /// parks while holding the world lock is the run-1 violation. The
    /// name is also the livelock trip record's site (X5-R13 (b)).
    pub fn yield_baton_at(&self, me: TaskId, site: &'static str) -> Result<(), SimFail> {
        if world_held_by_me() {
            return Err(SimFail::Baton(site));
        }
        self.yield_baton_budget(me, site)
    }

    /// Release the workload tasks after construction (they wait on
    /// [`WaitKey::Join`] until this). Called by the harness while it holds
    /// the baton; the harness then parks on `Join` itself.
    pub fn start(&self) -> Result<(), SimFail> {
        let mut host = self.lock();
        {
            let core = host.sched_mut();
            for (t, w) in core.waiting.iter_mut().enumerate() {
                if t >= 1 {
                    *w = None;
                }
            }
        }
        self.park_locked(&mut host, HARNESS, WaitKey::Join)?;
        drop(host);
        self.park(HARNESS)
    }

    /// Park a workload task until the baton first reaches it, before its
    /// body runs (departure S1, card A-6b-0; A-1b-F1 (a)). The body is
    /// exactly one call of the private `park`, made a public entry:
    /// it returns once the baton is handed to `me` (by the harness's
    /// [`Self::start`] or by a later handoff), draws nothing from the
    /// schedule stream and counts no yield, so a body whose first call
    /// yields keeps its schedule and every `crash_at_yield` number. It
    /// returns `Err(SimFail::Crashed)` if the world failed first, as `park`
    /// does. Why: a body that calls an ungated or non-yielding step first
    /// (`rand`, `now`, `wake`, the accept hook) would otherwise act before
    /// its task holds the baton, in OS thread start order (S-DET).
    pub fn await_first_baton(&self, me: TaskId) -> Result<(), SimFail> {
        self.park(me)
    }

    /// A plain yield point: give up the baton, get it back. This is the
    /// primitive behind every `E4Env` call, `write_guard`, gate attempt,
    /// WAL enqueue and commit-boundary yield the card names (1.2). No
    /// call-site name: the trip record (X5-R13 (b)) names the generic
    /// `sched:yield` site for this form; prefer [`Self::yield_baton_at`].
    ///
    /// A task's first scheduler call goes through [`Self::park`] too: a
    /// thread that has not been handed the baton yet must not act (S-DET —
    /// otherwise OS thread start order leaks into the schedule).
    pub fn yield_baton(&self, me: TaskId) -> Result<(), SimFail> {
        self.yield_baton_budget(me, "sched:yield")
    }

    /// The site-named form of [`Self::yield_baton`], shared by both (the
    /// budget check lives here, once).
    fn yield_baton_budget(&self, me: TaskId, site: &'static str) -> Result<(), SimFail> {
        self.park(me)?;
        let mut host = self.lock();
        {
            let core = host.sched_mut();
            if core.crashed || core.deadlock {
                return Err(SimFail::Crashed);
            }
            core.yields += 1;
            if let Some(y) = core.yld_per_task.get_mut(me) {
                *y += 1;
            }
            if let Some(t) = core.crash_at_yield
                && t == core.yields
            {
                core.crashed = true;
            }
            if core.crashed {
                self.diag("CRASH-TRIP", &format!("y={}", core.yields));
                // X5-R14: the deterministic unwind begins — the turn is
                // the smallest unfinished task.
                core.unwind_turn = smallest_unfinished(core);
                core.running = None;
                for w in core.waiting.iter_mut() {
                    *w = None;
                }
                for cv in self.cvs.iter() {
                    cv.notify_all();
                }
                return Err(SimFail::Crashed);
            }
            // X5-R13 (b): a world past the budget is livelocked. Fail it
            // exactly as the deadlock path does (crashed, waiters
            // cleared, all notified) and return the new failure naming
            // the tripping site. Guarded by `!core.livelock` so the
            // single trip that sets the flag is also the only one that
            // reports it; every later caller sees `crashed` above.
            if core.yields > YIELD_BUDGET && !core.livelock {
                core.livelock = true;
                core.livelock_site = site;
                // Snapshot the stuck pattern before the world-fail
                // clears it (diagnosis; the deadlock detector never
                // saw it because some task kept yielding).
                core.livelock_parked = core.waiting.clone();
                core.crashed = true;
                // X5-R14: the deterministic unwind (as at the crash trip).
                core.unwind_turn = smallest_unfinished(core);
                core.running = None;
                for w in core.waiting.iter_mut() {
                    *w = None;
                }
                for cv in self.cvs.iter() {
                    cv.notify_all();
                }
                return Err(SimFail::Livelock(site));
            }
        }
        self.handoff_locked(&mut host)?;
        drop(host);
        self.park(me)
    }

    /// Park until we hold the baton again (or the world crashed).
    fn park(&self, me: TaskId) -> Result<(), SimFail> {
        self.diag("PARK-ENTER", &format!("me={me}"));
        let mut host = self.lock();
        loop {
            {
                let core = host.sched_mut();
                if core.crashed || core.deadlock {
                    self.diag("PARK-OUT", &format!("me={me} crashed y={}", core.yields));
                    return Err(SimFail::Crashed);
                }
                if core.running == Some(me) {
                    return Ok(());
                }
            }
            let Some(cv) = self.cvs.get(me) else {
                return Err(SimFail::Internal("cvs shorter than tasks"));
            };
            match cv.wait(host) {
                Ok(g) => host = g,
                Err(p) => {
                    // Cannot be poisoned (no panic crosses this boundary
                    // while holding the lock; panicking tasks are caught
                    // outside), but stay fail-stop rather than blocking
                    // forever.
                    drop(p.into_inner());
                    return Err(SimFail::Deadlock);
                }
            }
        }
    }

    /// Park the current task on `key` and hand the baton to someone else.
    /// The caller must re-check its condition after this returns and park
    /// again if it still does not hold (condition variables here are
    /// Mesa-style). Checks the S-BATON held flag first (rule 4).
    pub fn wait_for(&self, me: TaskId, key: WaitKey) -> Result<(), SimFail> {
        if world_held_by_me() {
            return Err(SimFail::Baton("wait_for"));
        }
        self.park(me)?;
        let mut host = self.lock();
        {
            let core = host.sched_mut();
            if core.crashed || core.deadlock {
                return Err(SimFail::Crashed);
            }
        }
        self.park_locked(&mut host, me, key)?;
        drop(host);
        self.park(me)
    }

    /// Mark `me` waiting on `key` and hand off, keeping the lock.
    fn park_locked(
        &self,
        host: &mut MutexGuard<'_, H>,
        me: TaskId,
        key: WaitKey,
    ) -> Result<(), SimFail> {
        if let Some(w) = host.sched_mut().waiting.get_mut(me) {
            *w = Some(key);
        }
        self.handoff_locked(host)
    }

    /// Pick the next runnable task and signal it. The caller must already
    /// be parked or leaving. Deadlock: no runnable task while some task is
    /// still alive — a per-seed failure — **unless the baton is already
    /// home with the harness** (FLAKE-FIX-1): "all tasks blocked" cannot
    /// be a deadlock when `running == Some(HARNESS)` and every workload
    /// task has finished — `finish`'s all-done branch already answered
    /// the harness's `Join`, and a `start` that parks late falls through
    /// to the everyone-finished handoff below (whose re-setting
    /// `running = Some(HARNESS)` and notifying is a harmless no-op).
    /// `running == Some(HARNESS)` with alive workload tasks is
    /// unreachable by construction — `running` leaves the harness only
    /// via a handoff to a workload task and returns only via `finish`'s
    /// all-done branch or this fall-through — so no genuine deadlock is
    /// suppressed.
    fn handoff_locked(&self, host: &mut MutexGuard<'_, H>) -> Result<(), SimFail> {
        let (yields, pick) = {
            let core = host.sched_mut();
            let mut cands: Vec<TaskId> = Vec::new();
            for (t, w) in core.waiting.iter().enumerate() {
                let done = core.finished.get(t).copied().unwrap_or(true);
                if !done && w.is_none() {
                    cands.push(t);
                }
            }
            if cands.is_empty() {
                let any_alive = core.finished.iter().any(|f| !*f);
                // FLAKE-FIX-1: the baton already sitting home with the
                // harness means every workload task has finished
                // (unreachable by construction otherwise, per the fn
                // docs) — a completed life, not a deadlock. This is the
                // SIMENV-FLAKE-1 start/finish race: `finish`'s all-done
                // branch handed the baton home and notified a harness
                // that had not parked yet; the late `start` parks here
                // and must fall through to the everyone-finished handoff
                // below instead of failing the healthy world.
                if any_alive && !core.crashed && core.running != Some(HARNESS) {
                    core.deadlock = true;
                    core.crashed = true;
                    // X5-R14: the deterministic unwind (as at the crash
                    // trip) — the deadlock release also empties
                    // `running`, so unwinding cleanup must be turn-
                    // ordered here too.
                    core.unwind_turn = smallest_unfinished(core);
                    core.running = None;
                    for w in core.waiting.iter_mut() {
                        *w = None;
                    }
                    for cv in self.cvs.iter() {
                        cv.notify_all();
                    }
                    return Err(SimFail::Deadlock);
                }
                // Everyone finished; the baton goes to the harness.
                core.running = Some(HARNESS);
                if let Some(cv) = self.cvs.get(HARNESS) {
                    cv.notify_one();
                }
                return Ok(());
            }
            let idx = core.rng.below(cands.len() as u64) as usize;
            let pick = cands.get(idx).copied().ok_or(SimFail::Internal("pick"))?;
            core.running = Some(pick);
            (core.yields, pick)
        };
        if host.sched_mut().trace.len() < 4096 {
            host.sched_mut().trace.push((yields, pick));
        }
        self.diag("HANDOFF", &format!("y={yields} -> {pick}"));
        if let Some(cv) = self.cvs.get(pick) {
            cv.notify_one();
        }
        Ok(())
    }

    /// Deliver a crash now (card X-5b Work 13's hook and task-yield crash
    /// points): mark the world crashed, make every waiter runnable and
    /// broadcast, so each task unwinds with `Crashed` at its next
    /// scheduler call instead of parking forever.
    pub fn deliver_crash(&self) {
        let mut host = self.lock();
        self.diag("DELIVER-CRASH", "manual");
        let cvs: Vec<&Condvar> = self.cvs.iter().collect();
        {
            let core = host.sched_mut();
            core.crashed = true;
            // X5-R14: the deterministic unwind begins here too (the
            // hook/task-yield crash arms deliver directly).
            core.unwind_turn = smallest_unfinished(core);
            core.running = None;
            for w in core.waiting.iter_mut() {
                *w = None;
            }
        }
        for cv in cvs {
            cv.notify_all();
        }
    }

    /// Wake every task parked on `key` (their condition may have changed).
    /// They become candidates for the next handoff; no signal is needed
    /// (see the module's wakeup protocol).
    pub fn wake(&self, key: WaitKey) {
        let mut host = self.lock();
        let core = host.sched_mut();
        for w in core.waiting.iter_mut() {
            if *w == Some(key) {
                *w = None;
            }
        }
    }

    /// A task ended its life; it never runs again. The last workload task
    /// to finish wakes the harness; a finishing baton holder hands the
    /// baton on. In a failed world it also advances the deterministic
    /// unwind turn (X5-R14): the turn moves to the smallest unfinished
    /// task and every turn waiter is woken to re-check.
    pub fn finish(&self, me: TaskId) {
        let mut host = self.lock();
        let all_done = {
            let core = host.sched_mut();
            if let Some(f) = core.finished.get_mut(me) {
                *f = true;
            }
            if let Some(w) = core.waiting.get_mut(me) {
                *w = None;
            }
            core.finished.iter().skip(1).all(|f| *f)
        };
        if host.sched_mut().crashed || host.sched_mut().deadlock {
            let core = host.sched_mut();
            core.unwind_turn = smallest_unfinished(core);
            for cv in self.cvs.iter() {
                cv.notify_all();
            }
        }
        if all_done {
            let core = host.sched_mut();
            for w in core.waiting.iter_mut() {
                if *w == Some(WaitKey::Join) {
                    *w = None;
                }
            }
            core.running = Some(HARNESS);
            if let Some(cv) = self.cvs.get(HARNESS) {
                cv.notify_all();
            }
            return;
        }
        if host.sched_mut().running == Some(me) {
            host.sched_mut().running = None;
            let _ = self.handoff_locked(&mut host);
        }
    }
}

/// The world guard handed out by [`Sched::acquire`]: a `MutexGuard` that
/// maintains the S-BATON thread-local held flag, so the parking entry
/// points can detect a task about to park while holding the world. The
/// flag clears in this type's own `Drop` body, which runs **before** the
/// inner guard unlocks (field drops happen after `Drop::drop`) — the
/// flag is already clear when the lock is free again.
pub struct WorldGuard<'a, H: SchedHost> {
    g: MutexGuard<'a, H>,
}

impl<H: SchedHost> Drop for WorldGuard<'_, H> {
    fn drop(&mut self) {
        WORLD_HELD.with(|h| h.set(false));
    }
}

impl<H: SchedHost> std::ops::Deref for WorldGuard<'_, H> {
    type Target = H;
    fn deref(&self) -> &H {
        &self.g
    }
}

impl<H: SchedHost> std::ops::DerefMut for WorldGuard<'_, H> {
    fn deref_mut(&mut self) -> &mut H {
        &mut self.g
    }
}
