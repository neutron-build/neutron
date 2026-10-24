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
    /// Trace of `(yield_count, picked_task)` handoffs, for the determinism
    /// self-test and diagnosis; bounded so long rounds stay cheap.
    pub trace: Vec<(u64, TaskId)>,
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
            trace: Vec::new(),
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

/// The baton scheduler over host `H`.
pub struct Sched<H: SchedHost> {
    host: Mutex<H>,
    cvs: Vec<Condvar>,
}

impl<H: SchedHost> Sched<H> {
    /// Build a scheduler around `host`; task 0 holds the baton.
    pub fn new(mut host: H) -> Self {
        let n = host.sched_mut().ntasks;
        let mut cvs = Vec::with_capacity(n);
        for _ in 0..n {
            cvs.push(Condvar::new());
        }
        Self {
            host: Mutex::new(host),
            cvs,
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
    pub fn acquire(&self, site: &'static str) -> Result<WorldGuard<'_, H>, SimFail> {
        if world_held_by_me() {
            // Re-entrant acquisition by this thread: the run-1 deadlock
            // shape, caught before it hangs.
            return Err(SimFail::Baton(site));
        }
        // The handoff-in-flight window: the task that handed the baton to
        // us may still hold the lock inside its own park for a moment.
        // Retry with a bounded yield loop; a genuine violator never frees
        // it and is reported rather than hung.
        for _ in 0..1_000_000 {
            match self.host.try_lock() {
                Ok(g) => {
                    WORLD_HELD.with(|h| h.set(true));
                    return Ok(WorldGuard { g });
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(SimFail::Baton(site));
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::yield_now();
                }
            }
        }
        Err(SimFail::Baton(site))
    }

    /// A plain yield point with a call-site name: gives up the baton, gets
    /// it back. Checks the S-BATON held flag first (rule 4): a task that
    /// parks while holding the world lock is the run-1 violation.
    pub fn yield_baton_at(&self, me: TaskId, site: &'static str) -> Result<(), SimFail> {
        if world_held_by_me() {
            return Err(SimFail::Baton(site));
        }
        self.yield_baton(me)
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

    /// A plain yield point: give up the baton, get it back. This is the
    /// primitive behind every `E4Env` call, `write_guard`, gate attempt,
    /// WAL enqueue and commit-boundary yield the card names (1.2).
    ///
    /// A task's first scheduler call goes through [`Self::park`] too: a
    /// thread that has not been handed the baton yet must not act (S-DET —
    /// otherwise OS thread start order leaks into the schedule).
    pub fn yield_baton(&self, me: TaskId) -> Result<(), SimFail> {
        self.park(me)?;
        let mut host = self.lock();
        {
            let core = host.sched_mut();
            if core.crashed || core.deadlock {
                return Err(SimFail::Crashed);
            }
            core.yields += 1;
            if let Some(t) = core.crash_at_yield
                && t == core.yields
            {
                core.crashed = true;
            }
            if core.crashed {
                core.running = None;
                for w in core.waiting.iter_mut() {
                    *w = None;
                }
                for cv in self.cvs.iter() {
                    cv.notify_all();
                }
                return Err(SimFail::Crashed);
            }
        }
        self.handoff_locked(&mut host)?;
        drop(host);
        self.park(me)
    }

    /// Park until we hold the baton again (or the world crashed).
    fn park(&self, me: TaskId) -> Result<(), SimFail> {
        let mut host = self.lock();
        loop {
            {
                let core = host.sched_mut();
                if core.crashed || core.deadlock {
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
    /// still alive — a per-seed failure.
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
                if any_alive && !core.crashed {
                    core.deadlock = true;
                    core.crashed = true;
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
        if let Some(cv) = self.cvs.get(pick) {
            cv.notify_one();
        }
        Ok(())
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
    /// baton on.
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
/// flag clears in this type's own `Drop`, which runs after the inner
/// guard has unlocked — no user code of this thread runs in between.
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
