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
//! Wakeup protocol (no lost wakeups): a parked task checks `running` under
//! the world lock and then waits on its condvar, which releases the lock
//! atomically. A handoff sets `running` and signals the picked task's
//! condvar while holding the lock; `wake` only flips a waiter to runnable —
//! every later handoff considers it, and signalling it then is correct.

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
    /// An internal invariant of the spike was violated (a sizing or length
    /// assumption). Recorded as a PANIC-class per-seed failure by the
    /// harness — never a pass.
    #[error("internal: {0}")]
    Internal(&'static str),
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
    /// Mesa-style).
    pub fn wait_for(&self, me: TaskId, key: WaitKey) -> Result<(), SimFail> {
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
