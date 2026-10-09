//! C-T0 §6, the parts this card owns: parking and wake generations, without
//! the wait-for graph (C-T2b builds the graph around [`WaitHook`]).
//!
//! Every txn has a wake generation `gen(T)` in its status entry, bumped on
//! every event that can unblock its waiters: commit step 5 (§3), abort
//! (§7.1), `ROLLBACK TO`/[`Core::bump_and_wake`], any release of an
//! in-memory lock it holds. Whoever wakes waiters **bumps first, then wakes**
//! (§6 "Waking"), so a parked waiter always sees the new generation when it
//! re-checks.
//!
//! [`Core::wait_on`] implements §6 "Waiting" steps 1–3 without the graph:
//! register as a waiter, re-check (the gen-recheck rule that makes a lost
//! wakeup between the generation read and the registration impossible), then
//! park until the target's generation changes or the waiter is cancelled.
//! Parking goes through the [`Parker`] trait; the simulator replaces the
//! maker. A thread never holds a latch across a park (§1): `wait_on` takes
//! none.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::boot::Core;
use crate::status::Remembered;
use crate::txn::{CancelFlag, Txn};
use crate::{Ts, TxnId, TxnStatus};

/// How long a parked waiter re-checks at worst before timing out and parking
/// again. Wakeups are generation-driven; the timeout only bounds a lost
/// wakeup as a slow test instead of a hang.
const PARK_SLICE: Duration = Duration::from_secs(1);

/// Parking seam (§6). `park` blocks the calling thread until [`unpark`] or
/// the timeout; it returns `true` when unparked, `false` on timeout. The
/// std implementation is a condvar; the deterministic simulator replaces the
/// maker so it can schedule wakeups.
pub trait Parker: Send + Sync {
    fn park(&self, timeout: Duration) -> bool;
    fn unpark(&self);
}

/// Makes one [`Parker`] per wait (the simulator swaps the implementation).
pub trait MakeParker: Send + Sync {
    fn make(&self) -> Arc<dyn Parker>;
}

/// The std condvar [`Parker`].
pub struct CondvarParker {
    unparked: Mutex<bool>,
    cv: Condvar,
}

impl CondvarParker {
    pub fn new() -> CondvarParker {
        CondvarParker {
            unparked: Mutex::new(false),
            cv: Condvar::new(),
        }
    }
}

impl Default for CondvarParker {
    fn default() -> Self {
        Self::new()
    }
}

impl Parker for CondvarParker {
    fn park(&self, timeout: Duration) -> bool {
        let mut f = self.unparked.lock().unwrap_or_else(PoisonError::into_inner);
        if !*f {
            let (guard, res) = self
                .cv
                .wait_timeout(f, timeout)
                .unwrap_or_else(PoisonError::into_inner);
            f = guard;
            if res.timed_out() && !*f {
                return false;
            }
        }
        let woke = *f;
        *f = false;
        woke
    }

    fn unpark(&self) {
        let mut f = self.unparked.lock().unwrap_or_else(PoisonError::into_inner);
        *f = true;
        drop(f);
        self.cv.notify_all();
    }
}

/// The default [`MakeParker`]: one [`CondvarParker`] per wait.
#[derive(Default)]
pub struct CondvarParkers;

impl MakeParker for CondvarParkers {
    fn make(&self) -> Arc<dyn Parker> {
        Arc::new(CondvarParker::new())
    }
}

/// The C-T2b hook: called at the start and end of every wait, exactly where
/// the wait-for graph's edge insert/remove runs. No-op by default.
pub trait WaitHook: Send + Sync {
    fn on_wait_start(&self, waiter: TxnId, target: TxnId);
    fn on_wait_end(&self, waiter: TxnId, target: TxnId);
}

/// The no-op [`WaitHook`].
#[derive(Default)]
pub struct NoWaitHook;

impl WaitHook for NoWaitHook {
    fn on_wait_start(&self, _waiter: TxnId, _target: TxnId) {}
    fn on_wait_end(&self, _waiter: TxnId, _target: TxnId) {}
}

/// Why [`Core::wait_on`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The target's status entry is gone: it ended and its release ran
    /// (§4, §7.4). Like `Aborted` or a fully-released visible commit for
    /// every caller decision.
    Ended,
    /// The target is Aborted.
    Aborted,
    /// The target is a visible commit: `commit_ts <= visible_ts`.
    Committed(Ts),
    /// The target's wake generation is no longer `g` (§6 step 2): it was
    /// woken by commit step 5, abort, `ROLLBACK TO` or a lock release.
    GenChanged,
    /// The waiter's cancel flag was set (§6).
    Cancelled,
}

/// One registered waiter: its txn and the parker it parks on.
struct WaitSlot {
    waiter: TxnId,
    parker: Arc<dyn Parker>,
}

/// The waiter tables: per-target lists of registered waiters, the parker
/// maker and the wait hook. Guarded by one mutex; never held across a park
/// or together with a latch or the registry mutex (§3.1 lock order — this
/// mutex stands where C-T2b puts the graph mutex).
pub struct Waits {
    tables: Mutex<BTreeMap<TxnId, Vec<Arc<WaitSlot>>>>,
    parkers: Mutex<Arc<dyn MakeParker>>,
    hook: Mutex<Arc<dyn WaitHook>>,
}

impl Waits {
    pub(crate) fn new() -> Waits {
        Waits {
            tables: Mutex::new(BTreeMap::new()),
            parkers: Mutex::new(Arc::new(CondvarParkers)),
            hook: Mutex::new(Arc::new(NoWaitHook)),
        }
    }

    fn lock_tables(&self) -> MutexGuard<'_, BTreeMap<TxnId, Vec<Arc<WaitSlot>>>> {
        self.tables.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replaces the parker maker (the simulator's seam).
    pub fn set_parker_maker(&self, maker: Arc<dyn MakeParker>) {
        *self.parkers.lock().unwrap_or_else(PoisonError::into_inner) = maker;
    }

    /// Installs the wait hook (C-T2b's seam around every wait).
    pub fn set_hook(&self, hook: Arc<dyn WaitHook>) {
        *self.hook.lock().unwrap_or_else(PoisonError::into_inner) = hook;
    }

    fn hook(&self) -> Arc<dyn WaitHook> {
        Arc::clone(&self.hook.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn make_parker(&self) -> Arc<dyn Parker> {
        self.parkers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .make()
    }

    fn register(&self, target: TxnId, slot: Arc<WaitSlot>) {
        self.lock_tables().entry(target).or_default().push(slot);
    }

    fn unregister(&self, target: TxnId, waiter: TxnId) {
        let mut t = self.lock_tables();
        if let Some(list) = t.get_mut(&target) {
            list.retain(|s| s.waiter != waiter);
            if list.is_empty() {
                t.remove(&target);
            }
        }
    }

    /// Wakes every waiter registered on `target` (§6 "Waking"): the caller
    /// bumped the generation first. Spurious unparks are harmless — waiters
    /// re-check the generation.
    pub(crate) fn wake(&self, target: TxnId) {
        let slots = self.lock_tables().remove(&target);
        if let Some(slots) = slots {
            for s in slots {
                s.parker.unpark();
            }
        }
    }
}

impl<K: nucleus_kv::OrderedKv> Core<K> {
    /// §6 "Waiting" steps 1–3 on one target:
    /// [`Core::wait_on_any`] with a one-element target list.
    pub fn wait_on(&self, waiter: &Txn, target: TxnId, g: u64) -> WaitOutcome {
        self.wait_on_any(waiter, &[(target, g)])
    }

    /// §6 "Waiting" steps 1–3 over several targets (C-T0 §5.1: a row op may
    /// have to wait on a foreign intent's owner and on every conflicting
    /// shared-row-lock holder at once):
    /// 1. the hook fires and the waiter registers on **every** target;
    /// 2. re-check (the gen-recheck rule): return at once if any target has
    ///    ended, is Aborted, is a visible commit, or its `gen != g`;
    /// 3. park until any target's generation changes, any target ends, or
    ///    the waiter is cancelled, re-checking after every wake.
    ///
    /// Every `g` is the generation the caller read where it observed the
    /// conflict (under the key's latch, §5.1). All targets share one parker,
    /// so any target's wake (or the cancel flag) unblocks the park. The
    /// waiter unregisters from every target and fires `on_wait_end` for each
    /// on **every** return path. Never called with a latch held.
    pub fn wait_on_any(&self, waiter: &Txn, targets: &[(TxnId, u64)]) -> WaitOutcome {
        debug_assert!(
            !targets.is_empty(),
            "wait_on_any with no targets: there is nothing to wait on"
        );
        if targets.is_empty() {
            // Defensive: an empty wait is vacuously over (§4: a missing
            // status means ended and released).
            return WaitOutcome::Ended;
        }
        let hook = self.waits.hook();
        let parker = self.waits.make_parker();
        let slot = Arc::new(WaitSlot {
            waiter: waiter.id,
            parker: Arc::clone(&parker),
        });
        for (t, _) in targets {
            hook.on_wait_start(waiter.id, *t);
            self.waits.register(*t, Arc::clone(&slot));
        }
        let outcome = self.park_until_unblocked(targets, &parker, &waiter.cancel);
        for (t, _) in targets {
            self.waits.unregister(*t, waiter.id);
            hook.on_wait_end(waiter.id, *t);
        }
        outcome
    }

    /// The park loop. Split from [`Core::wait_on_any`] so the register step
    /// is visibly before the first re-check (§6 step 2's order: register,
    /// then re-check — that is what makes a lost wakeup impossible).
    fn park_until_unblocked(
        &self,
        targets: &[(TxnId, u64)],
        parker: &Arc<dyn Parker>,
        cancel: &CancelFlag,
    ) -> WaitOutcome {
        // Publish the parker so `cancel()` can unpark this park (§6). The
        // store happens before the loop's first flag re-check, which closes
        // the set-then-store race: a cancel that ran before the store is
        // either already visible to the flag check below, or its unpark
        // lands on the stored parker.
        cancel.set_parked(Arc::clone(parker));
        let outcome = self.wait_loop(targets, parker, cancel);
        // Every return path of the wait clears the publication.
        cancel.clear_parked();
        outcome
    }

    fn wait_loop(
        &self,
        targets: &[(TxnId, u64)],
        parker: &Arc<dyn Parker>,
        cancel: &CancelFlag,
    ) -> WaitOutcome {
        loop {
            // §6 step 2 re-check, per target, in order: missing status first
            // (`gen` lives in the status entry), then Aborted, then visible
            // commit, then the generation. The first target that unblocked
            // decides the outcome.
            for (target, g) in targets {
                match self.status.lookup_remembered(*target) {
                    Remembered::Ended => return WaitOutcome::Ended,
                    Remembered::Live(TxnStatus::Aborted, _) => return WaitOutcome::Aborted,
                    Remembered::Live(TxnStatus::Committed(c), _) if c <= self.visible_ts() => {
                        return WaitOutcome::Committed(c);
                    }
                    // Committed but not yet visible, or Pending: keep
                    // waiting; commit step 5 (after visibility) bumps the
                    // generation.
                    Remembered::Live(_, gen) if gen != *g => return WaitOutcome::GenChanged,
                    Remembered::Live(..) => {}
                }
            }
            if cancel.is_cancelled() {
                return WaitOutcome::Cancelled;
            }
            // Park (§6: the cancel path unparks this parker too). A
            // timeout re-enters the loop and re-checks everything.
            parker.park(PARK_SLICE);
        }
    }

    /// Bumps `txn`'s wake generation and wakes its waiters (§6): the wake
    /// path of `ROLLBACK TO` (§5.5) and of every in-memory lock release.
    /// Bump first, then wake.
    pub fn bump_and_wake(&self, txn: TxnId) -> Result<u64, crate::TxnError> {
        let gen = self.status.bump_gen(txn)?;
        self.waits.wake(txn);
        Ok(gen)
    }
}
