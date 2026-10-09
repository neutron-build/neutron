//! C-T0 §6, the parts this card owns: the **wait-for graph**, parking and
//! wake generations, deadlock detection after `deadlock_timeout`, and
//! `lock_timeout`.
//!
//! Every txn has a wake generation `gen(T)` in its status entry, bumped on
//! every event that can unblock its waiters: commit step 5 (§3), abort
//! (§7.1), `ROLLBACK TO`/[`Core::bump_and_wake`], any release of an
//! in-memory lock it holds. Whoever wakes waiters **bumps first, then
//! wakes** (§6 "Waking"), so a parked waiter always sees the new
//! generation when it re-checks.
//!
//! ## The graph
//!
//! One **graph mutex** guards both the edges `waiter -> target` and every
//! target's waiter slots. Every wait kind goes through it: row intents,
//! shared row locks, relation locks, advisory locks, deferrable-prefix
//! waits (§6: "One graph covers every kind of wait"). A wait is split into
//! non-blocking steps so tests and the deterministic simulator (C-SIM) can
//! drive it on one thread; the blocking wait ([`Core::wait_on_any`]) is a
//! loop over those steps:
//!
//! - **Begin** ([`Core::wait_begin`], §6 "Waiting" steps 1–2): under the
//!   graph mutex, re-check every target (missing status first — `gen`
//!   lives in the status entry — then Aborted, visible commit, `gen != g`,
//!   then the waiter's cancel flag). If any target is unblocked, return
//!   [`WaitBegin::Done`] without inserting anything. Otherwise insert an
//!   edge to **every** target and register the waiter's slot on each, then
//!   release the mutex. Re-check and insert in one critical section means
//!   no edge is ever inserted for a wait that was already satisfied
//!   (I-LIVE c). Status lookups inside the graph mutex are allowed (the
//!   status table is a leaf, §3.1); a latch or the registry mutex never is.
//! - **Poll** ([`Core::wait_poll`]): re-runs the step-2 checks (no mutex
//!   beyond the status table) and returns `Some` when the wait is over.
//! - **Deadlock check** ([`Core::wait_deadlock_check`]): under the graph
//!   mutex, a DFS from the waiter over the current edges; if a cycle
//!   contains the waiter, remove the waiter's own edges and slots **before
//!   releasing the mutex** and return `true` (the caller raises 40P01).
//!   Exactly one member of a cycle aborts (I-LIVE b).
//! - **End** ([`Core::wait_end`]): removes this wait's remaining edges
//!   and slots. [`WaitHandle`]'s `Drop` does the same (and is a no-op
//!   after `wait_end`), so no path leaks an edge. Every registration has
//!   its own id and is removed by id, so ending or dropping one wait never
//!   touches another wait of the same txn.
//! - **Wake** ([`Core::bump_and_wake`], §6 "Waking"): the one wake path
//!   (commit step 5, abort, `ROLLBACK TO`, any lock release). Under the
//!   graph mutex it bumps the target's generation (the status table is a
//!   leaf, §3.1), removes every edge `* -> target` and takes the target's
//!   slots; it releases the mutex and only then unparks them (seed 59:
//!   the DFS never sees an edge for a wait that has already been
//!   satisfied). Bump and edge removal in one critical section mean a
//!   waiter that registers with the **new** generation always registers
//!   after the removal, so its edge and slot survive (no missed deadlock,
//!   no lost wakeup).
//!
//! `deadlock_timeout` (default 1 s, PostgreSQL's default) is a [`Waits`]
//! setting: after it has elapsed since the wait began, the blocking wait
//! runs the deadlock check **once** for that wait. `lock_timeout` is
//! per-statement (`StmtCtx::lock_timeout`, C-T2b): past the deadline the
//! wait returns [`WaitOutcome::LockTimeout`] (55P03, §6). Parking goes
//! through the [`Parker`] trait; the simulator replaces the maker. A
//! thread never holds a latch across a park (§1): the wait steps take none.
//!
//! [`WaitHook`] is kept as a passive observation seam (existing tests and
//! C-SIM record waits through it); it inserts no edges — the graph does,
//! and only the graph does.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::boot::Core;
use crate::status::Remembered;
use crate::txn::{CancelFlag, Txn};
use crate::{Ts, TxnId, TxnStatus};

/// The default `deadlock_timeout` (§6): PostgreSQL's 1 s.
pub const DEFAULT_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(1);

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

/// A passive observation seam around every **blocking** wait (§6):
/// `on_wait_start` fires per target before the wait registers,
/// `on_wait_end` per target after it unregisters, on every return path.
/// It observes only — the wait-for graph inserts and removes the edges.
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

/// Why a wait returned.
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
    /// The deadlock check found a cycle through the waiter (§6): the
    /// waiter's edges are already removed; the caller raises 40P01.
    Deadlock,
    /// The `lock_timeout` deadline passed (§6): the caller raises 55P03.
    LockTimeout,
}

/// One registered waiter slot: the registration it belongs to and the
/// parker it parks on.
struct WaitSlot {
    reg: u64,
    waiter: TxnId,
    parker: Arc<dyn Parker>,
}

/// One registered wait: the waiter and the targets it still has an edge
/// to (a wake of a target removes that target).
struct Reg {
    waiter: TxnId,
    targets: BTreeSet<TxnId>,
}

/// The graph state under the one graph mutex: every registered wait (its
/// edges `waiter -> target`, keyed by registration id) and every target's
/// waiter slots. `BTree` containers keep iteration deterministic (C-SIM
/// replays seeds).
#[derive(Default)]
struct Graph {
    next_reg: u64,
    regs: BTreeMap<u64, Reg>,
    slots: BTreeMap<TxnId, Vec<Arc<WaitSlot>>>,
}

impl Graph {
    /// Removes registration `reg`'s edges and its slots on every target
    /// (wait end, `Drop`, a positive deadlock check). Idempotent, and
    /// never touches another registration of the same waiter.
    fn remove_reg(&mut self, reg: u64) {
        self.regs.remove(&reg);
        for list in self.slots.values_mut() {
            list.retain(|s| s.reg != reg);
        }
        self.slots.retain(|_, l| !l.is_empty());
    }

    /// The current edges `waiter -> targets`, merged over registrations.
    fn edges(&self) -> BTreeMap<TxnId, BTreeSet<TxnId>> {
        let mut out: BTreeMap<TxnId, BTreeSet<TxnId>> = BTreeMap::new();
        for r in self.regs.values() {
            if !r.targets.is_empty() {
                out.entry(r.waiter)
                    .or_default()
                    .extend(r.targets.iter().copied());
            }
        }
        out
    }
}

struct WaitsInner {
    graph: Mutex<Graph>,
    parkers: Mutex<Arc<dyn MakeParker>>,
    hook: Mutex<Arc<dyn WaitHook>>,
    /// `deadlock_timeout` in milliseconds (§6).
    deadlock_timeout_ms: AtomicU64,
}

impl WaitsInner {
    fn lock_graph(&self) -> MutexGuard<'_, Graph> {
        self.graph.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The wait-for graph and its parker maker. Guarded state lives behind one
/// `Arc` so a [`WaitHandle`] can remove its own edges on `Drop` from
/// anywhere. The graph mutex is the §3.1 wait-for-graph mutex: a leaf plus
/// status lookups — never taken while holding a latch or the registry
/// mutex, never held across a park.
pub struct Waits {
    inner: Arc<WaitsInner>,
}

impl Waits {
    pub(crate) fn new() -> Waits {
        Waits {
            inner: Arc::new(WaitsInner {
                graph: Mutex::new(Graph::default()),
                parkers: Mutex::new(Arc::new(CondvarParkers)),
                hook: Mutex::new(Arc::new(NoWaitHook)),
                deadlock_timeout_ms: AtomicU64::new(DEFAULT_DEADLOCK_TIMEOUT.as_millis() as u64),
            }),
        }
    }

    fn lock_graph(&self) -> MutexGuard<'_, Graph> {
        self.inner.lock_graph()
    }

    /// Replaces the parker maker (the simulator's seam).
    pub fn set_parker_maker(&self, maker: Arc<dyn MakeParker>) {
        *self
            .inner
            .parkers
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = maker;
    }

    /// Installs the wait observation hook.
    pub fn set_hook(&self, hook: Arc<dyn WaitHook>) {
        *self
            .inner
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = hook;
    }

    fn hook(&self) -> Arc<dyn WaitHook> {
        Arc::clone(
            &self
                .inner
                .hook
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    /// The `deadlock_timeout` setting (§6; PostgreSQL's default is 1 s).
    pub fn set_deadlock_timeout(&self, d: Duration) {
        let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        self.inner.deadlock_timeout_ms.store(ms, Ordering::SeqCst);
    }

    fn deadlock_timeout(&self) -> Duration {
        Duration::from_millis(self.inner.deadlock_timeout_ms.load(Ordering::SeqCst))
    }

    fn make_parker(&self) -> Arc<dyn Parker> {
        self.inner
            .parkers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .make()
    }
}

/// A registered wait: what [`Core::wait_end`] and the park loop hold, and
/// what the wakers of §6 unpark (through the slot registered at begin).
/// `Drop` removes the waiter's remaining edges and slots, so no path leaks
/// an edge.
pub struct WaitHandle {
    inner: Arc<WaitInner>,
}

struct WaitInner {
    waits: Arc<WaitsInner>,
    /// This registration's id in the graph: end, `Drop` and the deadlock
    /// check remove by it.
    reg: u64,
    /// Set by [`Core::wait_end`]; `Drop` is then a no-op.
    ended: AtomicBool,
    waiter: TxnId,
    targets: Vec<(TxnId, u64)>,
    parker: Arc<dyn Parker>,
    cancel: Arc<CancelFlag>,
}

impl WaitHandle {
    /// The waiting txn.
    pub fn waiter(&self) -> TxnId {
        self.inner.waiter
    }

    /// The targets this wait registered on, with the generations it read
    /// where it observed the conflict (§6).
    pub fn targets(&self) -> &[(TxnId, u64)] {
        &self.inner.targets
    }

    /// Parks on this wait's parker for at most `timeout`: the step-driven
    /// stand-in for the blocking wait's park. Returns `true` when unparked
    /// (a wake of §6), `false` on timeout.
    pub fn park(&self, timeout: Duration) -> bool {
        self.inner.parker.park(timeout)
    }
}

impl Drop for WaitHandle {
    fn drop(&mut self) {
        if !self.inner.ended.swap(true, Ordering::SeqCst) {
            self.inner.waits.lock_graph().remove_reg(self.inner.reg);
        }
    }
}

impl std::fmt::Debug for WaitHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitHandle")
            .field("waiter", &self.inner.waiter)
            .field("targets", &self.inner.targets)
            .finish_non_exhaustive()
    }
}

/// [`Core::wait_begin`]'s result: the wait was already satisfied (no edge
/// inserted), or the waiter is registered and parked-able.
#[derive(Debug)]
pub enum WaitBegin {
    /// The step-2 re-check unblocked the wait (§6 step 2): nothing was
    /// inserted.
    Done(WaitOutcome),
    /// The edges are in the graph and the slot registered on every target.
    Registered(WaitHandle),
}

/// §6 step 2's re-check, shared by begin (under the graph mutex) and poll:
/// per target, in order — missing status first (`gen` lives in the status
/// entry), then Aborted, then visible commit, then the generation — and
/// only then the waiter's cancel flag. The first target that unblocked
/// decides the outcome. Callable while holding the graph mutex: the status
/// table and the cancel flag are leaves.
fn recheck<K: nucleus_kv::OrderedKv>(
    core: &Core<K>,
    targets: &[(TxnId, u64)],
    cancel: &CancelFlag,
) -> Option<WaitOutcome> {
    for (target, g) in targets {
        match core.status.lookup_remembered(*target) {
            // §4/§7.4: a missing status for a remembered id means ended
            // and released.
            Remembered::Ended => return Some(WaitOutcome::Ended),
            Remembered::Live(TxnStatus::Aborted, _) => return Some(WaitOutcome::Aborted),
            Remembered::Live(TxnStatus::Committed(c), _) if c <= core.visible_ts() => {
                return Some(WaitOutcome::Committed(c));
            }
            // Committed but not yet visible, or Pending: keep waiting;
            // commit step 5 (after visibility) bumps the generation.
            Remembered::Live(_, gen) if gen != *g => return Some(WaitOutcome::GenChanged),
            Remembered::Live(..) => {}
        }
    }
    if cancel.is_cancelled() {
        return Some(WaitOutcome::Cancelled);
    }
    None
}

/// Whether a DFS from `w` over the current edges returns to `w` (§6
/// "Deadlock"): a cycle containing the waiter. Pure; runs under the graph
/// mutex.
fn cycle_through(edges: &BTreeMap<TxnId, BTreeSet<TxnId>>, w: TxnId) -> bool {
    let Some(succ) = edges.get(&w) else {
        return false;
    };
    let mut stack: Vec<TxnId> = succ.iter().rev().cloned().collect();
    let mut seen: BTreeSet<TxnId> = BTreeSet::new();
    while let Some(t) = stack.pop() {
        if t == w {
            return true;
        }
        if !seen.insert(t) {
            continue;
        }
        if let Some(next) = edges.get(&t) {
            stack.extend(next.iter().rev());
        }
    }
    false
}

/// The sorted `(waiter, target)` pairs of an edge map.
fn flatten_edges(edges: &BTreeMap<TxnId, BTreeSet<TxnId>>) -> Vec<(TxnId, TxnId)> {
    edges
        .iter()
        .flat_map(|(w, ts)| ts.iter().map(move |t| (*w, *t)))
        .collect()
}

impl<K: nucleus_kv::OrderedKv> Core<K> {
    /// §6 "Waiting" steps 1–3 on one target:
    /// [`Core::wait_on_any`] with a one-element target list.
    pub fn wait_on(&self, waiter: &Txn, target: TxnId, g: u64) -> WaitOutcome {
        self.wait_on_any(waiter, &[(target, g)])
    }

    /// §6 "Waiting" steps 1–3 over several targets (C-T0 §5.1: a row op may
    /// have to wait on a foreign intent's owner and on every conflicting
    /// shared-row-lock holder at once), with no `lock_timeout` deadline.
    pub fn wait_on_any(&self, waiter: &Txn, targets: &[(TxnId, u64)]) -> WaitOutcome {
        self.wait_on_any_deadline(waiter, targets, None)
    }

    /// The blocking wait (§6): begin; loop { park until woken, the cancel
    /// flag, `deadlock_timeout` (the first park is bounded by it, so the
    /// check cannot be delayed past it) or the deadline; poll; once
    /// `deadlock_timeout` has elapsed since begin, run the deadlock check
    /// **once** for this wait; past the deadline return `LockTimeout` };
    /// end. The parker is published to the cancel flag for the whole loop
    /// (seed 48: setting the flag also wakes a parked session).
    pub fn wait_on_any_deadline(
        &self,
        waiter: &Txn,
        targets: &[(TxnId, u64)],
        deadline: Option<Instant>,
    ) -> WaitOutcome {
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
        for (t, _) in targets {
            hook.on_wait_start(waiter.id, *t);
        }
        let outcome = self.wait_blocking(waiter, targets, deadline);
        for (t, _) in targets {
            hook.on_wait_end(waiter.id, *t);
        }
        outcome
    }

    /// The park loop over the wait steps.
    fn wait_blocking(
        &self,
        waiter: &Txn,
        targets: &[(TxnId, u64)],
        deadline: Option<Instant>,
    ) -> WaitOutcome {
        let parker = self.waits.make_parker();
        let handle = match self.wait_begin_with(waiter, targets, parker) {
            WaitBegin::Done(o) => return o,
            WaitBegin::Registered(h) => h,
        };
        // Publish the parker so `cancel()` can unpark this park (§6). The
        // store happens before the loop's first flag re-check, which closes
        // the set-then-store race: a cancel that ran before the store is
        // either already visible to the flag check below, or its unpark
        // lands on the stored parker.
        waiter.cancel.set_parked(handle.inner.parker.clone());
        let began = Instant::now();
        let dl_at = began
            .checked_add(self.waits.deadlock_timeout())
            .unwrap_or(began);
        let mut checked = false;
        let outcome = loop {
            if let Some(o) = self.wait_poll(&handle) {
                break o;
            }
            if waiter.cancel.is_cancelled() {
                break WaitOutcome::Cancelled;
            }
            // Park, bounded by the lost-wakeup slice, the deadline and
            // (until the check has run) the deadlock timeout, so the check
            // runs on time even if an earlier park was cut short; a zero
            // bound skips the park.
            let now = Instant::now();
            let mut bound = PARK_SLICE;
            if !checked {
                bound = bound.min(dl_at.saturating_duration_since(now));
            }
            match deadline {
                Some(d) if now < d => bound = bound.min(d - now),
                Some(_) => bound = Duration::ZERO,
                None => {}
            }
            if bound > Duration::ZERO {
                handle.park(bound);
            }
            if let Some(o) = self.wait_poll(&handle) {
                break o;
            }
            if waiter.cancel.is_cancelled() {
                break WaitOutcome::Cancelled;
            }
            // §6 "Deadlock": after `deadlock_timeout`, once per wait.
            if !checked && Instant::now() >= dl_at {
                checked = true;
                if self.wait_deadlock_check(&handle) {
                    break WaitOutcome::Deadlock;
                }
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                break WaitOutcome::LockTimeout;
            }
        };
        waiter.cancel.clear_parked();
        self.wait_end(handle);
        outcome
    }

    /// §6 "Waiting" steps 1–2 as one step: re-check every target, and only
    /// if none unblocked, insert an edge to every target and register the
    /// waiter's slot on each — both in one graph critical section (I-LIVE
    /// c: no edge for an already-satisfied wait). Returns
    /// [`WaitBegin::Done`] without inserting anything when the re-check
    /// unblocks.
    pub fn wait_begin(&self, waiter: &Txn, targets: &[(TxnId, u64)]) -> WaitBegin {
        let parker = self.waits.make_parker();
        self.wait_begin_with(waiter, targets, parker)
    }

    fn wait_begin_with(
        &self,
        waiter: &Txn,
        targets: &[(TxnId, u64)],
        parker: Arc<dyn Parker>,
    ) -> WaitBegin {
        let mut g = self.waits.lock_graph();
        if let Some(o) = recheck(self, targets, &waiter.cancel) {
            return WaitBegin::Done(o);
        }
        let reg = g.next_reg;
        g.next_reg = g.next_reg.wrapping_add(1);
        let slot = Arc::new(WaitSlot {
            reg,
            waiter: waiter.id,
            parker: Arc::clone(&parker),
        });
        g.regs.insert(
            reg,
            Reg {
                waiter: waiter.id,
                targets: targets.iter().map(|(t, _)| *t).collect(),
            },
        );
        for (t, _) in targets {
            g.slots.entry(*t).or_default().push(Arc::clone(&slot));
        }
        WaitBegin::Registered(WaitHandle {
            inner: Arc::new(WaitInner {
                waits: Arc::clone(&self.waits.inner),
                reg,
                ended: AtomicBool::new(false),
                waiter: waiter.id,
                targets: targets.to_vec(),
                parker,
                cancel: Arc::clone(&waiter.cancel),
            }),
        })
    }

    /// Poll: re-runs the §6 step-2 checks (no mutex beyond the status
    /// table) and returns `Some(outcome)` when the wait is over.
    pub fn wait_poll(&self, h: &WaitHandle) -> Option<WaitOutcome> {
        recheck(self, &h.inner.targets, &h.inner.cancel)
    }

    /// The §6 deadlock check: under the graph mutex, a DFS from the waiter
    /// over the **current** edges (never anything else); if a cycle
    /// contains the waiter, this wait's edges and slots are removed
    /// **before the mutex is released** and the answer is `true` — the
    /// caller raises 40P01 and aborts the waiter, so exactly one member of
    /// the cycle aborts (I-LIVE b, c). `false` otherwise.
    pub fn wait_deadlock_check(&self, h: &WaitHandle) -> bool {
        self.deadlock_check(h).is_some()
    }

    /// [`Core::wait_deadlock_check`] that also returns, when the waiter is
    /// the victim, the sorted edges the DFS ran over (before the victim's
    /// own were removed) — diagnostics, so a test can re-derive the cycle
    /// from the exact graph the check saw. `None` when no cycle contains
    /// the waiter.
    pub fn wait_deadlock_check_traced(&self, h: &WaitHandle) -> Option<Vec<(TxnId, TxnId)>> {
        self.deadlock_check(h).map(|edges| flatten_edges(&edges))
    }

    fn deadlock_check(&self, h: &WaitHandle) -> Option<BTreeMap<TxnId, BTreeSet<TxnId>>> {
        let mut g = self.waits.lock_graph();
        let edges = g.edges();
        if cycle_through(&edges, h.inner.waiter) {
            g.remove_reg(h.inner.reg);
            Some(edges)
        } else {
            None
        }
    }

    /// Ends the wait: removes this wait's remaining edges and slots (and
    /// only this wait's: another wait of the same txn keeps its own).
    /// Dropping the handle is also a valid end; after `wait_end` the
    /// handle's `Drop` does nothing.
    pub fn wait_end(&self, h: WaitHandle) {
        if !h.inner.ended.swap(true, Ordering::SeqCst) {
            self.waits.lock_graph().remove_reg(h.inner.reg);
        }
    }

    /// A sorted snapshot of the current wait-for edges — diagnostics (the
    /// `pg_locks` analogue of §6). Tests observe the graph through it;
    /// nothing in the crate reads it to make a decision.
    pub fn wait_edges(&self) -> Vec<(TxnId, TxnId)> {
        flatten_edges(&self.waits.lock_graph().edges())
    }

    /// A sorted snapshot of the registered waiter slots as
    /// `(target, waiter)` pairs — diagnostics like [`Core::wait_edges`]
    /// (a slot is what a wake of `target` unparks). Nothing in the crate
    /// reads it to make a decision.
    pub fn wait_slots(&self) -> Vec<(TxnId, TxnId)> {
        let g = self.waits.lock_graph();
        let mut out: Vec<(TxnId, TxnId)> = g
            .slots
            .iter()
            .flat_map(|(t, l)| l.iter().map(move |s| (*t, s.waiter)))
            .collect();
        out.sort_unstable();
        out
    }

    /// Bumps `txn`'s wake generation and wakes its waiters (§6 "Waking"):
    /// the one wake path — commit step 5, abort, `ROLLBACK TO` (§5.5) and
    /// every in-memory lock release. Under the graph mutex: bump the
    /// generation (the status table mutex is a leaf, §3.1), remove every
    /// edge `* -> txn`, take `txn`'s slots; release the mutex; then
    /// unpark. A waiter registering with the new generation can only do
    /// so after this critical section, so its edge and slot survive.
    /// Spurious unparks are harmless — waiters re-check the generation.
    pub fn bump_and_wake(&self, txn: TxnId) -> Result<u64, crate::TxnError> {
        let (gen, slots) = {
            let mut g = self.waits.lock_graph();
            let gen = self.status.bump_gen(txn)?;
            for r in g.regs.values_mut() {
                r.targets.remove(&txn);
            }
            (gen, g.slots.remove(&txn))
        };
        #[cfg(test)]
        tests::after_bump(txn);
        if let Some(slots) = slots {
            for s in slots {
                s.parker.unpark();
            }
        }
        Ok(gen)
    }
}

#[cfg(test)]
mod tests {
    //! The bump/wake window (§6 "Waking"): a seam between the graph
    //! critical section of [`Core::bump_and_wake`] and its unparks, where a
    //! test registers a wait with the target's **new** generation — the
    //! interleaving that, with bump and edge removal in separate critical
    //! sections, lost the edge and the slot.

    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    use nucleus_kv::MemKv;

    use super::{WaitBegin, WaitHandle, WaitOutcome};
    use crate::boot::Core;
    use crate::status::Remembered;
    use crate::txn::Isolation;
    use crate::TxnId;

    type AfterBump = Box<dyn FnOnce(TxnId)>;

    thread_local! {
        static AFTER_BUMP: RefCell<Option<AfterBump>> = const { RefCell::new(None) };
    }

    /// Runs (once) the hook the current thread installed, if any.
    pub(super) fn after_bump(txn: TxnId) {
        if let Some(f) = AFTER_BUMP.with(|h| h.borrow_mut().take()) {
            f(txn);
        }
    }

    fn gen_of(core: &Core<MemKv>, id: TxnId) -> u64 {
        match core.status.lookup_remembered(id) {
            Remembered::Live(_, g) => g,
            Remembered::Ended => panic!("{id:?} has no status entry"),
        }
    }

    /// W registers its wait on T with T's new generation inside the
    /// bump/wake window: W -> T stays in the graph, W's slot on T stays
    /// registered (a later wake of T unparks W), and a T <-> W cycle is
    /// detected.
    /// Mutant: bump outside the graph mutex, the edge removal in a second
    /// critical section after the window (the reviewer's race: W -> T and
    /// the slot are lost; the deadlock goes undetected).
    #[test]
    fn register_in_bump_wake_window_keeps_edge_and_slot() {
        let core = Arc::new(match Core::open(MemKv::new()) {
            Ok(c) => c,
            Err(e) => panic!("core: {e:?}"),
        });
        let t = core.begin(Isolation::ReadCommitted);
        let w = Arc::new(core.begin(Isolation::ReadCommitted));
        let stash: Rc<RefCell<Option<WaitHandle>>> = Rc::new(RefCell::new(None));
        {
            let (core, w, stash) = (Arc::clone(&core), Arc::clone(&w), Rc::clone(&stash));
            AFTER_BUMP.with(|h| {
                *h.borrow_mut() = Some(Box::new(move |target: TxnId| {
                    let g = gen_of(&core, target);
                    match core.wait_begin(&w, &[(target, g)]) {
                        WaitBegin::Registered(h) => *stash.borrow_mut() = Some(h),
                        other => panic!("W must register with the new gen, got {other:?}"),
                    }
                }));
            });
        }
        let g1 = match core.bump_and_wake(t.id) {
            Ok(g) => g,
            Err(e) => panic!("bump: {e:?}"),
        };
        assert_eq!(g1, 1);
        let wh = match stash.borrow_mut().take() {
            Some(h) => h,
            None => panic!("the hook did not run"),
        };
        assert!(
            core.wait_edges().contains(&(w.id, t.id)),
            "W -> T survived the wake that preceded its registration"
        );
        assert!(
            core.wait_slots().contains(&(t.id, w.id)),
            "W's slot on T survived"
        );
        assert_eq!(core.wait_poll(&wh), None, "W's wait is not over");

        // T waits on W: a real two-cycle, detected.
        let th = match core.wait_begin(&t, &[(w.id, gen_of(&core, w.id))]) {
            WaitBegin::Registered(h) => h,
            other => panic!("T must register, got {other:?}"),
        };
        assert!(
            core.wait_deadlock_check(&th),
            "the T <-> W cycle is detected"
        );
        core.wait_end(th);

        // No lost wakeup: the next wake of T unparks W.
        if let Err(e) = core.bump_and_wake(t.id) {
            panic!("bump: {e:?}");
        }
        assert!(wh.park(Duration::ZERO), "W's parker was unparked");
        assert_eq!(core.wait_poll(&wh), Some(WaitOutcome::GenChanged));
        core.wait_end(wh);
        assert!(core.wait_edges().is_empty());
        assert!(core.wait_slots().is_empty());
    }
}
