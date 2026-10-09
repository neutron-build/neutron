//! C-T0 §6, the in-memory lock side: the shared row-lock table, relation
//! locks and transaction-scoped advisory locks, and the [`ReleaseHook`]
//! that drops the relation and advisory locks of a txn at commit step 5
//! (§3), abort (§7.1) and `ROLLBACK TO` (§5.5).
//!
//! - The **shared row-lock table** ([`RowLockTable`]) implements C-T2's
//!   [`RowLocks`] seam. Shared modes (KEY SHARE, SHARE) never appear in
//!   intents (§2.1); they live here, keyed by logical key, one acquisition
//!   per `(txn, mode, seq)`, checked under the same latch as `k@INTENT`
//!   (§5.0). Safe in memory because a crash aborts every holder.
//! - **Relation locks** (§6: AccessShare … AccessExclusive) are held to
//!   txn end; `ROLLBACK TO` releases those taken after the savepoint.
//!   Conflicts follow PostgreSQL's table-level matrix
//!   ([`REL_LOCK_CONFLICTS`]). Lock queues are not fair: a released lock
//!   wakes every waiter and each re-tries.
//! - **Advisory locks** are xact scope only (§6; session scope is §12 Q5).
//!   Exclusive conflicts with any other txn's lock on the key; shared
//!   conflicts only with exclusive. Re-entrant per txn: a txn holds a
//!   key/mode once until txn end or `ROLLBACK TO`, however often it takes
//!   it (there is no unlock in xact scope, so no counter).
//! - Conflict checks ignore ended holders (§6): a holder that is Aborted
//!   or a visible commit never conflicts, even before its release ran; a
//!   missing status means ended (§4). A committed-but-not-visible holder
//!   counts like Pending (§3.2).
//!
//! Lock discipline (§3.1): the row-lock, relation-lock and advisory-lock
//! table mutexes are leaves — each may be taken under any lock above,
//! never held while taking another lock, except that status lookups (the
//! status table is itself a leaf) are allowed inside them. Waits happen
//! outside every table mutex, through the wait-for graph
//! ([`Core::wait_on_any_deadline`](crate::wait::Core::wait_on_any_deadline)),
//! so relation and advisory waits join the same graph as row waits and one
//! deadlock detector covers every kind (§6). Nothing here bumps a wake
//! generation or wakes on its own: commit step 5, abort and
//! [`Core::rollback_to`](crate::boot::Core::rollback_to) bump and wake
//! after the release ran (§6 "Waking"; bump first, then wake).
//!
//! [`LockManager::install`] installs the row-lock table and the release
//! hook on a [`Core`](crate::boot::Core) in one call — the only setup step
//! tests and the simulator (C-SIM) need.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use nucleus_kv::{Key, OrderedKv};

use crate::boot::Core;
use crate::commit::ReleaseHook;
use crate::status::Remembered;
use crate::txn::Txn;
use crate::wait::WaitOutcome;
use crate::write::{LockWait, RowLocks};
use crate::{RowLockMode, Seq, TxnError, TxnId, TxnStatus};

// ---------------------------------------------------------------------------
// The shared row-lock table (§6)
// ---------------------------------------------------------------------------

/// One acquisition of a shared row lock: the txn, the mode, the acquiring
/// seq (§5.5: `ROLLBACK TO s` releases those taken at `seq >= s`), and the
/// §5.0 latch prefix the grant ran under (so every release path latches
/// `latch_key(key, prefix)` — the same latch, seed 46).
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowAcquisition {
    txn: TxnId,
    mode: RowLockMode,
    seq: Seq,
    latch_prefix: Option<usize>,
}

/// The table's guarded state. `BTree` containers keep every iteration
/// order deterministic (C-SIM replays seeds).
#[derive(Default)]
struct RowTableInner {
    /// Per logical key, every acquisition, in grant order.
    by_key: BTreeMap<Key, Vec<RowAcquisition>>,
    /// Per txn, the keys it holds (any acquisition) — the index
    /// `keys_of` walks.
    by_txn: BTreeMap<TxnId, BTreeSet<Key>>,
}

/// The shared row-lock table (§6): one mutex (a leaf: it never takes
/// another lock while held; status lookups are the caller's business,
/// outside the latch protocol this table lives under). `holders`,
/// `grant` and `release` are called only under `latch(latch_key(key))`
/// (§5.0) — the write path's §5.1 loop on the grant side, commit step 5,
/// abort and `ROLLBACK TO` on the release side.
pub struct RowLockTable {
    inner: Mutex<RowTableInner>,
}

impl RowLockTable {
    pub fn new() -> RowLockTable {
        RowLockTable {
            inner: Mutex::new(RowTableInner::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, RowTableInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for RowLockTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RowLocks for RowLockTable {
    fn holders(&self, key: &[u8]) -> Vec<(TxnId, RowLockMode, Seq)> {
        let t = self.lock();
        t.by_key
            .get(key)
            .map(|acs| acs.iter().map(|a| (a.txn, a.mode, a.seq)).collect())
            .unwrap_or_default()
    }

    fn grant(
        &self,
        key: &[u8],
        txn: TxnId,
        mode: RowLockMode,
        seq: Seq,
        latch_prefix: Option<usize>,
    ) -> Result<(), TxnError> {
        if !matches!(mode, RowLockMode::KeyShare | RowLockMode::Share) {
            return Err(TxnError::Invariant(format!(
                "the shared row-lock table grants KEY SHARE and SHARE only, \
                 got {mode:?} (§2.1: exclusive modes live in intents)"
            )));
        }
        let mut t = self.lock();
        // One acquisition per (txn, mode, seq): a re-grant of an identical
        // acquisition (a §5.1 retry that re-runs the grant) is a no-op, so
        // `release` drops exactly the acquisitions `keys_of` reported.
        let acs = t.by_key.entry(key.to_vec()).or_default();
        if acs
            .iter()
            .any(|a| a.txn == txn && a.mode == mode && a.seq == seq)
        {
            return Ok(());
        }
        acs.push(RowAcquisition {
            txn,
            mode,
            seq,
            latch_prefix,
        });
        t.by_txn.entry(txn).or_default().insert(key.to_vec());
        Ok(())
    }

    fn keys_of(&self, txn: TxnId, from_seq: Seq) -> Vec<(Key, Option<usize>)> {
        let t = self.lock();
        let Some(keys) = t.by_txn.get(&txn) else {
            return Vec::new();
        };
        keys.iter()
            .filter_map(|key| {
                t.by_key.get(key).and_then(|acs| {
                    acs.iter()
                        .find(|a| a.txn == txn && a.seq >= from_seq)
                        .map(|a| (key.clone(), a.latch_prefix))
                })
            })
            .collect()
    }

    fn release(&self, key: &[u8], txn: TxnId, from_seq: Seq) {
        let mut t = self.lock();
        let mut key_empty = false;
        if let Some(acs) = t.by_key.get_mut(key) {
            acs.retain(|a| !(a.txn == txn && a.seq >= from_seq));
            key_empty = acs.is_empty();
            let still_holds = acs.iter().any(|a| a.txn == txn);
            if !still_holds {
                if let Some(keys) = t.by_txn.get_mut(&txn) {
                    keys.remove(key);
                }
            }
        }
        if key_empty {
            t.by_key.remove(key);
        }
        if t.by_txn.get(&txn).is_some_and(|k| k.is_empty()) {
            t.by_txn.remove(&txn);
        }
    }
}

// ---------------------------------------------------------------------------
// Relation lock modes (§6)
// ---------------------------------------------------------------------------

/// PostgreSQL's eight table-level lock modes (§6), weakest to strongest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RelLockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
}

impl RelLockMode {
    /// The mode's index in [`REL_LOCK_CONFLICTS`].
    fn index(self) -> usize {
        self as usize
    }

    /// Whether a request of `self` conflicts with a held `other` lock
    /// (§6: PostgreSQL's table-level conflict matrix, symmetric).
    pub fn conflicts_with(self, other: RelLockMode) -> bool {
        REL_LOCK_CONFLICTS[self.index()][other.index()]
    }
}

/// PostgreSQL's table-level conflict matrix (§6): row = requested, column
/// = held, `X` = conflict. Encoded once as a const so the test suite can
/// compare it against its own literal.
///
/// ```text
///        requested \ held    AS RS RE SUE S SRE E AE
///        AccessShare         .  .  .  .   . .   .  X
///        RowShare            .  .  .  .   . .   X  X
///        RowExclusive        .  .  .  .   X X   X  X
///        ShareUpdateExcl.    .  .  .  X   X X   X  X
///        Share               .  .  X  X   . X   X  X
///        ShareRowExclusive   .  .  X  X   X X   X  X
///        Exclusive           .  X  X  X   X X   X  X
///        AccessExclusive     X  X  X  X   X X   X  X
/// ```
#[rustfmt::skip]
pub const REL_LOCK_CONFLICTS: [[bool; 8]; 8] = [
    // held: AS  RS  RE  SUE  S  SRE  E   AE
    /* AS  requested */ [false, false, false, false, false, false, false, true ],
    /* RS  requested */ [false, false, false, false, false, false, true , true ],
    /* RE  requested */ [false, false, false, false, true , true , true , true ],
    /* SUE requested */ [false, false, false, true , true , true , true , true ],
    /* S   requested */ [false, false, true , true , false, true , true , true ],
    /* SRE requested */ [false, false, true , true , true , true , true , true ],
    /* E   requested */ [false, true , true , true , true , true , true , true ],
    /* AE  requested */ [true , true , true , true , true , true , true , true ],
];

// ---------------------------------------------------------------------------
// The lock manager (§6: relation and advisory locks, release)
// ---------------------------------------------------------------------------

/// One relation-lock acquisition (§6): the holder, the mode, and the txn's
/// seq when taken (`ROLLBACK TO` releases those with `seq >= s`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelAcquisition {
    txn: TxnId,
    mode: RelLockMode,
    seq: Seq,
}

/// One advisory-lock entry per `(key, txn, shared)`: the txn's seq when
/// first taken (§6). Re-taking it adds nothing: xact-scope locks have no
/// unlock, so they are held once until txn end or `ROLLBACK TO`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdvEntry {
    txn: TxnId,
    shared: bool,
    seq: Seq,
}

/// The relation and advisory lock tables plus the shared row-lock table,
/// installed on a [`Core`] in one call through [`LockManager::install`].
///
/// The two table mutexes are leaves (§3.1): status lookups are allowed
/// inside them, no other lock is taken while one is held, and no wait ever
/// happens under one. All waits go through the wait-for graph, so one
/// deadlock detector covers row, relation and advisory waits together.
pub struct LockManager {
    rows: Arc<RowLockTable>,
    relations: Mutex<BTreeMap<u64, Vec<RelAcquisition>>>,
    advisory: Mutex<BTreeMap<i64, Vec<AdvEntry>>>,
}

impl LockManager {
    /// An empty manager (tests and C-SIM install it through
    /// [`LockManager::install`], which also wires the core's
    /// [`RowLocks`] slot and release hook).
    pub fn new() -> LockManager {
        LockManager {
            rows: Arc::new(RowLockTable::new()),
            relations: Mutex::new(BTreeMap::new()),
            advisory: Mutex::new(BTreeMap::new()),
        }
    }

    /// Installs the shared row-lock table and the release hook on `core`
    /// (§6): the one-call setup tests and C-SIM use. Returns the manager
    /// its relation and advisory APIs live on.
    pub fn install<K: OrderedKv>(core: &Core<K>) -> Arc<LockManager> {
        let mgr = Arc::new(LockManager::new());
        core.set_row_locks(Arc::clone(&mgr.rows) as Arc<dyn RowLocks>);
        core.set_release_hook(Arc::clone(&mgr) as Arc<dyn ReleaseHook>);
        mgr
    }

    fn lock_relations(&self) -> MutexGuard<'_, BTreeMap<u64, Vec<RelAcquisition>>> {
        self.relations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The shared row-lock table this manager installs (§6): the typed
    /// handle tests and diagnostics read `holders` through. The same
    /// object [`LockManager::install`] put in the core's [`RowLocks`]
    /// slot.
    pub fn row_table(&self) -> Arc<RowLockTable> {
        Arc::clone(&self.rows)
    }

    fn lock_advisory(&self) -> MutexGuard<'_, BTreeMap<i64, Vec<AdvEntry>>> {
        self.advisory.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// §6 `LOCK TABLE` on `rel_oid` in `mode`. `Ok(true)`: acquired.
    /// `Ok(false)`: skipped ([`LockWait::SkipLocked`]). An error: 55P03
    /// for [`LockWait::NoWait`] and an expired `lock_timeout`, 57014 when
    /// cancelled, 40P01 when the wait's deadlock check fired.
    ///
    /// Under the table mutex: the holders of `rel_oid` other than `txn`
    /// that are not ended (Aborted or a visible commit never conflict,
    /// even before their release ran; a missing status is ended) and that
    /// conflict with `mode`; their generations are read in the same
    /// section. With none, the lock is recorded (with `txn.seq()`, so
    /// `ROLLBACK TO` can drop it) and `Ok(true)` returns. Otherwise the
    /// mutex is released before the [`LockWait`] policy runs;
    /// [`LockWait::Block`] parks on every conflicting holder through the
    /// wait-for graph (`lock_timeout`, when set, bounds the wait) and
    /// retries — the queue is not fair, every woken waiter re-tries (§6).
    /// A txn's own locks never conflict with its own requests, and
    /// re-acquiring a held mode is a no-op that keeps the older seq (so a
    /// later rollback does not drop it).
    pub fn lock_relation<K: OrderedKv>(
        &self,
        core: &Core<K>,
        txn: &Txn,
        rel_oid: u64,
        mode: RelLockMode,
        wait: LockWait,
        lock_timeout: Option<Duration>,
    ) -> Result<bool, TxnError> {
        let deadline = lock_timeout.map(|t| Instant::now() + t);
        loop {
            // One leaf critical section: the conflict scan, the
            // generations and (when free) the acquisition itself.
            let blockers = {
                let mut rel = self.lock_relations();
                if rel
                    .get(&rel_oid)
                    .is_some_and(|acs| acs.iter().any(|a| a.txn == txn.id && a.mode == mode))
                {
                    // Re-acquiring a held mode: keep the older seq.
                    return Ok(true);
                }
                let mut blockers: Vec<(TxnId, u64)> = Vec::new();
                if let Some(acs) = rel.get(&rel_oid) {
                    for a in acs {
                        if a.txn == txn.id || !a.mode.conflicts_with(mode) {
                            continue;
                        }
                        if let Some(gen) = live_holder(core, a.txn) {
                            blockers.push((a.txn, gen));
                        }
                    }
                }
                if blockers.is_empty() {
                    rel.entry(rel_oid).or_default().push(RelAcquisition {
                        txn: txn.id,
                        mode,
                        seq: txn.seq(),
                    });
                    return Ok(true);
                }
                blockers
            };
            match wait {
                LockWait::NoWait => return Err(TxnError::LockNotAvailable),
                LockWait::SkipLocked => return Ok(false),
                LockWait::Block => {}
            }
            map_wait(core.wait_on_any_deadline(txn, &blockers, deadline))?;
        }
    }

    /// §6 `pg_advisory_xact_lock` / `pg_try_advisory_xact_lock`
    /// (transaction scope only; session scope is §12 Q5). Exclusive
    /// (`shared = false`) conflicts with any other txn's lock on `key`;
    /// shared conflicts only with another txn's exclusive. Re-entrant per
    /// txn: re-taking a held key/mode is a no-op that keeps the older seq. `Ok(true)`: acquired. `Ok(false)`:
    /// [`LockWait::NoWait`] or [`LockWait::SkipLocked`] found a conflict
    /// (the `pg_try_advisory_xact_lock` form — advisory NOWAIT skips
    /// instead of erroring). Block parks through the wait-for graph,
    /// bounded by `lock_timeout`, and retries.
    pub fn advisory_xact_lock<K: OrderedKv>(
        &self,
        core: &Core<K>,
        txn: &Txn,
        key: i64,
        shared: bool,
        wait: LockWait,
        lock_timeout: Option<Duration>,
    ) -> Result<bool, TxnError> {
        let deadline = lock_timeout.map(|t| Instant::now() + t);
        loop {
            let blockers = {
                let mut adv = self.lock_advisory();
                if adv
                    .get(&key)
                    .is_some_and(|es| es.iter().any(|e| e.txn == txn.id && e.shared == shared))
                {
                    // Re-entrant: already held in this mode; keep the
                    // older seq.
                    return Ok(true);
                }
                let mut blockers: Vec<(TxnId, u64)> = Vec::new();
                if let Some(entries) = adv.get(&key) {
                    for e in entries {
                        if e.txn == txn.id {
                            continue;
                        }
                        let conflicts = if shared {
                            !e.shared
                        } else {
                            // Exclusive conflicts with any other lock.
                            true
                        };
                        if conflicts {
                            if let Some(gen) = live_holder(core, e.txn) {
                                blockers.push((e.txn, gen));
                            }
                        }
                    }
                }
                if blockers.is_empty() {
                    adv.entry(key).or_default().push(AdvEntry {
                        txn: txn.id,
                        shared,
                        seq: txn.seq(),
                    });
                    return Ok(true);
                }
                blockers
            };
            match wait {
                // The pg_try form: advisory NoWait reports "not acquired",
                // never an error.
                LockWait::NoWait | LockWait::SkipLocked => return Ok(false),
                LockWait::Block => {}
            }
            map_wait(core.wait_on_any_deadline(txn, &blockers, deadline))?;
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ReleaseHook for LockManager {
    /// §3 step 5 / §7.1: remove the txn's relation and advisory locks
    /// under their table mutexes. The generation bump and the wake that
    /// follow are already done by the callers (commit step 5, abort) —
    /// this never bumps twice. Shared row locks are **not** touched here:
    /// [`Core`]'s `release_row_locks` releases them under each key's
    /// latch before this hook runs.
    fn release_all(&self, txn: TxnId) {
        {
            let mut rel = self.lock_relations();
            for acs in rel.values_mut() {
                acs.retain(|a| a.txn != txn);
            }
            rel.retain(|_, acs| !acs.is_empty());
        }
        {
            let mut adv = self.lock_advisory();
            for entries in adv.values_mut() {
                entries.retain(|e| e.txn != txn);
            }
            adv.retain(|_, entries| !entries.is_empty());
        }
    }

    /// §5.5 `ROLLBACK TO s`: drop this txn's relation and advisory
    /// acquisitions taken at `seq >= s` (§6). The wake is
    /// [`Core::rollback_to`](crate::boot::Core::rollback_to)'s own
    /// bump-and-wake, which runs right after this — the same way commit
    /// step 5 wakes after `release_all`.
    fn release_from(&self, txn: TxnId, from_seq: Seq) {
        {
            let mut rel = self.lock_relations();
            for acs in rel.values_mut() {
                acs.retain(|a| !(a.txn == txn && a.seq >= from_seq));
            }
            rel.retain(|_, acs| !acs.is_empty());
        }
        {
            let mut adv = self.lock_advisory();
            for entries in adv.values_mut() {
                entries.retain(|e| !(e.txn == txn && e.seq >= from_seq));
            }
            adv.retain(|_, entries| !entries.is_empty());
        }
    }
}

/// §6: whether a lock holder still counts as holding. Aborted and visible
/// commits never conflict (even before their release ran); a missing
/// status means ended (§4); Pending and committed-but-not-visible count
/// (§3.2). Returns the holder's wake generation for the wait (§6: read
/// under the lock-table mutex). Pure status-table lookups — allowed under
/// a table mutex (§3.1).
fn live_holder<K: OrderedKv>(core: &Core<K>, id: TxnId) -> Option<u64> {
    match core.status.lookup_remembered(id) {
        Remembered::Ended => None,
        Remembered::Live(TxnStatus::Aborted, _) => None,
        Remembered::Live(TxnStatus::Committed(c), _gen) if c <= core.visible_ts() => None,
        Remembered::Live(_, gen) => Some(gen),
    }
}

/// The §6 outcome mapping of the blocking relation and advisory drivers —
/// the same arms [`crate::write`]'s row drivers map through
/// `map_wait_outcome` (crate-private there): cancel 57014, deadlock
/// 40P01, `lock_timeout` 55P03; everything else retries.
fn map_wait(o: WaitOutcome) -> Result<(), TxnError> {
    match o {
        WaitOutcome::Cancelled => Err(TxnError::QueryCanceled),
        WaitOutcome::Deadlock => Err(TxnError::Deadlock),
        WaitOutcome::LockTimeout => Err(TxnError::LockNotAvailable),
        WaitOutcome::Ended | WaitOutcome::Aborted | WaitOutcome::Committed(_) => Ok(()),
        WaitOutcome::GenChanged => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    //! Table internals the public API cannot observe: the per-txn index
    //! of the row table is emptied, re-taking an advisory lock adds no
    //! entry, and a holder with no status entry (§4: ended and released)
    //! never conflicts.

    use std::sync::Arc;

    use nucleus_kv::MemKv;

    use super::{LockManager, RelAcquisition, RelLockMode, RowLockTable};
    use crate::boot::Core;
    use crate::txn::Isolation;
    use crate::write::{LockWait, RowLocks};
    use crate::{RowLockMode, TxnId};

    fn core() -> Arc<Core<MemKv>> {
        match Core::open(MemKv::new()) {
            Ok(c) => Arc::new(c),
            Err(e) => panic!("core: {e:?}"),
        }
    }

    fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    /// Releasing a txn's last acquisition removes its per-txn index entry.
    /// Mutant: the empty `by_txn` entry is never removed.
    #[test]
    fn row_table_release_empties_the_txn_index() {
        let t = RowLockTable::new();
        let x = TxnId { epoch: 1, n: 7 };
        ok(t.grant(b"k1", x, RowLockMode::KeyShare, 1, None));
        ok(t.grant(b"k2", x, RowLockMode::Share, 2, None));
        t.release(b"k1", x, 0);
        assert!(t.lock().by_txn.contains_key(&x), "x still holds k2");
        t.release(b"k2", x, 0);
        let inner = t.lock();
        assert!(inner.by_txn.is_empty(), "no empty per-txn entry is kept");
        assert!(inner.by_key.is_empty());
    }

    /// Re-taking a held advisory key/mode adds no entry (it keeps the
    /// older seq); the other mode is a separate entry.
    /// Mutant: the re-entrant early return removed (a duplicate entry).
    #[test]
    fn advisory_reacquire_adds_no_entry() {
        let core = core();
        let mgr = LockManager::install(&core);
        let t = core.begin(Isolation::ReadCommitted);
        for _ in 0..3 {
            assert!(ok(mgr.advisory_xact_lock(
                &core,
                &t,
                9,
                true,
                LockWait::NoWait,
                None
            )));
        }
        assert_eq!(mgr.lock_advisory().get(&9).map(Vec::len), Some(1));
        assert!(ok(mgr.advisory_xact_lock(
            &core,
            &t,
            9,
            false,
            LockWait::NoWait,
            None
        )));
        assert_eq!(mgr.lock_advisory().get(&9).map(Vec::len), Some(2));
        ok(core.abort(t));
        assert!(mgr.lock_advisory().is_empty());
    }

    /// A holder whose status entry is gone (§4: a missing status means
    /// ended and released) does not block a conflicting NOWAIT request,
    /// relation or advisory. The entries are planted directly: no public
    /// path leaves a lock behind a truncated status.
    /// Mutant: a missing status counted as a live holder.
    #[test]
    fn missing_status_holder_never_blocks() {
        let core = core();
        let mgr = LockManager::install(&core);
        let ghost = TxnId {
            epoch: 1,
            n: 1_000_000,
        };
        assert!(core.status.entry(ghost).is_none());
        mgr.lock_relations()
            .entry(4)
            .or_default()
            .push(RelAcquisition {
                txn: ghost,
                mode: RelLockMode::AccessExclusive,
                seq: 0,
            });
        mgr.lock_advisory()
            .entry(4)
            .or_default()
            .push(super::AdvEntry {
                txn: ghost,
                shared: false,
                seq: 0,
            });
        let u = core.begin(Isolation::ReadCommitted);
        assert!(ok(mgr.lock_relation(
            &core,
            &u,
            4,
            RelLockMode::AccessExclusive,
            LockWait::NoWait,
            None
        )));
        assert!(ok(mgr.advisory_xact_lock(
            &core,
            &u,
            4,
            false,
            LockWait::NoWait,
            None
        )));
        ok(core.abort(u));
    }
}
