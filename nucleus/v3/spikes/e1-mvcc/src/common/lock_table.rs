//! The in-memory lock table (card X-1a Work 3; PLAN-v3 §2 N2; C-T0 §6).
//!
//! Exclusive row locks live here as values-free owners: an entry names
//! the owning txn and nothing else. Every key-existence or row operation
//! locks its key, including keys that do not exist yet (N2). There is no
//! deadlock detector — tests use timeouts (card Work 3).
//!
//! Invariants owned here:
//! - L-EXCL (C-LOCK-EXCL): no two txns hold the same key, and while a
//!   txn holds the table-wide escalated owner no other txn holds or
//!   acquires any row.
//! - L-NOOP: acquiring a key the txn already owns is a no-op.
//! - L-WAIT: `NoWait` returns [`TxnErr::LockNotAvailable`] on conflict;
//!   `Block(d)` returns [`TxnErr::LockTimeout`] on expiry, and is
//!   otherwise immune to spurious wakeups.
//! - L-ESCALATE: past `escalate_at` row locks the txn's rows are replaced
//!   by **one** table-wide exclusive owner held until release
//!   (`escalations()` counts it). Escalation is attempted when the count
//!   crosses the threshold and is deferred (not lost — the next acquire
//!   retries) while any foreign txn still holds a row, because a
//!   table-wide owner must exclude every row (L-EXCL). The claim/insert
//!   handshake below keeps escalation atomic against concurrent inserts.
//! - L-RELEASE: `release_all` wakes the waiters of every shard it freed
//!   and of the table owner (C-T0 §6 "waking").
//!
//! Lock order (module docs, [`crate::common`]): table-owner mutex →
//! per-txn index mutex → shard mutexes in index order. A thread waiting
//! on a condvar holds **only** the condvar's mutex.
//!
//! The escalation race (L-ESCALATE) and its closure: an inserter checks
//! the table owner, inserts its row, then **re-checks** the owner; an
//! escalant claims the owner, sweeps the shards, and only keeps the claim
//! if no foreign row appeared. Whichever of the two acted first is seen
//! by the other's second step, so the pair can never interleave into
//! "escalant owns the table while an inserter keeps its row".

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use crate::api::{TxnErr, TxnId, Wait};

/// Number of threads currently inside [`LockTable::acquire`]'s wait
/// paths, process-wide (see [`waiting`]).
static WAITING: AtomicU64 = AtomicU64::new(0);

/// Counts one parked waiter for as long as it lives (RAII, so every
/// return path — timeout, engine error, clear — decrements).
struct WaitingGuard;

impl WaitingGuard {
    fn new() -> Self {
        WAITING.fetch_add(1, Ordering::AcqRel);
        WaitingGuard
    }
}

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        WAITING.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The number of threads currently inside [`LockTable::acquire`]'s wait
/// paths (shard or table-owner), across every lock table in the process.
///
/// This is test observability for the suite's blocked-path scenarios
/// (verdict X-1a D1): a scenario spawns a waiter, waits until this
/// count rises, and only then resolves the blocker. The barrier proves
/// the waiter entered the wait path — and therefore ran everything the
/// engine does before it, including any pre-acquire conflict checks —
/// **while the blocker still held the lock**. Without it the waiter
/// thread may not be scheduled until after the blocker's commit, and
/// the check-after-wait re-check the scenario exists to exercise never
/// runs (the verdict's O7 mutant then passes by scheduling luck).
///
/// Scenarios run sequentially inside `run_all` and join their threads
/// before returning, so at a barrier the count is 0 until the scenario's
/// own waiter parks. Like [`crate::common::commit::apply_gate`] the
/// counter is process-global because the [`crate::api::TxnEngine`]
/// trait exposes no per-engine hooks.
///
/// Cost: one atomic pair, taken only on paths that are about to park on
/// a condvar — the uncontended acquire path (the common case) never
/// touches it.
pub fn waiting() -> u64 {
    WAITING.load(Ordering::Acquire)
}

/// Number of independently latched key shards (card Work 3).
pub const SHARDS: usize = 64;

/// Default per-txn row-lock budget past which locks escalate to one
/// table-wide owner (card Work 3; PLAN-v3 N2).
pub const DEFAULT_ESCALATE_AT: usize = 1_000_000;

/// The owner of one lock-table entry: the owning txn, and nothing else
/// (N2: "an entry is a values-free intent ... owner"). Table-wide
/// escalation is held in the table-owner slot, not in rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Owner(
    /// The owning txn.
    pub TxnId,
);

/// Per-txn lock bookkeeping (the "per-txn count" of card Work 3).
#[derive(Default)]
struct TxnLocks {
    keys: HashSet<Vec<u8>>,
    escalated: bool,
}

/// The table-wide exclusive owner slot (L-ESCALATE).
#[derive(Default)]
struct TableState {
    owner: Option<TxnId>,
}

/// The sharded lock table (see the module docs).
pub struct LockTable {
    shards: Vec<Mutex<HashMap<Vec<u8>, Owner>>>,
    cvs: Vec<Condvar>,
    table: Mutex<TableState>,
    table_cv: Condvar,
    by_txn: Mutex<HashMap<TxnId, TxnLocks>>,
    escalate_at: usize,
    escalations: AtomicU64,
}

impl LockTable {
    /// A lock table with the default escalation budget
    /// ([`DEFAULT_ESCALATE_AT`]).
    pub fn new() -> Self {
        Self::with_escalate_at(DEFAULT_ESCALATE_AT)
    }

    /// A lock table whose per-txn escalation budget is `escalate_at`
    /// rows (tests use small values to exercise L-ESCALATE).
    pub fn with_escalate_at(escalate_at: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            cvs: (0..SHARDS).map(|_| Condvar::new()).collect(),
            table: Mutex::new(TableState::default()),
            table_cv: Condvar::new(),
            by_txn: Mutex::new(HashMap::new()),
            escalate_at,
            escalations: AtomicU64::new(0),
        }
    }

    fn shard_of(key: &[u8]) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % SHARDS
    }

    fn lock_table(&self) -> Result<std::sync::MutexGuard<'_, TableState>, TxnErr> {
        self.table
            .lock()
            .map_err(|e| TxnErr::Engine(crate::common::was_poisoned(&e)))
    }

    fn lock_by_txn(&self) -> Result<std::sync::MutexGuard<'_, HashMap<TxnId, TxnLocks>>, TxnErr> {
        self.by_txn
            .lock()
            .map_err(|e| TxnErr::Engine(crate::common::was_poisoned(&e)))
    }

    /// Acquire `key` for `txn` (L-EXCL, L-NOOP, L-WAIT). An already-owned
    /// key is a no-op; a foreign table owner or row owner is resolved per
    /// `wait`.
    pub fn acquire(&self, key: &[u8], txn: TxnId, wait: Wait) -> Result<(), TxnErr> {
        let deadline = match wait {
            Wait::Block(d) => Some(Instant::now() + d),
            Wait::NoWait => None,
        };
        loop {
            // (1) The table-wide owner excludes every row.
            {
                let t = self.lock_table()?;
                if let Some(owner) = t.owner {
                    if owner == txn {
                        return Ok(()); // we escalated: every key is ours
                    }
                    drop(t);
                    self.wait_table_owner(owner, wait, deadline)?;
                    continue;
                }
            }
            // (2) The row, under the per-txn index and the shard.
            let idx = Self::shard_of(key);
            let count;
            {
                let mut by = self.lock_by_txn()?;
                let mut sh = match self.shards.get(idx).map(|m| m.lock()) {
                    Some(Ok(g)) => g,
                    Some(Err(e)) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
                    None => return Err(TxnErr::Engine("lock shard unavailable".into())),
                };
                if let Some(owner) = sh.get(key).copied() {
                    if owner.0 == txn {
                        return Ok(()); // already ours (L-NOOP)
                    }
                    drop(by);
                    drop(sh);
                    self.wait_shard(idx, key, txn, wait, deadline)?;
                    continue;
                }
                sh.insert(key.to_vec(), Owner(txn));
                drop(sh);
                count = {
                    let entry = by.entry(txn).or_default();
                    entry.keys.insert(key.to_vec());
                    entry.keys.len()
                };
                drop(by);
            }
            // (3) Re-check the table owner: if an escalation claimed the
            // table between (1) and the insert, undo the insert and take
            // the wait path instead (L-ESCALATE's handshake).
            {
                let t = self.lock_table()?;
                if let Some(owner) = t.owner
                    && owner != txn
                {
                    drop(t);
                    self.remove_row(key, txn);
                    self.wait_table_owner(owner, wait, deadline)?;
                    continue;
                }
            }
            // (4) Past the budget, replace our rows with the table owner.
            if count > self.escalate_at {
                let _ = self.try_escalate(txn);
            }
            return Ok(());
        }
    }

    /// Release every lock `txn` holds (rows and a table-wide owner) and
    /// wake its waiters (L-RELEASE).
    pub fn release_all(&self, txn: TxnId) {
        if let Ok(mut t) = self.table.lock()
            && t.owner == Some(txn)
        {
            t.owner = None;
        }
        self.table_cv.notify_all();
        if let Ok(mut by) = self.by_txn.lock() {
            by.remove(&txn);
        }
        for i in 0..SHARDS {
            if let Some(sh) = self.shards.get(i)
                && let Ok(mut sh) = sh.lock()
            {
                sh.retain(|_, o| o.0 != txn);
            }
            if let Some(cv) = self.cvs.get(i) {
                cv.notify_all();
            }
        }
    }

    /// Total entries held: row entries plus one for a table-wide owner
    /// (the `entries()` stat of card Work 3).
    pub fn entries(&self) -> u64 {
        let mut n = 0u64;
        for sh in &self.shards {
            if let Ok(sh) = sh.lock() {
                n += sh.len() as u64;
            }
        }
        if let Ok(t) = self.table.lock()
            && t.owner.is_some()
        {
            n += 1;
        }
        n
    }

    /// Rows currently held by `txn` (0 once escalated; the table-wide
    /// owner covers every key).
    pub fn held_by(&self, txn: TxnId) -> u64 {
        match self.by_txn.lock() {
            Ok(by) => by.get(&txn).map_or(0, |l| l.keys.len() as u64),
            Err(_) => 0,
        }
    }

    /// Whether `txn` holds the table-wide owner (L-ESCALATE).
    pub fn is_escalated(&self, txn: TxnId) -> bool {
        match self.by_txn.lock() {
            Ok(by) => by.get(&txn).is_some_and(|l| l.escalated),
            Err(_) => false,
        }
    }

    /// How many times a txn escalated (the `escalations()` stat).
    pub fn escalations(&self) -> u64 {
        self.escalations.load(Ordering::Relaxed)
    }

    /// Remove one row owned by `txn` (the escalation-race undo of step
    /// (3), and `try_escalate`'s sweep).
    fn remove_row(&self, key: &[u8], txn: TxnId) {
        let idx = Self::shard_of(key);
        if let Ok(mut by) = self.by_txn.lock() {
            if let Some(lock) = by.get_mut(&txn) {
                lock.keys.remove(key);
            }
            drop(by);
        }
        if let Some(sh) = self.shards.get(idx)
            && let Ok(mut sh) = sh.lock()
        {
            sh.retain(|k, o| !(k.as_slice() == key && o.0 == txn));
        }
    }

    /// Try to replace `txn`'s rows with the table-wide owner
    /// (L-ESCALATE). Returns whether the escalation happened; a foreign
    /// row or a foreign owner defers it to a later acquire without
    /// dropping any of `txn`'s rows.
    fn try_escalate(&self, txn: TxnId) -> bool {
        // Claim the owner slot: exactly one escalant can hold it.
        {
            let Ok(mut t) = self.table.lock() else {
                return false;
            };
            if t.owner.is_some() {
                return false; // another txn owns the table: not now
            }
            t.owner = Some(txn);
        }
        // Pass 1: a foreign row still held? Then un-claim and retry on a
        // later acquire — a table owner must exclude every row (L-EXCL).
        let mut foreign = false;
        for sh in &self.shards {
            if let Ok(sh) = sh.lock()
                && sh.values().any(|o| o.0 != txn)
            {
                foreign = true;
                break;
            }
        }
        if foreign {
            if let Ok(mut t) = self.table.lock()
                && t.owner == Some(txn)
            {
                t.owner = None;
            }
            self.table_cv.notify_all();
            return false;
        }
        // Pass 2: drop our rows. Inserts that raced the claim undo
        // themselves in acquire step (3), so no new row can appear under
        // the claim.
        for (i, sh) in self.shards.iter().enumerate() {
            if let Ok(mut sh) = sh.lock() {
                sh.retain(|_, o| o.0 != txn);
            }
            if let Some(cv) = self.cvs.get(i) {
                cv.notify_all();
            }
        }
        if let Ok(mut by) = self.by_txn.lock()
            && let Some(lock) = by.get_mut(&txn)
        {
            lock.keys.clear();
            lock.escalated = true;
        }
        self.escalations.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Wait until the table-wide `owner` is gone (L-WAIT). `Ok(())`
    /// means "something changed, retry the acquire loop".
    fn wait_table_owner(
        &self,
        owner: TxnId,
        wait: Wait,
        deadline: Option<Instant>,
    ) -> Result<(), TxnErr> {
        if matches!(wait, Wait::NoWait) {
            return Err(TxnErr::LockNotAvailable);
        }
        let deadline = match deadline {
            Some(d) => d,
            None => return Err(TxnErr::LockNotAvailable),
        };
        let mut t = self.lock_table()?;
        let _waiting = WaitingGuard::new();
        loop {
            if t.owner != Some(owner) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(TxnErr::LockTimeout);
            }
            let (guard, _) = match self
                .table_cv
                .wait_timeout(t, deadline.saturating_duration_since(now))
            {
                Ok(w) => w,
                Err(e) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
            };
            t = guard;
        }
    }

    /// Wait until the shard entry for `key` is gone or ours (L-WAIT).
    fn wait_shard(
        &self,
        idx: usize,
        key: &[u8],
        txn: TxnId,
        wait: Wait,
        deadline: Option<Instant>,
    ) -> Result<(), TxnErr> {
        if matches!(wait, Wait::NoWait) {
            return Err(TxnErr::LockNotAvailable);
        }
        let deadline = match deadline {
            Some(d) => d,
            None => return Err(TxnErr::LockNotAvailable),
        };
        let shard_mutex = match self.shards.get(idx) {
            Some(m) => m,
            None => return Err(TxnErr::Engine("lock shard unavailable".into())),
        };
        let cv = match self.cvs.get(idx) {
            Some(c) => c,
            None => return Err(TxnErr::Engine("lock shard condvar unavailable".into())),
        };
        let mut sh = match shard_mutex.lock() {
            Ok(g) => g,
            Err(e) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
        };
        let _waiting = WaitingGuard::new();
        loop {
            let clear = match sh.get(key) {
                None => true,
                Some(o) => o.0 == txn,
            };
            if clear {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(TxnErr::LockTimeout);
            }
            let (guard, _) = match cv.wait_timeout(sh, deadline.saturating_duration_since(now)) {
                Ok(w) => w,
                Err(e) => return Err(TxnErr::Engine(crate::common::was_poisoned(&e))),
            };
            sh = guard;
        }
    }
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::LockTable;
    use crate::api::{TxnErr, Wait};

    /// L-NOOP: acquiring an owned key again succeeds immediately.
    #[test]
    fn already_owned_is_a_noop() {
        let lt = LockTable::new();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        assert_eq!(lt.entries(), 1);
    }

    /// L-WAIT: NoWait conflicts with 55P03; Block(d) expires with
    /// LockTimeout.
    #[test]
    fn wait_policies() {
        let lt = LockTable::new();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        assert_eq!(
            lt.acquire(b"k", 2, Wait::NoWait),
            Err(TxnErr::LockNotAvailable)
        );
        assert_eq!(
            lt.acquire(b"k", 2, Wait::Block(Duration::from_millis(20))),
            Err(TxnErr::LockTimeout)
        );
        // a foreign NoWait while escalated behaves the same (below)
        lt.release_all(1);
        assert!(lt.acquire(b"k", 2, Wait::NoWait).is_ok());
    }

    /// L-RELEASE: release_all wakes a blocked waiter, which then acquires.
    #[test]
    fn release_wakes_the_waiter() {
        let lt = LockTable::new();
        lt.acquire(b"k", 1, Wait::NoWait).unwrap();
        let result = thread::scope(|s| {
            let waiter = {
                let lt = &lt;
                s.spawn(move || lt.acquire(b"k", 2, Wait::Block(Duration::from_secs(30))))
            };
            thread::sleep(Duration::from_millis(50)); // let the waiter park
            lt.release_all(1);
            waiter.join().is_ok_and(|r| r.is_ok())
        });
        assert!(result);
        assert_eq!(lt.held_by(2), 1, "the waiter took the key");
    }

    /// L-ESCALATE: past the budget the rows are replaced by one
    /// table-wide owner that excludes every other txn until release.
    #[test]
    fn escalation_replaces_rows_with_a_table_owner() {
        let lt = LockTable::with_escalate_at(2);
        for i in 0..3u8 {
            lt.acquire(&[i], 9, Wait::NoWait).unwrap();
        }
        assert!(lt.is_escalated(9), "three rows past the budget of two");
        assert_eq!(lt.held_by(9), 0, "rows were replaced by the owner");
        assert_eq!(lt.escalations(), 1);
        assert_eq!(lt.entries(), 1, "only the table owner remains");
        // every other txn is excluded, NoWait or not-yet-waited
        assert_eq!(
            lt.acquire(b"zzz", 7, Wait::NoWait),
            Err(TxnErr::LockNotAvailable)
        );
        // and the escalated txn itself acquires anything as a no-op
        lt.acquire(b"zzz", 9, Wait::NoWait).unwrap();
        // held until release
        lt.release_all(9);
        assert!(!lt.is_escalated(9));
        assert!(lt.acquire(b"zzz", 7, Wait::NoWait).is_ok());
    }

    /// L-ESCALATE: a foreign row defers escalation instead of breaking
    /// L-EXCL; the escalation lands on a later acquire once the table is
    /// otherwise empty.
    #[test]
    fn escalation_waits_for_foreign_rows() {
        let lt = LockTable::with_escalate_at(1);
        lt.acquire(b"a", 7, Wait::NoWait).unwrap();
        // txn 9 crosses the budget but 7 holds a row: no escalation yet
        lt.acquire(b"b", 9, Wait::NoWait).unwrap();
        lt.acquire(b"c", 9, Wait::NoWait).unwrap();
        assert!(!lt.is_escalated(9));
        assert_eq!(lt.escalations(), 0);
        // 7 releases; 9's next acquire completes the escalation
        lt.release_all(7);
        lt.acquire(b"d", 9, Wait::NoWait).unwrap();
        assert!(lt.is_escalated(9));
        assert_eq!(lt.escalations(), 1);
    }
}
