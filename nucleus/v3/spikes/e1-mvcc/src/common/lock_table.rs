//! The in-memory lock table (card X-1a Work 3, perf rework card X-1a-F1;
//! PLAN-v3 §2 N2; C-T0 §6).
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
//!   crosses the threshold and is deferred, not lost, while any foreign
//!   txn still holds a row, because a table-wide owner must exclude
//!   every row (L-EXCL). The claim/insert handshake below keeps
//!   escalation atomic against concurrent inserts.
//! - L-RELEASE: `release_all` costs O(the keys the txn itself holds),
//!   takes at most `min(keys, SHARDS)` key-shard locks (counted by
//!   `shard_visits()`), and wakes the waiters of every shard it freed
//!   and of the table owner (C-T0 §6 "waking"). An escalated txn holds
//!   no rows: its release clears the owner and touches no shard.
//! - **L-ESCALATE-DEFER (decided in card X-1a-F1; S-2 decides the
//!   production rule).** Escalation never *waits* on a foreign row
//!   holder: a waiting escalant already holding a million rows
//!   deadlocks against a foreign holder blocked on one of them, and
//!   this lock table has no deadlock detector. Escalation is
//!   **opportunistic**: it is attempted when a txn's row count first
//!   passes `escalate_at`, and after a failed attempt it is retried
//!   only after another *retry step* of acquires by the same txn —
//!   `escalation_retry_step()` = `max(escalate_at / 8, 1)`. Each
//!   attempt's foreign-row test is O(1): the table's total row-entry
//!   counter minus the txn's own count — never a shard scan. Under
//!   sustained foreign traffic a big txn may therefore never escalate;
//!   that outcome is legitimate and is what W6 must report.
//!
//! Lock order (one order everywhere): **per-txn index shard → key
//! shard.** The per-txn index is itself sharded by `TxnId`
//! ([`IDX_SHARDS`]), so txns landing on different index shards never
//! contend on acquire (the pre-F1 global index mutex serialized every
//! acquire in the process). The only nested acquisition in the table is
//! an index shard held across a key shard (acquire step (2) below). The
//! table-owner mutex is always taken alone — never held across an
//! index-shard or key-shard acquisition, and neither is held while
//! taking it. A thread waiting on a condvar holds only that condvar's
//! mutex.
//!
//! The escalation race (L-ESCALATE) and its closure, re-derived for the
//! counter layout: an inserter inserts its row and bumps the row
//! counter, then re-checks the table owner, undoing the insert if an
//! owner appeared; an escalant tests the counter (`total > own` =
//! a foreign row exists), claims the owner, and re-tests the counter,
//! un-claiming if a foreign row appeared. The inserter bumps the
//! counter before its owner check, and the escalant reads the counter
//! after releasing its claim's critical section, so whichever of the
//! two acted first is seen by the other's second step through the
//! table mutex — the pair can never interleave into "escalant owns the
//! table while an inserter keeps its row".

use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Number of independently latched per-txn index shards (card X-1a-F1
/// Work 2). Index shards and key shards are unrelated lattices: the
/// first is selected by `TxnId`, the second by key.
const IDX_SHARDS: usize = 64;

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
    /// The key count at which the next escalation attempt is allowed
    /// (L-ESCALATE-DEFER's retry step bookkeeping; 0 = attempt as soon
    /// as the count passes `escalate_at`). Only the txn's own thread
    /// reads and writes it, under its index shard's mutex.
    retry_at: usize,
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
    by_txn: Vec<Mutex<HashMap<TxnId, TxnLocks>>>,
    /// Total row entries across all shards: the O(1) foreign-row test of
    /// L-ESCALATE-DEFER is `rows > own count`. Maintained under the
    /// shard mutex at every insert and removal.
    rows: AtomicU64,
    escalate_at: usize,
    escalations: AtomicU64,
    attempts: AtomicU64,
    visits: AtomicU64,
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
            by_txn: (0..IDX_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            rows: AtomicU64::new(0),
            escalate_at,
            escalations: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            visits: AtomicU64::new(0),
        }
    }

    fn shard_of(key: &[u8]) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % SHARDS
    }

    fn idx_of(txn: TxnId) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        txn.hash(&mut h);
        (h.finish() as usize) % IDX_SHARDS
    }

    fn lock_table(&self) -> Result<std::sync::MutexGuard<'_, TableState>, TxnErr> {
        self.table
            .lock()
            .map_err(|e| TxnErr::Engine(crate::common::was_poisoned(&e)))
    }

    /// Lock the per-txn index shard owning `txn`'s entry (lock order:
    /// index shard → key shard; never the reverse).
    fn lock_index(
        &self,
        txn: TxnId,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<TxnId, TxnLocks>>, TxnErr> {
        match self.by_txn.get(Self::idx_of(txn)) {
            Some(m) => m
                .lock()
                .map_err(|e| TxnErr::Engine(crate::common::was_poisoned(&e))),
            None => Err(TxnErr::Engine("per-txn index shard unavailable".into())),
        }
    }

    /// The escalation retry step of L-ESCALATE-DEFER: after a failed
    /// attempt, the txn retries only once another this-many acquires
    /// have happened. A per-table value: `max(escalate_at / 8, 1)`.
    pub fn escalation_retry_step(&self) -> usize {
        (self.escalate_at / 8).max(1)
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
            // (2) The row, under the per-txn index shard and the key
            // shard, in that order. The counter bump is inside the
            // shard's critical section and before step (3), so any
            // escalant that claims after our insert sees our count
            // (module docs, "the escalation race").
            let idx = Self::shard_of(key);
            let count;
            let retry_at;
            {
                let mut by = self.lock_index(txn)?;
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
                self.rows.fetch_add(1, Ordering::Relaxed);
                let entry = by.entry(txn).or_default();
                entry.keys.insert(key.to_vec());
                count = entry.keys.len();
                retry_at = entry.retry_at;
                drop(sh);
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
            // (4) Past the budget, replace our rows with the table
            // owner — but only when the retry step allows a new
            // attempt (L-ESCALATE-DEFER). A failed attempt defers the
            // next one by one step of our own acquires.
            if count > self.escalate_at && count >= retry_at {
                if self.try_escalate(txn) {
                    return Ok(());
                }
                let step = self.escalation_retry_step();
                if let Ok(mut by) = self.lock_index(txn)
                    && let Some(entry) = by.get_mut(&txn)
                {
                    entry.retry_at = count.saturating_add(step);
                }
            }
            return Ok(());
        }
    }

    /// Release every lock `txn` holds (rows and a table-wide owner) and
    /// wake its waiters (L-RELEASE), through the per-txn index: group
    /// the txn's own keys by shard, and in each affected shard remove
    /// only keys whose owner is `txn`. Cost O(keys held by `txn`) plus
    /// at most `min(keys, SHARDS)` key-shard locks; an escalated txn
    /// holds no rows, so it clears the owner and touches no shard.
    pub fn release_all(&self, txn: TxnId) {
        // The table owner first (always alone): waiters on it wake as
        // soon as it clears.
        if let Ok(mut t) = self.table.lock()
            && t.owner == Some(txn)
        {
            t.owner = None;
            self.table_cv.notify_all();
        }
        // Take the key set out of the index entry in one pass.
        let keys = match self.lock_index(txn) {
            Ok(mut by) => by.remove(&txn).map_or_else(HashSet::new, |l| l.keys),
            Err(_) => return,
        };
        if keys.is_empty() {
            return;
        }
        // Group our keys by shard, then remove only ours from each
        // affected shard. Ascending shard order keeps the visit pattern
        // (and `shard_visits`) deterministic for a given key set.
        let mut grouped: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
        for k in keys {
            grouped.entry(Self::shard_of(&k)).or_default().push(k);
        }
        for (i, ks) in grouped {
            if let Some(sh) = self.shards.get(i)
                && let Ok(mut sh) = sh.lock()
            {
                self.visits.fetch_add(1, Ordering::Relaxed);
                for k in &ks {
                    if sh.get(k).is_some_and(|o| o.0 == txn) {
                        sh.remove(k);
                        self.rows.fetch_sub(1, Ordering::Relaxed);
                    }
                }
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
        match self.lock_index(txn) {
            Ok(by) => by.get(&txn).map_or(0, |l| l.keys.len() as u64),
            Err(_) => 0,
        }
    }

    /// Whether `txn` holds the table-wide owner (L-ESCALATE).
    pub fn is_escalated(&self, txn: TxnId) -> bool {
        match self.lock_index(txn) {
            Ok(by) => by.get(&txn).is_some_and(|l| l.escalated),
            Err(_) => false,
        }
    }

    /// How many times a txn escalated (the `escalations()` stat).
    pub fn escalations(&self) -> u64 {
        self.escalations.load(Ordering::Relaxed)
    }

    /// How many escalation attempts were made, successful or deferred
    /// (L-ESCALATE-DEFER observability: with a foreign row held, this
    /// grows by one per retry step, not one per acquire).
    #[doc(hidden)]
    pub fn escalation_attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    /// Key-shard mutex acquisitions made by `release_all` and
    /// `try_escalate` — the complexity observability of card X-1a-F1:
    /// releasing a txn's locks touches only the shards its own keys map
    /// to, and an escalation attempt that defers before claiming
    /// touches none.
    #[doc(hidden)]
    pub fn shard_visits(&self) -> u64 {
        self.visits.load(Ordering::Relaxed)
    }

    /// Remove one row owned by `txn` (the escalation-race undo of step
    /// (3)).
    fn remove_row(&self, key: &[u8], txn: TxnId) {
        if let Ok(mut by) = self.lock_index(txn) {
            if let Some(lock) = by.get_mut(&txn) {
                lock.keys.remove(key);
            }
            drop(by);
        }
        let idx = Self::shard_of(key);
        if let Some(sh) = self.shards.get(idx)
            && let Ok(mut sh) = sh.lock()
            && sh.get(key).is_some_and(|o| o.0 == txn)
        {
            sh.remove(key);
            self.rows.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Try to replace `txn`'s rows with the table-wide owner
    /// (L-ESCALATE, L-ESCALATE-DEFER). Returns whether the escalation
    /// happened; a foreign row (O(1) counter test, before **and** after
    /// the claim) or a foreign owner defers it to a later retry step
    /// without dropping any of `txn`'s rows.
    fn try_escalate(&self, txn: TxnId) -> bool {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        // Our own row count. Stable during the attempt: only this txn's
        // thread mutates its entry.
        let own = match self.lock_index(txn) {
            Ok(by) => by.get(&txn).map_or(0, |l| l.keys.len()),
            Err(_) => return false,
        } as u64;
        // Pre-claim foreign test (O(1)): claiming only to un-claim
        // would stall every other inserter on the owner slot.
        if self.rows.load(Ordering::Acquire) > own {
            return false;
        }
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
        // Post-claim re-test. An inserter that raced its bump between
        // the pre-claim test and the claim will un-do itself in acquire
        // step (3) — but one whose step-(3) check ran before our claim
        // does not, and only this re-test sees it (module docs).
        if self.rows.load(Ordering::Acquire) > own {
            if let Ok(mut t) = self.table.lock()
                && t.owner == Some(txn)
            {
                t.owner = None;
            }
            self.table_cv.notify_all();
            return false;
        }
        // Pass 2: drop our rows, grouped from our own key set — not a
        // scan of all shards.
        let keys = match self.lock_index(txn) {
            Ok(mut by) => match by.get_mut(&txn) {
                Some(l) => std::mem::take(&mut l.keys),
                None => HashSet::new(),
            },
            Err(_) => HashSet::new(),
        };
        let mut grouped: BTreeMap<usize, Vec<Vec<u8>>> = BTreeMap::new();
        for k in keys {
            grouped.entry(Self::shard_of(&k)).or_default().push(k);
        }
        for (i, ks) in grouped {
            if let Some(sh) = self.shards.get(i)
                && let Ok(mut sh) = sh.lock()
            {
                self.visits.fetch_add(1, Ordering::Relaxed);
                for k in &ks {
                    if sh.get(k).is_some_and(|o| o.0 == txn) {
                        sh.remove(k);
                        self.rows.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            if let Some(cv) = self.cvs.get(i) {
                cv.notify_all();
            }
        }
        // Mark the txn escalated while it still holds the claim.
        if let Ok(mut by) = self.lock_index(txn)
            && let Some(l) = by.get_mut(&txn)
        {
            l.escalated = true;
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
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use super::{LockTable, Owner};
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

    /// The `NO-RECLAIM-AFTER-CLAIM` catcher of card X-1a-F1: inserters
    /// race an escalant past its budget, and no foreign row may coexist
    /// with the table owner (L-EXCL).
    ///
    /// The interleaving being probed is microscopic: an inserter's
    /// counter bump and step-(3) owner check must both land between the
    /// escalant's pre-claim counter read and its claim of the table
    /// mutex. The round therefore multiplies the chances: eight
    /// inserters acquire and release continuously (dense insert traffic
    /// into every claim window), the escalant runs escalate/release
    /// cycles for as long as the inserters live, and every inserter
    /// that gets an `Ok` holds its row for a short randomized window
    /// (0–7 µs, covering a claim→pass-2 completion) while watching the
    /// `escalations()` level against a snapshot taken **after** the
    /// `Ok`.
    ///
    /// The detector is sound for correct code: `escalations()` is
    /// incremented only after the post-claim re-test passed and pass 2
    /// dropped the escalant's rows, so an inserter that has held its
    /// row continuously since its `Ok` (and whose snapshot postdates
    /// that `Ok`, hence every earlier escalation) can see a new
    /// increment only if some claim completed while its row was live —
    /// and the race closure in the module docs (the inserter's bump
    /// precedes its owner check; the re-test reads the counter after
    /// the claim) makes that impossible when the re-test exists.
    /// The `NO-RECLAIM-AFTER-CLAIM` catcher of card X-1a-F1 (white-box:
    /// it lives beside the code because the interleaving it probes is
    /// narrower than any scheduling luck — an inserter's counter bump
    /// *and* its step-(3) owner check must both land between the
    /// escalant's pre-claim counter read and its claim, and a fair table
    /// mutex always serves the queued escalant first).
    ///
    /// The orchestration parks a real escalant thread **inside** that
    /// gap: the test holds the table mutex while the thread runs
    /// [`LockTable::try_escalate`], so the thread has read the counter
    /// (no foreign row) and is queued to claim. The test then performs
    /// an in-flight inserter's step (2) exactly — row inserted, counter
    // bumped, and its step-(3) owner check already past (it saw no
    /// owner, so nothing will undo the row) — and releases the mutex.
    ///
    /// Correct code: the post-claim re-test reads the bumped counter,
    /// un-claims and defers; the foreign row and all of the escalant's
    /// rows survive untouched. A mutant that skips the re-test
    /// escalates onto a live foreign row and fails every assertion.
    /// The test cannot false-fail: if the escalant thread were delayed
    /// past the bump, its *pre*-claim test defers instead — the same
    /// observable outcome.
    #[test]
    fn escalation_claim_reclaims_racing_inserters() {
        const TRIALS: u64 = 5;
        const OWN: u64 = 32; // at with_escalate_at(32): setup never crosses
        for trial in 0..TRIALS {
            let lt = LockTable::with_escalate_at(32);
            for i in 0..OWN {
                lt.acquire(&i.to_be_bytes(), 1, Wait::NoWait).unwrap();
            }
            assert_eq!(lt.entries(), OWN);
            // Park the escalant between its pre-claim read and its
            // claim: it queues here while the test holds the mutex.
            let table = crate::common::lock_unpoisoned(&lt.table);
            let result = thread::scope(|s| {
                let lt = &lt;
                let escalant = s.spawn(move || lt.try_escalate(1));
                // Let the escalant pass its pre-claim read and queue on
                // the held mutex (generous margin; the whole path is a
                // few µs).
                thread::sleep(Duration::from_millis(2));
                // The in-flight inserter's step (2), verbatim: row in
                // its shard, total bumped. (No index entry: the point
                // is the shard row plus the counter the re-test reads.)
                let foreign = b"in-flight-inserter-row";
                let idx = LockTable::shard_of(foreign);
                if let Some(sh) = lt.shards.get(idx) {
                    let mut sh = crate::common::lock_unpoisoned(sh);
                    sh.insert(foreign.to_vec(), Owner(2));
                }
                lt.rows.fetch_add(1, Ordering::Relaxed);
                // Let the escalant claim.
                drop(table);
                escalant.join().unwrap()
            });
            assert!(
                !result,
                "trial {trial}: the escalation must not complete over a foreign row"
            );
            assert!(!lt.is_escalated(1), "trial {trial}");
            assert_eq!(lt.escalations(), 0, "trial {trial}");
            assert_eq!(
                lt.entries(),
                OWN + 1,
                "trial {trial}: the foreign row and the escalant's rows are all intact"
            );
            assert_eq!(lt.held_by(1), OWN, "trial {trial}");
            assert!(lt.escalation_attempts() >= 1, "trial {trial}: non-vacuity");
        }
    }
}
