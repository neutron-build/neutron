//! Table-level strict two-phase locking, for engines that have no versioning.
//!
//! # Why locking and not SSI
//!
//! `MvccStorageAdapter` gets serializability from SSI: snapshots plus
//! rw-antidependency tracking. SSI is built ON snapshot isolation — it needs a
//! consistent read snapshot to detect conflicts against. `BufferedDiskEngine`
//! has no versioning at all; its reads go straight through to the current state
//! of `DiskEngine`, so there is no snapshot to build on and SSI is not
//! available at any price short of putting MVCC on disk.
//!
//! Locking needs none of that. Strict 2PL — acquire before touching, release
//! only at commit/abort — yields conflict-serializable schedules directly from
//! the lock discipline, with no version storage anywhere. It costs concurrency
//! rather than correctness, which is the right trade for a level the caller had
//! to ask for by name.
//!
//! # Why TABLE granularity
//!
//! Row-level locks would allow more concurrency and would NOT be sufficient:
//! serializability has to rule out phantoms, and a row lock cannot lock a row
//! that does not exist yet. Postgres solves that with predicate locks over the
//! SSI graph; a locking engine solves it by locking the predicate's whole
//! domain, which at this granularity is the table. So table-level S/X locks buy
//! phantom-freedom for free, and anything finer would need a predicate-locking
//! scheme layered back on top to be correct at all.
//!
//! # Scope: SERIALIZABLE transactions only
//!
//! Only transactions that asked for SERIALIZABLE take locks. This matches
//! PostgreSQL, whose own guarantee holds only among serializable transactions —
//! "a serializable transaction can be affected by concurrent non-serializable
//! transactions" — and it means every existing session pays exactly one atomic
//! load on the read path and nothing else.
//!
//! # Deadlock: wait-die, not detection
//!
//! Two transactions can always deadlock under 2PL (T1 holds A wants B, T2 holds
//! B wants A), so something must break the cycle. Wait-die does it without a
//! waits-for graph: on conflict, an OLDER transaction waits, a YOUNGER one dies
//! immediately with a serialization failure. Age is the monotone sequence
//! assigned at first lock acquisition, so "older" is a total order and no cycle
//! can form — every edge in a wait-for graph would have to point from older to
//! younger, and such a graph is acyclic by construction. No detector to run, no
//! timeout to tune, and no false negatives. A transaction that dies is retried
//! by the client exactly as it would retry an SSI abort.
//!
//! Lock UPGRADE (holding S, wanting X) is the one case wait-die does not cover
//! on its own: two transactions each holding S on a table and each wanting X
//! deadlock regardless of age, because neither is waiting on a lock the other
//! could be made to release. It is resolved the same way — the younger dies —
//! by treating an upgrade as a conflict against every other S holder.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::StorageError;

/// What a transaction holds on one table.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LockMode {
    /// Read. Compatible with other shared holders.
    Shared,
    /// Write. Compatible with nothing.
    Exclusive,
}

#[derive(Default)]
struct TableLock {
    /// Transaction ids holding this table in shared mode.
    shared: Vec<u64>,
    /// The transaction id holding this table exclusively, if any.
    exclusive: Option<u64>,
}

impl TableLock {
    fn is_free(&self) -> bool {
        self.shared.is_empty() && self.exclusive.is_none()
    }
}

/// Table-level strict-2PL lock table, shared by all sessions of one engine.
pub struct LockManager {
    locks: Mutex<HashMap<String, TableLock>>,
    /// Age counter. Lower = older = higher priority under wait-die.
    next_age: AtomicU64,
    /// Age assigned to each live locking transaction, and the tables it holds.
    txns: Mutex<HashMap<u64, TxnLocks>>,
    /// Woken whenever any lock is released, so waiters re-check.
    released: Notify,
}

#[derive(Default)]
struct TxnLocks {
    age: u64,
    held: HashMap<String, LockMode>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            next_age: AtomicU64::new(1),
            txns: Mutex::new(HashMap::new()),
            released: Notify::new(),
        }
    }

    /// The serialization failure a dying transaction reports. Worded so the
    /// client sees the same actionable thing an SSI abort produces: retry.
    fn die(table: &str) -> StorageError {
        StorageError::Io(format!(
            "could not serialize access to table '{table}' due to concurrent \
             update: this SERIALIZABLE transaction was aborted to break a \
             potential deadlock (retry the transaction)"
        ))
    }

    /// Age of `txn`, assigning one on first use. Ages are handed out at first
    /// lock, not at BEGIN, which is all wait-die needs — it needs a consistent
    /// total order, not wall-clock start times.
    fn age_of(&self, txn: u64) -> u64 {
        let mut txns = self.txns.lock();
        txns.entry(txn)
            .or_insert_with(|| TxnLocks {
                age: self.next_age.fetch_add(1, Ordering::Relaxed),
                held: HashMap::new(),
            })
            .age
    }

    /// Try to grant `mode` on `table` to `txn` without blocking.
    ///
    /// `Ok(true)` granted, `Ok(false)` must wait, `Err` must die.
    fn try_grant(&self, txn: u64, table: &str, mode: LockMode) -> Result<bool, StorageError> {
        let my_age = self.age_of(txn);
        let mut locks = self.locks.lock();
        let entry = locks.entry(table.to_string()).or_default();

        // Already hold it at this strength or stronger.
        if entry.exclusive == Some(txn) {
            return Ok(true);
        }
        if mode == LockMode::Shared && entry.shared.contains(&txn) {
            return Ok(true);
        }

        let conflicting: Vec<u64> = match mode {
            LockMode::Shared => entry.exclusive.iter().copied().collect(),
            // For X, every other holder conflicts — including other S holders,
            // which is the upgrade case.
            LockMode::Exclusive => entry
                .exclusive
                .iter()
                .copied()
                .chain(entry.shared.iter().copied().filter(|&t| t != txn))
                .collect(),
        };

        if conflicting.is_empty() {
            match mode {
                LockMode::Shared => entry.shared.push(txn),
                LockMode::Exclusive => {
                    entry.shared.retain(|&t| t != txn); // upgrade in place
                    entry.exclusive = Some(txn);
                }
            }
            drop(locks);
            let mut txns = self.txns.lock();
            if let Some(t) = txns.get_mut(&txn) {
                t.held.insert(table.to_string(), mode);
            }
            return Ok(true);
        }

        // Wait-die: if ANY conflicting holder is older than me, I die. Comparing
        // against all of them (rather than the first) keeps the rule total —
        // waiting on a set containing an older transaction is exactly the case
        // that could close a cycle.
        drop(locks);
        let txns = self.txns.lock();
        let any_older = conflicting
            .iter()
            .any(|t| txns.get(t).is_some_and(|o| o.age < my_age));
        drop(txns);
        if any_older {
            return Err(Self::die(table));
        }
        Ok(false)
    }

    /// Acquire `mode` on `table` for `txn`, waiting if an older transaction
    /// holds it and dying if a younger one does.
    pub async fn acquire(
        &self,
        txn: u64,
        table: &str,
        mode: LockMode,
    ) -> Result<(), StorageError> {
        loop {
            // Register interest BEFORE the try, so a release racing between the
            // failed try and the await cannot be missed.
            let woken = self.released.notified();
            if self.try_grant(txn, table, mode)? {
                return Ok(());
            }
            woken.await;
        }
    }

    /// Release everything `txn` holds. Called at commit and at abort — strict
    /// 2PL releases at end of transaction and nowhere else, which is what makes
    /// the schedule recoverable as well as serializable.
    pub fn release_all(&self, txn: u64) {
        let held = {
            let mut txns = self.txns.lock();
            match txns.remove(&txn) {
                Some(t) => t.held,
                None => return,
            }
        };
        if held.is_empty() {
            self.released.notify_waiters();
            return;
        }
        {
            let mut locks = self.locks.lock();
            for table in held.keys() {
                if let Some(entry) = locks.get_mut(table) {
                    entry.shared.retain(|&t| t != txn);
                    if entry.exclusive == Some(txn) {
                        entry.exclusive = None;
                    }
                    if entry.is_free() {
                        locks.remove(table);
                    }
                }
            }
        }
        self.released.notify_waiters();
    }

    /// Whether `txn` currently holds any lock (test/observability).
    #[cfg(test)]
    pub fn holds_any(&self, txn: u64) -> bool {
        self.txns
            .lock()
            .get(&txn)
            .is_some_and(|t| !t.held.is_empty())
    }

    /// Total tables currently locked (test/observability).
    #[cfg(test)]
    pub fn locked_tables(&self) -> usize {
        self.locks.lock().len()
    }
}

/// Shared handle.
pub type SharedLockManager = Arc<LockManager>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_locks_do_not_conflict() {
        let lm = LockManager::new();
        lm.acquire(1, "t", LockMode::Shared).await.unwrap();
        lm.acquire(2, "t", LockMode::Shared).await.unwrap();
        assert!(lm.holds_any(1) && lm.holds_any(2));
    }

    #[tokio::test]
    async fn exclusive_excludes_shared() {
        let lm = LockManager::new();
        // txn 1 is older (acquires first, so gets the lower age).
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
        // txn 2 is younger and conflicts → dies rather than waiting forever.
        let r = lm.acquire(2, "t", LockMode::Shared).await;
        assert!(r.is_err(), "younger reader must die against an older writer");
    }

    #[tokio::test]
    async fn releasing_lets_a_waiter_proceed() {
        let lm = Arc::new(LockManager::new());
        // Establish ages: 2 becomes OLDER than 1 by locking something first.
        lm.acquire(2, "other", LockMode::Shared).await.unwrap();
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();

        // txn 2 is older than txn 1, so it WAITS rather than dying.
        let lm2 = lm.clone();
        let waiter = tokio::spawn(async move { lm2.acquire(2, "t", LockMode::Shared).await });
        // Give the waiter a chance to block, then release.
        tokio::task::yield_now().await;
        lm.release_all(1);
        assert!(waiter.await.unwrap().is_ok(), "older waiter must be granted");
    }

    #[tokio::test]
    async fn upgrade_from_shared_to_exclusive_succeeds_when_alone() {
        let lm = LockManager::new();
        lm.acquire(1, "t", LockMode::Shared).await.unwrap();
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
        // And the upgrade actually replaced the shared entry rather than
        // leaving a stale self-conflict behind.
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
    }

    #[tokio::test]
    async fn conflicting_upgrades_kill_the_younger_instead_of_deadlocking() {
        let lm = LockManager::new();
        lm.acquire(1, "t", LockMode::Shared).await.unwrap(); // older
        lm.acquire(2, "t", LockMode::Shared).await.unwrap(); // younger
        // Both want X. Under plain 2PL this is the classic upgrade deadlock;
        // wait-die must break it by killing the younger.
        let r = lm.acquire(2, "t", LockMode::Exclusive).await;
        assert!(r.is_err(), "younger upgrader must die, not deadlock");
    }

    #[tokio::test]
    async fn release_all_frees_the_table_entry() {
        let lm = LockManager::new();
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
        assert_eq!(lm.locked_tables(), 1);
        lm.release_all(1);
        assert_eq!(lm.locked_tables(), 0, "released locks must not leak entries");
        assert!(!lm.holds_any(1));
    }

    #[tokio::test]
    async fn an_older_transaction_waits_rather_than_dying() {
        let lm = Arc::new(LockManager::new());
        lm.acquire(1, "a", LockMode::Shared).await.unwrap(); // age 1, older
        lm.acquire(2, "b", LockMode::Exclusive).await.unwrap(); // age 2, younger

        // Older txn 1 wants what younger txn 2 holds → waits.
        let lm2 = lm.clone();
        let waiter = tokio::spawn(async move { lm2.acquire(1, "b", LockMode::Shared).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "older transaction should be waiting");
        lm.release_all(2);
        assert!(waiter.await.unwrap().is_ok());
    }
}
