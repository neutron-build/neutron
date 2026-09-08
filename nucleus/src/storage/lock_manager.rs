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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::StorageError;

/// Default `lock_timeout`, in milliseconds. Long enough that a legitimately
/// slow transaction is not cut off, short enough that a stuck one surfaces as
/// an error rather than a hang.
const DEFAULT_LOCK_TIMEOUT_MS: u64 = 10_000;

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

/// What a lock acquisition did, for metrics. The lock manager deliberately
/// does not depend on the metrics registry — it is a storage primitive and the
/// registry lives above it — so it reports and lets the caller record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AcquireOutcome {
    /// Granted with no conflicting holder.
    Immediate,
    /// Granted, but only after blocking for `waited`.
    Waited(std::time::Duration),
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
    /// How long a waiter may block before giving up, in milliseconds.
    /// 0 disables the timeout (wait forever, the original behaviour).
    ///
    /// Wait-die guarantees no DEADLOCK, but it does not bound how long an older
    /// transaction waits: the holder it is waiting on may simply be slow, or
    /// idle-in-transaction because a client walked away mid-statement. Without
    /// a bound, one such transaction parks every older one behind it
    /// indefinitely, which is indistinguishable from a hang to everyone
    /// involved. PostgreSQL exposes the same escape hatch as `lock_timeout`.
    timeout_ms: AtomicU64,
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
            timeout_ms: AtomicU64::new(DEFAULT_LOCK_TIMEOUT_MS),
        }
    }

    /// Set the lock wait bound. 0 disables it.
    pub fn set_timeout_ms(&self, ms: u64) {
        self.timeout_ms.store(ms, Ordering::Relaxed);
    }

    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms.load(Ordering::Relaxed)
    }

    /// The error a waiter returns when it gives up.
    ///
    /// Deliberately NOT a serialization failure: 40001 tells a client "retry,
    /// this was a conflict you could win next time", and a lock still held by
    /// a stuck transaction is not that. Reporting it as 40001 would send
    /// clients into a retry loop against a lock that is not going anywhere.
    /// `55P03 lock_not_available` is what PostgreSQL uses.
    fn timed_out(table: &str, waited_ms: u64) -> StorageError {
        StorageError::Io(format!(
            "lock_not_available: timed out after {waited_ms}ms waiting for a lock \
             on table '{table}' (raise lock_timeout, or find the transaction \
             holding it)"
        ))
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
    ) -> Result<AcquireOutcome, StorageError> {
        // Register interest BEFORE the first try, so a release racing between
        // a failed try and the await cannot be missed.
        let woken = self.released.notified();
        if self.try_grant(txn, table, mode)? {
            return Ok(AcquireOutcome::Immediate);
        }
        let started = std::time::Instant::now();
        let budget = self.timeout_ms();
        let mut woken = Some(woken);
        loop {
            let wait = match woken.take() {
                Some(w) => w,
                None => self.released.notified(),
            };
            if budget == 0 {
                wait.await;
            } else {
                let elapsed = started.elapsed().as_millis() as u64;
                if elapsed >= budget {
                    return Err(Self::timed_out(table, elapsed));
                }
                let remaining = std::time::Duration::from_millis(budget - elapsed);
                // A timeout here is not itself the answer — the lock may have
                // been released in the same instant — so fall through to one
                // more try_grant and let the elapsed check above decide.
                let _ = tokio::time::timeout(remaining, wait).await;
            }
            let next = self.released.notified();
            if self.try_grant(txn, table, mode)? {
                return Ok(AcquireOutcome::Waited(started.elapsed()));
            }
            if budget > 0 && started.elapsed().as_millis() as u64 >= budget {
                return Err(Self::timed_out(table, started.elapsed().as_millis() as u64));
            }
            woken = Some(next);
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

    /// Tables currently held under at least one lock.
    pub fn locked_table_count(&self) -> usize {
        self.locks.lock().len()
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

/// One row's lock identity: the base table name plus the row's primary-key
/// values, in constraint order. Not a hash — the actual key tuple, keyed by
/// `Value`'s hand-written `Hash`/`Eq` so `Int32(1)` and `Int64(1)` are the
/// same row by construction. A hash could collide and make SKIP LOCKED skip a
/// row nobody holds; the tuple cannot.
pub type RowLockKey = (String, Vec<crate::types::Value>);

/// What a non-blocking attempt on one row found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RowTry {
    /// The lock is now held by `session` (it was free, or already ours).
    Acquired,
    /// Another live transaction holds it.
    HeldElsewhere,
}

/// Row-level transaction locks for `FOR UPDATE` / `FOR SHARE` clauses.
///
/// This is deliberately NOT part of [`LockManager`]. That table is the
/// SERIALIZABLE strict-2PL machinery: wait-die, ages, upgrade kills — a
/// discipline for transactions that opted into locking as their isolation.
/// Row locks answer a different question for every isolation level: "may this
/// session's transaction proceed with THIS row while another holds it". A
/// READ COMMITTED claim query is the primary customer, so the row table has
/// its own, simpler protocol:
///
/// - **Plain `FOR UPDATE`** blocks until the holder's transaction ends, with
///   the same `lock_timeout` bound (default 10s) and the same `55P03
///   lock_not_available` failure the table manager reports — a timeout is not
///   a conflict the client can win by retrying.
/// - **`SKIP LOCKED` / `NOWAIT`** never wait, so they can never deadlock and
///   never time out; one reports the row as skipped, the other as `55P03`.
///
/// # Deadlock posture
///
/// A single statement takes all of its row locks in sorted `(table, key)`
/// order (the executor sorts before acquiring), so two statements — the claim
/// shape — cannot wait on each other: both climb the same total order. Two
/// *statements of one transaction* can still cross (statement one holds row A,
/// the other transaction's statement holds row B, statement two wants B), and
/// no ordering rule inside one statement can prevent that. The honest answer
/// is the bound: plain `FOR UPDATE` gives up after `lock_timeout` with 55P03,
/// exactly the escape hatch PostgreSQL exposes. `SKIP LOCKED` — the clause
/// queues actually use — is immune by construction because it does not wait.
///
/// # Lifecycle
///
/// Locks are held by session id and released at COMMIT, ROLLBACK, the end of
/// an autocommit statement, and session teardown — the same points that
/// release the unique-key gate. An abandoned session must not park rows
/// forever.
pub struct RowLockManager {
    held: Mutex<RowHeld>,
    /// Woken on every release so waiters re-check.
    released: Notify,
    timeout_ms: AtomicU64,
    /// Per-session row-lock count limit. A transaction may hold at most this
    /// many rows locked at once; an acquisition past the limit is refused with
    /// the `too_many_row_locks` error (SQLSTATE 53200 via the wire codec).
    ///
    /// Locks are only released at transaction end, so without a bound one
    /// session in one long transaction can grow the lock table without limit
    /// — each entry is a full key tuple, not a pointer. PostgreSQL bounds its
    /// lock table the same way (`max_locks_per_transaction`, exhausted →
    /// 53200 "out of shared memory").
    max_locks_per_session: AtomicUsize,
}

/// Default per-session row-lock limit. Generous against real claim workloads
/// (queue claims lock in the tens to hundreds), while 100k key tuples is a
/// few MB — a real ceiling on the per-session footprint, not a paper one.
pub const DEFAULT_MAX_ROW_LOCKS_PER_SESSION: usize = 100_000;

#[derive(Default)]
struct RowHeld {
    /// Row key → the session holding it.
    owner: HashMap<RowLockKey, u64>,
    /// Session → the keys it holds, for release.
    by_session: HashMap<u64, Vec<RowLockKey>>,
}

impl Default for RowLockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            held: Mutex::new(RowHeld::default()),
            released: Notify::new(),
            timeout_ms: AtomicU64::new(DEFAULT_LOCK_TIMEOUT_MS),
            max_locks_per_session: AtomicUsize::new(DEFAULT_MAX_ROW_LOCKS_PER_SESSION),
        }
    }

    /// Set the per-session row-lock limit. Applied to acquisitions after this
    /// call; already-held locks are untouched.
    pub fn set_max_locks_per_session(&self, n: usize) {
        self.max_locks_per_session.store(n, Ordering::Relaxed);
    }

    /// The error reported when a session's lock count is at the limit. The
    /// `too_many_row_locks` prefix is what the wire error codec keys SQLSTATE
    /// 53200 (out_of_memory) on — PostgreSQL's class for lock-table
    /// exhaustion, and not a retryable conflict.
    fn too_many_row_locks(session: u64, held: usize, limit: usize) -> StorageError {
        StorageError::Io(format!(
            "too_many_row_locks: session {session} already holds {held} row locks \
             (limit {limit}); the transaction must commit or roll back before \
             locking more rows (raise limits.max_row_locks_per_session if this \
             is a legitimate workload)"
        ))
    }

    /// Set the plain-`FOR UPDATE` wait bound. 0 disables it (wait forever).
    pub fn set_timeout_ms(&self, ms: u64) {
        self.timeout_ms.store(ms, Ordering::Relaxed);
    }

    /// The error a plain `FOR UPDATE` waiter reports on timeout, and a
    /// `NOWAIT` acquisition reports immediately. The `lock_not_available`
    /// prefix is what the wire error codec keys SQLSTATE 55P03 on.
    fn lock_not_available(key: &RowLockKey, waited_ms: u64) -> StorageError {
        StorageError::Io(format!(
            "lock_not_available: could not lock row in table '{}' (key {:?}) \
             after {waited_ms}ms: another transaction holds it (NOWAIT was \
             requested, or lock_timeout was reached)",
            key.0, key.1
        ))
    }

    /// Attempt one row lock without waiting. Re-entrant: a key this session
    /// already holds is `Acquired` again — a transaction re-reading its own
    /// locked rows must not skip or fail them. A NEW key past the session's
    /// lock limit is an error, not a skip: `SKIP LOCKED` semantics must never
    /// silently swallow a resource refusal.
    pub fn try_lock(&self, session: u64, key: &RowLockKey) -> Result<RowTry, StorageError> {
        let limit = self.max_locks_per_session.load(Ordering::Relaxed);
        let mut held = self.held.lock();
        match held.owner.get(key) {
            None => {
                let already = held.by_session.get(&session).map_or(0, Vec::len);
                if already >= limit {
                    return Err(Self::too_many_row_locks(session, already, limit));
                }
                held.owner.insert(key.clone(), session);
                held.by_session
                    .entry(session)
                    .or_default()
                    .push(key.clone());
                Ok(RowTry::Acquired)
            }
            Some(&owner) if owner == session => Ok(RowTry::Acquired),
            Some(_) => Ok(RowTry::HeldElsewhere),
        }
    }

    /// Acquire one row lock for plain `FOR UPDATE`, waiting until the holder's
    /// transaction ends. Bounded by the timeout; the failure is 55P03, not a
    /// serialization failure, for the same reason the table manager's is: a
    /// held row is not a conflict a retry can win.
    pub async fn lock(&self, session: u64, key: &RowLockKey) -> Result<(), StorageError> {
        if self.try_lock(session, key)? == RowTry::Acquired {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let budget = self.timeout_ms.load(Ordering::Relaxed);
        loop {
            // Register interest BEFORE the next try, so a release landing
            // between the failed try and the await cannot be missed.
            let woken = self.released.notified();
            if self.try_lock(session, key)? == RowTry::Acquired {
                return Ok(());
            }
            if budget == 0 {
                woken.await;
                continue;
            }
            let elapsed = started.elapsed().as_millis() as u64;
            if elapsed >= budget {
                return Err(Self::lock_not_available(key, elapsed));
            }
            let remaining = std::time::Duration::from_millis(budget - elapsed);
            // A timeout here is not itself the answer — the lock may have been
            // released in the same instant — so fall through to one more
            // try_lock and let the elapsed check decide.
            let _ = tokio::time::timeout(remaining, woken).await;
        }
    }

    /// Release every row lock `session` holds. Called at COMMIT, ROLLBACK,
    /// autocommit statement end and session teardown. Idempotent.
    pub fn release_session(&self, session: u64) {
        let keys = {
            let mut held = self.held.lock();
            match held.by_session.remove(&session) {
                Some(keys) => {
                    for k in &keys {
                        if held.owner.get(k) == Some(&session) {
                            held.owner.remove(k);
                        }
                    }
                    keys
                }
                None => return,
            }
        };
        if !keys.is_empty() {
            self.released.notify_waiters();
        }
    }

    /// Rows currently locked, across all sessions. Test/observability.
    pub fn held_count(&self) -> usize {
        self.held.lock().owner.len()
    }

    /// Row locks one session currently holds. Test/observability.
    pub fn session_held_count(&self, session: u64) -> usize {
        self.held
            .lock()
            .by_session
            .get(&session)
            .map_or(0, Vec::len)
    }

    /// Whether `session` currently holds any row lock. Test/observability.
    #[cfg(test)]
    pub fn holds_any(&self, session: u64) -> bool {
        self.held
            .lock()
            .by_session
            .get(&session)
            .is_some_and(|v| !v.is_empty())
    }
}

#[cfg(test)]
mod row_lock_tests {
    use super::*;
    use crate::types::Value;

    fn key(id: i64) -> RowLockKey {
        ("jobs".to_string(), vec![Value::Int64(id)])
    }

    #[tokio::test]
    async fn a_free_row_locks_immediately() {
        let lm = RowLockManager::new();
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        assert!(lm.holds_any(1));
        assert_eq!(lm.held_count(), 1);
    }

    #[tokio::test]
    async fn another_session_sees_the_row_held() {
        let lm = RowLockManager::new();
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        assert_eq!(lm.try_lock(2, &key(7)).unwrap(), RowTry::HeldElsewhere);
    }

    #[tokio::test]
    async fn the_holder_reacquires_its_own_row() {
        let lm = RowLockManager::new();
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        assert_eq!(
            lm.try_lock(1, &key(7)).unwrap(),
            RowTry::Acquired,
            "re-locking own rows must be a no-op, not a skip"
        );
        lm.release_session(1);
        assert_eq!(
            lm.held_count(),
            0,
            "the double-take must not leak a second entry"
        );
    }

    #[tokio::test]
    async fn different_keys_do_not_conflict() {
        let lm = RowLockManager::new();
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        assert_eq!(lm.try_lock(2, &key(8)).unwrap(), RowTry::Acquired);
    }

    /// Integer widths are one row: `Int32(1)` and `Int64(1)` are the same
    /// primary key, and the lock identity must agree with that or two claims
    /// of "the same" row would serialize against nothing.
    #[tokio::test]
    async fn integer_widths_are_the_same_row() {
        let lm = RowLockManager::new();
        assert_eq!(
            lm.try_lock(1, &("t".to_string(), vec![Value::Int32(7)])).unwrap(),
            RowTry::Acquired
        );
        assert_eq!(
            lm.try_lock(2, &("t".to_string(), vec![Value::Int64(7)])).unwrap(),
            RowTry::HeldElsewhere,
            "Int32(7) and Int64(7) are the same primary key"
        );
    }

    /// Tables are part of the identity: row 7 of `a` and row 7 of `b` are
    /// different rows.
    #[tokio::test]
    async fn same_key_in_different_tables_is_a_different_row() {
        let lm = RowLockManager::new();
        assert_eq!(
            lm.try_lock(1, &("a".to_string(), vec![Value::Int64(7)])).unwrap(),
            RowTry::Acquired
        );
        assert_eq!(
            lm.try_lock(2, &("b".to_string(), vec![Value::Int64(7)])).unwrap(),
            RowTry::Acquired
        );
    }

    #[tokio::test]
    async fn release_session_frees_the_rows() {
        let lm = RowLockManager::new();
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        assert_eq!(lm.try_lock(1, &key(8)).unwrap(), RowTry::Acquired);
        lm.release_session(1);
        assert_eq!(lm.held_count(), 0);
        assert!(!lm.holds_any(1));
        // And the rows are lockable again — no stale entry survives release.
        assert_eq!(lm.try_lock(2, &key(7)).unwrap(), RowTry::Acquired);
    }

    #[tokio::test]
    async fn plain_lock_blocks_then_proceeds_after_release() {
        let lm = Arc::new(RowLockManager::new());
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        let lm2 = lm.clone();
        let k = key(7);
        let waiter = tokio::spawn(async move { lm2.lock(2, &k).await });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the waiter must be blocked, not failed"
        );
        lm.release_session(1);
        waiter
            .await
            .unwrap()
            .expect("a released row must be grantable");
    }

    /// A waiter must not miss a release that lands between its failed try and
    /// its await — the notified-before-try ordering.
    #[tokio::test]
    async fn a_release_during_the_wait_wakes_the_waiter() {
        let lm = Arc::new(RowLockManager::new());
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        let lm2 = lm.clone();
        let k = key(7);
        let waiter = tokio::spawn(async move { lm2.lock(2, &k).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        lm.release_session(1);
        waiter
            .await
            .unwrap()
            .expect("release during wait must grant, not time out");
    }

    #[tokio::test]
    async fn a_plain_wait_gives_up_after_the_timeout() {
        let lm = RowLockManager::new();
        lm.set_timeout_ms(60);
        assert_eq!(lm.try_lock(1, &key(7)).unwrap(), RowTry::Acquired);
        let started = std::time::Instant::now();
        let err = lm
            .lock(2, &key(7))
            .await
            .expect_err("a held row must time out, not hang");
        let waited = started.elapsed();
        assert!(
            err.to_string().contains("lock_not_available"),
            "the failure must be the 55P03 wording, got: {err}"
        );
        assert!(waited >= std::time::Duration::from_millis(50));
        assert!(waited < std::time::Duration::from_secs(5));
    }

    // ── per-session lock-count limit ──────────────────────────────────────

    /// The limit is on DISTINCT keys: re-acquiring rows the session already
    /// holds is free, or a claim query re-reading its own locked rows would
    /// burn budget on every pass.
    #[tokio::test]
    async fn re_locking_owned_rows_does_not_consume_budget() {
        let lm = RowLockManager::new();
        lm.set_max_locks_per_session(2);
        assert_eq!(lm.try_lock(1, &key(1)).unwrap(), RowTry::Acquired);
        assert_eq!(lm.try_lock(1, &key(1)).unwrap(), RowTry::Acquired);
        assert_eq!(lm.try_lock(1, &key(2)).unwrap(), RowTry::Acquired);
        assert_eq!(
            lm.session_held_count(1),
            2,
            "re-locks must not add entries"
        );
    }

    /// A session at its limit is REFUSED, not skipped: `SKIP LOCKED` treats an
    /// entry as claimable-by-no-one when it returns HeldElsewhere, so a
    /// resource refusal surfacing as a skip would silently return fewer rows
    /// with no error anywhere. Control: another session under its own limit
    /// locks freely.
    #[tokio::test]
    async fn an_acquisition_past_the_limit_is_refused_not_skipped() {
        let lm = RowLockManager::new();
        lm.set_max_locks_per_session(3);
        for i in 1..=3 {
            assert_eq!(lm.try_lock(1, &key(i)).unwrap(), RowTry::Acquired);
        }
        let err = lm
            .try_lock(1, &key(4))
            .expect_err("the 4th distinct row must be refused");
        assert!(
            err.to_string().contains("too_many_row_locks"),
            "got: {err}"
        );
        // Control: session 2 has its own budget.
        assert_eq!(lm.try_lock(2, &key(4)).unwrap(), RowTry::Acquired);
        // And the refused lock took no entry.
        assert_eq!(lm.held_count(), 4);
        assert_eq!(lm.session_held_count(1), 3);
    }

    /// The plain (blocking) acquisition path enforces the same limit — the
    /// cap must not be bypassable by choosing the waiting mode.
    #[tokio::test]
    async fn the_plain_lock_path_enforces_the_limit() {
        let lm = RowLockManager::new();
        lm.set_max_locks_per_session(1);
        assert_eq!(lm.try_lock(1, &key(1)).unwrap(), RowTry::Acquired);
        let err = lm
            .lock(1, &key(2))
            .await
            .expect_err("a second distinct row must be refused");
        assert!(
            err.to_string().contains("too_many_row_locks"),
            "got: {err}"
        );
    }

    /// Release makes budget available again — the limit tracks held locks,
    /// not acquisitions over the session's lifetime.
    #[tokio::test]
    async fn release_restores_budget() {
        let lm = RowLockManager::new();
        lm.set_max_locks_per_session(1);
        assert_eq!(lm.try_lock(1, &key(1)).unwrap(), RowTry::Acquired);
        assert!(lm.try_lock(1, &key(2)).is_err());
        lm.release_session(1);
        assert_eq!(lm.try_lock(1, &key(2)).unwrap(), RowTry::Acquired);
    }
}

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
        assert!(
            r.is_err(),
            "younger reader must die against an older writer"
        );
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
        assert!(
            waiter.await.unwrap().is_ok(),
            "older waiter must be granted"
        );
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
        assert_eq!(
            lm.locked_tables(),
            0,
            "released locks must not leak entries"
        );
        assert!(!lm.holds_any(1));
    }

    #[tokio::test]
    async fn a_wait_reports_that_it_waited() {
        let lm = Arc::new(LockManager::new());
        lm.acquire(2, "other", LockMode::Shared).await.unwrap();
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
        let lm2 = lm.clone();
        let waiter = tokio::spawn(async move { lm2.acquire(2, "t", LockMode::Shared).await });
        tokio::task::yield_now().await;
        lm.release_all(1);
        // The outcome distinguishes "granted immediately" from "blocked", which
        // is the whole point of reporting it: a lock table with no waits and one
        // saturated with them look identical without this.
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            AcquireOutcome::Waited(_)
        ));
    }

    #[tokio::test]
    async fn an_immediate_grant_reports_immediate() {
        let lm = LockManager::new();
        assert_eq!(
            lm.acquire(1, "t", LockMode::Exclusive).await.unwrap(),
            AcquireOutcome::Immediate
        );
    }

    /// Wait-die rules out deadlock but NOT a long wait: the holder may simply
    /// be slow, or idle-in-transaction because a client walked away. Without a
    /// bound, one such transaction parks every older one behind it forever,
    /// which is indistinguishable from a hang.
    #[tokio::test]
    async fn a_wait_gives_up_after_lock_timeout() {
        let lm = Arc::new(LockManager::new());
        lm.set_timeout_ms(60);
        // txn 2 is OLDER (locks first), so it waits rather than dying...
        lm.acquire(2, "other", LockMode::Shared).await.unwrap();
        // ...behind txn 1, which holds and never releases.
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();

        let started = std::time::Instant::now();
        let r = lm.acquire(2, "t", LockMode::Shared).await;
        let waited = started.elapsed();

        let err = r.expect_err("the wait should have timed out");
        assert!(
            err.to_string().contains("lock_not_available"),
            "a timeout must be distinguishable from a deadlock kill: {err}"
        );
        assert!(
            waited >= std::time::Duration::from_millis(50),
            "gave up after only {waited:?} — it should have waited out the budget"
        );
        assert!(
            waited < std::time::Duration::from_secs(5),
            "waited {waited:?}, far past the 60ms budget"
        );
    }

    /// A timeout of 0 means the original behaviour: wait indefinitely.
    #[tokio::test]
    async fn a_zero_timeout_waits_indefinitely() {
        let lm = Arc::new(LockManager::new());
        lm.set_timeout_ms(0);
        lm.acquire(2, "other", LockMode::Shared).await.unwrap();
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
        let lm2 = lm.clone();
        let waiter = tokio::spawn(async move { lm2.acquire(2, "t", LockMode::Shared).await });
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            !waiter.is_finished(),
            "a 0 timeout must not give up — it disables the bound"
        );
        lm.release_all(1);
        assert!(waiter.await.unwrap().is_ok());
    }

    /// The waiter must be granted the lock if it is released in the same
    /// instant the budget expires, rather than reporting a spurious timeout.
    #[tokio::test]
    async fn a_release_just_before_the_deadline_still_grants() {
        let lm = Arc::new(LockManager::new());
        lm.set_timeout_ms(400);
        lm.acquire(2, "other", LockMode::Shared).await.unwrap();
        lm.acquire(1, "t", LockMode::Exclusive).await.unwrap();
        let lm2 = lm.clone();
        let waiter = tokio::spawn(async move { lm2.acquire(2, "t", LockMode::Shared).await });
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        lm.release_all(1);
        assert!(
            waiter.await.unwrap().is_ok(),
            "a lock released well inside the budget must be granted"
        );
    }

    #[tokio::test]
    async fn locked_table_count_tracks_live_locks() {
        let lm = LockManager::new();
        assert_eq!(lm.locked_table_count(), 0);
        lm.acquire(1, "a", LockMode::Shared).await.unwrap();
        lm.acquire(1, "b", LockMode::Exclusive).await.unwrap();
        assert_eq!(lm.locked_table_count(), 2);
        lm.release_all(1);
        assert_eq!(lm.locked_table_count(), 0);
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
