//! Serializes the check-then-write of UNIQUE / PRIMARY KEY enforcement.
//!
//! `check_unique_constraints` reads a snapshot and the write happens afterwards.
//! Between the two, another session can insert the same key, and on the engines
//! the server actually runs — `BufferedDiskEngine::new(DiskEngine)` — nothing
//! stops it: `StorageEngine::insert_unique` carries a trait default that is a
//! plain `insert()`, overridden only by `MvccStorageAdapter`, and below
//! SERIALIZABLE no table lock is taken either. Measured on the paged pair before
//! this existed: four concurrent sessions inserting the same primary key landed
//! duplicates in 14 of 20 rounds, and one round kept all four rows.
//!
//! A reservation map alone does not close it. If A reserves, inserts, commits
//! and releases inside the window between B's snapshot read and B's own
//! reservation, B reserves cleanly and writes a duplicate — B's *check* was the
//! stale part, so the check has to happen inside the critical section, not
//! before it.
//!
//! So this gate is held across check *and* write: for the statement when the
//! session is in autocommit, and to end-of-transaction otherwise, which is when
//! the row becomes visible to anyone else's check. That is also PostgreSQL's
//! observable behaviour — a second inserter of the same key blocks until the
//! first transaction ends and then reports the duplicate.
//!
//! **The gate slot is a hash, deliberately.** Two logically equal keys must map
//! to the same slot or the gate does nothing; `Value`'s `Hash` is hand-written
//! to be integer-width-canonical (`as_canonical_int`), so `Int32(1)` and
//! `Int64(1)` collide by construction — which is the required direction. A
//! `Debug`- or `Ord`-derived slot would not: `Debug` is derived and prints the
//! variant, and `Ord` has no arm for JSONB/ARRAY/VECTOR (they all compare
//! Equal). An accidental collision between two genuinely different keys costs a
//! little needless serialization and cannot cause a missed duplicate, because
//! the gate does not decide uniqueness — `check_unique_constraints` does.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::types::Value;

/// How long a session may wait for a key before giving up, in milliseconds.
///
/// Matches `LockManager`'s default. Sorted acquisition makes a deadlock
/// impossible within one statement, but a multi-statement transaction can still
/// take keys in an order that crosses another's, so the wait is bounded rather
/// than unbounded — the same escape hatch PostgreSQL exposes as `lock_timeout`.
const DEFAULT_GATE_TIMEOUT_MS: u64 = 10_000;

/// One UNIQUE/PK slot: table, which constraint, and the hashed key.
pub(crate) type GateKey = (String, usize, u64);

#[derive(Default)]
struct Held {
    /// Slot → the session holding it.
    owner: HashMap<GateKey, u64>,
    /// Session → the slots it holds, for release.
    by_session: HashMap<u64, Vec<GateKey>>,
}

pub(crate) struct UniqueGate {
    held: Mutex<Held>,
    /// Woken on every release so waiters re-check.
    released: Notify,
    timeout_ms: AtomicU64,
}

impl Default for UniqueGate {
    fn default() -> Self {
        Self::new()
    }
}

impl UniqueGate {
    pub(crate) fn new() -> Self {
        Self {
            held: Mutex::new(Held::default()),
            released: Notify::new(),
            timeout_ms: AtomicU64::new(DEFAULT_GATE_TIMEOUT_MS),
        }
    }

    /// Slot identity for one constraint's key values. Returns `None` when any
    /// column is NULL — SQL says NULLs never conflict, so there is nothing to
    /// serialize against.
    pub(crate) fn slot(table: &str, constraint_idx: usize, key: &[Value]) -> Option<GateKey> {
        if key.iter().any(|v| matches!(v, Value::Null)) {
            return None;
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        Some((table.to_string(), constraint_idx, h.finish()))
    }

    /// Take every slot in `keys` for `session`, waiting for any another session
    /// holds. Re-entrant: a slot this session already holds is already taken.
    ///
    /// Returns the slots this call newly took, so a caller can release exactly
    /// those and nothing else — statement execution nests (a stored function
    /// body, a CTAS, a view expansion all run statements inside a statement),
    /// and an inner INSERT releasing the whole session would unlock the outer
    /// one's keys while it is still mid-write.
    ///
    /// Slots are sorted before acquisition, so two statements taking the same
    /// set take it in the same order and cannot deadlock against each other.
    /// Across statements the order is not controllable — a multi-row INSERT
    /// discovers its keys row by row — so the wait is bounded and reports which
    /// table it gave up on, rather than hanging.
    pub(crate) async fn acquire(
        &self,
        session: u64,
        keys: &[GateKey],
    ) -> Result<Vec<GateKey>, crate::storage::StorageError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut sorted: Vec<GateKey> = keys.to_vec();
        sorted.sort();
        sorted.dedup();

        let budget = self.timeout_ms.load(Ordering::Relaxed);
        let mut taken: Vec<GateKey> = Vec::new();
        for key in sorted {
            let started = std::time::Instant::now();
            loop {
                // Register interest BEFORE trying, so a release landing between
                // the failed try and the await is not missed.
                let woken = self.released.notified();
                {
                    let mut held = self.held.lock();
                    match held.owner.get(&key) {
                        None => {
                            held.owner.insert(key.clone(), session);
                            held.by_session
                                .entry(session)
                                .or_default()
                                .push(key.clone());
                            taken.push(key);
                            break;
                        }
                        // Already ours. A second INSERT of the same key inside
                        // one transaction is a real duplicate, but that is the
                        // constraint check's call to make, not the gate's —
                        // blocking here would deadlock the session against
                        // itself.
                        Some(&owner) if owner == session => break,
                        Some(_) => {}
                    }
                }
                if budget == 0 {
                    woken.await;
                    continue;
                }
                let elapsed = started.elapsed().as_millis() as u64;
                if elapsed >= budget {
                    // Give back what this call took before failing: an
                    // abandoned partial acquisition would park every other
                    // session on keys nobody is using.
                    self.release(session, &taken);
                    return Err(crate::storage::StorageError::Io(format!(
                        "timed out after {elapsed}ms waiting for another transaction to release \
                         a conflicting unique key on '{}'",
                        key.0
                    )));
                }
                let remaining = std::time::Duration::from_millis(budget - elapsed);
                let _ = tokio::time::timeout(remaining, woken).await;
            }
        }
        Ok(taken)
    }

    /// Release exactly `keys`, and only those this `session` still owns.
    pub(crate) fn release(&self, session: u64, keys: &[GateKey]) {
        if keys.is_empty() {
            return;
        }
        {
            let mut held = self.held.lock();
            for k in keys {
                if held.owner.get(k) == Some(&session) {
                    held.owner.remove(k);
                }
            }
            if let Some(list) = held.by_session.get_mut(&session) {
                list.retain(|k| !keys.contains(k));
                if list.is_empty() {
                    held.by_session.remove(&session);
                }
            }
        }
        self.released.notify_waiters();
    }

    /// Release every slot `session` holds. Called at the end of an autocommit
    /// statement and at COMMIT / ROLLBACK / session teardown. Idempotent.
    pub(crate) fn release_session(&self, session: u64) {
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

    /// Slots currently held, across all sessions. Test/metric use.
    #[cfg(test)]
    pub(crate) fn held_count(&self) -> usize {
        self.held.lock().owner.len()
    }
}

/// Holds the slots one statement took and gives them back when it ends,
/// however it ends — the check-then-write it protects has a dozen early
/// returns and every one of them must not strand a key.
///
/// [`keep_for_transaction`](Self::keep_for_transaction) disarms it: inside an
/// explicit transaction the row is not visible to anyone else's constraint
/// check until COMMIT, so the slot has to outlive the statement. Those are
/// released by [`UniqueGate::release_session`] at COMMIT, ROLLBACK, or session
/// teardown.
pub(crate) struct GateGuard<'a> {
    gate: &'a UniqueGate,
    session: u64,
    keys: Vec<GateKey>,
}

impl<'a> GateGuard<'a> {
    pub(crate) fn new(gate: &'a UniqueGate, session: u64) -> Self {
        Self {
            gate,
            session,
            keys: Vec::new(),
        }
    }

    /// Take `keys` and record them for release.
    pub(crate) async fn take(
        &mut self,
        keys: &[GateKey],
    ) -> Result<(), crate::storage::StorageError> {
        let newly = self.gate.acquire(self.session, keys).await?;
        self.keys.extend(newly);
        Ok(())
    }

    /// Hand the slots to the transaction; they are no longer this statement's
    /// to release.
    pub(crate) fn keep_for_transaction(&mut self) {
        self.keys.clear();
    }
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        if !self.keys.is_empty() {
            self.gate.release(self.session, &self.keys);
        }
    }
}

/// The session the current task is running as. `0` is "no explicit session"
/// (embedded, or a test that never created one), and every such caller shares
/// one owner — correct, because without sessions there is nothing to isolate.
pub(crate) fn gate_session_id() -> u64 {
    #[cfg(feature = "server")]
    {
        crate::storage::STORAGE_SESSION_ID
            .try_with(|&id| id)
            .unwrap_or(0)
    }
    #[cfg(not(feature = "server"))]
    {
        crate::storage::get_storage_session_id()
    }
}

impl super::Executor {
    /// Every UNIQUE / PRIMARY KEY slot `row` would occupy in `table`.
    ///
    /// Mirrors the constraint enumeration in `check_unique_constraints`,
    /// including its two opt-outs, because a slot the check does not enforce is
    /// pure contention and a constraint the check enforces without a slot is
    /// the race this exists to close.
    pub(super) fn unique_slots_for_row(
        &self,
        table_name: &str,
        table_def: &crate::catalog::TableDef,
        row: &[Value],
    ) -> Vec<GateKey> {
        use crate::catalog::TableConstraint;

        // ReplacingMergeTree keeps multiple versions per key on purpose.
        if crate::columnar::replacing_config(table_name).is_some() {
            return Vec::new();
        }
        if crate::bench_hooks::skip_unique_probe() {
            return Vec::new();
        }

        let mut slots = Vec::new();
        for (cid, constraint) in table_def.constraints.iter().enumerate() {
            let columns = match constraint {
                TableConstraint::PrimaryKey { columns, .. }
                | TableConstraint::Unique { columns, .. } => columns,
                _ => continue,
            };
            let indices: Vec<usize> = columns
                .iter()
                .filter_map(|c| table_def.column_index(c))
                .collect();
            if indices.len() != columns.len() {
                continue;
            }
            let key: Vec<Value> = indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            if let Some(slot) = UniqueGate::slot(table_name, cid, &key) {
                slots.push(slot);
            }
        }
        slots
    }

    /// Whether this session is inside an explicit transaction, and so must hold
    /// its unique slots past the end of the statement.
    pub(super) async fn in_explicit_txn(&self) -> bool {
        self.current_session().txn_state.read().await.active
    }

    /// Release every unique slot this session holds. Called at COMMIT,
    /// ROLLBACK and session teardown — the points at which a transaction's
    /// rows either become visible to everyone else's constraint check or cease
    /// to exist.
    pub(super) fn release_unique_slots(&self, session: u64) {
        self.unique_gate.release_session(session);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_columns_take_no_slot() {
        assert!(UniqueGate::slot("t", 0, &[Value::Null]).is_none());
        assert!(UniqueGate::slot("t", 0, &[Value::Int32(1), Value::Null]).is_none());
        assert!(UniqueGate::slot("t", 0, &[Value::Int32(1)]).is_some());
    }

    /// The property the gate depends on: equal logical keys share a slot even
    /// when they arrive as different integer widths. If this ever fails, two
    /// sessions inserting the "same" key would serialize against nothing.
    #[test]
    fn integer_widths_share_a_slot() {
        let a = UniqueGate::slot("t", 0, &[Value::Int32(7)]).unwrap();
        let b = UniqueGate::slot("t", 0, &[Value::Int64(7)]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn different_tables_and_constraints_do_not_share_a_slot() {
        let a = UniqueGate::slot("t", 0, &[Value::Int32(7)]).unwrap();
        assert_ne!(a, UniqueGate::slot("u", 0, &[Value::Int32(7)]).unwrap());
        assert_ne!(a, UniqueGate::slot("t", 1, &[Value::Int32(7)]).unwrap());
    }

    #[tokio::test]
    async fn second_session_waits_then_proceeds_after_release() {
        let gate = std::sync::Arc::new(UniqueGate::new());
        let key = UniqueGate::slot("t", 0, &[Value::Int32(1)]).unwrap();
        gate.acquire(1, std::slice::from_ref(&key)).await.unwrap();
        assert_eq!(gate.held_count(), 1);

        let g2 = gate.clone();
        let k2 = key.clone();
        let waiter = tokio::spawn(async move { g2.acquire(2, &[k2]).await });
        // The waiter must not be able to take it while session 1 holds it.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "session 2 took a held slot");

        gate.release_session(1);
        waiter.await.unwrap().unwrap();
        assert_eq!(gate.held_count(), 1, "session 2 should now hold it");
        gate.release_session(2);
        assert_eq!(gate.held_count(), 0);
    }

    #[tokio::test]
    async fn reacquiring_own_slot_does_not_deadlock() {
        let gate = UniqueGate::new();
        let key = UniqueGate::slot("t", 0, &[Value::Int32(1)]).unwrap();
        gate.acquire(1, std::slice::from_ref(&key)).await.unwrap();
        gate.acquire(1, std::slice::from_ref(&key)).await.unwrap();
        gate.release_session(1);
        assert_eq!(gate.held_count(), 0);
    }

    #[tokio::test]
    async fn waiting_is_bounded() {
        let gate = UniqueGate::new();
        gate.timeout_ms.store(80, Ordering::Relaxed);
        let key = UniqueGate::slot("t", 0, &[Value::Int32(1)]).unwrap();
        gate.acquire(1, std::slice::from_ref(&key)).await.unwrap();
        let err = gate.acquire(2, &[key]).await.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "expected a bounded wait, got {err}"
        );
    }
}
