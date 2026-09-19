//! Cross-table snapshot lease (Consumer-2 / teploy-observe F45).
//!
//! A point-in-time backup boundary: `ACQUIRE SNAPSHOT LEASE` pins one
//! database-wide mutation window. While the lease is held:
//!
//! - the holder's transaction keeps a read snapshot pinned to the ACQUIRE
//!   moment (stable across statements), so its reads are one logical
//!   moment across every table;
//! - every OTHER session's SQL mutations (DML + table-shape DDL +
//!   SELECT-carried specialty writes such as `SELECT kv_set(...)`) WAIT at
//!   the dispatch gate until the lease is released or expires — no commit
//!   can advance the database past the holder's moment while it reads;
//! - the holder's own mutations are refused (the lease view is read-only;
//!   letting the holder write would both dirty the "moment" and deadlock
//!   the writer gate against itself).
//!
//! Acquisition DRAINS first: `ACQUIRE` waits (bounded by the lease's own
//! TIMEOUT) for every other session's open write-bearing transaction to
//! end. A transaction that already executed its writes when the lease is
//! requested is invisible to the writer gate — its COMMIT would land
//! mid-window and, on engines without versioning (the disk stack, and
//! per-table override engines whose writes are immediate), its uncommitted
//! rows are visible to readers outright. Draining resolves both before the
//! window opens: an idle transaction that has written nothing does not
//! block acquisition. On versioning engines the holder's snapshot is then
//! re-taken at the acquire instant (`refresh_txn_snapshot`), so the pinned
//! moment is ACQUIRE, not BEGIN.
//!
//! Because no writer can commit during the window, even plain autocommit
//! reads from unrelated sessions observe the same frozen state — a dump
//! tool does not need to know anything about transactions to get a
//! consistent copy.
//!
//! Release happens on `RELEASE SNAPSHOT LEASE`, at COMMIT/ROLLBACK of the
//! holding transaction, when the holding session disconnects, or at
//! timeout expiry (lazy: every waiter and every new acquisition re-checks
//! the deadline; blocked writers wake at the deadline at the latest).
//!
//! Scope, stated honestly: the gate covers SQL statements through the
//! executor's central dispatch, the KV wire fast path, and the
//! SELECT-carried specialty functions the dispatch can see in the AST.
//! Specialty-model writes that bypass all of those (RESP-wire direct,
//! streams/CDC appends from background tasks) are not lease-gated, and a
//! mutation that is already mid-statement at the acquire instant races the
//! drain the same way it races the DML gate — the lease cannot retroactively
//! stop work that began before it existed.

use std::time::Duration;

/// Default lease timeout when `TIMEOUT` is not given: 30 seconds. Long
/// enough for a small dump, short enough that a crashed holder cannot
/// wedge writers for a meaningful multiple of it.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// One held lease.
///
/// Deadlines use tokio's clock so a paused-time test (start_paused) sees
/// the same expiry the runtime does.
#[derive(Debug, Clone, Copy)]
struct Lease {
    session_id: u64,
    deadline: tokio::time::Instant,
}

/// Why an acquisition was refused.
#[derive(Debug)]
pub enum LeaseRefusal {
    /// Another session holds the lease; the value is the remaining time.
    HeldByOther(u64),
}

impl std::fmt::Display for LeaseRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseRefusal::HeldByOther(remaining_ms) => write!(
                f,
                "a snapshot lease is already held by another session ({remaining_ms} ms remaining)"
            ),
        }
    }
}

/// Process-wide lease registry. One per executor; the executor is shared
/// across every connection, so the registry is shared with it.
pub(crate) struct SnapshotLeaseRegistry {
    state: parking_lot::Mutex<Option<Lease>>,
    /// Woken on every release/expiry so blocked writers re-check promptly.
    changed: tokio::sync::Notify,
}

impl Default for SnapshotLeaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotLeaseRegistry {
    pub(crate) fn new() -> Self {
        Self {
            state: parking_lot::Mutex::new(None),
            changed: tokio::sync::Notify::new(),
        }
    }

    /// The active lease for `session_id`'s acquisition attempt. A lease
    /// held by the SAME session is renew-or-idempotent (the executor layer
    /// refuses double-acquire before reaching here, but the registry stays
    /// total on its own).
    fn active(&self, now: tokio::time::Instant) -> Option<Lease> {
        let mut guard = self.state.lock();
        match *guard {
            Some(lease) if lease.deadline > now => Some(lease),
            Some(_) => {
                // Lazily expire: a crashed or stalled holder cannot wedge
                // the database past its own timeout.
                *guard = None;
                None
            }
            None => None,
        }
    }

    /// Try to take the lease for `session_id`.
    pub(crate) fn try_acquire(
        &self,
        session_id: u64,
        timeout_ms: u64,
    ) -> Result<(), LeaseRefusal> {
        let now = tokio::time::Instant::now();
        let mut guard = self.state.lock();
        if let Some(existing) = *guard
            && existing.deadline > now && existing.session_id != session_id {
                return Err(LeaseRefusal::HeldByOther(
                    existing.deadline.duration_since(now).as_millis() as u64,
                ));
            }
            // Same session re-acquire, or an expired lease: replace.
        *guard = Some(Lease {
            session_id,
            deadline: now + Duration::from_millis(timeout_ms),
        });
        Ok(())
    }

    /// Release the lease if `session_id` holds it. Returns whether a lease
    /// this session held was released.
    pub(crate) fn release(&self, session_id: u64) -> bool {
        let released = {
            let mut guard = self.state.lock();
            match *guard {
                Some(lease) if lease.session_id == session_id => {
                    *guard = None;
                    true
                }
                _ => false,
            }
        };
        if released {
            self.changed.notify_waiters();
        }
        released
    }

    /// The holder's session id and remaining milliseconds, for SHOW.
    pub(crate) fn holder(&self) -> Option<(u64, u64)> {
        let now = tokio::time::Instant::now();
        self.active(now)
            .map(|l| (l.session_id, l.deadline.duration_since(now).as_millis() as u64))
    }

    /// The writer gate. For a session that is not the holder: wait until
    /// no unexpired lease exists. The holder itself never waits here —
    /// the executor refuses its writes before calling this (a holder
    /// waiting for its own release would deadlock).
    pub(crate) async fn wait_for_mutation_window(&self, writer_session_id: u64) {
        loop {
            let now = tokio::time::Instant::now();
            let Some(lease) = self.active(now) else {
                return; // free (or expired) — the writer may proceed
            };
            if lease.session_id == writer_session_id {
                // Defensive: the executor should have refused this write.
                return;
            }
            // Wait until the deadline (expiry) or a release notification,
            // whichever comes first. `notify_waiters` (not `notified`
            // registration races) is fine here because the loop re-checks
            // state after every wake.
            let wait = tokio::time::sleep_until(lease.deadline);
            tokio::select! {
                _ = self.changed.notified() => {}
                _ = wait => {}
            }
        }
    }
}

/// SQL surface for the lease: `ACQUIRE SNAPSHOT LEASE [TIMEOUT <millis>]`,
/// `RELEASE SNAPSHOT LEASE`, and `SHOW SNAPSHOT LEASE`.
impl super::Executor {
    /// Whether `session`'s open transaction holds uncommitted writes, from
    /// the EXECUTOR's side: before-images for tables served by engines with
    /// no transaction of their own (per-table overrides, a non-MVCC default
    /// engine), cross-model enlistment (KV/specialty writes inside the
    /// transaction), or staged policy/GIN/derived-state changes. The
    /// engine's own buffers are asked through
    /// [`StorageEngine::session_has_uncommitted_writes`].
    async fn session_txn_has_uncommitted_writes(&self, id: u64, session: &super::Session) -> bool {
        if !session
            .txn_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return false;
        }
        if self.storage.session_has_uncommitted_writes(id) {
            return true;
        }
        if session
            .cross_model
            .lock()
            .as_ref()
            .is_some_and(|cm| !cm.enlisted.is_empty())
        {
            return true;
        }
        let txn = session.txn_state.read().await;
        !txn.engine_snapshots.is_empty()
            || txn.policy_dirty
            || txn.gin_dirty
            || !txn.derived_dirty_tables.is_empty()
    }

    /// Count of OTHER sessions whose open transactions hold uncommitted
    /// writes — the acquisition drain set. Named sessions plus, when the
    /// acquiring session is not itself the shared default (id 0), the
    /// default session too: embedded/background statements that never
    /// created a session land there, and their transaction is exactly as
    /// able to straddle the window.
    async fn foreign_write_bearing_txns(&self, me: u64) -> usize {
        let sessions: Vec<(u64, std::sync::Arc<super::Session>)> = self
            .sessions
            .read()
            .iter()
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let mut n = 0;
        for (id, session) in &sessions {
            if *id == me {
                continue;
            }
            if self.session_txn_has_uncommitted_writes(*id, session).await {
                n += 1;
            }
        }
        if me != 0
            && self
                .session_txn_has_uncommitted_writes(0, &self.default_session)
                .await
        {
            n += 1;
        }
        n
    }

    /// ACQUIRE SNAPSHOT LEASE [TIMEOUT <millis>].
    ///
    /// Requires an active transaction: the lease's point-in-time view IS the
    /// transaction's read snapshot, and tying the lease to the transaction is
    /// what makes release-on-commit/rollback structural rather than best
    /// effort. Fails with the remaining window if another session holds the
    /// lease.
    pub(super) async fn execute_acquire_snapshot_lease(
        &self,
        sql: &str,
    ) -> Result<super::ExecResult, super::ExecError> {
        let sess = self.current_session();
        if !sess
            .txn_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(super::ExecError::Runtime(
                "ACQUIRE SNAPSHOT LEASE requires an active transaction (BEGIN first) — \
                 the lease is released at that transaction's COMMIT/ROLLBACK"
                    .into(),
            ));
        }
        let trimmed = sql.trim().trim_end_matches(';');
        let rest = trimmed["ACQUIRE SNAPSHOT LEASE".len()..].trim();
        let timeout_ms = if rest.is_empty() {
            DEFAULT_TIMEOUT_MS
        } else if let Some(num) = rest.to_ascii_uppercase().strip_prefix("TIMEOUT") {
            let num = num.trim();
            num.parse::<u64>().map_err(|_| {
                super::ExecError::Unsupported(format!(
                    "ACQUIRE SNAPSHOT LEASE: expected 'TIMEOUT <millis>', got {rest:?}"
                ))
            })?
        } else {
            return Err(super::ExecError::Unsupported(format!(
                "ACQUIRE SNAPSHOT LEASE: unexpected argument {rest:?} (expected TIMEOUT <millis>)"
            )));
        };
        if timeout_ms == 0 {
            return Err(super::ExecError::Unsupported(
                "ACQUIRE SNAPSHOT LEASE: TIMEOUT must be at least 1 ms".into(),
            ));
        }
        let session_id = super::unique_gate::gate_session_id();
        if let Some((holder, _)) = self.snapshot_leases.holder()
            && holder == session_id
        {
            return Err(super::ExecError::Runtime(
                "this session already holds the snapshot lease".into(),
            ));
        }
        // Refuse to acquire inside a transaction that has already written.
        // The lease's moment cannot include the holder's own uncommitted
        // work (its snapshot would, on every engine, expose it to the
        // "frozen" view), and draining FOREIGN writers while this
        // transaction holds write-side resources (unique slots, row locks)
        // can deadlock a writer the drain is waiting for.
        {
            let session = self.current_session();
            if self
                .session_txn_has_uncommitted_writes(session_id, &session)
                .await
            {
                return Err(super::ExecError::Runtime(
                    "ACQUIRE SNAPSHOT LEASE: this transaction has already written — \
                     acquire at transaction start, before any writes"
                        .into(),
                ));
            }
        }
        // DRAIN: wait for every other session's write-bearing transaction
        // to end, bounded by the lease's own TIMEOUT. A writer whose
        // statement ran before the lease existed is invisible to the writer
        // gate; without the drain its COMMIT would land mid-window (and on
        // engines without versioning its uncommitted rows are readable
        // outright). An idle transaction that has written nothing does not
        // block.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let blockers = self.foreign_write_bearing_txns(session_id).await;
            if blockers == 0 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(super::ExecError::Runtime(format!(
                    "ACQUIRE SNAPSHOT LEASE: timed out waiting for {blockers} \
                     in-flight writer transaction(s) to end — retry, or raise TIMEOUT"
                )));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Pin the holder's read view to THIS instant. On versioning engines
        // the transaction's snapshot is re-taken (the moment is ACQUIRE,
        // not BEGIN — commits between the two belong in the view) and held
        // for the rest of the transaction. Elsewhere a no-op: the drain +
        // writer gate are what freeze the state there.
        self.storage.refresh_txn_snapshot();
        self.snapshot_leases
            .try_acquire(session_id, timeout_ms)
            .map_err(|refusal| super::ExecError::Runtime(refusal.to_string()))?;
        Ok(super::ExecResult::Command {
            tag: format!("ACQUIRE SNAPSHOT LEASE {timeout_ms}"),
            rows_affected: 0,
        })
    }

    /// RELEASE SNAPSHOT LEASE — explicit release by the holder.
    pub(super) fn execute_release_snapshot_lease(
        &self,
    ) -> Result<super::ExecResult, super::ExecError> {
        let session_id = super::unique_gate::gate_session_id();
        if !self.snapshot_leases.release(session_id) {
            return Err(super::ExecError::Runtime(
                "RELEASE SNAPSHOT LEASE: this session does not hold the lease".into(),
            ));
        }
        Ok(super::ExecResult::Command {
            tag: "RELEASE SNAPSHOT LEASE".into(),
            rows_affected: 0,
        })
    }

    /// SHOW SNAPSHOT LEASE — holder session id and remaining milliseconds,
    /// or an empty result when the lease is free.
    pub(super) fn execute_show_snapshot_lease(
        &self,
    ) -> Result<super::ExecResult, super::ExecError> {
        let rows = match self.snapshot_leases.holder() {
            Some((session_id, remaining_ms)) => vec![vec![
                crate::types::Value::Int64(session_id as i64),
                crate::types::Value::Int64(remaining_ms as i64),
            ]],
            None => vec![],
        };
        Ok(super::ExecResult::Select {
            columns: vec![
                ("holder_session".into(), crate::types::DataType::Int64),
                ("remaining_ms".into(), crate::types::DataType::Int64),
            ],
            rows,
        })
    }
}

#[cfg(test)]
mod tests {    use super::*;

    #[tokio::test]
    async fn acquire_release_and_second_holder() {
        let reg = SnapshotLeaseRegistry::new();
        reg.try_acquire(1, 60_000).unwrap();
        match reg.try_acquire(2, 60_000) {
            Err(LeaseRefusal::HeldByOther(_)) => {}
            Ok(()) => panic!("second holder admitted"),
        }
        assert_eq!(reg.holder().map(|(s, _)| s), Some(1));
        assert!(reg.release(1));
        assert!(!reg.release(1), "release is idempotent-once");
        reg.try_acquire(2, 60_000).unwrap();
        assert_eq!(reg.holder().map(|(s, _)| s), Some(2));
    }

    #[tokio::test]
    async fn wrong_session_cannot_release() {
        let reg = SnapshotLeaseRegistry::new();
        reg.try_acquire(1, 60_000).unwrap();
        assert!(!reg.release(2));
        assert_eq!(reg.holder().map(|(s, _)| s), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_lease_is_lazily_reclaimed() {
        let reg = SnapshotLeaseRegistry::new();
        reg.try_acquire(1, 50).unwrap();
        tokio::time::advance(Duration::from_millis(60)).await;
        assert!(reg.holder().is_none(), "expired lease must not report a holder");
        reg.try_acquire(2, 60_000)
            .expect("expired lease must not block acquisition");
    }

    #[tokio::test(start_paused = true)]
    async fn writer_wakes_at_expiry() {
        let reg = SnapshotLeaseRegistry::new();
        reg.try_acquire(1, 50).unwrap();
        let writer = reg.wait_for_mutation_window(2);
        tokio::pin!(writer);
        // Not free yet: the writer is parked.
        tokio::select! {
            _ = &mut writer => panic!("writer proceeded while the lease was held"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        tokio::time::advance(Duration::from_millis(60)).await;
        (&mut writer).await;
    }

    #[tokio::test(start_paused = true)]
    async fn writer_wakes_on_release() {
        let reg = std::sync::Arc::new(SnapshotLeaseRegistry::new());
        reg.try_acquire(1, 60_000).unwrap();
        let releaser = reg.clone();
        tokio::join!(
            async move { reg.wait_for_mutation_window(2).await },
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                releaser.release(1);
            }
        );
    }
}
