//! Cross-table snapshot lease (Consumer-2 / teploy-observe F45).
//!
//! A point-in-time backup boundary: `ACQUIRE SNAPSHOT LEASE` pins one
//! database-wide mutation window. While the lease is held:
//!
//! - the holder's transaction keeps its MVCC snapshot (stable across
//!   statements), so its reads are one logical moment across every table;
//! - every OTHER session's SQL mutations (DML + table-shape DDL) WAIT at
//!   the dispatch gate until the lease is released or expires — no commit
//!   can advance the database past the holder's moment while it reads;
//! - the holder's own mutations are refused (the lease view is read-only;
//!   letting the holder write would both dirty the "moment" and deadlock
//!   the writer gate against itself).
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
//! executor's central dispatch. The KV wire fast path and specialty-model
//! writes that bypass the executor are not lease-gated.

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
        if let Some(existing) = *guard {
            if existing.deadline > now && existing.session_id != session_id {
                return Err(LeaseRefusal::HeldByOther(
                    existing.deadline.duration_since(now).as_millis() as u64,
                ));
            }
            // Same session re-acquire, or an expired lease: replace.
        }
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
    /// ACQUIRE SNAPSHOT LEASE [TIMEOUT <millis>].
    ///
    /// Requires an active transaction: the lease's point-in-time view IS the
    /// transaction's MVCC snapshot, and tying the lease to the transaction is
    /// what makes release-on-commit/rollback structural rather than best
    /// effort. Fails with the remaining window if another session holds the
    /// lease.
    pub(super) fn execute_acquire_snapshot_lease(
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
