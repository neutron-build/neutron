//! Graceful shutdown drain coordination.
//!
//! "Graceful shutdown" only means something if the ordering is enforced:
//!
//! 1. stop admitting new work,
//! 2. wait (bounded) for in-flight work to finish,
//! 3. *then* persist durable state,
//! 4. then exit.
//!
//! Doing 3 concurrently with 2 is the common bug: the persist step captures a
//! snapshot that a still-running request is about to invalidate, or the
//! process exits out from under a request that was already acknowledged as
//! accepted. [`ShutdownCoordinator`] makes the ordering explicit and, more
//! importantly, testable: [`ShutdownCoordinator::await_drain`] cannot return
//! `Drained` while any [`InflightGuard`] is alive.
//!
//! The wait is bounded. If work does not finish within the budget the caller
//! gets [`DrainOutcome::TimedOut`] with the number of stragglers, so shutdown
//! is never unbounded and the operator sees what did not finish.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

/// Result of a bounded drain wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// All in-flight work completed within the budget.
    Drained {
        /// How many units of work were in flight when the drain started.
        started_with: usize,
    },
    /// The budget expired first.
    TimedOut {
        /// Units of work still running when the budget expired.
        remaining: usize,
    },
}

impl DrainOutcome {
    pub fn is_drained(&self) -> bool {
        matches!(self, DrainOutcome::Drained { .. })
    }
}

/// RAII marker for one unit of in-flight work.
///
/// While any guard is alive the coordinator is not drained. Dropping the last
/// guard wakes the drain waiter, including on a panic unwind — a panicking
/// request must not wedge shutdown.
#[derive(Debug)]
pub struct InflightGuard {
    coordinator: Arc<ShutdownCoordinator>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let previous = self.coordinator.inflight.fetch_sub(1, Ordering::SeqCst);
        if previous == 1 {
            self.coordinator.drained.notify_waiters();
        }
    }
}

/// Tracks whether the server is draining and how much work is still running.
#[derive(Debug)]
pub struct ShutdownCoordinator {
    draining: AtomicBool,
    inflight: AtomicUsize,
    drained: Notify,
    /// Set once the persist phase has completed, so status output and tests
    /// can assert the ordering rather than infer it.
    persisted: AtomicBool,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self {
            draining: AtomicBool::new(false),
            inflight: AtomicUsize::new(0),
            drained: Notify::new(),
            persisted: AtomicBool::new(false),
        }
    }
}

impl ShutdownCoordinator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Admit one unit of work, unless shutdown has begun.
    ///
    /// Returns `None` once [`Self::begin_drain`] has been called; callers
    /// must then reject the request rather than starting it.
    pub fn try_admit(self: &Arc<Self>) -> Option<InflightGuard> {
        if self.draining.load(Ordering::SeqCst) {
            return None;
        }
        self.inflight.fetch_add(1, Ordering::SeqCst);
        // Re-check: `begin_drain` may have landed between the check and the
        // increment. Releasing here keeps the invariant that nothing new is
        // admitted after drain begins.
        if self.draining.load(Ordering::SeqCst) {
            let previous = self.inflight.fetch_sub(1, Ordering::SeqCst);
            if previous == 1 {
                self.drained.notify_waiters();
            }
            return None;
        }
        Some(InflightGuard {
            coordinator: self.clone(),
        })
    }

    /// Whether shutdown has begun.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Units of work currently in flight.
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Stop admitting new work. Idempotent.
    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
        // Wake any waiter that started before there was work to wait on.
        self.drained.notify_waiters();
    }

    /// Wait (bounded) for all in-flight work to finish.
    ///
    /// Implies [`Self::begin_drain`] so no work can be admitted while waiting;
    /// otherwise a steady arrival rate could keep the drain from ever
    /// completing.
    pub async fn await_drain(&self, budget: Duration) -> DrainOutcome {
        let started_with = self.inflight();
        self.begin_drain();
        if self.inflight() == 0 {
            return DrainOutcome::Drained { started_with };
        }

        let deadline = tokio::time::Instant::now() + budget;
        loop {
            // Register interest *before* re-reading the counter so a guard
            // dropped concurrently cannot be missed.
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.inflight() == 0 {
                return DrainOutcome::Drained { started_with };
            }

            let remaining_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining_budget.is_zero() {
                return DrainOutcome::TimedOut {
                    remaining: self.inflight(),
                };
            }

            if tokio::time::timeout(remaining_budget, notified)
                .await
                .is_err()
            {
                return DrainOutcome::TimedOut {
                    remaining: self.inflight(),
                };
            }
        }
    }

    /// Record that durable state has been persisted.
    pub fn mark_persisted(&self) {
        self.persisted.store(true, Ordering::SeqCst);
    }

    /// Whether the persist phase has completed.
    pub fn is_persisted(&self) -> bool {
        self.persisted.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[tokio::test]
    async fn drains_immediately_when_idle() {
        let c = ShutdownCoordinator::new();
        let outcome = c.await_drain(Duration::from_secs(1)).await;
        assert_eq!(outcome, DrainOutcome::Drained { started_with: 0 });
        assert!(c.is_draining());
    }

    #[tokio::test]
    async fn no_new_work_is_admitted_after_drain_begins() {
        let c = ShutdownCoordinator::new();
        assert!(c.try_admit().is_some());
        c.begin_drain();
        assert!(
            c.try_admit().is_none(),
            "work admitted after shutdown began"
        );
    }

    /// The core claim: `await_drain` must not report `Drained` until every
    /// in-flight request has actually finished its work.
    #[tokio::test]
    async fn drain_waits_for_in_flight_work_to_complete() {
        let c = ShutdownCoordinator::new();
        let completed = Arc::new(AtomicBool::new(false));

        let guard = c.try_admit().expect("admitted");
        let completed_task = completed.clone();
        let worker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(120)).await;
            completed_task.store(true, Ordering::SeqCst);
            drop(guard);
        });

        let outcome = c.await_drain(Duration::from_secs(5)).await;
        assert_eq!(outcome, DrainOutcome::Drained { started_with: 1 });
        assert!(
            completed.load(Ordering::SeqCst),
            "drain returned before in-flight work finished"
        );
        assert_eq!(c.inflight(), 0);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn drain_waits_for_every_in_flight_unit() {
        let c = ShutdownCoordinator::new();
        let done = Arc::new(AtomicU32::new(0));
        let mut workers = Vec::new();
        for i in 0..8u64 {
            let guard = c.try_admit().expect("admitted");
            let done = done.clone();
            workers.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20 + i * 10)).await;
                done.fetch_add(1, Ordering::SeqCst);
                drop(guard);
            }));
        }
        let outcome = c.await_drain(Duration::from_secs(10)).await;
        assert_eq!(outcome, DrainOutcome::Drained { started_with: 8 });
        assert_eq!(done.load(Ordering::SeqCst), 8);
        for w in workers {
            w.await.unwrap();
        }
    }

    /// Persist must observe a fully drained server, and must run after the
    /// last request completes — not concurrently with it.
    #[tokio::test]
    async fn persist_runs_only_after_the_drain_completes() {
        let c = ShutdownCoordinator::new();
        let order: Arc<parking_lot::Mutex<Vec<&'static str>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));

        let guard = c.try_admit().expect("admitted");
        let order_task = order.clone();
        let worker = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            order_task.lock().push("request-finished");
            drop(guard);
        });

        assert!(c.await_drain(Duration::from_secs(5)).await.is_drained());
        assert_eq!(c.inflight(), 0, "persist would run with work in flight");
        order.lock().push("persist");
        c.mark_persisted();

        worker.await.unwrap();
        assert_eq!(&*order.lock(), &["request-finished", "persist"]);
        assert!(c.is_persisted());
    }

    #[tokio::test]
    async fn drain_is_bounded_and_reports_stragglers() {
        let c = ShutdownCoordinator::new();
        let _stuck_a = c.try_admit().expect("admitted");
        let _stuck_b = c.try_admit().expect("admitted");
        let started = std::time::Instant::now();
        let outcome = c.await_drain(Duration::from_millis(100)).await;
        assert_eq!(outcome, DrainOutcome::TimedOut { remaining: 2 });
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "drain wait was not bounded"
        );
        assert!(!c.is_persisted());
    }

    #[tokio::test]
    async fn panicking_request_still_releases_its_slot() {
        let c = ShutdownCoordinator::new();
        let guard = c.try_admit().expect("admitted");
        let worker = tokio::spawn(async move {
            let _g = guard;
            panic!("request blew up");
        });
        assert!(worker.await.is_err());
        assert_eq!(c.inflight(), 0, "panicking request leaked its drain slot");
        assert!(c.await_drain(Duration::from_millis(200)).await.is_drained());
    }

    #[tokio::test]
    async fn zero_budget_with_work_times_out_without_hanging() {
        let c = ShutdownCoordinator::new();
        let _g = c.try_admit().expect("admitted");
        let outcome = c.await_drain(Duration::ZERO).await;
        assert_eq!(outcome, DrainOutcome::TimedOut { remaining: 1 });
    }
}
