//! Circuit breaker middleware for downstream service protection.
//!
//! Implements the circuit breaker pattern with three states:
//!
//! - **Closed** — normal operation, requests pass through
//! - **Open** — failing fast, returns 503 immediately
//! - **Half-Open** — allows one probe request to test recovery
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::circuit_breaker::CircuitBreaker;
//! use std::time::Duration;
//!
//! let router = Router::new()
//!     .middleware(CircuitBreaker::new()
//!         .failure_threshold(5)
//!         .recovery_timeout(Duration::from_secs(30))
//!         .success_threshold(2))
//!     .get("/api/proxy", proxy_handler);
//! ```
//!
//! ## State Transitions
//!
//! ```text
//! ┌────────┐  failure_threshold  ┌──────┐  recovery_timeout  ┌───────────┐
//! │ Closed ├─────────exceeded───►│ Open ├────────elapsed────►│ Half-Open │
//! └───▲────┘                     └──────┘                    └─────┬─────┘
//!     │                                                            │
//!     └──────────success_threshold met─────────────────────────────┘
//!     │                                                            │
//!     └──────────────failure───────────────────────────►Open───────┘
//! ```

use std::future::Future;
use std::pin::Pin;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::StatusCode;

use crate::handler::{IntoResponse, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Normal operation — requests pass through.
    Closed,
    /// Failing fast — requests are rejected immediately with 503.
    Open,
    /// Testing recovery — one probe request is allowed through.
    HalfOpen,
}

struct CircuitInner {
    state: State,
    epoch: u64,
    failures: u32,
    successes: u32,
    probe: bool,
    last_failure: Option<Instant>,
}
struct CircuitState {
    inner: Mutex<CircuitInner>,
    failure_threshold: u32,
    success_threshold: u32,
    recovery_timeout: Duration,
    on_state_change: Option<Arc<dyn Fn(State, State) + Send + Sync>>,
}
impl CircuitState {
    fn notify(&self, change: Option<(State, State)>) {
        if let (Some((from, to)), Some(cb)) = (change, &self.on_state_change) {
            cb(from, to);
        }
    }
    fn transition(inner: &mut CircuitInner, to: State) -> Option<(State, State)> {
        let from = inner.state;
        if from == to {
            return None;
        }
        inner.state = to;
        inner.epoch = inner.epoch.wrapping_add(1);
        inner.probe = false;
        inner.successes = 0;
        Some((from, to))
    }
    fn recover(&self, inner: &mut CircuitInner) -> Option<(State, State)> {
        if inner.state == State::Open
            && inner
                .last_failure
                .is_some_and(|t| t.elapsed() >= self.recovery_timeout)
        {
            Self::transition(inner, State::HalfOpen)
        } else {
            None
        }
    }
    fn current_state(&self) -> State {
        let mut inner = self.inner.lock().unwrap();
        let change = self.recover(&mut inner);
        let state = inner.state;
        drop(inner);
        self.notify(change);
        state
    }
    fn acquire(self: &Arc<Self>) -> Option<ProbePermit> {
        let mut inner = self.inner.lock().unwrap();
        let change = self.recover(&mut inner);
        let permit = match inner.state {
            State::Open => None,
            State::HalfOpen if inner.probe => None,
            state => {
                let probe = state == State::HalfOpen;
                if probe {
                    inner.probe = true;
                }
                Some(ProbePermit {
                    state: self.clone(),
                    epoch: inner.epoch,
                    probe,
                })
            }
        };
        drop(inner);
        self.notify(change);
        permit
    }
    fn record(&self, epoch: u64, failed: bool) {
        let mut inner = self.inner.lock().unwrap();
        if inner.epoch != epoch {
            return;
        }
        let change = match (inner.state, failed) {
            (State::Closed, false) => {
                inner.failures = 0;
                None
            }
            (State::Closed, true) => {
                inner.failures = inner.failures.saturating_add(1);
                if inner.failures >= self.failure_threshold {
                    inner.last_failure = Some(Instant::now());
                    Self::transition(&mut inner, State::Open)
                } else {
                    None
                }
            }
            (State::HalfOpen, true) => {
                inner.last_failure = Some(Instant::now());
                Self::transition(&mut inner, State::Open)
            }
            (State::HalfOpen, false) => {
                inner.successes = inner.successes.saturating_add(1);
                if inner.successes >= self.success_threshold {
                    inner.failures = 0;
                    Self::transition(&mut inner, State::Closed)
                } else {
                    None
                }
            }
            _ => None,
        };
        drop(inner);
        self.notify(change);
    }
}

// ---------------------------------------------------------------------------
// CircuitBreaker middleware
// ---------------------------------------------------------------------------

/// Circuit breaker middleware.
///
/// See [module-level docs](self) for details.
pub struct CircuitBreaker {
    state: Arc<CircuitState>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default settings.
    ///
    /// Defaults: 5 failures to open, 30s recovery, 2 successes to close.
    pub fn new() -> Self {
        Self {
            state: Arc::new(CircuitState {
                inner: Mutex::new(CircuitInner {
                    state: State::Closed,
                    epoch: 0,
                    failures: 0,
                    successes: 0,
                    probe: false,
                    last_failure: None,
                }),
                failure_threshold: 5,
                success_threshold: 2,
                recovery_timeout: Duration::from_secs(30),
                on_state_change: None,
            }),
        }
    }

    /// Set the number of failures before opening the circuit (default: 5).
    pub fn failure_threshold(mut self, threshold: u32) -> Self {
        Arc::get_mut(&mut self.state).unwrap().failure_threshold = threshold;
        self
    }

    /// Set the number of successes in half-open before closing (default: 2).
    pub fn success_threshold(mut self, threshold: u32) -> Self {
        Arc::get_mut(&mut self.state).unwrap().success_threshold = threshold;
        self
    }

    /// Set the recovery timeout — how long the circuit stays open (default: 30s).
    pub fn recovery_timeout(mut self, timeout: Duration) -> Self {
        Arc::get_mut(&mut self.state).unwrap().recovery_timeout = timeout;
        self
    }

    /// Set a callback for state changes (useful for alerting/logging).
    pub fn on_state_change(
        mut self,
        callback: impl Fn(State, State) + Send + Sync + 'static,
    ) -> Self {
        Arc::get_mut(&mut self.state).unwrap().on_state_change = Some(Arc::new(callback));
        self
    }

    /// Get the current circuit state.
    pub fn state(&self) -> State {
        self.state.current_state()
    }

    /// Get a handle for inspecting circuit state from handlers.
    pub fn handle(&self) -> CircuitBreakerHandle {
        CircuitBreakerHandle {
            state: Arc::clone(&self.state),
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for inspecting circuit breaker state from handlers.
#[derive(Clone)]
pub struct CircuitBreakerHandle {
    state: Arc<CircuitState>,
}

impl CircuitBreakerHandle {
    /// Get the current circuit state.
    pub fn state(&self) -> State {
        self.state.current_state()
    }

    /// Get the current failure count.
    pub fn failure_count(&self) -> u32 {
        self.state.inner.lock().unwrap().failures
    }
}

fn is_failure(status: StatusCode) -> bool {
    status.is_server_error()
}

/// RAII ownership of the half-open probe permit. Clearing the flag after
/// the `await` (the old pattern) leaked the permit when the probe future
/// was CANCELLED — e.g. by an outer `Timeout` dropping `next.run(req)` —
/// leaving `probe_in_flight == true` forever. `HalfOpen` has no expiry
/// transition, so every later request returned 503 permanently. Drop-based
/// release keeps the circuit eligible for a new probe under cancellation.
struct ProbePermit {
    state: Arc<CircuitState>,
    epoch: u64,
    probe: bool,
}
impl Drop for ProbePermit {
    fn drop(&mut self) {
        if self.probe {
            let mut inner = self.state.inner.lock().unwrap();
            if inner.epoch == self.epoch {
                inner.probe = false;
            }
        }
    }
}
impl MiddlewareTrait for CircuitBreaker {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let Some(permit) = state.acquire() else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Circuit breaker is open or probing",
                )
                    .into_response();
            };
            let resp = next.run(req).await;
            state.record(permit.epoch, is_failure(resp.status()));
            resp
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::testing::TestClient;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn test_setup(
        threshold: u32,
        recovery_ms: u64,
    ) -> (TestClient, Arc<AtomicBool>, CircuitBreakerHandle) {
        let fail = Arc::new(AtomicBool::new(false));
        let fail_clone = fail.clone();

        let cb = CircuitBreaker::new()
            .failure_threshold(threshold)
            .success_threshold(1)
            .recovery_timeout(Duration::from_millis(recovery_ms));

        let handle = cb.handle();

        let client = TestClient::new(Router::new().middleware(cb).get("/", move || {
            let should_fail = fail_clone.clone();
            async move {
                if should_fail.load(Ordering::SeqCst) {
                    (StatusCode::INTERNAL_SERVER_ERROR, "error").into_response()
                } else {
                    "ok".into_response()
                }
            }
        }));

        (client, fail, handle)
    }

    #[tokio::test]
    async fn starts_closed() {
        let (_client, _fail, handle) = test_setup(3, 1000);
        assert_eq!(handle.state(), State::Closed);
    }

    #[tokio::test]
    async fn passes_requests_when_closed() {
        let (client, _fail, _handle) = test_setup(3, 1000);

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "ok");
    }

    #[tokio::test]
    async fn opens_after_threshold_failures() {
        let (client, fail, handle) = test_setup(3, 1000);

        fail.store(true, Ordering::SeqCst);

        // 3 failures
        for _ in 0..3 {
            client.get("/").send().await;
        }

        assert_eq!(handle.state(), State::Open);
    }

    #[tokio::test]
    async fn returns_503_when_open() {
        let (client, fail, handle) = test_setup(2, 5000);

        fail.store(true, Ordering::SeqCst);

        // Trigger opening
        client.get("/").send().await;
        client.get("/").send().await;

        assert_eq!(handle.state(), State::Open);

        // Next request should be 503 (fast-fail)
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.text().await, "Circuit breaker is open or probing");
    }

    #[tokio::test]
    async fn transitions_to_half_open_after_recovery() {
        let (client, fail, handle) = test_setup(2, 50);

        fail.store(true, Ordering::SeqCst);

        client.get("/").send().await;
        client.get("/").send().await;
        assert_eq!(handle.state(), State::Open);

        // Wait for recovery timeout
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(handle.state(), State::HalfOpen);
    }

    #[tokio::test]
    async fn closes_on_success_in_half_open() {
        let (client, fail, handle) = test_setup(2, 50);

        fail.store(true, Ordering::SeqCst);
        client.get("/").send().await;
        client.get("/").send().await;

        // Wait for recovery
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(handle.state(), State::HalfOpen);

        // Successful probe
        fail.store(false, Ordering::SeqCst);
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);

        assert_eq!(handle.state(), State::Closed);
    }

    #[tokio::test]
    async fn reopens_on_failure_in_half_open() {
        let (client, fail, handle) = test_setup(2, 50);

        fail.store(true, Ordering::SeqCst);
        client.get("/").send().await;
        client.get("/").send().await;

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(handle.state(), State::HalfOpen);

        // Failed probe
        client.get("/").send().await;
        assert_eq!(handle.state(), State::Open);
    }

    #[tokio::test]
    async fn successes_reset_failure_count() {
        let (client, fail, handle) = test_setup(3, 1000);

        fail.store(true, Ordering::SeqCst);
        client.get("/").send().await; // fail 1
        client.get("/").send().await; // fail 2

        assert_eq!(handle.failure_count(), 2);

        // One success resets
        fail.store(false, Ordering::SeqCst);
        client.get("/").send().await;

        assert_eq!(handle.failure_count(), 0);
        assert_eq!(handle.state(), State::Closed);
    }

    #[tokio::test]
    async fn state_change_callback() {
        let transitions = Arc::new(Mutex::new(Vec::new()));
        let transitions_clone = transitions.clone();

        let cb = CircuitBreaker::new()
            .failure_threshold(2)
            .success_threshold(1)
            .recovery_timeout(Duration::from_millis(50))
            .on_state_change(move |from, to| {
                transitions_clone.lock().unwrap().push((from, to));
            });

        let fail = Arc::new(AtomicBool::new(true));
        let fail_clone = fail.clone();

        let client = TestClient::new(Router::new().middleware(cb).get("/", move || {
            let f = fail_clone.clone();
            async move {
                if f.load(Ordering::SeqCst) {
                    (StatusCode::INTERNAL_SERVER_ERROR, "err").into_response()
                } else {
                    "ok".into_response()
                }
            }
        }));

        // Trigger open
        client.get("/").send().await;
        client.get("/").send().await;

        // Wait for half-open
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Successful probe → closed
        fail.store(false, Ordering::SeqCst);
        client.get("/").send().await;

        let t = transitions.lock().unwrap();
        assert!(t.contains(&(State::Closed, State::Open)));
        assert!(t.contains(&(State::Open, State::HalfOpen)));
        assert!(t.contains(&(State::HalfOpen, State::Closed)));
    }

    #[tokio::test]
    async fn only_server_errors_count_as_failures() {
        // Client errors (4xx) should NOT count as circuit breaker failures
        let cb = CircuitBreaker::new()
            .failure_threshold(3)
            .success_threshold(1)
            .recovery_timeout(Duration::from_secs(30));

        let handle = cb.handle();

        let client = TestClient::new(Router::new().middleware(cb).get("/", || async {
            (StatusCode::BAD_REQUEST, "bad").into_response()
        }));

        for _ in 0..5 {
            client.get("/").send().await;
        }

        // Should still be closed — 400s are not server errors
        assert_eq!(handle.state(), State::Closed);
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Regression (RS-33): a half-open probe whose future is CANCELLED
    /// (here: an outer timeout dropping the request) used to leak the probe
    /// permit — `probe_in_flight` stayed true forever, HalfOpen has no
    /// expiry, and every subsequent request returned 503 permanently. The
    /// permit is now RAII-owned, so a cancelled probe releases it and the
    /// next request can probe again.
    #[tokio::test]
    async fn cancelled_half_open_probe_does_not_wedge_the_breaker() {
        use crate::timeout::Timeout;

        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let stall = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fail2 = fail.clone();
        let stall2 = stall.clone();
        let entered2 = entered.clone();
        let release = Arc::new(tokio::sync::Notify::new());
        let release2 = release.clone();

        let cb = CircuitBreaker::new()
            .failure_threshold(1)
            .success_threshold(1)
            .recovery_timeout(Duration::from_millis(20));
        let handle = cb.handle();

        let client = TestClient::new(
            Router::new()
                .middleware(Timeout::from_millis(30))
                .middleware(cb)
                .get("/", move || {
                    let should_fail = fail2.clone();
                    let should_stall = stall2.clone();
                    let e = entered2.clone();
                    let rel = release2.clone();
                    async move {
                        if should_fail.load(Ordering::SeqCst) {
                            return (StatusCode::INTERNAL_SERVER_ERROR, "error").into_response();
                        }
                        if should_stall.load(Ordering::SeqCst) {
                            e.store(true, Ordering::SeqCst);
                            rel.notified().await; // stall past the outer timeout
                        }
                        "ok".into_response()
                    }
                }),
        );

        // 1. Trip the breaker open (threshold 1).
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(handle.state(), State::Open);

        // 2. Recovery elapses -> HalfOpen. A stalled probe under the outer
        //    timeout is cancelled mid-flight (408) while holding the permit.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(handle.state(), State::HalfOpen);
        fail.store(false, Ordering::SeqCst);
        stall.store(true, Ordering::SeqCst);
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
        assert!(entered.load(Ordering::SeqCst), "probe should have started");

        // 3. Without the fix, this next request 503s forever ("probe in
        //    progress") — the wedged state from the audit. With RAII release
        //    it must be admitted as a probe, succeed, and close the circuit.
        stall.store(false, Ordering::SeqCst);
        let resp = tokio::time::timeout(Duration::from_millis(500), client.get("/").send())
            .await
            .expect("breaker must not be wedged");
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(handle.state(), State::Closed);
    }

    #[tokio::test]
    async fn old_outcomes_cannot_decide_new_recovery_epoch() {
        let breaker = CircuitBreaker::new()
            .failure_threshold(1)
            .success_threshold(1)
            .recovery_timeout(Duration::ZERO);
        let old_success = breaker.state.acquire().unwrap();
        let old_failure = breaker.state.acquire().unwrap();
        let trip = breaker.state.acquire().unwrap();
        breaker.state.record(trip.epoch, true);
        let probe = breaker.state.acquire().unwrap();
        assert_eq!(breaker.state(), State::HalfOpen);
        breaker.state.record(old_success.epoch, false);
        breaker.state.record(old_failure.epoch, true);
        assert_eq!(breaker.state(), State::HalfOpen);
        assert!(breaker.state.acquire().is_none());
        drop(probe);
        let replacement = breaker.state.acquire().unwrap();
        breaker.state.record(replacement.epoch, false);
        assert_eq!(breaker.state(), State::Closed);
    }
}
