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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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

// Internal state repr for atomics
const STATE_CLOSED: u32 = 0;
const STATE_OPEN: u32 = 1;
const STATE_HALF_OPEN: u32 = 2;

fn state_from_u32(v: u32) -> State {
    match v {
        STATE_CLOSED => State::Closed,
        STATE_OPEN => State::Open,
        STATE_HALF_OPEN => State::HalfOpen,
        _ => State::Closed,
    }
}

// ---------------------------------------------------------------------------
// CircuitState (shared)
// ---------------------------------------------------------------------------

struct CircuitState {
    state: AtomicU32,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    /// Guards half-open probing: only one request may probe at a time.
    probe_in_flight: AtomicBool,
    last_failure_time: Mutex<Option<Instant>>,
    failure_threshold: u32,
    success_threshold: u32,
    recovery_timeout: Duration,
    on_state_change: Option<Arc<dyn Fn(State, State) + Send + Sync>>,
}

impl CircuitState {
    fn current_state(&self) -> State {
        let state = state_from_u32(self.state.load(Ordering::SeqCst));

        // If Open, check if recovery timeout has elapsed
        if state == State::Open {
            let last_failure = self.last_failure_time.lock().unwrap();
            if let Some(t) = *last_failure {
                if t.elapsed() >= self.recovery_timeout {
                    // Transition to half-open
                    self.transition(State::Open, State::HalfOpen);
                    return State::HalfOpen;
                }
            }
        }

        state
    }

    fn transition(&self, from: State, to: State) {
        let from_u32 = match from {
            State::Closed => STATE_CLOSED,
            State::Open => STATE_OPEN,
            State::HalfOpen => STATE_HALF_OPEN,
        };
        let to_u32 = match to {
            State::Closed => STATE_CLOSED,
            State::Open => STATE_OPEN,
            State::HalfOpen => STATE_HALF_OPEN,
        };

        if self
            .state
            .compare_exchange(from_u32, to_u32, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if let Some(ref cb) = self.on_state_change {
                cb(from, to);
            }
        }
    }

    fn record_success(&self) {
        let state = self.current_state();

        match state {
            State::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            State::HalfOpen => {
                let prev = self.success_count.fetch_add(1, Ordering::SeqCst);
                if prev + 1 >= self.success_threshold {
                    // Enough successes — close the circuit
                    self.failure_count.store(0, Ordering::SeqCst);
                    self.success_count.store(0, Ordering::SeqCst);
                    self.transition(State::HalfOpen, State::Closed);
                }
            }
            State::Open => {} // shouldn't happen
        }
    }

    fn record_failure(&self) {
        let state = self.current_state();

        match state {
            State::Closed => {
                let prev = self.failure_count.fetch_add(1, Ordering::SeqCst);
                if prev + 1 >= self.failure_threshold {
                    // Threshold exceeded — open the circuit
                    *self.last_failure_time.lock().unwrap() = Some(Instant::now());
                    self.success_count.store(0, Ordering::SeqCst);
                    self.transition(State::Closed, State::Open);
                }
            }
            State::HalfOpen => {
                // Probe failed — reopen
                *self.last_failure_time.lock().unwrap() = Some(Instant::now());
                self.success_count.store(0, Ordering::SeqCst);
                self.transition(State::HalfOpen, State::Open);
            }
            State::Open => {} // shouldn't happen
        }
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
                state: AtomicU32::new(STATE_CLOSED),
                failure_count: AtomicU32::new(0),
                success_count: AtomicU32::new(0),
                probe_in_flight: AtomicBool::new(false),
                last_failure_time: Mutex::new(None),
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
        self.state.failure_count.load(Ordering::SeqCst)
    }
}

fn is_failure(status: StatusCode) -> bool {
    status.is_server_error()
}

impl MiddlewareTrait for CircuitBreaker {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let state = Arc::clone(&self.state);

        Box::pin(async move {
            match state.current_state() {
                State::Open => {
                    // Fast-fail
                    (StatusCode::SERVICE_UNAVAILABLE, "Circuit breaker is open").into_response()
                }
                State::HalfOpen => {
                    // Only one probe request allowed at a time
                    if state
                        .probe_in_flight
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        // We are the probe — send request
                        let resp = next.run(req).await;

                        state.probe_in_flight.store(false, Ordering::SeqCst);

                        if is_failure(resp.status()) {
                            state.record_failure();
                        } else {
                            state.record_success();
                        }

                        resp
                    } else {
                        // Another request is already probing — reject
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "Circuit breaker is half-open (probe in progress)",
                        )
                            .into_response()
                    }
                }
                State::Closed => {
                    // Normal operation
                    let resp = next.run(req).await;

                    if is_failure(resp.status()) {
                        state.record_failure();
                    } else {
                        state.record_success();
                    }

                    resp
                }
            }
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
    use std::sync::atomic::AtomicBool;

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

        let client = TestClient::new(
            Router::new()
                .middleware(cb)
                .get("/", move || {
                    let should_fail = fail_clone.clone();
                    async move {
                        if should_fail.load(Ordering::SeqCst) {
                            (StatusCode::INTERNAL_SERVER_ERROR, "error").into_response()
                        } else {
                            "ok".into_response()
                        }
                    }
                }),
        );

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
        assert_eq!(resp.text().await, "Circuit breaker is open");
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

        let client = TestClient::new(
            Router::new()
                .middleware(cb)
                .get("/", move || {
                    let f = fail_clone.clone();
                    async move {
                        if f.load(Ordering::SeqCst) {
                            (StatusCode::INTERNAL_SERVER_ERROR, "err").into_response()
                        } else {
                            "ok".into_response()
                        }
                    }
                }),
        );

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

        let client = TestClient::new(
            Router::new()
                .middleware(cb)
                .get("/", || async {
                    (StatusCode::BAD_REQUEST, "bad").into_response()
                }),
        );

        for _ in 0..5 {
            client.get("/").send().await;
        }

        // Should still be closed — 400s are not server errors
        assert_eq!(handle.state(), State::Closed);
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
