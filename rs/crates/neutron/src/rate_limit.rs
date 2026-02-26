//! Rate limiting middleware using a sliding window counter algorithm.
//!
//! Smoothly approximates a fixed-rate limit without the burst-at-boundary
//! problem of simple fixed windows.
//!
//! # Example
//!
//! ```rust,ignore
//! use std::time::Duration;
//! use neutron::prelude::*;
//! use neutron::rate_limit::RateLimiter;
//!
//! // 100 requests per minute
//! let limiter = RateLimiter::new(100, Duration::from_secs(60));
//!
//! let router = Router::new()
//!     .middleware(limiter)
//!     .get("/api/data", handle_data);
//! ```
//!
//! ## Custom key function
//!
//! By default, clients are identified by `X-Forwarded-For`, then `X-Real-Ip`
//! header, falling back to `"anonymous"`. You can customize this:
//!
//! ```rust,ignore
//! let limiter = RateLimiter::new(100, Duration::from_secs(60))
//!     .key_fn(|req| {
//!         req.headers()
//!             .get("x-api-key")
//!             .and_then(|v| v.to_str().ok())
//!             .unwrap_or("anonymous")
//!             .to_string()
//!     });
//! ```
//!
//! ## Response Headers
//!
//! All responses include rate limit information:
//! - `X-RateLimit-Limit` — Maximum requests per window
//! - `X-RateLimit-Remaining` — Requests remaining in current window
//! - `X-RateLimit-Reset` — Seconds until the window resets
//!
//! Rate-limited responses (429) also include:
//! - `Retry-After` — Seconds to wait before retrying

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::{HeaderValue, StatusCode};

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// Rate limiter
// ---------------------------------------------------------------------------

/// Rate limiting middleware.
///
/// Uses a **sliding window counter** algorithm: the estimated request count is
/// `prev_window_count * (1 - elapsed%) + current_window_count`. This gives a
/// smooth approximation without the burst-at-boundary problem of fixed windows.
///
/// Each unique client key gets an independent counter. By default the key is
/// extracted from `X-Forwarded-For` / `X-Real-Ip` headers; use
/// [`key_fn`](Self::key_fn) to override.
pub struct RateLimiter {
    max_requests: u64,
    window: Duration,
    key_fn: Arc<dyn Fn(&Request) -> String + Send + Sync>,
    store: Arc<Mutex<Store>>,
}

struct Store {
    entries: HashMap<String, WindowEntry>,
    last_cleanup: Instant,
}

struct WindowEntry {
    /// Request count in the current window.
    current_count: u64,
    /// Request count in the previous window (used for sliding estimate).
    previous_count: u64,
    /// When the current window started.
    window_start: Instant,
}

enum CheckResult {
    Allowed {
        limit: u64,
        remaining: u64,
        reset_secs: u64,
    },
    Limited {
        limit: u64,
        reset_secs: u64,
    },
}

impl RateLimiter {
    /// Create a rate limiter allowing `max_requests` per `window` duration.
    ///
    /// ```rust,ignore
    /// use std::time::Duration;
    /// use neutron::rate_limit::RateLimiter;
    ///
    /// // 100 requests per 60 seconds
    /// let limiter = RateLimiter::new(100, Duration::from_secs(60));
    /// ```
    pub fn new(max_requests: u64, window: Duration) -> Self {
        Self {
            max_requests,
            window,
            key_fn: Arc::new(default_key_fn),
            store: Arc::new(Mutex::new(Store {
                entries: HashMap::new(),
                last_cleanup: Instant::now(),
            })),
        }
    }

    /// Set a custom function to extract the rate-limit key from each request.
    ///
    /// Different keys are tracked independently. By default, the key is
    /// extracted from `X-Forwarded-For` or `X-Real-Ip` headers.
    pub fn key_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&Request) -> String + Send + Sync + 'static,
    {
        self.key_fn = Arc::new(f);
        self
    }

    fn check(&self, key: &str, now: Instant) -> CheckResult {
        let mut store = self.store.lock().unwrap();

        // Periodic cleanup of stale entries (every window)
        if now.duration_since(store.last_cleanup) > self.window {
            store
                .entries
                .retain(|_, e| now.duration_since(e.window_start) < self.window * 2);
            store.last_cleanup = now;
        }

        let entry = store
            .entries
            .entry(key.to_string())
            .or_insert_with(|| WindowEntry {
                current_count: 0,
                previous_count: 0,
                window_start: now,
            });

        let elapsed = now.duration_since(entry.window_start);

        if elapsed >= self.window * 2 {
            // Both windows fully expired — reset
            entry.previous_count = 0;
            entry.current_count = 0;
            entry.window_start = now;
        } else if elapsed >= self.window {
            // Current window expired — rotate
            entry.previous_count = entry.current_count;
            entry.current_count = 0;
            entry.window_start += self.window;
        }

        // Sliding window estimate
        let elapsed_in_window = now.duration_since(entry.window_start);
        let pct = if self.window.as_nanos() > 0 {
            elapsed_in_window.as_secs_f64() / self.window.as_secs_f64()
        } else {
            1.0
        };
        let pct = pct.clamp(0.0, 1.0);

        let estimated =
            entry.previous_count as f64 * (1.0 - pct) + entry.current_count as f64;

        let remaining_window = self.window.saturating_sub(elapsed_in_window);
        let reset_secs = remaining_window.as_secs().max(1);

        if estimated >= self.max_requests as f64 {
            CheckResult::Limited {
                limit: self.max_requests,
                reset_secs,
            }
        } else {
            entry.current_count += 1;
            let remaining =
                (self.max_requests as f64 - estimated - 1.0).max(0.0) as u64;

            CheckResult::Allowed {
                limit: self.max_requests,
                remaining,
                reset_secs,
            }
        }
    }
}

fn default_key_fn(req: &Request) -> String {
    // Try X-Forwarded-For first (first IP = closest to client)
    if let Some(val) = req.headers().get("x-forwarded-for") {
        if let Ok(s) = val.to_str() {
            return s.split(',').next().unwrap_or("").trim().to_string();
        }
    }
    // Then X-Real-Ip
    if let Some(val) = req.headers().get("x-real-ip") {
        if let Ok(s) = val.to_str() {
            return s.trim().to_string();
        }
    }
    "anonymous".to_string()
}

fn header_val(n: u64) -> HeaderValue {
    HeaderValue::from_str(&n.to_string()).unwrap()
}

impl MiddlewareTrait for RateLimiter {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let key = (self.key_fn)(&req);
        let result = self.check(&key, Instant::now());

        Box::pin(async move {
            match result {
                CheckResult::Allowed {
                    limit,
                    remaining,
                    reset_secs,
                } => {
                    let mut resp = next.run(req).await;
                    resp.headers_mut()
                        .insert("x-ratelimit-limit", header_val(limit));
                    resp.headers_mut()
                        .insert("x-ratelimit-remaining", header_val(remaining));
                    resp.headers_mut()
                        .insert("x-ratelimit-reset", header_val(reset_secs));
                    resp
                }
                CheckResult::Limited { limit, reset_secs } => {
                    http::Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .header("content-type", "text/plain; charset=utf-8")
                        .header("retry-after", header_val(reset_secs))
                        .header("x-ratelimit-limit", header_val(limit))
                        .header("x-ratelimit-remaining", header_val(0))
                        .header("x-ratelimit-reset", header_val(reset_secs))
                        .body(Body::full("Too Many Requests"))
                        .unwrap()
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

    #[tokio::test]
    async fn allows_requests_under_limit() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(5, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        for _ in 0..5 {
            let resp = client.get("/").send().await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.text().await, "ok");
        }
    }

    #[tokio::test]
    async fn rejects_over_limit() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(3, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        for _ in 0..3 {
            let resp = client.get("/").send().await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn rate_limit_headers_present() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(10, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("x-ratelimit-limit").unwrap(), "10");
        assert_eq!(resp.header("x-ratelimit-remaining").unwrap(), "9");
        assert!(resp.header("x-ratelimit-reset").is_some());
    }

    #[tokio::test]
    async fn retry_after_on_429() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(1, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        client.get("/").send().await;

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(resp.header("retry-after").is_some());
        assert_eq!(resp.header("x-ratelimit-remaining").unwrap(), "0");
    }

    #[tokio::test]
    async fn different_ips_tracked_separately() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(2, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        // Client A: uses up its limit
        for _ in 0..2 {
            let resp = client
                .get("/")
                .header("x-forwarded-for", "1.2.3.4")
                .send()
                .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Client A is now limited
        let resp = client
            .get("/")
            .header("x-forwarded-for", "1.2.3.4")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // Client B is unaffected
        let resp = client
            .get("/")
            .header("x-forwarded-for", "5.6.7.8")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn custom_key_function() {
        let client = TestClient::new(
            Router::new()
                .middleware(
                    RateLimiter::new(2, Duration::from_secs(60)).key_fn(|req| {
                        req.headers()
                            .get("x-api-key")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("no-key")
                            .to_string()
                    }),
                )
                .get("/", || async { "ok" }),
        );

        // Key "abc" uses up limit
        for _ in 0..2 {
            let resp = client
                .get("/")
                .header("x-api-key", "abc")
                .send()
                .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Key "abc" is limited
        let resp = client
            .get("/")
            .header("x-api-key", "abc")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // Key "xyz" is still allowed
        let resp = client
            .get("/")
            .header("x-api-key", "xyz")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn remaining_decrements() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(5, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-ratelimit-remaining").unwrap(), "4");

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-ratelimit-remaining").unwrap(), "3");

        let resp = client.get("/").send().await;
        assert_eq!(resp.header("x-ratelimit-remaining").unwrap(), "2");
    }

    #[tokio::test]
    async fn window_resets_after_expiry() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(2, Duration::from_millis(100)))
                .get("/", || async { "ok" }),
        );

        // Use up the limit
        for _ in 0..2 {
            let resp = client.get("/").send().await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Should be limited
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // Wait for both windows to expire (2x window for sliding counter)
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Should be allowed again
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn x_real_ip_fallback() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(2, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        for _ in 0..2 {
            let resp = client
                .get("/")
                .header("x-real-ip", "10.0.0.1")
                .send()
                .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Limited for this IP
        let resp = client
            .get("/")
            .header("x-real-ip", "10.0.0.1")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        // Different IP still allowed
        let resp = client
            .get("/")
            .header("x-real-ip", "10.0.0.2")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn response_body_on_429() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(1, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        client.get("/").send().await;

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.text().await, "Too Many Requests");
    }

    #[tokio::test]
    async fn forwarded_for_takes_first_ip() {
        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(2, Duration::from_secs(60)))
                .get("/", || async { "ok" }),
        );

        // Multiple IPs in X-Forwarded-For — should use the first
        for _ in 0..2 {
            let resp = client
                .get("/")
                .header("x-forwarded-for", "1.2.3.4, 5.6.7.8, 9.10.11.12")
                .send()
                .await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Same first IP = same client, even with different proxy chain
        let resp = client
            .get("/")
            .header("x-forwarded-for", "1.2.3.4, 99.99.99.99")
            .send()
            .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn handler_not_called_when_limited() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&call_count);

        let client = TestClient::new(
            Router::new()
                .middleware(RateLimiter::new(2, Duration::from_secs(60)))
                .get("/", move || {
                    let counter = Arc::clone(&counter);
                    async move {
                        counter.fetch_add(1, Ordering::Relaxed);
                        "ok"
                    }
                }),
        );

        // First 2 requests hit the handler
        client.get("/").send().await;
        client.get("/").send().await;
        assert_eq!(call_count.load(Ordering::Relaxed), 2);

        // 3rd request is blocked before reaching the handler
        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
    }
}
