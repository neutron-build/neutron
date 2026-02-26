//! Per-request timeout middleware.
//!
//! Aborts handler execution if it does not complete within the configured
//! duration, returning `408 Request Timeout`.
//!
//! ```rust,ignore
//! Router::new().middleware(Timeout::from_secs(30))
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use http::StatusCode;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

/// Middleware that enforces a per-request time limit.
///
/// If the downstream handler does not complete within the configured duration,
/// the request is aborted and a `408 Request Timeout` response is returned.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
/// use std::time::Duration;
///
/// let router = Router::new()
///     .middleware(Timeout::from_secs(30))
///     .get("/", handler);
/// ```
#[derive(Clone)]
pub struct Timeout {
    duration: Duration,
}

impl Timeout {
    /// Create a timeout middleware with the given duration.
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }

    /// Create a timeout middleware with the given number of seconds.
    pub fn from_secs(secs: u64) -> Self {
        Self::new(Duration::from_secs(secs))
    }

    /// Create a timeout middleware with the given number of milliseconds.
    pub fn from_millis(millis: u64) -> Self {
        Self::new(Duration::from_millis(millis))
    }
}

impl MiddlewareTrait for Timeout {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let duration = self.duration;
        Box::pin(async move {
            match tokio::time::timeout(duration, next.run(req)).await {
                Ok(response) => response,
                Err(_elapsed) => http::Response::builder()
                    .status(StatusCode::REQUEST_TIMEOUT)
                    .header("content-type", "text/plain; charset=utf-8")
                    .body(Body::full("Request Timeout"))
                    .unwrap(),
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
    async fn normal_request_passes() {
        let client = TestClient::new(
            Router::new()
                .middleware(Timeout::from_secs(5))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "ok");
    }

    #[tokio::test]
    async fn slow_handler_times_out() {
        let client = TestClient::new(
            Router::new()
                .middleware(Timeout::from_millis(50))
                .get("/slow", || async {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    "done"
                }),
        );

        let resp = client.get("/slow").send().await;
        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn handler_just_under_timeout_passes() {
        let client = TestClient::new(
            Router::new()
                .middleware(Timeout::from_millis(200))
                .get("/", || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    "fast enough"
                }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "fast enough");
    }

    #[tokio::test]
    async fn timeout_preserves_response_headers() {
        let client = TestClient::new(
            Router::new()
                .middleware(Timeout::from_secs(5))
                .get("/", || async {
                    let mut headers = http::HeaderMap::new();
                    headers.insert("x-custom", "value".parse().unwrap());
                    (headers, "ok")
                }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("x-custom").unwrap(), "value");
    }

    #[tokio::test]
    async fn timeout_response_body() {
        let client = TestClient::new(
            Router::new()
                .middleware(Timeout::from_millis(10))
                .get("/", || async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    "never"
                }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(resp.text().await, "Request Timeout");
    }

    #[tokio::test]
    async fn works_with_other_middleware() {
        use crate::request_id::RequestId;

        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::new())
                .middleware(Timeout::from_secs(5))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.header("x-request-id").is_some());
    }
}
