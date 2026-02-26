//! Request body size limiting middleware.
//!
//! Rejects requests whose body exceeds a configured maximum, returning
//! `413 Payload Too Large`.
//!
//! **Note:** The server enforces a global body size limit (default 2 MiB) at
//! the transport layer *before* the body is read into memory. Use
//! [`Neutron::max_body_size`](crate::app::Neutron::max_body_size) to configure
//! the global cap, and this middleware for per-route limits stricter than the
//! global default.
//!
//! ```rust,ignore
//! Router::new().middleware(BodyLimit::new(1024 * 1024)) // 1 MB
//! ```

use std::future::Future;
use std::pin::Pin;

use http::StatusCode;

use crate::handler::{Body, Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

/// Middleware that rejects requests with a body larger than the configured limit.
///
/// Returns `413 Payload Too Large` if the request body exceeds `max_bytes`.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// let router = Router::new()
///     .middleware(BodyLimit::new(1024 * 1024)) // 1 MB
///     .post("/upload", handle_upload);
/// ```
#[derive(Clone)]
pub struct BodyLimit {
    max_bytes: usize,
}

impl BodyLimit {
    /// Create a body limit middleware with the given maximum size in bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes }
    }
}

impl MiddlewareTrait for BodyLimit {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let max = self.max_bytes;

        Box::pin(async move {
            if req.body().len() > max {
                return http::Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .header("content-type", "text/plain; charset=utf-8")
                    .body(Body::full("Payload Too Large"))
                    .unwrap();
            }
            next.run(req).await
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
    async fn allows_body_under_limit() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(100))
                .post("/data", |body: String| async move { body }),
        );

        let resp = client.post("/data").body("hello").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "hello");
    }

    #[tokio::test]
    async fn allows_body_at_exact_limit() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(5))
                .post("/data", |body: String| async move { body }),
        );

        let resp = client.post("/data").body("12345").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "12345");
    }

    #[tokio::test]
    async fn rejects_body_over_limit() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(5))
                .post("/data", |body: String| async move { body }),
        );

        let resp = client.post("/data").body("123456").send().await;
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn empty_body_always_allowed() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(0))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_any_body_with_zero_limit() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(0))
                .post("/data", || async { "ok" }),
        );

        let resp = client.post("/data").body("x").send().await;
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn get_without_body_passes() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(10))
                .get("/", || async { "hello" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "hello");
    }

    #[tokio::test]
    async fn large_limit_allows_reasonable_body() {
        let client = TestClient::new(
            Router::new()
                .middleware(BodyLimit::new(1024 * 1024)) // 1 MB
                .post("/data", |body: String| async move {
                    format!("len={}", body.len())
                }),
        );

        let payload = "x".repeat(1000);
        let resp = client.post("/data").body(payload).send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await, "len=1000");
    }
}
