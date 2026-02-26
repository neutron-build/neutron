//! Unique request ID middleware.
//!
//! Assigns each request a unique `x-request-id` header (or preserves an
//! existing one from an upstream load balancer) and copies it to the response.
//!
//! ```rust,ignore
//! Router::new().middleware(RequestId::new())
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::handler::{Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID.
///
/// Format: `{timestamp_hex}-{counter_hex}` (e.g., `0018d5a3c1b00a42-00000001`).
fn generate_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{ts:016x}-{count:08x}")
}

/// Middleware that assigns a unique request ID to each request.
///
/// - If the request already has an `x-request-id` header (e.g., from a load balancer),
///   that value is preserved.
/// - Otherwise, a new ID is generated.
/// - The ID is always set on the response as `x-request-id`.
///
/// # Example
///
/// ```rust,ignore
/// use neutron::prelude::*;
///
/// let router = Router::new()
///     .middleware(RequestId::new())
///     .get("/", handler);
/// ```
#[derive(Clone)]
pub struct RequestId {
    header_name: &'static str,
}

impl RequestId {
    /// Create a new RequestId middleware using the default header name `x-request-id`.
    pub fn new() -> Self {
        Self {
            header_name: "x-request-id",
        }
    }

    /// Create a new RequestId middleware with a custom header name.
    pub fn header(header_name: &'static str) -> Self {
        Self { header_name }
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl MiddlewareTrait for RequestId {
    fn call(&self, req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let header_name = self.header_name;

        // Use existing request ID or generate a new one
        let id = req
            .headers()
            .get(header_name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(generate_id);

        Box::pin(async move {
            let mut resp = next.run(req).await;
            resp.headers_mut()
                .insert(header_name, id.parse().unwrap());
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
    use http::StatusCode;

    #[tokio::test]
    async fn generates_request_id_on_response() {
        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::new())
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        let id = resp.header("x-request-id");
        assert!(id.is_some(), "response should have x-request-id header");
        assert!(!id.unwrap().is_empty());
    }

    #[tokio::test]
    async fn preserves_incoming_request_id() {
        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::new())
                .get("/", || async { "ok" }),
        );

        let resp = client
            .get("/")
            .header("x-request-id", "from-loadbalancer-123")
            .send()
            .await;

        assert_eq!(resp.header("x-request-id").unwrap(), "from-loadbalancer-123");
    }

    #[tokio::test]
    async fn generates_unique_ids() {
        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::new())
                .get("/", || async { "ok" }),
        );

        let resp1 = client.get("/").send().await;
        let resp2 = client.get("/").send().await;
        let resp3 = client.get("/").send().await;

        let id1 = resp1.header("x-request-id").unwrap().to_string();
        let id2 = resp2.header("x-request-id").unwrap().to_string();
        let id3 = resp3.header("x-request-id").unwrap().to_string();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn custom_header_name() {
        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::header("x-trace-id"))
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        assert!(resp.header("x-trace-id").is_some());
        assert!(resp.header("x-request-id").is_none());
    }

    #[tokio::test]
    async fn custom_header_preserves_incoming() {
        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::header("x-trace-id"))
                .get("/", || async { "ok" }),
        );

        let resp = client
            .get("/")
            .header("x-trace-id", "my-trace-abc")
            .send()
            .await;

        assert_eq!(resp.header("x-trace-id").unwrap(), "my-trace-abc");
    }

    #[tokio::test]
    async fn id_format_is_hex_dash_hex() {
        let client = TestClient::new(
            Router::new()
                .middleware(RequestId::new())
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        let id = resp.header("x-request-id").unwrap().to_string();

        // Format: 16 hex chars, dash, 8 hex chars
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 16);
        assert_eq!(parts[1].len(), 8);
        assert!(parts[0].chars().all(|c| c.is_ascii_hexdigit()));
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
    }
}
