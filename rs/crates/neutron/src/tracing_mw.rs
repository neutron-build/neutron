//! Per-request tracing middleware with W3C Trace Context (traceparent) propagation.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::tracing_mw::{TracingLayer, TraceId};
//! use neutron::extract::Extension;
//!
//! let router = Router::new()
//!     .middleware(TracingLayer)
//!     .get("/", |Extension(trace_id): Extension<TraceId>| async move {
//!         format!("trace: {}", trace_id.0)
//!     });
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use rand::Rng;

use crate::handler::{Request, Response};
use crate::middleware::{MiddlewareTrait, Next};

// ---------------------------------------------------------------------------
// TraceId
// ---------------------------------------------------------------------------

/// A W3C-compatible trace identifier (32 hex characters / 16 bytes).
#[derive(Clone, Debug)]
pub struct TraceId(pub String);

impl TraceId {
    /// Generate a random trace ID (32 hex characters).
    pub fn generate() -> Self {
        let bytes: [u8; 16] = rand::thread_rng().gen();
        let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        TraceId(hex)
    }

    /// Parse a trace ID from a W3C `traceparent` header value.
    ///
    /// Expected format: `00-{32 hex trace_id}-{16 hex span_id}-{2 hex flags}`
    pub fn from_traceparent(header: &str) -> Option<TraceId> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        // Version must be "00"
        if parts[0] != "00" {
            return None;
        }

        let trace_id = parts[1];
        let span_id = parts[2];
        let flags = parts[3];

        // Validate lengths
        if trace_id.len() != 32 || span_id.len() != 16 || flags.len() != 2 {
            return None;
        }

        // Validate hex characters
        if !trace_id.chars().all(|c| c.is_ascii_hexdigit())
            || !span_id.chars().all(|c| c.is_ascii_hexdigit())
            || !flags.chars().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }

        Some(TraceId(trace_id.to_string()))
    }

    /// Format this trace ID as a W3C `traceparent` header value.
    ///
    /// Generates a new random span ID for each call. Uses flags `01` (sampled).
    pub fn to_traceparent(&self) -> String {
        let span_bytes: [u8; 8] = rand::thread_rng().gen();
        let span_id = span_bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        format!("00-{}-{}-01", self.0, span_id)
    }
}

// ---------------------------------------------------------------------------
// TracingLayer middleware
// ---------------------------------------------------------------------------

/// Per-request tracing middleware.
///
/// For each request this middleware:
/// 1. Reads or generates a W3C `traceparent` trace ID
/// 2. Creates a `tracing` span with method, path, and trace ID
/// 3. Stores the [`TraceId`] as a request extension (available via `Extension<TraceId>`)
/// 4. After the response: logs status and duration
/// 5. Adds the `traceparent` header to the response
pub struct TracingLayer;

impl MiddlewareTrait for TracingLayer {
    fn call(&self, mut req: Request, next: Next) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        // Extract or generate trace ID
        let trace_id = req
            .headers()
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .and_then(TraceId::from_traceparent)
            .unwrap_or_else(TraceId::generate);

        // Store trace ID as a request extension for handlers
        req.set_extension(trace_id.clone());

        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        let trace_id_str = trace_id.0.clone();

        Box::pin(async move {
            let span = tracing::info_span!(
                "request",
                method = %method,
                path = %path,
                trace_id = %trace_id_str,
            );
            let _guard = span.enter();

            let start = Instant::now();
            let mut resp = next.run(req).await;
            let duration = start.elapsed();

            let status = resp.status().as_u16();
            tracing::info!(
                status = status,
                duration_ms = duration.as_millis() as u64,
                "response"
            );

            // Add traceparent header to response
            let traceparent = trace_id.to_traceparent();
            if let Ok(value) = traceparent.parse() {
                resp.headers_mut().insert("traceparent", value);
            }

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

    // -----------------------------------------------------------------------
    // TraceId unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn generate_produces_32_hex_chars() {
        let id = TraceId::generate();
        assert_eq!(id.0.len(), 32);
        assert!(id.0.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn parse_valid_traceparent() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let id = TraceId::from_traceparent(header);
        assert!(id.is_some());
        assert_eq!(id.unwrap().0, "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn parse_invalid_traceparent_returns_none() {
        // Wrong number of parts
        assert!(TraceId::from_traceparent("not-a-valid-header").is_none());
        // Wrong version
        assert!(TraceId::from_traceparent(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        .is_none());
        // Too-short trace ID
        assert!(TraceId::from_traceparent("00-4bf92f-00f067aa0ba902b7-01").is_none());
        // Non-hex characters
        assert!(TraceId::from_traceparent(
            "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01"
        )
        .is_none());
        // Empty string
        assert!(TraceId::from_traceparent("").is_none());
    }

    #[test]
    fn generate_traceparent_format_is_valid() {
        let id = TraceId::generate();
        let traceparent = id.to_traceparent();

        let parts: Vec<&str> = traceparent.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00"); // version
        assert_eq!(parts[1].len(), 32); // trace ID
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(parts[2].len(), 16); // span ID
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(parts[3], "01"); // flags (sampled)

        // The trace ID in the header should match the original
        assert_eq!(parts[1], id.0);
    }

    // -----------------------------------------------------------------------
    // Middleware integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn middleware_adds_traceparent_to_response() {
        let client = TestClient::new(
            Router::new()
                .middleware(TracingLayer)
                .get("/", || async { "ok" }),
        );

        let resp = client.get("/").send().await;
        let traceparent = resp.header("traceparent");
        assert!(traceparent.is_some(), "response should have traceparent header");

        // Validate the format
        let value = traceparent.unwrap();
        let parts: Vec<&str> = value.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
        assert_eq!(parts[3], "01");
    }

    #[tokio::test]
    async fn middleware_propagates_incoming_traceparent() {
        let client = TestClient::new(
            Router::new()
                .middleware(TracingLayer)
                .get("/", || async { "ok" }),
        );

        let incoming_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
        let incoming = format!("00-{incoming_trace_id}-00f067aa0ba902b7-01");

        let resp = client
            .get("/")
            .header("traceparent", &incoming)
            .send()
            .await;

        let traceparent = resp.header("traceparent").expect("missing traceparent header");
        let parts: Vec<&str> = traceparent.split('-').collect();

        // The trace ID should be preserved from the incoming header
        assert_eq!(parts[1], incoming_trace_id);
        // But the span ID should be newly generated (different from incoming)
        assert_ne!(parts[2], "00f067aa0ba902b7");
    }

    #[tokio::test]
    async fn middleware_generates_traceparent_when_missing() {
        let client = TestClient::new(
            Router::new()
                .middleware(TracingLayer)
                .get("/", || async { "ok" }),
        );

        // First request — no incoming traceparent
        let resp1 = client.get("/").send().await;
        let tp1 = resp1.header("traceparent").expect("missing traceparent");

        // Second request — also no incoming traceparent
        let resp2 = client.get("/").send().await;
        let tp2 = resp2.header("traceparent").expect("missing traceparent");

        // Both should be valid but have different trace IDs (random)
        let parts1: Vec<&str> = tp1.split('-').collect();
        let parts2: Vec<&str> = tp2.split('-').collect();
        assert_eq!(parts1.len(), 4);
        assert_eq!(parts2.len(), 4);
        assert_ne!(parts1[1], parts2[1], "different requests should get different trace IDs");
    }
}
