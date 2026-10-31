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

use tracing::Instrument;

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
            // The span is attached with `Instrument`, NOT a held `enter()`
            // guard: a guard kept across `await` stays entered while the
            // task is suspended, so another request polled on the same
            // thread in the meantime emitted its events into THIS request's
            // span (cross-attributed logs). Instrumentation enters/exits the
            // span around each poll, which is the documented-safe pattern.
            let start = Instant::now();
            let mut resp = next.run(req).instrument(span.clone()).await;
            let duration = start.elapsed();

            let status = resp.status().as_u16();
            span.in_scope(|| {
                tracing::info!(
                    status = status,
                    duration_ms = duration.as_millis() as u64,
                    "response"
                )
            });

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
        assert!(
            traceparent.is_some(),
            "response should have traceparent header"
        );

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

        let traceparent = resp
            .header("traceparent")
            .expect("missing traceparent header");
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
        assert_ne!(
            parts1[1], parts2[1],
            "different requests should get different trace IDs"
        );
    }

    /// Regression (RS-35): the request span used to be kept `enter()`ed
    /// across the `next.run(req).await`, so when two request futures
    /// interleaved on one thread, the second request's events were recorded
    /// inside the FIRST request's (suspended) span — cross-attributed logs.
    /// The span is now attached via `Instrument` (enter/exit per poll). Two
    /// barrier-interleaved requests must emit every event inside their own
    /// span.
    #[test]
    fn interleaved_requests_keep_their_own_spans() {
        // Fresh thread: `tracing` caches callsite interest per thread, so a
        // subscriber installed by an earlier test on a reused harness thread
        // can otherwise make this test's spans (but not its events)
        // invisible — pure test-infrastructure nondeterminism.
        std::thread::Builder::new()
            .name("rs35-interleaved-spans".into())
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                rt.block_on(async {
                    interleaved_requests_keep_their_own_spans_inner().await;
                });
            })
            .expect("spawn test thread")
            .join()
            .expect("test thread panicked");
    }

    #[test]
    #[ignore = "run alone: intentionally installs a process-global subscriber"]
    fn interleaved_capture_with_an_already_installed_global_subscriber() {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::Registry::default());
        interleaved_requests_keep_their_own_spans();
    }

    async fn interleaved_requests_keep_their_own_spans_inner() {
        use crate::extract::Extension;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        struct CaptureLayer(Arc<Capture>);
        impl<S> tracing_subscriber::Layer<S> for CaptureLayer
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                id: &tracing::span::Id,
                ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0.on_new_span(attrs, id, ctx);
            }
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0.on_event(event, ctx);
            }
        }

        #[derive(Default)]
        struct FieldGrab {
            trace_id: Option<String>,
            own_id: Option<String>,
            path: Option<String>,
            status: Option<u64>,
            duration_ms: Option<u64>,
        }
        impl tracing::field::Visit for FieldGrab {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                match field.name() {
                    "status" => self.status = Some(value),
                    "duration_ms" => self.duration_ms = Some(value),
                    _ => {}
                }
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "trace_id" => self.trace_id = Some(value.to_string()),
                    "own_id" => self.own_id = Some(value.to_string()),
                    "path" => self.path = Some(value.to_string()),
                    _ => {}
                }
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                match field.name() {
                    "own_id" => self.own_id = Some(format!("{value:?}")),
                    "trace_id" => self.trace_id = Some(format!("{value:?}")),
                    "path" => self.path = Some(format!("{value:?}")),
                    _ => {}
                }
            }
        }

        #[derive(Default)]
        struct Capture {
            spans: Mutex<HashMap<u64, (String, String)>>, // id -> (trace_id, path)
            events: Mutex<Vec<(Option<String>, Option<u64>, Option<u64>, String, String)>>,
        }
        impl<S> tracing_subscriber::Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                id: &tracing::span::Id,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut v = FieldGrab::default();
                attrs.record(&mut v);
                if let Some(t) = v.trace_id {
                    self.spans
                        .lock()
                        .unwrap()
                        .insert(id.into_u64(), (t, v.path.unwrap_or_default()));
                }
            }
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut v = FieldGrab::default();
                event.record(&mut v);
                let span_context = ctx
                    .current_span()
                    .id()
                    .and_then(|id| self.spans.lock().unwrap().get(&id.into_u64()).cloned())
                    .unwrap_or_default();
                self.events.lock().unwrap().push((
                    v.own_id,
                    v.status,
                    v.duration_ms,
                    span_context.0,
                    span_context.1,
                ));
            }
        }

        let capture = Arc::new(Capture::default());
        let cap_layer = CaptureLayer(Arc::clone(&capture));
        let registry = tracing_subscriber::Registry::default().with(cap_layer);
        // This fixture owns a single runtime thread. Keep the local subscriber
        // guard alive across every poll/assertion, regardless of global ownership.
        let _subscriber = tracing::subscriber::set_default(registry);

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let router = Router::new()
            .middleware(TracingLayer)
            .get("/a", move |Extension(trace): Extension<TraceId>| {
                let b = Arc::clone(&b1);
                async move {
                    tracing::info!(own_id = trace.0.as_str(), "first event a");
                    b.wait().await;
                    tracing::info!(own_id = trace.0.as_str(), "second event a");
                    "a"
                }
            })
            .get("/b", move |Extension(trace): Extension<TraceId>| {
                let b = Arc::clone(&b2);
                async move {
                    tracing::info!(own_id = trace.0.as_str(), "first event b");
                    b.wait().await;
                    tracing::info!(own_id = trace.0.as_str(), "second event b");
                    "b"
                }
            });
        let client = crate::testing::TestClient::new(router);

        // Both requests run on this single-threaded task set; the barrier
        // forces a to suspend with its span "in progress" while b polls.
        let requests = async { tokio::join!(client.get("/a").send(), client.get("/b").send()) };
        tokio::pin!(requests);
        let (r1, r2) = std::future::poll_fn(|cx| {
            let result = requests.as_mut().poll(cx);
            // Instrument must exit after EVERY poll, including Pending/Ready.
            assert!(
                tracing::Span::current().id().is_none(),
                "request span leaked between polls"
            );
            result
        })
        .await;
        assert_eq!(r1.status(), 200);
        assert_eq!(r2.status(), 200);
        let tid_a = TraceId::from_traceparent(r1.header("traceparent").unwrap())
            .unwrap()
            .0;
        let tid_b = TraceId::from_traceparent(r2.header("traceparent").unwrap())
            .unwrap()
            .0;
        assert_ne!(tid_a, tid_b);
        let expected = HashMap::from([("/a", tid_a), ("/b", tid_b)]);
        let events = capture.events.lock().unwrap().clone();
        assert_eq!(
            events.len(),
            6,
            "two handler events and one response per request: {events:?}"
        );
        for (path, tid) in expected {
            let attributed: Vec<_> = events.iter().filter(|event| event.4 == path).collect();
            assert_eq!(attributed.len(), 3, "wrong request attribution: {events:?}");
            let mut handlers = 0;
            let mut responses = 0;
            for (own, status, duration, span_tid, _) in attributed {
                assert_eq!(span_tid, &tid, "wrong request span: {events:?}");
                if let Some(own) = own {
                    handlers += 1;
                    assert_eq!(own, &tid, "handler belongs to a different request");
                    assert!(status.is_none());
                } else {
                    responses += 1;
                    assert_eq!(*status, Some(200));
                    assert!(duration.is_some(), "response duration absent");
                }
            }
            assert_eq!((handlers, responses), (2, 1));
        }

        // Poll one real request to its barrier, then cancel it while suspended.
        let mut canceled = Box::pin(client.get("/a").send());
        std::future::poll_fn(|cx| {
            assert!(canceled.as_mut().poll(cx).is_pending());
            assert!(
                tracing::Span::current().id().is_none(),
                "span leaked after yielding"
            );
            std::task::Poll::Ready(())
        })
        .await;
        drop(canceled);
        assert!(
            tracing::Span::current().id().is_none(),
            "span leaked after cancellation"
        );
    }
}
