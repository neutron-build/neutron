//! Server-Sent Events (SSE) support.
//!
//! # Example
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron::sse::{Sse, SseEvent};
//! use tokio_stream::StreamExt;
//!
//! async fn events() -> Sse {
//!     let stream = tokio_stream::wrappers::IntervalStream::new(
//!         tokio::time::interval(std::time::Duration::from_secs(1)),
//!     )
//!     .map(|_| SseEvent::new().data("tick"));
//!
//!     Sse::new(stream)
//! }
//!
//! let router = Router::new().get("/events", events);
//! ```

use std::convert::Infallible;
use std::fmt::Write;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use http_body::Frame;
use tokio_stream::Stream;

use crate::handler::{Body, IntoResponse, Response};

/// A single SSE event.
///
/// At least one of `data`, `event`, `id`, or `comment` must be set
/// for the event to produce any output.
#[derive(Default, Clone)]
pub struct SseEvent {
    data: Option<String>,
    event: Option<String>,
    id: Option<String>,
    retry: Option<u64>,
    comment: Option<String>,
}

impl SseEvent {
    /// Create a new empty SSE event.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the `data:` field. Newlines in the data are handled automatically
    /// (each line becomes a separate `data:` line per the SSE spec).
    pub fn data(mut self, data: impl Into<String>) -> Self {
        self.data = Some(data.into());
        self
    }

    /// Set the `event:` field (event type).
    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }

    /// Set the `id:` field (last event ID).
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the `retry:` field (reconnection time in milliseconds).
    pub fn retry(mut self, millis: u64) -> Self {
        self.retry = Some(millis);
        self
    }

    /// Set a comment line (`:` prefix, useful for keep-alive).
    pub fn comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Format this event as an SSE text block.
    fn to_sse_string(&self) -> String {
        let mut buf = String::new();

        if let Some(ref comment) = self.comment {
            for line in comment.lines() {
                let _ = writeln!(buf, ":{line}");
            }
        }

        if let Some(ref event) = self.event {
            let _ = writeln!(buf, "event:{event}");
        }

        if let Some(ref data) = self.data {
            for line in data.lines() {
                let _ = writeln!(buf, "data:{line}");
            }
            // Handle edge case: if data ends with newline, emit extra data: line
            if data.ends_with('\n') {
                let _ = writeln!(buf, "data:");
            }
            // Handle empty data
            if data.is_empty() {
                let _ = writeln!(buf, "data:");
            }
        }

        if let Some(ref id) = self.id {
            let _ = writeln!(buf, "id:{id}");
        }

        if let Some(retry) = self.retry {
            let _ = writeln!(buf, "retry:{retry}");
        }

        // Terminate event with blank line
        buf.push('\n');
        buf
    }
}

/// SSE response that streams events to the client.
///
/// Implements [`IntoResponse`] so it can be returned directly from handlers.
pub struct Sse {
    stream: Pin<Box<dyn Stream<Item = SseEvent> + Send>>,
    keep_alive: Option<Duration>,
}

impl Sse {
    /// Create a new SSE response from a stream of events.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = SseEvent> + Send + 'static,
    {
        Self {
            stream: Box::pin(stream),
            keep_alive: None,
        }
    }

    /// Enable keep-alive comments at the given interval.
    ///
    /// Sends a `: keep-alive` comment periodically to prevent proxies
    /// and load balancers from closing idle connections.
    pub fn keep_alive(mut self, interval: Duration) -> Self {
        self.keep_alive = Some(interval);
        self
    }
}

/// Internal stream that yields SSE frames, optionally with keep-alive.
struct SseStream {
    inner: Pin<Box<dyn Stream<Item = SseEvent> + Send>>,
    keep_alive: Option<Pin<Box<tokio::time::Interval>>>,
}

impl Stream for SseStream {
    type Item = Result<Frame<Bytes>, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Check for events from the inner stream first.
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(event)) => {
                let text = event.to_sse_string();
                return Poll::Ready(Some(Ok(Frame::data(Bytes::from(text)))));
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }

        // If no event is ready, check keep-alive timer.
        if let Some(ref mut interval) = self.keep_alive {
            match interval.as_mut().poll_tick(cx) {
                Poll::Ready(_) => {
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from(": keep-alive\n\n")))));
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

impl IntoResponse for Sse {
    fn into_response(self) -> Response {
        let keep_alive = self.keep_alive.map(|d| {
            let mut interval = tokio::time::interval(d);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            Box::pin(interval)
        });

        let sse_stream = SseStream {
            inner: self.stream,
            keep_alive,
        };

        let body = Body::stream(http_body_util::StreamBody::new(sse_stream));

        http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("connection", "keep-alive")
            .body(body)
            .unwrap()
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
    // SseEvent formatting
    // -----------------------------------------------------------------------

    #[test]
    fn event_data_only() {
        let event = SseEvent::new().data("hello");
        assert_eq!(event.to_sse_string(), "data:hello\n\n");
    }

    #[test]
    fn event_with_type() {
        let event = SseEvent::new().event("message").data("hello");
        assert_eq!(event.to_sse_string(), "event:message\ndata:hello\n\n");
    }

    #[test]
    fn event_with_id() {
        let event = SseEvent::new().data("hello").id("42");
        assert_eq!(event.to_sse_string(), "data:hello\nid:42\n\n");
    }

    #[test]
    fn event_with_retry() {
        let event = SseEvent::new().data("hello").retry(3000);
        assert_eq!(event.to_sse_string(), "data:hello\nretry:3000\n\n");
    }

    #[test]
    fn event_multiline_data() {
        let event = SseEvent::new().data("line1\nline2\nline3");
        assert_eq!(
            event.to_sse_string(),
            "data:line1\ndata:line2\ndata:line3\n\n"
        );
    }

    #[test]
    fn event_comment() {
        let event = SseEvent::new().comment("ping");
        assert_eq!(event.to_sse_string(), ":ping\n\n");
    }

    #[test]
    fn event_empty_data() {
        let event = SseEvent::new().data("");
        assert_eq!(event.to_sse_string(), "data:\n\n");
    }

    #[test]
    fn event_all_fields() {
        let event = SseEvent::new()
            .comment("test")
            .event("update")
            .data("payload")
            .id("1")
            .retry(5000);
        let text = event.to_sse_string();
        assert!(text.contains(":test\n"));
        assert!(text.contains("event:update\n"));
        assert!(text.contains("data:payload\n"));
        assert!(text.contains("id:1\n"));
        assert!(text.contains("retry:5000\n"));
        assert!(text.ends_with("\n\n"));
    }

    // -----------------------------------------------------------------------
    // SSE response
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn sse_response_headers() {
        let client = TestClient::new(Router::new().get("/events", || async {
            let stream = tokio_stream::iter(vec![SseEvent::new().data("test")]);
            Sse::new(stream)
        }));

        let resp = client.get("/events").send().await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.header("content-type").unwrap(), "text/event-stream");
        assert_eq!(resp.header("cache-control").unwrap(), "no-cache");
    }

    #[tokio::test]
    async fn sse_single_event() {
        let client = TestClient::new(Router::new().get("/events", || async {
            let stream = tokio_stream::iter(vec![SseEvent::new().data("hello")]);
            Sse::new(stream)
        }));

        let resp = client.get("/events").send().await;
        assert_eq!(resp.text().await, "data:hello\n\n");
    }

    #[tokio::test]
    async fn sse_multiple_events() {
        let client = TestClient::new(Router::new().get("/events", || async {
            let stream = tokio_stream::iter(vec![
                SseEvent::new().event("msg").data("first"),
                SseEvent::new().event("msg").data("second"),
                SseEvent::new().event("msg").data("third"),
            ]);
            Sse::new(stream)
        }));

        let resp = client.get("/events").send().await;
        let text = resp.text().await;

        assert!(text.contains("data:first\n"));
        assert!(text.contains("data:second\n"));
        assert!(text.contains("data:third\n"));
        assert_eq!(text.matches("event:msg\n").count(), 3);
    }

    #[tokio::test]
    async fn sse_json_data() {
        let client = TestClient::new(Router::new().get("/events", || async {
            let json = serde_json::json!({"count": 42}).to_string();
            let stream = tokio_stream::iter(vec![SseEvent::new().event("update").data(json)]);
            Sse::new(stream)
        }));

        let resp = client.get("/events").send().await;
        let text = resp.text().await;
        assert!(text.contains("event:update\n"));
        assert!(text.contains(r#"data:{"count":42}"#));
    }

    #[tokio::test]
    async fn sse_with_id_and_retry() {
        let client = TestClient::new(Router::new().get("/events", || async {
            let stream = tokio_stream::iter(vec![
                SseEvent::new().data("hello").id("1").retry(5000),
            ]);
            Sse::new(stream)
        }));

        let resp = client.get("/events").send().await;
        let text = resp.text().await;
        assert!(text.contains("data:hello\n"));
        assert!(text.contains("id:1\n"));
        assert!(text.contains("retry:5000\n"));
    }

    #[cfg(feature = "compress")]
    #[tokio::test]
    async fn sse_compression_skipped() {
        use crate::compress::Compress;

        let client = TestClient::new(
            Router::new()
                .middleware(Compress::new())
                .get("/events", || async {
                    let stream =
                        tokio_stream::iter(vec![SseEvent::new().data("a]".repeat(1000))]);
                    Sse::new(stream)
                }),
        );

        let resp = client
            .get("/events")
            .header("accept-encoding", "gzip, br")
            .send()
            .await;

        // Streaming body — compression should be skipped.
        assert!(resp.header("content-encoding").is_none());
        assert_eq!(resp.header("content-type").unwrap(), "text/event-stream");
    }
}
