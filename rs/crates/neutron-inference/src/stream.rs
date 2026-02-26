//! `InferStream` — an SSE streaming response that proxies inference chunks.
//!
//! Used when a handler wants to forward a token stream from the inference
//! server directly to the browser/client.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use http::StatusCode;
use http_body::Body as HttpBody;
use http_body::Frame;
use http_body::SizeHint;
use neutron::handler::{Body, IntoResponse, Response};

use crate::error::InferError;
use crate::response::InferenceChunk;

// ---------------------------------------------------------------------------
// SseBody — wraps a stream of InferenceChunk into an SSE byte stream
// ---------------------------------------------------------------------------

type ChunkStream = Pin<Box<dyn Stream<Item = Result<InferenceChunk, InferError>> + Send>>;

struct SseBody {
    inner: ChunkStream,
    done:  bool,
}

impl SseBody {
    fn format_chunk(chunk: &InferenceChunk) -> Bytes {
        let json = serde_json::to_string(chunk).unwrap_or_default();
        Bytes::from(format!("data: {json}\n\n"))
    }
}

impl HttpBody for SseBody {
    type Data  = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if self.done {
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                // Send SSE stream terminator.
                Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"data: [DONE]\n\n")))))
            }
            Poll::Ready(Some(Ok(chunk))) => {
                if chunk.done {
                    self.done = true;
                }
                Poll::Ready(Some(Ok(Frame::data(Self::format_chunk(&chunk)))))
            }
            Poll::Ready(Some(Err(e))) => {
                self.done = true;
                // Emit the error as a SSE error event and terminate.
                let msg = format!("event: error\ndata: {e}\n\n");
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(msg)))))
            }
        }
    }

    fn is_end_stream(&self) -> bool { self.done }
    fn size_hint(&self)   -> SizeHint { SizeHint::default() }
}

// ---------------------------------------------------------------------------
// InferStream — the public type
// ---------------------------------------------------------------------------

/// An HTTP response that streams inference chunks as Server-Sent Events.
///
/// ```rust,ignore
/// async fn generate(
///     client: State<InferenceClient>,
///     Json(req): Json<InferenceRequest>,
/// ) -> InferStream {
///     InferStream::new(client.stream(req.stream()).await)
/// }
/// ```
pub struct InferStream(ChunkStream);

impl InferStream {
    /// Create from any stream of inference chunks.
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<InferenceChunk, InferError>> + Send + 'static,
    {
        Self(Box::pin(stream))
    }
}

impl IntoResponse for InferStream {
    fn into_response(self) -> Response {
        http::Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("x-accel-buffering", "no") // disable nginx buffering
            .body(Body::stream(SseBody { inner: self.0, done: false }))
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::FinishReason;
    use futures_util::stream;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn sse_body_formats_chunks_and_terminates() {
        let chunks = vec![
            Ok(InferenceChunk::data("Hello")),
            Ok(InferenceChunk::data(" world")),
            Ok(InferenceChunk::final_chunk("!", FinishReason::Stop)),
        ];
        let s = InferStream::new(stream::iter(chunks));
        let resp = s.into_response();

        assert_eq!(resp.headers().get("content-type").unwrap(), "text/event-stream");

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();

        // Each chunk produces a "data: {...}\n\n" line.
        assert!(text.contains("data: "));
        // Last chunk contains done=true.
        assert!(text.contains("\"done\":true"));
    }

    #[tokio::test]
    async fn sse_body_emits_error_event_on_error() {
        let chunks: Vec<Result<InferenceChunk, InferError>> =
            vec![Err(InferError::Protocol("bad frame".to_string()))];
        let s = InferStream::new(stream::iter(chunks));
        let body = s.into_response().into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("event: error"));
    }
}
