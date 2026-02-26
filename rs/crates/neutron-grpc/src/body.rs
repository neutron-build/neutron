//! gRPC response body with data frame + HTTP/2 trailers.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body as HttpBody, Frame, SizeHint};

// ---------------------------------------------------------------------------
// Wire-format helpers
// ---------------------------------------------------------------------------

/// Encode a message payload into the gRPC wire format:
/// `[compressed(1)][length_be(4)][message bytes]`
pub fn frame_message(msg: impl Into<Bytes>) -> Bytes {
    let msg = msg.into();
    let mut out = Vec::with_capacity(5 + msg.len());
    out.push(0u8); // not compressed
    out.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    out.extend_from_slice(&msg);
    Bytes::from(out)
}

/// Decode the 5-byte gRPC wire header and return `(message_bytes, compressed)`.
///
/// Returns `None` if the buffer is too short or the declared length exceeds the data.
pub fn unframe_message(data: &[u8]) -> Option<(&[u8], bool)> {
    if data.len() < 5 {
        return None;
    }
    let compressed = data[0] != 0;
    let msg_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if data.len() < 5 + msg_len {
        return None;
    }
    Some((&data[5..5 + msg_len], compressed))
}

// ---------------------------------------------------------------------------
// GrpcBodyStream
// ---------------------------------------------------------------------------

/// Streaming HTTP body that emits one gRPC data frame followed by trailers.
///
/// This is the mechanism gRPC uses over HTTP/2 to deliver `grpc-status` and
/// `grpc-message` trailers after the response body.
pub struct GrpcBodyStream {
    data:     Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl GrpcBodyStream {
    /// Body with a framed message payload and `grpc-status: 0` trailer.
    pub fn ok(framed_message: Bytes) -> Self {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        Self {
            data: if framed_message.is_empty() { None } else { Some(framed_message) },
            trailers: Some(trailers),
        }
    }

    /// Body with only trailers (no data frame) — used for gRPC errors.
    pub fn error(trailers: HeaderMap) -> Self {
        Self { data: None, trailers: Some(trailers) }
    }

    /// Body with a framed message and a custom trailer set.
    pub fn with_trailers(framed_message: Bytes, trailers: HeaderMap) -> Self {
        Self {
            data: if framed_message.is_empty() { None } else { Some(framed_message) },
            trailers: Some(trailers),
        }
    }
}

impl HttpBody for GrpcBodyStream {
    type Data  = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut(); // safe: GrpcBodyStream is Unpin

        if let Some(data) = this.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = this.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none() && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn frame_roundtrip() {
        let msg = b"hello world";
        let framed = frame_message(Bytes::from_static(msg));

        assert_eq!(framed.len(), 5 + msg.len());
        assert_eq!(framed[0], 0); // not compressed

        let (decoded, compressed) = unframe_message(&framed).unwrap();
        assert!(!compressed);
        assert_eq!(decoded, msg);
    }

    #[test]
    fn frame_empty_message() {
        let framed = frame_message(Bytes::new());
        assert_eq!(&framed[..5], &[0, 0, 0, 0, 0]);
        let (decoded, _) = unframe_message(&framed).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn unframe_too_short_returns_none() {
        assert!(unframe_message(&[]).is_none());
        assert!(unframe_message(&[0, 0, 0, 0]).is_none()); // only 4 bytes
    }

    #[test]
    fn unframe_truncated_message_returns_none() {
        // Header says 10 bytes but only 3 present
        let data = [0u8, 0, 0, 0, 10, 1, 2, 3];
        assert!(unframe_message(&data).is_none());
    }

    #[tokio::test]
    async fn ok_body_emits_data_then_grpc_status_trailer() {
        let framed = frame_message(b"payload".as_slice());
        let body = GrpcBodyStream::ok(framed.clone());

        let collected = body.collect().await.unwrap();
        let trailers = collected.trailers().cloned().unwrap_or_default();
        let data = collected.to_bytes();

        assert_eq!(data, framed);
        assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    }

    #[tokio::test]
    async fn error_body_emits_only_trailers() {
        let mut t = HeaderMap::new();
        t.insert("grpc-status", "5".parse().unwrap());
        t.insert("grpc-message", "not found".parse().unwrap());

        let body = GrpcBodyStream::error(t);
        let collected = body.collect().await.unwrap();

        // Get trailers before consuming (to_bytes takes self)
        let trailers = collected.trailers().cloned().unwrap_or_default();
        assert_eq!(trailers.get("grpc-status").unwrap(), "5");
        // data frame should be empty (no message for error responses)
        let data = collected.to_bytes();
        assert!(data.is_empty());
    }

    #[test]
    fn is_end_stream_when_empty() {
        let body = GrpcBodyStream { data: None, trailers: None };
        assert!(body.is_end_stream());
    }
}
