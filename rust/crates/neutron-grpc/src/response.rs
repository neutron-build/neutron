//! gRPC response type.

use bytes::Bytes;
use http::HeaderMap;
use neutron::handler::{Body, IntoResponse, Response};

use crate::body::{frame_message, GrpcBodyStream};
use crate::status::GrpcStatus;

/// gRPC response — serializes to HTTP 200 with a 5-byte-framed body and
/// `grpc-status` / `grpc-message` trailers.
///
/// ```rust,ignore
/// async fn get_user(GrpcRequest(payload): GrpcRequest) -> GrpcResponse {
///     let req = GetUserRequest::decode(payload.as_ref()).unwrap();
///     match db.find_user(req.id).await {
///         Ok(user) => GrpcResponse::ok(user.encode_to_vec()),
///         Err(_)   => GrpcResponse::error(GrpcStatus::NotFound, "user not found"),
///     }
/// }
/// ```
///
/// For fallible handlers, call `.unwrap_or_else(|e| e.into_response())` to convert.
pub struct GrpcResponse {
    pub status: GrpcStatus,
    pub message: Option<Bytes>,
    pub status_message: Option<String>,
}

impl GrpcResponse {
    /// Successful response with a message payload.
    pub fn ok(message: impl Into<Bytes>) -> Self {
        Self {
            status: GrpcStatus::Ok,
            message: Some(message.into()),
            status_message: None,
        }
    }

    /// Error response with no data frame.
    pub fn error(status: GrpcStatus, text: impl Into<String>) -> Self {
        Self {
            status,
            message: None,
            status_message: Some(text.into()),
        }
    }

    /// Add a human-readable status message to any response.
    pub fn with_status_message(mut self, text: impl Into<String>) -> Self {
        self.status_message = Some(text.into());
        self
    }
}

/// Percent-encode a `grpc-message` trailer value per the gRPC HTTP/2 spec:
/// the header is restricted to visible ASCII (0x20-0x7E), so any byte
/// outside that range — and `%` itself — is emitted as `%HH`.
fn percent_encode_grpc_message(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        match b {
            0x20..=0x7e if b != b'%' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl IntoResponse for GrpcResponse {
    fn into_response(self) -> Response {
        // Every `Some(message)` is framed — INCLUDING a zero-byte one. An
        // empty protobuf response is a valid successful message and must be
        // emitted as a 5-byte zero-length frame; dropping it (the old
        // `!msg.is_empty()` guard) silently removed the message body from
        // successful unary responses. `None` means "no message at all".
        let framed = match self.message {
            Some(msg) => frame_message(msg),
            None => Bytes::new(),
        };

        let mut trailers = HeaderMap::new();
        trailers.insert(
            "grpc-status",
            self.status.as_u32().to_string().parse().unwrap(),
        );
        if let Some(text) = self.status_message {
            if let Ok(v) = percent_encode_grpc_message(&text).parse() {
                trailers.insert("grpc-message", v);
            }
        }

        http::Response::builder()
            .status(http::StatusCode::OK)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Body::stream(GrpcBodyStream::with_trailers(
                framed, trailers,
            )))
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::unframe_message;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn ok_response_is_http_200_grpc() {
        let resp = GrpcResponse::ok(b"payload".as_slice()).into_response();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/grpc"
        );
    }

    #[tokio::test]
    async fn ok_response_data_frame_is_framed() {
        let payload = b"test message";
        let resp = GrpcResponse::ok(payload.as_slice()).into_response();

        let (_, body) = resp.into_parts();
        let collected = body.collect().await.unwrap();
        let data = collected.to_bytes();

        let (decoded, compressed) = unframe_message(&data).unwrap();
        assert!(!compressed);
        assert_eq!(decoded, payload);
    }

    /// Regression (RS-34): a successful EMPTY message (an empty protobuf
    /// response) must still be emitted as a 5-byte zero-length frame — the
    /// old `!msg.is_empty()` guard silently removed it.
    #[tokio::test]
    async fn ok_response_empty_message_is_framed_as_five_bytes() {
        let resp = GrpcResponse::ok(Bytes::new()).into_response();
        let (_, body) = resp.into_parts();
        let data = body.collect().await.unwrap().to_bytes();
        assert_eq!(data.as_ref(), [0u8, 0, 0, 0, 0]);
        let (decoded, compressed) = unframe_message(&data).unwrap();
        assert!(decoded.is_empty());
        assert!(!compressed);
    }

    /// `grpc-message` is percent-encoded per the gRPC HTTP/2 spec so that
    /// non-ASCII status messages survive the visible-ASCII trailer rule.
    #[tokio::test]
    async fn status_message_is_percent_encoded() {
        let resp = GrpcResponse::error(GrpcStatus::NotFound, "café not found").into_response();
        // Inspect trailers via the stream
        let (_, body) = resp.into_parts();
        let collected = body.collect().await.unwrap();
        if let Some(trailers) = collected.trailers() {
            let msg = trailers.get("grpc-message").unwrap().to_str().unwrap();
            assert_eq!(msg, "caf%C3%A9 not found");
        }
    }

    #[test]
    fn percent_encode_grpc_message_table() {
        assert_eq!(percent_encode_grpc_message("plain"), "plain");
        assert_eq!(percent_encode_grpc_message("100%"), "100%25");
        assert_eq!(percent_encode_grpc_message("é"), "%C3%A9");
        assert_eq!(percent_encode_grpc_message("a b~c"), "a b~c");
    }

    #[tokio::test]
    async fn ok_response_has_grpc_status_0_trailer() {
        let resp = GrpcResponse::ok(b"ok".as_slice()).into_response();
        let (_, body) = resp.into_parts();
        let collected = body.collect().await.unwrap();
        let trailers = collected.trailers().cloned().unwrap_or_default();
        assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    }

    #[tokio::test]
    async fn error_response_has_correct_status_trailer() {
        let resp = GrpcResponse::error(GrpcStatus::NotFound, "not here").into_response();
        let (_, body) = resp.into_parts();
        let collected = body.collect().await.unwrap();
        let trailers = collected.trailers().cloned().unwrap_or_default();
        assert_eq!(trailers.get("grpc-status").unwrap(), "5"); // NotFound
        assert_eq!(trailers.get("grpc-message").unwrap(), "not here");
    }
}
