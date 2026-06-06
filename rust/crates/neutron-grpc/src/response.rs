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
    pub status:         GrpcStatus,
    pub message:        Option<Bytes>,
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
        Self { status, message: None, status_message: Some(text.into()) }
    }

    /// Add a human-readable status message to any response.
    pub fn with_status_message(mut self, text: impl Into<String>) -> Self {
        self.status_message = Some(text.into());
        self
    }
}

impl IntoResponse for GrpcResponse {
    fn into_response(self) -> Response {
        let framed = match self.message {
            Some(msg) if !msg.is_empty() => frame_message(msg),
            _ => Bytes::new(),
        };

        let mut trailers = HeaderMap::new();
        trailers.insert(
            "grpc-status",
            self.status.as_u32().to_string().parse().unwrap(),
        );
        if let Some(text) = self.status_message {
            if let Ok(v) = text.parse() {
                trailers.insert("grpc-message", v);
            }
        }

        http::Response::builder()
            .status(http::StatusCode::OK)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Body::stream(GrpcBodyStream::with_trailers(framed, trailers)))
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use crate::body::unframe_message;

    #[tokio::test]
    async fn ok_response_is_http_200_grpc() {
        let resp = GrpcResponse::ok(b"payload".as_slice()).into_response();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers().get("content-type").unwrap(), "application/grpc");
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
