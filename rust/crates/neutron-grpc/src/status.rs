//! gRPC status codes and error type.

use http::HeaderMap;
use neutron::handler::{Body, IntoResponse, Response};

use crate::body::GrpcBodyStream;

/// Standard gRPC status codes (https://grpc.github.io/grpc/core/md_doc_statuscodes.html).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum GrpcStatus {
    Ok                 = 0,
    Cancelled          = 1,
    Unknown            = 2,
    InvalidArgument    = 3,
    DeadlineExceeded   = 4,
    NotFound           = 5,
    AlreadyExists      = 6,
    PermissionDenied   = 7,
    ResourceExhausted  = 8,
    FailedPrecondition = 9,
    Aborted            = 10,
    OutOfRange         = 11,
    Unimplemented      = 12,
    Internal           = 13,
    Unavailable        = 14,
    DataLoss           = 15,
    Unauthenticated    = 16,
}

impl GrpcStatus {
    /// Numeric value sent in the `grpc-status` trailer.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Build an error response (HTTP 200 with non-zero `grpc-status` trailer, no data frame).
    pub fn error_response(self, msg: &str) -> Response {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", self.as_u32().to_string().parse().unwrap());
        if let Ok(v) = msg.parse() {
            trailers.insert("grpc-message", v);
        }
        http::Response::builder()
            .status(http::StatusCode::OK)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(Body::stream(GrpcBodyStream::error(trailers)))
            .unwrap()
    }
}

impl std::fmt::Display for GrpcStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_u32())
    }
}

/// A gRPC error with a status code and human-readable message.
///
/// Implements `IntoResponse` — return it from a handler or use `?` with a
/// `Result<GrpcResponse, GrpcError>` return type.
#[derive(Debug)]
pub struct GrpcError {
    pub status: GrpcStatus,
    pub message: String,
}

impl GrpcError {
    pub fn new(status: GrpcStatus, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(GrpcStatus::NotFound, msg)
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(GrpcStatus::InvalidArgument, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(GrpcStatus::Internal, msg)
    }

    pub fn unimplemented(msg: impl Into<String>) -> Self {
        Self::new(GrpcStatus::Unimplemented, msg)
    }

    pub fn unauthenticated(msg: impl Into<String>) -> Self {
        Self::new(GrpcStatus::Unauthenticated, msg)
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self::new(GrpcStatus::PermissionDenied, msg)
    }
}

impl IntoResponse for GrpcError {
    fn into_response(self) -> Response {
        self.status.error_response(&self.message)
    }
}

impl std::fmt::Display for GrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gRPC {} ({}): {}", self.status, self.status.as_u32(), self.message)
    }
}

impl std::error::Error for GrpcError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes_as_u32() {
        assert_eq!(GrpcStatus::Ok.as_u32(), 0);
        assert_eq!(GrpcStatus::Cancelled.as_u32(), 1);
        assert_eq!(GrpcStatus::NotFound.as_u32(), 5);
        assert_eq!(GrpcStatus::Unimplemented.as_u32(), 12);
        assert_eq!(GrpcStatus::Unauthenticated.as_u32(), 16);
    }

    #[test]
    fn status_display() {
        assert_eq!(GrpcStatus::Ok.to_string(), "0");
        assert_eq!(GrpcStatus::NotFound.to_string(), "5");
    }

    #[test]
    fn grpc_error_constructors() {
        let e = GrpcError::not_found("user 42 not found");
        assert_eq!(e.status, GrpcStatus::NotFound);
        assert_eq!(e.message, "user 42 not found");

        let e = GrpcError::unauthenticated("token expired");
        assert_eq!(e.status, GrpcStatus::Unauthenticated);
    }

    #[tokio::test]
    async fn error_response_is_http_200_with_grpc_status_trailer() {
        use http_body_util::BodyExt;

        let resp = GrpcStatus::NotFound.error_response("not here");

        // gRPC errors are always HTTP 200
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/grpc"
        );

        // Collect all frames including trailers
        let (_, body) = resp.into_parts();
        let collected = body.collect().await.unwrap();
        let trailers = collected.trailers().cloned().unwrap_or_default();

        assert_eq!(trailers.get("grpc-status").unwrap(), "5"); // NotFound
        assert_eq!(trailers.get("grpc-message").unwrap(), "not here");
    }
}
