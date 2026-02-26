//! Structured application error type.
//!
//! [`AppError`] pairs an HTTP status code with a message and serializes to
//! a JSON `{"error": {"status": 404, "message": "..."}}` response body.
//!
//! ```rust,ignore
//! async fn handler() -> Result<String, AppError> {
//!     Err(AppError::not_found())
//! }
//! ```

use http::StatusCode;
use std::fmt;

use crate::handler::{Body, IntoResponse, Response};

/// Application error with an associated HTTP status code.
#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
}

impl AppError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "Not found")
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status.as_u16(), self.message)
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // With JSON: serialize to {"error": {"status": N, "message": "..."}}
        #[cfg(feature = "json")]
        {
            let body = serde_json::json!({
                "error": {
                    "status": self.status.as_u16(),
                    "message": self.message,
                }
            });
            let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
            http::Response::builder()
                .status(self.status)
                .header("content-type", "application/json")
                .body(Body::full(body_bytes))
                .unwrap()
        }
        // Without JSON: plain text fallback
        #[cfg(not(feature = "json"))]
        {
            http::Response::builder()
                .status(self.status)
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::full(format!("{}: {}", self.status.as_u16(), self.message)))
                .unwrap()
        }
    }
}
