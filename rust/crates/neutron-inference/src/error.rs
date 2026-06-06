//! Error type for neutron-inference.

use std::fmt;

#[derive(Debug)]
pub enum InferError {
    /// HTTP transport error (connection refused, timeout, etc.).
    Http(String),
    /// The inference server returned a non-200 status.
    Status(u16, String),
    /// Failed to parse the server's JSON response.
    Json(serde_json::Error),
    /// An SSE stream frame was malformed.
    Protocol(String),
}

impl fmt::Display for InferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(e)          => write!(f, "Inference HTTP error: {e}"),
            Self::Status(s, body)  => write!(f, "Inference server returned {s}: {body}"),
            Self::Json(e)          => write!(f, "Inference JSON parse error: {e}"),
            Self::Protocol(msg)    => write!(f, "Inference protocol error: {msg}"),
        }
    }
}

impl std::error::Error for InferError {}

impl From<serde_json::Error> for InferError {
    fn from(e: serde_json::Error) -> Self { Self::Json(e) }
}
