//! Storage error type.

use std::fmt;

/// Errors returned by `StorageClient` operations.
#[derive(Debug)]
pub enum StorageError {
    /// The HTTP/TLS layer could not connect.
    Connect(String),
    /// The provider returned a non-2xx response.
    Status {
        code: u16,
        body: String,
    },
    /// Failed to build or sign a request.
    Sign(String),
    /// Failed to parse a provider response (e.g. malformed XML).
    Parse(String),
    /// Configuration is invalid.
    Config(String),
    /// I/O error during data transfer.
    Io(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Connect(s)          => write!(f, "storage connect error: {s}"),
            StorageError::Status { code, body } => write!(f, "storage HTTP {code}: {body}"),
            StorageError::Sign(s)             => write!(f, "storage signing error: {s}"),
            StorageError::Parse(s)            => write!(f, "storage parse error: {s}"),
            StorageError::Config(s)           => write!(f, "storage config error: {s}"),
            StorageError::Io(s)               => write!(f, "storage I/O error: {s}"),
        }
    }
}

impl std::error::Error for StorageError {}
