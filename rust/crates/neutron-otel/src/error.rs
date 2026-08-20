use std::fmt;

/// Errors produced by the neutron-otel exporter.
#[derive(Debug)]
pub enum OtelError {
    /// Failed to connect to the OTLP endpoint.
    Connect(String),
    /// Failed to export spans (non-2xx response or I/O error).
    Export(String),
    /// Invalid configuration supplied by the caller.
    Config(String),
}

impl fmt::Display for OtelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OtelError::Connect(msg) => write!(f, "OtelError::Connect: {msg}"),
            OtelError::Export(msg)  => write!(f, "OtelError::Export: {msg}"),
            OtelError::Config(msg)  => write!(f, "OtelError::Config: {msg}"),
        }
    }
}

impl std::error::Error for OtelError {}
