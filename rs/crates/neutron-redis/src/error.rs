//! Error type for neutron-redis.

use std::fmt;

/// Error produced by a Redis operation.
#[derive(Debug)]
pub enum RedisError {
    /// Connection or command error from the `redis` crate.
    Redis(redis::RedisError),
    /// JSON serialisation/deserialisation error.
    Serialisation(serde_json::Error),
    /// Tried to call Redis from outside a Tokio runtime context.
    NoRuntime,
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Redis(e)         => write!(f, "redis error: {e}"),
            Self::Serialisation(e) => write!(f, "serialisation error: {e}"),
            Self::NoRuntime        => write!(f, "no tokio runtime available"),
        }
    }
}

impl std::error::Error for RedisError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Redis(e)         => Some(e),
            Self::Serialisation(e) => Some(e),
            Self::NoRuntime        => None,
        }
    }
}

impl From<redis::RedisError> for RedisError {
    fn from(e: redis::RedisError) -> Self { Self::Redis(e) }
}

impl From<serde_json::Error> for RedisError {
    fn from(e: serde_json::Error) -> Self { Self::Serialisation(e) }
}
