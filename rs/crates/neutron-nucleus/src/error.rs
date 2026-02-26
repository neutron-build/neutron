//! Error type for neutron-nucleus.

use std::fmt;

/// All errors that can arise when using [`NucleusPool`](crate::NucleusPool) or [`Db`](crate::Db).
#[derive(Debug)]
pub enum NucleusError {
    /// TCP / TLS / authentication failure establishing a new connection.
    Connect(tokio_postgres::Error),
    /// A query or execute call was rejected by the server.
    Query(tokio_postgres::Error),
    /// All pool slots are in use and the semaphore was closed (shutdown).
    PoolExhausted,
    /// A migration step failed.
    Migration { step: String, source: Box<NucleusError> },
    /// I/O error reading migration files.
    Io(std::io::Error),
}

impl fmt::Display for NucleusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(e)              => write!(f, "Nucleus connect: {e}"),
            Self::Query(e)               => write!(f, "Nucleus query: {e}"),
            Self::PoolExhausted          => write!(f, "Nucleus pool exhausted"),
            Self::Migration { step, source } => write!(f, "Migration '{step}' failed: {source}"),
            Self::Io(e)                  => write!(f, "Nucleus I/O: {e}"),
        }
    }
}

impl std::error::Error for NucleusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(e)                 => Some(e),
            Self::Query(e)                   => Some(e),
            Self::Migration { source, .. }   => Some(source.as_ref()),
            Self::Io(e)                      => Some(e),
            Self::PoolExhausted              => None,
        }
    }
}
