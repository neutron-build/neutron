//! Error type for neutron-postgres.

use std::fmt;

/// All errors that can arise when using [`PgPool`](crate::PgPool) or [`Db`](crate::Db).
#[derive(Debug)]
pub enum PgError {
    /// TCP / TLS / authentication failure establishing a new connection.
    Connect(tokio_postgres::Error),
    /// A query or execute call was rejected by the server.
    Query(tokio_postgres::Error),
    /// All pool slots are in use and the semaphore was closed (shutdown).
    PoolExhausted,
    /// A migration step failed.
    Migration { step: String, source: Box<PgError> },
    /// I/O error reading migration files.
    Io(std::io::Error),
    /// Invalid connection URL.
    InvalidConfig(String),
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(e)                 => write!(f, "Postgres connect: {e}"),
            Self::Query(e)                   => write!(f, "Postgres query: {e}"),
            Self::PoolExhausted              => write!(f, "Postgres pool exhausted"),
            Self::Migration { step, source } => write!(f, "Migration '{step}' failed: {source}"),
            Self::Io(e)                      => write!(f, "Postgres I/O: {e}"),
            Self::InvalidConfig(msg)         => write!(f, "Postgres config: {msg}"),
        }
    }
}

impl std::error::Error for PgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(e)               => Some(e),
            Self::Query(e)                 => Some(e),
            Self::Migration { source, .. } => Some(source.as_ref()),
            Self::Io(e)                    => Some(e),
            Self::PoolExhausted            => None,
            Self::InvalidConfig(_)         => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_pool_exhausted() {
        let e = PgError::PoolExhausted;
        assert!(e.to_string().contains("exhausted"));
    }

    #[test]
    fn display_invalid_config() {
        let e = PgError::InvalidConfig("bad url".to_string());
        assert!(e.to_string().contains("bad url"));
    }

    #[test]
    fn display_migration() {
        let inner = PgError::PoolExhausted;
        let e = PgError::Migration {
            step:   "001_init.sql".to_string(),
            source: Box::new(inner),
        };
        assert!(e.to_string().contains("001_init.sql"));
    }

    #[test]
    fn source_none_for_pool_exhausted() {
        use std::error::Error;
        let e = PgError::PoolExhausted;
        assert!(e.source().is_none());
    }
}
