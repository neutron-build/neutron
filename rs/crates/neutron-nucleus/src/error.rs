//! Error type for neutron-nucleus.

use std::fmt;

/// All errors that can arise when using [`NucleusPool`](crate::NucleusPool),
/// [`Db`](crate::Db), or [`NucleusClient`](crate::NucleusClient).
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
    /// JSON serialization/deserialization failure.
    Serde(String),
    /// An invalid SQL identifier was provided.
    InvalidIdentifier(String),
    /// A Nucleus-specific feature was called against plain PostgreSQL.
    NucleusRequired { feature: String },
}

impl fmt::Display for NucleusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(e)                    => write!(f, "Nucleus connect: {e}"),
            Self::Query(e)                      => write!(f, "Nucleus query: {e}"),
            Self::PoolExhausted                 => write!(f, "Nucleus pool exhausted"),
            Self::Migration { step, source }    => write!(f, "Migration '{step}' failed: {source}"),
            Self::Io(e)                         => write!(f, "Nucleus I/O: {e}"),
            Self::Serde(msg)                    => write!(f, "Nucleus serde: {msg}"),
            Self::InvalidIdentifier(name)       => write!(f, "Nucleus invalid identifier: {name}"),
            Self::NucleusRequired { feature }   => write!(f, "{feature} requires Nucleus database, but connected to plain PostgreSQL"),
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
            Self::Serde(_)                   => None,
            Self::InvalidIdentifier(_)       => None,
            Self::NucleusRequired { .. }     => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn display_pool_exhausted() {
        let err = NucleusError::PoolExhausted;
        assert_eq!(err.to_string(), "Nucleus pool exhausted");
    }

    #[test]
    fn display_serde() {
        let err = NucleusError::Serde("bad json".to_string());
        assert_eq!(err.to_string(), "Nucleus serde: bad json");
    }

    #[test]
    fn display_invalid_identifier() {
        let err = NucleusError::InvalidIdentifier("drop;table".to_string());
        assert_eq!(err.to_string(), "Nucleus invalid identifier: drop;table");
    }

    #[test]
    fn display_nucleus_required() {
        let err = NucleusError::NucleusRequired {
            feature: "GRAPH_QUERY".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "GRAPH_QUERY requires Nucleus database, but connected to plain PostgreSQL"
        );
    }

    #[test]
    fn display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = NucleusError::Io(io_err);
        assert!(err.to_string().starts_with("Nucleus I/O:"));
    }

    #[test]
    fn display_migration() {
        let source = NucleusError::Serde("parse error".to_string());
        let err = NucleusError::Migration {
            step: "001_create_tables".to_string(),
            source: Box::new(source),
        };
        let msg = err.to_string();
        assert!(msg.contains("001_create_tables"));
        assert!(msg.contains("parse error"));
    }

    #[test]
    fn source_pool_exhausted_is_none() {
        let err = NucleusError::PoolExhausted;
        assert!(err.source().is_none());
    }

    #[test]
    fn source_serde_is_none() {
        let err = NucleusError::Serde("msg".into());
        assert!(err.source().is_none());
    }

    #[test]
    fn source_invalid_identifier_is_none() {
        let err = NucleusError::InvalidIdentifier("bad".into());
        assert!(err.source().is_none());
    }

    #[test]
    fn source_nucleus_required_is_none() {
        let err = NucleusError::NucleusRequired { feature: "test".into() };
        assert!(err.source().is_none());
    }

    #[test]
    fn source_io_returns_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err = NucleusError::Io(io_err);
        let src = err.source().unwrap();
        assert!(src.to_string().contains("denied"));
    }

    #[test]
    fn source_migration_returns_inner() {
        let inner = NucleusError::PoolExhausted;
        let err = NucleusError::Migration {
            step: "step1".into(),
            source: Box::new(inner),
        };
        let src = err.source().unwrap();
        assert_eq!(src.to_string(), "Nucleus pool exhausted");
    }

    #[test]
    fn debug_format() {
        let err = NucleusError::PoolExhausted;
        let dbg = format!("{:?}", err);
        assert!(dbg.contains("PoolExhausted"));
    }
}
