//! Typed model wrappers for all 14 Nucleus data models.
//!
//! Each module provides an ergonomic Rust API that generates the correct
//! Nucleus SQL functions and executes them over pgwire via the connection pool.

pub mod blob;
pub mod cdc;
pub mod columnar;
pub mod datalog;
pub mod document;
pub mod fts;
pub mod geo;
pub mod graph;
pub mod kv;
pub mod pubsub;
pub mod sql;
pub mod streams;
pub mod timeseries;
pub mod vector;

/// Validate that a string is a safe SQL identifier (table name, column name).
/// Only allows alphanumeric characters and underscores, must start with a
/// letter or underscore.
pub(crate) fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let first = name.as_bytes()[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return false;
    }
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_identifiers() {
        assert!(is_valid_identifier("users"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("table_123"));
        assert!(is_valid_identifier("A"));
    }

    #[test]
    fn invalid_identifiers() {
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123abc"));
        assert!(!is_valid_identifier("no spaces"));
        assert!(!is_valid_identifier("drop;table"));
        assert!(!is_valid_identifier("table-name"));
    }
}
