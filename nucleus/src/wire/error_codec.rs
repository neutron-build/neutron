//! Error encoding and mapping across different protocols.
//!
//! Different wire protocols require different error representations:
//! - pgwire uses PostgreSQL SQLSTATE codes
//! - Binary protocol uses custom error codes
//!
//! This module provides an abstraction to encode errors consistently
//! without duplicating error handling logic.

use crate::executor::ExecError;

/// Error codes common to all protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Syntax error in SQL query
    SyntaxError,
    /// Referenced table does not exist
    UndefinedTable,
    /// Referenced column does not exist
    UndefinedColumn,
    /// Column count mismatch in INSERT/VALUES
    ColumnCountMismatch,
    /// Feature not supported by this database
    FeatureNotSupported,
    /// Insufficient privilege for operation
    InsufficientPrivilege,
    /// Unique constraint violation (duplicate key)
    UniqueViolation,
    /// Foreign key constraint violation
    ForeignKeyViolation,
    /// NOT NULL constraint violation
    NotNullViolation,
    /// CHECK constraint violation
    CheckViolation,
    /// Generic integrity constraint violation
    IntegrityConstraintViolation,
    /// Duplicate table in catalog
    DuplicateTable,
    /// Transaction serialization failure (conflict)
    SerializationFailure,
    /// A lock could not be obtained within `lock_timeout`
    LockNotAvailable,
    /// Statement issued inside an already-aborted transaction block
    InFailedSqlTransaction,
    /// Statement cancelled at user request (wire CancelRequest / timeout)
    QueryCanceled,
    /// Generic catalog error
    CatalogError,
    /// Storage layer error (internal)
    StorageError,
    /// Division by zero
    DivisionByZero,
    /// Numeric value out of range
    NumericValueOutOfRange,
    /// Other data exception
    DataException,
    /// Internal server error
    InternalError,
    /// Generic runtime error
    RuntimeError,
    /// Insufficient resources (memory pressure, etc.)
    InsufficientResources,
    /// The data directory's filesystem is out of usable space
    DiskFull,
    /// A write was attempted while the server is in read-only mode
    ReadOnlySqlTransaction,
}

/// Protocol-independent error details.
#[derive(Debug, Clone)]
pub struct ErrorDetails {
    /// The error code category
    pub code: ErrorCode,
    /// Human-readable error message
    pub message: String,
}

impl ErrorDetails {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Trait for encoding errors in a protocol-specific way.
pub trait ErrorCodec {
    /// Map a nucleus error to protocol-specific error details.
    fn encode(&self, err: &ExecError) -> ErrorDetails;

    /// Get the protocol-specific error code for a given ErrorCode.
    /// For pgwire, returns SQLSTATE code (5 characters).
    /// For binary protocol, returns custom code (u16).
    fn code_to_string(&self, code: ErrorCode) -> String;
}

/// PostgreSQL wire protocol error codec.
/// Maps nucleus errors to PostgreSQL SQLSTATE codes.
pub struct PgWireErrorCodec;

impl ErrorCodec for PgWireErrorCodec {
    fn encode(&self, err: &ExecError) -> ErrorDetails {
        match err {
            ExecError::Parse(_) => ErrorDetails::new(ErrorCode::SyntaxError, err.to_string()),
            ExecError::TableNotFound(name) => ErrorDetails::new(
                ErrorCode::UndefinedTable,
                format!("relation \"{}\" does not exist", name),
            ),
            ExecError::ColumnNotFound(name) => ErrorDetails::new(
                ErrorCode::UndefinedColumn,
                format!("column \"{}\" does not exist", name),
            ),
            ExecError::ColumnCountMismatch { expected, got } => ErrorDetails::new(
                ErrorCode::ColumnCountMismatch,
                format!(
                    "INSERT has more expressions than target columns; {} vs {}",
                    got, expected
                ),
            ),
            ExecError::Unsupported(msg) => {
                ErrorDetails::new(ErrorCode::FeatureNotSupported, msg.clone())
            }
            ExecError::PermissionDenied(msg) => {
                ErrorDetails::new(ErrorCode::InsufficientPrivilege, msg.clone())
            }
            ExecError::ConstraintViolation(msg) => {
                let code = if msg.contains("unique constraint") || msg.contains("duplicate key") {
                    ErrorCode::UniqueViolation
                } else if msg.contains("foreign key") || msg.contains("violates foreign key") {
                    ErrorCode::ForeignKeyViolation
                } else if msg.contains("not-null") || msg.contains("NOT NULL") {
                    ErrorCode::NotNullViolation
                } else if msg.contains("check constraint") {
                    ErrorCode::CheckViolation
                } else {
                    ErrorCode::IntegrityConstraintViolation
                };
                ErrorDetails::new(code, msg.clone())
            }
            ExecError::Catalog(cat_err) => {
                let msg = cat_err.to_string();
                let code = if msg.contains("not found") {
                    ErrorCode::UndefinedTable
                } else if msg.contains("already exists") {
                    ErrorCode::DuplicateTable
                } else {
                    ErrorCode::CatalogError
                };
                ErrorDetails::new(code, msg)
            }
            ExecError::Storage(stor_err) => {
                let msg = stor_err.to_string();
                // `could not serialize access` is the 2PL deadlock-break kill
                // (see `storage::lock_manager`); "write conflict" is the MVCC
                // SSI abort. Both are SQLSTATE 40001, and the code is the only
                // thing a client acts on: pgx, psycopg and JDBC all retry on
                // 40001 and none of them read the message. A serialization
                // failure delivered as a generic storage error is a
                // transaction that silently stops being retried.
                // Checked before the serialization arm: a lock timeout is NOT
                // a conflict the client can win by retrying — the holder is
                // still there — so classifying it as 40001 would send drivers
                // into a retry loop against a lock that is not moving.
                let code = if msg.contains("current transaction is aborted") {
                    // Commands issued after a transaction was aborted. Checked
                    // FIRST because the engine reports this through
                    // `StorageError`, where the `Runtime` arm's identical check
                    // never sees it — so a serializable transaction killed to
                    // break a deadlock could have its FOLLOW-UP statement
                    // classified XX000, which tells a driver nothing. 25P02 is
                    // what PostgreSQL sends and what clients act on.
                    ErrorCode::InFailedSqlTransaction
                } else if msg.contains("lock_not_available") {
                    ErrorCode::LockNotAvailable
                } else if msg.contains("write conflict")
                    || msg.contains("WriteConflict")
                    || msg.contains("could not serialize")
                {
                    ErrorCode::SerializationFailure
                } else if msg.contains("not found") {
                    ErrorCode::UndefinedTable
                } else {
                    ErrorCode::StorageError
                };
                ErrorDetails::new(code, msg)
            }
            ExecError::Runtime(msg) => {
                let code = if msg.contains("division by zero") {
                    ErrorCode::DivisionByZero
                } else if msg.contains("out of range") {
                    ErrorCode::NumericValueOutOfRange
                } else if msg.contains("current transaction is aborted") {
                    ErrorCode::InFailedSqlTransaction
                } else if msg.contains("canceling statement") {
                    ErrorCode::QueryCanceled
                } else {
                    ErrorCode::DataException
                };
                ErrorDetails::new(code, msg.clone())
            }
            ExecError::MemoryExceeded(msg) => {
                ErrorDetails::new(ErrorCode::InsufficientResources, msg.clone())
            }
            ExecError::DiskFull(msg) => ErrorDetails::new(ErrorCode::DiskFull, msg.clone()),
            ExecError::ReadOnly(msg) => {
                ErrorDetails::new(ErrorCode::ReadOnlySqlTransaction, msg.clone())
            }
        }
    }

    fn code_to_string(&self, code: ErrorCode) -> String {
        match code {
            ErrorCode::InFailedSqlTransaction => "25P02".to_string(),
            ErrorCode::QueryCanceled => "57014".to_string(),
            ErrorCode::SyntaxError => "42601".to_string(),
            ErrorCode::UndefinedTable => "42P01".to_string(),
            ErrorCode::UndefinedColumn => "42703".to_string(),
            ErrorCode::ColumnCountMismatch => "42601".to_string(),
            ErrorCode::FeatureNotSupported => "0A000".to_string(),
            ErrorCode::InsufficientPrivilege => "42501".to_string(),
            ErrorCode::UniqueViolation => "23505".to_string(),
            ErrorCode::ForeignKeyViolation => "23503".to_string(),
            ErrorCode::NotNullViolation => "23502".to_string(),
            ErrorCode::CheckViolation => "23514".to_string(),
            ErrorCode::IntegrityConstraintViolation => "23000".to_string(),
            ErrorCode::DuplicateTable => "42P07".to_string(),
            ErrorCode::SerializationFailure => "40001".to_string(),
            ErrorCode::LockNotAvailable => "55P03".to_string(),
            ErrorCode::CatalogError => "42000".to_string(),
            ErrorCode::StorageError => "XX000".to_string(),
            ErrorCode::DivisionByZero => "22012".to_string(),
            ErrorCode::NumericValueOutOfRange => "22003".to_string(),
            ErrorCode::DataException => "22000".to_string(),
            ErrorCode::InternalError => "XX000".to_string(),
            ErrorCode::RuntimeError => "22000".to_string(),
            ErrorCode::InsufficientResources => "53200".to_string(),
            ErrorCode::DiskFull => "53100".to_string(),
            ErrorCode::ReadOnlySqlTransaction => "25006".to_string(),
        }
    }
}

/// Binary protocol error codec.
/// Maps nucleus errors to custom binary protocol error codes.
pub struct BinaryErrorCodec;

impl ErrorCodec for BinaryErrorCodec {
    fn encode(&self, err: &ExecError) -> ErrorDetails {
        // Binary protocol uses same error categorization,
        // just with different code representation (see code_to_string)
        match err {
            ExecError::Parse(_) => ErrorDetails::new(ErrorCode::SyntaxError, err.to_string()),
            ExecError::TableNotFound(name) => ErrorDetails::new(
                ErrorCode::UndefinedTable,
                format!("relation \"{}\" does not exist", name),
            ),
            ExecError::ColumnNotFound(name) => ErrorDetails::new(
                ErrorCode::UndefinedColumn,
                format!("column \"{}\" does not exist", name),
            ),
            ExecError::ColumnCountMismatch { expected, got } => ErrorDetails::new(
                ErrorCode::ColumnCountMismatch,
                format!(
                    "INSERT has more expressions than target columns; {} vs {}",
                    got, expected
                ),
            ),
            ExecError::Unsupported(msg) => {
                ErrorDetails::new(ErrorCode::FeatureNotSupported, msg.clone())
            }
            ExecError::PermissionDenied(msg) => {
                ErrorDetails::new(ErrorCode::InsufficientPrivilege, msg.clone())
            }
            ExecError::ConstraintViolation(msg) => {
                let code = if msg.contains("unique constraint") || msg.contains("duplicate key") {
                    ErrorCode::UniqueViolation
                } else if msg.contains("foreign key") || msg.contains("violates foreign key") {
                    ErrorCode::ForeignKeyViolation
                } else if msg.contains("not-null") || msg.contains("NOT NULL") {
                    ErrorCode::NotNullViolation
                } else if msg.contains("check constraint") {
                    ErrorCode::CheckViolation
                } else {
                    ErrorCode::IntegrityConstraintViolation
                };
                ErrorDetails::new(code, msg.clone())
            }
            ExecError::Catalog(cat_err) => {
                let msg = cat_err.to_string();
                let code = if msg.contains("not found") {
                    ErrorCode::UndefinedTable
                } else if msg.contains("already exists") {
                    ErrorCode::DuplicateTable
                } else {
                    ErrorCode::CatalogError
                };
                ErrorDetails::new(code, msg)
            }
            ExecError::Storage(stor_err) => {
                let msg = stor_err.to_string();
                // `could not serialize access` is the 2PL deadlock-break kill
                // (see `storage::lock_manager`); "write conflict" is the MVCC
                // SSI abort. Both are SQLSTATE 40001, and the code is the only
                // thing a client acts on: pgx, psycopg and JDBC all retry on
                // 40001 and none of them read the message. A serialization
                // failure delivered as a generic storage error is a
                // transaction that silently stops being retried.
                // Checked before the serialization arm: a lock timeout is NOT
                // a conflict the client can win by retrying — the holder is
                // still there — so classifying it as 40001 would send drivers
                // into a retry loop against a lock that is not moving.
                let code = if msg.contains("current transaction is aborted") {
                    // Commands issued after a transaction was aborted. Checked
                    // FIRST because the engine reports this through
                    // `StorageError`, where the `Runtime` arm's identical check
                    // never sees it — so a serializable transaction killed to
                    // break a deadlock could have its FOLLOW-UP statement
                    // classified XX000, which tells a driver nothing. 25P02 is
                    // what PostgreSQL sends and what clients act on.
                    ErrorCode::InFailedSqlTransaction
                } else if msg.contains("lock_not_available") {
                    ErrorCode::LockNotAvailable
                } else if msg.contains("write conflict")
                    || msg.contains("WriteConflict")
                    || msg.contains("could not serialize")
                {
                    ErrorCode::SerializationFailure
                } else if msg.contains("not found") {
                    ErrorCode::UndefinedTable
                } else {
                    ErrorCode::StorageError
                };
                ErrorDetails::new(code, msg)
            }
            ExecError::Runtime(msg) => {
                let code = if msg.contains("division by zero") {
                    ErrorCode::DivisionByZero
                } else if msg.contains("out of range") {
                    ErrorCode::NumericValueOutOfRange
                } else {
                    ErrorCode::DataException
                };
                ErrorDetails::new(code, msg.clone())
            }
            ExecError::MemoryExceeded(msg) => {
                ErrorDetails::new(ErrorCode::InsufficientResources, msg.clone())
            }
            ExecError::DiskFull(msg) => ErrorDetails::new(ErrorCode::DiskFull, msg.clone()),
            ExecError::ReadOnly(msg) => {
                ErrorDetails::new(ErrorCode::ReadOnlySqlTransaction, msg.clone())
            }
        }
    }

    fn code_to_string(&self, code: ErrorCode) -> String {
        // Binary protocol uses custom u16 error codes (this can be optimized)
        // For now, we map to a simpler code scheme suitable for binary representation
        match code {
            ErrorCode::SyntaxError => "1001".to_string(),
            ErrorCode::UndefinedTable => "1002".to_string(),
            ErrorCode::UndefinedColumn => "1003".to_string(),
            ErrorCode::ColumnCountMismatch => "1004".to_string(),
            ErrorCode::FeatureNotSupported => "1005".to_string(),
            ErrorCode::InsufficientPrivilege => "1006".to_string(),
            ErrorCode::UniqueViolation => "2001".to_string(),
            ErrorCode::ForeignKeyViolation => "2002".to_string(),
            ErrorCode::NotNullViolation => "2003".to_string(),
            ErrorCode::CheckViolation => "2004".to_string(),
            ErrorCode::IntegrityConstraintViolation => "2000".to_string(),
            ErrorCode::DuplicateTable => "1007".to_string(),
            ErrorCode::SerializationFailure => "3001".to_string(),
            ErrorCode::LockNotAvailable => "3004".to_string(),
            ErrorCode::InFailedSqlTransaction => "3002".to_string(),
            ErrorCode::QueryCanceled => "3003".to_string(),
            ErrorCode::CatalogError => "1008".to_string(),
            ErrorCode::StorageError => "5001".to_string(),
            ErrorCode::DivisionByZero => "4001".to_string(),
            ErrorCode::NumericValueOutOfRange => "4002".to_string(),
            ErrorCode::DataException => "4000".to_string(),
            ErrorCode::InternalError => "5000".to_string(),
            ErrorCode::RuntimeError => "4999".to_string(),
            ErrorCode::InsufficientResources => "5002".to_string(),
            ErrorCode::DiskFull => "5003".to_string(),
            ErrorCode::ReadOnlySqlTransaction => "5004".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pgwire_codec_undefined_column() {
        let codec = PgWireErrorCodec;
        let err = ExecError::ColumnNotFound("email".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::UndefinedColumn);
        assert_eq!(codec.code_to_string(ErrorCode::UndefinedColumn), "42703");
    }

    /// Both serialization-failure shapes must reach the client as SQLSTATE
    /// 40001, because that code is the entire retry contract: pgx, psycopg and
    /// JDBC all retry on 40001 and none of them read the message. Delivered as
    /// a generic `XX000` storage error, a killed serializable transaction
    /// silently stops being retried by every driver in the ecosystem — the
    /// feature is then correct in the engine and unusable over the wire.
    ///
    /// The 2PL kill (`could not serialize access`, `storage::lock_manager`)
    /// was exactly that until this test existed; only the MVCC SSI abort's
    /// "write conflict" wording was matched.
    #[test]
    fn serialization_failures_map_to_40001_over_the_wire() {
        let cases = [
            // MVCC SSI abort.
            "write conflict on table t",
            // 2PL deadlock-break kill.
            "could not serialize access to table 'accounts' due to concurrent \
             update: this SERIALIZABLE transaction was aborted to break a \
             potential deadlock (retry the transaction)",
        ];
        for msg in cases {
            for codec in [&PgWireErrorCodec as &dyn ErrorCodec, &BinaryErrorCodec] {
                let err = ExecError::Storage(crate::storage::StorageError::Io(msg.to_string()));
                let details = codec.encode(&err);
                assert_eq!(
                    details.code,
                    ErrorCode::SerializationFailure,
                    "not classified as a serialization failure: {msg}"
                );
            }
        }
        assert_eq!(
            PgWireErrorCodec.code_to_string(ErrorCode::SerializationFailure),
            "40001"
        );

        // A killed transaction's FOLLOW-UP statement reports that the
        // transaction is aborted, through StorageError rather than Runtime.
        // That must be 25P02, not the XX000 catch-all: a driver reading XX000
        // learns nothing, where 25P02 says "roll back and start over".
        let after = ExecError::Storage(crate::storage::StorageError::Io(
            "current transaction is aborted (it was killed to break a deadlock), \
             commands ignored until end of transaction block: issue ROLLBACK"
                .into(),
        ));
        assert_eq!(
            PgWireErrorCodec.encode(&after).code,
            ErrorCode::InFailedSqlTransaction
        );
        assert_eq!(
            PgWireErrorCodec.code_to_string(ErrorCode::InFailedSqlTransaction),
            "25P02"
        );
    }

    #[test]
    fn test_pgwire_codec_unsupported() {
        let codec = PgWireErrorCodec;
        let err = ExecError::Unsupported("distributed transactions not supported".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::FeatureNotSupported);
        assert_eq!(
            codec.code_to_string(ErrorCode::FeatureNotSupported),
            "0A000"
        );
    }

    #[test]
    fn test_pgwire_codec_permission_denied() {
        let codec = PgWireErrorCodec;
        let err = ExecError::PermissionDenied("user lacks privilege".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::InsufficientPrivilege);
        assert_eq!(
            codec.code_to_string(ErrorCode::InsufficientPrivilege),
            "42501"
        );
    }

    #[test]
    fn test_pgwire_codec_unique_violation() {
        let codec = PgWireErrorCodec;
        let err =
            ExecError::ConstraintViolation("duplicate key violates unique constraint".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::UniqueViolation);
        assert_eq!(codec.code_to_string(ErrorCode::UniqueViolation), "23505");
    }

    #[test]
    fn test_pgwire_codec_fk_violation() {
        let codec = PgWireErrorCodec;
        let err = ExecError::ConstraintViolation("violates foreign key constraint".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::ForeignKeyViolation);
        assert_eq!(
            codec.code_to_string(ErrorCode::ForeignKeyViolation),
            "23503"
        );
    }

    #[test]
    fn test_pgwire_codec_not_null_violation() {
        let codec = PgWireErrorCodec;
        let err = ExecError::ConstraintViolation("NOT NULL constraint violated".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::NotNullViolation);
        assert_eq!(codec.code_to_string(ErrorCode::NotNullViolation), "23502");
    }

    #[test]
    fn test_pgwire_codec_runtime_error() {
        let codec = PgWireErrorCodec;
        let err = ExecError::Runtime("division by zero".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::DivisionByZero);
        assert_eq!(codec.code_to_string(ErrorCode::DivisionByZero), "22012");
    }

    #[test]
    fn test_binary_codec_undefined_table() {
        let codec = BinaryErrorCodec;
        let err = ExecError::TableNotFound("users".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::UndefinedTable);
        assert_eq!(codec.code_to_string(ErrorCode::UndefinedTable), "1002");
    }

    #[test]
    fn test_binary_codec_unique_violation() {
        let codec = BinaryErrorCodec;
        let err = ExecError::ConstraintViolation("duplicate key".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::UniqueViolation);
        assert_eq!(codec.code_to_string(ErrorCode::UniqueViolation), "2001");
    }

    #[test]
    fn test_pgwire_codec_disk_full_is_53100() {
        let codec = PgWireErrorCodec;
        let err = ExecError::DiskFull("read-only: 1% free".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::DiskFull);
        assert_eq!(codec.code_to_string(ErrorCode::DiskFull), "53100");
        assert!(details.message.contains("1% free"));
    }

    #[test]
    fn test_pgwire_codec_read_only_is_25006() {
        let codec = PgWireErrorCodec;
        let err = ExecError::ReadOnly("operator maintenance".to_string());
        let details = codec.encode(&err);
        assert_eq!(details.code, ErrorCode::ReadOnlySqlTransaction);
        assert_eq!(
            codec.code_to_string(ErrorCode::ReadOnlySqlTransaction),
            "25006"
        );
    }

    #[test]
    fn test_binary_codec_disk_full_and_read_only_are_distinct() {
        let codec = BinaryErrorCodec;
        assert_eq!(
            codec.encode(&ExecError::DiskFull("x".into())).code,
            ErrorCode::DiskFull
        );
        assert_eq!(
            codec.encode(&ExecError::ReadOnly("x".into())).code,
            ErrorCode::ReadOnlySqlTransaction
        );
        assert_ne!(
            codec.code_to_string(ErrorCode::DiskFull),
            codec.code_to_string(ErrorCode::ReadOnlySqlTransaction)
        );
    }

    #[test]
    fn test_error_code_strings_pgwire() {
        let codec = PgWireErrorCodec;
        // Verify all codes produce valid SQLSTATE strings
        assert_eq!(codec.code_to_string(ErrorCode::SyntaxError), "42601");
        assert_eq!(codec.code_to_string(ErrorCode::UniqueViolation), "23505");
        assert_eq!(codec.code_to_string(ErrorCode::InternalError), "XX000");
    }

    #[test]
    fn test_error_code_strings_binary() {
        let codec = BinaryErrorCodec;
        // Verify all codes produce valid binary protocol codes
        assert_eq!(codec.code_to_string(ErrorCode::SyntaxError), "1001");
        assert_eq!(codec.code_to_string(ErrorCode::UniqueViolation), "2001");
        assert_eq!(codec.code_to_string(ErrorCode::InternalError), "5000");
    }
}
