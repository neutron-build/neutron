//! The typed layer's error (C-T6b1 item 3): the SQLSTATE-bearing summary of
//! everything under it — txn-protocol errors ([`TxnError`]), row/definition
//! codec errors ([`RowError`]) and the layer's own conditions. The SQL
//! layer maps [`DbError::sqlstate`] onto client errors.

use nucleus_codec::row::RowError;
use nucleus_txn::TxnError;

/// An error raised by the typed layer.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DbError {
    /// A transaction-protocol error (C-T0): 40001, 23505, 55P03, … — the
    /// inner error's own SQLSTATE.
    #[error("{0}")]
    Txn(TxnError),
    /// A row or definition codec error (C-T6 §2/§3/§6 bytes).
    #[error("{0}")]
    Row(RowError),
    /// 42P01.
    #[error("relation does not exist")]
    UndefinedTable,
    /// 42703.
    #[error("column does not exist")]
    UndefinedColumn,
    /// 42701.
    #[error("column already exists")]
    DuplicateColumn,
    /// 23502 (raised here when the caller's statement violates a NOT NULL
    /// the codec cannot see, e.g. a NULL primary-key value).
    #[error("null value violates a not-null constraint")]
    NotNullViolation,
    /// 0A000: not in 2.0 (index maintenance, ALTER, expression indexes, …).
    #[error("feature not supported: {0}")]
    FeatureNotSupported(&'static str),
    /// 25001.
    #[error("cannot run inside a transaction block")]
    ActiveSqlTransaction,
    /// 54000: the encoded primary key exceeds the §3 limit, the statement
    /// exhausted its ordinal counter, ….
    #[error("program limit exceeded")]
    ProgramLimitExceeded,
    /// 55000: e.g. an online index build that found its index changed.
    #[error("object is not in a prerequisite state: {0}")]
    ObjectNotInPrerequisiteState(&'static str),
}

impl From<TxnError> for DbError {
    fn from(e: TxnError) -> DbError {
        DbError::Txn(e)
    }
}

impl From<RowError> for DbError {
    fn from(e: RowError) -> DbError {
        DbError::Row(e)
    }
}

impl DbError {
    /// The SQLSTATE of this error.
    ///
    /// - [`DbError::Txn`] carries the inner [`TxnError`]'s own SQLSTATE.
    /// - [`DbError::Row`]: a corrupt row/definition is XX001 (§2, §3
    ///   decode rule), a row ahead of the definition XX001 (§3), an
    ///   undecodable value XX001 (§2); a NOT NULL violation of the writer
    ///   is 23502; anything else is an internal error (XX000).
    pub fn sqlstate(&self) -> &'static str {
        match self {
            DbError::Txn(e) => e.sqlstate(),
            DbError::Row(RowError::Corrupt(_)) => "XX001",
            DbError::Row(RowError::VersionAhead { .. }) => "XX001",
            DbError::Row(RowError::TypeMismatch { .. }) => "XX001",
            DbError::Row(RowError::NotNull { .. }) => "23502",
            DbError::Row(RowError::Arity { .. }) => "XX000",
            DbError::Row(RowError::InvalidDdl(_)) => "XX000",
            DbError::UndefinedTable => "42P01",
            DbError::UndefinedColumn => "42703",
            DbError::DuplicateColumn => "42701",
            DbError::NotNullViolation => "23502",
            DbError::FeatureNotSupported(_) => "0A000",
            DbError::ActiveSqlTransaction => "25001",
            DbError::ProgramLimitExceeded => "54000",
            DbError::ObjectNotInPrerequisiteState(_) => "55000",
        }
    }
}

#[cfg(test)]
mod tests {
    //! The SQLSTATE table, pinned (item 3).

    use super::DbError;
    use nucleus_codec::row::RowError;
    use nucleus_txn::TxnError;

    #[test]
    fn sqlstates_are_pinned() {
        assert_eq!(DbError::UndefinedTable.sqlstate(), "42P01");
        assert_eq!(DbError::UndefinedColumn.sqlstate(), "42703");
        assert_eq!(DbError::DuplicateColumn.sqlstate(), "42701");
        assert_eq!(
            DbError::NotNullViolation.sqlstate(),
            "23502",
            "NOT NULL is 23502"
        );
        assert_eq!(
            DbError::FeatureNotSupported("x").sqlstate(),
            "0A000",
            "feature not supported is 0A000"
        );
        assert_eq!(DbError::ActiveSqlTransaction.sqlstate(), "25001");
        assert_eq!(DbError::ProgramLimitExceeded.sqlstate(), "54000");
        assert_eq!(
            DbError::ObjectNotInPrerequisiteState("x").sqlstate(),
            "55000"
        );
        // Row: corrupt and version-ahead are XX001, NotNull 23502.
        assert_eq!(
            DbError::Row(RowError::Corrupt("row magic")).sqlstate(),
            "XX001"
        );
        assert_eq!(
            DbError::Row(RowError::VersionAhead { row: 2, schema: 1 }).sqlstate(),
            "XX001"
        );
        assert_eq!(
            DbError::Row(RowError::NotNull { attnum: 1 }).sqlstate(),
            "23502"
        );
        // Txn: the inner error's own SQLSTATE.
        assert_eq!(
            DbError::Txn(TxnError::SerializationFailure).sqlstate(),
            "40001"
        );
        assert_eq!(DbError::Txn(TxnError::UniqueViolation).sqlstate(), "23505");
        assert_eq!(
            DbError::Txn(TxnError::Invariant("…".into())).sqlstate(),
            "XX000"
        );
    }
}
