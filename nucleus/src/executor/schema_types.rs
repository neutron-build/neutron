//! Schema-level type definitions used by the executor.
//!
//! These are metadata types for views, triggers, roles, sequences, cursors,
//! and stored functions.

use crate::types::{DataType, Row};
use std::collections::HashMap;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ViewDef {
    pub name: String,
    pub sql: String,
    pub columns: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct MaterializedViewDef {
    pub name: String,
    pub sql: String,
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Row>,
    /// Base tables this MV depends on (populated from the MV's SELECT query).
    pub source_tables: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct SequenceDef {
    pub current: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    /// START value, so a bare `ALTER SEQUENCE … RESTART` rewinds here (PG
    /// semantics) instead of MINVALUE. Pre-upgrade files without it load
    /// with min_value (sequences.json) / 1 (meta.json).
    pub start: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct TriggerDef {
    pub name: String,
    pub table_name: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub for_each_row: bool,
    pub body: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

/// A Postgres extension tracked as a catalog no-op. Nucleus provides the
/// functionality most bootstrap extensions ask for (uuid, crypto, trigram,
/// vector, etc.) natively, so `CREATE EXTENSION` records the name/schema/version
/// for truthful `pg_extension` introspection without loading any real .so.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ExtensionDef {
    pub name: String,
    pub schema: String,
    pub version: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct RoleDef {
    pub name: String,
    /// Encoded SCRAM-SHA-256 verifier. Raw passwords are never retained.
    pub password_hash: Option<String>,
    pub is_superuser: bool,
    pub bypass_rls: bool,
    pub can_login: bool,
    /// `VALID UNTIL` — UTC microseconds after which the password no longer
    /// authenticates. `None` means no expiry, which is PostgreSQL's default
    /// and what every role created before this field had.
    ///
    /// This is a password expiry, not a role expiry: an expired role still
    /// exists, still owns its objects, and can still be granted to. Only its
    /// ability to authenticate lapses.
    pub valid_until: Option<i64>,
    /// Roles this role may assume via SET ROLE (transitively inherited).
    pub member_of: Vec<String>,
    pub privileges: HashMap<String, Vec<Privilege>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    All,
    Create,
    Drop,
    Usage,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct CursorDef {
    pub name: String,
    /// The fully materialized result, taken from the DECLARE-time snapshot.
    pub rows: Vec<Row>,
    pub columns: Vec<(String, DataType)>,
    /// PostgreSQL cursor position: 0 is before the first row, `1..=rows.len()`
    /// is on that row, `rows.len() + 1` is after the last row.
    pub position: usize,
    /// Declared `NO SCROLL`: any fetch that does not move strictly forward is
    /// refused (55000). An unspecified or `SCROLL` cursor may move both ways
    /// because the rows are held in memory.
    pub no_scroll: bool,
    /// Declared `WITH HOLD`: survives COMMIT. Every other cursor is closed
    /// when its transaction ends.
    pub hold: bool,
    /// Declared inside the transaction that is still open. A `WITH HOLD`
    /// cursor loses this at COMMIT and is dropped by ROLLBACK while it is set.
    pub opened_in_txn: bool,
    /// Estimated heap bytes held by `rows`, as charged against the budget.
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FunctionLanguage {
    Sql,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FunctionKind {
    Function,
    Procedure,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FunctionDef {
    pub name: String,
    pub kind: FunctionKind,
    pub params: Vec<(String, DataType)>,
    pub return_type: Option<DataType>,
    pub body: String,
    pub language: FunctionLanguage,
}
