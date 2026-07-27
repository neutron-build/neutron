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
    pub rows: Vec<Row>,
    pub columns: Vec<(String, DataType)>,
    pub position: usize,
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
