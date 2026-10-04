//! Schema-level type definitions used by the executor.
//!
//! These are metadata types for views, triggers, roles, sequences, cursors,
//! and stored functions.

use super::types::ColMeta;
use crate::types::{DataType, Row, Value};
use sqlparser::ast::{Expr, SelectItem};
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
    /// Empty for a lazy cursor (`lazy` is set), which holds no rows at all.
    pub rows: Vec<Row>,
    pub columns: Vec<(String, DataType)>,
    /// PostgreSQL cursor position: 0 is before the first row, `1..=rows.len()`
    /// is on that row, `rows.len() + 1` is after the last row. For a lazy
    /// cursor the same numbering holds, with the row count learned as the
    /// producer runs.
    pub position: usize,
    /// Declared `NO SCROLL`: any fetch that does not move strictly forward is
    /// refused (55000). An unspecified or `SCROLL` cursor may move both ways
    /// because the rows are held in memory. A lazy cursor is always forward
    /// only, so it sets this too.
    pub no_scroll: bool,
    /// Declared `WITH HOLD`: survives COMMIT. Every other cursor is closed
    /// when its transaction ends.
    pub hold: bool,
    /// Declared inside the transaction that is still open. A `WITH HOLD`
    /// cursor loses this at COMMIT and is dropped by ROLLBACK while it is set.
    pub opened_in_txn: bool,
    /// Estimated heap bytes held by `rows`, as charged against the budget.
    pub bytes: usize,
    /// Declaration order within the session. ROLLBACK TO SAVEPOINT closes
    /// every cursor declared at or after the savepoint's mark.
    pub seq: u64,
    /// The resumable producer of a lazy cursor; `None` for a materialized one.
    pub lazy: Option<SeriesCursor>,
}

/// The resumable row producer behind a lazy cursor.
///
/// It covers exactly one query shape: a select over a single
/// `generate_series(start, stop[, step])` call with constant integer
/// arguments. That source reads no table, so there is no snapshot to pin and
/// nothing another session can change between FETCH calls; the producer's whole
/// state is a handful of integers plus the parsed select list. A FETCH clones
/// this state, advances the clone, and stores it back only when the FETCH
/// succeeded, so a refused or failed FETCH never moves the cursor.
#[derive(Debug, Clone)]
pub(crate) struct SeriesCursor {
    /// Next series value to emit.
    pub next: i64,
    pub stop: i64,
    pub step: i64,
    /// Set once the series has run past `stop` (or its next value overflowed).
    pub finished: bool,
    /// The series' single output column: the row shape the WHERE clause and
    /// the select list evaluate against.
    pub col_meta: Vec<ColMeta>,
    pub selection: Option<Expr>,
    pub projection: Vec<SelectItem>,
    /// Query-level OFFSET rows still to discard, counted after WHERE.
    pub skip: usize,
    /// Query-level LIMIT rows still allowed; `None` is unbounded.
    pub remaining: Option<usize>,
    /// Rows returned or skipped by FETCH so far: the cursor's row index.
    pub emitted: usize,
    /// The cursor ran off the end and sits after the last row.
    pub after_last: bool,
    /// The row the cursor is on (what `FETCH 0` returns again).
    pub current: Option<Row>,
}

impl SeriesCursor {
    /// The next raw series value, advancing the series.
    pub fn next_value(&mut self) -> Option<i64> {
        if self.finished {
            return None;
        }
        let in_range = if self.step > 0 {
            self.next <= self.stop
        } else {
            self.next >= self.stop
        };
        if !in_range {
            self.finished = true;
            return None;
        }
        let value = self.next;
        match self.next.checked_add(self.step) {
            Some(n) => self.next = n,
            None => self.finished = true,
        }
        Some(value)
    }

    /// PostgreSQL cursor position: 0 before the first row, the row index on a
    /// row, one past the last row once the producer is exhausted.
    pub fn position(&self) -> usize {
        if self.after_last {
            self.emitted.saturating_add(1)
        } else {
            self.emitted
        }
    }

    /// The first value of the series as a one-column row, used once at DECLARE
    /// to type the select list the way a materialized cursor types it from its
    /// first row. `None` for an empty series.
    pub fn probe_row(&self) -> Option<Row> {
        let in_range = if self.step > 0 {
            self.next <= self.stop
        } else {
            self.next >= self.stop
        };
        in_range.then(|| vec![Value::Int64(self.next)])
    }
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
