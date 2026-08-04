//! Write-admission classification.
//!
//! When the server is degraded to read-only (disk watermark crossed, or an
//! operator request), every statement that could add durable state must be
//! refused with an actionable error, while reads keep working so operators
//! can diagnose and applications can keep serving traffic.
//!
//! Classification is **fail-closed**: only an explicit allow-list of
//! non-mutating statements is admitted, and anything the parser produces that
//! this module has not seen is treated as a write. A new statement kind
//! therefore defaults to "refused while degraded" rather than silently
//! consuming the last of the disk.
//!
//! Two statement families are deliberately admitted *even while degraded*
//! because they are the operator's recovery path — refusing them would make
//! the degraded state unrecoverable from SQL:
//!
//! * `VACUUM` — reclaims space inside the data files.
//! * `CHECKPOINT` — flushes and truncates WAL segments.

use sqlparser::ast::Statement;

use super::{ExecError, Executor};

/// Whether a parsed statement can add durable state.
///
/// Returns `false` only for statements that are known to be reads, session
/// state, transaction control, or space-reclaiming maintenance.
pub(super) fn statement_mutates(stmt: &Statement) -> bool {
    !matches!(
        stmt,
        // Reads.
        Statement::Query(_)
            | Statement::Explain { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowVariable { .. }
            // Session-local state: no durable effect.
            | Statement::Set(_)
            | Statement::Reset(_)
            | Statement::Discard { .. }
            | Statement::Deallocate { .. }
            | Statement::Prepare { .. }
            | Statement::Declare { .. }
            | Statement::Fetch { .. }
            | Statement::Close { .. }
            | Statement::LISTEN { .. }
            | Statement::UNLISTEN { .. }
            // Transaction control. A degraded server must still let open
            // transactions finish or roll back.
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Savepoint { .. }
            | Statement::ReleaseSavepoint { .. }
            // Space reclamation is the recovery path out of a disk watermark.
            | Statement::Vacuum(_)
    )
}

/// Human-readable label for the refusal message.
pub(super) fn statement_label(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Copy { .. } => "COPY",
        Statement::Truncate(_) => "TRUNCATE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::AlterTable(_) => "ALTER TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::Analyze(_) => "ANALYZE",
        Statement::Execute { .. } => "EXECUTE",
        Statement::Call(_) => "CALL",
        _ => "this statement",
    }
}

/// Specialty-store SQL functions that mutate durable state.
///
/// The relational statement gate above cannot see these: `SELECT kv_set(...)`
/// parses as an ordinary query. Listed explicitly rather than pattern-matched
/// so a read never gets refused by accident and, more importantly, so adding
/// a new mutating function is a deliberate edit here.
const MUTATING_SCALAR_FNS: [&str; 55] = [
    "BLOB_DELETE",
    "BLOB_STORE",
    "BLOB_TAG",
    "COLUMNAR_INSERT",
    "CYPHER",
    "DATALOG_ASSERT",
    "DATALOG_CLEAR",
    "DATALOG_IMPORT",
    "DATALOG_IMPORT_GRAPH",
    "DATALOG_IMPORT_NODES",
    "DATALOG_RETRACT",
    "DATALOG_RULE",
    "DB_BRANCH_CREATE",
    "DB_BRANCH_DELETE",
    "DB_BRANCH_MERGE",
    "DOC_DELETE",
    "DOC_INSERT",
    "DOC_UPDATE",
    "FTS_INDEX",
    "FTS_INDEX_FACETED",
    "FTS_REMOVE",
    "GRAPH_ADD_EDGE",
    "GRAPH_ADD_NODE",
    "GRAPH_DELETE_EDGE",
    "GRAPH_DELETE_NODE",
    "KV_CDEL",
    "KV_CEXPIRE",
    "KV_DEL",
    "KV_EXPIRE",
    "KV_FLUSHDB",
    "KV_HDEL",
    "KV_HSET",
    "KV_INCR",
    "KV_LPOP",
    "KV_LPUSH",
    "KV_PFADD",
    "KV_PFMERGE",
    "KV_RPOP",
    "KV_RPUSH",
    "KV_SADD",
    "KV_SET",
    "KV_SETNX",
    "KV_SREM",
    "KV_ZADD",
    "KV_ZREM",
    "PROC_DROP",
    "PROC_REGISTER",
    "PUBSUB_PUBLISH",
    "SPARSE_INSERT",
    "SPARSE_REMOVE",
    "STREAM_XACK",
    "STREAM_XADD",
    "STREAM_XGROUP_CREATE",
    "TENSOR_STORE",
    "TS_INSERT",
];

/// Additional mutating functions that do not fit the sorted array above
/// (kept separate so the array stays a compile-time-checked constant list).
const MUTATING_SCALAR_FNS_EXTRA: [&str; 5] = [
    "TS_RETENTION",
    "VECTOR_DELETE",
    "VECTOR_INSERT",
    "VERSION_BRANCH",
    "VERSION_COMMIT",
];

/// Whether a built-in scalar function mutates durable state.
/// `fname` is the upper-cased function name used by the dispatcher.
pub(super) fn scalar_fn_mutates(fname: &str) -> bool {
    MUTATING_SCALAR_FNS.binary_search(&fname).is_ok()
        || MUTATING_SCALAR_FNS_EXTRA.binary_search(&fname).is_ok()
}

impl Executor {
    /// The shared write-admission state (read-write vs degraded read-only).
    pub fn service(&self) -> &std::sync::Arc<crate::ops::ServiceState> {
        &self.service
    }

    /// Replace the write-admission state, so the server-level disk guard and
    /// the executor share one gate.
    pub fn set_service_state(&mut self, service: std::sync::Arc<crate::ops::ServiceState>) {
        self.service = service;
    }

    /// Statement-level admission gate.
    #[inline]
    pub(super) fn admit_statement(&self, stmt: &Statement) -> Result<(), ExecError> {
        if self.service.is_read_only() && statement_mutates(stmt) {
            return self.service.admit_write(statement_label(stmt));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap_or_else(|e| panic!("parse {sql}: {e}"))
            .remove(0)
    }

    #[test]
    fn writes_are_classified_as_writes() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "CREATE TABLE t (a INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN b INT",
            "CREATE INDEX i ON t (a)",
            "TRUNCATE TABLE t",
            "COPY t FROM STDIN;",
            "GRANT SELECT ON t TO app",
            "ANALYZE t",
            // EXECUTE can run a prepared INSERT, so it must be refused.
            "EXECUTE p",
        ] {
            assert!(statement_mutates(&parse(sql)), "{sql} should be a write");
        }
    }

    #[test]
    fn reads_and_session_state_are_not_writes() {
        for sql in [
            "SELECT 1",
            "SELECT * FROM t WHERE a = 1",
            "EXPLAIN SELECT 1",
            "SET search_path = public",
            "RESET ALL",
            "DISCARD ALL",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT s1",
            "PREPARE p AS SELECT 1",
            "DEALLOCATE p",
            "DECLARE c CURSOR FOR SELECT 1",
            "CLOSE c",
            "LISTEN ch",
            "UNLISTEN ch",
            // Space reclamation must stay available while degraded, or the
            // disk watermark becomes unrecoverable from SQL.
            "VACUUM",
            "VACUUM t",
        ] {
            assert!(
                !statement_mutates(&parse(sql)),
                "{sql} should not be a write"
            );
        }
    }

    #[test]
    fn mutating_scalar_function_lists_are_sorted_for_binary_search() {
        let mut sorted = MUTATING_SCALAR_FNS;
        sorted.sort_unstable();
        assert_eq!(
            sorted, MUTATING_SCALAR_FNS,
            "MUTATING_SCALAR_FNS must be sorted"
        );
        let mut sorted_extra = MUTATING_SCALAR_FNS_EXTRA;
        sorted_extra.sort_unstable();
        assert_eq!(sorted_extra, MUTATING_SCALAR_FNS_EXTRA);
    }

    #[test]
    fn specialty_write_functions_are_recognised() {
        for f in [
            "KV_SET",
            "KV_DEL",
            "KV_INCR",
            "DOC_INSERT",
            "GRAPH_ADD_NODE",
            "FTS_INDEX",
            "TS_INSERT",
            "VECTOR_INSERT",
            "VECTOR_DELETE",
            "BLOB_STORE",
            "STREAM_XADD",
            "VERSION_COMMIT",
            "VERSION_BRANCH",
            "CYPHER",
        ] {
            assert!(scalar_fn_mutates(f), "{f} should be a write");
        }
        for f in [
            "KV_GET",
            "KV_EXISTS",
            "KV_TTL",
            "DOC_GET",
            "GRAPH_NEIGHBORS",
            "FTS_SEARCH",
            "TS_LAST",
            "VECTOR_SEARCH",
            "BLOB_GET",
            "STREAM_XREAD",
            "UPPER",
            "COUNT",
            "CDC_READ",
            "COLUMNAR_SUM",
        ] {
            assert!(!scalar_fn_mutates(f), "{f} should be a read");
        }
    }
}
