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
pub(super) const MUTATING_SCALAR_FNS: [&str; 55] = [
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
pub(super) const MUTATING_SCALAR_FNS_EXTRA: [&str; 5] = [
    "TS_RETENTION",
    "VECTOR_DELETE",
    "VECTOR_INSERT",
    "VERSION_BRANCH",
    "VERSION_COMMIT",
];

/// Whether a built-in scalar function mutates durable state.
/// `fname` is the upper-cased function name used by the dispatcher.
///
/// The two arrays above are no longer the only authority: anything
/// `scalar_fns::SIDE_EFFECTING_FN_NAMES` declares as writing is refused too.
/// Those two lists answered the same question and had drifted apart in both
/// directions — six functions (NEXTVAL, SETVAL, RETENTION_SET,
/// STREAM_XREADGROUP, SUBSCRIBE, UNSUBSCRIBE) were declared side-effecting
/// there and admitted here, so a server that had just refused an INSERT for
/// want of disk would still allocate a durable identifier or claim stream
/// entries. `mutating_registries_agree` now checks both directions. (NU-216)
pub(super) fn scalar_fn_mutates(fname: &str) -> bool {
    MUTATING_SCALAR_FNS.binary_search(&fname).is_ok()
        || MUTATING_SCALAR_FNS_EXTRA.binary_search(&fname).is_ok()
        || side_effecting(fname)
}

#[cfg(feature = "server")]
fn side_effecting(fname: &str) -> bool {
    super::scalar_fns::SIDE_EFFECTING_FN_NAMES
        .binary_search(&fname)
        .is_ok()
}

#[cfg(not(feature = "server"))]
fn side_effecting(_fname: &str) -> bool {
    false
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

    /// Raw-text admission gate for the extension dispatch block (EXE-8).
    ///
    /// Extension commands are matched on raw text and return before
    /// `execute_statement_inner`, so they never met the statement-level
    /// gate: on a degraded (read-only) server, ALTER SEQUENCE kept
    /// persisting sequences.json and BACKUP kept writing files while
    /// INSERT was refused 53100. This shim classifies the raw text with
    /// the same prefixes the dispatch block matches: read-only and
    /// session-local extensions are admitted, mutating ones are refused
    /// through the standard admission error, and text matching NO
    /// extension arm is left to the parsed path, where
    /// [`Self::admit_statement`] remains the (fail-closed) gate. CALL no
    /// longer has a raw arm — the parser routes it and `Statement::Call`
    /// is already classified mutating.
    pub(super) fn admit_extension(&self, upper: &str) -> Result<(), ExecError> {
        if !self.service.is_read_only() {
            return Ok(());
        }
        let cmd = upper.trim_end().trim_end_matches(';').trim_end();
        // Reads and session-local commands. FETCH SUBSCRIPTION/UNSUBSCRIBE
        // only touch this session's subscription state; MEMORY PRESSURE is
        // advisory and writes nothing durable.
        let admitted = cmd.starts_with("SHOW TABLE STATS ")
            || cmd.starts_with("SHOW MODELS")
            || cmd.starts_with("SHOW PROCEDURES")
            || cmd.starts_with("SHOW MASKING POLICIES")
            || cmd.starts_with("SHOW MEMORY")
            || cmd.starts_with("SHOW WAL_STATUS")
            || cmd.starts_with("SHOW TRANSACTIONS")
            || cmd.starts_with("SHOW BRANCHES")
            || cmd.starts_with("CACHE_GET ")
            || cmd.starts_with("CACHE_GET(")
            || cmd == "CACHE_STATS"
            || cmd == "CACHE_STATS()"
            || cmd == "MEMORY PRESSURE"
            || cmd == "CHECKPOINT"
            || cmd.starts_with("FETCH SUBSCRIPTION ")
            || cmd.starts_with("UNSUBSCRIBE ");
        if admitted {
            return Ok(());
        }
        let mutating: [(&str, &str); 18] = [
            ("CREATE MASKING POLICY", "CREATE MASKING POLICY"),
            ("DROP MASKING POLICY", "DROP MASKING POLICY"),
            ("ALTER SEQUENCE ", "ALTER SEQUENCE"),
            ("CACHE_SET ", "CACHE_SET"),
            ("CACHE_SET(", "CACHE_SET"),
            ("CACHE_DEL ", "CACHE_DEL"),
            ("CACHE_DEL(", "CACHE_DEL"),
            ("CACHE_TTL ", "CACHE_TTL"),
            ("CACHE_TTL(", "CACHE_TTL"),
            ("BACKUP DATABASE TO ", "BACKUP"),
            ("REFRESH MATERIALIZED VIEW ", "REFRESH MATERIALIZED VIEW"),
            ("DROP MATERIALIZED VIEW ", "DROP MATERIALIZED VIEW"),
            ("CREATE MODEL ", "CREATE MODEL"),
            ("DROP MODEL ", "DROP MODEL"),
            ("CREATE PROCEDURE ", "CREATE PROCEDURE"),
            ("CREATE OR REPLACE PROCEDURE ", "CREATE PROCEDURE"),
            ("DROP PROCEDURE ", "DROP PROCEDURE"),
            ("SUBSCRIBE ", "SUBSCRIBE"),
        ];
        for (prefix, label) in mutating {
            if cmd.starts_with(prefix) {
                return self.service.admit_write(label);
            }
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
