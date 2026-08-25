//! Executor test suite — split into focused test modules.

use super::*;
use crate::catalog::Catalog;
use crate::storage::MemoryEngine;

/// Helper: create an executor backed by in-memory storage.
pub(super) fn test_executor() -> Executor {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    Executor::new(catalog, storage)
}

/// Helper: execute SQL and return results.
pub(super) async fn exec(executor: &Executor, sql: &str) -> Vec<ExecResult> {
    executor.execute(sql).await.expect("SQL execution failed")
}

/// Helper: extract rows from a SELECT result.
pub(super) fn rows(result: &ExecResult) -> &Vec<Row> {
    match result {
        ExecResult::Select { rows, .. } => rows,
        _ => panic!("expected SELECT result"),
    }
}

/// Helper: extract the single value from a 1-row, 1-col result.
pub(super) fn scalar(result: &ExecResult) -> &Value {
    let r = rows(result);
    assert_eq!(r.len(), 1, "expected 1 row");
    assert_eq!(r[0].len(), 1, "expected 1 column");
    &r[0][0]
}

/// Helper: extract a TEXT scalar (panics on any other shape).
pub(super) fn text_of(result: ExecResult) -> String {
    match scalar(&result) {
        Value::Text(s) => s.clone(),
        other => panic!("expected TEXT, got {other:?}"),
    }
}

mod test_2pl_census; // R6: serializable anomaly census for the DISK engine (strict 2PL)
mod test_admin;
mod test_aggregate_overflow_checked; // QPP-4 family: aggregate overflow errors on every path
mod test_alter_policy; // N14: ALTER POLICY and policy introspection
mod test_ast_cache_utf8; // AST-cache literal extraction must be UTF-8-safe (WIR-4 family)
mod test_audit_events; // N18: durable security audit events
mod test_cache_coherence; // M2: cache + specialty-index invalidation oracle
mod test_call_pipeline; // EXE-1/5 + PRC-1/3/4/5/7: the CALL pipeline end-to-end
mod test_collections;
mod test_copy; // COPY FROM STDIN payload reconstruction
mod test_cross_model;
mod test_cross_model_atomicity; // S63 slice 1: SQL+streams discard-on-no-commit-record
mod test_ddl;
mod test_dml;
mod test_doc_collections; // GO-055: `collection` must isolate, not decorate
mod test_drop_table_dependents; // CAT-5: DROP TABLE must respect FK and matview dependents
mod test_drop_view_dependents; // CAT-11: DROP VIEW must refuse dependents and clear dep keys
mod test_durability_format; // M3: format rejection + full-state recovery
mod test_e2e_smoke; // End-to-end smoke tests exercising all Nucleus capabilities
mod test_filter_lazy; // Phase 2C: Lazy materialization for WHERE clause filtering
mod test_fts_index; // Table-attached FTS: USING FTS, @@, BM25, hybrid RRF
mod test_index;
mod test_index_path_coverage; // which WHERE forms actually reach an index
mod test_integration;
mod test_join_plan_path; // which JOIN spellings actually reach the plan executor
mod test_jsonb;
mod test_logical_dump; // T2.1: logical (SQL-text) backup round-trip
mod test_lost_update; // N22: concurrent UPDATEs must not silently lose writes
mod test_masking;
mod test_masking_ddl; // N13: the CREATE MASKING POLICY surface
mod test_memory_budget; // T1.2: query memory-budget enforcement (gating)
mod test_meta_persistence;
mod test_module_wiring;
mod test_multimodel;
mod test_mv_writetime; // Phase 3: Write-time materialized view refresh
mod test_password_lifecycle; // N16: password creation, rotation, expiry
mod test_pk_write_cost;
mod test_plan_cache_session_isolation; // the plan-cache key hint must not cross sessions
mod test_predicate_agreement; // SELECT p ≡ WHERE p, with and without an index
mod test_query;
mod test_read_only_mode; // M10: degraded read-only write admission
mod test_rename_table_dependents; // CAT-4: RENAME TO must rewrite FK/view/matview dependents
mod test_replacing_engine_recovery; // replacing_mergetree engine metadata must survive a restart
mod test_rls;
mod test_rls_surfaces; // M5: adversarial alternate-surface RLS exfiltration matrix
mod test_row_locks;
mod test_s33_executor_edges; // S33-11/S33-14: hash-join decline + SIMD case-insensitive binding
mod test_scalar_fns;
mod test_semi_anti_joins; // QPP-1a/QPP-12: SEMI/ANTI refusals + hash-join residual propagation
mod test_specialty_persistence;
mod test_specialty_surface_guard; // N15: the specialty fail-closed guard, audited against the dispatcher
mod test_spill_sweep; // B2: executor sweeps orphaned query-spill files on startup
mod test_sql_wal_ack_durability; // R4: an acked autocommit SQL write is fsync-durable
mod test_ssi_census; // B1: end-to-end SSI anomaly census (gate for MVCC scan changes)
mod test_streaming_aggregate; // Grace hash aggregation: bounded-memory GROUP BY with spill
mod test_streaming_filter; // Phase 1.2 read-side: streaming WHERE filter (SIREAD-safe full scan)
mod test_streaming_join; // Grace hash join: bounded-memory two-table equi-JOIN with spill
mod test_streaming_lazy; // Lazy per-partition/pair output emitters for the Grace operators
mod test_streaming_metamorphic; // streaming ≡ materialized over random queries (transitive SQLite oracle)
mod test_streaming_scan; // Phase 1.1: opt-in streaming scan (SET stream_results = on)
mod test_streams_persistence; // S31-04/S31-05: stream rollback compensation + consumer-group durability across a restart
mod test_table_engine_checkpoint; // R4: per-table engine WAL compaction is reachable, not just implemented
mod test_temporal_predicates; // mixed temporal literal/column comparisons
mod test_temporal_range_cost; // S66: TIMESTAMP/DATE range predicates must prune // S65: UPDATE/DELETE by PK must not scan the table
mod test_triggers; // EXE-2: row-binding tables must never touch user tables named _new/_old
mod test_txn;
mod test_txn_lazy_snapshot; // R8: BEGIN/SAVEPOINT do not clone the whole database // Phase 4: JSONB @> containment, GIN indexes, subscript syntax
