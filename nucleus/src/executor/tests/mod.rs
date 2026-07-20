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

mod test_admin;
mod test_collections;
mod test_cross_model;
mod test_ddl;
mod test_dml;
mod test_e2e_smoke; // End-to-end smoke tests exercising all Nucleus capabilities
mod test_filter_lazy; // Phase 2C: Lazy materialization for WHERE clause filtering
mod test_index;
mod test_integration;
mod test_jsonb;
mod test_logical_dump; // T2.1: logical (SQL-text) backup round-trip
mod test_memory_budget; // T1.2: query memory-budget enforcement (gating)
mod test_meta_persistence;
mod test_module_wiring;
mod test_multimodel;
mod test_mv_writetime; // Phase 3: Write-time materialized view refresh
mod test_query;
mod test_rls;
mod test_scalar_fns;
mod test_specialty_persistence;
mod test_spill_sweep; // B2: executor sweeps orphaned query-spill files on startup
mod test_ssi_census; // B1: end-to-end SSI anomaly census (gate for MVCC scan changes)
mod test_streaming_aggregate; // Grace hash aggregation: bounded-memory GROUP BY with spill
mod test_streaming_join; // Grace hash join: bounded-memory two-table equi-JOIN with spill
mod test_streaming_filter; // Phase 1.2 read-side: streaming WHERE filter (SIREAD-safe full scan)
mod test_streaming_lazy; // Lazy per-partition/pair output emitters for the Grace operators
mod test_streaming_scan; // Phase 1.1: opt-in streaming scan (SET stream_results = on)
mod test_txn; // Phase 4: JSONB @> containment, GIN indexes, subscript syntax
