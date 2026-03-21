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

mod test_scalar_fns;
mod test_query;
mod test_dml;
mod test_ddl;
mod test_admin;
mod test_multimodel;
mod test_index;
mod test_txn;
mod test_integration;
mod test_collections;
mod test_cross_model;
mod test_meta_persistence;
mod test_specialty_persistence;
mod test_module_wiring;
mod test_filter_lazy;  // Phase 2C: Lazy materialization for WHERE clause filtering
mod test_e2e_smoke;    // End-to-end smoke tests exercising all Nucleus capabilities
mod test_mv_writetime; // Phase 3: Write-time materialized view refresh
mod test_jsonb;        // Phase 4: JSONB @> containment, GIN indexes, subscript syntax
