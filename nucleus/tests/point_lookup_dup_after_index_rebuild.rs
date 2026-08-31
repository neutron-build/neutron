//! Regression gate: a PK point lookup must never return a row twice after
//! UPDATE on a table whose indexes were (re)built over existing versions.
//!
//! The bug (probe_index_coherence, 2026-08-28): `create_index` built its
//! `idx.map` from EVERY row version — including dead ones. `index_lookup_sync`
//! serves that map with no MVCC visibility filtering, so after an UPDATE the
//! map held both the tombstoned version and its successor and
//! `SELECT ... WHERE id = K` returned the row twice.
//!
//! The trigger chain: an IvfFlat index makes `incremental_maintenance_eligible`
//! false (positional postings), so UPDATE falls back to
//! `rebuild_table_derived_state`, which re-runs `create_index` for every btree
//! index — including the implicit `<table>_pkey` — over a table that now has a
//! dead version. Minimal shape: one vector index, one row inserted after index
//! creation, one UPDATE of that row.

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;
use std::sync::Arc;

async fn ids_of(ex: &Executor, sql: &str) -> Vec<i64> {
    let r = ex.execute(sql).await.unwrap();
    let nucleus::executor::ExecResult::Select { rows, .. } = &r[0] else {
        panic!("expected rows for: {sql}");
    };
    rows.iter()
        .filter_map(|row| match row.first() {
            Some(Value::Int32(n)) => Some(*n as i64),
            Some(Value::Int64(n)) => Some(*n),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn point_lookup_dup_after_index_rebuild() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn nucleus::storage::StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    let steps: Vec<&str> = vec![
        "CREATE TABLE t (id INT PRIMARY KEY, val INT, code TEXT, v VECTOR(6))",
        "INSERT INTO t VALUES (1, 0, 'c1', VECTOR('[0.5500002,-2.75,7,-2.9,-4.74,3.38]'))",
        "INSERT INTO t VALUES (2, 5, 'c2', VECTOR('[-3.65,-3.9499998,6.040001,-6.2,-2.9099998,-1.1800003]'))",
        "INSERT INTO t VALUES (3, 0, 'c3', VECTOR('[-3.35,2.6000004,-3.1,8.059999,5.99,2.4399996]'))",
        "CREATE INDEX t_v ON t USING ivfflat (v)",
        "INSERT INTO t VALUES (4, 18, 'c4', VECTOR('[1.4099998,-2.2399998,8.59,-1.0600004,-9.56,9.34]'))",
        "UPDATE t SET val = 4 WHERE id = 4",
        "INSERT INTO t VALUES (5, 15, 'c5', VECTOR('[4.59,-2.65,-1.5900002,3.3100004,-6.52,-9.55]'))",
    ];
    for (i, sql) in steps.iter().enumerate() {
        let r = ex.execute(sql).await;
        assert!(r.is_ok(), "step {i} failed: {sql} -> {r:?}",);

        let scan = ids_of(&ex, "SELECT id FROM t ORDER BY id").await;
        let mut sorted = scan.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            scan, sorted,
            "duplicate visible versions after step {i}: {sql}"
        );

        for id in &scan {
            let got = ids_of(&ex, &format!("SELECT id FROM t WHERE id = {id}")).await;
            assert_eq!(
                got,
                vec![*id],
                "pk point lookup id={id} after step {i} ({sql}): got {got:?}"
            );
        }
    }
}
