#![cfg(feature = "server")]
//! Regression tests for correctness bugs surfaced by the differential fuzzer
//! (`src/bin/fuzz.rs`, Nucleus vs SQLite). Each test pins one bug class that
//! previously diverged from SQLite. See the fuzzer for how these were found.
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use std::sync::Arc;

async fn fresh() -> Arc<Executor> {
    let c = Arc::new(Catalog::new());
    let s: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    Arc::new(Executor::new(c, s))
}

async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows,
        o => panic!("expected Select, got {o:?}"),
    }
}

async fn seed(ex: &Executor) {
    ex.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, e INTEGER NOT NULL, a INTEGER, g TEXT NOT NULL)",
    )
    .await
    .unwrap();
    // a has NULLs; e is the non-null grouping/order column.
    ex.execute(
        "INSERT INTO t (id,e,a,g) VALUES \
         (1,0,5,'red'),(2,0,NULL,'red'),(3,1,7,'blue'),\
         (4,1,NULL,'blue'),(5,2,9,'amber'),(6,2,NULL,'amber')",
    )
    .await
    .unwrap();
}

/// COUNT(col) must exclude NULLs; only COUNT(*) counts every row.
#[tokio::test]
async fn count_col_excludes_nulls() {
    let ex = fresh().await;
    seed(&ex).await;
    assert_eq!(
        rows(&ex, "SELECT COUNT(*) FROM t").await[0][0],
        Value::Int64(6)
    );
    assert_eq!(
        rows(&ex, "SELECT COUNT(a) FROM t").await[0][0],
        Value::Int64(3)
    );
}

/// MIN/MAX/SUM over a GROUP BY must not be silently dropped by the fast path,
/// and SUM must equal the true non-NULL sum (not avg*count).
#[tokio::test]
async fn group_by_aggregates_with_nulls() {
    let ex = fresh().await;
    seed(&ex).await;
    let r = rows(
        &ex,
        "SELECT e, MIN(a), MAX(a), SUM(a), COUNT(a) FROM t GROUP BY e ORDER BY e",
    )
    .await;
    assert_eq!(
        r[0],
        vec![
            Value::Int32(0),
            Value::Int32(5),
            Value::Int32(5),
            Value::Int64(5),
            Value::Int64(1)
        ]
    );
    assert_eq!(
        r[1],
        vec![
            Value::Int32(1),
            Value::Int32(7),
            Value::Int32(7),
            Value::Int64(7),
            Value::Int64(1)
        ]
    );
    assert_eq!(
        r[2],
        vec![
            Value::Int32(2),
            Value::Int32(9),
            Value::Int32(9),
            Value::Int64(9),
            Value::Int64(1)
        ]
    );

    // SUM(a) GROUP BY with NULLs, single aggregate (hits the fast path).
    let r = rows(&ex, "SELECT e, SUM(a) FROM t GROUP BY e ORDER BY e").await;
    assert_eq!(r[0], vec![Value::Int32(0), Value::Int64(5)]);
}

/// `x NOT IN (...)` / `NOT (x IN ...)` must EXCLUDE rows where x IS NULL
/// (NULL IN (..) is unknown; NOT unknown is unknown; WHERE drops it).
#[tokio::test]
async fn null_not_in_three_valued_logic() {
    let ex = fresh().await;
    seed(&ex).await;
    // a IS NULL for ids 2,4,6. Those must NOT appear in NOT IN results.
    let r = rows(&ex, "SELECT id FROM t WHERE NOT (a IN (5,7)) ORDER BY id").await;
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        ids,
        vec![5],
        "only id 5 (a=9, not null, not in list) qualifies"
    );
}

/// ORDER BY a non-projected column must sort by it even when the projection
/// is a single indexed column (index-only scan must not drop the sort).
#[tokio::test]
async fn order_by_non_projected_via_indexed_projection() {
    let ex = fresh().await;
    seed(&ex).await;
    // SELECT id ORDER BY e: id is the PK (indexed), e is not projected.
    let r = rows(&ex, "SELECT id FROM t ORDER BY e ASC, id ASC").await;
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
}

/// A `col = literal` point lookup must still enforce the rest of the predicate
/// (contradictory comparisons must yield zero rows).
#[tokio::test]
async fn point_lookup_keeps_residual_filter() {
    let ex = fresh().await;
    seed(&ex).await;
    // id = 1 but e >= 7 is false (e=0) → no rows.
    assert!(
        rows(&ex, "SELECT * FROM t WHERE e >= 7 AND id = 1")
            .await
            .is_empty()
    );
    // Contradictory equality+range on the same column.
    assert!(
        rows(&ex, "SELECT * FROM t WHERE id > 6 AND id = 1")
            .await
            .is_empty()
    );
}

/// Plan cache must not reuse a plan across different LIMIT values: running a
/// small LIMIT first must not poison a later larger LIMIT of the same shape.
#[tokio::test]
async fn limit_not_shared_across_plan_cache() {
    let ex = fresh().await;
    seed(&ex).await;
    let q = |k: u32| format!("SELECT * FROM t ORDER BY e ASC, id ASC LIMIT {k}");
    // Warm the plan cache with LIMIT 1, then ask for LIMIT 4.
    assert_eq!(rows(&ex, &q(1)).await.len(), 1);
    assert_eq!(
        rows(&ex, &q(4)).await.len(),
        4,
        "LIMIT 4 must return 4 rows after LIMIT 1"
    );
    assert_eq!(rows(&ex, &q(6)).await.len(), 6);
    assert_eq!(rows(&ex, &q(2)).await.len(), 2);
}
