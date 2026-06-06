//! Regression tests for correctness bugs surfaced by the differential fuzzer
//! (`src/bin/fuzz.rs`, Nucleus vs SQLite). Each case is the minimized repro the
//! fuzzer's delta-debugger produced for a distinct root cause.
#![cfg(feature = "server")]
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
async fn ids(ex: &Executor, sql: &str) -> Vec<i32> {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows
            .into_iter()
            .map(|r| match r[0] {
                Value::Int32(n) => n,
                Value::Int64(n) => n as i32,
                ref v => panic!("non-int id: {v:?}"),
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

/// Zone-map stale after DELETE+INSERT: DELETE cleared the table's zone map, but
/// the next INSERT only added its own row's stats, leaving surviving rows
/// unrepresented. A granule reflecting only the NULL-valued new row then made
/// `can_skip_granule` prune the whole granule for any range / IS NOT NULL
/// predicate on the nullable column — dropping valid rows.
#[tokio::test]
async fn zone_map_survivor_not_pruned_after_delete_insert() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c5 INTEGER)").await.unwrap();
    ex.execute("INSERT INTO t (id,c5) VALUES (15,-3)").await.unwrap();
    ex.execute("INSERT INTO t (id,c5) VALUES (16,17)").await.unwrap();
    ex.execute("DELETE FROM t WHERE id = 16").await.unwrap();
    ex.execute("INSERT INTO t (id,c5) VALUES (17,NULL)").await.unwrap();

    // id15 (c5=-3) matches; id17 (c5=NULL) does not.
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c5 < 16 ORDER BY id").await, vec![15]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c5 IS NOT NULL ORDER BY id").await, vec![15]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c5 > -100 ORDER BY id").await, vec![15]);
}

/// Same defect, multi-column / UPDATE variant: every range filter on a nullable
/// column must still see surviving rows after a DELETE then INSERT.
#[tokio::test]
async fn zone_map_nullable_columns_after_mutations() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL, c2 TEXT NOT NULL, c3 REAL, c4 INTEGER NOT NULL, c5 INTEGER, c6 INTEGER, c7 TEXT NOT NULL)").await.unwrap();
    ex.execute("INSERT INTO t (id,c1,c2,c3,c4,c5,c6,c7) VALUES (15,13,'red',4.5,0,-3,-4,'red'),(16,16,'str3',-2.6,18,17,15,'amber')").await.unwrap();
    ex.execute("DELETE FROM t WHERE c2 >= 'str2'").await.unwrap();
    ex.execute("INSERT INTO t (id,c1,c2,c3,c4,c5,c6,c7) VALUES (17,-1,'amber',NULL,20,NULL,NULL,'amber')").await.unwrap();

    // Nullable columns c3/c5/c6: id15 has a value, id17 is NULL.
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c5 < 99 ORDER BY id").await, vec![15]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c6 < 99 ORDER BY id").await, vec![15]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c3 < 99 ORDER BY id").await, vec![15]);
    // NOT NULL columns stay correct too.
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c1 < 99 ORDER BY id").await, vec![15, 17]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c4 < 99 ORDER BY id").await, vec![15, 17]);
}

/// `x NOT IN (<empty subquery>)` must be TRUE for every row, including NULLs:
/// `x IN ()` is unconditionally FALSE in SQL (nothing to be equal to), so the
/// 3-valued helper must short-circuit the empty set BEFORE the NULL-value check.
#[tokio::test]
async fn not_in_empty_subquery_includes_null_rows() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c4 INTEGER)").await.unwrap();
    ex.execute("INSERT INTO t (id,c4) VALUES (1,5),(2,NULL),(3,7),(4,NULL)").await.unwrap();

    // Inner subquery (c4 = 18) is empty → NOT IN is TRUE for all 4 rows.
    assert_eq!(
        ids(&ex, "SELECT id FROM t WHERE c4 NOT IN (SELECT c4 FROM t WHERE c4 = 18) ORDER BY id").await,
        vec![1, 2, 3, 4]
    );
    // IN over the empty set is FALSE for all rows (including NULLs).
    assert_eq!(
        ids(&ex, "SELECT id FROM t WHERE c4 IN (SELECT c4 FROM t WHERE c4 = 18) ORDER BY id").await,
        Vec::<i32>::new()
    );
}

/// SIMD fast-filter truncated a non-integral float bound to i64 before an
/// integer comparison: `c1 > -2.333` became `c1 > -2`, wrongly dropping c1 = -2
/// (and `c1 = 2.5` would have matched c1 = 2). Non-integral float bounds on
/// integer columns must fall back to the exact int↔float-promoting filter.
#[tokio::test]
async fn int_column_vs_fractional_float_bound() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)").await.unwrap();
    ex.execute("INSERT INTO t (id,c1) VALUES (1,0),(2,-2),(3,-3)").await.unwrap();

    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c1 > -2.333 ORDER BY id").await, vec![1, 2]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE -2.333 < c1 ORDER BY id").await, vec![1, 2]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c1 < -2.333 ORDER BY id").await, vec![3]);
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c1 = 2.5 ORDER BY id").await, Vec::<i32>::new());
    assert_eq!(ids(&ex, "SELECT id FROM t WHERE c1 <> 2.5 ORDER BY id").await, vec![1, 2, 3]);
}

/// The same fractional-bound bug as observed through a scalar AVG subquery:
/// AVG(c1) = -28/12 = -2.333, so `c1 > AVG(c1)` must include the eight -2 rows.
#[tokio::test]
async fn gt_avg_scalar_subquery() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)").await.unwrap();
    ex.execute("INSERT INTO t (id,c1) VALUES (1,0),(2,-2),(5,-2),(6,-2),(8,-2),(11,-2),(12,-2),(13,-5),(15,-4),(16,-2),(17,-2),(20,-3)").await.unwrap();
    assert_eq!(
        ids(&ex, "SELECT id FROM t WHERE c1 > (SELECT AVG(c1) FROM t) ORDER BY id").await,
        vec![1, 2, 5, 6, 8, 11, 12, 16, 17]
    );
}
