//! Regression for `chained_cte`: a later CTE in a single WITH list may
//! reference any CTE defined earlier in that same list (SQL standard /
//! PostgreSQL: "each WITH query can refer to itself and earlier WITH
//! queries"). Previously Nucleus's non-recursive CTE resolution executed each
//! CTE body without exposing the in-progress sibling CTEs, so a later CTE
//! referencing an earlier one failed with TableNotFound. SQLite (the
//! differential oracle) and Postgres both accept these queries.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::*;
use nucleus::types::Value;
use std::sync::Arc;

fn ex(st: Arc<dyn StorageEngine>) -> Executor {
    Executor::new(Arc::new(Catalog::new()), st)
}

async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        other => panic!("unexpected value: {other:?}"),
    }
}

/// The exact failing shape from the differential fuzzer: a second CTE
/// aggregating over the first must succeed and return one aggregate row.
#[tokio::test]
async fn chained_cte_second_references_first() {
    let e = ex(Arc::new(MvccStorageAdapter::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER)")
        .await
        .unwrap();
    e.execute(
        "INSERT INTO t (id, c1) VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80)",
    )
    .await
    .unwrap();

    let r = rows(
        &e,
        "WITH first AS (SELECT id, c1 FROM t WHERE id <= 7), \
              totals AS (SELECT SUM(c1) AS s, COUNT(*) AS n FROM first) \
         SELECT s, n FROM totals",
    )
    .await;

    assert_eq!(r.len(), 1, "aggregate over earlier CTE must yield one row");
    // ids 1..=7 -> c1 = 10+20+30+40+50+60+70 = 280, count = 7.
    assert_eq!(as_i64(&r[0][0]), 280, "SUM over earlier CTE");
    assert_eq!(as_i64(&r[0][1]), 7, "COUNT over earlier CTE");
}

/// Three-deep chain: each CTE references the immediately preceding one.
#[tokio::test]
async fn chained_cte_three_deep() {
    let e = ex(Arc::new(MvccStorageAdapter::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER)")
        .await
        .unwrap();
    e.execute("INSERT INTO t (id, c1) VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        .await
        .unwrap();

    let r = rows(
        &e,
        "WITH a AS (SELECT id, c1 FROM t WHERE id <= 4), \
              b AS (SELECT id, c1 * 2 AS d FROM a), \
              c AS (SELECT SUM(d) AS s FROM b) \
         SELECT s FROM c",
    )
    .await;

    assert_eq!(r.len(), 1);
    // ids 1..=4: c1 = 1+2+3+4 = 10, doubled = 20.
    assert_eq!(as_i64(&r[0][0]), 20);
}

/// A later CTE joining an earlier CTE against a base table.
#[tokio::test]
async fn chained_cte_join_earlier() {
    let e = ex(Arc::new(MvccStorageAdapter::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER)")
        .await
        .unwrap();
    e.execute("INSERT INTO t (id, c1) VALUES (1,10),(2,20),(3,30)")
        .await
        .unwrap();

    let r = rows(
        &e,
        "WITH lo AS (SELECT id, c1 FROM t WHERE id <= 2), \
              j AS (SELECT lo.id AS lid, t.c1 AS tc1 FROM lo JOIN t ON lo.id = t.id) \
         SELECT lid, tc1 FROM j ORDER BY lid",
    )
    .await;

    assert_eq!(r.len(), 2);
    assert_eq!(as_i64(&r[0][0]), 1);
    assert_eq!(as_i64(&r[0][1]), 10);
    assert_eq!(as_i64(&r[1][0]), 2);
    assert_eq!(as_i64(&r[1][1]), 20);
}
