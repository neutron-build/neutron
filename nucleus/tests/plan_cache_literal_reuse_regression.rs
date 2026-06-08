//! Regression: the plan cache keys on SQL with literals stripped, but on a cache
//! hit only the WHERE clause is re-bound (`transplant_scan_exprs`). A re-bindable
//! literal anywhere else — projection, ORDER BY ordinal, etc. — used to keep the
//! *first* query's value when a structurally-identical later query reused the plan.
//!
//! Found by the full-scale differential fuzzer (seed 305419896, iter 4464):
//!   SELECT DISTINCT (c6 BETWEEN 6 AND 6) FROM t;   -- populates the plan cache
//!   SELECT DISTINCT (c6 BETWEEN 8 AND 13) FROM t;  -- reused 6 AND 13 → wrong
//!
//! These pin that the second query of each pair is evaluated with its OWN literals,
//! while the WHERE-clause point-lookup fast path still reuses correctly.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;

async fn rows(ex: &Executor, s: u64, sql: &str) -> Vec<Vec<String>> {
    let r = ex.execute_with_session(s, sql).await.unwrap();
    match r.into_iter().next_back().unwrap() {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|v| format!("{v:?}")).collect())
            .collect(),
        o => panic!("expected select, got {o:?}"),
    }
}

#[tokio::test]
async fn projection_between_literals_not_reused() {
    let ex = Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new()));
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE t (id INTEGER PRIMARY KEY, c6 INTEGER)").await.unwrap();
    ex.execute_with_session(s, "INSERT INTO t (id,c6) VALUES (1,11)").await.unwrap();

    // First populates the plan cache entry for `(c6 BETWEEN $N AND $N)`.
    let _ = rows(&ex, s, "SELECT DISTINCT (c6 BETWEEN 6 AND 6) FROM t").await;
    // Second must use ITS bounds: 11 BETWEEN 8 AND 13 = true.
    let second = rows(&ex, s, "SELECT DISTINCT (c6 BETWEEN 8 AND 13) FROM t").await;
    assert_eq!(second, vec![vec!["Bool(true)".to_string()]], "second query reused first's BETWEEN bounds");

    // Non-DISTINCT flavor too.
    let _ = rows(&ex, s, "SELECT (c6 BETWEEN 0 AND 1) FROM t").await;
    let nd = rows(&ex, s, "SELECT (c6 BETWEEN 8 AND 13) FROM t").await;
    assert_eq!(nd, vec![vec!["Bool(true)".to_string()]], "non-distinct projection reused stale bounds");
}

#[tokio::test]
async fn projection_string_literal_not_reused() {
    let ex = Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new()));
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT)").await.unwrap();
    ex.execute_with_session(s, "INSERT INTO t (id,c) VALUES (1,'blue')").await.unwrap();

    let _ = rows(&ex, s, "SELECT (c = 'red') FROM t").await;
    let second = rows(&ex, s, "SELECT (c = 'blue') FROM t").await;
    assert_eq!(second, vec![vec!["Bool(true)".to_string()]], "string-literal projection reused stale value");
}

#[tokio::test]
async fn where_point_lookup_reuse_still_correct() {
    // The safe, common case the plan cache exists for must still re-bind WHERE.
    let ex = Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new()));
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await.unwrap();
    ex.execute_with_session(s, "INSERT INTO t (id,v) VALUES (1,10),(2,20),(3,30)").await.unwrap();

    let r1 = rows(&ex, s, "SELECT v FROM t WHERE id = 1").await;
    let r2 = rows(&ex, s, "SELECT v FROM t WHERE id = 2").await;
    let r3 = rows(&ex, s, "SELECT v FROM t WHERE id = 3").await;
    assert_eq!(r1, vec![vec!["Int32(10)".to_string()]]);
    assert_eq!(r2, vec![vec!["Int32(20)".to_string()]], "WHERE point-lookup reuse returned wrong row");
    assert_eq!(r3, vec![vec!["Int32(30)".to_string()]]);
}
