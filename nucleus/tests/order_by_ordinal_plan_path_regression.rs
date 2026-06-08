//! Regression: the plan path's Sort resolves keys by output-column NAME only, so
//! `ORDER BY <ordinal>` and `ORDER BY <expr>` resolved to nothing and silently did
//! NOT sort (returned insertion order). `query_eligible_for_plan` now routes any
//! ORDER BY that isn't a bare column identifier to the AST execution path, which
//! evaluates positions and expressions correctly.
//!
//! Surfaced while fixing the plan-cache literal-reuse bug (full-scale fuzzer).
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;

async fn rows(ex: &Executor, s: u64, sql: &str) -> Vec<(i32, i32)> {
    let r = ex.execute_with_session(s, sql).await.unwrap();
    match r.into_iter().next_back().unwrap() {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|row| {
                let g = |v: &nucleus::types::Value| match v {
                    nucleus::types::Value::Int32(n) => *n,
                    nucleus::types::Value::Int64(n) => *n as i32,
                    o => panic!("non-int {o:?}"),
                };
                (g(&row[0]), g(&row[1]))
            })
            .collect(),
        o => panic!("expected select, got {o:?}"),
    }
}

#[tokio::test]
async fn order_by_ordinal_and_expr_sort_in_plan_path() {
    let ex = Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new()));
    let s = ex.create_session();
    ex.execute_with_session(s, "CREATE TABLE t (a INTEGER, b INTEGER)").await.unwrap();
    // Insertion order is deliberately none of the sorted orders.
    ex.execute_with_session(s, "INSERT INTO t (a,b) VALUES (3,1),(1,2),(2,0)").await.unwrap();

    // ORDER BY 1 ≡ ORDER BY a.
    assert_eq!(
        rows(&ex, s, "SELECT a, b FROM t ORDER BY 1").await,
        vec![(1, 2), (2, 0), (3, 1)],
        "ORDER BY 1 did not sort by the first column",
    );
    // ORDER BY 2 ≡ ORDER BY b.
    assert_eq!(
        rows(&ex, s, "SELECT a, b FROM t ORDER BY 2").await,
        vec![(2, 0), (3, 1), (1, 2)],
        "ORDER BY 2 did not sort by the second column",
    );
    // Descending ordinal.
    assert_eq!(
        rows(&ex, s, "SELECT a, b FROM t ORDER BY 1 DESC").await,
        vec![(3, 1), (2, 0), (1, 2)],
        "ORDER BY 1 DESC did not sort descending",
    );
    // Expression: a+b = 4, 3, 2 → ascending (2,0),(1,2),(3,1).
    assert_eq!(
        rows(&ex, s, "SELECT a, b FROM t ORDER BY a + b").await,
        vec![(2, 0), (1, 2), (3, 1)],
        "ORDER BY a+b did not sort by the expression",
    );
    // Sanity: bare column still works (stayed on the plan fast path).
    assert_eq!(
        rows(&ex, s, "SELECT a, b FROM t ORDER BY a").await,
        vec![(1, 2), (2, 0), (3, 1)],
    );
}
