#![cfg(feature = "server")]
//! Regression: ORDER BY a column not in the SELECT list must still sort by it.
//! The plan path projected before sorting, silently dropping the sort key — so
//! `SELECT country FROM ev ORDER BY ts DESC` returned physical order, and a
//! correlated `(SELECT col ... ORDER BY ts DESC LIMIT 1)` returned the wrong row.
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
async fn col0(ex: &Executor, sql: &str) -> Vec<String> {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|r| match &r[0] {
                Value::Text(s) => s.clone(),
                v => format!("{v:?}"),
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}
#[tokio::test]
async fn order_by_non_projected_column() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE ev (person TEXT, country TEXT, ts BIGINT)")
        .await
        .unwrap();
    ex.execute("INSERT INTO ev VALUES ('p1','US',1),('p1','CA',3),('p1','UK',2)")
        .await
        .unwrap();
    // ORDER BY ts DESC, ts NOT projected → must be CA(3),UK(2),US(1)
    assert_eq!(
        col0(&ex, "SELECT country FROM ev ORDER BY ts DESC").await,
        vec!["CA", "UK", "US"]
    );
    assert_eq!(
        col0(&ex, "SELECT country FROM ev ORDER BY ts ASC").await,
        vec!["US", "UK", "CA"]
    );
    // LIMIT 1 on the non-projected sort
    assert_eq!(
        col0(&ex, "SELECT country FROM ev ORDER BY ts DESC LIMIT 1").await,
        vec!["CA"]
    );
    // Correlated scalar subquery (most-recent-country-per-person)
    assert_eq!(col0(&ex, "SELECT (SELECT country FROM ev e2 WHERE e2.person = ev.person ORDER BY ts DESC LIMIT 1) FROM ev WHERE person='p1' LIMIT 1").await, vec!["CA"]);
    // Still works when ts IS projected
    assert_eq!(
        col0(&ex, "SELECT country, ts FROM ev ORDER BY ts DESC LIMIT 1").await,
        vec!["CA"]
    );
}
