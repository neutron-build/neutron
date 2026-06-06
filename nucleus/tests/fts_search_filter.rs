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
async fn txt(ex: &Executor, sql: &str) -> String {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Text(s) => s.clone(),
            v => format!("{v:?}"),
        },
        o => panic!("{o:?}"),
    }
}
#[tokio::test]

async fn fts_filter() {
    let ex = fresh().await;
    // site_a docs: 1,2 ; site_b docs: 3,4 — all about "database error timeout"
    ex.execute(
        "SELECT FTS_INDEX_FACETED(1, 'database connection timeout error', 'site_id', 'site_a')",
    )
    .await
    .unwrap();
    ex.execute("SELECT FTS_INDEX_FACETED(2, 'timeout error on query', 'site_id', 'site_a')")
        .await
        .unwrap();
    ex.execute("SELECT FTS_INDEX_FACETED(3, 'database error timeout fatal', 'site_id', 'site_b')")
        .await
        .unwrap();
    ex.execute("SELECT FTS_INDEX_FACETED(4, 'connection error timeout', 'site_id', 'site_b')")
        .await
        .unwrap();
    let all = txt(&ex, "SELECT FTS_SEARCH('timeout error', 10)").await;
    let site_a = txt(
        &ex,
        "SELECT FTS_SEARCH_FILTER('timeout error', 10, 'site_id', 'site_a')",
    )
    .await;
    let site_b = txt(
        &ex,
        "SELECT FTS_SEARCH_FILTER('timeout error', 10, 'site_id', 'site_b')",
    )
    .await;
    eprintln!("ALL: {all}");
    eprintln!("site_a: {site_a}");
    eprintln!("site_b: {site_b}");
    // site_a must contain doc 1 and 2 only; site_b doc 3,4 only.
    assert!(site_a.contains("\"doc_id\":1") && site_a.contains("\"doc_id\":2"));
    assert!(
        !site_a.contains("\"doc_id\":3") && !site_a.contains("\"doc_id\":4"),
        "site_a must not include site_b docs"
    );
    assert!(site_b.contains("\"doc_id\":3") && site_b.contains("\"doc_id\":4"));
    assert!(!site_b.contains("\"doc_id\":1") && !site_b.contains("\"doc_id\":2"));
}
