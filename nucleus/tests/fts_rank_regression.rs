//! Regression for finding #6: FTS_RANK must use BM25-shaped term-frequency
//! saturation (Okapi BM25, k1=1.2), not raw tf/length which could rank documents
//! INVERSELY to FTS_SEARCH's BM25. FTS_RANK is a corpus-free scalar so it can't
//! replicate IDF / cross-document length normalization, but its score must be
//! monotonic and saturating in term frequency, matching BM25's tf component.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

async fn score(ex: &Executor, doc: &str, query: &str) -> f64 {
    let sql = format!("SELECT FTS_RANK('{doc}', '{query}')");
    let mut r = ex.execute_with_session(0, &sql).await.unwrap();
    match r.pop().unwrap() {
        ExecResult::Select { rows, .. } => match rows[0][0] {
            Value::Float64(f) => f,
            _ => panic!("expected float"),
        },
        o => panic!("{o:?}"),
    }
}

#[tokio::test]
async fn fts_rank_is_bm25_saturating_in_tf() {
    let ex = Executor::new(Arc::new(Catalog::new()), Arc::new(MvccStorageAdapter::new()));

    // More occurrences of the query term → strictly higher score (monotonic).
    let s1 = score(&ex, "alpha beta gamma", "alpha").await;
    let s2 = score(&ex, "alpha alpha beta gamma", "alpha").await;
    let s3 = score(&ex, "alpha alpha alpha beta gamma", "alpha").await;
    assert!(s1 > 0.0 && s2 > s1 && s3 > s2, "FTS_RANK must increase with tf: {s1} {s2} {s3}");

    // Saturation: BM25's tf*(k1+1)/(tf+k1) is concave — each extra occurrence
    // adds less than the previous one (unlike linear tf/len).
    assert!(s3 - s2 < s2 - s1, "FTS_RANK must saturate (concave in tf): {s1} {s2} {s3}");

    // A term absent from the doc contributes nothing.
    assert_eq!(score(&ex, "alpha beta", "zzz").await, 0.0, "absent term scores 0");

    // No inversion: a doc with more occurrences must not score lower than one
    // with fewer, regardless of length (the raw tf/len bug could invert this).
    let many_long = score(&ex, "alpha alpha alpha alpha x y z w q r s t u v", "alpha").await;
    let few_short = score(&ex, "alpha beta", "alpha").await;
    assert!(many_long > few_short, "more occurrences must rank >= fewer: {many_long} vs {few_short}");
}
