#![cfg(feature = "server")]
//! KV_PFMERGE: cross-bucket HyperLogLog union for correct unique counts.
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
fn iv(v: &Value) -> i64 {
    match v {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        _ => panic!("{v:?}"),
    }
}
async fn one(ex: &Executor, sql: &str) -> Value {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows[0][0].clone(),
        o => panic!("{o:?}"),
    }
}
#[tokio::test]
async fn kv_pfmerge_union_is_not_oversum() {
    let ex = fresh().await;
    for k in 1..=600 {
        ex.execute(&format!("SELECT KV_PFADD('a','v{k}')"))
            .await
            .unwrap();
    }
    for k in 400..=1000 {
        ex.execute(&format!("SELECT KV_PFADD('b','v{k}')"))
            .await
            .unwrap();
    }
    let ca = iv(&one(&ex, "SELECT KV_PFCOUNT('a')").await);
    let cb = iv(&one(&ex, "SELECT KV_PFCOUNT('b')").await);
    assert!(matches!(
        one(&ex, "SELECT KV_PFMERGE('m','a','b')").await,
        Value::Bool(true)
    ));
    let cm = iv(&one(&ex, "SELECT KV_PFCOUNT('m')").await);
    // True unique across both buckets is 1000 (1..1000).
    assert!((cm - 1000).abs() < 40, "merged ~1000, got {cm}");
    assert!(
        cm < ca + cb - 100,
        "merged ({cm}) must be below the over-counting sum ({})",
        ca + cb
    );
}
