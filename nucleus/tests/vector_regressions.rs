//! Sanity pins for the vector model, validated by the vector differential fuzzer
//! (`src/bin/probe_vector.rs`, 480K scalar checks + KNN, 0 divergences).
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
async fn f(ex: &Executor, sql: &str) -> f64 {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => match rows[0][0] {
            Value::Float64(x) => x,
            Value::Int32(n) => n as f64,
            Value::Int64(n) => n as f64,
            ref v => panic!("{v:?}"),
        },
        _ => panic!("non-select"),
    }
}
fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-4 + 1e-4 * a.abs().max(b.abs())
}

#[tokio::test]
async fn distance_metrics() {
    let ex = fresh().await;
    // a=[3,4,0], b=[0,0,0]: l2 = 5; a·b = 0
    assert!(close(
        f(
            &ex,
            "SELECT VECTOR_DISTANCE(VECTOR('[3,4,0]'), VECTOR('[3,4,0]'))"
        )
        .await,
        0.0
    ));
    assert!(close(
        f(
            &ex,
            "SELECT VECTOR_DISTANCE(VECTOR('[3,4,0]'), VECTOR('[0,0,0]'), 'l2')"
        )
        .await,
        5.0
    ));
    assert!(close(
        f(&ex, "SELECT VECTOR_L2_DISTANCE('[1,2,2]','[0,0,0]')").await,
        3.0
    ));
    // inner product (positive) of [1,2,3]·[4,5,6] = 32
    assert!(close(
        f(&ex, "SELECT VECTOR_INNER_PRODUCT('[1,2,3]','[4,5,6]')").await,
        32.0
    ));
    // cosine of identical unit-ish vectors -> 0
    assert!(close(
        f(&ex, "SELECT VECTOR_COSINE_DISTANCE('[1,0,0]','[1,0,0]')").await,
        0.0
    ));
    // cosine of orthogonal -> 1
    assert!(close(
        f(&ex, "SELECT VECTOR_COSINE_DISTANCE('[1,0,0]','[0,1,0]')").await,
        1.0
    ));
    assert!(close(
        f(&ex, "SELECT VECTOR_DIMS(VECTOR('[1,2,3,4]'))").await,
        4.0
    ));
}

#[tokio::test]
async fn knn_ordering() {
    let ex = fresh().await;
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3))")
        .await
        .unwrap();
    ex.execute("INSERT INTO t VALUES (1,VECTOR('[1,0,0]')),(2,VECTOR('[0,1,0]')),(3,VECTOR('[0.9,0.1,0]')),(4,VECTOR('[0,0,5]'))").await.unwrap();
    let r = match ex
        .execute(
            "SELECT id FROM t ORDER BY VECTOR_DISTANCE(v, VECTOR('[1,0,0]'), 'l2') ASC LIMIT 2",
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows,
        _ => panic!(),
    };
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            _ => -1,
        })
        .collect();
    assert_eq!(ids, vec![1, 3]); // closest two to [1,0,0]
}
