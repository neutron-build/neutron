//! Regression: a reversed/contradictory range bound on an indexed column
//! (e.g. `id >= 20 AND id <= -5`, or `id > 6 AND id < 5`) must return an empty
//! result, not panic. The PK two-sided-range index path fed such bounds straight
//! into `BTreeMap::range`, which panics when start > end. Found by the SQL
//! differential fuzzer; fixed by guarding the range lookups in mvcc.rs.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use std::sync::Arc;

async fn ids(ex: &Executor, sql: &str) -> Vec<i64> {
    match ex.execute(sql).await.unwrap().pop().unwrap() {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|r| match r[0] {
                nucleus::types::Value::Int32(n) => n as i64,
                nucleus::types::Value::Int64(n) => n,
                ref v => panic!("{v:?}"),
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

#[tokio::test]
async fn reversed_pk_range_returns_empty_not_panic() {
    let ex = Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()) as Arc<dyn StorageEngine>,
    );
    ex.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)")
        .await
        .unwrap();
    ex.execute("INSERT INTO t VALUES (1,4),(2,3),(7,18),(12,0),(20,5)")
        .await
        .unwrap();
    assert_eq!(
        ids(&ex, "SELECT id FROM t WHERE id > 6 AND id < 5").await,
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(&ex, "SELECT id FROM t WHERE id >= 20 AND id <= -5").await,
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(
            &ex,
            "SELECT id FROM t WHERE (id >= 11 AND id >= 20) AND id <= -5"
        )
        .await,
        Vec::<i64>::new()
    );
    // a valid range still works
    assert_eq!(
        ids(
            &ex,
            "SELECT id FROM t WHERE id >= 2 AND id <= 7 ORDER BY id"
        )
        .await,
        vec![2, 7]
    );
}
