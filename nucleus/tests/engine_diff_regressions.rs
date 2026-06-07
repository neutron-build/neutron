//! Findings from the engine-vs-engine differential fuzzer (`src/bin/probe_engines.rs`).
//! Mvcc (the default engine) and Memory agree perfectly; LSM and Columnar each
//! have a confirmed correctness bug. The buggy cases are `#[ignore]`d and assert
//! the CORRECT (Mvcc) behavior — remove `#[ignore]` once the engine is fixed.
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
        _ => vec![],
    }
}

/// Baseline: the default (Mvcc) and Memory engines return integer GROUP BY keys
/// as integers in numeric order.
#[tokio::test]
async fn groupby_int_key_baseline_mvcc_memory() {
    for st in [
        Arc::new(MvccStorageAdapter::new()) as Arc<dyn StorageEngine>,
        Arc::new(MemoryEngine::new()) as Arc<dyn StorageEngine>,
    ] {
        let e = ex(st);
        e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)").await.unwrap();
        e.execute("INSERT INTO t VALUES (1,2),(2,11),(3,2),(4,1)").await.unwrap();
        let r = rows(&e, "SELECT c1 FROM t GROUP BY c1 ORDER BY c1 ASC").await;
        let keys: Vec<i64> = r.iter().map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            ref v => panic!("non-int group key: {v:?}"),
        }).collect();
        assert_eq!(keys, vec![1, 2, 11]);
    }
}

/// BUG (columnar): the columnar fast group-by returns an INTEGER group key as
/// Text and orders it lexicographically (e.g. 1, 11, 2 instead of 1, 2, 11).
#[tokio::test]
async fn columnar_groupby_int_key_type() {
    let e = ex(Arc::new(ColumnarStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)").await.unwrap();
    e.execute("INSERT INTO t VALUES (1,2),(2,11),(3,2),(4,1)").await.unwrap();
    let r = rows(&e, "SELECT c1 FROM t GROUP BY c1 ORDER BY c1 ASC").await;
    let keys: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Int32(n) => n as i64,
        Value::Int64(n) => n,
        ref v => panic!("non-int group key: {v:?}"),
    }).collect();
    assert_eq!(keys, vec![1, 2, 11]);
}

/// Baseline: Mvcc/Memory return all rows in a primary-key range.
#[tokio::test]
async fn pk_range_baseline_mvcc_memory() {
    for st in [
        Arc::new(MvccStorageAdapter::new()) as Arc<dyn StorageEngine>,
        Arc::new(MemoryEngine::new()) as Arc<dyn StorageEngine>,
    ] {
        let e = ex(st);
        e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)").await.unwrap();
        e.execute("INSERT INTO t VALUES (1,5),(2,6),(3,7),(4,8),(5,9)").await.unwrap();
        let r = rows(&e, "SELECT id FROM t WHERE id BETWEEN 2 AND 4 ORDER BY id").await;
        let ids: Vec<i64> = r.iter().map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            ref v => panic!("{v:?}"),
        }).collect();
        assert_eq!(ids, vec![2, 3, 4]);
    }
}

/// BUG (LSM): a primary-key range filter (`id BETWEEN ...`) returns NO rows,
/// even on freshly-inserted data — silent data loss. A non-PK BETWEEN works,
/// so the defect is in the LSM primary-key range path.
#[tokio::test]
async fn lsm_pk_range_returns_rows() {
    let e = ex(Arc::new(LsmStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)").await.unwrap();
    e.execute("INSERT INTO t VALUES (1,5),(2,6),(3,7),(4,8),(5,9)").await.unwrap();
    let r = rows(&e, "SELECT id FROM t WHERE id BETWEEN 2 AND 4 ORDER BY id").await;
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Int32(n) => n as i64,
        Value::Int64(n) => n,
        ref v => panic!("{v:?}"),
    }).collect();
    assert_eq!(ids, vec![2, 3, 4]);
}

/// BUG (columnar, residual — found while fixing the GROUP BY key-type bug): the
/// columnar AVG/SUM group-by path drops a group whose aggregated column is
/// entirely NULL, instead of emitting it with a NULL aggregate (which Mvcc does).
/// `probe_engines --engine columnar` still shows ~19 divergences from this.
#[tokio::test]
async fn columnar_all_null_group_kept() {
    let e = ex(Arc::new(ColumnarStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL, c2 INTEGER)").await.unwrap();
    // group c1=5 has only NULL c2; group c1=6 has a value.
    e.execute("INSERT INTO t VALUES (1,5,NULL),(2,5,NULL),(3,6,10)").await.unwrap();
    let r = rows(&e, "SELECT c1, AVG(c2) FROM t GROUP BY c1 ORDER BY c1 ASC").await;
    // Mvcc returns both groups: (5, NULL) and (6, 10). Columnar drops the first.
    assert_eq!(r.len(), 2, "expected 2 groups (incl. the all-NULL one), got {r:?}");
}
