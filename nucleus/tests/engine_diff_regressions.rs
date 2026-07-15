//! Findings from the engine-vs-engine differential fuzzer (`src/bin/probe_engines.rs`).
//! Each historical engine mismatch below is now fixed. The tests remain active
//! so MVCC, Memory, LSM, and Columnar cannot silently diverge again.
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
        e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)")
            .await
            .unwrap();
        e.execute("INSERT INTO t VALUES (1,2),(2,11),(3,2),(4,1)")
            .await
            .unwrap();
        let r = rows(&e, "SELECT c1 FROM t GROUP BY c1 ORDER BY c1 ASC").await;
        let keys: Vec<i64> = r
            .iter()
            .map(|row| match row[0] {
                Value::Int32(n) => n as i64,
                Value::Int64(n) => n,
                ref v => panic!("non-int group key: {v:?}"),
            })
            .collect();
        assert_eq!(keys, vec![1, 2, 11]);
    }
}

/// BUG (columnar): the columnar fast group-by returns an INTEGER group key as
/// Text and orders it lexicographically (e.g. 1, 11, 2 instead of 1, 2, 11).
#[tokio::test]
async fn columnar_groupby_int_key_type() {
    let e = ex(Arc::new(ColumnarStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)")
        .await
        .unwrap();
    e.execute("INSERT INTO t VALUES (1,2),(2,11),(3,2),(4,1)")
        .await
        .unwrap();
    let r = rows(&e, "SELECT c1 FROM t GROUP BY c1 ORDER BY c1 ASC").await;
    let keys: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            ref v => panic!("non-int group key: {v:?}"),
        })
        .collect();
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
        e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)")
            .await
            .unwrap();
        e.execute("INSERT INTO t VALUES (1,5),(2,6),(3,7),(4,8),(5,9)")
            .await
            .unwrap();
        let r = rows(&e, "SELECT id FROM t WHERE id BETWEEN 2 AND 4 ORDER BY id").await;
        let ids: Vec<i64> = r
            .iter()
            .map(|row| match row[0] {
                Value::Int32(n) => n as i64,
                Value::Int64(n) => n,
                ref v => panic!("{v:?}"),
            })
            .collect();
        assert_eq!(ids, vec![2, 3, 4]);
    }
}

/// BUG (LSM): a primary-key range filter (`id BETWEEN ...`) returns NO rows,
/// even on freshly-inserted data — silent data loss. A non-PK BETWEEN works,
/// so the defect is in the LSM primary-key range path.
#[tokio::test]
async fn lsm_pk_range_returns_rows() {
    let e = ex(Arc::new(LsmStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL)")
        .await
        .unwrap();
    e.execute("INSERT INTO t VALUES (1,5),(2,6),(3,7),(4,8),(5,9)")
        .await
        .unwrap();
    let r = rows(&e, "SELECT id FROM t WHERE id BETWEEN 2 AND 4 ORDER BY id").await;
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Int32(n) => n as i64,
            Value::Int64(n) => n,
            ref v => panic!("{v:?}"),
        })
        .collect();
    assert_eq!(ids, vec![2, 3, 4]);
}

/// BUG (columnar, residual — found while fixing the GROUP BY key-type bug): the
/// columnar AVG/SUM group-by path drops a group whose aggregated column is
/// entirely NULL, instead of emitting it with a NULL aggregate (which Mvcc does).
/// `probe_engines --engine columnar` still shows ~19 divergences from this.
#[tokio::test]
async fn columnar_all_null_group_kept() {
    let e = ex(Arc::new(ColumnarStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL, c2 INTEGER)")
        .await
        .unwrap();
    // group c1=5 has only NULL c2; group c1=6 has a value.
    e.execute("INSERT INTO t VALUES (1,5,NULL),(2,5,NULL),(3,6,10)")
        .await
        .unwrap();
    let r = rows(&e, "SELECT c1, AVG(c2) FROM t GROUP BY c1 ORDER BY c1 ASC").await;
    // Mvcc returns both groups: (5, NULL) and (6, 10). Columnar drops the first.
    assert_eq!(
        r.len(),
        2,
        "expected 2 groups (incl. the all-NULL one), got {r:?}"
    );
}

/// The executor must coerce INSERT values to the declared schema before the
/// columnar engine encodes them, and reject invalid primitive values instead of
/// leaving a mixed-type column that fails later during scans or aggregation.
#[tokio::test]
async fn columnar_insert_schema_coercion_is_strict() {
    let e = ex(Arc::new(ColumnarStorageEngine::new()));
    e.execute(
        "CREATE TABLE typed (id INTEGER, wide BIGINT, score DOUBLE, active BOOLEAN, note TEXT)",
    )
    .await
    .unwrap();
    e.execute("INSERT INTO typed VALUES ('7', '9000000000', '3.5', 'true', 42)")
        .await
        .unwrap();

    assert_eq!(
        rows(&e, "SELECT id, wide, score, active, note FROM typed").await,
        vec![vec![
            Value::Int32(7),
            Value::Int64(9_000_000_000),
            Value::Float64(3.5),
            Value::Bool(true),
            Value::Text("42".into()),
        ]]
    );

    assert!(
        e.execute("INSERT INTO typed VALUES ('not-an-int', 1, 1, true, 'bad')")
            .await
            .is_err()
    );
    assert_eq!(
        rows(&e, "SELECT COUNT(*) FROM typed").await[0][0],
        Value::Int64(1)
    );
}

/// An UPDATE literal must be coerced just like an INSERT literal. Previously,
/// assigning an Int32 literal to a BIGINT column made the rebuilt columnar batch
/// choose Int32 storage; every untouched Int64 in that column decoded as NULL.
#[tokio::test]
async fn columnar_update_preserves_declared_type_and_neighbor_values() {
    let e = ex(Arc::new(ColumnarStorageEngine::new()));
    e.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, big BIGINT NOT NULL, optional BIGINT)")
        .await
        .unwrap();
    e.execute("INSERT INTO t VALUES (1, 10, 1), (2, 20, NULL), (3, 30, 3)")
        .await
        .unwrap();
    e.execute("UPDATE t SET big = -3 WHERE id <> 3")
        .await
        .unwrap();

    assert_eq!(
        rows(&e, "SELECT id, big, optional FROM t ORDER BY id").await,
        vec![
            vec![Value::Int32(1), Value::Int64(-3), Value::Int64(1)],
            vec![Value::Int32(2), Value::Int64(-3), Value::Null],
            vec![Value::Int32(3), Value::Int64(30), Value::Int64(3)],
        ]
    );
}

/// Exact NUMERIC semantics must not depend on the physical table engine. In
/// particular, columnar f64 aggregate shortcuts must never consume decimals.
#[tokio::test]
async fn numeric_is_exact_and_checked_across_engines() {
    let engines: Vec<(&str, Arc<dyn StorageEngine>)> = vec![
        ("mvcc", Arc::new(MvccStorageAdapter::new())),
        ("memory", Arc::new(MemoryEngine::new())),
        ("lsm", Arc::new(LsmStorageEngine::new())),
        ("columnar", Arc::new(ColumnarStorageEngine::new())),
    ];
    for (name, storage) in engines {
        let e = ex(storage);
        e.execute("CREATE TABLE exact (id INT, bucket TEXT, amount NUMERIC)")
            .await
            .unwrap();
        e.execute("INSERT INTO exact VALUES (1, 'a', '0.1'), (2, 'a', '0.2'), (3, 'b', NULL)")
            .await
            .unwrap();

        assert_eq!(
            rows(&e, "SELECT SUM(amount), AVG(amount) FROM exact").await,
            vec![vec![
                Value::Numeric("0.3".into()),
                Value::Numeric("0.15".into()),
            ]],
            "{name} plain aggregates"
        );
        assert_eq!(
            rows(
                &e,
                "SELECT bucket, SUM(amount) FROM exact GROUP BY bucket ORDER BY bucket",
            )
            .await,
            vec![
                vec![Value::Text("a".into()), Value::Numeric("0.3".into())],
                vec![Value::Text("b".into()), Value::Null],
            ],
            "{name} grouped aggregates"
        );
        assert!(
            e.execute("INSERT INTO exact VALUES (4, 'bad', 'not-a-number')")
                .await
                .is_err(),
            "{name} must reject malformed NUMERIC writes"
        );
    }
}
