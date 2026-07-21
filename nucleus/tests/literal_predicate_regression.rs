//! Regression: a literal-only predicate (`WHERE 1=1`, `WHERE 2>1`) must not be
//! bound to an index. The planner's per-predicate index-selection loop skipped
//! its column-match check when no column could be extracted, planning
//! `WHERE 1=1` as a PK point lookup keyed on the literal — zero rows,
//! silently. Found by Prisma's `count()` (it emits `WHERE 1=1`).
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

#[tokio::test]
async fn literal_only_predicates_never_use_an_index() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let exec = Arc::new(Executor::new(catalog, storage));
    exec.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    exec.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .await
        .unwrap();
    for (sql, want) in [
        ("SELECT id FROM t WHERE 1=1", 2usize),
        ("SELECT id FROM t WHERE 2>1", 2),
        ("SELECT id FROM t WHERE 1=2", 0),
        ("SELECT id FROM t WHERE 1=1 AND id = 2", 1),
    ] {
        let r = exec.execute(sql).await.unwrap();
        let ExecResult::Select { rows, .. } = r.into_iter().next().unwrap() else {
            panic!("not a select: {sql}");
        };
        assert_eq!(rows.len(), want, "{sql}");
    }
    let r = exec
        .execute("SELECT COUNT(*) FROM t WHERE 1=1")
        .await
        .unwrap();
    let ExecResult::Select { rows, .. } = r.into_iter().next().unwrap() else {
        panic!("count not a select");
    };
    assert_eq!(rows[0][0], Value::Int64(2), "COUNT(*) WHERE 1=1");
}
