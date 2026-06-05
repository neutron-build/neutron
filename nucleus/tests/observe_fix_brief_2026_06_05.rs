#![cfg(feature = "server")]
//! Regression tests for NUCLEUS_FIX_BRIEF_FROM_OBSERVE_2026-06-05.md.
//!
//! Three issues surfaced by teploy-observe dogfooding Nucleus over pgwire:
//!   BUG 1 — `concat_columns` panicked on a mixed-variant column (one bad writer
//!           could take down reads for a whole table).
//!   BUG 2 — a string-bound value (e.g. `'5'`) inserted into a declared numeric
//!           column was stored as `Text`, not coerced to the column's type — the
//!           state that made columns go mixed-variant (BUG 1's trigger).
//!   BUG 3 — the ClickHouse `ENGINE=ReplacingMergeTree(v)` DDL parsed but never
//!           registered the version column / read-time dedup, so `COUNT(*)`/`SUM`
//!           counted superseded versions. (The brief read this as "aggregate
//!           dedup doesn't happen"; the root cause was the ignored engine clause.)

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{ColumnarStorageEngine, StorageEngine};
use nucleus::types::Value;

async fn fresh() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}
async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().expect("a result")
}
async fn one(ex: &Executor, sql: &str) -> Value {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows[0][0].clone(),
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}
fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("expected an integer value, got {other:?}"),
    }
}

// BUG 2: string-bound insert into a BIGINT column is coerced to Int64 at write
// time, so the column never goes mixed-variant and ordering stays numeric.
#[tokio::test]
async fn string_insert_into_bigint_is_coerced() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE c (id BIGINT, v BIGINT) \
         WITH (engine='replacing_mergetree', version_column='v') ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO c (id,v) VALUES (1,'2'),(2,'10')").await;
    // Stored as Int64, so MAX compares numerically (10 > 2), not lexically ("2" > "10").
    let mx = one(&ex, "SELECT MAX(v) FROM c").await;
    assert!(
        matches!(mx, Value::Int64(10)),
        "MAX(v) should be Int64(10), got {mx:?}"
    );
}

// BUG 1: a mixed-variant column must never panic a read. With write-time
// coercion (BUG 2) the column no longer goes mixed, so the brief's exact repro
// now returns cleanly; the low-level unify path is unit-tested in
// `columnar::tests::concat_columns_unifies_mixed_types_without_panic`.
#[tokio::test]
async fn mixed_bound_inserts_do_not_panic() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE m (id BIGINT, v BIGINT NOT NULL DEFAULT 0) \
         WITH (engine='replacing_mergetree', version_column='v') ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO m (id, v) VALUES (1, '5')").await; // text-bound
    exec(&ex, "INSERT INTO m (id, v) VALUES (1, 7)").await; // int-bound
    let r = one(&ex, "SELECT argMax(id, v) FROM m").await; // must not panic
    assert_eq!(as_i64(&r), 1);
}

// BUG 3: the ClickHouse `ENGINE=ReplacingMergeTree(v) ORDER BY id` DDL registers
// the version column and read-time dedup, so aggregates collapse superseded rows.
#[tokio::test]
async fn clickhouse_engine_syntax_registers_dedup() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE r (id BIGINT, n BIGINT, v BIGINT DEFAULT 0) \
         ENGINE=ReplacingMergeTree(v) ORDER BY id",
    )
    .await;
    exec(&ex, "INSERT INTO r (id,n,v) VALUES (1,100,1)").await;
    exec(&ex, "INSERT INTO r (id,n,v) VALUES (1,999,2)").await;
    // Newest-wins dedup: one logical row, n from the highest version.
    assert_eq!(
        as_i64(&one(&ex, "SELECT COUNT(*) FROM r").await),
        1,
        "COUNT(*) should dedup"
    );
    assert_eq!(
        as_i64(&one(&ex, "SELECT SUM(n) FROM r").await),
        999,
        "SUM(n) should see only newest"
    );
}
