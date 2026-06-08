//! Verification harness for the teploy-observe dogfood findings (2026-06).
//!
//! Each test asserts the behavior observe's data-integrity / analytics fixes
//! depend on. A FAILING test here is a still-open engine bug; a PASSING test is
//! the "supported as-is" verdict observe asked for. Grouped by the finding
//! numbers in `_internal/NUCLEUS_AUDIT_FROM_TEPLOY_OBSERVE_2026-06.md`.
//!
//! NOTE: every test uses a UNIQUE table name. The `replacing_mergetree` dedup
//! config lives in a process-global registry keyed by bare table name
//! (`columnar::REPLACING_REGISTRY`), shared across the `Executor`/engine
//! instances these parallel tests build; reusing a name would leak a replacing
//! config onto a later plain table. (A real server has one engine + unique
//! names + DROP-clears-registry, so this is a test-only concern.)

#![cfg(feature = "server")]

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
async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}
fn i64v(v: &Value) -> i64 {
    match v {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        Value::Float64(f) => *f as i64,
        Value::Text(s) => s.trim().parse().unwrap_or(-999),
        other => panic!("not numeric: {other:?}"),
    }
}

// ---- #1: ReplacingMergeTree BIGINT version dedup (observe's migration target) ----
#[tokio::test]
async fn f1_bigint_version_newest_wins() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f1_rt (k TEXT, v TEXT, ver BIGINT) WITH (engine='replacing_mergetree', version_column='ver') ORDER BY (k)").await;
    exec(&ex, "INSERT INTO f1_rt (k, v, ver) VALUES ('a', 'old', 1)").await;
    exec(&ex, "INSERT INTO f1_rt (k, v, ver) VALUES ('a', 'new', 2)").await;
    let r = rows(&ex, "SELECT v FROM f1_rt").await;
    assert_eq!(r.len(), 1, "dedup left {} rows", r.len());
    assert_eq!(r[0][0], Value::Text("new".into()), "newest version did not win");
}

// ---- #4: aggregates over a replacing table dedup before summing ----
#[tokio::test]
async fn f4_sum_dedups_before_aggregating() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f4_rt (k TEXT, amt BIGINT, ver BIGINT) WITH (engine='replacing_mergetree', version_column='ver') ORDER BY (k)").await;
    exec(&ex, "INSERT INTO f4_rt (k, amt, ver) VALUES ('a', 10, 1)").await;
    exec(&ex, "INSERT INTO f4_rt (k, amt, ver) VALUES ('a', 99, 2)").await; // supersedes
    let r = rows(&ex, "SELECT SUM(amt) FROM f4_rt").await;
    assert_eq!(i64v(&r[0][0]), 99, "SUM double-counted superseded version");
}

// ---- #5: argMax(value, ordering) ----
#[tokio::test]
async fn f5_argmax_supported() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f5_plain (k TEXT, amt BIGINT, ver BIGINT)").await;
    exec(&ex, "INSERT INTO f5_plain (k, amt, ver) VALUES ('a', 10, 1)").await;
    exec(&ex, "INSERT INTO f5_plain (k, amt, ver) VALUES ('a', 99, 3)").await;
    exec(&ex, "INSERT INTO f5_plain (k, amt, ver) VALUES ('a', 50, 2)").await;
    let r = rows(&ex, "SELECT argMax(amt, ver) FROM f5_plain GROUP BY k").await;
    assert_eq!(i64v(&r[0][0]), 99, "argMax did not return amt at max version");
}

// ---- #7: retention DELETE with numeric + text predicate coercion ----
#[tokio::test]
async fn f7_delete_range_int_literal() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f7a (id BIGINT PRIMARY KEY, ts BIGINT)").await;
    exec(&ex, "INSERT INTO f7a (id, ts) VALUES (1, 100), (2, 200), (3, 300)").await;
    exec(&ex, "DELETE FROM f7a WHERE ts < 250").await;
    assert_eq!(i64v(&rows(&ex, "SELECT COUNT(*) FROM f7a").await[0][0]), 1, "int-literal retention DELETE missed rows");
}
#[tokio::test]
async fn f7_delete_range_text_literal() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f7b (id BIGINT PRIMARY KEY, ts BIGINT)").await;
    exec(&ex, "INSERT INTO f7b (id, ts) VALUES (1, 100), (2, 200), (3, 300)").await;
    // pgx simple protocol: WHERE ts < $1 arrives as WHERE ts < '250'
    exec(&ex, "DELETE FROM f7b WHERE ts < '250'").await;
    assert_eq!(i64v(&rows(&ex, "SELECT COUNT(*) FROM f7b").await[0][0]), 1, "text-literal retention DELETE missed rows");
}

// ---- #8: result cache must not serve stale rows after DELETE ----
#[tokio::test]
async fn f8_cache_invalidated_after_delete() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f8 (id BIGINT PRIMARY KEY, val TEXT)").await;
    exec(&ex, "INSERT INTO f8 (id, val) VALUES (1, 'x'), (2, 'y')").await;
    let _ = rows(&ex, "SELECT COUNT(*) FROM f8").await; // prime any cache
    exec(&ex, "DELETE FROM f8 WHERE id = 1").await;
    assert_eq!(i64v(&rows(&ex, "SELECT COUNT(*) FROM f8").await[0][0]), 1, "stale cached COUNT after DELETE");
}

// ---- #10: open projection / CAST / HAVING bugs ----
#[tokio::test]
async fn f10_24_cast_aggregate_as_text() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f10_24 (id BIGINT)").await;
    exec(&ex, "INSERT INTO f10_24 (id) VALUES (1), (2), (3)").await;
    let r = rows(&ex, "SELECT CAST(COUNT(*) AS TEXT) FROM f10_24").await;
    assert_eq!(r.len(), 1, "CAST(aggregate AS TEXT) returned {} rows, want 1", r.len());
    assert_eq!(r[0][0], Value::Text("3".into()));
}
#[tokio::test]
async fn f10_26_coalesce_max() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f10_26 (id BIGINT)").await;
    exec(&ex, "INSERT INTO f10_26 (id) VALUES (5), (2)").await;
    let r = rows(&ex, "SELECT COALESCE(MAX(id), 0) FROM f10_26").await;
    assert_eq!(i64v(&r[0][0]), 5, "COALESCE(MAX(),0) wrong/rejected");
}
#[tokio::test]
async fn f10_28_having_count() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f10_28 (g TEXT)").await;
    exec(&ex, "INSERT INTO f10_28 (g) VALUES ('a'), ('a'), ('a'), ('b')").await;
    let r = rows(&ex, "SELECT g FROM f10_28 GROUP BY g HAVING COUNT(*) >= 2").await;
    assert_eq!(r.len(), 1, "HAVING COUNT(*)>=N returned {} rows, want 1", r.len());
    assert_eq!(r[0][0], Value::Text("a".into()));
}

// ---- #2: in-place ALTER COLUMN TYPE rewrites existing rows ----
#[tokio::test]
async fn f2_alter_column_type_rewrites_existing_data() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE f2 (id BIGINT, v TEXT)").await;
    exec(&ex, "INSERT INTO f2 (id, v) VALUES (1, '0'), (2, '42')").await;
    exec(&ex, "ALTER TABLE f2 ALTER COLUMN v TYPE BIGINT").await;
    // Existing TEXT values are rewritten to BIGINT (not just new writes).
    let r = rows(&ex, "SELECT v FROM f2 ORDER BY id").await;
    assert_eq!(i64v(&r[0][0]), 0);
    assert_eq!(i64v(&r[1][0]), 42);
    assert_eq!(i64v(&rows(&ex, "SELECT SUM(v) FROM f2").await[0][0]), 42, "numeric SUM after ALTER");
}

// ---- #6: AggregatingMergeTree accepted; percentiles via functional form ----
#[tokio::test]
async fn f6_aggregating_engine_and_percentiles() {
    let ex = fresh().await;
    // AggregatingMergeTree is a real engine (SummingMergeTree falls through to
    // Default — accepted but no summing semantics; observe should not rely on it).
    exec(&ex, "CREATE TABLE f6g (k TEXT, n BIGINT) WITH (engine='aggregating_mergetree') ORDER BY (k)").await;
    // Percentiles: functional form QUANTILE/PERCENTILE_CONT(value, fraction),
    // MEDIAN(value). Standard `WITHIN GROUP (ORDER BY ...)` is NOT supported.
    exec(&ex, "CREATE TABLE f6q (lat BIGINT)").await;
    exec(&ex, "INSERT INTO f6q (lat) VALUES (10), (20), (30), (40), (100)").await;
    assert_eq!(i64v(&rows(&ex, "SELECT MEDIAN(lat) FROM f6q").await[0][0]), 30, "MEDIAN");
    assert_eq!(i64v(&rows(&ex, "SELECT QUANTILE(lat, 1.0) FROM f6q").await[0][0]), 100, "QUANTILE p100");
    assert_eq!(i64v(&rows(&ex, "SELECT COUNT(DISTINCT lat) FROM f6q").await[0][0]), 5, "COUNT(DISTINCT)");
}
