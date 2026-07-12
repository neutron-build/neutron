//! Regressions for the aggregate-wrapper executor bug family (teploy-observe
//! dogfood findings #24 / #26 / #28 / #15):
//!
//! - #24a: aggregate over an empty-filter result set must return ONE row
//!   (COUNT -> 0, SUM/MAX/... -> NULL), not zero rows.
//! - #24b: CAST(<aggregate> AS TEXT) must render the value, not "".
//! - #26:  COALESCE(MAX(x), 0) — the aggregate detector must descend through
//!   COALESCE/CASE/arithmetic instead of erroring "outside of aggregate
//!   context".
//! - #15:  CASE WHEN SUM(x) > 0 THEN SUM(y)/SUM(x) ELSE 0 END with GROUP BY.
//! - #28:  GROUP BY ... HAVING COUNT(*) >= N must keep matching groups.

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
        Value::Text(s) => s.trim().parse().unwrap_or(i64::MIN),
        Value::Null => i64::MIN,
        other => panic!("not numeric: {other:?}"),
    }
}

async fn seeded(table: &str) -> Arc<Executor> {
    let ex = fresh().await;
    exec(
        &ex,
        &format!("CREATE TABLE {table} (site TEXT, uid TEXT, n BIGINT)"),
    )
    .await;
    exec(
        &ex,
        &format!(
            "INSERT INTO {table} VALUES \
             ('s1', 'a', 10), ('s1', 'a', 20), ('s1', 'b', 5), ('s2', 'c', 7)"
        ),
    )
    .await;
    ex
}

// ── #24a: aggregate over empty filter returns one row ──────────────────────

#[tokio::test]
async fn count_over_empty_filter_returns_one_row() {
    let ex = seeded("aw_t1").await;
    let r = rows(&ex, "SELECT COUNT(*) FROM aw_t1 WHERE site = 'nope'").await;
    assert_eq!(r.len(), 1, "COUNT over empty filter must return one row");
    assert_eq!(i64v(&r[0][0]), 0);
}

#[tokio::test]
async fn cast_count_over_empty_filter_returns_one_row() {
    let ex = seeded("aw_t2").await;
    let r = rows(
        &ex,
        "SELECT CAST(COUNT(*) AS TEXT) FROM aw_t2 WHERE site = 'nope'",
    )
    .await;
    assert_eq!(
        r.len(),
        1,
        "CAST(COUNT) over empty filter must return one row"
    );
    assert_eq!(r[0][0], Value::Text("0".into()));
}

#[tokio::test]
async fn sum_over_empty_filter_returns_one_null_row() {
    let ex = seeded("aw_t3").await;
    let r = rows(&ex, "SELECT SUM(n) FROM aw_t3 WHERE site = 'nope'").await;
    assert_eq!(r.len(), 1, "SUM over empty filter must return one row");
    assert_eq!(r[0][0], Value::Null, "SUM of no rows is NULL per SQL spec");
}

// ── #24b: CAST(<aggregate> AS TEXT) renders the value ──────────────────────

#[tokio::test]
async fn cast_aggregates_to_text_render_values() {
    let ex = seeded("aw_t4").await;
    let r = rows(&ex, "SELECT CAST(COUNT(*) AS TEXT) FROM aw_t4").await;
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("4".into()), "CAST(COUNT(*) AS TEXT)");

    let r = rows(&ex, "SELECT CAST(SUM(n) AS TEXT) FROM aw_t4").await;
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("42".into()), "CAST(SUM(n) AS TEXT)");
}

// ── #26: COALESCE / arithmetic around aggregates ────────────────────────────

#[tokio::test]
async fn coalesce_max_is_valid_aggregate_context() {
    let ex = seeded("aw_t5").await;
    let r = rows(
        &ex,
        "SELECT COUNT(*), COALESCE(MAX(n), 0) FROM aw_t5 WHERE site = 's1'",
    )
    .await;
    assert_eq!(r.len(), 1);
    assert_eq!(i64v(&r[0][0]), 3);
    assert_eq!(i64v(&r[0][1]), 20);
}

#[tokio::test]
async fn coalesce_max_over_empty_filter_defaults() {
    let ex = seeded("aw_t6").await;
    let r = rows(
        &ex,
        "SELECT COALESCE(MAX(n), 0) FROM aw_t6 WHERE site = 'nope'",
    )
    .await;
    assert_eq!(r.len(), 1);
    assert_eq!(
        i64v(&r[0][0]),
        0,
        "COALESCE default must apply to empty MAX"
    );
}

// ── #15: CASE WHEN around aggregates (with GROUP BY) ────────────────────────

#[tokio::test]
async fn case_when_sum_guard_with_group_by() {
    let ex = seeded("aw_t7").await;
    let r = rows(
        &ex,
        "SELECT site, CASE WHEN SUM(n) > 0 THEN SUM(n * 2) / SUM(n) ELSE 0 END \
         FROM aw_t7 GROUP BY site ORDER BY site",
    )
    .await;
    assert_eq!(r.len(), 2, "one row per group");
    assert_eq!(i64v(&r[0][1]), 2, "s1: 70/35 = 2");
    assert_eq!(i64v(&r[1][1]), 2, "s2: 14/7 = 2");
}

// ── #28: HAVING over aggregate counts ───────────────────────────────────────

#[tokio::test]
async fn having_count_keeps_matching_groups() {
    let ex = seeded("aw_t8").await;
    let r = rows(
        &ex,
        "SELECT uid FROM aw_t8 WHERE site = 's1' AND uid != '' \
         GROUP BY uid HAVING COUNT(*) >= 1",
    )
    .await;
    assert_eq!(
        r.len(),
        2,
        "HAVING COUNT(*) >= 1 must keep both s1 uids (got {r:?})"
    );

    let r = rows(
        &ex,
        "SELECT uid FROM aw_t8 WHERE site = 's1' GROUP BY uid HAVING COUNT(*) >= 2",
    )
    .await;
    assert_eq!(r.len(), 1, "only uid 'a' has >= 2 events (got {r:?})");
    assert_eq!(r[0][0], Value::Text("a".into()));
}
