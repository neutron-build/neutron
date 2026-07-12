//! Regression for `ts_range_bound`: TS_RANGE_COUNT / TS_RANGE_AVG must treat
//! their interval as INCLUSIVE on both ends, i.e. [start, end]. A point whose
//! timestamp equals `end` must be counted/averaged.
//!
//! This matches point-interval time-series semantics (the Prometheus HTTP range
//! query treats both start and end as inclusive). Bucketed/windowed aggregation
//! deliberately stays half-open elsewhere to avoid double-counting at adjacent
//! bucket edges, but TS_RANGE_* is a single point-interval query.
//!
//! The bug was in `Series::query_range_indices` (filtered `ts >= start && ts <
//! end`, dropping a sample at `ts == end`) coupled with the partition pruner
//! `TimeIndex::range` (broke on `meta.min_ts >= end_ts`, which could skip the
//! whole partition that holds the `end` sample). With points at t=10,20,30,
//! TS_RANGE_COUNT(s,10,30) returned 2 (dropping t=30) instead of 3.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use std::sync::Arc;

async fn fresh() -> Arc<Executor> {
    let c = Arc::new(Catalog::new());
    let s: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    Arc::new(Executor::new(c, s))
}

async fn one(ex: &Executor, sql: &str) -> Result<String, String> {
    match ex.execute(sql).await {
        Ok(mut r) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .first()
                .and_then(|r| r.first())
                .map(|v| v.to_string())
                .unwrap_or_default()),
            o => Err(format!("{o:?}")),
        },
        Err(e) => Err(format!("{e:?}")),
    }
}

/// The canonical reproduction: three points at t=10,20,30; the inclusive range
/// [10, 30] must include the sample sitting exactly on the upper bound.
#[tokio::test]
async fn ts_range_count_includes_end_boundary() {
    let ex = fresh().await;
    one(&ex, "SELECT TS_INSERT('s', 10, 1.0)").await.unwrap();
    one(&ex, "SELECT TS_INSERT('s', 20, 2.0)").await.unwrap();
    one(&ex, "SELECT TS_INSERT('s', 30, 3.0)").await.unwrap();

    // Inclusive [10, 30] -> all three points (the bug returned 2).
    assert_eq!(
        one(&ex, "SELECT TS_RANGE_COUNT('s', 10, 30)")
            .await
            .unwrap(),
        "3"
    );
    // Lower bound is inclusive too (unchanged): [20, 30] -> 20, 30.
    assert_eq!(
        one(&ex, "SELECT TS_RANGE_COUNT('s', 20, 30)")
            .await
            .unwrap(),
        "2"
    );
    // A range that ends just before the last point still excludes it.
    assert_eq!(
        one(&ex, "SELECT TS_RANGE_COUNT('s', 10, 29)")
            .await
            .unwrap(),
        "2"
    );
    // Single-point interval where start == end picks up exactly that point.
    assert_eq!(
        one(&ex, "SELECT TS_RANGE_COUNT('s', 30, 30)")
            .await
            .unwrap(),
        "1"
    );
}

/// TS_RANGE_AVG must average the end-boundary point as well.
#[tokio::test]
async fn ts_range_avg_includes_end_boundary() {
    let ex = fresh().await;
    one(&ex, "SELECT TS_INSERT('s', 10, 1.0)").await.unwrap();
    one(&ex, "SELECT TS_INSERT('s', 20, 2.0)").await.unwrap();
    one(&ex, "SELECT TS_INSERT('s', 30, 3.0)").await.unwrap();

    // (1+2+3)/3 = 2.0 — the bug averaged only (1+2)/2 = 1.5.
    let avg = one(&ex, "SELECT TS_RANGE_AVG('s', 10, 30)").await.unwrap();
    let v: f64 = avg.parse().unwrap();
    assert!((v - 2.0).abs() < 1e-9, "expected 2.0, got {v}");
}

/// Exercises the coupled partition-pruner fix: with enough spread that the
/// `end` sample lands in its own partition whose min_ts == end, the partition
/// must still be scanned (the prune break must use `>` not `>=`). We use 1-hour
/// timestamps so each point falls in a distinct hour partition under the
/// default Hour bucketing of the time-series store.
#[tokio::test]
async fn ts_range_count_end_in_own_partition() {
    let ex = fresh().await;
    let hour = 3_600_000u64;
    // Points at hour 0, 1, 2, 3 (each in its own hour partition).
    for h in 0..4u64 {
        one(
            &ex,
            &format!("SELECT TS_INSERT('p', {}, {}.0)", h * hour, h),
        )
        .await
        .unwrap();
    }
    // Inclusive [0, 3h]: the point at exactly 3h sits in a partition whose
    // min_ts == end and must not be pruned -> all four points.
    assert_eq!(
        one(&ex, &format!("SELECT TS_RANGE_COUNT('p', 0, {})", 3 * hour))
            .await
            .unwrap(),
        "4"
    );
    // [0, 2h] -> hours 0,1,2 = 3 points.
    assert_eq!(
        one(&ex, &format!("SELECT TS_RANGE_COUNT('p', 0, {})", 2 * hour))
            .await
            .unwrap(),
        "3"
    );
}
