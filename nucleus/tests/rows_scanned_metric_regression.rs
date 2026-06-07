//! Regression for `rows_scanned_metric`: the `rows_scanned` metric must report
//! rows EXAMINED by a scan, not rows RETURNED.
//!
//! `rows_scanned` is documented as "Total rows scanned" and is the signal the
//! cost-based index advisor uses to recommend `CREATE INDEX`. By authoritative
//! DB-engine convention (Postgres `EXPLAIN ANALYZE` reports Seq Scan "rows" =
//! matched + "rows removed by filter"), a full sequential scan must report the
//! number of rows it inspected — roughly N for an N-row table.
//!
//! The bug was in the fused fast-equality path: `executor::query` called
//! `storage.fast_scan_where_eq()` and then did
//! `metrics.rows_scanned.inc_by(rows.len())`. But `fast_scan_where_eq` iterates
//! EVERY visible row and returns only the matched rows, so `rows.len()` was the
//! (typically tiny) match count, not the scan size. A
//! `SELECT * FROM t WHERE non_indexed_col = X` over N=2000 reported
//! rows_scanned=2 instead of ~2000.
//!
//! The fix makes `fast_scan_where_eq` return `(matched_rows, rows_examined)`
//! and has the executor report the examined count to the metric.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::metrics::MetricsRegistry;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use std::sync::Arc;

const N: usize = 2000;

async fn seeded() -> (Arc<Executor>, Arc<MetricsRegistry>) {
    let metrics = Arc::new(MetricsRegistry::new());
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage).with_metrics(Arc::clone(&metrics)));

    // `id` is the PK (indexed); `val` is NOT indexed — equality on `val` forces
    // a full sequential scan.
    ex.execute("CREATE TABLE eff_test (id INTEGER PRIMARY KEY, val INTEGER)")
        .await
        .unwrap();

    const BATCH: usize = 200;
    let mut inserted = 0usize;
    while inserted < N {
        let end = (inserted + BATCH).min(N);
        // val = i*7 % 1000 → a few rows share each value, so the match count is
        // small while the scan must still examine all N rows.
        let values: Vec<String> = (inserted..end)
            .map(|i| format!("({}, {})", i as i64, (i as i64) * 7 % 1000))
            .collect();
        ex.execute(&format!("INSERT INTO eff_test VALUES {}", values.join(", ")))
            .await
            .unwrap();
        inserted = end;
    }
    (ex, metrics)
}

/// A non-indexed equality predicate performs a full sequential scan, so
/// `rows_scanned` must reflect ~N rows examined — NOT the handful of rows that
/// matched. Before the fix this reported the match count (2-3) instead of ~2000.
#[tokio::test]
async fn non_indexed_equality_reports_full_scan_size() {
    let (ex, metrics) = seeded().await;

    // `val = 0` matches only ids 0, 1000 (i*7 % 1000 == 0) → 2 matched rows,
    // but the scan must examine all N rows.
    let sql = "SELECT * FROM eff_test WHERE val = 0";

    let before = metrics.rows_scanned.get();
    let mut r = ex.execute(sql).await.unwrap();
    let scanned = metrics.rows_scanned.get() - before;

    // Sanity: the result set is tiny (this is what the bug erroneously counted).
    let returned = match r.pop() {
        Some(nucleus::executor::ExecResult::Select { rows, .. }) => rows.len(),
        other => panic!("expected SELECT result, got {other:?}"),
    };
    assert!(
        returned < (N / 2),
        "test precondition: match count should be small, got {returned}"
    );

    // The metric must reflect rows EXAMINED by the full scan (≈ N), matching
    // Postgres Seq Scan semantics. The probe (probe_efficiency Test 4) flags
    // `nonpk_scanned < N/2` as FullScanTooLow; assert the same lower bound.
    let lower_bound = (N as u64) / 2;
    assert!(
        scanned >= lower_bound,
        "non-indexed equality reported rows_scanned={scanned} for N={N}; \
         expected a full scan (~{N}, at least {lower_bound}). The metric must \
         count rows EXAMINED, not the {returned} rows returned."
    );
}

/// Cross-check: an indexed PK equality lookup must NOT inflate `rows_scanned`.
/// The examined-count fix is confined to the non-indexed seq-scan fast path; the
/// O(log N) index lookup path still reports only the candidate rows it touched.
#[tokio::test]
async fn indexed_pk_equality_stays_cheap() {
    let (ex, metrics) = seeded().await;

    let before = metrics.rows_scanned.get();
    let _ = ex.execute("SELECT * FROM eff_test WHERE id = 1234").await.unwrap();
    let scanned = metrics.rows_scanned.get() - before;

    assert!(
        scanned < 100,
        "indexed PK lookup scanned {scanned} rows; expected an O(log N) index \
         lookup touching only the matched row(s), not a full scan"
    );
}
