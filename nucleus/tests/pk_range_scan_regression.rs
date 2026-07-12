//! Regression for `pk_range_scan`: a two-sided range over an indexed/PK column
//! (`id >= lo AND id <= hi`) must execute as a B-tree range scan whose cost is
//! O(log N + range_width), NOT O(N - lo).
//!
//! This matches PostgreSQL, which folds compatible range quals on a leading
//! B-tree column into ONE Index Scan with both bounds as `Index Cond`
//! (`id >= lo AND id <= hi`) rather than letting the upper bound degrade to a
//! filter.
//!
//! The bug was in `planner::plan_scan_unified`: `classify_predicate` mapped each
//! of `id >= lo` and `id <= hi` to an independent `Range` predicate, and the
//! per-predicate index loop only special-cased a single sqlparser `BETWEEN`
//! node. A `>=`/`<=` pair therefore produced an IndexScan carrying only the
//! lower bound (as a `lookup_key`), demoting the upper bound to a residual
//! Filter. The executor's IndexScan handler cannot drive a column-bearing
//! `lookup_key` through the index, so it fell through to an AST scan from `lo`
//! to end-of-table — O(N - lo) rows. Results were correct; only efficiency was
//! wrong.
//!
//! The fix detects a two-sided range on a BTree-indexed column up front (via the
//! existing `find_range_scan_opportunity` helper, the same one `plan_scan`
//! already uses) and emits an IndexScan with BOTH `range_lo`/`range_hi` set so
//! the executor's range branch + storage `index_lookup_range`
//! (BTreeMap::range) serve it in O(log N + k).
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::metrics::MetricsRegistry;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use std::sync::Arc;

const N: usize = 2000;

async fn seeded() -> (Arc<Executor>, Arc<MetricsRegistry>) {
    let metrics = Arc::new(MetricsRegistry::new());
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage).with_metrics(Arc::clone(&metrics)));

    ex.execute("CREATE TABLE eff_test (id INTEGER PRIMARY KEY, val INTEGER)")
        .await
        .unwrap();

    // Bulk-insert N rows in batches.
    const BATCH: usize = 200;
    let mut inserted = 0usize;
    while inserted < N {
        let end = (inserted + BATCH).min(N);
        let values: Vec<String> = (inserted..end)
            .map(|i| format!("({}, {})", i as i64, (i as i64) * 7 % 1000))
            .collect();
        ex.execute(&format!(
            "INSERT INTO eff_test VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();
        inserted = end;
    }
    (ex, metrics)
}

async fn select_ids(ex: &Executor, sql: &str) -> Vec<i64> {
    let mut out = Vec::new();
    if let Ok(mut r) = ex.execute(sql).await
        && let Some(ExecResult::Select { rows, columns }) = r.pop()
    {
        let id_col = columns
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case("id"))
            .unwrap_or(0);
        for row in rows {
            match &row[id_col] {
                nucleus::types::Value::Int32(i) => out.push(*i as i64),
                nucleus::types::Value::Int64(i) => out.push(*i),
                _ => {}
            }
        }
    }
    out.sort_unstable();
    out
}

async fn explain(ex: &Executor, sql: &str) -> String {
    let mut r = ex.execute(&format!("EXPLAIN {sql}")).await.unwrap();
    if let Some(ExecResult::Select { rows, .. }) = r.pop() {
        rows.iter()
            .flat_map(|row| row.iter())
            .filter_map(|v| {
                if let nucleus::types::Value::Text(s) = v {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        String::new()
    }
}

/// Core efficiency assertion: a 10-row inclusive range over the PK scans
/// O(range_width) rows, not O(N - lo). Before the fix this scanned ~N - lo
/// (1307 rows for lo=693, N=2000); after, it scans ~10.
#[tokio::test]
async fn pk_two_sided_range_uses_index_not_tail_scan() {
    let (ex, metrics) = seeded().await;

    let lo = 693i64;
    let hi = 702i64; // 10-row inclusive window
    let sql = format!("SELECT * FROM eff_test WHERE id >= {lo} AND id <= {hi}");

    // Correctness: exactly the 10 ids in [lo, hi].
    let got = select_ids(&ex, &sql).await;
    let expected: Vec<i64> = (lo..=hi).collect();
    assert_eq!(got, expected, "wrong rows for inclusive PK range");

    // Efficiency: a B-tree range scan must read close to the range width, far
    // below the O(N - lo) tail scan the bug produced (= 1307 here). We allow
    // generous slack (5% of N) so the test tracks the algorithmic class, not a
    // brittle exact count.
    let before = metrics.rows_scanned.get();
    let _ = ex.execute(&sql).await.unwrap();
    let scanned = metrics.rows_scanned.get() - before;
    let budget = (N as u64) / 20; // 100 rows; the bug scanned 1307
    assert!(
        scanned <= budget,
        "PK two-sided range scanned {scanned} rows (budget {budget}); \
         expected O(range_width) ~10, not an O(N - lo) tail scan"
    );

    // Plan shape: both bounds must drive the index as a single Index Range
    // covering `[lo, hi]`. The bug produced a `Filter` *node* wrapping an
    // IndexScan whose Index Cond carried only the lower bound; the fix yields a
    // single IndexScan with `Index Range: [lo, hi]`. (The IndexScan still lists
    // an indented `Filter:` attribute for the inclusive range_predicate
    // post-check — that is the correctness guard, not a wrapper node.)
    let plan = explain(&ex, &sql).await;
    assert!(
        plan.contains("Index Scan"),
        "expected an Index Scan, got plan:\n{plan}"
    );
    assert!(
        plan.contains(&format!("[{lo}, {hi}]")),
        "both bounds should drive the index as a single Index Range [{lo}, {hi}]; \
         a single-bound scan (upper bound demoted to a Filter wrapper) would not \
         show this. plan:\n{plan}"
    );
    // The index must serve BOTH bounds as a single Index Range (asserted above) —
    // that, plus the O(range_width) row budget, is what proves the O(N-lo) tail
    // scan is gone. A residual `Filter` node re-checking the predicates on the
    // narrowed rows is acceptable and, in fact, REQUIRED for correctness when the
    // indexed column carries extra predicates beyond the two folded bounds
    // (e.g. `id BETWEEN 7 AND 13 AND id <= -4`): the index only narrows
    // candidates, so the full WHERE must be reapplied. Re-checking the two bounds
    // on ~range_width rows is negligible. (We intentionally do NOT assert the
    // absence of a Filter wrapper.)
}

/// Strict bounds (`>` / `<`) must stay correct — the executor post-filters the
/// inclusive BTreeMap range with the original predicate.
#[tokio::test]
async fn pk_strict_bounds_are_exclusive_and_indexed() {
    let (ex, metrics) = seeded().await;

    let sql = "SELECT * FROM eff_test WHERE id > 693 AND id < 702";
    let got = select_ids(&ex, sql).await;
    let expected: Vec<i64> = (694..=701).collect(); // strict on both ends
    assert_eq!(got, expected, "strict bounds returned wrong rows");

    let before = metrics.rows_scanned.get();
    let _ = ex.execute(sql).await.unwrap();
    let scanned = metrics.rows_scanned.get() - before;
    assert!(
        scanned <= (N as u64) / 20,
        "strict-bound range did not use the index (scanned {scanned})"
    );
}

/// Mixed bounds (one inclusive, one strict) and reversed operand order
/// (`hi >= id`) must also be recognized as a two-sided range.
#[tokio::test]
async fn pk_mixed_and_reversed_bounds() {
    let (ex, _metrics) = seeded().await;

    // `id >= 100 AND 110 >= id`  ==  100 <= id <= 110
    let got = select_ids(&ex, "SELECT * FROM eff_test WHERE id >= 100 AND 110 >= id").await;
    assert_eq!(got, (100..=110).collect::<Vec<_>>());

    // Inclusive lower, strict upper.
    let got = select_ids(&ex, "SELECT * FROM eff_test WHERE id >= 100 AND id < 110").await;
    assert_eq!(got, (100..=109).collect::<Vec<_>>());
}

/// A two-sided PK range combined with a non-indexed equality on another column
/// must still return correct results: the range drives the index and the extra
/// predicate is applied as a residual filter (it must NOT be silently dropped).
#[tokio::test]
async fn pk_range_with_extra_predicate_stays_correct() {
    let (ex, _metrics) = seeded().await;

    // val = i*7 % 1000. Within [0, 50], find ids whose val == that id's val.
    // Just assert the residual predicate is honored: pick a concrete value.
    let sql = "SELECT * FROM eff_test WHERE id >= 0 AND id <= 50 AND val = 70";
    let got = select_ids(&ex, sql).await;
    // val == 70 -> i*7 % 1000 == 70 -> i == 10 within [0,50].
    assert_eq!(got, vec![10], "residual equality predicate was not applied");
}
