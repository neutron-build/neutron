//! Temporal predicates across mixed literal/column types.
//!
//! `Value`'s `Ord` ranks mismatched temporal variants by TYPE TAG rather than by
//! instant (Date=6, Timestamp=7, TimestampTz=8), and the storage fast-scan paths
//! compare with raw `Value` ordering. So a `TIMESTAMP` literal against a
//! `TIMESTAMPTZ` column silently matched nothing: `v >= low` was true because 8
//! outranks 7, and `v <= high` was false for the same reason.
//!
//! The invariant these tests protect: a predicate means the same thing wherever
//! it is evaluated. If `SELECT <pred> FROM t` says true for a row, then
//! `SELECT * FROM t WHERE <pred>` must return that row — whatever fast path the
//! executor picks, and whether or not an index exists.

use super::*;

const INSTANT: &str = "2026-07-01 00:00:00";
const BEFORE: &str = "2026-01-01 00:00:00";
const AFTER: &str = "2027-01-01 00:00:00";

async fn seeded(ex: &Executor, indexed: bool) {
    exec(
        ex,
        "CREATE TABLE t (id INT PRIMARY KEY, tstz TIMESTAMPTZ, tsp TIMESTAMP, d DATE)",
    )
    .await;
    if indexed {
        exec(ex, "CREATE INDEX t_tstz ON t (tstz)").await;
        exec(ex, "CREATE INDEX t_tsp ON t (tsp)").await;
    }
    exec(
        ex,
        &format!(
            "INSERT INTO t VALUES (1, TIMESTAMP '{INSTANT}', TIMESTAMP '{INSTANT}', DATE '2026-07-01')"
        ),
    )
    .await;
}

async fn count(ex: &Executor, sql: &str) -> usize {
    let r = exec(ex, sql).await;
    rows(&r[0]).len()
}

/// The predicate as a projected expression is the oracle: whatever it says for
/// a row, the WHERE clause must agree.
async fn assert_where_agrees_with_projection(ex: &Executor, predicate: &str, label: &str) {
    let projected = exec(ex, &format!("SELECT {predicate} FROM t")).await;
    let expected: usize = rows(&projected[0])
        .iter()
        .filter(|r| matches!(r[0], Value::Bool(true)))
        .count();
    let actual = count(ex, &format!("SELECT id FROM t WHERE {predicate}")).await;
    assert_eq!(
        expected, actual,
        "{label}: `SELECT {predicate}` says {expected} row(s) match but WHERE returned {actual}"
    );
}

#[tokio::test]
async fn test_timestamp_literal_against_timestamptz_column() {
    for indexed in [false, true] {
        let ex = test_executor();
        seeded(&ex, indexed).await;
        let label = if indexed { "indexed" } else { "unindexed" };

        // One-sided bounds: the shape that goes through the synthetic-sentinel
        // range in `try_storage_fast_scan`.
        assert_where_agrees_with_projection(&ex, &format!("tstz >= TIMESTAMP '{BEFORE}'"), label)
            .await;
        assert_where_agrees_with_projection(&ex, &format!("tstz > TIMESTAMP '{BEFORE}'"), label)
            .await;
        assert_where_agrees_with_projection(&ex, &format!("tstz <= TIMESTAMP '{AFTER}'"), label)
            .await;
        assert_where_agrees_with_projection(&ex, &format!("tstz < TIMESTAMP '{AFTER}'"), label)
            .await;
        // Equality, which goes through the point-lookup path.
        assert_where_agrees_with_projection(&ex, &format!("tstz = TIMESTAMP '{INSTANT}'"), label)
            .await;
        // Two-sided window: the observability shape.
        assert_where_agrees_with_projection(
            &ex,
            &format!("tstz >= TIMESTAMP '{BEFORE}' AND tstz < TIMESTAMP '{AFTER}'"),
            label,
        )
        .await;
        // BETWEEN, which reaches the range path by a different route.
        assert_where_agrees_with_projection(
            &ex,
            &format!("tstz BETWEEN TIMESTAMP '{BEFORE}' AND TIMESTAMP '{AFTER}'"),
            label,
        )
        .await;
    }
}

#[tokio::test]
async fn test_timestamptz_literal_against_timestamp_column() {
    for indexed in [false, true] {
        let ex = test_executor();
        seeded(&ex, indexed).await;
        let label = if indexed { "indexed" } else { "unindexed" };
        assert_where_agrees_with_projection(
            &ex,
            &format!("tsp >= TIMESTAMPTZ '{BEFORE}+00'"),
            label,
        )
        .await;
        assert_where_agrees_with_projection(
            &ex,
            &format!("tsp >= TIMESTAMPTZ '{BEFORE}+00' AND tsp < TIMESTAMPTZ '{AFTER}+00'"),
            label,
        )
        .await;
    }
}

#[tokio::test]
async fn test_date_column_against_timestamp_literal() {
    // A DATE column compared to a TIMESTAMP literal must not silently drop
    // rows either. Note the literal must NOT be truncated to a date: comparing
    // against midday is not the same as comparing against midnight.
    let ex = test_executor();
    seeded(&ex, false).await;
    assert_where_agrees_with_projection(&ex, &format!("d >= TIMESTAMP '{BEFORE}'"), "date col")
        .await;
    assert_where_agrees_with_projection(&ex, &format!("d < TIMESTAMP '{AFTER}'"), "date col").await;
}

/// Adding an index must never change a result set.
#[tokio::test]
async fn test_index_does_not_change_temporal_results() {
    let unindexed = test_executor();
    seeded(&unindexed, false).await;
    let indexed = test_executor();
    seeded(&indexed, true).await;

    for predicate in [
        format!("tstz >= TIMESTAMP '{BEFORE}'"),
        format!("tstz = TIMESTAMP '{INSTANT}'"),
        format!("tstz >= TIMESTAMP '{BEFORE}' AND tstz < TIMESTAMP '{AFTER}'"),
        format!("tsp >= TIMESTAMP '{BEFORE}'"),
        format!("tstz >= TIMESTAMP '{AFTER}'"),
    ] {
        let sql = format!("SELECT id FROM t WHERE {predicate}");
        assert_eq!(
            count(&unindexed, &sql).await,
            count(&indexed, &sql).await,
            "adding an index changed the answer for: {predicate}"
        );
    }
}

/// A bounded window over an indexed column must read the matching rows, not the
/// table.
///
/// `TIMESTAMP '…'` parses as `Expr::TypedString`, which `expr_has_unsupported`
/// treated as an unsupported *feature* rather than a constant. That made
/// `query_eligible_for_plan` reject the whole query, so it fell to the AST path
/// and gave up the plan path's index range scan — a time window over an indexed
/// TIMESTAMP column full-scanned while the identical query over a BIGINT epoch
/// column used the index. Answers stayed correct, so only a scan-count or timing
/// assertion catches a regression; the row assertions above will not.
#[tokio::test]
async fn test_temporal_window_uses_the_index() {
    const ROWS: i64 = 1_000;

    for (ty, mk, lo, hi) in [
        (
            "BIGINT",
            0u8,
            "1700000000000".to_string(),
            "1700000005000".to_string(),
        ),
        (
            "TIMESTAMP",
            1,
            "TIMESTAMP '2026-07-01 00:00:00'".to_string(),
            "TIMESTAMP '2026-07-01 00:00:05'".to_string(),
        ),
        (
            "TIMESTAMPTZ",
            1,
            "TIMESTAMP '2026-07-01 00:00:00'".to_string(),
            "TIMESTAMP '2026-07-01 00:00:05'".to_string(),
        ),
        (
            "DATE",
            2,
            "DATE '2026-01-01'".to_string(),
            "DATE '2026-01-06'".to_string(),
        ),
    ] {
        let ex = test_executor();
        exec(
            &ex,
            &format!("CREATE TABLE w (id INT PRIMARY KEY, ts {ty})"),
        )
        .await;
        exec(&ex, "CREATE INDEX w_ts ON w (ts)").await;
        for i in 0..ROWS {
            let v = match mk {
                0 => format!("{}", 1_700_000_000_000i64 + i * 1000),
                1 => format!(
                    "TIMESTAMP '2026-07-01 {:02}:{:02}:{:02}'",
                    i / 3600,
                    (i / 60) % 60,
                    i % 60
                ),
                _ => format!("DATE '2026-01-01' + {i}"),
            };
            exec(&ex, &format!("INSERT INTO w VALUES ({i}, {v})")).await;
        }

        let sql = format!("SELECT id FROM w WHERE ts >= {lo} AND ts < {hi}");
        let before = ex.metrics().rows_scanned.get();
        let result = exec(&ex, &sql).await;
        let scanned = ex.metrics().rows_scanned.get() - before;
        let matched = rows(&result[0]).len();

        assert_eq!(matched, 5, "{ty}: wrong rows for the window — {sql}");
        // Generous ceiling: a regression to a full scan reads ROWS, an index
        // range scan reads the 5 matches. Anything in between still passes, so
        // this fires on the failure mode rather than on plan-choice noise.
        assert!(
            scanned < ROWS as u64 / 10,
            "{ty}: window matching {matched} rows scanned {scanned} of {ROWS} — \
             the index range scan was not used"
        );
    }
}

/// The structural guard: `Value`'s `Ord` and `compare_values` are two
/// comparators over the same values, and every optimised path picks one while
/// the general path picks the other. They must not disagree — that divergence
/// is the entire bug.
#[test]
fn test_value_ord_agrees_with_compare_values_on_temporals() {
    // 2026-07-01 as days and as microseconds, both on the 2000-01-01 epoch.
    const DAY: i32 = 9678;
    const MICROS: i64 = DAY as i64 * 86_400_000_000;

    let values = [
        Value::Date(DAY),
        Value::Date(DAY + 1),
        Value::Timestamp(MICROS),
        Value::Timestamp(MICROS + 1),
        Value::TimestampTz(MICROS),
        Value::TimestampTz(MICROS - 1),
        Value::Timestamp(i64::MAX),
        Value::TimestampTz(i64::MIN),
    ];

    for a in &values {
        for b in &values {
            let via_helper = crate::executor::helpers::compare_values(a, b)
                .expect("temporal values are always comparable");
            let via_ord = a.cmp(b);
            assert_eq!(
                via_helper, via_ord,
                "comparator divergence: {a:?} vs {b:?} — compare_values says \
                 {via_helper:?}, Value::Ord says {via_ord:?}"
            );
        }
    }
}

/// A window that matches nothing must return nothing — the fix must not
/// over-correct into matching everything.
#[tokio::test]
async fn test_non_matching_temporal_windows_stay_empty() {
    for indexed in [false, true] {
        let ex = test_executor();
        seeded(&ex, indexed).await;
        assert_eq!(
            count(
                &ex,
                &format!("SELECT id FROM t WHERE tstz >= TIMESTAMP '{AFTER}'")
            )
            .await,
            0,
            "a window after every row matched something"
        );
        assert_eq!(
            count(
                &ex,
                &format!("SELECT id FROM t WHERE tstz < TIMESTAMP '{BEFORE}'")
            )
            .await,
            0,
            "a window before every row matched something"
        );
    }
}

/// Interval↔interval predicates. `compare_values` had no `(Interval,
/// Interval)` arm, so `iv = INTERVAL '1 day'` silently matched nothing while
/// `Ord for Value` ordered intervals fine — the same projection/WHERE
/// divergence shape this file exists for (WIR-5).
#[tokio::test]
async fn test_interval_predicates() {
    let ex = test_executor();

    let r = exec(
        &ex,
        "SELECT INTERVAL '1 day' = INTERVAL '1 day', INTERVAL '1 day' < INTERVAL '2 days', \
         INTERVAL '1 month' > INTERVAL '30 days'",
    )
    .await;
    assert_eq!(
        rows(&r[0])[0],
        vec![Value::Bool(true), Value::Bool(true), Value::Bool(true),],
        "interval comparisons must compare, not silently answer false"
    );

    exec(
        &ex,
        "CREATE TABLE shifts (id INT PRIMARY KEY, len INTERVAL)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO shifts VALUES (1, INTERVAL '1 day'), (2, INTERVAL '2 days')",
    )
    .await;
    assert_eq!(
        count(&ex, "SELECT id FROM shifts WHERE len = INTERVAL '1 day'").await,
        1,
        "equality predicate on an interval column matched nothing"
    );
    assert_eq!(
        count(&ex, "SELECT id FROM shifts WHERE len < INTERVAL '2 days'").await,
        1
    );
}
