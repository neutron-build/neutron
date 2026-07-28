//! Which WHERE expression forms actually reach an index.
//!
//! Two bugs shipped because an eligibility list silently omitted an expression
//! form: `Expr::TypedString` (`TIMESTAMP '…'`) and `Expr::Interval` were each
//! classified as unsupported *features* rather than constants, so
//! `query_eligible_for_plan` rejected the whole query and it fell to the AST
//! path — losing the index range scan. Answers stayed correct in both cases, so
//! no row-level assertion could catch them; only a scan count can.
//!
//! These tests assert the scan count, not the rows. A regression here means a
//! query got slower, never wrong, which is exactly why it needs its own guard.

use super::*;

const ROWS: i64 = 1_000;

/// One row per distinct value on every indexed column, so a point lookup
/// matches exactly one row and a five-wide window matches five.
async fn indexed_table() -> Executor {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE a (id INT PRIMARY KEY, n BIGINT, ts TIMESTAMP, d DATE, s TEXT, u UUID)",
    )
    .await;
    for (name, col) in [
        ("a_n", "n"),
        ("a_ts", "ts"),
        ("a_d", "d"),
        ("a_s", "s"),
        ("a_u", "u"),
    ] {
        exec(&ex, &format!("CREATE INDEX {name} ON a ({col})")).await;
    }
    for i in 0..ROWS {
        exec(
            &ex,
            &format!(
                "INSERT INTO a VALUES ({i}, {}, TIMESTAMP '2026-07-01 {:02}:{:02}:{:02}', \
                 DATE '2026-01-01' + {i}, 'k{:05}', '00000000-0000-0000-0000-{:012}')",
                1_000_000 + i,
                i / 3600,
                (i / 60) % 60,
                i % 60,
                i,
                i
            ),
        )
        .await;
    }
    ex
}

/// Rows the engine reported reading, and rows returned.
async fn scan_cost(ex: &Executor, sql: &str) -> (u64, usize) {
    let before = ex.metrics().rows_scanned.get();
    let result = exec(ex, sql).await;
    let scanned = ex.metrics().rows_scanned.get() - before;
    (scanned, rows(&result[0]).len())
}

/// Generous ceiling: a full scan reads every row, an index reads a handful.
/// Anything between still passes, so this fires on the failure mode rather than
/// on plan-choice noise.
async fn assert_indexed(ex: &Executor, expected_rows: usize, label: &str, sql: &str) {
    let (scanned, matched) = scan_cost(ex, sql).await;
    assert_eq!(matched, expected_rows, "{label}: wrong row count — {sql}");
    assert!(
        scanned < ROWS as u64 / 10,
        "{label}: matched {matched} row(s) but scanned {scanned} of {ROWS} — \
         this form no longer reaches the index. {sql}"
    );
}

/// Bounded windows over an indexed column, spelled every way a caller might.
#[tokio::test]
async fn test_range_windows_reach_the_index() {
    let ex = indexed_table().await;

    for (label, sql) in [
        (
            "integer bounds",
            "SELECT id FROM a WHERE n >= 1000000 AND n < 1000005",
        ),
        (
            "TIMESTAMP literal bounds",
            "SELECT id FROM a WHERE ts >= TIMESTAMP '2026-07-01 00:00:00' \
             AND ts < TIMESTAMP '2026-07-01 00:00:05'",
        ),
        (
            "DATE literal bounds",
            "SELECT id FROM a WHERE d >= DATE '2026-01-01' AND d < DATE '2026-01-06'",
        ),
        (
            "text bounds",
            "SELECT id FROM a WHERE s >= 'k00000' AND s < 'k00005'",
        ),
        (
            "BETWEEN integer",
            "SELECT id FROM a WHERE n BETWEEN 1000000 AND 1000004",
        ),
        (
            "BETWEEN timestamp",
            "SELECT id FROM a WHERE ts BETWEEN TIMESTAMP '2026-07-01 00:00:00' \
             AND TIMESTAMP '2026-07-01 00:00:04'",
        ),
        (
            "parenthesised",
            "SELECT id FROM a WHERE (n >= 1000010 AND n < 1000015)",
        ),
        (
            "CAST bounds",
            "SELECT id FROM a WHERE n >= CAST('1000020' AS BIGINT) \
             AND n < CAST('1000025' AS BIGINT)",
        ),
        (
            ":: cast bounds",
            "SELECT id FROM a WHERE n >= '1000030'::BIGINT AND n < '1000035'::BIGINT",
        ),
        // The canonical dashboard predicate. `Expr::Interval` was missing from
        // the eligibility list, so spelling the upper bound as an offset
        // full-scanned while the identical absolute bound used the index.
        (
            "interval-offset upper bound",
            "SELECT id FROM a WHERE ts >= TIMESTAMP '2026-07-01 00:00:00' \
             AND ts < TIMESTAMP '2026-07-01 00:00:00' + INTERVAL '5 seconds'",
        ),
    ] {
        assert_indexed(&ex, 5, label, sql).await;
    }
}

/// A window must stay indexed once wrapped in the shapes real queries use.
#[tokio::test]
async fn test_windows_stay_indexed_under_projection_and_aggregation() {
    let ex = indexed_table().await;
    let window = "ts >= TIMESTAMP '2026-07-01 00:02:00' AND ts < TIMESTAMP '2026-07-01 00:02:05'";

    assert_indexed(
        &ex,
        3,
        "ORDER BY + LIMIT",
        &format!("SELECT id FROM a WHERE {window} ORDER BY id LIMIT 3"),
    )
    .await;
    assert_indexed(
        &ex,
        1,
        "COUNT(*)",
        &format!("SELECT COUNT(*) FROM a WHERE {window}"),
    )
    .await;
    assert_indexed(
        &ex,
        5,
        "GROUP BY",
        &format!("SELECT id, COUNT(*) FROM a WHERE {window} GROUP BY id"),
    )
    .await;
}

/// Point lookups. NOTE: `n` (BIGINT) and `s` (TEXT) are deliberately absent —
/// equality on those columns currently full-scans even with an index, while
/// UUID / DATE / TIMESTAMP use it. That gap is tracked separately; asserting
/// the working types here keeps them from regressing to match.
#[tokio::test]
async fn test_point_lookups_reach_the_index() {
    let ex = indexed_table().await;

    assert_indexed(
        &ex,
        1,
        "timestamp equality",
        "SELECT id FROM a WHERE ts = TIMESTAMP '2026-07-01 00:00:07'",
    )
    .await;
    assert_indexed(
        &ex,
        1,
        "date equality",
        "SELECT id FROM a WHERE d = DATE '2026-01-10'",
    )
    .await;
    assert_indexed(
        &ex,
        1,
        "uuid equality",
        "SELECT id FROM a WHERE u = UUID '00000000-0000-0000-0000-000000000005'",
    )
    .await;
}
