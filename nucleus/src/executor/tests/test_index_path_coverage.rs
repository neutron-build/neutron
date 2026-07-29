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

/// Column values the engine materialized, and rows returned.
async fn value_cost(ex: &Executor, sql: &str) -> (u64, usize) {
    let before = ex.metrics().values_scanned.get();
    let result = exec(ex, sql).await;
    (
        ex.metrics().values_scanned.get() - before,
        rows(&result[0]).len(),
    )
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

/// Point lookups on every indexed column type.
///
/// `n` (BIGINT) and `s` (TEXT) used to full-scan here while UUID / DATE /
/// TIMESTAMP did not — not because of the types, but because the executor
/// preferred a linear `fast_scan_where_eq` whenever the planner's row estimate
/// exceeded an absolute threshold, and that estimate is `row_count * 10%` for
/// any column ANALYZE has not measured. The types that appeared to work were
/// simply the ones whose engine fast-scan is unimplemented.
#[tokio::test]
async fn test_point_lookups_reach_the_index() {
    let ex = indexed_table().await;

    for (label, expected, sql) in [
        (
            "bigint equality",
            1,
            "SELECT id FROM a WHERE n = 1000040",
        ),
        ("text equality", 1, "SELECT id FROM a WHERE s = 'k00042'"),
        (
            "timestamp equality",
            1,
            "SELECT id FROM a WHERE ts = TIMESTAMP '2026-07-01 00:00:07'",
        ),
        (
            "date equality",
            1,
            "SELECT id FROM a WHERE d = DATE '2026-01-10'",
        ),
        (
            "uuid equality",
            1,
            "SELECT id FROM a WHERE u = UUID '00000000-0000-0000-0000-000000000005'",
        ),
    ] {
        assert_indexed(&ex, expected, label, sql).await;
    }
}

/// A ten-column table: wide enough that reading all of it is visibly wrong.
/// `a` counts up, `b` cycles 0..9 so a filter on it keeps 90% of the rows.
async fn wide_table() -> Executor {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE wide (id INT PRIMARY KEY, a INT, b INT, c TEXT, d TEXT, \
         e TEXT, f TEXT, g TEXT, h TEXT, payload TEXT)",
    )
    .await;
    for i in 0..ROWS {
        exec(
            &ex,
            &format!(
                "INSERT INTO wide VALUES ({i}, {i}, {}, 'c{i}', 'd{i}', 'e{i}', \
                 'f{i}', 'g{i}', 'h{i}', '{}')",
                i % 10,
                "x".repeat(64)
            ),
        )
        .await;
    }
    ex
}

/// The queries whose scans get narrowed, and how many of the ten columns each
/// one genuinely needs — output columns plus filter columns.
const NARROWED: [(&str, usize, &str); 5] = [
    ("bare projection", 1, "SELECT a FROM wide"),
    (
        "projection under a filter",
        2,
        "SELECT a FROM wide WHERE b > 0",
    ),
    (
        "aggregate under a filter",
        2,
        "SELECT AVG(a) FROM wide WHERE b > 0",
    ),
    (
        "grouped aggregate",
        2,
        "SELECT b, COUNT(a) FROM wide GROUP BY b",
    ),
    (
        "two columns out, a third filtered on",
        3,
        "SELECT a, c FROM wide WHERE b > 0",
    ),
];

/// Narrowing a scan must not change a single answer.
///
/// `plan_execution = off` routes the same SQL down the AST path, which never
/// projects — so it is an oracle for exactly the thing that could go wrong
/// here: a projection that drops a column the query still reads. The failure
/// mode this guards against is silent (a filter binding to the wrong column
/// yields plausible rows), so equality against an unprojected run is the
/// assertion that catches it.
#[tokio::test]
async fn test_narrowed_scans_return_what_full_scans_return() {
    let ex = wide_table().await;

    for (label, _, sql) in NARROWED {
        let mut projected = rows(&exec(&ex, sql).await[0]).clone();

        exec(&ex, "SET plan_execution = off").await;
        // The result cache keys on the SQL text, so without this the second run
        // is served the first run's answer and the comparison is between a
        // result and itself. A deliberately broken projection passed this test
        // until the cache was cleared here.
        ex.clear_all_query_caches();
        let mut unprojected = rows(&exec(&ex, sql).await[0]).clone();
        exec(&ex, "SET plan_execution = on").await;
        ex.clear_all_query_caches();

        // Compared as multisets: none of these queries has an ORDER BY, and
        // the two paths group by different mechanisms, so row order genuinely
        // differs. Content is the thing a dropped column would change.
        projected.sort();
        unprojected.sort();
        assert_eq!(
            projected, unprojected,
            "{label}: narrowing the scan changed the answer — {sql}"
        );
    }
}

/// A scan must read the columns the query touches and no others.
///
/// `scan_projected` shipped, was correct, and was called **zero times** across
/// an entire benchmark: the pushdown demanded that the scan sit directly under
/// the `Project` node and carry no filter, which between them excludes every
/// aggregate and every `WHERE`. Answers were identical with and without it, so
/// nothing but a cost assertion could see that the optimisation was dead.
///
/// Widths below are the columns each query genuinely needs — output columns
/// plus filter columns. The ceiling is half the table width, so this fires on
/// "read everything" rather than on an extra column.
#[tokio::test]
async fn test_scans_read_only_the_columns_the_query_touches() {
    const WIDTH: usize = 10;
    let ex = wide_table().await;

    // Control: a wildcard has to read the whole row, and does.
    let (full, _) = value_cost(&ex, "SELECT * FROM wide WHERE b > 0").await;
    assert!(
        full >= ROWS as u64 * WIDTH as u64,
        "SELECT * should read every column of every row, read {full}"
    );

    for (label, needs, sql) in NARROWED {
        let (values, _) = value_cost(&ex, sql).await;
        assert!(
            values <= ROWS as u64 * (WIDTH as u64 / 2),
            "{label}: needs {needs} of {WIDTH} columns but materialized {values} values \
             over {ROWS} rows — the scan is no longer projected. {sql}"
        );
    }
}

/// The sequential path still exists for a genuinely unselective equality, and
/// must return the same rows the index would. This asserts correctness rather
/// than which path ran — the point is that changing the crossover rule did not
/// change any answer.
#[tokio::test]
async fn test_unselective_equality_still_correct() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lowcard (id INT PRIMARY KEY, flag INT)").await;
    exec(&ex, "CREATE INDEX lowcard_flag ON lowcard (flag)").await;
    for i in 0..ROWS {
        exec(&ex, &format!("INSERT INTO lowcard VALUES ({i}, {})", i % 2)).await;
    }
    exec(&ex, "ANALYZE lowcard").await;

    let (_, matched) = scan_cost(&ex, "SELECT id FROM lowcard WHERE flag = 0").await;
    assert_eq!(
        matched,
        ROWS as usize / 2,
        "half the table matches flag = 0"
    );
    let (_, none) = scan_cost(&ex, "SELECT id FROM lowcard WHERE flag = 9").await;
    assert_eq!(none, 0, "no row has flag = 9");
}

/// An index that stops being used must say so.
///
/// `if let Ok(Some(rows)) = …index_lookup_range(…)` treated "this table has no
/// index", "this index cannot serve this predicate" and "this index is unusable
/// inside a transaction" as the same silent miss. In an engine whose
/// characteristic failure is a query that quietly stops using its index while
/// still returning the right answer, that is the one thing an operator needs to
/// be able to see.
#[tokio::test]
async fn test_index_scan_fallbacks_are_counted() {
    let ex = indexed_table().await;

    let counts = |ex: &Executor| {
        (
            ex.metrics().index_scan_attempts.get(),
            ex.metrics().index_scan_served.get(),
            ex.metrics().index_scan_fallbacks.get(),
        )
    };

    // A point lookup the index can serve.
    let (a0, s0, f0) = counts(&ex);
    exec(&ex, "SELECT id FROM a WHERE n = 1000040").await;
    let (a1, s1, f1) = counts(&ex);
    assert!(a1 > a0, "an indexed lookup must count as an attempt");
    assert!(s1 > s0, "the index served it, so `served` must move");
    assert_eq!(f1, f0, "nothing fell back");

    // Same predicate inside a transaction: the committed index cannot see the
    // transaction's own writes, so it must decline — visibly.
    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT id FROM a WHERE n = 1000041").await;
    exec(&ex, "COMMIT").await;
    let (_, s2, f2) = counts(&ex);
    assert_eq!(s2, s1, "no index served the in-transaction lookup");
    assert!(
        f2 > f1,
        "declining inside a transaction must be counted, not silent"
    );
}

/// A plan that fails for a real reason must not be retried on the AST path.
///
/// The fallback was `if let Ok(..) = execute_plan_node(..)`, which swallowed
/// every error kind: a budget refusal, a storage error and "this plan shape is
/// not implemented" were one branch. The first two then re-ran the whole query
/// down a second path, so the reported outcome was whatever the retry happened
/// to produce rather than the failure that actually occurred.
///
/// The assertion is on which counter moves, not only on the error: the AST path
/// has its own memory gate and refuses too, so an error alone cannot tell a
/// propagated refusal from a re-executed one.
#[tokio::test]
async fn test_a_refused_plan_is_not_silently_re_executed() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE big (id INT PRIMARY KEY, payload TEXT)").await;
    for i in 0..200 {
        exec(
            &ex,
            &format!("INSERT INTO big VALUES ({i}, '{}')", "x".repeat(512)),
        )
        .await;
    }
    // Budget far below the ~100 KB working set, so the refusal does not depend
    // on exact per-row accounting.
    ex.set_query_memory_limit(4 * 1024);
    let fallbacks_before = ex.metrics().plan_path_fallbacks.get();
    let errors_before = ex.metrics().plan_path_errors.get();

    let err = ex
        .execute("SELECT id, payload FROM big")
        .await
        .expect_err("a scan over the budget must fail, not fall back and succeed");
    assert!(
        matches!(err, ExecError::MemoryExceeded(_)),
        "expected the budget refusal to surface, got {err:?}"
    );
    assert!(
        ex.metrics().plan_path_errors.get() > errors_before,
        "the refusal must be recorded as a plan-path error"
    );
    assert_eq!(
        ex.metrics().plan_path_fallbacks.get(),
        fallbacks_before,
        "a budget refusal is not a 'this plan cannot run' fallback — counting it \
         as one is exactly the retry this guards against"
    );
}
