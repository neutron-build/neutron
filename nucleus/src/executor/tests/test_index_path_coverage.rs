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
//!
//! The 14 data models reduce to five physical access paths
//! (`ENGINE_PERFORMANCE_PROGRAM.md` §3). One test per path fails if that path
//! is bypassed:
//!
//! | access path | test |
//! |---|---|
//! | range scan | `test_range_windows_reach_the_index` |
//! | point lookup | `test_point_lookups_reach_the_index` |
//! | ordered scan | `test_ordered_scan_prunes_to_the_sort_key` |
//! | inverted lookup | `test_inverted_lookup_narrows_the_scan` |
//! | similarity search | `test_similarity_search_reaches_the_vector_index` |
//!
//! Two of those paths reach their index and still read the whole table — see
//! the characterization section near the bottom of this file, which pins the
//! gap and names the change that should break it.

use super::*;

const ROWS: i64 = 1_000;

/// One row per distinct value on every indexed column, so a point lookup
/// matches exactly one row and a five-wide window matches five.
async fn indexed_table() -> Executor {
    let ex = test_executor();
    seed_indexed(&ex).await;
    ex
}

/// The same fixture against a caller-supplied executor, so the identical cost
/// claims can be re-asserted on the engine `main.rs` builds.
async fn seed_indexed(ex: &Executor) {
    exec(
        ex,
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
        exec(ex, &format!("CREATE INDEX {name} ON a ({col})")).await;
    }
    for i in 0..ROWS {
        exec(
            ex,
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
        ("bigint equality", 1, "SELECT id FROM a WHERE n = 1000040"),
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

/// A key-set predicate must use the index it has.
///
/// `col IN (a, b, c)` is a disjunction of equalities, which no single range
/// covers, so it fell through to a full scan even with an index on the column —
/// leaving the ORM batch-fetch shape (`findMany`, `WHERE id = ANY($1)`) as the
/// one everyday query guaranteed never to use its index.
#[tokio::test]
async fn test_key_set_predicates_reach_the_index() {
    let ex = indexed_table().await;

    for (label, expected, sql) in [
        (
            "single-element IN",
            1,
            "SELECT id FROM a WHERE n IN (1000040)",
        ),
        (
            "multi-element IN",
            3,
            "SELECT id FROM a WHERE n IN (1000040, 1000041, 1000042)",
        ),
        (
            "text IN",
            2,
            "SELECT id FROM a WHERE s IN ('k00042', 'k00043')",
        ),
        (
            "IN with duplicates",
            2,
            "SELECT id FROM a WHERE n IN (1000050, 1000050, 1000051)",
        ),
        (
            "text-spelled elements against a BIGINT column",
            2,
            "SELECT id FROM a WHERE n IN ('1000060', '1000061')",
        ),
        (
            "equality disjunction, the same key set spelled with OR",
            2,
            "SELECT id FROM a WHERE n = 1000070 OR n = 1000071",
        ),
        (
            "IN as one conjunct of a larger predicate",
            1,
            "SELECT id FROM a WHERE n IN (1000080, 1000081) AND s = 'k00080'",
        ),
    ] {
        assert_indexed(&ex, expected, label, sql).await;
    }
}

/// The probe is only valid where the key set is a necessary condition. These
/// shapes must keep their answers even though the index cannot serve them.
///
/// Under `OR` a row can satisfy the predicate without matching any probed key,
/// so probing would silently drop it — the failure would be missing rows, not a
/// slow query, which is why these are asserted on results rather than on cost.
#[tokio::test]
async fn test_key_set_probe_does_not_change_answers() {
    let ex = indexed_table().await;

    for (label, expected, sql) in [
        (
            // i=90, i=91 by n; i=95 by s.
            "IN under OR with another column",
            3,
            "SELECT id FROM a WHERE n IN (1000090, 1000091) OR s = 'k00095'",
        ),
        (
            "NOT IN",
            ROWS as usize - 2,
            "SELECT id FROM a WHERE n NOT IN (1000100, 1000101)",
        ),
        (
            "disjunction over two different columns",
            2,
            "SELECT id FROM a WHERE n = 1000110 OR s = 'k00111'",
        ),
        (
            "IN with no matching key",
            0,
            "SELECT id FROM a WHERE n IN (7, 8, 9)",
        ),
        (
            "empty-ish IN with one miss",
            0,
            "SELECT id FROM a WHERE n IN (0)",
        ),
    ] {
        ex.clear_all_query_caches();
        let (_, matched) = scan_cost(&ex, sql).await;
        assert_eq!(matched, expected, "{label}: wrong row count — {sql}");
    }
}

/// Zone maps may only prune a complete scan, in scan order.
///
/// Granule statistics are positional: granule `i` describes the rows at offset
/// `i * GRANULE_SIZE` of a full scan. Handed any other row set — index-probe
/// hits, a LIMIT-truncated scan — granule `i`'s min/max is charged against
/// whatever rows happen to sit at that offset, and rows that match get dropped.
///
/// The existing safety net compares granule row counts against the row count
/// being filtered, which reads as a guard and is not one: equal counts are not
/// evidence that these are the same rows. A single index hit was pruned by a
/// single-granule zone map and `WHERE id IN (6)` returned nothing for a row
/// that existed, while `WHERE id = 6` on the same row returned it.
#[tokio::test]
async fn test_index_hits_are_not_pruned_by_positional_zone_maps() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE z (id INTEGER PRIMARY KEY, c1 INTEGER NOT NULL, tag TEXT NOT NULL)",
    )
    .await;
    let vals: Vec<String> = (1..=29)
        .map(|i| format!("({i},{i},'{}')", if i % 3 == 0 { "keep" } else { "move" }))
        .collect();
    exec(&ex, &format!("INSERT INTO z VALUES {}", vals.join(","))).await;
    // An UPDATE that rewrites most of the table, then an INSERT — the shape
    // that leaves the zone map describing a different row set than the index.
    exec(&ex, "UPDATE z SET c1 = 1 WHERE tag <> 'keep'").await;
    exec(&ex, "INSERT INTO z VALUES (30, -5, 'new')").await;

    for (label, sql, expected) in [
        ("equality", "SELECT id FROM z WHERE id = 6", vec![6]),
        ("key set", "SELECT id FROM z WHERE id IN (6)", vec![6]),
        (
            "multi key set",
            "SELECT id FROM z WHERE id IN (6, 30)",
            vec![6, 30],
        ),
        (
            "key set with a residual predicate",
            "SELECT id FROM z WHERE id IN (6) AND tag = 'keep'",
            vec![6],
        ),
    ] {
        ex.clear_all_query_caches();
        let got: Vec<i64> = rows(&exec(&ex, sql).await[0])
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Int32(n)) => Some(i64::from(*n)),
                Some(Value::Int64(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(got, expected, "{label}: {sql}");
    }
}

// ============================================================================
// The five access paths
//
// `ENGINE_PERFORMANCE_PROGRAM.md` §3: all 14 data models reduce to five
// physical access patterns — ordered scan, point lookup, similarity search,
// inverted lookup, range scan. Point lookup and range scan are asserted above.
// The other three had no cost assertion anywhere in the tree, and writing these
// found two of the three doing no I/O narrowing at all.
//
// Every test here pairs the narrowed path with a control that bypasses it, in
// the same run: an absolute bound alone cannot tell "the index worked" from
// "the counter stopped moving", and a guard that has never been observed to
// fail is not a guard.
// ============================================================================

const ORDERED_ROWS: i64 = 2_000;
const TS_BASE: i64 = 1_700_000_000;

/// Time-ordered rows on a caller-chosen engine — the shape a dashboard queries.
async fn ordered_table(ddl: &str) -> Executor {
    let ex = test_executor();
    exec(&ex, ddl).await;
    for i in 0..ORDERED_ROWS {
        exec(
            &ex,
            &format!("INSERT INTO m VALUES ({i}, {}, {})", TS_BASE + i, i % 7),
        )
        .await;
    }
    ex
}

const HEAP_DDL: &str = "CREATE TABLE m (id INT, ts BIGINT, val INT)";
const COLUMNAR_DDL: &str = "CREATE TABLE m (id INT, ts BIGINT, val INT) WITH (engine='columnar')";
const MERGETREE_DDL: &str = "CREATE TABLE m (id INT, ts BIGINT, val INT) \
                             WITH (engine='mergetree') ORDER BY (ts)";

/// **Ordered scan.** A declared sort key must prune the scan to the part that
/// can hold the window, and a window outside every part must read nothing.
///
/// This is the single most-repeated entry in §2's declared-vs-reality table:
/// the `ORDER BY` was parsed, persisted to `engines.json`, restored at boot,
/// registered — and landed on the executor's columnar store while columnar
/// tables are served by a per-table engine, so `MergeTree::scan` had zero
/// callers and the sort key was inert. Answers were identical throughout.
///
/// Heap and columnar are the controls. They read every row for the same query,
/// which is what proves the counter is measuring pruning rather than reporting
/// a matched count.
#[tokio::test]
async fn test_ordered_scan_prunes_to_the_sort_key() {
    let narrow = format!(
        "SELECT COUNT(*) FROM m WHERE ts >= {} AND ts < {}",
        TS_BASE + 100,
        TS_BASE + 105
    );
    // Entirely past the last row: a sort key makes this free, a scan does not.
    let excluded = format!(
        "SELECT COUNT(*) FROM m WHERE ts >= {} AND ts < {}",
        TS_BASE + ORDERED_ROWS + 1_000,
        TS_BASE + ORDERED_ROWS + 2_000
    );

    for (label, ddl) in [("heap", HEAP_DDL), ("columnar", COLUMNAR_DDL)] {
        let ex = ordered_table(ddl).await;
        for (what, sql) in [("narrow", &narrow), ("range-excluded", &excluded)] {
            ex.clear_all_query_caches();
            let (scanned, _) = scan_cost(&ex, sql).await;
            assert!(
                scanned >= ORDERED_ROWS as u64,
                "{label}/{what}: expected a full scan as the control, scanned {scanned} \
                 of {ORDERED_ROWS} — if this engine gained pruning, this test's \
                 control is no longer a control and the bounds below must be re-derived"
            );
        }
    }

    let ex = ordered_table(MERGETREE_DDL).await;

    ex.clear_all_query_caches();
    let (scanned, _) = scan_cost(&ex, &narrow).await;
    assert!(
        scanned < ORDERED_ROWS as u64 / 10,
        "a five-wide window on the declared sort key scanned {scanned} of \
         {ORDERED_ROWS} — the sort key is inert again"
    );

    ex.clear_all_query_caches();
    let (scanned, _) = scan_cost(&ex, &excluded).await;
    assert_eq!(
        scanned, 0,
        "a window past every part must read nothing; scanned {scanned}"
    );
}

/// Pruning that drops a matching row is far worse than pruning that never
/// happens, and `ZoneMap::can_skip` was sound only for matching scalar types:
/// `scalar_lt` returns false for every mixed pair, so the `Gt`/`Lt` arms
/// evaluated to `!false` and would have skipped **every** part. It was
/// unreachable only because nothing called the pruning scan — which is no
/// longer true, so the bound is asserted on answers, not on cost.
#[tokio::test]
async fn test_sort_key_pruning_never_drops_a_row() {
    let ex = ordered_table(MERGETREE_DDL).await;

    for (label, expected, sql) in [
        (
            "same-type window",
            5,
            format!(
                "SELECT COUNT(*) FROM m WHERE ts >= {} AND ts < {}",
                TS_BASE + 100,
                TS_BASE + 105
            ),
        ),
        (
            "text-spelled bounds against a BIGINT sort key",
            5,
            format!(
                "SELECT COUNT(*) FROM m WHERE ts >= '{}' AND ts < '{}'",
                TS_BASE + 100,
                TS_BASE + 105
            ),
        ),
        (
            "float bounds against a BIGINT sort key",
            5,
            format!(
                "SELECT COUNT(*) FROM m WHERE ts >= {}.0 AND ts < {}.0",
                TS_BASE + 100,
                TS_BASE + 105
            ),
        ),
        (
            "predicate on a column that is not the sort key",
            (ORDERED_ROWS as usize).div_ceil(7),
            "SELECT COUNT(*) FROM m WHERE val = 0".to_string(),
        ),
        (
            "no predicate at all",
            ORDERED_ROWS as usize,
            "SELECT COUNT(*) FROM m".to_string(),
        ),
    ] {
        ex.clear_all_query_caches();
        let result = exec(&ex, &sql).await;
        let got = match scalar(&result[0]) {
            Value::Int64(n) => *n as usize,
            Value::Int32(n) => *n as usize,
            other => panic!("{label}: non-integer COUNT — {other:?}"),
        };
        assert_eq!(got, expected, "{label}: pruning changed the answer — {sql}");
    }
}

/// **Inverted lookup.** A term the index can serve must turn into point
/// lookups, and a term it cannot must decline *before* building the hit set.
///
/// Both halves were defects: the index once scanned the whole table and
/// filtered by candidate membership, so it only ever saved the `@@` recheck;
/// and the crossover was decided after scoring, which left a broad term
/// measurably slower through the index than without it.
#[tokio::test]
async fn test_inverted_lookup_narrows_the_scan() {
    const DOCS: i64 = 1_000;
    const RARE: usize = 10;

    async fn corpus(indexed: bool) -> Executor {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").await;
        for i in 0..DOCS {
            let body = if i % 100 == 7 {
                format!("needle unique{i} filler text")
            } else {
                format!("common filler text number {i}")
            };
            exec(&ex, &format!("INSERT INTO docs VALUES ({i}, '{body}')")).await;
        }
        if indexed {
            exec(&ex, "CREATE INDEX ON docs USING FTS (body)").await;
        }
        ex
    }

    // Control: without the index every term costs a full tokenizing scan.
    let plain = corpus(false).await;
    for term in ["needle", "zzzabsent"] {
        plain.clear_all_query_caches();
        let (scanned, _) = scan_cost(
            &plain,
            &format!("SELECT id FROM docs WHERE body @@ '{term}'"),
        )
        .await;
        assert_eq!(
            scanned, DOCS as u64,
            "control: '{term}' without an index must read every row, read {scanned}"
        );
    }

    let ex = corpus(true).await;

    ex.clear_all_query_caches();
    let (scanned, matched) = scan_cost(&ex, "SELECT id FROM docs WHERE body @@ 'needle'").await;
    assert_eq!(matched, RARE, "the corpus holds {RARE} needles");
    assert!(
        scanned <= RARE as u64,
        "a rare term must cost one point lookup per hit; scanned {scanned} for \
         {matched} hits — the index is proposing candidates and then scanning"
    );

    ex.clear_all_query_caches();
    let (scanned, matched) = scan_cost(&ex, "SELECT id FROM docs WHERE body @@ 'zzzabsent'").await;
    assert_eq!(matched, 0, "no document holds that term");
    assert_eq!(
        scanned, 0,
        "an empty posting list must read nothing at all; scanned {scanned}"
    );

    // A term in every document: the index must decline, and must say so.
    let fallbacks = ex.metrics().index_scan_fallbacks.get();
    ex.clear_all_query_caches();
    let (scanned, matched) = scan_cost(&ex, "SELECT id FROM docs WHERE body @@ 'filler'").await;
    assert_eq!(matched, DOCS as usize, "every document holds 'filler'");
    assert_eq!(
        scanned, DOCS as u64,
        "a term matching everything is cheaper scanned; scanned {scanned}"
    );
    assert!(
        ex.metrics().index_scan_fallbacks.get() > fallbacks,
        "declining must be counted — an unrecorded decline is how a broad query \
         that ran slower THROUGH the index stayed invisible"
    );
}

const VEC_DIMS: usize = 8;
const VEC_ROWS: i64 = 400;

fn vec_literal(i: i64) -> String {
    let dims: Vec<String> = (0..VEC_DIMS)
        .map(|k| format!("{}", i as f32 + k as f32))
        .collect();
    format!("VECTOR('[{}]')", dims.join(","))
}

async fn vector_table(indexed: bool) -> Executor {
    let ex = test_executor();
    exec(
        &ex,
        &format!("CREATE TABLE v (id INT PRIMARY KEY, e VECTOR({VEC_DIMS}))"),
    )
    .await;
    for i in 0..VEC_ROWS {
        exec(
            &ex,
            &format!("INSERT INTO v VALUES ({i}, {})", vec_literal(i)),
        )
        .await;
    }
    if indexed {
        exec(&ex, "CREATE INDEX v_e ON v USING HNSW (e)").await;
    }
    ex
}

/// **Similarity search.** `ORDER BY VECTOR_DISTANCE(col, q) LIMIT k` must reach
/// the vector index, and must be observable when it does not.
///
/// Nothing could see this path before: no counter moved either way, and the
/// answers are identical because HNSW returns the same top-k as an exact sort
/// at these sizes — so a query that silently fell back to sorting the whole
/// table by brute force was indistinguishable from one the index served. The
/// assertion is therefore on which counter moves, with the unindexed table as
/// the control that proves the counter can tell them apart.
#[tokio::test]
async fn test_similarity_search_reaches_the_vector_index() {
    const PROBE: i64 = 137;
    let sql = format!(
        "SELECT id FROM v ORDER BY VECTOR_DISTANCE(e, {}) LIMIT 5",
        vec_literal(PROBE)
    );

    let counts = |ex: &Executor| {
        (
            ex.metrics().index_scan_served.get(),
            ex.metrics().index_scan_fallbacks.get(),
        )
    };

    // Control: no index, so the ordering is brute force — and that must be
    // counted as a fallback rather than passing for a served lookup.
    let plain = vector_table(false).await;
    let (s0, f0) = counts(&plain);
    let rows_out = rows(&exec(&plain, &sql).await[0]).clone();
    let (s1, f1) = counts(&plain);
    assert_eq!(s1, s0, "there is no index, so nothing can have served it");
    assert!(
        f1 > f0,
        "a similarity query with no index must count as a fallback"
    );
    assert_eq!(rows_out.len(), 5, "LIMIT 5 must return five rows");

    let ex = vector_table(true).await;
    let (s0, f0) = counts(&ex);
    ex.clear_all_query_caches();
    let served_rows = rows(&exec(&ex, &sql).await[0]).clone();
    let (s1, f1) = counts(&ex);
    assert!(
        s1 > s0,
        "the vector index must serve an indexed similarity query"
    );
    assert_eq!(f1, f0, "nothing fell back");

    // The probe is one of the stored vectors, so the nearest neighbour is that
    // row at distance zero. Exact for any ordering that is not broken, which
    // keeps this assertion off HNSW's approximate tail.
    let first = match served_rows.first().and_then(|r| r.first()) {
        Some(Value::Int32(n)) => i64::from(*n),
        Some(Value::Int64(n)) => *n,
        other => panic!("unexpected id column: {other:?}"),
    };
    assert_eq!(
        first, PROBE,
        "the exact-match row must rank first; got {first}"
    );
}

// ============================================================================
// CHARACTERIZATION — these pin behaviour that is KNOWN WRONG.
//
// They exist so the gap cannot be forgotten, and they are expected to FAIL when
// it is closed. A test asserting known-wrong behaviour is otherwise the exact
// anti-pattern that has cost this repo four bugs — a suite that guards the
// defect — so each one says which change should break it.
// ============================================================================

/// **Similarity search does not narrow the scan.** `try_vector_index_scan`
/// receives rows that a full table scan has already materialized and uses the
/// HNSW graph to reorder them, so the index saves the distance computation and
/// the sort — never the I/O. Vector search is O(table) in rows read no matter
/// how selective the query, which is the whole reason an ANN index exists.
///
/// The post-scan shape is correctness-motivated, not an oversight: the scanned
/// rows are the post-WHERE, post-RLS valid set handed to `search_filtered`, so
/// a predicate cannot be violated by the index. What is missing is the fast
/// path for the unfiltered case, where the valid set is the whole table and the
/// pre-filter buys nothing — the FTS path is the template (search, then a point
/// lookup per candidate PK).
///
/// **Delete this test when that path lands.** It should fail first.
#[tokio::test]
async fn test_similarity_search_still_reads_every_row() {
    let ex = vector_table(true).await;
    for probe in [10_i64, 200, 390] {
        let sql = format!(
            "SELECT id FROM v ORDER BY VECTOR_DISTANCE(e, {}) LIMIT 5",
            vec_literal(probe)
        );
        ex.clear_all_query_caches();
        let (scanned, matched) = scan_cost(&ex, &sql).await;
        assert_eq!(matched, 5, "LIMIT 5");
        assert_eq!(
            scanned, VEC_ROWS as u64,
            "characterization: a top-5 similarity query still reads all \
             {VEC_ROWS} rows (scanned {scanned}). If this failed because the \
             number dropped, the index now narrows the scan — delete this test \
             and tighten `test_similarity_search_reaches_the_vector_index` to \
             assert the new bound."
        );
    }
}

/// **JSONB containment does not narrow the scan either.** `try_gin_index_scan`
/// builds a candidate set from the posting map and then scans the whole table,
/// keeping rows whose *position* is in the set — so the GIN index saves the
/// containment recheck and nothing else. Even a key present in no document
/// reads every row.
///
/// The blocker is structural rather than an omission: GIN candidates are
/// positional row ids from `enumerate()`, not stable keys, so they cannot be
/// turned into point lookups the way FTS candidates can — `stable_row_id` is
/// what makes the FTS path possible. Closing this needs GIN postings keyed on
/// the primary key, which is a format change.
///
/// **Delete this test when that lands.** It should fail first.
#[tokio::test]
async fn test_gin_containment_still_reads_every_row() {
    const EVENTS: i64 = 1_000;
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ev (id INT PRIMARY KEY, props JSONB)").await;
    for i in 0..EVENTS {
        let tag = if i % 100 == 7 { "rare" } else { "common" };
        exec(
            &ex,
            &format!("INSERT INTO ev VALUES ({i}, '{{\"tag\":\"{tag}\",\"i\":{i}}}')"),
        )
        .await;
    }
    exec(&ex, "CREATE INDEX ev_gin ON ev USING GIN (props)").await;

    for (label, expected, sql) in [
        (
            "a tag on 1% of rows",
            10,
            "SELECT id FROM ev WHERE props @> '{\"tag\":\"rare\"}'",
        ),
        (
            "a tag on no rows at all",
            0,
            "SELECT id FROM ev WHERE props @> '{\"tag\":\"nope\"}'",
        ),
    ] {
        ex.clear_all_query_caches();
        let (scanned, matched) = scan_cost(&ex, sql).await;
        assert_eq!(matched, expected, "{label}: wrong row count");
        assert_eq!(
            scanned, EVENTS as u64,
            "characterization: {label} still reads all {EVENTS} rows \
             (scanned {scanned}). If the number dropped, GIN now narrows the \
             scan — delete this test and assert the real bound."
        );
    }
}

// ============================================================================
// The engine the server actually constructs
// ============================================================================

/// Every cost assertion above runs on `MemoryEngine`. `main.rs` constructs
/// `BufferedDiskEngine::new(DiskEngine)`, and §2's third declared-vs-reality
/// row is exactly a wrapper that did not forward an override — `scan_projected`
/// shipped, was correct, and the trait default ran in the only configuration
/// that ships. A cost test on the wrong engine cannot see that class at all.
///
/// So the index-path claims are re-asserted here on the served engine. They
/// agree with the in-memory numbers today; the point is that a divergence
/// would now be visible.
///
/// Column pruning is deliberately **not** re-asserted here.
/// `integration_tests::test_projected_scan_through_the_production_engine`
/// already covers it and covers it better: `values_scanned` counts what the
/// executor asked for, so it cannot tell a forwarded `scan_projected` from a
/// wrapper that read every column and projected the result itself. That test
/// pairs it with `DiskEngine::projected_scan_count()`, which counts the work.
/// Repeating the weaker half here would read as coverage and prove less.
#[tokio::test]
async fn test_cost_claims_hold_on_the_engine_the_server_constructs() {
    use crate::storage::buffered_engine::BufferedDiskEngine;
    use crate::storage::disk_engine::DiskEngine;

    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.path().join("cost.db"), catalog.clone()).unwrap());
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    let ex = Executor::new(catalog, storage);

    seed_indexed(&ex).await;

    for (label, expected, sql) in [
        ("point lookup", 1, "SELECT id FROM a WHERE n = 1000040"),
        (
            "text point lookup",
            1,
            "SELECT id FROM a WHERE s = 'k00042'",
        ),
        (
            "range window",
            5,
            "SELECT id FROM a WHERE ts >= TIMESTAMP '2026-07-01 00:00:00' \
             AND ts < TIMESTAMP '2026-07-01 00:00:05'",
        ),
        (
            "key set",
            3,
            "SELECT id FROM a WHERE n IN (1000040, 1000041, 1000042)",
        ),
    ] {
        ex.clear_all_query_caches();
        assert_indexed(&ex, expected, label, sql).await;
    }

    // A full scan must still cost a full scan on this engine — otherwise the
    // bounds above are met by a counter that stopped moving.
    ex.clear_all_query_caches();
    let (scanned, matched) = scan_cost(&ex, "SELECT id FROM a WHERE s LIKE 'k%'").await;
    assert_eq!(matched, ROWS as usize, "every key starts with k");
    assert_eq!(
        scanned, ROWS as u64,
        "control: an unindexable predicate must read every row through \
         BufferedDiskEngine, read {scanned}"
    );
}

/// The analytics engines must narrow the columns a query asks for, not only
/// the rows.
///
/// `engine='columnar'` shipped with no `scan_projected` override, so it read
/// every column and discarded what the query did not want — and `batches_for_read`
/// existed to borrow rather than clone, with only the aggregate fast paths
/// calling it, so every ordinary scan copied the whole table first. That copy
/// *was* the cost of a columnar query.
///
/// What this measures is the **request**: `values_scanned` counts the values
/// the executor materialized, so it catches the pushdown being gated off (it
/// was, for every filtered query, and unreached for every aggregate) but not an
/// engine that reads wide and narrows on the way out. Only `DiskEngine` carries
/// a work-level counter — see `projected_scan_count` and
/// `integration_tests::test_projected_scan_through_the_production_engine`.
#[tokio::test]
async fn test_analytics_engines_narrow_columns() {
    const WIDTH: u64 = 10;

    for (label, engine_clause) in [
        ("columnar", "WITH (engine='columnar')"),
        ("mergetree", "WITH (engine='mergetree') ORDER BY (a)"),
    ] {
        let ex = test_executor();
        exec(
            &ex,
            &format!(
                "CREATE TABLE w (id INT PRIMARY KEY, a INT, b INT, c TEXT, d TEXT, \
                 e TEXT, f TEXT, g TEXT, h TEXT, payload TEXT) {engine_clause}"
            ),
        )
        .await;
        for i in 0..ROWS {
            exec(
                &ex,
                &format!(
                    "INSERT INTO w VALUES ({i}, {i}, {}, 'c{i}', 'd{i}', 'e{i}', \
                     'f{i}', 'g{i}', 'h{i}', '{}')",
                    i % 10,
                    "x".repeat(64)
                ),
            )
            .await;
        }

        // Control first: a wildcard has to materialize the whole row. Without
        // it the narrowed assertion below is satisfied by a counter that never
        // moves — which is how this engine reports an aggregate (zero values
        // materialized, because it answers inside the engine).
        ex.clear_all_query_caches();
        let (full, _) = value_cost(&ex, "SELECT * FROM w WHERE b > 0").await;
        assert!(
            full >= ROWS as u64 * WIDTH,
            "{label}: SELECT * must materialize every column, materialized {full}"
        );

        for (what, sql) in [
            ("one column out", "SELECT a FROM w"),
            (
                "aggregate under a filter",
                "SELECT AVG(a) FROM w WHERE b > 0",
            ),
        ] {
            ex.clear_all_query_caches();
            let (values, _) = value_cost(&ex, sql).await;
            assert!(
                values <= ROWS as u64 * WIDTH / 2,
                "{label}/{what}: materialized {values} values over {ROWS} rows of \
                 {WIDTH} columns — the scan is not narrowed. {sql}"
            );
        }
    }
}
