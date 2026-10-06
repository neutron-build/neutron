//! SQL cursors (DECLARE / FETCH / CLOSE): the materialization budgets, the
//! transaction lifetime, FETCH position semantics, and the lazy producer.
//!
//! A cursor is one of two things (see `execute_declare_cursor`). A query over
//! a bare `generate_series(...)` is lazy: DECLARE runs nothing, the cursor
//! holds a few integers, and each FETCH produces only the rows it returns.
//! Every other query is a materialized snapshot taken at DECLARE under the row
//! and byte budgets. The lazy tests assert the observable that separates the
//! two (`CursorDef::lazy` set, no rows held) together with behaviour that a
//! DECLARE-time evaluation would have broken: a budget far below the series
//! length, a row that errors only when it is reached, a range too long to
//! build. The rest pin what is PostgreSQL-faithful for both kinds: a budget
//! refusal stores nothing, a non-holdable cursor dies with its transaction or
//! with a rollback to an earlier savepoint, and every FETCH direction lands on
//! the position PostgreSQL would. Errors raised inside an explicit transaction
//! abort it (as in PostgreSQL), so each refusal case opens its own transaction
//! and ends it with ROLLBACK.

use super::*;
use crate::types::Value;
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

fn sqlstate_of(err: &ExecError) -> String {
    let codec = PgWireErrorCodec;
    codec.code_to_string(codec.encode(err).code)
}

/// A `cur_t(id INT, name TEXT)` table holding ids `1..=n`.
async fn seeded(n: i64) -> Executor {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cur_t (id INT, name TEXT)").await;
    for i in 1..=n {
        exec(&ex, &format!("INSERT INTO cur_t VALUES ({i}, 'row{i}')")).await;
    }
    ex
}

fn ids(result: &ExecResult) -> Vec<i64> {
    rows(result)
        .iter()
        .map(|row| match &row[0] {
            Value::Int32(n) => i64::from(*n),
            Value::Int64(n) => *n,
            other => panic!("expected an integer id, got {other:?}"),
        })
        .collect()
}

async fn fetched(ex: &Executor, sql: &str) -> Vec<i64> {
    let results = exec(ex, sql).await;
    ids(&results[0])
}

async fn position_of(ex: &Executor, name: &str) -> usize {
    ex.current_session()
        .cursors
        .read()
        .await
        .get(name)
        .expect("cursor exists")
        .position
}

async fn cursor_names(ex: &Executor) -> Vec<String> {
    let mut names: Vec<String> = ex
        .current_session()
        .cursors
        .read()
        .await
        .keys()
        .cloned()
        .collect();
    names.sort();
    names
}

const DECLARE_ALL: &str = "SELECT id FROM cur_t ORDER BY id";

/// A series short enough to count by hand and long enough to need a few FETCHes.
const SERIES_10: &str = "SELECT g FROM generate_series(1, 10) g";

fn int_col(result: &ExecResult, idx: usize) -> Vec<i64> {
    rows(result)
        .iter()
        .map(|row| match &row[idx] {
            Value::Int32(n) => i64::from(*n),
            Value::Int64(n) => *n,
            other => panic!("expected an integer, got {other:?}"),
        })
        .collect()
}

async fn is_lazy(ex: &Executor, name: &str) -> bool {
    ex.current_session()
        .cursors
        .read()
        .await
        .get(name)
        .expect("cursor exists")
        .lazy
        .is_some()
}

/// Rows the session holds for the cursor: always 0 for a lazy one.
async fn held_rows(ex: &Executor, name: &str) -> usize {
    ex.current_session()
        .cursors
        .read()
        .await
        .get(name)
        .expect("cursor exists")
        .rows
        .len()
}

// ── FETCH semantics ─────────────────────────────────────────────────────

/// Every direction moves from the position the previous one left, exactly as
/// PostgreSQL does, including the before-first and after-last positions.
#[tokio::test]
async fn fetch_directions_track_position_like_postgres() {
    let ex = seeded(5).await;
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE c SCROLL CURSOR FOR {DECLARE_ALL}")).await;

    // (statement, rows it returns, position afterwards)
    let steps: &[(&str, &[i64], usize)] = &[
        ("FETCH NEXT FROM c", &[1], 1),
        ("FETCH 2 FROM c", &[2, 3], 3),
        ("FETCH FORWARD 1 FROM c", &[4], 4),
        ("FETCH PRIOR FROM c", &[3], 3),
        ("FETCH BACKWARD 2 FROM c", &[2, 1], 1),
        ("FETCH PRIOR FROM c", &[], 0),
        ("FETCH NEXT FROM c", &[1], 1),
        ("FETCH LAST FROM c", &[5], 5),
        ("FETCH NEXT FROM c", &[], 6),
        ("FETCH PRIOR FROM c", &[5], 5),
        ("FETCH FIRST FROM c", &[1], 1),
        ("FETCH ABSOLUTE 4 FROM c", &[4], 4),
        ("FETCH RELATIVE 1 FROM c", &[5], 5),
        ("FETCH RELATIVE 0 FROM c", &[5], 5),
        ("FETCH RELATIVE 3 FROM c", &[], 6),
        ("FETCH ABSOLUTE 0 FROM c", &[], 0),
        ("FETCH ABSOLUTE 9 FROM c", &[], 6),
        ("FETCH BACKWARD ALL FROM c", &[5, 4, 3, 2, 1], 0),
        ("FETCH ALL FROM c", &[1, 2, 3, 4, 5], 6),
        ("FETCH ALL FROM c", &[], 6),
        ("FETCH 0 FROM c", &[], 6),
        ("FETCH ABSOLUTE 2 FROM c", &[2], 2),
        ("FETCH 0 FROM c", &[2], 2),
        ("FETCH FORWARD ALL FROM c", &[3, 4, 5], 6),
    ];
    for (sql, expected, position) in steps {
        assert_eq!(&fetched(&ex, sql).await, expected, "rows of: {sql}");
        assert_eq!(
            position_of(&ex, "c").await,
            *position,
            "position after: {sql}"
        );
    }
    exec(&ex, "COMMIT").await;
}

/// A count past the end returns what is left and lands after the last row.
#[tokio::test]
async fn fetch_count_past_the_end_returns_the_remainder() {
    let ex = seeded(5).await;
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE c CURSOR FOR {DECLARE_ALL}")).await;
    assert_eq!(fetched(&ex, "FETCH 2 FROM c").await, vec![1, 2]);
    assert_eq!(fetched(&ex, "FETCH 10 FROM c").await, vec![3, 4, 5]);
    assert_eq!(position_of(&ex, "c").await, 6);
    assert!(fetched(&ex, "FETCH 10 FROM c").await.is_empty());
    exec(&ex, "COMMIT").await;
}

/// NO SCROLL refuses every fetch that does not move strictly forward with
/// 55000, leaves the position alone, and still allows every forward form.
#[tokio::test]
async fn no_scroll_cursor_refuses_backward_fetches() {
    let backward = [
        "FETCH PRIOR FROM c",
        "FETCH BACKWARD 1 FROM c",
        "FETCH BACKWARD ALL FROM c",
        "FETCH FIRST FROM c",
        "FETCH LAST FROM c",
        "FETCH ABSOLUTE 1 FROM c",
    ];
    for sql in backward {
        let ex = seeded(5).await;
        exec(&ex, "BEGIN").await;
        exec(
            &ex,
            &format!("DECLARE c NO SCROLL CURSOR FOR {DECLARE_ALL}"),
        )
        .await;
        assert_eq!(fetched(&ex, "FETCH 2 FROM c").await, vec![1, 2]);
        let err = ex
            .execute(sql)
            .await
            .expect_err("a non-forward fetch on NO SCROLL must be refused");
        assert!(
            err.to_string().contains("cursor can only scan forward"),
            "{sql}: {err}"
        );
        assert_eq!(sqlstate_of(&err), "55000", "{sql}");
        assert_eq!(position_of(&ex, "c").await, 2, "{sql} moved the cursor");
        exec(&ex, "ROLLBACK").await;
    }

    // Control: the forward forms all work on the same kind of cursor.
    let ex = seeded(5).await;
    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        &format!("DECLARE c NO SCROLL CURSOR FOR {DECLARE_ALL}"),
    )
    .await;
    assert_eq!(fetched(&ex, "FETCH NEXT FROM c").await, vec![1]);
    assert_eq!(fetched(&ex, "FETCH FORWARD 1 FROM c").await, vec![2]);
    assert_eq!(fetched(&ex, "FETCH 0 FROM c").await, vec![2]);
    assert_eq!(fetched(&ex, "FETCH RELATIVE 1 FROM c").await, vec![3]);
    assert_eq!(fetched(&ex, "FETCH ABSOLUTE 5 FROM c").await, vec![5]);
    assert!(fetched(&ex, "FETCH ALL FROM c").await.is_empty());
    exec(&ex, "COMMIT").await;

    // Control: a cursor that did not say NO SCROLL may move backward.
    let ex = seeded(5).await;
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE c CURSOR FOR {DECLARE_ALL}")).await;
    assert_eq!(fetched(&ex, "FETCH 3 FROM c").await, vec![1, 2, 3]);
    assert_eq!(fetched(&ex, "FETCH PRIOR FROM c").await, vec![2]);
    exec(&ex, "COMMIT").await;
}

/// A count that is not an integer used to become 1 silently; it is now an
/// error (22023), and so is a cursor that does not exist (34000) for FETCH
/// and CLOSE alike.
#[tokio::test]
async fn bad_counts_and_missing_cursors_are_errors() {
    let bad_counts = [
        "FETCH 1.5 FROM c",
        "FETCH 99999999999999999999 FROM c",
        "FETCH FORWARD 1.5 FROM c",
        "FETCH ABSOLUTE 1.5 FROM c",
    ];
    for sql in bad_counts {
        let ex = seeded(3).await;
        exec(&ex, "BEGIN").await;
        exec(&ex, &format!("DECLARE c CURSOR FOR {DECLARE_ALL}")).await;
        let err = ex.execute(sql).await.expect_err("a bad count must fail");
        assert!(
            err.to_string().contains("invalid FETCH count"),
            "{sql}: {err}"
        );
        assert_eq!(sqlstate_of(&err), "22023", "{sql}");
        assert_eq!(position_of(&ex, "c").await, 0, "{sql} moved the cursor");
        exec(&ex, "ROLLBACK").await;
    }

    for sql in ["FETCH NEXT FROM nope", "CLOSE nope"] {
        let ex = seeded(1).await;
        exec(&ex, "BEGIN").await;
        let err = ex
            .execute(sql)
            .await
            .expect_err("an unknown cursor must fail");
        assert!(
            err.to_string().contains("cursor \"nope\" does not exist"),
            "{sql}: {err}"
        );
        assert_eq!(sqlstate_of(&err), "34000", "{sql}");
        exec(&ex, "ROLLBACK").await;
    }
}

/// BINARY would silently return text rows, so it is refused (0A000).
#[tokio::test]
async fn binary_cursors_are_refused() {
    let ex = seeded(1).await;
    exec(&ex, "BEGIN").await;
    let err = ex
        .execute(&format!("DECLARE c BINARY CURSOR FOR {DECLARE_ALL}"))
        .await
        .expect_err("BINARY cursors are not supported");
    assert_eq!(sqlstate_of(&err), "0A000");
    assert!(cursor_names(&ex).await.is_empty());
    exec(&ex, "ROLLBACK").await;
}

// ── Lifetime ────────────────────────────────────────────────────────────

/// Outside a transaction block only WITH HOLD is accepted (25P01 otherwise).
#[tokio::test]
async fn declare_outside_a_transaction_requires_hold() {
    let ex = seeded(3).await;
    let err = ex
        .execute(&format!("DECLARE c CURSOR FOR {DECLARE_ALL}"))
        .await
        .expect_err("a non-holdable cursor needs a transaction block");
    assert!(
        err.to_string()
            .contains("DECLARE CURSOR can only be used in transaction blocks"),
        "got: {err}"
    );
    assert_eq!(sqlstate_of(&err), "25P01");
    assert!(
        cursor_names(&ex).await.is_empty(),
        "a refusal stored a cursor"
    );

    // Control: WITH HOLD is accepted and fetchable with no transaction.
    exec(
        &ex,
        &format!("DECLARE h CURSOR WITH HOLD FOR {DECLARE_ALL}"),
    )
    .await;
    assert_eq!(fetched(&ex, "FETCH 2 FROM h").await, vec![1, 2]);
}

/// COMMIT drops the non-holdable cursor and keeps the held one, position and
/// all; the held cursor is then no longer tied to any transaction.
#[tokio::test]
async fn commit_closes_cursors_unless_held() {
    let ex = seeded(4).await;
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE a CURSOR FOR {DECLARE_ALL}")).await;
    exec(
        &ex,
        &format!("DECLARE h CURSOR WITH HOLD FOR {DECLARE_ALL}"),
    )
    .await;
    assert_eq!(fetched(&ex, "FETCH 1 FROM h").await, vec![1]);
    assert_eq!(cursor_names(&ex).await, vec!["a", "h"]);
    exec(&ex, "COMMIT").await;

    assert_eq!(cursor_names(&ex).await, vec!["h"]);
    assert_eq!(fetched(&ex, "FETCH 1 FROM h").await, vec![2]);
    let err = ex
        .execute("FETCH NEXT FROM a")
        .await
        .expect_err("the non-holdable cursor must be gone after COMMIT");
    assert_eq!(sqlstate_of(&err), "34000");

    // The held cursor survives a later ROLLBACK too: it no longer belongs to
    // a transaction.
    exec(&ex, "BEGIN").await;
    exec(&ex, "ROLLBACK").await;
    assert_eq!(cursor_names(&ex).await, vec!["h"]);
}

/// ROLLBACK drops every non-holdable cursor and any held cursor declared in
/// the rolled-back transaction; a held cursor from before it survives.
#[tokio::test]
async fn rollback_closes_cursors_and_drops_holds_declared_in_the_transaction() {
    let ex = seeded(3).await;
    exec(
        &ex,
        &format!("DECLARE earlier CURSOR WITH HOLD FOR {DECLARE_ALL}"),
    )
    .await;
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE a CURSOR FOR {DECLARE_ALL}")).await;
    exec(
        &ex,
        &format!("DECLARE h CURSOR WITH HOLD FOR {DECLARE_ALL}"),
    )
    .await;
    assert_eq!(cursor_names(&ex).await, vec!["a", "earlier", "h"]);
    exec(&ex, "ROLLBACK").await;
    assert_eq!(cursor_names(&ex).await, vec!["earlier"]);
    assert_eq!(fetched(&ex, "FETCH 1 FROM earlier").await, vec![1]);
}

/// A transaction that hit an error commits as a rollback, and its cursors go
/// with it.
#[tokio::test]
async fn aborted_transaction_closes_its_cursors() {
    let ex = seeded(3).await;
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE a CURSOR FOR {DECLARE_ALL}")).await;
    ex.execute("SELECT * FROM no_such_table")
        .await
        .expect_err("the statement fails and aborts the transaction");
    let results = exec(&ex, "COMMIT").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "ROLLBACK"),
        other => panic!("expected a command tag, got {other:?}"),
    }
    assert!(cursor_names(&ex).await.is_empty());
}

// ── Materialization budgets ─────────────────────────────────────────────

/// A DECLARE over the row budget is refused with 54000 and stores nothing.
/// Controls: a result exactly at the budget, and a smaller user LIMIT, are
/// accepted; a user LIMIT above the budget does not bypass it.
#[tokio::test]
async fn row_budget_refuses_declare_and_stores_nothing() {
    let ex = seeded(5).await;
    ex.set_cursor_budgets(3, 1 << 30);

    exec(&ex, "BEGIN").await;
    let err = ex
        .execute(&format!("DECLARE c CURSOR FOR {DECLARE_ALL}"))
        .await
        .expect_err("5 rows exceed a 3-row budget");
    assert!(
        err.to_string().contains("too_many_cursor_rows"),
        "got: {err}"
    );
    assert_eq!(sqlstate_of(&err), "54000");
    assert!(
        cursor_names(&ex).await.is_empty(),
        "a refusal stored a cursor"
    );
    exec(&ex, "ROLLBACK").await;

    exec(&ex, "BEGIN").await;
    let err = ex
        .execute(&format!("DECLARE c CURSOR FOR {DECLARE_ALL} LIMIT 100"))
        .await
        .expect_err("a LIMIT above the budget must not bypass it");
    assert!(
        err.to_string().contains("too_many_cursor_rows"),
        "got: {err}"
    );
    exec(&ex, "ROLLBACK").await;

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "DECLARE at_budget CURSOR FOR SELECT id FROM cur_t WHERE id <= 3 ORDER BY id",
    )
    .await;
    exec(
        &ex,
        &format!("DECLARE limited CURSOR FOR {DECLARE_ALL} LIMIT 2"),
    )
    .await;
    assert_eq!(
        fetched(&ex, "FETCH ALL FROM at_budget").await,
        vec![1, 2, 3]
    );
    assert_eq!(fetched(&ex, "FETCH ALL FROM limited").await, vec![1, 2]);
    exec(&ex, "COMMIT").await;
}

/// The byte budget refuses wide rows that fit the row budget. Controls: one
/// wide row fits; the budget is per cursor, so the refusal leaves existing
/// cursors alone.
#[tokio::test]
async fn byte_budget_refuses_wide_rows() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE wide (id INT, body TEXT)").await;
    for i in 1..=3 {
        exec(
            &ex,
            &format!("INSERT INTO wide VALUES ({i}, '{}')", "x".repeat(3000)),
        )
        .await;
    }
    ex.set_cursor_budgets(1_000_000, 4096);

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "DECLARE one CURSOR FOR SELECT * FROM wide WHERE id = 1",
    )
    .await;
    let err = ex
        .execute("DECLARE all_rows CURSOR FOR SELECT * FROM wide ORDER BY id")
        .await
        .expect_err("9 KB of rows exceed a 4 KB budget");
    assert!(
        err.to_string().contains("too_many_cursor_bytes"),
        "got: {err}"
    );
    assert_eq!(sqlstate_of(&err), "54000");
    assert_eq!(cursor_names(&ex).await, vec!["one"]);
    exec(&ex, "ROLLBACK").await;
}

/// The shipped defaults are finite and the setter is the config knob.
#[test]
fn cursor_budget_defaults_are_finite() {
    let ex = test_executor();
    let rows = ex
        .max_cursor_rows
        .load(std::sync::atomic::Ordering::Relaxed);
    let bytes = ex
        .max_cursor_bytes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(rows, crate::executor::DEFAULT_MAX_CURSOR_ROWS);
    assert_eq!(bytes, crate::executor::DEFAULT_MAX_CURSOR_BYTES);
    assert!(rows < usize::MAX && bytes < usize::MAX);
    ex.set_cursor_budgets(7, 9);
    assert_eq!(
        ex.max_cursor_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        7
    );
    assert_eq!(
        ex.max_cursor_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        9
    );
}

// ── Lazy cursors over generate_series ───────────────────────────────────

/// DECLARE over a table-free series executes nothing: a range far beyond any
/// budget is accepted, holds no rows, and FETCH produces only what it returns.
/// The row budget is tiny on purpose: an implementation that ran the series at
/// DECLARE refused all of these with 54000. Controls: SCROLL needs the rows
/// held, so it is materialized and held to the same budget, and a range too
/// long to build is refused without building it.
#[tokio::test]
async fn declare_over_a_series_executes_nothing() {
    let ex = test_executor();
    ex.set_cursor_budgets(10, 1 << 30);
    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "DECLARE huge NO SCROLL CURSOR FOR SELECT g FROM generate_series(1, 1000000000000) g",
    )
    .await;
    exec(
        &ex,
        "DECLARE small CURSOR FOR SELECT g FROM generate_series(1, 1000) g",
    )
    .await;
    for name in ["huge", "small"] {
        assert!(is_lazy(&ex, name).await, "{name} was materialized");
        assert_eq!(held_rows(&ex, name).await, 0, "{name} holds rows");
    }
    assert_eq!(fetched(&ex, "FETCH 3 FROM huge").await, vec![1, 2, 3]);
    assert_eq!(position_of(&ex, "huge").await, 3);
    assert_eq!(fetched(&ex, "FETCH 2 FROM huge").await, vec![4, 5]);
    assert_eq!(position_of(&ex, "huge").await, 5);
    assert_eq!(held_rows(&ex, "huge").await, 0);

    let err = ex
        .execute("DECLARE s SCROLL CURSOR FOR SELECT g FROM generate_series(1, 1000) g")
        .await
        .expect_err("a SCROLL cursor over 1000 rows exceeds a 10-row budget");
    assert_eq!(sqlstate_of(&err), "54000");
    exec(&ex, "ROLLBACK").await;

    exec(&ex, "BEGIN").await;
    let err = ex
        .execute("DECLARE s SCROLL CURSOR FOR SELECT g FROM generate_series(1, 1000000000000) g")
        .await
        .expect_err("a materialized series that long is refused, not built");
    assert!(
        err.to_string().contains("too_many_cursor_rows"),
        "got: {err}"
    );
    assert_eq!(sqlstate_of(&err), "54000");
    assert!(cursor_names(&ex).await.is_empty());
    exec(&ex, "ROLLBACK").await;
}

/// A row that errors only when it is produced fails the FETCH that reaches it,
/// not the DECLARE. The failing cursor is closed (its producer is released),
/// and the error aborts the transaction like any other statement error.
#[tokio::test]
async fn a_late_row_error_surfaces_at_fetch_and_closes_the_cursor() {
    let ex = test_executor();
    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "DECLARE c NO SCROLL CURSOR FOR SELECT 100 / (50 - g) AS q FROM generate_series(1, 100) g",
    )
    .await;
    assert!(is_lazy(&ex, "c").await);
    assert_eq!(fetched(&ex, "FETCH 49 FROM c").await.len(), 49);
    assert_eq!(position_of(&ex, "c").await, 49);

    let err = ex
        .execute("FETCH 1 FROM c")
        .await
        .expect_err("series value 50 divides by zero");
    assert_eq!(sqlstate_of(&err), "22012");
    assert!(
        cursor_names(&ex).await.is_empty(),
        "a failed producer was left open"
    );
    let err = ex
        .execute("SELECT 1")
        .await
        .expect_err("the failed FETCH aborted the transaction");
    assert_eq!(sqlstate_of(&err), "25P02");
    exec(&ex, "ROLLBACK").await;
}

/// WHERE, OFFSET, LIMIT and the select list give the rows the plain query
/// gives, and running off the end lands after the last row.
#[tokio::test]
async fn lazy_cursor_rows_equal_the_plain_query() {
    let ex = test_executor();
    let query =
        "SELECT g, g * 2 AS dbl FROM generate_series(1, 20) g WHERE g % 3 = 0 LIMIT 4 OFFSET 1";
    let plain = exec(&ex, query).await;
    let expected = int_col(&plain[0], 0);
    assert_eq!(expected, vec![6, 9, 12, 15]);

    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE c NO SCROLL CURSOR FOR {query}")).await;
    assert!(is_lazy(&ex, "c").await);
    let all = exec(&ex, "FETCH ALL FROM c").await;
    assert_eq!(int_col(&all[0], 0), expected);
    assert_eq!(int_col(&all[0], 1), vec![12, 18, 24, 30]);
    match (&plain[0], &all[0]) {
        (ExecResult::Select { columns: want, .. }, ExecResult::Select { columns: got, .. }) => {
            let names = |cols: &Vec<(String, crate::types::DataType)>| -> Vec<String> {
                cols.iter().map(|(name, _)| name.clone()).collect()
            };
            assert_eq!(names(got), names(want));
        }
        other => panic!("expected two SELECT results, got {other:?}"),
    }
    assert_eq!(position_of(&ex, "c").await, 5);
    assert!(fetched(&ex, "FETCH NEXT FROM c").await.is_empty());
    assert_eq!(position_of(&ex, "c").await, 5);
    exec(&ex, "COMMIT").await;
}

/// Every forward movement lands where PostgreSQL's would, including a zero
/// count re-fetching the current row, ABSOLUTE and RELATIVE skipping ahead,
/// and the after-last position. The 5-row budget is below the series length,
/// so a materialized cursor would have been refused at DECLARE.
#[tokio::test]
async fn fetch_advances_a_lazy_cursor_like_postgres() {
    let ex = test_executor();
    ex.set_cursor_budgets(5, 1 << 30);
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE c CURSOR FOR {SERIES_10}")).await;
    assert!(is_lazy(&ex, "c").await);

    // (statement, rows it returns, position afterwards)
    let steps: &[(&str, &[i64], usize)] = &[
        ("FETCH 0 FROM c", &[], 0),
        ("FETCH NEXT FROM c", &[1], 1),
        ("FETCH 2 FROM c", &[2, 3], 3),
        ("FETCH FORWARD 1 FROM c", &[4], 4),
        ("FETCH 0 FROM c", &[4], 4),
        ("FETCH RELATIVE 0 FROM c", &[4], 4),
        ("FETCH ABSOLUTE 7 FROM c", &[7], 7),
        ("FETCH RELATIVE 2 FROM c", &[9], 9),
        ("FETCH ALL FROM c", &[10], 11),
        ("FETCH NEXT FROM c", &[], 11),
        ("FETCH 0 FROM c", &[], 11),
        ("FETCH ABSOLUTE 12 FROM c", &[], 11),
    ];
    for (sql, expected, position) in steps {
        assert_eq!(&fetched(&ex, sql).await, expected, "rows of: {sql}");
        assert_eq!(
            position_of(&ex, "c").await,
            *position,
            "position after: {sql}"
        );
    }
    assert_eq!(held_rows(&ex, "c").await, 0);
    exec(&ex, "COMMIT").await;
}

/// A lazy cursor keeps no rows, so a movement that is not strictly forward is
/// refused with 55000 and leaves the cursor where it was. An explicit SCROLL
/// is the way to ask for backward movement: it is materialized and honoured.
#[tokio::test]
async fn a_lazy_cursor_is_forward_only_and_scroll_materializes() {
    let backward = [
        "FETCH PRIOR FROM c",
        "FETCH BACKWARD 1 FROM c",
        "FETCH BACKWARD ALL FROM c",
        "FETCH FIRST FROM c",
        "FETCH LAST FROM c",
        "FETCH ABSOLUTE 1 FROM c",
    ];
    for sql in backward {
        let ex = test_executor();
        exec(&ex, "BEGIN").await;
        exec(&ex, &format!("DECLARE c CURSOR FOR {SERIES_10}")).await;
        assert!(is_lazy(&ex, "c").await);
        assert_eq!(fetched(&ex, "FETCH 2 FROM c").await, vec![1, 2]);
        let err = ex
            .execute(sql)
            .await
            .expect_err("a lazy cursor cannot move backward");
        assert!(
            err.to_string().contains("cursor can only scan forward"),
            "{sql}: {err}"
        );
        assert_eq!(sqlstate_of(&err), "55000", "{sql}");
        assert_eq!(position_of(&ex, "c").await, 2, "{sql} moved the cursor");
        assert_eq!(
            cursor_names(&ex).await,
            vec!["c"],
            "{sql} closed the cursor"
        );
        exec(&ex, "ROLLBACK").await;
    }

    // Control: SCROLL is held in memory and may move both ways.
    let ex = test_executor();
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE s SCROLL CURSOR FOR {SERIES_10}")).await;
    assert!(!is_lazy(&ex, "s").await);
    assert_eq!(held_rows(&ex, "s").await, 10);
    assert_eq!(fetched(&ex, "FETCH 3 FROM s").await, vec![1, 2, 3]);
    assert_eq!(fetched(&ex, "FETCH PRIOR FROM s").await, vec![2]);
    assert_eq!(fetched(&ex, "FETCH LAST FROM s").await, vec![10]);
    exec(&ex, "COMMIT").await;
}

/// A FETCH is bounded by the cursor budgets whatever the series length: the
/// row budget refuses `FETCH ALL` and an oversized count, the byte budget
/// refuses a result that is too wide, and a refusal consumes nothing. The
/// cursors are WITH HOLD so no transaction is aborted by the refusal and the
/// same cursor can be fetched again afterwards.
#[tokio::test]
async fn fetch_is_bounded_by_the_row_and_byte_budgets() {
    let ex = test_executor();
    ex.set_cursor_budgets(10, 1 << 30);
    exec(
        &ex,
        "DECLARE c CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 100) g",
    )
    .await;
    assert!(is_lazy(&ex, "c").await);
    assert_eq!(fetched(&ex, "FETCH 2 FROM c").await, vec![1, 2]);
    for sql in [
        "FETCH ALL FROM c",
        "FETCH 11 FROM c",
        "FETCH FORWARD 50 FROM c",
    ] {
        let err = ex
            .execute(sql)
            .await
            .expect_err("more rows than the budget must be refused");
        assert!(
            err.to_string().contains("too_many_cursor_rows"),
            "{sql}: {err}"
        );
        assert_eq!(sqlstate_of(&err), "54000", "{sql}");
        assert_eq!(position_of(&ex, "c").await, 2, "{sql} consumed rows");
    }
    // Control: exactly the budget is fine, and picks up where the cursor was.
    assert_eq!(
        fetched(&ex, "FETCH 10 FROM c").await,
        (3..=12).collect::<Vec<i64>>()
    );
    assert_eq!(position_of(&ex, "c").await, 12);

    ex.set_cursor_budgets(1_000_000, 200);
    let err = ex
        .execute("FETCH 10 FROM c")
        .await
        .expect_err("ten rows exceed a 200-byte budget");
    assert!(
        err.to_string().contains("too_many_cursor_bytes"),
        "got: {err}"
    );
    assert_eq!(sqlstate_of(&err), "54000");
    assert_eq!(position_of(&ex, "c").await, 12);
    // Control: one row fits.
    assert_eq!(fetched(&ex, "FETCH 1 FROM c").await, vec![13]);
}

/// A cancel that arrives while a FETCH runs stops the producer: the FETCH
/// fails with 57014, the cursor is closed and its state released, the flag is
/// consumed, and the transaction is aborted like after any statement error.
#[tokio::test]
async fn cancel_during_fetch_stops_the_producer_and_closes_the_cursor() {
    let ex = test_executor();
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE c CURSOR FOR {SERIES_10}")).await;
    assert_eq!(fetched(&ex, "FETCH 2 FROM c").await, vec![1, 2]);

    ex.current_session()
        .cancel_requested
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let err = ex
        .execute("FETCH 5 FROM c")
        .await
        .expect_err("a cancelled FETCH must fail");
    assert_eq!(sqlstate_of(&err), "57014");
    assert!(
        !ex.current_session()
            .cancel_requested
            .load(std::sync::atomic::Ordering::Relaxed),
        "the cancel flag outlived the command it cancelled"
    );
    assert!(
        cursor_names(&ex).await.is_empty(),
        "a cancelled cursor was left open"
    );
    let err = ex
        .execute("SELECT 1")
        .await
        .expect_err("the cancelled FETCH aborted the transaction");
    assert_eq!(sqlstate_of(&err), "25P02");
    exec(&ex, "ROLLBACK").await;
}

/// Lazy cursors die with their transaction like any other; a WITH HOLD one
/// survives COMMIT with its producer intact (nothing in it belonged to the
/// transaction) and is dropped by a ROLLBACK of the transaction that declared
/// it.
#[tokio::test]
async fn transaction_end_closes_lazy_cursors_unless_held() {
    let ex = test_executor();
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE a CURSOR FOR {SERIES_10}")).await;
    exec(&ex, &format!("DECLARE h CURSOR WITH HOLD FOR {SERIES_10}")).await;
    assert!(is_lazy(&ex, "a").await && is_lazy(&ex, "h").await);
    assert_eq!(fetched(&ex, "FETCH 2 FROM h").await, vec![1, 2]);
    exec(&ex, "COMMIT").await;

    assert_eq!(cursor_names(&ex).await, vec!["h"]);
    assert!(is_lazy(&ex, "h").await);
    assert_eq!(fetched(&ex, "FETCH 3 FROM h").await, vec![3, 4, 5]);
    let err = ex
        .execute("FETCH NEXT FROM a")
        .await
        .expect_err("the non-holdable cursor must be gone after COMMIT");
    assert_eq!(sqlstate_of(&err), "34000");

    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE r CURSOR FOR {SERIES_10}")).await;
    exec(&ex, &format!("DECLARE rh CURSOR WITH HOLD FOR {SERIES_10}")).await;
    exec(&ex, "ROLLBACK").await;
    assert_eq!(cursor_names(&ex).await, vec!["h"]);
    assert_eq!(fetched(&ex, "FETCH 1 FROM h").await, vec![6]);

    // Outside a transaction block only WITH HOLD is accepted, lazy or not.
    let err = ex
        .execute(&format!("DECLARE x CURSOR FOR {SERIES_10}"))
        .await
        .expect_err("a non-holdable cursor needs a transaction block");
    assert_eq!(sqlstate_of(&err), "25P01");
}

// ── ROLLBACK TO SAVEPOINT ───────────────────────────────────────────────

/// A cursor declared after a savepoint does not survive rolling back to it
/// (PostgreSQL closes them, held or not); a cursor declared before it keeps
/// the position that FETCHes inside the savepoint left it at, because cursor
/// motion is not undone.
#[tokio::test]
async fn rollback_to_savepoint_closes_cursors_declared_inside_it() {
    let ex = test_executor();
    exec(&ex, "BEGIN").await;
    exec(&ex, &format!("DECLARE outer_c CURSOR FOR {SERIES_10}")).await;
    assert_eq!(fetched(&ex, "FETCH 3 FROM outer_c").await, vec![1, 2, 3]);
    exec(&ex, "SAVEPOINT sp").await;
    exec(&ex, &format!("DECLARE inner_lazy CURSOR FOR {SERIES_10}")).await;
    exec(
        &ex,
        &format!("DECLARE inner_scroll SCROLL CURSOR FOR {SERIES_10}"),
    )
    .await;
    exec(
        &ex,
        &format!("DECLARE inner_hold CURSOR WITH HOLD FOR {SERIES_10}"),
    )
    .await;
    assert_eq!(fetched(&ex, "FETCH 2 FROM outer_c").await, vec![4, 5]);
    assert_eq!(
        cursor_names(&ex).await,
        vec!["inner_hold", "inner_lazy", "inner_scroll", "outer_c"]
    );

    exec(&ex, "ROLLBACK TO SAVEPOINT sp").await;
    assert_eq!(cursor_names(&ex).await, vec!["outer_c"]);
    assert_eq!(position_of(&ex, "outer_c").await, 5);
    assert_eq!(fetched(&ex, "FETCH 1 FROM outer_c").await, vec![6]);
    exec(&ex, "COMMIT").await;

    let err = ex
        .execute("FETCH NEXT FROM inner_lazy")
        .await
        .expect_err("the cursor declared inside the savepoint is gone");
    assert_eq!(sqlstate_of(&err), "34000");
}

/// Nested savepoints: rolling back to one closes only what was declared after
/// it, the savepoint itself stays usable, RELEASE keeps its cursors (they now
/// belong to the enclosing savepoint), and the outer rollback closes them.
#[tokio::test]
async fn savepoint_cursor_marks_nest_and_release_keeps_cursors() {
    let ex = test_executor();
    exec(&ex, "BEGIN").await;
    exec(&ex, "SAVEPOINT a").await;
    exec(&ex, &format!("DECLARE c1 CURSOR FOR {SERIES_10}")).await;
    exec(&ex, "SAVEPOINT b").await;
    exec(&ex, &format!("DECLARE c2 CURSOR FOR {SERIES_10}")).await;

    exec(&ex, "ROLLBACK TO SAVEPOINT b").await;
    assert_eq!(cursor_names(&ex).await, vec!["c1"]);

    // `b` survives its own rollback, so a second one closes the new cursor too.
    exec(&ex, &format!("DECLARE c3 SCROLL CURSOR FOR {SERIES_10}")).await;
    assert_eq!(cursor_names(&ex).await, vec!["c1", "c3"]);
    exec(&ex, "ROLLBACK TO SAVEPOINT b").await;
    assert_eq!(cursor_names(&ex).await, vec!["c1"]);

    exec(&ex, "SAVEPOINT r").await;
    exec(&ex, &format!("DECLARE c4 CURSOR FOR {SERIES_10}")).await;
    exec(&ex, "RELEASE SAVEPOINT r").await;
    assert_eq!(cursor_names(&ex).await, vec!["c1", "c4"]);

    exec(&ex, "ROLLBACK TO SAVEPOINT a").await;
    assert!(cursor_names(&ex).await.is_empty());
    exec(&ex, "COMMIT").await;
}

// ── Limits and shape ────────────────────────────────────────────────────

/// Many cursors per session are bounded by the cap, and what each lazy one
/// costs is a few integers: no rows, no bytes, whatever its series length and
/// however small the per-cursor budgets. The cap is refused with 54000 before
/// any planning, CLOSE makes room, and re-declaring a name replaces it.
#[tokio::test]
async fn lazy_cursors_per_session_are_capped_and_hold_no_rows() {
    let ex = test_executor();
    ex.set_session_statement_limits(1024, 3);
    ex.set_cursor_budgets(2, 1);
    for i in 0..3 {
        exec(
            &ex,
            &format!(
                "DECLARE c{i} CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 1000000000000) g"
            ),
        )
        .await;
    }
    for i in 0..3 {
        let name = format!("c{i}");
        assert!(is_lazy(&ex, &name).await, "{name} was materialized");
        assert_eq!(held_rows(&ex, &name).await, 0, "{name} holds rows");
        assert_eq!(
            ex.current_session().cursors.read().await[&name].bytes,
            0,
            "{name} holds bytes"
        );
    }
    let err = ex
        .execute("DECLARE c3 CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 5) g")
        .await
        .expect_err("the 4th distinct cursor must be refused");
    assert!(err.to_string().contains("too_many_cursors"), "got: {err}");
    assert_eq!(sqlstate_of(&err), "54000");

    // Controls: replacing a name does not use budget, CLOSE makes room.
    exec(
        &ex,
        "DECLARE c0 CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 5) g",
    )
    .await;
    exec(&ex, "CLOSE c1").await;
    exec(
        &ex,
        "DECLARE c3 CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 5) g",
    )
    .await;
    assert_eq!(cursor_names(&ex).await, vec!["c0", "c2", "c3"]);
}

/// Exactly one shape is lazy. Everything that needs the whole input before it
/// can emit a row (a sort, DISTINCT), computes with a function the producer
/// will not evaluate at FETCH time, or reads a table is materialized under the
/// budgets instead, and so is an explicit SCROLL.
#[tokio::test]
async fn only_the_bare_series_shape_is_lazy() {
    let ex = seeded(3).await;
    let lazy = [
        ("l1", "SELECT g FROM generate_series(1, 5) g"),
        ("l2", "SELECT * FROM generate_series(1, 5)"),
        (
            "l3",
            "SELECT n FROM generate_series(5, 1, -2) AS t(n) WHERE n > 1 LIMIT 2 OFFSET 1",
        ),
        ("l4", "SELECT g, g + 1 AS nxt FROM generate_series(1, 5) g"),
    ];
    for (name, query) in lazy {
        exec(&ex, &format!("DECLARE {name} CURSOR WITH HOLD FOR {query}")).await;
        assert!(is_lazy(&ex, name).await, "{query} was materialized");
        assert_eq!(held_rows(&ex, name).await, 0, "{query} holds rows");
    }
    assert_eq!(fetched(&ex, "FETCH ALL FROM l3").await, vec![3]);
    assert_eq!(fetched(&ex, "FETCH ALL FROM l2").await, vec![1, 2, 3, 4, 5]);

    let materialized = [
        (
            "m1",
            "SELECT g FROM generate_series(1, 5) g ORDER BY g DESC",
            5,
        ),
        ("m2", "SELECT abs(g) FROM generate_series(1, 5) g", 5),
        ("m3", "SELECT DISTINCT g FROM generate_series(1, 5) g", 5),
        ("m4", "SELECT id FROM cur_t", 3),
    ];
    for (name, query, rows) in materialized {
        exec(&ex, &format!("DECLARE {name} CURSOR WITH HOLD FOR {query}")).await;
        assert!(!is_lazy(&ex, name).await, "{query} was lazy");
        assert_eq!(held_rows(&ex, name).await, rows, "{query}");
    }
    exec(
        &ex,
        "DECLARE m5 SCROLL CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 5) g",
    )
    .await;
    assert!(!is_lazy(&ex, "m5").await);
    assert_eq!(held_rows(&ex, "m5").await, 5);
}

/// Session reset (pool return, disconnect cleanup) closes lazy cursors.
#[cfg(feature = "server")]
#[tokio::test]
async fn session_reset_closes_lazy_cursors() {
    let ex = test_executor();
    let id = ex.create_session();
    ex.execute_with_session(
        id,
        "DECLARE h CURSOR WITH HOLD FOR SELECT g FROM generate_series(1, 5) g",
    )
    .await
    .unwrap();
    assert_eq!(ex.get_session(id).cursors.read().await.len(), 1);
    let actions = ex.reset_session(id).await;
    assert!(
        actions.iter().any(|a| a == "CLOSE ALL cursors"),
        "got: {actions:?}"
    );
    assert!(ex.get_session(id).cursors.read().await.is_empty());
    ex.drop_session(id);
}
