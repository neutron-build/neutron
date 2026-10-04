//! SQL cursors (DECLARE / FETCH / CLOSE): the materialization budgets, the
//! transaction lifetime, and FETCH position semantics.
//!
//! A cursor here is a materialized snapshot taken at DECLARE (see
//! `execute_declare_cursor`), not a lazy scan. What these tests pin is the
//! part that IS bounded and PostgreSQL-faithful: a budget refusal stores
//! nothing, a non-holdable cursor dies with its transaction, and every FETCH
//! direction lands on the position PostgreSQL would. Errors raised inside an
//! explicit transaction abort it (as in PostgreSQL), so each refusal case
//! opens its own transaction and ends it with ROLLBACK.

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
