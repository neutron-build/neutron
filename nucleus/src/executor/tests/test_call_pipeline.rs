//! The CALL pipeline: raw-text interception vs the AST path.
//!
//! Every `CALL ` statement used to be intercepted as raw text before the
//! parser ever saw it, then split by hand — which produced a reversed-slice
//! panic on `CALL (`, comma-split literals, an is-SQL sniff that EXECUTED
//! builtin output (second-order injection), unbounded recursion, and Latin-1
//! mojibake of non-ASCII arguments. These tests pin the corrected pipeline:
//! CALL goes through the real parser, recursion is depth-bounded, builtin
//! output is data, and substitution is UTF-8 byte-exact.

use super::*;

// ── EXE-1/PRC-2: malformed CALL text must error, not panic ─────────────

#[tokio::test]
async fn call_without_close_paren_errors_cleanly() {
    let ex = Arc::new(test_executor());
    // Pre-fix: `rest[1..0]` slice panic inside execute_call_procedure.
    // The connection task runs the handler to completion; a panic inside
    // it would unwind the task (and pre-RUN-1, leak the admission permit).
    // Here the future is spawned on its own task: a panic surfaces as a
    // JoinError instead of taking the test process down.
    let ex2 = ex.clone();
    let handle = tokio::spawn(async move { ex2.execute("CALL (").await });
    match handle.await {
        Err(join_err) => panic!("CALL ( panicked: {join_err}"),
        Ok(Err(e)) => assert!(
            e.to_string().to_lowercase().contains("paren")
                || e.to_string().to_lowercase().contains("expected")
                || e.to_string().to_lowercase().contains("parse"),
            "expected a clean error, got: {e}"
        ),
        Ok(Ok(v)) => panic!("CALL ( must not succeed: {v:?}"),
    }
}

#[tokio::test]
async fn malformed_call_variants_error_or_run_cleanly() {
    let ex = test_executor();
    for sql in ["CALL p(", "CALL p", "CALL ()", "CALL ,,"] {
        // No requirement on Err vs Ok — only on NOT panicking.
        let _ = ex.execute(sql).await;
    }
}

// ── EXE-5/PRC-6: argument literals must survive intact ────────────────

#[tokio::test]
async fn call_text_argument_with_comma_stays_one_argument() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE greet_log (who TEXT)").await;
    exec(
        &ex,
        "CREATE PROCEDURE greet(name TEXT) LANGUAGE sql AS 'INSERT INTO greet_log VALUES ($name)'",
    )
    .await;
    exec(&ex, "CALL greet('Smith, John')").await;
    let rows = rows(&exec(&ex, "SELECT who FROM greet_log").await[0]).clone();
    assert_eq!(rows.len(), 1, "exactly one row: one argument, one insert");
    assert_eq!(
        rows[0][0],
        Value::Text("Smith, John".into()),
        "comma inside a quoted literal must not split the argument"
    );
}

#[tokio::test]
async fn call_quoted_number_stays_text() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nums (v TEXT)").await;
    exec(
        &ex,
        "CREATE PROCEDURE put(v TEXT) LANGUAGE sql AS 'INSERT INTO nums VALUES ($v)'",
    )
    .await;
    exec(&ex, "CALL put('0123')").await;
    let rows = rows(&exec(&ex, "SELECT v FROM nums").await[0]).clone();
    assert_eq!(
        rows[0][0],
        Value::Text("0123".into()),
        "quoted number must arrive as text with the leading zero intact"
    );
}

// ── PRC-5: builtin output is data, never executed ─────────────────────

#[tokio::test]
async fn builtin_output_shaped_like_sql_is_not_executed() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE important (id INT)").await;
    exec(&ex, "INSERT INTO important VALUES (1)").await;

    let res = ex
        .execute("CALL json_extract('{\"a\":\"DROP TABLE important\"}','a')")
        .await
        .expect("CALL json_extract must succeed");
    match &res[0] {
        ExecResult::Select { rows, .. } => assert_eq!(
            rows[0][0],
            Value::Text("DROP TABLE important".into()),
            "builtin output must come back as a data row"
        ),
        other => panic!("expected a data row, got {other:?}"),
    }
    let kept = rows(&exec(&ex, "SELECT * FROM important").await[0]).clone();
    assert_eq!(
        kept.len(),
        1,
        "the DROP in the builtin's output must NOT run"
    );
}

#[tokio::test]
async fn builtin_coalesce_output_shaped_like_sql_is_not_executed() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE keepme (id INT)").await;
    exec(&ex, "INSERT INTO keepme VALUES (1)").await;
    let _ = ex
        .execute("CALL coalesce('UPDATE keepme SET id = 99')")
        .await
        .expect("CALL coalesce must succeed");
    let got = scalar(&exec(&ex, "SELECT id FROM keepme").await[0]).clone();
    assert_eq!(got, Value::Int32(1), "the UPDATE must NOT run");
}

#[tokio::test]
async fn sql_procedure_selecting_drops_still_executes() {
    // Positive control: a REGISTERED SQL procedure's body must still run.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE probe (id INT)").await;
    exec(
        &ex,
        "CREATE PROCEDURE do_insert() LANGUAGE sql AS 'INSERT INTO probe VALUES (7)'",
    )
    .await;
    exec(&ex, "CALL do_insert()").await;
    let got = scalar(&exec(&ex, "SELECT id FROM probe").await[0]).clone();
    assert_eq!(got, Value::Int32(7));
}

// ── PRC-3 / PRC-4: substitution is byte-exact and backslash-honest ────

#[tokio::test]
async fn call_non_ascii_argument_survives_substitution() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE echo (v TEXT)").await;
    exec(
        &ex,
        "CREATE PROCEDURE echo(v TEXT) LANGUAGE sql AS 'INSERT INTO echo VALUES ($v)'",
    )
    .await;
    exec(&ex, "CALL echo('Zoë — ünïcode')").await;
    let rows = rows(&exec(&ex, "SELECT v FROM echo").await[0]).clone();
    assert_eq!(
        rows[0][0],
        Value::Text("Zoë — ünïcode".into()),
        "non-ASCII argument must arrive byte-exact (no Latin-1 mojibake)"
    );
}

#[tokio::test]
async fn udf_non_ascii_argument_survives_substitution() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE FUNCTION echo_udf(v TEXT) RETURNS TEXT LANGUAGE SQL AS $$ SELECT $1 $$",
    )
    .await;
    let res = exec(&ex, "SELECT echo_udf('你好')").await;
    assert_eq!(
        scalar(&res[0]),
        &Value::Text("你好".into()),
        "non-ASCII UDF argument must arrive byte-exact"
    );
}

#[tokio::test]
async fn call_backslash_in_text_argument_is_not_doubled() {
    // PostgreSqlDialect is standard-conforming: '\' inside '...' is literal.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE paths (p TEXT)").await;
    exec(
        &ex,
        "CREATE PROCEDURE put_path(p TEXT) LANGUAGE sql AS 'INSERT INTO paths VALUES ($p)'",
    )
    .await;
    exec(&ex, "CALL put_path('C:\\path\\to')").await;
    let rows = rows(&exec(&ex, "SELECT p FROM paths").await[0]).clone();
    assert_eq!(
        rows[0][0],
        Value::Text("C:\\path\\to".into()),
        "backslash must be preserved literally, not doubled"
    );
}

// ── PRC-7: CREATE PROCEDURE body quoting ──────────────────────────────

#[tokio::test]
async fn create_procedure_body_with_escaped_quotes_round_trips() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE PROCEDURE hi() LANGUAGE sql AS 'SELECT ''hi'' AS greeting'",
    )
    .await;
    let res = exec(&ex, "CALL hi()").await;
    assert_eq!(
        scalar(&res[0]),
        &Value::Text("hi".into()),
        "'SELECT ''hi''' must store as SELECT 'hi' and return hi"
    );
}

#[tokio::test]
async fn create_procedure_dollar_quoted_body_round_trips() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE PROCEDURE dj() LANGUAGE sql AS $$ SELECT 'a''b' AS v $$",
    )
    .await;
    let res = exec(&ex, "CALL dj()").await;
    assert_eq!(scalar(&res[0]), &Value::Text("a'b".into()));
}

// ── PRC-1: recursion is depth-bounded ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_recursive_procedure_returns_depth_error() {
    let ex = test_executor();
    exec(&ex, "CREATE PROCEDURE p() LANGUAGE sql AS 'CALL p()'").await;
    let res = ex.execute("CALL p()").await;
    match res {
        Err(e) => assert!(
            e.to_string().to_lowercase().contains("depth"),
            "expected a depth-limit error, got: {e}"
        ),
        Ok(v) => panic!("infinite recursion must error, not succeed: {v:?}"),
    }
}

// Multi_thread flavor matches production (the wire runs on multi_thread
// workers, where sync_block_on nests via block_in_place on the same thread —
// the headroom guard then sees the real shrinking stack).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_recursive_udf_returns_depth_error() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE SQL AS $$ SELECT f($1) $$",
    )
    .await;
    let res = ex.execute("SELECT f(1)").await;
    match res {
        Err(e) => assert!(
            e.to_string().to_lowercase().contains("depth"),
            "expected a depth-limit error, got: {e}"
        ),
        Ok(v) => panic!("infinite recursion must error, not succeed: {v:?}"),
    }
}

#[test]
#[cfg(feature = "server")]
fn bounded_mutual_recursion_still_succeeds() {
    // The 11-level chain needs real stack headroom (debug poll frames are
    // hundreds of KB per level); the headroom guard would refuse it on a
    // small test thread exactly as designed. Production stacks (tokio
    // workers configured per server, main thread 8 MB) have the room.
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                let ex = test_executor();
                // ev(n) -> od(n-1) -> ev(n-2) -> ... terminating at 0. Ten
                // frames deep must pass — the caps must not reject
                // legitimate finite recursion.
                exec(
                    &ex,
                    "CREATE FUNCTION ev(x INT) RETURNS INT LANGUAGE SQL AS $$ SELECT CASE WHEN $1 <= 0 THEN 0 ELSE od($1 - 1) END $$",
                )
                .await;
                exec(
                    &ex,
                    "CREATE FUNCTION od(x INT) RETURNS INT LANGUAGE SQL AS $$ SELECT CASE WHEN $1 <= 0 THEN 1 ELSE ev($1 - 1) END $$",
                )
                .await;
                let res = exec(&ex, "SELECT ev(10)").await;
                // ev(10) -> od(9) -> ... -> ev(0): terminates in ev's zero arm.
                assert_eq!(scalar(&res[0]), &Value::Int32(0));
            });
        })
        .expect("spawn big-stack thread")
        .join()
        .expect("big-stack thread panicked");
}
