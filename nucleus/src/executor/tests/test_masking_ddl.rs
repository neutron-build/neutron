//! `CREATE MASKING POLICY` over the wire.
//!
//! Enforcement was already real and tested (`test_masking`). What did not
//! exist was any way to DECLARE a policy from a client: `add_policy` is a Rust
//! API, so masking was reachable only from inside the process — which in
//! practice meant only from the test suite. These tests drive the SQL surface,
//! and the enforcement assertions run through it rather than around it, so a
//! DDL statement that parsed but installed the wrong policy fails here.

use super::*;

async fn seeded() -> Executor {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE people (id INT PRIMARY KEY, name TEXT, ssn TEXT, email TEXT)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO people VALUES (1, 'ada', '123-45-6789', 'ada@example.com')",
    )
    .await;
    exec(&ex, "CREATE ROLE analyst LOGIN PASSWORD 'p'").await;
    exec(&ex, "GRANT SELECT ON people TO analyst").await;
    ex
}

async fn as_analyst(ex: &Executor) -> u64 {
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "analyst").await.unwrap();
    sid
}

fn cell(result: &ExecResult, row: usize, col: usize) -> String {
    match &rows(result)[row][col] {
        Value::Text(t) => t.clone(),
        other => format!("{other:?}"),
    }
}

/// The whole point: a policy created over SQL is enforced.
#[tokio::test]
async fn a_policy_created_over_sql_is_enforced() {
    let ex = seeded().await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT '***'",
    )
    .await;
    let sid = as_analyst(&ex).await;
    let res = ex
        .execute_with_session(sid, "SELECT ssn, name FROM people")
        .await
        .expect("select");
    assert_eq!(
        cell(&res[0], 0, 0),
        "***",
        "the mask declared over SQL did not apply"
    );
    assert_eq!(cell(&res[0], 0, 1), "ada", "an unmasked column changed");
}

/// Every rule the engine has is reachable from the grammar. A rule that can be
/// stored but not written is the same gap one level down.
#[tokio::test]
async fn every_rule_is_reachable_from_sql() {
    for (sql_rule, want) in [
        ("REDACT 'X'", "X"),
        ("EMAIL", "a**@example.com"),
        ("PARTIAL (2, 3, '#')", "ad##########com"),
        ("NONE", "ada@example.com"),
    ] {
        let ex = seeded().await;
        exec(
            &ex,
            &format!("CREATE MASKING POLICY ON people (email) TO analyst USING {sql_rule}"),
        )
        .await;
        let sid = as_analyst(&ex).await;
        let res = ex
            .execute_with_session(sid, "SELECT email FROM people")
            .await
            .expect("select");
        assert_eq!(cell(&res[0], 0, 0), want, "rule `{sql_rule}` misapplied");
    }
    // HASH is checked separately: its output is a digest, so the assertion is
    // that it is neither the plaintext nor empty.
    let ex = seeded().await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (email) TO analyst USING HASH",
    )
    .await;
    let sid = as_analyst(&ex).await;
    let res = ex
        .execute_with_session(sid, "SELECT email FROM people")
        .await
        .expect("select");
    let got = cell(&res[0], 0, 0);
    assert!(
        got != "ada@example.com" && !got.is_empty(),
        "HASH returned {got:?}"
    );
}

/// Creating a policy twice replaces it rather than stacking a second one that
/// may or may not win.
#[tokio::test]
async fn creating_twice_replaces_rather_than_stacks() {
    let ex = seeded().await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT 'first'",
    )
    .await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT 'second'",
    )
    .await;
    let listed = exec(&ex, "SHOW MASKING POLICIES").await;
    assert_eq!(rows(&listed[0]).len(), 1, "a duplicate policy was stored");

    let sid = as_analyst(&ex).await;
    let res = ex
        .execute_with_session(sid, "SELECT ssn FROM people")
        .await
        .expect("select");
    assert_eq!(cell(&res[0], 0, 0), "second", "the later policy must win");
}

/// DROP removes it, and the column comes back in the clear for the same
/// session — so the drop is not merely bookkeeping.
#[tokio::test]
async fn drop_removes_the_mask_and_takes_effect() {
    let ex = seeded().await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT '***'",
    )
    .await;
    let sid = as_analyst(&ex).await;
    let masked = ex
        .execute_with_session(sid, "SELECT ssn FROM people")
        .await
        .unwrap();
    assert_eq!(cell(&masked[0], 0, 0), "***");

    exec(&ex, "DROP MASKING POLICY ON people (ssn) TO analyst").await;
    let clear = ex
        .execute_with_session(sid, "SELECT ssn FROM people")
        .await
        .unwrap();
    assert_eq!(
        cell(&clear[0], 0, 0),
        "123-45-6789",
        "the drop did not take effect on a live session — a cached plan or \
         result outlived the policy change"
    );

    let err = ex
        .execute("DROP MASKING POLICY ON people (ssn) TO analyst")
        .await
        .expect_err("dropping a policy that is not there must say so");
    assert!(err.to_string().contains("no masking policy"), "{err}");
}

/// `SHOW MASKING POLICIES` renders what would recreate the policy.
#[tokio::test]
async fn show_lists_policies_in_creatable_form() {
    let ex = seeded().await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (email) TO analyst USING PARTIAL (1, 4, '#')",
    )
    .await;
    let listed = exec(&ex, "SHOW MASKING POLICIES").await;
    let r = rows(&listed[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(cell(&listed[0], 0, 0), "people");
    assert_eq!(cell(&listed[0], 0, 1), "email");
    assert_eq!(cell(&listed[0], 0, 2), "analyst");
    assert_eq!(cell(&listed[0], 0, 3), "PARTIAL (1, 4, '#')");
}

/// A policy naming a column or role that does not exist never applies, and
/// reads as protection. It is refused instead.
#[tokio::test]
async fn a_policy_that_could_never_apply_is_refused() {
    let ex = seeded().await;
    for (sql, expect) in [
        (
            "CREATE MASKING POLICY ON people (nope) TO analyst USING HASH",
            "does not exist",
        ),
        (
            "CREATE MASKING POLICY ON people (ssn) TO ghost USING HASH",
            "does not exist",
        ),
        (
            "CREATE MASKING POLICY ON missing_table (ssn) TO analyst USING HASH",
            "missing_table",
        ),
        (
            "CREATE MASKING POLICY ON people (ssn) TO analyst USING WHATEVER",
            "unknown masking rule",
        ),
        ("CREATE MASKING POLICY ON people (ssn) TO analyst", "USING"),
        (
            "CREATE MASKING POLICY ON people (ssn) USING HASH",
            "TO <role>",
        ),
        (
            "CREATE MASKING POLICY ON people ssn TO analyst USING HASH",
            "parentheses",
        ),
        (
            "CREATE MASKING POLICY ON people (ssn) TO analyst USING PARTIAL (1)",
            "PARTIAL",
        ),
        (
            "CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT",
            "REDACT",
        ),
    ] {
        let err = ex
            .execute(sql)
            .await
            .expect_err(&format!("`{sql}` must be refused"));
        assert!(
            err.to_string().contains(expect),
            "`{sql}` was refused with the wrong message: {err}"
        );
    }
    // Control: none of those left a policy behind.
    let listed = exec(&ex, "SHOW MASKING POLICIES").await;
    assert!(
        rows(&listed[0]).is_empty(),
        "a refused statement stored a policy"
    );
}

/// Masking DDL is superuser-only, like RLS policy DDL.
#[tokio::test]
async fn masking_ddl_requires_superuser() {
    let ex = seeded().await;
    let sid = as_analyst(&ex).await;
    for sql in [
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING HASH",
        "DROP MASKING POLICY ON people (ssn) TO analyst",
        "SHOW MASKING POLICIES",
    ] {
        let err = ex
            .execute_with_session(sid, sql)
            .await
            .expect_err(&format!("`{sql}` must require superuser authority"));
        assert!(err.to_string().contains("superuser"), "`{sql}`: {err}");
    }
}

/// The stable column id is bound at CREATE. Nothing else could bind it —
/// `MaskingPolicy::column_id` carried the comment "masking has no CREATE DDL
/// surface yet, so there is no statement at which to resolve the id", so it
/// stayed 0 until a rename happened to stamp it. Until then a mask followed
/// its column NAME, which is the direction that fails open.
#[tokio::test]
async fn create_binds_the_stable_column_id() {
    let ex = seeded().await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING HASH",
    )
    .await;
    let bound = ex.with_visible_security(|s| {
        s.masking
            .all_policies()
            .iter()
            .map(|p| p.column_id)
            .collect::<Vec<_>>()
    });
    assert_eq!(bound.len(), 1);
    assert!(
        bound[0] != 0,
        "the column id must be resolved at CREATE, not left for a later rename"
    );
}

/// SEC-2: masking DDL inside a transaction staged into `security_pending`,
/// SHOW (which reads the staged view) listed it, and COMMIT then silently
/// discarded the staged catalog — masking never set `policy_dirty`, so the
/// publish gate at COMMIT skipped it and the policy vanished. Mirror of
/// test_rls' `policy_ddl_obeys_transactions_and_savepoints`.
#[tokio::test]
async fn masking_ddl_obeys_transactions_and_savepoints() {
    let ex = seeded().await;
    let sid = as_analyst(&ex).await;

    // COMMIT publishes: staged for this session, invisible to others, then
    // live for everyone.
    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT '***'",
    )
    .await;
    let staged = exec(&ex, "SHOW MASKING POLICIES").await;
    assert_eq!(
        rows(&staged[0]).len(),
        1,
        "the creating session must see its own staged policy"
    );
    let res = ex
        .execute_with_session(sid, "SELECT ssn FROM people")
        .await
        .unwrap();
    assert_eq!(
        cell(&res[0], 0, 0),
        "123-45-6789",
        "an uncommitted mask leaked to another session"
    );
    exec(&ex, "COMMIT").await;

    // THE drop point: still listed — and enforced — after COMMIT.
    let listed = exec(&ex, "SHOW MASKING POLICIES").await;
    assert_eq!(
        rows(&listed[0]).len(),
        1,
        "COMMIT silently dropped the staged masking policy"
    );
    let res = ex
        .execute_with_session(sid, "SELECT ssn FROM people")
        .await
        .unwrap();
    assert_eq!(
        cell(&res[0], 0, 0),
        "***",
        "COMMIT did not publish the mask to enforcement"
    );

    // ROLLBACK discards.
    exec(&ex, "BEGIN").await;
    exec(&ex, "DROP MASKING POLICY ON people (ssn) TO analyst").await;
    exec(&ex, "ROLLBACK").await;
    let listed = exec(&ex, "SHOW MASKING POLICIES").await;
    assert_eq!(rows(&listed[0]).len(), 1, "ROLLBACK kept the drop");

    // ROLLBACK TO SAVEPOINT reverts the DDL after the savepoint and keeps
    // the DDL before it.
    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        "CREATE MASKING POLICY ON people (email) TO analyst USING EMAIL",
    )
    .await;
    exec(&ex, "SAVEPOINT before_drop").await;
    exec(&ex, "DROP MASKING POLICY ON people (ssn) TO analyst").await;
    exec(&ex, "ROLLBACK TO SAVEPOINT before_drop").await;
    exec(&ex, "COMMIT").await;
    let listed = exec(&ex, "SHOW MASKING POLICIES").await;
    assert_eq!(
        rows(&listed[0]).len(),
        2,
        "savepoint rollback must restore the pair exactly: {listed:?}"
    );
}
