//! M5: unsupported policy expressions and protected specialty calls fail
//! CLOSED.
//!
//! `test_rls_surfaces.rs` attacks the ENFORCED paths — for every alternate
//! surface it tries to exfiltrate a forbidden row. This file attacks the
//! POLICY-COMPILATION boundary and the specialty guard from the other
//! direction: an expression the predicate representation does not cover must
//! be REJECTED at DDL time, never accepted with a different meaning, and a
//! rejected policy must leave the table protected. Sessions here mirror the
//! authenticated wire session (`bind_authenticated_session` is what a SCRAM
//! login calls), so these legs run the same policy-aware SQL path the server
//! serves.
//!
//! The rule for every expression-shape test: `CREATE POLICY` either errors
//! (acceptable), or compiles to a predicate that enforces EXACTLY the written
//! expression (acceptable). Silently compiling to a WEAKER predicate — or to
//! a different one — is the fail-open bug this file exists to catch, because
//! the policy author reads the DDL as accepted and enforced.

use super::*;

/// Run SQL on the trusted bootstrap session (the superuser DDL path).
async fn admin(ex: &Executor, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
    ex.execute(sql).await
}

/// `docs` holds 2 alice rows (1, 3) and 1 bob row (2); the good policy hides
/// bob's row from alice. The bad-policy attempts below must neither install
/// an anything-goes policy nor disturb this one.
async fn setup(ex: &Executor) -> u64 {
    exec(
        ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT, score INT)",
    )
    .await;
    exec(
        ex,
        "INSERT INTO docs VALUES (1, 'alice', 'a1', 10), (2, 'bob', 'b1', 99), (3, 'alice', 'a2', 30)",
    )
    .await;
    exec(ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    exec(ex, "GRANT SELECT ON docs TO alice").await;
    exec(
        ex,
        "CREATE POLICY owner_isolation ON docs FOR SELECT TO PUBLIC \
         USING (owner = CURRENT_USER)",
    )
    .await;
    exec(ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    sid
}

/// The rows alice can see, as sorted ids — the exact set the good policy
/// admits. Every rejection test ends by re-asserting this set, so a CREATE
/// POLICY that "failed" but actually installed something cannot pass.
async fn visible_ids(ex: &Executor, sid: u64) -> Vec<i64> {
    let out = ex
        .execute_with_session(sid, "SELECT id FROM docs ORDER BY id")
        .await
        .expect("select must not error under a valid policy");
    let r = rows(&out[0]);
    r.iter()
        .map(|row| match row.first() {
            Some(Value::Int32(n)) => i64::from(*n),
            Some(Value::Int64(n)) => *n,
            other => panic!("expected integer id, got {other:?}"),
        })
        .collect()
}

// ============================================================================
// Group A — unsupported expressions are rejected, and rejection is inert
// ============================================================================

/// Every expression here is OUTSIDE the supported predicate set
/// (RLS_SECURITY.md "Policy DDL"). Each CREATE POLICY must error, and after
/// all of them the table must still be protected by the one good policy:
/// alice sees exactly her rows.
#[tokio::test]
async fn unsupported_policy_expressions_are_rejected_without_disturbing_enforcement() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    let unsupported: Vec<(&str, &str)> = vec![
        // Unknown / arbitrary scalar functions.
        ("unknown function", "USING (lower(owner) = 'alice')"),
        ("function as the predicate", "USING (md5('x'))"),
        // Arithmetic on the guarded column.
        ("arithmetic", "USING (score + 1 > 10)"),
        // Column-to-column comparison: the predicate representation only
        // knows column-vs-literal.
        ("column to column", "USING (owner = body)"),
        // Pattern matching and conditional forms.
        ("LIKE", "USING (owner LIKE 'a%')"),
        (
            "CASE",
            "USING (CASE WHEN score > 1 THEN true ELSE false END)",
        ),
        ("BETWEEN", "USING (score BETWEEN 1 AND 99)"),
        // Subqueries, correlated or not.
        ("EXISTS subquery", "USING (EXISTS (SELECT 1 FROM docs))"),
        ("IN subquery", "USING (owner IN (SELECT owner FROM docs))"),
        // IN list with a non-literal element.
        ("IN list with column", "USING (owner IN (owner, 'x'))"),
        // A NULL literal is not a policy value: rejecting it at compile time
        // is the fail-closed direction (SQL says the comparison is unknown).
        ("NULL literal comparison", "USING (owner = NULL)"),
        // has_role() with a non-literal argument.
        ("has_role of column", "USING (has_role(owner))"),
        // Non-boolean predicates.
        ("string literal predicate", "USING ('yes')"),
        ("numeric literal predicate", "USING (42)"),
        // IS NULL of something that is not a plain column.
        ("IS NULL of a function", "USING (lower(owner) IS NULL)"),
        // Literal-vs-literal comparison names no column to guard with.
        ("literal to literal", "USING ('a' < 'b')"),
        // session_user() with parentheses is not in the supported spelling
        // list; the bare SESSION_USER is. Pinned as a denial so widening it
        // is a deliberate act rather than silent drift.
        ("session_user() parens", "USING (owner = session_user())"),
    ];

    for (i, (label, clause)) in unsupported.iter().enumerate() {
        let sql = format!("CREATE POLICY bad_{i} ON docs FOR SELECT TO PUBLIC {clause}");
        // Any error is acceptable — a policy-compiler refusal or a parse
        // error are both fail-closed. What is not acceptable is Ok.
        admin(&ex, &sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("'{label}' compiled into an RLS policy: {sql}"));
    }

    // Rejection was inert: the good policy still applies, exactly.
    assert_eq!(visible_ids(&ex, sid).await, vec![1, 3]);
}

/// A setting NAME that merely contains `nucleus.tenant_id` is a different
/// setting: `nucleus.tenant_id_x` does not exist, and a policy naming it must
/// be rejected, not silently rewritten into equality with the REAL tenant
/// setting. The render-based matcher accepted any `current_setting(...)`
/// whose text contained the trusted substring.
#[tokio::test]
async fn a_near_miss_setting_name_is_rejected_not_rewritten() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    let sql = "CREATE POLICY bad_setting ON docs FOR SELECT TO PUBLIC \
               USING (owner = current_setting('nucleus.tenant_id_x'))";
    let err = admin(&ex, sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("a near-miss setting name compiled into tenant-equality policy"));
    assert!(
        err.to_string().to_ascii_lowercase().contains("unsupported"),
        "rejected for the wrong reason: {err}"
    );
    assert_eq!(visible_ids(&ex, sid).await, vec![1, 3]);
}

/// Concatenating anything onto the tenant setting changes what the policy
/// compares; the whole `current_setting(...) || 'x'` expression must be
/// rejected rather than matched on its leading tokens as plain tenant
/// equality.
#[tokio::test]
async fn a_composed_setting_expression_is_rejected_not_matched_on_its_prefix() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    let sql = "CREATE POLICY bad_concat ON docs FOR SELECT TO PUBLIC \
               USING (owner = current_setting('nucleus.tenant_id') || 'x')";
    let err = admin(&ex, sql).await.err().unwrap_or_else(|| {
        panic!("current_setting(...) || 'x' compiled into plain tenant equality")
    });
    assert!(
        err.to_string().to_ascii_lowercase().contains("unsupported"),
        "rejected for the wrong reason: {err}"
    );
    assert_eq!(visible_ids(&ex, sid).await, vec![1, 3]);
}

/// The same two traps through ALTER POLICY, which reuses the compiler: an
/// unsupported USING there must fail and leave the policy exactly as it was.
#[tokio::test]
async fn alter_policy_rejects_the_same_shapes_and_keeps_the_original() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    for (label, clause) in [
        ("near-miss setting", "nucleus.tenant_id_x"),
        ("composed setting", "nucleus.tenant_id') || 'x"),
    ] {
        let sql = format!(
            "ALTER POLICY owner_isolation ON docs \
             USING (owner = current_setting('{clause}'))"
        );
        admin(&ex, &sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("ALTER POLICY accepted the {label} shape"));
        // The original predicate survives untouched.
        assert_eq!(
            visible_ids(&ex, sid).await,
            vec![1, 3],
            "the {label} ALTER disturbed the live policy"
        );
    }
}

// ============================================================================
// Group B — NULL and type mismatches deny at evaluation, on the SQL path
// ============================================================================

/// A NULL guarded column denies every comparison form and `IS NULL` is the
/// only form that reads it — end to end through SQL, not just the predicate
/// unit tests in `security/mod.rs`.
#[tokio::test]
async fn null_columns_deny_comparisons_on_the_sql_path() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, owner TEXT, score INT)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO t VALUES (1, 'alice', 10), (2, NULL, 20), (3, 'alice', NULL)",
    )
    .await;
    exec(&ex, "CREATE ROLE alice LOGIN PASSWORD 'p'").await;
    exec(&ex, "GRANT SELECT ON t TO alice").await;
    exec(
        &ex,
        "CREATE POLICY eq ON t FOR SELECT TO PUBLIC USING (owner = 'alice')",
    )
    .await;
    exec(&ex, "ALTER TABLE t ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();

    // owner = 'alice' admits ids 1 and 3; the NULL owner row stays hidden.
    let out = ex
        .execute_with_session(sid, "SELECT id FROM t ORDER BY id")
        .await
        .unwrap();
    let got: Vec<i64> = rows(&out[0])
        .iter()
        .map(|r| {
            i64::from(match r.first() {
                Some(Value::Int32(n)) => *n,
                other => panic!("expected int, got {other:?}"),
            })
        })
        .collect();
    assert_eq!(got, vec![1, 3], "NULL owner row must not be admitted");

    // A numeric comparison over a NULL column denies the same way: id 3's
    // score is NULL so it stays hidden, while ids 1 (10) and 2 (20) pass
    // `score > 5` on merit — the policy reads score, not owner.
    exec(&ex, "DROP POLICY eq ON t").await;
    exec(
        &ex,
        "CREATE POLICY gt ON t FOR SELECT TO PUBLIC USING (score > 5)",
    )
    .await;
    let out = ex
        .execute_with_session(sid, "SELECT id FROM t ORDER BY id")
        .await
        .unwrap();
    let got: Vec<i64> = rows(&out[0])
        .iter()
        .map(|r| {
            i64::from(match r.first() {
                Some(Value::Int32(n)) => *n,
                other => panic!("expected int, got {other:?}"),
            })
        })
        .collect();
    assert_eq!(got, vec![1, 2], "NULL score row must not be admitted by >");

    // And the only form that reads NULL positively is IS NULL itself.
    exec(&ex, "DROP POLICY gt ON t").await;
    exec(
        &ex,
        "CREATE POLICY isn ON t FOR SELECT TO PUBLIC USING (owner IS NULL)",
    )
    .await;
    let out = ex
        .execute_with_session(sid, "SELECT id FROM t ORDER BY id")
        .await
        .unwrap();
    let got: Vec<i64> = rows(&out[0])
        .iter()
        .map(|r| {
            i64::from(match r.first() {
                Some(Value::Int32(n)) => *n,
                other => panic!("expected int, got {other:?}"),
            })
        })
        .collect();
    assert_eq!(got, vec![2], "IS NULL must see exactly the NULL owner row");
}

// ============================================================================
// Group C — protected specialty calls stay gated on the SQL path
// ============================================================================

/// Specialty stores carry no table-policy metadata, so under an active policy
/// the calls are refused for the RLS subject — including through the
/// schema-qualified spelling psql sends. Pure computations that merely share
/// a prefix stay available, so the gate cannot quietly widen.
#[tokio::test]
async fn specialty_calls_stay_refused_for_the_rls_subject() {
    let ex = test_executor();
    let sid = setup(&ex).await;

    for sql in [
        "SELECT KV_GET('k')",
        "SELECT pg_catalog.KV_SET('k', 'v')",
        "SELECT DOC_GET(1)",
        "SELECT GRAPH_QUERY('g', 'MATCH (n) RETURN n')",
        "SELECT RETENTION_CHECK()",
    ] {
        let err = ex
            .execute_with_session(sid, sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("specialty surface reachable under RLS via `{sql}`"));
        assert!(
            !err.to_string().to_ascii_lowercase().contains("leak"),
            "unexpected error shape for `{sql}`: {err}"
        );
    }

    // Control: a pure computation with a colliding prefix still works, so
    // this test fails if the guard is widened into removing plain SQL.
    let mut out = ex
        .execute_with_session(sid, "SELECT UPPER('fine')")
        .await
        .expect("pure computation must stay available under RLS");
    assert_eq!(text_of(out.remove(0)), "FINE");
}
