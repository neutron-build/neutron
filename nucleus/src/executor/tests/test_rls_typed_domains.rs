//! NE-05 / NE-06: policy comparison semantics must match SQL's.
//!
//! NE-05 — the RLS evaluator compared rendered strings and guessed numeric
//! semantics whenever both sides parsed as numbers. On a TEXT column,
//! `code > '2'` admitted '10' (both parse as integers; SQL compares lexically
//! and denies); with a decimal boundary beyond 2^53, f64 rounding made
//! 9007199254740993 <= 9007199254740992.0 hold (SQL says otherwise). Domains
//! are now bound at CREATE POLICY time from the catalog column type: TEXT is
//! lexical, numerics compare exactly.
//!
//! NE-06 — a literal NULL in a policy compiled to AlwaysFalse, so
//! `USING (NOT NULL)` evaluated to TRUE and granted every row. SQL evaluates
//! NULL (and NOT NULL) to UNKNOWN, which never grants.

use super::*;

/// Harness: a table under RLS plus a limited role bound to a fresh session.
async fn rls_session(ex: &Executor, setup: &str, policy: &str) -> u64 {
    for stmt in setup.split(';').filter(|s| !s.trim().is_empty()) {
        exec(ex, stmt).await;
    }
    exec(
        ex,
        "CREATE ROLE viewer LOGIN PASSWORD 'p'",
    )
    .await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "viewer").await.unwrap();
    // Grant and policy are installed after the role exists; the session is
    // already bound so the policy applies to it from creation.
    exec(ex, "GRANT SELECT ON t TO viewer").await;
    exec(ex, policy).await;
    exec(ex, "ALTER TABLE t ENABLE ROW LEVEL SECURITY").await;
    sid
}

async fn visible(ex: &Executor, sid: u64, what: &str) -> Vec<String> {
    let res = ex
        .execute_with_session(sid, &format!("SELECT {what} FROM t ORDER BY 1"))
        .await
        .expect("select");
    rows(&res[0])
        .iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.clone(),
            Value::Int64(v) => v.to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Float64(v) => v.to_string(),
            Value::Numeric(n) => n.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// NE-05, TEXT domain: `code > '2'` must compare lexically — '10' is NOT
/// greater than '2' as text, whatever the numeric reading says.
#[tokio::test]
async fn text_comparison_is_lexical_not_numeric() {
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, code TEXT); INSERT INTO t VALUES (1,'2'),(2,'10'),(3,'9')",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (code > '2')",
    )
    .await;
    // Lexically: '9' > '2' admitted; '10' and '2' denied. The old numeric
    // heuristic admitted '10' — the fail-open direction.
    assert_eq!(
        visible(&ex, sid, "code").await,
        vec!["9".to_string()],
        "TEXT policy comparison must be lexical"
    );
}

/// NE-05, TEXT equality: `code = '10'` is string identity, and IN must not
/// reinterpret numeric-looking members.
#[tokio::test]
async fn text_equality_and_in_are_string_identity() {
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, code TEXT); INSERT INTO t VALUES (1,'010'),(2,'10')",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (code IN ('10'))",
    )
    .await;
    assert_eq!(
        visible(&ex, sid, "code").await,
        vec!["10".to_string()],
        "text IN must compare rendered strings"
    );
}

/// NE-05, exact numeric boundaries: a decimal bound beyond 2^53 must not
/// round through an f64 mantissa. SQL: 9007199254740993 > 9007199254740992.0.
#[tokio::test]
async fn numeric_boundary_beyond_double_mantissa_is_exact() {
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC); INSERT INTO t VALUES (1, 9007199254740992), (2, 9007199254740993)",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (n > 9007199254740992.0)",
    )
    .await;
    // Only 9007199254740993 satisfies n > 9007199254740992.0 (the bound row
    // is equal, not greater). The old f64 path rounded BOTH to 2^53, so the
    // comparison was Equal and NOTHING was admitted — fail-closed, but
    // wrongly. The more dangerous mirror (n <= bound admitting 9007199254740993)
    // is checked next.
    assert_eq!(
        visible(&ex, sid, "n").await,
        vec!["9007199254740993".to_string()],
        "exact decimal comparison beyond 2^53"
    );

    // Mirror: only 9007199254740992 satisfies n <= 9007199254740992.0; the
    // old f64 rounding made 9007199254740993 pass this bound — fail-open.
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC); INSERT INTO t VALUES (1, 9007199254740992), (2, 9007199254740993)",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (n <= 9007199254740992.0)",
    )
    .await;
    assert_eq!(
        visible(&ex, sid, "n").await,
        vec!["9007199254740992".to_string()],
        "f64 rounding must not admit a row beyond the exact bound"
    );
}

/// NE-05, decimal scales: 10.10 > 10.1 numerically, whatever the string
/// padding says.
#[tokio::test]
async fn numeric_scale_differences_compare_by_value() {
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC); INSERT INTO t VALUES (1, 10.1), (2, 10.10)",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (n = 10.10)",
    )
    .await;
    // Both rows equal 10.10 by value under exact decimal comparison.
    assert_eq!(
        visible(&ex, sid, "n").await.len(),
        2,
        "numeric equality compares values, not rendered padding"
    );
}

/// NE-06: NULL constants are UNKNOWN under SQL three-valued logic — including
/// under NOT — and never grant. Old behavior compiled NULL to AlwaysFalse, so
/// `NOT NULL` granted every row.
#[tokio::test]
async fn null_predicates_never_grant_even_under_not() {
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'a'),(2,'b')",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (NOT NULL)",
    )
    .await;
    assert!(
        visible(&ex, sid, "v").await.is_empty(),
        "USING (NOT NULL) must grant nothing"
    );

    // Bare NULL is unknown (denied before and after — control).
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'a'),(2,'b')",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (NULL)",
    )
    .await;
    assert!(
        visible(&ex, sid, "v").await.is_empty(),
        "USING (NULL) must grant nothing"
    );

    // TRUE OR NULL is TRUE — the allowed control.
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'a'),(2,'b')",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (true OR NULL)",
    )
    .await;
    assert_eq!(
        visible(&ex, sid, "v").await.len(),
        2,
        "TRUE OR NULL must grant"
    );

    // NOT (NULL OR FALSE) stays unknown: denied.
    let ex = test_executor();
    let sid = rls_session(
        &ex,
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'a'),(2,'b')",
        "CREATE POLICY p ON t FOR SELECT TO viewer USING (NOT (NULL OR FALSE))",
    )
    .await;
    assert!(
        visible(&ex, sid, "v").await.is_empty(),
        "NOT (NULL OR FALSE) must grant nothing"
    );
}
