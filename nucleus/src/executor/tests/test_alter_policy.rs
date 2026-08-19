//! `ALTER POLICY`, and why DROP + CREATE is not the same thing.
//!
//! Before this, changing an RLS policy meant dropping it and creating it
//! again. On a live system that is the whole problem: between the two
//! statements the table is unprotected by that policy, so an operator
//! TIGHTENING a predicate has to briefly loosen it. The statement itself
//! errored with "statement type not yet supported", which at least failed
//! closed — but it left the only route to a policy change through a window
//! where rows escape.
//!
//! Introspection was already there and did not need building: `pg_policies`
//! and `pg_policy` are populated from the live engine.

use super::*;

async fn seeded() -> (Executor, u64) {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT)").await;
    exec(&ex, "INSERT INTO docs VALUES (1, 'ada'), (2, 'bob')").await;
    exec(&ex, "CREATE ROLE reader LOGIN PASSWORD 'p'").await;
    exec(&ex, "CREATE ROLE other LOGIN PASSWORD 'p'").await;
    exec(&ex, "GRANT SELECT ON docs TO reader").await;
    exec(
        &ex,
        "CREATE POLICY only_ada ON docs FOR SELECT TO reader USING (owner = 'ada')",
    )
    .await;
    exec(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "reader").await.unwrap();
    (ex, sid)
}

async fn visible(ex: &Executor, sid: u64) -> Vec<String> {
    let res = ex
        .execute_with_session(sid, "SELECT owner FROM docs ORDER BY id")
        .await
        .expect("select");
    rows(&res[0])
        .iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Changing the predicate takes effect, on a session that is already open.
#[tokio::test]
async fn altering_using_changes_what_a_live_session_sees() {
    let (ex, sid) = seeded().await;
    assert_eq!(visible(&ex, sid).await, vec!["ada"]);

    exec(&ex, "ALTER POLICY only_ada ON docs USING (owner = 'bob')").await;
    assert_eq!(
        visible(&ex, sid).await,
        vec!["bob"],
        "the new predicate did not take effect — a cached plan or result \
         outlived the policy change"
    );
}

/// A rename keeps the policy in force. The failure to avoid is a rename that
/// quietly drops it.
#[tokio::test]
async fn renaming_keeps_the_policy_in_force() {
    let (ex, sid) = seeded().await;
    exec(&ex, "ALTER POLICY only_ada ON docs RENAME TO ada_only").await;

    assert_eq!(
        visible(&ex, sid).await,
        vec!["ada"],
        "the renamed policy stopped applying"
    );
    let names = ex.with_visible_security(|s| {
        s.rls
            .all_policies()
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>()
    });
    assert_eq!(names, vec!["ada_only"], "the old name survived the rename");
}

/// Retargeting to another role removes it from the first one's path.
#[tokio::test]
async fn altering_to_retargets_the_policy() {
    let (ex, sid) = seeded().await;
    assert_eq!(visible(&ex, sid).await, vec!["ada"]);

    exec(&ex, "ALTER POLICY only_ada ON docs TO other").await;
    // `reader` is no longer targeted. With RLS enabled and no policy applying
    // to this principal, PostgreSQL semantics deny every row — the direction
    // that fails closed.
    assert!(
        visible(&ex, sid).await.is_empty(),
        "a policy retargeted away from this role must not keep applying to it"
    );
}

/// A failed ALTER leaves the original policy exactly as it was. This is the
/// property that makes ALTER better than DROP + CREATE: there is no window.
#[tokio::test]
async fn a_failed_alter_leaves_the_policy_untouched() {
    let (ex, sid) = seeded().await;
    for sql in [
        "ALTER POLICY only_ada ON docs USING (owner LIKE ANY (SELECT 1))",
        "ALTER POLICY only_ada ON docs TO ghost_role",
        "ALTER POLICY missing ON docs USING (owner = 'bob')",
        "ALTER POLICY only_ada ON no_such_table USING (owner = 'bob')",
    ] {
        let _ = ex
            .execute(sql)
            .await
            .expect_err(&format!("`{sql}` must be refused"));
        assert_eq!(
            visible(&ex, sid).await,
            vec!["ada"],
            "`{sql}` was refused but changed the policy anyway"
        );
    }
    // Renaming onto an existing name is refused too.
    exec(
        &ex,
        "CREATE POLICY second ON docs FOR SELECT TO other USING (owner = 'bob')",
    )
    .await;
    let err = ex
        .execute("ALTER POLICY only_ada ON docs RENAME TO second")
        .await
        .expect_err("a rename onto an existing policy name must be refused");
    assert!(err.to_string().contains("already exists"), "{err}");
}

/// An ALTER that says nothing is an error rather than a silent no-op.
#[tokio::test]
async fn an_empty_alter_is_refused() {
    let (ex, _sid) = seeded().await;
    let err = ex
        .execute("ALTER POLICY only_ada ON docs")
        .await
        .expect_err("ALTER POLICY with no operation must be refused");
    assert!(
        err.to_string().contains("requires") || err.to_string().contains("RENAME"),
        "{err}"
    );
}

/// Superuser-only, like the rest of policy DDL.
#[tokio::test]
async fn alter_policy_requires_superuser() {
    let (ex, sid) = seeded().await;
    let err = ex
        .execute_with_session(sid, "ALTER POLICY only_ada ON docs USING (owner = 'bob')")
        .await
        .expect_err("ALTER POLICY must require superuser authority");
    assert!(err.to_string().contains("superuser"), "{err}");
}

/// Introspection already existed and must show the altered state — otherwise
/// an operator checking their work reads the old policy.
#[tokio::test]
async fn pg_policies_reflects_the_alteration() {
    let (ex, _sid) = seeded().await;
    exec(&ex, "ALTER POLICY only_ada ON docs RENAME TO ada_only").await;
    let res = exec(&ex, "SELECT policyname FROM pg_policies").await;
    let names: Vec<String> = rows(&res[0])
        .iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    assert!(
        names.contains(&"ada_only".to_string()) && !names.contains(&"only_ada".to_string()),
        "pg_policies shows {names:?} after the rename"
    );
}
