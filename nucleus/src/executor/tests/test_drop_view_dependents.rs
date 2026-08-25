//! CAT-11: DROP VIEW removed the view from `views` and stripped its name
//! from every dependency SET, but never refused while other views depended
//! on it (the dependent's stored SQL then selected a dropped name and only
//! failed at SELECT time) and never removed the now-empty dep KEY — which
//! made a future same-named TABLE's DROP guard refuse forever.

use super::*;

async fn two_level_views(ex: &Executor) {
    exec(ex, "CREATE TABLE cat11t (id INT)").await;
    exec(ex, "INSERT INTO cat11t VALUES (1), (2)").await;
    exec(ex, "CREATE VIEW v1 AS SELECT id FROM cat11t").await;
    exec(ex, "CREATE VIEW v2 AS SELECT id FROM v1").await;
    let r = exec(ex, "SELECT id FROM v2 ORDER BY id").await;
    assert_eq!(rows(&r[0]).len(), 2);
}

#[tokio::test]
async fn drop_view_with_dependent_view_is_refused() {
    let ex = test_executor();
    two_level_views(&ex).await;

    let err = ex
        .execute("DROP VIEW v1")
        .await
        .expect_err("DROP VIEW must refuse while another view depends on it");
    assert!(
        err.to_string().contains("v2"),
        "the error must name the dependent view, got: {err}"
    );

    // The refused drop must leave everything working.
    let r = exec(&ex, "SELECT id FROM v2 ORDER BY id").await;
    assert_eq!(rows(&r[0]).len(), 2);

    // Inner-first order still works.
    exec(&ex, "DROP VIEW v2").await;
    exec(&ex, "DROP VIEW v1").await;
}

#[tokio::test]
async fn drop_view_leaves_no_stale_dependency_key() {
    let ex = test_executor();
    two_level_views(&ex).await;
    exec(&ex, "DROP VIEW v2").await;
    exec(&ex, "DROP VIEW v1").await;

    // A table recreated under the dropped view's name must be droppable —
    // pre-fix the stale `v1` key in view_deps made DROP TABLE falsely refuse.
    exec(&ex, "CREATE TABLE v1 (id INT)").await;
    exec(&ex, "DROP TABLE v1").await;
}

#[tokio::test]
async fn drop_view_if_exists_missing_still_succeeds() {
    let ex = test_executor();
    exec(&ex, "DROP VIEW IF EXISTS cat11nope").await;
}
