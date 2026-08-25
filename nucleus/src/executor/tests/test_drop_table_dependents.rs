//! CAT-5: DROP TABLE checked view dependencies only. Dropping a parent
//! table bricked every child (each INSERT/DELETE hit TableNotFound inside
//! FK validation, with no hint), and matviews over the dropped table
//! silently stopped refreshing. PostgreSQL's default is RESTRICT.

use super::*;

#[tokio::test]
async fn drop_table_with_fk_dependents_is_refused() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cat5parent (id INT PRIMARY KEY)").await;
    exec(
        &ex,
        "CREATE TABLE cat5child (id INT, pid INT REFERENCES cat5parent(id))",
    )
    .await;
    exec(&ex, "INSERT INTO cat5parent VALUES (1)").await;
    exec(&ex, "INSERT INTO cat5child VALUES (10, 1)").await;

    let err = ex
        .execute("DROP TABLE cat5parent")
        .await
        .expect_err("DROP TABLE must refuse while a child FK references it");
    assert!(
        err.to_string().contains("cat5child"),
        "the error must name the dependent child, got: {err}"
    );

    // The failed drop must not have mutated anything: the child still
    // validates, and dropping the FK (via the child) unblocks the drop.
    exec(&ex, "INSERT INTO cat5child VALUES (11, 1)").await;
    exec(&ex, "DROP TABLE cat5child").await;
    exec(&ex, "DROP TABLE cat5parent").await;
}

#[tokio::test]
async fn drop_table_with_matview_dependent_is_refused() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cat5base (id INT, v INT)").await;
    exec(&ex, "INSERT INTO cat5base VALUES (1, 10)").await;
    exec(
        &ex,
        "CREATE MATERIALIZED VIEW cat5mv AS SELECT id FROM cat5base",
    )
    .await;

    let err = ex
        .execute("DROP TABLE cat5base")
        .await
        .expect_err("DROP TABLE must refuse while a matview depends on it");
    assert!(
        err.to_string().contains("cat5mv"),
        "the error must name the matview, got: {err}"
    );

    // Dropping the MV unblocks the drop.
    exec(&ex, "DROP MATERIALIZED VIEW cat5mv").await;
    exec(&ex, "DROP TABLE cat5base").await;
}
