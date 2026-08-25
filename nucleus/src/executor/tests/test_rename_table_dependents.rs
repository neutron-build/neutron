//! CAT-4: ALTER TABLE RENAME TO left four dependent stores keyed on the old
//! name untouched — incoming FK `ref_table` in other tables (every child
//! INSERT errored "table does not exist" inside FK validation forever), view
//! SQL text (a recreated same-named table silently rebound the view), the
//! `view_deps` DROP guard keys, and materialized-view `source_tables`/SQL.

use super::*;

#[tokio::test]
async fn rename_table_rewrites_incoming_foreign_keys() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE parent (id INT PRIMARY KEY)").await;
    exec(
        &ex,
        "CREATE TABLE child (id INT, pid INT REFERENCES parent(id))",
    )
    .await;
    exec(&ex, "INSERT INTO parent VALUES (1)").await;

    exec(&ex, "ALTER TABLE parent RENAME TO parent2").await;
    exec(&ex, "INSERT INTO child VALUES (10, 1)").await;

    // The FK must still VALIDATE against the renamed parent.
    let err = ex
        .execute("INSERT INTO child VALUES (11, 99)")
        .await
        .expect_err("FK must still reject a missing parent value after rename");
    assert!(
        err.to_string().to_lowercase().contains("foreign key"),
        "got: {err}"
    );
}

#[tokio::test]
async fn rename_table_rewrites_self_referential_fk() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE org (id INT PRIMARY KEY, boss INT REFERENCES org(id))",
    )
    .await;
    exec(&ex, "INSERT INTO org VALUES (1, NULL)").await;

    exec(&ex, "ALTER TABLE org RENAME TO org2").await;
    exec(&ex, "INSERT INTO org2 VALUES (2, 1)").await;
    let err = ex
        .execute("INSERT INTO org2 VALUES (3, 99)")
        .await
        .expect_err("self-FK must validate after the table's own rename");
    assert!(
        err.to_string().to_lowercase().contains("foreign key"),
        "got: {err}"
    );
}

#[tokio::test]
async fn rename_table_rebinds_view_and_blocks_decoy_rebind() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE src (id INT, v TEXT)").await;
    exec(&ex, "INSERT INTO src VALUES (1, 'one'), (2, 'two')").await;
    exec(&ex, "CREATE VIEW src_v AS SELECT id, v FROM src").await;
    let r = exec(&ex, "SELECT id FROM src_v ORDER BY id").await;
    assert_eq!(rows(&r[0]).len(), 2);

    exec(&ex, "ALTER TABLE src RENAME TO src2").await;
    let r = exec(&ex, "SELECT id FROM src_v ORDER BY id").await;
    assert_eq!(
        rows(&r[0]).len(),
        2,
        "the view must follow the renamed table"
    );

    // Recreate a DIFFERENT table under the old name: the view must not
    // silently rebind to it. Pre-fix the stored SQL still said `src`.
    exec(&ex, "CREATE TABLE src (id INT, v TEXT)").await;
    exec(&ex, "INSERT INTO src VALUES (9, 'decoy')").await;
    let r = exec(&ex, "SELECT id FROM src_v ORDER BY id").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(
        ids,
        vec![&Value::Int32(1), &Value::Int32(2)],
        "the view rebound to a decoy table recreated under the old name"
    );

    // The DROP guard must fire for the RENAMED table.
    let err = ex
        .execute("DROP TABLE src2")
        .await
        .expect_err("view dependency guard must follow the rename");
    assert!(err.to_string().contains("depend"), "got: {err}");
    // Dropping the decoy must be allowed (no stale dep key under 'src'
    // pointing at the view) — and dropping the view then src2 must work.
    exec(&ex, "DROP VIEW src_v").await;
    exec(&ex, "DROP TABLE src").await;
    exec(&ex, "DROP TABLE src2").await;
}

#[tokio::test]
async fn rename_table_keeps_matview_refreshable() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales (id INT, region TEXT, revenue INT)").await;
    exec(&ex, "INSERT INTO sales VALUES (1, 'east', 10)").await;
    exec(
        &ex,
        "CREATE MATERIALIZED VIEW mv_regions AS SELECT region, SUM(revenue) AS total FROM sales GROUP BY region",
    )
    .await;

    exec(&ex, "ALTER TABLE sales RENAME TO sales2").await;
    // New writes must refresh the MV through the renamed table.
    exec(&ex, "INSERT INTO sales2 VALUES (2, 'west', 5)").await;

    let r = exec(&ex, "SELECT region FROM mv_regions ORDER BY region").await;
    let regions: Vec<String> = rows(&r[0])
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            other => other.to_string(),
        })
        .collect();
    assert_eq!(
        regions,
        vec!["east".to_string(), "west".to_string()],
        "matview must refresh from the renamed base table — pre-fix the \
         write-time refresh silently failed and the MV went stale"
    );

    // And the DROP guard for the renamed table must name the matview.
    let err = ex
        .execute("DROP TABLE sales2")
        .await
        .expect_err("matview dependency must block dropping the renamed base");
    assert!(err.to_string().contains("depend"), "got: {err}");
}
