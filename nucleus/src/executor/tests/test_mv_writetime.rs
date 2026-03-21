use super::*;

// ======================================================================
// Write-time materialized view refresh tests (Phase 3)
// ======================================================================

#[tokio::test]
async fn test_mv_writetime_basic() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE orders (id INT, product TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO orders VALUES (1, 'widget', 100)").await;

    exec(&ex, "CREATE MATERIALIZED VIEW mv_orders AS SELECT id, product, amount FROM orders").await;

    // MV should have the initial row
    let results = exec(&ex, "SELECT * FROM mv_orders").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);

    // Insert another row — MV should auto-refresh
    exec(&ex, "INSERT INTO orders VALUES (2, 'gadget', 200)").await;

    let results = exec(&ex, "SELECT * FROM mv_orders").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2, "MV should have 2 rows after write-time refresh");
    assert_eq!(r[1][1], Value::Text("gadget".into()));
}

#[tokio::test]
async fn test_mv_writetime_where_filter() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE events (id INT, kind TEXT, score INT)").await;
    exec(&ex, "INSERT INTO events VALUES (1, 'click', 10)").await;
    exec(&ex, "INSERT INTO events VALUES (2, 'view', 5)").await;

    // MV with WHERE — only clicks
    exec(&ex, "CREATE MATERIALIZED VIEW mv_clicks AS SELECT id, score FROM events WHERE kind = 'click'").await;

    let results = exec(&ex, "SELECT * FROM mv_clicks").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1, "MV should only have click events");

    // Insert a non-matching row
    exec(&ex, "INSERT INTO events VALUES (3, 'view', 3)").await;
    let results = exec(&ex, "SELECT * FROM mv_clicks").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1, "MV should still have 1 row (view events filtered out)");

    // Insert a matching row
    exec(&ex, "INSERT INTO events VALUES (4, 'click', 20)").await;
    let results = exec(&ex, "SELECT * FROM mv_clicks").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2, "MV should have 2 rows after matching insert");
}

#[tokio::test]
async fn test_mv_writetime_aggregation() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales (id INT, region TEXT, revenue INT)").await;
    exec(&ex, "INSERT INTO sales VALUES (1, 'east', 100)").await;
    exec(&ex, "INSERT INTO sales VALUES (2, 'west', 200)").await;
    exec(&ex, "INSERT INTO sales VALUES (3, 'east', 150)").await;

    exec(&ex, "CREATE MATERIALIZED VIEW mv_region_totals AS SELECT region, SUM(revenue) AS total FROM sales GROUP BY region").await;

    let results = exec(&ex, "SELECT * FROM mv_region_totals").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2, "Should have 2 regions");

    // Insert another east sale — aggregate should update
    exec(&ex, "INSERT INTO sales VALUES (4, 'east', 50)").await;

    let results = exec(&ex, "SELECT region, total FROM mv_region_totals").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2, "Still 2 regions");

    // Find the east row and verify sum
    let east_row = r.iter().find(|row| row[0] == Value::Text("east".into())).expect("east region");
    assert_eq!(east_row[1], Value::Int64(300), "east total should be 300 (100+150+50)");
}

#[tokio::test]
async fn test_mv_writetime_count() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (id INT, category TEXT)").await;
    exec(&ex, "INSERT INTO items VALUES (1, 'A')").await;
    exec(&ex, "INSERT INTO items VALUES (2, 'B')").await;

    exec(&ex, "CREATE MATERIALIZED VIEW mv_counts AS SELECT category, COUNT(*) AS cnt FROM items GROUP BY category").await;

    let results = exec(&ex, "SELECT * FROM mv_counts").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);

    exec(&ex, "INSERT INTO items VALUES (3, 'A')").await;

    let results = exec(&ex, "SELECT category, cnt FROM mv_counts").await;
    let r = rows(&results[0]);
    let a_row = r.iter().find(|row| row[0] == Value::Text("A".into())).expect("category A");
    assert_eq!(a_row[1], Value::Int64(2), "category A count should be 2");
}

#[tokio::test]
async fn test_mv_drop_cleanup() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE base (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO base VALUES (1, 'x')").await;

    exec(&ex, "CREATE MATERIALIZED VIEW mv_base AS SELECT id, val FROM base").await;

    // Verify MV works
    let results = exec(&ex, "SELECT * FROM mv_base").await;
    assert_eq!(rows(&results[0]).len(), 1);

    // Drop the MV
    let results = exec(&ex, "DROP MATERIALIZED VIEW mv_base").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "DROP MATERIALIZED VIEW"),
        _ => panic!("expected Command result"),
    }

    // Insert into base should not error (no dangling MV deps)
    exec(&ex, "INSERT INTO base VALUES (2, 'y')").await;

    // Querying the dropped MV should fail or return no results
    let result = ex.execute("SELECT * FROM mv_base").await;
    assert!(result.is_err(), "querying dropped MV should fail");
}

#[tokio::test]
async fn test_mv_drop_if_exists() {
    let ex = test_executor();

    // DROP IF EXISTS on non-existent MV should not error
    let results = exec(&ex, "DROP MATERIALIZED VIEW IF EXISTS nonexistent_mv").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "DROP MATERIALIZED VIEW"),
        _ => panic!("expected Command result"),
    }
}

#[tokio::test]
async fn test_mv_multiple_on_same_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE data (id INT, category TEXT, value INT)").await;
    exec(&ex, "INSERT INTO data VALUES (1, 'A', 10)").await;
    exec(&ex, "INSERT INTO data VALUES (2, 'B', 20)").await;

    // Two MVs on the same base table
    exec(&ex, "CREATE MATERIALIZED VIEW mv_all AS SELECT id, category, value FROM data").await;
    exec(&ex, "CREATE MATERIALIZED VIEW mv_sum AS SELECT category, SUM(value) AS total FROM data GROUP BY category").await;

    // Insert a new row — both MVs should update
    exec(&ex, "INSERT INTO data VALUES (3, 'A', 30)").await;

    let results = exec(&ex, "SELECT * FROM mv_all").await;
    assert_eq!(rows(&results[0]).len(), 3, "mv_all should have 3 rows");

    let results = exec(&ex, "SELECT category, total FROM mv_sum").await;
    let r = rows(&results[0]);
    let a_row = r.iter().find(|row| row[0] == Value::Text("A".into())).expect("category A");
    assert_eq!(a_row[1], Value::Int64(40), "A total should be 40 (10+30)");
}

#[tokio::test]
async fn test_mv_no_overhead_on_unrelated_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE tracked (id INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE untracked (id INT, val TEXT)").await;

    exec(&ex, "CREATE MATERIALIZED VIEW mv_tracked AS SELECT id, val FROM tracked").await;

    // Insert into untracked table — should succeed with no MV overhead
    exec(&ex, "INSERT INTO untracked VALUES (1, 'hello')").await;

    // MV should still have 0 rows (only tracks 'tracked' table)
    let results = exec(&ex, "SELECT * FROM mv_tracked").await;
    assert_eq!(rows(&results[0]).len(), 0, "MV should have 0 rows");

    // Insert into tracked table — MV should update
    exec(&ex, "INSERT INTO tracked VALUES (1, 'world')").await;
    let results = exec(&ex, "SELECT * FROM mv_tracked").await;
    assert_eq!(rows(&results[0]).len(), 1, "MV should have 1 row");
}
