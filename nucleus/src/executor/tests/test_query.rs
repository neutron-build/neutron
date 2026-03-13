use super::*;

// ======================================================================
// Subquery tests
// ======================================================================

#[tokio::test]
async fn test_scalar_subquery() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nums (val INT)").await;
    exec(&ex, "INSERT INTO nums VALUES (10)").await;
    exec(&ex, "INSERT INTO nums VALUES (20)").await;
    exec(&ex, "INSERT INTO nums VALUES (30)").await;

    let results = exec(&ex, "SELECT (SELECT MAX(val) FROM nums)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(30));
}

#[tokio::test]
async fn test_exists_subquery() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items2 (id INT)").await;
    exec(&ex, "INSERT INTO items2 VALUES (1)").await;

    let results = exec(&ex, "SELECT EXISTS (SELECT 1 FROM items2)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));

    let results = exec(
        &ex,
        "SELECT NOT EXISTS (SELECT 1 FROM items2 WHERE id = 999)",
    )
    .await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));
}

#[tokio::test]
async fn test_in_subquery() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE dept (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO dept VALUES (1, 'engineering')").await;
    exec(&ex, "INSERT INTO dept VALUES (2, 'sales')").await;

    exec(&ex, "CREATE TABLE emp (name TEXT, dept_id INT)").await;
    exec(&ex, "INSERT INTO emp VALUES ('Alice', 1)").await;
    exec(&ex, "INSERT INTO emp VALUES ('Bob', 2)").await;
    exec(&ex, "INSERT INTO emp VALUES ('Charlie', 3)").await;

    let results = exec(
        &ex,
        "SELECT name FROM emp WHERE dept_id IN (SELECT id FROM dept)",
    )
    .await;
    assert_eq!(rows(&results[0]).len(), 2);
}

#[tokio::test]
async fn test_subquery_in_from() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE data (x INT)").await;
    exec(&ex, "INSERT INTO data VALUES (1)").await;
    exec(&ex, "INSERT INTO data VALUES (2)").await;
    exec(&ex, "INSERT INTO data VALUES (3)").await;

    let results = exec(
        &ex,
        "SELECT sub.x FROM (SELECT x FROM data WHERE x > 1) AS sub",
    )
    .await;
    assert_eq!(rows(&results[0]).len(), 2);
}


// ======================================================================
// CTE tests
// ======================================================================

#[tokio::test]
async fn test_cte_basic() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE orders (product TEXT, qty INT)").await;
    exec(&ex, "INSERT INTO orders VALUES ('A', 10)").await;
    exec(&ex, "INSERT INTO orders VALUES ('B', 20)").await;
    exec(&ex, "INSERT INTO orders VALUES ('A', 30)").await;

    let results = exec(
        &ex,
        "WITH totals AS (SELECT product, SUM(qty) AS total FROM orders GROUP BY product) SELECT product, total FROM totals ORDER BY product",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("A".into()));
}

// ======================================================================

// UNION / INTERSECT / EXCEPT tests
// ======================================================================

#[tokio::test]
async fn test_union_all() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t1 (x INT)").await;
    exec(&ex, "INSERT INTO t1 VALUES (1)").await;
    exec(&ex, "INSERT INTO t1 VALUES (2)").await;

    exec(&ex, "CREATE TABLE t2 (x INT)").await;
    exec(&ex, "INSERT INTO t2 VALUES (2)").await;
    exec(&ex, "INSERT INTO t2 VALUES (3)").await;

    let results = exec(
        &ex,
        "SELECT x FROM t1 UNION ALL SELECT x FROM t2",
    )
    .await;
    assert_eq!(rows(&results[0]).len(), 4);
}

#[tokio::test]
async fn test_union_distinct() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE u1 (x INT)").await;
    exec(&ex, "INSERT INTO u1 VALUES (1)").await;
    exec(&ex, "INSERT INTO u1 VALUES (2)").await;

    exec(&ex, "CREATE TABLE u2 (x INT)").await;
    exec(&ex, "INSERT INTO u2 VALUES (2)").await;
    exec(&ex, "INSERT INTO u2 VALUES (3)").await;

    let results = exec(
        &ex,
        "SELECT x FROM u1 UNION SELECT x FROM u2",
    )
    .await;
    assert_eq!(rows(&results[0]).len(), 3); // 1, 2, 3
}

#[tokio::test]
async fn test_intersect() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE i1 (x INT)").await;
    exec(&ex, "INSERT INTO i1 VALUES (1)").await;
    exec(&ex, "INSERT INTO i1 VALUES (2)").await;

    exec(&ex, "CREATE TABLE i2 (x INT)").await;
    exec(&ex, "INSERT INTO i2 VALUES (2)").await;
    exec(&ex, "INSERT INTO i2 VALUES (3)").await;

    let results = exec(
        &ex,
        "SELECT x FROM i1 INTERSECT SELECT x FROM i2",
    )
    .await;
    assert_eq!(rows(&results[0]).len(), 1); // just 2
    assert_eq!(rows(&results[0])[0][0], Value::Int32(2));
}

#[tokio::test]
async fn test_except() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE e1 (x INT)").await;
    exec(&ex, "INSERT INTO e1 VALUES (1)").await;
    exec(&ex, "INSERT INTO e1 VALUES (2)").await;

    exec(&ex, "CREATE TABLE e2 (x INT)").await;
    exec(&ex, "INSERT INTO e2 VALUES (2)").await;
    exec(&ex, "INSERT INTO e2 VALUES (3)").await;

    let results = exec(
        &ex,
        "SELECT x FROM e1 EXCEPT SELECT x FROM e2",
    )
    .await;
    assert_eq!(rows(&results[0]).len(), 1); // just 1
    assert_eq!(rows(&results[0])[0][0], Value::Int32(1));
}

// ======================================================================

// Window function tests
// ======================================================================

#[tokio::test]
async fn test_row_number() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ranked (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO ranked VALUES ('Alice', 90)").await;
    exec(&ex, "INSERT INTO ranked VALUES ('Bob', 80)").await;
    exec(&ex, "INSERT INTO ranked VALUES ('Charlie', 70)").await;

    let results = exec(
        &ex,
        "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM ranked",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Int64(1)); // Alice (90)
    assert_eq!(r[1][1], Value::Int64(2)); // Bob (80)
    assert_eq!(r[2][1], Value::Int64(3)); // Charlie (70)
}

#[tokio::test]
async fn test_rank_with_ties() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ranked2 (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO ranked2 VALUES ('A', 90)").await;
    exec(&ex, "INSERT INTO ranked2 VALUES ('B', 90)").await;
    exec(&ex, "INSERT INTO ranked2 VALUES ('C', 80)").await;

    let results = exec(
        &ex,
        "SELECT name, RANK() OVER (ORDER BY score DESC) FROM ranked2",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int64(1));
    assert_eq!(r[1][1], Value::Int64(1)); // tie
    assert_eq!(r[2][1], Value::Int64(3)); // gap
}

#[tokio::test]
async fn test_lag_lead() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE seq (val INT)").await;
    exec(&ex, "INSERT INTO seq VALUES (10)").await;
    exec(&ex, "INSERT INTO seq VALUES (20)").await;
    exec(&ex, "INSERT INTO seq VALUES (30)").await;

    let results = exec(
        &ex,
        "SELECT val, LAG(val) OVER (ORDER BY val), LEAD(val) OVER (ORDER BY val) FROM seq",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Null);       // LAG of first
    assert_eq!(r[1][1], Value::Int32(10));   // LAG of second
    assert_eq!(r[1][2], Value::Int32(30));   // LEAD of second
    assert_eq!(r[2][2], Value::Null);        // LEAD of last
}

#[tokio::test]
async fn test_sum_over() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE running (val INT)").await;
    exec(&ex, "INSERT INTO running VALUES (1)").await;
    exec(&ex, "INSERT INTO running VALUES (2)").await;
    exec(&ex, "INSERT INTO running VALUES (3)").await;

    let results = exec(
        &ex,
        "SELECT val, SUM(val) OVER (ORDER BY val) FROM running",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Float64(1.0));  // running sum: 1
    assert_eq!(r[1][1], Value::Float64(3.0));  // running sum: 1+2
    assert_eq!(r[2][1], Value::Float64(6.0));  // running sum: 1+2+3
}

// ======================================================================

// FULL OUTER JOIN test
// ======================================================================

#[tokio::test]
async fn test_full_outer_join() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE left_t (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO left_t VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO left_t VALUES (2, 'b')").await;

    exec(&ex, "CREATE TABLE right_t (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO right_t VALUES (2, 'x')").await;
    exec(&ex, "INSERT INTO right_t VALUES (3, 'y')").await;

    let results = exec(
        &ex,
        "SELECT left_t.id, right_t.id FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id ORDER BY 1",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3); // (1,NULL), (2,2), (NULL,3)
}

// ======================================================================

// ======================================================================
// VALUES clause test
// ======================================================================

#[tokio::test]
async fn test_values_clause() {
    let ex = test_executor();
    let results = exec(&ex, "VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
    assert_eq!(rows(&results[0]).len(), 3);
    assert_eq!(rows(&results[0])[0][0], Value::Int32(1));
}

// ======================================================================

// EXPLAIN tests
// ======================================================================

/// Helper: join all EXPLAIN output rows into a single string.
fn plan_text(result: &ExecResult) -> String {
    let r = rows(result);
    r.iter().map(|row| row[0].to_string()).collect::<Vec<_>>().join("\n")
}

#[tokio::test]
async fn test_explain_basic_scan() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE expl (id INT, name TEXT)").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM expl").await;
    let text = plan_text(&results[0]);
    // Should show Seq Scan with the table name
    assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
    assert!(text.contains("expl"), "expected table name 'expl' in: {text}");
    assert!(text.contains("rows="), "expected row estimate in: {text}");
    // Column name should be QUERY PLAN
    if let ExecResult::Select { columns, .. } = &results[0] {
        assert_eq!(columns[0].0, "QUERY PLAN");
    } else {
        panic!("expected SELECT result");
    }
}

#[tokio::test]
async fn test_explain_with_filter() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE users_expl (id INT, age INT, name TEXT)").await;
    exec(&ex, "INSERT INTO users_expl VALUES (1, 25, 'alice')").await;
    exec(&ex, "INSERT INTO users_expl VALUES (2, 17, 'bob')").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM users_expl WHERE age > 18").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
    assert!(text.contains("users_expl"), "expected table name in: {text}");
    assert!(text.contains("age > 18") || text.contains("Filter"), "expected filter info in: {text}");
}

#[tokio::test]
async fn test_explain_with_join() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE orders_expl (id INT, user_id INT, amount INT)").await;
    exec(&ex, "CREATE TABLE customers_expl (id INT, name TEXT)").await;
    let results = exec(
        &ex,
        "EXPLAIN SELECT * FROM orders_expl JOIN customers_expl ON orders_expl.id = customers_expl.id",
    ).await;
    let text = plan_text(&results[0]);
    // Should contain a join node and both table names
    assert!(
        text.contains("Join") || text.contains("Loop"),
        "expected join node in: {text}"
    );
    assert!(text.contains("orders_expl"), "expected 'orders_expl' in: {text}");
    assert!(text.contains("customers_expl"), "expected 'customers_expl' in: {text}");
}

#[tokio::test]
async fn test_explain_with_sort() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sorted_expl (id INT, val INT)").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM sorted_expl ORDER BY val DESC").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("Sort"), "expected 'Sort' in: {text}");
    assert!(text.contains("Scan"), "expected scan node in: {text}");
}

#[tokio::test]
async fn test_explain_with_aggregate() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales_expl (id INT, amount INT)").await;
    let results = exec(&ex, "EXPLAIN SELECT SUM(amount) FROM sales_expl").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("Aggregate"), "expected 'Aggregate' in: {text}");
    assert!(text.contains("SUM"), "expected 'SUM' in: {text}");
    assert!(text.contains("Scan"), "expected scan node under aggregate in: {text}");
}

#[tokio::test]
async fn test_explain_with_group_by() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE grouped_expl (category TEXT, amount INT)").await;
    let results = exec(&ex, "EXPLAIN SELECT category, COUNT(*) FROM grouped_expl GROUP BY category").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("HashAggregate"), "expected 'HashAggregate' in: {text}");
    assert!(text.contains("Group Key"), "expected 'Group Key' in: {text}");
    assert!(text.contains("category"), "expected 'category' in group key in: {text}");
}

#[tokio::test]
async fn test_explain_analyze() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE analyze_expl (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO analyze_expl VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO analyze_expl VALUES (2, 'b')").await;
    exec(&ex, "INSERT INTO analyze_expl VALUES (3, 'c')").await;
    let results = exec(&ex, "EXPLAIN ANALYZE SELECT * FROM analyze_expl").await;
    let text = plan_text(&results[0]);
    // EXPLAIN ANALYZE should show the plan plus actual execution stats
    assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
    assert!(text.contains("Actual Rows: 3"), "expected 'Actual Rows: 3' in: {text}");
    assert!(text.contains("Execution Time"), "expected 'Execution Time' in: {text}");
    assert!(text.contains("ms"), "expected time unit 'ms' in: {text}");
}

#[tokio::test]
async fn test_explain_with_limit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lim_expl (id INT)").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM lim_expl LIMIT 10").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("Limit"), "expected 'Limit' in: {text}");
    assert!(text.contains("limit=10"), "expected 'limit=10' in: {text}");
}

// ======================================================================

// Planner integration tests
// ======================================================================

#[tokio::test]
async fn test_analyze_feeds_explain_stats() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_stats (id INT, name TEXT)").await;
    for i in 0..50 {
        exec(&ex, &format!("INSERT INTO plan_stats VALUES ({i}, 'n{i}')")).await;
    }
    // ANALYZE collects real stats
    exec(&ex, "ANALYZE plan_stats").await;
    // EXPLAIN should use those stats (50 rows, not default 1000)
    let results = exec(&ex, "EXPLAIN SELECT * FROM plan_stats").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("Seq Scan"), "expected Seq Scan in: {text}");
    assert!(text.contains("rows=50"), "expected 'rows=50' in: {text}");
}

#[tokio::test]
async fn test_explain_shows_index_scan_with_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_idx (id INT, val TEXT)").await;
    // Insert enough rows that index scan is preferred by cost model
    for i in 0..200 {
        exec(&ex, &format!("INSERT INTO plan_idx VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_plan_id ON plan_idx (id)").await;
    exec(&ex, "ANALYZE plan_idx").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM plan_idx WHERE id = 42").await;
    let text = plan_text(&results[0]);
    // With 200 rows and a B-tree index, the planner should choose IndexScan
    assert!(text.contains("Index Scan"), "expected 'Index Scan' in: {text}");
    assert!(text.contains("idx_plan_id"), "expected index name in: {text}");
}

#[tokio::test]
async fn test_explain_seq_scan_without_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_noidx (id INT, val TEXT)").await;
    for i in 0..50 {
        exec(&ex, &format!("INSERT INTO plan_noidx VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "ANALYZE plan_noidx").await;
    // No index: should show Seq Scan even with WHERE
    let results = exec(&ex, "EXPLAIN SELECT * FROM plan_noidx WHERE id = 10").await;
    let text = plan_text(&results[0]);
    assert!(text.contains("Seq Scan"), "expected 'Seq Scan' in: {text}");
}

#[tokio::test]
async fn test_explain_index_scan_analyze_stats_accuracy() {
    // Verify ANALYZE stats produce accurate cost estimates.
    // With 500 rows and a unique-ish column, index scan for a point lookup
    // is cheaper than scanning 5 pages sequentially.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_acc (id INT, val TEXT)").await;
    for i in 0..500 {
        exec(&ex, &format!("INSERT INTO plan_acc VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_plan_acc ON plan_acc (id)").await;
    exec(&ex, "ANALYZE plan_acc").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM plan_acc WHERE id = 42").await;
    let text = plan_text(&results[0]);
    // 500 rows → 5 pages, seq scan cost ≈10. Index scan for 1 row ≈3.3.
    assert!(text.contains("Index Scan"), "expected 'Index Scan' in: {text}");
}

#[tokio::test]
async fn test_explain_analyze_with_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_ea (id INT, val TEXT)").await;
    // Need enough rows that index scan is cheaper than seq scan (page_count > 2)
    for i in 0..500 {
        exec(&ex, &format!("INSERT INTO plan_ea VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_plan_ea ON plan_ea (id)").await;
    exec(&ex, "ANALYZE plan_ea").await;
    let results = exec(&ex, "EXPLAIN ANALYZE SELECT * FROM plan_ea WHERE id = 50").await;
    let text = plan_text(&results[0]);
    // EXPLAIN ANALYZE should show the plan (Index Scan) + actual execution stats
    assert!(text.contains("Index Scan"), "expected 'Index Scan' in: {text}");
    assert!(text.contains("Actual Rows"), "expected actual rows in: {text}");
    assert!(text.contains("Execution Time"), "expected execution time in: {text}");
}

#[tokio::test]
async fn test_explain_join_uses_shared_stats() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_ord (id INT, cust_id INT)").await;
    exec(&ex, "CREATE TABLE plan_cust (id INT, name TEXT)").await;
    for i in 0..50 {
        exec(&ex, &format!("INSERT INTO plan_ord VALUES ({i}, {i})")).await;
    }
    for i in 0..10 {
        exec(&ex, &format!("INSERT INTO plan_cust VALUES ({i}, 'c{i}')")).await;
    }
    exec(&ex, "ANALYZE plan_ord").await;
    exec(&ex, "ANALYZE plan_cust").await;
    let results = exec(&ex, "EXPLAIN SELECT * FROM plan_ord JOIN plan_cust ON plan_ord.cust_id = plan_cust.id").await;
    let text = plan_text(&results[0]);
    // Should show a join node with both tables
    assert!(text.contains("Join"), "expected Join in: {text}");
    assert!(text.contains("plan_ord") || text.contains("plan_cust"),
        "expected table name in: {text}");
}

// ======================================================================

// Plan-driven execution tests (opt-in via SET plan_execution = on)
// ======================================================================

#[tokio::test]
async fn test_plan_exec_simple_select() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO pe_t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO pe_t VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO pe_t VALUES (3, 'charlie')").await;
    exec(&ex, "SET plan_execution = on").await;
    // Simple SELECT *
    let results = exec(&ex, "SELECT * FROM pe_t").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    // SELECT with WHERE
    let results = exec(&ex, "SELECT * FROM pe_t WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("bob".into()));
}

#[tokio::test]
async fn test_plan_exec_order_by_limit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_sort (id INT, val TEXT)").await;
    for i in 0..10 {
        exec(&ex, &format!("INSERT INTO pe_sort VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "SET plan_execution = on").await;
    let results = exec(&ex, "SELECT * FROM pe_sort ORDER BY id DESC LIMIT 3").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(9));
    assert_eq!(r[1][0], Value::Int32(8));
    assert_eq!(r[2][0], Value::Int32(7));
}

#[tokio::test]
async fn test_plan_exec_projection() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_proj (id INT, name TEXT, age INT)").await;
    exec(&ex, "INSERT INTO pe_proj VALUES (1, 'alice', 30)").await;
    exec(&ex, "SET plan_execution = on").await;
    let results = exec(&ex, "SELECT name, age FROM pe_proj").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("alice".into()));
    assert_eq!(r[0][1], Value::Int32(30));
}

#[tokio::test]
async fn test_plan_exec_falls_back_for_aggregates() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_agg (id INT, val INT)").await;
    exec(&ex, "INSERT INTO pe_agg VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO pe_agg VALUES (2, 20)").await;
    exec(&ex, "SET plan_execution = on").await;
    // Aggregates should fall back to AST path and still work
    let results = exec(&ex, "SELECT COUNT(*) FROM pe_agg").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int64(2));
}

#[tokio::test]
async fn test_plan_exec_falls_back_for_like() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_like (name TEXT)").await;
    exec(&ex, "INSERT INTO pe_like VALUES ('alice')").await;
    exec(&ex, "INSERT INTO pe_like VALUES ('bob')").await;
    exec(&ex, "INSERT INTO pe_like VALUES ('abby')").await;
    exec(&ex, "SET plan_execution = on").await;
    // LIKE should fall back to AST path and still work
    let results = exec(&ex, "SELECT name FROM pe_like WHERE name LIKE 'a%'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_plan_exec_between_predicate() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_between (id INT, val TEXT)").await;
    for i in 1..=5 {
        exec(&ex, &format!("INSERT INTO pe_between VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "SET plan_execution = on").await;
    let results = exec(&ex, "SELECT id FROM pe_between WHERE id BETWEEN 2 AND 4 ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(2));
    assert_eq!(r[1][0], Value::Int32(3));
    assert_eq!(r[2][0], Value::Int32(4));
}

#[tokio::test]
async fn test_plan_exec_group_by_with_qualified_columns() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_orders (id INT, amount INT, category INT, day_key INT)").await;
    exec(&ex, "INSERT INTO pe_orders VALUES (1, 100, 1, 2), (2, 150, 1, 4), (3, 75, 2, 3), (4, 40, 1, 8)").await;
    exec(&ex, "SET plan_execution = on").await;
    let sql = "SELECT o.category, COUNT(*), SUM(o.amount) \
               FROM pe_orders o \
               WHERE o.day_key BETWEEN 1 AND 5 \
               GROUP BY o.category ORDER BY o.category";
    let results = exec(&ex, sql).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Int64(2));
    assert_eq!(r[0][2], Value::Int64(250)); // SUM of INT columns → Int64
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[1][1], Value::Int64(1));
    assert_eq!(r[1][2], Value::Int64(75)); // SUM of INT columns → Int64
}

#[tokio::test]
async fn test_plan_exec_join_with_qualified_where_filters() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_join_accounts (id INT, region INT)").await;
    exec(&ex, "CREATE TABLE pe_join_orders (id INT, account_id INT, amount INT, day_key INT)").await;
    exec(&ex, "INSERT INTO pe_join_accounts VALUES (1, 1), (2, 2), (3, 2)").await;
    exec(&ex, "INSERT INTO pe_join_orders VALUES (1, 1, 100, 2), (2, 2, 50, 3), (3, 3, 30, 4), (4, 3, 90, 9)").await;
    exec(&ex, "SET plan_execution = on").await;
    let results = exec(
        &ex,
        "SELECT o.id, a.region FROM pe_join_orders o JOIN pe_join_accounts a ON a.id = o.account_id WHERE a.region = 2 AND o.day_key BETWEEN 1 AND 5 ORDER BY o.id",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(2));
    assert_eq!(r[0][1], Value::Int32(2));
    assert_eq!(r[1][0], Value::Int32(3));
    assert_eq!(r[1][1], Value::Int32(2));
}

#[tokio::test]
async fn test_ast_join_with_qualified_where_filters() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ast_join_accounts (id INT, region INT)").await;
    exec(&ex, "CREATE TABLE ast_join_orders (id INT, account_id INT, amount INT, day_key INT)").await;
    exec(&ex, "INSERT INTO ast_join_accounts VALUES (1, 1), (2, 2), (3, 2)").await;
    exec(&ex, "INSERT INTO ast_join_orders VALUES (1, 1, 100, 2), (2, 2, 50, 3), (3, 3, 30, 4), (4, 3, 90, 9)").await;
    // plan_execution is OFF by default; this validates AST path behavior.
    let results = exec(
        &ex,
        "SELECT o.id, a.region FROM ast_join_orders o JOIN ast_join_accounts a ON a.id = o.account_id WHERE a.region = 2 AND o.day_key BETWEEN 1 AND 5 ORDER BY o.id",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(2));
    assert_eq!(r[0][1], Value::Int32(2));
    assert_eq!(r[1][0], Value::Int32(3));
    assert_eq!(r[1][1], Value::Int32(2));
}

#[tokio::test]
async fn test_plan_exec_having_count_function() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pe_having (category INT, amount INT)").await;
    exec(&ex, "INSERT INTO pe_having VALUES (1, 10), (1, 20), (2, 5)").await;
    exec(&ex, "SET plan_execution = on").await;
    let results = exec(
        &ex,
        "SELECT category, COUNT(*) FROM pe_having GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Int64(2));
}

// ======================================================================

// ======================================================================
// Aggregate plan execution tests
// ======================================================================

#[tokio::test]
async fn test_aggregate_plan_execution() {
    // Test the Aggregate plan node (no GROUP BY) by constructing one directly
    use crate::planner::{PlanNode, Cost};
    let ex = test_executor();
    // Populate table
    exec(&ex, "CREATE TABLE agg_plan (val INT)").await;
    for i in 1..=5 {
        exec(&ex, &format!("INSERT INTO agg_plan VALUES ({i})")).await;
    }
    // Build plan manually: SeqScan → Aggregate [COUNT(*), SUM(val)]
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::SeqScan {
            table: "agg_plan".into(),
            estimated_rows: 5,
            estimated_cost: Cost(1.0),
            filter: None,
            filter_expr: None,
            scan_limit: None,
            projection: None,
        }),
        aggregates: vec!["COUNT(*)".into(), "SUM(val)".into()],
        estimated_cost: Cost(2.0),
    };
    let cte_tables = std::collections::HashMap::new();
    let result = ex.execute_plan_node(&plan, &cte_tables).await;
    assert!(result.is_ok(), "aggregate plan should succeed: {result:?}");
    let (meta, rows) = result.unwrap();
    assert_eq!(rows.len(), 1, "aggregate should return 1 row");
    assert_eq!(meta.len(), 2);
    assert_eq!(rows[0][0], Value::Int64(5)); // COUNT(*)
    assert_eq!(rows[0][1], Value::Int64(15)); // SUM(1+2+3+4+5) — Int input → Int result
}

#[tokio::test]
async fn test_hash_aggregate_plan_execution() {
    use crate::planner::{PlanNode, Cost};
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hagg (grp TEXT, val INT)").await;
    exec(&ex, "INSERT INTO hagg VALUES ('a', 10)").await;
    exec(&ex, "INSERT INTO hagg VALUES ('a', 20)").await;
    exec(&ex, "INSERT INTO hagg VALUES ('b', 30)").await;
    exec(&ex, "INSERT INTO hagg VALUES ('b', 40)").await;
    exec(&ex, "INSERT INTO hagg VALUES ('b', 50)").await;
    // Build plan: SeqScan → HashAggregate [GROUP BY grp, COUNT(*), SUM(val)]
    let plan = PlanNode::HashAggregate {
        input: Box::new(PlanNode::SeqScan {
            table: "hagg".into(),
            estimated_rows: 5,
            estimated_cost: Cost(1.0),
            filter: None,
            filter_expr: None,
            scan_limit: None,
            projection: None,
        }),
        group_keys: vec!["grp".into()],
        aggregates: vec!["COUNT(*)".into(), "SUM(val)".into()],
        estimated_rows: 2,
        estimated_cost: Cost(2.0),
    };
    let cte_tables = std::collections::HashMap::new();
    let result = ex.execute_plan_node(&plan, &cte_tables).await;
    assert!(result.is_ok(), "hash aggregate plan should succeed: {result:?}");
    let (meta, rows) = result.unwrap();
    assert_eq!(rows.len(), 2, "should have 2 groups");
    assert_eq!(meta.len(), 3); // grp, COUNT(*), SUM(val)
    // Find group 'a' and 'b'
    for row in &rows {
        match &row[0] {
            Value::Text(s) if s == "a" => {
                assert_eq!(row[1], Value::Int64(2));
                assert_eq!(row[2], Value::Int64(30)); // SUM of int → Int64
            }
            Value::Text(s) if s == "b" => {
                assert_eq!(row[1], Value::Int64(3));
                assert_eq!(row[2], Value::Int64(120)); // SUM of int → Int64
            }
            other => panic!("unexpected group key: {other:?}"),
        }
    }
}

// ======================================================================

// Plan-driven execution alignment test (AST-path — plan_execution off)
// ======================================================================

#[tokio::test]
async fn test_plan_driven_index_scan_matches_execution() {
    // Verify that when EXPLAIN says IndexScan, the executor actually uses the index
    // (i.e., the plan matches actual execution behavior)
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_exec (id INT, val TEXT)").await;
    for i in 0..500 {
        exec(&ex, &format!("INSERT INTO plan_exec VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_plan_exec ON plan_exec (id)").await;
    exec(&ex, "ANALYZE plan_exec").await;

    // EXPLAIN should show Index Scan
    let plan_results = exec(&ex, "EXPLAIN SELECT * FROM plan_exec WHERE id = 42").await;
    let text = plan_text(&plan_results[0]);
    assert!(text.contains("Index Scan"), "planner chose: {text}");

    // Actual execution should return the correct row
    let exec_results = exec(&ex, "SELECT * FROM plan_exec WHERE id = 42").await;
    let r = rows(&exec_results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("v42".into()));
}

#[tokio::test]
async fn test_plan_driven_hash_join_matches_execution() {
    // Verify hash join results match what nested loop would produce
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plan_hj_a (id INT, name TEXT)").await;
    exec(&ex, "CREATE TABLE plan_hj_b (aid INT, score INT)").await;
    for i in 0..20 {
        exec(&ex, &format!("INSERT INTO plan_hj_a VALUES ({i}, 'n{i}')")).await;
    }
    for i in 0..30 {
        let aid = i % 20;
        exec(&ex, &format!("INSERT INTO plan_hj_b VALUES ({aid}, {i})")).await;
    }
    // The equi-join triggers hash join internally
    let results = exec(&ex, "SELECT plan_hj_a.name, plan_hj_b.score FROM plan_hj_a JOIN plan_hj_b ON plan_hj_a.id = plan_hj_b.aid ORDER BY plan_hj_b.score").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 30); // Every b row has a matching a row
    // Verify ordering
    for i in 0..30 {
        assert_eq!(r[i][1], Value::Int32(i as i32));
    }
}

// ======================================================================

// Hash join tests
// ======================================================================

#[tokio::test]
async fn test_hash_join_inner() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hj_orders (id INT, cust_id INT, amount INT)").await;
    exec(&ex, "CREATE TABLE hj_customers (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO hj_customers VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO hj_customers VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO hj_customers VALUES (3, 'charlie')").await;
    exec(&ex, "INSERT INTO hj_orders VALUES (10, 1, 100)").await;
    exec(&ex, "INSERT INTO hj_orders VALUES (11, 2, 200)").await;
    exec(&ex, "INSERT INTO hj_orders VALUES (12, 1, 150)").await;
    exec(&ex, "INSERT INTO hj_orders VALUES (13, 4, 50)").await; // no matching customer
    let results = exec(&ex, "SELECT hj_orders.id, hj_customers.name, hj_orders.amount FROM hj_orders JOIN hj_customers ON hj_orders.cust_id = hj_customers.id ORDER BY hj_orders.id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Text("alice".into()));
    assert_eq!(r[1][1], Value::Text("bob".into()));
    assert_eq!(r[2][1], Value::Text("alice".into()));
}

#[tokio::test]
async fn test_hash_join_left() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hjl_a (id INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE hjl_b (id INT, info TEXT)").await;
    exec(&ex, "INSERT INTO hjl_a VALUES (1, 'x')").await;
    exec(&ex, "INSERT INTO hjl_a VALUES (2, 'y')").await;
    exec(&ex, "INSERT INTO hjl_a VALUES (3, 'z')").await;
    exec(&ex, "INSERT INTO hjl_b VALUES (1, 'match1')").await;
    exec(&ex, "INSERT INTO hjl_b VALUES (3, 'match3')").await;
    let results = exec(&ex, "SELECT hjl_a.id, hjl_b.info FROM hjl_a LEFT JOIN hjl_b ON hjl_a.id = hjl_b.id ORDER BY hjl_a.id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Text("match1".into()));
    assert_eq!(r[1][1], Value::Null); // id=2 has no match
    assert_eq!(r[2][1], Value::Text("match3".into()));
}

#[tokio::test]
async fn test_hash_join_right() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hjr_a (id INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE hjr_b (id INT, info TEXT)").await;
    exec(&ex, "INSERT INTO hjr_a VALUES (1, 'x')").await;
    exec(&ex, "INSERT INTO hjr_b VALUES (1, 'match1')").await;
    exec(&ex, "INSERT INTO hjr_b VALUES (2, 'match2')").await;
    let results = exec(&ex, "SELECT hjr_a.val, hjr_b.info FROM hjr_a RIGHT JOIN hjr_b ON hjr_a.id = hjr_b.id ORDER BY hjr_b.id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("x".into()));
    assert_eq!(r[1][0], Value::Null); // id=2 has no left match
}

#[tokio::test]
async fn test_hash_join_full() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hjf_a (id INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE hjf_b (id INT, info TEXT)").await;
    exec(&ex, "INSERT INTO hjf_a VALUES (1, 'x')").await;
    exec(&ex, "INSERT INTO hjf_a VALUES (2, 'y')").await;
    exec(&ex, "INSERT INTO hjf_b VALUES (2, 'match2')").await;
    exec(&ex, "INSERT INTO hjf_b VALUES (3, 'match3')").await;
    let results = exec(&ex, "SELECT hjf_a.id, hjf_a.val, hjf_b.info FROM hjf_a FULL OUTER JOIN hjf_b ON hjf_a.id = hjf_b.id ORDER BY COALESCE(hjf_a.id, hjf_b.id)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    // id=1: left only
    assert_eq!(r[0][1], Value::Text("x".into()));
    assert_eq!(r[0][2], Value::Null);
    // id=2: both matched
    assert_eq!(r[1][1], Value::Text("y".into()));
    assert_eq!(r[1][2], Value::Text("match2".into()));
    // id=3: right only
    assert_eq!(r[2][0], Value::Null);
    assert_eq!(r[2][2], Value::Text("match3".into()));
}

#[tokio::test]
async fn test_hash_join_multi_key() {
    // Join on composite key: (a, b)
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hjm_a (a INT, b INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE hjm_b (x INT, y INT, info TEXT)").await;
    exec(&ex, "INSERT INTO hjm_a VALUES (1, 10, 'r1')").await;
    exec(&ex, "INSERT INTO hjm_a VALUES (1, 20, 'r2')").await;
    exec(&ex, "INSERT INTO hjm_a VALUES (2, 10, 'r3')").await;
    exec(&ex, "INSERT INTO hjm_b VALUES (1, 10, 'match')").await;
    exec(&ex, "INSERT INTO hjm_b VALUES (2, 20, 'no_match')").await;
    let results = exec(&ex, "SELECT hjm_a.val, hjm_b.info FROM hjm_a JOIN hjm_b ON hjm_a.a = hjm_b.x AND hjm_a.b = hjm_b.y").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1); // Only (1,10) matches
    assert_eq!(r[0][0], Value::Text("r1".into()));
    assert_eq!(r[0][1], Value::Text("match".into()));
}

#[tokio::test]
async fn test_hash_join_null_handling() {
    // NULLs should never match in equi-joins
    let ex = test_executor();
    exec(&ex, "CREATE TABLE hjn_a (id INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE hjn_b (id INT, info TEXT)").await;
    exec(&ex, "INSERT INTO hjn_a VALUES (1, 'x')").await;
    exec(&ex, "INSERT INTO hjn_a VALUES (NULL, 'y')").await;
    exec(&ex, "INSERT INTO hjn_b VALUES (1, 'match')").await;
    exec(&ex, "INSERT INTO hjn_b VALUES (NULL, 'null_match')").await;
    let results = exec(&ex, "SELECT hjn_a.val, hjn_b.info FROM hjn_a JOIN hjn_b ON hjn_a.id = hjn_b.id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1); // NULL = NULL should NOT match
    assert_eq!(r[0][0], Value::Text("x".into()));
}

// ======================================================================

// generate_series and table function tests
// ======================================================================

#[tokio::test]
async fn test_generate_series_basic() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT * FROM generate_series(1, 5)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);
    assert_eq!(r[0][0], Value::Int64(1));
    assert_eq!(r[4][0], Value::Int64(5));
}

#[tokio::test]
async fn test_generate_series_with_step() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT * FROM generate_series(0, 10, 2)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 6); // 0, 2, 4, 6, 8, 10
    assert_eq!(r[0][0], Value::Int64(0));
    assert_eq!(r[5][0], Value::Int64(10));
}

#[tokio::test]
async fn test_generate_series_descending() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT * FROM generate_series(5, 1, -1)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);
    assert_eq!(r[0][0], Value::Int64(5));
    assert_eq!(r[4][0], Value::Int64(1));
}

#[tokio::test]
async fn test_generate_series_empty() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT * FROM generate_series(5, 1)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0); // empty because default step is 1 and 5 > 1
}

// ======================================================================

// Recursive CTEs
// ======================================================================

#[tokio::test]
async fn test_recursive_cte() {
    let ex = test_executor();
    let results = exec(&ex, "
        WITH RECURSIVE cnt(x) AS (
            SELECT 1
            UNION ALL
            SELECT x + 1 FROM cnt WHERE x < 5
        )
        SELECT x FROM cnt
    ").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);
    // Literal 1 produces Int32, arithmetic x+1 also produces Int32
    let first = match &r[0][0] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        v => panic!("unexpected type: {v:?}"),
    };
    let last = match &r[4][0] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        v => panic!("unexpected type: {v:?}"),
    };
    assert_eq!(first, 1);
    assert_eq!(last, 5);
}

#[tokio::test]
async fn test_recursive_cte_hierarchy() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, manager_id INT)").await;
    exec(&ex, "INSERT INTO employees VALUES (1, 'CEO', NULL)").await;
    exec(&ex, "INSERT INTO employees VALUES (2, 'VP', 1)").await;
    exec(&ex, "INSERT INTO employees VALUES (3, 'Director', 2)").await;
    exec(&ex, "INSERT INTO employees VALUES (4, 'Manager', 3)").await;

    let results = exec(&ex, "
        WITH RECURSIVE org(id, name, depth) AS (
            SELECT id, name, 0 FROM employees WHERE manager_id IS NULL
            UNION ALL
            SELECT e.id, e.name, org.depth + 1
            FROM employees e JOIN org ON e.manager_id = org.id
        )
        SELECT name, depth FROM org ORDER BY depth
    ").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 4);
    assert_eq!(r[0][0], Value::Text("CEO".into()));
    // Depth can be Int32 or Int64 depending on expression evaluation
    let depth_0 = match &r[0][1] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        v => panic!("unexpected depth type: {v:?}"),
    };
    let depth_3 = match &r[3][1] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        v => panic!("unexpected depth type: {v:?}"),
    };
    assert_eq!(depth_0, 0);
    assert_eq!(r[3][0], Value::Text("Manager".into()));
    assert_eq!(depth_3, 3);
}

// ======================================================================

// Advanced aggregates
// ======================================================================

#[tokio::test]
async fn test_string_agg() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE tags (id INT, tag TEXT)").await;
    exec(&ex, "INSERT INTO tags VALUES (1, 'rust')").await;
    exec(&ex, "INSERT INTO tags VALUES (1, 'database')").await;
    exec(&ex, "INSERT INTO tags VALUES (1, 'sql')").await;

    let results = exec(&ex, "SELECT STRING_AGG(tag, ', ') FROM tags WHERE id = 1").await;
    let val = scalar(&results[0]);
    match val {
        Value::Text(s) => assert!(s.contains("rust") && s.contains("database") && s.contains("sql")),
        _ => panic!("expected text, got {val:?}"),
    }
}

#[tokio::test]
async fn test_array_agg() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nums (n INT)").await;
    exec(&ex, "INSERT INTO nums VALUES (10)").await;
    exec(&ex, "INSERT INTO nums VALUES (20)").await;
    exec(&ex, "INSERT INTO nums VALUES (30)").await;

    let results = exec(&ex, "SELECT ARRAY_AGG(n) FROM nums").await;
    let val = scalar(&results[0]);
    match val {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("expected array, got {val:?}"),
    }
}

#[tokio::test]
async fn test_bool_and_or() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE flags (b BOOLEAN)").await;
    exec(&ex, "INSERT INTO flags VALUES (true)").await;
    exec(&ex, "INSERT INTO flags VALUES (true)").await;
    exec(&ex, "INSERT INTO flags VALUES (false)").await;

    let results = exec(&ex, "SELECT BOOL_AND(b) FROM flags").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(false));

    let results = exec(&ex, "SELECT BOOL_OR(b) FROM flags").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));

    let results = exec(&ex, "SELECT EVERY(b) FROM flags").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_bit_and_or() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE bits (v INT)").await;
    exec(&ex, "INSERT INTO bits VALUES (7)").await;
    exec(&ex, "INSERT INTO bits VALUES (3)").await;

    let results = exec(&ex, "SELECT BIT_AND(v) FROM bits").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(3)); // 7 & 3 = 3

    let results = exec(&ex, "SELECT BIT_OR(v) FROM bits").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(7)); // 7 | 3 = 7
}

#[tokio::test]
async fn test_count_distinct() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE dup_vals (v INT)").await;
    exec(&ex, "INSERT INTO dup_vals VALUES (1)").await;
    exec(&ex, "INSERT INTO dup_vals VALUES (2)").await;
    exec(&ex, "INSERT INTO dup_vals VALUES (2)").await;
    exec(&ex, "INSERT INTO dup_vals VALUES (3)").await;
    exec(&ex, "INSERT INTO dup_vals VALUES (3)").await;

    let results = exec(&ex, "SELECT COUNT(DISTINCT v) FROM dup_vals").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(3));
}

#[tokio::test]
async fn test_sum_distinct() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE dup_nums (v INT)").await;
    exec(&ex, "INSERT INTO dup_nums VALUES (1)").await;
    exec(&ex, "INSERT INTO dup_nums VALUES (2)").await;
    exec(&ex, "INSERT INTO dup_nums VALUES (2)").await;
    exec(&ex, "INSERT INTO dup_nums VALUES (3)").await;

    let results = exec(&ex, "SELECT SUM(DISTINCT v) FROM dup_nums").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(6)); // 1 + 2 + 3
}

// ======================================================================

// PERCENT_RANK and CUME_DIST window functions
// ======================================================================

#[tokio::test]
async fn test_percent_rank() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE prank (val INT)").await;
    exec(&ex, "INSERT INTO prank VALUES (10)").await;
    exec(&ex, "INSERT INTO prank VALUES (20)").await;
    exec(&ex, "INSERT INTO prank VALUES (30)").await;
    exec(&ex, "INSERT INTO prank VALUES (40)").await;

    let results = exec(&ex, "SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM prank").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 4);
    // First row: (10 - 1) / (4 - 1) = 0.0
    assert_eq!(r[0][1], Value::Float64(0.0));
    // Last row: (4 - 1) / (4 - 1) = 1.0
    assert_eq!(r[3][1], Value::Float64(1.0));
}

#[tokio::test]
async fn test_cume_dist() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cdist (val INT)").await;
    exec(&ex, "INSERT INTO cdist VALUES (10)").await;
    exec(&ex, "INSERT INTO cdist VALUES (20)").await;
    exec(&ex, "INSERT INTO cdist VALUES (30)").await;
    exec(&ex, "INSERT INTO cdist VALUES (40)").await;

    let results = exec(&ex, "SELECT val, CUME_DIST() OVER (ORDER BY val) AS cd FROM cdist").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 4);
    // First row: 1/4 = 0.25
    assert_eq!(r[0][1], Value::Float64(0.25));
    // Last row: 4/4 = 1.0
    assert_eq!(r[3][1], Value::Float64(1.0));
}

// ======================================================================

// GROUPING SETS / CUBE / ROLLUP
// ======================================================================

#[tokio::test]
async fn test_rollup() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales (region TEXT, product TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO sales VALUES ('East', 'A', 100)").await;
    exec(&ex, "INSERT INTO sales VALUES ('East', 'B', 200)").await;
    exec(&ex, "INSERT INTO sales VALUES ('West', 'A', 150)").await;

    let results = exec(&ex, "
        SELECT region, SUM(amount) AS total
        FROM sales
        GROUP BY ROLLUP(region)
    ").await;
    let r = rows(&results[0]);
    // ROLLUP(region) = GROUPING SETS ((region), ())
    // Should have: East=300, West=150, grand total=450
    assert!(r.len() >= 3, "Expected at least 3 rows for ROLLUP, got {}", r.len());
}

#[tokio::test]
async fn test_cube() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cube_sales (region TEXT, product TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO cube_sales VALUES ('East', 'A', 100)").await;
    exec(&ex, "INSERT INTO cube_sales VALUES ('West', 'B', 200)").await;

    let results = exec(&ex, "
        SELECT region, SUM(amount) AS total
        FROM cube_sales
        GROUP BY CUBE(region)
    ").await;
    let r = rows(&results[0]);
    // CUBE(region) = GROUPING SETS ((), (region))
    assert!(r.len() >= 3, "Expected at least 3 rows for CUBE, got {}", r.len());
}

// ======================================================================

// LATERAL join
// ======================================================================

#[tokio::test]
async fn test_lateral_join() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lat_dept (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO lat_dept VALUES (1, 'Engineering')").await;
    exec(&ex, "INSERT INTO lat_dept VALUES (2, 'Sales')").await;

    exec(&ex, "CREATE TABLE lat_emp (id INT, dept_id INT, name TEXT, salary INT)").await;
    exec(&ex, "INSERT INTO lat_emp VALUES (1, 1, 'Alice', 100)").await;
    exec(&ex, "INSERT INTO lat_emp VALUES (2, 1, 'Bob', 90)").await;
    exec(&ex, "INSERT INTO lat_emp VALUES (3, 2, 'Charlie', 80)").await;

    // Simple LATERAL join: for each dept, get matching employees
    let results = exec(&ex, "
        SELECT lat_dept.name, sub.name AS emp_name
        FROM lat_dept
        JOIN LATERAL (
            SELECT lat_emp.name FROM lat_emp
            WHERE lat_emp.dept_id = lat_dept.id
        ) AS sub ON true
    ").await;
    let r = rows(&results[0]);
    // Dept 1 has 2 employees, dept 2 has 1 = 3 total
    assert_eq!(r.len(), 3);
}
// ======================================================================

// SELECT DISTINCT
// ======================================================================

#[tokio::test]
async fn test_select_distinct() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE dup_test (color TEXT)").await;
    exec(&ex, "INSERT INTO dup_test VALUES ('red')").await;
    exec(&ex, "INSERT INTO dup_test VALUES ('blue')").await;
    exec(&ex, "INSERT INTO dup_test VALUES ('red')").await;
    exec(&ex, "INSERT INTO dup_test VALUES ('green')").await;
    exec(&ex, "INSERT INTO dup_test VALUES ('blue')").await;

    let results = exec(&ex, "SELECT DISTINCT color FROM dup_test ORDER BY color").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Text("blue".into()));
    assert_eq!(r[1][0], Value::Text("green".into()));
    assert_eq!(r[2][0], Value::Text("red".into()));
}

#[tokio::test]
async fn test_select_distinct_on() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE scores (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO scores VALUES ('Alice', 100)").await;
    exec(&ex, "INSERT INTO scores VALUES ('Alice', 90)").await;
    exec(&ex, "INSERT INTO scores VALUES ('Bob', 80)").await;
    exec(&ex, "INSERT INTO scores VALUES ('Bob', 95)").await;

    let results = exec(&ex, "SELECT DISTINCT ON (name) name, score FROM scores ORDER BY name, score DESC").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("Alice".into()));
    // Should get the first row for each name after ordering by score DESC
    let score = match &r[0][1] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        v => panic!("unexpected: {v:?}"),
    };
    assert_eq!(score, 100);
}

// ======================================================================

// Qualified wildcard (table.*)
// ======================================================================

#[tokio::test]
async fn test_qualified_wildcard() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE qw_a (id INT, name TEXT)").await;
    exec(&ex, "CREATE TABLE qw_b (id INT, label TEXT)").await;
    exec(&ex, "INSERT INTO qw_a VALUES (1, 'foo')").await;
    exec(&ex, "INSERT INTO qw_b VALUES (1, 'bar')").await;

    let results = exec(&ex, "SELECT qw_a.* FROM qw_a JOIN qw_b ON qw_a.id = qw_b.id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].len(), 2); // id and name from qw_a only
}

// ======================================================================

// ORDER BY alias and table-qualified column tests
// ======================================================================

#[tokio::test]
async fn test_order_by_column_alias() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;

    // ORDER BY column alias
    let results = exec(&ex, "SELECT id AS i, name FROM t ORDER BY i").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[2][0], Value::Int32(3));
}

#[tokio::test]
async fn test_order_by_table_alias() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;

    // ORDER BY table-qualified column using table alias
    let results = exec(&ex, "SELECT t.id, t.name FROM t ORDER BY t.id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[2][0], Value::Int32(3));
}

// ======================================================================

// Window frame tests
// ======================================================================

#[tokio::test]
async fn test_window_frame_rows_between_preceding_and_following() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE wf (id INT, val INT)").await;
    exec(&ex, "INSERT INTO wf VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO wf VALUES (2, 20)").await;
    exec(&ex, "INSERT INTO wf VALUES (3, 30)").await;
    exec(&ex, "INSERT INTO wf VALUES (4, 40)").await;
    exec(&ex, "INSERT INTO wf VALUES (5, 50)").await;

    // SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    // Row 1 (id=1, val=10): frame=[10,20]       => 30
    // Row 2 (id=2, val=20): frame=[10,20,30]    => 60
    // Row 3 (id=3, val=30): frame=[20,30,40]    => 90
    // Row 4 (id=4, val=40): frame=[30,40,50]    => 120
    // Row 5 (id=5, val=50): frame=[40,50]        => 90
    let results = exec(
        &ex,
        "SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as running_sum FROM wf",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);
    assert_eq!(r[0][1], Value::Float64(30.0));   // 10 + 20
    assert_eq!(r[1][1], Value::Float64(60.0));   // 10 + 20 + 30
    assert_eq!(r[2][1], Value::Float64(90.0));   // 20 + 30 + 40
    assert_eq!(r[3][1], Value::Float64(120.0));  // 30 + 40 + 50
    assert_eq!(r[4][1], Value::Float64(90.0));   // 40 + 50
}

#[tokio::test]
async fn test_window_frame_cumulative_avg() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE wf2 (id INT, val INT)").await;
    exec(&ex, "INSERT INTO wf2 VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO wf2 VALUES (2, 20)").await;
    exec(&ex, "INSERT INTO wf2 VALUES (3, 30)").await;
    exec(&ex, "INSERT INTO wf2 VALUES (4, 40)").await;

    // AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    // Row 1: avg(10) = 10
    // Row 2: avg(10,20) = 15
    // Row 3: avg(10,20,30) = 20
    // Row 4: avg(10,20,30,40) = 25
    let results = exec(
        &ex,
        "SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_avg FROM wf2",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 4);
    assert_eq!(r[0][1], Value::Float64(10.0));
    assert_eq!(r[1][1], Value::Float64(15.0));
    assert_eq!(r[2][1], Value::Float64(20.0));
    assert_eq!(r[3][1], Value::Float64(25.0));
}

#[tokio::test]
async fn test_window_frame_unbounded_following() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE wf3 (id INT, val INT)").await;
    exec(&ex, "INSERT INTO wf3 VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO wf3 VALUES (2, 20)").await;
    exec(&ex, "INSERT INTO wf3 VALUES (3, 30)").await;

    // SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    // Row 1: sum(10,20,30) = 60
    // Row 2: sum(20,30) = 50
    // Row 3: sum(30) = 30
    let results = exec(
        &ex,
        "SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as rev_sum FROM wf3",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Float64(60.0));
    assert_eq!(r[1][1], Value::Float64(50.0));
    assert_eq!(r[2][1], Value::Float64(30.0));
}

#[tokio::test]
async fn test_window_frame_entire_partition() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE wf4 (id INT, val INT)").await;
    exec(&ex, "INSERT INTO wf4 VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO wf4 VALUES (2, 20)").await;
    exec(&ex, "INSERT INTO wf4 VALUES (3, 30)").await;

    // SUM(val) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    // All rows see the full partition sum = 60
    let results = exec(
        &ex,
        "SELECT id, SUM(val) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total FROM wf4",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Float64(60.0));
    assert_eq!(r[1][1], Value::Float64(60.0));
    assert_eq!(r[2][1], Value::Float64(60.0));
}

#[tokio::test]
async fn test_window_frame_count_min_max() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE wf5 (id INT, val INT)").await;
    exec(&ex, "INSERT INTO wf5 VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO wf5 VALUES (2, 20)").await;
    exec(&ex, "INSERT INTO wf5 VALUES (3, 30)").await;
    exec(&ex, "INSERT INTO wf5 VALUES (4, 40)").await;

    // COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    let results = exec(
        &ex,
        "SELECT id, COUNT(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as cnt FROM wf5",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int64(2));  // rows 1,2
    assert_eq!(r[1][1], Value::Int64(3));  // rows 1,2,3
    assert_eq!(r[2][1], Value::Int64(3));  // rows 2,3,4
    assert_eq!(r[3][1], Value::Int64(2));  // rows 3,4

    // MIN(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    let results = exec(
        &ex,
        "SELECT id, MIN(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as mn FROM wf5",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int32(10));  // min(10,20)
    assert_eq!(r[1][1], Value::Int32(10));  // min(10,20,30)
    assert_eq!(r[2][1], Value::Int32(20));  // min(20,30,40)
    assert_eq!(r[3][1], Value::Int32(30));  // min(30,40)

    // MAX(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    let results = exec(
        &ex,
        "SELECT id, MAX(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as mx FROM wf5",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int32(20));  // max(10,20)
    assert_eq!(r[1][1], Value::Int32(30));  // max(10,20,30)
    assert_eq!(r[2][1], Value::Int32(40));  // max(20,30,40)
    assert_eq!(r[3][1], Value::Int32(40));  // max(30,40)
}

// ======================================================================

// Cluster query routing tests
// ======================================================================

#[tokio::test]
async fn test_check_route_standalone_returns_none() {
    let ex = test_executor();
    // No cluster configured → always returns None
    assert!(ex.check_route("SELECT * FROM t WHERE id = 42").is_none());
}

#[tokio::test]
async fn test_check_route_with_cluster() {
    use crate::distributed::{ClusterCoordinator, RouteDecision};
    // Configure a cluster in PrimaryReplica mode
    let cluster = Arc::new(parking_lot::RwLock::new(
        ClusterCoordinator::new_primary_replica(0x1, 0x2, "127.0.0.1:9001"),
    ));
    // Add shards to the router
    {
        let mut coord = cluster.write();
        let router = coord.router_mut();
        router.add_shard(1, 0x2, 0, 100);  // shard 1 owned by node 0x2 (remote)
        router.add_shard(2, 0x1, 100, 200); // shard 2 owned by us (local)
    }
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let ex = Executor::new(catalog, storage).with_cluster(cluster);
    // Query targeting shard 1 (remote) → Forward
    let route = ex.check_route("SELECT * FROM t WHERE id = 50");
    assert!(matches!(route, Some(RouteDecision::Forward { .. })));
    // Query targeting shard 2 (local) → None (handled locally)
    let route = ex.check_route("SELECT * FROM t WHERE id = 150");
    assert!(route.is_none());
}

// ======================================================================
// Audit-driven correctness tests
// ======================================================================

// GROUP BY NULL grouping: NULL values must all group together into one bucket.
#[tokio::test]
async fn test_group_by_null_groups_together() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE grp_null_test (cat TEXT, val INT)").await;
    exec(&ex, "INSERT INTO grp_null_test VALUES (NULL, 1), (NULL, 2), ('a', 3), ('a', 4), (NULL, 5)").await;

    let res = exec(&ex, "SELECT cat, SUM(val) FROM grp_null_test GROUP BY cat ORDER BY cat").await;
    let r = rows(&res[0]);
    // Expect two groups: NULL (sum=8) and 'a' (sum=7).
    // ORDER BY NULL LAST by default — NULL group goes last.
    assert_eq!(r.len(), 2, "expected 2 groups, got {}", r.len());
    // Find the NULL group and the 'a' group regardless of order.
    let null_group = r.iter().find(|row| matches!(row[0], Value::Null));
    let a_group = r.iter().find(|row| row[0] == Value::Text("a".into()));
    assert!(null_group.is_some(), "missing NULL group");
    assert!(a_group.is_some(), "missing 'a' group");
    assert_eq!(null_group.unwrap()[1], Value::Int64(8), "NULL group SUM should be 8");
    assert_eq!(a_group.unwrap()[1], Value::Int64(7), "'a' group SUM should be 7");
}

// NULLIF edge cases: NULLIF(NULL, NULL), NULLIF(NULL, x), NULLIF(x, NULL).
#[tokio::test]
async fn test_nullif_null_edge_cases() {
    let ex = test_executor();
    // NULLIF(1, NULL) → 1  (second arg NULL: never equal, return first)
    let r = exec(&ex, "SELECT NULLIF(1, NULL)").await;
    assert!(!matches!(scalar(&r[0]), Value::Null), "NULLIF(1, NULL) should not be NULL");

    // NULLIF(NULL, 1) → NULL  (first arg is NULL, return NULL)
    let r = exec(&ex, "SELECT NULLIF(NULL, 1)").await;
    assert_eq!(scalar(&r[0]), &Value::Null);

    // NULLIF(NULL, NULL) → NULL  (first arg is NULL, return NULL)
    let r = exec(&ex, "SELECT NULLIF(NULL, NULL)").await;
    assert_eq!(scalar(&r[0]), &Value::Null);

    // NULLIF(5, 5) → NULL  (equal args)
    let r = exec(&ex, "SELECT NULLIF(5, 5)").await;
    assert_eq!(scalar(&r[0]), &Value::Null);

    // NULLIF(5, 6) → 5  (different args)
    let r = exec(&ex, "SELECT NULLIF(5, 6)").await;
    assert!(!matches!(scalar(&r[0]), Value::Null), "NULLIF(5, 6) should not be NULL");
}

