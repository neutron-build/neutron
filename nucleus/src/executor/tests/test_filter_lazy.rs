use super::*;
use crate::types::Value;

// ======================================================================
// Phase 2C: Lazy Materialization for WHERE clause filtering
// ======================================================================

#[tokio::test]
async fn test_where_filter_simple_equality() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE users (id INT, name TEXT, age INT)").await;
    exec(&ex, "INSERT INTO users VALUES (1, 'Alice', 30)").await;
    exec(&ex, "INSERT INTO users VALUES (2, 'Bob', 25)").await;
    exec(&ex, "INSERT INTO users VALUES (3, 'Charlie', 35)").await;

    let results = exec(&ex, "SELECT * FROM users WHERE age > 30").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][2], Value::Int32(35));
}

#[tokio::test]
async fn test_where_filter_multiple_conditions() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE products (id INT, category TEXT, price INT)").await;
    exec(&ex, "INSERT INTO products VALUES (1, 'electronics', 100)").await;
    exec(&ex, "INSERT INTO products VALUES (2, 'books', 20)").await;
    exec(&ex, "INSERT INTO products VALUES (3, 'electronics', 500)").await;
    exec(&ex, "INSERT INTO products VALUES (4, 'books', 15)").await;

    // WHERE category = 'electronics' AND price > 200
    let results = exec(&ex, "SELECT * FROM products WHERE category = 'electronics' AND price > 200").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][2], Value::Int32(500));
}

#[tokio::test]
async fn test_where_filter_in_clause() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE orders (id INT, status TEXT)").await;
    exec(&ex, "INSERT INTO orders VALUES (1, 'pending')").await;
    exec(&ex, "INSERT INTO orders VALUES (2, 'shipped')").await;
    exec(&ex, "INSERT INTO orders VALUES (3, 'delivered')").await;
    exec(&ex, "INSERT INTO orders VALUES (4, 'cancelled')").await;

    let results = exec(&ex, "SELECT * FROM orders WHERE status IN ('shipped', 'delivered')").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_where_filter_null_handling() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nullable_data (id INT, value INT)").await;
    exec(&ex, "INSERT INTO nullable_data VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO nullable_data VALUES (2, NULL)").await;
    exec(&ex, "INSERT INTO nullable_data VALUES (3, 30)").await;

    // WHERE value > 15 should not match NULL
    let results = exec(&ex, "SELECT * FROM nullable_data WHERE value > 15").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Int32(30));
}

#[tokio::test]
async fn test_where_filter_all_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE simple (x INT)").await;
    exec(&ex, "INSERT INTO simple VALUES (1)").await;
    exec(&ex, "INSERT INTO simple VALUES (2)").await;
    exec(&ex, "INSERT INTO simple VALUES (3)").await;

    // WHERE x > 0 should match all rows
    let results = exec(&ex, "SELECT * FROM simple WHERE x > 0").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
}

#[tokio::test]
async fn test_where_filter_no_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE impossible (x INT)").await;
    exec(&ex, "INSERT INTO impossible VALUES (1)").await;
    exec(&ex, "INSERT INTO impossible VALUES (2)").await;
    exec(&ex, "INSERT INTO impossible VALUES (3)").await;

    // WHERE x > 1000 should match no rows
    let results = exec(&ex, "SELECT * FROM impossible WHERE x > 1000").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0);
}

#[tokio::test]
async fn test_where_filter_complex_boolean_expr() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE data (a INT, b INT, c TEXT)").await;
    exec(&ex, "INSERT INTO data VALUES (5, 10, 'x')").await;
    exec(&ex, "INSERT INTO data VALUES (15, 20, 'y')").await;
    exec(&ex, "INSERT INTO data VALUES (25, 30, 'z')").await;

    // WHERE (a > 5 AND b < 25) OR c = 'z'
    let results = exec(&ex, "SELECT * FROM data WHERE (a > 5 AND b < 25) OR c = 'z'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_where_filter_with_order_by() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sorted (id INT, score INT)").await;
    exec(&ex, "INSERT INTO sorted VALUES (1, 50)").await;
    exec(&ex, "INSERT INTO sorted VALUES (2, 100)").await;
    exec(&ex, "INSERT INTO sorted VALUES (3, 75)").await;
    exec(&ex, "INSERT INTO sorted VALUES (4, 60)").await;

    // WHERE score > 55 ORDER BY score DESC
    let results = exec(&ex, "SELECT * FROM sorted WHERE score > 55 ORDER BY score DESC").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Int32(100));
    assert_eq!(r[1][1], Value::Int32(75));
    assert_eq!(r[2][1], Value::Int32(60));
}

#[tokio::test]
async fn test_where_filter_with_limit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE limited (id INT)").await;
    for i in 1..=100 {
        exec(&ex, &format!("INSERT INTO limited VALUES ({})", i)).await;
    }

    // WHERE id > 50 LIMIT 10
    let results = exec(&ex, "SELECT * FROM limited WHERE id > 50 LIMIT 10").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 10);
}

#[tokio::test]
async fn test_where_filter_between() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ranges (x INT)").await;
    exec(&ex, "INSERT INTO ranges VALUES (1)").await;
    exec(&ex, "INSERT INTO ranges VALUES (5)").await;
    exec(&ex, "INSERT INTO ranges VALUES (10)").await;
    exec(&ex, "INSERT INTO ranges VALUES (15)").await;
    exec(&ex, "INSERT INTO ranges VALUES (20)").await;

    // WHERE x BETWEEN 5 AND 15
    let results = exec(&ex, "SELECT * FROM ranges WHERE x BETWEEN 5 AND 15").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(5));
    assert_eq!(r[1][0], Value::Int32(10));
    assert_eq!(r[2][0], Value::Int32(15));
}

#[tokio::test]
async fn test_where_filter_like() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE text_data (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO text_data VALUES (1, 'Alice')").await;
    exec(&ex, "INSERT INTO text_data VALUES (2, 'Bob')").await;
    exec(&ex, "INSERT INTO text_data VALUES (3, 'Charlie')").await;
    exec(&ex, "INSERT INTO text_data VALUES (4, 'David')").await;

    // WHERE name LIKE 'A%'
    let results = exec(&ex, "SELECT * FROM text_data WHERE name LIKE 'A%'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("Alice".into()));
}

#[tokio::test]
async fn test_where_filter_selective_hit_rate() {
    // Test a selective filter (low hit rate) that exercises lazy materialization benefits
    let ex = test_executor();
    exec(&ex, "CREATE TABLE large_table (id INT, category INT)").await;

    // Insert 100 rows with mostly category = 1
    for i in 1..=100 {
        let category = if i > 95 { 2 } else { 1 };
        exec(&ex, &format!("INSERT INTO large_table VALUES ({}, {})", i, category)).await;
    }

    // WHERE category = 2 should match only 5 out of 100 rows (5% hit rate)
    let results = exec(&ex, "SELECT * FROM large_table WHERE category = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);

    // Verify all matched rows have category = 2
    for row in r {
        assert_eq!(row[1], Value::Int32(2));
    }
}
