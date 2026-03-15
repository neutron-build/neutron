//! End-to-end smoke tests for Nucleus — exercises ALL major capabilities
//! through the embedded Database API (same executor code path as pgwire).
//!
//! Simulates a real SaaS application that uses multiple Nucleus models together:
//! relational SQL, KV, FTS, Vector, Graph, Document, TimeSeries, and more.

use super::*;

// ======================================================================
// 1. Schema Setup — various column types
// ======================================================================

#[tokio::test]
async fn test_e2e_schema_setup() {
    let ex = test_executor();

    // Create tables with diverse column types
    exec(&ex, "CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INT,
        balance FLOAT,
        is_active BOOLEAN DEFAULT true,
        metadata JSONB,
        created_at TIMESTAMP
    )").await;

    exec(&ex, "CREATE TABLE organizations (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        slug TEXT UNIQUE
    )").await;

    exec(&ex, "CREATE TABLE memberships (
        user_id INT REFERENCES users(id) ON DELETE CASCADE,
        org_id INT REFERENCES organizations(id) ON DELETE CASCADE,
        role TEXT DEFAULT 'member'
    )").await;

    exec(&ex, "CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        org_id INT REFERENCES organizations(id),
        name TEXT NOT NULL,
        price FLOAT NOT NULL,
        stock INT DEFAULT 0,
        tags JSONB,
        embedding VECTOR(4)
    )").await;

    exec(&ex, "CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INT REFERENCES users(id),
        product_id INT REFERENCES products(id),
        quantity INT NOT NULL,
        total FLOAT NOT NULL,
        status TEXT DEFAULT 'pending'
    )").await;

    // Verify tables were created
    let results = exec(&ex, "SELECT table_name FROM information_schema.tables ORDER BY table_name").await;
    let r = rows(&results[0]);
    assert!(r.len() >= 5, "should have at least 5 tables, got {}", r.len());
}

// ======================================================================
// 2. Indexes — B-tree and unique indexes
// ======================================================================

#[tokio::test]
async fn test_e2e_indexes() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE idx_users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE, age INT)").await;
    exec(&ex, "CREATE TABLE idx_orders (id INT, user_id INT, amount INT, status TEXT)").await;

    // Create B-tree index
    exec(&ex, "CREATE INDEX idx_orders_user ON idx_orders (user_id)").await;
    exec(&ex, "CREATE INDEX idx_orders_status ON idx_orders (status)").await;

    // Insert data
    for i in 1..=20 {
        exec(&ex, &format!(
            "INSERT INTO idx_users VALUES ({i}, 'user_{i}', 'user{i}@test.com', {})",
            20 + i
        )).await;
    }
    for i in 1..=50 {
        let uid = (i % 20) + 1;
        let status = if i % 3 == 0 { "complete" } else { "pending" };
        exec(&ex, &format!(
            "INSERT INTO idx_orders VALUES ({i}, {uid}, {}, '{status}')",
            i * 10
        )).await;
    }

    // Verify indexes work via queries
    let results = exec(&ex, "SELECT COUNT(*) FROM idx_orders WHERE user_id = 5").await;
    let count = match scalar(&results[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert!(count >= 1, "should find orders for user 5");

    // Verify unique index rejects duplicates
    let err = ex.execute("INSERT INTO idx_users VALUES (100, 'dup', 'user1@test.com', 25)").await;
    assert!(err.is_err(), "unique index should reject duplicate email");

    // Verify primary key index
    let err = ex.execute("INSERT INTO idx_users VALUES (1, 'dup', 'unique@test.com', 25)").await;
    assert!(err.is_err(), "primary key should reject duplicate id");
}

// ======================================================================
// 3. CRUD Operations — INSERT, SELECT, UPDATE, DELETE
// ======================================================================

#[tokio::test]
async fn test_e2e_crud() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE crud_items (id INT PRIMARY KEY, name TEXT, price FLOAT, active BOOLEAN)").await;

    // INSERT multiple rows
    exec(&ex, "INSERT INTO crud_items VALUES (1, 'Widget', 9.99, true)").await;
    exec(&ex, "INSERT INTO crud_items VALUES (2, 'Gadget', 19.99, true)").await;
    exec(&ex, "INSERT INTO crud_items VALUES (3, 'Doohickey', 5.49, false)").await;
    exec(&ex, "INSERT INTO crud_items VALUES (4, 'Thingamajig', 29.99, true)").await;

    // SELECT with WHERE
    let results = exec(&ex, "SELECT name, price FROM crud_items WHERE active = true ORDER BY price").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Text("Widget".into()));
    assert_eq!(r[2][0], Value::Text("Thingamajig".into()));

    // UPDATE
    exec(&ex, "UPDATE crud_items SET price = 7.99 WHERE id = 3").await;
    let results = exec(&ex, "SELECT price FROM crud_items WHERE id = 3").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(7.99));

    // UPDATE multiple rows — Gadget (19.99) and Thingamajig (29.99) get deactivated
    exec(&ex, "UPDATE crud_items SET active = false WHERE price > 15.0").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM crud_items WHERE active = true").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1)); // Only Widget (9.99) remains active

    // DELETE
    exec(&ex, "DELETE FROM crud_items WHERE id = 4").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM crud_items").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));

    // UPSERT (ON CONFLICT DO UPDATE)
    exec(&ex, "INSERT INTO crud_items VALUES (1, 'Super Widget', 12.99, true)
               ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, price = EXCLUDED.price").await;
    let results = exec(&ex, "SELECT name, price FROM crud_items WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("Super Widget".into()));
    assert_eq!(r[0][1], Value::Float64(12.99));

    // DELETE with RETURNING
    let results = exec(&ex, "DELETE FROM crud_items WHERE id = 2 RETURNING name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("Gadget".into()));
}

// ======================================================================
// 4. Transactions — BEGIN/COMMIT, BEGIN/ROLLBACK
// ======================================================================

#[tokio::test]
async fn test_e2e_transactions() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE txn_accounts (id INT PRIMARY KEY, name TEXT, balance FLOAT)").await;
    exec(&ex, "INSERT INTO txn_accounts VALUES (1, 'Alice', 1000.0)").await;
    exec(&ex, "INSERT INTO txn_accounts VALUES (2, 'Bob', 500.0)").await;

    // Transaction that COMMITs (transfer money)
    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE txn_accounts SET balance = balance - 200.0 WHERE id = 1").await;
    exec(&ex, "UPDATE txn_accounts SET balance = balance + 200.0 WHERE id = 2").await;
    exec(&ex, "COMMIT").await;

    // Verify balances persisted
    let results = exec(&ex, "SELECT balance FROM txn_accounts WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(800.0));
    let results = exec(&ex, "SELECT balance FROM txn_accounts WHERE id = 2").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(700.0));

    // Transaction that ROLLBACKs (failed transfer)
    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE txn_accounts SET balance = balance - 5000.0 WHERE id = 1").await;
    // Oops, overdraft — rollback
    exec(&ex, "ROLLBACK").await;

    // Verify balances unchanged
    let results = exec(&ex, "SELECT balance FROM txn_accounts WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(800.0));

    // Savepoint test
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO txn_accounts VALUES (3, 'Charlie', 300.0)").await;
    exec(&ex, "SAVEPOINT sp1").await;
    exec(&ex, "INSERT INTO txn_accounts VALUES (4, 'Diana', 400.0)").await;
    exec(&ex, "ROLLBACK TO SAVEPOINT sp1").await;
    exec(&ex, "COMMIT").await;

    // Charlie should exist, Diana should not
    let results = exec(&ex, "SELECT COUNT(*) FROM txn_accounts").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));
    let results = exec(&ex, "SELECT name FROM txn_accounts WHERE id = 3").await;
    assert_eq!(*scalar(&results[0]), Value::Text("Charlie".into()));
    let results = exec(&ex, "SELECT COUNT(*) FROM txn_accounts WHERE id = 4").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(0));
}

// ======================================================================
// 5. Aggregations — COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING
// ======================================================================

#[tokio::test]
async fn test_e2e_aggregations() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE agg_sales (id INT, region TEXT, product TEXT, amount FLOAT)").await;
    exec(&ex, "INSERT INTO agg_sales VALUES (1, 'East', 'Widget', 100.0)").await;
    exec(&ex, "INSERT INTO agg_sales VALUES (2, 'East', 'Widget', 150.0)").await;
    exec(&ex, "INSERT INTO agg_sales VALUES (3, 'West', 'Gadget', 200.0)").await;
    exec(&ex, "INSERT INTO agg_sales VALUES (4, 'East', 'Gadget', 75.0)").await;
    exec(&ex, "INSERT INTO agg_sales VALUES (5, 'West', 'Widget', 300.0)").await;
    exec(&ex, "INSERT INTO agg_sales VALUES (6, 'West', 'Gadget', 125.0)").await;

    // COUNT
    let results = exec(&ex, "SELECT COUNT(*) FROM agg_sales").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(6));

    // SUM
    let results = exec(&ex, "SELECT SUM(amount) FROM agg_sales").await;
    match scalar(&results[0]) {
        Value::Float64(v) => assert!((*v - 950.0).abs() < 0.01),
        other => panic!("expected Float64, got {other:?}"),
    }

    // AVG
    let results = exec(&ex, "SELECT AVG(amount) FROM agg_sales").await;
    match scalar(&results[0]) {
        Value::Float64(v) => assert!((*v - 158.33).abs() < 0.5),
        other => panic!("expected Float64, got {other:?}"),
    }

    // MIN / MAX
    let results = exec(&ex, "SELECT MIN(amount), MAX(amount) FROM agg_sales").await;
    let r = rows(&results[0]);
    match (&r[0][0], &r[0][1]) {
        (Value::Float64(mn), Value::Float64(mx)) => {
            assert!((*mn - 75.0).abs() < 0.01);
            assert!((*mx - 300.0).abs() < 0.01);
        }
        other => panic!("expected Float64 pair, got {other:?}"),
    }

    // GROUP BY
    let results = exec(&ex, "SELECT region, SUM(amount) AS total FROM agg_sales GROUP BY region ORDER BY region").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("East".into()));
    assert_eq!(r[1][0], Value::Text("West".into()));

    // HAVING
    let results = exec(&ex, "SELECT product, COUNT(*) AS cnt FROM agg_sales GROUP BY product HAVING COUNT(*) >= 3 ORDER BY product").await;
    let r = rows(&results[0]);
    assert!(r.len() >= 1, "at least one product with >= 3 sales");
}

// ======================================================================
// 6. Joins — INNER JOIN, LEFT JOIN
// ======================================================================

#[tokio::test]
async fn test_e2e_joins() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE join_depts (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO join_depts VALUES (1, 'Engineering')").await;
    exec(&ex, "INSERT INTO join_depts VALUES (2, 'Sales')").await;
    exec(&ex, "INSERT INTO join_depts VALUES (3, 'Marketing')").await;

    exec(&ex, "CREATE TABLE join_emps (id INT PRIMARY KEY, name TEXT, dept_id INT)").await;
    exec(&ex, "INSERT INTO join_emps VALUES (1, 'Alice', 1)").await;
    exec(&ex, "INSERT INTO join_emps VALUES (2, 'Bob', 1)").await;
    exec(&ex, "INSERT INTO join_emps VALUES (3, 'Charlie', 2)").await;
    exec(&ex, "INSERT INTO join_emps VALUES (4, 'Diana', NULL)").await;

    // INNER JOIN
    let results = exec(&ex,
        "SELECT join_emps.name AS emp_name, join_depts.name AS dept_name \
         FROM join_emps INNER JOIN join_depts ON join_emps.dept_id = join_depts.id \
         ORDER BY join_emps.name"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3); // Alice, Bob, Charlie (Diana has no dept)
    // Verify column names in result
    match &results[0] {
        ExecResult::Select { columns, .. } => {
            // First column should be emp name, second should be dept name
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("expected Select"),
    }
    // Check that Alice is first and paired with Engineering
    assert_eq!(r[0][0], Value::Text("Alice".into()));

    // LEFT JOIN — should include Diana with NULL department
    let results = exec(&ex,
        "SELECT join_emps.name AS emp_name, join_depts.name AS dept_name \
         FROM join_emps LEFT JOIN join_depts ON join_emps.dept_id = join_depts.id \
         ORDER BY join_emps.name"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 4);
    // Diana should have NULL for department name
    let diana = r.iter().find(|row| row[0] == Value::Text("Diana".into())).unwrap();
    assert_eq!(diana[1], Value::Null, "Diana has no department");

    // LEFT JOIN — department with no employees
    let results = exec(&ex,
        "SELECT join_depts.name, COUNT(join_emps.id) AS emp_count \
         FROM join_depts LEFT JOIN join_emps ON join_depts.id = join_emps.dept_id \
         GROUP BY join_depts.name \
         ORDER BY join_depts.name"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    // Marketing has 0 employees
    let marketing = r.iter().find(|row| row[0] == Value::Text("Marketing".into())).unwrap();
    assert_eq!(marketing[1], Value::Int64(0));
}

// ======================================================================
// 7. Subqueries — IN, EXISTS, scalar subquery
// ======================================================================

#[tokio::test]
async fn test_e2e_subqueries() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE sub_categories (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO sub_categories VALUES (1, 'electronics')").await;
    exec(&ex, "INSERT INTO sub_categories VALUES (2, 'books')").await;
    exec(&ex, "INSERT INTO sub_categories VALUES (3, 'clothing')").await;

    exec(&ex, "CREATE TABLE sub_products (id INT PRIMARY KEY, name TEXT, cat_id INT, price FLOAT)").await;
    exec(&ex, "INSERT INTO sub_products VALUES (1, 'Laptop', 1, 999.99)").await;
    exec(&ex, "INSERT INTO sub_products VALUES (2, 'Phone', 1, 699.99)").await;
    exec(&ex, "INSERT INTO sub_products VALUES (3, 'Novel', 2, 14.99)").await;
    exec(&ex, "INSERT INTO sub_products VALUES (4, 'T-Shirt', 3, 24.99)").await;
    exec(&ex, "INSERT INTO sub_products VALUES (5, 'Jacket', 3, 89.99)").await;

    // WHERE col IN (SELECT ...)
    let results = exec(&ex,
        "SELECT name FROM sub_products \
         WHERE cat_id IN (SELECT id FROM sub_categories WHERE name = 'electronics') \
         ORDER BY name"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("Laptop".into()));
    assert_eq!(r[1][0], Value::Text("Phone".into()));

    // EXISTS
    let results = exec(&ex,
        "SELECT name FROM sub_categories c \
         WHERE EXISTS (SELECT 1 FROM sub_products p WHERE p.cat_id = c.id AND p.price > 500.0) \
         ORDER BY name"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("electronics".into()));

    // Scalar subquery
    let results = exec(&ex,
        "SELECT name, price, (SELECT AVG(price) FROM sub_products) AS avg_price FROM sub_products WHERE id = 1"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("Laptop".into()));
}

// ======================================================================
// 8. Window Functions — ROW_NUMBER(), RANK() with PARTITION BY
// ======================================================================

#[tokio::test]
async fn test_e2e_window_functions() {
    let ex = test_executor();

    // Use INT scores like the existing passing window function tests
    exec(&ex, "CREATE TABLE win_sales (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO win_sales VALUES ('Alice', 90)").await;
    exec(&ex, "INSERT INTO win_sales VALUES ('Bob', 80)").await;
    exec(&ex, "INSERT INTO win_sales VALUES ('Charlie', 70)").await;

    // ROW_NUMBER() OVER (ORDER BY ...)
    let results = exec(&ex,
        "SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM win_sales"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][1], Value::Int64(1)); // Alice (90)
    assert_eq!(r[1][1], Value::Int64(2)); // Bob (80)
    assert_eq!(r[2][1], Value::Int64(3)); // Charlie (70)

    // RANK() with ties
    exec(&ex, "CREATE TABLE win_ranked (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO win_ranked VALUES ('A', 90)").await;
    exec(&ex, "INSERT INTO win_ranked VALUES ('B', 90)").await;
    exec(&ex, "INSERT INTO win_ranked VALUES ('C', 80)").await;

    let results = exec(&ex,
        "SELECT name, RANK() OVER (ORDER BY score DESC) FROM win_ranked"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int64(1)); // tied at 90
    assert_eq!(r[1][1], Value::Int64(1)); // tied at 90
    assert_eq!(r[2][1], Value::Int64(3)); // gap: 3, not 2

    // Running SUM with ORDER BY (using INT scores)
    exec(&ex, "CREATE TABLE win_running (val INT)").await;
    exec(&ex, "INSERT INTO win_running VALUES (1)").await;
    exec(&ex, "INSERT INTO win_running VALUES (2)").await;
    exec(&ex, "INSERT INTO win_running VALUES (3)").await;

    let results = exec(&ex,
        "SELECT val, SUM(val) OVER (ORDER BY val) FROM win_running"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Float64(1.0));  // running sum: 1
    assert_eq!(r[1][1], Value::Float64(3.0));  // running sum: 1+2
    assert_eq!(r[2][1], Value::Float64(6.0));  // running sum: 1+2+3
}

// ======================================================================
// 9. KV Operations — kv_set, kv_get, kv_del
// ======================================================================

#[tokio::test]
async fn test_e2e_kv_operations() {
    let ex = test_executor();

    // SET
    let res = exec(&ex, "SELECT kv_set('session:user1', 'token_abc123')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("OK".into()));

    // GET
    let res = exec(&ex, "SELECT kv_get('session:user1')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("token_abc123".into()));

    // GET missing key
    let res = exec(&ex, "SELECT kv_get('session:nonexistent')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);

    // EXISTS
    let res = exec(&ex, "SELECT kv_exists('session:user1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));

    // INCR (counter pattern — auto-creates key starting at 0)
    let res = exec(&ex, "SELECT kv_incr('page:views')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
    let res = exec(&ex, "SELECT kv_incr('page:views')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    let res = exec(&ex, "SELECT kv_incr('page:views', 10)").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(12));

    // DEL
    let res = exec(&ex, "SELECT kv_del('session:user1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    let res = exec(&ex, "SELECT kv_get('session:user1')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);

    // DEL missing key
    let res = exec(&ex, "SELECT kv_del('session:user1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));

    // SETNX (set-if-not-exists for distributed locks)
    let res = exec(&ex, "SELECT kv_setnx('lock:resource', 'owner1')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    let res = exec(&ex, "SELECT kv_setnx('lock:resource', 'owner2')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
    let res = exec(&ex, "SELECT kv_get('lock:resource')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("owner1".into()));
}

// ======================================================================
// 10. FTS Operations — fts_index, fts_search
// ======================================================================

#[tokio::test]
async fn test_e2e_fts_operations() {
    let ex = test_executor();

    // Index documents
    exec(&ex, "SELECT fts_index(1, 'Rust is a systems programming language focused on safety and performance')").await;
    exec(&ex, "SELECT fts_index(2, 'Python is great for data science and machine learning applications')").await;
    exec(&ex, "SELECT fts_index(3, 'JavaScript powers the modern web with React and Node.js')").await;
    exec(&ex, "SELECT fts_index(4, 'Rust and WebAssembly enable high-performance web applications')").await;

    // Search for "rust" — should find docs 1 and 4
    let res = exec(&ex, "SELECT fts_search('rust', 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("\"doc_id\":1") || json.contains("\"doc_id\":4"),
        "should find rust docs: {json}");

    // Search for "machine learning" — should find doc 2
    let res = exec(&ex, "SELECT fts_search('machine learning', 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("\"doc_id\":2"), "should find ML doc: {json}");

    // Search for non-existent term — should return empty
    let res = exec(&ex, "SELECT fts_search('nonexistent_term_xyz', 10)").await;
    assert_eq!(scalar(&res[0]), &Value::Text("[]".into()));

    // Doc count
    let res = exec(&ex, "SELECT fts_doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(4));

    // Remove a document
    exec(&ex, "SELECT fts_remove(3)").await;
    let res = exec(&ex, "SELECT fts_doc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(3));
}

// ======================================================================
// 11. Vector Operations — VECTOR(), VECTOR_DISTANCE()
// ======================================================================

#[tokio::test]
async fn test_e2e_vector_operations() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE vec_docs (id INT PRIMARY KEY, title TEXT, embedding VECTOR(3))").await;

    // Insert vectors
    exec(&ex, "INSERT INTO vec_docs VALUES (1, 'Database internals', VECTOR('[1.0, 0.0, 0.0]'))").await;
    exec(&ex, "INSERT INTO vec_docs VALUES (2, 'Machine learning', VECTOR('[0.0, 1.0, 0.0]'))").await;
    exec(&ex, "INSERT INTO vec_docs VALUES (3, 'Web development', VECTOR('[0.0, 0.0, 1.0]'))").await;
    exec(&ex, "INSERT INTO vec_docs VALUES (4, 'Data engineering', VECTOR('[0.7, 0.7, 0.0]'))").await;

    // Verify vectors stored correctly
    let results = exec(&ex, "SELECT embedding FROM vec_docs WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Vector(vec![1.0, 0.0, 0.0]));

    // VECTOR_DIMS
    let results = exec(&ex, "SELECT VECTOR_DIMS(embedding) FROM vec_docs WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(3));

    // VECTOR_DISTANCE — L2
    let results = exec(&ex,
        "SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') AS dist \
         FROM vec_docs WHERE id = 1"
    ).await;
    let r = rows(&results[0]);
    match &r[0][1] {
        Value::Float64(d) => assert!(d.abs() < 0.001, "self-distance should be ~0"),
        other => panic!("expected Float64, got {other:?}"),
    }

    // VECTOR_DISTANCE — cosine
    let results = exec(&ex,
        "SELECT VECTOR_DISTANCE(VECTOR('[1,0,0]'), VECTOR('[0,1,0]'), 'cosine')"
    ).await;
    match scalar(&results[0]) {
        Value::Float64(d) => assert!((*d - 1.0).abs() < 0.001, "orthogonal vectors cosine distance = 1"),
        other => panic!("expected Float64, got {other:?}"),
    }

    // ORDER BY vector distance (nearest neighbor search)
    let results = exec(&ex,
        "SELECT id, title FROM vec_docs \
         ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'l2') \
         LIMIT 2"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    // Closest to [1,0,0] should be id=1, then id=4 ([0.7,0.7,0])
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(4));

    // NORMALIZE
    let results = exec(&ex, "SELECT NORMALIZE(VECTOR('[3,4,0]'))").await;
    match scalar(&results[0]) {
        Value::Vector(v) => {
            assert!((v[0] - 0.6).abs() < 0.001);
            assert!((v[1] - 0.8).abs() < 0.001);
            assert!(v[2].abs() < 0.001);
        }
        other => panic!("expected Vector, got {other:?}"),
    }
}

// ======================================================================
// 12. Graph Operations — nodes, edges, shortest path
// ======================================================================

#[tokio::test]
async fn test_e2e_graph_operations() {
    let ex = test_executor();

    // Add nodes
    let res = exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"Alice\"}')").await;
    let alice_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert!(alice_id > 0);

    let res = exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"Bob\"}')").await;
    let bob_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };

    let res = exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"Charlie\"}')").await;
    let charlie_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };

    let res = exec(&ex, "SELECT GRAPH_ADD_NODE('Person', '{\"name\":\"Diana\"}')").await;
    let diana_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };

    // Add edges: Alice -> Bob -> Charlie, Alice -> Diana
    exec(&ex, &format!("SELECT GRAPH_ADD_EDGE({alice_id}, {bob_id}, 'KNOWS')")).await;
    exec(&ex, &format!("SELECT GRAPH_ADD_EDGE({bob_id}, {charlie_id}, 'KNOWS')")).await;
    exec(&ex, &format!("SELECT GRAPH_ADD_EDGE({alice_id}, {diana_id}, 'KNOWS')")).await;

    // Verify node count
    assert_eq!(ex.graph_store().read().node_count(), 4);

    // Shortest path: Alice -> Charlie (should be Alice -> Bob -> Charlie)
    let results = exec(&ex,
        &format!("SELECT GRAPH_SHORTEST_PATH({alice_id}, {charlie_id})")
    ).await;
    let path = match scalar(&results[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text path, got {other:?}"),
    };
    // Path should contain all three node IDs
    assert!(!path.is_empty(), "path should exist between Alice and Charlie");

    // Shortest path: Alice -> Diana (direct)
    let results = exec(&ex,
        &format!("SELECT GRAPH_SHORTEST_PATH({alice_id}, {diana_id})")
    ).await;
    let path = match scalar(&results[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text path, got {other:?}"),
    };
    assert!(!path.is_empty(), "direct path should exist");
}

// ======================================================================
// 13. Document Operations — doc_insert, doc_get, doc_query
// ======================================================================

#[tokio::test]
async fn test_e2e_document_operations() {
    let ex = test_executor();

    // Insert JSON documents
    let res = exec(&ex, r#"SELECT doc_insert('{"type":"user","name":"Alice","role":"admin","age":30}')"#).await;
    let id1 = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert_eq!(id1, 1);

    let res = exec(&ex, r#"SELECT doc_insert('{"type":"user","name":"Bob","role":"viewer","age":25}')"#).await;
    let id2 = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert_eq!(id2, 2);

    let res = exec(&ex, r#"SELECT doc_insert('{"type":"event","action":"login","user":"Alice"}')"#).await;
    let id3 = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert_eq!(id3, 3);

    // Get by ID
    let res = exec(&ex, "SELECT doc_get(1)").await;
    let text = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(text.contains("Alice"));
    assert!(text.contains("admin"));

    // Get missing doc
    let res = exec(&ex, "SELECT doc_get(999)").await;
    assert_eq!(scalar(&res[0]), &Value::Null);

    // Query by containment
    let res = exec(&ex, r#"SELECT doc_query('{"type":"user"}')"#).await;
    let ids = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    let id_set: std::collections::HashSet<&str> = ids.split(',').collect();
    assert!(id_set.contains("1"), "Alice should match");
    assert!(id_set.contains("2"), "Bob should match");
    assert!(!id_set.contains("3"), "event should not match user query");
}

// ======================================================================
// 14. TimeSeries — ts_insert, ts_range, ts_last
// ======================================================================

#[tokio::test]
async fn test_e2e_timeseries() {
    let ex = test_executor();

    // Insert time-series data
    exec(&ex, "SELECT ts_insert('cpu_usage', 1000, 45.0)").await;
    exec(&ex, "SELECT ts_insert('cpu_usage', 2000, 62.0)").await;
    exec(&ex, "SELECT ts_insert('cpu_usage', 3000, 38.0)").await;
    exec(&ex, "SELECT ts_insert('cpu_usage', 4000, 71.0)").await;
    exec(&ex, "SELECT ts_insert('cpu_usage', 5000, 55.0)").await;

    // Count
    let res = exec(&ex, "SELECT ts_count('cpu_usage')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(5));

    // Last value
    let res = exec(&ex, "SELECT ts_last('cpu_usage')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(55.0));

    // Range count: [2000, 4000) should contain timestamps 2000 and 3000
    let res = exec(&ex, "SELECT ts_range_count('cpu_usage', 2000, 4000)").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));

    // Range avg: [1000, 5000) should avg 45+62+38+71 = 216 / 4 = 54.0
    let res = exec(&ex, "SELECT ts_range_avg('cpu_usage', 1000, 5000)").await;
    match scalar(&res[0]) {
        Value::Float64(v) => assert!((*v - 54.0).abs() < 0.01, "avg should be 54.0, got {v}"),
        other => panic!("expected Float64, got {other:?}"),
    }

    // Count for missing series
    let res = exec(&ex, "SELECT ts_count('nonexistent_series')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));

    // Last for missing series
    let res = exec(&ex, "SELECT ts_last('nonexistent_series')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

// ======================================================================
// 15. Prepared Statements — Database::prepare() + execute_prepared()
// ======================================================================

#[tokio::test]
async fn test_e2e_prepared_statements() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE prep_users (id INT PRIMARY KEY, name TEXT, age INT)").await;
    exec(&ex, "INSERT INTO prep_users VALUES (1, 'Alice', 30)").await;
    exec(&ex, "INSERT INTO prep_users VALUES (2, 'Bob', 25)").await;
    exec(&ex, "INSERT INTO prep_users VALUES (3, 'Charlie', 35)").await;

    // Prepare a parameterized query
    let handle = ex.prepare("SELECT name, age FROM prep_users WHERE id = $1").unwrap();

    // Execute with different parameters
    let result = ex.execute_prepared(&handle, &[Value::Int32(1)]).await.unwrap();
    match &result {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("Alice".into()));
            assert_eq!(rows[0][1], Value::Int32(30));
        }
        _ => panic!("expected Select"),
    }

    let result = ex.execute_prepared(&handle, &[Value::Int32(2)]).await.unwrap();
    match &result {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("Bob".into()));
        }
        _ => panic!("expected Select"),
    }

    // Prepare an INSERT
    let insert_handle = ex.prepare("INSERT INTO prep_users VALUES ($1, $2, $3)").unwrap();
    let result = ex.execute_prepared(
        &insert_handle,
        &[Value::Int32(4), Value::Text("Diana".into()), Value::Int32(28)],
    ).await.unwrap();
    match &result {
        ExecResult::Command { tag, rows_affected } => {
            assert_eq!(tag, "INSERT");
            assert_eq!(*rows_affected, 1);
        }
        _ => panic!("expected Command"),
    }

    // Verify the insert worked
    let result = ex.execute_prepared(&handle, &[Value::Int32(4)]).await.unwrap();
    match &result {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("Diana".into()));
        }
        _ => panic!("expected Select"),
    }
}

// ======================================================================
// 16. NULL Handling — GREATEST/LEAST with NULLs, IS NULL, COALESCE
// ======================================================================

#[tokio::test]
async fn test_e2e_null_handling() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE null_test (id INT, name TEXT, value INT)").await;
    exec(&ex, "INSERT INTO null_test VALUES (1, 'Alice', 100)").await;
    exec(&ex, "INSERT INTO null_test VALUES (2, NULL, 200)").await;
    exec(&ex, "INSERT INTO null_test VALUES (3, 'Charlie', NULL)").await;
    exec(&ex, "INSERT INTO null_test VALUES (4, NULL, NULL)").await;

    // IS NULL
    let results = exec(&ex, "SELECT COUNT(*) FROM null_test WHERE name IS NULL").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));

    // IS NOT NULL
    let results = exec(&ex, "SELECT COUNT(*) FROM null_test WHERE value IS NOT NULL").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));

    // COALESCE — returns first non-NULL
    let results = exec(&ex, "SELECT COALESCE(NULL, NULL, 42)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(42));

    let results = exec(&ex, "SELECT id, COALESCE(name, 'Unknown') FROM null_test WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Text("Unknown".into()));

    // NULLIF
    let results = exec(&ex, "SELECT NULLIF(1, 1)").await;
    assert_eq!(*scalar(&results[0]), Value::Null);

    let results = exec(&ex, "SELECT NULLIF(1, 2)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(1));

    // GREATEST / LEAST — with NULLs
    let results = exec(&ex, "SELECT GREATEST(1, 5, 3)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(5));

    let results = exec(&ex, "SELECT LEAST(1, 5, 3)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(1));

    // NULL comparison behavior
    let results = exec(&ex, "SELECT COUNT(*) FROM null_test WHERE value > 0").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2), "NULL values should not match > comparison");
}

// ======================================================================
// 17. ALTER TABLE — ADD COLUMN, DROP COLUMN, ADD/DROP CONSTRAINT
// ======================================================================

#[tokio::test]
async fn test_e2e_alter_table() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE alter_test (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO alter_test VALUES (1, 'Alice')").await;
    exec(&ex, "INSERT INTO alter_test VALUES (2, 'Bob')").await;

    // ADD COLUMN
    exec(&ex, "ALTER TABLE alter_test ADD COLUMN age INT").await;
    let results = exec(&ex, "SELECT * FROM alter_test WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0].len(), 3, "should have 3 columns now");
    assert_eq!(r[0][2], Value::Null, "new column defaults to NULL");

    // ADD COLUMN with DEFAULT
    exec(&ex, "ALTER TABLE alter_test ADD COLUMN status TEXT DEFAULT 'active'").await;
    let results = exec(&ex, "SELECT status FROM alter_test WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Text("active".into()));

    // DROP COLUMN
    exec(&ex, "ALTER TABLE alter_test DROP COLUMN age").await;
    let results = exec(&ex, "SELECT * FROM alter_test WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0].len(), 3, "should have 3 columns after drop (id, name, status)");

    // ADD CONSTRAINT (UNIQUE)
    exec(&ex, "ALTER TABLE alter_test ADD CONSTRAINT uq_name UNIQUE (name)").await;
    let err = ex.execute("INSERT INTO alter_test VALUES (3, 'Alice', 'active')").await;
    assert!(err.is_err(), "unique constraint should prevent duplicate name");

    // DROP CONSTRAINT
    exec(&ex, "ALTER TABLE alter_test DROP CONSTRAINT uq_name").await;
    // Now duplicate should be allowed
    exec(&ex, "INSERT INTO alter_test VALUES (3, 'Alice', 'inactive')").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM alter_test WHERE name = 'Alice'").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));

    // ADD CHECK CONSTRAINT
    exec(&ex, "CREATE TABLE alter_check (id INT, age INT)").await;
    exec(&ex, "INSERT INTO alter_check VALUES (1, 25)").await;
    exec(&ex, "ALTER TABLE alter_check ADD CONSTRAINT ck_age CHECK (age >= 0)").await;
    let err = ex.execute("INSERT INTO alter_check VALUES (2, -5)").await;
    assert!(err.is_err(), "check constraint should prevent negative age");

    // RENAME COLUMN
    exec(&ex, "ALTER TABLE alter_test RENAME COLUMN status TO state").await;
    let results = exec(&ex, "SELECT state FROM alter_test WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Text("active".into()));
}

// ======================================================================
// 18. Edge Cases — empty tables, long strings, special chars
// ======================================================================

#[tokio::test]
async fn test_e2e_edge_cases() {
    let ex = test_executor();

    // Empty table queries
    exec(&ex, "CREATE TABLE empty_tbl (id INT, name TEXT, value FLOAT)").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM empty_tbl").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(0));

    let results = exec(&ex, "SELECT * FROM empty_tbl").await;
    assert_eq!(rows(&results[0]).len(), 0);

    let results = exec(&ex, "SELECT SUM(value) FROM empty_tbl").await;
    // SUM of empty set is NULL
    assert_eq!(*scalar(&results[0]), Value::Null);

    // Very long strings
    let long_str = "x".repeat(10_000);
    exec(&ex, "CREATE TABLE long_str_tbl (id INT, data TEXT)").await;
    exec(&ex, &format!("INSERT INTO long_str_tbl VALUES (1, '{long_str}')")).await;
    let results = exec(&ex, "SELECT LENGTH(data) FROM long_str_tbl WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(10_000));

    // Special characters in data
    exec(&ex, "CREATE TABLE special_chars (id INT, data TEXT)").await;
    exec(&ex, "INSERT INTO special_chars VALUES (1, 'hello world')").await;
    exec(&ex, "INSERT INTO special_chars VALUES (2, 'quotes: don''t stop')").await;
    exec(&ex, "INSERT INTO special_chars VALUES (3, 'unicode: cafe')").await;
    exec(&ex, "INSERT INTO special_chars VALUES (4, 'tabs\tand\nnewlines')").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM special_chars").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(4));

    // Verify special chars round-trip
    let results = exec(&ex, "SELECT data FROM special_chars WHERE id = 2").await;
    assert_eq!(*scalar(&results[0]), Value::Text("quotes: don't stop".into()));

    // Zero-row UPDATE/DELETE
    exec(&ex, "CREATE TABLE zero_ops (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO zero_ops VALUES (1, 'a')").await;
    let results = exec(&ex, "UPDATE zero_ops SET val = 'b' WHERE id = 999").await;
    match &results[0] {
        ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 0),
        _ => panic!("expected Command"),
    }
    let results = exec(&ex, "DELETE FROM zero_ops WHERE id = 999").await;
    match &results[0] {
        ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 0),
        _ => panic!("expected Command"),
    }

    // LIMIT 0
    exec(&ex, "INSERT INTO zero_ops VALUES (2, 'b'), (3, 'c')").await;
    let results = exec(&ex, "SELECT * FROM zero_ops LIMIT 0").await;
    assert_eq!(rows(&results[0]).len(), 0);

    // SELECT with DISTINCT
    exec(&ex, "CREATE TABLE dup_vals (val TEXT)").await;
    exec(&ex, "INSERT INTO dup_vals VALUES ('a'), ('b'), ('a'), ('c'), ('b')").await;
    let results = exec(&ex, "SELECT DISTINCT val FROM dup_vals ORDER BY val").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);

    // BETWEEN
    exec(&ex, "CREATE TABLE between_test (n INT)").await;
    exec(&ex, "INSERT INTO between_test VALUES (1),(2),(3),(4),(5)").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM between_test WHERE n BETWEEN 2 AND 4").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));

    // IN list
    let results = exec(&ex, "SELECT COUNT(*) FROM between_test WHERE n IN (1, 3, 5)").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));
}

// ======================================================================
// 19. String and Math Functions
// ======================================================================

#[tokio::test]
async fn test_e2e_builtin_functions() {
    let ex = test_executor();

    // String functions
    let results = exec(&ex, "SELECT UPPER('hello')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("HELLO".into()));

    let results = exec(&ex, "SELECT LOWER('WORLD')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("world".into()));

    let results = exec(&ex, "SELECT LENGTH('hello')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(5));

    let results = exec(&ex, "SELECT TRIM('  hi  ')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("hi".into()));

    let results = exec(&ex, "SELECT CONCAT('hello', ' ', 'world')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("hello world".into()));

    let results = exec(&ex, "SELECT SUBSTRING('hello world', 7, 5)").await;
    assert_eq!(*scalar(&results[0]), Value::Text("world".into()));

    let results = exec(&ex, "SELECT REPLACE('hello world', 'world', 'nucleus')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("hello nucleus".into()));

    // Math functions
    let results = exec(&ex, "SELECT ABS(-42)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(42));

    let results = exec(&ex, "SELECT CEIL(3.2)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(4.0));

    let results = exec(&ex, "SELECT FLOOR(3.8)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(3.0));

    let results = exec(&ex, "SELECT SQRT(16.0)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(4.0));

    let results = exec(&ex, "SELECT POWER(2.0, 10.0)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(1024.0));

    // Type introspection
    let results = exec(&ex, "SELECT PG_TYPEOF(42)").await;
    assert_eq!(*scalar(&results[0]), Value::Text("integer".into()));
}

// ======================================================================
// 20. MVCC Transaction Isolation
// ======================================================================

#[tokio::test]
async fn test_e2e_mvcc_isolation() {
    // Use MVCC executor for snapshot isolation tests
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(
        crate::storage::MvccStorageAdapter::new()
    );
    let ex = Executor::new(catalog, storage);

    exec(&ex, "CREATE TABLE mvcc_test (id INT, val TEXT)").await;

    // Cycle 1: INSERT + COMMIT
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO mvcc_test VALUES (1, 'committed')").await;
    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT * FROM mvcc_test").await;
    assert_eq!(rows(&results[0]).len(), 1, "committed row should persist");

    // Cycle 2: INSERT + ROLLBACK — row should vanish
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO mvcc_test VALUES (2, 'phantom')").await;
    exec(&ex, "ROLLBACK").await;

    let results = exec(&ex, "SELECT * FROM mvcc_test").await;
    assert_eq!(rows(&results[0]).len(), 1, "rolled back insert should vanish");

    // Cycle 3: another COMMIT
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO mvcc_test VALUES (3, 'also_committed')").await;
    exec(&ex, "COMMIT").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM mvcc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2)); // rows 1 and 3

    // Verify UPDATE + ROLLBACK preserves original
    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE mvcc_test SET val = 'changed' WHERE id = 1").await;
    exec(&ex, "ROLLBACK").await;

    let results = exec(&ex, "SELECT val FROM mvcc_test WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("committed".into()));

    // Verify DELETE + ROLLBACK preserves row
    exec(&ex, "BEGIN").await;
    exec(&ex, "DELETE FROM mvcc_test WHERE id = 3").await;
    exec(&ex, "ROLLBACK").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM mvcc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));
}

// ======================================================================
// 21. Cross-Model Integration — combining multiple models in one workflow
// ======================================================================

#[tokio::test]
async fn test_e2e_cross_model_saas_workflow() {
    let ex = test_executor();

    // --- Set up relational tables ---
    exec(&ex, "CREATE TABLE cm_users (id INT PRIMARY KEY, name TEXT, email TEXT)").await;
    exec(&ex, "INSERT INTO cm_users VALUES (1, 'Alice', 'alice@example.com')").await;
    exec(&ex, "INSERT INTO cm_users VALUES (2, 'Bob', 'bob@example.com')").await;

    // --- KV: session tokens ---
    exec(&ex, "SELECT kv_set('session:alice', 'tok_alice_123')").await;
    exec(&ex, "SELECT kv_set('session:bob', 'tok_bob_456')").await;

    // --- FTS: index user profiles ---
    exec(&ex, "SELECT fts_index(1, 'Alice is a senior engineer working on databases')").await;
    exec(&ex, "SELECT fts_index(2, 'Bob is a product manager focused on growth')").await;

    // --- TimeSeries: activity metrics ---
    exec(&ex, "SELECT ts_insert('user:1:logins', 1000, 1.0)").await;
    exec(&ex, "SELECT ts_insert('user:1:logins', 2000, 1.0)").await;
    exec(&ex, "SELECT ts_insert('user:1:logins', 3000, 1.0)").await;
    exec(&ex, "SELECT ts_insert('user:2:logins', 1000, 1.0)").await;

    // --- Graph: social connections ---
    exec(&ex, "SELECT GRAPH_ADD_NODE('User', '{\"id\":1}')").await;
    exec(&ex, "SELECT GRAPH_ADD_NODE('User', '{\"id\":2}')").await;
    exec(&ex, "SELECT GRAPH_ADD_EDGE(1, 2, 'FOLLOWS')").await;

    // --- Document: user preferences ---
    exec(&ex, r#"SELECT doc_insert('{"user_id":1,"theme":"dark","notifications":true}')"#).await;
    exec(&ex, r#"SELECT doc_insert('{"user_id":2,"theme":"light","notifications":false}')"#).await;

    // --- Verify cross-model data ---

    // Relational query
    let results = exec(&ex, "SELECT name FROM cm_users ORDER BY name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("Alice".into()));

    // KV session lookup
    let res = exec(&ex, "SELECT kv_get('session:alice')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("tok_alice_123".into()));

    // FTS search
    let res = exec(&ex, "SELECT fts_search('engineer database', 10)").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("\"doc_id\":1"), "should find Alice's profile");

    // TimeSeries count
    let res = exec(&ex, "SELECT ts_count('user:1:logins')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(3));

    // Graph path
    let results = exec(&ex, "SELECT GRAPH_SHORTEST_PATH(1, 2)").await;
    match scalar(&results[0]) {
        Value::Text(s) => assert!(!s.is_empty(), "path should exist"),
        other => panic!("expected Text path, got {other:?}"),
    }

    // Document query
    let res = exec(&ex, r#"SELECT doc_query('{"user_id":1}')"#).await;
    let ids = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(ids.contains("1"), "should find Alice's preferences");
}

// ======================================================================
// 22. Views, CTEs, and SET operations
// ======================================================================

#[tokio::test]
async fn test_e2e_views_ctes_set_ops() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE v_orders (id INT, product TEXT, region TEXT, amount FLOAT)").await;
    exec(&ex, "INSERT INTO v_orders VALUES (1, 'Widget', 'East', 100.0)").await;
    exec(&ex, "INSERT INTO v_orders VALUES (2, 'Gadget', 'West', 200.0)").await;
    exec(&ex, "INSERT INTO v_orders VALUES (3, 'Widget', 'East', 150.0)").await;
    exec(&ex, "INSERT INTO v_orders VALUES (4, 'Widget', 'West', 75.0)").await;

    // CREATE VIEW
    exec(&ex, "CREATE VIEW east_orders AS SELECT * FROM v_orders WHERE region = 'East'").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM east_orders").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));

    // CTE
    let results = exec(&ex,
        "WITH summary AS (
            SELECT product, SUM(amount) AS total
            FROM v_orders GROUP BY product
        )
        SELECT product, total FROM summary ORDER BY total DESC"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("Widget".into())); // 325.0

    // UNION
    exec(&ex, "CREATE TABLE v_products1 (name TEXT)").await;
    exec(&ex, "INSERT INTO v_products1 VALUES ('A'), ('B')").await;
    exec(&ex, "CREATE TABLE v_products2 (name TEXT)").await;
    exec(&ex, "INSERT INTO v_products2 VALUES ('B'), ('C')").await;
    let results = exec(&ex, "SELECT name FROM v_products1 UNION SELECT name FROM v_products2 ORDER BY name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3); // A, B, C (distinct)

    // INTERSECT
    let results = exec(&ex, "SELECT name FROM v_products1 INTERSECT SELECT name FROM v_products2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("B".into()));

    // EXCEPT
    let results = exec(&ex, "SELECT name FROM v_products1 EXCEPT SELECT name FROM v_products2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("A".into()));
}

// ======================================================================
// 23. LIKE, CASE, type casting
// ======================================================================

#[tokio::test]
async fn test_e2e_patterns_and_expressions() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE pat_test (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO pat_test VALUES ('Alice', 95)").await;
    exec(&ex, "INSERT INTO pat_test VALUES ('Bob', 60)").await;
    exec(&ex, "INSERT INTO pat_test VALUES ('Anna', 88)").await;
    exec(&ex, "INSERT INTO pat_test VALUES ('alex', 72)").await;

    // LIKE
    let results = exec(&ex, "SELECT name FROM pat_test WHERE name LIKE 'A%' ORDER BY name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2); // Alice, Anna

    // ILIKE (case-insensitive)
    let results = exec(&ex, "SELECT name FROM pat_test WHERE name ILIKE 'a%' ORDER BY name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3); // Alice, Anna, alex

    // CASE expression
    let results = exec(&ex,
        "SELECT name, CASE
            WHEN score >= 90 THEN 'A'
            WHEN score >= 80 THEN 'B'
            WHEN score >= 70 THEN 'C'
            ELSE 'F'
         END AS grade
         FROM pat_test ORDER BY name"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Text("A".into()));  // Alice: 95
    assert_eq!(r[1][1], Value::Text("B".into()));  // Anna: 88
    assert_eq!(r[2][1], Value::Text("F".into()));  // Bob: 60
    assert_eq!(r[3][1], Value::Text("C".into()));  // alex: 72

    // Arithmetic expressions
    let results = exec(&ex, "SELECT 2 + 3 * 4").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(14));

    let results = exec(&ex, "SELECT (2 + 3) * 4").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(20));
}

// ======================================================================
// 24. JSONB operations
// ======================================================================

#[tokio::test]
async fn test_e2e_jsonb_operations() {
    let ex = test_executor();

    // JSONB_BUILD_OBJECT
    let results = exec(&ex, "SELECT JSONB_BUILD_OBJECT('name', 'Alice', 'age', 30)").await;
    match scalar(&results[0]) {
        Value::Jsonb(v) => {
            assert_eq!(v["name"], "Alice");
            assert_eq!(v["age"], 30);
        }
        other => panic!("expected Jsonb, got {other:?}"),
    }

    // JSONB_BUILD_ARRAY
    let results = exec(&ex, "SELECT JSONB_BUILD_ARRAY(1, 2, 3)").await;
    match scalar(&results[0]) {
        Value::Jsonb(serde_json::Value::Array(arr)) => assert_eq!(arr.len(), 3),
        other => panic!("expected Jsonb array, got {other:?}"),
    }

    // JSON path extraction
    exec(&ex, "CREATE TABLE json_test (data TEXT)").await;
    exec(&ex, r#"INSERT INTO json_test VALUES ('{"a":{"b":42}}')"#).await;

    let results = exec(&ex, r#"SELECT data::jsonb #> '{a,b}' FROM json_test"#).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Jsonb(serde_json::json!(42)));

    let results = exec(&ex, r#"SELECT data::jsonb #>> '{a,b}' FROM json_test"#).await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("42".to_string()));
}

// ======================================================================
// 25. Sequences and SERIAL
// ======================================================================

#[tokio::test]
async fn test_e2e_sequences_and_serial() {
    let ex = test_executor();

    // Explicit sequence
    exec(&ex, "CREATE SEQUENCE order_seq INCREMENT BY 1 START WITH 1000").await;
    let results = exec(&ex, "SELECT NEXTVAL('order_seq')").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1000));
    let results = exec(&ex, "SELECT NEXTVAL('order_seq')").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1001));
    let results = exec(&ex, "SELECT CURRVAL('order_seq')").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1001));

    // SERIAL column
    exec(&ex, "CREATE TABLE serial_test (id SERIAL PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO serial_test (name) VALUES ('first')").await;
    exec(&ex, "INSERT INTO serial_test (name) VALUES ('second')").await;
    exec(&ex, "INSERT INTO serial_test (name) VALUES ('third')").await;

    let results = exec(&ex, "SELECT id, name FROM serial_test ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[2][0], Value::Int32(3));
}

// ======================================================================
// 26. Foreign Key enforcement
// ======================================================================

#[tokio::test]
async fn test_e2e_foreign_keys() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE fk_orgs (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO fk_orgs VALUES (1, 'Acme')").await;
    exec(&ex, "INSERT INTO fk_orgs VALUES (2, 'Globex')").await;

    exec(&ex, "CREATE TABLE fk_members (id INT, org_id INT REFERENCES fk_orgs(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO fk_members VALUES (1, 1)").await;
    exec(&ex, "INSERT INTO fk_members VALUES (2, 1)").await;
    exec(&ex, "INSERT INTO fk_members VALUES (3, 2)").await;

    // Invalid FK should fail
    let err = ex.execute("INSERT INTO fk_members VALUES (4, 999)").await;
    assert!(err.is_err(), "FK violation should fail");

    // CASCADE DELETE
    exec(&ex, "DELETE FROM fk_orgs WHERE id = 1").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM fk_members").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1), "cascade should delete members of org 1");

    let results = exec(&ex, "SELECT id FROM fk_members").await;
    assert_eq!(rows(&results[0])[0][0], Value::Int32(3), "only member 3 (org 2) should remain");
}

// ======================================================================
// 27. TRUNCATE and DROP TABLE
// ======================================================================

#[tokio::test]
async fn test_e2e_truncate_and_drop() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE trunc_test (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO trunc_test VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));

    exec(&ex, "TRUNCATE TABLE trunc_test").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(0));

    // Table still exists, can insert again
    exec(&ex, "INSERT INTO trunc_test VALUES (10, 'new')").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1));

    // DROP TABLE
    exec(&ex, "DROP TABLE trunc_test").await;
    let err = ex.execute("SELECT * FROM trunc_test").await;
    assert!(err.is_err(), "dropped table should not be queryable");

    // DROP TABLE IF EXISTS (no error on nonexistent)
    exec(&ex, "DROP TABLE IF EXISTS trunc_test").await;
}

// ======================================================================
// 28. INSERT ... SELECT, multi-row VALUES
// ======================================================================

#[tokio::test]
async fn test_e2e_insert_select_and_multi_row() {
    let ex = test_executor();

    // Multi-row INSERT
    exec(&ex, "CREATE TABLE multi_src (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO multi_src VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM multi_src").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(5));

    // INSERT ... SELECT
    exec(&ex, "CREATE TABLE multi_dst (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO multi_dst SELECT * FROM multi_src WHERE id > 2").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM multi_dst").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));

    let results = exec(&ex, "SELECT id FROM multi_dst ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(3));
    assert_eq!(r[1][0], Value::Int32(4));
    assert_eq!(r[2][0], Value::Int32(5));
}

// ======================================================================
// 29. Datalog operations
// ======================================================================

#[tokio::test]
async fn test_e2e_datalog() {
    let ex = test_executor();

    // Assert facts
    exec(&ex, "SELECT DATALOG_ASSERT('parent(tom, bob)')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('parent(tom, liz)')").await;
    exec(&ex, "SELECT DATALOG_ASSERT('parent(bob, ann)')").await;

    // Query
    let res = exec(&ex, "SELECT DATALOG_QUERY('parent(tom, X)')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("bob") || json.contains("liz"), "should find tom's children: {json}");

    // Query with different pattern
    let res = exec(&ex, "SELECT DATALOG_QUERY('parent(X, ann)')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("bob"), "ann's parent should be bob: {json}");
}

// ======================================================================
// 30. Columnar store operations
// ======================================================================

#[tokio::test]
async fn test_e2e_columnar() {
    let ex = test_executor();

    // Insert into columnar store
    exec(&ex, "SELECT columnar_insert('metrics', 'value', 10, 'host', 'web1')").await;
    exec(&ex, "SELECT columnar_insert('metrics', 'value', 20, 'host', 'web2')").await;
    exec(&ex, "SELECT columnar_insert('metrics', 'value', 30, 'host', 'web3')").await;

    // Count
    let res = exec(&ex, "SELECT columnar_count('metrics')").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(3));

    // Sum
    let res = exec(&ex, "SELECT columnar_sum('metrics', 'value')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(60.0));

    // Avg
    let res = exec(&ex, "SELECT columnar_avg('metrics', 'value')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(20.0));

    // Min/Max
    let res = exec(&ex, "SELECT columnar_min('metrics', 'value')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(10.0));
    let res = exec(&ex, "SELECT columnar_max('metrics', 'value')").await;
    assert_eq!(scalar(&res[0]), &Value::Float64(30.0));
}
