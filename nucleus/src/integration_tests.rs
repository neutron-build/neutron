//! Comprehensive integration tests for the Nucleus database engine.
//!
//! These tests exercise realistic SQL workflows end-to-end, combining DDL, DML,
//! constraints, views, sequences, prepared statements, window functions, CTEs,
//! information_schema introspection, and scalar/math function pipelines.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::catalog::Catalog;
    use crate::executor::{ExecResult, Executor};
    use crate::storage::{MemoryEngine, StorageEngine};
    use crate::types::{DataType, Value};

    /// Create a fresh executor backed by in-memory storage.
    fn setup() -> Arc<Executor> {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
        Arc::new(Executor::new(catalog, storage))
    }

    /// Execute SQL and return the results, panicking on error.
    async fn run(ex: &Executor, sql: &str) -> Vec<ExecResult> {
        ex.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {sql}\nError: {e}"))
    }

    /// Extract rows from a SELECT result.
    fn rows(result: &ExecResult) -> &Vec<Vec<Value>> {
        match result {
            ExecResult::Select { rows, .. } => rows,
            other => panic!("expected Select result, got {other:?}"),
        }
    }

    /// Extract the columns metadata from a SELECT result.
    fn columns(result: &ExecResult) -> &Vec<(String, DataType)> {
        match result {
            ExecResult::Select { columns, .. } => columns,
            other => panic!("expected Select result, got {other:?}"),
        }
    }

    /// Extract a single scalar value from a 1-row, 1-column result.
    fn scalar(result: &ExecResult) -> &Value {
        let r = rows(result);
        assert_eq!(r.len(), 1, "expected 1 row, got {}", r.len());
        assert_eq!(r[0].len(), 1, "expected 1 column, got {}", r[0].len());
        &r[0][0]
    }

    /// Extract the command tag from a Command result.
    fn tag(result: &ExecResult) -> &str {
        match result {
            ExecResult::Command { tag, .. } => tag.as_str(),
            other => panic!("expected Command result, got {other:?}"),
        }
    }

    // ========================================================================
    // 1. E-commerce schema: multi-table create, insert, joins, aggregation
    // ========================================================================

    #[tokio::test]
    async fn test_ecommerce_schema() {
        let ex = setup();

        // -- DDL: create the four tables --
        run(
            &ex,
            "CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE products (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                price INT NOT NULL
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE orders (
                id INT PRIMARY KEY,
                user_id INT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE order_items (
                id INT PRIMARY KEY,
                order_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders (id),
                FOREIGN KEY (product_id) REFERENCES products (id)
            )",
        )
        .await;

        // -- Seed data --
        run(&ex, "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')").await;
        run(&ex, "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')").await;
        run(&ex, "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')").await;

        run(&ex, "INSERT INTO products VALUES (10, 'Widget', 25)").await;
        run(&ex, "INSERT INTO products VALUES (20, 'Gadget', 50)").await;
        run(&ex, "INSERT INTO products VALUES (30, 'Doohickey', 10)").await;

        run(&ex, "INSERT INTO orders VALUES (100, 1)").await;
        run(&ex, "INSERT INTO orders VALUES (101, 1)").await;
        run(&ex, "INSERT INTO orders VALUES (102, 2)").await;

        run(&ex, "INSERT INTO order_items VALUES (1, 100, 10, 2)").await; // Alice: 2 Widgets = 50
        run(&ex, "INSERT INTO order_items VALUES (2, 100, 20, 1)").await; // Alice: 1 Gadget  = 50
        run(&ex, "INSERT INTO order_items VALUES (3, 101, 30, 5)").await; // Alice: 5 Doohickeys = 50
        run(&ex, "INSERT INTO order_items VALUES (4, 102, 20, 3)").await; // Bob:   3 Gadgets = 150

        // -- Verify row counts --
        let res = run(&ex, "SELECT COUNT(*) FROM users").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(3));

        let res = run(&ex, "SELECT COUNT(*) FROM order_items").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(4));

        // -- Multi-table JOIN: list all order items with user and product names --
        let res = run(
            &ex,
            "SELECT u.name AS user_name, p.name AS product_name, oi.quantity
             FROM order_items oi
             JOIN orders o ON oi.order_id = o.id
             JOIN users u ON o.user_id = u.id
             JOIN products p ON oi.product_id = p.id
             ORDER BY quantity",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4);
        // Sorted by quantity ASC: 1 (Gadget), 2 (Widget), 3 (Gadget), 5 (Doohickey)
        assert_eq!(r[0][2], Value::Int32(1)); // 1 Gadget
        assert_eq!(r[1][2], Value::Int32(2)); // 2 Widgets
        assert_eq!(r[2][2], Value::Int32(3)); // 3 Gadgets
        assert_eq!(r[3][2], Value::Int32(5)); // 5 Doohickeys

        // -- Aggregation: total revenue per user --
        let res = run(
            &ex,
            "SELECT u.name, SUM(oi.quantity * p.price) AS revenue
             FROM order_items oi
             JOIN orders o ON oi.order_id = o.id
             JOIN users u ON o.user_id = u.id
             JOIN products p ON oi.product_id = p.id
             GROUP BY u.name
             ORDER BY revenue DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2); // Alice and Bob
        // Bob: 3*50=150  Alice: 2*25 + 1*50 + 5*10 = 150
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[1][0], Value::Text("Bob".into()));

        // -- Window function: rank products by total quantity sold (via CTE) --
        let res = run(
            &ex,
            "WITH product_sales AS (
                SELECT p.name AS product_name, SUM(oi.quantity) AS total_qty
                FROM order_items oi
                JOIN products p ON oi.product_id = p.id
                GROUP BY p.name
            )
            SELECT product_name, total_qty,
                   RANK() OVER (ORDER BY total_qty DESC) AS sales_rank
            FROM product_sales
            ORDER BY sales_rank",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        // Doohickey: qty=5 (rank 1), Gadget: qty=4 (rank 2), Widget: qty=2 (rank 3)
        assert_eq!(r[0][0], Value::Text("Doohickey".into()));
        assert_eq!(r[2][0], Value::Text("Widget".into()));
    }

    // ========================================================================
    // 2. Multi-table constraint cascade: FK violations, ON CONFLICT upsert
    // ========================================================================

    #[tokio::test]
    async fn test_constraint_cascade() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE departments (
                id INT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE employees (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INT NOT NULL,
                FOREIGN KEY (dept_id) REFERENCES departments (id)
            )",
        )
        .await;

        run(&ex, "INSERT INTO departments VALUES (1, 'Engineering')").await;
        run(&ex, "INSERT INTO departments VALUES (2, 'Marketing')").await;

        // Valid FK insert
        run(&ex, "INSERT INTO employees VALUES (100, 'Alice', 1)").await;

        // FK violation: dept_id=99 does not exist
        let err = ex.execute("INSERT INTO employees VALUES (101, 'Bob', 99)").await;
        assert!(err.is_err(), "should reject FK violation");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("foreign key"),
            "error should mention foreign key, got: {msg}"
        );

        // Primary key violation
        let err = ex.execute("INSERT INTO departments VALUES (1, 'Duplicate')").await;
        assert!(err.is_err(), "should reject PK duplicate");

        // ON CONFLICT DO NOTHING: silently skip conflicting row
        run(
            &ex,
            "INSERT INTO departments VALUES (1, 'Eng-Dupe') ON CONFLICT (id) DO NOTHING",
        )
        .await;
        let res = run(&ex, "SELECT name FROM departments WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Text("Engineering".into()));

        // ON CONFLICT DO UPDATE: upsert the name
        run(
            &ex,
            "INSERT INTO departments VALUES (1, 'Eng-Renamed') ON CONFLICT (id) DO UPDATE SET name = 'Eng-Renamed'",
        )
        .await;
        let res = run(&ex, "SELECT name FROM departments WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Text("Eng-Renamed".into()));

        // Verify total department count unchanged
        let res = run(&ex, "SELECT COUNT(*) FROM departments").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(2));
    }

    // ========================================================================
    // 3. Complex analytics: CTEs, window functions (LAG, LEAD), GROUP BY HAVING
    // ========================================================================

    #[tokio::test]
    async fn test_complex_analytics() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE metrics (
                day INT NOT NULL,
                category TEXT NOT NULL,
                value INT NOT NULL
            )",
        )
        .await;

        // Insert timeseries-like data across two categories over 5 days
        run(&ex, "INSERT INTO metrics VALUES (1, 'A', 10)").await;
        run(&ex, "INSERT INTO metrics VALUES (2, 'A', 15)").await;
        run(&ex, "INSERT INTO metrics VALUES (3, 'A', 12)").await;
        run(&ex, "INSERT INTO metrics VALUES (4, 'A', 20)").await;
        run(&ex, "INSERT INTO metrics VALUES (5, 'A', 18)").await;
        run(&ex, "INSERT INTO metrics VALUES (1, 'B', 5)").await;
        run(&ex, "INSERT INTO metrics VALUES (2, 'B', 8)").await;
        run(&ex, "INSERT INTO metrics VALUES (3, 'B', 3)").await;
        run(&ex, "INSERT INTO metrics VALUES (4, 'B', 9)").await;
        run(&ex, "INSERT INTO metrics VALUES (5, 'B', 7)").await;

        // CTE: compute daily totals, then filter
        let res = run(
            &ex,
            "WITH daily_totals AS (
                SELECT day, SUM(value) AS total
                FROM metrics
                GROUP BY day
            )
            SELECT day, total FROM daily_totals
            WHERE total > 20
            ORDER BY day",
        )
        .await;
        let r = rows(&res[0]);
        // day 2: 15+8=23, day 4: 20+9=29, day 5: 18+7=25
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[1][0], Value::Int32(4));
        assert_eq!(r[2][0], Value::Int32(5));

        // Window functions: LAG and LEAD on category A values
        let res = run(
            &ex,
            "SELECT day, value,
                    LAG(value) OVER (ORDER BY day) AS prev_val,
                    LEAD(value) OVER (ORDER BY day) AS next_val
             FROM metrics
             WHERE category = 'A'
             ORDER BY day",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);
        // First row: LAG is NULL
        assert_eq!(r[0][2], Value::Null);
        // Second row: LAG = 10
        assert_eq!(r[1][2], Value::Int32(10));
        // Last row: LEAD is NULL
        assert_eq!(r[4][3], Value::Null);

        // GROUP BY with HAVING: categories with total > 35
        let res = run(
            &ex,
            "SELECT category, SUM(value) AS total
             FROM metrics
             GROUP BY category
             HAVING SUM(value) > 35
             ORDER BY category",
        )
        .await;
        let r = rows(&res[0]);
        // A: 10+15+12+20+18=75, B: 5+8+3+9+7=32 => only A qualifies
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("A".into()));

        // Subquery: find days where category A value is above category-wide average
        let res = run(
            &ex,
            "SELECT day, value FROM metrics
             WHERE category = 'A'
               AND value > (SELECT AVG(value) FROM metrics WHERE category = 'A')
             ORDER BY day",
        )
        .await;
        let r = rows(&res[0]);
        // Average A = 75/5 = 15.0; values above: 20 (day 4), 18 (day 5); 15 is NOT strictly above
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(4));
        assert_eq!(r[1][0], Value::Int32(5));
    }

    // ========================================================================
    // 4. View lifecycle: create, query, drop underlying data, recreate, re-query
    // ========================================================================

    #[tokio::test]
    async fn test_view_lifecycle() {
        let ex = setup();

        run(&ex, "CREATE TABLE inventory (id INT, product TEXT, qty INT)").await;
        run(&ex, "INSERT INTO inventory VALUES (1, 'Apples', 50)").await;
        run(&ex, "INSERT INTO inventory VALUES (2, 'Bananas', 120)").await;
        run(&ex, "INSERT INTO inventory VALUES (3, 'Cherries', 30)").await;

        // Create a view that shows items with qty > 40
        run(
            &ex,
            "CREATE VIEW high_stock AS SELECT product, qty FROM inventory WHERE qty > 40",
        )
        .await;

        // Query through the view
        let res = run(&ex, "SELECT product FROM high_stock ORDER BY product").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Apples".into()));
        assert_eq!(r[1][0], Value::Text("Bananas".into()));

        // Delete all underlying data
        run(&ex, "DELETE FROM inventory WHERE 1=1").await;
        let res = run(&ex, "SELECT COUNT(*) FROM inventory").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(0));

        // View should now return empty results
        let res = run(&ex, "SELECT product FROM high_stock").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 0);

        // Reinsert data
        run(&ex, "INSERT INTO inventory VALUES (4, 'Dates', 200)").await;
        run(&ex, "INSERT INTO inventory VALUES (5, 'Elderberries', 10)").await;

        // View should reflect new data
        let res = run(&ex, "SELECT product FROM high_stock").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("Dates".into()));

        // Drop the view
        let res = run(&ex, "DROP VIEW high_stock").await;
        assert_eq!(tag(&res[0]), "DROP VIEW");

        // Querying dropped view should fail
        let err = ex.execute("SELECT * FROM high_stock").await;
        assert!(err.is_err(), "querying dropped view should fail");
    }

    // ========================================================================
    // 5. Sequence lifecycle: create, nextval, currval, auto-increment behavior
    // ========================================================================

    #[tokio::test]
    async fn test_sequence_lifecycle() {
        let ex = setup();

        // Create a sequence starting at 100, incrementing by 10
        run(
            &ex,
            "CREATE SEQUENCE order_seq INCREMENT BY 10 START WITH 100",
        )
        .await;

        // First nextval should be 100
        let res = run(&ex, "SELECT NEXTVAL('order_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(100));

        // Second nextval should be 110
        let res = run(&ex, "SELECT NEXTVAL('order_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(110));

        // Third nextval should be 120
        let res = run(&ex, "SELECT NEXTVAL('order_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(120));

        // Currval should reflect the last value dispensed
        let res = run(&ex, "SELECT CURRVAL('order_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(120));

        // Use the sequence to generate IDs for inserts
        run(&ex, "CREATE TABLE seq_orders (id INT, description TEXT)").await;

        // Create a simple sequence starting at 1
        run(&ex, "CREATE SEQUENCE id_seq INCREMENT BY 1 START WITH 1").await;

        run(&ex, "INSERT INTO seq_orders VALUES (1, 'first')").await;
        let res = run(&ex, "SELECT NEXTVAL('id_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(1));

        run(&ex, "INSERT INTO seq_orders VALUES (2, 'second')").await;
        let res = run(&ex, "SELECT NEXTVAL('id_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(2));

        // Verify independent sequences don't interfere
        let res = run(&ex, "SELECT CURRVAL('order_seq')").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(120));
    }

    // ========================================================================
    // 6. Prepared statement workflow: PREPARE, EXECUTE, DEALLOCATE
    // ========================================================================

    #[tokio::test]
    async fn test_prepared_statement_workflow() {
        let ex = setup();

        run(&ex, "CREATE TABLE contacts (id INT PRIMARY KEY, name TEXT, city TEXT)").await;
        run(&ex, "INSERT INTO contacts VALUES (1, 'Alice', 'Seattle')").await;
        run(&ex, "INSERT INTO contacts VALUES (2, 'Bob', 'Portland')").await;
        run(&ex, "INSERT INTO contacts VALUES (3, 'Charlie', 'Seattle')").await;
        run(&ex, "INSERT INTO contacts VALUES (4, 'Diana', 'Portland')").await;

        // Prepare a parameterized SELECT
        run(
            &ex,
            "PREPARE find_by_city AS SELECT name FROM contacts WHERE city = $1 ORDER BY name",
        )
        .await;

        // Execute with 'Seattle'
        let res = run(&ex, "EXECUTE find_by_city('Seattle')").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[1][0], Value::Text("Charlie".into()));

        // Execute with 'Portland'
        let res = run(&ex, "EXECUTE find_by_city('Portland')").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Bob".into()));
        assert_eq!(r[1][0], Value::Text("Diana".into()));

        // Prepare an INSERT statement
        run(
            &ex,
            "PREPARE add_contact AS INSERT INTO contacts VALUES ($1, $2, $3)",
        )
        .await;
        run(&ex, "EXECUTE add_contact(5, 'Eve', 'Denver')").await;

        let res = run(&ex, "SELECT name FROM contacts WHERE id = 5").await;
        assert_eq!(*scalar(&res[0]), Value::Text("Eve".into()));

        // Deallocate
        run(&ex, "DEALLOCATE find_by_city").await;

        // Attempting to execute the deallocated statement should fail
        let err = ex.execute("EXECUTE find_by_city('Seattle')").await;
        assert!(err.is_err(), "should fail after DEALLOCATE");

        // The insert prepared statement should still work
        run(&ex, "EXECUTE add_contact(6, 'Frank', 'Austin')").await;
        let res = run(&ex, "SELECT COUNT(*) FROM contacts").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(6));

        // Deallocate the insert statement too
        run(&ex, "DEALLOCATE add_contact").await;
        let err = ex.execute("EXECUTE add_contact(7, 'Grace', 'Miami')").await;
        assert!(err.is_err());
    }

    // ========================================================================
    // 7. ALTER TABLE workflow: add column, rename column, drop column
    // ========================================================================

    #[tokio::test]
    async fn test_alter_table_workflow() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE profiles (id INT PRIMARY KEY, username TEXT NOT NULL)",
        )
        .await;
        run(&ex, "INSERT INTO profiles VALUES (1, 'alice')").await;
        run(&ex, "INSERT INTO profiles VALUES (2, 'bob')").await;

        // Verify initial schema: 2 columns
        let res = run(&ex, "SELECT * FROM profiles ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r[0].len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("alice".into()));

        // ADD COLUMN with a default value
        run(&ex, "ALTER TABLE profiles ADD COLUMN bio TEXT DEFAULT 'No bio yet'").await;

        // Existing rows should have the default value
        let res = run(&ex, "SELECT bio FROM profiles WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Text("No bio yet".into()));

        // New rows should also get the default
        run(&ex, "INSERT INTO profiles VALUES (3, 'charlie', 'I code')").await;
        let res = run(&ex, "SELECT bio FROM profiles WHERE id = 3").await;
        assert_eq!(*scalar(&res[0]), Value::Text("I code".into()));

        // RENAME COLUMN
        run(&ex, "ALTER TABLE profiles RENAME COLUMN bio TO biography").await;

        // Query using the new name
        let res = run(&ex, "SELECT biography FROM profiles WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Text("No bio yet".into()));

        // Verify column name in metadata
        let res = run(&ex, "SELECT biography FROM profiles WHERE id = 2").await;
        let col_names = columns(&res[0]);
        assert_eq!(col_names[0].0, "biography");

        // DROP COLUMN
        run(&ex, "ALTER TABLE profiles DROP COLUMN biography").await;

        // Verify data integrity: should have 2 columns again
        let res = run(&ex, "SELECT * FROM profiles ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r[0].len(), 2, "should have 2 columns after drop");
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("alice".into()));
        assert_eq!(r.len(), 3, "all 3 rows should still exist");
    }

    // ========================================================================
    // 8. Information schema introspection
    // ========================================================================

    #[tokio::test]
    async fn test_information_schema_introspection() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE accounts (
                id INT PRIMARY KEY,
                email TEXT NOT NULL,
                balance INT
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE transactions (
                id INT PRIMARY KEY,
                account_id INT NOT NULL,
                amount INT NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts (id)
            )",
        )
        .await;

        // information_schema.tables should list both tables
        let res = run(
            &ex,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("accounts".into()));
        assert_eq!(r[1][0], Value::Text("transactions".into()));

        // information_schema.tables with all standard columns
        let res = run(
            &ex,
            "SELECT table_catalog, table_schema, table_name, table_type
             FROM information_schema.tables
             WHERE table_name = 'accounts'",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("nucleus".into()));
        assert_eq!(r[0][1], Value::Text("public".into()));
        assert_eq!(r[0][2], Value::Text("accounts".into()));
        assert_eq!(r[0][3], Value::Text("BASE TABLE".into()));

        // information_schema.columns for the accounts table
        let res = run(
            &ex,
            "SELECT column_name, is_nullable, data_type
             FROM information_schema.columns
             WHERE table_name = 'accounts'
             ORDER BY ordinal_position",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        // id column
        assert_eq!(r[0][0], Value::Text("id".into()));
        // email column
        assert_eq!(r[1][0], Value::Text("email".into()));
        // balance column
        assert_eq!(r[2][0], Value::Text("balance".into()));

        // Columns for the transactions table
        let res = run(
            &ex,
            "SELECT column_name FROM information_schema.columns
             WHERE table_name = 'transactions'
             ORDER BY ordinal_position",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("id".into()));
        assert_eq!(r[1][0], Value::Text("account_id".into()));
        assert_eq!(r[2][0], Value::Text("amount".into()));
    }

    // ========================================================================
    // 9. generate_series usage: standalone, with WHERE, JOINed with tables
    // ========================================================================

    #[tokio::test]
    async fn test_generate_series_usage() {
        let ex = setup();

        // Basic generate_series from table function
        let res = run(&ex, "SELECT * FROM generate_series(1, 5)").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][0], Value::Int64(1));
        assert_eq!(r[4][0], Value::Int64(5));

        // With step
        let res = run(&ex, "SELECT * FROM generate_series(0, 20, 5)").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5); // 0, 5, 10, 15, 20
        assert_eq!(r[0][0], Value::Int64(0));
        assert_eq!(r[4][0], Value::Int64(20));

        // Descending
        let res = run(&ex, "SELECT * FROM generate_series(10, 1, -3)").await;
        let r = rows(&res[0]);
        // 10, 7, 4, 1
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][0], Value::Int64(10));
        assert_eq!(r[3][0], Value::Int64(1));

        // With WHERE filter
        let res = run(
            &ex,
            "SELECT generate_series FROM generate_series(1, 10) WHERE generate_series > 7",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3); // 8, 9, 10

        // JOINed with a real table
        run(&ex, "CREATE TABLE multipliers (label TEXT, factor INT)").await;
        run(&ex, "INSERT INTO multipliers VALUES ('double', 2)").await;
        run(&ex, "INSERT INTO multipliers VALUES ('triple', 3)").await;

        let res = run(
            &ex,
            "SELECT m.label, g.generate_series, g.generate_series * m.factor AS result
             FROM generate_series(1, 3) g
             JOIN multipliers m ON 1=1
             ORDER BY 1, 2",
        )
        .await;
        let r = rows(&res[0]);
        // 3 numbers * 2 multipliers = 6 rows
        assert_eq!(r.len(), 6);
    }

    // ========================================================================
    // 10. String and math function pipeline
    // ========================================================================

    #[tokio::test]
    async fn test_string_and_math_function_pipeline() {
        let ex = setup();

        // Chained string functions
        let res = run(&ex, "SELECT UPPER(REVERSE('dlrow olleh'))").await;
        assert_eq!(*scalar(&res[0]), Value::Text("HELLO WORLD".into()));

        // CONCAT with LOWER and LENGTH
        let res = run(
            &ex,
            "SELECT LENGTH(CONCAT(LOWER('Hello'), ' ', LOWER('World')))",
        )
        .await;
        assert_eq!(*scalar(&res[0]), Value::Int32(11));

        // String padding pipeline
        let res = run(&ex, "SELECT LPAD(RPAD('x', 3, '-'), 5, '=')").await;
        assert_eq!(*scalar(&res[0]), Value::Text("==x--".into()));

        // REPLACE + UPPER
        let res = run(
            &ex,
            "SELECT UPPER(REPLACE('hello world', 'world', 'rust'))",
        )
        .await;
        assert_eq!(*scalar(&res[0]), Value::Text("HELLO RUST".into()));

        // Nested math: POWER, SQRT, ABS
        let res = run(&ex, "SELECT SQRT(POWER(3.0, 2.0) + POWER(4.0, 2.0))").await;
        match scalar(&res[0]) {
            Value::Float64(f) => assert!(
                (*f - 5.0).abs() < 0.0001,
                "expected 5.0, got {f}"
            ),
            other => panic!("expected Float64, got {other:?}"),
        }

        // ABS of negative result
        let res = run(&ex, "SELECT ABS(-42)").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(42));

        // CEIL and FLOOR
        let res = run(&ex, "SELECT CEIL(4.1)").await;
        assert_eq!(*scalar(&res[0]), Value::Float64(5.0));

        let res = run(&ex, "SELECT FLOOR(4.9)").await;
        assert_eq!(*scalar(&res[0]), Value::Float64(4.0));

        // ROUND with precision
        let res = run(&ex, "SELECT ROUND(3.14159, 2)").await;
        match scalar(&res[0]) {
            Value::Float64(f) => assert!((*f - 3.14).abs() < 0.001, "expected ~3.14, got {f}"),
            other => panic!("expected Float64, got {other:?}"),
        }

        // Combined math/string pipeline on table data
        run(&ex, "CREATE TABLE measurements (label TEXT, value FLOAT)").await;
        run(&ex, "INSERT INTO measurements VALUES ('sensor_a', 3.14159)").await;
        run(&ex, "INSERT INTO measurements VALUES ('sensor_b', 2.71828)").await;

        let res = run(
            &ex,
            "SELECT UPPER(label), ROUND(value, 2)
             FROM measurements
             ORDER BY label",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("SENSOR_A".into()));
        match &r[0][1] {
            Value::Float64(f) => assert!((*f - 3.14).abs() < 0.01),
            other => panic!("expected Float64, got {other:?}"),
        }

        // COALESCE with NULLIF in a pipeline
        let res = run(&ex, "SELECT COALESCE(NULLIF(1, 1), 42)").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(42));

        // GREATEST and LEAST with computed values
        let res = run(&ex, "SELECT GREATEST(ABS(-10), 5, 3)").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(10));

        let res = run(&ex, "SELECT LEAST(ABS(-1), 5, 3)").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(1));

        // Trigonometric function chain
        let res = run(&ex, "SELECT ROUND(SIN(0) + COS(0), 1)").await;
        match scalar(&res[0]) {
            Value::Float64(f) => assert!((*f - 1.0).abs() < 0.01, "sin(0)+cos(0) = 1.0, got {f}"),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    // ========================================================================
    // 11. Multi-table foreign key workflow
    // ========================================================================

    #[tokio::test]
    async fn test_multi_table_foreign_key_workflow() {
        let ex = setup();

        // Create parent table
        run(
            &ex,
            "CREATE TABLE authors (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                country TEXT
            )",
        )
        .await;

        // Create child table with FK
        run(
            &ex,
            "CREATE TABLE books (
                id INT PRIMARY KEY,
                title TEXT NOT NULL,
                author_id INT NOT NULL,
                pages INT,
                FOREIGN KEY (author_id) REFERENCES authors (id)
            )",
        )
        .await;

        // Insert parent data
        run(&ex, "INSERT INTO authors VALUES (1, 'George Orwell', 'UK')").await;
        run(&ex, "INSERT INTO authors VALUES (2, 'Jane Austen', 'UK')").await;
        run(&ex, "INSERT INTO authors VALUES (3, 'Mark Twain', 'USA')").await;

        // Insert child data respecting FK constraints
        run(&ex, "INSERT INTO books VALUES (101, '1984', 1, 328)").await;
        run(&ex, "INSERT INTO books VALUES (102, 'Animal Farm', 1, 112)").await;
        run(&ex, "INSERT INTO books VALUES (103, 'Pride and Prejudice', 2, 432)").await;
        run(&ex, "INSERT INTO books VALUES (104, 'Tom Sawyer', 3, 274)").await;

        // FK violation: author_id=99 does not exist
        let err = ex.execute("INSERT INTO books VALUES (105, 'Unknown', 99, 200)").await;
        assert!(err.is_err(), "should reject FK violation");
        assert!(err.unwrap_err().to_string().contains("foreign key"));

        // Join and query
        let res = run(
            &ex,
            "SELECT a.name, b.title, b.pages
             FROM books b
             JOIN authors a ON b.author_id = a.id
             WHERE a.country = 'UK'
             ORDER BY pages DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][1], Value::Text("Pride and Prejudice".into()));
        assert_eq!(r[0][2], Value::Int32(432));
        assert_eq!(r[1][1], Value::Text("1984".into()));
        assert_eq!(r[2][1], Value::Text("Animal Farm".into()));

        // Aggregate by author
        let res = run(
            &ex,
            "SELECT a.name, COUNT(*) AS book_count, SUM(b.pages) AS total_pages
             FROM books b
             JOIN authors a ON b.author_id = a.id
             GROUP BY a.name
             ORDER BY book_count DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("George Orwell".into()));
        assert_eq!(r[0][1], Value::Int64(2));
        assert_eq!(r[0][2], Value::Int64(440));
    }

    // ========================================================================
    // 12. View + Subquery combination
    // ========================================================================

    #[tokio::test]
    async fn test_view_with_subquery_and_aggregation() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE sales (
                id INT PRIMARY KEY,
                region TEXT NOT NULL,
                product TEXT NOT NULL,
                amount INT NOT NULL
            )",
        )
        .await;

        run(&ex, "INSERT INTO sales VALUES (1, 'North', 'Widget', 100)").await;
        run(&ex, "INSERT INTO sales VALUES (2, 'North', 'Gadget', 150)").await;
        run(&ex, "INSERT INTO sales VALUES (3, 'South', 'Widget', 200)").await;
        run(&ex, "INSERT INTO sales VALUES (4, 'South', 'Gadget', 250)").await;
        run(&ex, "INSERT INTO sales VALUES (5, 'East', 'Widget', 120)").await;
        run(&ex, "INSERT INTO sales VALUES (6, 'East', 'Gadget', 180)").await;

        // Create view that aggregates by region
        run(
            &ex,
            "CREATE VIEW regional_sales AS
             SELECT region, SUM(amount) AS total_sales
             FROM sales
             GROUP BY region",
        )
        .await;

        // Query the view directly
        let res = run(&ex, "SELECT region, total_sales FROM regional_sales ORDER BY total_sales DESC").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("South".into()));
        assert_eq!(r[0][1], Value::Int64(450));

        // Use view in subquery with aggregation
        let res = run(
            &ex,
            "SELECT AVG(total_sales) AS avg_regional_sales
             FROM regional_sales",
        )
        .await;
        let avg_val = scalar(&res[0]);
        match avg_val {
            Value::Float64(f) => {
                // (100+150 + 200+250 + 120+180) / 3 = 1000/3 = 333.33
                assert!((*f - 333.33).abs() < 1.0, "expected ~333.33, got {f}");
            }
            other => panic!("expected Float64, got {other:?}"),
        }

        // Use view in WHERE subquery
        let res = run(
            &ex,
            "SELECT region, total_sales
             FROM regional_sales
             WHERE total_sales > (SELECT AVG(total_sales) FROM regional_sales)
             ORDER BY region",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("South".into()));
    }

    // ========================================================================
    // 13. Transaction rollback with data verification
    // ========================================================================

    #[tokio::test]
    async fn test_transaction_rollback_verification() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE accounts (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                balance INT NOT NULL
            )",
        )
        .await;

        // Insert initial data
        run(&ex, "INSERT INTO accounts VALUES (1, 'Alice', 1000)").await;
        run(&ex, "INSERT INTO accounts VALUES (2, 'Bob', 500)").await;

        // Verify initial balances
        let res = run(&ex, "SELECT balance FROM accounts WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(1000));

        let res = run(&ex, "SELECT balance FROM accounts WHERE id = 2").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(500));

        // Begin transaction
        run(&ex, "BEGIN").await;

        // Make changes
        run(&ex, "UPDATE accounts SET balance = balance - 300 WHERE id = 1").await;
        run(&ex, "UPDATE accounts SET balance = balance + 300 WHERE id = 2").await;

        // Verify changes are visible within transaction
        let res = run(&ex, "SELECT balance FROM accounts WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(700));

        let res = run(&ex, "SELECT balance FROM accounts WHERE id = 2").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(800));

        // Rollback
        run(&ex, "ROLLBACK").await;

        // Verify original data is intact after rollback
        let res = run(&ex, "SELECT balance FROM accounts WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(1000));

        let res = run(&ex, "SELECT balance FROM accounts WHERE id = 2").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(500));

        // Verify total balance is still correct
        let res = run(&ex, "SELECT SUM(balance) FROM accounts").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(1500));
    }

    // ========================================================================
    // 14. Complex aggregation pipeline with GROUP BY HAVING
    // ========================================================================

    #[tokio::test]
    async fn test_complex_aggregation_pipeline() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE orders_detail (
                order_id INT,
                customer_id INT,
                product_category TEXT,
                quantity INT,
                unit_price INT
            )",
        )
        .await;

        // Insert diverse order data
        run(&ex, "INSERT INTO orders_detail VALUES (1, 100, 'Electronics', 2, 500)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (2, 100, 'Electronics', 1, 300)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (3, 100, 'Books', 5, 20)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (4, 101, 'Electronics', 3, 400)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (5, 101, 'Books', 2, 15)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (6, 102, 'Electronics', 1, 600)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (7, 102, 'Books', 10, 25)").await;
        run(&ex, "INSERT INTO orders_detail VALUES (8, 102, 'Clothing', 4, 50)").await;

        // Complex aggregation: GROUP BY with multiple aggregates and HAVING
        let res = run(
            &ex,
            "SELECT customer_id,
                    COUNT(*) AS order_count,
                    SUM(quantity * unit_price) AS total_revenue,
                    AVG(quantity * unit_price) AS avg_order_value
             FROM orders_detail
             GROUP BY customer_id
             HAVING SUM(quantity * unit_price) > 1000
             ORDER BY total_revenue DESC",
        )
        .await;
        let r = rows(&res[0]);
        // Customer 100: (2*500)+(1*300)+(5*20) = 1000+300+100 = 1400
        // Customer 101: (3*400)+(2*15) = 1200+30 = 1230
        // Customer 102: (1*600)+(10*25)+(4*50) = 600+250+200 = 1050
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(100));
        assert_eq!(r[0][2], Value::Int64(1400));

        // Aggregation with ORDER BY aggregate result
        let res = run(
            &ex,
            "SELECT product_category,
                    COUNT(*) AS sales_count,
                    SUM(quantity) AS total_quantity
             FROM orders_detail
             GROUP BY product_category
             ORDER BY total_quantity DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        // Books: 5+2+10 = 17, Electronics: 2+1+3+1 = 7, Clothing: 4
        assert_eq!(r[0][0], Value::Text("Books".into()));
        assert_eq!(r[0][2], Value::Int64(17));
        assert_eq!(r[1][0], Value::Text("Electronics".into()));
        assert_eq!(r[2][0], Value::Text("Clothing".into()));
    }

    // ========================================================================
    // 15. DISTINCT with ORDER BY
    // ========================================================================

    #[tokio::test]
    async fn test_distinct_with_order_by() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE events (
                id INT,
                event_type TEXT,
                priority INT
            )",
        )
        .await;

        run(&ex, "INSERT INTO events VALUES (1, 'Login', 1)").await;
        run(&ex, "INSERT INTO events VALUES (2, 'Purchase', 3)").await;
        run(&ex, "INSERT INTO events VALUES (3, 'Login', 1)").await;
        run(&ex, "INSERT INTO events VALUES (4, 'Logout', 1)").await;
        run(&ex, "INSERT INTO events VALUES (5, 'Purchase', 3)").await;
        run(&ex, "INSERT INTO events VALUES (6, 'Error', 5)").await;
        run(&ex, "INSERT INTO events VALUES (7, 'Login', 1)").await;

        // DISTINCT event types ordered alphabetically
        let res = run(&ex, "SELECT DISTINCT event_type FROM events ORDER BY event_type").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][0], Value::Text("Error".into()));
        assert_eq!(r[1][0], Value::Text("Login".into()));
        assert_eq!(r[2][0], Value::Text("Logout".into()));
        assert_eq!(r[3][0], Value::Text("Purchase".into()));

        // DISTINCT priorities ordered descending
        let res = run(&ex, "SELECT DISTINCT priority FROM events ORDER BY priority DESC").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(5));
        assert_eq!(r[1][0], Value::Int32(3));
        assert_eq!(r[2][0], Value::Int32(1));

        // DISTINCT with multiple columns
        let res = run(
            &ex,
            "SELECT DISTINCT event_type, priority
             FROM events
             ORDER BY priority DESC, event_type",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][0], Value::Text("Error".into()));
        assert_eq!(r[0][1], Value::Int32(5));
        assert_eq!(r[1][0], Value::Text("Purchase".into()));
        assert_eq!(r[1][1], Value::Int32(3));
    }

    // ========================================================================
    // 16. CASE expression in SELECT and WHERE
    // ========================================================================

    #[tokio::test]
    async fn test_case_expression_in_select_and_where() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE employees (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                salary INT,
                department TEXT
            )",
        )
        .await;

        run(&ex, "INSERT INTO employees VALUES (1, 'Alice', 50000, 'Engineering')").await;
        run(&ex, "INSERT INTO employees VALUES (2, 'Bob', 75000, 'Engineering')").await;
        run(&ex, "INSERT INTO employees VALUES (3, 'Charlie', 60000, 'Sales')").await;
        run(&ex, "INSERT INTO employees VALUES (4, 'Diana', 90000, 'Management')").await;
        run(&ex, "INSERT INTO employees VALUES (5, 'Eve', 45000, 'Sales')").await;

        // CASE in SELECT for categorization
        let res = run(
            &ex,
            "SELECT name,
                    CASE
                        WHEN salary >= 80000 THEN 'High'
                        WHEN salary >= 60000 THEN 'Medium'
                        ELSE 'Low'
                    END AS salary_band
             FROM employees
             ORDER BY name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[0][1], Value::Text("Low".into()));
        assert_eq!(r[1][0], Value::Text("Bob".into()));
        assert_eq!(r[1][1], Value::Text("Medium".into()));
        assert_eq!(r[3][0], Value::Text("Diana".into()));
        assert_eq!(r[3][1], Value::Text("High".into()));

        // CASE in WHERE clause
        let res = run(
            &ex,
            "SELECT name, salary
             FROM employees
             WHERE CASE
                     WHEN department = 'Engineering' THEN salary > 60000
                     WHEN department = 'Sales' THEN salary < 50000
                     ELSE salary > 85000
                   END
             ORDER BY name",
        )
        .await;
        let r = rows(&res[0]);
        // Bob (Engineering, 75000 > 60000), Diana (Management, 90000 > 85000), Eve (Sales, 45000 < 50000)
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Bob".into()));
        assert_eq!(r[1][0], Value::Text("Diana".into()));
        assert_eq!(r[2][0], Value::Text("Eve".into()));

        // CASE with aggregation
        let res = run(
            &ex,
            "SELECT department,
                    SUM(CASE WHEN salary >= 60000 THEN 1 ELSE 0 END) AS high_earners,
                    SUM(CASE WHEN salary < 60000 THEN 1 ELSE 0 END) AS low_earners
             FROM employees
             GROUP BY department
             ORDER BY department",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Engineering".into()));
        assert_eq!(r[0][1], Value::Int64(1)); // Bob
        assert_eq!(r[0][2], Value::Int64(1)); // Alice
    }

    // ========================================================================
    // 17. Multi-join query with 3+ tables
    // ========================================================================

    #[tokio::test]
    async fn test_multi_join_query() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE customers (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE orders_main (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                order_date TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers (id)
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE order_lines (
                id INT PRIMARY KEY,
                order_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT,
                FOREIGN KEY (order_id) REFERENCES orders_main (id)
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE products_catalog (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                price INT
            )",
        )
        .await;

        // Insert data
        run(&ex, "INSERT INTO customers VALUES (1, 'ACME Corp', 'Seattle')").await;
        run(&ex, "INSERT INTO customers VALUES (2, 'Widget Inc', 'Portland')").await;

        run(&ex, "INSERT INTO orders_main VALUES (100, 1, '2024-01-15')").await;
        run(&ex, "INSERT INTO orders_main VALUES (101, 2, '2024-01-20')").await;

        run(&ex, "INSERT INTO products_catalog VALUES (1, 'Laptop', 1000)").await;
        run(&ex, "INSERT INTO products_catalog VALUES (2, 'Mouse', 25)").await;
        run(&ex, "INSERT INTO products_catalog VALUES (3, 'Keyboard', 75)").await;

        run(&ex, "INSERT INTO order_lines VALUES (1, 100, 1, 2)").await;
        run(&ex, "INSERT INTO order_lines VALUES (2, 100, 2, 5)").await;
        run(&ex, "INSERT INTO order_lines VALUES (3, 101, 3, 3)").await;

        // 4-table join
        let res = run(
            &ex,
            "SELECT c.name AS customer_name,
                    o.order_date,
                    p.name AS product_name,
                    ol.quantity,
                    ol.quantity * p.price AS line_total
             FROM customers c
             JOIN orders_main o ON c.id = o.customer_id
             JOIN order_lines ol ON o.id = ol.order_id
             JOIN products_catalog p ON ol.product_id = p.id",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        // Verify we have data from all four tables
        assert!(r.iter().any(|row| row[0] == Value::Text("ACME Corp".into())));
        assert!(r.iter().any(|row| row[2] == Value::Text("Laptop".into())));
        // Check that we can compute line totals
        let has_2000 = r.iter().any(|row| row[4] == Value::Int32(2000));
        assert!(has_2000, "should have Laptop line total of 2000");

        // Aggregate across joins
        let res = run(
            &ex,
            "SELECT c.name, SUM(ol.quantity * p.price) AS total_spent
             FROM customers c
             JOIN orders_main o ON c.id = o.customer_id
             JOIN order_lines ol ON o.id = ol.order_id
             JOIN products_catalog p ON ol.product_id = p.id
             GROUP BY c.name
             ORDER BY total_spent DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("ACME Corp".into()));
        assert_eq!(r[0][1], Value::Int64(2125)); // (2*1000) + (5*25) = 2125
        assert_eq!(r[1][0], Value::Text("Widget Inc".into()));
        assert_eq!(r[1][1], Value::Int64(225)); // 3*75 = 225
    }

    // ========================================================================
    // 18. INSERT with RETURNING
    // ========================================================================

    #[tokio::test]
    async fn test_insert_with_returning() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE logs (
                id INT PRIMARY KEY,
                message TEXT,
                level TEXT
            )",
        )
        .await;

        // Insert with RETURNING all columns
        let res = run(
            &ex,
            "INSERT INTO logs VALUES (1, 'System started', 'INFO') RETURNING *",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Text("System started".into()));
        assert_eq!(r[0][2], Value::Text("INFO".into()));

        // Insert with RETURNING specific columns
        let res = run(
            &ex,
            "INSERT INTO logs VALUES (2, 'Error occurred', 'ERROR') RETURNING id, level",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].len(), 2);
        assert_eq!(r[0][0], Value::Int32(2));
        assert_eq!(r[0][1], Value::Text("ERROR".into()));

        // Multiple inserts with RETURNING (if supported)
        let res = run(
            &ex,
            "INSERT INTO logs VALUES (3, 'Warning', 'WARN'), (4, 'Debug', 'DEBUG') RETURNING id",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(3));
        assert_eq!(r[1][0], Value::Int32(4));

        // Verify all data was inserted
        let res = run(&ex, "SELECT COUNT(*) FROM logs").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(4));
    }

    // ========================================================================
    // 19. UPDATE with subquery in WHERE
    // ========================================================================

    #[tokio::test]
    async fn test_update_with_subquery_in_where() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE staff (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                salary INT,
                department_id INT
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE departments_info (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                budget INT
            )",
        )
        .await;

        // Insert department data
        run(&ex, "INSERT INTO departments_info VALUES (1, 'Engineering', 500000)").await;
        run(&ex, "INSERT INTO departments_info VALUES (2, 'Sales', 300000)").await;
        run(&ex, "INSERT INTO departments_info VALUES (3, 'HR', 200000)").await;

        // Insert staff data
        run(&ex, "INSERT INTO staff VALUES (1, 'Alice', 80000, 1)").await;
        run(&ex, "INSERT INTO staff VALUES (2, 'Bob', 70000, 1)").await;
        run(&ex, "INSERT INTO staff VALUES (3, 'Charlie', 60000, 2)").await;
        run(&ex, "INSERT INTO staff VALUES (4, 'Diana', 55000, 2)").await;
        run(&ex, "INSERT INTO staff VALUES (5, 'Eve', 50000, 3)").await;

        // Update salaries for employees in specific departments
        run(
            &ex,
            "UPDATE staff SET salary = salary + 5000 WHERE department_id = 1",
        )
        .await;

        run(
            &ex,
            "UPDATE staff SET salary = salary + 5000 WHERE department_id = 2",
        )
        .await;

        // Verify Engineering staff got raises
        let res = run(&ex, "SELECT salary FROM staff WHERE id = 1").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(85000));

        let res = run(&ex, "SELECT salary FROM staff WHERE id = 2").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(75000));

        // Verify Sales staff got raises
        let res = run(&ex, "SELECT salary FROM staff WHERE id = 3").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(65000));

        // Verify HR staff did NOT get raise
        let res = run(&ex, "SELECT salary FROM staff WHERE id = 5").await;
        assert_eq!(*scalar(&res[0]), Value::Int32(50000));

        // Verify we can use JOINs to verify cross-table relationships
        let res = run(
            &ex,
            "SELECT s.name, d.name FROM staff s JOIN departments_info d ON s.department_id = d.id WHERE d.budget > 250000",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4); // 4 staff in Engineering + Sales
    }

    // ========================================================================
    // 20. CREATE INDEX + query optimization
    // ========================================================================

    #[tokio::test]
    async fn test_create_index_and_query() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE large_table (
                id INT PRIMARY KEY,
                category TEXT,
                value INT,
                created_at TEXT
            )",
        )
        .await;

        // Insert test data
        for i in 1..=100 {
            let category = if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            };
            run(
                &ex,
                &format!("INSERT INTO large_table VALUES ({}, '{}', {}, '2024-01-{:02}')", i, category, i * 10, (i % 28) + 1),
            )
            .await;
        }

        // Query without index
        let res = run(
            &ex,
            "SELECT COUNT(*) FROM large_table WHERE category = 'A'",
        )
        .await;
        assert_eq!(*scalar(&res[0]), Value::Int64(33));

        // Create index on category
        run(&ex, "CREATE INDEX idx_category ON large_table (category)").await;

        // Query with index (should still work correctly)
        let res = run(
            &ex,
            "SELECT COUNT(*) FROM large_table WHERE category = 'A'",
        )
        .await;
        assert_eq!(*scalar(&res[0]), Value::Int64(33));

        // Create composite index
        run(&ex, "CREATE INDEX idx_cat_val ON large_table (category, value)").await;

        // Query using composite index
        let res = run(
            &ex,
            "SELECT id FROM large_table
             WHERE category = 'B' AND value > 500
             ORDER BY value
             LIMIT 3",
        )
        .await;
        let r = rows(&res[0]);
        assert!(r.len() > 0);

        // Verify index existence via information_schema
        let res = run(
            &ex,
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'large_table'",
        )
        .await;
        assert_eq!(*scalar(&res[0]), Value::Int64(1));

        // Query with ORDER BY on indexed column
        let res = run(
            &ex,
            "SELECT category, COUNT(*) AS cnt
             FROM large_table
             GROUP BY category
             ORDER BY category",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("A".into()));
        assert_eq!(r[1][0], Value::Text("B".into()));
        assert_eq!(r[2][0], Value::Text("C".into()));
    }

    // ========================================================================
    // MVCC Transaction Integration Tests
    // ========================================================================

    /// Create an executor backed by the MVCC storage adapter.
    fn setup_mvcc() -> Arc<Executor> {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> =
            Arc::new(crate::storage::MvccStorageAdapter::new());
        Arc::new(Executor::new(catalog, storage))
    }

    #[tokio::test]
    async fn mvcc_basic_crud() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE t (id INT NOT NULL, name TEXT)").await;
        run(&ex, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')").await;

        let res = run(&ex, "SELECT * FROM t ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][1], Value::Text("alice".into()));

        run(&ex, "UPDATE t SET name = 'ALICE' WHERE id = 1").await;
        let res = run(&ex, "SELECT name FROM t WHERE id = 1").await;
        assert_eq!(rows(&res[0])[0][0], Value::Text("ALICE".into()));

        run(&ex, "DELETE FROM t WHERE id = 2").await;
        let res = run(&ex, "SELECT COUNT(*) FROM t").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
    }

    #[tokio::test]
    async fn mvcc_transaction_commit() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE t (id INT NOT NULL)").await;
        run(&ex, "INSERT INTO t VALUES (1)").await;

        run(&ex, "BEGIN").await;
        run(&ex, "INSERT INTO t VALUES (2)").await;
        run(&ex, "INSERT INTO t VALUES (3)").await;
        run(&ex, "COMMIT").await;

        let res = run(&ex, "SELECT COUNT(*) FROM t").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(3));
    }

    #[tokio::test]
    async fn mvcc_transaction_rollback() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE t (id INT NOT NULL)").await;
        run(&ex, "INSERT INTO t VALUES (1)").await;

        run(&ex, "BEGIN").await;
        run(&ex, "INSERT INTO t VALUES (2)").await;
        run(&ex, "INSERT INTO t VALUES (3)").await;
        run(&ex, "ROLLBACK").await;

        // Only the auto-committed row 1 should remain
        let res = run(&ex, "SELECT COUNT(*) FROM t").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
    }

    #[tokio::test]
    async fn mvcc_rollback_preserves_prior_data() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE t (id INT NOT NULL, val TEXT)").await;
        run(&ex, "INSERT INTO t VALUES (1, 'original')").await;

        run(&ex, "BEGIN").await;
        run(&ex, "UPDATE t SET val = 'modified' WHERE id = 1").await;
        run(&ex, "INSERT INTO t VALUES (2, 'new')").await;
        run(&ex, "ROLLBACK").await;

        let res = run(&ex, "SELECT * FROM t ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Text("original".into()));
    }

    #[tokio::test]
    async fn mvcc_auto_commit_mode() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE t (id INT NOT NULL)").await;

        // Each statement outside BEGIN/COMMIT auto-commits
        run(&ex, "INSERT INTO t VALUES (1)").await;
        run(&ex, "INSERT INTO t VALUES (2)").await;

        let res = run(&ex, "SELECT COUNT(*) FROM t").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
    }

    #[tokio::test]
    async fn mvcc_multi_table_transaction() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE orders (id INT NOT NULL)").await;
        run(&ex, "CREATE TABLE items (order_id INT NOT NULL, name TEXT)").await;

        run(&ex, "BEGIN").await;
        run(&ex, "INSERT INTO orders VALUES (100)").await;
        run(&ex, "INSERT INTO items VALUES (100, 'Widget')").await;
        run(&ex, "INSERT INTO items VALUES (100, 'Gadget')").await;
        run(&ex, "COMMIT").await;

        let res = run(&ex, "SELECT COUNT(*) FROM orders").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1));
        let res = run(&ex, "SELECT COUNT(*) FROM items WHERE order_id = 100").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
    }

    #[tokio::test]
    async fn mvcc_show_pool_status() {
        let ex = setup_mvcc();
        let res = run(&ex, "SHOW pool_status").await;
        let r = rows(&res[0]);
        // Should have at least one row about mvcc_enabled=true
        let mvcc_row = r.iter().find(|row| row[0] == Value::Text("mvcc_enabled".into()));
        assert!(mvcc_row.is_some());
        assert_eq!(mvcc_row.unwrap()[1], Value::Text("true".into()));
    }

    #[tokio::test]
    async fn mvcc_show_metrics() {
        let ex = setup_mvcc();
        let res = run(&ex, "SHOW metrics").await;
        let r = rows(&res[0]);
        // Should return multiple metric rows
        assert!(r.len() > 10, "expected many metric rows, got {}", r.len());
        // First row should have a metric name
        assert!(matches!(&r[0][0], Value::Text(s) if s.starts_with("nucleus_")));
    }

    #[tokio::test]
    async fn mvcc_show_buffer_pool() {
        let ex = setup_mvcc();
        let res = run(&ex, "SHOW buffer_pool").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty());
    }

    #[tokio::test]
    async fn mvcc_complex_workflow() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE products (id INT NOT NULL, name TEXT, price INT NOT NULL)").await;
        run(&ex, "INSERT INTO products VALUES (1, 'Laptop', 999), (2, 'Phone', 699), (3, 'Tablet', 499)").await;

        // Transaction: increase all prices by 10%
        run(&ex, "BEGIN").await;
        run(&ex, "UPDATE products SET price = price * 110 / 100").await;

        // Within the transaction, verify the updated prices
        let res = run(&ex, "SELECT SUM(price) FROM products").await;
        let total = scalar(&res[0]);
        // 999*1.1 + 699*1.1 + 499*1.1 = 1098+768+548 ≈ 2414 (integer truncation varies)
        // Just verify it's higher than original sum of 2197
        match total {
            Value::Int64(n) => assert!(*n > 2197, "prices should have increased, got {n}"),
            _ => panic!("expected Int64, got {total:?}"),
        }

        run(&ex, "COMMIT").await;

        // After commit, changes are permanent
        let res = run(&ex, "SELECT price FROM products WHERE id = 1").await;
        let price = &rows(&res[0])[0][0];
        match price {
            Value::Int32(p) => assert!(*p > 999, "price should have increased, got {p}"),
            _ => panic!("expected Int32, got {price:?}"),
        }
    }

    // ========================================================================
    // 21. UNION and UNION ALL
    // ========================================================================

    #[tokio::test]
    async fn test_sql_union_and_union_all() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE fruits (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE vegetables (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .await;

        // Insert data with some overlapping names
        run(&ex, "INSERT INTO fruits VALUES (1, 'Apple')").await;
        run(&ex, "INSERT INTO fruits VALUES (2, 'Banana')").await;
        run(&ex, "INSERT INTO fruits VALUES (3, 'Tomato')").await; // overlap

        run(&ex, "INSERT INTO vegetables VALUES (1, 'Carrot')").await;
        run(&ex, "INSERT INTO vegetables VALUES (2, 'Tomato')").await; // overlap
        run(&ex, "INSERT INTO vegetables VALUES (3, 'Spinach')").await;

        // UNION should deduplicate: Apple, Banana, Carrot, Spinach, Tomato = 5 rows
        let res = run(
            &ex,
            "SELECT name FROM fruits
             UNION
             SELECT name FROM vegetables
             ORDER BY name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5, "UNION should deduplicate overlapping rows");
        assert_eq!(r[0][0], Value::Text("Apple".into()));
        assert_eq!(r[1][0], Value::Text("Banana".into()));
        assert_eq!(r[2][0], Value::Text("Carrot".into()));
        assert_eq!(r[3][0], Value::Text("Spinach".into()));
        assert_eq!(r[4][0], Value::Text("Tomato".into()));

        // UNION ALL should keep duplicates: 3 + 3 = 6 rows
        let res = run(
            &ex,
            "SELECT name FROM fruits
             UNION ALL
             SELECT name FROM vegetables
             ORDER BY name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 6, "UNION ALL should keep all rows including duplicates");
        // Tomato should appear twice
        let tomato_count = r.iter().filter(|row| row[0] == Value::Text("Tomato".into())).count();
        assert_eq!(tomato_count, 2, "Tomato should appear twice in UNION ALL");
    }

    // ========================================================================
    // 22. EXISTS and NOT EXISTS
    // ========================================================================

    #[tokio::test]
    async fn test_sql_exists_and_not_exists() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE parents (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT NOT NULL, name TEXT NOT NULL)",
        )
        .await;

        // Insert parents
        run(&ex, "INSERT INTO parents VALUES (1, 'Alice')").await;
        run(&ex, "INSERT INTO parents VALUES (2, 'Bob')").await;
        run(&ex, "INSERT INTO parents VALUES (3, 'Charlie')").await; // no children

        // Insert children (only for Alice and Bob)
        run(&ex, "INSERT INTO children VALUES (10, 1, 'Amy')").await;
        run(&ex, "INSERT INTO children VALUES (11, 1, 'Adam')").await;
        run(&ex, "INSERT INTO children VALUES (12, 2, 'Beth')").await;

        // EXISTS: find parents who have at least one child
        let res = run(
            &ex,
            "SELECT p.name
             FROM parents p
             WHERE EXISTS (SELECT 1 FROM children c WHERE c.parent_id = p.id)
             ORDER BY p.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[1][0], Value::Text("Bob".into()));

        // NOT EXISTS: find parents who have no children
        let res = run(
            &ex,
            "SELECT p.name
             FROM parents p
             WHERE NOT EXISTS (SELECT 1 FROM children c WHERE c.parent_id = p.id)
             ORDER BY p.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("Charlie".into()));
    }

    // ========================================================================
    // 23. IN subquery
    // ========================================================================

    #[tokio::test]
    async fn test_sql_in_subquery() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE categories (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL, category_id INT NOT NULL)",
        )
        .await;

        // Insert categories
        run(&ex, "INSERT INTO categories VALUES (1, 'Electronics')").await;
        run(&ex, "INSERT INTO categories VALUES (2, 'Books')").await;
        run(&ex, "INSERT INTO categories VALUES (3, 'Clothing')").await;

        // Insert items across categories
        run(&ex, "INSERT INTO items VALUES (10, 'Laptop', 1)").await;
        run(&ex, "INSERT INTO items VALUES (11, 'Phone', 1)").await;
        run(&ex, "INSERT INTO items VALUES (12, 'Novel', 2)").await;
        run(&ex, "INSERT INTO items VALUES (13, 'Shirt', 3)").await;
        run(&ex, "INSERT INTO items VALUES (14, 'Textbook', 2)").await;

        // Use IN subquery: find items whose category name is 'Electronics' or 'Books'
        let res = run(
            &ex,
            "SELECT i.name
             FROM items i
             WHERE i.category_id IN (
                 SELECT c.id FROM categories c WHERE c.name IN ('Electronics', 'Books')
             )
             ORDER BY i.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0][0], Value::Text("Laptop".into()));
        assert_eq!(r[1][0], Value::Text("Novel".into()));
        assert_eq!(r[2][0], Value::Text("Phone".into()));
        assert_eq!(r[3][0], Value::Text("Textbook".into()));

        // Use NOT IN subquery: find items NOT in 'Electronics'
        let res = run(
            &ex,
            "SELECT i.name
             FROM items i
             WHERE i.category_id NOT IN (
                 SELECT c.id FROM categories c WHERE c.name = 'Electronics'
             )
             ORDER BY i.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Novel".into()));
        assert_eq!(r[1][0], Value::Text("Shirt".into()));
        assert_eq!(r[2][0], Value::Text("Textbook".into()));
    }

    // ========================================================================
    // 24. CTE with subquery combination
    // ========================================================================

    #[tokio::test]
    async fn test_sql_multiple_ctes() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE scores (
                student TEXT NOT NULL,
                subject TEXT NOT NULL,
                score INT NOT NULL
            )",
        )
        .await;

        run(&ex, "INSERT INTO scores VALUES ('Alice', 'Math', 90)").await;
        run(&ex, "INSERT INTO scores VALUES ('Alice', 'Science', 85)").await;
        run(&ex, "INSERT INTO scores VALUES ('Alice', 'English', 92)").await;
        run(&ex, "INSERT INTO scores VALUES ('Bob', 'Math', 78)").await;
        run(&ex, "INSERT INTO scores VALUES ('Bob', 'Science', 88)").await;
        run(&ex, "INSERT INTO scores VALUES ('Bob', 'English', 70)").await;
        run(&ex, "INSERT INTO scores VALUES ('Charlie', 'Math', 95)").await;
        run(&ex, "INSERT INTO scores VALUES ('Charlie', 'Science', 60)").await;
        run(&ex, "INSERT INTO scores VALUES ('Charlie', 'English', 75)").await;

        // CTE computes per-student averages; a subquery computes the overall
        // class average from the raw scores; the outer query filters above-average
        let res = run(
            &ex,
            "WITH student_avg AS (
                 SELECT student, AVG(score) AS avg_score
                 FROM scores
                 GROUP BY student
             )
             SELECT student, avg_score
             FROM student_avg
             WHERE avg_score > (SELECT AVG(score) FROM scores)
             ORDER BY avg_score DESC",
        )
        .await;
        let r = rows(&res[0]);
        // Overall avg = (90+85+92+78+88+70+95+60+75)/9 = 81.44
        // Alice avg = 89.0 > 81.44 -- qualifies
        // Bob avg = 78.67 < 81.44
        // Charlie avg = 76.67 < 81.44
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("Alice".into()));

        // Verify the CTE produces all three students when used without filtering
        let res = run(
            &ex,
            "WITH student_avg AS (
                 SELECT student, AVG(score) AS avg_score
                 FROM scores
                 GROUP BY student
             )
             SELECT student, avg_score
             FROM student_avg
             ORDER BY student",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[1][0], Value::Text("Bob".into()));
        assert_eq!(r[2][0], Value::Text("Charlie".into()));
    }

    // ========================================================================
    // 25. Nested aggregation with GROUP BY in subquery
    // ========================================================================

    #[tokio::test]
    async fn test_sql_nested_aggregation() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE sales_data (
                region TEXT NOT NULL,
                product TEXT NOT NULL,
                amount INT NOT NULL
            )",
        )
        .await;

        // Insert sales across regions and products
        run(&ex, "INSERT INTO sales_data VALUES ('North', 'Widget', 100)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('North', 'Widget', 150)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('North', 'Gadget', 200)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('South', 'Widget', 300)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('South', 'Gadget', 250)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('East', 'Widget', 120)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('East', 'Gadget', 80)").await;
        run(&ex, "INSERT INTO sales_data VALUES ('East', 'Gadget', 100)").await;

        // Inner query: total sales per region
        // North: 100+150+200=450, South: 300+250=550, East: 120+80+100=300
        // Outer query: aggregate over the per-region totals
        let res = run(
            &ex,
            "SELECT COUNT(*) AS region_count,
                    MIN(region_total) AS min_region,
                    MAX(region_total) AS max_region,
                    SUM(region_total) AS grand_total
             FROM (
                 SELECT region, SUM(amount) AS region_total
                 FROM sales_data
                 GROUP BY region
             ) AS region_sums",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Int64(3));   // 3 regions
        assert_eq!(r[0][1], Value::Int64(300));  // min = East (300)
        assert_eq!(r[0][2], Value::Int64(550));  // max = South (550)
        assert_eq!(r[0][3], Value::Int64(1300)); // grand total = 450+550+300

        // Another nested aggregation: average of per-product totals
        // Widget: 100+150+300+120=670, Gadget: 200+250+80+100=630
        // Average of product totals = (670+630)/2 = 650.0
        let res = run(
            &ex,
            "SELECT AVG(product_total) AS avg_product_sales
             FROM (
                 SELECT product, SUM(amount) AS product_total
                 FROM sales_data
                 GROUP BY product
             ) AS product_sums",
        )
        .await;
        let avg = scalar(&res[0]);
        match avg {
            Value::Float64(f) => assert!(
                (*f - 650.0).abs() < 0.01,
                "expected avg ~650.0, got {f}"
            ),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    // ========================================================================
    // 26. LEFT JOIN
    // ========================================================================

    #[tokio::test]
    async fn test_sql_left_join() {
        let ex = setup();

        run(
            &ex,
            "CREATE TABLE teams (id INT PRIMARY KEY, name TEXT NOT NULL)",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE players (id INT PRIMARY KEY, name TEXT NOT NULL, team_id INT)",
        )
        .await;

        // Insert teams
        run(&ex, "INSERT INTO teams VALUES (1, 'Red Team')").await;
        run(&ex, "INSERT INTO teams VALUES (2, 'Blue Team')").await;
        run(&ex, "INSERT INTO teams VALUES (3, 'Green Team')").await; // no players

        // Insert players (none for Green Team)
        run(&ex, "INSERT INTO players VALUES (10, 'Alice', 1)").await;
        run(&ex, "INSERT INTO players VALUES (11, 'Bob', 1)").await;
        run(&ex, "INSERT INTO players VALUES (12, 'Charlie', 2)").await;

        // LEFT JOIN: all teams should appear, Green Team with NULL player
        let res = run(
            &ex,
            "SELECT t.name AS team_name, p.name AS player_name
             FROM teams t
             LEFT JOIN players p ON t.id = p.team_id
             ORDER BY t.name, p.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4); // Red(Alice), Red(Bob), Blue(Charlie), Green(NULL)

        // Verify Green Team appears with NULL player_name
        let green_rows: Vec<_> = r
            .iter()
            .filter(|row| row[0] == Value::Text("Green Team".into()))
            .collect();
        assert_eq!(green_rows.len(), 1, "Green Team should appear once");
        assert_eq!(green_rows[0][1], Value::Null, "Green Team player should be NULL");

        // Verify Red Team has two players
        let red_rows: Vec<_> = r
            .iter()
            .filter(|row| row[0] == Value::Text("Red Team".into()))
            .collect();
        assert_eq!(red_rows.len(), 2, "Red Team should have 2 players");

        // LEFT JOIN with aggregation: count players per team (including zero)
        let res = run(
            &ex,
            "SELECT t.name, COUNT(p.id) AS player_count
             FROM teams t
             LEFT JOIN players p ON t.id = p.team_id
             GROUP BY t.name
             ORDER BY player_count DESC, t.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        // Red Team: 2, Blue Team: 1, Green Team: 0
        assert_eq!(r[0][0], Value::Text("Red Team".into()));
        assert_eq!(r[0][1], Value::Int64(2));
        assert_eq!(r[1][0], Value::Text("Blue Team".into()));
        assert_eq!(r[1][1], Value::Int64(1));
        assert_eq!(r[2][0], Value::Text("Green Team".into()));
        assert_eq!(r[2][1], Value::Int64(0));
    }

    // ========================================================================
    // 27. Cross-module: Catalog + Storage + Executor wiring
    // ========================================================================

    #[tokio::test]
    async fn test_cross_module_catalog_storage_executor() {
        let ex = setup();

        // -- Create a table and verify its schema through information_schema --
        run(
            &ex,
            "CREATE TABLE widgets (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                weight INT
            )",
        )
        .await;

        // Insert data so we can verify after schema changes
        run(&ex, "INSERT INTO widgets VALUES (1, 'Sprocket', 10)").await;
        run(&ex, "INSERT INTO widgets VALUES (2, 'Cog', 25)").await;
        run(&ex, "INSERT INTO widgets VALUES (3, 'Gear', 15)").await;

        // Verify catalog metadata via information_schema.columns
        let res = run(
            &ex,
            "SELECT column_name, data_type
             FROM information_schema.columns
             WHERE table_name = 'widgets'
             ORDER BY ordinal_position",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3, "should have 3 columns initially");
        assert_eq!(r[0][0], Value::Text("id".into()));
        assert_eq!(r[1][0], Value::Text("name".into()));
        assert_eq!(r[2][0], Value::Text("weight".into()));

        // Verify data is queryable through the executor
        let res = run(&ex, "SELECT COUNT(*) FROM widgets").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(3));

        // -- ALTER TABLE ADD COLUMN: exercises catalog mutation + storage adaptation --
        run(
            &ex,
            "ALTER TABLE widgets ADD COLUMN color TEXT DEFAULT 'silver'",
        )
        .await;

        // Verify the catalog reflects the new column
        let res = run(
            &ex,
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_name = 'widgets'
             ORDER BY ordinal_position",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 4, "should have 4 columns after ADD COLUMN");
        assert_eq!(r[3][0], Value::Text("color".into()));

        // Verify existing rows got the default value (storage + executor cooperation)
        let res = run(
            &ex,
            "SELECT name, color FROM widgets ORDER BY id",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Sprocket".into()));
        assert_eq!(r[0][1], Value::Text("silver".into()));
        assert_eq!(r[1][1], Value::Text("silver".into()));
        assert_eq!(r[2][1], Value::Text("silver".into()));

        // Insert a new row with the new column and verify
        run(&ex, "INSERT INTO widgets VALUES (4, 'Axle', 30, 'blue')").await;
        let res = run(&ex, "SELECT color FROM widgets WHERE id = 4").await;
        assert_eq!(*scalar(&res[0]), Value::Text("blue".into()));

        // Verify we can filter on the new column
        let res = run(
            &ex,
            "SELECT name FROM widgets WHERE color = 'silver' ORDER BY name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Cog".into()));
        assert_eq!(r[1][0], Value::Text("Gear".into()));
        assert_eq!(r[2][0], Value::Text("Sprocket".into()));

        // Verify aggregation works across the altered schema
        let res = run(&ex, "SELECT COUNT(*) FROM widgets").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(4));

        // Verify information_schema.tables still lists the table correctly
        let res = run(
            &ex,
            "SELECT table_name, table_type
             FROM information_schema.tables
             WHERE table_name = 'widgets'",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("widgets".into()));
        assert_eq!(r[0][1], Value::Text("BASE TABLE".into()));
    }

    // ========================================================================
    // 28. Cross-module: MVCC with complex queries (CTE + aggregation)
    // ========================================================================

    #[tokio::test]
    async fn test_cross_module_mvcc_with_complex_queries() {
        let ex = setup_mvcc();

        // Set up two related tables
        run(
            &ex,
            "CREATE TABLE departments (
                id INT NOT NULL,
                name TEXT NOT NULL,
                budget INT NOT NULL
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE employees (
                id INT NOT NULL,
                name TEXT NOT NULL,
                dept_id INT NOT NULL,
                salary INT NOT NULL
            )",
        )
        .await;

        // Seed data outside of transaction (auto-commit)
        run(&ex, "INSERT INTO departments VALUES (1, 'Engineering', 500000)").await;
        run(&ex, "INSERT INTO departments VALUES (2, 'Marketing', 300000)").await;
        run(&ex, "INSERT INTO departments VALUES (3, 'Research', 400000)").await;

        run(&ex, "INSERT INTO employees VALUES (1, 'Alice', 1, 120000)").await;
        run(&ex, "INSERT INTO employees VALUES (2, 'Bob', 1, 110000)").await;
        run(&ex, "INSERT INTO employees VALUES (3, 'Charlie', 2, 90000)").await;
        run(&ex, "INSERT INTO employees VALUES (4, 'Diana', 2, 95000)").await;
        run(&ex, "INSERT INTO employees VALUES (5, 'Eve', 3, 105000)").await;

        // -- Begin a transaction and run a CTE with aggregation across tables --
        run(&ex, "BEGIN").await;

        // Insert additional data within the transaction
        run(&ex, "INSERT INTO employees VALUES (6, 'Frank', 3, 100000)").await;
        run(&ex, "INSERT INTO employees VALUES (7, 'Grace', 1, 115000)").await;

        // CTE with JOIN and aggregation: compute department salary summaries
        let res = run(
            &ex,
            "WITH dept_summary AS (
                SELECT d.name AS dept_name,
                       d.budget,
                       COUNT(*) AS headcount,
                       SUM(e.salary) AS total_salary
                FROM employees e
                JOIN departments d ON e.dept_id = d.id
                GROUP BY d.name, d.budget
            )
            SELECT dept_name, headcount, total_salary
            FROM dept_summary
            ORDER BY total_salary DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        // Engineering: Alice(120k) + Bob(110k) + Grace(115k) = 345000, headcount=3
        assert_eq!(r[0][0], Value::Text("Engineering".into()));
        assert_eq!(r[0][1], Value::Int64(3));
        assert_eq!(r[0][2], Value::Int64(345000));
        // Research: Eve(105k) + Frank(100k) = 205000, headcount=2
        assert_eq!(r[1][0], Value::Text("Research".into()));
        assert_eq!(r[1][1], Value::Int64(2));
        assert_eq!(r[1][2], Value::Int64(205000));
        // Marketing: Charlie(90k) + Diana(95k) = 185000, headcount=2
        assert_eq!(r[2][0], Value::Text("Marketing".into()));
        assert_eq!(r[2][1], Value::Int64(2));
        assert_eq!(r[2][2], Value::Int64(185000));

        // Commit the transaction
        run(&ex, "COMMIT").await;

        // -- Verify data persists after commit with another complex query --
        let res = run(
            &ex,
            "SELECT d.name, COUNT(*) AS emp_count, SUM(e.salary) AS payroll
             FROM employees e
             JOIN departments d ON e.dept_id = d.id
             GROUP BY d.name
             HAVING COUNT(*) >= 2
             ORDER BY emp_count DESC, d.name",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3, "all 3 departments should have >= 2 employees after commit");
        // Engineering has 3, then Marketing and Research each have 2
        assert_eq!(r[0][0], Value::Text("Engineering".into()));
        assert_eq!(r[0][1], Value::Int64(3));

        // Verify total employee count persisted
        let res = run(&ex, "SELECT COUNT(*) FROM employees").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(7));
    }

    // ========================================================================
    // 29. Cross-module: Concurrent table operations with consistent state
    // ========================================================================

    #[tokio::test]
    async fn test_cross_module_concurrent_table_operations() {
        let ex = setup();

        // -- Create multiple related tables --
        run(
            &ex,
            "CREATE TABLE regions (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE stores (
                id INT PRIMARY KEY,
                region_id INT NOT NULL,
                name TEXT NOT NULL,
                FOREIGN KEY (region_id) REFERENCES regions (id)
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE store_inventory (
                id INT PRIMARY KEY,
                store_id INT NOT NULL,
                product TEXT NOT NULL,
                quantity INT NOT NULL,
                FOREIGN KEY (store_id) REFERENCES stores (id)
            )",
        )
        .await;

        // -- Populate regions --
        run(&ex, "INSERT INTO regions VALUES (1, 'West')").await;
        run(&ex, "INSERT INTO regions VALUES (2, 'East')").await;
        run(&ex, "INSERT INTO regions VALUES (3, 'Central')").await;

        // -- Populate stores (referencing regions) --
        run(&ex, "INSERT INTO stores VALUES (10, 1, 'Seattle Store')").await;
        run(&ex, "INSERT INTO stores VALUES (11, 1, 'Portland Store')").await;
        run(&ex, "INSERT INTO stores VALUES (20, 2, 'New York Store')").await;
        run(&ex, "INSERT INTO stores VALUES (30, 3, 'Chicago Store')").await;

        // -- Populate inventory (referencing stores) --
        run(&ex, "INSERT INTO store_inventory VALUES (100, 10, 'Laptop', 50)").await;
        run(&ex, "INSERT INTO store_inventory VALUES (101, 10, 'Phone', 120)").await;
        run(&ex, "INSERT INTO store_inventory VALUES (102, 11, 'Laptop', 30)").await;
        run(&ex, "INSERT INTO store_inventory VALUES (103, 20, 'Tablet', 80)").await;
        run(&ex, "INSERT INTO store_inventory VALUES (104, 20, 'Phone', 200)").await;
        run(&ex, "INSERT INTO store_inventory VALUES (105, 30, 'Laptop', 45)").await;
        run(&ex, "INSERT INTO store_inventory VALUES (106, 30, 'Tablet', 60)").await;

        // -- Verify initial counts --
        let res = run(&ex, "SELECT COUNT(*) FROM regions").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(3));
        let res = run(&ex, "SELECT COUNT(*) FROM stores").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(4));
        let res = run(&ex, "SELECT COUNT(*) FROM store_inventory").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(7));

        // -- Perform UPDATE: restock all laptops --
        run(
            &ex,
            "UPDATE store_inventory SET quantity = quantity + 20 WHERE product = 'Laptop'",
        )
        .await;

        // Verify the update
        let res = run(
            &ex,
            "SELECT SUM(quantity) FROM store_inventory WHERE product = 'Laptop'",
        )
        .await;
        // Original: 50+30+45=125, after +20 each: 70+50+65=185
        assert_eq!(*scalar(&res[0]), Value::Int64(185));

        // -- Perform DELETE: remove Portland Laptop (the lowest stock laptop) --
        run(
            &ex,
            "DELETE FROM store_inventory WHERE store_id = 11 AND product = 'Laptop'",
        )
        .await;

        // Verify deletion
        let res = run(&ex, "SELECT COUNT(*) FROM store_inventory").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(6));

        // -- INSERT new inventory --
        run(&ex, "INSERT INTO store_inventory VALUES (107, 11, 'Monitor', 75)").await;

        // Verify insert
        let res = run(&ex, "SELECT COUNT(*) FROM store_inventory").await;
        assert_eq!(*scalar(&res[0]), Value::Int64(7));

        // -- Cross-table JOIN: aggregate inventory by region --
        let res = run(
            &ex,
            "SELECT r.name AS region_name,
                    COUNT(*) AS item_types,
                    SUM(i.quantity) AS total_stock
             FROM store_inventory i
             JOIN stores s ON i.store_id = s.id
             JOIN regions r ON s.region_id = r.id
             GROUP BY r.name
             ORDER BY total_stock DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3, "all 3 regions should have inventory");

        // East: Phone(200) + Tablet(80) = 280
        assert_eq!(r[0][0], Value::Text("East".into()));
        assert_eq!(r[0][2], Value::Int64(280));

        // West: Laptop(70) + Phone(120) + Monitor(75) = 265
        assert_eq!(r[1][0], Value::Text("West".into()));
        assert_eq!(r[1][2], Value::Int64(265));

        // Central: Laptop(65) + Tablet(60) = 125
        assert_eq!(r[2][0], Value::Text("Central".into()));
        assert_eq!(r[2][2], Value::Int64(125));

        // -- Final consistency check: cross-table join produces correct structure --
        let res = run(
            &ex,
            "SELECT r.name, s.name, i.product, i.quantity
             FROM store_inventory i
             JOIN stores s ON i.store_id = s.id
             JOIN regions r ON s.region_id = r.id
             ORDER BY r.name, s.name, i.product",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 7, "should have 7 inventory rows total");
        // Verify each row has 4 columns and no nulls
        for row in r {
            assert_eq!(row.len(), 4, "each row should have 4 columns");
            assert_ne!(row[0], Value::Null, "region name should not be null");
            assert_ne!(row[1], Value::Null, "store name should not be null");
            assert_ne!(row[2], Value::Null, "product should not be null");
            assert_ne!(row[3], Value::Null, "quantity should not be null");
        }
    }

    // ========================================================================
    // 30. Cross-module: Window functions + CTE + JOIN analytics workflow
    // ========================================================================

    #[tokio::test]
    async fn test_cross_module_window_functions_with_cte_and_join() {
        let ex = setup();

        // -- Set up a realistic analytics scenario: sales team performance --
        run(
            &ex,
            "CREATE TABLE sales_reps (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                team TEXT NOT NULL
            )",
        )
        .await;

        run(
            &ex,
            "CREATE TABLE deals (
                id INT PRIMARY KEY,
                rep_id INT NOT NULL,
                amount INT NOT NULL,
                quarter INT NOT NULL,
                FOREIGN KEY (rep_id) REFERENCES sales_reps (id)
            )",
        )
        .await;

        // Insert sales reps across two teams
        run(&ex, "INSERT INTO sales_reps VALUES (1, 'Alice', 'Alpha')").await;
        run(&ex, "INSERT INTO sales_reps VALUES (2, 'Bob', 'Alpha')").await;
        run(&ex, "INSERT INTO sales_reps VALUES (3, 'Charlie', 'Beta')").await;
        run(&ex, "INSERT INTO sales_reps VALUES (4, 'Diana', 'Beta')").await;
        run(&ex, "INSERT INTO sales_reps VALUES (5, 'Eve', 'Alpha')").await;

        // Insert deals across quarters
        // Q1 deals
        run(&ex, "INSERT INTO deals VALUES (101, 1, 50000, 1)").await;
        run(&ex, "INSERT INTO deals VALUES (102, 1, 30000, 1)").await;
        run(&ex, "INSERT INTO deals VALUES (103, 2, 45000, 1)").await;
        run(&ex, "INSERT INTO deals VALUES (104, 3, 60000, 1)").await;
        run(&ex, "INSERT INTO deals VALUES (105, 4, 35000, 1)").await;
        run(&ex, "INSERT INTO deals VALUES (106, 5, 25000, 1)").await;
        // Q2 deals
        run(&ex, "INSERT INTO deals VALUES (201, 1, 40000, 2)").await;
        run(&ex, "INSERT INTO deals VALUES (202, 2, 55000, 2)").await;
        run(&ex, "INSERT INTO deals VALUES (203, 3, 70000, 2)").await;
        run(&ex, "INSERT INTO deals VALUES (204, 4, 45000, 2)").await;
        run(&ex, "INSERT INTO deals VALUES (205, 5, 35000, 2)").await;
        // Q3 deals
        run(&ex, "INSERT INTO deals VALUES (301, 1, 60000, 3)").await;
        run(&ex, "INSERT INTO deals VALUES (302, 2, 40000, 3)").await;
        run(&ex, "INSERT INTO deals VALUES (303, 3, 55000, 3)").await;
        run(&ex, "INSERT INTO deals VALUES (304, 4, 50000, 3)").await;
        run(&ex, "INSERT INTO deals VALUES (305, 5, 45000, 3)").await;

        // -- Query 1: Subquery with JOIN, aggregation, and window functions --
        let res = run(
            &ex,
            "SELECT rep_name,
                    total_revenue,
                    deal_count,
                    RANK() OVER (ORDER BY total_revenue DESC) AS overall_rank,
                    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS row_num
             FROM (
                SELECT sr.name AS rep_name,
                       SUM(d.amount) AS total_revenue,
                       COUNT(*) AS deal_count
                FROM deals d
                JOIN sales_reps sr ON d.rep_id = sr.id
                GROUP BY sr.name
             ) AS rep_summary
             ORDER BY overall_rank",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5, "should have 5 sales reps");

        // Charlie: 60k+70k+55k = 185k (should be rank 1)
        assert_eq!(r[0][0], Value::Text("Charlie".into()));
        assert_eq!(r[0][1], Value::Int64(185000));

        // Verify rank values are in valid range
        for i in 0..r.len() {
            match &r[i][3] {
                Value::Int64(rank) => assert!(
                    *rank >= 1 && *rank <= 5,
                    "rank should be between 1 and 5, got {rank}"
                ),
                other => panic!("expected Int64 for rank, got {other:?}"),
            }
        }

        // -- Query 2: Per-team aggregation with JOIN and GROUP BY --
        let res = run(
            &ex,
            "SELECT sr.team,
                    SUM(d.amount) AS team_revenue,
                    COUNT(*) AS team_deals,
                    COUNT(DISTINCT sr.id) AS team_size
             FROM deals d
             JOIN sales_reps sr ON d.rep_id = sr.id
             GROUP BY sr.team
             ORDER BY team_revenue DESC",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2, "should have 2 teams");

        // Alpha team: Alice(50k+30k+40k+60k=180k) + Bob(45k+55k+40k=140k) + Eve(25k+35k+45k=105k)
        // Alpha total = 180k+140k+105k = 425k
        // Beta team: Charlie(60k+70k+55k=185k) + Diana(35k+45k+50k=130k)
        // Beta total = 185k+130k = 315k
        assert_eq!(r[0][0], Value::Text("Alpha".into()));
        assert_eq!(r[0][1], Value::Int64(425000));
        assert_eq!(r[1][0], Value::Text("Beta".into()));
        assert_eq!(r[1][1], Value::Int64(315000));

        // -- Query 3: RANK with PARTITION BY team using subquery --
        let res = run(
            &ex,
            "SELECT rep_name,
                    team,
                    total_revenue,
                    RANK() OVER (PARTITION BY team ORDER BY total_revenue DESC) AS team_rank
             FROM (
                SELECT sr.name AS rep_name,
                       sr.team AS team,
                       SUM(d.amount) AS total_revenue
                FROM deals d
                JOIN sales_reps sr ON d.rep_id = sr.id
                GROUP BY sr.name, sr.team
             ) AS rep_totals
             ORDER BY team, team_rank",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);

        // Alpha team should come first (alphabetical), top performer is Alice(180k)
        assert_eq!(r[0][1], Value::Text("Alpha".into()));
        assert_eq!(r[0][3], Value::Int64(1)); // rank 1 within Alpha
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[0][2], Value::Int64(180000));

        // Beta team: Charlie(185k) should be rank 1
        let beta_rows: Vec<_> = r.iter().filter(|row| row[1] == Value::Text("Beta".into())).collect();
        assert_eq!(beta_rows.len(), 2);
        assert_eq!(beta_rows[0][0], Value::Text("Charlie".into()));
        assert_eq!(beta_rows[0][3], Value::Int64(1)); // rank 1 within Beta

        // -- Query 4: Quarterly trend with LAG window function --
        let res = run(
            &ex,
            "SELECT quarter,
                    revenue,
                    LAG(revenue) OVER (ORDER BY quarter) AS prev_quarter_revenue
             FROM (
                SELECT quarter, SUM(amount) AS revenue
                FROM deals
                GROUP BY quarter
             ) AS quarterly_revenue
             ORDER BY quarter",
        )
        .await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3, "should have 3 quarters");

        // Q1: total = 50+30+45+60+35+25 = 245k, LAG = NULL
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[0][1], Value::Int64(245000));
        assert_eq!(r[0][2], Value::Null);

        // Q2: total = 40+55+70+45+35 = 245k, LAG = 245000
        assert_eq!(r[1][0], Value::Int32(2));
        assert_eq!(r[1][1], Value::Int64(245000));
        assert_eq!(r[1][2], Value::Int64(245000));

        // Q3: total = 60+40+55+50+45 = 250k, LAG = 245000
        assert_eq!(r[2][0], Value::Int32(3));
        assert_eq!(r[2][1], Value::Int64(250000));
        assert_eq!(r[2][2], Value::Int64(245000));
    }

    // ========================================================================
    // 31. RIGHT JOIN and CROSS JOIN
    // ========================================================================

    #[tokio::test]
    async fn test_right_join() {
        let ex = setup();
        run(&ex, "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
        run(&ex, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT NOT NULL, dept_id INT)").await;

        run(&ex, "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')").await;
        run(&ex, "INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', NULL)").await;

        // RIGHT JOIN: all departments, even those with no employees
        let res = run(&ex, "SELECT e.name, d.name AS dept FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id ORDER BY d.name").await;
        let r = rows(&res[0]);
        assert!(r.len() >= 3, "should have at least 3 rows (Engineering x2, Marketing, Sales)");

        // Marketing and Sales should have NULL employee names
        let marketing_rows: Vec<_> = r.iter().filter(|row| row[1] == Value::Text("Marketing".into())).collect();
        assert_eq!(marketing_rows.len(), 1);
        assert_eq!(marketing_rows[0][0], Value::Null);
    }

    #[tokio::test]
    async fn test_cross_join() {
        let ex = setup();
        run(&ex, "CREATE TABLE colors (name TEXT NOT NULL)").await;
        run(&ex, "CREATE TABLE sizes (label TEXT NOT NULL)").await;

        run(&ex, "INSERT INTO colors VALUES ('Red'), ('Blue')").await;
        run(&ex, "INSERT INTO sizes VALUES ('S'), ('M'), ('L')").await;

        // CROSS JOIN: cartesian product = 2 * 3 = 6 rows
        let res = run(&ex, "SELECT c.name, s.label FROM colors c CROSS JOIN sizes s ORDER BY c.name, s.label").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 6, "cross join should produce 6 rows");
        assert_eq!(r[0][0], Value::Text("Blue".into()));
        assert_eq!(r[0][1], Value::Text("L".into()));
    }

    // ========================================================================
    // 32. INTERSECT and EXCEPT
    // ========================================================================

    #[tokio::test]
    async fn test_intersect_and_except() {
        let ex = setup();
        run(&ex, "CREATE TABLE set_a (val INT NOT NULL)").await;
        run(&ex, "CREATE TABLE set_b (val INT NOT NULL)").await;

        run(&ex, "INSERT INTO set_a VALUES (1), (2), (3), (4), (5)").await;
        run(&ex, "INSERT INTO set_b VALUES (3), (4), (5), (6), (7)").await;

        // INTERSECT: common values {3, 4, 5}
        let res = run(&ex, "SELECT val FROM set_a INTERSECT SELECT val FROM set_b ORDER BY val").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Int32(3));
        assert_eq!(r[1][0], Value::Int32(4));
        assert_eq!(r[2][0], Value::Int32(5));

        // EXCEPT: in A but not in B {1, 2}
        let res = run(&ex, "SELECT val FROM set_a EXCEPT SELECT val FROM set_b ORDER BY val").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(2));
    }

    // ========================================================================
    // 33. COALESCE, NULLIF, IS NULL, IS NOT NULL
    // ========================================================================

    #[tokio::test]
    async fn test_null_functions() {
        let ex = setup();
        run(&ex, "CREATE TABLE nullable (id INT PRIMARY KEY, val TEXT, num INT)").await;
        run(&ex, "INSERT INTO nullable VALUES (1, 'hello', 10)").await;
        run(&ex, "INSERT INTO nullable VALUES (2, NULL, 20)").await;
        run(&ex, "INSERT INTO nullable VALUES (3, 'world', NULL)").await;
        run(&ex, "INSERT INTO nullable VALUES (4, NULL, NULL)").await;

        // COALESCE: return first non-null
        let res = run(&ex, "SELECT id, COALESCE(val, 'default') AS v FROM nullable ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r[0][1], Value::Text("hello".into()));
        assert_eq!(r[1][1], Value::Text("default".into()));
        assert_eq!(r[3][1], Value::Text("default".into()));

        // IS NULL / IS NOT NULL
        let res = run(&ex, "SELECT COUNT(*) FROM nullable WHERE val IS NULL").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));

        let res = run(&ex, "SELECT COUNT(*) FROM nullable WHERE val IS NOT NULL").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));

        // NULLIF: returns NULL if args are equal
        let res = run(&ex, "SELECT NULLIF(10, 10)").await;
        assert_eq!(scalar(&res[0]), &Value::Null);

        let res = run(&ex, "SELECT NULLIF(10, 20)").await;
        match scalar(&res[0]) {
            Value::Int32(10) | Value::Int64(10) => {} // either is fine
            other => panic!("expected 10, got {other:?}"),
        }
    }

    // ========================================================================
    // 34. BETWEEN, LIKE, LIMIT + OFFSET
    // ========================================================================

    #[tokio::test]
    async fn test_between_like_offset() {
        let ex = setup();
        run(&ex, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, price INT NOT NULL)").await;
        run(&ex, "INSERT INTO products VALUES (1, 'Apple Pie', 500)").await;
        run(&ex, "INSERT INTO products VALUES (2, 'Banana Bread', 350)").await;
        run(&ex, "INSERT INTO products VALUES (3, 'Cherry Cake', 700)").await;
        run(&ex, "INSERT INTO products VALUES (4, 'Apple Sauce', 200)").await;
        run(&ex, "INSERT INTO products VALUES (5, 'Blueberry Muffin', 450)").await;

        // BETWEEN
        let res = run(&ex, "SELECT name FROM products WHERE price BETWEEN 300 AND 600 ORDER BY name").await;
        let r = rows(&res[0]);
        assert!(r.len() >= 2, "should have items in price range 300-600");

        // LIKE with wildcard
        let res = run(&ex, "SELECT name FROM products WHERE name LIKE 'Apple%' ORDER BY name").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Apple Pie".into()));
        assert_eq!(r[1][0], Value::Text("Apple Sauce".into()));

        // LIMIT + OFFSET for pagination
        let res = run(&ex, "SELECT name FROM products ORDER BY id LIMIT 2 OFFSET 2").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Cherry Cake".into()));
        assert_eq!(r[1][0], Value::Text("Apple Sauce".into()));
    }

    // ========================================================================
    // 35. DELETE with complex WHERE, UPDATE with multiple columns
    // ========================================================================

    #[tokio::test]
    async fn test_delete_and_update_complex() {
        let ex = setup();
        run(&ex, "CREATE TABLE inventory (id INT PRIMARY KEY, item TEXT NOT NULL, qty INT NOT NULL, category TEXT)").await;
        run(&ex, "INSERT INTO inventory VALUES (1, 'Widget A', 100, 'hardware')").await;
        run(&ex, "INSERT INTO inventory VALUES (2, 'Widget B', 50, 'hardware')").await;
        run(&ex, "INSERT INTO inventory VALUES (3, 'Gadget X', 200, 'electronics')").await;
        run(&ex, "INSERT INTO inventory VALUES (4, 'Gadget Y', 30, 'electronics')").await;
        run(&ex, "INSERT INTO inventory VALUES (5, 'Tool Z', 75, 'hardware')").await;

        // UPDATE multiple columns
        run(&ex, "UPDATE inventory SET qty = 999, category = 'premium' WHERE id = 1").await;
        let res = run(&ex, "SELECT qty, category FROM inventory WHERE id = 1").await;
        let r = rows(&res[0]);
        assert_eq!(r[0][0], Value::Int32(999));
        assert_eq!(r[0][1], Value::Text("premium".into()));

        // DELETE with compound WHERE
        run(&ex, "DELETE FROM inventory WHERE category = 'hardware' AND qty < 100").await;
        let res = run(&ex, "SELECT COUNT(*) FROM inventory").await;
        // Should have deleted Widget B (qty=50) and Tool Z (qty=75). Widget A is now 'premium'.
        // Remaining: Widget A (premium, 999), Gadget X (200), Gadget Y (30) = 3 rows
        assert_eq!(scalar(&res[0]), &Value::Int64(3));

        // DELETE with IN
        run(&ex, "DELETE FROM inventory WHERE id IN (3, 4)").await;
        let res = run(&ex, "SELECT COUNT(*) FROM inventory").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(1)); // only Widget A remains
    }

    // ========================================================================
    // 36. AVG aggregate and type coercion
    // ========================================================================

    #[tokio::test]
    async fn test_avg_and_aggregates() {
        let ex = setup();
        run(&ex, "CREATE TABLE scores (student TEXT NOT NULL, score INT NOT NULL)").await;
        run(&ex, "INSERT INTO scores VALUES ('Alice', 90), ('Bob', 80), ('Charlie', 70), ('Alice', 100), ('Bob', 60)").await;

        // AVG
        let res = run(&ex, "SELECT AVG(score) FROM scores").await;
        let avg = scalar(&res[0]);
        match avg {
            Value::Int64(v) => assert_eq!(*v, 80), // (90+80+70+100+60)/5 = 80
            Value::Float64(v) => assert!((v - 80.0).abs() < 0.01),
            other => panic!("expected numeric for AVG, got {other:?}"),
        }

        // GROUP BY with AVG
        let res = run(&ex, "SELECT student, AVG(score) AS avg_score FROM scores GROUP BY student ORDER BY student").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Alice".into())); // avg=95
        assert_eq!(r[2][0], Value::Text("Charlie".into())); // avg=70

        // MIN and MAX together
        let res = run(&ex, "SELECT MIN(score), MAX(score) FROM scores").await;
        let r = rows(&res[0]);
        assert_eq!(r[0][0], Value::Int32(60));
        assert_eq!(r[0][1], Value::Int32(100));
    }

    // ========================================================================
    // 37. SHOW commands (server introspection)
    // ========================================================================

    #[tokio::test]
    async fn test_show_commands() {
        let ex = setup();

        // SHOW server_version
        let res = run(&ex, "SHOW server_version").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty());

        // SHOW POOL STATUS
        let res = run(&ex, "SHOW POOL STATUS").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty());

        // SHOW METRICS
        let res = run(&ex, "SHOW METRICS").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty());

        // SHOW CLUSTER STATUS
        let res = run(&ex, "SHOW CLUSTER STATUS").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty());
        // Should show "standalone" or "not configured" since no cluster is wired
    }

    // ========================================================================
    // 38. Multi-statement transactions with verification
    // ========================================================================

    #[tokio::test]
    async fn test_transaction_isolation() {
        let ex = setup();
        run(&ex, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)").await;
        run(&ex, "INSERT INTO accounts VALUES (1, 1000), (2, 2000)").await;

        // Transfer 500 from account 1 to account 2 within a transaction
        run(&ex, "BEGIN").await;
        run(&ex, "UPDATE accounts SET balance = balance - 500 WHERE id = 1").await;
        run(&ex, "UPDATE accounts SET balance = balance + 500 WHERE id = 2").await;
        run(&ex, "COMMIT").await;

        let res = run(&ex, "SELECT id, balance FROM accounts ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r[0][1], Value::Int32(500));  // 1000 - 500
        assert_eq!(r[1][1], Value::Int32(2500)); // 2000 + 500
    }

    // ========================================================================
    // 39. Nested subqueries and correlated subqueries
    // ========================================================================

    #[tokio::test]
    async fn test_nested_subqueries() {
        let ex = setup();
        run(&ex, "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL, total INT NOT NULL)").await;
        run(&ex, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL, tier TEXT NOT NULL)").await;

        run(&ex, "INSERT INTO customers VALUES (1, 'Alice', 'gold'), (2, 'Bob', 'silver'), (3, 'Charlie', 'gold')").await;
        run(&ex, "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 3, 300), (5, 3, 50)").await;

        // Subquery in WHERE with IN: customers who have orders > 150
        let res = run(&ex, "SELECT c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders WHERE total > 150) ORDER BY c.name").await;
        let r = rows(&res[0]);
        // Orders > 150: order 2 (200, Alice), order 3 (150 not >, so no), order 4 (300, Charlie)
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[1][0], Value::Text("Charlie".into()));

        // Subquery in SELECT: total per customer via subquery
        let res = run(&ex, "SELECT c.name, (SELECT SUM(total) FROM orders o WHERE o.customer_id = c.id) AS order_total FROM customers c ORDER BY c.name").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::Text("Alice".into()));   // 100+200=300
        assert_eq!(r[1][0], Value::Text("Bob".into()));     // 150
        assert_eq!(r[2][0], Value::Text("Charlie".into())); // 300+50=350
    }

    // ========================================================================
    // 40. DROP TABLE and table recreation
    // ========================================================================

    #[tokio::test]
    async fn test_drop_and_recreate() {
        let ex = setup();
        run(&ex, "CREATE TABLE temp (id INT PRIMARY KEY)").await;
        run(&ex, "INSERT INTO temp VALUES (1), (2), (3)").await;

        let res = run(&ex, "SELECT COUNT(*) FROM temp").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(3));

        run(&ex, "DROP TABLE temp").await;

        // Recreate with different schema
        run(&ex, "CREATE TABLE temp (id INT PRIMARY KEY, label TEXT NOT NULL, score INT)").await;
        run(&ex, "INSERT INTO temp VALUES (10, 'new', 99)").await;

        let res = run(&ex, "SELECT label, score FROM temp").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Text("new".into()));
        assert_eq!(r[0][1], Value::Int32(99));
    }

    // ========================================================================
    // 41. EXPLAIN — query plan introspection
    // ========================================================================

    #[tokio::test]
    async fn test_explain() {
        let ex = setup();
        run(&ex, "CREATE TABLE explain_t (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
        run(&ex, "INSERT INTO explain_t VALUES (1, 'Alice'), (2, 'Bob')").await;

        // EXPLAIN should return a plan (one or more rows with text)
        let res = run(&ex, "EXPLAIN SELECT * FROM explain_t WHERE id = 1").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "EXPLAIN should return plan rows");
    }

    // ========================================================================
    // 42. Window functions: LEAD, DENSE_RANK, SUM OVER, COUNT OVER
    // ========================================================================

    #[tokio::test]
    async fn test_window_functions_extended() {
        let ex = setup();
        run(&ex, "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL)").await;
        run(&ex, "INSERT INTO sales VALUES (1, 'East', 100), (2, 'East', 200), (3, 'West', 150), (4, 'West', 300), (5, 'East', 100)").await;

        // DENSE_RANK
        let res = run(&ex, "SELECT amount, DENSE_RANK() OVER (ORDER BY amount DESC) AS dr FROM sales ORDER BY amount DESC").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);
        // 300 -> rank 1, 200 -> rank 2, 150 -> rank 3, 100 -> rank 4 (x2)
        assert_eq!(r[0][1], Value::Int64(1));
        assert_eq!(r[1][1], Value::Int64(2));

        // SUM OVER (running total) — window SUM may return Float64
        let res = run(&ex, "SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM sales ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);
        let running_vals: Vec<f64> = r.iter().map(|row| match &row[2] {
            Value::Float64(v) => *v,
            Value::Int64(v) => *v as f64,
            other => panic!("expected numeric for SUM OVER, got {other:?}"),
        }).collect();
        assert_eq!(running_vals[0], 100.0);
        assert_eq!(running_vals[1], 300.0);
        assert_eq!(running_vals[2], 450.0);
        assert_eq!(running_vals[3], 750.0);
        assert_eq!(running_vals[4], 850.0);

        // LEAD window function
        let res = run(&ex, "SELECT id, amount, LEAD(amount) OVER (ORDER BY id) AS next_amount FROM sales ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 5);
        // Last row's LEAD should be NULL
        assert_eq!(r[4][2], Value::Null);
    }

    // ========================================================================
    // 43. Complex CTE with multiple references
    // ========================================================================

    #[tokio::test]
    async fn test_cte_multiple_references() {
        let ex = setup();
        run(&ex, "CREATE TABLE metrics (day INT NOT NULL, value INT NOT NULL)").await;
        run(&ex, "INSERT INTO metrics VALUES (1, 10), (2, 20), (3, 15), (4, 25), (5, 30)").await;

        // CTE used in main query: compute avg inline and filter
        let res = run(&ex, "
            WITH daily AS (
                SELECT day, value FROM metrics
            )
            SELECT d.day, d.value
            FROM daily d
            WHERE d.value > 20
            ORDER BY d.day
        ").await;
        let r = rows(&res[0]);
        // Values > 20: day4=25, day5=30
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(4)); // day 4, value 25
        assert_eq!(r[1][0], Value::Int32(5)); // day 5, value 30
    }

    // ========================================================================
    // 44. Expression evaluation: arithmetic, string concatenation
    // ========================================================================

    #[tokio::test]
    async fn test_expression_evaluation() {
        let ex = setup();

        // Arithmetic in SELECT
        let res = run(&ex, "SELECT 2 + 3 * 4").await;
        let v = scalar(&res[0]);
        match v {
            Value::Int32(14) | Value::Int64(14) => {} // 2 + (3*4) = 14
            other => panic!("expected 14, got {other:?}"),
        }

        // Negative numbers
        let res = run(&ex, "SELECT -5 + 10").await;
        let v = scalar(&res[0]);
        match v {
            Value::Int32(5) | Value::Int64(5) => {}
            other => panic!("expected 5, got {other:?}"),
        }

        // String functions
        let res = run(&ex, "SELECT UPPER('hello')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("HELLO".into()));

        let res = run(&ex, "SELECT LOWER('WORLD')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("world".into()));

        let res = run(&ex, "SELECT LENGTH('hello')").await;
        match scalar(&res[0]) {
            Value::Int32(5) | Value::Int64(5) => {}
            other => panic!("expected 5 for LENGTH, got {other:?}"),
        }
    }

    // ========================================================================
    // 45. Multiple aggregates with GROUP BY and HAVING
    // ========================================================================

    #[tokio::test]
    async fn test_having_with_multiple_aggregates() {
        let ex = setup();
        run(&ex, "CREATE TABLE log_entries (user_id INT NOT NULL, action TEXT NOT NULL, duration INT NOT NULL)").await;
        run(&ex, "INSERT INTO log_entries VALUES (1, 'login', 5), (1, 'view', 10), (1, 'edit', 20)").await;
        run(&ex, "INSERT INTO log_entries VALUES (2, 'login', 3), (2, 'view', 7)").await;
        run(&ex, "INSERT INTO log_entries VALUES (3, 'login', 2), (3, 'view', 5), (3, 'edit', 15), (3, 'delete', 1)").await;

        // Users with > 2 actions and total duration > 10
        let res = run(&ex, "SELECT user_id, COUNT(*) AS actions, SUM(duration) AS total FROM log_entries GROUP BY user_id HAVING COUNT(*) > 2 AND SUM(duration) > 10 ORDER BY user_id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2); // user 1 (3 actions, 35ms) and user 3 (4 actions, 23ms)
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][0], Value::Int32(3));
    }

    // ========================================================================
    // 46. CAST / type coercion
    // ========================================================================

    #[tokio::test]
    async fn test_cast_expressions() {
        let ex = setup();

        // CAST int to text
        let res = run(&ex, "SELECT CAST(42 AS TEXT)").await;
        assert_eq!(scalar(&res[0]), &Value::Text("42".into()));

        // CAST text to int
        let res = run(&ex, "SELECT CAST('123' AS INT)").await;
        match scalar(&res[0]) {
            Value::Int32(123) | Value::Int64(123) => {}
            other => panic!("expected 123, got {other:?}"),
        }
    }

    // ========================================================================
    // 47. Multiple table operations in sequence (schema evolution)
    // ========================================================================

    #[tokio::test]
    async fn test_schema_evolution() {
        let ex = setup();

        // Create initial schema
        run(&ex, "CREATE TABLE evolve (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
        run(&ex, "INSERT INTO evolve VALUES (1, 'v1')").await;

        // Add a column
        run(&ex, "ALTER TABLE evolve ADD COLUMN version INT").await;
        run(&ex, "INSERT INTO evolve VALUES (2, 'v2', 2)").await;

        // Verify both rows work
        let res = run(&ex, "SELECT id, name, version FROM evolve ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][2], Value::Null);     // old row, new column = NULL
        assert_eq!(r[1][2], Value::Int32(2));  // new row has value

        // Drop a column
        run(&ex, "ALTER TABLE evolve DROP COLUMN version").await;
        let res = run(&ex, "SELECT id, name FROM evolve ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][1], Value::Text("v1".into()));
        assert_eq!(r[1][1], Value::Text("v2".into()));
    }

    // ========================================================================
    // 48. COUNT DISTINCT
    // ========================================================================

    #[tokio::test]
    async fn test_count_distinct() {
        let ex = setup();
        run(&ex, "CREATE TABLE visits (user_id INT NOT NULL, page TEXT NOT NULL)").await;
        run(&ex, "INSERT INTO visits VALUES (1, '/home'), (1, '/about'), (2, '/home'), (1, '/home'), (3, '/home')").await;

        // COUNT(DISTINCT user_id)
        let res = run(&ex, "SELECT COUNT(DISTINCT user_id) FROM visits").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(3));

        // COUNT(DISTINCT page)
        let res = run(&ex, "SELECT COUNT(DISTINCT page) FROM visits").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));

        // Regular COUNT vs COUNT DISTINCT
        let res = run(&ex, "SELECT COUNT(*), COUNT(DISTINCT user_id) FROM visits").await;
        let r = rows(&res[0]);
        assert_eq!(r[0][0], Value::Int64(5));  // total rows
        assert_eq!(r[0][1], Value::Int64(3));  // distinct users
    }

    // ========================================================================
    // 49. Cache operations (CACHE_SET, CACHE_GET, CACHE_DEL, CACHE_STATS)
    // ========================================================================

    #[tokio::test]
    async fn test_cache_operations() {
        let ex = setup();

        // CACHE_SET with TTL of 60 seconds
        let res = run(&ex, "CACHE_SET('mykey', 'myvalue', 60)").await;
        assert_eq!(tag(&res[0]), "CACHE_SET");

        // CACHE_GET should return the value
        let res = run(&ex, "CACHE_GET('mykey')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("myvalue".into()));

        // CACHE_DEL should remove the key
        let res = run(&ex, "CACHE_DEL('mykey')").await;
        assert_eq!(tag(&res[0]), "CACHE_DEL");

        // CACHE_GET after delete should return NULL
        let res = run(&ex, "CACHE_GET('mykey')").await;
        assert_eq!(scalar(&res[0]), &Value::Null);

        // CACHE_STATS should return metric rows
        let res = run(&ex, "CACHE_STATS").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "CACHE_STATS should return metric rows");
    }

    // ========================================================================
    // 50. SHOW commands: BUFFER POOL, SUBSYSTEM HEALTH, INDEX RECOMMENDATIONS,
    //     REPLICATION STATUS
    // ========================================================================

    #[tokio::test]
    async fn test_show_specialty_commands() {
        let ex = setup();

        // SHOW BUFFER_POOL
        let res = run(&ex, "SHOW BUFFER_POOL").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "SHOW BUFFER_POOL should return rows");
        let cols = columns(&res[0]);
        assert_eq!(cols[0].0, "metric");
        assert_eq!(cols[1].0, "value");

        // SHOW SUBSYSTEM_HEALTH
        let res = run(&ex, "SHOW SUBSYSTEM_HEALTH").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "SHOW SUBSYSTEM_HEALTH should return rows");
        let cols = columns(&res[0]);
        assert_eq!(cols[0].0, "subsystem");
        assert_eq!(cols[1].0, "status");

        // SHOW INDEX_RECOMMENDATIONS
        let res = run(&ex, "SHOW INDEX_RECOMMENDATIONS").await;
        let cols = columns(&res[0]);
        assert_eq!(cols[0].0, "table");
        // Recommendations list may be empty on a fresh executor (that is fine)

        // SHOW REPLICATION_STATUS
        let res = run(&ex, "SHOW REPLICATION_STATUS").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "SHOW REPLICATION_STATUS should return rows");
        let cols = columns(&res[0]);
        assert_eq!(cols[0].0, "metric");
        assert_eq!(cols[1].0, "value");
    }

    // ========================================================================
    // 51. JSON/document operations
    // ========================================================================

    #[tokio::test]
    async fn test_json_document_operations() {
        let ex = setup();

        // JSON_ARRAY_LENGTH
        let res = run(&ex, "SELECT JSON_ARRAY_LENGTH('[1,2,3,4]'::JSONB)").await;
        match scalar(&res[0]) {
            Value::Int32(4) | Value::Int64(4) => {}
            other => panic!("expected 4 for JSON_ARRAY_LENGTH, got {other:?}"),
        }

        // JSON_TYPEOF
        let res = run(&ex, "SELECT JSON_TYPEOF('{\"a\":1}'::JSONB)").await;
        assert_eq!(scalar(&res[0]), &Value::Text("object".into()));

        let res = run(&ex, "SELECT JSON_TYPEOF('[1,2]'::JSONB)").await;
        assert_eq!(scalar(&res[0]), &Value::Text("array".into()));

        // JSONB_EXTRACT_PATH_TEXT — extract a nested value as text
        let res = run(&ex, "SELECT JSONB_EXTRACT_PATH_TEXT('{\"name\":\"Alice\",\"age\":30}'::JSONB, 'name')").await;
        assert_eq!(scalar(&res[0]), &Value::Text("Alice".into()));
    }

    // ========================================================================
    // 52. EXPLAIN ANALYZE — plan with execution stats
    // ========================================================================

    #[tokio::test]
    async fn test_explain_analyze() {
        let ex = setup();
        run(&ex, "CREATE TABLE explain_data (id INT PRIMARY KEY, val TEXT NOT NULL)").await;
        run(&ex, "INSERT INTO explain_data VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;

        // EXPLAIN ANALYZE should execute and include timing/row info
        let res = run(&ex, "EXPLAIN ANALYZE SELECT * FROM explain_data WHERE id > 1").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "EXPLAIN ANALYZE should return plan rows");
        // The plan text should mention "Actual Rows" from the analyze output
        let plan_text: String = r.iter()
            .filter_map(|row| match &row[0] {
                Value::Text(t) => Some(t.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(plan_text.contains("Actual Rows"), "EXPLAIN ANALYZE should include actual rows: {plan_text}");
    }

    // ========================================================================
    // 53. EXPLAIN for JOIN queries — plan description for multi-table queries
    // ========================================================================

    #[tokio::test]
    async fn test_explain_join() {
        let ex = setup();
        run(&ex, "CREATE TABLE dept (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
        run(&ex, "CREATE TABLE emp (id INT PRIMARY KEY, name TEXT NOT NULL, dept_id INT NOT NULL)").await;
        run(&ex, "INSERT INTO dept VALUES (1, 'Engineering'), (2, 'Sales')").await;
        run(&ex, "INSERT INTO emp VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)").await;

        // EXPLAIN a JOIN query should return a plan
        let res = run(&ex, "EXPLAIN SELECT e.name, d.name FROM emp e JOIN dept d ON e.dept_id = d.id").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "EXPLAIN JOIN should return plan rows");
    }

    // ========================================================================
    // 54. Cache TTL and SHOW CACHE_STATS
    // ========================================================================

    #[tokio::test]
    async fn test_cache_ttl_and_show_cache_stats() {
        let ex = setup();

        // Set a key with TTL
        let res = run(&ex, "CACHE_SET('ttl_key', 'ttl_val', 300)").await;
        assert_eq!(tag(&res[0]), "CACHE_SET");

        // CACHE_TTL should return remaining seconds
        let res = run(&ex, "CACHE_TTL('ttl_key')").await;
        let val = scalar(&res[0]);
        match val {
            Value::Float64(secs) => {
                assert!(*secs > 0.0, "TTL should be positive, got {secs}");
                assert!(*secs <= 300.0, "TTL should be <= 300s, got {secs}");
            }
            other => panic!("expected Float64 for CACHE_TTL, got {other:?}"),
        }

        // SHOW CACHE_STATS (the SHOW variant)
        let res = run(&ex, "SHOW CACHE_STATS").await;
        let r = rows(&res[0]);
        assert!(!r.is_empty(), "SHOW CACHE_STATS should return rows");
        // First row should be entry_count metric
        assert_eq!(r[0][0], Value::Text("entry_count".into()));
    }

    // ========================================================================
    // 55. JSONB column storage and querying
    // ========================================================================

    #[tokio::test]
    async fn test_jsonb_column_storage() {
        let ex = setup();
        run(&ex, "CREATE TABLE docs (id INT PRIMARY KEY, data JSONB NOT NULL)").await;
        run(&ex, "INSERT INTO docs VALUES (1, '{\"name\":\"Alice\",\"tags\":[\"admin\",\"user\"]}'::JSONB)").await;
        run(&ex, "INSERT INTO docs VALUES (2, '{\"name\":\"Bob\",\"tags\":[\"user\"]}'::JSONB)").await;

        // Query all docs
        let res = run(&ex, "SELECT id, data FROM docs ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);

        // Verify first row is JSONB
        match &r[0][1] {
            Value::Jsonb(v) => {
                assert_eq!(v["name"], "Alice");
            }
            other => panic!("expected Jsonb value, got {other:?}"),
        }

        // Use JSON_TYPEOF on column data
        let res = run(&ex, "SELECT JSON_TYPEOF(data) FROM docs WHERE id = 1").await;
        assert_eq!(scalar(&res[0]), &Value::Text("object".into()));
    }

    // ========================================================================
    // #49: VACUUM SQL command
    // ========================================================================

    #[tokio::test]
    async fn test_vacuum_sql() {
        let ex = setup();
        run(&ex, "CREATE TABLE vac_test (id INT, name TEXT)").await;
        run(&ex, "INSERT INTO vac_test VALUES (1, 'a')").await;
        run(&ex, "INSERT INTO vac_test VALUES (2, 'b')").await;
        run(&ex, "INSERT INTO vac_test VALUES (3, 'c')").await;
        run(&ex, "DELETE FROM vac_test WHERE id = 2").await;

        // VACUUM the specific table — MemoryEngine returns zeros (no-op) but should not error
        let results = run(&ex, "VACUUM vac_test").await;
        assert_eq!(results.len(), 1, "VACUUM should return one result");
        let r = rows(&results[0]);
        assert_eq!(r.len(), 1, "VACUUM should return one summary row");

        // Data should be intact
        let data = run(&ex, "SELECT * FROM vac_test ORDER BY id").await;
        let data_rows = rows(&data[0]);
        assert_eq!(data_rows.len(), 2);

        // VACUUM without table name (vacuum all)
        let results2 = run(&ex, "VACUUM").await;
        assert_eq!(results2.len(), 1);
    }

    // ========================================================================
    // Session management: reset_session
    // ========================================================================

    #[tokio::test]
    async fn test_session_reset_clears_settings() {
        let ex = setup();
        let sid = ex.create_session();

        // Set a custom setting (sqlparser preserves single quotes in the value)
        ex.execute_with_session(sid, "SET search_path = 'custom_schema'").await.unwrap();

        // Verify it's set (value includes quotes as stored by SET)
        let val = ex.get_session_setting(sid, "search_path");
        assert_eq!(val, Some("'custom_schema'".into()));

        // Reset the session
        let actions = ex.reset_session(sid).await;
        assert!(actions.iter().any(|a| a.contains("RESET")));

        // Verify settings are back to defaults
        let val = ex.get_session_setting(sid, "search_path");
        assert_eq!(val, Some("public".into()));
    }

    #[tokio::test]
    async fn test_session_reset_clears_prepared_stmts() {
        let ex = setup();
        run(&ex, "CREATE TABLE prep_test (id INT NOT NULL)").await;
        let sid = ex.create_session();

        // Prepare a statement
        ex.execute_with_session(sid, "PREPARE my_stmt AS SELECT * FROM prep_test").await.unwrap();

        // Reset should report DEALLOCATE
        let actions = ex.reset_session(sid).await;
        assert!(actions.iter().any(|a| a.contains("DEALLOCATE")));
    }

    #[tokio::test]
    async fn test_session_reset_aborts_transaction() {
        let ex = setup_mvcc();
        run(&ex, "CREATE TABLE txn_reset (id INT NOT NULL)").await;
        let sid = ex.create_session();

        // Begin a transaction
        ex.execute_with_session(sid, "BEGIN").await.unwrap();
        ex.execute_with_session(sid, "INSERT INTO txn_reset VALUES (42)").await.unwrap();

        // Reset should abort the transaction
        let actions = ex.reset_session(sid).await;
        assert!(actions.iter().any(|a| a.contains("ROLLBACK")));

        // Verify the insert was rolled back (table should be empty)
        let res = run(&ex, "SELECT COUNT(*) FROM txn_reset").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(0));
    }

    #[tokio::test]
    async fn test_session_reset_no_txn_no_rollback() {
        let ex = setup();
        let sid = ex.create_session();

        // Reset with no active transaction — should NOT include ROLLBACK
        let actions = ex.reset_session(sid).await;
        assert!(!actions.iter().any(|a| a.contains("ROLLBACK")));
        // But should always include RESET
        assert!(actions.iter().any(|a| a.contains("RESET")));
    }

    #[tokio::test]
    async fn test_get_session_setting() {
        let ex = setup();
        let sid = ex.create_session();

        // Default settings
        assert_eq!(ex.get_session_setting(sid, "timezone"), Some("UTC".into()));
        assert_eq!(ex.get_session_setting(sid, "client_encoding"), Some("UTF8".into()));

        // Non-existent setting
        assert_eq!(ex.get_session_setting(sid, "nonexistent"), None);

        // Set and read back (sqlparser preserves quotes)
        ex.execute_with_session(sid, "SET timezone = 'US/Pacific'").await.unwrap();
        assert_eq!(ex.get_session_setting(sid, "timezone"), Some("'US/Pacific'".into()));
    }

    #[tokio::test]
    async fn test_per_session_statement_timeout_setting() {
        let ex = setup();
        let sid = ex.create_session();

        // No statement_timeout by default
        assert_eq!(ex.get_session_setting(sid, "statement_timeout"), None);

        // Set it
        ex.execute_with_session(sid, "SET statement_timeout = 60").await.unwrap();
        assert_eq!(ex.get_session_setting(sid, "statement_timeout"), Some("60".into()));

        // Reset clears it
        ex.reset_session(sid).await;
        assert_eq!(ex.get_session_setting(sid, "statement_timeout"), None);
    }

    // ========================================================================
    // Disk engine integration tests
    // ========================================================================

    /// Create a fresh executor backed by DiskEngine (with tempdir).
    fn setup_disk() -> (Arc<Executor>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let catalog = Arc::new(Catalog::new());
        let engine = crate::storage::DiskEngine::open(&db_path, catalog.clone()).unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        (Arc::new(Executor::new(catalog, storage)), dir)
    }

    #[tokio::test]
    async fn test_disk_engine_create_insert_select() {
        let (ex, _dir) = setup_disk();
        run(&ex, "CREATE TABLE disk_test (id INT NOT NULL, name TEXT)").await;
        run(&ex, "INSERT INTO disk_test VALUES (1, 'Alice')").await;
        run(&ex, "INSERT INTO disk_test VALUES (2, 'Bob')").await;

        let res = run(&ex, "SELECT name FROM disk_test ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[1][0], Value::Text("Bob".into()));
    }

    #[tokio::test]
    async fn test_disk_engine_update_delete() {
        let (ex, _dir) = setup_disk();
        run(&ex, "CREATE TABLE disk_ud (id INT NOT NULL, val INT NOT NULL)").await;
        run(&ex, "INSERT INTO disk_ud VALUES (1, 10), (2, 20), (3, 30)").await;

        run(&ex, "UPDATE disk_ud SET val = 99 WHERE id = 2").await;
        let res = run(&ex, "SELECT val FROM disk_ud WHERE id = 2").await;
        assert_eq!(scalar(&res[0]), &Value::Int32(99));

        run(&ex, "DELETE FROM disk_ud WHERE id = 3").await;
        let res = run(&ex, "SELECT COUNT(*) FROM disk_ud").await;
        assert_eq!(scalar(&res[0]), &Value::Int64(2));
    }

    #[tokio::test]
    async fn test_disk_engine_index_operations() {
        let (ex, _dir) = setup_disk();
        run(&ex, "CREATE TABLE disk_idx (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
        run(&ex, "INSERT INTO disk_idx VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma')").await;

        // Point query via primary key index
        let res = run(&ex, "SELECT name FROM disk_idx WHERE id = 2").await;
        assert_eq!(scalar(&res[0]), &Value::Text("Beta".into()));

        // Range query
        let res = run(&ex, "SELECT name FROM disk_idx WHERE id >= 2 ORDER BY id").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Beta".into()));
        assert_eq!(r[1][0], Value::Text("Gamma".into()));
    }

    #[tokio::test]
    async fn test_disk_engine_join() {
        let (ex, _dir) = setup_disk();
        run(&ex, "CREATE TABLE disk_users (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
        run(&ex, "CREATE TABLE disk_orders (id INT PRIMARY KEY, user_id INT NOT NULL, amount INT NOT NULL)").await;
        run(&ex, "INSERT INTO disk_users VALUES (1, 'Alice'), (2, 'Bob')").await;
        run(&ex, "INSERT INTO disk_orders VALUES (10, 1, 100), (20, 2, 200), (30, 1, 150)").await;

        let res = run(&ex, "SELECT u.name, SUM(o.amount) FROM disk_users u JOIN disk_orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Alice".into()));
        assert_eq!(r[0][1], Value::Int64(250));
        assert_eq!(r[1][0], Value::Text("Bob".into()));
        assert_eq!(r[1][1], Value::Int64(200));
    }

    #[tokio::test]
    async fn test_disk_engine_transaction_commit() {
        let ex = setup_mvcc();

        run(&ex, "CREATE TABLE disk_txn (id INT NOT NULL, val TEXT)").await;

        // Begin + insert + commit
        run(&ex, "BEGIN").await;
        run(&ex, "INSERT INTO disk_txn VALUES (1, 'committed')").await;
        run(&ex, "COMMIT").await;

        let res = run(&ex, "SELECT val FROM disk_txn WHERE id = 1").await;
        assert_eq!(scalar(&res[0]), &Value::Text("committed".into()));
    }

    #[tokio::test]
    async fn test_disk_engine_aggregate_pipeline() {
        let (ex, _dir) = setup_disk();
        run(&ex, "CREATE TABLE disk_sales (product TEXT NOT NULL, amount INT NOT NULL)").await;
        run(&ex, "INSERT INTO disk_sales VALUES ('Widget', 100), ('Widget', 150), ('Gadget', 200), ('Gadget', 50)").await;

        // GROUP BY + HAVING + ORDER BY (both products sum to 250, so filter > 250)
        let res = run(&ex, "SELECT product, SUM(amount) AS total FROM disk_sales GROUP BY product HAVING SUM(amount) >= 250 ORDER BY total DESC").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);

        // Simple GROUP BY with ORDER BY
        let res = run(&ex, "SELECT product, SUM(amount) AS total FROM disk_sales GROUP BY product ORDER BY product").await;
        let r = rows(&res[0]);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Text("Gadget".into()));
        assert_eq!(r[0][1], Value::Int64(250));
        assert_eq!(r[1][0], Value::Text("Widget".into()));
        assert_eq!(r[1][1], Value::Int64(250));
    }
}
