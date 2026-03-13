use super::*;

// ======================================================================
// Transaction / DDL tests
// ======================================================================

#[tokio::test]
async fn test_transaction_statements() {
    let ex = test_executor();
    let results = exec(&ex, "BEGIN").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "BEGIN"),
        _ => panic!("expected Command"),
    }

    let results = exec(&ex, "COMMIT").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "COMMIT"),
        _ => panic!("expected Command"),
    }

    let results = exec(&ex, "ROLLBACK").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "ROLLBACK"),
        _ => panic!("expected Command"),
    }
}

#[tokio::test]
async fn test_set_and_show() {
    let ex = test_executor();
    let results = exec(&ex, "SET client_encoding = 'UTF8'").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "SET"),
        _ => panic!("expected Command"),
    }

    let results = exec(&ex, "SHOW server_version").await;
    let val = scalar(&results[0]);
    match val {
        Value::Text(s) => assert!(s.contains("Nucleus")),
        _ => panic!("expected Text"),
    }
}

#[tokio::test]
async fn test_create_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE users (id INT, name TEXT)").await;
    let results = exec(&ex, "CREATE INDEX idx_users_id ON users (id)").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE INDEX"),
        _ => panic!("expected Command"),
    }
}

#[tokio::test]
async fn test_create_hash_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE products (id INT, name TEXT, price INT)").await;
    exec(&ex, "INSERT INTO products VALUES (1, 'Widget', 100)").await;
    exec(&ex, "INSERT INTO products VALUES (2, 'Gadget', 200)").await;
    exec(&ex, "INSERT INTO products VALUES (3, 'Doohickey', 100)").await;

    // Create a hash index
    let results = exec(&ex, "CREATE INDEX idx_products_price ON products USING hash (price)").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE INDEX"),
        _ => panic!("expected Command"),
    }

    // Verify hash_indexes was populated
    assert!(ex.hash_indexes.read().contains_key(&("products".to_string(), "price".to_string())));

    // Equality lookup on the hash-indexed column should still work
    let results = exec(&ex, "SELECT name FROM products WHERE price = 100").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_truncate() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;
    exec(&ex, "INSERT INTO t VALUES (2)").await;

    let results = exec(&ex, "SELECT * FROM t").await;
    assert_eq!(rows(&results[0]).len(), 2);

    exec(&ex, "TRUNCATE TABLE t").await;

    let results = exec(&ex, "SELECT * FROM t").await;
    assert_eq!(rows(&results[0]).len(), 0);
}


// View tests
// ======================================================================

#[tokio::test]
async fn test_create_and_query_view() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE vt (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO vt VALUES (1, 'Alice')").await;
    exec(&ex, "INSERT INTO vt VALUES (2, 'Bob')").await;

    exec(&ex, "CREATE VIEW active_users AS SELECT id, name FROM vt WHERE id > 0").await;
    let results = exec(&ex, "SELECT name FROM active_users").await;
    assert_eq!(rows(&results[0]).len(), 2);
}

#[tokio::test]
async fn test_drop_view() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE vt2 (id INT)").await;
    exec(&ex, "CREATE VIEW v2 AS SELECT id FROM vt2").await;
    let results = exec(&ex, "DROP VIEW v2").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "DROP VIEW"),
        _ => panic!("expected Command"),
    }
}

// ======================================================================

// Sequence tests
// ======================================================================

#[tokio::test]
async fn test_create_sequence_and_nextval() {
    let ex = test_executor();
    exec(&ex, "CREATE SEQUENCE my_seq INCREMENT BY 1 START WITH 1").await;

    let results = exec(&ex, "SELECT NEXTVAL('my_seq')").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(1));

    let results = exec(&ex, "SELECT NEXTVAL('my_seq')").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(2));

    let results = exec(&ex, "SELECT CURRVAL('my_seq')").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(2));
}


// ALTER TABLE tests
// ======================================================================

#[tokio::test]
async fn test_alter_table_add_column() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t1 (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t1 VALUES (1, 'alice')").await;
    exec(&ex, "ALTER TABLE t1 ADD COLUMN age INT").await;
    let results = exec(&ex, "SELECT * FROM t1").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].len(), 3);
    assert_eq!(r[0][2], Value::Null); // new column defaults to NULL
}

#[tokio::test]
async fn test_alter_table_drop_column() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t2 (id INT, name TEXT, age INT)").await;
    exec(&ex, "INSERT INTO t2 VALUES (1, 'bob', 30)").await;
    exec(&ex, "ALTER TABLE t2 DROP COLUMN age").await;
    let results = exec(&ex, "SELECT * FROM t2").await;
    let r = rows(&results[0]);
    assert_eq!(r[0].len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("bob".into()));
}

#[tokio::test]
async fn test_alter_table_rename_column() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t3 (id INT, name TEXT)").await;
    exec(&ex, "ALTER TABLE t3 RENAME COLUMN name TO full_name").await;
    let results = exec(&ex, "SELECT full_name FROM t3").await;
    match &results[0] {
        ExecResult::Select { columns, .. } => {
            assert_eq!(columns[0].0, "full_name");
        }
        _ => panic!("expected SELECT"),
    }
}

#[tokio::test]
async fn test_alter_table_rename_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE old_name (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO old_name VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO old_name VALUES (2, 'bob')").await;
    exec(&ex, "ALTER TABLE old_name RENAME TO new_name").await;

    // Verify data is accessible under the new name
    let results = exec(&ex, "SELECT * FROM new_name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("alice".into()));
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[1][1], Value::Text("bob".into()));

    // Verify old name no longer works
    let err = ex.execute("SELECT * FROM old_name").await;
    assert!(err.is_err(), "old table name should no longer exist");
}

#[tokio::test]
async fn test_alter_column_set_default() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t_def (id INT, status TEXT)").await;
    exec(&ex, "ALTER TABLE t_def ALTER COLUMN status SET DEFAULT 'active'").await;

    // Insert a row without specifying status — should get the default
    exec(&ex, "INSERT INTO t_def (id) VALUES (1)").await;
    let results = exec(&ex, "SELECT status FROM t_def WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("active".into()));
}

#[tokio::test]
async fn test_alter_column_drop_default() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t_ddef (id INT, status TEXT DEFAULT 'pending')").await;

    // Drop the default
    exec(&ex, "ALTER TABLE t_ddef ALTER COLUMN status DROP DEFAULT").await;

    // Insert without specifying status — should be NULL now
    exec(&ex, "INSERT INTO t_ddef (id) VALUES (1)").await;
    let results = exec(&ex, "SELECT status FROM t_ddef WHERE id = 1").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Null);
}

#[tokio::test]
async fn test_alter_column_set_not_null() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t_nn (id INT, name TEXT)").await;
    exec(&ex, "ALTER TABLE t_nn ALTER COLUMN name SET NOT NULL").await;

    // Inserting NULL into a NOT NULL column should fail
    let err = ex.execute("INSERT INTO t_nn VALUES (1, NULL)").await;
    assert!(err.is_err(), "inserting NULL into NOT NULL column should fail");
}

#[tokio::test]
async fn test_alter_column_drop_not_null() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t_dnn (id INT, name TEXT NOT NULL)").await;

    // Verify NOT NULL is enforced initially
    let err = ex.execute("INSERT INTO t_dnn VALUES (1, NULL)").await;
    assert!(err.is_err(), "NULL should be rejected before DROP NOT NULL");

    // Drop the NOT NULL constraint
    exec(&ex, "ALTER TABLE t_dnn ALTER COLUMN name DROP NOT NULL").await;

    // Now inserting NULL should succeed
    exec(&ex, "INSERT INTO t_dnn VALUES (2, NULL)").await;
    let results = exec(&ex, "SELECT name FROM t_dnn WHERE id = 2").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Null);
}

#[tokio::test]
async fn test_alter_table_drop_column_if_exists() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE t_dce (id INT, name TEXT)").await;

    // DROP COLUMN IF EXISTS with a nonexistent column should not error
    exec(&ex, "ALTER TABLE t_dce DROP COLUMN IF EXISTS nonexistent").await;

    // DROP COLUMN without IF EXISTS on nonexistent column should error
    let err = ex.execute("ALTER TABLE t_dce DROP COLUMN nonexistent").await;
    assert!(err.is_err(), "DROP COLUMN on nonexistent should fail without IF EXISTS");
}


// ANALYZE tests
// ========================================================================

#[tokio::test]
async fn test_analyze_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE analyze_test (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO analyze_test VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO analyze_test VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO analyze_test VALUES (3, 'charlie')").await;

    let results = exec(&ex, "ANALYZE analyze_test").await;
    // ANALYZE now returns a Command result with tag "ANALYZE"
    match &results[0] {
        ExecResult::Command { tag, rows_affected } => {
            assert_eq!(tag, "ANALYZE");
            assert_eq!(*rows_affected, 3);
        }
        _ => panic!("expected Command result from ANALYZE"),
    }
}

#[tokio::test]
async fn test_analyze_collects_stats() {
    // Create a table, insert data, run ANALYZE, verify per-column stats
    let ex = test_executor();
    exec(&ex, "CREATE TABLE astats (id INT, name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO astats VALUES (1, 'alice', 90)").await;
    exec(&ex, "INSERT INTO astats VALUES (2, 'bob', 80)").await;
    exec(&ex, "INSERT INTO astats VALUES (3, 'alice', 70)").await;
    exec(&ex, "INSERT INTO astats VALUES (4, NULL, 95)").await;

    exec(&ex, "ANALYZE astats").await;

    // Use SHOW TABLE STATS to verify
    let results = exec(&ex, "SHOW TABLE STATS astats").await;
    let r = rows(&results[0]);
    // 3 columns: id, name, score
    assert_eq!(r.len(), 3);

    // Check column names in order
    assert_eq!(r[0][0], Value::Text("id".into()));
    assert_eq!(r[1][0], Value::Text("name".into()));
    assert_eq!(r[2][0], Value::Text("score".into()));

    // id: 4 distinct, 0 nulls, min=1, max=4
    assert_eq!(r[0][1], Value::Int64(4)); // distinct_count
    assert_eq!(r[0][2], Value::Int64(0)); // null_count
    assert_eq!(r[0][3], Value::Text("1".into())); // min
    assert_eq!(r[0][4], Value::Text("4".into())); // max

    // name: 2 distinct (alice, bob), 1 null
    assert_eq!(r[1][1], Value::Int64(2)); // distinct_count (alice, bob)
    assert_eq!(r[1][2], Value::Int64(1)); // null_count

    // score: 4 distinct, 0 nulls, min=70, max=95
    assert_eq!(r[2][1], Value::Int64(4)); // distinct_count
    assert_eq!(r[2][2], Value::Int64(0)); // null_count
    assert_eq!(r[2][3], Value::Text("70".into())); // min
    assert_eq!(r[2][4], Value::Text("95".into())); // max
}

#[tokio::test]
async fn test_show_table_stats_returns_correct_data() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sts (x INT, y TEXT)").await;
    exec(&ex, "INSERT INTO sts VALUES (10, 'hello')").await;
    exec(&ex, "INSERT INTO sts VALUES (20, 'world')").await;
    exec(&ex, "INSERT INTO sts VALUES (30, 'hello')").await;

    exec(&ex, "ANALYZE sts").await;

    let results = exec(&ex, "SHOW TABLE STATS sts").await;
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            // Check column schema
            assert_eq!(columns.len(), 5);
            assert_eq!(columns[0].0, "column_name");
            assert_eq!(columns[1].0, "distinct_count");
            assert_eq!(columns[2].0, "null_count");
            assert_eq!(columns[3].0, "min_value");
            assert_eq!(columns[4].0, "max_value");

            // 2 columns: x and y
            assert_eq!(rows.len(), 2);

            // x: 3 distinct, 0 nulls, min=10, max=30
            assert_eq!(rows[0][0], Value::Text("x".into()));
            assert_eq!(rows[0][1], Value::Int64(3));
            assert_eq!(rows[0][2], Value::Int64(0));
            assert_eq!(rows[0][3], Value::Text("10".into()));
            assert_eq!(rows[0][4], Value::Text("30".into()));

            // y: 2 distinct (hello, world), 0 nulls
            assert_eq!(rows[1][0], Value::Text("y".into()));
            assert_eq!(rows[1][1], Value::Int64(2));
            assert_eq!(rows[1][2], Value::Int64(0));
        }
        _ => panic!("expected Select result"),
    }
}

#[tokio::test]
async fn test_analyze_empty_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE empty_stats (id INT, name TEXT)").await;

    let results = exec(&ex, "ANALYZE empty_stats").await;
    match &results[0] {
        ExecResult::Command { tag, rows_affected } => {
            assert_eq!(tag, "ANALYZE");
            assert_eq!(*rows_affected, 0);
        }
        _ => panic!("expected Command result from ANALYZE"),
    }

    // SHOW TABLE STATS should work on empty table
    let results = exec(&ex, "SHOW TABLE STATS empty_stats").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2); // id and name columns
    // All stats should show 0/1 distinct (max(0,1)), 0 nulls, NULL min/max
    assert_eq!(r[0][0], Value::Text("id".into()));
    assert_eq!(r[0][1], Value::Int64(1)); // distinct_count is max(0, 1) = 1
    assert_eq!(r[0][2], Value::Int64(0)); // null_count
    assert_eq!(r[0][3], Value::Null);     // min_value (no data)
    assert_eq!(r[0][4], Value::Null);     // max_value (no data)
}

#[tokio::test]
async fn test_analyze_updates_after_more_inserts() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE upd_stats (val INT)").await;
    exec(&ex, "INSERT INTO upd_stats VALUES (1)").await;
    exec(&ex, "INSERT INTO upd_stats VALUES (2)").await;

    // First ANALYZE
    let results = exec(&ex, "ANALYZE upd_stats").await;
    match &results[0] {
        ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 2),
        _ => panic!("expected Command"),
    }

    let results = exec(&ex, "SHOW TABLE STATS upd_stats").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int64(2)); // 2 distinct
    assert_eq!(r[0][3], Value::Text("1".into())); // min
    assert_eq!(r[0][4], Value::Text("2".into())); // max

    // Insert more data and re-analyze
    exec(&ex, "INSERT INTO upd_stats VALUES (3)").await;
    exec(&ex, "INSERT INTO upd_stats VALUES (4)").await;
    exec(&ex, "INSERT INTO upd_stats VALUES (5)").await;

    let results = exec(&ex, "ANALYZE upd_stats").await;
    match &results[0] {
        ExecResult::Command { rows_affected, .. } => assert_eq!(*rows_affected, 5),
        _ => panic!("expected Command"),
    }

    let results = exec(&ex, "SHOW TABLE STATS upd_stats").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Int64(5)); // 5 distinct now
    assert_eq!(r[0][3], Value::Text("1".into())); // min still 1
    assert_eq!(r[0][4], Value::Text("5".into())); // max now 5
}

#[tokio::test]
async fn test_show_table_stats_without_analyze_errors() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE no_analyze (id INT)").await;

    // SHOW TABLE STATS should error when ANALYZE hasn't been run
    let result = ex.execute("SHOW TABLE STATS no_analyze").await;
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("no statistics available"));
}

#[tokio::test]
async fn test_analyze_no_table_name() {
    // ANALYZE without a table name should return a Command result
    let ex = test_executor();
    let results = exec(&ex, "ANALYZE").await;
    match &results[0] {
        ExecResult::Command { tag, rows_affected } => {
            assert_eq!(tag, "ANALYZE");
            assert_eq!(*rows_affected, 0);
        }
        _ => panic!("expected Command result"),
    }
}

// ========================================================================

// User-defined function tests
// ========================================================================

#[tokio::test]
async fn test_create_and_call_function() {
    let ex = test_executor();
    exec(&ex, "CREATE FUNCTION double_it(x INT) RETURNS INT LANGUAGE SQL AS $$ SELECT $1 * 2 $$").await;

    // Call the UDF
    let results = exec(&ex, "SELECT double_it(21)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(42));
}

#[tokio::test]
async fn test_create_and_drop_function() {
    let ex = test_executor();
    exec(&ex, "CREATE FUNCTION my_func() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$").await;

    let results = exec(&ex, "SELECT my_func()").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(1));

    exec(&ex, "DROP FUNCTION my_func").await;

    // Should fail now
    let err = ex.execute("SELECT my_func()").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_drop_function_if_exists() {
    let ex = test_executor();
    // Should not error when function doesn't exist
    exec(&ex, "DROP FUNCTION IF EXISTS nonexistent_func").await;
}

#[tokio::test]
async fn test_udf_with_named_params() {
    let ex = test_executor();
    exec(&ex, "CREATE FUNCTION add_nums(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT $1 + $2 $$").await;

    let results = exec(&ex, "SELECT add_nums(10, 32)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(42));
}

#[tokio::test]
async fn test_udf_with_table_data() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE scores (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO scores VALUES ('alice', 95)").await;
    exec(&ex, "INSERT INTO scores VALUES ('bob', 87)").await;

    exec(&ex, "CREATE FUNCTION passing_grade(threshold INT) RETURNS INT LANGUAGE SQL AS $$ SELECT COUNT(*) FROM scores WHERE score >= $1 $$").await;

    let results = exec(&ex, "SELECT passing_grade(90)").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1));
}

// ========================================================================

// DROP INDEX tests
// ======================================================================

#[tokio::test]
async fn test_drop_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE di_test (id INT, name TEXT)").await;
    exec(&ex, "CREATE INDEX di_idx ON di_test (name)").await;

    // Verify index exists via pg_catalog
    let results = exec(&ex, "SELECT indexname FROM pg_indexes").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);

    // Drop the index
    exec(&ex, "DROP INDEX di_idx").await;

    // Verify it's gone
    let results = exec(&ex, "SELECT indexname FROM pg_indexes").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0);
}

#[tokio::test]
async fn test_drop_index_if_exists() {
    let ex = test_executor();
    // Should not error when index doesn't exist
    exec(&ex, "DROP INDEX IF EXISTS nonexistent_idx").await;
}

// ======================================================================

// Materialized views
// ======================================================================

#[tokio::test]
async fn test_materialized_view() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE mv_data (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO mv_data VALUES (1, 'hello')").await;
    exec(&ex, "INSERT INTO mv_data VALUES (2, 'world')").await;

    exec(&ex, "CREATE MATERIALIZED VIEW mv_test AS SELECT id, val FROM mv_data").await;

    let results = exec(&ex, "SELECT id, val FROM mv_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][1], Value::Text("hello".into()));
}

// ======================================================================

// CREATE SCHEMA
// ======================================================================

#[tokio::test]
async fn test_create_schema() {
    let ex = test_executor();
    let results = exec(&ex, "CREATE SCHEMA my_schema").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE SCHEMA"),
        _ => panic!("expected Command result"),
    }
}

// ======================================================================

// IF NOT EXISTS / OR REPLACE tests
// ======================================================================

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ine_tbl (id INT, name TEXT)").await;
    // This should succeed without error since IF NOT EXISTS is specified
    exec(&ex, "CREATE TABLE IF NOT EXISTS ine_tbl (id INT, name TEXT)").await;
    // Verify table still has original structure
    exec(&ex, "INSERT INTO ine_tbl VALUES (1, 'alice')").await;
    let results = exec(&ex, "SELECT id, name FROM ine_tbl").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
}

#[tokio::test]
async fn test_create_index_if_not_exists() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE idx_tbl (id INT, val TEXT)").await;
    exec(&ex, "CREATE INDEX idx_val ON idx_tbl(val)").await;
    // This should succeed without error since IF NOT EXISTS is specified
    exec(&ex, "CREATE INDEX IF NOT EXISTS idx_val ON idx_tbl(val)").await;
}

#[tokio::test]
async fn test_alter_table_add_column_if_not_exists() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE altcol_tbl (id INT, name TEXT)").await;
    exec(&ex, "ALTER TABLE altcol_tbl ADD COLUMN age INT").await;
    // This should succeed without error since IF NOT EXISTS is specified
    exec(&ex, "ALTER TABLE altcol_tbl ADD COLUMN IF NOT EXISTS age INT").await;
    // Verify only one age column exists
    exec(&ex, "INSERT INTO altcol_tbl VALUES (1, 'alice', 30)").await;
    let results = exec(&ex, "SELECT id, name, age FROM altcol_tbl").await;
    let r = rows(&results[0]);
    assert_eq!(r[0].len(), 3); // Should have exactly 3 columns
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE droptbl (id INT)").await;
    exec(&ex, "DROP TABLE IF EXISTS droptbl").await;
    // This should succeed without error even though table doesn't exist
    exec(&ex, "DROP TABLE IF EXISTS droptbl").await;
    exec(&ex, "DROP TABLE IF EXISTS nonexistent_table").await;
}

#[tokio::test]
async fn test_create_or_replace_view() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE orv_tbl (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO orv_tbl VALUES (1, 'a'), (2, 'b')").await;
    exec(&ex, "CREATE VIEW orv AS SELECT id FROM orv_tbl").await;
    // Replace the view with a different query
    exec(&ex, "CREATE OR REPLACE VIEW orv AS SELECT id, val FROM orv_tbl WHERE id > 1").await;
    let results = exec(&ex, "SELECT * FROM orv").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1); // Only one row with id > 1
    assert_eq!(r[0][0], Value::Int32(2));
    assert_eq!(r[0][1], Value::Text("b".into()));
}

// ==========================================================================

// ======================================================================
// SERIAL / BIGSERIAL / SMALLSERIAL / GENERATED AS IDENTITY tests
// ======================================================================

#[tokio::test]
async fn test_serial_column_creates_sequence() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE serial_test (id SERIAL, name TEXT)").await;
    // Insert without specifying id — it should be auto-filled via nextval.
    exec(&ex, "INSERT INTO serial_test (name) VALUES ('alice')").await;
    let results = exec(&ex, "SELECT id FROM serial_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    // id should be 1 (first nextval)
    assert_eq!(r[0][0], Value::Int32(1), "first SERIAL id should be 1, got {:?}", r[0][0]);
}

#[tokio::test]
async fn test_bigserial_column() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE bigserial_test (id BIGSERIAL PRIMARY KEY, val TEXT)").await;
    exec(&ex, "INSERT INTO bigserial_test (val) VALUES ('row1')").await;
    let results = exec(&ex, "SELECT id FROM bigserial_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    // BIGSERIAL returns Int64
    assert_eq!(r[0][0], Value::Int64(1), "first BIGSERIAL id should be Int64(1), got {:?}", r[0][0]);
}

#[tokio::test]
async fn test_serial_multiple_inserts() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE serial_multi (id SERIAL, val TEXT)").await;
    exec(&ex, "INSERT INTO serial_multi (val) VALUES ('a')").await;
    exec(&ex, "INSERT INTO serial_multi (val) VALUES ('b')").await;
    exec(&ex, "INSERT INTO serial_multi (val) VALUES ('c')").await;
    let results = exec(&ex, "SELECT id FROM serial_multi ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Int32(1), "first id should be 1");
    assert_eq!(r[1][0], Value::Int32(2), "second id should be 2");
    assert_eq!(r[2][0], Value::Int32(3), "third id should be 3");
}

#[tokio::test]
async fn test_smallserial_column() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE smallserial_test (id SMALLSERIAL, label TEXT)").await;
    exec(&ex, "INSERT INTO smallserial_test (label) VALUES ('x')").await;
    exec(&ex, "INSERT INTO smallserial_test (label) VALUES ('y')").await;
    let results = exec(&ex, "SELECT id FROM smallserial_test ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(2));
}

#[tokio::test]
async fn test_identity_column_generated_always() {
    let ex = test_executor();
    // INT GENERATED ALWAYS AS IDENTITY: column is INT (Int32), sequence auto-increments.
    exec(&ex, "CREATE TABLE identity_test (id INT GENERATED ALWAYS AS IDENTITY, val TEXT)").await;
    exec(&ex, "INSERT INTO identity_test (val) VALUES ('hello')").await;
    exec(&ex, "INSERT INTO identity_test (val) VALUES ('world')").await;
    let results = exec(&ex, "SELECT id FROM identity_test ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    // GENERATED ALWAYS AS IDENTITY on INT column: coerced to Int32 (column type)
    assert_eq!(r[0][0], Value::Int32(1), "first identity id should be 1, got {:?}", r[0][0]);
    assert_eq!(r[1][0], Value::Int32(2), "second identity id should be 2, got {:?}", r[1][0]);
}

#[tokio::test]
async fn test_json_agg_basic() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE jtest (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO jtest VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
    let results = exec(&ex, "SELECT JSON_AGG(val) FROM jtest").await;
    // Should return a single row with a JSON array
    if let crate::executor::ExecResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows.len(), 1);
        // Should be a Jsonb value containing an array
        match &rows[0][0] {
            crate::types::Value::Jsonb(v) => assert!(v.is_array()),
            other => panic!("expected Jsonb, got {:?}", other),
        }
    }
}

#[tokio::test]
async fn test_json_agg_preserves_types() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE jtest2 (n INT)").await;
    exec(&ex, "INSERT INTO jtest2 VALUES (1), (2), (3)").await;
    let results = exec(&ex, "SELECT JSON_AGG(n) FROM jtest2").await;
    if let crate::executor::ExecResult::Select { rows, .. } = &results[0] {
        if let crate::types::Value::Jsonb(serde_json::Value::Array(arr)) = &rows[0][0] {
            assert_eq!(arr.len(), 3);
        } else { panic!("expected JSON array"); }
    }
}

#[tokio::test]
async fn test_create_type_enum_basic() {
    let ex = test_executor();
    // Create enum type
    exec(&ex, "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')").await;

    // Use in table
    exec(&ex, "CREATE TABLE person (name TEXT, feeling mood)").await;
    exec(&ex, "INSERT INTO person VALUES ('Alice', 'happy')").await;
    exec(&ex, "INSERT INTO person VALUES ('Bob', 'sad')").await;

    let r = exec(&ex, "SELECT name, feeling FROM person ORDER BY name").await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::Text("happy".into()));
        assert_eq!(rows[1][1], Value::Text("sad".into()));
    } else { panic!("expected select"); }
}

#[tokio::test]
async fn test_create_type_enum_invalid_value() {
    let ex = test_executor();
    exec(&ex, "CREATE TYPE status AS ENUM ('active', 'inactive')").await;
    exec(&ex, "CREATE TABLE items (name TEXT, state status)").await;

    // Valid insert succeeds
    let r = exec(&ex, "INSERT INTO items VALUES ('x', 'active')").await;
    assert!(matches!(r[0], ExecResult::Command { .. }));

    // Invalid value should return an error
    let r = ex.execute("INSERT INTO items VALUES ('y', 'unknown')").await;
    assert!(r.is_err(), "expected enum constraint violation for invalid value");
}

#[tokio::test]
async fn test_drop_type_enum() {
    let ex = test_executor();
    exec(&ex, "CREATE TYPE color AS ENUM ('red', 'green', 'blue')").await;
    exec(&ex, "DROP TYPE color").await;
    // Re-create should succeed
    exec(&ex, "CREATE TYPE color AS ENUM ('cyan', 'magenta')").await;
}

#[tokio::test]
async fn test_nulls_first_last_order_by() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ntest (val INT)").await;
    exec(&ex, "INSERT INTO ntest VALUES (3), (NULL), (1), (NULL), (2)").await;

    // NULLS LAST (default for ASC): NULLs at end
    let r = exec(&ex, "SELECT val FROM ntest ORDER BY val ASC NULLS LAST").await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
        assert_eq!(vals[0], Value::Int32(1));
        assert_eq!(vals[1], Value::Int32(2));
        assert_eq!(vals[2], Value::Int32(3));
        assert_eq!(vals[3], Value::Null);
        assert_eq!(vals[4], Value::Null);
    } else { panic!("expected select"); }

    // NULLS FIRST (explicit): NULLs at start
    let r = exec(&ex, "SELECT val FROM ntest ORDER BY val ASC NULLS FIRST").await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
        assert_eq!(vals[0], Value::Null);
        assert_eq!(vals[1], Value::Null);
        assert_eq!(vals[2], Value::Int32(1));
    } else { panic!("expected select"); }

    // DESC NULLS LAST (non-default): NULLs at end
    let r = exec(&ex, "SELECT val FROM ntest ORDER BY val DESC NULLS LAST").await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        let vals: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
        assert_eq!(vals[0], Value::Int32(3));
        assert_eq!(vals[3], Value::Null);
        assert_eq!(vals[4], Value::Null);
    } else { panic!("expected select"); }
}

#[tokio::test]
async fn test_json_path_operators() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE jptest (data TEXT)").await;
    exec(&ex, r#"INSERT INTO jptest VALUES ('{"a":{"b":42}}')"#).await;

    // #> returns JSONB
    let r = exec(&ex, r#"SELECT data::jsonb #> '{a,b}' FROM jptest"#).await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        assert_eq!(rows[0][0], Value::Jsonb(serde_json::json!(42)));
    } else { panic!("expected select"); }

    // #>> returns Text
    let r = exec(&ex, r#"SELECT data::jsonb #>> '{a,b}' FROM jptest"#).await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        assert_eq!(rows[0][0], Value::Text("42".to_string()));
    } else { panic!("expected select"); }

    // Missing path returns NULL
    let r = exec(&ex, r#"SELECT data::jsonb #> '{a,z}' FROM jptest"#).await;
    if let ExecResult::Select { rows, .. } = &r[0] {
        assert_eq!(rows[0][0], Value::Null);
    } else { panic!("expected select"); }
}

// ======================================================================
