use super::*;

// GRANT / REVOKE tests
// ======================================================================

#[tokio::test]
async fn test_grant_revoke() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE grant_t (id INT)").await;
    let results = exec(&ex, "GRANT SELECT, INSERT ON grant_t TO testuser").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "GRANT"),
        _ => panic!("expected command"),
    }
    let results = exec(&ex, "REVOKE INSERT ON grant_t FROM testuser").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "REVOKE"),
        _ => panic!("expected command"),
    }
}

#[tokio::test]
async fn test_create_role() {
    let ex = test_executor();
    let results = exec(&ex, "CREATE ROLE app_user LOGIN PASSWORD 'secret'").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE ROLE"),
        _ => panic!("expected command"),
    }
}

// ======================================================================

// Cursor tests
// ======================================================================

#[tokio::test]
async fn test_declare_fetch_close_cursor() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cur_t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO cur_t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO cur_t VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO cur_t VALUES (3, 'charlie')").await;
    exec(&ex, "DECLARE my_cursor CURSOR FOR SELECT * FROM cur_t").await;

    // Fetch 2 rows
    let results = exec(&ex, "FETCH 2 FROM my_cursor").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);

    // Fetch remaining
    let results = exec(&ex, "FETCH ALL FROM my_cursor").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);

    // Close
    let results = exec(&ex, "CLOSE my_cursor").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "CLOSE"),
        _ => panic!("expected command"),
    }
}

// ======================================================================

// LISTEN / NOTIFY tests
// ======================================================================

#[tokio::test]
async fn test_listen_notify() {
    let ex = test_executor();
    let results = exec(&ex, "LISTEN my_channel").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "LISTEN"),
        _ => panic!("expected command"),
    }
    let results = exec(&ex, "NOTIFY my_channel, 'hello world'").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "NOTIFY"),
        _ => panic!("expected command"),
    }
}

// ======================================================================

// SET/SHOW integration test
// ======================================================================

#[tokio::test]
async fn test_set_show_roundtrip() {
    let ex = test_executor();
    exec(&ex, "SET my_var = 'hello'").await;
    let results = exec(&ex, "SHOW my_var").await;
    // SET stores the value as-is from sqlparser (includes quotes)
    let val = scalar(&results[0]);
    match val {
        Value::Text(s) => assert!(s.contains("hello")),
        _ => panic!("expected text"),
    }
}

// ======================================================================

// PREPARE / EXECUTE tests
// ========================================================================

#[tokio::test]
async fn test_prepare_execute() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE prep_test (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO prep_test VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO prep_test VALUES (2, 'bob')").await;

    exec(&ex, "PREPARE find_user AS SELECT name FROM prep_test WHERE id = $1").await;
    let results = exec(&ex, "EXECUTE find_user(1)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("alice".into()));
}

#[tokio::test]
async fn test_prepare_execute_insert() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE prep_ins (id INT, val TEXT)").await;
    exec(&ex, "PREPARE ins AS INSERT INTO prep_ins VALUES ($1, $2)").await;
    exec(&ex, "EXECUTE ins(1, 'hello')").await;

    let results = exec(&ex, "SELECT * FROM prep_ins").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
}

#[tokio::test]
async fn test_deallocate() {
    let ex = test_executor();
    exec(&ex, "PREPARE test_stmt AS SELECT 1").await;
    exec(&ex, "DEALLOCATE test_stmt").await;

    // Should fail
    let err = ex.execute("EXECUTE test_stmt()").await;
    assert!(err.is_err());
}

// ========================================================================

// TRUNCATE tests
// ========================================================================

#[tokio::test]
async fn test_truncate_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE trunc_test (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO trunc_test VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO trunc_test VALUES (2, 'bob')").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));

    exec(&ex, "TRUNCATE TABLE trunc_test").await;

    let results = exec(&ex, "SELECT COUNT(*) FROM trunc_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(0));
}

// ======================================================================

// PostgreSQL system function tests
// ======================================================================

#[tokio::test]
async fn test_pg_backend_pid() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_backend_pid()").await;
    match scalar(&results[0]) {
        Value::Int32(pid) => assert!(*pid > 0, "pid should be positive"),
        other => panic!("expected Int32, got {other:?}"),
    }
}

#[tokio::test]
async fn test_txid_current() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT txid_current()").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(1));
}

#[tokio::test]
async fn test_obj_description() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT obj_description(12345, 'pg_class')").await;
    assert_eq!(scalar(&results[0]), &Value::Null);
}

#[tokio::test]
async fn test_col_description() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT col_description(12345, 1)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);
}

#[tokio::test]
async fn test_format_type() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT format_type(23, -1)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("integer".into()));

    let results = exec(&ex, "SELECT format_type(25, -1)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("text".into()));

    let results = exec(&ex, "SELECT format_type(16, -1)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("boolean".into()));

    let results = exec(&ex, "SELECT format_type(701, -1)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("double precision".into()));

    let results = exec(&ex, "SELECT format_type(99999, -1)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("unknown".into()));
}

#[tokio::test]
async fn test_pg_get_expr() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_get_expr('some_expression', 0)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("some_expression".into()));
}

#[tokio::test]
async fn test_pg_table_is_visible() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_table_is_visible(12345)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));
}

#[tokio::test]
async fn test_has_table_privilege() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT has_table_privilege('nucleus', 'pg_class', 'SELECT')").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));
}

#[tokio::test]
async fn test_has_schema_privilege() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT has_schema_privilege('nucleus', 'public', 'USAGE')").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));
}

#[tokio::test]
async fn test_pg_encoding_to_char() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_encoding_to_char(6)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("UTF8".into()));
}

#[tokio::test]
async fn test_pg_postmaster_start_time() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_postmaster_start_time()").await;
    match scalar(&results[0]) {
        Value::Text(ts) => assert!(ts.contains('-'), "should be a timestamp string"),
        other => panic!("expected Text timestamp, got {other:?}"),
    }
}

#[tokio::test]
async fn test_quote_ident() {
    let ex = test_executor();
    // Simple identifier that doesn't need quoting
    let results = exec(&ex, "SELECT quote_ident('simple')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("simple".into()));

    // Identifier with spaces needs quoting
    let results = exec(&ex, "SELECT quote_ident('has space')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("\"has space\"".into()));

    // Identifier with uppercase needs quoting
    let results = exec(&ex, "SELECT quote_ident('MyTable')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("\"MyTable\"".into()));
}

#[tokio::test]
async fn test_pg_get_userbyid() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_get_userbyid(10)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("nucleus".into()));
}

#[tokio::test]
async fn test_pg_get_constraintdef() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_get_constraintdef(12345)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);
}

#[tokio::test]
async fn test_pg_get_indexdef() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT pg_get_indexdef(12345)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);
}

#[tokio::test]
async fn test_current_schema_fn() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT current_schema()").await;
    assert_eq!(scalar(&results[0]), &Value::Text("public".into()));
}

// ========================================================================

// Virtual table / system catalog tests (extended)
// ========================================================================

#[tokio::test]
async fn test_information_schema_tables_ordered() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE users (id INT, name TEXT)").await;
    exec(&ex, "CREATE TABLE orders (id INT, total FLOAT)").await;

    let results = exec(&ex, "SELECT table_name FROM information_schema.tables ORDER BY table_name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("orders".into()));
    assert_eq!(r[1][0], Value::Text("users".into()));
}

#[tokio::test]
async fn test_information_schema_tables_all_columns() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (id INT)").await;

    let results = exec(&ex, "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("nucleus".into()));
    assert_eq!(r[0][1], Value::Text("public".into()));
    assert_eq!(r[0][2], Value::Text("items".into()));
    assert_eq!(r[0][3], Value::Text("BASE TABLE".into()));
}

#[tokio::test]
async fn test_information_schema_columns_udt_name() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE products (id INT NOT NULL, name TEXT, price FLOAT)").await;

    let results = exec(&ex, "SELECT column_name, ordinal_position, is_nullable, data_type, udt_name FROM information_schema.columns WHERE table_name = 'products' ORDER BY ordinal_position").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    // id column
    assert_eq!(r[0][0], Value::Text("id".into()));
    assert_eq!(r[0][1], Value::Int32(1));
    assert_eq!(r[0][2], Value::Text("NO".into()));
    assert_eq!(r[0][3], Value::Text("INTEGER".into()));
    assert_eq!(r[0][4], Value::Text("int4".into()));
    // name column
    assert_eq!(r[1][0], Value::Text("name".into()));
    assert_eq!(r[1][1], Value::Int32(2));
    assert_eq!(r[1][2], Value::Text("YES".into()));
    assert_eq!(r[1][3], Value::Text("TEXT".into()));
    assert_eq!(r[1][4], Value::Text("text".into()));
}

#[tokio::test]
async fn test_pg_tables_full_columns() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE abc (id INT)").await;

    let results = exec(&ex, "SELECT schemaname, tablename, tableowner FROM pg_tables").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("public".into()));
    assert_eq!(r[0][1], Value::Text("abc".into()));
    assert_eq!(r[0][2], Value::Text("nucleus".into()));
}

#[tokio::test]
async fn test_pg_catalog_pg_tables() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE xyz (val TEXT)").await;

    let results = exec(&ex, "SELECT tablename FROM pg_catalog.pg_tables").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("xyz".into()));
}

#[tokio::test]
async fn test_pg_type() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE typed (a INT, b TEXT, c BOOLEAN)").await;

    let results = exec(&ex, "SELECT typname, typcategory FROM pg_catalog.pg_type WHERE typname = 'int4'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("int4".into()));
    assert_eq!(r[0][1], Value::Text("N".into()));
}

#[tokio::test]
async fn test_pg_type_includes_base_types() {
    let ex = test_executor();
    // No tables created, but base types should still be present
    let results = exec(&ex, "SELECT typname FROM pg_type WHERE typname = 'varchar'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("varchar".into()));
}

#[tokio::test]
async fn test_pg_class() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cls_test (id INT, name TEXT)").await;

    let results = exec(&ex, "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("cls_test".into()));
    assert_eq!(r[0][1], Value::Text("r".into()));
}

#[tokio::test]
async fn test_pg_class_with_indexes() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE idx_cls (id INT, name TEXT)").await;
    exec(&ex, "CREATE INDEX idx_cls_name ON idx_cls (name)").await;

    let results = exec(&ex, "SELECT relname, relkind FROM pg_catalog.pg_class ORDER BY relname").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    // Should have the table and the index
    let kinds: Vec<&Value> = r.iter().map(|row| &row[1]).collect();
    assert!(kinds.contains(&&Value::Text("r".into())));
    assert!(kinds.contains(&&Value::Text("i".into())));
}

#[tokio::test]
async fn test_pg_namespace() {
    let ex = test_executor();

    let results = exec(&ex, "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY oid").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Text("pg_catalog".into()));
    assert_eq!(r[1][0], Value::Text("public".into()));
    assert_eq!(r[2][0], Value::Text("information_schema".into()));
}

#[tokio::test]
async fn test_pg_attribute() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE attr_test (id INT NOT NULL, name TEXT, active BOOLEAN NOT NULL)").await;

    let results = exec(&ex, "SELECT attname, attnum, attnotnull FROM pg_catalog.pg_attribute ORDER BY attnum").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Text("id".into()));
    assert_eq!(r[0][1], Value::Int32(1));
    assert_eq!(r[0][2], Value::Bool(true));
    assert_eq!(r[1][0], Value::Text("name".into()));
    assert_eq!(r[1][1], Value::Int32(2));
    assert_eq!(r[1][2], Value::Bool(false));
    assert_eq!(r[2][0], Value::Text("active".into()));
    assert_eq!(r[2][1], Value::Int32(3));
    assert_eq!(r[2][2], Value::Bool(true));
}

#[tokio::test]
async fn test_pg_index() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE idx_test (id INT, email TEXT, name TEXT)").await;
    exec(&ex, "CREATE UNIQUE INDEX idx_email ON idx_test (email)").await;

    let results = exec(&ex, "SELECT indisunique, indisprimary, indkey FROM pg_catalog.pg_index").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Bool(true));  // unique
    assert_eq!(r[0][1], Value::Bool(false)); // not primary
    assert_eq!(r[0][2], Value::Text("2".into())); // email is column 2
}

#[tokio::test]
async fn test_pg_database() {
    let ex = test_executor();

    let results = exec(&ex, "SELECT oid, datname, datcollate FROM pg_catalog.pg_database").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("nucleus".into()));
    assert_eq!(r[0][2], Value::Text("en_US.UTF-8".into()));
}

#[tokio::test]
async fn test_pg_settings() {
    let ex = test_executor();

    let results = exec(&ex, "SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'timezone'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("timezone".into()));
    assert_eq!(r[0][1], Value::Text("UTC".into()));
}

#[tokio::test]
async fn test_pg_settings_all_defaults() {
    let ex = test_executor();

    let results = exec(&ex, "SELECT name FROM pg_settings ORDER BY name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);
    assert_eq!(r[0][0], Value::Text("client_encoding".into()));
    assert_eq!(r[1][0], Value::Text("plan_execution".into()));
    assert_eq!(r[2][0], Value::Text("search_path".into()));
    assert_eq!(r[3][0], Value::Text("standard_conforming_strings".into()));
    assert_eq!(r[4][0], Value::Text("timezone".into()));
}

#[tokio::test]
async fn test_information_schema_schemata() {
    let ex = test_executor();

    let results = exec(&ex, "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Text("information_schema".into()));
    assert_eq!(r[1][0], Value::Text("pg_catalog".into()));
    assert_eq!(r[2][0], Value::Text("public".into()));
}

#[tokio::test]
async fn test_virtual_table_with_alias() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE aliased (id INT)").await;

    let results = exec(&ex, "SELECT t.table_name FROM information_schema.tables AS t").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("aliased".into()));
}

#[tokio::test]
async fn test_virtual_table_empty_catalog() {
    let ex = test_executor();

    // No tables created - should return empty
    let results = exec(&ex, "SELECT table_name FROM information_schema.tables").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0);
}

#[tokio::test]
async fn test_pg_attribute_type_oid() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE type_oid_test (flag BOOLEAN, label TEXT, count BIGINT)").await;

    let results = exec(&ex, "SELECT attname, atttypid FROM pg_attribute ORDER BY attnum").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 3);
    // bool OID = 16, text OID = 25, int8 OID = 20
    assert_eq!(r[0][1], Value::Int32(16));
    assert_eq!(r[1][1], Value::Int32(25));
    assert_eq!(r[2][1], Value::Int32(20));
}

// ======================================================================

// CALL statement
// ======================================================================

#[tokio::test]
async fn test_call_procedure() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE call_test (id INT, name TEXT)").await;

    // Create a function that inserts and returns
    exec(&ex, "CREATE FUNCTION insert_user(p_id INT, p_name TEXT) RETURNS VOID LANGUAGE sql AS $$ INSERT INTO call_test VALUES ($1, $2) $$").await;

    exec(&ex, "CALL insert_user(1, 'Alice')").await;

    let results = exec(&ex, "SELECT * FROM call_test").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("Alice".into()));
}

// ======================================================================

// VACUUM (no-op but should not error)
// ======================================================================

#[tokio::test]
async fn test_vacuum() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE vac_test (id INT)").await;
    // Should not error
    exec(&ex, "VACUUM").await;
}

// ======================================================================

// Privilege checking tests
// ======================================================================

#[tokio::test]
async fn test_privilege_checking_insert() {
    let ex = test_executor();

    // Create a table
    exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;

    // Create a non-superuser role
    exec(&ex, "CREATE ROLE testuser").await;

    // Grant only SELECT privilege (not INSERT)
    exec(&ex, "GRANT SELECT ON priv_test TO testuser").await;

    // Set session authorization to testuser
    exec(&ex, "SET session_authorization = 'testuser'").await;

    // Attempt to INSERT should fail due to lack of INSERT privilege
    let result = ex.execute("INSERT INTO priv_test VALUES (1, 'test')").await;
    assert!(result.is_err(), "INSERT should fail without INSERT privilege");

    // Check the error is PermissionDenied
    match result {
        Err(error) => {
            assert!(
                matches!(error, ExecError::PermissionDenied(_)),
                "Expected PermissionDenied error, got: {:?}",
                error
            );
        }
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[tokio::test]
async fn test_privilege_checking_update() {
    let ex = test_executor();

    // Create a table with data
    exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;

    // Create a non-superuser role with only SELECT
    exec(&ex, "CREATE ROLE testuser").await;
    exec(&ex, "GRANT SELECT ON priv_test TO testuser").await;

    // Set session authorization to testuser
    exec(&ex, "SET session_authorization = 'testuser'").await;

    // Attempt to UPDATE should fail
    let result = ex.execute("UPDATE priv_test SET name = 'updated'").await;
    assert!(result.is_err(), "UPDATE should fail without UPDATE privilege");

    match result {
        Err(error) => {
            assert!(
                matches!(error, ExecError::PermissionDenied(_)),
                "Expected PermissionDenied error, got: {:?}",
                error
            );
        }
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[tokio::test]
async fn test_privilege_checking_delete() {
    let ex = test_executor();

    // Create a table with data
    exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;

    // Create a non-superuser role with only SELECT
    exec(&ex, "CREATE ROLE testuser").await;
    exec(&ex, "GRANT SELECT ON priv_test TO testuser").await;

    // Set session authorization to testuser
    exec(&ex, "SET session_authorization = 'testuser'").await;

    // Attempt to DELETE should fail
    let result = ex.execute("DELETE FROM priv_test WHERE id = 1").await;
    assert!(result.is_err(), "DELETE should fail without DELETE privilege");

    match result {
        Err(error) => {
            assert!(
                matches!(error, ExecError::PermissionDenied(_)),
                "Expected PermissionDenied error, got: {:?}",
                error
            );
        }
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[tokio::test]
async fn test_privilege_checking_superuser() {
    let ex = test_executor();

    // Create a table
    exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;

    // Create a superuser role
    exec(&ex, "CREATE ROLE superuser WITH SUPERUSER").await;

    // Set session authorization to superuser
    exec(&ex, "SET session_authorization = 'superuser'").await;

    // Superuser should be able to do everything without explicit grants
    exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;
    exec(&ex, "UPDATE priv_test SET name = 'updated'").await;
    exec(&ex, "DELETE FROM priv_test WHERE id = 1").await;
}

#[tokio::test]
async fn test_privilege_checking_all_privilege() {
    let ex = test_executor();

    // Create a table
    exec(&ex, "CREATE TABLE priv_test (id INT, name TEXT)").await;

    // Create a role and grant ALL privileges
    exec(&ex, "CREATE ROLE testuser").await;
    exec(&ex, "GRANT ALL ON priv_test TO testuser").await;

    // Set session authorization to testuser
    exec(&ex, "SET session_authorization = 'testuser'").await;

    // Should be able to do all operations with ALL privilege
    exec(&ex, "INSERT INTO priv_test VALUES (1, 'test')").await;
    exec(&ex, "UPDATE priv_test SET name = 'updated'").await;
    exec(&ex, "DELETE FROM priv_test WHERE id = 1").await;
}

// ======================================================================

// pg_stat and query cache tests

// ====================================================================
// pg_stat_* views
// ====================================================================

#[tokio::test]
async fn test_pg_stat_activity() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT datname, state FROM pg_stat_activity").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("nucleus".into()));
    assert_eq!(r[0][1], Value::Text("active".into()));
}

#[tokio::test]
async fn test_pg_stat_user_tables() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE stats_test (id INT, name TEXT)").await;
    let results = exec(&ex, "SELECT relname FROM pg_stat_user_tables").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("stats_test".into()));
}

#[tokio::test]
async fn test_pg_stat_user_indexes() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE idx_stat_test (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "CREATE INDEX idx_name ON idx_stat_test (name)").await;
    let results = exec(&ex, "SELECT relname, indexrelname FROM pg_stat_user_indexes").await;
    let r = rows(&results[0]);
    assert!(r.len() >= 1, "should have at least one index");
}

#[tokio::test]
async fn test_pg_stat_database() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT datname, numbackends FROM pg_stat_database").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("nucleus".into()));
}

// ====================================================================
// Query Result Cache tests
// ====================================================================

#[tokio::test]
async fn test_query_cache_put_get() {
    let ex = test_executor();
    // Put a result into the cache
    let columns = vec![
        ("id".to_string(), DataType::Int32),
        ("name".to_string(), DataType::Text),
    ];
    let rows_data = vec![
        vec![Value::Int32(1), Value::Text("alice".into())],
        vec![Value::Int32(2), Value::Text("bob".into())],
    ];
    ex.query_cache_put("SELECT * FROM users", &columns, &rows_data);

    // Should hit
    let cached = ex.query_cache_get("SELECT * FROM users");
    assert!(cached.is_some());
    if let Some(ExecResult::Select { columns: c, rows: r }) = cached {
        assert_eq!(c.len(), 2);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Int32(1));
        assert_eq!(r[1][1], Value::Text("bob".into()));
    }

    // Case-insensitive key: same SQL lowercased should hit
    let cached2 = ex.query_cache_get("select * from users");
    assert!(cached2.is_some());
}

#[tokio::test]
async fn test_query_cache_invalidate() {
    let ex = test_executor();
    let columns = vec![("x".to_string(), DataType::Int32)];
    let rows_data = vec![vec![Value::Int32(42)]];
    ex.query_cache_put("SELECT x FROM t", &columns, &rows_data);
    assert!(ex.query_cache_get("SELECT x FROM t").is_some());

    ex.query_cache_invalidate_all();
    assert!(ex.query_cache_get("SELECT x FROM t").is_none());
    assert_eq!(ex.query_cache_len(), 0);
}

#[tokio::test]
async fn test_query_cache_miss() {
    let ex = test_executor();
    assert!(ex.query_cache_get("SELECT * FROM nonexistent").is_none());
}

#[test]
fn test_follower_read_no_manager() {
    // Without a follower_read_mgr, all reads should be allowed (standalone mode)
    let ex = test_executor();
    assert!(ex.check_follower_read_eligibility().is_ok());
}

#[test]
fn test_follower_read_fresh_data() {
    let ex = test_executor();
    // Create a FollowerReadManager with recent timestamp
    let mut mgr = crate::distributed::FollowerReadManager::new(1);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    mgr.advance_timestamp(crate::distributed::HybridTimestamp {
        physical_ms: now_ms,
        logical: 0,
        node_id: 0,
    });
    let ex = Executor {
        follower_read_mgr: Some(Arc::new(parking_lot::RwLock::new(mgr))),
        ..ex
    };
    // No cluster configured → not a follower → should be ok
    assert!(ex.check_follower_read_eligibility().is_ok());
}

#[test]
fn test_follower_read_stale_data() {
    let ex = test_executor();
    // Create a stale FollowerReadManager (timestamp is 10 seconds old, threshold is 5s)
    let mut mgr = crate::distributed::FollowerReadManager::new(1);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    mgr.advance_timestamp(crate::distributed::HybridTimestamp {
        physical_ms: now_ms.saturating_sub(10_000), // 10 seconds ago
        logical: 0,
        node_id: 0,
    });
    mgr.max_staleness_ms = 5_000; // 5 second threshold
    // Set up as follower via multi-raft cluster coordinator (has raft_manager, is NOT leader)
    let cluster = Arc::new(parking_lot::RwLock::new(
        crate::distributed::ClusterCoordinator::new_multi_raft(0x2, vec![(0x1, "127.0.0.1:5432".into())]),
    ));
    let ex = Executor {
        follower_read_mgr: Some(Arc::new(parking_lot::RwLock::new(mgr))),
        cluster: Some(cluster),
        ..ex
    };
    // This node is not the leader → should reject stale reads
    let result = ex.check_follower_read_eligibility();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("stale"));
}
