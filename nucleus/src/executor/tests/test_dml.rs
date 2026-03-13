use super::*;

// ======================================================================
// Constraint tests
// ======================================================================

#[tokio::test]
async fn test_not_null_constraint() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nn (id INT NOT NULL, name TEXT)").await;
    let result = ex.execute("INSERT INTO nn VALUES (NULL, 'test')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not-null"));
}

#[tokio::test]
async fn test_primary_key_constraint() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pk (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO pk VALUES (1, 'alice')").await;
    let result = ex.execute("INSERT INTO pk VALUES (1, 'bob')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("duplicate key") || err_msg.contains("unique"));
}

#[tokio::test]
async fn test_unique_constraint() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE uq (id INT, email TEXT UNIQUE)").await;
    exec(&ex, "INSERT INTO uq VALUES (1, 'a@b.com')").await;
    let result = ex.execute("INSERT INTO uq VALUES (2, 'a@b.com')").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_unique_allows_multiple_nulls() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE un (id INT, email TEXT UNIQUE)").await;
    exec(&ex, "INSERT INTO un VALUES (1, NULL)").await;
    exec(&ex, "INSERT INTO un VALUES (2, NULL)").await; // should succeed
    let results = exec(&ex, "SELECT COUNT(*) FROM un").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));
}

#[tokio::test]
async fn test_add_column_with_default() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE df (id INT)").await;
    exec(&ex, "INSERT INTO df VALUES (1)").await;
    exec(&ex, "ALTER TABLE df ADD COLUMN status TEXT DEFAULT 'active'").await;
    let results = exec(&ex, "SELECT status FROM df WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Text("active".into()));
}

// ======================================================================

// information_schema tests
// ======================================================================

#[tokio::test]
async fn test_information_schema_tables() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE alpha (id INT)").await;
    exec(&ex, "CREATE TABLE beta (name TEXT)").await;
    let results = exec(&ex, "SELECT table_name FROM information_schema.tables").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    let names: Vec<String> = r.iter().map(|row| row[0].to_string()).collect();
    assert!(names.contains(&"alpha".to_string()));
    assert!(names.contains(&"beta".to_string()));
}

#[tokio::test]
async fn test_information_schema_columns() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ic (id INT NOT NULL, name TEXT)").await;
    let results = exec(&ex, "SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_name = 'ic'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_pg_tables() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pgt (id INT)").await;
    let results = exec(&ex, "SELECT tablename FROM pg_tables").await;
    let r = rows(&results[0]);
    assert!(r.iter().any(|row| row[0] == Value::Text("pgt".into())));
}

// ======================================================================

// ON CONFLICT (upsert) tests
// ======================================================================

#[tokio::test]
async fn test_on_conflict_do_nothing() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE upsert1 (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO upsert1 VALUES (1, 'alice')").await;
    // This would conflict on id=1 but should be silently skipped
    exec(&ex, "INSERT INTO upsert1 VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING").await;
    let results = exec(&ex, "SELECT name FROM upsert1 WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Text("alice".into()));
}

#[tokio::test]
async fn test_on_conflict_do_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE upsert2 (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO upsert2 VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO upsert2 VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob'").await;
    let results = exec(&ex, "SELECT name FROM upsert2 WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Text("bob".into()));
}

// ======================================================================

// RETURNING clause tests
// ======================================================================

#[tokio::test]
async fn test_insert_returning() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ret1 (id INT, name TEXT)").await;
    let results = exec(&ex, "INSERT INTO ret1 VALUES (1, 'alice') RETURNING *").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("alice".into()));
}

#[tokio::test]
async fn test_delete_returning() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ret2 (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO ret2 VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO ret2 VALUES (2, 'bob')").await;
    let results = exec(&ex, "DELETE FROM ret2 WHERE id = 1 RETURNING name").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("alice".into()));
}

#[tokio::test]
async fn test_update_returning() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ret3 (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO ret3 VALUES (1, 'alice')").await;
    let results = exec(&ex, "UPDATE ret3 SET name = 'bob' WHERE id = 1 RETURNING *").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("bob".into()));
}

// ======================================================================

// Comprehensive constraint enforcement tests
// ======================================================================

#[tokio::test]
async fn test_pk_constraint_prevents_duplicate() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pk_test (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO pk_test VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO pk_test VALUES (2, 'bob')").await;

    // Duplicate PK should fail
    let result = ex.execute("INSERT INTO pk_test VALUES (1, 'charlie')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("duplicate key") || err_msg.contains("unique constraint"));

    // Original rows should be untouched
    let results = exec(&ex, "SELECT COUNT(*) FROM pk_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));
}

#[tokio::test]
async fn test_unique_constraint_prevents_duplicate() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE uq_test (id INT, email TEXT UNIQUE, name TEXT)").await;
    exec(&ex, "INSERT INTO uq_test VALUES (1, 'alice@test.com', 'Alice')").await;
    exec(&ex, "INSERT INTO uq_test VALUES (2, 'bob@test.com', 'Bob')").await;

    // Duplicate unique column should fail
    let result = ex.execute("INSERT INTO uq_test VALUES (3, 'alice@test.com', 'Charlie')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("duplicate key") || err_msg.contains("unique constraint"));

    // NULL values in unique columns should be allowed (multiple NULLs OK)
    exec(&ex, "INSERT INTO uq_test VALUES (3, NULL, 'Charlie')").await;
    exec(&ex, "INSERT INTO uq_test VALUES (4, NULL, 'Diana')").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM uq_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(4));
}

#[tokio::test]
async fn test_not_null_constraint_enforced() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nn_test (id INT NOT NULL, name TEXT NOT NULL, bio TEXT)").await;

    // NULL in NOT NULL column should fail
    let result = ex.execute("INSERT INTO nn_test VALUES (NULL, 'alice', 'hi')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not-null"));
    assert!(err_msg.contains("id"));

    // NULL in second NOT NULL column should also fail
    let result = ex.execute("INSERT INTO nn_test VALUES (1, NULL, 'hi')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not-null"));
    assert!(err_msg.contains("name"));

    // NULL in nullable column should succeed
    exec(&ex, "INSERT INTO nn_test VALUES (1, 'alice', NULL)").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM nn_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1));
}

#[tokio::test]
async fn test_check_constraint() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ck_test (id INT, age INT CHECK (age >= 0), name TEXT)").await;

    // Value violating CHECK should fail
    let result = ex.execute("INSERT INTO ck_test VALUES (1, -5, 'alice')").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("check constraint"));

    // Value satisfying CHECK should succeed
    exec(&ex, "INSERT INTO ck_test VALUES (1, 25, 'alice')").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM ck_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(1));

    // Boundary value (age = 0) should succeed
    exec(&ex, "INSERT INTO ck_test VALUES (2, 0, 'bob')").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM ck_test").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(2));
}

#[tokio::test]
async fn test_fk_constraint() {
    let ex = test_executor();
    // Create parent table
    exec(&ex, "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO departments VALUES (1, 'Engineering')").await;
    exec(&ex, "INSERT INTO departments VALUES (2, 'Sales')").await;

    // Create child table with FK
    exec(&ex, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id))").await;

    // Insert with valid FK should succeed
    exec(&ex, "INSERT INTO employees VALUES (1, 'Alice', 1)").await;
    exec(&ex, "INSERT INTO employees VALUES (2, 'Bob', 2)").await;

    // Insert with invalid FK should fail
    let result = ex.execute("INSERT INTO employees VALUES (3, 'Charlie', 999)").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("foreign key"));

    // Insert with NULL FK should succeed (NULLs bypass FK checks)
    exec(&ex, "INSERT INTO employees VALUES (3, 'Charlie', NULL)").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM employees").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(3));
}

#[tokio::test]
async fn test_constraint_on_update() {
    let ex = test_executor();

    // -- NOT NULL on UPDATE --
    exec(&ex, "CREATE TABLE upd_nn (id INT NOT NULL, name TEXT NOT NULL)").await;
    exec(&ex, "INSERT INTO upd_nn VALUES (1, 'alice')").await;
    let result = ex.execute("UPDATE upd_nn SET name = NULL WHERE id = 1").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not-null"));

    // -- UNIQUE on UPDATE --
    exec(&ex, "CREATE TABLE upd_uq (id INT PRIMARY KEY, email TEXT UNIQUE)").await;
    exec(&ex, "INSERT INTO upd_uq VALUES (1, 'a@b.com')").await;
    exec(&ex, "INSERT INTO upd_uq VALUES (2, 'c@d.com')").await;
    // Updating to a duplicate unique value should fail
    let result = ex.execute("UPDATE upd_uq SET email = 'a@b.com' WHERE id = 2").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("duplicate key") || err_msg.contains("unique constraint"));
    // Updating to the same value (self) should succeed
    exec(&ex, "UPDATE upd_uq SET email = 'a@b.com' WHERE id = 1").await;

    // -- CHECK on UPDATE --
    exec(&ex, "CREATE TABLE upd_ck (id INT, val INT CHECK (val > 0))").await;
    exec(&ex, "INSERT INTO upd_ck VALUES (1, 10)").await;
    let result = ex.execute("UPDATE upd_ck SET val = -1 WHERE id = 1").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("check constraint"));

    // -- FK on UPDATE --
    exec(&ex, "CREATE TABLE upd_parent (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO upd_parent VALUES (1, 'dept1')").await;
    exec(&ex, "CREATE TABLE upd_child (id INT, parent_id INT REFERENCES upd_parent(id))").await;
    exec(&ex, "INSERT INTO upd_child VALUES (1, 1)").await;
    let result = ex.execute("UPDATE upd_child SET parent_id = 999 WHERE id = 1").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("foreign key"));
}

// ======================================================================

// INSERT ... SELECT tests
// ======================================================================

#[tokio::test]
async fn test_insert_select_from_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE source_tbl (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO source_tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')").await;
    exec(&ex, "CREATE TABLE target_tbl (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO target_tbl SELECT * FROM source_tbl WHERE id > 1").await;
    let results = exec(&ex, "SELECT id, name FROM target_tbl ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(2));
    assert_eq!(r[0][1], Value::Text("bob".into()));
    assert_eq!(r[1][0], Value::Int32(3));
    assert_eq!(r[1][1], Value::Text("charlie".into()));
}

// ======================================================================

// INSERT with DEFAULT values tests
// ======================================================================

#[tokio::test]
async fn test_insert_default_value() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE def_tbl (id INT PRIMARY KEY, name TEXT DEFAULT 'unnamed', score INT DEFAULT 0)").await;
    exec(&ex, "INSERT INTO def_tbl VALUES (1, DEFAULT, DEFAULT)").await;
    let results = exec(&ex, "SELECT id, name, score FROM def_tbl").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("unnamed".into()));
    assert_eq!(r[0][2], Value::Int32(0));
}

#[tokio::test]
async fn test_insert_partial_default() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE def_tbl2 (id INT PRIMARY KEY, name TEXT DEFAULT 'anon', active BOOLEAN DEFAULT true)").await;
    // Specify only id and name, active should get its default
    exec(&ex, "INSERT INTO def_tbl2 (id, name) VALUES (1, 'alice')").await;
    let results = exec(&ex, "SELECT id, name, active FROM def_tbl2").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("alice".into()));
    assert_eq!(r[0][2], Value::Bool(true));
}

#[tokio::test]
async fn test_insert_mixed_default_and_literal() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE def_tbl3 (id INT, val TEXT DEFAULT 'x', num INT DEFAULT 42)").await;
    exec(&ex, "INSERT INTO def_tbl3 VALUES (1, 'custom', DEFAULT)").await;
    let results = exec(&ex, "SELECT id, val, num FROM def_tbl3").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("custom".into()));
    assert_eq!(r[0][2], Value::Int32(42));
}

// ======================================================================

// ON CONFLICT with EXCLUDED pseudo-table tests
// ======================================================================

#[tokio::test]
async fn test_on_conflict_excluded() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE excl_tbl (id INT PRIMARY KEY, name TEXT, count INT)").await;
    exec(&ex, "INSERT INTO excl_tbl VALUES (1, 'alice', 10)").await;
    // Upsert: use EXCLUDED to reference the values from the conflicting INSERT
    exec(&ex, "INSERT INTO excl_tbl VALUES (1, 'bob', 5) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, count = EXCLUDED.count").await;
    let results = exec(&ex, "SELECT id, name, count FROM excl_tbl").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("bob".into()));
    assert_eq!(r[0][2], Value::Int32(5));
}

#[tokio::test]
async fn test_on_conflict_excluded_expr() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE excl_tbl2 (id INT PRIMARY KEY, counter INT)").await;
    exec(&ex, "INSERT INTO excl_tbl2 VALUES (1, 10)").await;
    // Upsert: add EXCLUDED.counter to existing counter
    exec(&ex, "INSERT INTO excl_tbl2 VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET counter = excl_tbl2.counter + EXCLUDED.counter").await;
    let results = exec(&ex, "SELECT id, counter FROM excl_tbl2").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Int32(15));
}

// ======================================================================

// ======================================================================
// Foreign Key ON DELETE / ON UPDATE enforcement tests
// ======================================================================

#[tokio::test]
async fn test_fk_restrict_prevents_parent_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE fk_parent (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO fk_parent VALUES (1, 'Engineering')").await;
    exec(&ex, "CREATE TABLE fk_child (id INT, parent_id INT REFERENCES fk_parent(id) ON DELETE RESTRICT)").await;
    exec(&ex, "INSERT INTO fk_child VALUES (1, 1)").await;

    let result = ex.execute("DELETE FROM fk_parent WHERE id = 1").await;
    assert!(result.is_err(), "DELETE should fail with RESTRICT when children exist");
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("foreign key"), "error should mention foreign key: {err_msg}");
}

#[tokio::test]
async fn test_fk_restrict_allows_delete_no_children() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE fk_p2 (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO fk_p2 VALUES (1, 'A')").await;
    exec(&ex, "INSERT INTO fk_p2 VALUES (2, 'B')").await;
    exec(&ex, "CREATE TABLE fk_c2 (id INT, pid INT REFERENCES fk_p2(id) ON DELETE RESTRICT)").await;
    exec(&ex, "INSERT INTO fk_c2 VALUES (1, 1)").await;

    // Delete parent id=2 which has no children — should succeed
    exec(&ex, "DELETE FROM fk_p2 WHERE id = 2").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM fk_p2").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(1));
}

#[tokio::test]
async fn test_fk_cascade_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cascade_parent (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO cascade_parent VALUES (1, 'A'), (2, 'B')").await;
    exec(&ex, "CREATE TABLE cascade_child (id INT, pid INT REFERENCES cascade_parent(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO cascade_child VALUES (10, 1), (20, 1), (30, 2)").await;

    // Delete parent id=1 — should cascade-delete children with pid=1
    exec(&ex, "DELETE FROM cascade_parent WHERE id = 1").await;

    let r = exec(&ex, "SELECT COUNT(*) FROM cascade_child").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(1), "only child with pid=2 should remain");
    let r = exec(&ex, "SELECT id FROM cascade_child").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(30));
}

#[tokio::test]
async fn test_fk_cascade_delete_multi_level() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE gp (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO gp VALUES (1)").await;
    exec(&ex, "CREATE TABLE par (id INT PRIMARY KEY, gp_id INT REFERENCES gp(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO par VALUES (10, 1)").await;
    exec(&ex, "CREATE TABLE ch (id INT, par_id INT REFERENCES par(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO ch VALUES (100, 10)").await;

    // Delete grandparent — should cascade through parent to child
    exec(&ex, "DELETE FROM gp WHERE id = 1").await;

    let r = exec(&ex, "SELECT COUNT(*) FROM par").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(0), "parent should be cascade-deleted");
    let r = exec(&ex, "SELECT COUNT(*) FROM ch").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(0), "child should be cascade-deleted");
}

#[tokio::test]
async fn test_fk_set_null_on_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sn_parent (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO sn_parent VALUES (1, 'X')").await;
    exec(&ex, "CREATE TABLE sn_child (id INT, pid INT REFERENCES sn_parent(id) ON DELETE SET NULL)").await;
    exec(&ex, "INSERT INTO sn_child VALUES (10, 1)").await;

    exec(&ex, "DELETE FROM sn_parent WHERE id = 1").await;

    let r = exec(&ex, "SELECT pid FROM sn_child WHERE id = 10").await;
    assert_eq!(rows(&r[0])[0][0], Value::Null, "child FK column should be NULL after SET NULL");
}

#[tokio::test]
async fn test_fk_set_default_on_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sd_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO sd_parent VALUES (1), (0)").await;
    exec(&ex, "CREATE TABLE sd_child (id INT, pid INT DEFAULT 0 REFERENCES sd_parent(id) ON DELETE SET DEFAULT)").await;
    exec(&ex, "INSERT INTO sd_child VALUES (10, 1)").await;

    exec(&ex, "DELETE FROM sd_parent WHERE id = 1").await;

    let r = exec(&ex, "SELECT pid FROM sd_child WHERE id = 10").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(0), "child FK should be reset to default (0)");
}

#[tokio::test]
async fn test_fk_no_action_default() {
    let ex = test_executor();
    // No ON DELETE clause — default is NoAction (same as Restrict)
    exec(&ex, "CREATE TABLE na_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO na_parent VALUES (1)").await;
    exec(&ex, "CREATE TABLE na_child (id INT, pid INT REFERENCES na_parent(id))").await;
    exec(&ex, "INSERT INTO na_child VALUES (1, 1)").await;

    let result = ex.execute("DELETE FROM na_parent WHERE id = 1").await;
    assert!(result.is_err(), "default FK action (NoAction) should prevent delete with children");
}

#[tokio::test]
async fn test_fk_cascade_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cu_parent (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO cu_parent VALUES (1, 'A')").await;
    exec(&ex, "CREATE TABLE cu_child (id INT, pid INT REFERENCES cu_parent(id) ON UPDATE CASCADE)").await;
    exec(&ex, "INSERT INTO cu_child VALUES (10, 1)").await;

    // Update parent PK from 1 to 99 — should cascade to child
    exec(&ex, "UPDATE cu_parent SET id = 99 WHERE id = 1").await;

    let r = exec(&ex, "SELECT pid FROM cu_child WHERE id = 10").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(99), "child FK should be updated to new parent PK");
}

#[tokio::test]
async fn test_fk_restrict_prevents_parent_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ru_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO ru_parent VALUES (1)").await;
    exec(&ex, "CREATE TABLE ru_child (id INT, pid INT REFERENCES ru_parent(id) ON UPDATE RESTRICT)").await;
    exec(&ex, "INSERT INTO ru_child VALUES (1, 1)").await;

    let result = ex.execute("UPDATE ru_parent SET id = 99 WHERE id = 1").await;
    assert!(result.is_err(), "UPDATE should fail with RESTRICT when children reference old PK");
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("foreign key"), "error should mention foreign key: {err_msg}");
}

#[tokio::test]
async fn test_fk_set_null_on_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE su_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO su_parent VALUES (1)").await;
    exec(&ex, "CREATE TABLE su_child (id INT, pid INT REFERENCES su_parent(id) ON UPDATE SET NULL)").await;
    exec(&ex, "INSERT INTO su_child VALUES (10, 1)").await;

    exec(&ex, "UPDATE su_parent SET id = 99 WHERE id = 1").await;

    let r = exec(&ex, "SELECT pid FROM su_child WHERE id = 10").await;
    assert_eq!(rows(&r[0])[0][0], Value::Null, "child FK should be NULL after ON UPDATE SET NULL");
}

#[tokio::test]
async fn test_fk_multiple_children_cascade() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE mc_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO mc_parent VALUES (1)").await;
    exec(&ex, "CREATE TABLE mc_child1 (id INT, pid INT REFERENCES mc_parent(id) ON DELETE CASCADE)").await;
    exec(&ex, "CREATE TABLE mc_child2 (id INT, pid INT REFERENCES mc_parent(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO mc_child1 VALUES (10, 1)").await;
    exec(&ex, "INSERT INTO mc_child2 VALUES (20, 1)").await;

    exec(&ex, "DELETE FROM mc_parent WHERE id = 1").await;

    let r1 = exec(&ex, "SELECT COUNT(*) FROM mc_child1").await;
    assert_eq!(*scalar(&r1[0]), Value::Int64(0), "child1 should be cascade-deleted");
    let r2 = exec(&ex, "SELECT COUNT(*) FROM mc_child2").await;
    assert_eq!(*scalar(&r2[0]), Value::Int64(0), "child2 should be cascade-deleted");
}

#[tokio::test]
async fn test_fk_self_referencing_cascade() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE employees_sr (id INT PRIMARY KEY, manager_id INT REFERENCES employees_sr(id) ON DELETE SET NULL, name TEXT)").await;
    exec(&ex, "INSERT INTO employees_sr VALUES (1, NULL, 'CEO')").await;
    exec(&ex, "INSERT INTO employees_sr VALUES (2, 1, 'VP')").await;
    exec(&ex, "INSERT INTO employees_sr VALUES (3, 1, 'Director')").await;

    // Delete CEO — VP and Director manager_id should become NULL
    exec(&ex, "DELETE FROM employees_sr WHERE id = 1").await;

    let r = exec(&ex, "SELECT manager_id FROM employees_sr WHERE id = 2").await;
    assert_eq!(rows(&r[0])[0][0], Value::Null, "VP manager_id should be NULL");
    let r = exec(&ex, "SELECT manager_id FROM employees_sr WHERE id = 3").await;
    assert_eq!(rows(&r[0])[0][0], Value::Null, "Director manager_id should be NULL");
}

#[tokio::test]
async fn test_fk_partial_delete_cascade() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pd_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO pd_parent VALUES (1), (2), (3)").await;
    exec(&ex, "CREATE TABLE pd_child (id INT, pid INT REFERENCES pd_parent(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO pd_child VALUES (10, 1), (20, 3)").await;

    // Delete parent id=2 — no children reference it, so child table unchanged
    exec(&ex, "DELETE FROM pd_parent WHERE id = 2").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM pd_child").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(2), "no children should be deleted");

    // Delete parent id=1 — only child with pid=1 should be removed
    exec(&ex, "DELETE FROM pd_parent WHERE id = 1").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM pd_child").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(1), "one child should remain (pid=3)");
}

#[tokio::test]
async fn test_fk_constraint_parsing_on_delete_cascade() {
    let ex = test_executor();
    // Verify ON DELETE CASCADE is correctly parsed from SQL
    exec(&ex, "CREATE TABLE cp_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE cp_child (id INT, pid INT REFERENCES cp_parent(id) ON DELETE CASCADE ON UPDATE SET NULL)").await;

    // Verify the constraint is stored with the correct actions
    let table_def = ex.catalog.get_table("cp_child").await.unwrap();
    let fk = table_def.constraints.iter().find(|c| {
        matches!(c, crate::catalog::TableConstraint::ForeignKey { .. })
    }).expect("should have FK constraint");
    match fk {
        crate::catalog::TableConstraint::ForeignKey { on_delete, on_update, .. } => {
            assert_eq!(*on_delete, crate::catalog::FkAction::Cascade, "on_delete should be Cascade");
            assert_eq!(*on_update, crate::catalog::FkAction::SetNull, "on_update should be SetNull");
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_fk_table_level_constraint_parsing() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE tl_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE tl_child (id INT, pid INT, FOREIGN KEY (pid) REFERENCES tl_parent(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO tl_parent VALUES (1)").await;
    exec(&ex, "INSERT INTO tl_child VALUES (10, 1)").await;

    exec(&ex, "DELETE FROM tl_parent WHERE id = 1").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM tl_child").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(0), "table-level FK CASCADE should work");
}

#[tokio::test]
async fn test_fk_null_child_bypasses_constraint() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nb_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO nb_parent VALUES (1)").await;
    exec(&ex, "CREATE TABLE nb_child (id INT, pid INT REFERENCES nb_parent(id) ON DELETE RESTRICT)").await;
    exec(&ex, "INSERT INTO nb_child VALUES (10, NULL)").await;

    // Delete parent — child has NULL FK so constraint should not trigger
    exec(&ex, "DELETE FROM nb_parent WHERE id = 1").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM nb_parent").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(0));
}

#[tokio::test]
async fn test_fk_cascade_delete_all_parents() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE da_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO da_parent VALUES (1), (2), (3)").await;
    exec(&ex, "CREATE TABLE da_child (id INT, pid INT REFERENCES da_parent(id) ON DELETE CASCADE)").await;
    exec(&ex, "INSERT INTO da_child VALUES (10, 1), (20, 2), (30, 3)").await;

    // Delete all parents at once
    exec(&ex, "DELETE FROM da_parent").await;

    let r = exec(&ex, "SELECT COUNT(*) FROM da_child").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(0), "all children should be cascade-deleted");
}

#[tokio::test]
async fn test_fk_update_non_referenced_column_ok() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE un_parent (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO un_parent VALUES (1, 'old')").await;
    exec(&ex, "CREATE TABLE un_child (id INT, pid INT REFERENCES un_parent(id) ON UPDATE RESTRICT)").await;
    exec(&ex, "INSERT INTO un_child VALUES (10, 1)").await;

    // Update non-PK column — should succeed even with RESTRICT
    exec(&ex, "UPDATE un_parent SET name = 'new' WHERE id = 1").await;
    let r = exec(&ex, "SELECT name FROM un_parent WHERE id = 1").await;
    assert_eq!(rows(&r[0])[0][0], Value::Text("new".into()));
}

#[tokio::test]
async fn test_fk_cascade_update_multi_level() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cu_gp (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO cu_gp VALUES (1)").await;
    exec(&ex, "CREATE TABLE cu_par (id INT, gp_id INT PRIMARY KEY REFERENCES cu_gp(id) ON UPDATE CASCADE)").await;
    exec(&ex, "INSERT INTO cu_par VALUES (10, 1)").await;
    exec(&ex, "CREATE TABLE cu_ch (id INT, par_id INT REFERENCES cu_par(gp_id) ON UPDATE CASCADE)").await;
    exec(&ex, "INSERT INTO cu_ch VALUES (100, 1)").await;

    // Update grandparent PK from 1 to 99 — should cascade through parent to child
    exec(&ex, "UPDATE cu_gp SET id = 99 WHERE id = 1").await;

    let r = exec(&ex, "SELECT gp_id FROM cu_par").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(99), "parent FK should be 99");
    let r = exec(&ex, "SELECT par_id FROM cu_ch").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(99), "child FK should cascade to 99");
}

#[tokio::test]
async fn test_fk_set_default_on_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sdu_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO sdu_parent VALUES (1), (0)").await;
    exec(&ex, "CREATE TABLE sdu_child (id INT, pid INT DEFAULT 0 REFERENCES sdu_parent(id) ON UPDATE SET DEFAULT)").await;
    exec(&ex, "INSERT INTO sdu_child VALUES (10, 1)").await;

    exec(&ex, "UPDATE sdu_parent SET id = 99 WHERE id = 1").await;

    let r = exec(&ex, "SELECT pid FROM sdu_child WHERE id = 10").await;
    assert_eq!(rows(&r[0])[0][0], Value::Int32(0), "child FK should be reset to default (0) on update");
}

#[tokio::test]
async fn test_fk_mixed_actions_delete_cascade_update_restrict() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE mx_parent (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO mx_parent VALUES (1)").await;
    exec(&ex, "CREATE TABLE mx_child (id INT, pid INT REFERENCES mx_parent(id) ON DELETE CASCADE ON UPDATE RESTRICT)").await;
    exec(&ex, "INSERT INTO mx_child VALUES (10, 1)").await;

    // UPDATE should fail (RESTRICT)
    let result = ex.execute("UPDATE mx_parent SET id = 99 WHERE id = 1").await;
    assert!(result.is_err(), "UPDATE should fail with ON UPDATE RESTRICT");

    // DELETE should succeed (CASCADE)
    exec(&ex, "DELETE FROM mx_parent WHERE id = 1").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM mx_child").await;
    assert_eq!(*scalar(&r[0]), Value::Int64(0));
}


// CTE with INSERT/UPDATE/DELETE tests
// ======================================================================

#[tokio::test]
async fn test_cte_insert_select() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cte_ins (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "WITH vals AS (SELECT 1 AS id, 'test' AS name) INSERT INTO cte_ins SELECT * FROM vals").await;
    let results = exec(&ex, "SELECT id, name FROM cte_ins").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("test".into()));
}

#[tokio::test]
async fn test_cte_insert_select_multi_row() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cte_ins2 (id INT PRIMARY KEY, val TEXT)").await;
    exec(&ex, "WITH data AS (SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b') INSERT INTO cte_ins2 SELECT * FROM data").await;
    let results = exec(&ex, "SELECT id, val FROM cte_ins2 ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[0][1], Value::Text("a".into()));
    assert_eq!(r[1][0], Value::Int32(2));
    assert_eq!(r[1][1], Value::Text("b".into()));
}

#[tokio::test]
async fn test_cte_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cte_upd (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO cte_upd VALUES (1, 'old'), (2, 'keep')").await;
    exec(&ex, "WITH targets AS (SELECT 1 AS id) UPDATE cte_upd SET name = 'new' WHERE id IN (SELECT id FROM targets)").await;
    let results = exec(&ex, "SELECT id, name FROM cte_upd ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Text("new".into()));
    assert_eq!(r[1][1], Value::Text("keep".into()));
}

#[tokio::test]
async fn test_cte_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cte_del (id INT PRIMARY KEY, name TEXT)").await;
    exec(&ex, "INSERT INTO cte_del VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
    exec(&ex, "WITH to_remove AS (SELECT 2 AS id) DELETE FROM cte_del WHERE id IN (SELECT id FROM to_remove)").await;
    let results = exec(&ex, "SELECT id FROM cte_del ORDER BY id").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(3));
}

// ======================================================================
