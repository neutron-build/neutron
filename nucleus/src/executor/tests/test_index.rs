use super::*;

// ================================================================
// B-tree range index scan tests
// ================================================================

/// Basic range scan: `col > lo AND col < hi` should use IndexScan, not SeqScan.
#[tokio::test]
async fn test_btree_range_scan_exclusive_bounds() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE nums (id INT, val INT)").await;
    for i in 1..=100i32 {
        exec(&ex, &format!("INSERT INTO nums VALUES ({i}, {i})")).await;
    }
    exec(&ex, "CREATE INDEX idx_nums_val ON nums (val)").await;

    // Exclusive bounds: val > 10 AND val < 20  →  should return 11..=19
    let results = exec(&ex, "SELECT val FROM nums WHERE val > 10 AND val < 20 ORDER BY val").await;
    let vals: Vec<i32> = rows(&results[0])
        .iter()
        .map(|r| match &r[0] { Value::Int32(n) => *n, _ => panic!("expected i32") })
        .collect();
    assert_eq!(vals, (11..=19).collect::<Vec<_>>());
}

/// Inclusive bounds: `col >= lo AND col <= hi`
#[tokio::test]
async fn test_btree_range_scan_inclusive_bounds() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE scores (id INT, score INT)").await;
    for i in 1..=50i32 {
        exec(&ex, &format!("INSERT INTO scores VALUES ({i}, {i})")).await;
    }
    exec(&ex, "CREATE INDEX idx_scores_score ON scores (score)").await;

    // Inclusive bounds: score >= 20 AND score <= 30  →  20..=30
    let results =
        exec(&ex, "SELECT score FROM scores WHERE score >= 20 AND score <= 30 ORDER BY score").await;
    let vals: Vec<i32> = rows(&results[0])
        .iter()
        .map(|r| match &r[0] { Value::Int32(n) => *n, _ => panic!("expected i32") })
        .collect();
    assert_eq!(vals, (20..=30).collect::<Vec<_>>());
}

/// Mixed inclusive/exclusive: `col >= lo AND col < hi`
#[tokio::test]
async fn test_btree_range_scan_mixed_bounds() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE events (id INT, ts INT)").await;
    for i in 0..=20i32 {
        exec(&ex, &format!("INSERT INTO events VALUES ({i}, {i})")).await;
    }
    exec(&ex, "CREATE INDEX idx_events_ts ON events (ts)").await;

    // ts >= 5 AND ts < 15  →  5..=14
    let results =
        exec(&ex, "SELECT ts FROM events WHERE ts >= 5 AND ts < 15 ORDER BY ts").await;
    let vals: Vec<i32> = rows(&results[0])
        .iter()
        .map(|r| match &r[0] { Value::Int32(n) => *n, _ => panic!("expected i32") })
        .collect();
    assert_eq!(vals, (5..=14).collect::<Vec<_>>());
}

/// Range scan on a large table should return correct results and not include boundary violations.
#[tokio::test]
async fn test_btree_range_scan_large_table() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE big (id INT, v INT)").await;
    for i in 1..=1000i32 {
        exec(&ex, &format!("INSERT INTO big VALUES ({i}, {i})")).await;
    }
    exec(&ex, "CREATE INDEX idx_big_v ON big (v)").await;

    // v >= 400 AND v <= 600 → 201 rows
    let results =
        exec(&ex, "SELECT COUNT(*) FROM big WHERE v >= 400 AND v <= 600").await;
    let count = match &rows(&results[0])[0][0] {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("expected count, got {other:?}"),
    };
    assert_eq!(count, 201);
}

/// Range scan with no matching rows returns empty result.
#[tokio::test]
async fn test_btree_range_scan_empty_result() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE emp (id INT, age INT)").await;
    for i in 1..=10i32 {
        exec(&ex, &format!("INSERT INTO emp VALUES ({i}, {i}0)")).await;
    }
    exec(&ex, "CREATE INDEX idx_emp_age ON emp (age)").await;

    // age > 200 AND age < 300 → no rows (ages are 10,20,..,100)
    let results = exec(&ex, "SELECT * FROM emp WHERE age > 200 AND age < 300").await;
    assert_eq!(rows(&results[0]).len(), 0);
}

/// EXPLAIN shows IndexScan with Index Range when range predicates are present.
#[tokio::test]
async fn test_btree_range_scan_explain_shows_index_range() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE things (id INT, n INT)").await;
    for i in 1..=200i32 {
        exec(&ex, &format!("INSERT INTO things VALUES ({i}, {i})")).await;
    }
    exec(&ex, "CREATE INDEX idx_things_n ON things (n)").await;

    let results = exec(&ex, "EXPLAIN SELECT * FROM things WHERE n > 50 AND n < 150").await;
    let plan_text: String = rows(&results[0])
        .iter()
        .map(|r| match &r[0] { Value::Text(s) => s.clone(), _ => String::new() })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        plan_text.contains("Index Scan") || plan_text.contains("Index Range"),
        "Expected index scan in plan, got:\n{plan_text}"
    );
}

// Encrypted index integration tests
// ================================================================

#[tokio::test]
async fn test_encrypted_index_creation() {
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
    }
    let ex = test_executor();
    exec(&ex, "CREATE TABLE secrets (id INT, ssn TEXT)").await;
    exec(&ex, "INSERT INTO secrets VALUES (1, '123-45-6789')").await;
    exec(&ex, "INSERT INTO secrets VALUES (2, '987-65-4321')").await;

    // Create encrypted index.
    exec(&ex, "CREATE INDEX ssn_enc ON secrets USING encrypted (ssn)").await;

    // Verify index was created.
    let indexes = ex.encrypted_indexes.read();
    assert!(indexes.contains_key("ssn_enc"));
    let entry = indexes.get("ssn_enc").unwrap();
    assert_eq!(entry.table_name, "secrets");
    assert_eq!(entry.column_name, "ssn");
    assert_eq!(entry.index.len(), 2);
}

#[tokio::test]
async fn test_encrypted_index_lookup_function() {
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
    }
    let ex = test_executor();
    exec(&ex, "CREATE TABLE patients (id INT, ssn TEXT)").await;
    exec(&ex, "INSERT INTO patients VALUES (1, 'AAA')").await;
    exec(&ex, "INSERT INTO patients VALUES (2, 'BBB')").await;
    exec(&ex, "INSERT INTO patients VALUES (3, 'AAA')").await;

    exec(&ex, "CREATE INDEX pat_ssn_enc ON patients USING encrypted (ssn)").await;

    // Lookup via ENCRYPTED_LOOKUP function.
    let results = exec(&ex, "SELECT ENCRYPTED_LOOKUP('pat_ssn_enc', 'AAA') FROM patients LIMIT 1").await;
    let r = rows(&results[0]);
    // Should find row IDs for both rows with 'AAA'.
    let ids_str = match &r[0][0] {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(!ids_str.is_empty(), "should find matching rows");
}

#[tokio::test]
async fn test_encrypted_index_maintained_on_insert() {
    unsafe {
        std::env::set_var("NUCLEUS_ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef");
    }
    let ex = test_executor();
    exec(&ex, "CREATE TABLE enc_data (id INT, code TEXT)").await;

    // Create index first (empty table).
    exec(&ex, "CREATE INDEX code_enc ON enc_data USING encrypted (code)").await;
    {
        let indexes = ex.encrypted_indexes.read();
        assert_eq!(indexes.get("code_enc").unwrap().index.len(), 0);
    }

    // Insert rows — encrypted index should be maintained.
    exec(&ex, "INSERT INTO enc_data VALUES (1, 'alpha')").await;
    exec(&ex, "INSERT INTO enc_data VALUES (2, 'beta')").await;
    exec(&ex, "INSERT INTO enc_data VALUES (3, 'alpha')").await;
    {
        let indexes = ex.encrypted_indexes.read();
        // len() counts unique encrypted keys: 'alpha' and 'beta' = 2 unique keys
        assert_eq!(indexes.get("code_enc").unwrap().index.len(), 2);
    }
}

// ================================================================

// SIMD-accelerated aggregate test
// ================================================================

#[tokio::test]
async fn test_simd_sum_integration() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nums (id INT, val INT)").await;
    exec(&ex, "INSERT INTO nums VALUES (1, 10)").await;
    exec(&ex, "INSERT INTO nums VALUES (2, 20)").await;
    exec(&ex, "INSERT INTO nums VALUES (3, 30)").await;
    exec(&ex, "INSERT INTO nums VALUES (4, 40)").await;

    let results = exec(&ex, "SELECT SUM(val) FROM nums").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Int64(100));
}

#[tokio::test]
async fn test_simd_sum_with_group_by() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales (cat TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO sales VALUES ('a', 10)").await;
    exec(&ex, "INSERT INTO sales VALUES ('a', 20)").await;
    exec(&ex, "INSERT INTO sales VALUES ('b', 30)").await;
    exec(&ex, "INSERT INTO sales VALUES ('b', 40)").await;

    let results = exec(&ex, "SELECT cat, SUM(amount) FROM sales GROUP BY cat ORDER BY cat").await;
    let r = rows(&results[0]);
    assert_eq!(r[0][0], Value::Text("a".into()));
    assert_eq!(r[0][1], Value::Int64(30));
    assert_eq!(r[1][0], Value::Text("b".into()));
    assert_eq!(r[1][1], Value::Int64(70));
}

// ================================================================

// Index-Aware Execution Tests (DiskEngine)
// ======================================================================

/// Create a DiskEngine-backed executor in a temp directory.
fn disk_executor() -> (Executor, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    let catalog = Arc::new(Catalog::new());
    let engine = crate::storage::DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);
    (Executor::new(catalog, storage), tmp)
}

#[tokio::test]
async fn test_index_scan_basic_equality() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE users (id INT, name TEXT)").await;
    for i in 1..=100 {
        exec(&ex, &format!("INSERT INTO users VALUES ({i}, 'user_{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_users_id ON users (id)").await;

    // Query with WHERE id = 42 should use index scan
    let results = exec(&ex, "SELECT * FROM users WHERE id = 42").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Int32(42) | Value::Int64(42)));
    assert_eq!(r[0][1], Value::Text("user_42".into()));
}

#[tokio::test]
async fn test_index_scan_no_match() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE items (id INT, val TEXT)").await;
    for i in 1..=10 {
        exec(&ex, &format!("INSERT INTO items VALUES ({i}, 'v{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_items_id ON items (id)").await;

    // Query for a non-existent value
    let results = exec(&ex, "SELECT * FROM items WHERE id = 999").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0);
}

#[tokio::test]
async fn test_index_scan_with_remaining_predicate() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE products (id INT, category TEXT, price INT)").await;
    exec(&ex, "INSERT INTO products VALUES (1, 'A', 10)").await;
    exec(&ex, "INSERT INTO products VALUES (2, 'A', 20)").await;
    exec(&ex, "INSERT INTO products VALUES (3, 'B', 10)").await;
    exec(&ex, "INSERT INTO products VALUES (4, 'A', 10)").await;
    exec(&ex, "CREATE INDEX idx_cat ON products (category)").await;

    // Index on category, but also filter by price
    let results = exec(&ex, "SELECT * FROM products WHERE category = 'A' AND price = 10").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2); // id=1 and id=4
}

#[tokio::test]
async fn test_index_scan_text_key() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE kv (key TEXT, val TEXT)").await;
    exec(&ex, "INSERT INTO kv VALUES ('alpha', 'one')").await;
    exec(&ex, "INSERT INTO kv VALUES ('beta', 'two')").await;
    exec(&ex, "INSERT INTO kv VALUES ('gamma', 'three')").await;
    exec(&ex, "CREATE INDEX idx_kv_key ON kv (key)").await;

    let results = exec(&ex, "SELECT * FROM kv WHERE key = 'beta'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("two".into()));
}

#[tokio::test]
async fn test_index_scan_after_insert() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "CREATE INDEX idx_t_id ON t (id)").await;

    // Insert AFTER index creation — index should be maintained
    exec(&ex, "INSERT INTO t VALUES (10, 'ten')").await;
    exec(&ex, "INSERT INTO t VALUES (20, 'twenty')").await;
    exec(&ex, "INSERT INTO t VALUES (30, 'thirty')").await;

    let results = exec(&ex, "SELECT * FROM t WHERE id = 20").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][1], Value::Text("twenty".into()));
}

#[tokio::test]
async fn test_index_scan_not_used_for_non_equality() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE nums (n INT)").await;
    for i in 1..=10 {
        exec(&ex, &format!("INSERT INTO nums VALUES ({i})")).await;
    }
    exec(&ex, "CREATE INDEX idx_n ON nums (n)").await;

    // Range predicate — should fall back to full scan
    let results = exec(&ex, "SELECT * FROM nums WHERE n > 5").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 5);
}

#[tokio::test]
async fn test_index_scan_used_for_join_pushdown_filters() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE a (id INT, val TEXT)").await;
    exec(&ex, "CREATE TABLE b (aid INT, extra TEXT)").await;
    for i in 1..=1000 {
        exec(&ex, &format!("INSERT INTO a VALUES ({i}, 'v_{i}')")).await;
        exec(&ex, &format!("INSERT INTO b VALUES ({i}, 'e_{i}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_a_id ON a (id)").await;
    exec(&ex, "CREATE INDEX idx_b_aid ON b (aid)").await;

    let before_scanned = ex.metrics().rows_scanned.get();
    let before_index_join_used = ex.metrics().index_join_used.get();
    // LEFT JOIN forces AST path (plan execution intentionally skips LEFT/RIGHT/FULL),
    // so this validates join-aware index pushdown in AST execution.
    let results = exec(
        &ex,
        "SELECT a.id, b.extra FROM a LEFT JOIN b ON a.id = b.aid WHERE a.id = 777 AND b.aid = 777",
    )
    .await;
    let after_scanned = ex.metrics().rows_scanned.get();
    let scanned_delta = after_scanned.saturating_sub(before_scanned);
    let after_index_join_used = ex.metrics().index_join_used.get();
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Int32(777) | Value::Int64(777)));
    assert_eq!(r[0][1], Value::Text("e_777".into()));
    assert!(
        after_index_join_used > before_index_join_used,
        "expected index-join optimization to be used"
    );
    assert!(
        scanned_delta < 100,
        "expected indexed join pushdown to avoid full scans; scanned_delta={scanned_delta}"
    );
}

#[tokio::test]
async fn test_index_join_respects_all_equi_join_keys() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE a (id1 INT, id2 INT, payload TEXT)").await;
    exec(&ex, "CREATE TABLE b (k1 INT, k2 INT, extra TEXT)").await;
    exec(&ex, "INSERT INTO a VALUES (1, 10, 'a_1_10')").await;
    exec(&ex, "INSERT INTO a VALUES (1, 20, 'a_1_20')").await;
    exec(&ex, "INSERT INTO b VALUES (1, 10, 'b_1_10')").await;
    exec(&ex, "INSERT INTO b VALUES (1, 99, 'b_1_99')").await;
    exec(&ex, "CREATE INDEX idx_b_k1 ON b (k1)").await;

    let before_used = ex.metrics().index_join_used.get();
    let results = exec(
        &ex,
        "SELECT a.payload, b.extra \
         FROM a LEFT JOIN b ON a.id1 = b.k1 AND a.id2 = b.k2 \
         WHERE a.id1 = 1 AND b.k1 = 1 \
         ORDER BY a.id2",
    )
    .await;
    let after_used = ex.metrics().index_join_used.get();
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("a_1_10".into()));
    assert_eq!(r[0][1], Value::Text("b_1_10".into()));
    assert!(
        after_used > before_used,
        "expected index-join optimization to run for multi-key join"
    );
}

#[tokio::test]
async fn test_index_scan_drop_index_reverts_to_scan() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'b')").await;
    exec(&ex, "CREATE INDEX idx_id ON t (id)").await;

    // Should use index
    let res1 = exec(&ex, "SELECT * FROM t WHERE id = 1").await;
    assert_eq!(rows(&res1[0]).len(), 1);

    // Drop the index
    exec(&ex, "DROP INDEX idx_id").await;

    // Should still work (falls back to full scan)
    let res2 = exec(&ex, "SELECT * FROM t WHERE id = 1").await;
    assert_eq!(rows(&res2[0]).len(), 1);
}

#[tokio::test]
async fn test_index_scan_multiple_indexes() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE multi (a INT, b TEXT, c INT)").await;
    exec(&ex, "INSERT INTO multi VALUES (1, 'x', 100)").await;
    exec(&ex, "INSERT INTO multi VALUES (2, 'y', 200)").await;
    exec(&ex, "INSERT INTO multi VALUES (3, 'x', 300)").await;
    exec(&ex, "CREATE INDEX idx_a ON multi (a)").await;
    exec(&ex, "CREATE INDEX idx_b ON multi (b)").await;

    // Query on indexed column a
    let res1 = exec(&ex, "SELECT * FROM multi WHERE a = 2").await;
    let r1 = rows(&res1[0]);
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0][1], Value::Text("y".into()));

    // Query on indexed column b
    let res2 = exec(&ex, "SELECT * FROM multi WHERE b = 'x'").await;
    assert_eq!(rows(&res2[0]).len(), 2);
}

#[tokio::test]
async fn test_index_maintained_after_delete() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;
    exec(&ex, "CREATE INDEX idx_id ON t (id)").await;

    // Verify index works before delete
    let res = exec(&ex, "SELECT * FROM t WHERE id = 2").await;
    assert_eq!(rows(&res[0]).len(), 1);

    // Delete bob (id=2)
    exec(&ex, "DELETE FROM t WHERE id = 2").await;

    // Index should no longer find id=2
    let res = exec(&ex, "SELECT * FROM t WHERE id = 2").await;
    assert_eq!(rows(&res[0]).len(), 0);

    // Other entries should still be found
    let res = exec(&ex, "SELECT * FROM t WHERE id = 1").await;
    assert_eq!(rows(&res[0]).len(), 1);
    let res = exec(&ex, "SELECT * FROM t WHERE id = 3").await;
    assert_eq!(rows(&res[0]).len(), 1);
}

#[tokio::test]
async fn test_index_maintained_after_update() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE t (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO t VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO t VALUES (3, 'carol')").await;
    exec(&ex, "CREATE INDEX idx_name ON t (name)").await;

    // Verify index works before update
    let res = exec(&ex, "SELECT * FROM t WHERE name = 'bob'").await;
    assert_eq!(rows(&res[0]).len(), 1);

    // Update bob → dave
    exec(&ex, "UPDATE t SET name = 'dave' WHERE id = 2").await;

    // Old value should no longer be found via index
    let res = exec(&ex, "SELECT * FROM t WHERE name = 'bob'").await;
    assert_eq!(rows(&res[0]).len(), 0);

    // New value should be found via index
    let res = exec(&ex, "SELECT * FROM t WHERE name = 'dave'").await;
    assert_eq!(rows(&res[0]).len(), 1);

    // Other entries unchanged
    let res = exec(&ex, "SELECT * FROM t WHERE name = 'alice'").await;
    assert_eq!(rows(&res[0]).len(), 1);
}

#[tokio::test]
async fn test_index_maintained_delete_all_reinsert() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE t (id INT)").await;
    exec(&ex, "CREATE INDEX idx_id ON t (id)").await;
    exec(&ex, "INSERT INTO t VALUES (10)").await;
    exec(&ex, "INSERT INTO t VALUES (20)").await;

    // Delete all
    exec(&ex, "DELETE FROM t WHERE id = 10").await;
    exec(&ex, "DELETE FROM t WHERE id = 20").await;

    // Index should find nothing
    let res = exec(&ex, "SELECT * FROM t WHERE id = 10").await;
    assert_eq!(rows(&res[0]).len(), 0);
    let res = exec(&ex, "SELECT * FROM t WHERE id = 20").await;
    assert_eq!(rows(&res[0]).len(), 0);

    // Re-insert same values
    exec(&ex, "INSERT INTO t VALUES (10)").await;
    exec(&ex, "INSERT INTO t VALUES (20)").await;

    // Index should find them again
    let res = exec(&ex, "SELECT * FROM t WHERE id = 10").await;
    assert_eq!(rows(&res[0]).len(), 1);
    let res = exec(&ex, "SELECT * FROM t WHERE id = 20").await;
    assert_eq!(rows(&res[0]).len(), 1);
}

// ======================================================================

// SIMD WHERE filter tests (Sprint 4 — Phase 4 performance)
// ========================================================================

#[tokio::test]
async fn test_simd_filter_i64_equals() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_eq (id INT, val BIGINT)").await;
    exec(&ex, "INSERT INTO sf_eq VALUES (1, 10), (2, 20), (3, 10), (4, 30), (5, 10)").await;
    let r = exec(&ex, "SELECT id FROM sf_eq WHERE val = 10").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&Value::Int32(1), &Value::Int32(3), &Value::Int32(5)]);
}

#[tokio::test]
async fn test_simd_filter_i64_greater() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_gt (id INT, val BIGINT)").await;
    exec(&ex, "INSERT INTO sf_gt VALUES (1, 5), (2, 15), (3, 25), (4, 35)").await;
    let r = exec(&ex, "SELECT id FROM sf_gt WHERE val > 15").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&Value::Int32(3), &Value::Int32(4)]);
}

#[tokio::test]
async fn test_simd_filter_i64_less() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_lt (id INT, val BIGINT)").await;
    exec(&ex, "INSERT INTO sf_lt VALUES (1, 5), (2, 15), (3, 25), (4, 35)").await;
    let r = exec(&ex, "SELECT id FROM sf_lt WHERE val < 20").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&Value::Int32(1), &Value::Int32(2)]);
}

#[tokio::test]
async fn test_simd_filter_f64_greater() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_fgt (id INT, val DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO sf_fgt VALUES (1, 1.5), (2, 3.7), (3, 5.9), (4, 8.1)").await;
    let r = exec(&ex, "SELECT id FROM sf_fgt WHERE val > 4.0").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&Value::Int32(3), &Value::Int32(4)]);
}

#[tokio::test]
async fn test_simd_filter_compound_fallback() {
    // Compound predicate (AND) should fall back to per-row eval, not SIMD
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_comp (id INT, a BIGINT, b BIGINT)").await;
    exec(&ex, "INSERT INTO sf_comp VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)").await;
    let r = exec(&ex, "SELECT id FROM sf_comp WHERE a = 10 AND b > 150").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&Value::Int32(3)]);
}

#[tokio::test]
async fn test_simd_filter_preserves_row_order() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_ord (id INT, val BIGINT)").await;
    exec(&ex, "INSERT INTO sf_ord VALUES (10, 1), (20, 2), (30, 1), (40, 2), (50, 1)").await;
    let r = exec(&ex, "SELECT id FROM sf_ord WHERE val = 1").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    // Must preserve insertion order
    assert_eq!(ids, vec![&Value::Int32(10), &Value::Int32(30), &Value::Int32(50)]);
}

#[tokio::test]
async fn test_simd_filter_empty_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_empty (id INT, val BIGINT)").await;
    let r = exec(&ex, "SELECT id FROM sf_empty WHERE val = 42").await;
    assert!(rows(&r[0]).is_empty());
}

#[tokio::test]
async fn test_simd_filter_all_rows_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sf_all (id INT, val BIGINT)").await;
    exec(&ex, "INSERT INTO sf_all VALUES (1, 100), (2, 200), (3, 300)").await;
    let r = exec(&ex, "SELECT id FROM sf_all WHERE val > 0").await;
    let ids: Vec<&Value> = rows(&r[0]).iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&Value::Int32(1), &Value::Int32(2), &Value::Int32(3)]);
}

// ========================================================================

// i64 GROUP BY fast path tests (Sprint 5 — Phase 4 performance)
// ========================================================================

#[tokio::test]
async fn test_i64_group_by_count() {
    // Single integer GROUP BY with COUNT — should use the i64 fast path.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ig_cnt (dept INT, name TEXT)").await;
    exec(&ex, "INSERT INTO ig_cnt VALUES (1, 'alice'), (1, 'bob'), (2, 'charlie'), (3, 'dave'), (2, 'eve')").await;
    let r = exec(&ex, "SELECT dept, COUNT(*) FROM ig_cnt GROUP BY dept ORDER BY dept").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 3);
    assert_eq!(rr[0][0], Value::Int32(1));
    assert_eq!(rr[0][1], Value::Int64(2));
    assert_eq!(rr[1][0], Value::Int32(2));
    assert_eq!(rr[1][1], Value::Int64(2));
    assert_eq!(rr[2][0], Value::Int32(3));
    assert_eq!(rr[2][1], Value::Int64(1));
}

#[tokio::test]
async fn test_i64_group_by_sum() {
    // Single integer GROUP BY with SUM — should use the i64 fast path.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ig_sum (cat INT, amount INT)").await;
    exec(&ex, "INSERT INTO ig_sum VALUES (10, 100), (10, 200), (20, 300), (20, 400), (30, 500)").await;
    let r = exec(&ex, "SELECT cat, SUM(amount) FROM ig_sum GROUP BY cat ORDER BY cat").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 3);
    assert_eq!(rr[0][0], Value::Int32(10));
    assert_eq!(rr[0][1], Value::Int64(300));
    assert_eq!(rr[1][0], Value::Int32(20));
    assert_eq!(rr[1][1], Value::Int64(700));
    assert_eq!(rr[2][0], Value::Int32(30));
    assert_eq!(rr[2][1], Value::Int64(500));
}

#[tokio::test]
async fn test_i64_group_by_avg() {
    // Single integer GROUP BY with AVG — should use the i64 fast path.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ig_avg (grp BIGINT, val FLOAT)").await;
    exec(&ex, "INSERT INTO ig_avg VALUES (1, 10.0), (1, 20.0), (2, 30.0), (2, 40.0), (2, 50.0)").await;
    let r = exec(&ex, "SELECT grp, AVG(val) FROM ig_avg GROUP BY grp ORDER BY grp").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 2);
    assert_eq!(rr[0][0], Value::Int32(1));
    // AVG of 10.0 and 20.0 = 15.0
    assert_eq!(rr[0][1], Value::Float64(15.0));
    assert_eq!(rr[1][0], Value::Int32(2));
    // AVG of 30.0, 40.0, 50.0 = 40.0
    assert_eq!(rr[1][1], Value::Float64(40.0));
}

#[tokio::test]
async fn test_i64_group_by_having() {
    // HAVING clause works with the i64 fast path.
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ig_hav (region INT, sales INT)").await;
    exec(&ex, "INSERT INTO ig_hav VALUES (1, 10), (1, 20), (2, 5), (3, 100), (3, 200)").await;
    let r = exec(&ex, "SELECT region, SUM(sales) FROM ig_hav GROUP BY region HAVING SUM(sales) > 25 ORDER BY region").await;
    let rr = rows(&r[0]);
    // Region 1: SUM=30 (>25), Region 2: SUM=5 (<=25), Region 3: SUM=300 (>25)
    assert_eq!(rr.len(), 2);
    assert_eq!(rr[0][0], Value::Int32(1));
    assert_eq!(rr[0][1], Value::Int64(30));
    assert_eq!(rr[1][0], Value::Int32(3));
    assert_eq!(rr[1][1], Value::Int64(300));
}

#[tokio::test]
async fn test_multi_col_group_by_fallback() {
    // Multi-column GROUP BY should fall back to the generic path (not i64 fast path).
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ig_multi (a INT, b INT, val INT)").await;
    exec(&ex, "INSERT INTO ig_multi VALUES (1, 10, 100), (1, 10, 200), (1, 20, 300), (2, 10, 400)").await;
    let r = exec(&ex, "SELECT a, b, SUM(val) FROM ig_multi GROUP BY a, b ORDER BY a, b").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 3);
    assert_eq!(rr[0][0], Value::Int32(1));
    assert_eq!(rr[0][1], Value::Int32(10));
    assert_eq!(rr[0][2], Value::Int64(300));
    assert_eq!(rr[1][0], Value::Int32(1));
    assert_eq!(rr[1][1], Value::Int32(20));
    assert_eq!(rr[1][2], Value::Int64(300));
    assert_eq!(rr[2][0], Value::Int32(2));
    assert_eq!(rr[2][1], Value::Int32(10));
    assert_eq!(rr[2][2], Value::Int64(400));
}

#[tokio::test]
async fn test_text_group_by_fallback() {
    // Text column GROUP BY should fall back to the generic path (not i64 fast path).
    let ex = test_executor();
    exec(&ex, "CREATE TABLE ig_text (label TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO ig_text VALUES ('x', 10), ('y', 20), ('x', 30), ('y', 40)").await;
    let r = exec(&ex, "SELECT label, SUM(amount) FROM ig_text GROUP BY label ORDER BY label").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 2);
    assert_eq!(rr[0][0], Value::Text("x".into()));
    assert_eq!(rr[0][1], Value::Int64(40));
    assert_eq!(rr[1][0], Value::Text("y".into()));
    assert_eq!(rr[1][1], Value::Int64(60));
}

// Extended index routing tests

/// Setup: create table with 10 rows (id 1..=10), create index on id.
async fn setup_indexed_table(ex: &Executor, table: &str) {
    exec(ex, &format!("CREATE TABLE {table} (id INT, name TEXT)")).await;
    for i in 1..=10 {
        exec(ex, &format!("INSERT INTO {table} VALUES ({i}, 'row_{i}')")).await;
    }
    exec(ex, &format!("CREATE INDEX idx_{table}_id ON {table} (id)")).await;
}

// ------------------------------------------------------------------
// 1. WHERE id > N — memory engine
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_gt() {
    let ex = test_executor();
    setup_indexed_table(&ex, "gt_tbl").await;

    let r = exec(&ex, "SELECT id FROM gt_tbl WHERE id > 7 ORDER BY id").await;
    let ids: Vec<i32> = rows(&r[0]).iter().map(|r| match &r[0] {
        Value::Int32(v) => *v,
        other => panic!("expected Int32, got {other:?}"),
    }).collect();
    assert_eq!(ids, vec![8, 9, 10], "WHERE id > 7 should return 8, 9, 10");
}

// ------------------------------------------------------------------
// 2. WHERE id < N — memory engine
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_lt() {
    let ex = test_executor();
    setup_indexed_table(&ex, "lt_tbl").await;

    let r = exec(&ex, "SELECT id FROM lt_tbl WHERE id < 4 ORDER BY id").await;
    let ids: Vec<i32> = rows(&r[0]).iter().map(|r| match &r[0] {
        Value::Int32(v) => *v,
        other => panic!("expected Int32, got {other:?}"),
    }).collect();
    assert_eq!(ids, vec![1, 2, 3], "WHERE id < 4 should return 1, 2, 3");
}

// ------------------------------------------------------------------
// 3. WHERE id >= N — disk engine (B-tree range scan)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_gte_disk() {
    let (ex, _tmp) = disk_executor();
    setup_indexed_table(&ex, "gte_tbl").await;

    let r = exec(&ex, "SELECT id FROM gte_tbl WHERE id >= 9 ORDER BY id").await;
    let ids: Vec<i32> = rows(&r[0]).iter().map(|r| match &r[0] {
        Value::Int32(v) => *v,
        other => panic!("expected Int32, got {other:?}"),
    }).collect();
    assert_eq!(ids, vec![9, 10], "WHERE id >= 9 should return 9, 10");
}

// ------------------------------------------------------------------
// 4. WHERE id <= N — disk engine (B-tree range scan)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_lte_disk() {
    let (ex, _tmp) = disk_executor();
    setup_indexed_table(&ex, "lte_tbl").await;

    let r = exec(&ex, "SELECT id FROM lte_tbl WHERE id <= 2 ORDER BY id").await;
    let ids: Vec<i32> = rows(&r[0]).iter().map(|r| match &r[0] {
        Value::Int32(v) => *v,
        other => panic!("expected Int32, got {other:?}"),
    }).collect();
    assert_eq!(ids, vec![1, 2], "WHERE id <= 2 should return 1, 2");
}

// ------------------------------------------------------------------
// 5. Combined: comparison + remaining WHERE clause
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_gt_with_additional_filter() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE combo (id INT, category TEXT)").await;
    for i in 1..=20 {
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        exec(&ex, &format!("INSERT INTO combo VALUES ({i}, '{cat}')")).await;
    }
    exec(&ex, "CREATE INDEX idx_combo_id ON combo (id)").await;

    // id > 15 AND category = 'even' → only 16, 18, 20
    let r = exec(&ex, "SELECT id FROM combo WHERE id > 15 AND category = 'even' ORDER BY id").await;
    let ids: Vec<i32> = rows(&r[0]).iter().map(|r| match &r[0] {
        Value::Int32(v) => *v,
        other => panic!("expected Int32, got {other:?}"),
    }).collect();
    assert_eq!(ids, vec![16, 18, 20], "WHERE id > 15 AND category = 'even' should return 16, 18, 20");
}

// ------------------------------------------------------------------
// 6. Empty result set (no rows match the comparison)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_gt_empty_result() {
    let ex = test_executor();
    setup_indexed_table(&ex, "empty_tbl").await;

    let r = exec(&ex, "SELECT id FROM empty_tbl WHERE id > 100 ORDER BY id").await;
    assert!(rows(&r[0]).is_empty(), "WHERE id > 100 should return no rows");

    let r = exec(&ex, "SELECT id FROM empty_tbl WHERE id < 0 ORDER BY id").await;
    assert!(rows(&r[0]).is_empty(), "WHERE id < 0 should return no rows");
}

// ------------------------------------------------------------------
// 7. Text column comparison with index
// ------------------------------------------------------------------
#[tokio::test]
async fn test_index_text_comparison() {
    let (ex, _tmp) = disk_executor();
    exec(&ex, "CREATE TABLE txt_cmp (label TEXT, val INT)").await;
    exec(&ex, "INSERT INTO txt_cmp VALUES ('apple', 1)").await;
    exec(&ex, "INSERT INTO txt_cmp VALUES ('banana', 2)").await;
    exec(&ex, "INSERT INTO txt_cmp VALUES ('cherry', 3)").await;
    exec(&ex, "INSERT INTO txt_cmp VALUES ('date', 4)").await;
    exec(&ex, "INSERT INTO txt_cmp VALUES ('elderberry', 5)").await;
    exec(&ex, "CREATE INDEX idx_txt_label ON txt_cmp (label)").await;

    // label >= 'cherry' should return cherry, date, elderberry
    let r = exec(&ex, "SELECT label FROM txt_cmp WHERE label >= 'cherry' ORDER BY label").await;
    let labels: Vec<String> = rows(&r[0]).iter().map(|r| match &r[0] {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    }).collect();
    assert_eq!(labels, vec!["cherry", "date", "elderberry"],
        "WHERE label >= 'cherry' should return cherry, date, elderberry");
}

// Parallel scan tests

// ------------------------------------------------------------------
// 1. Large table WHERE filter produces correct results
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_filter_large_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_large (id INT, val INT)").await;
    // Insert 100 rows: val cycles 0..9
    for i in 1..=100 {
        exec(&ex, &format!("INSERT INTO pf_large VALUES ({i}, {})", i % 10)).await;
    }
    let r = exec(&ex, "SELECT id FROM pf_large WHERE val = 5").await;
    let rr = rows(&r[0]);
    // ids: 5, 15, 25, 35, 45, 55, 65, 75, 85, 95
    assert_eq!(rr.len(), 10);
}

// ------------------------------------------------------------------
// 2. Below threshold uses serial (verify correctness)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_filter_below_threshold() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_small (id INT, flag BOOLEAN)").await;
    for i in 1..=20 {
        let flag = if i % 2 == 0 { "TRUE" } else { "FALSE" };
        exec(&ex, &format!("INSERT INTO pf_small VALUES ({i}, {flag})")).await;
    }
    let r = exec(&ex, "SELECT id FROM pf_small WHERE flag = TRUE").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 10); // even ids 2,4,6,...,20
}

// ------------------------------------------------------------------
// 3. Empty table returns no rows
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_filter_empty_table() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_empty (id INT, name TEXT)").await;
    let r = exec(&ex, "SELECT id FROM pf_empty WHERE id > 0").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 0);
}

// ------------------------------------------------------------------
// 4. All rows match the filter
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_filter_all_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_all (id INT, val INT)").await;
    for i in 1..=50 {
        exec(&ex, &format!("INSERT INTO pf_all VALUES ({i}, 1)")).await;
    }
    let r = exec(&ex, "SELECT id FROM pf_all WHERE val = 1").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 50);
}

// ------------------------------------------------------------------
// 5. No rows match the filter
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_filter_none_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_none (id INT, val INT)").await;
    for i in 1..=30 {
        exec(&ex, &format!("INSERT INTO pf_none VALUES ({i}, {i})")).await;
    }
    let r = exec(&ex, "SELECT id FROM pf_none WHERE val = 999").await;
    let rr = rows(&r[0]);
    assert_eq!(rr.len(), 0);
}

// ------------------------------------------------------------------
// 6. Parallel SUM aggregate (large dataset)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_sum() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_sum (id INT, val INT, grp INT)").await;
    // Insert 200 rows: grp = id % 2
    for i in 1..=200 {
        exec(&ex, &format!("INSERT INTO pf_sum VALUES ({i}, {i}, {})", i % 2)).await;
    }
    // SUM of val WHERE grp = 0 → sum of even numbers 2+4+...+200 = 100*101 = 10100
    let r = exec(&ex, "SELECT SUM(val) FROM pf_sum WHERE grp = 0").await;
    let v = scalar(&r[0]);
    match v {
        Value::Int64(n) => assert_eq!(*n, 10100),
        Value::Int32(n) => assert_eq!(*n as i64, 10100),
        other => panic!("expected Int64 or Int32, got {other:?}"),
    }
}

// ------------------------------------------------------------------
// 7. Parallel COUNT (large dataset)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_count() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_cnt (id INT, cat INT)").await;
    // Insert 150 rows: cat cycles 0..2
    for i in 1..=150 {
        exec(&ex, &format!("INSERT INTO pf_cnt VALUES ({i}, {})", i % 3)).await;
    }
    // COUNT WHERE cat = 1 → 50 rows (ids 1,4,7,...,148)
    let r = exec(&ex, "SELECT COUNT(*) FROM pf_cnt WHERE cat = 1").await;
    let v = scalar(&r[0]);
    match v {
        Value::Int64(n) => assert_eq!(*n, 50),
        Value::Int32(n) => assert_eq!(*n as i64, 50),
        other => panic!("expected Int64 or Int32, got {other:?}"),
    }
}

// ------------------------------------------------------------------
// 8. Parallel AVG (large dataset)
// ------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_avg() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_avg (id INT, score INT, pass BOOLEAN)").await;
    // Insert 100 rows: pass = TRUE for even ids
    for i in 1..=100 {
        let pass = if i % 2 == 0 { "TRUE" } else { "FALSE" };
        exec(&ex, &format!("INSERT INTO pf_avg VALUES ({i}, {i}, {pass})")).await;
    }
    // AVG(score) WHERE pass = TRUE → avg of 2,4,6,...,100 = 51
    let r = exec(&ex, "SELECT AVG(score) FROM pf_avg WHERE pass = TRUE").await;
    let v = scalar(&r[0]);
    match v {
        Value::Float64(f) => assert!((f - 51.0).abs() < 0.001, "expected avg ~51, got {f}"),
        Value::Int64(n) => assert_eq!(*n, 51),
        other => panic!("expected Float64, got {other:?}"),
    }
}

// ========================================================================
// SIMD aggregate fast-path tests (plan Aggregate node — simd_aggregate)
// ========================================================================

/// SUM on DOUBLE PRECISION column uses SIMD f64 fast-path.
#[tokio::test]
async fn test_simd_aggregate_sum_f64() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sa_f64 (id INT, val DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO sa_f64 VALUES (1, 1.5), (2, 2.5), (3, 3.0), (4, 4.0)").await;
    let r = exec(&ex, "SELECT SUM(val) FROM sa_f64").await;
    let v = scalar(&r[0]);
    match v {
        Value::Float64(f) => assert!((f - 11.0).abs() < 1e-9, "expected 11.0, got {f}"),
        other => panic!("expected Float64, got {other:?}"),
    }
}

/// MIN on DOUBLE PRECISION column uses SIMD f64 min fast-path.
#[tokio::test]
async fn test_simd_aggregate_min_f64() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sa_min (id INT, score DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO sa_min VALUES (1, 5.5), (2, 1.2), (3, 9.9), (4, 3.3)").await;
    let r = exec(&ex, "SELECT MIN(score) FROM sa_min").await;
    let v = scalar(&r[0]);
    match v {
        Value::Float64(f) => assert!((f - 1.2).abs() < 1e-9, "expected 1.2, got {f}"),
        other => panic!("expected Float64, got {other:?}"),
    }
}

/// MAX on DOUBLE PRECISION column uses SIMD f64 max fast-path.
#[tokio::test]
async fn test_simd_aggregate_max_f64() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sa_max (id INT, score DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO sa_max VALUES (1, 5.5), (2, 1.2), (3, 9.9), (4, 3.3)").await;
    let r = exec(&ex, "SELECT MAX(score) FROM sa_max").await;
    let v = scalar(&r[0]);
    match v {
        Value::Float64(f) => assert!((f - 9.9).abs() < 1e-9, "expected 9.9, got {f}"),
        other => panic!("expected Float64, got {other:?}"),
    }
}

/// SUM on BIGINT column uses SIMD i64 checked fast-path.
#[tokio::test]
async fn test_simd_aggregate_sum_i64_plan() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sa_i64 (id INT, amount BIGINT)").await;
    exec(&ex, "INSERT INTO sa_i64 VALUES (1, 100), (2, 200), (3, 300), (4, 400)").await;
    let r = exec(&ex, "SELECT SUM(amount) FROM sa_i64").await;
    let v = scalar(&r[0]);
    match v {
        Value::Int64(n) => assert_eq!(*n, 1000),
        other => panic!("expected Int64, got {other:?}"),
    }
}

/// SUM on NULL-only column returns NULL via SIMD fast-path.
#[tokio::test]
async fn test_simd_aggregate_sum_all_null() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sa_null (id INT, val DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO sa_null VALUES (1, NULL), (2, NULL)").await;
    let r = exec(&ex, "SELECT SUM(val) FROM sa_null").await;
    let v = scalar(&r[0]);
    assert_eq!(*v, Value::Null, "SUM of all-NULL should be NULL");
}

/// SIMD filter in plan Filter node: subquery forces a Filter plan node.
#[tokio::test]
async fn test_simd_plan_filter_node() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pf_plan (id INT, amount BIGINT)").await;
    for i in 1i64..=20 {
        exec(&ex, &format!("INSERT INTO pf_plan VALUES ({i}, {i}00)")).await;
    }
    // Force plan-path Filter node via CTE
    let r = exec(&ex,
        "WITH base AS (SELECT id, amount FROM pf_plan) \
         SELECT id FROM base WHERE amount > 1000 ORDER BY id"
    ).await;
    let ids: Vec<i64> = rows(&r[0]).iter().map(|row| match &row[0] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        other => panic!("expected int, got {other:?}"),
    }).collect();
    // amount > 1000 → ids 11..=20
    assert_eq!(ids, (11..=20).collect::<Vec<_>>());
}
