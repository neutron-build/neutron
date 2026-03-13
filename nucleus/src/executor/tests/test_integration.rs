use super::*;

// ======================================================================
// Reactive / CDC integration tests
// ======================================================================

#[tokio::test]
async fn test_subscribe_and_unsubscribe() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT subscription_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    // Subscribe to a query watching table t1
    let res = exec(&ex, "SELECT subscribe('SELECT * FROM t1', 't1')").await;
    let sub_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert!(sub_id > 0);
    let res = exec(&ex, "SELECT subscription_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
    // Unsubscribe
    let sql = format!("SELECT unsubscribe({sub_id})");
    let res = exec(&ex, &sql).await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    let res = exec(&ex, "SELECT subscription_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_cdc_log_from_dml() {
    let ex = test_executor();
    // CDC log should be empty
    let res = exec(&ex, "SELECT cdc_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    // Create table and insert data — DML hooks should append to CDC log
    exec(&ex, "CREATE TABLE cdc_test (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO cdc_test VALUES (1, 'a')").await;
    exec(&ex, "INSERT INTO cdc_test VALUES (2, 'b')").await;
    // CDC log should have entries
    let res = exec(&ex, "SELECT cdc_count()").await;
    let count = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    assert!(count >= 2, "expected >=2 CDC entries, got {count}");
    // Read CDC log
    let res = exec(&ex, "SELECT cdc_read(0, 10)").await;
    let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
    assert!(json.contains("INSERT"), "CDC log should show INSERT: {json}");
    assert!(json.contains("cdc_test"), "CDC log should reference table: {json}");
}

#[tokio::test]
async fn test_cdc_table_read() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cdc_a (id INT)").await;
    exec(&ex, "CREATE TABLE cdc_b (id INT)").await;
    exec(&ex, "INSERT INTO cdc_a VALUES (1)").await;
    exec(&ex, "INSERT INTO cdc_b VALUES (2)").await;
    // Read only cdc_a entries
    let res = exec(&ex, "SELECT cdc_table_read('cdc_a', 0, 10)").await;
    let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
    assert!(json.contains("cdc_a"));
    assert!(!json.contains("cdc_b"), "should only have cdc_a entries: {json}");
}

#[tokio::test]
async fn test_cdc_update_and_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cdc_ud (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO cdc_ud VALUES (1, 'x')").await;
    exec(&ex, "UPDATE cdc_ud SET val = 'y' WHERE id = 1").await;
    exec(&ex, "DELETE FROM cdc_ud WHERE id = 1").await;
    let res = exec(&ex, "SELECT cdc_table_read('cdc_ud', 0, 100)").await;
    let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
    assert!(json.contains("INSERT"));
    assert!(json.contains("UPDATE"));
    assert!(json.contains("DELETE"));
}

// ======================================================================

// Graph store integration tests
// ======================================================================

#[tokio::test]
async fn test_graph_add_node_and_edge() {
    let ex = test_executor();
    // Add two nodes
    let res = exec(&ex, r#"SELECT graph_add_node('Person', '{"name":"Alice"}')"#).await;
    let alice_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("expected int, got {v:?}") };
    let res = exec(&ex, r#"SELECT graph_add_node('Person', '{"name":"Bob"}')"#).await;
    let bob_id = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("expected int, got {v:?}") };
    // Add edge
    let sql = format!("SELECT graph_add_edge({alice_id}, {bob_id}, 'KNOWS')");
    let res = exec(&ex, &sql).await;
    match scalar(&res[0]) {
        Value::Int64(_) => {} // edge ID
        v => panic!("expected int edge ID, got {v:?}"),
    }
    // Counts
    let res = exec(&ex, "SELECT graph_node_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    let res = exec(&ex, "SELECT graph_edge_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
}

#[tokio::test]
async fn test_graph_neighbors_and_shortest_path() {
    let ex = test_executor();
    // Build chain: A → B → C
    let res = exec(&ex, "SELECT graph_add_node('N')").await;
    let a = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    let res = exec(&ex, "SELECT graph_add_node('N')").await;
    let b = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    let res = exec(&ex, "SELECT graph_add_node('N')").await;
    let c = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    exec(&ex, &format!("SELECT graph_add_edge({a}, {b}, 'NEXT')")).await;
    exec(&ex, &format!("SELECT graph_add_edge({b}, {c}, 'NEXT')")).await;
    // Neighbors of A (outgoing) → B
    let res = exec(&ex, &format!("SELECT graph_neighbors({a})")).await;
    let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
    assert!(json.contains(&format!("\"neighbor_id\":{b}")));
    // Shortest path A→C
    let res = exec(&ex, &format!("SELECT graph_shortest_path({a}, {c})")).await;
    let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
    assert!(json.contains(&a.to_string()));
    assert!(json.contains(&c.to_string()));
}

#[tokio::test]
async fn test_graph_delete() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT graph_add_node('X')").await;
    let nid = match scalar(&res[0]) { Value::Int64(n) => *n, ref v => panic!("{v:?}") };
    let res = exec(&ex, &format!("SELECT graph_delete_node({nid})")).await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    // Double-delete → false
    let res = exec(&ex, &format!("SELECT graph_delete_node({nid})")).await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
    let res = exec(&ex, "SELECT graph_node_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_graph_cypher_query() {
    let ex = test_executor();
    // Create nodes via SQL functions
    exec(&ex, r#"SELECT graph_add_node('Person', '{"name":"Eve"}')"#).await;
    // Run Cypher MATCH
    let res = exec(&ex, "SELECT graph_query('MATCH (p:Person) RETURN p.name')").await;
    let json = match scalar(&res[0]) { Value::Text(s) => s.clone(), ref v => panic!("{v:?}") };
    assert!(json.contains("Eve"), "cypher should find Eve: {json}");
}

// ======================================================================

// Blob storage integration tests
// ======================================================================

#[tokio::test]
async fn test_blob_store_and_get() {
    let ex = test_executor();
    // Store a blob (hex-encoded "hello")
    let res = exec(&ex, "SELECT blob_store('myfile', '68656c6c6f', 'text/plain')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    // Retrieve it
    let res = exec(&ex, "SELECT blob_get('myfile')").await;
    assert_eq!(scalar(&res[0]), &Value::Text("68656c6c6f".into()));
    // Missing key → NULL
    let res = exec(&ex, "SELECT blob_get('nope')").await;
    assert_eq!(scalar(&res[0]), &Value::Null);
}

#[tokio::test]
async fn test_blob_delete_and_count() {
    let ex = test_executor();
    let res = exec(&ex, "SELECT blob_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    exec(&ex, "SELECT blob_store('a', 'ff', 'application/octet-stream')").await;
    exec(&ex, "SELECT blob_store('b', 'ee', 'application/octet-stream')").await;
    let res = exec(&ex, "SELECT blob_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(2));
    let res = exec(&ex, "SELECT blob_delete('a')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    let res = exec(&ex, "SELECT blob_count()").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(1));
    // Double-delete → false
    let res = exec(&ex, "SELECT blob_delete('a')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_blob_meta_and_tag() {
    let ex = test_executor();
    exec(&ex, "SELECT blob_store('img', 'cafebabe', 'image/png')").await;
    // Metadata
    let res = exec(&ex, "SELECT blob_meta('img')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(json.contains(r#""content_type":"image/png""#));
    assert!(json.contains(r#""size":4"#)); // 4 bytes decoded from cafebabe
    // Tag
    let res = exec(&ex, "SELECT blob_tag('img', 'category', 'photos')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(true));
    // Tag on missing key → false
    let res = exec(&ex, "SELECT blob_tag('nope', 'k', 'v')").await;
    assert_eq!(scalar(&res[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_blob_list_and_dedup() {
    let ex = test_executor();
    exec(&ex, "SELECT blob_store('data/a', 'aabb', 'application/octet-stream')").await;
    exec(&ex, "SELECT blob_store('data/b', 'ccdd', 'application/octet-stream')").await;
    exec(&ex, "SELECT blob_store('other', 'eeff', 'application/octet-stream')").await;
    // List all
    let res = exec(&ex, "SELECT blob_list()").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(json.contains("data/a"));
    assert!(json.contains("other"));
    // List prefix
    let res = exec(&ex, "SELECT blob_list('data/')").await;
    let json = match scalar(&res[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    assert!(json.contains("data/a"));
    assert!(json.contains("data/b"));
    assert!(!json.contains("other"));
    // Dedup ratio
    let res = exec(&ex, "SELECT blob_dedup_ratio()").await;
    match scalar(&res[0]) {
        Value::Float64(f) => assert!(*f >= 1.0),
        other => panic!("expected float, got {other:?}"),
    }
}

// ======================================================================

// Complex query tests
// ======================================================================

#[tokio::test]
async fn test_complex_query_with_subquery_and_join() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE orders (id INT, customer_id INT, amount INT)").await;
    exec(&ex, "CREATE TABLE customers (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO customers VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO customers VALUES (2, 'bob')").await;
    exec(&ex, "INSERT INTO orders VALUES (1, 1, 100)").await;
    exec(&ex, "INSERT INTO orders VALUES (2, 1, 200)").await;
    exec(&ex, "INSERT INTO orders VALUES (3, 2, 150)").await;

    // Join with aggregation
    let results = exec(&ex, "SELECT c.name, SUM(o.amount) as total FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name ORDER BY total DESC").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("alice".into()));

    // Subquery in WHERE
    let results = exec(&ex, "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 100)").await;
    let r = rows(&results[0]);
    assert!(r.len() >= 1);
}

#[tokio::test]
async fn test_cte_with_window_function() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales (region TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO sales VALUES ('east', 100)").await;
    exec(&ex, "INSERT INTO sales VALUES ('east', 200)").await;
    exec(&ex, "INSERT INTO sales VALUES ('west', 150)").await;
    exec(&ex, "INSERT INTO sales VALUES ('west', 250)").await;

    // CTE with aggregation
    let results = exec(&ex, "WITH totals AS (SELECT region, SUM(amount) as total FROM sales GROUP BY region) SELECT * FROM totals ORDER BY total DESC").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("west".into()));
}

#[tokio::test]
async fn test_nested_subquery_exists() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE dept (id INT, name TEXT)").await;
    exec(&ex, "CREATE TABLE emp (id INT, dept_id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO dept VALUES (1, 'engineering')").await;
    exec(&ex, "INSERT INTO dept VALUES (2, 'marketing')").await;
    exec(&ex, "INSERT INTO emp VALUES (1, 1, 'alice')").await;

    // EXISTS subquery
    let results = exec(&ex, "SELECT name FROM dept WHERE EXISTS (SELECT 1 FROM emp WHERE emp.dept_id = dept.id)").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Text("engineering".into()));
}

#[tokio::test]
async fn test_regexp_replace() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT REGEXP_REPLACE('hello world', 'world', 'rust')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("hello rust".into()));
}

#[tokio::test]
async fn test_age_function() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT AGE(MAKE_DATE(2024, 1, 1), MAKE_DATE(2020, 1, 1))").await;
    match scalar(&results[0]) {
        Value::Text(s) => assert!(s.contains("4 years")),
        _ => panic!("expected text"),
    }
}

#[tokio::test]
async fn test_to_timestamp() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TO_TIMESTAMP(0)").await;
    // 0 epoch seconds since 2000-01-01
    assert_eq!(*scalar(&results[0]), Value::Timestamp(0));
}

#[tokio::test]
async fn test_ends_with() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ENDS_WITH('hello.txt', '.txt')").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(true));
}

#[tokio::test]
async fn test_jsonb_strip_nulls() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSONB_STRIP_NULLS('{\"a\": 1, \"b\": null}'::JSONB)").await;
    match scalar(&results[0]) {
        Value::Jsonb(v) => {
            assert!(v.get("a").is_some());
            assert!(v.get("b").is_none());
        }
        _ => panic!("expected jsonb"),
    }
}

// ========================================================================

// Fault isolation integration tests
// ================================================================

#[tokio::test]
async fn test_health_registry_initialized() {
    let ex = test_executor();
    let health = ex.subsystem_health();
    // All subsystems should be registered and healthy.
    assert!(health.len() >= 6);
    for (name, status) in &health {
        assert_eq!(
            *status,
            SubsystemHealth::Healthy,
            "{name} should be healthy"
        );
    }
}

#[tokio::test]
async fn test_failed_vector_subsystem_blocks_vector_distance() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (id INT, v VECTOR(3))").await;
    exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,0,0]'))").await;

    // Mark vector subsystem as failed.
    {
        let mut reg = ex.health_registry.write();
        reg.mark_failed("vector", "test failure");
    }

    // VECTOR_DISTANCE should now return an error.
    let result = ex
        .execute("SELECT VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') FROM items")
        .await;
    assert!(result.is_err(), "should fail when vector subsystem is down");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("vector subsystem unavailable"),
        "got: {err_msg}"
    );
}

#[tokio::test]
async fn test_failed_fts_subsystem_blocks_ts_rank() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE docs (id INT, body TEXT)").await;
    exec(&ex, "INSERT INTO docs VALUES (1, 'hello world')").await;

    // Mark FTS as failed.
    {
        let mut reg = ex.health_registry.write();
        reg.mark_failed("fts", "index corruption");
    }

    let result = ex
        .execute("SELECT TS_RANK(body, 'hello') FROM docs")
        .await;
    assert!(result.is_err(), "should fail when fts subsystem is down");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("fts subsystem unavailable"),
        "got: {err_msg}"
    );
}

#[tokio::test]
async fn test_failed_geo_subsystem_blocks_st_distance() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE places (id INT)").await;
    exec(&ex, "INSERT INTO places VALUES (1)").await;

    // Mark geo as failed.
    {
        let mut reg = ex.health_registry.write();
        reg.mark_failed("geo", "rtree panic");
    }

    let result = ex
        .execute("SELECT ST_DISTANCE(0.0, 0.0, 1.0, 1.0) FROM places")
        .await;
    assert!(result.is_err(), "should fail when geo subsystem is down");
}

#[tokio::test]
async fn test_recovered_subsystem_works_again() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (id INT, v VECTOR(3))").await;
    exec(&ex, "INSERT INTO items VALUES (1, VECTOR('[1,0,0]'))").await;

    // Fail, then recover.
    {
        let mut reg = ex.health_registry.write();
        reg.mark_failed("vector", "temporary");
    }
    let result = ex
        .execute("SELECT VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') FROM items")
        .await;
    assert!(result.is_err());

    // Recover.
    {
        let mut reg = ex.health_registry.write();
        reg.mark_healthy("vector");
    }
    let result = ex
        .execute("SELECT VECTOR_DISTANCE(v, VECTOR('[0,1,0]'), 'l2') FROM items")
        .await;
    assert!(result.is_ok(), "should work after recovery");
}

#[tokio::test]
async fn test_run_in_subsystem_catches_panic() {
    let ex = test_executor();

    // Run something that panics inside the vector subsystem.
    let result: Result<i32, ExecError> = ex.run_in_subsystem("vector", || {
        panic!("simulated vector crash");
    });
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("panicked"), "got: {err_msg}");

    // Vector should now be marked failed.
    let reg = ex.health_registry.read();
    assert!(matches!(
        reg.status("vector"),
        Some(SubsystemHealth::Failed(_))
    ));
}

// ── Cypher SQL bridge tests ─────────────────────────────────────────

#[tokio::test]
async fn test_cypher_create_and_match_via_sql() {
    let ex = test_executor();

    // Create nodes via CYPHER() function
    let results = exec(&ex, r#"SELECT CYPHER('CREATE (a:Person {name: "Alice"})')"#).await;
    assert_eq!(results.len(), 1);

    // Create more nodes
    exec(&ex, r#"SELECT CYPHER('CREATE (b:Person {name: "Bob"})')"#).await;

    // Query them back
    let results = exec(&ex, "SELECT CYPHER('MATCH (n:Person) RETURN COUNT(*)')").await;
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            // The CYPHER function returns text with "COUNT(*)\n2"
            let text = match &rows[0][0] {
                Value::Text(s) => s.clone(),
                other => panic!("Expected text, got {other:?}"),
            };
            assert!(text.contains("2"), "expected 2 people, got: {text}");
        }
        _ => panic!("Expected Select result"),
    }
}

#[tokio::test]
async fn test_cypher_create_edge_and_traverse() {
    let ex = test_executor();

    // Create a graph with edges
    exec(&ex, r#"SELECT CYPHER('CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (a)-[:FRIENDS]->(b)')"#).await;

    // Traverse the edge
    let results = exec(
        &ex,
        "SELECT CYPHER('MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a.name, b.name')",
    ).await;
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            let text = match &rows[0][0] {
                Value::Text(s) => s.clone(),
                other => panic!("Expected text, got {other:?}"),
            };
            assert!(text.contains("Alice"), "got: {text}");
            assert!(text.contains("Bob"), "got: {text}");
        }
        _ => panic!("Expected Select result"),
    }
}

#[tokio::test]
async fn test_execute_cypher_query_direct() {
    let ex = test_executor();

    // Use the direct execute_cypher_query API
    ex.execute_cypher_query(r#"CREATE (a:City {name: "NYC"})"#).unwrap();
    ex.execute_cypher_query(r#"CREATE (b:City {name: "LA"})"#).unwrap();

    let result = ex.execute_cypher_query("MATCH (c:City) RETURN COUNT(*)").unwrap();
    match result {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns[0].0, "COUNT(*)");
            assert_eq!(rows[0][0], Value::Int64(2));
        }
        _ => panic!("Expected Select result"),
    }
}

#[tokio::test]
async fn test_cypher_with_where_clause() {
    let ex = test_executor();

    exec(&ex, r#"SELECT CYPHER('CREATE (a:Person {name: "Alice", age: 30})')"#).await;
    exec(&ex, r#"SELECT CYPHER('CREATE (b:Person {name: "Bob", age: 25})')"#).await;

    let result = ex
        .execute_cypher_query("MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
        .unwrap();
    match result {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("Alice".into()));
        }
        _ => panic!("Expected Select result"),
    }
}

#[tokio::test]
async fn test_cypher_graph_subsystem_failure() {
    let ex = test_executor();

    // Mark graph subsystem as failed.
    {
        let mut reg = ex.health_registry.write();
        reg.mark_failed("graph", "corruption detected");
    }

    let result = ex.execute_cypher_query("MATCH (n) RETURN n");
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("graph subsystem unavailable"), "got: {err}");
}

#[tokio::test]
async fn test_cypher_persistent_graph_store() {
    let ex = test_executor();

    // Create in one call, query in another — graph store persists across calls
    ex.execute_cypher_query(r#"CREATE (n:Animal {species: "Dog"})"#).unwrap();
    ex.execute_cypher_query(r#"CREATE (n:Animal {species: "Cat"})"#).unwrap();
    ex.execute_cypher_query(r#"CREATE (n:Animal {species: "Bird"})"#).unwrap();

    // Verify persistence
    let gs = ex.graph_store().read();
    assert_eq!(gs.node_count(), 3);
}

// ========================================================================

// Reactive subscription wiring tests
// ========================================================================

#[tokio::test]
async fn test_reactive_insert_notifies_subscribers() {
    let ex = test_executor();
    ex.execute("CREATE TABLE events (id INT, name TEXT)").await.unwrap();

    // Subscribe to 'events' table changes — keep rx alive
    let mut rx = {
        let mut notifier = ex.change_notifier().write();
        notifier.subscribe("events")
    };

    ex.execute("INSERT INTO events VALUES (1, 'test')").await.unwrap();

    // Verify we received the notification
    let event = rx.try_recv().unwrap();
    assert_eq!(event.table, "events");
    assert_eq!(event.change_type, crate::reactive::ChangeType::Insert);
}

#[tokio::test]
async fn test_reactive_update_notifies_subscribers() {
    let ex = test_executor();
    ex.execute("CREATE TABLE scores (id INT, val INT)").await.unwrap();
    ex.execute("INSERT INTO scores VALUES (1, 100)").await.unwrap();

    // Subscribe and capture change
    let mut rx = {
        let mut notifier = ex.change_notifier().write();
        notifier.subscribe("scores")
    };

    ex.execute("UPDATE scores SET val = 200 WHERE id = 1").await.unwrap();

    let event = rx.try_recv().unwrap();
    assert_eq!(event.table, "scores");
    assert_eq!(event.change_type, crate::reactive::ChangeType::Update);
}

#[tokio::test]
async fn test_reactive_delete_notifies_subscribers() {
    let ex = test_executor();
    ex.execute("CREATE TABLE logs (id INT, msg TEXT)").await.unwrap();
    ex.execute("INSERT INTO logs VALUES (1, 'hello')").await.unwrap();

    let mut rx = {
        let mut notifier = ex.change_notifier().write();
        notifier.subscribe("logs")
    };

    ex.execute("DELETE FROM logs WHERE id = 1").await.unwrap();

    let event = rx.try_recv().unwrap();
    assert_eq!(event.table, "logs");
    assert_eq!(event.change_type, crate::reactive::ChangeType::Delete);
}

#[tokio::test]
async fn test_reactive_no_notification_on_zero_rows() {
    let ex = test_executor();
    ex.execute("CREATE TABLE empty_tbl (id INT)").await.unwrap();

    let mut rx = {
        let mut notifier = ex.change_notifier().write();
        notifier.subscribe("empty_tbl")
    };

    // Delete from empty table — 0 rows affected, no notification
    ex.execute("DELETE FROM empty_tbl WHERE id = 1").await.unwrap();

    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_reactive_subscription_manager_wired() {
    let ex = test_executor();
    ex.execute("CREATE TABLE orders (id INT, total INT)").await.unwrap();

    // Subscribe via subscription manager
    {
        let mut mgr = ex.subscription_manager().write();
        let (id, _rx) = mgr.subscribe("SELECT * FROM orders", vec!["orders".to_string()]);
        assert!(id > 0);
        assert_eq!(mgr.active_count(), 1);

        // Check affected subscriptions
        let affected = mgr.affected_subscriptions("orders");
        assert_eq!(affected.len(), 1);
    }
}

#[tokio::test]
async fn test_reactive_unsubscribe() {
    let ex = test_executor();

    let sub_id = {
        let mut mgr = ex.subscription_manager().write();
        let (id, _rx) = mgr.subscribe("SELECT 1", vec!["t1".to_string()]);
        id
    };

    {
        let mut mgr = ex.subscription_manager().write();
        assert_eq!(mgr.active_count(), 1);
        mgr.unsubscribe(sub_id);
        assert_eq!(mgr.active_count(), 0);
    }
}

#[tokio::test]
async fn test_reactive_multiple_table_subscribers() {
    let ex = test_executor();
    ex.execute("CREATE TABLE t1 (id INT)").await.unwrap();
    ex.execute("CREATE TABLE t2 (id INT)").await.unwrap();

    let mut rx1 = {
        let mut notifier = ex.change_notifier().write();
        notifier.subscribe("t1")
    };
    let mut rx2 = {
        let mut notifier = ex.change_notifier().write();
        notifier.subscribe("t2")
    };

    ex.execute("INSERT INTO t1 VALUES (1)").await.unwrap();
    ex.execute("INSERT INTO t2 VALUES (2)").await.unwrap();

    // Only t1 subscriber gets t1 event
    let e1 = rx1.try_recv().unwrap();
    assert_eq!(e1.table, "t1");
    assert!(rx1.try_recv().is_err()); // no t2 event

    // Only t2 subscriber gets t2 event
    let e2 = rx2.try_recv().unwrap();
    assert_eq!(e2.table, "t2");
    assert!(rx2.try_recv().is_err()); // no t1 event
}

// ========================================================================

// Tier 1 integration tests — metrics, advisor, SHOW commands
// ========================================================================

#[tokio::test]
async fn test_metrics_tracking_after_queries() {
    let ex = test_executor();
    ex.execute("CREATE TABLE m (id INT)").await.unwrap();
    ex.execute("INSERT INTO m VALUES (1)").await.unwrap();
    ex.execute("INSERT INTO m VALUES (2)").await.unwrap();
    ex.execute("SELECT * FROM m").await.unwrap();
    ex.execute("UPDATE m SET id = 3 WHERE id = 1").await.unwrap();
    ex.execute("DELETE FROM m WHERE id = 2").await.unwrap();

    let m = ex.metrics();
    assert!(m.queries_total.get() >= 5);
    assert!(m.queries_select.get() >= 1);
    assert!(m.queries_insert.get() >= 2);
    assert!(m.queries_update.get() >= 1);
    assert!(m.queries_delete.get() >= 1);
    assert!(m.query_duration.count() >= 5);
}

#[tokio::test]
async fn test_show_metrics_returns_real_values() {
    let ex = test_executor();
    ex.execute("CREATE TABLE sm (id INT)").await.unwrap();
    ex.execute("INSERT INTO sm VALUES (1)").await.unwrap();

    let results = exec(&ex, "SHOW METRICS").await;
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0].0, "metric");
            // queries_total should be > 0 now
            let qt = rows.iter().find(|r| {
                matches!(&r[0], Value::Text(t) if t == "nucleus_queries_total")
            }).unwrap();
            // Value should not be "0" since we ran queries
            let val = match &qt[2] { Value::Text(t) => t.clone(), _ => "0".into() };
            assert_ne!(val, "0");
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_show_index_recommendations() {
    let ex = test_executor();
    let results = exec(&ex, "SHOW INDEX_RECOMMENDATIONS").await;
    match &results[0] {
        ExecResult::Select { columns, rows: _ } => {
            assert_eq!(columns.len(), 6);
            assert_eq!(columns[0].0, "table");
            assert_eq!(columns[1].0, "columns");
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_show_replication_status() {
    let ex = test_executor();
    let results = exec(&ex, "SHOW REPLICATION_STATUS").await;
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert!(rows.len() >= 3);
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_show_subsystem_health() {
    let ex = test_executor();
    let results = exec(&ex, "SHOW SUBSYSTEM_HEALTH").await;
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].0, "subsystem");
            assert!(rows.len() >= 4); // vector, fts, geo, timeseries, storage, graph
            // All should be healthy
            for row in rows {
                match &row[1] {
                    Value::Text(s) => assert_eq!(s, "healthy"),
                    _ => panic!("expected text"),
                }
            }
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_metrics_rows_returned_counted() {
    let ex = test_executor();
    ex.execute("CREATE TABLE rc (id INT)").await.unwrap();
    ex.execute("INSERT INTO rc VALUES (1)").await.unwrap();
    ex.execute("INSERT INTO rc VALUES (2)").await.unwrap();
    ex.execute("INSERT INTO rc VALUES (3)").await.unwrap();

    let before = ex.metrics().rows_returned.get();
    ex.execute("SELECT * FROM rc").await.unwrap();
    let after = ex.metrics().rows_returned.get();
    assert!(after >= before + 3);
}

#[tokio::test]
async fn test_shared_metrics_registry() {
    let shared = Arc::new(crate::metrics::MetricsRegistry::new());
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    let ex = Executor::new(catalog, storage).with_metrics(shared.clone());

    ex.execute("CREATE TABLE shr (id INT)").await.unwrap();
    ex.execute("INSERT INTO shr VALUES (1)").await.unwrap();

    // The shared registry should have the counts
    assert!(shared.queries_total.get() >= 2);
}

#[tokio::test]
async fn test_subscribe_returns_subscription_id() {
    let ex = test_executor();
    ex.execute("CREATE TABLE orders (id INT, status TEXT)").await.unwrap();

    let results = ex.execute("SUBSCRIBE SELECT * FROM orders WHERE status = 'pending'").await.unwrap();
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns[0].0, "subscription_id");
            assert_eq!(columns[1].0, "query");
            assert_eq!(columns[2].0, "tables");
            assert_eq!(rows.len(), 1);
            // Subscription ID should be a positive integer
            match &rows[0][0] {
                Value::Int64(id) => assert!(*id > 0),
                _ => panic!("expected Int64"),
            }
            // Tables should contain "orders"
            match &rows[0][2] {
                Value::Text(t) => assert!(t.contains("orders")),
                _ => panic!("expected Text"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_unsubscribe_removes_subscription() {
    let ex = test_executor();
    ex.execute("CREATE TABLE items (id INT)").await.unwrap();

    let results = ex.execute("SUBSCRIBE SELECT * FROM items").await.unwrap();
    let sub_id = match &results[0] {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(id) => *id,
            _ => panic!("expected Int64"),
        },
        _ => panic!("expected select"),
    };

    let results = ex.execute(&format!("UNSUBSCRIBE {sub_id}")).await.unwrap();
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "UNSUBSCRIBE"),
        _ => panic!("expected command"),
    }
}

#[tokio::test]
async fn test_subscribe_quoted_query() {
    let ex = test_executor();
    ex.execute("CREATE TABLE events (id INT)").await.unwrap();

    let results = ex.execute("SUBSCRIBE 'SELECT * FROM events'").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0][2] {
                Value::Text(t) => assert!(t.contains("events")),
                _ => panic!("expected Text"),
            }
        }
        _ => panic!("expected select"),
    }
}

// ========================================================================
// FETCH SUBSCRIPTION — reactive diff polling
// ========================================================================

#[tokio::test]
async fn test_fetch_subscription_empty() {
    let ex = test_executor();
    ex.execute("CREATE TABLE feed (id INT, val TEXT)").await.unwrap();

    let sub_results = ex.execute("SUBSCRIBE SELECT * FROM feed").await.unwrap();
    let sub_id = match &sub_results[0] {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(id) => *id,
            _ => panic!("expected Int64"),
        },
        _ => panic!("expected select"),
    };

    // No DML yet — fetch returns no rows
    let results = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id}")).await.unwrap();
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns[0].0, "subscription_id");
            assert_eq!(columns[1].0, "added");
            assert_eq!(columns[2].0, "removed");
            assert!(rows.is_empty());
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_fetch_subscription_after_insert() {
    let ex = test_executor();
    ex.execute("CREATE TABLE products (id INT, name TEXT)").await.unwrap();

    let sub_results = ex.execute("SUBSCRIBE SELECT * FROM products").await.unwrap();
    let sub_id = match &sub_results[0] {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(id) => *id,
            _ => panic!("expected Int64"),
        },
        _ => panic!("expected select"),
    };

    ex.execute("INSERT INTO products VALUES (1, 'widget')").await.unwrap();
    ex.execute("INSERT INTO products VALUES (2, 'gadget')").await.unwrap();

    let results = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id}")).await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            // Two inserts → two diff entries
            assert_eq!(rows.len(), 2);
            // Each row: subscription_id, added (JSON), removed (JSON)
            match &rows[0][0] {
                Value::Int64(id) => assert_eq!(*id, sub_id),
                _ => panic!("expected Int64 subscription_id"),
            }
            // The added field should be a JSON array (non-empty)
            match &rows[0][1] {
                Value::Text(json) => assert!(!json.is_empty() && json != "[]"),
                _ => panic!("expected Text for added"),
            }
        }
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_fetch_subscription_drains_buffer() {
    let ex = test_executor();
    ex.execute("CREATE TABLE logs (id INT)").await.unwrap();

    let sub_results = ex.execute("SUBSCRIBE SELECT * FROM logs").await.unwrap();
    let sub_id = match &sub_results[0] {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(id) => *id,
            _ => panic!("expected Int64"),
        },
        _ => panic!("expected select"),
    };

    ex.execute("INSERT INTO logs VALUES (1)").await.unwrap();
    ex.execute("INSERT INTO logs VALUES (2)").await.unwrap();

    // First fetch returns 2 diffs
    let r1 = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id}")).await.unwrap();
    let count1 = match &r1[0] {
        ExecResult::Select { rows, .. } => rows.len(),
        _ => panic!("expected select"),
    };
    assert_eq!(count1, 2);

    // Second fetch returns 0 — buffer drained
    let r2 = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id}")).await.unwrap();
    match &r2[0] {
        ExecResult::Select { rows, .. } => assert!(rows.is_empty()),
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_fetch_subscription_with_limit() {
    let ex = test_executor();
    ex.execute("CREATE TABLE events (id INT)").await.unwrap();

    let sub_results = ex.execute("SUBSCRIBE SELECT * FROM events").await.unwrap();
    let sub_id = match &sub_results[0] {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(id) => *id,
            _ => panic!("expected Int64"),
        },
        _ => panic!("expected select"),
    };

    for i in 1..=5 {
        ex.execute(&format!("INSERT INTO events VALUES ({i})")).await.unwrap();
    }

    // Fetch only 2
    let r = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id} LIMIT 2")).await.unwrap();
    match &r[0] {
        ExecResult::Select { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected select"),
    }

    // 3 remain
    let r2 = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id}")).await.unwrap();
    match &r2[0] {
        ExecResult::Select { rows, .. } => assert_eq!(rows.len(), 3),
        _ => panic!("expected select"),
    }
}

#[tokio::test]
async fn test_fetch_subscription_delete_produces_removed_rows() {
    let ex = test_executor();
    ex.execute("CREATE TABLE items (id INT, name TEXT)").await.unwrap();
    ex.execute("INSERT INTO items VALUES (1, 'alpha')").await.unwrap();

    let sub_results = ex.execute("SUBSCRIBE SELECT * FROM items").await.unwrap();
    let sub_id = match &sub_results[0] {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(id) => *id,
            _ => panic!("expected Int64"),
        },
        _ => panic!("expected select"),
    };

    ex.execute("DELETE FROM items WHERE id = 1").await.unwrap();

    let r = ex.execute(&format!("FETCH SUBSCRIPTION {sub_id}")).await.unwrap();
    match &r[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            // removed should be non-empty JSON
            match &rows[0][2] {
                Value::Text(json) => assert!(!json.is_empty() && json != "[]"),
                _ => panic!("expected Text for removed"),
            }
        }
        _ => panic!("expected select"),
    }
}

// ========================================================================

// Cache SQL function tests (Tier 3.6)
// ========================================================================

#[tokio::test]
async fn test_cache_set_and_get() {
    let ex = test_executor();
    let results = ex.execute("CACHE_SET('mykey', 'myvalue')").await.unwrap();
    match &results[0] {
        ExecResult::Command { tag, rows_affected } => {
            assert_eq!(tag, "CACHE_SET");
            assert_eq!(*rows_affected, 1);
        }
        _ => panic!("expected Command"),
    }
    let results = ex.execute("CACHE_GET('mykey')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Text("myvalue".into()));
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_get_missing_key() {
    let ex = test_executor();
    let results = ex.execute("CACHE_GET('nonexistent')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Null);
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_set_with_ttl() {
    let ex = test_executor();
    ex.execute("CACHE_SET('ttlkey', 'ttlvalue', 300)").await.unwrap();
    let results = ex.execute("CACHE_TTL('ttlkey')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            match &rows[0][0] {
                Value::Float64(secs) => assert!(*secs > 290.0 && *secs <= 300.0),
                _ => panic!("expected Float64"),
            }
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_del() {
    let ex = test_executor();
    ex.execute("CACHE_SET('delme', 'val')").await.unwrap();
    let results = ex.execute("CACHE_DEL('delme')").await.unwrap();
    match &results[0] {
        ExecResult::Command { tag, rows_affected } => {
            assert_eq!(tag, "CACHE_DEL");
            assert_eq!(*rows_affected, 1);
        }
        _ => panic!("expected Command"),
    }
    // Should be gone now
    let results = ex.execute("CACHE_GET('delme')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Null);
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_del_nonexistent() {
    let ex = test_executor();
    let results = ex.execute("CACHE_DEL('ghost')").await.unwrap();
    match &results[0] {
        ExecResult::Command { rows_affected, .. } => {
            assert_eq!(*rows_affected, 0);
        }
        _ => panic!("expected Command"),
    }
}

#[tokio::test]
async fn test_cache_ttl_no_ttl_key() {
    let ex = test_executor();
    ex.execute("CACHE_SET('noexpiry', 'val')").await.unwrap();
    let results = ex.execute("CACHE_TTL('noexpiry')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Null);
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_stats() {
    let ex = test_executor();
    ex.execute("CACHE_SET('a', '1')").await.unwrap();
    ex.execute("CACHE_SET('b', '2')").await.unwrap();
    ex.execute("CACHE_GET('a')").await.unwrap();
    ex.execute("CACHE_GET('miss')").await.unwrap();
    let results = ex.execute("CACHE_STATS").await.unwrap();
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns[0].0, "metric");
            assert_eq!(columns[1].0, "value");
            // Should have 6 metric rows
            assert_eq!(rows.len(), 6);
            // entry_count = 2
            assert_eq!(rows[0][1], Value::Text("2".into()));
            // hits = 1
            assert_eq!(rows[3][1], Value::Text("1".into()));
            // misses = 1
            assert_eq!(rows[4][1], Value::Text("1".into()));
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_overwrite() {
    let ex = test_executor();
    ex.execute("CACHE_SET('k', 'v1')").await.unwrap();
    ex.execute("CACHE_SET('k', 'v2')").await.unwrap();
    let results = ex.execute("CACHE_GET('k')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Text("v2".into()));
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_show_cache_stats() {
    let ex = test_executor();
    let results = exec(&ex, "SHOW CACHE_STATS").await;
    match &results[0] {
        ExecResult::Select { columns, rows } => {
            assert_eq!(columns[0].0, "metric");
            assert!(rows.len() >= 6);
        }
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_cache_paren_syntax() {
    let ex = test_executor();
    ex.execute("CACHE_SET('p1', 'pval')").await.unwrap();
    let results = ex.execute("CACHE_GET('p1')").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], Value::Text("pval".into()));
        }
        _ => panic!("expected Select"),
    }
}

// ── Append-only table tests ─────────────────────────────────────

#[tokio::test]
async fn test_append_only_create_and_insert() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE audit_log (id INT, event TEXT) WITH (append_only = true)").await;
    exec(&ex, "INSERT INTO audit_log VALUES (1, 'login')").await;
    exec(&ex, "INSERT INTO audit_log VALUES (2, 'logout')").await;
    let results = ex.execute("SELECT * FROM audit_log").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!("expected Select"),
    }
}

#[tokio::test]
async fn test_append_only_rejects_update() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE audit_log2 (id INT, event TEXT) WITH (append_only = true)").await;
    exec(&ex, "INSERT INTO audit_log2 VALUES (1, 'login')").await;
    let err = ex.execute("UPDATE audit_log2 SET event = 'changed' WHERE id = 1").await;
    assert!(err.is_err(), "UPDATE should fail on append-only table");
    let msg = format!("{:?}", err.unwrap_err());
    assert!(msg.contains("append-only"), "error should mention append-only: {msg}");
}

#[tokio::test]
async fn test_append_only_rejects_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE audit_log3 (id INT, event TEXT) WITH (append_only = true)").await;
    exec(&ex, "INSERT INTO audit_log3 VALUES (1, 'login')").await;
    let err = ex.execute("DELETE FROM audit_log3 WHERE id = 1").await;
    assert!(err.is_err(), "DELETE should fail on append-only table");
    let msg = format!("{:?}", err.unwrap_err());
    assert!(msg.contains("append-only"), "error should mention append-only: {msg}");
}

#[tokio::test]
async fn test_non_append_only_allows_update_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE normal_table (id INT, val TEXT)").await;
    exec(&ex, "INSERT INTO normal_table VALUES (1, 'a')").await;
    // UPDATE should succeed on normal table
    ex.execute("UPDATE normal_table SET val = 'b' WHERE id = 1").await.unwrap();
    // DELETE should succeed on normal table
    ex.execute("DELETE FROM normal_table WHERE id = 1").await.unwrap();
}

#[tokio::test]
async fn test_append_only_with_options_false() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE not_append (id INT) WITH (append_only = false)").await;
    exec(&ex, "INSERT INTO not_append VALUES (1)").await;
    // Should allow UPDATE and DELETE since append_only = false
    ex.execute("UPDATE not_append SET id = 2 WHERE id = 1").await.unwrap();
    ex.execute("DELETE FROM not_append WHERE id = 2").await.unwrap();
}

#[tokio::test]
async fn test_append_only_multiple_inserts() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE events (id INT, ts TEXT) WITH (append_only = true)").await;
    for i in 1..=10 {
        exec(&ex, &format!("INSERT INTO events VALUES ({i}, 'event_{i}')")).await;
    }
    let results = ex.execute("SELECT * FROM events").await.unwrap();
    match &results[0] {
        ExecResult::Select { rows, .. } => assert_eq!(rows.len(), 10),
        _ => panic!("expected Select"),
    }
}

// ======================================================================

// Per-table columnar routing tests (Sprint 3)
// ======================================================================

#[tokio::test]
async fn test_columnar_table_create_insert_scan() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE analytics (id INT, amount FLOAT) WITH (engine = 'columnar')").await;
    exec(&ex, "INSERT INTO analytics VALUES (1, 100.0)").await;
    exec(&ex, "INSERT INTO analytics VALUES (2, 200.0)").await;
    let results = exec(&ex, "SELECT * FROM analytics").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_columnar_table_count_fast_path() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE col_tbl (x INT, y FLOAT) WITH (engine = 'columnar')").await;
    for i in 1..=50 {
        exec(&ex, &format!("INSERT INTO col_tbl VALUES ({i}, {}.0)", i * 2)).await;
    }
    let results = exec(&ex, "SELECT COUNT(*) FROM col_tbl").await;
    let v = scalar(&results[0]);
    assert_eq!(*v, Value::Int64(50));
}

#[tokio::test]
async fn test_columnar_table_sum_fast_path() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE col_sum (id INT, val FLOAT) WITH (engine = 'columnar')").await;
    exec(&ex, "INSERT INTO col_sum VALUES (1, 10.0), (2, 20.0), (3, 30.0)").await;
    let results = exec(&ex, "SELECT SUM(val) FROM col_sum").await;
    let v = scalar(&results[0]);
    assert!(matches!(v, Value::Float64(f) if (*f - 60.0).abs() < 1e-9));
}

#[tokio::test]
async fn test_columnar_and_regular_tables_coexist() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE regular (id INT, name TEXT)").await;
    exec(&ex, "CREATE TABLE columnar (id INT, name TEXT) WITH (engine = 'columnar')").await;
    exec(&ex, "INSERT INTO regular VALUES (1, 'alice')").await;
    exec(&ex, "INSERT INTO columnar VALUES (2, 'bob')").await;
    let r1 = exec(&ex, "SELECT * FROM regular").await;
    let r2 = exec(&ex, "SELECT * FROM columnar").await;
    assert_eq!(rows(&r1[0]).len(), 1);
    assert_eq!(rows(&r2[0]).len(), 1);
    // Each table is isolated — regular doesn't see columnar's row and vice versa
    assert_eq!(rows(&r1[0])[0][1], Value::Text("alice".into()));
    assert_eq!(rows(&r2[0])[0][1], Value::Text("bob".into()));
}

#[tokio::test]
async fn test_columnar_table_drop() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE to_drop (x INT) WITH (engine = 'columnar')").await;
    exec(&ex, "INSERT INTO to_drop VALUES (1)").await;
    exec(&ex, "DROP TABLE to_drop").await;
    // Table should be gone from catalog
    let result = ex.execute("SELECT * FROM to_drop").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_columnar_table_delete() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE col_del (id INT, v FLOAT) WITH (engine = 'columnar')").await;
    exec(&ex, "INSERT INTO col_del VALUES (1, 1.0), (2, 2.0), (3, 3.0)").await;
    exec(&ex, "DELETE FROM col_del WHERE id = 2").await;
    let results = exec(&ex, "SELECT COUNT(*) FROM col_del").await;
    let v = scalar(&results[0]);
    assert_eq!(*v, Value::Int64(2));
}

#[tokio::test]
async fn test_columnar_group_by_fast_path() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE col_grp (status TEXT, amount FLOAT) WITH (engine = 'columnar')").await;
    exec(&ex, "INSERT INTO col_grp VALUES ('a', 10.0), ('b', 20.0), ('a', 30.0)").await;
    let results = exec(&ex, "SELECT status, COUNT(*) FROM col_grp GROUP BY status").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

// ======================================================================

// Per-table LSM engine routing tests
// ======================================================================

#[tokio::test]
async fn test_lsm_table_create_insert_scan() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE events (id INT, msg TEXT) WITH (engine = 'lsm')").await;
    exec(&ex, "INSERT INTO events VALUES (1, 'hello')").await;
    exec(&ex, "INSERT INTO events VALUES (2, 'world')").await;
    let results = exec(&ex, "SELECT * FROM events").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_lsm_table_count() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lsm_tbl (x INT) WITH (engine = 'lsm')").await;
    for i in 1..=20 {
        exec(&ex, &format!("INSERT INTO lsm_tbl VALUES ({i})")).await;
    }
    let results = exec(&ex, "SELECT COUNT(*) FROM lsm_tbl").await;
    let v = scalar(&results[0]);
    assert_eq!(*v, Value::Int64(20));
}

#[tokio::test]
async fn test_lsm_and_regular_coexist() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE regular (id INT)").await;
    exec(&ex, "CREATE TABLE lsm_data (id INT) WITH (engine = 'lsm')").await;
    exec(&ex, "INSERT INTO regular VALUES (1)").await;
    exec(&ex, "INSERT INTO lsm_data VALUES (2)").await;
    let r1 = exec(&ex, "SELECT * FROM regular").await;
    let r2 = exec(&ex, "SELECT * FROM lsm_data").await;
    assert_eq!(rows(&r1[0]).len(), 1);
    assert_eq!(rows(&r2[0]).len(), 1);
}

// ======================================================================

// ORM-compatibility: typed literals, gen_random_uuid, NOW() type
// ======================================================================

#[tokio::test]
async fn test_typed_timestamp_literal() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TIMESTAMP '2024-03-15 12:30:00'").await;
    match scalar(&results[0]) {
        Value::Timestamp(us) => {
            // 2024-03-15 12:30:00 — verify the year comes out right when displayed
            let s = Value::Timestamp(*us).to_string();
            assert!(s.starts_with("2024-03-15"), "got: {s}");
        }
        other => panic!("expected Timestamp, got {other:?}"),
    }
}

#[tokio::test]
async fn test_typed_date_literal() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE '2024-03-15'").await;
    match scalar(&results[0]) {
        Value::Date(d) => {
            let s = Value::Date(*d).to_string();
            assert_eq!(s, "2024-03-15");
        }
        other => panic!("expected Date, got {other:?}"),
    }
}

#[tokio::test]
async fn test_typed_uuid_literal() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT UUID '550e8400-e29b-41d4-a716-446655440000'").await;
    match scalar(&results[0]) {
        Value::Uuid(bytes) => {
            let s = Value::Uuid(*bytes).to_string();
            assert_eq!(s, "550e8400-e29b-41d4-a716-446655440000");
        }
        other => panic!("expected Uuid, got {other:?}"),
    }
}

#[tokio::test]
async fn test_gen_random_uuid() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT gen_random_uuid()").await;
    match scalar(&results[0]) {
        Value::Uuid(bytes) => {
            // Version 4: bits 6 of byte 6 should be 0x40 mask
            assert_eq!(bytes[6] & 0xf0, 0x40, "UUID version should be 4");
            // Variant: bits of byte 8 should be 10xxxxxx
            assert_eq!(bytes[8] & 0xc0, 0x80, "UUID variant should be RFC 4122");
            // Two calls should produce different UUIDs
            let r2 = exec(&ex, "SELECT gen_random_uuid()").await;
            match scalar(&r2[0]) {
                Value::Uuid(b2) => assert_ne!(bytes, b2, "UUIDs should differ"),
                other => panic!("expected Uuid, got {other:?}"),
            }
        }
        other => panic!("expected Uuid, got {other:?}"),
    }
}

#[tokio::test]
async fn test_now_returns_timestamptz() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT NOW()").await;
    assert!(matches!(scalar(&results[0]), Value::TimestampTz(_)));
}

#[tokio::test]
async fn test_current_date_returns_date() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CURRENT_DATE()").await;
    match scalar(&results[0]) {
        Value::Date(d) => {
            // Should be a date in 2025 or later
            let (y, _, _) = crate::types::days_to_ymd(*d);
            assert!(y >= 2025, "year should be >= 2025, got {y}");
        }
        other => panic!("expected Date, got {other:?}"),
    }
}

#[tokio::test]
async fn test_uuid_column_insert_and_select() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE uuidtest (id UUID, name TEXT)").await;
    exec(&ex, "INSERT INTO uuidtest VALUES (gen_random_uuid(), 'alice')").await;
    exec(&ex, "INSERT INTO uuidtest VALUES (gen_random_uuid(), 'bob')").await;
    let results = exec(&ex, "SELECT id, name FROM uuidtest").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    // All IDs should be UUIDs
    for row in r.iter() {
        assert!(matches!(row[0], Value::Uuid(_)), "id should be Uuid, got {:?}", row[0]);
    }
    // IDs should be distinct
    assert_ne!(r[0][0], r[1][0], "UUIDs should be distinct");
}

#[tokio::test]
async fn test_timestamp_column_with_now_default() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE tstest (id INT, created_at TIMESTAMPTZ DEFAULT NOW())").await;
    exec(&ex, "INSERT INTO tstest (id) VALUES (1)").await;
    let results = exec(&ex, "SELECT id, created_at FROM tstest").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][1], Value::TimestampTz(_)), "created_at should be TimestampTz");
}

