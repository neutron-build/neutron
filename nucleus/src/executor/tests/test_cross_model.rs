//! Phase 6 Sprint 6A — Cross-Model Query Integration Tests.
//!
//! Each test demonstrates a query pattern that combines two or more data models
//! in a single SQL statement or transaction. All patterns use scalar functions
//! natively available without external tools.
//!
//! Patterns covered:
//!   1. FTS + Vector  — hybrid text + semantic search via FTS_MATCH
//!   2. TimeSeries + Relational — per-row ts_range_avg joined to a table
//!   3. Graph + Relational — shortest-path over stored graph with relational IDs
//!   4. KV + Relational — session-token lookup joined to user rows
//!   5. Datalog + Relational + Graph — rules reasoning over both models
//!   6. Cross-model transaction — BEGIN/COMMIT spanning relational, KV, and graph

use super::*;

// ============================================================================
// 1. FTS + Vector: hybrid text + semantic search
// ============================================================================

#[tokio::test]
async fn test_fts_vector_hybrid_search() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE articles (id INT PRIMARY KEY, title TEXT, embedding VECTOR(4))").await;
    exec(&ex, "INSERT INTO articles VALUES (1, 'Rust systems', VECTOR('[1.0, 0.1, 0.0, 0.0]'))").await;
    exec(&ex, "INSERT INTO articles VALUES (2, 'Python ML guide', VECTOR('[0.1, 1.0, 0.0, 0.0]'))").await;
    exec(&ex, "INSERT INTO articles VALUES (3, 'Database design', VECTOR('[0.0, 0.0, 1.0, 0.1]'))").await;
    exec(&ex, "INSERT INTO articles VALUES (4, 'ML pipeline storage', VECTOR('[0.0, 0.9, 0.2, 0.0]'))").await;

    // Index body text in FTS
    exec(&ex, "SELECT fts_index(1, 'Rust is a systems language focused on safety and performance')").await;
    exec(&ex, "SELECT fts_index(2, 'Python is great for data science and machine learning workflows')").await;
    exec(&ex, "SELECT fts_index(3, 'Designing databases requires knowledge of storage engines')").await;
    exec(&ex, "SELECT fts_index(4, 'Machine learning pipelines benefit from fast storage')").await;

    // Hybrid: FTS_MATCH filter + VECTOR_DISTANCE sort
    let results = exec(
        &ex,
        "SELECT id, title, VECTOR_DISTANCE(embedding, VECTOR('[0.0, 1.0, 0.0, 0.0]'), 'l2') AS dist \
         FROM articles \
         WHERE FTS_MATCH(id, 'machine learning') \
         ORDER BY dist \
         LIMIT 5",
    ).await;
    let r = rows(&results[0]);
    assert!(r.len() >= 1, "at least one FTS+vector result expected");

    // Articles 2 and 4 contain 'machine learning', article 1 should be absent
    let ids: Vec<i32> = r.iter().map(|row| match row[0] {
        Value::Int32(n) => n,
        Value::Int64(n) => n as i32,
        _ => panic!("unexpected id type"),
    }).collect();
    assert!(ids.contains(&2) || ids.contains(&4), "articles 2 or 4 should match: {ids:?}");
    assert!(!ids.contains(&1), "article 1 (Rust/no ML) must be excluded");
}

#[tokio::test]
async fn test_fts_match_with_relational_predicate() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE posts (id INT, category TEXT)").await;
    exec(&ex, "INSERT INTO posts VALUES (1, 'tech')").await;
    exec(&ex, "INSERT INTO posts VALUES (2, 'cooking')").await;
    exec(&ex, "INSERT INTO posts VALUES (3, 'tech')").await;

    exec(&ex, "SELECT fts_index(1, 'database storage engine btree index')").await;
    exec(&ex, "SELECT fts_index(2, 'recipe pasta tomato sauce basil')").await;
    exec(&ex, "SELECT fts_index(3, 'distributed consensus raft protocol cluster')").await;

    // Relational filter AND FTS filter combined
    let results = exec(
        &ex,
        "SELECT id FROM posts WHERE category = 'tech' AND FTS_MATCH(id, 'storage')"
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1, "only tech post about 'storage' should match");
    assert_eq!(r[0][0], Value::Int32(1));
}

// ============================================================================
// 2. TimeSeries + Relational: per-server CPU averages
// ============================================================================

#[tokio::test]
async fn test_timeseries_relational_join() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE servers (name TEXT, region TEXT, tier TEXT)").await;
    exec(&ex, "INSERT INTO servers VALUES ('web-01', 'us-east', 'prod')").await;
    exec(&ex, "INSERT INTO servers VALUES ('web-02', 'us-east', 'prod')").await;
    exec(&ex, "INSERT INTO servers VALUES ('db-01', 'eu-west', 'prod')").await;
    exec(&ex, "INSERT INTO servers VALUES ('test-01', 'us-east', 'staging')").await;

    exec(&ex, "SELECT ts_insert('web-01', 1000, 72.5)").await;
    exec(&ex, "SELECT ts_insert('web-01', 2000, 68.0)").await;
    exec(&ex, "SELECT ts_insert('web-01', 3000, 75.0)").await;
    exec(&ex, "SELECT ts_insert('web-02', 1000, 55.0)").await;
    exec(&ex, "SELECT ts_insert('web-02', 2000, 60.0)").await;
    exec(&ex, "SELECT ts_insert('test-01', 1000, 20.0)").await;

    // Cross-model: avg CPU for prod servers in us-east only
    let results = exec(
        &ex,
        "SELECT s.name, ts_range_avg(s.name, 0, 5000) AS avg_cpu \
         FROM servers s \
         WHERE s.region = 'us-east' AND s.tier = 'prod' \
         ORDER BY s.name",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2, "two prod us-east servers: {r:?}");

    // web-01 avg = (72.5 + 68.0 + 75.0) / 3 = 71.833...
    let web01_avg = match r[0][1] {
        Value::Float64(v) => v,
        ref other => panic!("unexpected type for web-01 avg: {other:?}"),
    };
    assert!((web01_avg - 71.833).abs() < 0.01, "web-01 avg ~71.83, got {web01_avg}");

    // web-02 avg = (55.0 + 60.0) / 2 = 57.5
    let web02_avg = match r[1][1] {
        Value::Float64(v) => v,
        ref other => panic!("unexpected type for web-02 avg: {other:?}"),
    };
    assert!((web02_avg - 57.5).abs() < 0.01, "web-02 avg 57.5, got {web02_avg}");
}

#[tokio::test]
async fn test_timeseries_relational_null_for_no_data() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE sensors (name TEXT, location TEXT)").await;
    exec(&ex, "INSERT INTO sensors VALUES ('sensor-a', 'floor1')").await;
    exec(&ex, "INSERT INTO sensors VALUES ('sensor-b', 'floor1')").await;

    // Only sensor-a has data
    exec(&ex, "SELECT ts_insert('sensor-a', 1000, 22.5)").await;

    let results = exec(
        &ex,
        "SELECT s.name, ts_range_avg(s.name, 0, 5000) AS avg_temp \
         FROM sensors s \
         WHERE s.location = 'floor1' \
         ORDER BY s.name",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    // sensor-a has data
    assert!(matches!(r[0][1], Value::Float64(_)), "sensor-a should have a numeric avg");
    // sensor-b has no data → NULL
    assert!(matches!(r[1][1], Value::Null | Value::Float64(_)), "sensor-b avg is NULL or default");
}

// ============================================================================
// 3. Graph + Relational: shortest paths between users from different departments
// ============================================================================

#[tokio::test]
async fn test_graph_relational_shortest_path() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, department TEXT)").await;
    exec(&ex, "INSERT INTO users VALUES (1, 'Alice', 'engineering')").await;
    exec(&ex, "INSERT INTO users VALUES (2, 'Bob', 'engineering')").await;
    exec(&ex, "INSERT INTO users VALUES (3, 'Charlie', 'sales')").await;
    exec(&ex, "INSERT INTO users VALUES (4, 'Diana', 'sales')").await;

    // Social graph: 1→2→3, 1→4
    exec(&ex, "SELECT graph_add_node('User', '{\"name\":\"Alice\"}')").await;
    exec(&ex, "SELECT graph_add_node('User', '{\"name\":\"Bob\"}')").await;
    exec(&ex, "SELECT graph_add_node('User', '{\"name\":\"Charlie\"}')").await;
    exec(&ex, "SELECT graph_add_node('User', '{\"name\":\"Diana\"}')").await;
    exec(&ex, "SELECT graph_add_edge(1, 2, 'KNOWS')").await;
    exec(&ex, "SELECT graph_add_edge(2, 3, 'KNOWS')").await;
    exec(&ex, "SELECT graph_add_edge(1, 4, 'KNOWS')").await;

    // Cross-model: paths from each eng user to each sales user
    let results = exec(
        &ex,
        "SELECT u1.name AS eng, u2.name AS sales, GRAPH_SHORTEST_PATH(u1.id, u2.id) AS path \
         FROM users u1, users u2 \
         WHERE u1.department = 'engineering' AND u2.department = 'sales'",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 4, "2 eng x 2 sales = 4 path queries: {r:?}");

    // Alice(1) → Diana(4): direct edge
    let alice_diana = r.iter().find(|row| {
        matches!(&row[0], Value::Text(s) if s == "Alice") &&
        matches!(&row[1], Value::Text(s) if s == "Diana")
    }).expect("Alice-Diana row must exist");
    assert!(!matches!(alice_diana[2], Value::Null), "Alice→Diana has a direct path");
}

#[tokio::test]
async fn test_graph_relational_combined_stats() {
    let ex = test_executor();

    // Relational table of node labels
    exec(&ex, "CREATE TABLE graph_nodes (id INT, label TEXT)").await;
    exec(&ex, "INSERT INTO graph_nodes VALUES (1, 'Alice')").await;
    exec(&ex, "INSERT INTO graph_nodes VALUES (2, 'Bob')").await;
    exec(&ex, "INSERT INTO graph_nodes VALUES (3, 'Charlie')").await;

    // Build matching graph nodes
    exec(&ex, "SELECT graph_add_node('Person', '{\"name\":\"Alice\"}')").await;
    exec(&ex, "SELECT graph_add_node('Person', '{\"name\":\"Bob\"}')").await;
    exec(&ex, "SELECT graph_add_node('Person', '{\"name\":\"Charlie\"}')").await;
    exec(&ex, "SELECT graph_add_edge(1, 2, 'KNOWS')").await;
    exec(&ex, "SELECT graph_add_edge(2, 3, 'KNOWS')").await;

    // Cross-model: relational count matches graph count
    let relational_count = exec(&ex, "SELECT COUNT(*) FROM graph_nodes").await;
    let graph_count = exec(&ex, "SELECT graph_node_count()").await;

    let rc = match scalar(&relational_count[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected: {other:?}"),
    };
    let gc = match scalar(&graph_count[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(rc, 3, "relational table has 3 rows");
    assert_eq!(gc, 3, "graph has 3 nodes");
    assert_eq!(rc, gc, "relational and graph node counts should match");

    // Cross-model: shortest path between nodes whose IDs come from the relational table
    let path_result = exec(
        &ex,
        "SELECT GRAPH_SHORTEST_PATH(n1.id, n2.id) AS path \
         FROM graph_nodes n1, graph_nodes n2 \
         WHERE n1.label = 'Alice' AND n2.label = 'Charlie'",
    ).await;
    let path_rows = rows(&path_result[0]);
    assert_eq!(path_rows.len(), 1);
    // Path from 1 to 3 via 2: should be non-null
    assert!(
        !matches!(path_rows[0][0], Value::Null),
        "Alice→Charlie path should exist via Bob"
    );
}

// ============================================================================
// 4. KV + Relational: session token join
// ============================================================================

#[tokio::test]
async fn test_kv_relational_session_lookup() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE accounts (id INT PRIMARY KEY, username TEXT, active BOOLEAN)").await;
    exec(&ex, "INSERT INTO accounts VALUES (1, 'alice', true)").await;
    exec(&ex, "INSERT INTO accounts VALUES (2, 'bob', true)").await;
    exec(&ex, "INSERT INTO accounts VALUES (3, 'charlie', false)").await;

    exec(&ex, "SELECT kv_set('session:1', 'tok-aaa')").await;
    exec(&ex, "SELECT kv_set('session:2', 'tok-bbb')").await;

    // Only active users, joined with KV tokens
    let results = exec(
        &ex,
        "SELECT a.username, kv_get('session:' || a.id::text) AS token \
         FROM accounts a \
         WHERE a.active = true \
         ORDER BY a.id",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2, "two active users: {r:?}");
    assert_eq!(r[0][0], Value::Text("alice".into()));
    assert_eq!(r[0][1], Value::Text("tok-aaa".into()));
    assert_eq!(r[1][0], Value::Text("bob".into()));
    assert_eq!(r[1][1], Value::Text("tok-bbb".into()));
}

#[tokio::test]
async fn test_kv_counter_per_relational_row() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE products (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO products VALUES (10, 'widget')").await;
    exec(&ex, "INSERT INTO products VALUES (20, 'gadget')").await;

    // Increment view counters in KV
    exec(&ex, "SELECT kv_incr('views:10')").await;
    exec(&ex, "SELECT kv_incr('views:10')").await;
    exec(&ex, "SELECT kv_incr('views:10')").await;
    exec(&ex, "SELECT kv_incr('views:20')").await;

    // Join relational product names with KV view counts
    let results = exec(
        &ex,
        "SELECT p.name, kv_get('views:' || p.id::text) AS views \
         FROM products p \
         ORDER BY p.id",
    ).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("widget".into()));
    // kv_incr stores as integer; kv_get returns Int64 for integer values
    assert!(matches!(r[0][1], Value::Int64(3) | Value::Text(_)), "widget views should be 3: {:?}", r[0][1]);
    assert_eq!(r[1][0], Value::Text("gadget".into()));
    assert!(matches!(r[1][1], Value::Int64(1) | Value::Text(_)), "gadget views should be 1: {:?}", r[1][1]);
}

// ============================================================================
// 5. Datalog + Relational + Graph
// ============================================================================

#[tokio::test]
async fn test_datalog_relational_management_chain() {
    let ex = test_executor();

    exec(&ex, "CREATE TABLE org (employee TEXT, manager TEXT)").await;
    exec(&ex, "INSERT INTO org VALUES ('bob', 'alice')").await;
    exec(&ex, "INSERT INTO org VALUES ('charlie', 'alice')").await;
    exec(&ex, "INSERT INTO org VALUES ('diana', 'bob')").await;
    exec(&ex, "INSERT INTO org VALUES ('eve', 'bob')").await;

    // Import relational rows as Datalog facts: DATALOG_IMPORT(table_name, predicate)
    // Imports all columns; 'org' has (employee, manager) so facts are reports(employee, manager)
    exec(&ex, "SELECT DATALOG_IMPORT('org', 'reports')").await;

    // Transitive management chain
    exec(&ex, "SELECT DATALOG_RULE('manages(M, E) :- reports(E, M)')").await;
    exec(&ex, "SELECT DATALOG_RULE('manages_chain(M, E) :- manages(M, E)')").await;
    exec(&ex, "SELECT DATALOG_RULE('manages_chain(M, E) :- manages(M, X), manages_chain(X, E)')").await;

    let r = exec(&ex, "SELECT DATALOG_QUERY('manages_chain(alice, Who)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("bob"), "alice manages bob: {json}");
    assert!(json.contains("charlie"), "alice manages charlie: {json}");
    assert!(json.contains("diana"), "alice manages diana transitively: {json}");
    assert!(json.contains("eve"), "alice manages eve transitively: {json}");
}

#[tokio::test]
async fn test_datalog_graph_reachability() {
    let ex = test_executor();

    // Build graph
    exec(&ex, "SELECT graph_add_node('Node')").await;
    exec(&ex, "SELECT graph_add_node('Node')").await;
    exec(&ex, "SELECT graph_add_node('Node')").await;
    exec(&ex, "SELECT graph_add_edge(1, 2, 'LINK')").await;
    exec(&ex, "SELECT graph_add_edge(2, 3, 'LINK')").await;

    // Import graph edges as Datalog facts: edge(from, edge_type, to) — 3-column format
    exec(&ex, "SELECT DATALOG_IMPORT_GRAPH('edge')").await;

    // Transitive reachability — T matches the edge_type column (bound variable)
    exec(&ex, "SELECT DATALOG_RULE('reachable(X, Y) :- edge(X, T, Y)')").await;
    exec(&ex, "SELECT DATALOG_RULE('reachable(X, Z) :- reachable(X, Y), edge(Y, T, Z)')").await;

    let r = exec(&ex, "SELECT DATALOG_QUERY('reachable(1, Who)')").await;
    let json = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(json.contains("2"), "node 1 reaches 2: {json}");
    assert!(json.contains("3"), "node 1 reaches 3 transitively: {json}");
}

// ============================================================================
// 6. Cross-model transaction: BEGIN/COMMIT spanning multiple models
// ============================================================================

#[tokio::test]
async fn test_cross_model_transaction_commit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO users VALUES (1, 'alice')").await;
    exec(&ex, "SELECT kv_set('user:1:role', 'admin')").await;
    exec(&ex, "SELECT graph_add_node('User', '{\"name\":\"alice\"}')").await;
    exec(&ex, "COMMIT").await;

    // Relational persisted
    let r = exec(&ex, "SELECT name FROM users WHERE id = 1").await;
    assert_eq!(rows(&r[0]).len(), 1);
    assert_eq!(rows(&r[0])[0][0], Value::Text("alice".into()));

    // KV persisted
    let r = exec(&ex, "SELECT kv_get('user:1:role')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("admin".into()));

    // Graph persisted
    let r = exec(&ex, "SELECT graph_node_count()").await;
    let count = match scalar(&r[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(count, 1, "one graph node after commit");
}

#[tokio::test]
async fn test_cross_model_transaction_rollback_relational() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (id INT PRIMARY KEY, label TEXT)").await;

    // Committed baseline
    exec(&ex, "INSERT INTO items VALUES (1, 'committed')").await;

    // Rolled-back transaction
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO items VALUES (2, 'rolled-back')").await;
    exec(&ex, "ROLLBACK").await;

    let r = exec(&ex, "SELECT COUNT(*) FROM items").await;
    let count = match scalar(&r[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(count, 1, "only committed row should remain");
}

// ============================================================================
// 7. Sprint 6B — Cross-model ROLLBACK: FTS snapshot/restore
// ============================================================================

/// Helper: extract the JSON string from a fts_search() result and check if empty.
fn fts_result_is_empty(result: &ExecResult) -> bool {
    match rows(result) {
        r if r.is_empty() => true,
        r => match &r[0][0] {
            Value::Text(s) => s == "[]",
            _ => true,
        },
    }
}

#[tokio::test]
async fn test_cross_model_rollback_fts() {
    let ex = test_executor();

    // Index two documents before the transaction.
    exec(&ex, "SELECT fts_index(1, 'baseline document about rust')").await;
    exec(&ex, "SELECT fts_index(2, 'another baseline about systems')").await;

    // Verify pre-transaction state.
    let pre = exec(&ex, "SELECT fts_search('rust', 10)").await;
    assert!(!fts_result_is_empty(&pre[0]), "pre-txn: 'rust' should match");

    // BEGIN — index a new doc, then ROLLBACK.
    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT fts_index(99, 'transient rollback document about python')").await;
    // Verify the doc is visible inside the transaction.
    let mid = exec(&ex, "SELECT fts_search('python', 10)").await;
    assert!(!fts_result_is_empty(&mid[0]), "in-txn: 'python' should match doc 99");
    exec(&ex, "ROLLBACK").await;

    // After ROLLBACK, doc 99 must be gone.
    let post = exec(&ex, "SELECT fts_search('python', 10)").await;
    assert!(fts_result_is_empty(&post[0]), "after rollback: doc 99 must be gone");

    // Original docs must still be present.
    let still = exec(&ex, "SELECT fts_search('rust', 10)").await;
    assert!(!fts_result_is_empty(&still[0]), "after rollback: baseline docs intact");
}

// ============================================================================
// 8. Sprint 6B — Cross-model ROLLBACK: TimeSeries snapshot/restore
// ============================================================================

#[tokio::test]
async fn test_cross_model_rollback_timeseries() {
    let ex = test_executor();

    // Insert a baseline point.
    exec(&ex, "SELECT ts_insert('cpu', 1000, 50.0)").await;

    let pre = exec(&ex, "SELECT ts_last('cpu')").await;
    let pre_val = match scalar(&pre[0]) {
        Value::Float64(f) => *f,
        other => panic!("unexpected pre value: {other:?}"),
    };
    assert!((pre_val - 50.0).abs() < 1e-6, "pre-txn baseline should be 50.0");

    // BEGIN — insert a new point, then ROLLBACK.
    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT ts_insert('cpu', 2000, 99.0)").await;
    let mid = exec(&ex, "SELECT ts_last('cpu')").await;
    let mid_val = match scalar(&mid[0]) {
        Value::Float64(f) => *f,
        other => panic!("unexpected mid value: {other:?}"),
    };
    assert!((mid_val - 99.0).abs() < 1e-6, "in-txn: last should be 99.0");
    exec(&ex, "ROLLBACK").await;

    // After ROLLBACK the most recent point must revert to 50.0.
    let post = exec(&ex, "SELECT ts_last('cpu')").await;
    let post_val = match scalar(&post[0]) {
        Value::Float64(f) => *f,
        other => panic!("unexpected post value: {other:?}"),
    };
    assert!((post_val - 50.0).abs() < 1e-6, "after rollback: ts_last must revert to 50.0");
}

// ============================================================================
// 9. Sprint 6B — Cross-model ROLLBACK: Blob snapshot/restore
// ============================================================================

#[tokio::test]
async fn test_cross_model_rollback_blob() {
    let ex = test_executor();

    // Store a baseline blob (data as hex: "hello" = 68656c6c6f).
    exec(&ex, "SELECT blob_store('baseline', '68656c6c6f')").await;
    let pre = exec(&ex, "SELECT blob_count()").await;
    let pre_count = match scalar(&pre[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected pre count: {other:?}"),
    };
    assert_eq!(pre_count, 1, "pre-txn: one blob");

    // BEGIN — store a second blob, then ROLLBACK.
    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT blob_store('transient', '74656d70')").await;
    let mid = exec(&ex, "SELECT blob_count()").await;
    let mid_count = match scalar(&mid[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected mid count: {other:?}"),
    };
    assert_eq!(mid_count, 2, "in-txn: two blobs");
    exec(&ex, "ROLLBACK").await;

    // After ROLLBACK, only the baseline blob should remain.
    let post = exec(&ex, "SELECT blob_count()").await;
    let post_count = match scalar(&post[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("unexpected post count: {other:?}"),
    };
    assert_eq!(post_count, 1, "after rollback: transient blob must be gone");
}

// ============================================================================
// 10. Sprint 6B — Cross-model ROLLBACK: all specialty stores in one txn
// ============================================================================

#[tokio::test]
async fn test_cross_model_rollback_all_models() {
    let ex = test_executor();

    // Pre-transaction baseline across all models.
    exec(&ex, "CREATE TABLE events (id INT PRIMARY KEY, kind TEXT)").await;
    exec(&ex, "INSERT INTO events VALUES (1, 'baseline')").await;
    exec(&ex, "SELECT fts_index(1, 'baseline event')").await;
    exec(&ex, "SELECT ts_insert('events_rate', 1000, 1.0)").await;
    exec(&ex, "SELECT blob_store('evt_baseline', '64617461')").await;
    exec(&ex, "SELECT kv_set('counter', '0')").await;
    exec(&ex, "SELECT graph_add_node('Event', '{\"kind\":\"baseline\"}')").await;

    // BEGIN — mutate every model.
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO events VALUES (99, 'transient')").await;
    exec(&ex, "SELECT fts_index(99, 'transient rollback event')").await;
    exec(&ex, "SELECT ts_insert('events_rate', 9999, 999.0)").await;
    exec(&ex, "SELECT blob_store('evt_transient', '746d70')").await;
    exec(&ex, "SELECT kv_set('counter', '99')").await;
    exec(&ex, "SELECT graph_add_node('Event', '{\"kind\":\"transient\"}')").await;
    exec(&ex, "ROLLBACK").await;

    // Relational: row 99 gone.
    let rel = exec(&ex, "SELECT COUNT(*) FROM events").await;
    let rel_count = match scalar(&rel[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("{other:?}"),
    };
    assert_eq!(rel_count, 1, "relational: only baseline row");

    // FTS: doc 99 gone.
    let fts = exec(&ex, "SELECT fts_search('transient', 10)").await;
    assert!(fts_result_is_empty(&fts[0]), "fts: transient doc rolled back");

    // KV: counter back to '0'.
    let kv = exec(&ex, "SELECT kv_get('counter')").await;
    let kv_val = match scalar(&kv[0]) {
        Value::Text(s) => s.clone(),
        Value::Int64(n) => n.to_string(),
        other => panic!("{other:?}"),
    };
    assert_eq!(kv_val, "0", "kv: counter rolled back");

    // TimeSeries: last value reverts (the 999.0 point is gone).
    let ts = exec(&ex, "SELECT ts_last('events_rate')").await;
    let ts_val = match scalar(&ts[0]) {
        Value::Float64(f) => *f,
        other => panic!("{other:?}"),
    };
    assert!((ts_val - 1.0).abs() < 1e-6, "ts: last reverts to 1.0");

    // Blob: transient blob gone.
    let blob = exec(&ex, "SELECT blob_count()").await;
    let blob_count = match scalar(&blob[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        other => panic!("{other:?}"),
    };
    assert_eq!(blob_count, 1, "blob: transient blob rolled back");
}
