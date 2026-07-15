use super::*;

// ======================================================================
// @> containment operator on JSONB columns
// ======================================================================

#[tokio::test]
async fn test_jsonb_containment_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE events (id INT, properties JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO events VALUES (1, '{"browser": "chrome", "os": "linux"}')"#,
    )
    .await;
    exec(
        &ex,
        r#"INSERT INTO events VALUES (2, '{"browser": "firefox", "os": "macos"}')"#,
    )
    .await;
    exec(
        &ex,
        r#"INSERT INTO events VALUES (3, '{"browser": "chrome", "os": "windows"}')"#,
    )
    .await;

    // Match: properties @> '{"browser": "chrome"}'
    let results = exec(
        &ex,
        r#"SELECT id FROM events WHERE properties @> '{"browser": "chrome"}' ORDER BY id"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(3));
}

#[tokio::test]
async fn test_jsonb_containment_no_match() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE logs (id INT, data JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO logs VALUES (1, '{"level": "info", "msg": "ok"}')"#,
    )
    .await;

    // No match: data @> '{"level": "error"}'
    let results = exec(
        &ex,
        r#"SELECT id FROM logs WHERE data @> '{"level": "error"}'"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0);
}

#[tokio::test]
async fn test_jsonb_containment_nested() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE configs (id INT, settings JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO configs VALUES (1, '{"db": {"host": "localhost", "port": 5432}, "cache": true}')"#,
    )
    .await;
    exec(
        &ex,
        r#"INSERT INTO configs VALUES (2, '{"db": {"host": "remote", "port": 5432}, "cache": false}')"#,
    )
    .await;

    // Nested containment: settings @> '{"db": {"host": "localhost"}}'
    let results = exec(
        &ex,
        r#"SELECT id FROM configs WHERE settings @> '{"db": {"host": "localhost"}}'"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
}

#[tokio::test]
async fn test_jsonb_containment_array() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE tags (id INT, data JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO tags VALUES (1, '{"tags": ["rust", "db", "sql"]}')"#,
    )
    .await;
    exec(
        &ex,
        r#"INSERT INTO tags VALUES (2, '{"tags": ["python", "ml"]}')"#,
    )
    .await;

    // Array containment: data @> '{"tags": ["rust"]}' -- rust is in the array
    let results = exec(
        &ex,
        r#"SELECT id FROM tags WHERE data @> '{"tags": ["rust"]}'"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
}

#[tokio::test]
async fn test_jsonb_containment_with_null() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nullable (id INT, data JSONB)").await;
    exec(&ex, "INSERT INTO nullable VALUES (1, NULL)").await;
    exec(&ex, r#"INSERT INTO nullable VALUES (2, '{"key": "val"}')"#).await;

    // NULL @> anything should not match (NULL yields false in WHERE)
    let results = exec(
        &ex,
        r#"SELECT id FROM nullable WHERE data @> '{"key": "val"}' ORDER BY id"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(2));
}

#[tokio::test]
async fn test_jsonb_containment_empty_object() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE docs (id INT, data JSONB)").await;
    exec(&ex, r#"INSERT INTO docs VALUES (1, '{"a": 1}')"#).await;

    // Any object contains the empty object
    let results = exec(&ex, r#"SELECT id FROM docs WHERE data @> '{}'"#).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));
}

// ======================================================================
// <@ contained-by operator
// ======================================================================

#[tokio::test]
async fn test_jsonb_contained_by() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (id INT, data JSONB)").await;
    exec(&ex, r#"INSERT INTO items VALUES (1, '{"a": 1}')"#).await;
    exec(&ex, r#"INSERT INTO items VALUES (2, '{"a": 1, "b": 2}')"#).await;

    // data <@ '{"a": 1, "b": 2, "c": 3}' -- data is contained by the right side
    let results = exec(
        &ex,
        r#"SELECT id FROM items WHERE data <@ '{"a": 1, "b": 2, "c": 3}' ORDER BY id"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Int32(1));
    assert_eq!(r[1][0], Value::Int32(2));
}

// ======================================================================
// @> with non-JSONB columns (should return false)
// ======================================================================

#[tokio::test]
async fn test_containment_on_non_jsonb() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE plain (id INT, name TEXT)").await;
    exec(&ex, "INSERT INTO plain VALUES (1, 'hello')").await;

    // @> on plain TEXT (not valid JSON) should return false / no rows
    let results = exec(
        &ex,
        r#"SELECT id FROM plain WHERE name @> '{"key": "val"}'"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 0);
}

// ======================================================================
// Existing -> and ->> operators still work
// ======================================================================

#[tokio::test]
async fn test_arrow_operators_still_work() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE jtest (id INT, data JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO jtest VALUES (1, '{"name": "Alice", "age": 30}')"#,
    )
    .await;

    // -> returns JSONB
    let results = exec(&ex, r#"SELECT data -> 'name' FROM jtest"#).await;
    assert_eq!(
        scalar(&results[0]),
        &Value::Jsonb(serde_json::json!("Alice"))
    );

    // ->> returns TEXT
    let results = exec(&ex, r#"SELECT data ->> 'name' FROM jtest"#).await;
    assert_eq!(scalar(&results[0]), &Value::Text("Alice".to_string()));
}

#[tokio::test]
async fn test_path_arrow_operators_still_work() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nested (id INT, data JSONB)").await;
    exec(&ex, r#"INSERT INTO nested VALUES (1, '{"a": {"b": 42}}')"#).await;

    // #> returns JSONB at path
    let results = exec(&ex, r#"SELECT data #> '{a,b}' FROM nested"#).await;
    assert_eq!(scalar(&results[0]), &Value::Jsonb(serde_json::json!(42)));

    // #>> returns TEXT at path
    let results = exec(&ex, r#"SELECT data #>> '{a,b}' FROM nested"#).await;
    assert_eq!(scalar(&results[0]), &Value::Text("42".to_string()));
}

// ======================================================================
// GIN index creation on JSONB columns
// ======================================================================

#[tokio::test]
async fn test_gin_index_creation() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE indexed_events (id INT, props JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO indexed_events VALUES (1, '{"browser": "chrome"}')"#,
    )
    .await;
    exec(
        &ex,
        r#"INSERT INTO indexed_events VALUES (2, '{"browser": "firefox"}')"#,
    )
    .await;

    // CREATE INDEX USING GIN should succeed
    let results = exec(
        &ex,
        "CREATE INDEX idx_props_gin ON indexed_events USING GIN (props)",
    )
    .await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "CREATE INDEX"),
        other => panic!("expected Command result, got {other:?}"),
    }

    // Queries still work after GIN index creation, and the public planner
    // reports the accelerator rather than silently leaving it unused.
    let results = exec(
        &ex,
        r#"SELECT id FROM indexed_events WHERE props @> '{"browser": "chrome"}'"#,
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Int32(1));

    let explained = exec(
        &ex,
        r#"EXPLAIN SELECT id FROM indexed_events WHERE props @> '{"browser": "chrome"}'"#,
    )
    .await;
    let plan = rows(&explained[0])
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Text(line)) => Some(line.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("Index Scan"),
        "expected GIN Index Scan:\n{plan}"
    );
}

#[tokio::test]
async fn test_gin_index_tracks_mutations_and_array_containment() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE gin_docs (id INT, body JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO gin_docs VALUES
           (1, '{"tags": ["db", "rust"], "state": "draft"}'),
           (2, '{"tags": ["python"], "state": "draft"}')"#,
    )
    .await;
    exec(
        &ex,
        "CREATE INDEX gin_docs_body ON gin_docs USING GIN (body)",
    )
    .await;

    // JSON array containment is order-independent. Normalizing array paths in
    // GIN must not miss `rust` merely because it is the second stored element.
    let result = exec(
        &ex,
        r#"SELECT id FROM gin_docs WHERE body @> '{"tags": ["rust"]}'"#,
    )
    .await;
    assert_eq!(rows(&result[0]), &vec![vec![Value::Int32(1)]]);

    exec(
        &ex,
        r#"UPDATE gin_docs SET body = '{"tags": ["db"], "state": "published"}' WHERE id = 1"#,
    )
    .await;
    let old = exec(
        &ex,
        r#"SELECT id FROM gin_docs WHERE body @> '{"tags": ["rust"]}'"#,
    )
    .await;
    assert!(rows(&old[0]).is_empty());
    let updated = exec(
        &ex,
        r#"SELECT id FROM gin_docs WHERE body @> '{"state": "published"}'"#,
    )
    .await;
    assert_eq!(rows(&updated[0]), &vec![vec![Value::Int32(1)]]);

    exec(&ex, "DELETE FROM gin_docs WHERE id = 1").await;
    let deleted = exec(
        &ex,
        r#"SELECT id FROM gin_docs WHERE body @> '{"state": "published"}'"#,
    )
    .await;
    assert!(rows(&deleted[0]).is_empty());

    // Removing the accelerator must preserve query semantics through the
    // ordinary scan path.
    exec(&ex, "DROP INDEX gin_docs_body").await;
    let remaining = exec(
        &ex,
        r#"SELECT id FROM gin_docs WHERE body @> '{"state": "draft"}'"#,
    )
    .await;
    assert_eq!(rows(&remaining[0]), &vec![vec![Value::Int32(2)]]);
}

#[tokio::test]
async fn test_gin_bypasses_uncommitted_state_and_rebuilds_after_commit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE gin_tx (id INT, body JSONB)").await;
    exec(&ex, r#"INSERT INTO gin_tx VALUES (1, '{"state": "old"}')"#).await;
    exec(&ex, "CREATE INDEX gin_tx_body ON gin_tx USING GIN (body)").await;

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        r#"UPDATE gin_tx SET body = '{"state": "new"}' WHERE id = 1"#,
    )
    .await;
    let inside = exec(
        &ex,
        r#"SELECT id FROM gin_tx WHERE body @> '{"state": "new"}'"#,
    )
    .await;
    assert_eq!(rows(&inside[0]), &vec![vec![Value::Int32(1)]]);
    exec(&ex, "COMMIT").await;

    let committed = exec(
        &ex,
        r#"SELECT id FROM gin_tx WHERE body @> '{"state": "new"}'"#,
    )
    .await;
    assert_eq!(rows(&committed[0]), &vec![vec![Value::Int32(1)]]);
    let stale = exec(
        &ex,
        r#"SELECT id FROM gin_tx WHERE body @> '{"state": "old"}'"#,
    )
    .await;
    assert!(rows(&stale[0]).is_empty());
}

#[tokio::test]
async fn test_gin_ddl_rejects_unsupported_shapes_without_side_effects() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE gin_types (id INT, body JSONB, label TEXT)",
    )
    .await;

    assert!(
        ex.execute("CREATE INDEX bad_gin ON gin_types USING GIN (label)")
            .await
            .is_err()
    );
    exec(&ex, "CREATE INDEX good_gin ON gin_types USING GIN (body)").await;
    // IF NOT EXISTS must return before touching the live index associated with
    // the existing catalog name, even when the proposed definition differs.
    exec(
        &ex,
        "CREATE INDEX IF NOT EXISTS good_gin ON gin_types USING GIN (label)",
    )
    .await;
    exec(
        &ex,
        r#"INSERT INTO gin_types VALUES (1, '{"ok": true}', 'x')"#,
    )
    .await;
    let result = exec(
        &ex,
        r#"SELECT id FROM gin_types WHERE body @> '{"ok": true}'"#,
    )
    .await;
    assert_eq!(rows(&result[0]), &vec![vec![Value::Int32(1)]]);
}

// ======================================================================
// Subscript syntax: column['key'] as sugar for column -> 'key'
// ======================================================================

#[tokio::test]
async fn test_subscript_syntax() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sub_test (id INT, data JSONB)").await;
    exec(
        &ex,
        r#"INSERT INTO sub_test VALUES (1, '{"name": "Bob", "score": 95}')"#,
    )
    .await;

    // column['key'] should work like column -> 'key'
    let results = exec(&ex, r#"SELECT data['name'] FROM sub_test"#).await;
    assert_eq!(scalar(&results[0]), &Value::Jsonb(serde_json::json!("Bob")));
}
