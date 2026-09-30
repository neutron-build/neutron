use super::*;

#[tokio::test]
async fn x12_recursive_union_distinct_reaches_fixpoint_on_real_cycle() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_edges (parent INT, child INT)").await;
    // Duplicate paths and a genuine 1 -> 2 -> 3 -> 1 cycle.
    exec(
        &ex,
        "INSERT INTO x12_edges VALUES (1,2),(1,2),(2,3),(3,1),(2,4),(3,4)",
    )
    .await;
    let result = exec(&ex, "WITH RECURSIVE d(n) AS (SELECT child FROM x12_edges WHERE parent = 1 UNION SELECT e.child FROM x12_edges e JOIN d ON e.parent = d.n) SELECT n FROM d ORDER BY n").await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
            vec![Value::Int32(4)]
        ]
    );
}

#[tokio::test]
async fn x12_recursive_union_distinct_deduplicates_seed_and_null_rows() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x12_seeds (n INT)").await;
    exec(&ex, "INSERT INTO x12_seeds VALUES (1),(1),(NULL),(NULL)").await;
    let result = exec(&ex, "WITH RECURSIVE d(n) AS (SELECT n FROM x12_seeds UNION SELECT n FROM d) SELECT count(*) FROM d").await;
    assert_eq!(scalar(&result[0]), &Value::Int64(2));
}

#[tokio::test]
async fn x12_recursive_cte_subqueries_and_siblings_keep_their_scope() {
    let ex = test_executor();
    // The inner WITH must inherit seeds; a nested recursive arm must see
    // the current d, and the following sibling must see the completed d.
    let result = exec(&ex, "WITH seeds(n) AS (SELECT 1), answer AS (WITH RECURSIVE d(n) AS (SELECT n FROM seeds UNION SELECT n + 1 FROM (SELECT n FROM d) step WHERE n < 3), totals AS (SELECT count(*) AS total FROM d) SELECT total FROM totals) SELECT total FROM answer").await;
    assert_eq!(scalar(&result[0]), &Value::Int64(3));
    assert!(ex.current_session().active_ctes.read().is_empty());
    assert!(ex.execute("SELECT * FROM d").await.is_err());
}

#[tokio::test]
async fn x12_recursive_union_all_preserves_duplicates_and_refuses_overflow() {
    let ex = test_executor();
    let result = exec(&ex, "WITH RECURSIVE d(n) AS (VALUES (1),(1) UNION ALL SELECT n + 1 FROM d WHERE n < 3) SELECT count(*) FROM d").await;
    assert_eq!(scalar(&result[0]), &Value::Int64(6));
    for quantifier in ["UNION", "UNION ALL"] {
        let error = ex.execute(&format!("WITH RECURSIVE d(n) AS (SELECT 1 {quantifier} SELECT n + 1 FROM d) SELECT count(*) FROM d")).await.unwrap_err();
        assert!(
            matches!(error, ExecError::Unsupported(ref message) if message.contains("exceeded 1000 iterations")),
            "{error:?}"
        );
        assert!(ex.current_session().active_ctes.read().is_empty());
    }
    let error = ex
        .execute("WITH RECURSIVE d(n) AS (SELECT 1 UNION SELECT n, n FROM d) SELECT * FROM d")
        .await
        .unwrap_err();
    assert!(
        matches!(error, ExecError::Unsupported(ref message) if message.contains("same number of columns")),
        "{error:?}"
    );
    assert!(ex.current_session().active_ctes.read().is_empty());
}

#[tokio::test]
async fn x12_actual_studio_table_metadata_sql_runs_with_recursive_union() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE x12_studio_recursive (id INT, k INT, amount INT, PRIMARY KEY (k, id))",
    )
    .await;
    // Execute the actual CLI source, including its recursive UNION inside
    // EXISTS and both correlated catalog subqueries. Only bind parameters.
    let source = include_str!("../../../../cli/internal/studio/table_rows.go");
    let sql = source
        .split("const tableMetaSQL = `")
        .nth(1)
        .unwrap()
        .split('`')
        .next()
        .unwrap();
    let sql = sql
        .replace("$1", "'public'")
        .replace("$2", "'x12_studio_recursive'");
    let result = exec(&ex, &sql).await;
    let metadata = rows(&result[0]);
    assert_eq!(metadata.len(), 3);
    assert_eq!(metadata[0][0], Value::Text("id".into()));
    assert_eq!(metadata[0][1], Value::Int64(2));
    assert_eq!(metadata[1][0], Value::Text("k".into()));
    assert_eq!(metadata[1][1], Value::Int64(1));
    assert_eq!(metadata[2][1], Value::Int64(0));
    for column in metadata {
        assert_eq!(column.len(), 16);
        assert_eq!(column[14], Value::Bool(false));
    }
}

#[tokio::test]
async fn x12_with_recursive_nonrecursive_union_executes_each_arm_once() {
    let ex = test_executor();
    for quantifier in ["UNION", "UNION ALL"] {
        let result = exec(
            &ex,
            &format!(
                "WITH RECURSIVE c(x) AS (SELECT 1 {quantifier} SELECT 2) SELECT x FROM c ORDER BY x"
            ),
        )
        .await;
        assert_eq!(
            rows(&result[0]),
            &vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
        );
    }
    // A string containing the CTE name is not a relation reference.
    let result = exec(
        &ex,
        "WITH RECURSIVE c(x) AS (SELECT 'first' UNION ALL SELECT 'c') SELECT x FROM c ORDER BY x",
    )
    .await;
    assert_eq!(
        rows(&result[0]),
        &vec![
            vec![Value::Text("c".into())],
            vec![Value::Text("first".into())]
        ]
    );
    assert!(ex.current_session().active_ctes.read().is_empty());
}
