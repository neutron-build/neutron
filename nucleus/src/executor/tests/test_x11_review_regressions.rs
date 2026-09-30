use super::*;

#[tokio::test]
async fn x11_json_aggregate_preserves_number_identity_and_digits() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_numeric (v NUMERIC)").await;
    exec(
        &ex,
        "INSERT INTO x11_numeric VALUES (1.00), ('1234567890123456789012345678'::NUMERIC)",
    )
    .await;
    let result = exec(&ex, "SELECT JSONB_AGG(v) FROM x11_numeric").await;
    let Value::Jsonb(serde_json::Value::Array(values)) = scalar(&result[0]) else {
        panic!("JSON array expected")
    };
    assert!(
        values.iter().all(serde_json::Value::is_number),
        "{values:?}"
    );
    assert_eq!(values[1].to_string(), "1234567890123456789012345678");
}

#[tokio::test]
async fn x11_jsonb_text_retains_decimal_scale() {
    let ex = test_executor();
    let result = exec(&ex, "SELECT '{\"n\":1.00}'::JSONB::TEXT").await;
    assert_eq!(scalar(&result[0]), &Value::Text("{\"n\": 1.00}".into()));
}

#[tokio::test]
async fn x11_grouped_windows_refuse_instead_of_ignoring_grouping() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_window (v INT)").await;
    exec(&ex, "INSERT INTO x11_window VALUES (1), (1), (2)").await;
    assert!(matches!(
        ex.execute("SELECT v, ROW_NUMBER() OVER (ORDER BY v) FROM x11_window GROUP BY v")
            .await,
        Err(ExecError::Unsupported(_))
    ));
}

#[tokio::test]
async fn x11_joined_writes_refuse_and_preserve_targets() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_target (id INT PRIMARY KEY, v INT)").await;
    exec(&ex, "CREATE TABLE x11_empty (id INT)").await;
    exec(&ex, "INSERT INTO x11_target VALUES (1, 7)").await;
    for sql in [
        "UPDATE x11_target SET v = 9 FROM x11_empty",
        "DELETE FROM x11_target USING x11_empty",
    ] {
        assert!(
            matches!(ex.execute(sql).await, Err(ExecError::Unsupported(_))),
            "{sql}"
        );
        let result = exec(&ex, "SELECT v FROM x11_target").await;
        assert_eq!(scalar(&result[0]), &Value::Int32(7));
    }
}

#[tokio::test]
async fn x11_empty_wrapped_window_retains_integer_metadata() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_empty_window (v INT)").await;
    let result = exec(
        &ex,
        "SELECT ROW_NUMBER() OVER (ORDER BY v) + 1 FROM x11_empty_window",
    )
    .await;
    let ExecResult::Select { columns, rows, .. } = &result[0] else {
        panic!("SELECT expected")
    };
    assert!(rows.is_empty());
    assert_eq!(columns[0].1, DataType::Int64);
}

#[tokio::test]
async fn x11_json_aggregate_empty_null_order_and_duplicate_contracts() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_json (k TEXT, v INT)").await;
    let result = exec(&ex, "SELECT JSONB_AGG(v) FROM x11_json").await;
    assert_eq!(scalar(&result[0]), &Value::Null);
    let ExecResult::Select { columns, .. } = &result[0] else {
        panic!("SELECT expected")
    };
    assert_eq!(columns[0].1, DataType::Jsonb);
    exec(
        &ex,
        "INSERT INTO x11_json VALUES ('b', 2), ('a', NULL), ('b', 1)",
    )
    .await;
    let result = exec(&ex, "SELECT JSONB_AGG(v ORDER BY v) FROM x11_json").await;
    assert_eq!(
        scalar(&result[0]),
        &Value::Jsonb(serde_json::json!([1, 2, null]))
    );
    let result = exec(
        &ex,
        "SELECT JSONB_OBJECT_AGG(k, v ORDER BY v) FROM x11_json",
    )
    .await;
    assert_eq!(
        scalar(&result[0]),
        &Value::Jsonb(serde_json::json!({"a": null, "b": 2}))
    );
    assert!(matches!(
        ex.execute("SELECT JSON_OBJECT_AGG(k, v) FROM x11_json")
            .await,
        Err(ExecError::Unsupported(_))
    ));
}

#[tokio::test]
async fn x11_derived_alias_lists_rename_columns_positionally() {
    let ex = test_executor();
    let result = exec(
        &ex,
        "SELECT renamed FROM (SELECT 7 AS original) AS d(renamed)",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Int32(7));
    assert!(
        ex.execute("SELECT * FROM (SELECT 7 AS original) AS d(a, b)")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn x11_untyped_columnar_aggregates_refuse_instead_of_returning_zero() {
    let ex = test_executor();
    exec(&ex, "SELECT COLUMNAR_INSERT('x11_untyped', 'v', '1')").await;
    exec(&ex, "SELECT COLUMNAR_INSERT('x11_untyped', 'v', '3')").await;
    for function in [
        "COLUMNAR_SUM",
        "COLUMNAR_AVG",
        "COLUMNAR_MIN",
        "COLUMNAR_MAX",
    ] {
        assert!(matches!(
            ex.execute(&format!("SELECT {function}('x11_untyped', 'v')"))
                .await,
            Err(ExecError::Unsupported(_))
        ));
    }
    let result = exec(&ex, "SELECT COLUMNAR_COUNT('x11_untyped')").await;
    assert_eq!(scalar(&result[0]), &Value::Int64(2));
    exec(&ex, "SELECT COLUMNAR_INSERT('x11_typed', 'v', 1::int8)").await;
    exec(&ex, "SELECT COLUMNAR_INSERT('x11_typed', 'v', 3::int8)").await;
    let result = exec(&ex, "SELECT COLUMNAR_SUM('x11_typed', 'v')").await;
    assert_eq!(scalar(&result[0]), &Value::Float64(4.0));
}

#[tokio::test]
async fn x11_delete_using_empty_source_refuses_before_mutation() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_delete_target (id INT PRIMARY KEY)").await;
    exec(&ex, "CREATE TABLE x11_delete_empty (id INT)").await;
    exec(&ex, "INSERT INTO x11_delete_target VALUES (1)").await;
    assert!(matches!(
        ex.execute("DELETE FROM x11_delete_target USING x11_delete_empty")
            .await,
        Err(ExecError::Unsupported(_))
    ));
    let result = exec(&ex, "SELECT id FROM x11_delete_target").await;
    assert_eq!(scalar(&result[0]), &Value::Int32(1));
}

#[tokio::test]
async fn x11_jsonb_numeric_scale_does_not_change_equality_distinct_or_joins() {
    let ex = test_executor();
    for sql in [
        "SELECT '1.0'::jsonb = '1.00'::jsonb",
        "SELECT '{\"x\":[1.0]}'::jsonb = '{\"x\":[1.00]}'::jsonb",
    ] {
        assert_eq!(
            scalar(&exec(&ex, sql).await[0]),
            &Value::Bool(true),
            "{sql}"
        );
    }
    exec(&ex, "CREATE TABLE x11_json_keys (id int, v jsonb)").await;
    exec(
        &ex,
        "INSERT INTO x11_json_keys VALUES (1, '{\"x\":[1.0]}'), (2, '{\"x\":[1.00]}')",
    )
    .await;
    let result = exec(&ex, "SELECT JSONB_AGG(DISTINCT v) FROM x11_json_keys").await;
    let Value::Jsonb(serde_json::Value::Array(values)) = scalar(&result[0]) else {
        panic!("JSONB array expected");
    };
    assert_eq!(values.len(), 1);
    let result = exec(&ex, "SELECT DISTINCT v FROM x11_json_keys").await;
    assert_eq!(rows(&result[0]).len(), 1);
    assert_eq!(
        scalar(
            &exec(
                &ex,
                "SELECT COUNT(*) FROM x11_json_keys a JOIN x11_json_keys b ON a.v = b.v"
            )
            .await[0]
        ),
        &Value::Int64(4)
    );
}

#[tokio::test]
async fn x11_first_null_wrapped_window_retains_declared_metadata() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x11_null_window (v INT)").await;
    exec(&ex, "INSERT INTO x11_null_window VALUES (1), (2)").await;
    let result = exec(&ex, "SELECT (CASE WHEN ROW_NUMBER() OVER (ORDER BY v) = 1 THEN NULL ELSE ROW_NUMBER() OVER (ORDER BY v) END)::BIGINT FROM x11_null_window").await;
    let ExecResult::Select { columns, rows, .. } = &result[0] else {
        panic!("SELECT expected")
    };
    assert_eq!(rows, &vec![vec![Value::Null], vec![Value::Int64(2)]]);
    assert_eq!(columns[0].1, DataType::Int64);
}

#[tokio::test]
async fn x11_jsonb_numeric_scale_does_not_change_containment() {
    let ex = test_executor();
    for sql in [
        "SELECT '1.0'::jsonb @> '1.00'::jsonb",
        "SELECT '{\"x\":[1.0]}'::jsonb @> '{\"x\":[1.00]}'::jsonb",
        "SELECT '{\"x\":[1.0]}'::jsonb <@ '{\"x\":[1.00]}'::jsonb",
    ] {
        assert_eq!(
            scalar(&exec(&ex, sql).await[0]),
            &Value::Bool(true),
            "{sql}"
        );
    }
    assert_eq!(scalar(&exec(&ex, "SELECT '[1234567890123456789012345678901234567890]'::jsonb @> '[1234567890123456789012345678901234567891]'::jsonb").await[0]), &Value::Bool(false));
}
