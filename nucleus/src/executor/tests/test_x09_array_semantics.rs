//! Expected values and types are independently pinned to PostgreSQL 17.
use super::*;

#[tokio::test]
async fn x09_array_casts_keep_declared_types_even_empty_or_null() {
    let ex = test_executor();
    for (sql, expected_type, expected) in [
        (
            "SELECT ARRAY['1','2']::int[]",
            DataType::Array(Box::new(DataType::Int32)),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
        ),
        (
            "SELECT '{}'::bigint[]",
            DataType::Array(Box::new(DataType::Int64)),
            Value::Array(vec![]),
        ),
        (
            "SELECT ARRAY[NULL]::int[]",
            DataType::Array(Box::new(DataType::Int32)),
            Value::Array(vec![Value::Null]),
        ),
        (
            "SELECT NULL::int[]",
            DataType::Array(Box::new(DataType::Int32)),
            Value::Null,
        ),
    ] {
        let results = ex.execute(sql).await.unwrap();
        match &results[0] {
            ExecResult::Select { columns, rows, .. } => {
                assert_eq!(columns[0].1, expected_type, "{sql}");
                assert_eq!(rows[0][0], expected, "{sql}");
            }
            other => panic!("{sql}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn x09_timestamptz_array_literals_and_writes_use_session_zone() {
    let ex = test_executor();
    exec(&ex, "SET TIME ZONE 'America/New_York'").await;
    exec(
        &ex,
        "CREATE TABLE x09_array (id int, instants timestamptz[])",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO x09_array VALUES (1, '{\"2026-01-02 03:04:05\"}')",
    )
    .await;
    let expected = Value::Array(vec![Value::TimestampTz(
        i64::from(crate::types::ymd_to_days(2026, 1, 2)) * 86_400_000_000
            + 8 * 3_600_000_000
            + 4 * 60_000_000
            + 5_000_000,
    )]);
    for sql in [
        "SELECT instants FROM x09_array",
        "SELECT ARRAY['2026-01-02 03:04:05']::timestamptz[]",
    ] {
        let results = ex.execute(sql).await.unwrap();
        assert_eq!(scalar(&results[0]), &expected, "{sql}");
    }
}

#[tokio::test]
async fn x09_ragged_array_constructors_and_literals_are_refused_before_storage() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x09_array (items int[])").await;
    for sql in [
        "INSERT INTO x09_array VALUES (ARRAY[ARRAY[1],ARRAY[2,3]])",
        "INSERT INTO x09_array VALUES (ARRAY[ARRAY[1],NULL])",
        "INSERT INTO x09_array VALUES ('{{1},{2,3}}')",
        "INSERT INTO x09_array VALUES ('[0:1]={7,9}')",
        "INSERT INTO x09_array VALUES ('{{1,2},{3,4}}')",
        "INSERT INTO x09_array VALUES (ARRAY[1,1.5])",
    ] {
        assert!(ex.execute(sql).await.is_err(), "{sql}");
    }
    assert_eq!(
        scalar(&ex.execute("SELECT count(*) FROM x09_array").await.unwrap()[0]),
        &Value::Int64(0)
    );
}

#[tokio::test]
async fn x09_fractional_extract_and_bytea_encoding_preserve_values() {
    let ex = test_executor();
    for (sql, expected) in [
        (
            "SELECT EXTRACT(SECOND FROM TIMESTAMPTZ '2000-01-01 00:00:01.25+00')",
            Value::Numeric("1.250000".into()),
        ),
        (
            "SELECT EXTRACT(EPOCH FROM TIMESTAMPTZ '1969-12-31 23:59:59.75+00')",
            Value::Numeric("-0.250000".into()),
        ),
        (
            "SELECT DATE_PART('second', TIMESTAMPTZ '2000-01-01 00:00:01.25+00')",
            Value::Float64(1.25),
        ),
        (
            "SELECT ENCODE(DECODE('00ff2a', 'hex'), 'hex')",
            Value::Text("00ff2a".into()),
        ),
        (
            "SELECT INTERVAL '1.25 seconds'::text",
            Value::Text("00:00:01.25".into()),
        ),
    ] {
        let result = exec(&ex, sql).await;
        assert_eq!(scalar(&result[0]), &expected, "{sql}");
    }
}

#[tokio::test]
async fn x09_date_assignment_to_timestamptz_uses_session_midnight() {
    let ex = test_executor();
    exec(&ex, "SET TIME ZONE 'America/Vancouver'").await;
    exec(
        &ex,
        "CREATE TABLE x09_dates (id int, instant timestamptz, instants timestamptz[])",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO x09_dates VALUES (1, DATE '2026-01-02', ARRAY[DATE '2026-01-02'])",
    )
    .await;
    let expected = Value::TimestampTz(
        i64::from(crate::types::ymd_to_days(2026, 1, 2)) * 86_400_000_000 + 8 * 3_600_000_000,
    );
    for sql in [
        "SELECT instant FROM x09_dates",
        "SELECT instants[1] FROM x09_dates",
    ] {
        assert_eq!(scalar(&exec(&ex, sql).await[0]), &expected, "{sql}");
    }
    exec(&ex, "UPDATE x09_dates SET instant = DATE '2026-07-02', instants = ARRAY[DATE '2026-07-02'] WHERE id = 1").await;
    let summer = Value::TimestampTz(
        i64::from(crate::types::ymd_to_days(2026, 7, 2)) * 86_400_000_000 + 7 * 3_600_000_000,
    );
    for sql in [
        "SELECT instant FROM x09_dates",
        "SELECT instants[1] FROM x09_dates",
    ] {
        assert_eq!(scalar(&exec(&ex, sql).await[0]), &summer, "{sql}");
    }
}

#[tokio::test]
async fn x09_temporal_extraction_describes_same_type_for_empty_results() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE x09_extract (value timestamp)").await;
    for (expr, expected_type, expected_value) in [
        (
            "EXTRACT(SECOND FROM value)",
            DataType::Numeric,
            Value::Numeric("1.250000".into()),
        ),
        (
            "DATE_PART('second', value)",
            DataType::Float64,
            Value::Float64(1.25),
        ),
        (
            "EXTRACT(YEAR FROM value)",
            DataType::Numeric,
            Value::Numeric("2000".into()),
        ),
        (
            "DATE_PART('year', value)",
            DataType::Float64,
            Value::Float64(2000.0),
        ),
    ] {
        let result = exec(&ex, &format!("SELECT {expr} FROM x09_extract")).await;
        match &result[0] {
            ExecResult::Select { columns, rows, .. } => {
                assert!(rows.is_empty());
                assert_eq!(columns[0].1, expected_type, "{expr}");
            }
            other => panic!("{other:?}"),
        }
        exec(
            &ex,
            "INSERT INTO x09_extract VALUES (TIMESTAMP '2000-01-01 00:00:01.25')",
        )
        .await;
        let result = exec(&ex, &format!("SELECT {expr} FROM x09_extract")).await;
        match &result[0] {
            ExecResult::Select { columns, rows, .. } => {
                assert_eq!(columns[0].1, expected_type, "{expr}");
                assert_eq!(rows[0][0], expected_value, "{expr}");
            }
            other => panic!("{other:?}"),
        }
        exec(&ex, "DELETE FROM x09_extract").await;
    }
    assert_eq!(
        scalar(&exec(&ex, "SELECT DATE_PART('epoch', DATE '1970-01-01')").await[0]),
        &Value::Float64(0.0)
    );
    assert_eq!(
        scalar(&exec(&ex, "SELECT DATE_PART('dow', DATE '2000-01-02')").await[0]),
        &Value::Float64(0.0)
    );
}

#[tokio::test]
async fn x09_bytea_encoding_functions_are_strict_for_both_arguments() {
    let ex = test_executor();
    for sql in [
        "SELECT ENCODE(DECODE('00ff','hex'), NULL)",
        "SELECT DECODE('00ff', NULL)",
        "SELECT ENCODE(NULL, 'hex')",
        "SELECT DECODE(NULL, 'hex')",
    ] {
        assert_eq!(scalar(&exec(&ex, sql).await[0]), &Value::Null, "{sql}");
    }
}
