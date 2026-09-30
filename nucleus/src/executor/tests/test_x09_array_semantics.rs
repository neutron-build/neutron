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
