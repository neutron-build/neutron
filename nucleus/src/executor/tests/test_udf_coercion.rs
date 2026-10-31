use super::*;
use crate::types::Value;

#[tokio::test]
async fn float_parameter_coerces_a_decimal_literal_and_returns_a_float() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE FUNCTION udf_double(p FLOAT) RETURNS FLOAT LANGUAGE SQL AS $$ SELECT $1 * 2.0 $$",
    )
    .await;
    let r = exec(&ex, "SELECT udf_double(5.0)").await;
    assert_eq!(*scalar(&r[0]), Value::Float64(10.0));
    let r = exec(&ex, "SELECT udf_double(3)").await;
    assert_eq!(*scalar(&r[0]), Value::Float64(6.0));
}

#[tokio::test]
async fn numeric_parameter_keeps_an_exact_decimal() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE FUNCTION udf_add_tenth(p NUMERIC) RETURNS NUMERIC LANGUAGE SQL AS $$ SELECT $1 + 0.1 $$",
    )
    .await;
    let r = exec(&ex, "SELECT udf_add_tenth(0.2)").await;
    assert_eq!(*scalar(&r[0]), Value::Numeric("0.3".into()));
}
