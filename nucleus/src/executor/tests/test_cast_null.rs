use super::*;
use crate::types::Value;

#[tokio::test]
async fn cast_of_a_null_value_to_text_stays_null() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE cast_null (id INT PRIMARY KEY, j JSONB, t TEXT, n INT)",
    )
    .await;
    exec(&ex, "INSERT INTO cast_null (id) VALUES (1)").await;
    let results = exec(
        &ex,
        "SELECT j::text, t::text, n::text, CAST(j AS VARCHAR), (j IS NULL) FROM cast_null WHERE id = 1",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1);
    for (column, cell) in r[0].iter().take(4).enumerate() {
        assert_eq!(*cell, Value::Null, "column {column} must stay NULL");
    }
    assert_eq!(r[0][4], Value::Bool(true));
}

#[tokio::test]
async fn cast_of_a_present_value_to_text_still_renders_it() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE cast_present (id INT PRIMARY KEY, j JSONB, n INT)",
    )
    .await;
    exec(&ex, "INSERT INTO cast_present VALUES (1, '{\"a\": 1}', 5)").await;
    let results = exec(
        &ex,
        "SELECT j::text, n::text FROM cast_present WHERE id = 1",
    )
    .await;
    let r = rows(&results[0]);
    assert!(matches!(&r[0][0], Value::Text(text) if text.contains("\"a\"")));
    assert_eq!(r[0][1], Value::Text("5".into()));
}
