use super::*;

// ======================================================================
// Scalar function tests
// ======================================================================

#[tokio::test]
async fn test_string_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT UPPER('hello')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("HELLO".into()));

    let results = exec(&ex, "SELECT LOWER('WORLD')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("world".into()));

    let results = exec(&ex, "SELECT LENGTH('hello')").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(5));

    let results = exec(&ex, "SELECT TRIM('  hi  ')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("hi".into()));

    let results = exec(&ex, "SELECT REVERSE('abc')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("cba".into()));

    let results = exec(&ex, "SELECT INITCAP('hello world')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("Hello World".into()));

    let results = exec(&ex, "SELECT LEFT('hello', 3)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("hel".into()));

    let results = exec(&ex, "SELECT RIGHT('hello', 3)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("llo".into()));

    let results = exec(&ex, "SELECT REPEAT('ab', 3)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("ababab".into()));
}

#[tokio::test]
async fn test_concat_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CONCAT('hello', ' ', 'world')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("hello world".into()));

    let results = exec(&ex, "SELECT CONCAT_WS('-', 'a', 'b', 'c')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("a-b-c".into()));
}

#[tokio::test]
async fn test_substring() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT SUBSTRING('hello world', 7, 5)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("world".into()));
}

#[tokio::test]
async fn test_replace() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT REPLACE('hello world', 'world', 'rust')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("hello rust".into()));
}

#[tokio::test]
async fn test_math_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ABS(-42)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(42));

    let results = exec(&ex, "SELECT CEIL(3.2)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(4.0));

    let results = exec(&ex, "SELECT FLOOR(3.8)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(3.0));

    let results = exec(&ex, "SELECT SQRT(16.0)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(4.0));

    let results = exec(&ex, "SELECT POWER(2.0, 10.0)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(1024.0));

    let results = exec(&ex, "SELECT SIGN(-5)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(-1));
}

#[tokio::test]
async fn test_round() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ROUND(3.14159, 2)").await;
    match scalar(&results[0]) {
        Value::Float64(f) => assert!((f - 3.14).abs() < 0.001),
        other => panic!("expected Float64, got {other:?}"),
    }
}

#[tokio::test]
async fn test_null_handling_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT COALESCE(NULL, NULL, 42)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(42));

    let results = exec(&ex, "SELECT NULLIF(1, 1)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);

    let results = exec(&ex, "SELECT NULLIF(1, 2)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(1));

    let results = exec(&ex, "SELECT GREATEST(1, 5, 3)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(5));

    let results = exec(&ex, "SELECT LEAST(1, 5, 3)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(1));
}

#[tokio::test]
async fn test_type_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT PG_TYPEOF(42)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("integer".into()));

    let results = exec(&ex, "SELECT PG_TYPEOF('hello')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("text".into()));
}

#[tokio::test]
async fn test_json_build_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSONB_BUILD_OBJECT('name', 'Alice', 'age', 30)").await;
    match scalar(&results[0]) {
        Value::Jsonb(v) => {
            assert_eq!(v["name"], "Alice");
            assert_eq!(v["age"], 30);
        }
        other => panic!("expected Jsonb, got {other:?}"),
    }

    let results = exec(&ex, "SELECT JSONB_BUILD_ARRAY(1, 2, 3)").await;
    match scalar(&results[0]) {
        Value::Jsonb(serde_json::Value::Array(arr)) => {
            assert_eq!(arr.len(), 3);
        }
        other => panic!("expected Jsonb array, got {other:?}"),
    }
}


// ======================================================================
// LIKE / CASE tests
// ======================================================================

#[tokio::test]
async fn test_like_pattern() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE products (name TEXT)").await;
    exec(&ex, "INSERT INTO products VALUES ('Apple')").await;
    exec(&ex, "INSERT INTO products VALUES ('Banana')").await;
    exec(&ex, "INSERT INTO products VALUES ('Avocado')").await;

    let results = exec(&ex, "SELECT name FROM products WHERE name LIKE 'A%'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2); // Apple, Avocado
}

#[tokio::test]
async fn test_ilike_pattern() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE items (name TEXT)").await;
    exec(&ex, "INSERT INTO items VALUES ('Hello')").await;
    exec(&ex, "INSERT INTO items VALUES ('HELLO')").await;
    exec(&ex, "INSERT INTO items VALUES ('world')").await;

    let results = exec(&ex, "SELECT name FROM items WHERE name ILIKE 'hello'").await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_case_expression() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE scores (name TEXT, score INT)").await;
    exec(&ex, "INSERT INTO scores VALUES ('Alice', 95)").await;
    exec(&ex, "INSERT INTO scores VALUES ('Bob', 60)").await;

    let results = exec(
        &ex,
        "SELECT name, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END FROM scores",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r[0][1], Value::Text("A".into()));
    assert_eq!(r[1][1], Value::Text("C".into()));
}


// ======================================================================
// Integration: full query with functions
// ======================================================================

#[tokio::test]
async fn test_functions_in_where_clause() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE people (name TEXT, age INT)").await;
    exec(&ex, "INSERT INTO people VALUES ('alice', 25)").await;
    exec(&ex, "INSERT INTO people VALUES ('bob', 30)").await;
    exec(&ex, "INSERT INTO people VALUES ('charlie', 35)").await;

    let results = exec(
        &ex,
        "SELECT UPPER(name), age FROM people WHERE age > 27",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Text("BOB".into()));
    assert_eq!(r[1][0], Value::Text("CHARLIE".into()));
}

#[tokio::test]
async fn test_functions_with_aggregates() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE sales (region TEXT, amount INT)").await;
    exec(&ex, "INSERT INTO sales VALUES ('east', 100)").await;
    exec(&ex, "INSERT INTO sales VALUES ('east', 200)").await;
    exec(&ex, "INSERT INTO sales VALUES ('west', 150)").await;

    let results = exec(
        &ex,
        "SELECT UPPER(region), SUM(amount) FROM sales GROUP BY region",
    )
    .await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_lpad_rpad() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT LPAD('42', 5, '0')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("00042".into()));

    let results = exec(&ex, "SELECT RPAD('hi', 5, '!')").await;
    assert_eq!(scalar(&results[0]), &Value::Text("hi!!!".into()));
}

#[tokio::test]
async fn test_md5() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT MD5('hello')").await;
    match scalar(&results[0]) {
        Value::Text(s) => assert_eq!(s.len(), 16), // 16 hex chars for u64
        other => panic!("expected Text, got {other:?}"),
    }
}

#[tokio::test]
async fn test_current_database() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CURRENT_DATABASE()").await;
    assert_eq!(scalar(&results[0]), &Value::Text("nucleus".into()));
}


// ML / Embedding pipeline function tests
// ======================================================================

#[tokio::test]
async fn test_embed_function() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT EMBED('bow', 'hello world')").await;
    let val = scalar(&results[0]);
    // Should return a vector string like "[0.500000,0.500000]"
    match val {
        Value::Text(s) => {
            assert!(s.starts_with('['), "embed result should start with [: {s}");
            assert!(s.ends_with(']'), "embed result should end with ]: {s}");
            // Should have at least one float value
            let inner = &s[1..s.len()-1];
            assert!(!inner.is_empty(), "embed result should not be empty");
        }
        _ => panic!("expected text value from embed(), got: {val:?}"),
    }
}

#[tokio::test]
async fn test_embed_null_input() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT EMBED('model', NULL)").await;
    assert_eq!(*scalar(&results[0]), Value::Null);
}

#[tokio::test]
async fn test_predict_function() {
    let ex = test_executor();
    // Register a linear model: y = 2*x1 + 3*x2 + 1
    ex.model_registry.write().register_linear("linmod", vec![2.0, 3.0], 1.0);
    let results = exec(&ex, "SELECT PREDICT('linmod', 1.0, 2.0)").await;
    let val = scalar(&results[0]);
    // Expected: 2*1 + 3*2 + 1 = 9.0
    match val {
        Value::Text(s) => {
            assert!(s.contains("9.0"), "predict result should contain 9.0: {s}");
        }
        _ => panic!("expected text from predict(), got: {val:?}"),
    }
}

#[tokio::test]
async fn test_classify_function() {
    let ex = test_executor();
    // Register a softmax model with 3 classes
    ex.model_registry.write().register_softmax("clf", vec![
        vec![1.0, 0.0],
        vec![0.0, 1.0],
        vec![0.5, 0.5],
    ], vec![0.0, 0.0, 0.0]);
    // Input [10.0, 1.0] → class 0 should have highest dot product (1*10 + 0*1 = 10)
    let results = exec(&ex, "SELECT CLASSIFY('clf', 10.0, 1.0)").await;
    let val = scalar(&results[0]);
    match val {
        Value::Text(s) => {
            assert!(s.starts_with("class_"), "classify should return class_N: {s}");
        }
        _ => panic!("expected text from classify(), got: {val:?}"),
    }
}

#[tokio::test]
async fn test_create_model_without_onnx() {
    // Without the onnx feature, CREATE MODEL should return a helpful error.
    let ex = test_executor();
    let result = ex.execute("CREATE MODEL 'test' FROM '/tmp/model.onnx'").await;
    #[cfg(not(feature = "onnx"))]
    assert!(result.is_err(), "CREATE MODEL should fail without onnx feature");
    #[cfg(feature = "onnx")]
    {
        // With onnx feature, it should fail because the file doesn't exist.
        assert!(result.is_err(), "CREATE MODEL should fail with bad path");
    }
}

#[tokio::test]
async fn test_show_models_empty() {
    let ex = test_executor();
    let results = exec(&ex, "SHOW MODELS").await;
    match &results[0] {
        ExecResult::Select { rows, columns } => {
            assert!(rows.is_empty(), "no models registered yet");
            assert_eq!(columns.len(), 4);
            assert_eq!(columns[0].0, "name");
        }
        _ => panic!("SHOW MODELS should return Select"),
    }
}

#[tokio::test]
async fn test_drop_model() {
    let ex = test_executor();
    ex.model_registry.write().register_linear("to_drop", vec![1.0], 0.0);
    assert_eq!(ex.model_registry.read().list_models().len(), 1);
    exec(&ex, "DROP MODEL to_drop").await;
    assert_eq!(ex.model_registry.read().list_models().len(), 0);
}

#[tokio::test]
async fn test_show_models_with_registered() {
    let ex = test_executor();
    ex.model_registry.write().register_linear("my_linear", vec![1.0, 2.0], 0.5);
    let results = exec(&ex, "SHOW MODELS").await;
    match &results[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("my_linear".into()));
        }
        _ => panic!("SHOW MODELS should return Select"),
    }
}

// New scalar function tests
// ======================================================================

#[tokio::test]
async fn test_split_part() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT SPLIT_PART('a.b.c', '.', 2)").await;
    assert_eq!(*scalar(&results[0]), Value::Text("b".into()));
}

#[tokio::test]
async fn test_translate() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TRANSLATE('hello', 'helo', 'HELO')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("HELLO".into()));
}

#[tokio::test]
async fn test_starts_with() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT STARTS_WITH('hello world', 'hello')").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(true));
}

#[tokio::test]
async fn test_ascii_chr() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ASCII('A')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(65));
    let results = exec(&ex, "SELECT CHR(65)").await;
    assert_eq!(*scalar(&results[0]), Value::Text("A".into()));
}

#[tokio::test]
async fn test_trig_functions() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT SIN(0)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(0.0));
    let results = exec(&ex, "SELECT COS(0)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(1.0));
}

#[tokio::test]
async fn test_gcd_lcm() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT GCD(12, 8)").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(4));
    let results = exec(&ex, "SELECT LCM(4, 6)").await;
    assert_eq!(*scalar(&results[0]), Value::Int64(12));
}

#[tokio::test]
async fn test_generate_series() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT GENERATE_SERIES(1, 5)").await;
    match scalar(&results[0]) {
        Value::Array(vals) => assert_eq!(vals.len(), 5),
        _ => panic!("expected array"),
    }
}

#[tokio::test]
async fn test_date_trunc() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE_TRUNC('month', MAKE_DATE(2024, 3, 15))").await;
    let val = scalar(&results[0]);
    assert_eq!(*val, Value::Date(crate::types::ymd_to_days(2024, 3, 1)));
}

#[tokio::test]
async fn test_date_part() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE_PART('year', MAKE_DATE(2024, 3, 15))").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(2024));
    let results = exec(&ex, "SELECT DATE_PART('month', MAKE_DATE(2024, 3, 15))").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(3));
    let results = exec(&ex, "SELECT DATE_PART('day', MAKE_DATE(2024, 3, 15))").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(15));
}

#[tokio::test]
async fn test_make_date() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT MAKE_DATE(2024, 1, 1)").await;
    assert_eq!(*scalar(&results[0]), Value::Date(crate::types::ymd_to_days(2024, 1, 1)));
}

#[tokio::test]
async fn test_to_char() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TO_CHAR(MAKE_DATE(2024, 3, 15), 'YYYY-MM-DD')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("2024-03-15".into()));
}

#[tokio::test]
async fn test_jsonb_set() {
    let ex = test_executor();
    // Test JSONB_SET with jsonb args
    let results = exec(&ex, "SELECT JSONB_SET('{\"a\": 1, \"b\": 2}'::JSONB, 'c'::TEXT, '3'::TEXT)").await;
    // JSONB_SET should add key 'c'
    let val = scalar(&results[0]);
    match val {
        Value::Jsonb(v) => assert!(v.get("c").is_some()),
        _ => panic!("expected jsonb, got {val:?}"),
    }
}

#[tokio::test]
async fn test_jsonb_pretty() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSONB_PRETTY('{\"a\":1}'::JSONB)").await;
    let val = scalar(&results[0]);
    match val {
        Value::Text(s) => assert!(s.contains('\n')),
        _ => panic!("expected text"),
    }
}

#[tokio::test]
async fn test_jsonb_object_keys() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSONB_OBJECT_KEYS('{\"a\":1,\"b\":2}'::JSONB)").await;
    match scalar(&results[0]) {
        Value::Jsonb(serde_json::Value::Array(arr)) => assert_eq!(arr.len(), 2),
        _ => panic!("expected jsonb array"),
    }
}

#[tokio::test]
async fn test_jsonb_extract_path() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSONB_EXTRACT_PATH_TEXT('{\"a\":{\"b\":\"hello\"}}'::JSONB, 'a', 'b')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("hello".into()));
}

#[tokio::test]
async fn test_json_build_object_returns_valid_json() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSON_BUILD_OBJECT('name', 'Alice', 'age', 30)").await;
    match scalar(&results[0]) {
        Value::Jsonb(v) => {
            assert_eq!(v["name"], "Alice");
            assert_eq!(v["age"], 30);
            // Ensure it round-trips through serde as valid JSON
            let serialized = serde_json::to_string(v).unwrap();
            let _parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        }
        other => panic!("expected Jsonb, got {other:?}"),
    }
}

#[tokio::test]
async fn test_json_array_length() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSON_ARRAY_LENGTH('[1,2,3]'::JSONB)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(3));
}

#[tokio::test]
async fn test_json_typeof() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT JSON_TYPEOF('{\"a\":1}'::JSONB)").await;
    assert_eq!(*scalar(&results[0]), Value::Text("object".into()));
}

#[tokio::test]
async fn test_trunc() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT TRUNC(3.789, 1)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(3.7));
}

#[tokio::test]
async fn test_degrees_radians() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DEGREES(PI())").await;
    match scalar(&results[0]) {
        Value::Float64(f) => assert!((f - 180.0).abs() < 0.001),
        _ => panic!("expected float"),
    }
}

#[tokio::test]
async fn test_insert_with_column_list() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE collist (id INT, name TEXT, status TEXT)").await;
    exec(&ex, "INSERT INTO collist (id, name) VALUES (1, 'alice')").await;
    let results = exec(&ex, "SELECT status FROM collist WHERE id = 1").await;
    assert_eq!(*scalar(&results[0]), Value::Null);
}

#[tokio::test]
async fn test_octet_length() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT OCTET_LENGTH('hello')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(5));
}

#[tokio::test]
async fn test_bit_length() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT BIT_LENGTH('hello')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(40));
}


// EXTRACT syntax tests
// ======================================================================

#[tokio::test]
async fn test_extract_from_date() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT EXTRACT(YEAR FROM MAKE_DATE(2024, 6, 15))").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(2024));
    let results = exec(&ex, "SELECT EXTRACT(MONTH FROM MAKE_DATE(2024, 6, 15))").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(6));
    let results = exec(&ex, "SELECT EXTRACT(DAY FROM MAKE_DATE(2024, 6, 15))").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(15));
}

// ======================================================================

// IS DISTINCT FROM tests
// ======================================================================

#[tokio::test]
async fn test_is_distinct_from() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT 1 IS DISTINCT FROM 2").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(true));
    let results = exec(&ex, "SELECT 1 IS DISTINCT FROM 1").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(false));
    let results = exec(&ex, "SELECT NULL IS DISTINCT FROM NULL").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(false));
    let results = exec(&ex, "SELECT NULL IS NOT DISTINCT FROM NULL").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(true));
    let results = exec(&ex, "SELECT 1 IS DISTINCT FROM NULL").await;
    assert_eq!(*scalar(&results[0]), Value::Bool(true));
}

// ======================================================================

// Type cast tests
// ======================================================================

#[tokio::test]
async fn test_cast_to_date() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST('2024-03-15' AS DATE)").await;
    assert_eq!(*scalar(&results[0]), Value::Date(crate::types::ymd_to_days(2024, 3, 15)));
}

#[tokio::test]
async fn test_cast_to_numeric() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(42 AS NUMERIC)").await;
    assert_eq!(*scalar(&results[0]), Value::Numeric("42".to_string()));
}

#[tokio::test]
async fn test_cast_text_to_int() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST('123' AS INT)").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(123));
}

#[tokio::test]
async fn test_cast_int_to_float() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(42 AS DOUBLE PRECISION)").await;
    assert_eq!(*scalar(&results[0]), Value::Float64(42.0));
}

// ======================================================================

// Array constructor tests
// ======================================================================

#[tokio::test]
async fn test_array_constructor() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT ARRAY[1, 2, 3]").await;
    match scalar(&results[0]) {
        Value::Array(vals) => {
            assert_eq!(vals.len(), 3);
            assert_eq!(vals[0], Value::Int32(1));
        }
        _ => panic!("expected array"),
    }
}

// ======================================================================

// Type casting tests
// ======================================================================

#[tokio::test]
async fn test_cast_text_to_integer() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST('42' AS INTEGER)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(42));
}

#[tokio::test]
async fn test_cast_text_to_bigint() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST('9999999999' AS BIGINT)").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(9999999999));
}

#[tokio::test]
async fn test_cast_text_to_float() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST('3.14' AS DOUBLE PRECISION)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(3.14));
}

#[tokio::test]
async fn test_cast_text_to_boolean() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST('true' AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));

    let results = exec(&ex, "SELECT CAST('false' AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(false));

    let results = exec(&ex, "SELECT CAST('yes' AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));
}

#[tokio::test]
async fn test_cast_int_to_text() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(123 AS TEXT)").await;
    assert_eq!(scalar(&results[0]), &Value::Text("123".into()));
}

#[tokio::test]
async fn test_cast_bool_to_int() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(TRUE AS INTEGER)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(1));

    let results = exec(&ex, "SELECT CAST(FALSE AS INTEGER)").await;
    assert_eq!(scalar(&results[0]), &Value::Int32(0));
}

#[tokio::test]
async fn test_cast_bool_to_bigint() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(TRUE AS BIGINT)").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(1));

    let results = exec(&ex, "SELECT CAST(FALSE AS BIGINT)").await;
    assert_eq!(scalar(&results[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_cast_int64_to_boolean() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(CAST(1 AS BIGINT) AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));

    let results = exec(&ex, "SELECT CAST(CAST(0 AS BIGINT) AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_cast_float_to_boolean() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(1.5 AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(true));

    let results = exec(&ex, "SELECT CAST(0.0 AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_cast_bool_to_float() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(TRUE AS DOUBLE PRECISION)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(1.0));
}

#[tokio::test]
async fn test_cast_null_passthrough() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CAST(NULL AS INTEGER)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);

    let results = exec(&ex, "SELECT CAST(NULL AS TEXT)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);

    let results = exec(&ex, "SELECT CAST(NULL AS BOOLEAN)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);

    let results = exec(&ex, "SELECT CAST(NULL AS BIGINT)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);

    let results = exec(&ex, "SELECT CAST(NULL AS DOUBLE PRECISION)").await;
    assert_eq!(scalar(&results[0]), &Value::Null);
}

// ======================================================================

// LOG10 math function test
// ======================================================================

#[tokio::test]
async fn test_log10_function() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT LOG10(100)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(2.0));

    let results = exec(&ex, "SELECT LOG10(1000)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(3.0));

    let results = exec(&ex, "SELECT LOG10(1)").await;
    assert_eq!(scalar(&results[0]), &Value::Float64(0.0));
}

// ======================================================================

// Date/time function tests
// ======================================================================

#[tokio::test]
async fn test_now_returns_timestamp_like_string() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT NOW()").await;
    // NOW() now returns Value::TimestampTz; its Display is "YYYY-MM-DD HH:MM:SS+00"
    match scalar(&results[0]) {
        Value::TimestampTz(us) => {
            let s = Value::TimestampTz(*us).to_string();
            assert!(s.contains("-"), "expected date separator, got: {s}");
            assert!(s.contains(":"), "expected time separator, got: {s}");
        }
        other => panic!("expected TimestampTz, got {other:?}"),
    }
}

#[tokio::test]
async fn test_current_time() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CURRENT_TIME()").await;
    match scalar(&results[0]) {
        Value::Text(s) => {
            assert!(s.contains(":"), "expected time with colons, got: {s}");
            assert_eq!(s.len(), 8, "expected HH:MM:SS format, got: {s}");
        }
        other => panic!("expected Text, got {other:?}"),
    }
}

#[tokio::test]
async fn test_clock_timestamp() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT CLOCK_TIMESTAMP()").await;
    // CLOCK_TIMESTAMP() returns Value::TimestampTz like NOW()
    match scalar(&results[0]) {
        Value::TimestampTz(us) => assert!(*us > 0, "expected positive timestamp: {us}"),
        other => panic!("expected TimestampTz, got {other:?}"),
    }
}

#[tokio::test]
async fn test_extract_year_from_text() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT EXTRACT(YEAR FROM '2024-06-15')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(2024));
}

#[tokio::test]
async fn test_extract_month_day_from_text() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT EXTRACT(MONTH FROM '2024-06-15')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(6));
    let results = exec(&ex, "SELECT EXTRACT(DAY FROM '2024-06-15')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(15));
}

#[tokio::test]
async fn test_extract_hour_minute_second_from_text() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT EXTRACT(HOUR FROM '2024-06-15 14:30:45')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(14));
    let results = exec(&ex, "SELECT EXTRACT(MINUTE FROM '2024-06-15 14:30:45')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(30));
    let results = exec(&ex, "SELECT EXTRACT(SECOND FROM '2024-06-15 14:30:45')").await;
    assert_eq!(*scalar(&results[0]), Value::Int32(45));
}

#[tokio::test]
async fn test_date_trunc_text_month() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE_TRUNC('month', '2024-06-15 14:30:00')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("2024-06-01 00:00:00".into()));
}

#[tokio::test]
async fn test_date_trunc_text_year() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE_TRUNC('year', '2024-06-15 14:30:00')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("2024-01-01 00:00:00".into()));
}

#[tokio::test]
async fn test_date_trunc_text_day() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE_TRUNC('day', '2024-06-15 14:30:00')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("2024-06-15 00:00:00".into()));
}

#[tokio::test]
async fn test_date_trunc_text_hour() {
    let ex = test_executor();
    let results = exec(&ex, "SELECT DATE_TRUNC('hour', '2024-06-15 14:30:45')").await;
    assert_eq!(*scalar(&results[0]), Value::Text("2024-06-15 14:00:00".into()));
}

#[tokio::test]
async fn test_discard_all() {
    let ex = test_executor();
    exec(&ex, "SET search_path TO myschema").await;
    exec(&ex, "DISCARD ALL").await;
    let results = exec(&ex, "SHOW search_path").await;
    let value = scalar(&results[0]);
    assert!(matches!(value, Value::Text(s) if s == "public"));
}

#[tokio::test]
async fn test_reset_all() {
    let ex = test_executor();
    exec(&ex, "SET search_path TO schema1, schema2").await;
    exec(&ex, "RESET ALL").await;
    let results = exec(&ex, "SHOW search_path").await;
    let value = scalar(&results[0]);
    assert!(matches!(value, Value::Text(s) if s == "public"));
}

#[tokio::test]
async fn test_reset_specific() {
    let ex = test_executor();
    exec(&ex, "SET search_path TO myschema").await;
    exec(&ex, "RESET search_path").await;
    let results = exec(&ex, "SHOW search_path").await;
    let value = scalar(&results[0]);
    assert!(matches!(value, Value::Text(s) if s == "public"));
}

#[tokio::test]
async fn test_show_all() {
    let ex = test_executor();
    exec(&ex, "SET search_path TO custom_schema").await;
    let results = exec(&ex, "SHOW ALL").await;
    let rows_vec = rows(&results[0]);
    assert!(rows_vec.len() > 10);
    if let ExecResult::Select { columns, .. } = &results[0] {
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].0, "name");
    }
}

// ======================================================================
