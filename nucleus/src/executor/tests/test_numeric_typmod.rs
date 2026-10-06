//! NUMERIC(p, s) and exact decimal literals (NP03 follow-up).
//!
//! A declared precision and scale is enforced on every write and on an explicit
//! cast: the value is rounded half away from zero to the scale, padded to it,
//! and refused with 22003 when it needs more than `p - s` integer digits. A
//! declaration PostgreSQL rejects is 22023, one it accepts but Nucleus cannot
//! hold (negative scale, scale past precision, scale past the 28 digits of the
//! physical decimal) is 0A000. Decimal literals keep their digits when they meet
//! a NUMERIC column or operand instead of passing through f64.
//!
//! Every assertion here is one the code before this change violates.

use super::*;
use crate::types::{DataType, NumericTypmod, Value};
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

fn sqlstate_of(err: &ExecError) -> String {
    let codec = PgWireErrorCodec;
    codec.code_to_string(codec.encode(err).code)
}

/// The SQLSTATE of a statement that must fail.
async fn refused(ex: &Executor, sql: &str) -> String {
    let err = ex
        .execute(sql)
        .await
        .expect_err(&format!("must be refused: {sql}"));
    sqlstate_of(&err)
}

/// One column of one table, in id order, as NUMERIC text (NULL as `None`).
async fn numerics(ex: &Executor, sql: &str) -> Vec<Option<String>> {
    let results = exec(ex, sql).await;
    rows(&results[0])
        .iter()
        .map(|row| match &row[0] {
            Value::Numeric(text) => Some(text.clone()),
            Value::Null => None,
            other => panic!("{sql}: expected NUMERIC or NULL, got {other:?}"),
        })
        .collect()
}

async fn numeric_of(ex: &Executor, sql: &str) -> String {
    let results = exec(ex, sql).await;
    match scalar(&results[0]) {
        Value::Numeric(text) => text.clone(),
        other => panic!("{sql}: expected NUMERIC, got {other:?}"),
    }
}

fn some(text: &str) -> Option<String> {
    Some(text.to_string())
}

const MAX_NUMERIC: &str = "79228162514264337593543950335";

// ---------------------------------------------------------------------------
// The typmod itself
// ---------------------------------------------------------------------------

/// A write rounds half away from zero to the scale and pads to it. The old
/// column kept whatever was written ('1.005', '2.5').
#[tokio::test]
async fn typmod_rounds_half_away_from_zero_and_pads_the_scale() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE typ_round (id INT PRIMARY KEY, v NUMERIC(5,2))",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO typ_round VALUES (1, '1.005'), (2, '-1.005'), (3, '2.5'), \
         (4, '0.994'), (5, 3.1), (6, 7), (7, '999.994'), (8, '-0.001'), (9, NULL)",
    )
    .await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM typ_round ORDER BY id").await,
        vec![
            some("1.01"),
            some("-1.01"),
            some("2.50"),
            some("0.99"),
            some("3.10"),
            some("7.00"),
            some("999.99"),
            // PostgreSQL has no negative zero.
            some("0.00"),
            None,
        ]
    );
}

/// More than `p - s` integer digits after rounding is 22003 `numeric field
/// overflow`, on INSERT, on UPDATE (the row keeps its old value) and when an
/// expression produces the value.
#[tokio::test]
async fn typmod_overflow_is_refused_with_22003() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE typ_over (id INT PRIMARY KEY, v NUMERIC(5,2))",
    )
    .await;
    exec(&ex, "INSERT INTO typ_over VALUES (1, 1.5)").await;
    for sql in [
        "INSERT INTO typ_over VALUES (2, '1000')",
        // Rounds up to 1000.00, which needs four integer digits.
        "INSERT INTO typ_over VALUES (3, '999.995')",
        "INSERT INTO typ_over VALUES (4, -1000.5)",
        "UPDATE typ_over SET v = 1000 WHERE id = 1",
        "UPDATE typ_over SET v = v * 1000 WHERE id = 1",
        "INSERT INTO typ_over VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = 1000",
    ] {
        assert_eq!(refused(&ex, sql).await, "22003", "{sql}");
    }
    let err = ex
        .execute("INSERT INTO typ_over VALUES (5, '12345.678')")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("numeric field overflow"), "{err}");
    // Nothing was written by a refused statement.
    assert_eq!(
        numerics(&ex, "SELECT v FROM typ_over ORDER BY id").await,
        vec![some("1.50")]
    );
}

/// ON CONFLICT DO UPDATE does not coerce its assignments, so it applies the
/// typmod itself.
#[tokio::test]
async fn on_conflict_update_applies_the_typmod() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE typ_upsert (id INT PRIMARY KEY, v NUMERIC(5,2))",
    )
    .await;
    exec(&ex, "INSERT INTO typ_upsert VALUES (1, 1)").await;
    exec(
        &ex,
        "INSERT INTO typ_upsert VALUES (1, 9) ON CONFLICT (id) DO UPDATE SET v = 1.005",
    )
    .await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM typ_upsert").await,
        vec![some("1.01")]
    );
}

/// precision == scale leaves no integer digit; scale 0 rounds to an integer.
#[tokio::test]
async fn typmod_edge_declarations() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE typ_edge (id INT, a NUMERIC(2,2), b NUMERIC(3))",
    )
    .await;
    exec(&ex, "INSERT INTO typ_edge VALUES (1, '0.994', '2.5')").await;
    exec(&ex, "INSERT INTO typ_edge VALUES (2, '-0.5', '-2.5')").await;
    exec(&ex, "INSERT INTO typ_edge VALUES (3, '0.1', '999.4')").await;
    assert_eq!(
        numerics(&ex, "SELECT a FROM typ_edge ORDER BY id").await,
        vec![some("0.99"), some("-0.50"), some("0.10")]
    );
    assert_eq!(
        numerics(&ex, "SELECT b FROM typ_edge ORDER BY id").await,
        vec![some("3"), some("-3"), some("999")]
    );
    for sql in [
        // 0.995 rounds to 1.00: one integer digit where none is allowed.
        "INSERT INTO typ_edge VALUES (4, '0.995', '1')",
        "INSERT INTO typ_edge VALUES (5, '1', '1')",
        // 999.5 rounds to 1000.
        "INSERT INTO typ_edge VALUES (6, '0.1', '999.5')",
    ] {
        assert_eq!(refused(&ex, sql).await, "22003", "{sql}");
    }
}

/// Wide values and the 28-digit scale ceiling. Padding to the declared scale
/// must still fit the 96-bit coefficient, or the value is refused (22003)
/// rather than stored in a form the parser would later reject.
#[tokio::test]
async fn typmod_wide_values_and_the_scale_ceiling() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE typ_wide (id INT PRIMARY KEY, a NUMERIC(40,2), b NUMERIC(29), \
         c NUMERIC(28), d NUMERIC(30,28), e NUMERIC(1000,28))",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO typ_wide (id, a) VALUES (1, '1234567890123456789012345.678')",
    )
    .await;
    assert_eq!(
        numerics(&ex, "SELECT a FROM typ_wide").await,
        vec![some("1234567890123456789012345.68")]
    );
    // The largest value fits NUMERIC(29) (29 digits) and not NUMERIC(28).
    exec(
        &ex,
        &format!("INSERT INTO typ_wide (id, b) VALUES (2, '{MAX_NUMERIC}')"),
    )
    .await;
    assert_eq!(
        numerics(&ex, "SELECT b FROM typ_wide WHERE id = 2").await,
        vec![some(MAX_NUMERIC)]
    );
    assert_eq!(
        refused(
            &ex,
            &format!("INSERT INTO typ_wide (id, c) VALUES (3, '{MAX_NUMERIC}')")
        )
        .await,
        "22003"
    );
    // The same value padded to scale 2 needs 31 digits: past the coefficient.
    assert_eq!(
        refused(
            &ex,
            &format!("INSERT INTO typ_wide (id, a) VALUES (4, '{MAX_NUMERIC}')")
        )
        .await,
        "22003"
    );
    // Scale 28 padding: 1.5 becomes 29 digits (fits), 12.5 becomes 30 (does not).
    exec(&ex, "INSERT INTO typ_wide (id, d) VALUES (5, '1.5')").await;
    assert_eq!(
        numerics(&ex, "SELECT d FROM typ_wide WHERE id = 5").await,
        vec![some("1.5000000000000000000000000000")]
    );
    assert_eq!(
        refused(&ex, "INSERT INTO typ_wide (id, d) VALUES (6, '12.5')").await,
        "22003"
    );
    // A 28-digit fraction at the largest precision PostgreSQL allows.
    exec(
        &ex,
        "INSERT INTO typ_wide (id, e) VALUES (7, '0.1234567890123456789012345678')",
    )
    .await;
    assert_eq!(
        numerics(&ex, "SELECT e FROM typ_wide WHERE id = 7").await,
        vec![some("0.1234567890123456789012345678")]
    );
}

/// Invalid declarations. 22023 where PostgreSQL itself rejects the number,
/// 0A000 where PostgreSQL accepts it and Nucleus cannot hold it; a refused
/// CREATE leaves nothing behind.
#[tokio::test]
async fn invalid_declarations_are_refused_with_their_sqlstates() {
    let ex = test_executor();
    for (decl, state) in [
        ("NUMERIC(0)", "22023"),
        ("NUMERIC(0,0)", "22023"),
        ("DECIMAL(1001)", "22023"),
        ("NUMERIC(5,1001)", "22023"),
        ("NUMERIC(5,6)", "0A000"),
        ("NUMERIC(5,-1)", "0A000"),
        ("NUMERIC(40,29)", "0A000"),
    ] {
        let sql = format!("CREATE TABLE bad_decl (v {decl})");
        assert_eq!(refused(&ex, &sql).await, state, "{decl}");
    }
    // Nothing was created by the refusals above.
    exec(&ex, "CREATE TABLE bad_decl (v NUMERIC(40,28))").await;
    exec(&ex, "CREATE TABLE bad_decl_max (v NUMERIC(1000,28))").await;
    assert_eq!(
        refused(&ex, "ALTER TABLE bad_decl ADD COLUMN w NUMERIC(0)").await,
        "22023"
    );
    assert_eq!(
        refused(&ex, "ALTER TABLE bad_decl ALTER COLUMN v TYPE NUMERIC(5,6)").await,
        "0A000"
    );
    assert_eq!(refused(&ex, "SELECT CAST(1 AS NUMERIC(0))").await, "22023");
    assert_eq!(
        refused(&ex, "SELECT CAST(1 AS NUMERIC(5,6))").await,
        "0A000"
    );
}

// ---------------------------------------------------------------------------
// CAST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cast_applies_the_typmod() {
    let ex = test_executor();
    for (sql, expected) in [
        ("SELECT CAST('123.456' AS NUMERIC(5,2))", "123.46"),
        ("SELECT CAST('-123.456' AS NUMERIC(5,2))", "-123.46"),
        ("SELECT CAST(7 AS NUMERIC(5,2))", "7.00"),
        // The shorthand, with a decimal literal that must not pass through f64.
        ("SELECT 2.675::NUMERIC(5,2)", "2.68"),
        (
            "SELECT CAST(0.1234567890123456789 AS NUMERIC(25,19))",
            "0.1234567890123456789",
        ),
        // A signed literal with an exponent.
        ("SELECT CAST(-1.5e1 AS NUMERIC(6,2))", "-15.00"),
        // float8 goes through PostgreSQL's 15 significant digits first.
        (
            "SELECT CAST(CAST(2.675 AS DOUBLE PRECISION) AS NUMERIC(5,2))",
            "2.68",
        ),
    ] {
        assert_eq!(numeric_of(&ex, sql).await, expected, "{sql}");
    }
    assert_eq!(
        refused(&ex, "SELECT CAST('1234.5' AS NUMERIC(5,2))").await,
        "22003"
    );
    assert_eq!(
        scalar(&exec(&ex, "SELECT CAST(NULL AS NUMERIC(5,2))").await[0]),
        &Value::Null
    );
}

// ---------------------------------------------------------------------------
// ALTER / DEFAULT / dump
// ---------------------------------------------------------------------------

/// ALTER COLUMN TYPE numeric(p, s) rewrites the stored values even though the
/// engine type does not change, refuses (and changes nothing) when a value no
/// longer fits, and numeric(p, s) -> numeric stops enforcing.
#[tokio::test]
async fn alter_column_type_rewrites_and_enforces_the_new_typmod() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE alt_typ (id INT PRIMARY KEY, v NUMERIC)").await;
    exec(
        &ex,
        "INSERT INTO alt_typ VALUES (1, '1.25'), (2, '2.35'), (3, '10.005')",
    )
    .await;
    exec(&ex, "ALTER TABLE alt_typ ALTER COLUMN v TYPE NUMERIC(6,1)").await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM alt_typ ORDER BY id").await,
        vec![some("1.3"), some("2.4"), some("10.0")]
    );
    exec(&ex, "INSERT INTO alt_typ VALUES (4, '5.55')").await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM alt_typ WHERE id = 4").await,
        vec![some("5.6")]
    );

    // 10.0 needs two integer digits; NUMERIC(2,1) allows one.
    assert_eq!(
        refused(&ex, "ALTER TABLE alt_typ ALTER COLUMN v TYPE NUMERIC(2,1)").await,
        "22003"
    );
    assert_eq!(
        numerics(&ex, "SELECT v FROM alt_typ ORDER BY id").await,
        vec![some("1.3"), some("2.4"), some("10.0"), some("5.6")],
        "a refused ALTER must leave every value untouched"
    );
    // The refused ALTER also left the old declaration in force.
    exec(&ex, "INSERT INTO alt_typ VALUES (5, '99.99')").await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM alt_typ WHERE id = 5").await,
        vec![some("100.0")]
    );

    exec(&ex, "ALTER TABLE alt_typ ALTER COLUMN v TYPE NUMERIC").await;
    exec(&ex, "INSERT INTO alt_typ VALUES (6, '1.2345')").await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM alt_typ WHERE id = 6").await,
        vec![some("1.2345")],
        "an unconstrained numeric must stop rounding"
    );
}

/// ADD COLUMN with a typmod and a decimal DEFAULT: the backfilled value and a
/// later defaulted INSERT are both padded to the scale.
#[tokio::test]
async fn default_and_add_column_apply_the_typmod() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE def_typ (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO def_typ VALUES (1), (2)").await;
    exec(
        &ex,
        "ALTER TABLE def_typ ADD COLUMN w NUMERIC(5,2) DEFAULT 1.5",
    )
    .await;
    exec(
        &ex,
        "ALTER TABLE def_typ ADD COLUMN x NUMERIC(8,3) DEFAULT 2.5",
    )
    .await;
    exec(&ex, "INSERT INTO def_typ (id) VALUES (3)").await;
    assert_eq!(
        numerics(&ex, "SELECT w FROM def_typ ORDER BY id").await,
        vec![some("1.50"), some("1.50"), some("1.50")]
    );
    assert_eq!(
        numerics(&ex, "SELECT x FROM def_typ ORDER BY id").await,
        vec![some("2.500"), some("2.500"), some("2.500")]
    );
}

/// A numeric(p, s)[] column applies the declaration to every element.
#[tokio::test]
async fn array_columns_apply_the_element_typmod() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE typ_arr (id INT PRIMARY KEY, v NUMERIC(5,2)[])",
    )
    .await;
    exec(&ex, "INSERT INTO typ_arr VALUES (1, '{1.005,2}')").await;
    let result = exec(&ex, "SELECT v FROM typ_arr").await;
    assert_eq!(
        scalar(&result[0]),
        &Value::Array(vec![
            Value::Numeric("1.01".into()),
            Value::Numeric("2.00".into())
        ])
    );
    assert_eq!(
        refused(&ex, "INSERT INTO typ_arr VALUES (2, '{1000}')").await,
        "22003"
    );
}

/// A logical dump keeps the declaration and the written scale, so a restore
/// still rounds and refuses (and does not turn 1.50 into 1.5).
#[tokio::test]
async fn logical_dump_round_trips_the_typmod_and_exact_values() {
    let src = test_executor();
    exec(
        &src,
        "CREATE TABLE dump_typ (id INT PRIMARY KEY, v NUMERIC(8,3))",
    )
    .await;
    exec(
        &src,
        "INSERT INTO dump_typ VALUES (1, 1.5), (2, '-0.0005'), (3, 12345.678)",
    )
    .await;
    let script = src.dump_logical().await.expect("dump");
    assert!(script.contains("NUMERIC(8,3)"), "{script}");

    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");
    assert_eq!(
        numerics(&dst, "SELECT v FROM dump_typ ORDER BY id").await,
        numerics(&src, "SELECT v FROM dump_typ ORDER BY id").await,
    );
    assert_eq!(
        numerics(&dst, "SELECT v FROM dump_typ ORDER BY id").await,
        vec![some("1.500"), some("-0.001"), some("12345.678")]
    );
    assert_eq!(
        refused(&dst, "INSERT INTO dump_typ VALUES (4, '123456')").await,
        "22003",
        "the restored column must still enforce its precision"
    );
}

/// The catalog reports the same declaration that is enforced.
#[tokio::test]
async fn catalog_reports_the_enforced_declaration() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cat_typ (v NUMERIC(10,2))").await;
    let result = exec(
        &ex,
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
         WHERE attrelid = 'cat_typ'::regclass AND attname = 'v'",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Text("numeric(10,2)".into()));
    let result = exec(
        &ex,
        "SELECT numeric_precision, numeric_scale FROM information_schema.columns \
         WHERE table_name = 'cat_typ'",
    )
    .await;
    assert_eq!(
        rows(&result[0]),
        &vec![vec![Value::Int32(10), Value::Int32(2)]]
    );
}

// ---------------------------------------------------------------------------
// atttypmod encoding (what pg_attribute and a typmod-carrying wire layer use)
// ---------------------------------------------------------------------------

#[test]
fn atttypmod_round_trips_and_matches_postgresql() {
    // ((p << 16) | s) + 4, PostgreSQL's NUMERIC(10,2) is 655366.
    let typmod = NumericTypmod::new(10, 2).unwrap();
    assert_eq!(typmod.atttypmod(), 655_366);
    assert_eq!(NumericTypmod::from_atttypmod(655_366), Some(typmod));
    for (p, s) in [(1, 0), (5, 5), (38, 28), (1000, 28), (29, 0), (1000, 0)] {
        let typmod = NumericTypmod::new(p, s).unwrap();
        assert_eq!(
            NumericTypmod::from_atttypmod(typmod.atttypmod()),
            Some(typmod)
        );
    }
    // -1 is "unconstrained"; nothing below 4 is a declaration.
    for unconstrained in [-1, 0, 3] {
        assert_eq!(NumericTypmod::from_atttypmod(unconstrained), None);
    }
}

#[test]
fn typmod_apply_rounds_on_digits_never_through_a_float() {
    let t = NumericTypmod::new(30, 10).unwrap();
    assert_eq!(
        t.apply("0.12345678905").unwrap(),
        "0.1234567891",
        "a trailing 5 rounds away from zero"
    );
    assert_eq!(t.apply("-0.12345678905").unwrap(), "-0.1234567891");
    assert_eq!(t.apply("9.99999999995").unwrap(), "10.0000000000");
    assert_eq!(t.apply("1").unwrap(), "1.0000000000");
    assert_eq!(t.apply("-0.00000000004").unwrap(), "0.0000000000");
    let err = t.apply("100000000000000000000").unwrap_err();
    assert!(err.contains("numeric field overflow"), "{err}");
    // Arrays are applied element by element; other values are untouched.
    let mut array = Value::Array(vec![Value::Numeric("1.5".into()), Value::Null]);
    NumericTypmod::new(5, 2)
        .unwrap()
        .apply_to_value(&mut array)
        .unwrap();
    assert_eq!(
        array,
        Value::Array(vec![Value::Numeric("1.50".into()), Value::Null])
    );
}

// ---------------------------------------------------------------------------
// Literal typing
// ---------------------------------------------------------------------------

/// A decimal literal written into a NUMERIC column keeps every digit and its
/// written scale. It used to be evaluated as f64 first: 1.10 was stored as 1.1
/// and anything past about 17 significant digits was silently different.
#[tokio::test]
async fn decimal_literals_into_numeric_columns_keep_their_digits() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lit (id INT PRIMARY KEY, v NUMERIC)").await;
    exec(
        &ex,
        "INSERT INTO lit VALUES (1, 0.1234567890123456789012345678), (2, 1.10), \
         (3, -0.50), (4, 1.5e1), (5, 12345678901234567890.123)",
    )
    .await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM lit ORDER BY id").await,
        vec![
            some("0.1234567890123456789012345678"),
            some("1.10"),
            some("-0.50"),
            some("15"),
            some("12345678901234567890.123"),
        ]
    );
    exec(&ex, "UPDATE lit SET v = 3.140 WHERE id = 1").await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM lit WHERE id = 1").await,
        vec![some("3.140")]
    );
    // A literal that cannot be held exactly is refused, not rounded to float.
    for sql in [
        "INSERT INTO lit VALUES (6, 0.12345678901234567890123456789012)",
        "INSERT INTO lit VALUES (7, 1e29)",
        "UPDATE lit SET v = 0.00000000000000000000000000001 WHERE id = 1",
    ] {
        assert_eq!(refused(&ex, sql).await, "22003", "{sql}");
    }
    assert_eq!(
        numerics(&ex, "SELECT v FROM lit WHERE id = 1").await,
        vec![some("3.140")]
    );
}

/// Beside a NUMERIC or integer operand a decimal literal is NUMERIC, so the
/// arithmetic is exact. Beside a float8 operand it stays float8.
#[tokio::test]
async fn decimal_literals_beside_numeric_operands_are_numeric() {
    let ex = test_executor();
    for (sql, expected) in [
        ("SELECT CAST('0.1' AS NUMERIC) + 0.2", "0.3"),
        ("SELECT 0.2 + CAST('0.1' AS NUMERIC)", "0.3"),
        ("SELECT CAST('3' AS NUMERIC) * 0.1", "0.3"),
        // An integer operand: float8 would give 0.30000000000000004.
        ("SELECT 3 * 0.1", "0.3"),
        ("SELECT -0.5 + 3", "2.5"),
        (
            "SELECT CAST('1' AS NUMERIC) - 0.1234567890123456789",
            "0.8765432109876543211",
        ),
    ] {
        assert_eq!(numeric_of(&ex, sql).await, expected, "{sql}");
    }

    exec(&ex, "CREATE TABLE fl (x DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO fl VALUES (0.1)").await;
    assert_eq!(
        scalar(&exec(&ex, "SELECT x + 0.2 FROM fl").await[0]),
        &Value::Float64(0.1f64 + 0.2f64),
        "float8 contexts stay float8"
    );
    // NUMERIC with float8 is float8 arithmetic (PostgreSQL promotes the
    // NUMERIC); it used to be a refusal.
    assert_eq!(
        scalar(&exec(&ex, "SELECT x * CAST('2' AS NUMERIC) FROM fl").await[0]),
        &Value::Float64(0.1f64 * 2.0f64)
    );
    // A literal beside a NUMERIC that NUMERIC cannot hold is refused, not
    // compared or combined as a float.
    assert_eq!(
        refused(
            &ex,
            "SELECT CAST('1' AS NUMERIC) + 0.12345678901234567890123456789012"
        )
        .await,
        "22003"
    );
}

/// Comparison with a decimal literal is exact for a NUMERIC column. Two values
/// that differ past the 17th digit are the same double, so the f64 comparison
/// called them equal.
#[tokio::test]
async fn numeric_comparison_with_a_decimal_literal_is_exact() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE cmp_lit (v NUMERIC)").await;
    exec(&ex, "INSERT INTO cmp_lit VALUES ('0.12345678901234567890')").await;
    assert_eq!(
        scalar(&exec(&ex, "SELECT v < 0.12345678901234567891 FROM cmp_lit").await[0]),
        &Value::Bool(true)
    );
    assert_eq!(
        scalar(
            &exec(
                &ex,
                "SELECT COUNT(*) FROM cmp_lit WHERE v < 0.12345678901234567891"
            )
            .await[0]
        ),
        &Value::Int64(1)
    );
    assert_eq!(
        scalar(
            &exec(
                &ex,
                "SELECT COUNT(*) FROM cmp_lit WHERE v >= 0.12345678901234567891"
            )
            .await[0]
        ),
        &Value::Int64(0)
    );
    // The float8 comparison is unchanged: a float8 column against a literal.
    exec(&ex, "CREATE TABLE cmp_fl (x DOUBLE PRECISION)").await;
    exec(&ex, "INSERT INTO cmp_fl VALUES (0.1)").await;
    assert_eq!(
        scalar(&exec(&ex, "SELECT COUNT(*) FROM cmp_fl WHERE x = 0.1").await[0]),
        &Value::Int64(1)
    );
}

/// An integer literal beyond bigint is NUMERIC in PostgreSQL. It used to become
/// a float8 that had silently lost its low digits.
#[tokio::test]
async fn integer_literals_beyond_bigint_are_exact_or_refused() {
    let ex = test_executor();
    assert_eq!(
        numeric_of(&ex, "SELECT 9223372036854775808").await,
        "9223372036854775808"
    );
    assert_eq!(
        numeric_of(&ex, "SELECT 79228162514264337593543950335").await,
        MAX_NUMERIC
    );
    assert_eq!(
        refused(&ex, "SELECT 99999999999999999999999999999999999999").await,
        "22003"
    );
    // A decimal literal that overflows float8 is refused rather than +inf.
    assert_eq!(refused(&ex, "SELECT 1e400").await, "22003");
}

// ---------------------------------------------------------------------------
// NaN / Infinity, float8 conversion
// ---------------------------------------------------------------------------

/// NaN and the infinities are valid PostgreSQL numerics that the bounded exact
/// decimal cannot hold. The decision is a refusal with 0A000 on every route
/// into NUMERIC, before any comparison, hash or arithmetic could see one.
#[tokio::test]
async fn nan_and_infinity_are_refused_on_every_route() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE nan_t (v NUMERIC, w NUMERIC(5,2))").await;
    for sql in [
        "SELECT CAST('NaN' AS NUMERIC)",
        "SELECT 'Infinity'::NUMERIC",
        "SELECT '-Infinity'::NUMERIC",
        "SELECT CAST('NaN' AS NUMERIC(5,2))",
        "SELECT CAST(CAST('NaN' AS DOUBLE PRECISION) AS NUMERIC)",
        "SELECT CAST(CAST('Infinity' AS DOUBLE PRECISION) AS NUMERIC)",
        "SELECT CAST(CAST('-Infinity' AS DOUBLE PRECISION) AS NUMERIC)",
        "INSERT INTO nan_t (v) VALUES ('NaN')",
        "INSERT INTO nan_t (w) VALUES ('Infinity')",
    ] {
        assert_eq!(refused(&ex, sql).await, "0A000", "{sql}");
    }
    assert_eq!(
        scalar(&exec(&ex, "SELECT COUNT(*) FROM nan_t").await[0]),
        &Value::Int64(0)
    );
}

/// float8 to NUMERIC follows PostgreSQL: 15 significant digits, no invented
/// digits beyond them, and a magnitude outside the exact range is refused.
#[tokio::test]
async fn float8_to_numeric_uses_fifteen_significant_digits() {
    let ex = test_executor();
    for (sql, expected) in [
        (
            "SELECT CAST(CAST(0.30000000000000004 AS DOUBLE PRECISION) AS NUMERIC)",
            "0.3",
        ),
        (
            "SELECT CAST(CAST(123456789.123456789 AS DOUBLE PRECISION) AS NUMERIC)",
            "123456789.123457",
        ),
        (
            "SELECT CAST(CAST(1e21 AS DOUBLE PRECISION) AS NUMERIC)",
            "1000000000000000000000",
        ),
        ("SELECT CAST(CAST(0.0 AS DOUBLE PRECISION) AS NUMERIC)", "0"),
        (
            "SELECT CAST(CAST(-2.5 AS DOUBLE PRECISION) AS NUMERIC)",
            "-2.5",
        ),
    ] {
        assert_eq!(numeric_of(&ex, sql).await, expected, "{sql}");
    }
    for sql in [
        "SELECT CAST(CAST(1e300 AS DOUBLE PRECISION) AS NUMERIC)",
        // 1e-30 needs 30 fractional digits; the ceiling is 28.
        "SELECT CAST(CAST(1e-30 AS DOUBLE PRECISION) AS NUMERIC)",
    ] {
        assert_eq!(refused(&ex, sql).await, "22003", "{sql}");
    }
}

// ---------------------------------------------------------------------------
// Scalar functions, aggregates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn numeric_functions_are_exact_and_have_no_negative_zero() {
    let ex = test_executor();
    for (sql, expected) in [
        // TRUNC ignored a negative scale (returned 1234.5678 truncated to 0 dp).
        ("SELECT TRUNC(CAST('1234.5678' AS NUMERIC), -2)", "1200"),
        ("SELECT TRUNC(CAST('-1234.5678' AS NUMERIC), -2)", "-1200"),
        ("SELECT TRUNC(CAST('1234.5678' AS NUMERIC), 2)", "1234.56"),
        ("SELECT TRUNC(CAST('-0.5' AS NUMERIC))", "0"),
        ("SELECT CEIL(CAST('-0.5' AS NUMERIC))", "0"),
        ("SELECT FLOOR(CAST('-0.5' AS NUMERIC))", "-1"),
        ("SELECT ROUND(CAST('-0.4' AS NUMERIC))", "0"),
        ("SELECT ABS(CAST('-1.50' AS NUMERIC))", "1.50"),
        // Rounding at 10^29 of a value below 5e28 is 0; the old code returned 0
        // for every scale past 28, including for values at or above 5e28.
        ("SELECT ROUND(CAST('4e28' AS NUMERIC), -29)", "0"),
        ("SELECT ROUND(CAST('-4e28' AS NUMERIC), -29)", "0"),
    ] {
        assert_eq!(numeric_of(&ex, sql).await, expected, "{sql}");
    }
    // 5e28 rounds to 1e29, which the decimal cannot hold: refused, not 0.
    assert_eq!(
        refused(&ex, "SELECT ROUND(CAST('5e28' AS NUMERIC), -29)").await,
        "22003"
    );
    // A scale argument beyond i32 is an error, not wrapped to scale 0.
    assert_eq!(
        refused(&ex, "SELECT ROUND(CAST('1.5' AS NUMERIC), 4294967296)").await,
        "22003"
    );
}

/// `-x` keeps the written scale and never produces a negative zero.
#[tokio::test]
async fn unary_minus_on_a_numeric_column_has_no_negative_zero() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE neg_t (id INT PRIMARY KEY, v NUMERIC)").await;
    exec(&ex, "INSERT INTO neg_t VALUES (1, '0.00'), (2, '1.50')").await;
    assert_eq!(
        numerics(&ex, "SELECT -v FROM neg_t ORDER BY id").await,
        vec![some("0.00"), some("-1.50")]
    );
}

/// The integer/float accumulators of the untyped aggregate path skipped NUMERIC
/// values, so the sum silently dropped them.
#[test]
fn untyped_sum_and_avg_never_skip_numeric_values() {
    use super::super::helpers::compute_aggregate;
    let rows: Vec<Row> = vec![
        vec![Value::Numeric("1.5".into())],
        vec![Value::Int32(2)],
        vec![Value::Null],
        vec![Value::Numeric("0.25".into())],
    ];
    assert_eq!(
        compute_aggregate("SUM", Some(0), &rows).unwrap(),
        Value::Numeric("3.75".into())
    );
    assert_eq!(
        compute_aggregate("AVG", Some(0), &rows).unwrap(),
        Value::Numeric("1.25".into())
    );
    // A float beside a NUMERIC is refused, not dropped.
    let mixed: Vec<Row> = vec![
        vec![Value::Numeric("1.5".into())],
        vec![Value::Float64(2.0)],
    ];
    assert!(compute_aggregate("SUM", Some(0), &mixed).is_err());
}

// ---------------------------------------------------------------------------
// Persistence and recovery
// ---------------------------------------------------------------------------

/// The typmod survives a restart: catalog.json stores it, and a restarted
/// executor still rounds and refuses.
#[tokio::test]
async fn typmod_survives_a_restart() {
    use crate::catalog::Catalog;
    use crate::storage::{DiskEngine, StorageEngine};

    async fn open(dir: &std::path::Path) -> Executor {
        let catalog = Arc::new(Catalog::new());
        let catalog_path = dir.join("catalog.json");
        crate::storage::persistence::CatalogPersistence::new(&catalog_path)
            .load_catalog(&catalog)
            .await
            .unwrap();
        let engine = DiskEngine::open(&dir.join("nucleus.db"), catalog.clone()).unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir))
    }

    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open(dir.path()).await;
        exec(
            &ex,
            "CREATE TABLE rst_typ (id INT PRIMARY KEY, v NUMERIC(6,2), w NUMERIC(8,3)[])",
        )
        .await;
        exec(&ex, "INSERT INTO rst_typ VALUES (1, '1.005', NULL)").await;
    }
    let json = std::fs::read_to_string(dir.path().join("catalog.json")).unwrap();
    let snapshot: serde_json::Value = serde_json::from_str(&json).unwrap();
    let column = |name: &str| {
        snapshot["tables"][0]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
            .unwrap_or_else(|| panic!("no column {name} in {json}"))
            .clone()
    };
    // atttypmod of NUMERIC(6,2) is ((6 << 16) | 2) + 4 = 393222.
    assert_eq!(column("v")["numeric_typmod"], 393_222, "{json}");
    // An unconstrained column carries no key at all.
    assert!(column("id").get("numeric_typmod").is_none(), "{json}");

    let ex = open(dir.path()).await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM rst_typ").await,
        vec![some("1.01")]
    );
    exec(&ex, "INSERT INTO rst_typ VALUES (2, 7, NULL)").await;
    assert_eq!(
        numerics(&ex, "SELECT v FROM rst_typ ORDER BY id").await,
        vec![some("1.01"), some("7.00")]
    );
    assert_eq!(
        refused(&ex, "INSERT INTO rst_typ VALUES (3, '10000', NULL)").await,
        "22003",
        "the recovered column must still refuse an overflow"
    );
    let result = exec(
        &ex,
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
         WHERE attrelid = 'rst_typ'::regclass AND attname = 'w'",
    )
    .await;
    assert_eq!(scalar(&result[0]), &Value::Text("numeric(8,3)[]".into()));
}

/// A catalog.json written before typmods existed has no `numeric_typmod` key:
/// it must load as unconstrained, and an unconstrained column is saved without
/// the key, so old files stay byte-compatible in both directions.
#[tokio::test]
async fn a_catalog_without_typmods_loads_as_unconstrained() {
    use crate::catalog::Catalog;
    use crate::storage::persistence::CatalogPersistence;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog.json");
    std::fs::write(
        &path,
        r#"{"tables":[{"name":"legacy","columns":[
            {"name":"id","data_type":"Int32","nullable":false},
            {"name":"v","data_type":"Numeric","nullable":true,"id":2}]}],"indexes":[]}"#,
    )
    .unwrap();
    let catalog = Catalog::new();
    CatalogPersistence::new(&path)
        .load_catalog(&catalog)
        .await
        .unwrap();
    let table = catalog.get_table("legacy").await.expect("legacy table");
    assert!(table.columns.iter().all(|c| c.numeric_typmod.is_none()));
    assert_eq!(table.columns[1].data_type, DataType::Numeric);

    CatalogPersistence::new(&path)
        .save_catalog(&catalog)
        .await
        .unwrap();
    let saved = std::fs::read_to_string(&path).unwrap();
    assert!(!saved.contains("numeric_typmod"), "{saved}");
}
