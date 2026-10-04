//! NUMERIC is exact-or-refused (NP03): text that cannot be held exactly is an
//! error with PostgreSQL's SQLSTATE, never a rounded value, and arithmetic or
//! aggregation that would round is refused instead of returning a different
//! number. Unit-level coverage of the parser and the wire encodings lives in
//! `types` and `wire`; this file pins the SQL surface and the SQLSTATEs.

use super::*;
use crate::types::Value;
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

fn sqlstate_of(err: &ExecError) -> String {
    let codec = PgWireErrorCodec;
    codec.code_to_string(codec.encode(err).code)
}

async fn numeric_of(ex: &Executor, sql: &str) -> String {
    let results = exec(ex, sql).await;
    match scalar(&results[0]) {
        Value::Numeric(text) => text.clone(),
        other => panic!("{sql}: expected NUMERIC, got {other:?}"),
    }
}

const MAX_NUMERIC: &str = "79228162514264337593543950335";

/// A cast keeps the written scale, and PostgreSQL has no negative zero.
#[tokio::test]
async fn cast_keeps_written_scale_and_has_no_negative_zero() {
    let ex = test_executor();
    for (written, expected) in [
        ("1.500", "1.500"),
        ("-12.3400", "-12.3400"),
        ("-0.000", "0.000"),
        ("-0", "0"),
        ("  7.5  ", "7.5"),
        ("+3", "3"),
    ] {
        assert_eq!(
            numeric_of(&ex, &format!("SELECT CAST('{written}' AS NUMERIC)")).await,
            expected,
            "{written:?}"
        );
    }
}

/// An exponent moves the decimal point; it never rounds a digit away.
#[tokio::test]
async fn scientific_notation_is_expanded_exactly() {
    let ex = test_executor();
    for (written, expected) in [
        ("1.50e1", "15.0"),
        ("1e3", "1000"),
        ("2.5E-3", "0.0025"),
        ("1e-28", "0.0000000000000000000000000001"),
    ] {
        assert_eq!(
            numeric_of(&ex, &format!("SELECT CAST('{written}' AS NUMERIC)")).await,
            expected,
            "{written:?}"
        );
    }
}

/// Every refusal carries the SQLSTATE PostgreSQL uses: 22003 for a valid number
/// that does not fit, 22P02 for text that is not a number, 0A000 for NaN and
/// the infinities. The first case used to be silently rounded to 28 places
/// (`from_scientific` parsed its mantissa with the rounding parser).
#[tokio::test]
async fn numeric_text_refusals_have_postgres_sqlstates() {
    let cases = [
        ("0.12345678901234567890123456789012345e2", "22003"),
        ("0.12345678901234567890123456789012345", "22003"),
        ("1e-29", "22003"),
        ("1e29", "22003"),
        ("1e5000", "22003"),
        ("79228162514264337593543950336", "22003"),
        ("1_000", "22P02"),
        ("0x10", "22P02"),
        ("abc", "22P02"),
        ("1.2.3", "22P02"),
        ("1e", "22P02"),
        ("", "22P02"),
        ("NaN", "0A000"),
        ("Infinity", "0A000"),
        ("-Infinity", "0A000"),
    ];
    let ex = test_executor();
    for (written, state) in cases {
        let err = ex
            .execute(&format!("SELECT CAST('{written}' AS NUMERIC)"))
            .await
            .expect_err("this text must be refused");
        assert_eq!(sqlstate_of(&err), state, "{written:?}: {err}");
    }
    // Control: the largest value, and 28 fractional digits, are accepted.
    assert_eq!(
        numeric_of(&ex, &format!("SELECT CAST('{MAX_NUMERIC}' AS NUMERIC)")).await,
        MAX_NUMERIC
    );
    assert_eq!(
        numeric_of(
            &ex,
            "SELECT CAST('0.1234567890123456789012345678' AS NUMERIC)"
        )
        .await,
        "0.1234567890123456789012345678"
    );
}

/// `+`, `-`, `*` and `%` return the exact result or fail with 22003. The
/// rounding implementation returned a different number for `MAX + 0.4` and for
/// `1e-28 * 0.5`.
#[tokio::test]
async fn arithmetic_that_would_round_is_refused() {
    let ex = test_executor();
    let refused = [
        format!("SELECT CAST('{MAX_NUMERIC}' AS NUMERIC) + CAST('0.4' AS NUMERIC)"),
        format!("SELECT CAST('{MAX_NUMERIC}' AS NUMERIC) + CAST('1' AS NUMERIC)"),
        format!("SELECT CAST('-{MAX_NUMERIC}' AS NUMERIC) - CAST('0.4' AS NUMERIC)"),
        "SELECT CAST('0.0000000000000000000000000001' AS NUMERIC) * CAST('0.5' AS NUMERIC)"
            .to_string(),
        format!("SELECT CAST('{MAX_NUMERIC}' AS NUMERIC) * CAST('2' AS NUMERIC)"),
    ];
    for sql in &refused {
        let err = ex
            .execute(sql)
            .await
            .expect_err("inexact result must be refused");
        assert_eq!(sqlstate_of(&err), "22003", "{sql}: {err}");
    }
    // Controls: results that are exact are returned unchanged.
    for (sql, expected) in [
        (
            "SELECT CAST('0.1' AS NUMERIC) + CAST('0.2' AS NUMERIC)",
            "0.3",
        ),
        (
            "SELECT CAST('1' AS NUMERIC) - CAST('1.50' AS NUMERIC)",
            "-0.5",
        ),
        (
            "SELECT CAST('1.5' AS NUMERIC) * CAST('1.5' AS NUMERIC)",
            "2.25",
        ),
        (
            "SELECT CAST('7.5' AS NUMERIC) % CAST('2' AS NUMERIC)",
            "1.5",
        ),
        (
            "SELECT CAST('-7.5' AS NUMERIC) % CAST('2' AS NUMERIC)",
            "-1.5",
        ),
        (
            "SELECT CAST('-0.5' AS NUMERIC) + CAST('0.5' AS NUMERIC)",
            "0",
        ),
    ] {
        assert_eq!(numeric_of(&ex, sql).await, expected, "{sql}");
    }
}

/// SUM refuses a total it cannot hold exactly instead of rounding it.
#[tokio::test]
async fn sum_that_would_round_is_refused() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE exact_sum (v NUMERIC)").await;
    exec(
        &ex,
        &format!("INSERT INTO exact_sum VALUES ('{MAX_NUMERIC}'), ('0.4')"),
    )
    .await;
    let err = ex
        .execute("SELECT SUM(v) FROM exact_sum")
        .await
        .expect_err("the exact total does not fit");
    assert_eq!(sqlstate_of(&err), "22003", "{err}");

    exec(&ex, "CREATE TABLE ok_sum (v NUMERIC)").await;
    exec(&ex, "INSERT INTO ok_sum VALUES ('1.5'), ('2.25')").await;
    assert_eq!(numeric_of(&ex, "SELECT SUM(v) FROM ok_sum").await, "3.75");
}
