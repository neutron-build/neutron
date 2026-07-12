//! Regression test for `parser_dos`: deeply-nested CAST chains used to trigger
//! exponential backtracking in sqlparser, pinning a CPU core for minutes (DoS).
//!
//! The fix adds a cheap O(n) pre-parse paren-nesting guard in `nucleus::sql::parse`
//! that rejects pathologically deep nesting in bounded time, the way Postgres
//! returns `54001 statement too complex` instead of spinning.

use std::time::{Duration, Instant};

use nucleus::sql::parse;

/// A depth-60 nested CAST chain (well past sqlparser's ~48 blow-up cliff) must be
/// REJECTED with a parse error in sub-millisecond / low-millisecond time, never
/// hang. Before the fix this never returned.
#[test]
fn deep_cast_chain_rejected_fast() {
    let mut expr = "1".to_string();
    for _ in 0..60 {
        expr = format!("CAST({expr} AS INTEGER)");
    }
    let sql = format!("SELECT {expr}");

    let t0 = Instant::now();
    let res = parse(&sql);
    let elapsed = t0.elapsed();

    assert!(
        res.is_err(),
        "deep CAST chain must be rejected, not accepted"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "deep CAST chain must be rejected quickly (took {elapsed:?}); the DoS guard \
         must run before sqlparser to avoid exponential backtracking"
    );
}

/// Pure paren nesting beyond the cap is also rejected quickly.
#[test]
fn deep_paren_nesting_rejected_fast() {
    let depth = 200;
    let sql = format!("SELECT {}1{}", "(".repeat(depth), ")".repeat(depth));

    let t0 = Instant::now();
    let res = parse(&sql);
    assert!(res.is_err(), "deep paren nesting must be rejected");
    assert!(
        t0.elapsed() < Duration::from_millis(500),
        "deep paren nesting must be rejected quickly"
    );
}

/// CRITICAL false-positive guard: parentheses inside STRING LITERALS must NOT be
/// counted toward nesting depth, or legitimate queries with paren-heavy text data
/// would be wrongly rejected.
#[test]
fn parens_inside_string_literal_not_counted() {
    // 300 open-parens, but all inside a single-quoted string literal.
    let payload = "(".repeat(300);
    let sql = format!("SELECT id FROM t WHERE note = '{payload}'");
    let res = parse(&sql);
    assert!(
        res.is_ok(),
        "parens inside a string literal must not trip the nesting guard: {res:?}"
    );
}

/// Parens inside comments must also be ignored.
#[test]
fn parens_inside_comments_not_counted() {
    let line = format!("SELECT 1 -- {}\n", "(".repeat(300));
    assert!(
        parse(&line).is_ok(),
        "parens in line comment must be ignored"
    );

    let block = format!("SELECT 1 /* {} */", "(".repeat(300));
    assert!(
        parse(&block).is_ok(),
        "parens in block comment must be ignored"
    );
}

/// Legitimate queries that nest only a handful of CASTs / parens / subqueries
/// deep must still parse fine — the cap is generous (100).
#[test]
fn legitimate_nested_queries_still_parse() {
    // A handful of nested CASTs (realistic).
    let mut expr = "1".to_string();
    for _ in 0..8 {
        expr = format!("CAST({expr} AS INTEGER)");
    }
    assert!(parse(&format!("SELECT {expr}")).is_ok());

    // CTE + window function + join + nested parens.
    let sql = "WITH ranked AS (\
                 SELECT a.id, ROW_NUMBER() OVER (PARTITION BY a.grp ORDER BY a.v DESC) AS rn \
                 FROM items a JOIN cats b ON (a.cat_id = b.id) \
                 WHERE a.v > ((1 + 2) * (3 - 1)) \
               ) SELECT * FROM ranked WHERE rn = 1";
    assert!(parse(sql).is_ok(), "legitimate complex query must parse");

    // Moderately nested subqueries (well under the cap).
    let mut sq = "SELECT 1".to_string();
    for d in 0..20 {
        sq = format!("SELECT * FROM ({sq}) AS s{d}");
    }
    assert!(parse(&sq).is_ok(), "20-deep subquery must parse");
}

/// Positional parameters ($1) and other lone '$' must not be mistaken for a
/// dollar-quoted string opener that swallows the rest of the statement.
#[test]
fn positional_params_not_treated_as_dollar_quote() {
    let res = parse("SELECT (id) FROM t WHERE a = $1 AND b = $2");
    assert!(res.is_ok(), "positional params must parse: {res:?}");
}
