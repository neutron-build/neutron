//! AST-cache literal extraction must be UTF-8-safe.
//!
//! `normalize_sql_with_literals` scanned SQL byte-by-byte and re-emitted every
//! non-quote byte as `bytes[i] as char` — Latin-1 widening. String literals
//! extracted that way were mojibake, and a cache hit substitutes them into the
//! cloned AST, so `SELECT POSITION('界' IN '你好世界')` returned 4 on a fresh
//! executor and 10 (the char offset inside the mangled text) on every
//! same-shape execution afterwards. The wave-1 fix family (WIR-4,
//! substitute_positional_placeholders) is: copy raw bytes, decode once.

use super::*;
use crate::executor::types::CacheLiteral;

/// Same-shape executions after the first must return the same answer as the
/// first — the cache-hit path substitutes extracted literals into a cloned
/// AST, and those literals used to be Latin-1 mojibake.
#[tokio::test]
async fn ast_cache_hit_preserves_non_ascii_literals() {
    let ex = test_executor();
    let sql = "SELECT POSITION('界' IN '你好世界')";
    let first = scalar(&ex.execute(sql).await.unwrap()[0]).clone();
    assert_eq!(
        first,
        Value::Int32(4),
        "fresh parse: 界 is the 4th character"
    );
    // Second execution takes the AST-cache path: clone + literal substitution.
    let second = scalar(&ex.execute(sql).await.unwrap()[0]).clone();
    assert_eq!(
        second, first,
        "cache-hit execution must not mangle non-ASCII literals"
    );
    // Same shape, different literals — exercises substitution on a hit keyed
    // by the first statement.
    let other = scalar(
        &ex.execute("SELECT POSITION('好' IN '你好世界')")
            .await
            .unwrap()[0],
    )
    .clone();
    assert_eq!(other, Value::Int32(2), "好 is the 2nd character");
}

/// Unit level: extraction itself must be byte-exact for multi-byte literals,
/// quoted identifiers, and text outside literals.
#[test]
fn normalize_extracts_non_ascii_literals_byte_exact() {
    let (norm, lits) = Executor::normalize_sql_with_literals("SELECT POSITION('界' IN '你好世界')");
    assert_eq!(norm, "SELECT POSITION($S IN $S)");
    assert_eq!(lits.len(), 2);
    assert!(matches!(&lits[0], CacheLiteral::String(s) if s == "界"));
    assert!(matches!(&lits[1], CacheLiteral::String(s) if s == "你好世界"));

    let (norm, lits) =
        Executor::normalize_sql_with_literals("SELECT \"Täble\".c FROM \"Täble\" WHERE x = 'Zoë'");
    assert_eq!(norm, "SELECT \"Täble\".c FROM \"Täble\" WHERE x = $S");
    assert_eq!(lits.len(), 1);
    assert!(matches!(&lits[0], CacheLiteral::String(s) if s == "Zoë"));
}
