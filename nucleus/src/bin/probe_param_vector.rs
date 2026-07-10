//! Repro probe for the extended-protocol vector corruption: the same
//! statement executed (a) as an inline SQL string and (b) as a cached AST
//! with `$N` placeholders substituted — the wire layer's prepared path —
//! must produce identical results. Build:
//! `cargo run --release --features server --bin probe_param_vector`.
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{param_subst, ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn first_float(results: &[ExecResult]) -> Option<f64> {
    for r in results {
        if let ExecResult::Select { rows, .. } = r {
            match rows.first().and_then(|row| row.first()) {
                Some(Value::Float64(f)) => return Some(*f),
                Some(Value::Int32(n)) => return Some(*n as f64),
                Some(Value::Int64(n)) => return Some(*n as f64),
                other => {
                    println!("  (first cell was {other:?})");
                    return None;
                }
            }
        }
    }
    None
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    let inline = "SELECT VECTOR_DISTANCE(VECTOR('[1,0,0]'), VECTOR('[0,1,0]'), 'cosine') AS d";
    let inline_result = ex.execute(inline).await.expect("inline exec");
    let inline_d = first_float(&inline_result).expect("inline float");
    println!("inline:            d = {inline_d}");

    // The wire layer's prepared path: parse WITH the placeholder, substitute,
    // execute the statement list with a session.
    let with_param = "SELECT VECTOR_DISTANCE(VECTOR('[1,0,0]'), VECTOR('[0,1,0]'), 'cosine') AS d, $1 AS x";
    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, with_param).expect("parse");
    for stmt in &mut stmts {
        param_subst::substitute_params_in_stmt(stmt, &[Value::Text("hi".into())]);
    }
    println!("substituted SQL:   {}", stmts[0]);
    let ast_result = ex
        .execute_statements_with_session(1, stmts)
        .await
        .expect("ast exec");
    let ast_d = first_float(&ast_result).expect("ast float");
    println!("prepared/AST path: d = {ast_d}");

    // The wire layer also primes the plan-cache key hint (normalized at
    // Parse time) before executing — execute_prepared() is the public API
    // with identical behavior (substitute into cached AST + seed the hint).
    let handle = ex.prepare(with_param).expect("prepare");
    let prep_result = ex
        .execute_prepared(&handle, &[Value::Text("hi".into())])
        .await
        .expect("prepared exec");
    let prep_d = first_float(std::slice::from_ref(&prep_result)).expect("prepared float");
    println!("execute_prepared:  d = {prep_d}");

    // Also the substituted TEXT re-executed as a plain string (the wire
    // layer's fallback path) — isolates AST-exec vs string-exec.
    let substituted_text =
        "SELECT VECTOR_DISTANCE(VECTOR('[1,0,0]'), VECTOR('[0,1,0]'), 'cosine') AS d, 'hi' AS x";
    let text_result = ex.execute(substituted_text).await.expect("text exec");
    let text_d = first_float(&text_result).expect("text float");
    println!("substituted text:  d = {text_d}");

    if (inline_d - ast_d).abs() > 1e-6
        || (inline_d - text_d).abs() > 1e-6
        || (inline_d - prep_d).abs() > 1e-6
    {
        println!("MISMATCH — bug reproduced in-process");
        std::process::exit(1);
    }
    println!("all paths agree");
}
