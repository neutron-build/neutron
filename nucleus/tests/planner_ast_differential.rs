//! Deterministic differential gate for the optimized PlanNode executor.
//!
//! Every query runs against the same committed state twice: once with plan
//! execution enabled and once through the AST executor. A supported fast path
//! may optimize work, but it may not change columns, types, values, ordering,
//! NULL behavior, or errors.
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::{DataType, Row};

async fn select(ex: &Executor, session: u64, sql: &str) -> (Vec<(String, DataType)>, Vec<Row>) {
    let result = ex
        .execute_with_session(session, sql)
        .await
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"));
    match result.into_iter().next_back().expect("one result") {
        ExecResult::Select { columns, rows } => (columns, rows),
        other => panic!("expected SELECT for {sql}, got {other:?}"),
    }
}

#[tokio::test]
async fn planner_and_ast_match_supported_select_corpus() {
    let ex = Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    );
    let setup = ex.create_session();
    let planned = ex.create_session();
    let ast = ex.create_session();

    for sql in [
        "CREATE TABLE accounts (id INT PRIMARY KEY, region TEXT, active BOOLEAN)",
        "CREATE TABLE orders (id INT PRIMARY KEY, account_id INT, amount BIGINT, note TEXT)",
        "INSERT INTO accounts VALUES (1, 'west', true), (2, 'east', true), (3, NULL, false), (4, 'west', true)",
        "INSERT INTO orders VALUES (10, 1, 100, 'alpha'), (11, 1, 50, NULL), (12, 2, 75, 'beta'), (13, 4, 25, 'alpine'), (14, 4, NULL, 'zero')",
        "CREATE INDEX orders_amount_idx ON orders (amount)",
        "CREATE TABLE docs (id INT PRIMARY KEY, body JSONB)",
        r#"INSERT INTO docs VALUES (1, '{"kind":"a","tags":["db","rust"]}'), (2, '{"kind":"b"}')"#,
        "CREATE INDEX docs_body_gin ON docs USING GIN (body)",
    ] {
        ex.execute_with_session(setup, sql).await.unwrap();
    }
    ex.execute_with_session(planned, "SET plan_execution = on")
        .await
        .unwrap();
    ex.execute_with_session(ast, "SET plan_execution = off")
        .await
        .unwrap();

    let corpus = [
        // scan, projection, predicates, NULL/three-valued logic
        "SELECT id, amount FROM orders ORDER BY id",
        "SELECT id FROM orders WHERE amount >= 50 AND amount < 100 ORDER BY id",
        "SELECT id FROM orders WHERE note IS NULL OR note LIKE 'alp%' ORDER BY id",
        "SELECT id FROM orders WHERE amount BETWEEN 25 AND 75 ORDER BY amount, id",
        "SELECT id FROM orders WHERE account_id IN (1, 4) AND NOT (amount = 50) ORDER BY id",
        // expressions, aliases, DISTINCT, ordering, limits/offsets
        "SELECT id, amount + 5 AS raised, COALESCE(note, 'missing') AS label FROM orders ORDER BY id",
        "SELECT DISTINCT account_id FROM orders ORDER BY account_id DESC",
        "SELECT id, amount FROM orders ORDER BY amount DESC NULLS LAST, id LIMIT 3 OFFSET 1",
        "SELECT id, CASE WHEN amount >= 75 THEN 'large' ELSE 'small' END AS bucket FROM orders ORDER BY id",
        // aggregates and grouping
        "SELECT COUNT(*), COUNT(amount), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
        "SELECT account_id, COUNT(*), SUM(amount) FROM orders GROUP BY account_id HAVING COUNT(*) >= 1 ORDER BY account_id",
        // joins and qualified filters/projections
        "SELECT o.id, a.region, o.amount FROM orders o JOIN accounts a ON a.id = o.account_id WHERE a.active = true ORDER BY o.id",
        // index access paths (B-tree and GIN both retain exact semantics)
        "SELECT id FROM orders WHERE amount = 75 ORDER BY id",
        r#"SELECT id FROM docs WHERE body @> '{"tags":["rust"]}' ORDER BY id"#,
        // supported fallback shapes still form part of the public SELECT surface
        "WITH west AS (SELECT id FROM accounts WHERE region = 'west') SELECT id FROM west ORDER BY id",
        "SELECT id FROM accounts WHERE id IN (SELECT account_id FROM orders WHERE amount >= 75) ORDER BY id",
        "SELECT id FROM accounts WHERE active = true UNION SELECT account_id FROM orders ORDER BY id",
        "SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM orders ORDER BY id",
    ];

    for sql in corpus {
        ex.query_cache_invalidate_all();
        let plan_result = select(&ex, planned, sql).await;
        ex.query_cache_invalidate_all();
        let ast_result = select(&ex, ast, sql).await;
        assert_eq!(
            plan_result, ast_result,
            "plan/AST divergence for:\n{sql}\nplanned={plan_result:?}\nast={ast_result:?}"
        );
    }
}
