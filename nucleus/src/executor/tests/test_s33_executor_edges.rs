//! S33-11 / S33-14 executor edges.
//!
//! S33-11: when the plan executor's HashJoin key resolved on NEITHER side, it
//! silently fell open to a full cross product — the join condition was
//! dropped and the caller got a Cartesian answer capped only by
//! MAX_CROSS_JOIN_ROWS. It must decline loudly so the query falls back to
//! the AST path (which resolves joins by a different, more complete route).
//!
//! S33-14: `try_simd_filter` bound qualified columns by bare name with a
//! byte-exact column lookup — the only case-SENSITIVE resolution in the
//! executor. It failed closed (perf miss, never a wrong answer), but made
//! SIMD eligibility depend on the case the query happened to spell the
//! qualifier in.

use super::*;
use crate::planner::{Cost, JoinPlanType, PlanNode};

#[tokio::test]
async fn hash_join_with_unresolvable_key_declines_instead_of_cross_product() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE s33a (id INT, x INT)").await;
    exec(&ex, "CREATE TABLE s33b (id INT, y INT)").await;
    exec(&ex, "INSERT INTO s33a VALUES (1, 10), (2, 20)").await;
    exec(&ex, "INSERT INTO s33b VALUES (1, 100), (2, 200)").await;

    // A key that resolves on neither side (columns named x/y, key says
    // a.k = b.k). Pre-fix: 2x2 = 4 rows of cross product with the condition
    // silently dropped.
    let plan = PlanNode::HashJoin {
        left: Box::new(PlanNode::SeqScan {
            table: "s33a".into(),
            estimated_rows: 2,
            estimated_cost: Cost(1.0),
            filter: None,
            filter_expr: None,
            scan_limit: None,
            projection: None,
        }),
        right: Box::new(PlanNode::SeqScan {
            table: "s33b".into(),
            estimated_rows: 2,
            estimated_cost: Cost(1.0),
            filter: None,
            filter_expr: None,
            scan_limit: None,
            projection: None,
        }),
        join_type: JoinPlanType::Inner,
        hash_keys: vec!["s33a.k = s33b.k".into()],
        estimated_rows: 2,
        estimated_cost: Cost(2.0),
    };
    let result = ex
        .execute_plan_node(&plan, &std::collections::HashMap::new())
        .await;
    match result {
        Err(e) => assert!(
            e.to_string().to_lowercase().contains("hash join"),
            "the decline must name the hash join, got: {e}"
        ),
        Ok((_, rows)) => panic!(
            "an unresolvable key must decline, not return a cross product \
             ({} rows)",
            rows.len()
        ),
    }
}

/// S33-14: the case-differing qualified form of a predicate must still bind
/// the SIMD fast path — byte-exact matching made eligibility depend on the
/// query's spelling of the column name.
#[test]
fn simd_filter_binds_case_differing_qualified_columns() {
    let ex = test_executor();
    let rows = vec![
        vec![Value::Int32(1), Value::Int32(10)],
        vec![Value::Int32(2), Value::Int32(20)],
    ];
    let col_meta = vec![
        crate::executor::types::ColMeta {
            table: Some("t".into()),
            name: "id".into(),
            dtype: crate::types::DataType::Int32,
        },
        crate::executor::types::ColMeta {
            table: Some("t".into()),
            name: "v".into(),
            dtype: crate::types::DataType::Int32,
        },
    ];
    // T.V = 20 — qualifier uppercase, column uppercase; schema is lowercase.
    let expr = Executor::parse_expr_string("T.V = 20").expect("parse predicate");
    let indices = ex.try_simd_filter(&expr, &rows, &col_meta);
    assert_eq!(
        indices,
        Some(vec![1usize]),
        "a case-differing qualified column must still bind the SIMD filter"
    );
}
