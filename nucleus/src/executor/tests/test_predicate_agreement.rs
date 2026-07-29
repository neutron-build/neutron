//! A predicate must mean the same thing wherever it is written.
//!
//! `WHERE p` and `SELECT p` reach different code: the filter path goes through
//! `eval_where_plan` / `try_simd_filter` / the index eligibility list, while the
//! projection path goes through `eval_expr_plan`. Adding an index introduces a
//! third. Nothing forces those to agree, and twice they have not:
//!
//! - `tstz >= TIMESTAMP '…'` was **true in a projection and matched zero rows in
//!   a WHERE** — `Value::Ord` had no temporal cross-variant arms and compared
//!   type tags (Date=6/Timestamp=7/TimestampTz=8) instead of instants.
//! - `Expr::TypedString` and `Expr::Interval` were classified as unsupported
//!   *features*, so an indexed range silently became a full scan.
//!
//! Both were found by hand. The property below finds that whole class
//! mechanically: for every predicate P over every column type,
//!
//!   { id : SELECT (P) FROM t  is TRUE }  ==  { id : SELECT id FROM t WHERE P }
//!
//! and both are unchanged by the presence of an index. Three-valued logic is
//! respected on purpose — `WHERE` keeps only TRUE, so the projection side counts
//! only rows where the expression is exactly `true`, and NULL comparisons drop
//! out of both sides identically.

use super::*;
use std::collections::BTreeSet;

/// A column type, the rows to put in it, and the literals worth comparing
/// against — including *differently spelled* literals for the same instant,
/// which is where the temporal bug lived.
struct TypeCase {
    label: &'static str,
    sql_type: &'static str,
    /// SQL literals for the column, in id order. `NULL` is spelled literally.
    values: &'static [&'static str],
    /// Right-hand sides to compare the column against.
    literals: &'static [&'static str],
    /// Operators to try. Ordered types get the full set; others get equality.
    ordered: bool,
}

const CASES: &[TypeCase] = &[
    TypeCase {
        label: "INT",
        sql_type: "INT",
        values: &["1", "2", "3", "10", "NULL"],
        literals: &["2", "'2'", "CAST('2' AS INT)", "2::BIGINT"],
        ordered: true,
    },
    TypeCase {
        label: "BIGINT",
        sql_type: "BIGINT",
        values: &["1000000", "2000000", "3000000", "NULL"],
        literals: &["2000000", "'2000000'", "2000000::INT"],
        ordered: true,
    },
    TypeCase {
        label: "TEXT",
        sql_type: "TEXT",
        values: &["'alpha'", "'beta'", "'gamma'", "''", "NULL"],
        literals: &["'beta'", "''"],
        ordered: true,
    },
    TypeCase {
        label: "DATE",
        sql_type: "DATE",
        values: &[
            "DATE '2026-01-01'",
            "DATE '2026-06-15'",
            "DATE '2026-12-31'",
            "NULL",
        ],
        literals: &["DATE '2026-06-15'", "'2026-06-15'", "'2026-06-15'::DATE"],
        ordered: true,
    },
    TypeCase {
        label: "TIMESTAMP",
        sql_type: "TIMESTAMP",
        values: &[
            "TIMESTAMP '2026-01-01 00:00:00'",
            "TIMESTAMP '2026-06-15 12:30:00'",
            "TIMESTAMP '2026-12-31 23:59:59'",
            "NULL",
        ],
        literals: &[
            "TIMESTAMP '2026-06-15 12:30:00'",
            "'2026-06-15 12:30:00'",
            // A DATE bound against a TIMESTAMP column: cross-variant, the exact
            // shape whose comparator arm was missing.
            "DATE '2026-06-15'",
        ],
        ordered: true,
    },
    TypeCase {
        label: "TIMESTAMPTZ",
        sql_type: "TIMESTAMP WITH TIME ZONE",
        values: &[
            "TIMESTAMP '2026-01-01 00:00:00'",
            "TIMESTAMP '2026-06-15 12:30:00'",
            "TIMESTAMP '2026-12-31 23:59:59'",
            "NULL",
        ],
        // The regression: a TIMESTAMP-spelled bound on a TIMESTAMPTZ column.
        literals: &[
            "TIMESTAMP '2026-06-15 12:30:00'",
            "'2026-06-15 12:30:00'",
            "DATE '2026-06-15'",
        ],
        ordered: true,
    },
    TypeCase {
        label: "NUMERIC",
        sql_type: "NUMERIC",
        values: &["1.5", "2.25", "3.125", "NULL"],
        literals: &["2.25", "'2.25'", "2"],
        ordered: true,
    },
    TypeCase {
        label: "DOUBLE",
        sql_type: "DOUBLE PRECISION",
        values: &["1.5", "2.25", "3.125", "NULL"],
        literals: &["2.25", "2"],
        ordered: true,
    },
    TypeCase {
        label: "UUID",
        sql_type: "UUID",
        values: &[
            "'00000000-0000-0000-0000-000000000001'",
            "'00000000-0000-0000-0000-000000000002'",
            "'00000000-0000-0000-0000-000000000003'",
            "NULL",
        ],
        literals: &[
            "'00000000-0000-0000-0000-000000000002'",
            "UUID '00000000-0000-0000-0000-000000000002'",
        ],
        ordered: true,
    },
    TypeCase {
        label: "BOOL",
        sql_type: "BOOLEAN",
        values: &["true", "false", "NULL"],
        literals: &["true", "false"],
        ordered: false,
    },
];

const ORDERED_OPS: &[&str] = &["=", "<>", "<", "<=", ">", ">="];
const EQ_OPS: &[&str] = &["=", "<>"];

/// Ids returned by `sql`, or `None` if the statement did not execute — an
/// unsupported literal spelling is not a disagreement, and both sides get the
/// same chance to decline.
async fn ids(ex: &Executor, sql: &str) -> Option<BTreeSet<i64>> {
    let res = ex.execute(sql).await.ok()?;
    let ExecResult::Select { rows: r, .. } = res.first()? else {
        return None;
    };
    r.iter()
        .map(|row| match row.first()? {
            Value::Int32(n) => Some(i64::from(*n)),
            Value::Int64(n) => Some(*n),
            _ => None,
        })
        .collect()
}

/// Ids whose projected predicate evaluated to exactly TRUE. NULL and FALSE are
/// both excluded, matching what `WHERE` keeps.
async fn ids_where_projection_is_true(ex: &Executor, predicate: &str) -> Option<BTreeSet<i64>> {
    let res = ex
        .execute(&format!("SELECT id, ({predicate}) FROM p"))
        .await
        .ok()?;
    let ExecResult::Select { rows: r, .. } = res.first()? else {
        return None;
    };
    let mut out = BTreeSet::new();
    for row in r {
        let id = match row.first() {
            Some(Value::Int32(n)) => i64::from(*n),
            Some(Value::Int64(n)) => *n,
            _ => return None,
        };
        if matches!(row.get(1), Some(Value::Bool(true))) {
            out.insert(id);
        }
    }
    Some(out)
}

async fn seed(case: &TypeCase) -> Executor {
    let ex = test_executor();
    exec(&ex, &format!("CREATE TABLE p (id INT PRIMARY KEY, v {})", case.sql_type)).await;
    for (i, v) in case.values.iter().enumerate() {
        exec(&ex, &format!("INSERT INTO p VALUES ({}, {v})", i + 1)).await;
    }
    ex
}

/// `SELECT p` and `WHERE p` must agree, before and after an index exists, and
/// the index must not change which rows match.
#[tokio::test]
async fn test_predicate_means_the_same_in_projection_and_filter() {
    let mut checked = 0usize;
    for case in CASES {
        let ex = seed(case).await;
        let ops = if case.ordered { ORDERED_OPS } else { EQ_OPS };

        for lit in case.literals {
            for op in ops {
                let predicate = format!("v {op} {lit}");

                // Unindexed: projection vs filter.
                ex.clear_all_query_caches();
                let Some(projected) = ids_where_projection_is_true(&ex, &predicate).await else {
                    continue; // this spelling does not execute at all; not a disagreement
                };
                ex.clear_all_query_caches();
                let Some(filtered) =
                    ids(&ex, &format!("SELECT id FROM p WHERE {predicate}")).await
                else {
                    panic!(
                        "{}: `WHERE {predicate}` failed while `SELECT ({predicate})` succeeded",
                        case.label
                    );
                };
                assert_eq!(
                    projected, filtered,
                    "{}: `SELECT ({predicate})` is TRUE for {projected:?} but \
                     `WHERE {predicate}` matched {filtered:?}",
                    case.label
                );

                // Indexed: the same predicate must match the same rows.
                exec(&ex, "CREATE INDEX IF NOT EXISTS p_v ON p (v)").await;
                ex.clear_all_query_caches();
                let indexed = ids(&ex, &format!("SELECT id FROM p WHERE {predicate}")).await;
                assert_eq!(
                    indexed.as_ref(),
                    Some(&filtered),
                    "{}: `WHERE {predicate}` matched {filtered:?} without an index \
                     and {indexed:?} with one",
                    case.label
                );
                ex.clear_all_query_caches();
                let projected_indexed = ids_where_projection_is_true(&ex, &predicate).await;
                assert_eq!(
                    projected_indexed.as_ref(),
                    Some(&projected),
                    "{}: `SELECT ({predicate})` changed once an index existed",
                    case.label
                );
                exec(&ex, "DROP INDEX p_v").await;
                checked += 1;
            }
        }
    }
    assert!(
        checked > 100,
        "only {checked} predicate forms ran — the matrix collapsed"
    );
}

/// The same property for two-sided windows, the shape a dashboard actually
/// sends and the one whose index path was lost to an eligibility list.
#[tokio::test]
async fn test_range_windows_mean_the_same_in_projection_and_filter() {
    for case in CASES.iter().filter(|c| c.ordered && c.values.len() >= 4) {
        let ex = seed(case).await;
        let (lo, hi) = (case.values[0], case.values[2]);
        for predicate in [
            format!("v >= {lo} AND v < {hi}"),
            format!("v BETWEEN {lo} AND {hi}"),
            format!("(v >= {lo} AND v <= {hi})"),
            format!("NOT (v < {lo})"),
        ] {
            ex.clear_all_query_caches();
            let Some(projected) = ids_where_projection_is_true(&ex, &predicate).await else {
                continue;
            };
            ex.clear_all_query_caches();
            let filtered = ids(&ex, &format!("SELECT id FROM p WHERE {predicate}")).await;
            assert_eq!(
                filtered.as_ref(),
                Some(&projected),
                "{}: `SELECT ({predicate})` is TRUE for {projected:?} but \
                 `WHERE {predicate}` matched {filtered:?}",
                case.label
            );

            exec(&ex, "CREATE INDEX IF NOT EXISTS p_v ON p (v)").await;
            ex.clear_all_query_caches();
            let indexed = ids(&ex, &format!("SELECT id FROM p WHERE {predicate}")).await;
            assert_eq!(
                indexed.as_ref(),
                Some(&projected),
                "{}: window `{predicate}` matched different rows once indexed",
                case.label
            );
            exec(&ex, "DROP INDEX p_v").await;
        }
    }
}
