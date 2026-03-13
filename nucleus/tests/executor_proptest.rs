//! Property-based tests verifying the executor does not panic on valid SQL sequences.

use nucleus::embedded::Database;
use proptest::prelude::*;

/// Fixed set of table names used across all generated SQL.
const TABLES: &[&str] = &["t1", "t2", "t3"];

/// Fixed set of column types.
const COL_TYPES: &[&str] = &["INT", "TEXT", "FLOAT"];

// ---------------------------------------------------------------------------
// Strategy helpers
// ---------------------------------------------------------------------------

/// Generate a `CREATE TABLE IF NOT EXISTS` statement with 1-3 columns.
fn arb_create_table() -> impl Strategy<Value = String> {
    (
        prop::sample::select(TABLES),
        prop::collection::vec(
            prop::sample::select(COL_TYPES),
            1..4,
        ),
    )
        .prop_map(|(table, col_types)| {
            let col_defs: Vec<String> = col_types
                .iter()
                .enumerate()
                .map(|(i, typ)| format!("c{i} {typ}"))
                .collect();
            format!(
                "CREATE TABLE IF NOT EXISTS {table} ({})",
                col_defs.join(", ")
            )
        })
}

/// Generate an `INSERT INTO` statement that matches a plausible schema (up to 3 columns).
fn arb_insert() -> impl Strategy<Value = String> {
    (
        prop::sample::select(TABLES),
        prop::collection::vec(arb_literal(), 1..4),
    )
        .prop_map(|(table, vals)| {
            format!("INSERT INTO {table} VALUES ({})", vals.join(", "))
        })
}

/// Generate a random SQL literal value (integer, float, or string).
fn arb_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        // integer literal
        (-1000i64..1000i64).prop_map(|n| n.to_string()),
        // float literal
        (-1000.0f64..1000.0f64).prop_map(|f| format!("{f:.2}")),
        // string literal (simple alpha to avoid SQL injection edge-cases)
        "[a-zA-Z]{0,10}".prop_map(|s| format!("'{s}'")),
    ]
}

/// Generate a `SELECT` statement — either `SELECT *` or `SELECT` specific columns.
fn arb_select() -> impl Strategy<Value = String> {
    (
        prop::sample::select(TABLES),
        prop::bool::ANY,
    )
        .prop_map(|(table, select_star)| {
            if select_star {
                format!("SELECT * FROM {table}")
            } else {
                // Pick a deterministic column name that may or may not exist.
                format!("SELECT c0 FROM {table}")
            }
        })
}

/// Generate a `DROP TABLE IF EXISTS` statement.
fn arb_drop_table() -> impl Strategy<Value = String> {
    prop::sample::select(TABLES)
        .prop_map(|table| format!("DROP TABLE IF EXISTS {table}"))
}

/// Generate a single arbitrary SQL statement (one of the four kinds).
fn arb_sql_statement() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => arb_create_table(),
        3 => arb_insert(),
        2 => arb_select(),
        1 => arb_drop_table(),
    ]
}

/// Generate a sequence of 5-20 SQL statements.
fn arb_sql_sequence() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(arb_sql_statement(), 5..=20)
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The executor must never panic when fed a sequence of valid SQL
    /// statements, even if the statements are logically inconsistent
    /// (e.g. inserting into a table that was already dropped). Errors
    /// are fine; panics are not.
    #[test]
    fn prop_executor_no_panic_on_valid_sql(statements in arb_sql_sequence()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let db = Database::memory();

        rt.block_on(async {
            for sql in &statements {
                // We intentionally ignore errors — the property under test
                // is that the executor does not *panic*, not that every
                // random statement succeeds.
                let _ = db.execute(sql).await;
            }
        });
    }
}
