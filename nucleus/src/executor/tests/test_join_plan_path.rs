//! Which JOIN forms actually reach the plan executor.
//!
//! A benchmark found the 2-table join running at 0.1x SQLite, and the cause was
//! not the join algorithm: joins never reached the plan executor at all. Three
//! separate gates sent every real-world join shape to `build_from_rows_with_ctes`
//! instead. Answers stayed correct, so no row assertion could see it.
//!
//! Most of these assert on the SERVED counter rather than on rows, because the
//! failure mode is a join that got slower and never wrong. Note which counter:
//! a query rejected by `query_eligible_for_plan` never enters the plan block
//! and so records no *fallback* either, which is how three of the four join
//! spellings passed a first version of this file that watched fallbacks.
//!
//! `computed_projections_over_joins_are_not_one_operand` is the exception — it
//! guards a silent wrong ANSWER that routing joins here exposed.

use super::*;

const USERS: i64 = 50;
const ORDERS: i64 = 500;

async fn joined_tables() -> Executor {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").await;
    exec(
        &ex,
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount BIGINT)",
    )
    .await;
    for i in 0..USERS {
        exec(&ex, &format!("INSERT INTO users VALUES ({i}, 'user{i}')")).await;
    }
    for i in 0..ORDERS {
        exec(
            &ex,
            &format!("INSERT INTO orders VALUES ({i}, {}, {})", i % USERS, i * 10),
        )
        .await;
    }
    ex
}

/// Rows returned, and whether the plan executor answered this statement.
///
/// It has to be the SERVED counter, not the fallback one. A query rejected by
/// `query_eligible_for_plan` never enters the plan block, so it records no
/// fallback — "never planned" and "planned and ran" both read as zero
/// fallbacks, which is how three of the four join spellings stayed hidden.
async fn plan_outcome(ex: &Executor, sql: &str) -> (usize, bool) {
    let before = ex.metrics().plan_path_served.get();
    let result = exec(ex, sql).await;
    let served = ex.metrics().plan_path_served.get() > before;
    (rows(&result[0]).len(), served)
}

/// Four spellings of one join. They are semantically identical and must all
/// produce the same rows on the same path.
const UNALIASED: &str = "SELECT users.name, orders.amount FROM orders \
     JOIN users ON users.id = orders.user_id WHERE orders.id < 100";
const ALIASED: &str = "SELECT u.name, o.amount FROM orders o \
     JOIN users u ON u.id = o.user_id WHERE o.id < 100";
const COMMA: &str = "SELECT users.name, orders.amount FROM users, orders \
     WHERE users.id = orders.user_id AND orders.id < 100";
const COMMA_ALIASED: &str = "SELECT u.name, o.amount FROM users u, orders o \
     WHERE u.id = o.user_id AND o.id < 100";

const SPELLINGS: [(&str, &str); 4] = [
    ("unaliased ON", UNALIASED),
    ("aliased ON", ALIASED),
    ("comma join", COMMA),
    ("aliased comma join", COMMA_ALIASED),
];

#[tokio::test]
async fn every_join_spelling_reaches_the_plan_executor() {
    let ex = joined_tables().await;
    for (label, sql) in SPELLINGS {
        let (matched, served) = plan_outcome(&ex, sql).await;
        assert_eq!(matched, 100, "{label}: wrong row count — {sql}");
        assert!(
            served,
            "{label}: the plan executor never answered it — {sql}"
        );
    }
}

#[tokio::test]
async fn all_join_spellings_agree() {
    let ex = joined_tables().await;
    let sorted = |r: &ExecResult| {
        let mut v = rows(r).clone();
        v.sort();
        v
    };
    let canonical = sorted(&exec(&ex, UNALIASED).await[0]);
    assert_eq!(canonical.len(), 100, "fixture produced the wrong row count");
    for (label, sql) in SPELLINGS {
        let got = sorted(&exec(&ex, sql).await[0]);
        assert_eq!(got, canonical, "{label} disagrees — {sql}");
    }
}

/// The defect the join benchmark actually exposed, isolated from any join.
///
/// A one-sided range on an indexed column had no way to be expressed: the
/// storage API took an inclusive `(low, high)` pair, so the planner emitted the
/// whole `<` predicate as a `lookup_key`, the executor read that slot as an
/// equality, and the index scan declined. The query then re-ran end to end on
/// the AST path with no index. `BETWEEN` and `=` worked, which is why every
/// existing benchmark and test missed it.
#[tokio::test]
async fn one_sided_ranges_use_the_index() {
    let ex = joined_tables().await;
    for (sql, expected) in [
        ("SELECT * FROM orders WHERE id < 100", 100),
        ("SELECT * FROM orders WHERE orders.id < 100", 100),
        ("SELECT * FROM orders WHERE id <= 100", 101),
        ("SELECT * FROM orders WHERE id > 400", 99),
        ("SELECT * FROM orders WHERE id >= 400", 100),
        // The two-sided forms that always worked, as a control.
        ("SELECT * FROM orders WHERE id BETWEEN 1 AND 99", 99),
        ("SELECT * FROM orders WHERE id > 10 AND id < 20", 9),
    ] {
        let served_before = ex.metrics().index_scan_served.get();
        let (matched, plan_served) = plan_outcome(&ex, sql).await;
        assert_eq!(matched, expected, "wrong row count — {sql}");
        assert!(plan_served, "the plan executor never answered it — {sql}");
        assert!(
            ex.metrics().index_scan_served.get() > served_before,
            "answered without the index — {sql}"
        );
    }
}

/// A contradictory or empty range must return no rows rather than panic in
/// `BTreeMap::range`, and an open side must not be mistaken for a bound.
#[tokio::test]
async fn degenerate_ranges_are_empty_not_panics() {
    let ex = joined_tables().await;
    for (sql, expected) in [
        ("SELECT * FROM orders WHERE id >= 20 AND id <= 5", 0),
        ("SELECT * FROM orders WHERE id > 5 AND id < 6", 0),
        ("SELECT * FROM orders WHERE id < 0", 0),
        ("SELECT * FROM orders WHERE id >= 500", 0),
    ] {
        let r = exec(&ex, sql).await;
        assert_eq!(rows(&r[0]).len(), expected, "wrong row count — {sql}");
    }
}

/// A computed projection over two joined tables must be the computation, not
/// one of its operands.
///
/// `resolve_plan_col_idx` split ANY spec on its last `.` and matched the
/// trailing segment against the column list, so `l.qty * p.price` resolved to
/// the column `price` and the projection emitted 1000 where 2000 was the
/// answer. Reversing the operands returned the other one. It is a silent wrong
/// answer with no error and a plausible-looking value, and it only became
/// reachable once joins started reaching the plan executor — the operand check
/// is what distinguishes it from an off-by-one.
#[tokio::test]
async fn computed_projections_over_joins_are_not_one_operand() {
    let ex = joined_tables().await;
    for (sql, expected) in [
        // Order 53 has amount 530 and user_id 3. The product, 1590, differs
        // from BOTH operands — which is the whole point. Order 51 (amount 510,
        // user 1) would have let the reversed case pass with the bug present,
        // because 510 * 1 equals the very operand the bug returned.
        (
            "SELECT orders.amount * users.id AS v FROM orders \
             JOIN users ON users.id = orders.user_id WHERE orders.id = 53",
            Value::Int64(1590),
        ),
        // Operands reversed: the old bug returned whichever came last.
        (
            "SELECT users.id * orders.amount AS v FROM orders \
             JOIN users ON users.id = orders.user_id WHERE orders.id = 53",
            Value::Int64(1590),
        ),
        (
            "SELECT o.amount + u.id AS v FROM orders o \
             JOIN users u ON u.id = o.user_id WHERE o.id = 53",
            Value::Int64(533),
        ),
    ] {
        let r = exec(&ex, sql).await;
        let got = rows(&r[0]);
        assert_eq!(got.len(), 1, "expected one row — {sql}");
        assert_eq!(got[0][0], expected, "wrong value — {sql}");
    }
}

/// A self-join cannot be handled by rewriting aliases to base table names —
/// both sides would collapse onto one relation. It must stay on the AST path
/// and, above all, keep returning the right rows.
#[tokio::test]
async fn self_join_stays_correct() {
    let ex = joined_tables().await;
    let r = exec(
        &ex,
        "SELECT a.id, b.id FROM orders a JOIN orders b ON a.user_id = b.user_id \
         WHERE a.id = 0 AND b.id < 100",
    )
    .await;
    // user 0 owns orders 0, 50, ... — two of them are below id 100.
    assert_eq!(rows(&r[0]).len(), 2);
}
