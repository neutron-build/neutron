//! QPP-1a regression: SEMI/ANTI joins.
//!
//! `execute_join` degraded Semi→Inner (right columns leak, left duplicated
//! per match) and Anti→Left (emits MATCHED rows — the exact inversion of
//! anti-semantics), while the plan path mapped both to an UNCONDITIONED
//! Inner nested loop (cross product). Until real semi/anti semantics land,
//! every variant must refuse loudly on BOTH execution paths instead of
//! returning wrong rows.
//!
//! QPP-12 lives here too: hash-join residual evaluation errors were swallowed
//! (`unwrap_or(false)`), so LEFT/RIGHT/FULL emitted a null-extended row where
//! the nested-loop path errors — a hash-vs-NLJ divergence inside one operator.

use super::*;

/// All left rows match: anti-semantics diverge maximally from the old
/// Anti→Left behavior (which returned every left row).
async fn anti_fixture(ex: &Executor) {
    exec(ex, "CREATE TABLE users (id INT, name TEXT)").await;
    exec(ex, "CREATE TABLE banned (user_id INT, reason TEXT)").await;
    for i in 1..=3 {
        exec(ex, &format!("INSERT INTO users VALUES ({i}, 'u{i}')")).await;
        exec(ex, &format!("INSERT INTO banned VALUES ({i}, 'spam')")).await;
    }
}

#[tokio::test]
async fn semi_anti_joins_refuse_loudly_on_both_paths() {
    let shapes = [
        "SELECT * FROM users u LEFT SEMI JOIN banned b ON u.id = b.user_id",
        "SELECT * FROM users u LEFT ANTI JOIN banned b ON u.id = b.user_id",
        "SELECT * FROM users u SEMI JOIN banned b ON u.id = b.user_id",
        "SELECT * FROM users u ANTI JOIN banned b ON u.id = b.user_id",
        "SELECT * FROM users u RIGHT SEMI JOIN banned b ON u.id = b.user_id",
        "SELECT * FROM users u RIGHT ANTI JOIN banned b ON u.id = b.user_id",
    ];
    for plan in [true, false] {
        let ex = test_executor();
        anti_fixture(&ex).await;
        if !plan {
            exec(&ex, "SET plan_execution = off").await;
        }
        for sql in shapes {
            match ex.execute(sql).await {
                Err(e) => {
                    let msg = e.to_string().to_uppercase();
                    assert!(
                        msg.contains("SEMI") || msg.contains("ANTI") || msg.contains("JOIN"),
                        "refusal must name the unsupported join, got: {e} ({sql})"
                    );
                }
                Ok(results) => panic!(
                    "must refuse instead of mis-executing ({sql}, plan_execution={}): {:?}",
                    plan,
                    results.len()
                ),
            }
        }
    }
}

/// QPP-12: an erroring residual conjunct must propagate from the hash path
/// for Inner/Left/Right/Full exactly as it does from the nested-loop path —
/// the swallow turned the error into a null-extended (or missing) row.
#[tokio::test]
async fn hash_join_residual_errors_propagate() {
    for join in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        let ex = test_executor();
        exec(&ex, "CREATE TABLE hj_a (k INT)").await;
        exec(&ex, "CREATE TABLE hj_b (k INT, w INT)").await;
        exec(&ex, "INSERT INTO hj_a VALUES (1)").await;
        exec(&ex, "INSERT INTO hj_b VALUES (1, 7)").await;

        // One equi conjunct (forces the hash path) + one erroring residual:
        // integer division by zero errors at evaluation time.
        let sql = format!("SELECT * FROM hj_a {join} hj_b ON hj_a.k = hj_b.k AND 1/0 = 1");
        let r = ex.execute(&sql).await;
        assert!(
            r.is_err(),
            "residual error must propagate through a {join} hash join: {r:?}"
        );

        // Control: the same join shape with a valid residual matches once.
        let sql = format!("SELECT * FROM hj_a {join} hj_b ON hj_a.k = hj_b.k AND hj_b.w = 7");
        let r = exec(&ex, &sql).await;
        assert_eq!(rows(&r[0]).len(), 1, "valid residual control for {join}");
    }
}
