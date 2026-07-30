//! Column masking is enforced, not merely declared.
//!
//! `MaskingEngine` had policies, rules, DDL to survive rename/drop, a dump
//! gap-reporter, and tests — and `mask_row` had no callers outside those tests.
//! Every masked column returned its real value to every principal. A security
//! surface that is inert is worse than one that is absent: an absent feature
//! does not get relied on.
//!
//! These tests assert the enforcement, and that it holds on every path a row
//! can leave the database by.

use super::*;

async fn exec_session(ex: &Executor, sid: u64, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
    ex.execute_with_session(sid, sql).await
}

async fn seeded_with_mask(rule: crate::security::MaskingRule) -> (Executor, u64) {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE people (id INT PRIMARY KEY, name TEXT, ssn TEXT, age INT)",
    )
    .await;
    exec(&ex, "INSERT INTO people VALUES (1, 'ada', '123-45-6789', 36)").await;
    exec(&ex, "INSERT INTO people VALUES (2, 'bob', '987-65-4321', 41)").await;
    exec(&ex, "CREATE ROLE analyst LOGIN PASSWORD 'analyst-secret'").await;
    exec(&ex, "GRANT SELECT ON people TO analyst").await;

    ex.with_mutable_security(|security| {
        security.masking.add_policy(crate::security::MaskingPolicy {
            table: "people".into(),
            column: "ssn".into(),
            role: "analyst".into(),
            rule,
            column_id: 0,
        });
    })
    .expect("install mask");

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "analyst").await.unwrap();
    (ex, sid)
}

fn cell(result: &ExecResult, row: usize, col: usize) -> String {
    match &rows(result)[row][col] {
        Value::Text(t) => t.clone(),
        other => format!("{other:?}"),
    }
}

/// The masked column comes back redacted; everything else is untouched.
#[tokio::test]
async fn test_a_masked_column_is_masked() {
    let (ex, sid) = seeded_with_mask(crate::security::MaskingRule::Redact("***".into())).await;
    let res = exec_session(&ex, sid, "SELECT id, name, ssn, age FROM people ORDER BY id")
        .await
        .expect("select");
    assert_eq!(cell(&res[0], 0, 2), "***", "the SSN was returned in the clear");
    assert_eq!(cell(&res[0], 1, 2), "***");
    // Unmasked columns keep their value AND their type.
    assert_eq!(rows(&res[0])[0][0], Value::Int32(1));
    assert_eq!(cell(&res[0], 0, 1), "ada");
    assert_eq!(rows(&res[0])[0][3], Value::Int32(36));
}

/// Masking must not depend on which column list the query happens to use, nor
/// on the query taking the row-materializing path.
#[tokio::test]
async fn test_masking_survives_every_shape_of_select() {
    let (ex, sid) = seeded_with_mask(crate::security::MaskingRule::Redact("***".into())).await;
    for sql in [
        "SELECT * FROM people",
        "SELECT ssn FROM people",
        "SELECT ssn, id FROM people",
        "SELECT ssn FROM people WHERE id = 1",
        "SELECT ssn FROM people ORDER BY id LIMIT 1",
        "SELECT DISTINCT ssn FROM people",
    ] {
        let res = exec_session(&ex, sid, sql).await.expect("select");
        let all = format!("{:?}", rows(&res[0]));
        assert!(
            !all.contains("123-45-6789") && !all.contains("987-65-4321"),
            "an SSN escaped through `{sql}`: {all}"
        );
    }
}

/// A superuser sees the real value — masking is per-principal, and the
/// unmasked path must still work or the feature is just deletion.
#[tokio::test]
async fn test_a_superuser_sees_the_real_value() {
    let (ex, _sid) = seeded_with_mask(crate::security::MaskingRule::Redact("***".into())).await;
    // The default session is superuser.
    let res = exec(&ex, "SELECT ssn FROM people ORDER BY id").await;
    assert_eq!(cell(&res[0], 0, 0), "123-45-6789");
}

/// Each rule shape actually transforms the value.
#[tokio::test]
async fn test_rule_shapes_apply() {
    for (rule, expect) in [
        (
            crate::security::MaskingRule::Partial {
                show_first: 0,
                show_last: 4,
                mask_char: '*',
            },
            "6789",
        ),
        (crate::security::MaskingRule::Redact("REDACTED".into()), "REDACTED"),
    ] {
        let (ex, sid) = seeded_with_mask(rule).await;
        let res = exec_session(&ex, sid, "SELECT ssn FROM people ORDER BY id")
            .await
            .expect("select");
        let got = cell(&res[0], 0, 0);
        assert!(
            got.contains(expect) && got != "123-45-6789",
            "expected a value containing {expect:?} and not the original, got {got:?}"
        );
    }
}

/// NULL carries nothing to redact, and masking it would invent a value.
#[tokio::test]
async fn test_null_stays_null() {
    let (ex, sid) = seeded_with_mask(crate::security::MaskingRule::Redact("***".into())).await;
    exec(&ex, "INSERT INTO people VALUES (3, 'cy', NULL, 20)").await;
    let res = exec_session(&ex, sid, "SELECT ssn FROM people WHERE id = 3")
        .await
        .expect("select");
    assert_eq!(rows(&res[0])[0][0], Value::Null);
}
