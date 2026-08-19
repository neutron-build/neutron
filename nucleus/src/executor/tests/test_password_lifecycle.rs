//! Adversarial suite for the password lifecycle: creation, rotation, expiry,
//! and what each of them must deny.
//!
//! The defect this suite was written around is the reason it is adversarial
//! rather than a happy-path check. `CREATE ROLE r LOGIN PASSWORD 'p' VALID
//! UNTIL '2020-01-01'` parsed, succeeded, and the expiry was **discarded** —
//! `CreateRole::valid_until` and `RoleOption::ValidUntil` both fell through
//! unmatched arms. The role authenticated indefinitely, `pg_roles.rolvaliduntil`
//! and `pg_shadow.valuntil` reported NULL for every role because nothing ever
//! filled them, and an operator's only evidence that the deadline existed was
//! that the statement had not errored.
//!
//! That is the same bug class as `FOR UPDATE SKIP LOCKED` being parsed and
//! never read: a clause that carries a GUARANTEE, accepted and silently
//! dropped. Every test here asks what an attacker gets, not what a user sees.

use super::*;

/// Microseconds since the Unix epoch, now.
fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

async fn valid_until_of(ex: &Executor, role: &str) -> Option<i64> {
    ex.roles.read().await.get(role).and_then(|r| r.valid_until)
}

async fn password_hash_of(ex: &Executor, role: &str) -> Option<String> {
    ex.roles
        .read()
        .await
        .get(role)
        .and_then(|r| r.password_hash.clone())
}

/// An expired password must authenticate nothing, through either gate.
#[tokio::test]
async fn an_expired_password_authenticates_nothing() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE ROLE expired_user LOGIN PASSWORD 'secret' VALID UNTIL '2020-01-01 00:00:00'",
    )
    .await;
    // Control, in the same test: an identical role whose deadline has not
    // passed. Without it, a check that denied everything would pass.
    exec(
        &ex,
        "CREATE ROLE live_user LOGIN PASSWORD 'secret' VALID UNTIL '2999-01-01 00:00:00'",
    )
    .await;

    assert!(
        ex.scram_credentials("expired_user").await.is_none(),
        "an expired role must not hand out SCRAM material"
    );
    assert!(
        ex.scram_credentials("live_user").await.is_some(),
        "control: an unexpired role must still authenticate"
    );

    // The session gate is checked separately from the SCRAM path on purpose:
    // trust and certificate authentication never ask for a verifier, so a
    // check that lives only beside the password covers only the password.
    let sid = ex.create_session();
    let err = ex
        .bind_authenticated_session(sid, "expired_user")
        .await
        .expect_err("an expired role must not bind a session");
    let msg = err.to_string();
    assert!(
        msg.contains("expired"),
        "the refusal must say the password expired rather than blaming login \
         permission; got: {msg}"
    );

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "live_user")
        .await
        .expect("control: an unexpired role must bind");
}

/// The deadline is a moment, not a flag: a role expires while it is running.
#[tokio::test]
async fn a_deadline_one_second_away_expires_without_any_ddl() {
    let ex = test_executor();
    exec(&ex, "CREATE ROLE soon LOGIN PASSWORD 'secret'").await;
    {
        let mut roles = ex.roles.write().await;
        let r = roles.get_mut("soon").unwrap();
        r.valid_until = Some(now_us() + 50_000); // 50ms out
    }
    assert!(
        ex.scram_credentials("soon").await.is_some(),
        "before the deadline the role authenticates"
    );
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert!(
        ex.scram_credentials("soon").await.is_none(),
        "after the deadline it must stop, with no statement having run"
    );
}

/// An unparseable deadline must fail the statement, not drop the clause.
///
/// This is the exact shape of the original defect: accepting the statement and
/// discarding the guarantee leaves a role that looks protected and is not.
#[tokio::test]
async fn an_unparseable_deadline_fails_the_statement() {
    let ex = test_executor();
    let err = ex
        .execute("CREATE ROLE bad_user LOGIN PASSWORD 'p' VALID UNTIL 'not-a-timestamp'")
        .await
        .expect_err("an unparseable VALID UNTIL must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("valid until"),
        "the error must name the clause it rejected; got: {err}"
    );
    assert!(
        !ex.roles.read().await.contains_key("bad_user"),
        "a rejected VALID UNTIL must not leave behind a role with NO expiry — \
         that is worse than the error, because it looks like it worked"
    );
}

/// Rotation: the deadline can be moved, and cleared.
#[tokio::test]
async fn a_deadline_can_be_rotated_and_cleared() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE ROLE rot LOGIN PASSWORD 'p' VALID UNTIL '2020-01-01 00:00:00'",
    )
    .await;
    assert!(ex.scram_credentials("rot").await.is_none());

    exec(&ex, "ALTER ROLE rot VALID UNTIL '2999-01-01 00:00:00'").await;
    assert!(
        ex.scram_credentials("rot").await.is_some(),
        "extending the deadline must revive the login"
    );
    let extended = valid_until_of(&ex, "rot").await;
    assert!(extended.is_some());

    exec(&ex, "ALTER ROLE rot VALID UNTIL 'infinity'").await;
    assert_eq!(
        valid_until_of(&ex, "rot").await,
        None,
        "'infinity' means no expiry, as in PostgreSQL"
    );
}

/// Rotating the password replaces the verifier; the old one cannot be
/// recovered from what is stored.
#[tokio::test]
async fn rotating_a_password_replaces_the_stored_verifier() {
    let ex = test_executor();
    exec(&ex, "CREATE ROLE rotate_pw LOGIN PASSWORD 'first-secret'").await;
    let first = password_hash_of(&ex, "rotate_pw").await.unwrap();

    exec(&ex, "ALTER ROLE rotate_pw PASSWORD 'second-secret'").await;
    let second = password_hash_of(&ex, "rotate_pw").await.unwrap();

    assert_ne!(first, second, "rotation must change the stored verifier");
    for stored in [&first, &second] {
        assert!(
            !stored.contains("first-secret") && !stored.contains("second-secret"),
            "a raw password must never be retained; stored form was {stored}"
        );
    }

    exec(&ex, "ALTER ROLE rotate_pw PASSWORD NULL").await;
    assert_eq!(
        password_hash_of(&ex, "rotate_pw").await,
        None,
        "PASSWORD NULL must remove the credential"
    );
    assert!(
        ex.scram_credentials("rotate_pw").await.is_none(),
        "a role with no credential must not authenticate"
    );
}

/// NOLOGIN and expiry are independent denials, and neither substitutes for the
/// other.
#[tokio::test]
async fn nologin_and_expiry_deny_independently() {
    let ex = test_executor();
    exec(&ex, "CREATE ROLE nologin_role PASSWORD 'p'").await; // no LOGIN
    exec(
        &ex,
        "CREATE ROLE expired_role LOGIN PASSWORD 'p' VALID UNTIL '2020-01-01 00:00:00'",
    )
    .await;

    let sid = ex.create_session();
    let msg = ex
        .bind_authenticated_session(sid, "nologin_role")
        .await
        .expect_err("NOLOGIN must be refused")
        .to_string();
    assert!(
        msg.contains("not permitted to log in"),
        "NOLOGIN must be reported as NOLOGIN; got: {msg}"
    );

    let sid = ex.create_session();
    let msg = ex
        .bind_authenticated_session(sid, "expired_role")
        .await
        .expect_err("an expired password must be refused")
        .to_string();
    assert!(
        msg.contains("expired"),
        "expiry must be reported as expiry; got: {msg}"
    );
}

/// The catalog views report the deadline. `pg_roles.rolvaliduntil` and
/// `pg_user.valuntil` both declared the column and filled it with NULL for
/// every role, which reads as "no role in this database has an expiry".
#[tokio::test]
async fn pg_roles_and_pg_user_report_the_deadline() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE ROLE viewed LOGIN PASSWORD 'p' VALID UNTIL '2030-06-01 12:00:00'",
    )
    .await;

    for (view, name_col, until_col) in [
        ("pg_roles", "rolname", "rolvaliduntil"),
        ("pg_user", "usename", "valuntil"),
    ] {
        let results = exec(&ex, &format!("SELECT {name_col}, {until_col} FROM {view}")).await;
        let r = rows(&results[0]);
        let row = r
            .iter()
            .find(|row| matches!(&row[0], Value::Text(n) if n == "viewed"))
            .unwrap_or_else(|| panic!("{view} did not list the role"));
        match &row[1] {
            Value::Text(s) => assert!(
                s.contains("2030-06-01"),
                "{view}.{until_col} must carry the deadline; got {s}"
            ),
            other => panic!("{view}.{until_col} must not be {other:?} for a role that has one"),
        }
        // Control: the bootstrap role has no deadline and must still read NULL.
        let boot = r
            .iter()
            .find(|row| matches!(&row[0], Value::Text(n) if n == "nucleus"));
        if let Some(boot) = boot {
            assert!(
                matches!(boot[1], Value::Null),
                "a role with no expiry must report NULL, not a default deadline"
            );
        }
    }
}

/// A dump that drops the deadline restores a password that never expires.
#[tokio::test]
async fn a_logical_dump_carries_the_deadline() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE ROLE dumped LOGIN PASSWORD 'p' VALID UNTIL '2031-02-03 04:05:06'",
    )
    .await;
    let roles = ex.roles.read().await;
    let line = super::super::logical_dump::render_create_role(roles.get("dumped").unwrap());
    assert!(
        line.contains("VALID UNTIL") && line.contains("2031-02-03"),
        "the dump must carry the deadline, or a restore silently removes it: {line}"
    );
}
