//! The security audit log records what an incident review has to reconstruct.
//!
//! `security::AuditLog` existed before this and had no callers anywhere in the
//! crate: an in-memory `Vec` that nothing wrote to, nothing bounded and
//! nothing persisted. So "who logged in, who failed, who changed authority"
//! had no answer at all. These tests assert the events reach disk through the
//! ordinary statement paths, and — the part worth guarding — that nothing
//! sensitive rides along with them.

use super::super::Executor;
use super::exec;
use super::test_meta_persistence::open_executor;
use crate::audit::AuditSink;

async fn lines(dir: &std::path::Path) -> Vec<String> {
    AuditSink::read_all(&dir.join("audit"))
}

fn has_kind(lines: &[String], kind: &str, principal: &str) -> bool {
    lines
        .iter()
        .any(|l| l.contains(&format!("\"kind\":\"{kind}\"")) && l.contains(principal))
}

#[tokio::test]
async fn authority_changes_are_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let ex: Executor = open_executor(dir.path()).await;

    exec(&ex, "CREATE TABLE t (id INT, tenant TEXT)").await;
    exec(&ex, "CREATE ROLE app_user LOGIN PASSWORD 'hunter2'").await;
    exec(&ex, "ALTER ROLE app_user VALID UNTIL '2030-01-01 00:00:00'").await;
    exec(&ex, "GRANT SELECT ON t TO app_user").await;
    exec(&ex, "REVOKE SELECT ON t FROM app_user").await;
    exec(
        &ex,
        "CREATE POLICY tenant_read ON t FOR SELECT TO app_user USING (tenant = 'acme')",
    )
    .await;
    // The most destructive authority change must leave a record too —
    // an intruder erasing principals is exactly the case the trail exists for.
    exec(&ex, "DROP ROLE app_user").await;
    exec(&ex, "DROP ROLE IF EXISTS ghost_never_existed").await;

    let l = lines(dir.path()).await;
    for (kind, principal) in [
        ("role_created", "app_user"),
        ("role_altered", "app_user"),
        ("privilege_granted", "app_user"),
        ("privilege_revoked", "app_user"),
        ("policy_changed", "tenant_read"),
        ("role_dropped", "app_user"),
    ] {
        assert!(
            has_kind(&l, kind, principal),
            "no {kind} event for {principal}; log was:\n{}",
            l.join("\n")
        );
    }
    assert!(
        !l.iter()
            .any(|line| line.contains("role_dropped") && line.contains("ghost_never_existed")),
        "DROP ROLE IF EXISTS on a missing role must not record a drop; log was:\n{}",
        l.join("\n")
    );
}

/// The audit log must never become the place the password leaks.
///
/// It records that a password changed, not what it changed to — and the
/// statement text is deliberately not stored, because `CREATE ROLE ...
/// PASSWORD 'x'` carries the credential in the clear.
#[tokio::test]
async fn no_credential_material_reaches_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let ex: Executor = open_executor(dir.path()).await;
    exec(&ex, "CREATE ROLE leaky LOGIN PASSWORD 'super-secret-value'").await;
    exec(&ex, "ALTER ROLE leaky PASSWORD 'another-secret-value'").await;

    let l = lines(dir.path()).await;
    let joined = l.join("\n");
    assert!(
        !joined.contains("secret-value"),
        "a password literal reached the audit log:\n{joined}"
    );
    assert!(
        !joined.contains("SCRAM-SHA-256$"),
        "a stored verifier reached the audit log:\n{joined}"
    );
    // Control: the events themselves ARE there, so the absence above is not
    // just an empty log.
    assert!(has_kind(&l, "role_created", "leaky"), "{joined}");
    assert!(
        l.iter()
            .any(|line| line.contains("role_altered") && line.contains("password")),
        "the fact of a password change must be recorded: {joined}"
    );
}

/// Both refusal paths and the success path are recorded.
#[tokio::test]
async fn logins_and_refusals_are_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let ex: Executor = open_executor(dir.path()).await;
    exec(&ex, "CREATE ROLE good LOGIN PASSWORD 'p'").await;
    exec(&ex, "CREATE ROLE nologin_role PASSWORD 'p'").await;
    exec(
        &ex,
        "CREATE ROLE stale LOGIN PASSWORD 'p' VALID UNTIL '2020-01-01 00:00:00'",
    )
    .await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "good").await.unwrap();
    let sid = ex.create_session();
    let _ = ex.bind_authenticated_session(sid, "nologin_role").await;
    let sid = ex.create_session();
    let _ = ex.bind_authenticated_session(sid, "stale").await;

    let l = lines(dir.path()).await;
    assert!(has_kind(&l, "login_succeeded", "good"), "{l:?}");
    assert!(has_kind(&l, "login_refused", "nologin_role"), "{l:?}");
    assert!(
        l.iter().any(|line| line.contains("login_refused")
            && line.contains("stale")
            && line.contains("expired")),
        "an expired-password refusal must be distinguishable from a NOLOGIN one: {l:?}"
    );
}

/// The log survives a restart — it is the same directory, reopened.
#[tokio::test]
async fn events_survive_a_restart_and_keep_accumulating() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex: Executor = open_executor(dir.path()).await;
        exec(&ex, "CREATE ROLE before_restart LOGIN PASSWORD 'p'").await;
    }
    let first = lines(dir.path()).await.len();
    assert!(first > 0);
    {
        let ex: Executor = open_executor(dir.path()).await;
        exec(&ex, "CREATE ROLE after_restart LOGIN PASSWORD 'p'").await;
    }
    let l = lines(dir.path()).await;
    assert!(
        has_kind(&l, "role_created", "before_restart")
            && has_kind(&l, "role_created", "after_restart"),
        "a restart must not truncate the audit log: {l:?}"
    );
}
