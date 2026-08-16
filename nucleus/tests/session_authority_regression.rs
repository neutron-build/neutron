//! NU-218: an unknown session id must not resolve to the bootstrap superuser,
//! and above all must not be *writable* as one.
//!
//! `Executor::get_session` answers an unknown id with `default_session`, which
//! is constructed as the bootstrap superuser ("nucleus", role "superuser") so
//! that an unconfigured single-user deployment bypasses RLS. That fallback is
//! deliberate for reads in embedded mode. What is not deliberate is that
//! `bind_authenticated_session` and `bind_trusted_tenant` — the two calls that
//! install authority — resolve the id the same way and then WRITE to whatever
//! comes back. An id that names no session therefore stamps a principal onto
//! the process-wide fallback identity, changing who every later fallback runs
//! as.
//!
//! The audit described a read fallback. The reachable shape is a mutable global
//! identity object writable from the authentication path, which is worse.
//!
//! The wire layer manufactures exactly such an id: `session_id_from_client`
//! ends in `.unwrap_or(0)` while ids are allocated from 1, so a peer address
//! missing from the registry authenticates against session 0 — which is no
//! session at all.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;

fn executor() -> Arc<Executor> {
    Arc::new(Executor::new(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
    ))
}

async fn run(ex: &Executor, sql: &str) -> Vec<ExecResult> {
    ex.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Select { rows, .. } => rows.len(),
        other => panic!("expected a SELECT, got {other:?}"),
    }
}

async fn setup(ex: &Executor) {
    run(
        ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT)",
    )
    .await;
    run(
        ex,
        "INSERT INTO docs VALUES (1, 'alice', 'a1'), (2, 'bob', 'b1'), (3, 'alice', 'a2')",
    )
    .await;
    run(ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    run(ex, "GRANT SELECT ON docs TO alice").await;
    run(
        ex,
        "CREATE POLICY owner_isolation ON docs FOR ALL TO PUBLIC \
         USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
    )
    .await;
    run(ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
}

/// Binding authentication to an id that names no session must be refused, not
/// applied to the shared fallback identity.
///
/// The discriminating signal is the SECOND unauthenticated read: if the bind
/// mutated the shared default, the same query that saw every row before the
/// bind comes back RLS-filtered as alice afterwards, without anyone having
/// opened a session.
#[tokio::test]
async fn binding_authentication_to_an_unknown_session_is_refused() {
    let ex = executor();
    setup(&ex).await;

    let before = row_count(&run(&ex, "SELECT id FROM docs").await[0]);
    assert_eq!(
        before, 3,
        "the fallback identity is the bootstrap superuser and should see every row"
    );

    // 999_999 was never returned by create_session.
    let bind = ex.bind_authenticated_session(999_999, "alice").await;
    assert!(
        bind.is_err(),
        "binding a principal to a nonexistent session must fail closed, not \
         install that principal on the shared default session"
    );

    let after = row_count(&run(&ex, "SELECT id FROM docs").await[0]);
    assert_eq!(
        after, before,
        "an unknown-id bind changed who an unauthenticated query runs as: \
         the shared fallback identity was mutated from outside any session"
    );
}

/// Session 0 specifically, because that is the value the wire layer produces
/// when a peer address is missing from the registry (`unwrap_or(0)`, against
/// ids allocated from 1).
#[tokio::test]
async fn binding_authentication_to_session_zero_is_refused() {
    let ex = executor();
    setup(&ex).await;

    let before = row_count(&run(&ex, "SELECT id FROM docs").await[0]);
    assert!(ex.bind_authenticated_session(0, "alice").await.is_err());
    let after = row_count(&run(&ex, "SELECT id FROM docs").await[0]);
    assert_eq!(
        after, before,
        "session 0 is the id the wire layer manufactures when the registry \
         lookup misses; it must never authenticate"
    );
}

/// A tenant claim is authority too — `bind_trusted_tenant`'s own comment says
/// SQL `SET nucleus.tenant_id` is deliberately not an authority source, which
/// is precisely why this entry point must not accept an unknown id either.
#[tokio::test]
async fn binding_a_tenant_to_an_unknown_session_is_refused() {
    let ex = executor();
    setup(&ex).await;
    assert!(
        ex.bind_trusted_tenant(999_999, Some("acme".into()))
            .is_err(),
        "a tenant claim must not be installable on the shared default session"
    );
}

/// The legitimate path still works: a real session binds, and the binding is
/// scoped to it rather than leaking to the fallback.
#[tokio::test]
async fn a_real_session_still_binds_and_stays_scoped() {
    let ex = executor();
    setup(&ex).await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice")
        .await
        .expect("a real session must still be bindable");

    let as_alice = ex
        .execute_with_session(sid, "SELECT id FROM docs")
        .await
        .unwrap();
    assert_eq!(
        row_count(&as_alice[0]),
        2,
        "alice should see only her own rows"
    );

    // The fallback identity is untouched.
    assert_eq!(
        row_count(&run(&ex, "SELECT id FROM docs").await[0]),
        3,
        "binding a real session must not change the fallback identity"
    );
    ex.drop_session(sid);
}
