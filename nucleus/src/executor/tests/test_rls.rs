use super::*;

async fn exec_session(ex: &Executor, sid: u64, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
    ex.execute_with_session(sid, sql).await
}

async fn setup_owner_policy(ex: &Executor) {
    exec(
        ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT)",
    )
    .await;
    exec(
        ex,
        "INSERT INTO docs VALUES (1, 'alice', 'a1'), (2, 'bob', 'b1'), (3, 'alice', 'a2')",
    )
    .await;
    exec(ex, "CREATE ROLE alice LOGIN PASSWORD 'alice-secret'").await;
    exec(ex, "CREATE ROLE bob LOGIN PASSWORD 'bob-secret'").await;
    exec(
        ex,
        "GRANT SELECT, INSERT, UPDATE, DELETE ON docs TO alice, bob",
    )
    .await;
    exec(
        ex,
        "CREATE POLICY owner_isolation ON docs FOR ALL TO PUBLIC USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
    )
    .await;
    exec(ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
}

#[tokio::test]
async fn rls_filters_reads_before_joins_subqueries_and_aggregates() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();

    let result = exec_session(&ex, sid, "SELECT id FROM docs ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 2);

    let result = exec_session(&ex, sid, "SELECT COUNT(*) FROM docs")
        .await
        .unwrap();
    assert_eq!(scalar(&result[0]), &Value::Int64(2));

    let result = exec_session(
        &ex,
        sid,
        "SELECT a.id, b.id FROM docs a JOIN docs b ON a.owner = b.owner ORDER BY a.id, b.id",
    )
    .await
    .unwrap();
    assert_eq!(rows(&result[0]).len(), 4);

    let result = exec_session(
        &ex,
        sid,
        "SELECT id FROM docs WHERE id IN (SELECT id FROM docs) ORDER BY id",
    )
    .await
    .unwrap();
    assert_eq!(rows(&result[0]).len(), 2);
}

#[tokio::test]
async fn rls_enforces_old_rows_and_with_check_on_every_dml_shape() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();

    let result = exec_session(&ex, sid, "UPDATE docs SET body = 'x' WHERE id = 2")
        .await
        .unwrap();
    assert!(matches!(
        &result[0],
        ExecResult::Command {
            rows_affected: 0,
            ..
        }
    ));

    let result = exec_session(&ex, sid, "DELETE FROM docs WHERE id = 2")
        .await
        .unwrap();
    assert!(matches!(
        &result[0],
        ExecResult::Command {
            rows_affected: 0,
            ..
        }
    ));

    exec_session(&ex, sid, "INSERT INTO docs VALUES (4, 'alice', 'allowed')")
        .await
        .unwrap();
    assert!(
        exec_session(&ex, sid, "INSERT INTO docs VALUES (5, 'bob', 'denied')")
            .await
            .is_err()
    );
    assert!(
        exec_session(&ex, sid, "UPDATE docs SET owner = 'bob' WHERE id = 1")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn authenticated_identity_cannot_be_spoofed_and_cache_cannot_cross_principals() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    let alice = ex.create_session();
    let bob = ex.create_session();
    ex.bind_authenticated_session(alice, "alice").await.unwrap();
    ex.bind_authenticated_session(bob, "bob").await.unwrap();

    assert!(
        exec_session(&ex, alice, "SET SESSION AUTHORIZATION nucleus")
            .await
            .is_err()
    );
    assert!(
        exec_session(&ex, alice, "SET nucleus.tenant_id = 'tenant-b'")
            .await
            .is_err()
    );

    let alice_rows = exec_session(&ex, alice, "SELECT owner FROM docs ORDER BY id")
        .await
        .unwrap();
    let bob_rows = exec_session(&ex, bob, "SELECT owner FROM docs ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&alice_rows[0]).len(), 2);
    assert_eq!(rows(&bob_rows[0]).len(), 1);
    assert_eq!(rows(&bob_rows[0])[0][0], Value::Text("bob".into()));
}

#[tokio::test]
async fn unsupported_alternate_surfaces_fail_closed_under_rls() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    assert!(exec_session(&ex, sid, "SELECT CDC_COUNT()").await.is_err());
    assert!(exec_session(&ex, sid, "SELECT DOC_GET('1')").await.is_err());
    assert!(
        exec_session(&ex, sid, "SELECT DATALOG_IMPORT('docs', 'leaked')")
            .await
            .is_err()
    );
    assert!(
        exec_session(&ex, sid, "SELECT CYPHER('MATCH (n) RETURN n')")
            .await
            .is_err()
    );
    assert!(
        exec_session(&ex, sid, "SUBSCRIBE SELECT * FROM docs")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn enabled_rls_without_an_applicable_permissive_policy_is_default_deny() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE secrets (id INT)").await;
    exec(&ex, "INSERT INTO secrets VALUES (1)").await;
    exec(&ex, "CREATE ROLE reader LOGIN PASSWORD 'secret'").await;
    exec(&ex, "GRANT SELECT ON secrets TO reader").await;
    exec(&ex, "ALTER TABLE secrets ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "reader").await.unwrap();
    let result = exec_session(&ex, sid, "SELECT * FROM secrets")
        .await
        .unwrap();
    assert!(rows(&result[0]).is_empty());
}

#[tokio::test]
async fn set_role_requires_catalog_membership_and_membership_can_be_revoked() {
    let ex = test_executor();
    exec(&ex, "CREATE ROLE analyst").await;
    exec(&ex, "CREATE ROLE app_user LOGIN PASSWORD 'secret'").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "app_user")
        .await
        .unwrap();
    assert!(exec_session(&ex, sid, "SET ROLE analyst").await.is_err());

    exec(&ex, "GRANT ROLE analyst TO app_user").await;
    exec_session(&ex, sid, "SET ROLE analyst").await.unwrap();

    exec(&ex, "REVOKE ROLE analyst FROM app_user").await;
    assert!(exec_session(&ex, sid, "SET ROLE analyst").await.is_err());
}

#[tokio::test]
async fn policy_ddl_obeys_transactions_and_savepoints() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE guarded (id INT, owner TEXT)").await;
    exec(&ex, "INSERT INTO guarded VALUES (1, 'alice'), (2, 'bob')").await;
    exec(&ex, "CREATE ROLE alice LOGIN PASSWORD 'secret'").await;
    exec(&ex, "GRANT SELECT ON guarded TO alice").await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY").await;
    exec(
        &ex,
        "CREATE POLICY rolled_back ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    // The policy catalog is transaction-local until COMMIT. Another session
    // must continue to see the committed (RLS-disabled) state.
    let result = exec_session(&ex, sid, "SELECT * FROM guarded")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 2);

    exec(&ex, "ROLLBACK").await;
    let result = exec_session(&ex, sid, "SELECT * FROM guarded")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 2);

    exec(&ex, "BEGIN").await;
    exec(&ex, "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY").await;
    exec(&ex, "SAVEPOINT before_policy").await;
    exec(
        &ex,
        "CREATE POLICY discarded ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await;
    exec(&ex, "ROLLBACK TO SAVEPOINT before_policy").await;
    exec(&ex, "COMMIT").await;

    let result = exec_session(&ex, sid, "SELECT * FROM guarded")
        .await
        .unwrap();
    assert!(rows(&result[0]).is_empty());
}

#[tokio::test]
async fn table_rename_moves_policies_and_drop_removes_them() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    exec(&ex, "ALTER TABLE docs RENAME TO renamed_docs").await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    let result = exec_session(&ex, sid, "SELECT * FROM renamed_docs")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 2);

    exec(&ex, "DROP TABLE renamed_docs").await;
    exec(
        &ex,
        "CREATE TABLE renamed_docs (id INT, owner TEXT, body TEXT)",
    )
    .await;
    exec(&ex, "INSERT INTO renamed_docs VALUES (9, 'bob', 'fresh')").await;
    exec(&ex, "GRANT SELECT ON renamed_docs TO alice").await;
    let result = exec_session(&ex, sid, "SELECT * FROM renamed_docs")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 1);
}

#[tokio::test]
async fn restrictive_policy_never_grants_access_by_itself() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE guarded (id INT)").await;
    exec(&ex, "INSERT INTO guarded VALUES (1)").await;
    exec(&ex, "CREATE ROLE reader LOGIN PASSWORD 'secret'").await;
    exec(&ex, "GRANT SELECT ON guarded TO reader").await;
    exec(
        &ex,
        "CREATE POLICY only_restriction ON guarded AS RESTRICTIVE FOR SELECT TO PUBLIC USING (true)",
    )
    .await;
    exec(&ex, "ALTER TABLE guarded ENABLE ROW LEVEL SECURITY").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "reader").await.unwrap();
    let result = exec_session(&ex, sid, "SELECT * FROM guarded")
        .await
        .unwrap();
    assert!(rows(&result[0]).is_empty());
}

#[tokio::test]
async fn copy_to_exports_only_policy_visible_rows() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    let result = exec_session(&ex, sid, "COPY docs TO STDOUT").await.unwrap();
    match &result[0] {
        ExecResult::CopyOut { data, row_count } => {
            assert_eq!(*row_count, 2);
            assert!(data.contains("alice"));
            assert!(!data.contains("bob"));
        }
        other => panic!("expected COPY output, got {other:?}"),
    }
}

#[cfg(feature = "server")]
#[tokio::test]
async fn principal_less_protocol_and_cluster_forwarding_fail_closed() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    assert!(ex.rls_configured());
    assert!(
        ex.execute_principal_less_forward("SELECT * FROM docs")
            .await
            .is_err()
    );
    assert!(
        ex.execute_principal_less_forward("DELETE FROM docs")
            .await
            .is_err()
    );
}

/// `ALTER TABLE ... RENAME TO` must carry the GRANTs with the table.
///
/// Privileges are keyed by table name in `RoleDef::privileges`, and the rename
/// path moved the RLS policies and masking rules but left the grants behind
/// under the old name — so every grantee silently lost access to the table they
/// had been granted. It stayed invisible for as long as privileges were not
/// consulted on the read path.
///
/// Asserted through a real bound session rather than `has_table_privilege`,
/// which ignores its user argument and reports on the *current* session — in a
/// test that runs as superuser it answers `true` for everything, so it cannot
/// witness this.
#[tokio::test]
async fn rename_table_carries_grants_with_it() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    exec(&ex, "ALTER TABLE docs RENAME TO renamed_docs").await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    let result = exec_session(&ex, sid, "SELECT id FROM renamed_docs ORDER BY id")
        .await
        .expect("alice's GRANT did not follow the table across RENAME");
    assert_eq!(
        rows(&result[0]).len(),
        2,
        "alice should still see her 2 rows"
    );
}

/// Renaming the column a policy reads must keep the policy on that COLUMN.
///
/// Predicates used to store only the column NAME. A rename left the policy
/// naming a column that no longer existed, which failed closed on its own —
/// but `ADD COLUMN` could then recreate the old name and the policy would
/// silently begin guarding the new, attacker-chosen column instead. Predicates
/// now carry the column's stable id and the name is refreshed through it.
#[tokio::test]
async fn renaming_a_policy_column_keeps_the_policy_on_that_column() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    exec(&ex, "ALTER TABLE docs RENAME COLUMN owner TO owner_real").await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    let result = exec_session(&ex, sid, "SELECT id FROM docs ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows(&result[0]).len(),
        2,
        "policy should still filter on the renamed column, showing alice her 2 rows"
    );

    // The escalation this closes: reintroduce the old name and make it match
    // every row. The policy must keep reading `owner_real`, not this decoy.
    exec(
        &ex,
        "ALTER TABLE docs ADD COLUMN owner TEXT DEFAULT 'alice'",
    )
    .await;
    let after = exec_session(&ex, sid, "SELECT id FROM docs ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows(&after[0]).len(),
        2,
        "a re-added column with the old name captured the policy: alice saw {} rows",
        rows(&after[0]).len()
    );
}

/// Dropping a column a policy reads must fail, and CASCADE must drop the policy.
#[tokio::test]
async fn dropping_a_policy_column_requires_cascade() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;

    let blocked = ex.execute("ALTER TABLE docs DROP COLUMN owner").await;
    let err = blocked.expect_err("dropping a column a policy depends on must fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("owner_isolation"),
        "the error should name the dependent policy, got: {msg}"
    );

    exec(&ex, "ALTER TABLE docs DROP COLUMN owner CASCADE").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "alice").await.unwrap();
    // The policy is gone with the column; RLS is still enabled, so with no
    // policy admitting anything the table denies rather than opens.
    let result = exec_session(&ex, sid, "SELECT id FROM docs").await.unwrap();
    assert!(
        rows(&result[0]).is_empty(),
        "with its only policy CASCADE-dropped, an RLS-enabled table must deny, not open"
    );
}

/// `has_table_privilege(user, table, privilege)` must answer about the NAMED
/// user, not the caller.
///
/// It previously ignored its first argument and reported on the current
/// session, so a superuser asking about anyone got `true` for everything — and
/// this is the function an audit query trusts. It also made an earlier version
/// of the rename test above pass vacuously.
#[tokio::test]
async fn has_table_privilege_reports_on_the_named_user() {
    let ex = test_executor();
    setup_owner_policy(&ex).await;
    exec(&ex, "CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)").await;
    exec(&ex, "CREATE ROLE mallory LOGIN PASSWORD 'x'").await;

    // Asked as superuser, about someone else, on a table they were never granted.
    let denied = exec(
        &ex,
        "SELECT has_table_privilege('mallory', 'secrets', 'SELECT')",
    )
    .await;
    assert_eq!(
        *scalar(&denied[0]),
        Value::Bool(false),
        "mallory has no GRANT on secrets, so this must be false even when a superuser asks"
    );

    // And true where the grant genuinely exists.
    let allowed = exec(&ex, "SELECT has_table_privilege('alice', 'docs', 'SELECT')").await;
    assert_eq!(
        *scalar(&allowed[0]),
        Value::Bool(true),
        "alice was granted SELECT on docs in setup"
    );
}

/// A renamed column must not strand the derived-index registries or the durable
/// engine sidecar.
///
/// `TableEngineMeta` records ORDER BY / version / aggregate columns by NAME in
/// engines.json and nothing rewrote them on RENAME COLUMN, so a stale name
/// survived restart. The derived-index registries also key on the column name,
/// but those turn out to be repaired already by the catalog index drop/recreate
/// on rename — this asserts the end-to-end behaviour rather than registry
/// internals, which is what actually has to hold.
#[tokio::test]
async fn rename_column_rewrites_derived_index_registries() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE emb (id INT PRIMARY KEY, body TEXT, v VECTOR(3))",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO emb VALUES (1,'a',VECTOR('[1,0,0]')),(2,'b',VECTOR('[0,1,0]'))",
    )
    .await;
    exec(&ex, "CREATE INDEX ix_v ON emb USING IVFFLAT (v)").await;

    exec(&ex, "ALTER TABLE emb RENAME COLUMN v TO embedding").await;

    // End-to-end is the meaningful assertion here. A registry-contents check
    // passes even with the rewrite disabled, because the catalog index is
    // dropped and recreated under the new name by the rewrite above.
    let knn = exec(
        &ex,
        "SELECT id FROM emb ORDER BY VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]')) LIMIT 1",
    )
    .await;
    assert_eq!(rows(&knn[0]).len(), 1);
}

#[tokio::test]
async fn rls_comparison_in_list_and_null_predicates_enforce() {
    let ex = test_executor();
    exec(
        &ex,
        "CREATE TABLE ledger (id INT PRIMARY KEY, amount INT, region TEXT)",
    )
    .await;
    // Row 4 carries a NULL amount and a NULL region — the fail-closed case.
    exec(
        &ex,
        "INSERT INTO ledger VALUES (1, 9, 'eu'), (2, 200, 'us'), (3, 100, 'apac'), (4, NULL, NULL)",
    )
    .await;
    exec(&ex, "CREATE ROLE auditor LOGIN PASSWORD 'auditor-secret'").await;
    exec(&ex, "GRANT SELECT ON ledger TO auditor").await;
    exec(
        &ex,
        "CREATE POLICY big_only ON ledger FOR SELECT TO PUBLIC USING (amount > 100)",
    )
    .await;
    exec(&ex, "ALTER TABLE ledger ENABLE ROW LEVEL SECURITY").await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "auditor").await.unwrap();

    // Only id=2 (200). id=1 is 9 — which would pass a LEXICAL "9" > "100"
    // compare, so this asserts the numeric path. id=3 is not strictly greater,
    // id=4 is NULL and must be withheld.
    let result = exec_session(&ex, sid, "SELECT id FROM ledger ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 1);
    assert_eq!(rows(&result[0])[0][0], Value::Int32(2));

    // IN over a literal list.
    exec(&ex, "DROP POLICY big_only ON ledger").await;
    exec(
        &ex,
        "CREATE POLICY known_regions ON ledger FOR SELECT TO PUBLIC USING (region IN ('eu', 'us'))",
    )
    .await;
    let result = exec_session(&ex, sid, "SELECT id FROM ledger ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 2);

    // IS NULL reaches exactly the row the other predicates withheld.
    exec(&ex, "DROP POLICY known_regions ON ledger").await;
    exec(
        &ex,
        "CREATE POLICY unset_only ON ledger FOR SELECT TO PUBLIC USING (region IS NULL)",
    )
    .await;
    let result = exec_session(&ex, sid, "SELECT id FROM ledger ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows(&result[0]).len(), 1);
    assert_eq!(rows(&result[0])[0][0], Value::Int32(4));
}

/// Extension-prefix commands (SHOW TABLE STATS, REFRESH MV, masking DDL, ...)
/// run on raw text before the parsed path's per-statement
/// `recompute_session_context` — a session whose authority was revoked kept
/// its stale context indefinitely on those commands. A demoted superuser's
/// session used `bypass_rls` from before the demotion to read planner stats
/// of an RLS table.
#[tokio::test]
async fn extension_commands_recompute_session_context() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE guarded_stats (id INT, ssn TEXT)").await;
    exec(&ex, "ALTER TABLE guarded_stats ENABLE ROW LEVEL SECURITY").await;
    exec(&ex, "CREATE ROLE boss LOGIN PASSWORD 'x' SUPERUSER").await;
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "boss").await.unwrap();
    // Prime the session context while boss still holds the attribute.
    exec_session(&ex, sid, "SELECT 1").await.unwrap();

    // Demote from the bootstrap session.
    exec(&ex, "ALTER ROLE boss NOSUPERUSER").await;

    // The extension-prefix arm must see the REVOKED authority, not the
    // context snapshotted while boss was a superuser.
    let res = exec_session(&ex, sid, "SHOW TABLE STATS guarded_stats").await;
    match res {
        Err(ExecError::PermissionDenied(msg)) => assert!(msg.contains("RLS-protected")),
        Err(other) => panic!("expected PermissionDenied, got: {other}"),
        Ok(v) => {
            panic!("stale superuser context leaked RLS-table stats via SHOW TABLE STATS: {v:?}")
        }
    }
}
