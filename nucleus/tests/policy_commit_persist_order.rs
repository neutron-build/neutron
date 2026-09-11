//! A7: policy publication and persistence are tied to the commit decision.
//!
//! The COMMIT path used to publish the staged policy catalog to shared state
//! and persist meta.json BEFORE the storage commit. Two consequences:
//!
//!  * a persistence failure (or a commit failure) restored the BEGIN-era
//!    whole-catalog snapshot — wiping OTHER sessions' policy DDL committed
//!    since this BEGIN, in memory and, via the re-persist, on disk;
//!  * a crash between persist and commit left DURABLE policy the transaction
//!    never committed (a rolled-back CREATE ROLE resurrected from meta.json
//!    on restart).
//!
//! Now the order is decide (storage commit) → publish in memory → persist,
//! and a persist failure un-publishes the pre-publish LIVE catalog (other
//! sessions' committed changes survive) and surfaces the error.
//!
//! This binary arms `NUCLEUS_IOFAULT=meta.write` in-process, so it must stay
//! a single test: the fault counter is process-global. The skip count lets
//! three meta.json saves pass (CREATE TABLE, ALTER TABLE ENABLE RLS, the
//! autocommit CREATE POLICY) and fails the fourth — the COMMIT-time save.

#![cfg(feature = "server")]

use nucleus::executor::open_persistent_executor;

async fn policy_names(ex: &nucleus::executor::Executor) -> Vec<String> {
    let mut r = ex
        .execute("SELECT policyname FROM pg_policies")
        .await
        .expect("pg_policies");
    match r.pop().expect("one result") {
        nucleus::executor::ExecResult::Select { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                nucleus::types::Value::Text(s) => s.clone(),
                other => format!("{other:?}"),
            })
            .collect(),
        other => panic!("expected SELECT, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_time_policy_persist_failure_never_wipes_or_leaks() {
    let tmp = tempfile::tempdir().unwrap();
    // SAFETY: this binary holds exactly one test; the variable is armed
    // before any database work, and nothing else reads the environment.
    unsafe {
        std::env::set_var("NUCLEUS_IOFAULT", "meta.write");
        std::env::set_var("NUCLEUS_IOFAULT_SKIP", "3");
    }

    let ex = open_persistent_executor(tmp.path()).await.expect("open");
    ex.execute("CREATE TABLE guarded (id INT PRIMARY KEY, owner TEXT)")
        .await
        .expect("create table");

    // An already-committed policy from another (autocommit) statement; the
    // failing COMMIT below must not wipe it — the old code restored the
    // BEGIN-era snapshot, which predated this policy, and re-persisted the
    // wipe.
    ex.execute("ALTER TABLE guarded ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable rls");
    ex.execute(
        "CREATE POLICY other_policy ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await
    .expect("committed policy");

    // The transaction stages its own policy plus a data row, then COMMITs:
    // the storage commit succeeds, publication happens, and the meta.json
    // save is the third arrival — the one the fault fires on.
    let sid = ex.create_session();
    ex.execute_with_session(sid, "BEGIN").await.expect("begin");
    ex.execute_with_session(
        sid,
        "CREATE POLICY txn_policy ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
    )
    .await
    .expect("staged policy");
    ex.execute_with_session(sid, "INSERT INTO guarded VALUES (1, 'alice')")
        .await
        .expect("data row");
    let failed = ex.execute_with_session(sid, "COMMIT").await;
    let err = failed.expect_err("the commit-time meta.json save must fail");
    assert!(
        err.to_string().contains("meta write"),
        "the failure must be the injected meta.json fault, got: {err}"
    );

    // The transaction's data committed (the commit decision preceded the
    // persist attempt) — the surfaced error is about durability, not data.
    let mut r = ex.execute("SELECT COUNT(*) FROM guarded").await.expect("count");
    match r.pop().unwrap() {
        nucleus::executor::ExecResult::Select { rows, .. } => assert_eq!(
            rows[0][0],
            nucleus::types::Value::Int64(1),
            "the committed data row must survive the persist failure"
        ),
        other => panic!("expected SELECT, got {other:?}"),
    }
    // The session is idle and usable after the failed COMMIT.
    ex.execute_with_session(sid, "SELECT 1").await.expect("idle");

    // In memory: the other session's policy survived, the transaction's own
    // staged policy was un-published with the error.
    assert_eq!(
        policy_names(&ex).await,
        vec!["other_policy".to_string()],
        "persist failure must un-publish only this transaction's policy"
    );

    // And durable: the same holds after a restart. meta.json's last
    // successful save is the autocommit policy — no durable policy the
    // transaction did not commit.
    ex.drop_session(sid);
    drop(ex);
    let reopened = open_persistent_executor(tmp.path()).await.expect("reopen");
    assert_eq!(
        policy_names(&reopened).await,
        vec!["other_policy".to_string()],
        "meta.json must not contain policy from a transaction whose persistence failed"
    );
}
