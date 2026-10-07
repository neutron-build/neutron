//! NE-03 / NE-04: policy publication must not revert concurrent policy DDL.
//!
//! NE-03 — the three-way merge at COMMIT compared a staged catalog cloned at
//! the session's FIRST policy write against a baseline cloned at BEGIN. Any
//! policy another session committed between those two moments was inside the
//! staged copy but absent from the baseline, so the merge classified the
//! PEER's entry as this transaction's delta and COMMIT reinstalled the peer's
//! outdated version over its later tightening (rename, drop, …).
//!
//! NE-04(a) — the merge itself cloned the live catalog under a read lock and
//! installed under a different write lock, with the merge between them holding
//! nothing. Two committing sessions could clone the same pre-publish state and
//! the second install silently discarded the first session's published delta.
//! Publication is now clone+merge+install under one write guard; the stress
//! test below is probabilistic on the old code and deterministic on the new.

use super::*;

fn policy_names(ex: &Executor) -> Vec<String> {
    let res = futures::executor::block_on(ex.execute("SELECT policyname FROM pg_policies"))
        .expect("pg_policies");
    rows(&res[0])
        .iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// NE-03: a peer's policy TIGHTENING (rename) between this transaction's BEGIN
/// and its first policy write must survive this transaction's COMMIT.
#[tokio::test]
async fn peer_tightening_between_begin_and_staging_survives_commit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT)").await;
    exec(&ex, "CREATE TABLE logs (id INT PRIMARY KEY)").await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();

    // Peer adds a policy while A's transaction is open.
    exec(
        &ex,
        "CREATE POLICY keep ON docs FOR SELECT TO PUBLIC USING (true)",
    )
    .await;

    // A stages an UNRELATED policy — this is where the staged catalog is
    // cloned (and, post-NE-03, the baseline with it).
    ex.execute_with_session(
        a,
        "CREATE POLICY unrelated ON logs FOR SELECT TO PUBLIC USING (true)",
    )
    .await
    .unwrap();

    // Peer tightens its policy after A staged. Pre-NE-03, A's COMMIT
    // reinstated the pre-rename `keep` because the BEGIN-era baseline lacked
    // the policy entirely, so the merge saw it as A's own delta.
    exec(&ex, "ALTER POLICY keep ON docs RENAME TO keep_tight").await;

    ex.execute_with_session(a, "COMMIT").await.unwrap();

    let names = policy_names(&ex);
    assert!(
        names.contains(&"keep_tight".to_string()) && !names.contains(&"keep".to_string()),
        "peer tightening lost at COMMIT: {names:?}"
    );
    assert!(
        names.contains(&"unrelated".to_string()),
        "this transaction's own policy lost: {names:?}"
    );
}

/// NE-03, drop variant: a peer's policy DROP after this transaction staged must
/// survive the COMMIT (the old baseline mismatch resurrected the policy).
#[tokio::test]
async fn peer_drop_between_begin_and_staging_survives_commit() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT)").await;
    exec(&ex, "CREATE TABLE logs (id INT PRIMARY KEY)").await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();

    exec(
        &ex,
        "CREATE POLICY doomed ON docs FOR SELECT TO PUBLIC USING (true)",
    )
    .await;
    ex.execute_with_session(
        a,
        "CREATE POLICY marker ON logs FOR SELECT TO PUBLIC USING (true)",
    )
    .await
    .unwrap();
    exec(&ex, "DROP POLICY doomed ON docs").await;

    ex.execute_with_session(a, "COMMIT").await.unwrap();

    let names = policy_names(&ex);
    assert!(
        !names.contains(&"doomed".to_string()),
        "peer's DROP was reverted by COMMIT: {names:?}"
    );
    assert!(
        names.contains(&"marker".to_string()),
        "this transaction's own policy lost: {names:?}"
    );
}

/// NE-04(a): two sessions committing DISJOINT policy edits concurrently must
/// both survive publication. The publication window (live clone → merge →
/// install) was unguarded between the locks, so two OS threads publishing at
/// once could clone the same pre-publish state and the second install
/// discarded the first delta. Probabilistic on the old code, deterministic on
/// the new one.
#[test]
fn concurrent_disjoint_policy_commits_both_survive() {
    use std::sync::{Arc, Barrier};

    let ex = test_executor();
    ex_sendable(&ex, "CREATE TABLE t1 (id INT PRIMARY KEY)");
    ex_sendable(&ex, "CREATE TABLE t2 (id INT PRIMARY KEY)");

    let rounds = 64u32;
    let ex = Arc::new(ex);
    for r in 0..rounds {
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for (ex_l, table, name) in [
            (ex.clone(), "t1", format!("p1_{r}")),
            (ex.clone(), "t2", format!("p2_{r}")),
        ] {
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let sid = ex_l.create_session();
                    ex_l.execute_with_session(sid, "BEGIN").await.unwrap();
                    ex_l.execute_with_session(
                        sid,
                        &format!(
                            "CREATE POLICY {name} ON {table} FOR SELECT TO PUBLIC USING (true)"
                        ),
                    )
                    .await
                    .unwrap();
                    barrier.wait();
                    ex_l.execute_with_session(sid, "COMMIT").await.unwrap();
                });
            }));
        }
        for h in handles {
            h.join().expect("policy commit thread");
        }
        // Both disjoint edits must be live immediately after the pair commits.
        let names = policy_names(&ex);
        assert!(
            names.contains(&format!("p1_{r}")),
            "round {r}: session 1's policy lost to concurrent publication: {names:?}"
        );
        assert!(
            names.contains(&format!("p2_{r}")),
            "round {r}: session 2's policy lost to concurrent publication: {names:?}"
        );
    }
}

/// `Executor::execute` from a non-async context via a fresh single-thread
/// runtime (the harness helper is async-only).
fn ex_sendable(ex: &Executor, sql: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(ex.execute(sql)).expect(sql);
}
