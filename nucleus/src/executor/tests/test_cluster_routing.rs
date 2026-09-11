//! Cluster routing must cover BOTH execution entries (audit A14) and fail
//! closed when routing cannot produce a replicated commit (audit A15).
//!
//! A14: `execute_statements_with_session` — the extended-protocol AST fast
//! path every real driver takes — used to call `execute_statement` directly,
//! so every gate only the text path applied (follower forward, leader Raft
//! propose, security-DDL refusal, follower-read freshness) was skipped for
//! the protocol that matters.
//!
//! A15: on a follower with no leader, on a leader whose proposal fails, and
//! with no replicator, DML used to fall through to local execution — an
//! acked write no other node will ever apply.

use super::*;

/// A multi-raft cluster coordinator that believes itself a FOLLOWER of shard
/// 0 with NO leader elected (fresh group, no election has run).
fn follower_cluster() -> Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>> {
    Arc::new(parking_lot::RwLock::new(
        crate::distributed::ClusterCoordinator::new_multi_raft(
            0x2,
            vec![(0x1, "127.0.0.1:19001".into())],
        ),
    ))
}

/// A multi-raft coordinator whose shard-0 group has elected THIS node leader.
fn leader_cluster() -> Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>> {
    let cluster = Arc::new(parking_lot::RwLock::new(
        crate::distributed::ClusterCoordinator::new_multi_raft(
            0x1,
            vec![(0x2, "127.0.0.1:19002".into())],
        ),
    ));
    {
        let mut coord = cluster.write();
        let mgr = coord.raft_manager_mut().unwrap();
        mgr.create_group(0, vec![0x1, 0x2]);
        let group = mgr.get_group_mut(0).unwrap();
        group.start_election(0x1);
        assert!(
            group.receive_vote(0x2, true),
            "fixture: node 0x1 must win the election"
        );
    }
    cluster
}

fn executor_with(
    cluster: Arc<parking_lot::RwLock<crate::distributed::ClusterCoordinator>>,
) -> Executor {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MemoryEngine::new());
    Executor::new(catalog, storage).with_cluster(cluster)
}

/// A configured follower refuses security DDL IDENTICALLY through the text
/// entry and the extended-protocol AST entry (audit A14's acceptance: the
/// AST entry used to execute it locally).
#[tokio::test]
async fn follower_refuses_security_ddl_from_text_and_ast_entries() {
    let ex = executor_with(follower_cluster());
    let sess = ex.create_session();

    let sql = "CREATE ROLE sneak LOGIN PASSWORD 'x'";
    let text_err = ex
        .execute_with_session(sess, sql)
        .await
        .expect_err("text entry: follower must refuse security DDL");
    assert!(
        text_err.to_string().contains("cluster leader"),
        "text entry refusal must name the leader requirement: {text_err}"
    );

    let ast = crate::sql::parse(sql).unwrap();
    let ast_err = ex
        .execute_statements_with_session(sess, ast, None)
        .await
        .expect_err("AST entry: follower must refuse security DDL identically");
    assert_eq!(
        text_err.to_string(),
        ast_err.to_string(),
        "both entries must hit the same routing gate"
    );
}

/// The AST entry routes DML too: on a follower with no leader the INSERT is
/// refused — never executed locally (A14 makes the AST entry reach the A15
/// gate; A15 makes that gate fail closed).
#[tokio::test]
async fn ast_entry_routes_dml_through_the_cluster_gate() {
    let ex = executor_with(follower_cluster());
    ex.execute("CREATE TABLE gated (id INT)").await.unwrap();
    let sess = ex.create_session();

    let sql = "INSERT INTO gated VALUES (1)";
    let ast = crate::sql::parse(sql).unwrap();
    let err = ex
        .execute_statements_with_session(sess, ast, None)
        .await
        .expect_err("AST entry must not execute DML locally on a follower");
    assert!(
        err.to_string().contains("no cluster leader"),
        "expected the no-leader refusal, got: {err}"
    );

    let r = ex.execute("SELECT COUNT(*) FROM gated").await.unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Int64(0),
        "a refused INSERT must not change rows"
    );
}

/// A15: follower + no known leader -> refuse DML; rows unchanged. (Pre-A15
/// this fell through to local execution and acked.)
#[tokio::test]
async fn follower_without_a_leader_refuses_dml() {
    let ex = executor_with(follower_cluster());
    ex.execute("CREATE TABLE a15 (id INT)").await.unwrap();

    let err = ex
        .execute("INSERT INTO a15 VALUES (1)")
        .await
        .expect_err("a follower with no leader must refuse DML");
    assert!(
        err.to_string().contains("no cluster leader"),
        "unexpected refusal: {err}"
    );

    let r = ex.execute("SELECT COUNT(*) FROM a15").await.unwrap();
    assert_eq!(scalar(&r[0]), &Value::Int64(0), "rows must be unchanged");
}

/// A15: leader + no replicator -> refuse DML. The coordinator claims
/// leadership but nothing can carry the write to a quorum.
#[tokio::test]
async fn leader_without_a_replicator_refuses_dml() {
    let ex = executor_with(leader_cluster());
    ex.execute("CREATE TABLE a15b (id INT)").await.unwrap();

    let err = ex
        .execute("INSERT INTO a15b VALUES (1)")
        .await
        .expect_err("a leader with no replicator must refuse DML");
    assert!(
        err.to_string().contains("no Raft replicator"),
        "unexpected refusal: {err}"
    );

    let r = ex.execute("SELECT COUNT(*) FROM a15b").await.unwrap();
    assert_eq!(scalar(&r[0]), &Value::Int64(0), "rows must be unchanged");
}

/// A15: a leader-side step-down (the coordinator still believes it leads,
/// the replicator knows it does not) must never ack local-only success.
/// `propose_and_await` on a non-leader replicator fails fast with NotLeader.
#[tokio::test]
async fn stepped_down_leader_never_acks_local_only_dml() {
    use crate::distributed::RaftReplicator;
    use crate::transport::TcpTransport;

    let ex = executor_with(leader_cluster());
    ex.execute("CREATE TABLE a15c (id INT)").await.unwrap();

    // A fresh replicator is a follower; the coordinator above still says
    // leader — exactly the between-views window of a step-down.
    let transport = Arc::new(TcpTransport::new(0x1, "127.0.0.1:0"));
    let (replicator, _apply_rx) =
        RaftReplicator::new(0x1, vec![(0x2, "127.0.0.1:19002".into())], transport);
    ex.set_raft_replicator(Arc::new(replicator));

    let err = ex
        .execute("INSERT INTO a15c VALUES (1)")
        .await
        .expect_err("a stepped-down leader must refuse DML instead of acking local-only success");
    assert!(
        err.to_string().contains("Raft proposal failed"),
        "unexpected refusal: {err}"
    );

    let r = ex.execute("SELECT COUNT(*) FROM a15c").await.unwrap();
    assert_eq!(
        scalar(&r[0]),
        &Value::Int64(0),
        "no local-only write may survive a refused proposal"
    );
}

/// Standalone mode keeps its fast path: no cluster configured, DML executes
/// locally exactly as before (A15 must not touch it).
#[tokio::test]
async fn standalone_dml_is_untouched() {
    let ex = test_executor();
    ex.execute("CREATE TABLE plain (id INT)").await.unwrap();
    ex.execute("INSERT INTO plain VALUES (1)").await.unwrap();
    let r = ex.execute("SELECT COUNT(*) FROM plain").await.unwrap();
    assert_eq!(scalar(&r[0]), &Value::Int64(1));
}
