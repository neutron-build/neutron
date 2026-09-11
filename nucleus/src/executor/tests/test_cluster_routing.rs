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
