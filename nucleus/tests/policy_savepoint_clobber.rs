//! TXN-1: ROLLBACK TO SAVEPOINT used to re-stage the savepoint-time security
//! catalog unconditionally and force `policy_dirty`, so a transaction that
//! never ran policy DDL entered COMMIT holding a BEGIN-era copy of the whole
//! policy catalog — which COMMIT then wrote over the live catalog and
//! persisted, erasing other sessions' committed policy DDL from memory and
//! from meta.json.
//!
//! Drives a real server: the clobber is wire-reachable from any two clients
//! interleaving a savepoint-only transaction with policy DDL.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use nucleus::executor::open_persistent_executor;
use nucleus::wire::{NucleusHandler, NucleusServer};

struct Server {
    port: u16,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn start(data_dir: &Path) -> Server {
    let executor = open_persistent_executor(data_dir)
        .await
        .expect("open persistent executor");
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler.clone()));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    let accept = tokio::spawn(async move {
        loop {
            let Ok((socket, peer)) = listener.accept().await else {
                break;
            };
            let srv = server.clone();
            let cleanup = handler.clone();
            let peer_addr = peer.to_string();
            tokio::spawn(async move {
                let _ =
                    pgwire::tokio::process_socket(socket, None::<pgwire::tokio::TlsAcceptor>, srv)
                        .await;
                cleanup.cleanup_session(&peer_addr);
            });
        }
    });
    Server { port, accept }
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let connstr = format!("host=127.0.0.1 port={port} user=nucleus dbname=test");
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn policy_names(c: &tokio_postgres::Client) -> Vec<String> {
    let rows = c
        .query(
            "SELECT policyname FROM pg_policies ORDER BY policyname",
            &[],
        )
        .await
        .expect("pg_policies");
    rows.into_iter().map(|r| r.get::<_, String>(0)).collect()
}

/// The reported scenario: A's savepoint-only transaction must not clobber B's
/// committed policy DDL — in memory (enforcement) and on disk (restart).
#[tokio::test]
async fn rollback_to_savepoint_does_not_clobber_committed_policy() {
    let tmp = tempfile::tempdir().unwrap();

    {
        let server = start(tmp.path()).await;
        let admin = connect(server.port).await;
        admin
            .simple_query("CREATE TABLE guarded (id INT PRIMARY KEY, owner TEXT)")
            .await
            .expect("create table");
        admin
            .simple_query("INSERT INTO guarded VALUES (1, 'alice'), (2, 'bob')")
            .await
            .expect("insert");
        admin
            .simple_query("CREATE ROLE alice LOGIN PASSWORD 'p'")
            .await
            .expect("create role");
        admin
            .simple_query("GRANT SELECT ON guarded TO alice")
            .await
            .expect("grant");

        // Session A: a transaction that never touches policy DDL.
        let a = connect(server.port).await;
        a.simple_query("BEGIN").await.expect("begin");
        a.simple_query("SAVEPOINT s").await.expect("savepoint");
        a.simple_query("INSERT INTO guarded VALUES (3, 'alice')")
            .await
            .expect("A's own DML after the savepoint");

        // Session B commits policy DDL while A is open.
        let b = connect(server.port).await;
        b.simple_query("ALTER TABLE guarded ENABLE ROW LEVEL SECURITY")
            .await
            .expect("enable rls");
        b.simple_query(
            "CREATE POLICY owner_read ON guarded TO PUBLIC USING (owner = CURRENT_USER)",
        )
        .await
        .expect("create policy");

        // A rolls to the savepoint and commits — zero policy DDL of its own.
        a.simple_query("ROLLBACK TO SAVEPOINT s")
            .await
            .expect("rollback to");
        a.simple_query("COMMIT").await.expect("commit");

        // B's policy must survive A's COMMIT: still catalogued...
        let names = policy_names(&b).await;
        assert_eq!(
            names,
            vec!["owner_read".to_string()],
            "A's savepoint-only COMMIT erased B's committed policy"
        );
        // ...and still enforced for a fresh reader session.
        let reader = connect(server.port).await;
        reader
            .simple_query("SET ROLE alice")
            .await
            .expect("set role");
        let rows = reader
            .simple_query("SELECT id FROM guarded ORDER BY id")
            .await
            .expect("select");
        let ids: Vec<String> = rows
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        assert_eq!(
            ids,
            vec!["1"],
            "RLS enforcement lost with the policy: got {ids:?}"
        );
    }

    // And on disk: restart on a crash-copy must still hold B's policy.
    let crashed = tempfile::tempdir().unwrap();
    copy_dir(tmp.path(), crashed.path());
    {
        let server = start(crashed.path()).await;
        let c = connect(server.port).await;
        let names = policy_names(&c).await;
        assert_eq!(
            names,
            vec!["owner_read".to_string()],
            "A's COMMIT persisted the clobber to meta.json"
        );
    }
}

/// Controls for the pair-restore: policy DDL before the savepoint survives a
/// rollback-to; DDL after the savepoint is reverted by it; DDL after the
/// rollback-to is kept.
#[tokio::test]
async fn rollback_to_savepoint_restores_the_policy_pair_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let admin = connect(server.port).await;
    admin
        .simple_query("CREATE TABLE guarded (id INT PRIMARY KEY, owner TEXT)")
        .await
        .expect("create table");
    admin
        .simple_query("INSERT INTO guarded VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("insert");

    // Control 1: DDL before the savepoint is kept across ROLLBACK TO (only
    // post-savepoint work reverts); COMMIT publishes it.
    let a = connect(server.port).await;
    a.simple_query("BEGIN").await.expect("begin");
    a.simple_query("ALTER TABLE guarded ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable rls");
    a.simple_query("CREATE POLICY before_sp ON guarded TO PUBLIC USING (owner = CURRENT_USER)")
        .await
        .expect("create before_sp");
    a.simple_query("SAVEPOINT s").await.expect("savepoint");
    a.simple_query("ROLLBACK TO SAVEPOINT s")
        .await
        .expect("rollback to");
    a.simple_query("COMMIT").await.expect("commit");
    assert_eq!(
        policy_names(&admin).await,
        vec!["before_sp".to_string()],
        "DDL before the savepoint must survive ROLLBACK TO and COMMIT"
    );
    // Clean slate for control 2.
    admin
        .simple_query("DROP POLICY before_sp ON guarded")
        .await
        .expect("drop before_sp");

    // Control 2: DDL after the savepoint is reverted; DDL after the
    // rollback-to survives.
    let a2 = connect(server.port).await;
    a2.simple_query("BEGIN").await.expect("begin");
    a2.simple_query("CREATE POLICY keep_me ON guarded TO PUBLIC USING (owner = CURRENT_USER)")
        .await
        .expect("create keep_me");
    a2.simple_query("SAVEPOINT s2").await.expect("savepoint");
    a2.simple_query("CREATE POLICY discard_me ON guarded TO PUBLIC USING (owner = CURRENT_USER)")
        .await
        .expect("create discard_me");
    a2.simple_query("ROLLBACK TO SAVEPOINT s2")
        .await
        .expect("rollback to");
    a2.simple_query("CREATE POLICY after_sp ON guarded TO PUBLIC USING (owner = CURRENT_USER)")
        .await
        .expect("create after_sp");
    a2.simple_query("COMMIT").await.expect("commit");
    assert_eq!(
        policy_names(&admin).await,
        vec!["after_sp".to_string(), "keep_me".to_string()],
        "pair-restore must keep the pre-savepoint and post-rollback DDL, and drop the in-window one"
    );
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create dst");
    for entry in std::fs::read_dir(src).expect("read src") {
        let entry = entry.expect("entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("type").is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).expect("copy file");
        }
    }
}
