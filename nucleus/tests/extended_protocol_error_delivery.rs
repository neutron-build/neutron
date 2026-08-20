//! An execution error must reach an extended-protocol client.
//!
//! Reported by a Studio dispatch on 2026-08-19: "extended-protocol execution
//! errors never reach pgx clients (simple protocol and psql `\bind` are fine),
//! so every pgx-default client sees success, 0 rows". If true that is severe —
//! `tokio-postgres`, `pgx`, asyncpg and every ORM built on them use the
//! extended protocol by default, so a failed write would look like a successful
//! one.
//!
//! This is the check, not the claim.

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
    let executor = open_persistent_executor(data_dir).await.expect("executor");
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

/// `client.execute` is the extended protocol: Parse, Bind, Execute, Sync.
/// A constraint violation there must come back as an error.
#[tokio::test]
async fn an_execution_error_reaches_an_extended_protocol_client() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    c.simple_query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (1, 'first')")
        .await
        .expect("seed");

    // Duplicate primary key: parses fine, fails at execution.
    let err = c
        .execute("INSERT INTO t VALUES (1, 'duplicate')", &[])
        .await
        .expect_err(
            "a duplicate primary key must be reported to an extended-protocol \
             client, not swallowed into a successful 0-row result",
        );
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.to_lowercase().contains("unique")
            || message.to_lowercase().contains("duplicate")
            || message.to_lowercase().contains("primary key"),
        "the error reached the client but does not say what happened: {message}"
    );

    // And the row was not written.
    let rows = c
        .simple_query("SELECT v FROM t WHERE id = 1")
        .await
        .expect("read back");
    let values: Vec<String> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["first".to_string()]);
}

/// The same error through the SIMPLE protocol, as the control: if this one
/// fails too, the defect is not protocol-specific.
#[tokio::test]
async fn an_execution_error_reaches_a_simple_protocol_client() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    c.simple_query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (1, 'first')")
        .await
        .expect("seed");
    c.simple_query("INSERT INTO t VALUES (1, 'duplicate')")
        .await
        .expect_err("a duplicate primary key must be reported over simple query too");
}

/// A SELECT that fails at execution — not at parse — must also report.
#[tokio::test]
async fn a_failing_query_reaches_an_extended_protocol_client() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    let err = c
        .query("SELECT * FROM no_such_table", &[])
        .await
        .expect_err("a missing table must be reported to an extended-protocol client");
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.to_lowercase().contains("no_such_table")
            || message.to_lowercase().contains("does not exist")
            || message.to_lowercase().contains("not found"),
        "unhelpful error text: {message}"
    );
}

/// The specific shape the dispatch reported: an **RLS denial** through the
/// extended protocol.
///
/// The guard that refuses specialty-store functions while row-level security is
/// active returns `PermissionDenied`, and a client that receives "success, 0
/// rows" instead cannot tell a denial from an empty result. That is worse than
/// a loud failure: the application takes the empty answer as data.
#[tokio::test]
async fn an_rls_denial_reaches_an_extended_protocol_client() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    c.simple_query("CREATE TABLE t (id INT PRIMARY KEY, owner TEXT)")
        .await
        .expect("create");
    c.simple_query("CREATE ROLE reader LOGIN PASSWORD 'p'")
        .await
        .expect("role");
    c.simple_query("SELECT KV_SET('k', 'v')")
        .await
        .expect("kv write before any policy exists");
    c.simple_query("CREATE POLICY p ON t FOR SELECT TO reader USING (owner = 'ada')")
        .await
        .expect("policy");
    c.simple_query("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable rls");

    // The connection authenticates as `nucleus`, a superuser, for whom the
    // guard is deliberately inactive — so this must still WORK. A test that
    // asserted a denial here would be asserting the wrong thing.
    c.query("SELECT KV_GET($1)", &[&"k"])
        .await
        .expect("a superuser is exempt from the specialty guard");

    // Now the reported shape itself: a NON-superuser session. The connection
    // authenticates as the bootstrap superuser, for whom the guard is inactive,
    // so `SET ROLE` is how a wire client reaches the denial path without SCRAM
    // configured.
    c.simple_query("GRANT SELECT ON t TO reader")
        .await
        .expect("grant");
    c.simple_query("SET ROLE reader").await.expect("set role");
    let err = c.query("SELECT KV_GET($1)", &[&"k"]).await.expect_err(
        "an RLS denial must reach an extended-protocol client, not arrive as \
             a successful empty result — this is the reported shape",
    );
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.to_lowercase().contains("row-level security")
            || message.to_lowercase().contains("unavailable"),
        "the denial reached the client without its reason: {message}"
    );
    // Schema-qualified: `pg_catalog.kv_get(...)` must be denied too. A
    // dispatch reported this as a live bypass; the strip that makes the guard
    // see the canonical name was moved BEFORE the policy decision when the same
    // hole was found in 2026-07, and this is the pin that it stayed fixed.
    let err = c
        .query("SELECT pg_catalog.kv_get($1)", &[&"k"])
        .await
        .expect_err("a schema-qualified specialty call must be denied under RLS too");
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.to_lowercase().contains("row-level security")
            || message.to_lowercase().contains("unavailable"),
        "`pg_catalog.` qualification bypassed the specialty guard: {message}"
    );

    c.simple_query("RESET ROLE").await.expect("reset role");

    // And one that applies to everyone: a specialty call inside an explicit
    // transaction that cannot be rolled back.
    c.simple_query("BEGIN").await.expect("begin");
    let err = c
        .execute("SELECT KV_HSET('h', 'f', 'v')", &[])
        .await
        .expect_err(
            "a refusal must reach an extended-protocol client, not arrive as a \
             successful empty result",
        );
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.to_lowercase().contains("rollback"),
        "the refusal reached the client without its reason: {message}"
    );
    let _ = c.simple_query("ROLLBACK").await;
}
