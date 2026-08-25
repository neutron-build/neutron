//! Describe must never execute the original statement, and its probe must
//! run as the describing client's session (WIR-6).
//!
//! Pre-fix, `describe_select_columns` fell back to executing the FULL query
//! whenever the `LIMIT 0` probe failed to parse — which happens for any
//! statement that already ends in its own LIMIT — and the probe ran as
//! session 0, the bootstrap superuser. So a restricted client's Describe of
//! `SELECT * FROM hr LIMIT 1` leaked the table's columns (the fallback
//! executed the original as superuser), and a side-effecting projection
//! fired once at Describe time and again at Execute.
//!
//! This is a wire test because both halves only exist on the extended
//! protocol path: Parse + Describe + Execute against a real server.

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

fn rows_of(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

/// A restricted client's Describe must not fall back to executing the
/// original statement as the bootstrap superuser: the probe
/// (`... LIMIT 1 LIMIT 0`) fails to parse, and pre-fix the fallback then
/// ran `SELECT * FROM hr LIMIT 1` on session 0 — succeeding, and handing
/// the unprivileged client the table's column list.
#[tokio::test]
async fn describe_probe_failure_must_not_execute_as_superuser() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let sup = connect(server.port).await;

    sup.simple_query("CREATE TABLE hr (id INT, ssn TEXT)")
        .await
        .expect("create hr");
    sup.simple_query("CREATE ROLE mallory LOGIN PASSWORD 'm'")
        .await
        .expect("create mallory");

    let restricted = connect(server.port).await;
    restricted.simple_query("SET ROLE mallory").await.unwrap();

    // Parse + Describe of a statement the LIMIT-0 probe cannot parse. No
    // column metadata may come back: pre-fix the superuser fallback leaked
    // both columns to a role with no grants.
    let stmt = restricted
        .prepare("SELECT * FROM hr LIMIT 1")
        .await
        .expect("prepare");
    assert_eq!(
        stmt.columns().len(),
        0,
        "Describe handed column metadata to a role with no SELECT — the \
         probe fallback executed the query as the bootstrap superuser"
    );

    // Control: the superuser still gets full metadata when the probe can
    // run (no trailing LIMIT).
    let stmt = sup.prepare("SELECT * FROM hr").await.expect("prepare");
    assert_eq!(stmt.columns().len(), 2);
}

/// A Describe of a side-effecting projection must not execute it — even
/// when the side-effect registry's textual scan misses the call shape. A
/// comment between the function name and its argument list defeats the
/// identifier-immediately-followed-by-`(` scan, so pre-fix the LIMIT-0
/// probe failed to parse and the fallback EXECUTED the statement at
/// Describe time: the counter burned a value before the client's Execute
/// ever ran, and the execution ran as session 0 rather than the client.
#[tokio::test]
async fn describe_never_executes_side_effecting_statements() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    let stmt = c
        .prepare("SELECT kv_incr /* not a side effect? */ ('w6ctr') LIMIT 1")
        .await
        .expect("prepare");

    let rows = rows_of(&c.simple_query("SELECT kv_get('w6ctr')").await.unwrap());
    assert_eq!(
        rows,
        vec!["".to_string()],
        "something executed at Describe time — the counter must still be empty"
    );

    c.execute(&stmt, &[]).await.expect("execute");
    let rows = rows_of(&c.simple_query("SELECT kv_get('w6ctr')").await.unwrap());
    assert_eq!(
        rows,
        vec!["1".to_string()],
        "exactly one execution must have happened, at Execute"
    );
}
