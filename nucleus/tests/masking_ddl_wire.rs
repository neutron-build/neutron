//! `CREATE MASKING POLICY` must work over pgwire, which is the gate.
//!
//! In-process tests call `Executor::execute`, and that is not the same path a
//! client takes. Two things could go wrong only over the wire, and both have
//! bitten this codebase before: the extended query protocol Parses and
//! Describes a statement before executing it, and a non-standard statement has
//! no `sqlparser` AST to describe; and a `SELECT`-shaped extension has to
//! return real column metadata or a client refuses the row.
//!
//! So this drives the real server with tokio-postgres, in both the simple and
//! the extended protocol.

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

#[tokio::test]
async fn masking_ddl_works_over_the_wire() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    c.simple_query("CREATE TABLE people (id INT PRIMARY KEY, ssn TEXT)")
        .await
        .expect("create table");
    c.simple_query("CREATE ROLE analyst LOGIN PASSWORD 'p'")
        .await
        .expect("create role");

    // Simple query protocol.
    c.simple_query("CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT '***'")
        .await
        .expect("CREATE MASKING POLICY over simple query");

    // SHOW must return its rows with usable column metadata.
    let rows = c
        .simple_query("SHOW MASKING POLICIES")
        .await
        .expect("SHOW MASKING POLICIES");
    let listed: Vec<_> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(listed.len(), 1, "SHOW returned no policy over the wire");
    assert_eq!(listed[0].get(0), Some("people"));
    assert_eq!(listed[0].get(1), Some("ssn"));
    assert_eq!(listed[0].get(2), Some("analyst"));
    assert_eq!(listed[0].get(3), Some("REDACT '***'"));

    // Extended query protocol: Parse + Describe + Bind + Execute. This is what
    // every driver except psql actually sends.
    c.execute("DROP MASKING POLICY ON people (ssn) TO analyst", &[])
        .await
        .expect("DROP MASKING POLICY over the extended protocol");

    let rows = c
        .simple_query("SHOW MASKING POLICIES")
        .await
        .expect("SHOW after drop");
    let remaining = rows
        .into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(remaining, 0, "the drop did not take effect over the wire");

    // And a refusal must arrive as an error, not as a silent success.
    let err = c
        .simple_query("CREATE MASKING POLICY ON people (nope) TO analyst USING HASH")
        .await
        .expect_err("a policy on a missing column must be refused over the wire");
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.contains("does not exist"),
        "the refusal must reach the client with its reason; got: {message}"
    );
}
