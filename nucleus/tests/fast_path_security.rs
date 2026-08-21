//! The wire OLTP fast path must enforce GRANTs and masking.
//!
//! This is a wire test because it cannot be anything else. The fast path is
//! reached only from `NucleusHandler`, and it used to be called with no session
//! scope at all -- so `current_session()` returned `default_session`, the
//! bootstrap superuser, and every in-path guard that consults the session was
//! dead code on this route. An in-process test calling the executor directly
//! passes against that bug.
//!
//! Verified against a running server before the fix: a genuinely authenticated
//! role holding NO privileges could SELECT, UPDATE and DELETE arbitrary rows
//! here while `has_table_privilege` answered false for the same table.
//!
//! Each statement is issued SEPARATELY. A multi-statement batch does not parse
//! as a fast path, so a test that bundles `SET ROLE` with the query silently
//! exercises the parsed path and proves nothing -- which is how this surface was
//! missed once already.

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

/// Rows a simple_query returned, flattened to "col|col" strings.
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

#[tokio::test]
async fn fast_path_enforces_grants_and_masking() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    c.simple_query("CREATE TABLE hr (id INT PRIMARY KEY, salary INT)")
        .await
        .expect("create hr");
    c.simple_query("INSERT INTO hr VALUES (1,100000),(2,250000)")
        .await
        .expect("seed hr");
    c.simple_query("CREATE TABLE people (id INT PRIMARY KEY, ssn TEXT)")
        .await
        .expect("create people");
    c.simple_query("INSERT INTO people VALUES (1,'123-45-6789')")
        .await
        .expect("seed people");
    c.simple_query("CREATE ROLE grace LOGIN PASSWORD 'g'")
        .await
        .expect("create grace");
    c.simple_query("CREATE ROLE hank LOGIN PASSWORD 'h'")
        .await
        .expect("create hank");
    c.simple_query("GRANT SELECT ON people TO hank")
        .await
        .expect("grant people");
    c.simple_query("CREATE MASKING POLICY ON people (ssn) TO hank USING REDACT '***'")
        .await
        .expect("masking policy");

    // (d) the superuser keeps every shape, so the gate did not simply disable
    // the fast path for everyone.
    let sup = connect(server.port).await;
    assert_eq!(
        rows_of(
            &sup.simple_query("SELECT * FROM hr WHERE id = 2")
                .await
                .unwrap()
        ),
        vec!["2|250000".to_string()],
        "a superuser point SELECT must still be served"
    );
    sup.simple_query("UPDATE hr SET salary = 250001 WHERE id = 2")
        .await
        .expect("superuser point UPDATE");
    sup.simple_query("INSERT INTO hr VALUES (3,1)")
        .await
        .expect("superuser insert");
    sup.simple_query("DELETE FROM hr WHERE id = 3")
        .await
        .expect("superuser delete");

    // (a) and (c): a role with NO grant is refused on every fast-path shape.
    let g = connect(server.port).await;
    g.simple_query("SET ROLE grace").await.expect("set role");
    for sql in [
        "SELECT * FROM hr WHERE id = 2",
        "UPDATE hr SET salary = 1 WHERE id = 2",
        "DELETE FROM hr WHERE id = 1",
        "INSERT INTO hr VALUES (9,9)",
    ] {
        let err = g
            .simple_query(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` must be refused for a role with no GRANT"));
        let msg = err
            .as_db_error()
            .map(|e| e.message().to_string())
            .unwrap_or_else(|| err.to_string());
        assert!(
            msg.contains("permission denied"),
            "`{sql}` should be permission denied, got: {msg}"
        );
    }

    // The refusals must also have changed nothing.
    assert_eq!(
        rows_of(
            &sup.simple_query("SELECT id, salary FROM hr ORDER BY id")
                .await
                .unwrap()
        ),
        vec!["1|100000".to_string(), "2|250001".to_string()],
        "a refused write must not have applied"
    );

    // (b) a GRANTed non-superuser reads, and the masking policy is applied.
    let h = connect(server.port).await;
    h.simple_query("SET ROLE hank").await.expect("set role");
    assert_eq!(
        rows_of(
            &h.simple_query("SELECT * FROM people WHERE id = 1")
                .await
                .unwrap()
        ),
        vec!["1|***".to_string()],
        "a granted role must read, and the masked column must be redacted"
    );
    assert_eq!(
        rows_of(
            &sup.simple_query("SELECT * FROM people WHERE id = 1")
                .await
                .unwrap()
        ),
        vec!["1|123-45-6789".to_string()],
        "the superuser must still see the real value"
    );
}
