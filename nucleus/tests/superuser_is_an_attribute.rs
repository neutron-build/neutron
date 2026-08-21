//! Authority must come from the SUPERUSER attribute, never from a role's NAME.
//!
//! Enforcement used to read the session's role NAME set (`has_role("superuser")`)
//! while every administrative and introspection surface read the role ATTRIBUTE.
//! So any role literally named "superuser" -- creatable by a security admin,
//! importable from a migrated role catalog, or reachable through GRANT
//! membership -- conferred full RLS, masking and privilege bypass on every
//! member, while `pg_roles.rolsuper` reported false for them.
//!
//! Two halves are tested here: the name is now reserved, and the bootstrap and
//! real-attribute superusers still work (the flip would otherwise have stripped
//! authority from single-user mode, which is the failure this test exists to
//! catch).

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

#[tokio::test]
async fn superuser_is_an_attribute_not_a_name() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    // The bootstrap session must still be authoritative. If flipping enforcement
    // to the attribute had stripped the default session's bypass, single-user
    // mode would break -- everything below this line would still pass, so this
    // assertion goes first.
    c.simple_query("CREATE TABLE secrets (id INT PRIMARY KEY, v TEXT)")
        .await
        .expect("bootstrap session must be able to create a table");
    c.simple_query("INSERT INTO secrets VALUES (1,'plaintext')")
        .await
        .expect("bootstrap insert");
    assert_eq!(
        rows_of(
            &c.simple_query("SELECT * FROM secrets WHERE id = 1")
                .await
                .unwrap()
        ),
        vec!["1|plaintext".to_string()],
        "the bootstrap identity must still read its own data"
    );

    // The name is reserved.
    let err = c
        .simple_query("CREATE ROLE superuser LOGIN PASSWORD 'x'")
        .await
        .expect_err("CREATE ROLE superuser must be refused");
    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("reserved"),
        "should say the name is reserved, got: {msg}"
    );

    // Case-insensitively, since role names compare that way.
    assert!(
        c.simple_query("CREATE ROLE SuperUser LOGIN PASSWORD 'x'")
            .await
            .is_err(),
        "the reservation must be case-insensitive"
    );

    // A name that merely resembles it is fine -- the guard must not over-reach.
    c.simple_query("CREATE ROLE supervisor LOGIN PASSWORD 'x'")
        .await
        .expect("CREATE ROLE supervisor must still succeed");

    // A role holding the real attribute still bypasses.
    c.simple_query("CREATE ROLE admin2 SUPERUSER LOGIN PASSWORD 'x'")
        .await
        .expect("create attribute superuser");
    let a = connect(server.port).await;
    a.simple_query("SET ROLE admin2").await.expect("set role");
    assert_eq!(
        rows_of(
            &a.simple_query("SELECT * FROM secrets WHERE id = 1")
                .await
                .unwrap()
        ),
        vec!["1|plaintext".to_string()],
        "a role with the SUPERUSER attribute must still bypass"
    );

    // A role without it does not, even though it is a perfectly ordinary role.
    let s = connect(server.port).await;
    s.simple_query("SET ROLE supervisor")
        .await
        .expect("set role");
    let err = s
        .simple_query("SELECT * FROM secrets WHERE id = 1")
        .await
        .expect_err("a non-superuser with no GRANT must be refused");
    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("permission denied"),
        "expected permission denied, got: {msg}"
    );
}
