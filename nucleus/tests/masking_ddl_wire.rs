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

/// SEC-2: masking DDL committed on one connection must reach a second
/// connection's reads, and must survive a restart. In-txn CREATE used to
/// stage into `security_pending` and COMMIT silently discarded it (masking
/// never set `policy_dirty`); autocommit CREATE mutated the live policy set
/// but never persisted, so a restart lost every mask.
#[tokio::test]
async fn masking_ddl_publishes_at_commit_and_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    {
        let server = start(tmp.path()).await;
        let admin = connect(server.port).await;
        admin
            .simple_query("CREATE TABLE people (id INT PRIMARY KEY, ssn TEXT)")
            .await
            .expect("create table");
        admin
            .simple_query("INSERT INTO people VALUES (1, '123-45-6789')")
            .await
            .expect("insert");
        admin
            .simple_query("CREATE ROLE analyst LOGIN PASSWORD 'p'")
            .await
            .expect("create role");
        admin
            .simple_query("GRANT SELECT ON people TO analyst")
            .await
            .expect("grant");

        // A second connection reads through the committed policy set.
        let reader = connect(server.port).await;
        reader
            .simple_query("SET ROLE analyst")
            .await
            .expect("set role");
        let raw = reader
            .simple_query("SELECT ssn FROM people")
            .await
            .expect("select");
        let value = raw
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .next()
            .expect("one row");
        assert_eq!(value, "123-45-6789", "control: unmasked baseline");

        // Transactional CREATE over the wire: staged on the DDL connection,
        // invisible to the reader until COMMIT.
        let ddl = connect(server.port).await;
        ddl.simple_query("BEGIN").await.expect("begin");
        ddl.simple_query("CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT '***'")
            .await
            .expect("in-txn CREATE MASKING POLICY");

        let pre = reader
            .simple_query("SELECT ssn FROM people")
            .await
            .expect("select pre-commit");
        let value = pre
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .next()
            .expect("one row");
        assert_eq!(
            value, "123-45-6789",
            "an uncommitted mask leaked to another connection"
        );

        ddl.simple_query("COMMIT").await.expect("commit");

        let post = reader
            .simple_query("SELECT ssn FROM people")
            .await
            .expect("select post-commit");
        let value = post
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .next()
            .expect("one row");
        assert_eq!(
            value, "***",
            "COMMIT did not publish the mask to other connections"
        );
    }

    // Restart on a crash-copy of the directory: the autocommit-persisted
    // policy must still be listed and still mask a non-superuser read.
    let crashed = tempfile::tempdir().unwrap();
    copy_dir(tmp.path(), crashed.path());
    {
        let server = start(crashed.path()).await;
        let reader = connect(server.port).await;
        reader
            .simple_query("SET ROLE analyst")
            .await
            .expect("set role");
        let got = reader
            .simple_query("SELECT ssn FROM people")
            .await
            .expect("select after restart");
        let value = got
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
                _ => None,
            })
            .next()
            .expect("one row");
        assert_eq!(
            value, "***",
            "the masking policy did not survive the restart"
        );
    }
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
