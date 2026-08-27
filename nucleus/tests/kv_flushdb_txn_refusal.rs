//! D3 (S63, task-plan Batch 2 decision 9): KV_FLUSHDB refuses inside an
//! explicit transaction, over the real pgwire path.
//!
//! FLUSHDB's WAL effect is an empty snapshot — committed by construction and
//! untaggable — so an in-transaction flush launders the wipe past the S6
//! discard filter even when the transaction rolls back. The refusal is the
//! ratified fix; the wipe stays legal in autocommit, where no rollback exists
//! to contradict it.
//!
//! This file reproduces the hole end-to-end before the fix: `BEGIN;
//! SELECT KV_FLUSHDB()` returned OK and destroyed keys a later ROLLBACK could
//! not bring back.

#![cfg(feature = "server")]

use std::path::PathBuf;
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

async fn start(data_dir: &std::path::Path) -> Server {
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

async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    let messages = client.simple_query(sql).await.expect(sql);
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            return row.get(0).map(|v| v.to_string());
        }
    }
    None
}

fn tmpdir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nucleus-d3-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// In a transaction the flush must be REFUSED with an error that names the
/// transaction, and the keyspace must survive the ROLLBACK intact.
#[tokio::test]
async fn kv_flushdb_refuses_inside_explicit_transaction_and_keeps_keys() {
    let dir = tmpdir("refuse");
    let server = start(&dir).await;
    let client = connect(server.port).await;

    scalar(&client, "SELECT KV_SET('d3:a', 'one')").await;
    scalar(&client, "SELECT KV_SET('d3:b', 'two')").await;
    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:a')").await.as_deref(),
        Some("one")
    );

    client.simple_query("BEGIN").await.expect("BEGIN");

    // Pre-fix: this returned OK and wiped both keys.
    let err = client
        .simple_query("SELECT KV_FLUSHDB()")
        .await
        .expect_err("KV_FLUSHDB inside an explicit transaction must error");
    let msg = err
        .as_db_error()
        .map(|d| d.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.to_lowercase().contains("transaction"),
        "the refusal must name the transaction, got: {msg}"
    );

    client.simple_query("ROLLBACK").await.expect("ROLLBACK");

    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:a')").await.as_deref(),
        Some("one"),
        "the refused flush must not have wiped d3:a"
    );
    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:b')").await.as_deref(),
        Some("two"),
        "the refused flush must not have wiped d3:b"
    );
}

/// Outside a transaction the flush keeps its whole contract: OK now, empty
/// keyspace after.
#[tokio::test]
async fn kv_flushdb_still_works_in_autocommit() {
    let dir = tmpdir("autocommit");
    let server = start(&dir).await;
    let client = connect(server.port).await;

    scalar(&client, "SELECT KV_SET('d3:c', 'three')").await;
    assert_eq!(
        scalar(&client, "SELECT KV_FLUSHDB()").await.as_deref(),
        Some("OK"),
        "autocommit flush must succeed"
    );
    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:c')").await,
        None,
        "autocommit flush must empty the keyspace"
    );
}

/// A refused in-txn flush aborts the transaction (standard 25P02 semantics,
/// same as Postgres: any error inside a transaction block poisons it), and a
/// ROLLBACK must leave the session and the store fully usable — no half
/// enlistment, no wiped keyspace.
#[tokio::test]
async fn kv_flushdb_refusal_leaves_the_session_usable_after_rollback() {
    let dir = tmpdir("usable");
    let server = start(&dir).await;
    let client = connect(server.port).await;

    scalar(&client, "SELECT KV_SET('d3:e', 'before')").await;
    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:e')").await.as_deref(),
        Some("before")
    );

    client.simple_query("BEGIN").await.expect("BEGIN");
    let err = client.simple_query("SELECT KV_FLUSHDB()").await;
    assert!(err.is_err(), "the flush must refuse in-txn");
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");

    // The store survived the refused flush and the session serves writes.
    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:e')").await.as_deref(),
        Some("before"),
        "the refused flush must not have wiped the keyspace"
    );
    scalar(&client, "SELECT KV_SET('d3:f', 'after')").await;
    assert_eq!(
        scalar(&client, "SELECT KV_GET('d3:f')").await.as_deref(),
        Some("after"),
        "the session must serve writes after the refused flush"
    );
}
