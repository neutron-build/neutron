#![cfg(feature = "server")]
//! N25 — a client that sends Terminate must get its socket closed.
//!
//! PostgreSQL answers Terminate ('X') with nothing at all: it closes the
//! connection, and that close is the only signal the client gets. asyncpg's
//! `Connection.close()` writes Terminate and then waits for `connection_lost`,
//! so a server that keeps the socket open leaves it waiting forever — measured
//! against a virgin connection that had run no queries: PostgreSQL 17 closed in
//! 0.001s, Nucleus never closed, and `NucleusClient.close()` in the Python SDK
//! hung with it. `terminate()` returned instantly only because it is a
//! unilateral socket close by the client.
//!
//! The cause was in the pgwire dependency, not in Nucleus: 0.36's message
//! dispatch ends in a `_ => {}` catch-all that swallows Terminate, and its
//! connection loop exits only on EOF. Nucleus therefore runs its own copy of
//! that loop (`wire::process_socket_closing_on_terminate`) until the dependency
//! reaches 0.40.1, where it is fixed upstream.
//!
//! This test drives a RAW SOCKET rather than a client library on purpose. Every
//! Postgres driver hides the close behind its own timeout and retry behaviour,
//! so a driver-level test can pass on a server that never closes — the driver
//! simply gives up and reports success. The observable contract is "the server
//! sends FIN", and only a raw socket can assert exactly that.

use std::sync::Arc;
use std::time::Duration;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer, process_socket_closing_on_terminate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start() -> u16 {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                break;
            };
            let srv = server.clone();
            tokio::spawn(async move {
                let _ =
                    process_socket_closing_on_terminate(s, None::<pgwire::tokio::TlsAcceptor>, srv)
                        .await;
            });
        }
    });
    port
}

/// Startup, then read until ReadyForQuery ('Z') so the connection is live.
async fn startup(sock: &mut TcpStream) {
    let params = b"user\0postgres\0database\0postgres\0\0";
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes()); // protocol 3.0
    body.extend_from_slice(params);
    let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    sock.write_all(&msg).await.unwrap();

    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf))
            .await
            .expect("timed out waiting for ReadyForQuery")
            .unwrap();
        assert!(n > 0, "server closed during startup");
        seen.extend_from_slice(&buf[..n]);
        // ReadyForQuery is 'Z' + int32 len(5) + one status byte.
        if seen.len() >= 6 && seen[seen.len() - 6] == b'Z' {
            return;
        }
    }
}

async fn assert_closes_after_terminate(port: u16, what: &str) {
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    startup(&mut sock).await;

    // Terminate: 'X' with a length of 4 and no payload.
    sock.write_all(b"X").await.unwrap();
    sock.write_all(&4i32.to_be_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let read = tokio::time::timeout(Duration::from_secs(10), sock.read(&mut buf)).await;
    match read {
        Ok(Ok(0)) => {}
        Ok(Ok(n)) => panic!("{what}: server sent {n} bytes after Terminate instead of closing"),
        Ok(Err(e)) => panic!("{what}: socket error after Terminate: {e}"),
        Err(_) => panic!(
            "{what}: the server did not close the socket within 10s of Terminate. A client \
             waiting for that close — which is what asyncpg's Connection.close() does — hangs \
             forever."
        ),
    }
}

/// The reported shape: a connection that ran nothing at all.
#[tokio::test]
async fn terminate_closes_a_connection_that_ran_no_queries() {
    let port = start().await;
    assert_closes_after_terminate(port, "virgin connection").await;
}

/// And one that did some work first, so the close does not depend on the
/// connection still being in its startup state.
#[tokio::test]
async fn terminate_closes_a_connection_that_ran_a_query() {
    let port = start().await;
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=u dbname=d"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let pump = tokio::spawn(async move {
        let _ = conn.await;
    });
    client.simple_query("SELECT 1").await.unwrap();
    drop(client);
    let _ = pump.await;

    assert_closes_after_terminate(port, "connection after a query").await;
}
