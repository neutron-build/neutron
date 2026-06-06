#![cfg(feature = "server")]
//! Regression for observe remediation finding #1: a binary-format (extended
//! protocol) result for an integer column must be encoded at the column's
//! DECLARED width, not the stored variant's. Nucleus stores small values as
//! Int32 even in BIGINT columns; before the fix, a BIGINT column holding such a
//! value emitted a 4-byte int4 payload under an int8 RowDescription, so pgx /
//! tokio-postgres failed with "error deserializing column".
//!
//! Also pins finding #2: DELETE is visible to a subsequent read on the same
//! connection (it was reported as a possible gap; it is not).

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn start() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let h = tokio::spawn(async move {
        loop {
            let Ok((s, _)) = listener.accept().await else {
                break;
            };
            let srv = server.clone();
            tokio::spawn(async move {
                let _ =
                    pgwire::tokio::process_socket(s, None::<pgwire::tokio::TlsAcceptor>, srv).await;
            });
        }
    });
    (port, h)
}
async fn connect(port: u16) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=u dbname=d"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

#[tokio::test]
async fn binary_int_results_decode() {
    let (port, server) = start().await;
    let c = connect(port).await;
    // big holds values that fit in i32 (stored as Int32) under a BIGINT column,
    // and one that does NOT fit in i32 (stored as Int64).
    c.simple_query("CREATE TABLE m (version INT, big BIGINT, name TEXT)")
        .await
        .unwrap();
    c.simple_query("INSERT INTO m VALUES (1, 100, 'a'), (2, 5000000000, 'b')")
        .await
        .unwrap();

    // Extended protocol → binary result format for int4/int8.
    let rows = c
        .query("SELECT version, big, name FROM m ORDER BY version", &[])
        .await
        .unwrap();
    let v0: i32 = rows[0].get(0);
    let b0: i64 = rows[0].get(1);
    let n0: &str = rows[0].get(2);
    let b1: i64 = rows[1].get(1);
    assert_eq!(v0, 1);
    assert_eq!(
        b0, 100,
        "BIGINT column holding an i32-fitting value must decode as int8"
    );
    assert_eq!(n0, "a");
    assert_eq!(b1, 5_000_000_000, "BIGINT value beyond i32 must decode too");

    // Prepared statement (extended/binary) with an int parameter.
    let stmt = c
        .prepare("SELECT version, big FROM m WHERE version = $1")
        .await
        .unwrap();
    let pr = c.query(&stmt, &[&2i32]).await.unwrap();
    let pv: i32 = pr[0].get(0);
    let pb: i64 = pr[0].get(1);
    assert_eq!((pv, pb), (2, 5_000_000_000));

    server.abort();
}

#[tokio::test]
async fn delete_visible_same_session() {
    let (port, server) = start().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE share_links (token TEXT, site_id TEXT, created_at BIGINT)")
        .await
        .unwrap();
    c.simple_query("INSERT INTO share_links (token, site_id, created_at) VALUES ('t','s',0)")
        .await
        .unwrap();
    c.simple_query("DELETE FROM share_links WHERE token = 't'")
        .await
        .unwrap();
    let rows = c
        .query("SELECT site_id FROM share_links WHERE token = $1", &[&"t"])
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        0,
        "DELETE must be visible to a subsequent same-session read"
    );
    server.abort();
}
