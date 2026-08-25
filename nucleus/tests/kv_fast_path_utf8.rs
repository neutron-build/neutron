//! The KV/OLTP fast path must not mojibake non-ASCII text (WIR-4).
//!
//! Both sides of the fast path mangle identically (`'café'` → `'cafÃ©'` on
//! store AND on lookup), so a fast-path-only round-trip looks green while
//! every stored value is corrupt. The regression is therefore cross-protocol:
//! store through one protocol, read through the other, assert byte-exact.

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

/// Store via the extended protocol (real parser, correct UTF-8), read back
/// via the simple-protocol fast path: pre-fix the mangled lookup key missed
/// and the answer was NULL.
#[tokio::test]
async fn fast_path_lookup_finds_non_ascii_key() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    let row = c
        .query_one("SELECT kv_set($1, $2)", &[&"café", &"1"])
        .await
        .expect("kv_set via extended protocol");
    assert_eq!(row.get::<_, String>(0), "OK");

    let rows = rows_of(
        &c.simple_query("SELECT kv_get('café')")
            .await
            .expect("fast-path kv_get"),
    );
    assert_eq!(
        rows,
        vec!["1".to_string()],
        "fast-path lookup of a non-ASCII key returned NULL — the key was \
         mojibaked by the fast-path parser"
    );
}

/// Store via the fast path (simple protocol), read back via the extended
/// protocol: pre-fix the STORED value was the mojibake.
#[tokio::test]
async fn fast_path_store_keeps_non_ascii_value_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let server = start(tmp.path()).await;
    let c = connect(server.port).await;

    c.simple_query("SELECT kv_set('name', 'Zoë')")
        .await
        .expect("fast-path kv_set");
    let row = c
        .query_one("SELECT kv_get($1)", &[&"name"])
        .await
        .expect("kv_get via extended protocol");
    let stored: String = row.get(0);
    assert_eq!(stored, "Zoë", "stored value was mojibaked by the fast path");

    c.simple_query("CREATE TABLE t (note TEXT)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES ('café')")
        .await
        .expect("fast-path INSERT");
    let row = c
        .query_one("SELECT note FROM t", &[])
        .await
        .expect("read back");
    let stored: String = row.get(0);
    assert_eq!(stored, "café", "fast-path INSERT stored mojibake");
}
