#![cfg(feature = "server")]
//! A parameterized call to a read-only extension function must Describe its
//! result columns, and must type its integer arguments.
//!
//! Two defects, both of which made real SDK methods fail over pgwire and
//! neither of which a literal-argument test can see.
//!
//! **Zero-column Describe.** `describe_static_fields` only answers for
//! functions in the return-type registry; everything else falls through to a
//! probe that substitutes the placeholders and executes. With a bound
//! parameter that probe fails INSIDE the function — an unbound placeholder is
//! not an id, a key or a timestamp — so Describe reported zero columns while
//! Execute returned one. asyncpg enforces that strictly: "the number of
//! columns in the result row (1) is different from what was described (0)".
//! Measured against a live server, 18 query strings taken verbatim from the
//! Go/Python/Rust SDKs described zero columns, so `Graph.Neighbors`,
//! `Graph.ShortestPath`, `CDC.Read`, `TimeSeries.RangeCount`/`RangeAvg`,
//! `Streams.XRange`/`XRead`, `Blob.Get`/`Meta`, `Datalog.Query` and the KV
//! range reads had never worked from Python at all — the same never-worked
//! class as `Document.get`, and found the same way.
//!
//! **Untyped integer arguments.** A parameter the server cannot infer is
//! described as TEXT, and every SDK passes NATIVE INTEGERS into these
//! positions (`fromID int64`, `int(node_id)`, `&after_sequence`), so a strict
//! driver refuses to bind before a byte is sent.
//!
//! The literal-argument form works in both cases, which is exactly what kept
//! this hidden: the probe can execute when the arguments are constants, so
//! every psql check and every test that inlines its values passes.
//!
//! The DOC_* family is deliberately absent from the typing half. Those SDKs
//! send ids as text on purpose, as a documented workaround for this gap, so
//! typing them is a coordinated breaking change rather than a fix.

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use tokio_postgres::types::Type;

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
                    pgwire::tokio::process_socket(s, None::<pgwire::tokio::TlsAcceptor>, srv).await;
            });
        }
    });
    port
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

/// Every one of these is a query string lifted from an SDK.
const SDK_CALLS: &[&str] = &[
    "SELECT BLOB_GET($1)",
    "SELECT BLOB_META($1)",
    "SELECT CDC_READ($1, $2)",
    "SELECT CDC_TABLE_READ($1, $2, $3)",
    "SELECT DATALOG_QUERY($1)",
    "SELECT GEO_AREA($1)",
    "SELECT GRAPH_NEIGHBORS($1, $2)",
    "SELECT GRAPH_QUERY($1)",
    "SELECT GRAPH_SHORTEST_PATH($1, $2)",
    "SELECT KV_LINDEX($1, $2)",
    "SELECT KV_LRANGE($1, $2, $3)",
    "SELECT KV_ZRANGE($1, $2, $3)",
    "SELECT KV_ZRANGEBYSCORE($1, $2, $3)",
    "SELECT STREAM_XRANGE($1, $2, $3, $4)",
    "SELECT STREAM_XREAD($1, $2, $3)",
    "SELECT TIME_BUCKET($1, $2)",
    "SELECT TS_RANGE_AVG($1, $2, $3)",
    "SELECT TS_RANGE_COUNT($1, $2, $3)",
];

#[tokio::test]
async fn parameterized_scalar_calls_describe_their_result_column() {
    let port = start().await;
    let c = connect(port).await;

    let mut zero = Vec::new();
    for sql in SDK_CALLS {
        let stmt = c
            .prepare(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        if stmt.columns().is_empty() {
            zero.push(*sql);
        }
    }
    assert!(
        zero.is_empty(),
        "these describe zero result columns, so a strict client fails at execute \
         with a column-count mismatch: {zero:#?}"
    );
}

/// The integer positions have to be typed, or a driver refuses to bind a
/// native integer — which is what every SDK passes.
#[tokio::test]
async fn integer_arguments_are_typed_not_text() {
    let port = start().await;
    let c = connect(port).await;

    // (sql, zero-based positions that must be an integer type)
    let cases: &[(&str, &[usize])] = &[
        ("SELECT GRAPH_NEIGHBORS($1, $2)", &[0]),
        ("SELECT GRAPH_SHORTEST_PATH($1, $2)", &[0, 1]),
        ("SELECT GRAPH_ADD_EDGE($1, $2, $3)", &[0, 1]),
        ("SELECT CDC_READ($1, $2)", &[0, 1]),
        ("SELECT CDC_TABLE_READ($1, $2, $3)", &[1, 2]),
        ("SELECT KV_EXPIRE($1, $2)", &[1]),
        ("SELECT TS_RANGE_COUNT($1, $2, $3)", &[1, 2]),
        ("SELECT TS_RANGE_AVG($1, $2, $3)", &[1, 2]),
        ("SELECT STREAM_XRANGE($1, $2, $3, $4)", &[1, 2, 3]),
        ("SELECT STREAM_XREAD($1, $2, $3)", &[1, 2]),
    ];

    for (sql, int_positions) in cases {
        let stmt = c
            .prepare(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let params = stmt.params();
        for &pos in *int_positions {
            assert_eq!(
                params[pos],
                Type::INT8,
                "{sql}: parameter ${} is described as {:?}; every SDK passes a native \
                 integer here, so a strict driver refuses to bind it",
                pos + 1,
                params[pos]
            );
        }
    }
}

/// Binding native integers has to actually work end to end, not merely
/// describe well.
#[tokio::test]
async fn native_integer_parameters_execute() {
    let port = start().await;
    let c = connect(port).await;
    c.simple_query("SELECT GRAPH_ADD_NODE('person', '{}')")
        .await
        .unwrap();

    c.query("SELECT GRAPH_NEIGHBORS($1, $2)", &[&1i64, &"out"])
        .await
        .expect("GRAPH_NEIGHBORS with a native integer id");
    c.query("SELECT CDC_READ($1, $2)", &[&0i64, &10i64])
        .await
        .expect("CDC_READ with native integers");
    c.query("SELECT TS_RANGE_COUNT($1, $2, $3)", &[&"m", &0i64, &99i64])
        .await
        .expect("TS_RANGE_COUNT with native integers");
    c.query("SELECT KV_EXPIRE($1, $2)", &[&"k", &30i64])
        .await
        .expect("KV_EXPIRE with a native integer ttl");
}

/// The document family is deliberately still TEXT. If this starts failing,
/// someone typed DOC_* — which is a coordinated breaking change across four
/// SDKs, not a bug fix. See N24 in OPEN_WORK before "fixing" it.
#[tokio::test]
async fn document_ids_are_still_described_as_text_on_purpose() {
    let port = start().await;
    let c = connect(port).await;

    for sql in ["SELECT DOC_GET($1)", "SELECT DOC_GET($1, $2)"] {
        let stmt = c.prepare(sql).await.unwrap();
        for (i, ty) in stmt.params().iter().enumerate() {
            assert_eq!(
                *ty,
                Type::TEXT,
                "{sql}: parameter ${} became {ty:?}. The Python, Go, Rust and TypeScript \
                 clients all send document ids as TEXT deliberately; changing this breaks \
                 them and has to ship with matching SDK releases.",
                i + 1
            );
        }
    }
}
