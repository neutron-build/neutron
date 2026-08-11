#![cfg(feature = "server")]
//! Binary-format parameters of TEXT/VARCHAR/JSONB must survive the wire.
//!
//! The parameter decoder had an unknown-OID catch-all that guessed an integer
//! from the payload LENGTH — 2 bytes an int2, 4 an int4, 8 an int8. TEXT (25),
//! VARCHAR (1043) and JSONB (3802) all fell into it, and Nucleus advertises
//! VARCHAR for every TEXT column and TEXT as its inference default. So any
//! string of exactly 2, 4 or 8 bytes, bound in binary format, was silently
//! stored as a number:
//!
//! ```text
//! 'ab'       -> 24930
//! 'abcd'     -> 1633837924
//! 'abcdefgh' -> 7017280452245743464
//! ```
//!
//! No error was raised at any point. `WHERE s = $1` then matched nothing for
//! those lengths, and `UPDATE ... SET email = $1` overwrote the column with the
//! integer. Binary JSONB never worked at all: its encoding is a one-byte
//! version header followed by the JSON text, and the header was passed straight
//! through into the parser.
//!
//! This is the default path for mainstream drivers — asyncpg (which the Python
//! SDK depends on), tokio-postgres, sqlx and Postgrex all bind parameters in
//! binary. It was not caught because `pg_compat.rs` binds no scalar `&str`
//! parameter anywhere: its parameterized tests bind `i32`, `i64` and one
//! `Vec<String>`, and the array path had its own decoder.
//!
//! Lengths 2, 4 and 8 are the whole point of this test. A test that binds
//! "hello" passes against the bug.

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

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

/// `query`/`execute` use the extended protocol, which binds parameters in
/// binary — the path that corrupted them.
#[tokio::test]
async fn binary_string_params_round_trip_at_every_length() {
    let port = start().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE t (id INT, s TEXT)")
        .await
        .unwrap();

    // 2, 4 and 8 are the lengths the length-guessing catch-all claimed; the
    // others are controls that passed even with the bug.
    let cases = ["a", "ab", "abc", "abcd", "hello", "abcdefgh", "deadbeef"];
    for (i, v) in cases.iter().enumerate() {
        c.execute("INSERT INTO t VALUES ($1, $2)", &[&(i as i32), v])
            .await
            .unwrap();
    }

    for (i, v) in cases.iter().enumerate() {
        let row = c
            .query_one("SELECT s FROM t WHERE id = $1", &[&(i as i32)])
            .await
            .unwrap();
        let got: &str = row.get(0);
        assert_eq!(
            got,
            *v,
            "a {}-byte string bound in binary format came back as {got:?} instead of {v:?}",
            v.len()
        );
    }
}

/// The corruption was not confined to what was written: a lookup BY such a
/// parameter silently matched nothing, so a row that exists reads as absent.
#[tokio::test]
async fn a_lookup_by_an_eight_byte_string_param_finds_its_row() {
    let port = start().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE t (id INT, s TEXT)")
        .await
        .unwrap();
    c.simple_query("INSERT INTO t VALUES (1, 'abcdefgh')")
        .await
        .unwrap();

    let rows = c
        .query("SELECT id FROM t WHERE s = $1", &[&"abcdefgh"])
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "a row whose value is 'abcdefgh' was not found when looked up by an \
         8-byte binary parameter — the parameter was decoded as an integer"
    );
}

/// And an UPDATE bound this way rewrote the column with the integer.
#[tokio::test]
async fn an_update_with_a_string_param_does_not_rewrite_the_column_as_a_number() {
    let port = start().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE u (id INT, email TEXT)")
        .await
        .unwrap();
    c.simple_query("INSERT INTO u VALUES (1, 'before@x.io')")
        .await
        .unwrap();

    c.execute(
        "UPDATE u SET email = $1 WHERE id = $2",
        &[&"chg@y.io", &1i32],
    )
    .await
    .unwrap();

    let row = c
        .query_one("SELECT email FROM u WHERE id = 1", &[])
        .await
        .unwrap();
    let got: &str = row.get(0);
    assert_eq!(got, "chg@y.io", "the UPDATE stored {got:?}");
}

/// JSONB in both directions: the parameter carries a version header that must
/// be stripped, and the result must carry one that clients expect.
#[tokio::test]
async fn binary_jsonb_params_and_results_round_trip() {
    let port = start().await;
    let c = connect(port).await;
    c.simple_query("CREATE TABLE j (id INT, doc JSONB)")
        .await
        .unwrap();

    // Binary JSONB in: the driver prepends the version header, which the server
    // has to strip. A short document is deliberate — `{"a":1}` is 7 bytes, so
    // with the header it is 8, which the old catch-all read as an int8.
    let small = serde_json::json!({"a": 1});
    let bigger = serde_json::json!({"hello": "world", "n": [1, 2, 3]});
    c.execute("INSERT INTO j VALUES (1, $1)", &[&small])
        .await
        .unwrap();
    c.execute("INSERT INTO j VALUES (2, $1)", &[&bigger])
        .await
        .unwrap();

    // Binary JSONB out: `query` takes the extended protocol, so the server must
    // emit the version header the client is about to strip.
    let rows = c.query("SELECT doc FROM j ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2, "expected two JSONB rows");
    let got0: serde_json::Value = rows[0].get(0);
    let got1: serde_json::Value = rows[1].get(0);
    assert_eq!(got0, small, "JSONB round-trip changed the document");
    assert_eq!(got1, bigger, "JSONB round-trip changed the document");

    // And the text format still returns the JSON alone, with no header leaking
    // into it.
    let text_rows = c
        .simple_query("SELECT doc FROM j ORDER BY id")
        .await
        .unwrap();
    let first = text_rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .unwrap();
    assert!(
        first.starts_with('{'),
        "text-format JSONB began with {first:?} — a binary version header leaked into it"
    );
}
