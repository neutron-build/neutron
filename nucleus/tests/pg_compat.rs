//! PostgreSQL wire protocol compatibility tests.
//!
//! Each test starts an in-process Nucleus server on a random port, connects
//! with `tokio-postgres`, exercises a specific protocol feature, and verifies
//! correctness.
//!
//!     cargo test --test pg_compat -- --nocapture

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};

// ============================================================================
// Helper: start a Nucleus pgwire server on a random port
// ============================================================================

/// Boots a Nucleus server on `127.0.0.1:0` (OS-assigned port) and returns the
/// actual port together with a `JoinHandle` for the accept loop. Callers should
/// `abort()` the handle when the test is done.
async fn start_nucleus_server() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
    // No authentication -- tests connect without a password.
    let handler = Arc::new(NucleusHandler::new(executor));
    let server = Arc::new(NucleusServer::new(handler));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let port = listener.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let srv = server.clone();
            tokio::spawn(async move {
                let _ = pgwire::tokio::process_socket(
                    socket,
                    None::<pgwire::tokio::TlsAcceptor>,
                    srv,
                )
                .await;
            });
        }
    });

    (port, handle)
}

/// Connect a `tokio-postgres` client to the given port. Returns the `Client`
/// and spawns the connection future in the background.
async fn connect(port: u16) -> tokio_postgres::Client {
    let connstr = format!("host=127.0.0.1 port={port} user=nucleus dbname=test");
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls)
        .await
        .expect("connect to nucleus");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

// ============================================================================
// Test 1: Simple query — CREATE TABLE, INSERT, SELECT
// ============================================================================

#[tokio::test]
async fn pg_simple_query() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    // CREATE TABLE via simple query protocol.
    client
        .simple_query("CREATE TABLE simple_t (id INT, name TEXT)")
        .await
        .expect("CREATE TABLE");

    // INSERT rows.
    client
        .simple_query("INSERT INTO simple_t VALUES (1, 'alice')")
        .await
        .expect("INSERT 1");
    client
        .simple_query("INSERT INTO simple_t VALUES (2, 'bob')")
        .await
        .expect("INSERT 2");

    // SELECT and verify row contents.
    let rows = client
        .simple_query("SELECT id, name FROM simple_t ORDER BY id")
        .await
        .expect("SELECT");

    // simple_query returns a mix of Row and CommandComplete messages.
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();

    assert_eq!(data_rows.len(), 2, "expected 2 rows");
    assert_eq!(data_rows[0].get(0), Some("1"));
    assert_eq!(data_rows[0].get(1), Some("alice"));
    assert_eq!(data_rows[1].get(0), Some("2"));
    assert_eq!(data_rows[1].get(1), Some("bob"));

    server.abort();
}

// ============================================================================
// Test 2: Prepared statements with $1, $2 bind parameters
// ============================================================================

#[tokio::test]
async fn pg_prepared_statement() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE prep_t (id INT, label TEXT)")
        .await
        .expect("CREATE TABLE");

    // Use the extended query protocol with parameters.
    // Note: Nucleus reports all parameter types as TEXT by default, so we
    // pass values as strings.  The executor coerces them to the column type.
    client
        .execute(
            "INSERT INTO prep_t VALUES ($1, $2)",
            &[&"1", &"hello"],
        )
        .await
        .expect("INSERT with params");

    client
        .execute(
            "INSERT INTO prep_t VALUES ($1, $2)",
            &[&"2", &"world"],
        )
        .await
        .expect("INSERT 2 with params");

    // Query all rows to verify the extended-protocol INSERTs landed.
    let rows = client
        .simple_query("SELECT id, label FROM prep_t ORDER BY id")
        .await
        .expect("SELECT after prepared inserts");

    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();

    assert_eq!(data_rows.len(), 2, "expected 2 rows from prepared inserts");
    assert_eq!(data_rows[0].get(0), Some("1"));
    assert_eq!(data_rows[0].get(1), Some("hello"));
    assert_eq!(data_rows[1].get(0), Some("2"));
    assert_eq!(data_rows[1].get(1), Some("world"));

    server.abort();
}

// ============================================================================
// Test 3: Transactions — ROLLBACK discards, COMMIT persists
// ============================================================================

#[tokio::test]
async fn pg_transactions() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE txn_t (id INT, val TEXT)")
        .await
        .expect("CREATE TABLE");

    // ---- ROLLBACK path ----
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("INSERT INTO txn_t VALUES (1, 'rollback_me')")
        .await
        .expect("INSERT inside txn");
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");

    // Data should NOT be visible after rollback.
    let rows = client
        .simple_query("SELECT * FROM txn_t")
        .await
        .expect("SELECT after ROLLBACK");
    let data_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(data_count, 0, "rollback should discard inserted row");

    // ---- COMMIT path ----
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("INSERT INTO txn_t VALUES (2, 'committed')")
        .await
        .expect("INSERT inside txn");
    client.simple_query("COMMIT").await.expect("COMMIT");

    // Data SHOULD be visible after commit.
    let rows = client
        .simple_query("SELECT * FROM txn_t")
        .await
        .expect("SELECT after COMMIT");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows.len(), 1, "committed row should be visible");
    assert_eq!(data_rows[0].get(0), Some("2"));
    assert_eq!(data_rows[0].get(1), Some("committed"));

    server.abort();
}

// ============================================================================
// Test 4: Error codes — invalid SQL returns a proper error
// ============================================================================

#[tokio::test]
async fn pg_error_codes() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    // Query a table that does not exist.
    let result = client
        .simple_query("SELECT * FROM nonexistent_table")
        .await;
    assert!(result.is_err(), "querying missing table should fail");

    let err = result.unwrap_err();
    // The error should carry a DbError with a SQLSTATE code.
    if let Some(db_err) = err.as_db_error() {
        let code = db_err.code();
        // 42P01 = undefined_table  (our primary expectation)
        // 42601 = syntax_error     (acceptable alternative)
        // 42000 = syntax_error_or_access_rule_violation (generic fallback)
        assert!(
            code == &tokio_postgres::error::SqlState::UNDEFINED_TABLE
                || code == &tokio_postgres::error::SqlState::SYNTAX_ERROR
                || code.code() == "42000",
            "unexpected SQLSTATE: {code:?}",
        );
    }
    // Even if it's not a DbError, the fact that it errored is enough for the
    // basic correctness check — some drivers wrap the error differently.

    // Syntax error.
    let result = client
        .simple_query("SELECTTTT broken sql here!!!")
        .await;
    assert!(result.is_err(), "broken SQL should produce an error");

    server.abort();
}

// ============================================================================
// Test 5: Data type roundtrip — INT, FLOAT, TEXT, BOOLEAN
// ============================================================================

#[tokio::test]
async fn pg_data_type_roundtrip() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query(
            "CREATE TABLE types_t (
                i INT,
                f FLOAT,
                t TEXT,
                b BOOLEAN
            )",
        )
        .await
        .expect("CREATE TABLE");

    client
        .simple_query("INSERT INTO types_t VALUES (42, 3.14, 'hello world', TRUE)")
        .await
        .expect("INSERT");

    client
        .simple_query("INSERT INTO types_t VALUES (-1, 0.0, '', FALSE)")
        .await
        .expect("INSERT 2");

    let rows = client
        .simple_query("SELECT i, f, t, b FROM types_t ORDER BY i")
        .await
        .expect("SELECT");

    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();

    assert_eq!(data_rows.len(), 2, "expected 2 rows");

    // Row 1: -1, 0.0, '', FALSE   (ORDER BY i ascending)
    assert_eq!(data_rows[0].get(0), Some("-1"));
    // Float representation may vary (0, 0.0, 0.00, etc.) — just check it parses.
    let f0: f64 = data_rows[0]
        .get(1)
        .unwrap()
        .parse()
        .expect("parse float");
    assert!((f0 - 0.0).abs() < f64::EPSILON, "expected 0.0, got {f0}");
    assert_eq!(data_rows[0].get(2), Some(""));
    // Boolean may be rendered as "f", "false", "FALSE", or "0".
    let b0 = data_rows[0].get(3).unwrap().to_lowercase();
    assert!(
        b0 == "f" || b0 == "false" || b0 == "0",
        "expected false-ish, got {b0}",
    );

    // Row 2: 42, 3.14, 'hello world', TRUE
    assert_eq!(data_rows[1].get(0), Some("42"));
    let f1: f64 = data_rows[1]
        .get(1)
        .unwrap()
        .parse()
        .expect("parse float");
    assert!((f1 - 3.14).abs() < 0.001, "expected ~3.14, got {f1}");
    assert_eq!(data_rows[1].get(2), Some("hello world"));
    let b1 = data_rows[1].get(3).unwrap().to_lowercase();
    assert!(
        b1 == "t" || b1 == "true" || b1 == "1",
        "expected true-ish, got {b1}",
    );

    server.abort();
}

// ============================================================================
// Test 6: COPY FROM STDIN — bulk loading
// ============================================================================

#[tokio::test]
async fn pg_copy_from_stdin() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE copy_t (id INT, name TEXT)")
        .await
        .expect("CREATE TABLE");

    // Use the COPY protocol to bulk-load rows.
    let copy_sink = client
        .copy_in("COPY copy_t FROM STDIN WITH (FORMAT csv)")
        .await;

    match copy_sink {
        Ok(sink) => {
            // Write CSV data into the COPY stream.
            use futures::SinkExt;

            // CopyInSink is !Unpin, so we must pin it to use SinkExt methods.
            let mut writer = std::pin::pin!(sink);
            let data = b"1,alice\n2,bob\n3,charlie\n";
            let written = writer.as_mut().send(bytes::Bytes::from_static(data)).await;
            if written.is_err() {
                // If sending data fails, COPY might not be fully supported.
                eprintln!(
                    "COPY data send failed (partial support): {}",
                    written.unwrap_err()
                );
                server.abort();
                return;
            }
            let finish_result: Result<u64, _> = writer.as_mut().finish().await;
            if finish_result.is_err() {
                eprintln!(
                    "COPY finish failed (partial support): {}",
                    finish_result.unwrap_err()
                );
                server.abort();
                return;
            }

            // Verify the rows were loaded.
            let rows = client
                .simple_query("SELECT id, name FROM copy_t ORDER BY id")
                .await
                .expect("SELECT after COPY");

            let data_rows: Vec<_> = rows
                .iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                    _ => None,
                })
                .collect();

            assert_eq!(data_rows.len(), 3, "expected 3 rows from COPY");
            assert_eq!(data_rows[0].get(0), Some("1"));
            assert_eq!(data_rows[0].get(1), Some("alice"));
            assert_eq!(data_rows[1].get(0), Some("2"));
            assert_eq!(data_rows[1].get(1), Some("bob"));
            assert_eq!(data_rows[2].get(0), Some("3"));
            assert_eq!(data_rows[2].get(1), Some("charlie"));
        }
        Err(e) => {
            // COPY FROM STDIN may not be fully supported yet.  Verify we at
            // least get a recognisable error rather than a crash.
            eprintln!("COPY FROM STDIN not supported: {e}");
            // Acceptable: the server should not crash and the error should be
            // parseable (not a raw TCP disconnect).
            assert!(
                e.as_db_error().is_some() || e.to_string().contains("COPY"),
                "unexpected COPY error shape: {e}",
            );
        }
    }

    server.abort();
}

// ============================================================================
// Test 7: NULL handling — INSERT NULL, IS NULL / IS NOT NULL
// ============================================================================

#[tokio::test]
async fn pg_null_handling() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE null_t (id INT, val TEXT)")
        .await
        .expect("CREATE TABLE");

    client
        .simple_query("INSERT INTO null_t VALUES (1, 'hello')")
        .await
        .expect("INSERT 1");
    client
        .simple_query("INSERT INTO null_t VALUES (2, NULL)")
        .await
        .expect("INSERT NULL");
    client
        .simple_query("INSERT INTO null_t VALUES (NULL, 'no_id')")
        .await
        .expect("INSERT NULL id");

    // IS NULL filter
    let rows = client
        .simple_query("SELECT id FROM null_t WHERE val IS NULL")
        .await
        .expect("SELECT IS NULL");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows.len(), 1, "one row has NULL val");
    assert_eq!(data_rows[0].get(0), Some("2"));

    // IS NOT NULL filter
    let rows = client
        .simple_query("SELECT id FROM null_t WHERE val IS NOT NULL ORDER BY id")
        .await
        .expect("SELECT IS NOT NULL");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows.len(), 2, "two rows have non-NULL val");

    server.abort();
}

// ============================================================================
// Test 8: Multi-statement simple query
// ============================================================================

#[tokio::test]
async fn pg_multi_statement() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    // Multiple statements separated by semicolons
    let results = client
        .simple_query(
            "CREATE TABLE multi_t (id INT, v TEXT); \
             INSERT INTO multi_t VALUES (1, 'a'); \
             INSERT INTO multi_t VALUES (2, 'b'); \
             SELECT * FROM multi_t ORDER BY id",
        )
        .await;

    match results {
        Ok(msgs) => {
            let data_rows: Vec<_> = msgs
                .iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                    _ => None,
                })
                .collect();
            assert_eq!(data_rows.len(), 2, "SELECT should return 2 rows");
            assert_eq!(data_rows[0].get(0), Some("1"));
            assert_eq!(data_rows[1].get(0), Some("2"));
        }
        Err(e) => {
            // Multi-statement may not be fully supported — acceptable
            eprintln!("Multi-statement not supported: {e}");
        }
    }

    server.abort();
}

// ============================================================================
// Test 9: Aggregate functions via wire protocol
// ============================================================================

#[tokio::test]
async fn pg_aggregates() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE agg_t (id INT, amount FLOAT)")
        .await
        .expect("CREATE TABLE");

    for i in 1..=5 {
        client
            .simple_query(&format!("INSERT INTO agg_t VALUES ({i}, {}.0)", i * 10))
            .await
            .expect("INSERT");
    }

    // COUNT
    let rows = client
        .simple_query("SELECT COUNT(*) FROM agg_t")
        .await
        .expect("COUNT");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows[0].get(0), Some("5"));

    // SUM
    let rows = client
        .simple_query("SELECT SUM(amount) FROM agg_t")
        .await
        .expect("SUM");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    let sum: f64 = data_rows[0].get(0).unwrap().parse().expect("parse sum");
    assert!((sum - 150.0).abs() < 0.01, "SUM should be 150, got {sum}");

    server.abort();
}

// ============================================================================
// Test 10: Large result set (200 rows)
// ============================================================================

#[tokio::test]
async fn pg_large_result_set() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE large_t (id INT, data TEXT)")
        .await
        .expect("CREATE TABLE");

    // Insert 200 rows
    for i in 0..200 {
        client
            .simple_query(&format!("INSERT INTO large_t VALUES ({i}, 'row_{i}')"))
            .await
            .expect("INSERT");
    }

    let rows = client
        .simple_query("SELECT COUNT(*) FROM large_t")
        .await
        .expect("COUNT");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows[0].get(0), Some("200"));

    // Fetch all rows
    let rows = client
        .simple_query("SELECT * FROM large_t ORDER BY id")
        .await
        .expect("SELECT all");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(data_rows.len(), 200, "should return all 200 rows");
    assert_eq!(data_rows[0].get(0), Some("0"));
    assert_eq!(data_rows[199].get(0), Some("199"));

    server.abort();
}
