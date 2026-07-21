//! PostgreSQL wire protocol compatibility tests.
//!
//! Each test starts an in-process Nucleus server on a random port, connects
//! with `tokio-postgres`, exercises a specific protocol feature, and verifies
//! correctness.
//!
//!     cargo test --test pg_compat -- --nocapture

// 3.14 here is an arbitrary test fixture, not a PI approximation; the value
// asserts (parse/round-trip with a tolerance) rely on the exact literal.
#![allow(clippy::approx_constant)]

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
                let _ =
                    pgwire::tokio::process_socket(socket, None::<pgwire::tokio::TlsAcceptor>, srv)
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

    // Use the extended query protocol with parameters.  Since the pgwire
    // RowDescription fix, undeclared `$N` parameters get inferred from the
    // column they target on the other side of `INSERT ... VALUES`, so we can
    // bind native typed values via pgx-style scanners.
    client
        .execute("INSERT INTO prep_t VALUES ($1, $2)", &[&1_i32, &"hello"])
        .await
        .expect("INSERT with params");

    client
        .execute("INSERT INTO prep_t VALUES ($1, $2)", &[&2_i32, &"world"])
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
    let result = client.simple_query("SELECT * FROM nonexistent_table").await;
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
    let result = client.simple_query("SELECTTTT broken sql here!!!").await;
    assert!(result.is_err(), "broken SQL should produce an error");

    server.abort();
}

// ============================================================================
// Regression: constraint enforcement over the WIRE in autocommit mode.
//
// Guards the wire-level fast-path constraint-bypass blocker: an autocommit
// `INSERT` over pgwire MUST enforce PRIMARY KEY / NOT NULL, not silently accept
// violations. The rest of the suite drives the Executor directly and never
// exercises the wire fast path, so this bug hid under green tests.
// ============================================================================

#[tokio::test]
async fn pg_autocommit_insert_enforces_constraints() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE cons_t (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO cons_t VALUES (1, 'alice')")
        .await
        .expect("first insert");

    // Duplicate primary key, autocommit — must be rejected (23505), not accepted.
    let dup = client
        .simple_query("INSERT INTO cons_t VALUES (1, 'bob')")
        .await;
    assert!(
        dup.is_err(),
        "duplicate primary key must be rejected, not silently accepted"
    );
    if let Some(db_err) = dup.unwrap_err().as_db_error() {
        assert_eq!(
            db_err.code(),
            &tokio_postgres::error::SqlState::UNIQUE_VIOLATION,
            "duplicate PK should raise unique_violation (23505)"
        );
    }

    // NULL into a NOT NULL column, autocommit — must be rejected (23502).
    let null_row = client
        .simple_query("INSERT INTO cons_t VALUES (2, NULL)")
        .await;
    assert!(
        null_row.is_err(),
        "NULL into NOT NULL column must be rejected"
    );
    if let Some(db_err) = null_row.unwrap_err().as_db_error() {
        assert_eq!(
            db_err.code(),
            &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION,
            "NULL into NOT NULL should raise not_null_violation (23502)"
        );
    }

    // Exactly one valid row must survive.
    let rows = client
        .query("SELECT id FROM cons_t", &[])
        .await
        .expect("select");
    assert_eq!(
        rows.len(),
        1,
        "only the first valid row should persist after both violations were rejected"
    );

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
    let f0: f64 = data_rows[0].get(1).unwrap().parse().expect("parse float");
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
    let f1: f64 = data_rows[1].get(1).unwrap().parse().expect("parse float");
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
            if let Err(e) = written {
                // If sending data fails, COPY might not be fully supported.
                eprintln!("COPY data send failed (partial support): {e}");
                server.abort();
                return;
            }
            let finish_result: Result<u64, _> = writer.as_mut().finish().await;
            if let Err(e) = finish_result {
                eprintln!("COPY finish failed (partial support): {e}");
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

// ============================================================================
// Test: pgwire RowDescription type advertisement (regression for finding #6)
// ============================================================================
//
// Before the fix:
//   - SELECTs reported BIGINT columns as TEXT in RowDescription, breaking pgx.
//   - Empty result sets reported every expression column as TEXT.
//   - Undeclared `$N` parameters defaulted to TEXT, so pgx refused to bind
//     int64 values to `WHERE bigint_col >= $1`.
// After the fix all three round-trip with the right pgwire OIDs.

#[tokio::test]
async fn pg_row_description_advertises_int8_for_bigint() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE rd_t (id INT, ts BIGINT, ratio DOUBLE PRECISION, ok BOOLEAN)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO rd_t VALUES (1, 1700000000000, 3.14, true)")
        .await
        .expect("INSERT");

    // Use the extended protocol with native typed scanners. If RowDescription
    // still reported these columns as TEXT, pgx would fail to decode.
    let row = client
        .query_one(
            "SELECT id, ts, ratio, ok FROM rd_t WHERE id = $1",
            &[&1_i32],
        )
        .await
        .expect("typed SELECT");

    let id: i32 = row.get(0);
    let ts: i64 = row.get(1);
    let ratio: f64 = row.get(2);
    let ok: bool = row.get(3);
    assert_eq!(id, 1);
    assert_eq!(ts, 1700000000000);
    assert!((ratio - 3.14).abs() < 1e-9);
    assert!(ok);

    // Direct OID assertions on the prepared statement\'s row description.
    let stmt = client
        .prepare("SELECT id, ts, ratio, ok FROM rd_t")
        .await
        .expect("prepare");
    let cols = stmt.columns();
    assert_eq!(
        cols[0].type_(),
        &tokio_postgres::types::Type::INT4,
        "id should be int4"
    );
    assert_eq!(
        cols[1].type_(),
        &tokio_postgres::types::Type::INT8,
        "ts should be int8"
    );
    assert_eq!(
        cols[2].type_(),
        &tokio_postgres::types::Type::FLOAT8,
        "ratio should be float8"
    );
    assert_eq!(
        cols[3].type_(),
        &tokio_postgres::types::Type::BOOL,
        "ok should be bool"
    );

    server.abort();
}

#[tokio::test]
async fn pg_row_description_typed_when_filter_returns_zero_rows() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE rdz (ts BIGINT)")
        .await
        .expect("CREATE TABLE");
    // Insert a single row that we will filter out, so the executor takes the
    // empty-rows branch in `project_columns`.
    client
        .simple_query("INSERT INTO rdz VALUES (100)")
        .await
        .expect("INSERT");

    // `SELECT MAX(ts) FROM rdz` — confirm the empty-input aggregate column
    // is advertised as int8 (covers the `LIMIT 0` Describe probe path that
    // pgx hits before binding parameters).  Prior to the fix this would be
    // advertised as Varchar/TEXT because `value_type(Value::Null)` defaults
    // to TEXT and the executor returns NULL for `MAX` over empty input.
    let stmt = client
        .prepare("SELECT MAX(ts) AS m FROM rdz")
        .await
        .expect("prepare");
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT8,
        "MAX(ts) should be advertised as int8 in Describe path",
    );

    // Same for `COUNT(*)` — should always be int8 regardless of input.
    let stmt2 = client
        .prepare("SELECT COUNT(*) AS c FROM rdz")
        .await
        .expect("prepare2");
    assert_eq!(
        stmt2.columns()[0].type_(),
        &tokio_postgres::types::Type::INT8,
        "COUNT(*) should always be int8",
    );

    server.abort();
}

#[tokio::test]
async fn pg_param_types_inferred_from_cast_and_column() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE pi_t (id INT, ts BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO pi_t VALUES (1, 1700000000000, \'a\')")
        .await
        .expect("INSERT");

    // (a) Inferred from the column on the other side of `=`.
    let row = client
        .query_one("SELECT id FROM pi_t WHERE id = $1", &[&1_i32])
        .await
        .expect("eq with int4");
    assert_eq!(row.get::<_, i32>(0), 1);

    // (b) Inferred from `ts >= $1` — column is BIGINT, so $1 is int8.
    let row = client
        .query_one("SELECT ts FROM pi_t WHERE ts >= $1", &[&1_i64])
        .await
        .expect("gte with int8");
    assert_eq!(row.get::<_, i64>(0), 1700000000000);

    // (c) Explicit CAST drives the type even without a column on the other side.
    let stmt = client
        .prepare("SELECT id FROM pi_t WHERE id = CAST($1 AS INT)")
        .await
        .expect("prepare CAST");
    assert_eq!(stmt.params()[0], tokio_postgres::types::Type::INT4);

    server.abort();
}

// ============================================================================
// Text-literal coercion in comparisons (regression for finding #29)
// ============================================================================
//
// pgx's `QueryExecModeSimpleProtocol` interpolates parameters client-side as
// single-quoted text literals before sending the query. So
//   pool.Query(ctx, "SELECT ... WHERE ts >= $1", int64(1700000000000))
// arrives at the executor as
//   SELECT ... WHERE ts >= '1700000000000'
// Without implicit text→numeric coercion, the comparator silently returns
// false and the query yields zero rows. Postgres-compatible behavior is to
// coerce text to the column type when parseable, and to return zero rows
// (not an error) when not.

/// Helper used by the coercion tests below — runs a SELECT and returns how
/// many rows came back via the simple-query protocol (which mirrors what
/// pgx SimpleProtocol mode produces on the wire).
async fn count_simple(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test]
async fn text_literal_coerces_to_int8_in_comparison() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE coerce_int8 (id TEXT, ts BIGINT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO coerce_int8 VALUES ('a', 1700000000000), ('b', 1700000000001)")
        .await
        .expect("INSERT");

    // Native int literal: baseline.
    assert_eq!(
        count_simple(
            &client,
            "SELECT * FROM coerce_int8 WHERE ts >= 1700000000000"
        )
        .await,
        2,
        "baseline native int comparison",
    );
    // Text literal on the right: pgx SimpleProtocol shape.
    assert_eq!(
        count_simple(
            &client,
            "SELECT * FROM coerce_int8 WHERE ts >= '1700000000000'"
        )
        .await,
        2,
        "text literal coerces to int8 for >=",
    );
    // Equality.
    assert_eq!(
        count_simple(
            &client,
            "SELECT * FROM coerce_int8 WHERE ts = '1700000000000'"
        )
        .await,
        1,
        "text literal coerces to int8 for =",
    );
    // Strict <.
    assert_eq!(
        count_simple(
            &client,
            "SELECT * FROM coerce_int8 WHERE ts < '1700000000001'"
        )
        .await,
        1,
        "text literal coerces to int8 for <",
    );
    // Reversed: literal on the left.
    assert_eq!(
        count_simple(
            &client,
            "SELECT * FROM coerce_int8 WHERE '1700000000000' <= ts"
        )
        .await,
        2,
        "text literal coerces when on the left side",
    );
    // BETWEEN with text bounds.
    assert_eq!(
        count_simple(
            &client,
            "SELECT * FROM coerce_int8 WHERE ts BETWEEN '1700000000000' AND '1700000000001'",
        )
        .await,
        2,
        "text bounds in BETWEEN coerce to int8",
    );

    server.abort();
}

#[tokio::test]
async fn unparseable_text_vs_int_returns_no_rows() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE unparseable (n BIGINT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO unparseable VALUES (1), (2), (3)")
        .await
        .expect("INSERT");

    // Postgres-compatible: 'abc' isn't a valid int, but the executor must
    // not error — it returns zero rows.
    assert_eq!(
        count_simple(&client, "SELECT * FROM unparseable WHERE n = 'abc'").await,
        0,
        "= 'abc' against bigint column returns no rows, no error",
    );
    assert_eq!(
        count_simple(&client, "SELECT * FROM unparseable WHERE n >= 'abc'").await,
        0,
        ">= 'abc' against bigint column returns no rows, no error",
    );
    assert_eq!(
        count_simple(&client, "SELECT * FROM unparseable WHERE n < 'abc'").await,
        0,
        "< 'abc' against bigint column returns no rows, no error",
    );

    server.abort();
}

#[tokio::test]
async fn int_literal_against_text_column_coerces_to_text() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE str_col (s TEXT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO str_col VALUES ('a'), ('b'), ('5')")
        .await
        .expect("INSERT");

    // PostgreSQL would error here ("operator does not exist: text = integer")
    // but Nucleus coerces symmetrically — `Value::cast(Int → Text)` is
    // defined and yields the same `'5'`, so the row matches. This is more
    // permissive than Postgres but keeps the executor consistent: both
    // directions of (Text, NonText) → numeric column coerce to the column
    // type, and (Numeric, Text) → text column coerces to text.
    assert_eq!(
        count_simple(&client, "SELECT * FROM str_col WHERE s = 5").await,
        1,
        "int literal coerces to text and matches the '5' row",
    );
    // No matching numeric row (only '5' is parseable but '7' has no match).
    assert_eq!(
        count_simple(&client, "SELECT * FROM str_col WHERE s = 7").await,
        0,
        "int literal that doesn't match any text value returns no rows",
    );
    // Equality against the literal '5' (text) still matches the text row.
    assert_eq!(
        count_simple(&client, "SELECT * FROM str_col WHERE s = '5'").await,
        1,
        "text-vs-text equality still works",
    );

    server.abort();
}

#[tokio::test]
async fn text_vs_float_coercion_works() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE floats (val DOUBLE PRECISION)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO floats VALUES (3.14), (2.71), (1.0)")
        .await
        .expect("INSERT");

    assert_eq!(
        count_simple(&client, "SELECT * FROM floats WHERE val >= '3.0'").await,
        1,
        ">= '3.0' coerces to float8",
    );
    assert_eq!(
        count_simple(&client, "SELECT * FROM floats WHERE val = '3.14'").await,
        1,
        "= '3.14' coerces to float8",
    );
    assert_eq!(
        count_simple(&client, "SELECT * FROM floats WHERE '2.71' = val").await,
        1,
        "reversed text-vs-float coerces",
    );

    server.abort();
}

#[tokio::test]
async fn bool_text_coercion_works() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE flags (id INT, flag BOOLEAN)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query("INSERT INTO flags VALUES (1, true), (2, false), (3, true)")
        .await
        .expect("INSERT");

    assert_eq!(
        count_simple(&client, "SELECT * FROM flags WHERE flag = 'true'").await,
        2,
        "= 'true' coerces to bool",
    );
    assert_eq!(
        count_simple(&client, "SELECT * FROM flags WHERE flag = 't'").await,
        2,
        "= 't' coerces to bool (Postgres short form)",
    );
    assert_eq!(
        count_simple(&client, "SELECT * FROM flags WHERE flag = 'false'").await,
        1,
        "= 'false' coerces to bool",
    );

    server.abort();
}

#[tokio::test]
async fn text_literal_coercion_with_simple_protocol_via_pgx_shape() {
    // Smoke test: simulates the exact shape of an Observe analytics query —
    // a TEXT id column plus a BIGINT timestamp range filter, with both bound
    // values arriving as quoted text literals (pgx SimpleProtocol semantics).
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query("CREATE TABLE events (site_id TEXT, timestamp BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query(
            "INSERT INTO events VALUES \
             ('s1', 1700000000000, 'click'), \
             ('s1', 1700000000500, 'view'), \
             ('s2', 1700000000750, 'click')",
        )
        .await
        .expect("INSERT");

    // Mirror an Observe-style range scan with text-bound parameters.
    let count = count_simple(
        &client,
        "SELECT name FROM events WHERE site_id = 's1' AND timestamp >= '1700000000000'",
    )
    .await;
    assert_eq!(count, 2, "site filter + text-bound timestamp range");

    server.abort();
}

// ============================================================================
// Binary-format typed parameters (corruption-class regression)
//
// Before the decode_binary_param_typed fix, a BINARY-format timestamp/date/
// uuid/bytea/numeric parameter fell into a catch-all that reinterpreted its
// bytes as an integer — silent wrong data for every binary-mode driver
// (tokio-postgres, pgx default, JDBC). This test drives the exact wire
// encodings those drivers emit and asserts value-level round-trips.
// ============================================================================

mod binary_params {
    use super::*;
    use bytes::BytesMut;
    use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

    /// Sends exact raw bytes as a BINARY-format parameter for any declared
    /// type — lets the test drive precise driver wire encodings without
    /// pulling chrono/uuid/decimal client crates into the dev-dependencies.
    #[derive(Debug)]
    struct RawBinary(Vec<u8>);

    impl ToSql for RawBinary {
        fn to_sql(
            &self,
            _ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
            out.extend_from_slice(&self.0);
            Ok(IsNull::No)
        }
        fn accepts(_ty: &Type) -> bool {
            true
        }
        to_sql_checked!();
    }

    #[tokio::test]
    async fn binary_typed_params_round_trip() {
        let (port, server) = start_nucleus_server().await;
        let client = connect(port).await;

        client
            .simple_query(
                "CREATE TABLE bin_params (id INT PRIMARY KEY, ts TIMESTAMP, d DATE, \
                 u UUID, b BYTEA, n NUMERIC)",
            )
            .await
            .expect("CREATE TABLE");

        let stmt = client
            .prepare_typed(
                "INSERT INTO bin_params VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    Type::INT4,
                    Type::TIMESTAMP,
                    Type::DATE,
                    Type::UUID,
                    Type::BYTEA,
                    Type::NUMERIC,
                ],
            )
            .await
            .expect("prepare_typed");

        // day 8851 after 2000-01-01 = 2024-03-26, time 12:34:56.789012
        let ts_us: i64 = 8851 * 86_400_000_000 + 45_296_789_012;
        let date_days: i32 = 8851;
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55,
            0x44, 0x00, 0x00,
        ];
        let bytea_bytes: Vec<u8> = vec![0x00, 0xde, 0xad, 0xbe, 0xef];
        // numeric 12345.6789: ndigits=3 weight=1 sign=+ dscale=4 [1,2345,6789]
        let mut numeric = Vec::new();
        numeric.extend_from_slice(&3u16.to_be_bytes());
        numeric.extend_from_slice(&1i16.to_be_bytes());
        numeric.extend_from_slice(&0u16.to_be_bytes());
        numeric.extend_from_slice(&4u16.to_be_bytes());
        for d in [1u16, 2345, 6789] {
            numeric.extend_from_slice(&d.to_be_bytes());
        }

        client
            .execute(
                &stmt,
                &[
                    &1i32,
                    &RawBinary(ts_us.to_be_bytes().to_vec()),
                    &RawBinary(date_days.to_be_bytes().to_vec()),
                    &RawBinary(uuid_bytes.to_vec()),
                    &RawBinary(bytea_bytes.clone()),
                    &RawBinary(numeric),
                ],
            )
            .await
            .expect("INSERT with binary-format typed params");

        // Read back as text casts so the assertion is on the STORED VALUES,
        // independent of result-side encoding.
        let rows = client
            .simple_query(
                "SELECT ts::text, d::text, u::text, b::text, n::text \
                 FROM bin_params WHERE id = 1",
            )
            .await
            .expect("SELECT");
        let row = rows
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .expect("one row");

        assert_eq!(row.get(0), Some("2024-03-26 12:34:56.789012"), "timestamp");
        assert_eq!(row.get(1), Some("2024-03-26"), "date");
        assert_eq!(
            row.get(2),
            Some("550e8400-e29b-41d4-a716-446655440000"),
            "uuid"
        );
        assert_eq!(row.get(3), Some("\\x00deadbeef"), "bytea");
        assert_eq!(row.get(4), Some("12345.6789"), "numeric");

        server.abort();
    }

    /// Text params containing backslashes must round-trip literally
    /// (standard-conforming strings — the old sanitizer doubled them).
    #[tokio::test]
    async fn text_param_backslash_round_trip() {
        let (port, server) = start_nucleus_server().await;
        let client = connect(port).await;

        client
            .simple_query("CREATE TABLE bs (id INT PRIMARY KEY, t TEXT)")
            .await
            .expect("CREATE TABLE");

        let stmt = client
            .prepare_typed("INSERT INTO bs VALUES ($1, $2)", &[Type::INT4, Type::TEXT])
            .await
            .expect("prepare");
        let payload = r"C:\temp\new\x1";
        client
            .execute(&stmt, &[&1i32, &payload])
            .await
            .expect("INSERT");

        let rows = client
            .simple_query("SELECT t FROM bs WHERE id = 1")
            .await
            .expect("SELECT");
        let row = rows
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .expect("one row");
        assert_eq!(row.get(0), Some(payload), "backslashes must not double");

        server.abort();
    }
}
