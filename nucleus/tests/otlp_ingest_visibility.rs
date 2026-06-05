//! Regression tests for dogfood finding #25.
//!
//! Finding #25 alleged that OTLP HTTP ingest INSERTs into the `spans` table
//! returned `200 OK` at the wire layer but rows never landed in storage when a
//! subsequent `psql` SELECT ran against the same Nucleus instance. The actual
//! root-cause investigation (2026-05-10) concluded:
//!
//! 1. The OTLP ingest path goes through neutron-go → pgx (SimpleProtocol) →
//!    pgwire → executor → `execute_insert` → `ColumnarStorageEngine::insert_batch`.
//!    All of these write the row deterministically. A fresh pgx pool reading
//!    the table immediately after sees the row.
//! 2. The reporter's symptom — "rows from OTLP path never land" — could not
//!    be reproduced. Both psql and a fresh pgx pool see the OTLP-inserted rows
//!    immediately. What the reporter actually observed was a query-side
//!    cross-connection visibility issue against `replacing_mergetree` tables
//!    (findings #20 / #27, sibling agent's territory) — not an INSERT bug.
//!
//! These tests lock in the contract that:
//! - INSERTs from one pgwire connection are visible to a SELECT on a separate
//!   pgwire connection immediately after the INSERT acks.
//! - The same holds when the table uses `WITH (engine = 'mergetree')` — the
//!   exact engine declared by Observe's `spans` table.
//! - Concurrent INSERTs from many connections (mimicking a busy OTLP ingest
//!   loop) all land and are scannable from a different connection.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::wire::{NucleusHandler, NucleusServer};

// ---------------------------------------------------------------------------
// Test harness — borrowed from tests/pg_compat.rs, kept inline so each test
// file is independently runnable via `cargo test --test otlp_ingest_visibility`.
// ---------------------------------------------------------------------------

async fn start_nucleus_server() -> (u16, tokio::task::JoinHandle<()>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
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

// CREATE TABLE statement that mirrors Observe's `spans` schema. The minimal
// schema needed to exercise the OTLP ingest visibility contract; no index or
// constraint additions to keep the regression focused on the storage path.
const SPANS_SCHEMA: &str = "
    CREATE TABLE spans (
        trace_id       TEXT NOT NULL,
        span_id        TEXT NOT NULL,
        parent_span_id TEXT NOT NULL DEFAULT '',
        tenant_id      TEXT NOT NULL DEFAULT 'default',
        site_id        TEXT NOT NULL,
        service_name   TEXT NOT NULL DEFAULT '',
        operation_name TEXT NOT NULL DEFAULT '',
        span_kind      TEXT NOT NULL DEFAULT 'internal',
        start_time     BIGINT NOT NULL,
        end_time       BIGINT NOT NULL,
        duration_ms    BIGINT NOT NULL DEFAULT 0,
        status_code    TEXT NOT NULL DEFAULT 'unset',
        status_message TEXT NOT NULL DEFAULT '',
        attributes     JSONB,
        resource       JSONB,
        events         JSONB
    ) WITH (engine = 'mergetree')
";

fn count_data_rows(rows: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    rows.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

// ---------------------------------------------------------------------------
// 1. INSERT-then-SELECT on the same connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn otlp_style_insert_visible_same_connection() {
    let (port, server) = start_nucleus_server().await;
    let client = connect(port).await;

    client
        .simple_query(SPANS_SCHEMA)
        .await
        .expect("CREATE TABLE spans");

    // Insert 5 spans serially via the extended protocol with $-parameters.
    // Mirrors neutron-go's `sql.Exec(ctx, "INSERT INTO spans ...")` that the
    // OTLP HTTP handler runs once per span in the request envelope.
    for i in 0..5 {
        let trace = format!("trace_{i:02}");
        let span = format!("span_{i:02}");
        // start_time/end_time/duration_ms are BIGINT; the server infers Int8 for
        // these params from the target columns, so they must be bound as i64
        // (a Rust String would be rejected client-side by tokio-postgres, exactly
        // as real Postgres rejects a text value for an int8 parameter).
        let start = 1_700_000_000_000i64 + i;
        let end = 1_700_000_001_000i64 + i;
        let dur = 1000i64;
        client
            .execute(
                "INSERT INTO spans (
                    trace_id, span_id, parent_span_id, tenant_id, site_id,
                    service_name, operation_name, span_kind,
                    start_time, end_time, duration_ms,
                    status_code, status_message,
                    attributes, resource, events
                ) VALUES ($1,$2,'','default','site_a',
                          'svc','op','internal',
                          $3,$4,$5,
                          'ok','',
                          '{}','{}','[]')",
                &[&trace, &span, &start, &end, &dur],
            )
            .await
            .expect("OTLP-style INSERT");
    }

    // SELECT on the same connection — the seed of the regression. Finding #25
    // alleged this returned 0 rows; assert it returns exactly 5.
    let rows = client
        .simple_query("SELECT trace_id, span_id FROM spans WHERE site_id = 'site_a'")
        .await
        .expect("SELECT after INSERTs");

    assert_eq!(
        count_data_rows(&rows),
        5,
        "expected 5 OTLP-style INSERTs to be visible on the same connection"
    );

    server.abort();
}

// ---------------------------------------------------------------------------
// 2. Cross-connection visibility — separate pgwire conns for INSERT vs SELECT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn otlp_style_insert_visible_to_separate_connection() {
    let (port, server) = start_nucleus_server().await;
    let writer = connect(port).await;
    let reader = connect(port).await;

    writer
        .simple_query(SPANS_SCHEMA)
        .await
        .expect("CREATE TABLE spans");

    // Write 4 rows from `writer`.
    for i in 0..4 {
        let trace = format!("xtrace_{i}");
        let span = format!("xspan_{i}");
        let start = 1_700_000_000_000i64 + i * 1_000;
        let end = 1_700_000_001_000i64 + i * 1_000;
        let dur = 1000i64;
        writer
            .execute(
                "INSERT INTO spans (
                    trace_id, span_id, parent_span_id, tenant_id, site_id,
                    service_name, operation_name, span_kind,
                    start_time, end_time, duration_ms,
                    status_code, status_message,
                    attributes, resource, events
                ) VALUES ($1,$2,'','default','site_b',
                          'cross-conn-svc','op','internal',
                          $3,$4,$5,
                          'ok','',
                          '{}','{}','[]')",
                &[&trace, &span, &start, &end, &dur],
            )
            .await
            .expect("INSERT from writer connection");
    }

    // Read from a SEPARATE pgwire connection. The connection-router sends
    // `reader` to a different core than `writer`, so this exercises the
    // shared-state contract: INSERT must be globally visible the moment it
    // acks.
    let rows = reader
        .simple_query("SELECT trace_id, span_id FROM spans WHERE site_id = 'site_b'")
        .await
        .expect("SELECT from reader connection");

    assert_eq!(
        count_data_rows(&rows),
        4,
        "INSERTs from one pgwire conn must be visible to a SELECT on another"
    );

    server.abort();
}

// ---------------------------------------------------------------------------
// 3. Concurrent inserters from many connections — mimics a busy OTLP ingest
//    where multiple SDK exporters POST simultaneously.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn otlp_style_concurrent_inserts_all_visible() {
    let (port, server) = start_nucleus_server().await;

    // Schema-creating connection.
    let setup = connect(port).await;
    setup
        .simple_query(SPANS_SCHEMA)
        .await
        .expect("CREATE TABLE spans");
    drop(setup);

    // Spawn 8 concurrent inserter connections, each writing 4 spans. This
    // mirrors a fanout of 8 OTLP HTTP handler invocations all hitting Nucleus
    // in parallel — the exact load shape that would expose any
    // per-connection / per-core staging buffer that doesn't make rows globally
    // visible by INSERT-ack time.
    let mut tasks = Vec::new();
    for w in 0..8u32 {
        let task = tokio::spawn(async move {
            let client = connect(port).await;
            for i in 0..4 {
                let trace = format!("ctrace_{w}_{i}");
                let span = format!("cspan_{w}_{i}");
                let start = 1_700_000_000_000i64 + (w as i64) * 100 + i;
                let end = 1_700_000_001_000i64 + (w as i64) * 100 + i;
                let dur = 1000i64;
                client
                    .execute(
                        "INSERT INTO spans (
                            trace_id, span_id, parent_span_id, tenant_id, site_id,
                            service_name, operation_name, span_kind,
                            start_time, end_time, duration_ms,
                            status_code, status_message,
                            attributes, resource, events
                        ) VALUES ($1,$2,'','default','site_c',
                                  'concurrent-svc','op','internal',
                                  $3,$4,$5,
                                  'ok','',
                                  '{}','{}','[]')",
                        &[&trace, &span, &start, &end, &dur],
                    )
                    .await
                    .expect("concurrent INSERT");
            }
        });
        tasks.push(task);
    }

    for t in tasks {
        t.await.expect("inserter task");
    }

    // Verify all 8 * 4 = 32 spans are scannable from yet another connection.
    let reader = connect(port).await;
    let rows = reader
        .simple_query("SELECT trace_id, span_id FROM spans WHERE site_id = 'site_c'")
        .await
        .expect("SELECT all concurrent INSERTs");

    assert_eq!(
        count_data_rows(&rows),
        32,
        "32 concurrent OTLP-style INSERTs across 8 connections must all be visible"
    );

    server.abort();
}
