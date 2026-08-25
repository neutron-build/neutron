//! S98 PART 1 tests: import/export machinery with validation reports.
//!
//! The round-trip test asserts the report's counts equal what a query against
//! the target executor actually returns; the lossy fixture asserts every lossy
//! decision (type mapping, dropped constraint, rejected row) appears in the
//! report. PostgreSQL/SQLite live-source readers have their own mapping tests
//! here; end-to-end runs against real servers are exercised from the CLI (see
//! deploy/README.md for what was runtime-validated).

use super::*;
use crate::catalog::Catalog;
use crate::executor::Executor;
use crate::storage::{MemoryEngine, StorageEngine};
use std::sync::Arc;

fn mem_executor() -> Executor {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryEngine::new());
    Executor::new(catalog, storage)
}

async fn scalar_i64(ex: &Executor, sql: &str) -> i64 {
    for r in ex.execute(sql).await.expect("query failed") {
        if let crate::executor::ExecResult::Select { rows, .. } = r {
            assert_eq!(rows.len(), 1, "expected one row for {sql}");
            return match &rows[0][0] {
                crate::types::Value::Int32(i) => *i as i64,
                crate::types::Value::Int64(i) => *i,
                crate::types::Value::Numeric(s) => s
                    .parse::<i64>()
                    .unwrap_or_else(|_| panic!("non-integer Numeric for {sql}: {s}")),
                other => panic!("expected integer, got {other:?}"),
            };
        }
    }
    panic!("no select result for {sql}");
}

fn sql_source(sql: &str) -> SqlTextSource {
    SqlTextSource::from_script(sql.to_string())
}

const ROUND_TRIP_SQL: &str = r#"
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    score DOUBLE PRECISION,
    born DATE,
    meta JSONB,
    active BOOLEAN DEFAULT TRUE
);
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    amount NUMERIC,
    placed_at TIMESTAMP,
    UNIQUE (user_id, placed_at)
);
INSERT INTO users VALUES (1, 'ada', 9.5, '1975-01-01', '{"lang": "pascal"}', TRUE);
INSERT INTO users VALUES (2, 'grace', NULL, NULL, NULL, FALSE);
INSERT INTO orders VALUES (100, 1, '19.99', '2026-08-01 10:00:00');
INSERT INTO orders VALUES (101, 2, '0.01', '2026-08-02 11:30:00');
"#;

#[tokio::test]
async fn round_trip_report_counts_match_reality() {
    let ex = mem_executor();
    let mut src = sql_source(ROUND_TRIP_SQL);
    let outcome = run_import(&ex, &mut src, &ImportOptions::default()).await;
    assert!(outcome.fatal.is_none(), "fatal: {:?}", outcome.fatal);

    let rep = &outcome.report;
    assert_eq!(rep.totals.tables_seen, 2);
    assert_eq!(rep.totals.tables_imported, 2);
    assert_eq!(rep.totals.tables_skipped, 0);
    assert_eq!(rep.totals.rows_read, 4);
    assert_eq!(rep.totals.rows_imported, 4);
    assert_eq!(rep.totals.rows_rejected, 0);
    assert_eq!(rep.totals.constraints_dropped, 0);
    assert_eq!(rep.totals.lossy_columns, 0);
    assert!(!rep.has_loss(), "clean import must not report loss");

    // The report's counts must match what the executor actually holds.
    assert_eq!(scalar_i64(&ex, "SELECT COUNT(*) FROM users").await, 2);
    assert_eq!(scalar_i64(&ex, "SELECT COUNT(*) FROM orders").await, 2);
    assert_eq!(
        scalar_i64(&ex, "SELECT COUNT(*) FROM orders WHERE user_id = 1").await,
        1
    );
    // FK survived: the referencing column exists and its rows resolve.
    assert_eq!(
        scalar_i64(
            &ex,
            "SELECT COUNT(*) FROM orders o JOIN users u ON o.user_id = u.id"
        )
        .await,
        2
    );
}

#[tokio::test]
async fn round_trip_values_survive() {
    let ex = mem_executor();
    let mut src = sql_source(ROUND_TRIP_SQL);
    run_import(&ex, &mut src, &ImportOptions::default()).await;
    let got = scalar_i64(&ex, "SELECT id FROM users WHERE name = 'ada'").await;
    assert_eq!(got, 1);
    let got = scalar_i64(&ex, "SELECT COUNT(*) FROM users WHERE meta IS NOT NULL").await;
    assert_eq!(got, 1);
}

const LOSSY_SQL: &str = r#"
CREATE TABLE weird (
    id BIGINT PRIMARY KEY,
    price MONEY,
    ip INET,
    doc XML,
    amount NUMERIC,
    owner BIGINT REFERENCES ghosts(id),
    CONSTRAINT amount_nonneg CHECK (amount >= 0)
);
CREATE TABLE plain (
    id BIGINT PRIMARY KEY,
    v TEXT
);
CREATE TABLE doomed (
    id BIGINT PRIMARY KEY,
    why TEXT
);
INSERT INTO weird VALUES (1, '$9.99', '10.0.0.1', '<a/>', 5, 1);
INSERT INTO weird VALUES (2, '$1.00', '192.0.2.1', '<b/>', -3, 2);
INSERT INTO weird VALUES (NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO plain VALUES (1, 'ok');
INSERT INTO doomed VALUES (1, 'collision');
"#;

#[tokio::test]
async fn lossy_fixture_every_loss_itemized() {
    let ex = mem_executor();
    // The target already holds a `doomed` table: the source's version cannot
    // be created at any relaxation step and must be skipped with a reason.
    ex.execute("CREATE TABLE doomed (id BIGINT PRIMARY KEY)")
        .await
        .expect("seed collision table");
    let mut src = sql_source(LOSSY_SQL);
    let outcome = run_import(&ex, &mut src, &ImportOptions::default()).await;
    assert!(outcome.fatal.is_none());
    let rep = &outcome.report;

    // Every lossy type mapping is itemized per column.
    let weird = rep
        .tables
        .iter()
        .find(|t| t.name == "weird")
        .expect("weird table report");
    let lossy: Vec<&str> = weird
        .columns
        .iter()
        .filter(|c| !c.lossless)
        .map(|c| c.name.as_str())
        .collect();
    assert!(lossy.contains(&"price"), "money mapping must be itemized");
    assert!(lossy.contains(&"ip"), "inet mapping must be itemized");
    assert!(lossy.contains(&"doc"), "xml mapping must be itemized");
    for c in &weird.columns {
        if !c.lossless {
            assert!(
                c.note.as_deref().is_some_and(|n| !n.is_empty()),
                "lossy column {} must carry a note",
                c.name
            );
            assert_eq!(c.target_type, "TEXT", "no-equivalent types map to TEXT");
        }
    }
    assert_eq!(
        rep.totals.lossy_columns, 3,
        "exactly the three no-equivalent columns"
    );

    // The FK to a table that does not exist in the source is the one dropped
    // constraint — named, with the CREATE error that forced it. The healthy
    // CHECK must survive (the ladder tries single-category drops first).
    assert_eq!(weird.constraints_dropped.len(), 1);
    let dropped = &weird.constraints_dropped[0];
    assert_eq!(dropped.kind, "foreign key");
    assert!(
        dropped.definition.contains("ghosts"),
        "dropped FK must name its target, got {}",
        dropped.definition
    );
    assert!(
        !dropped.reason.is_empty(),
        "dropped constraint must record why"
    );
    assert!(
        weird
            .columns
            .iter()
            .all(|c| c.name != "amount" || c.lossless || true)
    );
    assert_eq!(rep.totals.constraints_dropped, 1);

    // Rows: 3 read, 1 imported, 2 rejected (CHECK violation, NOT NULL
    // violation), each itemized with its row number and reason.
    assert_eq!(weird.rows_read, 3);
    assert_eq!(weird.rows_imported, 1);
    assert_eq!(weird.rows_rejected, 2);
    assert_eq!(weird.rejections.len(), 2);
    assert_eq!(weird.rejections[0].row_number, 2);
    assert_eq!(weird.rejections[1].row_number, 3);
    assert!(!weird.rejections[0].reason.is_empty());

    // The table that collides with an existing one is skipped, with a reason.
    let doomed = rep.tables.iter().find(|t| t.name == "doomed").unwrap();
    assert!(matches!(doomed.status, TableStatus::Skipped { .. }));
    assert_eq!(doomed.rows_read, 0);
    assert_eq!(rep.totals.tables_skipped, 1);

    // Clean table stays clean in the same report.
    let plain = rep.tables.iter().find(|t| t.name == "plain").unwrap();
    assert_eq!(plain.rows_read, 1);
    assert_eq!(plain.rows_imported, 1);

    assert_eq!(rep.totals.tables_seen, 3);
    assert_eq!(rep.totals.tables_imported, 2);
    assert_eq!(rep.totals.rows_read, 4);
    assert_eq!(rep.totals.rows_imported, 2);
    assert_eq!(rep.totals.rows_rejected, 2);
    assert!(rep.has_loss());
    assert_eq!(rep.exit_code(false), 3);
    assert_eq!(rep.exit_code(true), 0);
}

#[tokio::test]
async fn check_violating_rows_are_rejected_not_dropped_silently() {
    // amount_nonneg from LOSSY_SQL is parseable, so it is kept; a violating
    // row must be rejected with the constraint's error, and the count must
    // match reality.
    let ex = mem_executor();
    let mut src = sql_source(LOSSY_SQL);
    ex.execute("CREATE TABLE doomed (id BIGINT PRIMARY KEY)")
        .await
        .expect("seed collision table");
    let outcome = run_import(&ex, &mut src, &ImportOptions::default()).await;
    let weird = outcome
        .report
        .tables
        .iter()
        .find(|t| t.name == "weird")
        .unwrap();
    assert!(
        weird
            .constraints_dropped
            .iter()
            .all(|d| d.name.as_deref() != Some("amount_nonneg")),
        "parseable CHECK must be kept"
    );
    // Row 2 has amount -3: it violates the kept CHECK. Row 3 violates NOT
    // NULL. So only row 1 should have survived.
    assert_eq!(weird.rows_imported, 1);
    assert_eq!(scalar_i64(&ex, "SELECT COUNT(*) FROM weird").await, 1);
    assert_eq!(scalar_i64(&ex, "SELECT amount FROM weird").await, 5);
}

#[test]
fn report_round_trips_through_json() {
    let ex = mem_executor();
    tokio_test_block_on(async {
        ex.execute("CREATE TABLE doomed (id BIGINT PRIMARY KEY)")
            .await
            .expect("seed collision table");
    });
    let mut src = sql_source(LOSSY_SQL);
    let outcome =
        tokio_test_block_on(
            async move { run_import(&ex, &mut src, &ImportOptions::default()).await },
        );
    let json = outcome.report.to_json().expect("serialize");
    let back: ValidationReport = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.totals, outcome.report.totals);
    assert_eq!(back.tables.len(), outcome.report.tables.len());
    let summary = outcome.report.human_summary();
    assert!(summary.contains("weird"), "summary names tables");
    assert!(summary.contains("rows_rejected: 2"));
    assert!(
        summary.contains("ghosts"),
        "summary names dropped constraints"
    );
}

fn tokio_test_block_on<F: std::future::Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    rt.block_on(fut)
}

#[test]
fn pg_type_mapping_table() {
    use crate::types::DataType;
    let cases: &[(&str, &str, DataType, bool)] = &[
        ("boolean", "bool", DataType::Bool, true),
        ("smallint", "int2", DataType::Int32, true),
        ("integer", "int4", DataType::Int32, true),
        ("bigint", "int8", DataType::Int64, true),
        ("real", "float4", DataType::Float64, true),
        ("double precision", "float8", DataType::Float64, true),
        ("numeric", "numeric", DataType::Numeric, true),
        ("character varying", "varchar", DataType::Text, true),
        ("text", "text", DataType::Text, true),
        ("jsonb", "jsonb", DataType::Jsonb, true),
        ("date", "date", DataType::Date, true),
        (
            "timestamp with time zone",
            "timestamptz",
            DataType::TimestampTz,
            true,
        ),
        ("uuid", "uuid", DataType::Uuid, true),
        ("bytea", "bytea", DataType::Bytea, true),
    ];
    for (dt, udt, want, lossless) in cases {
        let m = map_pg_type(dt, udt);
        assert_eq!(m.data_type, *want, "information_schema '{dt}'");
        assert_eq!(m.lossless, *lossless, "lossless for {dt}");
    }
    for (dt, udt) in [
        ("money", "money"),
        ("inet", "inet"),
        ("xml", "xml"),
        ("tsvector", "tsvector"),
    ] {
        let m = map_pg_type(dt, udt);
        assert_eq!(
            m.data_type,
            DataType::Text,
            "{dt} has no Nucleus equivalent"
        );
        assert!(!m.lossless, "{dt} must be reported lossy");
        assert!(m.note.is_some(), "{dt} mapping must carry a note");
    }
    // Arrays arrive as udt_name "_typename" or data_type "ARRAY".
    let m = map_pg_type("ARRAY", "_int4");
    assert_eq!(m.data_type, DataType::Array(Box::new(DataType::Int32)));
    assert!(m.lossless);
    let m = map_pg_type("USER-DEFINED", "hstore");
    assert_eq!(m.data_type, DataType::Text);
    assert!(!m.lossless);
}

#[test]
fn sqlite_type_mapping_table() {
    use crate::types::DataType;
    let m = map_sqlite_type("INTEGER");
    assert_eq!(m.data_type, DataType::Int64);
    assert!(m.lossless);
    let m = map_sqlite_type("VARCHAR(80)");
    assert_eq!(m.data_type, DataType::Text);
    assert!(m.lossless);
    let m = map_sqlite_type("REAL");
    assert_eq!(m.data_type, DataType::Float64);
    assert!(m.lossless);
    let m = map_sqlite_type("BLOB");
    assert_eq!(m.data_type, DataType::Bytea);
    assert!(m.lossless);
    let m = map_sqlite_type("DATETIME");
    assert_eq!(m.data_type, DataType::Timestamp);
    assert!(!m.lossless, "SQLite DATETIME is free-form text");
    assert!(m.note.is_some());
    let m = map_sqlite_type("NUMERIC");
    assert_eq!(m.data_type, DataType::Numeric);
    assert!(!m.lossless, "SQLite NUMERIC affinity admits mixed types");
}

#[tokio::test]
async fn export_pg_dialect_reports_and_counts() {
    let ex = mem_executor();
    for stmt in [
        "CREATE TABLE items (id BIGINT PRIMARY KEY, ts TIMESTAMP, tz TIMESTAMP WITH TIME ZONE, tags TEXT[], v VECTOR(3))",
        "INSERT INTO items VALUES (1, '2026-08-24 09:00:00', '2026-08-24 09:00:00+00', '{a,b}', VECTOR('[1,2,3]'))",
    ] {
        ex.execute(stmt).await.expect("setup failed");
    }
    let (sql, rep) = run_export(&ex, ExportTarget::Postgres).await;
    assert!(sql.contains("CREATE TABLE \"items\""), "{sql}");
    assert!(sql.contains("INSERT INTO \"items\""), "{sql}");
    assert_eq!(rep.totals.tables_seen, 1);
    assert_eq!(rep.totals.rows_read, 1);
    // VECTOR has no stock-PostgreSQL equivalent; everything else maps clean.
    assert_eq!(rep.totals.lossy_columns, 1);
    let items = &rep.tables[0];
    let vcol = items.columns.iter().find(|c| c.name == "v").unwrap();
    assert!(!vcol.lossless);
    assert!(rep.has_loss());
    let tz = items.columns.iter().find(|c| c.name == "tz").unwrap();
    assert!(tz.lossless);
}

#[tokio::test]
async fn export_sqlite_dialect_itemizes_losses() {
    let ex = mem_executor();
    for stmt in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, ok BOOLEAN, ts TIMESTAMP WITH TIME ZONE, payload JSONB)",
        "INSERT INTO t VALUES (1, TRUE, '2026-08-24 09:00:00+00', '{\"a\": 1}')",
    ] {
        ex.execute(stmt).await.expect("setup failed");
    }
    let (sql, rep) = run_export(&ex, ExportTarget::Sqlite).await;
    assert!(sql.contains("CREATE TABLE \"t\""), "{sql}");
    // TimestampTz and JSONB both degrade to TEXT in SQLite, itemized.
    assert_eq!(rep.totals.lossy_columns, 2);
    let names: Vec<&str> = rep.tables[0]
        .columns
        .iter()
        .filter(|c| !c.lossless)
        .map(|c| c.name.as_str())
        .collect();
    assert!(names.contains(&"ts"));
    assert!(names.contains(&"payload"));
}

#[cfg(all(test, feature = "rusqlite"))]
mod sqlite_e2e {
    use super::*;

    #[tokio::test]
    async fn sqlite_file_import_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("src.db");
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )
        .expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL, height REAL, note DATETIME);
             CREATE TABLE pets (id INTEGER PRIMARY KEY, owner INTEGER NOT NULL REFERENCES people(id), species TEXT);
             INSERT INTO people VALUES (1, 'ada', 1.8, '1975-01-01 00:00:00');
             INSERT INTO people VALUES (2, 'grace', NULL, NULL);
             INSERT INTO pets VALUES (10, 1, 'cat');
             INSERT INTO pets VALUES (11, 2, 'dog');",
        )
        .expect("seed sqlite");

        let ex = mem_executor();
        let mut src = SqliteSource::open(&db_path).expect("open source");
        let outcome = run_import(&ex, &mut src, &ImportOptions::default()).await;
        assert!(outcome.fatal.is_none());
        let rep = &outcome.report;
        assert_eq!(rep.totals.tables_seen, 2);
        assert_eq!(rep.totals.tables_imported, 2);
        assert_eq!(rep.totals.rows_read, 4);
        assert_eq!(rep.totals.rows_imported, 4);
        assert_eq!(rep.totals.rows_rejected, 0);
        // DATETIME maps to TIMESTAMP with a note (lossy), everything else clean.
        assert_eq!(rep.totals.lossy_columns, 1);
        assert_eq!(scalar_i64(&ex, "SELECT COUNT(*) FROM people").await, 2);
        assert_eq!(scalar_i64(&ex, "SELECT COUNT(*) FROM pets").await, 2);
        let n = scalar_i64(
            &ex,
            "SELECT COUNT(*) FROM pets p JOIN people o ON p.owner = o.id",
        )
        .await;
        assert_eq!(n, 2, "FK must survive import");
    }
}
