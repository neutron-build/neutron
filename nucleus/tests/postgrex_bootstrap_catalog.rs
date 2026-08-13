//! Postgrex's type bootstrap must run, or no Elixir client can connect at all.
//!
//! Postgrex issues one type-introspection query before it will serve a single
//! statement. Against Nucleus that query errored, Postgrex retried the
//! bootstrap rather than surfacing the error, and the caller received a
//! `DBConnection` queue timeout — a symptom that names nothing. Every Elixir,
//! Ecto and Phoenix application was locked out, always, for as long as the
//! engine has reported a version >= 9.2 (it reports "16.0 (Nucleus)", and no
//! client option skips these joins above that version).
//!
//! Two engine-side blockers, both in `pg_catalog`:
//!
//!   1. `relation "pg_range" does not exist` — joined for rngsubtype /
//!      rngtypid / rngmultitypid.
//!   2. `column "t.typsend" does not exist` — `pg_type` had `typinput` but
//!      not `typoutput`, `typreceive` or `typsend`.
//!
//! These assert the SHAPE, not merely that the relations answer. A `pg_range`
//! with the wrong columns fails identically to a missing one, and it fails as
//! a client-side timeout, which is the least diagnosable form this can take.
//! Nothing else in the suite drives a real bootstrap, which is why a 40-plus
//! relation catalog could be missing one and nobody knew.
#![cfg(feature = "server")]

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;

async fn db() -> Database {
    Database::builder().memory().build().unwrap()
}

fn columns_of(r: Vec<ExecResult>) -> Vec<String> {
    match r.into_iter().next_back().unwrap() {
        ExecResult::Select { columns, .. } => {
            columns.into_iter().map(|(n, _)| n.to_lowercase()).collect()
        }
        o => panic!("expected a SELECT result, got {o:?}"),
    }
}

/// Every column Postgrex names must be selectable BY NAME. Selecting them
/// individually is the point: `SELECT *` would pass against a relation whose
/// columns are all wrong.
#[tokio::test]
async fn pg_range_answers_with_the_columns_postgrex_joins_on() {
    let db = db().await;

    for col in [
        "rngtypid",
        "rngsubtype",
        "rngmultitypid",
        "rngcollation",
        "rngsubopc",
    ] {
        db.execute(&format!("SELECT r.{col} FROM pg_range r"))
            .await
            .unwrap_or_else(|e| panic!("pg_range.{col} must be selectable: {e}"));
    }

    // Both spellings, qualified and bare, and through an alias — Postgrex uses
    // the aliased form inside a LEFT JOIN.
    for sql in [
        "SELECT * FROM pg_range",
        "SELECT * FROM pg_catalog.pg_range",
        "SELECT r.rngtypid, r.rngsubtype FROM pg_catalog.pg_range r",
    ] {
        db.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // Nucleus has no range types, so it is legitimately empty — but it must be
    // empty by answering, not by failing.
    let cols = columns_of(db.execute("SELECT * FROM pg_range").await.unwrap());
    for want in ["rngtypid", "rngsubtype", "rngmultitypid"] {
        assert!(
            cols.iter().any(|c| c == want),
            "pg_range is missing {want}; it has {cols:?}"
        );
    }
}

/// `typinput` was present and the other three were not, so a query naming all
/// four failed on the first one Postgrex asked for.
#[tokio::test]
async fn pg_type_carries_all_four_io_function_columns() {
    let db = db().await;

    for col in ["typinput", "typoutput", "typreceive", "typsend"] {
        db.execute(&format!("SELECT t.{col} FROM pg_type t"))
            .await
            .unwrap_or_else(|e| panic!("pg_type.{col} must be selectable: {e}"));
    }

    let rows = match db
        .execute(
            "SELECT t.typname, t.typinput, t.typoutput, t.typreceive, t.typsend FROM pg_type t",
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows,
        o => panic!("{o:?}"),
    };
    assert!(!rows.is_empty(), "pg_type must expose the base types");

    // The values follow Postgres's naming so a client that matches on them
    // (prisma keys array detection off 'array_in') keeps working.
    let text = |v: &nucleus::types::Value| match v {
        nucleus::types::Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    };
    for row in &rows {
        let name = text(&row[0]);
        assert_eq!(text(&row[1]), format!("{name}in"));
        assert_eq!(text(&row[2]), format!("{name}out"));
        assert_eq!(text(&row[3]), format!("{name}recv"));
        assert_eq!(text(&row[4]), format!("{name}send"));
    }
}

/// The bootstrap shape end to end: pg_type LEFT JOINed to pg_range, which is
/// how Postgrex asks. Neither half is useful if the join does not run.
#[tokio::test]
async fn the_bootstrap_join_shape_runs() {
    let db = db().await;
    let r = db
        .execute(
            "SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput, \
                    r.rngsubtype \
             FROM pg_type AS t \
             LEFT JOIN pg_range AS r ON r.rngtypid = t.oid",
        )
        .await
        .expect("Postgrex's bootstrap join shape must run");
    let cols = columns_of(r);
    assert_eq!(cols.len(), 7, "bootstrap projection shape: {cols:?}");
}
