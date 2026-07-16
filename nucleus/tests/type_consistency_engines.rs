//! End-to-end T0.1 repros across storage engines.
//!
//! The pre-existing executor suite runs on MemoryEngine only (`tests/mod.rs`), so
//! it never saw the disk-engine-specific manifestation of the `Value` type-
//! consistency defect. These drive the embedded `Database` on memory, mvcc, and
//! disk and assert the two things that made Nucleus untrustworthy as a system of
//! record:
//!   1. an indexed point lookup finds a row stored via `INSERT ... SELECT`
//!      (which yields `Int64`) using an `Int32` literal probe, and
//!   2. a duplicate PRIMARY KEY inserted at a different integer width is rejected.
//!
//! Before the fix: (1) returned empty on the disk engine and (2) let a duplicate
//! PK persist on the disk/memory engines — both silent.

#![cfg(feature = "server")]

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::types::Value;

const ENGINES: &[&str] = &["memory", "mvcc", "disk"];

fn make_db(engine: &str, tag: &str) -> Database {
    match engine {
        "memory" => Database::builder().memory().build().unwrap(),
        "mvcc" => Database::builder().mvcc().build().unwrap(),
        "disk" => {
            let path = std::env::temp_dir().join(format!("nucleus_tc_{tag}_disk.ndb"));
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(path.with_extension("wal"));
            let _ = std::fs::remove_dir_all(path.with_extension("wal.d"));
            Database::builder().disk(&path).build().unwrap()
        }
        other => panic!("unknown engine {other}"),
    }
}

fn select_rows(r: Vec<ExecResult>) -> Vec<Vec<Value>> {
    match r.into_iter().next_back().unwrap() {
        ExecResult::Select { rows, .. } => rows,
        o => panic!("expected SELECT, got {o:?}"),
    }
}

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        o => panic!("non-integer value {o:?}"),
    }
}

/// P0: a value stored as `Int64` (via `INSERT ... SELECT generate_series`) must be
/// found by an `Int32` literal probe on the primary key.
#[tokio::test]
async fn indexed_lookup_after_insert_select_finds_row() {
    for engine in ENGINES {
        let db = make_db(engine, "lookup");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        // generate_series yields Int64; stored into an INT PK column.
        db.execute("INSERT INTO t SELECT * FROM generate_series(1, 50)")
            .await
            .unwrap();

        // The bug: Int32(3) literal missed the Int64-keyed index entry.
        let r = select_rows(db.execute("SELECT id FROM t WHERE id = 3").await.unwrap());
        assert_eq!(
            r.len(),
            1,
            "[{engine}] WHERE id = 3 must find exactly one row"
        );
        assert_eq!(as_i64(&r[0][0]), 3, "[{engine}] found the wrong row");

        // Explicit-bigint probe must find the same row (parity).
        let rb = select_rows(
            db.execute("SELECT id FROM t WHERE id = CAST(3 AS BIGINT)")
                .await
                .unwrap(),
        );
        assert_eq!(
            rb.len(),
            1,
            "[{engine}] CAST(3 AS BIGINT) probe must also match"
        );

        // Full-table count is unaffected (was always correct).
        let c = select_rows(db.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(as_i64(&c[0][0]), 50, "[{engine}] count mismatch");
    }
}

/// Data-integrity blocker: a duplicate primary key inserted at a different integer
/// width (Int32 via VALUES, then Int64 via SELECT) must be rejected, not silently
/// persisted.
#[tokio::test]
async fn duplicate_pk_across_widths_is_rejected() {
    for engine in ENGINES {
        let db = make_db(engine, "dup");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (3)").await.unwrap(); // Int32(3)

        // Int64(3) via generate_series — the same logical key; must be rejected.
        let dup = db
            .execute("INSERT INTO t SELECT * FROM generate_series(3, 3)")
            .await;
        assert!(
            dup.is_err(),
            "[{engine}] duplicate PK (Int64 vs Int32) must be rejected, not persisted"
        );

        let c = select_rows(db.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            as_i64(&c[0][0]),
            1,
            "[{engine}] table must hold exactly one row after the rejected duplicate"
        );
    }
}

/// The reverse width order must also be rejected (Int64 first via SELECT, then
/// Int32 via VALUES).
#[tokio::test]
async fn duplicate_pk_reverse_width_order_is_rejected() {
    for engine in ENGINES {
        let db = make_db(engine, "dup_rev");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute("INSERT INTO t SELECT * FROM generate_series(5, 5)")
            .await
            .unwrap(); // Int64(5)

        let dup = db.execute("INSERT INTO t VALUES (5)").await; // Int32(5)
        assert!(
            dup.is_err(),
            "[{engine}] duplicate PK (Int32 vs Int64) must be rejected"
        );

        let c = select_rows(db.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(as_i64(&c[0][0]), 1, "[{engine}] must hold exactly one row");
    }
}

/// Metamorphic operator guard: the same logical value stored at two widths in one
/// column (Int32 via a bare literal, Int64 via CAST) must collapse under DISTINCT and
/// GROUP BY and be matched by an Int32 literal probe — on every engine. Guards the
/// hash/equality-keyed operator family (DISTINCT / GROUP BY / WHERE) against a width
/// regression. (Robust to per-column canonicalization: even if both rows canonicalize
/// to Int64, an Int32 literal probe must still match by value.)
#[tokio::test]
async fn distinct_groupby_and_probe_collapse_across_widths() {
    for engine in ENGINES {
        let db = make_db(engine, "metamorphic");
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, k BIGINT NOT NULL)")
            .await
            .unwrap();
        // k = 7 stored three ways: bare (Int32), CAST (Int64), bare (Int32) again.
        db.execute("INSERT INTO t VALUES (1, 7), (2, CAST(7 AS BIGINT)), (3, 7)")
            .await
            .unwrap();

        let d = select_rows(db.execute("SELECT DISTINCT k FROM t").await.unwrap());
        assert_eq!(
            d.len(),
            1,
            "[{engine}] DISTINCT k across widths must collapse to 1"
        );

        let g = select_rows(
            db.execute("SELECT k, COUNT(*) FROM t GROUP BY k")
                .await
                .unwrap(),
        );
        assert_eq!(
            g.len(),
            1,
            "[{engine}] GROUP BY k across widths must be 1 group"
        );
        assert_eq!(as_i64(&g[0][1]), 3, "[{engine}] group count must be 3");

        let w = select_rows(db.execute("SELECT id FROM t WHERE k = 7").await.unwrap());
        assert_eq!(
            w.len(),
            3,
            "[{engine}] Int32 literal probe must match all widths"
        );
    }
}

/// Storage-representation invariant: a value read back from a column has the column's
/// declared type regardless of the insert path — a small `Int32` literal placed in a
/// `BIGINT` column must read back as `Int64`, whether inserted through the constrained
/// (general) path or the constraint-free (fast) path. This is what canonicalization
/// buys over the comparison-layer fix: the physical representation itself is clean, so
/// no future raw-representation code path can reintroduce the width class.
#[tokio::test]
async fn stored_representation_matches_declared_type() {
    for engine in ENGINES {
        let db = make_db(engine, "reprinv");

        // Constrained table → general insert path.
        db.execute("CREATE TABLE c (id INT PRIMARY KEY, big BIGINT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO c VALUES (1, 10), (2, 20)")
            .await
            .unwrap();
        let rc = select_rows(db.execute("SELECT big FROM c ORDER BY id").await.unwrap());
        for row in &rc {
            assert!(
                matches!(row[0], Value::Int64(_)),
                "[{engine}] constrained BIGINT must read back Int64, got {:?}",
                row[0]
            );
        }

        // Constraint-free table → fast insert path.
        db.execute("CREATE TABLE f (big BIGINT)").await.unwrap();
        db.execute("INSERT INTO f VALUES (10), (20)").await.unwrap();
        let rf = select_rows(db.execute("SELECT big FROM f").await.unwrap());
        for row in &rf {
            assert!(
                matches!(row[0], Value::Int64(_)),
                "[{engine}] constraint-free (fast-path) BIGINT must read back Int64, got {:?}",
                row[0]
            );
        }
    }
}
