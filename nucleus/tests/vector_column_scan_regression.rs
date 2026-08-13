//! A `VECTOR` column with no declared dimension made its table's rows
//! unreadable, and the scan hid it.
//!
//! `convert_data_type` maps a bare `VECTOR` to `Vector(0)` — documented as
//! "dimension unknown" — but the tuple decoder read that 0 as a requirement
//! and rejected every tuple the column had itself written. The scan logged
//! "failed to deserialize tuple ... row omitted from scan" and CONTINUED, so
//! the table answered two different row counts depending on which path served
//! the query:
//!
//!     SELECT id FROM t     -> every row   (index-only, never touches the heap)
//!     SELECT COUNT(*)      -> every row
//!     SELECT * FROM t      -> NOTHING
//!     SELECT id, meta      -> NOTHING     (no vector column in the projection)
//!     UPDATE ... WHERE id  -> UPDATE 0
//!
//! No error reached the client in any of those. It was reported as
//! "VECTOR_DISTANCE in a select list returns zero rows", which is why the
//! assertions below deliberately use no vector function at all: the distance
//! call was a passenger, and `SELECT *` loses the data just as completely.
//!
//! These tests run against the DISK engine on purpose. `tests/` helpers and
//! `executor::tests` build on `MemoryEngine`, which never goes through
//! `storage::tuple`, so no test on that path could have caught this.
#![cfg(feature = "server")]

use nucleus::embedded::Database;
use nucleus::executor::ExecResult;

fn row_count(r: Vec<ExecResult>) -> usize {
    match r.into_iter().next_back().unwrap() {
        ExecResult::Select { rows, .. } => rows.len(),
        o => panic!("expected a SELECT result, got {o:?}"),
    }
}

async fn seeded(path: &std::path::Path, decl: &str) -> Database {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("wal"));
    let db = Database::builder().disk(path).build().unwrap();
    db.execute(&format!(
        "CREATE TABLE vprobe (id TEXT PRIMARY KEY, embedding {decl}, metadata JSONB)"
    ))
    .await
    .unwrap();
    for (id, v) in [("v1", "[1,0,0]"), ("v2", "[0,1,0]")] {
        db.execute(&format!(
            "INSERT INTO vprobe (id, embedding, metadata) \
             VALUES ('{id}', VECTOR('{v}'), '{{}}'::jsonb)"
        ))
        .await
        .unwrap();
    }
    db
}

/// Index-only reads and heap reads must agree about how many rows exist.
/// Disagreeing silently is the failure this pins.
#[tokio::test]
async fn dimensionless_vector_column_does_not_hide_rows_from_the_heap() {
    let path = std::env::temp_dir().join("nucleus_vector_dimensionless.ndb");
    let db = seeded(&path, "VECTOR").await;

    let by_index = row_count(db.execute("SELECT id FROM vprobe").await.unwrap());
    assert_eq!(by_index, 2, "index-only read");

    for sql in [
        "SELECT * FROM vprobe",
        "SELECT id, metadata FROM vprobe",
        "SELECT embedding FROM vprobe",
        "SELECT id, embedding, metadata FROM vprobe",
        "SELECT id FROM vprobe WHERE metadata IS NOT NULL",
    ] {
        assert_eq!(
            row_count(db.execute(sql).await.unwrap()),
            by_index,
            "`{sql}` disagrees with `SELECT id` about the row count; a heap read \
             must not silently return fewer rows than an index-only read"
        );
    }

    // The reported symptom, which was a consequence rather than the cause.
    assert_eq!(
        row_count(
            db.execute(
                "SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), 'cosine') FROM vprobe"
            )
            .await
            .unwrap()
        ),
        2,
    );
    assert_eq!(
        row_count(
            db.execute("SELECT id, VECTOR_DIMS(embedding) FROM vprobe")
                .await
                .unwrap()
        ),
        2,
    );
}

/// Writes must find the rows too — `UPDATE ... WHERE id = 'v1'` reported
/// `UPDATE 0` against a table that plainly contained `v1`.
#[tokio::test]
async fn dimensionless_vector_column_does_not_hide_rows_from_dml() {
    let path = std::env::temp_dir().join("nucleus_vector_dimensionless_dml.ndb");
    let db = seeded(&path, "VECTOR").await;

    let updated = match db
        .execute("UPDATE vprobe SET embedding = VECTOR('[2,0,0]') WHERE id = 'v1'")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Command { rows_affected, .. } => rows_affected,
        o => panic!("expected a command result, got {o:?}"),
    };
    assert_eq!(updated, 1, "UPDATE matched no row");
    assert_eq!(
        row_count(db.execute("SELECT * FROM vprobe").await.unwrap()),
        2
    );
}

/// Every distance metric the engine accepts, on a table it can actually read.
/// A declared dimension always worked, which is why the defect read as a
/// vector-search bug rather than a storage one.
#[tokio::test]
async fn declared_dimension_behaves_the_same_as_no_dimension() {
    let path = std::env::temp_dir().join("nucleus_vector_declared_dim.ndb");
    let db = seeded(&path, "VECTOR(3)").await;

    for metric in ["cosine", "l2", "inner"] {
        assert_eq!(
            row_count(
                db.execute(&format!(
                    "SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'), '{metric}') \
                     FROM vprobe"
                ))
                .await
                .unwrap()
            ),
            2,
            "metric {metric}"
        );
    }
    assert_eq!(
        row_count(db.execute("SELECT * FROM vprobe").await.unwrap()),
        2
    );
}
