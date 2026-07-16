//! T2.1 — logical (SQL-text) backup round-trip.
//!
//! Dump a populated instance to portable SQL, replay it into a FRESH instance,
//! and assert row-for-row equality — plus that reconstructed schema (PK
//! constraints, indexes) is actually live after restore.

use super::*;

async fn all_rows(ex: &Executor, sql: &str) -> Vec<Row> {
    match &exec(ex, sql).await[0] {
        ExecResult::Select { rows, .. } => rows.clone(),
        other => panic!("expected SELECT, got {other:?}"),
    }
}

#[tokio::test]
async fn logical_dump_round_trips_data_across_types() {
    let src = test_executor();
    exec(
        &src,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT, note TEXT)",
    )
    .await;
    exec(&src, "INSERT INTO users VALUES (1, 'alice', 9.5, 'hi')").await;
    // Embedded single quote + a NULL column must survive the literal emitter.
    exec(&src, "INSERT INTO users VALUES (2, 'o''brien', 0.0, NULL)").await;
    exec(&src, "INSERT INTO users VALUES (3, 'bob', -1.25, 'multi word')").await;

    let script = src.dump_logical().await.expect("dump");

    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    let a = all_rows(&src, "SELECT * FROM users ORDER BY id").await;
    let b = all_rows(&dst, "SELECT * FROM users ORDER BY id").await;
    assert_eq!(a, b, "restored rows must match source exactly");
}

#[tokio::test]
async fn logical_dump_restores_live_primary_key() {
    let src = test_executor();
    exec(&src, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    exec(&src, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    // The PK constraint must be enforced in the restored instance.
    let dup = dst.execute("INSERT INTO t VALUES (1, 'dup')").await;
    assert!(
        matches!(dup, Err(ExecError::ConstraintViolation(_))),
        "restored PK must reject a duplicate, got {dup:?}"
    );
}

#[tokio::test]
async fn logical_dump_round_trips_vector_index() {
    let src = test_executor();
    exec(&src, "CREATE TABLE emb (id INT PRIMARY KEY, v VECTOR(3))").await;
    exec(
        &src,
        "INSERT INTO emb VALUES (1, VECTOR('[1,0,0]')), (2, VECTOR('[0,1,0]')), (3, VECTOR('[0,0,1]'))",
    )
    .await;
    exec(&src, "CREATE INDEX emb_v ON emb USING hnsw (v)").await;

    let script = src.dump_logical().await.expect("dump");
    let dst = test_executor();
    dst.restore_logical(&script).await.expect("restore");

    // Data round-trips.
    let a = all_rows(&src, "SELECT id FROM emb ORDER BY id").await;
    let b = all_rows(&dst, "SELECT id FROM emb ORDER BY id").await;
    assert_eq!(a, b);

    // The restored HNSW index answers a KNN query (row 1 nearest to its own vec).
    let knn = all_rows(
        &dst,
        "SELECT id FROM emb ORDER BY VECTOR_DISTANCE(v, VECTOR('[1,0,0]'), 'l2') ASC LIMIT 1",
    )
    .await;
    assert_eq!(knn.first().and_then(|r| r.first()), Some(&Value::Int32(1)));
}
