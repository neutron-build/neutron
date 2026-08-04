//! Regression: embedded durable-MVCC recovery must restore CONSTRAINTS, not
//! just column schemas. Recovery previously rebuilt TableDefs from the WAL's
//! schema records (name + columns only), so a PRIMARY KEY silently stopped
//! being enforced after close+reopen — a duplicate key was accepted. The
//! catalog now persists to catalog.json in embedded durable mode too and is
//! restored (with constraints) before WAL schema registration.
#![cfg(feature = "server")]
use nucleus::embedded::Database;

#[tokio::test]
async fn pk_survives_durable_mvcc_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").await.unwrap();
        let dup = db.execute("INSERT INTO t VALUES (1, 'b')").await;
        eprintln!("dup before reopen rejected: {:?}", dup.is_err());
        assert!(dup.is_err(), "duplicate must be rejected before reopen");
        db.sync().unwrap();
    }
    let db = Database::durable_mvcc(dir.path()).unwrap();
    let dup = db.execute("INSERT INTO t VALUES (1, 'c')").await;
    eprintln!("dup after reopen rejected: {:?}", dup.is_err());
    assert!(
        dup.is_err(),
        "duplicate must be rejected after reopen (PK lost in recovery?)"
    );
}

#[tokio::test]
async fn serial_sequence_survives_durable_mvcc_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Database::durable_mvcc(dir.path()).unwrap();
        db.execute("CREATE TABLE s (id SERIAL PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO s (v) VALUES ('a'), ('b'), ('c')")
            .await
            .unwrap();
        db.sync().unwrap();
    }
    let db = Database::durable_mvcc(dir.path()).unwrap();
    // The sequence must resume past 3 — a reset-to-1 sequence would collide
    // with the recovered rows' PKs and fail (or, without PK recovery, silently
    // duplicate).
    db.execute("INSERT INTO s (v) VALUES ('d')").await.unwrap();
    let rows = db.query("SELECT COUNT(*) FROM s").await.unwrap();
    assert_eq!(rows[0][0], nucleus::types::Value::Int64(4));
}
