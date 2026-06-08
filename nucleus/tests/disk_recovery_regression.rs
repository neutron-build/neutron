//! Regression for finding #5: disk-mode tables must survive a reopen.
//!
//! DiskEngine::open restored the engine's internal table map from the on-disk
//! directory, but the directory stored only column TYPES (no names) and nothing
//! repopulated the CATALOG. So after reopening a disk-backed database the tables
//! existed physically but were invisible to SQL (`SELECT ... FROM t` →
//! TableNotFound), and even an engine-level restore couldn't reconstruct column
//! names. Fixed by persisting column names in the table directory and having the
//! embedded builder repopulate the catalog from DiskEngine::recovered_schemas()
//! (the DurableMvcc path already did the equivalent via its WAL).
#![cfg(feature = "server")]
use nucleus::embedded::Database;
use nucleus::executor::ExecResult;
use nucleus::types::Value;

#[tokio::test]
async fn disk_mode_table_and_data_survive_reopen() {
    let path = std::env::temp_dir().join("nucleus_disk_recovery_regression.ndb");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));

    {
        let db = Database::builder().disk(&path).build().unwrap();
        db.execute("CREATE TABLE acct (id INTEGER PRIMARY KEY, name TEXT, bal INTEGER)")
            .await
            .unwrap();
        db.execute("INSERT INTO acct VALUES (1,'alice',100),(2,'bob',200)")
            .await
            .unwrap();
        let _ = db.sync();
    }

    // Reopen: the table must be queryable by its real column names, with data.
    let db = Database::builder().disk(&path).build().unwrap();
    let rows = match db
        .execute("SELECT id, name, bal FROM acct ORDER BY id")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows,
        o => panic!("expected select, got {o:?}"),
    };
    assert_eq!(rows.len(), 2, "both rows must survive reopen");
    assert!(matches!(rows[0][0], Value::Int32(1) | Value::Int64(1)));
    assert!(matches!(&rows[0][1], Value::Text(s) if s == "alice"));
    assert!(matches!(rows[1][2], Value::Int32(200) | Value::Int64(200)));

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal"));
}
