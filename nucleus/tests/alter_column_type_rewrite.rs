//! ALTER TABLE ... ALTER COLUMN ... TYPE must rewrite stored values so the
//! physical representation matches the new declared type (teploy-observe D-7).
//! Otherwise a columnar/MergeTree table reconstructs values from the old
//! physical ColumnData and the catalog/storage types silently diverge.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{ColumnarStorageEngine, DiskEngine, StorageEngine};
use nucleus::types::Value;

async fn fresh() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(ColumnarStorageEngine::new());
    Arc::new(Executor::new(catalog, storage))
}
async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().unwrap()
}
async fn select_rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Boot a server-shaped stack (segmented-WAL DiskEngine wrapped in
/// BufferedDiskEngine, executor with catalog persistence), mirroring main.rs —
/// the disk engine caches its column schema, so these variants catch a
/// rewrite that serializes against the stale cached shape.
async fn boot(data: &Path) -> (Arc<Executor>, Arc<BufferedDiskEngine>) {
    std::fs::create_dir_all(data).unwrap();
    let catalog = Arc::new(Catalog::new());
    let catalog_path = data.join("catalog.json");
    let _ = CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;
    let db_path = data.join("nucleus.db");
    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(&db_path, catalog.clone(), 1024, 16, SyncMode::Fsync)
            .unwrap(),
    );
    for table in catalog.table_names().await {
        let _ = engine.create_table(&table).await;
    }
    let buffered = Arc::new(BufferedDiskEngine::new(engine));
    let exec = Arc::new(Executor::new_with_persistence(
        catalog,
        buffered.clone() as Arc<dyn StorageEngine>,
        Some(catalog_path),
        Some(data),
    ));
    exec.restore_table_engines().await;
    (exec, buffered)
}

#[tokio::test]
async fn alter_text_to_bigint_rewrites_stored_values() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE t (id INT, v TEXT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO t (id, v) VALUES (1, '10'), (2, '20')").await;

    exec(&ex, "ALTER TABLE t ALTER COLUMN v TYPE BIGINT").await;

    // Values must read back as integers, and numeric aggregates must work.
    let rows = select_rows(&ex, "SELECT v FROM t").await;
    let mut got: Vec<Value> = rows.into_iter().map(|r| r[0].clone()).collect();
    got.sort_by_key(|v| match v {
        Value::Int64(n) => *n,
        _ => panic!("expected Int64 after ALTER, got {v:?}"),
    });
    assert_eq!(got, vec![Value::Int64(10), Value::Int64(20)]);

    let sum = select_rows(&ex, "SELECT SUM(v) FROM t").await;
    assert_eq!(sum[0][0], Value::Int64(30));
}

#[tokio::test]
async fn alter_to_incompatible_type_is_rejected() {
    let ex = fresh().await;
    exec(
        &ex,
        "CREATE TABLE t (id INT, v TEXT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'not_a_number')").await;

    // A value that cannot be cast must abort the ALTER with an error rather than
    // silently diverging the catalog from storage.
    let res = ex.execute("ALTER TABLE t ALTER COLUMN v TYPE BIGINT").await;
    assert!(res.is_err(), "ALTER over an uncastable value must error");

    // The column must still read back as its original TEXT value (unchanged).
    let rows = select_rows(&ex, "SELECT v FROM t").await;
    assert_eq!(rows[0][0], Value::Text("not_a_number".into()));
}

// ── CAT-6: float→int retypes round (1.2 and 1.4 both become 1), and the
// retype never revalidated constraints over the post-cast rows — a FLOAT PK
// silently became a duplicate-PK INT table. ─────────────────────────────────

#[tokio::test]
async fn float_to_int_retype_colliding_pk_is_rejected() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (v FLOAT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO t VALUES (1.2), (1.4)").await;

    let res = ex.execute("ALTER TABLE t ALTER COLUMN v TYPE INT").await;
    let err = res.expect_err("colliding PK after rounding must abort the retype");
    assert!(
        err.to_string().to_lowercase().contains("collide"),
        "got: {err}"
    );

    // The refused retype must leave the table untouched.
    let rows = select_rows(&ex, "SELECT COUNT(*) FROM t").await;
    assert_eq!(rows[0][0], Value::Int64(2));
}

#[tokio::test]
async fn float_to_int_retype_non_colliding_succeeds() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (v FLOAT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO t VALUES (1.2), (1.7)").await;

    exec(&ex, "ALTER TABLE t ALTER COLUMN v TYPE INT").await;
    let rows = select_rows(&ex, "SELECT v FROM t ORDER BY v").await;
    assert_eq!(rows[0][0], Value::Int64(1));
    assert_eq!(rows[1][0], Value::Int64(2));
}

#[tokio::test]
async fn float_to_int_retype_colliding_unique_is_rejected() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (id INT, v FLOAT UNIQUE)").await;
    exec(&ex, "INSERT INTO t VALUES (1, 1.2), (2, 1.4)").await;

    let res = ex.execute("ALTER TABLE t ALTER COLUMN v TYPE INT").await;
    assert!(
        res.is_err(),
        "colliding UNIQUE after rounding must abort the retype"
    );
}

#[tokio::test]
async fn float_to_int_retype_violating_check_is_rejected() {
    let ex = fresh().await;
    // 1.2 and 1.4 satisfy `v <> 1` as floats; both round to 1 as ints.
    exec(&ex, "CREATE TABLE t (v FLOAT CHECK (v <> 1))").await;
    exec(&ex, "INSERT INTO t VALUES (1.2)").await;
    exec(&ex, "INSERT INTO t VALUES (1.4)").await;

    let res = ex.execute("ALTER TABLE t ALTER COLUMN v TYPE INT").await;
    let err = res.expect_err("a CHECK violated only after rounding must abort");
    assert!(
        err.to_string().to_lowercase().contains("check"),
        "got: {err}"
    );
}

#[tokio::test]
async fn numeric_to_int_retype_rounds_like_cast() {
    let ex = fresh().await;
    exec(&ex, "CREATE TABLE t (v NUMERIC)").await;
    exec(&ex, "INSERT INTO t VALUES (1.5)").await;

    // Pre-fix this aborted with a parse error where CAST('1.5' AS INT) = 2.
    exec(&ex, "ALTER TABLE t ALTER COLUMN v TYPE INT").await;
    let rows = select_rows(&ex, "SELECT v FROM t").await;
    assert_eq!(rows[0][0], Value::Int64(2));
}

// ── CAT-2: DROP COLUMN / ALTER COLUMN TYPE on the DISK engine skipped
// sync_schema — the rewrite serialized rows against the engine's STALE cached
// column schema. DROP COLUMN wrote N-1-wide tuples that N-wide cached reads
// failed to deserialize (every row silently vanished until restart);
// ALTER COLUMN TYPE serialized recast values under the old type (Int64→Int32
// wrap, Int64-bytes-as-Text garbage) and the DDL-commit flush_schema then
// persisted the stale schema. ─────────────────────────────────────────────

#[tokio::test]
async fn disk_drop_column_rows_survive_and_reopen_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE cat2_drop (id BIGINT, a TEXT, tag TEXT)").await;
        exec(
            &ex,
            "INSERT INTO cat2_drop VALUES (1, 'alpha_one', 'keep_a'), (2, 'alpha_two', 'keep_b')",
        )
        .await;

        exec(&ex, "ALTER TABLE cat2_drop DROP COLUMN a").await;

        // Pre-restart: rows must still read back (pre-fix: zero rows — the
        // narrowed tuples no longer deserialized under the stale 3-wide cache).
        let rows = select_rows(&ex, "SELECT * FROM cat2_drop ORDER BY id").await;
        assert_eq!(
            rows,
            vec![
                vec![Value::Int64(1), Value::Text("keep_a".into())],
                vec![Value::Int64(2), Value::Text("keep_b".into())],
            ],
            "DROP COLUMN must keep every surviving row readable with its own values"
        );
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT * FROM cat2_drop ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1), Value::Text("keep_a".into())],
            vec![Value::Int64(2), Value::Text("keep_b".into())],
        ],
        "post-reopen: dropped-column rows must be intact, not vanished or shifted"
    );
}

#[tokio::test]
async fn disk_drop_middle_column_keeps_trailing_values_unshifted() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(
            &ex,
            "CREATE TABLE cat2_mid (id BIGINT, gone TEXT, tail TEXT)",
        )
        .await;
        exec(
            &ex,
            "INSERT INTO cat2_mid VALUES (7, 'drop_me', 'tail_seven'), (8, 'drop_me_too', 'tail_eight')",
        )
        .await;

        exec(&ex, "ALTER TABLE cat2_mid DROP COLUMN gone").await;

        let rows = select_rows(&ex, "SELECT id, tail FROM cat2_mid ORDER BY id").await;
        assert_eq!(
            rows,
            vec![
                vec![Value::Int64(7), Value::Text("tail_seven".into())],
                vec![Value::Int64(8), Value::Text("tail_eight".into())],
            ],
            "dropping a MIDDLE column must not shift trailing values under wrong names"
        );
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT id, tail FROM cat2_mid ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(7), Value::Text("tail_seven".into())],
            vec![Value::Int64(8), Value::Text("tail_eight".into())],
        ],
        "post-reopen: middle-drop content identity"
    );
}

#[tokio::test]
async fn disk_int_to_bigint_retype_wide_values_survive_insert_and_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE cat2_retype (id BIGINT, v INT)").await;
        exec(&ex, "INSERT INTO cat2_retype VALUES (1, 100000), (2, 42)").await;

        exec(&ex, "ALTER TABLE cat2_retype ALTER COLUMN v TYPE BIGINT").await;

        let rows = select_rows(&ex, "SELECT v FROM cat2_retype ORDER BY id").await;
        assert_eq!(
            rows,
            vec![vec![Value::Int64(100000)], vec![Value::Int64(42)]],
            "post-retype read must return the exact stored values"
        );

        // A fresh INSERT after the retype must serialize under the NEW shape
        // (pre-fix: the stale Int32 cache coerced 9000000000 via `*n as i32`
        // and it silently wrapped to 410065408).
        exec(&ex, "INSERT INTO cat2_retype VALUES (3, 9000000000)").await;
        let rows = select_rows(&ex, "SELECT id, v FROM cat2_retype ORDER BY id").await;
        assert_eq!(
            rows,
            vec![
                vec![Value::Int64(1), Value::Int64(100000)],
                vec![Value::Int64(2), Value::Int64(42)],
                vec![Value::Int64(3), Value::Int64(9_000_000_000)],
            ],
            "an INSERT issued after the retype must not wrap wide values"
        );
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT id, v FROM cat2_retype ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Int64(1), Value::Int64(100000)],
            vec![Value::Int64(2), Value::Int64(42)],
            vec![Value::Int64(3), Value::Int64(9_000_000_000)],
        ],
        "post-reopen: the retyped column must read back intact — pre-fix the \
         stale Int32-serialized rows no longer decoded under the BIGINT schema"
    );
}

#[tokio::test]
async fn disk_text_to_bigint_retype_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");

    {
        let (ex, buffered) = boot(&data).await;
        exec(&ex, "CREATE TABLE cat2_t2b (id BIGINT, v TEXT)").await;
        exec(
            &ex,
            "INSERT INTO cat2_t2b VALUES (1, '12345678901'), (2, '77')",
        )
        .await;

        exec(&ex, "ALTER TABLE cat2_t2b ALTER COLUMN v TYPE BIGINT").await;

        let rows = select_rows(&ex, "SELECT v FROM cat2_t2b ORDER BY id").await;
        assert_eq!(
            rows,
            vec![vec![Value::Int64(12345678901)], vec![Value::Int64(77)]],
            "TEXT→BIGINT retype must read back the parsed integers (pre-fix: \
             Int64 bytes serialized under the stale Text schema were garbage \
             or the rows vanished)"
        );
        buffered.checkpoint().await.unwrap();
    }

    let (ex2, _) = boot(&data).await;
    let rows = select_rows(&ex2, "SELECT v FROM cat2_t2b ORDER BY id").await;
    assert_eq!(
        rows,
        vec![vec![Value::Int64(12345678901)], vec![Value::Int64(77)]],
        "post-reopen: TEXT→BIGINT content identity"
    );
}
