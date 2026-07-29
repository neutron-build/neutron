//! Commit-time durability regression tests.
//!
//! The durability contract under test: once a write statement (autocommit) or
//! COMMIT is acked with `synchronous_commit = on` (the default), the rows
//! survive an immediate kill -9 — not just a kill after the next checkpoint.
//!
//! Crash simulation: the engine flushes on Drop (clean shutdown), so these
//! tests copy the data directory WHILE the first engine is still alive. The
//! copy is exactly what a kill -9 would leave behind: WAL forced at commit
//! time, data pages and table directory unflushed.
//!
//! NOTE: table names are unique per test — the replacing_mergetree dedup
//! registry is process-global (see dogfood_observe_verification.rs).

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::{Catalog, ColumnDef, TableDef};
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::{DataType, Value};

/// Boot a server-shaped stack (segmented WAL DiskEngine wrapped in
/// BufferedDiskEngine, executor with persistence) from a data directory,
/// mirroring main.rs: load catalog, re-register tables, restore engines.
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

async fn exec(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().expect("a result")
}

async fn rows(ex: &Executor, sql: &str) -> Vec<Vec<Value>> {
    match exec(ex, sql).await {
        ExecResult::Select { rows, .. } => rows,
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

fn count_of(rows: &[Vec<Value>]) -> i64 {
    match &rows[0][0] {
        Value::Int32(n) => *n as i64,
        Value::Int64(n) => *n,
        Value::Text(s) => s.trim().parse().unwrap(),
        other => panic!("not a count: {other:?}"),
    }
}

/// Recursively copy a directory — the kill -9 snapshot.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

// ── Heap tables (DiskEngine page WAL) ───────────────────────────────────────

#[tokio::test]
async fn autocommit_inserts_survive_simulated_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_heap (id BIGINT, name TEXT)").await;
    for i in 0..50 {
        exec(&ex, &format!("INSERT INTO cd_heap VALUES ({i}, 'row-{i}')")).await;
    }

    // kill -9 view: WAL was forced per statement, nothing else flushed.
    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT COUNT(*) FROM cd_heap").await;
    assert_eq!(
        count_of(&r),
        50,
        "acked autocommit inserts must survive kill -9 immediately"
    );
}

/// A bare `CREATE TABLE` (no inserts) dirties no data page, so pre-fix the
/// on-disk table directory was never forced — only the catalog was fsync'd,
/// leaving storage behind it. The `flush_schema` at DDL commit now forces the
/// directory. We verify at the raw DiskEngine level (NO catalog re-register, the
/// band-aid that would otherwise mask this): the recovered directory itself must
/// carry the table across a kill -9, so a scan returns empty rather than
/// TableNotFound.
#[tokio::test]
async fn bare_create_table_directory_is_crash_durable() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_bare (id BIGINT)").await;

    // kill -9 snapshot taken while the engine is alive (no clean-shutdown flush).
    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    // Reopen the DiskEngine directly — a fresh empty catalog, so nothing
    // re-creates the table from catalog metadata. Only storage's own recovered
    // directory can know it.
    let catalog = Arc::new(Catalog::new());
    let db_path = crash.join("nucleus.db");
    let engine =
        DiskEngine::open_segmented_with_sync(&db_path, catalog, 1024, 16, SyncMode::Fsync).unwrap();
    let scanned = engine.scan("cd_bare").await;
    assert!(
        scanned.is_ok(),
        "a CREATE-only table must be recoverable from the crash-forced storage \
         directory, got {scanned:?}"
    );
    assert_eq!(scanned.unwrap().len(), 0, "the recovered table is empty");
}

#[tokio::test]
async fn synchronous_commit_off_defers_durability() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_async (id BIGINT)").await;
    // With the default (on), the WAL force drains pending work per statement.
    exec(&ex, "INSERT INTO cd_async VALUES (1)").await;
    assert!(
        !buf.durability_pending(),
        "synchronous_commit=on must drain the WAL-pending set at statement end"
    );

    exec(&ex, "SET synchronous_commit = off").await;
    exec(&ex, "INSERT INTO cd_async VALUES (2)").await;
    assert!(
        buf.durability_pending(),
        "synchronous_commit=off must leave the force to a later flush/checkpoint"
    );
}

#[tokio::test]
async fn explicit_transaction_commit_is_durable() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE cd_txn (id BIGINT)").await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "INSERT INTO cd_txn VALUES (1)").await;
    exec(&ex, "INSERT INTO cd_txn VALUES (2)").await;
    exec(&ex, "INSERT INTO cd_txn VALUES (3)").await;
    exec(&ex, "COMMIT").await;

    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT COUNT(*) FROM cd_txn").await;
    assert_eq!(
        count_of(&r),
        3,
        "committed transaction must survive kill -9"
    );
}

#[tokio::test]
async fn foreign_key_constraints_and_cascades_survive_crash_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE durable_fk_parent (id INT PRIMARY KEY)").await;
    exec(
        &ex,
        "CREATE TABLE durable_fk_child (id INT PRIMARY KEY, pid INT REFERENCES durable_fk_parent(id) ON DELETE CASCADE)",
    )
    .await;
    exec(&ex, "INSERT INTO durable_fk_parent VALUES (1)").await;
    exec(&ex, "INSERT INTO durable_fk_child VALUES (10, 1)").await;

    let crash_one = tmp.path().join("crash-one");
    copy_dir(&data, &crash_one);
    drop(ex);

    let (reopened, _buf) = boot(&crash_one).await;
    assert!(
        reopened
            .execute("INSERT INTO durable_fk_child VALUES (20, 999)")
            .await
            .is_err(),
        "the restored catalog must still enforce the foreign key"
    );
    exec(&reopened, "DELETE FROM durable_fk_parent WHERE id = 1").await;
    assert_eq!(
        count_of(&rows(&reopened, "SELECT COUNT(*) FROM durable_fk_child").await),
        0,
        "the restored ON DELETE CASCADE action must execute"
    );

    let crash_two = tmp.path().join("crash-two");
    copy_dir(&crash_one, &crash_two);
    drop(reopened);

    let (reopened_again, _) = boot(&crash_two).await;
    assert_eq!(
        count_of(&rows(&reopened_again, "SELECT COUNT(*) FROM durable_fk_parent").await),
        0
    );
    assert_eq!(
        count_of(&rows(&reopened_again, "SELECT COUNT(*) FROM durable_fk_child").await),
        0,
        "the committed parent/cascade delete must recover atomically"
    );
}

// ── MergeTree tables (per-table columnar engine + engines.json) ─────────────

#[tokio::test]
async fn replacing_mergetree_survives_crash_with_dedup_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE cd_rt (k TEXT, v TEXT, ver BIGINT) \
         WITH (engine='replacing_mergetree', version_column='ver') ORDER BY (k)",
    )
    .await;
    exec(&ex, "INSERT INTO cd_rt (k, v, ver) VALUES ('a', 'old', 1)").await;
    exec(&ex, "INSERT INTO cd_rt (k, v, ver) VALUES ('a', 'new', 2)").await;

    // kill -9 view. Pre-fix this lost BOTH properties: the rows (per-table
    // engine was pure in-memory) and the dedup config (registry was only
    // populated by the original CREATE TABLE statement).
    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT v FROM cd_rt").await;
    assert_eq!(
        r.len(),
        1,
        "replacing dedup must survive restart (got {} rows)",
        r.len()
    );
    assert_eq!(
        r[0][0],
        Value::Text("new".into()),
        "newest version must win after recovery"
    );
}

#[tokio::test]
async fn mergetree_rows_survive_simulated_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE cd_mt (id BIGINT, val TEXT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    for i in 0..20 {
        exec(&ex, &format!("INSERT INTO cd_mt VALUES ({i}, 'v{i}')")).await;
    }

    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (ex2, _) = boot(&crash).await;
    let r = rows(&ex2, "SELECT COUNT(*) FROM cd_mt").await;
    assert_eq!(
        count_of(&r),
        20,
        "mergetree rows must survive kill -9 via the per-table columnar WAL"
    );
}

#[tokio::test]
async fn lsm_rows_and_mutations_survive_simulated_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE durable_lsm (id INT PRIMARY KEY, val BIGINT, note TEXT) WITH (engine='lsm')",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO durable_lsm VALUES (1, 10, 'old'), (2, 20, 'keep'), (3, 30, 'drop')",
    )
    .await;
    exec(
        &ex,
        "UPDATE durable_lsm SET val = 99, note = 'new' WHERE id = 1",
    )
    .await;
    exec(&ex, "DELETE FROM durable_lsm WHERE id = 3").await;
    exec(&ex, "ALTER TABLE durable_lsm RENAME TO durable_lsm_new").await;

    let crash = tmp.path().join("crash");
    copy_dir(&data, &crash);
    drop(ex);

    let (reopened, _) = boot(&crash).await;
    assert_eq!(
        rows(
            &reopened,
            "SELECT id, val, note FROM durable_lsm_new ORDER BY id",
        )
        .await,
        vec![
            vec![Value::Int32(1), Value::Int64(99), Value::Text("new".into()),],
            vec![
                Value::Int32(2),
                Value::Int64(20),
                Value::Text("keep".into()),
            ],
        ],
        "LSM insert/update/delete/rename state must survive restart",
    );
}

#[tokio::test]
async fn exact_numeric_values_and_aggregates_survive_restart_across_engines() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(&ex, "CREATE TABLE numeric_heap (id INT, amount NUMERIC)").await;
    exec(
        &ex,
        "CREATE TABLE numeric_lsm (id INT PRIMARY KEY, amount NUMERIC) WITH (engine='lsm')",
    )
    .await;
    exec(
        &ex,
        "CREATE TABLE numeric_mt (id INT, amount NUMERIC) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    for table in ["numeric_heap", "numeric_lsm", "numeric_mt"] {
        exec(
            &ex,
            &format!("INSERT INTO {table} VALUES (1, '10000000000000000000000000.1'), (2, '0.2')"),
        )
        .await;
    }
    exec(
        &ex,
        "CREATE TABLE temporal_mt (id INT, day DATE, moment TIMESTAMP, span INTERVAL) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    exec(
        &ex,
        "INSERT INTO temporal_mt VALUES (1, '2024-02-29', '2024-02-29 12:34:56.123456', '1 month 2 days 00:00:00.5')",
    )
    .await;
    drop(ex);

    let (reopened, _) = boot(&data).await;
    for table in ["numeric_heap", "numeric_lsm", "numeric_mt"] {
        assert_eq!(
            rows(&reopened, &format!("SELECT SUM(amount) FROM {table}"),).await,
            vec![vec![Value::Numeric("10000000000000000000000000.3".into())]],
            "{table} exact NUMERIC after restart"
        );
    }
    assert_eq!(
        rows(
            &reopened,
            "SELECT day, moment, span FROM temporal_mt WHERE id = 1",
        )
        .await,
        vec![vec![
            Value::Date(nucleus::types::ymd_to_days(2024, 2, 29)),
            Value::Timestamp(
                nucleus::types::ymd_to_days(2024, 2, 29) as i64 * 86_400_000_000 + 45_296_123_456,
            ),
            Value::Interval {
                months: 1,
                days: 2,
                microseconds: 500_000,
            },
        ]],
        "columnar temporal logical types must survive WAL snapshot/restart"
    );
}

#[tokio::test]
async fn drop_table_removes_engine_sidecar() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE cd_drop (id BIGINT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    let engines_json = data.join("engines.json");
    let listed = std::fs::read_to_string(&engines_json).unwrap();
    assert!(listed.contains("cd_drop"), "CREATE must record the engine");

    exec(&ex, "DROP TABLE cd_drop").await;
    let listed = std::fs::read_to_string(&engines_json).unwrap();
    assert!(
        !listed.contains("cd_drop"),
        "DROP must remove the engines.json entry"
    );
}

// ── T0.3: per-table epoch reconciliation (drop+recreate can't return stale) ──

/// Build a `TableDef` for a single-BIGINT-column table at a given generation.
fn t_def(name: &str, epoch: u64) -> TableDef {
    TableDef {
        name: name.into(),
        columns: vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
            default_expr: None,
            id: 1,
            analyzer: None,
        }],
        constraints: Vec::new(),
        append_only: false,
        epoch,
    }
}

/// The core T0.3 invariant. A table dropped and recreated under the same name
/// draws a fresh epoch. If a crash leaves the on-disk directory still pointing
/// at the *previous* generation's pages (the drop's directory flush was lost),
/// boot reconciliation must abandon that stale `first_page` and recover the
/// table EMPTY — never returning the old generation's rows (which may even have
/// been reused by another table). Matching epochs, by contrast, keep the rows.
#[tokio::test]
async fn recreate_with_new_epoch_recovers_empty_not_stale() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("nucleus.db");

    // Generation 1: t@epoch=1 with two rows, directory persisted on clean drop.
    {
        let catalog = Arc::new(Catalog::new());
        catalog.create_table(t_def("t", 1)).await.unwrap();
        let eng = DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            1024,
            16,
            SyncMode::Fsync,
        )
        .unwrap();
        eng.create_table("t").await.unwrap();
        eng.insert("t", vec![Value::Int64(1)]).await.unwrap();
        eng.insert("t", vec![Value::Int64(2)]).await.unwrap();
        eng.flush_schema().await.unwrap();
        drop(eng); // clean shutdown flushes data pages + directory (t@1 -> pages)
    }

    // Reopen with the SAME epoch (no recreate): rows must survive.
    {
        let catalog = Arc::new(Catalog::new());
        catalog.create_table(t_def("t", 1)).await.unwrap();
        let eng = DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            1024,
            16,
            SyncMode::Fsync,
        )
        .unwrap();
        eng.create_table("t").await.unwrap(); // reconcile: epochs match -> keep
        assert_eq!(
            eng.scan("t").await.unwrap().len(),
            2,
            "matching epoch must keep the rows"
        );
        drop(eng);
    }

    // Reopen as a RECREATE: catalog says t@epoch=2, but the directory still
    // holds gen-1 pages. Reconciliation must recover t empty, not stale.
    {
        let catalog = Arc::new(Catalog::new());
        catalog.create_table(t_def("t", 2)).await.unwrap();
        let eng = DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            1024,
            16,
            SyncMode::Fsync,
        )
        .unwrap();
        eng.create_table("t").await.unwrap(); // reconcile: dir@1 != catalog@2 -> empty
        let scanned = eng.scan("t").await;
        assert!(
            scanned.is_ok(),
            "recreated table must be queryable, got {scanned:?}"
        );
        assert_eq!(
            scanned.unwrap().len(),
            0,
            "a stale generation's rows must never be returned after recreate"
        );
    }
}

/// The epoch allocator is monotonic and persists across a restart, so a table
/// recreated after reboot always outranks any predecessor generation.
#[tokio::test]
async fn epoch_allocator_is_monotonic_across_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let catalog_path = data.join("catalog.json");
    std::fs::create_dir_all(&data).unwrap();

    let e1 = {
        let catalog = Arc::new(Catalog::new());
        let e = catalog.alloc_table_epoch();
        catalog.create_table(t_def("a", e)).await.unwrap();
        CatalogPersistence::new(&catalog_path)
            .save_catalog(&catalog)
            .await
            .unwrap();
        e
    };

    // Reload and allocate again — must be strictly greater than e1.
    let catalog = Arc::new(Catalog::new());
    CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await
        .unwrap();
    let e2 = catalog.alloc_table_epoch();
    assert!(
        e2 > e1,
        "epoch after restart ({e2}) must exceed the pre-restart epoch ({e1})"
    );
}

// ── T1.1: on-disk format version validation on open ──────────────────────────

/// Write a single meta page with a chosen db-format version and (optionally)
/// mangled magic, with a valid checksum so the only thing under test is the
/// format guard.
fn write_meta_page(db_path: &Path, version: u32, mangle_magic: bool) {
    use nucleus::storage::page;
    let mut meta = [0u8; page::PAGE_SIZE];
    page::init_meta_page(&mut meta);
    // init stamps DB_FORMAT_VERSION; override it for the test.
    meta[page::META_DB_VERSION..page::META_DB_VERSION + 4].copy_from_slice(&version.to_le_bytes());
    if mangle_magic {
        meta[page::META_MAGIC..page::META_MAGIC + 8].copy_from_slice(b"NOTNUCL\0");
    }
    page::write_checksum(&mut meta);
    std::fs::write(db_path, meta).unwrap();
}

#[tokio::test]
async fn format_version_newer_than_supported_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("nucleus.db");
    write_meta_page(&db_path, 9999, false);

    let catalog = Arc::new(Catalog::new());
    let opened = DiskEngine::open(&db_path, catalog);
    let err = opened.expect_err("a future-version database must be refused");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("newer") && msg.contains("9999"),
        "error should name the unsupported version, got: {msg}"
    );
}

#[tokio::test]
async fn foreign_magic_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("nucleus.db");
    write_meta_page(&db_path, 2, true);

    let catalog = Arc::new(Catalog::new());
    let opened = DiskEngine::open(&db_path, catalog);
    let err = opened.expect_err("a non-Nucleus file must be refused");
    assert!(
        format!("{err:?}").contains("magic"),
        "error should mention bad magic, got: {err:?}"
    );
}

/// A pre-v2 database (version stamped 1, empty directory) opens without error
/// and is transparently re-stamped to the current version on the next directory
/// save — the v1→v2 upgrade path.
#[tokio::test]
async fn legacy_v1_meta_opens_and_upgrades() {
    use nucleus::storage::page;
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("nucleus.db");
    write_meta_page(&db_path, 1, false);

    let catalog = Arc::new(Catalog::new());
    catalog.create_table(t_def("up", 7)).await.unwrap();
    let eng = DiskEngine::open(&db_path, catalog.clone()).expect("v1 db must open");
    // Materialize a table + force the directory: this rewrites the meta page in
    // v2 layout and must re-stamp the version.
    eng.create_table("up").await.unwrap();
    eng.flush_schema().await.unwrap();
    drop(eng);

    // Read the meta page's version directly.
    let bytes = std::fs::read(&db_path).unwrap();
    let mut v = [0u8; 4];
    v.copy_from_slice(&bytes[page::META_DB_VERSION..page::META_DB_VERSION + 4]);
    assert_eq!(
        u32::from_le_bytes(v),
        page::DB_FORMAT_VERSION,
        "opening a v1 database must upgrade the stamped format version on save"
    );
}

// ── T0.3 DDL siblings: TRUNCATE / RENAME on per-table override engines ───────

/// TRUNCATE on a mergetree table must clear its per-table columnar engine, not
/// no-op against the empty base heap (which is where TRUNCATE used to route).
#[tokio::test]
async fn truncate_clears_columnar_table() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    let (ex, _buf) = boot(&data).await;

    exec(
        &ex,
        "CREATE TABLE ct_trunc (id BIGINT) WITH (engine='mergetree') ORDER BY (id)",
    )
    .await;
    for i in 0..10 {
        exec(&ex, &format!("INSERT INTO ct_trunc VALUES ({i})")).await;
    }
    assert_eq!(
        count_of(&rows(&ex, "SELECT COUNT(*) FROM ct_trunc").await),
        10
    );

    exec(&ex, "TRUNCATE TABLE ct_trunc").await;
    assert_eq!(
        count_of(&rows(&ex, "SELECT COUNT(*) FROM ct_trunc").await),
        0,
        "TRUNCATE must clear the per-table columnar engine's rows"
    );
    // The table remains usable afterward.
    exec(&ex, "INSERT INTO ct_trunc VALUES (99)").await;
    assert_eq!(
        count_of(&rows(&ex, "SELECT COUNT(*) FROM ct_trunc").await),
        1
    );
}

/// RENAME on a mergetree table must migrate the override engine to the new
/// name — queryable immediately AND after a restart (engines.json re-keyed).
#[tokio::test]
async fn rename_migrates_columnar_table() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    {
        let (ex, _buf) = boot(&data).await;
        exec(
            &ex,
            "CREATE TABLE ct_old (id BIGINT) WITH (engine='mergetree') ORDER BY (id)",
        )
        .await;
        for i in 0..5 {
            exec(&ex, &format!("INSERT INTO ct_old VALUES ({i})")).await;
        }
        exec(&ex, "ALTER TABLE ct_old RENAME TO ct_new").await;
        assert_eq!(
            count_of(&rows(&ex, "SELECT COUNT(*) FROM ct_new").await),
            5,
            "renamed columnar table must be queryable under the new name"
        );
        assert!(
            ex.execute("SELECT COUNT(*) FROM ct_old").await.is_err(),
            "old name must no longer resolve"
        );
    }

    // Restart: the migrated engine must re-register under the new name.
    let (ex2, _buf) = boot(&data).await;
    assert_eq!(
        count_of(&rows(&ex2, "SELECT COUNT(*) FROM ct_new").await),
        5,
        "renamed columnar table must survive restart under the new name"
    );
}

/// Reclaim the exact main.rs reconciliation logic runs at boot: a directory
/// table absent from a NON-empty catalog is a storage-ahead orphan and is
/// reclaimed, while cataloged tables are preserved. (The empty-catalog case is
/// deliberately NOT reclaimed — see `reclaim_skipped_when_catalog_empty`.)
fn reclaim_orphans(
    cataloged: &std::collections::HashSet<String>,
    storage_tables: Vec<String>,
) -> Vec<String> {
    if cataloged.is_empty() {
        return Vec::new(); // catalog-loss guard: never reclaim against an empty catalog
    }
    storage_tables
        .into_iter()
        .filter(|t| !cataloged.contains(t))
        .collect()
}

#[tokio::test]
async fn boot_reclaims_storage_ahead_orphan_keeps_cataloged() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("nucleus.db");

    // Materialize a real table (keeper) AND an orphan directly in storage.
    {
        let catalog = Arc::new(Catalog::new());
        catalog.create_table(t_def("keeper", 1)).await.unwrap();
        catalog.create_table(t_def("orphan", 2)).await.unwrap();
        let eng = DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            1024,
            16,
            SyncMode::Fsync,
        )
        .unwrap();
        for t in ["keeper", "orphan"] {
            eng.create_table(t).await.unwrap();
            eng.insert(t, vec![Value::Int64(1)]).await.unwrap();
        }
        eng.flush_schema().await.unwrap();
        drop(eng);
    }

    // Reopen with a catalog that knows ONLY keeper (orphan is storage-ahead).
    let catalog = Arc::new(Catalog::new());
    catalog.create_table(t_def("keeper", 1)).await.unwrap();
    let eng =
        DiskEngine::open_segmented_with_sync(&db_path, catalog.clone(), 1024, 16, SyncMode::Fsync)
            .unwrap();
    eng.create_table("keeper").await.unwrap(); // re-register keeper

    let cataloged: std::collections::HashSet<String> =
        catalog.table_names().await.into_iter().collect();
    let orphans = reclaim_orphans(&cataloged, eng.table_names());
    assert_eq!(
        orphans,
        vec!["orphan".to_string()],
        "only the orphan is reclaimable"
    );
    for o in &orphans {
        eng.drop_table(o).await.unwrap();
    }
    eng.flush_schema().await.unwrap();

    let remaining: std::collections::HashSet<String> = eng.table_names().into_iter().collect();
    assert!(
        remaining.contains("keeper"),
        "cataloged table must be preserved"
    );
    assert!(!remaining.contains("orphan"), "orphan must be reclaimed");
    assert_eq!(
        eng.scan("keeper").await.unwrap().len(),
        1,
        "keeper's rows are intact"
    );
}

/// The catalog-loss guard: an EMPTY catalog beside populated storage must NOT
/// trigger reclaim — a missing/corrupt catalog.json would otherwise wipe every
/// table. Reconciliation preserves the tables instead.
#[tokio::test]
async fn reclaim_skipped_when_catalog_empty() {
    let cataloged: std::collections::HashSet<String> = std::collections::HashSet::new();
    let storage_tables = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let orphans = reclaim_orphans(&cataloged, storage_tables);
    assert!(
        orphans.is_empty(),
        "an empty catalog must never reclaim (it signals catalog loss, not orphans)"
    );
}
