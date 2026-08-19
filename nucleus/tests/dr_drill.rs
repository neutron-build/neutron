//! An automated disaster-recovery drill: back up and restore through the real
//! `nucleus` binary, then prove every model's data came back.
//!
//! M4's fourth checkbox is "automated DR tests", and `DURABILITY.md` said
//! outright that nothing scheduled a restore-and-verify — "a backup you have
//! never restored is a hypothesis". `backup_restore_all_models` closes the
//! logical half but calls `backup_data_dir`/`restore_data_dir` as library
//! functions. What an operator runs during a recovery is the COMMAND: its
//! argument parsing, its exit code, its refusal to touch a non-empty
//! destination, and the report they read to decide whether it worked.
//!
//! So this drives `CARGO_BIN_EXE_nucleus` as a subprocess, exactly as
//! `pitr_cli_roundtrip` does for `restore-pitr`, and then opens the directory
//! the command produced and reads every model back out of it.
//!
//! It runs in the `integration` job on every Nucleus change and on that
//! workflow's weekly schedule, which is what makes it a drill rather than a
//! test someone remembers to run.

#![cfg(feature = "server")]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::Value;

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

async fn run(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql).await.expect(sql).pop().expect("a result")
}

async fn scalar(ex: &Executor, sql: &str) -> String {
    match run(ex, sql).await {
        ExecResult::Select { rows, .. } => match rows.first().and_then(|r| r.first()) {
            Some(Value::Text(s)) => s.clone(),
            Some(Value::Int32(n)) => n.to_string(),
            Some(Value::Int64(n)) => n.to_string(),
            Some(Value::Float64(f)) => f.to_string(),
            Some(Value::Null) | None => String::new(),
            Some(other) => format!("{other:?}"),
        },
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

/// Run the real binary and return `(stdout+stderr, success)`.
fn nucleus(args: &[&str]) -> (String, bool) {
    let out = Command::new(env!("CARGO_BIN_EXE_nucleus"))
        .args(args)
        .output()
        .expect("failed to run the nucleus binary");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (text, out.status.success())
}

#[tokio::test]
async fn dr_drill_backup_and_restore_through_the_cli() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("live");
    let snapshot = tmp.path().join("snap");
    let restored = tmp.path().join("restored");

    let reads: Vec<(&'static str, String)>;
    let before: Vec<(&'static str, String)>;
    {
        let (ex, _eng) = boot(&data).await;
        run(&ex, "CREATE TABLE orders (id INT PRIMARY KEY, sku TEXT)").await;
        run(&ex, "INSERT INTO orders VALUES (1, 'widget-1')").await;
        run(&ex, "SELECT KV_SET('session:1', 'live')").await;
        run(&ex, "SELECT DOC_INSERT('{\"kind\":\"invoice\"}')").await;
        run(&ex, "SELECT FTS_INDEX(11, 'quarterly revenue report')").await;
        run(
            &ex,
            "CREATE TABLE embeddings (id INT PRIMARY KEY, v VECTOR(3))",
        )
        .await;
        run(
            &ex,
            "INSERT INTO embeddings VALUES (1, VECTOR('[0.5,0.25,0.125]'))",
        )
        .await;
        run(&ex, "SELECT TS_INSERT('cpu', 1000, 17.5)").await;
        run(&ex, "SELECT BLOB_STORE('logo', 'cafebabe')").await;
        run(&ex, "SELECT STREAM_XADD('events', 'kind', 'order.created')").await;
        run(&ex, "SELECT COLUMNAR_INSERT('facts', 'amount', 250)").await;
        run(&ex, "SELECT DATALOG_ASSERT('owner(acme, order1)')").await;
        let n1 = scalar(&ex, "SELECT GRAPH_ADD_NODE('customer', '{}')").await;
        let n2 = scalar(&ex, "SELECT GRAPH_ADD_NODE('order', '{}')").await;
        run(&ex, &format!("SELECT GRAPH_ADD_EDGE({n1}, {n2}, 'placed')")).await;

        reads = vec![
            ("sql", "SELECT sku FROM orders WHERE id = 1".to_string()),
            ("kv", "SELECT KV_GET('session:1')".to_string()),
            ("document", "SELECT DOC_GET(1)".to_string()),
            ("fts", "SELECT FTS_SEARCH('quarterly', 10)".to_string()),
            (
                "vector",
                "SELECT v FROM embeddings WHERE id = 1".to_string(),
            ),
            ("timeseries", "SELECT TS_LAST('cpu')".to_string()),
            ("blob", "SELECT BLOB_GET('logo')".to_string()),
            (
                "streams",
                "SELECT STREAM_XRANGE('events', 0, 99999999999999, 10)".to_string(),
            ),
            (
                "columnar",
                "SELECT COLUMNAR_SUM('facts', 'amount')".to_string(),
            ),
            (
                "datalog",
                "SELECT DATALOG_QUERY('owner(acme, X)')".to_string(),
            ),
            ("graph", format!("SELECT GRAPH_NEIGHBORS({n1}, 'out')")),
            ("cdc", "SELECT CDC_READ(0, 100)".to_string()),
        ];

        let mut collected = Vec::new();
        for (model, sql) in &reads {
            let got = scalar(&ex, sql).await;
            assert!(
                !matches!(got.trim(), "" | "0" | "[]" | "null"),
                "{model} produced nothing before the drill — the fixture is not \
                 exercising it, so the drill would prove nothing about it"
            );
            collected.push((*model, got));
        }
        before = collected;
    }

    // --- The drill itself, through the command an operator runs. ------------

    let t0 = Instant::now();
    let (out, ok) = nucleus(&[
        "backup",
        "--data",
        data.to_str().unwrap(),
        "--output",
        snapshot.to_str().unwrap(),
    ]);
    let backup_secs = t0.elapsed().as_secs_f64();
    assert!(ok, "`nucleus backup` failed:\n{out}");
    assert!(
        out.contains("Files:") && out.contains("BLAKE3"),
        "the backup report must tell an operator what it wrote:\n{out}"
    );

    // A restore must refuse a destination that already holds something, rather
    // than merging into it. This is the mistake a real recovery makes.
    std::fs::create_dir_all(&restored).unwrap();
    std::fs::write(restored.join("leftover.txt"), b"previous attempt").unwrap();
    let (refused, ok) = nucleus(&[
        "restore",
        "--input",
        snapshot.to_str().unwrap(),
        "--data",
        restored.to_str().unwrap(),
    ]);
    assert!(
        !ok,
        "restoring into a non-empty directory must fail, not merge:\n{refused}"
    );
    assert!(
        restored.join("leftover.txt").exists(),
        "a refused restore must leave the destination untouched"
    );
    std::fs::remove_dir_all(&restored).unwrap();

    let t1 = Instant::now();
    let (out, ok) = nucleus(&[
        "restore",
        "--input",
        snapshot.to_str().unwrap(),
        "--data",
        restored.to_str().unwrap(),
    ]);
    let restore_secs = t1.elapsed().as_secs_f64();
    assert!(ok, "`nucleus restore` failed:\n{out}");

    // --- Verify, which is the half a backup script usually skips. -----------

    let (ex, _eng) = boot(&restored).await;
    let mut lost = Vec::new();
    for (model, sql) in &reads {
        let got = scalar(&ex, sql).await;
        let want = &before.iter().find(|(m, _)| m == model).unwrap().1;
        if want != &got {
            lost.push(format!("{model}: had {want:?}, got {got:?}"));
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} models did not survive a CLI backup + restore:\n  {}",
        lost.len(),
        reads.len(),
        lost.join("\n  ")
    );

    // Recovery time, measured rather than asserted. There is no threshold here
    // on purpose: this fixture's database is tiny, so a bound would encode the
    // fixture rather than the engine, and a benchmark threshold that fits the
    // fixture is how a green run stops meaning anything. What the drill
    // guarantees is that the numbers exist and come from the real commands.
    println!(
        "DR drill: backup {backup_secs:.2}s, restore {restore_secs:.2}s, \
         {} models verified",
        reads.len()
    );
}
