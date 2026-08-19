//! `nucleus restore-pitr` must work as a command, not just as a function.
//!
//! M4's restore-to-latest / restore-to-time-or-position checkbox sat unticked
//! with the note "LIKELY DONE, needs an end-to-end gate: ... left unchecked
//! until a restore is verified from a clean directory rather than in-process."
//!
//! The in-process half was already covered — `disk_engine::tests::
//! pitr_restores_row_set_at_target_lsn` calls `restore_pitr()` directly and
//! checks the exact row set. What nothing covered is `cmd_restore_pitr`: the
//! argument parsing, the target selection between `--lsn` / `--time` / neither,
//! the exit code, and the report an operator actually reads during a recovery.
//! A library that restores correctly behind a command that cannot be invoked
//! restores nothing.
//!
//! So this drives the real binary as a subprocess (`CARGO_BIN_EXE_nucleus`,
//! which Cargo builds for integration tests), then opens the directory it
//! produced and checks the rows.

#![cfg(feature = "server")]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::Value;

/// Boot a server-shaped stack with continuous archiving enabled.
///
/// Segments are deliberately tiny so writes roll over and reach the archive
/// during the test rather than at shutdown — PITR can only replay what was
/// archived, and a fixture that archives nothing would pass while proving
/// nothing.
async fn boot(data: &Path, archive: &Path) -> (Arc<Executor>, Arc<BufferedDiskEngine>) {
    std::fs::create_dir_all(data).unwrap();
    let catalog = Arc::new(Catalog::new());
    let catalog_path = data.join("catalog.json");
    let _ = CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;
    let db_path = data.join("nucleus.db");
    let engine = Arc::new(
        DiskEngine::open_segmented_archived(
            &db_path,
            catalog.clone(),
            1024,
            12_000,
            SyncMode::Fsync,
            archive,
        )
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

async fn count(ex: &Executor, sql: &str) -> i64 {
    match run(ex, sql).await {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            Value::Text(s) => s.trim().parse().unwrap(),
            other => panic!("not a count: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_pitr_command_rebuilds_a_database_in_a_clean_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("live");
    let archive = tmp.path().join("archive");
    let base = tmp.path().join("base");
    let restored = tmp.path().join("restored");

    // Batch A, then a physical base snapshot holding only A.
    {
        let (ex, eng) = boot(&data, &archive).await;
        run(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
        for i in 0..40 {
            run(&ex, &format!("INSERT INTO t VALUES ({i}, 'a{i}')")).await;
        }
        eng.checkpoint().await.unwrap();
    }
    nucleus::backup::backup_data_dir(&data, &base, false, env!("CARGO_PKG_VERSION")).unwrap();

    // Batch B lands after the snapshot, so it exists only in the archived WAL.
    // If the restore below only laid down the base, B would be missing — which
    // is what makes the final assertion mean something.
    {
        let (ex, eng) = boot(&data, &archive).await;
        for i in 100..140 {
            run(&ex, &format!("INSERT INTO t VALUES ({i}, 'b{i}')")).await;
        }
        eng.checkpoint().await.unwrap();
    }

    // The command under test. `--lsn`/`--time` omitted, so this is the
    // restore-to-latest workflow.
    let out = Command::new(env!("CARGO_BIN_EXE_nucleus"))
        .args([
            "restore-pitr",
            "--base",
            base.to_str().unwrap(),
            "--archive",
            archive.to_str().unwrap(),
            "--data",
            restored.to_str().unwrap(),
            "--db-file",
            "nucleus.db",
        ])
        .output()
        .expect("failed to run the nucleus binary");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "restore-pitr exited {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        out.status.code()
    );

    // The report is the only thing an operator sees during a recovery, so it is
    // part of the contract, not decoration.
    assert!(
        stdout.contains("PITR restore complete"),
        "no completion line in the report:\n{stdout}"
    );
    assert!(
        stdout.contains("Replayed to LSN"),
        "the report must state the LSN it reached:\n{stdout}"
    );
    assert!(
        stdout.contains("Recovery point"),
        "the report must state the recovery point in wall-clock terms:\n{stdout}"
    );
    // Deliberately NOT asserting the NU-030 "NOT replayed" line here. This
    // fixture drives an Executor directly rather than a full server, so no
    // specialty-model logs are attached and there is correctly nothing for it
    // to report. An `A || B` where B is always printed would pass regardless
    // and read like coverage -- `wal::prune_archive`'s own tests and the
    // `specialty_logs_in` unit test are where that behaviour is gated.
    assert!(
        stdout.contains("Start with:"),
        "the report must end by telling the operator how to start the restored \
         database; it stopped early:\n{stdout}"
    );

    // Open what the command produced and check the data, not just the log.
    let (ex, _eng) = boot(&restored, &tmp.path().join("archive2")).await;
    let total = count(&ex, "SELECT COUNT(*) FROM t").await;
    let a = count(&ex, "SELECT COUNT(*) FROM t WHERE id < 100").await;
    let b = count(&ex, "SELECT COUNT(*) FROM t WHERE id >= 100").await;

    assert_eq!(a, 40, "batch A (in the base snapshot) is incomplete");
    assert_eq!(
        b, 40,
        "batch B was written after the base snapshot and must come back from the \
         archived WAL — {b} of 40 present means replay did not happen, and a \
         restore that only unpacks the base would pass every other assertion here"
    );
    assert_eq!(total, 80);
}

#[tokio::test]
async fn restore_pitr_refuses_a_target_older_than_the_base() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("live");
    let archive = tmp.path().join("archive");
    let base = tmp.path().join("base");
    let restored = tmp.path().join("restored");

    {
        let (ex, eng) = boot(&data, &archive).await;
        run(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
        for i in 0..40 {
            run(&ex, &format!("INSERT INTO t VALUES ({i}, 'a{i}')")).await;
        }
        eng.checkpoint().await.unwrap();
    }
    nucleus::backup::backup_data_dir(&data, &base, false, env!("CARGO_PKG_VERSION")).unwrap();

    // Ask to roll BACK to LSN 1, before the base was taken. Replay only moves
    // forward, so the honest answer is a refusal: the base pages already carry
    // everything, and the old behaviour was to report success at the requested
    // LSN while handing back the base unchanged — an operator undoing a
    // destructive statement would believe it had been undone.
    let out = Command::new(env!("CARGO_BIN_EXE_nucleus"))
        .args([
            "restore-pitr",
            "--base",
            base.to_str().unwrap(),
            "--archive",
            archive.to_str().unwrap(),
            "--data",
            restored.to_str().unwrap(),
            "--db-file",
            "nucleus.db",
            "--lsn",
            "1",
        ])
        .output()
        .expect("failed to run the nucleus binary");

    assert!(
        !out.status.success(),
        "a target older than the base must fail, not report success"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("replay only moves forward") || stderr.contains("cannot restore to LSN"),
        "the refusal must explain itself; got:\n{stderr}"
    );
}

#[tokio::test]
async fn restore_pitr_rejects_both_lsn_and_time() {
    let tmp = tempfile::tempdir().unwrap();
    // Deliberately no setup: the mutual-exclusion check must fire before any
    // filesystem work, so a bad invocation cannot half-restore anything.
    let out = Command::new(env!("CARGO_BIN_EXE_nucleus"))
        .args([
            "restore-pitr",
            "--base",
            tmp.path().join("nope").to_str().unwrap(),
            "--archive",
            tmp.path().join("nope").to_str().unwrap(),
            "--data",
            tmp.path().join("out").to_str().unwrap(),
            "--lsn",
            "10",
            "--time",
            "20",
        ])
        .output()
        .expect("failed to run the nucleus binary");

    assert!(!out.status.success(), "--lsn with --time must be rejected");
    assert!(
        !tmp.path().join("out").exists(),
        "a rejected invocation must not create the data directory"
    );
}
