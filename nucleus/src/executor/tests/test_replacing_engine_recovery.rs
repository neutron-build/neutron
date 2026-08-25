//! ReplacingMergeTree engine metadata must survive a restart of an instance
//! that predates `engines.json`.
//!
//! The bug this file pins: engine metadata was durable ONLY in the
//! `engines.json` sidecar. A table created before that sidecar existed has no
//! entry in it, the catalog recorded nothing, and
//! `CREATE TABLE IF NOT EXISTS ... WITH (engine='replacing_mergetree')` on an
//! already-existing table is a no-op — so `replacing_config()` returned `None`
//! forever, read-time dedup was silently skipped, and every aggregate summed
//! every superseded version. Verified live on Nucleus v0.1.8: the observe
//! instance's `engines.json` listed 1 of ~60 tables, and a window whose raw
//! events proved 72 pageviews reported 158.

use std::path::Path;
use std::sync::Arc;

use super::super::{ExecResult, Executor};
use super::exec;
use crate::catalog::Catalog;
use crate::storage::{DiskEngine, StorageEngine};
use crate::types::Value;

/// Open an executor over `dir` exactly as `main.rs` does: load the catalog off
/// disk, then reconcile the per-table engines.
async fn open_executor(dir: &Path) -> Executor {
    let catalog = Arc::new(Catalog::new());
    let db_path = dir.join("nucleus.db");
    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);
    let ex = Executor::new_with_persistence(catalog, storage, None, Some(dir));
    ex.restore_table_engines().await;
    ex
}

fn i64_of(result: &ExecResult) -> i64 {
    match result {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            Value::Float64(v) => *v as i64,
            other => panic!("unexpected cell: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

fn count_of(result: &ExecResult) -> usize {
    match result {
        ExecResult::Select { rows, .. } => rows.len(),
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Strip every entry from `engines.json`, reproducing the on-disk layout of an
/// instance whose tables were created before the sidecar existed. This is
/// exactly what the live observe data directory looks like.
fn wipe_engines_sidecar(dir: &Path) {
    let path = dir.join("engines.json");
    assert!(
        path.exists(),
        "engines.json should have been written by CREATE TABLE"
    );
    std::fs::write(&path, "{}").unwrap();
}

/// THE upgrade-path test: an old instance layout (no `engines.json` entry),
/// restarted, must still collapse superseded versions on read.
#[tokio::test]
async fn replacing_dedup_survives_a_restart_with_no_engines_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(
            &ex,
            "CREATE TABLE rep_upgrade (k BIGINT, v BIGINT, version BIGINT) \
             WITH (engine='replacing_mergetree', version_column='version') \
             ORDER BY (k)",
        )
        .await;
        exec(&ex, "INSERT INTO rep_upgrade VALUES (1, 10, 1)").await;
        exec(&ex, "INSERT INTO rep_upgrade VALUES (1, 40, 4)").await;
        exec(&ex, "INSERT INTO rep_upgrade VALUES (2, 5, 1)").await;
        ex.checkpoint_table_engines().await;
    }
    // Age the directory back to a pre-sidecar Nucleus.
    wipe_engines_sidecar(dir.path());

    let ex = open_executor(dir.path()).await;
    let sum = i64_of(&exec(&ex, "SELECT SUM(v) FROM rep_upgrade").await[0]);
    assert_eq!(
        sum, 45,
        "SUM must see one row per key (40 + 5); a restart of a pre-engines.json \
         instance was summing every superseded version instead"
    );
    let n = count_of(&exec(&ex, "SELECT k, v FROM rep_upgrade").await[0]);
    assert_eq!(n, 2, "SELECT must return one row per key after the restart");
    let c = i64_of(&exec(&ex, "SELECT COUNT(*) FROM rep_upgrade").await[0]);
    assert_eq!(c, 2, "COUNT(*) must agree with SELECT after the restart");
}

/// The same instance, migrated forward the way an application actually does
/// it: the migration re-runs `CREATE TABLE IF NOT EXISTS ... WITH (engine=…)`.
/// That statement used to warn and change nothing, so an existing table could
/// never be repaired. It must now adopt the declaration.
#[tokio::test]
async fn create_if_not_exists_adopts_the_engine_for_an_existing_table() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        // A plain table, as a pre-engine-support migration would have created.
        exec(
            &ex,
            "CREATE TABLE rep_adopt (k BIGINT, v BIGINT, version BIGINT)",
        )
        .await;
        exec(&ex, "INSERT INTO rep_adopt VALUES (1, 10, 1)").await;
        exec(&ex, "INSERT INTO rep_adopt VALUES (1, 40, 4)").await;
    }
    let ex = open_executor(dir.path()).await;
    // The migration that adds the engine clause. Today's behaviour: skipped.
    exec(
        &ex,
        "CREATE TABLE IF NOT EXISTS rep_adopt (k BIGINT, v BIGINT, version BIGINT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (k)",
    )
    .await;
    let sum = i64_of(&exec(&ex, "SELECT SUM(v) FROM rep_adopt").await[0]);
    assert_eq!(
        sum, 40,
        "after the migration re-declares the engine, reads must collapse by \
         version — the declaration used to be silently discarded"
    );
    // And it must still be collapsed after the next restart.
    drop(ex);
    let ex = open_executor(dir.path()).await;
    let sum = i64_of(&exec(&ex, "SELECT SUM(v) FROM rep_adopt").await[0]);
    assert_eq!(sum, 40, "the adopted engine must be durable");
}

/// Highest version wins, not "whichever row the physical merge happened to
/// leave last". Reported live as `1@v500, 8@v1000, 8@v2000` collapsing to `1`.
#[tokio::test]
async fn replacing_keeps_the_highest_version_not_the_last_written() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(
        &ex,
        "CREATE TABLE rep_version (k BIGINT, v BIGINT, version BIGINT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (k)",
    )
    .await;
    // Deliberately out of version order, so "last write wins" and "highest
    // version wins" disagree.
    exec(&ex, "INSERT INTO rep_version VALUES (1, 20, 2000)").await;
    exec(&ex, "INSERT INTO rep_version VALUES (1, 10, 1000)").await;
    exec(&ex, "INSERT INTO rep_version VALUES (1, 5, 500)").await;
    let sum = i64_of(&exec(&ex, "SELECT SUM(v) FROM rep_version").await[0]);
    assert_eq!(sum, 20, "the v2000 row must win regardless of write order");
    // And across a restart, where the physical merge path is what collapses.
    ex.checkpoint_table_engines().await;
    drop(ex);
    let ex = open_executor(dir.path()).await;
    let sum = i64_of(&exec(&ex, "SELECT SUM(v) FROM rep_version").await[0]);
    assert_eq!(sum, 20, "highest version must still win after a restart");
}

/// `FINAL` must never be silently ignored. Nucleus collapses replacing tables
/// on every read, so the modifier is unnecessary — but a query that asks for it
/// and gets un-collapsed numbers back is the worst outcome, so it is rejected.
#[tokio::test]
async fn final_modifier_is_rejected_rather_than_silently_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(
        &ex,
        "CREATE TABLE rep_final (k BIGINT, v BIGINT, version BIGINT) \
         WITH (engine='replacing_mergetree', version_column='version') \
         ORDER BY (k)",
    )
    .await;
    let err = ex
        .execute("SELECT SUM(v) FROM rep_final FINAL")
        .await
        .expect_err("FINAL must not be accepted as a table alias");
    let msg = err.to_string();
    assert!(
        msg.to_uppercase().contains("FINAL"),
        "the error must name FINAL so the porting mistake is obvious: {msg}"
    );
    // A quoted alias that happens to spell the word is still just an alias.
    let ok = ex.execute("SELECT COUNT(*) FROM rep_final AS \"FINAL\"").await;
    assert!(ok.is_ok(), "a quoted alias must keep working: {ok:?}");
}
