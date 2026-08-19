//! M4 exit gate: "automated restore verification compares logical contents
//! across every durable model".
//!
//! What existed before this: `backup_then_restore_round_trips_bytes` compares
//! *bytes*, and `persistent_open_dump_restore_round_trip` exercises the logical
//! dump, which is a different mechanism entirely. Nothing wrote data into the
//! specialty models, took a physical backup, restored it, and read the data
//! back. So a model whose state did not survive a backup would have been caught
//! by nothing — and the byte-level test cannot catch it, because a model that
//! never wrote its state to disk in the first place round-trips its (absent)
//! bytes perfectly.
//!
//! That gap is worth closing now specifically: NU-006 (2026-08-18) changed when
//! six of these models reach disk at all.
//!
//! ## Coverage: 12 of the 14 models, and why the other two are absent
//!
//! Covered: SQL, KV, document, FTS, vector, timeseries, graph, blob, streams,
//! columnar, datalog, CDC.
//!
//! **Geo has no durable state to compare.** Its whole SQL surface —
//! `GEO_DISTANCE`, `GEO_WITHIN`, `GEO_AREA` — is pure functions over literal
//! arguments. `geo/wal.rs` exists and the executor opens and group-syncs it,
//! but `log_insert`/`log_delete` have no callers outside that file's own unit
//! tests, and nothing on the executor holds the `RTree` that
//! `rebuild_rtree` would populate. There is no geo state a backup could lose.
//!
//! **PubSub is not durable by design.** `PUBSUB_PUBLISH` delivers to live
//! subscribers; subscriptions are per-connection. Its durable sibling is
//! Streams, which is covered above.
//!
//! One arm is weaker than it looks: FTS persists in two places — the WAL under
//! `fts/` and a sidecar next to the catalog — so its read here is satisfied by
//! either (NU-014). The other eleven were each shown to fail this comparison
//! when their store was removed from the restored directory.
//!
//! Deliberately physical backup + restore, not PITR. PITR replays only the SQL
//! substrate's page WAL and leaves the specialty logs at the base snapshot
//! (NU-030) — a real limitation, reported by `restore-pitr`, and not what this
//! gate is about.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

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

/// First cell of the first row, as a display string. Models return their
/// scalars in different `Value` arms, and this gate cares that the *content*
/// came back, not which arm carried it.
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

/// Evidence that a read actually returned the model's data.
///
/// Emptiness alone is not enough. A count function returns the text `0` and a
/// JSON read returns `[]` when the model came back empty, and both are
/// non-empty strings — so a model whose state was silently lost would pass a
/// bare `!is_empty()` check on both sides of the backup and the comparison
/// would be vacuous. That is the same shape as the vacuous-pass this file's
/// pre-backup read exists to prevent, one layer down.
fn is_evidence(s: &str) -> bool {
    !matches!(s.trim(), "" | "0" | "[]" | "{}" | "null" | "NULL" | "false")
}

/// Run every read in `reads` and pair each with its model name.
async fn read_all(ex: &Executor, reads: &[(&'static str, String)]) -> Vec<(&'static str, String)> {
    let mut out = Vec::with_capacity(reads.len());
    for (model, sql) in reads {
        out.push((*model, scalar(ex, sql).await));
    }
    out
}

#[tokio::test]
async fn physical_backup_restore_preserves_every_model_it_touches() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("live");
    let snapshot = tmp.path().join("snap");
    let restored = tmp.path().join("restored");

    // The reads are built once and executed in both phases, so the two sides
    // cannot drift apart. Graph's read names a node id that its write returns,
    // which is why this is a Vec of owned strings and not a const table.
    let reads: Vec<(&'static str, String)>;

    // Write into each model, then read each back BEFORE the backup. The
    // pre-backup read is not redundant: it establishes that the model works at
    // all in this fixture, so a post-restore mismatch means the restore lost
    // it rather than the write never having happened. Without that, a model
    // that silently no-ops would produce two matching empty reads and pass.
    let before: Vec<(&'static str, String)> = {
        let (ex, _eng) = boot(&data).await;

        run(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
        run(&ex, "INSERT INTO t VALUES (1, 'sql-row')").await;
        run(&ex, "SELECT KV_SET('k1', 'kv-value')").await;
        run(&ex, "SELECT DOC_INSERT('{\"tag\":\"doc-value\"}')").await;
        run(&ex, "SELECT FTS_INDEX(7, 'searchable fts phrase')").await;
        run(&ex, "CREATE TABLE vec_t (id INT PRIMARY KEY, v VECTOR(3))").await;
        run(&ex, "INSERT INTO vec_t VALUES (1, VECTOR('[1.0,2.0,3.0]'))").await;
        run(&ex, "SELECT TS_INSERT('metric1', 1000, 42.0)").await;
        run(&ex, "SELECT BLOB_STORE('blob_key', '6465616462656566')").await;
        run(&ex, "SELECT STREAM_XADD('s1', 'key', 'stream-value')").await;
        run(&ex, "SELECT COLUMNAR_INSERT('col_t', 'metric', 99)").await;
        run(&ex, "SELECT DATALOG_ASSERT('parent(alice, bob)')").await;

        // Graph ids are assigned by the store, so capture them rather than
        // assuming a fresh database hands out 1 and 2.
        let n1 = scalar(&ex, "SELECT GRAPH_ADD_NODE('person', '{\"name\":\"n1\"}')").await;
        let n2 = scalar(&ex, "SELECT GRAPH_ADD_NODE('person', '{\"name\":\"n2\"}')").await;
        run(&ex, &format!("SELECT GRAPH_ADD_EDGE({n1}, {n2}, 'knows')")).await;

        reads = vec![
            ("sql", "SELECT v FROM t WHERE id = 1".to_string()),
            ("kv", "SELECT KV_GET('k1')".to_string()),
            ("document", "SELECT DOC_GET(1)".to_string()),
            ("fts", "SELECT FTS_SEARCH('searchable', 10)".to_string()),
            ("vector", "SELECT v FROM vec_t WHERE id = 1".to_string()),
            ("timeseries", "SELECT TS_LAST('metric1')".to_string()),
            ("blob", "SELECT BLOB_GET('blob_key')".to_string()),
            (
                "streams",
                "SELECT STREAM_XRANGE('s1', 0, 99999999999999, 10)".to_string(),
            ),
            (
                "columnar",
                "SELECT COLUMNAR_SUM('col_t', 'metric')".to_string(),
            ),
            (
                "datalog",
                "SELECT DATALOG_QUERY('parent(alice, X)')".to_string(),
            ),
            ("graph", format!("SELECT GRAPH_NEIGHBORS({n1}, 'out')")),
            // CDC is not written directly: it records the SQL mutations above,
            // which is exactly the property a restore has to preserve.
            ("cdc", "SELECT CDC_READ(0, 100)".to_string()),
        ];

        let before = read_all(&ex, &reads).await;
        for (model, got) in &before {
            assert!(
                is_evidence(got),
                "{model} produced no evidence BEFORE the backup (got {got:?}) — the \
                 fixture is not exercising it, so any post-restore comparison \
                 would be vacuous"
            );
        }
        before
    };
    // Engines dropped: a clean shutdown, which is what `backup_data_dir`
    // requires (it refuses a directory a live instance holds).

    nucleus::backup::backup_data_dir(&data, &snapshot, false, env!("CARGO_PKG_VERSION")).unwrap();
    nucleus::backup::restore_data_dir(&snapshot, &restored, false, env!("CARGO_PKG_VERSION"))
        .unwrap();

    let (ex, _eng) = boot(&restored).await;
    let after = read_all(&ex, &reads).await;

    let mut lost = Vec::new();
    for ((model, want), (model2, got)) in before.iter().zip(after.iter()) {
        assert_eq!(model, model2);
        if want != got {
            lost.push(format!("{model}: had {want:?}, got {got:?}"));
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} models did not survive backup + restore into a clean \
         directory:\n  {}",
        lost.len(),
        before.len(),
        lost.join("\n  ")
    );
}

/// Restoring must refuse a corrupted snapshot rather than half-apply it.
///
/// `restore_data_dir` verifies every manifest checksum before touching the
/// destination, and `corrupted_snapshot_is_rejected_without_touching_the_destination`
/// already covers the non-destructive part. What this adds is the operator's
/// path: corruption in a *specialty* model's file must be caught too, not just
/// in the SQL data file — the manifest fingerprints the whole tree, and this
/// asserts that is actually true rather than assumed.
#[tokio::test]
async fn a_corrupted_specialty_log_fails_verification() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("live");
    let snapshot = tmp.path().join("snap");
    let restored = tmp.path().join("restored");

    {
        let (ex, _eng) = boot(&data).await;
        run(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
        run(&ex, "INSERT INTO t VALUES (1, 'sql-row')").await;
        run(&ex, "SELECT DOC_INSERT('{\"tag\":\"doc-value\"}')").await;
    }

    nucleus::backup::backup_data_dir(&data, &snapshot, false, env!("CARGO_PKG_VERSION")).unwrap();

    // Find the document log inside the snapshot and corrupt it.
    let mut doc_wal = None;
    for entry in walkdir(&snapshot) {
        if entry.file_name().is_some_and(|n| n == "doc.wal") {
            doc_wal = Some(entry);
            break;
        }
    }
    let Some(doc_wal) = doc_wal else {
        // Not a silent skip: if the document log is absent from a snapshot of a
        // database that wrote a document, that is itself the finding.
        panic!("no doc.wal in the snapshot — the document model's log was not backed up at all");
    };
    let mut bytes = std::fs::read(&doc_wal).unwrap();
    assert!(!bytes.is_empty(), "doc.wal is empty; nothing to corrupt");
    bytes[0] ^= 0xFF;
    std::fs::write(&doc_wal, &bytes).unwrap();

    let err =
        nucleus::backup::restore_data_dir(&snapshot, &restored, false, env!("CARGO_PKG_VERSION"))
            .expect_err("a snapshot with a corrupted specialty log must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("doc.wal") || msg.to_lowercase().contains("checksum"),
        "the refusal must name what failed verification; got: {msg}"
    );
}

/// Every file under `root`, recursively.
fn walkdir(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}
