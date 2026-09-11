//! A8: the FTS index checkpoint is written durably before the WAL tail that
//! fed it is truncated.
//!
//! `save_fts_index` used to do a bare `std::fs::write` (not atomic, no
//! fsync) followed by the tail truncation. Power loss in that window lost
//! both halves: the checkpoint was torn or absent, and the tail it was
//! written from had already been truncated away. The fix mirrors the
//! WAL-side snapshot in `fts_wal.rs` — temp + fsync + rename + dir fsync
//! via `atomic_write` — and a failed checkpoint write propagates with the
//! tail untouched.
//!
//! This binary arms `NUCLEUS_IOFAULT=fts.checkpoint_write` in-process, so it
//! must stay a single test: the fault counter is process-global. The skip
//! count lets the first checkpoint pass (proving the success path truncates
//! the tail) and fails the second.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::{DiskEngine, StorageEngine};

async fn boot(data: &Path) -> Arc<Executor> {
    std::fs::create_dir_all(data).unwrap();
    let catalog = Arc::new(Catalog::new());
    let catalog_path = data.join("catalog.json");
    let _ = CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await;
    let engine = Arc::new(DiskEngine::open(&data.join("fts.db"), catalog.clone()).unwrap());
    Arc::new(Executor::new_with_persistence(
        catalog,
        engine as Arc<dyn StorageEngine>,
        Some(catalog_path),
        Some(data),
    ))
}

async fn doc_count(ex: &Executor) -> String {
    use nucleus::executor::ExecResult;
    use nucleus::types::Value;
    match ex
        .execute("SELECT FTS_DOC_COUNT()")
        .await
        .expect("doc count")
        .pop()
        .expect("a result")
    {
        ExecResult::Select { rows, .. } => match rows.first().and_then(|r| r.first()) {
            Some(Value::Text(s)) => s.clone(),
            Some(Value::Int64(n)) => n.to_string(),
            Some(other) => format!("{other:?}"),
            None => String::new(),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

fn wal_len(data: &Path) -> u64 {
    std::fs::metadata(data.join("fts").join("fts.wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_checkpoint_write_leaves_checkpoint_and_tail_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");
    // SAFETY: this binary holds exactly one test; the variables are armed
    // before any database work, and nothing else reads the environment.
    unsafe {
        std::env::set_var("NUCLEUS_IOFAULT", "fts.checkpoint_write");
        std::env::set_var("NUCLEUS_IOFAULT_SKIP", "1");
    }

    let ex = boot(&data).await;
    ex.execute("SELECT FTS_INDEX(1, 'first checkpointed document')")
        .await
        .unwrap();

    // Arrival 1: passes. The checkpoint absorbs the tail and truncates it —
    // after truncation the log holds only the empty SNAPSHOT entry (tag +
    // zero doc count = 5 bytes).
    ex.save_fts_index()
        .expect("the first checkpoint passes the fault skip");
    const EMPTY_SNAPSHOT: u64 = 1 + 4;
    let checkpoint = data.join("fts_index.json");
    assert!(checkpoint.exists(), "the checkpoint must exist");
    assert_eq!(
        wal_len(&data),
        EMPTY_SNAPSHOT,
        "a successful checkpoint must truncate the tail it absorbed"
    );

    // A second document, then arrival 2: the fault fires. The save must
    // return the error, and neither half of the pair may be destroyed — the
    // previous checkpoint stays intact and the new tail survives.
    ex.execute("SELECT FTS_INDEX(2, 'second document in the tail')")
        .await
        .unwrap();
    let tail = wal_len(&data);
    assert!(
        tail > EMPTY_SNAPSHOT,
        "the second document must have reached the WAL tail ({tail} bytes)"
    );
    let err = ex
        .save_fts_index()
        .expect_err("the injected checkpoint fault must fail the save");
    assert!(
        err.to_string().contains("injected"),
        "the failure must be the injected fault, got: {err}"
    );
    assert_eq!(
        wal_len(&data),
        tail,
        "a failed checkpoint write must NOT truncate the tail"
    );
    let json = std::fs::read_to_string(&checkpoint).expect("the old checkpoint must survive");
    assert!(
        json.trim_start().starts_with('{') && json.trim_end().ends_with('}'),
        "a failed checkpoint write must not tear the existing checkpoint"
    );

    // Recovery: the checkpoint (doc 1) plus the surviving tail (doc 2) both
    // come back on reopen — the tail replays idempotently on top.
    drop(ex);
    let reopened = boot(&data).await;
    assert_eq!(
        doc_count(&reopened).await,
        "2",
        "reopen must recover the checkpointed document AND the one whose \
         checkpoint failed — the tail was its only durable copy"
    );
}
