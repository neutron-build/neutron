//! NU-014: `fts_index.json` is the checkpoint, the FTS WAL is the tail.
//!
//! They used to be rivals, and the JSON always won. `InvertedIndex::wal` is
//! `#[serde(skip)]`, so the index deserialized from `fts_index.json` came back
//! with `wal: None` — and all three WAL write sites are `if let Some(wal)`.
//! From the SECOND boot onward the FTS WAL therefore received nothing, and the
//! JSON became the only durable copy of everything written after it.
//!
//! Measured at the time: the WAL directory was 64 bytes before a second
//! session's write and 64 bytes after it.
//!
//! The obvious fix — let the WAL win — destroys data on upgrade, because every
//! existing deployment's WAL has been stale since its own second boot. And the
//! two cannot be reconciled by conversion: the WAL stores document TEXT, the
//! JSON stores derived POSTINGS, and postings cannot be turned back into text.
//!
//! So neither wins: the JSON is loaded as the base, the WAL handle is
//! re-attached to it, the WAL's contents are applied as a tail, and each
//! checkpoint truncates the tail it just absorbed.
//!
//! The checkpoint is periodic — the server writes it on the same
//! `wal.checkpoint_interval_secs` tick as every other model's — so these tests
//! call `save_fts_index()` explicitly where the server's timer would. Writing
//! it on every mutation, which is what used to happen, is O(index) per write
//! and is exactly what made the JSON the only durable copy.

#![cfg(feature = "server")]

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::{DiskEngine, StorageEngine};
use nucleus::types::Value;

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

async fn scalar(ex: &Executor, sql: &str) -> String {
    match ex.execute(sql).await.expect(sql).pop().expect("a result") {
        ExecResult::Select { rows, .. } => match rows.first().and_then(|r| r.first()) {
            Some(Value::Text(s)) => s.clone(),
            Some(Value::Int64(n)) => n.to_string(),
            Some(other) => format!("{other:?}"),
            None => String::new(),
        },
        other => panic!("expected Select for `{sql}`, got {other:?}"),
    }
}

fn wal_len(data: &Path) -> u64 {
    std::fs::metadata(data.join("fts").join("fts.wal"))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// The bug itself: a write made on the SECOND boot must reach the WAL.
#[tokio::test]
async fn a_second_session_still_writes_to_the_fts_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");

    {
        let ex = boot(&data).await;
        ex.execute("SELECT FTS_INDEX(1, 'first boot document')")
            .await
            .unwrap();
        ex.save_fts_index(); // the server's checkpoint tick
    }
    assert!(
        data.join("fts_index.json").exists(),
        "the checkpoint should exist after the first session"
    );

    let ex = boot(&data).await;
    // The checkpoint's contents survived the reopen.
    assert_eq!(scalar(&ex, "SELECT FTS_DOC_COUNT()").await, "1");

    let before = wal_len(&data);
    ex.execute("SELECT FTS_INDEX(2, 'second boot document')")
        .await
        .unwrap();
    let after = wal_len(&data);

    assert!(
        after > before,
        "a write on the second boot did not reach the FTS WAL ({before} bytes before, \
         {after} after) — the checkpoint-loaded index has no WAL handle"
    );
    assert_eq!(scalar(&ex, "SELECT FTS_DOC_COUNT()").await, "2");
}

/// A checkpoint truncates the tail it absorbed, so the two cannot diverge and
/// the log does not grow without bound.
#[tokio::test]
async fn a_checkpoint_truncates_the_tail_it_absorbed() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");
    let ex = boot(&data).await;

    for i in 1..=20u64 {
        ex.execute(&format!("SELECT FTS_INDEX({i}, 'document number {i}')"))
            .await
            .unwrap();
    }
    // The tail carries all twenty, because nothing has checkpointed yet.
    let tail_before = wal_len(&data);
    assert!(
        tail_before > 0,
        "twenty writes produced an empty WAL tail — they are not being logged"
    );

    ex.save_fts_index(); // the server's checkpoint tick
    let tail_after = wal_len(&data);
    let checkpoint = std::fs::metadata(data.join("fts_index.json"))
        .unwrap()
        .len();
    assert!(
        tail_after < tail_before,
        "the checkpoint did not truncate the tail it absorbed ({tail_before} bytes \
         before, {tail_after} after)"
    );
    assert!(
        tail_after < checkpoint,
        "the tail ({tail_after} bytes) is not smaller than the checkpoint \
         ({checkpoint} bytes) after a checkpoint"
    );
    assert_eq!(scalar(&ex, "SELECT FTS_DOC_COUNT()").await, "20");
}

/// A tail that survives a crash is applied on top of the checkpoint, and
/// applying it twice changes nothing.
#[tokio::test]
async fn a_tail_is_applied_on_top_of_the_checkpoint_and_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");

    {
        let ex = boot(&data).await;
        ex.execute("SELECT FTS_INDEX(1, 'checkpointed document')")
            .await
            .unwrap();
        ex.save_fts_index();
    }

    // Simulate the crash window: a WAL entry written, the checkpoint NOT
    // rewritten. Appending straight to the log is exactly what that leaves.
    {
        let (wal, _state) =
            nucleus::fts::fts_wal::FtsWal::open(&data.join("fts")).expect("open fts wal");
        wal.log_index_doc(2, "tail document that never reached the checkpoint")
            .unwrap();
        wal.group_sync().unwrap();
    }

    let ex = boot(&data).await;
    assert_eq!(
        scalar(&ex, "SELECT FTS_DOC_COUNT()").await,
        "2",
        "the tail entry was lost — the checkpoint overwrote it instead of being \
         a base for it"
    );

    // Boot again: the tail was truncated by the first boot's checkpoint, or it
    // replays idempotently. Either way the count must not double.
    drop(ex);
    let ex = boot(&data).await;
    assert_eq!(
        scalar(&ex, "SELECT FTS_DOC_COUNT()").await,
        "2",
        "replaying the tail changed the document count — it is not idempotent"
    );
}

/// A removal in the tail must not be resurrected by the checkpoint under it.
#[tokio::test]
async fn a_removal_in_the_tail_is_not_resurrected() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");

    {
        let ex = boot(&data).await;
        ex.execute("SELECT FTS_INDEX(1, 'doomed document')")
            .await
            .unwrap();
        ex.execute("SELECT FTS_INDEX(2, 'surviving document')")
            .await
            .unwrap();
        ex.save_fts_index();
    }

    // The crash window again, this time for a removal.
    {
        let (wal, _state) =
            nucleus::fts::fts_wal::FtsWal::open(&data.join("fts")).expect("open fts wal");
        wal.log_remove_doc(1).unwrap();
        wal.group_sync().unwrap();
    }

    let ex = boot(&data).await;
    assert_eq!(
        scalar(&ex, "SELECT FTS_DOC_COUNT()").await,
        "1",
        "a document removed in the tail came back, because the checkpoint under \
         it still contained it"
    );
}

/// The property the whole change exists for: a write made AFTER the last
/// checkpoint survives a crash, because the WAL holds it.
///
/// Before this, the second session's writes reached no WAL at all — the
/// checkpoint-loaded index had `wal: None` — so anything written between two
/// checkpoints was lost by a crash, with the JSON quietly the only copy.
#[tokio::test]
async fn a_write_after_the_last_checkpoint_survives_a_crash() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");

    {
        let ex = boot(&data).await;
        ex.execute("SELECT FTS_INDEX(1, 'checkpointed before the crash')")
            .await
            .unwrap();
        ex.save_fts_index();
    }

    // Second session: write, do NOT checkpoint, and drop the executor as a
    // crash would.
    {
        let ex = boot(&data).await;
        ex.execute("SELECT FTS_INDEX(2, 'written after the last checkpoint')")
            .await
            .unwrap();
    }

    let ex = boot(&data).await;
    assert_eq!(
        scalar(&ex, "SELECT FTS_DOC_COUNT()").await,
        "2",
        "the document written after the last checkpoint was lost — the \
         checkpoint-loaded index is not logging to the WAL"
    );
}
