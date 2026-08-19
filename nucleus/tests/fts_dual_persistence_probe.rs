//! **Was a characterization test for NU-014. The defect is fixed; this is now
//! the UPGRADE test, which is the half the fix had to get right.**
//!
//! What it used to pin, and what was measured: FTS had two persistence
//! mechanisms —
//!
//! * a WAL-backed `InvertedIndex` under `fts/`, opened through `open_durable`
//!   and described in the code as "WAL-backed crash-recovery";
//! * a legacy `fts_index.json`, written by `save_fts_index` after mutations.
//!
//! `new_with_persistence` opened the first and then called `load_fts_index()`,
//! which replaced it wholesale from the second. `InvertedIndex::wal` is
//! `#[serde(skip)]`, so the replacement had `wal: None` and all three WAL write
//! sites are `if let Some(wal) = &self.wal`. **From the second boot onward the
//! FTS WAL received nothing**, silently, and the JSON was the only live durable
//! copy.
//!
//! Why the obvious correction was refused, and still is:
//!
//! 1. Letting the WAL win discards everything an existing deployment has
//!    written since its own second boot, because their WAL went stale then.
//! 2. It cannot be migrated by conversion. The WAL stores original document
//!    text and replays it; the JSON stores derived postings and `DocInfo` keeps
//!    only a token length. There is no text in the JSON to rebuild a WAL from.
//!
//! **The fix takes neither side.** `fts_index.json` is a CHECKPOINT, the WAL is
//! the TAIL applied on top of it, and the WAL handle is re-attached to the
//! index deserialized from the checkpoint. An existing deployment's JSON is
//! therefore its seed and nothing is discarded — which is exactly what this
//! file now tests, because a fix that loses a legacy deployment's data on
//! upgrade would satisfy every test in `fts_checkpoint_and_tail.rs` and still
//! be a disaster.
//!
//! `fts_checkpoint_and_tail.rs` covers the forward behaviour: writes reaching
//! the WAL, the tail applying on top of the checkpoint, removals in the tail,
//! and a write after the last checkpoint surviving a crash.

#![cfg(feature = "server")]
use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;

fn boot(dir: &Path) -> Arc<Executor> {
    std::fs::create_dir_all(dir).unwrap();
    Arc::new(Executor::new_with_persistence(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
        Some(dir.join("catalog.json")),
        Some(dir),
    ))
}

async fn run(ex: &Executor, sql: &str) -> ExecResult {
    ex.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .expect("a result")
}

/// `FTS_SEARCH` returns ONE row holding a JSON array, so counting rows returns
/// 1 whether or not anything matched. Count matches inside the payload — the
/// first version of this probe counted rows and could not tell the two apart.
fn hits(res: &ExecResult) -> usize {
    let cell = match res {
        ExecResult::Select { rows, .. } => rows
            .first()
            .and_then(|r| r.first())
            .cloned()
            .unwrap_or(nucleus::types::Value::Null),
        other => panic!("expected a SELECT, got {other:?}"),
    };
    match cell {
        nucleus::types::Value::Text(s) if s.trim() == "[]" => 0,
        nucleus::types::Value::Text(s) => s.matches("\"doc_id\"").count(),
        other => panic!("expected FTS_SEARCH text, got {other:?}"),
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// A data directory in the LEGACY state — a complete `fts_index.json` and a WAL
/// that went stale behind it — must open with nothing lost, and must start
/// logging again.
#[tokio::test]
async fn a_legacy_directory_upgrades_without_losing_the_json_only_documents() {
    let dir = std::env::temp_dir().join(format!("fts-dual-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let json = dir.join("fts_index.json");
    let fts_dir = dir.join("fts");

    // Build a directory that looks like one written by the old code: the
    // checkpoint holds both documents, and the WAL is stale behind it. The
    // truncation is the manufacture — under the old code the WAL simply stopped
    // receiving anything after the first boot, which ends in the same place.
    {
        let ex = boot(&dir);
        run(&ex, "SELECT FTS_INDEX(1, 'the quick brown fox')").await;
        run(&ex, "SELECT FTS_INDEX(3, 'a quick silver hare')").await;
        ex.save_fts_index();
    }
    std::fs::write(fts_dir.join("fts.wal"), b"").unwrap();
    assert!(json.exists(), "the checkpoint must exist for this fixture");
    let stale_wal = dir_bytes(&fts_dir);

    // Upgrade: both documents are still there, from the checkpoint.
    let ex = boot(&dir);
    assert_eq!(
        hits(&run(&ex, "SELECT FTS_SEARCH('quick', 10)").await),
        2,
        "opening a legacy directory lost documents that existed only in the \
         checkpoint — this is the data the naive fix would have discarded"
    );

    // And the WAL is live again: this session's write reaches it.
    run(&ex, "SELECT FTS_INDEX(5, 'quick as a fish')").await;
    let after_write = dir_bytes(&fts_dir);
    assert!(
        after_write > stale_wal,
        "a write after the upgrade did not reach the FTS WAL ({stale_wal} bytes \
         before, {after_write} after) — the checkpoint-loaded index has no WAL \
         handle, which is NU-014 itself"
    );

    // Crash before the next checkpoint: the tail carries the new document, and
    // the checkpoint under it still carries the other two.
    drop(ex);
    {
        let ex = boot(&dir);
        assert_eq!(
            hits(&run(&ex, "SELECT FTS_SEARCH('quick', 10)").await),
            3,
            "the document written after the upgrade was lost on reopen"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
