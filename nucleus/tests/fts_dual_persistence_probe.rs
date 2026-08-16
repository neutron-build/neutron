//! **Characterization test for a defect that is deliberately still open (NU-014).**
//!
//! Read this before "fixing" what it asserts. It pins CURRENT, WRONG behaviour
//! on purpose, because the obvious correction destroys data on upgrade and the
//! measurements below are what establish that.
//!
//! FTS has two persistence mechanisms:
//!
//! * a WAL-backed `InvertedIndex` under `fts/`, opened through `open_durable`
//!   and described in the code as "WAL-backed crash-recovery";
//! * a legacy `fts_index.json`, written by `save_fts_index` after mutations.
//!
//! `new_with_persistence` opens the first and then calls `load_fts_index()`,
//! which replaces it wholesale from the second. `InvertedIndex::wal` is
//! `#[serde(skip)]`, so the replacement has `wal: None`, and all three WAL write
//! sites are `if let Some(wal) = &self.wal`.
//!
//! Measured consequence, asserted below: **from the second boot onward the FTS
//! WAL receives nothing.** The legacy JSON is the only live durable copy.
//!
//! Why this is not simply corrected here:
//!
//! 1. Letting the WAL win would discard everything an existing deployment has
//!    written since its own second boot, because their WAL went stale then.
//! 2. It cannot be migrated by conversion. The WAL stores original document
//!    text and replays it; the JSON stores derived postings and `DocInfo` keeps
//!    only a token length. There is no text in the JSON to rebuild a WAL from,
//!    so JSON -> WAL is a re-index from base tables, not a format change.
//!
//! That is a product decision with a migration attached. If you are changing
//! this deliberately, this test SHOULD fail — update it along with the
//! migration, and do not just delete the assertions.
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

#[tokio::test]
async fn the_fts_wal_stops_receiving_writes_after_the_first_boot() {
    let dir = std::env::temp_dir().join(format!("fts-dual-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let json = dir.join("fts_index.json");
    let fts_dir = dir.join("fts");

    // Session 1: no legacy JSON exists yet, so the WAL-backed index survives
    // load_fts_index and stays attached. This write DOES reach the WAL.
    {
        let ex = boot(&dir);
        run(&ex, "SELECT FTS_INDEX(1, 'the quick brown fox')").await;
    }
    assert!(
        json.exists(),
        "save_fts_index should have written the legacy file"
    );
    let wal_after_first = dir_bytes(&fts_dir);

    // Proof that session 1's write reached the WAL: drop the JSON, reboot, and
    // the document is still searchable.
    {
        let stash = std::fs::read(&json).unwrap();
        std::fs::remove_file(&json).unwrap();
        let ex = boot(&dir);
        assert_eq!(
            hits(&run(&ex, "SELECT FTS_SEARCH('quick', 10)").await),
            1,
            "the first session's write should be recoverable from the WAL alone"
        );
        std::fs::write(&json, stash).unwrap();
    }

    // Session 2: the legacy JSON now exists at startup, so load_fts_index
    // replaces the index and the WAL handle goes with it.
    {
        let ex = boot(&dir);
        run(&ex, "SELECT FTS_INDEX(3, 'a quick silver hare')").await;
    }
    let wal_after_second = dir_bytes(&fts_dir);

    assert_eq!(
        wal_after_second, wal_after_first,
        "CHARACTERIZATION (NU-014): the FTS WAL is expected to be BYTE-IDENTICAL after a \
         second session's write, because the index loaded from the legacy JSON carries no \
         WAL handle. If this now differs, the WAL was re-attached — read this file's header \
         before assuming that is safe."
    );

    // With the legacy file present, both documents are there.
    {
        let ex = boot(&dir);
        assert_eq!(
            hits(&run(&ex, "SELECT FTS_SEARCH('quick', 10)").await),
            2,
            "the legacy JSON is the live durable copy and holds both documents"
        );
    }

    // Without it, the second session's document is simply gone.
    std::fs::remove_file(&json).unwrap();
    {
        let ex = boot(&dir);
        assert_eq!(
            hits(&run(&ex, "SELECT FTS_SEARCH('quick', 10)").await),
            1,
            "CHARACTERIZATION (NU-014): doc 3 never reached the WAL, so removing the legacy \
             file loses it. This is the data the naive fix would discard on upgrade."
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
