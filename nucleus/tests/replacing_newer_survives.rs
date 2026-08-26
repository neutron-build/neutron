//! The replacing-table write-path collapse must keep the NEWER version.
//!
//! Pinned after a Teploy/Observe report ("two rows on one ORDER BY key,
//! versions N and N+1, collapse in the memtable and the survivor is the older
//! one roughly half the time" -- measured on v0.1.8-era builds). Every merge
//! and dedup site honors the version column, and these shapes keep the newer
//! row on every run; the pin exists so a regression in any of the collapse
//! sites (memtable dedup, segment merge, part fold, read-time dedup) fails
//! here instead of silently reverting updates.

use std::path::Path;
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::ExecResult;
use nucleus::executor::Executor;
use nucleus::storage::StorageEngine;
use nucleus::storage::buffered_engine::BufferedDiskEngine;
use nucleus::storage::disk_engine::DiskEngine;
use nucleus::storage::persistence::CatalogPersistence;
use nucleus::storage::wal::SyncMode;
use nucleus::types::Value;

async fn open_segmented(dir: &Path) -> Executor {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());
    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();
    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            64,
            1, // 1 MB segments: sealing without megabytes of writes
            SyncMode::Fsync,
        )
        .unwrap(),
    );
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(engine));
    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    ex.restore_table_engines().await;
    ex
}

const DDL: &str = "CREATE TABLE rt (k INT, v TEXT, version BIGINT) \
WITH (engine='replacing_mergetree', version_column='version') ORDER BY (k)";

async fn winner(ex: &Executor) -> String {
    for r in ex.execute("SELECT v FROM rt").await.unwrap() {
        if let ExecResult::Select { rows, .. } = r {
            if rows.is_empty() {
                return String::new();
            }
            return match &rows[0][0] {
                Value::Text(s) => s.clone(),
                other => format!("{other:?}"),
            };
        }
    }
    String::new()
}

#[tokio::test]
async fn replacing_collapse_keeps_the_newer_version_in_every_shape() {
    let dir = tempfile::tempdir().unwrap();

    // Shape 1: one INSERT, both tuples, ascending version.
    {
        let ex = open_segmented(dir.path()).await;
        ex.execute("DROP TABLE IF EXISTS rt").await.unwrap();
        ex.execute(DDL).await.unwrap();
        ex.execute("INSERT INTO rt VALUES (1,'A',1000),(1,'B',1001)")
            .await
            .unwrap();
        assert_eq!(winner(&ex).await, "B", "single-batch ascending");
    }

    // Shape 2: one INSERT, descending version order (the concurrent-ingester
    // shape -- the newer row ARRIVES first).
    {
        let ex = open_segmented(dir.path()).await;
        ex.execute("DELETE FROM rt").await.unwrap();
        ex.execute("INSERT INTO rt VALUES (1,'B',1001),(1,'A',1000)")
            .await
            .unwrap();
        assert_eq!(winner(&ex).await, "B", "single-batch descending");
    }

    // Shape 3: sequential statements, reopen between writes so the older row
    // has been persisted to its own part before the newer arrives.
    {
        let ex = open_segmented(dir.path()).await;
        ex.execute("DELETE FROM rt").await.unwrap();
        ex.execute("INSERT INTO rt VALUES (1,'A',1000)")
            .await
            .unwrap();
    }
    {
        let ex = open_segmented(dir.path()).await;
        assert_eq!(winner(&ex).await, "A", "older row alone after reopen");
        ex.execute("INSERT INTO rt VALUES (1,'B',1001)")
            .await
            .unwrap();
        assert_eq!(winner(&ex).await, "B", "newer wins across a reopen");
    }

    // Shape 4: equal versions -- the LAST written row wins, never the older.
    {
        let ex = open_segmented(dir.path()).await;
        ex.execute("DELETE FROM rt").await.unwrap();
        ex.execute("INSERT INTO rt VALUES (1,'A',1000)")
            .await
            .unwrap();
        ex.execute("INSERT INTO rt VALUES (1,'B',1000)")
            .await
            .unwrap();
        assert_eq!(winner(&ex).await, "B", "tie keeps the later row");
    }

    // Shape 5: the reporter's exact A/B pair repeated across fresh reopens.
    for round in 0..10 {
        let ex = open_segmented(dir.path()).await;
        ex.execute("DELETE FROM rt").await.unwrap();
        ex.execute("INSERT INTO rt VALUES (1,'A',1000),(1,'B',1001)")
            .await
            .unwrap();
        let got = winner(&ex).await;
        assert_eq!(got, "B", "round {round}");
    }
}
