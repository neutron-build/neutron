//! B2: the executor sweeps orphaned query-spill files on startup.
//!
//! A crashed process can leave spill files behind (their unlink-on-drop guards
//! never ran). Opening an executor over that data directory must reclaim them,
//! the same crash-cleanup contract as the WAL temp sweep. Foreign files in the
//! spill dir are left untouched.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use crate::catalog::Catalog;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, StorageEngine};

async fn open_executor(dir: &Path) -> Executor {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();

    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);

    Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir))
}

#[tokio::test]
async fn startup_sweeps_orphan_spill_files_but_spares_others() {
    let dir = tempfile::tempdir().unwrap();
    let spill_dir = dir.path().join("spill");
    std::fs::create_dir_all(&spill_dir).unwrap();

    // A crash left two spill files and one unrelated file behind.
    let orphan_a = spill_dir.join("spill-4242-0-q1.tmp");
    let orphan_b = spill_dir.join("spill-4242-1-q2.tmp");
    let foreign = spill_dir.join("notes.txt");
    std::fs::write(&orphan_a, b"leftover run a").unwrap();
    std::fs::write(&orphan_b, b"leftover run b").unwrap();
    std::fs::write(&foreign, b"not ours").unwrap();

    // Opening the executor runs the startup sweep.
    let _ex = open_executor(dir.path()).await;

    assert!(
        !orphan_a.exists(),
        "orphan spill file A should be reclaimed"
    );
    assert!(
        !orphan_b.exists(),
        "orphan spill file B should be reclaimed"
    );
    assert!(foreign.exists(), "foreign file must be left untouched");
}
