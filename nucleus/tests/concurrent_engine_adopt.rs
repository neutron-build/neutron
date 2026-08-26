//! Regression hunt: concurrent `CREATE TABLE IF NOT EXISTS ... WITH (engine=...)`
//! racing on one executor, the shape of the Observe migration runner applying
//! schema from several connections at once.

use nucleus::executor::Executor;
use nucleus::storage::MemoryEngine;
use std::sync::Arc;

fn mem_executor() -> Executor {
    let catalog = Arc::new(nucleus::catalog::Catalog::new());
    let storage: Arc<dyn nucleus::storage::StorageEngine> = Arc::new(MemoryEngine::new());
    Executor::new(catalog, storage)
}

const DDL: &str = "CREATE TABLE IF NOT EXISTS log_pipelines (\
a TEXT, version BIGINT) WITH (engine='replacing_mergetree', version_column='version') \
ORDER BY (a)";

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_if_not_exists_with_engine_never_refuses() {
    // Fresh executor per round: the failure was observed on first-apply races.
    for round in 0..200 {
        let ex = Arc::new(mem_executor());
        // Seed: one plain CREATE of the same table (migration 9's first applier).
        let seeder = ex.clone();
        let seed = tokio::spawn(async move { seeder.execute(DDL).await });
        let mut racers = Vec::new();
        for _ in 0..6 {
            let r = ex.clone();
            racers.push(tokio::spawn(async move { r.execute(DDL).await }));
        }
        let _ = seed.await.unwrap();
        for r in racers {
            let out = r.await.unwrap();
            let ok = out.is_ok();
            assert!(
                ok,
                "round {round}: concurrent IF NOT EXISTS refused: {:?}",
                out.map(|_| ()).err()
            );
        }
    }
}
