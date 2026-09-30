use super::*;

#[test]
fn detached_fts_rebuild_preserves_completed_concurrent_insert() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let executor = Arc::new(test_executor());
    runtime.block_on(async {
        exec(
            &executor,
            "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT, v VECTOR(4))",
        )
        .await;
        let fillers: Vec<_> = (3..515)
            .map(|id| format!("({id}, 'filler', VECTOR('[1,0,0,0]'))"))
            .collect();
        exec(
            &executor,
            &format!(
                "INSERT INTO articles VALUES (1, 'needle', VECTOR('[1,0,0,0]')), {}",
                fillers.join(",")
            ),
        )
        .await;
        exec(
            &executor,
            "CREATE INDEX articles_fts ON articles USING FTS (body)",
        )
        .await;
        // Positional IVF maintenance forces the full derived refresh path.
        exec(
            &executor,
            "CREATE INDEX articles_vector ON articles USING IVFFLAT (v)",
        )
        .await;
    });
    let (entered, paused) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    *executor.derived_publish_hook.lock() = Some(super::super::derived_coherence::PublishHook {
        kind: "fts",
        table: "articles".into(),
        entered,
        resume: resumed,
    });
    let writer = executor.clone();
    let update = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(writer.execute_with_session(
                71,
                "UPDATE articles SET body = 'needle changed' WHERE id = 1",
            ))
    });
    if let Err(error) = paused.recv_timeout(std::time::Duration::from_secs(10)) {
        if update.is_finished() {
            panic!(
                "UPDATE completed before detached publication: {:?}",
                update.join().unwrap()
            );
        }
        panic!("UPDATE never reached detached publication: {error}");
    }
    let insertion = runtime.block_on(executor.execute_with_session(
        72,
        "INSERT INTO articles VALUES (2, 'needle', VECTOR('[1,0,0,0]'))",
    ));
    resume.send(()).unwrap();
    insertion.unwrap();
    let update = update.join().unwrap().unwrap();
    assert!(matches!(
        &update[0],
        ExecResult::Command {
            rows_affected: 1,
            ..
        }
    ));
    runtime.block_on(async {
        let changed = exec(&executor, "SELECT body FROM articles WHERE id = 1").await;
        assert_eq!(
            rows(&changed[0]),
            &vec![vec![Value::Text("needle changed".into())]]
        );
        let heap = exec(
            &executor,
            "SELECT id FROM articles WHERE (body || '') @@ 'needle' ORDER BY id",
        )
        .await;
        assert_eq!(
            rows(&heap[0]),
            &vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]
        );
        let indexed = exec(
            &executor,
            "SELECT id FROM articles WHERE body @@ 'needle' ORDER BY id",
        )
        .await;
        assert_eq!(
            rows(&indexed[0]),
            rows(&heap[0]),
            "detached publication lost a completed writer's posting"
        );
    });
}

#[test]
fn detached_zone_map_rebuild_preserves_same_count_concurrent_update() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let executor = Arc::new(test_executor());
    runtime.block_on(async {
        exec(
            &executor,
            "CREATE TABLE zoned (id INT PRIMARY KEY, val INT, v VECTOR(4))",
        )
        .await;
        exec(
            &executor,
            "INSERT INTO zoned VALUES (1, 37, VECTOR('[1,0,0,0]')), (2, 37, VECTOR('[1,0,0,0]'))",
        )
        .await;
        exec(
            &executor,
            "CREATE INDEX zoned_vector ON zoned USING IVFFLAT (v)",
        )
        .await;
    });
    let (entered, paused) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    *executor.derived_publish_hook.lock() = Some(super::super::derived_coherence::PublishHook {
        kind: "zone",
        table: "zoned".into(),
        entered,
        resume: resumed,
    });
    let writer = executor.clone();
    let update = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(writer.execute_with_session(73, "UPDATE zoned SET val = 38 WHERE id = 1"))
    });
    if let Err(error) = paused.recv_timeout(std::time::Duration::from_secs(10)) {
        if update.is_finished() {
            panic!(
                "UPDATE bypassed detached publication: {:?}",
                update.join().unwrap()
            );
        }
        panic!("UPDATE never reached detached publication: {error}");
    }
    let second = runtime
        .block_on(executor.execute_with_session(74, "UPDATE zoned SET val = 99 WHERE id = 2"));
    resume.send(()).unwrap();
    let second = second.unwrap();
    assert!(matches!(
        &second[0],
        ExecResult::Command {
            rows_affected: 1,
            ..
        }
    ));
    let first = update.join().unwrap().unwrap();
    assert!(matches!(
        &first[0],
        ExecResult::Command {
            rows_affected: 1,
            ..
        }
    ));
    runtime.block_on(async {
        let heap = exec(
            &executor,
            "SELECT id FROM zoned WHERE (val + 0) > 98 ORDER BY id",
        )
        .await;
        assert_eq!(rows(&heap[0]), &vec![vec![Value::Int32(2)]]);
        let pruned = exec(&executor, "SELECT id FROM zoned WHERE val > 98 ORDER BY id").await;
        assert_eq!(
            rows(&pruned[0]),
            rows(&heap[0]),
            "same-count stale statistics pruned a committed row"
        );
    });
}

#[tokio::test]
async fn open_transaction_fts_update_does_not_hide_committed_rows_from_other_session() {
    use crate::storage::buffered_engine::BufferedDiskEngine;
    use crate::storage::disk_engine::DiskEngine;
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.path().join("t.db"), catalog.clone()).unwrap());
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    let executor = Executor::new(catalog, storage);
    exec(
        &executor,
        "CREATE TABLE committed_articles (id INT PRIMARY KEY, body TEXT)",
    )
    .await;
    let fillers: Vec<_> = (2..514).map(|id| format!("({id}, 'filler')")).collect();
    exec(
        &executor,
        &format!(
            "INSERT INTO committed_articles VALUES (1, 'needle'), {}",
            fillers.join(",")
        ),
    )
    .await;
    exec(
        &executor,
        "CREATE INDEX committed_articles_fts ON committed_articles USING FTS (body)",
    )
    .await;
    for completion in ["ROLLBACK", "COMMIT"] {
        executor.execute_with_session(81, "BEGIN").await.unwrap();
        let changed = executor
            .execute_with_session(
                81,
                "UPDATE committed_articles SET body = 'changed' WHERE id = 1",
            )
            .await
            .unwrap();
        assert!(matches!(
            &changed[0],
            ExecResult::Command {
                rows_affected: 1,
                ..
            }
        ));
        let heap = executor
            .execute_with_session(
                82,
                "SELECT id FROM committed_articles WHERE (body || '') @@ 'needle' ORDER BY id",
            )
            .await
            .unwrap();
        assert_eq!(
            rows(&heap[0]),
            &vec![vec![Value::Int32(1)]],
            "reader did not see committed old heap value"
        );
        let indexed = executor
            .execute_with_session(
                82,
                "SELECT id FROM committed_articles WHERE body @@ 'needle' ORDER BY id",
            )
            .await
            .unwrap();
        assert_eq!(
            rows(&indexed[0]),
            rows(&heap[0]),
            "uncommitted FTS hook hid a committed row"
        );
        executor.execute_with_session(81, completion).await.unwrap();
        let after = executor
            .execute_with_session(
                82,
                "SELECT id FROM committed_articles WHERE body @@ 'needle' ORDER BY id",
            )
            .await
            .unwrap();
        let expected = if completion == "ROLLBACK" {
            vec![vec![Value::Int32(1)]]
        } else {
            vec![]
        };
        assert_eq!(
            rows(&after[0]),
            &expected,
            "wrong FTS result after {completion}"
        );
    }
}

/// The storage used by the server must reject a buffered conditional delete
/// after another transaction replaces that same primary key at the same slot.
#[tokio::test]
async fn conditional_delete_commit_rejects_same_key_revision_replacement() {
    use crate::storage::buffered_engine::BufferedDiskEngine;
    use crate::storage::disk_engine::DiskEngine;
    use crate::storage::{STORAGE_SESSION_ID, StorageError};
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.path().join("cas.db"), catalog.clone()).unwrap());
    let storage = Arc::new(BufferedDiskEngine::new(disk.clone()));
    let executor = Executor::new(catalog, storage.clone());
    exec(
        &executor,
        "CREATE TABLE sessions (id INT PRIMARY KEY, revision TEXT)",
    )
    .await;
    exec(&executor, "INSERT INTO sessions VALUES (1, 'old')").await;
    let target = storage.scan_physical("sessions").await.unwrap()[0].clone();
    for sid in [91, 92] {
        STORAGE_SESSION_ID
            .scope(sid, async {
                storage.begin_txn().await.unwrap();
                assert_eq!(
                    storage
                        .delete_if_unchanged("sessions", &[target.clone()])
                        .await
                        .unwrap(),
                    1
                );
            })
            .await;
    }
    STORAGE_SESSION_ID
        .scope(91, async {
            storage
                .insert(
                    "sessions",
                    vec![Value::Int32(1), Value::Text("winner".into())],
                )
                .await
                .unwrap();
            storage.commit_txn().await.unwrap();
        })
        .await;
    let replacement = disk.scan_physical("sessions").await.unwrap()[0].clone();
    assert_eq!(
        replacement.0, target.0,
        "fixture did not exercise slot recycling"
    );
    let loser = STORAGE_SESSION_ID
        .scope(92, async {
            storage
                .insert(
                    "sessions",
                    vec![Value::Int32(1), Value::Text("loser".into())],
                )
                .await
                .unwrap();
            storage.commit_txn().await
        })
        .await;
    assert!(
        matches!(loser, Err(StorageError::WriteConflict(_))),
        "both revision-conditional transactions acknowledged success: {loser:?}"
    );
    assert_eq!(
        disk.scan("sessions").await.unwrap(),
        vec![vec![Value::Int32(1), Value::Text("winner".into())]]
    );
}

#[tokio::test]
async fn pinned_read_transaction_fts_matches_its_visible_heap_after_writer_commit() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    let executor = Executor::new(catalog, storage);
    exec(
        &executor,
        "CREATE TABLE snapshot_articles (id INT PRIMARY KEY, body TEXT)",
    )
    .await;
    let fillers: Vec<_> = (2..514).map(|id| format!("({id}, 'filler')")).collect();
    exec(
        &executor,
        &format!(
            "INSERT INTO snapshot_articles VALUES (1, 'needle'), {}",
            fillers.join(",")
        ),
    )
    .await;
    exec(
        &executor,
        "CREATE INDEX snapshot_articles_fts ON snapshot_articles USING FTS (body)",
    )
    .await;
    executor
        .execute_with_session(83, "BEGIN READ ONLY")
        .await
        .unwrap();
    let initial = executor
        .execute_with_session(
            83,
            "SELECT id FROM snapshot_articles WHERE (body || '') @@ 'needle' ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(rows(&initial[0]), &vec![vec![Value::Int32(1)]]);
    let changed = executor
        .execute_with_session(
            84,
            "UPDATE snapshot_articles SET body = 'changed' WHERE id = 1",
        )
        .await
        .unwrap();
    assert!(matches!(
        &changed[0],
        ExecResult::Command {
            rows_affected: 1,
            ..
        }
    ));
    let current = executor
        .execute_with_session(
            84,
            "SELECT id FROM snapshot_articles WHERE (body || '') @@ 'needle' ORDER BY id",
        )
        .await
        .unwrap();
    assert!(
        rows(&current[0]).is_empty(),
        "competing update did not commit"
    );
    let heap = executor
        .execute_with_session(
            83,
            "SELECT id FROM snapshot_articles WHERE (body || '') @@ 'needle' ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows(&heap[0]),
        &vec![vec![Value::Int32(1)]],
        "reader snapshot was not pinned"
    );
    let indexed = executor
        .execute_with_session(
            83,
            "SELECT id FROM snapshot_articles WHERE body @@ 'needle' ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows(&indexed[0]),
        rows(&heap[0]),
        "current sidecar hid older snapshot row"
    );
    executor.execute_with_session(83, "ROLLBACK").await.unwrap();
}
