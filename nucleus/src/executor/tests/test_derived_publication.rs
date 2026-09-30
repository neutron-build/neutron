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
