//! N22 — concurrent UPDATEs must not silently lose writes.
//!
//! `UPDATE t SET n = n + 1 WHERE id = 1` computes its new value from the value
//! it read. Between that read and the write the statement awaits — triggers,
//! RLS, CHECK/FK constraints, derived-index maintenance — and another session
//! can commit its own increment inside that window.
//!
//! The engine did have a re-check before writing, and it passed anyway. It
//! asked whether the address still held the same ROW, comparing primary key
//! columns, and the primary key is exactly what a counter increment does not
//! touch. So both writes landed, the second erasing the first, and **both
//! reported `UPDATE 1`** — acknowledged, not skipped, no error. Found against a
//! live server over pgwire: 4 sessions x 100 increments landed 380-392 of 400,
//! reproducing 5 runs out of 5. Default isolation is `read committed`, where
//! PostgreSQL forbids this outright: it blocks on the row lock and then
//! re-evaluates the assignment against whatever the winner committed.
//!
//! A table with no primary key failed differently, which is why the mechanism
//! was once written off as refuted: with no key columns to compare, the check
//! falls back to comparing every column, catches the conflict, and skips the
//! write — losing the increment just the same, but reporting `UPDATE 0`. Both
//! shapes lose writes; only one of them lies about it. Both are covered here.
//!
//! Asserted below is the OUTCOME — the final counter — not the mechanism. Any
//! implementation that lets every acknowledged increment land passes.

use super::*;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::disk_engine::DiskEngine;

/// A disk-backed executor, matching what `main.rs` constructs. The bug lives in
/// the paged engine's write path, so an in-memory engine would prove nothing.
fn disk_executor(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.join("t.db"), catalog.clone()).unwrap());
    let engine: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    Arc::new(Executor::new(catalog, engine))
}

const WORKERS: usize = 4;
const INCREMENTS: usize = 100;

async fn read_counter(ex: &Arc<Executor>) -> i64 {
    let res = ex.execute("SELECT n FROM ctr WHERE id = 1").await.unwrap();
    match &res[0] {
        ExecResult::Select { rows, .. } => match rows[0][0] {
            Value::Int64(v) => v,
            Value::Int32(v) => v as i64,
            ref other => panic!("non-integer counter: {other:?}"),
        },
        other => panic!("expected a row, got {other:?}"),
    }
}

/// Every increment that was acknowledged has to be in the counter.
async fn increments_are_not_lost(create: &str) {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    ex.execute(create).await.unwrap();
    ex.execute("INSERT INTO ctr VALUES (1, 0)").await.unwrap();

    let mut tasks = Vec::new();
    for w in 0..WORKERS {
        let ex = ex.clone();
        tasks.push(tokio::spawn(async move {
            let mut acknowledged = 0usize;
            for _ in 0..INCREMENTS {
                // Autocommit, one statement per call: the shape a client sends,
                // and the shape that loses writes. Each worker is its own
                // session so the writes are genuinely concurrent.
                let res = ex
                    .execute_with_session(w as u64 + 1, "UPDATE ctr SET n = n + 1 WHERE id = 1")
                    .await
                    .unwrap();
                if let ExecResult::Command { rows_affected, .. } = &res[0] {
                    acknowledged += *rows_affected;
                }
            }
            acknowledged
        }));
    }

    let mut acknowledged = 0usize;
    for t in tasks {
        acknowledged += t.await.unwrap();
    }

    let total = (WORKERS * INCREMENTS) as i64;
    let counter = read_counter(&ex).await;

    // The stronger assertion, and the one that fails on the original bug: the
    // engine acknowledged N increments, so N increments must have happened.
    assert_eq!(
        counter,
        acknowledged as i64,
        "{acknowledged} increments were acknowledged with `UPDATE 1` but the counter \
         reached {counter} — {} acknowledged writes were overwritten by a concurrent \
         session and silently discarded",
        acknowledged as i64 - counter
    );

    // And nothing may be quietly dropped by reporting `UPDATE 0` either: every
    // statement issued has to have taken effect.
    assert_eq!(
        counter, total,
        "{total} statements ran but the counter reached {counter}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_are_not_lost_with_a_primary_key() {
    increments_are_not_lost("CREATE TABLE ctr (id INT PRIMARY KEY, n INT)").await;
}

/// The no-primary-key shape. Here the pre-existing check does fire, so the
/// failure was a skipped write reported as `UPDATE 0` rather than a silent
/// overwrite — a different lie with the same missing increment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_are_not_lost_without_a_primary_key() {
    increments_are_not_lost("CREATE TABLE ctr (id INT, n INT)").await;
}

/// A row whose value changed under the statement must not be written over, and
/// the engine has to say exactly WHICH rows it refused.
///
/// A bare count is not enough for the caller to recover: it cannot tell "row 3
/// conflicted" from "row 7 did", and re-evaluating the wrong one applies the
/// increment twice. This is the contract the executor's retry depends on.
#[tokio::test]
async fn a_value_that_moved_is_refused_and_named() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::new());
    let engine = Arc::new(DiskEngine::open(&dir.path().join("t.db"), catalog.clone()).unwrap());

    // Set the table up through SQL, then address the paged engine directly:
    // this test is about the storage contract, and going through the executor
    // for the write would test the retry loop instead.
    let buffered: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(engine.clone()));
    let ex = Arc::new(Executor::new(catalog.clone(), buffered));
    ex.execute("CREATE TABLE t (id INT, n INT)").await.unwrap();
    for i in 0..3 {
        ex.execute(&format!("INSERT INTO t VALUES ({i}, 0)"))
            .await
            .unwrap();
    }

    let rows = engine.scan_physical("t").await.unwrap();
    let (pos0, read0) = rows[0].clone();
    let (pos1, read1) = rows[1].clone();
    let (pos2, read2) = rows[2].clone();

    // Another session moves row 1 after we read it.
    engine
        .update("t", &[(pos1, vec![Value::Int32(1), Value::Int32(99)])])
        .await
        .unwrap();

    // All three writes are computed from the values we read; only two are still
    // valid.
    let applied = engine
        .update_if_value_unchanged(
            "t",
            &[
                (pos0, read0, vec![Value::Int32(0), Value::Int32(1)]),
                (pos1, read1, vec![Value::Int32(1), Value::Int32(1)]),
                (pos2, read2, vec![Value::Int32(2), Value::Int32(1)]),
            ],
        )
        .await
        .unwrap();

    assert_eq!(applied.len(), 2, "the stale write must be refused");
    assert!(
        applied.contains(&pos0) && applied.contains(&pos2),
        "the engine must name the positions it wrote: got {applied:?}, expected {pos0} and {pos2}"
    );
    assert!(
        !applied.contains(&pos1),
        "the refused position was reported as written"
    );

    // The concurrent write survives untouched — this is the whole point.
    let after = engine.scan("t").await.unwrap();
    let row1 = after.iter().find(|r| r[0] == Value::Int32(1)).unwrap();
    assert_eq!(
        row1[1],
        Value::Int32(99),
        "the concurrent write was overwritten by a stale read-modify-write"
    );
}
