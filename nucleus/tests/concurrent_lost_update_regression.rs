//! Regression for the concurrent lost-update finding (probe_concurrency_threads).
//!
//! Deterministic (no-timing) interleaving: two sessions read-modify-write the
//! same row. Whichever writes second MUST be rejected with a serialization/
//! write-conflict error; otherwise one increment is silently lost.
//!
//! Root cause (fixed): a PK/eq UPDATE finds its target rows via the index
//! (`index_version_lookup`). When the index had been rebuilt to the latest
//! committed snapshot (`rebuild_indexes_for_table`), it no longer held the older
//! version that the second writer's snapshot still needs, so the lookup returned
//! "0 visible rows". The executor then treated the UPDATE as matching nothing and
//! skipped it entirely — bypassing the per-row write-conflict (CAS) check and
//! silently dropping the write. The fix makes `index_version_lookup` advisory for
//! positive hits only: when a value is tracked but no version is visible it
//! returns None, so the caller falls back to the authoritative MVCC chain scan,
//! which sees every physical version and triggers the CAS conflict.
#![cfg(feature = "server")]
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::MvccStorageAdapter;
use nucleus::types::Value;

async fn val(ex: &Executor, sid: u64, sql: &str) -> Option<i64> {
    let mut r = ex.execute_with_session(sid, sql).await.ok()?;
    match r.pop()? {
        ExecResult::Select { rows, .. } => match rows.first()?.first()? {
            Value::Int64(n) => Some(*n),
            Value::Int32(n) => Some(*n as i64),
            _ => None,
        },
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_rmw_must_not_lose_update() {
    for iso in ["REPEATABLE READ", "SERIALIZABLE"] {
        let ex = Arc::new(Executor::new(
            Arc::new(Catalog::new()),
            Arc::new(MvccStorageAdapter::new()),
        ));
        let s = ex.create_session();
        ex.execute_with_session(
            s,
            "CREATE TABLE c (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        )
        .await
        .unwrap();
        ex.execute_with_session(s, "INSERT INTO c (id,v) VALUES (1,0)")
            .await
            .unwrap();
        ex.drop_session(s);

        let a = ex.create_session();
        let b = ex.create_session();

        // Both begin and read v=0 under their own snapshot.
        ex.execute_with_session(a, &format!("BEGIN ISOLATION LEVEL {iso}"))
            .await
            .unwrap();
        ex.execute_with_session(b, &format!("BEGIN ISOLATION LEVEL {iso}"))
            .await
            .unwrap();
        let va = val(&ex, a, "SELECT v FROM c WHERE id=1").await.unwrap();
        let vb = val(&ex, b, "SELECT v FROM c WHERE id=1").await.unwrap();
        assert_eq!((va, vb), (0, 0), "[{iso}] both should read 0");

        // A writes v=1 and commits.
        ex.execute_with_session(a, &format!("UPDATE c SET v={} WHERE id=1", va + 1))
            .await
            .unwrap();
        ex.execute_with_session(a, "COMMIT").await.unwrap();

        // B writes v=1 from its stale read. This UPDATE (or the COMMIT) MUST fail.
        let upd = ex
            .execute_with_session(b, &format!("UPDATE c SET v={} WHERE id=1", vb + 1))
            .await;
        let com = if upd.is_ok() {
            ex.execute_with_session(b, "COMMIT").await
        } else {
            let _ = ex.execute_with_session(b, "ROLLBACK").await;
            Err(nucleus::executor::ExecError::Storage(
                nucleus::storage::StorageError::WriteConflict("update".into()),
            ))
        };
        let b_rejected = upd.is_err() || com.is_err();

        let chk = ex.create_session();
        let final_v = val(&ex, chk, "SELECT v FROM c WHERE id=1").await.unwrap();
        ex.drop_session(chk);

        println!("[{iso}] b_rejected={b_rejected} final_v={final_v}");
        assert!(
            b_rejected,
            "[{iso}] second writer must be rejected (first-updater-wins); got success → LOST UPDATE"
        );
        assert_eq!(final_v, 1, "[{iso}] final value must be 1 (one increment)");
    }
}
