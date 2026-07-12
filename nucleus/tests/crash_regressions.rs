//! Regression tests for crashes/aborts surfaced by the crash/panic fuzzer
//! (`src/bin/probe_crash.rs`). These previously aborted the process (unbounded
//! allocation from a user-controlled size) or panicked (out-of-bounds slice).
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use std::sync::Arc;

async fn fresh() -> Arc<Executor> {
    let c = Arc::new(Catalog::new());
    let s: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    Arc::new(Executor::new(c, s))
}
async fn val(ex: &Executor, sql: &str) -> Result<String, String> {
    match ex.execute(sql).await {
        Ok(mut r) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => Ok(rows
                .first()
                .and_then(|r| r.first())
                .map(|v| v.to_string())
                .unwrap_or_default()),
            o => Err(format!("{o:?}")),
        },
        Err(e) => Err(format!("{e:?}")),
    }
}

/// LPAD/RPAD/REPEAT with a negative length yield empty (Postgres semantics);
/// with an extreme positive length they error instead of attempting an
/// ~i64::MAX-byte allocation that aborts the process.
#[tokio::test]
async fn pad_repeat_length_is_bounded() {
    let ex = fresh().await;
    assert_eq!(
        val(&ex, "SELECT LPAD('hi',-9223372036854775808)")
            .await
            .unwrap(),
        ""
    );
    assert_eq!(val(&ex, "SELECT RPAD('hi',-5)").await.unwrap(), "");
    assert_eq!(val(&ex, "SELECT REPEAT('ab',-1)").await.unwrap(), "");
    assert_eq!(val(&ex, "SELECT REPEAT('ab',0)").await.unwrap(), "");
    assert_eq!(val(&ex, "SELECT RPAD('ab',5,'xy')").await.unwrap(), "abxyx");

    assert!(val(&ex, "SELECT LPAD('hi',9000000000)").await.is_err());
    assert!(val(&ex, "SELECT RPAD('hi',9000000000)").await.is_err());
    assert!(val(&ex, "SELECT REPEAT('hi',9000000000)").await.is_err());
}

/// GENERATE_SERIES bounds its cardinality rather than building a multi-billion
/// element vector.
#[tokio::test]
async fn generate_series_is_bounded() {
    let ex = fresh().await;
    // small series still works
    assert!(val(&ex, "SELECT GENERATE_SERIES(1,3)").await.is_ok());
    // enormous range errors instead of OOM-aborting
    assert!(
        val(&ex, "SELECT GENERATE_SERIES(1,9223372036854775807)")
            .await
            .is_err()
    );
}

/// TENSOR_STORE hex_data of odd length must not panic (it sliced a 2-byte
/// window past the end of the string).
#[tokio::test]
async fn tensor_store_odd_hex_no_panic() {
    let ex = fresh().await;
    // 'false' is 5 chars (odd) — previously panicked "byte index 6 out of bounds".
    let r = val(
        &ex,
        "SELECT TENSOR_STORE('t','1','[1,2,3]','float32','false')",
    )
    .await;
    // Either Ok or a graceful Err is acceptable; the point is no panic/abort.
    let _ = r;
    // An odd hex string should be handled without crashing.
    let r2 = val(&ex, "SELECT TENSOR_STORE('t2','1','[2]','int8','abc')").await;
    let _ = r2;
}
