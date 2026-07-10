//! Regression tests for KV-model correctness bugs surfaced by the KV
//! differential fuzzer (`src/bin/probe_kv.rs`, Nucleus vs a Redis-semantics
//! reference).
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
async fn one(ex: &Executor, sql: &str) -> Result<String, String> {
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

/// INCR must work on a key whose value was SET as an integer-formatted string —
/// that's the normal Redis pattern (`SET k 5` stores the string "5"). Previously
/// incr_by only accepted natively-typed Int values and errored on Text.
#[tokio::test]
async fn incr_parses_string_integers() {
    let ex = fresh().await;
    one(&ex, "SELECT KV_SET('k','5')").await.unwrap();
    assert_eq!(one(&ex, "SELECT KV_INCR('k')").await.unwrap(), "6");

    one(&ex, "SELECT KV_SET('m','-3')").await.unwrap();
    assert_eq!(one(&ex, "SELECT KV_INCR('m',2)").await.unwrap(), "-1");

    // Non-integer string is still an error (Redis-compatible).
    one(&ex, "SELECT KV_SET('bad','abc')").await.unwrap();
    assert!(one(&ex, "SELECT KV_INCR('bad')").await.is_err());
}

/// A negative LINDEX that underflows past the front is out of range → NULL,
/// not a clamp to element 0.
#[tokio::test]
async fn lindex_negative_underflow_is_null() {
    let ex = fresh().await;
    one(&ex, "SELECT KV_RPUSH('l','a')").await.unwrap();
    one(&ex, "SELECT KV_RPUSH('l','b')").await.unwrap(); // l = [a,b]
    assert_eq!(one(&ex, "SELECT KV_LINDEX('l',-1)").await.unwrap(), "b");
    assert_eq!(one(&ex, "SELECT KV_LINDEX('l',-2)").await.unwrap(), "a");
    assert_eq!(one(&ex, "SELECT KV_LINDEX('l',-3)").await.unwrap(), "NULL");
    assert_eq!(one(&ex, "SELECT KV_LINDEX('l',5)").await.unwrap(), "NULL");

    one(&ex, "SELECT KV_RPUSH('o','x')").await.unwrap(); // o = [x]
    assert_eq!(one(&ex, "SELECT KV_LINDEX('o',-2)").await.unwrap(), "NULL");
}

/// LRANGE: start underflow clamps to 0 (Redis), but stop underflowing past the
/// front yields an empty range — not a clamp to element 0.
#[tokio::test]
async fn lrange_stop_underflow_is_empty() {
    let ex = fresh().await;
    one(&ex, "SELECT KV_RPUSH('l','a')").await.unwrap();
    one(&ex, "SELECT KV_RPUSH('l','b')").await.unwrap(); // l = [a,b]
    // KV collections speak JSON on the wire since b9d0bf6 (comma-corruption fix).
    assert_eq!(one(&ex, "SELECT KV_LRANGE('l',0,-1)").await.unwrap(), r#"["a","b"]"#);
    assert_eq!(one(&ex, "SELECT KV_LRANGE('l',0,-2)").await.unwrap(), r#"["a"]"#);
    assert_eq!(one(&ex, "SELECT KV_LRANGE('l',0,-3)").await.unwrap(), "[]"); // empty
    assert_eq!(one(&ex, "SELECT KV_LRANGE('l',-100,1)").await.unwrap(), r#"["a","b"]"#); // start clamps
}
