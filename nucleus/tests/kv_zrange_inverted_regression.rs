//! Regression for `kv_zrange_inverted`: ZRANGE / ZREVRANGE with an inverted
//! rank range (start > stop) must return an EMPTY result, matching Redis
//! ZRANGE/ZREVRANGE semantics. Previously `SortedSet::{zrange,zrevrange}` used
//! `.take(stop.saturating_sub(start) + 1)`, which floored to `take(1)` when
//! start > stop and emitted one spurious element at rank `start`.
//!
//! Covered through two surfaces:
//!  - the SQL `KV_ZRANGE` scalar function (executor path), and
//!  - the embedded `KvHandle::col_zrange` / `col_zrevrange` API.
#![cfg(feature = "server")]
use nucleus::catalog::Catalog;
use nucleus::embedded::Database;
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

/// SQL surface: KV_ZRANGE with start > stop must be empty; start == stop and
/// normal ascending ranges are unchanged.
#[tokio::test]
async fn kv_zrange_sql_inverted_range_is_empty() {
    let ex = fresh().await;
    // 3-element sorted set: a(1) < b(2) < c(3).
    one(&ex, "SELECT KV_ZADD('z',1,'a')").await.unwrap();
    one(&ex, "SELECT KV_ZADD('z',2,'b')").await.unwrap();
    one(&ex, "SELECT KV_ZADD('z',3,'c')").await.unwrap();

    // Inverted ranges must be empty (the bug returned one spurious element).
    assert_eq!(one(&ex, "SELECT KV_ZRANGE('z',2,1)").await.unwrap(), "");
    assert_eq!(one(&ex, "SELECT KV_ZRANGE('z',2,0)").await.unwrap(), "");

    // start == stop still returns exactly one element (unchanged behavior).
    assert_eq!(one(&ex, "SELECT KV_ZRANGE('z',0,0)").await.unwrap(), "a:1");

    // Normal ascending range still works (unchanged behavior).
    assert_eq!(
        one(&ex, "SELECT KV_ZRANGE('z',0,2)").await.unwrap(),
        "a:1,b:2,c:3"
    );
}

/// Embedded API surface: both col_zrange and col_zrevrange honor the inverted
/// guard, while start == stop and normal ranges are unchanged.
#[test]
fn kv_embedded_zrange_zrevrange_inverted_range_is_empty() {
    let db = Database::memory();
    let kv = db.kv();
    kv.col_zadd("z", "a", 1.0).unwrap();
    kv.col_zadd("z", "b", 2.0).unwrap();
    kv.col_zadd("z", "c", 3.0).unwrap();

    // Inverted ranges -> empty.
    assert!(kv.col_zrange("z", 2, 1).unwrap().is_empty());
    assert!(kv.col_zrange("z", 2, 0).unwrap().is_empty());
    assert!(kv.col_zrevrange("z", 2, 1).unwrap().is_empty());
    assert!(kv.col_zrevrange("z", 1, 0).unwrap().is_empty());

    // start == stop -> exactly one element (boundary preserved).
    let asc = kv.col_zrange("z", 0, 0).unwrap();
    assert_eq!(asc.len(), 1);
    assert_eq!(asc[0].member, "a");
    let desc = kv.col_zrevrange("z", 0, 0).unwrap();
    assert_eq!(desc.len(), 1);
    assert_eq!(desc[0].member, "c");

    // Normal ranges still span the whole set.
    assert_eq!(kv.col_zrange("z", 0, 2).unwrap().len(), 3);
    assert_eq!(kv.col_zrevrange("z", 0, 2).unwrap().len(), 3);
}
