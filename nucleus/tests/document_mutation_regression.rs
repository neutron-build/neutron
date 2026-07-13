//! Regression: the document store now exposes DOC_UPDATE / DOC_DELETE over
//! SQL. The neutron-nucleus TS client used to run `UPDATE documents` /
//! `DELETE FROM documents` against a relation that does not exist (the
//! document store is a specialty store reached only via DOC_* functions).
//! Also pins that DOC_* id arguments accept the TEXT-encoded integers a
//! pgwire client (node-postgres) sends.
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

async fn scalar(ex: &Executor, sql: &str) -> String {
    match ex.execute(sql).await {
        Ok(mut r) => match r.pop() {
            Some(ExecResult::Select { rows, .. }) => rows
                .first()
                .and_then(|r| r.first())
                .map(|v| v.to_string())
                .unwrap_or_default(),
            o => panic!("expected Select, got {o:?} for `{sql}`"),
        },
        Err(e) => panic!("`{sql}` errored: {e:?}"),
    }
}

#[tokio::test]
async fn doc_update_replaces_in_place() {
    let ex = fresh().await;
    let id = scalar(&ex, r#"SELECT DOC_INSERT('{"name":"a","n":1}')"#).await;

    // Update by id — must preserve the id and replace the body.
    let ok = scalar(&ex, &format!(r#"SELECT DOC_UPDATE({id}, '{{"name":"b","n":2}}')"#)).await;
    assert_eq!(ok, "true");

    let got = scalar(&ex, &format!("SELECT DOC_GET({id})")).await;
    assert!(got.contains("\"b\""), "updated body not stored: {got}");
    assert!(got.contains("\"n\":2") || got.contains("2"), "n not updated: {got}");

    // Count stays 1 (replace, not insert).
    assert_eq!(scalar(&ex, "SELECT DOC_COUNT()").await, "1");
}

#[tokio::test]
async fn doc_update_missing_returns_false() {
    let ex = fresh().await;
    assert_eq!(
        scalar(&ex, r#"SELECT DOC_UPDATE(9999, '{"x":1}')"#).await,
        "false"
    );
}

#[tokio::test]
async fn doc_delete_removes() {
    let ex = fresh().await;
    let id = scalar(&ex, r#"SELECT DOC_INSERT('{"k":1}')"#).await;
    assert_eq!(scalar(&ex, "SELECT DOC_COUNT()").await, "1");

    assert_eq!(scalar(&ex, &format!("SELECT DOC_DELETE({id})")).await, "true");
    assert_eq!(scalar(&ex, "SELECT DOC_COUNT()").await, "0");
    // Second delete is a no-op.
    assert_eq!(scalar(&ex, &format!("SELECT DOC_DELETE({id})")).await, "false");
    assert_eq!(scalar(&ex, &format!("SELECT DOC_GET({id})")).await, "NULL");
}

#[tokio::test]
async fn doc_functions_accept_text_encoded_ids() {
    // A pgwire client (node-postgres) sends bound params as TEXT: DOC_GET('1')
    // must work the same as DOC_GET(1). This is what the neutron-nucleus
    // client relies on (it never CASTs the id).
    let ex = fresh().await;
    let id = scalar(&ex, r#"SELECT DOC_INSERT('{"v":42}')"#).await;

    let got = scalar(&ex, &format!("SELECT DOC_GET('{id}')")).await;
    assert!(got.contains("42"), "text-id DOC_GET failed: {got}");

    assert_eq!(
        scalar(&ex, &format!(r#"SELECT DOC_UPDATE('{id}', '{{"v":43}}')"#)).await,
        "true"
    );
    assert_eq!(scalar(&ex, &format!("SELECT DOC_DELETE('{id}')")).await, "true");
}
