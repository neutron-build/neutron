//! Document-store collections: the isolation `DOC_*` promises when a caller
//! names one.
//!
//! Collections existed in every SDK signature as a parameter that reached an
//! engine with no concept of them — `Find`/`Update`/`Delete` took a collection,
//! addressed one global store, and returned everything (GO-055). A caller
//! could believe tenants were separated while every document was shared. These
//! tests are the contract that makes the parameter mean something: a document
//! belongs to exactly one collection, and an operation naming a collection sees
//! only that one.

use super::*;

/// The scalar text a `SELECT doc_fn(...)` returned, unwrapped.
async fn scalar(ex: &Executor, sql: &str) -> Value {
    let results = exec(ex, sql).await;
    let r = rows(&results[0]);
    assert_eq!(r.len(), 1, "expected exactly one row from {sql}");
    r[0][0].clone()
}

async fn insert_into(ex: &Executor, collection: &str, json: &str) -> i64 {
    let v = scalar(ex, &format!("SELECT DOC_INSERT('{collection}', '{json}')")).await;
    match v {
        Value::Int64(id) => id,
        other => panic!("DOC_INSERT returned {other:?}"),
    }
}

#[tokio::test]
async fn documents_in_two_collections_cannot_see_each_other() {
    let ex = test_executor();
    let a = insert_into(&ex, "tenant_a", r#"{"name": "shared", "secret": "a"}"#).await;
    let b = insert_into(&ex, "tenant_b", r#"{"name": "shared", "secret": "b"}"#).await;

    // Each reads its own.
    let got_a = scalar(&ex, &format!("SELECT DOC_GET('tenant_a', {a})")).await;
    assert!(
        matches!(&got_a, Value::Text(t) if t.contains("\"a\"")),
        "a collection must read its own document, got {got_a:?}"
    );

    // Neither reads the other's, even holding the id.
    let cross = scalar(&ex, &format!("SELECT DOC_GET('tenant_a', {b})")).await;
    assert_eq!(
        cross,
        Value::Null,
        "a document in another collection must read as absent"
    );

    // A query that matches both documents returns only the caller's.
    let ids = scalar(&ex, r#"SELECT DOC_QUERY('tenant_a', '{"name": "shared"}')"#).await;
    assert_eq!(
        ids,
        Value::Text(a.to_string()),
        "a collection-scoped query must not return another collection's matches"
    );
}

#[tokio::test]
async fn update_and_delete_cannot_reach_another_collection() {
    let ex = test_executor();
    let victim = insert_into(&ex, "tenant_b", r#"{"secret": "b"}"#).await;

    // An update naming the wrong collection reports "not found" and changes
    // nothing — it must not be able to overwrite across the boundary.
    let updated = scalar(
        &ex,
        &format!(r#"SELECT DOC_UPDATE('tenant_a', {victim}, '{{"secret": "stolen"}}')"#),
    )
    .await;
    assert_eq!(updated, Value::Bool(false));

    // A delete naming the wrong collection likewise.
    let deleted = scalar(&ex, &format!("SELECT DOC_DELETE('tenant_a', {victim})")).await;
    assert_eq!(deleted, Value::Bool(false));

    // The document is untouched and still the owner's.
    let still = scalar(&ex, &format!("SELECT DOC_GET('tenant_b', {victim})")).await;
    assert!(
        matches!(&still, Value::Text(t) if t.contains("\"b\"")),
        "a cross-collection update/delete modified the document: {still:?}"
    );

    // The owner can delete it.
    let owner_deleted = scalar(&ex, &format!("SELECT DOC_DELETE('tenant_b', {victim})")).await;
    assert_eq!(owner_deleted, Value::Bool(true));
}

#[tokio::test]
async fn path_reads_are_scoped_too() {
    // DOC_PATH_IN exists because DOC_PATH's variadic tail makes an arity
    // overload ambiguous. Without it, a path read would be the one hole in the
    // boundary: every other verb scoped, and this one able to pull a field out
    // of any collection's document by id.
    let ex = test_executor();
    let b = insert_into(&ex, "tenant_b", r#"{"secret": "b"}"#).await;

    let own = scalar(
        &ex,
        &format!("SELECT DOC_PATH_IN('tenant_b', {b}, 'secret')"),
    )
    .await;
    assert_eq!(own, Value::Text("\"b\"".into()));

    let cross = scalar(
        &ex,
        &format!("SELECT DOC_PATH_IN('tenant_a', {b}, 'secret')"),
    )
    .await;
    assert_eq!(
        cross,
        Value::Null,
        "DOC_PATH_IN read across a collection boundary"
    );
}

#[tokio::test]
async fn the_collection_less_api_is_its_own_collection_not_a_view_of_all() {
    // The important half of backward compatibility. Documents written by the
    // old, collection-less calls land in the default collection and keep
    // working exactly as before. What they must NOT do is see into named
    // collections — otherwise dropping the argument would be an exfiltration
    // path, and "isolated by collection" would be false the moment any caller
    // used the old spelling.
    let ex = test_executor();
    let legacy = match scalar(&ex, r#"SELECT DOC_INSERT('{"name": "shared"}')"#).await {
        Value::Int64(id) => id,
        other => panic!("DOC_INSERT returned {other:?}"),
    };
    let named = insert_into(&ex, "tenant_a", r#"{"name": "shared"}"#).await;

    // Legacy read of a legacy document: unchanged.
    let got = scalar(&ex, &format!("SELECT DOC_GET({legacy})")).await;
    assert!(matches!(&got, Value::Text(t) if t.contains("shared")));

    // Legacy read of a NAMED collection's document: absent.
    let cross = scalar(&ex, &format!("SELECT DOC_GET({named})")).await;
    assert_eq!(
        cross,
        Value::Null,
        "the collection-less API reached into a named collection"
    );

    // Legacy query returns only default-collection documents.
    let ids = scalar(&ex, r#"SELECT DOC_QUERY('{"name": "shared"}')"#).await;
    assert_eq!(ids, Value::Text(legacy.to_string()));

    // And the counts agree with that split.
    assert_eq!(scalar(&ex, "SELECT DOC_COUNT()").await, Value::Int64(1));
    assert_eq!(
        scalar(&ex, "SELECT DOC_COUNT('tenant_a')").await,
        Value::Int64(1)
    );
    assert_eq!(
        scalar(&ex, "SELECT DOC_COUNT('nonexistent')").await,
        Value::Int64(0)
    );
}

#[tokio::test]
async fn an_update_keeps_a_document_in_its_collection() {
    // `insert_with_id` is how DOC_UPDATE replaces a body. If it defaulted the
    // collection instead of preserving it, an ordinary update would silently
    // move the document into the default collection — losing the isolation
    // without any call naming a different one.
    let ex = test_executor();
    let id = insert_into(&ex, "tenant_a", r#"{"v": 1}"#).await;

    let updated = scalar(
        &ex,
        &format!(r#"SELECT DOC_UPDATE('tenant_a', {id}, '{{"v": 2}}')"#),
    )
    .await;
    assert_eq!(updated, Value::Bool(true));

    let still_scoped = scalar(&ex, &format!("SELECT DOC_GET('tenant_a', {id})")).await;
    assert!(
        matches!(&still_scoped, Value::Text(t) if t.contains("2")),
        "the update did not land: {still_scoped:?}"
    );
    assert_eq!(
        scalar(&ex, &format!("SELECT DOC_GET({id})")).await,
        Value::Null,
        "the update moved the document out of its collection"
    );
}

#[tokio::test]
async fn a_collection_is_not_taken_from_a_non_string_argument() {
    // `DOC_GET(1, 2)` must not quietly become collection "1": that would route
    // a read somewhere the caller never named. Refusing is the only safe
    // reading of an argument in the collection position.
    let ex = test_executor();
    let results = ex.execute("SELECT DOC_GET(1, 2)").await;
    assert!(
        results.is_err(),
        "a non-string collection argument must be refused, got {results:?}"
    );
}
