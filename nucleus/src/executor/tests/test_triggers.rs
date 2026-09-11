//! Row-level trigger firing stages its row bindings in engine-global tables
//! literally named `_new`/`_old`. Before EXE-2, setup failures were
//! eprintln-swallowed and teardown dropped the NAME unconditionally — a user
//! table called `_new` had trigger rows inserted into it and was then
//! DROPPed by teardown. Silent data loss from an unrelated INSERT.

use super::*;

/// A user table named `_new` must never be touched by trigger firing: the
/// statement must fail loudly instead of inserting into (and then dropping)
/// the user's table.
#[tokio::test]
async fn user_table_named_new_is_never_touched_by_trigger_firing() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE _new (payload TEXT)").await;
    exec(&ex, "INSERT INTO _new VALUES ('user-data')").await;
    exec(&ex, "CREATE TABLE watched (id INT)").await;
    exec(
        &ex,
        "CREATE TRIGGER watch_ins AFTER INSERT ON watched FOR EACH ROW BEGIN INSERT INTO _new VALUES ('trigger-ran'); END",
    )
    .await;

    // Pre-fix: succeeded silently, inserted 'trigger-ran' into the user's
    // table and DROPPED it at teardown.
    let res = ex.execute("INSERT INTO watched VALUES (1)").await;
    match res {
        Err(e) => assert!(
            e.to_string().contains("_new"),
            "the error must name the occupied binding table: {e}"
        ),
        Ok(_) => panic!("INSERT must fail loudly when the _new binding name is occupied"),
    }

    // The user's table and its rows survive byte-identical.
    let got = rows(&exec(&ex, "SELECT payload FROM _new").await[0]).clone();
    assert_eq!(got, vec![vec![Value::Text("user-data".into())]]);
}

/// Same for `_old` on the UPDATE/DELETE paths.
#[tokio::test]
async fn user_table_named_old_is_never_touched_by_trigger_firing() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE _old (payload TEXT)").await;
    exec(&ex, "INSERT INTO _old VALUES ('keep')").await;
    exec(&ex, "CREATE TABLE watched (id INT)").await;
    exec(&ex, "INSERT INTO watched VALUES (1)").await;
    exec(
        &ex,
        "CREATE TRIGGER watch_del AFTER DELETE ON watched FOR EACH ROW BEGIN SELECT 1; END",
    )
    .await;

    let res = ex.execute("DELETE FROM watched").await;
    assert!(
        res.is_err(),
        "DELETE must fail loudly when _old is occupied"
    );
    let got = rows(&exec(&ex, "SELECT payload FROM _old").await[0]).clone();
    assert_eq!(got, vec![vec![Value::Text("keep".into())]]);
}

/// After a firing, the binding tables must be gone (no name squatting on the
/// engine-global namespace).
#[tokio::test]
async fn binding_tables_do_not_leak_after_firing() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE watched (id INT)").await;
    exec(
        &ex,
        "CREATE TRIGGER t_ins AFTER INSERT ON watched FOR EACH ROW BEGIN SELECT 1; END",
    )
    .await;
    exec(&ex, "INSERT INTO watched VALUES (1)").await;
    assert!(
        ex.execute("SELECT * FROM _new").await.is_err(),
        "_new must not exist after the firing"
    );
}

/// Two sessions firing row-level triggers concurrently must not interleave
/// rows into the same binding tables or drop them under each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_firings_do_not_interleave() {
    let ex = std::sync::Arc::new(test_executor());
    exec(&ex, "CREATE TABLE watched (id INT)").await;
    exec(
        &ex,
        "CREATE TRIGGER t_conc AFTER INSERT ON watched FOR EACH ROW BEGIN SELECT 1; END",
    )
    .await;

    let mut handles = Vec::new();
    for t in 0..4 {
        let ex = ex.clone();
        handles.push(tokio::spawn(async move {
            let sid = ex.create_session();
            for i in 0..25 {
                ex.execute_with_session(sid, &format!("INSERT INTO watched VALUES ({t}{i:02})"))
                    .await
                    .expect("insert under concurrency");
            }
        }));
    }
    for h in handles {
        h.await.expect("task panicked");
    }
    let got = rows(&exec(&ex, "SELECT COUNT(*) FROM watched").await[0]).clone();
    assert_eq!(got[0][0], Value::Int32(100));
}

// ======================================================================
// DROP TRIGGER addressing + authorization (audit A18)
// ======================================================================

async fn two_same_named_triggers(ex: &Executor) {
    exec(&ex, "CREATE TABLE t_a (id INT)").await;
    exec(&ex, "CREATE TABLE t_b (id INT)").await;
    exec(
        &ex,
        "CREATE TRIGGER trg AFTER INSERT ON t_a FOR EACH ROW BEGIN SELECT 1; END",
    )
    .await;
    exec(
        &ex,
        "CREATE TRIGGER trg AFTER INSERT ON t_b FOR EACH ROW BEGIN SELECT 1; END",
    )
    .await;
}

async fn trigger_names(ex: &Executor) -> Vec<(String, String)> {
    ex.triggers
        .read()
        .await
        .iter()
        .map(|t| (t.name.clone(), t.table_name.clone()))
        .collect()
}

/// DROP TRIGGER removes only the (table, trigger) pair it addresses — the old
/// bare-name retain swept same-named triggers off every other table.
#[tokio::test]
async fn drop_trigger_removes_only_the_addressed_table_pair() {
    let ex = test_executor();
    two_same_named_triggers(&ex).await;

    exec(&ex, "DROP TRIGGER trg ON t_a").await;

    let left = trigger_names(&ex).await;
    assert_eq!(
        left,
        vec![("trg".to_string(), "t_b".to_string())],
        "dropping trg on t_a must leave the same-named trigger on t_b"
    );
}

/// Same authority as every other DROP statement: a non-superuser is refused
/// and the trigger set is left intact.
#[tokio::test]
async fn drop_trigger_requires_security_admin() {
    let ex = test_executor();
    two_same_named_triggers(&ex).await;
    exec(&ex, "CREATE ROLE nobody LOGIN PASSWORD 'x'").await;

    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "nobody").await.unwrap();
    let err = ex.execute_with_session(sid, "DROP TRIGGER trg ON t_a").await;
    match err {
        Err(ExecError::PermissionDenied(msg)) => {
            assert!(msg.contains("superuser"), "got: {msg}")
        }
        Err(other) => panic!("expected PermissionDenied, got: {other}"),
        Ok(v) => panic!("non-superuser dropped a trigger: {v:?}"),
    }
    assert_eq!(
        trigger_names(&ex).await.len(),
        2,
        "unauthorized drop must leave triggers intact"
    );
}

/// IF EXISTS with the wrong relation is a harmless no-op; a miss without it is
/// an error naming the relation; and the bare form — which used to sweep every
/// table — is refused outright.
#[tokio::test]
async fn drop_trigger_if_exists_with_wrong_relation_is_a_noop() {
    let ex = test_executor();
    two_same_named_triggers(&ex).await;

    exec(&ex, "DROP TRIGGER IF EXISTS trg ON missing_table").await;
    assert_eq!(trigger_names(&ex).await.len(), 2, "IF EXISTS must be a no-op");

    assert!(
        ex.execute("DROP TRIGGER nope ON t_a").await.is_err(),
        "dropping a trigger that does not exist on that relation must error"
    );

    assert!(
        ex.execute("DROP TRIGGER trg").await.is_err(),
        "bare DROP TRIGGER must not fall back to a cross-table sweep"
    );
    assert_eq!(
        trigger_names(&ex).await.len(),
        2,
        "no variant above may remove a trigger"
    );
}
