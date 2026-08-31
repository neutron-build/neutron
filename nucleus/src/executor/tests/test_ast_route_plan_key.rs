//! `execute_statements_with_session` must never inherit a plan-cache key.
//!
//! It is the one entry point that does not parse on the way in, so it is the
//! one that cannot refresh the session's plan-cache-key hint for the statement
//! it is about to run. It used to read whatever the slot happened to hold. A
//! plan carries its own projection, so executing under another statement's key
//! returns that statement's COLUMNS — which is how the pgwire Describe probe
//! came to answer `SELECT id, name_22 FROM tbl_22 …` with `["id", "name_7"]`.
//!
//! Distinct column NAMES per table here, and the assertion is on the names, so
//! a plan borrowed from the neighbouring table is unmistakable — two tables
//! with the same shape would hide the bug completely.

use super::*;

fn mem_executor() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MemoryEngine::new());
    Arc::new(Executor::new(catalog, storage))
}

fn columns_of(result: &ExecResult) -> Vec<String> {
    match result {
        ExecResult::Select { columns, .. } => columns.iter().map(|(n, _)| n.clone()).collect(),
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn pre_parsed_execution_ignores_a_stale_plan_cache_key() {
    let ex = mem_executor();
    for t in 0..2 {
        ex.execute(&format!("CREATE TABLE p{t} (id INT, name_{t} TEXT)"))
            .await
            .unwrap();
        ex.execute(&format!("INSERT INTO p{t} VALUES (1, 'v{t}')"))
            .await
            .unwrap();
    }
    let sess = ex.create_session();

    // Run statement A so its plan is in the cache under A's key, and capture
    // that key the way the wire's Parse phase does.
    let sql_a = "SELECT id, name_0 FROM p0 WHERE id = 1";
    let (_ast_a, key_a) = ex.parse_with_ast_cache_keyed(sql_a).expect("parse a");
    let key_a = key_a.expect("single statement has a key");
    let res_a = ex
        .execute_statements_with_session(
            sess,
            crate::sql::parse(sql_a).unwrap(),
            Some(key_a.clone()),
        )
        .await
        .expect("a");
    assert_eq!(columns_of(&res_a[0]), ["id", "name_0"]);

    // Now leave A's key behind in the session slot — exactly what any route
    // that sets the hint and then does not consume it leaves for the next
    // statement — and run a DIFFERENT statement with no key of its own.
    ex.set_plan_cache_key_hint_for(sess, key_a);
    let sql_b = "SELECT id, name_1 FROM p1 WHERE id = 1";
    let res_b = ex
        .execute_statements_with_session(sess, crate::sql::parse(sql_b).unwrap(), None)
        .await
        .expect("b");

    assert_eq!(
        columns_of(&res_b[0]),
        ["id", "name_1"],
        "a pre-parsed statement executed under the PREVIOUS statement's \
         plan-cache key and returned that statement's columns"
    );
    match &res_b[0] {
        ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][1], Value::Text("v1".into()), "wrong table's row");
        }
        other => panic!("expected Select, got {other:?}"),
    }
}
