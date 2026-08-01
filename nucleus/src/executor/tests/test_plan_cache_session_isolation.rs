//! The plan-cache key hint must never cross session boundaries.
//!
//! `parse_with_ast_cache()` computes a normalized SQL key and stashes it so
//! `execute_query_planned()` can look the plan up without re-serializing the
//! AST. That slot used to live on the `Executor`, shared by every connection.
//! Two sessions parsing concurrently would overwrite each other's key, and
//! whichever consumed it first executed the plan filed under the OTHER
//! statement's key — with its own literals re-bound. The observable result was
//! a `SELECT` against `acct2` returning a row from `acct1`: correct row id,
//! wrong table, no error.
//!
//! It surfaced as a serializability violation (the SIREAD was recorded against
//! the wrong table, so the rw-antidependency edge was never created) and hid
//! there for a long time only because every table in that probe held identical
//! values, which makes a cross-table read indistinguishable from a correct one.
//! Hence: distinct values per table here, and an assertion on the DATA rather
//! than on any isolation property.

use super::*;

/// Tables seeded far enough apart that a row from the wrong one is unmistakable.
const TABLES: usize = 6;
const ROWS: i64 = 4;
fn base_of(t: usize) -> i64 {
    100 + (t as i64) * 1000
}

fn mvcc_executor() -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    Arc::new(Executor::new(catalog, storage))
}

/// Many sessions, each repeatedly reading only from its OWN table, all through
/// one executor. Every statement is the same shape (`SELECT balance FROM
/// acctN WHERE id = M`), which is exactly what makes them collide in the
/// normalized-key cache — the shape is identical and only the table differs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_sessions_never_read_another_sessions_table() {
    let ex = mvcc_executor();
    for t in 0..TABLES {
        ex.execute(&format!("CREATE TABLE acct{t} (id INT, balance INT)"))
            .await
            .unwrap();
        for i in 1..=ROWS {
            ex.execute(&format!(
                "INSERT INTO acct{t} VALUES ({i}, {})",
                base_of(t)
            ))
            .await
            .unwrap();
        }
    }

    let mut handles = Vec::new();
    for t in 0..TABLES {
        let ex = ex.clone();
        handles.push(tokio::spawn(async move {
            let sess = ex.create_session();
            let mut wrong = Vec::new();
            for round in 0..200 {
                let id = (round % ROWS as usize) + 1;
                let res = ex
                    .execute_with_session(
                        sess,
                        &format!("SELECT balance FROM acct{t} WHERE id = {id}"),
                    )
                    .await
                    .expect("select");
                let got = match &res[0] {
                    ExecResult::Select { rows, .. } => match rows[0][0] {
                        Value::Int64(v) => v,
                        Value::Int32(v) => v as i64,
                        ref other => panic!("unexpected balance {other:?}"),
                    },
                    other => panic!("expected Select, got {other:?}"),
                };
                if got != base_of(t) {
                    wrong.push((round, id, got));
                }
            }
            (t, wrong)
        }));
    }

    let mut failures = Vec::new();
    for h in handles {
        let (t, wrong) = h.await.unwrap();
        for (round, id, got) in wrong {
            // Name the table the value actually came from — that is the whole
            // signature of the bug.
            let from = (got - 100) / 1000;
            failures.push(format!(
                "acct{t} id={id} (round {round}) returned {got}, which is acct{from}'s value"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "concurrent sessions read across table boundaries ({} bad reads):\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// The same collision through the wire protocol's two-phase shape: Parse
/// computes the key, Execute seeds it for a NAMED session and runs there.
///
/// Parse deliberately returns the key as a value rather than stashing it,
/// because the pgwire Parse handler runs outside any session scope — a stashed
/// key would land in the shared default session. This drives the same
/// sequence: derive the key, seed it for this session only, execute.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wire_shaped_parse_then_execute_keeps_sessions_apart() {
    let ex = mvcc_executor();
    for t in 0..TABLES {
        ex.execute(&format!("CREATE TABLE k{t} (id INT, v INT)"))
            .await
            .unwrap();
        ex.execute(&format!("INSERT INTO k{t} VALUES (1, {})", base_of(t)))
            .await
            .unwrap();
    }

    let mut handles = Vec::new();
    for t in 0..TABLES {
        let ex = ex.clone();
        handles.push(tokio::spawn(async move {
            let sess = ex.create_session();
            let mut bad = 0usize;
            let sql = format!("SELECT v FROM k{t} WHERE id = 1");
            for _ in 0..200 {
                // Parse phase: key returned by value, never stashed globally.
                let (_ast, key) = ex.parse_with_ast_cache_keyed(&sql).expect("parse");
                // Execute phase: seed it for THIS session only.
                if let Some(key) = key {
                    ex.set_plan_cache_key_hint_for(sess, key);
                }
                let res = ex.execute_with_session(sess, &sql).await.expect("select");
                let got = match &res[0] {
                    ExecResult::Select { rows, .. } => match rows[0][0] {
                        Value::Int64(v) => v,
                        Value::Int32(v) => v as i64,
                        ref other => panic!("unexpected v {other:?}"),
                    },
                    other => panic!("expected Select, got {other:?}"),
                };
                if got != base_of(t) {
                    bad += 1;
                }
            }
            (t, bad)
        }));
    }

    for h in handles {
        let (t, bad) = h.await.unwrap();
        assert_eq!(bad, 0, "session for table {t} saw {bad} cross-session leaks");
    }
}
