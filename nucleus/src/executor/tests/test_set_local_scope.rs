//! N1: `SET LOCAL` (and `SET ROLE` / `SET` inside a transaction) are scoped to
//! the transaction, as in PostgreSQL. Before the fix a role or setting
//! assumed inside a transaction outlived COMMIT and ROLLBACK, so a pooled
//! connection handed the next borrower the previous one's role.

use super::*;

async fn sql(ex: &Executor, sid: u64, q: &str) -> Vec<ExecResult> {
    ex.execute_with_session(sid, q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

async fn user(ex: &Executor, sid: u64) -> String {
    text_of(sql(ex, sid, "SELECT current_user").await.remove(0))
}

async fn show(ex: &Executor, sid: u64, name: &str) -> String {
    text_of(sql(ex, sid, &format!("SHOW {name}")).await.remove(0))
}

async fn fixture() -> (Executor, u64) {
    let ex = test_executor();
    exec(&ex, "CREATE ROLE n1_app").await;
    let sid = ex.create_session();
    (ex, sid)
}

#[tokio::test]
async fn set_local_role_reverts_at_commit_and_rollback() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    assert_eq!(user(&ex, sid).await, "n1_app");
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(user(&ex, sid).await, login, "role survived COMMIT");

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    sql(&ex, sid, "ROLLBACK").await;
    assert_eq!(user(&ex, sid).await, login, "role survived ROLLBACK");
}

#[tokio::test]
async fn set_local_setting_reverts_at_commit_and_rollback() {
    let (ex, sid) = fixture().await;
    sql(&ex, sid, "SET app.tenant = 'base'").await;

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'a'").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'b'").await;
    sql(&ex, sid, "SET LOCAL search_path = elsewhere").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'b'");
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'base'");
    assert_eq!(show(&ex, sid, "search_path").await, "public");

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'a'").await;
    sql(&ex, sid, "ROLLBACK").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'base'");

    // A setting that did not exist before goes away again.
    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL app.fresh = 'x'").await;
    sql(&ex, sid, "COMMIT").await;
    assert_ne!(show_or_empty(&ex, sid, "app.fresh").await, "'x'");
}

#[tokio::test]
async fn session_set_persists_at_commit_but_not_rollback() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET app.tenant = 'kept'").await;
    sql(&ex, sid, "SET ROLE n1_app").await;
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'kept'");
    assert_eq!(user(&ex, sid).await, "n1_app");

    sql(&ex, sid, "SET ROLE NONE").await;
    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET app.tenant = 'gone'").await;
    sql(&ex, sid, "SET ROLE n1_app").await;
    sql(&ex, sid, "ROLLBACK").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'kept'");
    assert_eq!(user(&ex, sid).await, login);

    // SET outside a transaction still persists.
    sql(&ex, sid, "SET app.tenant = 'auto'").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'auto'");
}

#[tokio::test]
async fn session_set_after_set_local_wins_at_commit() {
    let (ex, sid) = fixture().await;
    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'local'").await;
    sql(&ex, sid, "SET app.tenant = 'session'").await;
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'session'");

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET app.tenant = 'session2'").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'local2'").await;
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'session2'");
}

#[tokio::test]
async fn aborted_transaction_and_commit_of_aborted_revert() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    assert!(ex.execute_with_session(sid, "SELECT * FROM no_such_table").await.is_err());
    sql(&ex, sid, "ROLLBACK").await;
    assert_eq!(user(&ex, sid).await, login);

    // COMMIT of an aborted transaction is a ROLLBACK: session SET reverts too.
    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET ROLE n1_app").await;
    assert!(ex.execute_with_session(sid, "SELECT * FROM no_such_table").await.is_err());
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(user(&ex, sid).await, login);
}

#[tokio::test]
async fn rollback_to_savepoint_reverts_set_and_set_local() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;
    sql(&ex, sid, "SET app.tenant = 'base'").await;

    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET app.tenant = 'before'").await;
    sql(&ex, sid, "SAVEPOINT sp").await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'after'").await;
    sql(&ex, sid, "SET app.other = 'after'").await;
    sql(&ex, sid, "ROLLBACK TO SAVEPOINT sp").await;
    assert_eq!(user(&ex, sid).await, login);
    assert_eq!(show(&ex, sid, "app.tenant").await, "'before'");
    // The savepoint survives, and later SET LOCAL still ends with the txn.
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(user(&ex, sid).await, login);
    assert_eq!(show(&ex, sid, "app.tenant").await, "'before'");
}

#[tokio::test]
async fn release_savepoint_keeps_local_until_transaction_end() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;
    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SAVEPOINT sp").await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    sql(&ex, sid, "RELEASE SAVEPOINT sp").await;
    assert_eq!(user(&ex, sid).await, "n1_app");
    sql(&ex, sid, "COMMIT").await;
    assert_eq!(user(&ex, sid).await, login);
}

#[tokio::test]
async fn set_local_outside_transaction_has_no_effect() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    assert_eq!(user(&ex, sid).await, login);
    sql(&ex, sid, "SET app.tenant = 'base'").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'x'").await;
    assert_eq!(show(&ex, sid, "app.tenant").await, "'base'");
}

#[tokio::test]
async fn reset_role_restores_login_role() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;
    sql(&ex, sid, "SET ROLE n1_app").await;
    sql(&ex, sid, "RESET ROLE").await;
    assert_eq!(user(&ex, sid).await, login);

    sql(&ex, sid, "SET ROLE n1_app").await;
    sql(&ex, sid, "RESET ALL").await;
    assert_eq!(user(&ex, sid).await, login);

    sql(&ex, sid, "SET ROLE n1_app").await;
    sql(&ex, sid, "DISCARD ALL").await;
    assert_eq!(user(&ex, sid).await, login);
}

#[tokio::test]
async fn pool_return_drops_role_and_settings_of_open_transaction() {
    let (ex, sid) = fixture().await;
    let login = user(&ex, sid).await;
    sql(&ex, sid, "BEGIN").await;
    sql(&ex, sid, "SET LOCAL ROLE n1_app").await;
    sql(&ex, sid, "SET LOCAL app.tenant = 'a'").await;
    ex.reset_session(sid).await;
    assert_eq!(user(&ex, sid).await, login);
    assert!(!ex.session_in_transaction(sid));
    assert_ne!(show_or_empty(&ex, sid, "app.tenant").await, "'a'");
}

async fn show_or_empty(ex: &Executor, sid: u64, name: &str) -> String {
    match ex.execute_with_session(sid, &format!("SHOW {name}")).await {
        Ok(mut r) => match r.remove(0) {
            ExecResult::Select { rows, .. } => rows
                .first()
                .map(|r| format!("{}", r[0]))
                .unwrap_or_default(),
            _ => String::new(),
        },
        Err(_) => String::new(),
    }
}
