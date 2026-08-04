use super::*;

// SKIP LOCKED was parsed into `Query::locks` and never read, so the clause that
// stops two workers claiming the same job was silently discarded. The query
// returned the row either way, so nothing anywhere reported that the guarantee
// was missing — a job queue built on it would hand the same row to every worker
// that happened to poll at the same moment. Refusing it is the honest behaviour
// until row-level lock skipping exists.
#[tokio::test]
async fn skip_locked_is_refused_not_silently_ignored() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE joblocks (id INT, status TEXT)").await;
    exec(&ex, "INSERT INTO joblocks VALUES (1, 'pending')").await;

    let err = ex
        .execute("SELECT id FROM joblocks WHERE status = 'pending' FOR UPDATE SKIP LOCKED")
        .await
        .expect_err("SKIP LOCKED must not be silently ignored");
    assert!(
        err.to_string().contains("SKIP LOCKED"),
        "the error must name the clause it refused, got: {err}"
    );
}

#[tokio::test]
async fn nowait_is_refused_not_silently_ignored() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE joblocks_nw (id INT)").await;

    let err = ex
        .execute("SELECT id FROM joblocks_nw FOR UPDATE NOWAIT")
        .await
        .expect_err("NOWAIT must not be silently ignored");
    assert!(err.to_string().contains("NOWAIT"), "got: {err}");
}

// Plain FOR UPDATE stays allowed: it is an advisory pessimistic hint, and the
// isolation the engine already provides is a stronger guarantee than dropping it
// would imply. Refusing it would break ordinary Postgres-compatible SQL for no
// safety gain.
#[tokio::test]
async fn plain_for_update_is_still_accepted() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE joblocks_plain (id INT)").await;
    exec(&ex, "INSERT INTO joblocks_plain VALUES (1)").await;
    exec(&ex, "SELECT id FROM joblocks_plain FOR UPDATE").await;
}

// A partial index is not a hint: WHERE is what makes it partial. The predicate
// was parsed and discarded, so the statement succeeded and built a FULL index
// under the requested name — larger than asked for, and usable for queries the
// partial index was never meant to serve, with nothing reporting the
// substitution.
#[tokio::test]
async fn partial_index_is_refused_not_silently_widened() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pidx (id INT, status TEXT)").await;

    let err = ex
        .execute("CREATE INDEX pidx_pending ON pidx (id) WHERE status = 'pending'")
        .await
        .expect_err("a partial index must not silently become a full index");
    assert!(
        err.to_string().contains("partial index"),
        "the error must name what it refused, got: {err}"
    );
}

#[tokio::test]
async fn plain_index_still_builds() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pidx_ok (id INT)").await;
    exec(&ex, "CREATE INDEX pidx_ok_id ON pidx_ok (id)").await;
}
