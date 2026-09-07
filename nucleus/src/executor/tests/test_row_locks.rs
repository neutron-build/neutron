//! Row-level locking: `FOR UPDATE`, `SKIP LOCKED`, `NOWAIT`.
//!
//! The defect this suite was written around is the reason it is adversarial
//! rather than happy-path. `FOR UPDATE SKIP LOCKED` — the clause every SQL
//! job queue claims work with — was first parsed and silently dropped (the
//! guarantee-removal class), then converted to a loud refusal while a comment
//! claimed plain `FOR UPDATE` was safe to ignore because "the isolation the
//! engine already provides is a stronger guarantee". A behavioral examination
//! (2026-09-07, PostgresQueueDriver substrate) disproved that with the queue
//! driver itself: two workers, 50 jobs, clause stripped, literal LIMIT —
//! 51 deliveries for 50 jobs, one row `attempts=2`. Table-granularity 2PL
//! does not make an ignored plain FOR UPDATE safe, and snapshot reads do not
//! substitute for row locks. The clauses are now honoured: rows are locked at
//! emission, keyed by primary key, held to COMMIT/ROLLBACK.
//!
//! Every test asks what a concurrent claim GETS, not what a user sees, and
//! each carries its control — the same shape without the clause, a free row,
//! or a released lock — so a check that denied or granted everything would
//! fail rather than pass vacuously.

use super::*;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::disk_engine::DiskEngine;
use crate::wire::error_codec::{ErrorCodec, PgWireErrorCodec};

/// A disk-backed executor, matching what `main.rs` constructs — the engine
/// the wire server actually runs, and the engine the double-delivery was
/// measured on.
fn disk_executor(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.join("t.db"), catalog.clone()).unwrap());
    let engine: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    Arc::new(Executor::new(catalog, engine))
}

async fn seed_jobs(ex: &Executor, n: i64) {
    ex.execute("CREATE TABLE jobs (id INT PRIMARY KEY, status TEXT, attempts INT)")
        .await
        .unwrap();
    for i in 1..=n {
        ex.execute(&format!(
            "INSERT INTO jobs VALUES ({i}, 'pending', 0)"
        ))
        .await
        .unwrap();
    }
}

/// The ids a claim took, from RETURNING. A claim that matched nothing returns
/// `Command { rows_affected: 0 }` (the executor's empty-RETURNING shape), not
/// an empty Select — that is the drain signal the workers break on.
fn claimed_ids(res: &[ExecResult]) -> Vec<i64> {
    match &res[0] {
        ExecResult::Select { rows, .. } => rows
            .iter()
            .map(|r| match r[0] {
                Value::Int32(v) => v as i64,
                Value::Int64(v) => v,
                ref other => panic!("non-integer id: {other:?}"),
            })
            .collect(),
        ExecResult::Command { rows_affected: 0, .. } => Vec::new(),
        other => panic!("claim must return rows, got {other:?}"),
    }
}

/// A claim in the queue driver's exact shape. The locking clause sits inside
/// the `IN (subquery)` — the position the examination measured double
/// deliveries from, because the subquery's snapshot read is what both workers
/// overlap on.
const CLAIM: &str = "UPDATE jobs SET status = 'active', attempts = attempts + 1 \
     WHERE id IN (\
       SELECT id FROM jobs WHERE status = 'pending' ORDER BY id \
       LIMIT 5 FOR UPDATE SKIP LOCKED\
     ) \
     RETURNING id";

/// SKIP LOCKED excludes another transaction's held rows — deterministically,
/// no race required to observe.
#[tokio::test]
async fn skip_locked_excludes_held_rows_and_keeps_the_rest() {
    let ex = test_executor();
    seed_jobs(&ex, 6).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    let got_a = claimed_ids(
        &ex.execute_with_session(
            a,
            "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED",
        )
        .await
        .unwrap(),
    );
    assert_eq!(got_a, vec![1, 2, 3], "worker A takes the first three");

    // Control, in the same test: WITHOUT the clause B would re-read the same
    // three rows A holds — the overlap the clause exists to prevent.
    let control = claimed_ids(
        &ex.execute_with_session(
            a,
            "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 3",
        )
        .await
        .unwrap(),
    );
    assert_eq!(control, vec![1, 2, 3], "control: no clause, same rows");

    let b = ex.create_session();
    ex.execute_with_session(b, "BEGIN").await.unwrap();
    let got_b = claimed_ids(
        &ex.execute_with_session(
            b,
            "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED",
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        got_b,
        vec![4, 5, 6],
        "worker B must skip A's held rows and still fill its LIMIT, not \
         return short and not overlap"
    );

    // A's locks only last as long as A's transaction: after COMMIT the rows
    // are claimable again. B's own earlier locks are re-entrant, not skipped.
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    let got_b2 = claimed_ids(
        &ex.execute_with_session(
            b,
            "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 6 FOR UPDATE SKIP LOCKED",
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        got_b2,
        vec![1, 2, 3, 4, 5, 6],
        "after A commits, B sees all rows; its own held locks stay re-entrant"
    );
    ex.execute_with_session(b, "COMMIT").await.unwrap();
}

/// The acceptance scenario from the examination, in-repo: two workers, one
/// queue, every job delivered exactly once. This is the shape that measured
/// 51/50 with the clause stripped; with it honoured, the outcome is
/// timing-independent: the union of claims is exactly the 50 jobs, no id
/// appears twice, and the attempts column sums to 50.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_drain_fifty_jobs_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 50).await;

    let mut tasks = Vec::new();
    for _w in 0..2 {
        let ex = ex.clone();
        tasks.push(tokio::spawn(async move {
            let sid = ex.create_session();
            let mut deliveries: Vec<i64> = Vec::new();
            loop {
                ex.execute_with_session(sid, "BEGIN").await.unwrap();
                let res = ex.execute_with_session(sid, CLAIM).await.unwrap();
                let ids = claimed_ids(&res);
                if ids.is_empty() {
                    ex.execute_with_session(sid, "ROLLBACK").await.unwrap();
                    break;
                }
                deliveries.extend(ids);
                ex.execute_with_session(sid, "COMMIT").await.unwrap();
            }
            deliveries
        }));
    }

    let mut all: Vec<i64> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }

    assert_eq!(
        all.len(),
        50,
        "exactly 50 deliveries for 50 jobs (got {})",
        all.len()
    );
    let mut sorted = all.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        50,
        "zero overlap: some rows were claimed by both workers"
    );

    // And the database agrees with the deliveries: no attempts=2 row, the
    // exact double-delivery signature from the examination.
    let res = ex
        .execute("SELECT COUNT(*), SUM(attempts) FROM jobs WHERE attempts > 1")
        .await
        .unwrap();
    let ExecResult::Select { rows, .. } = &res[0] else {
        unreachable!()
    };
    assert_eq!(
        rows[0][0], Value::Int32(0),
        "no row may be attempted twice; SUM={:?}",
        rows[0][1]
    );
    let res = ex.execute("SELECT SUM(attempts) FROM jobs").await.unwrap();
    let ExecResult::Select { rows, .. } = &res[0] else {
        unreachable!()
    };
    match &rows[0][0] {
        Value::Numeric(s) => assert_eq!(s, "50"),
        Value::Int64(n) => assert_eq!(*n, 50),
        other => panic!("expected SUM(attempts) = 50, got {other:?}"),
    }
}

/// NOWAIT refuses with the SQLSTATE the clause is defined by: 55P03
/// lock_not_available. Silently blocking instead inverts the caller's intent.
#[tokio::test]
async fn nowait_reports_lock_not_available_not_a_block() {
    let ex = test_executor();
    seed_jobs(&ex, 2).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();

    let b = ex.create_session();
    let err = ex
        .execute_with_session(b, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT")
        .await
        .expect_err("NOWAIT on a held row must fail, not block");
    let msg = err.to_string();
    assert!(
        msg.contains("lock_not_available"),
        "the error must use the wording the wire codec keys 55P03 on; got: {msg}"
    );
    // And the codec actually delivers 55P03 — the code, not the message, is
    // the contract a driver acts on.
    let details = PgWireErrorCodec.encode(&err);
    assert_eq!(PgWireErrorCodec.code_to_string(details.code), "55P03");

    // Control: NOWAIT on a row nobody holds is not an error.
    ex.execute_with_session(b, "SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT")
        .await
        .expect("control: a free row must not refuse");
    ex.execute_with_session(a, "COMMIT").await.unwrap();
}

/// Plain FOR UPDATE blocks until the holder's transaction ends, then proceeds
/// — the semantics a claim that is willing to wait depends on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_for_update_blocks_then_proceeds_after_commit() {
    let ex = Arc::new(test_executor());
    seed_jobs(&ex, 1).await;

    let a = ex.create_session();
    let b = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();

    let ex2 = ex.clone();
    let waiter = tokio::spawn(async move {
        ex2.execute_with_session(b, "BEGIN").await.unwrap();
        let r = ex2
            .execute_with_session(b, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
            .await;
        let _ = ex2.execute_with_session(b, "COMMIT").await;
        r
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert!(
        !waiter.is_finished(),
        "the second FOR UPDATE must be waiting on the held row"
    );

    ex.execute_with_session(a, "COMMIT").await.unwrap();
    let out = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
        .await
        .expect("must be granted soon after the holder commits")
        .unwrap()
        .expect("the post-commit read must succeed");
    assert_eq!(claimed_ids(&out), vec![1]);
}

/// ROLLBACK releases the rows the transaction locked: nothing it read
/// changed, so nothing stays claimable-blocked.
#[tokio::test]
async fn rollback_releases_row_locks() {
    let ex = test_executor();
    seed_jobs(&ex, 1).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE SKIP LOCKED")
        .await
        .unwrap();
    ex.execute_with_session(a, "ROLLBACK").await.unwrap();

    let b = ex.create_session();
    ex.execute_with_session(b, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT")
        .await
        .expect("a rolled-back transaction must not keep rows locked");
}

/// An autocommit FOR UPDATE releases at statement end — the statement IS the
/// transaction. A lock that outlived it would park the row behind a session
/// that no longer thinks it holds anything.
#[tokio::test]
async fn autocommit_releases_row_locks_at_statement_end() {
    let ex = test_executor();
    seed_jobs(&ex, 1).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();

    let b = ex.create_session();
    ex.execute_with_session(b, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT")
        .await
        .expect("an autocommit statement must not leave its locks behind");
}

/// FOR SHARE is honoured with the one lock strength the engine has. The
/// coarser lock over-delivers the clause's guarantee (excludes strictly
/// more), never under it.
#[tokio::test]
async fn for_share_locks_the_rows_it_returns() {
    let ex = test_executor();
    seed_jobs(&ex, 2).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    let got = claimed_ids(
        &ex.execute_with_session(
            a,
            "SELECT id FROM jobs WHERE id = 1 FOR SHARE",
        )
        .await
        .unwrap(),
    );
    assert_eq!(got, vec![1]);

    let b = ex.create_session();
    let err = ex
        .execute_with_session(b, "SELECT id FROM jobs WHERE id = 1 FOR SHARE NOWAIT")
        .await
        .expect_err("a FOR SHARE row is held; NOWAIT must refuse");
    assert!(
        err.to_string().contains("lock_not_available"),
        "got: {err}"
    );
    ex.execute_with_session(a, "COMMIT").await.unwrap();
}

/// Shapes the lock implementation does not cover are refused, not
/// approximately locked. Each error names what it refused.
#[tokio::test]
async fn unsupported_lock_shapes_are_refused_by_name() {
    let ex = test_executor();
    ex.execute("CREATE TABLE lk (id INT PRIMARY KEY, v INT)").await.unwrap();
    ex.execute("CREATE TABLE other_lk (id INT PRIMARY KEY, lk_id INT)").await.unwrap();
    ex.execute("CREATE TABLE keyless (id INT, v INT)").await.unwrap();

    let join = ex
        .execute("SELECT lk.id FROM lk JOIN other_lk ON other_lk.lk_id = lk.id FOR UPDATE")
        .await
        .expect_err("FOR UPDATE over a join must be refused");
    assert!(
        join.to_string().contains("join"),
        "the error must name the join; got: {join}"
    );

    let agg = ex
        .execute("SELECT COUNT(*) FROM lk FOR UPDATE")
        .await
        .expect_err("FOR UPDATE with an aggregate must be refused");
    assert!(
        agg.to_string().to_lowercase().contains("aggregate"),
        "got: {agg}"
    );

    let grp = ex
        .execute("SELECT v FROM lk GROUP BY v FOR UPDATE")
        .await
        .expect_err("FOR UPDATE with GROUP BY must be refused");
    assert!(
        grp.to_string().contains("GROUP BY"),
        "got: {grp}"
    );

    let keyless = ex
        .execute("SELECT * FROM keyless FOR UPDATE")
        .await
        .expect_err("FOR UPDATE on a table with no primary key must be refused");
    assert!(
        keyless.to_string().contains("primary key"),
        "the error must say why: no identity to lock with; got: {keyless}"
    );
}

/// A table without a primary key is refused even with SKIP LOCKED: the
/// tempting shortcut — return the rows unlocked and skip nothing — is the
/// original silent-guarantee-drop wearing a new comment.
#[tokio::test]
async fn skip_locked_on_a_keyless_table_is_refused_not_ignored() {
    let ex = test_executor();
    ex.execute("CREATE TABLE nok (id INT)").await.unwrap();
    ex.execute("INSERT INTO nok VALUES (1)").await.unwrap();

    let err = ex
        .execute("SELECT id FROM nok FOR UPDATE SKIP LOCKED")
        .await
        .expect_err("a keyless table cannot honour a row lock");
    assert!(
        err.to_string().contains("primary key"),
        "got: {err}"
    );
}

/// Plain FOR UPDATE on an ordinary keyed table is accepted and locks — kept
/// from the pre-implementation suite so the acceptance cannot regress to
/// mere parsing.
#[tokio::test]
async fn plain_for_update_is_still_accepted() {
    let ex = test_executor();
    seed_jobs(&ex, 1).await;
    ex.execute("SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();
}

// A partial index is not a hint: WHERE is what makes it partial. Out of scope
// for the row-lock work and still refused, not silently widened.
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
        "the error must name what it refused; got: {err}"
    );
}

#[tokio::test]
async fn plain_index_still_builds() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE pidx_ok (id INT)").await;
    exec(&ex, "CREATE INDEX pidx_ok_id ON pidx_ok (id)").await;
}

// ---------------------------------------------------------------------------
// Parameterized LIMIT/OFFSET
// ---------------------------------------------------------------------------

/// `LIMIT $1` must resolve at execute time. The examination found it failing
/// as `0A000 LIMIT/OFFSET must be non-negative integer`: a LIMIT-position
/// parameter inside a scalar subquery had no inferred type, decoded as text,
/// and the const evaluator refused the string.
#[tokio::test]
async fn a_bound_limit_parameter_limits() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lim (id INT PRIMARY KEY)").await;
    for i in 1..=10 {
        exec(&ex, &format!("INSERT INTO lim VALUES ({i})")).await;
    }

    exec(&ex, "PREPARE take(INT) AS SELECT id FROM lim ORDER BY id LIMIT $1").await;
    let res = exec(&ex, "EXECUTE take(3)").await;
    assert_eq!(
        rows(&res[0]).len(),
        3,
        "LIMIT $1 must resolve to the bound value"
    );
    let res = exec(&ex, "EXECUTE take(0)").await;
    assert_eq!(rows(&res[0]).len(), 0, "LIMIT 0 returns no rows");
}

/// The same claim shape the queue driver sends: the parameterized LIMIT sits
/// inside the `IN (subquery)`.
#[tokio::test]
async fn a_bound_limit_parameter_inside_the_claim_subquery_limits() {
    let ex = test_executor();
    seed_jobs(&ex, 10).await;

    exec(
        &ex,
        "PREPARE claim(INT) AS UPDATE jobs SET status = 'active' \
         WHERE id IN (SELECT id FROM jobs WHERE status = 'pending' ORDER BY id \
         LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING id",
    )
    .await;
    let res = exec(&ex, "EXECUTE claim(4)").await;
    assert_eq!(claimed_ids(&res), vec![1, 2, 3, 4]);
    let res = exec(&ex, "EXECUTE claim(2)").await;
    assert_eq!(claimed_ids(&res), vec![5, 6]);
}

/// A LIMIT parameter that is not a non-negative integer errors properly —
/// naming the clause — instead of being ignored or coerced.
#[tokio::test]
async fn a_non_integer_limit_parameter_errors() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lim2 (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO lim2 VALUES (1), (2), (3)").await;

    exec(&ex, "PREPARE bad(TEXT) AS SELECT id FROM lim2 LIMIT $1").await;
    let err = ex
        .execute("EXECUTE bad('not-a-number')")
        .await
        .expect_err("a non-integer LIMIT must fail the statement");
    assert!(
        err.to_string().contains("LIMIT"),
        "the error must name LIMIT/OFFSET; got: {err}"
    );

    let err = ex
        .execute("SELECT id FROM lim2 LIMIT 'nope'")
        .await
        .expect_err("a non-integer literal LIMIT must fail too");
    assert!(
        err.to_string().contains("LIMIT"),
        "got: {err}"
    );
}

/// PostgreSQL accepts `LIMIT '5'` — an unknown-typed literal coerces to the
/// integer the position requires — and so does the executor, which is what
/// makes a text-decoded wire parameter work even when inference misses.
#[tokio::test]
async fn a_quoted_integer_literal_limits() {
    let ex = test_executor();
    exec(&ex, "CREATE TABLE lim3 (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO lim3 VALUES (1), (2), (3), (4)").await;

    let res = exec(&ex, "SELECT id FROM lim3 ORDER BY id LIMIT '2'").await;
    assert_eq!(rows(&res[0]).len(), 2);
}
