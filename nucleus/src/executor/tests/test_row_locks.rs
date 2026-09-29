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
        ex.execute(&format!("INSERT INTO jobs VALUES ({i}, 'pending', 0)"))
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
        ExecResult::Command {
            rows_affected: 0, ..
        } => Vec::new(),
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
        rows[0][0],
        Value::Int32(0),
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

/// The interleaving behind the `two_workers_drain_fifty_jobs_exactly_once`
/// flake, forced. B's scan reads job 1 as pending and then blocks on A's lock;
/// A claims the row and commits; B's lock is granted against a row that is no
/// longer pending. The stale image must not come back: B re-checks the row
/// after locking (PostgreSQL's EvalPlanQual) and drops it. Without the recheck
/// B returns job 1, and a claim built on it fails at commit with WriteConflict
/// or applies twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_changed_while_waiting_for_its_lock_is_not_returned_stale() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 2).await;

    let a = ex.create_session();
    let b = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    let got_a = claimed_ids(
        &ex.execute_with_session(
            a,
            "UPDATE jobs SET status = 'active', attempts = attempts + 1 \
             WHERE id IN (SELECT id FROM jobs WHERE status = 'pending' \
             ORDER BY id LIMIT 1 FOR UPDATE) RETURNING id",
        )
        .await
        .unwrap(),
    );
    assert_eq!(got_a, vec![1]);

    let ex2 = ex.clone();
    let waiter = tokio::spawn(async move {
        ex2.execute_with_session(b, "BEGIN").await.unwrap();
        let r = ex2
            .execute_with_session(
                b,
                "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE",
            )
            .await;
        let _ = ex2.execute_with_session(b, "COMMIT").await;
        r
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(
        !waiter.is_finished(),
        "control: B must be waiting on A's lock over job 1"
    );

    ex.execute_with_session(a, "COMMIT").await.unwrap();
    let out = tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
        .await
        .expect("must be granted soon after the holder commits")
        .unwrap()
        .expect("the post-commit read must succeed");
    assert!(
        !claimed_ids(&out).contains(&1),
        "job 1 is active now; B must not be handed its stale pending image"
    );
}

/// Spawn B: BEGIN, run `sql`, COMMIT-less; returns the result and leaves B's
/// transaction open so its locks can be probed. The caller ends it.
async fn blocked_reader(
    ex: &Arc<Executor>,
    b: u64,
    sql: &'static str,
) -> tokio::task::JoinHandle<Result<Vec<ExecResult>, ExecError>> {
    let ex2 = ex.clone();
    let h = tokio::spawn(async move {
        ex2.execute_with_session(b, "BEGIN").await.unwrap();
        ex2.execute_with_session(b, sql).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!h.is_finished(), "control: B must be waiting on A's lock");
    h
}

async fn wait_for(
    h: tokio::task::JoinHandle<Result<Vec<ExecResult>, ExecError>>,
) -> Vec<ExecResult> {
    tokio::time::timeout(std::time::Duration::from_secs(5), h)
        .await
        .expect("must be granted soon after the holder ends")
        .unwrap()
        .expect("the post-lock read must succeed")
}

async fn balances(ex: &Executor, sid: u64, sql: &str) -> Vec<i64> {
    claimed_ids(&ex.execute_with_session(sid, sql).await.unwrap())
}

/// Read-modify-write: A adds 50 to a locked row and commits. B, waiting on
/// the same row, must get it in its NEW state (bal=150) — PostgreSQL
/// re-evaluates WHERE on the newest version — not zero rows and not the old
/// image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_updated_while_waiting_is_returned_in_its_new_state() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    ex.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
        .await
        .unwrap();
    ex.execute("INSERT INTO acct VALUES (1, 100)")
        .await
        .unwrap();

    let (a, b) = (ex.create_session(), ex.create_session());
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT bal FROM acct WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();
    let h = blocked_reader(&ex, b, "SELECT bal FROM acct WHERE id = 1 FOR UPDATE").await;
    ex.execute_with_session(a, "UPDATE acct SET bal = bal + 50 WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    assert_eq!(claimed_ids(&wait_for(h).await), vec![150]);
    ex.execute_with_session(b, "COMMIT").await.unwrap();
}

/// A blocking claim with LIMIT 1: the top row is claimed and committed by A
/// while B waits. B's LIMIT must be refilled from the next candidate WITH its
/// lock taken — a third session's NOWAIT on that row must be refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refilled_limit_slot_is_locked_not_borrowed() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 3).await;

    let (a, b, c) = (
        ex.create_session(),
        ex.create_session(),
        ex.create_session(),
    );
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(
        a,
        "UPDATE jobs SET status = 'active' WHERE id IN \
         (SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE)",
    )
    .await
    .unwrap();
    let h = blocked_reader(
        &ex,
        b,
        "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE",
    )
    .await;
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    assert_eq!(claimed_ids(&wait_for(h).await), vec![2]);

    let probe = ex
        .execute_with_session(c, "SELECT id FROM jobs WHERE id = 2 FOR UPDATE NOWAIT")
        .await;
    assert!(
        probe.is_err(),
        "job 2 was returned to B, so B must hold its lock; NOWAIT from C got {probe:?}"
    );
    ex.execute_with_session(b, "COMMIT").await.unwrap();
}

/// A locked row that is dropped after the recheck must not stay held: B is
/// not returned job 1 (now active), so a third session can lock it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_row_releases_its_lock() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 1).await;

    let (a, b, c) = (
        ex.create_session(),
        ex.create_session(),
        ex.create_session(),
    );
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(
        a,
        "UPDATE jobs SET status = 'active' WHERE id IN \
         (SELECT id FROM jobs WHERE id = 1 FOR UPDATE)",
    )
    .await
    .unwrap();
    let h = blocked_reader(
        &ex,
        b,
        "SELECT id FROM jobs WHERE status = 'pending' FOR UPDATE",
    )
    .await;
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    assert!(claimed_ids(&wait_for(h).await).is_empty());

    let probe = ex
        .execute_with_session(c, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE NOWAIT")
        .await;
    assert!(
        probe.is_ok(),
        "B dropped job 1, so B's transaction must not still hold it: {probe:?}"
    );
    ex.execute_with_session(b, "COMMIT").await.unwrap();
}

/// A row deleted by the holder while B waits is gone: B gets nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_deleted_while_waiting_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 2).await;

    let (a, b) = (ex.create_session(), ex.create_session());
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .unwrap();
    let h = blocked_reader(&ex, b, "SELECT id FROM jobs ORDER BY id FOR UPDATE").await;
    ex.execute_with_session(a, "DELETE FROM jobs WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    assert_eq!(claimed_ids(&wait_for(h).await), vec![2]);
    ex.execute_with_session(b, "COMMIT").await.unwrap();
}

/// A transaction's own uncommitted writes are what its locking read returns.
#[tokio::test]
async fn a_locking_read_sees_the_transactions_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 2).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "UPDATE jobs SET attempts = 7 WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(a, "INSERT INTO jobs VALUES (3, 'pending', 0)")
        .await
        .unwrap();
    assert_eq!(
        balances(&ex, a, "SELECT attempts FROM jobs WHERE id = 1 FOR UPDATE").await,
        vec![7]
    );
    assert_eq!(
        balances(
            &ex,
            a,
            "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED"
        )
        .await,
        vec![1, 2, 3]
    );
    ex.execute_with_session(a, "COMMIT").await.unwrap();
}

/// A table under row security is refused, not silently left with the stale
/// lock window: the recheck is a raw read and cannot honour the policy.
#[tokio::test]
async fn for_update_on_a_table_with_row_security_is_refused() {
    let ex = test_executor();
    ex.execute("CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT)")
        .await
        .unwrap();
    ex.execute("INSERT INTO docs VALUES (1, 'ada')")
        .await
        .unwrap();
    ex.execute("CREATE ROLE reader LOGIN PASSWORD 'p'")
        .await
        .unwrap();
    ex.execute("GRANT SELECT ON docs TO reader").await.unwrap();
    ex.execute("CREATE POLICY p ON docs FOR SELECT TO reader USING (owner = 'ada')")
        .await
        .unwrap();
    ex.execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let sid = ex.create_session();
    ex.bind_authenticated_session(sid, "reader").await.unwrap();
    let err = ex
        .execute_with_session(sid, "SELECT id FROM docs FOR UPDATE")
        .await
        .expect_err("must be refused, not served with an open stale-lock window");
    assert!(
        err.to_string().contains("row-level security"),
        "unexpected error: {err}"
    );
}

/// Two blocking claims with opposite ORDER BY, both blocked behind A, both
/// dropping a row when A commits. Each then needs the row the other kept, so
/// a refill that waits while holding its kept row deadlocks: both stall until
/// lock_timeout and fail with 55P03. Acquisition must follow one global order
/// (primary key), so both finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opposite_order_blocking_claims_do_not_deadlock_on_refill() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, 4).await;

    let a = ex.create_session();
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(
        a,
        "SELECT id FROM jobs WHERE id IN (1, 4) ORDER BY id FOR UPDATE",
    )
    .await
    .unwrap();

    let mut waiters = Vec::new();
    for sql in [
        "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 2 FOR UPDATE",
        "SELECT id FROM jobs WHERE status = 'pending' ORDER BY id DESC LIMIT 2 FOR UPDATE",
    ] {
        let ex2 = ex.clone();
        let sid = ex.create_session();
        waiters.push(tokio::spawn(async move {
            ex2.execute_with_session(sid, "BEGIN").await.unwrap();
            let r = ex2.execute_with_session(sid, sql).await;
            let _ = ex2.execute_with_session(sid, "COMMIT").await;
            r
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        waiters.iter().all(|w| !w.is_finished()),
        "control: both claims must be waiting on A"
    );

    ex.execute_with_session(a, "UPDATE jobs SET status = 'active' WHERE id IN (1, 4)")
        .await
        .unwrap();
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    for w in waiters {
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), w)
            .await
            .expect("a refill deadlocked: the claim is still waiting")
            .unwrap()
            .expect("neither claim may fail");
        assert_eq!(claimed_ids(&out).len(), 2, "each claim fills its LIMIT");
    }
}

/// Masking is per role: a role no policy names is not refused because some
/// other role is masked on the table.
#[tokio::test]
async fn for_update_is_refused_only_for_the_masked_role() {
    let ex = test_executor();
    ex.execute("CREATE TABLE people (id INT PRIMARY KEY, ssn TEXT)")
        .await
        .unwrap();
    ex.execute("INSERT INTO people VALUES (1, '123')")
        .await
        .unwrap();
    for role in ["analyst", "worker"] {
        ex.execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'p'"))
            .await
            .unwrap();
        ex.execute(&format!("GRANT SELECT, UPDATE ON people TO {role}"))
            .await
            .unwrap();
    }
    ex.execute("CREATE MASKING POLICY ON people (ssn) TO analyst USING REDACT '***'")
        .await
        .unwrap();

    let worker = ex.create_session();
    ex.bind_authenticated_session(worker, "worker")
        .await
        .unwrap();
    let ok = ex
        .execute_with_session(worker, "SELECT id FROM people FOR UPDATE")
        .await;
    assert!(ok.is_ok(), "an unmasked role must not be refused: {ok:?}");

    let analyst = ex.create_session();
    ex.bind_authenticated_session(analyst, "analyst")
        .await
        .unwrap();
    let err = ex
        .execute_with_session(analyst, "SELECT id FROM people FOR UPDATE")
        .await
        .expect_err("the masked role must be refused");
    assert!(err.to_string().contains("masking"), "unexpected: {err}");
}

/// A changed row whose WHERE clause cannot be evaluated on a bare row must
/// fail closed with a retryable conflict, not be returned as stale. No SQL
/// shape tried reaches this (the evaluator handles everything a claim uses),
/// so the branch is driven directly with a predicate that cannot resolve.
#[tokio::test]
async fn an_unevaluable_predicate_on_a_changed_row_fails_closed() {
    let ex = test_executor();
    seed_jobs(&ex, 1).await;
    let table_def = ex.get_table("jobs").await.unwrap();
    let col_meta = ex.table_col_meta(&table_def);
    let ctx = row_locks::RowLockContext {
        table: "jobs".into(),
        pk_columns: vec!["id".into()],
        nonblock: None,
        selection: Some(
            sqlparser::parser::Parser::new(&sqlparser::dialect::GenericDialect {})
                .try_with_sql("no_such_column > 3")
                .unwrap()
                .parse_expr()
                .unwrap(),
        ),
    };
    let scanned = vec![
        Value::Int32(1),
        Value::Text("pending".into()),
        Value::Int32(0),
    ];
    let changed = vec![
        Value::Int32(1),
        Value::Text("pending".into()),
        Value::Int32(1),
    ];

    // Unchanged: no re-evaluation needed, kept as scanned.
    let same = ex
        .recheck_locked(&ctx, &table_def, &col_meta, scanned.clone(), Some(&scanned))
        .unwrap();
    assert_eq!(same, Some(scanned.clone()));
    // Changed: cannot be judged, so it must not come back stale.
    let err = ex
        .recheck_locked(&ctx, &table_def, &col_meta, scanned, Some(&changed))
        .expect_err("must fail closed");
    assert!(
        matches!(
            err,
            ExecError::Storage(crate::storage::StorageError::WriteConflict(_))
        ),
        "unexpected: {err}"
    );
}

/// Composite primary key: the lock and the recheck work without a
/// single-column key index (the recheck falls back to one scan per round).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_primary_key_rows_are_locked_and_rechecked() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    ex.execute("CREATE TABLE lines (a INT, b INT, qty INT, PRIMARY KEY (a, b))")
        .await
        .unwrap();
    ex.execute("INSERT INTO lines VALUES (1, 1, 10), (1, 2, 20)")
        .await
        .unwrap();

    let (a, b) = (ex.create_session(), ex.create_session());
    ex.execute_with_session(a, "BEGIN").await.unwrap();
    ex.execute_with_session(a, "SELECT qty FROM lines WHERE a = 1 AND b = 2 FOR UPDATE")
        .await
        .unwrap();
    let h = blocked_reader(
        &ex,
        b,
        "SELECT qty FROM lines WHERE a = 1 AND b = 2 FOR UPDATE",
    )
    .await;
    ex.execute_with_session(a, "UPDATE lines SET qty = qty + 5 WHERE a = 1 AND b = 2")
        .await
        .unwrap();
    ex.execute_with_session(a, "COMMIT").await.unwrap();
    assert_eq!(claimed_ids(&wait_for(h).await), vec![25]);
    ex.execute_with_session(b, "COMMIT").await.unwrap();
}

/// N workers drain a queue with the given claim; every job exactly once, no
/// error from any worker. `plain` uses blocking FOR UPDATE (no SKIP LOCKED).
async fn drain(workers: usize, jobs: i64, limit: usize, plain: bool) {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_jobs(&ex, jobs).await;
    let claim = format!(
        "UPDATE jobs SET status = 'active', attempts = attempts + 1 WHERE id IN (\
           SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT {limit} \
           FOR UPDATE{}) RETURNING id",
        if plain { "" } else { " SKIP LOCKED" }
    );
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let ex = ex.clone();
        let claim = claim.clone();
        tasks.push(tokio::spawn(async move {
            let sid = ex.create_session();
            let mut got: Vec<i64> = Vec::new();
            loop {
                ex.execute_with_session(sid, "BEGIN").await.unwrap();
                let ids = claimed_ids(&ex.execute_with_session(sid, &claim).await.unwrap());
                if ids.is_empty() {
                    ex.execute_with_session(sid, "ROLLBACK").await.unwrap();
                    break;
                }
                got.extend(ids);
                ex.execute_with_session(sid, "COMMIT").await.unwrap();
            }
            got
        }));
    }
    let mut all: Vec<i64> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    all.sort();
    let want: Vec<i64> = (1..=jobs).collect();
    assert_eq!(all, want, "every job exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_limit_1_skip_locked_drain_exactly_once() {
    drain(3, 30, 1, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_workers_limit_3_skip_locked_drain_exactly_once() {
    drain(4, 50, 3, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_workers_limit_5_skip_locked_drain_exactly_once() {
    drain(4, 60, 5, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_workers_limit_1_plain_for_update_drain_exactly_once() {
    drain(3, 30, 1, true).await;
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
        &ex.execute_with_session(a, "SELECT id FROM jobs WHERE id = 1 FOR SHARE")
            .await
            .unwrap(),
    );
    assert_eq!(got, vec![1]);

    let b = ex.create_session();
    let err = ex
        .execute_with_session(b, "SELECT id FROM jobs WHERE id = 1 FOR SHARE NOWAIT")
        .await
        .expect_err("a FOR SHARE row is held; NOWAIT must refuse");
    assert!(err.to_string().contains("lock_not_available"), "got: {err}");
    ex.execute_with_session(a, "COMMIT").await.unwrap();
}

/// Shapes the lock implementation does not cover are refused, not
/// approximately locked. Each error names what it refused.
#[tokio::test]
async fn unsupported_lock_shapes_are_refused_by_name() {
    let ex = test_executor();
    ex.execute("CREATE TABLE lk (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    ex.execute("CREATE TABLE other_lk (id INT PRIMARY KEY, lk_id INT)")
        .await
        .unwrap();
    ex.execute("CREATE TABLE keyless (id INT, v INT)")
        .await
        .unwrap();

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
    assert!(grp.to_string().contains("GROUP BY"), "got: {grp}");

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
    assert!(err.to_string().contains("primary key"), "got: {err}");
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

    exec(
        &ex,
        "PREPARE take(INT) AS SELECT id FROM lim ORDER BY id LIMIT $1",
    )
    .await;
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
    assert!(err.to_string().contains("LIMIT"), "got: {err}");
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
