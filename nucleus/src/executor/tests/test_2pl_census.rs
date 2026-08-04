//! R6 — serializable anomaly census for the DISK engine (strict 2PL).
//!
//! `test_ssi_census` is the same census against `MvccStorageAdapter`, which
//! gets serializability from SSI. This one covers `BufferedDiskEngine` — the
//! engine `main.rs` actually builds for every server deployment — which has no
//! versioning and gets serializability from table-level strict two-phase
//! locking instead (`storage::lock_manager`).
//!
//! It is a separate file rather than a parameterization of the SSI census
//! because the two mechanisms fail differently, and pretending otherwise would
//! weaken both. SSI is optimistic: operations never block, and the loser finds
//! out at commit. 2PL is pessimistic: the loser BLOCKS at the conflicting
//! operation, and only dies if breaking a potential deadlock requires it. So
//! the SSI census can drive both transactions from one sequential task, and
//! this one cannot — a sequential harness would block on the first conflict and
//! hang forever, having proved nothing. Each transaction here runs in its own
//! task, which is also how a real client produces these interleavings.
//!
//! What is asserted throughout is the OUTCOME, not the mechanism: whichever way
//! the engine resolves a conflict, the final state must be one a serial
//! execution could have produced.

use super::*;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::disk_engine::DiskEngine;

const BEGIN_SER: &str = "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE";

/// A disk-backed executor, matching what `main.rs` constructs.
fn disk_executor(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.join("t.db"), catalog.clone()).unwrap());
    let engine: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    Arc::new(Executor::new(catalog, engine))
}

/// Whether a losing transaction failed for a reason a client can act on.
///
/// Two shapes are legitimate and a real client sees both. The kill itself is a
/// serialization failure (SQLSTATE 40001, "retry me"). But a transaction is
/// killed at ONE statement and the client usually has more statements queued —
/// COMMIT at minimum — and those report that the transaction is aborted
/// (25P02, "roll back and start over"). Which one surfaces depends on where in
/// the script the kill landed, so a test that accepts only the first is
/// intermittently wrong: this one was, failing about one full-suite run in
/// three, and the investigation found that the follow-up error was being
/// classified XX000 rather than 25P02 — a real defect in the wire mapping,
/// not just a test artifact.
fn is_serialization_failure<T>(r: &Result<T, ExecError>) -> bool {
    match r {
        Err(e) => {
            let m = format!("{e:?}").to_lowercase();
            m.contains("serialize")
                || m.contains("serialization")
                || m.contains("40001")
                || m.contains("current transaction is aborted")
        }
        Ok(_) => false,
    }
}

async fn seed_accounts(ex: &Executor) {
    exec(ex, "CREATE TABLE accounts (id INT, balance INT)").await;
    exec(ex, "INSERT INTO accounts VALUES (1, 100)").await;
    exec(ex, "INSERT INTO accounts VALUES (2, 100)").await;
}

/// Read a single integer scalar.
async fn read_int(ex: &Executor, sql: &str) -> i64 {
    let res = exec(ex, sql).await;
    match &res[0] {
        ExecResult::Select { rows, .. } => match rows[0][0] {
            Value::Int64(v) => v,
            Value::Int32(v) => v as i64,
            ref other => panic!("expected an integer, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

// ── The census ───────────────────────────────────────────────────────────────

/// Classic write skew, with the conditional that makes it a real test.
///
/// Each transaction reads the OTHER account and only zeroes its own if what it
/// read was still positive — the "at least one must stay positive" invariant.
/// Serially the second transaction reads the first one's zero and declines to
/// write, so the invariant holds. Under snapshot isolation both read 100, both
/// write, and it breaks.
///
/// The conditional is load-bearing. An earlier version wrote unconditionally
/// and asserted "not both committed", which was wrong and intermittently failed
/// for the right reason: if the two transactions do not actually overlap, both
/// committing IS a correct serial execution. Asserting on the INVARIANT instead
/// of on who committed makes the test independent of how the tasks interleave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_skew_does_not_survive() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    /// Read `read_id`; if it is still positive, zero `write_id`.
    ///
    /// The barrier makes the dangerous interleaving DETERMINISTIC: both
    /// transactions complete their read before either attempts its write. That
    /// is the only interleaving under which write skew is possible at all, so
    /// without it the test passes for the uninteresting reason that the two
    /// transactions happened to run one after the other — which is exactly how
    /// an earlier version of this test managed to pass while the mechanism it
    /// was meant to check was doing nothing.
    async fn skew_txn(
        ex: Arc<Executor>,
        s: u64,
        read_id: i32,
        write_id: i32,
        gate: Arc<tokio::sync::Barrier>,
    ) -> Result<(), ExecError> {
        let r = async {
            ex.execute_with_session(s, BEGIN_SER).await?;
            let res = ex
                .execute_with_session(
                    s,
                    &format!("SELECT balance FROM accounts WHERE id = {read_id}"),
                )
                .await?;
            let seen = match &res[0] {
                ExecResult::Select { rows, .. } => match rows[0][0] {
                    Value::Int64(v) => v,
                    Value::Int32(v) => v as i64,
                    _ => panic!("non-integer balance"),
                },
                _ => panic!("expected Select"),
            };
            gate.wait().await;
            if seen > 0 {
                ex.execute_with_session(
                    s,
                    &format!("UPDATE accounts SET balance = 0 WHERE id = {write_id}"),
                )
                .await?;
            }
            ex.execute_with_session(s, "COMMIT").await
        }
        .await;
        // A real client rolls back after an error. The engine must not DEPEND
        // on that for liveness (a killed transaction releases its locks
        // immediately), but the test should model a correct client.
        if r.is_err() {
            let _ = ex.execute_with_session(s, "ROLLBACK").await;
        }
        r.map(|_| ())
    }

    let s1 = ex.create_session();
    let s2 = ex.create_session();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let a = tokio::spawn(skew_txn(ex.clone(), s1, 2, 1, gate.clone()));
    let b = tokio::spawn(skew_txn(ex.clone(), s2, 1, 2, gate.clone()));
    let (ra, rb) = (a.await.unwrap(), b.await.unwrap());

    let b1 = read_int(&ex, "SELECT balance FROM accounts WHERE id = 1").await;
    let b2 = read_int(&ex, "SELECT balance FROM accounts WHERE id = 2").await;
    assert!(
        b1 > 0 || b2 > 0,
        "write skew survived: both accounts were zeroed on stale reads \
         ({b1}, {b2}) — no serial order produces this"
    );
    assert!(
        ra.is_ok() || rb.is_ok(),
        "at least one transaction should have made progress; got {ra:?} / {rb:?}"
    );
    // Any transaction that did not commit must say so as a serialization
    // failure, not some other error the client cannot act on.
    for r in [&ra, &rb] {
        if r.is_err() {
            assert!(
                is_serialization_failure(r),
                "a conflict must surface as a serialization failure, got {r:?}"
            );
        }
    }
}

/// Lost update: both transactions read the same balance and write back a value
/// derived from it. Serially the second sees the first's write, so the two
/// increments compound. This is the anomaly R1 measured on this exact engine
/// before it started refusing SERIALIZABLE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_update_is_lost() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    let mut handles = Vec::new();
    for _ in 0..2 {
        let ex = ex.clone();
        let s = ex.create_session();
        handles.push(tokio::spawn(async move {
            let r = async {
                ex.execute_with_session(s, BEGIN_SER).await?;
                let read = ex
                    .execute_with_session(s, "SELECT balance FROM accounts WHERE id = 1")
                    .await?;
                let current = match &read[0] {
                    ExecResult::Select { rows, .. } => match rows[0][0] {
                        Value::Int64(v) => v,
                        Value::Int32(v) => v as i64,
                        _ => panic!("non-integer balance"),
                    },
                    _ => panic!("expected Select"),
                };
                ex.execute_with_session(
                    s,
                    &format!(
                        "UPDATE accounts SET balance = {} WHERE id = 1",
                        current + 10
                    ),
                )
                .await?;
                ex.execute_with_session(s, "COMMIT").await
            }
            .await;
            if r.is_err() {
                let _ = ex.execute_with_session(s, "ROLLBACK").await;
            }
            r
        }));
    }
    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let committed = results.iter().filter(|r| r.is_ok()).count();
    let balance = read_int(&ex, "SELECT balance FROM accounts WHERE id = 1").await;

    // Each committed transaction must be visible in the result. Two commits
    // means both increments compounded (100 -> 120); one commit means 110.
    let expected = 100 + 10 * committed as i64;
    assert_eq!(
        balance, expected,
        "{committed} transaction(s) committed, so the balance must be \
         {expected} — a read-modify-write was lost"
    );
    for r in &results {
        if r.is_err() {
            assert!(is_serialization_failure(r), "unexpected error: {r:?}");
        }
    }
}

/// Phantom read: T1 counts rows matching a predicate, T2 inserts a row that
/// matches it, T1 counts again. A serializable execution cannot show T1 two
/// different counts. Table-level S locks rule this out by construction — which
/// is exactly why the lock is taken on the table rather than on the rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_does_not_grow_underneath_a_reader() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    let s1 = ex.create_session();
    let s2 = ex.create_session();

    // T1 opens first and reads, so it holds S and is the older transaction.
    ex.execute_with_session(s1, BEGIN_SER).await.unwrap();
    let first = read_count(&ex, s1, "SELECT COUNT(*) FROM accounts WHERE balance > 50").await;

    // T2 tries to insert a matching row and runs to completion FIRST. Awaiting
    // it before T1's second read is what makes this deterministic: without
    // locking T2 commits and T1 provably sees the phantom, so the test fails
    // for the right reason rather than depending on scheduling. It cannot
    // deadlock — T2 is younger, so wait-die kills it rather than parking it
    // behind T1's shared lock.
    let inserter = {
        let ex = ex.clone();
        tokio::spawn(async move {
            let r = async {
                ex.execute_with_session(s2, BEGIN_SER).await?;
                ex.execute_with_session(s2, "INSERT INTO accounts VALUES (3, 999)")
                    .await?;
                ex.execute_with_session(s2, "COMMIT").await
            }
            .await;
            if r.is_err() {
                let _ = ex.execute_with_session(s2, "ROLLBACK").await;
            }
            r
        })
    };
    let r = inserter.await.unwrap();

    // T1 re-reads. T2 has finished one way or the other, so if it was allowed
    // to insert, the phantom is definitely visible by now.
    let second = read_count(&ex, s1, "SELECT COUNT(*) FROM accounts WHERE balance > 50").await;
    assert_eq!(
        first, second,
        "a serializable reader saw a phantom row appear mid-transaction"
    );
    ex.execute_with_session(s1, "COMMIT").await.unwrap();

    if r.is_err() {
        assert!(is_serialization_failure(&r), "unexpected error: {r:?}");
    }
}

async fn read_count(ex: &Executor, session: u64, sql: &str) -> i64 {
    let res = ex.execute_with_session(session, sql).await.unwrap();
    match &res[0] {
        ExecResult::Select { rows, .. } => match rows[0][0] {
            Value::Int64(v) => v,
            Value::Int32(v) => v as i64,
            ref other => panic!("expected an integer count, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Transactions touching disjoint tables must not interfere. A serializable
/// implementation that simply serialized everything would pass every test
/// above and be useless; this is the one that would catch it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disjoint_tables_do_not_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(&ex, "CREATE TABLE a (id INT, v INT)").await;
    exec(&ex, "CREATE TABLE b (id INT, v INT)").await;
    exec(&ex, "INSERT INTO a VALUES (1, 1)").await;
    exec(&ex, "INSERT INTO b VALUES (1, 1)").await;

    let s1 = ex.create_session();
    let s2 = ex.create_session();
    let t1 = {
        let ex = ex.clone();
        tokio::spawn(async move {
            let r = async {
                ex.execute_with_session(s1, BEGIN_SER).await?;
                ex.execute_with_session(s1, "SELECT v FROM a").await?;
                ex.execute_with_session(s1, "UPDATE a SET v = 2 WHERE id = 1")
                    .await?;
                ex.execute_with_session(s1, "COMMIT").await
            }
            .await;
            if r.is_err() {
                let _ = ex.execute_with_session(s1, "ROLLBACK").await;
            }
            r
        })
    };
    let t2 = {
        let ex = ex.clone();
        tokio::spawn(async move {
            let r = async {
                ex.execute_with_session(s2, BEGIN_SER).await?;
                ex.execute_with_session(s2, "SELECT v FROM b").await?;
                ex.execute_with_session(s2, "UPDATE b SET v = 2 WHERE id = 1")
                    .await?;
                ex.execute_with_session(s2, "COMMIT").await
            }
            .await;
            if r.is_err() {
                let _ = ex.execute_with_session(s2, "ROLLBACK").await;
            }
            r
        })
    };
    assert!(t1.await.unwrap().is_ok(), "disjoint transaction aborted");
    assert!(t2.await.unwrap().is_ok(), "disjoint transaction aborted");
}

/// A read-only serializable transaction must not be able to block a writer
/// forever, and must release its locks at COMMIT like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_transaction_releases_its_locks() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    let s1 = ex.create_session();
    ex.execute_with_session(s1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(s1, "SELECT * FROM accounts")
        .await
        .unwrap();
    ex.execute_with_session(s1, "COMMIT").await.unwrap();

    // With s1 finished, a writer must proceed without waiting.
    let s2 = ex.create_session();
    ex.execute_with_session(s2, BEGIN_SER).await.unwrap();
    ex.execute_with_session(s2, "UPDATE accounts SET balance = 5 WHERE id = 1")
        .await
        .expect("a released read lock must not block a later writer");
    ex.execute_with_session(s2, "COMMIT").await.unwrap();
    assert_eq!(
        read_int(&ex, "SELECT balance FROM accounts WHERE id = 1").await,
        5
    );
}

/// ROLLBACK must release locks too — otherwise one abandoned transaction
/// wedges the table permanently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_releases_locks() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    let s1 = ex.create_session();
    ex.execute_with_session(s1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(s1, "UPDATE accounts SET balance = 1 WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(s1, "ROLLBACK").await.unwrap();

    let s2 = ex.create_session();
    ex.execute_with_session(s2, BEGIN_SER).await.unwrap();
    ex.execute_with_session(s2, "UPDATE accounts SET balance = 7 WHERE id = 1")
        .await
        .expect("an aborted transaction's exclusive lock must be released");
    ex.execute_with_session(s2, "COMMIT").await.unwrap();
    assert_eq!(
        read_int(&ex, "SELECT balance FROM accounts WHERE id = 1").await,
        7,
        "the rolled-back write must not have survived either"
    );
}

/// A non-serializable session takes no locks at all, so the existing
/// read-committed behaviour is untouched — including that it can still write a
/// table a serializable transaction is reading. That is not a bug: PostgreSQL's
/// serializable guarantee likewise holds only among serializable transactions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_sessions_are_not_slowed_by_the_lock_table() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    let s1 = ex.create_session();
    ex.execute_with_session(s1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(s1, "SELECT * FROM accounts")
        .await
        .unwrap();

    // A plain (read-committed) session writes the same table without blocking.
    let s2 = ex.create_session();
    ex.execute_with_session(s2, "BEGIN").await.unwrap();
    ex.execute_with_session(s2, "UPDATE accounts SET balance = 42 WHERE id = 1")
        .await
        .expect("a read-committed writer must not be blocked by 2PL locks");
    ex.execute_with_session(s2, "COMMIT").await.unwrap();

    ex.execute_with_session(s1, "COMMIT").await.unwrap();
}

/// A transaction killed to break a deadlock must release its locks AT THE
/// MOMENT IT DIES, not when the client eventually says ROLLBACK.
///
/// This is a regression test for a real liveness bug the census caught on its
/// first run: the census hung. Two transactions each held S on `accounts` and
/// each wanted X (the lost-update interleaving). Wait-die correctly killed the
/// younger, but the killed transaction kept its shared lock, so the OLDER
/// transaction — which had done nothing wrong and was correctly waiting — waited
/// on a lock nobody would ever drop. A client that simply stops talking after
/// the error (or crashes) would wedge the table permanently.
///
/// Here the killed session deliberately never issues ROLLBACK, so the waiter
/// can only proceed if the engine dropped those locks on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_transaction_releases_its_locks_without_waiting_for_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;

    let older = ex.create_session();
    let younger = ex.create_session();

    // `older` reads first, so it gets the lower age and will WAIT rather than die.
    ex.execute_with_session(older, BEGIN_SER).await.unwrap();
    ex.execute_with_session(older, "SELECT * FROM accounts")
        .await
        .unwrap();
    // `younger` reads second: compatible shared lock, higher age.
    ex.execute_with_session(younger, BEGIN_SER).await.unwrap();
    ex.execute_with_session(younger, "SELECT * FROM accounts")
        .await
        .unwrap();

    // The older transaction wants to upgrade and must block on the younger's S.
    let waiter = {
        let ex = ex.clone();
        tokio::spawn(async move {
            ex.execute_with_session(older, "UPDATE accounts SET balance = 1 WHERE id = 1")
                .await
        })
    };

    // The younger one also tries to upgrade and is killed by wait-die. It then
    // goes silent — no ROLLBACK, exactly like a client that crashed.
    let killed = ex
        .execute_with_session(younger, "UPDATE accounts SET balance = 2 WHERE id = 1")
        .await;
    assert!(
        is_serialization_failure(&killed),
        "the younger upgrader should have been killed, got {killed:?}"
    );

    // The waiter must now be able to finish. If the killed transaction's locks
    // leaked, this await never returns.
    let r = waiter.await.unwrap();
    assert!(
        r.is_ok(),
        "the older transaction was left waiting on a dead transaction's lock: {r:?}"
    );
    ex.execute_with_session(older, "COMMIT").await.unwrap();
    assert_eq!(
        read_int(&ex, "SELECT balance FROM accounts WHERE id = 1").await,
        1
    );
}

/// A killed transaction must not be able to commit its buffered writes. Its
/// locks are already gone by then, so applying them would be exactly the
/// unlocked write the kill existed to prevent.
///
/// The buffered write goes to a DIFFERENT table from the one the kill happens
/// on, which is both what makes the setup possible (a write to the contended
/// table would be killed at that statement, before anything is buffered) and a
/// sharper assertion: the discard covers everything the transaction did, not
/// just the statement that lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_transaction_cannot_commit_anyway() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;
    exec(&ex, "CREATE TABLE side (id INT, v INT)").await;
    exec(&ex, "INSERT INTO side VALUES (1, 1)").await;

    let older = ex.create_session();
    let younger = ex.create_session();

    // `older` locks first, so it is the older transaction.
    ex.execute_with_session(older, BEGIN_SER).await.unwrap();
    ex.execute_with_session(older, "SELECT * FROM accounts")
        .await
        .unwrap();

    // `younger` buffers a write to an uncontended table...
    ex.execute_with_session(younger, BEGIN_SER).await.unwrap();
    ex.execute_with_session(younger, "UPDATE side SET v = 555 WHERE id = 1")
        .await
        .unwrap();
    // ...then reaches for the table `older` holds, and is killed.
    let killed = ex
        .execute_with_session(younger, "UPDATE accounts SET balance = 2 WHERE id = 1")
        .await;
    assert!(
        is_serialization_failure(&killed),
        "the younger transaction should have been killed, got {killed:?}"
    );

    // A client that ignores the error and commits anyway must not get its
    // writes. The COMMIT itself is allowed to SUCCEED — the executor turns
    // COMMIT of an aborted transaction into a ROLLBACK, which is what
    // PostgreSQL does and reports as `ROLLBACK` rather than an error. What
    // must not happen is the writes landing, so that is what is asserted.
    let _ = ex.execute_with_session(younger, "COMMIT").await;

    ex.execute_with_session(older, "COMMIT").await.unwrap();
    assert_eq!(
        read_int(&ex, "SELECT v FROM side WHERE id = 1").await,
        1,
        "the killed transaction's buffered write to an unrelated table must \
         not have landed either"
    );
}

/// `SET lock_timeout` bounds how long a serializable transaction blocks. The
/// value has to actually reach the lock table — a SET that parses, reports
/// success and changes nothing is the exact defect shape this whole series has
/// been about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_lock_timeout_bounds_the_wait() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    seed_accounts(&ex).await;
    exec(&ex, "SET lock_timeout = '80ms'").await;

    // `holder` locks first and never lets go, so `waiter` is OLDER-safe: it
    // waits rather than dying, which is the case a timeout has to rescue.
    let waiter_s = ex.create_session();
    let holder_s = ex.create_session();
    ex.execute_with_session(waiter_s, BEGIN_SER).await.unwrap();
    ex.execute_with_session(waiter_s, "SELECT * FROM accounts")
        .await
        .unwrap();
    ex.execute_with_session(holder_s, BEGIN_SER).await.unwrap();
    // holder is YOUNGER, so its write dies rather than waiting — take the
    // exclusive lock on a table waiter has not read, then have waiter want it.
    exec(&ex, "CREATE TABLE side (id INT, v INT)").await;
    exec(&ex, "INSERT INTO side VALUES (1, 1)").await;
    ex.execute_with_session(holder_s, "UPDATE side SET v = 2 WHERE id = 1")
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let r = ex
        .execute_with_session(waiter_s, "SELECT * FROM side")
        .await;
    let waited = started.elapsed();

    let err = r.expect_err("the older waiter should have timed out");
    assert!(
        format!("{err:?}").contains("lock_not_available"),
        "a lock timeout must be reported as such, not as a serialization \
         failure a client would retry forever: {err:?}"
    );
    assert!(
        waited < std::time::Duration::from_secs(5),
        "waited {waited:?} — the 80ms bound did not reach the lock table"
    );
    let _ = ex.execute_with_session(waiter_s, "ROLLBACK").await;
    let _ = ex.execute_with_session(holder_s, "ROLLBACK").await;
}

/// A bad `lock_timeout` must be refused. Silently coercing `'5s'` to 0 would
/// turn the setting into "wait forever" — the exact failure it prevents —
/// while telling the client the SET succeeded.
#[tokio::test]
async fn an_invalid_lock_timeout_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    for bad in ["'abc'", "'5 fortnights'", "'-1'"] {
        let r = ex.execute(&format!("SET lock_timeout = {bad}")).await;
        assert!(r.is_err(), "SET lock_timeout = {bad} should have failed");
    }
    // And the accepted forms really are accepted.
    for good in ["0", "5000", "'250ms'", "'2s'", "'1min'"] {
        exec(&ex, &format!("SET lock_timeout = {good}")).await;
    }
}

/// The disk engine now ACCEPTS serializable rather than refusing it (R1's
/// refusal was explicitly the honest interim, not the destination).
#[tokio::test]
async fn the_disk_engine_accepts_every_isolation_level() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    for level in [
        "SERIALIZABLE",
        "REPEATABLE READ",
        "READ COMMITTED",
        "SNAPSHOT",
    ] {
        exec(&ex, &format!("BEGIN TRANSACTION ISOLATION LEVEL {level}")).await;
        exec(&ex, "ROLLBACK").await;
    }
    exec(&ex, "SET transaction_isolation = 'serializable'").await;
}
