//! B1 — end-to-end SSI (serializable snapshot isolation) anomaly census.
//!
//! The existing SSI tests in `storage::txn` poke `record_siread` directly, so
//! they validate the *conflict-graph algorithm* but not the pipeline that feeds
//! it: SQL -> scan -> `record_siread` -> commit-time cycle check. A scan that
//! records SIREAD on the wrong row set (e.g. a future streaming/early-exit scan
//! that trims its read set) would leave that algorithm correct and still let a
//! write-skew slip through — invisible to every current test, and invisible to
//! the Nucleus-vs-SQLite differential fuzzer (SQLite has no SSI).
//!
//! This census drives real interleaved SQL through the executor over the MVCC
//! engine and asserts the serializable outcome (first-committer-wins; the loser
//! aborts with a serialization failure). It is the standing gate for every
//! change to the MVCC scan brackets — Phase 1 streaming scans and the deferred
//! MVCC `scan_limit` early-exit must keep this green.

use super::*;

/// MVCC-backed executor — the only engine that records SIREAD / enforces SSI.
fn mvcc_executor() -> Executor {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
    Executor::new(catalog, storage)
}

const BEGIN_SER: &str = "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE";

/// True if the result is a serialization failure (the SSI abort).
fn is_serialization_failure(r: &Result<Vec<ExecResult>, ExecError>) -> bool {
    match r {
        Err(e) => {
            let m = format!("{e:?}").to_lowercase();
            m.contains("serialize") || m.contains("serialization") || m.contains("40001")
        }
        Ok(_) => false,
    }
}

async fn seed_accounts(ex: &Executor) {
    exec(ex, "CREATE TABLE accounts (id INT, balance INT)").await;
    exec(ex, "INSERT INTO accounts VALUES (1, 100)").await;
    exec(ex, "INSERT INTO accounts VALUES (2, 100)").await;
}

#[tokio::test]
async fn write_skew_is_detected() {
    // Classic write-skew. T1 reads row 2 and writes row 1; T2 reads row 1 and
    // writes row 2. Each transaction's write invalidates a predicate the other
    // read → a rw-antidependency cycle. Under SI both commit (constraint broken);
    // under SSI exactly one must abort.
    let ex = mvcc_executor();
    seed_accounts(&ex).await;
    let t1 = ex.create_session();
    let t2 = ex.create_session();

    ex.execute_with_session(t1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(t2, BEGIN_SER).await.unwrap();

    // Reads (record SIREAD on the matched rows).
    ex.execute_with_session(t1, "SELECT balance FROM accounts WHERE id = 2")
        .await
        .unwrap();
    ex.execute_with_session(t2, "SELECT balance FROM accounts WHERE id = 1")
        .await
        .unwrap();

    // Writes (each to the row the other read).
    ex.execute_with_session(t1, "UPDATE accounts SET balance = 0 WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(t2, "UPDATE accounts SET balance = 0 WHERE id = 2")
        .await
        .unwrap();

    let c1 = ex.execute_with_session(t1, "COMMIT").await;
    let c2 = ex.execute_with_session(t2, "COMMIT").await;

    assert!(
        c1.is_ok(),
        "first committer should win, got {c1:?}"
    );
    assert!(
        is_serialization_failure(&c2),
        "second committer must abort with a serialization failure, got {c2:?}"
    );
}

#[tokio::test]
async fn write_skew_via_full_table_scan_reads() {
    // The read side is a full-table scan (no WHERE), which records SIREAD on
    // ALL visible rows via the trait scan path — the exact read set a streaming
    // or early-exit scan would trim. If a future scan under-records here, this
    // write-skew stops being detected. This is THE gate case for MVCC scan
    // changes.
    let ex = mvcc_executor();
    seed_accounts(&ex).await;
    let t1 = ex.create_session();
    let t2 = ex.create_session();

    ex.execute_with_session(t1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(t2, BEGIN_SER).await.unwrap();

    // Full-table reads: each transaction reads every row (incl. the row the
    // other will write).
    ex.execute_with_session(t1, "SELECT balance FROM accounts")
        .await
        .unwrap();
    ex.execute_with_session(t2, "SELECT balance FROM accounts")
        .await
        .unwrap();

    ex.execute_with_session(t1, "UPDATE accounts SET balance = 0 WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(t2, "UPDATE accounts SET balance = 0 WHERE id = 2")
        .await
        .unwrap();

    let c1 = ex.execute_with_session(t1, "COMMIT").await;
    let c2 = ex.execute_with_session(t2, "COMMIT").await;

    assert!(c1.is_ok(), "first committer should win, got {c1:?}");
    assert!(
        is_serialization_failure(&c2),
        "full-scan write-skew must be detected; got {c2:?}"
    );
}

#[tokio::test]
async fn disjoint_tables_both_commit_no_false_positive() {
    // Two serializable transactions operating on strictly disjoint relations
    // share no read/write set → both must commit. Guards against SSI
    // manufacturing conflicts out of nothing (an over-conservative regression
    // that aborted independent work).
    //
    // Note: an *unindexed* `WHERE id = k` read is a full-relation predicate read
    // (records SIREAD over the whole table, exactly like PostgreSQL's relation
    // SIReadLock), so two txns "disjoint by id" on the SAME table legitimately
    // conflict. True disjointness therefore means disjoint tables.
    let ex = mvcc_executor();
    exec(&ex, "CREATE TABLE a (id INT, v INT)").await;
    exec(&ex, "CREATE TABLE b (id INT, v INT)").await;
    exec(&ex, "INSERT INTO a VALUES (1, 100)").await;
    exec(&ex, "INSERT INTO b VALUES (1, 100)").await;
    let t1 = ex.create_session();
    let t2 = ex.create_session();

    ex.execute_with_session(t1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(t2, BEGIN_SER).await.unwrap();

    ex.execute_with_session(t1, "SELECT v FROM a").await.unwrap();
    ex.execute_with_session(t2, "SELECT v FROM b").await.unwrap();
    ex.execute_with_session(t1, "UPDATE a SET v = 1 WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(t2, "UPDATE b SET v = 1 WHERE id = 1")
        .await
        .unwrap();

    let c1 = ex.execute_with_session(t1, "COMMIT").await;
    let c2 = ex.execute_with_session(t2, "COMMIT").await;

    assert!(c1.is_ok(), "disjoint-table T1 should commit, got {c1:?}");
    assert!(
        c2.is_ok(),
        "disjoint-table T2 must not be spuriously aborted, got {c2:?}"
    );
}

#[tokio::test]
async fn read_only_transaction_does_not_abort_a_writer() {
    // A read-only serializable transaction concurrent with a writer forms no
    // dangerous structure on its own → the writer commits. Guards against a
    // read set that manufactures conflicts against read-only peers.
    let ex = mvcc_executor();
    seed_accounts(&ex).await;
    let reader = ex.create_session();
    let writer = ex.create_session();

    ex.execute_with_session(reader, BEGIN_SER).await.unwrap();
    ex.execute_with_session(writer, BEGIN_SER).await.unwrap();

    ex.execute_with_session(reader, "SELECT balance FROM accounts")
        .await
        .unwrap();
    ex.execute_with_session(writer, "UPDATE accounts SET balance = 5 WHERE id = 1")
        .await
        .unwrap();

    let cw = ex.execute_with_session(writer, "COMMIT").await;
    let cr = ex.execute_with_session(reader, "COMMIT").await;

    assert!(cw.is_ok(), "writer should commit, got {cw:?}");
    assert!(cr.is_ok(), "read-only txn should commit, got {cr:?}");
}

#[tokio::test]
async fn lost_update_same_row_is_prevented() {
    // Two transactions read the same row then both write it (read-modify-write).
    // A serializable engine must not silently lose one update: the second
    // committer aborts (first-updater/committer wins).
    let ex = mvcc_executor();
    seed_accounts(&ex).await;
    let t1 = ex.create_session();
    let t2 = ex.create_session();

    ex.execute_with_session(t1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(t2, BEGIN_SER).await.unwrap();

    ex.execute_with_session(t1, "SELECT balance FROM accounts WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(t2, "SELECT balance FROM accounts WHERE id = 1")
        .await
        .unwrap();
    ex.execute_with_session(t1, "UPDATE accounts SET balance = balance + 10 WHERE id = 1")
        .await
        .unwrap();
    // T2's write to the same row may conflict at write time or at commit; accept
    // either, but the net effect must not be a silently lost update.
    let w2 = ex
        .execute_with_session(t2, "UPDATE accounts SET balance = balance + 20 WHERE id = 1")
        .await;

    let c1 = ex.execute_with_session(t1, "COMMIT").await;
    let c2 = ex.execute_with_session(t2, "COMMIT").await;

    assert!(c1.is_ok(), "first writer should commit, got {c1:?}");
    let t2_blocked = is_serialization_failure(&w2)
        || w2.is_err()
        || is_serialization_failure(&c2)
        || c2.is_err();
    assert!(
        t2_blocked,
        "second read-modify-write must not silently commit a lost update; \
         write={w2:?} commit={c2:?}"
    );
}

// Known limitation (documented, deliberately not asserted): Nucleus records
// SIREAD on concrete existing row indices, not predicate/gap ranges. A pure
// *phantom* write-skew — two txns whose writes are new rows conditional on a
// predicate COUNT the other would have changed — is therefore NOT detected
// (both commit). PostgreSQL catches this via relation-level SIReadLock on a
// seqscan. This is an SSI completeness gap, tracked separately; it is distinct
// from the scan read-set regressions this census gates (which concern EXISTING
// rows and are covered above). Adding predicate-lock tracking would close it.
