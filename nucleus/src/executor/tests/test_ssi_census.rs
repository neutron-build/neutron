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
async fn write_skew_via_streaming_filtered_scan_reads() {
    // THE gate for the streaming WHERE filter. The read side streams a full scan
    // with a WHERE that matches NOTHING (`id = 99`): the filter reads (and must
    // record SIREAD on) every visible row, then emits none. A matched-only scan
    // would record an EMPTY read set here and let the write-skew slip through
    // undetected. Because this filter is a post-scan filter over the full-relation
    // scan, the conservative read set is preserved and the second committer aborts.
    //
    // The stream is drained (`materialize`) before the writes, replicating the wire
    // flow where a query's rows are fully sent before the next command arrives — so
    // the producer's SIREAD is recorded under this transaction before COMMIT.
    let ex = {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = Arc::new(crate::storage::MvccStorageAdapter::new());
        let ex = Arc::new(Executor::new(catalog, storage));
        ex.install_self_ref(); // the filter needs an owning Arc to engage
        ex
    };
    seed_accounts(&ex).await;
    let t1 = ex.create_session();
    let t2 = ex.create_session();
    for s in [t1, t2] {
        ex.execute_with_session(s, "SET stream_results = on")
            .await
            .unwrap();
    }

    ex.execute_with_session(t1, BEGIN_SER).await.unwrap();
    ex.execute_with_session(t2, BEGIN_SER).await.unwrap();

    // Streaming filtered reads — match nothing, but read (SIREAD) the whole table.
    for s in [t1, t2] {
        let mut rs = ex
            .execute_with_session(s, "SELECT balance FROM accounts WHERE id = 99")
            .await
            .unwrap();
        let r = rs.pop().unwrap();
        assert!(r.is_stream(), "the filtered read must actually stream");
        // Drain the stream so the full-relation scan (and its SIREAD) completes.
        let _ = r.materialize().await.unwrap();
    }

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
        "streaming filtered scan must keep the conservative full-relation SIREAD; got {c2:?}"
    );
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

// ─── Isolation levels an engine cannot honour must be refused ───────────────

/// An engine that cannot provide SERIALIZABLE must say so, not pretend.
///
/// `BEGIN ISOLATION LEVEL SERIALIZABLE` on the shipping disk engine used to
/// succeed and run at read-committed: `set_next_isolation_level` had a no-op
/// trait default and `BufferedDiskEngine` never overrode it, while
/// `supports_mvcc()` returned true. Two transactions doing a read-modify-write
/// both committed and one increment was lost, where PostgreSQL aborts one with
/// 40001. Nothing surfaced it because the request was accepted.
///
/// Losing a write is worse than refusing a connection setting: the application
/// cannot detect it, and the data is gone.
#[tokio::test]
async fn test_an_engine_refuses_isolation_it_cannot_provide() {
    use crate::storage::buffered_engine::BufferedDiskEngine;
    use crate::storage::disk_engine::DiskEngine;

    let dir = tempfile::tempdir().unwrap();
    let catalog = std::sync::Arc::new(crate::catalog::Catalog::new());
    let disk =
        std::sync::Arc::new(DiskEngine::open(&dir.path().join("t.db"), catalog.clone()).unwrap());
    let engine: std::sync::Arc<dyn crate::storage::StorageEngine> =
        std::sync::Arc::new(BufferedDiskEngine::new(disk));
    let ex = Executor::new(catalog, engine);

    for level in ["SERIALIZABLE", "REPEATABLE READ"] {
        let err = ex
            .execute(&format!("BEGIN TRANSACTION ISOLATION LEVEL {level}"))
            .await
            .expect_err("an engine that cannot provide {level} must refuse it");
        let msg = format!("{err}");
        assert!(
            msg.contains("READ COMMITTED"),
            "the error must name what the engine DOES provide: {msg}"
        );
    }

    // The level it can honour is still accepted, and so is a plain BEGIN.
    exec(&ex, "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").await;
    exec(&ex, "ROLLBACK").await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "ROLLBACK").await;

    // And the same refusal through SET, which is the other door into it.
    let err = ex
        .execute("SET transaction_isolation = 'serializable'")
        .await
        .expect_err("SET must refuse it too");
    assert!(format!("{err}").contains("READ COMMITTED"), "{err}");
}

/// The MVCC engine has SSI, so it must still accept every level.
#[tokio::test]
async fn test_the_mvcc_engine_still_accepts_serializable() {
    let catalog = std::sync::Arc::new(crate::catalog::Catalog::new());
    let storage: std::sync::Arc<dyn crate::storage::StorageEngine> =
        std::sync::Arc::new(crate::storage::MvccStorageAdapter::new());
    let ex = Executor::new(catalog, storage);
    for level in ["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED", "SNAPSHOT"] {
        exec(&ex, &format!("BEGIN TRANSACTION ISOLATION LEVEL {level}")).await;
        exec(&ex, "ROLLBACK").await;
    }
    exec(&ex, "SET transaction_isolation = 'serializable'").await;
}
