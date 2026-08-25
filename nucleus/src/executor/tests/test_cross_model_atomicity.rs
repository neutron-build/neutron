//! S63 slice 1: cross-model atomicity between SQL and Streams, and slice 2
//! between SQL and the KV strings WAL (`kv.wal`).
//!
//! The mechanism under test, end to end: every streams/KV WAL record written
//! inside an explicit transaction is tagged with that transaction's
//! coordinating id, the SQL COMMIT record carries the same id in its body,
//! and replay keeps a tagged record only when its id is autocommit or
//! present in the committed set recovered from the SQL WAL. Absence of a
//! commit record means discard, always.
//!
//! Every test here crosses the reopen boundary — the in-memory path was
//! always correct, which is exactly how the gaps this closes survived.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use super::{exec, scalar, text_of};
use crate::catalog::Catalog;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::wal::SyncMode;
use crate::storage::{DiskEngine, StorageEngine};
use crate::types::Value;

/// Open (or reopen) a SEGMENTED buffered-disk executor from `dir`, mirroring
/// `main.rs`'s stack — segmented because segment pruning (the reclaim half
/// of these tests) only exists on the segmented backend, buffered because
/// that is the served configuration whose COMMIT records carry the S63 body.
/// Returns the inner engine handle too, so a test can drive checkpoints the
/// way the background arm does.
async fn open_segmented(dir: &Path) -> (Executor, Arc<DiskEngine>) {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();

    let engine = Arc::new(
        DiskEngine::open_segmented_with_sync(
            &db_path,
            catalog.clone(),
            64,
            1, // 1 MB segments: sealing and pruning without megabytes of writes
            SyncMode::Fsync,
        )
        .unwrap(),
    );
    let storage: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(engine.clone()));

    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    (ex, engine)
}

async fn xlen(ex: &Executor, stream: &str) -> i64 {
    let r = exec(ex, &format!("SELECT STREAM_XLEN('{stream}')")).await;
    match scalar(&r[0]) {
        Value::Int64(n) => *n,
        other => panic!("STREAM_XLEN returned {other:?}"),
    }
}

/// KV_GET as an Option<Text>: `None` means the key is absent (or Null).
async fn kv_get(ex: &Executor, key: &str) -> Option<String> {
    let r = exec(ex, &format!("SELECT KV_GET('{key}')")).await;
    match scalar(&r[0]) {
        Value::Text(s) => Some(s.clone()),
        Value::Null => None,
        other => panic!("KV_GET({key}) returned {other:?}"),
    }
}

/// The S63 discard direction, no crash injection needed: dropping the
/// executor mid-transaction is the durable equivalent of dying before the
/// COMMIT record — the streams records were flushed by their statements, and
/// nothing vouches for them.
///
/// Before S63 the aborted XADD came BACK on restart (S31-04 fixed that for
/// ROLLBACK by rewriting the log); the uncommitted-tagged case is now closed
/// by construction instead: replay discards it because its id never
/// committed, and the COMMITTED transaction's entry in the SAME log survives
/// — the both-directions proof.
#[tokio::test]
async fn uncommitted_xadd_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + stream entry, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        exec(&ex, "SELECT STREAM_XADD('s', 'kind', 'committed')").await;
        exec(&ex, "COMMIT").await;
        // Abandoned: entry flushed to streams.wal, COMMIT never happens. The
        // executor is dropped with the transaction open — no rollback
        // compensation runs, so the ONLY thing standing between the flushed
        // record and recovery is the filter.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        exec(&ex, "SELECT STREAM_XADD('s', 'kind', 'abandoned')").await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        xlen(&ex, "s").await,
        1,
        "exactly the committed entry survives: the abandoned one was flushed to \
         the WAL and must be discarded by the filter, not replayed"
    );
    let rows = exec(&ex, "SELECT v FROM t ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "the abandoned transaction's SQL row is gone too"
    );
    assert_eq!(scalar(&rows[0]), &Value::Text("kept".into()));
}

/// Autocommit XADDs (id 0) and pre-S63 untagged records replay unconditionally.
#[tokio::test]
async fn autocommit_xadd_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        // Outside any transaction: tagged XACT_AUTOCOMMIT, no commit record
        // ever written for it — its durability point is the streams fsync.
        exec(&ex, "SELECT STREAM_XADD('auto', 'k', 'v1')").await;
        exec(&ex, "SELECT STREAM_XADD('auto', 'k', 'v2')").await;
        assert_eq!(xlen(&ex, "auto").await, 2);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        xlen(&ex, "auto").await,
        2,
        "autocommit records carry id 0 and must never be filtered"
    );
}

/// The id-monotonicity proof (S1a / risk #1): across open → checkpoint →
/// prune → reopen, a reopened executor must never mint an id that a
/// surviving record already references — a reused id makes a stale tagged
/// record indistinguishable from a live one and the filter RESURRECTS it
/// instead of discarding it.
///
/// The killer case is here: the SQL side is checkpointed and pruned (so the
/// commit-record bodies are gone) while the streams log is NOT checkpointed
/// (so the tags survive). A seed derived only from surviving SQL records
/// would restart at 1; the streams scan must hold the floor. The arm-faithful
/// pin (checkpoint_retaining at the pre-pass horizon) is what keeps the
/// bodies — and therefore the committed entries — alive through the prune.
#[tokio::test]
async fn xact_ids_never_reuse_across_checkpoint_prune_and_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: two enlisted transactions (xids 1 and 2) and one autocommit.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY)").await;
        for name in ["c1", "c2"] {
            exec(&ex, "BEGIN").await;
            exec(&ex, &format!("SELECT STREAM_XADD('m', 'n', '{name}')")).await;
            exec(&ex, "COMMIT").await;
        }
        exec(&ex, "SELECT STREAM_XADD('m', 'n', 'auto')").await;
        assert_eq!(xlen(&ex, "m").await, 3);
    }

    // Run 2: reopen (which seals run 1's segments), then an arm-faithful
    // checkpoint pass whose specialty half is skipped — the gate-tripped
    // shape. The horizon is still 1 (no pass completed in this process), so
    // the SQL checkpoint pins everything: the prune may not reclaim the
    // commit bodies while the streams tags they vouch for are un-folded.
    {
        let (ex, engine) = open_segmented(dir.path()).await;
        assert_eq!(xlen(&ex, "m").await, 3, "run 1's entries recovered");
        assert!(
            !ex.any_open_enlisted_txn(),
            "no transaction is open after reopen"
        );
        assert_eq!(ex.specialty_checkpoint_horizon(), 1);
        engine
            .checkpoint_retaining(ex.specialty_checkpoint_horizon())
            .unwrap();
    }

    // Run 3: reopen over the pruned directory. The committed entries must
    // still be there (their bodies survived the prune via the pin), and the
    // next minted id must be ABOVE every id the surviving records reference
    // — 3 or higher, never a reuse of 1 or 2.
    let (ex, engine) = open_segmented(dir.path()).await;
    assert_eq!(
        xlen(&ex, "m").await,
        3,
        "committed entries survive the prune: their commit bodies were pinned, \
         and a body-less tag would have been discarded as uncommitted"
    );
    let first_new_xid = ex.next_xact_id_probe();
    assert!(
        first_new_xid > 2,
        "id reuse across checkpoint/prune/reopen: next id is {first_new_xid}, but \
         surviving records reference ids 1 and 2 — a new transaction with either \
         id would resurrect the stale tagged records the filter exists to discard"
    );

    // And the restart-after-full-fold is the safe-reuse case: with a
    // specialty pass folding everything and the prune reclaiming the bodies,
    // ids MAY restart at 1 — nothing references the old ones anymore — and
    // nothing may be lost or resurrected.
    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT STREAM_XADD('m', 'n', 'post-prune')").await;
    exec(&ex, "COMMIT").await;
    let horizon = engine.current_wal_lsn();
    ex.checkpoint_streams_wal().unwrap();
    ex.note_specialty_checkpoint_pass(horizon);
    engine.checkpoint_retaining(horizon).unwrap();
    drop(ex);

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        xlen(&ex, "m").await,
        4,
        "after a full fold + prune, the snapshot carries everything and the \
         filter has nothing left to decide"
    );
}

/// The consumer-group writers are tagged too (recon drift #1: six opcodes,
/// four log_* writers — tagging only XADD would leave mid-transaction cursor
/// advances and acks untagged, and untagged means "keep").
#[tokio::test]
async fn group_state_of_an_uncommitted_transaction_is_discarded_on_replay() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('g', 'n', '1')").await;
        // Abandoned transaction creates a group and advances a cursor.
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('g', 'grp', 0)").await;
        let _ = exec(&ex, "SELECT STREAM_XREADGROUP('g', 'grp', 'c', 10)").await;
        // no COMMIT — dropped open
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    // The group must not exist: replaying its create/delivery records would
    // resurrect an uncommitted consumer group, whose cursor a client can
    // never rebuild. NOGROUP is the honest answer.
    let err = ex
        .execute("SELECT STREAM_XREADGROUP('g', 'grp', 'c', 10)")
        .await
        .expect_err("an abandoned transaction's group must not survive");
    assert!(
        err.to_string().contains("NOGROUP"),
        "expected NOGROUP for the discarded group, got {err}"
    );
    // The autocommit entry is untouched.
    assert_eq!(xlen(&ex, "g").await, 1);
}

/// The committed direction for group state: a group created, delivered-to and
/// acked inside a committed transaction recovers fully.
#[tokio::test]
async fn group_state_of_a_committed_transaction_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('gc', 'n', '1')").await;
        exec(&ex, "SELECT STREAM_XADD('gc', 'n', '2')").await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('gc', 'grp', 0)").await;
        let batch = text_of(
            exec(&ex, "SELECT STREAM_XREADGROUP('gc', 'grp', 'worker', 10)")
                .await
                .into_iter()
                .next()
                .unwrap(),
        );
        assert!(
            batch.contains("\"1\"") && batch.contains("\"2\""),
            "{batch}"
        );
        exec(&ex, "COMMIT").await;
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    // Cursor recovered past both entries: no redelivery.
    let again = text_of(
        exec(&ex, "SELECT STREAM_XREADGROUP('gc', 'grp', 'worker', 10)")
            .await
            .into_iter()
            .next()
            .unwrap(),
    );
    assert_eq!(
        again, "[]",
        "the committed cursor must be honoured: {again}"
    );
}

/// The S7 gate: an open ENLISTED transaction suppresses specialty
/// checkpoints; a SQL-only transaction does not.
#[tokio::test]
async fn gate_blocks_only_enlisted_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;

    assert!(!ex.any_open_enlisted_txn(), "no transaction at all");

    exec(&ex, "BEGIN").await;
    exec(&ex, "CREATE TABLE gate_t (id INTEGER)").await;
    assert!(
        !ex.any_open_enlisted_txn(),
        "a SQL-only transaction must not block a specialty checkpoint"
    );

    exec(&ex, "SELECT STREAM_XADD('gate_s', 'k', 'v')").await;
    assert!(
        ex.any_open_enlisted_txn(),
        "enlisting streams must trip the gate"
    );
    exec(&ex, "ROLLBACK").await;
    assert!(!ex.any_open_enlisted_txn(), "ROLLBACK releases the gate");
}

/// S31-13 completion: XREADGROUP's WAL error used to be discarded with
/// `let _ =` one arm below the fixed XADD/XGROUP_CREATE ones. A cursor a
/// restart cannot reproduce must not be acknowledged.
#[tokio::test]
async fn xreadgroup_fails_the_statement_when_its_wal_record_cannot_be_written() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('xrg', 'n', '1')").await;
        exec(&ex, "SELECT STREAM_XADD('xrg', 'n', '2')").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('xrg', 'g', 0)").await;

        ex.streams_wal().unwrap().set_fail_appends(true);
        let err = ex
            .execute("SELECT STREAM_XREADGROUP('xrg', 'g', 'c1', 10)")
            .await
            .expect_err("a delivery whose WAL append failed must not be acknowledged");
        assert!(
            err.to_string().contains("STREAM_XREADGROUP could not log"),
            "the error must name the failure, got {err}"
        );
        ex.streams_wal().unwrap().set_fail_appends(false);

        // The rejected delivery is rolled back in memory: a fresh consumer
        // sees both entries, so the undo restored the cursor and the PEL.
        let redelivered = text_of(
            exec(&ex, "SELECT STREAM_XREADGROUP('xrg', 'g', 'c2', 10)")
                .await
                .into_iter()
                .next()
                .unwrap(),
        );
        assert!(
            redelivered.contains("\"1\"") && redelivered.contains("\"2\""),
            "the failed statement's cursor advance must be undone: {redelivered}"
        );
    }

    // And across a restart the cursor is where the last SUCCESSFUL log put
    // it — c2's delivery — with c1's rejected one nowhere in the PEL.
    let (ex, _engine) = open_segmented(dir.path()).await;
    let pending = {
        let streams = ex.streams.read();
        streams["xrg"].xpending("g")
    };
    assert_eq!(
        pending,
        vec![("c2".to_string(), 2usize)],
        "only the acknowledged delivery is pending: {pending:?}"
    );
}

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 2: the KV strings WAL (kv.wal)
// ══════════════════════════════════════════════════════════════════════════

/// The S63 discard direction for KV, no crash injection needed: dropping
/// the executor mid-transaction is the durable equivalent of dying before
/// the COMMIT record — the KV record was flushed by its statement, and
/// nothing vouches for its id. Unlike the ROLLBACK case, NO compensation
/// runs here (the transaction is abandoned, not rolled back), so the only
/// thing standing between the flushed tagged record and recovery is the
/// filter. The committed transaction's key in the SAME log survives — the
/// both-directions proof.
#[tokio::test]
async fn uncommitted_kv_set_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + KV key, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        exec(&ex, "SELECT KV_SET('kept', 'committed')").await;
        exec(&ex, "COMMIT").await;
        // Abandoned: record flushed to kv.wal, COMMIT never happens. No
        // rollback compensation runs — the filter alone must discard it.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        exec(&ex, "SELECT KV_SET('lost', 'abandoned')").await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        kv_get(&ex, "kept").await,
        Some("committed".to_string()),
        "the committed transaction's KV write is vouched for and must survive"
    );
    assert_eq!(
        kv_get(&ex, "lost").await,
        None,
        "the abandoned transaction's KV record was flushed to kv.wal and must \
         be discarded by the filter, not replayed"
    );
    let rows = exec(&ex, "SELECT v FROM t ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "the abandoned transaction's SQL row is gone too"
    );
    assert_eq!(scalar(&rows[0]), &Value::Text("kept".into()));
}

/// Autocommit KV_SETs carry XACT_AUTOCOMMIT (0) and never need a commit
/// record — their durability point is the KV log's own fsync.
#[tokio::test]
async fn autocommit_kv_set_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT KV_SET('auto', 'v1')").await;
        exec(&ex, "SELECT KV_SET('auto2', 'v2')").await;
        assert_eq!(kv_get(&ex, "auto").await.as_deref(), Some("v1"));
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        kv_get(&ex, "auto").await.as_deref(),
        Some("v1"),
        "autocommit records carry id 0 and must never be filtered"
    );
    assert_eq!(kv_get(&ex, "auto2").await.as_deref(), Some("v2"));
}

/// A rolled-back transaction's tagged KV records are discarded by the
/// filter on replay. The rollback's compensating records ALSO handle this
/// (double protection, deliberately kept — see D4), so the outcome here is
/// asserted rather than discriminated; the filter-only discriminator is the
/// abandoned-transaction test above, where no compensation ever runs.
#[tokio::test]
async fn rolled_back_kv_set_is_gone_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT KV_SET('pre', 'before')").await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT KV_SET('rb', 'written-then-rolled-back')").await;
        exec(&ex, "SELECT KV_SET('pre', 'overwritten')").await;
        exec(&ex, "ROLLBACK").await;
        assert_eq!(kv_get(&ex, "pre").await.as_deref(), Some("before"));
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        kv_get(&ex, "rb").await,
        None,
        "the rolled-back transaction's KV write must not resurrect on reopen"
    );
    assert_eq!(
        kv_get(&ex, "pre").await.as_deref(),
        Some("before"),
        "the overwritten key's before-image must be what replay restores"
    );
}

/// The KV half of the id-monotonicity proof (S1a/D2): after a run whose
/// only surviving tagged records live in kv.wal — no COMMIT bodies (the
/// abandoned transaction never wrote one), no streams records — a reopened
/// executor must still mint ids ABOVE the ids those KV records carry.
///
/// This is the resurrection case the seed exists to prevent: an abandoned
/// record tagged 1 is on disk and unreferenced. If the counter restarted
/// at 1, the NEXT committed transaction would carry id 1, its COMMIT body
/// would vouch for id 1, and replay would resurrect the abandoned key as a
/// side effect of keeping the live one. The seed's KV scan (the max over
/// surviving tagged ids across ALL tagged logs) is what holds the floor
/// here — neither the SQL side nor streams has anything to contribute.
#[tokio::test]
async fn kv_tagged_ids_do_not_reuse_when_only_kv_wal_holds_the_floor() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: an abandoned transaction tags a KV record with id 1. Nothing
    // commits, so no COMMIT body exists anywhere afterwards.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT KV_SET('abandoned', 'never-committed')").await;
        // abandon — no COMMIT, no ROLLBACK, no compensation
    }

    // Run 2: reopen. The committed set is empty and streams hold nothing;
    // only kv.wal's max tagged id (1) can hold the floor above 1.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        assert_eq!(
            kv_get(&ex, "abandoned").await,
            None,
            "the abandoned record must be discarded on replay"
        );
        let next = ex.next_xact_id_probe();
        assert!(
            next > 1,
            "id reuse: the next minted id is {next}, but kv.wal still holds a \
             tagged record carrying id 1 — a fresh transaction with that id \
             would resurrect it"
        );
        // Prove the resurrection the seed prevents: commit a NEW KV write.
        // Its id is above 1, so vouching for it must not vouch for the
        // abandoned record.
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT KV_SET('live', 'committed-later')").await;
        exec(&ex, "COMMIT").await;
    }

    // Run 3: the live key survives, the abandoned one stays dead.
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        kv_get(&ex, "live").await.as_deref(),
        Some("committed-later")
    );
    assert_eq!(
        kv_get(&ex, "abandoned").await,
        None,
        "the committed id must not vouch for the stale tagged record"
    );
}
