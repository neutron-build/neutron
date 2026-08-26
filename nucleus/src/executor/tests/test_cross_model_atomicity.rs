//! S63 slice 1: cross-model atomicity between SQL and Streams, slice 2
//! between SQL and the KV strings WAL (`kv.wal`), slice 3 between SQL
//! and the document WAL (`doc.wal`), slice 4 between SQL and the
//! property-graph WAL (`graph.wal`), slices 5-7 between SQL and the
//! timeseries (`ts_wal.bin`), datalog (`datalog.wal`) and columnar-model
//! (`columnar.wal`) logs, slice 9 between SQL and the blob store
//! (`blob.wal`). The KV collections WAL (`collections.wal`) is tagged and
//! filtered but boundary-held: M8 refuses its mutators inside transactions,
//! so only the autocommit shape is assertable through SQL. (The geo WAL is
//! opened but receives zero writes — see `src/geo/wal.rs`'s header for the
//! determination — so there is nothing to make atomic on that side. The CDC
//! WAL is fire-and-forget by design — see `src/reactive/cdc_wal.rs`'s
//! header and NU-107 — so it records no coordinating ids to filter.)
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

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 3: the document WAL (doc.wal)
// ══════════════════════════════════════════════════════════════════════════

/// DOC_INSERT as the assigned id.
async fn doc_insert(ex: &Executor, json: &str) -> i64 {
    let r = exec(ex, &format!("SELECT DOC_INSERT('{json}')")).await;
    match scalar(&r[0]) {
        Value::Int64(id) => *id,
        other => panic!("DOC_INSERT returned {other:?}"),
    }
}

/// DOC_GET as an Option<JSON text>: `None` means the document is absent.
async fn doc_get(ex: &Executor, id: i64) -> Option<String> {
    let r = exec(ex, &format!("SELECT DOC_GET({id})")).await;
    match scalar(&r[0]) {
        Value::Text(s) => Some(s.clone()),
        Value::Null => None,
        other => panic!("DOC_GET({id}) returned {other:?}"),
    }
}

/// The S63 discard direction for documents, no crash injection needed:
/// dropping the executor mid-transaction is the durable equivalent of dying
/// before the COMMIT record — the doc.wal record was flushed by its
/// statement, and nothing vouches for its id. No compensation runs (the
/// transaction is abandoned, not rolled back), so the only thing standing
/// between the flushed tagged record and recovery is the filter. The
/// committed transaction's document in the SAME log survives — the
/// both-directions proof.
#[tokio::test]
async fn uncommitted_doc_insert_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + document, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        let kept_id = doc_insert(&ex, r#"{"kind":"committed"}"#).await;
        exec(&ex, "COMMIT").await;
        // Abandoned: record flushed to doc.wal, COMMIT never happens. No
        // rollback compensation runs — the filter alone must discard it.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        let lost_id = doc_insert(&ex, r#"{"kind":"abandoned"}"#).await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
        assert_ne!(kept_id, lost_id);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    let kept = doc_get(&ex, 1).await;
    let lost = doc_get(&ex, 2).await;
    assert!(
        kept.as_deref().is_some_and(|j| j.contains("committed")),
        "the committed transaction's document is vouched for and must survive"
    );
    assert_eq!(
        lost, None,
        "the abandoned transaction's doc record was flushed to doc.wal and must \
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

/// Autocommit DOC_INSERTs carry XACT_AUTOCOMMIT (0) and never need a commit
/// record — their durability point is the doc log's own fsync.
#[tokio::test]
async fn autocommit_doc_insert_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        doc_insert(&ex, r#"{"a":1}"#).await;
        doc_insert(&ex, r#"{"a":2}"#).await;
        assert!(doc_get(&ex, 1).await.is_some());
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    let got = doc_get(&ex, 1)
        .await
        .expect("autocommit doc must survive reopen");
    // Compare parsed JSON, not bytes: the WAL round-trip may reserialize
    // (spacing differs between the insert path and replay), which is not a
    // value change.
    let got_v: serde_json::Value = serde_json::from_str(&got).unwrap();
    let want_v: serde_json::Value = serde_json::from_str(r#"{"a":1}"#).unwrap();
    assert_eq!(
        got_v, want_v,
        "autocommit records carry id 0 and must never be filtered"
    );
    assert!(doc_get(&ex, 2).await.is_some());
}

/// A rolled-back transaction's tagged doc records are discarded by the
/// filter on replay. The rollback's compensating records ALSO handle this
/// (double protection, deliberately kept — see D4), so the outcome here is
/// asserted rather than discriminated; the filter-only discriminator is the
/// abandoned-transaction test above, where no compensation ever runs.
#[tokio::test]
async fn rolled_back_doc_writes_are_gone_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        doc_insert(&ex, r#"{"v":"before"}"#).await;
        exec(&ex, "BEGIN").await;
        doc_insert(&ex, r#"{"v":"written-then-rolled-back"}"#).await;
        exec(&ex, "SELECT DOC_UPDATE(1, '{\"v\":\"after\"}')").await;
        exec(&ex, "ROLLBACK").await;
        assert!(doc_get(&ex, 1).await.unwrap().contains("before"));
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        doc_get(&ex, 2).await,
        None,
        "the rolled-back transaction's insert must not resurrect on reopen"
    );
    assert!(
        doc_get(&ex, 1).await.unwrap().contains("before"),
        "the overwritten document's before-image must be what replay restores"
    );
}

/// The doc half of the id-monotonicity proof (S1a/D2): after a run whose
/// only surviving tagged record lives in doc.wal — no COMMIT bodies (the
/// abandoned transaction never wrote one), nothing in kv.wal or streams.wal
/// — a reopened executor must still mint ids ABOVE the id that record
/// carries.
///
/// This is the resurrection case the seed exists to prevent: an abandoned
/// record tagged 1 is on disk and unreferenced. If the counter restarted at
/// 1, the NEXT committed transaction would carry id 1, its COMMIT body
/// would vouch for id 1, and replay would resurrect the abandoned document
/// as a side effect of keeping the live one. The seed's doc scan is what
/// holds the floor here — neither the SQL side nor the other tagged logs
/// have anything to contribute.
#[tokio::test]
async fn doc_tagged_ids_do_not_reuse_when_only_doc_wal_holds_the_floor() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: an abandoned transaction tags a doc record with id 1. Nothing
    // commits, so no COMMIT body exists anywhere afterwards.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "BEGIN").await;
        doc_insert(&ex, r#"{"kind":"abandoned"}"#).await;
        // abandon — no COMMIT, no ROLLBACK, no compensation
    }

    // Run 2: reopen. The committed set is empty and the other tagged logs
    // hold nothing; only doc.wal's max tagged id (1) can hold the floor
    // above 1.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        assert_eq!(
            doc_get(&ex, 1).await,
            None,
            "the abandoned record must be discarded on replay"
        );
        let next = ex.next_xact_id_probe();
        assert!(
            next > 1,
            "id reuse: the next minted id is {next}, but doc.wal still holds \
             a tagged record carrying id 1 — a fresh transaction with that id \
             would resurrect it"
        );
        // Prove the resurrection the seed prevents: commit a NEW doc write.
        // Its id is above 1, so vouching for it must not vouch for the
        // abandoned record.
        exec(&ex, "BEGIN").await;
        doc_insert(&ex, r#"{"kind":"committed-later"}"#).await;
        exec(&ex, "COMMIT").await;
    }

    // Run 3: the live document survives, the abandoned one stays dead.
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert!(doc_get(&ex, 1).await.unwrap().contains("committed-later"));
}

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 4: the property-graph WAL (graph.wal)
// ══════════════════════════════════════════════════════════════════════════

/// GRAPH_ADD_NODE as the assigned node id.
async fn graph_add_node(ex: &Executor, label: &str) -> i64 {
    let r = exec(ex, &format!("SELECT GRAPH_ADD_NODE('{label}')")).await;
    match scalar(&r[0]) {
        Value::Int64(id) => *id,
        other => panic!("GRAPH_ADD_NODE({label}) returned {other:?}"),
    }
}

/// Count the nodes carrying `label`, via GRAPH_QUERY's JSON result.
async fn graph_count(ex: &Executor, label: &str) -> i64 {
    let r = exec(
        ex,
        &format!("SELECT GRAPH_QUERY('MATCH (n:{label}) RETURN COUNT(*)')"),
    )
    .await;
    let json = text_of(r.into_iter().next().unwrap());
    let v: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("GRAPH_QUERY returned unparseable {json:?}: {e}"));
    v["rows"][0][0]
        .as_i64()
        .or_else(|| v["rows"][0][0].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("no count in {json:?}"))
}

/// The S63 discard direction for the graph, no crash injection needed:
/// dropping the executor mid-transaction is the durable equivalent of dying
/// before the COMMIT record — the graph.wal record was flushed by its
/// statement, and nothing vouches for its id. No compensation runs (the
/// transaction is abandoned, not rolled back), so the only thing standing
/// between the flushed tagged record and recovery is the filter. The
/// committed transaction's node in the SAME log survives — the
/// both-directions proof.
#[tokio::test]
async fn uncommitted_graph_write_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + graph node, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        graph_add_node(&ex, "Kept").await;
        exec(&ex, "COMMIT").await;
        // Abandoned: record flushed to graph.wal, COMMIT never happens. No
        // rollback compensation runs — the filter alone must discard it.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        graph_add_node(&ex, "Lost").await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        graph_count(&ex, "Kept").await,
        1,
        "the committed transaction's graph write is vouched for and must survive"
    );
    assert_eq!(
        graph_count(&ex, "Lost").await,
        0,
        "the abandoned transaction's graph record was flushed to graph.wal and \
         must be discarded by the filter, not replayed"
    );
    let rows = exec(&ex, "SELECT v FROM t ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "the abandoned transaction's SQL row is gone too"
    );
    assert_eq!(scalar(&rows[0]), &Value::Text("kept".into()));
}

/// Autocommit GRAPH_ADD_NODEs carry XACT_AUTOCOMMIT (0) and never need a
/// commit record — their durability point is the graph log's own fsync.
#[tokio::test]
async fn autocommit_graph_write_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        graph_add_node(&ex, "Auto").await;
        graph_add_node(&ex, "Auto").await;
        assert_eq!(graph_count(&ex, "Auto").await, 2);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        graph_count(&ex, "Auto").await,
        2,
        "autocommit records carry id 0 and must never be filtered"
    );
}

/// A rolled-back transaction's tagged graph records are discarded by the
/// filter on replay. The rollback's compensating records ALSO handle this
/// (double protection, deliberately kept — see D4), so the outcome here is
/// asserted rather than discriminated; the filter-only discriminator is the
/// abandoned-transaction test above, where no compensation ever runs.
#[tokio::test]
async fn rolled_back_graph_writes_are_gone_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        let base = graph_add_node(&ex, "RbBase").await;
        exec(&ex, "BEGIN").await;
        graph_add_node(&ex, "RbNew").await;
        exec(&ex, &format!("SELECT GRAPH_DELETE_NODE({base})")).await;
        exec(&ex, "ROLLBACK").await;
        assert_eq!(graph_count(&ex, "RbBase").await, 1);
        assert_eq!(graph_count(&ex, "RbNew").await, 0);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        graph_count(&ex, "RbNew").await,
        0,
        "the rolled-back transaction's node must not resurrect on reopen"
    );
    assert_eq!(
        graph_count(&ex, "RbBase").await,
        1,
        "the deleted node's before-image must be what replay restores"
    );
}

/// The graph half of the id-monotonicity proof (S1a/D2): after a run whose
/// only surviving tagged record lives in graph.wal — no COMMIT bodies (the
/// abandoned transaction never wrote one), nothing in kv.wal, doc.wal or
/// streams.wal — a reopened executor must still mint ids ABOVE the id that
/// record carries.
///
/// This is the resurrection case the seed exists to prevent: an abandoned
/// record tagged 1 is on disk and unreferenced. If the counter restarted at
/// 1, the NEXT committed transaction would carry id 1, its COMMIT body
/// would vouch for id 1, and replay would resurrect the abandoned node as a
/// side effect of keeping the live one. The seed's graph scan is what holds
/// the floor here — no other source has anything to contribute.
#[tokio::test]
async fn graph_tagged_ids_do_not_reuse_when_only_graph_wal_holds_the_floor() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: an abandoned transaction tags a graph record with id 1.
    // Nothing commits, so no COMMIT body exists anywhere afterwards.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "BEGIN").await;
        graph_add_node(&ex, "Abandoned").await;
        // abandon — no COMMIT, no ROLLBACK, no compensation
    }

    // Run 2: reopen. The committed set is empty and the other tagged logs
    // hold nothing; only graph.wal's max tagged id (1) can hold the floor
    // above 1.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        assert_eq!(
            graph_count(&ex, "Abandoned").await,
            0,
            "the abandoned record must be discarded on replay"
        );
        let next = ex.next_xact_id_probe();
        assert!(
            next > 1,
            "id reuse: the next minted id is {next}, but graph.wal still holds \
             a tagged record carrying id 1 — a fresh transaction with that id \
             would resurrect it"
        );
        // Prove the resurrection the seed prevents: commit a NEW graph
        // write. Its id is above 1, so vouching for it must not vouch for
        // the abandoned record.
        exec(&ex, "BEGIN").await;
        graph_add_node(&ex, "Live").await;
        exec(&ex, "COMMIT").await;
    }

    // Run 3: the live node survives, the abandoned one stays dead.
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(graph_count(&ex, "Live").await, 1);
    assert_eq!(
        graph_count(&ex, "Abandoned").await,
        0,
        "the committed id must not vouch for the stale tagged record"
    );
}

/// The S63 discard direction through a crash before the COMMIT record —
/// the same shape as the abandoned-transaction test, but crossing the real
/// process boundary via the probe's crashpoint (the executor-level twin is
/// `probe_crossmodel_atomicity`'s graph scenarios).
///
/// Slices 1-3 proved the gate; this asserts graph joins it: an enlisted
/// transaction holding the gate open is visible to `any_open_enlisted_txn`
/// and names its xid for the skip warning.
#[tokio::test]
async fn graph_enlistment_trips_the_s7_gate_and_names_the_xid() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;

    exec(&ex, "BEGIN").await;
    exec(&ex, "CREATE TABLE gate_t (id INTEGER)").await;
    assert!(
        !ex.any_open_enlisted_txn(),
        "a SQL-only transaction must not block a specialty checkpoint"
    );
    graph_add_node(&ex, "Gate").await;
    assert!(
        ex.any_open_enlisted_txn(),
        "enlisting graph must trip the gate"
    );
    let xids = ex.open_enlisted_xids();
    assert_eq!(
        xids.len(),
        1,
        "exactly the enlisting transaction, got {xids:?}"
    );
    exec(&ex, "ROLLBACK").await;
    assert!(!ex.any_open_enlisted_txn(), "ROLLBACK releases the gate");
    assert!(
        ex.open_enlisted_xids().is_empty(),
        "no enlisted transaction outlives its ROLLBACK"
    );
}

/// The WAL-growth mitigation (S7): an open enlisted transaction holds
/// specialty checkpoints off, and the logs it pins grow one record per
/// write. The conservative remedy is a WARN once every SKIP_WARN_EVERY
/// consecutive skipped passes, naming the open xids — never a force-sweep,
/// which would bake uncommitted state into a snapshot.
#[tokio::test]
async fn held_gate_warns_once_per_ten_skipped_passes() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;

    // A SQL-only transaction never trips the gate, so its skips are not
    // the enlisted kind; hold a real enlisted one open.
    exec(&ex, "BEGIN").await;
    graph_add_node(&ex, "Holder").await;
    assert!(ex.any_open_enlisted_txn());
    assert_eq!(ex.open_enlisted_xids(), vec![1]);

    // Nine skipped passes: the warning must not fire yet.
    for _ in 0..9 {
        ex.note_specialty_checkpoint_skip(60);
    }
    assert_eq!(
        ex.specialty_checkpoint_warns(),
        0,
        "the warn is once per ten skips, not per skip"
    );
    // The tenth fires exactly once, and names the holder.
    ex.note_specialty_checkpoint_skip(60);
    assert_eq!(ex.specialty_checkpoint_warns(), 1);
    assert_eq!(ex.specialty_checkpoint_skips(), 10);

    // Ten more: the run continues, so the warn repeats on the twentieth.
    for _ in 0..9 {
        ex.note_specialty_checkpoint_skip(60);
    }
    assert_eq!(ex.specialty_checkpoint_warns(), 1);
    ex.note_specialty_checkpoint_skip(60);
    assert_eq!(ex.specialty_checkpoint_warns(), 2);

    // A completed pass ends the run: the warn cadence restarts, so nine
    // post-reset skips fire nothing and the tenth warns again.
    ex.note_specialty_checkpoint_pass(1);
    for _ in 0..9 {
        ex.note_specialty_checkpoint_skip(60);
    }
    assert_eq!(
        ex.specialty_checkpoint_warns(),
        2,
        "a completed pass resets the skip run: nine fresh skips warn nothing"
    );
    ex.note_specialty_checkpoint_skip(60);
    assert_eq!(ex.specialty_checkpoint_warns(), 3);
    ex.note_specialty_checkpoint_skip(60);
    assert_eq!(
        ex.specialty_checkpoint_warns(),
        3,
        "the eleventh skip of the run warns nothing — once per ten, not more"
    );

    // Ending the transaction releases the gate, and the xid list empties.
    exec(&ex, "ROLLBACK").await;
    assert!(ex.open_enlisted_xids().is_empty());
}

/// S95 finding 5: the S7 gate in `main.rs` is check-then-act — a BEGIN plus
/// tagged write landing between the gate read and a specialty snapshot bakes
/// uncommitted state into a snapshot the S6 recovery filter cannot discard.
/// Each tagged log's checkpoint therefore re-checks the gate itself and
/// declines this pass while any enlisted transaction is open. Arming an open
/// enlisted transaction and calling the checkpoint directly is exactly the
/// interleaving the background thread cannot be made to produce
/// deterministically.
#[tokio::test]
async fn tagged_checkpoints_decline_while_an_enlisted_transaction_is_open() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;
    exec(&ex, "SELECT STREAM_XADD('gate', 'k', 'v')").await;

    // An open transaction enlisted on KV (any model trips the gate).
    exec(&ex, "BEGIN").await;
    exec(&ex, "SELECT KV_SET('held', 'uncommitted')").await;
    assert!(ex.any_open_enlisted_txn(), "the fixture must hold the gate");

    let declined = ex
        .checkpoint_streams_wal()
        .expect_err("the streams checkpoint must decline under an open enlisted txn");
    assert_eq!(
        declined.kind(),
        std::io::ErrorKind::WouldBlock,
        "declines must be distinguishable from IO failures: {declined}"
    );
    for (name, attempt) in [
        ("KV", ex.checkpoint_kv_wal()),
        ("document", ex.checkpoint_doc_wal()),
        ("graph", ex.checkpoint_graph_wal()),
        ("timeseries", ex.checkpoint_ts_wal()),
        ("datalog", ex.checkpoint_datalog_wal()),
        ("columnar", ex.checkpoint_columnar_wal()),
    ] {
        let Err(e) = attempt else {
            panic!("the {name} checkpoint must decline under an open enlisted txn")
        };
        assert_eq!(
            e.kind(),
            std::io::ErrorKind::WouldBlock,
            "the {name} decline must be distinguishable from IO failures: {e}"
        );
    }

    // COMMIT releases the gate; the same checkpoints now fold.
    exec(&ex, "COMMIT").await;
    assert!(!ex.any_open_enlisted_txn(), "COMMIT releases the gate");
    ex.checkpoint_streams_wal()
        .expect("with no enlisted txn the streams checkpoint proceeds");
    ex.checkpoint_kv_wal()
        .expect("with no enlisted txn the KV checkpoint proceeds");
    ex.checkpoint_doc_wal()
        .expect("with no enlisted txn the document checkpoint proceeds");
    ex.checkpoint_graph_wal()
        .expect("with no enlisted txn the graph checkpoint proceeds");
    ex.checkpoint_ts_wal()
        .expect("with no enlisted txn the timeseries checkpoint proceeds");
    ex.checkpoint_datalog_wal()
        .expect("with no enlisted txn the datalog checkpoint proceeds");
    ex.checkpoint_columnar_wal()
        .expect("with no enlisted txn the columnar checkpoint proceeds");
}

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 5: the timeseries WAL (ts_wal.bin)
// ══════════════════════════════════════════════════════════════════════════

/// TS_COUNT as i64.
async fn ts_count(ex: &Executor, series: &str) -> i64 {
    let r = exec(ex, &format!("SELECT TS_COUNT('{series}')")).await;
    match scalar(&r[0]) {
        Value::Int64(n) => *n,
        other => panic!("TS_COUNT returned {other:?}"),
    }
}

/// The S63 discard direction for timeseries: dropping the executor
/// mid-transaction is the durable equivalent of dying before the COMMIT
/// record — the ts_wal record was flushed by its statement, and nothing
/// vouches for its id. No compensation runs (the transaction is abandoned,
/// not rolled back), so the only thing standing between the flushed tagged
/// record and recovery is the filter. The committed transaction's points in
/// the SAME log survive — the both-directions proof.
#[tokio::test]
async fn uncommitted_ts_insert_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + ts point, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        exec(&ex, "SELECT TS_INSERT('kept_s', 1000, 1.5)").await;
        exec(&ex, "COMMIT").await;
        // Abandoned: record flushed to ts_wal.bin, COMMIT never happens. No
        // rollback compensation runs — the filter alone must discard it.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        exec(&ex, "SELECT TS_INSERT('lost_s', 2000, 2.5)").await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        ts_count(&ex, "kept_s").await,
        1,
        "the committed transaction's point is vouched for and must survive"
    );
    assert_eq!(
        ts_count(&ex, "lost_s").await,
        0,
        "the abandoned transaction's ts record was flushed to ts_wal.bin and \
         must be discarded by the filter, not replayed"
    );
    let rows = exec(&ex, "SELECT v FROM t ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "the abandoned transaction's SQL row is gone too"
    );
    assert_eq!(scalar(&rows[0]), &Value::Text("kept".into()));
}

/// Autocommit TS_INSERTs carry XACT_AUTOCOMMIT (0) and never need a commit
/// record — their durability point is the ts log's own fsync.
#[tokio::test]
async fn autocommit_ts_insert_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT TS_INSERT('auto_s', 1000, 1.0)").await;
        exec(&ex, "SELECT TS_INSERT('auto_s', 2000, 2.0)").await;
        assert_eq!(ts_count(&ex, "auto_s").await, 2);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        ts_count(&ex, "auto_s").await,
        2,
        "autocommit records carry id 0 and must never be filtered"
    );
}

/// A rolled-back transaction's tagged ts records are discarded by the
/// filter on replay. The rollback's compensating records ALSO handle this
/// (double protection, deliberately kept — see D4), so the outcome here is
/// asserted rather than discriminated; the filter-only discriminator is the
/// abandoned-transaction test above, where no compensation ever runs.
#[tokio::test]
async fn rolled_back_ts_inserts_are_gone_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT TS_INSERT('pre_s', 1000, 1.0)").await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT TS_INSERT('rb_s', 2000, 2.0)").await;
        exec(&ex, "SELECT TS_INSERT('pre_s', 3000, 3.0)").await;
        exec(&ex, "ROLLBACK").await;
        assert_eq!(ts_count(&ex, "pre_s").await, 1);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        ts_count(&ex, "rb_s").await,
        0,
        "the rolled-back transaction's point must not resurrect on reopen"
    );
    assert_eq!(
        ts_count(&ex, "pre_s").await,
        1,
        "the overwritten series' before-image must be what replay restores"
    );
}

/// The ts half of the id-monotonicity proof (S1a/D2): after a run whose
/// only surviving tagged record lives in ts_wal.bin, a reopened executor
/// must still mint ids ABOVE the id that record carries — otherwise the
/// NEXT committed transaction would vouch for the abandoned record's id and
/// resurrect it.
#[tokio::test]
async fn ts_tagged_ids_do_not_reuse_when_only_ts_wal_holds_the_floor() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: an abandoned transaction tags a ts record with id 1. Nothing
    // commits, so no COMMIT body exists anywhere afterwards.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT TS_INSERT('abandoned_s', 1000, 1.0)").await;
        // abandon — no COMMIT, no ROLLBACK, no compensation
    }

    // Run 2: reopen. Only ts_wal.bin's max tagged id (1) can hold the floor
    // above 1.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        assert_eq!(
            ts_count(&ex, "abandoned_s").await,
            0,
            "the abandoned record must be discarded on replay"
        );
        let next = ex.next_xact_id_probe();
        assert!(
            next > 1,
            "id reuse: the next minted id is {next}, but ts_wal.bin still holds \
             a tagged record carrying id 1 — a fresh transaction with that id \
             would resurrect it"
        );
        // Prove the resurrection the seed prevents: commit a NEW ts write.
        // Its id is above 1, so vouching for it must not vouch for the
        // abandoned record.
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT TS_INSERT('live_s', 1000, 1.0)").await;
        exec(&ex, "COMMIT").await;
    }

    // Run 3: the live point survives, the abandoned one stays dead.
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(ts_count(&ex, "live_s").await, 1);
    assert_eq!(
        ts_count(&ex, "abandoned_s").await,
        0,
        "the committed id must not vouch for the stale tagged record"
    );
}

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 6: the datalog WAL (datalog.wal)
// ══════════════════════════════════════════════════════════════════════════

/// Whether the exact fact literal `fact` (e.g. `parent(a, b)`) is present,
/// via DATALOG_QUERY's JSON rows.
async fn dl_has_fact(ex: &Executor, fact: &str) -> bool {
    let r = exec(ex, &format!("SELECT DATALOG_QUERY('{fact}')")).await;
    let json = text_of(r.into_iter().next().unwrap());
    let v: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or_else(|e| panic!("DATALOG_QUERY returned unparseable {json:?}: {e}"));
    !v.as_array().is_some_and(|a| a.is_empty())
}

/// The S63 discard direction for datalog: dropping the executor
/// mid-transaction is the durable equivalent of dying before the COMMIT
/// record — the datalog.wal record was flushed by its statement, and
/// nothing vouches for its id. No compensation runs (the transaction is
/// abandoned, not rolled back), so the only thing standing between the
/// flushed tagged record and recovery is the filter. The committed
/// transaction's fact in the SAME log survives — the both-directions proof.
#[tokio::test]
async fn uncommitted_datalog_assert_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + fact, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        exec(&ex, "SELECT DATALOG_ASSERT('kept_fact(a, b).')").await;
        exec(&ex, "COMMIT").await;
        // Abandoned: record flushed to datalog.wal, COMMIT never happens. No
        // rollback compensation runs — the filter alone must discard it.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        exec(&ex, "SELECT DATALOG_ASSERT('lost_fact(a, b).')").await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert!(
        dl_has_fact(&ex, "kept_fact(a, b)").await,
        "the committed transaction's fact is vouched for and must survive"
    );
    assert!(
        !dl_has_fact(&ex, "lost_fact(a, b)").await,
        "the abandoned transaction's datalog record was flushed to datalog.wal \
         and must be discarded by the filter, not replayed"
    );
    let rows = exec(&ex, "SELECT v FROM t ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "the abandoned transaction's SQL row is gone too"
    );
    assert_eq!(scalar(&rows[0]), &Value::Text("kept".into()));
}

/// Autocommit DATALOG_ASSERTs carry XACT_AUTOCOMMIT (0) and never need a
/// commit record — their durability point is the datalog log's own fsync.
#[tokio::test]
async fn autocommit_datalog_assert_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT DATALOG_ASSERT('auto_fact(a, b).')").await;
        assert!(dl_has_fact(&ex, "auto_fact(a, b)").await);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert!(
        dl_has_fact(&ex, "auto_fact(a, b)").await,
        "autocommit records carry id 0 and must never be filtered"
    );
}

/// A rolled-back transaction's tagged datalog records are discarded by the
/// filter on replay. The rollback's WAL-checkpoint compensation ALSO handles
/// this (double protection, deliberately kept — see D4), so the outcome here
/// is asserted rather than discriminated; the filter-only discriminator is
/// the abandoned-transaction test above, where no compensation ever runs.
#[tokio::test]
async fn rolled_back_datalog_asserts_are_gone_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT DATALOG_ASSERT('pre_fact(a, b).')").await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT DATALOG_ASSERT('rb_fact(a, b).')").await;
        exec(&ex, "SELECT DATALOG_RETRACT('pre_fact(a, b).')").await;
        exec(&ex, "ROLLBACK").await;
        assert!(dl_has_fact(&ex, "pre_fact(a, b)").await);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert!(
        !dl_has_fact(&ex, "rb_fact(a, b)").await,
        "the rolled-back transaction's fact must not resurrect on reopen"
    );
    assert!(
        dl_has_fact(&ex, "pre_fact(a, b)").await,
        "the retracted fact's before-image must be what replay restores"
    );
}

/// The datalog half of the id-monotonicity proof (S1a/D2).
#[tokio::test]
async fn datalog_tagged_ids_do_not_reuse_when_only_datalog_wal_holds_the_floor() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: an abandoned transaction tags a datalog record with id 1.
    // Nothing commits, so no COMMIT body exists anywhere afterwards.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT DATALOG_ASSERT('abandoned_fact(a, b).')").await;
        // abandon — no COMMIT, no ROLLBACK, no compensation
    }

    // Run 2: reopen. Only datalog.wal's max tagged id (1) can hold the
    // floor above 1.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        assert!(
            !dl_has_fact(&ex, "abandoned_fact(a, b)").await,
            "the abandoned record must be discarded on replay"
        );
        let next = ex.next_xact_id_probe();
        assert!(
            next > 1,
            "id reuse: the next minted id is {next}, but datalog.wal still holds \
             a tagged record carrying id 1 — a fresh transaction with that id \
             would resurrect it"
        );
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT DATALOG_ASSERT('live_fact(a, b).')").await;
        exec(&ex, "COMMIT").await;
    }

    // Run 3: the live fact survives, the abandoned one stays dead.
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert!(dl_has_fact(&ex, "live_fact(a, b)").await);
    assert!(
        !dl_has_fact(&ex, "abandoned_fact(a, b)").await,
        "the committed id must not vouch for the stale tagged record"
    );
}

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 7: the columnar model WAL (columnar.wal)
// ══════════════════════════════════════════════════════════════════════════

/// COLUMNAR_COUNT as i64.
async fn columnar_count(ex: &Executor, table: &str) -> i64 {
    let r = exec(ex, &format!("SELECT COLUMNAR_COUNT('{table}')")).await;
    match scalar(&r[0]) {
        Value::Int64(n) => *n,
        other => panic!("COLUMNAR_COUNT returned {other:?}"),
    }
}

/// Why there is no uncommitted-discard test for columnar here: M8's
/// fail-loud boundary REFUSES `COLUMNAR_INSERT` inside an explicit
/// transaction (the store has no rollback before-image — see
/// `refused_in_transaction`), so the uncommitted-columnar-record shape
/// this slice exists to make safe cannot be produced through SQL in the
/// first place. The crash window is empty by construction, and the
/// filter that would close it if the boundary is ever lifted is proven at
/// the WAL level (`storage::columnar_wal::tests::
/// tagged_records_filter_on_the_committed_set`).
#[tokio::test]
async fn columnar_insert_is_refused_inside_a_transaction_the_m8_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;

    exec(&ex, "BEGIN").await;
    let err = ex
        .execute("SELECT COLUMNAR_INSERT('refused_t', 'v', 1)")
        .await
        .expect_err("the unrevertable columnar write must be refused inside a transaction");
    assert!(
        err.to_string().contains("rollback"),
        "the refusal must name the reason, got {err}"
    );
    exec(&ex, "ROLLBACK").await;

    // Nothing partial was written: the table holds no rows live, and the
    // log replays to the same empty state across a reopen.
    assert_eq!(columnar_count(&ex, "refused_t").await, 0);
    drop(ex);
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        columnar_count(&ex, "refused_t").await,
        0,
        "no record from the refused statement may reach recovery"
    );
}

/// Autocommit COLUMNAR_INSERTs carry XACT_AUTOCOMMIT (0) and never need a
/// commit record — their durability point is the columnar log's own fsync.
#[tokio::test]
async fn autocommit_columnar_insert_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT COLUMNAR_INSERT('auto_t', 'v', 1)").await;
        exec(&ex, "SELECT COLUMNAR_INSERT('auto_t', 'v', 2)").await;
        assert_eq!(columnar_count(&ex, "auto_t").await, 2);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        columnar_count(&ex, "auto_t").await,
        2,
        "autocommit records carry id 0 and must never be filtered"
    );
}

// A rolled-back transaction's tagged columnar records would be discarded
// by the filter on replay — but the shape cannot be produced through SQL:
// the M8 boundary refuses the write inside the transaction
// (`columnar_insert_is_refused_inside_a_transaction_the_m8_boundary`
// above), and the WAL-level filter test covers the discard itself.
// `every_mutating_function_is_enlisted_refused_or_declared` in
// `test_specialty_surface_guard` is what enforces that the refusal stays.

// ══════════════════════════════════════════════════════════════════════════
// S63 slice 9: the blob store WAL (blob.wal)
// ══════════════════════════════════════════════════════════════════════════

/// BLOB_GET as hex `Option<String>`: `None` means the blob is absent.
async fn blob_get(ex: &Executor, key: &str) -> Option<String> {
    let r = exec(ex, &format!("SELECT BLOB_GET('{key}')")).await;
    match scalar(&r[0]) {
        Value::Text(s) => Some(s.clone()),
        Value::Null => None,
        other => panic!("BLOB_GET({key}) returned {other:?}"),
    }
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The S63 discard direction for blob: dropping the executor
/// mid-transaction is the durable equivalent of dying before the COMMIT
/// record — the blob manifest was flushed by its statement, and nothing
/// vouches for its id. No compensation runs (the transaction is abandoned,
/// not rolled back), so the only thing standing between the flushed tagged
/// record and recovery is the filter. The committed transaction's blob in
/// the SAME log survives — the both-directions proof.
#[tokio::test]
async fn uncommitted_blob_put_is_discarded_and_committed_kept_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        // Committed: SQL row + blob manifest, both tagged with the txn's id,
        // vouched for by the COMMIT record body.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (1, 'kept')").await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('kept_b', '{}')", hex("kept")),
        )
        .await;
        exec(&ex, "COMMIT").await;
        // Abandoned: manifest flushed to blob.wal, COMMIT never happens. No
        // rollback compensation runs — the filter alone must discard it.
        exec(&ex, "BEGIN").await;
        exec(&ex, "INSERT INTO t (id, v) VALUES (2, 'lost')").await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('lost_b', '{}')", hex("lost")),
        )
        .await;
        // no COMMIT, no ROLLBACK — drop(ex) below abandons it
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        blob_get(&ex, "kept_b").await,
        Some(hex("kept")),
        "the committed transaction's blob is vouched for and must survive"
    );
    assert_eq!(
        blob_get(&ex, "lost_b").await,
        None,
        "the abandoned transaction's blob record was flushed to blob.wal and \
         must be discarded by the filter, not replayed"
    );
    let rows = exec(&ex, "SELECT v FROM t ORDER BY id").await;
    assert_eq!(
        rows.len(),
        1,
        "the abandoned transaction's SQL row is gone too"
    );
    assert_eq!(scalar(&rows[0]), &Value::Text("kept".into()));
}

/// Autocommit BLOB_STOREs carry XACT_AUTOCOMMIT (0) and never need a commit
/// record — their durability point is the blob log's own fsync.
#[tokio::test]
async fn autocommit_blob_put_survives_reopen_without_a_commit_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('auto_b', '{}')", hex("one")),
        )
        .await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('auto_b', '{}')", hex("two")),
        )
        .await;
        assert_eq!(blob_get(&ex, "auto_b").await, Some(hex("two")));
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        blob_get(&ex, "auto_b").await,
        Some(hex("two")),
        "autocommit records carry id 0 and must never be filtered"
    );
}

/// A rolled-back transaction's tagged blob records are discarded by the
/// filter on replay, and the rollback's compensating records (written
/// XACT_AUTOCOMMIT) restore the before-image. The compensations handle this
/// directly; the outcome is asserted rather than discriminated — the
/// filter-only discriminator is the abandoned-transaction test above.
#[tokio::test]
async fn rolled_back_blob_puts_are_gone_after_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('pre_b', '{}')", hex("pre")),
        )
        .await;
        exec(&ex, "BEGIN").await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('pre_b', '{}')", hex("clobbered")),
        )
        .await;
        exec(&ex, &format!("SELECT BLOB_STORE('rb_b', '{}')", hex("rb"))).await;
        exec(&ex, "ROLLBACK").await;
        assert_eq!(blob_get(&ex, "pre_b").await, Some(hex("pre")));
        assert_eq!(blob_get(&ex, "rb_b").await, None);
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(
        blob_get(&ex, "rb_b").await,
        None,
        "the rolled-back transaction's blob must not resurrect on reopen"
    );
    assert_eq!(
        blob_get(&ex, "pre_b").await,
        Some(hex("pre")),
        "the overwritten blob's before-image must be what replay restores"
    );
}

/// The blob half of the id-monotonicity proof (S1a/D2): after a run whose
/// only surviving tagged record lives in blob.wal, a reopened executor must
/// still mint ids ABOVE the id that record carries — otherwise the NEXT
/// committed transaction would vouch for the abandoned record's id and
/// resurrect it.
#[tokio::test]
async fn blob_tagged_ids_do_not_reuse_when_only_blob_wal_holds_the_floor() {
    let dir = tempfile::tempdir().unwrap();

    // Run 1: an abandoned transaction tags a blob record with id 1. Nothing
    // commits, so no COMMIT body exists anywhere afterwards.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "BEGIN").await;
        exec(
            &ex,
            &format!("SELECT BLOB_STORE('abandoned_b', '{}')", hex("x")),
        )
        .await;
        // abandon — no COMMIT, no ROLLBACK, no compensation
    }

    // Run 2: reopen. Only blob.wal's max tagged id (1) can hold the floor
    // above 1.
    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        assert_eq!(
            blob_get(&ex, "abandoned_b").await,
            None,
            "the abandoned record must be discarded on replay"
        );
        let next = ex.next_xact_id_probe();
        assert!(
            next > 1,
            "id reuse: the next minted id is {next}, but blob.wal still holds \
             a tagged record carrying id 1 — a fresh transaction with that id \
             would resurrect it"
        );
        // Prove the resurrection the seed prevents: commit a NEW blob write.
        // Its id is above 1, so vouching for it must not vouch for the
        // abandoned record.
        exec(&ex, "BEGIN").await;
        exec(&ex, &format!("SELECT BLOB_STORE('live_b', '{}')", hex("y"))).await;
        exec(&ex, "COMMIT").await;
    }

    // Run 3: the live blob survives, the abandoned one stays dead.
    let (ex, _engine) = open_segmented(dir.path()).await;
    assert_eq!(blob_get(&ex, "live_b").await, Some(hex("y")));
    assert_eq!(
        blob_get(&ex, "abandoned_b").await,
        None,
        "the committed id must not vouch for the stale tagged record"
    );
}

/// Blob joined the tagged logs' S7 gate with this slice (it was
/// warn-and-continue before): a blob-enlisted transaction open at checkpoint
/// time declines the blob WAL fold, because a SNAPSHOT_META carries no
/// transaction id and would bake uncommitted manifests into a state the
/// recovery filter cannot discard.
#[tokio::test]
async fn blob_enlistment_trips_the_s7_gate_on_its_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;

    exec(&ex, "BEGIN").await;
    exec(
        &ex,
        &format!("SELECT BLOB_STORE('gated_b', '{}')", hex("x")),
    )
    .await;
    let err = ex
        .checkpoint_blob_wal()
        .expect_err("an open blob-enlisted transaction must decline the blob fold");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::WouldBlock,
        "the decline is the S7 re-check, got {err}"
    );
    exec(&ex, "COMMIT").await;
    ex.checkpoint_blob_wal()
        .expect("with the transaction closed the fold proceeds");
}

// ══════════════════════════════════════════════════════════════════════════
// S63: the KV collections WAL (collections.wal) — plumbed, boundary-held
// ══════════════════════════════════════════════════════════════════════════

/// Why there is no uncommitted-discard test for collections here: M8's
/// fail-loud boundary REFUSES every KV collection mutator inside an
/// explicit transaction (the store has no rollback before-image — see
/// `refused_in_transaction`), so the uncommitted-collections-record shape
/// cannot be produced through SQL in the first place. The crash window is
/// empty by construction, and the filter that would close it if the
/// boundary is ever lifted is proven at the WAL level
/// (`kv::collections_wal::tests::tagged_records_filter_on_the_committed_set`).
/// Lifting the boundary additionally requires a before-image design and a
/// race-free tag attribution (the tag is process-global — see
/// `CollectionWal::set_xact_tag`); that is the escalation, not this file.
///
/// What IS asserted here: the plumbing is live end to end — autocommit
/// collection writes now leave TAGGED records (id 0), and those replay
/// across the reopen exactly as the untagged ones always did.
#[tokio::test]
async fn autocommit_collections_writes_leave_tagged_records_that_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let (ex, _engine) = open_segmented(dir.path()).await;
        exec(&ex, "SELECT KV_HSET('h', 'f1', 'v1')").await;
        exec(&ex, "SELECT KV_LPUSH('l', 'head')").await;
        exec(&ex, "SELECT KV_ZADD('z', 3.5, 'm')").await;
        exec(&ex, "SELECT KV_HSET('h', 'f2', 'v2')").await;
    }

    let (ex, _engine) = open_segmented(dir.path()).await;
    let r = exec(&ex, "SELECT KV_HGET('h', 'f1')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("v1".into()));
    let r = exec(&ex, "SELECT KV_HGET('h', 'f2')").await;
    assert_eq!(
        scalar(&r[0]),
        &Value::Text("v2".into()),
        "tagged records replay in order — the second HSET must win"
    );
    let r = exec(&ex, "SELECT KV_ZRANGE('z', 0, -1)").await;
    let zjson = text_of(r.into_iter().next().unwrap());
    assert!(
        zjson.contains("\"m\""),
        "the tagged ZADD must replay: got {zjson}"
    );
    assert!(
        zjson.contains("3.5"),
        "the tagged ZADD's score must replay: got {zjson}"
    );
    let r = exec(&ex, "SELECT KV_LLEN('l')").await;
    match scalar(&r[0]) {
        Value::Int64(n) => assert_eq!(*n, 1, "the tagged LPUSH must replay"),
        other => panic!("KV_LLEN returned {other:?}"),
    }
}

/// The refusal itself is the boundary that makes the slice honest; keep a
/// witness here beside the columnar one, so removing the guard shows up as
/// a failing test in this file and not only in the surface-guard audit.
#[tokio::test]
async fn collections_mutators_are_refused_inside_a_transaction_the_m8_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (ex, _engine) = open_segmented(dir.path()).await;

    exec(&ex, "BEGIN").await;
    let err = ex
        .execute("SELECT KV_HSET('refused_h', 'f', 'v')")
        .await
        .expect_err("the unrevertable collections write must be refused inside a transaction");
    assert!(
        err.to_string().contains("rollback"),
        "the refusal must name the reason, got {err}"
    );
    exec(&ex, "ROLLBACK").await;

    // Nothing partial was written: the hash holds nothing live, and the
    // collections log replays to the same empty state across a reopen.
    let r = exec(&ex, "SELECT KV_HGET('refused_h', 'f')").await;
    assert_eq!(*scalar(&r[0]), Value::Null);
    drop(ex);
    let (ex, _engine) = open_segmented(dir.path()).await;
    let r = exec(&ex, "SELECT KV_HGET('refused_h', 'f')").await;
    assert_eq!(
        *scalar(&r[0]),
        Value::Null,
        "no record from the refused statement may reach recovery"
    );
}

// ══════════════════════════════════════════════════════════════════════════
// Slice 8: geo — documentation-only (see src/geo/wal.rs's header)
// ══════════════════════════════════════════════════════════════════════════
