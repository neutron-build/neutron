//! Streams durability across a restart: rollback compensation (S31-04) and
//! consumer-group persistence (S31-05).
//!
//! A restart here is the real one — drop the `Executor` and open a new one from
//! the same directory, which replays `<dir>/streams/streams.wal` exactly as
//! `main.rs` does. Every test in this file must cross that boundary: the two
//! defects it covers both passed every in-memory test there was, because the
//! in-memory state was correct and only the log disagreed with it.

use std::path::Path;
use std::sync::Arc;

use super::super::Executor;
use super::{exec, scalar};
use crate::catalog::Catalog;
use crate::storage::persistence::CatalogPersistence;
use crate::storage::{DiskEngine, StorageEngine};
use crate::types::Value;

/// Open (or reopen) a DiskEngine-backed executor from `dir`, the way `main.rs`
/// does. `new_with_persistence` is what opens the streams WAL and replays it.
async fn open_executor(dir: &Path) -> Executor {
    let catalog_path = dir.join("catalog.json");
    let db_path = dir.join("nucleus.db");
    let catalog = Arc::new(Catalog::new());

    let cp = CatalogPersistence::new(&catalog_path);
    cp.load_catalog(&catalog).await.ok();

    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);

    let ex = Executor::new_with_persistence(catalog, storage, Some(catalog_path), Some(dir));
    ex.load_meta().await;
    ex
}

async fn xlen(ex: &Executor, stream: &str) -> i64 {
    let r = exec(ex, &format!("SELECT STREAM_XLEN('{stream}')")).await;
    match scalar(&r[0]) {
        Value::Int64(n) => *n,
        other => panic!("STREAM_XLEN returned {other:?}"),
    }
}

async fn text(ex: &Executor, sql: &str) -> String {
    let r = exec(ex, sql).await;
    match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("{sql} returned {other:?}"),
    }
}

// ── S31-04: ROLLBACK must not resurrect the entry on restart ─────────────────

/// `STREAM_XADD` appends to the WAL inside the transaction and `log_xadd` ends
/// in `write_all` + `flush`, so the record is in the kernel before `ROLLBACK` is
/// even parsed. Reverting only the in-memory map left it there, and the next
/// restart replayed it: the aborted entry read back as absent and then came
/// back. Nothing on the shutdown path checkpoints this log — only the 300 s
/// timer does — so a *graceful* restart was enough to resurrect it.
#[tokio::test]
async fn rolled_back_stream_write_stays_gone_across_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('orders', 'id', 'committed')").await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT STREAM_XADD('orders', 'id', 'aborted')").await;
        exec(&ex, "ROLLBACK").await;
        assert_eq!(
            xlen(&ex, "orders").await,
            1,
            "the aborted entry must be invisible immediately (this part already worked)"
        );
    }

    let ex = open_executor(dir.path()).await;
    assert_eq!(
        xlen(&ex, "orders").await,
        1,
        "a rolled-back XADD must not come back on restart: publishing an event that never \
         happened is worse than losing a write, because consumers may already have acted on it"
    );
    let sweep = text(
        &ex,
        &format!("SELECT STREAM_XRANGE('orders', 0, {}, 10)", u64::MAX / 2),
    )
    .await;
    assert!(
        sweep.contains("committed"),
        "the committed entry must survive: {sweep}"
    );
    assert!(
        !sweep.contains("aborted"),
        "the rolled-back entry must not be replayed: {sweep}"
    );
}

/// The rollback compensation must be scoped to what the transaction touched:
/// rewriting the log from live state must not drop another stream's writes.
#[tokio::test]
async fn rollback_compensation_preserves_untouched_streams() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('kept', 'k', 'v1')").await;
        exec(&ex, "SELECT STREAM_XADD('kept', 'k', 'v2')").await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT STREAM_XADD('aborted', 'k', 'x')").await;
        exec(&ex, "ROLLBACK").await;
    }

    let ex = open_executor(dir.path()).await;
    assert_eq!(xlen(&ex, "kept").await, 2, "untouched stream must survive");
    assert_eq!(
        xlen(&ex, "aborted").await,
        0,
        "a stream created only inside the aborted transaction must not exist"
    );
}

/// A committed stream write in a transaction must still be there afterwards —
/// otherwise the test above could be satisfied by dropping everything.
#[tokio::test]
async fn committed_stream_write_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "BEGIN").await;
        exec(&ex, "SELECT STREAM_XADD('committed_s', 'k', 'v')").await;
        exec(&ex, "COMMIT").await;
    }

    let ex = open_executor(dir.path()).await;
    assert_eq!(xlen(&ex, "committed_s").await, 1);
}

// ── S31-05: consumer groups, cursors, PEL and acks survive a restart ─────────

/// Before this, group state was never logged or checkpointed: a restart dropped
/// every group, and `STREAM_XREADGROUP` on the vanished group returned an empty
/// batch. Empty is what "caught up" looks like, so the consumer concluded it had
/// nothing to do and silently skipped its entire backlog.
#[tokio::test]
async fn consumer_group_cursor_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('s', 'n', '1')").await;
        exec(&ex, "SELECT STREAM_XADD('s', 'n', '2')").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('s', 'g', 0)").await;
        let first = text(&ex, "SELECT STREAM_XREADGROUP('s', 'g', 'c1', 10)").await;
        assert!(
            first.contains("\"1\"") && first.contains("\"2\""),
            "{first}"
        );
        // Two more entries land after the consumer went away.
        exec(&ex, "SELECT STREAM_XADD('s', 'n', '3')").await;
    }

    let ex = open_executor(dir.path()).await;
    assert_eq!(xlen(&ex, "s").await, 3, "entries survive (they always did)");
    let after = text(&ex, "SELECT STREAM_XREADGROUP('s', 'g', 'c1', 10)").await;
    assert!(
        after.contains("\"3\""),
        "the entry appended after the last delivery must be served: {after}"
    );
    assert!(
        !after.contains("\"1\"") && !after.contains("\"2\""),
        "already-delivered entries must not be redelivered — a lost cursor redelivers the \
         whole backlog: {after}"
    );
}

/// The pending list is the part a client cannot rebuild: an entry delivered and
/// not acknowledged must still be pending after a restart, and one that WAS
/// acknowledged must not come back to the PEL.
#[tokio::test]
async fn pending_list_and_acks_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let delivered;

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('p', 'n', 'a')").await;
        exec(&ex, "SELECT STREAM_XADD('p', 'n', 'b')").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('p', 'g', 0)").await;
        delivered = text(&ex, "SELECT STREAM_XREADGROUP('p', 'g', 'worker', 10)").await;
        // Acknowledge exactly the first of the two delivered ids.
        let first_id = delivered
            .split("\"id\":\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .expect("XREADGROUP returns ids")
            .to_string();
        let acked = exec(&ex, &format!("SELECT STREAM_XACK('p', 'g', '{first_id}')")).await;
        assert_eq!(scalar(&acked[0]), &Value::Int64(1));
    }

    let ex = open_executor(dir.path()).await;
    // There is no `STREAM_XPENDING` on the SQL surface, so read the recovered
    // PEL directly out of the rebuilt stream.
    let pending = {
        let streams = ex.streams.read();
        streams["p"].xpending("g")
    };
    assert_eq!(
        pending,
        vec![("worker".to_string(), 1usize)],
        "exactly one of the two delivered entries stays pending after a restart: the \
         unacknowledged one must still be claimed, and the acknowledged one must not come back"
    );
    // And the cursor is still past both, so neither is redelivered.
    let after = text(&ex, "SELECT STREAM_XREADGROUP('p', 'g', 'worker', 10)").await;
    assert_eq!(after, "[]", "both entries were already delivered: {after}");
}

/// A group created on an empty stream must survive too — group state is keyed
/// independently of whether the stream has entries.
#[tokio::test]
async fn group_on_empty_stream_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('empty_s', 'g', 0)").await;
    }

    let ex = open_executor(dir.path()).await;
    // Must not be NOGROUP: the group exists, it simply has nothing to deliver.
    let r = exec(&ex, "SELECT STREAM_XREADGROUP('empty_s', 'g', 'c', 10)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("[]".into()));
}

/// Group state must survive a checkpoint as well as a tail replay. A checkpoint
/// rewrites the log from live memory, so a snapshot that dropped groups would
/// silently un-persist every record `log_xgroup_create` had just written — the
/// sharper half of S31-05, which failed even *with* a successful checkpoint.
#[tokio::test]
async fn consumer_group_survives_checkpoint_then_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('c', 'n', '1')").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('c', 'g', 0)").await;
        let _ = text(&ex, "SELECT STREAM_XREADGROUP('c', 'g', 'c1', 10)").await;
        ex.checkpoint_streams_wal()
            .expect("checkpoint the streams WAL");
        exec(&ex, "SELECT STREAM_XADD('c', 'n', '2')").await;
    }

    let ex = open_executor(dir.path()).await;
    assert_eq!(xlen(&ex, "c").await, 2);
    let after = text(&ex, "SELECT STREAM_XREADGROUP('c', 'g', 'c1', 10)").await;
    assert!(
        after.contains("\"2\"") && !after.contains("\"1\""),
        "the cursor recorded in the snapshot must be honoured: {after}"
    );
}

// ── S31-05: a missing group must not read as "caught up" ────────────────────

#[tokio::test]
async fn xreadgroup_on_missing_group_is_an_error_not_an_empty_read() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(&ex, "SELECT STREAM_XADD('n', 'k', 'v')").await;

    let err = ex
        .execute("SELECT STREAM_XREADGROUP('n', 'nosuch', 'c', 10)")
        .await
        .expect_err("a read against a group that does not exist must not succeed");
    assert!(
        err.to_string().contains("NOGROUP"),
        "expected NOGROUP, got {err}"
    );

    // Same for a stream that does not exist at all.
    let err = ex
        .execute("SELECT STREAM_XREADGROUP('nostream', 'g', 'c', 10)")
        .await
        .expect_err("a read against a stream that does not exist must not succeed");
    assert!(
        err.to_string().contains("NOGROUP"),
        "expected NOGROUP, got {err}"
    );

    // And an existing group with nothing new is still an ordinary empty read,
    // so the error above cannot be satisfied by failing everything.
    exec(&ex, "SELECT STREAM_XGROUP_CREATE('n', 'g', 0)").await;
    let _ = text(&ex, "SELECT STREAM_XREADGROUP('n', 'g', 'c', 10)").await;
    let r = exec(&ex, "SELECT STREAM_XREADGROUP('n', 'g', 'c', 10)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("[]".into()));
}

// ── S31-11: a create must not destroy a live consumer group ─────────────────

/// `STREAM_XGROUP_CREATE` used to be an unconditional overwrite that reset the
/// cursor, dropped the pending list, and returned `true`. The sibling
/// implementation reachable over RESP (`kv::streams::Stream::xgroup_create`)
/// has always returned `BUSYGROUP`, and so does Redis; the two now agree.
///
/// The damage is a re-run of ordinary idempotent startup code — a migration, a
/// service that creates its group on boot, a replayed provisioning script —
/// silently redelivering the whole stream (`start_id = 0`) or silently
/// abandoning everything unacked (`start_id = now`).
#[tokio::test]
async fn xgroup_create_on_an_existing_group_is_busygroup_not_a_silent_reset() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    for n in ["1", "2", "3"] {
        exec(&ex, &format!("SELECT STREAM_XADD('bg', 'n', '{n}')")).await;
    }
    exec(&ex, "SELECT STREAM_XGROUP_CREATE('bg', 'g', 0)").await;
    let first = text(&ex, "SELECT STREAM_XREADGROUP('bg', 'g', 'c', 10)").await;
    assert!(
        first.contains("\"1\"") && first.contains("\"3\""),
        "{first}"
    );

    let err = ex
        .execute("SELECT STREAM_XGROUP_CREATE('bg', 'g', 0)")
        .await
        .expect_err("re-creating a live consumer group must not succeed");
    assert!(
        err.to_string().contains("BUSYGROUP"),
        "expected BUSYGROUP, got {err}"
    );

    // The cursor and the pending list are intact: nothing is redelivered.
    let r = exec(&ex, "SELECT STREAM_XREADGROUP('bg', 'g', 'c', 10)").await;
    assert_eq!(
        scalar(&r[0]),
        &Value::Text("[]".into()),
        "a refused create must not have rewound the group"
    );
}

/// Consumer-group state is persisted, so the overwrite destroyed durable state,
/// not just memory. The refusal has to hold against a group recovered from the
/// log as well as one created in this process.
#[tokio::test]
async fn busygroup_holds_for_a_group_recovered_from_the_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('r', 'n', '1')").await;
        exec(&ex, "SELECT STREAM_XGROUP_CREATE('r', 'g', 0)").await;
        let _ = text(&ex, "SELECT STREAM_XREADGROUP('r', 'g', 'c', 10)").await;
        exec(&ex, "SELECT STREAM_XADD('r', 'n', '2')").await;
    }

    let ex = open_executor(dir.path()).await;
    let err = ex
        .execute("SELECT STREAM_XGROUP_CREATE('r', 'g', 0)")
        .await
        .expect_err("a group replayed from the log is still a live group");
    assert!(
        err.to_string().contains("BUSYGROUP"),
        "expected BUSYGROUP, got {err}"
    );
    let after = text(&ex, "SELECT STREAM_XREADGROUP('r', 'g', 'c', 10)").await;
    assert!(
        after.contains("\"2\"") && !after.contains("\"1\""),
        "the recovered cursor must be unchanged by the refused create: {after}"
    );
}

/// Resetting a group is still possible — it just has to be asked for. The
/// fourth argument is the opt-in; without it the default is the safe one.
#[tokio::test]
async fn explicit_recreate_resets_the_group() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(&ex, "SELECT STREAM_XADD('rc', 'n', '1')").await;
    exec(&ex, "SELECT STREAM_XGROUP_CREATE('rc', 'g', 0)").await;
    let first = text(&ex, "SELECT STREAM_XREADGROUP('rc', 'g', 'c', 10)").await;
    assert!(first.contains("\"1\""), "{first}");

    exec(&ex, "SELECT STREAM_XGROUP_CREATE('rc', 'g', 0, true)").await;
    let again = text(&ex, "SELECT STREAM_XREADGROUP('rc', 'g', 'c', 10)").await;
    assert!(
        again.contains("\"1\""),
        "an explicit recreate rewinds the cursor: {again}"
    );

    // `false` is the default, and is still a refusal.
    let err = ex
        .execute("SELECT STREAM_XGROUP_CREATE('rc', 'g', 0, false)")
        .await
        .expect_err("recreate=false must behave exactly like a plain create");
    assert!(
        err.to_string().contains("BUSYGROUP"),
        "expected BUSYGROUP, got {err}"
    );
}

// ── S31-13: an XADD whose WAL record failed must not be acknowledged ─────────

/// `STREAM_XADD` discarded its WAL error with `let _ =` and not even a log
/// line: on a full disk it returned an entry id, the client counted the event
/// as published, and it existed only in memory until the next restart dropped
/// it — with nothing in the log to correlate afterwards.
#[tokio::test]
async fn xadd_fails_the_statement_when_its_wal_record_cannot_be_written() {
    let dir = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(dir.path()).await;
        exec(&ex, "SELECT STREAM_XADD('d', 'n', 'logged')").await;

        ex.streams_wal()
            .expect("the executor opened a streams WAL")
            .set_fail_appends(true);

        let err = ex
            .execute("SELECT STREAM_XADD('d', 'n', 'unloggable')")
            .await
            .expect_err("an XADD whose WAL append failed must not be acknowledged");
        assert!(
            err.to_string().contains("STREAM_XADD could not log"),
            "the error must name the failure, got {err}"
        );

        // The rejected entry is not visible in memory either: a statement that
        // failed must not leave its write behind for consumers to read.
        assert_eq!(
            xlen(&ex, "d").await,
            1,
            "a rejected XADD must not be readable"
        );

        ex.streams_wal().unwrap().set_fail_appends(false);
    }

    // And it is not there after a restart, which is where the old behaviour
    // finally showed itself.
    let ex = open_executor(dir.path()).await;
    assert_eq!(
        xlen(&ex, "d").await,
        1,
        "only the acknowledged entry survives the restart"
    );
    let all = text(&ex, "SELECT STREAM_XRANGE('d', 0, 9999999999999, 100)").await;
    assert!(
        all.contains("logged") && !all.contains("unloggable"),
        "{all}"
    );
}

/// The same rule for the group create two lines below it in the same match arm:
/// a group whose creation record was never written must not be reported as
/// created, because a restart will not produce it.
#[tokio::test]
async fn xgroup_create_fails_when_its_wal_record_cannot_be_written() {
    let dir = tempfile::tempdir().unwrap();
    let ex = open_executor(dir.path()).await;
    exec(&ex, "SELECT STREAM_XADD('gw', 'n', '1')").await;
    ex.streams_wal().unwrap().set_fail_appends(true);

    let err = ex
        .execute("SELECT STREAM_XGROUP_CREATE('gw', 'g', 0)")
        .await
        .expect_err("a group create whose WAL append failed must not be acknowledged");
    assert!(
        err.to_string()
            .contains("STREAM_XGROUP_CREATE could not log"),
        "the error must name the failure, got {err}"
    );

    ex.streams_wal().unwrap().set_fail_appends(false);
    // The group was rolled back, so it is neither present nor BUSYGROUP.
    exec(&ex, "SELECT STREAM_XGROUP_CREATE('gw', 'g', 0)").await;
    let served = text(&ex, "SELECT STREAM_XREADGROUP('gw', 'g', 'c', 10)").await;
    assert!(served.contains("\"1\""), "{served}");
}
