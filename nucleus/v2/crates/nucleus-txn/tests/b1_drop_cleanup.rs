//! C-B1b drop-cleanup tests (§9.2 SD3): the retired-storage row commits,
//! aborts and rolls back with its DDL, the cleanup waits for the durable
//! `W` to pass the snapshot that saw the row, TRUNCATE and DROP INDEX
//! retire their ranges (and the new storage is untouched), and intents
//! inside a retired range go through §7.3 before the `DeleteRange`. The
//! C-B1bfix follow-ups add: DROP TABLE of a table **with an index**
//! retires every noted range (L-1), and the job's `JobSet` wiring — the
//! W-rise wake (L-2) and the shutdown order around an in-flight marker
//! delete (L-3). Each test's doc names the mutant it kills.

mod b1_support;

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use b1_support::{ok, place_write, reopen_rig, tkey, B1Rig, KillKv};
use nucleus_kv::{MemKv, OrderedKv, Snapshot};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{table_range, Catalog, RetiredRow};
use nucleus_txn::commit::{FailStop, SyncCommit, SystemClock};
use nucleus_txn::gc::{GcConfig, GcJob, GcRunner};
use nucleus_txn::jobs::metrics::METRIC_DROP_CLEANUP_PENDING;
use nucleus_txn::jobs::{
    DropCleanup, JobConfig, JobKind, JobOutcome, JobPoint, JobProbe, JobSet, ShutdownStep,
    SHUTDOWN_ORDER,
};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::TxnId;

/// Installs the GC job (the cleanup retires through it).
fn gc<K: nucleus_kv::OrderedKv>(core: &Arc<Core<K>>) -> Arc<GcJob<K>> {
    Arc::new(ok(GcJob::install(core, GcConfig::default())))
}

/// A job config with a `secs`-seconds idle interval.
fn idle(secs: u64) -> JobConfig {
    JobConfig {
        idle_interval: Duration::from_secs(secs),
    }
}

/// Polls `f` every 2 ms until it answers true, failing after
/// `deadline_secs`.
fn wait_until(deadline_secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_secs(deadline_secs),
            "{what} did not happen within {deadline_secs}s"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The visible retired rows at `visible_ts` (the rig-free form of
/// `B1Rig::retired_rows`, for use once the rig's commit thread has been
/// attached to a `JobSet` — a partial move of the rig).
fn retired_rows_now(core: &Core<KillKv<MemKv>>) -> Vec<(u64, RetiredRow)> {
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: TxnId {
            epoch: core.epoch(),
            n: u64::MAX,
        },
        snapshot: core.visible_ts(),
        stmt_seq: 1,
    };
    ok(Catalog::scan_retired(core, &view, &ctx))
}

/// Whether `[lo, hi)` holds no raw entry at all (the rig-free form of
/// `B1Rig::raw_range_empty`; reads pass through the KillKv).
fn raw_empty(kv: &KillKv<MemKv>, lo: &[u8], hi: &[u8]) -> bool {
    let snap = kv.snapshot();
    let empty = snap
        .scan((Bound::Included(lo), Bound::Excluded(hi)), false)
        .next()
        .is_none();
    empty
}

/// The `SHUTDOWN_ORDER` steps the JobSet tests run, in order: the
/// cleanup, the commit thread, then the GC.
fn expected_jobset_log() -> Vec<ShutdownStep> {
    SHUTDOWN_ORDER
        .iter()
        .copied()
        .filter(|s| match s {
            ShutdownStep::Job(k) => matches!(k, JobKind::DropCleanup | JobKind::Gc),
            ShutdownStep::CommitThread => true,
        })
        .collect()
}

/// Advances `visible_ts` by one durable write so the next publish raises
/// `W` past the cleanup's remembered snapshot (a live system's own
/// commits play this part; `W` only passes a snapshot when
/// `visible_ts` does).
fn heartbeat<K: nucleus_kv::OrderedKv>(core: &Arc<Core<K>>, key: &[u8]) {
    let txn = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    place_write(core, &txn, key, b"hb");
    ok(core.commit(txn, SyncCommit::On));
}

/// The `nucleus_drop_cleanup_pending` gauge's value.
fn pending_gauge<K: nucleus_kv::OrderedKv>(core: &Core<K>) -> u64 {
    core.job_metrics()
        .into_iter()
        .find(|m| m.name == METRIC_DROP_CLEANUP_PENDING)
        .map(|m| m.value)
        .unwrap_or(u64::MAX)
}

/// Whether no stored intent remains anywhere (the settle condition): a
/// commit's cleanup enqueue legally lands after its ack (§3 steps 4→5),
/// so the newest commit's intent can still be placed-and-unresolved when
/// its caller returns. An intent is always placed before its commit, so
/// "no intent anywhere" plus an empty resolve queue is exactly the state
/// in which every enqueue has landed and been processed.
fn no_intents<K: nucleus_kv::OrderedKv>(core: &Core<K>) -> bool {
    let view = core.open_view();
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (stored, _) = ok(row);
        if matches!(
            nucleus_txn::encoding::parse_key(&stored),
            Some((_, nucleus_txn::encoding::Entry::Intent))
        ) {
            return false;
        }
    }
    true
}

/// DC-RAW (the retired row written with a raw `core.write` instead of
/// `insert_key`): a DROP's row commits with it (visible after the
/// commit), an abort leaves none (the raw write would survive the
/// abort), and `SAVEPOINT; DROP; ROLLBACK TO; commit` leaves none — the
/// cleanup job then leaves the rolled-back DDL's table data alone.
#[test]
fn retired_row_commits_aborts_and_rolls_back_with_the_ddl() {
    let rig = B1Rig::new();
    let core = &rig.core;

    // Committed DROP: exactly one row, its ranges exactly the noted ones.
    let t0 = rig.rc();
    let (oid_a, sid_a) = rig.create_table(&t0, b"A");
    ok(core.commit(t0, SyncCommit::On));
    let t1 = rig.rc();
    rig.drop_table(&t1, oid_a);
    ok(core.commit(t1, SyncCommit::On));
    let rows = rig.retired_rows();
    assert_eq!(rows.len(), 1, "one retired row per DDL statement: {rows:?}");
    assert_eq!(rows[0].1.ranges, vec![table_range(sid_a)]);
    assert_eq!(rows[0].1.rel_oid, oid_a);

    // Aborted DROP: no row for it (the write path discards the intent).
    let t2 = rig.rc();
    let (oid_b, _sid_b) = rig.create_table(&t2, b"B");
    ok(core.commit(t2, SyncCommit::On));
    let t3 = rig.rc();
    rig.drop_table(&t3, oid_b);
    ok(core.abort(t3));
    ok(Resolver::run_once(core)); // settle the abort cleanup
    let rows = rig.retired_rows();
    assert_eq!(
        rows.len(),
        1,
        "the aborted DROP left no retired row: {rows:?}"
    );
    assert_eq!(rows[0].1.rel_oid, oid_a, "only the committed DROP's row");

    // SAVEPOINT; DROP; ROLLBACK TO; commit: no row, and the table's data
    // survives (the cleanup job leaves it alone).
    let t4 = rig.rc();
    let (oid_c, sid_c) = rig.create_table(&t4, b"C");
    ok(core.commit(t4, SyncCommit::On));
    let t5 = rig.rc();
    let kc = tkey(sid_c, "keep");
    place_write(core, &t5, &kc, b"kept");
    ok(core.commit(t5, SyncCommit::On));
    ok(Resolver::run_once(core));
    let t6 = rig.rc();
    let sp = ok(t6.savepoint());
    rig.drop_table(&t6, oid_c);
    ok(core.rollback_to(&t6, sp));
    ok(core.commit(t6, SyncCommit::On));
    ok(Resolver::run_once(core));
    let rows = rig.retired_rows();
    assert_eq!(
        rows.len(),
        1,
        "the rolled-back DROP left no retired row: {rows:?}"
    );

    // The cleanup job (over the one real row) leaves C's data alone.
    let gc = gc(&rig.core);
    let mut cleanup = DropCleanup::new(Arc::clone(core), Arc::clone(&gc));
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));
    assert_eq!(
        rig.read_at(&kc, core.visible_ts()),
        Some(b"kept".to_vec()),
        "the rolled-back DDL's data is untouched"
    );
    let (lo, hi) = table_range(sid_c);
    assert!(
        !rig.raw_range_empty(&lo, &hi),
        "C's storage still holds data"
    );
    drop(cleanup);
    drop(gc);
}

/// DC-GATE (`retire_storage` called with `Ts::ZERO` — the S-gate gone):
/// a reader snapshot below the DROP holds `W`; the GC and the cleanup
/// delete nothing and the reader still reads its rows. Release, one
/// heartbeat commit, publish, cleanup: the range is raw-empty and the
/// row is gone. With the mutant the first cleanup round retires under
/// the reader's feet and its next read finds nothing.
#[test]
fn cleanup_waits_for_w_to_pass_the_ddl() {
    let rig = B1Rig::new();
    let core = &rig.core;

    let t0 = rig.rc();
    let (oid_a, sid_a) = rig.create_table(&t0, b"A");
    ok(core.commit(t0, SyncCommit::On));
    let t1 = rig.rc();
    let ka = tkey(sid_a, "a0");
    place_write(core, &t1, &ka, b"va");
    ok(core.commit(t1, SyncCommit::On));
    ok(Resolver::run_once(core)); // a clean version under the range

    // A reader snapshot below the upcoming DROP.
    let reader = core.registry.take_snapshot();
    let s_reader = reader.ts();

    let t2 = rig.rc();
    rig.drop_table(&t2, oid_a);
    let ddl_ts = ok(core.commit(t2, SyncCommit::On));
    assert!(ddl_ts > s_reader, "the DROP commits after the reader began");

    let gc = gc(&rig.core);
    ok(gc.publish(0));
    assert!(
        gc.durable_w() <= s_reader,
        "the reader's snapshot pins W at or below {:?}",
        s_reader
    );

    // The cleanup sees the row but the gate holds: nothing is deleted.
    let mut cleanup = DropCleanup::new(Arc::clone(core), Arc::clone(&gc));
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));
    assert_eq!(cleanup.pending(), 1, "the row is seen and pending");
    assert_eq!(pending_gauge(core), 1, "the gauge mirrors `seen.len()`");
    assert_eq!(
        rig.read_at(&ka, s_reader),
        Some(b"va".to_vec()),
        "the reader below the DROP still reads its rows"
    );
    let (lo, hi) = table_range(sid_a);
    assert!(!rig.raw_range_empty(&lo, &hi), "nothing was deleted yet");

    // Release, advance visible_ts past the row's remembered snapshot,
    // publish, cleanup: the range goes raw-empty and the row is gone.
    drop(reader);
    heartbeat(&rig.core, b"/t/1/hb1");
    ok(gc.publish(0));
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Progress));
    assert!(
        rig.raw_range_empty(&lo, &hi),
        "the retired range is raw-empty"
    );
    assert!(rig.retired_rows().is_empty(), "the marker row is deleted");
    assert_eq!(cleanup.pending(), 0);
    assert_eq!(pending_gauge(core), 0);
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));
    drop(cleanup);
    drop(gc);
}

/// DC-NOROW (TRUNCATE writes no row): the old table range and the
/// id-index ranges are cleaned, the new table storage is untouched, and
/// a later DROP INDEX retires its (re-allocated) ranges too.
#[test]
fn truncate_and_drop_index_retire_their_ranges() {
    let rig = B1Rig::new();
    let core = &rig.core;

    let t0 = rig.rc();
    let (oid_t, sid0) = rig.create_table(&t0, b"T");
    ok(core.commit(t0, SyncCommit::On));
    let t1 = rig.rc();
    let (oid_ix, sid_i0) = rig.create_index(&t1, oid_t);
    ok(core.commit(t1, SyncCommit::On));

    // Data under the old table range and both old index ranges
    // (committed, unresolved — the retire path removes them first).
    let t2 = rig.rc();
    let kr = tkey(sid0, "r0");
    let (u_lo, _) = nucleus_txn::catalog::index_ranges(sid_i0)[0].clone();
    let (i_lo, _) = nucleus_txn::catalog::index_ranges(sid_i0)[1].clone();
    let mut ku = u_lo.clone();
    ku.extend_from_slice(b"e0");
    let mut ki = i_lo.clone();
    ki.extend_from_slice(b"e0");
    place_write(core, &t2, &kr, b"r");
    place_write(core, &t2, &ku, b"u");
    place_write(core, &t2, &ki, b"i");
    ok(core.commit(t2, SyncCommit::On));

    // TRUNCATE: one row covering the old table and both old index
    // ranges; the index is re-allocated.
    let t3 = rig.rc();
    let sid1 = rig.truncate_table(&t3, oid_t);
    ok(core.commit(t3, SyncCommit::On));
    let rows = rig.retired_rows();
    assert_eq!(rows.len(), 1, "TRUNCATE writes one row per statement");
    let RetiredRow { rel_oid, ranges } = rows[0].1.clone();
    assert_eq!(rel_oid, oid_t);
    assert_eq!(ranges.len(), 3, "old table + two index ranges");
    assert!(ranges.contains(&table_range(sid0)));

    // Data in the new storage (must survive the cleanup).
    let t4 = rig.rc();
    let kn = tkey(sid1, "n0");
    place_write(core, &t4, &kn, b"n");
    ok(core.commit(t4, SyncCommit::On));

    // DROP INDEX: retires the re-allocated index ranges.
    let t5 = rig.rc();
    rig.drop_index(&t5, oid_ix);
    ok(core.commit(t5, SyncCommit::On));
    assert_eq!(rig.retired_rows().len(), 2, "DROP INDEX writes its own row");

    // Drive the cleanup to quiescence: scan, heartbeat, publish, clean.
    let gc = gc(&rig.core);
    let mut cleanup = DropCleanup::new(Arc::clone(&rig.core), Arc::clone(&gc));
    ok(cleanup.run_once()); // scan at S; gated (S == W)
    heartbeat(&rig.core, b"/t/1/hb2");
    ok(gc.publish(0));
    ok(cleanup.run_once()); // row 1
    ok(cleanup.run_once()); // row 2
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));

    // The old ranges are raw-empty; the new table storage is not.
    let (lo, hi) = table_range(sid0);
    assert!(
        rig.raw_range_empty(&lo, &hi),
        "the old table range is cleaned"
    );
    for (lo, hi) in nucleus_txn::catalog::index_ranges(sid_i0) {
        assert!(
            rig.raw_range_empty(&lo, &hi),
            "the old index range is cleaned"
        );
    }
    let (nlo, nhi) = table_range(sid1);
    assert!(
        !rig.raw_range_empty(&nlo, &nhi),
        "the new table storage is untouched"
    );
    assert_eq!(
        rig.read_at(&kn, core.visible_ts()),
        Some(b"n".to_vec()),
        "the new storage's data reads"
    );
    assert!(rig.retired_rows().is_empty(), "both marker rows are gone");
    drop(cleanup);
    drop(gc);
}

/// DT-IDXRANGES (DROP TABLE's retired row omits the index ranges — only
/// `retired[0]`): a table **with** an Id index, data under all three
/// ranges, is DROPped; the row's ranges must equal the noted set exactly
/// — the table range plus both index ranges, the order `note_retired`
/// received them — and every one of them must go raw-empty once the
/// cleanup passes the durable-W gate (C-B1b gate L-1: leaked index
/// storage is otherwise invisible forever). The filler relation keeps
/// `rel_oid` off 0 (N-2: the oid assertion must not be vacuous) and its
/// surviving storage proves the emptiness is the cleanup's doing.
#[test]
fn drop_table_with_an_index_retires_every_range() {
    let rig = B1Rig::new();
    let core = &rig.core;

    // A filler relation first: the dropped table's oid is non-zero, and
    // the filler's own data proves the cleanup's emptiness is not global.
    let t0 = rig.rc();
    let (_oid_f, sid_f) = rig.create_table(&t0, b"F");
    ok(core.commit(t0, SyncCommit::On));
    let tf = rig.rc();
    place_write(core, &tf, &tkey(sid_f, "f0"), b"filler");
    ok(core.commit(tf, SyncCommit::On));

    let t1 = rig.rc();
    let (oid_t, sid_t) = rig.create_table(&t1, b"T");
    ok(core.commit(t1, SyncCommit::On));
    let t2 = rig.rc();
    let (_oid_ix, sid_ix) = rig.create_index(&t2, oid_t);
    ok(core.commit(t2, SyncCommit::On));

    // Data under the table range and both index ranges (committed,
    // unresolved — the retire path removes the intents through §7.3
    // first).
    let t3 = rig.rc();
    let kr = tkey(sid_t, "r0");
    let (u_lo, _) = nucleus_txn::catalog::index_ranges(sid_ix)[0].clone();
    let (i_lo, _) = nucleus_txn::catalog::index_ranges(sid_ix)[1].clone();
    let mut ku = u_lo.clone();
    ku.extend_from_slice(b"e0");
    let mut ki = i_lo.clone();
    ki.extend_from_slice(b"e0");
    place_write(core, &t3, &kr, b"r");
    place_write(core, &t3, &ku, b"u");
    place_write(core, &t3, &ki, b"i");
    ok(core.commit(t3, SyncCommit::On));

    // DROP TABLE: one row, its ranges exactly the noted set.
    let t4 = rig.rc();
    rig.drop_table(&t4, oid_t);
    ok(core.commit(t4, SyncCommit::On));
    let rows = rig.retired_rows();
    assert_eq!(rows.len(), 1, "one retired row per DDL statement: {rows:?}");
    let RetiredRow { rel_oid, ranges } = rows[0].1.clone();
    assert_eq!(rel_oid, oid_t, "the row names the dropped relation");
    assert_ne!(
        rel_oid, 0,
        "not the first-ever relation, so the oid check is not vacuous (N-2)"
    );
    let mut expected = vec![table_range(sid_t)];
    expected.extend(nucleus_txn::catalog::index_ranges(sid_ix));
    assert_eq!(
        ranges, expected,
        "the row carries the table range and both index ranges"
    );

    // Drive the cleanup to quiescence: scan, heartbeat, publish, clean.
    let gc = gc(&rig.core);
    let mut cleanup = DropCleanup::new(Arc::clone(core), Arc::clone(&gc));
    ok(cleanup.run_once()); // scan at S; gated (S == W)
    heartbeat(&rig.core, b"/t/1/hb3");
    ok(gc.publish(0));
    ok(cleanup.run_once()); // the row
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));

    // Every noted range is raw-empty afterwards...
    for (lo, hi) in &expected {
        assert!(
            rig.raw_range_empty(lo, hi),
            "the retired range {lo:?}..{hi:?} is cleaned"
        );
    }
    // ...the filler's storage is not (its committed write survives; the
    // emptiness is the cleanup's doing, not the workload's)...
    let (flo, fhi) = table_range(sid_f);
    assert!(
        !rig.raw_range_empty(&flo, &fhi),
        "the filler relation's storage is untouched"
    );
    assert_eq!(
        rig.read_at(&tkey(sid_f, "f0"), core.visible_ts()),
        Some(b"filler".to_vec()),
        "the filler's data reads"
    );
    // ...and the marker row is gone.
    assert!(rig.retired_rows().is_empty(), "the marker row is deleted");
    drop(cleanup);
    drop(gc);
}

/// Intents inside a retired range go through §7.3 first (§9.2): an
/// aborted txn's uncleaned intent (its cleanup still queued) and an
/// older-epoch committed intent under the dropped range are removed with
/// full bookkeeping — the aborted txn's count reaches 0 and a resolver
/// round truncates it — and only then does the `DeleteRange` empty the
/// range.
#[test]
fn intents_in_a_retired_range_go_through_7_3_first() {
    let rig = B1Rig::new();
    let core = &rig.core;

    // Epoch 1: a committed-unresolved intent under the table's range.
    let t0 = rig.rc();
    let (oid_a, sid_a) = rig.create_table(&t0, b"A");
    ok(core.commit(t0, SyncCommit::On));
    let t1 = rig.rc();
    let k_old = tkey(sid_a, "old");
    place_write(core, &t1, &k_old, b"o");
    ok(core.commit(t1, SyncCommit::On));

    let kv = rig.kv.clone();
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2 = Arc::clone(&rig2.core);

    // Epoch 2: an aborted txn's uncleaned intent under the same range
    // (the abort queues its cleanup; the resolver is not run).
    let t2 = rig2.rc();
    let aborted_id = t2.id;
    let k_ab = tkey(sid_a, "ab");
    place_write(&core2, &t2, &k_ab, b"a");
    ok(core2.abort(t2));

    // The DROP commits; its row covers both intents.
    let t3 = rig2.rc();
    rig2.drop_table(&t3, oid_a);
    ok(core2.commit(t3, SyncCommit::On));

    let gc = gc(&core2);
    let mut cleanup = DropCleanup::new(Arc::clone(&core2), Arc::clone(&gc));
    ok(cleanup.run_once()); // scan at S; gated
    heartbeat(&core2, b"/t/1/hb3");
    ok(gc.publish(0));
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Progress));

    let (lo, hi) = table_range(sid_a);
    assert!(
        rig2.raw_range_empty(&lo, &hi),
        "the range is raw-empty after the retire"
    );

    // Counts exact: the aborted txn's §7.3 bookkeeping reached 0, so the
    // resolver's truncation pass removes its entry; the older-epoch
    // records (the sweep must finish first, §7.4 condition 1) go with
    // it. A count that never reached 0 (a `Discard` that skipped
    // bookkeeping) would leave the entry behind.
    {
        let mut sweep = nucleus_txn::jobs::BootSweep::new(Arc::clone(&core2));
        while ok(sweep.run_once()) != JobOutcome::Finished {
            // The sweep advances every step; `Finished` ends it.
        }
    }
    // Settle before asserting: a commit's cleanup enqueue legally lands
    // after its ack (§3 steps 4→5), and the newest commit here is the
    // cleanup job's own marker delete — its queue entry can still be in
    // flight on the commit thread. Drain like the C-B1a harness does
    // (until no intent remains anywhere and the queue is empty) instead
    // of one round; bounded, so a stuck enqueue still fails loudly.
    // Settle before asserting: a commit's cleanup enqueue legally lands
    // after its ack (§3 steps 4→5), and the newest commit here is the
    // cleanup job's own marker delete — its queue entry can still be in
    // flight on the commit thread. Drain like the C-B1a harness does
    // (until no intent remains anywhere and the queue is empty) instead
    // of one round; bounded, so a stuck enqueue still fails loudly.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            ok(Resolver::run_once(&core2));
            if no_intents(&core2) && core2.resolve_queue_len() == 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the resolver did not settle: an intent or a queued entry remains"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    assert!(
        core2.status.entry(aborted_id).is_none(),
        "the aborted txn truncated: its intent count reached 0 through §7.3"
    );
    let left: Vec<Vec<u8>> = {
        let view = core2.open_view();
        view.scan(
            (
                std::ops::Bound::Included(nucleus_txn::encoding::sys_txn_prefix().as_slice()),
                std::ops::Bound::Excluded(nucleus_txn::encoding::sys_txn_prefix_end().as_slice()),
            ),
            false,
        )
        .map(ok)
        .map(|(k, _)| k)
        .collect()
    };
    assert!(left.is_empty(), "no /sys/txn record remains: {left:?}");
    assert!(matches!(ok(cleanup.run_once()), JobOutcome::Idle));
    drop(cleanup);
    drop(gc);
}

/// NOWAKE (the `wake_job(DropCleanup)` on the durable-W rise removed):
/// the cleanup runs in a real `JobSet` with the GC runner and the commit
/// thread, at a 3600 s idle interval — the wake inside
/// `GcJob::publish`'s W-rose branch is then the only thing that can run
/// it again. One heartbeat lifts `visible_ts` past the row's remembered
/// snapshot; the row must be cleaned within a bounded window, with no
/// poke from the test (C-B1b gate L-2: without the wake only the idle
/// interval remains, a latency-only backstop).
#[test]
fn jobset_cleanup_cleans_once_w_rises_without_a_poke() {
    let rig = B1Rig::new();
    let core = &rig.core;
    // One dropped table with data under its range.
    let t0 = rig.rc();
    let (oid_a, sid_a) = rig.create_table(&t0, b"A");
    ok(core.commit(t0, SyncCommit::On));
    let t1 = rig.rc();
    let ka = tkey(sid_a, "a0");
    place_write(core, &t1, &ka, b"va");
    ok(core.commit(t1, SyncCommit::On));
    let t2 = rig.rc();
    rig.drop_table(&t2, oid_a);
    ok(core.commit(t2, SyncCommit::On));
    let (lo, hi) = table_range(sid_a);
    let kv = rig.kv.clone();

    let gc = gc(&rig.core);
    let mut set = JobSet::new(Arc::clone(&rig.core))
        .with_fail_stop(Arc::clone(&rig.hook) as Arc<dyn FailStop>);
    // 3600 s idle: without the W-rise wake the job never runs again
    // inside the window below.
    ok(set.start(
        Box::new(DropCleanup::new(Arc::clone(&rig.core), Arc::clone(&gc))),
        idle(3600),
    ));
    // The GC runner publishes every 20 ms: once the heartbeat lifts
    // visible_ts, its next publish raises the durable W past the row's
    // remembered snapshot — and wakes the cleanup.
    ok(set.start(
        Box::new(GcRunner::new(Arc::clone(&gc), Arc::new(SystemClock))),
        JobConfig {
            idle_interval: Duration::from_millis(20),
        },
    ));
    set.attach_commit_thread(rig.commit);

    // The first step (immediate at start) scanned the row and parked:
    // gated at S == W (W only passes a snapshot when visible_ts does).
    wait_until(5, "the cleanup saw the retired row", || {
        pending_gauge(&rig.core) == 1
    });
    // The W rise itself: one heartbeat commit. No poke of the cleanup.
    heartbeat(&rig.core, b"/t/1/hb1");

    // The row is cleaned within the window — by the wake, not the
    // interval. The gauge belongs in the condition too: `run_once`
    // publishes the marker delete (which makes the row read absent)
    // before it drains `seen` and zeroes the gauge, so observing the
    // empty catalog is not yet observing the finished step.
    wait_until(5, "the retired row was cleaned after W rose", || {
        retired_rows_now(&rig.core).is_empty()
            && raw_empty(&kv, &lo, &hi)
            && pending_gauge(&rig.core) == 0
    });
    assert_eq!(pending_gauge(&rig.core), 0, "nothing is pending anymore");

    ok(set.shutdown());
    assert!(
        rig.hook.errors().is_empty(),
        "a clean run reports nothing: {:?}",
        rig.hook.errors()
    );
    assert_eq!(set.shutdown_log(), expected_jobset_log());
}

/// SHUT-ORDER (`DropCleanup` moved after `CommitThread` in
/// `SHUTDOWN_ORDER`): a probe parks the cleanup's step 300 ms at
/// `RetireBeforeMarkerDelete` — its marker delete is genuinely in flight
/// — while the test calls `set.shutdown()`. In the prescribed order the
/// job stops **before** the commit thread: the join waits out the park
/// and the in-flight delete is acked by the still-running pipeline, so
/// the shutdown is clean. With the mutant the commit thread dies first
/// and the delete fails, fail-stopping the set (C-B1b gate L-3).
#[test]
fn jobset_shutdown_acks_an_inflight_marker_delete() {
    let rig = B1Rig::new();
    let core = &rig.core;
    // One dropped table with data under its range.
    let t0 = rig.rc();
    let (oid_a, sid_a) = rig.create_table(&t0, b"A");
    ok(core.commit(t0, SyncCommit::On));
    let t1 = rig.rc();
    let ka = tkey(sid_a, "a0");
    place_write(core, &t1, &ka, b"va");
    ok(core.commit(t1, SyncCommit::On));
    let t2 = rig.rc();
    rig.drop_table(&t2, oid_a);
    ok(core.commit(t2, SyncCommit::On));
    let (lo, hi) = table_range(sid_a);
    let kv = rig.kv.clone();

    /// Counts `RetireAfterRange` hits and parks 300 ms at the first
    /// `RetireBeforeMarkerDelete`, so the marker delete overlaps the
    /// shutdown call deterministically.
    struct ParkAtMarkerDelete {
        ranges: AtomicU64,
        parked: AtomicBool,
    }
    impl JobProbe for ParkAtMarkerDelete {
        fn at(&self, point: JobPoint) {
            match point {
                JobPoint::RetireAfterRange => {
                    self.ranges.fetch_add(1, Ordering::SeqCst);
                }
                JobPoint::RetireBeforeMarkerDelete if !self.parked.swap(true, Ordering::SeqCst) => {
                    std::thread::sleep(Duration::from_millis(300));
                }
                _ => {}
            }
        }
    }
    let probe = Arc::new(ParkAtMarkerDelete {
        ranges: AtomicU64::new(0),
        parked: AtomicBool::new(false),
    });
    rig.core
        .set_job_probe(Arc::clone(&probe) as Arc<dyn JobProbe>);

    let gc = gc(&rig.core);
    let mut set = JobSet::new(Arc::clone(&rig.core))
        .with_fail_stop(Arc::clone(&rig.hook) as Arc<dyn FailStop>);
    ok(set.start(
        Box::new(DropCleanup::new(Arc::clone(&rig.core), Arc::clone(&gc))),
        idle(3600),
    ));
    ok(set.start(
        Box::new(GcRunner::new(Arc::clone(&gc), Arc::new(SystemClock))),
        JobConfig {
            idle_interval: Duration::from_millis(20),
        },
    ));
    set.attach_commit_thread(rig.commit);

    // The row is seen and gated; the heartbeat lifts visible_ts past its
    // remembered snapshot and the next publish wakes the cleanup.
    wait_until(5, "the cleanup saw the retired row", || {
        pending_gauge(&rig.core) == 1
    });
    heartbeat(&rig.core, b"/t/1/hb2");
    // The woken step wrote the range's DeleteRange batch (the probe
    // counted it) and is now parked at the marker-delete probe.
    wait_until(5, "the retired range was deleted", || {
        probe.ranges.load(Ordering::SeqCst) >= 1
    });
    std::thread::sleep(Duration::from_millis(50));

    // The ordered shutdown: DropCleanup stops (the join waits out the
    // park; the in-flight delete is acked) before the commit thread.
    ok(set.shutdown());
    assert!(
        rig.hook.errors().is_empty(),
        "the in-flight marker delete was acked, not failed: {:?}",
        rig.hook.errors()
    );
    // The in-flight work landed: the row is gone and the range is empty.
    assert!(
        retired_rows_now(&rig.core).is_empty(),
        "the marker row is deleted"
    );
    assert!(raw_empty(&kv, &lo, &hi), "the retired range is raw-empty");
    assert_eq!(set.shutdown_log(), expected_jobset_log());
}
