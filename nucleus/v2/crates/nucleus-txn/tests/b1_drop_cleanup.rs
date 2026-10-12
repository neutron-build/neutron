//! C-B1b drop-cleanup tests (§9.2 SD3): the retired-storage row commits,
//! aborts and rolls back with its DDL, the cleanup waits for the durable
//! `W` to pass the snapshot that saw the row, TRUNCATE and DROP INDEX
//! retire their ranges (and the new storage is untouched), and intents
//! inside a retired range go through §7.3 before the `DeleteRange`. Each
//! test's doc names the mutant it kills (card item 7).

mod b1_support;

use std::sync::Arc;

use b1_support::{ok, place_write, reopen_rig, tkey, B1Rig};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{table_range, RetiredRow};
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::jobs::metrics::METRIC_DROP_CLEANUP_PENDING;
use nucleus_txn::jobs::{DropCleanup, JobOutcome};
use nucleus_txn::resolver::Resolver;

/// Installs the GC job (the cleanup retires through it).
fn gc<K: nucleus_kv::OrderedKv>(core: &Arc<Core<K>>) -> Arc<GcJob<K>> {
    Arc::new(ok(GcJob::install(core, GcConfig::default())))
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
    ok(Resolver::run_once(&core2));
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
