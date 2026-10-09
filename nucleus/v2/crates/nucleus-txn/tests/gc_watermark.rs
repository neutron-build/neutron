//! C-T4 watermark tests: `/sys/gc_w` publish ordering (G0 seeds 42, 22,
//! 31, 21, 55) and the §9.1 `/sys/ts_clock` window conversion.

mod gc_support;

use std::sync::Arc;

use gc_support::{
    commit_one, ok, preload_gc_w, preload_ts_clock, preload_ts_hwm, put_tombstone, put_version,
    read_at, read_through_view, reader_id, sys_gc_w, DurabilitySpy, FailOnKey, FailWatermark,
    SharedKv,
};
use nucleus_kv::{Durability, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::CommitPipeline;
use nucleus_txn::encoding::{end_key, intent_key, sys_gc_w_key};
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::{Ts, TxnError};

const K0: &[u8] = b"/t/0/k0";
const K1: &[u8] = b"/t/0/k1";

// ---- seed 42 ---------------------------------------------------------------

/// G0 seed 42 ("the compaction filter is handed `W` before `/sys/gc_w` is
/// synced"): the `/sys/gc_w` write fails, `publish` returns the error, and
/// the filter still runs at the old durable W = 10 — a compaction
/// afterwards drops nothing the old W would keep (k@10 is the newest
/// version <= 10, not shadowed).
///
/// Mutant: store the durable W before the write — the filter runs at 20,
/// k@20 shadows k@10 in one stream, and k@10 disappears.
#[test]
fn seed42_filter_w_only_after_sync() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    put_version(&kv, K0, 10, b"x");
    put_version(&kv, K0, 20, b"y");
    // k1: a tombstone newest-<= 20. Bait for a tombstone pass (or a retire
    // gate) running at the published 20 instead of the durable 10.
    put_version(&kv, K1, 10, b"p");
    put_tombstone(&kv, K1, 20, false);

    let failing = FailOnKey::new(kv.clone(), &sys_gc_w_key());
    let core = Arc::new(ok(Core::open(failing.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // Computed W = visible_ts = 20 > durable 10: the synced write fails.
    failing.arm();
    let err = job.publish(0).expect_err("publish must fail");
    assert!(
        matches!(err, TxnError::Kv(_)),
        "the injected /sys/gc_w failure, got {err:?}"
    );
    assert_eq!(
        core.registry.published_w(),
        Ts(20),
        "the registry published 20; only the durable W stayed at 10"
    );

    // Rework 2: every GC step still uses the old durable W = 10.
    assert_eq!(
        ok(job.drop_tombstones()),
        0,
        "at durable W = 10 no key's newest version <= W is a tombstone"
    );
    let before = kv.raw_entries(b"", &[0xff]);
    assert!(
        !ok(job.retire_storage(&intent_key(K1), &end_key(K1), Ts(15), &|_| None)),
        "durable 10 <= retired 15: the gate holds even though published is 20"
    );
    assert_eq!(
        kv.raw_entries(b"", &[0xff]),
        before,
        "neither GC step wrote anything"
    );

    // The filter still uses the old durable W: a full compaction drops
    // nothing the old W keeps.
    kv.compact_all();
    let raw = kv.raw_entries(&intent_key(K0), &end_key(K0));
    assert_eq!(
        raw.len(),
        2,
        "at durable W = 10 both k0@20 (> W) and k0@10 (the newest <= W) must survive: {raw:?}"
    );
    assert_eq!(
        kv.raw_entries(&intent_key(K1), &end_key(K1)).len(),
        2,
        "k1@20 and k1@10 survive for the same reason"
    );
}

// ---- Rework 3: /sys/gc_w is synced ------------------------------------------

/// Rework 3: the `/sys/gc_w` write is synced. A recording wrapper asserts
/// the durability of the one write that touches `/sys/gc_w` during a
/// successful publish.
///
/// Mutant: `Durability::No` in `publish` - the spy records `No` and the
/// assertion fails.
#[test]
fn gc_w_write_is_synced() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let spy = DurabilitySpy::new(kv.clone(), &sys_gc_w_key());
    let core = Arc::new(ok(Core::open(spy.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    assert_eq!(ok(job.publish(0)), Ts(20));
    assert_eq!(
        spy.seen(),
        vec![Durability::Yes],
        "/sys/gc_w is written with Durability::Yes (C-T0 9.1, Rework 3)"
    );
    assert_eq!(sys_gc_w(&core), 20);
}

// ---- seed 22 ---------------------------------------------------------------

/// G0 seed 22 ("`W` allowed to decrease"): after a synced W = 40 and a
/// reopen, a retention window whose computed floor is 2 must keep
/// `W = max(40, 2) = 40` — `/sys/gc_w` reads 40 and `register_at(15)` is
/// refused (72000) because 15 is below data GC already dropped at 40.
///
/// Mutant: publish `computed` instead of the max (C-T1a's registry; a
/// regression test) — W regresses to 2 and `register_at(15)` succeeds.
#[test]
fn seed22_w_never_decreases() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 40);
    preload_gc_w(&kv, 10);
    preload_ts_clock(&kv, 2, 100);
    preload_ts_clock(&kv, 5, 200);

    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));
    // Window none, nothing registered: computed = visible_ts = 40.
    assert_eq!(ok(job.publish(0)), Ts(40));
    assert_eq!(sys_gc_w(&core), 40);
    drop(job);

    // Reopen over the same store: boots from the synced /sys/gc_w = 40.
    let reopened = Arc::try_unwrap(core).ok().expect("sole core ref").into_kv();
    let core2 = Arc::new(ok(Core::open(reopened)));
    // now = 350, window = 250: cutoff = 100, floor = ts 2 (the sample at
    // wall 200 is past it), computed = 2.
    let job2 = ok(GcJob::install(
        &core2,
        GcConfig {
            as_of_window_secs: Some(250),
        },
    ));
    assert_eq!(
        ok(job2.publish(350)),
        Ts(40),
        "W is max(old, computed): 40, never the computed 2"
    );
    assert_eq!(sys_gc_w(&core2), 40);
    assert_eq!(
        core2.registry.register_at(Ts(15)).err(),
        Some(TxnError::SnapshotTooOld),
        "15 is below the published W = 40 (seed 22)"
    );
    // The boundary is admitted: t = W = 40 <= visible_ts = 40.
    assert!(core2.registry.register_at(Ts(40)).is_ok());
}

// ---- seed 31 ---------------------------------------------------------------

/// G0 seed 31 ("AS OF registration checked against an unpublished `W`"):
/// `publish_computed_w` run directly publishes W = 30 in the registry
/// while `/sys/gc_w` still holds 10 — `register_at` must check the
/// published W, refusing `t = 15` (72000). A check against the durable
/// `/sys/gc_w` (10) would admit a read below data GC may drop at 30.
#[test]
fn seed31_as_of_checks_published_w() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 30);
    preload_gc_w(&kv, 10);
    let core = ok(Core::open(kv.clone()));

    let published = core.registry.publish_computed_w(None);
    assert_eq!(published, Ts(30), "computed = visible_ts = 30");
    assert_eq!(sys_gc_w(&core), 10, "the durable /sys/gc_w is still 10");

    assert_eq!(
        core.registry.register_at(Ts(15)).err(),
        Some(TxnError::SnapshotTooOld),
        "15 >= durable 10 but < published 30: refused (seed 31)"
    );
    // Above visible_ts is refused as well; 30 itself is admitted.
    assert_eq!(
        core.registry.register_at(Ts(31)).err(),
        Some(TxnError::SnapshotTooOld)
    );
    assert!(core.registry.register_at(Ts(30)).is_ok());
}

// ---- seed 21 ---------------------------------------------------------------

/// G0 seed 21 ("taking `S` and registering it are two steps"), pinned from
/// the job side: a snapshot taken at 20 keeps `W <= 20` across publishes
/// until it is dropped, because `publish` computes the min over registered
/// snapshots inside the registry's critical section.
#[test]
fn seed21_snapshot_holds_w() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    let snap = core.registry.take_snapshot();
    assert_eq!(snap.ts(), Ts(20));

    // A commit past the snapshot: visible_ts = 21.
    commit_one(&core, &mut pipeline, K0, Some(b"v21".as_slice()));

    assert_eq!(ok(job.publish(0)), Ts(20), "the registered S = 20 holds W");
    assert_eq!(ok(job.publish(0)), Ts(20), "and keeps holding it");
    assert_eq!(sys_gc_w(&core), 20);

    drop(snap);
    assert_eq!(
        ok(job.publish(0)),
        Ts(21),
        "with the snapshot gone, W passes 20"
    );
    assert_eq!(sys_gc_w(&core), 21);
}

// ---- seed 55 ---------------------------------------------------------------

/// G0 seed 55 ("latest-state view does not hold `W`"): a view open at
/// `vts = 20` holds `W <= 20` across a publish and a compaction, so what
/// the view reads for k1 does not change while it is open; after it
/// closes, the next publish passes 20.
#[test]
fn seed55_open_view_holds_w() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    kv.flush();
    kv.settle();
    put_version(&kv, K1, 10, b"b");
    kv.flush();
    kv.settle();
    put_version(&kv, K1, 20, b"c");
    kv.flush();

    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    // The open view (a deferred-FK-style latest-state read, §3.1).
    let view = core.open_view();
    let vts = core.visible_ts();
    assert_eq!(vts, Ts(20));
    let reader = reader_id(&core);
    assert_eq!(
        read_through_view(&core, &view, K1, vts, reader).as_deref(),
        Some(b"c".as_slice())
    );

    // A newer committed, resolved version of k1 (ts 21).
    assert_eq!(
        commit_one(&core, &mut pipeline, K1, Some(b"new".as_slice())),
        Ts(21)
    );

    // W is held at the view's vts = 20.
    assert_eq!(ok(job.publish(0)), Ts(20));
    assert_eq!(sys_gc_w(&core), 20);

    // A full compaction at W = 20 keeps k1@20 (the newest <= W); the open
    // view's read must not change — in MemKv's LSM mode a filter drop is
    // visible to already-open snapshots, the adversarial choice, so a view
    // holding W only through its own vts would see its k1@20 disappear.
    kv.compact_all();
    assert_eq!(
        read_through_view(&core, &view, K1, vts, reader).as_deref(),
        Some(b"c".as_slice()),
        "the open view still reads k1@20 = 'c' (seed 55)"
    );
    // A fresh read sees the same at 20, and the new version at 21.
    assert_eq!(
        read_at(&core, K1, Ts(20), reader).as_deref(),
        Some(b"c".as_slice())
    );
    assert_eq!(
        read_at(&core, K1, Ts(21), reader).as_deref(),
        Some(b"new".as_slice())
    );

    // After the view closes, W passes 20.
    drop(view);
    assert_eq!(ok(job.publish(0)), Ts(21));
    assert_eq!(sys_gc_w(&core), 21);
}

// ---- the /sys/ts_clock window ----------------------------------------------

/// §9.1: with samples (ts 5, wall 100), (ts 9, wall 200), (ts 12, wall
/// 250, exactly the cutoff) and (ts 14, wall 300), with `visible_ts` =
/// 100: window 150 at now 400 gives cutoff 250 and floor ts 12 (the
/// sample whose wall time equals the cutoff qualifies); a cutoff below
/// every sample gives floor 0 (and W = 0 on a fresh store); a saturating
/// cutoff (now < window) gives floor 0.
///
/// Mutant: round up (pick ts 14) - the first case publishes 14, not 12.
/// Mutant: `<` instead of `<=` on the wall time - the boundary sample no
/// longer qualifies and the first case publishes 9, not 12.
#[test]
fn ts_clock_window_conversion() {
    let samples = [(5u64, 100u64), (9, 200), (12, 250), (14, 300)];
    let fresh = |window: u64| -> (SharedKv, Arc<Core<SharedKv>>, GcJob<SharedKv>) {
        let kv = SharedKv::lsm();
        preload_ts_hwm(&kv, 100);
        for (ts, wall) in samples {
            preload_ts_clock(&kv, ts, wall);
        }
        let core = Arc::new(ok(Core::open(kv.clone())));
        let job = ok(GcJob::install(
            &core,
            GcConfig {
                as_of_window_secs: Some(window),
            },
        ));
        (kv, core, job)
    };

    // cutoff 250: the samples at walls 100, 200 and 250 qualify (the last
    // exactly at the cutoff); the largest sampled ts among them is 12.
    let (kv, core, job) = fresh(150);
    assert_eq!(ok(job.publish(400)), Ts(12));
    assert_eq!(sys_gc_w(&core), 12);
    drop((kv, core, job));

    // No sample below the cutoff: floor 0 -> W = 0 (visible_ts is not a
    // floor once the window term is 0).
    let (kv2, core2, job2) = fresh(350);
    assert_eq!(
        ok(job2.publish(400)),
        Ts(0),
        "cutoff 50: no sample qualifies"
    );
    drop((kv2, core2, job2));

    // Saturating subtraction: now 100 < window 150 -> cutoff 0.
    let (kv3, core3, job3) = fresh(150);
    assert_eq!(ok(job3.publish(100)), Ts(0), "cutoff saturates at 0");
    drop((kv3, core3, job3));
}

// ---- install ----------------------------------------------------------------

/// `install` hands the KV the boot W as its watermark (§9.1): the
/// monotonic `set_gc_watermark` then refuses anything below it. With no
/// install the KV's watermark would still be 0 and 19 would be accepted.
#[test]
fn install_sets_the_kv_watermark_to_the_boot_w() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 20);
    let core = Arc::new(ok(Core::open(kv.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));
    assert_eq!(ok(job.publish(0)), Ts(20));
    assert_eq!(sys_gc_w(&core), 20, "W == durable: no new write is needed");
    assert!(
        kv.mem.set_gc_watermark(19).is_err(),
        "the watermark is monotonic below the installed 20"
    );
    assert!(kv.mem.set_gc_watermark(20).is_ok());
}

/// Rework 6: `install` takes the durable W from `/sys/gc_w` itself, not
/// from `registry.published_w()`. A direct `publish_computed_w` (allowed
/// outside the job) has raised the published W to 30 while `/sys/gc_w`
/// still holds the synced 10; the installed filter must run at 10 - a
/// filter handed the unpublished 30 would be a W no GC step may act on
/// yet (seed 42's rule at install time).
///
/// Mutant: `install` reads `registry.published_w()` - the filter runs at
/// 30, k0@20 becomes a shadowing version <= W and k0@10 is dropped by the
/// compaction, leaving one raw entry instead of two.
#[test]
fn install_reads_durable_gc_w_not_published() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 30);
    preload_gc_w(&kv, 10);
    put_version(&kv, K0, 10, b"a");
    put_tombstone(&kv, K0, 20, false);
    kv.settle();

    let core = Arc::new(ok(Core::open(kv.clone())));
    assert_eq!(core.registry.publish_computed_w(None), Ts(30));
    assert_eq!(sys_gc_w(&core), 10, "the durable /sys/gc_w is still 10");

    // No publish runs: the filter acts at exactly the W install handed it.
    let _job = ok(GcJob::install(&core, GcConfig::default()));

    kv.compact_all();
    assert_eq!(
        kv.raw_entries(&intent_key(K0), &end_key(K0)).len(),
        2,
        "the installed filter runs at the durable W = 10: k0@20 (> W) and \
         k0@10 (the newest <= W) both survive"
    );
}

/// Rework 6: a second `install` while one job is live on the core is an
/// error (one publisher, §9.1). Dropping the job releases the slot, so a
/// reinstall after the previous job is gone is allowed.
///
/// Mutant: no live-job check - the second `install` succeeds and the
/// `expect_err` fails.
#[test]
fn second_install_on_a_live_core_is_an_error() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    let core = Arc::new(ok(Core::open(kv.clone())));

    let job = ok(GcJob::install(&core, GcConfig::default()));
    let err = match GcJob::install(&core, GcConfig::default()) {
        Ok(_) => panic!("a second install on a live core must fail"),
        Err(e) => e,
    };
    assert!(
        matches!(err, TxnError::Invariant(_)),
        "Invariant expected, got {err:?}"
    );
    drop(job);
    // The slot is free again: a reinstall after the job is gone works.
    let job2 = GcJob::install(&core, GcConfig::default());
    assert!(job2.is_ok(), "reinstall after the previous job dropped");
    drop(job2);
}

/// Rework 6: an error from the KV watermark call propagates out of
/// `publish` (and the durable W the filter uses stays at the old value).
/// The `/sys/gc_w` write succeeds; only `set_kv_gc_watermark` fails.
///
/// Mutants: the error swallowed (`let _ =` on the watermark call) -
/// `publish` returns `Ok(20)`; `publish` skipping the KV watermark call -
/// same. Also the filter must still act at the old durable 10.
#[test]
fn kv_watermark_error_is_propagated() {
    let kv = SharedKv::lsm();
    preload_ts_hwm(&kv, 20);
    preload_gc_w(&kv, 10);
    put_version(&kv, K0, 10, b"x");
    put_version(&kv, K0, 20, b"y");

    let failing = FailWatermark::new(kv.clone());
    let core = Arc::new(ok(Core::open(failing.clone())));
    let job = ok(GcJob::install(&core, GcConfig::default()));

    failing.arm();
    let err = job
        .publish(0)
        .expect_err("the KV watermark failure propagates");
    assert!(
        matches!(err, TxnError::Kv(_)),
        "the injected gc-watermark failure, got {err:?}"
    );

    // The durable W the filter reads never moved: a full compaction keeps
    // everything the old W keeps.
    kv.compact_all();
    assert_eq!(
        kv.raw_entries(&intent_key(K0), &end_key(K0)).len(),
        2,
        "the filter still runs at the old durable W = 10"
    );
    // /sys/gc_w holds 20 (the write succeeded); the slot lags until a
    // later publish succeeds - the next round rewrites and re-arms.
    assert_eq!(sys_gc_w(&core), 20);
}
