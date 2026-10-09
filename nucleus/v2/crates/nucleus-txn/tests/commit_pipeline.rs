//! C-T1b tests: the §3 commit pipeline — pipeline order (I-VIS, I-ACK,
//! I-WAL-ORDER), the mixed off/on/off group, the G0-commit seed regressions
//! 7, 8 and 9, `/sys/ts_clock` sampling, the `CommitObserver` hook,
//! fail-stop, and the rework's API rules (one pipeline per core, the hwm
//! block, the default block size).

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ok, place_intent, Event, FailKv, FailNthKv, FailSyncKv, FakeClock, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{
    CommitConfig, CommitObserver, CommitPipeline, CommitRequest, FailStop, SyncCommit, TS_HWM_BLOCK,
};
use nucleus_txn::encoding::{
    intent_key, parse_sys_txn_key, sys_log_key, sys_ts_clock_key, sys_ts_hwm_key, sys_txn_key,
    sys_txn_prefix, sys_txn_prefix_end,
};
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnError, TxnStatus};

/// A txn committed through the real API, for the order tests.
fn commit_one(core: &Arc<Core<RecKv>>, i: usize, sync: SyncCommit) -> (nucleus_txn::TxnId, Ts) {
    let txn = core.begin(Isolation::ReadCommitted);
    let id = txn.id;
    let key = format!("/t/1/r{i}");
    place_intent(core, &txn, key.as_bytes(), format!("v{i}").as_bytes());
    txn.log_write(ok(txn.next_seq()), key.as_bytes()); // a second layer's entry
    let ts = ok(core.commit(txn, sync));
    (id, ts)
}

#[test]
fn pipeline_order_i_vis_and_i_ack() {
    // I-VIS and I-ACK as a poller regression (the G0 model proves them
    // exhaustively; this catches a pipeline that advances visible_ts before
    // the status or acks early). A record visible in a KV view plus a still
    // Pending status plus visible_ts >= ts is a violation in any sample.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core)));
    let stop = Arc::new(AtomicBool::new(false));
    let violation = Arc::new(Mutex::new(None::<String>));
    let samples = Arc::new(Mutex::new(0u64));

    let poller = {
        let core = Arc::clone(&core);
        let stop = Arc::clone(&stop);
        let violation = Arc::clone(&violation);
        let samples = Arc::clone(&samples);
        std::thread::spawn(move || {
            let lo = sys_txn_prefix();
            let hi = sys_txn_prefix_end();
            while !stop.load(Ordering::SeqCst) {
                let view = core.open_view();
                let rows: Vec<_> = view
                    .scan(
                        (
                            std::ops::Bound::Included(lo.as_slice()),
                            std::ops::Bound::Excluded(hi.as_slice()),
                        ),
                        false,
                    )
                    .map(ok)
                    .collect();
                for (k, v) in rows {
                    let Some(id) = parse_sys_txn_key(&k) else {
                        continue;
                    };
                    if v.len() != 8 {
                        continue;
                    }
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&v);
                    let ts = Ts(u64::from_be_bytes(b));
                    let vis1 = core.visible_ts();
                    let entry = core.status.entry(id);
                    let vis2 = core.visible_ts();
                    *samples.lock().expect("samples") += 1;
                    if let Some(e) = entry {
                        if e.status == TxnStatus::Pending && (vis1 >= ts || vis2 >= ts) {
                            *violation.lock().expect("violation") = Some(format!(
                                "I-VIS: {id:?} record {ts:?} read, status {:?}, visible {vis1:?}/{vis2:?}",
                                e.status
                            ));
                            return;
                        }
                    }
                }
            }
        })
    };

    let mut acked = Vec::new();
    for i in 0..200u32 {
        let sync = if i % 3 == 0 {
            SyncCommit::On
        } else {
            SyncCommit::Off
        };
        let (id, ts) = commit_one(&core, i as usize, sync);
        assert!(
            core.visible_ts() >= ts,
            "I-ACK: commit visible when it returns"
        );
        assert_eq!(
            core.status.entry(id).map(|e| e.status),
            Some(TxnStatus::Committed(ts)),
            "I-VIS: status set when it returns"
        );
        acked.push((id, ts));
    }
    // Every ts unique and assigned in order.
    let mut all: Vec<Ts> = acked.iter().map(|(_, t)| *t).collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), acked.len(), "no ts reused");

    stop.store(true, Ordering::SeqCst);
    poller.join().expect("poller");
    assert!(
        violation.lock().expect("violation").is_none(),
        "{:?}",
        violation.lock().expect("violation")
    );
    assert!(*samples.lock().expect("samples") > 0, "poller ran");
    ok(handle.shutdown());
}

#[test]
fn mixed_group_off_on_off_follows_step4() {
    // off prefix, then on, then off again, driven step by step through the
    // manual pipeline: acks arrive in ts order (the off prefix first), one
    // fsync covers the group, and visibility follows.
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));

    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    let t3 = core.begin(Isolation::ReadCommitted);
    for (txn, key) in [(&t1, "a"), (&t2, "b"), (&t3, "c")] {
        place_intent(&core, txn, key.as_bytes(), b"v");
    }
    let (r1, a1) = CommitRequest::new(t1.id, SyncCommit::Off, None, false, t1.write_set_keys());
    let (r2, a2) = CommitRequest::new(
        t2.id,
        SyncCommit::On,
        Some(b"log2".to_vec()),
        false,
        t2.write_set_keys(),
    );
    let (r3, a3) = CommitRequest::new(t3.id, SyncCommit::Off, None, false, t3.write_set_keys());
    ok(core.submit(r1));
    ok(core.submit(r2));
    ok(core.submit(r3));
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 3);
    pipeline.process_group(group);

    let ts1 = ok(ok(a1.recv_timeout(Duration::from_secs(5))));
    let ts2 = ok(ok(a2.recv_timeout(Duration::from_secs(5))));
    let ts3 = ok(ok(a3.recv_timeout(Duration::from_secs(5))));
    assert!(
        ts1 < ts2 && ts2 < ts3,
        "channel order: {ts1:?} {ts2:?} {ts3:?}"
    );
    assert_eq!(core.visible_ts(), ts3);
    for (txn, ts) in [(&t1, ts1), (&t2, ts2), (&t3, ts3)] {
        assert_eq!(
            core.status.entry(txn.id).map(|e| e.status),
            Some(TxnStatus::Committed(ts))
        );
    }
    // The optional /log record went in the same batch as the record.
    assert!(ok(core.latest_get(&sys_log_key(t2.id))).is_some());

    // Exactly one sync for the group, and it follows every record write.
    let log = kv.log();
    let syncs = log
        .iter()
        .enumerate()
        .filter_map(|(i, e)| matches!(e, Event::Sync).then_some(i))
        .collect::<Vec<_>>();
    assert_eq!(syncs.len(), 1, "one fsync per group: {log:?}");
    let records: Vec<usize> = log
        .iter()
        .enumerate()
        .filter_map(|(i, e)| {
            matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Put(k, _v) if parse_sys_txn_key(k).is_some())))
                .then_some(i)
        })
        .collect();
    assert_eq!(records.len(), 3);
    assert!(records[2] < syncs[0], "records precede the fsync");
}

#[test]
fn all_off_group_never_fsyncs() {
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    for txn in [&t1, &t2] {
        place_intent(&core, txn, b"k", b"v");
    }
    let (r1, a1) = CommitRequest::new(t1.id, SyncCommit::Off, None, false, t1.write_set_keys());
    let (r2, a2) = CommitRequest::new(t2.id, SyncCommit::Off, None, false, t2.write_set_keys());
    ok(core.submit(r1));
    ok(core.submit(r2));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(ok(a1.recv_timeout(Duration::from_secs(5))));
    ok(ok(a2.recv_timeout(Duration::from_secs(5))));
    assert!(
        !kv.log().iter().any(|e| matches!(e, Event::Sync)),
        "a group of only off requests does not fsync"
    );
}

#[test]
fn a_core_accepts_only_one_pipeline() {
    // Rework item: attaching a second CommitPipeline (spawned or manual)
    // to a Core that already has one is an error, never a silent takeover.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let _p1 = ok(CommitPipeline::new(Arc::clone(&core)));
    assert!(
        CommitPipeline::new(Arc::clone(&core)).is_err(),
        "a second manual pipeline must be refused"
    );
    let spawn = nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core));
    assert!(
        spawn.is_err(),
        "a spawned pipeline over an attached core must be refused"
    );
}

#[test]
fn seed7_visible_before_status_is_never_observed() {
    // Seed 7: `visible_ts` advanced before status is set. Tighter poller
    // than `pipeline_order_i_vis_and_i_ack`: a single off commit, watched
    // from the moment its record exists.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core)));
    let txn = core.begin(Isolation::ReadCommitted);
    let txn_id = txn.id;
    place_intent(&core, &txn, b"/t/1/r", b"v");
    let acked = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(Mutex::new(None::<String>));
    let watcher = {
        let core = Arc::clone(&core);
        let acked = Arc::clone(&acked);
        let bad = Arc::clone(&bad);
        std::thread::spawn(move || {
            while !acked.load(Ordering::SeqCst) {
                let v = core.visible_ts();
                let entry = core.status.entry(txn_id);
                if let Some(e) = entry {
                    if e.status == TxnStatus::Pending && v > Ts(0) {
                        // visible_ts moved off 0 while our txn (the only one)
                        // is still Pending.
                        *bad.lock().expect("bad") =
                            Some(format!("visible {v:?} ahead of status {:?}", e.status));
                        return;
                    }
                }
            }
        })
    };
    let ts = ok(core.commit(txn, SyncCommit::Off));
    acked.store(true, Ordering::SeqCst);
    watcher.join().expect("watcher");
    assert!(
        bad.lock().expect("bad").is_none(),
        "{:?}",
        bad.lock().expect("bad")
    );
    assert!(ts > Ts::ZERO);
    ok(handle.shutdown());
}

#[test]
fn seed8_ack_before_fsync_via_crash() {
    // Seed 8, end to end: ack before fsync for synchronous_commit=on. An
    // acked On commit followed by a crash that drops every unsynced batch
    // must survive (I-DURABLE); if the ack ran before the fsync, the record
    // is still unsynced and the crash eats it.
    use nucleus_kv::fault::Fault;
    use nucleus_kv::MemKv;
    let kv = Fault::new(MemKv::new(), || Ok(MemKv::new()));
    let core = Arc::new(ok(Core::open(kv)));
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core)));
    let mut acked = Vec::new();
    for i in 0..4 {
        let txn = core.begin(Isolation::ReadCommitted);
        let txn_id = txn.id;
        place_intent(&core, &txn, format!("/t/1/r{i}").as_bytes(), b"v");
        let ts = ok(core.commit(txn, SyncCommit::On));
        acked.push((txn_id, ts));
    }
    ok(handle.shutdown());
    let kv = match Arc::try_unwrap(core) {
        Ok(c) => c.into_kv(),
        Err(_) => panic!("core still shared"),
    };
    ok(kv.crash(0)); // drop every unsynced batch
    let core2 = ok(Core::open(kv));
    for (id, ts) in acked {
        assert_eq!(
            ok(core2.status.lookup_for_intent(id)),
            TxnStatus::Committed(ts),
            "I-DURABLE: acked On commit {id:?} survived"
        );
    }
}

#[test]
fn seed8_no_on_ack_precedes_the_sync() {
    // Seed 8, driven by hand (rework item 3): a group [off, on, off] whose
    // single `sync_wal` fails. Step 4 acks the off prefix, then syncs, then
    // acks the rest — so the On request's ack must be an *error* (the sync
    // failed before it), never a ts. A pipeline that acks `On` requests
    // before the fsync returns Ok here and fails this test.
    #[derive(Default)]
    struct Rec {
        errs: Mutex<Vec<String>>,
    }
    impl FailStop for Rec {
        fn on_kv_error(&self, err: &TxnError) {
            self.errs.lock().expect("errs").push(err.to_string());
        }
    }

    let (kv, fail) = FailSyncKv::shared();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let hook = Arc::new(Rec::default());
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(Arc::clone(&hook) as Arc<dyn FailStop>),
    ));

    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    let t3 = core.begin(Isolation::ReadCommitted);
    let (r1, a1) = CommitRequest::new(t1.id, SyncCommit::Off, None, false, t1.write_set_keys());
    let (r2, a2) = CommitRequest::new(t2.id, SyncCommit::On, None, false, t2.write_set_keys());
    let (r3, a3) = CommitRequest::new(t3.id, SyncCommit::Off, None, false, t3.write_set_keys());
    ok(core.submit(r1));
    ok(core.submit(r2));
    ok(core.submit(r3));
    fail.store(true, Ordering::SeqCst);
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 3);
    pipeline.process_group(group);

    // The off prefix acked Ok before the sync; the On request and the
    // trailing off request ack the fail-stop error.
    let ack1 = a1.recv_timeout(Duration::from_secs(5)).expect("off ack");
    assert!(ack1.is_ok(), "the off prefix is acked before the sync");
    let ts1 = ok(ack1);
    for (name, ack) in [("on", a2), ("off-after", a3)] {
        match ack.recv_timeout(Duration::from_secs(5)) {
            Ok(Err(_)) => {}
            Ok(Ok(ts)) => panic!("{name} acked Ok({ts:?}) although the sync failed"),
            Err(_) => panic!("{name} request never acked"),
        }
    }
    // The group stopped exactly at the sync: one attempt, no later
    // visible_ts advance, no step 5, statuses stay Pending.
    assert_eq!(kv.syncs(), 1, "exactly one sync attempt");
    assert_eq!(core.visible_ts(), ts1);
    assert_eq!(
        core.status.entry(t1.id).map(|e| e.status),
        Some(TxnStatus::Committed(ts1))
    );
    for t in [&t2, &t3] {
        assert_eq!(
            core.status.entry(t.id).map(|e| (e.status, e.released)),
            Some((TxnStatus::Pending, false)),
            "no status set and no step 5 for a request after the sync"
        );
    }
    assert_eq!(hook.errs.lock().expect("errs").len(), 1, "hook ran once");
}

#[test]
fn seed9_records_stay_in_ts_order_within_a_manual_group() {
    // Seed 9, driven by hand (rework item 4): five requests in one group;
    // the recording KV checks WAL order. /sys/txn records appear in
    // strictly increasing ts order, in channel order, and every intent
    // write of a txn precedes its record (I-WAL-ORDER). A pipeline that
    // writes the records in reverse within the group fails here.
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));

    const N: usize = 5;
    let mut txns = Vec::new();
    for i in 0..N {
        let txn = core.begin(Isolation::ReadCommitted);
        let key = format!("/t/1/r{i}");
        place_intent(&core, &txn, key.as_bytes(), b"v");
        txns.push((txn, key));
    }
    let mut acks = Vec::new();
    for (txn, _) in &txns {
        let (req, ack) =
            CommitRequest::new(txn.id, SyncCommit::Off, None, false, txn.write_set_keys());
        ok(core.submit(req));
        acks.push(ack);
    }
    let group = pipeline.drain_available();
    assert_eq!(group.len(), N);
    pipeline.process_group(group);

    // Acks come back in channel (ts) order.
    let mut ts_all = Vec::new();
    for ack in &acks {
        ts_all.push(ok(ok(ack.recv_timeout(Duration::from_secs(5)))));
    }
    let mut sorted = ts_all.clone();
    sorted.sort_unstable();
    assert_eq!(ts_all, sorted, "acks in channel order");
    assert_eq!(sorted.len(), N);

    // Records in strictly increasing ts order in the WAL, and the i-th
    // record belongs to the i-th request.
    let log = kv.log();
    let mut seen: Vec<Ts> = Vec::new();
    for e in &log {
        if let Event::Write(ops) = e {
            for op in ops {
                if let nucleus_kv::Op::Put(k, v) = op {
                    if let Some(id) = parse_sys_txn_key(k) {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(v);
                        let ts = Ts(u64::from_be_bytes(b));
                        if let Some(prev) = seen.last() {
                            assert!(
                                ts > *prev,
                                "I-WAL-ORDER: record {ts:?} written after {prev:?} within the group"
                            );
                        }
                        seen.push(ts);
                        let want = txns
                            .iter()
                            .position(|(t, _)| t.id == id)
                            .expect("known txn");
                        assert_eq!(ts, ts_all[want], "record order matches channel order");
                    }
                }
            }
        }
    }
    assert_eq!(seen.len(), N, "one record per request");
    // Every intent write precedes its txn's record.
    for (txn, key) in &txns {
        let ik = intent_key(key.as_bytes());
        let rk = sys_txn_key(txn.id);
        let intent_pos = log.iter().position(|e| matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Put(k, _) if k == &ik))));
        let record_pos = log.iter().position(|e| matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Put(k, _v) if k == &rk))));
        assert!(intent_pos.is_some() && record_pos.is_some());
        assert!(
            intent_pos < record_pos,
            "I-WAL-ORDER: intent of {:?} written after its record",
            txn.id
        );
    }
}

#[test]
fn fail_stop_at_a_chosen_write_stops_the_group() {
    // Rework item 7: a KV error inside step 2 stops the group where it
    // hit: no request is acked Ok (acks are step 4), no later record is
    // written, no sync, no visible_ts advance, no step 5, and every
    // request of the group gets an error ack.
    #[derive(Default)]
    struct Rec {
        errs: Mutex<Vec<String>>,
    }
    impl FailStop for Rec {
        fn on_kv_error(&self, err: &TxnError) {
            self.errs.lock().expect("errs").push(err.to_string());
        }
    }

    let kv = FailNthKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let hook = Arc::new(Rec::default());
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(Arc::clone(&hook) as Arc<dyn FailStop>),
    ));

    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    let t3 = core.begin(Isolation::ReadCommitted);
    let (r1, a1) = CommitRequest::new(t1.id, SyncCommit::On, None, false, t1.write_set_keys());
    let (r2, a2) = CommitRequest::new(t2.id, SyncCommit::On, None, false, t2.write_set_keys());
    let (r3, a3) = CommitRequest::new(t3.id, SyncCommit::On, None, false, t3.write_set_keys());
    ok(core.submit(r1));
    ok(core.submit(r2));
    ok(core.submit(r3));
    // Boot wrote once (the epoch); the group's second record write fails.
    kv.fail_from(kv.writes() + 2);
    let group = pipeline.drain_available();
    pipeline.process_group(group);

    for (i, ack) in [a1, a2, a3].into_iter().enumerate() {
        match ack.recv_timeout(Duration::from_secs(5)) {
            Ok(Err(_)) => {}
            Ok(Ok(ts)) => panic!("request {i} acked Ok({ts:?}) after a fail-stop"),
            Err(_) => panic!("request {i} never acked"),
        }
    }
    assert_eq!(hook.errs.lock().expect("errs").len(), 1, "hook ran once");
    assert_eq!(core.visible_ts(), Ts(0), "no visible_ts advance");
    for t in [&t1, &t2, &t3] {
        assert_eq!(
            core.status.entry(t.id).map(|e| (e.status, e.released)),
            Some((TxnStatus::Pending, false)),
            "no status, no step 5, for an unacked request"
        );
    }
    // Stopped: a later group is a no-op even with the KV healthy again.
    let t4 = core.begin(Isolation::ReadCommitted);
    let (r4, a4) = CommitRequest::new(t4.id, SyncCommit::Off, None, false, t4.write_set_keys());
    ok(core.submit(r4));
    let group2 = pipeline.drain_available();
    pipeline.process_group(group2);
    assert!(a4.recv_timeout(Duration::from_millis(200)).is_err());
}

#[test]
fn ts_clock_samples_at_most_once_per_clock_second() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let clock = FakeClock::new(100);
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    pipeline.set_clock(clock.clone());
    for i in 0..3 {
        let txn = core.begin(Isolation::ReadCommitted);
        txn.log_write(ok(txn.next_seq()), b"/t/1/r"); // a write, so no fast path
        let (req, ack) =
            CommitRequest::new(txn.id, SyncCommit::Off, None, false, txn.write_set_keys());
        ok(core.submit(req));
        let group = pipeline.drain_available();
        pipeline.process_group(group);
        ok(ok(ack.recv_timeout(Duration::from_secs(5))));
        if i == 1 {
            clock.set(101); // the second changes mid-sequence
        }
    }
    let view = core.open_view();
    let mut samples: Vec<(Ts, u64)> = Vec::new();
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (k, v) = ok(row);
        if k.starts_with(b"/sys/ts_clock/") && k.len() == b"/sys/ts_clock/".len() + 8 {
            let mut kb = [0u8; 8];
            kb.copy_from_slice(&k[k.len() - 8..]);
            let mut vb = [0u8; 8];
            vb.copy_from_slice(&v);
            samples.push((Ts(u64::from_be_bytes(kb)), u64::from_be_bytes(vb)));
        }
    }
    drop(view);
    assert_eq!(samples.len(), 2, "one sample per clock second: {samples:?}");
    assert!(samples.iter().all(|(_, s)| *s == 100 || *s == 101));
    // The sample sits at its commit ts, in the commit batch.
    assert!(samples[0].0 < samples[1].0);
    let key0 = sys_ts_clock_key(samples[0].0);
    assert!(ok(core.latest_get(&key0)).is_some());
}

#[test]
fn commit_observer_runs_before_any_status_is_set() {
    // §3 step 3: on_assigned runs for ssi requests before step 4 sets any
    // status. The observer samples the core from inside the call (the
    // manual pipeline runs process_group on this thread).
    struct Rec2 {
        core: Arc<Core<RecKv>>,
        seen: Mutex<Vec<(nucleus_txn::TxnId, Ts, TxnStatus, Ts)>>,
    }
    impl CommitObserver for Rec2 {
        fn on_assigned(&self, txn: nucleus_txn::TxnId, ts: Ts) {
            let entry = self.core.status.entry(txn);
            let seen = self.core.visible_ts();
            self.seen.lock().expect("seen").push((
                txn,
                ts,
                entry.map(|e| e.status).unwrap_or(TxnStatus::Pending),
                seen,
            ));
        }
    }

    let core = Arc::new(ok(Core::open(RecKv::new())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let obs = Arc::new(Rec2 {
        core: Arc::clone(&core),
        seen: Mutex::new(Vec::new()),
    });
    pipeline.set_observer(Arc::clone(&obs) as Arc<dyn CommitObserver>);

    let ssi_txn = core.begin(Isolation::Serializable);
    let plain_txn = core.begin(Isolation::ReadCommitted);
    let (r1, a1) = CommitRequest::new(
        ssi_txn.id,
        SyncCommit::On,
        None,
        true,
        ssi_txn.write_set_keys(),
    );
    let (r2, a2) = CommitRequest::new(
        plain_txn.id,
        SyncCommit::On,
        None,
        false,
        plain_txn.write_set_keys(),
    );
    ok(core.submit(r1));
    ok(core.submit(r2));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(ok(a1.recv_timeout(Duration::from_secs(5))));
    ok(ok(a2.recv_timeout(Duration::from_secs(5))));

    let seen = obs.seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 1, "only ssi requests report: {seen:?}");
    let (txn, ts, status, visible) = seen[0];
    assert_eq!(txn, ssi_txn.id);
    assert_eq!(status, TxnStatus::Pending, "status not set yet");
    assert!(visible < ts, "visible_ts behind the assigned ts");
}

#[test]
fn fail_stop_recording_hook_stops_the_pipeline() {
    // A KV write error is fail-stop: the hook runs, the pipeline stops, the
    // request is error-acked (rework: an unacked request learns through an
    // error ack, not silence), and later groups are not processed.
    #[derive(Default)]
    struct Rec {
        errs: Mutex<Vec<String>>,
    }
    impl FailStop for Rec {
        fn on_kv_error(&self, err: &TxnError) {
            self.errs.lock().expect("errs").push(err.to_string());
        }
    }

    let (kv, fail) = FailKv::shared();
    let core = Arc::new(ok(Core::open(kv)));
    let hook = Arc::new(Rec::default());
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(Arc::clone(&hook) as Arc<dyn FailStop>),
    ));

    let txn = core.begin(Isolation::ReadCommitted);
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::Off, None, false, txn.write_set_keys());
    ok(core.submit(req));
    fail.store(true, Ordering::SeqCst);
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    assert_eq!(hook.errs.lock().expect("errs").len(), 1, "hook ran");
    assert!(
        matches!(ack.recv_timeout(Duration::from_millis(200)), Ok(Err(_))),
        "the unacked request gets an error ack"
    );
    // The pipeline is stopped: another group is a no-op even with the KV
    // healthy again.
    fail.store(false, Ordering::SeqCst);
    let txn2 = core.begin(Isolation::ReadCommitted);
    let (req2, ack2) =
        CommitRequest::new(txn2.id, SyncCommit::Off, None, false, txn2.write_set_keys());
    ok(core.submit(req2));
    let group2 = pipeline.drain_available();
    pipeline.process_group(group2);
    assert!(ack2.recv_timeout(Duration::from_millis(200)).is_err());
    // Dropping the pipeline disconnects the channel: Core::commit reports
    // the stop instead of hanging.
    drop(pipeline);
    let txn3 = core.begin(Isolation::ReadCommitted);
    txn3.log_write(ok(txn3.next_seq()), b"k");
    match core.commit(txn3, SyncCommit::Off) {
        Err(TxnError::Invariant(msg)) => assert!(msg.contains("stopped"), "{msg}"),
        other => panic!("expected invariant error, got {other:?}"),
    }
}

#[test]
fn fail_stop_panicking_hook_surfaces_at_shutdown() {
    struct Panic;
    impl FailStop for Panic {
        fn on_kv_error(&self, _err: &TxnError) {
            panic!("injected fail-stop panic");
        }
    }

    let (kv, fail) = FailKv::shared();
    let core = Arc::new(ok(Core::open(kv)));
    let handle = ok(nucleus_txn::commit::spawn_commit_thread_with(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(Arc::new(Panic) as Arc<dyn FailStop>),
    ));
    fail.store(true, Ordering::SeqCst);
    let committer = {
        let core = Arc::clone(&core);
        std::thread::spawn(move || {
            let txn = core.begin(Isolation::ReadCommitted);
            txn.log_write(ok(txn.next_seq()), b"k");
            core.commit(txn, SyncCommit::Off)
        })
    };
    let res = committer.join().expect("committer thread");
    assert!(matches!(res, Err(TxnError::Invariant(_))), "{res:?}");
    match handle.shutdown() {
        Err(TxnError::Invariant(msg)) => assert!(msg.contains("panicked"), "{msg}"),
        other => panic!("expected the panic to surface, got {other:?}"),
    }
}

#[test]
fn no_writes_and_ser_committed_with_a_small_configured_hwm_block() {
    // A SERIALIZABLE txn with an empty write set still goes through the
    // pipeline, and the first commit reserves /sys/ts_hwm in a block in the
    // same batch. The block here is the *configured* one (8), asserted as
    // a literal so a mutant that hard-codes the reservation (e.g.
    // `TS_HWM_BLOCK = 1`) cannot pass by reading the constant back.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    pipeline.set_hwm_block(8);
    assert_eq!(pipeline.next_ts(), Ts(1));
    let txn = core.begin(Isolation::Serializable);
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::On, None, true, txn.write_set_keys());
    ok(core.submit(req));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    let ts = ok(ok(ack.recv_timeout(Duration::from_secs(5))));
    assert_eq!(ts, Ts(1));
    assert_eq!(pipeline.next_ts(), Ts(2));
    assert_eq!(
        ok(core.latest_get(&sys_ts_hwm_key())),
        Some(8u64.to_be_bytes().to_vec()),
        "hwm reserved in the configured block"
    );
}

#[test]
fn the_default_hwm_block_is_1024() {
    // Pins the production reservation block: one commit through the
    // default pipeline must reserve exactly 1024 timestamps (asserted as a
    // literal — reading TS_HWM_BLOCK back would be tautological).
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    assert_eq!(TS_HWM_BLOCK, 1024);
    let txn = core.begin(Isolation::Serializable); // empty write set still commits
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::Off, None, true, txn.write_set_keys());
    ok(core.submit(req));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(ok(ack.recv_timeout(Duration::from_secs(5))));
    assert_eq!(
        ok(core.latest_get(&sys_ts_hwm_key())),
        Some(1024u64.to_be_bytes().to_vec()),
        "the default reservation is one block of 1024"
    );
}

#[test]
fn commit_thread_drains_greedily_and_shuts_down() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core)));
    let mut ts_all = Vec::new();
    let start = Instant::now();
    for i in 0..50 {
        let txn = core.begin(Isolation::ReadCommitted);
        txn.log_write(ok(txn.next_seq()), b"k");
        let ts = ok(core.commit(
            txn,
            if i % 2 == 0 {
                SyncCommit::On
            } else {
                SyncCommit::Off
            },
        ));
        ts_all.push(ts);
    }
    assert!(start.elapsed() < Duration::from_secs(10));
    let mut sorted = ts_all.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), ts_all.len());
    ok(handle.shutdown());
    // After shutdown, new sends are refused rather than hanging.
    let txn = core.begin(Isolation::ReadCommitted);
    txn.log_write(ok(txn.next_seq()), b"k");
    assert!(core.commit(txn, SyncCommit::Off).is_err());
}

#[test]
fn commit_observer_and_clock_config_reach_a_spawned_thread() {
    // The CommitConfig path (rework item 11): the clock reaches a spawned
    // pipeline, so its ts_clock samples use the injected time.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let clock = FakeClock::new(7);
    let handle = ok(nucleus_txn::commit::spawn_commit_thread_with(
        Arc::clone(&core),
        CommitConfig::new().with_clock(clock),
    ));
    let txn = core.begin(Isolation::ReadCommitted);
    place_intent(&core, &txn, b"/t/1/r", b"v");
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert!(ts > Ts::ZERO);
    assert_eq!(
        ok(core.latest_get(&sys_ts_clock_key(ts))),
        Some(7u64.to_be_bytes().to_vec()),
        "the configured clock sampled"
    );
    ok(handle.shutdown());
}
