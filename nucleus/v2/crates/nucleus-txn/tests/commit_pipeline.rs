//! C-T1b tests: the §3 commit pipeline — pipeline order (I-VIS, I-ACK,
//! I-WAL-ORDER), the mixed off/on/off group, the G0-commit seed regressions
//! 7, 8 and 9, `/sys/ts_clock` sampling, the `CommitObserver` hook, and
//! fail-stop.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ok, place_intent, Event, FailKv, FakeClock, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitObserver, CommitPipeline, CommitRequest, FailStop, SyncCommit};
use nucleus_txn::encoding::{
    intent_key, parse_sys_txn_key, sys_log_key, sys_ts_clock_key, sys_txn_key, sys_txn_prefix,
    sys_txn_prefix_end,
};
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnError, TxnStatus};

/// A txn committed through the real API, for the order tests.
fn commit_one(core: &Arc<Core<RecKv>>, i: usize, sync: SyncCommit) -> (nucleus_txn::TxnId, Ts) {
    let txn = core.begin(Isolation::ReadCommitted);
    let key = format!("/t/1/r{i}");
    place_intent(core, &txn, key.as_bytes(), format!("v{i}").as_bytes());
    txn.log_write(txn.next_seq(), key.as_bytes()); // a second layer's entry
    let ts = ok(core.commit(&txn, sync));
    (txn.id, ts)
}

#[test]
fn pipeline_order_i_vis_and_i_ack() {
    // I-VIS and I-ACK as a poller regression (the G0 model proves them
    // exhaustively; this catches a pipeline that advances visible_ts before
    // the status or acks early). A record visible in a KV view plus a still
    // Pending status plus visible_ts >= ts is a violation in any sample.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core));
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
    let mut pipeline = CommitPipeline::new(Arc::clone(&core));

    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    let t3 = core.begin(Isolation::ReadCommitted);
    for (txn, key) in [(&t1, "a"), (&t2, "b"), (&t3, "c")] {
        place_intent(&core, txn, key.as_bytes(), b"v");
        core.register_write_set(txn);
    }
    let (r1, a1) = CommitRequest::new(t1.id, SyncCommit::Off, None, false);
    let (r2, a2) = CommitRequest::new(t2.id, SyncCommit::On, Some(b"log2".to_vec()), false);
    let (r3, a3) = CommitRequest::new(t3.id, SyncCommit::Off, None, false);
    ok(core.send_commit(r1));
    ok(core.send_commit(r2));
    ok(core.send_commit(r3));
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 3);
    pipeline.process_group(group);

    let ts1 = ok(a1.recv_timeout(Duration::from_secs(5)));
    let ts2 = ok(a2.recv_timeout(Duration::from_secs(5)));
    let ts3 = ok(a3.recv_timeout(Duration::from_secs(5)));
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
    let mut pipeline = CommitPipeline::new(Arc::clone(&core));
    let t1 = core.begin(Isolation::ReadCommitted);
    let t2 = core.begin(Isolation::ReadCommitted);
    for txn in [&t1, &t2] {
        place_intent(&core, txn, b"k", b"v");
        core.register_write_set(txn);
    }
    let (r1, a1) = CommitRequest::new(t1.id, SyncCommit::Off, None, false);
    let (r2, a2) = CommitRequest::new(t2.id, SyncCommit::Off, None, false);
    ok(core.send_commit(r1));
    ok(core.send_commit(r2));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(a1.recv_timeout(Duration::from_secs(5)));
    ok(a2.recv_timeout(Duration::from_secs(5)));
    assert!(
        !kv.log().iter().any(|e| matches!(e, Event::Sync)),
        "a group of only off requests does not fsync"
    );
}

#[test]
fn seed7_visible_before_status_is_never_observed() {
    // Seed 7: `visible_ts` advanced before status is set. Tighter poller
    // than `pipeline_order_i_vis_and_i_ack`: a single off commit, watched
    // from the moment its record exists.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core));
    let txn = core.begin(Isolation::ReadCommitted);
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
                let entry = core.status.entry(txn.id);
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
    let ts = ok(core.commit(&txn, SyncCommit::Off));
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
    // Seed 8: ack before fsync for synchronous_commit=on. An acked On
    // commit followed by a crash that drops every unsynced batch must
    // survive (I-DURABLE); if the ack ran before the fsync, the record is
    // still unsynced and the crash eats it.
    use nucleus_kv::fault::Fault;
    use nucleus_kv::MemKv;
    let kv = Fault::new(MemKv::new(), || Ok(MemKv::new()));
    let core = Arc::new(ok(Core::open(kv)));
    let handle = nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core));
    let mut acked = Vec::new();
    for i in 0..4 {
        let txn = core.begin(Isolation::ReadCommitted);
        place_intent(&core, &txn, format!("/t/1/r{i}").as_bytes(), b"v");
        let ts = ok(core.commit(&txn, SyncCommit::On));
        acked.push((txn.id, ts));
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
fn seed9_records_and_intents_stay_in_wal_order() {
    // Seed 9: commit records written out of commit_ts order. The recording
    // KV wrapper checks WAL order: /sys/txn records appear in strictly
    // increasing ts order, and every intent batch of a txn precedes its
    // record (I-WAL-ORDER).
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let handle = nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core));
    let mut committed = Vec::new();
    for i in 0..12u32 {
        let sync = if i % 2 == 0 {
            SyncCommit::On
        } else {
            SyncCommit::Off
        };
        let (id, ts) = commit_one(&core, i as usize, sync);
        committed.push((id, ts, format!("/t/1/r{i}")));
    }
    ok(handle.shutdown());

    let log = kv.log();
    // Records in strictly increasing ts order.
    let mut last: Option<Ts> = None;
    for e in &log {
        if let Event::Write(ops) = e {
            for op in ops {
                if let nucleus_kv::Op::Put(k, v) = op {
                    if let Some(_id) = parse_sys_txn_key(k) {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(v);
                        let ts = Ts(u64::from_be_bytes(b));
                        if let Some(prev) = last {
                            assert!(
                                ts > prev,
                                "I-WAL-ORDER: record {ts:?} written after {prev:?}"
                            );
                        }
                        last = Some(ts);
                    }
                }
            }
        }
    }
    assert_eq!(last, committed.last().map(|(_, t, _)| *t));
    // Every intent write precedes its txn's record.
    for (id, _ts, key) in &committed {
        let ik = intent_key(key.as_bytes());
        let rk = sys_txn_key(*id);
        let intent_pos = log.iter().position(|e| matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Put(k, _) if k == &ik))));
        let record_pos = log.iter().position(|e| matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Put(k, _) if k == &rk))));
        assert!(intent_pos.is_some() && record_pos.is_some());
        assert!(
            intent_pos < record_pos,
            "I-WAL-ORDER: intent of {id:?} written after its record"
        );
    }
}

#[test]
fn ts_clock_samples_at_most_once_per_clock_second() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let clock = FakeClock::new(100);
    let mut pipeline = CommitPipeline::new(Arc::clone(&core));
    pipeline.set_clock(clock.clone());
    for i in 0..3 {
        let txn = core.begin(Isolation::ReadCommitted);
        txn.log_write(txn.next_seq(), b"/t/1/r"); // a write, so no fast path
        core.register_write_set(&txn);
        let (req, ack) = CommitRequest::new(txn.id, SyncCommit::Off, None, false);
        ok(core.send_commit(req));
        let group = pipeline.drain_available();
        pipeline.process_group(group);
        ok(ack.recv_timeout(Duration::from_secs(5)));
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
    let mut pipeline = CommitPipeline::new(Arc::clone(&core));
    let obs = Arc::new(Rec2 {
        core: Arc::clone(&core),
        seen: Mutex::new(Vec::new()),
    });
    pipeline.set_observer(Arc::clone(&obs) as Arc<dyn CommitObserver>);

    let ssi_txn = core.begin(Isolation::Serializable);
    let plain_txn = core.begin(Isolation::ReadCommitted);
    let (r1, a1) = CommitRequest::new(ssi_txn.id, SyncCommit::On, None, true);
    let (r2, a2) = CommitRequest::new(plain_txn.id, SyncCommit::On, None, false);
    ok(core.send_commit(r1));
    ok(core.send_commit(r2));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ok(a1.recv_timeout(Duration::from_secs(5)));
    ok(a2.recv_timeout(Duration::from_secs(5)));

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
    // request is never acked, and later groups are not processed.
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
    let mut pipeline = CommitPipeline::new(Arc::clone(&core));
    let hook = Arc::new(Rec::default());
    pipeline.set_fail_stop(Arc::clone(&hook) as Arc<dyn FailStop>);

    let txn = core.begin(Isolation::ReadCommitted);
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::Off, None, false);
    ok(core.send_commit(req));
    fail.store(true, Ordering::SeqCst);
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    assert_eq!(hook.errs.lock().expect("errs").len(), 1, "hook ran");
    assert!(
        ack.recv_timeout(Duration::from_millis(200)).is_err(),
        "no ack after fail-stop"
    );
    // The pipeline is stopped: another group is a no-op even with the KV
    // healthy again.
    fail.store(false, Ordering::SeqCst);
    let txn2 = core.begin(Isolation::ReadCommitted);
    let (req2, ack2) = CommitRequest::new(txn2.id, SyncCommit::Off, None, false);
    ok(core.send_commit(req2));
    let group2 = pipeline.drain_available();
    pipeline.process_group(group2);
    assert!(ack2.recv_timeout(Duration::from_millis(200)).is_err());
    // Dropping the pipeline disconnects the channel: Core::commit reports
    // the stop instead of hanging.
    drop(pipeline);
    let txn3 = core.begin(Isolation::ReadCommitted);
    txn3.log_write(txn3.next_seq(), b"k");
    match core.commit(&txn3, SyncCommit::Off) {
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
    let handle = nucleus_txn::commit::spawn_commit_thread_with(
        Arc::clone(&core),
        Arc::new(Panic) as Arc<dyn FailStop>,
    );
    fail.store(true, Ordering::SeqCst);
    let committer = {
        let core = Arc::clone(&core);
        std::thread::spawn(move || {
            let txn = core.begin(Isolation::ReadCommitted);
            txn.log_write(txn.next_seq(), b"k");
            core.commit(&txn, SyncCommit::Off)
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
fn no_writes_and_ser_committed_with_ts_and_hwm_reserved() {
    // A SERIALIZABLE txn with an empty write set still goes through the
    // pipeline, and the first commit reserves /sys/ts_hwm in a block in the
    // same batch.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let mut pipeline = CommitPipeline::new(Arc::clone(&core));
    assert_eq!(pipeline.next_ts(), Ts(1));
    let txn = core.begin(Isolation::Serializable);
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::On, None, true);
    ok(core.send_commit(req));
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    let ts = ok(ack.recv_timeout(Duration::from_secs(5)));
    assert_eq!(ts, Ts(1));
    assert_eq!(pipeline.next_ts(), Ts(2));
    assert_eq!(
        ok(core.latest_get(&nucleus_txn::encoding::sys_ts_hwm_key())),
        Some(
            Ts(nucleus_txn::commit::TS_HWM_BLOCK)
                .0
                .to_be_bytes()
                .to_vec()
        ),
        "hwm reserved in a block"
    );
}

#[test]
fn commit_thread_drains_greedily_and_shuts_down() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = nucleus_txn::commit::spawn_commit_thread(Arc::clone(&core));
    let mut ts_all = Vec::new();
    let start = Instant::now();
    for i in 0..50 {
        let txn = core.begin(Isolation::ReadCommitted);
        txn.log_write(txn.next_seq(), b"k");
        let ts = ok(core.commit(
            &txn,
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
    txn.log_write(txn.next_seq(), b"k");
    assert!(core.commit(&txn, SyncCommit::Off).is_err());
}
