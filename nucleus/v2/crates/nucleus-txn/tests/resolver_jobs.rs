//! C-T1b tests: the resolution and truncation jobs (a minimal C-B1), with
//! the G0-commit seed regressions 10, 13 and 23 — truncation and count
//! races, via the resolver and inline removal interleaved with open views.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ok, place_intent, Event, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::encoding::{intent_key, version_key};
use nucleus_txn::removal::{remove_intent, RemovalMode, RemovalOutcome};
use nucleus_txn::resolver::{spawn_background, Resolver};
use nucleus_txn::status::Remembered;
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnStatus};

/// Commits a txn with one hand-placed intent through the real pipeline.
fn commit_one(
    core: &Arc<Core<RecKv>>,
    key: &[u8],
    value: &[u8],
    sync: SyncCommit,
) -> (nucleus_txn::TxnId, Ts) {
    let txn = core.begin(Isolation::ReadCommitted);
    place_intent(core, &txn, key, value);
    let ts = ok(core.commit(&txn, sync));
    (txn.id, ts)
}

#[test]
fn resolver_resolves_versions_and_truncates() {
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let (id, ts) = commit_one(&core, b"/t/1/r", b"v1", SyncCommit::On);
    // Queued in step 5; the intent is still there right after the ack.
    assert_eq!(core.status.entry(id).map(|e| e.intent_count), Some(1));
    assert!(ok(core.latest_get(&intent_key(b"/t/1/r"))).is_some());

    let n = ok(Resolver::run_once(&core));
    assert_eq!(n, 1);
    assert!(ok(core.latest_get(&intent_key(b"/t/1/r"))).is_none());
    assert_eq!(
        ok(core.latest_get(&version_key(b"/t/1/r", ts))),
        nucleus_txn::encoding::encode_version(&nucleus_txn::LayerData::Write {
            value: b"v1".to_vec(),
            key_changed: false,
        })
    );
    // Second round: nothing queued, entry now truncated (no views open).
    let n2 = ok(Resolver::run_once(&core));
    assert_eq!(n2, 0);
    assert_eq!(core.status.lookup_remembered(id), Remembered::Ended);
    ok(handle.shutdown());

    // The resolution batch precedes the /sys/txn delete in the WAL.
    let log = kv.log();
    let resolve_pos = log.iter().position(|e| {
        matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Delete(k) if k == &intent_key(b"/t/1/r"))))
    });
    let delete_pos = log.iter().position(|e| {
        matches!(e, Event::Write(ops) if ops.iter().any(|op| matches!(op, nucleus_kv::Op::Delete(k) if k == &nucleus_txn::encoding::sys_txn_key(id))))
    });
    assert!(resolve_pos.is_some() && delete_pos.is_some());
    assert!(
        resolve_pos < delete_pos,
        "I-TRUNC: the /sys/txn delete follows the resolution batch"
    );
}

#[test]
fn seed10_no_truncation_before_resolution() {
    // Seed 10: the /sys/txn delete written before the resolution (truncation
    // ignoring the count). A committed txn with a live intent is never
    // truncated, however many views open and close; a view held across the
    // resolution keeps it past the count reaching 0; only then does it go.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let (id, ts) = commit_one(&core, b"/t/1/r", b"v", SyncCommit::Off);
    assert!(ts > Ts::ZERO);

    // Views open and close: condition 2 stops blocking, the count must not.
    for _ in 0..3 {
        let _view = core.open_view();
    }
    assert!(
        !ok(core.truncate_status(id)),
        "condition 1: an unresolved intent blocks truncation"
    );
    assert_eq!(
        core.status.lookup_remembered(id),
        Remembered::Live(TxnStatus::Committed(ts), 1)
    );

    // A view held open across the resolution: the count reaches 0, the
    // still-open view blocks (condition 2).
    {
        let _view = core.open_view();
        ok(Resolver::run_once(&core));
        let entry = core.status.entry(id);
        assert_eq!(entry.map(|e| e.intent_count), Some(0), "resolved");
        assert!(
            !ok(core.truncate_status(id)),
            "condition 2: a view open at the removal blocks"
        );
    }
    // Every view open at the removal has closed: truncation goes through.
    assert!(ok(core.truncate_status(id)));
    assert_eq!(core.status.lookup_remembered(id), Remembered::Ended);
    ok(handle.shutdown());
}

#[test]
fn seed13_no_decrement_on_a_noop_removal() {
    // Seed 13: `intent_count` decremented on a removal that wrote nothing.
    // The inline remover (§5.1) resolves the intent first; the resolver,
    // still holding the step-5 queue entry, must no-op without touching the
    // count again.
    let kv = RecKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let (id, _ts) = commit_one(&core, b"/t/1/r", b"v", SyncCommit::On);

    // A view open before the removal keeps §7.4 condition 2 blocking, so
    // the entry (and its count) stays observable across the noop round.
    let _view = core.open_view();
    let writes_before = kv.log().len();

    // The §5.1-style inline removal beats the resolver to it.
    assert_eq!(
        ok(remove_intent(
            &core,
            b"/t/1/r",
            None,
            id,
            RemovalMode::Resolve
        )),
        RemovalOutcome::Removed
    );
    assert_eq!(core.status.entry(id).map(|e| e.intent_count), Some(0));
    assert_eq!(
        kv.log().len(),
        writes_before + 1,
        "only the resolution batch"
    );

    let n = ok(Resolver::run_once(&core));
    assert_eq!(n, 1, "the queued entry was processed");
    assert_eq!(
        kv.log().len(),
        writes_before + 1,
        "the noop removal wrote nothing"
    );
    assert_eq!(
        core.status.entry(id).map(|e| e.intent_count),
        Some(0),
        "no underflow on a noop"
    );
    drop(_view);
    ok(Resolver::run_once(&core));
    assert!(core.status.entry(id).is_none(), "truncated");
    ok(handle.shutdown());
}

#[test]
fn seed23_counter_set_with_the_decrement_under_interleaving() {
    // Seed 23: `intent_count` decremented before `last_removal_counter` is
    // set. Both updates are one status-mutex critical section
    // (`removal_bookkeeping`), so any sampled entry either has the count
    // still up or the counter already set. A view is kept open throughout
    // so a legitimate zeroing always records a counter >= 1.
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(Mutex::new(None::<String>));
    let samples = Arc::new(Mutex::new(0u64));

    let ids: Arc<Mutex<Vec<nucleus_txn::TxnId>>> = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::<(nucleus_txn::TxnId, Ts)>::new()));

    // The permanent view (counter 1) that makes every legitimate removal
    // record a counter >= 1.
    let keeper = core.open_view();

    let writers = (0..4).map(|w| {
        let core = Arc::clone(&core);
        let ids = Arc::clone(&ids);
        let committed = Arc::clone(&committed);
        std::thread::spawn(move || -> Result<(), nucleus_txn::TxnError> {
            for i in 0..15 {
                let key = format!("/t/{w}/r{i}");
                let txn = core.begin(Isolation::ReadCommitted);
                place_intent(&core, &txn, key.as_bytes(), format!("v{w}-{i}").as_bytes());
                ids.lock().expect("ids").push(txn.id);
                let ts = core.commit(
                    &txn,
                    if (w + i) % 2 == 0 {
                        SyncCommit::On
                    } else {
                        SyncCommit::Off
                    },
                )?;
                committed.lock().expect("committed").push((txn.id, ts));
            }
            Ok(())
        })
    });
    for w in writers {
        ok(w.join().expect("writer"));
    }

    // Interleave: resolver rounds, truncation attempts and entry sampling
    // race while views open and close.
    let jobs = {
        let core = Arc::clone(&core);
        let stop = Arc::clone(&stop);
        let bad = Arc::clone(&bad);
        let samples = Arc::clone(&samples);
        let ids = Arc::clone(&ids);
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let view = core.open_view();
                for id in ids.lock().expect("ids").iter() {
                    if let Some(e) = core.status.entry(*id) {
                        *samples.lock().expect("samples") += 1;
                        if e.intent_count == 0 && e.last_removal_counter < 1 {
                            // Every sampled txn placed exactly one intent,
                            // and view 1 has been open the whole time, so a
                            // zeroed count must carry a recorded counter.
                            *bad.lock().expect("bad") = Some(format!(
                                "seed 23: {id:?} count 0 with counter {}",
                                e.last_removal_counter
                            ));
                            return;
                        }
                    }
                }
                drop(view);
                if let Err(e) = Resolver::run_once(&core) {
                    *bad.lock().expect("bad") = Some(format!("resolver: {e}"));
                    return;
                }
            }
        })
    };

    // Let the interleaving run against the background jobs too.
    {
        let core2 = Arc::clone(&core);
        let bg = spawn_background(core2);
        std::thread::sleep(Duration::from_millis(200));
        stop.store(true, Ordering::SeqCst);
        jobs.join().expect("jobs");
        ok(bg.stop());
    }
    drop(keeper);

    assert!(
        bad.lock().expect("bad").is_none(),
        "{:?}",
        bad.lock().expect("bad")
    );
    assert!(*samples.lock().expect("samples") > 100, "sampling ran");
    // Drain: every txn ends released, count 0, truncated.
    for _ in 0..50 {
        if ok(Resolver::run_once(&core)) == 0 {
            let pending = ids
                .lock()
                .expect("ids")
                .iter()
                .filter(|id| core.status.entry(**id).is_some())
                .count();
            if pending == 0 {
                break;
            }
        }
    }
    for id in ids.lock().expect("ids").iter() {
        assert!(
            core.status.entry(*id).is_none(),
            "{id:?} not truncated after the drain"
        );
    }
    ok(handle.shutdown());
}

#[test]
fn background_jobs_drain_automatically() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let bg = spawn_background(Arc::clone(&core));
    let mut committed = Vec::new();
    for i in 0..10 {
        let key = format!("/t/1/r{i}");
        let (id, ts) = commit_one(&core, key.as_bytes(), b"v", SyncCommit::On);
        committed.push((id, ts, key));
    }
    // The background thread resolves and truncates without help.
    let start = Instant::now();
    loop {
        if committed
            .iter()
            .all(|(id, _, _)| core.status.entry(*id).is_none())
        {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(5), "drain timed out");
        std::thread::sleep(Duration::from_millis(5));
    }
    for (_, _, key) in &committed {
        assert!(ok(core.latest_get(&intent_key(key.as_bytes()))).is_none());
    }
    ok(bg.stop());
    ok(handle.shutdown());
}

#[test]
fn aborted_txn_cleanup_discards_intents() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = spawn_commit_thread(Arc::clone(&core));
    let txn = core.begin(Isolation::ReadCommitted);
    place_intent(&core, &txn, b"/t/1/r", b"doomed");
    ok(core.abort(&txn));
    assert_eq!(
        core.status.entry(txn.id).map(|e| e.status),
        Some(TxnStatus::Aborted)
    );
    assert_eq!(core.status.entry(txn.id).map(|e| e.released), Some(true));
    // Nothing persisted for an abort; the cleanup is queued.
    assert!(ok(core.latest_get(&intent_key(b"/t/1/r"))).is_some());
    ok(Resolver::run_once(&core));
    assert!(ok(core.latest_get(&intent_key(b"/t/1/r"))).is_none());
    // No version was written.
    assert!(core.status.entry(txn.id).is_none(), "truncated");
    let view = core.open_view();
    assert!(ok(view.get(&version_key(b"/t/1/r", Ts(1)))).is_none());
    drop(view);
    ok(handle.shutdown());
}
