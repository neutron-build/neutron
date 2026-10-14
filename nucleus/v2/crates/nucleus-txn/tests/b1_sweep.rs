//! C-B1b boot-sweep tests (§7.2, §7.4): the walk removes every
//! older-epoch intent (data and `/sys/catalog/`) and records
//! `sweep_counter` only after the last removal, `/sys/txn` records stop
//! accumulating across reboots, the sweep takes the deferrable latch from
//! the latch-key source, and it runs concurrently with writers. The
//! C-B1bfix follow-ups add the boundary coverage: current-epoch intents
//! are never touched (M-1), non-catalog `/sys/` keys are never decoded
//! (M-2), a step's walk stays bounded across a `/sys/` span (L-5), and a
//! misbehaving latch-key source is refused loudly (N-5). Each test's doc
//! names the mutant it kills.

mod b1_support;
mod common;

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use b1_support::{ok, place_write, reopen_rig, B1Rig};
use nucleus_kv::{Batch, Durability, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::Catalog;
use nucleus_txn::encoding::{
    intent_key, parse_key, sys_txn_key, sys_txn_prefix, sys_txn_prefix_end, Entry,
};
use nucleus_txn::jobs::{BootSweep, JobKind, JobOutcome, JobPoint, JobProbe, LatchKeySource};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::{Ts, TxnError, TxnId};

/// Every stored intent key in the whole keyspace (through a view).
fn all_intents<K: OrderedKv>(core: &Core<K>) -> Vec<Vec<u8>> {
    let view = core.open_view();
    let mut out = Vec::new();
    for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
        let (stored, _) = ok(row);
        if let Some((_, Entry::Intent)) = parse_key(&stored) {
            out.push(stored);
        }
    }
    out
}

/// The `/sys/txn` records, as raw keys.
fn txn_records<K: OrderedKv>(core: &Core<K>) -> Vec<Vec<u8>> {
    let view = core.open_view();
    view.scan(
        (
            Bound::Included(sys_txn_prefix().as_slice()),
            Bound::Excluded(sys_txn_prefix_end().as_slice()),
        ),
        false,
    )
    .map(ok)
    .map(|(k, _)| k)
    .collect()
}

/// Drives the sweep to `Finished` and returns the steps it took.
fn sweep_to_finish<K: OrderedKv>(sweep: &mut BootSweep<K>) -> usize {
    let mut steps = 0;
    loop {
        steps += 1;
        match ok(sweep.run_once()) {
            JobOutcome::Finished => return steps,
            JobOutcome::Progress => {}
            JobOutcome::Idle => panic!("the sweep is never Idle"),
        }
        assert!(steps < 10_000, "the sweep did not finish");
    }
}

/// SW-CAT (the sweep skips `/sys/catalog/`), SW-CHUNK (the resume key
/// skips one entry at a chunk boundary) and SW-MODE (`Discard` for a
/// `Committed` owner): a crash leaves committed-unresolved and pending
/// intents — including a committed-unresolved CREATE TABLE and a pending
/// DDL under `/sys/catalog/` — and after the reopen the sweep (chunk 2)
/// removes every one of them, the committed values still read,
/// `sweep_counter` is set, and a resolver round with no views open
/// empties `/sys/txn`.
#[test]
fn sweep_removes_every_older_epoch_intent() {
    let rig = B1Rig::new();
    let core = &rig.core;

    // A committed-unresolved CREATE TABLE (the resolver never runs).
    let t1 = rig.rc();
    let (oid_a, sid_a) = rig.create_table(&t1, b"A");
    let a_ts = ok(core.commit(t1, nucleus_txn::commit::SyncCommit::On));
    assert!(a_ts > Ts::ZERO);

    // Committed-unresolved data under the table's real storage range, and
    // on a literal key outside every catalog prefix.
    let t2 = rig.rc();
    let k_real = b1_support::tkey(sid_a, "r0");
    place_write(core, &t2, &k_real, b"real");
    place_write(core, &t2, b"/t/1/lit", b"lit");
    ok(core.commit(t2, nucleus_txn::commit::SyncCommit::On));

    // A pending DDL (never committed): its catalog-row intent stays.
    let t3 = rig.rc();
    let _ = rig.create_table(&t3, b"C");
    drop(t3);
    // A pending data write.
    let t4 = rig.rc();
    place_write(core, &t4, b"/t/1/pend", b"pend");
    drop(t4);

    let intents_before = all_intents(core);
    assert!(intents_before.len() >= 5, "the workload left intents");

    // Crash keeping everything, reopen: every intent above is now an
    // older-epoch intent (committed → Resolve, pending → Aborted).
    let kv = rig.kv.clone();
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2 = Arc::clone(&rig2.core);

    // The sweep with chunk 2 (several steps over the intents).
    let mut sweep = BootSweep::new(Arc::clone(&core2)).with_chunk(2);
    let steps = sweep_to_finish(&mut sweep);
    assert!(
        steps >= 2,
        "chunk 2 over {} intents takes several steps",
        intents_before.len()
    );

    assert!(
        all_intents(&core2).is_empty(),
        "no intent remains after the sweep: {:?}",
        all_intents(&core2)
    );
    assert!(
        core2.registry.sweep_counter().is_some(),
        "sweep_counter is set when the walk completes"
    );

    // The committed values read (the committed CREATE TABLE row and both
    // data writes resolve into versions).
    let row = {
        let view = core2.open_view();
        let ctx = nucleus_txn::visibility::ReadCtx {
            txn: TxnId {
                epoch: core2.epoch(),
                n: u64::MAX,
            },
            snapshot: core2.visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::read_rel(&core2, &view, &ctx, oid_a))
    };
    let row = row.expect("the committed CREATE TABLE's row reads after the sweep");
    assert_eq!(row.storage_id, sid_a);
    assert_eq!(
        rig2.read_at(&k_real, core2.visible_ts()),
        Some(b"real".to_vec()),
        "the committed write under the table's storage range reads"
    );
    assert_eq!(
        rig2.read_at(b"/t/1/lit", core2.visible_ts()),
        Some(b"lit".to_vec())
    );
    assert_eq!(rig2.read_at(b"/t/1/pend", core2.visible_ts()), None);

    // A resolver round with no views open truncates every older-epoch
    // record: the /sys/txn scan is empty.
    ok(Resolver::run_once(&core2));
    let left = txn_records(&core2);
    assert!(left.is_empty(), "/sys/txn records remain: {left:?}");
}

/// SW-EARLY (`record_sweep` at the first step): with chunk 1, at the
/// first `SweepAfterRemoval` the probe runs a resolver round and then
/// kills keeping everything. With the counter recorded only after the
/// last removal, the still-unwalked committed txn's record cannot
/// truncate (§7.4 condition 1 fails), so after the reopen both committed
/// values read. With the mutant, the resolver truncates the unwalked
/// txn's record while its intent lives, the kill keeps the `/sys/txn`
/// delete, and the reopened core reads the intent's owner as Aborted:
/// the committed value is gone.
#[test]
fn sweep_records_only_after_the_last_removal() {
    let rig = B1Rig::new();
    let core = &rig.core;

    // Two committed-unresolved txns on distinct keys: the sweep (chunk 1)
    // removes them in different steps, so at the first removal the second
    // intent is still unwalked.
    let t1 = rig.rc();
    place_write(core, &t1, b"/t/1/k1", b"one");
    ok(core.commit(t1, nucleus_txn::commit::SyncCommit::On));
    let t2 = rig.rc();
    place_write(core, &t2, b"/t/1/k2", b"two");
    ok(core.commit(t2, nucleus_txn::commit::SyncCommit::On));

    let kv = rig.kv.clone();
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2 = Arc::clone(&rig2.core);

    /// At the first SweepAfterRemoval: run one resolver round (the
    /// truncation pass is where an early sweep_counter would bite),
    /// then kill keeping every unsynced batch.
    struct ProbeAndKill<K: OrderedKv> {
        core: Arc<Core<K>>,
        kv: b1_support::KillKv<nucleus_kv::MemKv>,
        fired: AtomicBool,
    }
    impl<K: OrderedKv> JobProbe for ProbeAndKill<K> {
        fn at(&self, point: JobPoint) {
            if point == JobPoint::SweepAfterRemoval && !self.fired.swap(true, Ordering::SeqCst) {
                ok(Resolver::run_once(&self.core));
                self.kv.kill(usize::MAX);
            }
        }
    }
    core2.set_job_probe(Arc::new(ProbeAndKill {
        core: Arc::clone(&core2),
        kv: kv.clone(),
        fired: AtomicBool::new(false),
    }));

    // Drive chunk-1 steps until the one that removes k1's intent (the
    // probe fires inside it). The steps before it are skip-budget steps
    // across the non-catalog /sys/ keys (epoch, the two records), which
    // cost the chunk like any entry (C-B1b gate L-5) and remove nothing.
    // The step the probe fires in still returns Progress; nothing else is
    // driven — the KV is dead from here on.
    let mut sweep = BootSweep::new(Arc::clone(&core2)).with_chunk(1);
    let mut outcome = JobOutcome::Progress;
    let mut steps = 0;
    while !kv.killed() {
        steps += 1;
        assert!(steps < 10_000, "the sweep never reached k1's intent");
        outcome = ok(sweep.run_once());
    }
    drop(sweep);
    assert!(
        matches!(outcome, JobOutcome::Progress),
        "the probe's step still has the second txn ahead: {outcome:?}"
    );
    assert!(kv.killed(), "the probe killed the KV");

    let rig3 = reopen_rig(rig2);
    let visible = rig3.core.visible_ts();
    assert_eq!(
        rig3.read_at(b"/t/1/k1", visible),
        Some(b"one".to_vec()),
        "the swept txn's committed value is intact"
    );
    assert_eq!(
        rig3.read_at(b"/t/1/k2", visible),
        Some(b"two".to_vec()),
        "the not-yet-swept txn's committed value is intact (its record was \
         not truncated while its intent lived)"
    );
}

/// SW-LATCH (the source ignored; the key latched instead): the source
/// returns a prefix for one older-epoch `/i/` intent; with the test
/// holding `core.latches.lock(prefix)`, the sweep step on another thread
/// does not remove the intent within 300 ms, and does once the latch is
/// released.
#[test]
fn sweep_takes_the_deferrable_latch() {
    // An older-epoch intent on a deferrable-shaped key, via a plain
    // reopen (MemKv keeps every write; the epoch still bumps).
    let kv = nucleus_kv::MemKv::new();
    let core1 = ok(Core::open(kv));
    let txn = core1.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    place_write(&core1, &txn, b"/i/9/abpk", b"v");
    drop(txn);
    let kv = core1.into_kv();
    let core2 = Arc::new(ok(Core::open(kv)));

    /// Answers a prefix for the one deferrable entry of the test.
    struct OneDeferrable;
    impl LatchKeySource for OneDeferrable {
        fn deferrable_prefix(&self, key: &[u8]) -> Option<usize> {
            if key == b"/i/9/abpk" {
                Some(7) // "/i/9/ab"
            } else {
                None
            }
        }
    }
    core2.set_latch_key_source(Arc::new(OneDeferrable));

    // Hold the prefix latch on this thread; the sweep's removal of the
    // entry must block on it.
    let latch = core2.latches.lock(b"/i/9/ab");
    let step_result: Arc<Mutex<Option<Result<JobOutcome, nucleus_txn::TxnError>>>> =
        Arc::new(Mutex::new(None));
    let worker = {
        let core2 = Arc::clone(&core2);
        let step_result = Arc::clone(&step_result);
        std::thread::spawn(move || {
            let mut sweep = BootSweep::new(Arc::clone(&core2));
            let r = sweep.run_once();
            *step_result.lock().expect("step result") = Some(r);
        })
    };
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        step_result.lock().expect("step result").is_none(),
        "the sweep step must block on the deferrable prefix latch"
    );
    assert!(
        ok(core2.latest_get(&nucleus_txn::encoding::intent_key(b"/i/9/abpk"))).is_some(),
        "the intent is still there while the latch is held"
    );
    drop(latch);
    worker.join().expect("sweep worker");
    assert!(
        matches!(
            step_result.lock().expect("step result").as_ref(),
            Some(Ok(JobOutcome::Finished))
        ),
        "the step finished after the release"
    );
    assert!(
        ok(core2.latest_get(&nucleus_txn::encoding::intent_key(b"/i/9/abpk"))).is_none(),
        "the sweep removed the intent once the latch was released"
    );
}

/// SW-NONE (never `record_sweep`): three reopen cycles, each with a sweep
/// and a resolver round, leave `/sys/txn` empty every time. With the
/// mutant the older-epoch records never truncate and pile up.
#[test]
fn txn_records_do_not_accumulate_across_reboots() {
    let mut rig = B1Rig::new();
    for cycle in 0..3 {
        let core = &rig.core;
        // One committed-unresolved write (its record must truncate after
        // this cycle's sweep + resolver) and one pending write.
        let t = rig.rc();
        place_write(core, &t, format!("/t/1/c{cycle}").as_bytes(), b"v");
        ok(core.commit(t, nucleus_txn::commit::SyncCommit::On));
        let p = rig.rc();
        place_write(core, &p, format!("/t/1/p{cycle}").as_bytes(), b"p");
        drop(p);

        let kv = rig.kv.clone();
        kv.kill(usize::MAX);
        rig = reopen_rig(rig);
        let core2 = Arc::clone(&rig.core);
        let mut sweep = BootSweep::new(Arc::clone(&core2));
        sweep_to_finish(&mut sweep);
        ok(Resolver::run_once(&core2));
        let left = txn_records(&core2);
        assert!(
            left.is_empty(),
            "cycle {cycle}: /sys/txn records accumulated: {left:?}"
        );
        drop(core2);
        // Reopen into the next cycle (the truncation delete is unsynced
        // but kept by kill-everything).
        let kv = rig.kv.clone();
        kv.kill(usize::MAX);
        rig = reopen_rig(rig);
    }
}

/// The sweep runs concurrently with writers (§7.2): a JobSet with the
/// sweep, the resolver and a commit thread, while four writer threads —
/// started **before** the sweep, so the walk is live under current-epoch
/// writers from its first step — update keys that carry older-epoch
/// intents. The sweep's removals no-op on re-owned intents (§7.3 step 2)
/// and the walk itself must skip every current-epoch intent it meets
/// (a Pending one is never its business, §7.2); at the end no intent
/// remains and `common::assert_count_exact` holds for a fresh writer.
#[test]
fn sweep_runs_concurrently_with_writers() {
    let rig = B1Rig::new();
    let core = &rig.core;
    // Older-epoch intents on four keys: committed-unresolved (the
    // resolver is not running yet in this core).
    for i in 0..4u64 {
        let t = rig.rc();
        place_write(core, &t, format!("/t/1/w{i}").as_bytes(), b"old");
        ok(core.commit(t, nucleus_txn::commit::SyncCommit::On));
    }
    let kv = rig.kv.clone();
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2 = Arc::clone(&rig2.core);

    // The set: sweep + resolver over the reopened core, plus the commit
    // thread the writers commit through. The writers start **before**
    // `set.start(BootSweep …)`, so the walk is genuinely live under them
    // (C-B1b gate M-1): a current-epoch intent is in the walk's face from
    // its first step, not merely after a one-shot walk already finished.
    let mut set = nucleus_txn::jobs::JobSet::new(Arc::clone(&core2));
    set.attach_commit_thread(rig2.commit);

    // Four writer threads, each hammering its own key.
    let mut writers = Vec::new();
    for i in 0..4u64 {
        let core2 = Arc::clone(&core2);
        writers.push(std::thread::spawn(move || {
            for j in 0..50u64 {
                let txn = core2.begin(nucleus_txn::txn::Isolation::ReadCommitted);
                place_write(
                    &core2,
                    &txn,
                    format!("/t/1/w{i}").as_bytes(),
                    format!("new{i}-{j}").as_bytes(),
                );
                let r = core2.commit(txn, nucleus_txn::commit::SyncCommit::On);
                assert!(r.is_ok(), "writer commit failed: {r:?}");
            }
        }));
    }

    ok(set.start(
        Box::new(BootSweep::new(Arc::clone(&core2))),
        nucleus_txn::jobs::JobConfig::default_for(nucleus_txn::jobs::JobKind::BootSweep),
    ));
    ok(set.start(
        Box::new(nucleus_txn::resolver::ResolverJob::new(Arc::clone(&core2))),
        nucleus_txn::jobs::JobConfig::default_for(nucleus_txn::jobs::JobKind::Resolver),
    ));
    for w in writers {
        w.join().expect("writer");
    }

    // The sweep finishes and the resolver drains the writers: no intent
    // remains anywhere (bounded wait; a lost removal hangs here).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !all_intents(&core2).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "intents remain after the writers stopped: {:?}",
            all_intents(&core2)
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        core2.registry.sweep_counter().is_some(),
        "the sweep finished under the writers"
    );

    // I-COUNT holds for a fresh txn after the storm (still with the
    // commit thread attached).
    let t = core2.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    place_write(&core2, &t, b"/t/1/after", b"v");
    common::assert_count_exact(&core2, &t);
    ok(core2.commit(t, nucleus_txn::commit::SyncCommit::On));
    ok(set.shutdown());
}

/// IMM-EPOCH (the epoch test becomes `<=`, so the sweep treats
/// current-epoch intents as old): on a reopened core, before the sweep
/// starts, the new epoch holds a pending intent and a
/// committed-but-unresolved one. The walk must reach `Finished` without
/// touching either — a pending owner would hit the `other =>` arm of the
/// mode decision (a loud `Invariant`, fail-stop of the whole set) and a
/// committed-not-resolved one would be resolved out of band — and it must
/// still remove exactly the older-epoch intents (C-B1b gate M-1).
#[test]
fn sweep_leaves_current_epoch_intents_alone() {
    let rig = B1Rig::new();
    let core = &rig.core;
    // One committed-unresolved write: an older-epoch intent once the core
    // is reopened (its record makes it a visible commit at boot, §7.2).
    let t1 = rig.rc();
    place_write(core, &t1, b"/t/1/old", b"old");
    ok(core.commit(t1, nucleus_txn::commit::SyncCommit::On));

    let kv = rig.kv.clone();
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2 = Arc::clone(&rig2.core);

    // The new epoch's two boundary cases, in place before the sweep
    // starts. Pending: begun and dropped without commit or abort.
    let p = rig2.rc();
    place_write(&core2, &p, b"/t/1/pend2", b"p2");
    drop(p);
    // Committed but unresolved: acked, its resolution still queued (no
    // resolver job runs in this test until the assert phase).
    let c = rig2.rc();
    place_write(&core2, &c, b"/t/1/cur2", b"c2");
    ok(core2.commit(c, nucleus_txn::commit::SyncCommit::On));

    let k_pend = intent_key(b"/t/1/pend2");
    let k_cur = intent_key(b"/t/1/cur2");
    let k_old = intent_key(b"/t/1/old");
    assert!(
        ok(core2.latest_get(&k_old)).is_some(),
        "the older-epoch intent is there"
    );
    assert!(
        ok(core2.latest_get(&k_pend)).is_some(),
        "the pending current-epoch intent is placed"
    );
    assert!(
        ok(core2.latest_get(&k_cur)).is_some(),
        "the committed-unresolved current-epoch intent is placed"
    );

    // The walk (chunk 2, several steps) finishes with no error: no
    // current-epoch intent ever reaches the mode decision.
    let mut sweep = BootSweep::new(Arc::clone(&core2)).with_chunk(2);
    let steps = sweep_to_finish(&mut sweep);
    assert!(
        steps >= 2,
        "chunk 2 over the reopened core takes several steps"
    );

    // Counts exact: exactly the one older-epoch intent was removed.
    assert_eq!(
        core2.job_stats(JobKind::BootSweep).items,
        1,
        "one removal: the older-epoch intent"
    );
    assert!(
        ok(core2.latest_get(&k_old)).is_none(),
        "the older-epoch intent is gone"
    );
    assert!(
        ok(core2.latest_get(&k_pend)).is_some(),
        "the pending current-epoch intent is untouched"
    );
    assert!(
        ok(core2.latest_get(&k_cur)).is_some(),
        "the committed-unresolved current-epoch intent is untouched"
    );
    assert!(
        core2.registry.sweep_counter().is_some(),
        "the walk completed"
    );

    // Values intact: the older-epoch commit resolved through §7.3; the
    // current-epoch commit resolves through the resolver like any live
    // txn; the pending write still reads nothing.
    assert_eq!(
        rig2.read_at(b"/t/1/old", core2.visible_ts()),
        Some(b"old".to_vec())
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while ok(core2.latest_get(&k_cur)).is_some() {
        ok(Resolver::run_once(&core2));
        assert!(
            std::time::Instant::now() < deadline,
            "the committed-unresolved intent never resolved"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        rig2.read_at(b"/t/1/cur2", core2.visible_ts()),
        Some(b"c2".to_vec())
    );
    assert_eq!(rig2.read_at(b"/t/1/pend2", core2.visible_ts()), None);
}

/// NOSKIP-0x00 (the sweep decodes a non-catalog `/sys/` key): the intent
/// tag is a trailing `0x00`, so a `/sys/txn/{id}` key whose be64 id is a
/// multiple of 256 parses as an intent — and its record bytes are not an
/// intent value, so decoding one fails the step (a `Corrupt`, fail-stop
/// of the set). The walk must skip every non-catalog `/sys/` key whole,
/// before `parse_key`, and still finish (C-B1b gate M-2).
#[test]
fn sweep_never_decodes_non_catalog_sys_keys() {
    let rig = B1Rig::new();
    let core = &rig.core;
    // One committed-unresolved write: an older-epoch intent after the
    // reopen, on a `/t/` key that sorts after the whole `/sys/` span —
    // the walk must cross the planted keys to reach it.
    let t1 = rig.rc();
    place_write(core, &t1, b"/t/1/x", b"v");
    ok(core.commit(t1, nucleus_txn::commit::SyncCommit::On));

    let kv = rig.kv.clone();
    kv.kill(usize::MAX);
    let rig2 = reopen_rig(rig);
    let core2 = Arc::clone(&rig2.core);

    // The hazard: raw `/sys/txn` keys ending in `0x00` (be64 ids that are
    // multiples of 256), planted before the sweep starts. Their values
    // are record-shaped junk — `decode_intent` fails on the magic.
    let planted: Vec<(Vec<u8>, Vec<u8>)> = [256u64, 512, 768]
        .into_iter()
        .map(|n| (sys_txn_key(TxnId { epoch: 1, n }), b"junk record".to_vec()))
        .collect();
    {
        let mut batch = Batch::default();
        for (k, v) in &planted {
            batch.put(k.clone(), v.clone());
        }
        ok(core2.write(batch, Durability::No));
    }

    // The walk finishes with no error: the planted keys are skipped
    // whole, never decoded.
    let mut sweep = BootSweep::new(Arc::clone(&core2)).with_chunk(2);
    sweep_to_finish(&mut sweep);
    assert!(
        core2.registry.sweep_counter().is_some(),
        "the walk completed"
    );

    // The older-epoch intent is still removed and its value reads.
    assert!(
        ok(core2.latest_get(&intent_key(b"/t/1/x"))).is_none(),
        "the older-epoch intent is gone"
    );
    assert_eq!(
        rig2.read_at(b"/t/1/x", core2.visible_ts()),
        Some(b"v".to_vec())
    );

    // And every planted key is still there, byte for byte: skipped whole,
    // never decoded, never removed.
    for (k, v) in &planted {
        assert_eq!(
            ok(core2.latest_get(k)),
            Some(v.clone()),
            "the planted /sys/ key {k:?} is untouched"
        );
    }
}

/// UNBUDGET (skipped `/sys/` entries cost the step nothing and the resume
/// key never advances past them — the pre-C-B1bfix shape): a bounded-work
/// pin. With 300 planted `/sys/txn` keys and chunk 2, crossing the span
/// must cost at least 150 steps — each step's iterator work is bounded by
/// `chunk` stored entries, skipped or examined (C-B1b gate L-5). With the
/// budget gone, one step skips the whole span for free and the walk ends
/// in a step or two.
#[test]
fn a_steps_walk_is_bounded_across_sys_spans() {
    let core = Arc::new(ok(Core::open(nucleus_kv::MemKv::new())));
    let planted: Vec<Vec<u8>> = (0..300u64)
        .map(|n| {
            sys_txn_key(TxnId {
                epoch: 1,
                n: 1_000 + n,
            })
        })
        .collect();
    {
        let mut batch = Batch::default();
        for k in &planted {
            batch.put(k.clone(), 0u64.to_be_bytes().to_vec());
        }
        ok(core.write(batch, Durability::No));
    }
    let mut sweep = BootSweep::new(Arc::clone(&core)).with_chunk(2);
    let steps = sweep_to_finish(&mut sweep);
    assert!(
        steps >= 150,
        "crossing {} skipped /sys/ keys at chunk 2 must cost at least 150 \
         steps, took {steps}",
        planted.len()
    );
    // The span is crossed, not deleted: the planted keys are all still
    // there (non-catalog /sys/ is never the sweep's business).
    for k in [&planted[0], &planted[149], &planted[299]] {
        assert!(
            ok(core.latest_get(k)).is_some(),
            "the planted key {k:?} is untouched"
        );
    }
}

/// N-5 (the sweep clamps an over-long source prefix instead of refusing
/// it): a latch-key source that answers a prefix length beyond the key's
/// own length is a misbehaving source, and the sweep must fail loudly
/// with an `Invariant` — exactly like `GcJob::retire_locked` — instead of
/// silently latching finer than §5.1's placement.
#[test]
fn sweep_rejects_an_over_long_source_prefix_loudly() {
    // An older-epoch intent on a plain key, via a plain reopen (MemKv
    // keeps every write; the epoch still bumps).
    let kv = nucleus_kv::MemKv::new();
    let core1 = ok(Core::open(kv));
    let txn = core1.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    place_write(&core1, &txn, b"/t/1/k", b"v");
    drop(txn);
    let kv = core1.into_kv();
    let core2 = Arc::new(ok(Core::open(kv)));

    /// Answers a prefix no key can have.
    struct OverLongSource;
    impl LatchKeySource for OverLongSource {
        fn deferrable_prefix(&self, _key: &[u8]) -> Option<usize> {
            Some(9_999)
        }
    }
    core2.set_latch_key_source(Arc::new(OverLongSource));

    let mut sweep = BootSweep::new(Arc::clone(&core2));
    let err = sweep
        .run_once()
        .expect_err("an over-long source prefix is refused");
    assert!(
        matches!(&err, TxnError::Invariant(m) if m.contains("latch key source")),
        "the refusal is a loud Invariant naming the source: {err:?}"
    );
}
