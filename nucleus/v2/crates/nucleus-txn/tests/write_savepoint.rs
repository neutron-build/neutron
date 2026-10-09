//! C-T2 savepoint tests (§5.5): seeds 16, 25, 26, 46 and 50, plus the
//! queued-events discard rule. Latch-blocking tests hold the latch on a
//! helper thread (a `LatchGuard` is `!Send` by design).

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use common::{assert_count_exact, ok, TestRowLocks};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{decode_intent, intent_key};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::txn::Txn;
use nucleus_txn::write::RowLocks as _;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, RowOp, RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::RowLockMode;

use common::RecKv;

/// A hand-driven core + pipeline (the pipeline behind a mutex so a helper
/// thread can drive `process_group` while the main thread holds a latch).
struct Rig {
    core: Arc<Core<RecKv>>,
    locks: Arc<TestRowLocks>,
    pipeline: Arc<Mutex<CommitPipeline<RecKv>>>,
}

impl Rig {
    fn new() -> Rig {
        let core = Arc::new(ok(Core::open(RecKv::new())));
        let locks = TestRowLocks::new();
        core.set_row_locks(locks.clone());
        let pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        Rig {
            core,
            locks,
            pipeline: Arc::new(Mutex::new(pipeline)),
        }
    }

    fn txn(&self) -> Txn {
        self.core.begin(Isolation::ReadCommitted)
    }

    #[allow(dead_code)]
    fn process_queued(&self) {
        let mut p = self.pipeline.lock().expect("pipeline");
        let group = p.drain_available();
        p.process_group(group);
    }
}

fn upd(core: &Core<RecKv>, t: &Txn, seq: u32, key: &[u8], value: &[u8]) {
    ok(core.row_op(
        t,
        key,
        None,
        RowOp::Update {
            value: value.to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(core.visible_ts(), seq, seq),
        &mut Fixed(RowOp::Update {
            value: value.to_vec(),
            key_cols_changed: false,
        }),
    ));
}

struct Fixed(RowOp);
impl Epq for Fixed {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Apply(self.0.clone())
    }
}

/// Holds `key`'s latch on a helper thread until released; returns once the
/// latch is held.
fn hold_latch(core: &Arc<Core<RecKv>>, key: &[u8]) -> LatchHolder {
    let held = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let core2 = Arc::clone(core);
    let key = key.to_vec();
    let held2 = Arc::clone(&held);
    let release2 = Arc::clone(&release);
    std::thread::spawn(move || {
        let _guard = core2.latches.lock(&key);
        held2.store(true, Ordering::SeqCst);
        while !release2.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_micros(200));
        }
    });
    while !held.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_micros(200));
    }
    LatchHolder { release }
}

struct LatchHolder {
    release: Arc<AtomicBool>,
}

impl LatchHolder {
    fn release(self) {
        self.release.store(true, Ordering::SeqCst);
    }
}

/// Asserts `f` (run on this thread) does not finish within 200 ms.
fn not_within_200ms(runs: &mpsc::Receiver<()>) {
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        runs.try_recv().is_err(),
        "the blocked operation finished while the latch was held"
    );
}

// ---- seed 16 ---------------------------------------------------------------

#[test]
fn seed16_rollback_keeps_lock() {
    let rig = Rig::new();
    let t1 = rig.txn();
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k0", b"v1");
    let sp = ok(t1.savepoint());
    let s2 = ok(t1.next_seq());
    // FOR UPDATE k0 after the savepoint.
    ok(rig.core.row_op(
        &t1,
        b"/t/1/k0",
        None,
        RowOp::Lock(RowLockMode::Update),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        &mut Fixed(RowOp::Lock(RowLockMode::Update)),
    ));
    let top = top_layer(&rig, b"/t/1/k0");
    assert_eq!(top.lock, RowLockMode::Update);
    ok(rig.core.rollback_to(&t1, sp));
    // The restored top layer is the UPDATE's, with its data and lock
    // (seed 16).
    let top = top_layer(&rig, b"/t/1/k0");
    assert_eq!(top.seq, s1, "only the savepoint's layers are dropped");
    assert_eq!(top.lock, RowLockMode::NoKeyUpdate, "the lock survives");
    assert!(matches!(
        top.data,
        nucleus_txn::LayerData::Write { ref value, .. } if value == b"v1"
    ));
    assert_count_exact(&rig.core, &t1);

    // T2's UPDATE of k0 waits on T1's surviving lock.
    let t2 = rig.txn();
    let s3 = ok(t2.next_seq());
    let mut task = nucleus_txn::write::RowOpTask::new(
        b"/t/1/k0",
        None,
        RowOp::Update {
            value: b"v2".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), s3, s3),
    );
    match task.step(&rig.core, &t2).expect("step") {
        nucleus_txn::write::Step::Wait(w) => assert_eq!(w, vec![(t1.id, 1)]),
        s => panic!("expected Wait on T1 after ROLLBACK TO, got {s:?}"),
    }
    ok(rig.core.abort(t2));
    ok(rig.core.abort(t1));

    // The restored layer also keeps a lock that is stronger than its data
    // implies (an explicit `FOR UPDATE` before the savepoint): rollback
    // restores the stored `lock`, it does not recompute it from the data
    // (the mutant's "lock dropped with the data"). A `KEY SHARE` requester
    // still conflicts with the restored `Update` mode.
    let rig2 = Rig::new();
    let a = rig2.txn();
    let sa1 = ok(a.next_seq());
    upd(&rig2.core, &a, sa1, b"/t/1/k0", b"v1");
    let sa2 = ok(a.next_seq());
    ok(rig2.core.row_op(
        &a,
        b"/t/1/k0",
        None,
        RowOp::Lock(RowLockMode::Update),
        StmtCtx::new(rig2.core.visible_ts(), sa2, sa2),
        &mut Fixed(RowOp::Lock(RowLockMode::Update)),
    ));
    let sp2 = ok(a.savepoint());
    let sa3 = ok(a.next_seq());
    upd(&rig2.core, &a, sa3, b"/t/1/k0", b"v2");
    ok(rig2.core.rollback_to(&a, sp2));
    let top = top_layer(&rig2, b"/t/1/k0");
    assert_eq!(top.lock, RowLockMode::Update, "the explicit lock survives");
    let b = rig2.txn();
    let sb = ok(b.next_seq());
    let mut probe = nucleus_txn::write::RowOpTask::new(
        b"/t/1/k0",
        None,
        RowOp::Lock(RowLockMode::KeyShare),
        StmtCtx::new(rig2.core.visible_ts(), sb, sb),
    );
    match probe.step(&rig2.core, &b).expect("step") {
        // KEY SHARE conflicts with the restored UPDATE lock (§6 matrix).
        nucleus_txn::write::Step::Wait(_) => {}
        s => panic!("expected Wait on the restored Update lock, got {s:?}"),
    }
}

fn top_layer(rig: &Rig, key: &[u8]) -> nucleus_txn::Layer {
    let raw = ok(rig.core.latest_get(&intent_key(key))).expect("intent");
    let intent = ok(decode_intent(&raw));
    intent.layers.last().cloned().expect("layer")
}

// ---- seed 50 ---------------------------------------------------------------

#[test]
fn seed50_log_every_layer() {
    let rig = Rig::new();
    let t = rig.txn();
    let s1 = ok(t.next_seq()); // 1
    upd(&rig.core, &t, s1, b"/t/1/k0", b"v1");
    let sp = ok(t.savepoint()); // 2
    let s3 = ok(t.next_seq()); // 3
    upd(&rig.core, &t, s3, b"/t/1/k0", b"v3");
    let top = top_layer(&rig, b"/t/1/k0");
    assert_eq!(top.seq, s3);
    ok(rig.core.rollback_to(&t, sp));
    let top = top_layer(&rig, b"/t/1/k0");
    assert_eq!(top.seq, s1, "k0's top layer is seq 1's (seed 50)");
    assert!(matches!(
        top.data,
        nucleus_txn::LayerData::Write { ref value, .. } if value == b"v1"
    ));
    assert_count_exact(&rig.core, &t);
}

// ---- seed 26 ---------------------------------------------------------------

#[test]
fn seed26_rollback_wakes_waiter() {
    let rig = Rig::new();
    let parkers = common::InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());
    let t1 = rig.txn();
    let sp = ok(t1.savepoint());
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1"); // layer >= sp: dropped below

    // T2 waits on T1's intent, parked (infinite parker: a lost wake fails
    // the test, not a timeout rescue).
    let t2 = Arc::new(rig.txn());
    let s2 = ok(t2.next_seq());
    let core = Arc::clone(&rig.core);
    let t2c = Arc::clone(&t2);
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(core.row_op(
            &t2c,
            b"/t/1/k",
            None,
            RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(core.visible_ts(), s2, s2),
            &mut Fixed(RowOp::Update {
                value: b"v2".to_vec(),
                key_cols_changed: false,
            }),
        ));
    });
    parkers.wait_parked();
    let start = Instant::now();
    ok(rig.core.rollback_to(&t1, sp)); // drops T1's layer, bumps + wakes
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(RowOutcome::Applied)),
        Err(_) => panic!("the waiter was not woken within 100 ms (seed 26)"),
    }
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(
        top_owner(&rig, b"/t/1/k"),
        t2.id,
        "the retry placed T2's intent"
    );
}

fn top_owner(rig: &Rig, key: &[u8]) -> nucleus_txn::TxnId {
    let raw = ok(rig.core.latest_get(&intent_key(key))).expect("intent");
    ok(decode_intent(&raw)).txn
}

// ---- seed 25 ---------------------------------------------------------------

#[test]
fn seed25_deferrable_removal_latches_prefix() {
    let rig = Rig::new();
    // A deferrable entry: key = /i/1/d0 + pk; latch prefix = /i/1/d0.
    let prefix = b"/i/1/d0".to_vec();
    let mut key = prefix.clone();
    key.extend_from_slice(b"/t/1/r1");
    let t1 = rig.txn();
    let s1 = ok(t1.next_seq());
    ok(rig.core.insert_key(
        &t1,
        &key,
        Some(prefix.len()),
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s1, s1),
        UniqueRule::Deferrable,
    ));
    ok(rig.core.abort(t1)); // queues the cleanup entry

    // Abort cleanup (the resolver) latches the prefix: hold it, and the
    // cleanup must not finish within 200 ms.
    let holder = hold_latch(&rig.core, &prefix);
    let (tx, rx) = mpsc::channel::<()>();
    {
        let core = Arc::clone(&rig.core);
        std::thread::spawn(move || {
            let _ = Resolver::run_once(&core);
            let _ = tx.send(());
        });
    }
    not_within_200ms(&rx);
    holder.release();
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_ok(),
        "the removal finished after the release"
    );

    // The same for ROLLBACK TO.
    let t2 = rig.txn();
    let sp = ok(t2.savepoint());
    let s2 = ok(t2.next_seq());
    let mut key2 = prefix.clone();
    key2.extend_from_slice(b"/t/1/r2");
    ok(rig.core.insert_key(
        &t2,
        &key2,
        Some(prefix.len()),
        b"e".to_vec(),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        UniqueRule::Deferrable,
    ));
    let holder2 = hold_latch(&rig.core, &prefix);
    let (tx2, rx2) = mpsc::channel::<()>();
    {
        let core = Arc::clone(&rig.core);
        let t2 = Arc::new(t2);
        std::thread::spawn(move || {
            let _ = core.rollback_to(&t2, sp);
            let _ = tx2.send(());
        });
    }
    not_within_200ms(&rx2);
    holder2.release();
    assert!(
        rx2.recv_timeout(Duration::from_millis(200)).is_ok(),
        "the rollback finished after the release"
    );
}

// ---- seed 46 ---------------------------------------------------------------

#[test]
fn seed46_shared_release_under_latch() {
    let rig = Rig::new();
    // A shared lock taken after a savepoint: ROLLBACK TO releases it under
    // the key's latch.
    let t = rig.txn();
    let sp = ok(t.savepoint());
    let s = ok(t.next_seq());
    ok(rig.core.row_op(
        &t,
        b"/t/1/k",
        None,
        RowOp::Lock(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), s, s),
        &mut Fixed(RowOp::Lock(RowLockMode::Share)),
    ));
    let holder = hold_latch(&rig.core, b"/t/1/k");
    let (tx, rx) = mpsc::channel::<()>();
    {
        let core = Arc::clone(&rig.core);
        let t = Arc::new(t);
        std::thread::spawn(move || {
            let _ = core.rollback_to(&t, sp);
            let _ = tx.send(());
        });
    }
    not_within_200ms(&rx);
    assert!(
        !rig.locks.holders(b"/t/1/k").is_empty(),
        "the shared lock was released without the key's latch"
    );
    holder.release();
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_ok(),
        "the rollback finished after the release"
    );

    // Commit step 5 releases the remaining shared lock under the key's
    // latch too.
    let t2 = rig.txn();
    let s2 = ok(t2.next_seq());
    ok(rig.core.row_op(
        &t2,
        b"/t/1/k",
        None,
        RowOp::Lock(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), s2, s2),
        &mut Fixed(RowOp::Lock(RowLockMode::Share)),
    ));
    // A write (on another key) keeps t2 off the no-write fast path, so its
    // commit really goes through the pipeline and step 5.
    let s2b = ok(t2.next_seq());
    upd(&rig.core, &t2, s2b, b"/t/1/other", b"w");
    assert!(!rig.locks.holders(b"/t/1/k").is_empty());
    let t2_id = t2.id;
    let ticket = ok(rig.core.commit_submit(t2, SyncCommit::On));
    let holder2 = hold_latch(&rig.core, b"/t/1/k");
    {
        // Step 5 blocks in release_row_locks on the helper thread: the ack
        // (step 4) arrives, the release does not.
        let (tx5, rx5) = mpsc::channel::<()>();
        {
            let pipeline = Arc::clone(&rig.pipeline);
            std::thread::spawn(move || {
                let mut p = pipeline.lock().expect("pipeline");
                let group = p.drain_available();
                p.process_group(group);
                drop(p);
                let _ = tx5.send(());
            });
        }
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !rig.locks.holders(b"/t/1/k").is_empty(),
            "step 5 released the shared lock without the key's latch (seed 46)"
        );
        assert!(
            ticket.try_ack().is_some(),
            "the ack (step 4) is not blocked by step 5"
        );
        holder2.release();
        assert!(
            rx5.recv_timeout(Duration::from_millis(200)).is_ok(),
            "step 5 finished after the release"
        );
    }
    common::wait_released(&rig.core, t2_id);
}

// ---- queued events (§5.3/§5.5) ----------------------------------------------

#[test]
fn rollback_discards_queued_events_tagged_at_or_above_the_savepoint() {
    let rig = Rig::new();
    let t = rig.txn();
    let before = ok(t.savepoint());
    t.queue_event(before, b"kept-before".to_vec());
    let after = ok(t.next_seq());
    t.queue_event(after, b"dropped".to_vec());
    t.queue_event(after + 1, b"dropped-too".to_vec());
    ok(rig.core.rollback_to(&t, after)); // drops >= after only
    let events = t.take_events();
    assert_eq!(events, vec![(before, b"kept-before".to_vec())]);
}
