//! C-T2 wait tests over the write path: seed 37 (a truncated target ends a
//! wait) and §5.1's wait on **several** shared-lock holders.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ok, TestRowLocks};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{decode_intent, intent_key};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::wait::WaitOutcome;
use nucleus_txn::write::{RowOp, RowOpTask, RowOutcome, StmtCtx};
use nucleus_txn::RowLockMode;

use common::RecKv;

struct Rig {
    core: Arc<Core<RecKv>>,
    pipeline: CommitPipeline<RecKv>,
}

impl Rig {
    fn new() -> Rig {
        let core = Arc::new(ok(Core::open(RecKv::new())));
        core.set_row_locks(TestRowLocks::new());
        let pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        Rig { core, pipeline }
    }

    fn txn(&self) -> nucleus_txn::txn::Txn {
        self.core.begin(Isolation::ReadCommitted)
    }

    fn commit(&mut self, txn: nucleus_txn::txn::Txn) -> nucleus_txn::Ts {
        let ticket = ok(self.core.commit_submit(txn, SyncCommit::On));
        let group = self.pipeline.drain_available();
        self.pipeline.process_group(group);
        ticket.wait().expect("ack")
    }
}

fn upd(core: &Core<RecKv>, t: &nucleus_txn::txn::Txn, seq: u32, key: &[u8], value: &[u8]) {
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
impl nucleus_txn::write::Epq for Fixed {
    fn recheck(
        &mut self,
        _n: &nucleus_txn::write::CommittedVersion,
    ) -> nucleus_txn::write::EpqDecision {
        nucleus_txn::write::EpqDecision::Apply(self.0.clone())
    }
}

/// Seed 37: T2 parks on T1; T1 commits; the resolver resolves **and
/// truncates** T1's status; T2's wait returns (`Ended`) and its retry
/// places. A missing status must never read as Pending (I-LIVE).
#[test]
fn seed37_waiter_returns_when_target_truncated() {
    let mut rig = Rig::new();
    let parkers = common::InfiniteParkers::new();
    rig.core.waits.set_parker_maker(parkers.clone());

    // Preload a row so T1's update commits a write set.
    {
        let t0 = rig.txn();
        let s0 = ok(t0.next_seq());
        ok(rig.core.insert_key(
            &t0,
            b"/t/1/k",
            None,
            b"v0".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), s0, s0),
            nucleus_txn::write::UniqueRule::Unique { same_row: None },
        ));
        rig.commit(t0);
        ok(Resolver::run_once(&rig.core));
        ok(Resolver::run_once(&rig.core));
    }

    let t1 = rig.txn();
    let s1 = ok(t1.next_seq());
    upd(&rig.core, &t1, s1, b"/t/1/k", b"v1");
    let t1_id = t1.id;

    // T2's update parks on T1's intent (infinite parker: no rescue).
    let t2 = Arc::new(rig.txn());
    let s2 = ok(t2.next_seq());
    let core = Arc::clone(&rig.core);
    let t2c = Arc::clone(&t2);
    let (tx, rx) = std::sync::mpsc::channel();
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

    // T1 commits; the resolver resolves and truncates its status entry.
    rig.commit(t1);
    for _ in 0..50 {
        ok(Resolver::run_once(&rig.core));
        if rig.core.status.entry(t1_id).is_none() {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(
        rig.core.status.entry(t1_id).is_none(),
        "T1's status was truncated while T2 was parked on it"
    );

    // T2 returns and its retry places over the removed intent.
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(r) => assert_eq!(r, Ok(RowOutcome::Applied)),
        Err(_) => panic!("the waiter never returned after the truncation (seed 37)"),
    }
    let raw = ok(rig.core.latest_get(&intent_key(b"/t/1/k"))).expect("intent");
    assert_eq!(ok(decode_intent(&raw)).txn, t2.id, "the retry placed");

    // The deterministic arm of seed 37 (§4: a missing status for a
    // remembered TxnId means ended and released): a waiter that registers
    // on an already-truncated target returns `Ended` at once — treating it
    // as Pending would park with nobody left to wake it.
    {
        // A fresh key: T2 above still holds its intent on /t/1/k and never
        // ends in this test, so T3 writes elsewhere.
        let t3 = rig.txn();
        let s3 = ok(t3.next_seq());
        upd(&rig.core, &t3, s3, b"/t/1/k2", b"v3");
        let t3_id = t3.id;
        rig.commit(t3);
        for _ in 0..50 {
            ok(Resolver::run_once(&rig.core));
            if rig.core.status.entry(t3_id).is_none() {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(rig.core.status.entry(t3_id).is_none(), "T3 truncated");
        let t4 = rig.txn();
        let (tx4, rx4) = std::sync::mpsc::channel();
        let core = Arc::clone(&rig.core);
        std::thread::spawn(move || {
            let _ = tx4.send(core.wait_on(&t4, t3_id, 0));
        });
        match rx4.recv_timeout(Duration::from_millis(100)) {
            Ok(nucleus_txn::wait::WaitOutcome::Ended) => {}
            Ok(o) => panic!("wait on a truncated target must be Ended, got {o:?}"),
            Err(_) => panic!("a wait on an ended target parked (seed 37)"),
        }
    }
}

/// §5.1: two conflicting shared holders → one `Wait` with both; the waiter
/// wakes when either ends, re-waits on the rest, and finally places.
#[test]
fn waits_on_several_holders() {
    let mut rig = Rig::new();
    {
        let t0 = rig.txn();
        let s0 = ok(t0.next_seq());
        ok(rig.core.insert_key(
            &t0,
            b"/t/1/k",
            None,
            b"v0".to_vec(),
            StmtCtx::new(rig.core.visible_ts(), s0, s0),
            nucleus_txn::write::UniqueRule::Unique { same_row: None },
        ));
        rig.commit(t0);
        ok(Resolver::run_once(&rig.core));
    }
    // Two SHARE holders.
    let h1 = rig.txn();
    let sh1 = ok(h1.next_seq());
    ok(rig.core.row_op(
        &h1,
        b"/t/1/k",
        None,
        RowOp::Lock(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), sh1, sh1),
        &mut Fixed(RowOp::Lock(RowLockMode::Share)),
    ));
    let h2 = rig.txn();
    let sh2 = ok(h2.next_seq());
    ok(rig.core.row_op(
        &h2,
        b"/t/1/k",
        None,
        RowOp::Lock(RowLockMode::Share),
        StmtCtx::new(rig.core.visible_ts(), sh2, sh2),
        &mut Fixed(RowOp::Lock(RowLockMode::Share)),
    ));

    // An UPDATE conflicts with both: one Wait with both holders.
    let w = rig.txn();
    let sw = ok(w.next_seq());
    let mut task = RowOpTask::new(
        b"/t/1/k",
        None,
        RowOp::Update {
            value: b"v1".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(rig.core.visible_ts(), sw, sw),
    );
    let targets = match task.step(&rig.core, &w).expect("step") {
        nucleus_txn::write::Step::Wait(t) => t,
        s => panic!("expected Wait on both holders, got {s:?}"),
    };
    assert_eq!(
        targets,
        vec![(h1.id, 0), (h2.id, 0)],
        "one wait set with both"
    );

    // Either holder ending wakes the waiter: h1 aborts → the wait returns
    // (Aborted), the retry waits on h2 only, then h2 aborts → places.
    ok(rig.core.abort(h1));
    assert_eq!(
        rig.core.wait_on_any(&w, &targets),
        WaitOutcome::Aborted,
        "woken when either target ends"
    );
    let targets2 = match task.step(&rig.core, &w).expect("step") {
        nucleus_txn::write::Step::Wait(t) => t,
        nucleus_txn::write::Step::Done(o) => panic!("h2 still holds: expected Wait, got {o:?}"),
        s => panic!("expected Wait, got {s:?}"),
    };
    assert_eq!(
        targets2,
        vec![(h2.id, 0)],
        "only the surviving holder waits"
    );
    ok(rig.core.abort(h2));
    assert_eq!(rig.core.wait_on_any(&w, &targets2), WaitOutcome::Aborted);
    match task.step(&rig.core, &w).expect("step") {
        nucleus_txn::write::Step::Done(RowOutcome::Applied) => {}
        s => panic!("expected Done after both holders ended, got {s:?}"),
    }
}
