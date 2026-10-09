//! Module tests that need a pause point inside an SSI critical section
//! (seeds 20, 21) or the bare state (§8.6 promotion).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nucleus_kv::MemKv;

use super::state::{Retired, State};
use super::{Siread, Ssi};
use crate::boot::Core;
use crate::commit::{CommitConfig, CommitObserver, CommitPipeline, SyncCommit};
use crate::txn::{Isolation, Txn};
use crate::visibility::ReadCtx;
use crate::write::{CommittedVersion, Epq, EpqDecision, RowOp, StmtCtx, UniqueRule};
use crate::{Ts, TxnError, TxnId};

struct NoEpq;

impl Epq for NoEpq {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Skip
    }
}

fn rig() -> (Arc<Core<MemKv>>, Arc<Ssi>, CommitPipeline<MemKv>) {
    let core = Arc::new(Core::open(MemKv::new()).expect("open"));
    let ssi = Ssi::install(&core);
    let pipeline = CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_observer(ssi.commit_observer()),
    )
    .expect("pipeline");
    (core, ssi, pipeline)
}

fn preload(core: &Core<MemKv>, pipeline: &mut CommitPipeline<MemKv>, key: &[u8]) {
    let txn = core.begin(Isolation::ReadCommitted);
    let seq = txn.next_seq().expect("seq");
    core.insert_key(
        &txn,
        key,
        None,
        b"v0".to_vec(),
        StmtCtx::new(core.visible_ts(), seq, seq),
        UniqueRule::Unique { same_row: None },
    )
    .expect("insert");
    let ticket = core.commit_submit(txn, SyncCommit::On).expect("submit");
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    ticket.wait().expect("ack");
}

fn read(ssi: &Ssi, core: &Core<MemKv>, txn: &Txn, s: Ts, key: &[u8]) {
    let ctx = ReadCtx {
        txn: txn.id,
        snapshot: s,
        stmt_seq: txn.next_seq().expect("seq"),
    };
    ssi.read_key(core, txn.id, key, &ctx).expect("read");
}

fn update(core: &Core<MemKv>, txn: &Txn, s: Ts, key: &[u8]) {
    let seq = txn.next_seq().expect("seq");
    core.row_op(
        txn,
        key,
        None,
        RowOp::Update {
            value: b"v1".to_vec(),
            key_cols_changed: false,
        },
        StmtCtx::new(s, seq, seq),
        &mut NoEpq,
    )
    .expect("update");
}

/// Seed 20: the pre-commit check, prepare and enqueue are one critical
/// section. While T1 is paused between its check and its prepare, T2's
/// pre-commit cannot finish; afterwards exactly one of the write-skew pair
/// commits. Mutant: release the SSI mutex between check and prepare (T2,
/// doomed by T1's check, then fails at once, inside the 200 ms window).
#[test]
fn seed20_precommit_atomic() {
    let (core, ssi, mut pipeline) = rig();
    preload(&core, &mut pipeline, b"x");
    preload(&core, &mut pipeline, b"y");
    let t1 = core.begin(Isolation::Serializable);
    let g1 = ssi.begin(&core, &t1, false).expect("begin t1");
    let t2 = core.begin(Isolation::Serializable);
    let g2 = ssi.begin(&core, &t2, false).expect("begin t2");
    let (s1, s2) = (g1.ts(), g2.ts());
    read(&ssi, &core, &t1, s1, b"x");
    read(&ssi, &core, &t2, s2, b"y");
    update(&core, &t1, s1, b"y");
    update(&core, &t2, s2, b"x");
    let (id1, id2) = (t1.id, t2.id);
    assert!(ssi.edges().contains(&(id1, id2)) && ssi.edges().contains(&(id2, id1)));

    ssi.pause_precommit.arm();
    let b_done = AtomicBool::new(false);
    let (r1, r2) = std::thread::scope(|sc| {
        let a = sc.spawn(|| core.commit_submit(t1, SyncCommit::On));
        assert!(
            ssi.pause_precommit.wait_arrived(Duration::from_secs(10)),
            "T1 never reached the pause point"
        );
        let b = sc.spawn(|| {
            let r = core.commit_submit(t2, SyncCommit::On);
            b_done.store(true, Ordering::SeqCst);
            r
        });
        std::thread::sleep(Duration::from_millis(200));
        let finished_early = b_done.load(Ordering::SeqCst);
        ssi.pause_precommit.release();
        let r1 = a.join().expect("a");
        let r2 = b.join().expect("b");
        assert!(
            !finished_early,
            "T2's pre-commit finished while T1 was inside its critical section"
        );
        (r1, r2)
    });
    let ticket1 = r1.expect("T1 enqueued");
    assert_eq!(
        r2.err(),
        Some(TxnError::SerializationFailure),
        "T2 must fail"
    );
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 1, "exactly one request reached the channel");
    pipeline.process_group(group);
    ticket1.wait().expect("T1 commits");
    assert_eq!(
        core.status.lookup_for_intent(id2).expect("status"),
        crate::TxnStatus::Aborted
    );
    drop((g1, g2));
}

/// Seed 21: taking `S` and creating the SSI entry are one registry
/// critical section. With `begin` paused inside it (after reading `S`),
/// a retention pass on another thread blocks; afterwards the committed T
/// with `commit_ts > S(new)` is still kept. Mutant: `take_snapshot()` then a
/// separate SSI registration (retention runs in the gap and retires T).
#[test]
fn seed21_begin_registers_atomically() {
    let (core, ssi, _pipeline) = rig();
    let t = core.begin(Isolation::Serializable);
    let gt = ssi.begin(&core, &t, false).expect("begin t");
    assert_eq!(gt.ts(), Ts(0));
    // T committed at 1, not yet visible (§3 step 3 before step 4).
    ssi.on_assigned(t.id, Ts(1));
    let n = core.begin(Isolation::Serializable);
    ssi.pause_begin.arm();
    let ret_done = AtomicBool::new(false);
    let s_new = std::thread::scope(|sc| {
        let a = sc.spawn(|| ssi.begin(&core, &n, false).map(|g| g.ts()));
        assert!(
            ssi.pause_begin.wait_arrived(Duration::from_secs(10)),
            "begin never reached the pause point"
        );
        // Step 4 advances visible_ts while the new txn is inside begin.
        core.advance_visible_ts(Ts(1));
        let b = sc.spawn(|| {
            let n = ssi.run_retention(&core);
            ret_done.store(true, Ordering::SeqCst);
            n
        });
        std::thread::sleep(Duration::from_millis(200));
        let early = ret_done.load(Ordering::SeqCst);
        ssi.pause_begin.release();
        let s = a.join().expect("a").expect("begin n");
        b.join().expect("b");
        assert!(!early, "retention ran inside begin's critical section");
        s
    });
    assert_eq!(s_new, Ts(0));
    assert_eq!(ssi.writer_of(Ts(1)), Some(t.id), "T retired too early");
    ssi.run_retention(&core);
    assert_eq!(ssi.writer_of(Ts(1)), Some(t.id), "N (S=0) is still active");
    drop(gt);
}

fn id(n: u64) -> TxnId {
    TxnId { epoch: 1, n }
}

/// §8.6 promotion: fine SIREADs inside a retired range become one relation
/// SIREAD (keeping the earliest stamp); a range only overlapping it keeps
/// its fine lock too; SIREADs elsewhere are untouched.
#[test]
fn promotion_replaces_fine_sireads_inside_retired_ranges() {
    let mut st = State::default();
    assert!(st.begin(id(1), Ts(0), false));
    st.register(
        id(1),
        Siread::Point {
            key: b"o5".to_vec(),
        },
        7,
    );
    st.register(
        id(1),
        Siread::Range {
            lo: b"o1".to_vec(),
            hi: b"o3".to_vec(),
        },
        4,
    );
    st.register(
        id(1),
        Siread::Range {
            lo: b"n9".to_vec(),
            hi: b"o2".to_vec(),
        },
        9,
    );
    st.register(id(1), Siread::Point { key: b"q".to_vec() }, 2);
    st.promote(&Retired {
        rel_oid: 7,
        ranges: vec![(b"o".to_vec(), b"p".to_vec())],
    });
    let got: Vec<(Siread, u64)> = st.entries[&id(1)]
        .sireads
        .iter()
        .map(|(s, c)| (s.clone(), *c))
        .collect();
    assert_eq!(
        got,
        vec![
            (Siread::Point { key: b"q".to_vec() }, 2),
            (
                Siread::Range {
                    lo: b"n9".to_vec(),
                    hi: b"o2".to_vec()
                },
                9
            ),
            (Siread::Relation { rel_oid: 7 }, 4),
        ]
    );
}
