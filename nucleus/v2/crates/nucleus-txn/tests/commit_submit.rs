//! C-T2 `commit_submit` tests (§3): the ticket, the no-write fast path, the
//! `pre_commit` wrap (§8.4), and the C-T1b review follow-ups 1-4 this card
//! owns: a step-4 failure stops the group; a boot read error on
//! `/sys/ts_hwm` is an error, not 0; `CommitIndeterminate` (08007) for
//! error acks after records reached the WAL; `with_config` installs the
//! fail-stop hook only after the attach succeeds.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{ok, FailNthKv, FailReadsKv, RecKv, TestRowLocks};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitConfig, CommitPipeline, FailStop, SyncCommit};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Txn;
use nucleus_txn::write::RowLocks as _;
use nucleus_txn::{Ts, TxnError};

/// A fail-stop hook that records instead of aborting.
#[derive(Default)]
struct RecFailStop {
    calls: Mutex<Vec<String>>,
}

impl RecFailStop {
    fn new() -> Arc<RecFailStop> {
        Arc::new(RecFailStop::default())
    }
}

impl FailStop for RecFailStop {
    fn on_kv_error(&self, err: &TxnError) {
        self.calls.lock().expect("calls").push(format!("{err:?}"));
    }
}

struct Fixed;
impl nucleus_txn::write::Epq for Fixed {
    fn recheck(
        &mut self,
        _n: &nucleus_txn::write::CommittedVersion,
    ) -> nucleus_txn::write::EpqDecision {
        nucleus_txn::write::EpqDecision::Apply(nucleus_txn::write::RowOp::Lock(
            nucleus_txn::RowLockMode::Share,
        ))
    }
}

/// Gives a txn a write (through the write path) so it is not on the
/// no-write fast path.
fn one_write<K: nucleus_kv::OrderedKv>(core: &Arc<Core<K>>, txn: &Txn) {
    let s = ok(txn.next_seq());
    ok(core.insert_key(
        txn,
        format!("/t/1/k{}", txn.id.n).as_bytes(),
        None,
        b"v".to_vec(),
        nucleus_txn::write::StmtCtx::new(core.visible_ts(), s, s),
        nucleus_txn::write::UniqueRule::Unique { same_row: None },
    ));
}

#[test]
fn ticket_not_acked_before_process_group_and_acked_after() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let t = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t);
    let ticket = ok(core.commit_submit(t, SyncCommit::On));
    assert!(
        ticket.try_ack().is_none(),
        "not acked before the group is processed"
    );
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 1);
    pipeline.process_group(group);
    let ts = ok(ticket.try_ack().expect("acked after process_group"));
    assert!(ts > Ts::ZERO);
    // wait() on a consumed ticket still returns the ack.
    assert_eq!(ticket.wait().expect("wait"), ts);
}

#[test]
fn no_write_txn_ticket_acked_at_once_with_ts_zero() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let locks = TestRowLocks::new();
    core.set_row_locks(locks.clone());
    let _pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    // A shared lock the fast path must release.
    let t = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    let s = ok(t.next_seq());
    ok(core.row_op(
        &t,
        b"/t/1/k",
        None,
        nucleus_txn::write::RowOp::Lock(nucleus_txn::RowLockMode::Share),
        nucleus_txn::write::StmtCtx::new(core.visible_ts(), s, s),
        &mut Fixed,
    ));
    assert!(!locks.holders(b"/t/1/k").is_empty());
    let id = t.id;
    let ticket = ok(core.commit_submit(t, SyncCommit::On));
    assert_eq!(
        ticket.try_ack(),
        Some(Ok(Ts::ZERO)),
        "the no-write fast path is acked at once with Ts::ZERO"
    );
    assert!(
        locks.holders(b"/t/1/k").is_empty(),
        "the fast path released the shared lock"
    );
    assert!(
        core.status.entry(id).is_some_and(|e| e.released),
        "the fast path released the txn"
    );
}

#[test]
fn pre_commit_wraps_the_enqueue() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let ssi = common::RecordingSsi::new();
    core.set_ssi_hook(ssi.clone());
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));

    // The gate closed: the enqueue never runs and nothing reaches the
    // channel (§8.4: the enqueue runs inside the critical section).
    ssi.close_gate(TxnError::SerializationFailure);
    let t = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t);
    let t_id = t.id;
    let err = match core.commit_submit(t, SyncCommit::On) {
        Err(e) => e,
        Ok(_) => panic!("the gate fails the commit before the enqueue"),
    };
    assert_eq!(err, TxnError::SerializationFailure);
    assert!(
        pipeline.drain_available().is_empty(),
        "the request never reached the channel"
    );
    assert_eq!(&*ssi.pre_commits.lock().expect("pc"), &[t_id]);

    // The gate open: the enqueue runs inside the hook and the group has the
    // request.
    ssi.open_gate();
    let t2 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t2);
    let ticket = ok(core.commit_submit(t2, SyncCommit::On));
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 1, "the enqueue ran inside pre_commit");
    pipeline.process_group(group);
    let _ts = ticket.wait().expect("ack");
}

/// C-T1b follow-up 1: a `set_committed` failure in commit step 4 stops the
/// group — no later `visible_ts` advance, no step 5, error acks from that
/// request on (here `CommitIndeterminate`: the group's records reached the
/// WAL). Mutant: keep going after the status error.
#[test]
fn step4_status_failure_stops_the_group() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let fs = RecFailStop::new();
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(fs.clone()),
    ));

    let t1 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t1);
    let t1_id = t1.id;
    // A request for a txn that never began: its step-4 `set_committed`
    // fails (no status entry), stopping the group (follow-up 1).
    let ghost = nucleus_txn::TxnId {
        epoch: core.epoch(),
        n: 9_999,
    };
    let (ghost_req, ghost_ack) =
        nucleus_txn::commit::CommitRequest::new(ghost, SyncCommit::On, None, false, Vec::new());
    let t3 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t3);

    let a1 = ok(core.commit_submit(t1, SyncCommit::On));
    ok(core.submit(ghost_req));
    let a3 = ok(core.commit_submit(t3, SyncCommit::On));
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 3);
    pipeline.process_group(group);

    // t1 committed (its step 4 ran); t2's set_committed failed (Aborted);
    // t3 never ran: error ack, no later visible_ts advance, no step 5.
    let ts1 = ok(a1.try_ack().expect("t1 acked"));
    assert_eq!(
        core.visible_ts(),
        ts1,
        "visible_ts stopped at the last successful request"
    );
    let e2 = ghost_ack
        .try_recv()
        .expect("the ghost request error-acked")
        .expect_err("status error");
    let e3 = a3.try_ack().expect("t3 error-acked").expect_err("stopped");
    assert_eq!(e2, TxnError::CommitIndeterminate);
    assert_eq!(e3, TxnError::CommitIndeterminate);
    // No step 5: t1 was not released and nothing was queued for resolution.
    assert!(
        !core.status.entry(t1_id).is_some_and(|e| e.released),
        "step 5 never ran"
    );
    assert_eq!(ok(Resolver::run_once(&core)), 0, "no resolution queued");
    assert_eq!(
        fs.calls.lock().expect("calls").len(),
        1,
        "fail-stop fired once"
    );
}

/// C-T1b follow-up 3: a step-2 write failure at request i > 0 — the group's
/// earlier records reached the WAL, so every error ack is
/// `CommitIndeterminate` (08007), never a plain failure.
#[test]
fn wal_reached_error_acks_are_indeterminate() {
    let kv = FailNthKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let fs = RecFailStop::new();
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(fs.clone()),
    ));

    let t1 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t1);
    let t2 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t2);
    let a1 = ok(core.commit_submit(t1, SyncCommit::On));
    let a2 = ok(core.commit_submit(t2, SyncCommit::On));
    // Fail the second record write of the group (i = 1 > 0): the first
    // request's record is already in the WAL.
    kv.fail_from(kv.writes() + 2);
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    let e1 = a1.try_ack().expect("error-acked").expect_err("stopped");
    let e2 = a2.try_ack().expect("error-acked").expect_err("stopped");
    assert_eq!(e1, TxnError::CommitIndeterminate, "records reached the WAL");
    assert_eq!(e2, TxnError::CommitIndeterminate);
    assert_eq!(core.visible_ts(), Ts(0), "nothing became visible");
    assert_eq!(fs.calls.lock().expect("calls").len(), 1);
}

/// The same failure at i = 0 (nothing reached the WAL) acks the raw error.
#[test]
fn step2_failure_at_first_request_acks_raw_error() {
    let kv = FailNthKv::new();
    let core = Arc::new(ok(Core::open(kv.clone())));
    let fs = RecFailStop::new();
    let mut pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(fs.clone()),
    ));
    let t1 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t1);
    let a1 = ok(core.commit_submit(t1, SyncCommit::On));
    kv.fail_from(kv.writes() + 1); // the very first record write fails
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    let e = a1.try_ack().expect("error-acked").expect_err("failed");
    assert!(
        !matches!(e, TxnError::CommitIndeterminate),
        "no record reached the WAL: the raw error, got {e:?}"
    );
}

/// C-T1b follow-up 4: `with_config` installs the fail-stop hook only after
/// the attach succeeds — a core that already has a pipeline keeps its
/// previous hook. Mutant: install before attaching.
#[test]
fn with_config_installs_fail_stop_only_after_attach() {
    let (kv, fail) = common::FailKv::shared();
    let core = Arc::new(ok(Core::open(kv)));
    let h1 = RecFailStop::new();
    // The first attach succeeds and installs h1.
    let mut p1 = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(h1.clone()),
    ));
    // The second attach fails; h2 must not be installed.
    let h2 = RecFailStop::new();
    assert!(
        CommitPipeline::with_config(
            Arc::clone(&core),
            CommitConfig::new().with_fail_stop(h2.clone()),
        )
        .is_err(),
        "a second pipeline cannot attach"
    );
    // Trigger a fail-stop through the installed pipeline's stop path (a
    // group record write failure): h1 fires, h2 never does.
    let t = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    one_write(&core, &t);
    let a1 = ok(core.commit_submit(t, SyncCommit::On));
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    let group = p1.drain_available();
    p1.process_group(group);
    assert!(
        a1.try_ack().expect("error-acked").is_err(),
        "the armed failure stoped the group"
    );
    assert!(
        !h1.calls.lock().expect("calls").is_empty(),
        "the installed hook fired"
    );
    assert!(
        h2.calls.lock().expect("calls").is_empty(),
        "the hook of the failed attach was never installed"
    );
}

/// C-T1b follow-up 2: a KV read error on `/sys/ts_hwm` at `Core::open` is
/// an error, not 0. Mutant: read error becomes 0.
#[test]
fn boot_ts_hwm_read_error_is_an_error_not_zero() {
    let kv = FailReadsKv::new(|k: &[u8]| k == b"/sys/ts_hwm");
    let err = Core::open(kv).err().expect("boot fails on a read error");
    assert!(
        matches!(err, TxnError::Kv(_)),
        "a read error must surface as the KV error, not 0: {err:?}"
    );
}

/// `abort` reports to the SSI hook (§8.6).
#[test]
fn abort_reports_to_the_ssi_hook() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let ssi = common::RecordingSsi::new();
    core.set_ssi_hook(ssi.clone());
    let t = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    let id = t.id;
    ok(core.abort(t));
    assert_eq!(&*ssi.aborts.lock().expect("aborts"), &[id]);
    let _ = Duration::from_millis(0);
}

/// Gives a txn one write on a fixed key (through the write path) so it is
/// not on the no-write fast path.
fn one_write_at(core: &Arc<Core<RecKv>>, txn: &Txn, key: &[u8]) {
    let s = ok(txn.next_seq());
    ok(core.insert_key(
        txn,
        key,
        None,
        b"v".to_vec(),
        nucleus_txn::write::StmtCtx::new(core.visible_ts(), s, s),
        nucleus_txn::write::UniqueRule::Unique { same_row: None },
    ));
}

/// C-T2 rework 2: a failed `pre_commit` (here the §8.4 gate) aborts the
/// txn before the error surfaces — §7.1 runs (Aborted, release, bump +
/// wake, released, cleanup queued), a waiter on its intent wakes, and
/// after `Resolver::run_once` no intent remains. A txn that stayed
/// Pending would park its waiters forever. Mutant: return the error
/// without aborting.
#[test]
fn failed_pre_commit_aborts_the_txn_and_wakes_waiters() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    core.set_row_locks(TestRowLocks::new());
    let ssi = common::RecordingSsi::new();
    core.set_ssi_hook(ssi.clone());
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let parkers = common::InfiniteParkers::new();
    core.waits.set_parker_maker(parkers.clone());

    // T1 holds a write intent on /t/1/k.
    let t1 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    let t1_id = t1.id;
    one_write_at(&core, &t1, b"/t/1/k");

    // T2 waits on it, parked (the infinite parker makes a lost wake a
    // hang, not a timeout rescue). The thread also aborts T2 once the op
    // completes, so one resolver round drains both cleanups.
    let t2 = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    let s2 = ok(t2.next_seq());
    let (tx, rx) = std::sync::mpsc::channel();
    let (tx2, rx2) = std::sync::mpsc::channel();
    {
        let core2 = Arc::clone(&core);
        std::thread::spawn(move || {
            let _ = tx.send(core2.row_op(
                &t2,
                b"/t/1/k",
                None,
                nucleus_txn::write::RowOp::Update {
                    value: b"v2".to_vec(),
                    key_cols_changed: false,
                },
                nucleus_txn::write::StmtCtx::new(core2.visible_ts(), s2, s2),
                &mut Fixed,
            ));
            let _ = tx2.send(core2.abort(t2));
        });
    }
    parkers.wait_parked();

    // The gate fails the pre-commit with 40001, before the enqueue.
    ssi.close_gate(TxnError::SerializationFailure);
    let err = match core.commit_submit(t1, SyncCommit::On) {
        Err(e) => e,
        Ok(_) => panic!("the closed gate fails the commit"),
    };
    assert_eq!(err, TxnError::SerializationFailure);
    assert!(
        pipeline.drain_available().is_empty(),
        "the request never reached the channel"
    );
    // §7.1 ran for T1: Aborted and released, its SSI state dropped.
    let entry = core.status.entry(t1_id).expect("T1's status entry");
    assert_eq!(entry.status, nucleus_txn::TxnStatus::Aborted);
    assert!(entry.released, "the abort marked T1 released");
    assert_eq!(&*ssi.aborts.lock().expect("aborts"), &[t1_id]);

    // The waiter woke, retried past the aborted intent, and placed.
    match rx.recv_timeout(Duration::from_millis(100)) {
        Ok(Ok(nucleus_txn::write::RowOutcome::Applied)) => {}
        other => panic!("the waiter was not woken and placed after the abort: {other:?}"),
    }
    assert!(rx2.recv_timeout(Duration::from_millis(100)).is_ok());

    // Both cleanups were queued (T1's by the abort inside commit_submit,
    // T2's by its own abort): one resolver round, then no intent remains.
    ok(Resolver::run_once(&core));
    let view = core.open_view();
    let intents: Vec<nucleus_kv::Key> = view
        .scan(
            (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            false,
        )
        .filter_map(|row| {
            let (k, _) = row.expect("scan");
            nucleus_txn::encoding::parse_key(&k)
                .is_some_and(|(_, e)| matches!(e, nucleus_txn::encoding::Entry::Intent))
                .then_some(k)
        })
        .collect();
    drop(view);
    assert!(
        intents.is_empty(),
        "no intent remains after the resolver round: {intents:?}"
    );
}

/// C-T2 rework 7c: `seen_committed` is keyed by the wrapper's per-core
/// token, never by the core's address — a dropped core's address is
/// routinely handed to the next core, which would inherit the dead
/// core's acks and let `wait_released` pass for a txn that never
/// committed. Mutant: the address-keyed map.
#[test]
fn seen_committed_does_not_survive_address_reuse() {
    use nucleus_kv::MemKv;
    // One wrapper per attempt; the panic hook is silenced while the
    // correct code fails `wait_released` on purpose.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut collided = false;
    for _ in 0..64 {
        let c1 = common::TestCore::<MemKv>::open(MemKv::new()).expect("core");
        let addr1 = std::ptr::from_ref(c1.core.as_ref()).addr();
        let id = nucleus_txn::TxnId {
            epoch: c1.epoch(),
            n: 3,
        };
        common::note_committed(&c1, id);
        drop(c1);
        let c2 = common::TestCore::<MemKv>::open(MemKv::new()).expect("core");
        let addr2 = std::ptr::from_ref(c2.core.as_ref()).addr();
        if addr1 == addr2 {
            collided = true;
            // c2's txn 3 never began, let alone committed: wait_released
            // must fail (panic), not treat the vanished entry as released.
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                common::wait_released(&c2, id);
            }));
            assert!(
                r.is_err(),
                "the address-reused core inherited the dead core's seen acks"
            );
            break;
        }
    }
    std::panic::set_hook(prev);
    // No assertion on `collided`: the correct code passes with or without
    // an observed collision; the address-keyed mutant fails whenever the
    // allocator hands a dropped core's address to the next one (checked
    // by applying the mutant — it fires on the first collision).
    let _ = collided;
}

/// A hook that enqueues and then fails: it breaks C-T3's contract.
struct EnqueueThenFail;
impl nucleus_txn::write::SsiHook for EnqueueThenFail {
    fn covers(&self, _txn: nucleus_txn::TxnId, _key: &[u8]) -> bool {
        false
    }
    fn before_point_read(&self, _txn: nucleus_txn::TxnId, _key: &[u8]) {}
    fn on_data_placed(
        &self,
        _writer: nucleus_txn::TxnId,
        _isolation: nucleus_txn::txn::Isolation,
        _key: &[u8],
    ) -> Result<(), TxnError> {
        Ok(())
    }
    fn pre_commit(
        &self,
        _txn: nucleus_txn::TxnId,
        _isolation: nucleus_txn::txn::Isolation,
        enqueue: &mut dyn FnMut() -> Result<(), TxnError>,
    ) -> Result<(), TxnError> {
        enqueue()?;
        Err(TxnError::SerializationFailure)
    }
    fn on_abort(&self, _txn: nucleus_txn::TxnId) {}
}

/// Merge review: once the request is on the channel the txn will commit
/// (§3), so an error from the hook after the enqueue must not abort it
/// (the commit thread would fail-stop on an Aborted status). It surfaces
/// as an invariant error, and the queued commit still succeeds. Mutant:
/// abort on any `pre_commit` error.
#[test]
fn pre_commit_error_after_enqueue_does_not_abort() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    core.set_ssi_hook(Arc::new(EnqueueThenFail));
    let mut pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
    let t = core.begin(nucleus_txn::txn::Isolation::ReadCommitted);
    let id = t.id;
    one_write_at(&core, &t, b"/t/1/k");
    let r = core.commit_submit(t, SyncCommit::On);
    assert!(matches!(r, Err(TxnError::Invariant(_))), "{:?}", r.err());
    let group = pipeline.drain_available();
    assert_eq!(group.len(), 1, "the request reached the channel");
    pipeline.process_group(group);
    let entry = core.status.entry(id).expect("status entry");
    assert!(
        matches!(entry.status, nucleus_txn::TxnStatus::Committed(_)),
        "{:?}",
        entry.status
    );
}
