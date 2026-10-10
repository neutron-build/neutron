//! C-T2c round 3: the insert-step race coverage (§5.3.1(2)), ported from
//! the review harness that measured mutants M5 and M10. Many fresh keys,
//! `THREADS` txns per key released together by a barrier, mixed `DO
//! NOTHING` / `DO UPDATE`: when a key's unique check finds a concurrent
//! winner **during the insert step** (both attempts passed the pre-check
//! while the key was dead), the loser must abandon and restart from (1),
//! never raise 23505 — and exactly one txn may insert the key.
//!
//! - M5 removes the restart for the primary-key arbiter (`pk_arbiter`: the
//!   `/t/` key's unique check is the arbiter check): the losers' insert
//!   steps raise 23505 and the PK test fails.
//! - M10 removes it for a `/u/` arbiter entry: ditto on the entry test.
//!
//! The shipped stress test's keys each go dead→live only once, so its
//! insert-step race window opens only at first touch; these tests open it
//! on every round (the stress test additionally recycles keys, see
//! `c_t2c_stress.rs`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::time::Duration;

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::resolver::spawn_background;
use nucleus_txn::txn::{CancelHandle, Isolation, Txn};
use nucleus_txn::write::{IndexEntry, OnConflictAction, OnConflictResult, ProposedRow, StmtCtx};
use nucleus_txn::TxnError;

const THREADS: usize = 8;
const ROUNDS: usize = 400;
const WATCHDOG_AFTER: Duration = Duration::from_secs(30);

type Cancels = Arc<Mutex<Vec<CancelHandle>>>;

/// One round's outcome, recorded per thread: the round and its result.
type RoundLog = Arc<Mutex<Vec<(usize, Result<OnConflictResult, TxnError>)>>>;

fn begin(core: &Core<MemKv>, cancels: &Cancels) -> Txn {
    let t = core.begin(Isolation::ReadCommitted);
    cancels.lock().expect("cancels").push(t.cancel_handle());
    t
}

/// The shared harness: barrier-synchronized rounds, a watchdog that cancels
/// everything (failing the test through its error assertions) instead of
/// hanging, and the per-round result collection with the two oracle
/// assertions: no error at all (so no 23505 and no 40001/40P01 either) and
/// exactly one insert per round.
fn run_race(
    body: impl Fn(usize, usize, &Core<MemKv>, &Cancels) -> Result<OnConflictResult, TxnError>
        + Send
        + Sync
        + Copy
        + 'static,
) {
    let core = Arc::new(Core::open(MemKv::new()).expect("core"));
    let commit_handle = spawn_commit_thread(Arc::clone(&core)).expect("commit thread");
    let bg = spawn_background(Arc::clone(&core)).expect("resolver");
    let cancels: Cancels = Arc::new(Mutex::new(Vec::new()));
    let fired = Arc::new(AtomicBool::new(false));

    let (wd_tx, wd_rx) = mpsc::channel::<()>();
    let watchdog = {
        let cancels = Arc::clone(&cancels);
        let fired = Arc::clone(&fired);
        std::thread::spawn(move || {
            if wd_rx.recv_timeout(WATCHDOG_AFTER).is_err() {
                fired.store(true, Ordering::SeqCst);
                for c in cancels.lock().expect("cancels").iter() {
                    c.cancel();
                }
            }
        })
    };

    let barrier = Arc::new(Barrier::new(THREADS));
    let log: RoundLog = Arc::new(Mutex::new(Vec::new()));
    let hs: Vec<_> = (0..THREADS)
        .map(|i| {
            let core = Arc::clone(&core);
            let cancels = Arc::clone(&cancels);
            let barrier = Arc::clone(&barrier);
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                for round in 0..ROUNDS {
                    barrier.wait();
                    let r = body(i, round, &core, &cancels);
                    log.lock().expect("log").push((round, r));
                }
            })
        })
        .collect();
    for h in hs {
        h.join().expect("race thread");
    }
    wd_tx.send(()).expect("watchdog alive");
    watchdog.join().expect("watchdog");
    bg.stop().expect("resolver stop");
    commit_handle.shutdown().expect("shutdown");

    assert!(!fired.load(Ordering::SeqCst), "watchdog fired: a txn hung");
    let mut inserted = vec![0usize; ROUNDS];
    let mut errs: Vec<(usize, TxnError)> = Vec::new();
    for (round, r) in log.lock().expect("log").drain(..) {
        match r {
            Ok(OnConflictResult::Inserted) => inserted[round] += 1,
            Ok(
                OnConflictResult::Updated
                | OnConflictResult::Nothing
                | OnConflictResult::WhereFalse,
            ) => {}
            Err(e) => errs.push((round, e)),
        }
    }
    assert!(
        errs.is_empty(),
        "{} errors over {} rounds, e.g. {:?}",
        errs.len(),
        ROUNDS,
        errs.first()
    );
    let many: Vec<usize> = inserted
        .iter()
        .enumerate()
        .filter(|(_, &n)| n != 1)
        .map(|(r, _)| r)
        .collect();
    assert!(
        many.is_empty(),
        "rounds without exactly one insert: {many:?} (inserted {inserted:?})"
    );
}

/// The round body shared by both arbiter kinds: one RC txn per round,
/// `DO NOTHING` on even threads and `DO UPDATE v = v + 1` on odd ones.
fn upsert_round(
    core: &Core<MemKv>,
    cancels: &Cancels,
    row: ProposedRow,
    t_key_of: &dyn Fn(&[u8]) -> nucleus_kv::Key,
    thread: usize,
    mut f: impl FnMut(&[u8]) -> Option<(Vec<u8>, bool)>,
) -> Result<OnConflictResult, TxnError> {
    let txn = begin(core, cancels);
    let snap = core.registry.take_snapshot();
    let seq0 = txn.next_seq()?;
    let action = if thread.is_multiple_of(2) {
        OnConflictAction::DoNothing
    } else {
        OnConflictAction::DoUpdate(&mut f)
    };
    let r = core.insert_on_conflict(
        &txn,
        StmtCtx::new(snap.ts(), seq0, seq0),
        row,
        t_key_of,
        &mut |_| {},
        action,
    );
    drop(snap);
    match r {
        Ok(o) => {
            core.commit(txn, SyncCommit::Off)?;
            Ok(o.result)
        }
        Err(e) => {
            core.abort(txn)?;
            Err(e)
        }
    }
}

/// M5: the PK-arbiter insert-step restart. All threads propose the **same**
/// `/t/` key with `pk_arbiter`: one inserts, the others must restart from
/// (1) and skip/update — never 23505 — when the `/t/` unique check meets
/// the winner.
#[test]
fn pk_arbiter_race_fresh_pks_one_insert_never_23505() {
    run_race(|thread, round, core, cancels| {
        let row = ProposedRow {
            t_key: format!("/t/r/{round}").into_bytes(),
            value: vec![1],
            entries: vec![],
            pk_arbiter: true,
        };
        upsert_round(
            core,
            cancels,
            row,
            &|p: &[u8]| p.to_vec(),
            thread,
            |v: &[u8]| Some((vec![v[0].wrapping_add(1)], false)),
        )
    });
}

/// M10: the `/u/`-entry insert-step restart. Each thread proposes its own
/// `/t/` key carrying the **same** arbiter entry; the loser of the entry's
/// unique check must restart from (1) and skip/update — never 23505.
#[test]
fn entry_arbiter_race_fresh_keys_one_insert_never_23505() {
    run_race(|thread, round, core, cancels| {
        let row = ProposedRow {
            t_key: format!("/t/r/{round}-{thread}").into_bytes(),
            value: vec![1],
            entries: vec![IndexEntry {
                key: format!("/u/a/{round}").into_bytes(),
                value: format!("{round}-{thread}").into_bytes(),
                arbiter: true,
            }],
            pk_arbiter: false,
        };
        upsert_round(
            core,
            cancels,
            row,
            &|p: &[u8]| [b"/t/r/".as_slice(), p].concat(),
            thread,
            |v: &[u8]| Some((vec![v[0].wrapping_add(1)], false)),
        )
    });
}
