//! C-T2b stress test (§6): 8 real threads, 6 keys, one relation; each txn
//! takes the relation lock first (a random mode), then locks 2-3 keys in
//! random order with a random mix of UPDATE, `FOR SHARE` and
//! `FOR KEY SHARE`, and commits or aborts at random; `deadlock_timeout`
//! is 20 ms and every 40P01 aborts and retries. Row waits run through a
//! test-side wrapper of the step API (`wait_begin` / park / `wait_poll` /
//! `wait_deadlock_check` / `wait_end`) that captures `wait_edges()`
//! **before** each deadlock check, so every recorded 40P01 victim is
//! verifiably on a cycle of the current edges — never a flag the code
//! sets. Afterwards: no edge, no row/relation/advisory lock left (the
//! wait-hook's start/end counts balance, so every blocking wait returned
//! and unregistered), no thread hangs, at least 1000 txns, under 20 s.

mod locks_support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use locks_support::CountingWaitHook;
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::wait::{WaitBegin, WaitOutcome};
use nucleus_txn::write::{
    EpqDecision, LockWait, RowLocks as _, RowOp, RowOpTask, RowOutcome, Step, StmtCtx,
};
use nucleus_txn::{RowLockMode, TxnError, TxnId};

const THREADS: usize = 8;
const TXNS_PER_THREAD: usize = 150; // 8 * 150 = 1200 >= 1000
const KEYS: [&[u8]; 6] = [
    b"/t/1/k0", b"/t/1/k1", b"/t/1/k2", b"/t/1/k3", b"/t/1/k4", b"/t/1/k5",
];
const REL: u64 = 77;
const DEADLOCK_TIMEOUT: Duration = Duration::from_millis(20);
/// One park slice of the wrapper's loop: short enough that the deadlock
/// check fires on time, long enough that wakes (unparks) dominate.
const PARK: Duration = Duration::from_millis(1);
/// No wait may outlive this: the hang guard (a lost wake or a leaked
/// waiter fails the test instead of hanging it).
const HANG: Duration = Duration::from_secs(10);

/// Deterministic per-thread RNG (a plain LCG; the workload only needs
/// reproducible choices, not cryptographic quality).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_889_119_407 >> 1);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Whether `w` is on a cycle of `edges` (test-side re-derivation over the
/// captured snapshot, independent of the crate's DFS).
fn on_cycle(edges: &[(TxnId, TxnId)], w: TxnId) -> bool {
    use std::collections::BTreeSet;
    let map: BTreeMap<TxnId, Vec<TxnId>> = edges.iter().fold(BTreeMap::new(), |mut m, (a, b)| {
        m.entry(*a).or_default().push(*b);
        m
    });
    let mut stack: Vec<TxnId> = map.get(&w).cloned().unwrap_or_default();
    let mut seen: BTreeSet<TxnId> = BTreeSet::new();
    while let Some(t) = stack.pop() {
        if t == w {
            return true;
        }
        if !seen.insert(t) {
            continue;
        }
        if let Some(next) = map.get(&t) {
            stack.extend(next.iter().copied());
        }
    }
    false
}

/// Every 40P01 victim with the `wait_edges()` snapshot captured inside
/// (right before) its deadlock check.
type Victims = Mutex<Vec<(TxnId, Vec<(TxnId, TxnId)>)>>;

/// The test-side wrapper of the §6 wait steps: begin, park in short
/// slices, poll, run the deadlock check once after `deadlock_timeout`
/// (capturing `wait_edges()` first), end. Returns the driver-style
/// mapping: cancel 57014, deadlock 40P01; anything else retries.
fn wait_steps(
    core: &Arc<Core<MemKv>>,
    txn: &Txn,
    targets: &[(TxnId, u64)],
    victims: &Victims,
) -> Result<(), TxnError> {
    let h = match core.wait_begin(txn, targets) {
        WaitBegin::Done(_) => return Ok(()),
        WaitBegin::Registered(h) => h,
    };
    let began = Instant::now();
    let dl_at = began + DEADLOCK_TIMEOUT;
    let mut checked = false;
    let out = loop {
        if let Some(o) = core.wait_poll(&h) {
            break map_poll(o);
        }
        if txn.is_cancelled() {
            break Err(TxnError::QueryCanceled);
        }
        let now = Instant::now();
        let mut bound = PARK;
        if !checked && now < dl_at {
            bound = bound.min(dl_at - now);
        }
        h.park(bound);
        if let Some(o) = core.wait_poll(&h) {
            break map_poll(o);
        }
        if !checked && Instant::now() >= dl_at {
            checked = true;
            // Capture the edges BEFORE the check (the check removes the
            // victim's own edges): the victim must be on a cycle of the
            // current edges, computed test-side, never from a code flag.
            let edges = core.wait_edges();
            if core.wait_deadlock_check(&h) {
                assert!(
                    on_cycle(&edges, txn.id),
                    "a 40P01 victim was on a cycle of the captured edges"
                );
                victims.lock().expect("victims").push((txn.id, edges));
                break Err(TxnError::Deadlock);
            }
        }
        assert!(
            began.elapsed() < HANG,
            "a row wait hung: a wake was lost or a waiter leaked"
        );
    };
    core.wait_end(h);
    out
}

fn map_poll(o: WaitOutcome) -> Result<(), TxnError> {
    match o {
        WaitOutcome::Cancelled => Err(TxnError::QueryCanceled),
        WaitOutcome::Deadlock => Err(TxnError::Deadlock),
        WaitOutcome::LockTimeout => Err(TxnError::LockNotAvailable),
        WaitOutcome::Ended
        | WaitOutcome::Aborted
        | WaitOutcome::Committed(_)
        | WaitOutcome::GenChanged => Ok(()),
    }
}

/// One row op driven through the C-T2 step API with the wrapper's waits.
fn row_op_steps(
    core: &Arc<Core<MemKv>>,
    txn: &Txn,
    key: &[u8],
    op: RowOp,
    victims: &Victims,
) -> Result<RowOutcome, TxnError> {
    let seq = txn.next_seq()?;
    let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
    let mut task = RowOpTask::new(key, None, op.clone(), ctx);
    loop {
        if txn.is_cancelled() {
            return Err(TxnError::QueryCanceled);
        }
        match task.step(core, txn)? {
            Step::Done(o) => return Ok(o),
            Step::Again => {}
            Step::Epq(_) => {
                task.epq_result(EpqDecision::Apply(op.clone()))?;
            }
            Step::Wait(w) => wait_steps(core, txn, &w, victims)?,
            Step::Restart => return Err(TxnError::Invariant("no arbiter in the stress".into())),
        }
    }
}

#[test]
fn stress_locks_deadlocks_and_no_leaks() {
    let start = Instant::now();
    let core = Arc::new(Core::open(MemKv::new()).expect("core"));
    let locks = LockManager::install(&core);
    let handle = spawn_commit_thread(Arc::clone(&core)).expect("commit thread");
    core.waits.set_deadlock_timeout(DEADLOCK_TIMEOUT);

    let hook = Arc::new(CountingWaitHook::default());
    core.waits
        .set_hook(Arc::clone(&hook) as Arc<dyn nucleus_txn::wait::WaitHook>);

    // Preload every key so UPDATE ops always target a live row.
    for k in KEYS {
        let t = core.begin(Isolation::ReadCommitted);
        let s = t.next_seq().expect("seq");
        core.insert_key(
            &t,
            k,
            None,
            b"v".to_vec(),
            StmtCtx::new(core.visible_ts(), s, s),
            nucleus_txn::write::UniqueRule::Unique { same_row: None },
        )
        .expect("preload");
        core.commit(t, SyncCommit::On).expect("preload commit");
    }

    let victims: Arc<Victims> = Arc::new(Mutex::new(Vec::new()));
    let deadlocks = Arc::new(AtomicUsize::new(0));
    let modes = [
        RelLockMode::AccessShare,
        RelLockMode::RowShare,
        RelLockMode::RowExclusive,
        RelLockMode::ShareUpdateExclusive,
        RelLockMode::Share,
        RelLockMode::ShareRowExclusive,
        RelLockMode::Exclusive,
        RelLockMode::AccessExclusive,
    ];

    let mut threads = Vec::new();
    for tid in 0..THREADS {
        let core = Arc::clone(&core);
        let locks = Arc::clone(&locks);
        let victims = Arc::clone(&victims);
        let deadlocks = Arc::clone(&deadlocks);
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng(tid as u64 * 2_654_435_761 + 12_345);
            let mut finished = 0usize;
            'txn: while finished < TXNS_PER_THREAD {
                let txn = core.begin(Isolation::ReadCommitted);
                // The relation lock first: no cycle can pass through a
                // relation edge, so every deadlock victim below is a row
                // waiter the wrapper observes (the card's record).
                let mode = modes[rng.below(modes.len())];
                if let Err(e) = locks.lock_relation(&core, &txn, REL, mode, LockWait::Block, None) {
                    match e {
                        TxnError::Deadlock => {
                            deadlocks.fetch_add(1, Ordering::SeqCst);
                            core.abort(txn).expect("victim abort");
                            finished += 1;
                            continue;
                        }
                        other => panic!("relation lock: {other:?}"),
                    }
                }
                // 2-3 distinct keys in random order, random mix of UPDATE,
                // FOR SHARE and FOR KEY SHARE.
                let mut idx: Vec<usize> = (0..KEYS.len()).collect();
                for i in (1..idx.len()).rev() {
                    let j = rng.below(i + 1);
                    idx.swap(i, j);
                }
                let nkeys = 2 + rng.below(2);
                for &k in &idx[..nkeys] {
                    let op = match rng.below(3) {
                        0 => RowOp::Update {
                            value: b"s".to_vec(),
                            key_cols_changed: false,
                        },
                        1 => RowOp::Lock(RowLockMode::Share),
                        _ => RowOp::Lock(RowLockMode::KeyShare),
                    };
                    if let Err(TxnError::Deadlock) =
                        row_op_steps(&core, &txn, KEYS[k], op, &victims)
                    {
                        deadlocks.fetch_add(1, Ordering::SeqCst);
                        core.abort(txn).expect("victim abort");
                        finished += 1;
                        continue 'txn;
                    }
                }
                if rng.below(2) == 0 {
                    core.commit(txn, SyncCommit::Off).expect("commit");
                } else {
                    core.abort(txn).expect("abort");
                }
                finished += 1;
            }
            finished
        }));
    }
    let mut total = 0usize;
    for t in threads {
        total += t.join().expect("no thread panicked (no hang)");
    }

    // Shut the commit thread down first: shutdown drains and processes
    // everything left, so every commit's step 5 (release) has run.
    handle.shutdown().expect("commit thread shutdown");

    let elapsed = start.elapsed();
    assert!(total >= 1000, "at least 1000 txns, got {total}");
    assert!(
        elapsed < Duration::from_secs(20),
        "the stress finished in {elapsed:?}"
    );
    let dl = deadlocks.load(Ordering::SeqCst);
    assert!(
        dl >= 1,
        "the workload produced at least one 40P01 (got {dl})"
    );

    // No edge, no slot, no lock left.
    assert!(
        core.wait_edges().is_empty(),
        "no wait-for edge leaked: {:?}",
        core.wait_edges()
    );
    for k in KEYS {
        assert!(
            locks.row_table().holders(k).is_empty(),
            "no row lock leaked on {k:?}"
        );
    }
    // Every blocking wait returned: hook starts and ends balance per
    // (waiter, target) pair — no slot or parker leaked.
    {
        let starts = hook.starts.lock().expect("starts");
        let ends = hook.ends.lock().expect("ends");
        let mut keys: Vec<_> = starts.keys().collect();
        keys.extend(ends.keys());
        keys.sort();
        keys.dedup();
        for k in keys {
            assert_eq!(
                starts.get(k).copied().unwrap_or(0),
                ends.get(k).copied().unwrap_or(0),
                "wait hook unbalanced for {k:?}"
            );
        }
    }
    // Every 40P01 victim was on a cycle of the edges captured inside its
    // deadlock check (re-checked over the recordings).
    {
        let v = victims.lock().expect("victims");
        assert_eq!(v.len(), dl, "every 40P01 went through the wrapper");
        for (w, edges) in v.iter() {
            assert!(
                on_cycle(edges, *w),
                "victim {w:?} was on a cycle of {edges:?}"
            );
        }
    }
    // Behaviorally: a fresh txn can take everything (no live holder left).
    {
        let fresh = core.begin(Isolation::ReadCommitted);
        assert!(locks
            .lock_relation(
                &core,
                &fresh,
                REL,
                RelLockMode::AccessExclusive,
                LockWait::NoWait,
                None
            )
            .expect("relation probe"));
        for key in [1i64, 3, 5] {
            assert!(locks
                .advisory_xact_lock(&core, &fresh, key, false, LockWait::NoWait, None)
                .expect("advisory probe"));
        }
        core.abort(fresh).expect("probe abort");
    }
}
