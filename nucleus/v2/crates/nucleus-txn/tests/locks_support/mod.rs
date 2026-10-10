//! Shared support for the C-T2b lock tests (§6). Nothing here is imported
//! from `tests/common`: the card's real-table seeds rebuild their
//! scenarios on top of [`LockManager`] alone. Helpers mirror the shapes
//! the C-T1b/C-T2 suites use, so the scenarios stay comparable.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::locks::LockManager;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, RowOp, RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::{RowLockMode, Ts, TxnError};

/// Panics on `Err` (test-side; the crate itself never unwraps).
pub fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// A parker maker whose parks never time out — a lost wakeup fails the
/// test by hanging past its 100 ms bound instead of being rescued by the
/// 1 s re-check — and that counts parks so a test can wait until a thread
/// really parked.
pub struct InfiniteParkers {
    parks: Arc<AtomicUsize>,
}

impl InfiniteParkers {
    pub fn new() -> Arc<InfiniteParkers> {
        Arc::new(InfiniteParkers {
            parks: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Waits until at least `n` parks were entered (then a small beat, so
    /// the parking threads block).
    pub fn wait_parked_n(&self, n: usize) {
        while self.parks.load(Ordering::SeqCst) < n {
            std::thread::sleep(Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    pub fn wait_parked(&self) {
        self.wait_parked_n(1);
    }
}

impl nucleus_txn::wait::MakeParker for InfiniteParkers {
    fn make(&self) -> Arc<dyn nucleus_txn::wait::Parker> {
        Arc::new(OneParker {
            inner: nucleus_txn::wait::CondvarParker::new(),
            parks: Arc::clone(&self.parks),
        })
    }
}

struct OneParker {
    inner: nucleus_txn::wait::CondvarParker,
    parks: Arc<AtomicUsize>,
}

impl nucleus_txn::wait::Parker for OneParker {
    fn park(&self, _timeout: Duration) -> bool {
        self.parks.fetch_add(1, Ordering::SeqCst);
        // Effectively infinite: the PARK_SLICE re-check must not rescue a
        // lost-wakeup test.
        self.inner.park(Duration::from_secs(3600))
    }

    fn unpark(&self) {
        self.inner.unpark();
    }
}

/// A hand-driven core + pipeline with the real lock manager installed
/// (row-lock table and release hook): the C-T2b stand-in for C-T2's
/// `TestRowLocks` rig. The pipeline sits behind an `Arc<Mutex<..>>` so a
/// test can run `process_group` (commit step 5) on another thread while
/// holding a key's latch.
pub struct Rig {
    pub core: Arc<Core<MemKv>>,
    pub pipeline: Arc<Mutex<CommitPipeline<MemKv>>>,
    pub locks: Arc<LockManager>,
}

impl Rig {
    pub fn new() -> Rig {
        let core = Arc::new(ok(Core::open(MemKv::new())));
        let locks = LockManager::install(&core);
        let pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        Rig {
            core,
            pipeline: Arc::new(Mutex::new(pipeline)),
            locks,
        }
    }

    pub fn txn(&self, iso: Isolation) -> Txn {
        self.core.begin(iso)
    }

    /// submit + drain + process_group + wait: a commit at a chosen point.
    pub fn commit(&mut self, txn: Txn) -> Ts {
        let ticket = ok(self.core.commit_submit(txn, SyncCommit::On));
        let mut p = self.pipeline.lock().expect("pipeline");
        let group = p.drain_available();
        p.process_group(group);
        drop(p);
        ok(ticket.wait())
    }

    /// Insert `key` = `value` in its own txn, commit, resolve. Returns ts.
    pub fn preload(&mut self, key: &[u8], value: &[u8]) -> Ts {
        let txn = self.txn(Isolation::ReadCommitted);
        let seq = ok(txn.next_seq());
        let s = self.core.visible_ts();
        ok(self.core.insert_key(
            &txn,
            key,
            None,
            value.to_vec(),
            StmtCtx::new(s, seq, seq),
            UniqueRule::Unique { same_row: None },
        ));
        let ts = self.commit(txn);
        ok(Resolver::run_once(&self.core));
        ts
    }

    /// The decoded intent of `key`, if present.
    pub fn intent(&self, key: &[u8]) -> Option<nucleus_txn::Intent> {
        self.core
            .latest_get(&nucleus_txn::encoding::intent_key(key))
            .ok()
            .flatten()
            .and_then(|raw| nucleus_txn::encoding::decode_intent(&raw).ok())
    }
}

/// An [`Epq`] that always applies a fixed op.
pub struct Fixed(pub RowOp);
impl Epq for Fixed {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Apply(self.0.clone())
    }
}

/// A shared row-lock-only request op (§6).
pub fn lock_op(mode: RowLockMode) -> RowOp {
    RowOp::Lock(mode)
}

/// An update of `key` to `value` in txn `t`'s statement `seq` (no newer
/// versions exist in these setups unless stated, so EPQ applies the same
/// op when it fires).
pub fn upd(core: &Core<MemKv>, t: &Txn, seq: u32, key: &[u8], value: &[u8]) -> RowOutcome {
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
    ))
}

/// A shared lock request of `mode` on `key` through the blocking driver.
pub fn lock_row(
    core: &Core<MemKv>,
    t: &Txn,
    seq: u32,
    key: &[u8],
    mode: RowLockMode,
) -> RowOutcome {
    ok(core.row_op(
        t,
        key,
        None,
        RowOp::Lock(mode),
        StmtCtx::new(core.visible_ts(), seq, seq),
        &mut Fixed(RowOp::Lock(mode)),
    ))
}

/// The owner of `key`'s current intent, if any.
pub fn intent_owner(core: &Core<MemKv>, key: &[u8]) -> Option<nucleus_txn::TxnId> {
    core.latest_get(&nucleus_txn::encoding::intent_key(key))
        .ok()
        .flatten()
        .and_then(|raw| nucleus_txn::encoding::decode_intent(&raw).ok())
        .map(|i| i.txn)
}

/// Holds `key`'s latch on a parked thread until released. A release path
/// that needs the latch cannot finish while it is held (seed 46).
pub struct LatchHolder {
    release: Arc<AtomicBool>,
}

impl LatchHolder {
    pub fn release(self) {
        self.release.store(true, Ordering::SeqCst);
    }
}

pub fn hold_latch(core: &Arc<Core<MemKv>>, key: &[u8]) -> LatchHolder {
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

/// Asserts `rx` stays empty for 200 ms (a blocked path must not finish).
pub fn not_within_200ms(rx: &std::sync::mpsc::Receiver<()>) {
    match rx.recv_timeout(Duration::from_millis(200)) {
        Ok(()) => panic!("finished while the latch was held"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("the blocked thread died while the latch was held")
        }
    }
}

/// A [`nucleus_txn::wait::WaitHook`] counting (waiter, target) starts and
/// ends: balanced counts after every thread joined mean every blocking
/// wait returned — no slot or parker leaked.
#[derive(Default)]
pub struct CountingWaitHook {
    pub starts: Mutex<std::collections::BTreeMap<(nucleus_txn::TxnId, nucleus_txn::TxnId), usize>>,
    pub ends: Mutex<std::collections::BTreeMap<(nucleus_txn::TxnId, nucleus_txn::TxnId), usize>>,
}

impl nucleus_txn::wait::WaitHook for CountingWaitHook {
    fn on_wait_start(&self, waiter: nucleus_txn::TxnId, target: nucleus_txn::TxnId) {
        *self
            .starts
            .lock()
            .expect("starts")
            .entry((waiter, target))
            .or_insert(0) += 1;
    }

    fn on_wait_end(&self, waiter: nucleus_txn::TxnId, target: nucleus_txn::TxnId) {
        *self
            .ends
            .lock()
            .expect("ends")
            .entry((waiter, target))
            .or_insert(0) += 1;
    }
}

/// The phantom error the tests never expect.
#[allow(dead_code)]
pub fn never(e: TxnError) -> ! {
    panic!("unexpected error: {e:?}")
}
