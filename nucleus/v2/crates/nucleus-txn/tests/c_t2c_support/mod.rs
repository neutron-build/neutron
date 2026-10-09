//! Test support for the C-T2c tests (copied from `tests/common`, which this
//! card never edits): `ok`, the infinite-timeout parker, the I-COUNT
//! checker, the shared-lock table double, an SSI hook that records the
//! view counter at `before_point_read`, and a hand-driven rig.
#![allow(dead_code)]

use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitPipeline, SyncCommit};
use nucleus_txn::encoding::{decode_intent, intent_key};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{CommittedVersion, Epq, EpqDecision, RowOp, StmtCtx, UniqueRule};
use nucleus_txn::{Intent, Ts, TxnError};

pub fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// A parker maker whose parks never time out and that counts parks.
pub struct InfiniteParkers {
    parks: Arc<AtomicUsize>,
}

impl InfiniteParkers {
    pub fn new() -> Arc<InfiniteParkers> {
        Arc::new(InfiniteParkers {
            parks: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn parks(&self) -> usize {
        self.parks.load(Ordering::SeqCst)
    }

    /// Waits until more than `before` parks were entered (bounded), then a
    /// small beat.
    pub fn wait_parked_after(&self, before: usize) {
        self.wait_parked_unless(before, &|| false);
    }

    /// Like [`InfiniteParkers::wait_parked_after`], but returns early (no
    /// panic) once `done()` holds — the waiter thread finished without
    /// parking, so the caller's join surfaces what it returned.
    pub fn wait_parked_unless(&self, before: usize, done: &dyn Fn() -> bool) {
        for _ in 0..10_000 {
            if done() {
                return;
            }
            if self.parks.load(Ordering::SeqCst) > before {
                std::thread::sleep(Duration::from_millis(5));
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("the waiter never parked");
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
        self.inner.park(Duration::from_secs(3600))
    }

    fn unpark(&self) {
        self.inner.unpark();
    }
}

/// I-COUNT: `intent_count(txn)` equals the intents the txn owns in the KV,
/// and the write-set log names each of them.
pub fn assert_count_exact(core: &Core<MemKv>, txn: &Txn) {
    let view = core.open_view();
    let mut owned: Vec<Vec<u8>> = Vec::new();
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (k, v) = ok(row);
        if let Some((logical, nucleus_txn::encoding::Entry::Intent)) =
            nucleus_txn::encoding::parse_key(&k)
        {
            let intent = ok(decode_intent(&v));
            if intent.txn == txn.id {
                owned.push(logical.to_vec());
            }
        }
    }
    drop(view);
    let count = core
        .status
        .entry(txn.id)
        .map(|e| e.intent_count)
        .unwrap_or_default();
    assert_eq!(
        count,
        owned.len() as i64,
        "I-COUNT: {count} counted vs {owned:?} owned in the KV"
    );
    let log = txn.write_set_keys();
    for key in &owned {
        assert!(
            log.iter().any(|(k, _)| k == key),
            "I-COUNT: the write-set log does not name the owned intent {key:?}"
        );
    }
}

/// Every `@INTENT` in the KV.
pub fn count_intents(core: &Core<MemKv>) -> usize {
    let view = core.open_view();
    let mut n = 0;
    for row in view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    ) {
        let (k, _) = ok(row);
        if nucleus_txn::encoding::parse_key(&k)
            .is_some_and(|(_, e)| matches!(e, nucleus_txn::encoding::Entry::Intent))
        {
            n += 1;
        }
    }
    n
}

type Grant = (
    nucleus_kv::Key,
    nucleus_txn::TxnId,
    nucleus_txn::RowLockMode,
    nucleus_txn::Seq,
    Option<usize>,
);

/// The Vec-backed shared-lock table double.
#[derive(Default)]
pub struct TestRowLocks {
    grants: Mutex<Vec<Grant>>,
}

impl TestRowLocks {
    pub fn new() -> Arc<TestRowLocks> {
        Arc::new(TestRowLocks::default())
    }
}

impl nucleus_txn::write::RowLocks for TestRowLocks {
    fn holders(
        &self,
        key: &[u8],
    ) -> Vec<(
        nucleus_txn::TxnId,
        nucleus_txn::RowLockMode,
        nucleus_txn::Seq,
    )> {
        self.grants
            .lock()
            .expect("grants")
            .iter()
            .filter(|(k, _, _, _, _)| k.as_slice() == key)
            .map(|(_, t, m, s, _)| (*t, *m, *s))
            .collect()
    }

    fn grant(
        &self,
        key: &[u8],
        txn: nucleus_txn::TxnId,
        mode: nucleus_txn::RowLockMode,
        seq: nucleus_txn::Seq,
        latch_prefix: Option<usize>,
    ) -> Result<(), TxnError> {
        self.grants
            .lock()
            .expect("grants")
            .push((key.to_vec(), txn, mode, seq, latch_prefix));
        Ok(())
    }

    fn keys_of(
        &self,
        txn: nucleus_txn::TxnId,
        from_seq: nucleus_txn::Seq,
    ) -> Vec<(nucleus_kv::Key, Option<usize>)> {
        self.grants
            .lock()
            .expect("grants")
            .iter()
            .filter(|(_, t, _, s, _)| *t == txn && *s >= from_seq)
            .map(|(k, _, _, _, p)| (k.clone(), *p))
            .collect()
    }

    fn release(&self, key: &[u8], txn: nucleus_txn::TxnId, from_seq: nucleus_txn::Seq) {
        self.grants
            .lock()
            .expect("grants")
            .retain(|(k, t, _, s, _)| !(k.as_slice() == key && *t == txn && *s >= from_seq));
    }
}

/// An SSI hook that records, at each `before_point_read`, the registry's
/// view counter (so a test can prove the SIREAD came before the view).
#[derive(Default)]
pub struct OrderSsi {
    pub core: Mutex<Weak<Core<MemKv>>>,
    pub point_reads: Mutex<Vec<(Vec<u8>, u64)>>,
}

impl nucleus_txn::write::SsiHook for OrderSsi {
    fn covers(&self, _txn: nucleus_txn::TxnId, _key: &[u8]) -> bool {
        false
    }

    fn before_point_read(&self, _txn: nucleus_txn::TxnId, key: &[u8]) {
        let counter = self
            .core
            .lock()
            .expect("core")
            .upgrade()
            .map_or(0, |c| c.registry.with_registry(|st| st.view_counter()));
        self.point_reads
            .lock()
            .expect("reads")
            .push((key.to_vec(), counter));
    }

    fn on_data_placed(
        &self,
        _writer: nucleus_txn::TxnId,
        _isolation: Isolation,
        _key: &[u8],
    ) -> Result<(), TxnError> {
        Ok(())
    }

    fn pre_commit(
        &self,
        _txn: nucleus_txn::TxnId,
        _isolation: Isolation,
        enqueue: &mut dyn FnMut() -> Result<(), TxnError>,
    ) -> Result<(), TxnError> {
        enqueue()
    }

    fn on_abort(&self, _txn: nucleus_txn::TxnId) {}
}

/// An [`Epq`] that always applies a fixed op.
pub struct Fixed(pub RowOp);
impl Epq for Fixed {
    fn recheck(&mut self, _newest: &CommittedVersion) -> EpqDecision {
        EpqDecision::Apply(self.0.clone())
    }
}

/// A hand-driven core: commits go through `commit_submit` +
/// `process_group` + `try_ack`; resolution through `Resolver::run_once`.
pub struct Rig {
    pub core: Arc<Core<MemKv>>,
    pipeline: RefCell<CommitPipeline<MemKv>>,
}

impl Rig {
    pub fn new() -> Rig {
        let core = Arc::new(ok(Core::open(MemKv::new())));
        core.set_row_locks(TestRowLocks::new());
        let pipeline = ok(CommitPipeline::new(Arc::clone(&core)));
        Rig {
            core,
            pipeline: RefCell::new(pipeline),
        }
    }

    /// Installs the infinite-timeout parker and returns it.
    pub fn parkers(&self) -> Arc<InfiniteParkers> {
        let p = InfiniteParkers::new();
        self.core.waits.set_parker_maker(p.clone());
        p
    }

    pub fn txn(&self, iso: Isolation) -> Txn {
        self.core.begin(iso)
    }

    pub fn commit(&self, txn: Txn) -> Ts {
        let ticket = ok(self.core.commit_submit(txn, SyncCommit::On));
        let mut p = self.pipeline.borrow_mut();
        let group = p.drain_available();
        p.process_group(group);
        drop(p);
        ok(ticket.try_ack().expect("acked after process_group"))
    }

    pub fn resolve(&self) {
        while ok(Resolver::run_once(&self.core)) > 0 {}
    }

    /// Inserts `key` = `value` in its own txn; commits and resolves.
    pub fn preload(&self, key: &[u8], value: &[u8]) -> Ts {
        let t = self.txn(Isolation::ReadCommitted);
        let seq = ok(t.next_seq());
        ok(self.core.insert_key(
            &t,
            key,
            None,
            value.to_vec(),
            StmtCtx::new(self.core.visible_ts(), seq, seq),
            UniqueRule::Unique { same_row: None },
        ));
        let ts = self.commit(t);
        self.resolve();
        ts
    }

    /// Updates `key` to `value` in its own txn; commits and resolves.
    pub fn update(&self, key: &[u8], value: &[u8]) -> Ts {
        let t = self.txn(Isolation::ReadCommitted);
        let seq = ok(t.next_seq());
        let op = RowOp::Update {
            value: value.to_vec(),
            key_cols_changed: false,
        };
        ok(self.core.row_op(
            &t,
            key,
            None,
            op.clone(),
            StmtCtx::new(self.core.visible_ts(), seq, seq),
            &mut Fixed(op),
        ));
        let ts = self.commit(t);
        self.resolve();
        ts
    }

    /// `txn` deletes `key` in statement `seq` (no EPQ expected).
    pub fn delete_in(&self, txn: &Txn, seq: u32, key: &[u8]) {
        ok(self.core.row_op(
            txn,
            key,
            None,
            RowOp::Delete,
            StmtCtx::new(self.core.visible_ts(), seq, seq),
            &mut Fixed(RowOp::Delete),
        ));
    }

    /// The intent of `key`, if any.
    pub fn intent(&self, key: &[u8]) -> Option<Intent> {
        let raw = ok(self.core.latest_get(&intent_key(key)))?;
        Some(ok(decode_intent(&raw)))
    }

    /// The committed value of `key` in the latest visible state, through a
    /// registered snapshot and view.
    pub fn read_latest(&self, key: &[u8]) -> Option<Vec<u8>> {
        let snap = self.core.registry.take_snapshot();
        let view = self.core.open_view();
        ok(read_key(
            &self.core,
            &view,
            key,
            &ReadCtx {
                txn: nucleus_txn::TxnId { epoch: 0, n: 0 },
                snapshot: snap.ts(),
                stmt_seq: 0,
            },
            &mut NoSsi,
        ))
    }
}

pub fn u64v(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

pub fn as_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b.try_into().expect("8-byte value"))
}
