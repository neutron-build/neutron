//! Shared support for the C-T5 catalog tests (`tests/t5_*.rs`): a
//! hand-driven core + SSI + lock manager + catalog over a `Fault<MemKv>`
//! (the card's prescribed wrapper), the SER/RC txn steps the tests drive,
//! crash/reopen, and an SSI-storage-map probe (a writer W gives a
//! `Relation { rel }`-SIREAD holder R an edge `R -> W` exactly when W's
//! key maps to `rel`, §8.2 writer side — the observable form of the map,
//! which has no getter).
#![allow(dead_code)]

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, Key, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{
    table_range, Catalog, Ddl, IdxRow, KeyRange, RelRow, SYS_CATALOG_PREFIX,
};
use nucleus_txn::commit::{CommitConfig, CommitObserver, CommitPipeline, CommitTicket, SyncCommit};
use nucleus_txn::locks::LockManager;
use nucleus_txn::read::NoSsi;
use nucleus_txn::registry::{SnapshotGuard, ViewGuard};
use nucleus_txn::removal::{remove_intent, RemovalMode};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::{Siread, Ssi};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{RowOp, RowOutcome, StmtCtx, UniqueRule};
use nucleus_txn::{Ts, TxnError, TxnId};

pub fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// `OrderedKv` over an `Arc<Fault<MemKv>>` the test keeps, so a `Core` can
/// be opened on the fault while the test still drives `crash` (the
/// `gc_support::SharedKv` pattern, over `Fault`).
#[derive(Clone)]
pub struct SharedFault(pub Arc<Fault<MemKv>>);

impl OrderedKv for SharedFault {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> nucleus_kv::Result<()> {
        self.0.write(batch, sync)
    }
    fn sync_wal(&self) -> nucleus_kv::Result<()> {
        self.0.sync_wal()
    }
    fn snapshot(&self) -> Self::Snap {
        self.0.snapshot()
    }
    fn get_latest(&self, key: &[u8]) -> nucleus_kv::Result<Option<Vec<u8>>> {
        self.0.get_latest(key)
    }
    fn ingest_sorted(
        &self,
        entries: &mut dyn Iterator<Item = (Key, Vec<u8>)>,
    ) -> nucleus_kv::Result<()> {
        self.0.ingest_sorted(entries)
    }
    fn checkpoint(&self, dir: &std::path::Path) -> nucleus_kv::Result<()> {
        self.0.checkpoint(dir)
    }
    fn set_gc_filter(&self, filter: Box<dyn nucleus_kv::GcFilter>) {
        self.0.set_gc_filter(filter);
    }
    fn set_gc_watermark(&self, watermark: u64) -> nucleus_kv::Result<()> {
        self.0.set_gc_watermark(watermark)
    }
}

/// A counted signal: `hit` counts and wakes every waiter; waits panic on
/// their bound — a timeout is a failure, never a pass.
pub struct Flag {
    n: Mutex<u64>,
    cv: Condvar,
}

impl Flag {
    pub fn new() -> Flag {
        Flag {
            n: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, u64> {
        self.n.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn hit(&self) {
        let mut n = self.lock();
        *n += 1;
        self.cv.notify_all();
    }

    pub fn get(&self) -> u64 {
        *self.lock()
    }

    pub fn wait_at_least(&self, at_least: u64, bound: Duration) {
        let deadline = Instant::now() + bound;
        let mut n = self.lock();
        while *n < at_least {
            let now = Instant::now();
            if now >= deadline {
                panic!("flag reached only {} of {at_least} within {bound:?}", *n);
            }
            let (n2, timeout) = self
                .cv
                .wait_timeout(n, deadline - now)
                .unwrap_or_else(PoisonError::into_inner);
            n = n2;
            if timeout.timed_out() && *n < at_least && Instant::now() >= deadline {
                panic!("flag reached only {} of {at_least} within {bound:?}", *n);
            }
        }
    }
}

/// A SERIALIZABLE txn with its registered snapshot (the `ssi_support`
/// pattern).
pub struct Ser<'a> {
    pub txn: Txn,
    pub s: Ts,
    pub(crate) guard: SnapshotGuard<'a>,
}

impl Ser<'_> {
    pub fn id(&self) -> TxnId {
        self.txn.id
    }

    /// A fresh statement's ReadCtx (§4).
    pub fn ctx(&self) -> ReadCtx {
        ReadCtx {
            txn: self.txn.id,
            snapshot: self.s,
            stmt_seq: self.txn.next_seq().expect("seq"),
        }
    }
}

/// The catalog test rig: core + SSI + lock manager + catalog over one
/// `Fault<MemKv>`, with the commit pipeline driven by hand.
pub struct Rig {
    pub core: Arc<Core<SharedFault>>,
    pub ssi: Arc<Ssi>,
    pub locks: Arc<LockManager>,
    pub catalog: Catalog,
    pub kv: Arc<Fault<MemKv>>,
    pub(crate) pipeline: Mutex<CommitPipeline<SharedFault>>,
}

impl Rig {
    pub fn new() -> Rig {
        Rig::over(Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))))
    }

    fn over(kv: Arc<Fault<MemKv>>) -> Rig {
        let core = Arc::new(ok(Core::open(SharedFault(kv.clone()))));
        let ssi = Ssi::install(&core);
        let locks = LockManager::install(&core);
        let catalog = ok(Catalog::open(&core, &ssi, &locks));
        let pipeline = ok(CommitPipeline::with_config(
            Arc::clone(&core),
            CommitConfig::new().with_observer(ssi.commit_observer()),
        ));
        Rig {
            core,
            ssi,
            locks,
            catalog,
            kv,
            pipeline: Mutex::new(pipeline),
        }
    }

    /// Crashes (keeping `keep` unsynced batches) and reopens the whole
    /// stack on the surviving store: new epoch, new SSI state, the catalog
    /// rebuilt from the committed rows.
    pub fn crash_reopen(&self, keep: usize) -> Rig {
        ok(self.kv.crash(keep));
        Rig::over(self.kv.clone())
    }

    /// Drains the channel into one group and runs §3 steps 1-5 on it.
    pub fn process(&self) -> usize {
        let mut p = self.pipeline.lock().expect("pipeline");
        let group = p.drain_available();
        let n = group.len();
        p.process_group(group);
        n
    }

    /// Resolves every queued intent (§7.3 through the resolver).
    pub fn settle(&self) {
        ok(Resolver::run_once(&self.core));
    }

    /// A SERIALIZABLE txn with its SSI entry and registered snapshot.
    pub fn ser(&self, read_only: bool) -> Ser<'_> {
        let txn = self.core.begin(Isolation::Serializable);
        let guard = ok(self.ssi.begin(&self.core, &txn, read_only));
        Ser {
            s: guard.ts(),
            txn,
            guard,
        }
    }

    /// A fresh RC statement ctx at `visible_ts` (§4: RC re-takes per
    /// statement).
    pub fn rc(&self) -> Txn {
        self.core.begin(Isolation::ReadCommitted)
    }

    /// The blocking Ddl for `txn` at snapshot `s` (§6 default wait).
    pub fn ddl<'c>(&'c self, txn: &'c Txn, s: Ts) -> Ddl<'c, SharedFault> {
        let seq = txn.next_seq().expect("seq");
        Ddl::new(&self.core, txn, StmtCtx::new(s, seq, seq))
    }

    /// `catalog_create_table` driven end to end at snapshot `s`.
    pub fn create_table(&self, txn: &Txn, s: Ts, name: &[u8]) -> (u64, u64) {
        ok(self.catalog.catalog_create_table(
            &self.ddl(txn, s),
            name,
            nucleus_txn::catalog::RelKind::Table,
            b"",
        ))
    }

    /// `catalog_truncate_table` driven end to end.
    pub fn truncate_table(&self, txn: &Txn, s: Ts, oid: u64) -> u64 {
        ok(self.catalog.catalog_truncate_table(&self.ddl(txn, s), oid))
    }

    /// `catalog_drop_table` driven end to end.
    pub fn drop_table(&self, txn: &Txn, s: Ts, oid: u64) {
        ok(self.catalog.catalog_drop_table(&self.ddl(txn, s), oid));
    }

    /// Inserts `key = value` through the write path in `txn` (a DML
    /// stand-in; no unique semantics).
    pub fn insert(&self, txn: &Txn, s: Ts, key: &[u8], value: &[u8]) {
        let seq = txn.next_seq().expect("seq");
        ok(self.core.insert_key(
            txn,
            key,
            None,
            value.to_vec(),
            StmtCtx::new(s, seq, seq),
            UniqueRule::None,
        ));
    }

    /// A no-unique-check point read of `key` at snapshot `s` through a
    /// fresh registered view (§4; the ghost reader owns nothing).
    pub fn read_at(&self, key: &[u8], s: Ts) -> Option<Vec<u8>> {
        let view = self.core.open_view();
        let ctx = ReadCtx {
            txn: ghost_id(&self.core),
            snapshot: s,
            stmt_seq: 1,
        };
        ok(nucleus_txn::read::read_key(
            &self.core, &view, key, &ctx, &mut NoSsi,
        ))
    }

    /// The SERIALIZABLE read of `key` (SIREAD first, §8.1).
    pub fn ssi_read(&self, t: &Ser<'_>, key: &[u8]) -> Option<Vec<u8>> {
        ok(self.ssi.read_key(&self.core, t.id(), key, &t.ctx()))
    }

    /// Pre-commit + enqueue only (the txn is prepared, not committed).
    pub fn submit(&self, t: Ser<'_>) -> Result<CommitTicket, TxnError> {
        let Ser { txn, guard, .. } = t;
        let r = self.core.commit_submit(txn, SyncCommit::On);
        drop(guard);
        r
    }

    /// Submit, process the group, wait for the ack, resolve.
    pub fn commit(&self, t: Ser<'_>) -> Result<Ts, TxnError> {
        let ticket = self.submit(t)?;
        self.process();
        let ts = ticket.wait()?;
        self.settle();
        Ok(ts)
    }

    /// A blocking DDL txn: commit_submit + process + ack + resolve, for a
    /// plain (non-SER) txn.
    pub fn commit_rc(&self, txn: Txn) -> Result<Ts, TxnError> {
        let ticket = self.core.commit_submit(txn, SyncCommit::On)?;
        self.process();
        let ts = ticket.wait()?;
        self.settle();
        Ok(ts)
    }

    pub fn abort(&self, t: Ser<'_>) {
        let Ser { txn, guard, .. } = t;
        ok(self.core.abort(txn));
        self.settle();
        drop(guard);
    }

    /// Relation `oid`'s catalog row at `visible_ts` (a fresh reader).
    pub fn rel_row(&self, oid: u64) -> Option<RelRow> {
        let view = self.core.open_view();
        let ctx = ReadCtx {
            txn: ghost_id(&self.core),
            snapshot: self.core.visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::read_rel(&self.core, &view, &ctx, oid))
    }

    /// Index `oid`'s catalog row at `visible_ts`.
    pub fn idx_row(&self, oid: u64) -> Option<IdxRow> {
        let view = self.core.open_view();
        let ctx = ReadCtx {
            txn: ghost_id(&self.core),
            snapshot: self.core.visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::read_idx(&self.core, &view, &ctx, oid))
    }

    /// Relation `oid`'s row read at the SER reader's snapshot (the
    /// catalog-snapshot rule through a registered view).
    pub fn rel_row_at(&self, t: &Ser<'_>, oid: u64) -> Option<RelRow> {
        let view = self.core.open_view();
        ok(Catalog::read_rel(
            &self.core,
            &view,
            &ReadCtx {
                txn: t.id(),
                snapshot: t.s,
                stmt_seq: 1,
            },
            oid,
        ))
    }

    /// Whether SSI's storage map maps `key`'s range to `rel` (§8.2 writer
    /// side, the map's observable form): a fresh SER holder of
    /// `Relation { rel }` gets an edge from a SER writer on `key` exactly
    /// when the map resolves `key` to `rel`.
    pub fn storage_maps(&self, key: &[u8], rel: u32) -> bool {
        let r = self.ser(false);
        ok(self.ssi.lock_relation_read(r.id(), rel));
        let w = self.ser(false);
        let seq = w.txn.next_seq().expect("seq");
        ok(self.core.insert_key(
            &w.txn,
            key,
            None,
            b"probe".to_vec(),
            StmtCtx::new(w.s, seq, seq),
            UniqueRule::None,
        ));
        let edge = self.ssi.edges().contains(&(r.id(), w.id()));
        self.abort(w);
        self.abort(r);
        edge
    }

    /// The catalog's committed `/sys/catalog/rel/` rows at `visible_ts`.
    pub fn catalog_rows(&self) -> Vec<(Key, Vec<u8>)> {
        let view: ViewGuard<'_, <MemKv as OrderedKv>::Snap> = self.core.open_view();
        view.scan(
            (
                std::ops::Bound::Included(SYS_CATALOG_PREFIX),
                std::ops::Bound::Unbounded,
            ),
            false,
        )
        .map(ok)
        .filter(|(k, _)| k.starts_with(SYS_CATALOG_PREFIX))
        .collect()
    }

    /// Resolves a committed-but-unresolved intent by hand (§7.3): the
    /// post-crash resolution step of the crash matrix.
    pub fn resolve_intent(&self, key: &[u8], owner: TxnId) {
        ok(remove_intent(
            &self.core,
            key,
            None,
            owner,
            RemovalMode::Resolve,
        ));
    }

    /// The wait-for graph's edges (t2 parked on t1 shows as `(t2, t1)`).
    pub fn wait_edges(&self) -> Vec<(TxnId, TxnId)> {
        self.core.wait_edges()
    }

    /// A row op through the public driver (the DML stand-in for waits).
    pub fn row_update(&self, txn: &Txn, s: Ts, key: &[u8], value: &[u8]) -> RowOutcome {
        let seq = txn.next_seq().expect("seq");
        ok(self.core.row_op(
            txn,
            key,
            None,
            RowOp::Update {
                value: value.to_vec(),
                key_cols_changed: false,
            },
            StmtCtx::new(s, seq, seq),
            &mut NoEpq,
        ))
    }
}

impl Default for Rig {
    fn default() -> Self {
        Rig::new()
    }
}

/// RR/SER never run EPQ; RC callers here never need it.
pub struct NoEpq;

impl nucleus_txn::write::Epq for NoEpq {
    fn recheck(
        &mut self,
        _newest: &nucleus_txn::write::CommittedVersion,
    ) -> nucleus_txn::write::EpqDecision {
        nucleus_txn::write::EpqDecision::Skip
    }
}

/// A reader id that owns nothing and is never allocated (status ids start
/// at 1; `u64::MAX` is beyond every test's budget).
fn ghost_id(core: &Core<SharedFault>) -> TxnId {
    TxnId {
        epoch: core.epoch(),
        n: u64::MAX,
    }
}

/// A commit observer that does nothing (rigs that never commit SER txns
/// through it still need the slot filled).
pub struct NoObserver;

impl CommitObserver for NoObserver {
    fn on_assigned(&self, _txn: TxnId, _ts: Ts) {}
}

/// The SIREADs of `txn`, tags dropped (diagnostic shaping).
pub fn sireads(ssi: &Ssi, txn: TxnId) -> Vec<Siread> {
    ssi.sireads_of(txn).into_iter().map(|(s, _)| s).collect()
}

/// Waits until `f` holds, panicking after `bound` (a timeout is a failure).
pub fn wait_until(bound: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + bound;
    while !f() {
        if Instant::now() >= deadline {
            panic!("condition not reached within {bound:?}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A key inside storage `sid`'s table range (the be64 prefix
/// [`table_range`] defines, plus a suffix): the probe key of map tests.
pub fn table_key(sid: u64, suffix: &str) -> Vec<u8> {
    let (lo, _) = table_range(sid);
    let mut k = lo;
    k.extend_from_slice(suffix.as_bytes());
    k
}

/// A key-range pair as `[lo, hi)` bytes.
pub fn range_bytes(r: &KeyRange) -> (&[u8], &[u8]) {
    (r.0.as_slice(), r.1.as_slice())
}
