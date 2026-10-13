//! Shared support for the C-T6b1 typed-layer tests (`tests/t6b1_*.rs`):
//! the t5 rig (a hand-driven core + SSI + lock manager + catalog + commit
//! pipeline over a `Fault<MemKv>`) with the typed [`Db`] assembled from
//! the same parts, plus the schema fixtures and the statement helpers the
//! tests drive. Every blocking wait is bounded — a timeout is a failure,
//! never a pass.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nucleus_catalog::{Db, Stmt};
use nucleus_codec::row::{ColumnDef, Datum, RowError, TableSchema};
use nucleus_codec::{KeyType, Value};
use nucleus_kv::fault::Fault;
use nucleus_kv::{Key, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{Catalog, Ddl, IdxRow, RelRow};
use nucleus_txn::commit::{CommitConfig, CommitPipeline};
use nucleus_txn::locks::LockManager;
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::StmtCtx;
use nucleus_txn::{Ts, TxnError, TxnId};

pub fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// `OrderedKv` over an `Arc<Fault<MemKv>>` the test keeps, so a `Core` can
/// be opened on the fault while the test still drives `crash` (the
/// `t5_support::SharedFault` pattern).
#[derive(Clone)]
pub struct SharedFault(pub Arc<Fault<MemKv>>);

impl OrderedKv for SharedFault {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(
        &self,
        batch: nucleus_kv::Batch,
        sync: nucleus_kv::Durability,
    ) -> nucleus_kv::Result<()> {
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
        self.0.set_gc_filter(filter)
    }
    fn set_gc_watermark(&self, watermark: u64) -> nucleus_kv::Result<()> {
        self.0.set_gc_watermark(watermark)
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

/// The typed test rig: the t5 rig's parts with the [`Db`] on top, the
/// commit pipeline driven by hand.
pub struct Rig {
    pub db: Db<SharedFault>,
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
        let db = Db::new(core.clone(), ssi.clone(), locks.clone(), catalog);
        let pipeline = ok(CommitPipeline::with_config(
            Arc::clone(&core),
            CommitConfig::new().with_observer(ssi.commit_observer()),
        ));
        Rig {
            db,
            kv,
            pipeline: Mutex::new(pipeline),
        }
    }

    pub fn core(&self) -> &Arc<Core<SharedFault>> {
        &self.db.core
    }

    pub fn ssi(&self) -> &Arc<Ssi> {
        &self.db.ssi
    }

    pub fn locks(&self) -> &Arc<LockManager> {
        &self.db.locks
    }

    pub fn catalog(&self) -> &Catalog {
        &self.db.catalog
    }

    /// Crashes (keeping `keep` unsynced batches) and reopens the whole
    /// stack on the surviving store: new epoch, new SSI state, the catalog
    /// and the typed layer rebuilt from the committed rows.
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
        ok(Resolver::run_once(self.core()));
    }

    /// A fresh RC txn.
    pub fn rc(&self) -> Txn {
        self.core().begin(Isolation::ReadCommitted)
    }

    /// An RR txn with its registered snapshot, kept by the caller (the
    /// caller owns snapshot registration, §3.1).
    pub fn rr(&self) -> (Txn, SnapshotGuard<'_>, Ts) {
        let txn = self.core().begin(Isolation::RepeatableRead);
        let guard = self.core().registry.take_snapshot();
        let s = guard.ts();
        (txn, guard, s)
    }

    /// A SERIALIZABLE txn with its SSI entry and registered snapshot.
    pub fn ser(&self, read_only: bool) -> Ser<'_> {
        let txn = self.core().begin(Isolation::Serializable);
        let guard = ok(self.ssi().begin(self.core(), &txn, read_only));
        Ser {
            s: guard.ts(),
            txn,
            guard,
        }
    }

    /// The blocking Ddl for `txn` at snapshot `s` (§6 default wait).
    pub fn ddl<'c>(&'c self, txn: &'c Txn, s: Ts) -> Ddl<'c, SharedFault> {
        let seq = txn.next_seq().expect("seq");
        Ddl::new(self.core(), txn, StmtCtx::new(s, seq, seq))
    }

    /// Submit, process the group, wait for the ack, resolve — for a plain
    /// (non-SER) txn.
    pub fn commit_rc(&self, txn: Txn) -> Result<Ts, TxnError> {
        let ticket = self
            .core()
            .commit_submit(txn, nucleus_txn::commit::SyncCommit::On)?;
        self.process();
        let ts = ticket.wait()?;
        self.settle();
        Ok(ts)
    }

    /// §7.1 abort + cleanup, for a plain (non-SER) txn.
    pub fn abort_txn(&self, txn: Txn) {
        ok(self.core().abort(txn));
        self.settle();
    }

    /// The §7.1 abort of a SER txn (guard dropped after the abort).
    pub fn abort_ser(&self, t: Ser<'_>) {
        let Ser { txn, guard, .. } = t;
        ok(self.core().abort(txn));
        self.settle();
        drop(guard);
    }

    /// A fresh statement for `txn` at snapshot `s`.
    pub fn stmt<'t>(&self, txn: &'t Txn, s: Ts) -> Stmt<'t> {
        ok(Stmt::new(txn, s))
    }

    /// Relation `oid`'s catalog row at `visible_ts` (a fresh reader).
    pub fn rel_row(&self, oid: u64) -> Option<RelRow> {
        let view = self.core().open_view();
        let ctx = ReadCtx {
            txn: ghost_id(self.core()),
            snapshot: self.core().visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::read_rel(self.core(), &view, &ctx, oid))
    }

    /// Index `oid`'s catalog row at `visible_ts`.
    pub fn idx_row(&self, idx_oid: u64) -> Option<IdxRow> {
        let view = self.core().open_view();
        let ctx = ReadCtx {
            txn: ghost_id(self.core()),
            snapshot: self.core().visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::read_idx(self.core(), &view, &ctx, idx_oid))
    }

    /// The wait-for graph's edges (t2 parked on t1 shows as `(t2, t1)`).
    pub fn wait_edges(&self) -> Vec<(TxnId, TxnId)> {
        self.core().wait_edges()
    }
}

impl Default for Rig {
    fn default() -> Self {
        Rig::new()
    }
}

/// A reader id that owns nothing and is never allocated (status ids start
/// at 1).
fn ghost_id(core: &Core<SharedFault>) -> TxnId {
    TxnId {
        epoch: core.epoch(),
        n: u64::MAX,
    }
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

// ---- schema and datum fixtures -----------------------------------------------

/// `CREATE TABLE t (id int4 PRIMARY KEY, payload text)` — a pk table.
pub fn pk_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![
            ColumnDef {
                attnum: 1,
                name: "id".into(),
                ty: KeyType::Int4,
                not_null: true,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 2,
                name: "payload".into(),
                ty: KeyType::Text(nucleus_codec::Collation::C),
                not_null: false,
                dropped: false,
                missing: None,
            },
        ],
        vec![1],
    ))
}

/// `CREATE TABLE t (k text PRIMARY KEY, v int4)` — a text-pk table (the
/// key-limit tests).
pub fn text_pk_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![
            ColumnDef {
                attnum: 1,
                name: "k".into(),
                ty: KeyType::Text(nucleus_codec::Collation::C),
                not_null: true,
                dropped: false,
                missing: None,
            },
            ColumnDef {
                attnum: 2,
                name: "v".into(),
                ty: KeyType::Int4,
                not_null: false,
                dropped: false,
                missing: None,
            },
        ],
        vec![1],
    ))
}

/// `CREATE TABLE t (v int4)` — a no-PK table (hidden rowid, §3).
pub fn rowid_schema() -> TableSchema {
    ok(TableSchema::new(
        vec![ColumnDef {
            attnum: 1,
            name: "v".into(),
            ty: KeyType::Int4,
            not_null: false,
            dropped: false,
            missing: None,
        }],
        Vec::new(),
    ))
}

/// An inline int4 datum.
pub fn i32_d(n: i32) -> Option<Datum> {
    Some(Datum::Inline(Value::Int4(n)))
}

/// An inline text datum.
pub fn text_d(s: &str) -> Option<Datum> {
    Some(Datum::Inline(Value::Text(s.to_string())))
}

/// An int4 value (for pk lookups).
pub fn i32_v(n: i32) -> Option<Value> {
    Some(Value::Int4(n))
}

/// A text value (for pk lookups).
pub fn text_v(s: String) -> Option<Value> {
    Some(Value::Text(s))
}

/// A schema's decoded form from raw `RelRow.def` bytes (assertion helper).
pub fn decode_def(def: &[u8]) -> TableSchema {
    match TableSchema::decode(def) {
        Ok(s) => s,
        Err(RowError::InvalidDdl(m)) => panic!("invalid def: {m}"),
        Err(e) => panic!("def decode: {e:?}"),
    }
}
