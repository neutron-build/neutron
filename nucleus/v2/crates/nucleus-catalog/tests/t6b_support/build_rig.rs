//! The C-T6e2a build rig: the `t6b_support` parts with a **live commit
//! thread** (the online build driver blocks for each step's ack, so the
//! hand-driven pipeline cannot serve it) over the shared `Fault<MemKv>`,
//! reopenable across a `crash(keep)` through `Db::open` — the boot path
//! whose `cleanup_unfinished_builds` the resume/crash tests pin. The `Db`
//! sits in an `Arc` so the concurrent-writer tests can share it.

use std::sync::Arc;

use nucleus_catalog::{amcheck, Db, IndexDef, Stmt};
use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, Key, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{Catalog, IdxRow, RelRow};
use nucleus_txn::commit::{spawn_commit_thread_with, CommitConfig, CommitThreadHandle, SyncCommit};
use nucleus_txn::locks::LockManager;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnId};

use super::{i32_d, ok, pk_schema, text_d, SharedFault};

/// The threaded, crash-reopenable rig.
pub struct BuildRig {
    pub db: Arc<Db<SharedFault>>,
    pub kv: Arc<Fault<MemKv>>,
    commit: Option<CommitThreadHandle>,
}

impl BuildRig {
    /// A fresh rig on a fresh fault store.
    pub fn new() -> BuildRig {
        BuildRig::over(Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))))
    }

    /// Assembles the whole stack over `kv`, through `Db::open` (whose
    /// cleanup runs before the handle returns — the boot path).
    fn over(kv: Arc<Fault<MemKv>>) -> BuildRig {
        let core = Arc::new(ok(Core::open(SharedFault(kv.clone()))));
        let ssi = Ssi::install(&core);
        let locks = LockManager::install(&core);
        let commit = ok(spawn_commit_thread_with(
            Arc::clone(&core),
            CommitConfig::new().with_observer(ssi.commit_observer()),
        ));
        let catalog = ok(Catalog::open(&core, &ssi, &locks));
        let db = ok(Db::open(core.clone(), ssi, locks, catalog));
        BuildRig {
            db: Arc::new(db),
            kv,
            commit: Some(commit),
        }
    }

    /// Crashes (keeping the first `keep` unsynced batches — the build's
    /// own commits are `SyncCommit::On`, so they survive even `keep = 0`),
    /// stops this rig's commit thread and reopens the whole stack through
    /// `Db::open` (whose cleanup runs before the handle returns).
    pub fn crash_reopen(self, keep: usize) -> BuildRig {
        let mut me = self;
        if let Some(handle) = me.commit.take() {
            ok(handle.shutdown());
        }
        let kv = me.kv.clone();
        ok(me.kv.crash(keep));
        drop(me);
        BuildRig::over(kv)
    }

    /// Stops the commit thread (processing what precedes the stop).
    pub fn shutdown(mut self) {
        if let Some(handle) = self.commit.take() {
            ok(handle.shutdown());
        }
    }

    pub fn core(&self) -> &Arc<Core<SharedFault>> {
        &self.db.core
    }

    pub fn catalog(&self) -> &Catalog {
        &self.db.catalog
    }

    /// A fresh RC txn.
    pub fn rc(&self) -> Txn {
        self.core().begin(Isolation::ReadCommitted)
    }

    /// A fresh statement for `txn` at `visible_ts`.
    pub fn stmt<'t>(&self, txn: &'t Txn, s: Ts) -> Stmt<'t> {
        ok(Stmt::new(txn, s))
    }

    /// Commit through the running pipeline (blocking for the ack), then
    /// resolve — for a plain (non-SER) txn.
    pub fn commit_rc(&self, txn: Txn) -> Result<Ts, nucleus_txn::TxnError> {
        let ts = self.core().commit(txn, SyncCommit::On)?;
        self.settle();
        Ok(ts)
    }

    /// Resolves every queued intent (§7.3 through the resolver).
    pub fn settle(&self) {
        ok(Resolver::run_once(self.core()));
    }

    fn ghost_ctx(&self) -> ReadCtx {
        ReadCtx {
            txn: TxnId {
                epoch: self.core().epoch(),
                n: u64::MAX,
            },
            snapshot: self.core().visible_ts(),
            stmt_seq: 1,
        }
    }

    /// Relation `oid`'s catalog row at `visible_ts` (a ghost reader).
    pub fn rel_row(&self, oid: u64) -> Option<RelRow> {
        let view = self.core().open_view();
        ok(Catalog::read_rel(
            self.core(),
            &view,
            &self.ghost_ctx(),
            oid,
        ))
    }

    /// Index `oid`'s catalog row at `visible_ts`.
    pub fn idx_row(&self, idx_oid: u64) -> Option<IdxRow> {
        let view = self.core().open_view();
        ok(Catalog::read_idx(
            self.core(),
            &view,
            &self.ghost_ctx(),
            idx_oid,
        ))
    }

    /// Every visible index row of relation `oid` at `visible_ts`.
    pub fn idxs_of(&self, oid: u64) -> Vec<IdxRow> {
        let view = self.core().open_view();
        ok(Catalog::scan_idxs_of_rel(
            self.core(),
            &view,
            &self.ghost_ctx(),
            oid,
        ))
    }

    /// Plants a raw stored live version at logical key `k` with commit ts
    /// `ts` (no intent, no maintenance — the failure-injection primitive
    /// of the `t6b_support` rig, on this rig's core).
    pub fn plant_live_version(&self, k: &[u8], ts: Ts, payload: &[u8]) {
        let mut value = Vec::with_capacity(1 + payload.len());
        value.push(0x00); // §2.2 header: live
        value.extend_from_slice(payload);
        let mut batch = Batch::default();
        batch.put(nucleus_txn::encoding::version_key(k, ts), value);
        ok(self.core().write(batch, Durability::No));
    }

    /// The committed def of an index row (panics when absent).
    pub fn def_of(&self, idx_oid: u64) -> IndexDef {
        let row = self.idx_row(idx_oid).expect("idx row committed");
        ok(IndexDef::decode(&row.def))
    }

    /// The index storage id of a committed index row.
    pub fn isid_of(&self, idx_oid: u64) -> u64 {
        match self.idx_row(idx_oid).expect("idx row committed").storage {
            nucleus_txn::catalog::IdxStorage::Id(isid) => isid,
            nucleus_txn::catalog::IdxStorage::Range(..) => {
                panic!("an online-built index has an id storage")
            }
        }
    }

    /// amcheck at the latest committed state (a fresh registered
    /// snapshot).
    pub fn amcheck(&self, oid: u64) -> amcheck::Report {
        let guard = self.core().registry.take_snapshot();
        ok(amcheck::check_table(&self.db, oid, &guard))
    }

    /// amcheck at a caller-chosen ts (`register_at`, admitted while
    /// `t >= W`).
    pub fn amcheck_at(&self, oid: u64, ts: Ts) -> amcheck::Report {
        let guard = ok(self.core().registry.register_at(ts));
        ok(amcheck::check_table(&self.db, oid, &guard))
    }

    /// Every raw stored entry of index storage `isid`, both ranges:
    /// `(logical key, version ts — None for an intent, header byte,
    /// payload past the header)` in stored order.
    pub fn raw_idx_versions(&self, isid: u64) -> Vec<(Key, Option<Ts>, u8, Vec<u8>)> {
        use nucleus_kv::Snapshot as _;
        let mut out = Vec::new();
        for (lo, hi) in nucleus_txn::catalog::index_ranges(isid) {
            let snap = self.kv.snapshot();
            for entry in snap.scan(
                (
                    std::ops::Bound::Included(lo.as_slice()),
                    std::ops::Bound::Excluded(hi.as_slice()),
                ),
                false,
            ) {
                let (stored, value) = ok(entry);
                let (logical, kind) = match nucleus_txn::encoding::parse_key(&stored) {
                    Some(parsed) => parsed,
                    None => continue,
                };
                let ts = match kind {
                    nucleus_txn::encoding::Entry::Version(ts) => Some(ts),
                    nucleus_txn::encoding::Entry::Intent => None,
                };
                let header = value.first().copied().unwrap_or(0xFF);
                out.push((
                    logical.to_vec(),
                    ts,
                    header,
                    value[1.min(value.len())..].to_vec(),
                ));
            }
        }
        out
    }
}

impl Default for BuildRig {
    fn default() -> Self {
        BuildRig::new()
    }
}

/// `CREATE TABLE t (id int4 PRIMARY KEY, payload text)`, committed, plus
/// one committed row per `(id, payload)` pair — each row its own txn, so
/// every commit ts is distinct and single.
pub fn table_with_rows(rig: &BuildRig, name: &[u8], rows: &[(i32, &str)]) -> u64 {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let (oid, _) = ok(rig.db.create_table(&st, name, &pk_schema()));
    ok(rig.commit_rc(t));
    for &(id, payload) in rows {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig
            .db
            .open_table(&st, oid, nucleus_txn::locks::RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(id), text_d(payload)]));
        ok(rig.commit_rc(t));
    }
    oid
}
