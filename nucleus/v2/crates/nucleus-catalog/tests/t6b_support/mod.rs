//! Shared support for the C-T6b1 typed-layer tests (`tests/t6b1_*.rs`):
//! the t5 rig (a hand-driven core + SSI + lock manager + catalog + commit
//! pipeline over a `Fault<MemKv>`) with the typed [`Db`] assembled from
//! the same parts, plus the schema fixtures and the statement helpers the
//! tests drive. Every blocking wait is bounded — a timeout is a failure,
//! never a pass.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nucleus_catalog::{table_lo, Db, IndexColumn, IndexDef, IndexState, Stmt};
use nucleus_codec::row::{ColumnDef, Datum, RowError, TableSchema};
use nucleus_codec::{encode_key, Direction, KeyColumn, KeyType, Nulls, Value};
use nucleus_kv::fault::Fault;
use nucleus_kv::{Key, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{Catalog, Ddl, IdxRow, IdxStorage, RelRow};
use nucleus_txn::commit::{CommitConfig, CommitPipeline};
use nucleus_txn::jobs::{BootSweep, JobOutcome};
use nucleus_txn::locks::{LockManager, RelLockMode};
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

    // ---- C-T6b2a: index setup and entry inspection (§6) ----------------

    /// Creates an index (`catalog_create_index` at AccessExclusive, fresh
    /// storage id) and commits. Returns `(index oid, storage id)`.
    pub fn add_index(&self, owner: u64, def: &IndexDef) -> (u64, u64) {
        let isid = ok(self.catalog().alloc_storage_id(self.core()));
        let t = self.rc();
        let s = self.core().visible_ts();
        let (idx_oid, _) = ok(self.catalog().catalog_create_index(
            &self.ddl(&t, s),
            owner,
            IdxStorage::Id(isid),
            &ok(def.encode()),
        ));
        ok(self.commit_rc(t));
        (idx_oid, isid)
    }

    /// Rewrites an index's def (`catalog_alter_idx_def` at
    /// AccessExclusive — e.g. the state ladder a build drives) and
    /// commits.
    pub fn set_index_def(&self, idx_oid: u64, def: IndexDef) {
        let t = self.rc();
        let s = self.core().visible_ts();
        ok(self.catalog().catalog_alter_idx_def(
            &self.ddl(&t, s),
            idx_oid,
            &ok(def.encode()),
            RelLockMode::AccessExclusive,
        ));
        ok(self.commit_rc(t));
    }

    /// The visible entries of index storage `isid` at `visible_ts`
    /// (§4, through `read::scan` over both entry ranges — the oracle an
    /// index range read sees).
    pub fn scan_entries(&self, isid: u64) -> Vec<(Key, Vec<u8>)> {
        let ctx = ReadCtx {
            txn: ghost_id(self.core()),
            snapshot: self.core().visible_ts(),
            stmt_seq: 1,
        };
        let mut out = Vec::new();
        for (lo, hi) in nucleus_txn::catalog::index_ranges(isid) {
            let view = self.core().open_view();
            let rows = ok(nucleus_txn::read::scan(
                self.core(),
                &view,
                (
                    std::ops::Bound::Included(lo.as_slice()),
                    std::ops::Bound::Excluded(hi.as_slice()),
                ),
                &ctx,
                &mut nucleus_txn::read::NoSsi,
            )
            .collect::<Result<Vec<_>, _>>());
            out.extend(rows);
        }
        out
    }

    /// Every raw stored entry of index storage `isid`, both ranges:
    /// `(logical key, version ts — None for an intent, header byte,
    /// payload past the header)` in stored order. A tombstone's header is
    /// `0x02` (or `0x03`, moved), a live row's `0x00`/`0x01`.
    pub fn raw_entry_versions(&self, isid: u64) -> Vec<(Key, Option<Ts>, u8, Vec<u8>)> {
        use nucleus_kv::{OrderedKv as _, Snapshot as _};
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

impl Default for Rig {
    fn default() -> Self {
        Rig::new()
    }
}

// ---- C-T6b2b: a rig with a live commit thread + raw entry inspection ---

/// A rig whose commit pipeline runs on a real background thread
/// ([`spawn_commit_thread_with`]) instead of the hand-driven channel: for
/// entry points that begin, run and commit a txn by themselves and block
/// for the ack ([`create_index_own_txn`](nucleus_catalog::ddl::create_index_own_txn)).
/// Call [`ThreadedRig::shutdown`] when done (Drop only detaches the
/// thread).
pub struct ThreadedRig {
    pub db: Db<SharedFault>,
    pub kv: Arc<Fault<MemKv>>,
    commit: nucleus_txn::commit::CommitThreadHandle,
}

impl ThreadedRig {
    pub fn new() -> ThreadedRig {
        let kv = Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new())));
        let core = Arc::new(ok(Core::open(SharedFault(kv.clone()))));
        let ssi = Ssi::install(&core);
        let locks = LockManager::install(&core);
        let catalog = ok(Catalog::open(&core, &ssi, &locks));
        let db = Db::new(core.clone(), ssi.clone(), locks.clone(), catalog);
        let commit = ok(nucleus_txn::commit::spawn_commit_thread_with(
            Arc::clone(&core),
            CommitConfig::new().with_observer(ssi.commit_observer()),
        ));
        ThreadedRig { db, kv, commit }
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

    /// A fresh statement for `txn` at snapshot `s`.
    pub fn stmt<'t>(&self, txn: &'t Txn, s: Ts) -> Stmt<'t> {
        ok(Stmt::new(txn, s))
    }

    /// Resolves every queued intent (§7.3 through the resolver).
    pub fn settle(&self) {
        ok(Resolver::run_once(self.core()));
    }

    /// Submit through the running commit thread, wait for the ack,
    /// resolve — for a plain (non-SER) txn.
    pub fn commit_rc(&self, txn: Txn) -> Result<Ts, TxnError> {
        let ts = self
            .core()
            .commit(txn, nucleus_txn::commit::SyncCommit::On)?;
        self.settle();
        Ok(ts)
    }

    /// §7.1 abort + cleanup, for a plain (non-SER) txn.
    pub fn abort_txn(&self, txn: Txn) {
        ok(self.core().abort(txn));
        self.settle();
    }

    /// Index `oid`'s catalog row at `visible_ts`.
    pub fn idx_row(&self, idx_oid: u64) -> Option<IdxRow> {
        let view = self.core().open_view();
        let ctx = ReadCtx {
            txn: TxnId {
                epoch: self.core().epoch(),
                n: u64::MAX,
            },
            snapshot: self.core().visible_ts(),
            stmt_seq: 1,
        };
        ok(Catalog::read_idx(self.core(), &view, &ctx, idx_oid))
    }

    /// The visible entries of index storage `isid` at `visible_ts`
    /// (both entry ranges — the same oracle [`Rig::scan_entries`] is).
    pub fn scan_entries(&self, isid: u64) -> Vec<(Key, Vec<u8>)> {
        let ctx = ReadCtx {
            txn: TxnId {
                epoch: self.core().epoch(),
                n: u64::MAX,
            },
            snapshot: self.core().visible_ts(),
            stmt_seq: 1,
        };
        let mut out = Vec::new();
        for (lo, hi) in nucleus_txn::catalog::index_ranges(isid) {
            let view = self.core().open_view();
            let rows = ok(nucleus_txn::read::scan(
                self.core(),
                &view,
                (
                    std::ops::Bound::Included(lo.as_slice()),
                    std::ops::Bound::Excluded(hi.as_slice()),
                ),
                &ctx,
                &mut nucleus_txn::read::NoSsi,
            )
            .collect::<Result<Vec<_>, _>>());
            out.extend(rows);
        }
        out
    }

    /// Stops the commit thread (processing what precedes the stop) and
    /// joins it. A hang is a failure, never a pass.
    pub fn shutdown(self) {
        ok(self.commit.shutdown());
    }
}

impl Default for ThreadedRig {
    fn default() -> Self {
        ThreadedRig::new()
    }
}

/// Every stored entry (intent or version, any index storage) under the
/// `/i/` and `/u/` keyspaces: the "no entry anywhere survived" oracle for
/// an aborted or rolled-back CREATE INDEX whose storage id was never
/// committed and so cannot be named.
pub fn raw_entry_trace(rig: &Rig) -> Vec<Key> {
    use nucleus_kv::{OrderedKv as _, Snapshot as _};
    let mut out = Vec::new();
    let snap = rig.kv.snapshot();
    for (lo, hi) in [
        (b"/i/".to_vec(), b"/i0".to_vec()),
        (b"/u/".to_vec(), b"/u0".to_vec()),
    ] {
        for entry in snap.scan(
            (
                std::ops::Bound::Included(lo.as_slice()),
                std::ops::Bound::Excluded(hi.as_slice()),
            ),
            false,
        ) {
            let (stored, _) = ok(entry);
            out.push(stored);
        }
    }
    out
}

/// Plants a raw **live version** at logical key `k` with commit ts `ts`
/// and payload `payload` (`ingest_sorted`, C-T0 §10's primitive — the
/// amcheck-free way to give an index a stale entry no maintenance wrote).
pub fn plant_live_version(rig: &Rig, k: &[u8], ts: Ts, payload: &[u8]) {
    use nucleus_txn::encoding::version_key;
    let stored = version_key(k, ts);
    let mut value = Vec::with_capacity(1 + payload.len());
    value.push(0x00); // §2.2 header: live
    value.extend_from_slice(payload);
    ok(rig.kv.ingest_sorted(&mut std::iter::once((stored, value))));
}

/// Plants a raw stored version at logical key `k` with commit ts `ts`
/// through `Core::write` itself (C-T0's raw batch primitive — the
/// injection path the C-T6b3b amcheck tests use: no maintenance runs,
/// no intent exists, only the stored bytes appear). `header` is the §2.2
/// version header (`0x00`/`0x01` live, `0x02` tombstone); a tombstone
/// carries no payload.
pub fn core_plant_version(rig: &Rig, k: &[u8], ts: Ts, header: u8, payload: &[u8]) {
    use nucleus_kv::{Batch, Durability};
    use nucleus_txn::encoding::version_key;
    let mut value = Vec::with_capacity(1 + payload.len());
    value.push(header);
    value.extend_from_slice(payload);
    let mut batch = Batch::default();
    batch.put(version_key(k, ts), value);
    ok(rig.core().write(batch, Durability::No));
}

/// [`Rig::commit_rc`] with `synchronous_commit = off` (C-T0 §3 step 4's
/// off-prefix): the group is processed and acked with **no** `sync_wal`,
/// so the txn's intents and commit record stay in the fault's unsynced
/// tail — the crash-matrix primitive (a `Fault::crash(keep)` cut then
/// lands between them).
pub fn commit_off(rig: &Rig, txn: Txn) -> Ts {
    let ticket = ok(rig
        .core()
        .commit_submit(txn, nucleus_txn::commit::SyncCommit::Off));
    let n = rig.process();
    assert_eq!(n, 1, "one request per group in the hand-driven rig");
    ok(ticket.wait())
}

/// §7.2 resolve after a crash-reopen: drives the boot sweep to
/// `Finished` (it removes every older-epoch intent — `Resolve` for a
/// persisted `Committed` record, `Discard` otherwise — and records
/// `sweep_counter`), then one resolver pass for anything it queued.
pub fn sweep(rig: &Rig) {
    let mut sweep = BootSweep::new(Arc::clone(rig.core()));
    // The walk is over a finite store; a bound only turns a livelock
    // into a test failure.
    for _ in 0..100_000 {
        match ok(sweep.run_once()) {
            JobOutcome::Finished => break,
            JobOutcome::Progress | JobOutcome::Idle => continue,
        }
    }
    ok(Resolver::run_once(rig.core()));
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

// ---- C-T6b2a: §6 key oracles (hand-built, independent of the src) ----------

/// An index definition (§6 bytes) on one column.
pub fn idx_def(attnum: u16, unique: bool, state: IndexState) -> IndexDef {
    IndexDef {
        columns: vec![IndexColumn {
            attnum,
            dir: Direction::Asc,
            nulls: Nulls::Last,
        }],
        unique,
        nulls_not_distinct: false,
        concurrently: false,
        state,
        built_at: 0,
    }
}

/// The row key of pk values in table storage `sid` (§3):
/// `table_lo ‖ encode_key(pk ASC NULLS LAST)` — the independent oracle
/// for `pk_key`.
pub fn row_key_of(sid: u64, schema: &TableSchema, pk: &[Option<Value>]) -> Key {
    let cols: Vec<_> = schema
        .pk
        .iter()
        .map(|&attnum| {
            let col = schema
                .columns
                .iter()
                .find(|c| c.attnum == attnum)
                .expect("pk attnum");
            KeyColumn::asc(col.ty.clone())
        })
        .collect();
    let mut key = table_lo(sid);
    encode_key(&cols, pk, &mut key).expect("pk encodes");
    key
}

/// The `/u/{sid}/`-based entry logical key of `vals` (§6).
pub fn u_entry_key(isid: u64, cols: &[KeyColumn], vals: &[Option<Value>]) -> Key {
    let mut key = nucleus_txn::catalog::index_ranges(isid).remove(0).0;
    encode_key(cols, vals, &mut key).expect("index key encodes");
    key
}

/// The `/i/{sid}/`-based entry logical key of `vals` and its row key (§6).
pub fn i_entry_key(isid: u64, cols: &[KeyColumn], vals: &[Option<Value>], row_key: &[u8]) -> Key {
    let mut key = nucleus_txn::catalog::index_ranges(isid).remove(1).0;
    encode_key(cols, vals, &mut key).expect("index key encodes");
    key.extend_from_slice(row_key);
    key
}

/// One `(attnum, value)` assignment of an UPDATE's `set`.
pub fn set_col(attnum: u16, datum: Option<Datum>) -> (u16, Option<Datum>) {
    (attnum, datum)
}
