//! The g2x runner (card C-T6g item 2): sessions over one shared
//! `Db<MemKv>` — the real lock manager, catalog, commit thread, resolver
//! and SSI retention — whose 12 slots are the **rows** of an
//! access-method table `t(pk int8 PRIMARY KEY, v int8, m int8)`:
//! slot `k` is the row `pk = k`, its value `Val::new(uid, n)` lives in
//! `v`, and the mirror column `m` holds `pk` (so the one online-built
//! index `CREATE INDEX CONCURRENTLY ON t (m)` observes the same slot
//! space a pk scan does, and an index range scan recorded as
//! `OpKind::Scan` leaves the checker's semantics unchanged).
//!
//! A [`Session`] is one transaction attempt at RC/RR/SER over the real
//! statement paths: reads through `Db::get` / `Db::scan` /
//! `Db::index_scan` (the §4 "an index serves a read at `S` iff `Public`
//! and `built_at <= S`" rule is the **engine's** — the runner *attempts*
//! the index read whenever the index exists and answers through the pk
//! scan on the engine's own 0A000, which is what keeps the `built_at`
//! rule observable instead of pre-empted by the harness), inserts
//! through `Db::insert_with` (so index maintenance runs), and
//! updates/deletes through the §5 row-op paths with the runner's own EPQ
//! callback — the one place `Db::update_with`/`delete_with` cannot serve
//! the recorder: their EPQ base (the version the row op actually acted
//! on, which the checker's version order is built from) is internal, and
//! recording the statement's read instead would mislabel every RC
//! re-base as a lost update. Index maintenance is provably a no-op for
//! these updates (an entry on `m = pk` is a function of `(m, row key)`,
//! neither of which moves when only `v` does — §6's update maintenance
//! itself no-ops on an unchanged entry) and is replicated by hand for
//! deletes (the §6 blind entry delete through `Core::delete_entry`, in
//! every index state, the entry key computed from public primitives).
//!
//! The DDL session issues ADD COLUMN (no-rewrite, rows untouched) and
//! TRUNCATE — an RC txn that takes AccessExclusive **first**, reads every
//! slot under it (no writer can commit behind the lock), records
//! `Write::Del` for each live one and swaps the storage through
//! `catalog_truncate_table` — plus **one**
//! [`create_index_online`](nucleus_catalog::create_index_online)`(concurrently = true)`
//! per run, recorded as a pseudo-txn whose fate is the driver call's own
//! outcome.
//!
//! **The relation window.** A TRUNCATE that commits between an RR/SER
//! txn's snapshot and its next statement owes that statement a 40001
//! (§4's relation rule) — a conflict the row-space G2 checker has no
//! language for: its 40001 cross-check explains an abort through *slot
//! versions*, and after an empty table's TRUNCATE no slot has a version
//! above the snapshot at all. The runner therefore (a) skips the storage
//! swap when the table is empty (nothing is removed; the txn records the
//! no-op it is — the relation-swap surface is t6c's `diff` checker's,
//! whose TRUNCATE-window leg owns it), and (b) records a statement that
//! failed through the window as targeting every slot of the relation
//! (its kind rewritten off `Scan`, whose keys the checker's 40001
//! cross-check deliberately ignores): the conflict really is table-wide,
//! and the truncate's own `Del` versions then explain the abort the way
//! first-updater-wins explains a write conflict. The window is exact —
//! a statement holds its relation lock from `open_table` on, so no
//! TRUNCATE can commit behind one, and a 40001 whose relation had *not*
//! moved keeps the statement's own keys (a first-updater-wins abort).

use std::cell::Cell;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_catalog::{
    create_index_online, table_lo, BuildHooks, Db, IndexColumn, IndexDef, IndexState, Stmt,
    TableHandle,
};
use nucleus_codec::row::{ColumnDef, Datum, RowError, TableSchema};
use nucleus_codec::{decode_key_prefix, Direction, KeyColumn, KeyType, Nulls, Value};
use nucleus_kv::{Key, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{index_ranges, Catalog, Ddl, IdxStorage};
use nucleus_txn::commit::{spawn_commit_thread_with, CommitConfig, CommitThreadHandle, SyncCommit};
use nucleus_txn::encoding::VersionValue;
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::read::{read_key, scan as txn_scan, NoSsi};
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::resolver::{spawn_background, BackgroundHandle, Resolver};
use nucleus_txn::ssi::{spawn_retention, RetentionHandle, Ssi};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, LockWait, RowOp, RowOutcome, StmtCtx,
};
use nucleus_txn::{Ts, TxnError, TxnId};

use super::g2::history::{
    Fate, History, Level, Obs, OpKind, OpRec, Recorder, SqlErr, TxnRec, Val, Write,
};

/// The DDL session's id (DML sessions are `1..=threads`).
pub const DDL_SESSION: u16 = 4;

/// The plain-row slot count: slot `k` is the row `pk = k`, `k < SLOTS`.
/// Equals the checker's `history::PLAIN`; the parent/child slots of the
/// `nucleus-txn` key space are unused here.
pub const SLOTS: u8 = 12;

/// The ADD COLUMN budget of one run: the DDL mix may add no more columns
/// than this (every added column widens every row the workload writes;
/// the cap keeps the schema bounded while the mix stays deterministic).
pub const COL_CAP: u32 = 24;

// ----- keys, values, definitions ------------------------------------------

/// `t(pk int8 PRIMARY KEY, v int8, m int8)` — the card's table.
pub fn t_schema() -> TableSchema {
    let cols = vec![
        ColumnDef {
            attnum: 1,
            name: "pk".into(),
            ty: KeyType::Int8,
            not_null: true,
            dropped: false,
            missing: None,
        },
        ColumnDef {
            attnum: 2,
            name: "v".into(),
            ty: KeyType::Int8,
            not_null: false,
            dropped: false,
            missing: None,
        },
        ColumnDef {
            attnum: 3,
            name: "m".into(),
            ty: KeyType::Int8,
            not_null: false,
            dropped: false,
            missing: None,
        },
    ];
    TableSchema::new(cols, vec![1]).expect("the fixture schema is valid")
}

/// The one index of a run: non-unique, online-built, on the mirror
/// column `m` (attnum 3). `state`/`built_at` are the ladder's business.
pub fn m_def() -> IndexDef {
    IndexDef {
        columns: vec![IndexColumn {
            attnum: 3,
            dir: Direction::Asc,
            nulls: Nulls::Last,
        }],
        unique: false,
        nulls_not_distinct: false,
        concurrently: false,
        state: IndexState::DeleteOnly,
        built_at: 0,
    }
}

/// The setup txn's value of slot `k` (uid 1, `n = k + 1`).
pub fn iv(k: u8) -> Val {
    Val::new(1, u16::from(k) + 1)
}

/// A `Val` as the `v` column's int8 payload.
fn val_i64(v: Val) -> i64 {
    v.0 as i64
}

/// The `v` column's int8 payload as a `Val`.
fn i64_val(n: i64) -> Val {
    Val(n as u64)
}

/// One int8 value appended to `out` as an ASC NULLS LAST key part.
fn encode1(out: &mut Key, n: i64) {
    nucleus_codec::encode_key(
        &[KeyColumn::asc(KeyType::Int8)],
        &[Some(Value::Int8(n))],
        out,
    )
    .expect("an int8 key part encodes");
}

/// The row key of slot `k` in table storage `sid` (§3:
/// `table_lo ‖ encode_key(pk ASC NULLS LAST)`).
fn pk_row_key(sid: u64, k: u8) -> Key {
    let mut key = table_lo(sid);
    encode1(&mut key, i64::from(k));
    key
}

/// The `/i/{isid}/` entry logical key of slot `k`'s row: the §6 layout
/// for a non-unique index — prefix ‖ encoded key value ‖ row key —
/// computed from public primitives (`m = pk`, so the key value is `k`
/// for every version of the row).
fn m_entry_key(isid: u64, k: u8, row_key: &[u8]) -> Key {
    let mut key = index_ranges(isid)[1].0.clone();
    encode1(&mut key, i64::from(k));
    key.extend_from_slice(row_key);
    key
}

/// The `(pk, v)` of a decoded row, with the `m = pk` mirror asserted: a
/// row whose mirror broke would poison every entry the index serves.
fn t_row(schema: &TableSchema, live: &[Option<Datum>]) -> (i64, Val) {
    let pos = |attnum: u16| {
        schema
            .live_columns()
            .position(|c| c.attnum == attnum)
            .expect("pk/v/m are never dropped")
    };
    let datum = |attnum: u16| live.get(pos(attnum)).expect("the row carries its columns");
    let (pk, v, m) = (datum(1), datum(2), datum(3));
    let Some(Datum::Inline(Value::Int8(pk))) = pk else {
        panic!("pk datum: {pk:?}")
    };
    let Some(Datum::Inline(Value::Int8(v))) = v else {
        panic!("v datum: {v:?}")
    };
    let Some(Datum::Inline(Value::Int8(m))) = m else {
        panic!("m datum: {m:?}")
    };
    assert_eq!(*m, *pk, "the mirror column m must equal pk");
    (*pk, i64_val(*v))
}

/// The `v` value of a row's bytes, decoded under `schema`.
fn row_val(schema: &TableSchema, bytes: &[u8]) -> Val {
    let live = schema.decode_row(bytes).expect("the row decodes");
    t_row(schema, &live).1
}

/// The updated row bytes: `set v` applied to a decoded base (§3's lazy
/// upgrade at the current version — every column the base renders is
/// written back explicitly).
fn update_bytes(
    schema: &TableSchema,
    base: &mut [Option<Datum>],
    new_v: i64,
) -> Result<Vec<u8>, nucleus_catalog::DbError> {
    let pos = schema
        .live_columns()
        .position(|c| c.attnum == 2)
        .expect("v is never dropped");
    base[pos] = Some(Datum::Inline(Value::Int8(new_v)));
    Ok(schema.encode_row(base)?)
}

/// The insert datum list of a `(pk, v)` row over the handle's live
/// columns: `pk`, `v`, `m = pk`, NULL for every column a DDL added.
fn insert_datums(h: &TableHandle, k: u8, v: Val) -> Vec<Option<Datum>> {
    h.schema
        .live_columns()
        .map(|col| match col.attnum {
            1 => Some(Datum::Inline(Value::Int8(i64::from(k)))),
            2 => Some(Datum::Inline(Value::Int8(val_i64(v)))),
            3 => Some(Datum::Inline(Value::Int8(i64::from(k)))),
            _ => None,
        })
        .collect()
}

pub fn map_err(e: TxnError) -> SqlErr {
    match e {
        TxnError::SerializationFailure => SqlErr::SerFailure,
        TxnError::Deadlock => SqlErr::Deadlock,
        TxnError::LockNotAvailable => SqlErr::NotAvailable,
        TxnError::UniqueViolation => SqlErr::Unique,
        TxnError::ForeignKeyViolation => SqlErr::Fk,
        other => SqlErr::Other(other.to_string()),
    }
}

/// A typed-layer error into the recorder's SQLSTATE shape. Everything
/// the workload does not expect stays `Other`, which the checker reports
/// as an engine error — the honest failure path.
pub fn map_db_err(e: nucleus_catalog::DbError) -> SqlErr {
    match e {
        nucleus_catalog::DbError::Txn(t) => map_err(t),
        other => SqlErr::Other(format!("{}: {other}", other.sqlstate())),
    }
}

fn iso(l: Level) -> Isolation {
    match l {
        Level::ReadCommitted => Isolation::ReadCommitted,
        Level::RepeatableRead => Isolation::RepeatableRead,
        Level::Serializable => Isolation::Serializable,
    }
}

// ----- the rig ----------------------------------------------------------------

/// A shared handle to one `MemKv` (the `t6b_support::SharedFault`
/// pattern without the fault): the `Core` owns one clone, the rig keeps
/// another for the harness's own raw diagnostics.
#[derive(Clone)]
pub struct SharedMem(pub Arc<MemKv>);

impl OrderedKv for SharedMem {
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

/// The surfaces the soak floors run on (counted by the rig itself, not
/// derived from the history).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub truncates_committed: u64,
    pub add_columns_committed: u64,
    pub builds_ok: u64,
    pub builds_failed: u64,
    /// IndexScan statements the engine served through the index.
    pub index_scans_served: u64,
    /// IndexScan statements the engine refused (0A000, §4's usability
    /// rule) and the statement answered with the pk scan.
    pub index_scan_fallbacks: u64,
}

/// A finished run.
pub struct Run {
    pub history: History,
    pub leaked_intents: usize,
    pub leak_report: String,
    pub counts: Counts,
}

/// The shared engine: one `Db` over `Core<MemKv>` with the catalog, the
/// commit thread (SSI's writer-map observer), the background resolver,
/// the SSI retention thread and the recorder. `t_oid` is the one table;
/// its one index (when the DDL session has built it) is resolved from
/// the catalog at each execution.
pub struct Rig {
    pub db: Arc<Db<SharedMem>>,
    pub kv: SharedMem,
    pub t_oid: u64,
    pub rec: Recorder,
    counts: [AtomicU64; 6],
    status: Mutex<BTreeMap<u16, String>>,
    commit: Mutex<Option<CommitThreadHandle>>,
    resolver: Mutex<Option<BackgroundHandle>>,
    retention: Mutex<Option<RetentionHandle>>,
}

/// The reader ctx every harness-side catalog read uses: a ghost this
/// process never allocates (status ids start at 1), so nobody's own
/// writes count on it.
fn ghost_ctx(core: &Core<SharedMem>, snapshot: Ts) -> ReadCtx {
    ReadCtx {
        txn: TxnId {
            epoch: core.epoch(),
            n: 0,
        },
        snapshot,
        stmt_seq: 0,
    }
}

impl Rig {
    /// A fresh rig with the commit pipeline running (the g2 pattern).
    pub fn new(deadlock_ms: u64) -> Rig {
        let kv = SharedMem(Arc::new(MemKv::new()));
        let core = Arc::new(Core::open(kv.clone()).expect("open the core"));
        let ssi = Ssi::install(&core);
        let locks = LockManager::install(&core);
        let catalog = Catalog::open(&core, &ssi, &locks).expect("open the catalog");
        let db = Arc::new(Db::new(core.clone(), ssi.clone(), locks, catalog));
        core.waits
            .set_deadlock_timeout(Duration::from_millis(deadlock_ms));
        let commit = spawn_commit_thread_with(
            Arc::clone(&core),
            CommitConfig::new().with_observer(ssi.commit_observer()),
        )
        .expect("commit thread");
        let resolver = spawn_background(Arc::clone(&core)).expect("resolver");
        let retention = spawn_retention(
            Arc::clone(&core),
            Arc::clone(&ssi),
            Duration::from_millis(2),
        );
        Rig {
            kv,
            db,
            t_oid: 0,
            rec: Recorder::new(),
            counts: Default::default(),
            status: Mutex::new(BTreeMap::new()),
            commit: Mutex::new(Some(commit)),
            resolver: Mutex::new(Some(resolver)),
            retention: Mutex::new(Some(retention)),
        }
    }

    fn note(&self, session: u16, what: String) {
        if let Ok(mut m) = self.status.lock() {
            m.insert(session, what);
        }
    }

    /// The hang diagnostic: every session's last note and the wait-for
    /// graph.
    fn dump_state(&self) -> String {
        let mut out = String::new();
        if let Ok(m) = self.status.lock() {
            for (s, what) in m.iter() {
                out.push_str(&format!("  session {s}: {what}\n"));
            }
        }
        out.push_str(&format!("  wait edges: {:?}\n", self.db.core.wait_edges()));
        out
    }

    /// The initial state: CREATE TABLE `t` in one unrecorded RC txn (the
    /// `diff` runner's setup shape — catalog shape is not row space),
    /// then the 12 plain rows as one recorded RC txn (session 0, uid 1).
    pub fn setup(&mut self) {
        let txn = self.db.core.begin(Isolation::ReadCommitted);
        {
            let guard = self.db.core.registry.take_snapshot();
            let st = Stmt::new(&txn, guard.ts()).expect("the setup statement");
            let (oid, _) = self
                .db
                .create_table(&st, b"t", &t_schema())
                .expect("create t");
            self.t_oid = oid;
        }
        self.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("setup commit");
        let mut s = Session::begin(self, 0, 0, 0, Level::ReadCommitted, Bug::None);
        for k in 0..SLOTS {
            s.insert(k).expect("setup insert");
        }
        s.commit().expect("setup rows commit");
    }

    /// A bug-free session at `level`.
    pub fn session(&self, id: u16, level: Level) -> Session<'_> {
        Session::begin(self, id, 0, 0, level, Bug::None)
    }

    pub fn session_with(&self, id: u16, level: Level, bug: Bug) -> Session<'_> {
        Session::begin(self, id, 0, 0, level, bug)
    }

    /// §4's RR relation rule, evaluated by the harness: the relation row
    /// at `s` and at the latest committed state (both through a ghost
    /// reader, so nobody's own writes count) name different storage ids
    /// iff a foreign TRUNCATE committed above `s`.
    pub fn relation_moved(&self, s: Ts) -> bool {
        let core = &self.db.core;
        let read = |snapshot: Ts| -> Option<u64> {
            let view = core.open_view();
            Catalog::read_rel(core, &view, &ghost_ctx(core, snapshot), self.t_oid)
                .expect("the catalog read")
                .map(|rel| rel.storage_id)
        };
        match (read(s), read(core.visible_ts())) {
            (Some(at_s), Some(latest)) => at_s != latest,
            _ => false,
        }
    }

    /// Whether a read statement's handle **straddled** a relation swap:
    /// the handle's storage (read at the latest committed catalog) is
    /// not the relation's storage at the statement's snapshot, so the
    /// statement would answer from the post-TRUNCATE world at a
    /// pre-TRUNCATE snapshot — a shape §4's relation rule refuses for
    /// RR/SER but the spec leaves open at RC (the rule is RR/SER-only;
    /// t6c's `diff` checker carves the same window out). The row-space
    /// G2 checker cannot judge it either way, so the harness refuses the
    /// statement itself (40001, aborted txn, a fresh retry whose
    /// snapshot sits above the swap) — a client is always free to refuse
    /// a statement it can see straddled DDL, and the truncate's own
    /// `Del` versions explain the refusal to the checker.
    pub fn read_straddled(&self, s: Ts, handle_sid: u64) -> bool {
        let core = &self.db.core;
        let view = core.open_view();
        Catalog::read_rel(core, &view, &ghost_ctx(core, s), self.t_oid)
            .expect("the catalog read")
            .is_some_and(|rel| rel.storage_id != handle_sid)
    }

    /// The relation's one index row at the latest committed catalog, with
    /// its entry-storage id (`None` before the build's T0 commits).
    pub fn m_index(&self) -> Option<(u64, u64)> {
        let core = &self.db.core;
        let view = core.open_view();
        let idxs =
            Catalog::scan_idxs_of_rel(core, &view, &ghost_ctx(core, core.visible_ts()), self.t_oid)
                .expect("the index catalog scan");
        assert!(idxs.len() <= 1, "one index per run");
        let row = idxs.into_iter().next()?;
        match row.storage {
            IdxStorage::Id(isid) => Some((row.oid, isid)),
            IdxStorage::Range(..) => panic!("an online-built index has an id storage"),
        }
    }

    /// A committed index row's decoded def (the test binaries' pin).
    pub fn idx_def_of(&self, idx_oid: u64) -> IndexDef {
        let core = &self.db.core;
        let view = core.open_view();
        let row = Catalog::read_idx(core, &view, &ghost_ctx(core, core.visible_ts()), idx_oid)
            .expect("the idx catalog read")
            .expect("idx row committed");
        IndexDef::decode(&row.def).expect("the def decodes")
    }

    /// The live definition of `t` at the latest committed catalog (the
    /// index-scan arm's row decoding).
    pub fn rel_schema(&self) -> TableSchema {
        let core = &self.db.core;
        let view = core.open_view();
        let rel = Catalog::read_rel(core, &view, &ghost_ctx(core, core.visible_ts()), self.t_oid)
            .expect("the catalog read")
            .expect("t exists");
        TableSchema::decode(&rel.def).expect("the def decodes")
    }

    fn count_bump(&self, i: usize) {
        self.counts[i].fetch_add(1, Ordering::Relaxed);
    }

    const TRUNCATES: usize = 0;
    const ADDS: usize = 1;
    const BUILDS_OK: usize = 2;
    const BUILDS_FAILED: usize = 3;
    const ISCAN_SERVED: usize = 4;
    const ISCAN_FALLBACK: usize = 5;

    /// Stops the background threads, drains resolution, scans for leaked
    /// row/entry intents and hands the history back.
    pub fn finish(&self) -> Run {
        if let Some(c) = self.commit.lock().expect("commit slot").take() {
            c.shutdown().expect("commit thread shutdown");
        }
        if let Some(r) = self.retention.lock().expect("retention slot").take() {
            r.stop().expect("retention stop");
        }
        if let Some(r) = self.resolver.lock().expect("resolver slot").take() {
            r.stop().expect("resolver stop");
        }
        // Commit step 5 queued every resolution before the thread stopped.
        while Resolver::run_once(&self.db.core).expect("resolve") > 0 {}
        let view = self.db.core.open_view();
        let mut leaked = 0;
        let mut report = String::new();
        for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let (k, _) = row.expect("scan the kv");
            if let Some((logical, nucleus_txn::encoding::Entry::Intent)) =
                nucleus_txn::encoding::parse_key(&k)
            {
                if logical.starts_with(b"/t/") || logical.starts_with(b"/i/") {
                    leaked += 1;
                    report.push_str(&format!("  leaked intent on {logical:?}\n"));
                }
            }
        }
        drop(view);
        let c = &self.counts;
        Run {
            history: self.rec.drain(),
            leaked_intents: leaked,
            leak_report: report,
            counts: Counts {
                truncates_committed: c[Self::TRUNCATES].load(Ordering::Relaxed),
                add_columns_committed: c[Self::ADDS].load(Ordering::Relaxed),
                builds_ok: c[Self::BUILDS_OK].load(Ordering::Relaxed),
                builds_failed: c[Self::BUILDS_FAILED].load(Ordering::Relaxed),
                index_scans_served: c[Self::ISCAN_SERVED].load(Ordering::Relaxed),
                index_scan_fallbacks: c[Self::ISCAN_FALLBACK].load(Ordering::Relaxed),
            },
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        // A test that panicked mid-run still stops its threads.
        if let Ok(mut c) = self.commit.lock() {
            if let Some(c) = c.take() {
                let _ = c.shutdown();
            }
        }
        if let Ok(mut r) = self.retention.lock() {
            if let Some(r) = r.take() {
                let _ = r.stop();
            }
        }
        if let Ok(mut r) = self.resolver.lock() {
            if let Some(r) = r.take() {
                let _ = r.stop();
            }
        }
    }
}

// ----- sessions ------------------------------------------------------------------

/// A harness-level defect injected into a session or DDL txn (the g2
/// `Bug` pattern: the engine is never edited, only what the harness
/// does; the unchanged checker must flag the recorded history).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    None,
    /// **(a)** An RR session whose IndexScan statements take a fresh
    /// snapshot per statement. The engine statement and the record both
    /// run at the fresh snapshot (RC behaviour, which is why the read
    /// sees what honest RR would refuse); the lie is the txn's RR claim.
    IndexScanFreshSnapshot,
    /// **(b)** A TRUNCATE that records its reads but not its `Del`s: the
    /// rows really vanish (the engine truncates), so the first reader
    /// after it observes an absence no committed history explains.
    TruncateNoDels,
    /// **(c)** An IndexScan served by a test-only **raw scan of the
    /// index entry range** at the statement's snapshot, bypassing §4's
    /// `Public`/`built_at` gate: during a build, with the ingest at `S`
    /// above the reader's snapshot, the raw scan sees no entry at all
    /// and misses rows every committed history explains.
    RawEntryScanDuringBuild,
}

/// One transaction attempt.
pub struct Session<'r> {
    rig: &'r Rig,
    level: Level,
    bug: Bug,
    txn: Option<Txn>,
    guard: Option<SnapshotGuard<'r>>,
    rec: TxnRec,
    next_val: Cell<u16>,
    done: bool,
}

/// The UPDATE EPQ (§5.2): re-apply `set v` to the newest committed
/// version and remember the base — the value the row op overwrote and
/// the ts it was read at, which is what the record's version order is
/// built from.
struct UpdateEpq {
    schema: TableSchema,
    new_v: i64,
    seen: Option<(Ts, Obs)>,
    error: Option<nucleus_catalog::DbError>,
}

impl Epq for UpdateEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        let payload = match &newest.value {
            VersionValue::Live { payload, .. } => payload.clone(),
            VersionValue::Tombstone { .. } => {
                self.seen = Some((newest.ts, Obs::Absent));
                return EpqDecision::Skip;
            }
        };
        let mut live = match self.schema.decode_row(&payload) {
            Ok(l) => l,
            Err(e) => {
                self.error = Some(e.into());
                return EpqDecision::Skip;
            }
        };
        let saw = t_row(&self.schema, &live).1;
        let bytes = match update_bytes(&self.schema, &mut live, self.new_v) {
            Ok(b) => b,
            Err(e) => {
                self.error = Some(e);
                return EpqDecision::Skip;
            }
        };
        self.seen = Some((newest.ts, Obs::Val(saw)));
        EpqDecision::Apply(RowOp::Update {
            value: bytes,
            // `v` is neither the pk nor a column of any unique index.
            key_cols_changed: false,
        })
    }
}

/// The DELETE EPQ: the newest committed version is the base the row op
/// (and the entry maintenance) acts on; a tombstone skips. The base's
/// `v` — the value the delete removed — is what the record keeps.
struct DeleteEpq {
    schema: TableSchema,
    seen: Option<(Ts, Obs)>,
}

impl Epq for DeleteEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        let payload = match &newest.value {
            VersionValue::Live { payload, .. } => payload.clone(),
            VersionValue::Tombstone { .. } => {
                self.seen = Some((newest.ts, Obs::Absent));
                return EpqDecision::Skip;
            }
        };
        self.seen = Some((newest.ts, Obs::Val(row_val(&self.schema, &payload))));
        EpqDecision::Apply(RowOp::Delete)
    }
}

impl<'r> Session<'r> {
    pub fn begin(
        rig: &'r Rig,
        session: u16,
        prog: u32,
        attempt: u32,
        level: Level,
        bug: Bug,
    ) -> Session<'r> {
        let uid = rig.rec.next_uid();
        let start = rig.rec.tick();
        let txn = rig.db.core.begin(iso(level));
        let guard = match level {
            Level::ReadCommitted => None,
            Level::RepeatableRead => Some(rig.db.core.registry.take_snapshot()),
            Level::Serializable => Some(
                rig.db
                    .ssi
                    .begin(&rig.db.core, &txn, false)
                    .expect("ssi begin on a fresh SERIALIZABLE txn"),
            ),
        };
        let snap = guard.as_ref().map(|g| g.ts().0);
        Session {
            rig,
            level,
            bug,
            txn: Some(txn),
            guard,
            rec: TxnRec {
                uid,
                session,
                prog,
                attempt,
                level,
                snap,
                start,
                end: start,
                fate: Fate::Rolledback,
                ops: Vec::new(),
            },
            next_val: Cell::new(0),
            done: false,
        }
    }

    pub fn uid(&self) -> u32 {
        self.rec.uid
    }

    /// The txn's registered snapshot (RR/SER).
    pub fn snapshot(&self) -> Ts {
        self.guard.as_ref().expect("the txn snapshot guard").ts()
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    fn val(&self) -> Val {
        self.next_val.set(self.next_val.get() + 1);
        Val::new(self.rec.uid, self.next_val.get())
    }

    fn ser(&self) -> bool {
        self.level == Level::Serializable
    }

    fn finish(&mut self, fate: Fate) {
        self.done = true;
        self.rec.end = self.rig.rec.tick();
        self.rec.fate = fate;
        self.guard = None;
        self.rig.rec.push(self.rec.clone());
    }

    fn fail(&mut self, e: SqlErr) {
        if let Some(txn) = self.txn.take() {
            self.rig.db.core.abort(txn).expect("abort a failed txn");
        }
        self.finish(Fate::Failed(e));
    }

    pub fn commit(mut self) -> Result<(), SqlErr> {
        let Some(txn) = self.txn.take() else {
            panic!("commit on a txn-less session");
        };
        // The relation-level shape of a commit-time 40001 (a SER txn the
        // TRUNCATE's SSI storage unmap doomed): the abort is real, and
        // its conflict is the relation's, so the record says so — one
        // failed statement targeting every slot, exactly the widened
        // window op (the checker's 40001 cross-check then explains the
        // abort through the truncate's own `Del` versions).
        let snap = self.guard.as_ref().map(|g| g.ts());
        match self.rig.db.core.commit(txn, SyncCommit::On) {
            Ok(ts) => {
                self.finish(Fate::Committed(ts.0));
                Ok(())
            }
            Err(e) => {
                // `commit_submit` aborts on a pre-commit failure itself.
                let e = map_err(e);
                if e == SqlErr::SerFailure && snap.is_some_and(|s| self.rig.relation_moved(s)) {
                    self.rec.ops.push(OpRec {
                        kind: OpKind::Read,
                        keys: (0..SLOTS).collect(),
                        start: self.rig.rec.tick(),
                        end: self.rig.rec.tick(),
                        snap: snap.map(|s| s.0),
                        reads: Vec::new(),
                        writes: Vec::new(),
                        void: false,
                        err: Some(SqlErr::SerFailure),
                    });
                }
                self.finish(Fate::Failed(e.clone()));
                Err(e)
            }
        }
    }

    pub fn rollback(mut self) {
        if let Some(txn) = self.txn.take() {
            self.rig.db.core.abort(txn).expect("abort");
        }
        self.finish(Fate::Rolledback);
    }

    /// Records a statement that died in the §4 relation window: the
    /// conflict is the relation's, so the op targets every slot of it
    /// (and a `Scan` kind is rewritten to `Read` — the checker's 40001
    /// cross-check deliberately ignores a `Scan` op's keys, a range
    /// scan's conflict being everyone else's business).
    fn widen_window_op(op: &mut OpRec) {
        if op.kind == OpKind::Scan {
            op.kind = OpKind::Read;
        }
        op.keys = (0..SLOTS).collect();
    }

    /// Runs one statement: opens the op record, picks the statement's
    /// snapshot (RC — or the injected fresh-snapshot defect — fresh per
    /// statement; RR/SER the txn's), runs `body`, closes and files the
    /// record. An error aborts the txn.
    fn exec<T>(
        &mut self,
        kind: OpKind,
        keys: &[u8],
        fresh: bool,
        body: impl FnOnce(&Session<'r>, &Stmt<'_>, &mut OpRec) -> Result<T, nucleus_catalog::DbError>,
    ) -> Result<T, SqlErr> {
        assert!(!self.done, "statement on a finished session");
        let mut op = OpRec {
            kind,
            keys: keys.to_vec(),
            start: self.rig.rec.tick(),
            end: 0,
            snap: None,
            reads: Vec::new(),
            writes: Vec::new(),
            void: false,
            err: None,
        };
        self.rig.note(
            self.rec.session,
            format!(
                "T{} p{}.{} {kind:?} {keys:?}",
                self.rec.uid, self.rec.prog, self.rec.attempt
            ),
        );
        // SERIALIZABLE: a doomed txn fails before its next statement.
        let doomed = self
            .ser()
            .then(|| {
                let txn = self.txn.as_ref().expect("the txn is alive");
                self.rig.db.ssi.check_doomed(txn.id).err().map(map_err)
            })
            .flatten();
        if let Some(e) = doomed {
            // A doomed txn aborted on a relation-level conflict (a
            // TRUNCATE's SSI storage unmap above the txn's snapshot)
            // carries slot keys the row-space checker cannot explain;
            // the conflict's real target is the relation — see
            // [`Session::widen_window_op`].
            if e == SqlErr::SerFailure && self.rig.relation_moved(self.snapshot()) {
                Self::widen_window_op(&mut op);
            }
            op.err = Some(e.clone());
            op.end = self.rig.rec.tick();
            self.rec.ops.push(op);
            self.fail(e.clone());
            return Err(e);
        }
        let stmt_snap;
        let result = {
            let rc_guard;
            let s = if fresh || self.level == Level::ReadCommitted {
                rc_guard = Some(self.rig.db.core.registry.take_snapshot());
                rc_guard.as_ref().expect("just taken").ts()
            } else {
                self.guard.as_ref().expect("the txn snapshot guard").ts()
            };
            stmt_snap = s.0;
            op.snap = Some(s.0);
            let txn = self.txn.as_ref().expect("the txn is alive");
            let st = Stmt::new(txn, s).expect("a fresh statement");
            body(self, &st, &mut op)
        };
        op.end = self.rig.rec.tick();
        match result {
            Ok(v) => {
                self.rec.ops.push(op);
                Ok(v)
            }
            Err(e) => {
                let e = map_db_err(e);
                if e == SqlErr::SerFailure && self.rig.relation_moved(Ts(stmt_snap)) {
                    Self::widen_window_op(&mut op);
                }
                op.err = Some(e.clone());
                self.rec.ops.push(op);
                self.fail(e.clone());
                Err(e)
            }
        }
    }

    /// The raw row bytes of `key` at the statement's snapshot (§4): SER
    /// registers the SIREAD; other levels read through a plain
    /// registered view.
    fn read_row_raw(
        &self,
        st: &Stmt<'_>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, nucleus_catalog::DbError> {
        let ctx = ReadCtx {
            txn: st.txn().id,
            snapshot: st.snapshot(),
            stmt_seq: st.seq0(),
        };
        let found = match st.txn().isolation {
            Isolation::Serializable => self
                .rig
                .db
                .ssi
                .read_key(&self.rig.db.core, st.txn().id, key, &ctx)
                .map_err(nucleus_catalog::DbError::Txn)?,
            _ => {
                let view = self.rig.db.core.open_view();
                read_key(&self.rig.db.core, &view, key, &ctx, &mut NoSsi)
                    .map_err(nucleus_catalog::DbError::Txn)?
            }
        };
        Ok(found)
    }

    /// Records the reads of a whole-table scan at the statement's
    /// snapshot (the pk-scan arm of `Scan` and the fallback of
    /// `IndexScan`). Every slot's observation is recorded — the typed
    /// layer's `Db::scan` reads (and, under SERIALIZABLE, SIREADs) the
    /// whole table range whatever the query's predicate, so the record
    /// carries the whole footprint: without it, the phantom anti-
    /// dependencies SSI forms (a scan range-sires the table, any
    /// concurrent writer of any slot conflicts) would be invisible to
    /// the checker and every such abort would read as unexplained. The
    /// returned pairs stay filtered to `[lo, hi)` (the query's slots).
    fn scan_reads(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        lo: u8,
        hi: u8,
    ) -> Result<Vec<(u8, Val)>, nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::AccessShare)?;
        if self.rig.read_straddled(st.snapshot(), h.storage_id) {
            return Err(nucleus_catalog::DbError::Txn(
                TxnError::SerializationFailure,
            ));
        }
        let rows = self.rig.db.scan(st, &h)?;
        let mut live = BTreeMap::new();
        for (_, datums) in &rows {
            let (pk, v) = t_row(&h.schema, datums);
            live.insert(pk as u8, v);
        }
        Ok(record_range(op, &live, 0, SLOTS)).map(|all| {
            all.into_iter()
                .filter(|(k, _)| lo <= *k && *k < hi)
                .collect()
        })
    }

    /// The test-only raw entry scan of [`Bug::RawEntryScanDuringBuild`]:
    /// the `/i/` entry range read directly at the statement's snapshot,
    /// §4's `Public`/`built_at` gate never consulted, each entry's row
    /// fetched at the same snapshot.
    fn raw_entry_reads(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        isid: u64,
        lo: u8,
        hi: u8,
    ) -> Result<Vec<(u8, Val)>, nucleus_catalog::DbError> {
        let (prefix, end) = {
            let ranges = index_ranges(isid);
            (ranges[1].0.clone(), ranges[1].1.clone())
        };
        let ctx = ReadCtx {
            txn: st.txn().id,
            snapshot: st.snapshot(),
            stmt_seq: st.seq0(),
        };
        let entries = {
            let view = self.rig.db.core.open_view();
            txn_scan(
                &self.rig.db.core,
                &view,
                (
                    Bound::Included(prefix.as_slice()),
                    Bound::Excluded(end.as_slice()),
                ),
                &ctx,
                &mut NoSsi,
            )
            .collect::<Result<Vec<_>, _>>()
            .map_err(nucleus_catalog::DbError::Txn)?
        };
        let schema = self.rig.rel_schema();
        let cols = [KeyColumn::asc(KeyType::Int8)];
        let mut live = BTreeMap::new();
        for (key, _) in entries {
            let part = &key[prefix.len()..];
            let (vals, used) = decode_key_prefix(&cols, part)
                .map_err(|_| nucleus_catalog::DbError::Row(RowError::Corrupt("raw entry key")))?;
            let Some(Some(Value::Int8(m))) = vals.first().cloned() else {
                continue;
            };
            let row_key = &part[used..];
            let Some(bytes) = self.read_row_raw(st, row_key)? else {
                // The entry's row is not visible at the snapshot: the
                // raw scan simply misses it — the whole point of the
                // bypass the checker must flag.
                continue;
            };
            let (pk, v) = (m, row_val(&schema, &bytes));
            assert_eq!(pk, m, "an m-entry names its own row");
            live.insert(m as u8, v);
        }
        Ok(record_range(op, &live, lo, hi))
    }

    /// `SELECT v FROM t WHERE pk = k`.
    pub fn read(&mut self, k: u8) -> Result<Option<Val>, SqlErr> {
        self.exec(OpKind::Read, &[k], false, |me, st, op| {
            let h = me
                .rig
                .db
                .open_table(st, me.rig.t_oid, RelLockMode::AccessShare)?;
            if me.rig.read_straddled(st.snapshot(), h.storage_id) {
                return Err(nucleus_catalog::DbError::Txn(
                    TxnError::SerializationFailure,
                ));
            }
            let key = pk_row_key(h.storage_id, k);
            let bytes = me.read_row_raw(st, &key)?;
            let obs = bytes.map(|b| row_val(&h.schema, &b));
            op.reads.push((k, obs.map_or(Obs::Absent, Obs::Val)));
            Ok(obs)
        })
    }

    /// `SELECT ... WHERE pk >= lo AND pk < hi` (the pk scan).
    pub fn scan(&mut self, lo: u8, hi: u8) -> Result<Vec<(u8, Val)>, SqlErr> {
        self.exec(OpKind::Scan, &[lo, hi], false, |me, st, op| {
            me.scan_reads(st, op, lo, hi)
        })
    }

    /// `SELECT ... WHERE m >= lo AND m < hi` through the published index
    /// (§4), recorded as `OpKind::Scan` over the same slot range: an
    /// index range scan observes the same slots as a pk `Scan`. The
    /// engine owns the usability gate — the statement *attempts* the
    /// index read whenever the index exists and answers through the pk
    /// scan on the engine's 0A000 (`index not usable at snapshot`), the
    /// shape that keeps the `built_at` rule observable rather than
    /// pre-empted by the harness.
    pub fn index_scan(&mut self, lo: u8, hi: u8) -> Result<Vec<(u8, Val)>, SqlErr> {
        let fresh = self.bug == Bug::IndexScanFreshSnapshot && self.level == Level::RepeatableRead;
        self.exec(OpKind::Scan, &[lo, hi], fresh, |me, st, op| {
            let Some((idx_oid, isid)) = me.rig.m_index() else {
                // No index exists (the build's T0 has not committed): the
                // statement is the pk scan.
                return me.scan_reads(st, op, lo, hi);
            };
            if me.bug == Bug::RawEntryScanDuringBuild {
                return me.raw_entry_reads(st, op, isid, lo, hi);
            }
            // The straddle guard for the statement the engine gives no
            // handle back from: refuse before (the swap already
            // committed) and verify after (it landed mid-statement) —
            // conservative on the racy side, exact on the settled one.
            let s = st.snapshot();
            if me.rig.relation_moved(s) {
                return Err(nucleus_catalog::DbError::Txn(
                    TxnError::SerializationFailure,
                ));
            }
            let rows = me.rig.db.index_scan(
                st,
                idx_oid,
                Bound::Included(vec![Some(Value::Int8(i64::from(lo)))]),
                Bound::Excluded(vec![Some(Value::Int8(i64::from(hi)))]),
            );
            match rows {
                Ok(rows) => {
                    if me.rig.relation_moved(s) {
                        return Err(nucleus_catalog::DbError::Txn(
                            TxnError::SerializationFailure,
                        ));
                    }
                    me.rig.count_bump(Rig::ISCAN_SERVED);
                    let schema = me.rig.rel_schema();
                    let mut live = BTreeMap::new();
                    for (_, datums) in &rows {
                        let (pk, v) = t_row(&schema, datums);
                        live.insert(pk as u8, v);
                    }
                    Ok(record_range(op, &live, lo, hi))
                }
                Err(nucleus_catalog::DbError::FeatureNotSupported(
                    "index not usable at snapshot",
                )) => {
                    // The engine's own §4 verdict: the statement answers
                    // through the pk scan instead.
                    me.rig.count_bump(Rig::ISCAN_FALLBACK);
                    me.scan_reads(st, op, lo, hi)
                }
                Err(e) => Err(e),
            }
        })
    }

    /// `INSERT INTO t VALUES (k, ..)`.
    pub fn insert(&mut self, k: u8) -> Result<(), SqlErr> {
        let v = self.val();
        self.exec(OpKind::Insert, &[k], false, move |me, st, op| {
            let h = me
                .rig
                .db
                .open_table(st, me.rig.t_oid, RelLockMode::RowExclusive)?;
            me.rig.db.insert_with(st, &h, &insert_datums(&h, k, v))?;
            op.writes.push((k, Write::Put(v)));
            Ok(())
        })
    }

    /// `UPDATE t SET v = .. WHERE pk = k`: the §5 read at the statement's
    /// snapshot, then the row op with the runner's own EPQ (the recorded
    /// base is the version the row op acted on). Index maintenance is
    /// skipped: the entry of `m = pk` is a function of `(m, row key)` and
    /// neither moves, so §6's update maintenance is the no-op
    /// `maintain_update` itself performs for an unchanged entry.
    pub fn update(&mut self, k: u8) -> Result<bool, SqlErr> {
        let v = self.val();
        self.exec(OpKind::Update, &[k], false, move |me, st, op| {
            let txn = st.txn();
            let h = me
                .rig
                .db
                .open_table(st, me.rig.t_oid, RelLockMode::RowExclusive)?;
            if me.rig.read_straddled(st.snapshot(), h.storage_id) {
                return Err(nucleus_catalog::DbError::Txn(
                    TxnError::SerializationFailure,
                ));
            }
            let key = pk_row_key(h.storage_id, k);
            let Some(bytes) = me.read_row_raw(st, &key)? else {
                op.reads.push((k, Obs::Absent));
                return Ok(false);
            };
            let mut base = h.schema.decode_row(&bytes)?;
            let saw = t_row(&h.schema, &base).1;
            let mut epq = UpdateEpq {
                schema: h.schema.clone(),
                new_v: val_i64(v),
                seen: None,
                error: None,
            };
            let out = me.rig.db.core.row_op(
                txn,
                &key,
                None,
                RowOp::Update {
                    // The no-EPQ base is the read row; the EPQ callback
                    // rebuilds this for the version it re-based on.
                    value: update_bytes(&h.schema, &mut base, val_i64(v))?,
                    key_cols_changed: false,
                },
                st.ctx(),
                &mut epq,
            );
            let (obs, snap) = match epq.seen {
                Some((ts, o)) => (o, ts),
                None => (Obs::Val(saw), st.snapshot()),
            };
            op.reads.push((k, obs));
            op.snap = Some(snap.0);
            if let Some(e) = epq.error {
                return Err(e);
            }
            if out.map_err(nucleus_catalog::DbError::Txn)? == RowOutcome::Applied {
                op.writes.push((k, Write::Put(v)));
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    /// `DELETE FROM t WHERE pk = k`: the same read/EPQ shape, then the
    /// §6 blind entry delete of every index of the statement's handle
    /// (every state) through `Core::delete_entry`.
    pub fn delete(&mut self, k: u8) -> Result<bool, SqlErr> {
        self.exec(OpKind::Delete, &[k], false, |me, st, op| {
            let txn = st.txn();
            let h = me
                .rig
                .db
                .open_table(st, me.rig.t_oid, RelLockMode::RowExclusive)?;
            if me.rig.read_straddled(st.snapshot(), h.storage_id) {
                return Err(nucleus_catalog::DbError::Txn(
                    TxnError::SerializationFailure,
                ));
            }
            let key = pk_row_key(h.storage_id, k);
            let Some(bytes) = me.read_row_raw(st, &key)? else {
                op.reads.push((k, Obs::Absent));
                return Ok(false);
            };
            let saw = row_val(&h.schema, &bytes);
            let mut epq = DeleteEpq {
                schema: h.schema.clone(),
                seen: None,
            };
            let out = me
                .rig
                .db
                .core
                .row_op(txn, &key, None, RowOp::Delete, st.ctx(), &mut epq);
            let (snap, obs) = epq.seen.unwrap_or_else(|| (st.snapshot(), Obs::Val(saw)));
            op.reads.push((k, obs));
            op.snap = Some(snap.0);
            if out.map_err(nucleus_catalog::DbError::Txn)? != RowOutcome::Applied {
                return Ok(false);
            }
            for idx in &h.indexes {
                let entry = m_entry_key(idx.storage_id, k, &key);
                me.rig
                    .db
                    .core
                    .delete_entry(txn, &entry, None, st.ctx())
                    .map_err(nucleus_catalog::DbError::Txn)?;
            }
            op.writes.push((k, Write::Del));
            Ok(true)
        })
    }
}

/// Records `[lo, hi)`'s observations from a live map and returns the
/// live `(slot, Val)` pairs.
fn record_range(op: &mut OpRec, live: &BTreeMap<u8, Val>, lo: u8, hi: u8) -> Vec<(u8, Val)> {
    let mut out = Vec::new();
    for k in lo..hi {
        let obs = live.get(&k).map_or(Obs::Absent, |&v| Obs::Val(v));
        op.reads.push((k, obs));
        if let Obs::Val(v) = obs {
            out.push((k, v));
        }
    }
    out
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if !self.done {
            if let Some(txn) = self.txn.take() {
                let _ = self.rig.db.core.abort(txn);
            }
            self.finish(Fate::Rolledback);
        }
    }
}

// ----- the DDL session ------------------------------------------------------------

/// One DDL program of the DDL session's stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PDdl {
    AddColumn,
    Truncate,
    CreateIndex,
}

/// The DDL txn index at which the run's one online build starts (a pure
/// function of the seed).
pub fn build_at_txn(seed: u64) -> u32 {
    let mut rng = Rng::new(seed ^ 0x6dd1_0000);
    15 + rng.below(50) as u32
}

/// The DDL op of `(seed, index)`: the one build at its seeded txn, then a
/// TRUNCATE-heavy mix with bounded ADD COLUMNs (`cols` = committed adds
/// so far, `built` = the build already ran).
pub fn gen_ddl(seed: u64, index: u32, cols: u32, built: bool) -> PDdl {
    if !built && index >= build_at_txn(seed) {
        return PDdl::CreateIndex;
    }
    let mut rng = Rng::new(seed ^ 0x6dd1 ^ (u64::from(index) << 12));
    if cols < COL_CAP && rng.below(100) >= 70 {
        PDdl::AddColumn
    } else {
        PDdl::Truncate
    }
}

/// How a DDL txn attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlOutcome {
    Committed,
    Failed,
    Retry,
}

fn retryable(e: &SqlErr) -> DdlOutcome {
    if matches!(e, SqlErr::SerFailure | SqlErr::Deadlock) {
        DdlOutcome::Retry
    } else {
        DdlOutcome::Failed
    }
}

/// Files one DDL txn record under its own pre-drawn uid and start tick
/// (the DDL txns never run through the `Session` machinery: their
/// statements are catalog statements, not row statements).
fn ddl_rec(rig: &Rig, uid: u32, start: u64, prog: u32, attempt: u32, ops: Vec<OpRec>, fate: Fate) {
    rig.rec.push(TxnRec {
        uid,
        session: DDL_SESSION,
        prog,
        attempt,
        level: Level::ReadCommitted,
        snap: None,
        start,
        end: start,
        fate,
        ops,
    });
}

/// `ALTER TABLE t ADD COLUMN name int8` — one RC txn, recorded with no
/// ops (ADD COLUMN rewrites no row; the row-space checker sees nothing).
pub fn run_add_column(rig: &Rig, prog: u32, attempt: u32, name: &str) -> DdlOutcome {
    let uid = rig.rec.next_uid();
    let start = rig.rec.tick();
    rig.note(
        DDL_SESSION,
        format!("T{uid} p{prog}.{attempt} ADD COLUMN {name}"),
    );
    let txn = rig.db.core.begin(Isolation::ReadCommitted);
    let r = {
        let st = Stmt::new(&txn, rig.db.core.visible_ts()).expect("a fresh statement");
        rig.db
            .add_column(&st, rig.t_oid, name, KeyType::Int8, false, None)
    };
    match r {
        Ok(()) => match rig.db.core.commit(txn, SyncCommit::On) {
            Ok(ts) => {
                rig.count_bump(Rig::ADDS);
                ddl_rec(
                    rig,
                    uid,
                    start,
                    prog,
                    attempt,
                    Vec::new(),
                    Fate::Committed(ts.0),
                );
                DdlOutcome::Committed
            }
            Err(e) => {
                let e = map_err(e);
                ddl_rec(
                    rig,
                    uid,
                    start,
                    prog,
                    attempt,
                    Vec::new(),
                    Fate::Failed(e.clone()),
                );
                retryable(&e)
            }
        },
        Err(e) => {
            let _ = rig.db.core.abort(txn);
            let e = map_db_err(e);
            ddl_rec(
                rig,
                uid,
                start,
                prog,
                attempt,
                Vec::new(),
                Fate::Failed(e.clone()),
            );
            retryable(&e)
        }
    }
}

/// `TRUNCATE t` — one RC txn: AccessExclusive first, then every slot read
/// under it (no writer can commit behind the lock, so the read is exactly
/// the pre-truncate state), `Write::Del` recorded for each live slot,
/// then the storage swap. An empty table is **not** swapped: nothing is
/// removed, and the relation-level 40001s a swap would force onto
/// in-flight RR/SER readers are outside the row-space checker's language
/// (t6c's `diff` checker owns that surface); the txn records its reads
/// and no writes.
pub fn run_truncate(rig: &Rig, prog: u32, attempt: u32, bug: Bug) -> DdlOutcome {
    let uid = rig.rec.next_uid();
    let start = rig.rec.tick();
    rig.note(DDL_SESSION, format!("T{uid} p{prog}.{attempt} TRUNCATE"));
    let txn = rig.db.core.begin(Isolation::ReadCommitted);
    let mut op = OpRec {
        kind: OpKind::Delete,
        keys: Vec::new(),
        start: rig.rec.tick(),
        end: 0,
        snap: None,
        reads: Vec::new(),
        writes: Vec::new(),
        void: false,
        err: None,
    };
    let mut body = || -> Result<(), nucleus_catalog::DbError> {
        // §3/§10: the lock first, so the reads below see exactly the
        // state the truncate removes.
        rig.db
            .locks
            .lock_relation(
                &rig.db.core,
                &txn,
                rig.t_oid,
                RelLockMode::AccessExclusive,
                LockWait::Block,
                None,
            )
            .map_err(nucleus_catalog::DbError::Txn)?;
        let s = rig.db.core.visible_ts();
        let st = Stmt::new(&txn, s).expect("a fresh statement");
        let h = rig
            .db
            .open_table(&st, rig.t_oid, RelLockMode::AccessShare)?;
        let rows = rig.db.scan(&st, &h)?;
        let mut live = BTreeMap::new();
        for (_, datums) in &rows {
            let (pk, v) = t_row(&h.schema, datums);
            live.insert(pk as u8, v);
        }
        op.snap = Some(s.0);
        for k in 0..SLOTS {
            let obs = live.get(&k).map_or(Obs::Absent, |&v| Obs::Val(v));
            op.reads.push((k, obs));
            if let Obs::Val(_) = obs {
                op.keys.push(k);
                if bug != Bug::TruncateNoDels {
                    op.writes.push((k, Write::Del));
                }
            }
        }
        if live.is_empty() {
            // Nothing to remove: the recorded no-op.
            return Ok(());
        }
        let seq = txn.next_seq().map_err(nucleus_catalog::DbError::Txn)?;
        let ddl = Ddl::new(&rig.db.core, &txn, StmtCtx::new(s, seq, seq));
        rig.db
            .catalog
            .catalog_truncate_table(&ddl, rig.t_oid)
            .map_err(nucleus_catalog::DbError::Txn)?;
        Ok(())
    };
    let r = body();
    op.end = rig.rec.tick();
    match r {
        Ok(()) => match rig.db.core.commit(txn, SyncCommit::On) {
            Ok(ts) => {
                rig.count_bump(Rig::TRUNCATES);
                ddl_rec(
                    rig,
                    uid,
                    start,
                    prog,
                    attempt,
                    vec![op],
                    Fate::Committed(ts.0),
                );
                DdlOutcome::Committed
            }
            Err(e) => {
                let e = map_err(e);
                ddl_rec(
                    rig,
                    uid,
                    start,
                    prog,
                    attempt,
                    vec![op],
                    Fate::Failed(e.clone()),
                );
                retryable(&e)
            }
        },
        Err(e) => {
            let _ = rig.db.core.abort(txn);
            let e = map_db_err(e);
            op.err = Some(e.clone());
            ddl_rec(
                rig,
                uid,
                start,
                prog,
                attempt,
                vec![op],
                Fate::Failed(e.clone()),
            );
            retryable(&e)
        }
    }
}

/// The run's one `CREATE INDEX CONCURRENTLY ON t (m)` — the §7 driver on
/// the DDL thread, recorded as a pseudo-txn whose fate is the driver
/// call's own outcome (`Committed` at the published `built_at`, whose
/// commit the driver acked before returning). Never retried: the ladder
/// owns its txns, and a second call would start a second index.
pub fn run_create_index(rig: &Rig, prog: u32, attempt: u32) -> DdlOutcome {
    let uid = rig.rec.next_uid();
    let start = rig.rec.tick();
    rig.note(
        DDL_SESSION,
        format!("T{uid} p{prog}.{attempt} CREATE INDEX"),
    );
    match create_index_online(
        rig.db.as_ref(),
        rig.t_oid,
        &m_def(),
        true,
        &BuildHooks::default(),
    ) {
        Ok(idx_oid) => {
            rig.count_bump(Rig::BUILDS_OK);
            let def = rig.idx_def_of(idx_oid);
            assert_eq!(def.state, IndexState::Public, "the ladder published");
            ddl_rec(
                rig,
                uid,
                start,
                prog,
                attempt,
                Vec::new(),
                Fate::Committed(def.built_at),
            );
            DdlOutcome::Committed
        }
        Err(e) => {
            rig.count_bump(Rig::BUILDS_FAILED);
            let e = map_db_err(e);
            ddl_rec(rig, uid, start, prog, attempt, Vec::new(), Fate::Failed(e));
            DdlOutcome::Failed
        }
    }
}

// ----- seeded programs ----------------------------------------------------------

/// xorshift64*, seeded through splitmix64 so adjacent seeds differ (the
/// g2 `Rng`, verbatim: the program generator is part of the checked
/// contract).
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        Rng(z | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        (self.next() >> 11) % n
    }
}

/// One statement of a program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum POp {
    Read(u8),
    Scan(u8, u8),
    IndexScan(u8, u8),
    Insert(u8),
    Update(u8),
    Delete(u8),
}

/// A transaction program: fixed statements and a fixed ending,
/// independent of what the statements observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prog {
    pub ops: Vec<POp>,
    pub rollback: bool,
}

/// Statement mix weights (percent-like, relative).
#[derive(Debug, Clone, Copy)]
pub struct Mix {
    pub read: u64,
    /// Read then update the same slot in two statements.
    pub rmw: u64,
    pub scan: u64,
    pub index_scan: u64,
    pub insert: u64,
    pub update: u64,
    pub delete: u64,
}

impl Mix {
    pub const STANDARD: Mix = Mix {
        read: 24,
        rmw: 10,
        scan: 10,
        index_scan: 16,
        insert: 12,
        update: 12,
        delete: 8,
    };
}

/// The program of `(seed, session, index)`.
pub fn gen_prog(seed: u64, session: u16, index: u32, mix: &Mix) -> Prog {
    let mut rng = Rng::new(seed ^ (u64::from(session) << 40) ^ (u64::from(index) << 8) ^ 0x6733);
    let total =
        mix.read + mix.rmw + mix.scan + mix.index_scan + mix.insert + mix.update + mix.delete;
    let n = 1 + rng.below(4) as usize;
    let mut ops = Vec::new();
    for _ in 0..n {
        let mut r = rng.below(total);
        let mut pick = |w: u64| -> bool {
            if r < w {
                true
            } else {
                r -= w;
                false
            }
        };
        let row = |rng: &mut Rng| rng.below(u64::from(SLOTS)) as u8;
        let range = |rng: &mut Rng| {
            let lo = rng.below(u64::from(SLOTS) - 1) as u8;
            (lo, (lo + 1 + rng.below(6) as u8).min(SLOTS))
        };
        if pick(mix.read) {
            ops.push(POp::Read(row(&mut rng)));
        } else if pick(mix.rmw) {
            let k = row(&mut rng);
            ops.push(POp::Read(k));
            ops.push(POp::Update(k));
        } else if pick(mix.scan) {
            let (lo, hi) = range(&mut rng);
            ops.push(POp::Scan(lo, hi));
        } else if pick(mix.index_scan) {
            let (lo, hi) = range(&mut rng);
            ops.push(POp::IndexScan(lo, hi));
        } else if pick(mix.insert) {
            ops.push(POp::Insert(row(&mut rng)));
        } else if pick(mix.update) {
            ops.push(POp::Update(row(&mut rng)));
        } else {
            ops.push(POp::Delete(row(&mut rng)));
        }
    }
    let rollback = rng.below(10) == 0;
    Prog { ops, rollback }
}

/// How a program attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Committed,
    RolledBack,
    /// Failed with 40001 / 40P01: retry with a fresh txn.
    Retry,
    /// Failed with an error the program does not retry.
    Failed,
}

/// Runs one attempt of `prog`.
pub fn run_prog(
    rig: &Rig,
    session: u16,
    prog_idx: u32,
    attempt: u32,
    level: Level,
    bug: Bug,
    prog: &Prog,
) -> Outcome {
    let mut s = Session::begin(rig, session, prog_idx, attempt, level, bug);
    for op in prog.ops.iter() {
        let r = match *op {
            POp::Read(k) => s.read(k).map(|_| ()),
            POp::Scan(lo, hi) => s.scan(lo, hi).map(|_| ()),
            POp::IndexScan(lo, hi) => s.index_scan(lo, hi).map(|_| ()),
            POp::Insert(k) => s.insert(k),
            POp::Update(k) => s.update(k).map(|_| ()),
            POp::Delete(k) => s.delete(k).map(|_| ()),
        };
        if let Err(e) = r {
            return if e.retryable() {
                Outcome::Retry
            } else {
                Outcome::Failed
            };
        }
    }
    if prog.rollback {
        s.rollback();
        return Outcome::RolledBack;
    }
    match s.commit() {
        Ok(()) => Outcome::Committed,
        Err(e) if e.retryable() => Outcome::Retry,
        Err(_) => Outcome::Failed,
    }
}

// ----- the threaded workload -----------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub seed: u64,
    pub level: Level,
    /// DML sessions (the DDL session is the 4th).
    pub threads: u16,
    /// DML programs per session.
    pub progs: u32,
    /// DDL txns of the DDL session.
    pub ddl_txns: u32,
    pub max_attempts: u32,
    pub mix: Mix,
    pub deadlock_ms: u64,
    pub bug: Bug,
}

impl Config {
    pub fn new(seed: u64, level: Level) -> Config {
        Config {
            seed,
            level,
            threads: 3,
            progs: 150,
            ddl_txns: 150,
            max_attempts: 10,
            mix: Mix::STANDARD,
            deadlock_ms: 5,
            bug: Bug::None,
        }
    }
}

/// Runs the seeded workload — `threads` DML sessions at `level` plus the
/// DDL session (the ADD COLUMN / TRUNCATE mix and the one online build)
/// — on real threads over a fresh rig and returns the drained history.
pub fn run_workload(cfg: &Config) -> Run {
    let mut rig = Rig::new(cfg.deadlock_ms);
    rig.setup();
    let rig = &rig;
    let progress = AtomicU64::new(0);
    let finished = AtomicBool::new(false);
    std::thread::scope(|sc| {
        // A hang guard, not a timing assertion: only a run where no
        // attempt at all finishes for a very long stretch (a lost
        // wakeup) is cut short, with a diagnostic.
        sc.spawn(|| {
            let mut last = u64::MAX;
            let mut stalled = 0u32;
            while !finished.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(100));
                let now = progress.load(Ordering::SeqCst);
                if now == last {
                    stalled += 1;
                    if stalled >= 600 {
                        eprintln!(
                            "g2x: no attempt finished for 60 s: the run hung \
                             (seed {:#x}, level {})\n{}",
                            cfg.seed,
                            cfg.level.short(),
                            rig.dump_state()
                        );
                        std::process::abort();
                    }
                } else {
                    stalled = 0;
                    last = now;
                }
            }
        });
        let mut handles = Vec::new();
        for session in 1..=cfg.threads {
            let progress = &progress;
            handles.push(sc.spawn(move || {
                for idx in 0..cfg.progs {
                    let prog = gen_prog(cfg.seed, session, idx, &cfg.mix);
                    for attempt in 0..cfg.max_attempts {
                        let out = run_prog(rig, session, idx, attempt, cfg.level, cfg.bug, &prog);
                        progress.fetch_add(1, Ordering::SeqCst);
                        if out != Outcome::Retry {
                            break;
                        }
                        std::thread::yield_now();
                    }
                }
            }));
        }
        // The DDL session: the ADD COLUMN / TRUNCATE mix with the run's
        // one online build at its seeded txn.
        let progress = &progress;
        handles.push(sc.spawn(move || {
            let mut cols = 0u32;
            let mut built = false;
            for index in 0..cfg.ddl_txns {
                let op = gen_ddl(cfg.seed, index, cols, built);
                let mut attempt = 0u32;
                loop {
                    let out = match op {
                        PDdl::AddColumn => {
                            let name = format!("x{cols}");
                            run_add_column(rig, index, attempt, &name)
                        }
                        PDdl::Truncate => run_truncate(rig, index, attempt, cfg.bug),
                        PDdl::CreateIndex => run_create_index(rig, index, attempt),
                    };
                    progress.fetch_add(1, Ordering::SeqCst);
                    match out {
                        DdlOutcome::Committed => {
                            match op {
                                PDdl::AddColumn => cols += 1,
                                PDdl::CreateIndex => built = true,
                                PDdl::Truncate => {}
                            }
                            break;
                        }
                        // The one build never retries (a second ladder
                        // would be a second index).
                        DdlOutcome::Failed => {
                            if op == PDdl::CreateIndex {
                                built = true;
                            }
                            break;
                        }
                        DdlOutcome::Retry => {
                            attempt += 1;
                            if attempt >= cfg.max_attempts {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }));
        for h in handles {
            h.join().expect("a session thread panicked");
        }
        finished.store(true, Ordering::SeqCst);
    });
    rig.finish()
}
