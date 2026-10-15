//! The t6c runner (C-T6c1a item 1): sessions over one shared
//! `Db<MemKv>` with the real lock manager, commit thread, resolver and
//! SSI retention. Four DML sessions run seeded programs (RC/RR/SER, 1-4
//! ops over `t` and `u`); one DDL session issues each of ADD COLUMN,
//! DROP COLUMN, SET/DROP NOT NULL and TRUNCATE in its own RC txn, and
//! runs amcheck at three mid-run points and once at the end; retries on
//! 40001/40P01 are fresh txns. Everything a statement observes or
//! applies is recorded (item 2): level, statement snapshot, the reported
//! `def_version` and storage id, result rows (rowids included for `u`),
//! success or SQLSTATE, and per txn the fate.
//!
//! Every statement is one [`Stmt`] with one [`TableHandle`] opened at the
//! §4 lock mode for its op (AccessShare for reads, RowExclusive for
//! DML). RC statements take a fresh registered snapshot each (the guard
//! lives for the statement); RR/SER take one for the txn
//! (`Ssi::begin` under SERIALIZABLE). An UPDATE's effect is captured by
//! a read-back through a second statement of the same txn at the same
//! snapshot, which the read rule shows the txn's own earlier write — so
//! the recorded `new_row` is the effect the engine applied, EPQ re-base
//! included; the write-effect *oracle* is C-T6c1b's.
//!
//! `u` has no PK, so its rows are addressed by the hidden rowid the scan
//! returns; a delete drives `Core::row_op` on that key directly (the
//! typed `delete_with` takes pk values, which `u` has none of). `u` has
//! no indexes, so the raw row op is the whole statement.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_catalog::amcheck::{check_table, Report};
use nucleus_catalog::{Db, Stmt, TableHandle};
use nucleus_codec::row::{value_repr_eq, Datum, RowError, TableSchema};
use nucleus_codec::{decode_key_prefix, Collation, KeyColumn, KeyType, Value};
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::catalog::{Catalog, Ddl};
use nucleus_txn::commit::{spawn_commit_thread_with, CommitConfig, CommitThreadHandle, SyncCommit};
use nucleus_txn::encoding::{parse_key, Entry, VersionValue};
use nucleus_txn::locks::{LockManager, RelLockMode};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::resolver::{spawn_background, BackgroundHandle, Resolver};
use nucleus_txn::ssi::{spawn_retention, RetentionHandle, Ssi};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{CommittedVersion, Epq, EpqDecision, RowOp, RowOutcome};

use super::{
    derive_const, derive_text, gen_ddl, gen_prog, AmcheckRec, Config, Fate, Level, OpDetail,
    OpKind, OpOut, OpRec, PDdl, POp, Prog, Recorder, RowVals, Run, TxnRec,
};

/// The DDL session's id (DML sessions are `1..=threads`).
pub const DDL_SESSION: u16 = 5;

// ----- C-T6c2: harness-level injected bugs (G2's `Bug` pattern) ---------------

/// A harness-level defect injected into a session (C-T6c2 item 1, G2's
/// `Bug` pattern): the engine is never edited — each variant changes
/// only what the *harness* does, and the unchanged checker
/// (`diff::check`) must flag the recorded history. The proof of life
/// (`t6c_injected`) runs each bug's deterministic script over its fixed
/// seed set (5 of 5 runs reported) and the same scripts bug-free (clean).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    None,
    /// **B1** — rows decoded with the schema at `S` instead of the
    /// reported `def_version`. The harness's stand-in for the schema at
    /// the txn's snapshot is the live definition its ghost read returned
    /// when the txn began (no locks held: the read races nothing in the
    /// deterministic scripts). A statement whose handle reports a newer
    /// definition — a DDL committed between the txn's begin and its
    /// first `t` statement — records its rows rendered at the older one,
    /// so the new columns vanish from the record and the model's render
    /// at the reported version mismatches.
    DecodeAtSeenSchema,
    /// **B2** — a session claiming RR takes a fresh snapshot per
    /// statement. The engine statement runs at the fresh snapshot (RC
    /// behaviour, which is why it commits where honest RR raises 40001),
    /// while the record keeps the txn's claimed snapshot — the lie the
    /// REPEATABLE READ label tells. A foreign commit landing between two
    /// statements of the txn then reads back a row the model cannot have
    /// at the claimed snapshot.
    RrPerStmtSnapshot,
    /// **B3** — the harness records a write txn as committed but runs it
    /// in a txn it aborts. The abort is real (nothing leaks, amcheck
    /// stays clean) but the record claims `Committed` at the current
    /// visible ts, so the model applies a write the engine never made
    /// and every later reader and the final scan see the phantom.
    AbortedAsCommitted,
    /// **B4** — UpdateSet computed in the harness from the snapshot row
    /// and written as a full-row update. The session remembers the last
    /// row it read of `t` (its "snapshot row") and writes every column
    /// of that row back with the set applied, instead of the one set
    /// column — the application-level lost update: whatever a foreign
    /// txn committed in between is silently clobbered, and the
    /// write-effect oracle's recompute (base at the commit position plus
    /// the recorded set) no longer equals the read-back.
    FullRowUpdateFromRead,
    /// **B5** — the harness renders the missing default as NULL. A
    /// decoded value equal to its column's ADD COLUMN default is
    /// recorded as NULL — the harness twin of the E1 codec mutant (a
    /// decode rule that ignores `missing`): old rows lose their default
    /// in the record where the model renders it.
    MissingDefaultAsNull,
}

/// The `ddl_index` of the end-of-run amcheck records (distinct from every
/// mid-run point's index, which is `< ddl_txns`).
pub const END_AMCHECK: u32 = u32::MAX;

// ----- the rig ---------------------------------------------------------------

/// The shared engine: one `Db` over `Core<MemKv>` with the catalog, the
/// commit thread (SSI's writer-map observer), the resolver and retention
/// threads, and the recorder. `t_oid` is the DDL target; `u_oid` keeps a
/// fixed schema.
pub struct Rig {
    pub db: Db<MemKv>,
    pub t_oid: u64,
    pub u_oid: u64,
    pub rec: Recorder,
    amcheck: Mutex<Vec<AmcheckRec>>,
    /// Each session thread's last note (the hang diagnostic).
    status: Mutex<BTreeMap<u16, String>>,
    commit: Mutex<Option<CommitThreadHandle>>,
    resolver: Mutex<Option<BackgroundHandle>>,
    retention: Mutex<Option<RetentionHandle>>,
}

impl Rig {
    /// A fresh rig with the commit pipeline running (the g2 pattern).
    pub fn new(deadlock_ms: u64) -> Rig {
        let core = Arc::new(Core::open(MemKv::new()).expect("open the core"));
        let ssi = Ssi::install(&core);
        let locks = LockManager::install(&core);
        let catalog = Catalog::open(&core, &ssi, &locks).expect("open the catalog");
        let db = Db::new(core.clone(), ssi.clone(), locks.clone(), catalog);
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
            db,
            t_oid: 0,
            u_oid: 0,
            rec: Recorder::new(),
            amcheck: Mutex::new(Vec::new()),
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

    /// The setup (session 0): CREATE TABLE `t` and `u` in one unrecorded
    /// RC txn, then one recorded RC txn inserting a row per pk of the
    /// domain into `t` (`a = 1000 + pk`, `b = "init{pk}"`) and three rows
    /// into `u` (`a = 9000 + i`).
    pub fn setup(&mut self) {
        let (t_oid, u_oid) = {
            let txn = self.db.core.begin(Isolation::ReadCommitted);
            let guard = self.db.core.registry.take_snapshot();
            let (t_oid, _) = {
                let st = Stmt::new(&txn, guard.ts()).expect("setup statement");
                self.db
                    .create_table(&st, b"t", &t_schema())
                    .expect("create t")
            };
            let (u_oid, _) = {
                let st = Stmt::new(&txn, guard.ts()).expect("setup statement");
                self.db
                    .create_table(&st, b"u", &u_schema())
                    .expect("create u")
            };
            drop(guard);
            self.db
                .core
                .commit(txn, SyncCommit::On)
                .expect("setup commit");
            Resolver::run_once(&self.db.core).expect("resolve setup");
            (t_oid, u_oid)
        };
        self.t_oid = t_oid;
        self.u_oid = u_oid;
        self.setup_rows();
    }

    /// The setup's row inserts — one recorded RC txn (session 0, program
    /// 0, attempt 0) whose ops are ordinary InsertT/InsertU records.
    fn setup_rows(&self) {
        let mut s = Session::begin(self, 0, 0, 0, Level::ReadCommitted);
        for pk in 0..super::PKS {
            let a = Value::Int8(1000 + pk);
            let b = Value::Text(format!("init{pk}"));
            s.exec(pk as usize, OpKind::InsertT, |s, st, op| {
                s.insert_t(st, op, pk, a.clone(), Some(b.clone()))
                    .map(|_| ())
            })
            .expect("setup insert into t");
        }
        for i in 0..3usize {
            let a = Value::Int8(9000 + i as i64);
            let b = Value::Text(format!("uinit{i}"));
            s.exec(super::PKS as usize + i, OpKind::InsertU, |s, st, op| {
                s.insert_u(st, op, a.clone(), Some(b.clone())).map(|_| ())
            })
            .expect("setup insert into u");
        }
        s.commit().expect("setup rows commit");
    }

    /// Stops the background threads, drains resolution, scans for leaked
    /// `/t/`·`/u/` intents and hands the run back. The final full scans
    /// and the final amcheck run as the last readers of a live pipeline.
    pub fn finish(&self) -> Run {
        let final_t = self.final_scan_t();
        let final_u = self.final_scan_u();
        // amcheck at the end: one registered snapshot per table (§8's
        // "at the end"), filed with the mid-run reports.
        for (table, oid) in [("t", self.t_oid), ("u", self.u_oid)] {
            let guard = self.db.core.registry.take_snapshot();
            let report = match check_table(&self.db, oid, &guard) {
                Ok(r) => r,
                Err(e) => {
                    let mut r = Report::default();
                    r.violations.push(format!("check_table returned {e:?}"));
                    r
                }
            };
            drop(guard);
            self.amcheck.lock().expect("amcheck slot").push(AmcheckRec {
                ddl_index: END_AMCHECK,
                table,
                report,
            });
        }
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
        let (leaked_intents, leak_report) = self.leaked();
        Run {
            history: self.rec.drain(),
            amcheck: std::mem::take(&mut *self.amcheck.lock().expect("amcheck slot")),
            final_t,
            final_u,
            leaked_intents,
            leak_report,
        }
    }

    /// One full-table scan in its own committed RC txn: the def version,
    /// storage id and schema its handle reported, and the raw rows.
    fn final_scan_rows(&self, oid: u64) -> FinalScan {
        let txn = self.db.core.begin(Isolation::ReadCommitted);
        let guard = self.db.core.registry.take_snapshot();
        let st = Stmt::new(&txn, guard.ts()).expect("final scan statement");
        let h = self
            .db
            .open_table(&st, oid, RelLockMode::AccessShare)
            .expect("open the table for the final scan");
        let rows = self.db.scan(&st, &h).expect("the final scan");
        let out = FinalScan {
            version: h.def_version,
            sid: h.storage_id,
            schema: h.schema.clone(),
            rows,
        };
        drop(guard);
        self.db
            .core
            .commit(txn, SyncCommit::On)
            .expect("final scan commit");
        out
    }

    /// `t`'s final full scan, keyed by pk.
    fn final_scan_t(&self) -> (u32, Vec<(i64, RowVals)>) {
        let FinalScan {
            version,
            sid,
            schema,
            rows,
        } = self.final_scan_rows(self.t_oid);
        let keyed = rows
            .iter()
            .map(|(key, datums)| {
                let pk = pk_of_key(sid, key).expect("a t row key decodes");
                (pk, pairs(&schema, datums))
            })
            .collect();
        (version, keyed)
    }

    /// `u`'s final full scan, with the raw rowid keys.
    fn final_scan_u(&self) -> (u32, Vec<(Vec<u8>, RowVals)>) {
        let FinalScan {
            version,
            schema,
            rows,
            ..
        } = self.final_scan_rows(self.u_oid);
        let keyed = rows
            .iter()
            .map(|(key, datums)| (key.clone(), pairs(&schema, datums)))
            .collect();
        (version, keyed)
    }

    /// Intents left under `/t/` and `/u/` once everything drained.
    fn leaked(&self) -> (usize, String) {
        let view = self.db.core.open_view();
        let mut leaked = 0;
        let mut report = String::new();
        for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let (stored, _) = row.expect("scan the kv");
            if let Some((logical, Entry::Intent)) =
                parse_key(&stored).filter(|(l, _)| l.starts_with(b"/t/") || l.starts_with(b"/u/"))
            {
                leaked += 1;
                report.push_str(&format!("  leaked intent on {logical:?}\n"));
            }
        }
        (leaked, report)
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

// ----- the final scans ---------------------------------------------------------

/// One final full-table scan: the def version, storage id and schema its
/// handle reported, and the raw rows.
struct FinalScan {
    version: u32,
    sid: u64,
    schema: TableSchema,
    rows: Vec<(Vec<u8>, Vec<Option<Datum>>)>,
}

// ----- schemas, keys, rows ----------------------------------------------------

/// `t(pk int8 PK, a int8, b text, big text)` — C-T6d2's fourth column
/// carries the large values ([`super::BIG_LENS`], [`super::big_blob`]).
pub fn t_schema() -> TableSchema {
    schema_of(
        vec![
            ("pk", KeyType::Int8, true),
            ("a", KeyType::Int8, false),
            ("b", KeyType::Text(Collation::C), false),
            ("big", KeyType::Text(Collation::C), false),
        ],
        true,
    )
}

/// `u(a int8, b text)` — no PK, hidden rowid (§3).
pub fn u_schema() -> TableSchema {
    schema_of(
        vec![
            ("a", KeyType::Int8, false),
            ("b", KeyType::Text(Collation::C), false),
        ],
        false,
    )
}

/// The live attnum of `t`'s `big` column as this handle's definition has
/// it (`None` = dropped — a DropBig/AddBig cycle moves the attnum, so
/// the ops resolve it by name at execution, exactly as a UPDATE naming
/// the column would).
fn big_attnum(h: &TableHandle) -> Option<u16> {
    h.schema
        .live_columns()
        .find(|c| c.name == "big")
        .map(|c| c.attnum)
}

fn schema_of(cols: Vec<(&str, KeyType, bool)>, with_pk: bool) -> TableSchema {
    use nucleus_codec::row::ColumnDef;
    let cols: Vec<_> = cols
        .into_iter()
        .enumerate()
        .map(|(i, (name, ty, not_null))| ColumnDef {
            attnum: u16::try_from(i + 1).expect("attnum fits"),
            name: name.into(),
            ty,
            not_null,
            dropped: false,
            missing: None,
        })
        .collect();
    let pk = if with_pk { vec![1] } else { Vec::new() };
    TableSchema::new(cols, pk).expect("the fixture schema is valid")
}

/// A row as the records carry it: `(attnum, value)` over the handle's
/// live columns. A toasted datum (impossible for these values) becomes a
/// marker that can never equal a modeled value, so it surfaces as a
/// mismatch instead of being silently dropped.
fn pairs(schema: &TableSchema, datums: &[Option<Datum>]) -> RowVals {
    schema
        .live_columns()
        .zip(datums)
        .map(|(col, datum)| {
            (
                col.attnum,
                datum.as_ref().map(|d| match d {
                    Datum::Inline(v) => v.clone(),
                    Datum::Toast(_) => Value::Text("\u{0}TOAST".into()),
                }),
            )
        })
        .collect()
}

/// The pk encoded in a `t` row key (past `table_lo`).
fn pk_of_key(sid: u64, key: &[u8]) -> Option<i64> {
    let lo = nucleus_catalog::table_lo(sid);
    let suffix = key.strip_prefix(&lo[..])?;
    let cols = [KeyColumn::asc(KeyType::Int8)];
    let (vals, used) = decode_key_prefix(&cols, suffix).ok()?;
    if used != suffix.len() {
        return None;
    }
    match vals.first() {
        Some(Some(Value::Int8(pk))) => Some(*pk),
        _ => None,
    }
}

/// The DELETE EPQ of a `u` row: the newest committed version is the base
/// the row op acts on; a tombstone skips (§5.2).
struct UDeleteEpq;

impl Epq for UDeleteEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        match &newest.value {
            VersionValue::Live { .. } => EpqDecision::Apply(RowOp::Delete),
            VersionValue::Tombstone { .. } => EpqDecision::Skip,
        }
    }
}

// ----- sessions ----------------------------------------------------------------

/// One transaction attempt (the g2 `Session`, adapted to the typed
/// layer: statements are `Stmt`s, ops open their own handle).
pub struct Session<'r> {
    rig: &'r Rig,
    session: u16,
    prog: u32,
    attempt: u32,
    level: Level,
    bug: Bug,
    txn: Option<Txn>,
    guard: Option<SnapshotGuard<'r>>,
    /// Rowids this txn's earlier ScanU statements saw (DeleteU targets).
    u_seen: RefCell<Vec<Vec<u8>>>,
    /// B1's stand-in for "the schema at S": the live definition of `t`
    /// the ghost read returned when the txn began.
    begin_schema: RefCell<Option<TableSchema>>,
    /// B4's stand-in for "the snapshot row": the last row of `t` this
    /// txn read, by pk.
    t_read: RefCell<BTreeMap<i64, RowVals>>,
    rec: TxnRec,
    done: bool,
}

fn iso(level: Level) -> Isolation {
    match level {
        Level::ReadCommitted => Isolation::ReadCommitted,
        Level::RepeatableRead => Isolation::RepeatableRead,
        Level::Serializable => Isolation::Serializable,
    }
}

impl<'r> Session<'r> {
    pub fn begin(rig: &'r Rig, session: u16, prog: u32, attempt: u32, level: Level) -> Session<'r> {
        Session::begin_with(rig, session, prog, attempt, level, Bug::None)
    }

    /// [`Session::begin`] with an injected [`Bug`] (C-T6c2); the soak's
    /// sessions all run `Bug::None`, and no bug changes what a bug-free
    /// session records.
    pub fn begin_with(
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
        let txn_snap = guard.as_ref().map(|g| g.ts().0);
        // B1's snapshot-schema read: no statement has run, so the txn
        // holds no relation lock and the ghost read races nothing in the
        // deterministic scripts.
        let begin_schema = (bug == Bug::DecodeAtSeenSchema)
            .then(|| live_schema(rig).expect("the live schema at begin"));
        Session {
            rig,
            session,
            prog,
            attempt,
            level,
            bug,
            txn: Some(txn),
            guard,
            u_seen: RefCell::new(Vec::new()),
            begin_schema: RefCell::new(begin_schema),
            t_read: RefCell::new(BTreeMap::new()),
            rec: TxnRec {
                uid,
                session,
                prog,
                attempt,
                level,
                txn_snap,
                start,
                end: start,
                fate: Fate::Failed("in-flight".into()),
                ops: Vec::new(),
            },
            done: false,
        }
    }

    fn finish(&mut self, fate: Fate) {
        self.done = true;
        self.rec.end = self.rig.rec.tick();
        self.rec.fate = fate;
        self.guard = None;
        self.rig.rec.push(self.rec.clone());
    }

    fn fail(&mut self, code: String) {
        if let Some(txn) = self.txn.take() {
            self.rig.db.core.abort(txn).expect("abort a failed txn");
        }
        self.finish(Fate::Failed(code));
    }

    pub fn commit(mut self) -> Result<u64, String> {
        let Some(txn) = self.txn.take() else {
            let code = "XX000".to_string();
            self.finish(Fate::Failed(code.clone()));
            return Err(code);
        };
        if self.bug == Bug::AbortedAsCommitted {
            // B3: the txn really aborts (nothing leaks, amcheck stays
            // clean) while the record claims a commit at the current
            // visible ts — a phantom the model applies and no reader can
            // ever observe.
            self.rig.db.core.abort(txn).expect("abort the B3 txn");
            let ts = self.rig.db.core.visible_ts().0;
            self.finish(Fate::Committed(ts));
            return Ok(ts);
        }
        match self.rig.db.core.commit(txn, SyncCommit::On) {
            Ok(ts) => {
                self.finish(Fate::Committed(ts.0));
                Ok(ts.0)
            }
            Err(e) => {
                // `commit_submit` aborts on a pre-commit failure itself.
                let code = e.sqlstate().to_string();
                self.finish(Fate::Failed(code.clone()));
                Err(code)
            }
        }
    }

    /// Runs one program op as one statement of `kind`: opens the op
    /// record, runs `body` (which fills in the detail and outcome), then
    /// closes and files the record. An error aborts the txn (recorded)
    /// and stops the program.
    fn exec<T>(
        &mut self,
        pos: usize,
        kind: OpKind,
        body: impl FnOnce(&Session<'r>, &Stmt<'_>, &mut OpRec) -> Result<T, nucleus_catalog::DbError>,
    ) -> Result<T, String> {
        assert!(!self.done, "statement on a finished session");
        let mut op = OpRec {
            kind,
            detail: OpDetail::Pending,
            out: OpOut::Ok,
            snap: 0,
            def_version: None,
            sid: None,
            start: self.rig.rec.tick(),
            end: 0,
        };
        self.rig.note(
            self.session,
            format!(
                "T{} p{}.{}, op {pos} {:?}",
                self.rec.uid, self.prog, self.attempt, kind
            ),
        );
        let txn = self.txn.as_ref().expect("the txn is alive");
        if self.level == Level::Serializable {
            if let Err(e) = self.rig.db.ssi.check_doomed(txn.id) {
                let code = e.sqlstate().to_string();
                op.out = OpOut::Err(code.clone());
                op.end = self.rig.rec.tick();
                self.rec.ops.push(op);
                self.fail(code.clone());
                return Err(code);
            }
        }
        // RC: a fresh registered snapshot per statement, held for the
        // statement. RR/SER: the txn's. B2: an RR session whose
        // statements each take a fresh snapshot (RC behaviour under an
        // RR claim) — the engine statement runs at the fresh one, the
        // record keeps the txn's claimed snapshot.
        let claimed = self.guard.as_ref().map(|g| g.ts().0);
        let b2 = self.bug == Bug::RrPerStmtSnapshot && self.level == Level::RepeatableRead;
        let rc_guard;
        let snapshot = match self.guard.as_ref() {
            Some(g) if !b2 => g.ts(),
            _ => {
                rc_guard = self.rig.db.core.registry.take_snapshot();
                rc_guard.ts()
            }
        };
        let st = Stmt::new(txn, snapshot).expect("a fresh statement");
        op.snap = if b2 {
            claimed.unwrap_or(snapshot.0)
        } else {
            snapshot.0
        };
        let r = body(self, &st, &mut op);
        op.end = self.rig.rec.tick();
        match r {
            Ok(v) => {
                self.rec.ops.push(op);
                Ok(v)
            }
            Err(e) => {
                let code = e.sqlstate().to_string();
                op.out = OpOut::Err(code.clone());
                self.rec.ops.push(op);
                self.fail(code.clone());
                Err(code)
            }
        }
    }

    // ---- op bodies ---------------------------------------------------------

    /// Records a decoded row of `t` as the injected bugs direct
    /// (C-T6c2): B1 renders it with the schema the txn's ghost read saw
    /// at begin — the harness's stand-in for "the schema at S" — instead
    /// of the handle's reported definition, so columns a DDL added after
    /// the txn began vanish from the record; B5 renders a decoded value
    /// equal to its column's ADD COLUMN default as NULL — the harness's
    /// stand-in for a decode rule that ignored `missing`. A bug-free
    /// session renders at the handle's definition, exactly as the engine
    /// decoded the row.
    fn record_t_row(&self, h: &TableHandle, datums: &[Option<Datum>]) -> RowVals {
        let schema = match self.bug {
            Bug::DecodeAtSeenSchema => self
                .begin_schema
                .borrow()
                .clone()
                .unwrap_or_else(|| h.schema.clone()),
            _ => h.schema.clone(),
        };
        let mut row = pairs(&schema, datums);
        if self.bug == Bug::MissingDefaultAsNull {
            for (col, slot) in schema.live_columns().zip(row.iter_mut()) {
                if let Some(missing) = &col.missing {
                    let is_default = slot.1.as_ref().is_some_and(|v| value_repr_eq(v, missing));
                    if is_default {
                        slot.1 = None;
                    }
                }
            }
        }
        row
    }

    /// B4 remembers the last row of `t` the txn read (its "snapshot
    /// row"); B5/B1 never combine with it, and honest sessions cache
    /// nothing.
    fn note_t_read(&self, pk: i64, row: &RowVals) {
        if self.bug == Bug::FullRowUpdateFromRead {
            self.t_read.borrow_mut().insert(pk, row.clone());
        }
    }

    /// `SELECT * FROM t WHERE pk = ..`.
    fn get(&self, st: &Stmt<'_>, op: &mut OpRec, pk: i64) -> Result<(), nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::AccessShare)?;
        op.detail = OpDetail::Get { pk };
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let row = self.rig.db.get(st, &h, &[Some(Value::Int8(pk))])?;
        let recorded = row.as_deref().map(|d| self.record_t_row(&h, d));
        if let Some(r) = &recorded {
            self.note_t_read(pk, r);
        }
        op.out = OpOut::Row(recorded);
        Ok(())
    }

    /// `SELECT * FROM t`.
    fn scan_t(&self, st: &Stmt<'_>, op: &mut OpRec) -> Result<(), nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::AccessShare)?;
        op.detail = OpDetail::ScanT;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let rows = self.rig.db.scan(st, &h)?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, datums) in &rows {
            let pk = pk_of_key(h.storage_id, key).ok_or(nucleus_catalog::DbError::Row(
                RowError::Corrupt("a t row key does not decode"),
            ))?;
            let row = self.record_t_row(&h, datums);
            self.note_t_read(pk, &row);
            out.push((pk, row));
        }
        op.out = OpOut::RowsT(out);
        Ok(())
    }

    /// `INSERT INTO t VALUES (pk, ..)`: one datum per live column of the
    /// handle's definition — the pk, fresh constants for `a` and `b`
    /// (`b` NULL when chosen), NULL for columns a DDL added.
    fn insert_t(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        pk: i64,
        a: Value,
        b: Option<Value>,
    ) -> Result<(), nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::RowExclusive)?;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let row = build_row(&h, |name| match name {
            "pk" => Some(Value::Int8(pk)),
            "a" => Some(a.clone()),
            "b" => b.clone(),
            _ => None,
        });
        op.detail = OpDetail::InsertT {
            pk,
            row: row.clone(),
        };
        let datums = row
            .iter()
            .map(|(_, v)| v.clone().map(Datum::Inline))
            .collect::<Vec<_>>();
        self.rig.db.insert_with(st, &h, &datums)?;
        op.out = OpOut::Ok;
        Ok(())
    }

    /// `UPDATE t SET col = const WHERE pk = ..`. The column is resolved
    /// against the live definition at execution (the program pins a
    /// choice index; `av`/`tv` are the int8/text constants, picked by the
    /// column's type). The applied row is read back through a second
    /// statement of the same txn at the same snapshot, which the read
    /// rule shows the txn's own write — so `new_row` is the effect the
    /// engine applied, EPQ re-base included.
    fn update_set(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        pk: i64,
        choice: u32,
        av: Value,
        tv: Value,
    ) -> Result<bool, nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::RowExclusive)?;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let attnum = resolve_col(&h, choice).ok_or(nucleus_catalog::DbError::UndefinedColumn)?;
        let value = if col_is_text(&h, attnum) { tv } else { av };
        let mut set = vec![(attnum, Some(Datum::Inline(value.clone())))];
        if self.bug == Bug::FullRowUpdateFromRead {
            // B4: the whole row is computed from the row the txn last
            // read (its "snapshot row") and written back as a full-row
            // update — the application-level lost update. With no cached
            // row the bug is inert (the plain set column alone).
            if let Some(base) = self.t_read.borrow().get(&pk).cloned() {
                set = base
                    .iter()
                    .map(|(a, v)| (*a, v.clone().map(Datum::Inline)))
                    .collect();
                if let Some(slot) = set.iter_mut().find(|(a, _)| *a == attnum) {
                    slot.1 = Some(Datum::Inline(value.clone()));
                }
            }
        }
        op.detail = OpDetail::UpdateSet {
            pk,
            attnum,
            value: Some(value),
            new_row: None,
        };
        let applied = self
            .rig
            .db
            .update_with(st, &h, &[Some(Value::Int8(pk))], &set)?;
        if applied {
            // The read-back runs at `visible_ts` at RC (a legal RC
            // statement position; the txn's own intent shadows any
            // foreign version of the row): an EPQ re-base can have
            // carried a `big` pointer from a version committed **above**
            // the statement's snapshot, whose chunks are invisible at
            // that snapshot (§5) — reading back at S would report XX001
            // the engine never committed. At RR/SER the base cannot sit
            // above S (a newer version is 40001 there), so the read-back
            // stays at the statement's snapshot.
            let txn = self.txn.as_ref().expect("the txn is alive");
            let snap = if self.level == Level::ReadCommitted {
                self.rig.db.core.visible_ts()
            } else {
                st.snapshot()
            };
            let st2 = Stmt::new(txn, snap).expect("the read-back statement");
            let row = self.rig.db.get(&st2, &h, &[Some(Value::Int8(pk))])?;
            if let OpDetail::UpdateSet { new_row, .. } = &mut op.detail {
                *new_row = row.as_deref().map(|d| self.record_t_row(&h, d));
            }
        }
        op.out = OpOut::Applied(applied);
        Ok(applied)
    }

    /// `DELETE FROM t WHERE pk = ..`.
    fn delete_t(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        pk: i64,
    ) -> Result<bool, nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::RowExclusive)?;
        op.detail = OpDetail::DeleteT { pk };
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let applied = self.rig.db.delete_with(st, &h, &[Some(Value::Int8(pk))])?;
        op.out = OpOut::Applied(applied);
        Ok(applied)
    }

    /// C-T6d2's `UPDATE t SET big = <len-byte text> WHERE pk = ..` (§5):
    /// the value is [`big_blob`]`(tag, len)`, the column resolved by name
    /// against the live definition (a dropped `big` is the 42703 the
    /// statement deserves). The read-back is `update_set`'s: a second
    /// statement of the same txn at the same snapshot, so `new_row` is
    /// the effect the engine applied — the full detoasted value, EPQ
    /// re-base included.
    fn set_big(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        pk: i64,
        choice: u32,
        tag: u8,
    ) -> Result<bool, nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::RowExclusive)?;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let attnum = big_attnum(&h).ok_or(nucleus_catalog::DbError::UndefinedColumn)?;
        let len = super::BIG_LENS[choice as usize % super::BIG_LENS.len()];
        let value = super::big_blob(tag, len);
        op.detail = OpDetail::SetBig {
            pk,
            len: len as u32,
            tag,
            attnum,
            new_row: None,
        };
        let applied = self.rig.db.update_with(
            st,
            &h,
            &[Some(Value::Int8(pk))],
            &[(attnum, Some(Datum::Inline(value)))],
        )?;
        if applied {
            self.read_back_big(st, &h, op, pk)?;
        }
        op.out = OpOut::Applied(applied);
        Ok(applied)
    }

    /// C-T6d2's `UPDATE t SET big = NULL WHERE pk = ..`.
    fn null_big(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        pk: i64,
    ) -> Result<bool, nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.t_oid, RelLockMode::RowExclusive)?;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let attnum = big_attnum(&h).ok_or(nucleus_catalog::DbError::UndefinedColumn)?;
        op.detail = OpDetail::NullBig {
            pk,
            attnum,
            new_row: None,
        };
        let applied =
            self.rig
                .db
                .update_with(st, &h, &[Some(Value::Int8(pk))], &[(attnum, None)])?;
        if applied {
            self.read_back_big(st, &h, op, pk)?;
        }
        op.out = OpOut::Applied(applied);
        Ok(applied)
    }

    /// The post-write read-back a SetBig/NullBig records (`new_row`) —
    /// `update_set`'s shape and snapshot rule: at RC a fresh statement
    /// at `visible_ts` (the own-intent row shadows any foreign version,
    /// and the statement's own chunk intents are visible to a later
    /// statement at any position), at RR/SER the statement's snapshot.
    /// Errors propagate, so a read-back failure aborts the txn exactly
    /// as it would there.
    fn read_back_big(
        &self,
        st: &Stmt<'_>,
        h: &TableHandle,
        op: &mut OpRec,
        pk: i64,
    ) -> Result<(), nucleus_catalog::DbError> {
        let txn = self.txn.as_ref().expect("the txn is alive");
        let snap = if self.level == Level::ReadCommitted {
            self.rig.db.core.visible_ts()
        } else {
            st.snapshot()
        };
        let st2 = Stmt::new(txn, snap).expect("the read-back statement");
        let row = self.rig.db.get(&st2, h, &[Some(Value::Int8(pk))])?;
        if let OpDetail::SetBig { new_row, .. } | OpDetail::NullBig { new_row, .. } = &mut op.detail
        {
            *new_row = row.as_deref().map(|d| self.record_t_row(h, d));
        }
        Ok(())
    }

    /// `INSERT INTO u VALUES (..)` — hidden rowid (§3): the key comes
    /// from the statement's ordinal counter, the row from `a` and `b`.
    fn insert_u(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        a: Value,
        b: Option<Value>,
    ) -> Result<(), nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.u_oid, RelLockMode::RowExclusive)?;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let row = build_row(&h, |name| match name {
            "a" => Some(a.clone()),
            "b" => b.clone(),
            _ => None,
        });
        op.detail = OpDetail::InsertU { row: row.clone() };
        let datums = row
            .iter()
            .map(|(_, v)| v.clone().map(Datum::Inline))
            .collect::<Vec<_>>();
        self.rig.db.insert_with(st, &h, &datums)?;
        op.out = OpOut::Ok;
        Ok(())
    }

    /// `SELECT * FROM u` — rows with their rowids.
    fn scan_u(&self, st: &Stmt<'_>, op: &mut OpRec) -> Result<(), nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.u_oid, RelLockMode::AccessShare)?;
        op.detail = OpDetail::ScanU;
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let rows = self.rig.db.scan(st, &h)?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, datums) in &rows {
            out.push((key.clone(), pairs(&h.schema, datums)));
        }
        self.u_seen
            .borrow_mut()
            .extend(out.iter().map(|(key, _)| key.clone()));
        op.out = OpOut::RowsU(out);
        Ok(())
    }

    /// `DELETE FROM u WHERE rowid = ..` — a rowid an earlier ScanU of
    /// this txn saw (a no-op statement when it saw none). The delete
    /// drives `Core::row_op` on the row key after a §4 read of it: `u`
    /// has no callable key and no indexes, so the row op is the whole
    /// statement.
    fn delete_u(
        &self,
        st: &Stmt<'_>,
        op: &mut OpRec,
        choice: u32,
    ) -> Result<bool, nucleus_catalog::DbError> {
        let h = self
            .rig
            .db
            .open_table(st, self.rig.u_oid, RelLockMode::RowExclusive)?;
        op.detail = OpDetail::DeleteU { key: None };
        op.def_version = Some(h.def_version);
        op.sid = Some(h.storage_id);
        let seen_len = self.u_seen.borrow().len();
        let target = pick_index(choice, seen_len).and_then(|idx| {
            let mut seen = self.u_seen.borrow_mut();
            (idx < seen.len()).then(|| seen.swap_remove(idx))
        });
        let Some(key) = target else {
            op.out = OpOut::Applied(false);
            return Ok(false);
        };
        let ctx = ReadCtx {
            txn: st.txn().id,
            snapshot: st.snapshot(),
            stmt_seq: st.seq0(),
        };
        match st.txn().isolation {
            Isolation::Serializable => {
                self.rig
                    .db
                    .ssi
                    .read_key(&self.rig.db.core, st.txn().id, &key, &ctx)?;
            }
            _ => {
                let view = self.rig.db.core.open_view();
                read_key(&self.rig.db.core, &view, &key, &ctx, &mut NoSsi)?;
            }
        }
        let txn = self.txn.as_ref().expect("the txn is alive");
        let mut epq = UDeleteEpq;
        let outcome =
            self.rig
                .db
                .core
                .row_op(txn, &key, None, RowOp::Delete, st.ctx(), &mut epq)?;
        op.detail = OpDetail::DeleteU { key: Some(key) };
        let applied = outcome == RowOutcome::Applied;
        op.out = OpOut::Applied(applied);
        Ok(applied)
    }

    // ---- C-T6c2: the scripted-op surface the injected suite drives ---------

    /// A recorded `SELECT * FROM t WHERE pk = ..` (the observed row lands
    /// in the history; the script does not need it back) with explicit
    /// statement position 0 (the scripts name their own constants; `pos`
    /// only labels the hang diagnostic).
    pub fn op_get(&mut self, pk: i64) -> Result<(), String> {
        self.exec(0, OpKind::Get, |s, st, op| s.get(st, op, pk))
    }

    /// A recorded `SELECT * FROM t` (C-T6c1afix2 FU-3, closing c2 FU-C's
    /// letter): the statement runs the runner's real scan call-site —
    /// the same `scan_t` a workload `POp::ScanT` drives, every row
    /// rendered through the shared `record_t_row` path — so a script
    /// can pin the checker's scan arm without reshaping a Get after
    /// the fact.
    pub fn op_scan_t(&mut self) -> Result<(), String> {
        self.exec(0, OpKind::ScanT, |s, st, op| s.scan_t(st, op).map(|_| ()))
    }

    /// A recorded `INSERT INTO t VALUES (pk, ..)` with explicit values.
    pub fn op_insert_t(&mut self, pk: i64, a: Value, b: Option<Value>) -> Result<(), String> {
        self.exec(0, OpKind::InsertT, move |s, st, op| {
            s.insert_t(st, op, pk, a.clone(), b.clone()).map(|_| ())
        })
    }

    /// A recorded `UPDATE t SET col = const WHERE pk = ..` (`choice`
    /// picks the live non-pk column; `av`/`tv` the int8/text constant).
    pub fn op_update_set(
        &mut self,
        pk: i64,
        choice: u32,
        av: Value,
        tv: Value,
    ) -> Result<bool, String> {
        self.exec(0, OpKind::UpdateSet, move |s, st, op| {
            s.update_set(st, op, pk, choice, av.clone(), tv.clone())
        })
    }

    /// A recorded `ALTER TABLE t ADD COLUMN name int8 [DEFAULT d]` — the
    /// DDL session's own RC txn (the soak's DDL pattern), so the model
    /// learns the definition change.
    pub fn op_add_column(&mut self, name: &str, default: Option<i64>) -> Result<(), String> {
        self.exec(0, OpKind::AddColumn, |s, st, rec| {
            rec.detail = OpDetail::AddColumn {
                name: name.to_string(),
                default,
            };
            s.rig.db.add_column(
                st,
                s.rig.t_oid,
                name,
                KeyType::Int8,
                false,
                default.map(Value::Int8),
            )?;
            rec.out = OpOut::Ok;
            Ok(())
        })
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if !self.done {
            if let Some(txn) = self.txn.take() {
                let _ = self.rig.db.core.abort(txn);
            }
            self.finish(Fate::Failed("dropped".into()));
        }
    }
}

/// Builds the datum list of an insert over the handle's live columns:
/// `f(name)` decides each column's value (`None` return = NULL; an
/// unknown name — a column a DDL added — is NULL).
fn build_row(h: &TableHandle, f: impl Fn(&str) -> Option<Value>) -> RowVals {
    h.schema
        .live_columns()
        .map(|col| (col.attnum, f(&col.name)))
        .collect()
}

/// Resolves a program's column choice against the live non-pk columns of
/// `t` (the program cannot know which columns a concurrent DDL will have
/// dropped by the time it runs; the resolution is recorded).
fn resolve_col(h: &TableHandle, choice: u32) -> Option<u16> {
    let live: Vec<u16> = h
        .schema
        .live_columns()
        .filter(|c| !h.schema.pk.contains(&c.attnum))
        .map(|c| c.attnum)
        .collect();
    if live.is_empty() {
        None
    } else {
        Some(live[choice as usize % live.len()])
    }
}

fn col_is_text(h: &TableHandle, attnum: u16) -> bool {
    h.schema
        .live_columns()
        .any(|c| c.attnum == attnum && matches!(c.ty, KeyType::Text(_)))
}

/// A choice index into a list of `n` elements (`None` when empty).
fn pick_index(choice: u32, n: usize) -> Option<usize> {
    if n == 0 {
        None
    } else {
        Some(choice as usize % n)
    }
}

/// How an attempt ended (the g2 `Outcome`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Committed,
    Failed,
    Retry,
}

fn kind_of(pop: &POp) -> OpKind {
    match pop {
        POp::Get(_) => OpKind::Get,
        POp::ScanT => OpKind::ScanT,
        POp::InsertT(..) => OpKind::InsertT,
        POp::UpdateSet(..) => OpKind::UpdateSet,
        POp::DeleteT(_) => OpKind::DeleteT,
        POp::InsertU(_) => OpKind::InsertU,
        POp::ScanU => OpKind::ScanU,
        POp::DeleteU(_) => OpKind::DeleteU,
        POp::SetBig(..) => OpKind::SetBig,
        POp::NullBig(_) => OpKind::NullBig,
    }
}

/// Runs one attempt of a DML program.
fn run_prog(
    rig: &Rig,
    session: u16,
    prog_idx: u32,
    attempt: u32,
    prog: &Prog,
    seed: u64,
) -> Outcome {
    let mut s = Session::begin(rig, session, prog_idx, attempt, prog.level);
    for (pos, pop) in prog.ops.iter().enumerate() {
        let r = match *pop {
            POp::Get(pk) => s.exec(pos, kind_of(pop), |s, st, op| s.get(st, op, pk).map(|_| ())),
            POp::ScanT => s.exec(pos, kind_of(pop), |s, st, op| s.scan_t(st, op).map(|_| ())),
            POp::InsertT(pk, b_null) => {
                let a = Value::Int8(derive_const(
                    seed,
                    session,
                    prog_idx,
                    attempt,
                    pos,
                    super::TAG_T_A,
                ));
                let b = (!b_null).then(|| {
                    Value::Text(derive_text(
                        seed,
                        session,
                        prog_idx,
                        attempt,
                        pos,
                        super::TAG_T_B,
                    ))
                });
                s.exec(pos, kind_of(pop), move |s, st, op| {
                    s.insert_t(st, op, pk, a.clone(), b.clone()).map(|_| ())
                })
            }
            POp::UpdateSet(pk, choice) => {
                let av = Value::Int8(derive_const(
                    seed,
                    session,
                    prog_idx,
                    attempt,
                    pos,
                    super::TAG_T_SET,
                ));
                let tv = Value::Text(derive_text(
                    seed,
                    session,
                    prog_idx,
                    attempt,
                    pos,
                    super::TAG_T_SET,
                ));
                s.exec(pos, kind_of(pop), move |s, st, op| {
                    s.update_set(st, op, pk, choice, av.clone(), tv.clone())
                        .map(|_| ())
                })
            }
            POp::DeleteT(pk) => s.exec(pos, kind_of(pop), |s, st, op| {
                s.delete_t(st, op, pk).map(|_| ())
            }),
            POp::InsertU(b_null) => {
                let a = Value::Int8(derive_const(
                    seed,
                    session,
                    prog_idx,
                    attempt,
                    pos,
                    super::TAG_U_A,
                ));
                let b = (!b_null).then(|| {
                    Value::Text(derive_text(
                        seed,
                        session,
                        prog_idx,
                        attempt,
                        pos,
                        super::TAG_U_B,
                    ))
                });
                s.exec(pos, kind_of(pop), move |s, st, op| {
                    s.insert_u(st, op, a.clone(), b.clone()).map(|_| ())
                })
            }
            POp::ScanU => s.exec(pos, kind_of(pop), |s, st, op| s.scan_u(st, op).map(|_| ())),
            POp::DeleteU(choice) => s.exec(pos, kind_of(pop), |s, st, op| {
                s.delete_u(st, op, choice).map(|_| ())
            }),
            POp::SetBig(pk, choice) => {
                let tag =
                    derive_const(seed, session, prog_idx, attempt, pos, super::TAG_T_BIG) as u8;
                s.exec(pos, kind_of(pop), move |s, st, op| {
                    s.set_big(st, op, pk, choice, tag).map(|_| ())
                })
            }
            POp::NullBig(pk) => s.exec(pos, kind_of(pop), |s, st, op| {
                s.null_big(st, op, pk).map(|_| ())
            }),
        };
        if let Err(code) = r {
            return if super::retryable(&code) {
                Outcome::Retry
            } else {
                Outcome::Failed
            };
        }
    }
    match s.commit() {
        Ok(_) => Outcome::Committed,
        Err(code) if super::retryable(&code) => Outcome::Retry,
        Err(_) => Outcome::Failed,
    }
}

/// Runs one DDL program (its own RC txn).
fn run_ddl(rig: &Rig, index: u32, attempt: u32, op: &PDdl, seed: u64) -> Outcome {
    let kind = match op {
        PDdl::AddColumn(_) => OpKind::AddColumn,
        PDdl::DropColumn => OpKind::DropColumn,
        PDdl::SetNotNull => OpKind::SetNotNull,
        PDdl::DropNotNull => OpKind::DropNotNull,
        PDdl::Truncate => OpKind::Truncate,
        PDdl::DropBig => OpKind::DropBig,
        PDdl::AddBig => OpKind::AddBig,
    };
    let mut s = Session::begin(rig, DDL_SESSION, index, attempt, Level::ReadCommitted);
    let r = s.exec(0, kind, |s, st, rec| {
        run_ddl_op(s, st, rec, op, index, seed)
    });
    match r {
        Err(code) if super::retryable(&code) => Outcome::Retry,
        Err(_) => Outcome::Failed,
        Ok(()) => match s.commit() {
            Ok(_) => Outcome::Committed,
            Err(code) if super::retryable(&code) => Outcome::Retry,
            Err(_) => Outcome::Failed,
        },
    }
}

/// One DDL statement. Targets are resolved against the live definition
/// (read as the DDL itself would: the latest committed catalog through a
/// ghost reader) — DROP COLUMN prefers a column a DDL added (keeping `a`
/// and `b` live for the DML mix) and falls back to a seeded pick.
fn run_ddl_op(
    s: &Session<'_>,
    st: &Stmt<'_>,
    rec: &mut OpRec,
    op: &PDdl,
    index: u32,
    seed: u64,
) -> Result<(), nucleus_catalog::DbError> {
    let rig = s.rig;
    match *op {
        PDdl::AddColumn(with_default) => {
            let name = format!("c{index}");
            let default =
                with_default.then(|| derive_const(seed, DDL_SESSION, index, 0, 0, super::TAG_DDL));
            rec.detail = OpDetail::AddColumn {
                name: name.clone(),
                default,
            };
            rig.db.add_column(
                st,
                rig.t_oid,
                &name,
                KeyType::Int8,
                false,
                default.map(Value::Int8),
            )?;
            rec.out = OpOut::Ok;
            Ok(())
        }
        PDdl::DropColumn => {
            let schema = live_schema(rig)?;
            let mut rng = super::Rng::new(seed ^ 0xd101 ^ (u64::from(index) << 8));
            let attnum = drop_target(&schema, &mut rng).ok_or(
                nucleus_catalog::DbError::FeatureNotSupported("no live non-pk column to drop"),
            )?;
            rec.detail = OpDetail::DropColumn { attnum };
            rig.db.drop_column(st, rig.t_oid, attnum)?;
            rec.out = OpOut::Ok;
            Ok(())
        }
        PDdl::SetNotNull | PDdl::DropNotNull => {
            let set = matches!(op, PDdl::SetNotNull);
            let schema = live_schema(rig)?;
            let mut rng = super::Rng::new(seed ^ 0x5e17 ^ (u64::from(index) << 8));
            let candidates: Vec<u16> = schema
                .live_columns()
                .filter(|c| !schema.pk.contains(&c.attnum))
                .map(|c| c.attnum)
                .collect();
            let attnum = pick_index(rng.next() as u32, candidates.len())
                .map(|i| candidates[i])
                .ok_or(nucleus_catalog::DbError::UndefinedColumn)?;
            rec.detail = if set {
                OpDetail::SetNotNull { attnum }
            } else {
                OpDetail::DropNotNull { attnum }
            };
            rig.db.set_not_null(st, rig.t_oid, attnum, set)?;
            rec.out = OpOut::Ok;
            Ok(())
        }
        PDdl::Truncate => {
            let ddl = Ddl::new(&rig.db.core, st.txn(), st.ctx());
            let new_sid = rig.db.catalog.catalog_truncate_table(&ddl, rig.t_oid)?;
            rec.detail = OpDetail::Truncate { new_sid };
            rec.out = OpOut::Ok;
            Ok(())
        }
        // C-T6d2: DROP COLUMN big by name / its re-add. Both reuse the
        // AddColumn/DropColumn details (the model evolves through them),
        // the drop resolving the live attnum at execution.
        PDdl::DropBig => {
            let schema = live_schema(rig)?;
            let attnum = schema
                .live_columns()
                .find(|c| c.name == "big")
                .map(|c| c.attnum)
                .ok_or(nucleus_catalog::DbError::FeatureNotSupported(
                    "no live big column to drop",
                ))?;
            rec.detail = OpDetail::DropColumn { attnum };
            rig.db.drop_column(st, rig.t_oid, attnum)?;
            rec.out = OpOut::Ok;
            Ok(())
        }
        PDdl::AddBig => {
            let name = "big".to_string();
            rec.detail = OpDetail::AddColumn {
                name: name.clone(),
                default: None,
            };
            rig.db.add_column(
                st,
                rig.t_oid,
                &name,
                KeyType::Text(Collation::C),
                false,
                None,
            )?;
            rec.out = OpOut::Ok;
            Ok(())
        }
    }
}

/// The live definition of `t` as a DDL statement reads it (the latest
/// committed catalog, ghost reader: the DDL session is the only DDL
/// writer, so this is exactly what its AccessExclusive would see).
fn live_schema(rig: &Rig) -> Result<TableSchema, nucleus_catalog::DbError> {
    let ghost = ReadCtx {
        txn: nucleus_txn::TxnId {
            epoch: rig.db.core.epoch(),
            n: 0,
        },
        snapshot: rig.db.core.visible_ts(),
        stmt_seq: 0,
    };
    let view = rig.db.core.open_view();
    let rel = Catalog::read_rel(&rig.db.core, &view, &ghost, rig.t_oid)
        .map_err(nucleus_catalog::DbError::Txn)?
        .ok_or(nucleus_catalog::DbError::UndefinedTable)?;
    Ok(TableSchema::decode(&rel.def)?)
}

/// DROP COLUMN's target: a live **added** column (attnum ≥ 5 — `big` is
/// the fixture's attnum 4 and C-T6d2's DropBig owns it) — the highest, so
/// the workload's own `a`, `b` and `big` stay live and the DML mix keeps
/// exercising them. With none, the op is a legal 0A000 refusal (the card
/// names the target class "non-pk live"; which one the harness picks is
/// its own choice, and dropping only added columns keeps the run's write
/// surface from collapsing to the fixture's columns).
fn drop_target(schema: &TableSchema, _rng: &mut super::Rng) -> Option<u16> {
    schema
        .live_columns()
        .filter(|c| !schema.pk.contains(&c.attnum) && c.attnum >= 5)
        .map(|c| c.attnum)
        .last()
}

/// Runs one amcheck point (both tables) at a fresh registered snapshot.
fn amcheck_point(rig: &Rig, ddl_index: u32) {
    for (table, oid) in [("t", rig.t_oid), ("u", rig.u_oid)] {
        let guard = rig.db.core.registry.take_snapshot();
        let report = match check_table(&rig.db, oid, &guard) {
            Ok(r) => r,
            Err(e) => {
                let mut r = Report::default();
                r.violations.push(format!("check_table returned {e:?}"));
                r
            }
        };
        drop(guard);
        rig.amcheck.lock().expect("amcheck slot").push(AmcheckRec {
            ddl_index,
            table,
            report,
        });
    }
}

/// Runs the seeded workload on real threads over a fresh rig and returns
/// the drained run. The size is an iteration count (programs per
/// session), never a wall-clock bound; the watchdog only cuts short a run
/// where no attempt at all finishes for a very long stretch (a lost
/// wakeup), with a diagnostic — a hang is a failure, not a pass.
pub fn run_workload(cfg: &Config) -> Run {
    let mut rig = Rig::new(cfg.deadlock_ms);
    rig.setup();
    let rig = &rig;
    let progress = AtomicU64::new(0);
    let finished = AtomicBool::new(false);
    // The amcheck points, at quarters of the DDL program.
    let amcheck_at: Vec<u32> = [1u32, 2, 3].iter().map(|q| q * cfg.ddl_txns / 4).collect();
    std::thread::scope(|sc| {
        let progress = &progress;
        let finished = &finished;
        sc.spawn(move || {
            let mut last = u64::MAX;
            let mut stalled = 0u32;
            while !finished.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(100));
                let now = progress.load(Ordering::SeqCst);
                if now == last {
                    stalled += 1;
                    if stalled >= 600 {
                        eprintln!(
                            "t6c: no attempt finished for 60 s: the run hung (seed {:#x})\n{}",
                            cfg.seed,
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
        let dml: Vec<_> = (1..=cfg.threads)
            .map(|session| {
                sc.spawn(move || {
                    for idx in 0..cfg.progs {
                        let prog = gen_prog(cfg.seed, session, idx);
                        for attempt in 0..cfg.max_attempts {
                            let out = run_prog(rig, session, idx, attempt, &prog, cfg.seed);
                            progress.fetch_add(1, Ordering::SeqCst);
                            if out != Outcome::Retry {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        let ddl = sc.spawn(move || {
            for idx in 0..cfg.ddl_txns {
                let op = gen_ddl(cfg.seed, idx);
                for attempt in 0..cfg.max_attempts {
                    let out = run_ddl(rig, idx, attempt, &op, cfg.seed);
                    progress.fetch_add(1, Ordering::SeqCst);
                    if out != Outcome::Retry {
                        break;
                    }
                    std::thread::yield_now();
                }
                if amcheck_at.contains(&idx) {
                    amcheck_point(rig, idx);
                }
            }
        });
        for h in dml {
            h.join().expect("a DML thread panicked");
        }
        ddl.join().expect("the DDL thread panicked");
        finished.store(true, Ordering::SeqCst);
    });
    rig.finish()
}
