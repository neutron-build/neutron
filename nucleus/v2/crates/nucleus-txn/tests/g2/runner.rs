//! G2 runner: sessions over one shared `Core<MemKv>`. Every operation goes
//! through the public API of `nucleus-txn` (`Core::begin`, the SSI read
//! paths, `row_op` / `insert_key` / `insert_on_conflict` / `fk_check_*`,
//! `commit` and `abort`, `rollback_to`) with the real lock manager, the real
//! parker and the real commit, resolver and retention threads.
//!
//! A [`Session`] is one transaction attempt: it takes the snapshots its
//! isolation level calls for, runs statements, and records everything the
//! statements observed into a [`TxnRec`] pushed to the shared [`Recorder`]
//! when the txn ends. The soak drives sessions from seeded programs on N
//! threads; the injection suite drives them by hand to arrange interleavings
//! without any engine edit.
//!
//! **Determinism.** A program is a pure function of `(seed, session,
//! program index)`; the checker depends only on the recorded history, never
//! on scheduling. Thread interleavings (and so which attempts hit 40001) vary
//! between runs of one seed; the programs and their first-attempt op streams
//! do not.
//!
//! **Harness-level bug injection** ([`Bug`]): ways a *store* could be broken,
//! arranged entirely in the harness (the engine is never edited): sessions
//! that take a fresh snapshot per statement while claiming REPEATABLE READ,
//! that read straight from the intents (read uncommitted), or that apply each
//! write in its own autocommitted txn.

use std::cell::Cell;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread_with, CommitConfig, CommitThreadHandle, SyncCommit};
use nucleus_txn::encoding::{decode_intent, intent_key, parse_key, Entry, VersionValue};
use nucleus_txn::locks::LockManager;
use nucleus_txn::read::{read_key, scan, NoSsi};
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::resolver::{spawn_background, BackgroundHandle, Resolver};
use nucleus_txn::ssi::{spawn_retention, RetentionHandle, Ssi};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, FkParentMode, OnConflictAction, ProposedRow, RowOp,
    RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::{LayerData, Ts, TxnError};

use std::sync::Arc;

use super::history::{
    is_parent, parent_of, Fate, History, Level, Obs, OpKind, OpRec, Recorder, SqlErr, TxnRec, Val,
    Write, CHILD0, PARENT0, PLAIN,
};

// ----- keys and values --------------------------------------------------------

/// The engine key of slot `k`.
pub fn key_bytes(k: u8) -> Vec<u8> {
    if k < PLAIN {
        format!("/t/1/k{k:02}").into_bytes()
    } else if is_parent(k) {
        format!("/t/p/{}", k - PARENT0).into_bytes()
    } else {
        format!("/t/c/{}/0", k - CHILD0).into_bytes()
    }
}

fn child_lo(parent: u8) -> Vec<u8> {
    format!("/t/c/{}/", parent - PARENT0).into_bytes()
}

fn child_hi(parent: u8) -> Vec<u8> {
    // '/' + 1 == '0': every `/t/c/{i}/...` sorts below `/t/c/{i}0`.
    format!("/t/c/{}0", parent - PARENT0).into_bytes()
}

fn slot_of(key: &[u8]) -> Option<u8> {
    (0..super::history::SLOTS).find(|&k| key_bytes(k) == key)
}

fn enc(v: Val) -> Vec<u8> {
    v.0.to_be_bytes().to_vec()
}

/// A payload that is not a value of ours decodes to a value nobody wrote, so
/// the checker reports it as garbage rather than the harness panicking.
fn dec(b: &[u8]) -> Val {
    match <[u8; 8]>::try_from(b) {
        Ok(a) => Val(u64::from_be_bytes(a)),
        Err(_) => Val(u64::MAX),
    }
}

fn iso(l: Level) -> Isolation {
    match l {
        Level::ReadCommitted => Isolation::ReadCommitted,
        Level::RepeatableRead => Isolation::RepeatableRead,
        Level::Serializable => Isolation::Serializable,
    }
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

// ----- the rig -------------------------------------------------------------------

/// The shared engine: one core with the lock manager and SSI installed, the
/// commit thread (with SSI's writer-map observer), the background resolver
/// and the SSI retention thread, plus the recorder.
pub struct Rig {
    pub core: Arc<Core<MemKv>>,
    pub ssi: Arc<Ssi>,
    pub rec: Recorder,
    /// What each session thread is doing now (the hang diagnostic).
    status: Mutex<std::collections::BTreeMap<u16, String>>,
    commit: Mutex<Option<CommitThreadHandle>>,
    resolver: Mutex<Option<BackgroundHandle>>,
    retention: Mutex<Option<RetentionHandle>>,
}

/// What a finished rig hands back.
pub struct Run {
    pub history: History,
    /// `@INTENT` entries left in the KV once everything drained (I-LEAK).
    pub leaked_intents: usize,
    /// Who owns each leaked intent and what became of the owner.
    pub leak_report: String,
}

impl Rig {
    /// A fresh core. `deadlock_ms` is the `deadlock_timeout` (a setting, not
    /// an assertion: cycles are broken after it).
    pub fn new(deadlock_ms: u64) -> Rig {
        let core = Arc::new(Core::open(MemKv::new()).expect("open the core"));
        let _locks = LockManager::install(&core);
        let ssi = Ssi::install(&core);
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
            core,
            ssi,
            rec: Recorder::new(),
            status: Mutex::new(std::collections::BTreeMap::new()),
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

    /// The hang diagnostic: every session's last note and the wait-for graph.
    fn dump_state(&self) -> String {
        let mut out = String::new();
        if let Ok(m) = self.status.lock() {
            for (s, what) in m.iter() {
                out.push_str(&format!("  session {s}: {what}\n"));
            }
        }
        let view = self.core.open_view();
        for k in 0..super::history::SLOTS {
            let kb = key_bytes(k);
            let hi = nucleus_txn::encoding::end_key(&kb);
            let mut line = String::new();
            for row in view.scan((Bound::Included(&kb[..]), Bound::Excluded(&hi[..])), false) {
                let Ok((stored, value)) = row else { continue };
                match parse_key(&stored) {
                    Some((l, Entry::Intent)) if l == kb.as_slice() => {
                        if let Ok(i) = decode_intent(&value) {
                            let st = self.core.status.entry(i.txn).map(|e| e.status);
                            line.push_str(&format!(
                                " intent{{{:?} {st:?} layers {:?}}}",
                                i.txn.n,
                                i.layers
                                    .iter()
                                    .map(|l| (l.seq, l.lock, matches!(l.data, LayerData::Absent)))
                                    .collect::<Vec<_>>()
                            ));
                        }
                    }
                    Some((l, Entry::Version(ts))) if l == kb.as_slice() => {
                        line.push_str(&format!(" v@{}", ts.0));
                    }
                    _ => {}
                }
            }
            out.push_str(&format!("  kv {}:{line}\n", super::history::key_name(k)));
        }
        drop(view);
        out.push_str(&format!("  wait edges: {:?}\n", self.core.wait_edges()));
        out.push_str(&format!("  wait slots: {:?}\n", self.core.wait_slots()));
        out
    }

    /// A session at `level`, bug-free.
    pub fn session(&self, id: u16, level: Level) -> Session<'_> {
        Session::begin(self, id, 0, 0, level, Bug::None)
    }

    pub fn session_with(&self, id: u16, level: Level, bug: Bug) -> Session<'_> {
        Session::begin(self, id, 0, 0, level, bug)
    }

    /// The initial state, committed (and recorded) by txn 1: the even plain
    /// rows and both parents exist.
    pub fn setup(&self) {
        let mut s = self.session(0, Level::ReadCommitted);
        for k in (0..PLAIN).step_by(2).chain(PARENT0..PARENT0 + 2) {
            s.insert(k).expect("setup insert");
        }
        s.commit().expect("setup commit");
    }

    /// Stops the background threads, drains resolution, scans for leaked
    /// intents and hands the history back.
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
        while Resolver::run_once(&self.core).expect("resolve") > 0 {}
        let view = self.core.open_view();
        let mut leaked = 0;
        let mut report = String::new();
        for row in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
            let (k, v) = row.expect("scan the kv");
            // Only row keys: a `/sys/..` key whose last byte happens to be
            // 0x00 parses as an intent without a schema.
            if let Some((l, Entry::Intent)) = parse_key(&k).filter(|(l, _)| l.starts_with(b"/t/")) {
                leaked += 1;
                let slot = slot_of(l).map_or("?".to_string(), super::history::key_name);
                let owner = decode_intent(&v).map(|i| {
                    let st = self.core.status.entry(i.txn);
                    format!("owner {:?} status {:?} layers {:?}", i.txn, st, i.layers)
                });
                report.push_str(&format!("  leaked intent on {slot}: {owner:?}\n"));
            }
        }
        drop(view);
        Run {
            history: self.rec.drain(),
            leaked_intents: leaked,
            leak_report: report,
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

/// A harness-level defect injected into a session (the "broken store").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    None,
    /// A REPEATABLE READ session that takes a fresh snapshot per statement
    /// (RC behaviour under an RR claim).
    PerStmtSnapshot,
    /// Plain reads peek at the newest intent, committed or not.
    DirtyReads,
    /// Every write statement commits in its own txn at once.
    Autocommit,
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
    last_commit_ts: Cell<u64>,
    done: bool,
}

/// The EPQ callback for a statement that re-applies its own op to the newest
/// version, remembering which version it evaluated.
struct Requal {
    op: RowOp,
    seen: Option<(Ts, Obs)>,
}

impl Epq for Requal {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        match &newest.value {
            VersionValue::Live { payload, .. } => {
                self.seen = Some((newest.ts, Obs::Val(dec(payload))));
                EpqDecision::Apply(self.op.clone())
            }
            VersionValue::Tombstone { .. } => {
                self.seen = Some((newest.ts, Obs::Absent));
                EpqDecision::Skip
            }
        }
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
        let (txn, guard) = if bug == Bug::Autocommit {
            (None, None)
        } else {
            let txn = rig.core.begin(iso(level));
            let guard = match level {
                Level::ReadCommitted => None,
                Level::RepeatableRead => Some(rig.core.registry.take_snapshot()),
                Level::Serializable => Some(
                    rig.ssi
                        .begin(&rig.core, &txn, false)
                        .expect("ssi begin on a fresh SERIALIZABLE txn"),
                ),
            };
            (Some(txn), guard)
        };
        let guard_ts = guard.as_ref().map(|g| g.ts().0);
        Session {
            rig,
            level,
            bug,
            txn,
            guard,
            rec: TxnRec {
                uid,
                session,
                prog,
                attempt,
                level,
                snap: guard_ts,
                start,
                end: start,
                fate: Fate::Rolledback,
                ops: Vec::new(),
            },
            next_val: Cell::new(0),
            last_commit_ts: Cell::new(0),
            done: false,
        }
    }

    pub fn uid(&self) -> u32 {
        self.rec.uid
    }

    /// The engine id of the txn (None for an autocommit session).
    pub fn txn_id(&self) -> Option<nucleus_txn::TxnId> {
        self.txn.as_ref().map(|t| t.id)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    fn val(&self) -> Val {
        self.next_val.set(self.next_val.get() + 1);
        Val::new(self.rec.uid, self.next_val.get())
    }

    fn ser(&self) -> bool {
        self.level == Level::Serializable && self.bug != Bug::Autocommit
    }

    /// Finishes the record with `fate`, releases the snapshot and pushes the
    /// record.
    fn finish(&mut self, fate: Fate) {
        self.done = true;
        self.rec.end = self.rig.rec.tick();
        self.rec.fate = fate;
        self.guard = None;
        self.rig.rec.push(self.rec.clone());
    }

    fn fail(&mut self, e: SqlErr) {
        if let Some(txn) = self.txn.take() {
            self.rig.core.abort(txn).expect("abort a failed txn");
        }
        self.finish(Fate::Failed(e));
    }

    pub fn commit(mut self) -> Result<(), SqlErr> {
        let Some(txn) = self.txn.take() else {
            // Autocommit: every write already committed on its own.
            let ts = self.last_commit_ts.get();
            self.finish(Fate::Committed(ts));
            return Ok(());
        };
        match self.rig.core.commit(txn, SyncCommit::On) {
            Ok(ts) => {
                self.finish(Fate::Committed(ts.0));
                Ok(())
            }
            Err(e) => {
                // `commit_submit` aborts on a pre-commit failure itself.
                let e = map_err(e);
                self.finish(Fate::Failed(e.clone()));
                Err(e)
            }
        }
    }

    pub fn rollback(mut self) {
        if let Some(txn) = self.txn.take() {
            self.rig.core.abort(txn).expect("abort");
        }
        self.finish(Fate::Rolledback);
    }

    /// Runs one statement: opens the op record, runs `body` (which fills in
    /// the reads, snapshot and writes as it goes), closes and files the
    /// record. An error aborts the txn.
    fn exec<T>(
        &mut self,
        kind: OpKind,
        keys: &[u8],
        body: impl FnOnce(&Session<'r>, &mut OpRec) -> Result<T, TxnError>,
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
        let r = body(self, &mut op);
        op.end = self.rig.rec.tick();
        match r {
            Ok(v) => {
                self.rec.ops.push(op);
                Ok(v)
            }
            Err(e) => {
                let e = map_err(e);
                op.err = Some(e.clone());
                self.rec.ops.push(op);
                self.fail(e.clone());
                Err(e)
            }
        }
    }

    /// The engine txn and snapshot a read statement uses.
    fn read_env<R>(
        &self,
        f: impl FnOnce(&Txn, bool, Ts, u32) -> Result<R, TxnError>,
    ) -> Result<R, TxnError> {
        let Some(txn) = self.txn.as_ref() else {
            // Autocommit sessions read in a throwaway RC txn.
            let t = self.rig.core.begin(Isolation::ReadCommitted);
            let g = self.rig.core.registry.take_snapshot();
            let seq = t.next_seq()?;
            let r = f(&t, false, g.ts(), seq);
            drop(g);
            self.rig.core.abort(t)?;
            return r;
        };
        if self.ser() {
            self.rig.ssi.check_doomed(txn.id)?;
        }
        let fresh = self.level == Level::ReadCommitted
            || (self.level == Level::RepeatableRead && self.bug == Bug::PerStmtSnapshot);
        let seq = txn.next_seq()?;
        if fresh {
            let g = self.rig.core.registry.take_snapshot();
            f(txn, self.ser(), g.ts(), seq)
        } else {
            let s = self.guard.as_ref().map_or(Ts(0), SnapshotGuard::ts);
            f(txn, self.ser(), s, seq)
        }
    }

    /// The engine txn and snapshot a write statement uses; an autocommit
    /// session runs it in its own RC txn and commits at once.
    fn write_env<R>(
        &self,
        f: impl FnOnce(&Txn, bool, Ts, u32) -> Result<R, TxnError>,
    ) -> Result<R, TxnError> {
        if self.bug != Bug::Autocommit {
            return self.read_env(f);
        }
        let t = self.rig.core.begin(Isolation::ReadCommitted);
        let g = self.rig.core.registry.take_snapshot();
        let seq = t.next_seq()?;
        let r = f(&t, false, g.ts(), seq);
        drop(g);
        match r {
            Ok(v) => {
                let ts = self.rig.core.commit(t, SyncCommit::On)?;
                self.last_commit_ts.set(ts.0);
                Ok(v)
            }
            Err(e) => {
                self.rig.core.abort(t)?;
                Err(e)
            }
        }
    }

    fn point_read(
        &self,
        txn: &Txn,
        ser: bool,
        s: Ts,
        seq: u32,
        key: u8,
    ) -> Result<Option<Val>, TxnError> {
        let kb = key_bytes(key);
        let ctx = ReadCtx {
            txn: txn.id,
            snapshot: s,
            stmt_seq: seq,
        };
        let v = if ser {
            self.rig.ssi.read_key(&self.rig.core, txn.id, &kb, &ctx)?
        } else {
            let view = self.rig.core.open_view();
            read_key(&self.rig.core, &view, &kb, &ctx, &mut NoSsi)?
        };
        Ok(v.as_deref().map(dec))
    }

    // ----- statements ---------------------------------------------------------

    /// `SELECT v FROM t WHERE k = key`.
    pub fn read(&mut self, key: u8) -> Result<Option<Val>, SqlErr> {
        self.exec(OpKind::Read, &[key], |me, op| {
            if me.bug == Bug::DirtyReads {
                let v = me.dirty_peek(key)?;
                op.reads.push((key, v.map_or(Obs::Absent, Obs::Val)));
                return Ok(v);
            }
            me.read_env(|txn, ser, s, seq| {
                op.snap = Some(s.0);
                let v = me.point_read(txn, ser, s, seq, key)?;
                op.reads.push((key, v.map_or(Obs::Absent, Obs::Val)));
                Ok(v)
            })
        })
    }

    /// Reads the newest intent's data if there is one (a read uncommitted
    /// store), else the committed value.
    fn dirty_peek(&self, key: u8) -> Result<Option<Val>, TxnError> {
        let kb = key_bytes(key);
        {
            let _latch = self.rig.core.latches.lock(&kb);
            if let Some(raw) = self.rig.core.latest_get(&intent_key(&kb))? {
                let intent = decode_intent(&raw)?;
                match intent.top().map(|l| &l.data) {
                    Some(LayerData::Write { value, .. }) => return Ok(Some(dec(value))),
                    Some(LayerData::Delete { .. }) => return Ok(None),
                    _ => {}
                }
            }
        }
        self.read_env(|txn, ser, s, seq| self.point_read(txn, ser, s, seq, key))
    }

    /// `SELECT ... WHERE k >= lo AND k < hi` over the plain rows.
    pub fn scan(&mut self, lo: u8, hi: u8) -> Result<Vec<(u8, Val)>, SqlErr> {
        self.exec(OpKind::Scan, &[lo, hi], |me, op| {
            me.read_env(|txn, ser, s, seq| {
                op.snap = Some(s.0);
                let (lob, hib) = (key_bytes(lo), format!("/t/1/k{hi:02}").into_bytes());
                let ctx = ReadCtx {
                    txn: txn.id,
                    snapshot: s,
                    stmt_seq: seq,
                };
                let rows: Vec<(Vec<u8>, Vec<u8>)> = if ser {
                    me.rig.ssi.scan(&me.rig.core, txn.id, &lob, &hib, &ctx)?
                } else {
                    let view = me.rig.core.open_view();
                    scan(
                        &me.rig.core,
                        &view,
                        (Bound::Included(&lob[..]), Bound::Excluded(&hib[..])),
                        &ctx,
                        &mut NoSsi,
                    )
                    .collect::<Result<Vec<_>, _>>()?
                };
                let live: Vec<(u8, Val)> = rows
                    .iter()
                    .filter_map(|(k, v)| slot_of(k).map(|s| (s, dec(v))))
                    .collect();
                for k in lo..hi {
                    let o = live
                        .iter()
                        .find(|(s, _)| *s == k)
                        .map_or(Obs::Absent, |(_, v)| Obs::Val(*v));
                    op.reads.push((k, o));
                }
                Ok(live)
            })
        })
    }

    /// `INSERT INTO t VALUES (key, ...)`.
    pub fn insert(&mut self, key: u8) -> Result<(), SqlErr> {
        let v = self.val();
        self.exec(OpKind::Insert, &[key], |me, op| {
            me.write_env(|txn, _ser, s, seq| {
                me.rig.core.insert_key(
                    txn,
                    &key_bytes(key),
                    None,
                    enc(v),
                    StmtCtx::new(s, seq, seq),
                    UniqueRule::Unique { same_row: None },
                )
            })?;
            op.writes.push((key, Write::Put(v)));
            Ok(())
        })
    }

    /// `UPDATE t SET v = .. WHERE k = key`. Returns whether a row matched.
    pub fn update(&mut self, key: u8) -> Result<bool, SqlErr> {
        let v = self.val();
        let op_kind = RowOp::Update {
            value: enc(v),
            key_cols_changed: false,
        };
        self.exec(OpKind::Update, &[key], |me, op| {
            let rd = Cell::new(None);
            let r = me.write_env(|txn, ser, s, seq| me.modify(txn, ser, s, seq, key, op_kind, &rd));
            me.note_read(op, key, &rd);
            let applied = r?;
            if applied {
                op.writes.push((key, Write::Put(v)));
            }
            Ok(applied)
        })
    }

    /// `DELETE FROM t WHERE k = key`; a parent row is checked against its
    /// children (RESTRICT). Returns whether a row matched.
    pub fn delete(&mut self, key: u8) -> Result<bool, SqlErr> {
        self.exec(OpKind::Delete, &[key], |me, op| {
            let rd = Cell::new(None);
            let r = me.write_env(|txn, ser, s, seq| {
                let applied = me.modify(txn, ser, s, seq, key, RowOp::Delete, &rd)?;
                if applied && is_parent(key) {
                    let fk_seq = txn.next_seq()?;
                    me.rig.core.fk_check_parent(
                        txn,
                        &StmtCtx::new(s, seq, fk_seq).internal(),
                        &child_lo(key),
                        &child_hi(key),
                        FkParentMode::Restrict,
                    )?;
                }
                Ok(applied)
            });
            me.note_read(op, key, &rd);
            let applied = r?;
            if applied {
                op.writes.push((key, Write::Del));
            }
            Ok(applied)
        })
    }

    /// An UPDATE inside a savepoint that is rolled back: the read counts, the
    /// write never happened.
    pub fn update_rolled_back(&mut self, key: u8) -> Result<bool, SqlErr> {
        let v = self.val();
        let op_kind = RowOp::Update {
            value: enc(v),
            key_cols_changed: false,
        };
        self.exec(OpKind::SpUpdate, &[key], |me, op| {
            let rd = Cell::new(None);
            let r = me.write_env(|txn, ser, s, _seq| {
                // The savepoint comes before the statement's own seq, or the
                // statement's layer would sit below it and survive the
                // rollback.
                let sp = txn.savepoint()?;
                let seq = txn.next_seq()?;
                let applied = me.modify(txn, ser, s, seq, key, op_kind, &rd)?;
                me.rig.core.rollback_to(txn, sp)?;
                Ok(applied)
            });
            me.note_read(op, key, &rd);
            op.void = true;
            let applied = r?;
            if applied {
                op.writes.push((key, Write::Put(v)));
            }
            Ok(applied)
        })
    }

    fn note_read(&self, op: &mut OpRec, key: u8, rd: &Cell<Option<(Obs, Ts)>>) {
        if let Some((o, ts)) = rd.get() {
            op.snap = Some(ts.0);
            op.reads.push((key, o));
        }
    }

    /// Statement-level UPDATE/DELETE: read the row at the statement's
    /// snapshot, then the row op (RC re-evaluates through EPQ against the
    /// newest version; RR/SER raise 40001 on a newer one). `rd` receives the
    /// observation the write replaced: the row seen at the snapshot, or the
    /// version EPQ re-evaluated (with that version's ts as its snapshot).
    #[allow(clippy::too_many_arguments)]
    fn modify(
        &self,
        txn: &Txn,
        ser: bool,
        s: Ts,
        seq: u32,
        key: u8,
        op: RowOp,
        rd: &Cell<Option<(Obs, Ts)>>,
    ) -> Result<bool, TxnError> {
        let Some(old) = self.point_read(txn, ser, s, seq, key)? else {
            rd.set(Some((Obs::Absent, s)));
            return Ok(false);
        };
        rd.set(Some((Obs::Val(old), s)));
        let mut epq = Requal {
            op: op.clone(),
            seen: None,
        };
        let out = self.rig.core.row_op(
            txn,
            &key_bytes(key),
            None,
            op,
            StmtCtx::new(s, seq, seq),
            &mut epq,
        );
        if let Some((ts, o)) = epq.seen {
            rd.set(Some((o, ts)));
        }
        Ok(out? == RowOutcome::Applied)
    }

    /// `INSERT ... ON CONFLICT (pk) DO UPDATE SET v = ..`.
    pub fn upsert(&mut self, key: u8) -> Result<(), SqlErr> {
        let v = self.val();
        self.exec(OpKind::Upsert, &[key], |me, op| {
            let saw: Cell<Option<Val>> = Cell::new(None);
            let res = me.write_env(|txn, _ser, s, seq| {
                let row = ProposedRow {
                    t_key: key_bytes(key),
                    value: enc(v),
                    entries: Vec::new(),
                    pk_arbiter: true,
                };
                let mut f = |locked: &[u8]| {
                    saw.set(Some(dec(locked)));
                    Some((enc(v), false))
                };
                me.rig.core.insert_on_conflict(
                    txn,
                    StmtCtx::new(s, seq, seq).revisit_is_error(),
                    row,
                    &|p: &[u8]| p.to_vec(),
                    &mut |_| {},
                    OnConflictAction::DoUpdate(&mut f),
                )
            });
            if let Some(sv) = saw.get() {
                op.reads.push((key, Obs::Val(sv)));
            }
            res?;
            op.writes.push((key, Write::Put(v)));
            Ok(())
        })
    }

    /// Inserts a child row and checks its parent (the FK trigger).
    pub fn fk_insert(&mut self, child: u8) -> Result<(), SqlErr> {
        let parent = parent_of(child);
        let v = self.val();
        self.exec(OpKind::FkInsert, &[child, parent], |me, op| {
            let seen: Cell<Option<Val>> = Cell::new(None);
            let snap: Cell<Option<Ts>> = Cell::new(None);
            let calls: Cell<u32> = Cell::new(0);
            let r = me.write_env(|txn, ser, s, seq| {
                me.rig.core.insert_key(
                    txn,
                    &key_bytes(child),
                    None,
                    enc(v),
                    StmtCtx::new(s, seq, seq),
                    UniqueRule::Unique { same_row: None },
                )?;
                // RC reads the parent with a fresh snapshot, RR/SER at S.
                let guard;
                let s_fk = if ser || me.level != Level::ReadCommitted {
                    s
                } else {
                    guard = me.rig.core.registry.take_snapshot();
                    guard.ts()
                };
                snap.set(Some(s_fk));
                let fk_seq = txn.next_seq()?;
                let m = |p: &[u8]| {
                    seen.set(Some(dec(p)));
                    calls.set(calls.get() + 1);
                    true
                };
                me.rig.core.fk_check_child(
                    txn,
                    &StmtCtx::new(s_fk, seq, fk_seq).internal(),
                    &key_bytes(parent),
                    &m,
                )
            });
            if let Some(sn) = snap.get() {
                // A second evaluation is the EPQ re-check against a version
                // newer than the snapshot: its ts is not known here.
                op.snap = (calls.get() <= 1).then_some(sn.0);
                op.reads
                    .push((parent, seen.get().map_or(Obs::Absent, Obs::Val)));
            }
            r?;
            op.writes.push((child, Write::Put(v)));
            Ok(())
        })
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if !self.done {
            if let Some(txn) = self.txn.take() {
                let _ = self.rig.core.abort(txn);
            }
            self.finish(Fate::Rolledback);
        }
    }
}

// ----- seeded programs ----------------------------------------------------------

/// xorshift64*, seeded through splitmix64 so adjacent seeds differ.
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
    Insert(u8),
    Update(u8),
    Delete(u8),
    Upsert(u8),
    FkInsert(u8),
    SpUpdate(u8),
}

/// A transaction program: fixed statements and a fixed ending, independent
/// of what the statements observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prog {
    pub ops: Vec<POp>,
    pub rollback: bool,
}

/// Statement mix weights (percent-like, relative).
#[derive(Debug, Clone, Copy)]
pub struct Mix {
    pub read: u64,
    /// Read then update the same key in two statements.
    pub rmw: u64,
    pub update: u64,
    pub scan: u64,
    pub insert: u64,
    pub delete: u64,
    pub upsert: u64,
    pub fk_insert: u64,
    pub sp_update: u64,
    /// Whether write statements may target the FK parent rows. Off, parents
    /// are only read (by statements and by the FK check of child inserts).
    pub parent_writes: bool,
}

impl Mix {
    pub const STANDARD: Mix = Mix {
        read: 22,
        rmw: 14,
        update: 14,
        scan: 8,
        insert: 8,
        delete: 8,
        upsert: 10,
        fk_insert: 8,
        sp_update: 4,
        parent_writes: true,
    };

    /// The SERIALIZABLE soak's mix: the FK parents are never written. See the
    /// ESCALATE note in `soak`: the FK check of a child insert drops the
    /// reader-side rw edge to a concurrent parent writer.
    pub const SER: Mix = Mix {
        parent_writes: false,
        ..Mix::STANDARD
    };
}

/// The program of `(seed, session, index)`.
pub fn gen_prog(seed: u64, session: u16, index: u32, mix: &Mix) -> Prog {
    let mut rng = Rng::new(seed ^ (u64::from(session) << 40) ^ (u64::from(index) << 8) ^ 0x6732);
    let total = mix.read
        + mix.rmw
        + mix.update
        + mix.scan
        + mix.insert
        + mix.delete
        + mix.upsert
        + mix.fk_insert
        + mix.sp_update;
    let n = 1 + rng.below(4) as usize;
    let mut ops = Vec::new();
    // A plain row most of the time; sometimes a parent (rows other
    // statements may FK-reference).
    let writes_parents = mix.parent_writes;
    let row = |rng: &mut Rng, child_ok: bool, write: bool| -> u8 {
        match rng.below(12) {
            0 if writes_parents || !write => PARENT0 + rng.below(2) as u8,
            1 if child_ok => CHILD0 + rng.below(2) as u8,
            _ => rng.below(u64::from(PLAIN)) as u8,
        }
    };
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
        if pick(mix.read) {
            ops.push(POp::Read(row(&mut rng, true, false)));
        } else if pick(mix.rmw) {
            let k = row(&mut rng, true, true);
            ops.push(POp::Read(k));
            ops.push(POp::Update(k));
        } else if pick(mix.update) {
            ops.push(POp::Update(row(&mut rng, true, true)));
        } else if pick(mix.scan) {
            let lo = rng.below(u64::from(PLAIN) - 1) as u8;
            let hi = (lo + 1 + rng.below(6) as u8).min(PLAIN);
            ops.push(POp::Scan(lo, hi));
        } else if pick(mix.insert) {
            ops.push(POp::Insert(row(&mut rng, false, true)));
        } else if pick(mix.delete) {
            ops.push(POp::Delete(row(&mut rng, true, true)));
        } else if pick(mix.upsert) {
            // An upsert only ever runs in a txn that holds nothing yet (the
            // first write statement): see the ESCALATE note in `soak`, the
            // engine livelocks two upserts that wait on each other's rows.
            let k = row(&mut rng, false, true);
            let wrote = ops
                .iter()
                .any(|o| !matches!(o, POp::Read(_) | POp::Scan(..)));
            ops.push(if wrote {
                POp::Update(k)
            } else {
                POp::Upsert(k)
            });
        } else if pick(mix.fk_insert) {
            ops.push(POp::FkInsert(CHILD0 + rng.below(2) as u8));
        } else {
            ops.push(POp::SpUpdate(row(&mut rng, true, true)));
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
    /// Failed with an error the program does not retry (23505, 23503, ..).
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
    for (i, op) in prog.ops.iter().enumerate() {
        rig.note(
            session,
            format!(
                "T{} {:?} p{prog_idx}.{attempt} op {i}/{}: {op:?}",
                s.uid(),
                s.txn_id(),
                prog.ops.len()
            ),
        );
        let r = match *op {
            POp::Read(k) => s.read(k).map(|_| ()),
            POp::Scan(lo, hi) => s.scan(lo, hi).map(|_| ()),
            POp::Insert(k) => s.insert(k),
            POp::Update(k) => s.update(k).map(|_| ()),
            POp::Delete(k) => s.delete(k).map(|_| ()),
            POp::Upsert(k) => s.upsert(k),
            POp::FkInsert(c) => s.fk_insert(c),
            POp::SpUpdate(k) => s.update_rolled_back(k).map(|_| ()),
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
    pub threads: u16,
    /// Programs per thread.
    pub progs: u32,
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
            threads: 8,
            progs: 200,
            max_attempts: 12,
            mix: Mix::STANDARD,
            deadlock_ms: 5,
            bug: Bug::None,
        }
    }
}

/// Runs the seeded workload on `cfg.threads` real threads over a fresh rig
/// and returns the drained history.
pub fn run_workload(cfg: &Config) -> Run {
    let rig = Rig::new(cfg.deadlock_ms);
    rig.setup();
    let progress = AtomicU64::new(0);
    let finished = AtomicBool::new(false);
    std::thread::scope(|sc| {
        // A hang guard, not a timing assertion: only a run where no attempt
        // at all finishes for a very long stretch (a lost wakeup) is cut
        // short, with a diagnostic.
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
                            "g2: no attempt finished for 60 s: the run hung (seed {:#x}, level {})\n{}",
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
        let handles: Vec<_> = (1..=cfg.threads)
            .map(|session| {
                let rig = &rig;
                let progress = &progress;
                sc.spawn(move || {
                    for idx in 0..cfg.progs {
                        let prog = gen_prog(cfg.seed, session, idx, &cfg.mix);
                        for attempt in 0..cfg.max_attempts {
                            let out =
                                run_prog(rig, session, idx, attempt, cfg.level, cfg.bug, &prog);
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
        for h in handles {
            h.join().expect("a session thread panicked");
        }
        finished.store(true, Ordering::SeqCst);
    });
    rig.finish()
}
