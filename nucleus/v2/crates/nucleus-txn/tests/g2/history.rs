//! G2 history: what each session observed and wrote, recorded by the harness
//! and nothing else. The recorder is append-only and thread-safe; nothing
//! reads it until the run drains (the engine cannot observe it: no hook into
//! engine internals, only the public API's own return values).
//!
//! A history is a list of transactions (one per attempt, retries are fresh
//! txns). Each transaction holds its operations in order; each operation
//! carries `(session, real-time seq, op, keys, observed or written values,
//! outcome)`:
//!
//! - the session and the transaction's attempt are on the [`TxnRec`];
//! - `start`/`end` are real-time ticks of one global counter (used for
//!   concurrency and for the trace only, never for a verdict on values);
//! - `reads` are the observations (a point read, every slot of a scan, the
//!   row an UPDATE/DELETE/upsert acted on, the parent an FK check saw);
//! - `writes` are the effects when the op succeeded, `err` the SQLSTATE when
//!   it did not;
//! - the transaction's [`Fate`] is the commit ts or the abort reason.
//!
//! Values are unique per write (`Val::new(txn uid, n)`), so the checker can
//! identify the writer of any observed value without trusting engine
//! timestamps. The key space is 16 slots: 12 plain rows, 2 parents, 2
//! children (child `i` references parent `i`).

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

/// Slots `0..PLAIN` are plain rows.
pub const PLAIN: u8 = 12;
/// Slots `PARENT0..PARENT0 + 2` are FK parents.
pub const PARENT0: u8 = 12;
/// Slots `CHILD0..CHILD0 + 2` are FK children (child `CHILD0 + i` references
/// parent `PARENT0 + i`).
pub const CHILD0: u8 = 14;
/// All slots.
pub const SLOTS: u8 = 16;

pub fn is_parent(k: u8) -> bool {
    (PARENT0..PARENT0 + 2).contains(&k)
}

/// The child slot that references parent slot `p`.
pub fn child_of(p: u8) -> u8 {
    p - PARENT0 + CHILD0
}

/// The parent slot a child slot references.
pub fn parent_of(c: u8) -> u8 {
    c - CHILD0 + PARENT0
}

/// Short slot name for traces: `k07`, `p0`, `c1`.
pub fn key_name(k: u8) -> String {
    if k < PLAIN {
        format!("k{k:02}")
    } else if is_parent(k) {
        format!("p{}", k - PARENT0)
    } else {
        format!("c{}", k - CHILD0)
    }
}

/// A value written by exactly one write of one transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Val(pub u64);

impl Val {
    pub fn new(uid: u32, n: u16) -> Val {
        Val((u64::from(uid) << 16) | u64::from(n))
    }
}

impl std::fmt::Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}.{}", self.0 >> 16, self.0 & 0xffff)
    }
}

/// The isolation level a transaction ran (or, for a check, is held) at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Level {
    pub fn short(self) -> &'static str {
        match self {
            Level::ReadCommitted => "RC",
            Level::RepeatableRead => "RR",
            Level::Serializable => "SER",
        }
    }
}

/// An operation's SQLSTATE, as far as the harness distinguishes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlErr {
    /// 40001
    SerFailure,
    /// 40P01
    Deadlock,
    /// 55P03
    NotAvailable,
    /// 23505
    Unique,
    /// 23503
    Fk,
    /// Anything else: an engine error the workload never expects.
    Other(String),
}

impl SqlErr {
    pub fn code(&self) -> &str {
        match self {
            SqlErr::SerFailure => "40001",
            SqlErr::Deadlock => "40P01",
            SqlErr::NotAvailable => "55P03",
            SqlErr::Unique => "23505",
            SqlErr::Fk => "23503",
            SqlErr::Other(_) => "XXXXX",
        }
    }

    /// Whether the session retries the program with a fresh txn.
    pub fn retryable(&self) -> bool {
        matches!(self, SqlErr::SerFailure | SqlErr::Deadlock)
    }
}

/// What a read saw of one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Obs {
    Val(Val),
    /// No live row (never written, or deleted).
    Absent,
}

/// A write's effect on its slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    Put(Val),
    Del,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Read,
    Scan,
    Insert,
    Update,
    Delete,
    Upsert,
    /// Insert of a child row plus the FK check on its parent.
    FkInsert,
    /// An update inside a savepoint that is rolled back (its writes are
    /// void; its read still counts).
    SpUpdate,
}

impl OpKind {
    fn tag(self) -> &'static str {
        match self {
            OpKind::Read => "R",
            OpKind::Scan => "S",
            OpKind::Insert => "I",
            OpKind::Update => "U",
            OpKind::Delete => "D",
            OpKind::Upsert => "UP",
            OpKind::FkInsert => "FK",
            OpKind::SpUpdate => "SP",
        }
    }
}

/// One operation of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpRec {
    pub kind: OpKind,
    /// The statement's target slots (scan: `[lo, hi)`; FK insert: `[child,
    /// parent]`).
    pub keys: Vec<u8>,
    pub start: u64,
    pub end: u64,
    /// The snapshot ts the reads were taken at, when known.
    pub snap: Option<u64>,
    pub reads: Vec<(u8, Obs)>,
    pub writes: Vec<(u8, Write)>,
    /// The writes were rolled back (savepoint): they never exist.
    pub void: bool,
    pub err: Option<SqlErr>,
}

/// A transaction's end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// Committed; the ts is 0 for a read-only txn below SERIALIZABLE (no ts).
    Committed(u64),
    /// The program rolled back on purpose.
    Rolledback,
    /// An operation or the commit failed.
    Failed(SqlErr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnRec {
    pub uid: u32,
    pub session: u16,
    pub prog: u32,
    pub attempt: u32,
    pub level: Level,
    /// The txn's own snapshot ts (RR / SERIALIZABLE), when known.
    pub snap: Option<u64>,
    pub start: u64,
    pub end: u64,
    pub fate: Fate,
    pub ops: Vec<OpRec>,
}

impl TxnRec {
    pub fn committed(&self) -> bool {
        matches!(self.fate, Fate::Committed(_))
    }

    pub fn ts(&self) -> u64 {
        match self.fate {
            Fate::Committed(ts) => ts,
            _ => 0,
        }
    }
}

/// The drained history of a run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct History {
    pub txns: Vec<TxnRec>,
}

impl History {
    /// The compact trace of the whole history (the failing-seed dump):
    /// one line per txn, ops separated by `;`.
    pub fn trace(&self) -> String {
        let mut s = String::new();
        for t in &self.txns {
            s.push_str(&trace_txn(t));
            s.push('\n');
        }
        s
    }

    /// The trace of just the txns with these uids (a violation's context).
    pub fn trace_of(&self, uids: &[u32]) -> String {
        let mut s = String::new();
        for t in self.txns.iter().filter(|t| uids.contains(&t.uid)) {
            s.push_str(&trace_txn(t));
            s.push('\n');
        }
        s
    }
}

fn obs_str(o: Obs) -> String {
    match o {
        Obs::Val(v) => v.to_string(),
        Obs::Absent => "-".to_string(),
    }
}

pub fn trace_op(op: &OpRec) -> String {
    let mut s = String::new();
    let _ = write!(s, "{}[", op.kind.tag());
    for (i, k) in op.keys.iter().enumerate() {
        if i > 0 {
            s.push(if op.kind == OpKind::Scan { '.' } else { ',' });
        }
        s.push_str(&key_name(*k));
    }
    s.push(']');
    if !op.reads.is_empty() {
        s.push_str(" r");
        if op.kind == OpKind::Scan {
            // Only the live rows; the rest of the range was absent.
            for (k, o) in &op.reads {
                if let Obs::Val(v) = o {
                    let _ = write!(s, " {}={v}", key_name(*k));
                }
            }
        } else {
            for (k, o) in &op.reads {
                let _ = write!(s, " {}={}", key_name(*k), obs_str(*o));
            }
        }
        if let Some(sn) = op.snap {
            let _ = write!(s, " @{sn}");
        }
    }
    if !op.writes.is_empty() {
        s.push_str(if op.void { " w~" } else { " w" });
        for (k, w) in &op.writes {
            match w {
                Write::Put(v) => {
                    let _ = write!(s, " {}:={v}", key_name(*k));
                }
                Write::Del => {
                    let _ = write!(s, " {}:=del", key_name(*k));
                }
            }
        }
    }
    if let Some(e) = &op.err {
        let _ = write!(s, " !{}", e.code());
        if let SqlErr::Other(m) = e {
            let _ = write!(s, "({m})");
        }
    }
    s
}

pub fn trace_txn(t: &TxnRec) -> String {
    let fate = match &t.fate {
        Fate::Committed(ts) => format!("C@{ts}"),
        Fate::Rolledback => "ROLLBACK".to_string(),
        Fate::Failed(e) => format!("FAIL {}", e.code()),
    };
    let mut s = format!(
        "T{:<5} s{} p{}.{} {:<3} [{}-{}] {:<9}|",
        t.uid,
        t.session,
        t.prog,
        t.attempt,
        t.level.short(),
        t.start,
        t.end,
        fate
    );
    for (i, op) in t.ops.iter().enumerate() {
        s.push_str(if i == 0 { " " } else { "; " });
        s.push_str(&trace_op(op));
    }
    s
}

/// The shared recorder: a global tick counter, a uid allocator and the
/// append-only log. Sessions build their [`TxnRec`] locally and push it once
/// when the txn ends, so the lock is taken once per txn.
pub struct Recorder {
    clock: AtomicU64,
    uid: AtomicU32,
    log: Mutex<Vec<TxnRec>>,
}

impl Recorder {
    pub fn new() -> Recorder {
        Recorder {
            clock: AtomicU64::new(1),
            // Uid 1 is the setup txn; sessions start at 2.
            uid: AtomicU32::new(1),
            log: Mutex::new(Vec::new()),
        }
    }

    /// The next real-time tick.
    pub fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    /// A fresh transaction uid.
    pub fn next_uid(&self) -> u32 {
        self.uid.fetch_add(1, Ordering::SeqCst)
    }

    pub fn push(&self, rec: TxnRec) {
        self.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(rec);
    }

    /// Drains the log into a history ordered by end tick (then uid).
    pub fn drain(&self) -> History {
        let mut txns = std::mem::take(
            &mut *self
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        txns.sort_by_key(|t| (t.end, t.uid));
        History { txns }
    }
}

impl Default for Recorder {
    fn default() -> Self {
        Recorder::new()
    }
}

/// A builder for hand-made histories. Calls tick a shared clock in call
/// order, so the order of calls across transactions is the interleaving.
#[derive(Default)]
pub struct Hb {
    clock: u64,
    txns: Vec<TxnRec>,
}

impl Hb {
    pub fn new() -> Hb {
        Hb::default()
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Starts a transaction; the returned handle names it in later calls.
    pub fn begin(&mut self, uid: u32, level: Level) -> usize {
        let start = self.tick();
        self.txns.push(TxnRec {
            uid,
            session: uid as u16,
            prog: 0,
            attempt: 0,
            level,
            snap: None,
            start,
            end: start,
            fate: Fate::Rolledback,
            ops: Vec::new(),
        });
        self.txns.len() - 1
    }

    fn op(
        &mut self,
        t: usize,
        kind: OpKind,
        keys: &[u8],
        reads: Vec<(u8, Obs)>,
        writes: Vec<(u8, Write)>,
    ) {
        let start = self.tick();
        let end = self.tick();
        self.txns[t].ops.push(OpRec {
            kind,
            keys: keys.to_vec(),
            start,
            end,
            snap: None,
            reads,
            writes,
            void: false,
            err: None,
        });
    }

    /// The current clock, to backdate an op that blocked ([`Hb::set_start`]).
    pub fn mark(&self) -> u64 {
        self.clock
    }

    /// Backdates the start of the txn's last op (it was blocked from then).
    pub fn set_start(&mut self, t: usize, tick: u64) {
        if let Some(op) = self.txns[t].ops.last_mut() {
            op.start = tick;
        }
    }

    /// Sets the snapshot ts of the txn's last op.
    pub fn snap(&mut self, t: usize, ts: u64) -> &mut Hb {
        if let Some(op) = self.txns[t].ops.last_mut() {
            op.snap = Some(ts);
        }
        self
    }

    pub fn read(&mut self, t: usize, key: u8, obs: Obs) {
        self.op(t, OpKind::Read, &[key], vec![(key, obs)], vec![]);
    }

    pub fn scan(&mut self, t: usize, lo: u8, hi: u8, obs: &[Obs]) {
        let reads = (lo..hi).zip(obs.iter().copied()).collect();
        self.op(t, OpKind::Scan, &[lo, hi], reads, vec![]);
    }

    pub fn insert(&mut self, t: usize, key: u8, val: Val) {
        self.op(
            t,
            OpKind::Insert,
            &[key],
            vec![],
            vec![(key, Write::Put(val))],
        );
    }

    /// An UPDATE that overwrote `saw` with `val`.
    pub fn update(&mut self, t: usize, key: u8, saw: Val, val: Val) {
        self.op(
            t,
            OpKind::Update,
            &[key],
            vec![(key, Obs::Val(saw))],
            vec![(key, Write::Put(val))],
        );
    }

    pub fn delete(&mut self, t: usize, key: u8, saw: Val) {
        self.op(
            t,
            OpKind::Delete,
            &[key],
            vec![(key, Obs::Val(saw))],
            vec![(key, Write::Del)],
        );
    }

    /// An op that failed with `err`; `reads` is whatever it observed first.
    pub fn fail(
        &mut self,
        t: usize,
        kind: OpKind,
        keys: &[u8],
        reads: Vec<(u8, Obs)>,
        err: SqlErr,
    ) {
        self.op(t, kind, keys, reads, vec![]);
        if let Some(op) = self.txns[t].ops.last_mut() {
            op.err = Some(err);
        }
    }

    /// Marks the txn's last op void (rolled back to a savepoint).
    pub fn void_last(&mut self, t: usize) {
        if let Some(op) = self.txns[t].ops.last_mut() {
            op.void = true;
        }
    }

    pub fn commit(&mut self, t: usize, ts: u64) {
        self.txns[t].end = self.tick();
        self.txns[t].fate = Fate::Committed(ts);
    }

    pub fn rollback(&mut self, t: usize) {
        self.txns[t].end = self.tick();
        self.txns[t].fate = Fate::Rolledback;
    }

    pub fn failed(&mut self, t: usize, err: SqlErr) {
        self.txns[t].end = self.tick();
        self.txns[t].fate = Fate::Failed(err);
    }

    pub fn finish(self) -> History {
        let mut txns = self.txns;
        txns.sort_by_key(|t| (t.end, t.uid));
        History { txns }
    }
}
