//! G2 checker: decides from a drained [`History`] alone whether the run was
//! serializable, and which weaker guarantees it satisfies.
//!
//! Method (Elle, Liu et al.; Adya's thesis): histories of read/write sets
//! and observed values, not wall-clock logs. Every write carries a unique
//! value, so the writer of any observed value is known without trusting the
//! engine. The version order of a key is rebuilt from what the writes
//! themselves report: an UPDATE/DELETE/upsert records the row it overwrote
//! (first-updater-wins and EPQ make that the immediate predecessor), an
//! INSERT over a dead row is placed by commit ts after the newest earlier
//! version. From it the direct serialization graph (DSG) over committed txns
//! has three dependency kinds:
//!
//! - **ww**: a version and the version that overwrote it;
//! - **wr**: the txn that wrote a value, and a txn that read that value;
//! - **rw** (anti-dependency): a txn that read a version, and the txn that
//!   wrote the next version. A read of the initial/dead state implies an edge
//!   from the "initial state" pseudo-txn, which has no incoming edges and so
//!   never lies on a cycle; its rw edges to the first writer are kept.
//!
//! Aborted txns contribute their reads (wr edges into them, rw edges out of
//! them, used to explain their 40001) but not their writes.
//!
//! **Verdict.** A history is serializable iff the DSG of its committed txns
//! has no cycle and there is no G1a/G1b (Adya's PL-3 criterion: "a history is
//! serializable iff its DSG is acyclic and it has no G1a/G1b"). Cycles are
//! classified by their edge kinds into Adya's / Elle's anomalies:
//! G0 (ww only), G1c (ww+wr), G-single (exactly one rw; a same-key 2-cycle is
//! a lost update), G-nonadjacent (two or more rw, none adjacent; with
//! read-only readers it is the long fork) and G2-item (two adjacent rw; a
//! two-txn cycle is write skew). Each anomaly names the lowest level that
//! must prevent it: RC prevents G0/G1; RR (snapshot isolation) also prevents
//! every cycle without two adjacent rw edges (Cerone-Gotsman / Fekete: SI
//! allows only cycles with two adjacent anti-dependencies), non-repeatable
//! reads and statement-level lost updates; SER prevents every cycle.
//!
//! **Cross-checks with outcomes** (C-T0 §11 names in reports: I-SER,
//! I-SSI-PRECISION, I-UNIQUE, I-FK, I-LIVE): every 40001 is explained by a
//! DSG cycle through the aborted txn (a true positive), by a dangerous
//! structure `T1 -rw-> T2 -rw-> T3` among concurrent txns (a conservative
//! abort: counted as a *false positive*, which is legal), or by a write
//! conflict (first-updater-wins); anything else is a failure. Every 40P01
//! lies on a potential wait-for cycle among concurrent txns derived from the
//! recorded lock footprints. Every 23505 has a live row to collide with.
//! Every 23503 has a missing parent or a live child.
//!
//! Everything here depends only on the history, never on scheduling: the
//! output order is fixed by the history's order and sorted containers.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::rc::Rc;

use super::history::{
    child_of, is_parent, key_name, trace_txn, Fate, History, Level, Obs, OpKind, OpRec, SqlErr,
    Val, Write, SLOTS,
};

/// The anomaly classes (Adya / Elle names).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// ww-only cycle.
    G0,
    /// A read of a value written by an aborted (or rolled-back) txn.
    G1a,
    /// A read of a non-final value of a committed txn.
    G1b,
    /// ww+wr cycle.
    G1c,
    /// Exactly one rw edge on the cycle.
    GSingle,
    /// Two txns overwrote the same version, or a read-modify-write lost its
    /// read (same-key ww+rw two-cycle).
    LostUpdate,
    /// Two or more rw edges, no two adjacent (the long fork when the readers
    /// are read-only).
    LongFork,
    /// Two adjacent rw edges on a two-txn cycle.
    WriteSkew,
    /// Two adjacent rw edges on a longer cycle.
    G2Item,
    /// An UPDATE overwrote a value that was aborted or non-final.
    DirtyWrite,
    /// A read returned a value nobody wrote, or the wrong key's value.
    Garbage,
    /// A txn did not see its own earlier write.
    ReadYourWrites,
    /// Two reads of one key in one RR/SER txn differ.
    NonRepeatableRead,
    /// A read is not the newest version at its snapshot ts.
    SnapshotViolation,
    /// An INSERT succeeded over a live row.
    InsertOverLive,
    /// A cycle that closes through an INSERT over a tombstone newer than the
    /// inserter's snapshot. PostgreSQL permits it below SERIALIZABLE and the
    /// spec keeps it (C-T0 5.1 I-WW: "inserting over a tombstone newer than
    /// S is allowed"); SERIALIZABLE must still prevent it.
    InsertAfterDelete,
    /// A 40001 with no cycle, dangerous structure or write conflict.
    Unexplained40001,
    /// A 40P01 on no potential wait-for cycle.
    Unexplained40P01,
    /// A 23505 with no live row to collide with.
    Unexplained23505,
    /// A 23503 with a live parent and no live child.
    Unexplained23503,
    /// An engine error the workload never expects.
    EngineError,
}

/// One edge of a reported cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Edge {
    pub from: u32,
    pub to: u32,
    pub kind: Ek,
    pub key: u8,
    /// A ww edge into an INSERT over a dead row.
    pub ins: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ek {
    Ww,
    Wr,
    Rw,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anomaly {
    pub kind: Kind,
    /// The lowest level that must prevent it.
    pub min_level: Level,
    pub txns: Vec<u32>,
    pub detail: String,
    pub cycle: Vec<Edge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Stats {
    pub txns: usize,
    pub committed: usize,
    pub rolled_back: usize,
    pub e40001: usize,
    pub e40p01: usize,
    pub e23505: usize,
    pub e23503: usize,
    pub other_err: usize,
    pub ww: usize,
    pub wr: usize,
    pub rw: usize,
    /// 40001s on a DSG cycle through the aborted txn (true positives).
    pub f_cycle: usize,
    /// 40001s only on a dangerous structure: conservative aborts, legal,
    /// reported as false positives.
    pub f_dangerous: usize,
    /// 40001s from a write conflict (first-updater-wins, newer unique row).
    pub f_conflict: usize,
    pub f_unexplained: usize,
    /// 40P01s on a potential wait-for cycle.
    pub deadlocks_explained: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub level: Level,
    pub anomalies: Vec<Anomaly>,
    pub stats: Stats,
}

impl Verdict {
    /// The anomalies the checked level must have prevented.
    pub fn violations(&self) -> Vec<&Anomaly> {
        self.anomalies
            .iter()
            .filter(|a| a.min_level <= self.level)
            .collect()
    }

    pub fn has(&self, kind: Kind) -> bool {
        self.anomalies.iter().any(|a| a.kind == kind)
    }

    /// Whether `kind` was found and the checked level must prevent it.
    pub fn violates(&self, kind: Kind) -> bool {
        self.violations().iter().any(|a| a.kind == kind)
    }

    /// Serializable: no anomaly at all (a history with only
    /// allowed-below-SER anomalies is not).
    pub fn serializable(&self) -> bool {
        self.anomalies.is_empty()
    }

    /// Any cycle in the DSG of committed txns.
    pub fn has_cycle(&self) -> bool {
        self.anomalies.iter().any(|a| !a.cycle.is_empty())
    }

    /// The human report: stats, then each violation with the involved txns.
    pub fn report(&self, h: &History) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "level {}: {:?}", self.level.short(), self.stats);
        for a in &self.anomalies {
            let prevented = if a.min_level <= self.level {
                "VIOLATION"
            } else {
                "allowed"
            };
            let _ = writeln!(
                s,
                "{prevented} {:?} (prevented from {}): {}",
                a.kind,
                a.min_level.short(),
                a.detail
            );
            for e in &a.cycle {
                let _ = writeln!(
                    s,
                    "    T{} -{:?}[{}]-> T{}",
                    e.from,
                    e.kind,
                    key_name(e.key),
                    e.to
                );
            }
            s.push_str(&h.trace_of(&a.txns));
        }
        s
    }
}

/// Checks `h` against `level`.
pub fn check(h: &History, level: Level) -> Verdict {
    Checker::new(h, level).run()
}

// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Own {
    t: usize,
    key: u8,
    fin: bool,
    void: bool,
}

#[derive(Debug, Clone, Copy)]
struct Fin {
    w: Write,
    first_op: usize,
}

#[derive(Debug, Clone)]
struct Ver {
    key: u8,
    writer: Option<usize>,
    val: Option<Val>,
    ts: u64,
    pred: Option<usize>,
    /// Placed over a dead row by an INSERT (predecessor chosen by commit ts).
    ins: bool,
}

#[derive(Debug, Clone, Copy)]
struct Ext {
    key: u8,
    obs: Obs,
    snap: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct E {
    from: usize,
    to: usize,
    kind: Ek,
    key: u8,
    ins: bool,
}

struct Checker<'a> {
    h: &'a History,
    level: Level,
    anomalies: Vec<Anomaly>,
    stats: Stats,
    owner: HashMap<Val, Own>,
    finals: Vec<BTreeMap<u8, Fin>>,
    vers: Vec<Ver>,
    by_key: Vec<Vec<usize>>,
    ver_of_val: HashMap<Val, usize>,
    succ: Vec<Vec<usize>>,
    ext: Vec<Vec<Ext>>,
    /// Every read of every txn, own-write reads included: SSI takes a SIREAD
    /// for those too.
    all_reads: Vec<Vec<Ext>>,
    by_session: Vec<Vec<usize>>,
    /// Committed-txn graph: node of a txn index.
    node_of: Vec<Option<usize>>,
    nodes: Vec<usize>,
    edges: Vec<E>,
    ab_edges: Vec<E>,
    /// Memoised conflict queries of the outcome cross-checks.
    conc_cache: RefCell<HashMap<usize, Rc<Vec<usize>>>>,
    out_cache: RefCell<HashMap<usize, Rc<Vec<usize>>>>,
    dsg_adj: HashMap<usize, Vec<usize>>,
    /// Committed txns an aborted txn read from / was overwritten by.
    ab_in: HashMap<usize, Vec<usize>>,
    ab_out: HashMap<usize, Vec<usize>>,
}

impl<'a> Checker<'a> {
    fn new(h: &'a History, level: Level) -> Checker<'a> {
        Checker {
            h,
            level,
            anomalies: Vec::new(),
            stats: Stats::default(),
            owner: HashMap::new(),
            finals: Vec::new(),
            vers: Vec::new(),
            by_key: Vec::new(),
            ver_of_val: HashMap::new(),
            succ: Vec::new(),
            ext: Vec::new(),
            all_reads: Vec::new(),
            by_session: Vec::new(),
            node_of: Vec::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
            ab_edges: Vec::new(),
            conc_cache: RefCell::new(HashMap::new()),
            out_cache: RefCell::new(HashMap::new()),
            dsg_adj: HashMap::new(),
            ab_in: HashMap::new(),
            ab_out: HashMap::new(),
        }
    }

    fn uid(&self, t: usize) -> u32 {
        self.h.txns[t].uid
    }

    fn flag(&mut self, kind: Kind, min_level: Level, txns: Vec<u32>, detail: String) {
        self.anomalies.push(Anomaly {
            kind,
            min_level,
            txns,
            detail,
            cycle: Vec::new(),
        });
    }

    fn run(mut self) -> Verdict {
        self.count_outcomes();
        self.index_writes();
        self.build_versions();
        self.resolve_preds();
        self.scan_reads();
        self.build_edges();
        self.find_cycles();
        self.cross_check_outcomes();
        // A stable report order: by class, then by the txns involved.
        let order = |k: Kind| k as usize;
        self.anomalies
            .sort_by_key(|a| (order(a.kind), a.txns.clone()));
        Verdict {
            level: self.level,
            anomalies: self.anomalies,
            stats: self.stats,
        }
    }

    fn count_outcomes(&mut self) {
        let s = &mut self.stats;
        for t in &self.h.txns {
            s.txns += 1;
            match &t.fate {
                Fate::Committed(_) => s.committed += 1,
                Fate::Rolledback => s.rolled_back += 1,
                Fate::Failed(e) => match e {
                    SqlErr::SerFailure => s.e40001 += 1,
                    SqlErr::Deadlock => s.e40p01 += 1,
                    SqlErr::Unique => s.e23505 += 1,
                    SqlErr::Fk => s.e23503 += 1,
                    _ => s.other_err += 1,
                },
            }
        }
        for t in &self.h.txns {
            for op in &t.ops {
                if let Some(SqlErr::Other(m)) = &op.err {
                    let detail = format!("engine error in {}: {m}", trace_txn(t));
                    self.anomalies.push(Anomaly {
                        kind: Kind::EngineError,
                        min_level: Level::ReadCommitted,
                        txns: vec![t.uid],
                        detail,
                        cycle: Vec::new(),
                    });
                }
            }
        }
    }

    /// Per-txn final write per key and the value-to-writer registry.
    fn index_writes(&mut self) {
        let n = self.h.txns.len();
        self.finals = vec![BTreeMap::new(); n];
        for (ti, t) in self.h.txns.iter().enumerate() {
            for (oi, op) in t.ops.iter().enumerate() {
                if op.err.is_some() {
                    continue;
                }
                for &(k, w) in &op.writes {
                    if let Write::Put(v) = w {
                        self.owner.insert(
                            v,
                            Own {
                                t: ti,
                                key: k,
                                fin: false,
                                void: op.void,
                            },
                        );
                    }
                    if !op.void {
                        self.finals[ti]
                            .entry(k)
                            .and_modify(|f| f.w = w)
                            .or_insert(Fin { w, first_op: oi });
                    }
                }
            }
            if t.committed() {
                for f in self.finals[ti].values() {
                    if let Write::Put(v) = f.w {
                        if let Some(o) = self.owner.get_mut(&v) {
                            o.fin = true;
                        }
                    }
                }
            }
        }
        // Sessions run one txn at a time: per-session lists ordered by start
        // give the concurrency queries.
        let mut sessions: BTreeMap<u16, Vec<usize>> = BTreeMap::new();
        for (ti, t) in self.h.txns.iter().enumerate() {
            sessions.entry(t.session).or_default().push(ti);
        }
        for v in sessions.values_mut() {
            v.sort_by_key(|&ti| self.h.txns[ti].start);
        }
        self.by_session = sessions.into_values().collect();
    }

    fn build_versions(&mut self) {
        self.by_key = vec![Vec::new(); SLOTS as usize];
        for k in 0..SLOTS {
            self.vers.push(Ver {
                key: k,
                writer: None,
                val: None,
                ts: 0,
                pred: None,
                ins: false,
            });
            self.by_key[k as usize].push(self.vers.len() - 1);
        }
        for (ti, t) in self.h.txns.iter().enumerate() {
            if !t.committed() {
                continue;
            }
            for (&k, f) in &self.finals[ti] {
                let val = match f.w {
                    Write::Put(v) => Some(v),
                    Write::Del => None,
                };
                self.vers.push(Ver {
                    key: k,
                    writer: Some(ti),
                    val,
                    ts: t.ts(),
                    pred: None,
                    ins: false,
                });
                let id = self.vers.len() - 1;
                self.by_key[k as usize].push(id);
                if let Some(v) = val {
                    self.ver_of_val.insert(v, id);
                }
            }
        }
        for k in 0..SLOTS as usize {
            let vers = &self.vers;
            self.by_key[k].sort_by_key(|&id| (vers[id].ts, id));
        }
    }

    /// The newest version of `key` with ts <= `s` that is not `exclude`'s.
    fn newest_at(&self, key: u8, s: u64, exclude: Option<usize>) -> usize {
        let mut best = self.by_key[key as usize][0];
        for &id in &self.by_key[key as usize] {
            let v = &self.vers[id];
            if v.ts <= s && v.writer != exclude {
                best = id;
            }
        }
        best
    }

    /// The version an INSERT over a dead row follows: the newest other
    /// version of the key by commit ts below its own.
    fn newest_before(&self, id: usize) -> usize {
        let me = &self.vers[id];
        let mut best = self.by_key[me.key as usize][0];
        for &o in &self.by_key[me.key as usize] {
            let v = &self.vers[o];
            if o != id && v.ts < me.ts {
                best = o;
            }
        }
        best
    }

    fn resolve_preds(&mut self) {
        self.succ = vec![Vec::new(); self.vers.len()];
        for id in 0..self.vers.len() {
            let Some(w) = self.vers[id].writer else {
                continue;
            };
            let key = self.vers[id].key;
            let Some(fin) = self.finals[w].get(&key).copied() else {
                continue;
            };
            let op = &self.h.txns[w].ops[fin.first_op];
            let saw = op.reads.iter().find(|(k, _)| *k == key).map(|(_, o)| *o);
            let by_ts = |me: &Self| me.newest_before(id);
            let pred = match saw {
                Some(Obs::Val(sv)) => match self.owner.get(&sv).copied() {
                    Some(o) if o.key == key && !o.void && self.h.txns[o.t].committed() => {
                        if o.fin {
                            self.ver_of_val
                                .get(&sv)
                                .copied()
                                .unwrap_or_else(|| by_ts(self))
                        } else {
                            let uids = vec![self.uid(w), self.uid(o.t)];
                            let detail = format!(
                                "T{} overwrote {sv}, a non-final write of T{} (key {})",
                                self.uid(w),
                                self.uid(o.t),
                                key_name(key)
                            );
                            self.flag(Kind::DirtyWrite, Level::ReadCommitted, uids, detail);
                            by_ts(self)
                        }
                    }
                    Some(o) if o.key == key => {
                        let uids = vec![self.uid(w), self.uid(o.t)];
                        let detail = format!(
                            "T{} overwrote {sv}, written by an aborted or rolled-back T{} (key {})",
                            self.uid(w),
                            self.uid(o.t),
                            key_name(key)
                        );
                        self.flag(Kind::DirtyWrite, Level::ReadCommitted, uids, detail);
                        by_ts(self)
                    }
                    _ => {
                        let detail = format!(
                            "T{} overwrote {sv} on {}, which nobody wrote there",
                            self.uid(w),
                            key_name(key)
                        );
                        self.flag(
                            Kind::Garbage,
                            Level::ReadCommitted,
                            vec![self.uid(w)],
                            detail,
                        );
                        by_ts(self)
                    }
                },
                _ => {
                    // The row it replaced was dead: an INSERT (or an upsert
                    // that inserted). Placed by commit ts.
                    let p = by_ts(self);
                    if let Some(v) = self.vers[p].val {
                        let detail = format!(
                            "T{} inserted {} over the live {v} (T{})",
                            self.uid(w),
                            key_name(key),
                            self.vers[p].writer.map_or(0, |x| self.uid(x))
                        );
                        let mut uids = vec![self.uid(w)];
                        if let Some(x) = self.vers[p].writer {
                            uids.push(self.uid(x));
                        }
                        self.flag(Kind::InsertOverLive, Level::ReadCommitted, uids, detail);
                    }
                    self.vers[id].ins = true;
                    p
                }
            };
            self.vers[id].pred = Some(pred);
            self.succ[pred].push(id);
        }
        for p in 0..self.vers.len() {
            let vers = &self.vers;
            self.succ[p].sort_by_key(|&s| (vers[s].ts, s));
            if self.succ[p].len() > 1 {
                let writers: Vec<u32> = self.succ[p]
                    .iter()
                    .filter_map(|&s| self.vers[s].writer.map(|x| self.uid(x)))
                    .collect();
                let base = match self.vers[p].val {
                    Some(v) => v.to_string(),
                    None => "the dead row".to_string(),
                };
                let detail = format!(
                    "{} was overwritten by {} committed txns {:?} (key {}): an update was lost",
                    base,
                    writers.len(),
                    writers,
                    key_name(self.vers[p].key)
                );
                self.flag(Kind::LostUpdate, Level::ReadCommitted, writers, detail);
            }
        }
    }

    /// Per-txn read checks: read-your-writes, repeatable reads, G1a/G1b,
    /// the snapshot rule; and the external reads the graph is built from.
    fn scan_reads(&mut self) {
        self.ext = vec![Vec::new(); self.h.txns.len()];
        self.all_reads = vec![Vec::new(); self.h.txns.len()];
        for ti in 0..self.h.txns.len() {
            let t = &self.h.txns[ti];
            let mut own: HashMap<u8, Obs> = HashMap::new();
            let mut first: HashMap<u8, Obs> = HashMap::new();
            let mut found: Vec<(Kind, Level, Vec<u32>, String)> = Vec::new();
            let mut exts: Vec<Ext> = Vec::new();
            let mut all: Vec<Ext> = Vec::new();
            for op in &t.ops {
                for &(k, obs) in &op.reads {
                    all.push(Ext {
                        key: k,
                        obs,
                        snap: op.snap,
                    });
                    if let Some(&o) = own.get(&k) {
                        if o != obs {
                            found.push((
                                Kind::ReadYourWrites,
                                Level::ReadCommitted,
                                vec![t.uid],
                                format!(
                                    "T{} read {} after writing it and saw {:?}, expected {:?}",
                                    t.uid,
                                    key_name(k),
                                    obs,
                                    o
                                ),
                            ));
                        }
                        continue;
                    }
                    match first.get(&k) {
                        Some(&f) if f != obs => found.push((
                            Kind::NonRepeatableRead,
                            Level::RepeatableRead,
                            vec![t.uid],
                            format!(
                                "T{} read {} as {:?} and later as {:?} without writing it",
                                t.uid,
                                key_name(k),
                                f,
                                obs
                            ),
                        )),
                        Some(_) => {}
                        None => {
                            first.insert(k, obs);
                        }
                    }
                    exts.push(Ext {
                        key: k,
                        obs,
                        snap: op.snap,
                    });
                }
                if op.err.is_none() && !op.void {
                    for &(k, w) in &op.writes {
                        own.insert(
                            k,
                            match w {
                                Write::Put(v) => Obs::Val(v),
                                Write::Del => Obs::Absent,
                            },
                        );
                    }
                }
            }
            for e in &exts {
                if let Obs::Val(v) = e.obs {
                    match self.owner.get(&v).copied() {
                        None => found.push((
                            Kind::Garbage,
                            Level::ReadCommitted,
                            vec![t.uid],
                            format!(
                                "T{} read {v} on {}, which nobody wrote",
                                t.uid,
                                key_name(e.key)
                            ),
                        )),
                        Some(o) if o.key != e.key => found.push((
                            Kind::Garbage,
                            Level::ReadCommitted,
                            vec![t.uid, self.uid(o.t)],
                            format!(
                                "T{} read {v} on {} but T{} wrote it on {}",
                                t.uid,
                                key_name(e.key),
                                self.uid(o.t),
                                key_name(o.key)
                            ),
                        )),
                        Some(o) => {
                            let w = &self.h.txns[o.t];
                            if o.void || !w.committed() {
                                found.push((
                                    Kind::G1a,
                                    Level::ReadCommitted,
                                    vec![t.uid, w.uid],
                                    format!(
                                        "T{} read {v} on {}, written by T{} which did not commit ({:?})",
                                        t.uid,
                                        key_name(e.key),
                                        w.uid,
                                        if o.void { &Fate::Rolledback } else { &w.fate }
                                    ),
                                ));
                            } else if !o.fin {
                                found.push((
                                    Kind::G1b,
                                    Level::ReadCommitted,
                                    vec![t.uid, w.uid],
                                    format!(
                                        "T{} read {v} on {}, an intermediate write of T{}",
                                        t.uid,
                                        key_name(e.key),
                                        w.uid
                                    ),
                                ));
                            }
                        }
                    }
                }
                if let Some(s) = e.snap {
                    let exp = self.newest_at(e.key, s, Some(ti));
                    let ev = &self.vers[exp];
                    let ok = match e.obs {
                        Obs::Val(v) => ev.val == Some(v),
                        Obs::Absent => ev.val.is_none(),
                    };
                    if !ok {
                        let mut who = vec![t.uid];
                        if let Obs::Val(v) = e.obs {
                            if let Some(o) = self.owner.get(&v) {
                                who.push(self.uid(o.t));
                            }
                        }
                        if let Some(w) = ev.writer {
                            who.push(self.uid(w));
                        }
                        found.push((
                            Kind::SnapshotViolation,
                            Level::ReadCommitted,
                            who,
                            format!(
                                "T{} read {} as {:?} at snapshot {s}, but the newest version at {s} is {} (ts {})",
                                t.uid,
                                key_name(e.key),
                                e.obs,
                                ev.val.map_or("dead".to_string(), |v| v.to_string()),
                                ev.ts
                            ),
                        ));
                    }
                }
            }
            for (k, l, u, d) in found {
                self.flag(k, l, u, d);
            }
            self.ext[ti] = exts;
            self.all_reads[ti] = all;
        }
    }

    fn build_edges(&mut self) {
        self.node_of = vec![None; self.h.txns.len()];
        for (ti, t) in self.h.txns.iter().enumerate() {
            if t.committed() {
                self.node_of[ti] = Some(self.nodes.len());
                self.nodes.push(ti);
            }
        }
        let mut seen: HashSet<(usize, usize, Ek)> = HashSet::new();
        let mut push = |me: &mut Self, e: E| {
            if e.from == e.to || !seen.insert((e.from, e.to, e.kind)) {
                return;
            }
            let both = me.h.txns[e.from].committed() && me.h.txns[e.to].committed();
            if both {
                me.edges.push(e);
            } else {
                me.ab_edges.push(e);
            }
        };
        for v in 0..self.vers.len() {
            let succs = self.succ[v].clone();
            for (i, &s) in succs.iter().enumerate() {
                if let (Some(a), Some(b)) = (self.vers[v].writer, self.vers[s].writer) {
                    push(
                        self,
                        E {
                            from: a,
                            to: b,
                            kind: Ek::Ww,
                            key: self.vers[v].key,
                            ins: self.vers[s].ins,
                        },
                    );
                }
                if i > 0 {
                    if let (Some(a), Some(b)) =
                        (self.vers[succs[i - 1]].writer, self.vers[s].writer)
                    {
                        push(
                            self,
                            E {
                                from: a,
                                to: b,
                                kind: Ek::Ww,
                                key: self.vers[v].key,
                                ins: false,
                            },
                        );
                    }
                }
            }
        }
        for ti in 0..self.h.txns.len() {
            let exts = self.ext[ti].clone();
            for e in exts {
                let ver = match e.obs {
                    Obs::Val(v) => match self.owner.get(&v).copied() {
                        Some(o) if o.key == e.key && !o.void && self.h.txns[o.t].committed() => {
                            push(
                                self,
                                E {
                                    from: o.t,
                                    to: ti,
                                    kind: Ek::Wr,
                                    key: e.key,
                                    ins: false,
                                },
                            );
                            if o.fin {
                                self.ver_of_val.get(&v).copied()
                            } else {
                                None
                            }
                        }
                        _ => None,
                    },
                    Obs::Absent => match e.snap {
                        Some(s) => {
                            let id = self.newest_at(e.key, s, Some(ti));
                            if self.vers[id].val.is_none() {
                                Some(id)
                            } else {
                                None
                            }
                        }
                        None => Some(self.by_key[e.key as usize][0]),
                    },
                };
                if let Some(ver) = ver {
                    for &s in &self.succ[ver].clone() {
                        if let Some(w) = self.vers[s].writer {
                            push(
                                self,
                                E {
                                    from: ti,
                                    to: w,
                                    kind: Ek::Rw,
                                    key: e.key,
                                    ins: false,
                                },
                            );
                        }
                    }
                }
            }
        }
        for e in &self.edges {
            match e.kind {
                Ek::Ww => self.stats.ww += 1,
                Ek::Wr => self.stats.wr += 1,
                Ek::Rw => self.stats.rw += 1,
            }
        }
    }

    // ----- cycles ---------------------------------------------------------

    fn find_cycles(&mut self) {
        let n = self.nodes.len();
        let g = Graph::new(n, &self.nodes, &self.edges, self.h, false);
        // The same graph without the INSERT-over-newer-tombstone ww edges:
        // the search for what snapshot isolation forbids runs here, so a
        // PostgreSQL-permitted cycle cannot mask a real violation.
        let gs = Graph::new(n, &self.nodes, &self.edges, self.h, true);
        let full = g.adj(|_| true);
        let comp = scc(&full);
        if !has_nontrivial(&comp) {
            return;
        }
        let mut found: Vec<(Kind, Level, Vec<Edge>)> = Vec::new();
        // G0: a ww-only cycle.
        let ww = g.adj(|k| k == Ek::Ww);
        let wcomp = scc(&ww);
        if let Some(start) = first_in_nontrivial(&wcomp) {
            if let Some(path) = g.path(start, start, |k| k == Ek::Ww, &wcomp) {
                found.push((Kind::G0, Level::ReadCommitted, g.edges(&path)));
            }
        }
        // G1c: a ww+wr cycle through a wr edge.
        let b = g.adj(|k| k != Ek::Rw);
        let bcomp = scc(&b);
        if has_nontrivial(&bcomp) {
            let mut tried = 0;
            for (ei, e) in g.list.iter().enumerate() {
                if e.2 != Ek::Wr || bcomp[e.0] != bcomp[e.1] {
                    continue;
                }
                tried += 1;
                if tried > 2000 {
                    break;
                }
                if let Some(mut path) = g.path(e.1, e.0, |k| k != Ek::Rw, &bcomp) {
                    path.insert(0, ei);
                    found.push((Kind::G1c, Level::ReadCommitted, g.edges(&path)));
                    break;
                }
            }
        }
        // Cycles with an anti-dependency that snapshot isolation forbids:
        // one rw edge (G-single / lost update) or several, none adjacent
        // (long fork).
        let mut tried = 0;
        let comp_s = scc(&gs.adj(|_| true));
        let si_cycle = has_nontrivial(&scc(&gs.si_adj()));
        for (ei, e) in gs.list.iter().enumerate() {
            if !si_cycle || e.2 != Ek::Rw || comp_s[e.0] != comp_s[e.1] {
                continue;
            }
            tried += 1;
            if tried > 400 {
                break;
            }
            if let Some(mut path) = gs.path_no_adjacent_rw(e.1, e.0, &comp_s) {
                path.insert(0, ei);
                let cyc = gs.edges(&path);
                let (kind, min) = classify(&cyc);
                found.push((kind, min, cyc));
                break;
            }
        }
        // A cycle with two adjacent rw edges: allowed by snapshot isolation
        // (write skew), never by serializability.
        let mut tried = 0;
        'outer: for (ei, e) in g.list.iter().enumerate() {
            if e.2 != Ek::Rw || comp[e.0] != comp[e.1] {
                continue;
            }
            for &pi in &g.rw_in[e.0] {
                tried += 1;
                if tried > 800 {
                    break 'outer;
                }
                let p = g.list[pi];
                let back = if e.1 == p.0 {
                    Some(Vec::new())
                } else {
                    g.path(e.1, p.0, |_| true, &comp)
                };
                if let Some(mut path) = back {
                    path.insert(0, ei);
                    path.insert(0, pi);
                    let cyc = g.edges(&path);
                    let (kind, min) = classify(&cyc);
                    found.push((kind, min, cyc));
                    break 'outer;
                }
            }
        }
        // Any cycle at all (the SER criterion) if nothing above named one.
        if found.is_empty() {
            if let Some(start) = first_in_nontrivial(&comp) {
                if let Some(path) = g.path(start, start, |_| true, &comp) {
                    let cyc = g.edges(&path);
                    let (kind, min) = classify(&cyc);
                    found.push((kind, min, cyc));
                }
            }
        }
        for (kind, min_level, cycle) in found {
            let mut txns: Vec<u32> = cycle.iter().map(|e| e.from).collect();
            txns.sort_unstable();
            txns.dedup();
            let detail = format!("a cycle of {} txns", txns.len());
            self.anomalies.push(Anomaly {
                kind,
                min_level,
                txns,
                detail,
                cycle,
            });
        }
    }

    // ----- outcome cross-checks --------------------------------------------

    /// Indexes of the other txns whose interval overlaps `t`'s (a session
    /// runs one txn at a time, so each session's list is ordered by start and
    /// end).
    fn concurrent(&self, t: usize) -> Rc<Vec<usize>> {
        if let Some(c) = self.conc_cache.borrow().get(&t) {
            return Rc::clone(c);
        }
        let me = &self.h.txns[t];
        let mut out = Vec::new();
        for list in &self.by_session {
            let p = list.partition_point(|&x| self.h.txns[x].start < me.end);
            for &x in list[..p].iter().rev() {
                let o = &self.h.txns[x];
                if o.end <= me.start {
                    break;
                }
                if x != t {
                    out.push(x);
                }
            }
        }
        out.sort_unstable();
        let out = Rc::new(out);
        self.conc_cache.borrow_mut().insert(t, Rc::clone(&out));
        out
    }

    /// The txn's snapshot ts (RR/SER: one for the txn): its first known op
    /// snapshot.
    fn txn_snap(&self, t: usize) -> Option<u64> {
        let tr = &self.h.txns[t];
        if tr.level == Level::ReadCommitted {
            // Each RC statement has its own snapshot.
            return None;
        }
        tr.snap.or_else(|| tr.ops.iter().find_map(|o| o.snap))
    }

    /// Keys an op's failure may be about.
    fn conflict_keys(op: &OpRec) -> Vec<u8> {
        let mut ks = Vec::new();
        if op.kind != OpKind::Scan {
            ks.extend(op.keys.iter().copied());
        }
        for &k in &op.keys {
            if op.kind == OpKind::Delete && is_parent(k) {
                ks.push(child_of(k));
            }
        }
        ks
    }

    /// First-updater-wins and friends: a committed version of an op's key
    /// newer than the txn's snapshot.
    fn write_conflict(&self, t: usize) -> bool {
        let Some(op) = self.h.txns[t].ops.iter().rev().find(|o| o.err.is_some()) else {
            return false;
        };
        let Some(s) = self.txn_snap(t).or(op.snap) else {
            return false;
        };
        Self::conflict_keys(op).iter().any(|&k| {
            self.by_key[k as usize]
                .iter()
                .any(|&id| self.vers[id].writer.is_some_and(|w| w != t) && self.vers[id].ts > s)
        })
    }

    /// SSI-style conflicts `x -rw-> c`: x read a key c wrote (successfully,
    /// even if c later aborted), c is concurrent with x, and x did not see
    /// c's write.
    fn rwc_out(&self, x: usize) -> Rc<Vec<usize>> {
        if let Some(c) = self.out_cache.borrow().get(&x) {
            return Rc::clone(c);
        }
        let xs = self.txn_snap(x);
        let mut out = Vec::new();
        for &c in self.concurrent(x).iter() {
            let ct = &self.h.txns[c];
            let hit = self.all_reads[x].iter().any(|e| {
                ct.ops.iter().any(|op| {
                    // A write rolled back to a savepoint still recorded its
                    // conflicts (C-T0 5.5: SIREADs and rw-conflicts are kept).
                    op.err.is_none()
                        && op.writes.iter().any(|&(k, w)| {
                            k == e.key
                                && match w {
                                    Write::Put(v) => e.obs != Obs::Val(v),
                                    Write::Del => true,
                                }
                        })
                })
            });
            if !hit {
                continue;
            }
            // A committed writer at or below x's snapshot was seen.
            if ct.committed() && xs.is_some_and(|s| ct.ts() != 0 && ct.ts() <= s) {
                continue;
            }
            out.push(c);
        }
        let out = Rc::new(out);
        self.out_cache.borrow_mut().insert(x, Rc::clone(&out));
        out
    }

    fn rwc_in(&self, a: usize) -> Vec<usize> {
        self.concurrent(a)
            .iter()
            .copied()
            .filter(|&r| self.rwc_out(r).contains(&a))
            .collect()
    }

    /// `T1 -rw-> T2 -rw-> T3`, T3 committed first, `a` in any role.
    fn dangerous(&self, a: usize) -> bool {
        let ct = |x: usize| -> u64 {
            if self.h.txns[x].committed() {
                self.h.txns[x].ts()
            } else {
                u64::MAX
            }
        };
        let committed = |x: usize| self.h.txns[x].committed();
        // a as the pivot T2.
        let outs = self.rwc_out(a);
        let ins = self.rwc_in(a);
        for &t3 in outs.iter().filter(|&&x| committed(x)) {
            for &t1 in &ins {
                if t1 == t3 || ct(t1) > ct(t3) {
                    return true;
                }
            }
        }
        // a as T1.
        for &t2 in outs.iter() {
            for t3 in self.rwc_out(t2).iter().copied() {
                if committed(t3) && ct(t3) < ct(t2) {
                    return true;
                }
            }
        }
        // a as T3's partner in a two-cycle with a committed txn.
        outs.iter().any(|x| ins.contains(x) && committed(*x))
    }

    /// Whether `a`, had it committed, would sit on a cycle: a bounded search
    /// from its outgoing dependencies back to its incoming ones over the
    /// committed DSG.
    fn on_cycle_if_committed(&self, a: usize) -> bool {
        let mut goal: HashSet<usize> = HashSet::new();
        if let Some(v) = self.ab_in.get(&a) {
            goal.extend(v.iter().copied());
        }
        for c in self.rwc_in(a) {
            if self.h.txns[c].committed() {
                goal.insert(c);
            }
        }
        let mut start: Vec<usize> = self.ab_out.get(&a).cloned().unwrap_or_default();
        for &c in self.rwc_out(a).iter() {
            if self.h.txns[c].committed() {
                start.push(c);
            }
        }
        if goal.is_empty() || start.is_empty() {
            return false;
        }
        let mut seen: HashSet<usize> = start.iter().copied().collect();
        let mut q: VecDeque<usize> = start.into_iter().collect();
        while let Some(x) = q.pop_front() {
            if goal.contains(&x) {
                return true;
            }
            if seen.len() > 1000 {
                return false;
            }
            for &y in self.dsg_adj.get(&x).map(Vec::as_slice).unwrap_or(&[]) {
                if seen.insert(y) {
                    q.push_back(y);
                }
            }
        }
        false
    }

    fn cross_check_outcomes(&mut self) {
        let n = self.h.txns.len();
        for e in &self.edges {
            self.dsg_adj.entry(e.from).or_default().push(e.to);
        }
        for e in &self.ab_edges {
            if self.h.txns[e.to].committed() {
                self.ab_out.entry(e.from).or_default().push(e.to);
            }
            if self.h.txns[e.from].committed() {
                self.ab_in.entry(e.to).or_default().push(e.from);
            }
        }
        for t in 0..n {
            match self.h.txns[t].fate.clone() {
                Fate::Failed(SqlErr::SerFailure) => self.explain_40001(t),
                Fate::Failed(SqlErr::Deadlock) => self.explain_40p01(t),
                _ => {}
            }
            self.explain_op_errors(t);
        }
    }

    fn explain_40001(&mut self, t: usize) {
        let conflict = self.write_conflict(t);
        if self.level == Level::Serializable {
            let dangerous = self.dangerous(t);
            let cycle = (dangerous || !conflict) && self.on_cycle_if_committed(t);
            if cycle {
                self.stats.f_cycle += 1;
                return;
            }
            if dangerous {
                self.stats.f_dangerous += 1;
                return;
            }
        }
        if conflict {
            self.stats.f_conflict += 1;
            return;
        }
        self.stats.f_unexplained += 1;
        let uid = self.uid(t);
        let detail = format!(
            "40001 with no cycle, dangerous structure or write conflict (I-SSI-PRECISION): {}",
            trace_txn(&self.h.txns[t])
        );
        self.flag(
            Kind::Unexplained40001,
            Level::ReadCommitted,
            vec![uid],
            detail,
        );
    }

    /// Lock footprint of an op: (slot, mode) with 0 = KEY SHARE, 1 = NO KEY
    /// UPDATE, 2 = UPDATE.
    fn modes(op: &OpRec) -> Vec<(u8, u8)> {
        match op.kind {
            OpKind::Insert | OpKind::Update | OpKind::Upsert | OpKind::SpUpdate => {
                op.keys.iter().map(|&k| (k, 1)).collect()
            }
            OpKind::Delete => op.keys.iter().map(|&k| (k, 2)).collect(),
            OpKind::FkInsert => {
                let mut v = Vec::new();
                if let Some(&c) = op.keys.first() {
                    v.push((c, 1));
                }
                if let Some(&p) = op.keys.get(1) {
                    v.push((p, 0));
                }
                v
            }
            _ => Vec::new(),
        }
    }

    fn mode_conflict(a: u8, b: u8) -> bool {
        matches!((a, b), (0, 2) | (2, 0) | (1, 1) | (1, 2) | (2, 1) | (2, 2))
    }

    /// Whether `x` plausibly waited on `y`: an op of x on a slot where an
    /// op of y that completed earlier holds a conflicting lock, and y was
    /// still running when x's op started.
    fn waits_on(&self, x: usize, xop: Option<usize>, y: usize) -> bool {
        let xt = &self.h.txns[x];
        let yt = &self.h.txns[y];
        let xops: Vec<&OpRec> = match xop {
            Some(i) => vec![&xt.ops[i]],
            None => xt.ops.iter().collect(),
        };
        for xo in xops {
            if yt.end <= xo.start {
                continue;
            }
            for (xk, xm) in Self::modes(xo) {
                for yo in &yt.ops {
                    // An FK insert places its child row before it checks the
                    // parent, so it holds the child from its start whether
                    // it later succeeds, fails or is still blocked.
                    let mut held: Vec<(u8, u8)> = Vec::new();
                    if yo.err.is_none() && yo.end < xo.end {
                        held = Self::modes(yo);
                    }
                    if yo.kind == OpKind::FkInsert && yo.start < xo.end {
                        if let Some(&c) = yo.keys.first() {
                            held.push((c, 1));
                        }
                    }
                    if held
                        .iter()
                        .any(|&(yk, ym)| yk == xk && Self::mode_conflict(xm, ym))
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn explain_40p01(&mut self, v: usize) {
        let Some(vop) = self.h.txns[v].ops.iter().rposition(|o| o.err.is_some()) else {
            self.stats.deadlocks_explained += 1;
            return;
        };
        let mut set: Vec<usize> = self.concurrent(v).to_vec();
        set.push(v);
        // Depth-first from v over potential waits; v's own wait is its
        // failing op, every other member's is any op.
        let mut stack: Vec<usize> = vec![v];
        let mut seen: HashSet<usize> = HashSet::new();
        let mut cyc = false;
        'search: while let Some(x) = stack.pop() {
            for &y in &set {
                if y == x {
                    continue;
                }
                let w = if x == v {
                    self.waits_on(x, Some(vop), y)
                } else {
                    self.waits_on(x, None, y)
                };
                if !w {
                    continue;
                }
                if y == v {
                    cyc = true;
                    break 'search;
                }
                if seen.insert(y) {
                    stack.push(y);
                }
            }
        }
        if cyc {
            self.stats.deadlocks_explained += 1;
            return;
        }
        let uid = self.uid(v);
        let detail = format!(
            "40P01 on no potential wait-for cycle (I-LIVE c): {}",
            trace_txn(&self.h.txns[v])
        );
        self.flag(
            Kind::Unexplained40P01,
            Level::ReadCommitted,
            vec![uid],
            detail,
        );
    }

    /// 23505 needs a live row to collide with (I-UNIQUE); 23503 needs a
    /// missing parent or a live child (I-FK).
    fn explain_op_errors(&mut self, t: usize) {
        let tr = &self.h.txns[t];
        for (oi, op) in tr.ops.iter().enumerate() {
            match op.err {
                Some(SqlErr::Unique) => {
                    let Some(&k) = op.keys.first() else { continue };
                    let own_live = tr.ops[..oi]
                        .iter()
                        .filter(|o| o.err.is_none() && !o.void)
                        .flat_map(|o| o.writes.iter())
                        .rfind(|&&(wk, _)| wk == k)
                        .is_some_and(|&(_, w)| matches!(w, Write::Put(_)));
                    let other = self.h.txns.iter().enumerate().any(|(c, ct)| {
                        c != t
                            && ct.committed()
                            && ct.start < op.end
                            && self.finals[c]
                                .get(&k)
                                .is_some_and(|f| matches!(f.w, Write::Put(_)))
                    });
                    if !own_live && !other {
                        let detail = format!(
                            "23505 on {} with no live row to collide with (I-UNIQUE / I-LEAK): {}",
                            key_name(k),
                            trace_txn(tr)
                        );
                        let uid = tr.uid;
                        self.flag(
                            Kind::Unexplained23505,
                            Level::ReadCommitted,
                            vec![uid],
                            detail,
                        );
                    }
                }
                Some(SqlErr::Fk) => {
                    let ok = match op.kind {
                        OpKind::FkInsert => {
                            let Some(&p) = op.keys.get(1) else { continue };
                            op.reads.iter().any(|&(k, o)| k == p && o == Obs::Absent)
                                || self.h.txns.iter().enumerate().any(|(c, ct)| {
                                    c != t
                                        && ct.committed()
                                        && ct.start < op.end
                                        && self.finals[c]
                                            .get(&p)
                                            .is_some_and(|f| matches!(f.w, Write::Del))
                                })
                        }
                        OpKind::Delete => {
                            let Some(&p) = op.keys.first() else { continue };
                            let ch = child_of(p);
                            self.h.txns.iter().enumerate().any(|(c, ct)| {
                                (c == t || ct.committed())
                                    && ct.start < op.end
                                    && ct.ops.iter().any(|o| {
                                        o.err.is_none()
                                            && !o.void
                                            && o.start < op.end
                                            && o.writes.iter().any(|&(k, w)| {
                                                k == ch && matches!(w, Write::Put(_))
                                            })
                                    })
                            })
                        }
                        _ => false,
                    };
                    if !ok {
                        let detail = format!(
                            "23503 with a live parent and no live child (I-FK): {}",
                            trace_txn(tr)
                        );
                        let uid = tr.uid;
                        self.flag(
                            Kind::Unexplained23503,
                            Level::ReadCommitted,
                            vec![uid],
                            detail,
                        );
                    }
                }
                _ => {}
            }
        }
    }
}

/// Names a cycle by its edge kinds (Adya / Elle).
fn classify(cycle: &[Edge]) -> (Kind, Level) {
    let rw: Vec<bool> = cycle.iter().map(|e| e.kind == Ek::Rw).collect();
    let n_rw = rw.iter().filter(|&&b| b).count();
    if n_rw == 0 {
        return if cycle.iter().all(|e| e.kind == Ek::Ww) {
            (Kind::G0, Level::ReadCommitted)
        } else {
            (Kind::G1c, Level::ReadCommitted)
        };
    }
    if n_rw == 1 {
        if cycle.iter().any(|e| e.ins) {
            return (Kind::InsertAfterDelete, Level::Serializable);
        }
        if cycle.len() == 2 && cycle[0].key == cycle[1].key {
            return (Kind::LostUpdate, Level::RepeatableRead);
        }
        return (Kind::GSingle, Level::RepeatableRead);
    }
    let len = rw.len();
    let adjacent = (0..len).any(|i| rw[i] && rw[(i + 1) % len]);
    if !adjacent && cycle.iter().any(|e| e.ins) {
        return (Kind::InsertAfterDelete, Level::Serializable);
    }
    if !adjacent {
        (Kind::LongFork, Level::RepeatableRead)
    } else if cycle.len() == 2 {
        (Kind::WriteSkew, Level::Serializable)
    } else {
        (Kind::G2Item, Level::Serializable)
    }
}

// ----- graph machinery ------------------------------------------------------

struct Graph {
    n: usize,
    /// `(from node, to node, kind, key, ins)`, in a fixed order.
    list: Vec<(usize, usize, Ek, u8, bool)>,
    uids: Vec<u32>,
    out: Vec<Vec<usize>>,
    rw_in: Vec<Vec<usize>>,
}

impl Graph {
    fn new(n: usize, nodes: &[usize], edges: &[E], h: &History, drop_ins: bool) -> Graph {
        let mut node_of: HashMap<usize, usize> = HashMap::new();
        for (i, &t) in nodes.iter().enumerate() {
            node_of.insert(t, i);
        }
        let mut list = Vec::new();
        for e in edges {
            if drop_ins && e.ins {
                continue;
            }
            if let (Some(&a), Some(&b)) = (node_of.get(&e.from), node_of.get(&e.to)) {
                list.push((a, b, e.kind, e.key, e.ins));
            }
        }
        list.sort_by_key(|&(a, b, k, key, ins)| (a, b, k as u8, key, ins));
        let mut out = vec![Vec::new(); n];
        let mut rw_in = vec![Vec::new(); n];
        for (i, e) in list.iter().enumerate() {
            out[e.0].push(i);
            if e.2 == Ek::Rw {
                rw_in[e.1].push(i);
            }
        }
        Graph {
            n,
            list,
            uids: nodes.iter().map(|&t| h.txns[t].uid).collect(),
            out,
            rw_in,
        }
    }

    fn adj(&self, keep: impl Fn(Ek) -> bool) -> Vec<Vec<usize>> {
        let mut a = vec![Vec::new(); self.n];
        for e in &self.list {
            if keep(e.2) {
                a[e.0].push(e.1);
            }
        }
        a
    }

    fn edges(&self, path: &[usize]) -> Vec<Edge> {
        path.iter()
            .map(|&i| {
                let e = self.list[i];
                Edge {
                    from: self.uids[e.0],
                    to: self.uids[e.1],
                    kind: e.2,
                    key: e.3,
                    ins: e.4,
                }
            })
            .collect()
    }

    /// Shortest path (edge indices) `from` -> `to` over allowed kinds inside
    /// one component; `from == to` finds a cycle through it.
    fn path(
        &self,
        from: usize,
        to: usize,
        allow: impl Fn(Ek) -> bool,
        comp: &[usize],
    ) -> Option<Vec<usize>> {
        let mut parent: Vec<Option<usize>> = vec![None; self.n];
        let mut seen = vec![false; self.n];
        seen[from] = true;
        let mut q = VecDeque::new();
        q.push_back(from);
        while let Some(u) = q.pop_front() {
            for &ei in &self.out[u] {
                let e = self.list[ei];
                if !allow(e.2) || comp[e.1] != comp[from] {
                    continue;
                }
                if e.1 == to {
                    let mut path = vec![ei];
                    let mut cur = u;
                    while cur != from {
                        let pe = parent[cur]?;
                        path.push(pe);
                        cur = self.list[pe].0;
                    }
                    path.reverse();
                    return Some(path);
                }
                if !seen[e.1] {
                    seen[e.1] = true;
                    parent[e.1] = Some(ei);
                    q.push_back(e.1);
                }
            }
        }
        None
    }

    /// A path `from` -> `to` that never takes two rw edges in a row and
    /// ends with a non-rw edge (so closing it with the rw edge `to -> from`
    /// leaves no adjacent pair).
    fn path_no_adjacent_rw(&self, from: usize, to: usize, comp: &[usize]) -> Option<Vec<usize>> {
        // State = node * 2 + (last edge was rw); the caller took an rw edge
        // into `from`. `parent` holds `edge * 2 + (previous state's flag)`.
        let mut parent: Vec<Option<usize>> = vec![None; self.n * 2];
        let mut seen = vec![false; self.n * 2];
        let start = from * 2 + 1;
        seen[start] = true;
        let mut q = VecDeque::new();
        q.push_back(start);
        while let Some(st) = q.pop_front() {
            let (u, last_rw) = (st / 2, st % 2 == 1);
            for &ei in &self.out[u] {
                let e = self.list[ei];
                let is_rw = e.2 == Ek::Rw;
                if (is_rw && last_rw) || comp[e.1] != comp[from] {
                    continue;
                }
                if e.1 == to && !is_rw {
                    return self.rebuild(&parent, st, start, ei);
                }
                let ns = e.1 * 2 + usize::from(is_rw);
                if !seen[ns] {
                    seen[ns] = true;
                    parent[ns] = Some(ei * 2 + usize::from(last_rw));
                    q.push_back(ns);
                }
            }
        }
        None
    }

    /// The snapshot-isolation relation: `x -> y` for a non-rw edge, and
    /// `x -> z` for a non-rw edge followed by an rw edge. A cycle in it is a
    /// dependency cycle with no two adjacent rw edges.
    fn si_adj(&self) -> Vec<Vec<usize>> {
        let mut a = vec![Vec::new(); self.n];
        for e in &self.list {
            if e.2 == Ek::Rw {
                continue;
            }
            a[e.0].push(e.1);
            for &ri in &self.out[e.1] {
                let r = self.list[ri];
                if r.2 == Ek::Rw {
                    a[e.0].push(r.1);
                }
            }
        }
        a
    }

    fn rebuild(
        &self,
        parent: &[Option<usize>],
        mut st: usize,
        start: usize,
        last_edge: usize,
    ) -> Option<Vec<usize>> {
        let mut path = vec![last_edge];
        while st != start {
            let p = parent[st]?;
            let (ei, prev_rw) = (p / 2, p % 2 == 1);
            path.push(ei);
            st = self.list[ei].0 * 2 + usize::from(prev_rw);
        }
        path.reverse();
        Some(path)
    }
}

/// Strongly connected components (iterative Tarjan); returns each node's
/// component id.
fn scc(adj: &[Vec<usize>]) -> Vec<usize> {
    let n = adj.len();
    let mut index = vec![usize::MAX; n];
    let mut low = vec![0usize; n];
    let mut on = vec![false; n];
    let mut comp = vec![usize::MAX; n];
    let mut stack: Vec<usize> = Vec::new();
    let mut next = 0usize;
    let mut ncomp = 0usize;
    for root in 0..n {
        if index[root] != usize::MAX {
            continue;
        }
        let mut call: Vec<(usize, usize)> = vec![(root, 0)];
        index[root] = next;
        low[root] = next;
        next += 1;
        stack.push(root);
        on[root] = true;
        while let Some(&(v, i)) = call.last() {
            if i < adj[v].len() {
                if let Some(top) = call.last_mut() {
                    top.1 += 1;
                }
                let w = adj[v][i];
                if index[w] == usize::MAX {
                    index[w] = next;
                    low[w] = next;
                    next += 1;
                    stack.push(w);
                    on[w] = true;
                    call.push((w, 0));
                } else if on[w] {
                    low[v] = low[v].min(index[w]);
                }
            } else {
                call.pop();
                if let Some(&(parent, _)) = call.last() {
                    low[parent] = low[parent].min(low[v]);
                }
                if low[v] == index[v] {
                    while let Some(w) = stack.pop() {
                        on[w] = false;
                        comp[w] = ncomp;
                        if w == v {
                            break;
                        }
                    }
                    ncomp += 1;
                }
            }
        }
    }
    comp
}

fn comp_sizes(comp: &[usize]) -> Vec<usize> {
    let mut sizes = vec![0usize; comp.len()];
    for &c in comp {
        sizes[c] += 1;
    }
    sizes
}

fn has_nontrivial(comp: &[usize]) -> bool {
    comp_sizes(comp).iter().any(|&s| s > 1)
}

/// The lowest node index in a component of size > 1.
fn first_in_nontrivial(comp: &[usize]) -> Option<usize> {
    let sizes = comp_sizes(comp);
    (0..comp.len()).find(|&i| sizes[comp[i]] > 1)
}
