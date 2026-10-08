//! G0-commit (C-T0 §11): commit pipeline (§3), snapshot and view registration (§3.1),
//! the read rule (§4), intent removal (§7.3), status truncation (§7.4), crash and boot (§7.2).
//!
//! Workload (fixed): writer W0 writes k0 and k1 with `synchronous_commit=on`; writer W1
//! writes k0 (or, by an initial choice, the disjoint k2 so the two can share a commit
//! group) with `synchronous_commit=off` and may abort. Two snapshot readers read k0 and
//! k1; reader R1 plays the unlatched FK-style read of §3.1. Actors: the commit thread
//! (groups of up to two, status-set and visible-advance as separate steps), the async
//! resolver, W1's inline removal of W0's intent (a second remover), abort cleanup,
//! truncation, at most one crash at any point losing any unsynced suffix, and the boot sweep.
//!
//! A read changes no shared state, so it is not a step: every state checks that each reader
//! with an open view would read both keys correctly at that moment, which covers every point
//! at which a read could run.
//!
//! Every latch section of the spec is one atomic step here; the checked invariants are
//! WAL-ORDER, VIS, ACK, SNAP-ORDER, TRUNC, COUNT, ATOMIC and DURABLE.

use std::collections::BTreeMap;

use crate::Model;

pub type Ts = u8;

/// Seeded bugs of C-T0 §11 owned by this model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// Seed 1: reader opens its view before reading `S`.
    ViewBeforeSnapshot,
    /// Seed 2: status truncated without condition 2 (open views).
    TruncateIgnoresViews,
    /// Seed 7: `visible_ts` advanced before status is set.
    VisibleBeforeStatus,
    /// Seed 8: ack before fsync for `synchronous_commit=on`.
    AckBeforeFsync,
    /// Seed 9: commit records written out of `commit_ts` order.
    RecordsOutOfOrder,
    /// Seed 10: `/sys/txn` delete written before resolution (truncation ignores the count).
    TruncateBeforeResolution,
    /// Seed 13: `intent_count` decremented on a removal that wrote nothing.
    DecrementOnNoop,
    /// Seed 14: view counter taken after the view opens.
    CounterAfterOpen,
    /// Seed 23: `intent_count` decremented before `last_removal_counter` is set.
    DecrementBeforeCounter,
    /// Seed 44: unlatched read without a registered view.
    UnregisteredRead,
    /// Seed 51: older-epoch record truncated without the view-counter condition.
    OlderEpochIgnoresViews,
}

impl Bug {
    pub const ALL: [Bug; 11] = [
        Bug::ViewBeforeSnapshot,
        Bug::TruncateIgnoresViews,
        Bug::VisibleBeforeStatus,
        Bug::AckBeforeFsync,
        Bug::RecordsOutOfOrder,
        Bug::TruncateBeforeResolution,
        Bug::DecrementOnNoop,
        Bug::CounterAfterOpen,
        Bug::DecrementBeforeCounter,
        Bug::UnregisteredRead,
        Bug::OlderEpochIgnoresViews,
    ];

    pub fn seed(self) -> u8 {
        match self {
            Bug::ViewBeforeSnapshot => 1,
            Bug::TruncateIgnoresViews => 2,
            Bug::VisibleBeforeStatus => 7,
            Bug::AckBeforeFsync => 8,
            Bug::RecordsOutOfOrder => 9,
            Bug::TruncateBeforeResolution => 10,
            Bug::DecrementOnNoop => 13,
            Bug::CounterAfterOpen => 14,
            Bug::DecrementBeforeCounter => 23,
            Bug::UnregisteredRead => 44,
            Bug::OlderEpochIgnoresViews => 51,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxnId {
    pub epoch: u8,
    pub n: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Key {
    Intent(u8),
    Version(u8, Ts),
    Rec(TxnId),
    Hwm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Val {
    Intent { owner: TxnId, data: u8 },
    Data(u8),
    Ts(Ts),
}

type Kv = BTreeMap<Key, Val>;
type Batch = Vec<(Key, Option<Val>)>;

struct WriterSpec {
    keys: &'static [u8],
    data: u8,
    sync_on: bool,
    may_abort: bool,
}

const WRITERS: [WriterSpec; 2] = [
    WriterSpec {
        keys: &[0, 1],
        data: 1,
        sync_on: true,
        may_abort: false,
    },
    WriterSpec {
        keys: &[0],
        data: 2,
        sync_on: false,
        may_abort: true,
    },
];
const KEYS: [u8; 3] = [0, 1, 2];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WPhase {
    Active { placed: u8 },
    Requested,
    Acked,
    Aborted,
    Lost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum St {
    Pending,
    Committed(Ts),
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TxnState {
    st: St,
    released: bool,
    count: i8,
    last_removal: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Req {
    w: u8,
    ts: Ts,
    rec_lsn: Option<u32>,
    status: bool,
    visible: bool,
    acked: bool,
    done: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ROp {
    TakeSnapshot,
    Register,
    Open,
    Finish,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Reader {
    pc: u8,
    s: Option<Ts>,
    counter: Option<u32>,
    view: Option<Kv>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    epoch: u8,
    durable: Kv,
    pending: Vec<(u32, Batch)>,
    next_lsn: u32,
    status: BTreeMap<TxnId, TxnState>,
    writers: [WPhase; 2],
    channel: Vec<u8>,
    group: Vec<Req>,
    next_ts: Ts,
    visible_ts: Ts,
    last_rec_ts: Ts,
    resolve_q: Vec<(TxnId, u8)>,
    cleanup_q: Vec<(TxnId, u8)>,
    counter_pending: Vec<TxnId>,
    view_counter: u32,
    readers: [Reader; 2],
    crashed: bool,
    sweep_done: bool,
    sweep_counter: u32,
    /// Ghost: commits by writer, ts and the lsn of the commit record.
    ghost: Vec<(u8, Ts, u32)>,
    /// W1 writes k2 instead of k0: disjoint write sets, so both can share a commit group.
    w1_disjoint: Option<bool>,
    /// Ghost: a violation detected inside a step.
    bad: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Action {
    Choose { w1_disjoint: bool },
    Place(u8),
    InlineRemove(u8),
    Request(u8),
    Abort(u8),
    Drain(u8),
    WriteRecord(u8),
    SetStatus(u8),
    Advance(u8),
    Ack(u8),
    Step5(u8),
    Fsync,
    Resolve(TxnId, u8),
    SetCounter,
    Cleanup(TxnId, u8),
    Truncate(TxnId),
    Reader(u8),
    Crash { keep: u8 },
    Sweep(u8),
    SweepDone,
}

pub struct CommitModel {
    pub bug: Option<Bug>,
}

fn wid(epoch: u8, w: u8) -> TxnId {
    TxnId { epoch, n: w }
}

impl State {
    fn latest(&self) -> Kv {
        let mut kv = self.durable.clone();
        for (_, b) in &self.pending {
            apply(&mut kv, b);
        }
        kv
    }

    fn keys(&self, w: u8) -> &'static [u8] {
        if w == 1 && self.w1_disjoint == Some(true) {
            &[2]
        } else {
            WRITERS[w as usize].keys
        }
    }

    fn write(&mut self, b: Batch) -> u32 {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        self.pending.push((lsn, b));
        lsn
    }

    fn is_durable(&self, lsn: u32) -> bool {
        !self.pending.iter().any(|(l, _)| *l == lsn)
    }

    fn visible_commit(&self, t: TxnId) -> bool {
        matches!(self.status.get(&t), Some(TxnState { st: St::Committed(c), .. }) if *c <= self.visible_ts)
    }

    fn min_view(&self) -> u32 {
        self.readers
            .iter()
            .filter(|r| r.view.is_some() || r.counter.is_some())
            .filter_map(|r| r.counter)
            .min()
            .unwrap_or(u32::MAX)
    }

    fn expected(&self, k: u8, s: Ts) -> Option<u8> {
        self.ghost
            .iter()
            .filter(|(w, ts, _)| *ts <= s && self.keys(*w).contains(&k))
            .max_by_key(|(_, ts, _)| *ts)
            .map(|(w, _, _)| WRITERS[*w as usize].data)
    }

    /// §4 read of `k` at snapshot `s` from `kv`, with the current status table.
    fn read(&self, kv: &Kv, k: u8, s: Ts) -> Result<Option<u8>, String> {
        if let Some(Val::Intent { owner, data }) = kv.get(&Key::Intent(k)) {
            match self.status.get(owner) {
                None if owner.epoch == self.epoch => {
                    return Err(format!(
                        "I-TRUNC: intent on k{k} of {owner:?} has no status entry"
                    ))
                }
                Some(TxnState {
                    st: St::Committed(c),
                    ..
                }) if *c <= s => return Ok(Some(*data)),
                _ => {}
            }
        }
        Ok(kv
            .range(Key::Version(k, 0)..=Key::Version(k, s))
            .next_back()
            .and_then(|(_, v)| if let Val::Data(d) = v { Some(*d) } else { None }))
    }

    /// §7.3 steps 2-4 for intent `k` expected to be owned by `t`.
    fn remove(&mut self, t: TxnId, k: u8, bug: Option<Bug>, split_counter: bool) {
        let owned = matches!(self.latest().get(&Key::Intent(k)), Some(Val::Intent { owner, .. }) if *owner == t);
        if owned {
            let mut b: Batch = vec![(Key::Intent(k), None)];
            if let Some(TxnState {
                st: St::Committed(c),
                ..
            }) = self.status.get(&t)
            {
                let data = match self.latest().get(&Key::Intent(k)) {
                    Some(Val::Intent { data, .. }) => *data,
                    _ => 0,
                };
                b.push((Key::Version(k, *c), Some(Val::Data(data))));
            }
            self.write(b);
        }
        if owned || bug == Some(Bug::DecrementOnNoop) {
            let vc = self.view_counter;
            let current = t.epoch == self.epoch;
            if let Some(ts) = self.status.get_mut(&t) {
                if split_counter {
                    self.counter_pending.push(t);
                } else {
                    ts.last_removal = vc;
                }
                if current {
                    ts.count -= 1;
                }
            }
        }
    }
}

impl State {
    /// Canonical form for deduplication. LSNs only matter as "still pending, in this order"
    /// versus durable, and view counters only through `<`/`==` comparisons with each other,
    /// so both are renumbered by rank. Every guard and check sees the same answers.
    fn normalize(&mut self) {
        let lsns: Vec<u32> = self.pending.iter().map(|(l, _)| *l).collect();
        let map_lsn = |l: u32| match lsns.iter().position(|&p| p == l) {
            Some(i) => i as u32 + 1,
            None => 0,
        };
        for q in &mut self.group {
            q.rec_lsn = q.rec_lsn.map(map_lsn);
        }
        for g in &mut self.ghost {
            g.2 = map_lsn(g.2);
        }
        for (i, (l, _)) in self.pending.iter_mut().enumerate() {
            *l = i as u32 + 1;
        }
        self.next_lsn = lsns.len() as u32 + 1;

        let mut vals: Vec<u32> = vec![0, self.view_counter, self.sweep_counter];
        vals.extend(self.readers.iter().filter_map(|r| r.counter));
        vals.extend(self.status.values().map(|t| t.last_removal));
        vals.sort_unstable();
        vals.dedup();
        let rank = |v: u32| vals.binary_search(&v).unwrap_or(0) as u32;
        self.view_counter = rank(self.view_counter);
        self.sweep_counter = rank(self.sweep_counter);
        for r in &mut self.readers {
            r.counter = r.counter.map(rank);
        }
        for t in self.status.values_mut() {
            t.last_removal = rank(t.last_removal);
        }
    }
}

fn apply(kv: &mut Kv, b: &Batch) {
    for (k, v) in b {
        match v {
            Some(v) => kv.insert(*k, *v),
            None => kv.remove(k),
        };
    }
}

impl CommitModel {
    fn reader_ops(&self, r: u8) -> Vec<ROp> {
        use ROp::*;
        let setup = match (r, self.bug) {
            (0, Some(Bug::ViewBeforeSnapshot)) => vec![Open, TakeSnapshot, Register],
            (0, Some(Bug::CounterAfterOpen)) => vec![TakeSnapshot, Open, Register],
            (1, Some(Bug::UnregisteredRead)) => vec![TakeSnapshot, Open],
            _ => vec![TakeSnapshot, Register, Open],
        };
        let mut ops = setup;
        ops.push(Finish);
        ops
    }

    fn needs_fsync(&self, s: &State, i: usize) -> bool {
        self.bug != Some(Bug::AckBeforeFsync)
            && s.group[..=i].iter().any(|q| WRITERS[q.w as usize].sync_on)
    }

    fn rec_durable(&self, s: &State, i: usize) -> bool {
        s.group[i].rec_lsn.is_some_and(|l| s.is_durable(l))
    }
}

impl Model for CommitModel {
    type State = State;
    type Action = Action;

    fn init(&self) -> State {
        let mut status = BTreeMap::new();
        for w in 0..2 {
            status.insert(
                wid(0, w),
                TxnState {
                    st: St::Pending,
                    released: false,
                    count: 0,
                    last_removal: 0,
                },
            );
        }
        let reader = Reader {
            pc: 0,
            s: None,
            counter: None,
            view: None,
        };
        State {
            epoch: 0,
            durable: Kv::new(),
            pending: Vec::new(),
            next_lsn: 1,
            status,
            writers: [WPhase::Active { placed: 0 }; 2],
            channel: Vec::new(),
            group: Vec::new(),
            next_ts: 1,
            visible_ts: 0,
            last_rec_ts: 0,
            resolve_q: Vec::new(),
            cleanup_q: Vec::new(),
            counter_pending: Vec::new(),
            view_counter: 0,
            readers: [reader.clone(), reader],
            crashed: false,
            sweep_done: false,
            sweep_counter: 0,
            ghost: Vec::new(),
            w1_disjoint: None,
            bad: None,
        }
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        if s.bad.is_some() {
            return;
        }
        if s.w1_disjoint.is_none() {
            out.push(Action::Choose { w1_disjoint: false });
            out.push(Action::Choose { w1_disjoint: true });
            return;
        }
        let latest = s.latest();
        for w in 0..2u8 {
            let spec = &WRITERS[w as usize];
            let keys = s.keys(w);
            match s.writers[w as usize] {
                WPhase::Active { placed } if (placed as usize) < keys.len() => {
                    let k = keys[placed as usize];
                    match latest.get(&Key::Intent(k)) {
                        None => out.push(Action::Place(w)),
                        Some(Val::Intent { owner, .. }) => {
                            let ended = s.visible_commit(*owner)
                                || matches!(
                                    s.status.get(owner),
                                    Some(TxnState {
                                        st: St::Aborted,
                                        ..
                                    })
                                );
                            if ended {
                                out.push(Action::InlineRemove(w));
                            }
                        }
                        Some(_) => {}
                    }
                    if spec.may_abort {
                        out.push(Action::Abort(w));
                    }
                }
                WPhase::Active { .. } => {
                    out.push(Action::Request(w));
                    if spec.may_abort {
                        out.push(Action::Abort(w));
                    }
                }
                _ => {}
            }
        }
        if s.group.is_empty() && !s.channel.is_empty() {
            out.push(Action::Drain(1));
            if s.channel.len() >= 2 {
                out.push(Action::Drain(2));
            }
        }
        let unwritten: Vec<usize> = (0..s.group.len())
            .filter(|&i| s.group[i].rec_lsn.is_none())
            .collect();
        let next_rec = if self.bug == Some(Bug::RecordsOutOfOrder) {
            unwritten.last()
        } else {
            unwritten.first()
        };
        if let Some(&i) = next_rec {
            out.push(Action::WriteRecord(i as u8));
        }
        for i in 0..s.group.len() {
            let q = s.group[i];
            let prev_ok = |f: fn(&Req) -> bool| s.group[..i].iter().all(f);
            let fsync_ok = !self.needs_fsync(s, i) || self.rec_durable(s, i);
            if q.rec_lsn.is_some() && !q.status && prev_ok(|p| p.status) && fsync_ok {
                out.push(Action::SetStatus(i as u8));
            }
            let status_ok = q.status || self.bug == Some(Bug::VisibleBeforeStatus);
            if q.rec_lsn.is_some() && status_ok && !q.visible && prev_ok(|p| p.visible) && fsync_ok
            {
                out.push(Action::Advance(i as u8));
            }
            if q.visible && !q.acked && fsync_ok {
                out.push(Action::Ack(i as u8));
            }
            if q.visible && q.status && !q.done {
                out.push(Action::Step5(i as u8));
            }
        }
        if !s.pending.is_empty() {
            out.push(Action::Fsync);
        }
        for (t, mask) in &s.resolve_q {
            for k in KEYS {
                if mask & (1 << k) != 0 {
                    out.push(Action::Resolve(*t, k));
                }
            }
        }
        if !s.counter_pending.is_empty() {
            out.push(Action::SetCounter);
        }
        for (t, mask) in &s.cleanup_q {
            for k in KEYS {
                if mask & (1 << k) != 0 {
                    out.push(Action::Cleanup(*t, k));
                }
            }
        }
        for (t, ts) in &s.status {
            let older = t.epoch < s.epoch;
            let ok = if older {
                s.sweep_done
                    && (self.bug == Some(Bug::OlderEpochIgnoresViews)
                        || s.min_view() > ts.last_removal.max(s.sweep_counter))
            } else {
                ts.released
                    && (ts.count == 0 || self.bug == Some(Bug::TruncateBeforeResolution))
                    && (s.min_view() > ts.last_removal
                        || self.bug == Some(Bug::TruncateIgnoresViews))
            };
            if ok {
                out.push(Action::Truncate(*t));
            }
        }
        for r in 0..2u8 {
            if (s.readers[r as usize].pc as usize) < self.reader_ops(r).len() {
                out.push(Action::Reader(r));
            }
        }
        if !s.crashed {
            for keep in 0..=s.pending.len() {
                out.push(Action::Crash { keep: keep as u8 });
            }
        }
        if s.crashed && !s.sweep_done {
            let mut any = false;
            for k in KEYS {
                if let Some(Val::Intent { owner, .. }) = latest.get(&Key::Intent(k)) {
                    if owner.epoch < s.epoch {
                        out.push(Action::Sweep(k));
                        any = true;
                    }
                }
            }
            if !any {
                out.push(Action::SweepDone);
            }
        }
    }

    fn next(&self, s: &State, a: &Action) -> State {
        let mut s = s.clone();
        let latest = s.latest();
        match *a {
            Action::Choose { w1_disjoint } => s.w1_disjoint = Some(w1_disjoint),
            Action::Place(w) => {
                let spec = &WRITERS[w as usize];
                if let WPhase::Active { placed } = s.writers[w as usize] {
                    let k = s.keys(w)[placed as usize];
                    let me = wid(0, w);
                    if let Some(ts) = s.status.get_mut(&me) {
                        ts.count += 1;
                    }
                    s.write(vec![(
                        Key::Intent(k),
                        Some(Val::Intent {
                            owner: me,
                            data: spec.data,
                        }),
                    )]);
                    s.writers[w as usize] = WPhase::Active { placed: placed + 1 };
                }
            }
            Action::InlineRemove(w) => {
                if let WPhase::Active { placed } = s.writers[w as usize] {
                    let k = s.keys(w)[placed as usize];
                    if let Some(Val::Intent { owner, .. }) = latest.get(&Key::Intent(k)) {
                        let owner = *owner;
                        s.remove(owner, k, self.bug, false);
                    }
                }
            }
            Action::Request(w) => {
                s.writers[w as usize] = WPhase::Requested;
                s.channel.push(w);
            }
            Action::Abort(w) => {
                let placed = match s.writers[w as usize] {
                    WPhase::Active { placed } => placed,
                    _ => 0,
                };
                let me = wid(0, w);
                if let Some(ts) = s.status.get_mut(&me) {
                    ts.st = St::Aborted;
                    ts.released = true;
                }
                let mask = s.keys(w)[..placed as usize]
                    .iter()
                    .fold(0u8, |m, k| m | (1 << k));
                if mask != 0 {
                    s.cleanup_q.push((me, mask));
                }
                s.writers[w as usize] = WPhase::Aborted;
            }
            Action::Drain(n) => {
                for _ in 0..n {
                    let w = s.channel.remove(0);
                    let ts = s.next_ts;
                    s.next_ts += 1;
                    s.group.push(Req {
                        w,
                        ts,
                        rec_lsn: None,
                        status: false,
                        visible: false,
                        acked: false,
                        done: false,
                    });
                }
            }
            Action::WriteRecord(i) => {
                let q = s.group[i as usize];
                if q.ts < s.last_rec_ts {
                    s.bad = Some(format!(
                        "I-WAL-ORDER: commit record ts {} written after ts {}",
                        q.ts, s.last_rec_ts
                    ));
                }
                s.last_rec_ts = s.last_rec_ts.max(q.ts);
                let hwm = match latest.get(&Key::Hwm) {
                    Some(Val::Ts(h)) => (*h).max(q.ts),
                    _ => q.ts,
                };
                let lsn = s.write(vec![
                    (Key::Rec(wid(0, q.w)), Some(Val::Ts(q.ts))),
                    (Key::Hwm, Some(Val::Ts(hwm))),
                ]);
                s.group[i as usize].rec_lsn = Some(lsn);
                s.ghost.push((q.w, q.ts, lsn));
            }
            Action::SetStatus(i) => {
                let q = s.group[i as usize];
                if let Some(ts) = s.status.get_mut(&wid(0, q.w)) {
                    ts.st = St::Committed(q.ts);
                }
                s.group[i as usize].status = true;
            }
            Action::Advance(i) => {
                s.visible_ts = s.group[i as usize].ts;
                s.group[i as usize].visible = true;
            }
            Action::Ack(i) => {
                let q = s.group[i as usize];
                s.group[i as usize].acked = true;
                s.writers[q.w as usize] = WPhase::Acked;
                if s.group.iter().all(|q| q.acked && q.done) {
                    s.group.clear();
                }
            }
            Action::Step5(i) => {
                let q = s.group[i as usize];
                let me = wid(0, q.w);
                if let Some(ts) = s.status.get_mut(&me) {
                    ts.released = true;
                }
                let mask = s.keys(q.w).iter().fold(0u8, |m, k| m | (1 << k));
                s.resolve_q.push((me, mask));
                s.group[i as usize].done = true;
                if s.group.iter().all(|q| q.acked && q.done) {
                    s.group.clear();
                }
            }
            Action::Fsync => {
                let pending = std::mem::take(&mut s.pending);
                for (_, b) in &pending {
                    apply(&mut s.durable, b);
                }
            }
            Action::Resolve(t, k) | Action::Cleanup(t, k) => {
                let split = matches!(a, Action::Resolve(..))
                    && self.bug == Some(Bug::DecrementBeforeCounter);
                s.remove(t, k, self.bug, split);
                let q = if matches!(a, Action::Resolve(..)) {
                    &mut s.resolve_q
                } else {
                    &mut s.cleanup_q
                };
                if let Some(pos) = q.iter().position(|(x, _)| *x == t) {
                    q[pos].1 &= !(1 << k);
                    if q[pos].1 == 0 {
                        q.remove(pos);
                    }
                }
            }
            Action::SetCounter => {
                let t = s.counter_pending.remove(0);
                let vc = s.view_counter;
                if let Some(ts) = s.status.get_mut(&t) {
                    ts.last_removal = vc;
                }
            }
            Action::Truncate(t) => {
                let committed = matches!(
                    s.status.get(&t),
                    Some(TxnState {
                        st: St::Committed(_),
                        ..
                    })
                );
                s.status.remove(&t);
                if committed {
                    s.write(vec![(Key::Rec(t), None)]);
                }
            }
            Action::Reader(r) => {
                let ops = self.reader_ops(r);
                let op = ops[s.readers[r as usize].pc as usize];
                match op {
                    ROp::TakeSnapshot => s.readers[r as usize].s = Some(s.visible_ts),
                    ROp::Register => {
                        s.view_counter += 1;
                        s.readers[r as usize].counter = Some(s.view_counter);
                    }
                    ROp::Open => s.readers[r as usize].view = Some(latest),
                    ROp::Finish => {
                        s.readers[r as usize].counter = None;
                        s.readers[r as usize].view = None;
                    }
                }
                s.readers[r as usize].pc += 1;
            }
            Action::Crash { keep } => {
                let lost: Vec<u32> = s.pending[keep as usize..].iter().map(|(l, _)| *l).collect();
                let kept: Vec<(u32, Batch)> = s.pending[..keep as usize].to_vec();
                for (_, b) in &kept {
                    apply(&mut s.durable, b);
                }
                s.pending.clear();
                for q in &s.group {
                    let on = WRITERS[q.w as usize].sync_on;
                    if q.acked && on && q.rec_lsn.is_some_and(|l| lost.contains(&l)) {
                        s.bad = Some(format!(
                            "I-DURABLE: acked synchronous commit of W{} lost in crash",
                            q.w
                        ));
                    }
                }
                for (w, spec) in WRITERS.iter().enumerate() {
                    if s.writers[w] == WPhase::Acked && spec.sync_on {
                        let lsn = s.ghost.iter().find(|g| g.0 as usize == w).map(|g| g.2);
                        if lsn.is_some_and(|l| lost.contains(&l)) {
                            s.bad = Some(format!(
                                "I-DURABLE: acked synchronous commit of W{w} lost in crash"
                            ));
                        }
                    }
                }
                s.ghost.retain(|g| !lost.contains(&g.2));
                s.crashed = true;
                s.epoch = 1;
                s.status.clear();
                for (k, v) in &s.durable {
                    if let (Key::Rec(t), Val::Ts(c)) = (k, v) {
                        s.status.insert(
                            *t,
                            TxnState {
                                st: St::Committed(*c),
                                released: true,
                                count: 0,
                                last_removal: 0,
                            },
                        );
                    }
                }
                s.visible_ts = match s.durable.get(&Key::Hwm) {
                    Some(Val::Ts(h)) => *h,
                    _ => 0,
                };
                s.next_ts = s.visible_ts + 1;
                for w in 0..2 {
                    if s.writers[w] != WPhase::Aborted {
                        s.writers[w] = WPhase::Lost;
                    }
                }
                s.channel.clear();
                s.group.clear();
                s.resolve_q.clear();
                s.cleanup_q.clear();
                s.counter_pending.clear();
                for r in 0..2u8 {
                    let done = s.readers[r as usize].pc as usize == self.reader_ops(r).len();
                    if !done {
                        s.readers[r as usize] = Reader {
                            pc: 0,
                            s: None,
                            counter: None,
                            view: None,
                        };
                    }
                }
            }
            Action::Sweep(k) => {
                if let Some(Val::Intent { owner, .. }) = latest.get(&Key::Intent(k)) {
                    let owner = *owner;
                    s.remove(owner, k, None, false);
                }
            }
            Action::SweepDone => {
                s.sweep_done = true;
                s.sweep_counter = s.view_counter;
            }
        }
        s.normalize();
        s
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if let Some(b) = &s.bad {
            return Err(b.clone());
        }
        for q in &s.group {
            if q.visible && !q.status {
                return Err(format!(
                    "I-VIS: visible_ts reached ts {} of W{} before its status was set",
                    q.ts, q.w
                ));
            }
            if q.acked && s.visible_ts < q.ts {
                return Err(format!("I-ACK: W{} acked before visible", q.w));
            }
        }
        for (r, rd) in s.readers.iter().enumerate() {
            if let (Some(snap), Some(view)) = (rd.s, &rd.view) {
                for k in KEYS {
                    let got = s
                        .read(view, k, snap)
                        .map_err(|e| format!("reader R{r}: {e}"))?;
                    let want = s.expected(k, snap);
                    if got != want {
                        return Err(format!(
                            "I-SNAP-ORDER/I-ATOMIC: reader R{r} read k{k} at S={snap}: got {got:?}, want {want:?}"
                        ));
                    }
                }
            }
        }
        let latest = s.latest();
        for (t, ts) in &s.status {
            if t.epoch == s.epoch {
                let n = latest
                    .values()
                    .filter(|v| matches!(v, Val::Intent { owner, .. } if owner == t))
                    .count() as i8;
                if ts.count != n && !s.counter_pending.contains(t) {
                    return Err(format!(
                        "I-COUNT: intent_count({t:?}) = {}, KV holds {n}",
                        ts.count
                    ));
                }
            }
        }
        for k in KEYS {
            let got = s.read(&latest, k, s.visible_ts)?;
            let want = s.expected(k, s.visible_ts);
            if got != want {
                return Err(format!(
                    "I-ATOMIC: k{k} at visible_ts={} reads {got:?}, committed state says {want:?}",
                    s.visible_ts
                ));
            }
        }
        Ok(())
    }

    fn is_final(&self, s: &State) -> bool {
        s.bad.is_none()
    }
}
