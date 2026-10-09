//! The state behind the SSI mutex (§8): per-txn entries, the writer map,
//! the prepare counter and the storage map. Pure data plus the rules that
//! act on it; [`super::Ssi`] owns the mutex and the lock order.

use std::collections::{BTreeMap, BTreeSet};

use nucleus_kv::Key;

use crate::{Ts, TxnError, TxnId};

/// A catalog relation oid (§8.1: relation-level SIREADs are keyed by oid,
/// never by storage id).
pub type RelOid = u32;

/// One SIREAD lock (§8.1). Keys are logical KV keys: logical keys keep their
/// order in the KV (C-Q3s, §2.2), so a `Range` over them covers exactly the
/// entries a scan of that range may iterate, gaps included.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Siread {
    /// A point read of one logical key.
    Point { key: Key },
    /// A scan bound `[lo, hi)`.
    Range { lo: Key, hi: Key },
    /// Every key of the relation, through any storage range mapped to it.
    Relation { rel_oid: RelOid },
}

/// A SER txn's commit progress (§8.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Active,
    Prepared { prepare_seq: u64 },
    Assigned { prepare_seq: u64, commit_ts: Ts },
}

/// One SERIALIZABLE txn's SSI state.
pub(crate) struct Entry {
    pub(crate) snapshot: Ts,
    read_only: bool,
    wrote: bool,
    pub(crate) phase: Phase,
    pub(crate) doomed: bool,
    /// Each SIREAD with the registry `view_counter` read when it was
    /// registered (the earliest registration that still covers it).
    pub(crate) sireads: BTreeMap<Siread, u64>,
    /// Out-edges `self -> target`, each carrying `commit_ts(target)` once
    /// known (§8.2).
    pub(crate) out: BTreeMap<TxnId, Option<Ts>>,
    /// Sources of in-edges `source -> self`.
    ins: BTreeSet<TxnId>,
    pub(crate) eocc: Option<Ts>,
    pub(crate) last_view: Option<u64>,
}

impl Entry {
    fn commit_ts(&self) -> Option<Ts> {
        match self.phase {
            Phase::Assigned { commit_ts, .. } => Some(commit_ts),
            _ => None,
        }
    }

    /// Prepared or assigned: can no longer be a victim (§8.4).
    fn settled(&self) -> bool {
        !matches!(self.phase, Phase::Active)
    }

    /// `commit_ts(X) < commit_ts(self)` with an unassigned `commit_ts(self)`
    /// counting as infinity (§8.5); if so, lower `eocc`.
    fn note_out_commit(&mut self, x_ts: Ts) {
        if self.commit_ts().is_none_or(|own| x_ts < own) {
            self.eocc = Some(self.eocc.map_or(x_ts, |e| e.min(x_ts)));
        }
    }
}

/// Storage the DDL txn retires (§8.6), noted by the catalog.
#[derive(Debug, Clone)]
pub(crate) struct Retired {
    pub(crate) rel_oid: RelOid,
    pub(crate) ranges: Vec<(Key, Key)>,
}

/// Everything the SSI mutex guards.
#[derive(Default)]
pub(crate) struct State {
    pub(crate) entries: BTreeMap<TxnId, Entry>,
    /// `commit_ts -> TxnId` for SERIALIZABLE commits (§8.5).
    pub(crate) writers: BTreeMap<Ts, TxnId>,
    prepare_counter: u64,
    /// Storage range `[lo, hi)` (keyed by `lo`) -> relation oid.
    storage: BTreeMap<Key, (Key, RelOid)>,
}

fn in_range(key: &[u8], lo: &[u8], hi: &[u8]) -> bool {
    lo <= key && key < hi
}

fn overlaps(a: (&[u8], &[u8]), b: (&[u8], &[u8])) -> bool {
    a.0 < b.1 && b.0 < a.1
}

impl State {
    pub(crate) fn begin(&mut self, txn: TxnId, snapshot: Ts, read_only: bool) -> bool {
        if self.entries.contains_key(&txn) {
            return false;
        }
        self.entries.insert(
            txn,
            Entry {
                snapshot,
                read_only,
                wrote: false,
                phase: Phase::Active,
                doomed: false,
                sireads: BTreeMap::new(),
                out: BTreeMap::new(),
                ins: BTreeSet::new(),
                eocc: None,
                last_view: None,
            },
        );
        true
    }

    pub(crate) fn map_storage(&mut self, lo: Key, hi: Key, rel: RelOid) {
        self.storage.insert(lo, (hi, rel));
    }

    pub(crate) fn unmap_storage(&mut self, lo: &[u8], hi: &[u8]) {
        if self
            .storage
            .get(lo)
            .is_some_and(|(h, _)| h.as_slice() == hi)
        {
            self.storage.remove(lo);
        }
    }

    /// The relation whose storage range holds `key`.
    fn storage_rel(&self, key: &[u8]) -> Option<RelOid> {
        let (lo, (hi, rel)) = self.storage.range(..=key.to_vec()).next_back()?;
        in_range(key, lo, hi).then_some(*rel)
    }

    fn covers(&self, sr: &Siread, key: &[u8]) -> bool {
        match sr {
            Siread::Point { key: k } => k.as_slice() == key,
            Siread::Range { lo, hi } => in_range(key, lo, hi),
            Siread::Relation { rel_oid } => self.storage_rel(key) == Some(*rel_oid),
        }
    }

    /// Whether `sr` touches relation `rel`: its own oid, a storage range
    /// mapped to it, or one of `extra` (ranges this DDL txn retires).
    fn on_relation(&self, sr: &Siread, rel: RelOid, extra: &[(Key, Key)]) -> bool {
        let mapped = self
            .storage
            .iter()
            .filter(|(_, (_, r))| *r == rel)
            .map(|(lo, (hi, _))| (lo.as_slice(), hi.as_slice()));
        let mut ranges = mapped.chain(extra.iter().map(|(l, h)| (l.as_slice(), h.as_slice())));
        match sr {
            Siread::Relation { rel_oid } => *rel_oid == rel,
            Siread::Point { key } => ranges.any(|(lo, hi)| in_range(key, lo, hi)),
            Siread::Range { lo, hi } => ranges.any(|r| overlaps((lo, hi), r)),
        }
    }

    pub(crate) fn holds_cover(&self, txn: TxnId, key: &[u8]) -> bool {
        self.entries
            .get(&txn)
            .is_some_and(|e| e.sireads.keys().any(|sr| self.covers(sr, key)))
    }

    /// Adds a SIREAD stamped `stamp`; `false` when `txn` has no entry.
    pub(crate) fn register(&mut self, txn: TxnId, sr: Siread, stamp: u64) -> bool {
        match self.entries.get_mut(&txn) {
            Some(e) => {
                e.sireads.entry(sr).or_insert(stamp);
                true
            }
            None => false,
        }
    }

    /// Records `x -> y` (§8.2): ignored when either has no entry or they
    /// are the same txn.
    pub(crate) fn add_edge(&mut self, x: TxnId, y: TxnId) {
        if x == y || !self.entries.contains_key(&x) {
            return;
        }
        let y_ts = match self.entries.get_mut(&y) {
            Some(ye) => {
                ye.ins.insert(x);
                ye.commit_ts()
            }
            None => return,
        };
        if let Some(xe) = self.entries.get_mut(&x) {
            let slot = xe.out.entry(y).or_insert(None);
            if y_ts.is_some() {
                *slot = y_ts;
            }
            if let Some(c) = y_ts {
                xe.note_out_commit(c);
            }
        }
    }

    /// Reader-side edges of one read (§4, §8.2).
    pub(crate) fn record_read(&mut self, reader: TxnId, edges: &[crate::visibility::RwEdge]) {
        for edge in edges {
            match *edge {
                crate::visibility::RwEdge::ToIntentOwner(t) => self.add_edge(reader, t),
                crate::visibility::RwEdge::ToVersionWriter(ts) => {
                    // No entry: the writer was not SERIALIZABLE (§8.5).
                    if let Some(&w) = self.writers.get(&ts) {
                        self.add_edge(reader, w);
                    }
                }
            }
        }
    }

    /// Writer side (§8.2): every other SER holder of a SIREAD covering
    /// `key` that is concurrent with `writer` gives `holder -> writer`.
    pub(crate) fn on_data_placed(&mut self, writer: TxnId, key: &[u8]) {
        let Some(we) = self.entries.get_mut(&writer) else {
            return;
        };
        we.wrote = true;
        let s_w = we.snapshot;
        let holders: Vec<TxnId> = self
            .entries
            .iter()
            .filter(|(r, e)| {
                **r != writer
                    && concurrent_with(e, s_w)
                    && e.sireads.keys().any(|sr| self.covers(sr, key))
            })
            .map(|(r, _)| *r)
            .collect();
        for r in holders {
            self.add_edge(r, writer);
        }
    }

    /// DDL side (§8.2), at DDL execution: every other SER holder of a
    /// SIREAD on `rel` (any granularity, any storage of it, `extra` = the
    /// ranges this txn retires) that is concurrent gives `holder -> writer`.
    pub(crate) fn on_ddl(&mut self, writer: TxnId, rel: RelOid, extra: &[(Key, Key)]) {
        let Some(we) = self.entries.get_mut(&writer) else {
            return;
        };
        // A DROP/TRUNCATE/rewrite changes data: the writer is not read-only
        // for §8.3.
        we.wrote = true;
        let s_w = we.snapshot;
        let holders: Vec<TxnId> = self
            .entries
            .iter()
            .filter(|(r, e)| {
                **r != writer
                    && concurrent_with(e, s_w)
                    && e.sireads.keys().any(|sr| self.on_relation(sr, rel, extra))
            })
            .map(|(r, _)| *r)
            .collect();
        for r in holders {
            self.add_edge(r, writer);
        }
    }

    /// §3 step 3 / §8.5: the commit thread assigned `ts` to `txn`.
    pub(crate) fn on_assigned(&mut self, txn: TxnId, ts: Ts) {
        let Some(e) = self.entries.get_mut(&txn) else {
            return;
        };
        let prepare_seq = match e.phase {
            Phase::Prepared { prepare_seq } | Phase::Assigned { prepare_seq, .. } => prepare_seq,
            Phase::Active => {
                // Only reachable when a caller drives the observer by hand;
                // the txn still orders after every earlier prepare.
                self.prepare_counter += 1;
                self.prepare_counter
            }
        };
        e.phase = Phase::Assigned {
            prepare_seq,
            commit_ts: ts,
        };
        let sources: Vec<TxnId> = e.ins.iter().copied().collect();
        self.writers.insert(ts, txn);
        for t in sources {
            if let Some(te) = self.entries.get_mut(&t) {
                te.out.insert(txn, Some(ts));
                te.note_out_commit(ts);
            }
        }
    }

    pub(crate) fn prepare(&mut self, txn: TxnId) {
        self.prepare_counter += 1;
        let seq = self.prepare_counter;
        if let Some(e) = self.entries.get_mut(&txn) {
            e.phase = Phase::Prepared { prepare_seq: seq };
        }
    }

    pub(crate) fn doom(&mut self, txn: TxnId) {
        if let Some(e) = self.entries.get_mut(&txn) {
            e.doomed = true;
        }
    }

    /// §8.6 abort: the txn's SIREADs, entry and every edge to or from it.
    pub(crate) fn abort(&mut self, txn: TxnId) {
        let Some(e) = self.entries.remove(&txn) else {
            return;
        };
        for y in e.out.keys() {
            if let Some(ye) = self.entries.get_mut(y) {
                ye.ins.remove(&txn);
            }
        }
        for x in &e.ins {
            if let Some(xe) = self.entries.get_mut(x) {
                xe.out.remove(&txn);
            }
        }
    }

    /// §8.6 promotion: every finer SIREAD inside a retired range becomes a
    /// relation SIREAD (the coarse lock is added before the fine ones go).
    /// A range that only overlaps a retired range keeps its fine lock too.
    pub(crate) fn promote(&mut self, retired: &Retired) {
        for e in self.entries.values_mut() {
            let mut stamp: Option<u64> = None;
            let mut gone = Vec::new();
            for (sr, &st) in &e.sireads {
                let (hit, inside) = match sr {
                    Siread::Point { key } => {
                        let h = retired.ranges.iter().any(|(lo, hi)| in_range(key, lo, hi));
                        (h, h)
                    }
                    Siread::Range { lo, hi } => (
                        retired
                            .ranges
                            .iter()
                            .any(|(l, h)| overlaps((lo, hi), (l, h))),
                        retired.ranges.iter().any(|(l, h)| l <= lo && hi <= h),
                    ),
                    Siread::Relation { .. } => (false, false),
                };
                if hit {
                    stamp = Some(stamp.map_or(st, |s| s.min(st)));
                }
                if inside {
                    gone.push(sr.clone());
                }
            }
            if let Some(s) = stamp {
                let coarse = e
                    .sireads
                    .entry(Siread::Relation {
                        rel_oid: retired.rel_oid,
                    })
                    .or_insert(s);
                *coarse = (*coarse).min(s);
                for sr in gone {
                    e.sireads.remove(&sr);
                }
            }
        }
    }

    /// §8.6 retention: retires every assigned txn T with
    /// `visible_ts >= commit_ts(T)` and no active or prepared SER txn whose
    /// `S < commit_ts(T)`. Edges other txns hold to T stay, with their
    /// commit ts, and so does every `eocc`. Returns how many were retired.
    pub(crate) fn retire(&mut self, visible: Ts) -> usize {
        let min_live = self
            .entries
            .values()
            .filter(|e| !matches!(e.phase, Phase::Assigned { .. }))
            .map(|e| e.snapshot)
            .min();
        let gone: Vec<(TxnId, Ts)> = self
            .entries
            .iter()
            .filter_map(|(t, e)| e.commit_ts().map(|c| (*t, c)))
            .filter(|(_, c)| visible >= *c && min_live.is_none_or(|s| s >= *c))
            .collect();
        for (t, c) in &gone {
            if let Some(e) = self.entries.remove(t) {
                for y in e.out.keys() {
                    if let Some(ye) = self.entries.get_mut(y) {
                        ye.ins.remove(t);
                    }
                }
            }
            if self.writers.get(c) == Some(t) {
                self.writers.remove(c);
            }
        }
        gone.len()
    }

    // ---- §8.3 dangerous structures -------------------------------------

    /// Position in commit order: prepare order (§8.4), the committer taking
    /// the next prepare seq; an active member has none (later than all).
    fn order(&self, t: TxnId, committer: TxnId) -> Option<u64> {
        if t == committer {
            return Some(self.prepare_counter + 1);
        }
        match self.entries.get(&t)?.phase {
            Phase::Active => None,
            Phase::Prepared { prepare_seq } | Phase::Assigned { prepare_seq, .. } => {
                Some(prepare_seq)
            }
        }
    }

    /// T3 commits first among the distinct members.
    fn first(&self, t3: TxnId, members: [TxnId; 3], committer: TxnId) -> bool {
        let Some(o3) = self.order(t3, committer) else {
            return false;
        };
        members
            .iter()
            .filter(|m| **m != t3)
            .all(|m| self.order(*m, committer).is_none_or(|o| o > o3))
    }

    fn is_read_only(&self, t1: TxnId, committer: TxnId) -> bool {
        self.entries
            .get(&t1)
            .is_some_and(|e| e.read_only || (t1 == committer && !e.wrote))
    }

    /// Whether `T1 -> T2 -> T3` is dangerous (§8.3). For an assigned T2
    /// only `eocc(T2)` is consulted and `t3` is ignored.
    fn dangerous(&self, t1: TxnId, t2: TxnId, t3: TxnId, committer: TxnId) -> bool {
        let (Some(e1), Some(e2)) = (self.entries.get(&t1), self.entries.get(&t2)) else {
            return false;
        };
        let ro = self.is_read_only(t1, committer);
        if matches!(e2.phase, Phase::Assigned { .. }) {
            return e2.eocc.is_some_and(|e| !ro || e <= e1.snapshot);
        }
        if !self.entries.contains_key(&t3) || !self.first(t3, [t1, t2, t3], committer) {
            return false;
        }
        if ro {
            // A T3 without a commit ts yet is never `<= S(T1)`.
            let t3_ts = if t3 == committer {
                None
            } else {
                self.entries.get(&t3).and_then(Entry::commit_ts)
            };
            return t3_ts.is_some_and(|c| c <= e1.snapshot);
        }
        true
    }

    /// §8.3 victim: T2 if not prepared, else T1 if not prepared.
    fn victim(&self, t1: TxnId, t2: TxnId, committer: TxnId) -> Option<TxnId> {
        let open = |t: TxnId| t == committer || self.entries.get(&t).is_some_and(|e| !e.settled());
        if open(t2) {
            Some(t2)
        } else if open(t1) {
            Some(t1)
        } else {
            None
        }
    }

    /// The §8.4 check for `t`: every structure containing it, two hops both
    /// ways. Returns the other txns to doom; `Err(SerializationFailure)`
    /// when `t` itself is a victim (nothing changed).
    pub(crate) fn check(&self, t: TxnId) -> Result<BTreeSet<TxnId>, TxnError> {
        let Some(e) = self.entries.get(&t) else {
            return Err(TxnError::Invariant(format!(
                "SERIALIZABLE txn {t:?} has no SSI entry at pre-commit"
            )));
        };
        let mut structures: Vec<(TxnId, TxnId)> = Vec::new(); // (T1, T2)
                                                              // X -> T -> Y.
        for &x in &e.ins {
            for &y in e.out.keys() {
                if self.dangerous(x, t, y, t) {
                    structures.push((x, t));
                }
            }
        }
        // T -> Y -> Z.
        for &y in e.out.keys() {
            let Some(ye) = self.entries.get(&y) else {
                continue;
            };
            if matches!(ye.phase, Phase::Assigned { .. }) {
                if self.dangerous(t, y, y, t) {
                    structures.push((t, y));
                }
                continue;
            }
            for &z in ye.out.keys() {
                if self.dangerous(t, y, z, t) {
                    structures.push((t, y));
                }
            }
        }
        // X -> Y -> T. T must commit first, so an assigned (or prepared) Y
        // never qualifies; skipping assigned Y keeps the eocc rule, which
        // ignores T3, from naming a structure that does not contain T.
        for &y in &e.ins {
            let Some(ye) = self.entries.get(&y) else {
                continue;
            };
            if matches!(ye.phase, Phase::Assigned { .. }) {
                continue;
            }
            for &x in &ye.ins {
                if self.dangerous(x, y, t, t) {
                    structures.push((x, y));
                }
            }
        }
        let mut doom = BTreeSet::new();
        for (t1, t2) in structures {
            match self.victim(t1, t2, t) {
                Some(v) if v == t => return Err(TxnError::SerializationFailure),
                Some(v) => {
                    doom.insert(v);
                }
                None => {
                    return Err(TxnError::Invariant(format!(
                        "dangerous structure {t1:?} -> {t2:?} with no victim at {t:?}'s pre-commit"
                    )))
                }
            }
        }
        Ok(doom)
    }
}

/// §8.2: a holder that committed at or before `S(W)` is not concurrent.
fn concurrent_with(holder: &Entry, s_w: Ts) -> bool {
    !holder.commit_ts().is_some_and(|c| c <= s_w)
}
