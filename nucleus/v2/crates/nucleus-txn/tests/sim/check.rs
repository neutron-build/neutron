//! C-SIM work item 6: the invariants, checked after every step and at every
//! probe point against ghost state (`ghost.rs`) and public read paths. A
//! violation stops the run with the seed, config and trace.
//!
//! Oracles never read crate internals to decide a verdict: the ghost state
//! (what the simulator asked for and what the API returned) and public
//! reads (registered views, `status.entry`, the KV record) are the only
//! inputs. `wait_edges()` is used only for the end-of-run emptiness
//! assertion, never as an expected answer.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

use nucleus_txn::boot::Core;
use nucleus_txn::commit::{ProbePoint, SyncCommit};
use nucleus_txn::encoding::{
    decode_intent, decode_version, end_key, intent_key, parse_key, parse_sys_txn_key,
    sys_txn_prefix, sys_txn_prefix_end, Entry,
};
use nucleus_txn::read::{self, NoSsi};
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::RowLocks as _;
use nucleus_txn::{LayerData, RowLockMode, Seq, Ts, TxnId, TxnStatus};

use super::ghost::{self, GWrite};
use super::kv::{commit_record_of, op_is_data, RecEvent};
use super::{Sim, Stmt};

/// The value a write leaves on the key, if any.
fn write_value(w: &GWrite) -> Option<Vec<u8>> {
    match w {
        GWrite::Set(v) | GWrite::Inc { wrote: v } => Some(v.clone()),
        GWrite::Delete => None,
    }
}

/// Checker watermarks: amortised record walking, the dv tracker, I-SER
/// laziness, the pending post-crash battery.
pub struct Checker {
    /// KV record events already checked.
    rec_upto: usize,
    last_commit_ts: Option<Ts>,
    /// The era `last_commit_ts` belongs to (ts values restart per era).
    last_commit_era: u32,
    commit_pos: BTreeMap<TxnId, usize>,
    /// Positions of `sync_wal` and `Durability::Yes` writes, ascending.
    sync_positions: Vec<usize>,
    data_ops: u64,
    dv_prev: u64,
    i_ser_at: usize,
    /// The crash checks run at the next boot.
    pub pending_crash_checks: bool,
}

impl Checker {
    pub fn new() -> Checker {
        Checker {
            rec_upto: 0,
            last_commit_ts: None,
            last_commit_era: 0,
            commit_pos: BTreeMap::new(),
            sync_positions: Vec::new(),
            data_ops: 0,
            dv_prev: 0,
            i_ser_at: 0,
            pending_crash_checks: false,
        }
    }

    pub fn new_era(&mut self, _core: &Core<super::SimKv>) {}

    fn sync_after(&self, pos: usize) -> bool {
        self.sync_positions.iter().any(|&p| p > pos)
    }
}

/// After every step: the battery.
pub fn post_step(sim: &mut Sim) {
    battery(sim);
}

/// At every probe point: the same battery, plus the `Acked(i)`/`Step5Done(i)`
/// points feed the ghost directly — the pipeline sent the ack even though
/// the session may not have polled its ticket yet, and the resolver may
/// truncate the entry before it does.
pub fn probe_point(sim: &mut Sim, point: ProbePoint) {
    match point {
        ProbePoint::Acked(i) => {
            if let Some(tid) = sim.current_group.get(i).copied() {
                let ts = match sim.core.status.entry(tid) {
                    Some(e) => match e.status {
                        TxnStatus::Committed(ts) => Some(ts),
                        _ => None,
                    },
                    None => None,
                };
                if let Some(ts) = ts {
                    let released = sim.core.status.entry(tid).is_some_and(|e| e.released);
                    let has_writes = sim
                        .ghost
                        .txns
                        .get(&tid)
                        .is_some_and(|t| !t.writes.is_empty());
                    if let Some(t) = sim.ghost.txns.get_mut(&tid) {
                        if t.outcome == ghost::Outcome::Active {
                            t.outcome = ghost::Outcome::Acked(ts);
                            t.released_seen |= released;
                        }
                    }
                    if ts != Ts::ZERO {
                        sim.ghost.commit_at(tid, ts);
                    }
                    if has_writes {
                        sim.resolve_pending = true;
                    }
                    // I-ACK at the moment the pipeline sends the ack.
                    if sim.core.visible_ts() < ts {
                        sim.violate(
                            "I-ACK",
                            format!(
                                "ack of {tid:?} at ts {ts:?} sent with visible_ts {:?}",
                                sim.core.visible_ts()
                            ),
                        );
                        return;
                    }
                    if ts != Ts::ZERO {
                        on_ack(sim, &tid);
                        if sim.violation.is_some() {
                            return;
                        }
                    }
                }
            }
        }
        ProbePoint::Step5Done(i) => {
            if let Some(tid) = sim.current_group.get(i).copied() {
                if sim
                    .ghost
                    .txns
                    .get(&tid)
                    .is_some_and(|t| !t.writes.is_empty())
                {
                    sim.resolve_pending = true;
                }
            }
        }
        _ => {}
    }
    battery(sim);
}

fn battery(sim: &mut Sim) {
    walk_record(sim);
    observe_statuses(sim);
    let dv = sim.chk.data_ops
        + sim.ghost.committed.len() as u64
        + sim
            .ghost
            .txns
            .values()
            .filter(|t| t.outcome == ghost::Outcome::Aborted)
            .count() as u64;
    let changed = dv != sim.chk.dv_prev;
    sim.chk.dv_prev = dv;
    if changed {
        for s in &mut sim.sessions {
            s.streak = 0;
        }
        full_state_checks(sim);
        if sim.violation.is_some() {
            return;
        }
    } else {
        // I-PROGRESS: no session retries more than R times without another
        // actor changing committed or intent state in between.
        let r = sim.cfg.progress_r;
        if let Some(s) = sim.sessions.iter().find(|s| s.streak > r) {
            let id = s.id;
            let streak = s.streak;
            sim.violate(
                "I-PROGRESS",
                format!(
                    "session {id} took {streak} consecutive Again/Epq steps without a state change"
                ),
            );
            return;
        }
    }
    i_vis(sim);
    if sim.violation.is_some() {
        return;
    }
    i_live_a(sim);
    if sim.violation.is_some() {
        return;
    }
    if sim.cfg.check_i_ser && sim.ghost.committed.len() > sim.chk.i_ser_at {
        sim.chk.i_ser_at = sim.ghost.committed.len();
        i_ser(sim);
    }
}

// ---------------------------------------------------------------------------
// Ghost updates from the KV record and the status table
// ---------------------------------------------------------------------------

/// Walks the new KV record events: I-WAL-ORDER (commit records in ts order;
/// every intent write of a txn before its commit record) and the dv
/// data-op count.
fn walk_record(sim: &mut Sim) {
    let events = sim.kv.record();
    let mut i = sim.chk.rec_upto;
    while i < events.len() {
        match &events[i] {
            RecEvent::Write { ops, synced } => {
                if *synced {
                    sim.chk.sync_positions.push(i);
                }
                for op in ops {
                    if op_is_data(op) {
                        sim.chk.data_ops += 1;
                    }
                    if let Some((id, ts)) = commit_record_of(op) {
                        // Per era: ts values restart at every reboot (a
                        // later era may legally reuse the ts of a commit
                        // whose record the crash dropped together with its
                        // unsynced /sys/ts_hwm reservation, §3 boot).
                        let era = sim.kv.era_at(i);
                        if era != sim.chk.last_commit_era {
                            sim.chk.last_commit_era = era;
                            sim.chk.last_commit_ts = None;
                        }
                        if Some(ts) <= sim.chk.last_commit_ts {
                            let last = sim.chk.last_commit_ts;
                            sim.violate(
                                "I-WAL-ORDER",
                                format!(
                                    "commit record of {id:?} at ts {ts:?} out of order (last {last:?})"
                                ),
                            );
                            return;
                        }
                        sim.chk.last_commit_ts = Some(ts);
                        sim.chk.commit_pos.insert(id, i);
                        sim.ghost.note_ts(id, ts);
                        // I-WAL-ORDER: every intent write of the txn
                        // preceded its commit record.
                        let bound = sim.ghost.txns.get(&id).map_or(0, |t| t.placement_bound);
                        if bound > i {
                            sim.violate(
                                "I-WAL-ORDER",
                                format!(
                                    "{id:?} wrote an intent (record bound {bound}) after its commit record at {i}"
                                ),
                            );
                            return;
                        }
                    }
                }
            }
            RecEvent::Sync => sim.chk.sync_positions.push(i),
        }
        i += 1;
    }
    sim.chk.rec_upto = events.len();
}

/// Observes every live current-epoch ghost txn's status entry: committed
/// txns join the history (with their ts), released flags are remembered,
/// vanished entries are I-TRUNC violations unless the txn ended.
fn observe_statuses(sim: &mut Sim) {
    let epoch = sim.core.epoch();
    let ids: Vec<TxnId> = sim
        .ghost
        .txns
        .iter()
        .filter(|(id, t)| id.epoch == epoch && !t.truncated_seen)
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        match sim.core.status.entry(id) {
            None => {
                let active = sim
                    .ghost
                    .txns
                    .get(&id)
                    .is_some_and(|t| t.outcome == ghost::Outcome::Active);
                if active {
                    sim.violate(
                        "I-TRUNC",
                        format!("the status entry of active txn {id:?} vanished"),
                    );
                    return;
                }
                if let Some(t) = sim.ghost.txns.get_mut(&id) {
                    t.truncated_seen = true;
                }
            }
            Some(e) => {
                match e.status {
                    TxnStatus::Committed(ts) => sim.ghost.commit_at(id, ts),
                    TxnStatus::Aborted => {
                        let active = sim
                            .ghost
                            .txns
                            .get(&id)
                            .is_some_and(|t| t.outcome == ghost::Outcome::Active);
                        if active {
                            sim.violate(
                                "STATUS",
                                format!("{id:?} was aborted by someone other than its session"),
                            );
                            return;
                        }
                    }
                    TxnStatus::Pending => {}
                }
                if e.released {
                    if let Some(t) = sim.ghost.txns.get_mut(&id) {
                        t.released_seen = true;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// I-VIS and I-ACK
// ---------------------------------------------------------------------------

fn i_vis(sim: &mut Sim) {
    let visible = sim.core.visible_ts();
    let ids: Vec<TxnId> = sim
        .ghost
        .txns
        .iter()
        .filter(|(_, t)| {
            matches!(
                t.outcome,
                ghost::Outcome::Active
                    | ghost::Outcome::Acked(_)
                    | ghost::Outcome::CommittedByRecord(_)
            ) && t.known_ts.is_some()
        })
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        let Some(ts) = sim.ghost.txns.get(&id).and_then(|t| t.known_ts) else {
            continue;
        };
        if visible < ts {
            continue;
        }
        match sim.core.status.entry(id) {
            Some(e) => {
                if e.status != TxnStatus::Committed(ts) {
                    let st = e.status;
                    sim.violate(
                        "I-VIS",
                        format!(
                            "visible_ts {visible:?} >= {ts:?} of {id:?} but its status is {st:?}"
                        ),
                    );
                    return;
                }
            }
            None => {
                let ok = sim
                    .ghost
                    .txns
                    .get(&id)
                    .is_some_and(|t| t.truncated_seen && t.released_seen);
                if !ok {
                    sim.violate(
                        "I-VIS",
                        format!(
                            "{id:?} (ts {ts:?} <= visible {visible:?}) has no status entry and was not seen released before truncation (outcome {:?})",
                            sim.ghost.txns.get(&id).map(|t| &t.outcome),
                        ),
                    );
                    return;
                }
            }
        }
    }
}

/// I-ACK's durability half: an `On` ack is preceded, in the KV record, by a
/// `sync_wal` (or `Durability::Yes` write) after the txn's commit record.
/// Called when the session observes the ack and at the `Acked(i)` probe
/// point.
pub fn on_ack(sim: &mut Sim, id: &TxnId) {
    let sync = sim.ghost.txns.get(id).and_then(|t| t.sync);
    if sync != Some(SyncCommit::On) {
        return;
    }
    let pos = sim.chk.commit_pos.get(id).copied();
    match pos {
        None => {
            sim.violate(
                "I-ACK",
                format!("an On ack of {id:?} without a commit record in the KV record"),
            );
        }
        Some(pos) => {
            if !sim.chk.sync_after(pos) {
                sim.violate(
                    "I-ACK",
                    format!("an On ack of {id:?} with no sync after its commit record"),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// I-ONE-INTENT / I-COUNT / I-LOCK (on every committed/intent state change)
// ---------------------------------------------------------------------------

struct OwnedIntent {
    key: Vec<u8>,
    lock: RowLockMode,
    owner: TxnId,
}

fn scan_intents(sim: &Sim) -> Vec<OwnedIntent> {
    let view = sim.core.open_view();
    let mut out = Vec::new();
    for entry in view.scan((Bound::Unbounded, Bound::Unbounded), false) {
        let (k, v) = entry.expect("kv scan");
        if let Some((logical, Entry::Intent)) = parse_key(&k) {
            let intent = decode_intent(&v).expect("intent decode");
            let lock = intent.top().map_or(RowLockMode::NoKeyUpdate, |t| t.lock);
            out.push(OwnedIntent {
                key: logical.to_vec(),
                lock,
                owner: intent.txn,
            });
        }
    }
    out
}

/// Whether `txn` still holds its exclusive lock: Pending, or a commit not
/// yet visible (§3.2). Ended holders never conflict (§6).
fn holds_exclusively(sim: &Sim, txn: TxnId) -> bool {
    match sim.core.status.entry(txn) {
        None => false,
        Some(e) => match e.status {
            TxnStatus::Pending => true,
            TxnStatus::Committed(c) => c > sim.core.visible_ts(),
            TxnStatus::Aborted => false,
        },
    }
}

fn full_state_checks(sim: &mut Sim) {
    let intents = scan_intents(sim);
    // I-COUNT / I-ONE-INTENT: for every live current-epoch txn,
    // intent_count equals the intents it owns; a placement over a foreign
    // intent (I-ONE-INTENT) shows up as the previous owner's mismatch.
    let mut owned: BTreeMap<TxnId, Vec<Vec<u8>>> = BTreeMap::new();
    for it in &intents {
        owned.entry(it.owner).or_default().push(it.key.clone());
    }
    let epoch = sim.core.epoch();
    for (owner, keys) in &owned {
        let Some(e) = sim.core.status.entry(*owner) else {
            continue;
        };
        if owner.epoch != epoch {
            continue; // older epoch: no count (§7.3 step 4)
        }
        if e.intent_count != keys.len() as i64 {
            sim.violate(
                "I-COUNT",
                format!(
                    "intent_count of {owner:?} is {} but it owns {} intents {:?}",
                    e.intent_count,
                    keys.len(),
                    keys
                ),
            );
            return;
        }
    }
    // Ended-but-not-truncated txns: counts exact, and the write-set log
    // names every owned intent while the handle lives.
    for id in sim.ghost.txns.keys() {
        if id.epoch != epoch {
            continue;
        }
        let Some(e) = sim.core.status.entry(*id) else {
            continue;
        };
        let n = owned.get(id).map_or(0, |v| v.len()) as i64;
        if e.intent_count != n {
            sim.violate(
                "I-COUNT",
                format!(
                    "intent_count of {id:?} is {} but it owns {n} intents",
                    e.intent_count
                ),
            );
            return;
        }
        if let Some(keys) = owned.get(id) {
            if let Some(s) = sim
                .sessions
                .iter()
                .find(|s| s.txn.as_ref().is_some_and(|t| t.id == *id))
            {
                let log = s.txn.as_ref().expect("txn").write_set_keys();
                let missing = keys
                    .iter()
                    .any(|k| !log.iter().any(|(lk, _)| lk.as_slice() == k.as_slice()));
                if missing {
                    sim.violate(
                        "I-COUNT",
                        format!("the write-set log of {id:?} does not name all its intents"),
                    );
                    return;
                }
            }
        }
    }
    // I-LOCK: no two txns hold conflicting row-lock modes on one key (the
    // exclusive side lives in intent top layers, the shared side in the
    // row-lock table; two exclusives cannot coexist on one key — that is
    // I-ONE-INTENT, caught above as a count mismatch).
    let table = sim.locks.as_ref().expect("lock manager").row_table();
    for it in &intents {
        if !holds_exclusively(sim, it.owner) {
            continue;
        }
        for (h, m, _) in table.holders(&it.key) {
            if h != it.owner && holds_exclusively(sim, h) && m.conflicts_with(it.lock) {
                sim.violate(
                    "I-LOCK",
                    format!(
                        "{:?} (intent lock {:?}) and {h:?} (shared {m:?}) conflict on {:?}",
                        it.owner, it.lock, it.key
                    ),
                );
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// I-LIVE
// ---------------------------------------------------------------------------

/// The holders a waiter requesting `mode` on `key` is blocked by right now,
/// recomputed from ghost lock ownership (the intents in the KV and the shared
/// lock table, minus ended holders and `except`), in the order §5.1 checks
/// them: a conflicting foreign intent first; only when there is none, every
/// conflicting shared holder (§6: an edge to every conflicting holder of
/// that kind). A waiter parked on an intent owner has no edge to a shared
/// holder yet: it re-runs §5.1 after the owner ends and waits again then, so
/// a cycle that would only close through that second wait is not a deadlock
/// the check may report (the owner it waits for is not itself blocked).
fn conflicting_holders(sim: &Sim, key: &[u8], mode: RowLockMode, except: TxnId) -> Vec<TxnId> {
    let mut out = Vec::new();
    for it in scan_intents(sim) {
        if it.key.as_slice() == key
            && it.owner != except
            && holds_exclusively(sim, it.owner)
            && (mode.conflicts_with(it.lock) || it.lock.conflicts_with(mode))
        {
            out.push(it.owner);
        }
    }
    if !out.is_empty() {
        return out;
    }
    let table = sim.locks.as_ref().expect("lock manager").row_table();
    for (h, m, _) in table.holders(key) {
        if h != except && holds_exclusively(sim, h) && m.conflicts_with(mode) && !out.contains(&h) {
            out.push(h);
        }
    }
    out
}

/// I-LIVE (a): a parked waiter whose blockers no longer hold a conflicting
/// lock must not stay parked (the poll must return).
fn i_live_a(sim: &mut Sim) {
    let waiting: Vec<(usize, Vec<u8>, RowLockMode, TxnId)> = sim
        .sessions
        .iter()
        .filter(|s| s.wait.is_some())
        .map(|s| {
            let w = s.wait.as_ref().expect("wait");
            (s.id, w.key.clone(), w.mode, w.handle.waiter())
        })
        .collect();
    for (i, key, mode, waiter) in waiting {
        let parked = sim.sessions[i]
            .wait
            .as_ref()
            .is_some_and(|w| sim.core.wait_poll(&w.handle).is_none());
        if !parked {
            continue;
        }
        if conflicting_holders(sim, &key, mode, waiter).is_empty() {
            sim.violate(
                "I-LIVE(a)",
                format!(
                    "session {i} ({waiter:?}) stays parked on {key:?} with no conflicting holder left"
                ),
            );
            return;
        }
    }
}

/// The wait-for graph recomputed from ghost lock ownership at this moment:
/// every waiting session edges to the current conflicting holders of its
/// key. Independent of the crate's graph — that is the point (I-LIVE c).
fn ghost_wait_graph(sim: &Sim) -> BTreeMap<TxnId, BTreeSet<TxnId>> {
    let mut g: BTreeMap<TxnId, BTreeSet<TxnId>> = BTreeMap::new();
    for s in &sim.sessions {
        if let Some(w) = &s.wait {
            let waiter = w.handle.waiter();
            for h in conflicting_holders(sim, &w.key, w.mode, waiter) {
                g.entry(waiter).or_default().insert(h);
            }
        }
    }
    g
}

fn cycle_through(g: &BTreeMap<TxnId, BTreeSet<TxnId>>, w: TxnId) -> bool {
    let Some(succ) = g.get(&w) else {
        return false;
    };
    let mut stack: Vec<TxnId> = succ.iter().rev().copied().collect();
    let mut seen = BTreeSet::new();
    while let Some(t) = stack.pop() {
        if t == w {
            return true;
        }
        if !seen.insert(t) {
            continue;
        }
        if let Some(next) = g.get(&t) {
            stack.extend(next.iter().rev().copied());
        }
    }
    false
}

/// Whether `waiter` is on a cycle of the ghost wait graph right now.
pub fn waiter_on_ghost_cycle(sim: &Sim, waiter: TxnId) -> bool {
    cycle_through(&ghost_wait_graph(sim), waiter)
}

/// Whether the cycle through `waiter` is real: every member is still parked
/// (its poll would return `None`); a member about to wake dissolves the
/// cycle legitimately (its edges were removed by the waker).
pub fn ghost_cycle_is_parked(sim: &Sim, waiter: TxnId) -> bool {
    let mut g: BTreeMap<TxnId, BTreeSet<TxnId>> = BTreeMap::new();
    let parked: BTreeSet<TxnId> = sim
        .sessions
        .iter()
        .filter(|s| {
            s.wait
                .as_ref()
                .is_some_and(|w| sim.core.wait_poll(&w.handle).is_none())
        })
        .map(|s| s.wait.as_ref().expect("wait").handle.waiter())
        .collect();
    for s in &sim.sessions {
        if let Some(w) = &s.wait {
            let wid = w.handle.waiter();
            if !parked.contains(&wid) {
                continue;
            }
            for h in conflicting_holders(sim, &w.key, w.mode, wid) {
                if parked.contains(&h) {
                    g.entry(wid).or_default().insert(h);
                }
            }
        }
    }
    cycle_through(&g, waiter)
}

// ---------------------------------------------------------------------------
// The read oracle and I-WW
// ---------------------------------------------------------------------------

/// The serial-oracle expectation for reading `key` at snapshot `S` inside
/// txn `reader` at statement `seq0` (the ghost committed history overlaid
/// with the reader's own surviving writes with `seq < seq0`).
fn expected_read(sim: &Sim, reader: TxnId, seq0: Seq, s: Ts, key: &[u8]) -> Option<Vec<u8>> {
    if let Some(w) = sim
        .ghost
        .txns
        .get(&reader)
        .and_then(|t| t.own_write_at(key, seq0))
    {
        return write_value(w);
    }
    sim.ghost
        .version_at(key, s)
        .and_then(|(_, w)| write_value(w))
}

/// Checks one read's result against the oracle, at read time.
pub fn read_oracle(
    sim: &mut Sim,
    _core: &Core<super::SimKv>,
    reader: &TxnId,
    seq0: Seq,
    snapshot: Ts,
    kidx: usize,
    result: Option<&[u8]>,
) {
    let key = sim.keys[kidx].clone();
    let expected = expected_read(sim, *reader, seq0, snapshot, &key);
    let same = match (&expected, result) {
        (Some(a), Some(b)) => a.as_slice() == b,
        (None, None) => true,
        _ => false,
    };
    if !same {
        sim.violate(
            "READ-ORACLE",
            format!(
                "read of {key:?} at S={snapshot:?} by {reader:?} (seq0 {seq0}) returned {result:?}, the serial oracle gives {expected:?}"
            ),
        );
    }
}

/// I-WW for an applied row op under RR/SER with no own prior write on the
/// key: it must not succeed while the key's ghost versions newer than the
/// statement's `S` include a conflict (any version for update/delete/lock
/// modes; a tombstone for KEY SHARE — moved-tombstones and key-changed
/// writes do not occur in this workload).
pub fn i_ww_check(sim: &mut Sim, _core: &Core<super::SimKv>, tid: &TxnId, stmt: Stmt) {
    let iso = sim
        .ghost
        .txns
        .get(tid)
        .map_or(Isolation::ReadCommitted, |t| t.isolation);
    if !matches!(iso, Isolation::RepeatableRead | Isolation::Serializable) {
        return;
    }
    let (key, mode) = match stmt {
        Stmt::Update(k) => (sim.keys[k].clone(), RowLockMode::NoKeyUpdate),
        Stmt::Delete(k) => (sim.keys[k].clone(), RowLockMode::Update),
        Stmt::LockUpdate(k) => (sim.keys[k].clone(), RowLockMode::Update),
        Stmt::LockKeyShare(k) => (sim.keys[k].clone(), RowLockMode::KeyShare),
        _ => return,
    };
    let seq0 = sim
        .sessions
        .iter()
        .find(|s| s.txn.as_ref().is_some_and(|t| t.id == *tid))
        .map(|s| s.seq0)
        .unwrap_or(0);
    // With an own surviving prior write the op follows §5.4 (no newer-version
    // check); otherwise §5.1's rule applies.
    let own_prior = sim
        .ghost
        .txns
        .get(tid)
        .is_some_and(|t| t.writes.iter().any(|w| w.seq < seq0 && w.key == key));
    if own_prior {
        return;
    }
    let s = sim
        .sessions
        .iter()
        .find(|x| x.txn.as_ref().is_some_and(|t| t.id == *tid))
        .map(|x| x.stmt_snapshot)
        .unwrap_or(Ts(0));
    let newer: Vec<(Ts, GWrite)> = sim
        .ghost
        .versions_of(&key)
        .into_iter()
        .filter(|(ts, _)| *ts > s)
        .map(|(ts, w)| (ts, w.clone()))
        .collect();
    if newer.is_empty() {
        return;
    }
    let bad = if mode == RowLockMode::KeyShare {
        newer.iter().any(|(_, w)| matches!(w, GWrite::Delete))
    } else {
        true
    };
    if bad {
        sim.violate(
            "I-WW",
            format!(
                "{tid:?} applied a row op on {key:?} (mode {mode:?}, S={s:?}) with {} newer committed versions",
                newer.len()
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// I-GC
// ---------------------------------------------------------------------------

fn read_at(
    core: &Core<super::SimKv>,
    key: &[u8],
    s: Ts,
) -> Result<Option<Vec<u8>>, nucleus_txn::TxnError> {
    let _guard = core.registry.register_at(s)?;
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: TxnId { epoch: 0, n: 0 },
        snapshot: s,
        stmt_seq: 0,
    };
    read::read_key(core, &view, key, &ctx, &mut NoSsi)
}

/// Every registered snapshot (the live RR/SER txn snapshots) reading every
/// key, before a GC step.
pub fn gc_guard_reads(sim: &mut Sim) -> Vec<(Ts, Vec<Option<Vec<u8>>>)> {
    let core = Arc::clone(&sim.core);
    let snaps: Vec<Ts> = sim
        .sessions
        .iter()
        .filter(|s| s.txn.is_some())
        .filter(|s| matches!(s.iso(), Isolation::RepeatableRead | Isolation::Serializable))
        .map(|s| s.snapshot_ts)
        .collect();
    let mut out = Vec::new();
    for s in snaps {
        let mut row = Vec::new();
        for key in &sim.keys {
            match read_at(&core, key, s) {
                Ok(v) => row.push(v),
                Err(e) => {
                    sim.violate(
                        "I-GC",
                        format!("registered snapshot {s:?} cannot be re-registered: {e:?}"),
                    );
                    return out;
                }
            }
        }
        out.push((s, row));
    }
    out
}

pub fn gc_guard_check(sim: &mut Sim, before: &[(Ts, Vec<Option<Vec<u8>>>)]) {
    let core = Arc::clone(&sim.core);
    for (s, row) in before {
        for (k, key) in sim.keys.iter().enumerate() {
            let after = match read_at(&core, key, *s) {
                Ok(v) => v,
                Err(e) => {
                    sim.violate(
                        "I-GC",
                        format!("registered snapshot {s:?} failed after a GC step: {e:?}"),
                    );
                    return;
                }
            };
            if after != row[k] {
                sim.violate(
                    "I-GC",
                    format!(
                        "a GC step changed the read of {key:?} at registered snapshot {s:?}: {:?} -> {after:?}",
                        row[k]
                    ),
                );
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// I-SER
// ---------------------------------------------------------------------------

/// Builds the dependency graph of the committed SER txns from ghost state
/// (ww by ts order, wr from each read's ghost version, rw from each read to
/// the writer of the next version of that key above S — for scans, every
/// committed write inside the range above S) and checks it for cycles.
fn i_ser(sim: &mut Sim) {
    let ser: BTreeSet<TxnId> = sim
        .ghost
        .committed
        .values()
        .copied()
        .filter(|id| {
            sim.ghost
                .txns
                .get(id)
                .is_some_and(|t| t.isolation == Isolation::Serializable)
        })
        .collect();
    if ser.len() < 2 {
        return;
    }
    let mut g: BTreeMap<TxnId, BTreeSet<TxnId>> = BTreeMap::new();
    // (from, to, why) — provenance for the audit dump.
    let mut why_edges: Vec<(TxnId, TxnId, String)> = Vec::new();
    let edge = |g: &mut BTreeMap<TxnId, BTreeSet<TxnId>>,
                why: &mut Vec<(TxnId, TxnId, String)>,
                a: TxnId,
                b: TxnId,
                reason: String| {
        if a != b && g.entry(a).or_default().insert(b) {
            why.push((a, b, reason));
        }
    };
    for key in &sim.keys {
        // PostgreSQL identity semantics (§5.1's I-WW note: inserting over a
        // tombstone is legal): a tombstone ENDS a row, a later live write
        // starts a NEW row on the same key. ww edges therefore run only
        // within one row chain — insert -> updates -> delete — never across
        // a tombstone-to-reinsert boundary.
        let mut writers: Vec<(Ts, TxnId, bool)> = sim
            .ghost
            .committed
            .iter()
            .filter_map(|(ts, id)| {
                if !ser.contains(id) {
                    return None;
                }
                let live = sim.ghost.txns.get(id).is_some_and(|t| {
                    let mut last = None;
                    for w in &t.writes {
                        if w.key == *key {
                            last = Some(&w.kind);
                        }
                    }
                    last.is_some_and(|k| !matches!(k, GWrite::Delete))
                });
                // Only txns that wrote this key join its writer list; `live`
                // says whether their version is a live row or a tombstone.
                sim.ghost
                    .txns
                    .get(id)
                    .is_some_and(|t| t.writes.iter().any(|w| w.key == *key))
                    .then_some((*ts, *id, live))
            })
            .collect();
        writers.sort_unstable();
        let mut prev_live: Option<TxnId> = None;
        for (ts, id, live) in writers {
            if let Some(p) = prev_live {
                edge(
                    &mut g,
                    &mut why_edges,
                    p,
                    id,
                    format!("ww k{}@{}", key_index_of(sim, key), ts.0),
                );
            }
            prev_live = if live { Some(id) } else { None };
        }
    }
    for id in &ser {
        let Some(t) = sim.ghost.txns.get(id) else {
            continue;
        };
        for r in &t.reads {
            // A read satisfied by the txn's own write consumed no committed
            // version and creates no dependency on other writers (neither
            // wr from the version's writer nor rw to the next writer).
            if t.own_write_at(&r.key, r.seq0).is_some() {
                continue;
            }
            // wr: the writer of the version the read returned.
            if let Some((ts, _)) = sim.ghost.version_at(&r.key, r.snapshot) {
                if let Some(w) = sim.ghost.committed.get(&ts) {
                    if ser.contains(w) {
                        edge(
                            &mut g,
                            &mut why_edges,
                            *w,
                            *id,
                            format!("wr k{}@{}", key_index_of(sim, &r.key), ts.0),
                        );
                    }
                }
            }
            // rw: the writer of the next version of the key (or, for scans,
            // any committed write) above the read's S. The range covers the
            // read's key for point reads and `[lo, end(hi)]` for scans (all
            // logical keys are the same length, so the end key of the last
            // key bounds the range exactly).
            let range = r
                .range
                .clone()
                .unwrap_or_else(|| (r.key.clone(), end_key(&r.key)));
            let mut above: Vec<Ts> = sim
                .keys
                .iter()
                .filter(|k| k.as_slice() >= range.0.as_slice() && k.as_slice() < range.1.as_slice())
                .flat_map(|k| {
                    sim.ghost
                        .versions_of(k)
                        .into_iter()
                        .filter(|(ts, _)| *ts > r.snapshot)
                        .map(|(ts, _)| ts)
                        .collect::<Vec<_>>()
                })
                .collect();
            above.sort_unstable();
            if let Some(next) = above.first() {
                if let Some(w) = sim.ghost.committed.get(next) {
                    if ser.contains(w) {
                        edge(
                            &mut g,
                            &mut why_edges,
                            *id,
                            *w,
                            format!("rw k{} S={}", key_index_of(sim, &r.key), r.snapshot.0),
                        );
                    }
                }
            }
        }
    }
    for n in g.keys() {
        if cycle_through(&g, *n) {
            let mut desc = String::new();
            for (a, bs) in &g {
                for b in bs {
                    desc.push_str(&format!("{a:?}->{b:?} "));
                }
            }
            // The dependency footprint of the cycle's members, for auditing.
            let mut members = BTreeSet::new();
            members.insert(*n);
            for bs in g.values() {
                for b in bs {
                    members.insert(*b);
                }
            }
            let mut why = String::new();
            for m in &members {
                if let Some(t) = sim.ghost.txns.get(m) {
                    why.push_str(&format!(
                        "{m:?} S={:?} outcome={:?} ts={:?}: {:?}; ",
                        t.snapshot, t.outcome, t.known_ts, t.events
                    ));
                }
            }
            sim.violate(
                "I-SER",
                format!(
                    "a cycle in the committed SER dependency graph through {n:?}: {desc} | {why} | edges: {:?}",
                    why_edges
                        .iter()
                        .map(|(a, b, r)| format!("{a:?}->{b:?} {r}"))
                        .collect::<Vec<_>>()
                ),
            );
            return;
        }
    }
}

fn key_index_of(sim: &Sim, key: &[u8]) -> usize {
    sim.keys
        .iter()
        .position(|k| k.as_slice() == key)
        .unwrap_or(usize::MAX)
}

// ---------------------------------------------------------------------------
// Boot, crash and end-of-run checks
// ---------------------------------------------------------------------------

/// Loads the persisted commit records into the ghost (a surviving record of
/// a lost txn commits it, §7.2), then — after a crash — runs I-DURABLE,
/// I-ATOMIC, the `next_ts` rule and the older-epoch intent rule.
pub fn on_reboot(sim: &mut Sim, core: &Core<super::SimKv>) {
    let mut loaded: BTreeMap<TxnId, Ts> = BTreeMap::new();
    {
        let view = core.open_view();
        for entry in view.scan(
            (
                Bound::Included(sys_txn_prefix().as_slice()),
                Bound::Excluded(sys_txn_prefix_end().as_slice()),
            ),
            false,
        ) {
            let (k, v) = entry.expect("kv scan");
            if let Some(id) = parse_sys_txn_key(&k) {
                if v.len() == 8 {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&v);
                    loaded.insert(id, Ts(u64::from_be_bytes(b)));
                }
            }
        }
    }
    let ids: Vec<(TxnId, Ts)> = loaded.iter().map(|(a, b)| (*a, *b)).collect();
    for (id, ts) in &ids {
        if let Some(t) = sim.ghost.txns.get_mut(id) {
            if matches!(
                t.outcome,
                ghost::Outcome::LostInCrash | ghost::Outcome::Active
            ) {
                t.outcome = ghost::Outcome::CommittedByRecord(*ts);
            }
        }
        sim.ghost.commit_at(*id, *ts);
    }
    // Old-epoch txns the reboot did **not** load: their /sys/txn record is
    // either gone with a dropped batch (§7.2: an `off`-acked commit never
    // happened; the ts may be reused by this era) or was deleted by §7.4
    // truncation before the crash (truncation removes only released txns,
    // so both flags may be recorded). Without this, I-VIS would test a
    // dropped txn against a status table that legitimately has no entry
    // for it, at a ts this era reused.
    let epoch = core.epoch();
    let stale: Vec<TxnId> = sim
        .ghost
        .txns
        .iter()
        .filter(|(id, t)| {
            id.epoch < epoch
                && !loaded.contains_key(id)
                && matches!(
                    t.outcome,
                    ghost::Outcome::Acked(_) | ghost::Outcome::CommittedByRecord(_)
                )
        })
        .map(|(id, _)| *id)
        .collect();
    for id in stale {
        let ts = sim.ghost.txns.get(&id).and_then(|t| match t.outcome {
            ghost::Outcome::Acked(ts) | ghost::Outcome::CommittedByRecord(ts) => Some(ts),
            _ => None,
        });
        let Some(ts) = ts else { continue };
        let dropped = sim
            .chk
            .commit_pos
            .get(&id)
            .is_some_and(|&p| !sim.kv.event_live(p));
        let Some(t) = sim.ghost.txns.get_mut(&id) else {
            continue;
        };
        if dropped {
            t.outcome = ghost::Outcome::DroppedByCrash(ts);
        } else {
            t.released_seen = true;
            t.truncated_seen = true;
        }
    }
    if !sim.chk.pending_crash_checks {
        return;
    }
    sim.chk.pending_crash_checks = false;
    // The new next_ts must exceed every ts that survived the crash (an
    // On-acked record always survives, I-ACK; an Off-acked record may be
    // dropped together with its unsynced /sys/ts_hwm reservation — that ts
    // was never durable and may be reassigned, §7.2's boot).
    let hwm = core.ts_hwm();
    let max_surviving = loaded.values().max().copied().unwrap_or(Ts(0));
    if hwm.0 < max_surviving.0 {
        sim.violate(
            "NEXT-TS",
            format!(
                "after reboot next_ts {} does not exceed the largest surviving record ts {}",
                hwm.0 + 1,
                max_surviving.0
            ),
        );
        return;
    }
    // I-ATOMIC: every record-loaded txn's surviving writes are all visible
    // at visible_ts.
    let visible = core.visible_ts();
    for (id, ts) in &ids {
        let keys: BTreeSet<Vec<u8>> = sim
            .ghost
            .txns
            .get(id)
            .map(|t| t.writes.iter().map(|w| w.key.clone()).collect())
            .unwrap_or_default();
        for key in keys {
            if !write_visible(sim, core, *id, *ts, &key, visible) {
                sim.violate(
                    "I-ATOMIC",
                    format!(
                        "a write of {id:?} (ts {ts:?}) on {key:?} is not visible at {visible:?} after reboot"
                    ),
                );
                return;
            }
        }
    }
    // I-DURABLE: an acked `synchronous_commit=on` commit survives any crash
    // — every surviving write is visible. Its `/sys/txn` record may be gone
    // legitimately (§7.4 truncation wrote the delete before the crash; the
    // kept unsynced prefix replays it after the durable record — the data,
    // resolved before the delete, survives with it).
    let on_acked: Vec<(TxnId, Ts, BTreeSet<Vec<u8>>)> = sim
        .ghost
        .txns
        .iter()
        .filter(|(_, t)| {
            matches!(t.outcome, ghost::Outcome::Acked(_)) && t.sync == Some(SyncCommit::On)
        })
        .filter_map(|(id, t)| {
            let ts = match t.outcome {
                ghost::Outcome::Acked(ts) => ts,
                _ => return None,
            };
            let keys: BTreeSet<Vec<u8>> = t.writes.iter().map(|w| w.key.clone()).collect();
            Some((*id, ts, keys))
        })
        .collect();
    for (id, ts, keys) in on_acked {
        for key in keys {
            if !write_visible(sim, core, id, ts, &key, visible) {
                sim.violate(
                    "I-DURABLE",
                    format!(
                        "an On-acked commit {id:?} (ts {ts:?}) lost its write on {key:?} in the crash"
                    ),
                );
                return;
            }
        }
    }
    // Older-epoch intents read as their records say.
    let intents = scan_intents(sim);
    for it in &intents {
        if it.owner.epoch >= core.epoch() {
            continue;
        }
        let expected = expected_read(sim, TxnId { epoch: 0, n: 0 }, 0, visible, &it.key);
        let actual = read_at_visible(sim, core, &it.key);
        if actual != expected {
            sim.violate(
                "OLDER-EPOCH",
                format!(
                    "older-epoch intent of {:?} on {:?}: the key reads {actual:?} but the records say {expected:?}",
                    it.owner, it.key
                ),
            );
            return;
        }
    }
}

fn read_at_visible(sim: &Sim, core: &Core<super::SimKv>, key: &[u8]) -> Option<Vec<u8>> {
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: TxnId { epoch: 0, n: 0 },
        snapshot: core.visible_ts(),
        stmt_seq: 0,
    };
    let _ = sim;
    read::read_key(core, &view, key, &ctx, &mut NoSsi).expect("read at visible")
}

/// Whether `id`'s write on `key`, committed at `ts`, is visible at
/// `visible`: a version at exactly `ts` with the ghost value, or its
/// (unresolved) intent whose top layer carries the write.
fn write_visible(
    sim: &Sim,
    core: &Core<super::SimKv>,
    id: TxnId,
    ts: Ts,
    key: &[u8],
    visible: Ts,
) -> bool {
    if ts > visible {
        return false;
    }
    let Some(t) = sim.ghost.txns.get(&id) else {
        return false;
    };
    let mut last: Option<GWrite> = None;
    for w in &t.writes {
        if w.key == key {
            last = Some(w.kind.clone());
        }
    }
    let Some(w) = last else {
        return true; // no surviving write on the key
    };
    let view = core.open_view();
    let lo = intent_key(key);
    let hi = end_key(key);
    for entry in view.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        let (k, v) = entry.expect("kv scan");
        match parse_key(&k) {
            Some((_, Entry::Intent)) => {
                let intent = decode_intent(&v).expect("intent");
                if intent.txn == id {
                    let top = intent.top().expect("layer");
                    let same = match (&top.data, &w) {
                        (LayerData::Write { value, .. }, gw) => {
                            write_value(gw).is_some_and(|x| x == *value)
                        }
                        (LayerData::Delete { .. }, GWrite::Delete) => true,
                        _ => false,
                    };
                    if same {
                        return true;
                    }
                }
            }
            Some((_, Entry::Version(vts))) if vts == ts => {
                let decoded = decode_version(&v).expect("version");
                let same = match (&decoded, &w) {
                    (nucleus_txn::encoding::VersionValue::Live { payload, .. }, gw) => {
                        write_value(gw).is_some_and(|x| x == *payload)
                    }
                    (nucleus_txn::encoding::VersionValue::Tombstone { .. }, GWrite::Delete) => true,
                    _ => false,
                };
                if same {
                    return true;
                }
            }
            Some((_, Entry::Version(_))) | None => {}
        }
    }
    false
}

/// Marks that the crash checks run at the next boot.
pub fn after_crash_checks(sim: &mut Sim) {
    // Every commit record the crash dropped is invisible after the
    // reboot: exclude it from the ghost history (a later era may reuse
    // its ts). An Off-acked txn may legitimately be among them.
    for rec in sim.kv.commits() {
        if !sim.kv.event_live(rec.pos) {
            sim.ghost.mark_lost(rec.ts);
        }
    }
    sim.chk.pending_crash_checks = true;
}

/// The end-of-run battery: the drained system's quiescent state and the
/// committed fold (lost updates).
pub fn final_checks(sim: &mut Sim, core: &Core<super::SimKv>) {
    // No current-epoch intents remain (older-epoch ones are cleaned lazily,
    // §7.2).
    for it in scan_intents(sim) {
        if it.owner.epoch == core.epoch() {
            sim.violate(
                "END-STATE",
                format!(
                    "intent of {:?} on {:?} remains after the drain",
                    it.owner, it.key
                ),
            );
            return;
        }
    }
    // No wait edges or slots, no shared locks.
    if !core.wait_edges().is_empty() || !core.wait_slots().is_empty() {
        sim.violate(
            "END-STATE",
            "wait edges or slots remain after the drain".into(),
        );
        return;
    }
    let table = sim.locks.as_ref().expect("lock manager").row_table();
    for key in &sim.keys {
        if !table.holders(key).is_empty() {
            sim.violate("END-STATE", format!("shared locks on {key:?} remain"));
            return;
        }
    }
    // No Pending statuses remain.
    let pending: Vec<TxnId> = sim
        .ghost
        .txns
        .iter()
        .filter(|(id, t)| {
            id.epoch == core.epoch()
                && !t.truncated_seen
                && matches!(
                    sim.core.status.entry(**id),
                    Some(e) if e.status == TxnStatus::Pending
                )
        })
        .map(|(id, _)| *id)
        .collect();
    if let Some(id) = pending.first() {
        sim.violate(
            "END-STATE",
            format!("{id:?} is still Pending after the drain"),
        );
        return;
    }
    // Lost updates: each key's committed value equals its ghost fold (every
    // committed increment counted exactly once — a txn that updates a key
    // in two statements increments twice, so the fold applies all of a
    // txn's surviving writes in program order, and each increment must
    // have been computed from the value the fold gives at that point).
    let committed: Vec<(Ts, TxnId)> = sim.ghost.committed.iter().map(|(a, b)| (*a, *b)).collect();
    for key in &sim.keys {
        let mut cur: Option<u64> = None;
        let mut bad: Option<String> = None;
        for (_, id) in &committed {
            let Some(t) = sim.ghost.txns.get(id) else {
                continue;
            };
            for w in t.writes.iter().filter(|w| w.key == *key) {
                match &w.kind {
                    GWrite::Set(v) => cur = Some(ghost::payload_u64(v)),
                    GWrite::Inc { wrote } => match cur {
                        Some(c) => {
                            if ghost::payload_u64(wrote) != c + 1 {
                                bad = Some(format!(
                                    "{id:?} incremented {key:?} to {} but the fold had {c}",
                                    ghost::payload_u64(wrote)
                                ));
                            }
                            cur = Some(c + 1);
                        }
                        None => {
                            let history: Vec<String> = committed
                                .iter()
                                .filter_map(|(ts, tid)| {
                                    let tt = sim.ghost.txns.get(tid)?;
                                    let ws: Vec<String> = tt
                                        .writes
                                        .iter()
                                        .filter(|x| x.key == *key)
                                        .map(|x| format!("s{} {:?}", x.seq, x.kind))
                                        .collect();
                                    (!ws.is_empty()).then(|| {
                                        format!(
                                            "{tid:?}@{ts:?} {:?} S={:?}: {ws:?}",
                                            tt.isolation, tt.snapshot
                                        )
                                    })
                                })
                                .collect();
                            bad = Some(format!(
                                "{id:?} incremented absent key {key:?}; txn events {:?}; committed history of the key: {history:?}",
                                t.events
                            ));
                        }
                    },
                    GWrite::Delete => cur = None,
                }
                if bad.is_some() {
                    break;
                }
            }
            if bad.is_some() {
                break;
            }
        }
        if let Some(b) = bad {
            sim.violate("LOST-UPDATE", b);
            return;
        }
        let actual = read_at_visible(sim, core, key).map(|v| ghost::payload_u64(&v));
        if actual != cur {
            sim.violate(
                "LOST-UPDATE",
                format!(
                    "key {key:?} ends at {actual:?} but the committed fold gives {cur:?} (every increment must be counted exactly once)"
                ),
            );
            return;
        }
    }
    // I-SER once more over the final history.
    if sim.cfg.check_i_ser {
        i_ser(sim);
    }
}
