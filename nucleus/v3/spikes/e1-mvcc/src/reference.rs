//! The reference engine, `RefEngine` (card X-1a Work 4).
//!
//! One `Mutex<BTreeMap<Vec<u8>, Vec<(Ts, Option<Vec<u8>>)>>>` holds the
//! full version list of every key — the most obviously correct MVCC store
//! available — plus the same [`crate::common`] components and the same
//! rules as the arms. There is no X-0 tree here: this engine exists to be
//! the differential oracle of S13 and to validate the behaviour suite
//! itself (`tests/reference.rs`).
//!
//! Invariants owned here:
//! - R-ATOMIC: a commit's versions enter the map inside one critical
//!   section, so every reader sees all of a txn's writes or none (PLAN-v3
//!   §1.1 "atomic, committed prefix"; suite S11/S14).
//! - R-LOCK-VISIBLE: while a txn holds a key's row lock, every committed
//!   version of that key has ts `<= visible_ts` — writers release locks
//!   only after [`crate::common::commit_with`] marked them applied
//!   (C-COMMIT-ORDER). The write path therefore reads "the newest
//!   version" and may treat it as the newest *visible* commit without a
//!   second visibility filter (C-T0 §3.2's window is closed before the
//!   lock can change hands).
//! - R-EPQ: an RC updater's `f` sees that newest committed value (EPQ,
//!   C-T0 §5.2); an RR updater instead fails with 40001 if a version
//!   newer than its snapshot exists (C-T0 §5.1 I-WW).
//! - R-UNIQUE: `insert_unique` checks liveness of the **newest** committed
//!   version regardless of the snapshot (C-T0 §5.3 — a tombstone newer
//!   than `S` may be inserted over); the RR rule does not apply to it.
//! - R-GC: `gc` keeps every version above the watermark and the newest at
//!   or below it (C-T0 §9.2), so every registered snapshot keeps reading
//!   the same values (I-GC).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::api::{EngineStats, Isolation, SavepointId, Ts, TxnEngine, TxnErr, TxnId, Wait};
use crate::common::{LockTable, Sequencer, Snapshots, WriteSet, commit_with, lock_unpoisoned};

/// The version store: full version lists per key, `ts`-ascending, `None`
/// value = tombstone (card Work 4).
pub type VersionLists = BTreeMap<Vec<u8>, Vec<(Ts, Option<Vec<u8>>)>>;

/// The obviously-correct engine (see the module docs). It implements
/// [`TxnEngine`] with no page tree, no undo and no spill — the oracle the
/// differential scenario (S13) compares every arm against.
pub struct RefEngine {
    data: std::sync::Mutex<VersionLists>,
    seq: Sequencer,
    snaps: Snapshots,
    locks: LockTable,
    poison: AtomicBool,
    next_txn: AtomicU64,
    /// Engine-side view of every open txn's write-set bytes (the
    /// `write_set_bytes` metric; the txn owns the set itself).
    ws_bytes: std::sync::Mutex<HashMap<TxnId, u64>>,
    bytes_applied: AtomicU64,
}

/// The open-txn handle of [`RefEngine`].
pub struct RefTxn {
    id: TxnId,
    iso: Isolation,
    ws: WriteSet,
    snap: crate::common::SnapshotGuard,
}

impl RefEngine {
    /// A fresh empty engine with the default lock-table budget.
    pub fn new() -> Self {
        Self {
            data: std::sync::Mutex::new(BTreeMap::new()),
            seq: Sequencer::new(),
            snaps: Snapshots::new(),
            locks: LockTable::new(),
            poison: AtomicBool::new(false),
            next_txn: AtomicU64::new(1),
            ws_bytes: std::sync::Mutex::new(HashMap::new()),
            bytes_applied: AtomicU64::new(0),
        }
    }

    fn poison_check(&self) -> Result<(), TxnErr> {
        if self.poison.load(Ordering::Relaxed) {
            Err(TxnErr::Engine("poisoned".into()))
        } else {
            Ok(())
        }
    }

    /// The newest committed version of `key` (R-LOCK-VISIBLE: the caller
    /// holds the key's lock whenever this matters).
    fn newest_committed(&self, key: &[u8]) -> Option<(Ts, Option<Vec<u8>>)> {
        let data = lock_unpoisoned(&self.data);
        data.get(key).and_then(|vs| vs.last()).cloned()
    }

    /// The newest version of `key` at or below `snap` (C-T0 §4 step 2).
    fn at_snapshot(&self, key: &[u8], snap: Ts) -> Option<Option<Vec<u8>>> {
        let data = lock_unpoisoned(&self.data);
        data.get(key)
            .and_then(|vs| vs.iter().rev().find(|(t, _)| *t <= snap))
            .map(|(_, v)| v.clone())
    }

    /// The live committed keys in `[lo, hi)` at `snap`, ascending. An
    /// empty interval (`lo >= hi`) yields nothing (a `BTreeMap` range
    /// with inverted bounds would panic).
    fn visible_scan(&self, lo: &[u8], hi: &[u8], snap: Ts) -> Vec<(Vec<u8>, Vec<u8>)> {
        if lo >= hi {
            return Vec::new();
        }
        let data = lock_unpoisoned(&self.data);
        data.range(lo.to_vec()..hi.to_vec())
            .filter_map(|(k, vs)| {
                vs.iter()
                    .rev()
                    .find(|(t, _)| *t <= snap)
                    .and_then(|(_, v)| v.clone().map(|v| (k.clone(), v)))
            })
            .collect()
    }

    /// Record one own write and refresh the engine-side metric.
    fn ws_set(&self, t: &mut RefTxn, key: &[u8], value: Option<Vec<u8>>) {
        t.ws.set(key, value);
        lock_unpoisoned(&self.ws_bytes).insert(t.id, t.ws.bytes());
    }

    /// Refresh the engine-side metric after a rollback.
    fn ws_rolled_back(&self, t: &mut RefTxn) {
        lock_unpoisoned(&self.ws_bytes).insert(t.id, t.ws.bytes());
    }
}

impl Default for RefEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnEngine for RefEngine {
    type Txn = RefTxn;

    fn name(&self) -> &'static str {
        "reference"
    }

    fn begin(&self, iso: Isolation) -> RefTxn {
        let id = self.next_txn.fetch_add(1, Ordering::Relaxed);
        let snap = self.snaps.register(&self.seq);
        RefTxn {
            id,
            iso,
            ws: WriteSet::new(),
            snap,
        }
    }

    fn next_statement(&self, t: &mut RefTxn) {
        // RC takes a fresh snapshot at the statement boundary; RR keeps
        // the txn's one snapshot (A-RC).
        if t.iso == Isolation::ReadCommitted {
            t.snap = self.snaps.register(&self.seq);
        }
    }

    fn get(&self, t: &mut RefTxn, key: &[u8]) -> Result<Option<Vec<u8>>, TxnErr> {
        self.poison_check()?;
        // Own writes first (A-OWN), then the snapshot read.
        if let Some(own) = t.ws.get(key) {
            return Ok(own.clone());
        }
        Ok(self.at_snapshot(key, t.snap.ts()).flatten())
    }

    fn scan(
        &self,
        t: &mut RefTxn,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr> {
        self.poison_check()?;
        let base = self.visible_scan(lo, hi, t.snap.ts());
        // The limit applies after the merge (W-MERGE).
        Ok(t.ws
            .merge_scan(base, lo, hi)
            .into_iter()
            .take(limit)
            .collect())
    }

    fn update_with(
        &self,
        t: &mut RefTxn,
        key: &[u8],
        wait: Wait,
        f: &mut dyn FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    ) -> Result<(), TxnErr> {
        self.poison_check()?;
        self.locks.acquire(key, t.id, wait)?;
        // Own-row rule (C-T0 §5.4): with an own write present, the write
        // set has excluded foreign writers since its placement, so `f`
        // applies to the own value and no newer-version check runs.
        if let Some(own) = t.ws.get(key) {
            let new = f(own.as_deref());
            self.ws_set(t, key, new);
            return Ok(());
        }
        // R-LOCK-VISIBLE: holding the lock, the newest version is the
        // newest visible commit.
        let newest = self.newest_committed(key);
        if t.iso == Isolation::RepeatableRead
            && let Some((ts, _)) = newest.as_ref()
            && *ts > t.snap.ts()
        {
            // I-WW (C-T0 §5.1): the row changed after our snapshot.
            return Err(TxnErr::Serialization);
        }
        // R-EPQ: RC's `f` sees the newest committed value.
        let current = newest.map(|(_, v)| v);
        let current_ref = current.as_ref().and_then(|v| v.as_deref());
        let new = f(current_ref);
        self.ws_set(t, key, new);
        Ok(())
    }

    fn put(&self, t: &mut RefTxn, key: &[u8], value: &[u8], wait: Wait) -> Result<(), TxnErr> {
        // A-API: put is update_with with a constant Some(value) closure.
        let v = value.to_vec();
        let mut always = |_old: Option<&[u8]>| Some(v.clone());
        self.update_with(t, key, wait, &mut always)
    }

    fn insert_unique(
        &self,
        t: &mut RefTxn,
        key: &[u8],
        value: &[u8],
        wait: Wait,
    ) -> Result<(), TxnErr> {
        self.poison_check()?;
        self.locks.acquire(key, t.id, wait)?;
        // R-UNIQUE: live = an own live write, else the newest committed
        // live version, whatever its ts (C-T0 §5.3: an own Delete or a
        // tombstone counts as not live; proceed).
        let live = match t.ws.get(key) {
            Some(own) => own.is_some(),
            None => self.newest_committed(key).is_some_and(|(_, v)| v.is_some()),
        };
        if live {
            return Err(TxnErr::UniqueViolation);
        }
        self.ws_set(t, key, Some(value.to_vec()));
        Ok(())
    }

    fn delete(&self, t: &mut RefTxn, key: &[u8], wait: Wait) -> Result<bool, TxnErr> {
        let mut existed = false;
        let mut to_none = |old: Option<&[u8]>| {
            existed = old.is_some();
            None
        };
        self.update_with(t, key, wait, &mut to_none)
            .map(|()| existed)
    }

    fn savepoint(&self, t: &mut RefTxn) -> SavepointId {
        t.ws.savepoint()
    }

    fn rollback_to(&self, t: &mut RefTxn, sp: SavepointId) {
        t.ws.rollback_to(sp);
        self.ws_rolled_back(t);
    }

    fn commit(&self, t: RefTxn) -> Result<Ts, TxnErr> {
        self.poison_check()?;
        let RefTxn { id, ws, snap, .. } = t;
        // The txn's snapshot registration ends with the txn (C-RAII).
        drop(snap);
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            ws.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let data = &self.data;
        let res = commit_with(&self.seq, &self.locks, &self.poison, id, |ts| {
            // R-ATOMIC: all versions land in one critical section.
            let mut data = lock_unpoisoned(data);
            for (k, v) in &entries {
                data.entry(k.clone()).or_default().push((ts, v.clone()));
            }
            Ok(())
        });
        match res {
            Ok(ts) => {
                let applied: u64 = entries
                    .iter()
                    .map(|(k, v)| k.len() as u64 + v.as_ref().map_or(0, |x| x.len() as u64))
                    .sum();
                self.bytes_applied.fetch_add(applied, Ordering::Relaxed);
                lock_unpoisoned(&self.ws_bytes).remove(&id);
                Ok(ts)
            }
            Err(e) => Err(e),
        }
    }

    fn abort(&self, t: RefTxn) {
        let RefTxn { id, ws, snap, .. } = t;
        // Drop the write set and the snapshot registration (R- and
        // C-RAII); nothing was ever applied, so there is no trace (S10).
        drop(ws);
        drop(snap);
        lock_unpoisoned(&self.ws_bytes).remove(&id);
        self.locks.release_all(id);
    }

    fn gc(&self) -> u64 {
        let watermark = self.snaps.oldest(&self.seq);
        let mut data = lock_unpoisoned(&self.data);
        let mut reclaimed = 0u64;
        let keys: Vec<Vec<u8>> = data.keys().cloned().collect();
        for key in keys {
            let Some(versions) = data.get_mut(&key) else {
                continue;
            };
            // R-GC (C-T0 §9.2): keep every version above the watermark
            // and the newest at or below it; drop the older prefix.
            let Some(newest_le) = versions.iter().rposition(|(t, _)| *t <= watermark) else {
                continue;
            };
            for (_, v) in versions.drain(..newest_le) {
                reclaimed += key.len() as u64 + v.map_or(1, |x| x.len() as u64);
            }
        }
        reclaimed
    }

    fn stats(&self) -> EngineStats {
        let data = lock_unpoisoned(&self.data);
        let visible = self.seq.visible_ts();
        let mut versions = 0u64;
        let mut logical = 0u64;
        for (k, vs) in data.iter() {
            versions += vs.len() as u64;
            if let Some((_, v)) = vs.iter().rev().find(|(t, _)| *t <= visible)
                && let Some(v) = v
            {
                logical += k.len() as u64 + v.len() as u64;
            }
        }
        let write_set_bytes: u64 = lock_unpoisoned(&self.ws_bytes).values().sum();
        EngineStats {
            // No page tree and no undo chains in the reference engine
            // (card Work 4); the arms fill these.
            tree_bytes: 0,
            undo_bytes: 0,
            undo_spilled_bytes: 0,
            logical_bytes: logical,
            versions,
            lock_entries: self.locks.entries(),
            write_set_bytes,
            bytes_applied: self.bytes_applied.load(Ordering::Relaxed),
        }
    }
}
