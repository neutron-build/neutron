//! E1′ arm (c): the **newest committed version stored in place** in the
//! X-0 B+tree, older versions in an **undo log** (card X-3; PLAN-v3 §2
//! N3 arm (c), §4 E1′ — the HyPer/InnoDB scheme).
//!
//! The engine implements [`TxnEngine`] with the shared X-1a components
//! unchanged — [`Sequencer`], [`Snapshots`], [`WriteSet`], [`LockTable`]
//! and [`commit_with`] — so the only difference from arm (b) (card X-2,
//! timestamp-in-key) is the version layout (card Goal). Undo past the
//! in-memory budget spills to a temp file and **readers are never
//! aborted** (N3: "undo past the budget spills to a temp file; readers
//! are never aborted" — there is no snapshot-too-old error anywhere in
//! this crate).
//!
//! Invariants owned here:
//! - AC-NEWEST-IN-PLACE: the tree holds exactly one entry per key — the
//!   newest committed version, in Work 2's encoding. Every older version
//!   lives only in [`undo`].
//! - AC-UNDO-FIRST (Work 5's binding order): commit apply pushes a key's
//!   undo record **before** writing the tree value that supersedes it,
//!   so a reader that observes a tree value with ts above its snapshot
//!   finds every record its chain names. `tests/undo_order.rs` drives
//!   readers through the window (with the engine's apply-yield hook) and
//!   fails under the swapped order.
//! - AC-PERKEY-ASC: per key, the tree's installed ts values strictly
//!   ascend. A txn holds a key's row lock from its first write of it
//!   until `commit_with` releases it after `mark_applied`, and takes its
//!   commit ts while holding the lock (C-COMMIT-ORDER), so the next
//!   writer of the same key both acquires after this commit applied and
//!   takes a strictly higher ts (L-EXCL). This is what makes each key's
//!   undo chain descend monotonically and the tree value always the
//!   key's newest version.
//! - AC-WRITER-VISIBLE: writer-side decisions (the RC EPQ base, the RR
//!   newer-version rule, unique liveness) read the newest version with
//!   ts `<= visible_ts` — the chain walk of Work 4 with the snapshot
//!   replaced by `visible_ts` — which is the reference engine's
//!   `newest_visible` rule and keeps the commit-visibility window
//!   pending for writers exactly as in the reference (R-LOCK-VISIBLE,
//!   C-T0 §3.2).
//! - AC-GC: [`TxnEngine::gc`] trims the undo prefix and removes tree
//!   tombstones exactly per Work 7, and is semantically invisible to
//!   every snapshot at or above the watermark (the S13 oracle rule).
//!
//! Test hooks follow X-1a-F3: they are **per-engine** state. The suite's
//! [`TxnEngine::lock_table`] and [`TxnEngine::sequencer`] return this
//! engine's own shared instances — the very ones every acquire and
//! `commit_with` use — and the apply-yield hook of
//! [`ArmC::arm_apply_yield`] is an engine field, never process-global.

use std::collections::HashMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use e0_btree::store::{MemStore, PageId, PageStore, StoreError};
use e0_btree::{BTree, page};
use e1_mvcc::api::{EngineStats, Isolation, SavepointId, Ts, TxnEngine, TxnErr, TxnId, Wait};
use e1_mvcc::common::{LockTable, Sequencer, SnapshotGuard, Snapshots, WriteSet, commit_with};

pub mod undo;

use undo::{TAG_TOMBSTONE, TAG_VALUE, UndoLog};

/// How long commit apply sleeps between the undo push and the tree write
/// when the test hook of [`ArmC::arm_apply_yield`] is armed — long enough
/// that a concurrent reader samples inside the window.
const APPLY_YIELD: Duration = Duration::from_micros(300);

/// The lock-table identity [`TxnEngine::gc`] removes tombstones under
/// (Work 7). Txn ids start at 1, so 0 is free, and it is not the lock
/// table's reserved owner word (`u64::MAX`).
const GC_TXN: TxnId = 0;

/// Names the spill files of successive engines of one process (a name,
/// not engine state; the engine's own hooks are all per-instance).
static SPILL_FILE_N: AtomicU64 = AtomicU64::new(0);

/// `Arc<MemStore>` behind a fresh type so [`PageStore`] can be implemented
/// for it here: the engine needs a second handle on the store (its page
/// count, for the `tree_bytes` stat) without consuming the tree, and
/// `PageStore` is not implemented for `Arc` itself. Delegation only —
/// e0-btree and e1-mvcc stay untouched (card Do-not).
struct SharedStore(Arc<MemStore>);

impl PageStore for SharedStore {
    fn page_size(&self) -> usize {
        PageStore::page_size(&*self.0)
    }

    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<(), StoreError> {
        PageStore::read(&*self.0, id, buf)
    }

    fn write(&self, id: PageId, buf: &[u8]) -> Result<(), StoreError> {
        PageStore::write(&*self.0, id, buf)
    }

    fn allocate(&self) -> Result<PageId, StoreError> {
        PageStore::allocate(&*self.0)
    }

    fn free(&self, id: PageId) -> Result<(), StoreError> {
        PageStore::free(&*self.0, id)
    }
}

/// The tree-backed half of the engine (absent only when construction
/// failed; every op then fails through the poison path).
struct Core {
    /// The shared store handle, for the `tree_bytes` stat.
    store: Arc<MemStore>,
    /// The X-0 B+tree over that store (card Work 1).
    tree: BTree<SharedStore>,
    /// The undo log with its spill budget (card Work 3).
    undo: UndoLog,
}

/// The E1′ arm-(c) engine (card X-3): newest version in place on the X-0
/// B+tree, older versions in a spilling undo log, on the shared X-1a
/// txn components (see the crate docs for the invariants).
pub struct ArmC {
    core: Option<Core>,
    /// The construction failure, when `core` is `None` (surfaced by the
    /// poison path).
    init_err: Option<String>,
    seq: Sequencer,
    snaps: Snapshots,
    locks: LockTable,
    poison: AtomicBool,
    next_txn: AtomicU64,
    /// Engine-side view of every open txn's write-set bytes (the
    /// `write_set_bytes` metric; the txn owns the set itself).
    ws_bytes: Mutex<HashMap<TxnId, u64>>,
    bytes_applied: AtomicU64,
    /// Test hook (card mutation evidence WRITE-BEFORE-UNDO): when set,
    /// commit apply sleeps between pushing a key's undo record and
    /// writing its tree value — the window whose order Work 5 makes
    /// binding. Per-engine state, like every hook since X-1a-F3.
    apply_yield: AtomicBool,
}

/// The open-txn handle of [`ArmC`].
pub struct ArmCTxn {
    id: TxnId,
    iso: Isolation,
    ws: WriteSet,
    snap: SnapshotGuard,
}

/// Encode one tree value (card Work 2): `ts u64 BE | tag u8 (0 value,
/// 1 tombstone) | undo_ptr u64 BE (0 = none) | bytes`.
fn encode_tree_value(ts: Ts, tag: u8, undo_ptr: u64, bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17 + bytes.len());
    buf.extend_from_slice(&ts.to_be_bytes());
    buf.push(tag);
    buf.extend_from_slice(&undo_ptr.to_be_bytes());
    buf.extend_from_slice(bytes);
    buf
}

/// Decode one tree value ([`encode_tree_value`]'s inverse); `None` on
/// short bytes, never a panic.
fn decode_tree_value(buf: &[u8]) -> Option<(Ts, u8, u64, Vec<u8>)> {
    let ts = undo::be_u64(buf, 0)?;
    let tag = *buf.get(8)?;
    let undo_ptr = undo::be_u64(buf, 9)?;
    Some((ts, tag, undo_ptr, buf.get(17..)?.to_vec()))
}

/// A version resolved by [`ArmC::walk_version`]: its ts, and its value
/// (`None` for a tombstone).
type Resolved = (Ts, Option<Vec<u8>>);

/// Key/value pairs — the shape of a scan base and result.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Map any fail-stop error onto [`TxnErr::Engine`] (PLAN-v3 §6.1: the
/// engine never panics; store and undo errors are fail-stop).
fn eng<E: Display>(e: E) -> TxnErr {
    TxnErr::Engine(e.to_string())
}

impl ArmC {
    /// A fresh engine (card Work 1): a `pool_frames`-frame buffer pool of
    /// `page_size`-byte pages over an in-memory store, whose undo log
    /// spills from `undo_budget_bytes` of in-memory records into
    /// `spill_dir`.
    ///
    /// Infallible on the trait's terms: a construction failure (a zero
    /// pool, a page size outside the slotted format's range, an unusable
    /// spill directory) poisons the engine and every later call returns
    /// [`TxnErr::Engine`] — the spike never panics (PLAN-v3 §6.1).
    pub fn new(
        pool_frames: usize,
        page_size: usize,
        undo_budget_bytes: u64,
        spill_dir: impl AsRef<Path>,
    ) -> Self {
        match Self::try_new(pool_frames, page_size, undo_budget_bytes, spill_dir) {
            Ok(e) => e,
            Err(msg) => Self::poisoned_shell(msg),
        }
    }

    /// Build the engine, or return the failure as a string (see
    /// [`ArmC::new`]).
    fn try_new(
        pool_frames: usize,
        page_size: usize,
        undo_budget_bytes: u64,
        spill_dir: impl AsRef<Path>,
    ) -> Result<Self, String> {
        let store = Arc::new(MemStore::new(page_size).map_err(|e| e.to_string())?);
        let tree =
            BTree::new(SharedStore(Arc::clone(&store)), pool_frames).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(spill_dir.as_ref()).map_err(|e| e.to_string())?;
        let file = format!(
            "e1-arm-c-undo-{}-{}.log",
            std::process::id(),
            SPILL_FILE_N.fetch_add(1, Ordering::Relaxed)
        );
        let undo = UndoLog::new(undo_budget_bytes, spill_dir.as_ref().join(file));
        Ok(Self {
            core: Some(Core { store, tree, undo }),
            init_err: None,
            ..Self::shell()
        })
    }

    /// The fields every engine has, with no core.
    fn shell() -> Self {
        Self {
            core: None,
            init_err: None,
            seq: Sequencer::new(),
            snaps: Snapshots::new(),
            locks: LockTable::new(),
            poison: AtomicBool::new(false),
            next_txn: AtomicU64::new(1),
            ws_bytes: Mutex::new(HashMap::new()),
            bytes_applied: AtomicU64::new(0),
            apply_yield: AtomicBool::new(false),
        }
    }

    /// A shell with the poison flag set and the construction failure
    /// recorded.
    fn poisoned_shell(msg: String) -> Self {
        let mut e = Self::shell();
        e.init_err = Some(msg);
        e.poison.store(true, Ordering::Release);
        e
    }

    fn poison_check(&self) -> Result<(), TxnErr> {
        if let Some(msg) = &self.init_err {
            return Err(TxnErr::Engine(format!("engine construction failed: {msg}")));
        }
        if self.poison.load(Ordering::Relaxed) {
            Err(TxnErr::Engine("poisoned".into()))
        } else {
            Ok(())
        }
    }

    /// The core, failing through the poison path when construction
    /// failed.
    fn core(&self) -> Result<&Core, TxnErr> {
        self.poison_check()?;
        match self.core.as_ref() {
            Some(c) => Ok(c),
            None => Err(TxnErr::Engine("engine construction failed".into())),
        }
    }

    /// Test hook (card X-3 mutation evidence WRITE-BEFORE-UNDO): from now
    /// on, every commit apply sleeps briefly **between** pushing a key's
    /// undo record and writing its tree value — the window whose order
    /// Work 5 makes binding. Under the correct order the sleep is
    /// unobservable; under the swapped order a concurrent reader samples
    /// a tree value whose undo record does not exist yet, which
    /// `tests/undo_order.rs` reports. Per-engine state, like every hook
    /// since X-1a-F3; the engine's semantics do not depend on it.
    #[doc(hidden)]
    pub fn arm_apply_yield(&self) {
        self.apply_yield.store(true, Ordering::Release);
    }

    /// Walk one version chain to snapshot `snap` (card Work 4): the
    /// version itself if its ts is at or below the snapshot, else the
    /// first undo record whose `prev_ts <= snap`, else nothing (the key
    /// did not exist at `snap`). Returns the chosen version's ts and
    /// value, so writer-side decisions can compare the ts
    /// (AC-WRITER-VISIBLE reuses this with `snap = visible_ts`).
    ///
    /// By AC-UNDO-FIRST every record a correct engine's chain names
    /// exists: the tree value was published only after its record was
    /// appended, and records die only below the watermark no snapshot
    /// sits under (U-TRIM-SAFE). A missing record or a chain that fails
    /// to descend is therefore an internal break, surfaced as
    /// [`TxnErr::Engine`] — never a panic, never a wrong value quietly.
    fn walk_version(
        core: &Core,
        ts: Ts,
        tag: u8,
        undo_ptr: u64,
        bytes: Vec<u8>,
        snap: Ts,
    ) -> Result<Option<Resolved>, TxnErr> {
        if ts <= snap {
            let val = (tag == TAG_VALUE).then_some(bytes);
            return Ok(Some((ts, val)));
        }
        // A chain cannot be longer than the log (ptrs strictly descend),
        // so this bound turns a corrupt cycle into an Engine error.
        let bound = core.undo.record_count().saturating_add(1);
        let mut ptr = undo_ptr;
        let mut steps = 0u64;
        loop {
            if ptr == 0 {
                return Ok(None); // chain end: the key did not exist at snap
            }
            let rec = match core.undo.get(ptr).map_err(eng)? {
                Some(r) => r,
                None => {
                    return Err(TxnErr::Engine(format!(
                        "undo record {ptr} is missing from the log (AC-UNDO-FIRST broken)"
                    )));
                }
            };
            if rec.prev_ts <= snap {
                let val = (rec.prev_tag == TAG_VALUE).then_some(rec.prev_bytes);
                return Ok(Some((rec.prev_ts, val)));
            }
            ptr = rec.prev_undo_ptr;
            steps += 1;
            if steps > bound {
                return Err(TxnErr::Engine("undo chain failed to descend".into()));
            }
        }
    }

    /// The version of `key` at snapshot `snap` (card Work 4).
    fn read_at(&self, core: &Core, key: &[u8], snap: Ts) -> Result<Option<Vec<u8>>, TxnErr> {
        match core.tree.get(key).map_err(eng)? {
            Some(raw) => {
                let (ts, tag, undo_ptr, bytes) = decode_tree_value(&raw)
                    .ok_or_else(|| TxnErr::Engine("tree value did not decode".into()))?;
                Ok(Self::walk_version(core, ts, tag, undo_ptr, bytes, snap)?.and_then(|(_, v)| v))
            }
            None => Ok(None),
        }
    }

    /// The newest committed version of `key` with ts `<= visible_ts` and
    /// its ts (AC-WRITER-VISIBLE): the reference engine's
    /// `newest_visible` rule, as a chain walk.
    fn visible_version(&self, core: &Core, key: &[u8]) -> Result<Option<Resolved>, TxnErr> {
        let visible = self.seq.visible_ts();
        match core.tree.get(key).map_err(eng)? {
            Some(raw) => {
                let (ts, tag, undo_ptr, bytes) = decode_tree_value(&raw)
                    .ok_or_else(|| TxnErr::Engine("tree value did not decode".into()))?;
                Self::walk_version(core, ts, tag, undo_ptr, bytes, visible)
            }
            None => Ok(None),
        }
    }

    /// Record one own write and refresh the engine-side metric.
    fn ws_set(&self, t: &mut ArmCTxn, key: &[u8], value: Option<Vec<u8>>) {
        t.ws.set(key, value);
        lock_map(&self.ws_bytes).insert(t.id, t.ws.bytes());
    }

    /// Refresh the engine-side metric after a rollback.
    fn ws_rolled_back(&self, t: &mut ArmCTxn) {
        lock_map(&self.ws_bytes).insert(t.id, t.ws.bytes());
    }

    /// The live committed entries of `[lo, hi)` at `snap`, ascending —
    /// the scan base before own writes merge. An inverted or empty
    /// interval yields nothing.
    fn visible_scan(&self, core: &Core, lo: &[u8], hi: &[u8], snap: Ts) -> Result<Pairs, TxnErr> {
        let mut base = Vec::new();
        if lo >= hi {
            return Ok(base);
        }
        let iter = core.tree.range(lo, hi).map_err(eng)?;
        for item in iter {
            let (k, raw) = item.map_err(eng)?;
            let (ts, tag, undo_ptr, bytes) = decode_tree_value(&raw)
                .ok_or_else(|| TxnErr::Engine("tree value did not decode".into()))?;
            if let Some((_, Some(v))) = Self::walk_version(core, ts, tag, undo_ptr, bytes, snap)? {
                base.push((k, v));
            }
        }
        Ok(base)
    }
}

/// Lock a mutex whose critical sections are panic-free by construction
/// (the e1-mvcc `lock_unpoisoned` convention; pub(crate) there, so this
/// crate carries its own).
fn lock_map(m: &Mutex<HashMap<TxnId, u64>>) -> std::sync::MutexGuard<'_, HashMap<TxnId, u64>> {
    match m.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

impl TxnEngine for ArmC {
    type Txn = ArmCTxn;

    fn name(&self) -> &'static str {
        "arm-c"
    }

    fn begin(&self, iso: Isolation) -> ArmCTxn {
        let id = self.next_txn.fetch_add(1, Ordering::Relaxed);
        let snap = self.snaps.register(&self.seq);
        ArmCTxn {
            id,
            iso,
            ws: WriteSet::new(),
            snap,
        }
    }

    fn next_statement(&self, t: &mut ArmCTxn) {
        // RC takes a fresh snapshot at the statement boundary; RR keeps
        // the txn's one snapshot (A-RC).
        if t.iso == Isolation::ReadCommitted {
            t.snap = self.snaps.register(&self.seq);
        }
    }

    fn get(&self, t: &mut ArmCTxn, key: &[u8]) -> Result<Option<Vec<u8>>, TxnErr> {
        self.poison_check()?;
        // Own writes first (A-OWN), then the snapshot read.
        if let Some(own) = t.ws.get(key) {
            return Ok(own.clone());
        }
        let core = self.core()?;
        self.read_at(core, key, t.snap.ts())
    }

    fn scan(
        &self,
        t: &mut ArmCTxn,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr> {
        self.poison_check()?;
        let core = self.core()?;
        let base = self.visible_scan(core, lo, hi, t.snap.ts())?;
        // The limit applies after the merge (W-MERGE).
        Ok(t.ws
            .merge_scan(base, lo, hi)
            .into_iter()
            .take(limit)
            .collect())
    }

    fn update_with(
        &self,
        t: &mut ArmCTxn,
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
        // AC-WRITER-VISIBLE: the conflict check and the EPQ base read
        // the newest version at or below visible_ts, after the wait —
        // exactly the reference engine's rule (verdict X-1a mutant O7).
        let core = self.core()?;
        let newest = self.visible_version(core, key)?;
        if t.iso == Isolation::RepeatableRead
            && let Some((ts, _)) = newest.as_ref()
            && *ts > t.snap.ts()
        {
            // I-WW (C-T0 §5.1): the row changed after our snapshot.
            return Err(TxnErr::Serialization);
        }
        let current = newest.map(|(_, v)| v);
        let current_ref = current.as_ref().and_then(|v| v.as_deref());
        let new = f(current_ref);
        self.ws_set(t, key, new);
        Ok(())
    }

    fn put(&self, t: &mut ArmCTxn, key: &[u8], value: &[u8], wait: Wait) -> Result<(), TxnErr> {
        // A-API: put is update_with with a constant Some(value) closure.
        let v = value.to_vec();
        let mut always = |_old: Option<&[u8]>| Some(v.clone());
        self.update_with(t, key, wait, &mut always)
    }

    fn insert_unique(
        &self,
        t: &mut ArmCTxn,
        key: &[u8],
        value: &[u8],
        wait: Wait,
    ) -> Result<(), TxnErr> {
        self.poison_check()?;
        self.locks.acquire(key, t.id, wait)?;
        // R-UNIQUE: live = an own live write, else the newest committed
        // live version, whatever its ts (C-T0 §5.3: an own Delete or a
        // tombstone counts as not live; proceed).
        let core = self.core()?;
        let live = match t.ws.get(key) {
            Some(own) => own.is_some(),
            None => self
                .visible_version(core, key)?
                .is_some_and(|(_, v)| v.is_some()),
        };
        if live {
            return Err(TxnErr::UniqueViolation);
        }
        self.ws_set(t, key, Some(value.to_vec()));
        Ok(())
    }

    fn delete(&self, t: &mut ArmCTxn, key: &[u8], wait: Wait) -> Result<bool, TxnErr> {
        let mut existed = false;
        let mut to_none = |old: Option<&[u8]>| {
            existed = old.is_some();
            None
        };
        self.update_with(t, key, wait, &mut to_none)
            .map(|()| existed)
    }

    fn savepoint(&self, t: &mut ArmCTxn) -> SavepointId {
        t.ws.savepoint()
    }

    fn rollback_to(&self, t: &mut ArmCTxn, sp: SavepointId) {
        t.ws.rollback_to(sp);
        self.ws_rolled_back(t);
    }

    fn commit(&self, t: ArmCTxn) -> Result<Ts, TxnErr> {
        self.poison_check()?;
        let ArmCTxn { id, ws, snap, .. } = t;
        // The txn's snapshot registration ends with the txn (C-RAII).
        drop(snap);
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            ws.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let core = self.core()?;
        let res = commit_with(&self.seq, &self.locks, &self.poison, id, |ts| {
            for (key, value) in &entries {
                // Work 5, in key order (the write set iterates ascending):
                // (1) push the undo record of the current tree value, if
                // any — AC-UNDO-FIRST — then (2) write the new value with
                // undo_ptr pointing at that record. Deletes write a
                // tombstone in place.
                let raw = core.tree.get(key).map_err(eng)?;
                let mut new_ptr = 0u64;
                if let Some(raw) = raw {
                    let (prev_ts, prev_tag, prev_undo_ptr, prev_bytes) = decode_tree_value(&raw)
                        .ok_or_else(|| TxnErr::Engine("tree value did not decode".into()))?;
                    new_ptr = core
                        .undo
                        .append(ts, key, prev_ts, prev_tag, prev_undo_ptr, &prev_bytes)
                        .map_err(eng)?;
                }
                if self.apply_yield.load(Ordering::Acquire) {
                    thread::sleep(APPLY_YIELD);
                }
                let tag = if value.is_none() {
                    TAG_TOMBSTONE
                } else {
                    TAG_VALUE
                };
                let payload = value.as_deref().unwrap_or(&[]);
                core.tree
                    .insert(key, &encode_tree_value(ts, tag, new_ptr, payload))
                    .map_err(eng)?;
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
                lock_map(&self.ws_bytes).remove(&id);
                Ok(ts)
            }
            Err(e) => Err(e),
        }
    }

    fn abort(&self, t: ArmCTxn) {
        let ArmCTxn { id, ws, snap, .. } = t;
        // Drop the write set and the snapshot registration (R- and
        // C-RAII); nothing was ever applied, so there is no trace (S10).
        drop(ws);
        drop(snap);
        lock_map(&self.ws_bytes).remove(&id);
        self.locks.release_all(id);
    }

    fn gc(&self) -> u64 {
        let core = match self.core() {
            Ok(c) => c,
            Err(_) => return 0,
        };
        let watermark = self.snaps.oldest(&self.seq);
        // Work 7: trim the undo prefix (U-TRIM-SAFE). A spill-file error
        // is fail-stop: poison and report what was already reclaimed.
        let mut reclaimed = match core.undo.trim(watermark) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.poison.store(true, Ordering::Relaxed);
                return 0;
            }
        };
        // Work 7, one tree pass: collect tombstones whose ts is at or
        // below the watermark and whose undo pointer is now dead or zero.
        // The pass only collects — the deletes run after the cursor's
        // shared latches are gone (the tree forbids same-thread mutation
        // under a held iterator).
        let lo: &[u8] = &[];
        let hi = vec![0xFF; page::MAX_KEY + 1];
        let mut candidates: Vec<Vec<u8>> = Vec::new();
        if let Ok(iter) = core.tree.range(lo, &hi) {
            for item in iter {
                let Ok((key, raw)) = item else { break };
                if let Some((ts, tag, undo_ptr, _)) = decode_tree_value(&raw)
                    && tag == TAG_TOMBSTONE
                    && ts <= watermark
                    && (undo_ptr == 0 || !core.undo.contains(undo_ptr))
                {
                    candidates.push(key);
                }
            }
        }
        // Delete each candidate under its key's lock, re-checked after
        // the acquire: a writer that replaced the tombstone between the
        // pass and the delete needed the lock, so it either landed first
        // (the re-read sees its live value and the delete is skipped) or
        // waits for ours and then applies over a correctly absent key
        // (AC-PERKEY-ASC keeps its undo chain intact either way).
        for key in candidates {
            if self.locks.acquire(&key, GC_TXN, Wait::NoWait).is_err() {
                continue; // a writer holds it; a later gc() reconsiders
            }
            let still = core
                .tree
                .get(&key)
                .ok()
                .flatten()
                .and_then(|raw| decode_tree_value(&raw));
            if let Some((ts, tag, undo_ptr, _)) = still
                && tag == TAG_TOMBSTONE
                && ts <= watermark
                && (undo_ptr == 0 || !core.undo.contains(undo_ptr))
                && core.tree.delete(&key).is_ok()
            {
                reclaimed += key.len() as u64 + 1;
            }
            self.locks.release_all(GC_TXN);
        }
        reclaimed
    }

    fn stats(&self) -> EngineStats {
        let write_set_bytes: u64 = lock_map(&self.ws_bytes).values().sum();
        let mut tree_bytes = 0u64;
        let mut logical = 0u64;
        let mut versions = 0u64;
        let mut undo_bytes = 0u64;
        let mut undo_spilled_bytes = 0u64;
        if let Some(core) = &self.core {
            let visible = self.seq.visible_ts();
            undo_bytes = core.undo.mem_bytes();
            undo_spilled_bytes = core.undo.spill_bytes();
            versions = core.undo.record_count();
            tree_bytes = core
                .store
                .page_count()
                .map_or(0, |n| n as u64 * core.store.page_size() as u64);
            // One pass over the tree: entry count plus the live bytes of
            // the newest version at or below visible_ts. The high bound
            // sits above every legal key (the tree refuses longer keys),
            // so the pass sees every entry; a pass error degrades the
            // counts, never the engine.
            let lo: &[u8] = &[];
            let hi = vec![0xFF; page::MAX_KEY + 1];
            if let Ok(iter) = core.tree.range(lo, &hi) {
                for item in iter {
                    let Ok((key, raw)) = item else { break };
                    versions += 1;
                    if let Some((ts, tag, undo_ptr, bytes)) = decode_tree_value(&raw)
                        && let Ok(Some((_, Some(v)))) =
                            Self::walk_version(core, ts, tag, undo_ptr, bytes, visible)
                    {
                        logical += key.len() as u64 + v.len() as u64;
                    }
                }
            }
        }
        EngineStats {
            tree_bytes,
            undo_bytes,
            undo_spilled_bytes,
            logical_bytes: logical,
            versions,
            lock_entries: self.locks.entries(),
            write_set_bytes,
            bytes_applied: self.bytes_applied.load(Ordering::Relaxed),
        }
    }

    /// Card X-1a-F3: this engine's own shared lock table — the instance
    /// every acquire and `commit_with` uses. Never a dummy.
    #[doc(hidden)]
    fn lock_table(&self) -> &LockTable {
        &self.locks
    }

    /// Card X-1a-F3: this engine's own sequencer, whose apply gate holds
    /// only this engine's commits (armed through
    /// `Sequencer::apply_gate()`).
    #[doc(hidden)]
    fn sequencer(&self) -> &Sequencer {
        &self.seq
    }
}
