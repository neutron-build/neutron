//! Arm (b) of E1′: **timestamp-in-key versions** on the X-0 B+tree —
//! the v2 MVCC model without intents (N2) — card X-2; PLAN-v3 §4 E1′,
//! §2 N3 option (b).
//!
//! Every version of a user key is one tree entry keyed so that the
//! tree's memcmp order is `(user key asc, ts desc)` ([`crate::encode`]);
//! a read at snapshot `S` is one seek and the newest-version probe is
//! one seek (card Work 3–4). The txn surface is exactly
//! [`TxnEngine`](e1_mvcc::api::TxnEngine); the shared components —
//! [`Sequencer`](e1_mvcc::common::Sequencer),
//! [`Snapshots`](e1_mvcc::common::Snapshots),
//! [`WriteSet`](e1_mvcc::common::WriteSet),
//! [`LockTable`](e1_mvcc::common::LockTable) and
//! [`commit_with`](e1_mvcc::common::commit_with) — are e1-mvcc's, used
//! unchanged (card Work 1; card X-1a verdict: F1/F2/F3 and F1-F2 all
//! landed before this arm was built).
//!
//! Invariants owned here (the suite scenario that catches each mutant
//! in parentheses):
//! - B-STORE: the only version store is the [`BTree`]; writes buffer in
//!   the private [`WriteSet`] under [`LockTable`] locks and reach the
//!   tree only inside [`commit_with`]'s apply, in ascending user-key
//!   order (card Work 4–5; PLAN-v3 N2 "versions are written to trees at
//!   commit"). An abort leaves no trace (S10).
//! - B-SEEK: `get` seeks `enc(k) ++ !S` and takes the first entry with
//!   `k`'s prefix — the newest version with `ts <= S`; a tombstone
//!   means absent (card Work 3; TOMBSTONE-AS-VALUE ← S8).
//! - B-SCAN-SKIP: `scan` iterates the encoded range and, per user key,
//!   emits the first version `<= S` and skips the rest (card Work 3;
//!   SCAN-NO-SKIP ← S13).
//! - B-NEWEST: writer-side decisions answer "the newest committed
//!   version" with one seek at `enc(k) ++ !MAX` (card Work 4): the RR
//!   conflict rule (40001 iff that version's `ts >` the snapshot,
//!   C-T0 §5.1 I-WW — re-checked after the lock wait, so a blocker's
//!   commit is never missed) and `insert_unique`'s liveness (a
//!   tombstone is not live whatever its ts, C-T0 §5.3). Under
//!   C-COMMIT-ORDER — locks change hands only after `mark_applied` —
//!   this equals the newest *visible* version whenever the caller holds
//!   the key's lock, which is exactly when the seek runs
//!   (S4b–S4d, S6a–S6e).
//! - B-GC: `gc()` keeps every version above the watermark plus the
//!   newest at or below it, deletes older versions, and elides that
//!   kept version when it is a tombstone with nothing above the
//!   watermark left to separate readers from (card Work 6;
//!   GC-DROPS-VISIBLE ← S12 **and** S13, whose oracle replays every
//!   `Gc` as a no-op since card X-1a-F2). GC is semantically invisible:
//!   every registered snapshot is `>= W` (C-SNAP-REG), so the newest
//!   version at or below any readable snapshot is never a deleted one
//!   (C-GC-SAFE).
//! - B-HOOKS: [`TxnEngine::lock_table`] and [`TxnEngine::sequencer`]
//!   return the arm's own shared instances — the ones `commit_with`
//!   receives and every acquire uses (card Work 8;
//!   HOOK-DUMMY-LOCKTABLE ← S4b–S4d / S6a, whose parked-waiter barrier
//!   would otherwise watch a table nobody parks on).
//! - B-FAIL-STOP: a tree error inside a commit's apply poisons the
//!   engine (every later call returns `Engine("poisoned")`) and the
//!   locks stay held — C-FAIL-STOP of the shared commit protocol.
//!
//! Spike limits, inherited from the X-0 tree: values are capped (the
//! tag byte makes the effective cap `MAX_VALUE - 1` payload bytes) and
//! encoded keys are capped at `MAX_KEY` bytes, i.e. at most ~995
//! all-zero user-key bytes; the suite's keys and values are far below
//! both. `stats().tree_bytes` counts the store's pages — the pool
//! recycles reserved-but-unused ids (X-0-F2), so the store grows only
//! when the tree publishes a page.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use e0_btree::store::{MemStore, PageId, PageStore, StoreError};
use e0_btree::tree::BTree;
use e1_mvcc::api::{EngineStats, Isolation, SavepointId, Ts, TxnEngine, TxnErr, TxnId, Wait};
use e1_mvcc::common::{LockTable, Sequencer, SnapshotGuard, Snapshots, WriteSet, commit_with};

use crate::encode;

/// Lock a mutex whose critical sections are panic-free by construction
/// (the same total-without-`unwrap` pattern e1-mvcc uses privately).
fn unpoisoned<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

/// Map a tree/store error onto the trait's fail-stop variant.
fn tree_err(e: StoreError) -> TxnErr {
    TxnErr::Engine(format!("arm-b: {e}"))
}

/// The shared [`MemStore`] behind the tree (B-STORE's stats half): the
/// engine keeps one `Arc` to read the page count for `tree_bytes` while
/// the tree's pool owns the store through this adapter. Implementing
/// [`PageStore`] for this local wrapper adds no behaviour — every call
/// is [`MemStore`]'s own.
struct SharedMemStore {
    pages: Arc<MemStore>,
}

impl PageStore for SharedMemStore {
    fn page_size(&self) -> usize {
        PageStore::page_size(self.pages.as_ref())
    }

    fn read(&self, id: PageId, buf: &mut [u8]) -> Result<(), StoreError> {
        PageStore::read(self.pages.as_ref(), id, buf)
    }

    fn write(&self, id: PageId, buf: &[u8]) -> Result<(), StoreError> {
        PageStore::write(self.pages.as_ref(), id, buf)
    }

    fn allocate(&self) -> Result<PageId, StoreError> {
        PageStore::allocate(self.pages.as_ref())
    }

    fn free(&self, id: PageId) -> Result<(), StoreError> {
        PageStore::free(self.pages.as_ref(), id)
    }
}

/// The tree plus the handles its stats need (B-STORE).
struct ArmStore {
    tree: BTree<SharedMemStore>,
    /// The shared store handle, for `tree_bytes = pages × page_size`
    /// (card Work 7). The pool recycles ids of pages a split cascade
    /// did not publish (X-0-F2), so the count equals the pages the tree
    /// has published.
    pages: Arc<MemStore>,
    page_size: u64,
}

/// The E1′ arm (b) engine: timestamp-in-key versions on the X-0 B+tree
/// (card X-2 Work 1). See the module docs for the invariants.
pub struct ArmB {
    /// `None` when construction failed (`ArmB::new` is infallible
    /// because the suite's maker is); every store-touching call then
    /// returns the recorded `broken` error.
    store: Option<ArmStore>,
    broken: Option<String>,
    seq: Sequencer,
    snaps: Snapshots,
    locks: LockTable,
    poison: AtomicBool,
    next_txn: AtomicU64,
    /// Engine-side view of every open txn's write-set bytes (the
    /// `write_set_bytes` metric; the txn owns the set itself).
    ws_bytes: Mutex<HashMap<TxnId, u64>>,
    bytes_applied: AtomicU64,
    /// Tree entries (the `versions` metric): +1 per version a commit
    /// applies (encoded keys are unique — ts's are unique and one txn
    /// holds one entry per key), −1 per version `gc()` deletes.
    versions: AtomicU64,
}

/// The open-txn handle of [`ArmB`].
pub struct ArmBTxn {
    id: TxnId,
    iso: Isolation,
    ws: WriteSet,
    snap: SnapshotGuard,
}

impl ArmB {
    /// A fresh empty engine over an in-memory store of `page_size`-byte
    /// pages and a `pool_frames`-frame buffer pool (card Work 1).
    ///
    /// Infallible because the suite's engine maker is: unusable
    /// parameters (a zero pool, a page size outside the tree's
    /// `MIN_PAGE..=65535` window) construct a **broken** engine whose
    /// operations return [`TxnErr::Engine`] — never a panic.
    pub fn new(pool_frames: usize, page_size: usize) -> Self {
        let (store, broken) = Self::build(pool_frames, page_size);
        Self {
            store,
            broken,
            seq: Sequencer::new(),
            snaps: Snapshots::new(),
            locks: LockTable::new(),
            poison: AtomicBool::new(false),
            next_txn: AtomicU64::new(1),
            ws_bytes: Mutex::new(HashMap::new()),
            bytes_applied: AtomicU64::new(0),
            versions: AtomicU64::new(0),
        }
    }

    fn build(pool_frames: usize, page_size: usize) -> (Option<ArmStore>, Option<String>) {
        let pages = match MemStore::new(page_size) {
            Ok(p) => Arc::new(p),
            Err(e) => return (None, Some(format!("arm-b: {e}"))),
        };
        let shared = SharedMemStore {
            pages: Arc::clone(&pages),
        };
        match BTree::new(shared, pool_frames) {
            Ok(tree) => (
                Some(ArmStore {
                    tree,
                    pages,
                    page_size: page_size as u64,
                }),
                None,
            ),
            Err(e) => (None, Some(format!("arm-b: {e}"))),
        }
    }

    /// The usable store, or the reason there is none (poisoned or
    /// broken — B-FAIL-STOP).
    fn usable(&self) -> Result<&ArmStore, TxnErr> {
        if self.poison.load(Ordering::Relaxed) {
            return Err(TxnErr::Engine("poisoned".into()));
        }
        match (&self.store, self.broken.as_deref()) {
            (Some(st), _) => Ok(st),
            (None, Some(msg)) => Err(TxnErr::Engine(msg.to_string())),
            (None, None) => Err(TxnErr::Engine("arm-b: no store".into())),
        }
    }

    /// Record one own write and refresh the engine-side metric.
    fn ws_set(&self, t: &mut ArmBTxn, key: &[u8], value: Option<Vec<u8>>) {
        t.ws.set(key, value);
        unpoisoned(&self.ws_bytes).insert(t.id, t.ws.bytes());
    }

    /// Refresh the engine-side metric after a rollback.
    fn ws_rolled_back(&self, t: &mut ArmBTxn) {
        unpoisoned(&self.ws_bytes).insert(t.id, t.ws.bytes());
    }

    /// B-SEEK: the newest **live** version of `key` with `ts <= snap`,
    /// as one seek at `enc(k) ++ !snap` (card Work 3). `Ok(None)` = no
    /// version at or below the snapshot, or the newest one is a
    /// tombstone — absent either way.
    fn version_at(st: &ArmStore, key: &[u8], snap: Ts) -> Result<Option<Vec<u8>>, TxnErr> {
        let want = encode::prefix_of(key);
        let mut it = st
            .tree
            .range(
                &encode::encode_key(key, snap),
                &encode::encode_key_upper(key),
            )
            .map_err(tree_err)?;
        let found = it.next();
        drop(it);
        match found {
            // The first entry at or after the seek that carries this
            // key's prefix is the newest version <= snap; an entry of
            // any other key means nothing of `key` is at or below snap.
            Some(Ok((entry_key, entry_val))) if entry_key.starts_with(&want) => {
                decode_value_or(&entry_val)
            }
            Some(Ok(_)) => Ok(None),
            Some(Err(e)) => Err(tree_err(e)),
            None => Ok(None),
        }
    }

    /// B-NEWEST: the newest committed version of `key`, whatever its
    /// ts, as one seek at `enc(k) ++ !MAX` (card Work 4). The value is
    /// `None` for a tombstone.
    #[allow(clippy::type_complexity)]
    fn newest_version(st: &ArmStore, key: &[u8]) -> Result<Option<(Ts, Option<Vec<u8>>)>, TxnErr> {
        let want = encode::prefix_of(key);
        let mut it = st
            .tree
            .range(
                &encode::encode_seek_newest(key),
                &encode::encode_key_upper(key),
            )
            .map_err(tree_err)?;
        let found = it.next();
        drop(it);
        match found {
            Some(Ok((entry_key, entry_val))) if entry_key.starts_with(&want) => {
                let (_, ts) = match encode::decode_key(&entry_key) {
                    Some(pair) => pair,
                    None => {
                        return Err(TxnErr::Engine(
                            "arm-b: undecodable tree key in newest-version seek".into(),
                        ));
                    }
                };
                decode_value_or(&entry_val).map(|value| Some((ts, value)))
            }
            Some(Ok(_)) => Ok(None),
            Some(Err(e)) => Err(tree_err(e)),
            None => Ok(None),
        }
    }

    /// Per user key in `[lo, hi)`, the newest version at or below
    /// `snap` (B-SCAN-SKIP, card Work 3): the entries of one key are
    /// contiguous and ts-descending in the encoded order, so the walk
    /// skips versions with `ts > snap`, takes the first one at or below
    /// it, and skips the rest of the group. `None` value = tombstone.
    #[allow(clippy::type_complexity)]
    fn versions_visible_at(
        st: &ArmStore,
        lo: &[u8],
        hi: &[u8],
        snap: Ts,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, TxnErr> {
        let mut out = Vec::new();
        if lo >= hi {
            return Ok(out);
        }
        let it = st
            .tree
            .range(&encode::prefix_of(lo), &encode::prefix_of(hi))
            .map_err(tree_err)?;
        let mut current: Option<Vec<u8>> = None;
        let mut taken = false;
        for item in it {
            let (entry_key, entry_val) = item.map_err(tree_err)?;
            let (user_key, ts) = decode_key_or(&entry_key)?;
            if current.as_deref() != Some(user_key.as_slice()) {
                current = Some(user_key.clone());
                taken = false;
            }
            if taken || ts > snap {
                continue;
            }
            taken = true;
            out.push((user_key, decode_value_or(&entry_val)?));
        }
        Ok(out)
    }
}

/// Decode an entry value, blaming corruption (never panicking) when the
/// bytes are not one this arm could have written (B-ENC-ROUNDTRIP).
fn decode_value_or(entry_val: &[u8]) -> Result<Option<Vec<u8>>, TxnErr> {
    match encode::decode_value(entry_val) {
        Some(v) => Ok(v),
        None => Err(TxnErr::Engine("arm-b: undecodable tree value".into())),
    }
}

/// Decode an entry key, blaming corruption when it is not ours.
fn decode_key_or(entry_key: &[u8]) -> Result<(Vec<u8>, Ts), TxnErr> {
    match encode::decode_key(entry_key) {
        Some(pair) => Ok(pair),
        None => Err(TxnErr::Engine("arm-b: undecodable tree key".into())),
    }
}

impl TxnEngine for ArmB {
    type Txn = ArmBTxn;

    fn name(&self) -> &'static str {
        "arm-b (ts-in-key)"
    }

    fn begin(&self, iso: Isolation) -> ArmBTxn {
        let id = self.next_txn.fetch_add(1, Ordering::Relaxed);
        let snap = self.snaps.register(&self.seq);
        ArmBTxn {
            id,
            iso,
            ws: WriteSet::new(),
            snap,
        }
    }

    fn next_statement(&self, t: &mut ArmBTxn) {
        // RC takes a fresh snapshot at the statement boundary; RR keeps
        // the txn's one snapshot (A-RC).
        if t.iso == Isolation::ReadCommitted {
            t.snap = self.snaps.register(&self.seq);
        }
    }

    fn get(&self, t: &mut ArmBTxn, key: &[u8]) -> Result<Option<Vec<u8>>, TxnErr> {
        let st = self.usable()?;
        // Own writes first (A-OWN), then the one-seek snapshot read.
        if let Some(own) = t.ws.get(key) {
            return Ok(own.clone());
        }
        Self::version_at(st, key, t.snap.ts())
    }

    fn scan(
        &self,
        t: &mut ArmBTxn,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr> {
        let st = self.usable()?;
        // The limit applies after the merge (W-MERGE), so the base walk
        // covers the whole interval.
        let base = Self::versions_visible_at(st, lo, hi, t.snap.ts())?;
        Ok(t.ws
            .merge_scan(
                base.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))),
                lo,
                hi,
            )
            .into_iter()
            .take(limit)
            .collect())
    }

    fn update_with(
        &self,
        t: &mut ArmBTxn,
        key: &[u8],
        wait: Wait,
        f: &mut dyn FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    ) -> Result<(), TxnErr> {
        let st = self.usable()?;
        self.locks.acquire(key, t.id, wait)?;
        // Own-row rule (C-T0 §5.4): with an own write present the write
        // set has excluded foreign writers since its placement, so `f`
        // applies to the own value and no newer-version check runs.
        if let Some(own) = t.ws.get(key) {
            let new = f(own.as_deref());
            self.ws_set(t, key, new);
            return Ok(());
        }
        // B-NEWEST: one seek answers the newest committed version. The
        // RR rule (I-WW) and the RC EPQ base both read it here — after
        // the acquire returned, so a blocker's commit is included
        // whether it committed before the op began or during the wait.
        let newest = Self::newest_version(st, key)?;
        if t.iso == Isolation::RepeatableRead
            && let Some((ts, _)) = newest.as_ref()
            && *ts > t.snap.ts()
        {
            return Err(TxnErr::Serialization);
        }
        // R-EPQ: RC's `f` sees the newest committed value (a tombstone
        // reads as absent).
        let current = newest.map(|(_, v)| v);
        let new = f(current.as_ref().and_then(|v| v.as_deref()));
        self.ws_set(t, key, new);
        Ok(())
    }

    fn put(&self, t: &mut ArmBTxn, key: &[u8], value: &[u8], wait: Wait) -> Result<(), TxnErr> {
        // A-API: put is update_with with a constant Some(value) closure,
        // so both RR conflict rules apply to it (S4d).
        let v = value.to_vec();
        let mut always = |_old: Option<&[u8]>| Some(v.clone());
        self.update_with(t, key, wait, &mut always)
    }

    fn insert_unique(
        &self,
        t: &mut ArmBTxn,
        key: &[u8],
        value: &[u8],
        wait: Wait,
    ) -> Result<(), TxnErr> {
        let st = self.usable()?;
        self.locks.acquire(key, t.id, wait)?;
        // B-NEWEST: live = an own live write, else the newest committed
        // version live, whatever its ts (C-T0 §5.3: an own Delete or a
        // committed tombstone counts as not live; the RR newer-version
        // rule deliberately does not apply to key-existence ops).
        let live = match t.ws.get(key) {
            Some(own) => own.is_some(),
            None => Self::newest_version(st, key)?.is_some_and(|(_, v)| v.is_some()),
        };
        if live {
            return Err(TxnErr::UniqueViolation);
        }
        self.ws_set(t, key, Some(value.to_vec()));
        Ok(())
    }

    fn delete(&self, t: &mut ArmBTxn, key: &[u8], wait: Wait) -> Result<bool, TxnErr> {
        let mut existed = false;
        let mut to_none = |old: Option<&[u8]>| {
            existed = old.is_some();
            None
        };
        self.update_with(t, key, wait, &mut to_none)
            .map(|()| existed)
    }

    fn savepoint(&self, t: &mut ArmBTxn) -> SavepointId {
        t.ws.savepoint()
    }

    fn rollback_to(&self, t: &mut ArmBTxn, sp: SavepointId) {
        t.ws.rollback_to(sp);
        self.ws_rolled_back(t);
    }

    fn commit(&self, t: ArmBTxn) -> Result<Ts, TxnErr> {
        let st = self.usable()?;
        let ArmBTxn { id, ws, snap, .. } = t;
        // The txn's snapshot registration ends with the txn (C-RAII).
        drop(snap);
        // Ascending user-key order (the write set is an ordered map).
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            ws.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let versions = &self.versions;
        let bytes_applied = &self.bytes_applied;
        let res = commit_with(&self.seq, &self.locks, &self.poison, id, |ts| {
            for (key, value) in &entries {
                let entry_key = encode::encode_key(key, ts);
                let entry_val = encode::encode_value(value.as_deref());
                st.tree
                    .insert(&entry_key, &entry_val)
                    .map_err(|e| TxnErr::Engine(format!("arm-b apply: {e}")))?;
                versions.fetch_add(1, Ordering::Relaxed);
                bytes_applied.fetch_add(
                    (entry_key.len() + entry_val.len()) as u64,
                    Ordering::Relaxed,
                );
            }
            Ok(())
        });
        match res {
            Ok(ts) => {
                unpoisoned(&self.ws_bytes).remove(&id);
                Ok(ts)
            }
            Err(e) => Err(e),
        }
    }

    fn abort(&self, t: ArmBTxn) {
        let ArmBTxn { id, ws, snap, .. } = t;
        // Drop the write set and the snapshot registration (R- and
        // C-RAII); nothing was ever applied, so there is no trace (S10).
        drop(ws);
        drop(snap);
        unpoisoned(&self.ws_bytes).remove(&id);
        self.locks.release_all(id);
    }

    fn gc(&self) -> u64 {
        // Card Work 6: one full pass with watermark W = the oldest
        // registered snapshot (or visible_ts when none is active).
        let Some(st) = self.store.as_ref() else {
            return 0;
        };
        let watermark = self.snaps.oldest(&self.seq);
        // Pass 1 — decide, per user key (entries are contiguous and
        // ts-descending, so a key's versions above W all precede its
        // versions at or below W):
        //   keep every version with ts > W;
        //   keep the newest version with ts <= W — unless it is a
        //   tombstone with nothing above W left to need it, in which
        //   case elide it too (the card's "nothing older remains": the
        //   older versions are already gone, and with no version above
        //   W the key has nothing left for any reader >= W to find);
        //   delete every older version.
        let mut candidates: Vec<(Vec<u8>, u64)> = Vec::new();
        let it = match st.tree.range(&[], &encode::encode_table_upper()) {
            Ok(it) => it,
            Err(_) => return 0,
        };
        let mut current: Option<Vec<u8>> = None;
        let mut above_w: u64 = 0;
        let mut kept_newest_le = false;
        for item in it {
            let (entry_key, entry_val) = match item {
                Ok(pair) => pair,
                Err(_) => return 0,
            };
            let (user_key, ts) = match encode::decode_key(&entry_key) {
                Some(pair) => pair,
                None => return 0,
            };
            if current.as_deref() != Some(user_key.as_slice()) {
                current = Some(user_key);
                above_w = 0;
                kept_newest_le = false;
            }
            if ts > watermark {
                above_w += 1;
                continue;
            }
            if !kept_newest_le {
                kept_newest_le = true;
                let is_tombstone = entry_val.first() == Some(&encode::TAG_TOMBSTONE);
                if above_w == 0 && is_tombstone {
                    let bytes = (entry_key.len() + entry_val.len()) as u64;
                    candidates.push((entry_key, bytes));
                }
                continue;
            }
            let bytes = (entry_key.len() + entry_val.len()) as u64;
            candidates.push((entry_key, bytes));
        }
        // Pass 2 — delete outside the iterator (the tree forbids
        // mutating under a live cursor). A delete failure stops the
        // pass and reports only the bytes actually freed.
        let mut reclaimed = 0u64;
        let mut removed = 0u64;
        for (entry_key, bytes) in &candidates {
            if st.tree.delete(entry_key).is_err() {
                break;
            }
            reclaimed += *bytes;
            removed += 1;
        }
        self.versions.fetch_sub(removed, Ordering::Relaxed);
        reclaimed
    }

    fn stats(&self) -> EngineStats {
        // Card Work 7. A store error inside a metric walk reports that
        // metric as 0 — fail-stop territory (B-FAIL-STOP), never a
        // panic and never a wrong number presented as a right one.
        let mut tree_bytes = 0u64;
        let mut logical = 0u64;
        if let Some(st) = self.store.as_ref() {
            tree_bytes = st.pages.page_count().map_or(0, |n| n as u64 * st.page_size);
            let visible = self.seq.visible_ts();
            if let Ok(visible_versions) =
                Self::versions_visible_at(st, b"", &encode::encode_table_upper(), visible)
            {
                for (key, value) in visible_versions {
                    if let Some(v) = value {
                        logical += key.len() as u64 + v.len() as u64;
                    }
                }
            }
        }
        let write_set_bytes: u64 = unpoisoned(&self.ws_bytes).values().sum();
        EngineStats {
            tree_bytes,
            logical_bytes: logical,
            versions: self.versions.load(Ordering::Relaxed),
            undo_bytes: 0,
            undo_spilled_bytes: 0,
            lock_entries: self.locks.entries(),
            write_set_bytes,
            bytes_applied: self.bytes_applied.load(Ordering::Relaxed),
        }
    }

    /// Card X-1a-F3 / X-2 Work 8: the arm's own shared lock table — the
    /// instance every acquire uses and `commit_with` releases on. Never
    /// a dummy: the suite's parked-waiter barrier watches this table.
    #[doc(hidden)]
    fn lock_table(&self) -> &LockTable {
        &self.locks
    }

    /// Card X-1a-F3 / X-2 Work 8: the arm's own shared sequencer — the
    /// instance `commit_with` receives, whose apply gate can hold only
    /// this engine's commits.
    #[doc(hidden)]
    fn sequencer(&self) -> &Sequencer {
        &self.seq
    }
}
