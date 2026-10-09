//! G0-gc (C-T0 §11): GC, the watermark and the compaction filter (§9), with
//! the registration rules they rest on (§3.1: snapshots, AS OF, open views),
//! the read rule (§4), intent removal (§7.3) and status truncation (§7.4),
//! over an abstract LSM that reproduces `MemKv` LSM mode's semantics
//! (checked exhaustively against it by `tests/gc_lsm_diff.rs`).
//!
//! The abstract LSM mirrors `crates/nucleus-kv/src/mem.rs`: writes (including
//! `DeleteRange`, stored as a range tombstone) go to the memtable; `flush`
//! creates an L0 file; `Compact` merges chosen L0 files (expanded to every
//! same-level and lower-level file overlapping their combined range) into the
//! bottommost L1, running one fresh filter stream per compaction; range
//! tombstones are carried and dropped only at the bottom level when no file
//! above the output overlaps them; a compaction-filter drop is **visible to
//! already-open snapshots** through the live drop registry (era + dropped,
//! G0-R5-1: the adversarial choice the kv contract allows). Views are copies
//! of the whole published state (files + memtable), paired with the era they
//! opened at.
//!
//! ## Workload (fixed; no initial choice is needed)
//!
//! Preloaded KV (visible, `visible_ts = 20`, durable `W = 10`): logical keys
//! `/t/0/0` (k0) and `/t/0/1` (k1) under storage id 0, laid out so that
//! versions of one key sit in files whose ranges do not overlap (versions
//! sort newest first, so same-key files are disjoint and a per-file
//! compaction is a stream on its own):
//!
//! | where | file | content |
//! |---|---|---|
//! | L1 | A | k0@10 = live 'a' |
//! | L0 | B | k0@20 = tombstone |
//! | L0 | C | k1@10 = live 'b' |
//! | L0 | D | k1@20 = live 'c' |
//!
//! Actors (every one a separate step, interleaved by the BFS):
//! - **W0**: places a delete intent on k1 (under the latest catalog's storage
//!   id), commits (fused: record + status + visible + release + resolve
//!   queue), and is resolved by the async resolver.
//! - **W1**: the DDL txn: places an intent on the catalog key, commits as a
//!   `TRUNCATE` (a new storage id 1 recorded as a catalog version), resolved
//!   like any write.
//! - **R**: a snapshot reader: takes `S` (reading `visible_ts` and
//!   registering it; one step, §3.1), finishes.
//! - **AS**: an `AS OF t=15` reader: registers `t` (checked against the
//!   published `W` and `visible_ts`, resolving the catalog at `t`), finishes.
//!   Re-runs after a crash (sessions survive; snapshots do not).
//! - **FK**: one latest-state view (a deferred FK check): opens (counter +
//!   `vts` registered, file-set copy) and stays open across compactions
//!   until it closes.
//! - **GC job**: computes `W` from the registry and publishes it (one
//!   registry critical section), then syncs `/sys/gc_w` (a separate step;
//!   the only unsynced write in the model); removes newest-`<= W`
//!   tombstones with the §9.2 `DeleteRange`; retires the truncated storage
//!   (intent removal through §7.3, then the prefix `DeleteRange`).
//! - **Compaction**: `Flush`, per-file `Compact` of a chosen L0 file, and
//!   `FullCompact` (the quiesce precondition: flush + compact all of L0 +
//!   re-stream the unconsumed L1 files).
//! - **Truncation** of ended txns (§7.4 conditions 0-2).
//! - **Later**: one commit by an unrelated txn on another table, fused into
//!   one step. Without it nothing commits after the DDL, `W` (`<= visible_ts`)
//!   never passes the DDL's commit ts and the retire never runs while W0's
//!   pre-DDL intent is still unresolved (seed 35).
//! - **Crash** (once, any time): loses the unsynced `/sys/gc_w` write only;
//!   boots from the durable `W` (which is `>= ` every `W` a clean GC step
//!   acted on), widens or keeps the AS OF retention window, kills the
//!   registry, both txns and the FK view; committed statuses survive.
//!   Afterwards the GC job recomputes `W` (seed 22's widened window).
//!
//! Reads change no shared state, so every read is a per-state check: each
//! registered snapshot (all `>= W`, the clause of I-GC) and `visible_ts`
//! must read what the ghost commit history says (I-GC and I-ATOMIC), and the
//! open FK view must keep returning, for every key, what its own copy holds
//! (I-GC's open-view clause; the live drop registry may hide a raw key the
//! copy still holds -- the adversarial choice).
//!
//! ## Seed table (every seed -> the behaviour that catches it)
//!
//! | seed | bug (one local deviation) | catch |
//! |---|---|---|
//! | 3 | filter drops every tombstone `<= W` | compact file B at `W >= 20`: k0@20 (the newest `<= W` of its stream) is dropped, file A still holds k0@10; a registered `S >= 20` reads 'a' instead of not-found |
//! | 21 | taking `S` and registering it are two steps | R reads S=20; W0 commits+resolves k1@30; the GC job publishes W=30 (the registry lacks S) and the tombstone DeleteRange at 30 removes k1@{30,20,10}; R registers and reads k1 at 20: not-found instead of 'c' |
//! | 22 | the GC job publishes `W = computed`, not the max | pre-crash `W = 40` synced and acted (k1@{20,10} shadowed by k1@40 in one stream); crash + widened window: computed = 2, `W` regresses to 2, AS OF t=15 is admitted below the data GC dropped |
//! | 31 | AS OF checks `t` against the unsynced `/sys/gc_w` | publish W=30 (sync pending, durable 10): t=15 passes the durable floor, is registered below the published W; I-GC's "all registered ts `>= W`" clause fires |
//! | 33 | filter state shared across streams | compact C then D at W=20: stream 1 keeps k1@10 and marks k1 seen; stream 2 (shared state) drops k1@20 as "shadowed"; a registered `S >= 20` reads 'b' instead of 'c' |
//! | 34 | the tombstone DeleteRange start is exclusive | GcTombstones at W >= 20 hides k0@10 but keeps k0@20; after FullCompact the quiesce check still finds a tombstone as k0's newest version `<= W` |
//! | 35 | the retire's DeleteRange runs before the intent removals | W1 truncates @30, W0's k1 intent (committed @40, unresolved) sits under the retired prefix; the range first hides it so the §7.3 re-reads no-op while the retire's own bookkeeping still decrements: W0 truncates and its raw intent survives without a status entry (I-TRUNC, checked over raw storage) |
//! | 42 | the filter is handed `W` before `/sys/gc_w` is synced | publish W=30 (unsynced): the filter drops k1@{20,10} (shadowed by k1@30); the crash loses the write, W boots at 10, AS OF t=15 reads dropped data |
//! | 49 | AS OF resolves the latest catalog, not the catalog at `t` | after the TRUNCATE's catalog version, AS OF t=15 (admitted, W <= 15) resolves storage 1 (empty) and reads not-found instead of k0@10 = 'a' |
//! | 55 | the latest-state view's `vts` is not a registered min-term | the FK view opens at vts=20; W0's k1@40 commit lets W pass 20; the filter drops k1@{20,10} (shadowed by k1@40) and the registry hides them from the copy: the view's read of k1 changes while it is open |
//!
//! ## Scope cuts
//!
//! - The commit pipeline is fused into one atomic `Commit` step (record +
//!   status + visible + release + resolve queue). I-VIS, I-ACK, I-DURABLE and
//!   I-WAL-ORDER are G0-commit's scope; no seed of this model needs them split.
//! - The crash loses only the unsynced `/sys/gc_w` write (the crash the §11
//!   row names). Data writes are durable on write, so I-ATOMIC is re-checked
//!   here only through ghost reads at `visible_ts` (no all-or-none crash test).
//! - No abort path (no seed needs one). A txn that is active at the crash is
//!   Lost: older-epoch, no status entry, its intent unreadable (§4 treats a
//!   missing older-epoch owner as Aborted) and never truncated.
//! - Status truncation is current-epoch only; the older-epoch conditions
//!   (sweep counter) are G0-commit's seed 51.
//! - Compaction compacts one chosen L0 file per step (per-file, as the card
//!   states) plus the whole-keyspace `FullCompact`; arbitrary subsets are
//!   covered against `MemKv` by `tests/gc_lsm_diff.rs`.
//! - `Flush` runs only before the first commit, so versions written after it
//!   reach files only through the GC job's full compaction; per-file `Compact`
//!   runs at any time, over the preloaded files.
//! - The FK view opens only before the first commit (`vts = 20`). Opening it
//!   later as well roughly doubles the state space past the card's budget.
//! - The retire is gated on no *active* intent under the retired prefix (a
//!   pending txn's intent is not §7.3-removable; TRUNCATE holds
//!   AccessExclusive, so only already-placed intents of concurrent txns are in
//!   scope, and those are the ones the model covers: committed-unresolved).
//! - Seed 35's deviation is the order swap inside the retire step: the
//!   `DeleteRange` batch is written first, the §7.3 re-reads then find the
//!   intents hidden and write nothing, while the retire's driver bookkeeping
//!   (planned before the swap) still records `last_removal_counter` and
//!   decrements `intent_count`. The catch is I-TRUNC read over *raw* storage
//!   ("no reader **can** observe an intent of T after T's status entry is
//!   gone"): the prefix range tombstone hides the entry from merged reads, so
//!   no live reader observes it -- the ghost check is the worst-case reader
//!   over what is physically there. In the clean model the check is sound:
//!   every decrement is paired with a batch that deletes the raw entry, and
//!   placements are paired with an increment, before any truncation.

use std::collections::{BTreeMap, BTreeSet};

use crate::Model;

pub type Ts = u8;

/// The logical keys' payloads: a one-byte row value, or a delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Data {
    Live(u8),
    Tomb,
}

fn data_read(d: Data) -> Option<u8> {
    match d {
        Data::Live(p) => Some(p),
        Data::Tomb => None,
    }
}

/// Seeded bugs of C-T0 §11 owned by this model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// Seed 3: "Compaction filter drops the newest tombstone `<= W`."
    FilterDropsNewestTombstones,
    /// Seed 21: "Taking `S` and registering it are separate steps."
    SnapshotRegistrationSplit,
    /// Seed 22: "`W` allowed to decrease."
    WatermarkAllowedToDecrease,
    /// Seed 31: "AS OF registration checked against an unpublished `W`."
    AsOfCheckedAgainstUnpublishedW,
    /// Seed 33: "Compaction-filter state shared across streams."
    FilterStateSharedAcrossStreams,
    /// Seed 34: "GC DeleteRange with an exclusive start."
    DeleteRangeExclusiveStart,
    /// Seed 35: "Retired-prefix DeleteRange without removing intents first."
    RetireRangeBeforeIntentRemoval,
    /// Seed 42: "Compaction filter handed `W` before `/sys/gc_w` is synced."
    FilterHandedUnsyncedW,
    /// Seed 49: "AS OF resolves the latest catalog instead of the catalog at `t`."
    AsOfResolvesLatestCatalog,
    /// Seed 55: "Latest-state view does not hold `W`."
    LatestStateViewDoesNotHoldW,
}

impl Bug {
    pub const ALL: [Bug; 10] = [
        Bug::FilterDropsNewestTombstones,
        Bug::SnapshotRegistrationSplit,
        Bug::WatermarkAllowedToDecrease,
        Bug::AsOfCheckedAgainstUnpublishedW,
        Bug::FilterStateSharedAcrossStreams,
        Bug::DeleteRangeExclusiveStart,
        Bug::RetireRangeBeforeIntentRemoval,
        Bug::FilterHandedUnsyncedW,
        Bug::AsOfResolvesLatestCatalog,
        Bug::LatestStateViewDoesNotHoldW,
    ];

    pub fn seed(self) -> u8 {
        match self {
            Bug::FilterDropsNewestTombstones => 3,
            Bug::SnapshotRegistrationSplit => 21,
            Bug::WatermarkAllowedToDecrease => 22,
            Bug::AsOfCheckedAgainstUnpublishedW => 31,
            Bug::FilterStateSharedAcrossStreams => 33,
            Bug::DeleteRangeExclusiveStart => 34,
            Bug::RetireRangeBeforeIntentRemoval => 35,
            Bug::FilterHandedUnsyncedW => 42,
            Bug::AsOfResolvesLatestCatalog => 49,
            Bug::LatestStateViewDoesNotHoldW => 55,
        }
    }
}

// ---- The C-T0 §2.2 layout (the same shape conformance::layout implements;
// the lib cannot import nucleus-kv, so it is restated here). ----

/// `L` in the §2.2 layout: `be32(len) ‖ bytes`, prefix-free for any input.
pub fn logical(l: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + l.len());
    out.extend_from_slice(&(l.len() as u32).to_be_bytes());
    out.extend_from_slice(l);
    out
}

/// The `L ‖ 0x00` intent slot: sorts before every version of `L`.
pub fn intent_key(l: &[u8]) -> Vec<u8> {
    let mut k = logical(l);
    k.push(0x00);
    k
}

/// The `L ‖ 0x01 ‖ be64(u64::MAX - ts) ‖ 0x01` version key: newest first.
pub fn version_key(l: &[u8], ts: u64) -> Vec<u8> {
    let mut k = logical(l);
    k.push(0x01);
    k.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    k.push(0x01);
    k
}

/// `L ‖ 0x02`: the exclusive upper bound of every entry of `L`.
pub fn end_key(l: &[u8]) -> Vec<u8> {
    let mut k = logical(l);
    k.push(0x02);
    k
}

/// What follows the logical key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Intent,
    Version(u64),
}

/// Splits an [`intent_key`] or [`version_key`]; `None` for anything else.
pub fn parse_key(key: &[u8]) -> Option<(Vec<u8>, EntryKind)> {
    if key.len() < 5 {
        return None;
    }
    let mut n = [0u8; 4];
    n.copy_from_slice(&key[..4]);
    let len = u32::from_be_bytes(n) as usize;
    let split = 4usize.checked_add(len)?;
    if key.len() <= split {
        return None;
    }
    let l = key[4..split].to_vec();
    match key[split] {
        0x00 if key.len() == split + 1 => Some((l, EntryKind::Intent)),
        0x01 if key.len() == split + 10 && key[split + 9] == 0x01 => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&key[split + 1..split + 9]);
            Some((l, EntryKind::Version(u64::MAX - u64::from_be_bytes(b))))
        }
        _ => None,
    }
}

/// §2.2 version-value headers.
pub const HEADER_LIVE: u8 = 0x00;
pub const HEADER_TOMBSTONE: u8 = 0x02;

fn version_value(d: Data) -> Vec<u8> {
    match d {
        Data::Live(p) => vec![HEADER_LIVE, p],
        Data::Tomb => vec![HEADER_TOMBSTONE],
    }
}

fn value_is_tombstone(v: &[u8]) -> bool {
    v.first() == Some(&HEADER_TOMBSTONE)
}

/// A version value's live payload, if it is a live one.
fn value_live_payload(v: &[u8]) -> Option<u8> {
    match v.first() {
        Some(&HEADER_LIVE) => v.get(1).copied(),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxnId {
    pub epoch: u8,
    pub n: u8,
}

/// The intent value: `epoch ‖ n ‖ data`, with `data` = `data_value`.
fn intent_value(owner: TxnId, d: Data) -> Vec<u8> {
    let mut v = match d {
        Data::Live(p) => vec![HEADER_LIVE, p],
        Data::Tomb => vec![HEADER_TOMBSTONE],
    };
    v.insert(0, owner.n);
    v.insert(0, owner.epoch);
    v
}

fn parse_intent(v: &[u8]) -> Option<(TxnId, Data)> {
    if v.len() < 3 {
        return None;
    }
    let owner = TxnId {
        epoch: v[0],
        n: v[1],
    };
    let d = match v[2] {
        HEADER_LIVE => Data::Live(*v.get(3)?),
        HEADER_TOMBSTONE => Data::Tomb,
        _ => return None,
    };
    Some((owner, d))
}

// ---- The abstract LSM (a value-type port of MemKv's LSM mode). ----

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// One write into the LSM.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LsmOp {
    Put(Key, Value),
    Delete(Key),
    DeleteRange { start: Key, end: Key },
}

/// A stored point entry: a value, or a delete marker hiding older entries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LEntry {
    Put(Value),
    Tomb,
}

type SeqEntry = (u64, LEntry);
/// A range tombstone: `(seq, start, end)` covering `[start, end)`.
type Rt = (u64, Key, Key);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct File {
    id: u64,
    /// Closed key range the content touches.
    lo: Key,
    hi: Key,
    entries: BTreeMap<Key, SeqEntry>,
    rts: Vec<Rt>,
}

/// The reference §9.2 drop rule (what `SpecGcFilter` implements).
#[derive(Clone, Copy, Debug)]
pub struct GcSpec {
    pub w: u64,
}

/// The filter rules the model needs: the reference rule, plus seed 3's
/// tombstone-dropping variant.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StreamRule {
    Spec(GcSpec),
    DropsTombsLeW(u64),
}

/// The newest opinion about one key over some sources.
enum View<'a> {
    Put(u64, &'a Value),
    Tomb(u64),
    Hidden,
}

struct Src<'a> {
    entries: &'a BTreeMap<Key, SeqEntry>,
    rts: &'a [Rt],
}

fn covers(start: &[u8], end: &[u8], key: &[u8]) -> bool {
    start <= key && key < end
}

fn range_is_empty(start: &[u8], end: &[u8]) -> bool {
    start >= end
}

/// The highest-seq opinion about `key` over `srcs`.
fn newest<'a>(key: &[u8], srcs: &[Src<'a>]) -> Option<View<'a>> {
    let mut best: Option<(u64, View<'a>)> = None;
    for src in srcs {
        if let Some((seq, e)) = src.entries.get(key) {
            let v = match e {
                LEntry::Put(v) => View::Put(*seq, v),
                LEntry::Tomb => View::Tomb(*seq),
            };
            if best.as_ref().is_none_or(|(s, _)| *seq > *s) {
                best = Some((*seq, v));
            }
        }
        for (seq, start, end) in src.rts {
            if covers(start, end, key) && best.as_ref().is_none_or(|(s, _)| *seq > *s) {
                best = Some((*seq, View::Hidden));
            }
        }
    }
    best.map(|(_, v)| v)
}

/// The abstract LSM: memtable + L0 + the bottommost L1, plus the live
/// GC-drop registry. A pure value; every step clones it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Lsm {
    seq: u64,
    memtable: BTreeMap<Key, SeqEntry>,
    mem_rts: Vec<Rt>,
    next_file: u64,
    levels: Vec<Vec<File>>,
    dropped: BTreeMap<Key, u64>,
    era: u64,
}

impl Lsm {
    pub fn new() -> Self {
        Self {
            seq: 0,
            memtable: BTreeMap::new(),
            mem_rts: Vec::new(),
            next_file: 1,
            levels: vec![Vec::new(), Vec::new()],
            dropped: BTreeMap::new(),
            era: 0,
        }
    }

    pub fn apply(&mut self, op: LsmOp) {
        self.seq += 1;
        match op {
            LsmOp::Put(k, v) => {
                self.memtable.insert(k, (self.seq, LEntry::Put(v)));
            }
            LsmOp::Delete(k) => {
                self.memtable.insert(k, (self.seq, LEntry::Tomb));
            }
            LsmOp::DeleteRange { start, end } => {
                if !range_is_empty(&start, &end) {
                    self.mem_rts.push((self.seq, start, end));
                }
            }
        }
    }

    fn srcs(&self) -> Vec<Src<'_>> {
        let mut srcs = vec![Src {
            entries: &self.memtable,
            rts: &self.mem_rts,
        }];
        for f in self.levels.iter().flatten() {
            srcs.push(Src {
                entries: &f.entries,
                rts: &f.rts,
            });
        }
        srcs
    }

    /// The merged visible rows in `[lo, hi)`: every key whose newest opinion
    /// is a put (fresh-view semantics; the drop registry hides nothing from
    /// a view opening now).
    pub fn merged_scan(&self, lo: &Key, hi: &Key) -> Vec<(Key, Value)> {
        let srcs = self.srcs();
        let mut keys: Vec<&Key> = Vec::new();
        for src in &srcs {
            keys.extend(
                src.entries
                    .keys()
                    .filter(|k| k.as_slice() >= lo.as_slice() && k.as_slice() < hi.as_slice()),
            );
        }
        keys.sort_unstable();
        keys.dedup();
        let mut out = Vec::new();
        for k in keys {
            if let Some(View::Put(_, v)) = newest(k, &srcs) {
                out.push((k.clone(), v.clone()));
            }
        }
        out
    }

    /// The merged rows an already-open view (a copy opened at
    /// `copy.open_era()`) returns now: the copy's own state, minus the keys
    /// the live registry dropped after that era (the adversarial choice).
    pub fn snap_scan(copy: &Lsm, live: &Lsm, lo: &Key, hi: &Key) -> Vec<(Key, Value)> {
        copy.merged_scan(lo, hi)
            .into_iter()
            .filter(|(k, _)| !live.dropped.get(k).is_some_and(|e| *e > copy.era))
            .collect()
    }

    /// The merged latest value of one raw key (a fresh view).
    pub fn merged_get(&self, key: &Key) -> Option<Value> {
        match newest(key, &self.srcs()) {
            Some(View::Put(_, v)) => Some(v.clone()),
            _ => None,
        }
    }

    /// The era this state opened at (paired with the live `dropped` map).
    pub fn open_era(&self) -> u64 {
        self.era
    }

    pub fn level0_len(&self) -> usize {
        self.levels.first().map(Vec::len).unwrap_or(0)
    }

    /// A preloaded file (the model's initial LSM).
    fn with_file(mut self, level: usize, entries: Vec<(Key, Value)>) -> Self {
        let (lo, hi) = bounds_of(&entries);
        let mut map: BTreeMap<Key, SeqEntry> = BTreeMap::new();
        for (k, v) in entries {
            self.seq += 1;
            map.insert(k, (self.seq, LEntry::Put(v)));
        }
        let id = self.next_file;
        self.next_file += 1;
        self.levels[level].push(File {
            id,
            lo,
            hi,
            entries: map,
            rts: Vec::new(),
        });
        self
    }

    /// Memtable -> a new L0 file; `true` when one was created.
    pub fn flush(&mut self) -> bool {
        if self.memtable.is_empty() && self.mem_rts.is_empty() {
            return false;
        }
        let entries = std::mem::take(&mut self.memtable);
        let rts = std::mem::take(&mut self.mem_rts);
        let Some((lo, hi)) = bounds(&entries, &rts) else {
            self.memtable = entries;
            self.mem_rts = rts;
            return false;
        };
        let id = self.next_file;
        self.next_file += 1;
        self.levels[0].push(File {
            id,
            lo,
            hi,
            entries,
            rts,
        });
        true
    }

    /// `compact(level 0, files)`: merges the chosen L0 files (by position),
    /// expanded to every same-level and lower-level file overlapping their
    /// combined range, through one fresh filter stream per compaction.
    /// Returns `true` when the compaction ran (the positions were valid).
    pub fn compact_files(&mut self, positions: &[usize], filter: Option<GcSpec>) -> bool {
        let rule = filter.map(StreamRule::Spec);
        self.compact_rule(positions, rule.as_ref(), None)
    }

    pub(crate) fn compact_rule(
        &mut self,
        positions: &[usize],
        rule: Option<&StreamRule>,
        shared_seen: Option<&mut BTreeSet<Key>>,
    ) -> bool {
        if positions.is_empty() || self.levels.len() < 2 {
            return false;
        }
        if positions.iter().any(|p| *p >= self.levels[0].len()) {
            return false;
        }
        let mut inputs = Vec::new();
        let mut left = Vec::new();
        for (i, f) in self.levels[0].drain(..).enumerate() {
            if positions.contains(&i) {
                inputs.push(f);
            } else {
                left.push(f);
            }
        }
        self.levels[0] = left;
        let _ = self.run_compaction(rule, shared_seen, inputs);
        true
    }

    /// The whole-keyspace compaction (the quiesce precondition): flush,
    /// compact all of L0, then re-stream every L1 file that compaction did
    /// not consume. Returns the number of dropped keys.
    pub fn compact_all(&mut self, filter: Option<GcSpec>) -> usize {
        let rule = filter.map(StreamRule::Spec);
        self.compact_all_rule(rule.as_ref(), None)
    }

    pub(crate) fn compact_all_rule(
        &mut self,
        rule: Option<&StreamRule>,
        mut shared_seen: Option<&mut BTreeSet<Key>>,
    ) -> usize {
        self.flush();
        let l1_before: Vec<u64> = self.levels[1].iter().map(|f| f.id).collect();
        let mut dropped = self.compact_all_l0(rule, shared_seen.as_deref_mut());
        for id in l1_before {
            if self.levels[1].iter().any(|f| f.id == id) {
                dropped += self.restream_rule(rule, shared_seen.as_deref_mut(), id);
            }
        }
        dropped
    }

    /// The compaction half of the model's whole-keyspace GC pass: flush,
    /// then compact all of L0 (one stream), without the kv's bottom-level
    /// re-streams (not needed by any seed; the DeleteRange job fixes the
    /// property quiesce checks regardless of file layout).
    pub(crate) fn compact_all_l0(
        &mut self,
        rule: Option<&StreamRule>,
        shared_seen: Option<&mut BTreeSet<Key>>,
    ) -> usize {
        self.flush();
        let mut dropped = 0usize;
        if !self.levels[0].is_empty() {
            let inputs = self.take_files(0, |_| true);
            let (_, keys) = self.run_compaction(rule, shared_seen, inputs);
            dropped += keys.len();
        }
        dropped
    }

    /// Rewrites one bottommost file through a fresh stream.
    fn restream_rule(
        &mut self,
        rule: Option<&StreamRule>,
        shared_seen: Option<&mut BTreeSet<Key>>,
        id: u64,
    ) -> usize {
        let inputs = self.take_files(1, |i| i == id);
        if inputs.is_empty() {
            return 0;
        }
        let above: Vec<File> = self.levels[0].clone();
        let (entries, rts, dropped) = merge(&inputs, rule, shared_seen, &above);
        if let Some((lo, hi)) = bounds(&entries, &rts) {
            let nid = self.next_file;
            self.next_file += 1;
            self.levels[1].push(File {
                id: nid,
                lo,
                hi,
                entries,
                rts,
            });
        }
        self.record_drops(&dropped);
        dropped.len()
    }

    /// Moves the files of `level` whose id satisfies `keep` out of the level.
    fn take_files(&mut self, level: usize, keep: impl Fn(u64) -> bool) -> Vec<File> {
        let (mut taken, mut left) = (Vec::new(), Vec::new());
        for f in self.levels[level].drain(..) {
            if keep(f.id) {
                taken.push(f);
            } else {
                left.push(f);
            }
        }
        self.levels[level] = left;
        taken
    }

    /// The compaction itself (mem.rs discipline): same-level expansion until
    /// the combined range stops growing, then consume the overlapping files
    /// of the level below, merge through one stream, publish one output file
    /// at `level+1`, record the filter drops under a fresh era.
    fn run_compaction(
        &mut self,
        rule: Option<&StreamRule>,
        shared_seen: Option<&mut BTreeSet<Key>>,
        mut inputs: Vec<File>,
    ) -> (Vec<u64>, Vec<Key>) {
        while let Some((lo, hi)) = range_of(&inputs) {
            let extra = self.take_files(0, |_| true);
            let mut grew = Vec::new();
            let mut left = Vec::new();
            for f in extra {
                if f.lo <= hi && f.hi >= lo {
                    grew.push(f);
                } else {
                    left.push(f);
                }
            }
            self.levels[0] = left;
            if grew.is_empty() {
                break;
            }
            inputs.extend(grew);
        }
        let Some((lo, hi)) = range_of(&inputs) else {
            return (Vec::new(), Vec::new());
        };
        let below = self.take_files(1, |_| true);
        let (mut grew, mut left) = (Vec::new(), Vec::new());
        for f in below {
            if f.lo <= hi && f.hi >= lo {
                grew.push(f);
            } else {
                left.push(f);
            }
        }
        self.levels[1] = left;
        inputs.extend(grew);

        let above: Vec<File> = self.levels[0].clone();
        let (entries, rts, dropped) = merge(&inputs, rule, shared_seen, &above);
        let mut ids = Vec::new();
        if let Some((flo, fhi)) = bounds(&entries, &rts) {
            let id = self.next_file;
            self.next_file += 1;
            self.levels[1].push(File {
                id,
                lo: flo,
                hi: fhi,
                entries,
                rts,
            });
            ids.push(id);
        }
        self.record_drops(&dropped);
        (ids, dropped)
    }

    fn record_drops(&mut self, keys: &[Key]) {
        if keys.is_empty() {
            return;
        }
        self.era += 1;
        for k in keys {
            self.dropped.insert(k.clone(), self.era);
        }
    }

    /// Every raw intent entry (memtable and files) that has **no removal
    /// marker above it**: no delete marker at its key with a higher write
    /// seq, in the memtable or any file. A `§7.3` removal writes exactly that
    /// marker, so a file's leftover entry under one is properly removed; an
    /// entry hidden only by a covering range tombstone (a retired prefix
    /// whose DeleteRange ran first) is not. Ghost access: what is physically
    /// there.
    pub(crate) fn raw_intent_entries(&self) -> Vec<(Key, TxnId)> {
        let mut puts: Vec<(Key, u64, TxnId)> = Vec::new();
        let mut tombs: Vec<(Key, u64)> = Vec::new();
        let see = |k: &Key,
                   v: &SeqEntry,
                   puts: &mut Vec<(Key, u64, TxnId)>,
                   tombs: &mut Vec<(Key, u64)>| {
            if let (seq, LEntry::Put(v)) = v {
                if matches!(parse_key(k), Some((_, EntryKind::Intent))) {
                    if let Some((owner, _)) = parse_intent(v) {
                        puts.push((k.clone(), *seq, owner));
                    }
                }
            } else if let (seq, LEntry::Tomb) = v {
                tombs.push((k.clone(), *seq));
            }
        };
        for (k, v) in &self.memtable {
            see(k, v, &mut puts, &mut tombs);
        }
        for f in self.levels.iter().flatten() {
            for (k, v) in &f.entries {
                see(k, v, &mut puts, &mut tombs);
            }
        }
        puts.into_iter()
            .filter(|(k, seq, _)| !tombs.iter().any(|(tk, tseq)| tk == k && *tseq > *seq))
            .map(|(k, _, owner)| (k, owner))
            .collect()
    }

    /// Every raw key present (memtable and files), deduplicated and sorted.
    pub(crate) fn raw_keys(&self) -> Vec<Key> {
        let mut keys: Vec<Key> = self.memtable.keys().cloned().collect();
        for f in self.levels.iter().flatten() {
            keys.extend(f.entries.keys().cloned());
        }
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// Deterministic content digest for the differential harness's dedup:
    /// everything behaviourally observable except file ids (and the next-file
    /// counter).
    pub fn content_signature(&self) -> Vec<u8> {
        fn chunk(b: &mut Vec<u8>, bytes: &[u8]) {
            b.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            b.extend_from_slice(bytes);
        }
        fn entries_sig(b: &mut Vec<u8>, entries: &BTreeMap<Key, SeqEntry>) {
            b.extend_from_slice(&(entries.len() as u32).to_be_bytes());
            for (k, (seq, e)) in entries {
                chunk(b, k);
                b.extend_from_slice(&seq.to_be_bytes());
                match e {
                    LEntry::Put(v) => {
                        b.push(1);
                        chunk(b, v);
                    }
                    LEntry::Tomb => b.push(0),
                }
            }
        }
        fn rts_sig(b: &mut Vec<u8>, rts: &[Rt]) {
            let mut sorted: Vec<&Rt> = rts.iter().collect();
            sorted.sort();
            b.extend_from_slice(&(sorted.len() as u32).to_be_bytes());
            for (seq, s, e) in sorted {
                b.extend_from_slice(&seq.to_be_bytes());
                chunk(b, s);
                chunk(b, e);
            }
        }
        let mut b = Vec::new();
        entries_sig(&mut b, &self.memtable);
        rts_sig(&mut b, &self.mem_rts);
        for level in &self.levels {
            b.extend_from_slice(&(level.len() as u32).to_be_bytes());
            for f in level {
                entries_sig(&mut b, &f.entries);
                rts_sig(&mut b, &f.rts);
            }
        }
        b.extend_from_slice(&self.era.to_be_bytes());
        b.extend_from_slice(&(self.dropped.len() as u32).to_be_bytes());
        for (k, e) in &self.dropped {
            chunk(&mut b, k);
            b.extend_from_slice(&e.to_be_bytes());
        }
        b
    }
}

fn bounds_of(entries: &[(Key, Value)]) -> (Key, Key) {
    let lo = entries.first().map(|(k, _)| k.clone()).unwrap_or_default();
    let hi = entries.last().map(|(k, _)| k.clone()).unwrap_or_default();
    (lo, hi)
}

fn bounds(entries: &BTreeMap<Key, SeqEntry>, rts: &[Rt]) -> Option<(Key, Key)> {
    let mut lo: Option<Key> = None;
    let mut hi: Option<Key> = None;
    let mut see = |k: &Key| {
        if lo.as_ref().is_none_or(|l| *l > *k) {
            lo = Some(k.clone());
        }
        if hi.as_ref().is_none_or(|h| *h < *k) {
            hi = Some(k.clone());
        }
    };
    for k in entries.keys() {
        see(k);
    }
    for (_, s, e) in rts {
        see(s);
        see(e);
    }
    Some((lo?, hi?))
}

fn range_of(files: &[File]) -> Option<(Key, Key)> {
    let mut it = files.iter();
    let first = it.next()?;
    let (mut lo, mut hi) = (first.lo.clone(), first.hi.clone());
    for f in it {
        if f.lo < lo {
            lo = f.lo.clone();
        }
        if f.hi > hi {
            hi = f.hi.clone();
        }
    }
    Some((lo, hi))
}

/// The §9.2 drop rule within one stream: a version `<= W` may be dropped when
/// a newer version `<= W` of the same logical key was already seen in this
/// stream; the newest `<= W` seen for a key is never dropped; versions `> W`,
/// intents and keys outside the layout are kept. Seed 3's variant drops
/// every tombstone `<= W`.
fn stream_drop(rule: &StreamRule, seen: &mut BTreeSet<Key>, key: &Key, value: &Value) -> bool {
    match rule {
        StreamRule::Spec(spec) => match parse_key(key) {
            Some((l, EntryKind::Version(ts))) if ts <= spec.w => !seen.insert(l),
            _ => false,
        },
        StreamRule::DropsTombsLeW(w) => {
            matches!(parse_key(key), Some((_, EntryKind::Version(ts))) if ts <= *w)
                && value_is_tombstone(value)
        }
    }
}

/// One compaction over `files` (mem.rs `merge`): the newest opinion per key
/// wins; visible puts pass the filter in ascending key order; delete markers
/// are carried; entries hidden by a covering range tombstone are dropped;
/// range tombstones are carried, and dropped when no surviving file above
/// the output overlaps their range (always the bottom level here).
fn merge(
    files: &[File],
    rule: Option<&StreamRule>,
    shared_seen: Option<&mut BTreeSet<Key>>,
    above: &[File],
) -> (BTreeMap<Key, SeqEntry>, Vec<Rt>, Vec<Key>) {
    let srcs: Vec<Src<'_>> = files
        .iter()
        .map(|f| Src {
            entries: &f.entries,
            rts: &f.rts,
        })
        .collect();
    let mut keys: Vec<&Key> = Vec::new();
    for src in &srcs {
        keys.extend(src.entries.keys());
    }
    keys.sort_unstable();
    keys.dedup();
    let mut entries = BTreeMap::new();
    let mut dropped = Vec::new();
    let mut seen_local = BTreeSet::new();
    let seen: &mut BTreeSet<Key> = match shared_seen {
        Some(s) => s,
        None => &mut seen_local,
    };
    for k in keys {
        match newest(k, &srcs) {
            Some(View::Put(seq, v)) => {
                if rule.is_some_and(|r| stream_drop(r, seen, k, v)) {
                    dropped.push(k.clone());
                } else {
                    entries.insert(k.clone(), (seq, LEntry::Put(v.clone())));
                }
            }
            Some(View::Tomb(seq)) => {
                entries.insert(k.clone(), (seq, LEntry::Tomb));
            }
            Some(View::Hidden) | None => {}
        }
    }
    let rts: Vec<Rt> = files
        .iter()
        .flat_map(|f| f.rts.iter().cloned())
        .filter(|(_, s, e)| above.iter().any(|f| f.lo <= *e && f.hi >= *s))
        .collect();
    (entries, rts, dropped)
}

// ---- The model. ----

const S0: u8 = 0;
const S1: u8 = 1;
/// The unrelated table `Action::Later` writes to.
const OTHER: u8 = 9;
const CAT: &[u8] = b"c";
/// The AS OF reader's caller-chosen ts.
const AS_T: Ts = 15;
/// The retention window: 100 acts as "no floor" (visible_ts never reaches it).
const WINDOW_HI: Ts = 100;
/// The widened post-crash window (seed 22): floor below the durable W.
const WINDOW_LO: Ts = 2;
/// The initial durable watermark (preloaded, synced).
const W_INIT: Ts = 10;

fn rel_key(sid: u8, k: u8) -> Vec<u8> {
    vec![b't', sid, k]
}

fn wid(epoch: u8, w: u8) -> TxnId {
    TxnId { epoch, n: w }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum St {
    Pending,
    Committed(Ts),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TxnState {
    st: St,
    released: bool,
    count: i8,
    last_removal: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WPhase {
    /// Not yet committed (placement and commit are one step).
    Active,
    Committed {
        c: Ts,
    },
    Lost,
}

/// One ghost commit: what really committed, where, and when.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum GCommit {
    Rel { ts: Ts, sid: u8, k: u8, data: Data },
    Cat { ts: Ts, sid: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Ghost {
    commits: Vec<GCommit>,
    /// Max W published this boot (resets at a crash to the durable W).
    w_max: Ts,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Reader {
    pc: u8,
    s: Option<Ts>,
    registered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AsOf {
    pc: u8,
    registered: bool,
    storage: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FkOpen {
    vts: Ts,
    counter: u32,
    copy: Lsm,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    epoch: u8,
    lsm: Lsm,
    status: BTreeMap<TxnId, TxnState>,
    visible_ts: Ts,
    next_ts: Ts,
    w_pub: Ts,
    gc_w: Ts,
    gc_w_pending: bool,
    window: Ts,
    writers: [WPhase; 2],
    /// The storage id W0's intent landed under (recorded at placement).
    w0_sid: Option<u8>,
    resolve_q: Vec<TxnId>,
    reader: Reader,
    asof: AsOf,
    fk: Option<FkOpen>,
    view_counter: u32,
    crashed: bool,
    retired: bool,
    /// The DeleteRange job and the full compaction ran at `(W, write_gen)`;
    /// the generation moves on every write-producing step, so a version that
    /// appears after either step invalidates the quiesce preconditions.
    tombdr: Option<(Ts, u64)>,
    fullc: Option<(Ts, u64)>,
    /// Ghost: moves on every step that writes an intent or version.
    write_gen: u64,
    /// Seed 33's shared filter state (never written by the clean model).
    /// Seed 33's shared filter state (never written by the clean model).
    filter_seen: BTreeSet<Key>,
    /// The deferred-FK view opens at most once (re-opening is not needed by
    /// any seed; the crash kills it).
    fk_used: bool,
    /// The unrelated later commit (see `Action::Later`) has run.
    later: bool,
    ghost: Ghost,
    bad: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Action {
    /// Place the txn's single write and commit it (one step: no owned seed
    /// needs the pending-intent window; committed-but-unresolved is what the
    /// retire and the resolver interleave with).
    Commit(u8),
    Resolve(TxnId),
    GcPublish,
    GcSync,
    /// The GC job's whole-keyspace pass: the newest-`<= W` tombstone
    /// DeleteRanges, then a full compaction through the filter.
    GcPass,
    Retire,
    Flush,
    Compact(usize),
    Reader,
    AsOf,
    FkOpen,
    FkClose,
    Truncate(TxnId),
    /// One commit by an unrelated txn on another table (storage id 9),
    /// placed, committed and resolved in one step. It is what lets `W` pass
    /// the DDL's commit ts when W0 committed before the DDL (seed 35: a
    /// committed-unresolved intent under the retired prefix at retire time).
    Later,
    Crash,
}

pub struct GcModel {
    pub bug: Option<Bug>,
}

impl GcModel {
    /// The reader's op list: `TakeS` (read + register, one step, §3.1) then
    /// `Finish`; seed 21 splits the take into two steps.
    fn reader_len(&self) -> u8 {
        if self.bug == Some(Bug::SnapshotRegistrationSplit) {
            3
        } else {
            2
        }
    }

    /// The GC job's computed watermark (§9.1), from the registry.
    fn computed(&self, s: &State) -> Ts {
        let mut m = s.visible_ts.min(s.window);
        if s.reader.registered {
            if let Some(x) = s.reader.s {
                m = m.min(x);
            }
        }
        if s.asof.registered {
            m = m.min(AS_T);
        }
        if let Some(fk) = &s.fk {
            if self.bug != Some(Bug::LatestStateViewDoesNotHoldW) {
                m = m.min(fk.vts);
            }
        }
        m
    }

    /// The catalog as the protocol reads it at `s`: the read path (§4) over
    /// the catalog key (a committed-visible intent counts, §4 step 1).
    fn cat_at(&self, s: &State, t: Ts) -> u8 {
        let rows = s.lsm.merged_scan(&intent_key(CAT), &end_key(CAT));
        match rows_read(&rows, &s.status, t) {
            Some(sid) => sid,
            None => S0,
        }
    }

    /// The latest committed catalog (an `AS OF` bug resolves this instead).
    fn latest_cat(&self, s: &State) -> u8 {
        self.cat_at(s, s.visible_ts)
    }

    /// Every logical key whose merged newest version `<= w` is a tombstone:
    /// the C-B1 DeleteRange job's targets (§9.2).
    fn tombstone_targets(&self, s: &State, w: Ts) -> Vec<(Vec<u8>, u64)> {
        let mut out = Vec::new();
        let mut logicals: BTreeSet<Vec<u8>> = BTreeSet::new();
        for k in s.lsm.raw_keys() {
            if let Some((l, EntryKind::Version(_))) = parse_key(&k) {
                logicals.insert(l);
            }
        }
        for l in logicals {
            let rows = s.lsm.merged_scan(&intent_key(&l), &end_key(&l));
            for (k, v) in rows {
                if let Some((_, EntryKind::Version(ts))) = parse_key(&k) {
                    if ts <= w as u64 && value_is_tombstone(&v) {
                        out.push((l, ts));
                    }
                    break;
                }
            }
        }
        out
    }

    /// The W the GC filter runs at: the durable `/sys/gc_w` (seed 42's bug
    /// hands it the published, unsynced one instead).
    fn filter_w(&self, s: &State) -> Ts {
        if self.bug == Some(Bug::FilterHandedUnsyncedW) {
            s.w_pub
        } else {
            s.gc_w
        }
    }

    /// The compaction step's filter rule.
    fn filter_rule(&self, s: &State) -> StreamRule {
        let w = self.filter_w(s) as u64;
        if self.bug == Some(Bug::FilterDropsNewestTombstones) {
            StreamRule::DropsTombsLeW(w)
        } else {
            StreamRule::Spec(GcSpec { w })
        }
    }

    fn min_counters(&self, s: &State) -> u32 {
        s.fk.as_ref().map(|f| f.counter).unwrap_or(u32::MAX)
    }
}

/// §7.3's bookkeeping (step 4) for a removal of `t`'s intent.
fn bookkeep(s: &mut State, t: TxnId) {
    if let Some(ts) = s.status.get_mut(&t) {
        ts.last_removal = s.view_counter;
        if t.epoch == s.epoch {
            ts.count -= 1;
        }
    }
}

/// The §4 read over one merged row list (rows ascending: the intent first,
/// then versions newest first). Statuses are looked up live; a missing
/// status of an older-epoch owner means Aborted (fall through to versions).
fn rows_read(rows: &[(Key, Value)], status: &BTreeMap<TxnId, TxnState>, s: Ts) -> Option<u8> {
    for (k, v) in rows {
        match parse_key(k) {
            Some((_, EntryKind::Intent)) => {
                if let Some((owner, data)) = parse_intent(v) {
                    match status.get(&owner).map(|t| t.st) {
                        Some(St::Committed(c)) if c <= s => return data_read(data),
                        // Pending, aborted, not yet visible, or ended and
                        // released: fall through to versions.
                        _ => {}
                    }
                }
            }
            Some((_, EntryKind::Version(ts))) if ts <= s as u64 => {
                return value_live_payload(v);
            }
            _ => {}
        }
    }
    None
}

fn ghost_cat_at(g: &Ghost, t: Ts) -> u8 {
    let mut sid = S0;
    for c in &g.commits {
        if let GCommit::Cat { ts, sid: s } = c {
            if *ts <= t {
                sid = *s;
            }
        }
    }
    sid
}

fn ghost_read(g: &Ghost, sid: u8, k: u8, s: Ts) -> Option<u8> {
    let mut found: Option<Data> = None;
    for c in &g.commits {
        if let GCommit::Rel {
            ts,
            sid: cs,
            k: ck,
            data,
        } = c
        {
            if *cs == sid && *ck == k && *ts <= s {
                found = Some(*data);
            }
        }
    }
    found.and_then(data_read)
}

impl State {
    /// Canonical form for deduplication: values the protocol only compares
    /// (view counters and the counters derived from them; LSM write seqs and
    /// eras, only through `<`/`==` inside one state copy; file ids) are
    /// renumbered by rank. Every guard and check sees the same answers.
    fn normalize(&mut self) {
        // The open view's copy is never flushed or compacted, and its reads
        // merge over all of its sources, so its canonical form is the merged
        // rows themselves (with its opening era): two copies that merged
        // identically behave identically.
        if let Some(f) = &mut self.fk {
            let rows = f.copy.merged_scan(&Vec::new(), &vec![0xff; 32]);
            let era = f.copy.era;
            let mut copy = Lsm::new();
            for (k, v) in rows {
                copy.apply(LsmOp::Put(k, v));
            }
            copy.era = era;
            f.copy = copy;
        }

        let mut gens: Vec<u64> = vec![0, self.write_gen];
        if let Some((_, g)) = self.tombdr {
            gens.push(g);
        }
        if let Some((_, g)) = self.fullc {
            gens.push(g);
        }
        gens.sort_unstable();
        gens.dedup();
        let grank = |v: u64| match gens.binary_search(&v) {
            Ok(i) => i as u64 + 1,
            Err(_) => 0,
        };
        if let Some((_, g)) = &mut self.tombdr {
            *g = grank(*g);
        }
        if let Some((_, g)) = &mut self.fullc {
            *g = grank(*g);
        }
        self.write_gen = grank(self.write_gen);

        let mut vals: Vec<u32> = vec![0, self.view_counter];
        if let Some(f) = &self.fk {
            vals.push(f.counter);
        }
        vals.extend(self.status.values().map(|t| t.last_removal));
        vals.sort_unstable();
        vals.dedup();
        let rank = |v: u32| vals.binary_search(&v).unwrap_or(0) as u32;
        self.view_counter = rank(self.view_counter);
        if let Some(f) = &mut self.fk {
            f.counter = rank(f.counter);
        }
        for t in self.status.values_mut() {
            t.last_removal = rank(t.last_removal);
        }

        // LSM seqs and eras: rank over both the live state and the FK copy.
        let mut seqs: Vec<u64> = Vec::new();
        let mut eras: Vec<u64> = Vec::new();
        let collect = |l: &Lsm, seqs: &mut Vec<u64>, eras: &mut Vec<u64>| {
            seqs.push(l.seq);
            for (seq, _) in l.memtable.values() {
                seqs.push(*seq);
            }
            for (seq, _, _) in &l.mem_rts {
                seqs.push(*seq);
            }
            for f in l.levels.iter().flatten() {
                for (seq, _) in f.entries.values() {
                    seqs.push(*seq);
                }
                for (seq, _, _) in &f.rts {
                    seqs.push(*seq);
                }
            }
            eras.push(l.era);
            eras.extend(l.dropped.values().copied());
        };
        collect(&self.lsm, &mut seqs, &mut eras);
        if let Some(f) = &self.fk {
            collect(&f.copy, &mut seqs, &mut eras);
        }
        seqs.sort_unstable();
        seqs.dedup();
        eras.sort_unstable();
        eras.dedup();
        let seq_rank = |v: u64| match seqs.binary_search(&v) {
            Ok(i) => i as u64 + 1,
            Err(_) => 0,
        };
        let era_rank = |v: u64| match eras.binary_search(&v) {
            Ok(i) => i as u64 + 1,
            Err(_) => 0,
        };
        // The drop registry only ever matters against the open view's copy
        // (every later view opens at the current era, which no recorded drop
        // can exceed): with no view open it is inert; with one, entries that
        // cannot hide anything from that copy are inert. Canonicalising both
        // merges behaviourally identical states.
        match &self.fk {
            None => {
                self.lsm.dropped.clear();
                self.lsm.era = 0;
            }
            Some(f) => {
                let open_era = f.copy.era;
                let copy_keys: BTreeSet<Key> = f
                    .copy
                    .memtable
                    .keys()
                    .chain(
                        f.copy
                            .levels
                            .iter()
                            .flatten()
                            .flat_map(|x| x.entries.keys()),
                    )
                    .cloned()
                    .collect();
                self.lsm
                    .dropped
                    .retain(|k, e| *e > open_era && copy_keys.contains(k));
            }
        }
        if let Some(f) = &mut self.fk {
            // The copy's own registry snapshot is never consulted (reads use
            // the live one); only its era is.
            f.copy.dropped.clear();
        }

        let renumber = |l: &mut Lsm| {
            l.seq = seq_rank(l.seq);
            for (seq, _) in l.memtable.values_mut() {
                *seq = seq_rank(*seq);
            }
            for (seq, _, _) in &mut l.mem_rts {
                *seq = seq_rank(*seq);
            }
            // Range tombstones are consulted by seq comparison only; their
            // order in the vectors is behaviourally irrelevant.
            l.mem_rts.sort();
            for f in l.levels.iter_mut().flatten() {
                for (seq, _) in f.entries.values_mut() {
                    *seq = seq_rank(*seq);
                }
                for (seq, _, _) in f.rts.iter_mut() {
                    *seq = seq_rank(*seq);
                }
                f.rts.sort();
            }
            l.era = era_rank(l.era);
            for e in l.dropped.values_mut() {
                *e = era_rank(*e);
            }
        };
        renumber(&mut self.lsm);
        if let Some(f) = &mut self.fk {
            renumber(&mut f.copy);
        }

        // File ids: reassign in level order (only compaction addressing uses
        // them, by position).
        let mut next = 1u64;
        for level in self.lsm.levels.iter_mut() {
            for f in level.iter_mut() {
                f.id = next;
                next += 1;
            }
        }
        self.lsm.next_file = next;
    }

    fn phase(&self, w: u8) -> WPhase {
        self.writers[w as usize]
    }

    fn wid_of(&self, w: u8) -> TxnId {
        wid(0, w)
    }

    fn write_intent(&mut self, l: Vec<u8>, owner: TxnId, data: Data) {
        self.lsm
            .apply(LsmOp::Put(intent_key(&l), intent_value(owner, data)));
    }

    /// The logical key a txn's single write targets.
    fn target_of(&self, t: TxnId) -> Vec<u8> {
        if t.n == 0 {
            rel_key(self.w0_sid.unwrap_or(S0), 1)
        } else {
            CAT.to_vec()
        }
    }

    fn data_of(&self, t: TxnId) -> Data {
        if t.n == 0 {
            Data::Tomb
        } else {
            Data::Live(S1)
        }
    }
}

impl Model for GcModel {
    type State = State;
    type Action = Action;

    fn init(&self) -> State {
        let k0 = rel_key(S0, 0);
        let k1 = rel_key(S0, 1);
        // L1 holds k0@10; L0 holds two files whose ranges are disjoint (so a
        // per-file compaction is a stream on its own): {k0@20, k1@20} and
        // {k1@10}.
        let lsm = Lsm::new()
            .with_file(
                1,
                vec![(version_key(&k0, 10), version_value(Data::Live(b'a')))],
            )
            .with_file(
                0,
                vec![
                    (version_key(&k0, 20), version_value(Data::Tomb)),
                    (version_key(&k1, 20), version_value(Data::Live(b'c'))),
                ],
            )
            .with_file(
                0,
                vec![(version_key(&k1, 10), version_value(Data::Live(b'b')))],
            );
        let mut status = BTreeMap::new();
        for w in 0..2u8 {
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
        let ghost = Ghost {
            commits: vec![
                GCommit::Rel {
                    ts: 10,
                    sid: S0,
                    k: 0,
                    data: Data::Live(b'a'),
                },
                GCommit::Rel {
                    ts: 20,
                    sid: S0,
                    k: 0,
                    data: Data::Tomb,
                },
                GCommit::Rel {
                    ts: 10,
                    sid: S0,
                    k: 1,
                    data: Data::Live(b'b'),
                },
                GCommit::Rel {
                    ts: 20,
                    sid: S0,
                    k: 1,
                    data: Data::Live(b'c'),
                },
            ],
            w_max: W_INIT,
        };
        State {
            epoch: 0,
            lsm,
            status,
            visible_ts: 20,
            next_ts: 30,
            w_pub: W_INIT,
            gc_w: W_INIT,
            gc_w_pending: false,
            window: WINDOW_HI,
            writers: [WPhase::Active; 2],
            w0_sid: None,
            resolve_q: Vec::new(),
            reader: Reader {
                pc: 0,
                s: None,
                registered: false,
            },
            asof: AsOf {
                pc: 0,
                registered: false,
                storage: None,
            },
            fk: None,
            view_counter: 0,
            crashed: false,
            retired: false,
            tombdr: None,
            fullc: None,
            write_gen: 0,
            filter_seen: BTreeSet::new(),
            fk_used: false,
            later: false,
            ghost,
            bad: None,
        }
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        if s.bad.is_some() {
            return;
        }
        for w in 0..2u8 {
            if matches!(s.phase(w), WPhase::Active) {
                out.push(Action::Commit(w));
            }
        }
        for t in &s.resolve_q {
            out.push(Action::Resolve(*t));
        }
        let computed = self.computed(s);
        if computed > s.w_pub
            || (self.bug == Some(Bug::WatermarkAllowedToDecrease) && computed < s.w_pub)
        {
            out.push(Action::GcPublish);
        }
        if s.gc_w_pending {
            out.push(Action::GcSync);
        }
        if !s.crashed
            && (!self.tombstone_targets(s, s.gc_w).is_empty()
                || lsm_memtable_nonempty(&s.lsm)
                || s.lsm.level0_len() > 0)
        {
            out.push(Action::GcPass);
        }
        if !s.retired && !s.crashed {
            if let WPhase::Committed { c: ddl } = s.phase(1) {
                if s.gc_w > ddl {
                    out.push(Action::Retire);
                }
            }
        }
        if !s.crashed {
            if s.visible_ts == 20 && lsm_memtable_nonempty(&s.lsm) && s.lsm.level0_len() < 3 {
                out.push(Action::Flush);
            }
            for i in 0..s.lsm.level0_len().min(2) {
                out.push(Action::Compact(i));
            }
        }
        let reader_take_now = s.reader.pc == 0;
        if (s.reader.pc > 0 && s.reader.pc < self.reader_len()) || reader_take_now {
            out.push(Action::Reader);
        }
        if s.asof.pc < 2 {
            out.push(Action::AsOf);
        }
        if s.fk.is_none() {
            if !s.fk_used && s.visible_ts == 20 {
                out.push(Action::FkOpen);
            }
        } else {
            out.push(Action::FkClose);
        }
        let w0 = s.wid_of(0);
        if let Some(ts) = s.status.get(&w0) {
            if w0.epoch == s.epoch
                && ts.released
                && ts.count == 0
                && self.min_counters(s) > ts.last_removal
            {
                out.push(Action::Truncate(w0));
            }
        }
        if !s.later {
            out.push(Action::Later);
        }
        if !s.crashed && s.gc_w_pending {
            out.push(Action::Crash);
        }
    }

    fn next(&self, s: &State, a: &Action) -> State {
        let mut s = s.clone();
        match a {
            Action::Commit(w) => {
                let me = s.wid_of(*w);
                let c = s.next_ts;
                s.next_ts += 1;
                if let WPhase::Active = s.phase(*w) {
                    // Placement and commit in one step: count before the
                    // write (§5.1), then the status/visibility update.
                    s.write_gen += 1;
                    if let Some(ts) = s.status.get_mut(&me) {
                        ts.count += 1;
                    }
                    if *w == 0 {
                        let sid = self.latest_cat(&s);
                        s.w0_sid = Some(sid);
                        s.write_intent(rel_key(sid, 1), me, Data::Tomb);
                        s.resolve_q.push(me);
                    } else {
                        // The DDL's catalog version is written with its
                        // commit (resolution fused; no unresolved window).
                        s.write_intent(CAT.to_vec(), me, Data::Live(S1));
                        let ik = intent_key(CAT);
                        s.lsm.apply(LsmOp::Delete(ik));
                        s.lsm.apply(LsmOp::Put(
                            version_key(CAT, c as u64),
                            version_value(Data::Live(S1)),
                        ));
                    }
                    if let Some(ts) = s.status.get_mut(&me) {
                        ts.st = St::Committed(c);
                        ts.released = true;
                    }
                    s.visible_ts = c;
                    if *w == 0 {
                        let sid = s.w0_sid.unwrap_or(S0);
                        s.ghost.commits.push(GCommit::Rel {
                            ts: c,
                            sid,
                            k: 1,
                            data: Data::Tomb,
                        });
                    } else {
                        s.ghost.commits.push(GCommit::Cat { ts: c, sid: S1 });
                    }
                    s.writers[*w as usize] = WPhase::Committed { c };
                }
            }
            Action::Resolve(t) => {
                s.write_gen += 1;
                if let Some(pos) = s.resolve_q.iter().position(|x| *x == *t) {
                    s.resolve_q.remove(pos);
                }
                let l = s.target_of(*t);
                let ik = intent_key(&l);
                let owned = s
                    .lsm
                    .merged_get(&ik)
                    .and_then(|v| parse_intent(&v))
                    .is_some_and(|(owner, _)| owner == *t);
                if owned {
                    s.lsm.apply(LsmOp::Delete(ik));
                    if let Some(TxnState {
                        st: St::Committed(c),
                        ..
                    }) = s.status.get(t)
                    {
                        let c = *c;
                        s.lsm.apply(LsmOp::Put(
                            version_key(&l, c as u64),
                            version_value(s.data_of(*t)),
                        ));
                    }
                    bookkeep(&mut s, *t);
                }
            }
            Action::GcPublish => {
                let computed = self.computed(&s);
                s.w_pub = if self.bug == Some(Bug::WatermarkAllowedToDecrease) {
                    computed
                } else {
                    s.w_pub.max(computed)
                };
                s.ghost.w_max = s.ghost.w_max.max(s.w_pub);
                s.gc_w_pending = true;
            }
            Action::GcSync => {
                if s.w_pub < s.gc_w {
                    s.bad = Some(format!(
                        "GC sync: /sys/gc_w decrease ({} -> {}) refused by the kv; the caller treats it as fatal (C-T0 9.1)",
                        s.gc_w, s.w_pub
                    ));
                } else {
                    s.gc_w = s.w_pub;
                    s.gc_w_pending = false;
                }
            }
            Action::GcPass => {
                for (l, t) in self.tombstone_targets(&s, s.gc_w) {
                    let start = if self.bug == Some(Bug::DeleteRangeExclusiveStart) {
                        match t.checked_sub(1) {
                            Some(t1) => version_key(&l, t1),
                            None => end_key(&l),
                        }
                    } else {
                        version_key(&l, t)
                    };
                    s.lsm.apply(LsmOp::DeleteRange {
                        start,
                        end: end_key(&l),
                    });
                }
                s.tombdr = Some((s.gc_w, s.write_gen));
                // The pass's compaction: flush and compact all of L0 through
                // the filter (one stream). The L1 re-streams of the kv's
                // `compact_all` are not modelled: the DeleteRange job above
                // already fixes the property quiesce checks (no tombstone
                // newest <= W, whatever file it sits in), and no seed needs
                // a bottom-level re-stream.
                let rule = self.filter_rule(&s);
                let w = self.filter_w(&s);
                if self.bug == Some(Bug::FilterStateSharedAcrossStreams) {
                    let mut seen = std::mem::take(&mut s.filter_seen);
                    let mut seen_ref = &mut seen;
                    let _ = &mut seen_ref;
                    s.lsm.compact_all_l0(Some(&rule), Some(&mut seen));
                    s.filter_seen = seen;
                } else {
                    s.lsm.compact_all_l0(Some(&rule), None);
                }
                s.fullc = Some((w, s.write_gen));
            }
            Action::Retire => {
                s.write_gen += 1;
                let lo = intent_key(&rel_key(S0, 0));
                let hi = end_key(&rel_key(S0, 1));
                // The removal plan (scan first, in both variants; only
                // merged-observable intents -- the §7.3 re-read basis).
                let mut plan = Vec::new();
                for k in s.lsm.raw_keys() {
                    if let Some((l, EntryKind::Intent)) = parse_key(&k) {
                        if k >= lo && k < hi {
                            if let Some((owner, data)) =
                                s.lsm.merged_get(&k).and_then(|v| parse_intent(&v))
                            {
                                plan.push((owner, l, data));
                            }
                        }
                    }
                }
                if self.bug == Some(Bug::RetireRangeBeforeIntentRemoval) {
                    s.lsm.apply(LsmOp::DeleteRange { start: lo, end: hi });
                }
                for (owner, l, data) in plan {
                    let ik = intent_key(&l);
                    let visible = s
                        .lsm
                        .merged_get(&ik)
                        .and_then(|v| parse_intent(&v))
                        .is_some_and(|(o, _)| o == owner);
                    if visible {
                        s.lsm.apply(LsmOp::Delete(ik));
                        if let Some(TxnState {
                            st: St::Committed(c),
                            ..
                        }) = s.status.get(&owner)
                        {
                            let c = *c;
                            s.lsm
                                .apply(LsmOp::Put(version_key(&l, c as u64), version_value(data)));
                        }
                    }
                    // The retire's own bookkeeping for its planned removal
                    // (seed 35: it fires even though the DeleteRange-first
                    // made the §7.3 re-reads no-op).
                    bookkeep(&mut s, owner);
                }
                if self.bug != Some(Bug::RetireRangeBeforeIntentRemoval) {
                    s.lsm.apply(LsmOp::DeleteRange {
                        start: intent_key(&rel_key(S0, 0)),
                        end: end_key(&rel_key(S0, 1)),
                    });
                }
                s.retired = true;
            }
            Action::Flush => {
                s.lsm.flush();
            }
            Action::Compact(i) => {
                let rule = self.filter_rule(&s);
                if self.bug == Some(Bug::FilterStateSharedAcrossStreams) {
                    let mut seen = std::mem::take(&mut s.filter_seen);
                    s.lsm.compact_rule(&[*i], Some(&rule), Some(&mut seen));
                    s.filter_seen = seen;
                } else {
                    s.lsm.compact_rule(&[*i], Some(&rule), None);
                }
            }
            Action::Reader => {
                let split = self.bug == Some(Bug::SnapshotRegistrationSplit);
                match s.reader.pc {
                    0 => {
                        s.reader.s = Some(s.visible_ts);
                        if !split {
                            s.reader.registered = true;
                        }
                    }
                    1 if split => {
                        s.reader.registered = true;
                    }
                    _ => {
                        s.reader.s = None;
                        s.reader.registered = false;
                    }
                }
                s.reader.pc += 1;
            }
            Action::AsOf => match s.asof.pc {
                0 => {
                    let floor = if self.bug == Some(Bug::AsOfCheckedAgainstUnpublishedW) {
                        s.gc_w
                    } else {
                        s.w_pub
                    };
                    if AS_T <= s.visible_ts && AS_T >= floor {
                        s.asof.registered = true;
                        s.asof.storage =
                            Some(if self.bug == Some(Bug::AsOfResolvesLatestCatalog) {
                                self.latest_cat(&s)
                            } else {
                                self.cat_at(&s, AS_T)
                            });
                    }
                    s.asof.pc = 1;
                }
                _ => {
                    s.asof.registered = false;
                    s.asof.pc = 2;
                }
            },
            Action::FkOpen => {
                if s.fk.is_none() && !s.fk_used {
                    s.fk_used = true;
                    s.view_counter += 1;
                    s.fk = Some(FkOpen {
                        vts: s.visible_ts,
                        counter: s.view_counter,
                        copy: s.lsm.clone(),
                    });
                }
            }
            Action::FkClose => {
                s.fk = None;
            }
            Action::Truncate(t) => {
                s.status.remove(t);
            }
            Action::Later => {
                let c = s.next_ts;
                s.next_ts += 1;
                s.write_gen += 1;
                s.lsm.apply(LsmOp::Put(
                    version_key(&rel_key(OTHER, 0), c as u64),
                    version_value(Data::Live(b'z')),
                ));
                s.visible_ts = c;
                s.ghost.commits.push(GCommit::Rel {
                    ts: c,
                    sid: OTHER,
                    k: 0,
                    data: Data::Live(b'z'),
                });
                s.later = true;
            }
            Action::Crash => {
                s.crashed = true;
                s.epoch = 1;
                s.gc_w_pending = false;
                // Boot loads the durable W; the pending write is lost.
                s.w_pub = s.gc_w;
                s.ghost.w_max = s.gc_w;
                s.window = WINDOW_LO;
                for w in 0..2u8 {
                    if !matches!(s.phase(w), WPhase::Committed { .. }) {
                        s.writers[w as usize] = WPhase::Lost;
                    }
                }
                s.resolve_q.clear();
                // The snapshot reader and the FK view die with their session;
                // the AS OF session re-runs after boot.
                s.reader.pc = self.reader_len();
                s.reader.s = None;
                s.reader.registered = false;
                s.fk = None;
                s.asof = AsOf {
                    pc: 0,
                    registered: false,
                    storage: None,
                };
                s.tombdr = None;
                s.fullc = None;
            }
        }
        s.normalize();
        s
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if let Some(b) = &s.bad {
            return Err(b.clone());
        }
        if s.w_pub > s.visible_ts {
            return Err(format!(
                "W ({}) above visible_ts ({})",
                s.w_pub, s.visible_ts
            ));
        }
        if s.w_pub != s.ghost.w_max {
            return Err(format!(
                "I-GC (monotonic W): published W {} below this boot's max {}",
                s.w_pub, s.ghost.w_max
            ));
        }
        // I-GC's registered-ts clause: every registered snapshot, AS OF ts and
        // open view's vts is >= W.
        let mut registered: Vec<(Ts, &str)> = Vec::new();
        if s.reader.registered {
            if let Some(x) = s.reader.s {
                registered.push((x, "snapshot"));
            }
        }
        if s.asof.registered {
            registered.push((AS_T, "as-of"));
        }
        if let Some(f) = &s.fk {
            registered.push((f.vts, "view vts"));
        }
        for (v, what) in registered {
            if v < s.w_pub {
                return Err(format!(
                    "I-GC: registered {what} ts {v} below the published W {}",
                    s.w_pub
                ));
            }
        }
        // I-GC / I-ATOMIC: ghost reads at every registered ts and at
        // visible_ts. R and visible_ts resolve the catalog at their ts; the
        // AS OF reader resolves what it recorded at registration.
        let mut checks: Vec<(Ts, u8)> = Vec::new();
        if s.reader.registered {
            if let Some(x) = s.reader.s {
                checks.push((x, self.cat_at(s, x)));
            }
        }
        if s.asof.registered {
            checks.push((AS_T, s.asof.storage.unwrap_or(S0)));
        }
        checks.push((s.visible_ts, self.cat_at(s, s.visible_ts)));
        for (ts, sid) in checks {
            let want_sid = ghost_cat_at(&s.ghost, ts);
            for k in 0..2u8 {
                let expected = ghost_read(&s.ghost, want_sid, k, ts);
                let l = rel_key(sid, k);
                let rows = s.lsm.merged_scan(&intent_key(&l), &end_key(&l));
                let actual = rows_read(&rows, &s.status, ts);
                if actual != expected {
                    return Err(format!(
                        "I-GC/I-ATOMIC: k{k} under storage {sid} at ts {ts}: read {actual:?}, committed history says {expected:?}"
                    ));
                }
            }
        }
        // I-GC's open-view clause: the FK view keeps returning, for every
        // key, what its own copy holds (the live drop registry is the only
        // thing that may differ, and a correctly registered vts keeps every
        // version the view could return).
        if let Some(f) = &s.fk {
            for sid in 0..2u8 {
                for k in 0..2u8 {
                    let l = rel_key(sid, k);
                    let (lo, hi) = (intent_key(&l), end_key(&l));
                    let expected_rows = f.copy.merged_scan(&lo, &hi);
                    let expected = rows_read(&expected_rows, &s.status, f.vts);
                    let actual_rows = Lsm::snap_scan(&f.copy, &s.lsm, &lo, &hi);
                    let actual = rows_read(&actual_rows, &s.status, f.vts);
                    if expected != actual {
                        return Err(format!(
                            "I-GC (open view): k{k} under storage {sid} at vts {}: read {actual:?}, the view's own copy holds {expected:?}",
                            f.vts
                        ));
                    }
                }
            }
        }
        // I-TRUNC, over raw storage (the worst-case reader): no raw intent
        // entry of a current-epoch txn may outlive its status entry.
        for (k, owner) in s.lsm.raw_intent_entries() {
            if owner.epoch == s.epoch && !s.status.contains_key(&owner) {
                return Err(format!(
                    "I-TRUNC: raw intent {k:?} of {owner:?} survives without a status entry"
                ));
            }
        }
        // I-GC-QUIESCE: with the resolver drained, no txn active, W fixed,
        // the DeleteRange job and a full compaction both run at this W, no
        // key's newest version <= W is a tombstone.
        let ended = s
            .writers
            .iter()
            .all(|p| matches!(p, WPhase::Committed { .. } | WPhase::Lost));
        let no_intents = s.lsm.raw_intent_entries().is_empty();
        if ended
            && s.resolve_q.is_empty()
            && no_intents
            && !s.gc_w_pending
            && s.tombdr == Some((s.gc_w, s.write_gen))
            && s.fullc == Some((s.gc_w, s.write_gen))
        {
            if let Some((l, t)) = self.tombstone_targets(s, s.gc_w).into_iter().next() {
                return Err(format!(
                    "I-GC-QUIESCE: {l:?} still has tombstone @{} as its newest version <= W {}",
                    t, s.gc_w
                ));
            }
        }
        Ok(())
    }

    fn is_final(&self, s: &State) -> bool {
        s.bad.is_none()
    }
}

/// The memtable holds writes waiting for a flush.
fn lsm_memtable_nonempty(l: &Lsm) -> bool {
    !l.memtable.is_empty() || !l.mem_rts.is_empty()
}
