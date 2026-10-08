//! MemKv: `OrderedKv` over a persistent ordered map (V2 plan D1). For WASM,
//! tests and the G0 model.
//!
//! A write builds the next map version from the published one and publishes it
//! in one step, so batches are atomic. A snapshot is a clone of the published
//! state, cheap by structural sharing. There is no WAL: `Durability` and
//! `sync_wal` are no-ops and a process exit loses everything not in a
//! checkpoint. Crash semantics are modelled by `fault::Fault`.
//!
//! # LSM mode (C-K3b)
//!
//! `MemKv::lsm()` returns the same store driven like an LSM, so the G0 model
//! can reproduce C-T0 §9.2's tombstone resurrection. Writes (including
//! `DeleteRange`, stored as a range tombstone) go to the memtable; `flush`
//! turns it into a new L0 file; `compact(level, &[file_id])` merges files into
//! `level+1`, running one fresh `GcStream` per output file; `files()` lists
//! `(level, id, key_range)`. Two levels: L0 and the bottommost L1.
//!
//! Visibility is exact regardless of file placement: every point entry and
//! range tombstone carries the unique write seq that produced it, and reads
//! (and compaction merges) resolve a key by the highest seq touching it — the
//! memtable and the files merged newest to oldest. A range tombstone hides
//! older data in every lower file at read time. A snapshot holds its file set
//! (files are shared and immutable), but a compaction-filter drop is still
//! **visible to already-open snapshots**: the live drop registry (`era` +
//! `dropped`, G0-R5-1 — the adversarial choice the kv contract allows, which
//! G0-gc needs) hides a key from every snapshot opened before its latest
//! drop, even though the snapshot's own copy still holds it. The flat
//! single-map mode stays the default and unchanged (no registry; its
//! snapshots keep dropped keys by structural sharing).

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use imbl::OrdMap;

use crate::{Batch, Durability, GcFilter, Key, KvError, Op, OrderedKv, Result, Snapshot, Value};

type Map = OrdMap<Key, Value>;

/// Checkpoint file format version written by `checkpoint` (2 = LSM layout;
/// version 1 flat files remain readable).
pub const CHECKPOINT_VERSION: u32 = 2;
const CHECKPOINT_FILE: &str = "MEMKV";
const CHECKPOINT_TMP: &str = "MEMKV.tmp";
const MAGIC: &[u8; 8] = b"NKVMEMCK";
/// Number of levels in LSM mode: L0 (flushed memtables) and bottommost L1.
const LSM_LEVELS: usize = 2;

/// One live LSM file: `(level, file_id, (lo, hi))`, the closed key range its
/// entries and range tombstones touch.
pub type FileMeta = (usize, u64, (Key, Key));

pub struct MemKv {
    /// Published state. Readers clone it; only `publish` replaces it. Shared
    /// with snapshots so they can consult the live GC-drop registry.
    st: Arc<RwLock<KvState>>,
    /// Serialises writers (and compaction) so each builds on the latest state.
    writer: Mutex<()>,
    gc: RwLock<Option<Box<dyn GcFilter>>>,
    /// GC watermark (C-T0 §9.1). Monotonic engine metadata, not data: not
    /// part of a checkpoint (the engine persists it as `/sys/gc_w`).
    watermark: RwLock<u64>,
}

enum KvState {
    /// One flat map (the default mode).
    Flat(Map),
    /// LSM mode.
    Lsm(Box<LsmState>),
}

impl Clone for KvState {
    fn clone(&self) -> Self {
        match self {
            KvState::Flat(m) => KvState::Flat(m.clone()),
            KvState::Lsm(l) => KvState::Lsm(l.clone()),
        }
    }
}

/// One point entry: a value, or a delete marker hiding older entries.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Put(Value),
    Tomb,
}

/// A stored entry with the write seq that produced it.
type SeqEntry = (u64, Entry);

/// A range tombstone: `(seq, start, end)` covering `[start, end)`.
type Rt = (u64, Key, Key);

#[derive(Clone)]
struct LsmState {
    /// Monotonic write seq; every point op and range tombstone gets one.
    seq: u64,
    memtable: OrdMap<Key, SeqEntry>,
    /// Range tombstones written to the memtable, oldest first.
    mem_rts: Vec<Rt>,
    /// Next file id. Ids increase with creation.
    next_file: u64,
    /// levels[0] = L0, levels[1] = the bottommost level.
    levels: Vec<Vec<Arc<LsmFile>>>,
    /// GC-drop registry: keys the compaction filter dropped, with the era of
    /// their latest drop. A snapshot opened at an older era reads them as
    /// absent (G0-R5-1: the adversarial choice the kv contract allows), even
    /// though it keeps its file set alive.
    dropped: OrdMap<Key, u64>,
    /// Bumped by every compaction that drops keys; pairs with `dropped`.
    era: u64,
}

struct LsmFile {
    id: u64,
    /// Closed key range the content touches: entry keys and range-tombstone
    /// bounds (an exclusive tombstone end counts as touched, which
    /// over-approximates the range and is always safe for overlap tests).
    lo: Key,
    hi: Key,
    entries: OrdMap<Key, SeqEntry>,
    rts: Vec<Rt>,
}

/// A read/merge source: point entries plus range tombstones.
struct Src<'a> {
    entries: &'a OrdMap<Key, SeqEntry>,
    rts: &'a [Rt],
}

/// The newest opinion about one key over some sources.
enum View<'a> {
    /// A visible value at that seq.
    Put(u64, &'a Value),
    /// A delete marker at that seq: hides older entries, kept as an entry.
    Tomb(u64),
    /// Hidden by a covering range tombstone: dropped in merges.
    Hidden,
}

fn covers(start: &[u8], end: &[u8], key: &[u8]) -> bool {
    start <= key && key < end
}

/// The highest-seq opinion about `key` over `srcs`. Unique seqs make this
/// exact regardless of which file (or memtable) holds each entry.
fn newest<'a>(key: &[u8], srcs: &[Src<'a>]) -> Option<View<'a>> {
    let mut best: Option<(u64, View<'a>)> = None;
    for src in srcs {
        if let Some((seq, e)) = src.entries.get(key) {
            let v = match e {
                Entry::Put(v) => View::Put(*seq, v),
                Entry::Tomb => View::Tomb(*seq),
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

impl LsmState {
    fn new() -> Self {
        Self {
            seq: 0,
            memtable: OrdMap::new(),
            mem_rts: Vec::new(),
            next_file: 1,
            levels: vec![Vec::new(), Vec::new()],
            dropped: OrdMap::new(),
            era: 0,
        }
    }

    /// Records filter drops under a fresh era (atomic with the compaction's
    /// publish, so snapshots pair a state with its era exactly).
    fn record_drops(&mut self, keys: &[Key]) {
        if keys.is_empty() {
            return;
        }
        self.era += 1;
        for k in keys {
            self.dropped.insert(k.clone(), self.era);
        }
    }

    fn apply(&mut self, op: Op) {
        self.seq += 1;
        match op {
            Op::Put(k, v) => {
                self.memtable.insert(k, (self.seq, Entry::Put(v)));
            }
            Op::Delete(k) => {
                self.memtable.insert(k, (self.seq, Entry::Tomb));
            }
            Op::DeleteRange { start, end } => {
                if !range_is_empty(
                    Bound::Included(start.as_slice()),
                    Bound::Excluded(end.as_slice()),
                ) {
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

    /// Point read through the merged view (§ "newest to oldest" by seq).
    fn get(&self, key: &[u8]) -> Option<Value> {
        match newest(key, &self.srcs()) {
            Some(View::Put(_, v)) => Some(v.clone()),
            _ => None,
        }
    }

    /// The visible flat map: every key whose newest opinion is a put.
    fn merged(&self) -> Map {
        let srcs = self.srcs();
        let mut keys: BTreeSet<&Key> = BTreeSet::new();
        for src in &srcs {
            keys.extend(src.entries.iter().map(|(k, _)| k));
        }
        let mut out = Map::new();
        for k in keys {
            if let Some(View::Put(_, v)) = newest(k, &srcs) {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }

    /// Memtable -> a new L0 file. Returns its id, or `None` when there is
    /// nothing to flush (no file is created).
    fn flush(&mut self) -> Option<u64> {
        let entries = std::mem::take(&mut self.memtable);
        let rts = std::mem::take(&mut self.mem_rts);
        if entries.is_empty() && rts.is_empty() {
            self.memtable = entries;
            self.mem_rts = rts;
            return None;
        }
        match bounds(&entries, &rts) {
            // Unreachable with non-empty content; restoring keeps the data.
            None => {
                self.memtable = entries;
                self.mem_rts = rts;
                None
            }
            Some((lo, hi)) => {
                let id = self.next_file;
                self.next_file += 1;
                self.levels[0].push(Arc::new(LsmFile {
                    id,
                    lo,
                    hi,
                    entries,
                    rts,
                }));
                Some(id)
            }
        }
    }

    /// `compact(level, files)` (model-checker step): validates, then
    /// [`run_compaction`]. Returns the new file ids.
    fn compact(
        &mut self,
        filter: Option<&dyn GcFilter>,
        level: usize,
        files: &[u64],
    ) -> Result<Vec<u64>> {
        if self.levels.len() < 2 || level >= self.levels.len() - 1 {
            return Err(KvError::Backend(format!(
                "compact: level {level} has no level below to compact into"
            )));
        }
        if files.is_empty() {
            return Err(KvError::Backend("compact: no input files".into()));
        }
        let inputs = self.take_files(level, |id| files.contains(&id));
        let found: Vec<u64> = inputs.iter().map(|f| f.id).collect();
        for id in files {
            if !found.contains(id) {
                return Err(KvError::Backend(format!(
                    "compact: file {id} is not at level {level}"
                )));
            }
        }
        let (ids, _) = self.run_compaction(filter, level, inputs);
        Ok(ids)
    }

    /// Moves the files of `level` whose id satisfies `keep` out of the level.
    fn take_files(&mut self, level: usize, keep: impl Fn(u64) -> bool) -> Vec<Arc<LsmFile>> {
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

    /// The compaction itself. Beyond the named files, two expansion rules
    /// keep partial compactions sound (LevelDB discipline):
    ///
    /// 1. every file of `level` overlapping the combined input range joins
    ///    the compaction — otherwise older same-level data could stay above a
    ///    range tombstone moved below, un-hiding it;
    /// 2. the files of `level+1` overlapping the combined range are consumed.
    ///
    /// The output is one new file at `level+1` (one fresh `GcStream`; see
    /// [`merge`]).
    fn run_compaction(
        &mut self,
        filter: Option<&dyn GcFilter>,
        level: usize,
        mut inputs: Vec<Arc<LsmFile>>,
    ) -> (Vec<u64>, Vec<Key>) {
        // Same-level expansion, until the combined range stops growing.
        while let Some((lo, hi)) = range_of(&inputs) {
            let extra = self.take_files(level, |_| true);
            let mut grew = Vec::new();
            let mut left = Vec::new();
            for f in extra {
                if f.lo <= hi && f.hi >= lo {
                    grew.push(f);
                } else {
                    left.push(f);
                }
            }
            self.levels[level] = left;
            if grew.is_empty() {
                break;
            }
            inputs.extend(grew);
        }
        let Some((lo, hi)) = range_of(&inputs) else {
            return (Vec::new(), Vec::new());
        };
        // Consume the overlapping files of the level below.
        let below = self.take_files(level + 1, |_| true);
        let (mut grew, mut left) = (Vec::new(), Vec::new());
        for f in below {
            if f.lo <= hi && f.hi >= lo {
                grew.push(f);
            } else {
                left.push(f);
            }
        }
        self.levels[level + 1] = left;
        inputs.extend(grew);

        let above: Vec<Arc<LsmFile>> = self.levels[..=level]
            .iter()
            .flatten()
            .map(Arc::clone)
            .collect();
        let bottom = level + 1 == self.levels.len() - 1;
        let (entries, rts, dropped) = merge(&inputs, filter, bottom, &above);
        let mut ids = Vec::new();
        if let Some((flo, fhi)) = bounds(&entries, &rts) {
            let id = self.next_file;
            self.next_file += 1;
            self.levels[level + 1].push(Arc::new(LsmFile {
                id,
                lo: flo,
                hi: fhi,
                entries,
                rts,
            }));
            ids.push(id);
        }
        self.record_drops(&dropped);
        (ids, dropped)
    }

    /// Harness `settle`: flush, then compact all of L0 into L1 **without**
    /// running the GC filter. Later writes and range tombstones land above
    /// the settled data.
    fn settle(&mut self) {
        self.flush();
        if !self.levels[0].is_empty() {
            let inputs = self.take_files(0, |_| true);
            let _ = self.run_compaction(None, 0, inputs);
        }
    }

    /// Harness `compact`: flush, compact all of L0 into L1 through the
    /// filter, then re-stream the L1 files the compaction did not consume, so
    /// every live key passes a stream exactly once. Returns keys dropped.
    fn compact_all(&mut self, filter: Option<&dyn GcFilter>) -> usize {
        self.flush();
        let l1_before: Vec<u64> = self.levels[1].iter().map(|f| f.id).collect();
        let mut dropped = 0usize;
        if !self.levels[0].is_empty() {
            let inputs = self.take_files(0, |_| true);
            let (_, keys) = self.run_compaction(filter, 0, inputs);
            dropped += keys.len();
        }
        for id in l1_before {
            if self.levels[1].iter().any(|f| f.id == id) {
                dropped += self.restream(filter, id);
            }
        }
        dropped
    }

    /// Rewrites one bottommost file through a fresh stream (internal; the
    /// public `compact` refuses the bottommost level).
    fn restream(&mut self, filter: Option<&dyn GcFilter>, id: u64) -> usize {
        let inputs = self.take_files(1, |i| i == id);
        if inputs.is_empty() {
            return 0;
        }
        let above: Vec<Arc<LsmFile>> = self.levels[0].iter().map(Arc::clone).collect();
        let (entries, rts, dropped) = merge(&inputs, filter, true, &above);
        if let Some((lo, hi)) = bounds(&entries, &rts) {
            let nid = self.next_file;
            self.next_file += 1;
            self.levels[1].push(Arc::new(LsmFile {
                id: nid,
                lo,
                hi,
                entries,
                rts,
            }));
        }
        self.record_drops(&dropped);
        dropped.len()
    }

    /// The live files as `(level, id, (lo, hi))`.
    fn files(&self) -> Vec<FileMeta> {
        self.levels
            .iter()
            .enumerate()
            .flat_map(|(level, fs)| {
                fs.iter()
                    .map(move |f| (level, f.id, (f.lo.clone(), f.hi.clone())))
            })
            .collect()
    }
}

/// One compaction over `files`. For every key the newest opinion across the
/// inputs wins; the GC filter sees the visible put entries in ascending key
/// order through one fresh stream (`drop_key` per entry; delete markers are
/// not visible keys and are carried without consulting it). Entries hidden by
/// a covering range tombstone are dropped. At `bottom`, an input range
/// tombstone is dropped only when no surviving file above the output overlaps
/// its range (it may still hide older data up there); otherwise every input
/// range tombstone is carried into the output. Returns the output entries,
/// range tombstones, and the number of keys the filter dropped.
fn merge(
    files: &[Arc<LsmFile>],
    filter: Option<&dyn GcFilter>,
    bottom: bool,
    above: &[Arc<LsmFile>],
) -> (OrdMap<Key, SeqEntry>, Vec<Rt>, Vec<Key>) {
    let srcs: Vec<Src<'_>> = files
        .iter()
        .map(|f| Src {
            entries: &f.entries,
            rts: &f.rts,
        })
        .collect();
    let mut keys: BTreeSet<&Key> = BTreeSet::new();
    for src in &srcs {
        keys.extend(src.entries.iter().map(|(k, _)| k));
    }
    let mut entries = OrdMap::new();
    let mut dropped: Vec<Key> = Vec::new();
    let mut stream = filter.map(|f| f.begin());
    for k in keys {
        match newest(k, &srcs) {
            Some(View::Put(seq, v)) => {
                if stream.as_mut().is_some_and(|s| s.drop_key(k, v)) {
                    dropped.push(k.clone());
                } else {
                    entries.insert(k.clone(), (seq, Entry::Put(v.clone())));
                }
            }
            Some(View::Tomb(seq)) => {
                entries.insert(k.clone(), (seq, Entry::Tomb));
            }
            Some(View::Hidden) | None => {}
        }
    }
    let rts: Vec<Rt> = files
        .iter()
        .flat_map(|f| f.rts.iter().cloned())
        .filter(|(_, s, e)| !bottom || above.iter().any(|f| f.lo <= *e && f.hi >= *s))
        .collect();
    (entries, rts, dropped)
}

/// The closed combined range of `files`.
fn range_of(files: &[Arc<LsmFile>]) -> Option<(Key, Key)> {
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

/// The closed key range content touches: entry keys and range-tombstone
/// bounds (exclusive ends included, over-approximating). `None` only for
/// empty content.
fn bounds(entries: &OrdMap<Key, SeqEntry>, rts: &[Rt]) -> Option<(Key, Key)> {
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
    for k in entries.iter().map(|(k, _)| k) {
        see(k);
    }
    for (_, s, e) in rts {
        see(s);
        see(e);
    }
    Some((lo?, hi?))
}

impl Default for MemKv {
    fn default() -> Self {
        Self::from_state(KvState::Flat(Map::new()))
    }
}

impl MemKv {
    pub fn new() -> Self {
        Self::default()
    }

    /// MemKv in LSM mode (see the module docs): writes go to a memtable;
    /// `flush`/`compact`/`files` drive the levels. Flat mode (`new`) stays
    /// the default.
    pub fn lsm() -> Self {
        Self::from_state(KvState::Lsm(Box::new(LsmState::new())))
    }

    fn from_state(state: KvState) -> Self {
        Self {
            st: Arc::new(RwLock::new(state)),
            writer: Mutex::new(()),
            gc: RwLock::new(None),
            watermark: RwLock::new(0),
        }
    }

    fn current(&self) -> KvState {
        self.st
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Applies `f` to a copy of the latest state and publishes it, or nothing on error.
    fn publish(&self, f: impl FnOnce(&mut KvState) -> Result<()>) -> Result<()> {
        let _w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut next = self.current();
        f(&mut next)?;
        *self.st.write().unwrap_or_else(PoisonError::into_inner) = next;
        Ok(())
    }

    /// `publish` for LSM-mode operations; errors in flat mode.
    fn publish_lsm<T>(&self, f: impl FnOnce(&mut LsmState) -> Result<T>) -> Result<T> {
        let _w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut next = match self.current() {
            KvState::Lsm(l) => *l,
            KvState::Flat(_) => {
                return Err(KvError::Backend(
                    "MemKv is not in LSM mode (use MemKv::lsm())".into(),
                ))
            }
        };
        let out = f(&mut next)?;
        *self.st.write().unwrap_or_else(PoisonError::into_inner) = KvState::Lsm(Box::new(next));
        Ok(out)
    }

    /// LSM step: flush the memtable into a new L0 file. Returns its id, or
    /// `None` when the memtable is empty (no file is created).
    pub fn flush(&self) -> Result<Option<u64>> {
        self.publish_lsm(|l| Ok(l.flush()))
    }

    /// LSM step: `compact(level, &[file_id, ...])`. Merges the named files of
    /// `level` — expanded to every file of that level overlapping their
    /// combined range, and to the overlapping files of `level+1` — into new
    /// `level+1` files, running one fresh `GcStream` per output file. Range
    /// tombstones are dropped only when compacting into the bottommost level,
    /// and only when no file above the output overlaps their range. Returns
    /// the new file ids. Errors in flat mode.
    pub fn compact(&self, level: usize, files: &[u64]) -> Result<Vec<u64>> {
        let gc = self.gc.read().unwrap_or_else(PoisonError::into_inner);
        self.publish_lsm(|l| l.compact(gc.as_deref(), level, files))
    }

    /// LSM step: the live files as `(level, file_id, (lo, hi))`, the closed
    /// key range each file's entries and range tombstones touch. Errors in
    /// flat mode.
    pub fn files(&self) -> Result<Vec<FileMeta>> {
        match &*self.st.read().unwrap_or_else(PoisonError::into_inner) {
            KvState::Flat(_) => Err(KvError::Backend(
                "files: MemKv is not in LSM mode (use MemKv::lsm())".into(),
            )),
            KvState::Lsm(l) => Ok(l.files()),
        }
    }

    /// Harness `settle`: flush and compact everything down one level without
    /// running the GC filter. Errors in flat mode.
    pub fn settle(&self) -> Result<()> {
        self.publish_lsm(|l| {
            l.settle();
            Ok(())
        })
    }

    /// A full, synchronous compaction: every live key passes through the
    /// registered GC filter exactly once. Flat mode: one stream over the map.
    /// LSM mode: flush, compact all of L0 into L1, then re-stream the L1
    /// files that compaction did not consume. Returns the number dropped; 0
    /// without a filter. Flat mode: snapshots taken earlier keep seeing
    /// dropped keys. LSM mode: a drop is visible to already-open snapshots
    /// (G0-R5-1). The filter must not call back into this store.
    pub fn compact_all(&self) -> usize {
        let gc = self.gc.read().unwrap_or_else(PoisonError::into_inner);
        let filter = gc.as_deref();
        let mut dropped = 0;
        let res = self.publish(|st| {
            match st {
                KvState::Flat(map) => {
                    if let Some(f) = filter {
                        let mut stream = f.begin();
                        let doomed: Vec<Key> = map
                            .iter()
                            .filter(|(k, v)| stream.drop_key(k, v))
                            .map(|(k, _)| k.clone())
                            .collect();
                        for k in &doomed {
                            map.remove(k);
                        }
                        dropped = doomed.len();
                    }
                }
                KvState::Lsm(l) => dropped = l.compact_all(filter),
            }
            Ok(())
        });
        match res {
            Ok(()) => dropped,
            Err(_) => 0,
        }
    }

    /// Opens a checkpoint written by `checkpoint`. A damaged file is
    /// `KvError::Corruption`; an unknown format version is `KvError::Format`.
    pub fn open_checkpoint(dir: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        File::open(dir.join(CHECKPOINT_FILE))?.read_to_end(&mut bytes)?;
        Ok(Self::from_state(decode_state(&bytes)?))
    }
}

impl OrderedKv for MemKv {
    type Snap = MemSnap;

    fn write(&self, batch: Batch, _sync: Durability) -> Result<()> {
        self.publish(|st| {
            for op in batch.ops {
                match st {
                    KvState::Flat(map) => apply(map, op),
                    KvState::Lsm(l) => l.apply(op),
                }
            }
            Ok(())
        })
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(())
    }

    fn snapshot(&self) -> MemSnap {
        let st = self.st.read().unwrap_or_else(PoisonError::into_inner);
        let live = match &*st {
            KvState::Lsm(_) => Some(Arc::clone(&self.st)),
            KvState::Flat(_) => None,
        };
        MemSnap {
            state: st.clone(),
            live,
        }
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(
            match &*self.st.read().unwrap_or_else(PoisonError::into_inner) {
                KvState::Flat(m) => m.get(key).cloned(),
                KvState::Lsm(l) => l.get(key),
            },
        )
    }

    /// Rejects keys that are not strictly ascending (`KvError::Backend`),
    /// leaving the store untouched.
    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.publish(|st| {
            let mut prev: Option<Key> = None;
            for (k, v) in entries {
                if prev.as_ref().is_some_and(|p| k <= *p) {
                    return Err(KvError::Backend(
                        "ingest_sorted: keys not strictly ascending".into(),
                    ));
                }
                match st {
                    KvState::Flat(map) => {
                        map.insert(k.clone(), v);
                    }
                    KvState::Lsm(l) => l.apply(Op::Put(k.clone(), v)),
                }
                prev = Some(k);
            }
            Ok(())
        })
    }

    /// Writes one file, `dir/MEMKV`: magic, version, mode-specific body,
    /// CRC-32C trailer. `dir` must not exist; its parent must.
    fn checkpoint(&self, dir: &Path) -> Result<()> {
        let bytes = match &*self.st.read().unwrap_or_else(PoisonError::into_inner) {
            KvState::Flat(m) => encode(m)?,
            KvState::Lsm(l) => encode_lsm(l)?,
        };
        fs::create_dir(dir)?;
        let tmp = dir.join(CHECKPOINT_TMP);
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, dir.join(CHECKPOINT_FILE))?;
        sync_dir(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        *self.gc.write().unwrap_or_else(PoisonError::into_inner) = Some(filter);
    }

    /// No native timestamps: the watermark is tracked (monotonic, C-T0 §9.1)
    /// but GC runs through the filter in `compact_all` only.
    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        let mut w = self
            .watermark
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if watermark < *w {
            return Err(KvError::WatermarkRegressed {
                current: *w,
                requested: watermark,
            });
        }
        *w = watermark;
        Ok(())
    }
}

/// Point-in-time view: a structurally shared copy of the state (the file set
/// included, keeping it alive).
pub struct MemSnap {
    state: KvState,
    /// Live-state handle (LSM mode only): a key the compaction filter drops
    /// while this snapshot is open reads as absent through it — the
    /// adversarial choice the kv contract allows (C-T0 §11 G0-gc, G0-R5-1).
    /// Flat mode registers no drops; its snapshots keep dropped keys by
    /// structural sharing.
    live: Option<Arc<RwLock<KvState>>>,
}

impl MemSnap {
    /// The live GC-drop registry, cloned once per read (cheap: persistent
    /// map). Empty unless this is an LSM snapshot.
    fn live_drops(&self) -> OrdMap<Key, u64> {
        match &self.live {
            Some(live) => match &*live.read().unwrap_or_else(PoisonError::into_inner) {
                KvState::Lsm(l) => l.dropped.clone(),
                KvState::Flat(_) => OrdMap::new(),
            },
            None => OrdMap::new(),
        }
    }

    /// The drop era of the state this snapshot opened at.
    fn open_era(&self) -> u64 {
        match &self.state {
            KvState::Lsm(l) => l.era,
            KvState::Flat(_) => 0,
        }
    }

    /// A drop recorded after this snapshot opened hides the key from it.
    fn hidden(drops: &OrdMap<Key, u64>, era: u64, key: &[u8]) -> bool {
        drops.get(key).is_some_and(|e| *e > era)
    }
}

impl Snapshot for MemSnap {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(match &self.state {
            KvState::Flat(m) => m.get(key).cloned(),
            KvState::Lsm(l) => {
                let v = l.get(key);
                if v.is_some() && Self::hidden(&self.live_drops(), self.open_era(), key) {
                    None
                } else {
                    v
                }
            }
        })
    }

    fn scan<'a>(
        &'a self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a> {
        if range_is_empty(range.0, range.1) {
            return Box::new(std::iter::empty());
        }
        match &self.state {
            KvState::Flat(map) => {
                let it = map
                    .range::<_, [u8]>(range)
                    .map(|(k, v)| Ok((k.clone(), v.clone())));
                if reverse {
                    Box::new(it.rev())
                } else {
                    Box::new(it)
                }
            }
            KvState::Lsm(l) => {
                let drops = self.live_drops();
                let era = self.open_era();
                let rows: Vec<(Key, Value)> = l
                    .merged()
                    .into_iter()
                    .filter(|(k, _)| (range.0, range.1).contains(k.as_slice()))
                    .filter(|(k, _)| !Self::hidden(&drops, era, k))
                    .collect();
                let it = rows.into_iter().map(Ok);
                if reverse {
                    Box::new(it.rev())
                } else {
                    Box::new(it)
                }
            }
        }
    }
}

fn apply(map: &mut Map, op: Op) {
    match op {
        Op::Put(k, v) => {
            map.insert(k, v);
        }
        Op::Delete(k) => {
            map.remove(&k);
        }
        Op::DeleteRange { start, end } => {
            let range = (
                Bound::Included(start.as_slice()),
                Bound::Excluded(end.as_slice()),
            );
            if range_is_empty(range.0, range.1) {
                return;
            }
            let doomed: Vec<Key> = map
                .range::<_, [u8]>(range)
                .map(|(k, _)| k.clone())
                .collect();
            for k in &doomed {
                map.remove(k);
            }
        }
    }
}

/// True for ranges that contain no key, including inverted ones.
fn range_is_empty(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> bool {
    match (lo, hi) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b))
        | (Bound::Excluded(a), Bound::Included(b)) => a >= b,
        _ => false,
    }
}

fn encode(map: &Map) -> Result<Vec<u8>> {
    let mut b = Vec::new();
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&(map.len() as u64).to_le_bytes());
    for (k, v) in map.iter() {
        put_bytes(&mut b, k)?;
        put_bytes(&mut b, v)?;
    }
    let crc = crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    Ok(b)
}

fn encode_lsm(l: &LsmState) -> Result<Vec<u8>> {
    let mut b = Vec::new();
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
    b.extend_from_slice(&l.seq.to_le_bytes());
    b.extend_from_slice(&l.next_file.to_le_bytes());
    b.extend_from_slice(&l.era.to_le_bytes());
    b.extend_from_slice(&(l.dropped.len() as u64).to_le_bytes());
    for (k, era) in &l.dropped {
        put_bytes(&mut b, k)?;
        b.extend_from_slice(&era.to_le_bytes());
    }
    put_entries(&mut b, &l.memtable)?;
    b.extend_from_slice(&(l.mem_rts.len() as u64).to_le_bytes());
    for (seq, s, e) in &l.mem_rts {
        put_rt(&mut b, *seq, s, e)?;
    }
    b.push(l.levels.len() as u8);
    for level in &l.levels {
        b.extend_from_slice(&(level.len() as u64).to_le_bytes());
        for f in level {
            b.extend_from_slice(&f.id.to_le_bytes());
            put_entries(&mut b, &f.entries)?;
            b.extend_from_slice(&(f.rts.len() as u64).to_le_bytes());
            for (seq, s, e) in &f.rts {
                put_rt(&mut b, *seq, s, e)?;
            }
        }
    }
    let crc = crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    Ok(b)
}

fn put_entries(b: &mut Vec<u8>, entries: &OrdMap<Key, SeqEntry>) -> Result<()> {
    b.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (k, (seq, e)) in entries {
        put_bytes(b, k)?;
        b.extend_from_slice(&seq.to_le_bytes());
        match e {
            Entry::Put(v) => {
                b.push(1);
                put_bytes(b, v)?;
            }
            Entry::Tomb => b.push(0),
        }
    }
    Ok(())
}

fn put_rt(b: &mut Vec<u8>, seq: u64, start: &[u8], end: &[u8]) -> Result<()> {
    b.extend_from_slice(&seq.to_le_bytes());
    put_bytes(b, start)?;
    put_bytes(b, end)
}

fn put_bytes(b: &mut Vec<u8>, s: &[u8]) -> Result<()> {
    let n = u32::try_from(s.len())
        .map_err(|_| KvError::Backend("checkpoint: entry longer than u32::MAX".into()))?;
    b.extend_from_slice(&n.to_le_bytes());
    b.extend_from_slice(s);
    Ok(())
}

fn corrupt(what: &str) -> KvError {
    KvError::Corruption(format!("memkv checkpoint: {what}"))
}

/// Decodes a checkpoint body: version 1 is the flat layout, `CHECKPOINT_VERSION`
/// (2) the LSM layout, anything else `KvError::Format` (checked before the
/// checksum, like the flat decoder always did).
fn decode_state(b: &[u8]) -> Result<KvState> {
    let mut head = Cursor { b, pos: 0 };
    if head.take(MAGIC.len())? != MAGIC.as_slice() {
        return Err(corrupt("bad magic"));
    }
    let found = head.u32()?;
    if found != 1 && found != CHECKPOINT_VERSION {
        return Err(KvError::Format {
            found,
            supported: CHECKPOINT_VERSION,
        });
    }
    let body_len = b
        .len()
        .checked_sub(4)
        .filter(|&n| n >= head.pos)
        .ok_or_else(|| corrupt("truncated"))?;
    let (body, trailer) = b.split_at(body_len);
    let mut want = [0u8; 4];
    want.copy_from_slice(trailer);
    if crc32c(body) != u32::from_le_bytes(want) {
        return Err(corrupt("checksum mismatch"));
    }
    let mut c = Cursor {
        b: body,
        pos: head.pos,
    };
    match found {
        1 => Ok(KvState::Flat(decode_flat_body(&mut c)?)),
        _ => Ok(KvState::Lsm(Box::new(decode_lsm_body(&mut c)?))),
    }
}

fn decode_flat_body(c: &mut Cursor<'_>) -> Result<Map> {
    let count = c.u64()?;
    let mut map = Map::new();
    let mut prev: Option<&[u8]> = None;
    for _ in 0..count {
        let klen = c.u32()? as usize;
        let k = c.take(klen)?;
        let vlen = c.u32()? as usize;
        let v = c.take(vlen)?;
        if prev.is_some_and(|p| p >= k) {
            return Err(corrupt("keys out of order"));
        }
        map.insert(k.to_vec(), v.to_vec());
        prev = Some(k);
    }
    if c.pos != c.b.len() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(map)
}

fn decode_lsm_body(c: &mut Cursor<'_>) -> Result<LsmState> {
    let seq = c.u64()?;
    let next_file = c.u64()?;
    let era = c.u64()?;
    let mut dropped = OrdMap::new();
    for _ in 0..c.u64()? {
        let klen = c.u32()? as usize;
        let k = c.take(klen)?.to_vec();
        let e = c.u64()?;
        dropped.insert(k, e);
    }
    let memtable = take_entries(c)?;
    let mut mem_rts = Vec::new();
    for _ in 0..c.u64()? {
        mem_rts.push(take_rt(c)?);
    }
    let level_count = c
        .take(1)?
        .first()
        .copied()
        .ok_or_else(|| corrupt("truncated"))?;
    if level_count as usize != LSM_LEVELS {
        return Err(corrupt("unsupported level count"));
    }
    let mut levels = Vec::new();
    for _ in 0..level_count {
        let mut level = Vec::new();
        for _ in 0..c.u64()? {
            let id = c.u64()?;
            let entries = take_entries(c)?;
            let mut rts = Vec::new();
            for _ in 0..c.u64()? {
                rts.push(take_rt(c)?);
            }
            let (lo, hi) = bounds(&entries, &rts).ok_or_else(|| corrupt("empty file"))?;
            level.push(Arc::new(LsmFile {
                id,
                lo,
                hi,
                entries,
                rts,
            }));
        }
        levels.push(level);
    }
    if c.pos != c.b.len() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(LsmState {
        seq,
        next_file,
        era,
        dropped,
        memtable,
        mem_rts,
        levels,
    })
}

fn take_entries(c: &mut Cursor<'_>) -> Result<OrdMap<Key, SeqEntry>> {
    let mut map = OrdMap::new();
    let mut prev: Option<&[u8]> = None;
    for _ in 0..c.u64()? {
        let klen = c.u32()? as usize;
        let k = c.take(klen)?;
        let seq = c.u64()?;
        let kind = c
            .take(1)?
            .first()
            .copied()
            .ok_or_else(|| corrupt("truncated"))?;
        let e = match kind {
            0 => Entry::Tomb,
            1 => {
                let vlen = c.u32()? as usize;
                Entry::Put(c.take(vlen)?.to_vec())
            }
            _ => return Err(corrupt("bad entry kind")),
        };
        if prev.is_some_and(|p| p >= k) {
            return Err(corrupt("keys out of order"));
        }
        map.insert(k.to_vec(), (seq, e));
        prev = Some(k);
    }
    Ok(map)
}

fn take_rt(c: &mut Cursor<'_>) -> Result<Rt> {
    let seq = c.u64()?;
    let slen = c.u32()? as usize;
    let start = c.take(slen)?.to_vec();
    let elen = c.u32()? as usize;
    let end = c.take(elen)?.to_vec();
    Ok((seq, start, end))
}

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.b.len())
            .ok_or_else(|| corrupt("truncated"))?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u32(&mut self) -> Result<u32> {
        let mut a = [0u8; 4];
        a.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(a))
    }

    fn u64(&mut self) -> Result<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(a))
    }
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

const CRC32C_TABLE: [u32; 256] = crc32c_table();

const fn crc32c_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0x82F6_3B78 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

/// CRC-32C (Castagnoli).
fn crc32c(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c = CRC32C_TABLE[((c ^ u32::from(b)) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

/// Conformance harness for MemKv (flat mode).
#[cfg(any(test, feature = "conformance"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct MemHarness;

#[cfg(any(test, feature = "conformance"))]
impl crate::conformance::Harness for MemHarness {
    type Kv = MemKv;

    fn make(&self) -> MemKv {
        MemKv::new()
    }

    fn compact(&self, kv: &MemKv) -> Result<()> {
        kv.compact_all();
        Ok(())
    }

    fn open_checkpoint(&self, dir: &Path) -> Result<MemKv> {
        MemKv::open_checkpoint(dir)
    }
}

/// Conformance harness for MemKv in LSM mode: `settle` and `compact` flush
/// and compact everything (settle without running the filter).
#[cfg(any(test, feature = "conformance"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct MemLsmHarness;

#[cfg(any(test, feature = "conformance"))]
impl crate::conformance::Harness for MemLsmHarness {
    type Kv = MemKv;

    fn make(&self) -> MemKv {
        MemKv::lsm()
    }

    fn compact(&self, kv: &MemKv) -> Result<()> {
        kv.compact_all();
        Ok(())
    }

    fn open_checkpoint(&self, dir: &Path) -> Result<MemKv> {
        MemKv::open_checkpoint(dir)
    }

    fn settle(&self, kv: &MemKv) -> Result<()> {
        kv.settle()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{layout, read_at, DropsTombstonesLeW, FaultHarness, SpecGcFilter};
    use std::collections::BTreeSet;

    crate::kv_conformance_tests!(mem, MemHarness);
    crate::kv_conformance_tests!(fault_mem, FaultHarness(MemHarness));
    crate::kv_conformance_tests!(mem_lsm, MemLsmHarness);
    crate::kv_conformance_tests!(fault_mem_lsm, FaultHarness(MemLsmHarness));

    fn ok<T>(r: Result<T>, what: &str) -> T {
        match r {
            Ok(v) => v,
            Err(e) => panic!("{what}: {e}"),
        }
    }

    fn put(kv: &MemKv, key: &[u8], val: &[u8]) {
        ok(
            kv.write(
                Batch {
                    ops: vec![Op::Put(key.to_vec(), val.to_vec())],
                },
                Durability::No,
            ),
            "write",
        );
    }

    fn ops(kv: &MemKv, batch: Vec<Op>) {
        ok(kv.write(Batch { ops: batch }, Durability::No), "write");
    }

    fn get(kv: &MemKv, key: &[u8]) -> Option<Value> {
        ok(kv.get_latest(key), "get_latest")
    }

    fn dump(snap: &MemSnap) -> Vec<(Key, Value)> {
        snap.scan((Bound::Unbounded, Bound::Unbounded), false)
            .map(|r| match r {
                Ok(kv) => kv,
                Err(e) => panic!("scan: {e}"),
            })
            .collect()
    }

    fn flush_id(kv: &MemKv) -> u64 {
        match ok(kv.flush(), "flush") {
            Some(id) => id,
            None => panic!("flush produced no file"),
        }
    }

    #[test]
    fn crc32c_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    fn sample() -> MemKv {
        let kv = MemKv::new();
        let mut b = Batch::default();
        b.put(vec![0], vec![1, 2, 3]);
        b.put(b"key".to_vec(), b"value".to_vec());
        b.put(vec![0xff, 0xff], Vec::new());
        assert!(kv.write(b, Durability::Yes).is_ok());
        kv
    }

    fn flat_state(kv: &MemKv) -> Map {
        match kv.current() {
            KvState::Flat(m) => m,
            KvState::Lsm(_) => panic!("expected flat mode"),
        }
    }

    fn checkpoint_bytes() -> Vec<u8> {
        match encode(&flat_state(&sample())) {
            Ok(b) => b,
            Err(e) => panic!("encode: {e}"),
        }
    }

    #[test]
    fn decode_round_trips() {
        let map = flat_state(&sample());
        assert!(matches!(decode_state(&checkpoint_bytes()), Ok(KvState::Flat(m)) if m == map));
    }

    #[test]
    fn every_flipped_byte_is_detected() {
        let good = checkpoint_bytes();
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x01;
            match decode_state(&bad) {
                Err(KvError::Corruption(_)) => {}
                // A flipped version field reads as an unknown version.
                Err(KvError::Format { .. }) if (8..12).contains(&i) => {}
                other => panic!(
                    "byte {i}: {:?}",
                    other.map(|s| match s {
                        KvState::Flat(m) => m.len(),
                        KvState::Lsm(_) => usize::MAX,
                    })
                ),
            }
        }
    }

    #[test]
    fn every_truncation_is_detected() {
        let good = checkpoint_bytes();
        for n in 0..good.len() {
            assert!(
                matches!(decode_state(&good[..n]), Err(KvError::Corruption(_))),
                "len {n}"
            );
        }
    }

    #[test]
    fn unknown_version_is_format_error() {
        let mut b = checkpoint_bytes();
        b[8..12].copy_from_slice(&3u32.to_le_bytes());
        assert!(matches!(
            decode_state(&b),
            Err(KvError::Format {
                found: 3,
                supported: CHECKPOINT_VERSION
            })
        ));
    }

    #[test]
    fn unsorted_body_with_valid_crc_is_corruption() {
        let mut b = Vec::new();
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&2u64.to_le_bytes());
        for k in [b"b", b"a"] {
            assert!(put_bytes(&mut b, k).is_ok());
            assert!(put_bytes(&mut b, b"v").is_ok());
        }
        let crc = crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(decode_state(&b), Err(KvError::Corruption(_))));
    }

    #[test]
    fn checkpoint_refuses_existing_dir_and_reports_damage() {
        let dir = crate::conformance::scratch_dir("memkv-damage");
        let kv = sample();
        assert!(kv.checkpoint(&dir).is_ok());
        assert!(kv.checkpoint(&dir).is_err());
        let path = dir.join(CHECKPOINT_FILE);
        let mut bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) => panic!("read: {e}"),
        };
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x80;
        assert!(fs::write(&path, &bytes).is_ok());
        assert!(matches!(
            MemKv::open_checkpoint(&dir),
            Err(KvError::Corruption(_))
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_without_filter_drops_nothing() {
        assert_eq!(sample().compact_all(), 0);
    }

    // ---- LSM mode ----

    #[test]
    fn lsm_steps_reject_flat_mode_and_bad_inputs() {
        let flat = MemKv::new();
        assert!(flat.flush().is_err());
        assert!(flat.compact(0, &[1]).is_err());
        assert!(flat.files().is_err());
        assert!(flat.settle().is_err());
        assert_eq!(flat.compact_all(), 0);
        assert_eq!(get(&flat, b"nope"), None);

        let kv = MemKv::lsm();
        assert!(kv.compact(0, &[]).is_err(), "no input files");
        assert!(kv.compact(0, &[1]).is_err(), "no such file");
        assert!(kv.compact(1, &[1]).is_err(), "bottommost level");
        assert_eq!(ok(kv.files(), "files"), vec![]);

        put(&kv, b"a", b"1");
        put(&kv, b"b", b"1");
        let f1 = flush_id(&kv);
        assert!(kv.compact(0, &[f1, 99]).is_err(), "one id missing");
        assert_eq!(
            get(&kv, b"a"),
            Some(b"1".to_vec()),
            "state changed by a failed compact"
        );
        ok(kv.compact(0, &[f1]), "compact");
        let files = ok(kv.files(), "files");
        assert_eq!(
            files,
            vec![(1, 2, (b"a".to_vec(), b"b".to_vec()))],
            "one L1 file covering the data"
        );
    }

    #[test]
    fn lsm_reads_merge_memtable_and_files() {
        let kv = MemKv::lsm();
        put(&kv, b"k/a", b"1");
        put(&kv, b"k/b", b"1");
        put(&kv, b"k/c", b"1");
        let f = flush_id(&kv);
        ok(kv.compact(0, &[f]), "compact"); // all in L1

        // Memtable shadows L1; a delete marker hides it; a range tombstone
        // hides older data in lower files.
        put(&kv, b"k/b", b"2");
        ops(&kv, vec![Op::Delete(b"k/a".to_vec())]);
        ops(
            &kv,
            vec![Op::DeleteRange {
                start: b"k/c".to_vec(),
                end: b"k/z".to_vec(),
            }],
        );
        put(&kv, b"k/c", b"3"); // newer than the range tombstone
        let snap = kv.snapshot();
        let model: Vec<(Key, Value)> = vec![
            (b"k/b".to_vec(), b"2".to_vec()),
            (b"k/c".to_vec(), b"3".to_vec()),
        ];
        assert_eq!(dump(&snap), model);
        let mut rev = model.clone();
        rev.reverse();
        assert_eq!(
            snap.scan((Bound::Unbounded, Bound::Unbounded), true)
                .map(|r| match r {
                    Ok(kv) => kv,
                    Err(e) => panic!("scan: {e}"),
                })
                .collect::<Vec<_>>(),
            rev
        );
        assert_eq!(get(&kv, b"k/a"), None);
        assert_eq!(get(&kv, b"k/b"), Some(b"2".to_vec()));
        assert_eq!(get(&kv, b"k/c"), Some(b"3".to_vec()));

        // The snapshot is isolated from later writes and outlives the store.
        put(&kv, b"k/b", b"9");
        ops(&kv, vec![Op::Delete(b"k/c".to_vec())]);
        assert_eq!(dump(&snap), model);
        drop(kv);
        assert_eq!(dump(&snap), model);
    }

    #[test]
    fn lsm_range_tombstone_survives_partial_compaction() {
        // Data older than a range tombstone may sit in a *higher* level file
        // (it was flushed earlier). Compacting only the tombstone's file must
        // not un-hide that data: the same-level expansion pulls the older
        // file into the compaction.
        let kv = MemKv::lsm();
        put(&kv, b"x/a", b"old");
        put(&kv, b"x/b", b"old");
        let older = flush_id(&kv);
        ops(
            &kv,
            vec![Op::DeleteRange {
                start: b"x/a".to_vec(),
                end: b"x/c".to_vec(),
            }],
        );
        let newer = flush_id(&kv);
        assert_ne!(older, newer);
        ok(kv.compact(0, &[newer]), "compact only the tombstone's file");
        assert_eq!(get(&kv, b"x/a"), None, "un-hidden by the compaction");
        assert_eq!(get(&kv, b"x/b"), None, "un-hidden by the compaction");
        assert!(ok(kv.files(), "files").is_empty(), "everything merged away");

        // Writes after the range tombstone stay visible.
        put(&kv, b"x/b", b"new");
        assert_eq!(get(&kv, b"x/b"), Some(b"new".to_vec()));
        ok(kv.settle(), "settle");
        assert_eq!(get(&kv, b"x/b"), Some(b"new".to_vec()));
        assert_eq!(get(&kv, b"x/a"), None);
    }

    #[test]
    fn lsm_drop_visible_to_open_snapshots() {
        // G0-R5-1: a compaction-filter drop is visible to already-open
        // snapshots (the adversarial choice the kv contract allows), which the
        // G0-gc model needs from LSM mode. Kept keys never change and later
        // writes stay invisible to the old snapshot.
        use crate::GcStream;

        struct DropsGarbage;
        struct DropsGarbageStream;
        impl GcFilter for DropsGarbage {
            fn begin(&self) -> Box<dyn GcStream> {
                Box::new(DropsGarbageStream)
            }
        }
        impl GcStream for DropsGarbageStream {
            fn drop_key(&mut self, _key: &[u8], value: &[u8]) -> bool {
                value.starts_with(b"garbage")
            }
        }

        let kv = MemKv::lsm();
        put(&kv, b"gk/a", b"garbage");
        put(&kv, b"gk/b", b"live");
        ok(kv.settle(), "settle");
        let snap = kv.snapshot();
        assert_eq!(ok(snap.get(b"gk/a"), "get"), Some(b"garbage".to_vec()));

        kv.set_gc_filter(Box::new(DropsGarbage));
        assert_eq!(kv.compact_all(), 1, "gk/a dropped");
        assert_eq!(get(&kv, b"gk/a"), None, "dropped from the live state");
        assert_eq!(
            ok(snap.get(b"gk/a"), "get"),
            None,
            "drop visible to the open snapshot"
        );
        assert_eq!(
            ok(snap.get(b"gk/b"), "get"),
            Some(b"live".to_vec()),
            "kept key changed"
        );
        put(&kv, b"gk/c", b"later");
        assert_eq!(ok(snap.get(b"gk/c"), "get"), None, "later write visible");
        assert_eq!(
            dump(&snap),
            vec![(b"gk/b".to_vec(), b"live".to_vec())],
            "scan hides the dropped key"
        );

        // The registry survives a checkpoint round trip: the reopened store
        // reads exact state and does not hide keys on fresh snapshots.
        let dir = crate::conformance::scratch_dir("memkv-lsm-drops");
        ok(kv.checkpoint(&dir), "checkpoint");
        let back = ok(MemKv::open_checkpoint(&dir), "open_checkpoint");
        assert_eq!(get(&back, b"gk/a"), None);
        assert_eq!(get(&back, b"gk/b"), Some(b"live".to_vec()));
        assert_eq!(get(&back, b"gk/c"), Some(b"later".to_vec()));
        put(&back, b"gk/a", b"again");
        assert_eq!(get(&back, b"gk/a"), Some(b"again".to_vec()));
        let back_snap = back.snapshot();
        assert_eq!(ok(back_snap.get(b"gk/a"), "get"), Some(b"again".to_vec()));
        assert_eq!(ok(back_snap.get(b"gk/b"), "get"), Some(b"live".to_vec()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn lsm_checkpoint_round_trip() {
        let kv = MemKv::lsm();
        ops(
            &kv,
            vec![
                Op::Put(b"a".to_vec(), b"1".to_vec()),
                Op::Put(b"c".to_vec(), b"1".to_vec()),
                Op::Put(b"d".to_vec(), b"1".to_vec()),
                Op::DeleteRange {
                    start: b"c".to_vec(),
                    end: b"e".to_vec(),
                },
            ],
        );
        let f1 = flush_id(&kv);
        ok(kv.compact(0, &[f1]), "compact");
        ops(
            &kv,
            vec![
                Op::Put(b"c".to_vec(), b"2".to_vec()),
                Op::Delete(b"a".to_vec()),
            ],
        );
        flush_id(&kv); // leave an L0 file in the checkpoint too
        put(&kv, b"m", b"memtable");
        let before = dump(&kv.snapshot());
        let files_before = ok(kv.files(), "files");
        assert_eq!(files_before.len(), 2);

        let dir = crate::conformance::scratch_dir("memkv-lsm-ckpt");
        ok(kv.checkpoint(&dir), "checkpoint");
        put(&kv, b"after", b"x");
        assert_eq!(get(&kv, b"after"), Some(b"x".to_vec()));

        let back = ok(MemKv::open_checkpoint(&dir), "open_checkpoint");
        assert_eq!(dump(&back.snapshot()), before, "reopened state");
        assert_eq!(ok(back.files(), "files"), files_before, "file set");
        // The reopened store keeps working as an LSM.
        assert_eq!(get(&back, b"c"), Some(b"2".to_vec()));
        assert_eq!(get(&back, b"a"), None);
        assert_eq!(get(&back, b"after"), None);
        put(&back, b"new", b"y");
        ok(back.settle(), "settle");
        assert_eq!(get(&back, b"new"), Some(b"y".to_vec()));
        assert_eq!(get(&back, b"after"), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn lsm_checkpoint_damage_detected() {
        let kv = MemKv::lsm();
        ops(
            &kv,
            vec![
                Op::Put(b"a".to_vec(), b"1".to_vec()),
                Op::Put(b"b".to_vec(), b"2".to_vec()),
                Op::DeleteRange {
                    start: b"c".to_vec(),
                    end: b"e".to_vec(),
                },
                Op::Delete(b"b".to_vec()),
            ],
        );
        let f = flush_id(&kv);
        ok(kv.compact(0, &[f]), "compact");
        put(&kv, b"m", b"v");
        let dir = crate::conformance::scratch_dir("memkv-lsm-damage");
        ok(kv.checkpoint(&dir), "checkpoint");
        let good = match fs::read(dir.join(CHECKPOINT_FILE)) {
            Ok(b) => b,
            Err(e) => panic!("read: {e}"),
        };
        let _ = fs::remove_dir_all(&dir);
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x01;
            match decode_state(&bad) {
                Err(KvError::Corruption(_)) => {}
                Err(KvError::Format { .. }) if (8..12).contains(&i) => {}
                other => panic!(
                    "byte {i}: {:?}",
                    other.map(|s| match s {
                        KvState::Flat(m) => m.len(),
                        KvState::Lsm(_) => usize::MAX,
                    })
                ),
            }
        }
        for n in 0..good.len() {
            assert!(
                matches!(decode_state(&good[..n]), Err(KvError::Corruption(_))),
                "len {n}"
            );
        }
    }

    // ---- C-K3b item 7: tombstone resurrection ----

    /// The no-GC reference for a read at `s`: the newest written version
    /// with `ts <= s`, tombstones reading as not-found (C-T0 §4 step 2).
    fn shadow_read(written: &[(u64, Value)], s: u64) -> Option<Value> {
        for (ts, v) in written.iter().rev() {
            if *ts <= s {
                return if layout::is_tombstone(v) {
                    None
                } else {
                    Some(v.clone())
                };
            }
        }
        None
    }

    #[test]
    fn gc_tombstone_resurrection_lsm() {
        // (a) The bug the model must be able to exhibit (C-T0 §11 seed 3):
        // a filter that drops the newest tombstone <= W, run over only the
        // L0 file, resurrects the live version in the L1 file. The L1 file
        // does not overlap the L0 file's range ([k@101, k@101] vs [k@90,
        // k@90]), so it never joins the compaction stream.
        {
            let kv = MemKv::lsm();
            let version = |ts: u64, v: Value| Op::Put(layout::version_key(b"k", ts), v);
            ops(&kv, vec![version(90, layout::live_value(b"v90"))]);
            let f1 = flush_id(&kv);
            ok(kv.compact(0, &[f1]), "k@90 into an L1 file");
            ops(&kv, vec![version(101, layout::tombstone_value())]);
            let f2 = flush_id(&kv); // the L0 file
            kv.set_gc_filter(Box::new(DropsTombstonesLeW { w: 101 }));
            ok(kv.compact(0, &[f2]), "compact only the L0 file");
            assert_eq!(
                get(&kv, &layout::version_key(b"k", 101)),
                None,
                "tombstone dropped"
            );
            assert_eq!(
                read_at(&kv.snapshot(), b"k", 101),
                Some(layout::live_value(b"v90")),
                "resurrection: read at S >= 101 returns the live k@90"
            );
            assert_eq!(
                read_at(&kv.snapshot(), b"k", 500),
                Some(layout::live_value(b"v90"))
            );
        }

        // (b) SpecGcFilter over every order of write/flush/compact steps:
        // a read at any S >= W never changes (I-GC), and schedules that pass
        // all versions of the key through one stream do drop the shadowed
        // ones.
        struct Scenario {
            /// (ts, value) in commit order (ascending ts).
            versions: Vec<(u64, Value)>,
            w: u64,
        }
        let scenarios = [
            Scenario {
                versions: vec![
                    (90, layout::live_value(b"v90")),
                    (101, layout::tombstone_value()),
                ],
                w: 101,
            },
            Scenario {
                versions: vec![
                    (10, layout::live_value(b"v10")),
                    (20, layout::tombstone_value()),
                    (30, layout::live_value(b"v30")),
                    (40, layout::tombstone_value()),
                    (50, layout::live_value(b"v50")),
                ],
                w: 60,
            },
        ];

        fn state_sig(lsm: &LsmState, written: usize) -> Vec<u8> {
            fn chunk(b: &mut Vec<u8>, bytes: &[u8]) {
                b.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                b.extend_from_slice(bytes);
            }
            fn entries_sig(b: &mut Vec<u8>, entries: &OrdMap<Key, SeqEntry>) {
                b.extend_from_slice(&(entries.len() as u32).to_be_bytes());
                for (k, (seq, e)) in entries {
                    chunk(b, k);
                    b.extend_from_slice(&seq.to_be_bytes());
                    match e {
                        Entry::Put(v) => {
                            b.push(1);
                            chunk(b, v);
                        }
                        Entry::Tomb => b.push(0),
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
            // File ids are excluded: states differing only in ids are
            // isomorphic for every future action.
            let mut b = Vec::new();
            b.extend_from_slice(&(written as u32).to_be_bytes());
            entries_sig(&mut b, &lsm.memtable);
            rts_sig(&mut b, &lsm.mem_rts);
            for level in &lsm.levels {
                b.extend_from_slice(&(level.len() as u32).to_be_bytes());
                for f in level {
                    entries_sig(&mut b, &f.entries);
                    rts_sig(&mut b, &f.rts);
                }
            }
            b
        }

        fn dfs(
            lsm: &LsmState,
            written: usize,
            sc: &Scenario,
            filter: &dyn GcFilter,
            visited: &mut BTreeSet<Vec<u8>>,
            stats: &mut (usize, usize, usize), // states, terminals, reduced
        ) {
            // I-GC: reads at every S >= W match the no-GC shadow.
            let max_ts = sc.versions[..written]
                .iter()
                .map(|(ts, _)| *ts)
                .max()
                .unwrap_or(sc.w);
            let snap = MemSnap {
                state: KvState::Lsm(Box::new(lsm.clone())),
                live: None,
            };
            for s in sc.w..=max_ts + 1 {
                assert_eq!(
                    read_at(&snap, b"k", s),
                    shadow_read(&sc.versions[..written], s),
                    "I-GC broken at S={s} with {written} versions written"
                );
            }

            let sig = state_sig(lsm, written);
            if !visited.insert(sig) {
                return;
            }
            stats.0 += 1;

            let mut acted = false;
            if written < sc.versions.len() {
                acted = true;
                let (ts, v) = &sc.versions[written];
                let mut next = lsm.clone();
                next.apply(Op::Put(layout::version_key(b"k", *ts), v.clone()));
                dfs(&next, written + 1, sc, filter, visited, stats);
            }
            if !lsm.memtable.is_empty() || !lsm.mem_rts.is_empty() {
                acted = true;
                let mut next = lsm.clone();
                next.flush();
                dfs(&next, written, sc, filter, visited, stats);
            }
            let l0: Vec<u64> = lsm.levels[0].iter().map(|f| f.id).collect();
            if !l0.is_empty() {
                acted = true;
                for mask in 1..(1u64 << l0.len()) {
                    let subset: Vec<u64> = l0
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| mask & (1 << i) != 0)
                        .map(|(_, id)| *id)
                        .collect();
                    let mut next = lsm.clone();
                    if next.compact(Some(filter), 0, &subset).is_ok() {
                        dfs(&next, written, sc, filter, visited, stats);
                    }
                }
            }
            // Bottommost re-compaction: one L1 file alone through a fresh
            // stream (it may hold an older version whose shadowing newer
            // version sits in another file). Not an `acted` step: it is
            // always available, so terminals are the states without the
            // steps above.
            for id in lsm.levels[1].iter().map(|f| f.id) {
                let mut next = lsm.clone();
                next.restream(Some(filter), id);
                dfs(&next, written, sc, filter, visited, stats);
            }
            if !acted {
                // Terminal: every version written, memtable and L0 empty.
                stats.1 += 1;
                let visible: Vec<u64> = {
                    let mut ts: Vec<u64> = lsm
                        .merged()
                        .iter()
                        .filter_map(|(k, _)| match layout::parse(k) {
                            Some((l, layout::Entry::Version(ts))) if l.as_slice() == b"k" => {
                                Some(ts)
                            }
                            _ => None,
                        })
                        .collect();
                    ts.sort_unstable();
                    ts
                };
                let newest_le_w = sc
                    .versions
                    .iter()
                    .rev()
                    .map(|(ts, _)| *ts)
                    .find(|ts| *ts <= sc.w);
                let expected: Vec<u64> = {
                    let mut v: Vec<u64> = sc
                        .versions
                        .iter()
                        .map(|(ts, _)| *ts)
                        .filter(|ts| *ts > sc.w)
                        .collect();
                    if let Some(n) = newest_le_w {
                        v.push(n);
                    }
                    v.sort_unstable();
                    v
                };
                assert_eq!(
                    newest_le_w.and_then(|n| visible.iter().find(|ts| **ts == n).copied()),
                    newest_le_w,
                    "terminal lost the newest version <= W"
                );
                // One stream keeps at most one version <= W per key.
                for f in &lsm.levels[1] {
                    let n = f
                        .entries
                        .iter()
                        .filter(|(k, _)| {
                            matches!(
                                layout::parse(k),
                                Some((l, layout::Entry::Version(ts))) if l.as_slice() == b"k" && ts <= sc.w
                            )
                        })
                        .count();
                    assert!(n <= 1, "file {} kept {n} versions <= W", f.id);
                }
                if visible == expected {
                    stats.2 += 1;
                }
            }
        }

        for sc in &scenarios {
            let filter = SpecGcFilter { w: sc.w };
            let f: &dyn GcFilter = &filter;
            let mut visited = BTreeSet::new();
            let mut stats = (0, 0, 0);
            dfs(&LsmState::new(), 0, sc, f, &mut visited, &mut stats);
            assert!(stats.0 > 0, "nothing explored");
            assert!(stats.1 > 0, "no terminal state reached");
            assert!(
                stats.2 > 0,
                "no schedule passed all versions through one stream (shadowed never dropped)"
            );
        }
    }
}
