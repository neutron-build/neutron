//! Card A-3a's model tests of `buf` (Work 13): an oracle written from the
//! card, not from `pool.rs`, which names no frame, no bucket and no state but
//! residency, driven against the pool over every sequence of up to four ops
//! of a twelve-op alphabet, and over seeded random sequences; and the frame
//! state machine's transition table, with compile-time guards.
//!
//! Card A-3b-0 (Work 11) changes the oracle's rules for a full pool, since
//! the frame acquisition now evicts: an op that needs a frame from a full
//! pool evicts exactly one clean, unpinned page, which the driver observes
//! (by `frame_of`, before and after) and the oracle checks; with none
//! evictable it is starved, unless every frame is held, when the op is not
//! driven (one thread would wait for an unpin). The oracle stays frame-free:
//! which evictable page the clock takes is not modelled. The table gains two
//! `try_reserve` rows.
//!
//! No thread, and no hook outside `transition_table` and the guard `run`
//! installs, which only fails a run that reaches the wait for an unpin.

mod buf_support;

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use buf_support::{
    Event, EventLog, FaultFile, MARK, PS, Pauser, SPOIL, TestMap, fixture, marker, offset, page,
    put_leaf, spawn,
};
use nucleus_env::{Env, EnvError, File, MemEnv, MemFile, OpenMode};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, FrameId, FrameInfo, FrameState, HookPoint, PageKey, Pin, PinOutcome, Pool,
    PoolConfig, PoolStats, Removal,
};
use nucleus_store::meta::Ref;
use nucleus_store::page::{self, Kind};
use proptest::prelude::*;
use proptest::test_runner::{RngSeed, TestCaseError};

const L1: PageKey = PageKey::Lpid(1);
const L2: PageKey = PageKey::Lpid(2);
const L3: PageKey = PageKey::Lpid(3);
const L4: PageKey = PageKey::Lpid(4);
const M0: PageKey = PageKey::MapLeaf(0);

/// One op of the model's alphabet (card A-3a, Work 13). Indices pick a held
/// pin modulo the number held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Pin(PageKey),
    PinResident(PageKey),
    Drop(usize),
    Write(usize, u8),
    Evict(PageKey),
    DropClean(PageKey),
    Create(PageKey),
    Free(usize),
    Corrupt(PageKey),
}

/// An op's outcome, as the model predicts it and the pool gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// A pin, with the page's marker.
    Pinned(u8),
    Unmapped,
    Starved,
    Corrupt,
    /// `pin_resident` found the key not resident.
    NotResident,
    /// The op's precondition was unmet; not driven.
    Skipped,
    Invalid,
    Dropped,
    Written,
    /// Not resident, or pinned: no victim.
    NoVictim,
    Removed,
    Kept,
    Created,
    Exists,
    Freed,
    Spoiled,
}

/// A resident page in the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Res {
    content: u8,
    dirty: bool,
    freed: bool,
    pins: u64,
}

/// The oracle (card A-3a, Work 13's table; card A-3b-0, Work 11's rules for a
/// full pool): a page file, the test map, the resident pages, the pins held,
/// the reads made and the frame acquisition's evictions. It knows `F` only as
/// a count: `free = F − |res|`.
struct Model {
    frames: u32,
    disk: HashMap<u64, (u8, bool)>,
    entry: HashMap<PageKey, Option<u64>>,
    res: HashMap<PageKey, Res>,
    held: Vec<PageKey>,
    reads: u64,
    next_slot: u64,
    /// How many pages the pool's own frame acquisition evicted (card
    /// A-3b-0: `PoolStats::evictions`); the test-only evictor's do not count.
    evictions: u64,
}

/// The next fresh slot at or after `s`, never a reserved one (§2.4: 256, 257
/// at 4 KiB).
fn fresh_slot(s: u64) -> u64 {
    if (256..258).contains(&s) { 258 } else { s }
}

/// A key as a sortable pair: its namespace, then its number.
fn key_order(k: PageKey) -> (u8, u64) {
    match k {
        PageKey::Lpid(x) => (0, x),
        PageKey::MapLeaf(i) => (1, i),
    }
}

impl Model {
    fn free(&self) -> usize {
        self.frames as usize - self.res.len()
    }

    fn entry(&self, k: PageKey) -> Option<u64> {
        self.entry.get(&k).copied().flatten()
    }

    /// Whether `op` needs a frame the pool has none free for, so that the
    /// pool's frame acquisition may evict for it (card A-3b-0, Work 11): a
    /// pin of a non-resident key, or a creation, on a full pool.
    fn needs_frame(&self, op: Op) -> bool {
        self.free() == 0
            && match op {
                Op::Pin(k) => !self.res.contains_key(&k),
                Op::Create(_) => true,
                _ => false,
            }
    }

    /// `E`: the resident pages with no pin that are not dirty (a freed page
    /// is clean), in a fixed order: the ones a full pool's frame acquisition
    /// may evict.
    fn evictable(&self) -> Vec<PageKey> {
        let mut e: Vec<PageKey> = self
            .res
            .iter()
            .filter(|(_, r)| r.pins == 0 && !r.dirty)
            .map(|(k, _)| *k)
            .collect();
        e.sort_by_key(|k| key_order(*k));
        e
    }

    /// Whether a single thread driving `op` would wait for an unpin (card
    /// A-3b-0, Work 11): the op needs a frame, none is evictable and every
    /// frame is held. Such an op is not driven.
    fn would_wait(&self, op: Op) -> bool {
        self.needs_frame(op) && self.evictable().is_empty() && self.res.values().all(|r| r.pins > 0)
    }

    /// The frame acquisition's part of an op that needs a frame from a full
    /// pool (card A-3b-0, Work 11): `evicted` is the one page the pool took
    /// out of residency for it, as the driver observed it, or `None`. A page
    /// is taken only if it is in `E`; with `E` empty nothing is taken. `Ok`
    /// says whether a frame was had; `Err` names a disagreement.
    fn acquire(&mut self, evicted: Option<PageKey>) -> Result<bool, String> {
        let e = self.evictable();
        match evicted {
            Some(v) => {
                if !e.contains(&v) {
                    return Err(format!(
                        "the pool evicted {v:?}, which is not a clean, unpinned page (it may evict {e:?})"
                    ));
                }
                self.res.remove(&v);
                self.evictions += 1;
                Ok(true)
            }
            None => {
                if !e.is_empty() {
                    return Err(format!("the pool evicted nothing, with {e:?} to evict"));
                }
                Ok(false)
            }
        }
    }

    /// Predict `op`'s outcome. For an op that needs a frame from a full pool
    /// `evicted` is the page the pool evicted first (card A-3b-0, Work 11);
    /// `None` for every other op.
    fn apply(&mut self, op: Op, evicted: Option<PageKey>) -> Result<Outcome, String> {
        Ok(match op {
            Op::Pin(k) => {
                if let Some(r) = self.res.get_mut(&k) {
                    r.pins += 1;
                    self.held.push(k);
                    return Ok(Outcome::Pinned(r.content));
                }
                if self.free() == 0 && !self.acquire(evicted)? {
                    // Nothing is evictable. A frame no pin holds is dirty
                    // all the way: starved. Otherwise every frame is held,
                    // and the op was not driven.
                    return Ok(if self.res.values().any(|r| r.pins == 0) {
                        Outcome::Starved
                    } else {
                        Outcome::Skipped
                    });
                }
                let Some(slot) = self.entry(k) else {
                    return Ok(Outcome::Unmapped);
                };
                self.reads += 1;
                let (content, corrupt) = self.disk.get(&slot).copied().unwrap_or((0, true));
                if corrupt {
                    return Ok(Outcome::Corrupt);
                }
                self.res.insert(
                    k,
                    Res {
                        content,
                        dirty: false,
                        freed: false,
                        pins: 1,
                    },
                );
                self.held.push(k);
                Outcome::Pinned(content)
            }
            Op::PinResident(k) => match self.res.get_mut(&k) {
                Some(r) => {
                    r.pins += 1;
                    self.held.push(k);
                    Outcome::Pinned(r.content)
                }
                None => Outcome::NotResident,
            },
            Op::Drop(i) => {
                if self.held.is_empty() {
                    return Ok(Outcome::Skipped);
                }
                let k = self.held.remove(i % self.held.len());
                if let Some(r) = self.res.get_mut(&k) {
                    r.pins -= 1;
                }
                Outcome::Dropped
            }
            Op::Write(i, v) => {
                if self.held.is_empty() {
                    return Ok(Outcome::Skipped);
                }
                let k = self.held[i % self.held.len()];
                let Some(r) = self.res.get_mut(&k) else {
                    return Ok(Outcome::Skipped);
                };
                if r.freed {
                    return Ok(Outcome::Invalid);
                }
                r.content = v;
                r.dirty = true;
                Outcome::Written
            }
            Op::Evict(k) => {
                let Some(r) = self.res.get(&k).copied() else {
                    return Ok(Outcome::NoVictim);
                };
                if r.pins > 0 {
                    return Ok(Outcome::NoVictim);
                }
                if r.dirty {
                    let slot = fresh_slot(self.next_slot);
                    self.disk.insert(slot, (r.content, false));
                    self.entry.insert(k, Some(slot));
                    self.next_slot = slot + 1;
                }
                self.res.remove(&k);
                Outcome::Removed
            }
            Op::DropClean(k) => {
                let Some(r) = self.res.get(&k).copied() else {
                    return Ok(Outcome::NoVictim);
                };
                if r.pins > 0 {
                    return Ok(Outcome::NoVictim);
                }
                if r.dirty {
                    return Ok(Outcome::Kept);
                }
                self.res.remove(&k);
                Outcome::Removed
            }
            Op::Create(k) => {
                if self.free() == 0 && !self.acquire(evicted)? {
                    return Ok(if self.res.values().any(|r| r.pins == 0) {
                        Outcome::Starved
                    } else {
                        Outcome::Skipped
                    });
                }
                // As with a free frame. The creation's own reservation may
                // have evicted `k` itself, so a freed `k` is then made on the
                // fresh frame, and otherwise in its own, the reservation
                // returning to the free list.
                match self.res.get_mut(&k) {
                    Some(r) if r.freed => {
                        *r = Res {
                            content: 0,
                            dirty: true,
                            freed: false,
                            pins: r.pins + 1,
                        };
                        self.held.push(k);
                        Outcome::Created
                    }
                    Some(r) => {
                        r.pins += 1;
                        self.held.push(k);
                        Outcome::Exists
                    }
                    None => {
                        self.res.insert(
                            k,
                            Res {
                                content: 0,
                                dirty: true,
                                freed: false,
                                pins: 1,
                            },
                        );
                        self.held.push(k);
                        Outcome::Created
                    }
                }
            }
            Op::Free(i) => {
                if self.held.is_empty() {
                    return Ok(Outcome::Skipped);
                }
                let k = self.held[i % self.held.len()];
                self.entry.insert(k, None);
                if let Some(r) = self.res.get_mut(&k) {
                    r.freed = true;
                    r.dirty = false;
                }
                Outcome::Freed
            }
            Op::Corrupt(k) => match self.entry(k) {
                None => Outcome::Skipped,
                Some(slot) => {
                    if let Some(d) = self.disk.get_mut(&slot) {
                        d.1 = true;
                    }
                    Outcome::Spoiled
                }
            },
        })
    }
}

/// The kind a key's pages have.
fn kind_of(k: PageKey) -> Kind {
    match k {
        PageKey::Lpid(_) => Kind::Leaf,
        PageKey::MapLeaf(_) => Kind::MapLeaf,
    }
}

/// The pool side: the pool, its file and map, the pins the test holds (in the
/// model's order), the evictor's next fresh slot and, from the last
/// creation, whether its reservation evicted the created key's own page.
struct Driver<'p> {
    file: &'p FaultFile,
    map: &'p TestMap,
    pool: &'p Pool<FaultFile>,
    held: Vec<Pin<'p, FaultFile>>,
    next_slot: u64,
    /// Set by `Create` (card A-3b-0, Work 11).
    reserve_evicted: Option<PageKey>,
}

/// A store error as text.
fn text(e: StoreError) -> String {
    e.to_string()
}

impl<'p> Driver<'p> {
    fn drive(&mut self, op: Op) -> Result<Outcome, String> {
        let pool = self.pool;
        Ok(match op {
            Op::Pin(k) => match pool.pin(k, self.map) {
                Ok(PinOutcome::Pinned(p)) => {
                    let m = marker(&p).map_err(text)?;
                    self.held.push(p);
                    Outcome::Pinned(m)
                }
                Ok(PinOutcome::Unmapped) => Outcome::Unmapped,
                Err(StoreError::PoolStarved) => Outcome::Starved,
                Err(StoreError::Corrupt(_)) => Outcome::Corrupt,
                Err(e) => return Err(format!("pin: {e}")),
            },
            Op::PinResident(k) => match pool.pin_resident(k).map_err(text)? {
                Some(p) => {
                    let m = marker(&p).map_err(text)?;
                    self.held.push(p);
                    Outcome::Pinned(m)
                }
                None => Outcome::NotResident,
            },
            Op::Drop(i) => {
                if self.held.is_empty() {
                    return Ok(Outcome::Skipped);
                }
                let n = self.held.len();
                drop(self.held.remove(i % n));
                Outcome::Dropped
            }
            Op::Write(i, v) => {
                if self.held.is_empty() {
                    return Ok(Outcome::Skipped);
                }
                let n = self.held.len();
                let mut g = self.held[i % n].latch_exclusive().map_err(text)?;
                match g.mark_dirty(1) {
                    Ok(()) => {
                        g[MARK] = v;
                        Outcome::Written
                    }
                    Err(StoreError::Invalid(_)) => Outcome::Invalid,
                    Err(e) => return Err(format!("mark_dirty: {e}")),
                }
            }
            Op::Evict(k) => {
                let Some(f) = pool.frame_of(k).map_err(text)? else {
                    return Ok(Outcome::NoVictim);
                };
                let Some(mut v) = pool.try_victim(f).map_err(text)? else {
                    return Ok(Outcome::NoVictim);
                };
                if v.is_dirty() {
                    let slot = fresh_slot(self.next_slot);
                    let crc = page::seal(&mut v).map_err(text)?;
                    self.file
                        .write_at(offset(slot), &v)
                        .map_err(|e| e.to_string())?;
                    let home = Ref {
                        slot,
                        epoch: v.content_epoch(),
                        crc,
                    };
                    self.map.set(k, home);
                    v.mark_written(home).map_err(text)?;
                    self.next_slot = slot + 1;
                }
                match v.remove_from_table().map_err(text)? {
                    Removal::Removed(res) => {
                        drop(res);
                        Outcome::Removed
                    }
                    Removal::Kept => Outcome::Kept,
                }
            }
            Op::DropClean(k) => {
                let Some(f) = pool.frame_of(k).map_err(text)? else {
                    return Ok(Outcome::NoVictim);
                };
                let Some(v) = pool.try_victim(f).map_err(text)? else {
                    return Ok(Outcome::NoVictim);
                };
                match v.remove_from_table().map_err(text)? {
                    Removal::Removed(res) => {
                        drop(res);
                        Outcome::Removed
                    }
                    Removal::Kept => Outcome::Kept,
                }
            }
            Op::Create(k) => {
                // Card A-3b-0, Work 11: on a full pool the creation's own
                // `reserve` may evict `k`'s page, freed and unpinned. It is
                // resident again once the op ends, made on the fresh frame,
                // so only a look between the two calls sees that eviction.
                self.reserve_evicted = None;
                let had = pool.frame_of(k).map_err(text)?.is_some();
                let res = match pool.reserve() {
                    Ok(res) => res,
                    Err(StoreError::PoolStarved) => return Ok(Outcome::Starved),
                    Err(e) => return Err(format!("reserve: {e}")),
                };
                if had && pool.frame_of(k).map_err(text)?.is_none() {
                    self.reserve_evicted = Some(k);
                }
                match pool.create(res, k, kind_of(k), 1).map_err(text)? {
                    CreateOutcome::Created(c) => {
                        self.held.push(c.release());
                        Outcome::Created
                    }
                    CreateOutcome::Exists(p, r) => {
                        drop(r);
                        self.held.push(p);
                        Outcome::Exists
                    }
                    CreateOutcome::Loading(_) => {
                        return Err("a single-threaded create found a load".into());
                    }
                }
            }
            Op::Free(i) => {
                if self.held.is_empty() {
                    return Ok(Outcome::Skipped);
                }
                let n = self.held.len();
                let p = &self.held[i % n];
                let mut g = p.latch_exclusive().map_err(text)?;
                self.map.clear(p.key());
                g.mark_freed().map_err(text)?;
                Outcome::Freed
            }
            Op::Corrupt(k) => {
                let r = self.map.get(k);
                if r.slot == 0 {
                    return Ok(Outcome::Skipped);
                }
                self.file
                    .write_at(offset(r.slot) + SPOIL as u64, &[0xFF])
                    .map_err(|e| e.to_string())?;
                Outcome::Spoiled
            }
        })
    }
}

/// After an op: the stats (every field, no `..`), each key's residency and
/// frame bookkeeping (every field, no `..`), and every held pin's page. The
/// clock's position and each frame's reference bit are the frame-level
/// choices this oracle does not model (card A-3b-0, Work 11): `buf_evict`'s
/// `clock_order` pins them.
fn compare(model: &Model, d: &Driver<'_>, keys: &[PageKey], buckets0: u32) -> Result<(), String> {
    let PoolStats {
        frames,
        free,
        reserved,
        loading,
        resident,
        orphan,
        reads,
        buckets,
        evictions,
        clock_hand: _,
        unpin_waiters,
    } = d.pool.stats().map_err(text)?;
    let want = (
        model.frames,
        model.free() as u32,
        0,
        0,
        model.res.len() as u32,
        0,
        model.reads,
        buckets0,
        model.evictions,
        0,
    );
    let got = (
        frames,
        free,
        reserved,
        loading,
        resident,
        orphan,
        reads,
        buckets,
        evictions,
        unpin_waiters,
    );
    if got != want {
        return Err(format!("stats {got:?}, model {want:?}"));
    }
    if d.map.locked_lookups() != 0 {
        return Err(format!(
            "{} reference reads under the table mutex",
            d.map.locked_lookups()
        ));
    }
    if reads != d.file.reads() as u64 {
        return Err(format!("stats.reads {reads}, file log {}", d.file.reads()));
    }
    for &k in keys {
        let f = d.pool.frame_of(k).map_err(text)?;
        let r = model.res.get(&k);
        if f.is_some() != r.is_some() {
            return Err(format!("{k:?}: frame {f:?}, model {r:?}"));
        }
        if let (Some(f), Some(r)) = (f, r) {
            let FrameInfo {
                state,
                key,
                pins,
                freed,
                referenced: _,
            } = d.pool.frame_info(f).map_err(text)?;
            let want = (FrameState::Resident, Some(k), r.pins, r.freed);
            if (state, key, pins, freed) != want {
                return Err(format!(
                    "{k:?}: {:?}, model {want:?}",
                    (state, key, pins, freed)
                ));
            }
        }
    }
    if d.held.len() != model.held.len() {
        return Err(format!("{} pins, model {}", d.held.len(), model.held.len()));
    }
    for (p, &k) in d.held.iter().zip(&model.held) {
        let r = model.res.get(&k).copied();
        let g = p.latch_shared().map_err(text)?;
        let got = (
            p.key(),
            Some(g[MARK]),
            Some(g.is_dirty()),
            Some(g.is_freed()),
        );
        let want = (
            k,
            r.map(|r| r.content),
            r.map(|r| r.dirty),
            r.map(|r| r.freed),
        );
        if got != want {
            return Err(format!("held pin {got:?}, model {want:?}"));
        }
    }
    Ok(())
}

/// A page of the fixture, built once: its key, slot, bytes and reference.
struct Fixed {
    key: PageKey,
    slot: u64,
    bytes: Vec<u8>,
    r: Ref,
    marker: u8,
}

/// Build `(key, slot, marker)` pages, epoch 1.
fn fixed(pages: &[(PageKey, u64, u8)]) -> Result<Vec<Fixed>, StoreError> {
    pages
        .iter()
        .map(|&(key, slot, marker)| {
            let (bytes, crc) = page(kind_of(key), key.header_id()?, 1, marker)?;
            Ok(Fixed {
                key,
                slot,
                bytes,
                r: Ref {
                    slot,
                    epoch: 1,
                    crc,
                },
                marker,
            })
        })
        .collect()
}

/// The runs' page file. A fresh `MemFile` zero-fills every byte below slot
/// 10 one at a time in a debug build, which dominated 22,620 runs; so the
/// file is made once and, before every run, reset to the fixture's image
/// (the pages at their slots, zeros elsewhere) and checked against it byte
/// for byte through a handle of its own. Each run then starts from exactly
/// the bytes a fresh file would hold, through a fresh `FaultFile` handle with
/// an empty log, a fresh pool and a fresh test map.
struct Disk {
    env: MemEnv,
    raw: MemFile,
    image: Vec<u8>,
}

impl Disk {
    fn new(pages: &[Fixed]) -> Result<Disk, String> {
        let env = MemEnv::new(1);
        let raw = env
            .open("data", OpenMode::CreateNew)
            .map_err(|e| e.to_string())?;
        let len = pages.iter().map(|p| offset(p.slot + 1)).max().unwrap_or(0);
        let mut image = vec![0u8; len as usize];
        for p in pages {
            let at = offset(p.slot) as usize;
            image[at..at + p.bytes.len()].copy_from_slice(&p.bytes);
        }
        Ok(Disk { env, raw, image })
    }

    /// A `FaultFile` with an empty log over the file, reset to the image.
    fn fresh(&self) -> Result<Arc<FaultFile>, String> {
        let text = |e: EnvError| e.to_string();
        let len = self.image.len() as u64;
        self.raw.set_len(len).map_err(text)?;
        self.raw.write_at(0, &self.image).map_err(text)?;
        let mut back = vec![0u8; self.image.len()];
        let n = self.raw.read_at(0, &mut back).map_err(text)?;
        if n != self.image.len() || back != self.image || self.raw.len().map_err(text)? != len {
            return Err("the reset page file differs from the fixture's image".into());
        }
        let handle = self.env.open("data", OpenMode::ReadWrite).map_err(text)?;
        Ok(Arc::new(FaultFile::over(handle)))
    }
}

/// Which of `keys` the page table holds, by `frame_of`, in `keys`' order.
fn resident(pool: &Pool<FaultFile>, keys: &[PageKey]) -> Result<Vec<bool>, String> {
    keys.iter()
        .map(|k| pool.frame_of(*k).map(|f| f.is_some()).map_err(text))
        .collect()
}

/// Run `ops` on a fresh fixture of `frames` frames holding `pages` (on
/// `disk`, reset), comparing the pool with the model after every op; `keys`
/// are every key of the run.
fn run(
    disk: &Disk,
    frames: u32,
    pages: &[Fixed],
    keys: &[PageKey],
    ops: &[Op],
) -> Result<(), String> {
    let file = disk.fresh()?;
    // Card A-3b-0 (Work 11): these pools are below §5.4's minimum, so they
    // open through the test-only constructor; A-3a's fixture is otherwise
    // kept.
    let pool = Pool::new_below_minimum_for_tests(
        Arc::clone(&file),
        PoolConfig {
            frames,
            page_size: PS,
            callers: 1,
        },
    )
    .map_err(text)?;
    // Card A-3b-0: no op the model drives may reach the wait for an unpin
    // (it leaves those undriven). One that does fails here, in the hook,
    // instead of holding the only thread.
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_no_wait()));
    let map = TestMap::new();
    let mut model = Model {
        frames,
        disk: HashMap::new(),
        entry: keys.iter().map(|&k| (k, None)).collect(),
        res: HashMap::new(),
        held: Vec::new(),
        reads: 0,
        next_slot: 40,
        evictions: 0,
    };
    for p in pages {
        map.set(p.key, p.r);
        model.disk.insert(p.slot, (p.marker, false));
        model.entry.insert(p.key, Some(p.slot));
    }
    let buckets0 = pool.stats().map_err(text)?.buckets;
    let mut d = Driver {
        file: &file,
        map: &map,
        pool: &pool,
        held: Vec::new(),
        next_slot: 40,
        reserve_evicted: None,
    };
    for (i, &op) in ops.iter().enumerate() {
        let at = |what: String| format!("op {i} {op:?}: {what}");
        // An op that needs a frame from a full pool evicts first: the pool
        // chooses the page, and the driver names it (card A-3b-0, Work 11).
        let full = model.needs_frame(op);
        let before = full.then(|| resident(&pool, keys)).transpose()?;
        let logged = log.len();
        d.reserve_evicted = None;
        let got = if model.would_wait(op) {
            // Every frame is held: not driven.
            None
        } else {
            Some(d.drive(op).map_err(at)?)
        };
        let evicted = match before {
            Some(b) => {
                let after = resident(&pool, keys)?;
                let mut gone: Vec<PageKey> = keys
                    .iter()
                    .zip(&b)
                    .zip(&after)
                    .filter(|((_, b), a)| **b && !**a)
                    .map(|((k, _), _)| *k)
                    .collect();
                // A creation whose reservation evicted its own key: that page
                // is resident again already.
                if gone.is_empty()
                    && let Some(k) = d.reserve_evicted
                {
                    gone.push(k);
                }
                if gone.len() > 1 {
                    return Err(at(format!("the pool evicted {gone:?}")));
                }
                gone.first().copied()
            }
            None => None,
        };
        if full {
            // The pool's own account of the eviction agrees: one `Evicted`
            // for the page the driver named, none when it named none.
            let fired: Vec<PageKey> = log
                .since(logged, HookPoint::Evicted)
                .into_iter()
                .map(|(k, _)| k)
                .collect();
            if fired != evicted.into_iter().collect::<Vec<_>>() {
                return Err(at(format!(
                    "the pool reports evicting {fired:?}, the driver saw {evicted:?}"
                )));
            }
        }
        let want = model.apply(op, evicted).map_err(at)?;
        match (got, want) {
            (None, Outcome::Skipped) => {}
            (Some(got), want) if got == want => {}
            (got, want) => return Err(at(format!("pool {got:?}, model {want:?}"))),
        }
        compare(&model, &d, keys, buckets0).map_err(at)?;
    }
    Ok(())
}

#[test]
fn buf_model_exhaustive() {
    // F 2; L1 at slot 10 (marker 1), L2 at slot 11 (marker 2), L4 unmapped.
    let pages = fixed(&[(L1, 10, 1), (L2, 11, 2)]).unwrap();
    let disk = Disk::new(&pages).unwrap();
    let keys = [L1, L2, L4];
    // The alphabet; `Write`'s marker is set per sequence, 0x40 up.
    let alphabet = [
        Op::Pin(L1),
        Op::Pin(L2),
        Op::Pin(L4),
        Op::PinResident(L1),
        Op::Drop(0),
        Op::Write(0, 0),
        Op::Evict(L1),
        Op::Evict(L2),
        Op::DropClean(L2),
        Op::Create(L4),
        Op::Free(0),
        Op::Corrupt(L2),
    ];
    let n = alphabet.len();
    let mut runs = 0u32;
    for len in 1..=4u32 {
        for code in 0..n.pow(len) {
            let mut c = code;
            let mut writes = 0u8;
            let ops: Vec<Op> = (0..len)
                .map(|_| {
                    let op = alphabet[c % n];
                    c /= n;
                    match op {
                        Op::Write(i, _) => {
                            writes += 1;
                            Op::Write(i, 0x3F + writes)
                        }
                        other => other,
                    }
                })
                .collect();
            if let Err(e) = run(&disk, 2, &pages, &keys, &ops) {
                panic!("{ops:?}: {e}");
            }
            runs += 1;
        }
    }
    assert_eq!(runs, 22_620);
}

/// A key of the random runs.
fn key() -> impl Strategy<Value = PageKey> {
    prop_oneof![Just(L1), Just(L2), Just(L3), Just(L4), Just(M0)]
}

/// An op of the random runs: every kind, random keys, indices and markers.
fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        key().prop_map(Op::Pin),
        key().prop_map(Op::PinResident),
        (0usize..8).prop_map(Op::Drop),
        (0usize..8, any::<u8>()).prop_map(|(i, v)| Op::Write(i, v)),
        key().prop_map(Op::Evict),
        key().prop_map(Op::DropClean),
        key().prop_map(Op::Create),
        (0usize..8).prop_map(Op::Free),
        key().prop_map(Op::Corrupt),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        rng_seed: RngSeed::Fixed(0x0A3A_5EED),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    #[test]
    fn buf_model_random(frames in 1u32..=3, ops in prop::collection::vec(op(), 1..=48)) {
        // L1, L2, L3 at slots 10–12, M0 at slot 13, L4 unmapped.
        let pages = fixed(&[(L1, 10, 1), (L2, 11, 2), (L3, 12, 3), (M0, 13, 0x30)]).unwrap();
        let disk = Disk::new(&pages).unwrap();
        run(&disk, frames, &pages, &[L1, L2, L3, L4, M0], &ops).map_err(TestCaseError::fail)?;
    }
}

/// The events of the transition table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ev {
    Reserve,
    DropReservation,
    PinPublishes,
    LoadSucceeds,
    RefSlotZero,
    LoadFails,
    LoadFailsWithWaiter,
    WaiterDrops,
    SecondPin,
    LoadSucceedsWithWaiter,
    PinOrPinResident,
    PinDropped,
    CreateAbsent,
    CreateLoading,
    CreateFreed,
    CreateLive,
    TryVictimUnpinned,
    TryVictimPinned,
    RemoveClean,
    RemoveDirty,
    RemovePinnedAtLatch,
    MarkDirty,
    MarkWritten,
    MarkFreed,
    TryVictimAndPinFrame,
    /// Card A-3b-0: `try_reserve` on a full pool whose every frame but one
    /// is held; two rows, the clean frame and the dirty one.
    TryReserve,
}

/// Card A-3a's transition table (Work 13), with card A-3b-0's two
/// `try_reserve` rows (Work 11): one row per transition, with the from-state
/// each row builds. "(miss)" is the Free frame a miss takes.
const ROWS: &[(FrameState, Ev)] = &[
    (FrameState::Free, Ev::Reserve),
    (FrameState::Reserved, Ev::DropReservation),
    (FrameState::Free, Ev::PinPublishes),
    (FrameState::Loading, Ev::LoadSucceeds),
    (FrameState::Loading, Ev::RefSlotZero),
    (FrameState::Loading, Ev::LoadFails),
    (FrameState::Loading, Ev::LoadFailsWithWaiter),
    (FrameState::Orphan, Ev::WaiterDrops),
    (FrameState::Loading, Ev::SecondPin),
    (FrameState::Loading, Ev::LoadSucceedsWithWaiter),
    (FrameState::Resident, Ev::PinOrPinResident),
    (FrameState::Resident, Ev::PinDropped),
    (FrameState::Reserved, Ev::CreateAbsent),
    (FrameState::Reserved, Ev::CreateLoading),
    (FrameState::Reserved, Ev::CreateFreed),
    (FrameState::Reserved, Ev::CreateLive),
    (FrameState::Resident, Ev::TryVictimUnpinned),
    (FrameState::Resident, Ev::TryVictimPinned),
    (FrameState::Resident, Ev::RemoveClean),
    (FrameState::Resident, Ev::RemoveDirty),
    (FrameState::Resident, Ev::RemovePinnedAtLatch),
    (FrameState::Resident, Ev::MarkDirty),
    (FrameState::Resident, Ev::MarkWritten),
    (FrameState::Resident, Ev::MarkFreed),
    (FrameState::Free, Ev::TryVictimAndPinFrame),
    (FrameState::Reserved, Ev::TryVictimAndPinFrame),
    (FrameState::Loading, Ev::TryVictimAndPinFrame),
    (FrameState::Orphan, Ev::TryVictimAndPinFrame),
    // Card A-3b-0, Work 11: "Resident, count 0, clean, every other frame
    // held | try_reserve | Reserved, Evicted fired (the first rotation clears
    // its bit, the second takes it); Free once the reservation drops", then
    // "Resident, count 0, dirty, every other frame held | try_reserve |
    // unchanged, None". The row body tells them apart by their order here.
    (FrameState::Resident, Ev::TryReserve),
    (FrameState::Resident, Ev::TryReserve),
];

/// The pool's frames by state: free, reserved, loading, resident, orphan.
fn states(pool: &Pool<FaultFile>) -> (u32, u32, u32, u32, u32) {
    match pool.stats() {
        Ok(s) => (s.free, s.reserved, s.loading, s.resident, s.orphan),
        Err(_) => (u32::MAX, 0, 0, 0, 0),
    }
}

/// A frame's bookkeeping, for the table's expectations. Card A-3b-0: every
/// `Resident` frame of these rows holds its count from a user pin, so it
/// carries the clock's bit; no other state does.
fn info(state: FrameState, key: Option<PageKey>, pins: u64, freed: bool) -> FrameInfo {
    FrameInfo {
        state,
        key,
        pins,
        freed,
        referenced: state == FrameState::Resident,
    }
}

#[test]
fn transition_table() {
    use FrameState::{Free, Loading, Orphan, Reserved, Resident};

    // The compile-time guards: no wildcard arm, so a state or an event added
    // later fails to compile here until the table names it.
    let state_slot = |s: FrameState| match s {
        Free => 0,
        Reserved => 1,
        Loading => 2,
        Resident => 3,
        Orphan => 4,
    };
    let ev_slot = |e: Ev| match e {
        Ev::Reserve => 0,
        Ev::DropReservation => 1,
        Ev::PinPublishes => 2,
        Ev::LoadSucceeds => 3,
        Ev::RefSlotZero => 4,
        Ev::LoadFails => 5,
        Ev::LoadFailsWithWaiter => 6,
        Ev::WaiterDrops => 7,
        Ev::SecondPin => 8,
        Ev::LoadSucceedsWithWaiter => 9,
        Ev::PinOrPinResident => 10,
        Ev::PinDropped => 11,
        Ev::CreateAbsent => 12,
        Ev::CreateLoading => 13,
        Ev::CreateFreed => 14,
        Ev::CreateLive => 15,
        Ev::TryVictimUnpinned => 16,
        Ev::TryVictimPinned => 17,
        Ev::RemoveClean => 18,
        Ev::RemoveDirty => 19,
        Ev::RemovePinnedAtLatch => 20,
        Ev::MarkDirty => 21,
        Ev::MarkWritten => 22,
        Ev::MarkFreed => 23,
        Ev::TryVictimAndPinFrame => 24,
        Ev::TryReserve => 25,
    };
    let mut per_state = [0u32; 5];
    let mut per_event = [false; 26];
    for &(from, ev) in ROWS {
        per_state[state_slot(from)] += 1;
        per_event[ev_slot(ev)] = true;
    }
    assert!(
        per_state.iter().all(|&n| n > 0),
        "rows per state {per_state:?}"
    );
    assert!(per_event.iter().all(|&b| b), "events {per_event:?}");

    let f0 = FrameId(0);
    let fi = |pool: &Pool<FaultFile>, f: FrameId| pool.frame_info(f).unwrap();
    // L1 at slot 10 (marker 0x11), optionally with byte 200 spoiled.
    let with_l1 = |frames: u32, spoiled: bool| {
        let (file, map, pool) = fixture(frames).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        if spoiled {
            file.write_at(offset(10) + SPOIL as u64, &[0xFF]).unwrap();
        }
        (file, map, pool)
    };
    let pin_l1 = |pool: &Pool<FaultFile>, map: &TestMap| match pool.pin(L1, map) {
        Ok(PinOutcome::Pinned(p)) => p.frame(),
        other => panic!("expected Pinned, got {other:?}"),
    };
    // Card A-3b-0: which of the two `try_reserve` rows is being run.
    let mut try_reserve_rows = 0u32;

    for &(from, ev) in ROWS {
        let row = format!("{from:?} --{ev:?}-->");
        match (from, ev) {
            (Free, Ev::Reserve) => {
                let (_f, _m, pool) = fixture(2).unwrap();
                assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
                let res = pool.reserve().unwrap();
                assert_eq!(res.frame(), f0, "{row}");
                assert_eq!(fi(&pool, f0), info(Reserved, None, 0, false), "{row}");
            }
            (Reserved, Ev::DropReservation) => {
                let (_f, _m, pool) = fixture(2).unwrap();
                let res = pool.reserve().unwrap();
                assert_eq!(fi(&pool, f0), info(Reserved, None, 0, false), "{row}");
                drop(res);
                assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
            }
            (Free, Ev::PinPublishes)
            | (Loading, Ev::LoadSucceeds)
            | (Loading, Ev::RefSlotZero)
            | (Loading, Ev::LoadFails)
            | (Loading, Ev::TryVictimAndPinFrame) => {
                // T1's load paused at LoadPublished: T1 holds its count on the
                // Loading frame; no latch, no mutex.
                let (_file, map, pool) = with_l1(2, ev == Ev::LoadFails);
                let key = if ev == Ev::RefSlotZero {
                    PageKey::Lpid(9)
                } else {
                    L1
                };
                assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
                let pz = Pauser::new();
                pool.set_hook(Some(pz.hook()));
                pz.pause_at(HookPoint::LoadPublished, key);
                let (first, at_pause, probes, result) = thread::scope(|s| {
                    let t1 = spawn(s, &pz, "T1", || {
                        pool.pin(key, &map).map(|o| match o {
                            PinOutcome::Pinned(p) => Some(p),
                            PinOutcome::Unmapped => None,
                        })
                    });
                    let first = pz.next_event();
                    let (mut at_pause, mut probes) = (None, None);
                    if first == Event::Arrived(HookPoint::LoadPublished, key, f0) {
                        at_pause = Some((
                            pool.frame_info(f0).unwrap(),
                            pool.frame_of(key).unwrap(),
                            states(&pool),
                        ));
                        if ev == Ev::TryVictimAndPinFrame {
                            probes = Some((
                                pool.try_victim(f0).unwrap().is_none(),
                                pool.pin_frame(f0).unwrap().is_none(),
                                pool.frame_info(f0).unwrap(),
                            ));
                        }
                    }
                    pz.release_all();
                    (first, at_pause, probes, t1.join().unwrap())
                });
                assert_eq!(
                    first,
                    Event::Arrived(HookPoint::LoadPublished, key, f0),
                    "{row}"
                );
                // Free, reserved, loading, resident, orphan.
                assert_eq!(
                    at_pause,
                    Some((
                        info(Loading, Some(key), 1, false),
                        Some(f0),
                        (1, 0, 1, 0, 0)
                    )),
                    "{row}"
                );
                match ev {
                    Ev::PinPublishes | Ev::LoadSucceeds => {
                        let p = result.unwrap().unwrap();
                        assert_eq!(fi(&pool, f0), info(Resident, Some(L1), 1, false), "{row}");
                        let g = p.latch_shared().unwrap();
                        assert!(!g.is_dirty() && !g.is_freed(), "{row}");
                        assert_eq!(g.content_epoch(), 1, "{row}");
                    }
                    Ev::RefSlotZero => {
                        assert!(result.unwrap().is_none(), "{row}");
                        assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
                    }
                    Ev::LoadFails => {
                        assert!(matches!(result, Err(StoreError::Corrupt(_))), "{row}");
                        assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
                    }
                    _ => {
                        assert_eq!(
                            probes,
                            Some((true, true, info(Loading, Some(L1), 1, false))),
                            "{row}"
                        );
                        drop(result.unwrap());
                    }
                }
            }
            (Loading, Ev::LoadFailsWithWaiter)
            | (Orphan, Ev::WaiterDrops)
            | (Orphan, Ev::TryVictimAndPinFrame) => {
                // T1 paused at LoadRead (its count and the frame's exclusive
                // latch); T2 waiting (its count); then T2 paused at WaitWoken
                // (its count; no latch, no mutex).
                let (file, map, pool) = with_l1(2, true);
                let pz = Pauser::new();
                pool.set_hook(Some(pz.hook()));
                pz.pause_at(HookPoint::LoadRead, L1);
                pz.pause_at(HookPoint::WaitWoken, L1);
                pz.report(HookPoint::LoadWaiting, L1);
                let pinned = |p: Result<PinOutcome<'_, FaultFile>, StoreError>| p.map(|_| ());
                let (events, waiting, woken, probes, r1, r2) = thread::scope(|s| {
                    let t1 = spawn(s, &pz, "T1", || pinned(pool.pin(L1, &map)));
                    let mut events = vec![pz.next_event()];
                    let (mut waiting, mut woken, mut probes, mut t2) = (None, None, None, None);
                    if events[0] == Event::Arrived(HookPoint::LoadRead, L1, f0) {
                        t2 = Some(spawn(s, &pz, "T2", || pinned(pool.pin(L1, &map))));
                        events.push(pz.next_event());
                        if events[1] == Event::Seen(HookPoint::LoadWaiting, L1, f0) {
                            waiting = Some(pool.frame_info(f0).unwrap());
                            pz.release(HookPoint::LoadRead, L1);
                            let mut t1_done = false;
                            let mut at_woken = false;
                            while !(t1_done && at_woken) {
                                let e = pz.next_event();
                                events.push(e);
                                if e == Event::Done("T1") {
                                    t1_done = true;
                                } else if e == Event::Arrived(HookPoint::WaitWoken, L1, f0) {
                                    at_woken = true;
                                } else {
                                    break;
                                }
                            }
                            if t1_done && at_woken {
                                woken = Some((
                                    pool.frame_info(f0).unwrap(),
                                    pool.frame_of(L1).unwrap(),
                                    states(&pool),
                                ));
                                if ev == Ev::TryVictimAndPinFrame {
                                    probes = Some((
                                        pool.try_victim(f0).unwrap().is_none(),
                                        pool.pin_frame(f0).unwrap().is_none(),
                                        pool.frame_info(f0).unwrap(),
                                    ));
                                }
                            }
                        }
                    }
                    pz.release_all();
                    (
                        events,
                        waiting,
                        woken,
                        probes,
                        t1.join().unwrap(),
                        t2.map(|h| h.join().unwrap()),
                    )
                });
                assert_eq!(
                    waiting,
                    Some(info(Loading, Some(L1), 2, false)),
                    "{row} {events:?}"
                );
                assert_eq!(
                    woken,
                    Some((info(Orphan, Some(L1), 1, false), None, (1, 0, 0, 0, 1))),
                    "{row} {events:?}"
                );
                assert!(matches!(r1, Err(StoreError::Corrupt(_))), "{row}");
                assert!(matches!(r2, Some(Err(StoreError::Corrupt(_)))), "{row}");
                if ev == Ev::TryVictimAndPinFrame {
                    assert_eq!(
                        probes,
                        Some((true, true, info(Orphan, Some(L1), 1, false))),
                        "{row}"
                    );
                }
                // The waiter dropped its count: the frame is Free (and T2's
                // own retry, which failed too, left it Free).
                assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
                let st = pool.stats().unwrap();
                assert_eq!((st.free, st.orphan), (2, 0), "{row}");
                assert_eq!(file.reads(), 2, "{row}");
            }
            (Loading, Ev::SecondPin) | (Loading, Ev::LoadSucceedsWithWaiter) => {
                let (_file, map, pool) = with_l1(2, false);
                let pz = Pauser::new();
                pool.set_hook(Some(pz.hook()));
                pz.pause_at(HookPoint::LoadPublished, L1);
                if ev == Ev::SecondPin {
                    pz.pause_at(HookPoint::LoadWaiting, L1);
                } else {
                    pz.report(HookPoint::LoadWaiting, L1);
                }
                let take = |o| match o {
                    Ok(PinOutcome::Pinned(p)) => Some(p),
                    _ => None,
                };
                let (first, second, at_second, p1, p2) = thread::scope(|s| {
                    let t1 = spawn(s, &pz, "T1", || take(pool.pin(L1, &map)));
                    let first = pz.next_event();
                    let (mut second, mut at_second, mut t2) = (None, None, None);
                    if first == Event::Arrived(HookPoint::LoadPublished, L1, f0) {
                        t2 = Some(spawn(s, &pz, "T2", || take(pool.pin(L1, &map))));
                        let e = pz.next_event();
                        second = Some(e);
                        at_second = Some(pool.frame_info(f0).unwrap());
                    }
                    pz.release_all();
                    (
                        first,
                        second,
                        at_second,
                        t1.join().unwrap(),
                        t2.map(|h| h.join().unwrap()),
                    )
                });
                assert_eq!(
                    first,
                    Event::Arrived(HookPoint::LoadPublished, L1, f0),
                    "{row}"
                );
                let waiting = if ev == Ev::SecondPin {
                    Event::Arrived(HookPoint::LoadWaiting, L1, f0)
                } else {
                    Event::Seen(HookPoint::LoadWaiting, L1, f0)
                };
                assert_eq!(second, Some(waiting), "{row}");
                if ev == Ev::SecondPin {
                    // count + 1 at LoadWaiting, the frame still Loading.
                    assert_eq!(at_second, Some(info(Loading, Some(L1), 2, false)), "{row}");
                }
                let (p1, p2) = (p1.unwrap(), p2.unwrap().unwrap());
                assert_eq!((p1.frame(), p2.frame()), (f0, f0), "{row}");
                assert_eq!(fi(&pool, f0), info(Resident, Some(L1), 2, false), "{row}");
            }
            (Resident, Ev::PinOrPinResident) => {
                let (_file, map, pool) = with_l1(2, false);
                let f = pin_l1(&pool, &map);
                let a = match pool.pin(L1, &map) {
                    Ok(PinOutcome::Pinned(p)) => p,
                    other => panic!("{row} {other:?}"),
                };
                assert_eq!(fi(&pool, f), info(Resident, Some(L1), 1, false), "{row}");
                let b = pool.pin_resident(L1).unwrap().unwrap();
                assert_eq!(fi(&pool, f), info(Resident, Some(L1), 2, false), "{row}");
                drop((a, b));
            }
            (Resident, Ev::PinDropped) => {
                let (_file, map, pool) = with_l1(2, false);
                let f = pin_l1(&pool, &map);
                let a = pool.pin_resident(L1).unwrap().unwrap();
                let b = pool.pin_frame(f).unwrap().unwrap();
                assert_eq!(fi(&pool, f), info(Resident, Some(L1), 2, false), "{row}");
                drop(a);
                assert_eq!(fi(&pool, f), info(Resident, Some(L1), 1, false), "{row}");
                drop(b);
            }
            (Reserved, Ev::CreateAbsent) => {
                let (_f, _m, pool) = fixture(2).unwrap();
                let res = pool.reserve().unwrap();
                let r = res.frame();
                let c = match pool.create(res, L2, Kind::Leaf, 1) {
                    Ok(CreateOutcome::Created(c)) => c,
                    other => panic!("{row} {other:?}"),
                };
                assert_eq!(c.frame(), r, "{row}");
                assert_eq!(fi(&pool, r), info(Resident, Some(L2), 1, false), "{row}");
                assert!(c.is_dirty() && !c.is_freed(), "{row}");
            }
            (Reserved, Ev::CreateLoading) => {
                let (_file, map, pool) = fixture(3).unwrap();
                let l7 = PageKey::Lpid(7);
                let pz = Pauser::new();
                pool.set_hook(Some(pz.hook()));
                pz.pause_at(HookPoint::LoadPublished, l7);
                let (first, seen, r1) = thread::scope(|s| {
                    let t1 = spawn(s, &pz, "T1", || {
                        pool.pin(l7, &map)
                            .map(|o| matches!(o, PinOutcome::Unmapped))
                    });
                    let first = pz.next_event();
                    let mut seen = None;
                    if first == Event::Arrived(HookPoint::LoadPublished, l7, f0) {
                        let res = pool.reserve().unwrap();
                        let r = res.frame();
                        let reserved = pool.frame_info(r).unwrap();
                        let out = pool.create(res, l7, Kind::Leaf, 1).unwrap();
                        let loading = matches!(out, CreateOutcome::Loading(_));
                        seen = Some((
                            reserved,
                            loading,
                            pool.frame_info(f0).unwrap(),
                            pool.frame_info(r).unwrap(),
                        ));
                        drop(out);
                    }
                    pz.release_all();
                    (first, seen, t1.join().unwrap())
                });
                assert_eq!(
                    first,
                    Event::Arrived(HookPoint::LoadPublished, l7, f0),
                    "{row}"
                );
                let (reserved, loading, loading_frame, res_frame) = seen.unwrap();
                assert_eq!(reserved, info(Reserved, None, 0, false), "{row}");
                assert!(loading, "{row}: create gives Loading(res)");
                assert_eq!(loading_frame, info(Loading, Some(l7), 1, false), "{row}");
                assert_eq!(res_frame, info(Reserved, None, 0, false), "{row}");
                assert!(r1.unwrap(), "{row}");
            }
            (Reserved, Ev::CreateFreed) | (Reserved, Ev::CreateLive) => {
                let (_f, _m, pool) = fixture(3).unwrap();
                let kept = match pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1) {
                    Ok(CreateOutcome::Created(c)) => c.release(),
                    other => panic!("{row} {other:?}"),
                };
                let g = kept.frame();
                if ev == Ev::CreateFreed {
                    kept.latch_exclusive().unwrap().mark_freed().unwrap();
                }
                let res = pool.reserve().unwrap();
                let r = res.frame();
                assert_eq!(fi(&pool, r), info(Reserved, None, 0, false), "{row}");
                match pool.create(res, L2, Kind::Leaf, 2) {
                    Ok(CreateOutcome::Created(c)) if ev == Ev::CreateFreed => {
                        assert_eq!(c.frame(), g, "{row}");
                        assert_eq!(fi(&pool, g), info(Resident, Some(L2), 2, false), "{row}");
                        assert!(c.is_dirty() && !c.is_freed(), "{row}");
                        assert_eq!(c.content_epoch(), 2, "{row}");
                        assert_eq!(fi(&pool, r), info(Free, None, 0, false), "{row}");
                    }
                    Ok(CreateOutcome::Exists(p, res)) if ev == Ev::CreateLive => {
                        assert_eq!(p.frame(), g, "{row}");
                        assert_eq!(fi(&pool, g), info(Resident, Some(L2), 2, false), "{row}");
                        assert_eq!(fi(&pool, r), info(Reserved, None, 0, false), "{row}");
                        drop(res);
                        assert_eq!(fi(&pool, r), info(Free, None, 0, false), "{row}");
                    }
                    other => panic!("{row} {other:?}"),
                }
            }
            (Resident, Ev::TryVictimUnpinned) | (Resident, Ev::TryVictimPinned) => {
                let (_file, map, pool) = with_l1(2, false);
                let f = pin_l1(&pool, &map);
                let held =
                    (ev == Ev::TryVictimPinned).then(|| pool.pin_resident(L1).unwrap().unwrap());
                let pins = u64::from(held.is_some());
                let v = pool.try_victim(f).unwrap();
                assert_eq!(v.is_some(), held.is_none(), "{row}");
                assert_eq!(fi(&pool, f), info(Resident, Some(L1), pins, false), "{row}");
                if let Some(v) = v {
                    assert_eq!((v.key(), v.frame()), (L1, f), "{row}");
                    // The guard holds the latch.
                    assert!(pool.try_victim(f).unwrap().is_none(), "{row}");
                }
            }
            (Resident, Ev::RemoveClean) | (Resident, Ev::RemoveDirty) => {
                let (_file, map, pool) = with_l1(2, false);
                let f = pin_l1(&pool, &map);
                if ev == Ev::RemoveDirty {
                    let p = pool.pin_resident(L1).unwrap().unwrap();
                    p.latch_exclusive().unwrap().mark_dirty(1).unwrap();
                }
                let v = pool.try_victim(f).unwrap().unwrap();
                match (ev, v.remove_from_table().unwrap()) {
                    (Ev::RemoveClean, Removal::Removed(res)) => {
                        assert_eq!(fi(&pool, f), info(Reserved, None, 0, false), "{row}");
                        assert_eq!(pool.frame_of(L1).unwrap(), None, "{row}");
                        drop(res);
                        assert_eq!(fi(&pool, f), info(Free, None, 0, false), "{row}");
                    }
                    (Ev::RemoveDirty, Removal::Kept) => {
                        assert_eq!(fi(&pool, f), info(Resident, Some(L1), 0, false), "{row}");
                        assert_eq!(pool.frame_of(L1).unwrap(), Some(f), "{row}");
                    }
                    (_, other) => panic!("{row} {other:?}"),
                }
            }
            (Resident, Ev::RemovePinnedAtLatch) => {
                let (_file, map, pool) = with_l1(2, false);
                let f = pin_l1(&pool, &map);
                let pz = Pauser::new();
                pool.set_hook(Some(pz.hook()));
                pz.pause_at(HookPoint::RemoveLatched, L1);
                let (first, p, kept) = thread::scope(|s| {
                    let t1 = spawn(s, &pz, "T1", || {
                        let v = pool.try_victim(f).unwrap().unwrap();
                        matches!(v.remove_from_table().unwrap(), Removal::Kept)
                    });
                    // T1 holds f's exclusive latch; no pin, no mutex.
                    let first = pz.next_event();
                    let mut p = None;
                    if first == Event::Arrived(HookPoint::RemoveLatched, L1, f) {
                        p = pool.pin_resident(L1).unwrap();
                    }
                    pz.release_all();
                    (first, p, t1.join().unwrap())
                });
                assert_eq!(
                    first,
                    Event::Arrived(HookPoint::RemoveLatched, L1, f),
                    "{row}"
                );
                assert!(p.is_some() && kept, "{row}");
                assert_eq!(fi(&pool, f), info(Resident, Some(L1), 1, false), "{row}");
            }
            (Resident, Ev::MarkDirty) => {
                let (_file, map, pool) = with_l1(2, false);
                pin_l1(&pool, &map);
                let p = pool.pin_resident(L1).unwrap().unwrap();
                let mut g = p.latch_exclusive().unwrap();
                assert!(!g.is_dirty(), "{row}");
                g.mark_dirty(1).unwrap();
                assert!(g.is_dirty() && !g.is_freed(), "{row}");
                drop(g);
                assert_eq!(
                    fi(&pool, p.frame()),
                    info(Resident, Some(L1), 1, false),
                    "{row}"
                );
            }
            (Resident, Ev::MarkWritten) | (Resident, Ev::MarkFreed) => {
                let (_f, _m, pool) = fixture(2).unwrap();
                let mut c = match pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1) {
                    Ok(CreateOutcome::Created(c)) => c,
                    other => panic!("{row} {other:?}"),
                };
                assert!(c.is_dirty(), "{row}");
                if ev == Ev::MarkWritten {
                    let crc = page::seal(&mut c).unwrap();
                    c.mark_written(Ref {
                        slot: 20,
                        epoch: 1,
                        crc,
                    })
                    .unwrap();
                    assert!(!c.is_dirty() && !c.is_freed(), "{row}");
                    assert_eq!(
                        fi(&pool, c.frame()),
                        info(Resident, Some(L2), 1, false),
                        "{row}"
                    );
                } else {
                    c.mark_freed().unwrap();
                    assert!(!c.is_dirty() && c.is_freed(), "{row}");
                    assert_eq!(
                        fi(&pool, c.frame()),
                        info(Resident, Some(L2), 1, true),
                        "{row}"
                    );
                }
            }
            (Free, Ev::TryVictimAndPinFrame) | (Reserved, Ev::TryVictimAndPinFrame) => {
                let (_f, _m, pool) = fixture(2).unwrap();
                let res = (from == Reserved).then(|| pool.reserve().unwrap());
                let want = info(from, None, 0, false);
                assert_eq!(fi(&pool, f0), want, "{row}");
                assert!(pool.try_victim(f0).unwrap().is_none(), "{row}");
                assert!(pool.pin_frame(f0).unwrap().is_none(), "{row}");
                assert_eq!(fi(&pool, f0), want, "{row}");
                drop(res);
            }
            (Resident, Ev::TryReserve) => {
                // Card A-3b-0, Work 11: the pool is full and every frame but
                // frame 0's is held by a pin, so `try_reserve`'s one sweep
                // decides. The clean frame is the first of the two rows, the
                // dirty one the second.
                try_reserve_rows += 1;
                let clean = try_reserve_rows == 1;
                let f1 = FrameId(1);
                let (file, map, pool) = with_l1(2, false);
                put_leaf(&file, &map, 2, 11, 0x22).unwrap();
                let log = EventLog::new();
                pool.set_hook(Some(log.hook_no_wait()));
                let f0 = pin_l1(&pool, &map);
                assert_eq!(f0, FrameId(0), "{row}");
                let held = match pool.pin(L2, &map) {
                    Ok(PinOutcome::Pinned(p)) => p,
                    other => panic!("{row} {other:?}"),
                };
                assert_eq!(held.frame(), f1, "{row}");
                if !clean {
                    let p = pool.pin_resident(L1).unwrap().unwrap();
                    p.latch_exclusive().unwrap().mark_dirty(1).unwrap();
                    drop(p);
                }
                assert_eq!(fi(&pool, f0), info(Resident, Some(L1), 0, false), "{row}");
                let got = pool.try_reserve();
                let evicted = log.since(0, HookPoint::Evicted);
                if clean {
                    let Ok(Some(res)) = got else {
                        panic!("{row}: {got:?}")
                    };
                    assert_eq!(res.frame(), f0, "{row}");
                    assert_eq!(fi(&pool, f0), info(Reserved, None, 0, false), "{row}");
                    assert_eq!(pool.frame_of(L1).unwrap(), None, "{row}");
                    // The first rotation clears frame 0's bit, the second
                    // takes it: the hand moved three positions of two, and
                    // rests on the held frame.
                    assert_eq!(pool.stats().unwrap().clock_hand, 1, "{row}");
                    assert_eq!(pool.stats().unwrap().evictions, 1, "{row}");
                    assert_eq!(evicted, vec![(L1, f0)], "{row}");
                    drop(res);
                    assert_eq!(fi(&pool, f0), info(Free, None, 0, false), "{row}");
                } else {
                    assert!(matches!(got, Ok(None)), "{row}: {got:?}");
                    assert_eq!(pool.frame_of(L1).unwrap(), Some(f0), "{row}");
                    // Unchanged but for its bit, which the sweep cleared as
                    // it passed: a dirty frame is no victim at this card.
                    assert_eq!(
                        fi(&pool, f0),
                        FrameInfo {
                            state: Resident,
                            key: Some(L1),
                            pins: 0,
                            freed: false,
                            referenced: false
                        },
                        "{row}"
                    );
                    assert_eq!(evicted, vec![], "{row}");
                    assert_eq!(pool.stats().unwrap().evictions, 0, "{row}");
                }
                assert_eq!(fi(&pool, f1), info(Resident, Some(L2), 1, false), "{row}");
                drop(held);
            }
            (from, ev) => panic!("no row body for {from:?} / {ev:?}"),
        }
    }
}
