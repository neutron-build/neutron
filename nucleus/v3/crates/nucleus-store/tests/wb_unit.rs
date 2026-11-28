//! Card A-3c's single-threaded tests of the write-back (Work 14.1): the
//! slot allocator against an independent model and its growth, the LPID
//! allocator against an independent model through the pool, the map's
//! growth, the eviction's steps and errors, §4.1's pins, the touch check,
//! the gate's rules and what a pool built with `Pool::new` refuses.
//!
//! Fixture: `wb_pool::<StdPrimitives>` at `PS`, each test stating `n`, `F`
//! (computed from R-1-memory §4.2's formula at the lowered floor, not asked
//! of the build), fanout, `slot_count` and epoch. Every marker is a distinct
//! byte. The models are written in the tests from S-STORE's sentences as the
//! card restates them; none calls the code it judges.

mod wb_support;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use nucleus_env::File;
use nucleus_store::StoreError;
use nucleus_store::buf::{
    AllocRestart, EvictOutcome, Hold, HookPoint, ImageRoot, LpidAlloc, LpidStats, MapPinInfo,
    PageKey, Pin, PinOutcome, Pool, PoolConfig, SlotAlloc, SlotInit, SlotStats, SlotUse,
};
use nucleus_store::meta::Ref;
use nucleus_store::page::Kind;
use nucleus_store::sync::StdPrimitives;
use wb_support::drive::{
    evict, evict_out, s1_flip, s3_free, s3_make_mapleaf, s3_new_page, s3_touch,
};
use wb_support::file::WbFile;
use wb_support::{H_MIN_TEST, PS, f_min, frame_of, min_f, pin, pin_read, wb_pool, wb_pool_on};

/// The key of an LPID.
fn lpid(x: u64) -> PageKey {
    PageKey::Lpid(x)
}

/// The key of a MapLeaf.
fn mapleaf(i: u64) -> PageKey {
    PageKey::MapLeaf(i)
}

/// Whether `r` is `Err(StoreError::Invalid(_))`.
fn is_invalid<T>(r: &Result<T, StoreError>) -> bool {
    matches!(r, Err(StoreError::Invalid(_)))
}

/// The slot of the page write the log recorded for `(kind, id)`, the last
/// one if several.
fn home_in_log(file: &WbFile, kind: u8, id: u64) -> Option<u64> {
    file.writes()
        .into_iter()
        .rev()
        .find(|w| w.kind == kind && w.id == id)
        .map(|w| w.offset / u64::from(PS))
}

/// Run `body` on every sequence of `alphabet` symbols (each `0..alphabet`)
/// of length 1 to `max_len`.
fn every_sequence(alphabet: u64, max_len: usize, mut body: impl FnMut(&[u8])) {
    let mut digits = vec![0u8; max_len];
    for len in 1..=max_len {
        let total = alphabet.pow(len as u32);
        for index in 0..total {
            let mut rest = index;
            for d in digits.iter_mut().take(len) {
                *d = u8::try_from(rest % alphabet).unwrap_or(u8::MAX);
                rest /= alphabet;
            }
            body(&digits[..len]);
        }
    }
}

/// A test mutex guard, recovered from poison.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------- the slots

/// The slot alphabet of `slot_table` (card A-3c, Work 14.1): the ten ops the
/// card names, and `ReleaseTwice`, since the card counts eleven ops and
/// 16,104 sequences (`11 + 11² + 11³ + 11⁴`): a release naming one used slot
/// twice, which S5 refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotOp {
    Alloc,
    SupersedeCur,
    SupersedeOld,
    SupersedeFuture,
    SupersedeReserved,
    ReleaseEmpty,
    ReleaseOldestUsed,
    ReleaseReserved,
    ReleaseFree,
    ReleaseTwice,
    Flip,
}

/// The eleven ops.
const SLOT_OPS: [SlotOp; 11] = [
    SlotOp::Alloc,
    SlotOp::SupersedeCur,
    SlotOp::SupersedeOld,
    SlotOp::SupersedeFuture,
    SlotOp::SupersedeReserved,
    SlotOp::ReleaseEmpty,
    SlotOp::ReleaseOldestUsed,
    SlotOp::ReleaseReserved,
    SlotOp::ReleaseFree,
    SlotOp::ReleaseTwice,
    SlotOp::Flip,
];

/// What the model says one op does: what the test drives and what it must
/// answer.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SlotAnswer {
    /// The op's precondition fails: not driven.
    Skipped,
    /// `alloc`: the slot, or an error.
    Alloc(Result<u64, ()>),
    /// `supersede`: the reference to pass, and whether it is refused.
    Supersede {
        slot: u64,
        epoch: u32,
        refused: bool,
    },
    /// `release_step6`: the list to pass, and whether it is refused.
    Release { list: Vec<u64>, refused: bool },
    /// `Flip`: the test's own epoch moves; nothing is driven.
    Flip,
}

/// The slot model, written from Work 4's S1–S5 text: the free, pending and
/// used sets, `slot_count`, `e` and how many times the file grew.
#[derive(Debug, Clone)]
struct SlotModel {
    free: BTreeSet<u64>,
    pending: BTreeSet<u64>,
    used: BTreeSet<u64>,
    slot_count: u64,
    e: u32,
    growths: u64,
    /// The slots this sequence allocated, in order.
    alloc_order: Vec<u64>,
}

/// The reserved slot of the fixture.
const SLOT5: u64 = 5;

/// The growth in slots at the file's present length (§4.5).
fn growth_slots(slot_count: u64) -> u64 {
    let len = slot_count * u64::from(PS);
    (len / 16).max(1 << 20) / u64::from(PS)
}

impl SlotModel {
    /// `SlotAlloc::new(4096, 8, 5..6, Fresh)`: free {3, 4, 6, 7}, reserved {5}.
    fn fresh() -> SlotModel {
        SlotModel {
            free: BTreeSet::from([3, 4, 6, 7]),
            pending: BTreeSet::new(),
            used: BTreeSet::new(),
            slot_count: 8,
            e: 1,
            growths: 0,
            alloc_order: Vec::new(),
        }
    }

    /// The last slot this sequence allocated that is not free, for a
    /// `Supersede*` op.
    fn supersede_target(&self) -> Option<u64> {
        self.alloc_order
            .iter()
            .rev()
            .copied()
            .find(|s| !self.free.contains(s))
    }

    /// S4 on a valid page slot: freed iff `epoch == e`; `old.epoch > e` is
    /// refused; a slot already free is refused.
    fn supersede(&mut self, slot: u64, epoch: u32) -> bool {
        if self.free.contains(&slot) {
            return true;
        }
        match epoch.cmp(&self.e) {
            std::cmp::Ordering::Equal => {
                self.pending.remove(&slot);
                self.used.remove(&slot);
                self.free.insert(slot);
                false
            }
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Greater => true,
        }
    }

    /// S5: pending to free, then `list` exactly pending; refused for a fixed,
    /// reserved, past-end, free or repeated slot, changing nothing.
    fn release(&mut self, list: &[u64]) -> bool {
        let mut seen = BTreeSet::new();
        for &s in list {
            if s < 3
                || s == SLOT5
                || s >= self.slot_count
                || self.free.contains(&s)
                || !seen.insert(s)
            {
                return true;
            }
        }
        let old = self.pending.clone();
        self.pending.clear();
        for s in old {
            self.free.insert(s);
        }
        for &s in list {
            self.used.remove(&s);
            self.pending.insert(s);
        }
        false
    }

    /// Apply `op` and say what the test drives.
    fn apply(&mut self, op: SlotOp) -> SlotAnswer {
        match op {
            SlotOp::Alloc => {
                let slot = match self.free.iter().next().copied() {
                    Some(s) => s,
                    None => {
                        let grow = growth_slots(self.slot_count);
                        for s in self.slot_count..self.slot_count + grow {
                            self.free.insert(s);
                        }
                        self.slot_count += grow;
                        self.growths += 1;
                        *self.free.iter().next().unwrap_or(&0)
                    }
                };
                self.free.remove(&slot);
                self.used.insert(slot);
                self.alloc_order.push(slot);
                SlotAnswer::Alloc(Ok(slot))
            }
            SlotOp::SupersedeCur | SlotOp::SupersedeOld | SlotOp::SupersedeFuture => {
                let Some(slot) = self.supersede_target() else {
                    return SlotAnswer::Skipped;
                };
                let epoch = match op {
                    SlotOp::SupersedeCur => Some(self.e),
                    SlotOp::SupersedeOld => self.e.checked_sub(1),
                    _ => self.e.checked_add(1),
                };
                let Some(epoch) = epoch else {
                    return SlotAnswer::Skipped;
                };
                let refused = self.supersede(slot, epoch);
                SlotAnswer::Supersede {
                    slot,
                    epoch,
                    refused,
                }
            }
            SlotOp::SupersedeReserved => SlotAnswer::Supersede {
                slot: SLOT5,
                epoch: self.e,
                refused: true,
            },
            SlotOp::ReleaseEmpty => {
                self.release(&[]);
                SlotAnswer::Release {
                    list: Vec::new(),
                    refused: false,
                }
            }
            SlotOp::ReleaseOldestUsed | SlotOp::ReleaseTwice => {
                let Some(s) = self.used.iter().next().copied() else {
                    return SlotAnswer::Skipped;
                };
                let list = if op == SlotOp::ReleaseOldestUsed {
                    vec![s]
                } else {
                    vec![s, s]
                };
                let refused = self.release(&list);
                SlotAnswer::Release { list, refused }
            }
            SlotOp::ReleaseReserved => {
                let refused = self.release(&[SLOT5]);
                SlotAnswer::Release {
                    list: vec![SLOT5],
                    refused,
                }
            }
            SlotOp::ReleaseFree => {
                let Some(s) = self.free.iter().next().copied() else {
                    return SlotAnswer::Skipped;
                };
                let refused = self.release(&[s]);
                SlotAnswer::Release {
                    list: vec![s],
                    refused,
                }
            }
            SlotOp::Flip => {
                self.e += 1;
                SlotAnswer::Flip
            }
        }
    }

    /// What slot `s` is.
    fn use_of(&self, s: u64) -> SlotUse {
        if s < 3 {
            SlotUse::Fixed
        } else if s == SLOT5 {
            SlotUse::Reserved
        } else if s >= self.slot_count {
            SlotUse::PastEnd
        } else if self.free.contains(&s) {
            SlotUse::Free
        } else if self.pending.contains(&s) {
            SlotUse::Pending
        } else {
            SlotUse::Used
        }
    }
}

/// Compare the allocator with the model: every state the card names, and
/// `stats()` destructured with no `..` (a field added to `SlotStats` fails
/// to compile here until the comparison names it).
fn check_slots(alloc: &SlotAlloc<StdPrimitives>, model: &SlotModel) -> Result<(), StoreError> {
    for s in (0..12u64).chain([8, 263, 264]) {
        assert_eq!(alloc.state(s)?, model.use_of(s), "state({s})");
    }
    let SlotStats {
        slot_count,
        free,
        pending,
        growths,
    } = alloc.stats()?;
    assert_eq!(slot_count, model.slot_count, "slot_count");
    assert_eq!(free, model.free.len() as u64, "free");
    assert_eq!(pending, model.pending.len() as u64, "pending");
    assert_eq!(growths, model.growths, "growths");
    Ok(())
}

/// `slot_table` (exhaustive, against an independent model; card A-3c, Work
/// 14.1). `SlotAlloc::new(4096, 8, 5..6, SlotInit::Fresh)` over a `WbFile`
/// of 8 slots: free {3, 4, 6, 7}, reserved {5}. Every sequence of length 1
/// to 4 over the 11 ops, each on a fresh allocator. Mutants
/// WB-SLOT-NOT-LOWEST, WB-SUPERSEDE-NEVER, WB-PENDING-NEVER-FREED,
/// WB-GROW-ONE-SLOT.
#[test]
fn slot_table() {
    let mut sequences = 0usize;
    // One file for every allocator: nothing is written, only its length is
    // read.
    let file = Arc::new(WbFile::new().unwrap());
    file.set_len(8 * u64::from(PS)).unwrap();
    every_sequence(11, 4, |seq| {
        sequences += 1;
        let alloc =
            SlotAlloc::<StdPrimitives>::new(PS, 8, SLOT5..SLOT5 + 1, SlotInit::Fresh).unwrap();
        let mut model = SlotModel::fresh();
        let mut e = 1u32;
        for &digit in seq {
            let op = SLOT_OPS.get(usize::from(digit)).copied().unwrap();
            match model.apply(op) {
                SlotAnswer::Skipped => {}
                SlotAnswer::Alloc(want) => {
                    let got = alloc.alloc(&*file).map_err(|_| ());
                    assert_eq!(got, want, "alloc in {seq:?}");
                }
                SlotAnswer::Supersede {
                    slot,
                    epoch,
                    refused,
                } => {
                    let got = is_invalid(&alloc.supersede(
                        Ref {
                            slot,
                            epoch,
                            crc: 0,
                        },
                        e,
                    ));
                    assert_eq!(got, refused, "supersede {slot} at {epoch} in {seq:?}");
                }
                SlotAnswer::Release { list, refused } => {
                    let got = is_invalid(&alloc.release_step6(&list));
                    assert_eq!(got, refused, "release {list:?} in {seq:?}");
                }
                SlotAnswer::Flip => e += 1,
            }
            check_slots(&alloc, &model).unwrap();
        }
    });
    assert_eq!(sequences, 11 + 11 * 11 + 11 * 11 * 11 + 11 * 11 * 11 * 11);
}

/// One growth case of `slot_growth`: drain every free slot, fail the
/// extension once (nothing changes), then grow by `want` slots.
fn growth_case(
    page_size: u32,
    slot_count: u64,
    reserved: Range<u64>,
    want: u64,
) -> Result<(), StoreError> {
    let file = Arc::new(WbFile::new().map_err(StoreError::Env)?);
    file.set_len(slot_count * u64::from(page_size))
        .map_err(StoreError::Env)?;
    let alloc = SlotAlloc::<StdPrimitives>::new(page_size, slot_count, reserved, SlotInit::Fresh)?;
    let free = alloc.stats()?.free;
    for _ in 0..free {
        let slot = alloc.alloc(&*file)?;
        assert!(slot >= 3, "a page slot");
    }
    let before = alloc.stats()?;
    assert_eq!(before.free, 0);
    let len_before = file.len().map_err(StoreError::Env)?;
    file.fail_set_len(true);
    assert!(matches!(alloc.alloc(&*file), Err(StoreError::Env(_))));
    assert_eq!(alloc.stats()?, before);
    assert_eq!(file.len().map_err(StoreError::Env)?, len_before);
    file.fail_set_len(false);
    assert_eq!(alloc.alloc(&*file)?, slot_count, "the old slot_count");
    let after = alloc.stats()?;
    assert_eq!(
        after,
        SlotStats {
            slot_count: slot_count + want,
            free: want - 1,
            pending: 0,
            growths: 1,
        }
    );
    assert_eq!(
        file.len().map_err(StoreError::Env)?,
        (slot_count + want) * u64::from(page_size)
    );
    assert_eq!(alloc.state(slot_count)?, SlotUse::Used);
    if page_size == PS {
        assert_eq!(alloc.state(256)?, SlotUse::Reserved);
        assert_eq!(alloc.state(257)?, SlotUse::Reserved);
    }
    Ok(())
}

/// `slot_growth` (card A-3c, Work 14.1; S-STORE §4.5: `max(1 MiB, file_len /
/// 16)` rounded down to whole slots). At 4 KiB, `slot_count` 258 grows by
/// 256 slots, 4,100 by 256 (`16,793,600 / 16` bytes is 256.25 slots) and
/// 8,192 by 512; at 64 KiB, `slot_count` 17 grows by 16. A failed extension
/// changes nothing. Mutant WB-GROW-ONE-SLOT.
#[test]
fn slot_growth() {
    growth_case(PS, 258, 256..258, 256).unwrap();
    growth_case(PS, 4100, 256..258, 256).unwrap();
    growth_case(PS, 8192, 256..258, 512).unwrap();
    growth_case(65536, 17, 16..17, 16).unwrap();
}

/// `slot_given_init` (card A-3c, Work 14.1): `SlotInit::Given` refuses slot
/// 0, 1, 2, a reserved slot, one at `slot_count`, a duplicate and a slot in
/// both lists; a valid one gives the states listed. Mutant
/// REPLICA-SLOT-FREE's `Given` half.
#[test]
fn slot_given_init() {
    let bad = |free: Vec<u64>, pending: Vec<u64>| {
        let r =
            SlotAlloc::<StdPrimitives>::new(PS, 600, 256..258, SlotInit::Given { free, pending });
        assert!(is_invalid(&r));
    };
    bad(vec![0], vec![]);
    bad(vec![1], vec![]);
    bad(vec![2], vec![]);
    bad(vec![256], vec![]);
    bad(vec![257], vec![]);
    bad(vec![600], vec![]);
    bad(vec![3, 3], vec![]);
    bad(vec![], vec![4, 4]);
    bad(vec![7], vec![7]);
    let alloc = SlotAlloc::<StdPrimitives>::new(
        PS,
        600,
        256..258,
        SlotInit::Given {
            free: vec![258, 3],
            pending: vec![4],
        },
    )
    .unwrap();
    for (s, want) in [
        (0, SlotUse::Fixed),
        (2, SlotUse::Fixed),
        (3, SlotUse::Free),
        (4, SlotUse::Pending),
        (5, SlotUse::Used),
        (256, SlotUse::Reserved),
        (257, SlotUse::Reserved),
        (258, SlotUse::Free),
        (259, SlotUse::Used),
        (599, SlotUse::Used),
        (600, SlotUse::PastEnd),
    ] {
        assert_eq!(alloc.state(s).unwrap(), want, "state({s})");
    }
    assert_eq!(
        alloc.stats().unwrap(),
        SlotStats {
            slot_count: 600,
            free: 2,
            pending: 1,
            growths: 0,
        }
    );
}

// ----------------------------------------------------------------- the LPIDs

/// The LPID alphabet of `lpid_table` (card A-3c, Work 14.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LpOp {
    /// `alloc_lpids(1, &[])`.
    A1,
    /// `alloc_lpids(1, &held)`.
    A1Held,
    /// `alloc_lpids(2, &[])`.
    A2,
    /// `alloc_lpids(2, &held)`.
    A2Held,
    /// `release_lpid(x)`.
    Release(u64),
    /// Evict MapLeaf `i`.
    Cold(u64),
    /// Pin MapLeaf `i` and drop the pin.
    Warm(u64),
    /// Make MapLeaf 3 by the restart path.
    Make3,
}

/// The 19 ops.
const LP_OPS: [LpOp; 19] = [
    LpOp::A1,
    LpOp::A1Held,
    LpOp::A2,
    LpOp::A2Held,
    LpOp::Release(1),
    LpOp::Release(2),
    LpOp::Release(3),
    LpOp::Release(4),
    LpOp::Release(5),
    LpOp::Release(6),
    LpOp::Cold(0),
    LpOp::Cold(1),
    LpOp::Cold(2),
    LpOp::Cold(3),
    LpOp::Warm(0),
    LpOp::Warm(1),
    LpOp::Warm(2),
    LpOp::Warm(3),
    LpOp::Make3,
];

/// The fanout of `lpid_table`.
const LF: u64 = 2;

/// What the model says an allocation answers.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LpAnswer {
    Taken { lpids: Vec<u64>, pinned: Vec<u64> },
    Restart { leaf: u64, emptied: Vec<u64> },
}

/// What one op does: whether it is driven, and what the test does.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LpStep {
    /// Not driven.
    Skipped,
    /// An allocation under `held`, answering `answer`.
    Alloc { held: Vec<u64>, answer: LpAnswer },
    /// `release_lpid(x)`.
    Release(u64),
    /// Evict MapLeaf `i`.
    Cold(u64),
    /// Pin MapLeaf `i`.
    Warm(u64),
    /// Make MapLeaf 3.
    Make3,
}

/// The LPID model, written from §4.2 and §5.6 as Work 5's L1–L3 restate them.
#[derive(Debug, Clone)]
struct LpModel {
    next: u64,
    free: BTreeSet<u64>,
    made: BTreeSet<u64>,
    resident: BTreeSet<u64>,
    held: BTreeSet<u64>,
}

impl LpModel {
    /// The fixture: MapLeafs 0, 1 and 2 made and resident, LPIDs 1..5
    /// allocated, `next_lpid` 6, MapLeaf 3 not made.
    fn fresh() -> LpModel {
        LpModel {
            next: 6,
            free: BTreeSet::new(),
            made: BTreeSet::from([0, 1, 2]),
            resident: BTreeSet::from([0, 1, 2]),
            held: BTreeSet::new(),
        }
    }

    /// The lowest free LPID from `from` on under no held MapLeaf.
    fn first_outside_held(&self, held: &BTreeSet<u64>, from: u64) -> Option<u64> {
        self.free
            .range(from..)
            .copied()
            .find(|x| !held.contains(&(x / LF)))
    }

    /// L1–L3.
    fn alloc(&mut self, n: usize, use_held: bool) -> LpAnswer {
        let held: BTreeSet<u64> = if use_held {
            self.held.clone()
        } else {
            BTreeSet::new()
        };
        let mut cands = Vec::new();
        let mut emptied = Vec::new();
        for &i in &held {
            let mut any = false;
            for x in self
                .free
                .range((i * LF).max(1)..((i + 1) * LF).min(self.next))
            {
                cands.push(*x);
                any = true;
            }
            if i * LF <= self.next && self.next < (i + 1) * LF {
                cands.extend(self.next..(i + 1) * LF);
                any = true;
            }
            if !any {
                emptied.push(i);
            }
        }
        cands.sort_unstable();
        // §5.6: under a pinned new MapLeaf the LPIDs from `next_lpid` on
        // count as free, so they are candidates above; what `next_lpid++`
        // hands out next starts past them, since no LPID is taken twice.
        let fresh = cands
            .iter()
            .copied()
            .filter(|&x| x >= self.next)
            .max()
            .map_or(self.next, |m| m + 1);
        if cands.len() >= n {
            let lpids: Vec<u64> = cands.into_iter().take(n).collect();
            self.commit(&lpids);
            return LpAnswer::Taken {
                lpids,
                pinned: Vec::new(),
            };
        }
        if !emptied.is_empty() {
            let leaf = self.first_outside_held(&held, 1).unwrap_or(fresh) / LF;
            return LpAnswer::Restart { leaf, emptied };
        }
        let mut lpids = cands;
        let mut pinned = Vec::new();
        let mut cursor = 1;
        while lpids.len() < n {
            match self.first_outside_held(&held, cursor) {
                Some(x) => {
                    lpids.push(x);
                    cursor = x + 1;
                    if !pinned.contains(&(x / LF)) {
                        pinned.push(x / LF);
                    }
                }
                None => break,
            }
        }
        let mut x = fresh;
        while lpids.len() < n {
            lpids.push(x);
            if !pinned.contains(&(x / LF)) {
                pinned.push(x / LF);
            }
            x += 1;
        }
        for &l in &pinned {
            if !self.resident.contains(&l) {
                return LpAnswer::Restart {
                    leaf: l,
                    emptied: Vec::new(),
                };
            }
        }
        self.commit(&lpids);
        LpAnswer::Taken { lpids, pinned }
    }

    fn commit(&mut self, lpids: &[u64]) {
        let old_next = self.next;
        for &x in lpids {
            if x < old_next {
                self.free.remove(&x);
            }
        }
        // `next_lpid` passes every LPID taken at or past it, wherever it
        // stands in the list.
        for &x in lpids {
            self.next = self.next.max(x + 1);
        }
    }

    /// Apply `op`.
    fn apply(&mut self, op: LpOp) -> LpStep {
        match op {
            LpOp::A1 | LpOp::A1Held | LpOp::A2 | LpOp::A2Held => {
                let (n, use_held) = match op {
                    LpOp::A1 => (1, false),
                    LpOp::A1Held => (1, true),
                    LpOp::A2 => (2, false),
                    _ => (2, true),
                };
                LpStep::Alloc {
                    held: if use_held {
                        self.held.iter().copied().collect()
                    } else {
                        Vec::new()
                    },
                    answer: self.alloc(n, use_held),
                }
            }
            LpOp::Release(x) => {
                if x >= self.next || self.free.contains(&x) {
                    return LpStep::Skipped;
                }
                self.free.insert(x);
                LpStep::Release(x)
            }
            LpOp::Cold(i) => {
                if !self.resident.contains(&i) || self.held.contains(&i) {
                    return LpStep::Skipped;
                }
                self.resident.remove(&i);
                LpStep::Cold(i)
            }
            LpOp::Warm(i) => {
                if !self.made.contains(&i) || self.resident.contains(&i) || self.held.contains(&i) {
                    return LpStep::Skipped;
                }
                self.resident.insert(i);
                LpStep::Warm(i)
            }
            LpOp::Make3 => {
                if self.made.contains(&3) {
                    return LpStep::Skipped;
                }
                self.made.insert(3);
                self.resident.insert(3);
                LpStep::Make3
            }
        }
    }

    /// The fixture's state after a restart at `leaf`: the pin of each
    /// `emptied` MapLeaf is dropped, and MapLeaf `leaf` is pinned if it was
    /// made.
    fn restart(&mut self, leaf: u64, emptied: &[u64]) {
        for i in emptied {
            self.held.remove(i);
        }
        if self.made.contains(&leaf) {
            self.held.insert(leaf);
            self.resident.insert(leaf);
        }
    }
}

/// Compare the pool with the model: the outcome, `lpid_stats()` destructured,
/// `lpid_is_free(x)` for `x` in 1..=10 and `frame_of(MapLeaf(i))` for `i` in
/// 0..=4.
fn check_lpids(pool: &Pool<WbFile, StdPrimitives>, model: &LpModel) -> Result<(), StoreError> {
    assert_eq!(
        pool.lpid_stats()?,
        LpidStats {
            next_lpid: model.next,
            free: model.free.len() as u64,
        }
    );
    for x in 1..=10 {
        assert_eq!(
            pool.lpid_is_free(x)?,
            model.free.contains(&x),
            "LPID {x} free"
        );
    }
    for i in 0..=4 {
        assert_eq!(
            pool.frame_of(mapleaf(i))?.is_some(),
            model.resident.contains(&i),
            "MapLeaf {i} resident"
        );
    }
    Ok(())
}

/// `lpid_table` (exhaustive, through a pool; card A-3c, Work 14.1). `n` 1,
/// `F` 18, fanout 2, `slot_count` 600, epoch 1. Every sequence of length 1
/// to 3 over the 19 ops, each on a fresh fixture. Mutants
/// ALLOC-RETRY-LOWEST, WB-LPID-SPLIT-HOLD, WB-LPID-EMPTIED-TAKES.
#[test]
fn lpid_table() {
    assert_eq!(min_f(1), 18);
    let mut sequences = 0usize;
    let file = Arc::new(WbFile::new().unwrap());
    file.set_len(600 * u64::from(PS)).unwrap();
    every_sequence(19, 3, |seq| {
        sequences += 1;
        let pool = wb_pool_on(Arc::clone(&file), 18, LF, 600, 1, H_MIN_TEST).unwrap();
        for i in 0..=2 {
            s3_make_mapleaf(&pool, i).unwrap();
        }
        {
            let g = pool.gate_shared().unwrap();
            for _ in 0..5 {
                match pool.alloc_lpids(1, &[], &g).unwrap() {
                    LpidAlloc::Taken { .. } => {}
                    LpidAlloc::Restart(r) => {
                        panic!("the fixture's allocation restarted at {}", r.leaf)
                    }
                }
            }
        }
        let mut model = LpModel::fresh();
        let mut held: Vec<Pin<'_, WbFile, StdPrimitives>> = Vec::new();
        check_lpids(&pool, &model).unwrap();
        for &digit in seq {
            let op = LP_OPS.get(usize::from(digit)).copied().unwrap();
            let step = model.apply(op);
            match step {
                LpStep::Skipped => {}
                LpStep::Alloc {
                    held: want_held,
                    answer,
                } => {
                    let g = pool.gate_shared().unwrap();
                    let (n, _use_held) = match op {
                        LpOp::A1 | LpOp::A1Held => (1, ()),
                        _ => (2, ()),
                    };
                    let got = pool.alloc_lpids(n, &want_held, &g).unwrap();
                    let got = match &got {
                        LpidAlloc::Taken { lpids, pins } => LpAnswer::Taken {
                            lpids: lpids.clone(),
                            pinned: pins
                                .iter()
                                .map(|p| match p.key() {
                                    PageKey::MapLeaf(i) => i,
                                    other => panic!("a pin on {other:?}"),
                                })
                                .collect(),
                        },
                        LpidAlloc::Restart(AllocRestart { leaf, emptied }) => LpAnswer::Restart {
                            leaf: *leaf,
                            emptied: emptied.clone(),
                        },
                    };
                    assert_eq!(got, answer, "alloc in {seq:?}");
                    match got {
                        LpAnswer::Taken { .. } => {}
                        LpAnswer::Restart { leaf, emptied } => {
                            // The pins of `emptied` go; MapLeaf `leaf` is
                            // pinned by `pin_page` and kept, as §5.6's
                            // restart does.
                            let mut kept = Vec::new();
                            for p in held {
                                match p.key() {
                                    PageKey::MapLeaf(i) if emptied.contains(&i) => drop(p),
                                    _ => kept.push(p),
                                }
                            }
                            held = kept;
                            if let Ok(PinOutcome::Pinned(p)) =
                                pool.pin_page(mapleaf(leaf), &Hold::nothing())
                            {
                                held.push(p);
                            }
                            model.restart(leaf, &emptied);
                        }
                    }
                    drop(g);
                }
                LpStep::Release(x) => pool.release_lpid(x).unwrap(),
                LpStep::Cold(i) => {
                    evict_out(&pool, mapleaf(i)).unwrap();
                }
                LpStep::Warm(i) => {
                    let p = pin(&pool, mapleaf(i), &Hold::nothing()).unwrap();
                    drop(p);
                }
                LpStep::Make3 => s3_make_mapleaf(&pool, 3).unwrap(),
            }
            check_lpids(&pool, &model).unwrap();
        }
    });
    assert_eq!(sequences, 19 + 19 * 19 + 19 * 19 * 19);
}

/// `lpid_table`'s fixture, for the scripted cases of `lpid_held_scripts`:
/// `n` 1, `F` 18, fanout 2, `slot_count` 600, epoch 1; MapLeafs 0, 1 and 2
/// made and resident, LPIDs 1..=5 allocated with no page, `next_lpid` 6.
fn lp_fixture() -> Result<(Arc<WbFile>, Pool<WbFile, StdPrimitives>), StoreError> {
    let (file, pool) =
        wb_pool::<StdPrimitives>(18, LF, 600, 1, H_MIN_TEST).map_err(StoreError::Invalid)?;
    for i in 0..=2 {
        s3_make_mapleaf(&pool, i)?;
    }
    {
        let g = pool.gate_shared()?;
        for _ in 0..5 {
            match pool.alloc_lpids(1, &[], &g)? {
                LpidAlloc::Taken { .. } => {}
                LpidAlloc::Restart(r) => {
                    return Err(StoreError::Invalid(format!(
                        "the fixture's allocation restarted at {}",
                        r.leaf
                    )));
                }
            }
        }
    }
    Ok((file, pool))
}

/// One allocation of `n` LPIDs under `held` in a gated operation of its own,
/// as an `LpAnswer`: the LPIDs and the MapLeafs newly pinned (whose pins are
/// then dropped), or the restart.
fn lp_alloc(
    pool: &Pool<WbFile, StdPrimitives>,
    n: usize,
    held: &[u64],
) -> Result<LpAnswer, StoreError> {
    let g = pool.gate_shared()?;
    let answer = match pool.alloc_lpids(n, held, &g)? {
        LpidAlloc::Taken { lpids, pins } => {
            let mut pinned = Vec::new();
            for p in &pins {
                match p.key() {
                    PageKey::MapLeaf(i) => pinned.push(i),
                    other => return Err(StoreError::Invalid(format!("a pin on {other:?}"))),
                }
            }
            LpAnswer::Taken { lpids, pinned }
        }
        LpidAlloc::Restart(AllocRestart { leaf, emptied }) => LpAnswer::Restart { leaf, emptied },
    };
    drop(g);
    Ok(answer)
}

/// `lpid_held_scripts` (card A-3c, Work 14.1, beside `lpid_table`, whose
/// sequences of length 1 to 3 never allocate with a held MapLeaf, since a
/// held MapLeaf needs a restart at a made one first). L1, L2 and L3 with
/// held MapLeafs, scripted on `lpid_table`'s fixture (fanout 2: MapLeaf `i`
/// maps LPIDs `2i` and `2i + 1`), each answer worked out in the test from
/// S-STORE §4.2 and §5.6 "Restart" as Work 5 restates them. Mutants
/// ALLOC-RETRY-LOWEST (a), WB-LPID-EMPTIED-TAKES (b), (c), (d); the two
/// defects the implementer's review found, an LPID handed out twice in one
/// call (e) and `next_lpid` left below an LPID taken from it (f); the
/// bound `1 ≤ n ≤ 64` (g); and the LPID bitmap's growth by adoption, which
/// keeps its bits (h).
#[test]
fn lpid_held_scripts() {
    let taken = |lpids: &[u64], pinned: &[u64]| LpAnswer::Taken {
        lpids: lpids.to_vec(),
        pinned: pinned.to_vec(),
    };
    let restart = |leaf: u64, emptied: &[u64]| LpAnswer::Restart {
        leaf,
        emptied: emptied.to_vec(),
    };
    let stats = |next_lpid: u64, free: u64| LpidStats { next_lpid, free };

    // (a) L1: LPIDs 1 (MapLeaf 0) and 5 (MapLeaf 2) free, MapLeaf 2 held.
    // The retry takes 5, the lowest free LPID under a MapLeaf it holds, not
    // 1, the lowest overall, and pins nothing new.
    {
        let (_file, pool) = lp_fixture().unwrap();
        pool.release_lpid(1).unwrap();
        pool.release_lpid(5).unwrap();
        let _held = pin(&pool, mapleaf(2), &Hold::nothing()).unwrap();
        assert_eq!(lp_alloc(&pool, 1, &[2]).unwrap(), taken(&[5], &[]), "(a)");
        assert_eq!(pool.lpid_stats().unwrap(), stats(6, 1), "(a)");
        assert!(pool.lpid_is_free(1).unwrap(), "(a)");
    }
    // (b) L2: LPID 1 free, MapLeaf 2 held and left with no free LPID.
    // Nothing is taken; the restart names MapLeaf 0, LPID 1's, and drops 2.
    {
        let (_file, pool) = lp_fixture().unwrap();
        pool.release_lpid(1).unwrap();
        let _held = pin(&pool, mapleaf(2), &Hold::nothing()).unwrap();
        assert_eq!(lp_alloc(&pool, 1, &[2]).unwrap(), restart(0, &[2]), "(b)");
        assert_eq!(pool.lpid_stats().unwrap(), stats(6, 1), "(b)");
        assert!(pool.lpid_is_free(1).unwrap(), "(b)");
    }
    // (c) L2 beside L1: MapLeafs 0 (LPID 1 free) and 2 (none free) held, two
    // LPIDs asked. One candidate is too few and 2 is emptied, so nothing is
    // taken; past the held MapLeafs, L3 would take `next_lpid` 6, under
    // MapLeaf 3.
    {
        let (_file, pool) = lp_fixture().unwrap();
        pool.release_lpid(1).unwrap();
        let _h0 = pin(&pool, mapleaf(0), &Hold::nothing()).unwrap();
        let _h2 = pin(&pool, mapleaf(2), &Hold::nothing()).unwrap();
        assert_eq!(
            lp_alloc(&pool, 2, &[0, 2]).unwrap(),
            restart(3, &[2]),
            "(c)"
        );
        assert_eq!(pool.lpid_stats().unwrap(), stats(6, 1), "(c)");
    }
    // (d) L2 with `next_lpid` under a held MapLeaf: MapLeaf 3 made and held
    // (it covers `next_lpid` 6, so LPIDs 6 and 7 count as free), MapLeaf 2
    // held and emptied, three asked. The restart names the MapLeaf of the
    // first LPID past 7, MapLeaf 4, never 3, which the caller holds.
    {
        let (_file, pool) = lp_fixture().unwrap();
        s3_make_mapleaf(&pool, 3).unwrap();
        let _h2 = pin(&pool, mapleaf(2), &Hold::nothing()).unwrap();
        let _h3 = pin(&pool, mapleaf(3), &Hold::nothing()).unwrap();
        assert_eq!(
            lp_alloc(&pool, 3, &[2, 3]).unwrap(),
            restart(4, &[2]),
            "(d)"
        );
        assert_eq!(pool.lpid_stats().unwrap(), stats(6, 0), "(d)");
    }
    // (e) L3 past L1's candidates from `next_lpid`: MapLeafs 3 and 4 made, 3
    // held, four asked. L1 counts 6 and 7; L3 adds 8 and 9, under MapLeaf 4,
    // which it pins. Each LPID once, and `next_lpid` becomes 10.
    {
        let (_file, pool) = lp_fixture().unwrap();
        s3_make_mapleaf(&pool, 3).unwrap();
        s3_make_mapleaf(&pool, 4).unwrap();
        let _h3 = pin(&pool, mapleaf(3), &Hold::nothing()).unwrap();
        assert_eq!(
            lp_alloc(&pool, 4, &[3]).unwrap(),
            taken(&[6, 7, 8, 9], &[4]),
            "(e)"
        );
        assert_eq!(pool.lpid_stats().unwrap(), stats(10, 0), "(e)");
    }
    // (f) `next_lpid` passes every LPID taken from it: MapLeaf 3 made and
    // held, LPID 1 free, three asked. L1's 6 and 7, then LPID 1 under MapLeaf
    // 0, which L3 pins; `next_lpid` becomes 8 although 1 comes last. The
    // next allocation then needs LPID 8, under MapLeaf 4, never made.
    {
        let (_file, pool) = lp_fixture().unwrap();
        s3_make_mapleaf(&pool, 3).unwrap();
        pool.release_lpid(1).unwrap();
        let _h3 = pin(&pool, mapleaf(3), &Hold::nothing()).unwrap();
        assert_eq!(
            lp_alloc(&pool, 3, &[3]).unwrap(),
            taken(&[6, 7, 1], &[0]),
            "(f)"
        );
        assert_eq!(pool.lpid_stats().unwrap(), stats(8, 0), "(f)");
        assert!(!pool.lpid_is_free(1).unwrap(), "(f)");
        assert_eq!(lp_alloc(&pool, 1, &[]).unwrap(), restart(4, &[]), "(f)");
    }
    // (h) The LPID bitmap grows for a released LPID past its length and
    // keeps every bit it had: LPID 2 released (the bitmap now covers 1..6),
    // MapLeaf 3 made and held, two LPIDs taken from `next_lpid` (6 and 7),
    // then 7 released, past the bitmap's length: 2 and 7 are free.
    {
        let (_file, pool) = lp_fixture().unwrap();
        pool.release_lpid(2).unwrap();
        s3_make_mapleaf(&pool, 3).unwrap();
        let _h3 = pin(&pool, mapleaf(3), &Hold::nothing()).unwrap();
        assert_eq!(
            lp_alloc(&pool, 2, &[3]).unwrap(),
            taken(&[6, 7], &[]),
            "(h)"
        );
        pool.release_lpid(7).unwrap();
        assert_eq!(pool.lpid_stats().unwrap(), stats(8, 2), "(h)");
        assert!(pool.lpid_is_free(2).unwrap(), "(h)");
        assert!(pool.lpid_is_free(7).unwrap(), "(h)");
        assert!(!pool.lpid_is_free(6).unwrap(), "(h)");
    }
    // (g) One call takes 1 to 64 LPIDs (Work 5): 0 and 65 are `Invalid`; 64
    // is answered (here a restart at MapLeaf 3, which is not made), and
    // nothing is taken.
    {
        let (_file, pool) = lp_fixture().unwrap();
        let g = pool.gate_shared().unwrap();
        assert!(is_invalid(&pool.alloc_lpids(0, &[], &g)), "(g) 0");
        assert!(is_invalid(&pool.alloc_lpids(65, &[], &g)), "(g) 65");
        drop(g);
        assert_eq!(lp_alloc(&pool, 64, &[]).unwrap(), restart(3, &[]), "(g) 64");
        assert_eq!(pool.lpid_stats().unwrap(), stats(6, 0), "(g)");
    }
}

/// `release_step6_pending_refused` (card A-3c, Work 4, S5; the implementer's
/// review): a slot that is already pending is not made pending again. Image
/// `k−1` never references a slot pending since the previous step 6 (one
/// image `k−2` referenced and `k−1` does not), and taking it would leave it
/// both free, by the old generation's move, and pending, so that `alloc`
/// could hand out a slot an image may still read. `Invalid`, changing
/// nothing.
#[test]
fn release_step6_pending_refused() {
    let alloc = SlotAlloc::<StdPrimitives>::new(
        PS,
        600,
        256..258,
        SlotInit::Given {
            free: vec![3],
            pending: vec![4],
        },
    )
    .unwrap();
    let before = alloc.stats().unwrap();
    assert!(is_invalid(&alloc.release_step6(&[5, 4])));
    assert_eq!(alloc.stats().unwrap(), before);
    assert_eq!(alloc.state(4).unwrap(), SlotUse::Pending);
    assert_eq!(alloc.state(5).unwrap(), SlotUse::Used);
    // The next release frees slot 4 once: free, and no longer pending.
    alloc.release_step6(&[5]).unwrap();
    assert_eq!(alloc.state(4).unwrap(), SlotUse::Free);
    assert_eq!(alloc.state(5).unwrap(), SlotUse::Pending);
    assert_eq!(
        alloc.stats().unwrap(),
        SlotStats {
            slot_count: 600,
            free: 2,
            pending: 1,
            growths: 0,
        }
    );
}

// ------------------------------------------------------------------ the map

/// `cmap_growth` (card A-3c, Work 14.1; S-STORE §4.1 "Growth"): fanout 2; the
/// root's level, the node count (the test's own count of distinct
/// positions), each grown leaf's level-1 MapInner and its stable id. Mutant
/// WB-GROW-INDEX-FROM-ROOT.
#[test]
fn cmap_growth() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 2, 600, 1, H_MIN_TEST).unwrap();
    assert_eq!(
        pool.map_shape().unwrap(),
        nucleus_store::buf::MapShape {
            root_level: 1,
            inners: 1
        }
    );
    let leaves = [5u64, 0, 13, 2, 40, 7, 63, 64, 1, 100];
    let mut grown: BTreeSet<u64> = BTreeSet::new();
    let mut parents: BTreeMap<u64, Option<u32>> = BTreeMap::new();
    for &x in &leaves {
        for pair in 0..2 {
            let before = pool.map_shape().unwrap();
            {
                let g = pool.gate_shared().unwrap();
                pool.grow_map(x, &g).unwrap();
            }
            grown.insert(x);
            let shape = pool.map_shape().unwrap();
            // The smallest L ≥ 1 with every grown leaf below 2^L.
            let root_level = (1u32..)
                .map(|l| u8::try_from(l).unwrap_or(u8::MAX))
                .find(|l| grown.iter().all(|&v| v < 2u64.pow(u32::from(*l))))
                .unwrap();
            assert_eq!(shape.root_level, root_level, "root level after {x}");
            let mut positions: BTreeSet<(u8, u64)> = BTreeSet::new();
            for l in 1..=root_level {
                positions.insert((l, 0));
            }
            for &v in &grown {
                for l in 1..root_level {
                    positions.insert((l, v >> l));
                }
            }
            assert_eq!(
                u64::from(shape.inners),
                positions.len() as u64,
                "inners after {x}: {positions:?}"
            );
            let id = pool.leaf_parent_id(x).unwrap();
            match parents.get(&x) {
                None => {
                    parents.insert(x, id);
                }
                Some(prev) => assert_eq!(&id, prev, "leaf {x}'s parent moved"),
            }
            if pair == 1 {
                // The second call of the pair changes nothing.
                assert_eq!(shape, before, "the second grow_map({x}) changed the map");
            }
        }
    }
    for (&x, id) in &parents {
        let g = pool.gate_shared().unwrap();
        let _ = g;
        let lp = pool.latch_leaf_parent(x).unwrap();
        assert_eq!((lp.level(), lp.index()), (1, x >> 1), "leaf {x}'s parent");
        drop(lp);
        assert_eq!(pool.leaf_parent_id(x).unwrap(), *id);
    }
}

/// `entry_roundtrip` (card A-3c, Work 14.1): twelve pages written and their
/// MapLeafs, every frame gone, each page loaded again through its entry; the
/// first pin under a MapLeaf reads the MapLeaf's home before the page's.
#[test]
fn entry_roundtrip() {
    let (file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    for x in 1..=12u64 {
        let (_x, p) = s3_new_page(&pool, x as u8).unwrap();
        drop(p);
    }
    for x in 1..=12u64 {
        evict_out(&pool, lpid(x)).unwrap();
    }
    for i in 0..=3u64 {
        evict_out(&pool, mapleaf(i)).unwrap();
    }
    for x in 1..=12u64 {
        assert_eq!(pool.frame_of(lpid(x)).unwrap(), None);
    }
    for i in 0..=3u64 {
        assert_eq!(pool.frame_of(mapleaf(i)).unwrap(), None);
    }
    for x in 1..=12u64 {
        let reads_before = file.reads().len();
        let m = pin_read(&pool, lpid(x)).unwrap();
        assert_eq!(m, x as u8, "LPID {x}'s marker");
        // MapLeaf i maps LPIDs [i·4, (i+1)·4); LPID 0 is null, so the first
        // page under MapLeaf 0 is LPID 1.
        if x == 1 || x % 4 == 0 {
            // The first pin under MapLeaf x / 4: two reads, the MapLeaf's
            // home, then the page's.
            let reads = file.reads();
            let mine: Vec<u64> = reads.iter().skip(reads_before).map(|r| r.0).collect();
            assert_eq!(mine.len(), 2, "reads for LPID {x}");
            let mleaf = home_in_log(&file, 6, x / 4).unwrap();
            let page = home_in_log(&file, 3, x).unwrap();
            assert_eq!(mine, vec![mleaf * u64::from(PS), page * u64::from(PS)]);
        }
        assert_eq!(
            pool.entry_of(x).unwrap().slot,
            home_in_log(&file, 3, x).unwrap(),
            "LPID {x}'s entry"
        );
    }
}

// ------------------------------------------------------------- the eviction

/// One record of a dirty eviction's steps, taken by the recording hook of
/// `evict_steps` at `EvictWrite` and `EvictEntrySet`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Step {
    point: HookPoint,
    entry: Ref,
    frame: Option<nucleus_store::buf::FrameId>,
    page: MapPinInfo,
    mapleaf: MapPinInfo,
    prev: Option<SlotUse>,
    writes: u64,
}

/// The recording hook of `evict_steps` for `Lpid(1)`: at `EvictWrite` and
/// `EvictEntrySet` it records the entry, the frame, §4.1's pins of the page
/// and of its MapLeaf, the use of the previous home and the write count.
/// Each call takes nothing the firing thread holds: it holds `x`'s latch and
/// the gate, not MapLeaf 0's latch.
fn step_hook(
    pool: &'static Pool<WbFile, StdPrimitives>,
    records: Arc<Mutex<Vec<Step>>>,
    prev: Arc<Mutex<Option<u64>>>,
) -> nucleus_store::buf::Hook {
    Arc::new(move |point, key, frame| {
        if !matches!(point, HookPoint::EvictWrite | HookPoint::EvictEntrySet) || key != lpid(1) {
            return;
        }
        let entry = pool.entry_of(1).unwrap_or(Ref::NULL);
        let held = pool.frame_of(lpid(1)).unwrap_or(None);
        let page = pool.map_pin_info(frame).unwrap_or(MapPinInfo {
            dirty_pins: 0,
            holds_dirty_pin: false,
        });
        let mapleaf = pool
            .frame_of(mapleaf(0))
            .ok()
            .flatten()
            .and_then(|f0| pool.map_pin_info(f0).ok())
            .unwrap_or(MapPinInfo {
                dirty_pins: 0,
                holds_dirty_pin: false,
            });
        let prev_use = lock(&prev).and_then(|h| pool.slot_use(h).ok());
        let writes = pool.wb_writes().unwrap_or(u64::MAX);
        lock(&records).push(Step {
            point,
            entry,
            frame: held,
            page,
            mapleaf,
            prev: prev_use,
            writes,
        });
    })
}

/// The two records of one dirty eviction of `Lpid(1)` in `evict_steps`.
fn two_steps(records: &Mutex<Vec<Step>>) -> (Step, Step) {
    let got = lock(records);
    assert_eq!(
        got.len(),
        2,
        "one EvictWrite and one EvictEntrySet: {got:?}"
    );
    let (first, second) = (got[0].clone(), got[1].clone());
    assert_eq!(first.point, HookPoint::EvictWrite);
    assert_eq!(second.point, HookPoint::EvictEntrySet);
    (first, second)
}

/// `evict_steps` (a recording hook, no pause; card A-3c, Work 14.1). `n` 1,
/// `F` 18, fanout 4. Page `x` = LPID 1 under MapLeaf 0. Round 1 (never
/// written), round 2 (the first home superseded at `e`), round 3 (after a
/// flip, the home of the previous epoch stays), and the error case of a
/// failed write. Mutants WB-SUPERSEDE-NEVER, WB-ENTRY-BEFORE-WRITE.
#[test]
fn evict_steps() {
    assert_eq!(min_f(1), 18);
    let (file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let pool: &'static Pool<WbFile, StdPrimitives> = Box::leak(Box::new(pool));
    let records = Arc::new(Mutex::new(Vec::new()));
    let prev = Arc::new(Mutex::new(None));
    pool.set_hook(Some(step_hook(
        pool,
        Arc::clone(&records),
        Arc::clone(&prev),
    )));

    // Round 1: never written.
    let (x, p) = s3_new_page(pool, 0xA1).unwrap();
    assert_eq!(x, 1);
    let fx = p.frame();
    drop(p);
    evict_out(pool, lpid(1)).unwrap();
    let (first, second) = two_steps(&records);
    assert_eq!(first.entry, Ref::NULL);
    assert_eq!(first.frame, Some(fx));
    assert_eq!(
        first.page,
        MapPinInfo {
            dirty_pins: 0,
            holds_dirty_pin: true
        }
    );
    assert_eq!(
        first.mapleaf,
        MapPinInfo {
            dirty_pins: 1,
            holds_dirty_pin: false
        }
    );
    assert_eq!((first.prev, first.writes), (None, 0));
    let h1 = home_in_log(&file, 3, 1).unwrap();
    assert_eq!((second.entry.slot, second.entry.epoch), (h1, 1));
    assert_eq!(second.frame, Some(fx));
    assert_eq!(
        (second.page.holds_dirty_pin, second.mapleaf.dirty_pins),
        (false, 0)
    );
    assert_eq!((second.prev, second.writes), (None, 1));
    assert_eq!(pool.frame_of(lpid(1)).unwrap(), None);

    // Round 2: written at h1, touched, evicted again: h1 is freed at once.
    lock(&records).clear();
    *lock(&prev) = Some(h1);
    {
        let p = pin(pool, lpid(1), &Hold::nothing()).unwrap();
        s3_touch(pool, &p, 0xA2).unwrap();
    }
    evict_out(pool, lpid(1)).unwrap();
    let (first, second) = two_steps(&records);
    assert_eq!((first.entry.slot, first.entry.epoch), (h1, 1));
    assert_eq!(first.prev, Some(SlotUse::Used));
    assert_eq!(first.writes, 1);
    let h2 = second.entry.slot;
    assert_ne!(h2, h1);
    assert_eq!(second.entry.epoch, 1);
    assert_eq!(second.prev, Some(SlotUse::Free), "h1's epoch is e");
    assert_eq!(second.writes, 2);
    assert_eq!(pool.slot_use(h1).unwrap(), SlotUse::Free);

    // Round 3: MapLeaf 0 written, a flip, then x again: h2 keeps epoch 1.
    lock(&records).clear();
    *lock(&prev) = Some(h2);
    evict_out(pool, mapleaf(0)).unwrap();
    assert_eq!(s1_flip(pool).unwrap(), 2);
    {
        let p = pin(pool, lpid(1), &Hold::nothing()).unwrap();
        s3_touch(pool, &p, 0xA3).unwrap();
    }
    evict_out(pool, lpid(1)).unwrap();
    let (_, second) = two_steps(&records);
    assert_eq!(second.entry.epoch, 2);
    assert_eq!(second.prev, Some(SlotUse::Used), "epoch 1 < 2");
    // Three writes of x and one of MapLeaf 0.
    assert_eq!(second.writes, 4);
    assert_eq!(pool.slot_use(h2).unwrap(), SlotUse::Used);
    assert_eq!(pin_read(pool, lpid(1)).unwrap(), 0xA3);

    // The error case, on a fresh fixture: the write fails at the slot the
    // oracle predicts; nothing else changes; cleared, the eviction goes on.
    let (file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let oracle = wb_support::oracle::WbOracle::new(PS, 600, 1);
    let (x, p) = s3_new_page(&pool, 0xB1).unwrap();
    assert_eq!(x, 1);
    let fx = p.frame();
    drop(p);
    let at = oracle.predict_alloc() * u64::from(PS);
    assert_eq!(at, 3 * u64::from(PS));
    file.fail_write_at(Some(at));
    assert!(matches!(evict(&pool, lpid(1)), Err(StoreError::Env(_))));
    assert_eq!(pool.frame_of(lpid(1)).unwrap(), Some(fx));
    assert_eq!(
        pool.map_pin_info(fx).unwrap(),
        MapPinInfo {
            dirty_pins: 0,
            holds_dirty_pin: true
        }
    );
    assert_eq!(pool.entry_of(1).unwrap(), Ref::NULL);
    assert_eq!(pool.wb_writes().unwrap(), 0);
    assert!(file.writes().is_empty());
    file.fail_write_at(None);
    assert!(matches!(
        evict(&pool, lpid(1)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
}

/// The body of `mapleaf_two_step` and its variant (card A-3c, Work 14.1):
/// MapLeaf 0 with page `x` written, dirty and unpinned; a recording hook at
/// `EvictEntrySet` for `MapLeaf(0)`; with `keep_pin`, the hook also takes a
/// `pin_resident` on the MapLeaf and keeps it. Mutants
/// WB-MAPLEAF-UNPINNED-GAP, MAP-DROP-BEFORE-REF.
fn two_step_body(keep_pin: bool) -> Result<(), StoreError> {
    let (file, pool) =
        wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).map_err(StoreError::Invalid)?;
    let pool: &'static Pool<WbFile, StdPrimitives> = Box::leak(Box::new(pool));
    let (x, p) = s3_new_page(pool, 0xC1)?;
    assert_eq!(x, 1);
    drop(p);
    evict_out(pool, lpid(x))?;
    let f_m = frame_of(pool, mapleaf(0))?;
    type Record = (
        Option<nucleus_store::buf::FrameId>,
        Option<nucleus_store::buf::FrameId>,
        Ref,
    );
    let records = Arc::new(Mutex::new(Vec::<Record>::new()));
    let kept: Arc<Mutex<Option<Pin<'static, WbFile, StdPrimitives>>>> = Arc::new(Mutex::new(None));
    {
        let records = Arc::clone(&records);
        let kept = Arc::clone(&kept);
        pool.set_hook(Some(Arc::new(move |point, key, _| {
            if point != HookPoint::EvictEntrySet || key != mapleaf(0) {
                return;
            }
            // A granted guard is dropped at once.
            let try_victim = match pool.try_victim(f_m) {
                Ok(Some(v)) => {
                    let f = v.frame();
                    drop(v);
                    Some(f)
                }
                _ => None,
            };
            let frame = pool.frame_of(mapleaf(0)).unwrap_or(None);
            let home = pool
                .latch_leaf_parent(0)
                .map(|lp| lp.leaf_ref())
                .unwrap_or(Ref::NULL);
            lock(&records).push((try_victim, frame, home));
            if keep_pin && let Ok(Some(p)) = pool.pin_resident(mapleaf(0)) {
                *lock(&kept) = Some(p);
            }
        })));
    }
    let out = evict(pool, mapleaf(0))?;
    let home = home_in_log(&file, 6, 0)
        .ok_or_else(|| StoreError::Invalid("MapLeaf 0 was not written".to_string()))?;
    let got = lock(&records).clone();
    assert_eq!(got.len(), 1, "one EvictEntrySet for MapLeaf 0: {got:?}");
    assert_eq!(
        (got[0].0, got[0].1),
        (None, Some(f_m)),
        "try_victim, frame_of"
    );
    assert_eq!(
        (got[0].2.slot, got[0].2.epoch),
        (home, 1),
        "the parent's ref"
    );
    if keep_pin {
        assert!(matches!(out, EvictOutcome::Kept), "{out:?}");
        let reads_before = file.reads().len();
        let p = lock(&kept).take();
        assert!(p.is_some(), "the hook kept a pin");
        drop(p);
        let frame = frame_of(pool, mapleaf(0))?;
        let info = pool.frame_info(frame)?;
        assert_eq!(
            (info.state, info.pins),
            (nucleus_store::buf::FrameState::Resident, 0)
        );
        assert_eq!(file.reads().len(), reads_before, "no read in between");
        // Clean: it leaves with no write.
        let writes = file.writes().len();
        evict_out(pool, mapleaf(0))?;
        assert_eq!(file.writes().len(), writes);
    } else {
        assert!(matches!(out, EvictOutcome::Obtained(_)), "{out:?}");
    }
    assert_eq!(pool.frame_of(mapleaf(0))?, None);
    // MapLeaf 0 from its home, then x from its entry.
    let reads_before = file.reads().len();
    assert_eq!(pin_read(pool, lpid(1))?, 0xC1);
    let x_home = home_in_log(&file, 3, 1)
        .ok_or_else(|| StoreError::Invalid("x was not written".to_string()))?;
    let mine: Vec<u64> = file
        .reads()
        .iter()
        .skip(reads_before)
        .map(|r| r.0)
        .collect();
    assert_eq!(mine, vec![home * u64::from(PS), x_home * u64::from(PS)]);
    Ok(())
}

/// `mapleaf_two_step` (card A-3c, Work 14.1; S-STORE §4.1's two-step write).
#[test]
fn mapleaf_two_step() {
    assert_eq!(min_f(1), 18);
    two_step_body(false).unwrap();
    two_step_body(true).unwrap();
}

/// `mapleaf_pinned_under_latch` (card A-3c, Work 7's E6; S-STORE §4.1's
/// two-step write: "keeping the frame pinned and in the page table; it then
/// releases the page's latch"; verdict A-3c, A-3c-F6's SW-E13). MapLeaf 0
/// with page `x` written, dirty and unpinned, evicted with a recording hook
/// at the first `TableLocking` after its `EvictWrite`: E6's pin of the
/// frame, the table hold after the write (a MapLeaf ends no §4.1 pin at its
/// `mark_written`). There the victim's latch is still held, so
/// `try_victim(f_M)` is `None`. With the latch released before the pin, the
/// clean, unpinned, unlatched frame is a victim there: another evictor could
/// take it while its parent's ref still names the old home, and E6 would
/// pin a frame that is no longer the MapLeaf's.
#[test]
fn mapleaf_pinned_under_latch() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let pool: &'static Pool<WbFile, StdPrimitives> = Box::leak(Box::new(pool));
    let (x, p) = s3_new_page(pool, 0xC5).unwrap();
    assert_eq!(x, 1);
    drop(p);
    evict_out(pool, lpid(x)).unwrap();
    let f_m = frame_of(pool, mapleaf(0)).unwrap();
    // `None`: not yet armed; `Some(false)`: armed by MapLeaf 0's
    // `EvictWrite`; `Some(true)`: the first `TableLocking` since was taken.
    let armed = Arc::new(Mutex::new(None::<bool>));
    let records = Arc::new(Mutex::new(Vec::<Option<nucleus_store::buf::FrameId>>::new()));
    {
        let armed = Arc::clone(&armed);
        let records = Arc::clone(&records);
        pool.set_hook(Some(Arc::new(move |point, key, _| {
            if point == HookPoint::EvictWrite && key == mapleaf(0) {
                *lock(&armed) = Some(false);
                return;
            }
            if point != HookPoint::TableLocking {
                return;
            }
            {
                let mut a = lock(&armed);
                if *a != Some(false) {
                    return;
                }
                // Disarmed before `try_victim`, whose own table hold fires
                // this point again.
                *a = Some(true);
            }
            // A granted guard is dropped at once.
            let got = match pool.try_victim(f_m) {
                Ok(Some(v)) => {
                    let f = v.frame();
                    drop(v);
                    Some(f)
                }
                _ => None,
            };
            lock(&records).push(got);
        })));
    }
    let out = evict(pool, mapleaf(0)).unwrap();
    pool.set_hook(None);
    assert_eq!(*lock(&armed), Some(true), "the hook was reached");
    assert_eq!(
        lock(&records).clone(),
        vec![None],
        "try_victim at E6's pin: the latch is still held"
    );
    assert!(matches!(out, EvictOutcome::Obtained(_)), "{out:?}");
    assert_eq!(pool.frame_of(mapleaf(0)).unwrap(), None);
    assert_eq!(pin_read(pool, lpid(1)).unwrap(), 0xC5);
}

/// `mappin_residency` (card A-3c, Work 14.1; S-STORE §4.1 "Residency"): a
/// MapLeaf stays pinned while any page it maps is dirty. Mutant
/// WB-MAPPIN-NOT-TAKEN.
#[test]
fn mappin_residency() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let (x, p) = s3_new_page(&pool, 0x51).unwrap();
    drop(p);
    let f0 = frame_of(&pool, mapleaf(0)).unwrap();
    assert!(matches!(
        pool.evict_frame(f0, &Hold::nothing()).unwrap(),
        EvictOutcome::NotVictim
    ));
    assert_eq!(pool.map_pin_info(f0).unwrap().dirty_pins, 1);
    assert!(matches!(
        evict(&pool, lpid(x)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    assert_eq!(pool.map_pin_info(f0).unwrap().dirty_pins, 0);
    assert!(matches!(
        evict(&pool, mapleaf(0)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    // Two dirty pages under one MapLeaf.
    let (y, p) = s3_new_page(&pool, 0x52).unwrap();
    drop(p);
    let (z, p) = s3_new_page(&pool, 0x53).unwrap();
    drop(p);
    let f0 = frame_of(&pool, mapleaf(0)).unwrap();
    assert_eq!(pool.map_pin_info(f0).unwrap().dirty_pins, 2);
    evict_out(&pool, lpid(y)).unwrap();
    assert_eq!(pool.map_pin_info(f0).unwrap().dirty_pins, 1);
    assert!(matches!(
        pool.evict_frame(f0, &Hold::nothing()).unwrap(),
        EvictOutcome::NotVictim
    ));
    evict_out(&pool, lpid(z)).unwrap();
    assert_eq!(pool.map_pin_info(f0).unwrap().dirty_pins, 0);
    assert!(matches!(
        evict(&pool, mapleaf(0)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
}

/// `dirty_needs_resident_mapleaf` (card A-3c, Work 6.2): a clean page whose
/// MapLeaf is not resident is not dirtied, and a creation under a cold
/// MapLeaf changes nothing. Mutant WB-DIRTY-COLD-IGNORED.
#[test]
fn dirty_needs_resident_mapleaf() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let (x, p) = s3_new_page(&pool, 0x11).unwrap();
    assert_eq!(x, 1);
    drop(p);
    evict_out(&pool, lpid(x)).unwrap();
    // x again, clean; MapLeaf 0 evicted.
    let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
    let fx = p.frame();
    drop(p);
    evict_out(&pool, mapleaf(0)).unwrap();
    assert_eq!(pool.frame_of(mapleaf(0)).unwrap(), None);
    let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
    {
        let mut g = p.latch_exclusive().unwrap();
        assert!(!g.is_dirty());
        assert!(is_invalid(&g.mark_dirty(1)));
        assert!(!g.is_dirty(), "the page stays clean");
    }
    assert_eq!(
        pool.map_pin_info(fx).unwrap(),
        MapPinInfo {
            dirty_pins: 0,
            holds_dirty_pin: false
        }
    );
    // LPID 9 lies under MapLeaf 2, which was never made.
    let free_before = pool.stats().unwrap().free;
    let res = pool.reserve().unwrap();
    assert!(is_invalid(&pool.create(res, lpid(9), Kind::Leaf, 1)));
    assert_eq!(pool.stats().unwrap().free, free_before);
    assert_eq!(pool.frame_of(lpid(9)).unwrap(), None);
}

/// `touch_check_table` (card A-3c, Work 6.4), each case after `s1_flip`:
/// (a) a clean MapLeaf at the old epoch may be mutated; (b) a dirty one may
/// not, and nothing changes; (c) once mutated, it goes on. Mutant
/// WB-TOUCH-SKIP.
#[test]
fn touch_check_table() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    // LPIDs 1..5: x = 1 under MapLeaf 0, y = 5 under MapLeaf 1.
    let mut pages = Vec::new();
    for m in 0x21..=0x25u8 {
        let (v, p) = s3_new_page(&pool, m).unwrap();
        pages.push(v);
        drop(p);
    }
    assert_eq!(pages, vec![1, 2, 3, 4, 5]);
    let (x, y) = (1u64, 5u64);
    // Every page written: both MapLeafs dirty at content epoch 1.
    for &v in &pages {
        evict_out(&pool, lpid(v)).unwrap();
    }
    // LPIDs 2, 3 and 4 freed before the flip, so that (c)'s page lies under
    // MapLeaf 0; then MapLeaf 0 written, which leaves MapLeaf 1 dirty.
    for v in [2u64, 3, 4] {
        s3_free(&pool, v, 4).unwrap();
    }
    evict_out(&pool, mapleaf(0)).unwrap();
    assert_eq!(s1_flip(&pool).unwrap(), 2);
    // (b) y touched under the dirty MapLeaf 1: the eviction is refused and
    // nothing changes.
    {
        let p = pin(&pool, lpid(y), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x23).unwrap();
    }
    let entry_before = pool.entry_of(y).unwrap();
    assert_ne!(entry_before.slot, 0);
    assert!(
        is_invalid(&evict(&pool, lpid(y))),
        "a dirty MapLeaf from the old epoch"
    );
    {
        let p = pin(&pool, lpid(y), &Hold::nothing()).unwrap();
        let g = p.latch_shared().unwrap();
        assert!(g.is_dirty(), "y is still dirty");
        assert_eq!(g.content_epoch(), 2);
    }
    assert_eq!(pool.entry_of(y).unwrap(), entry_before);
    // (a) x touched and evicted under MapLeaf 0, clean at content epoch 1:
    // the mutation goes on, and MapLeaf 0 is then dirty at content epoch 2.
    {
        let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x24).unwrap();
    }
    assert!(matches!(
        evict(&pool, lpid(x)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    {
        let p = pin(&pool, mapleaf(0), &Hold::nothing()).unwrap();
        let g = p.latch_exclusive().unwrap();
        assert!(g.is_dirty(), "MapLeaf 0 is dirty");
        assert_eq!(g.content_epoch(), 2, "at content epoch 2");
    }
    // (c) z under MapLeaf 0 (the freed LPID 2, the lowest free): touched and
    // evicted.
    let (z, p) = s3_new_page(&pool, 0x26).unwrap();
    assert_eq!(z, 2);
    drop(p);
    {
        let p = pin(&pool, lpid(z), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x26).unwrap();
    }
    assert!(matches!(
        evict(&pool, lpid(z)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    assert_eq!(pin_read(&pool, lpid(z)).unwrap(), 0x26);
}

/// `map_mutation_after_flip` (card A-3c, Work 3.5, 3.7 and 6.4, against
/// S-STORE 7.23 §5.6 "When it copies": "It never copies a MapInner"; the
/// implementer's review, ESCALATE 3). `n` 1, `F` 18, fanout 4, `slot_count`
/// 600. After an S1 flip a MapInner's ref and children are set with no
/// touch check, though every MapInner made before the flip is dirty at
/// content epoch 1. (a) MapLeaf 0, mutated in the new epoch, is evicted by
/// §4.1's two steps through its parent MapInner (1, 0): `Obtained`, the
/// parent's ref names the new home at epoch 2, and page `x` reloads with
/// its last marker through it. (b) The map grows under the root (2, 0),
/// made before the flip: MapLeaf 9's MapInner (1, 2) is made. A touch check
/// on a MapInner refuses both, (a) after `mark_written`, which left the
/// MapLeaf clean with its parent's ref on the old home.
#[test]
fn map_mutation_after_flip() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let (x, p) = s3_new_page(&pool, 0x31).unwrap();
    assert_eq!(x, 1);
    drop(p);
    // MapLeaf 5 needs level 2: the root (2, 0) is made in epoch 1.
    s3_make_mapleaf(&pool, 5).unwrap();
    assert_eq!(pool.map_shape().unwrap().root_level, 2);
    evict_out(&pool, lpid(x)).unwrap();
    evict_out(&pool, mapleaf(0)).unwrap();
    assert_eq!(s1_flip(&pool).unwrap(), 2);
    {
        let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x32).unwrap();
    }
    // x's entry is set in MapLeaf 0, clean at content epoch 1 (S1's
    // clause): MapLeaf 0 is then dirty at content epoch 2.
    assert!(matches!(
        evict(&pool, lpid(x)).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    // (a)
    assert!(
        matches!(evict(&pool, mapleaf(0)).unwrap(), EvictOutcome::Obtained(_)),
        "a MapLeaf eviction after the flip"
    );
    assert_eq!(pool.frame_of(mapleaf(0)).unwrap(), None);
    let leaf_ref = pool.latch_leaf_parent(0).unwrap().leaf_ref();
    assert_eq!(leaf_ref.epoch, 2, "the parent's ref names the new home");
    assert_eq!(pin_read(&pool, lpid(x)).unwrap(), 0x32);
    // (b)
    s3_make_mapleaf(&pool, 9).unwrap();
    assert_eq!(pool.leaf_parent_id(9).unwrap(), Some(2));
}

/// `evict_prev_epoch` (card A-3c, Work 7, E2): a dirty page from before the
/// flip is passed over until A-3c-C writes it. Mutant
/// WB-PREV-EPOCH-EVICTED.
#[test]
fn evict_prev_epoch() {
    assert_eq!(min_f(1), 18);
    let (file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let (x, p) = s3_new_page(&pool, 0x31).unwrap();
    drop(p);
    assert_eq!(s1_flip(&pool).unwrap(), 2);
    assert!(matches!(
        evict(&pool, lpid(x)).unwrap(),
        EvictOutcome::PrevEpoch
    ));
    assert!(file.writes().is_empty());
    let f = frame_of(&pool, lpid(x)).unwrap();
    let p = pool.pin_resident(lpid(x)).unwrap().unwrap();
    let g = p.latch_shared().unwrap();
    assert!(g.is_dirty());
    assert_eq!(g.content_epoch(), 1);
    assert_eq!(p.frame(), f);
}

/// `pin_hook_kinds` (card A-3c, Work 6.5): §4.1's pins fire `DirtyPinned` and
/// `DirtyUnpinned`, never `Pinned` or `Unpinned`, and an eviction fires no
/// `Pinned`; L3's residency pins, an operation's own, fire `Pinned` and
/// `Unpinned`; a revived freed page's §4.1 pin fires `DirtyPinned` once.
/// Mutants WB-DIRTYPIN-HOOKED, WB-EVICT-PIN-HOOKED; L3's pins taken quiet,
/// and `DirtyPinned` fired twice at a revival (both found in review).
#[test]
fn pin_hook_kinds() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let pool: &'static Pool<WbFile, StdPrimitives> = Box::leak(Box::new(pool));
    type Counters = HashMap<PageKey, [u64; 4]>;
    let counts: Arc<Mutex<Counters>> = Arc::new(Mutex::new(HashMap::new()));
    {
        let counts = Arc::clone(&counts);
        pool.set_hook(Some(Arc::new(move |point, key, _| {
            let at = match point {
                HookPoint::Pinned => 0,
                HookPoint::Unpinned => 1,
                HookPoint::DirtyPinned => 2,
                HookPoint::DirtyUnpinned => 3,
                _ => return,
            };
            lock(&counts).entry(key).or_default()[at] += 1;
        })));
    }
    // `exempt` is the one key whose pin the test holds at this step.
    let check = |step: &str, x_dirty: bool, exempt: Option<PageKey>| {
        let counts = lock(&counts).clone();
        for (key, c) in &counts {
            assert_eq!(
                c[0] - c[1],
                u64::from(exempt == Some(*key)),
                "{step}: live operation pins on {key:?}"
            );
        }
        let leaf0 = counts.get(&mapleaf(0)).copied().unwrap_or([0; 4]);
        assert_eq!(
            leaf0[2] - leaf0[3],
            u64::from(x_dirty),
            "{step}: live §4.1 pins on MapLeaf 0"
        );
        for (key, c) in &counts {
            if *key != mapleaf(0) {
                assert_eq!((c[2], c[3]), (0, 0), "{step}: no §4.1 pin on {key:?}");
            }
        }
    };
    let (x, p) = s3_new_page(pool, 0x41).unwrap();
    assert_eq!(x, 1);
    drop(p);
    check("after s3_new_page's pin is dropped", true, None);
    let evictions_pinned: usize;
    {
        let before = lock(&counts).values().map(|c| c[0]).sum::<u64>();
        evict_out(pool, lpid(x)).unwrap();
        evict_out(pool, mapleaf(0)).unwrap();
        let after = lock(&counts).values().map(|c| c[0]).sum::<u64>();
        evictions_pinned = usize::try_from(after - before).unwrap_or(usize::MAX);
    }
    assert_eq!(evictions_pinned, 0, "an eviction fired Pinned");
    check("after both evictions", false, None);
    let p = pin(pool, lpid(x), &Hold::nothing()).unwrap();
    assert_eq!(pin_read(pool, lpid(x)).unwrap(), 0x41);
    check("while pin_page(x)'s pin is held", false, Some(lpid(x)));
    drop(p);
    check("after the pin is dropped", false, None);

    // L3's residency pins are an operation's own (Work 6.5): an allocation
    // L3 serves under MapLeaf 0, which it pins, fires `Pinned` for it once,
    // and the pin's drop `Unpinned`.
    {
        let before = lock(&counts).get(&mapleaf(0)).copied().unwrap_or([0; 4]);
        let g = pool.gate_shared().unwrap();
        let (lpids, pins) = match pool.alloc_lpids(1, &[], &g).unwrap() {
            LpidAlloc::Taken { lpids, pins } => (lpids, pins),
            LpidAlloc::Restart(r) => panic!("the allocation restarted at {}", r.leaf),
        };
        assert_eq!(lpids, vec![2]);
        assert_eq!(
            pins.iter().map(|p| p.key()).collect::<Vec<_>>(),
            vec![mapleaf(0)]
        );
        let during = lock(&counts).get(&mapleaf(0)).copied().unwrap_or([0; 4]);
        assert_eq!(during[0] - before[0], 1, "L3's pin fired Pinned once");
        check(
            "while L3's pin on MapLeaf 0 is held",
            false,
            Some(mapleaf(0)),
        );
        drop(pins);
        drop(g);
        check("after L3's pin is dropped", false, None);
    }

    // A creation that revives a freed page's frame (`create`'s freed
    // branch) takes §4.1's pin once and fires `DirtyPinned` once.
    s3_free(pool, x, 4).unwrap();
    check("after s3_free(x)", false, None);
    let (y, p) = s3_new_page(pool, 0x42).unwrap();
    assert_eq!(y, x, "the lowest free LPID is x again");
    check("while the revived page's pin is held", true, Some(lpid(x)));
    drop(p);
    check("after the revived page's pin is dropped", true, None);
}

/// `s1_records_root` (card A-3c, Work 14.1): S1 records the root and its
/// level and advances the epoch; at `u32::MAX` the epoch refuses to move.
/// Mutants WB-EPOCH-NOT-ADVANCED, WB-ROOT-NOT-RECORDED.
#[test]
fn s1_records_root() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 2, 600, 1, H_MIN_TEST).unwrap();
    {
        let g = pool.gate_shared().unwrap();
        pool.grow_map(5, &g).unwrap();
    }
    assert_eq!(pool.map_shape().unwrap().root_level, 3);
    assert_eq!(s1_flip(&pool).unwrap(), 2);
    assert_eq!(
        pool.image_root().unwrap(),
        Some(ImageRoot { image: 1, level: 3 })
    );
    assert_eq!(s1_flip(&pool).unwrap(), 3);
    assert_eq!(
        pool.image_root().unwrap(),
        Some(ImageRoot { image: 2, level: 3 })
    );
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 2, 600, u32::MAX, H_MIN_TEST).unwrap();
    let mut flip = pool.gate_exclusive().unwrap();
    assert!(is_invalid(&flip.advance_epoch()));
    assert_eq!(pool.epoch().unwrap(), u32::MAX);
}

/// `gate_try_table` (card A-3c, Work 14.1; G1, G2): shared holds are
/// compatible, an exclusive hold is granted at once when nobody holds the
/// gate, and refuses every try while it lives.
#[test]
fn gate_try_table() {
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let exclusive_waits = Arc::new(Mutex::new(0usize));
    {
        let exclusive_waits = Arc::clone(&exclusive_waits);
        pool.set_hook(Some(Arc::new(move |point, _, _| {
            if point == HookPoint::GateExclusiveWaiting {
                *lock(&exclusive_waits) += 1;
            }
        })));
    }
    let a = pool.gate_try_shared().unwrap();
    let b = pool.gate_try_shared().unwrap();
    assert!(a.is_some() && b.is_some(), "shared holds are compatible");
    drop((a, b));
    let flip = pool.gate_exclusive().unwrap();
    assert_eq!(*lock(&exclusive_waits), 0, "granted at once");
    assert!(pool.gate_try_shared().unwrap().is_none());
    drop(flip);
    assert!(pool.gate_try_shared().unwrap().is_some());
}

/// `writeback_absent` (card A-3c, Notes OP-8): a pool built with `Pool::new`
/// refuses every write-back call, creates pages without §4.1's pin, and has
/// a gate.
#[test]
fn writeback_absent() {
    let file = Arc::new(WbFile::new().unwrap());
    let pool = Pool::new_below_minimum_for_tests(
        Arc::clone(&file),
        PoolConfig {
            frames: 3,
            page_size: PS,
            callers: 2,
        },
    )
    .unwrap();
    let g = pool.gate_try_shared().unwrap();
    assert!(g.is_some());
    let g = g.unwrap();
    assert!(is_invalid(&pool.pin_page(lpid(1), &Hold::nothing())));
    assert!(is_invalid(&pool.alloc_lpids(1, &[], &g)));
    assert!(is_invalid(&pool.grow_map(0, &g)));
    assert!(is_invalid(&pool.latch_leaf_parent(0)));
    assert!(is_invalid(&pool.set_entry(1, Ref::NULL, &g)));
    assert!(is_invalid(&pool.supersede(Ref::NULL, &g)));
    assert!(is_invalid(&pool.release_step6(&[])));
    assert!(is_invalid(&pool.release_lpid(1)));
    let res = pool.reserve().unwrap();
    let made = pool.create(res, lpid(2), Kind::Leaf, 1).unwrap();
    let nucleus_store::buf::CreateOutcome::Created(c) = made else {
        panic!("Lpid(2) is new");
    };
    assert_eq!(
        pool.map_pin_info(c.frame()).unwrap(),
        MapPinInfo {
            dirty_pins: 0,
            holds_dirty_pin: false
        }
    );
    assert!(c.is_dirty());
    // The dirty frame is not written: the eviction is refused.
    let fx = c.frame();
    drop(c);
    assert!(is_invalid(&pool.evict_frame(fx, &Hold::nothing())));
}

/// `freed_frame_not_written` (card A-3c, Work 14.1; S-STORE §4.2 "Freeing":
/// "no eviction (§5.2) and no step 2 … ever writes it"). Mutant
/// WB-EVICT-FREED-WRITTEN.
#[test]
fn freed_frame_not_written() {
    assert_eq!(min_f(1), 18);
    let (file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let (x, p) = s3_new_page(&pool, 0x71).unwrap();
    drop(p);
    evict_out(&pool, lpid(x)).unwrap();
    {
        let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x72).unwrap();
    }
    s3_free(&pool, x, 4).unwrap();
    let writes = file.writes().len();
    let f = frame_of(&pool, lpid(x)).unwrap();
    assert!(matches!(
        pool.evict_frame(f, &Hold::nothing()).unwrap(),
        EvictOutcome::Obtained(_)
    ));
    assert_eq!(file.writes().len(), writes);
    assert!(pool.entry_of(x).unwrap().is_null());
    assert!(pool.lpid_is_free(x).unwrap());
}

/// `callers_bounded_by_latch_capacity` (per verdict A-3b 569a6fb2, follow-up
/// A-3b-F6): a pool whose callers reach the frame latches' waiter capacity
/// plus one is refused with `Invalid`, before any allocation.
#[test]
fn callers_bounded_by_latch_capacity() {
    let callers = 65_536u32;
    let frames = f_min(callers, nucleus_store::buf::H_MIN);
    let file = Arc::new(WbFile::new().unwrap());
    let r = Pool::new(
        Arc::clone(&file),
        PoolConfig {
            frames,
            page_size: PS,
            callers,
        },
    );
    assert!(
        is_invalid(&r),
        "65,536 callers are above the capacity of 65,535"
    );
}
