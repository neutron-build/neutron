//! The write-back pool (card A-3c, Work 8.1 and 9; S-STORE §4.1–§4.5, §5.2,
//! §5.6): what a caller holds while it pins or reserves, the restart a
//! pending flip asks for, the configuration a write-back pool is built with,
//! and the pool's write-back state (the current map, the slot allocator, the
//! LPID state and the write counter).
//!
//! Invariants owned here:
//! - **Write-back only in pools built with it** (Notes OP-8): a pool from
//!   [`Pool::new`] has a gate at epoch 1 and nothing else; its frame
//!   acquisition never writes, it takes no §4.1 pins, and every write-back
//!   call on it is `Invalid`. A-3a's and A-3b's suites run on such pools.
//! - **The flip's restart** (S-STORE §5.2: "If no victim is clean, the
//!   thread releases its latches and restarts ... a thread that has
//!   published a page as being loaded ends that load first, serving
//!   nothing, and loads the page again only once the flip has run"; §5.3):
//!   a pin whose caller holds nothing waits for the flip and loads again; a
//!   caller that holds latches, the gate or a page being loaded gets
//!   [`StoreError::Restart`] and restarts itself.
//! - **Construction is validated before it allocates** (card A-3c, Work 9):
//!   `Invalid` for epoch 0, a file whose length is not `slot_count ×
//!   page_size`, `next_lpid` 0, or a free LPID that is 0, at or past
//!   `next_lpid` or listed twice; and for a fanout outside
//!   `2..=max_fanout(page_size)` (§4.1).
//!
//! Tests `writeback_absent`, `evict_prev_epoch`, `t_pin_load_race_k`,
//! `t_pin_load_race_k_reload`; mutants WB-GATE-BLOCKING, RELOAD-BEFORE-FLIP.

use nucleus_env::File;

use super::bitmap::Bitmap;
use super::cmap::CurrentMap;
use super::frame::FrameId;
use super::gate::GateHold;
use super::key::PageKey;
use super::lpids::LpidState;
use super::pool::{PinOutcome, Pool, PoolConfig};
use super::slots::{SlotAlloc, SlotInit, SlotStats, SlotUse};
use super::sync::{Arc, Mutex, Primitives, StdPrimitives};
use super::table::FrameMeta;
use crate::map::MapGeometry;
use crate::meta::Ref;
use crate::store::StoreError;

/// The key a hook fires with when it has no page (LPID 0 is null, §1).
pub(crate) const NO_KEY: PageKey = PageKey::Lpid(0);

/// The frame a hook fires with when it has none (no frame has it, since
/// `frames ≤ 2^31`).
pub(crate) const NO_FRAME: FrameId = FrameId(u32::MAX);

/// Why an operation must release what it holds and restart (card A-3c, Work
/// 11; never returned to a client).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restart {
    /// A flip is pending and no frame could be obtained without blocking
    /// where the caller may not block (S-STORE §5.2: "any other eviction ...
    /// only *tries* to take the gate shared. If the try fails because a flip
    /// is pending, the evictor takes a clean victim. If no victim is clean,
    /// the thread releases its latches and restarts (§5.3)").
    FlipPending,
}

/// What the caller of a pin or a reservation holds (card A-3c, Work 8.1):
/// whether an eviction it makes runs under the caller's gate hold, and
/// whether a restart for a pending flip is the caller's to make or the
/// pool's to wait out. Built only by its constructors, so that a caller
/// states what it holds in one word.
pub struct Hold<'a, 'p, H: File, P: Primitives = StdPrimitives> {
    /// The caller's gate hold, if any.
    pub(crate) gate: Option<&'a GateHold<'p, H, P>>,
    /// The caller holds frame latches.
    pub(crate) latched: bool,
    /// The caller is a gated operation past its first mutation.
    pub(crate) mutated: bool,
    /// The caller holds a page being loaded (a nested pin of a load).
    pub(crate) in_load: bool,
}

impl<'a, 'p, H: File, P: Primitives> Hold<'a, 'p, H, P> {
    /// The caller holds no latch, no gate and no page being loaded: a pin
    /// that must restart for a pending flip waits for the flip here and
    /// loads again (S-STORE §5.4 "The load").
    pub fn nothing() -> Hold<'a, 'p, H, P> {
        Hold {
            gate: None,
            latched: false,
            mutated: false,
            in_load: false,
        }
    }

    /// The caller holds frame latches and no gate, and may still restart: a
    /// pin that must restart returns [`StoreError::Restart`] to it
    /// (S-STORE §5.3: "with no evictable frame it releases its latches and
    /// restarts").
    pub fn latched() -> Hold<'a, 'p, H, P> {
        Hold {
            gate: None,
            latched: true,
            mutated: false,
            in_load: false,
        }
    }

    /// The caller holds the gate shared `g`, before its first mutation, and
    /// no latch: an eviction it makes runs under `g` (S-STORE §5.2).
    pub fn gated(g: &'a GateHold<'p, H, P>) -> Hold<'a, 'p, H, P> {
        Hold {
            gate: Some(g),
            latched: false,
            mutated: false,
            in_load: false,
        }
    }

    /// The caller holds the gate shared `g` and frame latches, before its
    /// first mutation.
    pub fn gated_latched(g: &'a GateHold<'p, H, P>) -> Hold<'a, 'p, H, P> {
        Hold {
            gate: Some(g),
            latched: true,
            mutated: false,
            in_load: false,
        }
    }

    /// The caller is a gated operation past its first mutation, holding `g`
    /// and maybe latches: it never restarts (S-STORE §5.6 "Restart"), and a
    /// frame acquisition it cannot complete waits for an unpin (§5.4).
    pub fn mutated(g: &'a GateHold<'p, H, P>) -> Hold<'a, 'p, H, P> {
        Hold {
            gate: Some(g),
            latched: true,
            mutated: true,
            in_load: false,
        }
    }

    /// The same holds, marked as made while the thread holds a page being
    /// loaded (Work 3.3, 8.4): a restart is returned to the outer load.
    pub(crate) fn in_load(&self) -> Hold<'a, 'p, H, P> {
        let mut hold = *self;
        hold.in_load = true;
        hold
    }

    /// Whether the caller holds nothing the pool must leave alone: no latch,
    /// no gate, no page being loaded.
    pub(crate) fn is_nothing(&self) -> bool {
        self.gate.is_none() && !self.latched && !self.in_load
    }
}

// `Copy` by hand: a derive would also ask `H` and `P` for `Clone`, and a
// pool is not clonable; the fields are a shared reference and three flags.
impl<H: File, P: Primitives> Clone for Hold<'_, '_, H, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H: File, P: Primitives> Copy for Hold<'_, '_, H, P> {}

impl<H: File, P: Primitives> core::fmt::Debug for Hold<'_, '_, H, P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Hold")
            .field("gate", &self.gate.is_some())
            .field("latched", &self.latched)
            .field("mutated", &self.mutated)
            .field("in_load", &self.in_load)
            .finish()
    }
}

/// What a write-back pool is built with (card A-3c, Work 9). A later card's
/// open (A-7d) builds one from an image and adds the map's image as an
/// input; this one has no such field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBackConfig {
    /// The map's fanout `f` (§4.1): `2 ≤ f ≤ max_fanout(page_size)`.
    pub fanout: u64,
    /// The epoch `e` the pool starts at (§1: 1 for a new file); not 0.
    pub epoch: u32,
    /// The file's length in slots, which the file must already have.
    pub slot_count: u64,
    /// What the slot bitmaps hold.
    pub slots: SlotInit,
    /// One past the highest LPID ever allocated (§2.2).
    pub next_lpid: u64,
    /// The LPIDs that are free: each `1 ≤ x < next_lpid`, none twice.
    pub free_lpids: Vec<u64>,
}

impl WriteBackConfig {
    /// A fresh file's configuration at `page_size`: epoch 1, `SlotInit::Fresh`,
    /// `next_lpid` 1, no free LPID, and a fresh map (S-STORE §2.3 step 3:
    /// "a page map whose root is one empty MapInner at level 1").
    pub fn fresh(_page_size: u32, fanout: u64, slot_count: u64) -> WriteBackConfig {
        WriteBackConfig {
            fanout,
            epoch: 1,
            slot_count,
            slots: SlotInit::Fresh,
            next_lpid: 1,
            free_lpids: Vec::new(),
        }
    }
}

/// A frame's §4.1 pins (the diagnostics of Work 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapPinInfo {
    /// On a MapLeaf's frame: how many dirty pages it maps hold §4.1's pin on
    /// it; on any other frame, 0.
    pub dirty_pins: u64,
    /// On an LPID page's frame: whether the page is dirty and so holds §4.1's
    /// pin on its MapLeaf.
    pub holds_dirty_pin: bool,
}

/// The write-back state of a pool (card A-3c, Work 9), built at construction
/// and never replaced.
pub(crate) struct WriteBack<P: Primitives> {
    /// The current page map (Work 3).
    pub(crate) cmap: CurrentMap<P>,
    /// The slot allocator (Work 4).
    pub(crate) slots: SlotAlloc<P>,
    /// The LPID state, under the free-set lock (Work 5).
    pub(crate) lpids: Mutex<P, LpidState>,
    /// How many page writes dirty evictions made (`wb_writes`).
    pub(crate) writes: Mutex<P, u64>,
}

impl<P: Primitives> WriteBack<P> {
    /// Build the state `wb` describes over `file`, validating it first
    /// (card A-3c, Work 9).
    fn build<H: File>(
        file: &H,
        page_size: u32,
        wb: &WriteBackConfig,
    ) -> Result<WriteBack<P>, StoreError> {
        let invalid = |what: &str| StoreError::Invalid(format!("write-back pool: {what}"));
        if wb.epoch == 0 {
            return Err(invalid("the epoch is 0 (S-STORE §1: it starts at 1)"));
        }
        let ps = u64::from(page_size);
        let want = wb
            .slot_count
            .checked_mul(ps)
            .ok_or_else(|| invalid("slot_count × page_size passed u64"))?;
        let len = file.len().map_err(StoreError::Env)?;
        if len != want {
            return Err(invalid(&format!(
                "the file is {len} bytes, not slot_count × page_size = {want}"
            )));
        }
        if wb.next_lpid == 0 {
            return Err(invalid("next_lpid is 0"));
        }
        let mut free = Bitmap::with_len(wb.next_lpid)?;
        for &x in &wb.free_lpids {
            if x == 0 || x >= wb.next_lpid {
                return Err(invalid(&format!(
                    "free LPID {x} is 0 or at or past next_lpid {}",
                    wb.next_lpid
                )));
            }
            if free.get(x) {
                return Err(invalid(&format!("free LPID {x} is listed twice")));
            }
            free.set(x, true)
                .map_err(|_| invalid("a free LPID past the bitmap"))?;
        }
        let page_len = usize::try_from(page_size).map_err(|_| invalid("a page size past usize"))?;
        let geom = MapGeometry::with_fanout(page_len, wb.fanout)?;
        let cmap = CurrentMap::fresh(geom, wb.epoch)?;
        let slots = SlotAlloc::new(
            page_size,
            wb.slot_count,
            crate::meta::reserved_slots(page_size)?,
            wb.slots.clone(),
        )?;
        Ok(WriteBack {
            cmap,
            slots,
            lpids: Mutex::new(LpidState {
                next_lpid: wb.next_lpid,
                free,
            }),
            writes: Mutex::new(0),
        })
    }
}

/// The most callers a pool takes: `LatchCore::new_at`'s capacity domain,
/// which every frame latch's waiter queue is sized by (`callers` plus the
/// store's own pinning threads, at most 65,535).
pub(crate) const MAX_CALLERS: u32 = 65_535;

impl<H: File, P: Primitives> Pool<H, P> {
    /// A write-back pool (card A-3c, Work 9): A-3b's pool construction, with
    /// its minimum check, plus the gate at `wb.epoch`, a fresh current map
    /// (`MapGeometry::with_fanout(page_size, fanout)`), the slot allocator
    /// over `meta::reserved_slots(page_size)` and `wb.slots`, and the LPID
    /// state. Every pool allocation is made here (BUF-FIXED); the map's
    /// growth, the LPID vectors and a file growth are the write-back's own,
    /// each fallible.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] as [`WriteBackConfig`] documents, and what
    /// [`Pool::new`] gives; [`StoreError::Env`] if the file's length cannot
    /// be read; [`StoreError::OutOfMemory`] (53200) if an allocation fails.
    pub fn with_writeback(
        file: Arc<H>,
        cfg: PoolConfig,
        wb: WriteBackConfig,
    ) -> Result<Pool<H, P>, StoreError> {
        let state = WriteBack::build(&*file, cfg.page_size, &wb)?;
        let s = super::pool::sizes(cfg)?;
        let reserved = crate::meta::reserved_slots(s.page_size)?;
        let sizing = super::sizing::PoolSizing::compute(
            cfg.frames,
            cfg.callers,
            super::sizing::OWN_PINNING_THREADS,
            super::sizing::STEP2_RESERVE_FRAMES,
            super::sizing::H_MIN,
        )?;
        Self::build(file, s, reserved, sizing, wb.epoch, Some(state))
    }

    /// [`Pool::with_writeback`] for a pool below its minimum (card A-3c;
    /// `test-hooks` only): the write-back twin of
    /// [`Pool::new_below_minimum_for_tests`], for this crate's own tests,
    /// which state pools of 18 or 46 frames at a lowered height floor. The
    /// same reach rule: no workspace crate other than nucleus-store may
    /// reference it.
    ///
    /// # Errors
    /// As [`Pool::with_writeback`], without the minimum.
    #[cfg(feature = "test-hooks")]
    pub fn with_writeback_below_minimum_for_tests(
        file: Arc<H>,
        cfg: PoolConfig,
        wb: WriteBackConfig,
    ) -> Result<Pool<H, P>, StoreError> {
        let state = WriteBack::build(&*file, cfg.page_size, &wb)?;
        let s = super::pool::sizes(cfg)?;
        let reserved = crate::meta::reserved_slots(s.page_size)?;
        let sizing = super::sizing::PoolSizing::below_minimum(
            cfg.frames,
            cfg.callers,
            super::sizing::OWN_PINNING_THREADS,
            super::sizing::STEP2_RESERVE_FRAMES,
            super::sizing::H_MIN,
        )?;
        Self::build(file, s, reserved, sizing, wb.epoch, Some(state))
    }

    /// [`Pool::pin_with`] on the pool's own current page map (Work 8.5), the
    /// pin of a write-back pool; `Invalid` on a pool built with
    /// [`Pool::new`].
    ///
    /// # Errors
    /// As [`Pool::pin_with`], and [`StoreError::Invalid`] on a pool without
    /// write-back.
    pub fn pin_page(
        &self,
        key: PageKey,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<PinOutcome<'_, H, P>, StoreError> {
        let map = self.cmap()?;
        self.pin_with(key, map, hold)
    }

    /// [`Pool::supersede`] stands for S-STORE §4.4's superseded home: free
    /// `old` iff its entry's epoch is the epoch of the caller's gate hold
    /// (Work 4, S4). The decision is made under the gate, so no flip changes
    /// `e` between the write and the decision.
    ///
    /// # Errors
    /// As [`SlotAlloc::supersede`]; [`StoreError::Invalid`] on a pool without
    /// write-back.
    pub fn supersede(&self, old: Ref, g: &GateHold<'_, H, P>) -> Result<(), StoreError> {
        self.slots()?.supersede(old, g.epoch())
    }

    /// Step 6's release (S-STORE §13 step 6; Work 4, S5), the call A-7c's
    /// checkpoint makes with "the slots that image `k−1` references and
    /// image `k` does not".
    ///
    /// # Errors
    /// As [`SlotAlloc::release_step6`]; [`StoreError::Invalid`] on a pool
    /// without write-back.
    pub fn release_step6(&self, newly_pending: &[u64]) -> Result<(), StoreError> {
        self.slots()?.release_step6(newly_pending)
    }

    /// Install or remove the slot hook (Work 10; `test-hooks` only).
    #[cfg(feature = "test-hooks")]
    pub fn set_slot_hook(&self, hook: Option<super::slots::SlotHook>) {
        if let Ok(slots) = self.slots() {
            slots.set_hook(hook);
        }
    }

    /// What slot `s` is (the diagnostics of Work 9).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back;
    /// [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn slot_use(&self, s: u64) -> Result<SlotUse, StoreError> {
        self.slots()?.state(s)
    }

    /// The slot allocator's counters (the diagnostics of Work 9).
    ///
    /// # Errors
    /// As [`Pool::slot_use`].
    pub fn slot_stats(&self) -> Result<SlotStats, StoreError> {
        self.slots()?.stats()
    }

    /// LPID `lpid`'s current entry (the diagnostics of Work 9): the MapLeaf
    /// is pinned with [`Hold::nothing`], which may load it. `Ref::NULL` when
    /// the LPID is unmapped or the MapLeaf was never made (§4.1).
    ///
    /// # Errors
    /// What the pin and the latch give; [`StoreError::Invalid`] on a pool
    /// without write-back.
    pub fn entry_of(&self, lpid: u64) -> Result<Ref, StoreError> {
        let cmap = self.cmap()?;
        let (i, j) = cmap.geom.lpid_entry(lpid);
        match self.pin_page(PageKey::MapLeaf(i), &Hold::nothing())? {
            PinOutcome::Unmapped => Ok(Ref::NULL),
            PinOutcome::Pinned(p) => {
                let g = p.latch_shared()?;
                let entry = u64::try_from(j)
                    .map_err(|_| StoreError::Invalid("a map entry past u64".into()))?;
                crate::map::map_ref(&g, &cmap.geom, entry)
            }
        }
    }

    /// How many page writes dirty evictions made (the diagnostics of Work 9).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] on a pool without write-back;
    /// [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn wb_writes(&self) -> Result<u64, StoreError> {
        let wb = self.writeback()?;
        wb.writes
            .lock()
            .map(|w| *w)
            .map_err(|_| StoreError::Poisoned("the write counter was poisoned by a panic".into()))
    }

    /// Frame `f`'s §4.1 pins (the diagnostics of Work 6): a table-only read,
    /// on any pool; a pool built with [`Pool::new`] takes none, so both are
    /// 0 there.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `f` is out of range;
    /// [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn map_pin_info(&self, f: FrameId) -> Result<MapPinInfo, StoreError> {
        let t = self.lock_table()?;
        let m: FrameMeta = *t.meta(f)?;
        Ok(MapPinInfo {
            dirty_pins: m.dirty_pins,
            holds_dirty_pin: m.holds_dirty_pin,
        })
    }

    /// Count one page write of a dirty eviction (Work 7, E4).
    pub(crate) fn count_wb_write(&self) -> Result<(), StoreError> {
        let wb = self.writeback()?;
        let mut w = wb.writes.lock().map_err(|_| {
            StoreError::Poisoned("the write counter was poisoned by a panic".into())
        })?;
        *w = w.saturating_add(1);
        Ok(())
    }
}
