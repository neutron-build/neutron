#![deny(clippy::indexing_slicing)]
//! The S-STORE subset under test (card X-5 Work 3–4): the file and metas
//! (§2), the page map and free set (§4), the buffer pool with the epoch
//! gate and `write_guard` (§5), and the slot/LPID allocators.
//!
//! Architecture (S-BATON in `src/sim`, card X-5b Work 11): all store state
//! lives in [`StoreInner`] inside the world behind the scheduler's world
//! lock; **no borrow of it ever survives a yield point**. Every store
//! operation is a free function over a [`Cx`] that composes **primitives**
//! — short lock-work-unlock steps ([`Cx::with`], whose acquisition is a
//! `try_lock` per S-BATON rule 4) — and carries page bytes, LPIDs, descent
//! paths and progress as owned locals between them. The logical locks
//! (epoch gate, per-tree sim mutex, pins, WAL acks, `mark_applied` turns)
//! **are** held across yields by design, expressed as scheduler state plus
//! [`crate::sim::WaitKey`]s (rule 2).
//!
//! Every page mutation goes through [`prims::store_page`] (the
//! `write_guard` yield point with the epoch-copy rule, §5.6); every page
//! read through [`prims::fetch_page`] (pool + §3.3's four checks).
//!
//! Invariants owned here:
//! - ST-STEP: each `Cx::with` body is atomic (only one task runs between
//!   its yields), so check-then-act pairs inside one step are race-free.
//! - ST-IMG (the flat-map simplification, card Work 12): during a
//!   checkpoint's flip window (step 0 → step 5) the **first post-flip
//!   touch** of each LPID freezes its image-k state (an override in
//!   [`MapState::img`]); after the data-write phase of step 2 the
//!   effective image (overrides ∪ `cur`) is snapshot-stable, so the map
//!   radix and the free set serialise the same image. The persisted
//!   format is the spec's §4.1 radix, written copy-on-write (only changed
//!   leaves, up to the root) and parsed at open/amcheck — never read from
//!   memory.
//!
//! Declared simplifications (README): the in-memory page map is flat with
//! per-entry image overrides (`R_MAP = 0`; MapLeaf residency and pinning
//! are A-3c/A-7's); latch coupling is a per-tree sim mutex (card 3.6); no
//! overflow chains (values ≤ `V_INLINE`); 4 KiB pages only.

pub mod amcheck;
pub mod ckpt;
pub mod page;
pub mod prims;
pub mod tree;
pub mod wal;

use std::collections::{BTreeMap, BTreeSet};

use crate::sim::{
    EnvRef, FileId, HasDisk, Sched, SchedCore, SchedHost, SimDisk, SimFail, TaskId, WaitKey,
};

/// WAL segment size (64 KiB, §12.1; card 4.1): a record is at most a
/// quarter of it.
pub const WAL_SEG: u64 = 64 * 1024;
/// The catalog tree id (card 2.1).
pub const CATALOG: u64 = 1;
/// The directory pseudo-tree id (its mutex key; root is `meta.dir_root`).
pub const DIR_TREE: u64 = 0;

/// The runtime mutants (card "Mutation evidence"), checked at exactly one
/// site each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutant {
    /// I-INTEGRITY: skip the header-crc-vs-bytes check.
    CrcSkip,
    /// I-INTEGRITY: skip the header-crc-vs-reference check.
    RefCrcSkip,
    /// I-INTEGRITY: skip the epoch check (single-site; see the paired
    /// note in the card's mutation table).
    EpochSkip,
    /// I-INTEGRITY: skip the id check (single-site).
    IdSkip,
    /// I-INTEGRITY: skip epoch + reference crc (the paired mutant).
    EpochSkipRefCrc,
    /// I-INTEGRITY: skip id + reference crc (the paired mutant).
    IdSkipRefCrc,
    /// I-FREE-SAFE: step 6's frees happen before step 5.
    FreeEarly,
    /// I-FREE-EXACT: the bitmaps omit the FreeSet pages' own slots.
    BitmapOmitsFreesetPages,
    /// I-META-ATOMIC: step 3 (the page-file sync) is skipped.
    MetaBeforeSync,
    /// I-CKPT-AT-T: the flip does not take the gate exclusively.
    FlipNoGate,
    /// I-CKPT-AT-T: step 1 syncs only through the last record with ts ≤ T.
    WalSyncTOnly,
    /// I-CKPT-AT-T: `write_guard` never makes the epoch copy.
    WriteGuardNoCopy,
    /// I-CKPT-AT-T: an epoch copy or previous-epoch victim is not recorded
    /// in the image map.
    EvictSkipImageMap,
    /// I-REDO-COMPLETE: the replay start ignores an open txn's first chunk.
    StartLsnPastChunk,
    /// I-REDO-COMPLETE: the replay start counts only marker-less txns.
    StartLsnMarkedChunk,
    /// I-WAL-TS-ORDER: the commit critical section ends between the ts
    /// assignment and the enqueue.
    TsAfterAppend,
    /// I-WAL-STALE: the reader skips the `lsn` ≠ expected check.
    LsnCheckSkip,
    /// I-BULK-PUBLISH: publish does not wait for the forced meta.
    PublishBeforeMeta,
    /// I-GCW: the meta carries the previous `gc_w`.
    GcwNotRecorded,
    /// I-GCW: the `gc_w ≤ T` fail-stop check is removed.
    GcwBoundSkip,
    /// I-REPLAY-IDEM: an identical re-insert is treated as a conflict.
    ReplayDupConflict,
    /// I-ENTRY-UNIQUE: a different-value re-insert is accepted.
    DupDifferentAccepted,
    /// I-NO-INPLACE: dirty eviction writes to the current home.
    InplaceEvict,
    /// I-LPID-EXACT: DropTree frees the root first.
    DropRootFirst,
    /// G1: no `sync_dir` after segment create/rename.
    RenameNoDirsync,
    /// G2: recovery truncates logically, not physically.
    TruncateLogicalOnly,
    /// G3: a missing start segment is treated as an empty WAL.
    StartSegmentMissingOk,
}

/// Store configuration (card 3.3–3.4 with the Work 12 amendment, 4.1).
#[derive(Debug, Clone)]
pub struct StoreCfg {
    /// `map_leaf_cap` override (spec 254 at 4 KiB; the campaign draws from
    /// {8, 16, 254}). Smaller caps give the persisted radix ≥ 2 levels.
    pub map_leaf_cap: u64,
    /// `freeset_leaf_bits` override (spec 32_576 at 4 KiB; the campaign
    /// draws from {128, 512, spec}). Smaller leaves give the free set ≥ 3
    /// pages.
    pub freeset_leaf_bits: u64,
    /// Pool frame count `F` (§5.4, never grows). Work 12: `R_MAP = 0`, so
    /// `F = tasks × (max_height + 2) + R_EPOCH`.
    pub pool_frames: usize,
    /// The epoch-copy reserve `R_EPOCH` (§5.6).
    pub r_epoch: usize,
    /// The active mutant, if any.
    pub mutant: Option<Mutant>,
}

/// The store failure type. `Corrupt` is XX001 (§ "Error classes").
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Fail {
    /// XX001: an integrity check failed.
    #[error("corrupt: {0}")]
    Corrupt(String),
    /// The simulated environment crashed / deadlocked / violated S-BATON.
    #[error("sim: {0:?}")]
    Sim(SimFail),
    /// The store is poisoned (I-FAIL-STOP, §9.2).
    #[error("poisoned: {0}")]
    Poisoned(String),
    /// A precondition was violated (a bug or a bad config).
    #[error("invalid: {0}")]
    Invalid(String),
}

impl From<SimFail> for Fail {
    fn from(e: SimFail) -> Self {
        Fail::Sim(e)
    }
}

/// A host that carries the store's state (the world's store slice).
pub trait HasStore {
    /// The store slice.
    fn store(&mut self) -> &mut StoreInner;
}

/// The combined host bound of every store function: scheduling, disk and
/// store slices (S-BATON's world).
pub trait StoreHost: SchedHost + HasDisk + HasStore {}
impl<T: SchedHost + HasDisk + HasStore> StoreHost for T {}

/// The per-seed world of the harness: everything the scheduler
/// serialises. The harness slice (`h`) is added by [`crate::harness`].
pub struct Core {
    /// The scheduling slice.
    pub sched: SchedCore,
    /// The disk slice.
    pub disk: SimDisk,
    /// The store slice.
    pub st: StoreInner,
}

impl SchedHost for Core {
    fn sched_mut(&mut self) -> &mut SchedCore {
        &mut self.sched
    }
}

impl HasDisk for Core {
    fn disk(&mut self) -> &mut SimDisk {
        &mut self.disk
    }
}

impl HasStore for Core {
    fn store(&mut self) -> &mut StoreInner {
        &mut self.st
    }
}

/// One pool frame.
#[derive(Debug, Clone)]
pub struct Frame {
    /// The page's LPID (0 = free frame).
    pub key: u64,
    /// The page bytes.
    pub bytes: Vec<u8>,
    /// The content epoch (§5.6).
    pub epoch: u32,
    /// Dirty (mutated since its slot was written).
    pub dirty: bool,
    /// Pin count (§5.4).
    pub pins: usize,
    /// An epoch copy holding image content (belongs to the checkpointer).
    pub is_epoch_copy: bool,
    /// The clock bit.
    pub refed: bool,
    /// The eviction latch (§5.2's try-latch stand-in): set from victim
    /// selection to write-out completion, invisible to fetch/store.
    pub evicting: bool,
}

/// A page-map entry: the current ref plus the image-k override
/// (`Same` = image k keeps `cur`; the others freeze a first post-flip
/// touch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Img {
    /// The image's entry equals the current one.
    Same,
    /// The image maps the LPID at `r` (a clean page's old home, a written
    /// epoch copy, or a previous-epoch eviction's fresh slot).
    At(page::Ref),
    /// The image leaves the LPID unmapped.
    Free,
    /// The image's bytes are held by an epoch copy not yet written out;
    /// step 2 (or a reserve eviction) resolves this to `At`.
    CopyPending,
    /// WRITE-GUARD-NO-COPY only: the image's bytes were never preserved
    /// (the mutant's damage marker).
    Lost,
}

/// The in-memory page map (flat; the persisted §4.1 radix is materialised
/// at step 2 and parsed at open — declared simplification, Work 12).
#[derive(Debug, Default, Clone)]
pub struct MapState {
    /// LPID → current ref (absent = unmapped).
    pub cur: BTreeMap<u64, page::Ref>,
    /// Post-flip overrides for the image being built (first-touch
    /// freezes; snapshot-stable after step 2's data phase).
    pub img: BTreeMap<u64, Img>,
    /// The previous image's persisted leaf layout (for the step-2 CoW
    /// diff): leaf index → `(content-hash, ref)`.
    pub prev_leaves: BTreeMap<u64, (u64, page::Ref)>,
    /// The previous image's inner-node refs by `(level, index)`.
    pub prev_inners: BTreeMap<(u8, u64), (u64, page::Ref)>,
    /// The previous image's root.
    pub prev_root: Option<page::Ref>,
}

/// The frozen image table of step 2's map phase (B5): taken in one
/// atomic step after the data phase, read by the map radix, the free set
/// and the step-5 durable set until step 5 resolves them. Every
/// referenced slot is pinned in `SlotState::image_refs` in the same
/// step, so concurrent evictions cannot unpin or redirect what the image
/// embeds.
#[derive(Debug, Clone, Default)]
pub struct FrozenImage {
    /// LPID → the image's ref (`None` = the image leaves it unmapped).
    pub entries: BTreeMap<u64, Option<page::Ref>>,
    /// The frozen `next_lpid` (the radix's leaf span and the free set's
    /// LPID-bit span).
    pub next_lpid: u64,
    /// The frozen `slot_count` (the free set's slot-bit span).
    pub slot_count: u64,
}

/// Slot bookkeeping (§4.4–§4.5).
#[derive(Debug, Default, Clone)]
pub struct SlotState {
    /// Current homes (the current map's pages, plus the current image's
    /// map/free-set pages).
    pub used_cur: BTreeSet<u64>,
    /// The durable image's referenced slots (all of them: data, map and
    /// free-set pages).
    pub used_durable: BTreeSet<u64>,
    /// Slots frozen into `At` overrides of the image being built (from
    /// freeze to step 5): protected from reuse while image k needs them.
    pub image_refs: BTreeSet<u64>,
    /// Slots freed at step 6 (between the frees and the recycle).
    pub pending_free: BTreeSet<u64>,
    /// The file length in slots.
    pub slot_count: u64,
    /// Free LPIDs (current map unmapped).
    pub free_lpids: BTreeSet<u64>,
}

/// The epoch gate (§5.6).
#[derive(Debug, Default, Clone)]
pub struct Gate {
    /// Shared holders.
    pub holders: usize,
    /// A flip is waiting (blocks new shared acquisitions).
    pub flip_pending: bool,
    /// Whether an exclusive flip hold is active (step 0's critical part).
    pub exclusive: bool,
}

/// The buffer pool (§5): fixed frames, clock eviction.
#[derive(Debug, Clone)]
pub struct Pool {
    /// The frames.
    pub frames: Vec<Frame>,
    /// The clock hand.
    pub hand: usize,
}

/// The sequencer / apply-frontier state (§12.4, §13 step 0).
#[derive(Debug, Default, Clone)]
pub struct SeqState {
    /// The next commit ts to hand out.
    pub next_ts: u64,
    /// `visible_ts` (advances in ts order, `mark_applied`).
    pub visible_ts: u64,
    /// Applied-but-not-yet-visible commit ts's.
    pub applied: BTreeSet<u64>,
    /// The commit critical section is taken (I-WAL-TS-ORDER).
    pub commit_cs: bool,
}

/// One directory entry (§6.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirEntry {
    /// The tree's root LPID (stable, §6.3).
    pub root: u64,
    /// `live` or `dropping`.
    pub dropping: bool,
}

/// The store's durable state and in-flight bookkeeping.
#[derive(Debug, Clone)]
pub struct StoreInner {
    /// The configuration.
    pub cfg: StoreCfg,
    /// The page file's inode.
    pub page_file: FileId,
    /// The last meta id written (k).
    pub k: u64,
    /// The current epoch e (starts at 1 after creation).
    pub e: u32,
    /// The opened meta.
    pub meta: page::Meta,
    /// The tree directory (id → entry).
    pub dir: BTreeMap<u64, DirEntry>,
    /// The page map.
    pub map: MapState,
    /// The slot bookkeeping.
    pub slots: SlotState,
    /// The pool.
    pub pool: Pool,
    /// The gate.
    pub gate: Gate,
    /// Per-tree sim mutex holders (task id; absent = free).
    pub tree_mu: BTreeMap<u64, usize>,
    /// The sequencer.
    pub seq: SeqState,
    /// `gc_w_used` (§10.5).
    pub gc_w_used: u64,
    /// Checkpoints completed.
    pub ckpt_done: u64,
    /// A checkpoint's flip window is open (step 0 → step 5): first
    /// post-flip touches freeze image state.
    pub flip_window: bool,
    /// The frozen image table (step 2's map phase, B5): built in one
    /// atomic step when the data phase ends — every LPID's image ref
    /// (`img_entry`: an `Img::At` override or the current home) plus the
    /// two size counters the radix and the free set must agree on. The
    /// map radix, the free set and the step-5 durable set read **only**
    /// this table while it is set, and every slot it references is
    /// pinned in `image_refs`, so an eviction that was already in flight
    /// when the table was taken (picked before, finishing after) can
    /// move the live home but not unpin or redirect anything the image
    /// embeds — the stale-embed race this field closes (prototype
    /// deviation fixed in B5; see the stage RESULT).
    pub image_frozen: Option<FrozenImage>,
    /// A checkpoint is between steps 0 and 6 (the Q4 window).
    pub ckpt_open: bool,
    /// Splits inside the open checkpoint window (Q4).
    pub ckpt_splits: u64,
    /// Merges inside the open checkpoint window (Q4).
    pub ckpt_merges: u64,
    /// R_EPOCH exhaustion count (Q10).
    pub epoch_exhausted: u64,
    /// The free-set serialisation grew the file (Q17).
    pub freeset_grew: bool,
    /// Stale-record stops at renamed segment starts (Q11).
    pub stale_seg_reads: u64,
    /// Named park points (card Work 13: the D5 split parking): name →
    /// remaining yields. `park()` at a named point yields while a budget
    /// remains.
    pub parks: BTreeMap<&'static str, u64>,
    /// The bulk-load publish waiters' last durable meta (§12.8).
    pub meta_durable_k: u64,
    /// The free-set pages of the image **being built** (step 2's
    /// serialisation): filled by `write_free_set`, consumed by the
    /// step-5 resolution. B5 fix: the draft never tracked them, so the
    /// step-5 durable set omitted the image's own free-set pages and
    /// the **previous** image's pages were never unpinned — a slot leak
    /// that made every early free undetectable (FREE-EARLY could not
    /// fire M-NOINPLACE because nothing ever reused a freed slot).
    pub fs_building: BTreeSet<u64>,
    /// The free-set pages of the **last durable** image (unpinned at the
    /// next checkpoint's step 5, when nothing references them anymore).
    pub fs_prev: BTreeSet<u64>,
    /// Poison reason (I-FAIL-STOP).
    pub poisoned: Option<String>,
    /// The WAL state.
    pub wal: wal::WalState,
}

impl StoreInner {
    /// An empty store shell (no file content): used by `create`/`open`/
    /// `recover` and by amcheck's probe. `slot_count` starts at 16 and
    /// grows on demand (§4.5 test override: `max(16, n/16)`).
    pub fn empty(cfg: StoreCfg, page_file: FileId) -> Self {
        let mut pool = Pool {
            frames: Vec::new(),
            hand: 0,
        };
        for _ in 0..cfg.pool_frames {
            pool.frames.push(Frame {
                key: 0,
                bytes: Vec::new(),
                epoch: 0,
                dirty: false,
                pins: 0,
                is_epoch_copy: false,
                refed: false,
                evicting: false,
            });
        }
        let slots = SlotState {
            slot_count: 16,
            ..SlotState::default()
        };
        Self {
            cfg,
            page_file,
            k: 0,
            e: 1,
            meta: page::Meta::default(),
            dir: BTreeMap::new(),
            map: MapState::default(),
            slots,
            pool,
            gate: Gate::default(),
            tree_mu: BTreeMap::new(),
            seq: SeqState::default(),
            gc_w_used: 0,
            ckpt_done: 0,
            flip_window: false,
            image_frozen: None,
            ckpt_open: false,
            ckpt_splits: 0,
            ckpt_merges: 0,
            epoch_exhausted: 0,
            freeset_grew: false,
            stale_seg_reads: 0,
            parks: BTreeMap::new(),
            meta_durable_k: 0,
            fs_building: BTreeSet::new(),
            fs_prev: BTreeSet::new(),
            poisoned: None,
            wal: wal::WalState::default(),
        }
    }

    /// Whether the store is usable (not poisoned, §9.2).
    pub fn alive(&self) -> Result<(), Fail> {
        match &self.poisoned {
            Some(m) => Err(Fail::Poisoned(m.clone())),
            None => Ok(()),
        }
    }
}

/// The execution context handed to every store function: the world's
/// scheduler plus the calling task. Cheap to construct per step.
pub struct Cx<'a, H: StoreHost> {
    /// The world's scheduler (which owns the world lock).
    pub w: &'a Sched<H>,
    /// The calling task.
    pub t: TaskId,
}

impl<'a, H: StoreHost> Cx<'a, H> {
    /// The env view (every method a yield point, per S-BATON rule 4).
    pub fn env(&self) -> EnvRef<'a, H> {
        EnvRef {
            sched: self.w,
            task: self.t,
        }
    }

    /// Run one atomic step under the world lock (no yields inside; rule 1).
    /// The acquisition is a `try_lock` that reports [`SimFail::Baton`]
    /// naming `site` on a violation (rule 4).
    pub fn with<R>(
        &self,
        site: &'static str,
        f: impl FnOnce(&mut H) -> Result<R, Fail>,
    ) -> Result<R, Fail> {
        let mut g = self.w.acquire(site).map_err(Fail::Sim)?;
        f(&mut g)
    }

    /// A plain yield point naming its call site (card 1.2, rule 4).
    pub fn yield_now(&self, site: &'static str) -> Result<(), Fail> {
        self.env().yield_at(site).map_err(Fail::Sim)
    }

    /// Park on a wait key (Mesa-style; re-check after; rule 4).
    pub fn wait_for(&self, key: WaitKey) -> Result<(), Fail> {
        self.env().wait_for(key).map_err(Fail::Sim)
    }

    /// A named park point (card Work 13): while `parks[name]` has budget,
    /// yield. The harness's script sets budgets; the store calls this at
    /// the midpoints a directed test needs to hold (the D5 split parking).
    pub fn park(&self, name: &'static str) -> Result<(), Fail> {
        loop {
            let more = self.with("park", |c| {
                let st = c.store();
                match st.parks.get_mut(name) {
                    Some(n) if *n > 0 => {
                        *n -= 1;
                        Ok(true)
                    }
                    _ => Ok(false),
                }
            })?;
            if !more {
                return Ok(());
            }
            self.yield_now("park")?;
        }
    }
}
