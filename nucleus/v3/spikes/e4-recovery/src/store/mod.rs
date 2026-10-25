#![deny(clippy::indexing_slicing)]
//! The S-STORE subset under test (card X-5 Work 3–4): the file and metas
//! (§2), the page map and free set (§4), the buffer pool with the epoch
//! gate and `write_guard` (§5), and the slot/LPID allocators.
//!
//! Architecture (S-BATON in `src/sim`): all store state lives in
//! [`Core`] behind the world lock; the running task holds it between
//! scheduler calls. Store operations are plain functions that compose
//! **primitives** — short lock-work-unlock steps — and hold page bytes as
//! owned copies between them, so no world borrow survives a yield point.
//! Every page mutation goes through `store_page` (the `write_guard` yield
//! point with the epoch-copy rule, §5.6); every page read through
//! `fetch_page` (pool + §3.3).
//!
//! Declared simplifications (README): the in-memory page map is a flat
//! `BTreeMap` with per-entry image overrides standing in for the §4.1
//! radix CoW (the persisted radix is materialised at checkpoint step 2 and
//! parsed at open and by amcheck — the physical format is the spec's, the
//! residency/pinning of MapLeaf pages in the pool is not exercised);
//! latch coupling is a per-tree sim mutex (card 3.6); no overflow chains
//! (values ≤ `V_INLINE`); 4 KiB pages only.

pub mod ckpt;
pub mod page;
pub mod prims;
pub mod tree;
pub mod wal;

use std::collections::{BTreeMap, BTreeSet};

use crate::sim::{
    EnvRef, FileId, HasDisk, Sched, SchedCore, SchedHost, SimDisk, SimFail, TaskId, WaitKey,
};

/// WAL segment size (64 KiB; §12.1): a record is at most a quarter of it.
pub const WAL_SEG: u64 = 64 * 1024;
/// The catalog tree id (card 2.1).
pub const CATALOG: u64 = 1;
/// The directory pseudo-tree id (its mutex key; root is `meta.dir_root`).
pub const DIR_TREE: u64 = 0;

/// The runtime mutants (card Work 8), checked at exactly one site each.
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
    /// I-WAL-TS-ORDER: a yield between ts assignment and enqueue.
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

/// Store configuration (card 3.3–3.4, 4.1).
#[derive(Debug, Clone)]
pub struct StoreCfg {
    /// `map_leaf_cap` override (spec 254 at 4 KiB; campaign {8, 16, 254}).
    pub map_leaf_cap: u64,
    /// `freeset_leaf_bits` override (spec 32576; campaign {128, 512, spec}).
    pub freeset_leaf_bits: u64,
    /// Pool frame count (§5.4; never grows).
    pub pool_frames: usize,
    /// The epoch-copy reserve `R_EPOCH` (§5.6).
    pub r_epoch: usize,
    /// The active mutant, if any.
    pub mutant: Option<Mutant>,
}

impl StoreCfg {
    /// `R_MAP` per §5.4 as implemented here: with the flat in-memory map
    /// (declared simplification) the MapLeaf pin rule is not exercised, so
    /// the reserve covers the pool's own metadata pressure instead.
    pub fn r_map(&self) -> usize {
        2
    }
}

/// The store failure type. `Corrupt` is XX001 (§ "Error classes").
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Fail {
    /// XX001: an integrity check failed.
    #[error("corrupt: {0}")]
    Corrupt(String),
    /// The simulated environment crashed / deadlocked.
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

/// A per-seed world: everything the scheduler serialises.
pub struct Core {
    /// The scheduling slice.
    pub sched: SchedCore,
    /// The disk slice.
    pub disk: SimDisk,
    /// The store slice.
    pub st: StoreInner,
    /// The harness slice (model hooks, monitors' inputs, quota counters).
    pub h: Harness,
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

/// Harness-side state the store reports to (never read back by the store:
/// the store never reads the model or ground truth).
#[derive(Debug, Default)]
pub struct Harness {
    /// Commits recorded this pass: ts → (mode, ops) for the model.
    pub model_commits: BTreeMap<u64, (crate::model::Mode, Vec<crate::model::Op>)>,
    /// Bulk loads this pass: (tree, S_L, entries).
    pub loads: Vec<(u64, u64, Vec<(Vec<u8>, Vec<u8>)>)>,
    /// The enqueue position at each flip (M-CKPT-T hook).
    pub flip_enqueue_pos: Vec<u64>,
    /// Counters for the coverage quotas and the record.
    pub counters: Counters,
    /// The commit budget for the pass.
    pub stop_after: u64,
    /// Checkpoint trigger (commits per checkpoint).
    pub ckpt_every: u64,
    /// The round index.
    pub round: u32,
    /// Commits done.
    pub commits_done: u64,
    /// The txn counter (ids).
    pub txn_counter: u64,
    /// Registered snapshots (long readers).
    pub snapshots: BTreeSet<u64>,
    /// GC resume positions per tree.
    pub gc_resume: BTreeMap<u64, Vec<u8>>,
    /// DDL tree ids handed out.
    pub ddl_trees: u64,
    /// Published trees (per the publish commits seen).
    pub published: BTreeSet<u64>,
    /// A forced checkpoint is pending (bulk load).
    pub force_ckpt: bool,
    /// The workload tasks are done.
    pub workload_done: bool,
}

/// Coverage and report counters (harness-side).
#[derive(Debug, Default, Clone)]
pub struct Counters {
    /// Splits between checkpoint step 0 and step 3 (Q4).
    pub ckpt_splits: u64,
    /// Merges between step 0 and step 3 (Q4).
    pub ckpt_merges: u64,
    /// Rounds whose flip saw a commit > T partially applied (Q9).
    pub partial_above_t: u64,
    /// Rounds where the epoch reserve was exhausted (Q10).
    pub epoch_exhausted: u64,
    /// Stale-record stops at a renamed segment's start (Q11).
    pub stale_seg_reads: u64,
    /// A tree was mid-`dropping` at the crash (Q7).
    pub drop_walk_crash: u64,
    /// The free-set serialisation grew the file (Q17).
    pub freeset_grew: u64,
    /// A flip with `gc_w_used > previous gc_w` (Q18).
    pub gc_advanced: u64,
    /// Bulk loads: crash between load and publish / after publish (Q6).
    pub load_unpublished_crash: u64,
    pub load_published_crash: u64,
    /// Chunked shapes at the crash (Q5).
    pub chunk_open: u64,
    pub chunk_marked_above_t: u64,
    pub chunk_partial_apply: u64,
}

/// A pooled page's key: tree pages by LPID (the map is flat in memory, the
/// free set materialised at step 2 — declared simplification).
pub type PKey = u64;

/// One pool frame.
#[derive(Debug, Clone)]
pub struct Frame {
    /// The page's LPID (0 = free frame).
    pub key: PKey,
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
}

/// A page-map entry: the current ref plus the image-k override
/// (`Same` = image k keeps `cur`; the others freeze a first post-flip
/// touch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Img {
    /// The image's entry equals the current one.
    Same,
    /// The image maps the LPID at `r` (a clean page's old home, or a
    /// written-out epoch copy).
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
/// at step 2 and parsed at open — declared simplification).
#[derive(Debug, Default)]
pub struct MapState {
    /// LPID → current ref (absent = unmapped).
    pub cur: BTreeMap<u64, page::Ref>,
    /// Post-flip overrides for the image being built.
    pub img: BTreeMap<u64, Img>,
    /// The previous image's persisted leaf layout (for the step-2 CoW
    /// diff): leaf index → (bytes-hash, ref).
    pub prev_leaves: BTreeMap<u64, (u64, page::Ref)>,
    /// The previous image's inner-node refs by (level, index).
    pub prev_inners: BTreeMap<(u8, u64), (u64, page::Ref)>,
    /// The previous image's root.
    pub prev_root: Option<page::Ref>,
}

/// Slot bookkeeping (§4.4–§4.5).
#[derive(Debug, Default)]
pub struct SlotState {
    /// Current homes (the current map's pages).
    pub used_cur: BTreeSet<u64>,
    /// The durable image's referenced slots.
    pub used_durable: BTreeSet<u64>,
    /// The image being written (from flip to step 5).
    pub image_refs: BTreeSet<u64>,
    /// Slots freed at step 6 (pending between steps 2 and 6).
    pub pending_free: BTreeSet<u64>,
    /// The file length in slots.
    pub slot_count: u64,
    /// Free LPIDs (current map unmapped).
    pub free_lpids: BTreeSet<u64>,
}

/// The epoch gate (§5.6).
#[derive(Debug, Default)]
pub struct Gate {
    /// Shared holders.
    pub holders: usize,
    /// A flip is waiting (blocks new shared acquisitions).
    pub flip_pending: bool,
    /// Whether an exclusive flip hold is active (step 0's critical part).
    pub exclusive: bool,
}

/// The buffer pool (§5): fixed frames, clock eviction.
#[derive(Debug)]
pub struct Pool {
    /// The frames.
    pub frames: Vec<Frame>,
    /// The clock hand.
    pub hand: usize,
}

/// The sequencer / apply-frontier state.
#[derive(Debug, Default)]
pub struct SeqState {
    /// The next commit ts to hand out.
    pub next_ts: u64,
    /// `visible_ts` (advances in ts order).
    pub visible_ts: u64,
    /// Applied-but-not-yet-visible commit ts's.
    pub applied: BTreeSet<u64>,
    /// The commit critical section is taken.
    pub commit_cs: bool,
    /// Per-commit applied-entry counts (Q9 instrumentation).
    pub apply_progress: BTreeMap<u64, (u64, u64)>, // ts -> (done, total)
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
#[derive(Debug)]
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
    /// The tree directory.
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
    /// Q5–Q7 flags (set by the harness at the crash).
    pub chunk_open_crash: bool,
    /// A marked chunked txn sat above T at the flip (Q5).
    pub chunk_marked_crash: bool,
    /// A marked chunked txn was partially applied at the flip (Q5).
    pub chunk_partial_crash: bool,
    /// A tree was mid-`dropping` at the crash (Q7).
    pub drop_walk_crash: bool,
    /// Crash between bulk load and publication (Q6).
    pub load_unpublished_crash: bool,
    /// Crash after publication with the load in the image (Q6).
    pub load_published_crash: bool,
    /// A flip saw `gc_w_used` advance (Q18).
    pub gc_advanced_flag: bool,
    /// Poison reason (I-FAIL-STOP).
    pub poisoned: Option<String>,
    /// The WAL state.
    pub wal: wal::WalState,
    /// The bulk-load publish waiters' last durable meta (§12.8).
    pub meta_durable_k: u64,
}

/// The execution context handed to every store function.
pub struct Cx<'a> {
    /// The world.
    pub w: &'a Sched<Core>,
    /// The calling task.
    pub t: TaskId,
}

impl<'a> Cx<'a> {
    /// The env view (every method a yield point).
    pub fn env(&self) -> EnvRef<'a, Core> {
        EnvRef {
            sched: self.w,
            task: self.t,
        }
    }

    /// Run one atomic step under the world lock (no yields inside).
    pub fn with<R>(&self, f: impl FnOnce(&mut Core) -> Result<R, Fail>) -> Result<R, Fail> {
        let mut g = self.w.lock();
        f(&mut g)
    }

    /// A plain yield point (card 1.2).
    pub fn yield_now(&self) -> Result<(), Fail> {
        self.env().yield_now().map_err(Fail::Sim)
    }

    /// Park on a wait key (Mesa-style; re-check after).
    pub fn wait_for(&self, key: WaitKey) -> Result<(), Fail> {
        self.env().wait_for(key).map_err(Fail::Sim)
    }
}

impl StoreInner {
    /// Whether the store is usable (not poisoned, §9.2).
    pub fn alive(&self) -> Result<(), Fail> {
        match &self.poisoned {
            Some(m) => Err(Fail::Poisoned(m.clone())),
            None => Ok(()),
        }
    }
}
