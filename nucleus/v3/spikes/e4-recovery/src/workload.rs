//! The seeded workload, the per-seed driver and the coverage quotas
//! (card X-5 Work 5, stage X-5c).
//!
//! Each seed derives every knob with splitmix64 streams (5.1), runs 1–3
//! rounds (5.5) — workload, crash, faults, recovery with nested recovery
//! crashes, the final recovery, the oracle, the harness checkpoint — and
//! every MP/MW round runs its **twin** first (Work 2.5): the same seed,
//! script and F-CRASH resolutions with the media faults removed, judged
//! as class C, supplying O-3 (c)'s read/referenced sets. The round engine
//! itself is X-5b's [`crate::harness::run_round`]; this module only
//! builds `Script`s and reads outcomes.
//!
//! Invariants owned here:
//! - W-DET: the script of `(seed, round)` is a pure function of the seed
//!   and the driver's model at the previous cut (itself a pure function
//!   of the seed) — never of store memory, wall-clock time or thread
//!   timing. The per-seed digest is byte-identical across runs and
//!   `--threads` values.
//! - W-SCRIPT: a script only references trees the recovered store can
//!   have (created ∧ not dropped in the model at the cut), so a scripted
//!   Put never parks forever on a directory entry no committer will
//!   install.
//!
//! The crash point (5.5): targeted — hook `CKPT_HOOKS[(seed/20) % 10]` of
//! a seeded checkpoint — when `(seed/10) % 2 == 0`; otherwise uniform
//! over the script's yield points (one per op, the harness's crashable
//! points; the service tasks' internal yields are not crash addresses).

use std::collections::{BTreeMap, BTreeSet};

use crate::harness::{
    self, CrashSpec, LoadPhase, MediaDraw, RoundFaults, RoundOutcome, Script, ScriptOp, ScriptTask,
    TwinSupply,
};
use crate::model::{CATALOG, Model, Op, PUBLIC, to_be64};
use crate::rng::{Rng, derive};
use crate::sim::{FaultClass, FaultKnobs, MediaKind};
use crate::store::ckpt::{CKPT_HOOKS, CkptHook};
use crate::store::{Mutant, StoreCfg, StoreInner, WAL_SEG};

/// Stream tags (independent splitmix64 derivations per concern; 1–6 are
/// the harness's, 7+ are the generator's).
const S_WORK: u64 = 2;
const S_CRASHP: u64 = 8;
const S_NESTED: u64 = 9;
const S_GEN: u64 = 10;
/// X5-R16 NEVER-FIRED: the targeted fallback draw's stream (a hook the
/// round's checkpoints actually reach).
const S_HOOK: u64 = 11;
/// **X5-R38 (card X-5c-F4-F1 step 3):** the uniform crash's
/// window-picker stream — one draw per uniform-mode round when
/// `w11_share + w12_share > 0`, never drawn (and never constructed)
/// when both shares are 0, so BASE consumes no stream state on it.
const S_CWIN: u64 = 12;
/// **X5-R41 (card X-5c-F4-F1-F1-F3 step 1; a 5.2 amendment, fixed
/// before this card's probes): the alignment selection stream.** One
/// draw per round when `r41_share > 0` **and** the round is eligible (a
/// targeted crash, no drop this round), never drawn (and never
/// constructed) when the share is 0, so BASE consumes no stream state
/// on it and every BASE round's script is bit-for-bit the pre-X5-R41
/// generator's. Independent of `S_GEN` and of `S_R41C`.
const S_R41: u64 = 14;
/// **X5-R41:** the selected block's content stream — the merge tree
/// `tm`, the split tree `ts`, and the two dense runs' bases. Drawn only
/// inside a selected round, so an unselected round consumes nothing on
/// it (the card's Limit 3).
const S_R41C: u64 = 15;
/// **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5a; a 5.2 amendment, fixed
/// before any measurement of this card): the prior-window alignment
/// selection stream.** One draw per round when `r41p_share > 0` **and**
/// the round is eligible (X5-R41-selected, a pre-step-5 targeted hook,
/// `target_ckpt ≥ 1`), never drawn (and never constructed) when the
/// share is 0, so BASE consumes no stream state on it and every BASE
/// round's script is bit-for-bit the pre-X5-R50 generator's.
/// Independent of `S_GEN`, `S_R41` and `S_R41PC`.
const S_R41P: u64 = 16;
/// **X5-R50:** block A's content stream — the prior-window alignment
/// block's merge tree, split tree and dense-run bases (block B keeps
/// `S_R41C`, so block B is bit-for-bit the same in a round with block
/// A and without it). Drawn only inside an eligible selected round at
/// `r41p_share > 0`.
const S_R41PC: u64 = 17;

/// **X5-R37 (P ruling nv3-X-5c-F4-esc-1; card X-5c-F4-F1 step 1): the
/// frozen tuning.** Every generator constant the X-5c-F4 retune may
/// change, in one place. A seed pin's identity is `(generator, seed)`,
/// not the seed alone, so the generator is a named value with two
/// instances:
///
/// - [`Tuning::BASE`] — 1b0ce54e's generator, bit for bit. It is
///   **never edited** (a later store fix that moves a BASE digest
///   updates that digest literal in its own commit, naming the seeds
///   and why — X5-R37's moved-digest rule). Every seed pin runs BASE.
/// - [`Tuning::FINAL`] — the campaign's generator (`run`/`verify`
///   re-run seeds under it). X-5c-F4-F3 sets it to S1 and runs the
///   pre-registered retune ladders on it; every other card keeps
///   `FINAL == BASE`.
/// - [`Tuning::MP_PRESSURE`] — X5-R44's MP-pressure tuning, written
///   over FINAL so it follows it (step 9 may set its ladder fields).
///
/// The knob-derivation streams and the draw order are unchanged by this
/// refactor: with `Tuning::BASE` every draw consumes the same stream
/// state with the same arguments as before, which the digest-literal
/// pin `x5_r37_base_tuning_reproduces_1b0ce54e` proves over every seed
/// any test passes to a seed runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuning {
    // ---- Knobs::derive's ranges and tables (5.1) ----
    /// Committer tasks: `lo + below(span)` (2–4).
    pub committers_lo: usize,
    /// The committer span.
    pub committers_span: u64,
    /// User trees: `lo + below(span)` (2–4).
    pub trees_lo: u64,
    /// The tree span.
    pub trees_span: u64,
    /// Keys per tree: `lo + below(span)`.
    pub keyspace_lo: u64,
    /// The keyspace span.
    pub keyspace_span: u64,
    /// The SyncOff fraction table, in tenths.
    pub syncoff_table: [u64; 3],
    /// The `map_leaf_cap` table.
    pub map_leaf_cap_table: [u64; 3],
    /// The `freeset_leaf_bits` table.
    pub freeset_leaf_bits_table: [u64; 3],
    /// The `R_EPOCH` table.
    pub r_epoch_table: [usize; 3],
    /// The F-CRASH `keep_latest_pm` table (per-mille).
    pub keep_latest_pm_table: [u32; 5],
    /// The F-CRASH `tear_pm` table (per-mille).
    pub tear_pm_table: [u32; 5],
    /// The round count by `below(10)` draw (weights 50/30/20).
    pub rounds_weights: [u32; 10],
    /// The targeted checkpoint index: `below(span)`.
    pub target_ckpt_span: u64,
    /// Checkpoint trigger: `(lo + below(span)).max(1)` commits.
    pub ckpt_every_lo: u64,
    /// The checkpoint-trigger span.
    pub ckpt_every_span: u64,
    // ---- the commit and chunked op counts and the value range ----
    /// The shared commit budget per round: `lo + below(span)`.
    pub commit_budget_lo: u64,
    /// The commit-budget span.
    pub commit_budget_span: u64,
    /// Ops per small commit: `lo + below(span)`.
    pub commit_ops_lo: u64,
    /// The per-commit op span.
    pub commit_ops_span: u64,
    /// The chunked transaction's op count: `lo + below(span)`.
    pub chunked_ops_lo: u64,
    /// The chunked op span.
    pub chunked_ops_span: u64,
    /// The small-commit value floor (B); values draw in
    /// `[value_lo, value_hi]`.
    pub value_lo: u64,
    /// The value ceiling (B) — `V_INLINE`'s 400 B cap.
    pub value_hi: u64,
    // ---- the delete rate ----
    /// A commit op is a Delete iff `below(delete_one_in) == 0`.
    pub delete_one_in: u64,
    // ---- the GC sweep count ----
    /// GC sweeps per GC task: `lo + below(span)`.
    pub gc_sweeps_lo: u64,
    /// The GC sweep span.
    pub gc_sweeps_span: u64,
    // ---- the bulk-load/drop shares ----
    /// A round runs a bulk load iff `below(bulk_load_one_in) == 0`.
    pub bulk_load_one_in: u64,
    /// A non-bulk round runs a DropTree iff `below(drop_one_in) == 0`.
    pub drop_one_in: u64,
    // ---- new fields, both 0 in BASE and so not drawn ----
    /// **Delete bursts** (X-5c-F4's Q4 candidate, from P's probe of
    /// 2026-11-04): the permille of one tree's chunked keys a round
    /// deletes in one contiguous burst, as commits of at most 20 ops.
    /// 0 in BASE: no burst is built and no burst draw is made.
    pub burst_frac: u64,
    /// Extra GC sweeps on the burst's tree after the burst (P's probe
    /// used 8). 0 in BASE: none run.
    pub burst_gc_loops: u64,
    /// **X5-R38's W11 share** (permille): in a uniform-mode round with
    /// `w11_share + w12_share > 0`, the probability the crash point is
    /// drawn over the W11 window list (a completed WAL sync leaving the
    /// synced end exactly at an untouched recycled segment boundary)
    /// instead of the uniform draw. F4's knob, each share in
    /// `[0.05, 0.20]`; 0 in BASE.
    pub w11_share: u32,
    /// **X5-R38's W12 share** (permille): the same, over the W12 window
    /// list (a `wal_recycle` rename returned, its `sync_dir` not yet
    /// complete). 0 in BASE.
    pub w12_share: u32,
    /// **X5-R41 (card X-5c-F4-F1-F1-F3 step 1; a 5.2 amendment, fixed
    /// before this card's probes): the window-targeted alignment share**
    /// (permille). In a round selected by the `S_R41` stream with this
    /// probability — a targeted-crash round with no drop — the GC task
    /// runs the alignment block of step 1 inside the target checkpoint's
    /// own window (the checkpointer holds it open at `After0`), so the
    /// block's splits, merges and LPID reuses land in that window's own
    /// counters (Q4's halves, and the reuse Q4-I keys on). 0 in BASE;
    /// **1000 in FINAL (X-5c-F4-F3 step 1: S1, the setting of verdict
    /// `2e8d1b13` — no other value without a new ruling)**.
    pub r41_share: u64,
    /// **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5a; a 5.2 amendment,
    /// fixed before any measurement of this card): the prior-window
    /// alignment share** (permille). In a round selected by this
    /// probability — an X5-R41-selected round whose targeted hook is
    /// one of `Before0`–`After4` (a crash before step 5 recovers image
    /// `idx−1`) with `target_ckpt ≥ 1` — the GC task runs a second
    /// alignment block (block A) inside checkpoint `idx−1`'s own held
    /// window, so the reuse the recovery opens lands in that window's
    /// own counts (the image arm Q4-I keys on). Block B and every other
    /// round's script are unchanged; block A's content draws come from
    /// `S_R41PC`, so block B is bit-for-bit the same with or without
    /// it. 0 in BASE; **500 in FINAL (X-5c-F4-F3 step 1: S1, the
    /// setting of verdict `2e8d1b13` — no other value without a new
    /// ruling)**.
    pub r41p_share: u64,
}

impl Tuning {
    /// 1b0ce54e's generator, bit for bit (X5-R37). Never edited.
    pub const BASE: Tuning = Tuning {
        committers_lo: 2,
        committers_span: 3,
        trees_lo: 2,
        trees_span: 3,
        keyspace_lo: 1200,
        keyspace_span: 1801,
        syncoff_table: [0, 3, 7],
        map_leaf_cap_table: [8, 16, 254],
        freeset_leaf_bits_table: [128, 512, 32_576],
        r_epoch_table: [1, 2, 4],
        keep_latest_pm_table: [100, 300, 500, 700, 900],
        tear_pm_table: [100, 300, 500, 700, 900],
        rounds_weights: [1, 1, 1, 1, 1, 2, 2, 2, 3, 3],
        target_ckpt_span: 3,
        ckpt_every_lo: 4,
        ckpt_every_span: 5,
        commit_budget_lo: 18,
        commit_budget_span: 20,
        commit_ops_lo: 1,
        commit_ops_span: 20,
        chunked_ops_lo: 320,
        chunked_ops_span: 381,
        value_lo: 8,
        value_hi: 400,
        delete_one_in: 4,
        gc_sweeps_lo: 2,
        gc_sweeps_span: 3,
        bulk_load_one_in: 4,
        drop_one_in: 3,
        burst_frac: 0,
        burst_gc_loops: 0,
        w11_share: 0,
        w12_share: 0,
        r41_share: 0,
        r41p_share: 0,
    };

    /// The campaign's generator (`run`/`verify` re-run under it).
    /// **X-5c-F4-F3 step 1: FINAL = S1** — `r41_share` 1000,
    /// `r41p_share` 500, the setting of verdict `2e8d1b13`; every other
    /// field still equals [`Tuning::BASE`], and the pre-registered
    /// ladders change only the knobs the card lists. Pinned by
    /// `x5_r37_final_r41_is_s1` (S1's two constants),
    /// `x5_r37_final_stays_in_card_ranges` (Limit 4) and
    /// `x5_r37_mp_pressure_r41_equals_final`.
    pub const FINAL: Tuning = Tuning {
        committers_lo: 2,
        committers_span: 3,
        trees_lo: 2,
        trees_span: 3,
        keyspace_lo: 1200,
        keyspace_span: 1801,
        syncoff_table: [0, 3, 7],
        map_leaf_cap_table: [8, 16, 254],
        freeset_leaf_bits_table: [128, 512, 32_576],
        r_epoch_table: [1, 2, 4],
        keep_latest_pm_table: [100, 300, 500, 700, 900],
        tear_pm_table: [100, 300, 500, 700, 900],
        rounds_weights: [1, 1, 1, 1, 1, 2, 2, 2, 3, 3],
        target_ckpt_span: 3,
        ckpt_every_lo: 4,
        ckpt_every_span: 5,
        commit_budget_lo: 18,
        commit_budget_span: 20,
        commit_ops_lo: 1,
        commit_ops_span: 20,
        chunked_ops_lo: 320,
        chunked_ops_span: 381,
        value_lo: 8,
        value_hi: 400,
        delete_one_in: 4,
        gc_sweeps_lo: 2,
        gc_sweeps_span: 3,
        bulk_load_one_in: 4,
        drop_one_in: 3,
        burst_frac: 0,
        burst_gc_loops: 0,
        w11_share: 0,
        w12_share: 0,
        r41_share: 1000,
        r41p_share: 500,
    };

    /// **X5-R44 (card X-5c-F4-F2 step 2; REFRESH 9; owner decision
    /// 2026-11-05, PLAN §10 item 11): the MP-pressure tuning**, written
    /// as `Tuning { ..Tuning::FINAL }` so it starts equal to FINAL and
    /// follows it — this card edits neither BASE nor FINAL (Limit 2),
    /// and the retune is X-5c-F4-F3's. Only the judged mp-pressure set
    /// and the `--probe mp-pressure` form select it, through
    /// `TuningId::MpPressure`; because it equals FINAL at this tip,
    /// only the id can show which one a set selects.
    pub const MP_PRESSURE: Tuning = Tuning { ..Tuning::FINAL };
}

/// The per-seed knobs (card Work 5.1, sized against the ≤ 20 min
/// campaign budget of 5.3 — per-seed work shrinks, never the seed count).
#[derive(Debug, Clone)]
pub struct Knobs {
    /// Committer tasks (2–4).
    pub committers: usize,
    /// User trees besides the catalog (2–4; ids `2..=trees+1`).
    pub trees: u64,
    /// Keys per tree (0–3,000; drawn from [1200, 3000] so one checkpoint
    /// interval can dirty `≥ 4·F` distinct LPIDs, Work 5.3).
    pub keyspace: u64,
    /// The SyncOff fraction, in tenths, from {0, 3, 7}.
    pub syncoff_tenths: u64,
    /// `map_leaf_cap` from {8, 16, 254}.
    pub map_leaf_cap: u64,
    /// `freeset_leaf_bits` from {128, 512, 32_576}.
    pub freeset_leaf_bits: u64,
    /// `R_EPOCH` from {1, 2, 4}.
    pub r_epoch: usize,
    /// Commits per round (the shared budget, besides the setup, chunked
    /// and DDL commits).
    pub commit_budget: u64,
    /// Checkpoint trigger (commits).
    pub ckpt_every: u64,
    /// F-CRASH knobs.
    pub fault: FaultKnobs,
    /// Rounds 1–3 (weights 50/30/20).
    pub rounds: u32,
    /// The targeted checkpoint index (0-based completion order) for
    /// targeted crashes.
    pub target_ckpt: u64,
    /// The chunked transaction's op count for the round that draws one
    /// (300–1,500 per the card; ≥ 320 here so the two-segment span is
    /// always reachable within the 400 B value cap).
    pub chunked_ops: u64,
    /// This round runs a bulk load (25% of rounds).
    pub bulk_load: bool,
    /// This round runs a DropTree (a published tree; the walk may span a
    /// checkpoint).
    pub drop_tree: bool,
}

impl Knobs {
    /// Derive every knob from the seed (5.1), under the generator
    /// constants of `tun` (X5-R37: the tuning is a named input, so a
    /// seed pin can pin `(Tuning::BASE, seed)` while the campaign runs
    /// `Tuning::FINAL`). `round` shapes the per-round feature mix
    /// (5.2): every round's stream is an independent derivation, so the
    /// mix is a pure function of `(seed, round)`.
    pub fn derive(seed: u64, round: u32, tun: &Tuning) -> Self {
        let mut r = Rng::new(derive(seed, S_WORK));
        let committers = tun.committers_lo + r.below(tun.committers_span) as usize;
        let trees = tun.trees_lo + r.below(tun.trees_span);
        let keyspace = tun.keyspace_lo + r.below(tun.keyspace_span);
        let syncoff_tenths = tun.syncoff_table[r.below(3) as usize];
        let map_leaf_cap = tun.map_leaf_cap_table[r.below(3) as usize];
        let freeset_leaf_bits = tun.freeset_leaf_bits_table[r.below(3) as usize];
        let r_epoch = tun.r_epoch_table[r.below(3) as usize];
        let commit_budget = tun.commit_budget_lo + r.below(tun.commit_budget_span);
        // ≥ 3 checkpoints per round (the targeted index ≤ 2 must exist),
        // and intervals short enough that many separate the dirtied set.
        let ckpt_every = (tun.ckpt_every_lo + r.below(tun.ckpt_every_span)).max(1);
        let fault = FaultKnobs {
            keep_latest_pm: tun.keep_latest_pm_table[r.below(5) as usize],
            tear_pm: tun.tear_pm_table[r.below(5) as usize],
        };
        let rounds = tun.rounds_weights[r.below(10) as usize];
        let target_ckpt = r.below(tun.target_ckpt_span);
        let chunked_ops = tun.chunked_ops_lo + r.below(tun.chunked_ops_span);
        let _ = round;
        // The per-round feature mix (5.2) from the round's own stream.
        let mut fr = Rng::new(derive(seed * 1_000_003 + round as u64, S_GEN));
        let bulk_load = fr.below(tun.bulk_load_one_in) == 0;
        let drop_tree = !bulk_load && fr.below(tun.drop_one_in) == 0;
        Self {
            committers,
            trees,
            keyspace,
            syncoff_tenths,
            map_leaf_cap,
            freeset_leaf_bits,
            r_epoch,
            commit_budget,
            ckpt_every,
            fault,
            rounds,
            target_ckpt,
            chunked_ops,
            bulk_load,
            drop_tree,
        }
    }

    /// The pool size (§5.4): `tasks × (max_height + 2) + R_EPOCH +
    /// R_MAP`, with `R_MAP = 0` (Work 12) and `max_height = 4` (a
    /// 3,000-key tree with the smallest values stays under height 5).
    pub fn pool_frames(&self) -> usize {
        let tasks = self.committers + 5;
        tasks * (4 + 2) + self.r_epoch
    }

    /// The store config of the seed.
    pub fn store_cfg(&self, mutant: Option<Mutant>) -> StoreCfg {
        StoreCfg {
            map_leaf_cap: self.map_leaf_cap,
            freeset_leaf_bits: self.freeset_leaf_bits,
            pool_frames: self.pool_frames(),
            r_epoch: self.r_epoch,
            mutant,
            wal_io_fail: None,
        }
    }

    /// The knob digest (crc32c of the canonical knob image).
    pub fn digest(&self) -> u32 {
        let s = format!(
            "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.committers,
            self.trees,
            self.keyspace,
            self.syncoff_tenths,
            self.map_leaf_cap,
            self.freeset_leaf_bits,
            self.r_epoch,
            self.commit_budget,
            self.ckpt_every,
            self.fault.keep_latest_pm,
            self.fault.tear_pm,
            self.rounds,
            self.target_ckpt,
            self.chunked_ops,
            u8::from(self.bulk_load) + 2 * u8::from(self.drop_tree),
        );
        crate::crc::crc32c(0, s.as_bytes())
    }
}

/// The seed's fault class (5.4): `seed % 10 ∈ 0..=6` C (7,000 seeds),
/// `{7, 8}` MP (2,000), `9` MW (1,000).
pub fn class_of(seed: u64) -> FaultClass {
    match seed % 10 {
        0..=6 => FaultClass::C,
        7 | 8 => FaultClass::Mp,
        _ => FaultClass::Mw,
    }
}

/// The nested recovery crashes drawn for a round (5.5 step 4): r ∈ {0, 1,
/// 2} with weights 60/30/10.
pub fn nested_draw_for(seed: u64, round: u32) -> u32 {
    let mut r = Rng::new(derive(seed * 61 + round as u64, S_NESTED));
    match r.below(10) {
        0..=5 => 0,
        6..=8 => 1,
        _ => 2,
    }
}

// ---- the script generator (5.1/5.2) ----

/// The trees the recovered store can have: created and not dropped in
/// the model at `cut` (W-SCRIPT's input).
pub fn trees_at(model: &Model, cut: u64) -> BTreeSet<u64> {
    let mut trees = BTreeSet::new();
    for rec in model.commits.values() {
        if rec.ts > cut {
            continue;
        }
        for op in &rec.ops {
            match op {
                Op::CreateTree { tree } => {
                    trees.insert(*tree);
                }
                Op::DropTree { tree } => {
                    trees.remove(tree);
                }
                _ => {}
            }
        }
    }
    trees
}

/// Build round `round`'s script (5.1's task mix, 5.2's feature mix),
/// under the generator constants of `tun` (X5-R37). Pure in
/// `(seed, round, tun, knobs, model at the previous cut)` (W-DET).
/// Returns the script, the crash description, and the targeted hook's
/// name (empty for uniform crashes).
pub fn build_round(
    seed: u64,
    round: u32,
    tun: &Tuning,
    knobs: &Knobs,
    model: &Model,
    cut: u64,
) -> (Script, String, String) {
    let mut rng = Rng::new(derive(seed * 1_000_003 + round as u64 + 1, S_GEN));
    let existing = trees_at(model, cut);
    let published = model.published_at(cut);
    // The user-tree span: `2..=trees+1`. A drop round retires the
    // highest tree for the whole round (committers avoid it; a scripted
    // Put on a tree a concurrent task drops mid-round would race the
    // drop's walk — the store's tree mutex serialises it, but the model
    // and the round's noise do not need it).
    let drop_target = knobs.drop_tree.then_some(knobs.trees + 1);
    let write_max = if drop_target.is_some() {
        knobs.trees
    } else {
        knobs.trees + 1
    };
    // The setup commit: (re)create every missing tree (the catalog
    // included — the publish rows live in it, and the store creates no
    // tree implicitly) and publish every unpublished one (the first op
    // of committer task 1).
    let mut setup: Vec<Op> = Vec::new();
    for t in std::iter::once(CATALOG)
        .chain(2..=knobs.trees + 1)
        .collect::<BTreeSet<u64>>()
    {
        if !existing.contains(&t) {
            setup.push(Op::CreateTree { tree: t });
        }
    }
    let created: BTreeSet<u64> = existing
        .iter()
        .copied()
        .chain(setup.iter().filter_map(|o| match o {
            Op::CreateTree { tree } => Some(*tree),
            _ => None,
        }))
        .collect();
    for t in 2..=knobs.trees + 1 {
        if created.contains(&t) && !published.contains(&t) {
            setup.push(Op::Put {
                tree: CATALOG,
                key: to_be64(t),
                value: PUBLIC.to_vec(),
            });
        }
    }
    // ---- the committer tasks ----
    // W-SCRIPT's runtime half: when a setup commit exists, every OTHER
    // task first waits for the created trees — otherwise a concurrent
    // committer (the chunked task above all: its chunks enqueue outside
    // the commit critical section) could put its records below the
    // setup's `CreateTree` in the WAL, and LSN-ordered replay would meet
    // a tree's ops before its creating record (the seed-3 development
    // failure: "tree 2 missing in directory" at the chunked marker).
    let all_trees: Vec<u64> = (2..=knobs.trees + 1).collect();
    let has_setup = !setup.is_empty();
    let barrier = |ops: &mut Vec<ScriptOp>| {
        if has_setup {
            ops.insert(
                0,
                ScriptOp::WaitTrees {
                    trees: all_trees.clone(),
                },
            );
        }
    };
    let mut committers: Vec<Vec<ScriptOp>> = vec![Vec::new(); knobs.committers];
    if has_setup {
        committers[0].push(ScriptOp::Commit {
            sync_on: true,
            ops: setup,
        });
    }
    // The chunked transaction (5.2): one large chunked txn per round, on
    // a seeded committer, spanning ≥ 2 segments by value sizing.
    let chunked_task = 1 + rng.below(knobs.committers as u64) as usize;
    let n = knobs.chunked_ops;
    // Per-op encoded size ≈ 24 + v; the stream must cross 132 KiB (≥ 2
    // segment boundaries) within the 400 B value cap. Keys are drawn
    // without replacement (one op per entry key per transaction —
    // I-ENTRY-UNIQUE, exactly like the small commits).
    let need = 132 * 1024u64;
    let v_min = (need.saturating_sub(24 * n)).div_ceil(n);
    let v_lo = v_min.clamp(tun.value_lo, tun.value_hi);
    let mut chunked_keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut chunked_ops: Vec<Op> = Vec::new();
    while chunked_ops.len() < n as usize {
        let key = to_be64(rng.below(keyspace_of(knobs)));
        if !chunked_keys.insert(key.clone()) {
            continue;
        }
        chunked_ops.push(Op::Put {
            tree: 2 + rng.below(write_max - 1),
            key,
            value: value_of(&mut rng, v_lo, tun),
        });
    }
    // **X5-R37's delete-burst knobs** (X-5c-F4's Q4 candidate; P's
    // probe of 2026-11-04): with `burst_frac > 0`, one burst per round
    // deletes a contiguous `burst_frac` permille of one tree's chunked
    // keys as commits of at most 20 ops, and `burst_gc_loops` extra GC
    // sweeps run on that tree (the block at the GC task below). The
    // draws live inside these two blocks, so BASE (`burst_frac == 0`)
    // consumes no stream state on them and its scripts are bit-for-bit
    // the pre-refactor generator's.
    let (burst_tree, burst_keys) = if tun.burst_frac > 0 {
        let tb = 2 + rng.below(write_max - 1);
        let mut mine: Vec<Vec<u8>> = chunked_ops
            .iter()
            .filter_map(|o| match o {
                Op::Put { tree, key, .. } if *tree == tb => Some(key.clone()),
                _ => None,
            })
            .collect();
        mine.sort();
        let run = mine.len() as u64 * tun.burst_frac / 1000;
        let start = if !mine.is_empty() && mine.len() as u64 > run {
            rng.below(mine.len() as u64 - run) as usize
        } else {
            0
        };
        let burst: Vec<Vec<u8>> = mine
            .iter()
            .skip(start)
            .take(run as usize)
            .cloned()
            .collect();
        (tb, burst)
    } else {
        (0, Vec::new())
    };
    committers[chunked_task - 1].push(ScriptOp::Chunked {
        sync_on: rng.below(10) >= knobs.syncoff_tenths,
        ops: chunked_ops,
    });
    // The burst's delete commits (empty in BASE: nothing is pushed and
    // no draw is made).
    for ch in burst_keys.chunks(20) {
        committers[chunked_task - 1].push(ScriptOp::Commit {
            sync_on: true,
            ops: ch
                .iter()
                .map(|k| Op::Delete {
                    tree: burst_tree,
                    key: k.clone(),
                })
                .collect(),
        });
    }
    // The small commits: the shared budget of commits, round-robin over
    // the committers, 1–20 ops each (5.1), the SyncOff mix, 25% deletes.
    let mut ci = 0usize;
    for _ in 0..knobs.commit_budget {
        let take = tun.commit_ops_lo + rng.below(tun.commit_ops_span);
        // One op per (tree, key) per commit: two ops on the same entry
        // key in one transaction would carry the same ts, and only an
        // identical re-insert is legal (I-ENTRY-UNIQUE; D13's domain) —
        // the generator never builds one.
        let mut seen: BTreeSet<(u64, Vec<u8>)> = BTreeSet::new();
        let mut ops: Vec<Op> = Vec::new();
        while ops.len() < take as usize {
            let tree = 2 + rng.below(write_max - 1);
            let key = to_be64(rng.below(knobs.keyspace));
            if !seen.insert((tree, key.clone())) {
                continue;
            }
            if rng.below(tun.delete_one_in) == 0 {
                ops.push(Op::Delete { tree, key });
            } else {
                ops.push(Op::Put {
                    tree,
                    key,
                    value: value_of(&mut rng, tun.value_lo, tun),
                });
            }
        }
        committers[ci].push(ScriptOp::Commit {
            sync_on: rng.below(10) >= knobs.syncoff_tenths,
            ops,
        });
        ci = (ci + 1) % knobs.committers;
    }
    // A long reader (5.1) on the last committer: register a snapshot and
    // hold it for a seeded span, then release (the op burns the span).
    committers[knobs.committers - 1].push(ScriptOp::SnapshotHold {
        span: 4 + rng.below(16),
    });
    // The setup barrier at the FRONT of every committer except the setup
    // task itself (its own ops are task-ordered after the setup commit).
    for ops in committers.iter_mut().skip(1) {
        barrier(ops);
    }
    // ---- the GC sweeper task: sweeps concurrent with checkpoints ----
    // The draws run in F4's fixed order (base sweeps, then the burst's).
    let mut base_sweep_ops: Vec<ScriptOp> = Vec::new();
    for _ in 0..tun.gc_sweeps_lo + rng.below(tun.gc_sweeps_span) {
        let tree = 2 + rng.below(write_max - 1);
        base_sweep_ops.push(ScriptOp::Yields(1 + rng.below(4)));
        base_sweep_ops.push(ScriptOp::Gc { tree, w: None });
    }
    // The burst's extra GC sweeps on its own tree (empty in BASE).
    let mut burst_sweep_ops: Vec<ScriptOp> = Vec::new();
    for _ in 0..tun.burst_gc_loops {
        burst_sweep_ops.push(ScriptOp::Yields(2 + rng.below(6)));
        burst_sweep_ops.push(ScriptOp::Gc {
            tree: burst_tree,
            w: None,
        });
    }
    // **X5-R41 (card X-5c-F4-F1-F1-F3 step 1; a 5.2 amendment, fixed
    // before this card's probes): the window-targeted SMO alignment.**
    // A round is selected when all hold: a targeted crash (`seed / 10`
    // even — the crash hooks of checkpoint index 0–2 are the reachable
    // early windows), no drop this round (`!knobs.drop_tree` — the
    // block's dense runs and sweep must not race the DDL task's drop
    // walk), and a draw from the round's own `S_R41` stream below
    // `r41_share` permille. Nothing is drawn at share 0 (BASE and this
    // card's FINAL); every content draw of the block comes from the
    // second new stream `S_R41C`, so an unselected round consumes no
    // stream state and its script is bit-for-bit the generator's.
    //
    // The block (at the front of the GC task, after the barrier):
    // - two sync-on commits of 20 Puts each at the value ceiling, a
    //   dense run of 40 keys in tree `tm` — the run overfills whatever
    //   leaf holds it, so splits land wherever it runs;
    // - two sync-on commits of Deletes of those keys — the deletes
    //   leave tombstones whose elision empties the run's leaves;
    // - `R41Wait` (spins until the checkpointer's hold opens the
    //   window — see the op's arm);
    // - `Gc { tree: tm }` — the sweep elides the tombstones and merges
    //   the emptied leaves, freeing their LPIDs **inside the window**
    //   (X5-R40's design took its merge material from the chunked
    //   txn's own keys, which stay above `w` until that txn's marker
    //   applies — the block's deletes are in their own commits, so
    //   their ts is below the sweep's watermark the moment they
    //   apply);
    // - one sync-on commit of 20 Puts at the ceiling, a dense run in a
    //   tree other than `tm` when one exists — the run's splits
    //   allocate LPIDs, and `alloc_lpid` reuses the freed ones in the
    //   same window (the reuse Q4-I keys on);
    // - `R41Done`, which releases the checkpointer's hold.
    // A `R41Arm { idx: knobs.target_ckpt }` goes before the barrier:
    // the checkpointer holds the window of exactly that checkpoint
    // index — the crash's own.
    let r41 = (seed / 10).is_multiple_of(2)
        && !knobs.drop_tree
        && r41_pick(seed, round, tun) == Some(true);
    let r41_block: Vec<ScriptOp> = if r41 {
        let mut g = Rng::new(derive(seed * 100 + round as u64, S_R41C));
        let span = write_max - 1; // |2..=write_max|
        let tm = 2 + g.below(span);
        let ts = if span >= 2 {
            2 + (tm - 2 + 1 + g.below(span - 1)) % span
        } else {
            tm
        };
        let ks = knobs.keyspace;
        let n = 40u64.min(ks);
        let base = g.below(ks - n + 1);
        let hi = tun.value_hi as usize;
        let mut block: Vec<ScriptOp> = Vec::new();
        for chunk in [base..base + n / 2, base + n / 2..base + n] {
            block.push(ScriptOp::Commit {
                sync_on: true,
                ops: chunk
                    .map(|k| Op::Put {
                        tree: tm,
                        key: to_be64(k),
                        value: vec![0x5A; hi],
                    })
                    .collect(),
            });
        }
        for chunk in [base..base + n / 2, base + n / 2..base + n] {
            block.push(ScriptOp::Commit {
                sync_on: true,
                ops: chunk
                    .map(|k| Op::Delete {
                        tree: tm,
                        key: to_be64(k),
                    })
                    .collect(),
            });
        }
        block.push(ScriptOp::R41Wait {
            idx: knobs.target_ckpt,
        });
        block.push(ScriptOp::Gc { tree: tm, w: None });
        let rl = 20u64.min(ks);
        let b2 = g.below(ks - rl + 1);
        block.push(ScriptOp::Commit {
            sync_on: true,
            ops: (b2..b2 + rl)
                .map(|k| Op::Put {
                    tree: ts,
                    key: to_be64(k),
                    value: vec![0xA5; hi],
                })
                .collect(),
        });
        block.push(ScriptOp::R41Done {
            idx: knobs.target_ckpt,
        });
        block
    } else {
        Vec::new()
    };
    // **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5a): the prior-window
    // alignment.** A round is eligible when all hold: it is
    // X5-R41-selected (the same predicate, unchanged); its targeted
    // hook `CKPT_HOOKS[(seed/20) % 10]` is one of `Before0`, `After0`,
    // `After1`, `Mid2`, `After2`, `After3` or `After4` — a crash
    // before step 5 recovers image `idx−1`, whose window X5-R41 never
    // aligned (seven of the ten targeted hooks are such crashes; only
    // the three post-step-5 hooks open image `idx`); and
    // `knobs.target_ckpt ≥ 1` (index `idx−1` exists). One draw from
    // the round's own `S_R41P` stream below `r41p_share` permille
    // decides; nothing is drawn at share 0 (BASE and this card's
    // FINAL) and nothing in an ineligible round.
    //
    // Block A has X5-R41's block shape exactly — the same counts
    // (two sync-on commits of 20 Puts, two of Deletes, one sweep, one
    // 20-Put commit), ceiling values, dense runs and sweep — paired
    // with index `idx−1`, and its content draws all come from the
    // second new stream `S_R41PC`, so block B is bit-for-bit the same
    // in a round with block A and without it. Under a merge-heavy
    // steady workload every checkpoint window sees frees and reuse;
    // one aligned window per round is an artifact of the spike's
    // short windows (X5-R41's own note).
    let r41p = r41
        && knobs.target_ckpt >= 1
        && matches!(
            CKPT_HOOKS[((seed / 20) % 10) as usize],
            CkptHook::Before0
                | CkptHook::After0
                | CkptHook::After1
                | CkptHook::Mid2
                | CkptHook::After2
                | CkptHook::After3
                | CkptHook::After4
        )
        && r41p_pick(seed, round, tun) == Some(true);
    let r41p_block: Vec<ScriptOp> = if r41p {
        let mut ga = Rng::new(derive(seed * 100 + round as u64, S_R41PC));
        let span = write_max - 1; // |2..=write_max|
        let tm = 2 + ga.below(span);
        let ts = if span >= 2 {
            2 + (tm - 2 + 1 + ga.below(span - 1)) % span
        } else {
            tm
        };
        let ks = knobs.keyspace;
        let n = 40u64.min(ks);
        let base = ga.below(ks - n + 1);
        let hi = tun.value_hi as usize;
        let mut block: Vec<ScriptOp> = Vec::new();
        for chunk in [base..base + n / 2, base + n / 2..base + n] {
            block.push(ScriptOp::Commit {
                sync_on: true,
                ops: chunk
                    .map(|k| Op::Put {
                        tree: tm,
                        key: to_be64(k),
                        value: vec![0x5A; hi],
                    })
                    .collect(),
            });
        }
        for chunk in [base..base + n / 2, base + n / 2..base + n] {
            block.push(ScriptOp::Commit {
                sync_on: true,
                ops: chunk
                    .map(|k| Op::Delete {
                        tree: tm,
                        key: to_be64(k),
                    })
                    .collect(),
            });
        }
        block.push(ScriptOp::R41Wait {
            idx: knobs.target_ckpt - 1,
        });
        block.push(ScriptOp::Gc { tree: tm, w: None });
        let rl = 20u64.min(ks);
        let b2 = ga.below(ks - rl + 1);
        block.push(ScriptOp::Commit {
            sync_on: true,
            ops: (b2..b2 + rl)
                .map(|k| Op::Put {
                    tree: ts,
                    key: to_be64(k),
                    value: vec![0xA5; hi],
                })
                .collect(),
        });
        block.push(ScriptOp::R41Done {
            idx: knobs.target_ckpt - 1,
        });
        block
    } else {
        Vec::new()
    };
    // Assemble: the barrier, then (in a selected round) block A (the
    // prior window's — checkpoint `idx−1` opens first) and block B,
    // then the base sweeps and the burst's; the arms go at the very
    // front, before the barrier, **only in a selected round** — an
    // unselected round's GC task is bit-for-bit the generator's. Block
    // B's arm is op 0 (slot 0), block A's beside it (slot 1).
    let mut gc_ops: Vec<ScriptOp> = Vec::new();
    barrier(&mut gc_ops);
    gc_ops.extend(r41p_block);
    gc_ops.extend(r41_block);
    gc_ops.extend(base_sweep_ops);
    gc_ops.extend(burst_sweep_ops);
    if r41 {
        gc_ops.insert(
            0,
            ScriptOp::R41Arm {
                idx: knobs.target_ckpt,
            },
        );
    }
    if r41p {
        gc_ops.insert(
            1,
            ScriptOp::R41Arm {
                idx: knobs.target_ckpt - 1,
            },
        );
    }
    // ---- the DDL/bulk task ----
    let mut ddl_ops: Vec<ScriptOp> = Vec::new();
    barrier(&mut ddl_ops);
    if knobs.bulk_load {
        // §12.8's real `BulkLoad`: a fresh tree id (never a user tree),
        // 90–160 entries, values 8–200 B (≤ V_INLINE).
        let tree = 50 + round as u64;
        let n = 90 + rng.below(71);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
            .map(|i| {
                (
                    to_be64(i % (4 * knobs.keyspace / 5)),
                    value_of(&mut rng, tun.value_lo, tun),
                )
            })
            .collect();
        ddl_ops.push(ScriptOp::Yields(1 + rng.below(3)));
        ddl_ops.push(ScriptOp::BulkLoad { tree, entries });
    } else if knobs.drop_tree {
        // The drop of a published tree (5.2): catalog Delete + DropTree
        // in one commit (the model's drop rule), after some yields so a
        // checkpoint window can be open around the walk.
        ddl_ops.push(ScriptOp::Yields(2 + rng.below(6)));
        ddl_ops.push(ScriptOp::Commit {
            sync_on: true,
            ops: vec![
                Op::Delete {
                    tree: CATALOG,
                    key: to_be64(knobs.trees + 1),
                },
                Op::DropTree {
                    tree: knobs.trees + 1,
                },
            ],
        });
    }
    ddl_ops.push(ScriptOp::Yields(2 + rng.below(4)));
    let mut tasks: Vec<ScriptTask> = committers
        .into_iter()
        .map(|ops| ScriptTask { ops })
        .collect();
    tasks.push(ScriptTask { ops: gc_ops });
    tasks.push(ScriptTask { ops: ddl_ops });
    let (crash, desc, hook) = crash_spec(seed, round, knobs, &tasks);
    (
        Script {
            tasks,
            writer: true,
            evictor: true,
            checkpointer_every: Some(knobs.ckpt_every),
            crash,
            nested: Vec::new(),
        },
        desc,
        hook,
    )
}

/// A keyspace draw for the chunked transaction: spread over the whole
/// keyspace.
fn keyspace_of(knobs: &Knobs) -> u64 {
    knobs.keyspace
}

/// A value of length drawn in `[lo, tun.value_hi]` (always ≤ `V_INLINE`;
/// no overflow chains — declared).
fn value_of(rng: &mut Rng, lo: u64, tun: &Tuning) -> Vec<u8> {
    let hi = tun.value_hi;
    let len = lo + rng.below(hi - lo + 1);
    let mut v = Vec::with_capacity(len as usize);
    for _ in 0..len {
        v.push((rng.next_u64() & 0xFF) as u8);
    }
    v
}

/// X5-R16 NEVER-FIRED: derive a crash spec that the round's checkpoints
/// actually reach, from the per-index hook masks of a round that ran its
/// workload to completion (an unfired attempt — its checkpoints all
/// completed, so the masks are complete). Keeps the drawn hook, moving
/// its index to one that reaches it (the drawn index if it does, else
/// the smallest); when the drawn hook is unreachable at every index
/// (seed 660 round 0's shape: the tiny pool's evictor wrote every dirty
/// page, so step 2 of every checkpoint wrote nothing and `Mid2` never
/// fired — "its step 2 has too little to write"), falls to a hook the
/// round does reach, drawn deterministically from `(seed, round)` over
/// the union mask — the same adaptation the uniform draw makes to the
/// counted yield total. The workload itself is untouched (no retune:
/// X5-R17 is X-5c-F4's, per X5-R31).
///
/// **X-5c-F2 step 1 (a): an empty mask list, or a union of zero (no
/// checkpoint reached any hook at all) is a loud `Err`, never a
/// re-derive.** The draft's fallback loop walks `CKPT_HOOKS` in order
/// decrementing `left`, and with `union == 0` no candidate ever
/// matches, so `left` never reaches 0 and the loop spins forever — a
/// hang, in the exact path that exists to fix an unfired round. The
/// caller records the `Err` as a seed-level defect (beside the
/// fired-guard's, which the unfired attempt trips anyway: both are
/// counted in `DEFECT_SEEDS`), and the round is never re-run on a spec
/// no checkpoint can reach.
pub fn reachable_hook_spec(
    masks: &[u16],
    hook: crate::store::ckpt::CkptHook,
    drawn_idx: u64,
    seed: u64,
    round: u32,
) -> Result<(u64, crate::store::ckpt::CkptHook), String> {
    let bit = crate::store::ckpt::hook_bit(hook);
    let reached: Vec<u64> = masks
        .iter()
        .enumerate()
        .filter(|(_, mask)| *mask & bit != 0)
        .map(|(i, _)| i as u64)
        .collect();
    if let Some(first) = reached.first() {
        let idx = if reached.contains(&drawn_idx) {
            drawn_idx
        } else {
            *first
        };
        Ok((idx, hook))
    } else {
        let union: u16 = masks.iter().fold(0u16, |a, m| a | m);
        if union == 0 {
            // Covers the empty list too (its fold is 0): no checkpoint
            // of the attempt reached any hook, so there is nothing to
            // re-derive a reachable spec from.
            return Err(format!(
                "no checkpoint hook was ever reached ({} complete masks, union 0): \
                 the crash spec cannot be made reachable",
                masks.len()
            ));
        }
        let mut hrng = Rng::new(derive(seed * 13 + round as u64, S_HOOK));
        let mut pick = hook;
        let mut left = 1 + hrng.below(10);
        while left > 0 {
            // Walk the reached hooks in `CKPT_HOOKS` order, `left`
            // steps: deterministic, and lands on a reached hook (a
            // completed checkpoint always reaches `Before0`).
            for cand in crate::store::ckpt::CKPT_HOOKS {
                if crate::store::ckpt::hook_bit(cand) & union != 0 {
                    pick = cand;
                    left -= 1;
                    if left == 0 {
                        break;
                    }
                }
            }
        }
        let idx = masks
            .iter()
            .position(|m| crate::store::ckpt::hook_bit(pick) & *m != 0)
            .map(|i| i as u64)
            .unwrap_or(drawn_idx);
        Ok((idx, pick))
    }
}

/// The round's crash point (5.5): targeted at hook
/// `CKPT_HOOKS[(seed/20) % 10]` of a seeded checkpoint when
/// `(seed/10) % 2 == 0`, else uniform over the round's yield points.
/// The uniform draw is **scheduler-yield-level**: the driver first
/// counts the round's total yields (a counting pass with the crash
/// disabled), then arms [`CrashSpec::Yield`] at `1 + below(count)` — so
/// every yield point of every task (the service tasks' env calls, the
/// checkpoint's internals, the rename/dirsync windows) is an address,
/// which is what 5.5's "uniform over the round's yield points" says.
pub fn crash_spec(
    seed: u64,
    round: u32,
    knobs: &Knobs,
    tasks: &[ScriptTask],
) -> (Option<CrashSpec>, String, String) {
    if (seed / 10).is_multiple_of(2) {
        let hook = CKPT_HOOKS[((seed / 20) % 10) as usize];
        let idx = knobs.target_ckpt;
        (
            Some(CrashSpec::Hook {
                ckpt_index: idx,
                hook,
            }),
            format!("{hook:?}@{idx}"),
            format!("{hook:?}"),
        )
    } else {
        let mut rng = Rng::new(derive(seed * 100 + round as u64, S_CRASHP));
        let total: u64 = tasks.iter().map(|t| t.ops.len() as u64).sum();
        if total == 0 {
            return (None, "none".to_owned(), String::new());
        }
        // A placeholder: the driver replaces it with the counted,
        // scheduler-level point once the counting pass has run.
        let k = 1 + rng.below(total);
        (
            Some(CrashSpec::TaskYield(1, k)),
            format!("uniform@y:{k}"),
            String::new(),
        )
    }
}

/// Draw the uniform crash's scheduler-yield point (5.5): `k` uniform in
/// `1..=counted`, from the round's own stream.
pub fn uniform_yield_point(seed: u64, round: u32, counted: u64) -> u64 {
    let mut rng = Rng::new(derive(seed * 100 + round as u64, S_CRASHP));
    1 + rng.below(counted.max(1))
}

/// **X5-R38 (card X-5c-F4-F1 step 3; a 5.5 amendment, pre-registered
/// before the final run): the uniform crash's window pick.** In a
/// uniform-mode round whose tuning has `w11_share + w12_share > 0`, one
/// draw from the round's own `S_CWIN` stream picks the W12 window with
/// probability `w12_share`, the W11 window with probability
/// `w11_share`, and otherwise the existing uniform point unchanged; a
/// picked window takes its point uniformly over the counting pass's
/// yield list for that window, and an empty list falls back to the
/// uniform point (recorded as `w12-`/`w11-`). With both shares 0 (BASE,
/// and this card's FINAL) nothing is drawn and the uniform point stands
/// — BASE digests never move on it.
pub fn window_crash_point(
    seed: u64,
    round: u32,
    obs: &harness::WorkloadObs,
    tun: &Tuning,
    counted: u64,
) -> (u64, Option<&'static str>) {
    if tun.w11_share as u64 + tun.w12_share as u64 == 0 {
        return (uniform_yield_point(seed, round, counted), None);
    }
    let mut r = Rng::new(derive(seed * 100 + round as u64, S_CWIN));
    let d = r.below(1000);
    let w12 = tun.w12_share as u64;
    let w11 = tun.w11_share as u64;
    let pick = |list: &[u64], r: &mut Rng| list.get(r.below(list.len() as u64) as usize).copied();
    if d < w12 {
        match pick(&obs.w12_yields, &mut r) {
            Some(k) => (k, Some("w12")),
            None => (uniform_yield_point(seed, round, counted), Some("w12-")),
        }
    } else if d < w12 + w11 {
        match pick(&obs.w11_yields, &mut r) {
            Some(k) => (k, Some("w11")),
            None => (uniform_yield_point(seed, round, counted), Some("w11-")),
        }
    } else {
        (uniform_yield_point(seed, round, counted), None)
    }
}

/// **X5-R41 (card X-5c-F4-F1-F1-F3 step 1): the alignment selection.**
/// One draw from the round's own `S_R41` stream, true with probability
/// `r41_share` permille. Returns `None` — **no draw made** — when
/// `r41_share == 0` (BASE and this card's FINAL): the stream is never
/// constructed, so no stream state is consumed (the card's Limit 3) and
/// the round's script is bit-for-bit the pre-X5-R41 generator's. The
/// `None`/`Some` split is the draw-count contract the
/// `x5_r41_not_drawn_at_base` pin asserts (drawing at `r41_share == 0`
/// would return `Some(false)`, observable here and nowhere else — the
/// stream is independent, so no digest can see it); the caller draws
/// only for eligible rounds (a targeted crash, no drop), so a uniform
/// or drop round consumes nothing even at a non-zero share.
pub fn r41_pick(seed: u64, round: u32, tun: &Tuning) -> Option<bool> {
    if tun.r41_share == 0 {
        return None;
    }
    let mut r = Rng::new(derive(seed * 100 + round as u64, S_R41));
    Some(r.below(1000) < tun.r41_share)
}

/// **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5a): the prior-window
/// alignment selection.** One draw from the round's own `S_R41P`
/// stream, true with probability `r41p_share` permille. Returns
/// `None` — **no draw made** — when `r41p_share == 0` (BASE and this
/// card's FINAL): the stream is never constructed, so no stream state
/// is consumed (the card's Limit 2) and the round's script is
/// bit-for-bit the pre-X5-R50 generator's — pinned by
/// `x5_r50_not_drawn_at_base` (BASE pinned by X5-R37's digest
/// literals). The caller draws only for eligible rounds (X5-R41
/// selected, a pre-step-5 targeted hook, `target_ckpt ≥ 1`), so an
/// ineligible round consumes nothing even at a non-zero share — pinned
/// by `x5_r50_ineligible_rounds_draw_nothing`.
pub fn r41p_pick(seed: u64, round: u32, tun: &Tuning) -> Option<bool> {
    if tun.r41p_share == 0 {
        return None;
    }
    let mut r = Rng::new(derive(seed * 100 + round as u64, S_R41P));
    Some(r.below(1000) < tun.r41p_share)
}

/// **X5-R42 (card X-5c-F4-F1-F1-F3 step 6): `q4i_raw`** — did the
/// window of the checkpoint whose meta the final recovery opened hold a
/// pre-freeze reuse? Pure in the per-seed ledger (`ckpt_id →
/// (splits, merges, whole-window reuses, pre-freeze reuses)`, last
/// write wins: the row for an id is always the most recent checkpoint
/// with that id, which is exactly the checkpoint whose meta bytes sit
/// in that id's slot) and the opened meta's `ckpt_id`. Reads the
/// **pre-freeze** element (X-5c-F4-F1-F1-F3-F1 step 2a, the owner's
/// row text: "reused before its step 2 froze the table"); the
/// whole-window count stays the Q4 arm's. `None` (a refused or failed
/// open — no opened meta) or a missing row (the harness checkpoint's
/// id, never recorded) is false.
pub fn q4i_raw_of(
    windows: &BTreeMap<u64, (u64, u64, u64, u64)>,
    opened_ckpt_id: Option<u64>,
) -> bool {
    opened_ckpt_id.is_some_and(|k| windows.get(&k).is_some_and(|w| w.3 >= 1))
}

/// **X5-R42 (card X-5c-F4-F1-F1-F3-F1 step 3b): the per-round ledger
/// fold, pure** — the exact steps the driver took inline before this
/// card: insert this attempt's ledger rows, key `q4i_raw` on the
/// opened meta's own id, then drop the harness checkpoint's id (a
/// later round's recovery can open its meta, and its window is not
/// recorded; without the removal a stale row from an older checkpoint
/// of the same id could answer for its meta). Pure over the ledger, so
/// the harness-checkpoint removal is pinnable directly
/// (`x5_r42_harness_checkpoint_image_is_not_q4i`; no 0..1000 round
/// exercises the divergence, measured — the pure case is the card's
/// named fallback). **G-LEDGER-NOREMOVE** (the harness checkpoint
/// stays in the ledger) drops the removal here and fails that pin.
pub fn fold_ckpt_windows(
    ckpt_windows: &mut BTreeMap<u64, (u64, u64, u64, u64)>,
    rows: &[(u64, u64, u64, u64, u64)],
    opened_ckpt_id: Option<u64>,
    harness_ckpt_id: Option<u64>,
) -> bool {
    for &(k, sp, mg, ru, rupf) in rows {
        ckpt_windows.insert(k, (sp, mg, ru, rupf));
    }
    let raw = q4i_raw_of(ckpt_windows, opened_ckpt_id);
    if let Some(hk) = harness_ckpt_id {
        ckpt_windows.remove(&hk);
    }
    raw
}

/// **X5-R42: `q4i`** — the Q4-I row's per-round predicate:
/// `q4i_raw` ∧ the round's own crash fired ∧ Q18's `end.is_some()`
/// (excluded like SIZING: nested recovery crashes — only the final
/// recovery's opened meta is read — the harness checkpoint, and refused
/// or failed opens).
pub fn q4i_of(q4i_raw: bool, fired: bool, end_some: bool) -> bool {
    q4i_raw && fired && end_some
}

/// A round whose crash spec never fired is a **harness defect** (5.5
/// step 2: every round crashes). A silent no-crash round would leave
/// every downstream field — faults, recovery, the oracle inputs —
/// meaningless while still being judged, so the driver records this and
/// fails the seed. The `TaskYield` draw could not miss (its address
/// space is the script's own op count); the scheduler-level `Yield`
/// draw can, if the counting pass and the real round ever diverge, and
/// so can an untriggered targeted hook — both are defects, never
/// passes.
pub fn crash_fired_defect(out: &harness::RoundOutcome, crash_desc: &str) -> Option<String> {
    if out.obs.crash_fired {
        None
    } else {
        Some(format!("the crash spec never fired ({crash_desc})"))
    }
}

/// Whether two crash specs name the same point (X5-R21 (c)'s pairing
/// diagnostic: the twin's final attempted spec vs the real round's
/// final attempted spec).
pub fn same_crash_spec(a: &CrashSpec, b: &CrashSpec) -> bool {
    match (a, b) {
        (
            CrashSpec::Hook {
                ckpt_index: i1,
                hook: h1,
            },
            CrashSpec::Hook {
                ckpt_index: i2,
                hook: h2,
            },
        ) => i1 == i2 && h1 == h2,
        (CrashSpec::Yield(k1), CrashSpec::Yield(k2)) => k1 == k2,
        (CrashSpec::TaskYield(t1, k1), CrashSpec::TaskYield(t2, k2)) => t1 == t2 && k1 == k2,
        _ => false,
    }
}

// ---- the per-round and per-seed records (Work 7) ----

/// The per-round record (Work 7's fields plus the quota flags of 5.6).
#[derive(Debug, Clone, Default)]
pub struct RoundRec {
    /// The crash point's description (hook@index or uniform@t:n).
    pub crash: String,
    /// The scripted crash fired during the workload.
    pub fired: bool,
    /// The targeted hook (Q1's per-hook count); empty for uniform.
    pub hook: String,
    /// Nested recovery crashes that fired.
    pub nested: u32,
    /// Media faults by kind (this round's slice).
    pub media_lost: u64,
    /// MISDIRECTED media faults.
    pub media_misdirected: u64,
    /// TORN media faults.
    pub media_torn: u64,
    /// `lo` (Work 2.2).
    pub lo: u64,
    /// `hi` (Work 2.2).
    pub hi: u64,
    /// The matched `c`.
    pub c: Option<u64>,
    /// The recovered apply frontier.
    pub frontier: u64,
    /// The opened meta's `gc_w`.
    pub gc_w: u64,
    /// O-1/O-1f/O-2/O-3 outcomes (the failure texts, when they failed).
    pub o1_fail: Option<String>,
    /// O-1f failed (the frontier is not a satisfying cut; reported
    /// apart, never inside the rule).
    pub o1f_fail: bool,
    /// O-2's failure text.
    pub o2_fail: Option<String>,
    /// O-3's failure text.
    pub o3_fail: Option<String>,
    /// O-3 (a)'s input (reported beside the rule, never inside it).
    pub served: bool,
    /// X5-R30 (a): every served range of the round lies wholly inside
    /// slots of the kept set `K` (vacuously true when nothing was
    /// served). A diagnostic beside the rule, never an oracle input;
    /// evidence for F3's O-3 (a) question.
    pub served_k: bool,
    /// Monitor hits.
    pub monitors: Vec<&'static str>,
    /// XX001 detection sites.
    pub detections: Vec<&'static str>,
    /// Acked commits lost above the cut: total, and the SyncOff ones.
    pub acked_lost: u64,
    /// The SyncOff subset of the acked commits lost.
    pub acked_lost_syncoff: u64,
    /// X5-R6: O-1 comparisons at the matched `c` satisfied by a
    /// detected loss (class MP only; reported beside the rule, never
    /// inside it).
    pub o1_detected_loss: u32,
    /// X5-R6: this round is an R6 round (class MP/MW, recovery `Ok`,
    /// at least one XX001).
    pub r6_round: bool,
    /// **X5-R32 (b):** the damaged-referenced arm fired (the verdict's
    /// `r32` — a diagnostic the pass fold never reads; from this card's
    /// Acceptance file on, every `r32 = true` round must be class MP).
    pub r32_round: bool,
    /// **X-5c-F3-F1 step 6 (X5-R30 (c)): `skx`** — rule 1's predicate,
    /// exact. A diagnostic beside the rule, never an oracle input;
    /// judging it is P's (a served round passes iff
    /// `(skx ∨ smx) ∧ pass_na`, applied from the final file with no
    /// re-run).
    pub skx: bool,
    /// **X-5c-F3-F1 step 6 (X5-R30 (c)): `smx`** — rule 2's predicate,
    /// exact (the opened meta slot's own serve, its bytes a
    /// sync_all-covered DWrite). A diagnostic, never an oracle input.
    pub smx: bool,
    /// **X-5c-F3-F1 step 6 (X5-R30 (c)): `pass_na`** — the round's
    /// verdict re-evaluated with `no_taint`, recorded only when
    /// `served ∧ (skx ∨ smx)`, else null. A diagnostic, never an oracle
    /// input.
    pub pass_na: Option<bool>,
    /// **X5-R34 (f):** the round's `ack_over` — the SyncOn commit ts's
    /// whose own §12.4 ack wait returned before the crash while their
    /// record ends above the derived durable end `D` (and outside the
    /// recycled set `Rc`). Empty when there are none; non-empty is an
    /// ACK-OVER-HOLE store failure (the round's task error carries the
    /// text). A diagnostic the pass fold never reads.
    pub ack_over: Vec<u64>,
    /// **S-1-F3 §14 step 3a (W6 (ii); X-5c-F3-F3b step 2):** the round's
    /// recovery `Err` is I-WAL-COVER's refusal (`Fail::WalLoss`, a typed
    /// match). A diagnostic in the record; the pass fold does not read
    /// it. X5-R33 (h) counts such rounds per class through it.
    pub cover_err: bool,
    /// **X5-R33 (c): `h`** — the opened meta's writing-life bound.
    pub x33h: u64,
    /// **X5-R33 (c): `a`** — the round's largest X5-R34-acked SyncOn ts
    /// below the first damaged WAL byte.
    pub x33a: u64,
    /// **X5-R33 (e): `wal_loss_explained`.**
    pub x33x: bool,
    /// **X5-R36 (e): `reuse_on_dropping`** — the opened image's
    /// directory holds some tree `t` as `dropping` while the model
    /// holds `CreateTree{t}` at a ts in `(T, frontier]` (the id-reuse
    /// hazard; `harness::reuse_on_dropping` computes it from the opened
    /// image's durable bytes and the model, never store memory). A
    /// diagnostic the pass fold never reads and the digest never sees:
    /// the round still runs the full oracle and counts by its outcome,
    /// and any `true` is an ESCALATE to P with the round.
    pub reuse_on_dropping: bool,
    /// X-5c: script ops that unwound on a client-visible XX001.
    pub client_xx001: u32,
    /// X5-R5: the opened meta is a rollback.
    pub meta_rollback: bool,
    /// X5-R5: `|R|`.
    pub r_len: usize,
    /// The round passed (the seed pass fold).
    pub pass: bool,
    /// **X5-R38 (card X-5c-F4-F1 step 3):** the uniform crash's window
    /// pick — `None` (the plain uniform point, and every targeted
    /// round), or `Some("w11" | "w12")` (a picked window's yield) or
    /// `Some("w11-" | "w12-")` (the window's list was empty; the
    /// uniform point stood). A digest input **only when it is not the
    /// plain uniform** (`Some`), so BASE digests do not move; never a
    /// quota or judgement input.
    pub cwin: Option<&'static str>,
    /// Per-seed failure flags.
    pub deadlock: bool,
    /// A livelock occurred (X5-R13 (b)): some world of the round
    /// exceeded `YIELD_BUDGET`. Folded into `deadlock` (it counts in
    /// `DEADLOCK=` and the rule's n) and recorded beside it.
    pub livelock: bool,
    /// The site of the yield that tripped the budget (X5-R13 (b)).
    pub livelock_site: String,
    /// A task panicked.
    pub panic: bool,
    /// An S-BATON violation was observed.
    pub baton: bool,
    /// A script task failed (store/env error); the round fails.
    pub task_error: Option<String>,
    /// The quota flags (5.6), one per named quota.
    pub q4_split_merge: bool,
    /// **X-5c-F4-F1-F1 step 1 (the Q4 halves):** splits inside the
    /// crash-time checkpoint window (the store's `ckpt_splits` counter
    /// at the `obs` snapshot — reset at each `ckpt:begin`, counting only
    /// while a checkpoint is open; at the crash it reads the current or
    /// last-opened window's count). A record diagnostic serialized in
    /// `campaign.json`, never a digest input and never a quota input.
    pub q4sp: u64,
    /// **X-5c-F4-F1-F1 step 1:** the same for merges (`ckpt_merges`).
    pub q4mg: u64,
    /// **X5-R41 (card X-5c-F4-F1-F1-F3 step 2): window reuses in the
    /// crash's window** — `alloc_lpid` handed out an LPID freed since
    /// this flip (the store's `ckpt_reuse` at the crash snapshot, reset
    /// per window like `q4sp`/`q4mg`). A record diagnostic serialized in
    /// `campaign.json`, never a digest input and never a quota input.
    pub q4reuse: u64,
    /// **X5-R41/R42 (step 2): the splits of the window of the last
    /// checkpoint whose step 5 returned** (the last ledger row at the
    /// crash snapshot; 0 when none returned). Diagnostics, never digest
    /// inputs.
    pub imgsp: u64,
    /// **X5-R41/R42:** the same window's merges.
    pub imgmg: u64,
    /// **X5-R41/R42:** the same window's reuses.
    pub imgreuse: u64,
    /// **X5-R42 (card X-5c-F4-F1-F1-F3-F1 step 2a): the same window's
    /// pre-freeze reuses** — `alloc_lpid` reuses of the last completed
    /// window counted only before its step 2 froze the table (the store
    /// observable `ckpt_reuse_pf`). The Q4-I row's `q4i_raw` reads this
    /// count through the per-seed ledger; `imgreuse` above keeps the
    /// whole window for the Q4 arm. A record diagnostic serialized in
    /// `campaign.json` as `imgrupf`, never a digest input.
    pub imgreuse_pf: u64,
    /// **X5-R42 (step 2): the opened image's own window held a reuse** —
    /// the window of the checkpoint whose meta this round's **final
    /// recovery opened** (by that meta's `ckpt_id`, from the per-seed
    /// ledger: a meta rollback reads the older image's window, never the
    /// last step-5 checkpoint's; the harness checkpoint's windows are
    /// not recorded, so its id has no row). Excluded like SIZING:
    /// nested recovery crashes (only the final recovery's opened meta is
    /// read), the harness checkpoint, and refused or failed opens (no
    /// opened meta). A record diagnostic, never a digest input.
    pub q4i_raw: bool,
    /// **X5-R42:** `q4i_raw` ∧ the round's own crash fired ∧ Q18's
    /// `end.is_some()` — the Q4-I row's per-round predicate (X5-R42's
    /// row counts it over the campaign's whole 10k). Never a digest
    /// input: it stays out of the digest so X5-R37's BASE literals
    /// cannot move, and `verify` compares it per round between the file
    /// and the re-run instead.
    pub q4i: bool,
    /// Q5: a ≥ 2-segment chunked txn open at the crash (no marker).
    pub q5_open: bool,
    /// Q5: a chunked txn marked, its marker above the flips so far.
    pub q5_marked: bool,
    /// Q5: a chunked txn marked and partially applied at the crash.
    pub q5_partial: bool,
    /// Q6: the crash fell between a bulk load and its publication.
    pub q6_unpublished: bool,
    /// Q6: the crash fell after the publication (the load in the image).
    pub q6_published: bool,
    /// Q7: the crash fell during a DropTree walk.
    pub q7_drop_walk: bool,
    /// Q8: acked SyncOff commits sit above the matched cut.
    pub q8_syncoff_lost: bool,
    /// Q9: some flip imaged part of a commit above its `T`.
    pub q9_partial_above_t: bool,
    /// Q10: `R_EPOCH` was exhausted (an epoch copy dirty-evicted).
    pub q10_exhausted: bool,
    /// Q11: a stale record was read at a renamed segment's start.
    pub q11_stale_seg: bool,
    /// Q12: a G1 rename was dropped by the F-CRASH draw.
    pub q12_dropped_rename: bool,
    /// Q13: a LOST fault landed on a referenced slot.
    pub q13_lost: bool,
    /// Q13: a MISDIRECTED fault landed on a referenced slot.
    pub q13_misdirected: bool,
    /// Q13: a TORN fault landed on a referenced slot.
    pub q13_torn: bool,
    /// Q14: a meta-slot fault (slot 1 or 2, either side of a stray).
    pub q14_meta_fault: bool,
    /// Q15: a LOST or TORN durable WAL record.
    pub q15_durable_record: bool,
    /// Q16: the persisted map radix has ≥ 2 levels.
    pub q16_map_depth2: bool,
    /// Q17: FreeSet allocation grew the file in a step 2.
    pub q17_freeset_grew: bool,
    /// Q18: a GC sweep advanced past the previous `gc_w` inside an open
    /// checkpoint window of a round that recovered an image.
    pub q18_gc_advanced: bool,
    /// Work 5.3's sizing assertion for this round's intervals (a miss is
    /// a harness defect, never a pass).
    pub sizing_ok: bool,
    /// **SIZING-C (card X-5c-F4-F1 step 4; ESCALATE 2):** the crash cut
    /// a ≥ 4·F interval — the crash fired and either the open interval
    /// at the crash had already reached 4·F, or a checkpoint was open at
    /// the crash whose begin step had folded a ≥ 4·F interval. A
    /// diagnostic pending OQ-1 (whether SIZING's per-seed definition
    /// changes to count crash-cut intervals is the owner's question);
    /// never a digest input, never an exit input.
    pub szc: bool,
    /// **X5-R39 (card X-5c-F4-F2 step 1; OQ-1's ruling, PLAN §10 item
    /// 10): `szq`** — the crash cut a ≥ 4·F interval (`szc`) **and the
    /// round's final recovery opened a store** (Q18's `end.is_some()`,
    /// read at the same point). PLAN §10 item 10's exclusion: "rounds
    /// whose final recovery yields no state (refused or failed open,
    /// Q18's condition)" never count — "a refused round tests
    /// detection, not the recovery under pressure". The SZC, SZC-MP and
    /// SZC-MW rows count it. A record diagnostic serialized as `szq`,
    /// never a digest input; `verify` compares it per round between the
    /// file and the re-run.
    pub szq: bool,
    /// **X5-R39: `szi`** — `szq`, the open checkpoint's begin-step fold
    /// ≥ 4·F (`ckpt_closed_dirty`), and the crash inside the
    /// [`crate::store::ckpt::ckpt_unflipped`] window (from the
    /// completion of step 0 until step 5's `sync_all` returned): the
    /// interrupted-checkpoint floor — "the same, but only from the
    /// completion of step 0 until step 5's `sync_all` returns" (PLAN
    /// §10 item 10) — counted **inside** the 1,000, not on top of it.
    /// The SZI row counts it. A record diagnostic serialized as `szi`,
    /// never a digest input; compared per round like `szq`.
    pub szi: bool,
    /// **X5-R46 (card X-5c-F4-F2 step 2; REFRESH 9): `rfs`** — the
    /// round's final recovery produced no store (`end.is_none()`, read
    /// at the same point as Q18's `end.is_some()`): a refused or
    /// failed-open round. The MP refusal rate (`MP_REFUSAL`) counts it
    /// over class MP only, reported and never an exit input. A record
    /// diagnostic serialized as `rfs`, never a digest input; compared
    /// per round between the file and the re-run like `szq`/`szi`.
    pub rfs: bool,
}

/// The twin's per-round outcome (judged as class C; Work 2.5).
#[derive(Debug, Clone, Default)]
pub struct TwinRound {
    /// The twin round passed (O-1/O-2/O-3 as class C requires).
    pub pass: bool,
    /// Its O-1 failure text, if any.
    pub o1_fail: Option<String>,
}

/// The per-seed record (Work 7).
#[derive(Debug, Clone, Default)]
pub struct SeedRec {
    /// The seed.
    pub seed: u64,
    /// "C" | "MP" | "MW".
    pub class: String,
    /// The knob digest.
    pub knobs: u32,
    /// The rounds, in order.
    pub rounds: Vec<RoundRec>,
    /// A round stopped the chain early (recovery returned no store).
    pub stopped_early: bool,
    /// The twin rounds' outcomes (MP/MW only).
    pub twin: Vec<TwinRound>,
    /// **Diagnostic only** (X5-R21 (c); never a digest input —
    /// [`SeedRec::finish`] does not read it — and never serialized):
    /// per executed round, whether Work 2.5's pairing held — the
    /// twin's final attempted crash spec equals the real round's
    /// final attempted spec. `true` for class C rounds (no twin) and
    /// for uniform rounds (whose twin may legitimately wrap its `k`
    /// into its own measured total; 4fedc648).
    pub twin_paired: Vec<bool>,
    /// **Diagnostic only** (X-5c-PINS-1 item 2, the `twin_paired`
    /// manner; never a digest input — [`SeedRec::finish`] does not
    /// read it — and never serialized): per executed round, whether
    /// the R21 (c) **joint arm** ran — the re-paired twin's re-run at
    /// the joint spec (`reachable_hook_spec` over the per-index AND of
    /// the two complete mask lists, X5-R48). `false` for every round
    /// without one.
    pub twin_joint_arm: Vec<bool>,
    /// **Diagnostic only** (beside [`SeedRec::twin_joint_arm`]): the
    /// stale-read count at the joint re-run's start — the reads the
    /// re-paired twin's unfired attempt left in the collector, which
    /// `treads.clear()` zeroes so the joint arm's supply (`supply2`'s
    /// `recovery_reads`) holds only the final attempt's reads (verdict
    /// X-5c-F4-F1-F1-CXX-F1 finding 2: an extra read can only suppress
    /// an oracle failure). `0` in clean code and for every round
    /// without a joint arm; the unfired attempt's read count when the
    /// clear is dropped (**G5-R48-TREADS-KEPT**; 17 stale reads at
    /// seed 7469 at CXX-F1's tip, verdict `5db63057`; no seed of
    /// 0..9999 reaches the arm at `2e8d1b13`, verdict `74f411f0`).
    pub twin_joint_stale_reads: Vec<usize>,
    /// A DEADLOCK occurred in some round.
    pub deadlock: bool,
    /// A task panicked in some round.
    pub panic: bool,
    /// An S-BATON violation was observed in some round.
    pub baton: bool,
    /// A harness defect (create failed, or a non-round failure path).
    pub defect: Option<String>,
    /// The canonical digest.
    pub digest: u64,
}

impl SeedRec {
    /// The seed passes iff every round passed, no DEADLOCK/PANIC/baton,
    /// and no harness defect (the pass fold of Work 2.4 plus X-5b's
    /// rule-4 fold).
    pub fn passes(&self) -> bool {
        self.defect.is_none()
            && !self.deadlock
            && !self.panic
            && !self.baton
            && self.rounds.iter().all(|r| r.pass)
    }

    /// Compute the canonical digest (crc32c over the record's image).
    pub fn finish(&mut self) {
        let mut s = format!(
            "{}|{}|{}|{}|{}|{}|{}|{:?}",
            self.seed,
            self.class,
            self.knobs,
            self.stopped_early,
            self.deadlock,
            self.panic,
            self.baton,
            self.defect
        );
        for r in &self.rounds {
            s.push_str(&format!(
                "|{},{},{}|{},{},{}|{},{},{},{}|{:?}|{:?}|{:?}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                r.crash,
                r.fired,
                r.nested,
                r.hook,
                r.media_lost,
                r.media_misdirected,
                r.media_torn,
                r.lo,
                r.hi,
                r.c.unwrap_or(u64::MAX),
                r.o1_fail,
                r.o2_fail,
                r.o3_fail,
                r.o1f_fail,
                r.served,
                r.served_k,
                r.meta_rollback,
                r.r_len,
                r.o1_detected_loss,
                r.r6_round,
                r.r32_round,
                r.skx,
                r.smx,
                r.pass_na.map(|b| b as u8).unwrap_or(2),
                r.client_xx001,
                r.pass,
                r.deadlock,
                r.livelock,
                r.livelock_site.as_str(),
                r.task_error.is_some(),
                (r.q4_split_merge as u8)
                    + 2 * (r.q5_open as u8)
                    + 4 * (r.q5_marked as u8)
                    + 8 * (r.q5_partial as u8)
                    + 16 * (r.q6_unpublished as u8)
                    + 32 * (r.q6_published as u8)
                    + 64 * (r.q7_drop_walk as u8)
                    + 128 * (r.q8_syncoff_lost as u8),
                (r.q9_partial_above_t as u8)
                    + 2 * (r.q10_exhausted as u8)
                    + 4 * (r.q11_stale_seg as u8)
                    + 8 * (r.q12_dropped_rename as u8)
                    + 16 * (r.q13_lost as u8)
                    + 32 * (r.q13_misdirected as u8)
                    + 64 * (r.q13_torn as u8)
                    + 128 * (r.q14_meta_fault as u8),
                (r.q15_durable_record as u8)
                    + 2 * (r.q16_map_depth2 as u8)
                    + 4 * (r.q17_freeset_grew as u8)
                    + 8 * (r.q18_gc_advanced as u8)
                    + 16 * (r.sizing_ok as u8),
                r.acked_lost,
            ));
            // **X5-R38:** the window pick enters the digest only when it
            // is not the plain uniform point, so BASE digests (no
            // shares, every pick `None`) never move on it.
            if let Some(w) = r.cwin {
                s.push_str(&format!("|cwin:{w}"));
            }
        }
        for t in &self.twin {
            s.push_str(&format!("|{}|{:?}", t.pass, t.o1_fail));
        }
        self.digest = crate::crc::crc32c(0, s.as_bytes()) as u64;
    }
}

// ---- the per-seed driver (5.5) ----

/// The round-chain state: what the next round starts from. Public with
/// public fields for the X-5c-F2 step 1 (b) pin
/// ([`twin_chain_after_repair`]); the twin chain and the real chain are
/// the same shape.
pub struct Chain {
    /// The durable disk at the chain point.
    pub disk: crate::sim::SimDisk,
    /// The recovered store at the chain point.
    pub st: crate::store::StoreInner,
    /// The model, truncated at `cut`.
    pub model: Model,
    /// The truncation cut.
    pub cut: u64,
}

/// X5-R21 (c) re-pair update (X-5c-F2 step 1 (b)): the twin chain
/// after the **re-paired** twin round — `Some(end_store)` continues it
/// from that round's own end state; `None` (the re-paired twin's
/// recovery produced no store) **re-seeds** it: the next round's supply
/// block rebuilds the twin chain from the real round's own start (Work
/// 2.5: the twin is the same script, crash and F-CRASH resolutions,
/// from the same base). The draft kept the first attempt's chain in
/// that case — a chain at a crash spec the round that ran never used.
pub fn twin_chain_after_repair(tout: &harness::RoundOutcome) -> Option<Chain> {
    tout.end_store.clone().map(|st| Chain {
        disk: tout.end_disk.clone(),
        st,
        model: harness::clone_model(&tout.end_model),
        cut: tout.cut,
    })
}

/// **X-5c-F4-F1-F1-F3-F2-F1-F2 (P ruling, (a); the directory re-seed):**
/// true iff the two chains' `dir` **key sets** differ. The `DirEntry`
/// values are not compared — roots and pointers differ between any two
/// chains — and this is exactly the set `recover`'s replay checks when
/// it fails with `Corrupt("tree N missing in directory")` (the narrow
/// DDL predicate of the twin re-seed; the two models also differ
/// whenever a real round loses ordinary commits, which is common in MW,
/// so re-seeding on a model difference would re-pair most MW rounds and
/// move digests far past the defect). Both `dir`s are `BTreeMap`s, so
/// the key comparison is order-deterministic.
pub fn twin_dirs_differ(real: &StoreInner, twin: &StoreInner) -> bool {
    real.dir.keys().ne(twin.dir.keys())
}

/// Run one seed: create, then 1–3 rounds (each with its twin first for
/// MP/MW), collecting the record. Pure in `(seed, mutant)` (W-DET).
/// Runs `Tuning::FINAL` — the campaign's generator (X5-R37).
pub fn run_seed(seed: u64, mutant: Option<Mutant>) -> SeedRec {
    run_seed_with(seed, mutant, &Tuning::FINAL)
}

/// **X5-R37 (card X-5c-F4-F1 step 1):** [`run_seed`] under an explicit
/// generator. Every seed pin runs `Tuning::BASE` (1b0ce54e's generator,
/// bit for bit) so a pin's identity is `(Tuning::BASE, seed)` and
/// X-5c-F4's retune of `Tuning::FINAL` cannot move it; the campaign's
/// `run`/`verify` keep [`run_seed`]'s FINAL.
pub fn run_seed_with(seed: u64, mutant: Option<Mutant>, tun: &Tuning) -> SeedRec {
    let class = class_of(seed);
    let base_knobs = Knobs::derive(seed, 0, tun);
    let round_counts = base_knobs.rounds;
    let mut rec = SeedRec {
        seed,
        class: class_name(class).to_owned(),
        knobs: base_knobs.digest(),
        ..SeedRec::default()
    };
    let created = harness::create_store(base_knobs.store_cfg(mutant), seed, seed as u128);
    let (disk0, st0) = match created {
        Ok(x) => x,
        Err(e) => {
            rec.defect = Some(format!("create failed: {e:?}"));
            rec.finish();
            return rec;
        }
    };
    let mut chain = Some(Chain {
        disk: disk0.clone(),
        st: st0.clone(),
        model: Model::new(),
        cut: 0,
    });
    let mut twin_chain = (class != FaultClass::C).then_some(Chain {
        disk: disk0,
        st: st0,
        model: Model::new(),
        cut: 0,
    });
    // **X5-R33 (a): the per-seed record-stream lives**, carried like the
    // model. Each real round's life (p0, its records, its boot, its
    // starting frontier, its append end) is appended after the round.
    let mut x33_lives: Vec<harness::X33Life> = Vec::new();
    let mut x33_prev_frontier = 0u64;
    // **X5-R42 (card X-5c-F4-F1-F1-F3 step 2): the per-seed checkpoint
    // window ledger** — every checkpoint's window counts for the whole
    // seed, keyed by `ckpt_id` (last write wins), each row
    // `(splits, merges, whole-window reuses, pre-freeze reuses)` (the
    // pre-freeze element added by X-5c-F4-F1-F1-F3-F1 step 2a: Q4-I
    // reads it, the Q4 arm keeps the whole-window count). A round's
    // `q4i_raw` looks up the opened meta's own id in it; the round's
    // harness checkpoint's id is removed after the round (its windows
    // are not recorded, and without the removal a stale row from an
    // older checkpoint of the same id could answer for its meta).
    let mut ckpt_windows: BTreeMap<u64, (u64, u64, u64, u64)> = BTreeMap::new();
    for round in 0..round_counts {
        let Some(ch) = chain.take() else {
            break;
        };
        let knobs = Knobs::derive(seed, round, tun);
        let cfg = knobs.store_cfg(mutant);
        let (base, _crash_desc, crash_hook) =
            build_round(seed, round, tun, &knobs, &ch.model, ch.cut);
        // The uniform crash is scheduler-yield-level: a counting pass on
        // a throwaway clone of the round's start (the crash disabled)
        // measures the total, then the real round — and its twin, which
        // must crash at the same point (Work 2.5) — arm `Yield(k)` at a
        // seeded point in `1..=count`.
        //
        // The targeted crash runs the same counting pass for its own
        // address (X5-R16 NEVER-FIRED, P ruling nv3-X-5c-esc-3): the
        // pass records, per checkpoint completion index, the hooks it
        // reached (`WorkloadObs::ckpt_hooks_hit`), and the draw keeps
        // the seed's hook but moves its index to one that reaches it
        // (the drawn index if it does; else the smallest that does).
        // Pre-fix, 150 seeds drew (index, Mid2) pairs whose checkpoint
        // step 2 had too little to write, the hook never fired, and
        // the fired-guard failed the seed ("the crash spec never
        // fired") — a harness defect, never a pass.
        let uniform = (seed / 10) % 2 == 1;
        let (script_twin, mut script_real, mut crash_desc);
        // X5-R38: the round's window pick (`None` for every targeted
        // round and every plain uniform draw).
        let mut cwin_pick: Option<&'static str> = None;
        if uniform {
            let probe = Script {
                crash: None,
                ..base.clone()
            };
            let out = harness::run_round(
                seed,
                round,
                &probe,
                &RoundFaults {
                    class: FaultClass::C,
                    knobs: knobs.fault,
                    directed: Vec::new(),
                    no_r_taint: false,
                    media: MediaDraw::None,
                    twin_supply: None,
                    nested_draw: 0,
                    probe_pass: true,
                },
                cfg.clone(),
                ch.disk.clone(),
                ch.st.clone(),
                &ch.model,
                ch.cut,
                None,
                None,
            );
            // **X5-R38 (step 3):** the uniform point — or, when the
            // tuning's window shares are non-zero, one S_CWIN draw
            // that picks a W11/W12 window yield list recorded by the
            // counting pass. BASE (shares 0) draws nothing here: the
            // plain uniform point stands and no digest moves.
            let (k, cwin) = window_crash_point(seed, round, &out.obs, tun, out.sched_yields.max(1));
            cwin_pick = cwin;
            script_twin = Script {
                crash: Some(CrashSpec::Yield(k)),
                ..base.clone()
            };
            script_real = Script {
                crash: Some(CrashSpec::Yield(k)),
                ..base
            };
            crash_desc = format!("uniform@y:{k}");
        } else {
            // **X5-R16 NEVER-FIRED, the cheap form (P ruling
            // nv3-X-5c-esc-3): no probe pass.** The drawn spec runs
            // (the twin first for MP/MW — Work 2.5 pairs their crash
            // points — the real round itself for class C); if it fires
            // (the overwhelming majority of rounds) nothing extra
            // happens. If it does not, the failed attempt's own
            // observables carry the **complete** hook masks (an
            // unfired crash means the workload ran to completion, so
            // every checkpoint completed) and the round re-runs with
            // [`reachable_hook_spec`]'s draw. Deterministic: the fired
            // flag, the masks and the re-run are all functions of
            // `(seed, mutant)`.
            script_twin = Script {
                nested: Vec::new(),
                ..base.clone()
            };
            script_real = base;
            crash_desc = _crash_desc;
        }
        let mut supply = None;
        let mut twin_points = Vec::new();
        // X5-R21 (c): the twin's final attempted crash spec and the
        // start for the pairing re-run (filled by the twin block;
        // `tch_pair` holds the same start the ruling's `tch_retry`
        // names — the twin's own round start, kept because the first
        // attempt consumes the chain's parts).
        let mut twin_final_spec: Option<CrashSpec> = None;
        let mut tch_pair: Option<Chain> = None;
        // **X5-R16 NO-TWIN (P ruling nv3-X-5c-esc-3): supply the twin
        // for every MP/MW round.** Two shapes lost it before: a twin
        // round whose recovery returned `Err` (its `end_store` was
        // `None`, so no supply was built, and the twin chain broke),
        // and any later round of the same seed (the chain was gone).
        // The real round then failed O-3 (c) as "MP/MW round with
        // XX001 and no twin (harness defect)" — 19 rounds in d2c8bba7.
        // Both are harness defects: the twin's recovery reads (up to
        // its failure — the collector runs on both arms) and the twin's
        // crashed-disk image slots (the image its recovery tried to
        // open) are exactly O-3 (c)'s sets, and a broken chain re-seeds
        // from the real round's own start (Work 2.5: the twin is the
        // same script, crash and F-CRASH resolutions, faultless, judged
        // as class C — from the same base the real round runs).
        if twin_chain.is_none() && class != FaultClass::C {
            twin_chain = Some(Chain {
                disk: ch.disk.clone(),
                st: ch.st.clone(),
                model: harness::clone_model(&ch.model),
                cut: ch.cut,
            });
        }
        // **X-5c-F4-F1-F1-F3-F2-F1-F2 (P ruling, (b); the directory
        // re-seed):** a real MW round whose WAL fault loses a DropTree
        // keeps the tree in the real chain's directory while the
        // faultless twin's drops it, and the next round's script — built
        // from the real model (`build_round(.., &ch.model, ..)`) — then
        // writes into a tree the twin's image no longer holds: the
        // twin's recovery replays a record into a missing tree and
        // fails with `Corrupt("tree N missing in directory")` (verdict
        // X-5c-F4-F1-F1-F3-F2-F1, finding 1). That twin no longer runs
        // "the same script, crash and F-CRASH resolutions, faultless …
        // from the same base" (Work 2.5), so whenever the two chains'
        // directory key sets differ at round start it is re-seeded from
        // the real round's own start — the same clone the broken-chain
        // re-seed above makes. `twin_chain` is `Some` only on MP/MW
        // seeds (class C never builds one), which is the ruling's "every
        // MP/MW round"; nothing else changes (the twin still runs class
        // C, faultless, at the same crash and resolutions, and no record
        // field or digest input is added).
        if twin_chain
            .as_ref()
            .is_some_and(|tch| twin_dirs_differ(&ch.st, &tch.st))
        {
            twin_chain = Some(Chain {
                disk: ch.disk.clone(),
                st: ch.st.clone(),
                model: harness::clone_model(&ch.model),
                cut: ch.cut,
            });
        }
        // The targeted draw of a class C round retries on its own
        // (no twin exists); keep the round's start for the re-run. The
        // MP/MW real round needs its own retry too (the twin and real
        // chains can have diverged — the twin chain re-seeds after a
        // failed twin recovery — so the twin's firing does not
        // guarantee the real round's).
        let mut ch_retry = (!uniform).then(|| Chain {
            disk: ch.disk.clone(),
            st: ch.st.clone(),
            model: harness::clone_model(&ch.model),
            cut: ch.cut,
        });
        if let Some(tch) = twin_chain.take() {
            let tf = RoundFaults {
                class: FaultClass::C,
                knobs: knobs.fault,
                directed: Vec::new(),
                no_r_taint: false,
                media: MediaDraw::None,
                twin_supply: None,
                nested_draw: nested_draw_for(seed, round),
                probe_pass: false,
            };
            // The twin's start, kept for re-runs (the first attempt
            // consumes the chain's parts): its own NEVER-FIRED re-run
            // (`tch_retry`), and — X5-R21 (c) — the pairing re-run
            // when the REAL round re-derives a targeted spec
            // (`tch_pair`, the same start).
            let tch_retry = Chain {
                disk: tch.disk.clone(),
                st: tch.st.clone(),
                model: harness::clone_model(&tch.model),
                cut: tch.cut,
            };
            tch_pair = Some(Chain {
                disk: tch.disk.clone(),
                st: tch.st.clone(),
                model: harness::clone_model(&tch.model),
                cut: tch.cut,
            });
            let mut script_twin_run = script_twin.clone();
            let mut out = {
                let mut reads = Vec::new();
                let out = harness::run_round(
                    seed,
                    round,
                    &script_twin_run,
                    &tf,
                    cfg.clone(),
                    tch.disk,
                    tch.st,
                    &tch.model,
                    tch.cut,
                    Some(&mut reads),
                    None,
                );
                (out, reads)
            };
            // **X5-R16 NEVER-FIRED**: a targeted twin whose crash never
            // fired re-derives the spec from its own complete masks and
            // re-runs; the failed attempt is discarded (its outcome
            // would trip the fired-guard for a crash point no checkpoint
            // reaches — a harness defect, never a pass). A **uniform**
            // twin whose counted point overshot (the twin chain may have
            // diverged from the real chain the count was taken on — the
            // re-seed after a failed twin recovery) wraps `k` into the
            // attempt's own measured total: an unfired crash means the
            // attempt ran to completion, so its `sched_yields` **is**
            // its total.
            let uniform_wrap = uniform && !out.0.obs.crash_fired;
            if !uniform
                && !out.0.obs.crash_fired
                && let Some(CrashSpec::Hook { ckpt_index, hook }) = script_twin_run.crash
            {
                // X-5c-F2 step 1 (a): the re-derive can refuse (no
                // checkpoint reached any hook — empty masks or union 0).
                // That is a loud seed defect, never a hang; the unfired
                // attempt this block would have replaced still trips the
                // fired-guard below, so the round can never pass.
                match reachable_hook_spec(&out.0.obs.ckpt_hooks_hit, hook, ckpt_index, seed, round)
                {
                    Ok((idx2, hook2)) => {
                        script_twin_run = Script {
                            crash: Some(CrashSpec::Hook {
                                ckpt_index: idx2,
                                hook: hook2,
                            }),
                            nested: Vec::new(),
                            ..script_twin_run
                        };
                        // The real round follows the twin's fixed spec (Work
                        // 2.5: the same crash point).
                        script_real = Script {
                            crash: Some(CrashSpec::Hook {
                                ckpt_index: idx2,
                                hook: hook2,
                            }),
                            ..script_real
                        };
                        crash_desc = format!("{hook2:?}@{idx2}");
                        let mut reads = Vec::new();
                        out = {
                            let o = harness::run_round(
                                seed,
                                round,
                                &script_twin_run,
                                &tf,
                                cfg.clone(),
                                tch_retry.disk,
                                tch_retry.st,
                                &tch_retry.model,
                                tch_retry.cut,
                                Some(&mut reads),
                                None,
                            );
                            (o, reads)
                        };
                    }
                    Err(d) => {
                        if rec.defect.is_none() {
                            rec.defect = Some(format!("twin round {round}: {d}"));
                        }
                    }
                }
            } else if uniform_wrap {
                let total = out.0.sched_yields.max(1);
                let k2 = match script_twin_run.crash {
                    Some(CrashSpec::Yield(k)) => 1 + (k - 1) % total,
                    _ => 0,
                };
                if k2 > 0 {
                    script_twin_run = Script {
                        crash: Some(CrashSpec::Yield(k2)),
                        ..script_twin_run
                    };
                    // `crash_desc` keeps describing the REAL round's
                    // point (the twin's record carries no crash desc;
                    // the real's k is counted on its own chain and
                    // fires).
                    let mut reads = Vec::new();
                    out = {
                        let o = harness::run_round(
                            seed,
                            round,
                            &script_twin_run,
                            &tf,
                            cfg.clone(),
                            tch_retry.disk,
                            tch_retry.st,
                            &tch_retry.model,
                            tch_retry.cut,
                            Some(&mut reads),
                            None,
                        );
                        (o, reads)
                    };
                }
            } else if !uniform {
                crash_desc = match script_twin_run.crash {
                    Some(CrashSpec::Hook { ckpt_index, hook }) => {
                        format!("{hook:?}@{ckpt_index}")
                    }
                    _ => crash_desc,
                };
            }
            let (out, reads) = out;
            // The final attempted spec is the twin's spec of record
            // (its re-derive branches above updated `script_twin_run`
            // before each re-run).
            twin_final_spec = script_twin_run.crash.clone();
            if let Some(d) = crash_fired_defect(&out, &crash_desc)
                && rec.defect.is_none()
            {
                rec.defect = Some(format!("twin round {round}: {d}"));
            }
            rec.twin.push(TwinRound {
                pass: out.pass,
                o1_fail: out.o1.clone().err(),
            });
            twin_points = out.nested_points.clone();
            // The supply (O-3 (c)'s sets) from the twin round — whether
            // or not its recovery succeeded (X5-R16 NO-TWIN): the
            // collected recovery reads and the twin's crashed-disk
            // image walk.
            supply = Some(TwinSupply {
                recovery_reads: reads,
                image_slots: harness::durable_image(&out.crashed_disk, &cfg).1,
            });
            if let Some(st) = out.end_store.clone() {
                twin_chain = Some(Chain {
                    disk: out.end_disk.clone(),
                    st,
                    model: out.end_model,
                    cut: out.cut,
                });
            }
        }
        // The real round: the class's faults, the twin's supply, and the
        // twin's nested points (class C draws its own).
        let script = Script {
            nested: twin_points,
            ..script_real
        };
        let faults = RoundFaults {
            class,
            knobs: knobs.fault,
            directed: Vec::new(),
            no_r_taint: false,
            media: match class {
                FaultClass::C => MediaDraw::None,
                FaultClass::Mp => MediaDraw::Page,
                FaultClass::Mw => MediaDraw::Wal,
            },
            twin_supply: supply,
            nested_draw: if class == FaultClass::C {
                nested_draw_for(seed, round)
            } else {
                0
            },
            probe_pass: false,
        };
        let mut out = harness::run_round(
            seed,
            round,
            &script,
            &faults,
            cfg.clone(),
            ch.disk,
            ch.st,
            &ch.model,
            ch.cut,
            None,
            Some(&harness::X33In {
                lives: &x33_lives,
                started_frontier: x33_prev_frontier,
            }),
        );
        // **X5-R16 NEVER-FIRED, the class C half** (no twin exists to
        // carry the drawn spec first): a targeted round whose crash
        // never fired re-derives the spec from its own complete masks
        // and re-runs from the same start; the failed attempt is
        // discarded (the fired-guard never records it).
        let mut real_final_spec: Option<CrashSpec> = script.crash.clone();
        // X-5c-PINS-1 item 2's diagnostic locals (the `twin_paired`
        // manner; recorded per round after the retry block): whether
        // the joint arm ran, and the stale-read count at its re-run's
        // start. Defaults for every round whose retry never reaches
        // the joint arm.
        let mut joint_arm_ran = false;
        let mut joint_stale_reads = 0usize;
        if let (Some(retry), false, true, Some(crash)) = (
            ch_retry.take(),
            out.obs.crash_fired,
            !uniform,
            script.crash.clone(),
        ) {
            if let CrashSpec::Hook { ckpt_index, hook } = crash {
                'derive: {
                    // X-5c-F2 step 1 (a): a refused re-derive (no hook was
                    // ever reached) is a loud seed defect; the unfired first
                    // attempt's outcome stands and trips the fired-guard, so
                    // the round can never pass and the retry is skipped.
                    let (idx2, hook2) = match reachable_hook_spec(
                        &out.obs.ckpt_hooks_hit,
                        hook,
                        ckpt_index,
                        seed,
                        round,
                    ) {
                        Ok(x) => x,
                        Err(d) => {
                            if rec.defect.is_none() {
                                rec.defect = Some(format!("round {round}: {d}"));
                            }
                            break 'derive;
                        }
                    };
                    let real_masks = out.obs.ckpt_hooks_hit.clone();
                    let mut spec2 = CrashSpec::Hook {
                        ckpt_index: idx2,
                        hook: hook2,
                    };
                    let mut script2 = Script {
                        crash: Some(spec2.clone()),
                        ..script
                    };
                    crash_desc = format!("{hook2:?}@{idx2}");
                    // **X5-R21 (c) (P ruling nv3-X-5c-F1-esc-1): the real
                    // round's retry re-pairs its twin.** The twin and real
                    // chains can diverge, so the twin's firing does not
                    // guarantee the real round's — and when the real round
                    // re-derives a targeted spec, the twin re-runs at that
                    // same spec, from its own start (`tch_pair`, the same
                    // start the ruling's `tch_retry` names), BEFORE the
                    // real round uses its supply and nested points. The
                    // first attempts' twin (at the old spec) is not the
                    // twin of the round that ran: Work 2.5's twin is the
                    // same script, crash and F-CRASH resolutions. Its
                    // record, supply, nested points and chain continuation
                    // replace the first attempts' (class C has no twin and
                    // keeps its own retry, below, unchanged).
                    let mut supply2 = None;
                    let mut twin_points2 = Vec::new();
                    if let Some(tch) = tch_pair.take() {
                        // **X5-R48:** the twin's own start for the joint
                        // re-run below (the first re-pair attempt
                        // consumes `tch`'s parts, as the first attempt
                        // consumed the chain's).
                        let tch_joint = Chain {
                            disk: tch.disk.clone(),
                            st: tch.st.clone(),
                            model: harness::clone_model(&tch.model),
                            cut: tch.cut,
                        };
                        let tscript = Script {
                            nested: Vec::new(),
                            ..script2.clone()
                        };
                        let tf_pair = RoundFaults {
                            class: FaultClass::C,
                            knobs: knobs.fault,
                            directed: Vec::new(),
                            no_r_taint: false,
                            media: MediaDraw::None,
                            twin_supply: None,
                            nested_draw: nested_draw_for(seed, round),
                            probe_pass: false,
                        };
                        let mut treads = Vec::new();
                        let mut tout = harness::run_round(
                            seed,
                            round,
                            &tscript,
                            &tf_pair,
                            cfg.clone(),
                            tch.disk,
                            tch.st,
                            &tch.model,
                            tch.cut,
                            Some(&mut treads),
                            None,
                        );
                        // **X5-R48 (amends X5-R21 (c)): the joint
                        // re-derive.** The re-paired twin ran at a spec
                        // derived only from the REAL round's masks, and
                        // the chains can diverge, so its crash may never
                        // fire — the fired-guard would then fail the
                        // seed for a crash point no checkpoint of the
                        // twin's chain reaches (7469: the R21 (c)
                        // re-pair was the one arm with no re-derive).
                        // Both unfired attempts — the real round's
                        // first, the re-paired twin — ran to completion,
                        // so both mask lists are complete: their
                        // per-index AND over the shorter length names
                        // the hooks BOTH chains reach, and one
                        // [`reachable_hook_spec`] draw on it (the same
                        // drawn `(hook, ckpt_index)`, the same `(seed,
                        // round)`) re-pairs the round at a spec both can
                        // fire. The twin re-runs at that spec from its
                        // own start; the real round then runs at the
                        // same one (`script2`/`spec2` name it below).
                        // A refused re-derive (union 0, or an empty
                        // list) draws nothing and changes nothing: the
                        // unfired attempt's fired-guard defect stands,
                        // exactly as before — and a twin that still does
                        // not fire at the joint spec trips the guard the
                        // same way. There is no third attempt.
                        if !tout.obs.crash_fired {
                            let joint: Vec<u16> = tout
                                .obs
                                .ckpt_hooks_hit
                                .iter()
                                .zip(real_masks.iter())
                                .map(|(a, b)| a & b)
                                .collect();
                            if let Ok((i3, h3)) =
                                reachable_hook_spec(&joint, hook, ckpt_index, seed, round)
                            {
                                spec2 = CrashSpec::Hook {
                                    ckpt_index: i3,
                                    hook: h3,
                                };
                                script2 = Script {
                                    crash: Some(spec2.clone()),
                                    ..script2.clone()
                                };
                                crash_desc = format!("{h3:?}@{i3}");
                                let t3 = Script {
                                    nested: Vec::new(),
                                    ..script2.clone()
                                };
                                treads.clear();
                                // X-5c-PINS-1 item 2 diagnostic: the
                                // stale-read count at the joint
                                // re-run's start. The clear above is
                                // what zeroes it — without it (the
                                // G5-R48-TREADS-KEPT shape) the
                                // collector still holds the unfired
                                // re-paired attempt's reads here, and
                                // the supply below carries them into
                                // the real round's oracle (17 at seed
                                // 7469 at CXX-F1's tip; no seed of
                                // 0..9999 reaches this arm at
                                // `2e8d1b13`). Never a digest input,
                                // never serialized; `SeedRec::finish`
                                // never reads it.
                                joint_arm_ran = true;
                                joint_stale_reads = treads.len();
                                tout = harness::run_round(
                                    seed,
                                    round,
                                    &t3,
                                    &tf_pair,
                                    cfg.clone(),
                                    tch_joint.disk,
                                    tch_joint.st,
                                    &tch_joint.model,
                                    tch_joint.cut,
                                    Some(&mut treads),
                                    None,
                                );
                            }
                        }
                        if let Some(d) = crash_fired_defect(&tout, &crash_desc)
                            && rec.defect.is_none()
                        {
                            rec.defect = Some(format!("twin round {round}: {d}"));
                        }
                        rec.twin.pop();
                        rec.twin.push(TwinRound {
                            pass: tout.pass,
                            o1_fail: tout.o1.clone().err(),
                        });
                        twin_points2 = tout.nested_points.clone();
                        supply2 = Some(TwinSupply {
                            recovery_reads: treads,
                            image_slots: harness::durable_image(&tout.crashed_disk, &cfg).1,
                        });
                        // X-5c-F2 step 1 (b): the re-paired twin's own
                        // outcome decides the chain — a recovery without
                        // a store re-seeds it (None), never keeps the
                        // first attempt's chain at the old spec.
                        twin_chain = twin_chain_after_repair(&tout);
                        twin_final_spec = Some(spec2.clone());
                    }
                    let script3 = Script {
                        nested: twin_points2,
                        ..script2
                    };
                    let faults_pair = RoundFaults {
                        class,
                        knobs: knobs.fault,
                        directed: Vec::new(),
                        no_r_taint: false,
                        media: match class {
                            FaultClass::C => MediaDraw::None,
                            FaultClass::Mp => MediaDraw::Page,
                            FaultClass::Mw => MediaDraw::Wal,
                        },
                        twin_supply: supply2,
                        nested_draw: if class == FaultClass::C {
                            nested_draw_for(seed, round)
                        } else {
                            0
                        },
                        probe_pass: false,
                    };
                    real_final_spec = Some(spec2);
                    out = harness::run_round(
                        seed,
                        round,
                        &script3,
                        &faults_pair,
                        cfg.clone(),
                        retry.disk,
                        retry.st,
                        &retry.model,
                        retry.cut,
                        None,
                        Some(&harness::X33In {
                            lives: &x33_lives,
                            started_frontier: x33_prev_frontier,
                        }),
                    );
                }
            }
        } else if !uniform && let Some(CrashSpec::Hook { ckpt_index, hook }) = script.crash {
            crash_desc = format!("{hook:?}@{ckpt_index}");
        }
        // X5-R21 (c) diagnostic (never a digest input, never
        // serialized): Work 2.5's pairing held for this round — the
        // twin's final attempted spec equals the real round's final
        // attempted spec. Judged on the targeted rounds (the fix's
        // scope); a uniform round's twin may legitimately wrap its `k`
        // into its own measured total (4fedc648), and class C has no
        // twin — both record `true`.
        rec.twin_paired
            .push(match (&twin_final_spec, &real_final_spec) {
                (Some(a), Some(b)) => {
                    let paired = uniform || same_crash_spec(a, b);
                    if std::env::var("E4_PAIR_DIAG").is_ok() {
                        eprintln!(
                            "PAIR diag: seed {seed} round {round} uniform={uniform} paired={paired} twin={a:?} real={b:?}"
                        );
                    }
                    paired
                }
                _ => true,
            });
        // X-5c-PINS-1 item 2's diagnostic (the twin_paired manner):
        // one entry per executed round, defaulting to no-joint-arm and
        // zero stale reads.
        rec.twin_joint_arm.push(joint_arm_ran);
        rec.twin_joint_stale_reads.push(joint_stale_reads);

        // **G8 (X5-R16's conditional fill):** a poisoned store ended the
        // workload — the crash was delivered at the poison point, and
        // the crash desc of record is `poison` (never a NEVER-FIRED:
        // `crash_fired` follows the world's `crashed` flag, which the
        // poison delivery sets).
        if out.obs.poison_crash {
            crash_desc = "poison".to_owned();
        }

        if let Some(d) = crash_fired_defect(&out, &crash_desc)
            && rec.defect.is_none()
        {
            rec.defect = Some(format!("round {round}: {d}"));
        }
        // X-5c-F2 step 1 (e): the anchor-0 precondition defect is
        // seed-level (counted in DEFECT_SEEDS), exactly as P's text
        // records it.
        if out.r5_anchor0_defect && rec.defect.is_none() {
            rec.defect = Some(harness::R5_ANCHOR0_DEFECT.to_owned());
        }
        let mut rr = round_rec(&knobs, &cfg, &out, &crash_desc, &crash_hook);
        // X5-R38: the window pick of record (a digest input only when
        // set — see `RoundRec::cwin`).
        rr.cwin = cwin_pick;
        // **X5-R42 (step 2): the image arm.** Fold this (final)
        // attempt's ledger rows into the per-seed ledger, key `q4i_raw`
        // on the opened meta's own id, then drop the harness
        // checkpoint's id — all in [`fold_ckpt_windows`], pure for the
        // X-5c-F4-F1-F1-F3-F1 step 3b pin.
        rr.q4i_raw = fold_ckpt_windows(
            &mut ckpt_windows,
            &out.obs.ckpt_windows,
            out.opened_ckpt_id,
            out.harness_ckpt_id,
        );
        rr.q4i = q4i_of(rr.q4i_raw, rr.fired, out.end_store.is_some());
        rec.deadlock |= rr.deadlock;
        rec.panic |= rr.panic;
        rec.baton |= rr.baton;
        if rr.task_error.is_some() && rec.defect.is_none() {
            // Not a verdict field: the round's own record carries it; a
            // defect at seed level stays create failures only.
        }
        // X5-R33 (a): append this (final attempt of the) round's life
        // and carry its frontier as the next round's starting frontier.
        if let Some(life) = out.x33_life.take() {
            x33_lives.push(life);
        }
        x33_prev_frontier = x33_prev_frontier.max(out.frontier);
        match out.end_store.clone() {
            Some(st) => {
                // Judgment call (documented in the README): the chain
                // continues only after a round that **passed** the
                // oracle. A failed round is X5-R3's root-cause unit —
                // running further rounds on its base would only stack
                // fallout of the failure the ruling must judge first
                // (the R6 case leaves a tainted referenced slot in the
                // recovered image; the next round's workload then trips
                // §3.3 as a task error). The failed round stays recorded
                // and still fails the seed.
                if out.pass {
                    chain = Some(Chain {
                        disk: out.end_disk.clone(),
                        st,
                        model: out.end_model,
                        cut: out.cut,
                    });
                } else {
                    rec.stopped_early = true;
                }
            }
            None => {
                // Recovery returned no store: 5.5's "the next round
                // continues on the recovered store" has no store to
                // continue on — the seed's rounds stop here, recorded.
                rec.stopped_early = true;
            }
        }
        rec.rounds.push(rr);
    }
    rec.finish();
    rec
}

/// Build the per-round record from the outcome: Work 7's fields and the
/// 5.6 quota flags. The flags read only the outcome (the crash-time
/// observables, this round's media slice, and the recovered end state) —
/// never store memory directly.
fn round_rec(
    _knobs: &Knobs,
    cfg: &StoreCfg,
    out: &RoundOutcome,
    crash_desc: &str,
    crash_hook: &str,
) -> RoundRec {
    let obs = &out.obs;
    // The (i)-point image slots, for Q13/Q14's "on a referenced slot".
    let image_slots = harness::durable_image(&out.crashed_disk, cfg).1;
    let pf = out.crashed_disk.stat(crate::sim::PAGE_FILE).unwrap_or(0);
    let mut rr = RoundRec {
        crash: crash_desc.to_owned(),
        ..RoundRec::default()
    };
    // The crash description: the spec the round ran. The hook name is
    // recorded beside it for Q1's per-hook count.
    rr.fired = obs.crash_fired;
    rr.hook = crash_hook.to_owned();
    rr.nested = out.nested_fired;
    for f in &out.media_faults {
        match &f.kind {
            MediaKind::Lost => rr.media_lost += 1,
            MediaKind::Misdirected { .. } => rr.media_misdirected += 1,
            MediaKind::Torn { .. } => rr.media_torn += 1,
        }
    }
    rr.lo = out.lo;
    rr.hi = out.hi;
    rr.c = out.o1.clone().ok();
    rr.frontier = out.frontier;
    rr.gc_w = out.gc_w;
    rr.o1_fail = out.o1.clone().err();
    rr.o1f_fail = !out.o1f_ok;
    rr.o2_fail = out.o2_fail.clone();
    rr.o3_fail = out.o3_fail.clone();
    rr.served = out.served;
    rr.served_k = out.served_k;
    rr.monitors = out.monitors.clone();
    rr.detections = out.detections.clone();
    rr.acked_lost = out.acked_lost.len() as u64;
    rr.acked_lost_syncoff = out.acked_lost.iter().filter(|(_, so)| *so).count() as u64;
    rr.o1_detected_loss = out.o1_detected_loss;
    rr.r6_round = out.r6_round;
    rr.r32_round = out.r32_round;
    rr.skx = out.skx;
    rr.smx = out.smx;
    rr.pass_na = out.pass_na;
    rr.ack_over = out.ack_over.clone();
    rr.cover_err = out.cover_err;
    rr.x33h = out.x33h;
    rr.x33a = out.x33a;
    rr.x33x = out.x33x;
    rr.reuse_on_dropping = out.reuse_on_dropping;
    rr.client_xx001 = out.client_xx001;
    rr.meta_rollback = out.meta_rollback;
    rr.r_len = out.rollback_slots.len();
    rr.pass = out.pass;
    rr.deadlock = out.deadlock;
    rr.livelock = out.livelock;
    rr.livelock_site = out.livelock_site.to_owned();
    rr.panic = out.panicked;
    rr.baton = out.baton;
    rr.task_error = out.task_error.clone();
    // ---- the quota flags (5.6) ----
    // Q4: the crash interrupted a checkpoint between step 0 and step 3
    // (the position codes 1..=6) with ≥ 1 split and ≥ 1 merge in the
    // window's own counters.
    rr.q4_split_merge =
        obs.crash_fired && (1..=6).contains(&obs.ckpt_step) && obs.splits >= 1 && obs.merges >= 1;
    // X-5c-F4-F1-F1 step 1: the halves themselves, beside the combined
    // flag (diagnostics only — see RoundRec::q4sp/q4mg).
    rr.q4sp = obs.splits;
    rr.q4mg = obs.merges;
    // X5-R41/R42 (card X-5c-F4-F1-F1-F3 step 2): the reuse half in the
    // crash's window, and the completed-window arm — the counts of the
    // last checkpoint whose step 5 returned (the last ledger row at the
    // crash snapshot). X-5c-F4-F1-F1-F3-F1 step 2a adds the pre-freeze
    // companion `imgreuse_pf` (Q4-I's reading); `imgreuse` keeps the
    // whole window (the Q4 arm's).
    rr.q4reuse = obs.reuses;
    if let Some(&(_, sp, mg, ru, rupf)) = obs.ckpt_windows.last() {
        rr.imgsp = sp;
        rr.imgmg = mg;
        rr.imgreuse = ru;
        rr.imgreuse_pf = rupf;
    }
    // Q5: the chunked cases. Open: a txn with chunks enqueued and no
    // marker, spanning ≥ 2 segments. Marked: a marker enqueued on a txn
    // still open (no flip has covered it — the marker sits above the
    // last flip's T). Partial: a chunked commit's apply interrupted
    // with ≥ 1 op applied.
    for oc in &obs.open_chunks {
        if oc.last - oc.first >= WAL_SEG {
            match oc.marker_ts {
                None => rr.q5_open = true,
                Some(_) => rr.q5_marked = true,
            }
        }
    }
    rr.q5_partial = obs
        .partial_applies
        .iter()
        .any(|(_, done, total, chunked)| *chunked && *done >= 1 && *done < *total);
    // Q6: the bulk-load phases at the crash.
    for (_, p) in &obs.load_phases {
        match p {
            LoadPhase::Loaded | LoadPhase::Publishing => rr.q6_unpublished = true,
            LoadPhase::Published => rr.q6_published = true,
            LoadPhase::Loading => {}
        }
    }
    // Q7: a DropTree walk was in flight.
    rr.q7_drop_walk = obs.drop_walk.is_some();
    // Q8: acked SyncOff commits above the cut (class C; in C every
    // acked-lost commit is SyncOff, since `lo` covers the acked SyncOn).
    rr.q8_syncoff_lost = rr.acked_lost_syncoff >= 1;
    // Q9: a flip imaged part of a commit above T.
    rr.q9_partial_above_t = obs.q9_partial_flips >= 1;
    // Q10/Q11/Q17: the store counters at the crash or in the recovered
    // end state (the harness checkpoint and recovery run there too).
    let end = out.end_store.as_ref();
    rr.q10_exhausted =
        obs.epoch_exhausted >= 1 || end.map(|s| s.epoch_exhausted >= 1).unwrap_or(false);
    rr.q11_stale_seg =
        obs.stale_seg_reads >= 1 || end.map(|s| s.stale_seg_reads >= 1).unwrap_or(false);
    rr.q17_freeset_grew = obs.freeset_grew || end.map(|s| s.freeset_grew).unwrap_or(false);
    // Q12: renames the F-CRASH directory draw dropped.
    rr.q12_dropped_rename = out.renames_dropped >= 1;
    // Q13/Q14: page faults on the (i)-point image's referenced slots,
    // and on a meta slot (either side of a MISDIRECTED).
    for f in &out.media_faults {
        if f.inode != pf {
            continue;
        }
        let mut slots = vec![f.off / 4096];
        if let MediaKind::Misdirected { to_off } = &f.kind {
            slots.push(*to_off / 4096);
        }
        if slots.iter().any(|s| image_slots.contains(s)) {
            match &f.kind {
                MediaKind::Lost => rr.q13_lost = true,
                MediaKind::Misdirected { .. } => rr.q13_misdirected = true,
                MediaKind::Torn { .. } => rr.q13_torn = true,
            }
        }
        if slots.iter().any(|s| *s == 1 || *s == 2) {
            rr.q14_meta_fault = true;
        }
    }
    // Q15: a LOST or TORN durable WAL record.
    rr.q15_durable_record = out
        .media_faults
        .iter()
        .any(|f| f.inode != pf && matches!(f.kind, MediaKind::Lost | MediaKind::Torn { .. }));
    // Q16: the persisted map radix has ≥ 2 levels (more LPIDs than one
    // leaf's cap) — read from the recovered meta, else the (i) walk's.
    let next_lpid = end
        .map(|s| s.meta.next_lpid)
        .or_else(|| {
            harness::durable_image(&out.crashed_disk, cfg)
                .2
                .map(|m| m.next_lpid)
        })
        .unwrap_or(0);
    rr.q16_map_depth2 = next_lpid > cfg.map_leaf_cap;
    // Q18: a GC sweep advanced past the previous gc_w inside an open
    // checkpoint window, in a round whose recovery opened an image.
    rr.q18_gc_advanced = obs.gc_adv_in_window && end.is_some();
    // Work 5.3: ≥ 4·F distinct dirtied LPIDs in at least one checkpoint
    // interval (completed intervals' max, or the crash-cut open one).
    let four_f = 4 * cfg.pool_frames as u64;
    rr.sizing_ok = sizing_ok_of(obs, four_f);
    // SIZING-C (step 4): the crash cut a ≥ 4·F interval.
    rr.szc = szc_of(obs, four_f);
    // X5-R39 (card X-5c-F4-F2 step 1): the sizing rows' predicates.
    // `szq` is `szc` and Q18's `end.is_some()` (a refused or failed-open
    // round tests detection, not the recovery under pressure); `szi` is
    // `szq`, the open checkpoint's folded interval ≥ 4·F, and the crash
    // inside the `ckpt_unflipped` window (the interrupted-checkpoint
    // floor). Record diagnostics, never digest inputs.
    rr.szq = rr.szc && out.end_store.is_some();
    rr.szi = rr.szq
        && obs.ckpt_closed_dirty >= four_f
        && crate::store::ckpt::ckpt_unflipped(obs.ckpt_step);
    // X5-R46 (card X-5c-F4-F2 step 2): the refusal flag, at the same
    // point Q18 reads its `end.is_some()`.
    rr.rfs = out.end_store.is_none();
    rr
}

/// Work 5.3's sizing flag, pure in the observables (pub for the step-4
/// directed pins): some interval of the round — a completed one
/// (`dirty_max`) or the open one at the crash (`dirty_now_len`) —
/// reached `four_f` distinct dirtied pool LPIDs.
pub fn sizing_ok_of(obs: &harness::WorkloadObs, four_f: u64) -> bool {
    obs.dirty_max >= four_f || obs.dirty_now_len >= four_f
}

/// **SIZING-C (step 4; ESCALATE 2 — a diagnostic pending OQ-1):** the
/// crash fired and either the open interval at the crash had already
/// reached `four_f`, or a checkpoint was open at the crash whose begin
/// step folded a `≥ four_f` interval (`ckpt_closed_dirty`, which is 0
/// whenever no checkpoint is open — so the second disjunct carries its
/// own openness condition).
pub fn szc_of(obs: &harness::WorkloadObs, four_f: u64) -> bool {
    obs.crash_fired && (obs.dirty_now_len >= four_f || obs.ckpt_closed_dirty >= four_f)
}

/// The class's short name.
fn class_name(class: FaultClass) -> &'static str {
    match class {
        FaultClass::C => "C",
        FaultClass::Mp => "MP",
        FaultClass::Mw => "MW",
    }
}

// ---- the coverage quotas (5.6) ----

/// One quota row: the id, what it counts, the achieved and required
/// numbers.
#[derive(Debug, Clone)]
pub struct QuotaRow {
    /// The quota id (Q1…Q18, SIZING for Work 5.3's per-seed reporting,
    /// or X5-R39's SZC/SZC-MP/SZC-MW/SZI sizing rows).
    pub id: &'static str,
    /// What the row counts.
    pub what: String,
    /// The achieved number.
    pub achieved: u64,
    /// The required minimum.
    pub required: u64,
}

/// Compute every 5.6 quota over the records. `expected_seeds` is the
/// campaign's seed count (10,000) — used by Q16's percentage.
pub fn quotas(recs: &[SeedRec], expected_seeds: u64) -> Vec<QuotaRow> {
    let mut q1: BTreeMap<String, u64> = BTreeMap::new();
    let mut q2 = 0u64;
    let mut q3 = 0u64;
    let mut q4 = 0u64;
    let (mut q5o, mut q5m, mut q5p) = (0u64, 0u64, 0u64);
    let (mut q6u, mut q6p) = (0u64, 0u64);
    let mut q7 = 0u64;
    let mut q8 = 0u64;
    let mut q9 = 0u64;
    let mut q10 = 0u64;
    let mut q11 = 0u64;
    let mut q12 = 0u64;
    let (mut q13l, mut q13m, mut q13t) = (0u64, 0u64, 0u64);
    let mut q14 = 0u64;
    let mut q15 = 0u64;
    let mut q16 = 0u64;
    let mut q17 = 0u64;
    let mut q18 = 0u64;
    // **X5-R39 (card X-5c-F4-F2 step 1): the sizing rows' round
    // counters** — `szq` per class and overall, and the `szi` floor.
    let (mut szc_all, mut szc_mp, mut szc_mw, mut szi) = (0u64, 0u64, 0u64, 0u64);
    // **X5-R17: the SIZING row counts seeds, not rounds** — Work 5.3
    // asserts per seed ("every seed must reach, in at least one
    // checkpoint interval, ≥ 4·F distinct dirtied LPIDs"), so a seed
    // misses iff **no** round of it reached the bound (a seed whose r0
    // crashed early but whose r1 reached it is met; a round-only miss
    // is not a seed miss). Pinned by
    // `x5r17_sizing_row_counts_seeds_not_rounds`.
    let mut sizing_miss = 0u64;
    for rec in recs {
        if !rec.rounds.iter().any(|r| r.sizing_ok) {
            sizing_miss += 1;
        }
        if rec.rounds.iter().any(|r| r.nested >= 1) {
            q2 += 1;
        }
        if rec.rounds.iter().any(|r| r.nested >= 2) {
            q3 += 1;
        }
        if rec.rounds.iter().any(|r| r.q8_syncoff_lost) && rec.class == "C" {
            q8 += 1;
        }
        if rec.rounds.iter().any(|r| r.q16_map_depth2) {
            q16 += 1;
        }
        // X5-R39: the per-class `szq` counts (SZC-MP/SZC-MW read them;
        // SZC counts every class).
        let mp = rec.class == "MP";
        let mw = rec.class == "MW";
        for r in &rec.rounds {
            if r.szq {
                szc_all += 1;
                if mp {
                    szc_mp += 1;
                }
                if mw {
                    szc_mw += 1;
                }
            }
            if r.szi {
                szi += 1;
            }
        }
        for r in &rec.rounds {
            if r.fired && !r.hook.is_empty() {
                *q1.entry(r.hook.clone()).or_insert(0) += 1;
            }
            if r.q4_split_merge {
                q4 += 1;
            }
            if r.q5_open {
                q5o += 1;
            }
            if r.q5_marked {
                q5m += 1;
            }
            if r.q5_partial {
                q5p += 1;
            }
            if r.q6_unpublished {
                q6u += 1;
            }
            if r.q6_published {
                q6p += 1;
            }
            if r.q7_drop_walk {
                q7 += 1;
            }
            if r.q9_partial_above_t {
                q9 += 1;
            }
            if r.q10_exhausted {
                q10 += 1;
            }
            if r.q11_stale_seg {
                q11 += 1;
            }
            if r.q12_dropped_rename {
                q12 += 1;
            }
            if r.q13_lost {
                q13l += 1;
            }
            if r.q13_misdirected {
                q13m += 1;
            }
            if r.q13_torn {
                q13t += 1;
            }
            if r.q14_meta_fault {
                q14 += 1;
            }
            if r.q15_durable_record {
                q15 += 1;
            }
            if r.q17_freeset_grew {
                q17 += 1;
            }
            if r.q18_gc_advanced {
                q18 += 1;
            }
        }
    }
    let mut rows = Vec::new();
    for (h, n) in q1.iter() {
        rows.push(QuotaRow {
            id: "Q1",
            what: format!("crash at hook {h}"),
            achieved: *n,
            required: 300,
        });
    }
    rows.extend([
        QuotaRow {
            id: "Q2",
            what: "seeds with ≥ 1 nested recovery crash".into(),
            achieved: q2,
            required: 2000,
        },
        QuotaRow {
            id: "Q3",
            what: "seeds with 2 nested recovery crashes".into(),
            achieved: q3,
            required: 200,
        },
        QuotaRow {
            id: "Q4",
            what: "interrupted ckpt with split and merge (steps 0–3)".into(),
            achieved: q4,
            required: 1000,
        },
        QuotaRow {
            id: "Q5a",
            what: "crash with a ≥ 2-segment chunked txn open (no marker)".into(),
            achieved: q5o,
            required: 200,
        },
        QuotaRow {
            id: "Q5b",
            what: "crash with a chunked txn marked above T at the flip".into(),
            achieved: q5m,
            required: 200,
        },
        QuotaRow {
            id: "Q5c",
            what: "crash with a chunked txn marked and partially applied".into(),
            achieved: q5p,
            required: 200,
        },
        QuotaRow {
            id: "Q6a",
            what: "crash between bulk load and publication".into(),
            achieved: q6u,
            required: 200,
        },
        QuotaRow {
            id: "Q6b",
            what: "crash after publication with the load in the image".into(),
            achieved: q6p,
            required: 200,
        },
        QuotaRow {
            id: "Q7",
            what: "crash during a DropTree walk".into(),
            achieved: q7,
            required: 200,
        },
        QuotaRow {
            id: "Q8",
            what: "seeds with acked SyncOff commits lost".into(),
            achieved: q8,
            required: 100,
        },
        QuotaRow {
            id: "Q9",
            what: "rounds whose image holds part of a commit > T".into(),
            achieved: q9,
            required: 500,
        },
        QuotaRow {
            id: "Q10",
            what: "rounds with R_EPOCH exhausted".into(),
            achieved: q10,
            required: 500,
        },
        QuotaRow {
            id: "Q11",
            what: "rounds with a stale record read at a renamed segment start".into(),
            achieved: q11,
            required: 100,
        },
        QuotaRow {
            id: "Q12",
            what: "rounds with a G1 rename dropped by F-CRASH".into(),
            achieved: q12,
            required: 100,
        },
        QuotaRow {
            id: "Q13a",
            what: "MP rounds with LOST on a referenced slot".into(),
            achieved: q13l,
            required: 300,
        },
        QuotaRow {
            id: "Q13b",
            what: "MP rounds with MISDIRECTED on a referenced slot".into(),
            achieved: q13m,
            required: 300,
        },
        QuotaRow {
            id: "Q13c",
            what: "MP rounds with TORN on a referenced slot".into(),
            achieved: q13t,
            required: 300,
        },
        QuotaRow {
            id: "Q14",
            what: "MP rounds with a meta-slot fault".into(),
            achieved: q14,
            required: 50,
        },
        QuotaRow {
            id: "Q15",
            what: "MW rounds with a LOST or TORN durable record".into(),
            achieved: q15,
            required: 300,
        },
        QuotaRow {
            id: "Q16",
            what: "seeds with map depth ≥ 2 (50%)".into(),
            achieved: q16,
            required: expected_seeds / 2,
        },
        QuotaRow {
            id: "Q17",
            what: "rounds where FreeSet allocation grew the file in step 2".into(),
            achieved: q17,
            required: 100,
        },
        QuotaRow {
            id: "Q18",
            what: "rounds with GC past the previous gc_w inside a recovering image".into(),
            achieved: q18,
            required: 1000,
        },
        QuotaRow {
            id: "SIZING",
            what: "seeds missing Work 5.3's >= 4*F dirtied LPIDs (per-seed reporting)".into(),
            achieved: sizing_miss,
            required: 0,
        },
        // **X5-R39 (card X-5c-F4-F2 step 1; OQ-1's ruling, PLAN §10
        // item 10): the four sizing rows.** SZC is the ≥ 1,000 quota
        // ("≥ 1,000 crash rounds whose crash cuts an interval of
        // ≥ 4·F or interrupts the checkpoint that closes one"), with
        // MP ≥ 100 and MW ≥ 50 beside it; SZI is the
        // interrupted-checkpoint floor (≥ 300, counted inside the
        // 1,000, not on top of it). The SIZING row above stays
        // per-seed reporting (PLAN item 10: "The per-seed count stays
        // reported … and is no longer an exit input").
        QuotaRow {
            id: "SZC",
            what: "crash rounds cutting a >= 4*F interval, recovery opened (szq)".into(),
            achieved: szc_all,
            required: 1000,
        },
        QuotaRow {
            id: "SZC-MP",
            what: "MP crash rounds cutting a >= 4*F interval, recovery opened".into(),
            achieved: szc_mp,
            required: 100,
        },
        QuotaRow {
            id: "SZC-MW",
            what: "MW crash rounds cutting a >= 4*F interval, recovery opened".into(),
            achieved: szc_mw,
            required: 50,
        },
        QuotaRow {
            id: "SZI",
            what: "crash rounds interrupting the checkpoint closing a >= 4*F interval".into(),
            achieved: szi,
            required: 300,
        },
    ]);
    rows
}
