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
use crate::store::ckpt::CKPT_HOOKS;
use crate::store::{Mutant, StoreCfg, WAL_SEG};

/// Stream tags (independent splitmix64 derivations per concern; 1–6 are
/// the harness's, 7+ are the generator's).
const S_WORK: u64 = 2;
const S_CRASHP: u64 = 8;
const S_NESTED: u64 = 9;
const S_GEN: u64 = 10;
/// X5-R16 NEVER-FIRED: the targeted fallback draw's stream (a hook the
/// round's checkpoints actually reach).
const S_HOOK: u64 = 11;

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
    /// Derive every knob from the seed (5.1). `round` shapes the
    /// per-round feature mix (5.2): every round's stream is an
    /// independent derivation, so the mix is a pure function of
    /// `(seed, round)`.
    pub fn derive(seed: u64, round: u32) -> Self {
        let mut r = Rng::new(derive(seed, S_WORK));
        let committers = 2 + r.below(3) as usize;
        let trees = 2 + r.below(3);
        let keyspace = 1200 + r.below(1801);
        let syncoff_tenths = [0u64, 3, 7][r.below(3) as usize];
        let map_leaf_cap = [8u64, 16, 254][r.below(3) as usize];
        let freeset_leaf_bits = [128u64, 512, 32_576][r.below(3) as usize];
        let r_epoch = [1usize, 2, 4][r.below(3) as usize];
        let commit_budget = 18 + r.below(20);
        // ≥ 3 checkpoints per round (the targeted index ≤ 2 must exist),
        // and intervals short enough that many separate the dirtied set.
        let ckpt_every = (4 + r.below(5)).max(1);
        let fault = FaultKnobs {
            keep_latest_pm: [100u32, 300, 500, 700, 900][r.below(5) as usize],
            tear_pm: [100u32, 300, 500, 700, 900][r.below(5) as usize],
        };
        let rounds = match r.below(10) {
            0..=4 => 1,
            5..=7 => 2,
            _ => 3,
        };
        let target_ckpt = r.below(3);
        let chunked_ops = 320 + r.below(381);
        let _ = round;
        // The per-round feature mix (5.2) from the round's own stream.
        let mut fr = Rng::new(derive(seed * 1_000_003 + round as u64, S_GEN));
        let bulk_load = fr.below(4) == 0;
        let drop_tree = !bulk_load && fr.below(3) == 0;
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

/// Build round `round`'s script (5.1's task mix, 5.2's feature mix).
/// Pure in `(seed, round, knobs, model at the previous cut)` (W-DET).
/// Returns the script, the crash description, and the targeted hook's
/// name (empty for uniform crashes).
pub fn build_round(
    seed: u64,
    round: u32,
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
    let v_lo = v_min.clamp(8, 400);
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
            value: value_of(&mut rng, v_lo),
        });
    }
    committers[chunked_task - 1].push(ScriptOp::Chunked {
        sync_on: rng.below(10) >= knobs.syncoff_tenths,
        ops: chunked_ops,
    });
    // The small commits: the shared budget of commits, round-robin over
    // the committers, 1–20 ops each (5.1), the SyncOff mix, 25% deletes.
    let mut ci = 0usize;
    for _ in 0..knobs.commit_budget {
        let take = 1 + rng.below(20);
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
            if rng.below(4) == 0 {
                ops.push(Op::Delete { tree, key });
            } else {
                ops.push(Op::Put {
                    tree,
                    key,
                    value: value_of(&mut rng, 8),
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
    let mut gc_ops: Vec<ScriptOp> = Vec::new();
    barrier(&mut gc_ops);
    for _ in 0..2 + rng.below(3) {
        let tree = 2 + rng.below(write_max - 1);
        gc_ops.push(ScriptOp::Yields(1 + rng.below(4)));
        gc_ops.push(ScriptOp::Gc { tree, w: None });
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
            .map(|i| (to_be64(i % (4 * knobs.keyspace / 5)), value_of(&mut rng, 8)))
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

/// A value of length drawn in `[lo, 400]` (always ≤ `V_INLINE`; no
/// overflow chains — declared).
fn value_of(rng: &mut Rng, lo: u64) -> Vec<u8> {
    let hi = 400u64;
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
    /// X-5c: script ops that unwound on a client-visible XX001.
    pub client_xx001: u32,
    /// X5-R5: the opened meta is a rollback.
    pub meta_rollback: bool,
    /// X5-R5: `|R|`.
    pub r_len: usize,
    /// The round passed (the seed pass fold).
    pub pass: bool,
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

/// Run one seed: create, then 1–3 rounds (each with its twin first for
/// MP/MW), collecting the record. Pure in `(seed, mutant)` (W-DET).
pub fn run_seed(seed: u64, mutant: Option<Mutant>) -> SeedRec {
    let class = class_of(seed);
    let base_knobs = Knobs::derive(seed, 0);
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
    for round in 0..round_counts {
        let Some(ch) = chain.take() else {
            break;
        };
        let knobs = Knobs::derive(seed, round);
        let cfg = knobs.store_cfg(mutant);
        let (base, _crash_desc, crash_hook) = build_round(seed, round, &knobs, &ch.model, ch.cut);
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
            let k = uniform_yield_point(seed, round, out.sched_yields.max(1));
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
                    let spec2 = CrashSpec::Hook {
                        ckpt_index: idx2,
                        hook: hook2,
                    };
                    let script2 = Script {
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
                        let tout = harness::run_round(
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
        let rr = round_rec(&knobs, &cfg, &out, &crash_desc, &crash_hook);
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
    rr.sizing_ok = obs.dirty_max >= four_f || obs.dirty_now_len >= four_f;
    rr
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
    /// The quota id (Q1…Q18, or SIZING for Work 5.3's assertion).
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
    let mut sizing_miss = 0u64;
    for rec in recs {
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
            if !r.sizing_ok {
                sizing_miss += 1;
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
            what: "rounds missing Work 5.3's ≥ 4·F dirtied LPIDs (harness defects)".into(),
            achieved: sizing_miss,
            required: 0,
        },
    ]);
    rows
}
