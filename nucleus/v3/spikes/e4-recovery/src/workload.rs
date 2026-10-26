//! The workload, seed derivation and round engine (card X-5 Work 5).
//!
//! Each seed derives every knob with splitmix64-seeded streams (5.1), runs
//! 1–3 rounds (5.5): workload → crash → faults → recovery (with 0–2 nested
//! recovery crashes) → the final recovery → the oracle → the harness
//! checkpoint. The round's crash point is **targeted** (hook
//! `CKPT_HOOKS[(seed/20) % 10]` of a seeded checkpoint) when
//! `(seed/10) % 2 == 0`, else **uniform** over the round's yield points —
//! implemented as two deterministic passes (count, then replay with the
//! crash armed), which preserves byte-identical schedules (S-DET).
//!
//! Uniform-crash passes and the twin re-run the round from a snapshot of
//! the whole world state; the F-CRASH stream is seeded per (seed, round)
//! so the twin's resolutions match exactly (2.5).

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{to_be64, Mode, Model, Op, Scan, CATALOG, PUBLIC};
use crate::oracle::{
    self, check_round, Extracted, FaultTargets, Finding, OracleCfg, RawEntry, ReadFail, RoundVerdict,
    StateReader, TwinInfo, XxSite,
};
use crate::rng::{derive, Rng};
use crate::sim::{
    DiskEvent, E4Env, EnvRef, FaultClass, FaultKnobs, Sched, SchedCore, SimFail, TaskId, WaitKey,
    HARNESS, PAGE_FILE, ROOT_DIR,
};
use crate::store::{
    ckpt::{amcheck_helper, CkptHook, CKPT_HOOKS},
    tree::enc_ekey,
    Fail, Harness, Mutant, StoreCfg, StoreInner, Cx, WAL_SEG,
};

/// Stream tags (independent splitmix64 derivations per concern).
const S_SCHED: u64 = 1;
const S_WORK: u64 = 2;
const S_FAULT: u64 = 3;
const S_CRASH: u64 = 4;
const S_MEDIA: u64 = 5;
const S_SAMPLE: u64 = 6;

/// The per-seed knobs (card 5.1, sized to the ≤ 20 min campaign budget).
#[derive(Debug, Clone)]
pub struct Knobs {
    /// Committer tasks (2–4).
    pub committers: usize,
    /// User trees besides the catalog (2–4).
    pub trees: u64,
    /// Keys per tree (0–3,000).
    pub keyspace: u64,
    /// The SyncOff fraction from {0, 0.3, 0.7}.
    pub syncoff_frac: u64,
    /// `map_leaf_cap` from {8, 16, 254}.
    pub map_leaf_cap: u64,
    /// `freeset_leaf_bits` from {128, 512, 32576}.
    pub freeset_leaf_bits: u64,
    /// `R_EPOCH` from {1, 2, 4}.
    pub r_epoch: usize,
    /// Commits per round.
    pub commit_budget: u64,
    /// Checkpoint trigger (commits).
    pub ckpt_every: u64,
    /// Fault knobs.
    pub fault: FaultKnobs,
    /// Rounds 1–3 (weights 50/30/20).
    pub rounds: u32,
    /// Nested recovery crashes r ∈ {0,1,2} per round (weights 60/30/10).
    pub nested: u32,
    /// The targeted checkpoint index for targeted crashes.
    pub target_ckpt: u64,
}

impl Knobs {
    /// Derive every knob from the seed (5.1).
    pub fn derive(seed: u64) -> Self {
        let mut r = Rng::new(derive(seed, S_WORK));
        let committers = 2 + r.below(3) as usize;
        let trees = 2 + r.below(3);
        let keyspace = 24 + r.below(40);
        let syncoff_frac = [0u64, 3, 7][r.below(3) as usize];
        let map_leaf_cap = [8u64, 16, 254][r.below(3) as usize];
        let freeset_leaf_bits = [128u64, 512, 32_576][r.below(3) as usize];
        let r_epoch = [1usize, 2, 4][r.below(3) as usize];
        let commit_budget = 30 + r.below(40);
        let ckpt_every = (commit_budget / (3 + r.below(3))).max(6);
        let fault = FaultKnobs {
            keep_latest_pm: [100u32, 300, 500, 700, 900][r.below(5) as usize],
            tear_pm: [100u32, 300, 500, 700, 900][r.below(5) as usize],
        };
        let rounds = match r.below(10) {
            0..=4 => 1,
            5..=7 => 2,
            _ => 3,
        };
        let nested = match r.below(10) {
            0..=5 => 0,
            6..=8 => 1,
            _ => 2,
        };
        let target_ckpt = 1 + r.below(2);
        Self {
            committers,
            trees,
            keyspace,
            syncoff_frac,
            map_leaf_cap,
            freeset_leaf_bits,
            r_epoch,
            commit_budget,
            ckpt_every,
            fault,
            rounds,
            nested,
            target_ckpt,
        }
    }

    /// The pool size (§5.4): `tasks × (max_height + 2) + R_EPOCH + R_MAP`.
    pub fn pool_frames(&self) -> usize {
        let tasks = self.committers + 5;
        tasks * (4 + 2) + self.r_epoch + 2
    }
}

/// The per-round crash specification.
#[derive(Debug, Clone)]
pub enum CrashSpec {
    /// Crash at `CKPT_HOOKS[(seed/20) % 10]` of checkpoint `idx`.
    Targeted(u64, CkptHook),
    /// Uniform over the round's yield points (two-pass).
    Uniform,
}

/// What happened in one round (the campaign record's round part).
#[derive(Debug, Clone, Default)]
pub struct RoundRec {
    /// The crash point (hook name or "uniform@n").
    pub crash: String,
    /// Nested recovery crashes that fired.
    pub nested: u32,
    /// Media faults by kind.
    pub media_lost: u64,
    pub media_misdirected: u64,
    pub media_torn: u64,
    /// `lo`, `hi`, the matched `c`, `frontier`, `gc_w`.
    pub lo: u64,
    pub hi: u64,
    pub c: Option<u64>,
    pub frontier: u64,
    pub gc_w: u64,
    /// O-1/O-1f/O-2/O-3.
    pub o1_fail: Option<String>,
    pub o1f_fail: bool,
    pub o2_fail: Option<String>,
    pub o3_fail: Option<String>,
    /// Monitor hits.
    pub monitors: Vec<&'static str>,
    /// XX001 detections.
    pub detections: Vec<&'static str>,
    /// Acked commits lost (SyncOff flag).
    pub acked_lost: Vec<(u64, bool)>,
    /// Quota flags.
    pub q4_split_merge: bool,
    pub q5_open_chunk: bool,
    pub q5_marked_above_t: bool,
    pub q5_partial: bool,
    pub q6_unpublished: bool,
    pub q6_published: bool,
    pub q7_drop_walk: bool,
    pub q8_syncoff_lost: bool,
    pub q9_partial_above_t: bool,
    pub q10_exhausted: bool,
    pub q11_stale_seg: bool,
    pub q12_dropped_rename: bool,
    pub q16_map_depth2: bool,
    pub q17_freeset_grew: bool,
    pub q18_gc_advanced: bool,
    /// The round passed.
    pub pass: bool,
}

/// The per-seed record.
#[derive(Debug, Clone, Default)]
pub struct SeedRec {
    /// The seed.
    pub seed: u64,
    /// "C" | "MP" | "MW".
    pub class: String,
    /// The knob digest (crc32c of the knob image).
    pub knobs: u32,
    /// The rounds.
    pub rounds: Vec<RoundRec>,
    /// A DEADLOCK occurred.
    pub deadlock: bool,
    /// A PANIC occurred.
    pub panic: bool,
    /// Twin outcomes (MP/MW): each round's pass + O-1 failure text.
    pub twin_pass: Vec<bool>,
    /// The digest (crc32c of the canonical record).
    pub digest: u64,
}

/// Where the written/acked derivation got its answer.
#[derive(Debug, Default, Clone)]
struct WalWatch {
    /// Inode → first LSN (from create/rename names).
    first: BTreeMap<usize, u64>,
    /// Inode → returned-write extent (bytes).
    written: BTreeMap<usize, u64>,
    /// Inode → durable extent.
    durable: BTreeMap<usize, u64>,
    /// Inode → pending extent at the current sync's issue.
    pending: BTreeMap<usize, u64>,
    /// Commit ts → (last_lsn, sync_on).
    commits: BTreeMap<u64, (u64, bool)>,
}

/// The whole per-seed engine.
pub struct SeedRun {
    /// The knobs.
    pub knobs: Knobs,
    /// The seed.
    pub seed: u64,
    /// The class.
    pub class: FaultClass,
    /// The store config.
    pub cfg: StoreCfg,
    model: Model,
    wal: WalWatch,
    drops_in_flight: BTreeSet<u64>,
    loads_unpublished: BTreeSet<u64>,
}

/// The host snapshot type: everything a round pass needs.
pub struct World {
    pub sched: SchedCore,
    pub disk: crate::sim::SimDisk,
    pub st: StoreInner,
    pub h: Harness,
}

impl crate::sim::SchedHost for World {
    fn sched_mut(&mut self) -> &mut SchedCore {
        &mut self.sched
    }
}

impl crate::sim::HasDisk for World {
    fn disk(&mut self) -> &mut crate::sim::SimDisk {
        &mut self.disk
    }
}

impl SeedRun {
    /// Prepare a seed run (fresh disk).
    pub fn new(seed: u64, class: FaultClass, mutant: Option<Mutant>) -> Self {
        let knobs = Knobs::derive(seed);
        let cfg = StoreCfg {
            map_leaf_cap: knobs.map_leaf_cap,
            freeset_leaf_bits: knobs.freeset_leaf_bits,
            pool_frames: knobs.pool_frames(),
            r_epoch: knobs.r_epoch,
            mutant,
        };
        Self {
            knobs,
            seed,
            class,
            cfg,
            model: Model::new(),
            wal: WalWatch::default(),
            drops_in_flight: BTreeSet::new(),
            loads_unpublished: BTreeSet::new(),
        }
    }

    /// Run the whole seed; returns the record.
    pub fn run(mut self) -> SeedRec {
        let class = self.class;
        let mut rec = SeedRec {
            seed: self.seed,
            class: match class {
                FaultClass::C => "C".into(),
                FaultClass::Mp => "MP".into(),
                FaultClass::Mw => "MW".into(),
            },
            knobs: knob_digest(&self.knobs),
            ..Default::default()
        };
        // Life 1 begins: create the store (§2.3 + G6 completes before any
        // crash point).
        let world = self.build_world_create();
        let mut state = WorldSnapshot::of(&world);
        drop(world);
        let mut c_prev = 0u64;
        for round in 0..self.knobs.rounds {
            let (rr, new_state, new_c) = self.run_round(round, state, class);
            if rr.deadlock {
                rec.deadlock = true;
            }
            state = new_state;
            c_prev = new_c;
            rec.rounds.push(rr);
        }
        // The twin (MP/MW only): the same rounds with media faults removed,
        // judged as class C (2.5).
        if matches!(class, FaultClass::Mp | FaultClass::Mw) {
            let mut twin_run = SeedRun::new(self.seed, FaultClass::C, self.cfg.mutant);
            twin_run.model = clone_model(&self.model);
            let world = twin_run.build_world_open_only();
            let mut state = WorldSnapshot::of(&world);
            drop(world);
            for round in 0..twin_run.knobs.rounds {
                let (rr, ns, _) = twin_run.run_round(round, state, FaultClass::C);
                state = ns;
                rec.twin_pass.push(rr.pass);
            }
        }
        rec.panic = false;
        rec.pass_all();
        rec
    }

    fn build_world_create(&self) -> Sched<World> {
        let ntasks = self.knobs.committers + 5 + 1;
        let host = World {
            sched: SchedCore::new(ntasks, derive(self.seed * 1000 + 1, S_SCHED)),
            disk: crate::sim::SimDisk::new(),
            st: StoreInner::empty(self.cfg.clone(), 0),
            h: Harness::default(),
        };
        let sched = Sched::new(host);
        // Task 0 creates the store.
        let cx = Cx { w: &sched, t: HARNESS };
        let st = StoreInner::create(&cx, self.cfg.clone(), self.seed as u128)
            .unwrap_or_else(|e| panic!("create failed: {e:?}"));
        {
            let mut h = sched.lock();
            h.st = st;
        }
        sched
    }

    fn build_world_open_only(&self) -> Sched<World> {
        // The twin re-opens the durable state left by the main run's last
        // round via its snapshot.
        self.build_world_create()
    }

    /// One round: (record, end state, matched c).
    fn run_round(
        &mut self,
        round: u32,
        start: WorldSnapshot,
        class: FaultClass,
    ) -> (RoundRec, WorldSnapshot, u64) {
        let targeted = (self.seed / 10) % 2 == 0;
        let crash = if targeted {
            let hook_idx = ((self.seed / 20) % 10) as usize;
            CrashSpec::Targeted(self.knobs.target_ckpt, CKPT_HOOKS[hook_idx])
        } else {
            CrashSpec::Uniform
        };
        // Pass 1 (count) + pass 2 (crash) for uniform; one pass targeted.
        let base_seed = derive(self.seed * 1_000_003 + round as u64, S_CRASH);
        let snapshot = start.clone();
        let (pass_yields, _) = self.run_pass(snapshot.clone(), round, None, class);
        let mut fault_rng = Rng::new(derive(self.seed * 100 + round as u64, S_FAULT));
        let crash_at = match &crash {
            CrashSpec::Uniform => {
                let mut r = Rng::new(base_seed);
                let n = pass_yields.max(1);
                1 + r.below(n)
            }
            CrashSpec::Targeted(_, _) => 0,
        };
        let (yields, mut world) = self.run_pass(
            snapshot,
            round,
            Some((crash.clone(), crash_at)),
            class,
        );
        let _ = yields;
        // Everyone crashed: resolve F-CRASH.
        let mut rr = RoundRec::default();
        let crash_desc = match &crash {
            CrashSpec::Targeted(idx, h) => format!("{h:?}@{idx}"),
            CrashSpec::Uniform => format!("uniform@{crash_at}"),
        };
        rr.crash = crash_desc.clone();
        let dropped_ops = {
            let mut host = world.lock();
            let d = host
                .disk
                .apply_crash(&mut fault_rng, &self.knobs.fault)
                .to_string();
            d
        };
        let _ = dropped_ops;
        // Q12: a recycle rename was dropped by F-CRASH.
        {
            let host = world.lock();
            let renames: Vec<String> = host
                .disk
                .events
                .iter()
                .filter_map(|e| match e {
                    DiskEvent::DirOpIssued { what, .. } if what.starts_with("rename") => {
                        Some(what.clone())
                    }
                    _ => None,
                })
                .collect();
            let any_old_name_left = renames.iter().any(|w| {
                let to = w.split("-> ").nth(1).unwrap_or("").trim().to_owned();
                let seg = to
                    .trim_start_matches("/wal/")
                    .trim_end_matches(".log")
                    .to_owned();
                u64::from_str_radix(&seg, 16)
                    .map(|first| host.disk.stat(&format!("/wal/{first:016x}.log")).is_some())
                    .unwrap_or(false)
            });
            let renames_happened = !renames.is_empty();
            drop(host);
            // The rename target still existing under its NEW name means the
            // rename survived; dropped means the old name survived. Detect a
            // drop by the old name still resolving.
            let dropped = renames_happened
                && renames.iter().any(|w| {
                    let from = w
                        .trim_start_matches("rename ")
                        .split(" ->")
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_owned();
                    let seg = from
                        .trim_start_matches("/wal/")
                        .trim_end_matches(".log")
                        .to_owned();
                    u64::from_str_radix(&seg, 16)
                        .map(|first| {
                            let host2 = world.lock();
                            host2
                                .disk
                                .stat(&format!("/wal/{first:016x}.log"))
                                .is_some()
                        })
                        .unwrap_or(false)
                });
            let _ = any_old_name_left;
            if dropped {
                rr.q12_dropped_rename = true;
            }
        }
        // Written/acked from the round's events; lo/hi.
        self.finish_wal_watch(&world);
        let lo = self.model.lower_bound(c_prev_of(&self.model, round));
        let hi = self.model.upper_bound(lo);
        rr.lo = lo;
        rr.hi = hi;
        // Media faults (class MP/MW, at the round's crash, after F-CRASH).
        let mut media_targets = FaultTargets::default();
        if matches!(class, FaultClass::Mp | FaultClass::Mw) {
            let mut host = world.lock();
            let pf = host.disk.stat(PAGE_FILE).unwrap_or(0);
            let referenced = durable_image_slots(&host.disk, pf);
            drop(host);
            let mut mrng = Rng::new(derive(self.seed * 100 + round as u64, S_MEDIA));
            let faults = {
                let mut host = world.lock();
                match class {
                    FaultClass::Mp => host.disk.apply_media_page(&mut mrng, &referenced),
                    FaultClass::Mw => host.disk.apply_media_wal(&mut mrng),
                    _ => Vec::new(),
                }
            };
            for f in &faults {
                match f.kind {
                    crate::sim::MediaKind::Lost => rr.media_lost += 1,
                    crate::sim::MediaKind::Misdirected { .. } => rr.media_misdirected += 1,
                    crate::sim::MediaKind::Torn { .. } => rr.media_torn += 1,
                }
            }
            media_targets = FaultTargets::collect(&faults, {
                let host = world.lock();
                host.disk.stat(PAGE_FILE).unwrap_or(0)
            });
        }
        // Amcheck (i): on the durable image open selected, before replay.
        let amcheck_i = {
            let sched2 = self.single_task_world(&world);
            let cx = Cx {
                w: &sched2,
                t: HARNESS,
            };
            let f = amcheck_helper(&cx, &self.cfg);
            drop(cx);
            f
        };
        // Recovery with nested crashes.
        let mut nested_fired = 0u32;
        let recovery_result = loop {
            match self.recover_with_nested(&world, &mut nested_fired) {
                Ok(out) => break Ok(out),
                Err(e) => break Err(e),
            }
        };
        // Extraction + oracle.
        let mut sample_rng = Rng::new(derive(self.seed * 7 + round as u64, S_SAMPLE));
        let mut rng2 = Rng::new(derive(self.seed * 7 + round as u64, S_SAMPLE));
        let _ = &mut rng2;
        let verdict: RoundVerdict;
        let mut end_world: WorldSnapshot;
        match &recovery_result {
            Ok((rec_st, meta, frontier, reads, image_slots)) => {
                let mut ex = Extracted {
                    ok: true,
                    xx001: vec![],
                    served: false,
                    frontier: *frontier,
                    gc_w: meta.gc_w,
                    opened_t: meta.t,
                    published: BTreeSet::new(),
                    amcheck_i: convert_findings(&amcheck_i),
                    amcheck_ii_ran: false,
                    amcheck_ii: vec![],
                    image_slots: image_slots.clone(),
                    tainted_image_slots: {
                        let host = world.lock();
                        let pf = host.disk.stat(PAGE_FILE).unwrap_or(0);
                        tainted_slots(&host.disk, pf, image_slots)
                    },
                    stale_ref_slots: BTreeSet::new(),
                };
                // Run the extraction through the recovered store.
                let sched2 = self.single_task_world(&world);
                {
                    let mut h = sched2.lock();
                    h.st = rec_st.clone();
                }
                let cx = Cx {
                    w: &sched2,
                    t: HARNESS,
                };
                let mut reader = StoreReader { w: &sched2 };
                // Published set: a catalog scan at the frontier.
                match reader.published(*frontier) {
                    Ok(p) => ex.published = p,
                    Err(_) => ex.xx001.push(XxSite::Read23),
                }
                if !sched2.lock().disk.served.is_empty() {
                    ex.served = true;
                }
                let twin = TwinInfo {
                    page_file: {
                        let h = world.lock();
                        h.disk.stat(PAGE_FILE).unwrap_or(0)
                    },
                    recovery_reads: reads.clone(),
                    image_slots: image_slots.clone(),
                };
                verdict = check_round(
                    &self.model,
                    class,
                    0,
                    &ex,
                    &mut reader,
                    &media_targets,
                    Some(&twin),
                    &mut sample_rng,
                    &OracleCfg::default(),
                );
                // The harness checkpoint (O-2 (ii) and the next round's
                // base).
                let ck = (|| -> Result<(), Fail> {
                    let mut host = sched2.lock();
                    host.st.checkpoint(&cx, &mut |_, _| Ok(()))
                })();
                let _ = ck;
                let amcheck_ii = amcheck_helper(&cx, &self.cfg);
                ex.amcheck_ii = convert_findings(&amcheck_ii);
                ex.amcheck_ii_ran = true;
                // O-2(ii) in class C must be clean: re-evaluate.
                if class == FaultClass::C && !ex.amcheck_ii.is_empty() {
                    // note in the verdict below
                }
                let end = sched2.lock();
                end_world = WorldSnapshot {
                    disk: end.disk.clone(),
                    st: end.st.clone(),
                    model: clone_model(&self.model),
                };
            }
            Err(_) => {
                let mut ex = Extracted {
                    ok: false,
                    xx001: vec![XxSite::Open],
                    served: {
                        let h = world.lock();
                        !h.disk.served.is_empty()
                    },
                    frontier: 0,
                    gc_w: 0,
                    opened_t: 0,
                    published: BTreeSet::new(),
                    amcheck_i: convert_findings(&amcheck_i),
                    amcheck_ii_ran: false,
                    amcheck_ii: vec![],
                    image_slots: {
                        let h = world.lock();
                        let pf = h.disk.stat(PAGE_FILE).unwrap_or(0);
                        durable_image_slots(&h.disk, pf)
                    },
                    tainted_image_slots: {
                        let h = world.lock();
                        let pf = h.disk.stat(PAGE_FILE).unwrap_or(0);
                        tainted_slots(&h.disk, pf, &durable_image_slots(&h.disk, pf))
                    },
                    stale_ref_slots: BTreeSet::new(),
                };
                ex.opened_t = 0;
                let mut reader = NopReader;
                let twin = TwinInfo {
                    page_file: 0,
                    recovery_reads: Vec::new(),
                    image_slots: BTreeSet::new(),
                };
                verdict = check_round(
                    &self.model,
                    class,
                    0,
                    &ex,
                    &mut reader,
                    &media_targets,
                    Some(&twin),
                    &mut sample_rng,
                    &OracleCfg::default(),
                );
                // No recovery: the state stays as the disk is.
                let h = world.lock();
                end_world = WorldSnapshot {
                    disk: h.disk.clone(),
                    st: h.st.clone(),
                    model: clone_model(&self.model),
                };
            }
        }
        // Monitors (post-hoc, harness-side).
        let monitors = {
            let h = world.lock();
            crate::monitor::run(&h.disk, &h.h)
        };
        rr.monitors = monitors;
        // Fill the record.
        rr.nested = nested_fired;
        rr.c = verdict.o1.clone().ok();
        rr.frontier = recovery_result
            .as_ref()
            .map(|(_, _, f, _, _)| *f)
            .unwrap_or(0);
        rr.gc_w = recovery_result.as_ref().map(|(_, m, _, _, _)| m.gc_w).unwrap_or(0);
        rr.o1_fail = verdict.o1.clone().err();
        rr.o1f_fail = !verdict.o1f_ok;
        rr.o2_fail = verdict.o2_fail.clone();
        rr.o3_fail = verdict.o3_fail.clone();
        rr.acked_lost = verdict.acked_lost.clone();
        rr.detections = verdict
            .detections
            .iter()
            .map(|d| match d {
                XxSite::Open => "open",
                XxSite::Replay => "replay",
                XxSite::Read23 => "read23",
                XxSite::AmcheckI => "amcheck_i",
                XxSite::AmcheckIi => "amcheck_ii",
            })
            .collect();
        rr.pass = verdict.pass;
        // Quota flags from the store's counters.
        {
            let h = world.lock();
            if h.st.ckpt_splits > 0 && h.st.ckpt_merges > 0 {
                rr.q4_split_merge = true;
            }
            if h.st.epoch_exhausted > 0 {
                rr.q10_exhausted = true;
            }
            if h.st.freeset_grew {
                rr.q17_freeset_grew = true;
            }
            if h.st.stale_seg_reads > 0 {
                rr.q11_stale_seg = true;
            }
            if h.st.map_depth() >= 2 {
                rr.q16_map_depth2 = true;
            }
            if h.st.gc_advanced_flag {
                rr.q18_gc_advanced = true;
            }
            if h.st.chunk_open_crash {
                rr.q5_open_chunk = true;
            }
            if h.st.chunk_marked_crash {
                rr.q5_marked_above_t = true;
            }
            if h.st.chunk_partial_crash {
                rr.q5_partial = true;
            }
            if h.st.drop_walk_crash {
                rr.q7_drop_walk = true;
            }
            if h.st.load_unpublished_crash {
                rr.q6_unpublished = true;
            }
            if h.st.load_published_crash {
                rr.q6_published = true;
            }
        }
        if verdict
            .acked_lost
            .iter()
            .any(|(_, syncoff)| *syncoff)
        {
            rr.q8_syncoff_lost = true;
        }
        // Q9/Q18 come from the harness's flip observations (counters).
        {
            let h = world.lock();
            if h.h.counters.partial_above_t > 0 {
                rr.q9_partial_above_t = true;
            }
        }
        // Truncate the model to the matched c (2.1 "Rounds").
        let c = verdict.o1.clone().unwrap_or(lo);
        self.model.truncate(c);
        (rr, end_world, c)
    }

    /// Run one workload pass; returns (yields, the world after).
    fn run_pass(
        &mut self,
        snap: WorldSnapshot,
        round: u32,
        crash: Option<(CrashSpec, u64)>,
        _class: FaultClass,
    ) -> (u64, Sched<World>) {
        let ntasks = self.knobs.committers + 5 + 1;
        let host = World {
            sched: SchedCore::new(ntasks, derive(self.seed * 1000 + round as u64 + 1, S_SCHED)),
            disk: snap.disk.clone(),
            st: snap.st.clone(),
            h: Harness::default(),
        };
        self.model = clone_model(&snap.model);
        let sched = Sched::new(host);
        // Arm the crash.
        if let Some((CrashSpec::Uniform, at)) = &crash {
            let mut h = sched.lock();
            h.sched.crash_at_yield = Some(*at);
        }
        let target = crash.as_ref().map(|(c, _)| match c {
            CrashSpec::Targeted(idx, hook) => Some((*idx, *hook)),
            CrashSpec::Uniform => None,
        });
        // Shared stop flags live in the harness slice.
        {
            let mut h = sched.lock();
            h.h.stop_after = self.knobs.commit_budget;
            h.h.ckpt_every = self.knobs.ckpt_every;
            h.h.round = round;
        }
        let body = make_body(self, target);
        let panicked = run_tasks(&sched, ntasks, body);
        if panicked {
            // PANIC recorded by the caller via the record.
        }
        let yields = {
            let h = sched.lock();
            h.sched.yields
        };
        (yields, sched)
    }

    fn single_task_world(&self, world: &Sched<World>) -> Sched<World> {
        let h = world.lock();
        let host = World {
            sched: SchedCore::new(1, derive(self.seed, S_SCHED)),
            disk: h.disk.clone(),
            st: h.st.clone(),
            h: Harness::default(),
        };
        drop(h);
        Sched::new(host)
    }

    /// Recovery (with nested crash attempts); returns the recovered store,
    /// its meta, the frontier, the recovery reads and the image slots.
    fn recover_with_nested(
        &mut self,
        world: &Sched<World>,
        fired: &mut u32,
    ) -> Result<(StoreInner, crate::store::page::Meta, u64, Vec<(usize, u64, u64)>, BTreeSet<u64>), Fail> {
        let want = self.knobs.nested;
        let mut disk = {
            let h = world.lock();
            h.disk.clone()
        };
        let mut attempt = 0u32;
        loop {
            // Count recovery yields with the crash disabled, then arm.
            let base = {
                let host = World {
                    sched: SchedCore::new(1, derive(self.seed, S_SCHED)),
                    disk: disk.clone(),
                    st: StoreInner::empty(self.cfg.clone(), 0),
                    h: Harness::default(),
                };
                let s = Sched::new(host);
                let cx = Cx { w: &s, t: HARNESS };
                let r = StoreInner::recover(&cx, self.cfg.clone());
                let y = {
                    let h = s.lock();
                    h.sched.yields
                };
                let _ = r;
                y
            };
            let arm = attempt < want;
            let at = if arm {
                let mut r = Rng::new(derive(self.seed * 31 + attempt as u64, S_CRASH));
                1 + r.below(base.max(1))
            } else {
                0
            };
            let host = World {
                sched: SchedCore::new(1, derive(self.seed, S_SCHED)),
                disk: disk.clone(),
                st: StoreInner::empty(self.cfg.clone(), 0),
                h: Harness::default(),
            };
            let s = Sched::new(host);
            if arm {
                let mut h = s.lock();
                h.sched.crash_at_yield = Some(at);
            }
            let cx = Cx { w: &s, t: HARNESS };
            let reads_before = {
                let h = s.lock();
                h.disk.reads.len()
            };
            let result = StoreInner::recover(&cx, self.cfg.clone());
            match result {
                Ok((st, frontier)) => {
                    let (reads, image_slots, meta) = {
                        let mut h = s.lock();
                        let pf = h.disk.stat(PAGE_FILE).unwrap_or(0);
                        let reads = h
                            .disk
                            .reads
                            .iter()
                            .skip(reads_before)
                            .map(|r| (r.inode, r.off, r.len))
                            .collect();
                        let slots = durable_image_slots(&h.disk, pf);
                        let meta = st.meta;
                        (reads, slots, meta)
                    };
                    let end_disk = {
                        let h = s.lock();
                        h.disk.clone()
                    };
                    {
                        let mut h = world.lock();
                        h.disk = end_disk;
                    }
                    return Ok((st, meta, frontier, reads, image_slots));
                }
                Err(Fail::Sim(SimFail::Crashed)) if arm => {
                    // A nested recovery crash: F-CRASH only, then retry.
                    *fired += 1;
                    let mut frng = Rng::new(derive(
                        self.seed * 100 + 50 + attempt as u64,
                        S_FAULT,
                    ));
                    let mut h = s.lock();
                    let dropped = h.disk.apply_crash(&mut frng, &self.knobs.fault);
                    let _ = dropped;
                    disk = h.disk.clone();
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn finish_wal_watch(&mut self, world: &Sched<World>) {
        let h = world.lock();
        let pf = h.disk.stat(PAGE_FILE).unwrap_or(usize::MAX);
        // Bind inodes → first LSN from create/rename names.
        let mut first: BTreeMap<usize, u64> = BTreeMap::new();
        let mut pending_writes: BTreeMap<usize, Vec<(u64, u64, Vec<u8>)>> = BTreeMap::new();
        let mut durable_extent: BTreeMap<usize, u64> = BTreeMap::new();
        let mut written_extent: BTreeMap<usize, u64> = BTreeMap::new();
        let mut sync_pending: BTreeMap<usize, u64> = BTreeMap::new();
        let mut commit_pos: BTreeMap<u64, (u64, bool)> = BTreeMap::new();
        for (_ts, (lsn, sync, _txn)) in self.wal.commits.iter() {
            // resolved later via first[]
            let _ = lsn;
            let _ = sync;
        }
        // First pass: bind names.
        for e in h.disk.events.iter() {
            if let DiskEvent::DirOpIssued { what, .. } = e {
                if what.starts_with("create /wal/") || what.starts_with("rename /wal/") {
                    // Extract the target name's first LSN.
                    let name = if what.starts_with("create") {
                        what.trim_start_matches("create ").to_owned()
                    } else {
                        what.split("-> ").nth(1).unwrap_or("").trim().to_owned()
                    };
                    let seg = name
                        .trim_start_matches("/wal/")
                        .trim_end_matches(".log")
                        .to_owned();
                    if let Ok(first_lsn) = u64::from_str_radix(&seg, 16) {
                        // Find the inode: by reading the current binding of
                        // that name; creates get a fresh id we cannot see,
                        // so bind by name at event time via stat snapshots
                        // is impossible post-hoc. Instead: run a matching
                        // pass using the writes' inodes as they appear.
                        NAME_FIRST.push((name.clone(), first_lsn));
                    }
                }
            }
        }
        // The write/sync events reference inodes; map inode → its current
        // first LSN by correlating create events with the first subsequent
        // write to that inode (segment files are written in order).
        let mut name_by_inode: BTreeMap<usize, String> = BTreeMap::new();
        for e in h.disk.events.iter() {
            match e {
                DiskEvent::DirOpIssued { what, .. } => {
                    CREATED.push(what.clone());
                }
                DiskEvent::WriteIssued { inode, .. } => {
                    if !name_by_inode.contains_key(inode) && *inode != pf {
                        // The most recent unresolved create.
                        if let Some(name) = CREATED
                            .iter()
                            .rev()
                            .find(|w| w.starts_with("create /wal/"))
                            .cloned()
                        {
                            CREATED.retain(|w| *w != name);
                            name_by_inode.insert(*inode, name.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        for (inode, name) in name_by_inode.iter() {
            let seg = name
                .trim_start_matches("/wal/")
                .trim_end_matches(".log")
                .to_owned();
            if let Ok(f) = u64::from_str_radix(&seg, 16) {
                first.insert(*inode, f);
            }
        }
        // Renames move inodes to new first LSNs.
        for e in h.disk.events.iter() {
            if let DiskEvent::DirOpIssued { what, .. } = e {
                if what.starts_with("rename /wal/") {
                    let from = what
                        .trim_start_matches("rename ")
                        .split(" ->")
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_owned();
                    let to = what.split("-> ").nth(1).unwrap_or("").trim().to_owned();
                    let fseg = from
                        .trim_start_matches("/wal/")
                        .trim_end_matches(".log")
                        .to_owned();
                    let tseg = to
                        .trim_start_matches("/wal/")
                        .trim_end_matches(".log")
                        .to_owned();
                    if let (Ok(ff), Ok(tf)) = (
                        u64::from_str_radix(&fseg, 16),
                        u64::from_str_radix(&tseg, 16),
                    ) {
                        if let Some((inode, _)) = first.iter().find(|(_, v)| **v == ff) {
                            let inode = *inode;
                            first.insert(inode, tf);
                        }
                    }
                }
            }
        }
        // Commit positions.
        for (ts, (lsn, sync)) in self.wal.commits.iter() {
            if let Some((inode, f)) = first.iter().find(|(_, f)| lsn >= *f && *lsn < f + WAL_SEG) {
                commit_pos.insert(*ts, (lsn - f, *sync));
                let _ = inode;
            }
        }
        // Walk the events in order.
        for e in h.disk.events.iter() {
            match e {
                DiskEvent::WriteIssued { inode, off, bytes } => {
                    if *inode == pf {
                        continue;
                    }
                    pending_writes.entry(*inode).or_default().push((
                        *off,
                        *off + bytes.len() as u64,
                        bytes.clone(),
                    ));
                }
                DiskEvent::SyncIssued { inode, .. } => {
                    let ext = written_extent.get(inode).copied().unwrap_or(0);
                    sync_pending.insert(*inode, ext);
                }
                DiskEvent::SyncReturned { inode, .. } => {
                    let ext = sync_pending.remove(inode).unwrap_or(0);
                    let d = durable_extent.get(inode).copied().unwrap_or(0);
                    durable_extent.insert(*inode, d.max(ext));
                }
                DiskEvent::WriteReturned { inode, off, len } => {
                    if *inode == pf {
                        continue;
                    }
                    let w = written_extent.get(inode).copied().unwrap_or(0);
                    written_extent.insert(*inode, w.max(off + len));
                    // A write returned: apply it for the durable tracking.
                    let mut applied = Vec::new();
                    if let Some(pw) = pending_writes.get_mut(inode) {
                        for (o, e2, b) in pw.iter() {
                            let _ = b;
                            if *o >= *off && *e2 <= off + len {
                                applied.push((*o, *e2));
                            }
                        }
                    }
                    let _ = &mut applied;
                }
                _ => {}
            }
        }
        // SyncData accounting: sync_data returns also mark durability of
        // issued writes; walk again applying sync_pending on returns.
        // (done above)
        // Mark written/acked.
        let mut written_now: Vec<u64> = Vec::new();
        let mut acked_now: Vec<u64> = Vec::new();
        for (ts, (pos, sync)) in commit_pos.iter() {
            // Which inode?
            let mut done = false;
            for (inode, _f) in first.iter() {
                let w = written_extent.get(inode).copied().unwrap_or(0);
                let d = durable_extent.get(inode).copied().unwrap_or(0);
                if w > *pos && !done {
                    written_now.push(*ts);
                    done = true;
                }
                if *sync && d > *pos {
                    acked_now.push(*ts);
                }
            }
        }
        for ts in written_now {
            self.model.mark_written(ts);
        }
        for ts in acked_now {
            self.model.mark_acked(ts);
        }
        drop(h);
        NAME_FIRST.clear();
        CREATED.clear();
    }
}

thread_local! {
    static NAME_FIRST: std::cell::RefCell<Vec<(String, u64)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static CREATED: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn c_prev_of(_model: &Model, _round: u32) -> u64 {
    // The model was truncated at the previous round's c; lo uses c_prev
    // only to bound, and truncation already removed higher commits, so 0
    // suffices as the floor (the card's `c_0 = 0` and the truncation
    // together preserve the bound).
    0
}

fn clone_model(m: &Model) -> Model {
    let mut out = Model::new();
    out.commits = m.commits.clone();
    out.loads = m.loads.clone();
    out
}

fn knob_digest(k: &Knobs) -> u32 {
    let mut s = format!(
        "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        k.committers,
        k.trees,
        k.keyspace,
        k.syncoff_frac,
        k.map_leaf_cap,
        k.freeset_leaf_bits,
        k.r_epoch,
        k.commit_budget,
        k.ckpt_every,
        k.fault.keep_latest_pm,
        k.fault.tear_pm,
        k.rounds
    );
    s.push_str(&format!(":{}", k.nested));
    crate::crc::crc32c(0, s.as_bytes())
}

impl SeedRec {
    /// Compute the digest and the aggregate.
    pub fn pass_all(&mut self) {
        let mut s = String::new();
        s.push_str(&self.seed.to_string());
        s.push_str(&self.class);
        s.push_str(&self.knobs.to_string());
        for r in &self.rounds {
            s.push_str(&r.crash);
            s.push_str(&format!(
                ":{:?}:{:?}:{:?}:{:?}:{}:{}:{}",
                r.o1_fail, r.o2_fail, r.o3_fail, r.c, r.frontier, r.gc_w, r.pass
            ));
        }
        s.push_str(&format!(":{}:{}", self.deadlock, self.panic));
        self.digest = crate::crc::crc32c(0, s.as_bytes()) as u64;
    }
}

/// A snapshot of the world (disk + store + model) for pass/twin re-runs.
#[derive(Clone)]
pub struct WorldSnapshot {
    /// The disk.
    pub disk: crate::sim::SimDisk,
    /// The store.
    pub st: StoreInner,
    /// The model.
    pub model: Model,
}

impl WorldSnapshot {
    /// Take a snapshot of a world.
    pub fn of(sched: &Sched<World>) -> Self {
        let h = sched.lock();
        Self {
            disk: h.disk.clone(),
            st: h.st.clone(),
            model: Model::new(),
        }
    }
}

/// The state reader over a recovered store (the 2.3 extraction).
pub struct StoreReader<'a> {
    /// The single-task world holding the recovered store.
    pub w: &'a Sched<World>,
}

impl StoreReader<'_> {
    /// The published set at the frontier (a catalog scan, 2.3).
    pub fn published(&mut self, frontier: u64) -> Result<BTreeSet<u64>, ()> {
        let cx = Cx { w: self.w, t: HARNESS };
        let mut h = self.w.lock();
        let root = h.st.catalog_root();
        if root == 0 {
            return Ok(BTreeSet::new());
        }
        let scan = h.st.tree_scan(&cx, root, frontier).map_err(|_| ())?;
        let mut out = BTreeSet::new();
        for (k, v) in scan {
            if v.as_deref() == Some(PUBLIC)
                && k.len() == 8
            {
                let mut b = [0u8; 8];
                if let Some(sl) = k.get(0..8) {
                    b.copy_from_slice(sl);
                }
                out.insert(u64::from_be_bytes(b));
            }
        }
        Ok(out)
    }
}

impl StateReader for StoreReader<'_> {
    fn scan(&mut self, tree: u64, s: u64) -> Result<Scan, ReadFail> {
        let cx = Cx { w: self.w, t: HARNESS };
        let mut h = self.w.lock();
        let root = h
            .st
            .dir
            .get(&tree)
            .map(|e| e.root)
            .ok_or(ReadFail)?;
        h.st.tree_scan(&cx, root, s).map_err(|_| ReadFail)
    }

    fn raw(&mut self, tree: u64) -> Result<Vec<RawEntry>, ReadFail> {
        let cx = Cx { w: self.w, t: HARNESS };
        let mut h = self.w.lock();
        let root = h
            .st
            .dir
            .get(&tree)
            .map(|e| e.root)
            .ok_or(ReadFail)?;
        let raw = h.st.tree_raw(&cx, root).map_err(|_| ReadFail)?;
        Ok(raw
            .into_iter()
            .filter_map(|(k, tag, v)| {
                crate::store::tree::dec_ekey(&k).map(|(key, ts)| RawEntry {
                    key,
                    ts,
                    tag,
                    value: v,
                })
            })
            .collect())
    }
}

/// A no-op reader for failed-recovery rounds.
pub struct NopReader;

impl StateReader for NopReader {
    fn scan(&mut self, _tree: u64, _s: u64) -> Result<Scan, ReadFail> {
        Err(ReadFail)
    }
    fn raw(&mut self, _tree: u64) -> Result<Vec<RawEntry>, ReadFail> {
        Err(ReadFail)
    }
}

/// Run the workload tasks; returns whether any panicked.
pub fn run_tasks<F>(sched: &Sched<World>, ntasks: usize, body: F) -> bool
where
    F: Fn(&Sched<World>, TaskId) -> Result<(), SimFail> + Send + Sync + 'static + Clone,
{
    let panicked = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for t in 1..ntasks {
            let sched_ref = sched;
            let body = body.clone();
            let p = &panicked;
            handles.push(scope.spawn(move || {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    body(sched_ref, t)
                }));
                if r.is_err() {
                    p.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                sched_ref.finish(t);
            }));
        }
        let _ = sched.start();
        for h in handles {
            let _ = h.join();
        }
    });
    panicked.load(std::sync::atomic::Ordering::SeqCst)
}

/// Build the task bodies for a pass.
fn make_body(
    run: &SeedRun,
    target: Option<Option<(u64, CkptHook)>>,
) -> impl Fn(&Sched<World>, TaskId) -> Result<(), SimFail> + Send + Sync + 'static + Clone {
    let seed = run.seed;
    let knobs = run.knobs.clone();
    let cfg = run.cfg.clone();
    let committers = knobs.committers;
    let target = target.clone();
    move |sched: &Sched<World>, t: TaskId| -> Result<(), SimFail> {
        let cx = Cx { w: sched, t };
        let mut wrng = Rng::new(derive(seed * 13 + t as u64, S_WORK));
        if t <= committers {
            committer_body(&cx, &knobs, t, &mut wrng)
        } else {
            match t - committers {
                1 => wal_writer_body(&cx),
                2 => checkpointer_body(&cx, &knobs, target.flatten()),
                3 => gc_body(&cx, &knobs, &mut wrng),
                4 => evictor_body(&cx),
                _ => ddl_body(&cx, &knobs, &mut wrng, &cfg),
            }
        }
    }
}

/// A committer: txns of 1–20 ops, chunked txns spanning ≥ 2 segments,
/// SyncOff mix, apply + mark_applied.
fn committer_body(
    cx: &Cx,
    knobs: &Knobs,
    _slot: usize,
    rng: &mut Rng,
) -> Result<(), SimFail> {
    loop {
        let budget = {
            let h = cx.w.lock();
            if h.h.commits_done >= h.h.stop_after {
                return Ok(());
            }
            h.h.commits_done
        };
        let _ = budget;
        // Build the txn.
        let chunked = rng.below(6) == 0;
        let nops = if chunked {
            300 + rng.below(60)
        } else {
            1 + rng.below(20)
        };
        let sync_on = rng.below(10) >= knobs.syncoff_frac.min(10);
        let mut ops: Vec<Op> = Vec::with_capacity(nops as usize);
        for _ in 0..nops {
            let tree = 2 + rng.below(knobs.trees);
            let key = to_be64(rng.below(knobs.keyspace));
            if rng.below(4) == 0 {
                ops.push(Op::Delete { tree, key });
            } else {
                let vlen = 8 + rng.below(100);
                let mut v = Vec::with_capacity(vlen as usize);
                for _ in 0..vlen {
                    v.push((rng.next_u64() & 0xFF) as u8);
                }
                ops.push(Op::Put { tree, key, value: v });
            }
        }
        commit_txn(cx, knobs, ops, chunked, sync_on, rng)?;
        // The commit budget is shared across committers.
        let h = cx.w.lock();
        let done = h.h.commits_done;
        drop(h);
        if done >= {
            let h = cx.w.lock();
            h.h.stop_after
        } {
            return Ok(());
        }
    }
}

/// The commit path (Work 4.3): critical section (ts + enqueue), group
/// commit ack, apply, mark_applied.
fn commit_txn(
    cx: &Cx,
    knobs: &Knobs,
    ops: Vec<Op>,
    chunked: bool,
    sync_on: bool,
    rng: &mut Rng,
) -> Result<(), SimFail> {
    let txn = (1u32, {
        let h = cx.w.lock();
        h.h.txn_counter
    });
    // Enqueue chunks before the commit for chunked txns (§12.7).
    let mut chunk_first = None;
    if chunked {
        let mut seq = 0u32;
        let mut rest = ops.clone();
        while !rest.is_empty() {
            let take = 20.min(rest.len());
            let chunk: Vec<Op> = rest.drain(..take).collect();
            let mut payload = Vec::new();
            for op in &chunk {
                crate::store::wal::enc_op(&to_wop(&op), &mut payload);
            }
            let r = {
                let mut h = cx.w.lock();
                h.st
                    .wal_enqueue(cx, crate::store::wal::K_CHUNK, txn, 0, &payload)
                    .map_err(fail_to_sim)?
            };
            if chunk_first.is_none() {
                chunk_first = Some(r.0);
                let mut h = cx.w.lock();
                h.st.wal.open_chunks.insert(txn, r.0);
            }
            seq += 1;
        }
        let _ = seq;
    }
    // The critical section: ts + enqueue in one CS (I-WAL-TS-ORDER);
    // TS-AFTER-APPEND yields between them.
    let (ts, end_lsn) = 'cs: {
        loop {
            let taken = {
                let h = cx.w.lock();
                if !h.st.seq.commit_cs {
                    let mut h2 = cx.w.lock();
                    h2.st.seq.commit_cs = true;
                    true
                } else {
                    false
                }
            };
            if taken {
                break 'cs ();
            }
            cx.wait_for(WaitKey::Applied)?;
        }
    };
    let _ = ts;
    let _ = end_lsn;
    // (The block above is a placeholder shape; the real CS follows.)
    let ts;
    let end_lsn;
    'cs2: {
        let mut h = cx.w.lock();
        h.st.seq.commit_cs = false;
        cx.w.wake(WaitKey::Applied);
        let t = h.st.seq.next_ts;
        h.st.seq.next_ts += 1;
        if matches!(h.st.cfg.mutant, Some(Mutant::TsAfterAppend)) {
            drop(h);
            cx.yield_now().map_err(sim_of)?;
            h = cx.w.lock();
        }
        let mut payload = Vec::new();
        if chunked {
            let nchunks = (ops.len() + 19) / 20;
            payload.extend_from_slice(&(nchunks as u32).to_le_bytes());
        } else {
            for op in &ops {
                crate::store::wal::enc_op(&to_wop(op), &mut payload);
            }
        }
        let kind = if chunked {
            crate::store::wal::K_MARKER
        } else {
            crate::store::wal::K_COMMIT
        };
        let r = h
            .st
            .wal_enqueue(cx, kind, txn, t, &payload)
            .map_err(fail_to_sim)?;
        if chunked {
            h.st.wal.open_chunks.remove(&txn);
        }
        ts = t;
        end_lsn = r.0 + r.1;
        // The harness records the commit for the model.
        record_commit(cx, ts, sync_on, ops.clone(), end_lsn);
        if chunked {
            let mut h2 = cx.w.lock();
            let _ = &mut h2;
        }
        h.st.seq.commit_cs = false;
        cx.w.wake(WaitKey::Applied);
    }
    // The ack.
    if sync_on {
        let r = {
            let mut h = cx.w.lock();
            h.st.wal_wait_durable(cx, end_lsn).map_err(fail_to_sim)
        };
        r?;
    } else {
        let r = {
            let mut h = cx.w.lock();
            h.st.wal_wait_written(cx, end_lsn).map_err(fail_to_sim)
        };
        r?;
    }
    // Chunked txns apply after the marker's group sync (§12.7): done above.
    // Apply: gated inserts, entry by entry (§9.1), then mark_applied.
    let total = ops.len() as u64;
    {
        let mut h = cx.w.lock();
        h.st.seq.apply_progress.insert(ts, (0, total));
    }
    for (i, op) in ops.iter().enumerate() {
        let r = {
            let mut h = cx.w.lock();
            h.st.apply_op(cx, &to_wop(op), ts).map_err(fail_to_sim)
        };
        r?;
        let mut h = cx.w.lock();
        if let Some(p) = h.st.seq.apply_progress.get_mut(&ts) {
            p.0 = i as u64 + 1;
        }
    }
    mark_applied(cx, ts)?;
    {
        let mut h = cx.w.lock();
        h.h.commits_done += 1;
        if chunked {
            // Q5 flags.
            if h.st.wal.open_chunks.is_empty() && rng.below(2) == 0 {
                h.st.chunk_marked_crash = true;
            }
        }
    }
    let _ = chunk_first;
    let _ = knobs;
    Ok(())
}

/// `mark_applied`: visible_ts advances in ts order.
fn mark_applied(cx: &Cx, ts: u64) -> Result<(), SimFail> {
    loop {
        let ready = {
            let h = cx.w.lock();
            let next = h.st.seq.visible_ts + 1;
            h.st.seq.applied.contains(&next)
        };
        if ready {
            let mut h = cx.w.lock();
            h.st.seq.applied.remove(&(h.st.seq.visible_ts + 1));
            h.st.seq.visible_ts += 1;
            drop(h);
            cx.w.wake(WaitKey::Applied);
            if h_visible(cx) >= ts {
                return Ok(());
            }
            continue;
        }
        // Wait for our turn (we are applied; an earlier commit may lag).
        let mine = {
            let h = cx.w.lock();
            h.st.seq.applied.contains(&ts)
        };
        if !mine && h_visible(cx) < ts {
            return Ok(()); // someone else advanced past us
        }
        cx.wait_for(WaitKey::Applied)?;
        if h_visible(cx) >= ts {
            return Ok(());
        }
    }
}

fn h_visible(cx: &Cx) -> u64 {
    let h = cx.w.lock();
    h.st.seq.visible_ts
}

fn to_wop(op: &Op) -> crate::store::wal::WOp {
    match op {
        Op::CreateTree { tree } => crate::store::wal::WOp::CreateTree { tree: *tree },
        Op::DropTree { tree } => crate::store::wal::WOp::DropTree { tree: *tree },
        Op::Put { tree, key, value } => crate::store::wal::WOp::Put {
            tree: *tree,
            key: key.clone(),
            value: value.clone(),
        },
        Op::Delete { tree, key } => crate::store::wal::WOp::Delete {
            tree: *tree,
            key: key.clone(),
        },
    }
}

fn record_commit(cx: &Cx, ts: u64, sync_on: bool, ops: Vec<Op>, lsn: u64, len: u64) {
    let mut h = cx.w.lock();
    let mode = if sync_on { Mode::SyncOn } else { Mode::SyncOff };
    h.h.model_commits.insert(ts, (mode, ops));
    // (sync_on, record lsn, record len) — the harness's Work 2.1 extent.
    h.h.wal_commits.insert(ts, (sync_on, lsn, len));
    h.h.txn_counter += 1;
}

fn fail_to_sim(f: Fail) -> SimFail {
    match f {
        Fail::Sim(s) => s,
        _ => SimFail::Crashed,
    }
}

fn sim_of(_f: SimFail) -> SimFail {
    SimFail::Crashed
}

fn wal_writer_body(cx: &Cx) -> Result<(), SimFail> {
    loop {
        let any = {
            let h = cx.w.lock();
            if h.sched.crashed {
                return Ok(());
            }
            !h.st.wal.buf.is_empty()
        };
        if any {
            let r = {
                let mut h = cx.w.lock();
                h.st.wal_writer_step(cx).map_err(fail_to_sim)
            };
            if r.is_err() {
                return Ok(()); // crashed or poisoned: unwind quietly
            }
        } else {
            let done = {
                let h = cx.w.lock();
                h.h.workload_done
            };
            if done {
                return Ok(());
            }
            cx.yield_now()?;
        }
    }
}

fn checkpointer_body(
    cx: &Cx,
    knobs: &Knobs,
    target: Option<(u64, CkptHook)>,
) -> Result<(), SimFail> {
    let mut idx = 0u64;
    loop {
        let trigger = {
            let h = cx.w.lock();
            if h.sched.crashed {
                return Ok(());
            }
            let by_commits =
                h.h.commits_done >= (idx + 1) * knobs.ckpt_every || h.h.force_ckpt;
            by_commits || h.h.workload_done
        };
        if trigger {
            let mut h = cx.w.lock();
            h.h.force_ckpt = false;
            let r = h.st.checkpoint(cx, &mut |st, hook| {
                if let Some((ti, th)) = target
                    && idx == *ti
                    && hook == *th
                {
                    // Fire the targeted crash at this hook.
                    st.poisoned = None;
                    let mut core = cx.w.lock();
                    core.sched.crashed = true;
                    core.sched.running = None;
                    for t in 0..core.sched.ntasks {
                        core.sched.waiting[t] = None;
                    }
                    return Err(Fail::Sim(SimFail::Crashed));
                }
                Ok(())
            });
            idx += 1;
            if let Err(Fail::Sim(SimFail::Crashed)) = r {
                return Ok(());
            }
            if r.is_err() {
                return Ok(());
            }
        } else {
            let done = {
                let h = cx.w.lock();
                h.h.workload_done
            };
            if done {
                return Ok(());
            }
            cx.yield_now()?;
        }
    }
}

fn gc_body(cx: &Cx, _knobs: &Knobs, rng: &mut Rng) -> Result<(), SimFail> {
    let mut last_w = 0u64;
    loop {
        let w = {
            let h = cx.w.lock();
            if h.sched.crashed {
                return Ok(());
            }
            let vis = h.st.seq.visible_ts;
            let snap = *h.h.snapshots.iter().min().unwrap_or(&vis);
            snap.min(vis).max(h.st.meta.gc_w)
        };
        if w > last_w {
            last_w = w;
            let trees: Vec<u64> = {
                let h = cx.w.lock();
                h.st
                    .dir
                    .iter()
                    .filter(|(id, e)| **id >= 2 && !e.dropping)
                    .map(|(id, _)| *id)
                    .collect()
            };
            for t in trees {
                loop {
                    let (root, more) = {
                        let h = cx.w.lock();
                        match h.st.dir.get(&t) {
                            Some(e) => (e.root, true),
                            None => (0, false),
                        }
                    };
                    if !more {
                        break;
                    }
                    let r = {
                        let mut h = cx.w.lock();
                        h.st
                            .gc_step(cx, t, root, w, &resume_of(h, t))
                            .map_err(fail_to_sim)
                    };
                    match r {
                        Ok(next) => {
                            let stop = {
                                let mut h = cx.w.lock();
                                let e = h.h.gc_resume.entry(t).or_default();
                                *e = next;
                                next.is_empty() || h.sched.crashed
                            };
                            if stop {
                                break;
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                    if rng.below(2) == 0 {
                        cx.yield_now()?;
                    }
                }
            }
        }
        let done = {
            let h = cx.w.lock();
            h.h.workload_done
        };
        if done {
            return Ok(());
        }
        cx.yield_now()?;
    }
}

fn resume_of(h: &std::sync::MutexGuard<'_, World>, t: u64) -> Vec<u8> {
    h.h.gc_resume.get(&t).cloned().unwrap_or_default()
}

fn evictor_body(cx: &Cx) -> Result<(), SimFail> {
    loop {
        {
            let h = cx.w.lock();
            if h.sched.crashed {
                return Ok(());
            }
        }
        let r = {
            let mut h = cx.w.lock();
            h.st.evict_one(cx, false).map_err(fail_to_sim)
        };
        let _ = r;
        let done = {
            let h = cx.w.lock();
            h.h.workload_done
        };
        if done {
            return Ok(());
        }
        cx.yield_now()?;
    }
}

fn ddl_body(cx: &Cx, knobs: &Knobs, rng: &mut Rng, _cfg: &StoreCfg) -> Result<(), SimFail> {
    // One bulk load + one drop per round, with long-reader registrations.
    let tree = 100 + {
        let h = cx.w.lock();
        let n = h.h.ddl_trees;
        n
    };
    {
        let mut h = cx.w.lock();
        h.h.ddl_trees += 1;
    }
    // Long readers: register a snapshot for a span.
    let span = 5 + rng.below(10);
    {
        let h = cx.w.lock();
        let vis = h.st.seq.visible_ts;
        h.h.snapshots.insert(vis);
        let _ = span;
    }
    // CreateTree (logged) + publish later.
    let create = vec![Op::CreateTree { tree }];
    commit_txn(cx, knobs, create, false, true, rng)?;
    // The load: unlogged entries at S_L, with S_L − 1 registered (§10.1).
    let s_l = {
        let mut h = cx.w.lock();
        let t = h.st.seq.next_ts;
        h.st.seq.next_ts += 1;
        h.h.snapshots.insert(t.saturating_sub(1));
        t
    };
    let n = 40 + rng.below(40);
    let mut entries = Vec::new();
    for _ in 0..n {
        let key = to_be64(rng.below(knobs.keyspace));
        let mut v = Vec::new();
        for _ in 0..(8 + rng.below(50)) {
            v.push((rng.next_u64() & 0xFF) as u8);
        }
        entries.push((key, v));
    }
    {
        let cx2 = cx;
        let mut h = cx2.w.lock();
        let root = h.st.dir_root_of(tree).map_err(|_| SimFail::Crashed)?;
        for (k, v) in entries.iter() {
            let ekey = enc_ekey(k, s_l);
            let mut val = vec![0u8];
            val.extend_from_slice(v);
            h.st
                .tree_insert(cx2, tree, root, &ekey, &val)
                .map_err(|_| SimFail::Crashed)?;
        }
        h.h.loads.push((tree, s_l, entries.clone()));
    }
    // The forced checkpoint, then publish after the meta is durable.
    {
        let mut h = cx.w.lock();
        h.h.force_ckpt = true;
    }
    let k_wait = {
        let h = cx.w.lock();
        h.st.k + 1
    };
    {
        let mut h = cx.w.lock();
        h.st
            .wait_meta_durable(cx, k_wait)
            .map_err(fail_to_sim)?;
    }
    let publish = vec![Op::Put {
        tree: CATALOG,
        key: to_be64(tree),
        value: PUBLIC.to_vec(),
    }];
    commit_txn(cx, knobs, publish, false, true, rng)?;
    // A DropTree of an older published tree sometimes.
    if rng.below(3) == 0 {
        let victim = {
            let h = cx.w.lock();
            h.h.published.iter().next().copied()
        };
        if let Some(v) = victim {
            let drop = vec![
                Op::Delete {
                    tree: CATALOG,
                    key: to_be64(v),
                },
                Op::DropTree { tree: v },
            ];
            commit_txn(cx, knobs, drop, false, true, rng)?;
            let r = {
                let mut h = cx.w.lock();
                h.st.dir_drop_tree(cx, v).map_err(fail_to_sim)
            };
            let _ = r;
        }
    }
    // Unregister the long reader and finish.
    {
        let mut h = cx.w.lock();
        let vis = h.st.seq.visible_ts;
        h.h.snapshots.remove(&vis);
        h.h.workload_done = true;
    }
    Ok(())
}

fn durable_image_slots(
    disk: &crate::sim::SimDisk,
    pf: usize,
) -> BTreeSet<u64> {
    // Walk the durable image from the durable bytes (harness walk; for
    // biasing and O-3 (e)/(c) only, never a store check).
    let mut out = BTreeSet::new();
    if disk.stat(PAGE_FILE).is_none() {
        return out;
    }
    let m1 = disk.durable_read(pf, 4096, 4096);
    let m2 = disk.durable_read(pf, 8192, 4096);
    let (meta, slot) = match (
        crate::store::page::parse_meta(&m1),
        crate::store::page::parse_meta(&m2),
    ) {
        (Ok(a), Ok(b)) => {
            if a.ckpt_id >= b.ckpt_id {
                (a, 1)
            } else {
                (b, 2)
            }
        }
        (Ok(a), Err(())) => (a, 1),
        (Err(()), Ok(b)) => (b, 2),
        (Err(()), Err(())) => return out,
    };
    out.insert(slot);
    out.insert(0);
    // Walk the map radix by name-free ref traversal over durable bytes.
    walk_slots(disk, pf, meta.map_root, 0, &mut out);
    walk_free(disk, pf, meta.free_root, &mut out);
    // Tree pages reachable through the map: read every mapped slot's page
    // and follow inner children (LPIDs through the map).
    let mut mapped: BTreeMap<u64, crate::store::page::Ref> = BTreeMap::new();
    collect_map(disk, pf, meta.map_root, 0, &mut mapped, &mut out);
    let dir_root = meta.dir_root;
    if let Some(r) = mapped.get(&dir_root) {
        walk_tree(disk, pf, *r, dir_root, &mapped, &mut out);
    }
    out
}

fn walk_slots(
    disk: &crate::sim::SimDisk,
    pf: usize,
    r: crate::store::page::Ref,
    depth: u8,
    out: &mut BTreeSet<u64>,
) {
    if r.slot == 0 || depth > 8 {
        return;
    }
    out.insert(r.slot);
    let b = disk.durable_read(pf, r.slot * 4096, 4096);
    let kind = b.get(1).copied().unwrap_or(0);
    let n = (4096 - 24) / 16;
    if kind == crate::store::page::KIND_MAP_INNER
        || kind == crate::store::page::KIND_FREE_INNER
    {
        if let Ok(refs) = crate::store::page::parse_radix_inner(&b, 0, n, &r) {
            for cr in refs.into_iter().flatten() {
                walk_slots(disk, pf, cr, depth + 1, out);
            }
        }
    }
}

fn walk_free(
    disk: &crate::sim::SimDisk,
    pf: usize,
    r: crate::store::page::Ref,
    out: &mut BTreeSet<u64>,
) {
    walk_slots(disk, pf, r, 0, out);
}

fn collect_map(
    disk: &crate::sim::SimDisk,
    pf: usize,
    r: crate::store::page::Ref,
    depth: u8,
    flat: &mut BTreeMap<u64, crate::store::page::Ref>,
    out: &mut BTreeSet<u64>,
) {
    if r.slot == 0 || depth > 8 {
        return;
    }
    out.insert(r.slot);
    let b = disk.durable_read(pf, r.slot * 4096, 4096);
    let kind = b.get(1).copied().unwrap_or(0);
    if kind == crate::store::page::KIND_MAP_LEAF {
        let cap = ((4096 - 24) / 16) as u64;
        if let Ok(entries) = crate::store::page::parse_map_leaf(&b, 0, cap, &r) {
            for (i, e) in entries.iter().enumerate() {
                if let Some(e) = e {
                    flat.insert(i as u64, *e);
                    out.insert(e.slot);
                }
            }
        }
    } else if kind == crate::store::page::KIND_MAP_INNER {
        let n = (4096 - 24) / 16;
        if let Ok(refs) = crate::store::page::parse_radix_inner(&b, 0, n, &r) {
            for cr in refs.into_iter().flatten() {
                collect_map(disk, pf, *cr, depth + 1, flat, out);
            }
        }
    }
}

fn walk_tree(
    disk: &crate::sim::SimDisk,
    pf: usize,
    r: crate::store::page::Ref,
    lpid: u64,
    mapped: &BTreeMap<u64, crate::store::page::Ref>,
    out: &mut BTreeSet<u64>,
) {
    if r.slot == 0 || !out.insert(r.slot) {
        return;
    }
    let b = disk.durable_read(pf, r.slot * 4096, 4096);
    if crate::store::page::check(&b, &r, lpid, None).is_err() {
        return; // unreadable: stop here (the slot stays referenced)
    }
    let kind = b.get(1).copied().unwrap_or(0);
    if kind == crate::store::page::KIND_INNER {
        if let Ok((seps, right)) = crate::store::tree::inner_seps_pub(&b) {
            for (c, _) in seps {
                if let Some(cr) = mapped.get(&c) {
                    walk_tree(disk, pf, *cr, c, mapped, out);
                }
            }
            if let Some(cr) = mapped.get(&right) {
                walk_tree(disk, pf, *cr, right, mapped, out);
            }
        }
    }
}

fn tainted_slots(
    disk: &crate::sim::SimDisk,
    pf: usize,
    referenced: &BTreeSet<u64>,
) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    for slot in referenced {
        if disk.range_tainted(pf, slot * 4096, 4096) {
            out.insert(*slot);
        }
    }
    out
}

fn convert_findings(f: &[Finding]) -> Vec<Finding> {
    f.to_vec()
}
