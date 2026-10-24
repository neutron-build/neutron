//! The oracle (card X-5 Work 2.2–2.5) — **P-written and binding**. The
//! gate compares this code with the card's Work 2 line by line. No check
//! may be weakened, skipped for a class not named there, or filtered by
//! seed.
//!
//! Bounds, per round (2.2):
//! - `lo = max(c_{r−1}, max{ts : SyncOn ∧ acked})`, with `c_0 = 0`;
//! - `hi = max{ts : written}` (defaulting to `lo` when nothing was
//!   written);
//! - in class MW only, `lo := T` of the meta that recovery opened (ruling
//!   X5-R1).
//!
//! Checks (2.4):
//! - **O-1** (plan clause 1, state): there exists `c` in
//!   `{lo} ∪ {ts ∈ H : lo < ts ≤ hi}` such that (a) the published set
//!   equals the model's at `c`; (b) for every published tree, `scan(c)`
//!   equals `model.scan(t, c)`; (c) no raw entry of any published tree has
//!   `ts > c`; (d) for every `S ∈ samp([gc_w, c])`, `scan(S)` equals
//!   `model.scan(t, S)` (X5-R4). Unpublished trees are excluded from
//!   (a)–(d) (the S-STORE §13 exception) but not from O-2.
//! - **O-1f** (spec, §14): `frontier` is one of the `c` that satisfy O-1.
//!   A separate counter, reported apart from the plan clauses.
//! - **O-2** (plan clause 2, amcheck): amcheck runs (i) on the durable
//!   image that open selected, read-only, before replay, and (ii) after
//!   recovery plus one harness checkpoint. In class C, both must be clean.
//! - **O-3** (plan clause 3, detection, every class): (a) no served
//!   corruption; (b) XX001 anywhere in the round is allowed only in class
//!   MP or MW, and counts as detection there; (c) in MP and MW, XX001 is a
//!   false positive (a failure) if every media fault in the round targeted
//!   a slot (or WAL range) that the twin run (2.5) shows was not read by
//!   recovery and not referenced by the opened image — a MISDIRECTED
//!   fault targets both its source and its landing slot (gate REWORK item
//!   6b) or landing range (gate REWORK round 2, item A), and a missing
//!   twin in an MP/MW round that has XX001 is a harness defect that fails
//!   the round, never an open pass (gate REWORK item 5); (d) in MP and MW, if `res` is `Ok` and no XX001 occurred,
//!   O-1 and O-2 must hold in full (silent damage is a failure); (e) in
//!   MP, amcheck (i)/(ii) must report XX001 for every tainted slot the
//!   opened image references, and only for those (X5-R2) — with exactly
//!   one exception, ruling X5-R5 (Work 2.7/2.8): findings may
//!   additionally name slots of the harness-computed set `R`
//!   ([`Extracted::rollback_slots`]) when the class is MP, the round's
//!   fault list has a page fault on meta slot 1 or 2 (condition 1, which
//!   the oracle checks itself, from the fault list) and
//!   [`Extracted::meta_rollback`] holds (condition 2,
//!   harness-computed). A `rollback_slots` that is not a subset of the
//!   image's untainted referenced slots fails the round with
//!   `O-3(e): harness R set malformed` (the structural guard: a harness
//!   defect must never pass). The completeness half runs whenever
//!   amcheck (i) ran and tainted referenced slots exist, whatever
//!   amcheck reported (the X5-R5 completeness fix).
//!
//! Pass (2.4): a round passes iff O-1, O-2 and O-3 hold as the class
//! requires. A seed passes iff every round passes and no `DEADLOCK` or
//! `PANIC` occurred. O-1f and the monitors are recorded separately.
//!
//! Twins (2.5): every MP and MW seed is also run as a twin — the same
//! seed, schedule and F-CRASH resolutions, with the media faults removed —
//! judged as class C, its results counted toward class C totals as extra
//! seeds (reported apart from the 10k), and supplying O-3 (c)'s
//! "read by recovery / referenced by the image" sets.
//!
//! Instrumentation-removal knobs ([`OracleCfg`]) exist **only** to
//! demonstrate that each check is load-bearing (card "Instrumentation
//! checks"): the campaign always runs with them all off. Removing O-1's
//! lower bound, O-1 (c), the taint comparison, the twin false-positive
//! rule or X5-R5's meta-slot-fault condition from the check itself lets
//! Work 2.6's must-fail cases 1, 4, 7, 9 and 13 pass; the knob demos in
//! `tests/oracle.rs` (`instrumentation_removals_are_load_bearing`) show
//! exactly that, each removal turning its red case green.

use std::collections::BTreeSet;

use crate::model::{CATALOG, Model, Scan};
use crate::rng::Rng;
use crate::sim::{FaultClass, MediaFault, MediaKind};

/// Where an XX001 was observed in the round (O-3 (b)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum XxSite {
    /// During open (meta selection, page 0, first image walk).
    Open,
    /// During WAL replay.
    Replay,
    /// During the 2.3 reads (frontier, published set, raw scans, scans).
    Read23,
    /// Amcheck (i), before replay.
    AmcheckI,
    /// Amcheck (ii), after the harness checkpoint.
    AmcheckIi,
}

/// One raw entry of a published tree, decoded by the 2.3 extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry {
    /// The user key.
    pub key: Vec<u8>,
    /// Its commit timestamp.
    pub ts: u64,
    /// Its value tag (0 live, 1 tombstone; others are XX001 at decode).
    pub tag: u8,
    /// Its value bytes (empty for a tombstone).
    pub value: Vec<u8>,
}

/// One amcheck finding, with the slot it names (non-page findings use the
/// meta slots they failed on; structural findings carry `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// The slot the finding is about, if any.
    pub slot: Option<u64>,
    /// What was found.
    pub what: String,
}

/// The extracted recovered state (Work 2.3), produced after recovery
/// returns, using the store's own read paths.
#[derive(Debug, Clone, Default)]
pub struct Extracted {
    /// Recovery returned `Ok`.
    pub ok: bool,
    /// Where XX001 occurred this round.
    pub xx001: Vec<XxSite>,
    /// A served corruption occurred (1.4: an accepted range tainted at
    /// read time).
    pub served: bool,
    /// The recovered apply frontier (§14: largest replayed ts, or `T`).
    pub frontier: u64,
    /// `gc_w` of the opened meta.
    pub gc_w: u64,
    /// `T` of the opened meta (ruling X5-R1's `lo := T`).
    pub opened_t: u64,
    /// The published set (a catalog scan at the frontier).
    pub published: BTreeSet<u64>,
    /// Amcheck (i) findings (on the image open selected, before replay).
    pub amcheck_i: Vec<Finding>,
    /// Whether amcheck (ii) ran (only after a successful recovery plus
    /// harness checkpoint).
    pub amcheck_ii_ran: bool,
    /// Amcheck (ii) findings.
    pub amcheck_ii: Vec<Finding>,
    /// The slots the opened image references at the amcheck (i) point
    /// (harness walk; O-3 (e)). X5-R5: the (ii) point has its own set,
    /// [`Extracted::image_slots_ii`].
    pub image_slots: BTreeSet<u64>,
    /// The opened image's referenced slots that are tainted at the amcheck
    /// (i) point (harness walk; O-3 (e)). X5-R5: the (ii) point has its
    /// own set, [`Extracted::tainted_image_slots_ii`].
    pub tainted_image_slots: BTreeSet<u64>,
    /// X5-R5 condition 2 (harness-computed, from SimDisk history only):
    /// the opened meta's `ckpt_id` is lower than that of some meta write
    /// covered by a completed `sync_all`, in any life of this seed — open
    /// selected a meta older than the newest synced one (a meta
    /// rollback).
    pub meta_rollback: bool,
    /// X5-R5: whether amcheck (i) ran at all. False only when no meta is
    /// valid (open fails with XX001, there is no opened image, and O-3
    /// (e) is vacuous). Gates the completeness half.
    pub amcheck_i_ran: bool,
    /// X5-R5: the exception set `R` — slots the opened image references,
    /// not tainted at amcheck (i), written after the first completed
    /// `sync_all` covering a meta newer than the opened one, with no
    /// M-NOINPLACE hit. Computed by the harness from SimDisk history
    /// only; it is honoured only when the exception's conditions hold
    /// (class MP, a page fault on meta slot 1 or 2 — the oracle checks
    /// that itself — and `meta_rollback`), and must be a subset of the
    /// image's untainted referenced slots (the structural guard).
    pub rollback_slots: BTreeSet<u64>,
    /// X5-R5: the slots the image references at the amcheck (ii) point
    /// (after recovery plus the harness checkpoint).
    pub image_slots_ii: BTreeSet<u64>,
    /// X5-R5: the image's referenced slots that are tainted at the
    /// amcheck (ii) point.
    pub tainted_image_slots_ii: BTreeSet<u64>,
}

/// Reads the recovered state for the oracle: the harness implements this
/// over the recovered store; the oracle self-tests implement it over
/// synthetic states (no store).
pub trait StateReader {
    /// `scan(t, S)` through the store's own read path. `Err` is recorded
    /// by the caller as a Read23 XX001 site before the oracle runs.
    fn scan(&mut self, tree: u64, s: u64) -> Result<Scan, ReadFail>;
    /// The raw entries of tree `t` (decoded user key, ts, tag, value).
    fn raw(&mut self, tree: u64) -> Result<Vec<RawEntry>, ReadFail>;
}

/// A read through the recovered store failed (XX001 during the 2.3 reads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadFail;

/// The twin's supply for O-3 (c) (Work 2.5): what recovery read and what
/// the opened image referenced, without media faults.
#[derive(Debug, Clone, Default)]
pub struct TwinInfo {
    /// The page-file inode id (to tell page reads from WAL reads).
    pub page_file: usize,
    /// `(file, offset, len)` of every read of the twin's recovery phase.
    pub recovery_reads: Vec<(usize, u64, u64)>,
    /// The slots the twin's opened image referenced.
    pub image_slots: BTreeSet<u64>,
}

/// Instrumentation-removal knobs (demonstrating each check is
/// load-bearing). The campaign runs with all off.
#[derive(Debug, Clone, Copy, Default)]
pub struct OracleCfg {
    /// Remove O-1's lower bound (`c ≥ lo`): allows `c = lo − 1`.
    pub no_lower_bound: bool,
    /// Remove O-1 (c) (the raw-entry ts cut).
    pub no_raw_cut: bool,
    /// Remove the taint comparison (served corruption ignored).
    pub no_taint: bool,
    /// Remove the twin false-positive rule (O-3 (c) never fires).
    pub no_twin_rule: bool,
    /// Remove X5-R5's condition 1 (the oracle's own check, from the fault
    /// list, that a page-file media fault damaged meta slot 1 or 2). The
    /// exception then honours `rollback_slots` in MP whenever
    /// `meta_rollback` holds; must turn must-fail 13 green.
    pub no_meta_slot_cond: bool,
}

/// The media faults of the round, mapped to page-file slots or WAL ranges
/// for O-3 (c), plus X5-R5's condition 1.
#[derive(Debug, Clone, Default)]
pub struct FaultTargets {
    /// MP: page-file slots (`off / 4096`), including a MISDIRECTED
    /// fault's landing slot (`to_off / 4096`, gate REWORK item 6b).
    pub page_slots: BTreeSet<u64>,
    /// MW: `(file, off, len)` WAL byte ranges, including a MISDIRECTED
    /// fault's landing range (`to_off`, its `len`) — the stray bytes are
    /// there, and a detection they cause on a range recovery read is not
    /// a false positive (gate REWORK round 2, item A; the page-file side
    /// got the same fix in round 1, item 6b).
    pub wal_ranges: Vec<(usize, u64, u64)>,
    /// X5-R5 condition 1, computed here from the raw fault list (the
    /// oracle checks it itself, never from harness-supplied state): at
    /// least one page-file media fault damaged a meta slot. That is a
    /// fault whose written range sits on slot 1 or 2 (LOST / TORN, or a
    /// MISDIRECTED's source) **or** a MISDIRECTED whose stray bytes land
    /// on slot 1 or 2 (the card's "target" of a misdirected write, 1.3).
    /// Both leave slot 1–2 with non-issued or reverted bytes, which is
    /// the only way a media fault can force open onto an older meta.
    pub meta_slot_fault: bool,
}

impl FaultTargets {
    /// Collect from the disk's applied media faults; `page_file` is the
    /// page-file inode id.
    pub fn collect(faults: &[MediaFault], page_file: usize) -> Self {
        let mut page_slots = BTreeSet::new();
        let mut wal_ranges = Vec::new();
        let mut meta_slot_fault = false;
        for f in faults {
            if f.inode == page_file {
                let slot = f.off / 4096;
                page_slots.insert(slot);
                // Gate REWORK item 6b: a MISDIRECTED fault also occupies
                // its landing slot — the stray bytes are there, and a
                // detection they cause on that referenced slot is not a
                // false positive. (The `meta_slot_fault` doc below
                // already counted the landing slot for condition 1; the
                // O-3 (c) target set now matches it.)
                if let MediaKind::Misdirected { to_off } = &f.kind {
                    page_slots.insert(*to_off / 4096);
                }
                let on_meta = slot == 1
                    || slot == 2
                    || matches!(
                        &f.kind,
                        MediaKind::Misdirected { to_off }
                            if *to_off / 4096 == 1 || *to_off / 4096 == 2
                    );
                if on_meta {
                    meta_slot_fault = true;
                }
            } else {
                wal_ranges.push((f.inode, f.off, f.len));
                // Gate REWORK round 2, item A: a WAL MISDIRECTED fault
                // also occupies its landing range — the stray bytes sit
                // there, and an XX001 they cause on a range the twin's
                // recovery read is a detection, not a false positive.
                // (Run 1 collected only the source range on the WAL
                // side; the page side landed with round 1, item 6b.)
                if let MediaKind::Misdirected { to_off } = &f.kind {
                    wal_ranges.push((f.inode, *to_off, f.len));
                }
            }
        }
        Self {
            page_slots,
            wal_ranges,
            meta_slot_fault,
        }
    }
}

/// The verdict of one round.
#[derive(Debug, Clone)]
pub struct RoundVerdict {
    /// O-1: the matched `c`, or the failure description.
    pub o1: Result<u64, String>,
    /// All `c` that satisfy O-1 (for O-1f).
    pub o1_all: Vec<u64>,
    /// O-1f: the frontier is one of the satisfying `c`.
    pub o1f_ok: bool,
    /// O-2 failure, if any.
    pub o2_fail: Option<String>,
    /// O-3 failure, if any.
    pub o3_fail: Option<String>,
    /// The XX001 sites counted as detections (MP/MW).
    pub detections: Vec<XxSite>,
    /// Whether the round passes (O-1 ∧ O-2 ∧ O-3 as the class requires).
    pub pass: bool,
    /// Acked commits lost above the matched cut (SyncOff in C; all in MW;
    /// reported, not judged — the known cost of X5-R1).
    pub acked_lost: Vec<(u64, bool)>,
}

/// Run Work 2's checks for one round.
///
/// - `model`: the model truncated at `c_{r−1}` (commits above never
///   happened as far as durability is concerned, but they carry the
///   written/acked flags the bounds need, so the harness truncates only
///   after computing `lo`/`hi`... per 2.1 the model is truncated to
///   `c_{r−1}` at the *previous* round's end, and this round's bounds are
///   computed over that; the harness passes `keep_above` — the commits
///   recorded this round — via the model itself).
/// - `reader`: the 2.3 extraction's read paths.
/// - `faults`: this round's media-fault targets (empty for class C and
///   for twins).
/// - `twin`: the twin's supply (MP/MW rounds only).
#[allow(clippy::too_many_arguments)]
pub fn check_round(
    model: &Model,
    class: FaultClass,
    c_prev: u64,
    ex: &Extracted,
    reader: &mut dyn StateReader,
    faults: &FaultTargets,
    twin: Option<&TwinInfo>,
    sample_rng: &mut Rng,
    cfg: &OracleCfg,
) -> RoundVerdict {
    // ---- bounds (2.2) ----
    let mut lo = model.lower_bound(c_prev);
    let hi = model.upper_bound(lo);
    if class == FaultClass::Mw {
        lo = ex.opened_t;
    }

    // ---- O-1: find the satisfying c's ----
    let h: BTreeSet<u64> = model.commit_ts();
    let mut candidates: Vec<u64> = Vec::new();
    if cfg.no_lower_bound {
        // Instrumentation check only: admit the model just below `lo`.
        candidates.push(lo.saturating_sub(1));
    }
    candidates.push(lo);
    for ts in h.iter() {
        if *ts > lo && *ts <= hi {
            candidates.push(*ts);
        }
    }
    candidates.sort_unstable();
    candidates.dedup();

    let mut all: Vec<u64> = Vec::new();
    let mut o1_err: Option<String> = None;
    for c in candidates.iter().copied() {
        match o1_at(model, c, ex, reader, sample_rng, cfg) {
            Ok(()) => all.push(c),
            Err(e) => {
                if o1_err.is_none() {
                    o1_err = Some(e);
                }
            }
        }
    }
    let o1 = match all.first() {
        Some(c) => Ok(*c),
        None => Err(o1_err.unwrap_or_else(|| "no candidate c".to_owned())),
    };

    // ---- O-1f ----
    let o1f_ok = all.contains(&ex.frontier);

    // ---- O-2 ----
    let mut o2_fail: Option<String> = None;
    if class == FaultClass::C {
        // In class C, both amchecks must be clean (they both run: recovery
        // succeeded if we got here with a state; if recovery failed, the
        // XX001 in class C fails O-3 below, and (i) still must be clean
        // because nothing damaged the image).
        if !ex.amcheck_i.is_empty() {
            o2_fail = Some(format!(
                "class C amcheck(i) not clean: {} findings",
                ex.amcheck_i.len()
            ));
        } else if ex.amcheck_ii_ran && !ex.amcheck_ii.is_empty() {
            o2_fail = Some(format!(
                "class C amcheck(ii) not clean: {} findings",
                ex.amcheck_ii.len()
            ));
        }
    }

    // ---- O-3 ----
    let mut o3_fail: Option<String> = None;
    let mut detections: Vec<XxSite> = Vec::new();
    if !cfg.no_taint && ex.served {
        o3_fail = Some("O-3(a): served corruption".to_owned());
    }
    if !ex.xx001.is_empty() {
        if class == FaultClass::C {
            o3_fail = Some(format!("O-3(b): XX001 in class C at {:?}", ex.xx001));
        } else {
            detections = ex.xx001.clone();
        }
    }
    if matches!(class, FaultClass::Mp | FaultClass::Mw) && !ex.xx001.is_empty() && !cfg.no_twin_rule
    {
        // O-3 (c): a false positive iff every media fault in the round
        // targeted a slot (or WAL range) that the twin shows was not read
        // by recovery and not referenced by the opened image. A missing
        // twin in an MP/MW round that has XX001 is a harness defect and
        // fails the round — the rule must never fail open (gate REWORK
        // item 5; same principle as the R structural guard: a harness
        // defect must never pass).
        match twin {
            Some(t) => {
                let mut any_fault = false;
                let mut all_uncovered = true;
                for slot in faults.page_slots.iter() {
                    any_fault = true;
                    let referenced = t.image_slots.contains(slot);
                    let read = t.recovery_reads.iter().any(|(f, off, len)| {
                        *f == t.page_file && *off < (slot + 1) * 4096 && off + len > slot * 4096
                    });
                    if referenced || read {
                        all_uncovered = false;
                    }
                }
                for (f, off, len) in faults.wal_ranges.iter() {
                    any_fault = true;
                    let read = t
                        .recovery_reads
                        .iter()
                        .any(|(rf, roff, rlen)| rf == f && *roff < off + len && roff + rlen > *off);
                    if read {
                        all_uncovered = false;
                    }
                }
                if any_fault && all_uncovered {
                    o3_fail = Some(
                        "O-3(c): XX001 with every media fault on unread, unreferenced targets"
                            .to_owned(),
                    );
                }
            }
            None => {
                o3_fail =
                    Some("O-3(c): MP/MW round with XX001 and no twin (harness defect)".to_owned());
            }
        }
    }
    if matches!(class, FaultClass::Mp | FaultClass::Mw) && ex.ok && ex.xx001.is_empty() {
        // O-3 (d): silent damage is a failure.
        if o1.is_err() {
            o3_fail = Some(format!("O-3(d): silent Ok with O-1 failing: {:?}", o1));
        } else if let Some(e) = &o2_fail {
            o3_fail = Some(format!("O-3(d): silent Ok with O-2 failing: {e}"));
        } else if ex.amcheck_ii_ran && !ex.amcheck_ii.is_empty() {
            o3_fail = Some("O-3(d): silent Ok with amcheck(ii) findings".to_owned());
        }
    }
    if class == FaultClass::Mp {
        // O-3 (e) under X5-R2 with exactly one exception, X5-R5 (Work
        // 2.7): amcheck must report every tainted slot its image
        // references, and only those, except that findings may
        // additionally name slots of the harness-computed set `R` when
        // the exception's conditions hold: class MP (checked above),
        // condition 1 (a page-file media fault damaged meta slot 1 or 2,
        // checked here from the fault list, never from harness state)
        // and condition 2 (`meta_rollback`, harness-computed). Otherwise
        // `rollback_slots` is ignored and the strict X5-R2 reading
        // applies unchanged.
        let cond1 = cfg.no_meta_slot_cond || faults.meta_slot_fault;
        let r_active = ex.meta_rollback && cond1;
        // The first (e) failure is the reported one (deterministic
        // diagnosis); it never overwrites an (a)-(d) failure.
        let mut o3e: Option<String> = None;
        // Structural guard (X5-R5): R must be a subset of the image's
        // untainted referenced slots. A harness defect must never pass.
        for slot in ex.rollback_slots.iter() {
            if !ex.image_slots.contains(slot) || ex.tainted_image_slots.contains(slot) {
                o3e.get_or_insert_with(|| format!("O-3(e): harness R set malformed (slot {slot})"));
            }
        }
        // Completeness half, the X5-R5 completeness fix: run it whenever
        // amcheck (i) ran and tainted referenced slots exist, whatever
        // amcheck reported. (Run 1 gated it on findings existing, which
        // let a fully missed detection pass beside any other XX001 of
        // the round.)
        if ex.amcheck_i_ran && !ex.tainted_image_slots.is_empty() {
            for slot in ex.tainted_image_slots.iter() {
                let reported = ex.amcheck_i.iter().any(|f| f.slot == Some(*slot));
                if !reported {
                    o3e.get_or_insert_with(|| {
                        format!("O-3(e): tainted referenced slot {slot} not reported")
                    });
                }
            }
        }
        // Exactness half (X5-R2's "and only for those", with X5-R5's R).
        for f in ex.amcheck_i.iter() {
            if let Some(slot) = f.slot {
                let tainted = ex.tainted_image_slots.contains(&slot);
                let in_r = r_active && ex.rollback_slots.contains(&slot);
                if !tainted && !in_r {
                    o3e.get_or_insert_with(|| {
                        format!(
                            "O-3(e): amcheck reported slot {slot} that is neither tainted nor in R"
                        )
                    });
                }
            }
        }
        // X5-R5: (e) applies to amcheck (ii) as well whenever it ran,
        // against the (ii) sets, with the same R.
        if ex.amcheck_ii_ran {
            for slot in ex.tainted_image_slots_ii.iter() {
                let reported = ex.amcheck_ii.iter().any(|f| f.slot == Some(*slot));
                if !reported {
                    o3e.get_or_insert_with(|| {
                        format!("O-3(e): (ii) tainted referenced slot {slot} not reported")
                    });
                }
            }
            for f in ex.amcheck_ii.iter() {
                if let Some(slot) = f.slot {
                    let tainted = ex.tainted_image_slots_ii.contains(&slot);
                    let in_r = r_active && ex.rollback_slots.contains(&slot);
                    if !tainted && !in_r {
                        o3e.get_or_insert_with(|| {
                            format!(
                                "O-3(e): (ii) reported slot {slot} that is neither tainted nor in R"
                            )
                        });
                    }
                }
            }
        }
        if o3_fail.is_none() {
            o3_fail = o3e;
        }
    }

    // ---- acked commits lost above the cut (reported, not judged) ----
    let mut acked_lost = Vec::new();
    if let Ok(c) = o1 {
        for rec in model.commits.values() {
            if rec.acked && rec.ts > c {
                acked_lost.push((rec.ts, rec.mode == crate::model::Mode::SyncOff));
            }
        }
    }

    let pass = if !ex.ok {
        // `res` is `Err`: in class C, O-3 (b) has already failed the round.
        // In MP/MW the XX001 is a detection (the plan's clause 3); the
        // recovered-state checks are vacuous — there is no recovered
        // state — so the round passes iff O-3's remaining clauses hold
        // ((a) no served corruption, (c) no false positive, (e) amcheck
        // exactness in MP).
        matches!(class, FaultClass::Mp | FaultClass::Mw) && o3_fail.is_none()
    } else {
        o1.is_ok() && o2_fail.is_none() && o3_fail.is_none()
    };
    RoundVerdict {
        o1,
        o1_all: all,
        o1f_ok,
        o2_fail,
        o3_fail,
        detections,
        pass,
        acked_lost,
    }
}

/// A seed passes iff every round passes and no `DEADLOCK` or `PANIC`
/// occurred (Work 2.4 "Pass").
pub fn seed_passes(verdicts: &[RoundVerdict], deadlock: bool, panic: bool) -> bool {
    !deadlock && !panic && verdicts.iter().all(|v| v.pass)
}

fn o1_at(
    model: &Model,
    c: u64,
    ex: &Extracted,
    reader: &mut dyn StateReader,
    sample_rng: &mut Rng,
    cfg: &OracleCfg,
) -> Result<(), String> {
    // (a) the published set equals the model's at c.
    let want_pub = model.published_at(c);
    if ex.published != want_pub {
        return Err(format!(
            "O-1(a) at c={c}: published {:?} != model {:?}",
            ex.published, want_pub
        ));
    }
    // (b)/(d) per published tree.
    for t in ex.published.iter() {
        if *t == CATALOG {
            continue;
        }
        let got = reader
            .scan(*t, c)
            .map_err(|_| "O-1(b): scan read failed".to_owned())?;
        let want = model.scan(*t, c);
        if got != want {
            return Err(format!("O-1(b) at c={c}, tree {t}: scan mismatch"));
        }
        let raw = reader
            .raw(*t)
            .map_err(|_| "O-1(c): raw read failed".to_owned())?;
        if !cfg.no_raw_cut && raw.iter().any(|e| e.ts > c) {
            return Err(format!(
                "O-1(c) at c={c}, tree {t}: raw entry above the cut (max ts {})",
                raw.iter().map(|e| e.ts).max().unwrap_or(0)
            ));
        }
        for s in model.sample_points(ex.gc_w, c, sample_rng) {
            let got = reader
                .scan(*t, s)
                .map_err(|_| "O-1(d): scan read failed".to_owned())?;
            let want = model.scan(*t, s);
            if got != want {
                return Err(format!("O-1(d) at c={c}, tree {t}, S={s}: scan mismatch"));
            }
        }
    }
    Ok(())
}
