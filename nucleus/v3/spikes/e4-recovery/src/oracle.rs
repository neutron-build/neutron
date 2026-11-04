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
//! X5-R7 (P ruling 2026-10-31, nv3-X-5c-esc-1): O-1 (b) and (d) compare
//! the §8.3 projection — live values only, a tombstone reads as absent —
//! so a tombstone §10.2 legally elided compares equal to the model's
//! held tombstone ([`live`]).
//!
//! X5-R36 (P ruling 2026-11-04, `verdicts/X-5c-F3-F3a-F2.md`; the README's
//! "Carried: X5-R36" block is the verbatim text): the model side of O-1
//! (b) at candidate `c` is `model.scan_at(t, c, c)` and of O-1 (d) at
//! sample `S` is `model.scan_at(t, S, c)` — the drop incarnation the cut
//! `c` defines. It removes only the expectation of pre-drop content in a
//! re-created tree (unservable per S-STORE §6.8); a store that serves
//! pre-drop content in a re-created tree now fails O-1 (b)/(d).
//!
//! X5-R6 (same ruling, fixed before any campaign data was judged): in
//! class MP with `res` `Ok`, an O-1 comparison whose read through the
//! store fails with XX001 is a **detected loss**, not a mismatch; every
//! comparison whose read returns must still be exact at the one matched
//! `c`. A round with a detected loss counts a Read23 detection (so O-3
//! (c) applies) and must have a damaged referenced slot to explain it
//! (tainted at (i), or in an active `R`). In MW and C a failed O-1 read
//! fails O-1, as before. If the published-set read failed
//! ([`Extracted::published_read_failed`]), (a) is a detected loss in MP
//! and (b)–(d) run over the model's published set at `c`.
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
    /// X5-R6: the published-set read (2.3) failed with XX001; the
    /// harness also records it as a Read23 site.
    pub published_read_failed: bool,
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
    /// X5-R28: the kept set `K` at the amcheck (i) point — slots the
    /// newer meta freed and the store reissued after a meta rollback,
    /// whose reissue **did not land** (durable bytes = the opened
    /// image's own content, `V_k(s)`), so they are tainted by 1.4 yet
    /// §3.3 passes and no correct amcheck can report them. Harness-
    /// computed from SimDisk history only; the completeness half skips
    /// them and the exactness half fails findings that name them,
    /// each only while `r_active` holds, and the structural guard
    /// holds them to `tainted ∩ referenced`, out of `R`, outside
    /// slots 0–2, whatever `r_active` is.
    pub kept_slots: BTreeSet<u64>,
    /// X5-R5: the slots the image references at the amcheck (ii) point
    /// (after recovery plus the harness checkpoint).
    pub image_slots_ii: BTreeSet<u64>,
    /// X5-R5: the image's referenced slots that are tainted at the
    /// amcheck (ii) point.
    pub tainted_image_slots_ii: BTreeSet<u64>,
    /// X5-R28: the kept set at the amcheck (ii) point, computed the
    /// same way as [`Extracted::kept_slots`] against the end image's
    /// own meta and the (ii) taint.
    pub kept_slots_ii: BTreeSet<u64>,
    /// **X5-R32 (a) 2:** the real round's own recovery reads — exactly
    /// the harness's `recovery_reads`: the reads of the recovery life
    /// whose result the round judges, on the `Ok` arm and the final
    /// `Err` arm alike. It is the real round's, **never the twin's**;
    /// `(file, offset, len)`.
    pub recovery_reads: Vec<(usize, u64, u64)>,
    /// **X5-R32 (a) 2:** the page-file inode id (which of
    /// [`Extracted::recovery_reads`] are page reads).
    pub page_file: usize,
    /// **X5-R33 (c):** `h` — the largest ts whose Commit/CommitMarker
    /// record lies wholly below the opened meta's `wal_ckpt_end`, in
    /// the stream as it stood when that meta was written. Harness-
    /// computed from SimDisk history and the meta bytes; an input to
    /// the MW lower bound.
    pub mw_h: u64,
    /// **X5-R33 (c):** `a` — the largest X5-R34-acked SyncOn ts whose
    /// record ends at or below the round's first damaged WAL byte (the
    /// round's largest acked SyncOn ts with no damaged WAL byte).
    pub mw_a: u64,
    /// **X5-R33 (e):** the recovery `Err` is `Fail::WalLoss`
    /// (I-WAL-COVER's refusal; the same typed predicate as the record's
    /// `cover_err`).
    pub wal_loss: bool,
    /// **X5-R33 (e):** the WAL-loss `Err` is explained — a fault of the
    /// round damaged a WAL byte in `[wal_start_lsn, wal_ckpt_end)` of
    /// the opened meta.
    pub wal_loss_explained: bool,
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
    /// Remove X5-R32's damaged-referenced arm (the O-3 (c) exception for
    /// a round whose XX001 sites are all explained by damaged,
    /// referenced, recovery-read slots from the fault history). Exists
    /// only for the load-bearing demonstration (`no_r32` turns the
    /// must-pass case mp8 red).
    pub no_r32: bool,
    /// **X5-R33:** remove the X5-R33 arm — in class MW the lower bound
    /// returns to X5-R1's `lo := T`, and a WAL-loss `Err` is judged by
    /// the twin rule again. Exists only for the load-bearing
    /// demonstration (`no_r33` turns mf30, mf31 and mf32 green).
    pub no_r33: bool,
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
    /// **X5-R32 (a) 1:** the page-file slots of every media fault
    /// applied to this round's disk **before this round's own draw**
    /// (`disk.media[..media_base]` in `harness::run_round`) — the
    /// SimDisk's own applied-fault list, which the real chain's disk
    /// carries from round to round. Each prior fault maps by
    /// [`FaultTargets::page_slots`]'s rule (`off / 4096`, plus
    /// `to_off / 4096` for a MISDIRECTED), and only for faults whose
    /// inode is the page file. It is empty in round 0, in classes C and
    /// MW (no page faults), and in every twin round (which applies no
    /// media faults).
    pub prior_page_slots: BTreeSet<u64>,
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
            prior_page_slots: BTreeSet::new(),
        }
    }

    /// **X5-R32 (a) 1:** fold the prior rounds' applied page faults into
    /// the target set — `prior` is `disk.media[..media_base]` in
    /// `harness::run_round` (this round's disk's applied-fault list
    /// before this round's own draw), mapped by `page_slots`'s rule.
    /// Called where the round's own targets are collected.
    pub fn with_prior(mut self, prior: &[MediaFault], page_file: usize) -> Self {
        for f in prior {
            if f.inode != page_file {
                continue;
            }
            self.prior_page_slots.insert(f.off / 4096);
            if let MediaKind::Misdirected { to_off } = &f.kind {
                self.prior_page_slots.insert(*to_off / 4096);
            }
        }
        self
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
    /// X5-R6: O-1 comparisons at the matched `c` satisfied by a detected
    /// loss (class MP only; reported beside the rule in RESULT item 7).
    pub o1_detected_loss: u32,
    /// **X5-R32 (b):** the damaged-referenced arm fired — the O-3 (c)
    /// twin rule would have failed the round, but every XX001 site is
    /// explained by a damaged (`H`), referenced, non-kept slot the real
    /// recovery read (or, for AmcheckI/Read23, by `D ≠ ∅`; for
    /// AmcheckIi, by `D_ii ≠ ∅`). A diagnostic: the pass fold does not
    /// read it. `false` in every other case.
    pub r32: bool,
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
        // **X5-R33 (c):** in class MW the lower bound is
        // `lo := max(c_{r−1}, T, h, a)`; with `no_r33` it is X5-R1's
        // `lo := T`, as before. `hi` is unchanged.
        lo = if cfg.no_r33 {
            ex.opened_t
        } else {
            c_prev.max(ex.opened_t).max(ex.mw_h).max(ex.mw_a)
        };
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

    // X5-R6: a failed O-1 read is a detected loss only in class MP with
    // `res` Ok; in C and MW it fails O-1 exactly as before.
    let detect_ok = class == FaultClass::Mp && ex.ok;
    let mut all: Vec<u64> = Vec::new();
    let mut lost_at_match: u32 = 0;
    let mut o1_err: Option<String> = None;
    for c in candidates.iter().copied() {
        match o1_at(model, c, ex, reader, sample_rng, cfg, detect_ok) {
            Ok(lost) => {
                if all.is_empty() {
                    lost_at_match = lost;
                }
                all.push(c);
            }
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
    // X5-R6: the oracle's own XX001s are detections like any other.
    let xx_any = !ex.xx001.is_empty() || lost_at_match > 0;
    let r_active_r6 = class == FaultClass::Mp
        && ex.meta_rollback
        && (cfg.no_meta_slot_cond || faults.meta_slot_fault);

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
    // X5-R32 (b): set by the damaged-referenced arm below; a diagnostic
    // the pass fold never reads.
    let mut r32 = false;
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
    if lost_at_match > 0 {
        // X5-R6: reachable only in class MP (detect_ok).
        if !detections.contains(&XxSite::Read23) {
            detections.push(XxSite::Read23);
        }
        // X5-R28 (b) (a tightening): a tainted slot in `K` does not
        // explain an O-1 read's XX001 — the rule is "some tainted
        // referenced slot outside K, or an active non-empty R".
        let explained = ex
            .tainted_image_slots
            .iter()
            .any(|s| !ex.kept_slots.contains(s))
            || (r_active_r6 && !ex.rollback_slots.is_empty());
        if !explained && o3_fail.is_none() {
            o3_fail = Some(
                "O-3(c): XX001 on an O-1 read with no damaged referenced slot (X5-R6)".to_owned(),
            );
        }
    }
    if matches!(class, FaultClass::Mp | FaultClass::Mw) && xx_any && !cfg.no_twin_rule {
        // O-3 (c): a false positive iff every media fault in the round
        // targeted a slot (or WAL range) that the twin shows was not read
        // by recovery and not referenced by the opened image. A missing
        // twin in an MP/MW round that has XX001 is a harness defect and
        // fails the round — the rule must never fail open (gate REWORK
        // item 5; same principle as the R structural guard: a harness
        // defect must never pass).
        // **X5-R33 (e):** in class MW with `!ex.ok && ex.wal_loss` (and
        // the arm on), the twin rule does not apply: the round fails
        // O-3 (c) iff the refusal is unexplained. Every other MW `Err`
        // is judged as before, by the twin rule.
        let r33_cover =
            class == FaultClass::Mw && !ex.ok && ex.wal_loss && !cfg.no_r33 && !cfg.no_twin_rule;
        if r33_cover && !ex.wal_loss_explained {
            o3_fail = Some(
                "O-3(c): WAL-loss XX001 with no fault in [wal_start_lsn, wal_ckpt_end) (X5-R33)"
                    .to_owned(),
            );
        }
        match twin {
            Some(_) if r33_cover => {}
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
                    // **X5-R32 (b): the damaged-referenced arm.** The
                    // twin's sets are a failed stand-in for the real
                    // round's when the damage is inherited: a slot the
                    // disk's fault history damaged in a prior round,
                    // the opened image references, and the real
                    // recovery read. The definitions (computed from
                    // SimDisk-derived inputs only — the fault history,
                    // the harness walk, and the real recovery's reads;
                    // never store memory, §3.3 or amcheck output):
                    // - `H = faults.page_slots ∪ faults.prior_page_slots`;
                    // - `D = { s ∈ ex.tainted_image_slots : s ∉ {0,1,2},
                    //     s ∈ H, s ∉ ex.kept_slots }`;
                    // - `D_ii` the same against the (ii) sets (empty
                    //     unless `ex.amcheck_ii_ran`);
                    // - `D_read` iff some `s ∈ D` has a read in the real
                    //     recovery's reads on the page file overlapping
                    //     `[s·4096, (s+1)·4096)`;
                    // - the sites are `ex.xx001`, plus `Read23` when
                    //     `lost_at_match > 0`.
                    // A site is explained when: `Open`/`Replay` by
                    // `D_read`; `AmcheckI`/`Read23` by `D ≠ ∅`;
                    // `AmcheckIi` by `D_ii ≠ ∅`. In class MP with the
                    // arm armed (`!cfg.no_r32`), a non-empty site list
                    // all of whose sites are explained does **not**
                    // fail O-3 (c) (`r32 = true`, a diagnostic the pass
                    // fold never reads). Everything else — the `None`
                    // branch, the twin's own sets and per-fault
                    // referenced/read test, the WAL ranges, class MW,
                    // X5-R6's `explained` rule, every other clause — is
                    // unchanged.
                    let r32_h: BTreeSet<u64> = faults
                        .page_slots
                        .union(&faults.prior_page_slots)
                        .copied()
                        .collect();
                    let r32_d: BTreeSet<u64> = ex
                        .tainted_image_slots
                        .iter()
                        .filter(|s| {
                            !matches!(s, 0..=2) && r32_h.contains(s) && !ex.kept_slots.contains(s)
                        })
                        .copied()
                        .collect();
                    let r32_d_ii: BTreeSet<u64> = if ex.amcheck_ii_ran {
                        ex.tainted_image_slots_ii
                            .iter()
                            .filter(|s| {
                                !matches!(s, 0..=2)
                                    && r32_h.contains(s)
                                    && !ex.kept_slots_ii.contains(s)
                            })
                            .copied()
                            .collect()
                    } else {
                        BTreeSet::new()
                    };
                    let r32_d_read = r32_d.iter().any(|s| {
                        ex.recovery_reads.iter().any(|(f, off, len)| {
                            *f == ex.page_file && *off < (s + 1) * 4096 && off + len > s * 4096
                        })
                    });
                    let mut r32_sites: Vec<XxSite> = ex.xx001.clone();
                    if lost_at_match > 0 && !r32_sites.contains(&XxSite::Read23) {
                        r32_sites.push(XxSite::Read23);
                    }
                    let r32_explained = |site: &XxSite| match site {
                        XxSite::Open | XxSite::Replay => r32_d_read,
                        XxSite::AmcheckI | XxSite::Read23 => !r32_d.is_empty(),
                        XxSite::AmcheckIi => !r32_d_ii.is_empty(),
                    };
                    let r32_applies = class == FaultClass::Mp
                        && !cfg.no_r32
                        && !r32_sites.is_empty()
                        && r32_sites.iter().all(r32_explained);
                    if r32_applies {
                        r32 = true;
                    } else {
                        o3_fail = Some(
                            "O-3(c): XX001 with every media fault on unread, unreferenced targets"
                                .to_owned(),
                        );
                    }
                }
            }
            None => {
                o3_fail =
                    Some("O-3(c): MP/MW round with XX001 and no twin (harness defect)".to_owned());
            }
        }
    }
    if matches!(class, FaultClass::Mp | FaultClass::Mw) && ex.ok && !xx_any {
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
        // applies unchanged. **X5-R28 adds the kept set `K` beside `R`:
        // a reissued-after-rollback slot whose reissue did not land is
        // tainted yet §3.3-clean — the completeness half skips it and
        // the exactness half fails a finding that names it, each only
        // while `r_active` holds; the completeness halves also skip the
        // meta slots 1 and 2 unconditionally (a meta-slot fault is
        // judged where it acts: open's selection, O-1, O-3 (a)–(d)).**
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
        // Structural guard (X5-R28): a kept slot must be a tainted
        // referenced slot outside R and outside the meta slots 0–2 —
        // checked whether or not `r_active` holds, like R's guard. A
        // harness defect must never pass.
        for slot in ex.kept_slots.iter() {
            if *slot <= 2
                || !ex.image_slots.contains(slot)
                || !ex.tainted_image_slots.contains(slot)
                || ex.rollback_slots.contains(slot)
            {
                o3e.get_or_insert_with(|| format!("O-3(e): harness K set malformed (slot {slot})"));
            }
        }
        // Completeness half, the X5-R5 completeness fix: run it whenever
        // amcheck (i) ran and tainted referenced slots exist, whatever
        // amcheck reported. (Run 1 gated it on findings existing, which
        // let a fully missed detection pass beside any other XX001 of
        // the round.) X5-R28: skip slots 1 and 2 (no §3.3 reference
        // model covers a meta slot) and, while `r_active` holds, the
        // kept slots — every other tainted referenced slot must still
        // be reported.
        if ex.amcheck_i_ran {
            for slot in ex.tainted_image_slots.iter() {
                if *slot <= 2 || (r_active && ex.kept_slots.contains(slot)) {
                    continue;
                }
                let reported = ex.amcheck_i.iter().any(|f| f.slot == Some(*slot));
                if !reported {
                    o3e.get_or_insert_with(|| {
                        format!("O-3(e): tainted referenced slot {slot} not reported")
                    });
                }
            }
        }
        // Exactness half (X5-R2's "and only for those", with X5-R5's R
        // and X5-R28's tightening: while `r_active` holds, a finding on
        // a kept slot fails — its bytes are the opened image's own
        // content, so a report names what amcheck cannot have seen; the
        // tainted ∪ active-R rule stands for everything else).
        for f in ex.amcheck_i.iter() {
            if let Some(slot) = f.slot {
                if r_active && ex.kept_slots.contains(&slot) {
                    o3e.get_or_insert_with(|| {
                        format!(
                            "O-3(e): amcheck reported kept slot {slot} (its bytes are the opened image's content)"
                        )
                    });
                    continue;
                }
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
        // against the (ii) sets and its own kept set `K_ii` (X5-R28),
        // with the same R.
        if ex.amcheck_ii_ran {
            // The (ii) structural guard (X5-R28), against the (ii)
            // sets, checked whether or not `r_active` holds.
            for slot in ex.kept_slots_ii.iter() {
                if *slot <= 2
                    || !ex.image_slots_ii.contains(slot)
                    || !ex.tainted_image_slots_ii.contains(slot)
                    || ex.rollback_slots.contains(slot)
                {
                    o3e.get_or_insert_with(|| {
                        format!("O-3(e): harness K set malformed (slot {slot})")
                    });
                }
            }
            for slot in ex.tainted_image_slots_ii.iter() {
                if *slot <= 2 || (r_active && ex.kept_slots_ii.contains(slot)) {
                    continue;
                }
                let reported = ex.amcheck_ii.iter().any(|f| f.slot == Some(*slot));
                if !reported {
                    o3e.get_or_insert_with(|| {
                        format!("O-3(e): (ii) tainted referenced slot {slot} not reported")
                    });
                }
            }
            for f in ex.amcheck_ii.iter() {
                if let Some(slot) = f.slot {
                    if r_active && ex.kept_slots_ii.contains(&slot) {
                        o3e.get_or_insert_with(|| {
                            format!(
                            "O-3(e): (ii) amcheck reported kept slot {slot} (its bytes are the opened image's content)"
                        )
                        });
                        continue;
                    }
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
        o1_detected_loss: lost_at_match,
        r32,
    }
}

/// A seed passes iff every round passes and no `DEADLOCK` or `PANIC`
/// occurred (Work 2.4 "Pass").
pub fn seed_passes(verdicts: &[RoundVerdict], deadlock: bool, panic: bool) -> bool {
    !deadlock && !panic && verdicts.iter().all(|v| v.pass)
}

/// X5-R7: the §8.3 projection of a scan — live values only; a tombstone
/// (and an elided tombstone) reads as absent.
pub fn live(s: &Scan) -> Vec<(&Vec<u8>, &Vec<u8>)> {
    s.iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
        .collect()
}

/// O-1 at one candidate `c`. `Ok(n)`: satisfied, with `n` comparisons
/// satisfied by a detected loss (X5-R6; always 0 unless `detect_ok`).
fn o1_at(
    model: &Model,
    c: u64,
    ex: &Extracted,
    reader: &mut dyn StateReader,
    sample_rng: &mut Rng,
    cfg: &OracleCfg,
    detect_ok: bool,
) -> Result<u32, String> {
    let mut lost: u32 = 0;
    // (a) the published set equals the model's at c.
    let want_pub = model.published_at(c);
    let trees: BTreeSet<u64> = if ex.published_read_failed {
        if !detect_ok {
            return Err("O-1(a): published-set read failed".to_owned());
        }
        lost += 1;
        want_pub
    } else {
        if ex.published != want_pub {
            return Err(format!(
                "O-1(a) at c={c}: published {:?} != model {:?}",
                ex.published, want_pub
            ));
        }
        ex.published.clone()
    };
    // (b)/(c)/(d) per published tree.
    for t in trees.iter() {
        if *t == CATALOG {
            continue;
        }
        match reader.scan(*t, c) {
            Ok(got) => {
                // X5-R36 (b): the model at (S = c, cut = c).
                let want = model.scan_at(*t, c, c);
                if live(&got) != live(&want) {
                    return Err(format!("O-1(b) at c={c}, tree {t}: scan mismatch"));
                }
            }
            Err(_) if detect_ok => lost += 1,
            Err(_) => return Err("O-1(b): scan read failed".to_owned()),
        }
        match reader.raw(*t) {
            Ok(raw) => {
                if !cfg.no_raw_cut && raw.iter().any(|e| e.ts > c) {
                    return Err(format!(
                        "O-1(c) at c={c}, tree {t}: raw entry above the cut (max ts {})",
                        raw.iter().map(|e| e.ts).max().unwrap_or(0)
                    ));
                }
            }
            Err(_) if detect_ok => lost += 1,
            Err(_) => return Err("O-1(c): raw read failed".to_owned()),
        }
        for s in model.sample_points(ex.gc_w, c, sample_rng) {
            match reader.scan(*t, s) {
                Ok(got) => {
                    // X5-R36 (b): the model at (S = s, cut = c) — the
                    // sample reads the store's recovered incarnation,
                    // which the candidate c names.
                    let want = model.scan_at(*t, s, c);
                    if live(&got) != live(&want) {
                        return Err(format!("O-1(d) at c={c}, tree {t}, S={s}: scan mismatch"));
                    }
                }
                Err(_) if detect_ok => lost += 1,
                Err(_) => return Err("O-1(d): scan read failed".to_owned()),
            }
        }
    }
    Ok(lost)
}
