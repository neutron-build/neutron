//! The seeded workload's self-tests (card X-5 Work 5, stage X-5c).
//!
//! These pin the generator's and driver's mechanics — determinism, the
//! W-SCRIPT tree rule, the hook wiring the workload needs, and the
//! 16-seed smoke the card names for `cargo test`. They deliberately do
//! **not** assert oracle outcomes: those are judged by the campaign
//! under rulings X5-R1–R6 (the worker never pre-reads campaign data; see
//! the README's X5-R6 note).

#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use e4_recovery::campaign;
use e4_recovery::harness::ScriptOp;
use e4_recovery::workload::{self, Knobs};

/// Seeds 0..16 run mechanically clean: no DEADLOCK, no PANIC, no S-BATON
/// violation, no task error, and the digests reproduce byte-identically
/// on a re-run (the card's 16-seed smoke; the gate's workspace test
/// stays under 60 s).
#[test]
fn smoke_16_seeds() {
    for seed in 0..16u64 {
        let rec = workload::run_seed(seed, None);
        assert!(
            !rec.deadlock,
            "seed {seed}: DEADLOCK (rounds: {})",
            rec.rounds.len()
        );
        assert!(!rec.panic, "seed {seed}: PANIC");
        assert!(!rec.baton, "seed {seed}: S-BATON violation");
        for (i, r) in rec.rounds.iter().enumerate() {
            assert!(
                r.task_error.is_none(),
                "seed {seed} round {i}: task error {:?}",
                r.task_error
            );
            assert!(r.fired, "seed {seed} round {i}: the crash never fired");
        }
        assert!(rec.defect.is_none(), "seed {seed}: defect {:?}", rec.defect);
        // The round count follows the knobs, except an early stop (a
        // failed round or a recovery without a store ends the chain —
        // recorded, never hidden).
        assert!(
            rec.rounds.len() as u32 <= Knobs::derive(seed, 0, &workload::Tuning::BASE).rounds,
            "seed {seed}: round count {} over the knobs",
            rec.rounds.len()
        );
        if !rec.stopped_early {
            assert_eq!(
                rec.rounds.len() as u32,
                Knobs::derive(seed, 0, &workload::Tuning::BASE).rounds,
                "seed {seed}: round count without an early stop"
            );
        }
        // The class follows seed % 10.
        let want = match seed % 10 {
            0..=6 => "C",
            7 | 8 => "MP",
            _ => "MW",
        };
        assert_eq!(rec.class, want, "seed {seed}: class");
        // The twin runs in lockstep with the real rounds for MP/MW
        // (none for C).
        let want_twin = if want == "C" { 0 } else { rec.rounds.len() };
        assert_eq!(
            rec.twin.len(),
            want_twin,
            "seed {seed}: twin rounds ({}) must match the real rounds ({})",
            rec.twin.len(),
            want_twin
        );
    }
    // Determinism: two runs of the same seed give the same digest, and a
    // threaded range run matches the single-threaded one.
    let a = workload::run_seed(7, None);
    let b = workload::run_seed(7, None);
    assert_eq!(a.digest, b.digest, "seed 7 digests differ across runs");
    let u = workload::run_seed(12, None);
    let u2 = workload::run_seed(12, None);
    assert_eq!(
        u.digest, u2.digest,
        "seed 12 (uniform crash) digests differ across runs"
    );
    let t1 = campaign::run_range(0, 5, None, 1);
    let t3 = campaign::run_range(0, 5, None, 3);
    for (x, y) in t1.iter().zip(t3.iter()) {
        assert_eq!(x.seed, y.seed);
        assert_eq!(x.digest, y.digest, "seed {} differs across threads", x.seed);
    }
}

/// **X5-R37 (P ruling nv3-X-5c-F4-esc-1; this card's step 1): the
/// frozen-tuning digest-literal pin.** A seed pin's identity is
/// `(generator, seed)`, not the seed alone: every literal below is the
/// digest of `run_seed_with(seed, None, &Tuning::BASE)` — 1b0ce54e's
/// generator, bit for bit — copied from X-5c-F4's Acceptance file
/// (`$HOME/.cache/nv3-review/runlogs/X-5c-F4.acc.campaign.json`, sha256
/// `bec7056c4463b5b812f8d27a162046cd42be1f1242970b1da4da458305ab3ac6`,
/// prov `commit=1b0ce54e63d044b239cae497d22104ccced41cba`), except
/// seed 4's literal, which moved with step 2's Q11 wiring (its r0 Q11
/// flag flipped 0→1 — the only digest input the wiring touches;
/// X5-R37's moved-digest rule). The seed
/// list is every seed any test passes to a seed runner (found by grep
/// over `tests/`, cross-checked against P's 43-seed list; the smoke and
/// round-trip seeds 0–9 are the grep's additions from the file, and P's
/// 7359 — one of the C-DIV-in-MW five — is run by no test at this tip;
/// the rework-1 pins added 41 and 303, their literals taken from the
/// same file — neither moved with the rework's store fixes, `szc` not
/// being a digest input; X-5c-F4-F1-F1 step 0 (a) added 2376, its
/// literal F4's own — the `wal_recycle` crash fix moves no digest
/// (gate-measured over the whole 10k; only seed 2376 r2's non-digest
/// `szc` changed)).
/// M-BASEDRIFT (one BASE constant changed) fails this pin.
///
/// **This card's movement, attributed (the REWORK verdict's finding 6,
/// re-measured at the rework tip):** Work 1 (B1) moved 190;
/// Work 2 (A) moved 66, 907, 1769, 6149, 7469, 7528; Work 3 (replay)
/// moved 112, 198, 303; the rework's pin hold (rework step 1) moved
/// 112 (`2438489582 -> 3003462667`), 1769 (`1311732889 ->
/// 2372375821`, back to 1b0ce54e's own value) and 7469
/// (`3658720859 -> 1670717970`) — each value below is this tip's own
/// measurement, the literals re-derived once (rework step 4).
///
/// **X-5c-F4-F1-F1-CXX-F1 (X5-R48, step 6):** the joint re-pair moved
/// 7469 alone (`1670717970 -> 1393729414`, the gate trial's value —
/// the re-paired twin now fires at the joint spec `Mid2@4` and the
/// seed gains its round 2 back). The 54 seeds re-run singly at this
/// tip moved nothing else, and the contiguous 0..200 moved no digest
/// and changed no record (the failing rounds stay the known 8/178
/// pair, `DEFECT_SEEDS=0`).
/// **X-5c-F4-F1-F1-F3-F2-F1 (X5-R49, step 2):** 13 of the 54 moved
/// with X5-R49 — 3 `923362676 -> 4196035152`, 47 `2526989435 ->
/// 1878234829`, 112 `3003462667 -> 841173046`, 198 `95622311 ->
/// 3654049329`, 303 `4147169426 -> 144621379`, 967 `2847369120 ->
/// 3916549382`, 987 `2492798953 -> 1449471436`, 1749 `1584051239 ->
/// 3785784973`, 2376 `3116539881 -> 926921172`, 2927 `1901399424 ->
/// 1916772534`, 5549 `308297210 -> 3614535945`, 7469 `1393729414 ->
/// 1742707490`, 7528 `122281874 -> 3346573892` (F2 measured them; the
/// gate's verdict confirms; this card re-measured singly, same 13) —
/// the freed-home release changes the allocation scan's yield pattern
/// from a workload's first free onward, so the drawn crash points and
/// slot numbers land elsewhere. The other 41 are byte-identical.
/// **X-5c-F4-F1-F1-F3-F2-F1-F2 re-derived one more:** 2669
/// `357787865 -> 1374321949` (twin directory re-seed at round 1 —
/// step 2's attribution event; the only one of the 54 that moved, and
/// the only literal this card re-derives).
///
/// **X-5c-PINS-1 item 3 (verdict `74f411f0` finding 1):** `6849` is a
/// new row; no existing literal moved. At round 1 of this MW seed the
/// twin directory re-seed fires with real `cut` 0 and twin `cut` 5.
/// Keeping the twin's own `cut` (**G-RESEED-TWIN-CUT**) moves the
/// digest `3952923334 -> 1958644185`.
#[test]
fn x5_r37_base_tuning_reproduces_1b0ce54e() {
    const WANT: &[(u64, u64)] = &[
        (0, 49696441),
        (1, 1198539130),
        (2, 927823387),
        (3, 4196035152),
        (4, 1441374082),
        (5, 980071820),
        (6, 2097771150),
        (7, 2267580514),
        (8, 861904777),
        (9, 429574859),
        (10, 1043516495),
        (11, 3292953852),
        (12, 1120327746),
        (13, 2781612228),
        (14, 2027633753),
        (15, 3135194667),
        (41, 2325274127),
        (47, 1878234829),
        (48, 605204002),
        (60, 2199024193),
        (66, 3628190779),
        (67, 1488955537),
        (112, 841173046),
        (118, 4034749261),
        (198, 3654049329),
        (221, 2732451886),
        (268, 317704428),
        (303, 144621379),
        (308, 522943877),
        (387, 2151920803),
        (403, 2657356820),
        (537, 2279204905),
        (660, 3256450980),
        (747, 4071845386),
        (907, 1749572610),
        (967, 3916549382),
        (987, 1449471436),
        (1267, 1225086008),
        (1749, 3785784973),
        (1769, 2372375821),
        (1787, 1119157608),
        (1887, 3622450829),
        (2376, 926921172),
        (2669, 1374321949),
        (2927, 1916772534),
        (4049, 2182387428),
        (4309, 3466635396),
        (5549, 3614535945),
        (5808, 2754578359),
        (6149, 123186068),
        (6849, 3952923334),
        (7469, 1742707490),
        (7528, 3346573892),
        (8728, 3345856243),
        (9809, 1177349546),
    ];
    // The seeds are independent (S-DET/W-DET, pinned), so the pin runs
    // them across a small thread pool; the digests are byte-identical
    // to a serial run.
    let got: Vec<(u64, u64)> = std::thread::scope(|s| {
        let handles: Vec<_> = WANT
            .iter()
            .map(|(seed, _)| {
                s.spawn(move || {
                    let rec = workload::run_seed_with(*seed, None, &workload::Tuning::BASE);
                    (*seed, rec.digest)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or((u64::MAX, u64::MAX)))
            .collect()
    });
    for ((seed, want), (_, actual)) in WANT.iter().zip(got.iter()) {
        assert_eq!(
            want, actual,
            "seed {seed}: Tuning::BASE must reproduce 1b0ce54e's digest bit for bit (X5-R37)"
        );
    }
}

/// **X5-R37: this card does no retune** — `Tuning::FINAL` equals
/// `Tuning::BASE` at this card's tip (X-5c-F4's relaunch is the only
/// card that may change FINAL, and the probe shares it sets in step 3
/// are reverted).
#[test]
fn x5_r37_final_equals_base_at_this_tip() {
    assert_eq!(
        workload::Tuning::FINAL,
        workload::Tuning::BASE,
        "this card builds the machinery only: FINAL == BASE (the card's Goal)"
    );
}

/// **X-5c-F4-F1-F1-F2 step 3: no late epoch copy installs at BASE**
/// (verdict X-5c-F4-F1-F1 finding 1). The late-copy events this card
/// fixes occur at BASE on their own (7 over 0..200: seed 96 twice, 104,
/// 161 and 190 three times, counted with the gate prototype's
/// `LATECOPY` probe on a scratch export of the pre-fix and post-fix
/// tips): a first touch's copy parks in the exhausted reserve path, the
/// checkpoint's step 2 resolves the override to `Img::At` (or the
/// window closes) while it waits, and the install — pre-fix — landed
/// past `ckpt:freecopies`. With the fix (`epoch_copy:install` installs
/// only while `Img::CopyPending` still holds) every one of those
/// installs is a **drop**: the count of late *installs* at BASE is
/// zero, which is what this pin holds.
///
/// The count itself is not observable inside this card's limits (a new
/// store counter would touch `StoreInner`, outside "record_image_copy
/// … nothing else"), so the pin holds the mechanism through the seeds
/// that hit the path, at the record level: their post-fix digests are
/// pinned, and 96 and 190 moved (83795711 → 4266526636 and 813405985 →
/// 4013865743) — the drop is the only difference. X-5c-F4-F1-F1-CXX
/// moved 190 again (4013865743 → 679383372): its Work 1 re-validation
/// closes the exhausted path's second outcome the -F2 verdict named
/// (the schedule the -F2 guard had moved onto B1 now resolves).
/// Reverting the guard
/// (M-LATECOPY-INSTALL) restores the pre-fix digests and fails this
/// pin; the directed pin
/// (`late_epoch_copy_must_not_enter_the_next_image`, `tests/store.rs`)
/// holds the schedule itself.
///
/// **X-5c-F4-F1-F1-F3-F2-F1 (X5-R49, step 2):** 96 and 190 moved again
/// (`4266526636 -> 1886292663` and `679383372 -> 3730452910`) — the
/// freed-home release shifts the allocation scan's yields, so the
/// exhausted-reserve interleavings that park these seeds' first-touch
/// copies land at other points; the drop (what the pin holds) is
/// unchanged and 104/161 are byte-identical.
#[test]
fn late_copy_count_is_zero_at_base() {
    // (seed, post-fix digest); 104 and 161 hit the path too but their
    // late installs were observably benign pre-fix — the digest is the
    // same either side, and pinning it keeps them under the pin.
    const WANT: &[(u64, u64)] = &[
        (96, 1886292663),
        (104, 3776021738),
        (161, 3908247512),
        (190, 3730452910),
    ];
    for (seed, want) in WANT {
        let rec = workload::run_seed_with(*seed, None, &workload::Tuning::BASE);
        assert_eq!(
            rec.digest, *want,
            "seed {seed}: the late-copy drop is part of BASE — a moved digest means \
             the install guard is gone (M-LATECOPY-INSTALL) or the drop changed"
        );
    }
}

/// **Q11's seed pin (card X-5c-F4-F1 step 2): P's probe hit.** Seed 4
/// round 0, under `Tuning::BASE`: the round's WAL crossed into a
/// recycled segment whose start still held its previous life's
/// records, the recovery walk read them, and the wired counter fires
/// the Q11 flag (P's probe of 2026-11-04: wired, 1 hit in 891 rounds
/// over 0..600, seed 4 r0 — ≈ 15 per 10k rounds; "probe, not
/// evidence"; the wiring alone cannot meet Q11's 100, which is what
/// X5-R38's W11 targets).
#[test]
fn q11_seed4_round0_reads_a_stale_record_at_a_recycled_segment_start() {
    let rec = workload::run_seed_with(4, None, &workload::Tuning::BASE);
    let r0 = &rec.rounds[0];
    assert!(
        r0.q11_stale_seg,
        "seed 4 r0: the stale record read at the recycled segment's start must set the Q11 flag"
    );
}

/// **Rework 1 (P gate verdict X-5c-F4-F1, finding 1; the verdict's
/// named pins): a sim-crash unwind must not reset `ckpt_closed_dirty`
/// — `RoundRec.szc` is true through the second disjunct.** The verdict:
/// "`checkpoint()`'s `ckpt:abort` runs on every `Err`, including a sim
/// crash, because the unwinding task gets its turn; it sets
/// `ckpt_closed_dirty = 0` before `run_round` snapshots `obs`" — 0 in
/// 15,386 rounds at the first attempt's tip, the second disjunct dead.
/// With the fix, the folded `n` survives the unwind into the crash-time
/// snapshot. Literal configs (both measured, under `Tuning::BASE`):
/// **seed 41 round 2** crashes at `After1@0` and **seed 303 round 2**
/// at `After3@0` — a checkpoint open at the crash whose begin step
/// folded 221 (41; 4·F = 208) and 241 (303; 4·F = 196) dirtied LPIDs
/// while the open interval at the crash held only 1 and 3 — so the
/// record's `szc` can only be true through `ckpt_closed_dirty ≥ 4·F`.
/// The record shows `szc` itself (the verdict's form); that the pins
/// pin the disjunct and the unwind is proven by the mutants:
/// M-SZC-ABORTRESET (reset on every `Err`) and M-SZC-NOCK (the
/// disjunct removed) each fail this pin. `szc` is not a digest input —
/// no digest moves (both seeds' literals in the X5-R37 pin are F4's
/// own).
#[test]
fn szc_survives_the_sim_crash_unwind_into_the_record() {
    for (seed, hook, four_f) in [(41u64, "After1@0", 208u64), (303, "After3@0", 196)] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        assert!(rec.defect.is_none(), "seed {seed}: {:?}", rec.defect);
        let r2 = rec
            .rounds
            .get(2)
            .unwrap_or_else(|| panic!("seed {seed}: three rounds"));
        assert_eq!(
            r2.crash, hook,
            "seed {seed} r2: the pinned hook crash (the open checkpoint at the crash)"
        );
        assert!(r2.fired, "seed {seed} r2: the crash fired");
        assert!(
            r2.szc,
            "seed {seed} r2: szc must be true — the open checkpoint folded \
             ≥ 4·F = {four_f} LPIDs and the unwind must not reset it"
        );
        assert!(
            r2.pass,
            "seed {seed} r2: the round passes: o1={:?} o2={:?} o3={:?}",
            r2.o1_fail, r2.o2_fail, r2.o3_fail
        );
    }
}

/// **Rework 1, the record-level M-SZC-ANY pin (the card's own mutant
/// `szc := sizing_ok` at `round_rec`, UNCAUGHT at the first attempt's
/// gate): a completed ≥ 4·F interval never makes the record's `szc`
/// true.** Literal config: seed 660 round 2, under `Tuning::BASE` —
/// the crash (`Mid2@3`) cut a checkpoint whose begin step folded a
/// small interval while `dirty_max` = 250 ≥ 4·F = 184 from an earlier,
/// completed one — so the record carries `sizing_ok` true and `szc`
/// false. The literal `szc := sizing_ok` at `round_rec` flips this
/// round's `szc` to true and fails this pin (the function-level pin
/// `szc_a_completed_4f_interval_and_a_small_cut` reads `szc_of`
/// directly and cannot see it).
#[test]
fn szc_a_completed_interval_never_sets_the_record_flag() {
    let rec = workload::run_seed_with(660, None, &workload::Tuning::BASE);
    let r2 = rec
        .rounds
        .get(2)
        .unwrap_or_else(|| panic!("seed 660: three rounds"));
    assert!(
        r2.sizing_ok,
        "seed 660 r2: an earlier interval reached 4·F (dirty_max 250 ≥ 184)"
    );
    assert!(
        !r2.szc,
        "seed 660 r2: the crash cut a small open interval (dirty_now 0 < 184) \
         with the open checkpoint's own fold small — szc must stay false"
    );
}

/// **X5-R38's W12 pin (card X-5c-F4-F1 step 3; a literal config): the
/// share-forced crash at the rename→dirsync window drops the rename
/// (Q12).** The tuning is BASE with `keep_latest_pm_table` zeroed
/// (every F-CRASH directory draw drops the volatile prefix) and the
/// W12 share at 1000 permille (every uniform draw takes the window
/// list); seed 11's round 0 is the pinned shape. M-W12ALL (the W12
/// draw over all yields) moves the crash out of the window and fails
/// this pin.
#[test]
fn x5_r38_w12_crash_drops_the_rename() {
    let mut tun = workload::Tuning::BASE;
    tun.keep_latest_pm_table = [0, 0, 0, 0, 0];
    tun.w12_share = 1000;
    let rec = workload::run_seed_with(11, None, &tun);
    assert!(rec.defect.is_none(), "seed 11: {:?}", rec.defect);
    let r0 = &rec.rounds[0];
    assert_eq!(
        r0.cwin,
        Some("w12"),
        "seed 11 r0: the crash must be drawn from the W12 window list"
    );
    assert!(
        r0.fired,
        "seed 11 r0: the window crash fired (never a NEVER-FIRED)"
    );
    assert!(
        r0.q12_dropped_rename,
        "seed 11 r0: the crash in the rename→dirsync window with keep-latest 0 must drop the G1 rename (Q12)"
    );
    assert!(
        r0.pass,
        "seed 11 r0: the round passes (o1={:?} o2={:?} o3={:?})",
        r0.o1_fail, r0.o2_fail, r0.o3_fail
    );
}

/// **X5-R38's W11 pin (card X-5c-F4-F1 step 3; a literal config): the
/// share-forced crash in the recycled-segment window reads the stale
/// record (Q11).** The tuning is BASE with `w11_share` at 1000
/// permille. Seed 276's round 0 crashes inside the window — after the
/// segment below the recycled boundary `b` synced durable through
/// `b`, before `b`'s own sync — so the crash drops `b`'s volatile
/// record, and the recovery walk hops into `b` and reads its previous
/// life's bytes.
#[test]
fn x5_r38_w11_crash_reads_stale_at_start() {
    let mut tun = workload::Tuning::BASE;
    tun.w11_share = 1000;
    let rec = workload::run_seed_with(276, None, &tun);
    assert!(rec.defect.is_none(), "seed 276: {:?}", rec.defect);
    let r0 = &rec.rounds[0];
    assert_eq!(
        r0.cwin,
        Some("w11"),
        "seed 276 r0: the crash must be drawn from the W11 window list"
    );
    assert!(
        r0.fired,
        "seed 276 r0: the window crash fired (never a NEVER-FIRED)"
    );
    assert!(
        r0.q11_stale_seg,
        "seed 276 r0: the crash in the recycled-segment window must make the walk read the stale record (Q11)"
    );
    assert!(
        r0.pass,
        "seed 276 r0: the round passes (o1={:?} o2={:?} o3={:?})",
        r0.o1_fail, r0.o2_fail, r0.o3_fail
    );
}

/// **The re-gate's finding 1 (verdict X-5c-F4-F1, MERGE-WITH-FOLLOWUPS;
/// card X-5c-F4-F1-F1 step 0 (a)): `wal_recycle` must not swallow a
/// delivered crash — `szc` keeps the open fold.** The sim's env maps
/// every disk error to `SimFail::Crashed`, so the pre-fix
/// `.map_err(Fail::Sim).is_ok()` read a crash delivered at the rename's
/// yield as "not renamed" and quietly skipped on: `After6` and
/// `ckpt:end` ran on the crashed world, the checkpointer's next poll
/// took its final checkpoint, and that `ckpt:begin` folded again before
/// the gate's yield unwound it — clearing `ckpt_closed_dirty` (and
/// `ckpt_splits`/`ckpt_merges`) after the crash, so the crash-time
/// snapshot undercounted. Literal config: seed 2376 round 2, under
/// `Tuning::BASE` — the crash `uniform@y:17768` lands at the rename's
/// yield with a checkpoint open whose begin step folded 235 ≥ 4·F = 184
/// dirtied LPIDs, so the record's `szc` can only be true through the
/// fold surviving the unwind into the snapshot.
/// **X-5c-F4-F1-F1-F3-F2-F1 (X5-R49, step 2):** the uniform crash
/// moved `uniform@y:17768 -> uniform@y:15359` — the freed-home release
/// shifts the scan yields, so the drawn uniform point lands elsewhere;
/// at the new point the round still crashes at the rename's yield with
/// the open checkpoint's fold (`szc` true, measured at this tip), so
/// the pinned shape is unchanged. **M-RENAME-SWALLOW**
/// (the `is_ok()` form restored) fails this pin (`szc` false, the
/// gate's measurement: 15 of 15,386 rounds had the observable move
/// after the fire); **M-RENAME-ANYERR** (a plain `?` on the rename —
/// wrong because "`to` exists" is a routine uncrashed skip, 111 times
/// over 0..200) fails the X5-R37 digest pin (67 of 200 digests moved).
/// No digest moves on the fix itself (seed 2376's X5-R37 literal is
/// F4's own).
#[test]
fn szc_rename_crash_keeps_the_open_fold() {
    let rec = workload::run_seed_with(2376, None, &workload::Tuning::BASE);
    assert!(rec.defect.is_none(), "seed 2376: {:?}", rec.defect);
    let r2 = rec
        .rounds
        .get(2)
        .unwrap_or_else(|| panic!("seed 2376: three rounds"));
    assert_eq!(
        r2.crash, "uniform@y:15359",
        "seed 2376 r2: the pinned uniform crash at the recycle rename's yield"
    );
    assert!(r2.fired, "seed 2376 r2: the crash fired");
    assert!(
        r2.szc,
        "seed 2376 r2: szc must be true — the open checkpoint folded 235 ≥ 4·F = 184 \
         LPIDs and the rename's delivered crash must not let ckpt:end clear it"
    );
}

/// **The re-gate's finding 2, first half (verdict X-5c-F4-F1;
/// card X-5c-F4-F1-F1 step 0 (b)): W11 needs "no completed `sync_data`
/// since" — the retire of `recycled_unsynced` is load-bearing.** Ruling
/// (i) opens the window only while the boundary `b` "has had no
/// completed `sync_data` since" its rename; a completed sync of a
/// segment at or above `b` retires every boundary `≤ s`
/// (`w11_window_note`'s `recycled_unsynced.retain`), so the window list
/// the crash draw reads holds only still-unsynced boundaries. Literal
/// config: BASE with `w11_share` 1000, seed 30 round 0 — the round's
/// every recycled boundary was synced over by the crash point, the
/// window list is empty, and the pick records the fallback `w11-` (the
/// plain uniform point stood, and it fired). **G-W11-NORETIRE** (the
/// retain line removed) fails this pin: the boundaries stay in the
/// set, the list is non-empty, and the pick reads `w11` instead — the
/// measured flip at this tip (the rework's pin hold re-derived the
/// schedule: seed 30 reads `w11-` clean and `w11` under the mutant,
/// with seeds 37, 98, 111, 138, 156, 170 and 192 the other flippers of
/// 0..200; the attempt-1 literal, seed 11, reads `w11-` both ways —
/// vacuous, the rework verdict's finding 2 — and 30 was the literal
/// before this card's drop rework moved it off).
#[test]
fn x5_r38_w11_needs_no_completed_sync_since() {
    let mut tun = workload::Tuning::BASE;
    tun.w11_share = 1000;
    let rec = workload::run_seed_with(30, None, &tun);
    assert!(rec.defect.is_none(), "seed 30: {:?}", rec.defect);
    let r0 = &rec.rounds[0];
    assert_eq!(
        r0.cwin,
        Some("w11-"),
        "seed 30 r0: every recycled boundary has had a completed sync since its rename, \
         so the W11 window list must be empty and the pick must record the fallback"
    );
    assert!(
        r0.fired,
        "seed 30 r0: the fallback uniform crash fired (never a NEVER-FIRED)"
    );
}

/// **Rework 2 (P gate verdict X-5c-F4-F1, ruling (i); the verdict's
/// added pin): a W11 crash's walk reaches `b`.** Ruling (i): the window
/// opens "when a `sync_data` of the segment below `b` completes with
/// `wal.written ≥ b`: that segment's whole extent … is then durable, so
/// the walk reaches `b`". The literal config mirrors the driver's
/// uniform-round machinery by hand: the counting pass (crash disabled,
/// `w11_share` 1000) records the round's W11 window openings, the real
/// round crashes at the first recorded window yield, and the recovered
/// store's `wal.append` — the walk's valid end — must be at or above
/// the boundary the crash-time snapshot names (`WorkloadObs::w11_open`,
/// the open window's `b`). Seed 10 round 0 is the pinned shape (the
/// gate's post-fix 0..200 probe measured it a Q11 hit, which is this
/// property plus `b`'s first sector not surviving). **M-W11-ANYSYNC**
/// (the window opened on **any** completed sync of the segment below
/// `b`, the `wal.written ≥ b` term removed — the gate's finding 2)
/// fails this pin: the crash lands where the walk stops on the
/// not-yet-durable tail below `b` (0 of 150 picks hit Q11 as built).
#[test]
fn x5_r38_w11_crash_walk_reaches_the_boundary() {
    use e4_recovery::harness::{self, CrashSpec, MediaDraw, RoundFaults, Script};
    use e4_recovery::model::Model;
    use e4_recovery::sim::FaultClass;

    let seed = 10u64;
    let mut tun = workload::Tuning::BASE;
    tun.w11_share = 1000;
    let knobs = Knobs::derive(seed, 0, &tun);
    let cfg = knobs.store_cfg(None);
    let (base, _desc, _hook) = workload::build_round(seed, 0, &tun, &knobs, &Model::new(), 0);
    let probe_faults = RoundFaults {
        class: FaultClass::C,
        knobs: knobs.fault,
        directed: Vec::new(),
        no_r_taint: false,
        media: MediaDraw::None,
        twin_supply: None,
        nested_draw: 0,
        probe_pass: true,
    };
    let (disk, st0) = harness::create_store(cfg.clone(), seed, seed as u128).expect("create");
    let probe = harness::run_round(
        seed,
        0,
        &Script {
            crash: None,
            ..base.clone()
        },
        &probe_faults,
        cfg.clone(),
        disk.clone(),
        st0.clone(),
        &Model::new(),
        0,
        None,
        None,
    );
    let k = *probe
        .obs
        .w11_yields
        .first()
        .expect("the counting pass opens a W11 window (seed 10 r0)");
    let real = harness::run_round(
        seed,
        0,
        &Script {
            crash: Some(CrashSpec::Yield(k)),
            ..base
        },
        &RoundFaults {
            class: FaultClass::C,
            knobs: knobs.fault,
            directed: Vec::new(),
            no_r_taint: false,
            media: MediaDraw::None,
            twin_supply: None,
            nested_draw: 0,
            probe_pass: false,
        },
        cfg.clone(),
        disk,
        st0,
        &Model::new(),
        0,
        None,
        None,
    );
    assert!(
        real.obs.crash_fired,
        "seed 10 r0: the window crash at yield {k} fired (never a NEVER-FIRED)"
    );
    let b = real
        .obs
        .w11_open
        .expect("the W11 window was open at the crash");
    let end = real
        .end_store
        .as_ref()
        .expect("the round's recovery produced a store");
    assert!(
        end.wal.append >= b,
        "the walk's valid end ({}) must reach the window's boundary b={b}: \
         everything below b was durable at the crash (ruling (i))",
        end.wal.append
    );
}

/// **Rework 3 (P gate verdict X-5c-F4-F1, finding 5): `cwin` is a
/// digest input — pinned at the record level.** Two per-seed records
/// differing only in a round's `cwin` have different digests (a window
/// pick `Some("w11")`/`Some("w12")` against the plain uniform `None`),
/// and two records whose shared round carries `None` on both sides are
/// unchanged by it (BASE's shape — every pick `None`, which is why BASE
/// digests never move on the field). **G-CWIN-NODIGEST** (the `cwin`
/// contribution removed from `SeedRec::finish`) fails this pin.
#[test]
fn cwin_is_a_digest_input_only_when_set() {
    use e4_recovery::workload::{RoundRec, SeedRec};
    let rec_of = |cwin: Option<&'static str>| {
        let mut rec = SeedRec {
            seed: 12,
            class: "C".to_owned(),
            knobs: 99,
            rounds: vec![RoundRec {
                cwin,
                ..RoundRec::default()
            }],
            ..SeedRec::default()
        };
        rec.finish();
        rec
    };
    let none = rec_of(None).digest;
    let w11 = rec_of(Some("w11")).digest;
    let w12 = rec_of(Some("w12")).digest;
    assert_ne!(
        none, w11,
        "a round picked into the W11 window must move the digest (G-CWIN-NODIGEST reverts this)"
    );
    assert_ne!(
        w11, w12,
        "the two windows are different digest inputs (a round picked into W12 vs W11)"
    );
    assert_eq!(
        none,
        rec_of(None).digest,
        "both `None` leaves the digest unchanged (BASE's shape: no shares, every pick None)"
    );
}

/// The scheduler-level uniform crash (5.5's "uniform over the round's
/// yield points"): the counting pass measures the round's true yield
/// total, the drawn point is always reachable — every round of the
/// uniform seeds fires, no silent no-crash round — and the draw is
/// pure. A never-fired crash is a harness defect through
/// [`workload::crash_fired_defect`], never a pass.
#[test]
fn uniform_crash_fires_at_the_counted_yield() {
    // Seeds 10..15 are uniform ((seed/10) % 2 == 1): 10/12/14 class C,
    // 11 MP, 13/15 MW. Every round must have fired its crash.
    for seed in [10u64, 11, 12, 13, 14, 15] {
        let rec = workload::run_seed(seed, None);
        assert!(rec.defect.is_none(), "seed {seed}: {:?}", rec.defect);
        assert!(!rec.rounds.is_empty(), "seed {seed}: no rounds");
        for (i, r) in rec.rounds.iter().enumerate() {
            assert!(r.fired, "seed {seed} round {i}: the crash never fired");
        }
    }
    // The point draw is pure and stays inside the counted total (and a
    // zero-yield round still addresses the first yield, never `0`).
    let k1 = workload::uniform_yield_point(12, 0, 10_000);
    let k2 = workload::uniform_yield_point(12, 0, 10_000);
    assert_eq!(k1, k2, "the yield point draw is not pure");
    assert!((1..=10_000).contains(&k1), "drawn point {k1} out of range");
    assert_eq!(
        workload::uniform_yield_point(12, 0, 0),
        1,
        "the empty round must still address yield 1"
    );
    // The defect rule: a never-fired crash is a defect whatever the
    // description; a fired crash never is.
    let mut out = e4_recovery::harness::RoundOutcome::default();
    assert!(workload::crash_fired_defect(&out, "uniform@y:7").is_some());
    out.obs.crash_fired = true;
    assert!(workload::crash_fired_defect(&out, "uniform@y:7").is_none());
}

/// The knob derivation is total and in the card's ranges (5.1), and the
/// per-round feature mix (5.2) is a pure function of (seed, round).
#[test]
fn knobs_derive_in_range() {
    for seed in 0..200u64 {
        let k = Knobs::derive(seed, 0, &workload::Tuning::BASE);
        assert!(
            (2..=4).contains(&k.committers),
            "committers {}",
            k.committers
        );
        assert!((2..=4).contains(&k.trees), "trees {}", k.trees);
        assert!(
            (1200..=3000).contains(&k.keyspace),
            "keyspace {}",
            k.keyspace
        );
        assert!(
            [0, 3, 7].contains(&k.syncoff_tenths),
            "syncoff {}",
            k.syncoff_tenths
        );
        assert!([8, 16, 254].contains(&k.map_leaf_cap));
        assert!([128, 512, 32_576].contains(&k.freeset_leaf_bits));
        assert!([1, 2, 4].contains(&k.r_epoch));
        assert!((1..=3).contains(&k.rounds));
        assert!((320..=700).contains(&k.chunked_ops));
        assert!(k.pool_frames() >= 12);
        let k2 = Knobs::derive(seed, 0, &workload::Tuning::BASE);
        assert_eq!(k.digest(), k2.digest(), "knob digest not pure");
        // The feature mix is per-round.
        let mix = |r: u32| {
            format!(
                "{}{}",
                u8::from(Knobs::derive(seed, r, &workload::Tuning::BASE).bulk_load),
                u8::from(Knobs::derive(seed, r, &workload::Tuning::BASE).drop_tree)
            )
        };
        assert_ne!(mix(0), "", "empty mix");
    }
    // The bulk-load feature really appears on some rounds (25%) and so
    // does the drop feature.
    let mut loads = 0u32;
    let mut drops = 0u32;
    for seed in 0..400u64 {
        for r in 0..3 {
            let k = Knobs::derive(seed, r, &workload::Tuning::BASE);
            loads += u32::from(k.bulk_load);
            drops += u32::from(k.drop_tree);
        }
    }
    assert!(loads > 20, "bulk loads too rare: {loads}");
    assert!(drops > 20, "drops too rare: {drops}");
}

/// The crash spec follows 5.5: targeted (hook `(seed/20) % 10`, a seeded
/// checkpoint index) iff `(seed/10) % 2 == 0`, else uniform over the
/// script's op points.
#[test]
fn crash_spec_targeting() {
    use e4_recovery::harness::CrashSpec;
    use e4_recovery::store::ckpt::CKPT_HOOKS;
    let knobs = Knobs::derive(0, 0, &workload::Tuning::BASE);
    let model = e4_recovery::model::Model::new();
    for seed in 0..40u64 {
        let (script, _d, hook) =
            workload::build_round(seed, 0, &workload::Tuning::BASE, &knobs, &model, 0);
        let total: u64 = script.tasks.iter().map(|t| t.ops.len() as u64).sum();
        assert!(total > 10, "seed {seed}: script too small ({total})");
        if (seed / 10) % 2 == 0 {
            let want = format!("{:?}", CKPT_HOOKS[((seed / 20) % 10) as usize]);
            assert_eq!(hook, want, "seed {seed}: targeted hook");
            assert!(matches!(script.crash, Some(CrashSpec::Hook { .. })));
        } else {
            assert!(hook.is_empty(), "seed {seed}: uniform crash has no hook");
            assert!(matches!(script.crash, Some(CrashSpec::TaskYield(..))));
        }
    }
}

/// W-SCRIPT: round r's script only references trees the model at the cut
/// has (created and not dropped), so no scripted Put can park on a
/// directory entry no committer will install.
#[test]
fn script_references_only_model_trees() {
    let knobs = Knobs::derive(3, 0, &workload::Tuning::BASE);
    let model = e4_recovery::model::Model::new();
    let (script, _, _) = workload::build_round(3, 0, &workload::Tuning::BASE, &knobs, &model, 0);
    let trees = workload::trees_at(&model, 0);
    assert!(trees.is_empty(), "the empty model has no trees");
    // Every Put/Delete references a tree the setup commit creates (the
    // first committer op) — checked by generation, so collect the set of
    // created trees of the whole script.
    use e4_recovery::model::Op;
    let mut created = std::collections::BTreeSet::new();
    for t in &script.tasks {
        for op in &t.ops {
            if let e4_recovery::harness::ScriptOp::Commit { ops, .. } = op {
                for o in ops {
                    if let Op::CreateTree { tree } = o {
                        created.insert(*tree);
                    }
                }
            }
        }
    }
    assert!(!created.is_empty(), "the setup commit creates the trees");
    for t in &script.tasks {
        for op in &t.ops {
            if let e4_recovery::harness::ScriptOp::Commit { ops, .. } = op {
                for o in ops {
                    match o {
                        Op::Put { tree, .. } | Op::Delete { tree, .. } => {
                            assert!(created.contains(tree), "op tree {tree} never created");
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

/// The chunked transaction spans ≥ 2 segments by construction (5.2):
/// its encoded stream crosses 132 KiB, so no draw can fit one segment.
#[test]
fn chunked_txn_spans_two_segments() {
    use e4_recovery::model::Op;
    let knobs = Knobs::derive(5, 0, &workload::Tuning::BASE);
    let model = e4_recovery::model::Model::new();
    let (script, _, _) = workload::build_round(5, 0, &workload::Tuning::BASE, &knobs, &model, 0);
    let mut spans = 0u32;
    for t in &script.tasks {
        for op in &t.ops {
            if let e4_recovery::harness::ScriptOp::Chunked { ops, .. } = op {
                let bytes: u64 = ops
                    .iter()
                    .map(|o| match o {
                        Op::Put { value, key, .. } => 24 + key.len() as u64 + value.len() as u64,
                        _ => 32,
                    })
                    .sum();
                assert!(bytes >= 132 * 1024, "chunked txn too small ({bytes} bytes)");
                assert!(
                    ops.len() >= 300,
                    "chunked txn op count {} below the card's 300",
                    ops.len()
                );
                // The record cap: every 20-op chunk stays under 16 KiB.
                assert!(bytes / (ops.len() as u64 / 20 + 1) < 16 * 1024);
                spans += 1;
            }
        }
    }
    assert!(spans >= 1, "every round carries a chunked txn");
}

/// The campaign JSON round-trips: what `emit_campaign` writes,
/// `parse_campaign` reads back (the digests and quota flags survive).
#[test]
fn campaign_json_round_trips() {
    let recs: Vec<_> = (0..3u64)
        .chain([7u64, 9, 13])
        .map(|s| workload::run_seed(s, None))
        .collect();
    let json = campaign::emit_campaign(&recs);
    let back = campaign::parse_campaign(&json).expect("parse");
    assert_eq!(back.len(), recs.len());
    for (a, b) in recs.iter().zip(back.iter()) {
        assert_eq!(a.seed, b.seed);
        assert_eq!(a.digest, b.digest, "seed {} digest", a.seed);
        assert_eq!(a.class, b.class);
        assert_eq!(a.rounds.len(), b.rounds.len());
        for (x, y) in a.rounds.iter().zip(b.rounds.iter()) {
            assert_eq!(x.fired, y.fired);
            assert_eq!(x.nested, y.nested);
            assert_eq!(x.q4_split_merge, y.q4_split_merge);
            assert_eq!(x.q4sp, y.q4sp, "seed {} round: the Q4 split half", a.seed);
            assert_eq!(x.q4mg, y.q4mg, "seed {} round: the Q4 merge half", a.seed);
            assert_eq!(x.q18_gc_advanced, y.q18_gc_advanced);
            assert_eq!(x.sizing_ok, y.sizing_ok);
        }
    }
    // The mutants JSON round-trips too (an empty sweep's shape).
    let rec = campaign::MutantRec {
        mutant: "CrcSkip".into(),
        directed_test: "d17_integrity",
        directed_encoded: true,
        directed_caught: Some(true),
        first_seed: Some(3),
        how: Some("O-3"),
        oracle_rate: 0.5,
        monitor_rate: 0.25,
        swept: 10,
    };
    let json = campaign::emit_mutants(&[rec]);
    let back = campaign::parse_mutants(&json).expect("parse");
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].mutant, "CrcSkip");
    assert_eq!(back[0].first_seed, Some(3));
    assert_eq!(back[0].how, Some("O-3"));
    assert_eq!(back[0].directed_caught, Some(true));
}

/// The regression pin for the round-setup barrier (the seed-3
/// development failure, with a literal config, not a seed number): a
/// chunked transaction racing the setup commit — without the barrier,
/// the chunk task's records reach the WAL **below** the setup's
/// `CreateTree`, and LSN-ordered replay applies the marker's Puts
/// against a directory without the tree ("tree 2 missing in directory",
/// a task error); with `WaitTrees`, the chunked task waits and the
/// round's recovery is whole.
#[test]
fn setup_barrier_guards_the_wal_order() {
    use e4_recovery::harness::{self, MediaDraw, RoundFaults, Script, ScriptOp, ScriptTask};
    use e4_recovery::model::Model;
    use e4_recovery::sim::FaultClass;

    // The hazard, demonstrated on the exact configuration that found it
    // (the generated round-0 script of seed 3, the barrier stripped):
    // the chunk task can win the enqueue race, and LSN-ordered replay
    // applies the marker's Puts against a directory without the tree
    // ("tree 2 missing in directory" through the recovery path). With
    // the barrier in force — the generator's own output — the same seed
    // recovers whole. (The barrier's presence is the literal variable;
    // the generated script is reproduced byte-identically by
    // `build_round`, so the pin does not depend on any code path beyond
    // it.)
    let knobs = Knobs::derive(3, 0, &workload::Tuning::BASE);
    let cfg3 = knobs.store_cfg(None);
    let (script_b, _, _) =
        workload::build_round(3, 0, &workload::Tuning::BASE, &knobs, &Model::new(), 0);
    let strip = |s: &Script| Script {
        tasks: s
            .tasks
            .iter()
            .map(|t| ScriptTask {
                ops: t
                    .ops
                    .iter()
                    .filter(|o| !matches!(o, ScriptOp::WaitTrees { .. }))
                    .cloned()
                    .collect(),
            })
            .collect(),
        writer: s.writer,
        evictor: s.evictor,
        checkpointer_every: s.checkpointer_every,
        crash: s.crash.clone(),
        nested: Vec::new(),
    };
    let faults3 = RoundFaults {
        class: FaultClass::C,
        knobs: knobs.fault,
        directed: Vec::new(),
        no_r_taint: false,
        media: MediaDraw::None,
        twin_supply: None,
        nested_draw: 0,
        probe_pass: false,
    };
    let run3 = |s: &Script| {
        let (disk, st) = harness::create_store(cfg3.clone(), 3, 3).expect("create");
        harness::run_round(
            3,
            0,
            s,
            &faults3,
            cfg3.clone(),
            disk,
            st,
            &Model::new(),
            0,
            None,
            None,
        )
    };
    let out_b = run3(&script_b);
    assert!(
        out_b.pass && out_b.o1.is_ok() && out_b.task_error.is_none(),
        "the barriered generator output must pass seed 3 (te={:?}, o1={:?}, o3={:?})",
        out_b.task_error,
        out_b.o1,
        out_b.o3_fail
    );
    let out_nb = run3(&strip(&script_b));
    assert!(
        !out_nb.pass,
        "the barrierless shape must fail visibly (te={:?}, o1={:?}, o3={:?})",
        out_nb.task_error, out_nb.o1, out_nb.o3_fail
    );
}

/// The directed-arms table names every mutant of the enum (the mutation
/// table's directed column), and the encoded arms still catch.
#[test]
fn directed_arms_table_is_complete() {
    use e4_recovery::store::Mutant;
    let all = [
        Mutant::CrcSkip,
        Mutant::RefCrcSkip,
        Mutant::EpochSkip,
        Mutant::IdSkip,
        Mutant::EpochSkipRefCrc,
        Mutant::IdSkipRefCrc,
        Mutant::FreeEarly,
        Mutant::BitmapOmitsFreesetPages,
        Mutant::MetaBeforeSync,
        Mutant::FlipNoGate,
        Mutant::WalSyncTOnly,
        Mutant::WriteGuardNoCopy,
        Mutant::EvictSkipImageMap,
        Mutant::StartLsnPastChunk,
        Mutant::StartLsnMarkedChunk,
        Mutant::TsAfterAppend,
        Mutant::LsnCheckSkip,
        Mutant::PublishBeforeMeta,
        Mutant::GcwNotRecorded,
        Mutant::GcwBoundSkip,
        Mutant::ReplayDupConflict,
        Mutant::DupDifferentAccepted,
        Mutant::InplaceEvict,
        Mutant::DropRootFirst,
        Mutant::RenameNoDirsync,
        Mutant::TruncateLogicalOnly,
        Mutant::StartSegmentMissingOk,
        Mutant::MetaWrongSlot,
    ];
    for m in all {
        assert!(
            campaign::directed_arms::TABLE
                .iter()
                .any(|(mm, _)| *mm == m),
            "{m:?} missing from the directed-arms table"
        );
    }
    // The two encoded arms catch their mutants (X-5b's D2 and D17
    // first-half shapes).
    let d2 = campaign::directed_arms::run_arm(Mutant::StartLsnMarkedChunk);
    assert_eq!(d2, Some(true), "D2's arm must catch START-LSN-MARKED-CHUNK");
    let d17 = campaign::directed_arms::run_arm(Mutant::CrcSkip);
    assert_eq!(d17, Some(true), "D17's arm must catch CRC-SKIP");
}

// ---- X5-R15 (P ruling nv3-X-5c-esc-3): the pass-fold line and verify's
// always-report-all-three exit ----

/// **X5-R15 (a) pin: the `E4 FOLD` line, from a literal 2-seed record
/// set.** Seed A is fully passing but carries a round with O-1 text
/// that **passes** the fold (the recovery-`Err` default O-1(a) text —
/// the 1,315 d2c8bba7 MP rounds the Work 7 line's `O1_FAIL` wrongly
/// counted as failures); it must contribute nothing. Seed B fails one
/// round of the fold, one twin round, and carries a seed-level defect.
/// The counts come from the pass fold and the seed-level
/// `defect`/`deadlock`/`panic`/`baton` flags, never from O-1 text
/// presence (validated against d2c8bba7: 1,598 failing seeds, 100
/// failing twin rounds, 150 defect seeds). A fold that counts O-1 text
/// presence (the reverted form) fails this test on seed A.
#[test]
fn r15a_fold_line_from_a_literal_2_seed_record_set() {
    use e4_recovery::workload::{RoundRec, SeedRec, TwinRound};
    let passing_round_with_o1_text = RoundRec {
        // The shape of d2c8bba7's recovery-Err MP rounds: O-1 text
        // present, round passing under the frozen fold.
        o1_fail: Some("published {} != model {2, 3}".to_owned()),
        pass: true,
        ..RoundRec::default()
    };
    let a = SeedRec {
        seed: 0,
        class: "MP".to_owned(),
        knobs: 1,
        rounds: vec![
            passing_round_with_o1_text,
            RoundRec {
                pass: true,
                ..RoundRec::default()
            },
        ],
        twin: vec![TwinRound {
            pass: true,
            o1_fail: None,
        }],
        ..SeedRec::default()
    };
    let b = SeedRec {
        seed: 1,
        class: "MP".to_owned(),
        knobs: 2,
        rounds: vec![
            RoundRec {
                pass: true,
                ..RoundRec::default()
            },
            RoundRec {
                pass: false,
                ..RoundRec::default()
            },
        ],
        twin: vec![TwinRound {
            pass: false,
            o1_fail: Some("scan mismatch".to_owned()),
        }],
        defect: Some("twin round 0: the crash spec never fired".to_owned()),
        ..SeedRec::default()
    };
    assert_eq!(
        campaign::fold_line(&[a.clone(), b.clone()]),
        "E4 FOLD FAILING_SEEDS=1 FAILING_ROUNDS=1 TWIN_FAILING_ROUNDS=1 DEFECT_SEEDS=1",
        "the fold counts pass-fold failures and seed-level flags, never O-1 text presence"
    );
    // And the Work 7 line keeps its own form (unchanged by X5-R15).
    assert!(campaign::final_line(&[a.clone(), b.clone()]).starts_with("E4 SEEDS=2 "));
}

/// **X5-R17 pin: the SIZING quota row counts seeds, not rounds** (the
/// ruling's own words: "`verify`'s SIZING row counts seeds"). Work 5.3
/// asserts per seed — "every seed must reach, in at least one
/// checkpoint interval, ≥ 4·F distinct dirtied LPIDs" — so a seed whose
/// round 0 missed the bound but whose round 1 reached it is **met**;
/// only a seed no round of which reached it counts. The literal set:
/// seed A misses r0 and reaches it in r1 (a seed hit); seed B misses
/// both rounds (a seed miss). The row must read `achieved 1` — the
/// rounds-based counting this pin reverts to (X-5c-F4 step 1's mutant)
/// reads 3 and fails here.
#[test]
fn x5r17_sizing_row_counts_seeds_not_rounds() {
    use e4_recovery::workload::{RoundRec, SeedRec};
    let seed_hit_in_r1 = SeedRec {
        seed: 0,
        class: "C".to_owned(),
        knobs: 1,
        rounds: vec![
            RoundRec {
                sizing_ok: false,
                ..RoundRec::default()
            },
            RoundRec {
                sizing_ok: true,
                ..RoundRec::default()
            },
        ],
        ..SeedRec::default()
    };
    let seed_all_miss = SeedRec {
        seed: 1,
        class: "C".to_owned(),
        knobs: 2,
        rounds: vec![
            RoundRec {
                sizing_ok: false,
                ..RoundRec::default()
            },
            RoundRec {
                sizing_ok: false,
                ..RoundRec::default()
            },
        ],
        ..SeedRec::default()
    };
    let rows = workload::quotas(&[seed_hit_in_r1, seed_all_miss], 2);
    let sizing = rows
        .iter()
        .find(|r| r.id == "SIZING")
        .expect("the SIZING row exists");
    assert_eq!(
        sizing.achieved, 1,
        "one seed misses (the seed no round of which reached 4F); a rounds-based count gives 3"
    );
    assert_eq!(sizing.required, 0, "SIZING passes only at zero seed misses");
}

/// **X5-R15 (b) pin: `verify` reports all three checks before its exit,
/// and `--no-quotas` does not fail on the quota table.** The exit code
/// is decided once, at the end, from all three outcomes together
/// ([`campaign::verify_exit_code`]): a seed-set failure or a
/// non-reproducing digest always fails; a missed quota fails only
/// without `--no-quotas` (the flag exists for X-5c-F1's Acceptance).
#[test]
fn r15b_verify_reports_all_three_checks_before_failing() {
    // (a) everything clean: exit 0, plain and --no-quotas alike.
    assert_eq!(campaign::verify_exit_code(true, 0, 0, false), 0);
    assert_eq!(campaign::verify_exit_code(true, 0, 5, true), 0);
    // (b) a missed quota: plain verify fails, --no-quotas does not (the
    // table is still printed by the command).
    assert_eq!(campaign::verify_exit_code(true, 0, 5, false), 2);
    // (c) a non-reproducing digest fails both — --no-quotas waives the
    // quota check only, never the digests or the seed set.
    assert_eq!(campaign::verify_exit_code(true, 1, 0, true), 2);
    assert_eq!(campaign::verify_exit_code(true, 1, 5, true), 2);
    // (d) a seed-set failure fails both.
    assert_eq!(campaign::verify_exit_code(false, 0, 0, true), 2);
    // The seed-set predicate: exactly 0..CAMPAIGN_SEEDS-1 in order.
    let good: Vec<e4_recovery::workload::SeedRec> = (0..campaign::CAMPAIGN_SEEDS)
        .map(|s| e4_recovery::workload::SeedRec {
            seed: s,
            ..Default::default()
        })
        .collect();
    assert!(campaign::verify_seed_set_ok(&good));
    let mut hole = good.clone();
    hole.truncate((campaign::CAMPAIGN_SEEDS - 1) as usize);
    assert!(!campaign::verify_seed_set_ok(&hole));
    // The digest report names the non-reproducing seeds.
    let x = e4_recovery::workload::SeedRec {
        seed: 7,
        digest: 1,
        ..Default::default()
    };
    let y = e4_recovery::workload::SeedRec {
        seed: 7,
        digest: 2,
        ..Default::default()
    };
    let (n, list) = campaign::verify_digest_report(&[x], &[y]);
    assert_eq!((n, list), (1, vec![7]));
}

/// **X5-R16 NEVER-FIRED pin (P ruling nv3-X-5c-esc-3; literal config =
/// d2c8bba7's defect seeds).** Seeds 60 and 660 both carried "the crash
/// spec never fired" in the committed campaign (60: the drawn
/// `Mid2@2`'s checkpoint never reached Mid2; 660: round 0's checkpoints
/// reached no Mid2 at all — the tiny pool's evictor wrote every dirty
/// page, so step 2 wrote nothing, P's stated cause "its step 2 has too
/// little to write"). The targeted draw's counting pass (the same probe
/// pattern as the uniform draw's) now records which hooks each
/// checkpoint index reaches (`WorkloadObs::ckpt_hooks_hit`) and the
/// draw lands on a reached (index, hook); every round's crash fires and
/// the seed carries no defect. On the reverted draw both assertions
/// fail (the defect text reappears and `fired` is false).
#[test]
fn r16_never_fired_targeted_draws_reach_their_hook() {
    for seed in [60u64, 660] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        assert!(
            rec.defect.is_none(),
            "seed {seed}: the fired-guard defect is back: {:?}",
            rec.defect
        );
        assert!(
            rec.rounds.iter().all(|r| r.fired),
            "seed {seed}: a round's crash spec never fired: {:?}",
            rec.rounds
                .iter()
                .map(|r| (r.crash.clone(), r.fired))
                .collect::<Vec<_>>()
        );
        // The twin rounds fired too (35 of the 150 defects were twin
        // rounds; the twin shares the real round's spec).
        assert!(
            !rec.rounds.iter().any(|r| r.crash.ends_with("never fired")),
            "seed {seed}: a stale never-fired description"
        );
    }
}

/// **X5-R16 NO-TWIN pin (P ruling nv3-X-5c-esc-3; literal config =
/// d2c8bba7's no-twin seeds).** Seed 1267's committed round carried the
/// oracle's own text "MP/MW round with XX001 and no twin (harness
/// defect)": its twin round's recovery returned `Err`, no supply was
/// built and the twin chain broke — the real round then had no O-3 (c)
/// sets. The harness now supplies the twin for every MP/MW round: a
/// failed twin still yields its recovery reads (collected on both
/// arms) and its crashed-disk image slots, and a broken twin chain
/// re-seeds from the real round's own start (Work 2.5's twin: the same
/// script, crash and F-CRASH resolutions, faultless). On the reverted
/// supply the "no twin" text reappears.
#[test]
fn r16_no_twin_is_always_supplied() {
    for seed in [1267u64, 1769] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        let no_twin = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("no twin"));
        assert!(
            !no_twin,
            "seed {seed}: the missing-twin harness defect is back (O-3 (c) never fails open)"
        );
    }
}

/// **X5-R16 MP-E pin — the completeness half (P ruling nv3-X-5c-esc-3;
/// literal config = d2c8bba7's completeness seeds 47/118/198, round 0
/// each).** All three carried "O-3(e): tainted referenced slot 0 not
/// reported": slot 0 is the §2.1 **file header**, which §3.3's
/// reference model has no reference for, so amcheck (i) can neither
/// check nor report it — yet the harness's referenced set hardcoded
/// it, making every media fault on its range an unreportable "tainted
/// referenced slot". The referenced set is the image's own slots (the
/// map's, the free set's, the metas'); the header stays covered by
/// open's own read (a corrupt header fails open with XX001 at `Open`).
/// On the reverted `used.insert(0)` the completeness text reappears.
/// (The exactness half of MP-E is escalated with its evidence — see
/// the README's stage record: seed 37's slot-11 finding is C-XX's
/// mechanism inside a Q14 rollback, not an instrumentation defect.)
#[test]
fn r16_mpe_referenced_set_excludes_the_file_header() {
    for seed in [47u64, 118, 198] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        let not_reported = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("not reported"));
        assert!(
            !not_reported,
            "seed {seed}: the header-slot completeness defect is back"
        );
    }
}

/// **X5-R20 shape A pin (P ruling nv3-X-5c-F1-esc-1; literal config =
/// bc4f3bba's shape-A seeds 67/268, round 0 each).** Both carried
/// "O-3(e): amcheck reported slot 1 that is neither tainted nor in R"
/// with `mrb=false`, `rlen=0` and the recovery-`Err` default O-1 text:
/// a Q14 page fault (seed 67: LOST on the create's meta k=0 write at
/// slot 1 — the only meta write ever issued; seed 268: two MISDIRECTED
/// faults with the same effect) left **no** valid meta anywhere, so
/// open failed XX001, `durable_image` took its `NoValid` arm, and that
/// arm's finding hardcoded `slot: Some(1)` — which the frozen oracle's
/// exactness half (not gated on `amcheck_i_ran`) judged against empty
/// taint and R sets. X5-R5: with both metas invalid there is no opened
/// image and O-3 (e) is **vacuous**; the finding is structural now (no
/// slot, like the `MisplacedNewer` arm), and the damage stays detected
/// at `Open` (XX001, never served). On the reverted `slot: Some(1)`
/// the exactness text reappears and the rounds fail.
/// **X5-R20 shape B pin (P ruling nv3-X-5c-F1-esc-1; literal config =
/// bc4f3bba's shape-B seeds 38/137, round 0 each).** Both carried
/// "O-3(e): amcheck reported slot 4 that is neither tainted nor in R"
/// with `mrb=true`, `rlen=0`: a Q14 fault tore/misdirected away the
/// newest meta (k=1 at slot 2), open rolled back to the create's
/// k=0 at slot 1, and the checkpoint after k=1's graduation had
/// **reused** k=0's freed map-root slot 4 — ten issued writes after
/// the graduation sync (seed 38: ev[132..287], epoch-2 map leaves),
/// none sync-covered, the crash keeping the last (an id-6 epoch-2
/// page). The bytes equal the last issued (untainted by 1.4), the
/// rolled-back image references the slot with its stale create-era
/// ref CRC (§3.3 check 2 fails) — X5-R5's rollback-reuse narrative
/// word for word, and R5's condition-3 text excuses it ("SimDisk
/// records a page-file write to `s` **issued** after the first
/// completed `sync_all` that covered a meta newer than the opened
/// one"). The draft's `exception_set_r` consulted only the
/// sync-covered durable-write list and missed every unsynced reuse;
/// it now also consults the issued-write event stream (the same
/// record R5's M-NOINPLACE guard reads). On the reverted event source
/// `rlen` is 0 again and the exactness text reappears.
#[test]
fn r20_mpe_r_reads_the_issued_write_history() {
    // X-5c-F3-F1 step 1 note: the WAL-serialization fix moved the
    // uniform crash points, so the pin's literal configs were
    // re-derived at the new tip (38/137 lost their rollback round;
    // 48 r0 and 308 r0 carry the same shape-B rollback with |R| ≥ 1
    // and every round passing). The assertions are unchanged, and the
    // events-half revert was re-demonstrated on the new configs.
    for seed in [48u64, 308] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        let exactness = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("neither tainted nor in R"));
        assert!(
            !exactness,
            "seed {seed}: the unsynced-rollback-reuse slot left R again (condition 3 must read the issued-write events)"
        );
        // The rollback rounds now carry a non-empty R (|R| ≥ 1) —
        // the reused slot itself — and no O-3 clause fails.
        assert!(
            rec.rounds.iter().any(|r| r.meta_rollback && r.r_len >= 1),
            "seed {seed}: the shape-B rollback round lost its R set",
        );
        assert!(
            rec.rounds.iter().all(|r| r.pass),
            "seed {seed}: a round fails beyond the removed defect: {:?}",
            rec.rounds
                .iter()
                .map(|r| (&r.crash, &r.o3_fail, r.pass))
                .collect::<Vec<_>>()
        );
    }
}

/// **X5-R21 (c) pin (P ruling nv3-X-5c-F1-esc-1; literal config = the
/// ruling's seeds 2669/4049/4309/9809 plus 9469 and 7469 — targeted MW
/// seeds that walk the retry and re-pair arms' edges).** The draft's
/// real-round retry re-derived the spec and re-ran alone, keeping the
/// twin's supply and nested points from the twin's run at the OLD
/// spec — a Work 2.5 pairing break (the twin is the same script,
/// crash and F-CRASH resolutions). Now the twin re-runs at the
/// re-derived spec, from its own start, before the real round uses
/// its supply and nested points; the twin record, supply, nested
/// points and chain continuation come from the re-run.
/// `twin_paired` (diagnostic only, never a digest input) asserts the
/// pairing on every round. On the reverted pairing re-run the flag
/// reads false on the re-derived round.
///
/// **Literal history (verdict X-5c-F4-F1-F1-CXX's finding 2,
/// corrected here per that card's step 4):** the ruling's fifth seed
/// was 7469; CXX's rework moved its schedule so its re-paired twin
/// never fired at the re-derived spec (the NEVER-FIRED defect), and
/// 9469 replaced it. Measured at CXX's tip, only **9469** reaches the
/// re-pair path with a firing twin; **2669** re-derives in the twin's
/// FIRST attempt (the twin's own NEVER-FIRED re-derive, which also
/// moves the real round's spec), and **4049, 4309 and 9809** derive
/// nothing (their drawn specs fire on both chains) — the earlier doc
/// overstated them as re-pair seeds. X5-R48 repairs the re-pair arm,
/// and 7469 is back: it reaches the re-pair path with a twin that
/// does NOT fire at the re-derived spec, the joint spec re-pairs both
/// (`r48_repaired_twin_fires_at_the_joint_spec` pins the literal),
/// no defect, every twin paired.
#[test]
fn r21c_the_real_rounds_retry_re_pairs_its_twin() {
    for seed in [2669u64, 4049, 4309, 7469, 9469, 9809] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        assert!(
            rec.defect.is_none(),
            "seed {seed}: the fired-guard defect is back: {:?}",
            rec.defect
        );
        assert_eq!(
            rec.twin_paired.len(),
            rec.rounds.len(),
            "seed {seed}: the pairing flag must cover every executed round"
        );
        assert!(
            rec.twin_paired.iter().all(|p| *p),
            "seed {seed}: a round ran with a twin at a different crash spec (Work 2.5 break)"
        );
    }
}

/// **X5-R48 pin (card X-5c-F4-F1-F1-CXX-F1; literal config = seed 7469,
/// verdict X-5c-F4-F1-F1-CXX's finding 1).** Round 1 of the MW seed
/// 7469: the twin fires at the drawn `Mid2@0`, the real round's first
/// attempt does not (its complete masks read `[3f7, 3ff, 3f7, 3ff,
/// ...]`), so the real round re-derives `Mid2@1` — and the R21 (c)
/// re-pair runs the twin at that spec on a chain that diverged from
/// the real one: the twin's own complete masks read `[3ff, 3f7, 3ff,
/// 3f7, ...]`, its checkpoint 1 never reaches `Mid2`, the crash never
/// fires and the fired-guard fails the seed — at CXX's tip the one
/// defect seed of 0..10000. X5-R48's joint spec (the per-index AND of
/// the two complete mask lists, `[3f7, 3f7, 3f7, 3f7, 3ff, 3ff, 3ff,
/// 3ff]`) re-derives `Mid2@4`, and the twin and the real round both
/// re-run there and fire: no defect, every round paired, round 1's
/// crash of record `Mid2@4` and fired. Under M-R48-REAL-MASKS-ONLY
/// (the second re-derive consulting the real round's masks alone) the
/// joint draw returns `Mid2@1` again, the re-paired twin never fires,
/// and the fired-guard defect returns — this pin fails.
///
/// **X-5c-F4-F1-F1-F3-F2-F1 (X5-R49, step 2), corrected by
/// X-5c-PINS-1 (verdict `74f411f0` finding 4):** round 1's spec moved
/// `Mid2@4 -> Mid2@3`. `Mid2@3` is the **first re-derived spec**, not
/// the joint spec: the re-paired twin fires there, so the joint draw
/// is not entered at this tip (`twin_joint_arm` reads false). Measured:
/// 2 rounds, no defect, round 1 `Mid2@3` fired and passing.
#[test]
fn r48_repaired_twin_fires_at_the_joint_spec() {
    let rec = workload::run_seed_with(7469, None, &workload::Tuning::BASE);
    assert!(
        rec.defect.is_none(),
        "seed 7469: the re-paired twin's fired-guard defect is back: {:?}",
        rec.defect
    );
    assert_eq!(
        rec.twin_paired.len(),
        rec.rounds.len(),
        "seed 7469: the pairing flag must cover every executed round"
    );
    assert!(
        rec.twin_paired.iter().all(|p| *p),
        "seed 7469: a round ran with a twin at a different crash spec (Work 2.5 break)"
    );
    assert!(
        rec.rounds.len() >= 2,
        "seed 7469: the literal shape needs its second round"
    );
    assert_eq!(
        rec.rounds[1].crash, "Mid2@3",
        "seed 7469: round 1 must run the first re-derived spec (Mid2@3), not the joint draw"
    );
    assert!(
        rec.rounds[1].fired,
        "seed 7469: round 1's re-derived-spec crash must fire"
    );
}

/// **X-5c-PINS-1 item 2 (verdict X-5c-F4-F1-F1-CXX-F1's follow-up (a),
/// finding 2): the joint arm's supply-freshness diagnostic.** The R21
/// (c) re-pair block's joint arm re-runs the twin at the joint spec
/// with the caller's collector `treads`; `run_round` extends it, so
/// `treads.clear()` before the re-run is what keeps the re-paired
/// twin's unfired attempt's recovery reads out of the supply the real
/// round's oracle then reads (`supply2`'s `recovery_reads`). The
/// diagnostic fields `twin_joint_arm`/`twin_joint_stale_reads` (the
/// `twin_paired` manner: never a digest input, `SeedRec::finish`
/// never reads them, never serialized) record whether the arm ran and
/// the stale-read count at its re-run's start.
///
/// **At this tip (`2e8d1b13`) the joint arm itself has no live seed,
/// and the pin records that.** At the CXX-F1 tip the arm ran at seed
/// 7469 alone of 10000 (verdict `5db63057`); X5-R49's slot renumbering
/// and the twin directory re-seed have since moved the schedules, and
/// 7469's re-paired twin now fires at the first re-derived spec
/// (`Mid2@3`) without the joint draw — measured: a 0..1000 census
/// plus the re-pair family (2669, 4049, 4309, 7469, 8469, 8869, 9469,
/// 9809) all read `twin_joint_arm` all-false (the census log is in the
/// README's step 2 record). The pin therefore holds what is true: one
/// diagnostic entry per executed round, a fresh supply (zero stale
/// reads) at the named seed, and the arm's absence asserted as the
/// tripwire — the day it fails, the joint arm is live again at 7469
/// and **G5-R48-TREADS-KEPT** (drop the `clear()`) becomes catchable
/// by the freshness assertion. Until then that mutant has no
/// must-fail case (P ruling G5-CENSUS; no seed of 0..9999 reaches the
/// clear, verdict `74f411f0`). A failure of the arm assertion is a
/// STOP for P, never an edit.
#[test]
fn r48_joint_supply_is_fresh_at_7469() {
    let rec = workload::run_seed_with(7469, None, &workload::Tuning::BASE);
    assert_eq!(
        rec.twin_joint_arm.len(),
        rec.rounds.len(),
        "seed 7469: the joint-arm diagnostic must cover every executed round"
    );
    assert!(
        rec.twin_joint_stale_reads.iter().all(|n| *n == 0),
        "seed 7469: the joint arm's supply must be fresh — zero stale reads at the \
         re-run's start (G5-R48-TREADS-KEPT reverts this): {:?}",
        rec.twin_joint_stale_reads
    );
    assert!(
        !rec.twin_joint_arm.iter().any(|ran| *ran),
        "seed 7469: at this tip no round reaches the joint arm (measured; the README's \
         step 2 census) — if this fails, the arm is live again and the freshness \
         assertion above is what G5-R48-TREADS-KEPT must fail"
    );
}

/// **X-5c-F2 step 1 (a) pin: an empty or all-zero hook-mask list is a
/// loud defect, never a hang.** The draft's `reachable_hook_spec`
/// fallback loop walks `CKPT_HOOKS` decrementing `left`, and with
/// `union == 0` no candidate ever matches, so it spun forever — in the
/// exact path that exists to repair an unfired round. The function now
/// returns `Err` (the caller records a seed-level defect beside the
/// fired-guard's). The call runs on a spawned thread with a 5 s channel
/// timeout (the card's test-only wall-clock exception): on the revert
/// the timeout is what fails the pin.
#[test]
fn f1f2a_empty_masks_are_a_defect_not_a_hang() {
    use std::sync::mpsc;
    use std::time::Duration;
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        use e4_recovery::store::ckpt::CkptHook;
        let empty = workload::reachable_hook_spec(&[], CkptHook::Mid2, 0, 7, 0);
        let zeros = workload::reachable_hook_spec(&[0, 0], CkptHook::After2, 1, 7, 0);
        let _ = tx.send((empty.is_err(), zeros.is_err()));
    });
    let got = rx.recv_timeout(Duration::from_secs(5));
    assert!(
        got.unwrap_or((false, false)) == (true, true),
        "reachable_hook_spec must refuse (&[] and &[0, 0]) within 5 s, not hang or loop"
    );
    handle.join().expect("the spec thread must finish");
}

/// **X-5c-F2 step 1 (b) pin: the R21 (c) re-pair update.** A re-paired
/// twin whose recovery returns no store re-seeds the twin chain
/// (`twin_chain = None` — the next round's supply block rebuilds it
/// from the real round's own start); a re-paired twin with a store
/// continues the chain from the re-paired round's own end state. The
/// draft kept the FIRST attempt's chain when the re-paired twin's
/// recovery failed — a chain at a crash spec the round that ran never
/// used. On the revert (`twin_chain_after_repair` restoring the first
/// attempt's chain, i.e. keeping the incoming chain on the `None`
/// arm) this pin fails.
#[test]
fn f1f2b_repairs_without_a_store_reseed_the_twin_chain() {
    use e4_recovery::harness::RoundOutcome;
    use e4_recovery::store::StoreInner;
    let cfg = Knobs::derive(7, 0, &workload::Tuning::BASE).store_cfg(None);
    let tout = RoundOutcome {
        cut: 3,
        end_store: None,
        ..RoundOutcome::default()
    };
    assert!(
        workload::twin_chain_after_repair(&tout).is_none(),
        "a re-paired twin whose recovery returned no store must re-seed the twin chain"
    );
    let tout = RoundOutcome {
        cut: 3,
        end_store: Some(StoreInner::empty(cfg, 0)),
        ..RoundOutcome::default()
    };
    let ch = workload::twin_chain_after_repair(&tout)
        .expect("a re-paired twin with a store continues the chain");
    assert_eq!(ch.cut, 3, "the chain carries the re-paired round's own cut");
}

/// **X5-R28 (e) pin (literal configs = the ruling's residual seeds 1787
/// and 6157, round 0 each).** The rounds that failed O-3 (e) as
/// "tainted referenced slot 10/42 not reported" now pass: their named
/// slot is in the kept set `K` (the diag prints `K slot … durable==V_k`
/// per round), the rollback conditions hold (`mrb=true`), and no O-3
/// text remains. That the clear comes **through K** is demonstrated on
/// the revert: with the harness K computation disabled (always empty —
/// mutant M-R28g) the completeness text returns and this pin fails.
#[test]
fn r28_residual_seeds_clear_via_k() {
    // X-5c-F3-F1 step 1 note: 6157's round moved with the WAL fix (no
    // rollback in it now; its twin fails O-1(c) — the inherited C-DIV
    // echo). 5808 r0 carries the same residual shape at the new tip
    // (K slot 53), so the pin's literal configs are [1787, 5808]; the
    // assertions are unchanged and the M-R28g revert was
    // re-demonstrated on both.
    for seed in [1787u64, 5808] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        assert!(
            rec.rounds.iter().any(|r| r.meta_rollback),
            "seed {seed}: the residual round's rollback condition must hold (the K gate)"
        );
        for r in &rec.rounds {
            assert!(
                r.o3_fail.is_none(),
                "seed {seed}: O-3 must be clear (the named slot is kept): {:?}",
                r.o3_fail
            );
            assert!(
                r.pass,
                "seed {seed}: the residual round must pass: {:?}",
                r.o3_fail
            );
        }
    }
}

/// **X-5c-F3 pin, class C-XX (literal configs = the class's repro seeds
/// 221 and 66, round 0 each, plus the MP-shape repro 907).** Freeing
/// the highest LPID never shrinks `next_lpid` (S-STORE §4.2/§4.3: a
/// freed LPID goes to the free set; the counter is monotone, and §4.3's
/// LPID bitmap is defined over `[1, next_lpid)` because of it). The
/// draft's decrement let a flip-window free re-issue an LPID the image
/// being built still maps, so the frozen radix reached `next_lpid`
/// itself: amcheck (i) reported "next_lpid not above every mapped lpid"
/// in faultless class C rounds (O-2 + O-3 (b), seeds 221/66), and a
/// recovery that re-freed the LPID hit "lpid N unmapped" (an XX001 at
/// Open with no fault at all, seed 221). 907 is the same defect seen
/// from class MP (O-3 (c): the unexplained XX001 beside faults that
/// target unread, unreferenced ranges). That the clear comes **through
/// the monotone counter** is demonstrated on the revert: with the
/// `next_lpid -= 1` branch restored (mutant M-CXXa) each seed fails
/// again with exactly its class text.
#[test]
fn cxx_highest_lpid_free_never_shrinks_next_lpid_rounds() {
    for seed in [221u64, 66, 907] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        for r in &rec.rounds {
            assert!(
                r.o2_fail.is_none() && r.o3_fail.is_none(),
                "seed {seed}: the C-XX defect is back: O-2 {:?} / O-3 {:?}",
                r.o2_fail,
                r.o3_fail
            );
            assert!(
                r.pass,
                "seed {seed}: a round fails beyond the removed defect: {:?}",
                r.o3_fail
            );
        }
    }
}

/// **X-5c-F3 pin, class C-XX's G3 family (literal configs = the MW
/// shape's repro 1769, round 0, and its faultless twin).** The nested
/// recovery crash that landed on open's G3 probe yield was swallowed
/// into `Corrupt("start segment … missing (G3)")`: the faultless twin
/// (judged as class C) failed `O-1(a)` through the recovery `Err`, and
/// the media round failed `O-3 (c)` because every fault targeted slots
/// the crashed twin never read or referenced — with `nested=0` recorded
/// (the crash never fired). With the crash propagating (the `exists`
/// probe), the retry resolves F-CRASH, recovery completes, the twin
/// passes and the O-3 (c) twin-rule text is gone. The round's own
/// remaining failure is its MW-ORPHAN shape (X5-R16's SPEC class,
/// evidence-only: `O-1 (c) raw entry above the cut` with `O-3 (d)`),
/// which this pin deliberately does not judge. Reverting the fix
/// (mutant M-CXXb) fails this pin with exactly the twin's `O-1(a)`
/// text and the round's O-3 (c).
#[test]
fn cxx_g3_probe_never_swallows_a_recovery_crash() {
    let rec = workload::run_seed_with(1769, None, &workload::Tuning::BASE);
    for t in &rec.twin {
        assert!(
            t.pass,
            "the faultless twin's recovery must not fail: {:?}",
            t.o1_fail
        );
    }
    for r in &rec.rounds {
        let o3 = r.o3_fail.as_deref().unwrap_or("");
        assert!(
            !o3.contains("unread, unreferenced targets"),
            "seed 1769: the O-3 (c) twin-rule failure is back: {:?}",
            r.o3_fail
        );
    }
}

#[test]
fn r20_mpe_no_opened_image_means_no_slotted_finding() {
    for seed in [67u64, 268] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        let exactness = rec
            .rounds
            .iter()
            .filter_map(|r| r.o3_fail.as_ref())
            .any(|t| t.contains("neither tainted nor in R"));
        assert!(
            !exactness,
            "seed {seed}: the no-opened-image slotted finding is back (O-3 (e) must be vacuous per X5-R5)"
        );
        // The rounds now pass: the XX001 at `Open` is a detection in
        // MP (the plan's clause 3), and no O-3 clause fails.
        assert!(
            rec.rounds.iter().all(|r| r.pass),
            "seed {seed}: a round fails beyond the removed defect: {:?}",
            rec.rounds
                .iter()
                .map(|r| (&r.crash, &r.o3_fail, r.pass))
                .collect::<Vec<_>>()
        );
    }
}

/// **X-5c-F3 pin, G8 (X5-R16's conditional fill; literal configs = the
/// MP-LIVELOCK class's repro seeds 987 and 1108, round 1 each).** P's
/// reading of I-FAIL-STOP (§9.2): a read that returns bytes failing
/// §3.3 is a failed physical read, and inside a gated operation —
/// apply, a split, a pack, GC — it poisons the store. The prototype
/// poisoned only on `gc_w > T` at the meta write, so a round whose
/// image held an inherited unreadable page (every MP-LIVELOCK round
/// sits directly after an R6 detected-loss round) let the failing task
/// unwind as a client detection while the rest parked forever behind
/// the service spins: the round livelocked at `h:writer`/`h:evictor`/
/// `h:ckpt` and failed through the DEADLOCK fold. With G8 the poison
/// ends the workload at once: the crash is delivered at the poison
/// point, the crash desc is recorded as `poison` (never a NEVER-FIRED
/// — `crash_fired` follows the world's `crashed` flag), no livelock
/// trips, and the faults, recovery and ruled oracle run as usual (the
/// round passes as the detection round it is). Reverting the store
/// half (mutant M-G8a, "XX001 in apply does not poison": the poison
/// mark removed from `fetch_page`) fails this pin — seed 987 round 1
/// livelocks again (`dl=true`, `ll=true`); reverting the harness half
/// (M-G8b: the `Fail::Poisoned` arm removed from `run_task`) fails it
/// the same way (the poison is set but the workload never ends).
#[test]
fn g8_poison_ends_the_workload_instead_of_livelocking() {
    // X-5c-F3-F1 step 1 note: 1108's round 0 flipped to a recovery-Err
    // detection round at the new tip (its chain stops; no r1 exists),
    // so the pin's second literal config was re-derived: seed 7 round 1
    // was the same MP-LIVELOCK shape (directly after an R6 round 0, the
    // crash desc `poison`, no livelock, the round passes). The
    // assertions are unchanged; M-G8a was re-demonstrated on both
    // configs (987 r1 and 7 r1 livelock again with the poison mark
    // removed).
    // X-5c-F3-F3a step 3 note: the store changes moved the workloads
    // again — seed 7's chain now stops after its passing r0 (no r1
    // exists; it fails only at the `rounds[1]` shape precondition) —
    // so the second config was re-pointed at seed 387 round 1 (the
    // same shape: r0 an R6 detection round, r1 the `poison` crash,
    // passing, no livelock), assertions byte-identical. 987 r1 still
    // holds.
    for seed in [987u64, 387] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        let r1 = &rec.rounds[1];
        assert!(
            !r1.deadlock && !r1.livelock,
            "seed {seed} round 1: the MP-LIVELOCK is back (site {:?})",
            r1.livelock_site.as_str()
        );
        assert!(
            r1.crash == "poison",
            "seed {seed} round 1: the crash desc must record the poison point: {:?}",
            r1.crash
        );
        assert!(
            r1.fired,
            "seed {seed} round 1: the poison delivery is the round's crash (never a NEVER-FIRED)"
        );
        assert!(
            r1.pass,
            "seed {seed} round 1: the round must pass as the detection round it is: o1={:?} o2={:?} o3={:?}",
            r1.o1_fail, r1.o2_fail, r1.o3_fail
        );
        assert!(rec.defect.is_none(), "seed {seed}: no harness defect");
    }
}

/// **X-5c-F4 step 4 pin (the new MP poison+task-error class; literal
/// configs = the class's repro seeds 747 and 967, round 1 each).** The
/// class: G8's harness contract ends a poisoned workload at the poison
/// crash ("the faults, the recovery and the oracle run as usual"), but
/// `checkpointer_body` fell through to a final checkpoint whose first
/// step refused on `alive()` with `Fail::Poisoned`, and its error arm
/// recorded that designed refusal as a round-failing task error
/// ("checkpoint failed: Poisoned("XX001 in gated op: …")") beside the
/// poison crash — 17 MP rounds in X-5c-F4's step-0 file (zero in
/// F3-F3a's), deterministically, whenever the checkpointer's poll
/// landed between the mark and the poisoner's own crash delivery. The
/// fix (`checkpoint_poison_end`) gives the checkpointer the script
/// task's arm: no step after the mark, the crash delivered at once,
/// the round judged as the detection round it is. Reverting the fix
/// (mutant M-CKPOISONa: the `h:ck` poison arm falls through to the
/// checkpoint again) fails this pin — seed 747 round 1 records
/// `terr=true` and the round fails. A second revert arm (M-CKPOISONb:
/// the mid-checkpoint `Fail::Poisoned` case removed) fails the same
/// assertions whenever the mark lands mid-checkpoint.
///
/// **X-5c-F4-F1-F1-F3-F2-F1 (X5-R49, step 2; P's re-selection rule,
/// fixed with X5-R51):** 967 r1 no longer exists — X5-R49 moved
/// checkpoint 1's `map_root` from 11 to 10 when a freed slot was
/// reused, so the same `Mid6@0` draw now fails the open in round 0 and
/// the seed stops at one round. The replacement is **47**, the lowest
/// seed in 0..1000 other than 747 whose BASE round 1 passes every
/// assertion of this pin at the tip with the body unchanged (scanned
/// over the tip's 0..1000 run: the qualifying seeds are 47, 67, 387,
/// 527, 567, 747, 767, 788, 987); 47's r0 now ends in an R6 detection
/// round and its r1 carries the `poison` crash, passing. 747 is not
/// re-selected and still passes.
#[test]
fn g8_checkpoint_poison_ends_the_workload() {
    for seed in [747u64, 47] {
        let rec = workload::run_seed_with(seed, None, &workload::Tuning::BASE);
        let r1 = &rec.rounds[1];
        assert!(
            r1.crash == "poison",
            "seed {seed} round 1: the crash desc must record the poison point: {:?}",
            r1.crash
        );
        assert!(
            r1.fired,
            "seed {seed} round 1: the poison delivery is the round's crash (never a NEVER-FIRED)"
        );
        assert!(
            r1.task_error.is_none(),
            "seed {seed} round 1: the store's designed post-mark refusal is never a harness \
             task error: {:?}",
            r1.task_error
        );
        assert!(
            !r1.deadlock && !r1.livelock,
            "seed {seed} round 1: the workload ends at the poison, no livelock (site {:?})",
            r1.livelock_site.as_str()
        );
        assert!(
            r1.pass,
            "seed {seed} round 1: the round must pass as the detection round it is: \
             o1={:?} o2={:?} o3={:?}",
            r1.o1_fail, r1.o2_fail, r1.o3_fail
        );
        assert!(rec.defect.is_none(), "seed {seed}: no harness defect");
    }
}

// ---- X5-R41/X5-R42/X5-R43 (card X-5c-F4-F1-F1-F3 steps 3, 6, 7) ----
// The literal settings below are probe-found (single seeds at the share
// 1000 tuning `Tuning::BASE` + `r41_share` 1000, the first probe share;
// no setting met the step-5 rule, so nothing was handed off — the pins
// pin the machinery, not a chosen setting) and are labelled probe-found,
// never evidence for a quota claim.

/// **X5-R41 (card X-5c-F4-F1-F1-F3 step 3): the draw-count pin.**
/// [`workload::r41_pick`] returns `None` — the `S_R41` stream is never
/// constructed, no stream state consumed (the card's Limit 3) — for
/// every round under `Tuning::BASE` (`r41_share` 0), and draws (returns
/// `Some`) the moment the share is non-zero. The BASE-digest half of
/// the pin is `x5_r37_base_tuning_reproduces_1b0ce54e` (above): a draw
/// made at share 0 whose result reached `S_GEN`'s order would move a
/// BASE digest (M-R41-DRAW reverts this).
#[test]
fn x5_r41_not_drawn_at_base() {
    for seed in 0..64u64 {
        for round in 0..3u32 {
            assert_eq!(
                workload::r41_pick(seed, round, &workload::Tuning::BASE),
                None,
                "seed {seed} round {round}: r41_share 0 must draw nothing (M-R41-DRAW reverts this)"
            );
        }
    }
    // A non-zero share draws (returns Some) for every eligible round,
    // and both outcomes appear at an intermediate share.
    let mut share1 = workload::Tuning::BASE;
    share1.r41_share = 100;
    let (mut somes, mut trues) = (0usize, 0usize);
    for seed in 0..500u64 {
        if let Some(pick) = workload::r41_pick(seed, 0, &share1) {
            somes += 1;
            trues += usize::from(pick);
        }
    }
    assert_eq!(somes, 500, "a non-zero share draws for every round");
    assert!(
        trues > 0 && trues < 500,
        "share 100 permille must both hit and miss over 500 seeds"
    );
    let mut share1000 = workload::Tuning::BASE;
    share1000.r41_share = 1000;
    for seed in 0..8u64 {
        assert_eq!(
            workload::r41_pick(seed, 0, &share1000),
            Some(true),
            "share 1000 permille selects every round"
        );
    }
}

/// **X5-R41 (step 3): the card's named pin — an aligned round with both
/// Q4 halves and a reuse in the crash's own held window.** The literal
/// config is the first probe setting (`r41_share` 1000, everything else
/// BASE) and seed **21 round 0** (crash `After0@1`): the record's
/// `q4_split_merge` is **true** with `q4sp == 4` and `q4mg == 8` exact
/// (verdict finding 3: `≥ 1` on both halves let G-Q4MG-FROM-SPLITS
/// pass; 4 ≠ 8 pins the halves apart) and `q4reuse >= 1`. The seed's
/// digest is pinned with it (a seed pin's identity is (generator,
/// seed), X5-R37). **M-R41-NOHOLD** (the hold returns at once — the
/// crash at `After0` beats the block, the window's counters stay 0),
/// **M-R41-WRONGWINDOW** (the hold and gate key on checkpoint 0, not
/// `idx` — this seed's `idx` is 1, so the block runs in checkpoint 0's
/// window and the crash at checkpoint 1's `After0` reads a fresh
/// window), **M-R41-NOPREP** (the block's Deletes removed — the sweep
/// has nothing to elide, `q4mg` falls to 0) and
/// **M-R41-AFTERCRASHARM** (the hold placed after the crash arm — this
/// seed's crash hook is `After0`, so the crash fires before the hold)
/// each move the digest and leave the exact values wrong.
#[test]
fn x5_r41_aligned_round_has_q4_and_reuse() {
    let mut tun = workload::Tuning::BASE;
    tun.r41_share = 1000;
    let rec = workload::run_seed_with(21, None, &tun);
    assert!(rec.defect.is_none(), "seed 21: {:?}", rec.defect);
    assert_eq!(rec.digest, 1771266996, "seed 21's schedule moved");
    let r0 = rec.rounds.first().expect("seed 21 runs round 0");
    assert_eq!(r0.crash, "After0@1", "round 0's crash point moved");
    assert!(
        r0.fired,
        "round 0's crash must fire (a silent round is a harness defect)"
    );
    assert!(
        r0.q4_split_merge,
        "the aligned round must carry q4_split_merge true"
    );
    assert_eq!(r0.q4sp, 4, "q4sp exact (G-Q4MG-FROM-SPLITS reverts this)");
    assert_eq!(r0.q4mg, 8, "q4mg exact (G-Q4MG-FROM-SPLITS reverts this)");
    assert!(
        r0.q4reuse >= 1,
        "the held window reused an LPID freed since the flip: q4reuse={}",
        r0.q4reuse
    );
    assert!(r0.pass, "the aligned round passes: {:?}", r0.o3_fail);
}

/// **X5-R41/R43 (steps 2/3/7): the completed-window arm.** Seed **142
/// round 1** at `r41_share` 1000 (crash `After5@1`): the armed
/// checkpoint's window ran to its step 5 — `imgsp == 15`, `imgmg == 12`
/// and `imgreuse == 9` exact (the window ledger's last row, the counts
/// of the checkpoint whose meta the round's recovery opened), the round
/// passes, and its `q4i_raw` holds. Under **M-REUSE-DROPS-IMG**
/// (`Mutant::ReuseDropsImg`, the seeded Joint fault) the same round
/// fails the oracle — the fault's drop of `map.img[l]` loses the reused
/// page from the image — so the pin runs both arms: applying the mutant
/// unconditionally breaks the clean arm and fails this pin.
#[test]
fn x5_r41_completed_window_arm() {
    use e4_recovery::store::Mutant;
    let mut tun = workload::Tuning::BASE;
    tun.r41_share = 1000;
    let rec = workload::run_seed_with(142, None, &tun);
    assert!(rec.defect.is_none(), "seed 142: {:?}", rec.defect);
    assert_eq!(rec.digest, 1664460399, "seed 142's schedule moved");
    let r1 = rec.rounds.get(1).expect("seed 142 runs round 1");
    assert_eq!(r1.crash, "After5@1", "round 1's crash point moved");
    assert!(r1.fired, "round 1's crash must fire");
    assert_eq!(r1.imgsp, 15, "imgsp exact");
    assert_eq!(r1.imgmg, 12, "imgmg exact");
    assert_eq!(
        r1.imgreuse, 9,
        "imgreuse exact (M-REUSE-DROPS-IMG reverts this)"
    );
    assert!(r1.q4i_raw, "the opened image is this window's meta");
    assert!(
        r1.pass,
        "the completed-window round passes: {:?}",
        r1.o3_fail
    );
    assert!(
        r1.o1_fail.is_none() && r1.o2_fail.is_none() && r1.o3_fail.is_none(),
        "the clean round passes the oracle outright (the X5-R43 catch shape)"
    );
    // The Joint fault's arm: the same seed under the mutant fails the
    // oracle on this round — asserted through the X5-R43 rule itself
    // (a round failing O-1/O-2/O-3 in the mutant's arm that passed in
    // the clean arm; an MP detection round may still pass the fold,
    // the rule reads the oracle texts).
    let mrec = workload::run_seed_with(142, Some(Mutant::ReuseDropsImg), &tun);
    let m1 = mrec.rounds.get(1).expect("the mutant arm runs round 1");
    assert!(
        m1.o1_fail.is_some() || m1.o2_fail.is_some() || m1.o3_fail.is_some(),
        "M-REUSE-DROPS-IMG must fail the oracle on 142 r1 (the X5-R43 new-failing rule \
         reads exactly these texts): o1={:?} o2={:?} o3={:?}",
        m1.o1_fail,
        m1.o2_fail,
        m1.o3_fail
    );
    assert!(
        !m1.deadlock && !m1.panic,
        "the catch is an oracle failure, never a hang"
    );
}

/// **X5-R42 (step 6): `q4i_raw` keys on the opened image, never on the
/// last step-5 checkpoint.** The pure half pins the lookup's
/// discriminating case (a ledger whose last row holds a reuse and whose
/// opened id's row does not — a meta rollback's shape); the literal
/// half pins seed **148 round 0** at `r41_share` 1000 (class MP, crash
/// `After5@1`): its last completed window held `imgreuse == 5` reuses
/// while its final recovery opened an image whose own window has no
/// ledger row, so `q4i_raw` (and `q4i`) must be **false**.
/// **M-Q4I-LASTCKPT** (`q4i_raw` reads the last step-5 checkpoint)
/// reads `imgreuse ≥ 1` as true and fails both halves.
#[test]
fn x5_r42_q4i_keys_on_the_opened_image() {
    use std::collections::BTreeMap;
    use workload::{Tuning, q4i_raw_of};
    // The discriminating ledger: ids 0..=3, the last row (id 3) holds a
    // reuse, the opened image is id 1 whose row does not. The fourth
    // element is the pre-freeze reuse count (X-5c-F4-F1-F1-F3-F1 step
    // 2a); the assertions are unchanged.
    let mut w: BTreeMap<u64, (u64, u64, u64, u64)> = BTreeMap::new();
    w.insert(0, (0, 0, 0, 0));
    w.insert(1, (2, 2, 0, 0));
    w.insert(2, (9, 9, 9, 9));
    w.insert(3, (15, 12, 9, 9));
    assert!(
        !q4i_raw_of(&w, Some(1)),
        "the opened image's own window (no reuse) decides, not the last row"
    );
    assert!(
        q4i_raw_of(&w, Some(2)),
        "an opened image whose own window held a reuse counts"
    );
    assert!(
        !q4i_raw_of(&w, None),
        "a refused or failed open never counts"
    );
    assert!(
        !q4i_raw_of(&w, Some(9)),
        "the harness checkpoint's id has no row (never recorded)"
    );
    // The literal seed: imgreuse 5, q4i_raw false.
    let mut tun = Tuning::BASE;
    tun.r41_share = 1000;
    let rec = workload::run_seed_with(148, None, &tun);
    assert!(rec.defect.is_none(), "seed 148: {:?}", rec.defect);
    assert_eq!(rec.digest, 139439729, "seed 148's schedule moved");
    let r0 = rec.rounds.first().expect("seed 148 runs round 0");
    assert_eq!(r0.crash, "After5@1");
    assert_eq!(r0.imgreuse, 5, "the last completed window held 5 reuses");
    assert!(
        !r0.q4i_raw,
        "the opened image's own window decides: q4i_raw must be false here \
         (M-Q4I-LASTCKPT reverts this)"
    );
    assert!(!r0.q4i, "q4i follows q4i_raw");
}

/// **X5-R42 (card X-5c-F4-F1-F1-F3-F1 step 2b): a post-freeze reuse is
/// not `q4i`.** The literal seed is **486 round 1** at `r41_share`
/// 1000 (crash `After2@1` — a pre-step-5 targeted crash, the class the
/// image arm reasons about; the recovery opened image id 3, the
/// round's own last completed window, so the record's counts are that
/// window's own): the opened image's window held exactly **1**
/// whole-window reuse (`imgreuse == 1`) and **0** pre-freeze reuses
/// (`imgreuse_pf == 0` — the reuse landed after step 2 froze the
/// table), so `q4i_raw` (and `q4i`) read **false** under the
/// pre-freeze count of X-5c-F4-F1-F1-F3-F1 step 2a.
/// **M-Q4I-WHOLEWINDOW** (`q4i_raw` reads the whole-window count
/// instead) reads the same window's `imgreuse == 1` as true and fails
/// this pin. (Scan-found over 0..1000 at share 1000: four such rounds
/// exist — 190 r2, 486 r1, 792 r2, 93 r2 — all with the opened image
/// the last completed window; 486 r1 is the targeted one.)
#[test]
fn x5_r42_post_freeze_reuse_is_not_q4i() {
    let mut tun = workload::Tuning::BASE;
    tun.r41_share = 1000;
    let rec = workload::run_seed_with(486, None, &tun);
    assert!(rec.defect.is_none(), "seed 486: {:?}", rec.defect);
    assert_eq!(rec.digest, 218615732, "seed 486's schedule moved");
    let r1 = rec.rounds.get(1).expect("seed 486 runs round 1");
    assert_eq!(r1.crash, "After2@1", "round 1's crash point moved");
    assert!(r1.fired, "round 1's crash must fire");
    assert_eq!(
        r1.imgreuse, 1,
        "the opened image's window held exactly 1 whole-window reuse"
    );
    assert_eq!(
        r1.imgreuse_pf, 0,
        "and 0 pre-freeze reuses — the reuse landed after the freeze"
    );
    assert!(
        !r1.q4i_raw,
        "a post-freeze reuse is not q4i: the pre-freeze count decides \
         (M-Q4I-WHOLEWINDOW reverts this)"
    );
    assert!(!r1.q4i, "q4i follows q4i_raw");
}

/// **X5-R41 (card X-5c-F4-F1-F1-F3-F1 step 3a): a uniform-crash round
/// is never selected, however its draw reads.** The literal seed is
/// **10 round 0** (`seed/10` odd, so 5.5 gives it a uniform crash; not
/// a drop round), and at `r41_share` 1000 its `S_R41` draw reads
/// `Some(true)` — below the share, a hit. The selection's
/// targeted-crash clause (`seed / 10` even) still excludes it: the
/// built GC task carries no `R41Arm` and none of the block's ops, in
/// any task. **G-SEL-NOTARGET** (the selection drops the targeted
/// clause) selects this round — the draw already hit — and the arm
/// appears, failing this pin.
#[test]
fn x5_r41_uniform_round_not_selected() {
    use e4_recovery::harness::ScriptOp;
    use e4_recovery::model::Model;
    use workload::{Knobs, Tuning, build_round, r41_pick};
    let mut tun = Tuning::BASE;
    tun.r41_share = 1000;
    let knobs = Knobs::derive(10, 0, &tun);
    assert_eq!(
        10u64 / 10,
        1,
        "seed 10's seed/10 is odd: a uniform-crash seed"
    );
    assert!(!knobs.drop_tree, "round 0 of seed 10 is not a drop round");
    assert_eq!(
        r41_pick(10, 0, &tun),
        Some(true),
        "the S_R41 draw hits at share 1000 — the clause is all that keeps this round out"
    );
    let (script, _desc, hook) = build_round(10, 0, &tun, &knobs, &Model::new(), 0);
    assert!(hook.is_empty(), "a uniform round has no targeted hook name");
    for t in &script.tasks {
        let has_arm = t.ops.iter().any(|op| matches!(op, ScriptOp::R41Arm { .. }));
        let has_block = t
            .ops
            .iter()
            .any(|op| matches!(op, ScriptOp::R41Wait { .. } | ScriptOp::R41Done { .. }));
        assert!(
            !has_arm && !has_block,
            "a task of seed 10's round 0 must carry neither the arm nor the block \
             (G-SEL-NOTARGET reverts this)"
        );
    }
}

/// **X5-R43 (card X-5c-F4-F1-F1-F3-F1 step 3a): "passes in the clean
/// arm" is the oracle, not the fold.** The literal seed is **7 round
/// 0** at BASE (class MP): the clean round **passes the fold**
/// (`pass == true`) while carrying the oracle text `O-1(a) at c=23:
/// published {} != model {2, 3, 4, 5}` — an MP detection round whose
/// failure text is expected. The same round under the Slot fault
/// (`M-FREE-HOME-ALLOC`) also fails the oracle. `seeded_new_failures`
/// must **not** count it: a fault that changes nothing about a round
/// that already carries an oracle text is not caught by it.
/// **G-SEEDED-FOLD** (`passes_oracle = cr.pass`) reads this round's
/// `pass == true` as passing and counts it, failing this pin.
#[test]
fn x5_r43_oracle_pass_not_fold() {
    use e4_recovery::campaign::seeded_new_failures;
    use e4_recovery::store::Mutant;
    use workload::Tuning;
    let clean = workload::run_seed_with(7, None, &Tuning::BASE);
    assert!(clean.defect.is_none(), "seed 7: {:?}", clean.defect);
    assert_eq!(clean.digest, 2267580514, "seed 7's BASE digest moved");
    let cr = clean.rounds.first().expect("seed 7 runs round 0");
    assert!(cr.pass, "the clean round passes the fold");
    assert!(
        cr.o1_fail.is_some() || cr.o2_fail.is_some() || cr.o3_fail.is_some(),
        "the clean round carries an oracle text: o1={:?}",
        cr.o1_fail
    );
    // The same round in the mutant arm fails the oracle too.
    let slot = workload::run_seed_with(7, Some(Mutant::FreeHomeAlloc), &Tuning::BASE);
    let fr = slot.rounds.first().expect("the Slot arm runs round 0");
    assert!(
        fr.o1_fail.is_some() || fr.o2_fail.is_some() || fr.o3_fail.is_some(),
        "the mutant arm's round carries an oracle text: o1={:?}",
        fr.o1_fail
    );
    assert!(
        !fr.deadlock && !fr.panic,
        "the arm's round is an oracle failure, never a hang"
    );
    let nf = seeded_new_failures(std::slice::from_ref(&clean), std::slice::from_ref(&slot));
    assert!(
        !nf.contains(&(7, 0)),
        "a clean round that already carries an oracle text never counts as new, however \
         its fold pass reads (G-SEEDED-FOLD reverts this): {nf:?}"
    );
}

/// **X5-R42 (card X-5c-F4-F1-F1-F3-F1 step 3b): the harness
/// checkpoint's image is not `q4i`.** No 0..1000 round at share 1000
/// or at BASE exercises the divergence (measured on a scratch
/// instrumented export: the harness-checkpoint removal never flips
/// `q4i_raw`), so this is the card's named fallback — **a
/// pure-function ledger case with a literal harness id**. Round m
/// completes its harness checkpoint with id **7**, an id whose stale
/// row (an older checkpoint of the same id, holding a reuse — a meta
/// rollback's shape) is in the ledger; the fold keys round m's
/// `q4i_raw` on its opened image id 3 and then drops id 7. Round m+1's
/// final recovery opens the harness checkpoint's image, id 7: its
/// window was never recorded, and the stale row is gone — `q4i_raw`
/// reads **false**. **G-LEDGER-NOREMOVE** (the harness checkpoint
/// stays in the ledger) keeps the stale row, and round m+1's lookup
/// answers with it — `true` — failing this pin.
#[test]
fn x5_r42_harness_checkpoint_image_is_not_q4i() {
    use std::collections::BTreeMap;
    use workload::fold_ckpt_windows;
    // The ledger holds a stale row for id 7 (an older checkpoint of
    // that id, its window held a whole-window reuse with its pre-freeze
    // half) and a row for the image round m opens.
    let mut w: BTreeMap<u64, (u64, u64, u64, u64)> = BTreeMap::new();
    w.insert(3, (4, 8, 2, 2));
    w.insert(7, (15, 12, 9, 9));
    // Round m: no rows of its own; opened id 3; harness checkpoint id 7.
    let raw_m = fold_ckpt_windows(&mut w, &[], Some(3), Some(7));
    assert!(
        raw_m,
        "round m opens image 3, whose own window held a reuse"
    );
    assert!(
        !w.contains_key(&7),
        "the harness checkpoint's id must leave the ledger — its window is not recorded"
    );
    // Round m+1: its final recovery opens the harness checkpoint's
    // image, id 7. No row answers (the stale row is gone): false.
    let raw_m1 = fold_ckpt_windows(&mut w, &[], Some(7), None);
    assert!(
        !raw_m1,
        "the harness checkpoint's image is not q4i, and the stale row for its id must not \
         answer for it (G-LEDGER-NOREMOVE reverts this)"
    );
    // And a real checkpoint of id 7, recorded by a later round, does
    // count — the removal never leaks to rows that belong there.
    let raw_m2 = fold_ckpt_windows(&mut w, &[(7, 15, 12, 9, 9)], Some(7), None);
    assert!(raw_m2, "a recorded window of the opened image still counts");
}

/// **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5b): nothing is drawn at
/// share 0, and BASE never moves.** `r41p_pick` returns `None` — no
/// `S_R41P` stream constructed, no state consumed — for every round at
/// `r41p_share` 0; a non-zero share draws for every round (both
/// outcomes at an intermediate share; every draw at 1000). The
/// BASE-digest half is `x5_r37_base_tuning_reproduces_1b0ce54e`
/// (X5-R37: a draw made at share 0 whose result reached `S_GEN`'s
/// order would move a BASE digest). **M-R50-DRAW** (`S_R41P` drawn at
/// share 0, its result fed into `S_GEN`'s order) reverts this.
#[test]
fn x5_r50_not_drawn_at_base() {
    for seed in 0..64u64 {
        for round in 0..3u32 {
            assert_eq!(
                workload::r41p_pick(seed, round, &workload::Tuning::BASE),
                None,
                "seed {seed} round {round}: r41p_share 0 must draw nothing"
            );
        }
    }
    let mut share1 = workload::Tuning::BASE;
    share1.r41_share = 1000;
    share1.r41p_share = 100;
    let (mut somes, mut trues) = (0usize, 0usize);
    for seed in 0..500u64 {
        if let Some(pick) = workload::r41p_pick(seed, 0, &share1) {
            somes += 1;
            trues += usize::from(pick);
        }
    }
    assert_eq!(somes, 500, "a non-zero share draws for every round");
    assert!(
        trues > 0 && trues < 500,
        "r41p_share 100 permille must both hit and miss over 500 seeds"
    );
    let mut share1000 = workload::Tuning::BASE;
    share1000.r41_share = 1000;
    share1000.r41p_share = 1000;
    for seed in 0..8u64 {
        assert_eq!(
            workload::r41p_pick(seed, 0, &share1000),
            Some(true),
            "r41p_share 1000 permille selects every eligible round"
        );
    }
}

/// Extract one alignment block's ops from a GC task's op list: the
/// four commits, the `R41Wait { idx }`, the sweep, the split commit
/// and the `R41Done { idx }` — the slice from four ops before the
/// wait through the done (test helper for the X5-R50 pins).
fn r50_block(ops: &[ScriptOp], idx: u64) -> &[ScriptOp] {
    let wait = ops
        .iter()
        .position(|o| matches!(o, ScriptOp::R41Wait { idx: i } if *i == idx))
        .unwrap_or_else(|| panic!("no R41Wait {{ idx: {idx} }} in the GC task"));
    let done = ops
        .iter()
        .position(|o| matches!(o, ScriptOp::R41Done { idx: i } if *i == idx))
        .unwrap_or_else(|| panic!("no R41Done {{ idx: {idx} }} in the GC task"));
    &ops[wait - 4..=done]
}

/// The GC task's op list of a built round (test helper).
fn r50_gc(script: &e4_recovery::harness::Script) -> &[ScriptOp] {
    &script
        .tasks
        .iter()
        .find(|t| t.ops.iter().any(|o| matches!(o, ScriptOp::Gc { .. })))
        .expect("the round has a GC task")
        .ops
}

/// **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5b): an eligible round's
/// prior window is aligned, and block B is untouched.** The literal
/// seed is **0 round 0** at (`r41_share` 1000, `r41p_share` 1000):
/// targeted (`seed/10` even), no drop, hook `Before0` (a pre-step-5
/// crash), `target_ckpt` 2 — eligible, and its `S_R41P` draw hits at
/// share 1000. The crash is `Before0@2`: checkpoint 2's window never
/// completed, so the last completed ledger row **is checkpoint
/// `idx−1` = 1's window** and the recovery opens its meta — the
/// record's `imgsp`/`imgmg`/`imgreuse`/`imgreuse_pf` (4/8/3/3 exact)
/// are that window's counts, and `q4i_raw` (and `q4i`) hold through
/// block A's pre-freeze reuses. At (`1000`, `0`) the same round reads
/// `q4i_raw` **false** (image 1's window was never aligned) with seed
/// 0's X5-R37 BASE digest, and block B's ops are **identical** at both
/// settings (its content draws come from `S_R41C` alone); block A
/// exists only at (1000, 1000), with its own arm.
/// **M-R50-SHAREDSTREAM** (block A draws from `S_R41C`) shifts block
/// B's draws and breaks the block-B identity; **M-R50-SHAREDFLAG**
/// (one window flag for both arms) ungates block B and moves the
/// digest — each fails this pin.
#[test]
fn x5_r50_prior_window_round_is_q4i() {
    use e4_recovery::model::Model;
    use e4_recovery::store::ckpt::{CKPT_HOOKS, CkptHook};
    use workload::{Knobs, Tuning, build_round};
    let mut t50 = Tuning::BASE;
    t50.r41_share = 1000;
    let mut tpp = Tuning::BASE;
    tpp.r41_share = 1000;
    tpp.r41p_share = 1000;
    // The eligibility premises, read the same way the generator does
    // (seed 0: seed/10 = 0, even — targeted; (seed/20) % 10 = 0).
    let knobs = Knobs::derive(0, 0, &tpp);
    let zero = 0u64;
    assert_eq!(
        zero / 10,
        0,
        "seed 0's seed/10 is even: a targeted-crash seed"
    );
    assert!(!knobs.drop_tree, "round 0 of seed 0 is not a drop round");
    assert_eq!(CKPT_HOOKS[0], CkptHook::Before0);
    assert_eq!(knobs.target_ckpt, 2, "idx is 2, so idx−1 = 1 exists");
    assert_eq!(
        workload::r41p_pick(0, 0, &tpp),
        Some(true),
        "the S_R41P draw hits at share 1000"
    );
    // The (1000, 1000) arm: the aligned prior window makes the round q4i.
    let rec = workload::run_seed_with(0, None, &tpp);
    assert!(
        rec.defect.is_none(),
        "seed 0 at (1000,1000): {:?}",
        rec.defect
    );
    assert_eq!(
        rec.digest, 1989125988,
        "seed 0's (1000,1000) schedule moved"
    );
    let r0 = rec.rounds.first().expect("seed 0 runs round 0");
    assert_eq!(r0.crash, "Before0@2", "round 0's crash point moved");
    assert!(r0.fired, "round 0's crash must fire");
    assert_eq!(r0.imgsp, 4, "imgsp exact (checkpoint idx-1 = 1's window)");
    assert_eq!(r0.imgmg, 8, "imgmg exact");
    assert_eq!(r0.imgreuse, 3, "imgreuse exact");
    assert_eq!(
        r0.imgreuse_pf, 3,
        "imgreuse_pf exact (block A's reuses, all pre-freeze)"
    );
    assert!(
        r0.q4i_raw,
        "the opened image is checkpoint 1's (idx-1): its window held reuses"
    );
    assert!(r0.q4i, "the round counts for the image arm");
    assert!(r0.pass, "the aligned round passes: {:?}", r0.o3_fail);
    // The (1000, 0) arm: no alignment, not q4i, BASE digest.
    let rec50 = workload::run_seed_with(0, None, &t50);
    assert_eq!(
        rec50.digest, 3702394764,
        "seed 0's (1000,0) digest equals its X5-R37 literal"
    );
    let r50 = rec50.rounds.first().expect("seed 0 runs round 0");
    assert!(
        !r50.q4i_raw,
        "without block A the opened window held no reuse"
    );
    // Block B's identity across the two settings, and block A's
    // presence beside it.
    let (s50, _, _) = build_round(0, 0, &t50, &Knobs::derive(0, 0, &t50), &Model::new(), 0);
    let (spp, _, _) = build_round(0, 0, &tpp, &Knobs::derive(0, 0, &tpp), &Model::new(), 0);
    let (g50, gpp) = (r50_gc(&s50), r50_gc(&spp));
    assert_eq!(
        format!("{:?}", r50_block(g50, 2)),
        format!("{:?}", r50_block(gpp, 2)),
        "block B's ops are bit-for-bit the same at (1000,0) and (1000,1000) \
         (M-R50-SHAREDSTREAM reverts this)"
    );
    assert!(
        !g50.iter().any(|o| matches!(
            o,
            ScriptOp::R41Arm { idx: 1 }
                | ScriptOp::R41Wait { idx: 1 }
                | ScriptOp::R41Done { idx: 1 }
        )),
        "at (1000, 0) no block A ops exist"
    );
    assert!(
        gpp.iter().any(|o| matches!(o, ScriptOp::R41Arm { idx: 1 })),
        "at (1000, 1000) block A's arm sits beside block B's"
    );
    // Block A exists at (1000, 1000) and is its own draws (would panic
    // here if the wait/done pair were missing).
    let _ = r50_block(gpp, 1);
}

/// **X5-R50 (card X-5c-F4-F1-F1-F3-F1 step 5b): an ineligible round
/// draws nothing and gains no block, however its draw would read.**
/// Two literal ineligible seeds at (`1000`, `1000`), both targeted,
/// both non-drop, both with a hitting `S_R41P` draw at share 1000:
/// **140 round 0** (hook `After5`, a post-step-5 hook — the crash
/// opens image `idx` itself, X5-R41's own alignment; `target_ckpt` 1)
/// and **1 round 0** (hook `Before0`, but `target_ckpt` 0 — index
/// `idx−1` does not exist). Neither round's GC task carries block A's
/// arm, wait or done at (1000, 1000), and each round's GC ops are
/// **identical** to the same round at (1000, 0).
/// **M-R50-ANYHOOK** (eligibility drops the hook clause) selects 140
/// and **M-R50-IDX0** (eligibility drops `idx ≥ 1`) selects 1 — each
/// fails this pin. A third literal (X-5c-PINS-1 item 1) is the
/// uniform-crash seed **13 round 0** (below), which kills
/// **GX-R50-NOSELECT** (`r41p` losing `r41 &&`).
#[test]
fn x5_r50_ineligible_rounds_draw_nothing() {
    use e4_recovery::model::Model;
    use e4_recovery::store::ckpt::{CKPT_HOOKS, CkptHook};
    use workload::{Knobs, Tuning, build_round};
    let mut t50 = Tuning::BASE;
    t50.r41_share = 1000;
    let mut tpp = Tuning::BASE;
    tpp.r41_share = 1000;
    tpp.r41p_share = 1000;
    for (seed, hook_want) in [(140u64, CkptHook::After5), (1u64, CkptHook::Before0)] {
        let knobs = Knobs::derive(seed, 0, &tpp);
        assert_eq!(seed / 10 % 2, 0, "seed {seed} is a targeted-crash seed");
        assert!(
            !knobs.drop_tree,
            "round 0 of seed {seed} is not a drop round"
        );
        assert_eq!(CKPT_HOOKS[((seed / 20) % 10) as usize], hook_want);
        assert_eq!(
            workload::r41p_pick(seed, 0, &tpp),
            Some(true),
            "seed {seed}'s S_R41P draw hits at share 1000 — only the clause keeps it out"
        );
        let (s50, _, _) = build_round(
            seed,
            0,
            &t50,
            &Knobs::derive(seed, 0, &t50),
            &Model::new(),
            0,
        );
        let (spp, _, _) = build_round(
            seed,
            0,
            &tpp,
            &Knobs::derive(seed, 0, &tpp),
            &Model::new(),
            0,
        );
        let (g50, gpp) = (r50_gc(&s50), r50_gc(&spp));
        // The prior index this round would have paired block A with;
        // `None` when idx is 0 (seed 1 — the idx clause's case).
        let prior = knobs.target_ckpt.checked_sub(1);
        for ops in [g50, gpp] {
            assert!(
                !ops.iter().any(|o| matches!(
                    o,
                    ScriptOp::R41Arm { idx: i }
                        | ScriptOp::R41Wait { idx: i }
                        | ScriptOp::R41Done { idx: i }
                    if Some(*i) == prior
                )),
                "seed {seed}: no block A op for index {prior:?} may exist (M-R50-ANYHOOK for \
                 seed 140, M-R50-IDX0 for seed 1)"
            );
        }
        assert_eq!(
            format!("{g50:?}"),
            format!("{gpp:?}"),
            "seed {seed}: the GC task is identical at (1000,0) and (1000,1000)"
        );
    }
    // The premise that makes the two seeds ineligible in different
    // clauses: 140's hook is post-step-5 (idx 1 exists), 1's idx is 0.
    let k140 = Knobs::derive(140, 0, &tpp);
    assert_eq!(
        k140.target_ckpt, 1,
        "seed 140: only the hook clause excludes it"
    );
    let k1 = Knobs::derive(1, 0, &tpp);
    assert_eq!(k1.target_ckpt, 0, "seed 1: only the idx clause excludes it");
    // **X-5c-PINS-1 item 1 (verdict X-5c-F4-F1-F1-F3-F1 §5's follow-up,
    // GX-R50-NOSELECT): a uniform-crash round draws nothing and gains no
    // block A, however its draw reads.** The literal seed is **13 round
    // 0**: `seed/10` odd (5.5 gives it a uniform crash, so the X5-R41
    // selection's targeted-crash clause is false), not a drop round,
    // its hook `Before0` is one of the seven pre-step-5 hooks, and
    // `target_ckpt` is 1 — every clause of the eligibility except the
    // selection holds, and at share 1000 the `S_R41P` draw hits. Only
    // the `r41` conjunction keeps the round out, so nothing is drawn
    // (the GC task is bit-for-bit the share-0 form's), no
    // `R41Arm { idx: idx−1 }` appears and block A's content never
    // enters the script. **GX-R50-NOSELECT** (`r41p` loses `r41 &&`)
    // selects this round — the draw already hit — and block A appears,
    // failing this pin.
    {
        let seed = 13u64;
        let knobs = Knobs::derive(seed, 0, &tpp);
        assert_eq!(seed / 10 % 2, 1, "seed {seed} is a uniform-crash seed");
        assert!(
            !knobs.drop_tree,
            "round 0 of seed {seed} is not a drop round"
        );
        assert_eq!(
            CKPT_HOOKS[((seed / 20) % 10) as usize],
            CkptHook::Before0,
            "seed {seed}: a pre-step-5 hook (the hook clause holds)"
        );
        assert_eq!(
            knobs.target_ckpt, 1,
            "seed {seed}: the idx clause holds (idx−1 = 0 exists)"
        );
        assert_eq!(
            workload::r41p_pick(seed, 0, &tpp),
            Some(true),
            "seed {seed}'s S_R41P draw hits at share 1000 — only the r41 clause keeps it out"
        );
        let (s50, _, _) = build_round(
            seed,
            0,
            &t50,
            &Knobs::derive(seed, 0, &t50),
            &Model::new(),
            0,
        );
        let (spp, _, _) = build_round(
            seed,
            0,
            &tpp,
            &Knobs::derive(seed, 0, &tpp),
            &Model::new(),
            0,
        );
        let (g50, gpp) = (r50_gc(&s50), r50_gc(&spp));
        // No block A op for index idx−1 = 0 in either setting.
        for ops in [&g50, &gpp] {
            assert!(
                !ops.iter().any(|o| matches!(
                    o,
                    ScriptOp::R41Arm { idx: 0 }
                        | ScriptOp::R41Wait { idx: 0 }
                        | ScriptOp::R41Done { idx: 0 }
                )),
                "seed {seed}: an unselected uniform round carries no block A op for index 0 \
                 (GX-R50-NOSELECT reverts this)"
            );
        }
        assert_eq!(
            format!("{g50:?}"),
            format!("{gpp:?}"),
            "seed {seed}: nothing is drawn and no block A content enters — the GC task is \
             identical at (1000,0) and (1000,1000)"
        );
    }
}

/// **X5-R43 (card X-5c-F4-F1-F1-F3-F1 step 7b): the Slot fault newly
/// fails a `q4_split_merge` round at the chosen setting.** The literal
/// seed is **20 round 1** at S1 = (`r41_share` 1000, `r41p_share`
/// 500), crash `After0@2` — the crash inside checkpoint 2's held
/// window (block B's: `q4sp` 5, `q4mg` 8, `q4reuse` 4, so
/// `q4_split_merge` holds) while the recovery opens image 1 (block
/// A's window). Clean: digest **462038950**, the round passes with no
/// oracle text. Slot arm (`M-FREE-HOME-ALLOC`): digest **3929895968**,
/// the same round fails the oracle (`O-1(b): scan read failed`) with
/// `q4_split_merge` still true — the freed home was handed out inside
/// the aligned window the Q4 predicate keys on, which is why the Slot
/// fault now catches where F3 read zero (the mechanism the Goal
/// states, confirmed; the full tables are in the README's step 7
/// record).
#[test]
fn x5_r43_slot_caught_on_a_q4_round() {
    use e4_recovery::campaign::seeded_new_failures;
    use e4_recovery::store::Mutant;
    let mut tun = workload::Tuning::BASE;
    tun.r41_share = 1000;
    tun.r41p_share = 500;
    let clean = workload::run_seed_with(20, None, &tun);
    assert!(clean.defect.is_none(), "seed 20: {:?}", clean.defect);
    assert_eq!(clean.digest, 462038950, "seed 20's S1 clean schedule moved");
    let slot = workload::run_seed_with(20, Some(Mutant::FreeHomeAlloc), &tun);
    assert!(slot.defect.is_none(), "seed 20 slot: {:?}", slot.defect);
    assert_eq!(slot.digest, 3929895968, "seed 20's S1 Slot digest moved");
    assert_ne!(clean.digest, slot.digest, "the fault must move the digest");
    let cr = clean.rounds.get(1).expect("seed 20 runs round 1");
    let fr = slot.rounds.get(1).expect("the Slot arm runs round 1");
    assert_eq!(cr.crash, "After0@2", "round 1's crash point moved");
    // The Q4 round's halves, both arms (the crash inside block B's
    // held window).
    for r in [cr, fr] {
        assert!(r.q4_split_merge, "the round carries Q4's own predicate");
        assert_eq!(r.q4sp, 5, "q4sp exact");
        assert_eq!(r.q4mg, 8, "q4mg exact");
        assert!(r.q4reuse >= 1, "the held window reused an LPID");
    }
    // The X5-R43 catch shape: clean passes the oracle, the fault's
    // arm fails it, and the rule counts the round.
    assert!(cr.pass, "the clean round passes: {:?}", cr.o3_fail);
    assert!(
        cr.o1_fail.is_none() && cr.o2_fail.is_none() && cr.o3_fail.is_none(),
        "the clean round carries no oracle text"
    );
    assert!(
        fr.o1_fail.is_some() || fr.o2_fail.is_some() || fr.o3_fail.is_some(),
        "M-FREE-HOME-ALLOC fails the oracle on 20 r1: o1={:?}",
        fr.o1_fail
    );
    assert!(!fr.deadlock && !fr.panic, "an oracle failure, never a hang");
    let nf = seeded_new_failures(std::slice::from_ref(&clean), std::slice::from_ref(&slot));
    assert_eq!(nf, vec![(20, 1)], "the round counts as new-failing");
}

/// **X5-R42 (step 6): a refused round is not `q4i`.** The fold is
/// `q4i_raw ∧ fired ∧ end.is_some()` (Q18's end): the exclusions match
/// SIZING's — nested recovery crashes, the harness checkpoint, and
/// rounds whose final recovery yields no state. **M-Q4I-REFUSED**
/// (the `end.is_some()` term dropped) fails the refused case.
#[test]
fn x5_r42_refused_round_is_not_q4i() {
    use workload::q4i_of;
    assert!(
        q4i_of(true, true, true),
        "the full conjunction holds: an opened image's reuse in a crash round"
    );
    assert!(
        !q4i_of(true, true, false),
        "a refused or failed open (no end state) is excluded (M-Q4I-REFUSED reverts this)"
    );
    assert!(!q4i_of(false, true, true), "no reuse in the opened window");
    assert!(
        !q4i_of(true, false, true),
        "a round whose own crash never fired is excluded"
    );
}

/// **X5-R43 (card X-5c-F4-F1-F1-F3-F1 step 1b): the Slot fault is
/// caught at BASE — `M-FREE-HOME-ALLOC` fails the oracle on a round
/// that passes clean.** The literal seed is **5 round 1**, the lowest
/// seed in 0..1000 at BASE whose Slot-arm digest differs from clean
/// and whose round fails O-1, O-2 or O-3 with the mutant while
/// passing the oracle without it (`pass` and no oracle text). Both
/// digests and both readings are asserted: clean digest **980071820**
/// (seed 5's X5-R37 literal, unchanged), Slot-arm digest **204526290**
/// (they differ — at F3's tip the mutant was inert, digest-identical
/// to clean, verdict finding D1); the clean round passes with no
/// O-1/O-2/O-3 text, and the same round under the mutant fails
/// `O-1(a)`. **G-SLOT-INERT** (the mutant's body emptied) reverts the
/// digest difference and the oracle failure and fails this pin.
#[test]
fn x5_r43_free_home_alloc_is_caught() {
    use e4_recovery::store::Mutant;
    let clean = workload::run_seed_with(5, None, &workload::Tuning::BASE);
    assert!(clean.defect.is_none(), "seed 5 clean: {:?}", clean.defect);
    assert_eq!(clean.digest, 980071820, "seed 5's clean BASE digest moved");
    let slot = workload::run_seed_with(5, Some(Mutant::FreeHomeAlloc), &workload::Tuning::BASE);
    assert!(slot.defect.is_none(), "seed 5 slot: {:?}", slot.defect);
    assert_eq!(slot.digest, 204526290, "seed 5's Slot-arm digest moved");
    assert_ne!(
        clean.digest, slot.digest,
        "the live Slot fault must move the digest (G-SLOT-INERT reverts this)"
    );
    let cr = clean.rounds.get(1).expect("seed 5 runs round 1");
    let fr = slot.rounds.get(1).expect("the Slot arm runs round 1");
    // The clean reading: pass, no oracle text.
    assert!(cr.pass, "the clean round passes: {:?}", cr.o3_fail);
    assert!(
        cr.o1_fail.is_none() && cr.o2_fail.is_none() && cr.o3_fail.is_none(),
        "the clean round carries no oracle text"
    );
    // The Slot-arm reading: the same round fails the oracle.
    assert!(
        fr.o1_fail.is_some() || fr.o2_fail.is_some() || fr.o3_fail.is_some(),
        "M-FREE-HOME-ALLOC must fail the oracle on 5 r1: o1={:?} o2={:?} o3={:?}",
        fr.o1_fail,
        fr.o2_fail,
        fr.o3_fail
    );
    assert!(
        !fr.deadlock && !fr.panic,
        "the catch is an oracle failure, never a hang"
    );
}

/// **X-5c-F4-F1-F1-F3-F2-F1-F2 (P ruling, (a)): `twin_dirs_differ`
/// compares the `dir` key sets and nothing else.** Two stores from
/// `harness::create_store` with equal keys read false (their `DirEntry`
/// values may differ — roots and pointers differ between any two
/// chains, and the values are not the predicate); a key removed from
/// one `dir`, or a key added to one, reads true; equal keys with one
/// `DirEntry` **value** changed read false. **M-TWIN-PRED-ALWAYS**
/// (the predicate returns true) and **M-TWIN-PRED-ENTRIES** (the
/// predicate compares `DirEntry` values too) both fail here.
#[test]
fn x5r52_twin_dirs_differ_compares_directory_keys() {
    use e4_recovery::store::DirEntry;
    let cfg = Knobs::derive(0, 0, &workload::Tuning::BASE).store_cfg(None);
    let (_, mut real) = e4_recovery::harness::create_store(cfg.clone(), 0, 0).expect("create");
    let (_, mut twin) = e4_recovery::harness::create_store(cfg, 0, 0).expect("create");
    assert!(
        !workload::twin_dirs_differ(&real, &twin),
        "two fresh stores have equal (empty) dir key sets"
    );
    // Equal key sets populated with different DirEntry values: not a
    // difference — the values are not the predicate.
    real.dir.insert(
        1,
        DirEntry {
            root: 11,
            dropping: false,
        },
    );
    real.dir.insert(
        2,
        DirEntry {
            root: 12,
            dropping: false,
        },
    );
    twin.dir.insert(
        1,
        DirEntry {
            root: 21,
            dropping: true,
        },
    );
    twin.dir.insert(
        2,
        DirEntry {
            root: 22,
            dropping: true,
        },
    );
    assert!(
        !workload::twin_dirs_differ(&real, &twin),
        "equal keys with different DirEntry values are not a difference"
    );
    // Equal keys, one DirEntry value changed: still not a difference.
    twin.dir.get_mut(&2).expect("the key exists").root ^= 1;
    assert!(
        !workload::twin_dirs_differ(&real, &twin),
        "a changed DirEntry value with equal keys is not a difference (M-TWIN-PRED-ENTRIES reverts this)"
    );
    // A key removed from the twin's dir: a difference.
    assert!(twin.dir.remove(&2).is_some());
    assert!(
        workload::twin_dirs_differ(&real, &twin),
        "a key the real dir holds and the twin dir lacks is a difference"
    );
    // Restoring the key set restores equality.
    twin.dir.insert(
        2,
        DirEntry {
            root: 12,
            dropping: false,
        },
    );
    assert!(
        !workload::twin_dirs_differ(&real, &twin),
        "restoring the key set restores equality"
    );
    // A key added to the twin's dir alone: a difference.
    twin.dir.insert(
        4,
        DirEntry {
            root: 9,
            dropping: true,
        },
    );
    assert!(
        workload::twin_dirs_differ(&real, &twin),
        "a key the twin dir holds and the real dir lacks is a difference (M-TWIN-PRED-ALWAYS reverts this)"
    );
    // **X-5c-PINS-1 item 3 (verdict `af5c8d63` §5): equal-size
    // directories with different key sets are a difference too.** The
    // real dir holds {1, 2} and the twin {1, 4}: the counts are equal,
    // so a predicate that compared key counts only would read false —
    // and the round would not re-seed, exactly the shape the verdict's
    // 10k census found no live case of ("0 cases of equal-size but
    // different key sets"), which is why no reading depends on it.
    // **G-PRED-LEN-ONLY** (the predicate compares key counts only)
    // reverts this.
    twin.dir.remove(&2).expect("key 2 is in the twin dir");
    assert_eq!(
        real.dir.len(),
        twin.dir.len(),
        "the premise: equal key counts"
    );
    assert!(
        workload::twin_dirs_differ(&real, &twin),
        "equal-size dirs with different key sets are a difference (G-PRED-LEN-ONLY reverts this)"
    );
}

/// **X-5c-F4-F1-F1-F3-F2-F1-F2 (P ruling, (b)): the directory re-seed
/// heals 2859's twin r1.** At the Base (verdict
/// `X-5c-F4-F1-F1-F3-F2-F1`, finding 1) the faultless twin's recovery
/// fails with `Corrupt("tree 4 missing in directory")`: real r0's WAL
/// fault loses `ts=3 DropTree 4`, the twin keeps it, and r1's script —
/// built from the real model — writes into tree 4. With the re-seed the
/// twin chain restarts r1 from the real round's own start, so its
/// directory holds every tree the script touches, and the twin round
/// passes with no O-1 text. **M-TWIN-NO-DIR-RESEED**, **M-TWIN-RESEED-
/// FROM-TWIN** and **M-TWIN-RESEED-LATE** all fail here.
#[test]
fn x5r52_twin_reseed_heals_2859_r1() {
    let rec = workload::run_seed_with(2859, None, &workload::Tuning::BASE);
    let t1 = rec.twin.get(1).expect("2859 runs two rounds");
    assert!(
        t1.pass,
        "2859 twin r1 must pass under the directory re-seed (o1: {:?})",
        t1.o1_fail
    );
    assert!(
        t1.o1_fail.is_none(),
        "2859 twin r1 must carry no O-1 text under the directory re-seed (got {:?})",
        t1.o1_fail
    );
}

/// **X-5c-F4-F1-F1-F3-F2-F1-F2 (P ruling, (b)): the same healing for
/// 609's twin r1** — the reference file's first pre-existing member of
/// the C-XX twin recovery-Err family (Base twin r1 fails with
/// `Corrupt("tree N missing in directory")`), pinned by the verdict's
/// follow-up. The same mutants as 2859 fail here.
#[test]
fn x5r52_twin_reseed_heals_609_r1() {
    let rec = workload::run_seed_with(609, None, &workload::Tuning::BASE);
    let t1 = rec.twin.get(1).expect("609 runs two rounds");
    assert!(
        t1.pass,
        "609 twin r1 must pass under the directory re-seed (o1: {:?})",
        t1.o1_fail
    );
    assert!(
        t1.o1_fail.is_none(),
        "609 twin r1 must carry no O-1 text under the directory re-seed (got {:?})",
        t1.o1_fail
    );
}

/// **X-5c-F4-F1-F1-F3-F2-F1-F2 (P ruling, step 3): the twin chain is
/// kept when the directories agree.** Seed **639** is the lowest MW seed
/// in 0..1000 with at least two rounds, no re-seed event in step 2's
/// attribution, and a digest that moves under M-TWIN-RESEED-ALWAYS
/// (`3366243401 → 1144734390`, measured; the thirteen lower candidates
/// 9, 129, 139, 189, 199, 309, 369, 399, 479, 509, 519, 559, 569 are
/// inert under it — their digests equal the baseline's). Its digest at
/// this tip equals the Base's literal, unchanged: (b) never fires for
/// it, so the seed runs bit for bit as at the Base (ruling (e)).
/// **M-TWIN-RESEED-ALWAYS** and **M-TWIN-PRED-ALWAYS** fail here.
#[test]
fn x5r52_twin_chain_kept_when_directories_agree_639() {
    let rec = workload::run_seed_with(639, None, &workload::Tuning::BASE);
    assert_eq!(
        rec.digest, 3366243401,
        "639 has no directory divergence: its digest must equal the Base's, \
         byte for byte (M-TWIN-RESEED-ALWAYS reverts this)"
    );
}

// ---- card X-5c-F4-F2 step 1: X5-R39's rows, pins on literal records ----

/// A record helper for the X5-R39 pins: one seed, `class`, whose rounds
/// are built by `mk` (a round's szc/szq/szi flags), with a stable
/// digest placeholder (these pins read the quota rows and the
/// reproduction comparison, never the digest itself).
fn x5r39_rec(class: &str, rounds: Vec<(bool, bool, bool)>) -> workload::SeedRec {
    workload::SeedRec {
        seed: 0,
        class: class.to_owned(),
        rounds: rounds
            .into_iter()
            .map(|(szc, szq, szi)| workload::RoundRec {
                szc,
                szq,
                szi,
                ..workload::RoundRec::default()
            })
            .collect(),
        ..workload::SeedRec::default()
    }
}

/// **X5-R39 pin (step 1): the four rows' shape and class split, on a
/// literal record set.** One C round with `szq ∧ szi`, two MP rounds
/// with `szq` (one of them `szi`), one MW round with `szq`, and one
/// refused C round (`szc` without `szq` — PLAN §10 item 10's exclusion:
/// "a refused round tests detection, not the recovery under pressure"):
/// SZC counts 4 (every class), SZC-MP 2, SZC-MW 1, SZI 2, and the
/// refused round counts in none of them. **M-SZQ-REFUSED** (the
/// `end.is_some()` term dropped, so a refused round's `szq` is its
/// `szc`) fails the SZC/SZC-MP/SZC-MW counts here.
#[test]
fn x5_r39_rows_shape_and_class_split() {
    let recs = vec![
        x5r39_rec("C", vec![(true, true, true)]),
        x5r39_rec("MP", vec![(true, true, false), (true, true, true)]),
        x5r39_rec("MW", vec![(true, true, false)]),
        // The refused round: the crash cut the interval, the recovery
        // opened nothing.
        x5r39_rec("C", vec![(true, false, false)]),
    ];
    let rows = workload::quotas(&recs, 4);
    let row = |id: &str| -> &workload::QuotaRow {
        rows.iter()
            .find(|r| r.id == id)
            .unwrap_or_else(|| panic!("row {id}"))
    };
    assert_eq!(
        row("SZC").achieved,
        4,
        "every class counts, the refused round does not"
    );
    assert_eq!(row("SZC-MP").achieved, 2);
    assert_eq!(row("SZC-MW").achieved, 1);
    assert_eq!(row("SZI").achieved, 2);
    assert_eq!(
        row("SZC").required,
        1_000,
        "the owner's quota (PLAN §10 item 10)"
    );
    assert_eq!(row("SZC-MP").required, 100);
    assert_eq!(row("SZC-MW").required, 50);
    assert_eq!(row("SZI").required, 300, "the interrupted-checkpoint floor");
    // A round that is szq without the fold/window terms is never szi
    // (the floor is a subset of the quota, counted inside it).
    let recs = vec![x5r39_rec("C", vec![(true, true, false)])];
    let rows = workload::quotas(&recs, 1);
    let szi = rows.iter().find(|r| r.id == "SZI").expect("the SZI row");
    assert_eq!(szi.achieved, 0);
}

/// **X5-R39 pin: the rows at 999/1,000, 99/100, 49/50 and 299/300.**
/// Each row misses at one under its floor and meets at it — the
/// floor's own edge, on literal records. **M-SZ-NOFLOOR** (the SZI
/// row's minimum goes to 0) fails the 299 half of the last pair.
#[test]
fn x5_r39_row_floors_at_the_edge() {
    let mk = |n: usize| {
        (0..n)
            .map(|_| x5r39_rec("C", vec![(true, true, true)]))
            .collect::<Vec<_>>()
    };
    let read = |recs: &[workload::SeedRec], id: &str| -> workload::QuotaRow {
        let rows = workload::quotas(recs, recs.len() as u64);
        rows.iter()
            .find(|r| r.id == id)
            .unwrap_or_else(|| panic!("row {id}"))
            .clone()
    };
    for (id, floor) in [("SZC", 1_000u64), ("SZI", 300u64)] {
        let under = read(&mk(floor as usize - 1), id);
        assert_eq!(under.achieved, floor - 1);
        assert!(under.achieved < under.required, "{id} misses at one under");
        let at = read(&mk(floor as usize), id);
        assert_eq!(at.achieved, floor);
        assert!(at.achieved >= at.required, "{id} meets at its floor");
    }
    // The per-class rows: MP and MW rounds only.
    let mk_mp = |n: usize| {
        (0..n)
            .map(|_| x5r39_rec("MP", vec![(true, true, true)]))
            .collect::<Vec<_>>()
    };
    let mk_mw = |n: usize| {
        (0..n)
            .map(|_| x5r39_rec("MW", vec![(true, true, true)]))
            .collect::<Vec<_>>()
    };
    for (id, mk, floor) in [
        ("SZC-MP", mk_mp as fn(usize) -> Vec<_>, 100u64),
        ("SZC-MW", mk_mw as fn(usize) -> Vec<_>, 50u64),
    ] {
        let under = read(&mk(floor as usize - 1), id);
        assert_eq!(under.achieved, floor - 1);
        assert!(under.achieved < under.required, "{id} misses at one under");
        let at = read(&mk(floor as usize), id);
        assert_eq!(at.achieved, floor);
        assert!(at.achieved >= at.required, "{id} meets at its floor");
    }
}

/// **X5-R39 pin: a per-seed miss with every row met exits 0.** OQ-1's
/// ruling (PLAN §10 item 10: "The per-seed count stays reported … and
/// is no longer an exit input") removed SIZING's exit role: the row's
/// achieved (the per-seed miss count) prints as `INFO SIZING …` and
/// never counts toward `missed`. The literal table has every judged row
/// met and SIZING achieved 3 required 0: no row is missed and the exit
/// is 0. **M-SZ-EXIT** (a SIZING seed miss fails the exit again) counts
/// the row and fails this pin.
#[test]
fn x5_r39_a_per_seed_sizing_miss_is_not_an_exit_input() {
    let met = |id: &'static str, what: &str, n: u64| workload::QuotaRow {
        id,
        what: what.to_owned(),
        achieved: n,
        required: n,
    };
    let rows = vec![
        met("Q1", "crash at hook After0", 300),
        met("Q4", "interrupted ckpt with split and merge", 1_000),
        met("SZC", "szq rounds", 1_000),
        met("SZC-MP", "MP szq rounds", 100),
        met("SZC-MW", "MW szq rounds", 50),
        met("SZI", "szi rounds", 300),
        // The per-seed miss count: achieved 3, judged by nothing.
        workload::QuotaRow {
            id: "SIZING",
            what: "seeds missing Work 5.3's >= 4*F dirtied LPIDs (per-seed reporting)".to_owned(),
            achieved: 3,
            required: 0,
        },
    ];
    assert_eq!(
        campaign::verify_quota_missed(campaign::CampaignSet::Main, &rows),
        0,
        "SIZING is INFO: a per-seed miss never counts (M-SZ-EXIT reverts this)"
    );
    assert_eq!(
        campaign::verify_exit_code(
            true,
            0,
            campaign::verify_quota_missed(campaign::CampaignSet::Main, &rows),
            false
        ),
        0
    );
    // Every judged row still counts: one SZC row short is a miss.
    let rows = vec![
        met("SZC", "szq rounds", 1_000),
        workload::QuotaRow {
            id: "SZC",
            what: "a second row".to_owned(),
            achieved: 999,
            required: 1_000,
        },
    ];
    assert_eq!(
        campaign::verify_quota_missed(campaign::CampaignSet::Main, &rows),
        1
    );
}

/// **X5-R39 pin (card X-5c-F4-F2 step 1): the `After4` literal config —
/// `szc`, `szq` and `szi` all true.** Seed 1926 round 2, under
/// `Tuning::BASE`: the crash (`After4@1`) fired inside the
/// `ckpt_unflipped` window (position code 7) with the open checkpoint's
/// begin-step fold at 180 ≥ 4·F = 176 dirtied LPIDs, and the final
/// recovery opened a store — so `szi` is true through all three terms.
/// **M-SZI-ABORTRESET** (cleared on every abort: the `!crash_unwind`
/// guard on `checkpoint()`'s abort-path `ckpt_closed_dirty` reset
/// dropped — this implementation's carrier of REFRESH 6's non-crash
/// clear, which `szi` conjoins) reads the fold as 0 after the sim-crash
/// unwind and fails this pin (`szi` false).
#[test]
fn x5_r39_after4_crash_with_a_4f_fold_is_szi() {
    let rec = workload::run_seed_with(1926, None, &workload::Tuning::BASE);
    assert!(rec.defect.is_none(), "seed 1926: {:?}", rec.defect);
    assert_eq!(
        rec.digest, 1_452_311_068,
        "seed 1926: the pinned BASE digest"
    );
    let r2 = rec
        .rounds
        .get(2)
        .unwrap_or_else(|| panic!("seed 1926: three rounds"));
    assert_eq!(
        r2.crash, "After4@1",
        "seed 1926 r2: the pinned hook crash inside the window"
    );
    assert!(r2.fired, "seed 1926 r2: the crash fired");
    assert!(
        r2.szc,
        "the crash cut a >= 4*F interval (the fold was 180 >= 176)"
    );
    assert!(r2.szq, "the final recovery opened a store");
    assert!(
        r2.szi,
        "szc with the 4*F fold and the crash inside the ckpt_unflipped window \
         must set szi (M-SZI-ABORTRESET reverts this)"
    );
    assert!(
        r2.pass,
        "seed 1926 r2: the round passes (o1={:?} o2={:?} o3={:?})",
        r2.o1_fail, r2.o2_fail, r2.o3_fail
    );
}

/// **X5-R39 pin: the `Before0` literal config — `szc` and `szq` true,
/// `szi` false.** Seed 1005 round 2, under `Tuning::BASE`: the crash
/// (`Before0@2`) fired with the checkpoint open (its begin step had
/// folded 209 ≥ 4·F = 184 — at `Before0` the open interval is still
/// empty, so `szc` can only be true through the fold) and the final
/// recovery opened a store; but nothing of the checkpoint had happened
/// yet at the crash point — the flip had not run — so `szi` is false.
/// **M-SZI-BEFORE0** (`ckpt_unflipped` set at begin) reads the position
/// code 1 as inside the window and fails this pin (`szi` true).
#[test]
fn x5_r39_before0_crash_never_sets_szi() {
    let rec = workload::run_seed_with(1005, None, &workload::Tuning::BASE);
    assert!(rec.defect.is_none(), "seed 1005: {:?}", rec.defect);
    assert_eq!(
        rec.digest, 3_142_266_577,
        "seed 1005: the pinned BASE digest"
    );
    let r2 = rec
        .rounds
        .get(2)
        .unwrap_or_else(|| panic!("seed 1005: three rounds"));
    assert_eq!(r2.crash, "Before0@2", "seed 1005 r2: the pinned hook crash");
    assert!(r2.fired, "seed 1005 r2: the crash fired");
    assert!(r2.szc, "the open checkpoint's fold was 209 >= 184");
    assert!(r2.szq, "the final recovery opened a store");
    assert!(
        !r2.szi,
        "the crash at Before0 is before step 0 completes: nothing of the \
         checkpoint has happened, so szi must stay false (M-SZI-BEFORE0 reverts this)"
    );
    assert!(
        r2.pass,
        "seed 1005 r2: the round passes (o1={:?} o2={:?} o3={:?})",
        r2.o1_fail, r2.o2_fail, r2.o3_fail
    );
}

/// **X5-R39 pin: the `After5` literal config — `szc` and `szq` true,
/// `szi` false.** Seed 344 round 1, under `Tuning::BASE`: the crash
/// (`After5@1`) fired with the checkpoint's fold at 173 ≥ 4·F = 172 and
/// the final recovery opening a store; but step 5's `sync_all` had
/// returned and the meta was durable (`meta_durable_k` set, the
/// interval closed), so `szi` is false. **M-SZI-AFTER5** (never cleared
/// at step 5) reads the position code 8 as inside the window and fails
/// this pin (`szi` true).
#[test]
fn x5_r39_after5_crash_never_sets_szi() {
    let rec = workload::run_seed_with(344, None, &workload::Tuning::BASE);
    assert!(rec.defect.is_none(), "seed 344: {:?}", rec.defect);
    assert_eq!(
        rec.digest, 3_836_486_934,
        "seed 344: the pinned BASE digest"
    );
    let r1 = rec
        .rounds
        .get(1)
        .unwrap_or_else(|| panic!("seed 344: two rounds"));
    assert_eq!(r1.crash, "After5@1", "seed 344 r1: the pinned hook crash");
    assert!(r1.fired, "seed 344 r1: the crash fired");
    assert!(r1.szc, "the checkpoint's fold was 173 >= 172");
    assert!(r1.szq, "the final recovery opened a store");
    assert!(
        !r1.szi,
        "the crash at After5 is past step 5's sync_all: the meta is durable \
         and the interval closed, so szi must stay false (M-SZI-AFTER5 reverts this)"
    );
    assert!(
        r1.pass,
        "seed 344 r1: the round passes (o1={:?} o2={:?} o3={:?})",
        r1.o1_fail, r1.o2_fail, r1.o3_fail
    );
}

/// **X5-R39 pin: the refused round — `szc` true, `szq` false.** Seed
/// 9149 round 2, under `Tuning::BASE`: the crash (`After5@1`) cut a
/// checkpoint interval (the fold was 224 ≥ 4·F = 172) but the round's
/// final recovery produced **no store** (Q18's `end.is_none()` — a
/// refused or failed-open round), so `szq` is false: PLAN §10 item
/// 10's exclusion — "a refused round tests detection, not the recovery
/// under pressure". **M-SZQ-REFUSED** (the `end.is_some()` term
/// dropped) flips this round's `szq` to true and fails this pin.
/// (Landed with step 2's commit: the literal search that found it —
/// 9,149 seeds in — overran step 1's attempt; the pin is step 1's.)
#[test]
fn x5_r39_a_refused_round_is_szc_but_never_szq() {
    let rec = workload::run_seed_with(9149, None, &workload::Tuning::BASE);
    assert!(rec.defect.is_none(), "seed 9149: {:?}", rec.defect);
    assert_eq!(
        rec.digest, 3_826_847_088,
        "seed 9149: the pinned BASE digest"
    );
    let r2 = rec
        .rounds
        .get(2)
        .unwrap_or_else(|| panic!("seed 9149: three rounds"));
    assert_eq!(r2.crash, "After5@1", "seed 9149 r2: the pinned hook crash");
    assert!(r2.fired, "seed 9149 r2: the crash fired");
    assert!(r2.szc, "the checkpoint's fold was 224 >= 172");
    assert!(
        !r2.szq,
        "the final recovery produced no store: szq must stay false \
         (M-SZQ-REFUSED reverts this)"
    );
    assert!(!r2.szi, "szi conjoins szq");
    // And the refusal flag itself (X5-R46) is recorded on the round.
    assert!(r2.rfs, "a round whose recovery opened nothing is rfs");
}

/// **X5-R46 pin: the round-level refusal flag and its per-round
/// comparison.** Two records with identical digests whose rounds differ
/// only in `rfs`: the seed counts as non-reproducing (the field stays
/// out of the digest; the file-versus-re-run comparison is its guard).
#[test]
fn x5_r46_flipped_rfs_fails_reproduction() {
    let rec = |rfs: bool| workload::SeedRec {
        seed: 9,
        digest: 4242,
        rounds: vec![workload::RoundRec {
            rfs,
            ..workload::RoundRec::default()
        }],
        ..workload::SeedRec::default()
    };
    let (bad, _) = campaign::verify_digest_report(&[rec(false)], &[rec(false)]);
    assert_eq!(bad, 0, "identical records reproduce");
    let (bad, list) = campaign::verify_digest_report(&[rec(true)], &[rec(false)]);
    assert_eq!(bad, 1, "a flipped rfs counts the seed as non-reproducing");
    assert_eq!(list, vec![9]);
}

/// **X5-R39 pin: a flipped `szi` (or `szq`) fails reproduction.** Two
/// records with identical digests whose rounds differ only in `szi`
/// (or `szq`): the seed counts as non-reproducing — the fields stay out
/// of the digest (X5-R37's BASE literals must not move), so the
/// file-versus-re-run comparison is their only guard.
/// **M-SZ-NOREPRO** (the comparison removed) fails this pin.
#[test]
fn x5_r39_flipped_szi_fails_reproduction() {
    let rec = |szq: bool, szi: bool| workload::SeedRec {
        seed: 7,
        digest: 12345,
        rounds: vec![workload::RoundRec {
            szq,
            szi,
            ..workload::RoundRec::default()
        }],
        ..workload::SeedRec::default()
    };
    let same = (rec(true, true), rec(true, true));
    let (bad, _) = campaign::verify_digest_report(&[same.0], &[same.1]);
    assert_eq!(bad, 0, "identical records reproduce");
    let flipped = (rec(true, true), rec(true, false));
    let (bad, list) = campaign::verify_digest_report(&[flipped.0], &[flipped.1]);
    assert_eq!(
        bad, 1,
        "a flipped szi must count the seed as non-reproducing (M-SZ-NOREPRO reverts this)"
    );
    assert_eq!(list, vec![7]);
    let flipped_q = (rec(true, false), rec(false, false));
    let (bad, _) = campaign::verify_digest_report(&[flipped_q.0], &[flipped_q.1]);
    assert_eq!(bad, 1, "a flipped szq counts the seed as non-reproducing");
}

// ---- card X-5c-F4-F2 step 2: X5-R44–R46, the sets and the modes ----

use e4_recovery::campaign::{
    CampaignSet, RunForm, TuningId, is_exit_row, mp_refusal_line, probe_mp_seeds, run_form,
    run_form_set, verify_quota_missed, verify_recorded_set_ok, verify_seed_set_for,
};

/// A sorted record set for a seed list (identity digests; the checks
/// read seeds only).
fn set_recs(seeds: &[u64]) -> Vec<workload::SeedRec> {
    seeds
        .iter()
        .map(|&s| workload::SeedRec {
            seed: s,
            ..workload::SeedRec::default()
        })
        .collect()
}

/// **X5-R44 pin: the three seed sets, exactly** — `main` is `0..10000`,
/// `held-out` is `10000..20000`, and `mp-pressure` is the 2,000 seeds
/// `s` in `20000..30000` with `s % 10 ∈ {7, 8}` (the MP class), each
/// named as the `--set` value. **M-MP-CLASS** (the set takes every seed
/// in the range: 10,000 seeds) and **M-HELD-RANGE** (held-out accepts
/// `0..10000`) fail the mp-pressure and held-out halves.
#[test]
fn x5_r44_the_three_seed_sets() {
    let main = CampaignSet::Main.seeds();
    assert_eq!(main.len(), 10_000);
    assert_eq!(&main[..3], &[0, 1, 2]);
    assert_eq!(*main.last().unwrap_or(&u64::MAX), 9_999);
    let held = CampaignSet::HeldOut.seeds();
    assert_eq!(held.len(), 10_000);
    assert_eq!(held.first().copied(), Some(10_000));
    assert_eq!(*held.last().unwrap_or(&u64::MAX), 19_999);
    let mp = CampaignSet::MpPressure.seeds();
    assert_eq!(mp.len(), 2_000, "20 seeds per 100 in 20000..30000");
    assert!(mp.iter().all(|s| (20_000..30_000).contains(s)));
    assert!(mp.iter().all(|s| s % 10 == 7 || s % 10 == 8));
    assert_eq!(mp.first().copied(), Some(20_007));
    assert_eq!(*mp.last().unwrap_or(&u64::MAX), 29_998);
    // The names round-trip.
    for set in [
        CampaignSet::Main,
        CampaignSet::HeldOut,
        CampaignSet::MpPressure,
    ] {
        assert_eq!(CampaignSet::from_name(set.name()), Some(set));
    }
    assert_eq!(CampaignSet::from_name("bogus"), None);
}

/// **X5-R44 pin: each mode's seed-set check at one seed short, one seed
/// over and one wrong class.** The check is exact set equality
/// ([`verify_seed_set_for`]): dropping the last seed (short), adding
/// one past the end (over), or swapping in a seed outside the set
/// (wrong class — a non-MP seed inside `mp-pressure`'s range) each
/// fails, and the exact list passes.
#[test]
fn x5_r45_seed_set_checks_short_over_wrong_class() {
    for set in [
        CampaignSet::Main,
        CampaignSet::HeldOut,
        CampaignSet::MpPressure,
    ] {
        let seeds = set.seeds();
        assert!(
            verify_seed_set_for(set, &set_recs(&seeds)),
            "{}: the exact list passes",
            set.name()
        );
        // One short.
        let short = &seeds[..seeds.len() - 1];
        assert!(
            !verify_seed_set_for(set, &set_recs(short)),
            "{}: one seed short must fail",
            set.name()
        );
        // One over.
        let mut over = seeds.clone();
        over.push(over[over.len() - 1] + 1);
        assert!(
            !verify_seed_set_for(set, &set_recs(&over)),
            "{}: one seed over must fail",
            set.name()
        );
    }
    // Wrong class, mp-pressure: a seed of the range that is not MP.
    let mp = CampaignSet::MpPressure.seeds();
    let mut wrong = mp.clone();
    wrong[0] = 20_000; // 20000 % 10 == 0: class C, inside the range
    assert!(
        !verify_seed_set_for(CampaignSet::MpPressure, &set_recs(&wrong)),
        "mp-pressure: a non-MP seed of the range must fail (M-MP-CLASS reverts this)"
    );
    // Wrong class, held-out: a main seed in a held-out check
    // (M-HELD-RANGE makes held-out's list 0..10000, which this then
    // accepts).
    let wrong_held: Vec<u64> = (0..10_000).collect();
    assert!(
        !verify_seed_set_for(CampaignSet::HeldOut, &set_recs(&wrong_held)),
        "held-out: 0..10000 must fail (M-HELD-RANGE reverts this)"
    );
}

/// **X5-R45 pin: the per-set exit rows.** `main` judges every row
/// except `SZC-MP` and `SIZING` (plus Q4-I); `held-out` judges none;
/// `mp-pressure` judges `SZC-MP` only. **M-MP-MAINEXIT** (`SZC-MP`
/// exits in `main`), **M-MP-NOEXIT** (`SZC-MP` never exits in
/// `mp-pressure`) and **M-HELD-QUOTA** (held-out exits on a quota)
/// each fail their half.
#[test]
fn x5_r45_exit_rows_per_set() {
    assert!(is_exit_row(CampaignSet::Main, "Q4"));
    assert!(is_exit_row(CampaignSet::Main, "Q18"));
    assert!(is_exit_row(CampaignSet::Main, "SZC"));
    assert!(is_exit_row(CampaignSet::Main, "SZC-MW"));
    assert!(is_exit_row(CampaignSet::Main, "SZI"));
    assert!(is_exit_row(CampaignSet::Main, "Q4-I"), "Q4-I exits in main");
    assert!(
        !is_exit_row(CampaignSet::Main, "SZC-MP"),
        "SZC-MP is the mp-pressure set's own row (M-MP-MAINEXIT reverts this)"
    );
    assert!(
        !is_exit_row(CampaignSet::Main, "SIZING"),
        "SIZING is per-seed reporting, never an exit row"
    );
    for id in [
        "Q4", "Q18", "SZC", "SZC-MP", "SZC-MW", "SZI", "SIZING", "Q4-I",
    ] {
        assert!(
            !is_exit_row(CampaignSet::HeldOut, id),
            "held-out judges no row ({id}; M-HELD-QUOTA reverts this)"
        );
    }
    assert!(
        is_exit_row(CampaignSet::MpPressure, "SZC-MP"),
        "SZC-MP is mp-pressure's one exit row (M-MP-NOEXIT reverts this)"
    );
    for id in ["Q4", "Q18", "SZC", "SZC-MW", "SZI", "SIZING", "Q4-I"] {
        assert!(
            !is_exit_row(CampaignSet::MpPressure, id),
            "mp-pressure judges SZC-MP only ({id})"
        );
    }
}

/// **X5-R45 pins: SZC-MP at 99/100 in `mp-pressure`, and 99 in `main`
/// exits 0.** A literal table with every other row met and SZC-MP one
/// short: in `mp-pressure` the miss counts (exit non-zero); in `main`
/// SZC-MP is INFO and the exit is 0.
#[test]
fn x5_r45_szc_mp_99_in_each_mode() {
    let met = |id: &'static str, n: u64| workload::QuotaRow {
        id,
        what: id.to_owned(),
        achieved: n,
        required: n,
    };
    let mut rows: Vec<_> = ["Q4", "Q18", "SZC", "SZC-MW", "SZI", "SIZING"]
        .iter()
        .map(|id| met(Box::leak(id.to_string().into_boxed_str()), 1))
        .collect();
    rows.push(workload::QuotaRow {
        id: "SZC-MP",
        what: "MP szq rounds".to_owned(),
        achieved: 99,
        required: 100,
    });
    assert_eq!(
        verify_quota_missed(CampaignSet::MpPressure, &rows),
        1,
        "SZC-MP at 99 misses in mp-pressure (M-MP-NOEXIT reverts this)"
    );
    assert_eq!(
        verify_quota_missed(CampaignSet::Main, &rows),
        0,
        "SZC-MP at 99 never fails main's exit (M-MP-MAINEXIT reverts this)"
    );
    // At the floor, mp-pressure meets too.
    let rows100 = vec![workload::QuotaRow {
        id: "SZC-MP",
        what: "MP szq rounds".to_owned(),
        achieved: 100,
        required: 100,
    }];
    assert_eq!(verify_quota_missed(CampaignSet::MpPressure, &rows100), 0);
}

/// **X5-R45 pin: a held-out file with every quota short exits 0.**
/// `held-out` judges no row, so a table with every row short still
/// misses nothing. **M-HELD-QUOTA** reverts this.
#[test]
fn x5_r45_held_out_exits_zero_with_every_quota_short() {
    let short = |id: &'static str| workload::QuotaRow {
        id,
        what: id.to_owned(),
        achieved: 0,
        required: 100,
    };
    let rows = vec![
        short("Q4"),
        short("Q18"),
        short("SZC"),
        short("SZC-MP"),
        short("SZC-MW"),
        short("SZI"),
    ];
    assert_eq!(verify_quota_missed(CampaignSet::HeldOut, &rows), 0);
    assert_eq!(
        campaign::verify_exit_code(
            true,
            0,
            verify_quota_missed(CampaignSet::HeldOut, &rows),
            false
        ),
        0,
        "held-out exits 0 with every quota short (M-HELD-QUOTA reverts this)"
    );
}

/// **X5-R45 pins: a mismatched `--set` is refused, and a file with no
/// set is refused** ([`verify_recorded_set_ok`], pure — the refusal
/// path re-runs nothing, which is why the pin is on the predicate the
/// command calls before anything runs). **M-SET-MISMATCH** (`verify`
/// ignores the file's set) reverts this. **M-PROBE-SET-NAME** (the
/// probe form records a set) is caught by the no-set half through the
/// emit: a probe file must carry no `set` key.
#[test]
fn x5_r45_the_recorded_set_gates_verify() {
    assert!(verify_recorded_set_ok(Some("main"), CampaignSet::Main));
    assert!(verify_recorded_set_ok(
        Some("held-out"),
        CampaignSet::HeldOut
    ));
    assert!(verify_recorded_set_ok(
        Some("mp-pressure"),
        CampaignSet::MpPressure
    ));
    assert!(
        !verify_recorded_set_ok(Some("main"), CampaignSet::HeldOut),
        "a mismatched --set is refused (M-SET-MISMATCH reverts this)"
    );
    assert!(
        !verify_recorded_set_ok(None, CampaignSet::Main),
        "a file with no set is refused"
    );
    assert!(!verify_recorded_set_ok(Some("bogus"), CampaignSet::Main));
    // The emit side: a --set run records its set, a probe records none
    // (M-PROBE-SET-SET-NAME's shape would put a set in the probe file).
    let recs = vec![workload::SeedRec {
        seed: 1,
        digest: 2,
        ..workload::SeedRec::default()
    }];
    let set_json = campaign::emit_campaign_for(&recs, Some(&CampaignSet::Main));
    assert!(
        set_json.starts_with("{\"e4\":1,\"set\":\"main\",\"seeds\":["),
        "the set form records the set: {set_json}"
    );
    let probe_json = campaign::emit_campaign_for(&recs, None);
    assert!(
        !probe_json.contains("\"set\":"),
        "a probe file records no set (M-PROBE-SET-NAME reverts this)"
    );
    // And the parser reads the set back.
    let (set, back) = campaign::parse_campaign_with_set(&set_json).expect("parse");
    assert_eq!(set.as_deref(), Some("main"));
    assert_eq!(back.len(), 1);
    let (set, _) = campaign::parse_campaign_with_set(&probe_json).expect("parse");
    assert_eq!(set, None);
}

/// **X5-R44 pin: `tuning_id()` is `MpPressure` for that set only**, and
/// `MP_PRESSURE` equals `FINAL` at this tip (Limit 2 writes it as
/// `Tuning { ..Tuning::FINAL }`, so it starts equal and follows).
/// **M-MP-TUNING** (`mp-pressure` selects `TuningId::Final`) fails the
/// id half; because the constants are equal here, only the id can show
/// which one a set selects.
#[test]
fn x5_r44_tuning_id_is_mp_pressure_for_that_set_only() {
    assert_eq!(CampaignSet::Main.tuning_id(), TuningId::Final);
    assert_eq!(CampaignSet::HeldOut.tuning_id(), TuningId::Final);
    assert_eq!(
        CampaignSet::MpPressure.tuning_id(),
        TuningId::MpPressure,
        "M-MP-TUNING reverts this"
    );
    assert_eq!(workload::Tuning::MP_PRESSURE, workload::Tuning::FINAL);
    assert_eq!(workload::Tuning::FINAL, workload::Tuning::BASE);
    // The constants are equal at this tip, so the id is the only
    // discriminator (the linker may even fold them into one constant);
    // `tuning()` matches on the id, and F4-F3's retune of FINAL moves
    // MP_PRESSURE with it.
}

/// **X5-R46 pin: `MP_REFUSAL` on a literal record set with mixed
/// classes** — class MP only: two MP rounds, one refused, beside C and
/// MW rounds whose `rfs` never counts. **M-REFUSAL-ALL** (every class
/// counted) reads rounds=4 refused=3 and fails the exact line.
#[test]
fn x5_r46_mp_refusal_counts_class_mp_only() {
    let round = |rfs: bool| workload::RoundRec {
        rfs,
        ..workload::RoundRec::default()
    };
    let recs = vec![
        workload::SeedRec {
            seed: 1,
            class: "C".to_owned(),
            rounds: vec![round(true)],
            ..workload::SeedRec::default()
        },
        workload::SeedRec {
            seed: 2,
            class: "MP".to_owned(),
            rounds: vec![round(true), round(false)],
            ..workload::SeedRec::default()
        },
        workload::SeedRec {
            seed: 3,
            class: "MW".to_owned(),
            rounds: vec![round(true)],
            ..workload::SeedRec::default()
        },
    ];
    assert_eq!(
        mp_refusal_line(&recs),
        "MP_REFUSAL rounds=2 refused=1 rate=0.5000",
        "M-REFUSAL-ALL reverts this (it would read rounds=4 refused=3 rate=0.7500)"
    );
    // A set with no MP rounds reads zero, never a division by zero.
    assert_eq!(
        mp_refusal_line(&recs[..1]),
        "MP_REFUSAL rounds=0 refused=0 rate=0.0000"
    );
}

/// **REFRESH 9 pins (card step 2): the two probe forms' seed lists and
/// refusals.** The MP-pressure probe runs exactly the 200 MP-class
/// seeds of `30000..31000` (`s % 10 ∈ {7, 8}`); the range probe is
/// refused at any bound reaching 10000 or above, and the forms refuse
/// every conflicting flag combination. **M-PROBE-RANGE** (the probe
/// form accepts a range reaching 10000) fails the bound half.
#[test]
fn x5_r44_probe_forms_seed_lists_and_refusals() {
    let probe = probe_mp_seeds();
    assert_eq!(probe.len(), 200);
    assert_eq!(probe.first().copied(), Some(30_007));
    assert_eq!(*probe.last().unwrap_or(&u64::MAX), 30_998);
    assert!(probe.iter().all(|s| (30_000..31_000).contains(s)));
    assert!(probe.iter().all(|s| s % 10 == 7 || s % 10 == 8));
    // The forms.
    let a = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    // Neither flag means main (the set is recorded).
    assert_eq!(
        run_form(None, None, None),
        Ok(RunForm::Set(CampaignSet::Main))
    );
    // --set dispatch.
    assert_eq!(
        run_form(Some("held-out"), None, None),
        Ok(RunForm::Set(CampaignSet::HeldOut))
    );
    assert_eq!(
        run_form(Some("main"), None, Some((0, 10_000))),
        Ok(RunForm::Set(CampaignSet::Main)),
        "--set main may name its own range"
    );
    assert!(run_form(Some("main"), None, Some((0, 500))).is_err());
    assert!(
        run_form(Some("mp-pressure"), None, Some((20_007, 20_009))).is_err(),
        "mp-pressure is not a range: --seeds with it is always refused"
    );
    assert!(run_form(Some("bogus"), None, None).is_err());
    // The probe range: below 10000 runs FINAL and records no set.
    assert_eq!(
        run_form(None, None, Some((9_900, 9_999))),
        Ok(RunForm::ProbeRange(9_900, 9_999))
    );
    assert!(
        run_form(None, None, Some((0, 10_000))).is_err(),
        "a range reaching 10000 is refused (M-PROBE-RANGE reverts this)"
    );
    assert!(
        run_form(None, None, Some((9_999, 10_001))).is_err(),
        "a range reaching past 10000 is refused"
    );
    assert!(
        run_form(None, None, Some((5, 5))).is_err(),
        "empty is refused"
    );
    // The mp-pressure probe refuses --set and --seeds.
    assert_eq!(
        run_form(None, Some("mp-pressure"), None),
        Ok(RunForm::ProbeMp)
    );
    assert!(run_form(Some("main"), Some("mp-pressure"), None).is_err());
    assert!(run_form(None, Some("mp-pressure"), Some((0, 5))).is_err());
    assert!(run_form(None, Some("bogus"), None).is_err());
    // The probe forms record no set (M-PROBE-SET-NAME reverts this).
    assert_eq!(run_form_set(RunForm::ProbeRange(9_900, 9_999)), None);
    assert_eq!(run_form_set(RunForm::ProbeMp), None);
    assert_eq!(
        run_form_set(RunForm::Set(CampaignSet::Main)),
        Some(CampaignSet::Main)
    );
    // Command level: the refusals exit 2 before anything runs (the
    // 0..10000 bound is pinned at the pure level, so a mutant that
    // shifts it cannot turn this loop into a 10k-seed run).
    let out = "/tmp/opencode/x5f2-probe-refusal.json";
    for args in [
        a(&["--seeds", "5000..10001", "--out", out]),
        a(&["--probe", "mp-pressure", "--seeds", "0..5", "--out", out]),
        a(&["--probe", "mp-pressure", "--set", "main", "--out", out]),
        a(&["--set", "mp-pressure", "--seeds", "0..100", "--out", out]),
        a(&["--set", "bogus", "--out", out]),
    ] {
        let mut full = vec!["run".to_string()];
        full.extend(args.iter().cloned());
        assert_eq!(campaign::main(&full), 2, "the refusal exits 2: {args:?}");
    }
}

// ---- card X-5c-F4-F2 step 3: the JOINT_ARM_CENSUS (P ruling G5-CENSUS (c)) ----

/// A record with per-round census fields (`twin_joint_arm`,
/// `twin_joint_stale_reads`), one entry per executed round.
fn census_rec(seed: u64, arms: Vec<bool>, stale: Vec<usize>) -> workload::SeedRec {
    workload::SeedRec {
        seed,
        twin_joint_arm: arms,
        twin_joint_stale_reads: stale,
        ..workload::SeedRec::default()
    }
}

/// **G5-CENSUS (c) pins (card step 3), on literal record sets.** The
/// line is pure in `(set, &[SeedRec])`: `seeds` counts the records
/// (never the set's requested count — **M-CENSUS-NOTFROMRUN** reverts
/// this), `arm_rounds` counts true `twin_joint_arm` entries
/// (**M-CENSUS-ZERO** reverts this), `stale_reads` sums
/// `twin_joint_stale_reads` (**M-CENSUS-NOSTALE** reverts this), and
/// `arm_seeds` is ascending even when the records arrive unsorted.
/// All zero reads the exact all-zero line of G5-CENSUS (d).
#[test]
fn x5f2s3_joint_arm_census_line() {
    use e4_recovery::campaign::joint_arm_census_line;
    // All zero: the known reading at BASE (G5-CENSUS (a)).
    assert_eq!(
        joint_arm_census_line(
            CampaignSet::Main,
            &[
                census_rec(0, vec![false, false], vec![0, 0]),
                census_rec(1, vec![false], vec![0]),
            ]
        ),
        "JOINT_ARM_CENSUS set=main seeds=2 arm_rounds=0 stale_reads=0 arm_seeds=[]"
    );
    // One live arm (seed 5 round 1) and one stale read elsewhere: the
    // arm_rounds, stale_reads and arm_seeds terms each move on their
    // own.
    assert_eq!(
        joint_arm_census_line(
            CampaignSet::MpPressure,
            &[
                census_rec(5, vec![false, true], vec![0, 0]),
                census_rec(20_007, vec![false], vec![3]),
            ]
        ),
        "JOINT_ARM_CENSUS set=mp-pressure seeds=2 arm_rounds=1 stale_reads=3 arm_seeds=[5]"
    );
    // Unsorted input prints ascending seeds, and the count is taken
    // from the records (three records of main's ten thousand).
    assert_eq!(
        joint_arm_census_line(
            CampaignSet::Main,
            &[
                census_rec(900, vec![true], vec![1]),
                census_rec(7, vec![true, true], vec![0, 2]),
                census_rec(100, vec![false], vec![0]),
            ]
        ),
        "JOINT_ARM_CENSUS set=main seeds=3 arm_rounds=3 stale_reads=3 arm_seeds=[7,900]"
    );
    assert_eq!(
        joint_arm_census_line(CampaignSet::HeldOut, &[]),
        "JOINT_ARM_CENSUS set=held-out seeds=0 arm_rounds=0 stale_reads=0 arm_seeds=[]"
    );
}
