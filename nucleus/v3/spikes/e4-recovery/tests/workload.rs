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
/// **X-5c-F4-F1-F1-F3-F2-F1 (X5-R49, step 2):** the joint spec's draw
/// moved `Mid2@4 -> Mid2@3` — checkpoint 1's slot numbering shifted
/// when freed homes became allocatable, so the per-index AND of the two
/// complete mask lists re-derives `Mid2@3`; the twin and the real
/// round both still fire there (measured at this tip: 2 rounds, no
/// defect, round 1 `Mid2@3` fired and passing).
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
        "seed 7469: round 1 must run the joint spec (X5-R48)"
    );
    assert!(
        rec.rounds[1].fired,
        "seed 7469: round 1's joint-spec crash must fire"
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
