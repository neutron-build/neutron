//! C-T6c1a's soak, carrying C-T6c1b's checks and C-T6d2's large-value
//! surface: seeded multi-session histories over `nucleus-catalog::Db` on
//! `Core<MemKv>` — 4 DML sessions at RC/RR/SER over `t(pk int8 PK, a
//! int8, b text, big text)` (pk ∈ 0..8; `big` takes 100/3000/20000-byte
//! values and NULLs, C-T6d2 §5) and `u(a int8, b text)` with its hidden
//! rowid, plus a DDL session issuing ADD COLUMN, DROP COLUMN, SET/DROP
//! NOT NULL, TRUNCATE, DROP COLUMN big and its re-add, each in its own
//! txn — checked against the sequential reference model (`tests/diff/`):
//! every read at its snapshot plus own writes (c1a), every write effect
//! re-derived at its commit position, duplicates refused, SET NOT NULL
//! outcomes validated, the committed SERIALIZABLE sub-history's
//! serialization graph acyclic (c1b), amcheck — I-TOAST included (d2) —
//! at three mid-run snapshots and at the end, and final-state scans.
//!
//! The size is an iteration count (3 fixed seeds × 200 txns per session),
//! never a wall-clock bound; timings are printed, not asserted. The
//! `#[ignore]`d long variant runs 50 seeds.
//!
//! C-T6e4 stabilization — the coverage floors (the bars over the
//! checker's Stats below) ride the racy threaded workload, and a
//! scheduling draw can starve one while the engine stays clean
//! (violations: 0, no leaks): the C-T6d2fix gate's H-1 census measured
//! ~16% of runs starving a bar on a loaded box, and this card's
//! acceptance loop caught the DeleteOnly-sight floor starving with all
//! three seeds at 0 on a legal history. The floors keep their values
//! and messages; `t6c_soak` instead REDRAWS a starved draw — same
//! seeds (programs are a pure function of the seed), fresh
//! interleaving, at most 4 draws — while correctness (`assert_clean`,
//! leaked intents, the amcheck point count) asserts on EVERY draw and
//! is never redrawn: the XX001 families fail on draw 1, and a
//! deterministic coverage regression starves every draw and fails with
//! the list.
//!
//! Mutation evidence (binding, card item 5): the four card-named mutants
//! are each caught —
//!
//! - **E1** — `decode_row` ignoring `missing` (nucleus-codec): every
//!   read of a row older than an ADD COLUMN with a default renders NULL
//!   where the model renders the default → a Get/ScanT mismatch. Caught
//!   by this soak failing in 5 of 5 runs.
//! - **E2** — `open_table` skipping the RR storage-id check
//!   (nucleus-catalog): a successful RR/SER read over the post-TRUNCATE
//!   storage returns rows the model at `S` does not have (or misses the
//!   ones it does) → a mismatch, plus the explicit TRUNCATE-window
//!   violation. Caught by this soak failing in 5 of 5 runs.
//! - **E3** — `catalog_alter_rel_def` locking RowExclusive instead of
//!   AccessExclusive (nucleus-txn): a writer no longer blocks behind an
//!   in-flight ALTER and encodes at the stale definition. Caught
//!   deterministically by `t6c_regress`'s parked-DDL schedule.
//! - **E4** — writes encoding at `version - 1` (the row header's
//!   schema_version): caught deterministically by `t6c_regress`'s
//!   single-thread ADD COLUMN → insert → decode check.
//!
//! C-T6e4fix (the gate's L-2): the redraw's fail-path is pinned by the
//! `#[ignore]`d `t6c_soak_stab_fail_exhausts_draws_and_panics_with_the_list`
//! — an unmeetable floor (one bar past the observed range, added to the
//! real floors) must exhaust the 4 draws and panic with the full starved
//! list, so a regression that makes starved floors stop failing cannot
//! pass silently (STAB-DROP's catcher). The draw loop lives in
//! `soak_draws`, shared by the soak and the leg; every bar, message and
//! the draw bound are unchanged by the extraction.

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use diff::runner::run_workload;
use diff::{check, gen_ddl, gen_prog, Config, PDdl, Run, Verdict};

/// The soak's three fixed seeds.
const SEEDS: [u64; 3] = [0x7c1a_0001, 0x7c1a_0002, 0x7c1a_0003];
/// The long variant's fifty.
const LONG_SEEDS: u64 = 50;
const LONG_BASE: u64 = 0x7c1a_1000;
/// A pk outside the workload's 0..8 domain, for the phantom final row
/// the can-fail test plants when a late truncate left `t` empty.
const PHANTOM_PK: i64 = 77;

fn run_seed(seed: u64) -> (Run, Verdict) {
    let cfg = Config::new(seed);
    let t0 = std::time::Instant::now();
    let run = run_workload(&cfg);
    let t1 = std::time::Instant::now();
    // The merged checker's every check — the write-side TRUNCATE-window
    // pass (c1b FU-B) and the per-level read split (c1a FU-5 / c1b
    // FU-C) folded into `diff::check` by C-T6c1afix2 FU-2.
    let verdict = check(&run);
    // Reported, never asserted: no wall-clock verdicts.
    println!(
        "t6c soak seed {seed:#x}: run {:?}, check {:?}, leaked {}",
        t1 - t0,
        t1.elapsed(),
        run.leaked_intents
    );
    (run, verdict)
}

fn summary(seed: u64, v: &Verdict) {
    let s = &v.stats;
    println!(
        "t6c soak seed {seed:#x}: {} txns ({} committed, {} failed, {} retried); \
         {} reads checked (RC {}, RR {}, SER {}); {} writes; {} DDL ({} truncates committed); \
         {} amcheck points; c1b: {} writes checked ({} effects recomputed, \
         {} RC carve-outs), {} SET NOT NULL verified ({} skipped), \
         {} SER txns / {} reads / {} wr + {} rw edges; c1d2: {} big writes \
         (lens {}/{}/{}), {} big drops / {} adds; c1e3: {} builds ok / {} failed, \
         {} resumes ({} no-op), {} drops, {} index scans ({} served, {} refused, \
         {} absent, {} no-target), sights DO {}/WO {}; violations: {}",
        s.txns,
        s.committed,
        s.failed,
        s.retryable_failures,
        s.reads_checked,
        s.reads_rc,
        s.reads_rr,
        s.reads_ser,
        s.writes,
        s.ddl,
        s.truncates_committed,
        s.amcheck_points,
        s.writes_checked,
        s.effects_recomputed,
        s.rc_window_carveouts,
        s.snn_checked,
        s.snn_23502_skipped,
        s.ser_txns,
        s.ser_reads,
        s.ser_wr_edges,
        s.ser_rw_edges,
        s.big_writes,
        s.big_lens[0],
        s.big_lens[1],
        s.big_lens[2],
        s.big_drops,
        s.big_adds,
        s.idx_builds_ok,
        s.idx_builds_failed,
        s.idx_resumes,
        s.idx_resumes_noop,
        s.idx_drops,
        s.idx_scans,
        s.idx_scans_served,
        s.idx_scans_refused,
        s.idx_scans_absent,
        s.idx_scans_noop,
        s.idx_sights_delete_only,
        s.idx_sights_write_only,
        v.violations.len()
    );
}

/// Fails with the verdict report when the run is not clean.
fn assert_clean(seed: u64, verdict: &Verdict) {
    if !verdict.is_clean() {
        panic!(
            "t6c soak seed {seed:#x} is not clean:\n{}",
            verdict.report()
        );
    }
}

/// The per-seed coverage floors, as messages (empty = this seed's draw
/// exercised every surface). The bars and their messages are unchanged
/// from the C-T6c1a/c1b/d2/d2fix/e3 cards; what C-T6e4 changed is only
/// who evaluates them — a starved floor no longer fails the run
/// outright, it redraws the draw (`t6c_soak`'s loop, module docs).
/// Correctness is NOT here: `assert_clean`, leaked intents and the
/// amcheck point count assert on every draw and are never redrawn.
fn seed_coverage_floors(seed: u64, v: &Verdict) -> Vec<String> {
    let s = &v.stats;
    let mut starved = Vec::new();
    // The workload actually exercised the surfaces under test: reads
    // were checked, DDL and truncates committed, and commits happened
    // at every level (the mix generates all three).
    if s.reads_checked <= 100 {
        starved.push(format!("too few reads checked: {s:?}"));
    }
    // The level split (c1a FU-5 / c1b FU-C, folded into the checker's
    // Stats by C-T6c1afix2 FU-2): successful reads must happen at every
    // level — an over-refusing engine that 40001'd all RR/SER reads
    // starves two of the three counters (refused txns leave no
    // effects, so nothing else would notice).
    if s.reads_rc <= 50 {
        starved.push(format!("too few RC reads checked: {s:?}"));
    }
    if s.reads_rr <= 50 {
        starved.push(format!("too few RR reads checked: {s:?}"));
    }
    if s.reads_ser <= 50 {
        starved.push(format!("too few SER reads checked: {s:?}"));
    }
    if s.truncates_committed <= 3 {
        starved.push(format!("too few truncates: {s:?}"));
    }
    if s.ddl <= 30 {
        starved.push(format!("too little DDL: {s:?}"));
    }
    if s.committed <= 200 {
        starved.push(format!("too few commits: {s:?}"));
    }
    // C-T6c1b's surfaces were exercised too: the write oracle ran, and
    // the serialization graph had SER txns to bite on (the mix makes
    // ~a third of the 800 programs SERIALIZABLE).
    if s.writes_checked <= 100 {
        starved.push(format!("too few writes checked: {s:?}"));
    }
    if s.ser_txns <= 5 {
        starved.push(format!("too few committed SER txns: {s:?}"));
    }
    // c1b FU-D: the SET NOT NULL surface stays exercised — a workload
    // or engine change that quietly stopped producing successful SET
    // NOT NULLs (or witnessed 23502s) would zero this without failing
    // anything else.
    if s.snn_checked == 0 {
        starved.push(format!("no SET NOT NULL verified: {s:?}"));
    }
    // C-T6d2fix FU-2 (gate F-2, recalibrated per the gate's H-1): the
    // write side is scheduling-sensitive after all — depressed seeds
    // under load commit as few as ~21 big writes and ~2 per class — so
    // the floors sit at > 10 / > 1 (every demonstrated mutant catch
    // survives with ~2x headroom: GA4 dies at 0, GA5's erosion lands
    // far below 10); only the drop/re-add pair stays aggregate, below.
    if s.big_writes <= 10 {
        starved.push(format!("too few big writes in seed {seed:#x}: {s:?}"));
    }
    if !s.big_lens.iter().all(|&n| n > 1) {
        starved.push(format!(
            "every big length class (100/3000/20000) must commit per seed \
             {seed:#x}: {s:?}"
        ));
    }
    // The DropBig→AddBig pairing (every drop's next DDL index is the
    // re-add) lands at least one committed AddBig per seed.
    if s.big_adds < 1 {
        starved.push(format!("no committed AddBig in seed {seed:#x}: {s:?}"));
    }
    starved
}

/// The three-seed aggregate coverage floors, as messages (empty = the
/// draw exercised every aggregate surface). Same bars and messages as
/// before the C-T6e4 draw loop; see `seed_coverage_floors`.
fn aggregate_coverage_floors(v: &Verdict) -> Vec<String> {
    let big = &v.stats;
    let mut starved = Vec::new();
    // C-T6d2's surface, aggregate side (FU-2): the drop/re-add cycle is
    // the scheduling-exposed half (4–6 drops per seed observed — whether
    // a given DropBig beats its DDL neighbours and commits is a race),
    // so its bar runs over the three seeds: the pair must land ≥ 4
    // times across them (every drop's forced re-add keeps the pair
    // count even), with at least one real committed drop. The per-seed
    // bars above carry the big-write, per-class and AddBig floors (each
    // implies the aggregate form it replaced).
    if big.big_drops + big.big_adds < 4 {
        starved.push(format!(
            "too few DropBig/AddBig pairs across the seeds: {big:?}"
        ));
    }
    if big.big_drops == 0 {
        starved.push(format!("no committed DropBig: {big:?}"));
    }
    // C-T6e3's surface: builds published and failed (the seeded
    // sabotage's 55000s), unfinished ones were resumed and dropped, and
    // the DML scans both served through a published index and were
    // refused by the ladder's mid-states — an engine that never
    // published, or one that never refused, starves one of each pair
    // (the served side is also the rows-equality oracle's only bite).
    if big.idx_builds_ok <= 3 {
        starved.push(format!("too few published online builds: {big:?}"));
    }
    if big.idx_builds_failed == 0 {
        starved.push(format!(
            "no failed online build (the seeded sabotage never bit): {big:?}"
        ));
    }
    if big.idx_resumes == 0 {
        starved.push(format!("no resume ran on a target: {big:?}"));
    }
    if big.idx_drops == 0 {
        starved.push(format!("no committed DROP INDEX: {big:?}"));
    }
    if big.idx_scans_served <= 10 {
        starved.push(format!("too few served index scans: {big:?}"));
    }
    if big.idx_scans_refused == 0 {
        starved.push(format!(
            "no justified index-scan refusal (the ladder's mid-states were \
             never seen mid-flight): {big:?}"
        ));
    }
    // The bracketed sights saw the ladder's first two states somewhere
    // in the run — the states amcheck's I-IDX-DO/I-IDX-WO arms check.
    if big.idx_sights_delete_only == 0 {
        starved.push(format!("no DeleteOnly sight: {big:?}"));
    }
    if big.idx_sights_write_only == 0 {
        starved.push(format!("no WriteOnly sight: {big:?}"));
    }
    starved
}

/// The soak's draw driver (C-T6e4's stabilization, extracted by
/// C-T6e4fix so the STAB-FAIL leg can inject its own floors): the
/// DRAWS-loop over the three fixed seeds with the coverage floors
/// injected by the caller — `t6c_soak` passes the real ones; the
/// `#[ignore]`d STAB-FAIL leg passes the real ones plus one unmeetable
/// bar to prove the fail-path. Everything else is the soak's own
/// contract, unchanged by the extraction: correctness — `assert_clean`,
/// leaked intents, the amcheck point count — asserts on EVERY draw and
/// is never redrawn; an empty starved list returns; exhaustion panics
/// with the last draw's full list.
fn soak_draws(
    seed_floors: &dyn Fn(u64, &Verdict) -> Vec<String>,
    aggregate_floors: &dyn Fn(&Verdict) -> Vec<String>,
) {
    // C-T6e4's draw loop. The coverage floors ride a racy threaded
    // workload: a scheduling draw can starve one while the engine stays
    // clean (violations: 0, no leaks, amcheck green) — the C-T6d2fix
    // gate's H-1 census measured ~16% of runs starving a bar at its
    // tree, and this card's acceptance loop watched the DeleteOnly-sight
    // floor starve with all three seeds at DO 0 on a legal history.
    // The floors may not be lowered (this card's limits), so the
    // stability mechanism is REDRAW: correctness — `assert_clean`,
    // leaked intents, the amcheck point count — asserts on EVERY draw
    // and is never redrawn (the XX001 families this lineage hunts die
    // there, on draw 1, never in a coverage floor), while a starved
    // floor redraws the whole three-seed soak — same seeds, same
    // programs (a pure function of the seed), a fresh interleaving —
    // up to DRAWS draws. A deterministic coverage regression (a
    // generator or engine change that genuinely stops exercising a
    // surface — the BAR-LOOSE / RANGE-SILENT / GA4 / GA5 class) starves
    // every draw and fails with the full list.
    const DRAWS: usize = 4;
    // C-T6d2's index-build coverage bars are taken over the three seeds
    // together: a build's fate depends on which truncate or drop raced
    // it, so per-seed floors would flake on interleavings, while the
    // aggregate floor is many times the bar. The large-value bars are
    // per-seed again (C-T6d2fix FU-2, gate F-2): observed runs commit
    // 60–117 big writes per seed and ≥ 9 per length class, so `> 10`
    // and `> 1` hold with margin — the C-T6d2 stabilization had moved
    // them to the 3-seed aggregate, which a regression that zeroes one
    // seed's big coverage passes silently.
    let mut starved_last: Vec<String> = Vec::new();
    for draw in 0..DRAWS {
        let mut big = diff::Verdict::default();
        let mut starved: Vec<String> = Vec::new();
        for seed in SEEDS {
            let (run, verdict) = run_seed(seed);
            summary(seed, &verdict);
            // Correctness — never redrawn.
            assert_clean(seed, &verdict);
            assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
            assert!(
                verdict.stats.amcheck_points == 8,
                "3 mid-run points × 2 tables + the end: {:?}",
                verdict.stats
            );
            starved.extend(seed_floors(seed, &verdict));
            // The aggregate of the large-value surface (Stats is Copy;
            // the per-field sums are what the bars below run on).
            let s = &verdict.stats;
            let acc = &mut big.stats;
            acc.big_writes += s.big_writes;
            for (agg, one) in acc.big_lens.iter_mut().zip(s.big_lens) {
                *agg += one;
            }
            acc.big_drops += s.big_drops;
            acc.big_adds += s.big_adds;
            // The same for C-T6e3's online-build surface (the c1d2
            // pattern: per-seed floors would flake on interleavings —
            // a build's fate depends on which truncate or drop raced it
            // — while the aggregate floor is many times the bar).
            acc.idx_builds_ok += s.idx_builds_ok;
            acc.idx_builds_failed += s.idx_builds_failed;
            acc.idx_resumes += s.idx_resumes;
            acc.idx_drops += s.idx_drops;
            acc.idx_scans_served += s.idx_scans_served;
            acc.idx_scans_refused += s.idx_scans_refused;
            acc.idx_sights_delete_only += s.idx_sights_delete_only;
            acc.idx_sights_write_only += s.idx_sights_write_only;
        }
        starved.extend(aggregate_floors(&big));
        if starved.is_empty() {
            return;
        }
        if draw + 1 < DRAWS {
            println!(
                "t6c soak draw {} of {DRAWS}: coverage starved (engine \
                 clean — violations 0, no leak), redrawing: {starved:#?}",
                draw + 1
            );
        }
        starved_last = starved;
    }
    panic!(
        "t6c soak: coverage floors starved on all {DRAWS} draws — not \
         scheduling luck but a surface the workload no longer exercises:\n\
         {starved_last:#?}"
    );
}

#[test]
fn t6c_soak() {
    soak_draws(&seed_coverage_floors, &aggregate_coverage_floors);
}

/// The long variant: 50 seeds, `#[ignore]`d (the card's item 5).
#[test]
#[ignore = "long: 50 seeds"]
fn t6c_soak_long() {
    for i in 0..LONG_SEEDS {
        let seed = LONG_BASE + i;
        let (run, verdict) = run_seed(seed);
        summary(seed, &verdict);
        assert_clean(seed, &verdict);
        assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
    }
}

/// C-T6e4fix (the gate's L-2): the redraw's fail-path, pinned. An
/// unmeetable floor — one bar raised past the observed range, the
/// STAB-FAIL shape (the C-T6e4 gate demonstrated it transiently; this
/// leg commits it) — must exhaust the draws and panic with the full
/// starved list, never silently pass. Without this leg a regression
/// that makes starved floors stop failing (an early `return`, the
/// starve-list check dropped, the panic removed) would convert a
/// coverage floor into a suggestion — the exact weakening class the
/// cards forbid — and nothing in-tree would notice (STAB-DROP's
/// catcher).
///
/// `#[ignore]d` because it deliberately runs four full three-seed draws
/// (~15 s) to reach the exhaustion; run it with `--ignored`. The
/// unmeetable bar is **added** to the real floors, never raised in
/// place — every real bar, value and message is untouched (the seed
/// floors' own comment records the observed 60–117 big writes per seed;
/// 100000 is past the range by construction).
#[test]
#[ignore = "STAB-FAIL demo: four deliberate draws, ~15 s; run with --ignored"]
fn t6c_soak_stab_fail_exhausts_draws_and_panics_with_the_list() {
    let stab_fail_seed_floors = |seed: u64, v: &Verdict| {
        let mut starved = seed_coverage_floors(seed, v);
        starved.push(format!(
            "STAB-FAIL bar: big_writes {} of 100000",
            v.stats.big_writes
        ));
        starved
    };
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        soak_draws(&stab_fail_seed_floors, &aggregate_coverage_floors);
    }));
    let err = panicked.expect_err(
        "the unmeetable floor was silently swallowed — starved floors \
         must exhaust the draws and fail, never pass (the redraw's \
         fail-path)",
    );
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("4 draws"),
        "the panic must name the draw exhaustion:\n{msg}"
    );
    assert!(
        msg.contains("STAB-FAIL bar"),
        "the panic must carry the starved list, the unmeetable bar in \
         it:\n{msg}"
    );
    assert!(
        msg.contains("not scheduling luck"),
        "the panic must keep its meaning — an exhausted floor is a \
         surface the workload no longer exercises:\n{msg}"
    );
}

// ----- determinism ------------------------------------------------------------

/// Programs are a pure function of `(seed, session, index)`: same triple,
/// same program; a different seed gives different programs.
#[test]
fn t6c_programs_are_a_pure_function_of_the_seed() {
    for session in 1..=5u16 {
        for idx in 0..60 {
            let a = gen_prog(0x1234, session, idx);
            let b = gen_prog(0x1234, session, idx);
            assert_eq!(a, b, "session {session} prog {idx}");
            let a = gen_ddl(0x1234, idx);
            let b = gen_ddl(0x1234, idx);
            assert_eq!(a, b, "ddl {idx}");
        }
    }
    let differs = (0..60).any(|i| gen_prog(0x1234, 1, i) != gen_prog(0x1235, 1, i));
    assert!(differs, "a different seed must give different programs");
    let ddl_differs = (0..60).any(|i| gen_ddl(0x1234, i) != gen_ddl(0x1235, i));
    assert!(ddl_differs, "a different seed must give different DDL");
    // The DDL mix actually contains every kind (each is issuable).
    let mut kinds = std::collections::BTreeSet::new();
    for i in 0..400 {
        match gen_ddl(0x1234, i) {
            PDdl::AddColumn(_) => {
                kinds.insert("add");
            }
            PDdl::DropColumn => {
                kinds.insert("drop");
            }
            PDdl::SetNotNull => {
                kinds.insert("snn");
            }
            PDdl::DropNotNull => {
                kinds.insert("dnn");
            }
            PDdl::Truncate => {
                kinds.insert("trunc");
            }
            PDdl::DropBig => {
                kinds.insert("dropbig");
            }
            PDdl::AddBig => {
                kinds.insert("addbig");
            }
            // C-T6e3: both CONCURRENTLY modes of the online build, and
            // its cleanup pair.
            PDdl::CreateIndex(true) => {
                kinds.insert("cidx_c");
            }
            PDdl::CreateIndex(false) => {
                kinds.insert("cidx_p");
            }
            PDdl::DropIndex => {
                kinds.insert("didx");
            }
            PDdl::ResumeBuild => {
                kinds.insert("rsmb");
            }
        }
    }
    assert_eq!(kinds.len(), 11, "every DDL kind appears: {kinds:?}");
    // C-T6d2fix FU-1 (gate F-1/GA4): pin the generator's big-value
    // sizes. The soak's bars count length classes by value-match
    // (`BIG_LENS.iter().position(|l| *l == len)`), so any three
    // distinct lens satisfy them — without this pin, a shrunk BIG_LENS
    // (GA4's [100, 3000, 4096]) passes the whole suite with the
    // 3-chunk class silently gone and the soak's amcheck never sees a
    // multi-chunk pointer.
    assert_eq!(
        diff::BIG_LENS,
        [100, 3000, 20_000],
        "the soak's big-value classes are pinned"
    );
    // And the pinned sizes mean the chunk shapes the card names: 100
    // stays inline (below the 2048 threshold), 3000 is one chunk,
    // 20000 is three — the multi-chunk class the soak must keep
    // generating.
    assert_eq!(
        diff::BIG_LENS.map(|len| {
            if len <= nucleus_catalog::TOAST_THRESHOLD {
                0
            } else {
                len.div_ceil(nucleus_catalog::TOAST_CHUNK)
            }
        }),
        [0, 1, 3],
        "the classes are the inline / one-chunk / three-chunk shapes"
    );
}

// ----- C-T6d2fix: the RC read-back position (FU-3, gate F-3/GA5) ---------------

/// One committed RC txn that replaces `big` of `pk` with a fresh 20 KB
/// value (three chunks) through the same `Db` calls the harness's SetBig
/// makes — the concurrent writer of the FU-3 shape.
fn fu3_replace_big(rig: &diff::runner::Rig, pk: i64, tag: u8) {
    use nucleus_txn::commit::SyncCommit;
    use nucleus_txn::locks::RelLockMode;
    use nucleus_txn::txn::Isolation;

    let txn = rig.db.core.begin(Isolation::ReadCommitted);
    let st = nucleus_catalog::Stmt::new(&txn, rig.db.core.visible_ts()).expect("the replace stmt");
    let h = rig
        .db
        .open_table(&st, rig.t_oid, RelLockMode::RowExclusive)
        .expect("open t for the replace");
    let attnum = h
        .schema
        .live_columns()
        .find(|c| c.name == "big")
        .map(|c| c.attnum)
        .expect("big is live");
    let applied = rig
        .db
        .update_with(
            &st,
            &h,
            &[Some(nucleus_codec::Value::Int8(pk))],
            &[(
                attnum,
                Some(nucleus_codec::row::Datum::Inline(diff::big_blob(
                    tag, 20_000,
                ))),
            )],
        )
        .expect("the replace applies");
    assert!(applied);
    rig.db
        .core
        .commit(txn, SyncCommit::On)
        .expect("the replace commits");
}

/// C-T6d2fix FU-3 (gate F-3, mutant GA5): the RC read-back position,
/// pinned in the harness's own shape. An RC UPDATE's statement snapshot
/// is taken at S; a concurrent replace of a toasted value commits above
/// S; the update's EPQ re-base carries the replace's `big` pointer into
/// the txn's own row version. The harness's read-back — a second
/// statement of the same txn at `visible_ts` — must return the own row
/// whole (§5: the carried chunks are visible at `visible_ts`, and the
/// own intent shadows every foreign version), which is exactly why the
/// read-back sits there and not at the statement's snapshot: the same
/// read at S is XX001 (§5's read-at-t rule — the carried chunks are
/// invisible above S), pinned by the raw leg (b). Mutant GA5 (both RC
/// read-backs reverted to `st.snapshot()`) fails leg (a): the read-back
/// XX001s and aborts a legal txn the engine never corrupted.
#[test]
fn t6c_rc_read_back_after_epq_carry() {
    let mut rig = diff::runner::Rig::new(5);
    rig.setup();
    let rig = &rig;

    // (a) The harness leg: the scripted UPDATE of `a` (`choice` 0
    // resolves to the first live non-pk column, an int8), with the gap
    // landing the concurrent replace above the statement's snapshot.
    let av = nucleus_codec::Value::Int8(0x5eed_0001);
    let mut s = diff::runner::Session::begin(rig, 1, 0, 0, diff::Level::ReadCommitted);
    let applied = s
        .op_update_set_with_gap(
            0,
            0,
            av.clone(),
            nucleus_codec::Value::Text("gap".into()),
            || {
                fu3_replace_big(rig, 0, 9);
            },
        )
        .expect("the update applies and the read-back returns the row");
    assert!(applied);
    s.commit().expect("the read-back left a committable txn");
    // The recorded effect is the own row whole: `a` is the set constant
    // and `big` is the concurrent replace's full 20 KB value, detoasted
    // from the carried pointer's chunks.
    let rec = rig
        .rec
        .drain()
        .txns
        .into_iter()
        .find(|t| t.session == 1)
        .expect("the session's record");
    let op = rec
        .ops
        .iter()
        .find(|o| o.kind == diff::OpKind::UpdateSet)
        .expect("the UpdateSet record");
    assert_eq!(op.out, diff::OpOut::Applied(true), "{:?}", op.out);
    let diff::OpDetail::UpdateSet {
        new_row: Some(row), ..
    } = &op.detail
    else {
        panic!("the read-back recorded no row: {:?}", op.detail);
    };
    let big = row
        .iter()
        .find(|(attnum, _)| *attnum == 4)
        .expect("big in the read-back row");
    assert_eq!(big.1, Some(diff::big_blob(9, 20_000)), "the carried value");
    let a = row
        .iter()
        .find(|(attnum, _)| *attnum == 2)
        .expect("a in the read-back row");
    assert_eq!(a.1, Some(av), "the set constant");

    // (b) §5's rule, documented raw: the same shape hand-driven — an RC
    // writer at S2, the concurrent replace committed above S2, the
    // update's EPQ re-base carrying its pointer — and the reads split.
    // At S2 the own intent carries a pointer whose chunks committed
    // above S2, invisible there: XX001. At `visible_ts` the same
    // statement reads the own row whole. That split is §5's read-at-t
    // rule, and it is exactly the read the GA5 mutant would have the
    // harness read-back make.
    use nucleus_txn::locks::RelLockMode;
    use nucleus_txn::txn::Isolation;

    let w = rig.db.core.begin(Isolation::ReadCommitted);
    let s2 = rig.db.core.visible_ts();
    fu3_replace_big(rig, 1, 12);
    let st = nucleus_catalog::Stmt::new(&w, s2).expect("the writer statement");
    let h = rig
        .db
        .open_table(&st, rig.t_oid, RelLockMode::RowExclusive)
        .expect("open t for the writer");
    let applied = rig
        .db
        .update_with(
            &st,
            &h,
            &[Some(nucleus_codec::Value::Int8(1))],
            &[(
                2,
                Some(nucleus_codec::row::Datum::Inline(
                    nucleus_codec::Value::Int8(0x5eed_0002),
                )),
            )],
        )
        .expect("the update applies (the EPQ base detoasts at its own ts)");
    assert!(applied);
    // The read at S2: corruption by §5 — a missing chunk, never a
    // partial value.
    let st_s = nucleus_catalog::Stmt::new(&w, s2).expect("the at-S statement");
    let err = rig
        .db
        .get(&st_s, &h, &[Some(nucleus_codec::Value::Int8(1))])
        .expect_err("the same read at S2 is XX001 by §5's read-at-t rule");
    assert_eq!(err.sqlstate(), "XX001", "{err:?}");
    // The read at `visible_ts`: the own row whole — the harness rule.
    let st_v =
        nucleus_catalog::Stmt::new(&w, rig.db.core.visible_ts()).expect("the read-back stmt");
    let row = rig
        .db
        .get(&st_v, &h, &[Some(nucleus_codec::Value::Int8(1))])
        .expect("the read-back returns the own row")
        .expect("the row is there");
    assert_eq!(
        row[3],
        Some(nucleus_codec::row::Datum::Inline(diff::big_blob(
            12, 20_000
        ))),
        "big is the carried pointer's whole value"
    );
    rig.db.core.abort(w).expect("abort the writer");
}

// ----- the checker can fail ---------------------------------------------------

/// The checker is not vacuous: a small clean run, corrupted one leg at
/// a time, must be flagged — every check leg can fail (c1a FU-1). The
/// uncorrupted run must itself be clean — the same assertion the soak
/// makes, at half the size (large enough that its row-bearing reads,
/// amcheck points and final scans are always there to corrupt). The
/// corruptions:
///
/// - a recorded read (a value the engine never returned);
/// - a final-scan row of `run.final_t` (a phantom row when a late
///   truncate left `t` empty) — the final scans are compared against
///   the model, never against themselves;
/// - one amcheck report's `violations` — a dirty report reaches the
///   verdict, it is never silently dropped;
/// - a refused RR/SER read rewritten as a successful one that answered
///   from the storage of a foreign truncate committed after its
///   snapshot — exactly the shape §4 obliges the engine to 40001. That
///   leg's history is hand-built (C-T6c1afix2's rider): whether the
///   seeded mini run contains a refused read with a later foreign
///   truncate is decided by the threads' interleaving — under parallel
///   test load the mined leg flaked in ~half the invocations — while
///   the built shape holds by construction (the record is still
///   rebuilt whole: a statement refused in its `open_table` never
///   recorded its parameters, so its detail is `Pending`).
#[test]
fn t6c_checker_flags_a_corrupted_read() {
    let mut cfg = Config::new(0x7c1a_00f1);
    cfg.threads = 4;
    // The soak's size (C-T6e3): the index DDL the stream now carries
    // slows it enough that a smaller mini run's refused-read-plus-later
    // truncate shape no longer lands reliably.
    cfg.progs = 200;
    cfg.ddl_txns = 200;
    let run = run_workload(&cfg);
    let verdict = check(&run);
    assert!(verdict.is_clean(), "{}", verdict.report());

    // Corrupt one successful read: the first Get that returned a row, or
    // failing that the first non-empty ScanT/ScanU, has one of its
    // values changed to something no write derived.
    let mut corrupted = run.history.clone();
    let mut corrupted_one = false;
    'outer: for txn in corrupted.txns.iter_mut() {
        for op in txn.ops.iter_mut() {
            let rows: &mut Vec<(u16, Option<nucleus_codec::Value>)> = match &mut op.out {
                diff::OpOut::Row(Some(row)) => row,
                diff::OpOut::RowsT(rows) if !rows.is_empty() => &mut rows[0].1,
                diff::OpOut::RowsU(rows) if !rows.is_empty() => &mut rows[0].1,
                _ => continue,
            };
            if let Some((_, val)) = rows.iter_mut().find(|(_, v)| v.is_some()) {
                *val = Some(nucleus_codec::Value::Text("\u{0}CORRUPT".into()));
                corrupted_one = true;
                break 'outer;
            }
        }
    }
    assert!(corrupted_one, "the mini run recorded no row-bearing read");
    let bad = diff::Run {
        history: corrupted,
        ..run.clone()
    };
    let verdict = check(&bad);
    assert!(
        !verdict.is_clean(),
        "the corrupted read went unnoticed: {}",
        verdict.report()
    );

    // (a) The final-state leg: one row of the run's own final scan
    // corrupted (a phantom row added instead when a late truncate left
    // `t` empty at the end — `u`'s scan would do as well, but `t` is
    // the leg the soak's stats lean on).
    let mut bad_final = run.clone();
    match bad_final
        .final_t
        .1
        .iter_mut()
        .find_map(|(_, row)| row.iter_mut().find(|(_, v)| v.is_some()))
    {
        Some((_, val)) => *val = Some(nucleus_codec::Value::Text("\u{0}CORRUPT".into())),
        None => bad_final.final_t.1.push((
            PHANTOM_PK,
            vec![(1, Some(nucleus_codec::Value::Int8(PHANTOM_PK)))],
        )),
    }
    let verdict = check(&bad_final);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("final state of t")),
        "the corrupted final scan went unnoticed: {}",
        verdict.report()
    );

    // (b) The amcheck leg: one report's violations, corrupted.
    let mut bad_amcheck = run.clone();
    assert!(
        !bad_amcheck.amcheck.is_empty(),
        "the mini run recorded no amcheck point"
    );
    bad_amcheck.amcheck[0]
        .report
        .violations
        .push("\u{0}CORRUPT".to_string());
    let verdict = check(&bad_amcheck);
    assert!(
        verdict.violations.iter().any(|v| v.contains("amcheck")),
        "the corrupted amcheck report went unnoticed: {}",
        verdict.report()
    );

    // (c) The TRUNCATE-window leg, over the hand-built refused-read
    // history: pk1 commits @ts1, the DDL session's TRUNCATE installs
    // storage 2 @ts2, and an RR txn begun at snap 1 (< ts 2) had its
    // Get refused 40001 in its `open_table` — such a statement never
    // records its parameters (`Pending`, no sid). The corruption
    // rewrites that record whole into the successful read it lies
    // about being, its sid set to the truncate's new storage: `snap <
    // ts` and the session difference hold by construction, so §4 owed
    // it a 40001.
    let mut history = diff::History {
        txns: vec![
            synth_txn(
                1,
                0,
                diff::Level::ReadCommitted,
                diff::Fate::Committed(1),
                vec![synth_op(
                    diff::OpKind::InsertT,
                    diff::OpDetail::InsertT {
                        pk: 1,
                        row: vec![
                            (1, Some(nucleus_codec::Value::Int8(1))),
                            (2, Some(nucleus_codec::Value::Int8(100))),
                            (3, Some(nucleus_codec::Value::Text("x".into()))),
                            (4, None),
                        ],
                    },
                    diff::OpOut::Ok,
                    0,
                    Some(1),
                )],
            ),
            synth_txn(
                2,
                diff::runner::DDL_SESSION,
                diff::Level::ReadCommitted,
                diff::Fate::Committed(2),
                vec![synth_op(
                    diff::OpKind::Truncate,
                    diff::OpDetail::Truncate { new_sid: 2 },
                    diff::OpOut::Ok,
                    1,
                    None,
                )],
            ),
            synth_txn(
                3,
                1,
                diff::Level::RepeatableRead,
                diff::Fate::Failed("40001".into()),
                vec![synth_op(
                    diff::OpKind::Get,
                    diff::OpDetail::Pending,
                    diff::OpOut::Err("40001".into()),
                    1,
                    None,
                )],
            ),
        ],
    };
    let op = &mut history.txns[2].ops[0];
    op.detail = diff::OpDetail::Get { pk: 0 };
    op.out = diff::OpOut::Row(None);
    op.sid = Some(2);
    let bad = diff::Run {
        history,
        amcheck: Vec::new(),
        final_t: (1, Vec::new()),
        final_u: (1, Vec::new()),
        leaked_intents: 0,
        leak_report: String::new(),
    };
    let verdict = check(&bad);
    assert!(
        verdict
            .violations
            .iter()
            .any(|v| v.contains("40001 was required")),
        "the rewritten in-window read went unnoticed: {}",
        verdict.report()
    );
}

// ----- the hand-built leg-(c) records (C-T6c1afix2's rider) --------------------

/// One synthetic txn record for the hand-built leg-(c) history: the
/// fields the checks read (session, level, fate, ops) carry the real
/// shapes; the identity fields take the values the recorder would have
/// assigned (ticks are trace-only and stay 0).
fn synth_txn(
    uid: u32,
    session: u16,
    level: diff::Level,
    fate: diff::Fate,
    ops: Vec<diff::OpRec>,
) -> diff::TxnRec {
    diff::TxnRec {
        uid,
        session,
        prog: uid,
        attempt: 0,
        level,
        txn_snap: None,
        start: 0,
        end: 0,
        fate,
        ops,
    }
}

/// One synthetic op record; `sid` is explicit because the window check
/// reads it, `def_version` stays `None` exactly as a statement refused
/// before its parameters resolved would have left it.
fn synth_op(
    kind: diff::OpKind,
    detail: diff::OpDetail,
    out: diff::OpOut,
    snap: u64,
    sid: Option<u64>,
) -> diff::OpRec {
    diff::OpRec {
        kind,
        detail,
        out,
        snap,
        def_version: None,
        sid,
        start: 0,
        end: 0,
    }
}
