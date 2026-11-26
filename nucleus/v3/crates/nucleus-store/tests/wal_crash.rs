//! Card A-6b's tests on SimEnv (Work 12): the WAL under power cuts and
//! process crashes, one task per life, every crash point swept.
//!
//! - `t_wal_chain_crash`: the `SegmentEnd` and exact-end hops under power
//!   cuts (§15.1, I-WAL-CHAIN; PREV-HOP-WRONG);
//! - `t_seg_ack_durable`: acked means the writer's synced report, S3 of
//!   §15's stand-ins (I-RO-OPEN; SEG-ACK-BEFORE-DURABLE, SEG-LEN-NOT-DURABLE);
//! - `t_wal_exact_segment_end`: the walk's valid end and the writer's tail
//!   (I-WAL-STALE; SEG-EXACT-NO-HOP, SEG-TAIL-SHORT);
//! - `t_wal_start`: segment 0's creation, the recycling order under lost
//!   renames and its targets, and the walk's start (I-WAL-START;
//!   START-SEG-AS-END, SEG0-LAZY, RECYCLE-UNORDERED, and S-STORE 7.17's
//!   RECYCLE-TARGET-LOW and START-SEG-SHORT-AS-END);
//! - `wal_model`: three boots over histories of crashes against a plain
//!   list of expected records (SYNC-OWN-SEGMENTS-ONLY).
//!
//! Every expected value is the oracle's: records placed by `oracle_place`,
//! in the order the stream gives, with the position after the `m`-th. A
//! round is a fresh `Sim` whose seed is fixed, its setup lives, the round's
//! life crashed at one yield (or not), the mode's fault, and a check life
//! that walks. SimEnv is a pure function of its seed, so every kill is
//! deterministic. No test runs two tasks, waits on real time, or holds a
//! lock across an Env call.

#![allow(missing_docs)]
// Test code: the workspace's unwrap/expect/panic allowance covers `#[test]`
// functions only, and the helpers below are called from them alone.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod wal_common;

use std::sync::{Arc, Mutex};

use nucleus_env::sim::{Sim, SimEnv};
use nucleus_env::{Env, EnvError, MemEnv};
use nucleus_store::wal::append::{Appended, Appender, Resume};
use nucleus_store::wal::segment::{create_wal, recycle_below};
use nucleus_store::wal::walk::{StopReason, Walk, WalkEnd, WalkStart};
use nucleus_store::wal::{PREV_NONE, RecordKind};
use wal_common::*;

// ---------------------------------------------------------------------
// The rounds (Work 10): lives, faults, checks
// ---------------------------------------------------------------------

/// A round's life: it reports each append's return and each sync's report
/// to the progress record it is handed.
type Life<'a> = &'a (
        dyn Fn(&nucleus_env::sim::SimEnv, &Arc<Mutex<Progress>>) -> Result<(), EnvError>
            + Send
            + Sync
    );

/// What a round leaves: the round's progress, the names its life left in
/// `wal/` before any cut, its check walk (or the text of the error the walk
/// returned), what the walk saw, and the machine.
struct Round {
    progress: Progress,
    before_cut: Vec<String>,
    walk: Result<Walk, String>,
    seen: Vec<Seen>,
    sim: Sim,
}

/// One state a boot left: where its walk ended, the chain head it gives
/// the next boot, what its walk accepted, what it reported and appended.
#[derive(Debug, Clone, Default)]
struct Hist {
    valid_end: u64,
    head: (u64, u32),
    seen: Vec<Seen>,
    reports: Vec<u64>,
    appended: Vec<Appended>,
}

/// A machine for a round of `mode` at `seed`: the mode's knobs and, for a
/// power-cut mode, its `DirLoss`.
fn machine(seed: u64, mode: &Mode) -> Sim {
    let mut sim = Sim::new(seed, mode.knobs);
    if let Some(loss) = mode.power_cut {
        sim.set_dir_loss(loss);
    }
    sim
}

/// A check life: walks from `start` and leaves what the walk ended with and
/// what it saw. The life itself must be clean.
fn walk_life(sim: &mut Sim, start: &WalkStart) -> (Result<Walk, String>, Vec<Seen>) {
    let out: Mutex<Option<Result<Walk, String>>> = Mutex::new(None);
    let seen: Mutex<Vec<Seen>> = Mutex::new(Vec::new());
    let life = sim.run_life(1, None, |env, _t| {
        let mut got = Vec::new();
        // The walk's Env calls are made here; nothing is locked over them.
        let r = walk_into(env, start, &mut got);
        let mut seen = seen.lock().unwrap();
        let mut out = out.lock().unwrap();
        match r {
            Ok(walk) => {
                *seen = got;
                *out = Some(Ok(walk));
                Ok(())
            }
            Err(e) => {
                let what = e.to_string();
                *out = Some(Err(what));
                // The check life must be clean: a walk that failed is the
                // test's finding, recorded in `out`, not the life's result.
                Ok(())
            }
        }
    });
    assert_clean(&life);
    let result = out.into_inner().unwrap().expect("the walk ran");
    (result, seen.into_inner().unwrap())
}

/// Runs one round: the machine, the setup lives, the round's life (crashed
/// at `crash` if it is armed), the mode's fault (and a media fault too, if
/// `media`), and the check walk.
fn run_round(
    seed: u64,
    mode: &Mode,
    crash: Option<u64>,
    media: bool,
    setup: &mut dyn FnMut(&mut Sim),
    life: Life<'_>,
    start: &WalkStart,
) -> Round {
    let mut sim = machine(seed, mode);
    setup(&mut sim);
    let progress = progress();
    let shared = Arc::clone(&progress);
    let out = sim.run_life(1, crash, move |env, _t| life(env, &shared));
    assert_life(out, crash);
    // What the life left in `wal/`, page cache included, before any cut:
    // which creates and renames ran.
    let before_cut = wal_names(&sim);
    if mode.power_cut.is_some() {
        sim.power_cut();
    }
    if media {
        // Class MW: F-MEDIA-WAL after F-CRASH (S-STORE §15), on the durable
        // writes the cut left. A crash before the first record leaves none
        // for it to strike.
        let _ = sim.media_wal();
    }
    let (walk, seen) = walk_life(&mut sim, start);
    Round {
        progress: progress_lock(&progress).clone(),
        before_cut,
        walk,
        seen,
        sim,
    }
}

/// What a life must have done: clean when no crash was armed, else crashed
/// at that yield and nowhere else.
fn assert_life(out: nucleus_env::sim::LifeOutcome, crash: Option<u64>) {
    match crash {
        None => assert_clean(&out),
        Some(k) => assert_crashed_at(&out, k),
    }
}

/// The yield count of the round's life without a crash, on the same setup
/// and machine: `Y`, the sweep's last crash point.
fn dry_yields(seed: u64, mode: &Mode, setup: &mut dyn FnMut(&mut Sim), life: Life<'_>) -> u64 {
    let mut sim = machine(seed, mode);
    setup(&mut sim);
    let progress = progress();
    let shared = Arc::clone(&progress);
    let out = sim.run_life(1, None, move |env, _t| life(env, &shared));
    assert_clean(&out);
    out.yields
}

/// A life that creates the WAL (the setup of every round but `t_wal_start`'s).
fn create_life(sim: &mut Sim) {
    let out = sim.run_life(1, None, |env, _t| create_wal(env, S).map_err(to_env_error));
    assert_clean(&out);
}

/// Appends `plan`'s records through an appender of `boot` resumed at
/// `resume`, syncing after the records whose 1-based places are in
/// `sync_after`, and reporting to `progress`. A `StoreError` that is not an
/// `Env` error halts the life (the test's finding).
fn append_records(
    env: &SimEnv,
    resume: Resume,
    boot: u32,
    plan: &[Plan],
    sync_after: &[usize],
    progress: &Arc<Mutex<Progress>>,
) -> Result<(), EnvError> {
    let mut app = Appender::new(env, S, boot, resume).map_err(to_env_error)?;
    for (i, p) in plan.iter().enumerate() {
        let ddl = ddl_payload(p.len, i as u64);
        let record = commit(p.boot, p.n, p.commit_ts, &ddl);
        let appended = app.append(&record).map_err(to_env_error)?;
        // Taken between Env calls, never across one.
        progress_lock(progress).appended.push(appended);
        if sync_after.contains(&(i + 1)) {
            let report = app.sync().map_err(to_env_error)?;
            progress_lock(progress).reports.push(report);
        }
    }
    Ok(())
}

/// A creating session's resume point: the WAL's first record names meta 0's
/// pair, all-ones and 0 (§12.2).
const CREATING: Resume = Resume {
    append_pos: 0,
    prev_lsn: u64::MAX,
    prev_crc: 0,
    durable_end: 0,
};

/// A life that appends `plan` for `boot` from `resume`, syncing after
/// `sync_after`.
fn append_life<'a>(
    resume: Resume,
    boot: u32,
    plan: &'a [Plan],
    sync_after: &'a [usize],
) -> impl Fn(&SimEnv, &Arc<Mutex<Progress>>) -> Result<(), EnvError> + Send + Sync + 'a {
    move |env, progress| append_records(env, resume, boot, plan, sync_after, progress)
}

/// One boot of the model: its life (crashed at `crash` if armed), the fault
/// of `cut` if it is a power-cut mode, and its check walk. Returns what it
/// left: where its walk ended, the chain head it gives the next boot, what
/// its walk accepted, and what it reported and appended.
fn boot_and_check(
    sim: &mut Sim,
    cut: Option<&Mode>,
    crash: Option<u64>,
    resume: Resume,
    boot: u32,
    plan: &[Plan],
    sync_after: &[usize],
) -> Hist {
    let progress = progress();
    let shared = Arc::clone(&progress);
    let life = append_life(resume, boot, plan, sync_after);
    let out = sim.run_life(1, crash, move |env, _t| life(env, &shared));
    assert_life(out, crash);
    if let Some(mode) = cut
        && mode.power_cut.is_some()
    {
        sim.power_cut();
    }
    let progress = progress_lock(&progress).clone();
    let (walk, seen) = walk_life(sim, &start0());
    let walk = walk.expect("the history's walk returns");
    Hist {
        valid_end: walk
            .valid_end()
            .unwrap_or_else(|| panic!("the history's walk did not stop: {:?}", walk.end)),
        head: walk.chain_head(&start0()),
        seen,
        reports: progress.reports,
        appended: progress.appended,
    }
}

/// The resume point a boot takes from the boot before it (§12.4 "After
/// recovery"): the append position and `written_end` at the valid end, the
/// chain head from the walk, and `durable_end` at the walk's start.
fn resume_from(h: &Hist) -> Resume {
    Resume {
        append_pos: h.valid_end,
        prev_lsn: h.head.0,
        prev_crc: h.head.1,
        durable_end: 0,
    }
}

// ---------------------------------------------------------------------
// The checks
// ---------------------------------------------------------------------

/// The end of a record the walk saw.
fn end_of(r: &Seen) -> u64 {
    r.lsn + u64::from(r.len)
}

/// The oracle's position after its first `m` records (§12.5's hops): the
/// start when `m` is 0; otherwise the record's end or, after a `SegmentEnd`
/// or a record that ends exactly at its segment's end, the next segment's
/// first LSN.
fn position_after(list: &[Seen], m: usize, start_lsn: u64) -> u64 {
    match m.checked_sub(1).and_then(|i| list.get(i)) {
        None => start_lsn,
        Some(last) if last.kind == RecordKind::SegmentEnd => (last.lsn / S + 1) * S,
        Some(last) => end_of(last),
    }
}

/// What one check of a round must hold (card Work 12; the oracle is the
/// plain list `oracle`): the walk returns `Ok` and ends in a stop; it
/// accepts exactly the oracle's first `m` records; it stops where the
/// oracle places the record after them (`start_lsn` when `m` is 0); its
/// `last` is the `m`-th record's start and boot, and `newest_boot` the
/// newest of them in serial order. Returns `m`.
fn check_prefix(
    walk: &Result<Walk, String>,
    seen: &[Seen],
    oracle: &[Seen],
    start_lsn: u64,
    what: &str,
) -> usize {
    let walk = walk
        .as_ref()
        .unwrap_or_else(|e| panic!("{what}: the walk failed: {e}"));
    let m = usize::try_from(walk.accepted).expect("a count");
    assert!(
        matches!(walk.end, WalkEnd::Stop { .. }),
        "{what}: the walk did not stop cleanly: {:?}",
        walk.end
    );
    assert!(
        m <= oracle.len(),
        "{what}: it accepted {m} records; the oracle has {}",
        oracle.len()
    );
    assert_eq!(seen, &oracle[..m], "{what}: what it accepted");
    assert_eq!(
        walk.valid_end(),
        Some(position_after(oracle, m, start_lsn)),
        "{what}: where it stopped"
    );
    let last = m.checked_sub(1).map(|i| (oracle[i].lsn, oracle[i].boot));
    assert_eq!(
        walk.last.map(|l| (l.lsn, l.boot)),
        last,
        "{what}: its chain head's record"
    );
    let newest = oracle[..m].iter().fold(None::<u32>, |acc, r| match acc {
        Some(n) if r.boot.wrapping_sub(n) >= 1 << 31 => acc,
        _ => Some(r.boot),
    });
    assert_eq!(walk.newest_boot, newest, "{what}: the newest boot id");
    m
}

/// That `m` covers every record whose end is at or below every report any
/// boot made: none of them lies beyond where the walk stopped.
fn check_covered(m: usize, oracle: &[Seen], reports: &[u64], what: &str) {
    let Some(l) = reports.iter().max() else {
        return;
    };
    for (i, r) in oracle.iter().enumerate() {
        assert!(
            end_of(r) > *l || i < m,
            "{what}: the record at {} ends at {}, at or below the report {l}, and the walk did not reach it",
            r.lsn,
            end_of(r)
        );
    }
}

/// That `m` covers the whole of the previous check's list and every record
/// whose append returned: the process-crash mode, where every write that
/// returned survives.
fn check_process(m: usize, oracle: &[Seen], appended: &[Appended], previous: usize, what: &str) {
    assert!(
        m >= previous,
        "{what}: the previous check's list is not covered"
    );
    for a in appended {
        let i = oracle
            .iter()
            .position(|r| r.lsn == a.lsn)
            .unwrap_or_else(|| {
                panic!(
                    "{what}: an appended record at {} is not in the oracle",
                    a.lsn
                )
            });
        assert!(
            i < m,
            "{what}: the appended record at {} was not reached",
            a.lsn
        );
    }
}

/// The checks of one round of a boot that follows a history: the oracle is
/// the history's accepted list, then this boot's stream from where it
/// resumed. Returns `m`.
#[allow(clippy::too_many_arguments)]
fn check_boot_round(
    round: &Round,
    history: &Hist,
    stream: &[Seen],
    reports_before: &[u64],
    process_crash: bool,
    what: &str,
) -> usize {
    let mut oracle = history.seen.clone();
    let previous = oracle.len();
    oracle.extend_from_slice(stream);
    let mut reports = reports_before.to_vec();
    reports.extend_from_slice(&round.progress.reports);
    let m = check_prefix(&round.walk, &round.seen, &oracle, 0, what);
    check_covered(m, &oracle, &reports, what);
    if process_crash {
        check_process(m, &oracle, &round.progress.appended, previous, what);
    }
    m
}

/// The sweep of one life over `mode`'s crash points (Work 10): the no-crash
/// round, and every `k` from 1 while the round's life at the round's own
/// seed has a `k`-th yield to crash at (a dry run of that life gives `Y`,
/// which a history with a power cut can make depend on the seed). Each
/// round's seed is `10_000 × mode index + k`. Calls `check` with `k` and the
/// round; returns how many ran.
fn sweep_rounds(
    mode: &Mode,
    setup: &mut dyn FnMut(&mut Sim),
    life: Life<'_>,
    start: &WalkStart,
    mut check: impl FnMut(u64, &Round),
) -> usize {
    let base = dry_yields(10_000 * mode.index, mode, setup, life);
    let mut ran = 0usize;
    let mut highest = 0u64;
    // The sweep stops 32 yields past the no-crash seed's `Y`: every life
    // here ends before that (asserted), so no crash point is left out.
    for k in 0..=base + 32 {
        let y = dry_yields(10_000 * mode.index + k, mode, setup, life);
        assert!(
            y <= base + 32,
            "the life at seed {k} has {y} yields, past the sweep's reach"
        );
        if k > y {
            // No `k`-th yield at this seed.
            continue;
        }
        let round = run_round(
            10_000 * mode.index + k,
            mode,
            if k == 0 { None } else { Some(k) },
            false,
            setup,
            life,
            start,
        );
        check(k, &round);
        ran += 1;
        highest = k;
    }
    assert!(highest > 0, "the sweep reached no crash point");
    ran
}

// ---------------------------------------------------------------------
// t_wal_chain_crash (§15.1, I-WAL-CHAIN; PREV-HOP-WRONG)
// ---------------------------------------------------------------------

/// The `SegmentEnd` and exact-end hops under power cuts (§15.1; S-STORE
/// §12.2, §12.5). An appender of boot 1 appends fixture F's 22 records,
/// syncing after records 5, 11, 15 and 22, and every crash point runs in
/// P0, P1, P2 and C. The check walk ends in a stop, never a splice, a
/// malformed record, damage or an error; it accepts exactly the oracle's
/// first `m` records, stops where the oracle says, and reaches every record
/// a report covered. PREV-HOP-WRONG (both forms) fails the no-crash rounds
/// with a splice, at 2048 under form (a) and at 1024 under form (b).
#[test]
fn t_wal_chain_crash() {
    let plan = fixture_f();
    let oracle = seen_stream(&oracle_stream(0, &plan));
    assert_eq!(oracle.len(), 25, "the stream ends at 4212 with 25 records");
    let life = append_life(CREATING, 1, &plan, &[5, 11, 15, 22]);
    let mut setup = |sim: &mut Sim| create_life(sim);
    let mut rounds = 0usize;
    let mut lost_unreported = 0usize;
    let mut yields = Vec::new();
    for mode in [P0, P1, P2, C] {
        let y = dry_yields(10_000 * mode.index, &mode, &mut setup, &life);
        yields.push((mode.name, y));
        rounds += sweep_rounds(&mode, &mut setup, &life, &start0(), |k, round| {
            let what = format!("{} at crash point {k}", mode.name);
            let m = check_prefix(&round.walk, &round.seen, &oracle, 0, &what);
            check_covered(m, &oracle, &round.progress.reports, &what);
            if mode.power_cut.is_none() {
                check_process(m, &oracle, &round.progress.appended, 0, &what);
            }
            if k == 0 {
                assert_eq!(m, 25, "{what}: every no-crash round accepts all 25");
                assert_eq!(round.progress.reports.last(), Some(&4212));
            }
            // Non-vacuity: some P0 round loses a record that was written
            // and not reported.
            if mode.name == "P0" && m < round.progress.appended.len() {
                lost_unreported += 1;
            }
        });
    }
    assert!(
        lost_unreported > 0,
        "no P0 round lost a written, unreported record"
    );
    eprintln!(
        "t_wal_chain_crash: Y per mode {yields:?}; {rounds} rounds; {lost_unreported} rounds lost a written, unreported record"
    );
}

// ---------------------------------------------------------------------
// t_seg_ack_durable (§15.1, I-RO-OPEN; S3 of §15's stand-ins)
// ---------------------------------------------------------------------

/// Acked means the writer's synced report (§15.1; S3: an `on` ack, taken as
/// the appender's report that the record is synced). A setup appends
/// fixture F's records 1 to 10 and syncs. The round resumes at 1740 and
/// appends records 11 to 15, the first through a hop into segment 2048,
/// syncing after 13 and 15, at every crash point in P0, P1 and P3. In the
/// rename case one more life has first renamed segment 0 to 2048 without
/// making the rename durable, and crashed as a process. The check walks
/// from `(1024, (800, crc of record 5), 1)` and must end in a stop whose
/// valid end is at least the end of every record the round's last report
/// covered. SEG-ACK-BEFORE-DURABLE (a: create; b: rename) and
/// SEG-LEN-NOT-DURABLE fail it in P3 at crash points after the first
/// report.
#[test]
fn t_seg_ack_durable() {
    let plan = fixture_f();
    let oracle = seen_stream(&oracle_stream(0, &plan));
    let head: Vec<Plan> = plan[..10].to_vec();
    let tail: Vec<Plan> = plan[10..15].to_vec();
    for case in ["create", "rename"] {
        // What the setup's appender returned.
        let recorded = Mutex::new(Vec::<Appended>::new());
        let mut setup = |sim: &mut Sim| {
            create_life(sim);
            let progress = progress();
            let shared = Arc::clone(&progress);
            let life = append_life(CREATING, 1, &head, &[10]);
            let out = sim.run_life(1, None, move |env, _t| life(env, &shared));
            assert_clean(&out);
            *recorded.lock().unwrap() = progress_lock(&progress).appended.clone();
            if case == "rename" {
                // `list_dir`, `rename`, `sync_dir`: the crash is the third
                // call's, so the rename ran and is not durable.
                let out = sim.run_life(1, Some(3), |env, _t| {
                    let pairs = recycle_below(env, S, 1024, 1740).map_err(to_env_error)?;
                    assert_eq!(pairs, vec![(0, 2048)]);
                    Ok(())
                });
                assert_crashed_at(&out, 3);
            }
        };
        // The resume point: from the setup, which the dry run runs.
        dry_yields(0, &P0, &mut setup, &|_, _| Ok(()));
        let appended = recorded.lock().unwrap().clone();
        assert_eq!(appended.len(), 10);
        assert_eq!(appended[4].lsn, 800, "record 5's start");
        assert_eq!(appended[9].end, 1740, "the append position after record 10");
        let resume = Resume {
            append_pos: 1740,
            prev_lsn: appended[9].lsn,
            prev_crc: appended[9].crc,
            durable_end: 1740,
        };
        let start = start(1024, (appended[4].lsn, appended[4].crc), 1);
        let life = append_life(resume, 1, &tail, &[3, 5]);
        // The round life's Env calls are its yields (S-ENV: every call is
        // one), and each crash point takes its meaning from them. A crash
        // at a yield is before that call. Before the new segment, the
        // appender makes segment 1024 usable, where record 11's `SegmentEnd`
        // goes (`open`, `len`, `sync_all`, `sync_dir`: yields 1 to 4; the
        // writer rule of S-STORE §12.5, "even one that existed at open"),
        // and writes that `SegmentEnd` (yield 5). The new segment, 2048,
        // is then made usable (card Work 5): created, by `open` `ReadWrite`
        // (`NotFound`), `open` `CreateNew`, `len`, `set_len`, `sync_all` and
        // `sync_dir`, yields 6 to 11; or found under the name the setup's
        // rename gave it, by `open`, `len`, `sync_all` and `sync_dir`,
        // yields 6 to 9. So the create has run from crash point 8, and is
        // durable from 12, after its own `sync_dir` (yield 11). The rename
        // ran in the setup's life and is durable from 5: segment 1024's
        // `sync_dir` at yield 4 makes every pending entry of `wal/` durable,
        // the rename included, and before it the cut undoes the rename.
        let (ran_from, durable_from): (u64, u64) = if case == "create" { (8, 12) } else { (0, 5) };
        let (n0, n2048) = (seg_file(0), seg_file(2048));
        let mut lost = 0usize;
        let mut kept = 0usize;
        let mut after_report = 0usize;
        let mut yields = Vec::new();
        for mode in [P0, P1, P3] {
            let y = dry_yields(10_000 * mode.index, &mode, &mut setup, &life);
            yields.push((mode.name, y));
            sweep_rounds(&mode, &mut setup, &life, &start, |k, round| {
                let what = format!("{case} case, {} at crash point {k}", mode.name);
                let walk = round
                    .walk
                    .as_ref()
                    .unwrap_or_else(|e| panic!("{what}: {e}"));
                let valid_end = walk
                    .valid_end()
                    .unwrap_or_else(|| panic!("{what}: not a stop: {:?}", walk.end));
                // It ends in a stop that reaches every record the last
                // report covered.
                if let Some(l) = round.progress.last_report() {
                    let reached = oracle
                        .iter()
                        .filter(|r| end_of(r) <= l)
                        .map(end_of)
                        .max()
                        .unwrap_or(0);
                    assert!(
                        valid_end >= reached,
                        "{what}: the valid end {valid_end} is below the reported {reached}"
                    );
                    if k > 0 {
                        after_report += 1;
                    }
                }
                // Non-vacuity in P3, where nothing unsynced survives
                // (S-STORE §12.5 "Short segments": a segment present at
                // open can be a crashed process's create or rename, "kept in
                // the page cache with no directory sync behind it"). What
                // the life left in `wal/` before the cut says whether the
                // create, or the rename, ran; what the cut left says
                // whether it was durable: it is lost by a crash that
                // precedes its `sync_dir`, and kept by one that follows it.
                if mode.name == "P3" {
                    let ran = round.before_cut.contains(&n2048)
                        && (case == "create" || !round.before_cut.contains(&n0));
                    let held = file_exists(&round.sim, 2048)
                        && (case == "create" || !file_exists(&round.sim, 0));
                    assert_eq!(
                        ran,
                        k == 0 || k >= ran_from,
                        "{what}: the segment's create or rename has run"
                    );
                    assert_eq!(
                        held,
                        k == 0 || k >= durable_from,
                        "{what}: the segment is under the new name after the cut"
                    );
                    if !ran {
                        assert!(!held, "{what}: a segment that never ran is present");
                    } else if held {
                        kept += 1;
                    } else {
                        lost += 1;
                    }
                }
            });
        }
        assert!(
            lost > 0,
            "{case} case: no P3 round cut between the new segment's create or rename and its sync_dir lost it"
        );
        assert!(
            kept > 0,
            "{case} case: no P3 round kept the new segment after its sync_dir"
        );
        assert!(after_report > 0);
        eprintln!(
            "t_seg_ack_durable {case}: Y per mode {yields:?}; in P3, {lost} rounds lost the segment and {kept} kept it"
        );
    }
}

// ---------------------------------------------------------------------
// t_wal_exact_segment_end (§15.1, I-WAL-STALE; SEG-EXACT-NO-HOP,
// SEG-TAIL-SHORT)
// ---------------------------------------------------------------------

/// The walk's valid end and the writer's tail (§15.1; §12.2, §12.5). (a) An
/// appender appends fixture F's records 1 to 8, record 5 ending exactly at
/// 1024, and syncs once; at every crash point in P0, P1 and P2, whenever
/// the report was recorded the valid end is at least 1324. (b) Without a
/// crash, a P0 cut: the walk accepts all 25 records, stops on the zeros at
/// 4212, and in every segment it leaves the last record it accepted is a
/// `SegmentEnd` or ends exactly at the segment's end.
#[test]
fn t_wal_exact_segment_end() {
    let plan = fixture_f();
    let oracle = seen_stream(&oracle_stream(0, &plan));
    // (a)
    {
        let head: Vec<Plan> = plan[..8].to_vec();
        let life = append_life(CREATING, 1, &head, &[8]);
        let mut setup = |sim: &mut Sim| create_life(sim);
        let mut reported = 0usize;
        for mode in [P0, P1, P2] {
            sweep_rounds(&mode, &mut setup, &life, &start0(), |k, round| {
                let what = format!("(a) {} at crash point {k}", mode.name);
                if let Some(report) = round.progress.last_report() {
                    assert_eq!(report, 1324, "{what}: the report");
                    let valid_end = round
                        .walk
                        .as_ref()
                        .unwrap_or_else(|e| panic!("{what}: {e}"))
                        .valid_end()
                        .unwrap_or_else(|| panic!("{what}: not a stop"));
                    assert!(valid_end >= 1324, "{what}: the valid end is {valid_end}");
                    reported += 1;
                }
            });
        }
        assert!(reported > 0);
    }
    // (b)
    {
        let life = append_life(CREATING, 1, &plan, &[22]);
        let mut setup = |sim: &mut Sim| create_life(sim);
        let round = run_round(
            10_000 * P0.index,
            &P0,
            None,
            false,
            &mut setup,
            &life,
            &start0(),
        );
        assert_eq!(round.progress.reports.last(), Some(&4212));
        let m = check_prefix(&round.walk, &round.seen, &oracle, 0, "(b)");
        assert_eq!(m, 25);
        let walk = round.walk.as_ref().expect("the walk returns");
        assert_eq!(
            walk.end,
            WalkEnd::Stop {
                valid_end: 4212,
                segment: 4096,
                reason: StopReason::Magic
            }
        );
        // The writer's tail: in every segment, the last record the walk
        // accepted there is a `SegmentEnd` or ends exactly at its end; in
        // the last, the room after it is 0 or at least a `SegmentEnd`'s.
        for seg in [0u64, 1024, 2048, 3072, 4096] {
            let last = oracle
                .iter()
                .take(m)
                .rfind(|r| r.lsn < seg + S)
                .unwrap_or_else(|| panic!("no record in segment {seg}"));
            let room = seg + S - end_of(last);
            if seg < 4096 {
                assert!(
                    last.kind == RecordKind::SegmentEnd || room == 0,
                    "segment {seg}: the last record is {:?} with {room} bytes after it",
                    last.kind
                );
            } else {
                assert!(
                    room == 0 || room >= 53,
                    "the tail of segment {seg} is {room} bytes"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------
// t_wal_start (§15.1, I-WAL-START)
// ---------------------------------------------------------------------

/// The writer's own `Appended` for fixture F, appended with a final sync:
/// the `SegmentEnd`s' starts and CRCs, as the appender reported them.
fn fixture_appended() -> Vec<Appended> {
    let plan = fixture_f();
    let mut sim = machine(1, &P0);
    create_life(&mut sim);
    let hist = boot_and_check(&mut sim, None, None, CREATING, 1, &plan, &[22]);
    hist.appended
}

/// Whether, among the old names, the present ones form an upper set: never
/// a segment renamed while a lower one is present (I-WAL-START). `names`
/// is a listing of `wal/`, which holds bare names ([`seg_file`]); a path
/// never matches one, so a check written with [`seg_name`] holds vacuously.
fn present_is_upper_set(old: &[u64], names: &[String]) -> bool {
    let present: Vec<bool> = old.iter().map(|f| names.contains(&seg_file(*f))).collect();
    // From the highest name down: present names, then renamed (absent)
    // ones, and never an absent one above a present one. Consecutive
    // present names alone are not enough: a highest-first recycling, the
    // descending form of RECYCLE-UNORDERED, leaves 0 and 1024 present with
    // 2048 and 3072 renamed.
    present.iter().rev().skip_while(|p| **p).all(|p| !*p)
}

/// Whether the walk accepted a record past a hop that boot `boot` wrote: a
/// `SegmentEnd`, or a record ending exactly at its segment's end, of that
/// boot, with the record after it in the next segment.
fn past_own_hop(seen: &[Seen], boot: u32) -> bool {
    seen.windows(2).any(|w| {
        let (a, b) = (&w[0], &w[1]);
        a.boot == boot
            && (a.kind == RecordKind::SegmentEnd || end_of(a).is_multiple_of(S))
            && b.lsn > a.lsn
            && seg_of(b.lsn) > seg_of(a.lsn)
    })
}

/// Segment 0's creation, the recycling order under lost renames and its
/// targets, and the walk's start (§15.1; §2.3 step 2, §12.1, §13 step 6,
/// §12.5 "The start segment" and "Short segments"; card A-6b, Work 12, and
/// S-STORE 7.17's two bullets for A-6b). START-SEG-AS-END, SEG0-LAZY,
/// RECYCLE-UNORDERED (both forms), RECYCLE-TARGET-LOW and
/// START-SEG-SHORT-AS-END fail it.
#[test]
fn t_wal_start() {
    // (a) Segment 0's creation at every yield, then a cut in P0 and P3.
    {
        let mut setup = |_: &mut Sim| {};
        let life =
            |env: &SimEnv, _: &Arc<Mutex<Progress>>| create_wal(env, S).map_err(to_env_error);
        for mode in [P0, P3] {
            let y = dry_yields(10_000 * mode.index, &mode, &mut setup, &life);
            let mut returned = 0usize;
            for k in 0..=y {
                let seed = 10_000 * mode.index + k;
                let crash = if k == 0 { None } else { Some(k) };
                let mut sim = machine(seed, &mode);
                let ok = Mutex::new(false);
                let out = sim.run_life(1, crash, |env, _t| {
                    let r = create_wal(env, S);
                    // Taken after the Env calls, never across one.
                    *ok.lock().unwrap() = r.is_ok();
                    r.map_err(to_env_error)
                });
                assert_life(out, crash);
                sim.power_cut();
                if *ok.lock().unwrap() {
                    returned += 1;
                    // It exists after the cut, with its durable length.
                    let id = sim.file_id(&seg_name(0)).ok().flatten().unwrap_or_else(|| {
                        panic!("{} at {k}: segment 0 is gone after the cut", mode.name)
                    });
                    assert_eq!(sim.disk().durable_len(id), S, "{} at {k}", mode.name);
                    assert_eq!(sim.disk().cur_len(id), S, "{} at {k}", mode.name);
                }
            }
            assert!(returned > 0, "{}: no life returned Ok", mode.name);
            eprintln!(
                "t_wal_start (a) {}: Y = {y}, {returned} lives returned Ok",
                mode.name
            );
        }
    }
    let appended = fixture_appended();
    assert_eq!(appended.len(), 22);
    // The `SegmentEnd` before record 21, at 4043: F's last.
    let (last_end_lsn, last_end_crc) = appended[20].segment_end.expect("record 21 hopped");
    assert_eq!(last_end_lsn, 4043);
    // (b) The recycling order.
    let plan = fixture_f();
    let oracle = seen_stream(&oracle_stream(0, &plan));
    let old = [0u64, 1024, 2048, 3072];
    let target = [5120u64, 6144, 7168, 8192];
    // The oracle itself (I-WAL-START), on bare names as a listing gives
    // them: the lower ones renamed is an upper set; a highest-first
    // recycling's leftovers are not, whether or not the names left are
    // consecutive.
    let names_of = |fs: &[u64]| fs.iter().map(|f| seg_file(*f)).collect::<Vec<_>>();
    assert!(present_is_upper_set(&old, &names_of(&[1024, 2048, 3072])));
    assert!(present_is_upper_set(&old, &names_of(&[])));
    assert!(!present_is_upper_set(&old, &names_of(&[0, 1024])));
    assert!(!present_is_upper_set(&old, &names_of(&[0, 1024, 2048])));
    let recorded = Mutex::new(Vec::<(u64, Vec<u8>)>::new());
    let mut setup = |sim: &mut Sim| {
        create_life(sim);
        let hist = boot_and_check(sim, None, None, CREATING, 1, &plan, &[22]);
        assert_eq!(hist.appended, appended);
        assert_eq!(hist.reports.last(), Some(&4212));
        // The four old segments' bytes, as the setup left them.
        let mut bytes = Vec::new();
        for first in old {
            let id = sim
                .file_id(&seg_name(first))
                .ok()
                .flatten()
                .expect("a segment");
            assert_eq!(sim.disk().durable_len(id), S);
            bytes.push((first, sim.disk().durable_read(id, 0, S)));
        }
        *recorded.lock().unwrap() = bytes;
    };
    let life = |env: &SimEnv, _: &Arc<Mutex<Progress>>| {
        let pairs = recycle_below(env, S, 4096, 4212).map_err(to_env_error)?;
        assert_eq!(
            pairs,
            vec![(0, 5120), (1024, 6144), (2048, 7168), (3072, 8192)]
        );
        Ok(())
    };
    // `list_dir`, then a rename and a `sync_dir` per segment: nine yields.
    let y = dry_yields(0, &P1, &mut setup, &life);
    assert_eq!(y, 9);
    let bytes = recorded.lock().unwrap().clone();
    // Per mode, how many renames ran (the life had moved the old name
    // before the cut) and the cut undid.
    let mut lost_renames: Vec<(&'static str, usize)> = Vec::new();
    let mut rounds = 0usize;
    let mut kept: Option<Round> = None;
    for mode in [P0, P1, P3] {
        let seeds: u64 = if mode.name == "P0" { 1 } else { 8 };
        let mut lost_here = 0usize;
        for k in 0..=y {
            for j in 0..seeds {
                let seed = 10_000 * mode.index + 16 * k + j;
                let crash = if k == 0 { None } else { Some(k) };
                let round = run_round(
                    seed,
                    &mode,
                    crash,
                    false,
                    &mut setup,
                    &life,
                    &start(4096, (last_end_lsn, last_end_crc), 1),
                );
                let what = format!("(b) {} at crash point {k}, seed {j}", mode.name);
                let names = wal_names(&round.sim);
                // No name but the old ones, their targets and 4096's.
                for name in &names {
                    assert!(
                        old.iter()
                            .chain(target.iter())
                            .any(|f| seg_file(*f) == *name)
                            || *name == seg_file(4096),
                        "{what}: {name} is not a segment the round could have made"
                    );
                }
                // Among the old names, the present ones are an upper set.
                assert!(
                    present_is_upper_set(&old, &names),
                    "{what}: a segment is renamed while a lower one is present: {names:?}"
                );
                // Each old segment is under exactly one of its two names,
                // with its bytes and its length unchanged.
                for ((o, t), was) in old.iter().zip(target.iter()).zip(bytes.iter()) {
                    let at_old = names.contains(&seg_file(*o));
                    let at_new = names.contains(&seg_file(*t));
                    assert_ne!(
                        at_old,
                        at_new,
                        "{what}: segment {} is under {} of its two names",
                        o,
                        usize::from(at_old) + usize::from(at_new)
                    );
                    let held = if at_new { *t } else { *o };
                    let id = round
                        .sim
                        .file_id(&seg_name(held))
                        .ok()
                        .flatten()
                        .expect("the file under one of its names");
                    assert_eq!(round.sim.disk().durable_len(id), S, "{what}");
                    assert_eq!(
                        round.sim.disk().durable_read(id, 0, S),
                        was.1,
                        "{what}: its bytes changed"
                    );
                }
                // 4096 is untouched.
                let untouched = round.sim.file_id(&seg_name(4096)).ok().flatten().unwrap();
                assert_eq!(round.sim.disk().durable_len(untouched), S);
                assert!(names.contains(&seg_file(4096)), "{what}");
                // Non-vacuity: a rename that ran and was lost. It ran when
                // the life had moved the old name before the cut (what the
                // life left in `wal/`, page cache included), and the cut
                // lost it when it put the old name back.
                for o in &old {
                    let name = seg_file(*o);
                    if !round.before_cut.contains(&name) && names.contains(&name) {
                        lost_here += 1;
                    }
                }
                // The round's walk, from 4096, when nothing was lost.
                if k == 0 {
                    check_prefix(&round.walk, &round.seen, &oracle[23..], 4096, &what);
                }
                if mode.name == "P0" && k == 0 {
                    kept = Some(round);
                } else {
                    rounds += 1;
                }
            }
        }
        lost_renames.push((mode.name, lost_here));
    }
    // The drawn mode bites (P1: each unsynced rename kept or lost by a
    // coin), and the deterministic one loses every unsynced rename (P3).
    for (name, lost) in &lost_renames {
        if *name != "P0" {
            assert!(*lost > 0, "{name}: no round lost a rename that ran");
        }
    }
    eprintln!(
        "t_wal_start (b): {} rounds; renames a cut undid, by mode: {lost_renames:?}",
        rounds + 1
    );
    // (c) The walk's start, after (b)'s no-crash round and its P0 cut.
    {
        let mut sim = kept.expect("the P0 no-crash round").sim;
        let (walk, _) = walk_life(&mut sim, &start0());
        let walk = walk.expect("the walk returns");
        assert_eq!(
            walk.end,
            WalkEnd::StartSegmentMissing { wal_start_lsn: 0 },
            "a missing start segment is never a valid end"
        );
        assert_eq!(walk.accepted, 0);
        assert!(walk.valid_end().is_none());
        assert_eq!(walk.chain_head(&start0()), PREV_NONE);
        assert!(walk.last.is_none());
    }
    // (c) On a MemEnv holding only segments 1024 and 2048: a walk from 10.
    {
        let env = MemEnv::new(3);
        zeros(&env, 1024);
        zeros(&env, 2048);
        let (walk, seen) = collect(&env, &start(10, PREV_NONE, 0)).expect("the walk returns");
        assert_eq!(walk.end, WalkEnd::StartSegmentMissing { wal_start_lsn: 10 });
        assert!(seen.is_empty());
        assert_eq!(walk.chain_head(&start(10, (7, 9), 0)), (7, 9));
    }
    // S-STORE 7.17: over two recycling runs, every rename's target lies
    // above every segment present and above the append position's segment
    // (RECYCLE-TARGET-LOW).
    {
        let env = MemEnv::new(5);
        for seg in [0u64, 1024, 2048, 3072, 4096] {
            zeros(&env, seg);
        }
        let first = recycle_below(&env, S, 4096, 4212).expect("the first run");
        assert_eq!(
            first,
            vec![(0, 5120), (1024, 6144), (2048, 7168), (3072, 8192)]
        );
        for (source, to) in &first {
            assert!(to > source && *to > 4096 && *to >= 4096 + S);
        }
        // The second run: the append position is now past the first run's
        // lowest target, 5120, and 0's name is free again. The segments
        // present are 4096 to 8192; the one wholly below the bound 5120 is
        // 4096.
        let second = recycle_below(&env, S, 5120, 6000).expect("the second run");
        assert_eq!(second, vec![(4096, 9216)]);
        let present = [4096u64, 5120, 6144, 7168, 8192];
        for (source, to) in &second {
            assert!(
                present.iter().all(|f| to > f),
                "target {to} is not above every present segment"
            );
            assert!(
                *to >= 5120 + S,
                "target {to} is not above the append position's segment"
            );
            assert!(to > source);
        }
        let names = env.list_dir("wal").expect("list");
        let mut firsts: Vec<u64> = names
            .iter()
            .map(|n| u64::from_str_radix(n.strip_suffix(".log").unwrap_or(""), 16).expect("a name"))
            .collect();
        firsts.sort_unstable();
        assert_eq!(firsts, vec![5120, 6144, 7168, 8192, 9216]);
    }
    // S-STORE 7.17: a walk from a start whose segment ends before a byte
    // the walk needs, at the start itself and after records the walk
    // accepted there, reports damage at that position, never a valid end
    // (START-SEG-SHORT-AS-END).
    for (file_len, lsn, accepted) in [(30u64, 0u64, 0usize), (100, 100, 1), (173, 100, 1)] {
        let env = MemEnv::new(7);
        let r1 = assemble(
            0,
            u64::MAX,
            0,
            RecordKind::Commit.byte(),
            1,
            1,
            101,
            &ddl_payload(100, 1),
        );
        let r2 = assemble(
            100,
            0,
            crc_of(&r1),
            RecordKind::Commit.byte(),
            1,
            2,
            102,
            &ddl_payload(100, 2),
        );
        let parts: [(u64, &[u8]); 2] = [(0, &r1), (100, &r2)];
        craft(&env, 0, file_len, &parts[..accepted + 1]);
        let (walk, seen) = collect(&env, &start(0, PREV_NONE, 1)).expect("the walk returns");
        assert_eq!(
            walk.end,
            WalkEnd::ShortRead {
                lsn,
                segment: 0,
                file_len
            },
            "a short start segment is damage"
        );
        assert!(walk.valid_end().is_none());
        assert_eq!(seen.len(), accepted);
    }
}

// ---------------------------------------------------------------------
// wal_model: three boots against a plain list
// ---------------------------------------------------------------------

/// Boot `b`'s plan: `n` and `commit_ts` as the model fixes them
/// (`1000 × b + i`), over the lengths.
fn model_plan(boot: u32, lens: &[u64]) -> Vec<Plan> {
    lens.iter()
        .enumerate()
        .map(|(i, len)| Plan {
            len: *len,
            boot,
            n: i as u64 + 1,
            commit_ts: 1000 * u64::from(boot) + i as u64 + 1,
        })
        .collect()
}

/// The records after which a boot syncs: every fourth, and the last.
fn sync_points(n: usize) -> Vec<usize> {
    let mut v: Vec<usize> = (4..=n).step_by(4).collect();
    if v.last() != Some(&n) {
        v.push(n);
    }
    v
}

const P2_LENS: [u64; 16] = [
    150, 256, 58, 200, 120, 256, 90, 180, 230, 61, 256, 140, 77, 210, 199, 64,
];
const P3_LENS: [u64; 10] = [256, 100, 256, 100, 58, 58, 230, 170, 256, 123];

/// The four sweep modes of boots 1 and 2.
const FOUR: [Mode; 4] = [P0, P1, P2, C];
/// The three sweep modes of boot 3.
const THREE: [Mode; 3] = [P0, P1, C];
/// The mode index of MW: its seeds are `10_000 × 5 + k`.
const MW_INDEX: u64 = 5;

/// Three boots against a plain list (card A-6b, Work 12; S-STORE §12.4
/// "After recovery", I-BOOT-DURABLE in its serial form, §12.2 the chain
/// head, §12.5). Boot 1: every crash point and none, in P0, P1, P2 and C.
/// Boot 2: after six boot-1 histories, resumed from each history's check
/// walk, every crash point and none in the same four modes. Boot 3: after
/// four boot-1-and-2 histories, every crash point and none in P0, P1 and
/// C. In MW (P0, then `media_wal()`), boot 1's every fourth crash point.
/// The oracle is the previous check's accepted list, then the boot's plan
/// placed by `oracle_place` from its resume position. SYNC-OWN-SEGMENTS-
/// ONLY fails it.
///
/// One machine carries one set of knobs (`Sim::new(seed, knobs)`), so in a
/// P1 or P2 round a history's "then P0" cut is a power cut resolved with
/// the round's knobs, and its outcome can depend on the seed: each crash
/// point is therefore taken where the round's own life has that yield
/// ([`sweep_rounds`]). In a P0 round the cut is P0's.
#[test]
fn wal_model() {
    let plan1 = model_plan(1, &fixture_f().iter().map(|p| p.len).collect::<Vec<_>>());
    let plan2 = model_plan(2, &P2_LENS);
    let plan3 = model_plan(3, &P3_LENS);
    let sync1 = sync_points(plan1.len());
    let sync2 = sync_points(plan2.len());
    let sync3 = sync_points(plan3.len());
    assert_eq!(&sync1, &[4, 8, 12, 16, 20, 22]);
    assert_eq!(&sync2, &[4, 8, 12, 16]);
    assert_eq!(&sync3, &[4, 8, 10]);
    let oracle1 = seen_stream(&oracle_stream(0, &plan1));
    assert_eq!(oracle1.len(), 25);
    let mut rounds = 0usize;
    let mut lost_unreported = 0usize;
    let mut crossed_boots = 0usize;
    let mut own_hop = 0usize;
    let mut mw_differs = 0usize;
    let mut yields = Vec::new();

    // ---- Boot 1: every crash point and none ----
    let mut setup = |sim: &mut Sim| create_life(sim);
    let life1 = append_life(CREATING, 1, &plan1, &sync1);
    let mut y1 = 0;
    for mode in FOUR {
        y1 = dry_yields(10_000 * mode.index, &mode, &mut setup, &life1);
        yields.push((mode.name, y1));
        let ran = sweep_rounds(&mode, &mut setup, &life1, &start0(), |k, round| {
            let what = format!("boot 1, {} at crash point {k}", mode.name);
            let m = check_prefix(&round.walk, &round.seen, &oracle1, 0, &what);
            check_covered(m, &oracle1, &round.progress.reports, &what);
            if mode.power_cut.is_none() {
                check_process(m, &oracle1, &round.progress.appended, 0, &what);
            }
            if k == 0 {
                assert_eq!(m, 25, "{what}");
                assert_eq!(round.progress.reports.last(), Some(&4212));
            }
            if mode.name == "P0" && m < round.progress.appended.len() {
                lost_unreported += 1;
            }
        });
        rounds += ran;
        eprintln!("wal_model: boot 1, {}: Y = {y1}, {ran} rounds", mode.name);
    }
    // The card's two derived crash points: "⌊Y1 / 2⌋" and "Y1 − 1, the
    // first `sync_data` of its final sync, which leaves the `SegmentEnd` at
    // 4043 in segment 3072 and records 21–22 in segment 4096 unsynced".
    let half1 = y1 / 2;
    let last1 = y1 - 1;

    // ---- MW: boot 1's every fourth crash point, P0 then media_wal ----
    let mut mw_rounds = 0usize;
    for k in (4..=y1).step_by(4) {
        let seed = 10_000 * MW_INDEX + k;
        let base = run_round(seed, &P0, Some(k), false, &mut setup, &life1, &start0());
        let faulted = run_round(seed, &P0, Some(k), true, &mut setup, &life1, &start0());
        let what = format!("MW at crash point {k}");
        let walk = faulted
            .walk
            .as_ref()
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        match &walk.end {
            WalkEnd::Stop { .. } | WalkEnd::Splice { .. } => {}
            other => panic!("{what}: the walk ended in {other:?}"),
        }
        let m = faulted.seen.len();
        assert_eq!(
            faulted.seen,
            &oracle1[..m],
            "{what}: what it accepted is not a prefix"
        );
        assert_eq!(walk.accepted, m as u64);
        // Non-vacuity: some round's walk differs without its media fault.
        if walk != base.walk.as_ref().expect("the walk returns") || faulted.seen != base.seen {
            mw_differs += 1;
        }
        rounds += 1;
        mw_rounds += 1;
    }

    // ---- Boot 2: after six boot-1 histories ----
    let histories: [(Option<u64>, Mode); 6] = [
        (None, P0),
        (None, C),
        (Some(half1), P0),
        (Some(half1), C),
        (Some(last1), P0),
        (Some(last1), C),
    ];
    // Boot 2's yields without a crash after "boot 1 with no crash" and
    // after "boot 1 crashed at Y1 − 1 then C": the two boot-1 histories of
    // boot 3, whose histories have no power cut and so do not vary.
    let mut y2_of: [u64; 2] = [0; 2];
    for (h, (crash1, mode1)) in histories.iter().enumerate() {
        let hist: Mutex<Hist> = Mutex::new(Hist::default());
        let mut setup = |sim: &mut Sim| {
            create_life(sim);
            let h = boot_and_check(sim, Some(mode1), *crash1, CREATING, 1, &plan1, &sync1);
            // What each crash point means, from the reports it left.
            match *crash1 {
                None => assert_eq!(h.reports.last(), Some(&4212)),
                Some(k) if k == half1 => assert_eq!(h.reports, vec![800, 1324], "⌊Y1 / 2⌋"),
                Some(k) if k == last1 => {
                    assert_eq!(h.reports.len(), 5, "Y1 − 1: before the final sync");
                    assert_eq!(h.reports.last(), Some(&4043));
                }
                Some(_) => unreachable!("a crash point of the six histories"),
            }
            *hist.lock().unwrap() = h;
        };
        let life2 = |env: &SimEnv, progress: &Arc<Mutex<Progress>>| {
            // Read between Env calls.
            let resume = resume_from(&hist.lock().unwrap());
            append_records(env, resume, 2, &plan2, &sync2, progress)
        };
        for mode in FOUR {
            let mut mode_yields = 0;
            let ran = sweep_rounds(&mode, &mut setup, &life2, &start0(), |k, round| {
                let what = format!(
                    "boot 2 after boot 1 at {crash1:?} then {}, {} at crash point {k}",
                    mode1.name, mode.name
                );
                let h = hist.lock().unwrap().clone();
                // The oracle: the history's list, then boot 2's plan placed
                // from where it resumed.
                let stream2 = seen_stream(&oracle_stream(h.valid_end, &plan2));
                check_boot_round(
                    round,
                    &h,
                    &stream2,
                    &h.reports,
                    mode.power_cut.is_none(),
                    &what,
                );
                // Non-vacuity: a boot-2 record that follows boot-1 records,
                // and a record past a hop that boot 2 wrote.
                if round.seen.iter().any(|r| r.boot == 2)
                    && round.seen.first().is_some_and(|r| r.boot == 1)
                {
                    crossed_boots += 1;
                }
                if past_own_hop(&round.seen, 2) {
                    own_hop += 1;
                }
                mode_yields = mode_yields.max(k);
            });
            rounds += ran;
            if h < 2 {
                y2_of[h] = mode_yields.max(y2_of[h]);
            }
            eprintln!(
                "wal_model: boot 2 after boot 1 at {crash1:?} then {}, {}: {ran} rounds, to crash point {mode_yields}",
                mode1.name, mode.name
            );
        }
    }

    // ---- Boot 3: after four boot-1-and-2 histories ----
    // "boot 1 with no crash, or crashed at Y1 − 1 then C": neither has a
    // power cut. The cut after boot 2's crash is P0's.
    for (which, crash1) in [None, Some(last1)].into_iter().enumerate() {
        // Boot 2's yields after that boot-1 history: ⌊Y2 / 2⌋ is the
        // crash point of the second boot-2 history.
        let half2 = y2_of[which] / 2;
        for crash2 in [None, Some(half2)] {
            let cut2: Option<&Mode> = if crash2.is_some() { Some(&P0) } else { None };
            let hists: Mutex<(Hist, Hist)> = Mutex::new((Hist::default(), Hist::default()));
            let mut setup = |sim: &mut Sim| {
                create_life(sim);
                let h1 = boot_and_check(sim, Some(&C), crash1, CREATING, 1, &plan1, &sync1);
                let h2 = boot_and_check(sim, cut2, crash2, resume_from(&h1), 2, &plan2, &sync2);
                *hists.lock().unwrap() = (h1, h2);
            };
            let life3 = |env: &SimEnv, progress: &Arc<Mutex<Progress>>| {
                // Read between Env calls.
                let resume = resume_from(&hists.lock().unwrap().1);
                append_records(env, resume, 3, &plan3, &sync3, progress)
            };
            for mode in THREE {
                let mut mode_yields = 0;
                let ran = sweep_rounds(&mode, &mut setup, &life3, &start0(), |k, round| {
                    let what = format!(
                        "boot 3 after boot 1 at {crash1:?}, boot 2 at {crash2:?} then {:?}, {} at crash point {k}",
                        cut2.map(|m| m.name),
                        mode.name
                    );
                    let (h1, h2) = hists.lock().unwrap().clone();
                    let stream3 = seen_stream(&oracle_stream(h2.valid_end, &plan3));
                    let mut reports = h1.reports.clone();
                    reports.extend_from_slice(&h2.reports);
                    check_boot_round(
                        round,
                        &h2,
                        &stream3,
                        &reports,
                        mode.power_cut.is_none(),
                        &what,
                    );
                    if past_own_hop(&round.seen, 3) {
                        own_hop += 1;
                    }
                    mode_yields = mode_yields.max(k);
                });
                rounds += ran;
                eprintln!(
                    "wal_model: boot 3 after boot 1 at {crash1:?}, boot 2 at {crash2:?} then {:?}, {}: {ran} rounds, to crash point {mode_yields}",
                    cut2.map(|m| m.name),
                    mode.name
                );
            }
        }
    }
    assert!(
        lost_unreported > 0,
        "no P0 round lost a written, unreported record"
    );
    assert!(
        crossed_boots > 0,
        "no boot-2 check accepted a boot-2 record that follows boot-1 records"
    );
    assert!(
        own_hop > 0,
        "no boot-2 or boot-3 check accepted a record past a hop that boot wrote"
    );
    assert!(
        mw_differs > 0,
        "no MW round's walk differs from the same round without the media fault"
    );
    eprintln!("wal_model: Y1 = {y1}; {rounds} rounds, {mw_rounds} of them MW");
}
