//! C-T6g regress (card item 6): the always-present pins — the program
//! generators' determinism, the same-seed/same-op-stream contract, and
//! the card's hand-scheduled history: an RR session takes its snapshot
//! and reads slot 0, the DDL session runs the whole online build to
//! publish, then the RR session IndexScans `[0, 12)` and commits.
//!
//! The hand-scheduled history is also the deterministic killer of the
//! binding mutant N6 (`index_scan` ignores `built_at`, C-T6b2b): the
//! ingest landed at `S = built_at` above the reader's `S_R`, so an
//! index served at `S_R` misses every row — while every committed
//! history explains all twelve. The honest engine refuses the index at
//! `S_R` (§4: `Public` and `built_at <= S`), the statement answers
//! through the pk scan, and the checker stays clean at every level.

mod g2x;

use g2x::g2::check::{check, Verdict};
use g2x::g2::history::{Fate, History, Level, Obs, OpKind};
use g2x::runner::{
    build_at_txn, gen_ddl, gen_prog, iv, m_def, run_workload, Config, Mix, PDdl, POp, Prog, Rig,
    SLOTS,
};

use nucleus_catalog::{create_index_online, BuildHooks, IndexState};

/// The verdict of `h` at `level`, asserting it is clean.
fn clean(h: &History, level: Level) -> Verdict {
    let verdict = check(h, level);
    assert!(
        verdict.violations().is_empty(),
        "unexpected violation at {}:\n{}{}",
        level.short(),
        verdict.report(h),
        h.trace()
    );
    verdict
}

// ----- determinism ------------------------------------------------------------

/// Programs and DDL picks are a pure function of their inputs: the same
/// triple gives the same program, a different seed gives different ones,
/// and the DDL mix actually contains every kind.
#[test]
fn t6g_programs_are_a_pure_function_of_the_seed() {
    for session in 1..=4u16 {
        for idx in 0..60 {
            let a = gen_prog(0x1234, session, idx, &Mix::STANDARD);
            let b = gen_prog(0x1234, session, idx, &Mix::STANDARD);
            assert_eq!(a, b, "session {session} prog {idx}");
        }
    }
    let differs = (0..60)
        .any(|i| gen_prog(0x1234, 1, i, &Mix::STANDARD) != gen_prog(0x1235, 1, i, &Mix::STANDARD));
    assert!(differs, "a different seed must give different programs");
    // The DDL generator: pure in (seed, index, cols, built), every kind
    // reachable, the build exactly once at its seeded txn.
    for seed in [0x1234u64, 0x6733_0001] {
        let mut cols = 0;
        let mut built = false;
        let mut builds = 0;
        let mut kinds = std::collections::BTreeSet::new();
        for i in 0..200u32 {
            let a = gen_ddl(seed, i, cols, built);
            let b = gen_ddl(seed, i, cols, built);
            assert_eq!(a, b);
            kinds.insert(match a {
                PDdl::AddColumn => "add",
                PDdl::Truncate => "trunc",
                PDdl::CreateIndex => "cidx",
            });
            match a {
                PDdl::AddColumn => cols += 1,
                PDdl::CreateIndex => {
                    assert!(!built, "one build per run");
                    assert_eq!(i, build_at_txn(seed), "the build at its seeded txn");
                    builds += 1;
                    built = true;
                }
                PDdl::Truncate => {}
            }
        }
        assert_eq!(builds, 1, "exactly one build in 200 DDL txns");
        assert_eq!(kinds.len(), 3, "every DDL kind appears: {kinds:?}");
        assert!(cols > 0, "the mix adds columns: {cols}");
    }
    // The build point is a pure function of the seed too.
    assert_eq!(build_at_txn(0x1234), build_at_txn(0x1234));
    assert_ne!(build_at_txn(0x1234), build_at_txn(0x1235));
}

/// The statement shape a program is recorded as.
fn shape(p: &Prog) -> Vec<(OpKind, Vec<u8>)> {
    p.ops
        .iter()
        .map(|op| match *op {
            POp::Read(k) => (OpKind::Read, vec![k]),
            POp::Scan(lo, hi) => (OpKind::Scan, vec![lo, hi]),
            POp::IndexScan(lo, hi) => (OpKind::Scan, vec![lo, hi]),
            POp::Insert(k) => (OpKind::Insert, vec![k]),
            POp::Update(k) => (OpKind::Update, vec![k]),
            POp::Delete(k) => (OpKind::Delete, vec![k]),
        })
        .collect()
}

/// Same seed, same recorded op streams per DML session: every txn that
/// ran its whole program (committed or rolled back on purpose) recorded
/// exactly the statements the seed's program for `(session, index)` says,
/// however the threads interleaved; and the checker's verdict is a
/// function of the history it is given.
#[test]
fn t6g_same_seed_same_op_streams() {
    let seed = 0x6733_0042;
    let mut histories: Vec<History> = Vec::new();
    for _ in 0..2 {
        let mut cfg = Config::new(seed, Level::ReadCommitted);
        cfg.progs = 40;
        cfg.ddl_txns = 40;
        histories.push(run_workload(&cfg).history);
    }
    for h in &histories {
        let mut finished = 0;
        for t in &h.txns {
            if t.session == 0
                || t.session > 3
                || !matches!(t.fate, Fate::Committed(_) | Fate::Rolledback)
            {
                continue;
            }
            let prog = gen_prog(seed, t.session, t.prog, &Mix::STANDARD);
            let recorded: Vec<(OpKind, Vec<u8>)> =
                t.ops.iter().map(|o| (o.kind, o.keys.clone())).collect();
            assert_eq!(
                recorded,
                shape(&prog),
                "session {} prog {}",
                t.session,
                t.prog
            );
            finished += 1;
        }
        assert!(finished > 60, "only {finished} finished programs");
        // The same history checked twice gives the same verdict.
        assert_eq!(
            check(h, Level::ReadCommitted),
            check(h, Level::ReadCommitted)
        );
    }
}

// ----- the hand-scheduled history (card item 6, the N6 killer) ------------------

/// The card's history: an RR session takes its snapshot and reads slot
/// 0, the DDL session runs the whole online build to publish, then the
/// RR session IndexScans `[0, 12)` and commits. The index's `built_at`
/// sits strictly above the reader's snapshot, so the honest engine
/// refuses the index (the runner's IndexScan falls back to the pk scan)
/// and the history is clean at every level. Under the N6 mutant the
/// engine serves the index at `S_R` instead, the entries ingested at
/// `built_at > S_R` are invisible there, the scan misses all twelve
/// rows, and the checker flags the non-repeatable read / snapshot
/// violation — this test fails.
#[test]
fn t6g_hand_scheduled_rr_index_scan_across_a_publish() {
    let mut rig = Rig::new(5);
    rig.setup();

    // The RR session: its snapshot, then slot 0 at the setup value.
    let mut rr = rig.session(1, Level::RepeatableRead);
    let s_r = rr.snapshot();
    assert_eq!(
        rr.read(0).expect("rr reads slot 0"),
        Some(iv(0)),
        "slot 0 at the txn snapshot"
    );

    // The DDL session's whole online build, to the publish.
    let idx_oid = create_index_online(
        rig.db.as_ref(),
        rig.t_oid,
        &m_def(),
        true,
        &BuildHooks::default(),
    )
    .expect("the online build publishes");
    let def = rig.idx_def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);
    assert!(
        def.built_at > s_r.0,
        "built_at ({}) is strictly above the reader's snapshot ({})",
        def.built_at,
        s_r.0
    );

    // The IndexScan over the whole slot range, at the RR claim.
    let rows = rr.index_scan(0, SLOTS).expect("the index scan statement");
    assert_eq!(
        rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        (0..SLOTS).collect::<Vec<_>>(),
        "every slot is observed, at the claimed snapshot's values"
    );
    assert_eq!(rows[0].1, iv(0), "slot 0 repeats its earlier read");
    rr.commit().expect("rr commits");

    // The fallback is what served the statement: the engine refused the
    // index at S_R (§4's rule), exactly once.
    let run = rig.finish();
    assert_eq!(run.counts.index_scan_fallbacks, 1, "the refusal");
    assert_eq!(run.counts.index_scans_served, 0, "nothing served");
    assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);

    // The record: one Scan op over [0, SLOTS) at S_R whose reads are all
    // twelve slots at their snapshot values (a pk scan's observations).
    let h = &run.history;
    let rr_txn = h
        .txns
        .iter()
        .find(|t| t.level == Level::RepeatableRead)
        .expect("the RR txn");
    let scan = rr_txn
        .ops
        .iter()
        .find(|o| o.kind == OpKind::Scan && o.keys == vec![0, SLOTS])
        .expect("the recorded index scan");
    assert_eq!(scan.snap, Some(s_r.0), "the scan claims the txn snapshot");
    assert_eq!(scan.reads.len(), usize::from(SLOTS));
    for (k, obs) in &scan.reads {
        assert_eq!(*obs, Obs::Val(iv(*k)), "slot {k} at its snapshot value");
    }

    // Clean at every level the history's sessions claim (RR) and above:
    // a single-reader history is serializable.
    clean(h, Level::ReadCommitted);
    clean(h, Level::RepeatableRead);
    let ser = clean(h, Level::Serializable);
    assert!(ser.serializable(), "{}", ser.report(h));
}

/// The served arm of the same statement, one commit later: a fresh RR
/// txn above `built_at` has its IndexScan served by the engine through
/// the published index (the counter the soak's floors run on), and the
/// history stays clean.
#[test]
fn t6g_hand_scheduled_index_scan_served_above_built_at() {
    let mut rig = Rig::new(5);
    rig.setup();
    let idx_oid = create_index_online(
        rig.db.as_ref(),
        rig.t_oid,
        &m_def(),
        true,
        &BuildHooks::default(),
    )
    .expect("the online build publishes");
    let def = rig.idx_def_of(idx_oid);
    assert_eq!(def.state, IndexState::Public);

    let mut rr = rig.session(1, Level::RepeatableRead);
    assert!(
        rr.snapshot().0 >= def.built_at,
        "the snapshot is above built_at"
    );
    let rows = rr.index_scan(0, SLOTS).expect("the served index scan");
    assert_eq!(rows.len(), usize::from(SLOTS));
    rr.commit().expect("rr commits");

    let run = rig.finish();
    assert_eq!(run.counts.index_scans_served, 1, "the engine served it");
    assert_eq!(run.counts.index_scan_fallbacks, 0);
    assert_eq!(run.leaked_intents, 0, "{}", run.leak_report);
    clean(&run.history, Level::Serializable);
}
