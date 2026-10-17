//! C-T6g inject (card item 5): the checker's proof of life over the
//! typed layer. Three harness-level bugs — ways a *store* could be
//! broken, arranged entirely in the harness (the engine is never
//! edited) — each of which the unchanged G2 checker, re-used verbatim
//! from `nucleus-txn`'s test tree, must flag in **5 of 5 runs** of its
//! deterministic script; each script also runs bug-free and stays clean
//! (the no-crying-wolf control):
//!
//! - **(a)** an IndexScan at a fresh snapshot while claiming RR (a
//!   concurrent commit lands between two statements of the txn and
//!   reads back — a non-repeatable read);
//! - **(b)** a TRUNCATE recorded without its `Del`s (the rows really
//!   vanish; the next reader observes an absence no committed history
//!   explains — a snapshot violation);
//! - **(c)** an IndexScan bypassing the `built_at` rule through a
//!   test-only raw entry scan during the build (the ingest landed above
//!   the reader's snapshot, the raw scan sees no entry at all, and the
//!   recorded scan misses rows every committed history explains).
//!
//! (c) is the harness twin of the binding engine mutant N6 (`index_scan`
//! ignores `built_at`, C-T6b2b), whose deterministic killer is the
//! hand-scheduled `t6g_regress` history.

mod g2x;

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use g2x::g2::check::{check, Kind, Verdict};
use g2x::g2::history::{History, Level};
use g2x::runner::{iv, m_def, run_truncate, Bug, Rig};

use nucleus_catalog::{create_index_online, BuildHooks, IndexState};

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, never a pass).
const BOUND: Duration = Duration::from_secs(30);

/// The runs each bug's proof of life needs.
const RUNS: usize = 5;

fn caught(h: &History, level: Level, kind: Kind) -> Verdict {
    let verdict = check(h, level);
    assert!(
        verdict.violates(kind),
        "the checker stayed silent about {kind:?} at {}:\n{}{}",
        level.short(),
        verdict.report(h),
        h.trace()
    );
    verdict
}

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

/// `rig.finish()` plus the leak assertion.
fn live(rig: &Rig) -> History {
    let run = rig.finish();
    assert_eq!(run.leaked_intents, 0, "I-LEAK\n{}", run.leak_report);
    run.history
}

// ----- (a) an IndexScan at a fresh snapshot under an RR claim ---------------------

/// The script: the m-index is published before the reader begins (so
/// the IndexScan truly goes through the engine's index path), the RR
/// session reads slot 0 at its snapshot, a concurrent writer commits a
/// new value of slot 0, and the session IndexScans `[0, 12)`. Bug-free,
/// the index scan runs at the txn snapshot and sees the old value; under
/// the bug it runs at a fresh statement snapshot, sees the new value,
/// and the RR claim is a lie.
fn script_fresh_snapshot(bug: Bug) -> History {
    let mut rig = Rig::new(5);
    rig.setup();
    let idx_oid = create_index_online(
        rig.db.as_ref(),
        rig.t_oid,
        &m_def(),
        true,
        &BuildHooks::default(),
    )
    .expect("the setup index build");
    assert_eq!(rig.idx_def_of(idx_oid).state, IndexState::Public);

    let mut rr = rig.session_with(1, Level::RepeatableRead, bug);
    assert_eq!(rr.read(0).expect("rr reads slot 0"), Some(iv(0)));

    let mut w = rig.session(2, Level::ReadCommitted);
    assert!(w.update(0).expect("w updates slot 0"));
    w.commit().expect("w commits");

    let rows = rr
        .index_scan(0, g2x::runner::SLOTS)
        .expect("the index scan");
    rr.commit().expect("rr commits");
    assert_eq!(rows.len(), usize::from(g2x::runner::SLOTS));
    live(&rig)
}

#[test]
fn t6g_inject_index_scan_fresh_snapshot_rr() {
    for _ in 0..RUNS {
        clean(&script_fresh_snapshot(Bug::None), Level::RepeatableRead);
        let h = script_fresh_snapshot(Bug::IndexScanFreshSnapshot);
        let v = caught(&h, Level::RepeatableRead, Kind::NonRepeatableRead);
        assert!(v.stats.committed >= 3, "{:?}", v.stats);
    }
}

// ----- (b) a TRUNCATE recorded without its Dels -----------------------------------

/// The script: a writer moves slot 0's value, the DDL session truncates
/// (bug-free: `Del` of every live slot; under the bug: reads recorded,
/// `Del`s dropped), and a fresh RC reader reads slot 0. Bug-free the
/// absence is the truncate's own `Del` version; under the bug the newest
/// version of slot 0 at the reader's snapshot is the writer's live
/// value, and the observed absence is a snapshot violation.
fn script_truncate_no_dels(bug: Bug) -> History {
    let mut rig = Rig::new(5);
    rig.setup();
    let mut w = rig.session(1, Level::ReadCommitted);
    assert!(w.update(0).expect("w updates slot 0"));
    w.commit().expect("w commits");

    assert_eq!(
        run_truncate(&rig, 0, 0, bug),
        g2x::runner::DdlOutcome::Committed,
        "the truncate commits either way"
    );

    let mut r = rig.session(2, Level::ReadCommitted);
    assert_eq!(r.read(0).expect("r reads slot 0"), None, "the row is gone");
    r.commit().expect("r commits");
    live(&rig)
}

#[test]
fn t6g_inject_truncate_without_dels() {
    for _ in 0..RUNS {
        clean(&script_truncate_no_dels(Bug::None), Level::ReadCommitted);
        let h = script_truncate_no_dels(Bug::TruncateNoDels);
        let v = caught(&h, Level::ReadCommitted, Kind::SnapshotViolation);
        assert!(v.stats.committed >= 3, "{:?}", v.stats);
    }
}

// ----- (c) a raw entry scan during the build --------------------------------------

/// The script: the RR session reads slot 0 at its snapshot `S_R`; the
/// DDL session's online build runs on its own thread until the publish
/// window — the ingest has landed at `S > S_R`, the index not yet
/// `Public`; the session IndexScans `[0, 12)` (bug-free: the engine
/// refuses the index and the statement answers through the pk scan;
/// under the bug: the raw entry scan at `S_R` sees no entry at all) and
/// the build is released to publish. Under the bug the recorded scan
/// misses all twelve rows at a snapshot where every one of them is live.
fn script_raw_entry_scan(bug: Bug) -> History {
    let mut rig = Rig::new(5);
    rig.setup();

    let mut rr = rig.session_with(1, Level::RepeatableRead, bug);
    assert_eq!(rr.read(0).expect("rr reads slot 0"), Some(iv(0)));

    // The build parks in the publish window (entries ingested, def not
    // yet Public) until the reader's scan is recorded. The ack receiver
    // lives behind a mutex: a hook must be `Fn() + Send + Sync` and a
    // `Receiver` is `!Sync` (the e2b `ReleaseKit` pattern).
    let (window_tx, window_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
    let db = std::sync::Arc::clone(&rig.db);
    let t_oid = rig.t_oid;
    let build = {
        let window_tx = window_tx.clone();
        let release_rx = std::sync::Arc::clone(&release_rx);
        thread::spawn(move || {
            let hooks = BuildHooks {
                before_publish: Some(Box::new(move || {
                    let _ = window_tx.send(());
                    match release_rx.lock().expect("release").recv_timeout(BOUND) {
                        Ok(()) => {}
                        Err(_) => panic!("the build was never released"),
                    }
                })),
                ..Default::default()
            };
            create_index_online(db.as_ref(), t_oid, &m_def(), true, &hooks)
        })
    };
    match window_rx.recv_timeout(BOUND) {
        Ok(()) => {}
        Err(_) => panic!("the build never reached the publish window"),
    }

    // Mid-build: the ingest has landed above S_R and the def is not yet
    // Public — the exact state §4's usability rule refuses.
    rr.index_scan(0, g2x::runner::SLOTS)
        .expect("the index scan");
    rr.commit().expect("rr commits");
    let _ = release_tx.send(());
    let idx_oid = match build.join().expect("the build thread") {
        Ok(oid) => oid,
        Err(e) => panic!("the build failed: {e:?}"),
    };
    assert_eq!(
        rig.idx_def_of(idx_oid).state,
        IndexState::Public,
        "the build published after the release"
    );
    live(&rig)
}

#[test]
fn t6g_inject_raw_entry_scan_during_build() {
    for _ in 0..RUNS {
        // The control: the engine's own refusal sends the statement to
        // the pk scan, which observes all twelve slots at S_R — clean.
        let h = script_raw_entry_scan(Bug::None);
        clean(&h, Level::RepeatableRead);

        // The bug: the raw scan at S_R sees no entry (the ingest sits
        // above S_R), so every slot reads back absent — a
        // non-repeatable read and a snapshot violation at the RR claim.
        let h = script_raw_entry_scan(Bug::RawEntryScanDuringBuild);
        let v = caught(&h, Level::RepeatableRead, Kind::NonRepeatableRead);
        caught(&h, Level::RepeatableRead, Kind::SnapshotViolation);
        assert!(v.stats.committed >= 2, "{:?}", v.stats);
    }
}
