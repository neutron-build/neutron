//! C-T6c2 item 1, the injected-bug suite: the C-T6c1 differential can
//! fail. Five harness-level defects (G2's `Bug` pattern —
//! `diff::runner::Bug`, the engine never edited), each driven by a
//! deterministic script over the real rig; the unchanged checker
//! (`diff::check`) must report each bug in **5 of 5 runs** of its fixed
//! seed set, and the same scripts bug-free must check clean.
//!
//! The scripts are hand-scheduled (single-threaded, no interleavings to
//! flake on: the schedules are the whole point — each arranges exactly
//! the window its bug needs), and the seeds vary the scripts' write
//! constants. What each bug is and which check bites:
//!
//! | bug | shape | caught by |
//! |---|---|---|
//! | B1 `DecodeAtSeenSchema` | rows recorded at the schema the txn's begin-time ghost read saw, not the handle's reported def | a Get mismatch: the added column vanishes from the record where the model renders its default |
//! | B2 `RrPerStmtSnapshot` | an RR session whose statements run at fresh snapshots while the record keeps the claimed one | a Get mismatch: a foreign commit reads back a row the model cannot have at the claimed snapshot |
//! | B3 `AbortedAsCommitted` | a write txn recorded committed but aborted by the harness | a Get over the phantom row (and the final-state check) |
//! | B4 `FullRowUpdateFromRead` | UpdateSet computed from the row the session last read, written as a full-row update | the write-effect oracle: the read-back differs from the recompute at the commit position (the lost update) |
//! | B5 `MissingDefaultAsNull` | a decoded value equal to the column's ADD COLUMN default recorded as NULL | a Get mismatch: old rows lose their default in the record |
//!
//! B1/B5 are the harness twins of the E1 codec mutant (a decode rule
//! ignoring `missing`), B2 of G2's `PerStmtSnapshot`, B4 of G2's
//! application-level lost update — none of them needs an engine edit,
//! which is the point: a differential that cannot fail proves nothing.

mod diff;
// diff/crash.rs (pulled in by diff/mod.rs) drives the t6b crash rig.
mod t6b_support;

use diff::check;
use diff::runner::{Bug, Rig, Session, DDL_SESSION};
use diff::{derive_const, derive_text, Level, Run, Verdict, TAG_DDL, TAG_T_A, TAG_T_B, TAG_T_SET};
use nucleus_codec::Value;

/// The five fixed seeds every bug's script runs over.
const SEEDS: [u64; 5] = [
    0x7cc2_0001,
    0x7cc2_0002,
    0x7cc2_0003,
    0x7cc2_0004,
    0x7cc2_0005,
];

fn rig() -> Rig {
    let mut rig = Rig::new(5);
    rig.setup();
    rig
}

/// The recorded DDL txn every script uses: ADD COLUMN `name` int8 with a
/// constant default, its own RC txn (the soak's DDL session pattern).
fn add_column(rig: &Rig, name: &str, default: i64) {
    let mut s = Session::begin_with(rig, DDL_SESSION, 0, 0, Level::ReadCommitted, Bug::None);
    s.op_add_column(name, Some(default))
        .unwrap_or_else(|e| panic!("the ADD COLUMN failed: {e}"));
    s.commit()
        .unwrap_or_else(|e| panic!("the DDL commit failed: {e}"));
}

/// The bug-free control of every script must check clean — the guard
/// against an oracle that cries wolf on legal histories.
fn clean(script: impl Fn(Bug, u64) -> Run, seed: u64) {
    let run = script(Bug::None, seed);
    let verdict = check(&run);
    assert!(
        verdict.is_clean(),
        "the bug-free run of seed {seed:#x} is not clean:\n{}",
        verdict.report()
    );
}

/// The buggy run must be reported, and by the check the bug exists to
/// exercise (never only by some unrelated violation).
fn reported(script: impl Fn(Bug, u64) -> Run, bug: Bug, seed: u64, needle: &str) {
    let run = script(bug, seed);
    let verdict = check(&run);
    assert!(
        !verdict.is_clean(),
        "{bug:?} at seed {seed:#x} went unnoticed:\n{}",
        verdict.report()
    );
    assert!(
        verdict.violations.iter().any(|v| v.contains(needle)),
        "{bug:?} at seed {seed:#x} was not reported by {needle:?}:\n{}",
        verdict.report()
    );
}

// ----- B1: rows decoded with the schema at S, not the reported def ---------

/// An RR txn begins (B1 snapshots the live def with a begin-time ghost
/// read), the ADD COLUMN commits under it — the txn holds no relation
/// lock until its first statement, so the DDL sails through — and the
/// txn's *first* `t` statement then answers at the reported def v2
/// while B1 records its rows at the v1 schema: the new column vanishes
/// from the record.
fn b1(bug: Bug, seed: u64) -> Run {
    let rig = rig();
    let d = derive_const(seed, DDL_SESSION, 0, 0, 0, TAG_DDL);
    let mut a = Session::begin_with(&rig, 1, 0, 0, Level::RepeatableRead, bug);
    // The DDL commits between the txn's begin and its first statement.
    add_column(&rig, "c9", d);
    a.op_get(0)
        .unwrap_or_else(|e| panic!("the get failed: {e}"));
    a.commit()
        .unwrap_or_else(|e| panic!("the commit failed: {e}"));
    rig.finish()
}

#[test]
fn t6c_injected_b1_decode_at_seen_schema() {
    for seed in SEEDS {
        reported(b1, Bug::DecodeAtSeenSchema, seed, "Get pk 0 returned");
        clean(b1, seed);
    }
}

// ----- B2: an RR claim with a fresh snapshot per statement -------------------

/// The RR txn reads pk 0, a foreign RC txn updates its `a` and commits
/// (RowExclusive does not conflict with the reader's AccessShare), and
/// the RR txn reads pk 0 again: B2's statement runs at a fresh snapshot
/// — it sees the new `a` — while the record keeps the claimed snapshot,
/// at which the model still has the old one.
fn b2(bug: Bug, seed: u64) -> Run {
    let rig = rig();
    let x = Value::Int8(derive_const(seed, 2, 0, 0, 0, TAG_T_A));
    let mut a = Session::begin_with(&rig, 1, 0, 0, Level::RepeatableRead, bug);
    a.op_get(0)
        .unwrap_or_else(|e| panic!("the first get failed: {e}"));
    {
        let mut b = Session::begin_with(&rig, 2, 0, 0, Level::ReadCommitted, Bug::None);
        b.op_update_set(0, 0, x, Value::Text(String::new()))
            .unwrap_or_else(|e| panic!("the foreign update failed: {e}"));
        b.commit()
            .unwrap_or_else(|e| panic!("the foreign commit failed: {e}"));
    }
    a.op_get(0)
        .unwrap_or_else(|e| panic!("the second get failed: {e}"));
    a.commit()
        .unwrap_or_else(|e| panic!("the commit failed: {e}"));
    rig.finish()
}

#[test]
fn t6c_injected_b2_rr_per_stmt_snapshot() {
    for seed in SEEDS {
        reported(b2, Bug::RrPerStmtSnapshot, seed, "Get pk 0 returned");
        clean(b2, seed);
    }
}

// ----- B3: a write recorded committed but run in an aborted txn --------------

/// The buggy session inserts a fresh pk and "commits" — B3 aborts the
/// engine txn while recording `Committed` — and an honest reader then
/// finds no row where the model has one (the final scan agrees with the
/// reader, not the record).
fn b3(bug: Bug, seed: u64) -> Run {
    let rig = rig();
    let a_val = Value::Int8(derive_const(seed, 1, 0, 0, 0, TAG_T_A));
    let b_val = Value::Text(derive_text(seed, 1, 0, 0, 0, TAG_T_B));
    {
        let mut a = Session::begin_with(&rig, 1, 0, 0, Level::ReadCommitted, bug);
        a.op_insert_t(40, a_val, Some(b_val))
            .unwrap_or_else(|e| panic!("the insert failed: {e}"));
        a.commit()
            .unwrap_or_else(|e| panic!("the (recorded) commit failed: {e}"));
    }
    {
        let mut r = Session::begin_with(&rig, 2, 0, 0, Level::ReadCommitted, Bug::None);
        r.op_get(40)
            .unwrap_or_else(|e| panic!("the read-back failed: {e}"));
        r.commit()
            .unwrap_or_else(|e| panic!("the reader commit failed: {e}"));
    }
    rig.finish()
}

#[test]
fn t6c_injected_b3_aborted_as_committed() {
    for seed in SEEDS {
        reported(b3, Bug::AbortedAsCommitted, seed, "the model has");
        clean(b3, seed);
    }
}

// ----- B4: UpdateSet computed from the read row, written whole ---------------

/// The session reads pk 0 (B4 remembers the row), a foreign RC txn
/// rewrites its `b` and commits, and the session's own UPDATE sets `a`:
/// B4 writes the whole row computed from the row it read — the stale
/// `b` clobbers the foreign one — and the oracle's recompute at the
/// commit position (the foreign row plus the recorded one-column set)
/// no longer equals the read-back. The bug-free control runs the same
/// schedule: honest `SET a` re-bases on the foreign row at RC and keeps
/// its `b`.
fn b4(bug: Bug, seed: u64) -> Run {
    let rig = rig();
    let av = Value::Int8(derive_const(seed, 1, 0, 0, 1, TAG_T_SET));
    let tv = Value::Text(derive_text(seed, 2, 0, 0, 1, TAG_T_SET));
    let mut a = Session::begin_with(&rig, 1, 0, 0, Level::ReadCommitted, bug);
    a.op_get(0)
        .unwrap_or_else(|e| panic!("the first get failed: {e}"));
    {
        let mut b = Session::begin_with(&rig, 2, 0, 0, Level::ReadCommitted, Bug::None);
        // choice 1: the live non-pk columns of the fixture are
        // [a (2), b (3)], so this sets `b`.
        b.op_update_set(0, 1, Value::Int8(0), tv.clone())
            .unwrap_or_else(|e| panic!("the foreign update failed: {e}"));
        b.commit()
            .unwrap_or_else(|e| panic!("the foreign commit failed: {e}"));
    }
    // choice 0: this sets `a`.
    a.op_update_set(0, 0, av, Value::Text(String::new()))
        .unwrap_or_else(|e| panic!("the update failed: {e}"));
    a.commit()
        .unwrap_or_else(|e| panic!("the commit failed: {e}"));
    rig.finish()
}

#[test]
fn t6c_injected_b4_full_row_update_from_read() {
    for seed in SEEDS {
        reported(
            b4,
            Bug::FullRowUpdateFromRead,
            seed,
            "differs from the recompute",
        );
        clean(b4, seed);
    }
}

// ----- B5: the missing default rendered as NULL -------------------------------

/// The ADD COLUMN with a constant default commits first, then the
/// reader runs: the setup row — written before the column existed —
/// decodes the default through §3's missing rule, and B5 records NULL
/// where the model renders the default.
fn b5(bug: Bug, seed: u64) -> Run {
    let rig = rig();
    let d = derive_const(seed, DDL_SESSION, 0, 0, 0, TAG_DDL);
    add_column(&rig, "c9", d);
    let mut a = Session::begin_with(&rig, 1, 0, 0, Level::ReadCommitted, bug);
    a.op_get(0)
        .unwrap_or_else(|e| panic!("the get failed: {e}"));
    a.commit()
        .unwrap_or_else(|e| panic!("the commit failed: {e}"));
    rig.finish()
}

#[test]
fn t6c_injected_b5_missing_default_as_null() {
    for seed in SEEDS {
        reported(b5, Bug::MissingDefaultAsNull, seed, "Get pk 0 returned");
        clean(b5, seed);
    }
}

// ----- the whole suite, one verdict at a time ----------------------------------

/// Every script over every seed in one place: the 5-of-5 split of each
/// bug's reported runs and its bug-free control's clean runs, printed
/// per bug and asserted (the per-bug tests above pin the same thing;
/// this one prints the summary line).
#[test]
fn t6c_injected_summary() {
    /// One suite row: the script, its bug, and the violation it must produce.
    type Suite = (&'static str, fn(Bug, u64) -> Run, Bug, &'static str);
    let suites: [Suite; 5] = [
        ("B1", b1, Bug::DecodeAtSeenSchema, "Get pk 0 returned"),
        ("B2", b2, Bug::RrPerStmtSnapshot, "Get pk 0 returned"),
        ("B3", b3, Bug::AbortedAsCommitted, "the model has"),
        (
            "B4",
            b4,
            Bug::FullRowUpdateFromRead,
            "differs from the recompute",
        ),
        ("B5", b5, Bug::MissingDefaultAsNull, "Get pk 0 returned"),
    ];
    for (name, script, bug, needle) in suites {
        let mut flagged = 0;
        let mut clean_runs = 0;
        for seed in SEEDS {
            let v: Verdict = check(&script(bug, seed));
            if v.violations.iter().any(|x| x.contains(needle)) {
                flagged += 1;
            }
            let v: Verdict = check(&script(Bug::None, seed));
            if v.is_clean() {
                clean_runs += 1;
            }
        }
        println!("t6c injected {name}: reported {flagged}/5, bug-free clean {clean_runs}/5");
        assert_eq!(flagged, 5, "{name}: every seed must report the bug");
        assert_eq!(clean_runs, 5, "{name}: every bug-free run must be clean");
    }
}
