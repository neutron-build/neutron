use nucleus_g0::check;
use nucleus_g0::ssi::{committed_ts, raised_structures, Action, Bug, Key, SsiModel, State, Work};
use nucleus_g0::Model;

fn max_states() -> usize {
    std::env::var("G0_MAX_STATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000_000)
}

#[test]
fn g0_ssi_clean_model_holds() {
    let r = check(&SsiModel::default(), max_states());
    eprintln!("G0-ssi: {} states, {} transitions", r.states, r.transitions);
    assert!(!r.truncated, "state space exceeds {}", max_states());
    if let Some(v) = r.violation {
        panic!("{}\ntrace:\n  {}", v.message, v.trace.join("\n  "));
    }
}

#[test]
fn g0_ssi_catches_every_seed() {
    let seeds: Vec<u8> = Bug::ALL.iter().map(|b| b.seed()).collect();
    assert_eq!(
        seeds,
        vec![5, 6, 20, 21, 28, 29, 30, 32, 36, 39, 40, 43, 60, 61, 62, 63],
        "Bug::ALL must map to the card's seed list, in order"
    );
    for bug in Bug::ALL {
        let r = check(
            &SsiModel {
                bug: Some(bug),
                ..Default::default()
            },
            max_states(),
        );
        let v = r.violation.unwrap_or_else(|| {
            panic!(
                "seed {} ({bug:?}) not caught in {} states",
                bug.seed(),
                r.states
            )
        });
        eprintln!(
            "seed {:2} {bug:?}: {} (trace {} steps)",
            bug.seed(),
            v.message,
            v.trace.len()
        );
    }
}

/// C-G0ssi-r work item 4: honest state-space accounting. The initial `Choose`
/// partitions the space by workload, so the full clean run must equal the sum
/// of the single-workload runs (minus the shared initial state, counted once
/// per run). A workload whose share changes — or a `Choose` list that silently
/// stopped offering one — shows up here instead of only in a reported total.
#[test]
fn g0_ssi_state_accounting_matches_full_run() {
    let full = check(&SsiModel::default(), max_states());
    assert!(!full.truncated, "state space exceeds {}", max_states());
    if let Some(v) = full.violation {
        panic!("{}\ntrace:\n  {}", v.message, v.trace.join("\n  "));
    }
    let works = SsiModel::default().works;
    let mut sum = 0usize;
    for w in &works {
        let r = check(
            &SsiModel {
                bug: None,
                works: vec![*w],
            },
            max_states(),
        );
        assert!(!r.truncated, "workload {w:?} exceeds {}", max_states());
        if let Some(v) = r.violation {
            panic!("{w:?}: {}\ntrace:\n  {}", v.message, v.trace.join("\n  "));
        }
        eprintln!("G0-ssi {w:?}: {} states", r.states);
        sum += r.states;
    }
    // Each single-workload run counts the shared initial state (work not yet
    // chosen) once; the full run counts it once in total.
    assert_eq!(
        full.states,
        sum - (works.len() - 1),
        "full-model states must equal the sum of the per-workload shares"
    );
}

/// C-G0ssi-r work item 4: §8.3's no-writes read-only exception belongs to the
/// txn committing now. An active write-less T0 in a T0 -> T1 -> T2 structure
/// with T2 committing first must abort the pivot T1 (T0 can still write, so
/// the structure through it is dangerous); the permissive form would spare
/// the pivot and let it commit. Mirrors `no_write_non_committer_gets_no_exception`
/// in nucleus-txn (C-T3).
#[test]
fn g0_ssi_write_less_non_committer_gets_no_exception() {
    let m = SsiModel {
        bug: None,
        ..Default::default()
    };
    let mut s = m.init();
    let run = |s: &State, acts: &[Action]| {
        let mut cur = s.clone();
        for a in acts {
            cur = m.next(&cur, a);
        }
        cur
    };
    // T0 reads h1 (active, write-less, undeclared); T1 snapshots and
    // registers its SIREAD on h0 before T2 commits.
    s = run(
        &s,
        &[
            Action::Choose(Work::Late),
            Action::Step(0),
            Action::Step(0),
            Action::Step(0),
            Action::Step(0),
            Action::Step(1),
            Action::Step(1),
        ],
    );
    // T2 writes h0 and commits first.
    s = run(
        &s,
        &[
            Action::Step(2),
            Action::Step(2),
            Action::Step(2),
            Action::Step(2),
            Action::Drain(1),
            Action::InsMap(0),
            Action::SetStatus(0),
            Action::Advance(0),
            Action::Step5(0),
            Action::Resolve(2, Key::Heap(0)),
        ],
    );
    // T1 reads h0 (skipping T2's version: T1 -> T2), writes h1 (T0's SIREAD:
    // T0 -> T1), then pre-commits: T0 is active and write-less, so it gets no
    // §8.3 exception and the pivot aborts itself with 40001.
    s = run(
        &s,
        &[
            Action::Step(1),
            Action::Step(1),
            Action::Step(1),
            Action::Step(1),
        ],
    );
    assert_eq!(raised_structures(&s, 1), vec![(0, 1, Some(2))]);
    assert!(committed_ts(&s, 1).is_none());
    // The schedule then completes: T0 writes h2 late and commits, T2 stays
    // committed, and the fired structure satisfies I-SSI-PRECISION.
    s = run(
        &s,
        &[
            Action::Step(0),
            Action::Step(0),
            Action::Step(0),
            Action::Drain(1),
            Action::InsMap(0),
            Action::SetStatus(0),
            Action::Advance(0),
            Action::Step5(0),
            Action::Resolve(0, Key::Heap(2)),
            Action::Cleanup(1, Key::Heap(1)),
        ],
    );
    assert!(committed_ts(&s, 0).is_some());
    assert!(committed_ts(&s, 2).is_some());
    assert!(m.check(&s).is_ok());
}

/// C-G0ssi-r work item 3: the exception's positive half. LateOpt's T0 never
/// writes, so at its own pre-commit — with T2 committed first and T1 active —
/// it is a no-writes txn that **is committing**: §8.3's second case, the
/// engine's `t1 == committer && !e.wrote` (C-T3
/// `committing_no_write_txn_gets_the_exception`). The exception must spare
/// the pivot T0 and let it commit; only T1, checking later against a
/// committed (no longer committing) write-less T0, aborts. Mirrors the Late
/// test above, which pins the negative half.
#[test]
fn g0_ssi_committing_no_write_txn_gets_the_exception() {
    let m = SsiModel::default();
    let mut s = m.init();
    let run = |s: &State, acts: &[Action]| {
        let mut cur = s.clone();
        for a in acts {
            cur = m.next(&cur, a);
        }
        cur
    };
    // T0 reads h1 (undeclared, and it will never write); T1 snapshots and
    // registers its SIREAD on h0 before T2 commits.
    s = run(
        &s,
        &[
            Action::Choose(Work::LateOpt),
            Action::Step(0),
            Action::Step(0),
            Action::Step(0),
            Action::Step(0),
            Action::Step(1),
            Action::Step(1),
        ],
    );
    // T2 writes h0 and commits first.
    s = run(
        &s,
        &[
            Action::Step(2),
            Action::Step(2),
            Action::Step(2),
            Action::Step(2),
            Action::Drain(1),
            Action::InsMap(0),
            Action::SetStatus(0),
            Action::Advance(0),
            Action::Step5(0),
            Action::Resolve(2, Key::Heap(0)),
        ],
    );
    // T1 reads h0 (skipping T2's version: T1 -> T2), writes h1 (T0's SIREAD:
    // T0 -> T1).
    s = run(&s, &[Action::Step(1), Action::Step(1), Action::Step(1)]);
    // T0 pre-commits on the structure T0 -> T1 -> T2 with T2 committed
    // first: T0 is write-less and committing, so the exception applies
    // (commit_ts(T2)=1 > S(T0)=0 spares it) and T0 must not abort.
    s = run(
        &s,
        &[
            Action::Step(0),
            Action::Step(0),
            Action::Drain(1),
            Action::InsMap(0),
            Action::SetStatus(0),
            Action::Advance(0),
            Action::Step5(0),
        ],
    );
    assert!(raised_structures(&s, 0).is_empty());
    assert_eq!(committed_ts(&s, 0), Some(2));
    // T1 pre-commits last: T0 is now committed — write-less, but no longer
    // the txn committing — so it gets no exception and the pivot aborts
    // itself with 40001 on T0 -> T1 -> T2.
    s = run(&s, &[Action::Step(1), Action::Cleanup(1, Key::Heap(1))]);
    assert_eq!(raised_structures(&s, 1), vec![(0, 1, Some(2))]);
    assert!(committed_ts(&s, 1).is_none());
    assert!(committed_ts(&s, 2).is_some());
    assert!(m.check(&s).is_ok());
}
