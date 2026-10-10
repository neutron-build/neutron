//! C-SIM work item 7: every minimized failing schedule found while
//! building this card, each as `(config, choice list)` with a comment
//! naming the bug, each running as its own `#[test]`.
//!
//! The simulator found no protocol bug in the engine while this card was
//! built (every violation it raised along the way was a simulator or oracle
//! defect, fixed in `tests/sim/`), so there is no `#[ignore]`d engine-bug
//! schedule here. A bug found later is recorded as: replay the minimized
//! choice list on a fixed seed and assert the same invariant fires (marked
//! `#[ignore]` with the bug named until the engine is fixed, then asserting
//! it no longer does — the minimized schedule is the regression).
//!
//! What this file holds today: the replay harness, and the minimized
//! mutation-evidence schedules. Each of those fails when its named mutant is
//! applied to the crate and must stay green on the real code.

#[path = "sim/mod.rs"]
mod sim;

use sim::{run, Config, RunResult};

/// Replays one `(config, seed, choice list)`; returns the violation.
fn replay(name: &str, seed: u64, choices: &[u64]) -> Option<sim::Violation> {
    match run(&Config::named(name), seed, Some(choices.to_vec())) {
        RunResult::Violation(v) => Some(*v),
        RunResult::Ok(_) => None,
    }
}

/// A schedule that does not fail (the harness's smoke test): an empty
/// choice list on a seed whose run the simulator itself replays.
#[test]
fn replay_harness_runs() {
    assert!(replay("rc_mix", 0, &[]).is_none());
}

/// Replays a minimized mutant-kill schedule on the real engine: every entry
/// must name an enabled action (the run consumes the list exactly) and no
/// invariant may fire. The same schedule fails when the named mutant is
/// applied to the crate (see the C-SIM evidence table).
fn assert_exact_and_green(cfg: Config, seed: u64, choices: &[u64]) {
    match run(&cfg, seed, Some(choices.to_vec())) {
        RunResult::Ok(f) => assert_eq!(
            &f.choices[..choices.len().min(f.choices.len())],
            choices,
            "the schedule no longer replays exactly (an entry named a disabled action)"
        ),
        RunResult::Violation(v) => panic!("violation {}: {}\n{}", v.inv, v.detail, v.trace),
    }
}

/// A reduced configuration: the named one with the minimizer's smaller
/// sessions / keys / txns per session / statements per txn / crashes.
fn reduced(name: &str, s: usize, k: usize, t: usize, m: usize, c: usize) -> Config {
    Config {
        sessions: s,
        keys: k,
        txns_per_session: t,
        max_stmts: m,
        crashes: c,
        ..Config::named(name)
    }
}

/// Mutant 7: advance visible_ts before setting the status (I-VIS). Minimized from `crash` seed 1 (11 choices).
#[test]
fn mutant_7_crash_i_vis() {
    let choices: &[u64] = &[
        14096124591738067474,
        11356687587340621409,
        13795216013587330594,
        15525551810481719128,
        14653971037609175131,
        15969007938986996583,
        14292239500611405217,
        17191754709355905095,
        12816920937924164708,
        2114185443546057840,
        3612914470946313744,
    ];
    assert_exact_and_green(reduced("crash", 2, 3, 1, 1, 0), 1, choices);
}

/// Mutant 9: records written out of ts order within a group (I-WAL-ORDER). Minimized from `rc_mix` seed 1 (19 choices).
#[test]
fn mutant_9_rc_mix_i_wal_order() {
    let choices: &[u64] = &[
        14096124591738067474,
        11356687587340621409,
        13795216013587330594,
        15969007938986996583,
        15525551810481719128,
        17191754709355905095,
        14653971037609175131,
        13795216013587330594,
        15969007938986996583,
        10473027141419423012,
        15525551810481719128,
        15969007938986996583,
        14653971037609175131,
        17191754709355905095,
        10473027141419423012,
        11759720759472125314,
        14292239500611405217,
        12816920937924164708,
        3244550020535922704,
    ];
    assert_exact_and_green(reduced("rc_mix", 2, 4, 1, 3, 0), 1, choices);
}

/// Mutant 26: ROLLBACK TO without bump_and_wake (I-LIVE(a)). Minimized from `locks_heavy` seed 1 (23 choices).
#[test]
fn mutant_26_locks_heavy_i_live_a() {
    let choices: &[u64] = &[
        14096124591738067474,
        14652152272998418323,
        11356687587340621409,
        12967361324936232772,
        17641425674783364393,
        15969007938986996583,
        12967361324936232772,
        18173676661964255793,
        17581488960850471074,
        15969007938986996583,
        17191754709355905095,
        13795216013587330594,
        14653971037609175131,
        10473027141419423012,
        15466796705907277188,
        15969007938986996583,
        12261068023409219113,
        12967361324936232772,
        17968049814706755930,
        15294641677182912795,
        12967361324936232772,
        18173676661964255793,
        10658688007863505570,
    ];
    assert_exact_and_green(reduced("locks_heavy", 4, 3, 1, 1, 0), 1, choices);
}

/// Mutant 8: ack of an On commit before its sync_wal (I-ACK). Minimized from `crash` seed 1 (25 choices).
#[test]
fn mutant_8_crash_i_ack() {
    let choices: &[u64] = &[
        14096124591738067474,
        11356687587340621409,
        13795216013587330594,
        15525551810481719128,
        14653971037609175131,
        15969007938986996583,
        13795216013587330594,
        15525551810481719128,
        17191754709355905095,
        15969007938986996583,
        10473027141419423012,
        14292239500611405217,
        16540696377105554909,
        15969007938986996583,
        16540696377105554909,
        16540696377105554909,
        16540696377105554909,
        17191754709355905095,
        12816920937924164708,
        1769661039505662495,
        940002233291805047,
        16540696377105554909,
        3650901824743153221,
        6118650617640139407,
        3950671758201046922,
    ];
    assert_exact_and_green(reduced("crash", 2, 3, 1, 3, 0), 1, choices);
}

/// Mutant 12: placing over a visible-committed foreign intent without removing it (I-COUNT). Minimized from `rc_mix` seed 2 (47 choices).
#[test]
fn mutant_12_rc_mix_i_count() {
    let choices: &[u64] = &[
        11356687587340621409,
        13795216013587330594,
        15525551810481719128,
        13795216013587330594,
        15525551810481719128,
        14096124591738067474,
        14292239500611405217,
        11356687587340621409,
        17269108783448632067,
        15969007938986996583,
        13795216013587330594,
        17191754709355905095,
        18383922002018568811,
        13795216013587330594,
        11759720759472125314,
        17269108783448632067,
        15525551810481719128,
        14096124591738067474,
        15969007938986996583,
        17581488960850471074,
        13795216013587330594,
        13875289228893918177,
        14292239500611405217,
        11356687587340621409,
        17269108783448632067,
        15969007938986996583,
        13795216013587330594,
        15525551810481719128,
        10473027141419423012,
        14653971037609175131,
        13795216013587330594,
        15969007938986996583,
        17191754709355905095,
        15525551810481719128,
        14653971037609175131,
        14292239500611405217,
        12816920937924164708,
        4259698845751218068,
        16540696377105554909,
        2461928127193980629,
        2806162187097067074,
        16540696377105554909,
        203336136484735540,
        5510847151972252991,
        6068167238384133651,
        8435482128952402059,
        10473027141419423012,
    ];
    assert_exact_and_green(reduced("rc_mix", 2, 4, 3, 2, 0), 2, choices);
}

/// Mutant 59: a waker that leaves wait edges (I-LIVE(c)). Minimized from `locks_heavy` seed 23 (58 choices).
#[test]
fn mutant_59_locks_heavy_i_live_c() {
    let choices: &[u64] = &[
        14096124591738067474,
        15969007938986996583,
        11356687587340621409,
        13795216013587330594,
        10473027141419423012,
        15969007938986996583,
        10473027141419423012,
        11759720759472125314,
        12816920937924164708,
        5638171408337722514,
        732647922666779517,
        4116454901569047845,
        1888664525960915954,
        4951068642753299942,
        8638359939281767007,
        17269108783448632067,
        1943666003567593788,
        14653971037609175131,
        12315296326122086742,
        14096124591738067474,
        13795216013587330594,
        15969007938986996583,
        17581488960850471074,
        15969007938986996583,
        14653971037609175131,
        17191754709355905095,
        14292239500611405217,
        12816920937924164708,
        3798713039437957508,
        1361550199656048655,
        889244132419851315,
        2636972639518802265,
        8550652312378409246,
        460310293853870759,
        8563609884413145644,
        17269108783448632067,
        16540696377105554909,
        11356687587340621409,
        13795216013587330594,
        15466796705907277188,
        12261068023409219113,
        17968049814706755930,
        14653971037609175131,
        13795216013587330594,
        14653971037609175131,
        10658688007863505570,
        10473027141419423012,
        12261068023409219113,
        17968049814706755930,
        9385075530570966465,
        15969007938986996583,
        10473027141419423012,
        15969007938986996583,
        10291396803082557962,
        15969007938986996583,
        10473027141419423012,
        10398018699998975627,
        16015394428703951663,
    ];
    assert_exact_and_green(reduced("locks_heavy", 3, 3, 2, 1, 0), 23, choices);
}
