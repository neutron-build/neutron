//! C-SIM work item 7: every minimized failing schedule found while
//! building this card, each as `(config, choice list)` with a comment
//! naming the bug, each running as its own `#[test]`.
//!
//! The suite is currently green: no protocol bug was found by the
//! simulator, so this file holds only the replay harness. A bug found
//! later is recorded as: replay the minimized choice list on a fixed seed
//! and assert the same invariant fires (or, for a fixed bug, that it no
//! longer does — the minimized schedule is the regression).

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
