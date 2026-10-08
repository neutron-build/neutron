use nucleus_g0::check;
use nucleus_g0::write::{Bug, WriteModel};

fn max_states() -> usize {
    std::env::var("G0_MAX_STATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000_000)
}

#[test]
fn g0_write_clean_model_holds() {
    let r = check(&WriteModel { bug: None }, max_states());
    eprintln!(
        "G0-write: {} states, {} transitions",
        r.states, r.transitions
    );
    assert!(!r.truncated, "state space exceeds {}", max_states());
    if let Some(v) = r.violation {
        panic!("{}\ntrace:\n  {}", v.message, v.trace.join("\n  "));
    }
}

#[test]
fn g0_write_catches_every_seed() {
    assert_eq!(
        Bug::ALL.map(|b| b.seed()),
        [4, 11, 12, 15, 16, 17, 18, 19, 24, 25, 26, 27, 37, 38, 45, 46, 47, 48, 50, 52, 59]
    );
    for bug in Bug::ALL {
        let r = check(&WriteModel { bug: Some(bug) }, max_states());
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
