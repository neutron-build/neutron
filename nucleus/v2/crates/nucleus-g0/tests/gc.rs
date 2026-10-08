use nucleus_g0::check;
use nucleus_g0::gc::{Bug, GcModel};

fn max_states() -> usize {
    std::env::var("G0_MAX_STATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000_000)
}

#[test]
fn g0_gc_clean_model_holds() {
    let r = check(&GcModel { bug: None }, max_states());
    eprintln!("G0-gc: {} states, {} transitions", r.states, r.transitions);
    assert!(!r.truncated, "state space exceeds {}", max_states());
    if let Some(v) = r.violation {
        panic!("{}\ntrace:\n  {}", v.message, v.trace.join("\n  "));
    }
}

#[test]
fn g0_gc_catches_every_seed() {
    assert_eq!(
        Bug::ALL.map(|b| b.seed()),
        [3, 21, 22, 31, 33, 34, 35, 42, 49, 55],
        "the owned seed list changed"
    );
    for bug in Bug::ALL {
        let r = check(&GcModel { bug: Some(bug) }, max_states());
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
