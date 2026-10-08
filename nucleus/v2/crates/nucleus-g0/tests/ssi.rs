use nucleus_g0::check;
use nucleus_g0::ssi::{Bug, SsiModel};

fn max_states() -> usize {
    std::env::var("G0_MAX_STATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000_000)
}

#[test]
fn g0_ssi_clean_model_holds() {
    let r = check(&SsiModel { bug: None }, max_states());
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
        let r = check(&SsiModel { bug: Some(bug) }, max_states());
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
