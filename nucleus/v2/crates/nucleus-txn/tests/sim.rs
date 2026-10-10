//! C-SIM work item 8: the suite. Configurations `rc_mix`, `rr_mix`,
//! `ser_only`, `locks_heavy`, `crash`, `gc`; 50 seeds per configuration by
//! default (`SIM_SEEDS=<n>` overrides), each bounded to 2000 steps, the
//! whole suite under 60 s in the dev profile. The first 20 seeds run twice
//! in debug-assert mode and the two traces are compared byte for byte (the
//! determinism check). `SIM_SEED=<u64>` (with `SIM_CONFIG=<name>`) runs
//! exactly one seed and prints the full trace; `SIM_MINIMIZE=1` minimizes a
//! failure's choice list and prints the minimized trace.

#[path = "sim/mod.rs"]
mod sim;

use std::time::Instant;

use sim::{min, run, Config, RunResult};

pub fn configs() -> Vec<Config> {
    ["rc_mix", "rr_mix", "ser_only", "locks_heavy", "crash", "gc"]
        .into_iter()
        .map(Config::named)
        .collect()
}

fn rc_mix() -> Config {
    Config::named("rc_mix")
}

fn seed_count() -> u64 {
    match std::env::var("SIM_SEEDS") {
        Ok(v) => v.parse().unwrap_or(50).max(1),
        Err(_) => 50,
    }
}

fn one_config(name: &str) -> Config {
    Config::named(name)
}

/// One (config, seed) run; on a violation, panics with the seed, the
/// invariant and the trace (and, with `SIM_MINIMIZE=1`, the minimized
/// schedule and trace).
fn expect_green(cfg: &Config, seed: u64) {
    if let RunResult::Violation(v) = run(cfg, seed, None) {
        let mut msg = format!(
            "\nC-SIM violation: {} (config {}, seed {}, step {})\n{}\n",
            v.inv, v.config, v.seed, v.step, v.detail
        );
        if std::env::var("SIM_MINIMIZE").as_deref() == Ok("1") {
            let cfg2 = cfg.clone();
            let wanted = v.inv;
            let seed2 = seed;
            let choices = v.choices.clone();
            let m = min::minimize(&choices, wanted, move |list| {
                match run(&cfg2, seed2, Some(list.to_vec())) {
                    RunResult::Violation(x) => Some(x.inv.to_string()),
                    RunResult::Ok(_) => None,
                }
            });
            msg.push_str(&format!(
                "minimized schedule ({} choices, was {}): {:?}\n",
                m.len(),
                choices.len(),
                m
            ));
            if let RunResult::Violation(v2) = run(cfg, seed, Some(m.clone())) {
                msg.push_str(&format!(
                    "minimized trace ({} steps, violation {}):\n{}\n",
                    m.len(),
                    v2.inv,
                    v2.trace
                ));
            }
        }
        msg.push_str(&format!("trace:\n{}\n", v.trace));
        panic!("{msg}");
    }
}

#[test]
fn suite() {
    // A single seeded run: print the full trace.
    if let Ok(seed) = std::env::var("SIM_SEED") {
        if let Ok(s) = seed.parse::<u64>() {
            let name = std::env::var("SIM_CONFIG").unwrap_or_else(|_| "rc_mix".into());
            let cfg = one_config(&name);
            match run(&cfg, s, None) {
                RunResult::Ok(f) => {
                    println!(
                        "seed {s} config {name}: ok, {} steps, {} probe points",
                        f.steps, f.probe_points
                    );
                    println!("{}", f.trace);
                }
                RunResult::Violation(v) => {
                    println!(
                        "seed {s} config {name}: VIOLATION {} at step {}: {}",
                        v.inv, v.step, v.detail
                    );
                    println!("{}", v.trace);
                    panic!("violation {}: {}", v.inv, v.detail);
                }
            }
            return;
        }
    }
    let start = Instant::now();
    let n = seed_count();
    let det_up_to = n.min(20);
    // `SIM_ONLY=<name>`: the one configuration to run (mutation evidence:
    // a mutant is hunted on its named configuration).
    let all = configs();
    let selected: Vec<Config> = match std::env::var("SIM_ONLY") {
        Ok(name) => vec![one_config(&name)],
        Err(_) => all.clone(),
    };
    let n_selected = selected.len();
    for cfg in selected {
        for seed in 1..=n {
            expect_green(&cfg, seed);
            // Determinism: the first seeds run twice, byte for byte.
            if cfg!(debug_assertions) && seed <= det_up_to {
                let a = run(&cfg, seed, None);
                let b = run(&cfg, seed, None);
                let (ta, ca) = match &a {
                    RunResult::Ok(f) => (f.trace.clone(), f.choices.clone()),
                    RunResult::Violation(v) => (v.trace.clone(), v.choices.clone()),
                };
                let (tb, cb) = match &b {
                    RunResult::Ok(f) => (f.trace.clone(), f.choices.clone()),
                    RunResult::Violation(v) => (v.trace.clone(), v.choices.clone()),
                };
                assert_eq!(
                    ta, tb,
                    "config {} seed {seed} is not deterministic",
                    cfg.name
                );
                assert_eq!(
                    ca, cb,
                    "config {} seed {seed} drew different schedules",
                    cfg.name
                );
            }
        }
    }
    let el = start.elapsed();
    println!(
        "suite: {n_selected} of {} configs x {n} seeds in {el:?}",
        all.len()
    );
    assert!(
        el.as_secs() < 60,
        "the default suite must stay under 60 s (took {el:?})"
    );
}

/// The probe actually interleaves: probe points must fire (otherwise the
/// yield points are dead code).
#[test]
fn probe_points_fire() {
    let cfg = rc_mix();
    let mut total = 0u64;
    for seed in 1..=5u64 {
        match run(&cfg, seed, None) {
            RunResult::Ok(f) => total += f.probe_points,
            RunResult::Violation(v) => panic!("violation {}: {}", v.inv, v.detail),
        }
    }
    assert!(total > 0, "no probe point fired across 5 runs");
}
