//! C-SIM work item 8: the suite. Configurations `rc_mix`, `rr_mix`,
//! `ser_only`, `locks_heavy`, `crash`, `gc`; 50 seeds per configuration by
//! default (`SIM_SEEDS=<n>` overrides), each bounded to 2000 steps, the
//! whole suite inside a per-configuration **step budget** (see
//! [`STEP_BUDGETS`]) — a load-independent measure: the scheduler is
//! single-threaded and every clock it reads is the step counter, so the
//! suite's total step count is deterministic, unlike its wall time (the
//! wall clock is still printed, as information only). The first 20 seeds
//! run twice in debug-assert mode and the two traces are compared byte for
//! byte (the determinism check). `SIM_SEED=<u64>` (with `SIM_CONFIG=<name>`)
//! runs exactly one seed and prints the full trace; `SIM_MINIMIZE=1`
//! minimizes a failure's choice list and prints the minimized trace.

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

/// Per-configuration step budgets (C-FIXUPS3): the scheduler steps of one
/// default suite run (50 seeds plus the first 20 seeds' two determinism
/// re-runs each, debug profile) summed per configuration. The count is
/// deterministic — the scheduler is single-threaded and every clock it
/// reads (`/sys/ts_clock`, `deadlock_after`, `crash_at`) is the step
/// counter, never the wall clock — so a budget on it is a real bound on
/// the suite's work, unlike the wall-clock assert it replaces: the suite
/// measured 11-23 s on an idle machine but 68-112 s under parallel load
/// and failed three acceptance runs that way.
///
/// Measured on the base (debug profile, `nucleus/v2`):
///
/// | config        | measured steps | budget (+50%) |
/// |---------------|----------------|---------------|
/// | `rc_mix`      | 15047          | 22571         |
/// | `rr_mix`      | 14368          | 21552         |
/// | `ser_only`    | 30226          | 45339         |
/// | `locks_heavy` | 41160          | 61740         |
/// | `crash`       | 13737          | 20606         |
/// | `gc`          | 42450          | 63675         |
///
/// Each budget is the measured count + 50% headroom, rounded up: room for
/// deliberate workload growth, no room for a silent doubling. A config
/// that grows past its budget should re-measure and update this table
/// together with the workload change.
fn step_budget(name: &str) -> u64 {
    match name {
        "rc_mix" => 22571,
        "rr_mix" => 21552,
        "ser_only" => 45339,
        "locks_heavy" => 61740,
        "crash" => 20606,
        "gc" => 63675,
        // An unknown configuration has no measurement to budget: the
        // named suite configurations are the only ones the budget governs.
        _ => u64::MAX,
    }
}

/// One (config, seed) run; on a violation, panics with the seed, the
/// invariant and the trace (and, with `SIM_MINIMIZE=1`, the minimized
/// schedule and trace). Returns the run's scheduler step count for the
/// suite's step budget.
fn expect_green(cfg: &Config, seed: u64) -> u64 {
    let r = run(cfg, seed, None);
    let steps = match &r {
        RunResult::Ok(f) => f.steps,
        // A violation panics right below; the count is never used.
        RunResult::Violation(_) => 0,
    };
    if let RunResult::Violation(v) = r {
        let mut msg = format!(
            "\nC-SIM violation: {} (config {}, seed {}, step {})\n{}\n",
            v.inv, v.config, v.seed, v.step, v.detail
        );
        if std::env::var("SIM_MINIMIZE").as_deref() == Ok("1") {
            let wanted = v.inv;
            let replay = |c: &Config, list: &[u64]| match run(c, seed, Some(list.to_vec())) {
                RunResult::Violation(x) => Some((x.inv.to_string(), x.choices.clone())),
                RunResult::Ok(_) => None,
            };
            let m = min::shrink(&v.choices, wanted, |l| replay(cfg, l));
            let (cfg2, m2) = min::reduce_config(cfg, &m, wanted, replay);
            msg.push_str(&format!(
                "minimized schedule ({} choices, was {}; config sessions={} keys={} txns={} stmts={} crashes={} max_steps={}): {:?}\n",
                m2.len(),
                v.choices.len(),
                cfg2.sessions,
                cfg2.keys,
                cfg2.txns_per_session,
                cfg2.max_stmts,
                cfg2.crashes,
                cfg2.max_steps,
                m2
            ));
            if let RunResult::Violation(v2) = run(&cfg2, seed, Some(m2.clone())) {
                msg.push_str(&format!(
                    "minimized trace ({} steps, violation {}):\n{}\n",
                    m2.len(),
                    v2.inv,
                    v2.trace
                ));
            }
        }
        msg.push_str(&format!("trace:\n{}\n", v.trace));
        panic!("{msg}");
    }
    steps
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
    // Every run's scheduler steps, per configuration (the load-independent
    // budget): the primary run of every seed plus the determinism check's
    // re-runs, all of them — the budget bounds the work the suite actually
    // executes, so extra runs trip it too.
    let mut cfg_steps: Vec<(&'static str, u64)> = Vec::with_capacity(n_selected);
    for cfg in selected {
        let mut total = 0u64;
        for seed in 1..=n {
            total += expect_green(&cfg, seed);
            // Determinism: the first seeds run twice, byte for byte.
            if cfg!(debug_assertions) && seed <= det_up_to {
                let a = run(&cfg, seed, None);
                let b = run(&cfg, seed, None);
                total += steps_of(&a) + steps_of(&b);
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
        cfg_steps.push((cfg.name, total));
    }
    let el = start.elapsed();
    for (name, steps) in &cfg_steps {
        println!("suite: config {name} took {steps} scheduler steps");
    }
    println!(
        "suite: {n_selected} of {} configs x {n} seeds in {el:?} (wall time, information only)",
        all.len()
    );
    // The budget is for the default seed count (`SIM_SEEDS` runs are long
    // runs by request).
    if std::env::var("SIM_SEEDS").is_err() {
        for (name, steps) in &cfg_steps {
            let budget = step_budget(name);
            assert!(
                *steps <= budget,
                "config {name} ran {steps} scheduler steps, over its \
                 budget of {budget} (the workload got heavier; raise the \
                 budget with the measurements in its doc comment)"
            );
        }
    }
}

/// A run's step count (0 for a violation; the caller panics on those).
fn steps_of(r: &RunResult) -> u64 {
    match r {
        RunResult::Ok(f) => f.steps,
        RunResult::Violation(_) => 0,
    }
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

/// The C-SIMr2 partial-layer savepoint shape is generated in every
/// configuration that weights it, and never in one with weight 0 (the
/// frozen regression configurations rely on that).
#[test]
fn partial_layer_shape_is_generated() {
    // write k, SAVEPOINT, write k again (or delete it), ROLLBACK TO: the
    // pre-savepoint write is what ROLLBACK TO must restore.
    let is_shape = |stmts: &[sim::Stmt]| -> bool {
        let key_of = |s: &sim::Stmt| -> Option<usize> {
            match s {
                sim::Stmt::Update(k) | sim::Stmt::Delete(k) => Some(*k),
                _ => None,
            }
        };
        stmts.len() >= 4
            && key_of(&stmts[0]).is_some()
            && stmts[1] == sim::Stmt::Savepoint
            && key_of(&stmts[2]).is_some()
            && key_of(&stmts[2]) == key_of(&stmts[0])
            && stmts[3] == sim::Stmt::RollbackTo
    };
    for name in ["rc_mix", "rr_mix", "ser_only", "locks_heavy", "crash", "gc"] {
        let cfg = one_config(name);
        let mut shapes = 0usize;
        for nth in 0..2000usize {
            let mut rng = sim::Rng::new(0xABCD ^ (nth as u64).wrapping_mul(7919));
            let p = sim::gen_program(&cfg, nth % cfg.sessions.max(1), &mut rng);
            if is_shape(&p.stmts) {
                shapes += 1;
            }
        }
        assert!(
            shapes > 10,
            "config {name} generated only {shapes} partial-layer programs in 2000 draws"
        );
    }
    let frozen = sim::Config {
        sp_layer_pm: 0,
        ..one_config("rc_mix")
    };
    let mut shapes = 0usize;
    for nth in 0..2000usize {
        let mut rng = sim::Rng::new(0xABCD ^ (nth as u64).wrapping_mul(7919));
        let p = sim::gen_program(&frozen, 0, &mut rng);
        if is_shape(&p.stmts) {
            shapes += 1;
        }
    }
    assert_eq!(
        shapes, 0,
        "weight 0 must generate no partial-layer programs"
    );
}
