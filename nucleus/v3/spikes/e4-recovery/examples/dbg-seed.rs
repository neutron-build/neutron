//! Debug helper (not part of the suite): reproduce one workload seed and
//! print the per-round state.
fn main() {
    let seeds: Vec<u64> = std::env::args()
        .skip(1)
        .filter_map(|s| s.parse().ok())
        .collect();
    let list = if seeds.is_empty() {
        (0..16).collect()
    } else {
        seeds
    };
    for seed in list {
        let rec = e4_recovery::workload::run_seed(seed, None);
        println!(
            "seed {seed}: pass={} rounds={} deadlock={} panic={} baton={} stopped_early={} te={:?}",
            rec.passes(),
            rec.rounds.len(),
            rec.deadlock,
            rec.panic,
            rec.baton,
            rec.stopped_early,
            rec.rounds
                .iter()
                .filter_map(|r| r.task_error.as_ref())
                .next()
        );
    }
}
