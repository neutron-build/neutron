//! Debug helper (not part of the suite): reproduce one workload seed and
//! print the parked state when the round deadlocks.
fn main() {
    let seed: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let rec = e4_recovery::workload::run_seed(seed, None);
    println!(
        "seed {seed}: rounds {} deadlock {} panic {} baton {}",
        rec.rounds.len(),
        rec.deadlock,
        rec.panic,
        rec.baton
    );
    for (i, r) in rec.rounds.iter().enumerate() {
        println!(
            "round {i}: crash {} fired {} pass {} dl {} te {:?} o1 {:?} o2 {:?} o3 {:?} monitors {:?}",
            r.crash, r.fired, r.pass, r.deadlock, r.task_error, r.o1_fail, r.o2_fail, r.o3_fail, r.monitors
        );
    }
    println!("twin: {:?}", rec.twin);
}
