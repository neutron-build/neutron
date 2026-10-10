//! C-SIM work item 7: reproduction and minimization. `SIM_SEED=<u64>` (and
//! `SIM_CONFIG=<name>`) runs exactly one seed and prints the full trace; the
//! minimizer repeatedly tries removing chunks (halves, then quarters, ...
//! then single choices) from a failing choice list and keeps any shorter
//! list that still fails with the same invariant.

/// Minimizes a failing choice list against `run`: a closure that runs a
/// schedule and returns the invariant name it fails with (`None` = no
/// failure). Returns a list that is strictly shorter and fails with the
/// same invariant. A removed choice whose action is no longer enabled is
/// skipped naturally: the replay picks `choice % enabled.len()`.
pub fn minimize(
    start: &[u64],
    wanted: &str,
    run: impl Fn(&[u64]) -> Option<String>,
) -> Vec<u64> {
    let mut cur: Vec<u64> = start.to_vec();
    let fails = |list: &[u64], cur: &[u64]| {
        run(list).is_some_and(|inv| inv == wanted) && list.len() < cur.len()
    };
    if !run(&cur).is_some_and(|inv| inv == wanted) {
        return cur;
    }
    // Chunk removal: halves, then quarters, ..., down to singles.
    let mut chunk = (cur.len() / 2).max(1);
    loop {
        let mut i = 0;
        while i < cur.len() {
            let end = (i + chunk).min(cur.len());
            let mut cand = cur.clone();
            cand.drain(i..end);
            if !cand.is_empty() && fails(&cand, &cur) {
                cur = cand;
            } else {
                i += chunk;
            }
        }
        if chunk == 1 {
            break;
        }
        chunk = (chunk + 1) / 2;
    }
    cur
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic failing schedule: three doors (A, B, C) must be opened
    /// in order; reaching C releases the invariant "BROKEN". A toy
    /// enabled-set mimics the simulator's positional replay
    /// (`choice % enabled`), so removals shift the meaning of later choices.
    fn toy_run(list: &[u64]) -> Option<String> {
        #[derive(PartialEq, Clone, Copy, Debug)]
        enum St {
            A,
            B,
            C,
        }
        let mut st = St::A;
        for &c in list {
            let n = match st {
                St::A => 2,
                St::B | St::C => 3,
            };
            let pick = (c % n) as u32;
            st = match (st, pick) {
                (St::A, 1) => St::B,
                (St::A, _) => St::A,
                (St::B, 2) => St::C,
                (St::B, _) => St::B,
                (St::C, _) => St::C,
            };
        }
        (st == St::C).then(|| "BROKEN".to_string())
    }

    #[test]
    fn minimizer_returns_a_strictly_shorter_schedule_failing_the_same_way() {
        // A schedule that reaches C and fails.
        let failing = vec![1u64, 5u64, 2u64, 0u64];
        assert_eq!(toy_run(&failing).as_deref(), Some("BROKEN"));
        let min = minimize(&failing, "BROKEN", toy_run);
        assert!(
            min.len() < failing.len(),
            "the minimizer must shorten {failing:?} (got {min:?})"
        );
        assert_eq!(
            toy_run(&min).as_deref(),
            Some("BROKEN"),
            "the minimized schedule must fail with the same invariant"
        );
    }

    #[test]
    fn minimizer_leaves_a_different_invariant_alone() {
        let failing = vec![1u64, 5u64, 2u64];
        let min = minimize(&failing, "OTHER", toy_run);
        assert_eq!(min, failing, "a different invariant is not minimized");
    }
}
