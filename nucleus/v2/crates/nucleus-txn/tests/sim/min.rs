//! C-SIM work item 7: reproduction and minimization. `SIM_SEED=<u64>` (and
//! `SIM_CONFIG=<name>`) runs exactly one seed and prints the full trace; the
//! minimizer repeatedly tries removing chunks (halves, then quarters, ...
//! then single choices) from a failing choice list and keeps any shorter
//! list that still fails with the same invariant; `shrink` iterates it to a
//! fixpoint on the consumed schedule and `reduce_config` then tries a smaller
//! configuration.

// `sim_regressions.rs` shares this module but replays schedules directly.
#![allow(dead_code)]

use super::Config;

/// Minimizes a failing choice list against `run`: a closure that runs a
/// schedule and returns the invariant name it fails with (`None` = no
/// failure). Returns a list that is strictly shorter and fails with the
/// same invariant. A removed choice whose action is no longer enabled is
/// skipped naturally: the replay picks `choice % enabled.len()`.
pub fn minimize(start: &[u64], wanted: &str, run: impl Fn(&[u64]) -> Option<String>) -> Vec<u64> {
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
        chunk = chunk.div_ceil(2);
    }
    cur
}

/// One replay's answer for the shrinkers: the invariant it failed with and
/// the choices it actually consumed up to the violation (a replay whose
/// list ran out draws the rest from the seed's stream, so a short list is
/// not the whole schedule; the consumed list is).
pub type Replay = (String, Vec<u64>);

/// [`minimize`] to a fixpoint, normalized: each round's result is replaced
/// by the choices the failing replay actually consumed, so the returned
/// list is the complete schedule (its length is the trace length) and it
/// replays exactly with no fallback draws.
pub fn shrink(start: &[u64], wanted: &str, run: impl Fn(&[u64]) -> Option<Replay>) -> Vec<u64> {
    let mut cur = start.to_vec();
    loop {
        let m = minimize(&cur, wanted, |l| run(l).map(|r| r.0));
        let eff = match run(&m) {
            Some((inv, taken)) if inv == wanted => taken,
            _ => m,
        };
        if eff.len() < cur.len() {
            cur = eff;
        } else {
            return cur;
        }
    }
}

/// Config reduction (after schedule minimization): tries fewer sessions,
/// fewer keys, fewer txns per session, fewer statements per txn and fewer
/// crashes, keeping each reduction under which the failure survives with the
/// same invariant (the schedule is re-shrunk under the reduced config).
/// `run(cfg, list)` replays the failing seed on `cfg`.
pub fn reduce_config(
    cfg: &Config,
    choices: &[u64],
    wanted: &str,
    run: impl Fn(&Config, &[u64]) -> Option<Replay>,
) -> (Config, Vec<u64>) {
    let mut cfg = cfg.clone();
    let mut cur = choices.to_vec();
    loop {
        let mut progressed = false;
        for step in 0..5 {
            let mut cand = cfg.clone();
            let ok = match step {
                0 if cand.sessions > 2 => {
                    cand.sessions -= 1;
                    true
                }
                1 if cand.keys > 3 => {
                    cand.keys -= 1;
                    true
                }
                2 if cand.txns_per_session > 1 => {
                    cand.txns_per_session -= 1;
                    true
                }
                3 if cand.max_stmts > 1 => {
                    cand.max_stmts -= 1;
                    true
                }
                4 if cand.crashes > 0 => {
                    cand.crashes -= 1;
                    true
                }
                _ => false,
            };
            if !ok {
                continue;
            }
            let replay = |l: &[u64]| run(&cand, l);
            let Some((inv, taken)) = replay(&cur) else {
                continue;
            };
            if inv != wanted {
                continue;
            }
            let shrunk = shrink(&taken, wanted, replay);
            // A reduced config is progress when the schedule did not grow.
            if shrunk.len() <= cur.len() {
                cfg = cand;
                cur = shrunk;
                progressed = true;
            }
        }
        if !progressed {
            return (cfg, cur);
        }
    }
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

    /// The consumed-schedule normalization: a replay that runs past its list
    /// draws the remainder (here: zeros) and reports what it consumed.
    #[test]
    fn shrink_returns_the_consumed_schedule_and_is_strictly_shorter() {
        let run = |list: &[u64]| {
            // Fails once it has seen a 7 followed by any choice; draws 0s
            // when the list ends before that.
            let mut taken = Vec::new();
            let mut seen7 = false;
            for i in 0.. {
                let c = list.get(i).copied().unwrap_or(0);
                taken.push(c);
                if seen7 {
                    return Some(("BOOM".to_string(), taken));
                }
                seen7 = c == 7;
                if i > list.len() + 4 {
                    return None;
                }
            }
            None
        };
        let start = vec![1u64, 2, 7, 3, 4, 5, 6];
        let m = shrink(&start, "BOOM", run);
        assert!(m.len() < start.len(), "{m:?}");
        let (inv, taken) = run(&m).expect("minimized schedule fails");
        assert_eq!(inv, "BOOM");
        assert_eq!(taken, m, "the shrunk list is the complete schedule");
    }
}
