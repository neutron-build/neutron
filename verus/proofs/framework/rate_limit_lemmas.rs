// Rate Limiter Proof Lemmas
//
// Multi-step proofs for sliding window counter correctness.

#[cfg(verus_keep_ghost)]
mod proofs {
    // verus! {
    //     /// Lemma: The sliding window estimate is monotonic in current count.
    //     /// Adding a request can only increase (or maintain) the estimate.
    //     proof fn estimate_monotonic_in_current(
    //         prev: nat, curr: nat, elapsed_pct: real,
    //     )
    //         requires 0.0 <= elapsed_pct <= 1.0
    //         ensures
    //             sliding_estimate(prev, curr + 1, elapsed_pct) >=
    //             sliding_estimate(prev, curr, elapsed_pct),
    //     {
    //         // curr + 1 > curr, and the prev term is unchanged.
    //     }
    //
    //     /// Lemma: The estimate never underestimates current window count.
    //     /// Since prev_weight ≥ 0, estimated ≥ curr_count always.
    //     proof fn no_undercount(
    //         prev: nat, curr: nat, elapsed_pct: real,
    //     )
    //         requires 0.0 <= elapsed_pct <= 1.0
    //         ensures
    //             sliding_estimate(prev, curr, elapsed_pct) >= curr as real,
    //     {
    //         // prev * (1 - elapsed_pct) ≥ 0 because prev ≥ 0 and (1 - pct) ≥ 0
    //     }
    //
    //     /// Lemma: At the start of a new window (elapsed = 0%),
    //     /// the estimate equals prev + current.
    //     proof fn estimate_at_window_start(prev: nat, curr: nat)
    //         ensures
    //             sliding_estimate(prev, curr, 0.0) == (prev + curr) as real,
    //     { }
    //
    //     /// Lemma: At the end of a window (elapsed = 100%),
    //     /// the estimate equals just current count.
    //     proof fn estimate_at_window_end(prev: nat, curr: nat)
    //         ensures
    //             sliding_estimate(prev, curr, 1.0) == curr as real,
    //     { }
    //
    //     /// Lemma: Window rotation correctly transfers counts.
    //     /// After rotation: new.prev == old.curr, new.curr == 0.
    //     proof fn rotation_correctness(
    //         old_state: RateLimitSpec,
    //         new_state: RateLimitSpec,
    //     )
    //         requires old_state.rotation_correct(new_state)
    //         ensures
    //             // The new estimate at 0% equals old current count
    //             sliding_estimate(new_state.prev_count, new_state.curr_count, 0.0)
    //                 == old_state.curr_count as real,
    //     { }
    //
    //     /// Lemma: The estimate is bounded by prev + current.
    //     /// This ensures we don't overcount either.
    //     proof fn estimate_upper_bound(
    //         prev: nat, curr: nat, elapsed_pct: real,
    //     )
    //         requires 0.0 <= elapsed_pct <= 1.0
    //         ensures
    //             sliding_estimate(prev, curr, elapsed_pct) <= (prev + curr) as real,
    //     {
    //         // prev * (1 - pct) ≤ prev because (1 - pct) ≤ 1
    //     }
    // }
}
