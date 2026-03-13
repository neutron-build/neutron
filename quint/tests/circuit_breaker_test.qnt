// Circuit breaker invariant tests.
module circuit_breaker_test {
    import circuit_breaker.*

    // Test: initial state is closed and valid
    run init_valid_test = {
        init.then(all { valid_state, non_negative_counts, closed_under_threshold })
    }

    // Test: success in closed state stays closed
    run closed_success_test = {
        init
            .then(closed_success)
            .then(all { valid_state, closed_under_threshold })
    }

    // Test: failures below threshold stay closed
    run below_threshold_test = {
        init
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(all { valid_state, closed_under_threshold })
    }

    // Test: reaching failure threshold trips to open
    run trip_open_test = {
        init
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(all { valid_state, open_has_bounded_ticks })
    }

    // Test: open rejects requests
    run open_rejects_test = {
        init
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(open_reject)
            .then(all { valid_state, reject_bound })
    }

    // Test: full cycle closed → open → half-open → closed
    run full_cycle_test = {
        init
            // Trip to open
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            // Wait for timeout (10 ticks)
            .then(open_timeout).then(open_timeout).then(open_timeout)
            .then(open_timeout).then(open_timeout).then(open_timeout)
            .then(open_timeout).then(open_timeout).then(open_timeout)
            .then(open_timeout)
            // Now half-open — succeed enough to close
            .then(half_open_success)
            .then(half_open_success)
            .then(half_open_success)
            .then(all { valid_state, closed_under_threshold })
    }

    // Test: half-open failure goes back to open
    run half_open_fallback_test = {
        init
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            .then(closed_failure)
            // Timeout to half-open
            .then(open_timeout).then(open_timeout).then(open_timeout)
            .then(open_timeout).then(open_timeout).then(open_timeout)
            .then(open_timeout).then(open_timeout).then(open_timeout)
            .then(open_timeout)
            // Fail in half-open
            .then(half_open_failure)
            .then(all { valid_state, open_has_bounded_ticks })
    }
}
