// Rate limiter invariant tests.
module rate_limiter_test {
    import rate_limiter.*

    // Test: initial state satisfies all invariants
    run init_valid_test = {
        init.then(all {
            offset_bounded, count_bounded, rate_enforced,
            fair_capacity, no_undercount
        })
    }

    // Test: single request is allowed
    run single_request_test = {
        init
            .then(request(1))
            .then(all { rate_enforced, count_bounded })
    }

    // Test: time advances correctly
    run tick_test = {
        init
            .then(tick)
            .then(tick)
            .then(all { offset_bounded, no_undercount })
    }

    // Test: window rollover resets counts
    run window_rollover_test = {
        init
            .then(request(1)).then(request(1))
            // Advance through full window
            .then(tick).then(tick).then(tick).then(tick).then(tick)
            .then(tick).then(tick).then(tick).then(tick).then(tick)
            .then(all { offset_bounded, count_bounded })
    }

    // Test: fairness — equal requests get equal treatment
    run fairness_test = {
        init
            .then(request(1))
            .then(request(2))
            .then(request(3))
            .then(fair_capacity)
    }

    // Test: requests and ticks interleaved
    run interleaved_test = {
        init
            .then(request(1))
            .then(tick)
            .then(request(2))
            .then(tick)
            .then(request(1))
            .then(all { rate_enforced, no_undercount, reject_accounting })
    }
}
