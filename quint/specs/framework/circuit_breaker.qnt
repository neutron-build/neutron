// Circuit Breaker — 3-state FSM (Closed → Open → Half-Open) with
// atomic transitions. Models the pattern used in rs/crates/neutron/.
module circuit_breaker {
    // === TYPES ===

    type State = str  // "closed" | "open" | "half_open"
    type RequestId = int

    // === CONFIGURATION ===

    pure val FAILURE_THRESHOLD: int = 5     // failures before opening
    pure val SUCCESS_THRESHOLD: int = 3     // successes in half-open to close
    pure val TIMEOUT_TICKS: int = 10        // ticks before open → half-open

    // === STATE ===

    var state: State
    var failure_count: int
    var success_count: int
    var ticks_in_open: int
    var total_requests: int
    var total_rejected: int

    // === ACTIONS ===

    action init = all {
        state' = "closed",
        failure_count' = 0,
        success_count' = 0,
        ticks_in_open' = 0,
        total_requests' = 0,
        total_rejected' = 0,
    }

    // A request succeeds while the breaker is closed
    action closed_success: bool = all {
        state == "closed",
        // Success resets the failure count
        failure_count' = 0,
        success_count' = success_count,
        state' = "closed",
        ticks_in_open' = 0,
        total_requests' = total_requests + 1,
        total_rejected' = total_rejected,
    }

    // A request fails while the breaker is closed
    action closed_failure: bool = all {
        state == "closed",
        val new_failures = failure_count + 1
        failure_count' = new_failures,
        success_count' = 0,
        // Trip open if threshold reached
        if (new_failures >= FAILURE_THRESHOLD)
            all { state' = "open", ticks_in_open' = 0 }
        else
            all { state' = "closed", ticks_in_open' = 0 },
        total_requests' = total_requests + 1,
        total_rejected' = total_rejected,
    }

    // Breaker is open — reject request immediately (fail-fast)
    action open_reject: bool = all {
        state == "open",
        // No state change — just reject
        state' = "open",
        failure_count' = failure_count,
        success_count' = success_count,
        ticks_in_open' = ticks_in_open,
        total_requests' = total_requests + 1,
        total_rejected' = total_rejected + 1,
    }

    // Time passes while open — after timeout, transition to half-open
    action open_timeout: bool = all {
        state == "open",
        val new_ticks = ticks_in_open + 1
        ticks_in_open' = new_ticks,
        if (new_ticks >= TIMEOUT_TICKS)
            all { state' = "half_open", success_count' = 0 }
        else
            all { state' = "open", success_count' = success_count },
        failure_count' = failure_count,
        total_requests' = total_requests,
        total_rejected' = total_rejected,
    }

    // A probe request succeeds in half-open
    action half_open_success: bool = all {
        state == "half_open",
        val new_successes = success_count + 1
        success_count' = new_successes,
        // Close the breaker if success threshold met
        if (new_successes >= SUCCESS_THRESHOLD)
            all { state' = "closed", failure_count' = 0 }
        else
            all { state' = "half_open", failure_count' = failure_count },
        ticks_in_open' = 0,
        total_requests' = total_requests + 1,
        total_rejected' = total_rejected,
    }

    // A probe request fails in half-open — back to open
    action half_open_failure: bool = all {
        state == "half_open",
        state' = "open",
        ticks_in_open' = 0,
        failure_count' = failure_count + 1,
        success_count' = 0,
        total_requests' = total_requests + 1,
        total_rejected' = total_rejected,
    }

    // Take any enabled action
    action step = any {
        closed_success,
        closed_failure,
        open_reject,
        open_timeout,
        half_open_success,
        half_open_failure,
    }

    // === SAFETY INVARIANTS ===

    // Valid state: always one of the three states
    val valid_state: bool =
        state == "closed" or state == "open" or state == "half_open"

    // Counts are non-negative
    val non_negative_counts: bool =
        failure_count >= 0 and success_count >= 0 and
        ticks_in_open >= 0 and total_rejected >= 0

    // Rejected only when open: total_rejected <= total_requests
    val reject_bound: bool =
        total_rejected <= total_requests

    // No stuck state: from open, timeout will always eventually reach half-open
    // (checked by model checker via liveness, stated here as safety bound)
    val open_has_bounded_ticks: bool =
        state == "open" implies ticks_in_open <= TIMEOUT_TICKS

    // Half-open never exceeds success threshold (it closes first)
    val half_open_bounded: bool =
        state == "half_open" implies success_count < SUCCESS_THRESHOLD

    // Closed means either we haven't failed enough or we recovered
    val closed_under_threshold: bool =
        state == "closed" implies failure_count < FAILURE_THRESHOLD
}
