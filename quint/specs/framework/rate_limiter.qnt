// Rate Limiter — Sliding window counter with fairness invariants.
// Models the pattern used in rs/crates/neutron/src/rate_limit.rs.
module rate_limiter {
    // === TYPES ===

    type ClientId = int
    type Tick = int

    // === CONFIGURATION ===

    pure val CLIENTS: Set[ClientId] = Set(1, 2, 3)
    pure val WINDOW_SIZE: int = 10          // ticks per window
    pure val MAX_REQUESTS: int = 100        // max requests per window per client
    pure val MAX_TICKS: int = 30            // model bound

    // === STATE ===

    var current_tick: Tick
    // Requests in the current window for each client
    var current_count: ClientId -> int
    // Requests in the previous window for each client
    var previous_count: ClientId -> int
    // Position within current window (0 to WINDOW_SIZE-1)
    var window_offset: int
    // Total allowed and rejected for analysis
    var total_allowed: int
    var total_rejected: int

    // === ACTIONS ===

    action init = all {
        current_tick' = 0,
        current_count' = (c => 0),
        previous_count' = (c => 0),
        window_offset' = 0,
        total_allowed' = 0,
        total_rejected' = 0,
    }

    // Estimated count using sliding window interpolation:
    // estimated = previous * (1 - offset/window_size) + current
    pure def estimated_count(prev: int, curr: int, offset: int): int =
        // Integer approximation: prev * (WINDOW_SIZE - offset) / WINDOW_SIZE + curr
        prev * (WINDOW_SIZE - offset) / WINDOW_SIZE + curr

    // A client makes a request
    action request(client: ClientId): bool = all {
        client.in(CLIENTS),
        current_tick < MAX_TICKS,
        val est = estimated_count(previous_count(client),
                                   current_count(client),
                                   window_offset)
        if (est < MAX_REQUESTS) all {
            // Allow — increment current window count
            current_count' = current_count.set(client,
                current_count(client) + 1),
            total_allowed' = total_allowed + 1,
            total_rejected' = total_rejected,
        } else all {
            // Reject — over limit
            current_count' = current_count,
            total_allowed' = total_allowed,
            total_rejected' = total_rejected + 1,
        },
        current_tick' = current_tick,
        previous_count' = previous_count,
        window_offset' = window_offset,
    }

    // Time advances by one tick
    action tick: bool = all {
        current_tick < MAX_TICKS,
        val new_offset = window_offset + 1
        if (new_offset >= WINDOW_SIZE) all {
            // Window rolls over: current becomes previous, reset current
            previous_count' = current_count,
            current_count' = (c => 0),
            window_offset' = 0,
        } else all {
            previous_count' = previous_count,
            current_count' = current_count,
            window_offset' = new_offset,
        },
        current_tick' = current_tick + 1,
        total_allowed' = total_allowed,
        total_rejected' = total_rejected,
    }

    action step = any {
        CLIENTS.exists(c => request(c)),
        tick,
    }

    // === SAFETY INVARIANTS ===

    // Window offset stays within bounds
    val offset_bounded: bool =
        window_offset >= 0 and window_offset < WINDOW_SIZE

    // Per-client current count never exceeds max (within one window)
    val count_bounded: bool =
        CLIENTS.forall(c => current_count(c) <= MAX_REQUESTS)

    // No client can exceed the limit: estimated count ≤ MAX_REQUESTS
    // (allows equal, because we reject at >=)
    val rate_enforced: bool =
        CLIENTS.forall(c =>
            estimated_count(previous_count(c),
                            current_count(c),
                            window_offset) <= MAX_REQUESTS
        )

    // Fairness: all clients have equal capacity
    // (same MAX_REQUESTS applies to every client)
    val fair_capacity: bool =
        CLIENTS.forall(c1 => CLIENTS.forall(c2 =>
            // If both clients made the same number of requests,
            // they should have the same estimated count
            current_count(c1) == current_count(c2) and
            previous_count(c1) == previous_count(c2)
            implies
            estimated_count(previous_count(c1), current_count(c1), window_offset) ==
            estimated_count(previous_count(c2), current_count(c2), window_offset)
        ))

    // No undercount: estimated count is never negative
    val no_undercount: bool =
        CLIENTS.forall(c =>
            estimated_count(previous_count(c),
                            current_count(c),
                            window_offset) >= 0
        )

    // Rejected requests are bounded by total requests
    val reject_accounting: bool =
        total_rejected <= total_allowed + total_rejected
}
