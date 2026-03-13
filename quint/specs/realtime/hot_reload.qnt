// Hot Reload Protocol — Mobile preview hot reload with reconnection
// and delta bundle ordering. Models the pattern used in mobile-preview/.
module hot_reload {
    // === TYPES ===

    type ClientId = int
    type BundleVersion = int

    // === CONFIGURATION ===

    pure val CLIENTS: Set[ClientId] = Set(1, 2)
    pure val MAX_VERSION: int = 10

    // === STATE ===

    // Server-side: current bundle version
    var server_version: BundleVersion
    // Per-client: last acknowledged version
    var client_version: ClientId -> BundleVersion
    // Per-client: connection state ("connected" | "disconnected" | "syncing")
    var client_state: ClientId -> str
    // Per-client: pending delta versions to apply (ordered queue)
    var pending_deltas: ClientId -> List[BundleVersion]
    // Total updates sent
    var total_updates: int

    // === ACTIONS ===

    action init = all {
        server_version' = 1,
        client_version' = (c => 0),
        client_state' = (c => "disconnected"),
        pending_deltas' = (c => List()),
        total_updates' = 0,
    }

    // Client connects (or reconnects)
    action connect(client: ClientId): bool = all {
        client.in(CLIENTS),
        client_state(client) == "disconnected",
        // If behind server, enter syncing state
        if (client_version(client) < server_version)
            all {
                client_state' = client_state.set(client, "syncing"),
                // Queue all missed versions
                pending_deltas' = pending_deltas.set(client,
                    List()),  // will be populated by sync action
            }
        else
            all {
                client_state' = client_state.set(client, "connected"),
                pending_deltas' = pending_deltas,
            },
        server_version' = server_version,
        client_version' = client_version,
        total_updates' = total_updates,
    }

    // Server sends sync bundle to a syncing client (full state catch-up)
    action sync_full(client: ClientId): bool = all {
        client.in(CLIENTS),
        client_state(client) == "syncing",
        // Full sync: jump to server version
        client_version' = client_version.set(client, server_version),
        client_state' = client_state.set(client, "connected"),
        pending_deltas' = pending_deltas.set(client, List()),
        server_version' = server_version,
        total_updates' = total_updates + 1,
    }

    // Client disconnects
    action disconnect(client: ClientId): bool = all {
        client.in(CLIENTS),
        client_state(client) != "disconnected",
        client_state' = client_state.set(client, "disconnected"),
        // Clear pending deltas
        pending_deltas' = pending_deltas.set(client, List()),
        server_version' = server_version,
        client_version' = client_version,
        total_updates' = total_updates,
    }

    // Developer saves a file — server bumps version
    action file_change: bool = all {
        server_version < MAX_VERSION,
        val new_ver = server_version + 1
        server_version' = new_ver,
        // Push delta to all connected clients
        pending_deltas' = (c =>
            if (client_state(c) == "connected")
                pending_deltas(c).append(new_ver)
            else pending_deltas(c)),
        client_state' = client_state,
        client_version' = client_version,
        total_updates' = total_updates,
    }

    // Client applies the next pending delta (must be in order)
    action apply_delta(client: ClientId): bool = all {
        client.in(CLIENTS),
        client_state(client) == "connected",
        pending_deltas(client).length() > 0,
        val delta_ver = pending_deltas(client)[0]
        // Delta must be exactly client_version + 1 (ordered application)
        delta_ver == client_version(client) + 1,
        client_version' = client_version.set(client, delta_ver),
        pending_deltas' = pending_deltas.set(client,
            pending_deltas(client).slice(1, pending_deltas(client).length())),
        client_state' = client_state,
        server_version' = server_version,
        total_updates' = total_updates + 1,
    }

    action step = any {
        CLIENTS.exists(c => connect(c)),
        CLIENTS.exists(c => disconnect(c)),
        CLIENTS.exists(c => sync_full(c)),
        CLIENTS.exists(c => apply_delta(c)),
        file_change,
    }

    // === SAFETY INVARIANTS ===

    // Client version never exceeds server version
    val version_monotonic: bool =
        CLIENTS.forall(c =>
            client_version(c) <= server_version
        )

    // Connected clients are at most 1 version behind per pending delta
    val connected_bounded_lag: bool =
        CLIENTS.forall(c =>
            client_state(c) == "connected" implies
            client_version(c) + pending_deltas(c).length() <= server_version
        )

    // Delta ordering: pending deltas are monotonically increasing
    val delta_ordering: bool =
        CLIENTS.forall(c =>
            val deltas = pending_deltas(c)
            0.to(deltas.length() - 2).forall(i =>
                deltas[i] < deltas[i + 1]
            )
        )

    // No gaps: applied deltas are contiguous (version always increments by 1)
    val no_version_gaps: bool =
        CLIENTS.forall(c =>
            val deltas = pending_deltas(c)
            deltas.length() > 0 implies
            deltas[0] == client_version(c) + 1
        )

    // Syncing clients eventually reach server version (via sync_full)
    val sync_reaches_server: bool =
        CLIENTS.forall(c =>
            client_state(c) == "syncing" implies
            client_version(c) <= server_version
        )

    // Server version only increases
    val server_monotonic: bool =
        server_version >= 1

    // Disconnected clients have no pending deltas
    val disconnected_no_pending: bool =
        CLIENTS.forall(c =>
            client_state(c) == "disconnected" implies
            pending_deltas(c).length() == 0
        )

    // Valid client states
    val valid_client_states: bool =
        CLIENTS.forall(c =>
            client_state(c) == "connected" or
            client_state(c) == "disconnected" or
            client_state(c) == "syncing"
        )
}
