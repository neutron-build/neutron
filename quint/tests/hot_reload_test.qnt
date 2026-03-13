// Hot reload protocol invariant tests.
module hot_reload_test {
    import hot_reload.*

    // Test: initial state is valid
    run init_valid_test = {
        init.then(all {
            version_monotonic, server_monotonic,
            valid_client_states, disconnected_no_pending,
        })
    }

    // Test: connect and receive update
    run basic_update_test = {
        init
            .then(connect(1))
            .then(sync_full(1))
            .then(file_change)
            .then(apply_delta(1))
            .then(all { version_monotonic, delta_ordering })
    }

    // Test: reconnection triggers sync
    run reconnect_sync_test = {
        init
            .then(connect(1))
            .then(sync_full(1))
            .then(file_change)
            .then(file_change)
            .then(disconnect(1))
            .then(connect(1))
            // Client is behind — should be syncing
            .then(sync_full(1))
            .then(all { version_monotonic, valid_client_states })
    }

    // Test: delta ordering is maintained
    run delta_order_test = {
        init
            .then(connect(1))
            .then(sync_full(1))
            .then(file_change)
            .then(file_change)
            .then(file_change)
            .then(apply_delta(1))
            .then(apply_delta(1))
            .then(all { delta_ordering, no_version_gaps })
    }

    // Test: disconnected clients have no pending deltas
    run disconnect_clears_test = {
        init
            .then(connect(1))
            .then(sync_full(1))
            .then(file_change)
            .then(disconnect(1))
            .then(disconnected_no_pending)
    }

    // Test: multiple clients independent
    run multi_client_test = {
        init
            .then(connect(1))
            .then(connect(2))
            .then(sync_full(1))
            .then(sync_full(2))
            .then(file_change)
            .then(apply_delta(1))
            // Client 2 still has pending delta
            .then(all { version_monotonic, delta_ordering })
    }
}
