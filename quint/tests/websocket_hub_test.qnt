// WebSocket hub invariant tests.
module websocket_hub_test {
    import websocket_hub.*

    // Test: initial state is valid
    run init_valid_test = {
        init.then(all { members_connected, empty_rooms_clean })
    }

    // Test: connect, join, broadcast, deliver
    run basic_flow_test = {
        init
            .then(connect(1))
            .then(connect(2))
            .then(join_room(1, 1))
            .then(join_room(2, 1))
            .then(broadcast(1, 1))
            .then(deliver(2, 1))
            .then(all {
                members_connected,
                no_self_delivery,
                no_duplicate_delivery,
            })
    }

    // Test: sender doesn't receive their own message
    run no_self_delivery_test = {
        init
            .then(connect(1))
            .then(connect(2))
            .then(join_room(1, 1))
            .then(join_room(2, 1))
            .then(broadcast(1, 1))
            .then(no_self_delivery)
    }

    // Test: disconnect removes from rooms
    run disconnect_cleanup_test = {
        init
            .then(connect(1))
            .then(join_room(1, 1))
            .then(disconnect(1))
            .then(all { members_connected, empty_rooms_clean })
    }

    // Test: leave room mid-broadcast
    run leave_during_test = {
        init
            .then(connect(1))
            .then(connect(2))
            .then(connect(3))
            .then(join_room(1, 1))
            .then(join_room(2, 1))
            .then(join_room(3, 1))
            .then(broadcast(1, 1))
            .then(leave_room(2, 1))
            // Message still pending for conn 2 even after leaving
            .then(all { members_connected, broadcast_scoped })
    }

    // Test: multiple rooms are independent
    run multi_room_test = {
        init
            .then(connect(1))
            .then(connect(2))
            .then(connect(3))
            .then(join_room(1, 1))
            .then(join_room(2, 1))
            .then(join_room(3, 2))
            // Broadcast to room 1 — conn 3 should not receive
            .then(broadcast(1, 1))
            .then(all { no_self_delivery, broadcast_scoped })
    }
}
