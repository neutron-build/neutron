// Negative witnesses inject forbidden mutations; each invariant must reject its independent history/scope violation.
module csrf_lifecycle_witness_test {
    import csrf_lifecycle.* from "../specs/framework/csrf_lifecycle"
    // Mutant acceptance repeats the actual success effect without the used guard.
    action broken_accept_again = all { token_state' = token_state.set(1,"used"), token_session' = token_session,
        token_ttl' = token_ttl, cookie_token' = cookie_token, next_token' = next_token,
        use_count' = use_count.set(1,use_count.get(1)+1), original_session' = original_session }
    run replay_history_witness_test = init.then(generate_token(1)).then(validate_success(1,1)).then(broken_accept_again).expect(not(no_replay))

    run session_rebinding_witness_test = init.then(generate_token(1)).then(all { token_state' = token_state, token_session' = token_session.set(1,2), token_ttl' = token_ttl, cookie_token' = cookie_token, next_token' = next_token, use_count' = use_count, original_session' = original_session }).expect(not(session_isolation))
}
module session_lifecycle_witness_test {
    import session_lifecycle.* from "../specs/framework/session_lifecycle"
    run owner_rebinding_witness_test = init.then(create_session(1)).then(all { phase' = phase, owner' = owner.set(1,2), abs_ttl' = abs_ttl, idle_ticks' = idle_ticks, renewals' = renewals, next_sid' = next_sid, terminal_sessions' = terminal_sessions, revoked_sessions' = revoked_sessions, original_owner' = original_owner }).expect(not(owner_immutable))
    action broken_resurrect = all { phase' = phase.set(1,"active"), owner' = owner, abs_ttl' = abs_ttl, idle_ticks' = idle_ticks, renewals' = renewals, next_sid' = next_sid, terminal_sessions' = terminal_sessions, revoked_sessions' = revoked_sessions, original_owner' = original_owner }
    run resurrection_witness_test = init.then(create_session(1)).then(revoke(1)).then(broken_resurrect).expect(not(terminal_permanent) and not(revoked_is_final))
    run expiry_resurrection_witness_test = init.then(create_session(1)).then(time_tick).then(time_tick).then(time_tick).then(time_tick).then(time_tick)
        .expect(phase.get(1) == "expired").then(broken_resurrect).expect(not(terminal_permanent))

}
module distributed_tx_witness_test {
    import distributed_tx.* from "../specs/nucleus/distributed_tx"
    run committed_abort_history_witness_test = init.then(begin_tx).then(all { tx_state' = tx_state.set(1,"aborted"), participant_votes' = participant_votes, next_tx_id' = next_tx_id, ever_committed' = Set(1), decisions' = decisions }).expect(not(no_committed_abort))
    run mixed_participant_outcomes_witness_test = init.then(begin_tx)
        .then(vote_prepare(1,1)).then(vote_prepare(1,2)).then(vote_prepare(1,3)).then(commit_tx(1)).then(deliver_decision(1,1))
        .then(all { tx_state' = tx_state, participant_votes' = participant_votes, next_tx_id' = next_tx_id,
            ever_committed' = ever_committed, decisions' = decisions.set(1,decisions.get(1).set(2,"aborted")) })
        .expect(not(atomicity))

}
module membership_witness_test {
    import membership.* from "../specs/nucleus/membership"
    run config_regression_witness_test = init.then(all { members' = members, pending_add' = pending_add, pending_remove' = pending_remove, config_version' = 1, previous_config_version' = 2 }).expect(not(config_monotonic))
}
module hot_reload_witness_test {
    import hot_reload.* from "../specs/realtime/hot_reload"
    run server_regression_witness_test = init.then(all { server_version' = 1, client_version' = client_version, client_state' = client_state, pending_deltas' = pending_deltas, total_updates' = total_updates, previous_server_version' = 2 }).expect(not(server_monotonic))
}
module snapshot_transfer_witness_test {
    import snapshot_transfer.* from "../specs/nucleus/snapshot_transfer"
    run source_mutation_witness_test = init.then(begin_transfer(1,2)).then(all { node_data' = node_data.set(1,node_data.get(1).exclude(Set(1)).union(Set(101))), snapshot_in_progress' = snapshot_in_progress, snapshot_source' = snapshot_source, snapshot_target' = snapshot_target, transferred_keys' = transferred_keys, original_source_data' = original_source_data }).expect(not(source_unchanged))
}
module websocket_hub_witness_test {
    import websocket_hub.* from "../specs/realtime/websocket_hub"
    run duplicate_delivery_witness_test = init.then(all { room_members' = room_members, connected' = connected, pending' = pending, delivered' = delivered, msg_sender' = msg_sender, msg_room' = msg_room, next_msg' = next_msg, broadcast_recipients' = broadcast_recipients, was_pending' = was_pending, delivery_count' = delivery_count.set(1,delivery_count.get(1).set(1,2)) }).expect(not(no_duplicate_delivery))
    run out_of_scope_delivery_witness_test = init.then(all { room_members' = room_members, connected' = connected, pending' = pending.set(1,Set(1)), delivered' = delivered, msg_sender' = msg_sender, msg_room' = msg_room, next_msg' = 2, broadcast_recipients' = broadcast_recipients, was_pending' = was_pending, delivery_count' = delivery_count }).expect(not(broadcast_scoped))
    run fabricated_delivery_witness_test = init.then(connect(1)).then(all { room_members' = room_members, connected' = connected,
        pending' = pending, delivered' = delivered.set(1,Set(1)), msg_sender' = msg_sender, msg_room' = msg_room,
        next_msg' = next_msg, broadcast_recipients' = broadcast_recipients, was_pending' = was_pending,
        delivery_count' = delivery_count.set(1,delivery_count.get(1).set(1,1)) }).expect(not(delivered_was_pending))

}
