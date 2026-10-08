// Multi-Raft invariant tests.
module multi_raft_test {
    import multi_raft.* from "../specs/nucleus/multi_raft"

    // Test: initial state satisfies both safety invariants
    run init_safe_test = {
        init.expect(all { election_safety, log_matching })
    }

    // Test: single election maintains safety
    run single_election_test = {
        init
            .then(request_vote(1, 1))
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .expect(all { election_safety, log_matching })
    }

    // Test: append entry after election
    run append_after_election_test = {
        init
            .then(request_vote(1, 1))
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1, "insert user"))
            .expect(all { election_safety, log_matching })
    }

    // Test: step down on higher term
    run step_down_test = {
        init
            .then(request_vote(1, 1))
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .then(step_down(1, 1, 5))
            .expect(all { election_safety, log_matching })
    }

    // Test: elections in different groups are independent
    run independent_groups_test = {
        init
            .then(request_vote(1, 1))   // node 1 runs for group 1
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .then(request_vote(2, 2))   // node 2 runs for group 2
            .then(deliver_vote(1, 2, 2)).then(receive_vote(1, 2, 2))
            .then(win_election(2, 2))
            .expect(all { election_safety, log_matching })
    }

    // Test: leader appends multiple entries then steps down
    run append_then_step_down_test = {
        init
            .then(request_vote(1, 1))
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1, "create table"))
            .then(append_entry(1, 1, "insert row"))
            .then(append_entry(1, 1, "commit"))
            .then(step_down(1, 1, 5))
            .expect(all { election_safety, log_matching })
    }

    // Test: new leader elected after step down
    run re_election_test = {
        init
            .then(request_vote(1, 1))
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1, "write"))
            .then(replicate(1, 2, 1))
            .then(step_down(1, 1, 3))
            .then(step_down(2, 1, 3))
            .then(request_vote(2, 1))
            .then(deliver_vote(1, 2, 1)).then(receive_vote(1, 2, 1))
            .then(win_election(2, 1))
            .expect(all { election_safety, log_matching })
    }

    // Test: concurrent leadership across groups with log activity
    run multi_group_log_test = {
        init
            .then(request_vote(1, 1))
            .then(deliver_vote(2, 1, 1)).then(receive_vote(2, 1, 1))
            .then(win_election(1, 1))
            .then(request_vote(2, 2))
            .then(deliver_vote(1, 2, 2)).then(receive_vote(1, 2, 2))
            .then(win_election(2, 2))
            .then(append_entry(1, 1, "shard1 write"))
            .then(append_entry(2, 2, "shard2 write"))
            .expect(all { election_safety, log_matching })
    }
    run durable_vote_survives_crash_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1))
        .then(crash(2)).then(recover(2)).then(request_vote(3,1))
        .expect(group_state.get((2,1)).voted_for == 1 and group_state.get((2,1)).current_term == group_state.get((3,1)).current_term and votes.get((3,1)).size() == 1 and election_safety)
    run leaders_above_twenty_witness_test = init.then(all { group_state' = group_state.set((1,1),{...group_state.get((1,1)),current_term:21,role:"leader"}).set((2,1),{...group_state.get((2,1)),current_term:21,role:"leader"}), snapshots' = snapshots, responses' = responses, requests' = requests, votes' = votes, elected' = elected, alive' = alive, partitioned' = partitioned }).expect(not(election_safety))
    run differing_prefix_witness_test = init.then(all { group_state' = group_state.set((1,1),{...group_state.get((1,1)),log:List({term:1,index:1,command:"left"},{term:3,index:2,command:"same"})}).set((2,1),{...group_state.get((2,1)),log:List({term:2,index:1,command:"right"},{term:3,index:2,command:"same"})}), snapshots' = snapshots, responses' = responses, requests' = requests, votes' = votes, elected' = elected, alive' = alive, partitioned' = partitioned }).expect(not(log_matching))
    // Persisted vote must reject a real second request after restart.
    run durable_vote_rejects_second_delivery_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1))
        .then(crash(2)).then(recover(2)).then(request_vote(3,1)).then(deliver_vote(2,3,1)).then(receive_vote(2,3,1))
        .expect(votes.get((3,1)) == Set(3) and group_state.get((2,1)).voted_for == 1 and election_safety)
    run delayed_response_old_term_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1))
        .then(request_vote(1,1)).then(receive_vote(2,1,1))
        .expect(votes.get((1,1)) == Set(1) and group_state.get((1,1)).current_term == 2)
    run immutable_request_payload_test = init.then(request_vote(1,1)).then(request_vote(1,1))
        .expect(requests.exists(r => r.candidate == 1 and r.term == 1 and r.length == 0) and
            requests.exists(r => r.candidate == 1 and r.term == 2))
    run crash_clears_candidate_votes_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1))
        .then(crash(1)).then(recover(1)).expect(votes.get((1,1)) == Set() and group_state.get((1,1)).voted_for == 1)
    run concrete_abstract_election_relation_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1))
        .then(win_election(1,1)).expect(concrete_abstract_safety and abstract_leaders(elected).size() == 1)
    run stale_snapshot_rejected_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .then(append_entry(1,1,"first")).then(queue_snapshot(1,2,1)).then(step_down(2,1,5))
        .then({ nondet message=snapshots.oneOf() receive_snapshot(message) })
        .expect(group_state.get((2,1)).current_term == 5 and group_state.get((2,1)).log == List())
    run immutable_snapshot_after_append_test = init.then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .then(append_entry(1,1,"first")).then(queue_snapshot(1,2,1)).then(append_entry(1,1,"second"))
        .then({ nondet message=snapshots.oneOf() receive_snapshot(message) })
        .expect(group_state.get((2,1)).log.length() == 1 and group_state.get((1,1)).log.length() == 2)
    run partitioned_candidate_no_quorum_test = init.then(partition(1,2)).then(partition(1,3)).then(request_vote(1,1))
        .expect(votes.get((1,1)) == Set(1) and election_safety)
}
