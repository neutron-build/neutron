// Multi-Raft invariant tests.
module multi_raft_test {
    import multi_raft.*

    // Test: initial state satisfies both safety invariants
    run init_safe_test = {
        init.then(all { election_safety, log_matching })
    }

    // Test: single election maintains safety
    run single_election_test = {
        init
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(all { election_safety, log_matching })
    }

    // Test: append entry after election
    run append_after_election_test = {
        init
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1, "insert user"))
            .then(all { election_safety, log_matching })
    }

    // Test: step down on higher term
    run step_down_test = {
        init
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(step_down(1, 1, 5))
            .then(all { election_safety, log_matching })
    }

    // Test: elections in different groups are independent
    run independent_groups_test = {
        init
            .then(request_vote(1, 1))   // node 1 runs for group 1
            .then(win_election(1, 1))
            .then(request_vote(2, 2))   // node 2 runs for group 2
            .then(win_election(2, 2))
            .then(all { election_safety, log_matching })
    }

    // Test: leader appends multiple entries then steps down
    run append_then_step_down_test = {
        init
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1, "create table"))
            .then(append_entry(1, 1, "insert row"))
            .then(append_entry(1, 1, "commit"))
            .then(step_down(1, 1, 5))
            .then(all { election_safety, log_matching })
    }

    // Test: new leader elected after step down
    run re_election_test = {
        init
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1, "write"))
            .then(step_down(1, 1, 3))
            .then(request_vote(2, 1))
            .then(win_election(2, 1))
            .then(all { election_safety, log_matching })
    }

    // Test: concurrent leadership across groups with log activity
    run multi_group_log_test = {
        init
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(request_vote(2, 2))
            .then(win_election(2, 2))
            .then(append_entry(1, 1, "shard1 write"))
            .then(append_entry(2, 2, "shard2 write"))
            .then(all { election_safety, log_matching })
    }
}
