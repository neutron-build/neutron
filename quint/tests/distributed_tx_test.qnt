// Distributed transaction invariant tests.
module distributed_tx_test {
    import distributed_tx.*

    // Test: initial state satisfies all invariants
    run init_safe_test = {
        init.then(all { commit_validity, atomicity, no_committed_abort })
    }

    // Test: prepare and commit flow
    run prepare_commit_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            .then(all { commit_validity, atomicity, no_committed_abort })
    }

    // Test: abort if any participant aborts
    run abort_flow_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_abort(1, 2))
            .then(abort_tx(1))
            .then(all { commit_validity, atomicity, no_committed_abort })
    }

    // Test: multiple transactions — one committed, one aborted
    run mixed_outcomes_test = {
        init
            .then(begin_tx)  // tx 1
            .then(begin_tx)  // tx 2
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            .then(vote_prepare(2, 1))
            .then(vote_abort(2, 2))
            .then(abort_tx(2))
            .then(all { commit_validity, atomicity, no_committed_abort })
    }

    // Test: multiple sequential commits
    run sequential_commits_test = {
        init
            .then(begin_tx)  // tx 1
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            .then(begin_tx)  // tx 2
            .then(vote_prepare(2, 1))
            .then(vote_prepare(2, 2))
            .then(vote_prepare(2, 3))
            .then(commit_tx(2))
            .then(all { commit_validity, atomicity, no_committed_abort })
    }

    // Test: partial prepare then abort preserves invariants
    run partial_prepare_abort_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_abort(1, 3))
            .then(abort_tx(1))
            .then(all { commit_validity, atomicity, no_committed_abort })
    }
}
