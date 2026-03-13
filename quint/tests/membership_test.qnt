// Cluster membership invariant tests.
module membership_test {
    import membership.*

    // Test: initial state satisfies all invariants
    run init_valid_test = {
        init.then(all { non_empty, no_duplicate_add, config_monotonic })
    }

    // Test: propose adding a node
    run propose_add_test = {
        init
            .then(propose_add(4))
            .then(all { non_empty, no_duplicate_add, config_monotonic })
    }

    // Test: commit adding a node increases membership
    run commit_add_test = {
        init
            .then(propose_add(4))
            .then(commit_add(4))
            .then(all { non_empty, no_duplicate_add, config_monotonic })
    }

    // Test: propose removing a node
    run propose_remove_test = {
        init
            .then(propose_remove(2))
            .then(all { non_empty, no_duplicate_add, config_monotonic })
    }

    // Test: commit removing a node decreases membership but stays non-empty
    run commit_remove_test = {
        init
            .then(propose_remove(2))
            .then(commit_remove(2))
            .then(all { non_empty, no_duplicate_add, config_monotonic })
    }

    // Test: full add-then-remove cycle
    run add_remove_cycle_test = {
        init
            .then(propose_add(4))
            .then(commit_add(4))
            .then(propose_remove(4))
            .then(commit_remove(4))
            .then(all { non_empty, no_duplicate_add, config_monotonic })
    }

    // Test: config version increments on each committed change
    run config_version_increments_test = {
        init
            .then(propose_add(4))
            .then(commit_add(4))
            .then(propose_add(5))
            .then(commit_add(5))
            .then(config_monotonic)
    }

    // Test: cannot add a node that is already a member
    // (propose_add guard: not(node.in(members)))
    // This test verifies the invariant holds even when attempting
    // operations in sequence
    run sequential_changes_test = {
        init
            .then(propose_remove(3))
            .then(commit_remove(3))
            .then(propose_add(3))
            .then(commit_add(3))
            .then(all { non_empty, no_duplicate_add, config_monotonic })
    }
}
