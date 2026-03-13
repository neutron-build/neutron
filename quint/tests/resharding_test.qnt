// Resharding invariant tests.
module resharding_test {
    import resharding.*

    // Test: initial state has all keys covered
    run init_coverage_test = {
        init.then(all { no_data_loss, no_double_ownership, key_conservation })
    }

    // Test: begin reshard maintains invariants
    run begin_reshard_test = {
        init
            .then(begin_reshard(1, 2))
            .then(start_copying)
            .then(all { no_data_loss, no_double_ownership, key_conservation })
    }

    // Test: copy key maintains no-data-loss and no-double-ownership
    run copy_key_test = {
        init
            .then(begin_reshard(1, 2))
            .then(start_copying)
            .then(copy_key(1))
            .then(all { no_data_loss, no_double_ownership })
    }

    // Test: full reshard cycle preserves all invariants
    run full_reshard_test = {
        init
            .then(begin_reshard(1, 2))
            .then(start_copying)
            .then(copy_key(1))
            .then(copy_key(2))
            .then(finish_reshard)
            .then(all { no_data_loss, no_double_ownership, key_conservation })
    }

    // Test: multiple keys copied preserve conservation
    run multi_key_conservation_test = {
        init
            .then(begin_reshard(1, 2))
            .then(start_copying)
            .then(copy_key(1))
            .then(copy_key(2))
            .then(copy_key(3))
            .then(copy_key(4))
            .then(copy_key(5))
            .then(all { no_data_loss, no_double_ownership, key_conservation })
    }

    // Test: resharding between different shard pairs
    run different_shards_test = {
        init
            .then(begin_reshard(3, 4))
            .then(start_copying)
            .then(copy_key(51))
            .then(copy_key(52))
            .then(finish_reshard)
            .then(all { no_data_loss, no_double_ownership, key_conservation })
    }

    // Test: sequential resharding operations
    run sequential_reshard_test = {
        init
            .then(begin_reshard(1, 2))
            .then(start_copying)
            .then(copy_key(1))
            .then(finish_reshard)
            // Second reshard after first completes
            .then(begin_reshard(2, 3))
            .then(start_copying)
            .then(copy_key(1))
            .then(finish_reshard)
            .then(all { no_data_loss, key_conservation })
    }
}
