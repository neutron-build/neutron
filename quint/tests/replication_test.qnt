// Replication protocol invariant tests.
module replication_test {
    import replication.*

    // Test: initial state satisfies all invariants
    run init_valid_test = {
        init.then(all { replicas_behind, sync_durability })
    }

    // Test: primary write maintains replica ordering
    run primary_write_test = {
        init
            .then(primary_write)
            .then(all { replicas_behind, sync_durability })
    }

    // Test: replica catches up to primary
    run replica_apply_test = {
        init
            .then(primary_write)
            .then(replica_apply(2))
            .then(all { replicas_behind, sync_durability })
    }

    // Test: multiple writes then replica catch-up
    run multi_write_catchup_test = {
        init
            .then(primary_write)
            .then(primary_write)
            .then(primary_write)
            .then(replica_apply(2))
            .then(replica_apply(2))
            .then(replica_apply(2))
            .then(all { replicas_behind, sync_durability })
    }

    // Test: both replicas catching up independently
    run dual_replica_test = {
        init
            .then(primary_write)
            .then(primary_write)
            .then(replica_apply(2))
            .then(replica_apply(3))
            .then(all { replicas_behind, sync_durability })
    }

    // Test: sync durability — after replica catches up to primary_lsn,
    // acked_lsn should reflect that
    run sync_ack_test = {
        init
            .then(primary_write)
            .then(replica_apply(2))
            // In synchronous mode, acked_lsn should now equal primary_lsn
            .then(all { replicas_behind, sync_durability })
    }

    // Test: staggered writes and applies maintain ordering
    run staggered_test = {
        init
            .then(primary_write)
            .then(replica_apply(2))
            .then(primary_write)
            .then(replica_apply(3))
            .then(primary_write)
            .then(replica_apply(2))
            .then(replica_apply(2))
            .then(all { replicas_behind, sync_durability })
    }
}
