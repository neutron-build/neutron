// Replication — synchronous and asynchronous replication protocols.
module replication {
    import types.*

    pure val PRIMARY: NodeId = 1
    pure val REPLICAS: Set[NodeId] = Set(2, 3)
    pure val ALL_NODES: Set[NodeId] = Set(1, 2, 3)

    var primary_lsn: LSN
    var replica_lsn: NodeId -> LSN
    var sync_mode: str  // "synchronous" | "asynchronous"
    var acked_lsn: LSN

    action init = all {
        primary_lsn' = 0,
        replica_lsn' = (n => 0),
        sync_mode' = "synchronous",
        acked_lsn' = 0,
    }

    // Primary writes a new entry
    action primary_write: bool = all {
        primary_lsn' = primary_lsn + 1,
        replica_lsn' = replica_lsn,
        sync_mode' = sync_mode,
        acked_lsn' = acked_lsn,
    }

    // Replica catches up to primary
    action replica_apply(r: NodeId): bool = all {
        r.in(REPLICAS),
        replica_lsn(r) < primary_lsn,
        replica_lsn' = replica_lsn.set(r, replica_lsn(r) + 1),
        primary_lsn' = primary_lsn,
        sync_mode' = sync_mode,
        // In sync mode, ack when at least one replica catches up
        acked_lsn' = if (sync_mode == "synchronous" and
                         replica_lsn(r) + 1 >= primary_lsn)
                     primary_lsn else acked_lsn,
    }

    action step = any {
        primary_write,
        REPLICAS.exists(r => replica_apply(r)),
    }

    // === SAFETY INVARIANTS ===

    // Replicas never ahead of primary
    val replicas_behind: bool =
        REPLICAS.forall(r => replica_lsn(r) <= primary_lsn)

    // Sync durability: acked writes exist on at least one replica
    val sync_durability: bool =
        sync_mode == "synchronous" implies
        acked_lsn == 0 or REPLICAS.exists(r => replica_lsn(r) >= acked_lsn)
}
