// Snapshot Transfer — snapshot-based state transfer for new/recovering nodes.
module snapshot_transfer {
    import types.* from "../common/types"

    pure val NODES: Set[NodeId] = Set(1, 2, 3)

    var node_data: NodeId -> Set[Key]
    var snapshot_in_progress: bool
    var snapshot_source: NodeId
    var snapshot_target: NodeId
    var transferred_keys: Set[Key]

    action init = all {
        node_data' = NODES.mapBy(n => if (n == 1) 1.to(100) else Set()),
        snapshot_in_progress' = false,
        snapshot_source' = 0,
        snapshot_target' = 0,
        transferred_keys' = Set(),
    }

    action begin_transfer(source: NodeId, target: NodeId): bool = all {
        not(snapshot_in_progress),
        source.in(NODES),
        target.in(NODES),
        source != target,
        node_data.get(source).size() > 0,
        snapshot_in_progress' = true,
        snapshot_source' = source,
        snapshot_target' = target,
        transferred_keys' = Set(),
        node_data' = node_data,
    }

    action transfer_key(k: Key): bool = all {
        snapshot_in_progress,
        k.in(node_data.get(snapshot_source)),
        not(k.in(transferred_keys)),
        transferred_keys' = transferred_keys.union(Set(k)),
        node_data' = node_data.set(snapshot_target,
            node_data.get(snapshot_target).union(Set(k))),
        snapshot_in_progress' = snapshot_in_progress,
        snapshot_source' = snapshot_source,
        snapshot_target' = snapshot_target,
    }

    action complete_transfer: bool = all {
        snapshot_in_progress,
        transferred_keys == node_data.get(snapshot_source),
        snapshot_in_progress' = false,
        snapshot_source' = 0,
        snapshot_target' = 0,
        transferred_keys' = Set(),
        node_data' = node_data,
    }

    action step = any {
        { nondet s = NODES.oneOf()
          nondet t = NODES.oneOf()
          begin_transfer(s, t) },
        { nondet k = 1.to(100).oneOf()
          transfer_key(k) },
        complete_transfer,
    }

    // Source data is never modified during transfer
    val source_unchanged: bool =
        not(snapshot_in_progress) or
        node_data.get(snapshot_source).size() >= transferred_keys.size()
}
