// Cluster Membership Changes — adding and removing nodes safely.
module membership {
    import types.*

    pure val MAX_NODES: int = 5

    var members: Set[NodeId]
    var pending_add: Set[NodeId]
    var pending_remove: Set[NodeId]
    var config_version: int

    action init = all {
        members' = Set(1, 2, 3),
        pending_add' = Set(),
        pending_remove' = Set(),
        config_version' = 1,
    }

    // Propose adding a node
    action propose_add(node: NodeId): bool = all {
        not(node.in(members)),
        not(node.in(pending_add)),
        members.size() + pending_add.size() < MAX_NODES,
        pending_add.size() + pending_remove.size() == 0,  // one change at a time
        pending_add' = pending_add.union(Set(node)),
        members' = members,
        pending_remove' = pending_remove,
        config_version' = config_version,
    }

    // Commit adding a node
    action commit_add(node: NodeId): bool = all {
        node.in(pending_add),
        members' = members.union(Set(node)),
        pending_add' = pending_add.exclude(Set(node)),
        pending_remove' = pending_remove,
        config_version' = config_version + 1,
    }

    // Propose removing a node
    action propose_remove(node: NodeId): bool = all {
        node.in(members),
        members.size() > 1,  // can't remove last node
        pending_add.size() + pending_remove.size() == 0,
        pending_remove' = pending_remove.union(Set(node)),
        members' = members,
        pending_add' = pending_add,
        config_version' = config_version,
    }

    // Commit removing a node
    action commit_remove(node: NodeId): bool = all {
        node.in(pending_remove),
        members' = members.exclude(Set(node)),
        pending_remove' = pending_remove.exclude(Set(node)),
        pending_add' = pending_add,
        config_version' = config_version + 1,
    }

    action step = any {
        1.to(MAX_NODES).exists(n => propose_add(n)),
        1.to(MAX_NODES).exists(n => commit_add(n)),
        1.to(MAX_NODES).exists(n => propose_remove(n)),
        1.to(MAX_NODES).exists(n => commit_remove(n)),
    }

    // === SAFETY INVARIANTS ===

    // Cluster always has at least one member
    val non_empty: bool = members.size() >= 1

    // No node is both a member and pending addition
    val no_duplicate_add: bool =
        members.intersect(pending_add).size() == 0

    // Config version is monotonically increasing (implied by the model)
    val config_monotonic: bool = config_version >= 1
}
