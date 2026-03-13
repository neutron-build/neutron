// Crash model — node failures and restarts.
module crash {
    import types.*

    var alive: Set[NodeId]
    var crashed_nodes: Set[NodeId]

    action crash_node(node: NodeId): bool = all {
        node.in(alive),
        alive' = alive.exclude(Set(node)),
        crashed_nodes' = crashed_nodes.union(Set(node)),
    }

    action restart_node(node: NodeId): bool = all {
        node.in(crashed_nodes),
        alive' = alive.union(Set(node)),
        crashed_nodes' = crashed_nodes.exclude(Set(node)),
    }

    // Invariant: a node is either alive or crashed, never both
    val node_partition: bool =
        alive.intersect(crashed_nodes).size() == 0
}
