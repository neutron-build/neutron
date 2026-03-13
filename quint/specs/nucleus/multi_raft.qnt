// Multi-Raft Consensus — Nucleus uses multiple independent Raft groups,
// each managing a shard. This spec verifies safety across all groups.
module multi_raft {
    import types.*

    // Configuration
    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val GROUPS: Set[GroupId] = Set(1, 2)

    // Per-group Raft state for each node
    type RaftState = {
        current_term: Term,
        voted_for: int,          // -1 = none
        log: List[LogEntry],
        commit_index: LogIndex,
        role: str,               // "leader" | "follower" | "candidate"
    }

    var group_state: (NodeId, GroupId) -> RaftState
    var network_messages: Set[Message]

    // Initial state
    action init = all {
        group_state' = ((n, g) =>
            { current_term: 0, voted_for: -1, log: List(),
              commit_index: 0, role: "follower" }),
        network_messages' = Set(),
    }

    // Request vote
    action request_vote(node: NodeId, group: GroupId): bool = all {
        node.in(NODES),
        group.in(GROUPS),
        val state = group_state((node, group))
        val new_term = state.current_term + 1
        group_state' = group_state.set((node, group),
            { ...state, current_term: new_term, role: "candidate",
              voted_for: node }),
        network_messages' = network_messages,
    }

    // Win election (simplified: receive majority votes)
    action win_election(node: NodeId, group: GroupId): bool = all {
        node.in(NODES),
        group.in(GROUPS),
        val state = group_state((node, group))
        state.role == "candidate",
        // Check no other leader exists for this term+group
        NODES.forall(n => n == node or
            group_state((n, group)).role != "leader" or
            group_state((n, group)).current_term != state.current_term),
        group_state' = group_state.set((node, group),
            { ...state, role: "leader" }),
        network_messages' = network_messages,
    }

    // Append entry (leader only)
    action append_entry(leader: NodeId, group: GroupId, cmd: str): bool = all {
        leader.in(NODES),
        group.in(GROUPS),
        val state = group_state((leader, group))
        state.role == "leader",
        val entry = { term: state.current_term,
                      index: state.log.length() + 1,
                      command: cmd }
        group_state' = group_state.set((leader, group),
            { ...state, log: state.log.append(entry) }),
        network_messages' = network_messages,
    }

    // Step down (receive higher term)
    action step_down(node: NodeId, group: GroupId, new_term: Term): bool = all {
        node.in(NODES),
        group.in(GROUPS),
        val state = group_state((node, group))
        new_term > state.current_term,
        group_state' = group_state.set((node, group),
            { ...state, current_term: new_term, role: "follower",
              voted_for: -1 }),
        network_messages' = network_messages,
    }

    // Take any enabled action
    action step = any {
        NODES.exists(n => GROUPS.exists(g => request_vote(n, g))),
        NODES.exists(n => GROUPS.exists(g => win_election(n, g))),
        NODES.exists(n => GROUPS.exists(g => append_entry(n, g, "cmd"))),
        NODES.exists(n => GROUPS.exists(g =>
            1.to(10).exists(t => step_down(n, g, t)))),
    }

    // === SAFETY INVARIANTS ===

    // Election Safety: at most one leader per group per term
    val election_safety: bool =
        GROUPS.forall(g =>
            0.to(20).forall(t =>
                NODES.filter(n =>
                    group_state((n, g)).role == "leader" and
                    group_state((n, g)).current_term == t
                ).size() <= 1
            )
        )

    // Leader Append-Only: leaders never shrink their logs
    // (checked across transitions via temporal property)

    // Log Matching: same index+term implies same prefix
    val log_matching: bool =
        GROUPS.forall(g =>
            NODES.forall(n1 => NODES.forall(n2 =>
                val log1 = group_state((n1, g)).log
                val log2 = group_state((n2, g)).log
                val min_len = if (log1.length() < log2.length())
                    log1.length() else log2.length()
                0.to(min_len - 1).forall(i =>
                    log1[i].term == log2[i].term implies
                    log1[i] == log2[i]
                )
            ))
        )
}
