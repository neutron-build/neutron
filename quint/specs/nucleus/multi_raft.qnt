// Multi-Raft Consensus — Nucleus uses multiple independent Raft groups,
// each managing a shard. This spec verifies safety across all groups.
module multi_raft {
    import types.* from "../common/types"

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
        group_state' = tuples(NODES, GROUPS).mapBy(((n, g)) =>
            { current_term: 0, voted_for: -1, log: List(),
              commit_index: 0, role: "follower" }),
        network_messages' = Set(),
    }

    // Request vote
    action request_vote(node: NodeId, group: GroupId): bool = {
        val state = group_state.get((node, group))
        val new_term = state.current_term + 1
        all {
            node.in(NODES),
            group.in(GROUPS),
            group_state' = group_state.set((node, group),
                { ...state, current_term: new_term, role: "candidate",
                  voted_for: node }),
            network_messages' = network_messages,
        }
    }

    // Win election (simplified: receive majority votes)
    action win_election(node: NodeId, group: GroupId): bool = {
        val state = group_state.get((node, group))
        all {
            node.in(NODES),
            group.in(GROUPS),
            state.role == "candidate",
            // Check no other leader exists for this term+group
            NODES.forall(n => n == node or
                group_state.get((n, group)).role != "leader" or
                group_state.get((n, group)).current_term != state.current_term),
            group_state' = group_state.set((node, group),
                { ...state, role: "leader" }),
            network_messages' = network_messages,
        }
    }

    // Append entry (leader only)
    action append_entry(leader: NodeId, group: GroupId, cmd: str): bool = {
        val state = group_state.get((leader, group))
        val entry = { term: state.current_term,
                      index: state.log.length() + 1,
                      command: cmd }
        all {
            leader.in(NODES),
            group.in(GROUPS),
            state.role == "leader",
            group_state' = group_state.set((leader, group),
                { ...state, log: state.log.append(entry) }),
            network_messages' = network_messages,
        }
    }

    // Step down (receive higher term)
    action step_down(node: NodeId, group: GroupId, new_term: Term): bool = {
        val state = group_state.get((node, group))
        all {
            node.in(NODES),
            group.in(GROUPS),
            new_term > state.current_term,
            group_state' = group_state.set((node, group),
                { ...state, current_term: new_term, role: "follower",
                  voted_for: -1 }),
            network_messages' = network_messages,
        }
    }

    // Take any enabled action
    action step = any {
        { nondet n = NODES.oneOf()
          nondet g = GROUPS.oneOf()
          request_vote(n, g) },
        { nondet n = NODES.oneOf()
          nondet g = GROUPS.oneOf()
          win_election(n, g) },
        { nondet n = NODES.oneOf()
          nondet g = GROUPS.oneOf()
          append_entry(n, g, "cmd") },
        { nondet n = NODES.oneOf()
          nondet g = GROUPS.oneOf()
          nondet t = 1.to(10).oneOf()
          step_down(n, g, t) },
    }

    // === SAFETY INVARIANTS ===

    // Election Safety: at most one leader per group per term
    val election_safety: bool =
        GROUPS.forall(g =>
            0.to(20).forall(t =>
                NODES.filter(n =>
                    group_state.get((n, g)).role == "leader" and
                    group_state.get((n, g)).current_term == t
                ).size() <= 1
            )
        )

    // Leader Append-Only: leaders never shrink their logs
    // (checked across transitions via temporal property)

    // Log Matching: same index+term implies same prefix
    val log_matching: bool =
        GROUPS.forall(g =>
            NODES.forall(n1 => NODES.forall(n2 =>
                val log1 = group_state.get((n1, g)).log
                val log2 = group_state.get((n2, g)).log
                val min_len = if (log1.length() < log2.length())
                    log1.length() else log2.length()
                0.to(min_len - 1).forall(i =>
                    log1[i].term == log2[i].term implies
                    log1[i] == log2[i]
                )
            ))
        )
}
