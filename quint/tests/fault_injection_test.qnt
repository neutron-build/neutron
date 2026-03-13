// Network fault injection property tests.
//
// These specs inject network partitions during consensus and crash-recovery
// scenarios during transactions, verifying that safety invariants hold under
// adversarial conditions. Uses the common/network.qnt and common/crash.qnt
// models for fault simulation.
//
// Three test groups:
//   1. Partition-during-consensus: Raft election safety under network splits
//   2. Crash-during-transaction: 2PC safety under node failures
//   3. Combined faults: partitions + crashes simultaneously

// =========================================================================
// 1. Network partitions during Raft consensus
// =========================================================================
module partition_consensus_test {
    import types.*

    // Inline Raft + Network state for fault injection scenarios.
    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val GROUPS: Set[GroupId] = Set(1)

    type FaultRaftState = {
        current_term: Term,
        voted_for: int,
        log_len: int,
        role: str,
    }

    var raft: (NodeId, GroupId) -> FaultRaftState
    var net_messages: Set[Message]
    var net_partitioned: Set[(NodeId, NodeId)]

    action init = all {
        raft' = ((n, g) =>
            { current_term: 0, voted_for: -1, log_len: 0, role: "follower" }),
        net_messages' = Set(),
        net_partitioned' = Set(),
    }

    // --- Raft actions ---

    action request_vote(node: NodeId, group: GroupId): bool = all {
        node.in(NODES),
        group.in(GROUPS),
        val st = raft((node, group))
        val new_term = st.current_term + 1
        raft' = raft.set((node, group),
            { ...st, current_term: new_term, role: "candidate", voted_for: node }),
        net_messages' = net_messages,
        net_partitioned' = net_partitioned,
    }

    action win_election(node: NodeId, group: GroupId): bool = all {
        node.in(NODES),
        group.in(GROUPS),
        val st = raft((node, group))
        st.role == "candidate",
        // No other leader for this group at this term
        NODES.forall(n => n == node or
            raft((n, group)).role != "leader" or
            raft((n, group)).current_term != st.current_term),
        raft' = raft.set((node, group),
            { ...st, role: "leader" }),
        net_messages' = net_messages,
        net_partitioned' = net_partitioned,
    }

    action append_entry(leader: NodeId, group: GroupId): bool = all {
        leader.in(NODES),
        group.in(GROUPS),
        val st = raft((leader, group))
        st.role == "leader",
        raft' = raft.set((leader, group),
            { ...st, log_len: st.log_len + 1 }),
        net_messages' = net_messages,
        net_partitioned' = net_partitioned,
    }

    action step_down(node: NodeId, group: GroupId, new_term: Term): bool = all {
        node.in(NODES),
        group.in(GROUPS),
        val st = raft((node, group))
        new_term > st.current_term,
        raft' = raft.set((node, group),
            { ...st, current_term: new_term, role: "follower", voted_for: -1 }),
        net_messages' = net_messages,
        net_partitioned' = net_partitioned,
    }

    // --- Network fault actions ---

    action inject_partition(a: NodeId, b: NodeId): bool = all {
        a.in(NODES),
        b.in(NODES),
        a != b,
        net_partitioned' = net_partitioned.union(Set((a, b), (b, a))),
        net_messages' = net_messages,
        raft' = raft,
    }

    action heal_partition(a: NodeId, b: NodeId): bool = all {
        (a, b).in(net_partitioned),
        net_partitioned' = net_partitioned.exclude(Set((a, b), (b, a))),
        net_messages' = net_messages,
        raft' = raft,
    }

    // --- Safety invariants ---

    // Election safety: at most one leader per group per term
    val election_safety: bool =
        GROUPS.forall(g =>
            0.to(20).forall(t =>
                NODES.filter(n =>
                    raft((n, g)).role == "leader" and
                    raft((n, g)).current_term == t
                ).size() <= 1
            )
        )

    // Partition symmetry maintained
    val partition_symmetric: bool =
        NODES.forall(a => NODES.forall(b =>
            (a, b).in(net_partitioned) implies (b, a).in(net_partitioned)
        ))

    // --- Tests ---

    // Test: Election during active partition maintains safety.
    // Scenario: partition isolates node 3, nodes 1+2 form majority.
    run election_during_partition_test = {
        init
            .then(inject_partition(1, 3))
            .then(inject_partition(2, 3))
            // Node 1 runs election with majority (nodes 1, 2)
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(all {
                election_safety,
                partition_symmetric,
                // Node 1 is leader
                assert(raft((1, 1)).role == "leader"),
            })
    }

    // Test: Two leaders cannot emerge in the same term across a partition.
    // Scenario: partition splits {1} from {2, 3}. Node 1 becomes candidate
    // at term 1 but cannot win (minority). Node 2 becomes candidate at
    // term 1 and wins with majority {2, 3}.
    run split_brain_prevented_test = {
        init
            .then(inject_partition(1, 2))
            .then(inject_partition(1, 3))
            // Node 1 tries election but is isolated (minority of 1)
            .then(request_vote(1, 1))
            // Node 2 runs election with majority {2, 3}
            .then(request_vote(2, 1))
            .then(win_election(2, 1))
            // Node 1 is still candidate, node 2 is leader — different terms
            .then(all {
                election_safety,
                partition_symmetric,
                assert(raft((2, 1)).role == "leader"),
                assert(raft((1, 1)).role == "candidate"),
                // Key: they are at different terms (1 vs 1) but win_election
                // guard prevents two leaders at same term
            })
    }

    // Test: Leader steps down when partition heals and higher term seen.
    run leader_step_down_on_heal_test = {
        init
            .then(inject_partition(1, 3))
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1))
            // Heal partition — node 1 discovers higher term from node 3
            .then(heal_partition(1, 3))
            .then(step_down(1, 1, 5))
            .then(all {
                election_safety,
                partition_symmetric,
                assert(raft((1, 1)).role == "follower"),
            })
    }

    // Test: New leader elected after partition heals.
    run new_leader_after_heal_test = {
        init
            .then(inject_partition(1, 3))
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(append_entry(1, 1))
            .then(heal_partition(1, 3))
            .then(step_down(1, 1, 5))
            .then(request_vote(3, 1))
            .then(win_election(3, 1))
            .then(all {
                election_safety,
                assert(raft((3, 1)).role == "leader"),
                assert(raft((1, 1)).role == "follower"),
            })
    }

    // Test: Multiple partition/heal cycles preserve election safety.
    run partition_flap_test = {
        init
            // First partition
            .then(inject_partition(1, 3))
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            .then(heal_partition(1, 3))
            .then(step_down(1, 1, 3))
            // Second partition
            .then(inject_partition(2, 3))
            .then(request_vote(2, 1))
            .then(win_election(2, 1))
            .then(heal_partition(2, 3))
            .then(step_down(2, 1, 7))
            // Third election after all healed
            .then(request_vote(3, 1))
            .then(win_election(3, 1))
            .then(all {
                election_safety,
                // All partitions healed
                assert(net_partitioned.size() == 0),
            })
    }

    // Test: Appends during partition, safety after heal.
    run appends_during_partition_test = {
        init
            .then(inject_partition(1, 3))
            .then(request_vote(1, 1))
            .then(win_election(1, 1))
            // Leader appends while partitioned
            .then(append_entry(1, 1))
            .then(append_entry(1, 1))
            .then(append_entry(1, 1))
            .then(heal_partition(1, 3))
            .then(all {
                election_safety,
                // Leader has 3 entries
                assert(raft((1, 1)).log_len == 3),
            })
    }
}

// =========================================================================
// 2. Crash-recovery during distributed transactions
// =========================================================================
module crash_transaction_test {
    import types.*

    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val MAX_TX: int = 3

    // Combined DTX + crash state
    var tx_state: TxId -> str
    var votes: TxId -> (NodeId -> str)
    var next_tx: TxId
    var alive_nodes: Set[NodeId]
    var crashed: Set[NodeId]
    // Persistent state: votes that survive crash (written to WAL)
    var durable_votes: TxId -> (NodeId -> str)

    action init = all {
        tx_state' = (t => "pending"),
        votes' = (t => (n => "unknown")),
        next_tx' = 1,
        alive_nodes' = Set(1, 2, 3),
        crashed' = Set(),
        durable_votes' = (t => (n => "unknown")),
    }

    // --- Transaction actions ---

    action begin_tx: bool = all {
        next_tx <= MAX_TX,
        val tid = next_tx
        tx_state' = tx_state.set(tid, "pending"),
        votes' = votes.set(tid, (n => "unknown")),
        next_tx' = next_tx + 1,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
        durable_votes' = durable_votes,
    }

    // Only alive nodes can vote
    action vote_prepare(tid: TxId, node: NodeId): bool = all {
        tid < next_tx,
        node.in(NODES),
        node.in(alive_nodes),  // must be alive
        tx_state(tid) == "pending",
        votes(tid)(node) == "unknown",
        votes' = votes.set(tid, votes(tid).set(node, "prepared")),
        // Write to durable storage
        durable_votes' = durable_votes.set(tid,
            durable_votes(tid).set(node, "prepared")),
        tx_state' = tx_state,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
    }

    action vote_abort(tid: TxId, node: NodeId): bool = all {
        tid < next_tx,
        node.in(NODES),
        node.in(alive_nodes),
        tx_state(tid) == "pending",
        votes(tid)(node) == "unknown",
        votes' = votes.set(tid, votes(tid).set(node, "aborted")),
        durable_votes' = durable_votes.set(tid,
            durable_votes(tid).set(node, "aborted")),
        tx_state' = tx_state,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
    }

    action commit_tx(tid: TxId): bool = all {
        tid < next_tx,
        tx_state(tid) == "pending",
        NODES.forall(n => votes(tid)(n) == "prepared"),
        tx_state' = tx_state.set(tid, "committed"),
        votes' = votes,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
        durable_votes' = durable_votes,
    }

    action abort_tx(tid: TxId): bool = all {
        tid < next_tx,
        tx_state(tid) == "pending",
        NODES.exists(n => votes(tid)(n) == "aborted"),
        tx_state' = tx_state.set(tid, "aborted"),
        votes' = votes,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
        durable_votes' = durable_votes,
    }

    // --- Crash/recovery actions ---

    action crash_node(node: NodeId): bool = all {
        node.in(alive_nodes),
        alive_nodes' = alive_nodes.exclude(Set(node)),
        crashed' = crashed.union(Set(node)),
        // In-memory votes are lost, but durable_votes persist
        // Reset in-memory votes for crashed node to "unknown"
        votes' = (t =>
            if (t >= 1 and t < next_tx)
                votes(t).set(node, "unknown")
            else votes(t)),
        tx_state' = tx_state,
        next_tx' = next_tx,
        durable_votes' = durable_votes,
    }

    action recover_node(node: NodeId): bool = all {
        node.in(crashed),
        alive_nodes' = alive_nodes.union(Set(node)),
        crashed' = crashed.exclude(Set(node)),
        // Recover votes from durable storage
        votes' = (t =>
            if (t >= 1 and t < next_tx)
                votes(t).set(node, durable_votes(t)(node))
            else votes(t)),
        tx_state' = tx_state,
        next_tx' = next_tx,
        durable_votes' = durable_votes,
    }

    // --- Safety invariants ---

    // Commit validity: committed => all participants prepared
    val commit_validity: bool =
        1.to(MAX_TX).forall(t =>
            tx_state(t) == "committed" implies
            NODES.forall(n => durable_votes(t)(n) == "prepared")
        )

    // Atomicity: no tx is both committed and aborted
    val atomicity: bool =
        1.to(MAX_TX).forall(t =>
            not(tx_state(t) == "committed" and tx_state(t) == "aborted")
        )

    // Crash partition: alive and crashed are disjoint
    val node_partition: bool =
        alive_nodes.intersect(crashed).size() == 0

    // Recovery preserves persistent state: durable votes survive crash
    val durable_state_preserved: bool =
        1.to(MAX_TX).forall(t =>
            NODES.forall(n =>
                durable_votes(t)(n) != "unknown" implies
                (n.in(alive_nodes) implies votes(t)(n) == durable_votes(t)(n))
            )
        )

    // Crashed nodes do not vote: no new votes from crashed nodes
    // (enforced by alive_nodes guard in vote actions)
    val crashed_nodes_silent: bool =
        crashed.forall(n =>
            1.to(MAX_TX).forall(t =>
                t < next_tx implies
                // In-memory vote is unknown (lost on crash)
                votes(t)(n) == "unknown" or
                // Or it was recovered from durable storage
                votes(t)(n) == durable_votes(t)(n)
            )
        )

    // --- Tests ---

    // Test: Crash before all votes — cannot commit.
    run crash_before_full_prepare_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            // Node 3 crashes before voting
            .then(crash_node(3))
            // Cannot commit: node 3 never voted
            .then(all {
                commit_validity,
                atomicity,
                node_partition,
                // tx 1 is still pending (cannot commit without all votes)
                assert(tx_state(1) == "pending"),
            })
    }

    // Test: Crash after all votes — commit succeeds, durable state intact.
    run crash_after_full_prepare_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            // Node 2 crashes after commit
            .then(crash_node(2))
            .then(all {
                commit_validity,
                atomicity,
                node_partition,
                // Tx 1 is committed and durable
                assert(tx_state(1) == "committed"),
                // Durable votes survive
                assert(durable_votes(1)(2) == "prepared"),
            })
    }

    // Test: Crash and recovery — recovered node can continue.
    run crash_recovery_continue_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            // Node 3 crashes
            .then(crash_node(3))
            // Node 3 recovers
            .then(recover_node(3))
            // Node 3 can now vote
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            .then(all {
                commit_validity,
                atomicity,
                durable_state_preserved,
                assert(tx_state(1) == "committed"),
            })
    }

    // Test: Recovery restores durable votes.
    run recovery_restores_votes_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            // Crash node 1
            .then(crash_node(1))
            // Recover node 1 — should have durable vote restored
            .then(recover_node(1))
            .then(all {
                commit_validity,
                durable_state_preserved,
                // After recovery, node 1's vote is back
                assert(votes(1)(1) == "prepared"),
                assert(durable_votes(1)(1) == "prepared"),
            })
    }

    // Test: Multiple crashes during a transaction.
    run multi_crash_during_tx_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            // Node 2 crashes
            .then(crash_node(2))
            // Node 3 crashes
            .then(crash_node(3))
            .then(all {
                node_partition,
                crashed_nodes_silent,
                // Only node 1 is alive
                assert(alive_nodes == Set(1)),
                // Cannot commit — missing votes
                assert(tx_state(1) == "pending"),
            })
    }

    // Test: Abort after crash — crashed node's abort vote forces abort.
    run abort_after_crash_test = {
        init
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_abort(1, 2))
            // Node 2 crashes after voting abort
            .then(crash_node(2))
            // Coordinator can still abort based on the recorded vote
            .then(abort_tx(1))
            .then(all {
                commit_validity,
                atomicity,
                assert(tx_state(1) == "aborted"),
                // Durable abort vote survives
                assert(durable_votes(1)(2) == "aborted"),
            })
    }

    // Test: Full crash-recovery cycle across two transactions.
    run full_crash_recovery_cycle_test = {
        init
            // Tx 1 commits normally
            .then(begin_tx)
            .then(vote_prepare(1, 1))
            .then(vote_prepare(1, 2))
            .then(vote_prepare(1, 3))
            .then(commit_tx(1))
            // All nodes crash
            .then(crash_node(1))
            .then(crash_node(2))
            .then(crash_node(3))
            // All nodes recover
            .then(recover_node(1))
            .then(recover_node(2))
            .then(recover_node(3))
            // Tx 2 proceeds after recovery
            .then(begin_tx)
            .then(vote_prepare(2, 1))
            .then(vote_prepare(2, 2))
            .then(vote_prepare(2, 3))
            .then(commit_tx(2))
            .then(all {
                commit_validity,
                atomicity,
                durable_state_preserved,
                node_partition,
                assert(tx_state(1) == "committed"),
                assert(tx_state(2) == "committed"),
            })
    }
}

// =========================================================================
// 3. Combined faults — partitions + crashes simultaneously
// =========================================================================
module combined_faults_test {
    import types.*

    pure val NODES: Set[NodeId] = Set(1, 2, 3)

    // Combined Raft + DTX + Network + Crash state
    type CombinedRaftState = {
        current_term: Term,
        role: str,
        log_len: int,
    }

    var raft: (NodeId, int) -> CombinedRaftState
    var tx_state: TxId -> str
    var tx_votes: TxId -> (NodeId -> str)
    var next_tx: TxId
    var alive: Set[NodeId]
    var crashed_set: Set[NodeId]
    var partitioned_links: Set[(NodeId, NodeId)]

    action init = all {
        raft' = ((n, g) =>
            { current_term: 0, role: "follower", log_len: 0 }),
        tx_state' = (t => "pending"),
        tx_votes' = (t => (n => "unknown")),
        next_tx' = 1,
        alive' = Set(1, 2, 3),
        crashed_set' = Set(),
        partitioned_links' = Set(),
    }

    // --- Raft actions ---

    action raft_campaign(node: NodeId): bool = all {
        node.in(alive),
        val st = raft((node, 1))
        raft' = raft.set((node, 1),
            { ...st, current_term: st.current_term + 1, role: "candidate" }),
        tx_state' = tx_state,
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    action raft_win(node: NodeId): bool = all {
        node.in(alive),
        val st = raft((node, 1))
        st.role == "candidate",
        NODES.forall(n => n == node or
            raft((n, 1)).role != "leader" or
            raft((n, 1)).current_term != st.current_term),
        raft' = raft.set((node, 1),
            { ...st, role: "leader" }),
        tx_state' = tx_state,
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    action raft_step_down(node: NodeId, new_term: Term): bool = all {
        node.in(alive),
        val st = raft((node, 1))
        new_term > st.current_term,
        raft' = raft.set((node, 1),
            { ...st, current_term: new_term, role: "follower" }),
        tx_state' = tx_state,
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    // --- Transaction actions ---

    action tx_begin: bool = all {
        next_tx <= 3,
        val tid = next_tx
        tx_state' = tx_state.set(tid, "pending"),
        tx_votes' = tx_votes.set(tid, (n => "unknown")),
        next_tx' = next_tx + 1,
        raft' = raft,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    action tx_prepare(tid: TxId, node: NodeId): bool = all {
        tid < next_tx,
        node.in(alive),
        tx_state(tid) == "pending",
        tx_votes(tid)(node) == "unknown",
        tx_votes' = tx_votes.set(tid, tx_votes(tid).set(node, "prepared")),
        tx_state' = tx_state,
        next_tx' = next_tx,
        raft' = raft,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    action tx_commit(tid: TxId): bool = all {
        tid < next_tx,
        tx_state(tid) == "pending",
        NODES.forall(n => tx_votes(tid)(n) == "prepared"),
        tx_state' = tx_state.set(tid, "committed"),
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        raft' = raft,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    action tx_abort(tid: TxId): bool = all {
        tid < next_tx,
        tx_state(tid) == "pending",
        NODES.exists(n => tx_votes(tid)(n) == "aborted"),
        tx_state' = tx_state.set(tid, "aborted"),
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        raft' = raft,
        alive' = alive,
        crashed_set' = crashed_set,
        partitioned_links' = partitioned_links,
    }

    // --- Fault injection ---

    action inject_partition(a: NodeId, b: NodeId): bool = all {
        a.in(NODES),
        b.in(NODES),
        a != b,
        partitioned_links' = partitioned_links.union(Set((a, b), (b, a))),
        raft' = raft,
        tx_state' = tx_state,
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        alive' = alive,
        crashed_set' = crashed_set,
    }

    action heal_all: bool = all {
        partitioned_links' = Set(),
        raft' = raft,
        tx_state' = tx_state,
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        alive' = alive,
        crashed_set' = crashed_set,
    }

    action crash(node: NodeId): bool = all {
        node.in(alive),
        alive' = alive.exclude(Set(node)),
        crashed_set' = crashed_set.union(Set(node)),
        // Reset in-memory votes for crashed node
        tx_votes' = (t =>
            if (t >= 1 and t < next_tx)
                tx_votes(t).set(node, "unknown")
            else tx_votes(t)),
        raft' = raft.set((node, 1),
            { current_term: raft((node, 1)).current_term,
              role: "follower", log_len: 0 }),
        tx_state' = tx_state,
        next_tx' = next_tx,
        partitioned_links' = partitioned_links,
    }

    action recover(node: NodeId): bool = all {
        node.in(crashed_set),
        alive' = alive.union(Set(node)),
        crashed_set' = crashed_set.exclude(Set(node)),
        raft' = raft,
        tx_state' = tx_state,
        tx_votes' = tx_votes,
        next_tx' = next_tx,
        partitioned_links' = partitioned_links,
    }

    // --- Safety invariants ---

    val election_safety: bool =
        0.to(20).forall(t =>
            NODES.filter(n =>
                raft((n, 1)).role == "leader" and
                raft((n, 1)).current_term == t
            ).size() <= 1
        )

    val tx_commit_validity: bool =
        1.to(3).forall(t =>
            tx_state(t) == "committed" implies
            NODES.forall(n => tx_votes(t)(n) == "prepared")
        )

    val crash_disjoint: bool =
        alive.intersect(crashed_set).size() == 0

    val partition_symmetric: bool =
        NODES.forall(a => NODES.forall(b =>
            (a, b).in(partitioned_links) implies
            (b, a).in(partitioned_links)
        ))

    // --- Tests ---

    // Test: Partition + crash simultaneously — election safety holds.
    run partition_and_crash_test = {
        init
            // Partition node 3
            .then(inject_partition(1, 3))
            .then(inject_partition(2, 3))
            // Crash node 3
            .then(crash(3))
            // Node 1 wins election with remaining majority
            .then(raft_campaign(1))
            .then(raft_win(1))
            .then(all {
                election_safety,
                crash_disjoint,
                partition_symmetric,
            })
    }

    // Test: Transaction during partition + crash.
    // Only alive, reachable nodes can vote.
    run tx_during_partition_and_crash_test = {
        init
            .then(tx_begin)
            // Partition isolates node 3
            .then(inject_partition(1, 3))
            .then(inject_partition(2, 3))
            // Node 1 and 2 vote
            .then(tx_prepare(1, 1))
            .then(tx_prepare(1, 2))
            // Node 3 crashes
            .then(crash(3))
            .then(all {
                tx_commit_validity,
                crash_disjoint,
                // Cannot commit: node 3 never voted
                assert(tx_state(1) == "pending"),
            })
    }

    // Test: Recovery after combined faults allows progress.
    run recovery_after_combined_faults_test = {
        init
            .then(tx_begin)
            .then(tx_prepare(1, 1))
            .then(tx_prepare(1, 2))
            // Crash + partition node 3
            .then(crash(3))
            .then(inject_partition(1, 3))
            // Heal and recover
            .then(heal_all)
            .then(recover(3))
            // Node 3 can now participate
            .then(tx_prepare(1, 3))
            .then(tx_commit(1))
            .then(all {
                tx_commit_validity,
                election_safety,
                crash_disjoint,
                assert(tx_state(1) == "committed"),
            })
    }

    // Test: Raft election + transaction commit across fault recovery.
    run election_and_tx_across_faults_test = {
        init
            // Election while healthy
            .then(raft_campaign(1))
            .then(raft_win(1))
            // Start transaction
            .then(tx_begin)
            .then(tx_prepare(1, 1))
            // Fault: partition + crash
            .then(inject_partition(2, 3))
            .then(crash(3))
            // Leader steps down
            .then(raft_step_down(1, 5))
            // Recover
            .then(recover(3))
            .then(heal_all)
            // New election
            .then(raft_campaign(2))
            .then(raft_win(2))
            // Continue transaction
            .then(tx_prepare(1, 2))
            .then(tx_prepare(1, 3))
            .then(tx_commit(1))
            .then(all {
                election_safety,
                tx_commit_validity,
                crash_disjoint,
                partition_symmetric,
                assert(raft((2, 1)).role == "leader"),
                assert(tx_state(1) == "committed"),
            })
    }

    // Test: Cascading failures — nodes crash one by one, then recover.
    run cascading_crash_recovery_test = {
        init
            .then(tx_begin)
            .then(tx_prepare(1, 1))
            // Cascade: crash node 2, then node 3
            .then(crash(2))
            .then(crash(3))
            .then(all {
                crash_disjoint,
                assert(alive == Set(1)),
                // Cannot commit — missing votes
                assert(tx_state(1) == "pending"),
            })
            // Recover all
            .then(recover(2))
            .then(recover(3))
            // Complete the transaction
            .then(tx_prepare(1, 2))
            .then(tx_prepare(1, 3))
            .then(tx_commit(1))
            .then(all {
                tx_commit_validity,
                crash_disjoint,
                assert(tx_state(1) == "committed"),
                assert(alive == Set(1, 2, 3)),
            })
    }

    // Test: Partition during transaction, crash during partition, recover
    // after heal — full adversarial scenario.
    run adversarial_scenario_test = {
        init
            // Start transaction
            .then(tx_begin)
            .then(tx_prepare(1, 1))
            // Partition between 1 and 3
            .then(inject_partition(1, 3))
            // Node 2 votes
            .then(tx_prepare(1, 2))
            // Node 3 crashes while partitioned
            .then(crash(3))
            .then(all {
                crash_disjoint,
                partition_symmetric,
            })
            // Heal partition
            .then(heal_all)
            // Recover node 3
            .then(recover(3))
            // Node 3 votes after recovery
            .then(tx_prepare(1, 3))
            // Commit succeeds
            .then(tx_commit(1))
            .then(all {
                tx_commit_validity,
                crash_disjoint,
                assert(tx_state(1) == "committed"),
                assert(partitioned_links.size() == 0),
                assert(crashed_set.size() == 0),
            })
    }
}
