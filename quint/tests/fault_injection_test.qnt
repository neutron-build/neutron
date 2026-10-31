// Fault scenarios use the concrete message/vote model. No liveness or implementation refinement claim.
module partition_consensus_test {
    import multi_raft.* from "../specs/nucleus/multi_raft"
    run election_during_partition_test = init.then(partition(1,3)).then(partition(2,3))
        .then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .expect(election_safety and group_state.get((1,1)).role == "leader")
    run split_brain_prevented_test = init.then(partition(1,2)).then(partition(1,3))
        .then(request_vote(1,1)).then(request_vote(2,1)).then(deliver_vote(3,2,1)).then(receive_vote(3,2,1)).then(win_election(2,1))
        .expect(election_safety and votes.get((1,1)).size() == 1 and group_state.get((1,1)).role == "candidate")
    run leader_step_down_on_heal_test = init.then(partition(1,3))
        .then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .then(append_entry(1,1,"write")).then(heal).then(step_down(1,1,5))
        .expect(election_safety and group_state.get((1,1)).role == "follower")
    run new_leader_after_heal_test = init.then(partition(1,3))
        .then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .then(append_entry(1,1,"write")).then(heal).then(replicate(1,3,1))
        .then(step_down(1,1,5)).then(step_down(3,1,5))
        .then(request_vote(3,1)).then(deliver_vote(2,3,1)).then(receive_vote(2,3,1)).then(win_election(3,1))
        .expect(election_safety and log_matching and group_state.get((3,1)).role == "leader")
    run partition_flap_test = init.then(partition(1,2)).then(heal).then(partition(1,3)).then(heal)
        .then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .expect(election_safety and partitioned.size() == 0)
    run appends_during_partition_test = init.then(partition(1,3))
        .then(request_vote(1,1)).then(deliver_vote(2,1,1)).then(receive_vote(2,1,1)).then(win_election(1,1))
        .then(append_entry(1,1,"first")).then(append_entry(1,1,"second")).then(replicate(1,2,1))
        .expect(election_safety and log_matching and group_state.get((3,1)).log.length() == 0 and group_state.get((2,1)).log.length() == 2)
}

module crash_transaction_test {
    import types.* from "../specs/common/types"

    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val MAX_TX: int = 3

    // Combined DTX + crash state
    var tx_state: TxId -> str
    var votes: TxId -> (NodeId -> str)
    var next_tx: TxId
    var alive_nodes: Set[NodeId]
    var crashed: Set[NodeId]
    // Persistent state: votes that survive crash (written to WAL)
    var terminal_decision: TxId -> str
    var durable_votes: TxId -> (NodeId -> str)

    action init = all {
        tx_state' = 1.to(MAX_TX).mapBy(t => "pending"),
        votes' = 1.to(MAX_TX).mapBy(t => NODES.mapBy(n => "unknown")),
        next_tx' = 1,
        alive_nodes' = Set(1, 2, 3),
        crashed' = Set(),
        terminal_decision' = 1.to(MAX_TX).mapBy(t => "unknown"),
        durable_votes' = 1.to(MAX_TX).mapBy(t => NODES.mapBy(n => "unknown")),
    }

    // --- Transaction actions ---

    action begin_tx: bool = {
        val tid = next_tx
        all {
        next_tx <= MAX_TX,
        tx_state' = tx_state.set(tid, "pending"),
        votes' = votes.set(tid, NODES.mapBy(n => "unknown")),
        next_tx' = next_tx + 1,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
        terminal_decision' = terminal_decision,
        durable_votes' = durable_votes,
            }
    }

    // Only alive nodes can vote
    action vote_prepare(tid: TxId, node: NodeId): bool = all {
        tid < next_tx,
        node.in(NODES),
        node.in(alive_nodes),  // must be alive
        tx_state.get(tid) == "pending",
        votes.get(tid).get(node) == "unknown",
        votes' = votes.set(tid, votes.get(tid).set(node, "prepared")),
        // Write to durable storage
        terminal_decision' = terminal_decision,
        durable_votes' = durable_votes.set(tid,
            durable_votes.get(tid).set(node, "prepared")),
        tx_state' = tx_state,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
    }

    action vote_abort(tid: TxId, node: NodeId): bool = all {
        tid < next_tx,
        node.in(NODES),
        node.in(alive_nodes),
        tx_state.get(tid) == "pending",
        votes.get(tid).get(node) == "unknown",
        votes' = votes.set(tid, votes.get(tid).set(node, "aborted")),
        terminal_decision' = terminal_decision,
        durable_votes' = durable_votes.set(tid,
            durable_votes.get(tid).set(node, "aborted")),
        tx_state' = tx_state,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
    }

    action commit_tx(tid: TxId): bool = all {
        tid < next_tx,
        tx_state.get(tid) == "pending",
        NODES.forall(n => votes.get(tid).get(n) == "prepared"),
        tx_state' = tx_state.set(tid, "committed"),
        votes' = votes,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
        terminal_decision' = terminal_decision.set(tid, "committed"),
        durable_votes' = durable_votes,
    }

    action abort_tx(tid: TxId): bool = all {
        tid < next_tx,
        tx_state.get(tid) == "pending",
        NODES.exists(n => durable_votes.get(tid).get(n) == "aborted"),
        tx_state' = tx_state.set(tid, "aborted"),
        votes' = votes,
        next_tx' = next_tx,
        alive_nodes' = alive_nodes,
        crashed' = crashed,
        terminal_decision' = terminal_decision.set(tid, "aborted"),
        durable_votes' = durable_votes,
    }

    // --- Crash/recovery actions ---

    action crash_node(node: NodeId): bool = all {
        node.in(alive_nodes),
        alive_nodes' = alive_nodes.exclude(Set(node)),
        crashed' = crashed.union(Set(node)),
        // In-memory votes are lost, but durable_votes persist
        // Reset in-memory votes for crashed node to "unknown"
        votes' = 1.to(MAX_TX).mapBy(t =>
            if (t >= 1 and t < next_tx)
                votes.get(t).set(node, "unknown")
            else votes.get(t)),
        tx_state' = tx_state,
        next_tx' = next_tx,
        terminal_decision' = terminal_decision,
        durable_votes' = durable_votes,
    }

    action recover_node(node: NodeId): bool = all {
        node.in(crashed),
        alive_nodes' = alive_nodes.union(Set(node)),
        crashed' = crashed.exclude(Set(node)),
        // Recover votes from durable storage
        votes' = 1.to(MAX_TX).mapBy(t =>
            if (t >= 1 and t < next_tx)
                votes.get(t).set(node, durable_votes.get(t).get(node))
            else votes.get(t)),
        tx_state' = tx_state,
        next_tx' = next_tx,
        terminal_decision' = terminal_decision,
        durable_votes' = durable_votes,
    }

    // --- Safety invariants ---

    // Commit validity: committed => all participants prepared
    val commit_validity: bool =
        1.to(MAX_TX).forall(t =>
            tx_state.get(t) == "committed" implies
            NODES.forall(n => durable_votes.get(t).get(n) == "prepared")
        )

    // Independent coordinator decision history must remain reflected in terminal state.
    val decision_history_consistent: bool =
        1.to(MAX_TX).forall(t => terminal_decision.get(t) != "unknown" implies tx_state.get(t) == terminal_decision.get(t))

    // Crash partition: alive and crashed are disjoint
    val node_partition: bool =
        alive_nodes.intersect(crashed).size() == 0

    // Recovery preserves persistent state: durable votes survive crash
    val durable_state_preserved: bool =
        1.to(MAX_TX).forall(t =>
            NODES.forall(n =>
                durable_votes.get(t).get(n) != "unknown" implies
                (n.in(alive_nodes) implies votes.get(t).get(n) == durable_votes.get(t).get(n))
            )
        )

    // Crashed nodes do not vote: no new votes from crashed nodes
    // (enforced by alive_nodes guard in vote actions)
    val crashed_nodes_silent: bool =
        crashed.forall(n =>
            1.to(MAX_TX).forall(t =>
                t < next_tx implies
                // In-memory vote is unknown (lost on crash)
                votes.get(t).get(n) == "unknown" or
                // Or it was recovered from durable storage
                votes.get(t).get(n) == durable_votes.get(t).get(n)
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
            .expect(all {
                commit_validity,
                decision_history_consistent,
                node_partition,
                // tx 1 is still pending (cannot commit without all votes)
                (tx_state.get(1) == "pending"),
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
            .expect(all {
                commit_validity,
                decision_history_consistent,
                node_partition,
                // Tx 1 is committed and durable
                (tx_state.get(1) == "committed"),
                // Durable votes survive
                (durable_votes.get(1).get(2) == "prepared"),
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
            .expect(all {
                commit_validity,
                decision_history_consistent,
                durable_state_preserved,
                (tx_state.get(1) == "committed"),
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
            .expect(all {
                commit_validity,
                durable_state_preserved,
                // After recovery, node 1's vote is back
                (votes.get(1).get(1) == "prepared"),
                (durable_votes.get(1).get(1) == "prepared"),
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
            .expect(all {
                node_partition,
                crashed_nodes_silent,
                // Only node 1 is alive
                (alive_nodes == Set(1)),
                // Cannot commit — missing votes
                (tx_state.get(1) == "pending"),
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
            .expect(all {
                commit_validity,
                decision_history_consistent,
                (tx_state.get(1) == "aborted"),
                // Durable abort vote survives
                (durable_votes.get(1).get(2) == "aborted"),
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
            .expect(all {
                commit_validity,
                decision_history_consistent,
                durable_state_preserved,
                node_partition,
                (tx_state.get(1) == "committed"),
                (tx_state.get(2) == "committed"),
            })
    }
}

// =========================================================================
// 3. Combined faults — partitions + crashes simultaneously
// =========================================================================
module combined_faults_test {
    import multi_raft as raft from "../specs/nucleus/multi_raft"
    import distributed_tx as tx from "../specs/nucleus/distributed_tx"
    action init = all { raft::init, tx::init }
    action split = all { raft::partition(1,3), tx::unchanged }
    action heal = all { raft::heal, tx::unchanged }
    action crash(n:int): bool = all { raft::crash(n), tx::unchanged }
    action recover(n:int): bool = all { raft::recover(n), tx::unchanged }
    action campaign(n:int): bool = all { raft::request_vote(n,1), tx::unchanged }
    action vote(v:int,c:int): bool = all { raft::deliver_vote(v,c,1), tx::unchanged }
    // Delivering a request only queues the voter's response; the candidate
    // must receive it before the vote is counted (win_election needs 2).
    action count(v:int,c:int): bool = all { raft::receive_vote(v,c,1), tx::unchanged }
    action win(n:int): bool = all { raft::win_election(n,1), tx::unchanged }
    action begin = all { tx::begin_tx, raft::unchanged }
    action prepare(n:int): bool = all { n.in(raft::alive), raft::connected(1,n), tx::vote_prepare(1,n), raft::unchanged }
    action commit = all { tx::commit_tx(1), raft::unchanged }
    val safety = raft::election_safety and tx::commit_validity and tx::no_committed_abort
    run partition_and_crash_test = init.then(split).then(crash(3)).then(campaign(1)).then(vote(2,1)).then(count(2,1)).then(win(1))
        .expect(safety and not(raft::connected(1,3)))
    run tx_during_partition_and_crash_test = init.then(begin).then(prepare(1)).then(split).then(crash(3)).then(prepare(2))
        .expect(safety and tx::participant_votes.get(1).get(3) == "unknown")
    run recovery_after_combined_faults_test = init.then(begin).then(prepare(1)).then(split).then(crash(3))
        .then(recover(3)).then(heal).then(prepare(2)).then(prepare(3)).then(commit)
        .expect(safety and tx::tx_state.get(1) == "committed")
    run election_and_tx_across_faults_test = init.then(begin).then(split).then(crash(3))
        .then(campaign(1)).then(vote(2,1)).then(count(2,1)).then(win(1)).then(recover(3)).then(heal)
        .then(prepare(1)).then(prepare(2)).then(prepare(3)).then(commit)
        .expect(safety and raft::group_state.get((1,1)).role == "leader")
    run cascading_crash_recovery_test = init.then(begin).then(prepare(1)).then(prepare(2)).then(prepare(3))
        .then(crash(1)).then(crash(2)).then(crash(3)).then(recover(1)).then(recover(2)).then(recover(3)).then(commit)
        .expect(safety and tx::tx_state.get(1) == "committed")
    run adversarial_scenario_test = init.then(split).then(crash(3)).then(campaign(1)).then(vote(2,1)).then(count(2,1)).then(win(1))
        .then(begin).then(prepare(1)).then(prepare(2)).then(crash(2)).then(recover(2)).then(recover(3)).then(heal)
        .then(prepare(3)).then(commit).expect(safety and tx::tx_state.get(1) == "committed")
}
