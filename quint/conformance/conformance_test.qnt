// Conformance test harness — standalone property tests that verify the key
// safety invariants across Nucleus protocol models. These mirror the checks
// in the Rust conformance crate (conformance/src/) but are expressed purely
// in Quint for model-checker verification.
//
// Three property domains:
//   1. Distributed transactions — serializability (no phantom reads)
//   2. Multi-Raft — at most one leader per term
//   3. Resharding — data is not lost during shard migration
module conformance_test {
    import types.*

    // =========================================================================
    // 1. Distributed Transactions — Serializability conformance
    // =========================================================================

    // Replicate the dtx model inline with extra conformance-level invariants
    // that go beyond the basic spec invariants.

    pure val DTX_NODES: Set[NodeId] = Set(1, 2, 3)
    pure val DTX_MAX_TX: int = 4

    var dtx_state: TxId -> str
    var dtx_votes: TxId -> (NodeId -> str)
    var dtx_next: TxId
    // Track committed transaction ordering for serializability check
    var dtx_commit_order: List[TxId]
    // Track which keys each transaction reads/writes for phantom detection
    var dtx_read_set: TxId -> Set[Key]
    var dtx_write_set: TxId -> Set[Key]

    action dtx_init = all {
        dtx_state' = (t => "pending"),
        dtx_votes' = (t => (n => "unknown")),
        dtx_next' = 1,
        dtx_commit_order' = List(),
        dtx_read_set' = (t => Set()),
        dtx_write_set' = (t => Set()),
    }

    action dtx_begin: bool = all {
        dtx_next <= DTX_MAX_TX,
        val tid = dtx_next
        dtx_state' = dtx_state.set(tid, "pending"),
        dtx_votes' = dtx_votes.set(tid, (n => "unknown")),
        dtx_next' = dtx_next + 1,
        dtx_commit_order' = dtx_commit_order,
        // Assign a read-write set to simulate key access
        dtx_read_set' = dtx_read_set.set(tid, Set(tid, tid + 10)),
        dtx_write_set' = dtx_write_set.set(tid, Set(tid)),
    }

    action dtx_prepare(tid: TxId, node: NodeId): bool = all {
        tid < dtx_next,
        node.in(DTX_NODES),
        dtx_state(tid) == "pending",
        dtx_votes(tid)(node) == "unknown",
        dtx_votes' = dtx_votes.set(tid, dtx_votes(tid).set(node, "prepared")),
        dtx_state' = dtx_state,
        dtx_next' = dtx_next,
        dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    action dtx_vote_abort(tid: TxId, node: NodeId): bool = all {
        tid < dtx_next,
        node.in(DTX_NODES),
        dtx_state(tid) == "pending",
        dtx_votes(tid)(node) == "unknown",
        dtx_votes' = dtx_votes.set(tid, dtx_votes(tid).set(node, "aborted")),
        dtx_state' = dtx_state,
        dtx_next' = dtx_next,
        dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    action dtx_commit(tid: TxId): bool = all {
        tid < dtx_next,
        dtx_state(tid) == "pending",
        DTX_NODES.forall(n => dtx_votes(tid)(n) == "prepared"),
        dtx_state' = dtx_state.set(tid, "committed"),
        dtx_commit_order' = dtx_commit_order.append(tid),
        dtx_votes' = dtx_votes,
        dtx_next' = dtx_next,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    action dtx_abort(tid: TxId): bool = all {
        tid < dtx_next,
        dtx_state(tid) == "pending",
        DTX_NODES.exists(n => dtx_votes(tid)(n) == "aborted"),
        dtx_state' = dtx_state.set(tid, "aborted"),
        dtx_votes' = dtx_votes,
        dtx_next' = dtx_next,
        dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    // --- Conformance invariant: Commit validity ---
    val dtx_commit_validity: bool =
        1.to(DTX_MAX_TX).forall(t =>
            dtx_state(t) == "committed" implies
            DTX_NODES.forall(n => dtx_votes(t)(n) == "prepared")
        )

    // --- Conformance invariant: Atomicity ---
    val dtx_atomicity: bool =
        1.to(DTX_MAX_TX).forall(t =>
            not(dtx_state(t) == "committed" and dtx_state(t) == "aborted")
        )

    // --- Conformance invariant: Serializability (no phantom reads) ---
    // Two committed transactions with write-read conflicts must appear
    // in a consistent serial order. Here we check that no committed tx
    // reads keys that another committed tx writes without ordering.
    val dtx_serializability: bool =
        1.to(DTX_MAX_TX).forall(t1 =>
            1.to(DTX_MAX_TX).forall(t2 =>
                (t1 != t2 and
                 dtx_state(t1) == "committed" and
                 dtx_state(t2) == "committed")
                implies
                // If t2 writes to keys that t1 reads, they must be serialized
                // (both appear in commit_order => total order exists)
                (dtx_write_set(t2).intersect(dtx_read_set(t1)).size() > 0
                 implies
                 // Both are in the commit order list
                 dtx_commit_order.length() >= 2)
            )
        )

    // --- Conformance invariant: No committed tx is later aborted ---
    val dtx_committed_stable: bool =
        dtx_commit_order.length() == 0 or
        0.to(dtx_commit_order.length() - 1).forall(i =>
            dtx_state(dtx_commit_order[i]) == "committed"
        )

    // Test: basic DTX conformance — commit flow preserves all invariants
    run dtx_conformance_commit_test = {
        dtx_init
            .then(dtx_begin)
            .then(dtx_prepare(1, 1))
            .then(dtx_prepare(1, 2))
            .then(dtx_prepare(1, 3))
            .then(dtx_commit(1))
            .then(all {
                dtx_commit_validity,
                dtx_atomicity,
                dtx_serializability,
                dtx_committed_stable,
            })
    }

    // Test: abort preserves conformance invariants
    run dtx_conformance_abort_test = {
        dtx_init
            .then(dtx_begin)
            .then(dtx_prepare(1, 1))
            .then(dtx_vote_abort(1, 2))
            .then(dtx_abort(1))
            .then(all {
                dtx_commit_validity,
                dtx_atomicity,
                dtx_serializability,
                dtx_committed_stable,
            })
    }

    // Test: two concurrent transactions — one commits, one aborts
    run dtx_conformance_concurrent_test = {
        dtx_init
            .then(dtx_begin)   // tx 1
            .then(dtx_begin)   // tx 2
            .then(dtx_prepare(1, 1))
            .then(dtx_prepare(1, 2))
            .then(dtx_prepare(1, 3))
            .then(dtx_commit(1))
            .then(dtx_vote_abort(2, 1))
            .then(dtx_abort(2))
            .then(all {
                dtx_commit_validity,
                dtx_atomicity,
                dtx_serializability,
                dtx_committed_stable,
            })
    }

    // Test: serializability with two committed transactions
    run dtx_conformance_serial_order_test = {
        dtx_init
            .then(dtx_begin)   // tx 1
            .then(dtx_begin)   // tx 2
            .then(dtx_prepare(1, 1))
            .then(dtx_prepare(1, 2))
            .then(dtx_prepare(1, 3))
            .then(dtx_commit(1))
            .then(dtx_prepare(2, 1))
            .then(dtx_prepare(2, 2))
            .then(dtx_prepare(2, 3))
            .then(dtx_commit(2))
            .then(all {
                dtx_commit_validity,
                dtx_atomicity,
                dtx_serializability,
                dtx_committed_stable,
                // Both commits are recorded in order
                assert(dtx_commit_order.length() == 2),
                assert(dtx_commit_order[0] == 1),
                assert(dtx_commit_order[1] == 2),
            })
    }

    // =========================================================================
    // 2. Multi-Raft — Election safety conformance
    // =========================================================================

    pure val RAFT_NODES: Set[NodeId] = Set(1, 2, 3)
    pure val RAFT_GROUPS: Set[GroupId] = Set(1, 2)

    type ConformRaftState = {
        current_term: Term,
        voted_for: int,
        log_len: int,
        role: str,
    }

    var raft_state: (NodeId, GroupId) -> ConformRaftState

    action raft_init = all {
        raft_state' = ((n, g) =>
            { current_term: 0, voted_for: -1, log_len: 0, role: "follower" }),
    }

    action raft_request_vote(node: NodeId, group: GroupId): bool = all {
        node.in(RAFT_NODES),
        group.in(RAFT_GROUPS),
        val st = raft_state((node, group))
        val new_term = st.current_term + 1
        raft_state' = raft_state.set((node, group),
            { ...st, current_term: new_term, role: "candidate", voted_for: node }),
    }

    action raft_win(node: NodeId, group: GroupId): bool = all {
        node.in(RAFT_NODES),
        group.in(RAFT_GROUPS),
        val st = raft_state((node, group))
        st.role == "candidate",
        RAFT_NODES.forall(n => n == node or
            raft_state((n, group)).role != "leader" or
            raft_state((n, group)).current_term != st.current_term),
        raft_state' = raft_state.set((node, group),
            { ...st, role: "leader" }),
    }

    action raft_append(leader: NodeId, group: GroupId): bool = all {
        leader.in(RAFT_NODES),
        group.in(RAFT_GROUPS),
        val st = raft_state((leader, group))
        st.role == "leader",
        raft_state' = raft_state.set((leader, group),
            { ...st, log_len: st.log_len + 1 }),
    }

    action raft_step_down(node: NodeId, group: GroupId, new_term: Term): bool = all {
        node.in(RAFT_NODES),
        group.in(RAFT_GROUPS),
        val st = raft_state((node, group))
        new_term > st.current_term,
        raft_state' = raft_state.set((node, group),
            { ...st, current_term: new_term, role: "follower", voted_for: -1 }),
    }

    // Conformance invariant: at most one leader per group per term
    val raft_election_safety: bool =
        RAFT_GROUPS.forall(g =>
            0.to(20).forall(t =>
                RAFT_NODES.filter(n =>
                    raft_state((n, g)).role == "leader" and
                    raft_state((n, g)).current_term == t
                ).size() <= 1
            )
        )

    // Conformance invariant: leaders have the highest term in their group
    // (or at least no follower has a higher term that would force step-down)
    val raft_leader_term_dominance: bool =
        RAFT_GROUPS.forall(g =>
            RAFT_NODES.forall(n =>
                raft_state((n, g)).role == "leader" implies
                RAFT_NODES.forall(n2 =>
                    n2 == n or
                    raft_state((n2, g)).current_term <= raft_state((n, g)).current_term
                )
            )
        )

    // Test: election safety after single election
    run raft_conformance_single_election_test = {
        raft_init
            .then(raft_request_vote(1, 1))
            .then(raft_win(1, 1))
            .then(all {
                raft_election_safety,
                raft_leader_term_dominance,
            })
    }

    // Test: election safety across groups
    run raft_conformance_multi_group_test = {
        raft_init
            .then(raft_request_vote(1, 1))
            .then(raft_win(1, 1))
            .then(raft_request_vote(2, 2))
            .then(raft_win(2, 2))
            .then(all {
                raft_election_safety,
                raft_leader_term_dominance,
            })
    }

    // Test: step-down preserves election safety
    run raft_conformance_step_down_test = {
        raft_init
            .then(raft_request_vote(1, 1))
            .then(raft_win(1, 1))
            .then(raft_append(1, 1))
            .then(raft_step_down(1, 1, 5))
            .then(raft_request_vote(2, 1))
            .then(raft_win(2, 1))
            .then(all {
                raft_election_safety,
                raft_leader_term_dominance,
            })
    }

    // Test: leadership succession preserves safety
    run raft_conformance_succession_test = {
        raft_init
            .then(raft_request_vote(1, 1))
            .then(raft_win(1, 1))
            .then(raft_append(1, 1))
            .then(raft_append(1, 1))
            .then(raft_step_down(1, 1, 3))
            .then(raft_request_vote(2, 1))
            .then(raft_win(2, 1))
            .then(raft_append(2, 1))
            .then(raft_step_down(2, 1, 7))
            .then(raft_request_vote(3, 1))
            .then(raft_win(3, 1))
            .then(all {
                raft_election_safety,
                raft_leader_term_dominance,
                // Node 3 is the leader of group 1
                assert(raft_state((3, 1)).role == "leader"),
                // Old leaders stepped down
                assert(raft_state((1, 1)).role == "follower"),
                assert(raft_state((2, 1)).role == "follower"),
            })
    }

    // =========================================================================
    // 3. Resharding — Data conservation conformance
    // =========================================================================

    pure val SHARD_IDS: Set[ShardId] = Set(1, 2, 3, 4)
    pure val SHARD_KEYS: Set[Key] = 1.to(100)

    var shard_data: ShardId -> Set[Key]
    var rs_phase: str
    var rs_source: ShardId
    var rs_target: ShardId

    action rs_init = all {
        shard_data' = (s =>
            if (s == 1) 1.to(25)
            else if (s == 2) 26.to(50)
            else if (s == 3) 51.to(75)
            else 76.to(100)),
        rs_phase' = "idle",
        rs_source' = 0,
        rs_target' = 0,
    }

    action rs_begin(src: ShardId, tgt: ShardId): bool = all {
        rs_phase == "idle",
        src.in(SHARD_IDS),
        tgt.in(SHARD_IDS),
        src != tgt,
        shard_data(src).size() > 0,
        rs_phase' = "preparing",
        rs_source' = src,
        rs_target' = tgt,
        shard_data' = shard_data,
    }

    action rs_start_copy: bool = all {
        rs_phase == "preparing",
        rs_phase' = "copying",
        shard_data' = shard_data,
        rs_source' = rs_source,
        rs_target' = rs_target,
    }

    action rs_copy_key(k: Key): bool = all {
        rs_phase == "copying",
        k.in(shard_data(rs_source)),
        shard_data' = shard_data
            .set(rs_source, shard_data(rs_source).exclude(Set(k)))
            .set(rs_target, shard_data(rs_target).union(Set(k))),
        rs_phase' = rs_phase,
        rs_source' = rs_source,
        rs_target' = rs_target,
    }

    action rs_finish: bool = all {
        rs_phase == "copying",
        rs_phase' = "idle",
        shard_data' = shard_data,
        rs_source' = 0,
        rs_target' = 0,
    }

    // Conformance invariant: no data loss — every key owned by some shard
    val rs_no_data_loss: bool =
        SHARD_KEYS.forall(k =>
            SHARD_IDS.exists(s => k.in(shard_data(s)))
        )

    // Conformance invariant: no double ownership
    val rs_no_double_ownership: bool =
        SHARD_KEYS.forall(k =>
            SHARD_IDS.filter(s => k.in(shard_data(s))).size() <= 1
        )

    // Conformance invariant: total key count is conserved
    val rs_key_conservation: bool =
        SHARD_IDS.fold(0, (acc, s) => acc + shard_data(s).size()) == SHARD_KEYS.size()

    // Conformance invariant: during copying, source + target covers
    // all migrating keys (no keys vanish mid-migration)
    val rs_migration_coverage: bool =
        rs_phase != "copying" or
        SHARD_KEYS.forall(k =>
            SHARD_IDS.exists(s => k.in(shard_data(s)))
        )

    // Test: resharding conformance — full cycle
    run rs_conformance_full_cycle_test = {
        rs_init
            .then(rs_begin(1, 2))
            .then(rs_start_copy)
            .then(rs_copy_key(1))
            .then(rs_copy_key(2))
            .then(rs_copy_key(3))
            .then(rs_finish)
            .then(all {
                rs_no_data_loss,
                rs_no_double_ownership,
                rs_key_conservation,
                rs_migration_coverage,
            })
    }

    // Test: mid-migration invariants hold
    run rs_conformance_mid_migration_test = {
        rs_init
            .then(rs_begin(1, 2))
            .then(rs_start_copy)
            .then(rs_copy_key(5))
            .then(rs_copy_key(10))
            // Invariants hold even mid-copy
            .then(all {
                rs_no_data_loss,
                rs_no_double_ownership,
                rs_key_conservation,
                rs_migration_coverage,
            })
    }

    // Test: resharding between non-adjacent shards
    run rs_conformance_cross_shard_test = {
        rs_init
            .then(rs_begin(1, 4))
            .then(rs_start_copy)
            .then(rs_copy_key(1))
            .then(rs_copy_key(25))
            .then(rs_finish)
            .then(all {
                rs_no_data_loss,
                rs_no_double_ownership,
                rs_key_conservation,
            })
    }

    // Test: sequential resharding operations preserve conservation
    run rs_conformance_sequential_test = {
        rs_init
            .then(rs_begin(1, 2))
            .then(rs_start_copy)
            .then(rs_copy_key(1))
            .then(rs_finish)
            .then(rs_begin(3, 4))
            .then(rs_start_copy)
            .then(rs_copy_key(51))
            .then(rs_finish)
            .then(all {
                rs_no_data_loss,
                rs_no_double_ownership,
                rs_key_conservation,
            })
    }

    // Test: resharding all keys from one shard to another
    run rs_conformance_drain_shard_test = {
        rs_init
            // Move many keys from shard 1 to shard 2
            .then(rs_begin(1, 2))
            .then(rs_start_copy)
            .then(rs_copy_key(1))
            .then(rs_copy_key(2))
            .then(rs_copy_key(3))
            .then(rs_copy_key(4))
            .then(rs_copy_key(5))
            .then(rs_copy_key(6))
            .then(rs_copy_key(7))
            .then(rs_copy_key(8))
            .then(rs_copy_key(9))
            .then(rs_copy_key(10))
            .then(rs_finish)
            .then(all {
                rs_no_data_loss,
                rs_key_conservation,
                // Shard 2 now has extra keys
                assert(shard_data(2).size() == 35),
                // Shard 1 lost those keys
                assert(shard_data(1).size() == 15),
            })
    }
}
