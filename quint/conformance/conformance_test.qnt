// Conformance test harness — standalone property tests that verify the key
// safety invariants across Nucleus protocol models. These mirror the checks
// in the Rust conformance crate (conformance/src/) but are expressed purely
// in Quint for model-checker verification.
//
// Three property domains:
//   1. Distributed transactions — point-key serialization graph
//   2. Multi-Raft — at most one leader per term
//   3. Resharding — data is not lost during shard migration
module conformance_test {
    import types.* from "../specs/common/types"

    // =========================================================================
    // 1. Distributed Transactions — Point-key serializability conformance
    // =========================================================================

    // Replicate the dtx model inline with extra conformance-level invariants
    // that go beyond the basic spec invariants.

    pure val DTX_NODES: Set[NodeId] = Set(1, 2, 3)
    pure val DTX_MAX_TX: int = 4
    pure val DTX_IDS: Set[TxId] = 1.to(DTX_MAX_TX)
    pure val DTX_KEYS: Set[Key] = Set(1, 2)

    var dtx_reads_from: TxId -> (Key -> TxId)
    var dtx_latest_writer: Key -> TxId
    var dtx_rank: TxId -> int
    var dtx_snapshot_rank: TxId -> int

    var dtx_state: TxId -> str
    var dtx_votes: TxId -> (NodeId -> str)
    var dtx_outcomes: TxId -> (NodeId -> str)
    var dtx_next: TxId
    // Track committed transaction ordering for serializability check
    var dtx_commit_order: List[TxId]
    // Track which keys each transaction reads/writes for phantom detection
    var dtx_read_set: TxId -> Set[Key]
    var dtx_write_set: TxId -> Set[Key]

    action dtx_init = all {
        dtx_state' = DTX_IDS.mapBy(t => "pending"),
        dtx_votes' = DTX_IDS.mapBy(t => DTX_NODES.mapBy(n => "unknown")), dtx_outcomes' = DTX_IDS.mapBy(t => DTX_NODES.mapBy(n => "unknown")),
        dtx_reads_from' = DTX_IDS.mapBy(t => DTX_KEYS.mapBy(k => 0)),
        dtx_latest_writer' = DTX_KEYS.mapBy(k => 0),
        dtx_rank' = DTX_IDS.mapBy(t => 0), dtx_snapshot_rank' = DTX_IDS.mapBy(t => 0),
        dtx_next' = 1,
        dtx_commit_order' = List(),
        dtx_read_set' = DTX_IDS.mapBy(t => Set()),
        dtx_write_set' = DTX_IDS.mapBy(t => Set()),
    }

    action dtx_begin: bool = {
        val tid = dtx_next
        all {
            dtx_next <= DTX_MAX_TX,
            dtx_state' = dtx_state.set(tid, "pending"),
            dtx_outcomes' = dtx_outcomes, dtx_votes' = dtx_votes.set(tid, DTX_NODES.mapBy(n => "unknown")),
            dtx_reads_from' = dtx_reads_from.set(tid, dtx_latest_writer), dtx_snapshot_rank' = dtx_snapshot_rank.set(tid,dtx_commit_order.length()),
            dtx_latest_writer' = dtx_latest_writer,
            dtx_rank' = dtx_rank,
            dtx_next' = dtx_next + 1,
            dtx_commit_order' = dtx_commit_order,
            // Assign a read-write set to simulate key access
            dtx_read_set' = dtx_read_set.set(tid, DTX_KEYS),
            dtx_write_set' = dtx_write_set.set(tid, Set(1 + tid % 2)),
        }
    }

    action dtx_prepare(tid: TxId, node: NodeId): bool = all {
        tid < dtx_next,
        node.in(DTX_NODES),
        dtx_state.get(tid) == "pending",
        dtx_votes.get(tid).get(node) == "unknown",
        dtx_outcomes' = dtx_outcomes, dtx_votes' = dtx_votes.set(tid, dtx_votes.get(tid).set(node, "prepared")),
        dtx_state' = dtx_state,
        dtx_reads_from' = dtx_reads_from, dtx_snapshot_rank' = dtx_snapshot_rank,
        dtx_latest_writer' = dtx_latest_writer,
        dtx_rank' = dtx_rank,
        dtx_next' = dtx_next,
        dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    action dtx_vote_abort(tid: TxId, node: NodeId): bool = all {
        tid < dtx_next,
        node.in(DTX_NODES),
        dtx_state.get(tid) == "pending",
        dtx_votes.get(tid).get(node) == "unknown",
        dtx_outcomes' = dtx_outcomes, dtx_votes' = dtx_votes.set(tid, dtx_votes.get(tid).set(node, "aborted")),
        dtx_state' = dtx_state,
        dtx_reads_from' = dtx_reads_from, dtx_snapshot_rank' = dtx_snapshot_rank,
        dtx_latest_writer' = dtx_latest_writer,
        dtx_rank' = dtx_rank,
        dtx_next' = dtx_next,
        dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    action dtx_commit(tid: TxId): bool = all {
        tid < dtx_next,
        dtx_state.get(tid) == "pending",
        DTX_NODES.forall(n => dtx_votes.get(tid).get(n) == "prepared"),
        dtx_state' = dtx_state.set(tid, "committed"),
        dtx_commit_order' = dtx_commit_order.append(tid),
        dtx_outcomes' = dtx_outcomes, dtx_votes' = dtx_votes,
        dtx_reads_from' = dtx_reads_from, dtx_snapshot_rank' = dtx_snapshot_rank,
        dtx_latest_writer' = DTX_KEYS.mapBy(k => if (k.in(dtx_write_set.get(tid))) tid else dtx_latest_writer.get(k)),
        dtx_rank' = dtx_rank.set(tid, dtx_commit_order.length() + 1),
        dtx_next' = dtx_next,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    action dtx_abort(tid: TxId): bool = all {
        tid < dtx_next,
        dtx_state.get(tid) == "pending",
        DTX_NODES.exists(n => dtx_votes.get(tid).get(n) == "aborted"),
        dtx_state' = dtx_state.set(tid, "aborted"),
        dtx_outcomes' = dtx_outcomes, dtx_votes' = dtx_votes,
        dtx_reads_from' = dtx_reads_from, dtx_snapshot_rank' = dtx_snapshot_rank,
        dtx_latest_writer' = dtx_latest_writer,
        dtx_rank' = dtx_rank,
        dtx_next' = dtx_next,
        dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set,
        dtx_write_set' = dtx_write_set,
    }

    // --- Conformance invariant: Commit validity ---
    val dtx_commit_validity: bool =
        DTX_IDS.forall(t =>
            dtx_state.get(t) == "committed" implies
            DTX_NODES.forall(n => dtx_votes.get(t).get(n) == "prepared")
        )

    // --- Conformance invariant: Atomicity ---
    val dtx_atomicity: bool = DTX_IDS.forall(t => DTX_NODES.forall(n =>
        dtx_outcomes.get(t).get(n) == "unknown" or dtx_outcomes.get(t).get(n) == dtx_state.get(t))) and
        DTX_IDS.forall(t => DTX_NODES.forall(a => DTX_NODES.forall(b =>
            not(dtx_outcomes.get(t).get(a) == "committed" and dtx_outcomes.get(t).get(b) == "aborted"))))

    action dtx_deliver(tid:TxId,node:NodeId):bool = all {
        tid.in(DTX_IDS), node.in(DTX_NODES),
        dtx_state.get(tid) == "committed" or dtx_state.get(tid) == "aborted",
        dtx_outcomes.get(tid).get(node) == "unknown",
        dtx_outcomes' = dtx_outcomes.set(tid,dtx_outcomes.get(tid).set(node,dtx_state.get(tid))),
        dtx_state' = dtx_state, dtx_votes' = dtx_votes, dtx_reads_from' = dtx_reads_from,
        dtx_snapshot_rank' = dtx_snapshot_rank, dtx_latest_writer' = dtx_latest_writer, dtx_rank' = dtx_rank,
        dtx_next' = dtx_next, dtx_commit_order' = dtx_commit_order,
        dtx_read_set' = dtx_read_set, dtx_write_set' = dtx_write_set,
    }

    // Serialization graph over shared key snapshots: WW, WR and RW dependencies.
    // Point-key model only. Predicate reads/phantoms require a separate range model.
    val dtx_edges: Set[(TxId, TxId)] = tuples(DTX_IDS, DTX_IDS).filter(((a,b)) =>
        a != b and dtx_state.get(a) == "committed" and dtx_state.get(b) == "committed" and (
          (dtx_write_set.get(a).intersect(dtx_write_set.get(b)).size() > 0 and dtx_rank.get(a) < dtx_rank.get(b)) or
          DTX_KEYS.exists(k => k.in(dtx_read_set.get(b)) and dtx_reads_from.get(b).get(k) == a) or
          DTX_KEYS.exists(k => k.in(dtx_read_set.get(a)) and k.in(dtx_write_set.get(b)) and
            (dtx_reads_from.get(a).get(k) == 0 or dtx_rank.get(dtx_reads_from.get(a).get(k)) < dtx_rank.get(b)))
        ))
    val dtx_closure: Set[(TxId,TxId)] = DTX_IDS.fold(dtx_edges, (reach,k) =>
        reach.union(tuples(DTX_IDS,DTX_IDS).filter(((a,b)) => (a,k).in(reach) and (k,b).in(reach))))
    val dtx_serializability: bool = DTX_IDS.forall(t => not((t,t).in(dtx_closure)))
    val dtx_conflict_observed: bool = dtx_edges.size() > 0

    // --- Conformance invariant: No committed tx is later aborted ---
    val dtx_committed_stable: bool =
        dtx_commit_order.length() == DTX_IDS.filter(t => dtx_state.get(t) == "committed").size() and
        (dtx_commit_order.length() == 0 or 0.to(dtx_commit_order.length()-1).forall(i =>
            dtx_state.get(dtx_commit_order[i]) == "committed" and
            dtx_rank.get(dtx_commit_order[i]) == i+1))

    val dtx_valid_reads_from: bool = DTX_IDS.forall(t => t >= dtx_next or DTX_KEYS.forall(k => val writer = dtx_reads_from.get(t).get(k)
        val candidates = DTX_IDS.filter(w => dtx_state.get(w) == "committed" and
            k.in(dtx_write_set.get(w)) and dtx_rank.get(w) <= dtx_snapshot_rank.get(t))
        (writer == 0 and candidates.size() == 0) or
        (writer.in(candidates) and writer != t and dtx_rank.get(writer) > 0 and
            candidates.forall(w => dtx_rank.get(w) <= dtx_rank.get(writer)))))

    // Test: basic DTX conformance — commit flow preserves all invariants
    run dtx_conformance_commit_test = {
        dtx_init
            .then(dtx_begin)
            .then(dtx_prepare(1, 1))
            .then(dtx_prepare(1, 2))
            .then(dtx_prepare(1, 3))
            .then(dtx_commit(1)).then(dtx_deliver(1,1)).then(dtx_deliver(1,2))
            .expect(dtx_commit_validity and dtx_atomicity and
                    dtx_serializability and dtx_valid_reads_from and dtx_committed_stable)
    }

    // Test: abort preserves conformance invariants
    run dtx_conformance_abort_test = {
        dtx_init
            .then(dtx_begin)
            .then(dtx_prepare(1, 1))
            .then(dtx_vote_abort(1, 2))
            .then(dtx_abort(1))
            .expect(dtx_commit_validity and dtx_atomicity and
                    dtx_serializability and dtx_valid_reads_from and dtx_committed_stable)
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
            .expect(dtx_commit_validity and dtx_atomicity and
                    dtx_serializability and dtx_valid_reads_from and dtx_committed_stable)
    }

    // Test: serializability with two committed transactions
    run dtx_conformance_serial_order_test = {
        dtx_init
            .then(dtx_begin)   // tx 1
            .then(dtx_prepare(1, 1))
            .then(dtx_prepare(1, 2))
            .then(dtx_prepare(1, 3))
            .then(dtx_commit(1))
            .then(dtx_begin)   // tx 2 sees tx 1 committed writes
            .then(dtx_prepare(2, 1))
            .then(dtx_prepare(2, 2))
            .then(dtx_prepare(2, 3))
            .then(dtx_commit(2))
            .expect(dtx_commit_validity and dtx_atomicity and
                    dtx_serializability and dtx_conflict_observed and dtx_committed_stable and
                    // Both commits are recorded in order
                    dtx_commit_order.length() == 2 and
                    dtx_commit_order[0] == 1 and
                    dtx_commit_order[1] == 2)
    }

    // Negative control: overlapping snapshots commit write skew; graph must reject it.
    run dtx_write_skew_witness_test = {
        dtx_init.then(dtx_begin).then(dtx_begin)
          .then(dtx_prepare(1,1)).then(dtx_prepare(1,2)).then(dtx_prepare(1,3)).then(dtx_commit(1))
          .then(dtx_prepare(2,1)).then(dtx_prepare(2,2)).then(dtx_prepare(2,3)).then(dtx_commit(2))
          .expect(dtx_conflict_observed and not(dtx_serializability))
    }

    // Erasing the commit list previously hid an actual conflicting pair.
    run dtx_missing_conflict_pair_witness_test = dtx_init.then(dtx_begin)
        .then(dtx_prepare(1,1)).then(dtx_prepare(1,2)).then(dtx_prepare(1,3)).then(dtx_commit(1))
        .then(dtx_begin).then(dtx_prepare(2,1)).then(dtx_prepare(2,2)).then(dtx_prepare(2,3)).then(dtx_commit(2))
        .then(all { dtx_commit_order' = List(), dtx_state' = dtx_state, dtx_outcomes' = dtx_outcomes, dtx_votes' = dtx_votes,
            dtx_reads_from' = dtx_reads_from, dtx_snapshot_rank' = dtx_snapshot_rank, dtx_latest_writer' = dtx_latest_writer, dtx_rank' = dtx_rank,
            dtx_next' = dtx_next, dtx_read_set' = dtx_read_set, dtx_write_set' = dtx_write_set })
        .expect(dtx_conflict_observed and not(dtx_committed_stable))

    run dtx_mixed_delivered_outcomes_witness_test = dtx_init.then(dtx_begin)
        .then(dtx_prepare(1,1)).then(dtx_prepare(1,2)).then(dtx_prepare(1,3)).then(dtx_commit(1)).then(dtx_deliver(1,1))
        .then(all { dtx_outcomes' = dtx_outcomes.set(1,dtx_outcomes.get(1).set(2,"aborted")),
            dtx_state' = dtx_state, dtx_votes' = dtx_votes, dtx_reads_from' = dtx_reads_from,
            dtx_snapshot_rank' = dtx_snapshot_rank, dtx_latest_writer' = dtx_latest_writer, dtx_rank' = dtx_rank,
            dtx_next' = dtx_next, dtx_commit_order' = dtx_commit_order,
            dtx_read_set' = dtx_read_set, dtx_write_set' = dtx_write_set }).expect(not(dtx_atomicity))

    // =========================================================================
    // 2. Concrete bounded Raft model. This is model conformance, not Rust refinement.
    import multi_raft as raft from "../specs/nucleus/multi_raft"
    run raft_conformance_single_election_test = raft::init
        .then(raft::request_vote(1,1)).then(raft::deliver_vote(2,1,1)).then(raft::receive_vote(2,1,1)).then(raft::win_election(1,1))
        .expect(raft::election_safety and raft::log_matching)
    run raft_conformance_multi_group_test = raft::init
        .then(raft::request_vote(1,1)).then(raft::deliver_vote(2,1,1)).then(raft::receive_vote(2,1,1)).then(raft::win_election(1,1))
        .then(raft::request_vote(2,2)).then(raft::deliver_vote(3,2,2)).then(raft::receive_vote(3,2,2)).then(raft::win_election(2,2))
        .expect(raft::election_safety and raft::log_matching)
    run raft_conformance_step_down_test = raft::init
        .then(raft::request_vote(1,1)).then(raft::deliver_vote(2,1,1)).then(raft::receive_vote(2,1,1)).then(raft::win_election(1,1))
        .then(raft::append_entry(1,1,"first")).then(raft::replicate(1,2,1))
        .then(raft::step_down(1,1,5)).then(raft::step_down(2,1,5))
        .then(raft::request_vote(2,1)).then(raft::deliver_vote(1,2,1)).then(raft::receive_vote(1,2,1)).then(raft::win_election(2,1))
        .expect(raft::election_safety and raft::log_matching)
    run raft_conformance_succession_test = raft::init
        .then(raft::request_vote(1,1)).then(raft::deliver_vote(2,1,1)).then(raft::receive_vote(2,1,1)).then(raft::win_election(1,1))
        .then(raft::append_entry(1,1,"first")).then(raft::append_entry(1,1,"second"))
        .then(raft::replicate(1,2,1)).then(raft::step_down(1,1,3)).then(raft::step_down(2,1,3))
        .then(raft::request_vote(2,1)).then(raft::deliver_vote(1,2,1)).then(raft::receive_vote(1,2,1)).then(raft::win_election(2,1))
        .then(raft::append_entry(2,1,"third")).then(raft::replicate(2,3,1))
        .then(raft::step_down(2,1,7)).then(raft::step_down(3,1,7))
        .then(raft::request_vote(3,1)).then(raft::deliver_vote(2,3,1)).then(raft::receive_vote(2,3,1)).then(raft::win_election(3,1))
        .expect(raft::election_safety and raft::log_matching and raft::group_state.get((3,1)).role == "leader")

    // 3. Resharding — Data conservation conformance
    // =========================================================================

    pure val SHARD_IDS: Set[ShardId] = Set(1, 2, 3, 4)
    pure val SHARD_KEYS: Set[Key] = 1.to(100)

    var shard_data: ShardId -> Set[Key]
    var rs_phase: str
    var rs_source: ShardId
    var rs_target: ShardId

    action rs_init = all {
        shard_data' = SHARD_IDS.mapBy(s =>
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
        shard_data.get(src).size() > 0,
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
        k.in(shard_data.get(rs_source)),
        shard_data' = shard_data
            .set(rs_source, shard_data.get(rs_source).exclude(Set(k)))
            .set(rs_target, shard_data.get(rs_target).union(Set(k))),
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
            SHARD_IDS.exists(s => k.in(shard_data.get(s)))
        )

    // Conformance invariant: no double ownership
    val rs_no_double_ownership: bool =
        SHARD_KEYS.forall(k =>
            SHARD_IDS.filter(s => k.in(shard_data.get(s))).size() <= 1
        )

    // Conformance invariant: total key count is conserved
    val rs_key_conservation: bool =
        SHARD_IDS.fold(0, (acc, s) => acc + shard_data.get(s).size()) == SHARD_KEYS.size()

    // Conformance invariant: during copying, source + target covers
    // all migrating keys (no keys vanish mid-migration)
    val rs_migration_coverage: bool =
        rs_phase != "copying" or
        SHARD_KEYS.forall(k =>
            SHARD_IDS.exists(s => k.in(shard_data.get(s)))
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
            .expect(rs_no_data_loss and rs_no_double_ownership and
                    rs_key_conservation and rs_migration_coverage)
    }

    // Test: mid-migration invariants hold
    run rs_conformance_mid_migration_test = {
        rs_init
            .then(rs_begin(1, 2))
            .then(rs_start_copy)
            .then(rs_copy_key(5))
            .then(rs_copy_key(10))
            // Invariants hold even mid-copy
            .expect(rs_no_data_loss and rs_no_double_ownership and
                    rs_key_conservation and rs_migration_coverage)
    }

    // Test: resharding between non-adjacent shards
    run rs_conformance_cross_shard_test = {
        rs_init
            .then(rs_begin(1, 4))
            .then(rs_start_copy)
            .then(rs_copy_key(1))
            .then(rs_copy_key(25))
            .then(rs_finish)
            .expect(rs_no_data_loss and rs_no_double_ownership and
                    rs_key_conservation)
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
            .expect(rs_no_data_loss and rs_no_double_ownership and
                    rs_key_conservation)
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
            .expect(rs_no_data_loss and rs_key_conservation and
                    // Shard 2 now has extra keys
                    shard_data.get(2).size() == 35 and
                    // Shard 1 lost those keys
                    shard_data.get(1).size() == 15)
    }
}
