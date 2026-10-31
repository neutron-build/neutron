// Distributed Transactions — 2PC coordinated through Raft groups.
module distributed_tx {
    import types.* from "../common/types"

    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val MAX_TX: int = 5
    pure val TX_IDS: Set[TxId] = 1.to(MAX_TX)

    type TxPhase = str  // "pending" | "prepared" | "committed" | "aborted"

    var tx_state: TxId -> TxPhase
    var participant_votes: TxId -> (NodeId -> str)  // "prepared" | "aborted" | "unknown"
    var next_tx_id: TxId

    var ever_committed: Set[TxId]
    var decisions: TxId -> (NodeId -> str)
    action init = all {
        tx_state' = TX_IDS.mapBy(t => "pending"),
        participant_votes' = TX_IDS.mapBy(t => NODES.mapBy(n => "unknown")),
        ever_committed' = Set(),
        decisions' = TX_IDS.mapBy(t => NODES.mapBy(n => "unknown")),
        next_tx_id' = 1,
    }

    // Begin a new transaction
    action begin_tx: bool = {
        val tid = next_tx_id
        all {
            next_tx_id <= MAX_TX,
            tx_state' = tx_state.set(tid, "pending"),
            participant_votes' = participant_votes.set(tid, NODES.mapBy(n => "unknown")),
            ever_committed' = ever_committed,
            decisions' = decisions,
            next_tx_id' = next_tx_id + 1,
        }
    }

    // Participant votes to prepare
    action vote_prepare(tid: TxId, node: NodeId): bool = all {
        tid < next_tx_id,
        node.in(NODES),
        tx_state.get(tid) == "pending",
        participant_votes.get(tid).get(node) == "unknown",
        participant_votes' = participant_votes.set(tid,
            participant_votes.get(tid).set(node, "prepared")),
        tx_state' = tx_state,
        ever_committed' = ever_committed,
        decisions' = decisions,
        next_tx_id' = next_tx_id,
    }

    // Participant votes to abort
    action vote_abort(tid: TxId, node: NodeId): bool = all {
        tid < next_tx_id,
        node.in(NODES),
        tx_state.get(tid) == "pending",
        participant_votes.get(tid).get(node) == "unknown",
        participant_votes' = participant_votes.set(tid,
            participant_votes.get(tid).set(node, "aborted")),
        tx_state' = tx_state,
        ever_committed' = ever_committed,
        decisions' = decisions,
        next_tx_id' = next_tx_id,
    }

    // Coordinator commits (all participants prepared)
    action commit_tx(tid: TxId): bool = all {
        tid < next_tx_id,
        tx_state.get(tid) == "pending",
        NODES.forall(n => participant_votes.get(tid).get(n) == "prepared"),
        tx_state' = tx_state.set(tid, "committed"),
        participant_votes' = participant_votes,
        ever_committed' = ever_committed.union(Set(tid)),
        decisions' = decisions,
        next_tx_id' = next_tx_id,
    }

    // Coordinator aborts (at least one participant aborted)
    action abort_tx(tid: TxId): bool = all {
        tid < next_tx_id,
        tx_state.get(tid) == "pending",
        NODES.exists(n => participant_votes.get(tid).get(n) == "aborted"),
        tx_state' = tx_state.set(tid, "aborted"),
        participant_votes' = participant_votes,
        ever_committed' = ever_committed,
        decisions' = decisions,
        next_tx_id' = next_tx_id,
    }

    action step = any {
        { nondet t = TX_IDS.oneOf()
          nondet n = NODES.oneOf()
          deliver_decision(t,n) },
        begin_tx,
        { nondet t = TX_IDS.oneOf()
          nondet n = NODES.oneOf()
          vote_prepare(t, n) },
        { nondet t = TX_IDS.oneOf()
          nondet n = NODES.oneOf()
          vote_abort(t, n) },
        { nondet t = TX_IDS.oneOf()
          commit_tx(t) },
        { nondet t = TX_IDS.oneOf()
          abort_tx(t) },
    }

    // === SAFETY INVARIANTS ===

    // Commit validity: committed → all participants prepared
    val commit_validity: bool =
        TX_IDS.forall(t =>
            tx_state.get(t) == "committed" implies
            NODES.forall(n => participant_votes.get(t).get(n) == "prepared")
        )

    // Delivered participant decisions may lag, but may never disagree.
    action deliver_decision(tid: TxId, node: NodeId): bool = all {
        tid.in(TX_IDS), node.in(NODES),
        tx_state.get(tid) == "committed" or tx_state.get(tid) == "aborted",
        decisions.get(tid).get(node) == "unknown",
        decisions' = decisions.set(tid, decisions.get(tid).set(node, tx_state.get(tid))),
        ever_committed' = ever_committed,
        tx_state' = tx_state, participant_votes' = participant_votes, next_tx_id' = next_tx_id,
    }
    val atomicity: bool = TX_IDS.forall(t => NODES.forall(a => NODES.forall(b =>
        not(decisions.get(t).get(a) == "committed" and decisions.get(t).get(b) == "aborted"))))
    val no_committed_abort: bool = ever_committed.forall(t => tx_state.get(t) == "committed")


    action unchanged = all { tx_state' = tx_state, participant_votes' = participant_votes, next_tx_id' = next_tx_id, ever_committed' = ever_committed, decisions' = decisions }
}
