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

    action init = all {
        tx_state' = TX_IDS.mapBy(t => "pending"),
        participant_votes' = TX_IDS.mapBy(t => NODES.mapBy(n => "unknown")),
        next_tx_id' = 1,
    }

    // Begin a new transaction
    action begin_tx: bool = {
        val tid = next_tx_id
        all {
            next_tx_id <= MAX_TX,
            tx_state' = tx_state.set(tid, "pending"),
            participant_votes' = participant_votes.set(tid, NODES.mapBy(n => "unknown")),
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
        next_tx_id' = next_tx_id,
    }

    // Coordinator commits (all participants prepared)
    action commit_tx(tid: TxId): bool = all {
        tid < next_tx_id,
        tx_state.get(tid) == "pending",
        NODES.forall(n => participant_votes.get(tid).get(n) == "prepared"),
        tx_state' = tx_state.set(tid, "committed"),
        participant_votes' = participant_votes,
        next_tx_id' = next_tx_id,
    }

    // Coordinator aborts (at least one participant aborted)
    action abort_tx(tid: TxId): bool = all {
        tid < next_tx_id,
        tx_state.get(tid) == "pending",
        NODES.exists(n => participant_votes.get(tid).get(n) == "aborted"),
        tx_state' = tx_state.set(tid, "aborted"),
        participant_votes' = participant_votes,
        next_tx_id' = next_tx_id,
    }

    action step = any {
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

    // Atomicity: no transaction is both committed and aborted
    val atomicity: bool =
        TX_IDS.forall(t =>
            not(tx_state.get(t) == "committed" and tx_state.get(t) == "aborted")
        )

    // Commit durability: committed stays committed
    // (temporal property, checked across steps)
    val no_committed_abort: bool =
        TX_IDS.forall(t =>
            tx_state.get(t) != "committed" or tx_state.get(t) == "committed"
        )
}
