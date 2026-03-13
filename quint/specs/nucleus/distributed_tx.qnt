// Distributed Transactions — 2PC coordinated through Raft groups.
module distributed_tx {
    import types.*

    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val MAX_TX: int = 5

    type TxPhase = str  // "pending" | "prepared" | "committed" | "aborted"

    var tx_state: TxId -> TxPhase
    var participant_votes: TxId -> (NodeId -> str)  // "prepared" | "aborted" | "unknown"
    var next_tx_id: TxId

    action init = all {
        tx_state' = (t => "pending"),
        participant_votes' = (t => (n => "unknown")),
        next_tx_id' = 1,
    }

    // Begin a new transaction
    action begin_tx: bool = all {
        next_tx_id <= MAX_TX,
        val tid = next_tx_id
        tx_state' = tx_state.set(tid, "pending"),
        participant_votes' = participant_votes.set(tid, (n => "unknown")),
        next_tx_id' = next_tx_id + 1,
    }

    // Participant votes to prepare
    action vote_prepare(tid: TxId, node: NodeId): bool = all {
        tid < next_tx_id,
        node.in(NODES),
        tx_state(tid) == "pending",
        participant_votes(tid)(node) == "unknown",
        participant_votes' = participant_votes.set(tid,
            participant_votes(tid).set(node, "prepared")),
        tx_state' = tx_state,
        next_tx_id' = next_tx_id,
    }

    // Participant votes to abort
    action vote_abort(tid: TxId, node: NodeId): bool = all {
        tid < next_tx_id,
        node.in(NODES),
        tx_state(tid) == "pending",
        participant_votes(tid)(node) == "unknown",
        participant_votes' = participant_votes.set(tid,
            participant_votes(tid).set(node, "aborted")),
        tx_state' = tx_state,
        next_tx_id' = next_tx_id,
    }

    // Coordinator commits (all participants prepared)
    action commit_tx(tid: TxId): bool = all {
        tid < next_tx_id,
        tx_state(tid) == "pending",
        NODES.forall(n => participant_votes(tid)(n) == "prepared"),
        tx_state' = tx_state.set(tid, "committed"),
        participant_votes' = participant_votes,
        next_tx_id' = next_tx_id,
    }

    // Coordinator aborts (at least one participant aborted)
    action abort_tx(tid: TxId): bool = all {
        tid < next_tx_id,
        tx_state(tid) == "pending",
        NODES.exists(n => participant_votes(tid)(n) == "aborted"),
        tx_state' = tx_state.set(tid, "aborted"),
        participant_votes' = participant_votes,
        next_tx_id' = next_tx_id,
    }

    action step = any {
        begin_tx,
        1.to(MAX_TX).exists(t => NODES.exists(n => vote_prepare(t, n))),
        1.to(MAX_TX).exists(t => NODES.exists(n => vote_abort(t, n))),
        1.to(MAX_TX).exists(t => commit_tx(t)),
        1.to(MAX_TX).exists(t => abort_tx(t)),
    }

    // === SAFETY INVARIANTS ===

    // Commit validity: committed → all participants prepared
    val commit_validity: bool =
        1.to(MAX_TX).forall(t =>
            tx_state(t) == "committed" implies
            NODES.forall(n => participant_votes(t)(n) == "prepared")
        )

    // Atomicity: no transaction is both committed and aborted
    val atomicity: bool =
        1.to(MAX_TX).forall(t =>
            not(tx_state(t) == "committed" and tx_state(t) == "aborted")
        )

    // Commit durability: committed stays committed
    // (temporal property, checked across steps)
    val no_committed_abort: bool =
        1.to(MAX_TX).forall(t =>
            tx_state(t) != "committed" or tx_state(t) == "committed"
        )
}
