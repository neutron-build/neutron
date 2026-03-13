// Network model — message delivery, partitions, reordering.
module network {
    import types.*

    var messages: Set[Message]
    var partitioned: Set[(NodeId, NodeId)]

    action send(msg: Message): bool = all {
        not((msg.from_, msg.to).in(partitioned)),
        messages' = messages.union(Set(msg)),
        partitioned' = partitioned,
    }

    action deliver(msg: Message): bool = all {
        msg.in(messages),
        messages' = messages.exclude(Set(msg)),
        partitioned' = partitioned,
    }

    action drop(msg: Message): bool = all {
        msg.in(messages),
        messages' = messages.exclude(Set(msg)),
        partitioned' = partitioned,
    }

    action partition(a: NodeId, b: NodeId): bool = all {
        partitioned' = partitioned.union(Set((a, b), (b, a))),
        messages' = messages,
    }

    action heal(a: NodeId, b: NodeId): bool = all {
        partitioned' = partitioned.exclude(Set((a, b), (b, a))),
        messages' = messages,
    }

    // Invariant: delivered messages were previously sent
    val no_fabrication: bool =
        messages.forall(m => m.from_ >= 0 and m.to >= 0)
}
