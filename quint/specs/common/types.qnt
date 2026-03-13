// Common types shared across all Nucleus protocol specifications.
module types {
    type NodeId = int
    type GroupId = int
    type Term = int
    type LogIndex = int
    type ShardId = int
    type TxId = int
    type LSN = int
    type Key = int
    type Value = int

    type KeyRange = { start: Key, end_: Key }

    type LogEntry = {
        term: Term,
        index: LogIndex,
        command: str,
    }

    type Message = {
        from_: NodeId,
        to: NodeId,
        term: Term,
        payload: str,
    }
}
