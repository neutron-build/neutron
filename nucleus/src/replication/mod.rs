//! Primary-replica streaming replication for 2-node deployments.
//!
//! When only two nodes exist, Raft consensus (which requires majority quorum)
//! is not practical. This module provides WAL-based streaming replication with
//! automatic failover instead.
//!
//! The primary appends all mutations to a write-ahead log and streams batches
//! of records to the replica. The replica applies them in order and confirms
//! receipt. A [`FailoverManager`] monitors heartbeats and can promote the
//! replica to primary when the original primary is unreachable.

use std::fmt;

use crate::tls::InternalTlsConfig;

// ---------------------------------------------------------------------------
// WAL types
// ---------------------------------------------------------------------------

/// Log Sequence Number -- monotonically increasing identifier for each WAL record.
pub type Lsn = u64;
/// Unique identifier for a node in the cluster.
pub type NodeId = u64;

/// A single entry in the write-ahead log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub timestamp_ms: u64,
    pub payload: WalPayload,
}

/// The contents of a WAL record. The WAL is type-agnostic: it stores raw page
/// writes rather than higher-level logical operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalPayload {
    /// Page-level write.
    PageWrite { page_id: u64, data: Vec<u8> },
    /// Transaction commit marker.
    Commit { txn_id: u64 },
    /// Transaction abort marker.
    Abort { txn_id: u64 },
    /// Checkpoint marker.
    Checkpoint,
}

impl WalPayload {
    /// Rough byte-size estimate used for lag tracking.
    fn estimated_size(&self) -> u64 {
        match self {
            WalPayload::PageWrite { data, .. } => 8 + data.len() as u64,
            WalPayload::Commit { .. } | WalPayload::Abort { .. } => 8,
            WalPayload::Checkpoint => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// WalWriter
// ---------------------------------------------------------------------------

/// Append-only WAL log. In production this would be backed by durable storage;
/// here we simulate durability via an explicit [`flush`](WalWriter::flush).
pub struct WalWriter {
    records: Vec<WalRecord>,
    next_lsn: Lsn,
    flushed_lsn: Lsn,
}

impl WalWriter {
    /// Create a new WAL writer. LSNs start at 1.
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            next_lsn: 1,
            flushed_lsn: 0,
        }
    }

    /// Append a new record and return its assigned LSN.
    pub fn append(&mut self, payload: WalPayload) -> Lsn {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        // Derive a monotonic timestamp from LSN for deterministic tests.
        self.records.push(WalRecord {
            lsn,
            timestamp_ms: lsn,
            payload,
        });
        lsn
    }

    /// Mark all written records as durably flushed.
    pub fn flush(&mut self) {
        if let Some(last) = self.records.last() {
            self.flushed_lsn = last.lsn;
        }
    }

    /// Return all records with `lsn > after_lsn`.
    pub fn records_since(&self, after_lsn: Lsn) -> &[WalRecord] {
        let start = self.records.partition_point(|r| r.lsn <= after_lsn);
        &self.records[start..]
    }

    /// The highest LSN written (not necessarily flushed).
    pub fn latest_lsn(&self) -> Lsn {
        self.records.last().map_or(0, |r| r.lsn)
    }

    /// The highest LSN that has been durably flushed.
    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn
    }

    /// Remove all records with `lsn < lsn` (WAL recycling).
    pub fn truncate_before(&mut self, lsn: Lsn) {
        let cutoff = self.records.partition_point(|r| r.lsn < lsn);
        self.records.drain(..cutoff);
    }

    /// Number of records currently held.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the WAL contains zero records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl Default for WalWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// ReplicationMode
// ---------------------------------------------------------------------------

/// Controls how the primary acknowledges writes relative to replica confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Wait for replica to confirm before acknowledging client (zero data loss).
    Synchronous,
    /// Acknowledge client immediately; stream to replica in background.
    Asynchronous,
}

// ---------------------------------------------------------------------------
// Node role and state
// ---------------------------------------------------------------------------

/// The role a node currently plays in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica,
    /// Single-node mode -- no replication.
    Standalone,
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Primary => write!(f, "Primary"),
            NodeRole::Replica => write!(f, "Replica"),
            NodeRole::Standalone => write!(f, "Standalone"),
        }
    }
}

/// Per-node replication bookkeeping.
pub struct ReplicaState {
    pub node_id: NodeId,
    pub role: NodeRole,
    pub applied_lsn: Lsn,
    pub received_lsn: Lsn,
    pub last_heartbeat_ms: u64,
    pub is_connected: bool,
    pub lag_bytes: u64,
}

impl ReplicaState {
    fn new(node_id: NodeId, role: NodeRole) -> Self {
        Self {
            node_id,
            role,
            applied_lsn: 0,
            received_lsn: 0,
            last_heartbeat_ms: 0,
            is_connected: false,
            lag_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// StreamStats
// ---------------------------------------------------------------------------

/// Cumulative statistics for a [`ReplicationStream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamStats {
    pub records_streamed: u64,
    pub bytes_streamed: u64,
    pub replica_confirmed_lsn: Lsn,
}

// ---------------------------------------------------------------------------
// ReplicationStream
// ---------------------------------------------------------------------------

/// Manages WAL streaming between a primary and its replica.
pub struct ReplicationStream {
    mode: ReplicationMode,
    primary_id: NodeId,
    replica_id: NodeId,
    replica_confirmed_lsn: Lsn,
    pending_batch: Vec<WalRecord>,
    batch_size: usize,
    records_streamed: u64,
    bytes_streamed: u64,
}

impl ReplicationStream {
    const DEFAULT_BATCH_SIZE: usize = 64;

    pub fn new(mode: ReplicationMode, primary_id: NodeId, replica_id: NodeId) -> Self {
        Self {
            mode,
            primary_id,
            replica_id,
            replica_confirmed_lsn: 0,
            pending_batch: Vec::new(),
            batch_size: Self::DEFAULT_BATCH_SIZE,
            records_streamed: 0,
            bytes_streamed: 0,
        }
    }

    /// Collect the next batch of WAL records the replica hasn't confirmed.
    /// Returns up to `batch_size` records with `lsn > replica_confirmed_lsn`.
    pub fn prepare_batch(&mut self, wal: &WalWriter) -> Vec<WalRecord> {
        let pending = wal.records_since(self.replica_confirmed_lsn);
        let take = pending.len().min(self.batch_size);
        let batch: Vec<WalRecord> = pending[..take].to_vec();
        self.records_streamed += batch.len() as u64;
        for rec in &batch {
            self.bytes_streamed += rec.payload.estimated_size();
        }
        self.pending_batch = batch.clone();
        batch
    }

    /// Record that the replica confirmed receiving all records up to `lsn`.
    pub fn confirm_received(&mut self, lsn: Lsn) {
        if lsn > self.replica_confirmed_lsn {
            self.replica_confirmed_lsn = lsn;
        }
    }

    /// How many LSNs the replica is behind the primary's latest WAL entry.
    pub fn replication_lag(&self, wal: &WalWriter) -> u64 {
        wal.latest_lsn().saturating_sub(self.replica_confirmed_lsn)
    }

    /// Whether the replica has confirmed every record currently in the WAL.
    pub fn is_caught_up(&self, wal: &WalWriter) -> bool {
        self.replica_confirmed_lsn >= wal.latest_lsn()
    }

    /// Cumulative streaming statistics.
    pub fn stats(&self) -> StreamStats {
        StreamStats {
            records_streamed: self.records_streamed,
            bytes_streamed: self.bytes_streamed,
            replica_confirmed_lsn: self.replica_confirmed_lsn,
        }
    }

    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }
    pub fn primary_id(&self) -> NodeId {
        self.primary_id
    }
    pub fn replica_id(&self) -> NodeId {
        self.replica_id
    }
}

// ---------------------------------------------------------------------------
// FailoverEvent / FailoverManager
// ---------------------------------------------------------------------------

/// Events produced by the failover state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverEvent {
    PrimaryDown { detected_at_ms: u64 },
    ReplicaPromoted { node_id: NodeId, at_lsn: Lsn },
    OldPrimaryRejoined { node_id: NodeId },
    ReplicationResumed { primary: NodeId, replica: NodeId },
}

/// Monitors heartbeats and manages failover decisions.
pub struct FailoverManager {
    local_node_id: NodeId,
    local_role: NodeRole,
    peer_node_id: Option<NodeId>,
    peer_last_seen_ms: u64,
    failover_timeout_ms: u64,
    history: Vec<FailoverEvent>,
    current_primary: Option<NodeId>,
    applied_lsn: Lsn,
}

impl FailoverManager {
    pub fn new(local_node_id: NodeId, role: NodeRole, failover_timeout_ms: u64) -> Self {
        let current_primary = match role {
            NodeRole::Primary => Some(local_node_id),
            _ => None,
        };
        Self {
            local_node_id,
            local_role: role,
            peer_node_id: None,
            peer_last_seen_ms: 0,
            failover_timeout_ms,
            history: Vec::new(),
            current_primary,
            applied_lsn: 0,
        }
    }

    /// Register the remote peer node.
    pub fn set_peer(&mut self, peer_id: NodeId) {
        self.peer_node_id = Some(peer_id);
    }

    /// Record a heartbeat from `from` at `timestamp_ms`.
    pub fn record_heartbeat(&mut self, from: NodeId, timestamp_ms: u64) {
        if self.peer_node_id == Some(from) && timestamp_ms > self.peer_last_seen_ms {
            self.peer_last_seen_ms = timestamp_ms;
        }
    }

    /// Check whether the peer has exceeded the failover timeout. If the local
    /// node is a Replica and the primary is unresponsive, returns `PrimaryDown`.
    pub fn check_failover(&mut self, now_ms: u64) -> Option<FailoverEvent> {
        let _peer = self.peer_node_id?;
        if self.local_role == NodeRole::Standalone {
            return None;
        }

        // Skip if we've never received a heartbeat — can't declare timeout
        // without a baseline. The peer may simply not have sent one yet.
        if self.peer_last_seen_ms == 0 {
            return None;
        }

        if now_ms.saturating_sub(self.peer_last_seen_ms) >= self.failover_timeout_ms
            && self.local_role == NodeRole::Replica {
                let evt = FailoverEvent::PrimaryDown {
                    detected_at_ms: now_ms,
                };
                self.history.push(evt.clone());
                return Some(evt);
            }
        None
    }

    /// Promote this node from Replica to Primary.
    pub fn promote_to_primary(&mut self) -> FailoverEvent {
        self.local_role = NodeRole::Primary;
        self.current_primary = Some(self.local_node_id);
        let evt = FailoverEvent::ReplicaPromoted {
            node_id: self.local_node_id,
            at_lsn: self.applied_lsn,
        };
        self.history.push(evt.clone());
        evt
    }

    /// Demote this node to Replica under `new_primary`.
    pub fn demote_to_replica(&mut self, new_primary: NodeId) -> FailoverEvent {
        self.local_role = NodeRole::Replica;
        self.current_primary = Some(new_primary);
        let evt = FailoverEvent::ReplicationResumed {
            primary: new_primary,
            replica: self.local_node_id,
        };
        self.history.push(evt.clone());
        evt
    }

    /// Old primary comes back online and rejoins as a replica.
    pub fn rejoin_as_replica(&mut self, primary_id: NodeId, primary_lsn: Lsn) -> FailoverEvent {
        self.local_role = NodeRole::Replica;
        self.current_primary = Some(primary_id);
        self.applied_lsn = primary_lsn;
        let evt = FailoverEvent::OldPrimaryRejoined {
            node_id: self.local_node_id,
        };
        self.history.push(evt.clone());
        evt
    }

    pub fn role(&self) -> NodeRole {
        self.local_role
    }
    pub fn current_primary(&self) -> Option<NodeId> {
        self.current_primary
    }
    pub fn history(&self) -> &[FailoverEvent] {
        &self.history
    }

    /// Update the locally-tracked applied LSN.
    pub fn set_applied_lsn(&mut self, lsn: Lsn) {
        self.applied_lsn = lsn;
    }
}

// ---------------------------------------------------------------------------
// ReplicationError
// ---------------------------------------------------------------------------

/// Errors produced by the replication subsystem.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplicationError {
    NotPrimary,
    NotReplica,
    StaleRecord { expected: Lsn, got: Lsn },
    NoPeer,
    ProtocolError(String),
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPrimary => write!(f, "operation requires Primary role"),
            Self::NotReplica => write!(f, "operation requires Replica role"),
            Self::StaleRecord { expected, got } => {
                write!(f, "stale record: expected LSN {expected}, got {got}")
            }
            Self::NoPeer => write!(f, "no peer configured"),
            Self::ProtocolError(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// ReplicationStatus
// ---------------------------------------------------------------------------

/// Snapshot of the current replication state for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationStatus {
    pub node_id: NodeId,
    pub role: NodeRole,
    pub wal_lsn: Lsn,
    pub applied_lsn: Lsn,
    pub replication_lag: u64,
    pub mode: ReplicationMode,
    pub peer_connected: bool,
}

// ---------------------------------------------------------------------------
// ReplicationManager
// ---------------------------------------------------------------------------

/// Top-level orchestrator tying together the WAL, streaming, and failover
/// components into a cohesive replication lifecycle.
pub struct ReplicationManager {
    wal: WalWriter,
    stream: Option<ReplicationStream>,
    failover: FailoverManager,
    mode: ReplicationMode,
    local_state: ReplicaState,
}

impl ReplicationManager {
    const DEFAULT_FAILOVER_TIMEOUT_MS: u64 = 5_000;

    /// Create a manager in Primary mode.
    pub fn new_primary(node_id: NodeId, mode: ReplicationMode) -> Self {
        Self {
            wal: WalWriter::new(),
            stream: None,
            failover: FailoverManager::new(
                node_id,
                NodeRole::Primary,
                Self::DEFAULT_FAILOVER_TIMEOUT_MS,
            ),
            mode,
            local_state: ReplicaState::new(node_id, NodeRole::Primary),
        }
    }

    /// Create a manager in Replica mode, replicating from `primary_id`.
    pub fn new_replica(node_id: NodeId, primary_id: NodeId) -> Self {
        let mut failover = FailoverManager::new(
            node_id,
            NodeRole::Replica,
            Self::DEFAULT_FAILOVER_TIMEOUT_MS,
        );
        failover.set_peer(primary_id);
        failover.set_applied_lsn(0);

        let stream = ReplicationStream::new(ReplicationMode::Asynchronous, primary_id, node_id);
        let mut state = ReplicaState::new(node_id, NodeRole::Replica);
        state.is_connected = true;

        Self {
            wal: WalWriter::new(),
            stream: Some(stream),
            failover,
            mode: ReplicationMode::Asynchronous,
            local_state: state,
        }
    }

    /// Create a manager in Standalone mode (single node, no replication).
    pub fn new_standalone(node_id: NodeId) -> Self {
        Self {
            wal: WalWriter::new(),
            stream: None,
            failover: FailoverManager::new(
                node_id,
                NodeRole::Standalone,
                Self::DEFAULT_FAILOVER_TIMEOUT_MS,
            ),
            mode: ReplicationMode::Asynchronous,
            local_state: ReplicaState::new(node_id, NodeRole::Standalone),
        }
    }

    /// Append a write to the WAL. Only primary or standalone may call this.
    pub fn write(&mut self, payload: WalPayload) -> Result<Lsn, ReplicationError> {
        match self.local_state.role {
            NodeRole::Primary | NodeRole::Standalone => {}
            NodeRole::Replica => return Err(ReplicationError::NotPrimary),
        }
        let lsn = self.wal.append(payload);
        self.wal.flush();
        self.local_state.applied_lsn = lsn;
        self.failover.set_applied_lsn(lsn);
        Ok(lsn)
    }

    /// (Primary) Prepare a batch of WAL records to send to the replica.
    pub fn prepare_replication_batch(&mut self) -> Vec<WalRecord> {
        match &mut self.stream {
            Some(stream) => stream.prepare_batch(&self.wal),
            None => Vec::new(),
        }
    }

    /// (Replica) Apply a batch of records received from the primary.
    /// Records must arrive in strict LSN order.
    pub fn apply_batch(&mut self, records: Vec<WalRecord>) -> Result<Lsn, ReplicationError> {
        if self.local_state.role != NodeRole::Replica {
            return Err(ReplicationError::NotReplica);
        }
        let mut last_applied: Lsn = self.local_state.applied_lsn;
        for rec in &records {
            let expected = last_applied + 1;
            if rec.lsn != expected {
                return Err(ReplicationError::StaleRecord {
                    expected,
                    got: rec.lsn,
                });
            }
            self.wal.append(rec.payload.clone());
            last_applied = rec.lsn;
        }
        self.wal.flush();
        self.local_state.applied_lsn = last_applied;
        self.local_state.received_lsn = last_applied;
        self.failover.set_applied_lsn(last_applied);
        Ok(last_applied)
    }

    /// (Primary) Record that the replica confirmed receiving up to `lsn`.
    pub fn confirm_replication(&mut self, lsn: Lsn) {
        if let Some(stream) = &mut self.stream {
            stream.confirm_received(lsn);
        }
    }

    /// Check health / failover state given the current timestamp.
    pub fn check_health(&mut self, now_ms: u64) -> Option<FailoverEvent> {
        self.failover.check_failover(now_ms)
    }

    /// Promote this replica to primary.
    pub fn promote(&mut self) -> Result<FailoverEvent, ReplicationError> {
        if self.local_state.role != NodeRole::Replica {
            return Err(ReplicationError::NotReplica);
        }
        let evt = self.failover.promote_to_primary();
        self.local_state.role = NodeRole::Primary;
        self.stream = None;
        Ok(evt)
    }

    /// Attach a replica to this primary, establishing a replication stream.
    pub fn attach_replica(&mut self, replica_id: NodeId) {
        self.failover.set_peer(replica_id);
        self.stream = Some(ReplicationStream::new(
            self.mode,
            self.local_state.node_id,
            replica_id,
        ));
        self.local_state.is_connected = true;
    }

    /// Build a diagnostic snapshot of the current replication state.
    pub fn status(&self) -> ReplicationStatus {
        let replication_lag = self
            .stream
            .as_ref()
            .map_or(0, |s| s.replication_lag(&self.wal));
        ReplicationStatus {
            node_id: self.local_state.node_id,
            role: self.local_state.role,
            wal_lsn: self.wal.latest_lsn(),
            applied_lsn: self.local_state.applied_lsn,
            replication_lag,
            mode: self.mode,
            peer_connected: self.local_state.is_connected,
        }
    }

    pub fn wal(&self) -> &WalWriter {
        &self.wal
    }
    pub fn wal_mut(&mut self) -> &mut WalWriter {
        &mut self.wal
    }
    pub fn failover(&self) -> &FailoverManager {
        &self.failover
    }
    pub fn failover_mut(&mut self) -> &mut FailoverManager {
        &mut self.failover
    }
    pub fn role(&self) -> NodeRole {
        self.local_state.role
    }

    /// Record a heartbeat from the peer at the given timestamp.
    pub fn record_heartbeat(&mut self, from: NodeId, timestamp_ms: u64) {
        self.failover.record_heartbeat(from, timestamp_ms);
    }

    /// Get the locally applied LSN.
    pub fn applied_lsn(&self) -> Lsn {
        self.local_state.applied_lsn
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.local_state.node_id
    }
}

// ---------------------------------------------------------------------------
// Streaming replication protocol
// ---------------------------------------------------------------------------

/// Messages exchanged over the streaming replication wire protocol.
///
/// These are higher-level than raw WAL records — they frame batches of
/// records with metadata needed for the replication state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamMessage {
    /// Primary → Replica: a batch of WAL records.
    WalBatch {
        /// The primary's node ID.
        sender_id: NodeId,
        /// Batch sequence number (monotonically increasing per stream).
        batch_seq: u64,
        /// The records in this batch.
        records: Vec<WalRecord>,
    },
    /// Replica → Primary: acknowledges receipt of records up to `confirmed_lsn`.
    WalBatchAck {
        /// The replica's node ID.
        sender_id: NodeId,
        /// The highest LSN the replica has durably applied.
        confirmed_lsn: Lsn,
        /// Batch sequence being acknowledged.
        batch_seq: u64,
    },
    /// Bidirectional heartbeat for liveness detection.
    ReplicationHeartbeat {
        sender_id: NodeId,
        role: NodeRole,
        current_lsn: Lsn,
        timestamp_ms: u64,
    },
    /// Replica → Primary: request to start streaming from a given LSN.
    StartStreaming { replica_id: NodeId, from_lsn: Lsn },
    /// Primary → Replica: confirms streaming has started.
    StreamingStarted {
        primary_id: NodeId,
        /// The primary's current WAL tip at the moment streaming begins.
        wal_tip_lsn: Lsn,
    },
}

// ---------------------------------------------------------------------------
// Binary serialization for StreamMessage
// ---------------------------------------------------------------------------

// Message tags
const SM_WAL_BATCH: u8 = 1;
const SM_WAL_BATCH_ACK: u8 = 2;
const SM_HEARTBEAT: u8 = 3;
const SM_START_STREAMING: u8 = 4;
const SM_STREAMING_STARTED: u8 = 5;

// WalPayload tags
const PL_PAGE_WRITE: u8 = 1;
const PL_COMMIT: u8 = 2;
const PL_ABORT: u8 = 3;
const PL_CHECKPOINT: u8 = 4;

// NodeRole tags
const ROLE_PRIMARY: u8 = 1;
const ROLE_REPLICA: u8 = 2;
const ROLE_STANDALONE: u8 = 3;

/// Serialize a `StreamMessage` into a framed byte vector.
///
/// Wire format: `[total_len: u32][tag: u8][...fields...]`
pub fn encode_stream_message(msg: &StreamMessage) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    // Reserve 4 bytes for total_len (filled in at the end)
    buf.extend_from_slice(&[0u8; 4]);

    match msg {
        StreamMessage::WalBatch {
            sender_id,
            batch_seq,
            records,
        } => {
            buf.push(SM_WAL_BATCH);
            buf.extend_from_slice(&sender_id.to_le_bytes());
            buf.extend_from_slice(&batch_seq.to_le_bytes());
            buf.extend_from_slice(&(records.len() as u32).to_le_bytes());
            for rec in records {
                encode_wal_record(&mut buf, rec);
            }
        }
        StreamMessage::WalBatchAck {
            sender_id,
            confirmed_lsn,
            batch_seq,
        } => {
            buf.push(SM_WAL_BATCH_ACK);
            buf.extend_from_slice(&sender_id.to_le_bytes());
            buf.extend_from_slice(&confirmed_lsn.to_le_bytes());
            buf.extend_from_slice(&batch_seq.to_le_bytes());
        }
        StreamMessage::ReplicationHeartbeat {
            sender_id,
            role,
            current_lsn,
            timestamp_ms,
        } => {
            buf.push(SM_HEARTBEAT);
            buf.extend_from_slice(&sender_id.to_le_bytes());
            buf.push(encode_role(*role));
            buf.extend_from_slice(&current_lsn.to_le_bytes());
            buf.extend_from_slice(&timestamp_ms.to_le_bytes());
        }
        StreamMessage::StartStreaming {
            replica_id,
            from_lsn,
        } => {
            buf.push(SM_START_STREAMING);
            buf.extend_from_slice(&replica_id.to_le_bytes());
            buf.extend_from_slice(&from_lsn.to_le_bytes());
        }
        StreamMessage::StreamingStarted {
            primary_id,
            wal_tip_lsn,
        } => {
            buf.push(SM_STREAMING_STARTED);
            buf.extend_from_slice(&primary_id.to_le_bytes());
            buf.extend_from_slice(&wal_tip_lsn.to_le_bytes());
        }
    }

    // Write total length (excluding the 4-byte length prefix itself)
    let total_len = (buf.len() - 4) as u32;
    buf[..4].copy_from_slice(&total_len.to_le_bytes());
    buf
}

/// Deserialize a `StreamMessage` from bytes (excluding the 4-byte length prefix).
pub fn decode_stream_message(data: &[u8]) -> Result<StreamMessage, ReplicationError> {
    if data.is_empty() {
        return Err(ReplicationError::ProtocolError("empty message".into()));
    }

    let tag = data[0];
    let rest = &data[1..];

    match tag {
        SM_WAL_BATCH => {
            if rest.len() < 20 {
                return Err(ReplicationError::ProtocolError("truncated WalBatch".into()));
            }
            let sender_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            let batch_seq = u64::from_le_bytes(rest[8..16].try_into().unwrap());
            let count = u32::from_le_bytes(rest[16..20].try_into().unwrap()) as usize;
            let mut pos = 20;
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                let (rec, consumed) = decode_wal_record(&rest[pos..])?;
                records.push(rec);
                pos += consumed;
            }
            Ok(StreamMessage::WalBatch {
                sender_id,
                batch_seq,
                records,
            })
        }
        SM_WAL_BATCH_ACK => {
            if rest.len() < 24 {
                return Err(ReplicationError::ProtocolError(
                    "truncated WalBatchAck".into(),
                ));
            }
            let sender_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            let confirmed_lsn = u64::from_le_bytes(rest[8..16].try_into().unwrap());
            let batch_seq = u64::from_le_bytes(rest[16..24].try_into().unwrap());
            Ok(StreamMessage::WalBatchAck {
                sender_id,
                confirmed_lsn,
                batch_seq,
            })
        }
        SM_HEARTBEAT => {
            if rest.len() < 25 {
                return Err(ReplicationError::ProtocolError(
                    "truncated Heartbeat".into(),
                ));
            }
            let sender_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            let role = decode_role(rest[8])?;
            let current_lsn = u64::from_le_bytes(rest[9..17].try_into().unwrap());
            let timestamp_ms = u64::from_le_bytes(rest[17..25].try_into().unwrap());
            Ok(StreamMessage::ReplicationHeartbeat {
                sender_id,
                role,
                current_lsn,
                timestamp_ms,
            })
        }
        SM_START_STREAMING => {
            if rest.len() < 16 {
                return Err(ReplicationError::ProtocolError(
                    "truncated StartStreaming".into(),
                ));
            }
            let replica_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            let from_lsn = u64::from_le_bytes(rest[8..16].try_into().unwrap());
            Ok(StreamMessage::StartStreaming {
                replica_id,
                from_lsn,
            })
        }
        SM_STREAMING_STARTED => {
            if rest.len() < 16 {
                return Err(ReplicationError::ProtocolError(
                    "truncated StreamingStarted".into(),
                ));
            }
            let primary_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            let wal_tip_lsn = u64::from_le_bytes(rest[8..16].try_into().unwrap());
            Ok(StreamMessage::StreamingStarted {
                primary_id,
                wal_tip_lsn,
            })
        }
        other => Err(ReplicationError::ProtocolError(format!(
            "unknown tag: {other}"
        ))),
    }
}

fn encode_wal_record(buf: &mut Vec<u8>, rec: &WalRecord) {
    buf.extend_from_slice(&rec.lsn.to_le_bytes());
    buf.extend_from_slice(&rec.timestamp_ms.to_le_bytes());
    match &rec.payload {
        WalPayload::PageWrite { page_id, data } => {
            buf.push(PL_PAGE_WRITE);
            buf.extend_from_slice(&page_id.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        }
        WalPayload::Commit { txn_id } => {
            buf.push(PL_COMMIT);
            buf.extend_from_slice(&txn_id.to_le_bytes());
        }
        WalPayload::Abort { txn_id } => {
            buf.push(PL_ABORT);
            buf.extend_from_slice(&txn_id.to_le_bytes());
        }
        WalPayload::Checkpoint => {
            buf.push(PL_CHECKPOINT);
        }
    }
}

fn decode_wal_record(data: &[u8]) -> Result<(WalRecord, usize), ReplicationError> {
    if data.len() < 17 {
        return Err(ReplicationError::ProtocolError(
            "truncated WalRecord".into(),
        ));
    }
    let lsn = u64::from_le_bytes(data[..8].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let payload_tag = data[16];
    let rest = &data[17..];

    let (payload, consumed) = match payload_tag {
        PL_PAGE_WRITE => {
            if rest.len() < 12 {
                return Err(ReplicationError::ProtocolError(
                    "truncated PageWrite".into(),
                ));
            }
            let page_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            let data_len = u32::from_le_bytes(rest[8..12].try_into().unwrap()) as usize;
            if rest.len() < 12 + data_len {
                return Err(ReplicationError::ProtocolError(
                    "truncated PageWrite data".into(),
                ));
            }
            let page_data = rest[12..12 + data_len].to_vec();
            (
                WalPayload::PageWrite {
                    page_id,
                    data: page_data,
                },
                12 + data_len,
            )
        }
        PL_COMMIT => {
            if rest.len() < 8 {
                return Err(ReplicationError::ProtocolError("truncated Commit".into()));
            }
            let txn_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            (WalPayload::Commit { txn_id }, 8)
        }
        PL_ABORT => {
            if rest.len() < 8 {
                return Err(ReplicationError::ProtocolError("truncated Abort".into()));
            }
            let txn_id = u64::from_le_bytes(rest[..8].try_into().unwrap());
            (WalPayload::Abort { txn_id }, 8)
        }
        PL_CHECKPOINT => (WalPayload::Checkpoint, 0),
        other => {
            return Err(ReplicationError::ProtocolError(format!(
                "unknown payload tag: {other}"
            )));
        }
    };

    Ok((
        WalRecord {
            lsn,
            timestamp_ms,
            payload,
        },
        17 + consumed,
    ))
}

fn encode_role(role: NodeRole) -> u8 {
    match role {
        NodeRole::Primary => ROLE_PRIMARY,
        NodeRole::Replica => ROLE_REPLICA,
        NodeRole::Standalone => ROLE_STANDALONE,
    }
}

fn decode_role(byte: u8) -> Result<NodeRole, ReplicationError> {
    match byte {
        ROLE_PRIMARY => Ok(NodeRole::Primary),
        ROLE_REPLICA => Ok(NodeRole::Replica),
        ROLE_STANDALONE => Ok(NodeRole::Standalone),
        other => Err(ReplicationError::ProtocolError(format!(
            "unknown role: {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// StreamingSender / StreamingReceiver
// ---------------------------------------------------------------------------

/// Tracks outbound WAL streaming from primary to replica.
///
/// Manages batch sequencing, outstanding acknowledgements, and
/// replication lag monitoring. The actual TCP I/O is handled externally;
/// this component produces/consumes `StreamMessage` values.
pub struct StreamingSender {
    primary_id: NodeId,
    #[allow(dead_code)]
    replica_id: NodeId,
    next_batch_seq: u64,
    last_confirmed_lsn: Lsn,
    last_sent_lsn: Lsn,
    bytes_sent: u64,
    batches_sent: u64,
    batches_acked: u64,
    mode: ReplicationMode,
}

impl StreamingSender {
    pub fn new(primary_id: NodeId, replica_id: NodeId, mode: ReplicationMode) -> Self {
        Self {
            primary_id,
            replica_id,
            next_batch_seq: 1,
            last_confirmed_lsn: 0,
            last_sent_lsn: 0,
            bytes_sent: 0,
            batches_sent: 0,
            batches_acked: 0,
            mode,
        }
    }

    /// Produce a `WalBatch` message from the given WAL, sending records
    /// after `last_confirmed_lsn` up to `max_batch_size` records.
    pub fn prepare_batch(
        &mut self,
        wal: &WalWriter,
        max_batch_size: usize,
    ) -> Option<StreamMessage> {
        let pending = wal.records_since(self.last_confirmed_lsn);
        if pending.is_empty() {
            return None;
        }
        let take = pending.len().min(max_batch_size);
        let records: Vec<WalRecord> = pending[..take].to_vec();

        if let Some(last) = records.last() {
            self.last_sent_lsn = last.lsn;
        }
        for rec in &records {
            self.bytes_sent += rec.payload.estimated_size();
        }

        let seq = self.next_batch_seq;
        self.next_batch_seq += 1;
        self.batches_sent += 1;

        Some(StreamMessage::WalBatch {
            sender_id: self.primary_id,
            batch_seq: seq,
            records,
        })
    }

    /// Process a `WalBatchAck` from the replica.
    pub fn handle_ack(&mut self, ack: &StreamMessage) {
        if let StreamMessage::WalBatchAck { confirmed_lsn, .. } = ack {
            if *confirmed_lsn > self.last_confirmed_lsn {
                self.last_confirmed_lsn = *confirmed_lsn;
            }
            self.batches_acked += 1;
        }
    }

    /// Build a heartbeat message from this primary.
    pub fn heartbeat(&self, current_lsn: Lsn, timestamp_ms: u64) -> StreamMessage {
        StreamMessage::ReplicationHeartbeat {
            sender_id: self.primary_id,
            role: NodeRole::Primary,
            current_lsn,
            timestamp_ms,
        }
    }

    /// Compute replication lag in LSN count.
    pub fn lag_lsns(&self, wal: &WalWriter) -> u64 {
        wal.latest_lsn().saturating_sub(self.last_confirmed_lsn)
    }

    /// Whether the replica has confirmed all WAL records.
    pub fn is_caught_up(&self, wal: &WalWriter) -> bool {
        self.last_confirmed_lsn >= wal.latest_lsn()
    }

    pub fn last_confirmed_lsn(&self) -> Lsn {
        self.last_confirmed_lsn
    }
    pub fn last_sent_lsn(&self) -> Lsn {
        self.last_sent_lsn
    }
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }
    pub fn batches_sent(&self) -> u64 {
        self.batches_sent
    }
    pub fn batches_acked(&self) -> u64 {
        self.batches_acked
    }
    pub fn mode(&self) -> ReplicationMode {
        self.mode
    }
}

/// Tracks inbound WAL streaming on a replica.
///
/// Receives `WalBatch` messages from the primary, applies them to the
/// local WAL, produces `WalBatchAck` responses, and monitors health.
pub struct StreamingReceiver {
    replica_id: NodeId,
    primary_id: NodeId,
    applied_lsn: Lsn,
    local_wal: WalWriter,
    batches_received: u64,
    bytes_received: u64,
    last_heartbeat_ms: u64,
    is_streaming: bool,
}

impl StreamingReceiver {
    pub fn new(replica_id: NodeId, primary_id: NodeId) -> Self {
        Self {
            replica_id,
            primary_id,
            applied_lsn: 0,
            local_wal: WalWriter::new(),
            batches_received: 0,
            bytes_received: 0,
            last_heartbeat_ms: 0,
            is_streaming: false,
        }
    }

    /// Process an incoming `WalBatch` message. Applies records to the local
    /// WAL in strict LSN order. Returns a `WalBatchAck` on success.
    pub fn handle_batch(&mut self, msg: &StreamMessage) -> Result<StreamMessage, ReplicationError> {
        let (batch_seq, records) = match msg {
            StreamMessage::WalBatch {
                batch_seq, records, ..
            } => (*batch_seq, records),
            _ => return Err(ReplicationError::ProtocolError("expected WalBatch".into())),
        };

        for rec in records {
            let expected = self.applied_lsn + 1;
            if rec.lsn != expected {
                return Err(ReplicationError::StaleRecord {
                    expected,
                    got: rec.lsn,
                });
            }
            self.local_wal.append(rec.payload.clone());
            self.applied_lsn = rec.lsn;
            self.bytes_received += rec.payload.estimated_size();
        }
        self.local_wal.flush();
        self.batches_received += 1;

        Ok(StreamMessage::WalBatchAck {
            sender_id: self.replica_id,
            confirmed_lsn: self.applied_lsn,
            batch_seq,
        })
    }

    /// Process a heartbeat from the primary.
    pub fn handle_heartbeat(&mut self, msg: &StreamMessage) {
        if let StreamMessage::ReplicationHeartbeat { timestamp_ms, .. } = msg {
            self.last_heartbeat_ms = *timestamp_ms;
        }
    }

    /// Build a StartStreaming request to send to the primary.
    pub fn request_streaming(&self) -> StreamMessage {
        StreamMessage::StartStreaming {
            replica_id: self.replica_id,
            from_lsn: self.applied_lsn,
        }
    }

    /// Process a StreamingStarted message from the primary.
    pub fn handle_streaming_started(&mut self, msg: &StreamMessage) {
        if let StreamMessage::StreamingStarted { .. } = msg {
            self.is_streaming = true;
        }
    }

    /// Build a heartbeat from this replica.
    pub fn heartbeat(&self, timestamp_ms: u64) -> StreamMessage {
        StreamMessage::ReplicationHeartbeat {
            sender_id: self.replica_id,
            role: NodeRole::Replica,
            current_lsn: self.applied_lsn,
            timestamp_ms,
        }
    }

    /// Replication lag relative to the primary's known WAL tip.
    pub fn lag_from(&self, primary_wal_tip: Lsn) -> u64 {
        primary_wal_tip.saturating_sub(self.applied_lsn)
    }

    /// Whether the primary's heartbeat has gone stale.
    pub fn is_primary_stale(&self, now_ms: u64, timeout_ms: u64) -> bool {
        if self.last_heartbeat_ms == 0 {
            return now_ms >= timeout_ms;
        }
        now_ms.saturating_sub(self.last_heartbeat_ms) >= timeout_ms
    }

    pub fn applied_lsn(&self) -> Lsn {
        self.applied_lsn
    }
    pub fn wal(&self) -> &WalWriter {
        &self.local_wal
    }
    pub fn batches_received(&self) -> u64 {
        self.batches_received
    }
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
    }
    pub fn is_streaming(&self) -> bool {
        self.is_streaming
    }
    pub fn primary_id(&self) -> NodeId {
        self.primary_id
    }
}

// ---------------------------------------------------------------------------
// ReplicationStatusReport (for SHOW REPLICATION STATUS)
// ---------------------------------------------------------------------------

/// Extended replication status for SQL `SHOW REPLICATION STATUS`.
#[derive(Debug, Clone)]
pub struct ReplicationStatusReport {
    pub node_id: NodeId,
    pub role: NodeRole,
    pub mode: ReplicationMode,
    pub wal_lsn: Lsn,
    pub applied_lsn: Lsn,
    pub confirmed_lsn: Lsn,
    pub replication_lag_lsns: u64,
    pub bytes_streamed: u64,
    pub batches_sent: u64,
    pub batches_received: u64,
    pub peer_connected: bool,
    pub is_streaming: bool,
}

impl ReplicationStatusReport {
    /// Build a status report from the primary side.
    pub fn from_sender(mgr: &ReplicationManager, sender: &StreamingSender) -> Self {
        Self {
            node_id: mgr.node_id(),
            role: NodeRole::Primary,
            mode: sender.mode(),
            wal_lsn: mgr.wal().latest_lsn(),
            applied_lsn: mgr.applied_lsn(),
            confirmed_lsn: sender.last_confirmed_lsn(),
            replication_lag_lsns: sender.lag_lsns(mgr.wal()),
            bytes_streamed: sender.bytes_sent(),
            batches_sent: sender.batches_sent(),
            batches_received: sender.batches_acked(),
            peer_connected: true,
            is_streaming: true,
        }
    }

    /// Build a status report from the replica side.
    pub fn from_receiver(receiver: &StreamingReceiver, primary_tip: Lsn) -> Self {
        Self {
            node_id: receiver.replica_id,
            role: NodeRole::Replica,
            mode: ReplicationMode::Asynchronous,
            wal_lsn: receiver.wal().latest_lsn(),
            applied_lsn: receiver.applied_lsn(),
            confirmed_lsn: receiver.applied_lsn(),
            replication_lag_lsns: receiver.lag_from(primary_tip),
            bytes_streamed: receiver.bytes_received(),
            batches_sent: 0,
            batches_received: receiver.batches_received(),
            peer_connected: receiver.is_streaming(),
            is_streaming: receiver.is_streaming(),
        }
    }

    /// Convert to rows suitable for SQL result set output.
    pub fn as_rows(&self) -> Vec<(String, String)> {
        vec![
            ("node_id".into(), self.node_id.to_string()),
            ("role".into(), self.role.to_string()),
            ("mode".into(), format!("{:?}", self.mode)),
            ("wal_lsn".into(), self.wal_lsn.to_string()),
            ("applied_lsn".into(), self.applied_lsn.to_string()),
            ("confirmed_lsn".into(), self.confirmed_lsn.to_string()),
            (
                "replication_lag_lsns".into(),
                self.replication_lag_lsns.to_string(),
            ),
            ("bytes_streamed".into(), self.bytes_streamed.to_string()),
            ("batches_sent".into(), self.batches_sent.to_string()),
            ("batches_received".into(), self.batches_received.to_string()),
            ("peer_connected".into(), self.peer_connected.to_string()),
            ("is_streaming".into(), self.is_streaming.to_string()),
        ]
    }
}

// ===========================================================================
// Cluster-wide replication status report (SHOW REPLICATION STATUS)
// ===========================================================================

/// A cluster-wide status report combining primary and replica info.
#[derive(Debug, Clone)]
pub struct ClusterReplicationStatus {
    pub primary_node_id: NodeId,
    pub primary_wal_lsn: Lsn,
    pub mode: ReplicationMode,
    pub replicas: Vec<ReplicaStatusEntry>,
}

/// Status of a single replica in the cluster.
#[derive(Debug, Clone)]
pub struct ReplicaStatusEntry {
    pub node_id: NodeId,
    pub state: StreamingState,
    pub applied_lsn: Lsn,
    pub confirmed_lsn: Lsn,
    pub lag_lsns: u64,
    pub lag_bytes: u64,
    pub is_connected: bool,
}

/// Streaming state of a replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingState {
    Streaming,
    CatchingUp,
    Disconnected,
}

impl fmt::Display for StreamingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamingState::Streaming => write!(f, "streaming"),
            StreamingState::CatchingUp => write!(f, "catching_up"),
            StreamingState::Disconnected => write!(f, "disconnected"),
        }
    }
}

impl ClusterReplicationStatus {
    /// Build from a primary's ReplicationManager and its stream.
    pub fn from_primary(mgr: &ReplicationManager) -> Self {
        let mut replicas = Vec::new();

        if let Some(stream) = &mgr.stream {
            let lag = stream.replication_lag(&mgr.wal);
            let lag_bytes: u64 = mgr
                .wal
                .records_since(stream.replica_confirmed_lsn)
                .iter()
                .map(|r| r.payload.estimated_size())
                .sum();

            let state = if !mgr.local_state.is_connected {
                StreamingState::Disconnected
            } else if lag == 0 {
                StreamingState::Streaming
            } else {
                StreamingState::CatchingUp
            };

            replicas.push(ReplicaStatusEntry {
                node_id: stream.replica_id,
                state,
                applied_lsn: stream.replica_confirmed_lsn,
                confirmed_lsn: stream.replica_confirmed_lsn,
                lag_lsns: lag,
                lag_bytes,
                is_connected: mgr.local_state.is_connected,
            });
        }

        Self {
            primary_node_id: mgr.local_state.node_id,
            primary_wal_lsn: mgr.wal.latest_lsn(),
            mode: mgr.mode,
            replicas,
        }
    }

    /// Build from a standalone (no replicas).
    pub fn standalone(node_id: NodeId) -> Self {
        Self {
            primary_node_id: node_id,
            primary_wal_lsn: 0,
            mode: ReplicationMode::Asynchronous,
            replicas: Vec::new(),
        }
    }

    /// Convert to row-based SQL output.
    pub fn as_result_rows(&self) -> Vec<Vec<(String, String)>> {
        let mut rows = Vec::new();
        // Header row for the primary.
        rows.push(vec![
            ("primary_node_id".into(), self.primary_node_id.to_string()),
            ("primary_wal_lsn".into(), self.primary_wal_lsn.to_string()),
            ("mode".into(), format!("{:?}", self.mode)),
            ("replica_count".into(), self.replicas.len().to_string()),
        ]);
        // One row per replica.
        for r in &self.replicas {
            rows.push(vec![
                ("replica_node_id".into(), r.node_id.to_string()),
                ("state".into(), r.state.to_string()),
                ("applied_lsn".into(), r.applied_lsn.to_string()),
                ("confirmed_lsn".into(), r.confirmed_lsn.to_string()),
                ("lag_lsns".into(), r.lag_lsns.to_string()),
                ("lag_bytes".into(), r.lag_bytes.to_string()),
                ("is_connected".into(), r.is_connected.to_string()),
            ]);
        }
        rows
    }
}

// ===========================================================================
// TCP Replication Bridge (3.3)
// ===========================================================================

/// Message types for the replication wire protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    /// Initial authentication handshake (shared token).
    Auth { token: String },
    /// Primary sends a batch of WAL records to the replica.
    WalBatch { records: Vec<WalRecord> },
    /// Replica confirms it has applied up to the given LSN.
    Confirm { applied_lsn: Lsn },
    /// Heartbeat from primary to replica.
    Heartbeat { primary_lsn: Lsn },
    /// Heartbeat response from replica.
    HeartbeatResponse { replica_lsn: Lsn },
}

impl ReplicationMessage {
    /// Serialize to a simple binary format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            ReplicationMessage::Auth { token } => {
                buf.push(5);
                let bytes = token.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            ReplicationMessage::WalBatch { records } => {
                buf.push(1); // tag
                buf.extend_from_slice(&(records.len() as u64).to_le_bytes());
                for rec in records {
                    buf.extend_from_slice(&rec.lsn.to_le_bytes());
                    buf.extend_from_slice(&rec.timestamp_ms.to_le_bytes());
                    match &rec.payload {
                        WalPayload::PageWrite { page_id, data } => {
                            buf.push(1);
                            buf.extend_from_slice(&page_id.to_le_bytes());
                            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
                            buf.extend_from_slice(data);
                        }
                        WalPayload::Commit { txn_id } => {
                            buf.push(2);
                            buf.extend_from_slice(&txn_id.to_le_bytes());
                        }
                        WalPayload::Abort { txn_id } => {
                            buf.push(3);
                            buf.extend_from_slice(&txn_id.to_le_bytes());
                        }
                        WalPayload::Checkpoint => {
                            buf.push(4);
                        }
                    }
                }
            }
            ReplicationMessage::Confirm { applied_lsn } => {
                buf.push(2);
                buf.extend_from_slice(&applied_lsn.to_le_bytes());
            }
            ReplicationMessage::Heartbeat { primary_lsn } => {
                buf.push(3);
                buf.extend_from_slice(&primary_lsn.to_le_bytes());
            }
            ReplicationMessage::HeartbeatResponse { replica_lsn } => {
                buf.push(4);
                buf.extend_from_slice(&replica_lsn.to_le_bytes());
            }
        }
        buf
    }

    /// Deserialize from binary format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.is_empty() {
            return Err("empty message".into());
        }
        let mut pos = 1;
        match data[0] {
            5 => {
                if data.len() < pos + 8 {
                    return Err("truncated auth token length".into());
                }
                let tlen = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
                pos += 8;
                if data.len() < pos + tlen {
                    return Err("truncated auth token".into());
                }
                let token = String::from_utf8(data[pos..pos + tlen].to_vec())
                    .map_err(|_| "invalid auth token utf8".to_string())?;
                Ok(ReplicationMessage::Auth { token })
            }
            1 => {
                // WalBatch
                if data.len() < pos + 8 {
                    return Err("truncated batch count".into());
                }
                let count = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
                pos += 8;
                let mut records = Vec::with_capacity(count);
                for _ in 0..count {
                    if data.len() < pos + 16 {
                        return Err("truncated record header".into());
                    }
                    let lsn = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let timestamp_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    if pos >= data.len() {
                        return Err("truncated payload tag".into());
                    }
                    let payload = match data[pos] {
                        1 => {
                            pos += 1;
                            if data.len() < pos + 16 {
                                return Err("truncated page write".into());
                            }
                            let page_id =
                                u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                            pos += 8;
                            let dlen =
                                u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
                            pos += 8;
                            if data.len() < pos + dlen {
                                return Err("truncated page data".into());
                            }
                            let d = data[pos..pos + dlen].to_vec();
                            pos += dlen;
                            WalPayload::PageWrite { page_id, data: d }
                        }
                        2 => {
                            pos += 1;
                            if data.len() < pos + 8 {
                                return Err("truncated commit".into());
                            }
                            let txn_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                            pos += 8;
                            WalPayload::Commit { txn_id }
                        }
                        3 => {
                            pos += 1;
                            if data.len() < pos + 8 {
                                return Err("truncated abort".into());
                            }
                            let txn_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                            pos += 8;
                            WalPayload::Abort { txn_id }
                        }
                        4 => {
                            pos += 1;
                            WalPayload::Checkpoint
                        }
                        t => return Err(format!("unknown payload tag: {t}")),
                    };
                    records.push(WalRecord {
                        lsn,
                        timestamp_ms,
                        payload,
                    });
                }
                Ok(ReplicationMessage::WalBatch { records })
            }
            2 => {
                if data.len() < pos + 8 {
                    return Err("truncated confirm".into());
                }
                let lsn = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                Ok(ReplicationMessage::Confirm { applied_lsn: lsn })
            }
            3 => {
                if data.len() < pos + 8 {
                    return Err("truncated heartbeat".into());
                }
                let lsn = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                Ok(ReplicationMessage::Heartbeat { primary_lsn: lsn })
            }
            4 => {
                if data.len() < pos + 8 {
                    return Err("truncated heartbeat response".into());
                }
                let lsn = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                Ok(ReplicationMessage::HeartbeatResponse { replica_lsn: lsn })
            }
            t => Err(format!("unknown message tag: {t}")),
        }
    }
}

/// Bridges the replication layer with TCP transport.
///
/// Wraps a [`ReplicationManager`] and adds methods to serialize WAL batches
/// into [`ReplicationMessage`] frames suitable for TCP transport.
pub struct TcpReplicationBridge {
    node_id: NodeId,
    role: NodeRole,
    last_sent_lsn: Lsn,
    last_received_lsn: Lsn,
    batch_size: usize,
    messages_sent: u64,
    messages_received: u64,
}

impl TcpReplicationBridge {
    /// Create a new bridge for the given node.
    pub fn new(node_id: NodeId, role: NodeRole) -> Self {
        Self {
            node_id,
            role,
            last_sent_lsn: 0,
            last_received_lsn: 0,
            batch_size: 64,
            messages_sent: 0,
            messages_received: 0,
        }
    }

    /// Prepare a WAL batch message from the primary's WAL.
    pub fn prepare_wal_batch(&mut self, wal: &WalWriter) -> Option<ReplicationMessage> {
        let records = wal.records_since(self.last_sent_lsn);
        if records.is_empty() {
            return None;
        }
        let take = records.len().min(self.batch_size);
        let batch: Vec<WalRecord> = records[..take].to_vec();
        if let Some(last) = batch.last() {
            self.last_sent_lsn = last.lsn;
        }
        self.messages_sent += 1;
        Some(ReplicationMessage::WalBatch { records: batch })
    }

    /// Process a received message and return an optional response.
    pub fn handle_message(&mut self, msg: &ReplicationMessage) -> Option<ReplicationMessage> {
        self.messages_received += 1;
        match msg {
            ReplicationMessage::Auth { .. } => None,
            ReplicationMessage::WalBatch { records } => {
                // Replica side: received records, confirm the last LSN
                if let Some(last) = records.last() {
                    self.last_received_lsn = last.lsn;
                }
                Some(ReplicationMessage::Confirm {
                    applied_lsn: self.last_received_lsn,
                })
            }
            ReplicationMessage::Confirm { applied_lsn } => {
                // Primary side: replica confirmed
                self.last_received_lsn = *applied_lsn;
                None
            }
            ReplicationMessage::Heartbeat { primary_lsn: _ } => {
                Some(ReplicationMessage::HeartbeatResponse {
                    replica_lsn: self.last_received_lsn,
                })
            }
            ReplicationMessage::HeartbeatResponse { replica_lsn } => {
                self.last_received_lsn = *replica_lsn;
                None
            }
        }
    }

    /// Create a heartbeat message.
    pub fn heartbeat(&mut self, current_lsn: Lsn) -> ReplicationMessage {
        self.messages_sent += 1;
        if self.role == NodeRole::Primary {
            ReplicationMessage::Heartbeat {
                primary_lsn: current_lsn,
            }
        } else {
            ReplicationMessage::HeartbeatResponse {
                replica_lsn: current_lsn,
            }
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub fn role(&self) -> NodeRole {
        self.role
    }
    pub fn last_sent_lsn(&self) -> Lsn {
        self.last_sent_lsn
    }
    pub fn last_received_lsn(&self) -> Lsn {
        self.last_received_lsn
    }
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent
    }
    pub fn messages_received(&self) -> u64 {
        self.messages_received
    }

    /// Set the batch size for WAL streaming.
    pub fn set_batch_size(&mut self, size: usize) {
        self.batch_size = size;
    }
}

// ===========================================================================
// WAL bridge — connects storage::wal to replication::WalWriter
// ===========================================================================

/// Convert a storage WAL record to a replication WAL record.
/// This bridges the disk-based `storage::wal::WalRecord` (page-level) to the
/// replication protocol's `WalRecord` (payload-based).
pub fn from_storage_wal_record(rec: &crate::storage::wal::WalRecord) -> WalRecord {
    let payload = match rec.record_type {
        crate::storage::wal::RECORD_PAGE_WRITE => {
            let data = rec
                .page_image
                .as_ref()
                .map(|img| img.to_vec())
                .unwrap_or_default();
            WalPayload::PageWrite {
                page_id: rec.page_id as u64,
                data,
            }
        }
        crate::storage::wal::RECORD_COMMIT => WalPayload::Commit { txn_id: rec.txn_id },
        crate::storage::wal::RECORD_ABORT => WalPayload::Abort { txn_id: rec.txn_id },
        crate::storage::wal::RECORD_CHECKPOINT => WalPayload::Checkpoint,
        _ => WalPayload::Checkpoint, // unknown → treat as checkpoint (safe no-op)
    };
    WalRecord {
        lsn: rec.lsn,
        timestamp_ms: rec.lsn, // Use LSN as timestamp (monotonic)
        payload,
    }
}

/// Convert a replication WAL record back to a storage WAL record.
/// Used by replicas to apply received records to their local storage.
pub fn to_storage_wal_record(rec: &WalRecord) -> crate::storage::wal::WalRecord {
    match &rec.payload {
        WalPayload::PageWrite { page_id, data } => {
            let mut page_buf = Box::new([0u8; crate::storage::page::PAGE_SIZE]);
            let copy_len = data.len().min(crate::storage::page::PAGE_SIZE);
            page_buf[..copy_len].copy_from_slice(&data[..copy_len]);
            crate::storage::wal::WalRecord {
                lsn: rec.lsn,
                txn_id: 0,
                record_type: crate::storage::wal::RECORD_PAGE_WRITE,
                page_id: *page_id as u32,
                page_image: Some(page_buf),
            }
        }
        WalPayload::Commit { txn_id } => crate::storage::wal::WalRecord {
            lsn: rec.lsn,
            txn_id: *txn_id,
            record_type: crate::storage::wal::RECORD_COMMIT,
            page_id: 0,
            page_image: None,
        },
        WalPayload::Abort { txn_id } => crate::storage::wal::WalRecord {
            lsn: rec.lsn,
            txn_id: *txn_id,
            record_type: crate::storage::wal::RECORD_ABORT,
            page_id: 0,
            page_image: None,
        },
        WalPayload::Checkpoint => crate::storage::wal::WalRecord {
            lsn: rec.lsn,
            txn_id: 0,
            record_type: crate::storage::wal::RECORD_CHECKPOINT,
            page_id: 0,
            page_image: None,
        },
    }
}

/// Bridge between the disk-based storage WAL and the replication protocol.
///
/// Reads new records from the storage WAL and feeds them into a ReplicationManager
/// for streaming to replicas. On the replica side, converts received replication
/// records back to storage format for local application.
pub struct WalBridge {
    /// Last LSN that was forwarded to the replication layer.
    last_forwarded_lsn: Lsn,
}

impl Default for WalBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl WalBridge {
    pub fn new() -> Self {
        Self {
            last_forwarded_lsn: 0,
        }
    }

    /// Read new records from the storage WAL and forward them to the replication manager.
    /// Returns the number of records forwarded.
    pub fn forward_new_records(
        &mut self,
        storage_wal_path: &std::path::Path,
        repl_mgr: &mut ReplicationManager,
    ) -> usize {
        let storage_records =
            crate::storage::wal::read_wal_records(storage_wal_path).unwrap_or_default();

        let mut count = 0;
        for srec in &storage_records {
            if srec.lsn <= self.last_forwarded_lsn {
                continue;
            }
            let repl_rec = from_storage_wal_record(srec);
            if let Ok(_lsn) = repl_mgr.write(repl_rec.payload) {
                self.last_forwarded_lsn = srec.lsn;
                count += 1;
            }
        }
        count
    }

    /// Apply a batch of replication records to a local storage WAL.
    /// Used by replicas to apply received page-write records from the primary.
    pub fn apply_to_storage_wal(
        storage_wal: &crate::storage::wal::Wal,
        repl_records: &[WalRecord],
    ) -> usize {
        let mut count = 0;
        for rrec in repl_records {
            let srec = to_storage_wal_record(rrec);
            if srec.record_type == crate::storage::wal::RECORD_PAGE_WRITE {
                if let Some(ref page_image) = srec.page_image {
                    if storage_wal
                        .log_page_write(srec.txn_id, srec.page_id, page_image)
                        .is_ok()
                    {
                        count += 1;
                    }
                }
            }
            // Control records (commit/abort/checkpoint) don't need to be written
            // to the local WAL — they're implicit in the replayed page state.
        }
        count
    }

    pub fn last_forwarded_lsn(&self) -> Lsn {
        self.last_forwarded_lsn
    }
}

// ===========================================================================
// WalNotifier — broadcast channel bridge from storage WAL to replication
// ===========================================================================

/// A notification emitted when a new WAL record is written on the primary.
/// This is the bridge between the storage WAL and the replication transport.
#[derive(Debug, Clone)]
pub struct WalNotification {
    /// The replication WAL record.
    pub record: WalRecord,
}

/// Bridges the storage WAL to the replication system via a `tokio::sync::broadcast` channel.
///
/// The primary registers a `WalNotifier`. When WAL writes happen (via `notify`),
/// records are broadcast to all connected replicas listening on the receiver side.
pub struct WalNotifier {
    sender: tokio::sync::broadcast::Sender<WalNotification>,
    /// Track last notified LSN to avoid duplicates.
    last_notified_lsn: Lsn,
}

impl WalNotifier {
    /// Create a new notifier with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            sender,
            last_notified_lsn: 0,
        }
    }

    /// Subscribe to WAL notifications. Returns a receiver that can be used
    /// by a replication server connection to stream records to a replica.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<WalNotification> {
        self.sender.subscribe()
    }

    /// Notify all subscribers of a new WAL record.
    /// Returns the number of receivers that received the notification.
    pub fn notify(&mut self, record: WalRecord) -> usize {
        if record.lsn <= self.last_notified_lsn {
            return 0;
        }
        self.last_notified_lsn = record.lsn;
        // send() returns Err if no receivers, which is fine.
        self.sender.send(WalNotification { record }).unwrap_or(0)
    }

    /// Notify from a batch of storage WAL records (reading the storage WAL file
    /// and forwarding new records as notifications).
    pub fn notify_from_storage_wal(&mut self, storage_wal_path: &std::path::Path) -> usize {
        let storage_records =
            crate::storage::wal::read_wal_records(storage_wal_path).unwrap_or_default();

        let mut count = 0;
        for srec in &storage_records {
            if srec.lsn <= self.last_notified_lsn {
                continue;
            }
            let repl_rec = from_storage_wal_record(srec);
            self.notify(repl_rec);
            count += 1;
        }
        count
    }

    /// Get the last notified LSN.
    pub fn last_notified_lsn(&self) -> Lsn {
        self.last_notified_lsn
    }

    /// Get the number of active subscribers.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

// ===========================================================================
// TCP Replication Transport — ReplicationServer / ReplicationClient
// ===========================================================================

/// Framed message I/O helpers for the replication wire protocol.
///
/// Each message is framed as: `[len: u32 little-endian][payload: len bytes]`
/// where `payload` is a `ReplicationMessage` serialized via `to_bytes()`.
pub mod framing {
    use super::{ReplicationError, ReplicationMessage};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Write a framed message to an async writer.
    pub async fn write_message<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        msg: &ReplicationMessage,
    ) -> std::io::Result<()> {
        let payload = msg.to_bytes();
        let len = payload.len() as u32;
        writer.write_all(&len.to_le_bytes()).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Read a framed message from an async reader.
    /// Returns `None` on clean EOF.
    pub async fn read_message<R: AsyncReadExt + Unpin>(
        reader: &mut R,
    ) -> Result<Option<ReplicationMessage>, ReplicationError> {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(ReplicationError::ProtocolError(format!("read len: {e}"))),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 || len > 64 * 1024 * 1024 {
            return Err(ReplicationError::ProtocolError(format!(
                "invalid frame length: {len}"
            )));
        }
        let mut payload = vec![0u8; len];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|e| ReplicationError::ProtocolError(format!("read payload: {e}")))?;
        let msg = ReplicationMessage::from_bytes(&payload)
            .map_err(|e| ReplicationError::ProtocolError(format!("decode: {e}")))?;
        Ok(Some(msg))
    }
}

/// A replication server that runs on the primary node.
///
/// Accepts TCP connections from replicas and streams WAL records to them
/// using the `WalNotifier` broadcast channel.
pub struct ReplicationServer {
    /// Address to listen on (e.g., "0.0.0.0:5434").
    listen_addr: String,
    /// Shared WAL notifier for broadcasting records to connected replicas.
    notifier: std::sync::Arc<tokio::sync::Mutex<WalNotifier>>,
    /// Optional shared token required for replica authentication.
    auth_token: Option<String>,
    /// Optional TLS material for encrypted replication transport.
    tls: Option<InternalTlsConfig>,
}

impl ReplicationServer {
    /// Create a new replication server.
    pub fn new(
        listen_addr: String,
        notifier: std::sync::Arc<tokio::sync::Mutex<WalNotifier>>,
        auth_token: Option<String>,
    ) -> Self {
        Self::new_with_tls(listen_addr, notifier, auth_token, None)
    }

    /// Create a new replication server with optional TLS.
    pub fn new_with_tls(
        listen_addr: String,
        notifier: std::sync::Arc<tokio::sync::Mutex<WalNotifier>>,
        auth_token: Option<String>,
        tls: Option<InternalTlsConfig>,
    ) -> Self {
        Self {
            listen_addr,
            notifier,
            auth_token,
            tls,
        }
    }

    /// Start listening for replica connections. This method runs forever.
    ///
    /// For each connected replica, spawns a task that:
    /// 1. Subscribes to the WAL notification broadcast channel
    /// 2. Forwards WAL records as `ReplicationMessage::WalBatch` frames
    /// 3. Reads `Confirm` messages back from the replica
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        tracing::info!("Replication server listening on {}", self.listen_addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            tracing::info!("Replica connected from {peer_addr}");

            let rx = {
                let notifier = self.notifier.lock().await;
                notifier.subscribe()
            };
            let auth_token = self.auth_token.clone();
            let tls = self.tls.clone();

            tokio::spawn(async move {
                if let Some(tls_cfg) = tls {
                    match tls_cfg.acceptor.accept(stream).await {
                        Ok(tls_stream) => {
                            if let Err(e) = Self::handle_replica(tls_stream, rx, auth_token).await {
                                tracing::warn!("Replica {peer_addr} disconnected: {e}");
                            } else {
                                tracing::info!("Replica {peer_addr} disconnected cleanly");
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Replication TLS accept failed from {peer_addr}: {e}");
                        }
                    }
                } else if let Err(e) = Self::handle_replica(stream, rx, auth_token).await {
                    tracing::warn!("Replica {peer_addr} disconnected: {e}");
                } else {
                    tracing::info!("Replica {peer_addr} disconnected cleanly");
                }
            });
        }
    }

    /// Handle a single replica connection.
    async fn handle_replica<S>(
        mut stream: S,
        mut rx: tokio::sync::broadcast::Receiver<WalNotification>,
        auth_token: Option<String>,
    ) -> Result<(), ReplicationError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        if let Some(expected) = auth_token.as_deref() {
            match framing::read_message(&mut stream).await? {
                Some(ReplicationMessage::Auth { token })
                    if constant_time_eq_token(token.as_bytes(), expected.as_bytes()) => {}
                Some(_) => {
                    return Err(ReplicationError::ProtocolError(
                        "replication auth failed: expected Auth token".into(),
                    ));
                }
                None => {
                    return Err(ReplicationError::ProtocolError(
                        "replication auth failed: connection closed".into(),
                    ));
                }
            }
        }

        let (reader, writer) = tokio::io::split(stream);
        let writer = std::sync::Arc::new(tokio::sync::Mutex::new(writer));
        let writer_for_recv = writer.clone();

        // Spawn a task to read confirmations from the replica
        let mut reader = reader;
        let recv_handle = tokio::spawn(async move {
            loop {
                match framing::read_message(&mut reader).await {
                    Ok(Some(ReplicationMessage::Confirm { applied_lsn })) => {
                        tracing::debug!("Replica confirmed LSN {applied_lsn}");
                    }
                    Ok(Some(ReplicationMessage::HeartbeatResponse { replica_lsn })) => {
                        tracing::debug!("Replica heartbeat response, LSN {replica_lsn}");
                    }
                    Ok(Some(other)) => {
                        tracing::debug!("Unexpected message from replica: {other:?}");
                    }
                    Ok(None) => {
                        tracing::debug!("Replica disconnected (EOF)");
                        break;
                    }
                    Err(e) => {
                        tracing::debug!("Error reading from replica: {e}");
                        break;
                    }
                }
            }
        });

        // Main loop: receive WAL notifications and forward to replica
        loop {
            match rx.recv().await {
                Ok(notification) => {
                    let msg = ReplicationMessage::WalBatch {
                        records: vec![notification.record],
                    };
                    let mut w = writer_for_recv.lock().await;
                    if let Err(e) = framing::write_message(&mut *w, &msg).await {
                        tracing::debug!("Error writing to replica: {e}");
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Replica lagged by {n} records, continuing...");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::debug!("WAL notifier channel closed");
                    break;
                }
            }
        }

        recv_handle.abort();
        Ok(())
    }

    /// Get the listen address.
    pub fn listen_addr(&self) -> &str {
        &self.listen_addr
    }
}

/// A replication client that runs on a replica node.
///
/// Connects to the primary's replication server via TCP and receives
/// WAL records, applying them to the local replication manager.
pub struct ReplicationClient {
    /// The primary's replication address (e.g., "192.168.1.1:5434").
    primary_addr: String,
    /// Optional shared token used to authenticate to the replication server.
    auth_token: Option<String>,
    /// Optional TLS material for encrypted replication transport.
    tls: Option<InternalTlsConfig>,
}

impl ReplicationClient {
    /// Create a new replication client targeting the given primary address.
    pub fn new(primary_addr: String, auth_token: Option<String>) -> Self {
        Self::new_with_tls(primary_addr, auth_token, None)
    }

    /// Create a new replication client with optional TLS.
    pub fn new_with_tls(
        primary_addr: String,
        auth_token: Option<String>,
        tls: Option<InternalTlsConfig>,
    ) -> Self {
        Self {
            primary_addr,
            auth_token,
            tls,
        }
    }

    /// Connect to the primary and start receiving WAL records.
    ///
    /// Received records are applied to the provided `ReplicationManager`
    /// (protected by `parking_lot::RwLock`). This method runs until the
    /// connection is lost or an error occurs.
    pub async fn run(
        &self,
        repl_mgr: std::sync::Arc<parking_lot::RwLock<ReplicationManager>>,
    ) -> Result<(), ReplicationError> {
        tracing::info!("Connecting to primary at {}...", self.primary_addr);

        let stream = tokio::net::TcpStream::connect(&self.primary_addr)
            .await
            .map_err(|e| ReplicationError::ProtocolError(format!("connect: {e}")))?;

        tracing::info!("Connected to primary at {}", self.primary_addr);

        if let Some(tls_cfg) = self.tls.as_ref() {
            let server_name = pgwire::tokio::tokio_rustls::rustls::pki_types::ServerName::try_from(
                tls_cfg.server_name.clone(),
            )
            .map_err(|e| {
                ReplicationError::ProtocolError(format!("invalid TLS server name: {e}"))
            })?;
            let tls_stream = tls_cfg
                .connector
                .connect(server_name, stream)
                .await
                .map_err(|e| {
                    ReplicationError::ProtocolError(format!("replication TLS connect: {e}"))
                })?;
            self.run_stream(tls_stream, repl_mgr).await
        } else {
            self.run_stream(stream, repl_mgr).await
        }
    }

    async fn run_stream<S>(
        &self,
        mut stream: S,
        repl_mgr: std::sync::Arc<parking_lot::RwLock<ReplicationManager>>,
    ) -> Result<(), ReplicationError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        if let Some(token) = self.auth_token.clone() {
            let msg = ReplicationMessage::Auth { token };
            framing::write_message(&mut stream, &msg)
                .await
                .map_err(|e| ReplicationError::ProtocolError(format!("send auth: {e}")))?;
        }

        let (mut reader, mut writer) = tokio::io::split(stream);

        loop {
            match framing::read_message(&mut reader).await {
                Ok(Some(ReplicationMessage::WalBatch { records })) => {
                    let batch_size = records.len();
                    let last_lsn = records.last().map(|r| r.lsn).unwrap_or(0);

                    {
                        let mut mgr = repl_mgr.write();
                        if let Err(e) = mgr.apply_batch(records) {
                            tracing::error!("Failed to apply batch: {e}");
                            continue;
                        }
                    }

                    tracing::debug!("Applied {batch_size} records, last LSN: {last_lsn}");

                    // Send confirmation
                    let confirm = ReplicationMessage::Confirm {
                        applied_lsn: last_lsn,
                    };
                    if let Err(e) = framing::write_message(&mut writer, &confirm).await {
                        return Err(ReplicationError::ProtocolError(format!(
                            "send confirm: {e}"
                        )));
                    }
                }
                Ok(Some(ReplicationMessage::Heartbeat { primary_lsn })) => {
                    tracing::debug!("Heartbeat from primary, LSN {primary_lsn}");
                    let replica_lsn = {
                        let mgr = repl_mgr.read();
                        mgr.applied_lsn()
                    };
                    let resp = ReplicationMessage::HeartbeatResponse { replica_lsn };
                    if let Err(e) = framing::write_message(&mut writer, &resp).await {
                        return Err(ReplicationError::ProtocolError(format!(
                            "send heartbeat response: {e}"
                        )));
                    }
                }
                Ok(Some(other)) => {
                    tracing::debug!("Unexpected message from primary: {other:?}");
                }
                Ok(None) => {
                    tracing::warn!("Primary disconnected");
                    return Err(ReplicationError::ProtocolError(
                        "primary disconnected".into(),
                    ));
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    /// Get the primary address.
    pub fn primary_addr(&self) -> &str {
        &self.primary_addr
    }
}

fn constant_time_eq_token(lhs: &[u8], rhs: &[u8]) -> bool {
    let max_len = lhs.len().max(rhs.len());
    let mut diff = lhs.len() ^ rhs.len();
    for i in 0..max_len {
        let a = *lhs.get(i).unwrap_or(&0);
        let b = *rhs.get(i).unwrap_or(&0);
        diff |= (a ^ b) as usize;
    }
    diff == 0
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- WAL ----------------------------------------------------------------

    #[test]
    fn constant_time_eq_token_matches_expected() {
        assert!(constant_time_eq_token(b"secret", b"secret"));
        assert!(!constant_time_eq_token(b"secret", b"secreT"));
        assert!(!constant_time_eq_token(b"secret", b"secret1"));
    }

    #[test]
    fn wal_append_and_records_since() {
        let mut wal = WalWriter::new();
        let lsn1 = wal.append(WalPayload::PageWrite {
            page_id: 1,
            data: vec![0xAA; 64],
        });
        let lsn2 = wal.append(WalPayload::Commit { txn_id: 100 });
        let lsn3 = wal.append(WalPayload::Checkpoint);

        assert_eq!((lsn1, lsn2, lsn3), (1, 2, 3));
        assert_eq!(wal.len(), 3);
        assert_eq!(wal.latest_lsn(), 3);

        let all = wal.records_since(0);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].lsn, 1);

        let tail = wal.records_since(1);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].lsn, 2);

        assert!(wal.records_since(3).is_empty());
    }

    #[test]
    fn wal_flush_and_flushed_lsn() {
        let mut wal = WalWriter::new();
        assert_eq!(wal.flushed_lsn(), 0);

        wal.append(WalPayload::Commit { txn_id: 1 });
        wal.append(WalPayload::Commit { txn_id: 2 });
        assert_eq!(wal.flushed_lsn(), 0);

        wal.flush();
        assert_eq!(wal.flushed_lsn(), 2);

        wal.append(WalPayload::Checkpoint);
        assert_eq!(wal.flushed_lsn(), 2);
        wal.flush();
        assert_eq!(wal.flushed_lsn(), 3);
    }

    #[test]
    fn wal_truncate_before() {
        let mut wal = WalWriter::new();
        for i in 0..5 {
            wal.append(WalPayload::Commit { txn_id: i });
        }
        assert_eq!(wal.len(), 5);

        wal.truncate_before(3); // remove LSNs 1 and 2
        assert_eq!(wal.len(), 3);
        let remaining = wal.records_since(0);
        assert_eq!(remaining[0].lsn, 3);
        assert_eq!(remaining[2].lsn, 5);
    }

    // -- ReplicationStream --------------------------------------------------

    #[test]
    fn stream_prepare_batch_returns_correct_records() {
        let mut wal = WalWriter::new();
        for i in 1..=10 {
            wal.append(WalPayload::Commit { txn_id: i });
        }
        let mut stream = ReplicationStream::new(ReplicationMode::Asynchronous, 1, 2);
        let batch = stream.prepare_batch(&wal);
        assert_eq!(batch.len(), 10);
        assert_eq!(batch[0].lsn, 1);
        assert_eq!(batch[9].lsn, 10);
    }

    #[test]
    fn stream_confirm_received_advances_lag() {
        let mut wal = WalWriter::new();
        for i in 1..=5 {
            wal.append(WalPayload::Commit { txn_id: i });
        }
        let mut stream = ReplicationStream::new(ReplicationMode::Synchronous, 1, 2);
        assert_eq!(stream.replication_lag(&wal), 5);

        stream.confirm_received(3);
        assert_eq!(stream.replication_lag(&wal), 2);

        stream.confirm_received(5);
        assert_eq!(stream.replication_lag(&wal), 0);
    }

    #[test]
    fn stream_is_caught_up() {
        let mut wal = WalWriter::new();
        wal.append(WalPayload::Checkpoint);
        wal.append(WalPayload::Checkpoint);

        let mut stream = ReplicationStream::new(ReplicationMode::Asynchronous, 1, 2);
        assert!(!stream.is_caught_up(&wal));

        stream.confirm_received(2);
        assert!(stream.is_caught_up(&wal));

        wal.append(WalPayload::Checkpoint);
        assert!(!stream.is_caught_up(&wal));
    }

    #[test]
    fn stream_stats_accumulate() {
        let mut wal = WalWriter::new();
        wal.append(WalPayload::PageWrite {
            page_id: 1,
            data: vec![0u8; 100],
        });
        wal.append(WalPayload::Commit { txn_id: 1 });

        let mut stream = ReplicationStream::new(ReplicationMode::Asynchronous, 1, 2);
        let batch = stream.prepare_batch(&wal);
        assert_eq!(batch.len(), 2);

        let stats = stream.stats();
        assert_eq!(stats.records_streamed, 2);
        // PageWrite(100 data + 8 overhead) + Commit(8) = 116
        assert_eq!(stats.bytes_streamed, 108 + 8);
    }

    // -- FailoverManager ----------------------------------------------------

    #[test]
    fn failover_detects_primary_down() {
        let mut fm = FailoverManager::new(2, NodeRole::Replica, 1000);
        fm.set_peer(1);
        fm.record_heartbeat(1, 100);

        assert!(fm.check_failover(500).is_none());

        let evt = fm.check_failover(1200);
        assert_eq!(
            evt.unwrap(),
            FailoverEvent::PrimaryDown {
                detected_at_ms: 1200
            }
        );
    }

    #[test]
    fn failover_promotes_replica() {
        let mut fm = FailoverManager::new(2, NodeRole::Replica, 1000);
        fm.set_peer(1);
        fm.set_applied_lsn(42);

        let evt = fm.promote_to_primary();
        assert_eq!(
            evt,
            FailoverEvent::ReplicaPromoted {
                node_id: 2,
                at_lsn: 42
            }
        );
        assert_eq!(fm.role(), NodeRole::Primary);
        assert_eq!(fm.current_primary(), Some(2));
    }

    #[test]
    fn failover_old_primary_rejoins() {
        let mut fm = FailoverManager::new(1, NodeRole::Primary, 1000);
        fm.set_peer(2);

        let evt = fm.rejoin_as_replica(2, 50);
        assert_eq!(evt, FailoverEvent::OldPrimaryRejoined { node_id: 1 });
        assert_eq!(fm.role(), NodeRole::Replica);
        assert_eq!(fm.current_primary(), Some(2));
        assert_eq!(fm.history().len(), 1);
    }

    // -- ReplicationManager -------------------------------------------------

    #[test]
    fn manager_primary_write_prepare_batch_replica_apply() {
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        primary.attach_replica(2);

        primary
            .write(WalPayload::PageWrite {
                page_id: 10,
                data: vec![1, 2, 3],
            })
            .unwrap();
        primary.write(WalPayload::Commit { txn_id: 1 }).unwrap();
        assert_eq!(primary.wal().latest_lsn(), 2);

        let batch = primary.prepare_replication_batch();
        assert_eq!(batch.len(), 2);

        let mut replica = ReplicationManager::new_replica(2, 1);
        let applied_lsn = replica.apply_batch(batch).unwrap();
        assert_eq!(applied_lsn, 2);
        assert_eq!(replica.wal().latest_lsn(), 2);

        primary.confirm_replication(applied_lsn);
        assert_eq!(primary.status().replication_lag, 0);
    }

    #[test]
    fn manager_sync_mode_full_cycle() {
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Synchronous);
        primary.attach_replica(2);
        let mut replica = ReplicationManager::new_replica(2, 1);

        for i in 1..=3 {
            primary.write(WalPayload::Commit { txn_id: i }).unwrap();
        }

        let batch = primary.prepare_replication_batch();
        assert_eq!(batch.len(), 3);

        let applied = replica.apply_batch(batch).unwrap();
        assert_eq!(applied, 3);
        primary.confirm_replication(applied);

        let status = primary.status();
        assert_eq!(status.wal_lsn, 3);
        assert_eq!(status.applied_lsn, 3);
        assert_eq!(status.replication_lag, 0);
        assert_eq!(status.role, NodeRole::Primary);
        assert_eq!(status.mode, ReplicationMode::Synchronous);
    }

    #[test]
    fn manager_standalone_accepts_writes() {
        let mut mgr = ReplicationManager::new_standalone(1);
        let lsn = mgr
            .write(WalPayload::PageWrite {
                page_id: 1,
                data: vec![42],
            })
            .unwrap();
        assert_eq!(lsn, 1);

        let status = mgr.status();
        assert_eq!(status.role, NodeRole::Standalone);
        assert_eq!(status.wal_lsn, 1);
        assert_eq!(status.replication_lag, 0);
        assert!(!status.peer_connected);
    }

    #[test]
    fn manager_replica_cannot_write() {
        let mut replica = ReplicationManager::new_replica(2, 1);
        assert_eq!(
            replica.write(WalPayload::Checkpoint),
            Err(ReplicationError::NotPrimary)
        );
    }

    #[test]
    fn manager_replica_rejects_out_of_order_batch() {
        let mut replica = ReplicationManager::new_replica(2, 1);

        let batch1 = vec![WalRecord {
            lsn: 1,
            timestamp_ms: 1,
            payload: WalPayload::Commit { txn_id: 1 },
        }];
        replica.apply_batch(batch1).unwrap();

        // Skip LSNs 2-4, try to apply LSN 5.
        let bad_batch = vec![WalRecord {
            lsn: 5,
            timestamp_ms: 5,
            payload: WalPayload::Commit { txn_id: 5 },
        }];
        assert_eq!(
            replica.apply_batch(bad_batch).unwrap_err(),
            ReplicationError::StaleRecord {
                expected: 2,
                got: 5
            },
        );
    }

    #[test]
    fn full_failover_scenario() {
        // Phase 1: Primary writes data and replicates.
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        primary.attach_replica(2);
        let mut replica = ReplicationManager::new_replica(2, 1);

        for i in 1..=5 {
            primary
                .write(WalPayload::PageWrite {
                    page_id: i,
                    data: vec![i as u8; 32],
                })
                .unwrap();
        }

        let batch = primary.prepare_replication_batch();
        assert_eq!(batch.len(), 5);
        let applied = replica.apply_batch(batch).unwrap();
        assert_eq!(applied, 5);
        primary.confirm_replication(applied);

        // Phase 2: Primary goes down. Replica detects and promotes.
        // Record an initial heartbeat so the detector has a baseline, then
        // check after the failover timeout has elapsed.
        replica.record_heartbeat(1, 1_000);
        let failover_evt = replica.check_health(10_000);
        assert!(matches!(
            failover_evt.unwrap(),
            FailoverEvent::PrimaryDown { .. }
        ));

        let promote_evt = replica.promote().unwrap();
        assert!(matches!(
            promote_evt,
            FailoverEvent::ReplicaPromoted {
                node_id: 2,
                at_lsn: 5
            }
        ));
        assert_eq!(replica.role(), NodeRole::Primary);

        // Phase 3: Promoted node continues accepting writes.
        assert_eq!(
            replica.write(WalPayload::Commit { txn_id: 100 }).unwrap(),
            6
        );
        assert_eq!(
            replica
                .write(WalPayload::PageWrite {
                    page_id: 99,
                    data: vec![0xFF; 16]
                })
                .unwrap(),
            7,
        );

        let status = replica.status();
        assert_eq!(status.role, NodeRole::Primary);
        assert_eq!(status.wal_lsn, 7);
        assert_eq!(status.applied_lsn, 7);
    }

    // -- StreamMessage encode/decode -----------------------------------------

    #[test]
    fn stream_message_wal_batch_roundtrip() {
        let msg = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 42,
            records: vec![
                WalRecord {
                    lsn: 1,
                    timestamp_ms: 100,
                    payload: WalPayload::PageWrite {
                        page_id: 10,
                        data: vec![0xAA; 64],
                    },
                },
                WalRecord {
                    lsn: 2,
                    timestamp_ms: 200,
                    payload: WalPayload::Commit { txn_id: 1 },
                },
                WalRecord {
                    lsn: 3,
                    timestamp_ms: 300,
                    payload: WalPayload::Checkpoint,
                },
            ],
        };
        let bytes = encode_stream_message(&msg);
        // Skip the 4-byte length prefix
        let decoded = decode_stream_message(&bytes[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stream_message_wal_batch_ack_roundtrip() {
        let msg = StreamMessage::WalBatchAck {
            sender_id: 2,
            confirmed_lsn: 99,
            batch_seq: 7,
        };
        let bytes = encode_stream_message(&msg);
        let decoded = decode_stream_message(&bytes[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stream_message_heartbeat_roundtrip() {
        let msg = StreamMessage::ReplicationHeartbeat {
            sender_id: 1,
            role: NodeRole::Primary,
            current_lsn: 50,
            timestamp_ms: 1234567890,
        };
        let bytes = encode_stream_message(&msg);
        let decoded = decode_stream_message(&bytes[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stream_message_start_streaming_roundtrip() {
        let msg = StreamMessage::StartStreaming {
            replica_id: 2,
            from_lsn: 10,
        };
        let bytes = encode_stream_message(&msg);
        let decoded = decode_stream_message(&bytes[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stream_message_streaming_started_roundtrip() {
        let msg = StreamMessage::StreamingStarted {
            primary_id: 1,
            wal_tip_lsn: 100,
        };
        let bytes = encode_stream_message(&msg);
        let decoded = decode_stream_message(&bytes[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stream_message_abort_payload_roundtrip() {
        let msg = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 1,
            records: vec![WalRecord {
                lsn: 1,
                timestamp_ms: 100,
                payload: WalPayload::Abort { txn_id: 42 },
            }],
        };
        let bytes = encode_stream_message(&msg);
        let decoded = decode_stream_message(&bytes[4..]).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn stream_message_decode_empty_fails() {
        let result = decode_stream_message(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn stream_message_decode_unknown_tag_fails() {
        let result = decode_stream_message(&[255]);
        assert!(result.is_err());
    }

    // -- StreamingSender / StreamingReceiver ----------------------------------

    #[test]
    fn streaming_sender_prepare_batch() {
        let mut wal = WalWriter::new();
        for i in 1..=5 {
            wal.append(WalPayload::Commit { txn_id: i });
        }

        let mut sender = StreamingSender::new(1, 2, ReplicationMode::Asynchronous);
        let msg = sender.prepare_batch(&wal, 3).unwrap();

        match &msg {
            StreamMessage::WalBatch {
                sender_id,
                batch_seq,
                records,
            } => {
                assert_eq!(*sender_id, 1);
                assert_eq!(*batch_seq, 1);
                assert_eq!(records.len(), 3);
                assert_eq!(records[0].lsn, 1);
                assert_eq!(records[2].lsn, 3);
            }
            _ => panic!("expected WalBatch"),
        }

        assert_eq!(sender.last_sent_lsn(), 3);
        assert_eq!(sender.batches_sent(), 1);
        assert_eq!(sender.lag_lsns(&wal), 5); // confirmed=0, wal tip=5
    }

    #[test]
    fn streaming_sender_catches_up_after_acks() {
        let mut wal = WalWriter::new();
        for i in 1..=3 {
            wal.append(WalPayload::Commit { txn_id: i });
        }

        let mut sender = StreamingSender::new(1, 2, ReplicationMode::Synchronous);
        let _msg = sender.prepare_batch(&wal, 100).unwrap();

        assert!(!sender.is_caught_up(&wal));

        let ack = StreamMessage::WalBatchAck {
            sender_id: 2,
            confirmed_lsn: 3,
            batch_seq: 1,
        };
        sender.handle_ack(&ack);

        assert!(sender.is_caught_up(&wal));
        assert_eq!(sender.last_confirmed_lsn(), 3);
        assert_eq!(sender.batches_acked(), 1);
    }

    #[test]
    fn streaming_sender_no_batch_when_caught_up() {
        let wal = WalWriter::new();
        let mut sender = StreamingSender::new(1, 2, ReplicationMode::Asynchronous);
        assert!(sender.prepare_batch(&wal, 100).is_none());
    }

    #[test]
    fn streaming_receiver_applies_batch() {
        let mut receiver = StreamingReceiver::new(2, 1);

        let batch_msg = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 1,
            records: vec![
                WalRecord {
                    lsn: 1,
                    timestamp_ms: 100,
                    payload: WalPayload::PageWrite {
                        page_id: 1,
                        data: vec![1, 2, 3],
                    },
                },
                WalRecord {
                    lsn: 2,
                    timestamp_ms: 200,
                    payload: WalPayload::Commit { txn_id: 1 },
                },
            ],
        };

        let ack = receiver.handle_batch(&batch_msg).unwrap();
        match &ack {
            StreamMessage::WalBatchAck {
                sender_id,
                confirmed_lsn,
                batch_seq,
            } => {
                assert_eq!(*sender_id, 2);
                assert_eq!(*confirmed_lsn, 2);
                assert_eq!(*batch_seq, 1);
            }
            _ => panic!("expected WalBatchAck"),
        }

        assert_eq!(receiver.applied_lsn(), 2);
        assert_eq!(receiver.wal().latest_lsn(), 2);
        assert_eq!(receiver.batches_received(), 1);
    }

    #[test]
    fn streaming_receiver_rejects_out_of_order() {
        let mut receiver = StreamingReceiver::new(2, 1);

        // Apply first record
        let batch1 = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 1,
            records: vec![WalRecord {
                lsn: 1,
                timestamp_ms: 100,
                payload: WalPayload::Commit { txn_id: 1 },
            }],
        };
        receiver.handle_batch(&batch1).unwrap();

        // Try to skip LSN 2 — should fail
        let bad_batch = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 2,
            records: vec![WalRecord {
                lsn: 5,
                timestamp_ms: 500,
                payload: WalPayload::Commit { txn_id: 5 },
            }],
        };
        let result = receiver.handle_batch(&bad_batch);
        assert!(matches!(
            result,
            Err(ReplicationError::StaleRecord {
                expected: 2,
                got: 5
            })
        ));
    }

    #[test]
    fn streaming_receiver_heartbeat_and_staleness() {
        let mut receiver = StreamingReceiver::new(2, 1);

        // Initially stale (no heartbeat received, now >= timeout)
        assert!(receiver.is_primary_stale(5000, 5000));

        // Receive heartbeat
        let hb = StreamMessage::ReplicationHeartbeat {
            sender_id: 1,
            role: NodeRole::Primary,
            current_lsn: 10,
            timestamp_ms: 1000,
        };
        receiver.handle_heartbeat(&hb);

        // Not stale at 1500 with 1000ms timeout
        assert!(!receiver.is_primary_stale(1500, 1000));

        // Stale at 2100 with 1000ms timeout
        assert!(receiver.is_primary_stale(2100, 1000));
    }

    #[test]
    fn streaming_receiver_request_and_start() {
        let mut receiver = StreamingReceiver::new(2, 1);
        assert!(!receiver.is_streaming());

        let req = receiver.request_streaming();
        assert!(matches!(
            req,
            StreamMessage::StartStreaming {
                replica_id: 2,
                from_lsn: 0
            }
        ));

        let started = StreamMessage::StreamingStarted {
            primary_id: 1,
            wal_tip_lsn: 100,
        };
        receiver.handle_streaming_started(&started);
        assert!(receiver.is_streaming());
    }

    // -- Full streaming replication cycle ------------------------------------

    #[test]
    fn full_streaming_replication_cycle() {
        // Set up primary WAL with some data
        let mut primary_mgr = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        for i in 1..=5 {
            primary_mgr
                .write(WalPayload::PageWrite {
                    page_id: i,
                    data: vec![i as u8; 16],
                })
                .unwrap();
        }
        primary_mgr.write(WalPayload::Commit { txn_id: 1 }).unwrap();

        // Create sender and receiver
        let mut sender = StreamingSender::new(1, 2, ReplicationMode::Asynchronous);
        let mut receiver = StreamingReceiver::new(2, 1);

        // Step 1: Replica requests streaming
        let req = receiver.request_streaming();
        assert!(matches!(req, StreamMessage::StartStreaming { .. }));

        // Step 2: Primary acknowledges
        let started = StreamMessage::StreamingStarted {
            primary_id: 1,
            wal_tip_lsn: primary_mgr.wal().latest_lsn(),
        };
        receiver.handle_streaming_started(&started);
        assert!(receiver.is_streaming());

        // Step 3: Primary sends first batch
        let batch1 = sender.prepare_batch(primary_mgr.wal(), 3).unwrap();
        assert_eq!(sender.batches_sent(), 1);

        // Step 4: Replica applies and acknowledges
        let ack1 = receiver.handle_batch(&batch1).unwrap();
        assert_eq!(receiver.applied_lsn(), 3);
        sender.handle_ack(&ack1);
        assert_eq!(sender.last_confirmed_lsn(), 3);

        // Step 5: Primary sends remaining records
        let batch2 = sender.prepare_batch(primary_mgr.wal(), 100).unwrap();
        let ack2 = receiver.handle_batch(&batch2).unwrap();
        sender.handle_ack(&ack2);

        // Both sides agree: fully caught up
        assert!(sender.is_caught_up(primary_mgr.wal()));
        assert_eq!(receiver.applied_lsn(), 6);
        assert_eq!(sender.lag_lsns(primary_mgr.wal()), 0);

        // Step 6: Heartbeat exchange
        let primary_hb = sender.heartbeat(6, 1000);
        receiver.handle_heartbeat(&primary_hb);
        assert!(!receiver.is_primary_stale(1500, 1000));

        // Step 7: Status reports
        let primary_report = ReplicationStatusReport::from_sender(&primary_mgr, &sender);
        assert_eq!(primary_report.role, NodeRole::Primary);
        assert_eq!(primary_report.replication_lag_lsns, 0);
        assert_eq!(primary_report.batches_sent, 2);

        let replica_report = ReplicationStatusReport::from_receiver(&receiver, 6);
        assert_eq!(replica_report.role, NodeRole::Replica);
        assert_eq!(replica_report.replication_lag_lsns, 0);
        assert_eq!(replica_report.batches_received, 2);

        // Step 8: Status report rows
        let rows = primary_report.as_rows();
        assert_eq!(rows.len(), 12);
        assert_eq!(rows[0].0, "node_id");
    }

    #[test]
    fn streaming_sender_heartbeat_message() {
        let sender = StreamingSender::new(1, 2, ReplicationMode::Synchronous);
        let hb = sender.heartbeat(42, 9999);
        assert_eq!(
            hb,
            StreamMessage::ReplicationHeartbeat {
                sender_id: 1,
                role: NodeRole::Primary,
                current_lsn: 42,
                timestamp_ms: 9999,
            }
        );
    }

    #[test]
    fn streaming_receiver_heartbeat_message() {
        let receiver = StreamingReceiver::new(2, 1);
        let hb = receiver.heartbeat(5000);
        assert_eq!(
            hb,
            StreamMessage::ReplicationHeartbeat {
                sender_id: 2,
                role: NodeRole::Replica,
                current_lsn: 0,
                timestamp_ms: 5000,
            }
        );
    }

    #[test]
    fn streaming_receiver_lag_from() {
        let mut receiver = StreamingReceiver::new(2, 1);
        assert_eq!(receiver.lag_from(100), 100);

        let batch = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 1,
            records: vec![WalRecord {
                lsn: 1,
                timestamp_ms: 100,
                payload: WalPayload::Commit { txn_id: 1 },
            }],
        };
        receiver.handle_batch(&batch).unwrap();
        assert_eq!(receiver.lag_from(100), 99);
    }

    #[test]
    fn replication_manager_new_accessors() {
        let mut mgr = ReplicationManager::new_primary(1, ReplicationMode::Synchronous);
        assert_eq!(mgr.node_id(), 1);
        assert_eq!(mgr.applied_lsn(), 0);

        mgr.write(WalPayload::Commit { txn_id: 1 }).unwrap();
        assert_eq!(mgr.applied_lsn(), 1);

        mgr.record_heartbeat(2, 500);
    }

    // ── Cluster replication status tests (SHOW REPLICATION STATUS) ──

    #[test]
    fn cluster_status_standalone() {
        let report = ClusterReplicationStatus::standalone(1);
        assert_eq!(report.primary_node_id, 1);
        assert!(report.replicas.is_empty());
        let rows = report.as_result_rows();
        assert_eq!(rows.len(), 1); // Just the header row
        assert_eq!(rows[0][3].1, "0"); // replica_count = 0
    }

    #[test]
    fn cluster_status_with_replica() {
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        primary.attach_replica(2);
        primary
            .write(WalPayload::PageWrite {
                page_id: 1,
                data: vec![1; 32],
            })
            .unwrap();
        primary.write(WalPayload::Commit { txn_id: 1 }).unwrap();

        let report = ClusterReplicationStatus::from_primary(&primary);
        assert_eq!(report.primary_node_id, 1);
        assert_eq!(report.primary_wal_lsn, 2);
        assert_eq!(report.replicas.len(), 1);
        let replica = &report.replicas[0];
        assert_eq!(replica.node_id, 2);
        assert_eq!(replica.state, StreamingState::CatchingUp);
        assert!(replica.is_connected);
        assert!(replica.lag_lsns > 0);
        assert!(replica.lag_bytes > 0);
    }

    #[test]
    fn cluster_status_caught_up() {
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Synchronous);
        primary.attach_replica(2);
        primary.write(WalPayload::Commit { txn_id: 1 }).unwrap();

        // Confirm all records
        let batch = primary.prepare_replication_batch();
        assert!(!batch.is_empty());
        primary.confirm_replication(batch.last().unwrap().lsn);

        let report = ClusterReplicationStatus::from_primary(&primary);
        assert_eq!(report.replicas[0].state, StreamingState::Streaming);
        assert_eq!(report.replicas[0].lag_lsns, 0);
    }

    #[test]
    fn cluster_status_disconnected() {
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        primary.attach_replica(2);
        primary.write(WalPayload::Commit { txn_id: 1 }).unwrap();
        primary.local_state.is_connected = false;

        let report = ClusterReplicationStatus::from_primary(&primary);
        assert_eq!(report.replicas[0].state, StreamingState::Disconnected);
        assert!(!report.replicas[0].is_connected);
    }

    #[test]
    fn cluster_status_as_rows_format() {
        let mut primary = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        primary.attach_replica(2);
        primary.write(WalPayload::Commit { txn_id: 1 }).unwrap();

        let report = ClusterReplicationStatus::from_primary(&primary);
        let rows = report.as_result_rows();
        assert_eq!(rows.len(), 2); // header + 1 replica
        assert_eq!(rows[0][0].0, "primary_node_id");
        assert_eq!(rows[1][0].0, "replica_node_id");
        assert_eq!(rows[1][0].1, "2");
    }

    #[test]
    fn replica_state_display() {
        assert_eq!(StreamingState::Streaming.to_string(), "streaming");
        assert_eq!(StreamingState::CatchingUp.to_string(), "catching_up");
        assert_eq!(StreamingState::Disconnected.to_string(), "disconnected");
    }

    // -- TCP Replication Bridge (3.3) ----------------------------------------

    #[test]
    fn repl_message_roundtrip_wal_batch() {
        let records = vec![
            WalRecord {
                lsn: 1,
                timestamp_ms: 100,
                payload: WalPayload::PageWrite {
                    page_id: 10,
                    data: vec![0xAA; 32],
                },
            },
            WalRecord {
                lsn: 2,
                timestamp_ms: 200,
                payload: WalPayload::Commit { txn_id: 42 },
            },
            WalRecord {
                lsn: 3,
                timestamp_ms: 300,
                payload: WalPayload::Abort { txn_id: 43 },
            },
            WalRecord {
                lsn: 4,
                timestamp_ms: 400,
                payload: WalPayload::Checkpoint,
            },
        ];
        let msg = ReplicationMessage::WalBatch {
            records: records.clone(),
        };
        let bytes = msg.to_bytes();
        let decoded = ReplicationMessage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn repl_message_roundtrip_confirm() {
        let msg = ReplicationMessage::Confirm { applied_lsn: 42 };
        let decoded = ReplicationMessage::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn repl_message_roundtrip_heartbeat() {
        let msg = ReplicationMessage::Heartbeat { primary_lsn: 100 };
        let decoded = ReplicationMessage::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn repl_message_roundtrip_heartbeat_response() {
        let msg = ReplicationMessage::HeartbeatResponse { replica_lsn: 99 };
        let decoded = ReplicationMessage::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn repl_message_roundtrip_auth() {
        let msg = ReplicationMessage::Auth {
            token: "secret-token".into(),
        };
        let decoded = ReplicationMessage::from_bytes(&msg.to_bytes()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn repl_message_decode_errors() {
        assert!(ReplicationMessage::from_bytes(&[]).is_err());
        assert!(ReplicationMessage::from_bytes(&[255]).is_err());
        assert!(ReplicationMessage::from_bytes(&[2]).is_err()); // truncated confirm
    }

    #[test]
    fn tcp_bridge_prepare_batch() {
        let mut wal = WalWriter::new();
        wal.append(WalPayload::Commit { txn_id: 1 });
        wal.append(WalPayload::Commit { txn_id: 2 });
        wal.append(WalPayload::Commit { txn_id: 3 });

        let mut bridge = TcpReplicationBridge::new(1, NodeRole::Primary);
        bridge.set_batch_size(2);

        let msg = bridge.prepare_wal_batch(&wal).unwrap();
        match &msg {
            ReplicationMessage::WalBatch { records } => assert_eq!(records.len(), 2),
            _ => panic!("expected WalBatch"),
        }
        assert_eq!(bridge.last_sent_lsn(), 2);

        // Second batch gets the remaining record
        let msg = bridge.prepare_wal_batch(&wal).unwrap();
        match &msg {
            ReplicationMessage::WalBatch { records } => assert_eq!(records.len(), 1),
            _ => panic!("expected WalBatch"),
        }
        assert_eq!(bridge.last_sent_lsn(), 3);

        // No more records
        assert!(bridge.prepare_wal_batch(&wal).is_none());
    }

    #[test]
    fn tcp_bridge_handle_wal_batch() {
        let mut bridge = TcpReplicationBridge::new(2, NodeRole::Replica);
        let batch = ReplicationMessage::WalBatch {
            records: vec![WalRecord {
                lsn: 5,
                timestamp_ms: 500,
                payload: WalPayload::Commit { txn_id: 1 },
            }],
        };
        let response = bridge.handle_message(&batch);
        assert!(response.is_some());
        match response.unwrap() {
            ReplicationMessage::Confirm { applied_lsn } => assert_eq!(applied_lsn, 5),
            _ => panic!("expected Confirm"),
        }
        assert_eq!(bridge.last_received_lsn(), 5);
    }

    #[test]
    fn tcp_bridge_handle_confirm() {
        let mut bridge = TcpReplicationBridge::new(1, NodeRole::Primary);
        let confirm = ReplicationMessage::Confirm { applied_lsn: 10 };
        let response = bridge.handle_message(&confirm);
        assert!(response.is_none()); // Primary doesn't respond to confirms
        assert_eq!(bridge.last_received_lsn(), 10);
    }

    #[test]
    fn tcp_bridge_heartbeat_flow() {
        let mut primary = TcpReplicationBridge::new(1, NodeRole::Primary);
        let mut replica = TcpReplicationBridge::new(2, NodeRole::Replica);

        let hb = primary.heartbeat(50);
        assert_eq!(hb, ReplicationMessage::Heartbeat { primary_lsn: 50 });

        let resp = replica.handle_message(&hb).unwrap();
        assert_eq!(
            resp,
            ReplicationMessage::HeartbeatResponse { replica_lsn: 0 }
        );

        let no_resp = primary.handle_message(&resp);
        assert!(no_resp.is_none());
    }

    #[test]
    fn tcp_bridge_message_counters() {
        let mut bridge = TcpReplicationBridge::new(1, NodeRole::Primary);
        let mut wal = WalWriter::new();
        wal.append(WalPayload::Commit { txn_id: 1 });

        bridge.prepare_wal_batch(&wal);
        bridge.heartbeat(1);
        assert_eq!(bridge.messages_sent(), 2);

        bridge.handle_message(&ReplicationMessage::Confirm { applied_lsn: 1 });
        assert_eq!(bridge.messages_received(), 1);
    }

    // -- WAL Bridge (storage WAL ↔ replication) --------------------------------

    #[test]
    fn wal_bridge_page_write_roundtrip() {
        // Create a storage WAL record with a page write
        let mut page_data = Box::new([0u8; crate::storage::page::PAGE_SIZE]);
        page_data[0] = 0xAA;
        page_data[1] = 0xBB;
        page_data[100] = 0xFF;

        let storage_rec = crate::storage::wal::WalRecord {
            lsn: 42,
            txn_id: 7,
            record_type: crate::storage::wal::RECORD_PAGE_WRITE,
            page_id: 5,
            page_image: Some(page_data),
        };

        // Convert to replication format
        let repl_rec = from_storage_wal_record(&storage_rec);
        assert_eq!(repl_rec.lsn, 42);
        match &repl_rec.payload {
            WalPayload::PageWrite { page_id, data } => {
                assert_eq!(*page_id, 5);
                assert_eq!(data[0], 0xAA);
                assert_eq!(data[1], 0xBB);
                assert_eq!(data[100], 0xFF);
            }
            _ => panic!("expected PageWrite"),
        }

        // Convert back to storage format
        let back = to_storage_wal_record(&repl_rec);
        assert_eq!(back.lsn, 42);
        assert_eq!(back.record_type, crate::storage::wal::RECORD_PAGE_WRITE);
        assert_eq!(back.page_id, 5);
        let img = back.page_image.unwrap();
        assert_eq!(img[0], 0xAA);
        assert_eq!(img[1], 0xBB);
        assert_eq!(img[100], 0xFF);
    }

    #[test]
    fn wal_bridge_commit_roundtrip() {
        let storage_rec = crate::storage::wal::WalRecord {
            lsn: 10,
            txn_id: 99,
            record_type: crate::storage::wal::RECORD_COMMIT,
            page_id: 0,
            page_image: None,
        };

        let repl_rec = from_storage_wal_record(&storage_rec);
        assert_eq!(repl_rec.lsn, 10);
        assert!(matches!(
            repl_rec.payload,
            WalPayload::Commit { txn_id: 99 }
        ));

        let back = to_storage_wal_record(&repl_rec);
        assert_eq!(back.lsn, 10);
        assert_eq!(back.txn_id, 99);
        assert_eq!(back.record_type, crate::storage::wal::RECORD_COMMIT);
        assert!(back.page_image.is_none());
    }

    #[test]
    fn wal_bridge_abort_roundtrip() {
        let storage_rec = crate::storage::wal::WalRecord {
            lsn: 20,
            txn_id: 55,
            record_type: crate::storage::wal::RECORD_ABORT,
            page_id: 0,
            page_image: None,
        };

        let repl_rec = from_storage_wal_record(&storage_rec);
        assert!(matches!(repl_rec.payload, WalPayload::Abort { txn_id: 55 }));

        let back = to_storage_wal_record(&repl_rec);
        assert_eq!(back.record_type, crate::storage::wal::RECORD_ABORT);
        assert_eq!(back.txn_id, 55);
    }

    #[test]
    fn wal_bridge_checkpoint_roundtrip() {
        let storage_rec = crate::storage::wal::WalRecord {
            lsn: 30,
            txn_id: 0,
            record_type: crate::storage::wal::RECORD_CHECKPOINT,
            page_id: 0,
            page_image: None,
        };

        let repl_rec = from_storage_wal_record(&storage_rec);
        assert!(matches!(repl_rec.payload, WalPayload::Checkpoint));

        let back = to_storage_wal_record(&repl_rec);
        assert_eq!(back.record_type, crate::storage::wal::RECORD_CHECKPOINT);
    }

    #[test]
    fn wal_bridge_unknown_record_type_maps_to_checkpoint() {
        let storage_rec = crate::storage::wal::WalRecord {
            lsn: 1,
            txn_id: 0,
            record_type: 255, // unknown
            page_id: 0,
            page_image: None,
        };

        let repl_rec = from_storage_wal_record(&storage_rec);
        assert!(matches!(repl_rec.payload, WalPayload::Checkpoint));
    }

    #[test]
    fn wal_bridge_apply_to_storage_wal() {
        // Create a temp WAL file
        let dir = std::env::temp_dir().join(format!("nucleus_test_bridge_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("test.wal");
        let storage_wal = crate::storage::wal::Wal::open(&wal_path).unwrap();

        // Create replication records with page writes
        let repl_records = vec![
            WalRecord {
                lsn: 1,
                timestamp_ms: 100,
                payload: WalPayload::PageWrite {
                    page_id: 10,
                    data: vec![0xAA; 64],
                },
            },
            WalRecord {
                lsn: 2,
                timestamp_ms: 200,
                payload: WalPayload::Commit { txn_id: 1 },
            },
            WalRecord {
                lsn: 3,
                timestamp_ms: 300,
                payload: WalPayload::PageWrite {
                    page_id: 20,
                    data: vec![0xBB; 128],
                },
            },
        ];

        // Apply — only PageWrite records should be written
        let applied = WalBridge::apply_to_storage_wal(&storage_wal, &repl_records);
        assert_eq!(applied, 2); // 2 page writes, 1 commit skipped

        // Verify the storage WAL has the records
        storage_wal.sync().unwrap();
        let disk_records = crate::storage::wal::read_wal_records(&wal_path).unwrap();
        assert_eq!(disk_records.len(), 2);
        assert_eq!(disk_records[0].page_id, 10);
        assert_eq!(disk_records[1].page_id, 20);

        // Verify page data was preserved
        let img = disk_records[0].page_image.as_ref().unwrap();
        assert_eq!(img[0], 0xAA);
        assert_eq!(img[63], 0xAA);

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_bridge_forward_new_records() {
        // Create a storage WAL with some records
        let dir = std::env::temp_dir().join(format!("nucleus_test_fwd_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("test.wal");
        let storage_wal = crate::storage::wal::Wal::open(&wal_path).unwrap();

        // Write 3 page writes to the storage WAL
        let page = [0u8; crate::storage::page::PAGE_SIZE];
        storage_wal.log_page_write(1, 100, &page).unwrap();
        storage_wal.log_page_write(1, 101, &page).unwrap();
        storage_wal.log_commit(1).unwrap();
        storage_wal.sync().unwrap();

        // Create a replication manager and bridge
        let mut repl_mgr = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        let mut bridge = WalBridge::new();
        assert_eq!(bridge.last_forwarded_lsn(), 0);

        // Forward all records
        let forwarded = bridge.forward_new_records(&wal_path, &mut repl_mgr);
        assert_eq!(forwarded, 3);
        assert_eq!(bridge.last_forwarded_lsn(), 3);
        assert_eq!(repl_mgr.wal().latest_lsn(), 3);

        // Forward again — no new records
        let forwarded2 = bridge.forward_new_records(&wal_path, &mut repl_mgr);
        assert_eq!(forwarded2, 0);

        // Write more to storage WAL
        storage_wal.log_page_write(2, 200, &page).unwrap();
        storage_wal.sync().unwrap();

        // Forward picks up only the new record
        let forwarded3 = bridge.forward_new_records(&wal_path, &mut repl_mgr);
        assert_eq!(forwarded3, 1);
        assert_eq!(bridge.last_forwarded_lsn(), 4);
        assert_eq!(repl_mgr.wal().latest_lsn(), 4);

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_bridge_forward_missing_file_returns_zero() {
        let mut repl_mgr = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        let mut bridge = WalBridge::new();

        let nonexistent = std::path::Path::new("/tmp/nucleus_nonexistent_wal_99999.wal");
        let forwarded = bridge.forward_new_records(nonexistent, &mut repl_mgr);
        assert_eq!(forwarded, 0);
    }

    // -- WalNotifier (broadcast channel bridge) --------------------------------

    #[tokio::test]
    async fn wal_notifier_basic_subscribe_and_notify() {
        let mut notifier = WalNotifier::new(16);

        // Subscribe before notifying
        let mut rx = notifier.subscribe();

        let rec = WalRecord {
            lsn: 1,
            timestamp_ms: 100,
            payload: WalPayload::Commit { txn_id: 1 },
        };
        let receivers = notifier.notify(rec.clone());
        assert_eq!(receivers, 1);
        assert_eq!(notifier.last_notified_lsn(), 1);

        // Receive the notification
        let notification = rx.try_recv().unwrap();
        assert_eq!(notification.record.lsn, 1);
        assert!(matches!(
            notification.record.payload,
            WalPayload::Commit { txn_id: 1 }
        ));
    }

    #[tokio::test]
    async fn wal_notifier_skips_duplicate_lsns() {
        let mut notifier = WalNotifier::new(16);
        let _rx = notifier.subscribe();

        let rec = WalRecord {
            lsn: 5,
            timestamp_ms: 500,
            payload: WalPayload::Checkpoint,
        };
        assert_eq!(notifier.notify(rec.clone()), 1);
        // Sending same LSN again should be skipped
        assert_eq!(notifier.notify(rec), 0);
        assert_eq!(notifier.last_notified_lsn(), 5);
    }

    #[tokio::test]
    async fn wal_notifier_multiple_subscribers() {
        let mut notifier = WalNotifier::new(16);
        let mut rx1 = notifier.subscribe();
        let mut rx2 = notifier.subscribe();
        assert_eq!(notifier.receiver_count(), 2);

        let rec = WalRecord {
            lsn: 1,
            timestamp_ms: 100,
            payload: WalPayload::Commit { txn_id: 1 },
        };
        let receivers = notifier.notify(rec);
        assert_eq!(receivers, 2);

        assert_eq!(rx1.try_recv().unwrap().record.lsn, 1);
        assert_eq!(rx2.try_recv().unwrap().record.lsn, 1);
    }

    #[tokio::test]
    async fn wal_notifier_no_subscribers_returns_zero() {
        let mut notifier = WalNotifier::new(16);
        // No subscribers
        let rec = WalRecord {
            lsn: 1,
            timestamp_ms: 100,
            payload: WalPayload::Checkpoint,
        };
        assert_eq!(notifier.notify(rec), 0);
        // LSN still advances even with no subscribers
        assert_eq!(notifier.last_notified_lsn(), 1);
    }

    #[tokio::test]
    async fn wal_notifier_from_storage_wal() {
        let dir =
            std::env::temp_dir().join(format!("nucleus_test_notifier_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("test.wal");
        let storage_wal = crate::storage::wal::Wal::open(&wal_path).unwrap();

        // Write records to storage WAL
        let page = [0u8; crate::storage::page::PAGE_SIZE];
        storage_wal.log_page_write(1, 100, &page).unwrap();
        storage_wal.log_commit(1).unwrap();
        storage_wal.sync().unwrap();

        let mut notifier = WalNotifier::new(16);
        let mut rx = notifier.subscribe();

        let count = notifier.notify_from_storage_wal(&wal_path);
        assert_eq!(count, 2);
        assert_eq!(notifier.last_notified_lsn(), 2);

        // Should receive both notifications
        let n1 = rx.try_recv().unwrap();
        let n2 = rx.try_recv().unwrap();
        assert_eq!(n1.record.lsn, 1);
        assert_eq!(n2.record.lsn, 2);

        // Second call should find no new records
        let count2 = notifier.notify_from_storage_wal(&wal_path);
        assert_eq!(count2, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- Framing (message read/write) -----------------------------------------

    #[tokio::test]
    async fn framing_write_and_read_roundtrip() {
        let msg = ReplicationMessage::WalBatch {
            records: vec![
                WalRecord {
                    lsn: 1,
                    timestamp_ms: 100,
                    payload: WalPayload::PageWrite {
                        page_id: 10,
                        data: vec![0xAA; 32],
                    },
                },
                WalRecord {
                    lsn: 2,
                    timestamp_ms: 200,
                    payload: WalPayload::Commit { txn_id: 42 },
                },
            ],
        };

        // Write to a buffer
        let mut buf = Vec::new();
        framing::write_message(&mut buf, &msg).await.unwrap();

        // Read it back
        let mut cursor = std::io::Cursor::new(buf);
        let decoded = framing::read_message(&mut cursor).await.unwrap().unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn framing_read_eof_returns_none() {
        let buf: Vec<u8> = Vec::new();
        let mut cursor = std::io::Cursor::new(buf);
        let result = framing::read_message(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn framing_multiple_messages() {
        let msgs = vec![
            ReplicationMessage::Heartbeat { primary_lsn: 42 },
            ReplicationMessage::Confirm { applied_lsn: 10 },
            ReplicationMessage::HeartbeatResponse { replica_lsn: 5 },
        ];

        let mut buf = Vec::new();
        for msg in &msgs {
            framing::write_message(&mut buf, msg).await.unwrap();
        }

        let mut cursor = std::io::Cursor::new(buf);
        for expected in &msgs {
            let decoded = framing::read_message(&mut cursor).await.unwrap().unwrap();
            assert_eq!(&decoded, expected);
        }

        // No more messages
        let eof = framing::read_message(&mut cursor).await.unwrap();
        assert!(eof.is_none());
    }

    // -- TCP transport (ReplicationServer / ReplicationClient) -----------------

    #[tokio::test]
    async fn tcp_transport_server_client_exchange() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        // Set up a notifier
        let notifier = Arc::new(Mutex::new(WalNotifier::new(64)));

        // Use port 0 to let the OS choose an available port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();
        let addr_str = actual_addr.to_string();

        // Use a oneshot to signal when the server has subscribed to the notifier
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn the server-side accept loop
        let notifier_for_server = notifier.clone();
        let server_handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let rx = {
                let n = notifier_for_server.lock().await;
                n.subscribe()
            };

            // Signal that we've subscribed
            let _ = ready_tx.send(());

            let (reader, writer) = tokio::io::split(stream);
            let writer = Arc::new(Mutex::new(writer));

            // Spawn sender: forward notifications
            let writer_clone = writer.clone();
            let mut rx = rx;
            let sender_task = tokio::spawn(async move {
                let mut sent = 0u32;
                loop {
                    match rx.recv().await {
                        Ok(notification) => {
                            let msg = ReplicationMessage::WalBatch {
                                records: vec![notification.record],
                            };
                            let mut w = writer_clone.lock().await;
                            framing::write_message(&mut *w, &msg).await.unwrap();
                            sent += 1;
                            if sent >= 3 {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });

            // Reader: collect confirms
            let mut reader = reader;
            let mut confirms = Vec::new();
            for _ in 0..3 {
                if let Ok(Some(msg)) = framing::read_message(&mut reader).await {
                    confirms.push(msg);
                }
            }

            sender_task.await.ok();
            confirms
        });

        // Give server a moment to be ready for accept
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect the client side
        let stream = tokio::net::TcpStream::connect(&addr_str).await.unwrap();
        let (mut reader, mut writer) = tokio::io::split(stream);

        // Wait for the server to subscribe before sending notifications
        ready_rx.await.unwrap();

        // Publish 3 records via the notifier
        {
            let mut n = notifier.lock().await;
            for i in 1..=3u64 {
                n.notify(WalRecord {
                    lsn: i,
                    timestamp_ms: i * 100,
                    payload: WalPayload::Commit { txn_id: i },
                });
            }
        }

        // Client receives and acknowledges
        let mut received_lsns = Vec::new();
        for _ in 0..3 {
            let msg = framing::read_message(&mut reader).await.unwrap().unwrap();
            match &msg {
                ReplicationMessage::WalBatch { records } => {
                    let lsn = records.last().unwrap().lsn;
                    received_lsns.push(lsn);
                    let confirm = ReplicationMessage::Confirm { applied_lsn: lsn };
                    framing::write_message(&mut writer, &confirm).await.unwrap();
                }
                other => panic!("expected WalBatch, got: {other:?}"),
            }
        }

        assert_eq!(received_lsns, vec![1, 2, 3]);

        // Server should have received 3 confirms
        let confirms = server_handle.await.unwrap();
        assert_eq!(confirms.len(), 3);
        for (i, confirm) in confirms.iter().enumerate() {
            match confirm {
                ReplicationMessage::Confirm { applied_lsn } => {
                    assert_eq!(*applied_lsn, (i + 1) as u64);
                }
                other => panic!("expected Confirm, got: {other:?}"),
            }
        }
    }
}
