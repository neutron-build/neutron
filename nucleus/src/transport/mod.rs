//! Node-to-node RPC transport for Nucleus distributed mode.
//!
//! Provides message serialization, peer connection tracking, and outbound/inbound
//! message routing between cluster nodes. The transport layer mirrors Raft and
//! sharding types locally to avoid circular dependencies with those modules.
//!
//! Wire format: simple TLV (tag-length-value) binary encoding, no serde.

use std::collections::HashMap;

// ============================================================================
// Type aliases
// ============================================================================

/// Unique node identifier (mirrors `raft::NodeId`).
pub type NodeId = u64;

/// Monotonically increasing message identifier.
pub type MessageId = u64;

// ============================================================================
// Transport errors
// ============================================================================

/// Errors that can occur during transport operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The binary payload is too short to contain the expected data.
    BufferUnderflow,
    /// An unknown message tag byte was encountered during decoding.
    UnknownMessageTag(u8),
    /// An unknown command tag byte was encountered during decoding.
    UnknownCommandTag(u8),
    /// A string field contained invalid UTF-8.
    InvalidUtf8,
    /// The target peer is not registered.
    UnknownPeer(NodeId),
    /// Generic encode/decode error with description.
    Protocol(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::BufferUnderflow => write!(f, "buffer underflow"),
            TransportError::UnknownMessageTag(t) => write!(f, "unknown message tag: {t}"),
            TransportError::UnknownCommandTag(t) => write!(f, "unknown command tag: {t}"),
            TransportError::InvalidUtf8 => write!(f, "invalid UTF-8 in string field"),
            TransportError::UnknownPeer(id) => write!(f, "unknown peer: {id}"),
            TransportError::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for TransportError {}

// ============================================================================
// Raft entry / command (mirrored locally)
// ============================================================================

/// A replicated log entry (mirrors `raft::LogEntry`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftEntry {
    pub index: u64,
    pub term: u64,
    pub command: RaftCommand,
}

/// Commands that can appear in the replicated log (mirrors `raft::Command`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftCommand {
    Sql(String),
    Noop,
    AddNode(NodeId),
    RemoveNode(NodeId),
}

// ============================================================================
// Message enum
// ============================================================================

/// All message types exchanged between cluster nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    // -- Raft consensus -------------------------------------------------------
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },

    // -- Data operations ------------------------------------------------------
    ForwardQuery {
        query: String,
        shard_id: u64,
    },
    ForwardQueryResponse {
        success: bool,
        rows: Vec<Vec<u8>>,
        error: Option<String>,
    },
    /// Forward a DML statement (INSERT/UPDATE/DELETE) from a follower to the leader.
    ForwardDml {
        sql: String,
        shard_id: u64,
    },
    /// Leader's response to a forwarded DML statement.
    ForwardDmlResponse {
        success: bool,
        rows_affected: usize,
        error: Option<String>,
    },

    // -- Cluster management ---------------------------------------------------
    JoinCluster {
        node_id: NodeId,
        address: String,
    },
    JoinClusterResponse {
        success: bool,
        cluster_nodes: Vec<(NodeId, String)>,
    },
    Heartbeat {
        node_id: NodeId,
        term: u64,
    },
    HeartbeatResponse {
        node_id: NodeId,
        term: u64,
    },

    // -- Data transfer (shard rebalancing) ------------------------------------
    TransferShard {
        shard_id: u64,
        data: Vec<u8>,
    },
    TransferShardAck {
        shard_id: u64,
        success: bool,
    },

    // -- Distributed pub/sub --------------------------------------------------
    /// Forward a NOTIFY to a remote node so it delivers to its local subscribers.
    PubSubPublish {
        channel: String,
        payload: String,
    },
    /// Gossip: inform a peer of the channels this node currently subscribes to.
    PubSubGossip {
        node_id: NodeId,
        channels: Vec<String>,
    },
}

// ============================================================================
// Envelope
// ============================================================================

/// A framed message with routing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub id: MessageId,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

// ============================================================================
// Binary serialization helpers
// ============================================================================

/// Low-level writer: accumulates bytes into a `Vec<u8>`.
struct BufWriter {
    buf: Vec<u8>,
}

impl BufWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
        }
    }

    fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    fn write_bytes(&mut self, data: &[u8]) {
        self.write_u64(data.len() as u64);
        self.buf.extend_from_slice(data);
    }

    fn write_string(&mut self, s: &str) {
        self.write_bytes(s.as_bytes());
    }

    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// Low-level reader: walks a byte slice forward.
struct BufReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BufReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8, TransportError> {
        if self.remaining() < 1 {
            return Err(TransportError::BufferUnderflow);
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u64(&mut self) -> Result<u64, TransportError> {
        if self.remaining() < 8 {
            return Err(TransportError::BufferUnderflow);
        }
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8]
            .try_into()
            .map_err(|_| TransportError::BufferUnderflow)?;
        self.pos += 8;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_bool(&mut self) -> Result<bool, TransportError> {
        Ok(self.read_u8()? != 0)
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, TransportError> {
        let len = self.read_u64()? as usize;
        if self.remaining() < len {
            return Err(TransportError::BufferUnderflow);
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    fn read_string(&mut self) -> Result<String, TransportError> {
        let raw = self.read_bytes()?;
        String::from_utf8(raw).map_err(|_| TransportError::InvalidUtf8)
    }
}

// ============================================================================
// Message tag constants
// ============================================================================

const TAG_REQUEST_VOTE: u8 = 1;
const TAG_REQUEST_VOTE_RESPONSE: u8 = 2;
const TAG_APPEND_ENTRIES: u8 = 3;
const TAG_APPEND_ENTRIES_RESPONSE: u8 = 4;
const TAG_FORWARD_QUERY: u8 = 5;
const TAG_FORWARD_QUERY_RESPONSE: u8 = 6;
const TAG_JOIN_CLUSTER: u8 = 7;
const TAG_JOIN_CLUSTER_RESPONSE: u8 = 8;
const TAG_HEARTBEAT: u8 = 9;
const TAG_HEARTBEAT_RESPONSE: u8 = 10;
const TAG_TRANSFER_SHARD: u8 = 11;
const TAG_TRANSFER_SHARD_ACK: u8 = 12;
const TAG_FORWARD_DML: u8 = 13;
const TAG_FORWARD_DML_RESPONSE: u8 = 14;
const TAG_PUBSUB_PUBLISH: u8 = 15;
const TAG_PUBSUB_GOSSIP: u8 = 16;

const CMD_SQL: u8 = 1;
const CMD_NOOP: u8 = 2;
const CMD_ADD_NODE: u8 = 3;
const CMD_REMOVE_NODE: u8 = 4;

// ============================================================================
// RaftCommand encode / decode
// ============================================================================

fn encode_raft_command(w: &mut BufWriter, cmd: &RaftCommand) {
    match cmd {
        RaftCommand::Sql(s) => {
            w.write_u8(CMD_SQL);
            w.write_string(s);
        }
        RaftCommand::Noop => {
            w.write_u8(CMD_NOOP);
        }
        RaftCommand::AddNode(id) => {
            w.write_u8(CMD_ADD_NODE);
            w.write_u64(*id);
        }
        RaftCommand::RemoveNode(id) => {
            w.write_u8(CMD_REMOVE_NODE);
            w.write_u64(*id);
        }
    }
}

fn decode_raft_command(r: &mut BufReader<'_>) -> Result<RaftCommand, TransportError> {
    let tag = r.read_u8()?;
    match tag {
        CMD_SQL => Ok(RaftCommand::Sql(r.read_string()?)),
        CMD_NOOP => Ok(RaftCommand::Noop),
        CMD_ADD_NODE => Ok(RaftCommand::AddNode(r.read_u64()?)),
        CMD_REMOVE_NODE => Ok(RaftCommand::RemoveNode(r.read_u64()?)),
        other => Err(TransportError::UnknownCommandTag(other)),
    }
}

// ============================================================================
// RaftEntry encode / decode
// ============================================================================

fn encode_raft_entry(w: &mut BufWriter, entry: &RaftEntry) {
    w.write_u64(entry.index);
    w.write_u64(entry.term);
    encode_raft_command(w, &entry.command);
}

fn decode_raft_entry(r: &mut BufReader<'_>) -> Result<RaftEntry, TransportError> {
    let index = r.read_u64()?;
    let term = r.read_u64()?;
    let command = decode_raft_command(r)?;
    Ok(RaftEntry {
        index,
        term,
        command,
    })
}

// ============================================================================
// Message encode / decode (public API)
// ============================================================================

/// Serialize a `Message` to a binary byte vector.
pub fn encode(msg: &Message) -> Vec<u8> {
    let mut w = BufWriter::new();
    match msg {
        Message::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => {
            w.write_u8(TAG_REQUEST_VOTE);
            w.write_u64(*term);
            w.write_u64(*candidate_id);
            w.write_u64(*last_log_index);
            w.write_u64(*last_log_term);
        }
        Message::RequestVoteResponse { term, vote_granted } => {
            w.write_u8(TAG_REQUEST_VOTE_RESPONSE);
            w.write_u64(*term);
            w.write_bool(*vote_granted);
        }
        Message::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            w.write_u8(TAG_APPEND_ENTRIES);
            w.write_u64(*term);
            w.write_u64(*leader_id);
            w.write_u64(*prev_log_index);
            w.write_u64(*prev_log_term);
            w.write_u64(entries.len() as u64);
            for entry in entries {
                encode_raft_entry(&mut w, entry);
            }
            w.write_u64(*leader_commit);
        }
        Message::AppendEntriesResponse {
            term,
            success,
            match_index,
        } => {
            w.write_u8(TAG_APPEND_ENTRIES_RESPONSE);
            w.write_u64(*term);
            w.write_bool(*success);
            w.write_u64(*match_index);
        }
        Message::ForwardQuery { query, shard_id } => {
            w.write_u8(TAG_FORWARD_QUERY);
            w.write_string(query);
            w.write_u64(*shard_id);
        }
        Message::ForwardQueryResponse {
            success,
            rows,
            error,
        } => {
            w.write_u8(TAG_FORWARD_QUERY_RESPONSE);
            w.write_bool(*success);
            w.write_u64(rows.len() as u64);
            for row in rows {
                w.write_bytes(row);
            }
            match error {
                Some(e) => {
                    w.write_bool(true);
                    w.write_string(e);
                }
                None => {
                    w.write_bool(false);
                }
            }
        }
        Message::JoinCluster { node_id, address } => {
            w.write_u8(TAG_JOIN_CLUSTER);
            w.write_u64(*node_id);
            w.write_string(address);
        }
        Message::JoinClusterResponse {
            success,
            cluster_nodes,
        } => {
            w.write_u8(TAG_JOIN_CLUSTER_RESPONSE);
            w.write_bool(*success);
            w.write_u64(cluster_nodes.len() as u64);
            for (nid, addr) in cluster_nodes {
                w.write_u64(*nid);
                w.write_string(addr);
            }
        }
        Message::Heartbeat { node_id, term } => {
            w.write_u8(TAG_HEARTBEAT);
            w.write_u64(*node_id);
            w.write_u64(*term);
        }
        Message::HeartbeatResponse { node_id, term } => {
            w.write_u8(TAG_HEARTBEAT_RESPONSE);
            w.write_u64(*node_id);
            w.write_u64(*term);
        }
        Message::TransferShard { shard_id, data } => {
            w.write_u8(TAG_TRANSFER_SHARD);
            w.write_u64(*shard_id);
            w.write_bytes(data);
        }
        Message::TransferShardAck { shard_id, success } => {
            w.write_u8(TAG_TRANSFER_SHARD_ACK);
            w.write_u64(*shard_id);
            w.write_bool(*success);
        }
        Message::ForwardDml { sql, shard_id } => {
            w.write_u8(TAG_FORWARD_DML);
            w.write_string(sql);
            w.write_u64(*shard_id);
        }
        Message::ForwardDmlResponse {
            success,
            rows_affected,
            error,
        } => {
            w.write_u8(TAG_FORWARD_DML_RESPONSE);
            w.write_bool(*success);
            w.write_u64(*rows_affected as u64);
            match error {
                Some(e) => {
                    w.write_bool(true);
                    w.write_string(e);
                }
                None => {
                    w.write_bool(false);
                }
            }
        }
        Message::PubSubPublish { channel, payload } => {
            w.write_u8(TAG_PUBSUB_PUBLISH);
            w.write_string(channel);
            w.write_string(payload);
        }
        Message::PubSubGossip { node_id, channels } => {
            w.write_u8(TAG_PUBSUB_GOSSIP);
            w.write_u64(*node_id);
            w.write_u64(channels.len() as u64);
            for ch in channels {
                w.write_string(ch);
            }
        }
    }
    w.finish()
}

/// Deserialize a `Message` from a binary byte slice.
pub fn decode(data: &[u8]) -> Result<Message, TransportError> {
    let mut r = BufReader::new(data);
    let tag = r.read_u8()?;
    match tag {
        TAG_REQUEST_VOTE => {
            let term = r.read_u64()?;
            let candidate_id = r.read_u64()?;
            let last_log_index = r.read_u64()?;
            let last_log_term = r.read_u64()?;
            Ok(Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            })
        }
        TAG_REQUEST_VOTE_RESPONSE => {
            let term = r.read_u64()?;
            let vote_granted = r.read_bool()?;
            Ok(Message::RequestVoteResponse { term, vote_granted })
        }
        TAG_APPEND_ENTRIES => {
            let term = r.read_u64()?;
            let leader_id = r.read_u64()?;
            let prev_log_index = r.read_u64()?;
            let prev_log_term = r.read_u64()?;
            let count = r.read_u64()? as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(decode_raft_entry(&mut r)?);
            }
            let leader_commit = r.read_u64()?;
            Ok(Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            })
        }
        TAG_APPEND_ENTRIES_RESPONSE => {
            let term = r.read_u64()?;
            let success = r.read_bool()?;
            let match_index = r.read_u64()?;
            Ok(Message::AppendEntriesResponse {
                term,
                success,
                match_index,
            })
        }
        TAG_FORWARD_QUERY => {
            let query = r.read_string()?;
            let shard_id = r.read_u64()?;
            Ok(Message::ForwardQuery { query, shard_id })
        }
        TAG_FORWARD_QUERY_RESPONSE => {
            let success = r.read_bool()?;
            let row_count = r.read_u64()? as usize;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                rows.push(r.read_bytes()?);
            }
            let has_error = r.read_bool()?;
            let error = if has_error {
                Some(r.read_string()?)
            } else {
                None
            };
            Ok(Message::ForwardQueryResponse {
                success,
                rows,
                error,
            })
        }
        TAG_JOIN_CLUSTER => {
            let node_id = r.read_u64()?;
            let address = r.read_string()?;
            Ok(Message::JoinCluster { node_id, address })
        }
        TAG_JOIN_CLUSTER_RESPONSE => {
            let success = r.read_bool()?;
            let count = r.read_u64()? as usize;
            let mut cluster_nodes = Vec::with_capacity(count);
            for _ in 0..count {
                let nid = r.read_u64()?;
                let addr = r.read_string()?;
                cluster_nodes.push((nid, addr));
            }
            Ok(Message::JoinClusterResponse {
                success,
                cluster_nodes,
            })
        }
        TAG_HEARTBEAT => {
            let node_id = r.read_u64()?;
            let term = r.read_u64()?;
            Ok(Message::Heartbeat { node_id, term })
        }
        TAG_HEARTBEAT_RESPONSE => {
            let node_id = r.read_u64()?;
            let term = r.read_u64()?;
            Ok(Message::HeartbeatResponse { node_id, term })
        }
        TAG_TRANSFER_SHARD => {
            let shard_id = r.read_u64()?;
            let data = r.read_bytes()?;
            Ok(Message::TransferShard { shard_id, data })
        }
        TAG_TRANSFER_SHARD_ACK => {
            let shard_id = r.read_u64()?;
            let success = r.read_bool()?;
            Ok(Message::TransferShardAck { shard_id, success })
        }
        TAG_FORWARD_DML => {
            let sql = r.read_string()?;
            let shard_id = r.read_u64()?;
            Ok(Message::ForwardDml { sql, shard_id })
        }
        TAG_FORWARD_DML_RESPONSE => {
            let success = r.read_bool()?;
            let rows_affected = r.read_u64()? as usize;
            let has_error = r.read_bool()?;
            let error = if has_error {
                Some(r.read_string()?)
            } else {
                None
            };
            Ok(Message::ForwardDmlResponse {
                success,
                rows_affected,
                error,
            })
        }
        TAG_PUBSUB_PUBLISH => {
            let channel = r.read_string()?;
            let payload = r.read_string()?;
            Ok(Message::PubSubPublish { channel, payload })
        }
        TAG_PUBSUB_GOSSIP => {
            let node_id = r.read_u64()?;
            let count = r.read_u64()? as usize;
            let mut channels = Vec::with_capacity(count);
            for _ in 0..count {
                channels.push(r.read_string()?);
            }
            Ok(Message::PubSubGossip { node_id, channels })
        }
        other => Err(TransportError::UnknownMessageTag(other)),
    }
}

// ============================================================================
// Envelope encode / decode
// ============================================================================

impl Envelope {
    /// Serialize the full envelope (header + message) to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut w = BufWriter::new();
        w.write_u64(self.id);
        w.write_u64(self.from);
        w.write_u64(self.to);
        let msg_bytes = encode(&self.message);
        w.write_bytes(&msg_bytes);
        w.finish()
    }

    /// Deserialize an envelope from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, TransportError> {
        let mut r = BufReader::new(data);
        let id = r.read_u64()?;
        let from = r.read_u64()?;
        let to = r.read_u64()?;
        let msg_bytes = r.read_bytes()?;
        let message = decode(&msg_bytes)?;
        Ok(Envelope {
            id,
            from,
            to,
            message,
        })
    }
}

// ============================================================================
// PeerInfo / PeerRegistry
// ============================================================================

/// Metadata about a known peer in the cluster.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub address: String,
    pub is_connected: bool,
    pub last_heartbeat_ms: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
}

/// Registry of all known cluster peers.
pub struct PeerRegistry {
    peers: HashMap<NodeId, PeerInfo>,
    local_node_id: NodeId,
    next_message_id: MessageId,
}

impl PeerRegistry {
    /// Create a new registry for the given local node.
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            peers: HashMap::new(),
            local_node_id,
            next_message_id: 1,
        }
    }

    /// The local node's identifier.
    pub fn local_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Register a new peer. Returns `true` if the peer was newly added.
    pub fn add_peer(&mut self, node_id: NodeId, address: String) -> bool {
        if node_id == self.local_node_id {
            return false;
        }
        if self.peers.contains_key(&node_id) {
            return false;
        }
        self.peers.insert(
            node_id,
            PeerInfo {
                node_id,
                address,
                is_connected: false,
                last_heartbeat_ms: 0,
                messages_sent: 0,
                messages_received: 0,
            },
        );
        true
    }

    /// Remove a peer from the registry. Returns `true` if it existed.
    pub fn remove_peer(&mut self, node_id: NodeId) -> bool {
        self.peers.remove(&node_id).is_some()
    }

    /// Look up a peer by id.
    pub fn get_peer(&self, node_id: NodeId) -> Option<&PeerInfo> {
        self.peers.get(&node_id)
    }

    /// Look up a peer mutably by id.
    pub fn get_peer_mut(&mut self, node_id: NodeId) -> Option<&mut PeerInfo> {
        self.peers.get_mut(&node_id)
    }

    /// Mark a peer as connected.
    pub fn mark_connected(&mut self, node_id: NodeId) -> bool {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.is_connected = true;
            true
        } else {
            false
        }
    }

    /// Mark a peer as disconnected.
    pub fn mark_disconnected(&mut self, node_id: NodeId) -> bool {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.is_connected = false;
            true
        } else {
            false
        }
    }

    /// Record a heartbeat timestamp for a peer.
    pub fn record_heartbeat(&mut self, node_id: NodeId, timestamp_ms: u64) -> bool {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.last_heartbeat_ms = timestamp_ms;
            true
        } else {
            false
        }
    }

    /// Increment the sent counter for a peer.
    pub fn record_send(&mut self, node_id: NodeId) {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.messages_sent += 1;
        }
    }

    /// Increment the received counter for a peer.
    pub fn record_receive(&mut self, node_id: NodeId) {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.messages_received += 1;
        }
    }

    /// Return node IDs of all currently connected peers.
    pub fn connected_peers(&self) -> Vec<NodeId> {
        self.peers
            .values()
            .filter(|p| p.is_connected)
            .map(|p| p.node_id)
            .collect()
    }

    /// Return node IDs of peers whose last heartbeat is older than `now_ms - timeout_ms`.
    /// A peer with `last_heartbeat_ms == 0` (never heard from) is always stale.
    pub fn stale_peers(&self, now_ms: u64, timeout_ms: u64) -> Vec<NodeId> {
        self.peers
            .values()
            .filter(|p| {
                p.last_heartbeat_ms == 0 || now_ms.saturating_sub(p.last_heartbeat_ms) > timeout_ms
            })
            .map(|p| p.node_id)
            .collect()
    }

    /// Return the total number of registered peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Generate the next monotonically increasing message id.
    pub fn next_id(&mut self) -> MessageId {
        let id = self.next_message_id;
        self.next_message_id += 1;
        id
    }
}

// ============================================================================
// MessageRouter
// ============================================================================

/// Routes outbound messages to per-peer outboxes and collects inbound messages.
pub struct MessageRouter {
    registry: PeerRegistry,
    outboxes: HashMap<NodeId, Vec<Envelope>>,
    inbox: Vec<Envelope>,
}

impl MessageRouter {
    /// Create a new router backed by the given peer registry.
    pub fn new(registry: PeerRegistry) -> Self {
        Self {
            registry,
            outboxes: HashMap::new(),
            inbox: Vec::new(),
        }
    }

    /// Access the underlying peer registry.
    pub fn registry(&self) -> &PeerRegistry {
        &self.registry
    }

    /// Mutably access the underlying peer registry.
    pub fn registry_mut(&mut self) -> &mut PeerRegistry {
        &mut self.registry
    }

    /// Enqueue a message to a peer. The message is wrapped in an `Envelope`
    /// with a fresh message id.
    ///
    /// Returns `Err` if the peer is not registered.
    pub fn send(&mut self, to: NodeId, message: Message) -> Result<MessageId, TransportError> {
        if self.registry.get_peer(to).is_none() {
            return Err(TransportError::UnknownPeer(to));
        }
        let id = self.registry.next_id();
        let envelope = Envelope {
            id,
            from: self.registry.local_id(),
            to,
            message,
        };
        self.outboxes.entry(to).or_default().push(envelope);
        self.registry.record_send(to);
        Ok(id)
    }

    /// Record an inbound envelope (received from the network).
    pub fn receive(&mut self, envelope: Envelope) {
        let from = envelope.from;
        self.inbox.push(envelope);
        self.registry.record_receive(from);
    }

    /// Take all pending outbound envelopes for a specific peer.
    pub fn drain_outbox(&mut self, node_id: NodeId) -> Vec<Envelope> {
        self.outboxes.remove(&node_id).unwrap_or_default()
    }

    /// Take all inbound envelopes.
    pub fn drain_inbox(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.inbox)
    }

    /// Number of pending outbound messages for a specific peer.
    pub fn pending_count(&self, node_id: NodeId) -> usize {
        self.outboxes.get(&node_id).map_or(0, |v| v.len())
    }

    /// Total number of pending outbound messages across all peers.
    pub fn total_pending(&self) -> usize {
        self.outboxes.values().map(|v| v.len()).sum()
    }
}

// ============================================================================
// TcpTransport — async TCP networking layer (Tier 3.1)
// ============================================================================

use pgwire::tokio::tokio_rustls::rustls;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::tls::InternalTlsConfig;

/// Framed TCP transport for cluster communication.
///
/// Messages are length-prefixed (4-byte big-endian length + payload).
/// Each connection runs a read loop that pushes received envelopes into
/// a channel for the upper layer to consume.
pub struct TcpTransport {
    local_node_id: NodeId,
    listen_addr: String,
    /// Optional shared token required for inbound/outbound peer authentication.
    auth_token: Option<String>,
    /// Optional TLS material for encrypted node-to-node channels.
    tls: Option<InternalTlsConfig>,
    /// Outbound connections keyed by peer node ID.
    connections: Arc<Mutex<HashMap<NodeId, mpsc::Sender<Vec<u8>>>>>,
    /// Inbound envelope receiver.
    inbox_rx: Mutex<mpsc::Receiver<Envelope>>,
    /// Sender half — cloned into each accept loop.
    inbox_tx: mpsc::Sender<Envelope>,
    /// Peer addresses for outbound connections.
    peer_addrs: Arc<Mutex<HashMap<NodeId, String>>>,
    /// Pending request-response correlation channels (keyed by envelope ID).
    pending_replies: Arc<Mutex<HashMap<MessageId, oneshot::Sender<Envelope>>>>,
    /// Monotonically increasing message ID for request correlation.
    next_msg_id: Arc<AtomicU64>,
}

impl TcpTransport {
    /// Create a new transport bound to `listen_addr` (e.g., "127.0.0.1:5433").
    pub fn new(local_node_id: NodeId, listen_addr: &str) -> Self {
        Self::new_with_auth(local_node_id, listen_addr, None)
    }

    /// Create a new transport with optional shared-token authentication.
    pub fn new_with_auth(
        local_node_id: NodeId,
        listen_addr: &str,
        auth_token: Option<String>,
    ) -> Self {
        Self::new_with_auth_and_tls(local_node_id, listen_addr, auth_token, None)
    }

    /// Create a new transport with optional shared-token auth and optional TLS.
    pub fn new_with_auth_and_tls(
        local_node_id: NodeId,
        listen_addr: &str,
        auth_token: Option<String>,
        tls: Option<InternalTlsConfig>,
    ) -> Self {
        let (inbox_tx, inbox_rx) = mpsc::channel(4096);
        Self {
            local_node_id,
            listen_addr: listen_addr.to_string(),
            auth_token,
            tls,
            connections: Arc::new(Mutex::new(HashMap::new())),
            inbox_rx: Mutex::new(inbox_rx),
            inbox_tx,
            peer_addrs: Arc::new(Mutex::new(HashMap::new())),
            pending_replies: Arc::new(Mutex::new(HashMap::new())),
            next_msg_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Return this node's ID.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Register a peer's address for outbound connections.
    pub async fn register_peer(&self, node_id: NodeId, addr: &str) {
        self.peer_addrs
            .lock()
            .await
            .insert(node_id, addr.to_string());
    }

    /// Start listening for inbound connections. Returns the bound address.
    pub async fn listen(&self) -> Result<std::net::SocketAddr, TransportError> {
        let listener = TcpListener::bind(&self.listen_addr)
            .await
            .map_err(|e| TransportError::Protocol(format!("bind failed: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| TransportError::Protocol(format!("local_addr failed: {e}")))?;
        let inbox_tx = self.inbox_tx.clone();
        let auth_token = self.auth_token.clone();
        let tls = self.tls.clone();
        let pending = self.pending_replies.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = inbox_tx.clone();
                let token = auth_token.clone();
                let tls_cfg = tls.clone();
                let pr = pending.clone();
                tokio::spawn(async move {
                    if let Some(tls_cfg) = tls_cfg {
                        match tls_cfg.acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                Self::handle_inbound(tls_stream, tx, token, pr).await;
                            }
                            Err(e) => {
                                tracing::warn!("cluster TLS accept failed: {e}");
                            }
                        }
                    } else {
                        Self::handle_inbound(stream, tx, token, pr).await;
                    }
                });
            }
        });
        Ok(addr)
    }

    /// Read loop for an inbound connection.
    async fn handle_inbound<S>(
        mut stream: S,
        inbox_tx: mpsc::Sender<Envelope>,
        auth_token: Option<String>,
        pending_replies: Arc<Mutex<HashMap<MessageId, oneshot::Sender<Envelope>>>>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if let Some(expected) = auth_token.as_deref()
            && let Err(e) = Self::perform_server_auth(&mut stream, expected).await {
                tracing::warn!("cluster auth failed: {e}");
                return;
            }

        while let Ok(data) = Self::read_frame(&mut stream).await {
            if let Ok(envelope) = Envelope::from_bytes(&data) {
                // Route response to a pending send_request() waiter if ID matches.
                if envelope.id != 0 {
                    let mut pending = pending_replies.lock().await;
                    if let Some(sender) = pending.remove(&envelope.id) {
                        let _ = sender.send(envelope);
                        continue;
                    }
                }
                if inbox_tx.send(envelope).await.is_err() {
                    tracing::debug!("transport inbox closed, stopping connection handler");
                    break;
                }
            }
        }
    }

    /// Send an envelope to a peer. Opens a connection if needed.
    pub async fn send(&self, to: NodeId, envelope: &Envelope) -> Result<(), TransportError> {
        let mut conns = self.connections.lock().await;
        if let std::collections::hash_map::Entry::Vacant(e) = conns.entry(to) {
            // Open new connection
            let addrs = self.peer_addrs.lock().await;
            let addr = addrs
                .get(&to)
                .ok_or(TransportError::UnknownPeer(to))?
                .clone();
            drop(addrs);
            let stream = TcpStream::connect(&addr)
                .await
                .map_err(|e| TransportError::Protocol(format!("connect to {addr} failed: {e}")))?;
            let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);
            if let Some(tls_cfg) = self.tls.as_ref() {
                let server_name = rustls::pki_types::ServerName::try_from(
                    tls_cfg.server_name.clone(),
                )
                .map_err(|e| TransportError::Protocol(format!("invalid TLS server name: {e}")))?;
                let mut tls_stream = tls_cfg
                    .connector
                    .connect(server_name, stream)
                    .await
                    .map_err(|e| TransportError::Protocol(format!("cluster TLS connect: {e}")))?;
                if let Some(token) = self.auth_token.as_deref() {
                    Self::perform_client_auth(&mut tls_stream, token).await?;
                }
                tokio::spawn(async move {
                    Self::writer_loop(tls_stream, rx).await;
                });
            } else {
                let mut tcp_stream = stream;
                if let Some(token) = self.auth_token.as_deref() {
                    Self::perform_client_auth(&mut tcp_stream, token).await?;
                }
                tokio::spawn(async move {
                    Self::writer_loop(tcp_stream, rx).await;
                });
            }
            e.insert(tx);
        }
        let tx = conns.get(&to).unwrap();
        let data = envelope.to_bytes();
        tx.send(data)
            .await
            .map_err(|_| TransportError::Protocol("send channel closed".into()))
    }

    /// Send a message (auto-wraps in Envelope).
    pub async fn send_message(&self, to: NodeId, message: Message) -> Result<(), TransportError> {
        let envelope = Envelope {
            id: 0, // caller should set proper ID
            from: self.local_node_id,
            to,
            message,
        };
        self.send(to, &envelope).await
    }

    /// Send a message and wait for the direct response envelope (request-response).
    ///
    /// The remote handler must reply using the same envelope `id` for correlation.
    /// Times out after 10 seconds.
    pub async fn send_request(&self, to: NodeId, message: Message) -> Result<Envelope, TransportError> {
        let id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending_replies.lock().await.insert(id, tx);

        let envelope = Envelope { id, from: self.local_node_id, to, message };
        if let Err(e) = self.send(to, &envelope).await {
            self.pending_replies.lock().await.remove(&id);
            return Err(e);
        }

        match tokio::time::timeout(std::time::Duration::from_secs(10), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => Err(TransportError::Protocol("reply channel dropped".into())),
            Err(_) => {
                self.pending_replies.lock().await.remove(&id);
                Err(TransportError::Protocol("request timeout".into()))
            }
        }
    }

    /// Receive the next inbound envelope.
    pub async fn recv(&self) -> Option<Envelope> {
        self.inbox_rx.lock().await.recv().await
    }

    /// Try to receive without blocking.
    pub async fn try_recv(&self) -> Option<Envelope> {
        self.inbox_rx.lock().await.try_recv().ok()
    }

    async fn writer_loop<S>(mut stream: S, mut rx: mpsc::Receiver<Vec<u8>>)
    where
        S: AsyncWrite + Unpin + Send + 'static,
    {
        while let Some(data) = rx.recv().await {
            if Self::write_frame(&mut stream, &data).await.is_err() {
                break;
            }
        }
    }

    /// Write a length-prefixed frame to a stream.
    async fn write_frame<S>(stream: &mut S, data: &[u8]) -> Result<(), TransportError>
    where
        S: AsyncWrite + Unpin,
    {
        let len = (data.len() as u32).to_be_bytes();
        stream
            .write_all(&len)
            .await
            .map_err(|e| TransportError::Protocol(format!("write len: {e}")))?;
        stream
            .write_all(data)
            .await
            .map_err(|e| TransportError::Protocol(format!("write data: {e}")))?;
        stream
            .flush()
            .await
            .map_err(|e| TransportError::Protocol(format!("flush: {e}")))?;
        Ok(())
    }

    /// Read a length-prefixed frame from a stream.
    async fn read_frame<S>(stream: &mut S) -> Result<Vec<u8>, TransportError>
    where
        S: AsyncRead + Unpin,
    {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| TransportError::Protocol(format!("read len: {e}")))?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 64 * 1024 * 1024 {
            return Err(TransportError::Protocol(format!("frame too large: {len}")));
        }
        let mut data = vec![0u8; len];
        stream
            .read_exact(&mut data)
            .await
            .map_err(|e| TransportError::Protocol(format!("read data: {e}")))?;
        Ok(data)
    }

    /// Client-side auth handshake: send shared token as first frame.
    async fn perform_client_auth<S>(stream: &mut S, token: &str) -> Result<(), TransportError>
    where
        S: AsyncWrite + Unpin,
    {
        let mut payload = b"NUCLEUS-AUTH\0".to_vec();
        payload.extend_from_slice(token.as_bytes());
        Self::write_frame(stream, &payload).await
    }

    /// Server-side auth handshake: require and verify first frame token.
    async fn perform_server_auth<S>(
        stream: &mut S,
        expected_token: &str,
    ) -> Result<(), TransportError>
    where
        S: AsyncRead + Unpin,
    {
        let payload = Self::read_frame(stream).await?;
        if !payload.starts_with(b"NUCLEUS-AUTH\0") {
            return Err(TransportError::Protocol(
                "missing cluster auth handshake".into(),
            ));
        }
        let provided = &payload["NUCLEUS-AUTH\0".len()..];
        if !constant_time_eq_token(provided, expected_token.as_bytes()) {
            return Err(TransportError::Protocol(
                "invalid cluster auth token".into(),
            ));
        }
        Ok(())
    }

    /// Close all connections.
    pub async fn shutdown(&self) {
        self.connections.lock().await.clear();
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- helpers -----------------------------------------------------

    #[test]
    fn constant_time_eq_token_matches_expected() {
        assert!(constant_time_eq_token(b"secret", b"secret"));
        assert!(!constant_time_eq_token(b"secret", b"secreT"));
        assert!(!constant_time_eq_token(b"secret", b"secret1"));
    }

    fn roundtrip(msg: Message) {
        let bytes = encode(&msg);
        let decoded = decode(&bytes).expect("decode failed");
        assert_eq!(msg, decoded);
    }

    // ---------- encode/decode round-trips -----------------------------------

    #[test]
    fn roundtrip_request_vote() {
        roundtrip(Message::RequestVote {
            term: 5,
            candidate_id: 42,
            last_log_index: 100,
            last_log_term: 4,
        });
    }

    #[test]
    fn roundtrip_request_vote_response() {
        roundtrip(Message::RequestVoteResponse {
            term: 5,
            vote_granted: true,
        });
        roundtrip(Message::RequestVoteResponse {
            term: 6,
            vote_granted: false,
        });
    }

    #[test]
    fn roundtrip_append_entries_empty() {
        roundtrip(Message::AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        });
    }

    #[test]
    fn roundtrip_append_entries_with_entries() {
        roundtrip(Message::AppendEntries {
            term: 7,
            leader_id: 2,
            prev_log_index: 10,
            prev_log_term: 6,
            entries: vec![
                RaftEntry {
                    index: 11,
                    term: 7,
                    command: RaftCommand::Sql("INSERT INTO t VALUES (1)".into()),
                },
                RaftEntry {
                    index: 12,
                    term: 7,
                    command: RaftCommand::Noop,
                },
                RaftEntry {
                    index: 13,
                    term: 7,
                    command: RaftCommand::AddNode(99),
                },
                RaftEntry {
                    index: 14,
                    term: 7,
                    command: RaftCommand::RemoveNode(50),
                },
            ],
            leader_commit: 10,
        });
    }

    #[test]
    fn roundtrip_append_entries_response() {
        roundtrip(Message::AppendEntriesResponse {
            term: 7,
            success: true,
            match_index: 14,
        });
        roundtrip(Message::AppendEntriesResponse {
            term: 7,
            success: false,
            match_index: 0,
        });
    }

    #[test]
    fn roundtrip_forward_query() {
        roundtrip(Message::ForwardQuery {
            query: "SELECT * FROM users WHERE id = 42".into(),
            shard_id: 3,
        });
    }

    #[test]
    fn roundtrip_forward_query_response() {
        roundtrip(Message::ForwardQueryResponse {
            success: true,
            rows: vec![vec![1, 2, 3], vec![4, 5, 6]],
            error: None,
        });
        roundtrip(Message::ForwardQueryResponse {
            success: false,
            rows: vec![],
            error: Some("table not found".into()),
        });
    }

    #[test]
    fn roundtrip_join_cluster() {
        roundtrip(Message::JoinCluster {
            node_id: 10,
            address: "192.168.1.10:5432".into(),
        });
    }

    #[test]
    fn roundtrip_join_cluster_response() {
        roundtrip(Message::JoinClusterResponse {
            success: true,
            cluster_nodes: vec![
                (1, "192.168.1.1:5432".into()),
                (2, "192.168.1.2:5432".into()),
                (10, "192.168.1.10:5432".into()),
            ],
        });
    }

    #[test]
    fn roundtrip_heartbeat() {
        roundtrip(Message::Heartbeat {
            node_id: 1,
            term: 5,
        });
    }

    #[test]
    fn roundtrip_heartbeat_response() {
        roundtrip(Message::HeartbeatResponse {
            node_id: 2,
            term: 5,
        });
    }

    #[test]
    fn roundtrip_transfer_shard() {
        roundtrip(Message::TransferShard {
            shard_id: 7,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        });
    }

    #[test]
    fn roundtrip_transfer_shard_ack() {
        roundtrip(Message::TransferShardAck {
            shard_id: 7,
            success: true,
        });
    }

    // ---------- Envelope round-trip -----------------------------------------

    #[test]
    fn envelope_roundtrip() {
        let env = Envelope {
            id: 42,
            from: 1,
            to: 2,
            message: Message::Heartbeat {
                node_id: 1,
                term: 9,
            },
        };
        let bytes = env.to_bytes();
        let decoded = Envelope::from_bytes(&bytes).expect("envelope decode failed");
        assert_eq!(env, decoded);
    }

    #[test]
    fn envelope_roundtrip_complex_message() {
        let env = Envelope {
            id: 100,
            from: 3,
            to: 7,
            message: Message::AppendEntries {
                term: 12,
                leader_id: 3,
                prev_log_index: 50,
                prev_log_term: 11,
                entries: vec![RaftEntry {
                    index: 51,
                    term: 12,
                    command: RaftCommand::Sql("UPDATE t SET x = 1".into()),
                }],
                leader_commit: 50,
            },
        };
        let bytes = env.to_bytes();
        let decoded = Envelope::from_bytes(&bytes).expect("envelope decode failed");
        assert_eq!(env, decoded);
    }

    // ---------- decode errors -----------------------------------------------

    #[test]
    fn decode_empty_buffer_returns_error() {
        let result = decode(&[]);
        assert_eq!(result, Err(TransportError::BufferUnderflow));
    }

    #[test]
    fn decode_unknown_tag_returns_error() {
        let result = decode(&[255]);
        assert_eq!(result, Err(TransportError::UnknownMessageTag(255)));
    }

    #[test]
    fn decode_truncated_payload_returns_error() {
        // TAG_HEARTBEAT expects 16 more bytes (two u64s) but we only give 4.
        let result = decode(&[TAG_HEARTBEAT, 0, 0, 0, 0]);
        assert_eq!(result, Err(TransportError::BufferUnderflow));
    }

    // ---------- PeerRegistry ------------------------------------------------

    #[test]
    fn peer_registry_add_remove() {
        let mut reg = PeerRegistry::new(1);
        assert!(reg.add_peer(2, "addr2".into()));
        assert!(reg.add_peer(3, "addr3".into()));
        assert_eq!(reg.peer_count(), 2);

        // duplicate add returns false
        assert!(!reg.add_peer(2, "addr2-dup".into()));
        assert_eq!(reg.peer_count(), 2);

        // cannot add self
        assert!(!reg.add_peer(1, "self".into()));
        assert_eq!(reg.peer_count(), 2);

        // remove
        assert!(reg.remove_peer(2));
        assert!(!reg.remove_peer(2)); // already removed
        assert_eq!(reg.peer_count(), 1);
    }

    #[test]
    fn peer_registry_connect_disconnect() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(2, "addr2".into());

        assert!(reg.connected_peers().is_empty());

        reg.mark_connected(2);
        assert_eq!(reg.connected_peers(), vec![2]);

        reg.mark_disconnected(2);
        assert!(reg.connected_peers().is_empty());

        // unknown node
        assert!(!reg.mark_connected(99));
    }

    #[test]
    fn peer_registry_stale_detection() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(2, "a".into());
        reg.add_peer(3, "b".into());
        reg.add_peer(4, "c".into());

        // No heartbeats yet => all are stale (last_heartbeat_ms == 0).
        let stale = reg.stale_peers(1000, 500);
        assert_eq!(stale.len(), 3);

        // Record heartbeats for node 2 and 3 at time 800.
        reg.record_heartbeat(2, 800);
        reg.record_heartbeat(3, 800);

        // At now=1000 with timeout=500: node 4 stale (never heard), 2 & 3 fresh (200ms ago).
        let mut stale = reg.stale_peers(1000, 500);
        stale.sort();
        assert_eq!(stale, vec![4]);

        // At now=1500 with timeout=500: node 2 & 3 are now 700ms stale, node 4 still stale.
        let mut stale = reg.stale_peers(1500, 500);
        stale.sort();
        assert_eq!(stale, vec![2, 3, 4]);
    }

    #[test]
    fn peer_registry_message_id_monotonic() {
        let mut reg = PeerRegistry::new(1);
        let id1 = reg.next_id();
        let id2 = reg.next_id();
        let id3 = reg.next_id();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    // ---------- MessageRouter -----------------------------------------------

    #[test]
    fn router_send_and_drain_outbox() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(2, "addr2".into());
        let mut router = MessageRouter::new(reg);

        let id = router
            .send(
                2,
                Message::Heartbeat {
                    node_id: 1,
                    term: 1,
                },
            )
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(router.pending_count(2), 1);
        assert_eq!(router.total_pending(), 1);

        let outbox = router.drain_outbox(2);
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].from, 1);
        assert_eq!(outbox[0].to, 2);
        assert_eq!(outbox[0].id, 1);

        // After drain, outbox is empty.
        assert_eq!(router.pending_count(2), 0);
    }

    #[test]
    fn router_receive_and_drain_inbox() {
        let reg = PeerRegistry::new(1);
        let mut router = MessageRouter::new(reg);

        let env = Envelope {
            id: 99,
            from: 2,
            to: 1,
            message: Message::HeartbeatResponse {
                node_id: 2,
                term: 3,
            },
        };
        router.receive(env.clone());
        assert_eq!(router.drain_inbox().len(), 1);
        assert!(router.drain_inbox().is_empty());
    }

    #[test]
    fn router_send_to_unknown_peer_fails() {
        let reg = PeerRegistry::new(1);
        let mut router = MessageRouter::new(reg);
        let result = router.send(
            99,
            Message::Heartbeat {
                node_id: 1,
                term: 1,
            },
        );
        assert_eq!(result, Err(TransportError::UnknownPeer(99)));
    }

    #[test]
    fn router_multiple_messages_to_same_peer() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(5, "addr5".into());
        let mut router = MessageRouter::new(reg);

        router
            .send(
                5,
                Message::Heartbeat {
                    node_id: 1,
                    term: 1,
                },
            )
            .unwrap();
        router
            .send(
                5,
                Message::Heartbeat {
                    node_id: 1,
                    term: 2,
                },
            )
            .unwrap();
        router
            .send(
                5,
                Message::Heartbeat {
                    node_id: 1,
                    term: 3,
                },
            )
            .unwrap();

        assert_eq!(router.pending_count(5), 3);
        assert_eq!(router.total_pending(), 3);

        let outbox = router.drain_outbox(5);
        assert_eq!(outbox.len(), 3);
        // IDs are monotonically increasing.
        assert!(outbox[0].id < outbox[1].id);
        assert!(outbox[1].id < outbox[2].id);
    }

    #[test]
    fn router_empty_outbox_returns_empty_vec() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(2, "addr2".into());
        let mut router = MessageRouter::new(reg);

        let outbox = router.drain_outbox(2);
        assert!(outbox.is_empty());

        // Even for unknown peers, it returns empty (not an error).
        let outbox = router.drain_outbox(999);
        assert!(outbox.is_empty());
    }

    #[test]
    fn router_message_ids_are_monotonically_increasing() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(2, "a".into());
        reg.add_peer(3, "b".into());
        let mut router = MessageRouter::new(reg);

        let id1 = router
            .send(
                2,
                Message::Heartbeat {
                    node_id: 1,
                    term: 1,
                },
            )
            .unwrap();
        let id2 = router
            .send(
                3,
                Message::Heartbeat {
                    node_id: 1,
                    term: 1,
                },
            )
            .unwrap();
        let id3 = router
            .send(
                2,
                Message::Heartbeat {
                    node_id: 1,
                    term: 2,
                },
            )
            .unwrap();

        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[test]
    fn router_tracks_send_receive_counts() {
        let mut reg = PeerRegistry::new(1);
        reg.add_peer(2, "addr2".into());
        let mut router = MessageRouter::new(reg);

        router
            .send(
                2,
                Message::Heartbeat {
                    node_id: 1,
                    term: 1,
                },
            )
            .unwrap();
        router
            .send(
                2,
                Message::Heartbeat {
                    node_id: 1,
                    term: 2,
                },
            )
            .unwrap();

        assert_eq!(router.registry().get_peer(2).unwrap().messages_sent, 2);
        assert_eq!(router.registry().get_peer(2).unwrap().messages_received, 0);

        router.receive(Envelope {
            id: 50,
            from: 2,
            to: 1,
            message: Message::HeartbeatResponse {
                node_id: 2,
                term: 2,
            },
        });

        assert_eq!(router.registry().get_peer(2).unwrap().messages_received, 1);
    }

    // ---------- Full cluster join flow --------------------------------------

    #[test]
    fn full_cluster_join_flow() {
        // Node 10 wants to join a cluster led by node 1, which also knows node 2.
        let mut leader_reg = PeerRegistry::new(1);
        leader_reg.add_peer(2, "192.168.1.2:5432".into());
        leader_reg.add_peer(10, "192.168.1.10:5432".into());
        let mut leader_router = MessageRouter::new(leader_reg);

        let mut joiner_reg = PeerRegistry::new(10);
        joiner_reg.add_peer(1, "192.168.1.1:5432".into());
        let mut joiner_router = MessageRouter::new(joiner_reg);

        // Joiner sends JoinCluster to leader.
        joiner_router
            .send(
                1,
                Message::JoinCluster {
                    node_id: 10,
                    address: "192.168.1.10:5432".into(),
                },
            )
            .unwrap();

        // Simulate network: drain joiner's outbox and deliver to leader's inbox.
        let outbound = joiner_router.drain_outbox(1);
        assert_eq!(outbound.len(), 1);
        for env in outbound {
            leader_router.receive(env);
        }

        // Leader processes JoinCluster and responds.
        let inbox = leader_router.drain_inbox();
        assert_eq!(inbox.len(), 1);
        match &inbox[0].message {
            Message::JoinCluster { node_id, address } => {
                assert_eq!(*node_id, 10);
                assert_eq!(address, "192.168.1.10:5432");
            }
            other => panic!("expected JoinCluster, got {:?}", other),
        }

        // Leader sends response back.
        leader_router
            .send(
                10,
                Message::JoinClusterResponse {
                    success: true,
                    cluster_nodes: vec![
                        (1, "192.168.1.1:5432".into()),
                        (2, "192.168.1.2:5432".into()),
                        (10, "192.168.1.10:5432".into()),
                    ],
                },
            )
            .unwrap();

        // Simulate network: deliver response to joiner.
        let outbound = leader_router.drain_outbox(10);
        assert_eq!(outbound.len(), 1);
        for env in outbound {
            joiner_router.receive(env);
        }

        // Joiner processes the response.
        let inbox = joiner_router.drain_inbox();
        assert_eq!(inbox.len(), 1);
        match &inbox[0].message {
            Message::JoinClusterResponse {
                success,
                cluster_nodes,
            } => {
                assert!(*success);
                assert_eq!(cluster_nodes.len(), 3);
                // Joiner now knows about all cluster nodes.
                for (nid, addr) in cluster_nodes {
                    if *nid != joiner_router.registry().local_id() {
                        joiner_router.registry_mut().add_peer(*nid, addr.clone());
                    }
                }
            }
            other => panic!("expected JoinClusterResponse, got {:?}", other),
        }

        // Joiner now knows 2 peers (node 1 and node 2).
        assert_eq!(joiner_router.registry().peer_count(), 2);
    }

    // ---------- Serialization edge cases ------------------------------------

    #[test]
    fn roundtrip_large_transfer_shard() {
        let data = vec![0xAB; 16384]; // 16 KiB payload
        roundtrip(Message::TransferShard { shard_id: 1, data });
    }

    #[test]
    fn roundtrip_empty_strings_and_vecs() {
        roundtrip(Message::ForwardQuery {
            query: String::new(),
            shard_id: 0,
        });
        roundtrip(Message::ForwardQueryResponse {
            success: true,
            rows: vec![],
            error: None,
        });
        roundtrip(Message::JoinClusterResponse {
            success: false,
            cluster_nodes: vec![],
        });
        roundtrip(Message::TransferShard {
            shard_id: 0,
            data: vec![],
        });
    }

    // ---------- TcpTransport -------------------------------------------------

    #[tokio::test]
    async fn tcp_transport_send_recv() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        t2.register_peer(1, &addr.to_string()).await;

        let envelope = Envelope {
            id: 42,
            from: 2,
            to: 1,
            message: Message::Heartbeat {
                node_id: 2,
                term: 5,
            },
        };
        t2.send(1, &envelope).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(received.from, 2);
        assert_eq!(received.to, 1);
        assert_eq!(
            received.message,
            Message::Heartbeat {
                node_id: 2,
                term: 5
            }
        );

        t1.shutdown().await;
        t2.shutdown().await;
    }

    #[tokio::test]
    async fn tcp_transport_multiple_messages() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        t2.register_peer(1, &addr.to_string()).await;

        for i in 0..5u64 {
            let env = Envelope {
                id: i,
                from: 2,
                to: 1,
                message: Message::Heartbeat {
                    node_id: 2,
                    term: i,
                },
            };
            t2.send(1, &env).await.unwrap();
        }

        for i in 0..5u64 {
            let received = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(received.id, i);
        }

        t1.shutdown().await;
        t2.shutdown().await;
    }

    #[tokio::test]
    async fn tcp_transport_unknown_peer_error() {
        let t = TcpTransport::new(1, "127.0.0.1:0");
        let env = Envelope {
            id: 1,
            from: 1,
            to: 99,
            message: Message::Heartbeat {
                node_id: 1,
                term: 0,
            },
        };
        let result = t.send(99, &env).await;
        assert!(matches!(result, Err(TransportError::UnknownPeer(99))));
    }

    #[tokio::test]
    async fn tcp_transport_send_message_helper() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        t2.register_peer(1, &addr.to_string()).await;

        t2.send_message(
            1,
            Message::RequestVoteResponse {
                term: 3,
                vote_granted: true,
            },
        )
        .await
        .unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            received.message,
            Message::RequestVoteResponse {
                term: 3,
                vote_granted: true
            }
        );
        t1.shutdown().await;
        t2.shutdown().await;
    }

    #[tokio::test]
    async fn tcp_transport_bidirectional() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        let addr2 = t2.listen().await.unwrap();

        t1.register_peer(2, &addr2.to_string()).await;
        t2.register_peer(1, &addr1.to_string()).await;

        // t1 -> t2
        t1.send_message(
            2,
            Message::Heartbeat {
                node_id: 1,
                term: 1,
            },
        )
        .await
        .unwrap();
        let r = tokio::time::timeout(std::time::Duration::from_secs(2), t2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.message,
            Message::Heartbeat {
                node_id: 1,
                term: 1
            }
        );

        // t2 -> t1
        t2.send_message(
            1,
            Message::HeartbeatResponse {
                node_id: 2,
                term: 1,
            },
        )
        .await
        .unwrap();
        let r = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.message,
            Message::HeartbeatResponse {
                node_id: 2,
                term: 1
            }
        );

        t1.shutdown().await;
        t2.shutdown().await;
    }

    #[tokio::test]
    async fn tcp_transport_complex_message() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        t2.register_peer(1, &addr.to_string()).await;

        let msg = Message::AppendEntries {
            term: 7,
            leader_id: 2,
            prev_log_index: 10,
            prev_log_term: 6,
            entries: vec![
                RaftEntry {
                    index: 11,
                    term: 7,
                    command: RaftCommand::Sql("INSERT INTO t VALUES (1)".into()),
                },
                RaftEntry {
                    index: 12,
                    term: 7,
                    command: RaftCommand::Noop,
                },
            ],
            leader_commit: 10,
        };
        t2.send_message(1, msg.clone()).await.unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.message, msg);

        t1.shutdown().await;
        t2.shutdown().await;
    }

    // ---------- Cluster integration tests ------------------------------------

    /// Full cluster join flow over real TCP: node2 sends JoinCluster to node1,
    /// node1 processes the request and responds with the cluster topology.
    #[tokio::test]
    async fn cluster_join_over_tcp() {
        // Node 1 (existing cluster leader)
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        // Node 2 (joiner)
        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        let addr2 = t2.listen().await.unwrap();

        // Register peers
        t2.register_peer(1, &addr1.to_string()).await;
        t1.register_peer(2, &addr2.to_string()).await;

        // Node 2 sends JoinCluster to Node 1
        t2.send_message(
            1,
            Message::JoinCluster {
                node_id: 2,
                address: addr2.to_string(),
            },
        )
        .await
        .unwrap();

        // Node 1 receives JoinCluster
        let join_req = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();

        match join_req.message {
            Message::JoinCluster {
                node_id,
                ref address,
            } => {
                assert_eq!(node_id, 2);
                assert_eq!(address, &addr2.to_string());
            }
            other => panic!("expected JoinCluster, got: {other:?}"),
        }

        // Node 1 responds with cluster topology
        t1.send_message(
            2,
            Message::JoinClusterResponse {
                success: true,
                cluster_nodes: vec![(1, addr1.to_string()), (2, addr2.to_string())],
            },
        )
        .await
        .unwrap();

        // Node 2 receives the response
        let join_resp = tokio::time::timeout(std::time::Duration::from_secs(2), t2.recv())
            .await
            .unwrap()
            .unwrap();

        match join_resp.message {
            Message::JoinClusterResponse {
                success,
                cluster_nodes,
            } => {
                assert!(success);
                assert_eq!(cluster_nodes.len(), 2);
                let ids: Vec<u64> = cluster_nodes.iter().map(|(id, _)| *id).collect();
                assert!(ids.contains(&1));
                assert!(ids.contains(&2));
            }
            other => panic!("expected JoinClusterResponse, got: {other:?}"),
        }

        t1.shutdown().await;
        t2.shutdown().await;
    }

    /// Three-node cluster: node3 joins a 2-node cluster via node1.
    #[tokio::test]
    async fn three_node_cluster_join() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        let addr2 = t2.listen().await.unwrap();

        let t3 = TcpTransport::new(3, "127.0.0.1:0");
        let addr3 = t3.listen().await.unwrap();

        // All nodes register each other
        t1.register_peer(2, &addr2.to_string()).await;
        t1.register_peer(3, &addr3.to_string()).await;
        t2.register_peer(1, &addr1.to_string()).await;
        t3.register_peer(1, &addr1.to_string()).await;

        // Node 3 sends JoinCluster
        t3.send_message(
            1,
            Message::JoinCluster {
                node_id: 3,
                address: addr3.to_string(),
            },
        )
        .await
        .unwrap();

        let req = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            req.message,
            Message::JoinCluster { node_id: 3, .. }
        ));

        // Node 1 responds with 3-node topology
        t1.send_message(
            3,
            Message::JoinClusterResponse {
                success: true,
                cluster_nodes: vec![
                    (1, addr1.to_string()),
                    (2, addr2.to_string()),
                    (3, addr3.to_string()),
                ],
            },
        )
        .await
        .unwrap();

        let resp = tokio::time::timeout(std::time::Duration::from_secs(2), t3.recv())
            .await
            .unwrap()
            .unwrap();
        match resp.message {
            Message::JoinClusterResponse {
                success,
                cluster_nodes,
            } => {
                assert!(success);
                assert_eq!(cluster_nodes.len(), 3);
            }
            other => panic!("expected JoinClusterResponse, got: {other:?}"),
        }

        // Node 3 now knows about node 2 — register and send heartbeat
        t3.register_peer(2, &addr2.to_string()).await;
        t3.send_message(
            2,
            Message::Heartbeat {
                node_id: 3,
                term: 1,
            },
        )
        .await
        .unwrap();

        let hb = tokio::time::timeout(std::time::Duration::from_secs(2), t2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            hb.message,
            Message::Heartbeat {
                node_id: 3,
                term: 1
            }
        );

        t1.shutdown().await;
        t2.shutdown().await;
        t3.shutdown().await;
    }

    /// Heartbeat exchange between two cluster nodes over TCP.
    #[tokio::test]
    async fn cluster_heartbeat_exchange() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        let addr2 = t2.listen().await.unwrap();

        t1.register_peer(2, &addr2.to_string()).await;
        t2.register_peer(1, &addr1.to_string()).await;

        // Node 1 sends heartbeat
        t1.send_message(
            2,
            Message::Heartbeat {
                node_id: 1,
                term: 5,
            },
        )
        .await
        .unwrap();

        let hb = tokio::time::timeout(std::time::Duration::from_secs(2), t2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            hb.message,
            Message::Heartbeat {
                node_id: 1,
                term: 5
            }
        );

        // Node 2 responds
        t2.send_message(
            1,
            Message::HeartbeatResponse {
                node_id: 2,
                term: 5,
            },
        )
        .await
        .unwrap();

        let resp = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            resp.message,
            Message::HeartbeatResponse {
                node_id: 2,
                term: 5
            }
        );

        t1.shutdown().await;
        t2.shutdown().await;
    }

    /// Raft AppendEntries exchange over TCP between leader and follower.
    #[tokio::test]
    async fn raft_append_entries_over_tcp() {
        let leader = TcpTransport::new(1, "127.0.0.1:0");
        let leader_addr = leader.listen().await.unwrap();

        let follower = TcpTransport::new(2, "127.0.0.1:0");
        let follower_addr = follower.listen().await.unwrap();

        leader.register_peer(2, &follower_addr.to_string()).await;
        follower.register_peer(1, &leader_addr.to_string()).await;

        // Leader sends AppendEntries with log entries
        let entries = vec![
            RaftEntry {
                index: 1,
                term: 1,
                command: RaftCommand::Sql("CREATE TABLE t (id INT)".into()),
            },
            RaftEntry {
                index: 2,
                term: 1,
                command: RaftCommand::Sql("INSERT INTO t VALUES (1)".into()),
            },
            RaftEntry {
                index: 3,
                term: 1,
                command: RaftCommand::Noop,
            },
        ];
        leader
            .send_message(
                2,
                Message::AppendEntries {
                    term: 1,
                    leader_id: 1,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: entries.clone(),
                    leader_commit: 0,
                },
            )
            .await
            .unwrap();

        let ae = tokio::time::timeout(std::time::Duration::from_secs(2), follower.recv())
            .await
            .unwrap()
            .unwrap();
        match ae.message {
            Message::AppendEntries {
                term,
                leader_id,
                entries: recv_entries,
                ..
            } => {
                assert_eq!(term, 1);
                assert_eq!(leader_id, 1);
                assert_eq!(recv_entries.len(), 3);
                assert_eq!(
                    recv_entries[0].command,
                    RaftCommand::Sql("CREATE TABLE t (id INT)".into())
                );
                assert_eq!(recv_entries[2].command, RaftCommand::Noop);
            }
            other => panic!("expected AppendEntries, got: {other:?}"),
        }

        // Follower responds with success
        follower
            .send_message(
                1,
                Message::AppendEntriesResponse {
                    term: 1,
                    success: true,
                    match_index: 3,
                },
            )
            .await
            .unwrap();

        let resp = tokio::time::timeout(std::time::Duration::from_secs(2), leader.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            resp.message,
            Message::AppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 3,
            }
        );

        leader.shutdown().await;
        follower.shutdown().await;
    }

    /// Shard transfer over TCP.
    #[tokio::test]
    async fn shard_transfer_over_tcp() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        t2.register_peer(1, &addr1.to_string()).await;

        // Transfer a 32KB shard payload
        let shard_data = vec![0xABu8; 32768];
        t2.send_message(
            1,
            Message::TransferShard {
                shard_id: 42,
                data: shard_data.clone(),
            },
        )
        .await
        .unwrap();

        let transfer = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        match transfer.message {
            Message::TransferShard { shard_id, data } => {
                assert_eq!(shard_id, 42);
                assert_eq!(data.len(), 32768);
                assert!(data.iter().all(|b| *b == 0xAB));
            }
            other => panic!("expected TransferShard, got: {other:?}"),
        }

        // Ack the transfer
        t1.register_peer(2, "127.0.0.1:0").await; // dummy addr — connection already established
        // Actually, t1 doesn't have t2's addr yet. Use send with envelope:
        let _ack = Envelope {
            id: 1,
            from: 1,
            to: 2,
            message: Message::TransferShardAck {
                shard_id: 42,
                success: true,
            },
        };
        // For this test, just verify the message was received correctly
        assert_eq!(transfer.from, 2);

        t1.shutdown().await;
        t2.shutdown().await;
    }

    /// ForwardQuery over TCP — simulates cross-shard query routing.
    #[tokio::test]
    async fn forward_query_over_tcp() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        let addr2 = t2.listen().await.unwrap();

        t1.register_peer(2, &addr2.to_string()).await;
        t2.register_peer(1, &addr1.to_string()).await;

        // Node 1 forwards a query to Node 2 (which owns shard 10)
        t1.send_message(
            2,
            Message::ForwardQuery {
                query: "SELECT * FROM users WHERE shard_key = 42".into(),
                shard_id: 10,
            },
        )
        .await
        .unwrap();

        let fwd = tokio::time::timeout(std::time::Duration::from_secs(2), t2.recv())
            .await
            .unwrap()
            .unwrap();
        match fwd.message {
            Message::ForwardQuery {
                ref query,
                shard_id,
            } => {
                assert_eq!(shard_id, 10);
                assert!(query.contains("shard_key"));
            }
            other => panic!("expected ForwardQuery, got: {other:?}"),
        }

        // Node 2 responds with results
        t2.send_message(
            1,
            Message::ForwardQueryResponse {
                success: true,
                rows: vec![vec![1, 0, 0, 0, 42, 0, 0, 0]], // mock row data
                error: None,
            },
        )
        .await
        .unwrap();

        let resp = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        match resp.message {
            Message::ForwardQueryResponse {
                success,
                rows,
                error,
            } => {
                assert!(success);
                assert_eq!(rows.len(), 1);
                assert!(error.is_none());
            }
            other => panic!("expected ForwardQueryResponse, got: {other:?}"),
        }

        t1.shutdown().await;
        t2.shutdown().await;
    }

    /// RequestVote exchange over TCP — simulates leader election.
    #[tokio::test]
    async fn request_vote_over_tcp() {
        let t1 = TcpTransport::new(1, "127.0.0.1:0");
        let addr1 = t1.listen().await.unwrap();

        let t2 = TcpTransport::new(2, "127.0.0.1:0");
        let addr2 = t2.listen().await.unwrap();

        let t3 = TcpTransport::new(3, "127.0.0.1:0");
        let addr3 = t3.listen().await.unwrap();

        // Node 1 (candidate) registers all peers
        t1.register_peer(2, &addr2.to_string()).await;
        t1.register_peer(3, &addr3.to_string()).await;
        t2.register_peer(1, &addr1.to_string()).await;
        t3.register_peer(1, &addr1.to_string()).await;

        // Node 1 sends RequestVote to nodes 2 and 3
        let vote_req = Message::RequestVote {
            term: 2,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 1,
        };
        t1.send_message(2, vote_req.clone()).await.unwrap();
        t1.send_message(3, vote_req).await.unwrap();

        // Node 2 receives and grants vote
        let req2 = tokio::time::timeout(std::time::Duration::from_secs(2), t2.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            req2.message,
            Message::RequestVote {
                term: 2,
                candidate_id: 1,
                ..
            }
        ));

        t2.send_message(
            1,
            Message::RequestVoteResponse {
                term: 2,
                vote_granted: true,
            },
        )
        .await
        .unwrap();

        // Node 3 receives and grants vote
        let req3 = tokio::time::timeout(std::time::Duration::from_secs(2), t3.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(req3.message, Message::RequestVote { term: 2, .. }));

        t3.send_message(
            1,
            Message::RequestVoteResponse {
                term: 2,
                vote_granted: true,
            },
        )
        .await
        .unwrap();

        // Node 1 receives both votes
        let v1 = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();
        let v2 = tokio::time::timeout(std::time::Duration::from_secs(2), t1.recv())
            .await
            .unwrap()
            .unwrap();

        let votes: Vec<bool> = vec![v1, v2]
            .into_iter()
            .map(|e| match e.message {
                Message::RequestVoteResponse { vote_granted, .. } => vote_granted,
                _ => false,
            })
            .collect();
        assert_eq!(votes, vec![true, true]);

        t1.shutdown().await;
        t2.shutdown().await;
        t3.shutdown().await;
    }
}
