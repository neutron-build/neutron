//! Distributed coordinator for Nucleus.
//!
//! Ties together multi-Raft consensus (one Raft group per shard range),
//! distributed two-phase commit (2PC) transactions, and cross-shard query
//! routing into a working cluster. No external dependencies beyond `std`;
//! everything is synchronous and testable without an async runtime.

use std::collections::HashMap;
use std::collections::HashSet;
use std::cmp::Ordering;
use std::fmt;
use std::time::SystemTime;

pub type NodeId = u64;
pub type ShardId = u64;
pub type Term = u64;
pub type TxnId = u64;

/// The deployment mode determines how the cluster operates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    /// Single node, no distribution.
    Standalone,
    /// Two nodes: primary-replica (WAL streaming, no Raft).
    PrimaryReplica,
    /// Three or more nodes: multi-Raft consensus.
    MultiRaft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnPhase {
    Active,
    Preparing,
    Committing,
    Aborting,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub enum DistributedError {
    NotLeader { shard_id: ShardId, leader: Option<NodeId> },
    ShardNotFound(ShardId),
    TxnNotFound(TxnId),
    TxnNotActive(TxnId),
    InvalidPhase { txn_id: TxnId, expected: TxnPhase, actual: TxnPhase },
    NoRoute { key: i64 },
    StandaloneMode,
    MessageTooShort,
    UnknownMessageTag(u8),
}

impl fmt::Display for DistributedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader { shard_id, leader } => {
                write!(f, "not leader for shard {shard_id}, leader: {leader:?}")
            }
            Self::ShardNotFound(id) => write!(f, "shard {id} not found"),
            Self::TxnNotFound(id) => write!(f, "transaction {id} not found"),
            Self::TxnNotActive(id) => write!(f, "transaction {id} is not active"),
            Self::InvalidPhase { txn_id, expected, actual } => {
                write!(f, "txn {txn_id}: expected phase {expected:?}, got {actual:?}")
            }
            Self::NoRoute { key } => write!(f, "no route for key {key}"),
            Self::StandaloneMode => write!(f, "operation not supported in standalone mode"),
            Self::MessageTooShort => write!(f, "message too short to deserialize"),
            Self::UnknownMessageTag(tag) => write!(f, "unknown message tag: {tag}"),
        }
    }
}

// --- Operation & LogEntry ---

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Sql(String),
    Noop,
    TxnPrepare { txn_id: TxnId },
    TxnCommit { txn_id: TxnId },
    TxnAbort { txn_id: TxnId },
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub index: u64,
    pub term: Term,
    pub operation: Operation,
}

// --- RaftGroup ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole { Follower, Candidate, Leader }

/// A single Raft consensus group managing one shard.
pub struct RaftGroup {
    pub shard_id: ShardId,
    pub role: RaftRole,
    pub term: Term,
    pub leader_id: Option<NodeId>,
    pub members: Vec<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub voted_for: Option<NodeId>,
    votes_received: usize,
}

impl RaftGroup {
    pub fn new(shard_id: ShardId, members: Vec<NodeId>) -> Self {
        Self {
            shard_id, role: RaftRole::Follower, term: 0, leader_id: None,
            members, log: Vec::new(), commit_index: 0, last_applied: 0,
            voted_for: None, votes_received: 0,
        }
    }

    /// Propose an operation. Only the leader may propose; appends to log and returns index.
    pub fn propose(&mut self, op: Operation) -> Result<u64, DistributedError> {
        if self.role != RaftRole::Leader {
            return Err(DistributedError::NotLeader {
                shard_id: self.shard_id, leader: self.leader_id,
            });
        }
        let index = self.log.len() as u64 + 1;
        self.log.push(LogEntry { index, term: self.term, operation: op });
        Ok(index)
    }

    /// Start an election: become candidate, increment term, vote for self.
    pub fn start_election(&mut self, local_node: NodeId) {
        self.term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(local_node);
        self.leader_id = None;
        self.votes_received = 1;
    }

    /// Receive a vote. Returns `true` if now elected leader (majority reached).
    pub fn receive_vote(&mut self, _from: NodeId, granted: bool) -> bool {
        if self.role != RaftRole::Candidate { return false; }
        if granted { self.votes_received += 1; }
        let majority = self.members.len() / 2 + 1;
        if self.votes_received >= majority {
            self.role = RaftRole::Leader;
            self.leader_id = self.voted_for;
            true
        } else {
            false
        }
    }

    /// Become a follower of the given leader at the specified term.
    pub fn set_leader(&mut self, leader: NodeId, term: Term) {
        self.role = RaftRole::Follower;
        self.leader_id = Some(leader);
        self.term = term;
        self.voted_for = None;
        self.votes_received = 0;
    }

    /// Advance the commit index up to `index`.
    pub fn commit_up_to(&mut self, index: u64) {
        if index > self.commit_index {
            let max = self.log.len() as u64;
            self.commit_index = if index > max { max } else { index };
        }
    }

    /// Return entries between last_applied and commit_index, then advance last_applied.
    pub fn apply_committed(&mut self) -> Vec<&LogEntry> {
        let (start, end) = (self.last_applied as usize, self.commit_index as usize);
        if start >= end { return Vec::new(); }
        let entries: Vec<&LogEntry> = self.log[start..end].iter().collect();
        self.last_applied = self.commit_index;
        entries
    }

    pub fn is_leader(&self) -> bool { self.role == RaftRole::Leader }
}

// --- MultiRaftManager ---

/// Manages all Raft groups hosted on this node.
pub struct MultiRaftManager {
    local_node_id: NodeId,
    groups: HashMap<ShardId, RaftGroup>,
    shard_leaders: HashMap<ShardId, NodeId>,
}

impl MultiRaftManager {
    pub fn new(local_node_id: NodeId) -> Self {
        Self { local_node_id, groups: HashMap::new(), shard_leaders: HashMap::new() }
    }

    pub fn create_group(&mut self, shard_id: ShardId, members: Vec<NodeId>) {
        self.groups.insert(shard_id, RaftGroup::new(shard_id, members));
    }

    pub fn remove_group(&mut self, shard_id: ShardId) {
        self.groups.remove(&shard_id);
        self.shard_leaders.remove(&shard_id);
    }

    pub fn get_group(&self, shard_id: ShardId) -> Option<&RaftGroup> {
        self.groups.get(&shard_id)
    }

    pub fn get_group_mut(&mut self, shard_id: ShardId) -> Option<&mut RaftGroup> {
        self.groups.get_mut(&shard_id)
    }

    /// Return all shard IDs where this node is the leader.
    pub fn groups_led_by_self(&self) -> Vec<ShardId> {
        self.groups.iter()
            .filter(|(_, g)| g.is_leader() && g.leader_id == Some(self.local_node_id))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Route a proposal to the correct Raft group.
    pub fn propose_to_shard(&mut self, shard_id: ShardId, op: Operation) -> Result<u64, DistributedError> {
        let group = self.groups.get_mut(&shard_id)
            .ok_or(DistributedError::ShardNotFound(shard_id))?;
        group.propose(op)
    }

    pub fn leader_for_shard(&self, shard_id: ShardId) -> Option<NodeId> {
        if let Some(group) = self.groups.get(&shard_id) {
            if let Some(leader) = group.leader_id { return Some(leader); }
        }
        self.shard_leaders.get(&shard_id).copied()
    }

    pub fn all_shards(&self) -> Vec<ShardId> { self.groups.keys().copied().collect() }
    pub fn group_count(&self) -> usize { self.groups.len() }
}

// --- RaftMessage (wire-format for inter-node Raft communication) ---

/// Messages exchanged between Raft nodes.
#[derive(Debug, Clone, PartialEq)]
pub enum RaftMessage {
    /// Request Vote (sent by candidates).
    RequestVote {
        shard_id: ShardId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: Term,
    },
    /// Vote response.
    VoteResponse {
        shard_id: ShardId,
        term: Term,
        voter_id: NodeId,
        granted: bool,
    },
    /// AppendEntries (log replication from leader).
    AppendEntries {
        shard_id: ShardId,
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    /// AppendEntries response.
    AppendEntriesResponse {
        shard_id: ShardId,
        term: Term,
        follower_id: NodeId,
        success: bool,
        match_index: u64,
    },
    /// Heartbeat (empty AppendEntries).
    Heartbeat {
        shard_id: ShardId,
        term: Term,
        leader_id: NodeId,
    },
    /// Heartbeat response.
    HeartbeatResponse {
        shard_id: ShardId,
        term: Term,
        follower_id: NodeId,
    },
}

impl RaftMessage {
    /// Get the shard_id from any message variant.
    pub fn shard_id(&self) -> ShardId {
        match self {
            RaftMessage::RequestVote { shard_id, .. }
            | RaftMessage::VoteResponse { shard_id, .. }
            | RaftMessage::AppendEntries { shard_id, .. }
            | RaftMessage::AppendEntriesResponse { shard_id, .. }
            | RaftMessage::Heartbeat { shard_id, .. }
            | RaftMessage::HeartbeatResponse { shard_id, .. } => *shard_id,
        }
    }

    /// Get the term from any message variant.
    pub fn term(&self) -> Term {
        match self {
            RaftMessage::RequestVote { term, .. }
            | RaftMessage::VoteResponse { term, .. }
            | RaftMessage::AppendEntries { term, .. }
            | RaftMessage::AppendEntriesResponse { term, .. }
            | RaftMessage::Heartbeat { term, .. }
            | RaftMessage::HeartbeatResponse { term, .. } => *term,
        }
    }

    /// Serialize to bytes (simple format: tag byte + fields).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            RaftMessage::RequestVote { shard_id, term, candidate_id, last_log_index, last_log_term } => {
                buf.push(1);
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
                buf.extend_from_slice(&candidate_id.to_le_bytes());
                buf.extend_from_slice(&last_log_index.to_le_bytes());
                buf.extend_from_slice(&last_log_term.to_le_bytes());
            }
            RaftMessage::VoteResponse { shard_id, term, voter_id, granted } => {
                buf.push(2);
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
                buf.extend_from_slice(&voter_id.to_le_bytes());
                buf.push(if *granted { 1 } else { 0 });
            }
            RaftMessage::AppendEntries { shard_id, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                buf.push(3);
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
                buf.extend_from_slice(&leader_id.to_le_bytes());
                buf.extend_from_slice(&prev_log_index.to_le_bytes());
                buf.extend_from_slice(&prev_log_term.to_le_bytes());
                buf.extend_from_slice(&leader_commit.to_le_bytes());
                buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for entry in entries {
                    buf.extend_from_slice(&entry.index.to_le_bytes());
                    buf.extend_from_slice(&entry.term.to_le_bytes());
                    let op_bytes = Self::operation_to_bytes(&entry.operation);
                    buf.extend_from_slice(&(op_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&op_bytes);
                }
            }
            RaftMessage::AppendEntriesResponse { shard_id, term, follower_id, success, match_index } => {
                buf.push(4);
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
                buf.extend_from_slice(&follower_id.to_le_bytes());
                buf.push(if *success { 1 } else { 0 });
                buf.extend_from_slice(&match_index.to_le_bytes());
            }
            RaftMessage::Heartbeat { shard_id, term, leader_id } => {
                buf.push(5);
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
                buf.extend_from_slice(&leader_id.to_le_bytes());
            }
            RaftMessage::HeartbeatResponse { shard_id, term, follower_id } => {
                buf.push(6);
                buf.extend_from_slice(&shard_id.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
                buf.extend_from_slice(&follower_id.to_le_bytes());
            }
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, DistributedError> {
        if data.is_empty() {
            return Err(DistributedError::MessageTooShort);
        }
        let tag = data[0];
        let rest = &data[1..];
        match tag {
            1 => Self::parse_request_vote(rest),
            2 => Self::parse_vote_response(rest),
            3 => Self::parse_append_entries(rest),
            4 => Self::parse_append_entries_response(rest),
            5 => Self::parse_heartbeat(rest),
            6 => Self::parse_heartbeat_response(rest),
            _ => Err(DistributedError::UnknownMessageTag(tag)),
        }
    }

    fn read_u64(data: &[u8], offset: usize) -> Result<u64, DistributedError> {
        if offset + 8 > data.len() {
            return Err(DistributedError::MessageTooShort);
        }
        let bytes: [u8; 8] = data[offset..offset+8].try_into().map_err(|_| DistributedError::MessageTooShort)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_u32(data: &[u8], offset: usize) -> Result<u32, DistributedError> {
        if offset + 4 > data.len() {
            return Err(DistributedError::MessageTooShort);
        }
        let bytes: [u8; 4] = data[offset..offset+4].try_into().map_err(|_| DistributedError::MessageTooShort)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn parse_request_vote(d: &[u8]) -> Result<RaftMessage, DistributedError> {
        if d.len() < 40 { return Err(DistributedError::MessageTooShort); }
        Ok(RaftMessage::RequestVote {
            shard_id: Self::read_u64(d, 0)?, term: Self::read_u64(d, 8)?,
            candidate_id: Self::read_u64(d, 16)?, last_log_index: Self::read_u64(d, 24)?,
            last_log_term: Self::read_u64(d, 32)?,
        })
    }

    fn parse_vote_response(d: &[u8]) -> Result<RaftMessage, DistributedError> {
        if d.len() < 25 { return Err(DistributedError::MessageTooShort); }
        Ok(RaftMessage::VoteResponse {
            shard_id: Self::read_u64(d, 0)?, term: Self::read_u64(d, 8)?,
            voter_id: Self::read_u64(d, 16)?, granted: d[24] != 0,
        })
    }

    fn parse_append_entries(d: &[u8]) -> Result<RaftMessage, DistributedError> {
        if d.len() < 52 { return Err(DistributedError::MessageTooShort); }
        let shard_id = Self::read_u64(d, 0)?;
        let term = Self::read_u64(d, 8)?;
        let leader_id = Self::read_u64(d, 16)?;
        let prev_log_index = Self::read_u64(d, 24)?;
        let prev_log_term = Self::read_u64(d, 32)?;
        let leader_commit = Self::read_u64(d, 40)?;
        let entry_count = Self::read_u32(d, 48)? as usize;
        let mut entries = Vec::with_capacity(entry_count);
        let mut offset = 52;
        for _ in 0..entry_count {
            if d.len() < offset + 20 { return Err(DistributedError::MessageTooShort); }
            let index = Self::read_u64(d, offset)?;
            let entry_term = Self::read_u64(d, offset + 8)?;
            let op_len = Self::read_u32(d, offset + 16)? as usize;
            offset += 20;
            if d.len() < offset + op_len { return Err(DistributedError::MessageTooShort); }
            let operation = Self::operation_from_bytes(&d[offset..offset+op_len])?;
            offset += op_len;
            entries.push(LogEntry { index, term: entry_term, operation });
        }
        Ok(RaftMessage::AppendEntries { shard_id, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit })
    }

    fn parse_append_entries_response(d: &[u8]) -> Result<RaftMessage, DistributedError> {
        if d.len() < 33 { return Err(DistributedError::MessageTooShort); }
        Ok(RaftMessage::AppendEntriesResponse {
            shard_id: Self::read_u64(d, 0)?, term: Self::read_u64(d, 8)?,
            follower_id: Self::read_u64(d, 16)?, success: d[24] != 0,
            match_index: Self::read_u64(d, 25)?,
        })
    }

    fn parse_heartbeat(d: &[u8]) -> Result<RaftMessage, DistributedError> {
        if d.len() < 24 { return Err(DistributedError::MessageTooShort); }
        Ok(RaftMessage::Heartbeat {
            shard_id: Self::read_u64(d, 0)?, term: Self::read_u64(d, 8)?, leader_id: Self::read_u64(d, 16)?,
        })
    }

    fn parse_heartbeat_response(d: &[u8]) -> Result<RaftMessage, DistributedError> {
        if d.len() < 24 { return Err(DistributedError::MessageTooShort); }
        Ok(RaftMessage::HeartbeatResponse {
            shard_id: Self::read_u64(d, 0)?, term: Self::read_u64(d, 8)?, follower_id: Self::read_u64(d, 16)?,
        })
    }

    fn operation_to_bytes(op: &Operation) -> Vec<u8> {
        let mut buf = Vec::new();
        match op {
            Operation::Noop => buf.push(0),
            Operation::Put { key, value } => {
                buf.push(1);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            Operation::Delete { key } => {
                buf.push(2);
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
            Operation::Sql(sql) => {
                buf.push(3);
                let bytes = sql.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Operation::TxnPrepare { txn_id } => {
                buf.push(4);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            Operation::TxnCommit { txn_id } => {
                buf.push(5);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            Operation::TxnAbort { txn_id } => {
                buf.push(6);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
        }
        buf
    }

    fn operation_from_bytes(data: &[u8]) -> Result<Operation, DistributedError> {
        if data.is_empty() { return Err(DistributedError::MessageTooShort); }
        match data[0] {
            0 => Ok(Operation::Noop),
            1 => {
                let key_len = Self::read_u32(data, 1)? as usize;
                if 5 + key_len > data.len() { return Err(DistributedError::MessageTooShort); }
                let key = data[5..5+key_len].to_vec();
                let value_len = Self::read_u32(data, 5+key_len)? as usize;
                if 9 + key_len + value_len > data.len() { return Err(DistributedError::MessageTooShort); }
                let value = data[9+key_len..9+key_len+value_len].to_vec();
                Ok(Operation::Put { key, value })
            }
            2 => {
                let key_len = Self::read_u32(data, 1)? as usize;
                if 5 + key_len > data.len() { return Err(DistributedError::MessageTooShort); }
                let key = data[5..5+key_len].to_vec();
                Ok(Operation::Delete { key })
            }
            3 => {
                let len = Self::read_u32(data, 1)? as usize;
                if 5 + len > data.len() { return Err(DistributedError::MessageTooShort); }
                let sql = String::from_utf8_lossy(&data[5..5+len]).into_owned();
                Ok(Operation::Sql(sql))
            }
            4 => {
                let txn_id = Self::read_u64(data, 1)?;
                Ok(Operation::TxnPrepare { txn_id })
            }
            5 => {
                let txn_id = Self::read_u64(data, 1)?;
                Ok(Operation::TxnCommit { txn_id })
            }
            6 => {
                let txn_id = Self::read_u64(data, 1)?;
                Ok(Operation::TxnAbort { txn_id })
            }
            _ => Err(DistributedError::UnknownMessageTag(data[0])),
        }
    }
}


// --- DistributedTxn & TransactionCoordinator ---

#[derive(Debug, Clone)]
pub struct DistributedTxn {
    pub txn_id: TxnId,
    pub coordinator_node: NodeId,
    pub phase: TxnPhase,
    pub shard_operations: HashMap<ShardId, Vec<Operation>>,
    pub prepare_votes: HashMap<ShardId, bool>,
    pub created_at_ms: u64,
}

// --- 2PC Recovery Log ---

#[derive(Debug, Clone, PartialEq)]
pub struct TxnLogEntry {
    pub txn_id: TxnId,
    pub phase: TxnPhase,
    pub shard_ids: Vec<ShardId>,
    pub timestamp_ms: u64,
}

pub struct TxnRecoveryLog {
    entries: Vec<TxnLogEntry>,
}

impl TxnRecoveryLog {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Append a state-transition entry to the log.
    pub fn log_transition(&mut self, txn_id: TxnId, phase: TxnPhase, shard_ids: Vec<ShardId>, timestamp_ms: u64) {
        self.entries.push(TxnLogEntry { txn_id, phase, shard_ids, timestamp_ms });
    }

    /// Return the last logged entry for each txn whose final phase is Preparing or Committing.
    pub fn in_doubt_txns(&self) -> Vec<TxnLogEntry> {
        let mut last_phase: HashMap<TxnId, &TxnLogEntry> = HashMap::new();
        for entry in &self.entries {
            last_phase.insert(entry.txn_id, entry);
        }
        last_phase.into_values()
            .filter(|e| e.phase == TxnPhase::Preparing || e.phase == TxnPhase::Committing)
            .cloned()
            .collect()
    }

    /// Return txn IDs that have reached a terminal state (Committed or Aborted).
    pub fn resolved_txn_ids(&self) -> HashSet<TxnId> {
        let mut resolved = HashSet::new();
        for entry in &self.entries {
            if entry.phase == TxnPhase::Committed || entry.phase == TxnPhase::Aborted {
                resolved.insert(entry.txn_id);
            }
        }
        resolved
    }

    /// Return all log entries for a given transaction.
    pub fn entries_for_txn(&self, txn_id: TxnId) -> Vec<&TxnLogEntry> {
        self.entries.iter().filter(|e| e.txn_id == txn_id).collect()
    }

    /// Remove all entries for transactions with ID < txn_id (log compaction).
    pub fn truncate_before(&mut self, txn_id: TxnId) {
        self.entries.retain(|e| e.txn_id >= txn_id);
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
}


pub struct TransactionCoordinator {
    local_node_id: NodeId,
    active_txns: HashMap<TxnId, DistributedTxn>,
    next_txn_id: TxnId,
    completed_txns: Vec<(TxnId, TxnPhase)>,
    timeout_ms: u64,
}

impl TransactionCoordinator {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id, active_txns: HashMap::new(),
            next_txn_id: 1, completed_txns: Vec::new(), timeout_ms: 30_000,
        }
    }

    /// Begin a new distributed transaction, returning its ID.
    pub fn begin(&mut self) -> TxnId {
        let txn_id = self.next_txn_id;
        self.next_txn_id += 1;
        self.active_txns.insert(txn_id, DistributedTxn {
            txn_id, coordinator_node: self.local_node_id, phase: TxnPhase::Active,
            shard_operations: HashMap::new(), prepare_votes: HashMap::new(),
            created_at_ms: 0,
        });
        txn_id
    }

    /// Begin with an explicit creation timestamp (milliseconds).
    pub fn begin_with_time(&mut self, created_at_ms: u64) -> TxnId {
        let txn_id = self.begin();
        if let Some(txn) = self.active_txns.get_mut(&txn_id) {
            txn.created_at_ms = created_at_ms;
        }
        txn_id
    }

    /// Add an operation to the transaction, targeting a specific shard.
    pub fn add_operation(&mut self, txn_id: TxnId, shard_id: ShardId, op: Operation) -> Result<(), DistributedError> {
        let txn = self.active_txns.get_mut(&txn_id).ok_or(DistributedError::TxnNotFound(txn_id))?;
        if txn.phase != TxnPhase::Active { return Err(DistributedError::TxnNotActive(txn_id)); }
        txn.shard_operations.entry(shard_id).or_default().push(op);
        Ok(())
    }

    /// Move to Preparing phase. Returns list of shard IDs that must vote.
    pub fn prepare(&mut self, txn_id: TxnId) -> Result<Vec<ShardId>, DistributedError> {
        let txn = self.active_txns.get_mut(&txn_id).ok_or(DistributedError::TxnNotFound(txn_id))?;
        if txn.phase != TxnPhase::Active {
            return Err(DistributedError::InvalidPhase {
                txn_id, expected: TxnPhase::Active, actual: txn.phase,
            });
        }
        txn.phase = TxnPhase::Preparing;
        let shards: Vec<ShardId> = txn.shard_operations.keys().copied().collect();
        for &s in &shards { txn.prepare_votes.insert(s, false); }
        Ok(shards)
    }

    /// Record a shard's prepare vote. Returns `Some(new_phase)` on transition,
    /// `None` if still waiting.
    pub fn vote(&mut self, txn_id: TxnId, shard_id: ShardId, prepared: bool) -> Result<Option<TxnPhase>, DistributedError> {
        let txn = self.active_txns.get_mut(&txn_id).ok_or(DistributedError::TxnNotFound(txn_id))?;
        if txn.phase != TxnPhase::Preparing {
            return Err(DistributedError::InvalidPhase {
                txn_id, expected: TxnPhase::Preparing, actual: txn.phase,
            });
        }
        txn.prepare_votes.insert(shard_id, prepared);
        if !prepared {
            txn.phase = TxnPhase::Aborting;
            return Ok(Some(TxnPhase::Aborting));
        }
        let all_yes = txn.shard_operations.keys()
            .all(|s| txn.prepare_votes.get(s).copied() == Some(true));
        if all_yes {
            txn.phase = TxnPhase::Committing;
            Ok(Some(TxnPhase::Committing))
        } else {
            Ok(None)
        }
    }

    /// Complete: Committing -> Committed, or Aborting -> Aborted.
    pub fn complete(&mut self, txn_id: TxnId) -> Result<TxnPhase, DistributedError> {
        let txn = self.active_txns.get_mut(&txn_id).ok_or(DistributedError::TxnNotFound(txn_id))?;
        let final_phase = match txn.phase {
            TxnPhase::Committing => TxnPhase::Committed,
            TxnPhase::Aborting => TxnPhase::Aborted,
            other => return Err(DistributedError::InvalidPhase {
                txn_id, expected: TxnPhase::Committing, actual: other,
            }),
        };
        self.active_txns.remove(&txn_id);
        self.completed_txns.push((txn_id, final_phase));
        Ok(final_phase)
    }

    pub fn get_txn(&self, txn_id: TxnId) -> Option<&DistributedTxn> { self.active_txns.get(&txn_id) }
    pub fn active_count(&self) -> usize { self.active_txns.len() }

    /// Return IDs of transactions that have exceeded the timeout.
    pub fn timed_out_txns(&self, now_ms: u64) -> Vec<TxnId> {
        self.active_txns.values()
            .filter(|txn| txn.created_at_ms > 0 && now_ms > txn.created_at_ms + self.timeout_ms)
            .map(|txn| txn.txn_id)
            .collect()
    }

    /// Recover in-doubt transactions from the recovery log after a crash.
    /// - Committing -> re-commit (Committed)
    /// - Preparing -> presumed abort (Aborted)
    pub fn recover_in_doubt(&mut self, log: &TxnRecoveryLog) -> Vec<(TxnId, TxnPhase)> {
        let in_doubt = log.in_doubt_txns();
        let mut resolved = Vec::new();
        for entry in in_doubt {
            let recovery_phase = match entry.phase {
                TxnPhase::Committing => TxnPhase::Committing,
                TxnPhase::Preparing => TxnPhase::Aborting,
                _ => continue,
            };
            // Re-insert the txn in the recovery phase so complete() can finalize it.
            self.active_txns.insert(entry.txn_id, DistributedTxn {
                txn_id: entry.txn_id,
                coordinator_node: self.local_node_id,
                phase: recovery_phase,
                shard_operations: entry.shard_ids.iter().map(|&s| (s, Vec::new())).collect(),
                prepare_votes: HashMap::new(),
                created_at_ms: entry.timestamp_ms,
            });
            // Ensure next_txn_id stays ahead.
            if entry.txn_id >= self.next_txn_id {
                self.next_txn_id = entry.txn_id + 1;
            }
            let final_phase = self.complete(entry.txn_id).unwrap();
            resolved.push((entry.txn_id, final_phase));
        }
        resolved
    }

    /// Record the current phase of the given transaction to the recovery log.
    pub fn log_phase_transition(&self, log: &mut TxnRecoveryLog, txn_id: TxnId) {
        if let Some(txn) = self.active_txns.get(&txn_id) {
            let shard_ids: Vec<ShardId> = txn.shard_operations.keys().copied().collect();
            log.log_transition(txn_id, txn.phase, shard_ids, txn.created_at_ms);
        }
    }
}

// --- QueryRouter ---

/// Result of routing a query.
#[derive(Debug, Clone)]
pub enum RouteDecision {
    Local { shard_id: ShardId },
    Forward { shard_id: ShardId, target_node: NodeId },
    ScatterGather { shards: Vec<(ShardId, NodeId)> },
    Standalone,
}

pub struct RouterStats {
    pub local_queries: u64,
    pub forwarded_queries: u64,
    pub scatter_gather_queries: u64,
    pub total_queries: u64,
}

pub struct QueryRouter {
    local_node_id: NodeId,
    mode: ClusterMode,
    shard_owners: HashMap<ShardId, NodeId>,
    shard_ranges: Vec<(ShardId, i64, i64)>,
    local_queries: u64,
    forwarded_queries: u64,
    scatter_queries: u64,
}

impl QueryRouter {
    pub fn new(local_node_id: NodeId, mode: ClusterMode) -> Self {
        Self {
            local_node_id, mode, shard_owners: HashMap::new(),
            shard_ranges: Vec::new(), local_queries: 0,
            forwarded_queries: 0, scatter_queries: 0,
        }
    }

    pub fn add_shard(&mut self, shard_id: ShardId, owner: NodeId, start_key: i64, end_key: i64) {
        self.shard_owners.insert(shard_id, owner);
        self.shard_ranges.retain(|(id, _, _)| *id != shard_id);
        self.shard_ranges.push((shard_id, start_key, end_key));
        self.shard_ranges.sort_by_key(|(_, s, _)| *s);
    }

    pub fn update_owner(&mut self, shard_id: ShardId, new_owner: NodeId) {
        self.shard_owners.insert(shard_id, new_owner);
    }

    /// Route a point query by key.
    pub fn route_by_key(&mut self, key: i64) -> RouteDecision {
        if self.mode == ClusterMode::Standalone {
            self.local_queries += 1;
            return RouteDecision::Standalone;
        }
        for &(shard_id, start, end) in &self.shard_ranges {
            if key >= start && key < end {
                if let Some(&owner) = self.shard_owners.get(&shard_id) {
                    if owner == self.local_node_id {
                        self.local_queries += 1;
                        return RouteDecision::Local { shard_id };
                    } else {
                        self.forwarded_queries += 1;
                        return RouteDecision::Forward { shard_id, target_node: owner };
                    }
                }
            }
        }
        RouteDecision::Standalone
    }

    /// Route a range scan. May need ScatterGather if the range spans shards.
    pub fn route_scan(&mut self, start_key: i64, end_key: i64) -> RouteDecision {
        if self.mode == ClusterMode::Standalone {
            self.local_queries += 1;
            return RouteDecision::Standalone;
        }
        let mut matching: Vec<(ShardId, NodeId)> = Vec::new();
        for &(shard_id, s, e) in &self.shard_ranges {
            if s < end_key && e > start_key {
                if let Some(&owner) = self.shard_owners.get(&shard_id) {
                    matching.push((shard_id, owner));
                }
            }
        }
        match matching.len() {
            0 => RouteDecision::Standalone,
            1 => {
                let (shard_id, owner) = matching[0];
                if owner == self.local_node_id {
                    self.local_queries += 1;
                    RouteDecision::Local { shard_id }
                } else {
                    self.forwarded_queries += 1;
                    RouteDecision::Forward { shard_id, target_node: owner }
                }
            }
            _ => { self.scatter_queries += 1; RouteDecision::ScatterGather { shards: matching } }
        }
    }

    /// Route a full table scan -- ScatterGather across all shards.
    pub fn route_full_scan(&mut self) -> RouteDecision {
        if self.mode == ClusterMode::Standalone {
            self.local_queries += 1;
            return RouteDecision::Standalone;
        }
        let all: Vec<(ShardId, NodeId)> = self.shard_ranges.iter()
            .filter_map(|(shard_id, _, _)| {
                self.shard_owners.get(shard_id).map(|&owner| (*shard_id, owner))
            })
            .collect();
        if all.is_empty() { return RouteDecision::Standalone; }
        self.scatter_queries += 1;
        RouteDecision::ScatterGather { shards: all }
    }

    pub fn stats(&self) -> RouterStats {
        RouterStats {
            local_queries: self.local_queries,
            forwarded_queries: self.forwarded_queries,
            scatter_gather_queries: self.scatter_queries,
            total_queries: self.local_queries + self.forwarded_queries + self.scatter_queries,
        }
    }
}

// --- ClusterStatus & ClusterCoordinator ---

pub struct ClusterStatus {
    pub node_id: NodeId,
    pub mode: ClusterMode,
    pub node_count: usize,
    pub shard_count: usize,
    pub epoch: u64,
    pub shards_led: usize,
    pub active_txns: usize,
}

/// Top-level coordinator that ties Raft, transactions, and routing together.
pub struct ClusterCoordinator {
    local_node_id: NodeId,
    mode: ClusterMode,
    raft_manager: Option<MultiRaftManager>,
    txn_coordinator: TransactionCoordinator,
    router: QueryRouter,
    cluster_nodes: HashMap<NodeId, String>,
    epoch: u64,
}

impl ClusterCoordinator {
    pub fn new_standalone(node_id: NodeId) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(node_id, "localhost".to_string());
        Self {
            local_node_id: node_id, mode: ClusterMode::Standalone,
            raft_manager: None, txn_coordinator: TransactionCoordinator::new(node_id),
            router: QueryRouter::new(node_id, ClusterMode::Standalone),
            cluster_nodes: nodes, epoch: 0,
        }
    }

    pub fn new_primary_replica(local_id: NodeId, peer_id: NodeId, peer_addr: &str) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(local_id, "localhost".to_string());
        nodes.insert(peer_id, peer_addr.to_string());
        Self {
            local_node_id: local_id, mode: ClusterMode::PrimaryReplica,
            raft_manager: None, txn_coordinator: TransactionCoordinator::new(local_id),
            router: QueryRouter::new(local_id, ClusterMode::PrimaryReplica),
            cluster_nodes: nodes, epoch: 0,
        }
    }

    pub fn new_multi_raft(local_id: NodeId, initial_nodes: Vec<(NodeId, String)>) -> Self {
        let mut nodes = HashMap::new();
        for (id, addr) in &initial_nodes { nodes.insert(*id, addr.clone()); }
        nodes.entry(local_id).or_insert_with(|| "localhost".to_string());
        Self {
            local_node_id: local_id, mode: ClusterMode::MultiRaft,
            raft_manager: Some(MultiRaftManager::new(local_id)),
            txn_coordinator: TransactionCoordinator::new(local_id),
            router: QueryRouter::new(local_id, ClusterMode::MultiRaft),
            cluster_nodes: nodes, epoch: 0,
        }
    }

    pub fn add_node(&mut self, node_id: NodeId, address: String) {
        self.cluster_nodes.insert(node_id, address);
        self.epoch += 1;
    }

    pub fn remove_node(&mut self, node_id: NodeId) {
        self.cluster_nodes.remove(&node_id);
        self.epoch += 1;
    }

    pub fn mode(&self) -> ClusterMode { self.mode }
    pub fn epoch(&self) -> u64 { self.epoch }
    pub fn node_count(&self) -> usize { self.cluster_nodes.len() }

    pub fn route_query(&mut self, key: i64) -> RouteDecision {
        self.router.route_by_key(key)
    }

    pub fn begin_txn(&mut self) -> TxnId { self.txn_coordinator.begin() }

    pub fn propose(&mut self, shard_id: ShardId, op: Operation) -> Result<u64, DistributedError> {
        match &mut self.raft_manager {
            Some(mgr) => mgr.propose_to_shard(shard_id, op),
            None => Err(DistributedError::StandaloneMode),
        }
    }

    pub fn status(&self) -> ClusterStatus {
        let shard_count = self.raft_manager.as_ref().map_or(0, |m| m.group_count());
        let shards_led = self.raft_manager.as_ref().map_or(0, |m| m.groups_led_by_self().len());
        ClusterStatus {
            node_id: self.local_node_id, mode: self.mode,
            node_count: self.cluster_nodes.len(), shard_count,
            epoch: self.epoch, shards_led,
            active_txns: self.txn_coordinator.active_count(),
        }
    }

    pub fn raft_manager_mut(&mut self) -> Option<&mut MultiRaftManager> { self.raft_manager.as_mut() }
    pub fn txn_coordinator_mut(&mut self) -> &mut TransactionCoordinator { &mut self.txn_coordinator }
    pub fn router_mut(&mut self) -> &mut QueryRouter { &mut self.router }

    /// Returns true if this node is the leader of shard 0, or if running standalone.
    pub fn is_leader(&self) -> bool {
        match &self.raft_manager {
            None => true, // standalone — acts as its own leader
            Some(mgr) => mgr.groups_led_by_self().contains(&0),
        }
    }

    /// Returns the transport address of the current leader of shard 0, if known.
    pub fn leader_addr(&self) -> Option<String> {
        let mgr = self.raft_manager.as_ref()?;
        let leader_id = mgr.leader_for_shard(0)?;
        self.cluster_nodes.get(&leader_id).cloned()
    }

    /// Return node IDs of all peers (excluding self).
    pub fn peer_node_ids(&self) -> Vec<NodeId> {
        self.cluster_nodes.keys()
            .filter(|&&id| id != self.local_node_id)
            .copied()
            .collect()
    }
}


// =============================================================================
// Hybrid Logical Clock (HLC)
// =============================================================================

/// A hybrid timestamp combining physical wall-clock time, a logical counter,
/// and a node identifier for total ordering across distributed nodes.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct HybridTimestamp {
    /// Wall clock milliseconds since UNIX epoch.
    pub physical_ms: u64,
    /// Logical counter within the same physical time.
    pub logical: u32,
    /// Originating node for tie-breaking.
    pub node_id: u16,
}

impl HybridTimestamp {
    /// Create a new hybrid timestamp.
    pub fn new(physical_ms: u64, logical: u32, node_id: u16) -> Self {
        Self { physical_ms, logical, node_id }
    }

    /// Serialize to a 14-byte big-endian wire format:
    /// [8 bytes physical_ms | 4 bytes logical | 2 bytes node_id]
    pub fn to_bytes(&self) -> [u8; 14] {
        let mut buf = [0u8; 14];
        buf[0..8].copy_from_slice(&self.physical_ms.to_be_bytes());
        buf[8..12].copy_from_slice(&self.logical.to_be_bytes());
        buf[12..14].copy_from_slice(&self.node_id.to_be_bytes());
        buf
    }

    /// Deserialize from a 14-byte big-endian wire format.
    pub fn from_bytes(bytes: [u8; 14]) -> Self {
        let physical_ms = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let logical = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let node_id = u16::from_be_bytes(bytes[12..14].try_into().unwrap());
        Self { physical_ms, logical, node_id }
    }
}

impl Ord for HybridTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.physical_ms.cmp(&other.physical_ms)
            .then(self.logical.cmp(&other.logical))
            .then(self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for HybridTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for HybridTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.physical_ms, self.logical, self.node_id)
    }
}

/// A Hybrid Logical Clock that generates monotonically increasing timestamps
/// combining physical wall-clock time with a logical counter to handle clock
/// skew in distributed systems.
pub struct HybridLogicalClock {
    node_id: u16,
    latest: HybridTimestamp,
}

impl HybridLogicalClock {
    /// Create a new HLC for the given node.
    pub fn new(node_id: u16) -> Self {
        Self {
            node_id,
            latest: HybridTimestamp::new(0, 0, node_id),
        }
    }

    /// Return the current physical time in milliseconds since UNIX epoch.
    fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as u64
    }

    /// Generate a new monotonically increasing timestamp.
    ///
    /// - If physical time advanced past latest, use new physical time with logical=0.
    /// - If physical time equals latest.physical_ms, increment logical.
    /// - If physical time went backwards (clock skew), keep latest.physical_ms with logical+1.
    pub fn now(&mut self) -> HybridTimestamp {
        let pt = Self::physical_now();
        if pt > self.latest.physical_ms {
            self.latest = HybridTimestamp::new(pt, 0, self.node_id);
        } else {
            // Same or backwards physical time: increment logical counter.
            self.latest = HybridTimestamp::new(
                self.latest.physical_ms,
                self.latest.logical + 1,
                self.node_id,
            );
        }
        self.latest
    }

    /// Update the local clock upon receiving a remote timestamp, ensuring
    /// the resulting timestamp is strictly greater than both the local
    /// latest and the received timestamp.
    pub fn update(&mut self, received: &HybridTimestamp) -> HybridTimestamp {
        let pt = Self::physical_now();
        let max_pt = pt.max(received.physical_ms).max(self.latest.physical_ms);

        let new_logical = if max_pt == self.latest.physical_ms
            && max_pt == received.physical_ms
        {
            // All three times are equal: take max of both logical counters + 1.
            self.latest.logical.max(received.logical) + 1
        } else if max_pt == self.latest.physical_ms {
            // Local physical is the max: advance local logical.
            self.latest.logical + 1
        } else if max_pt == received.physical_ms {
            // Received physical is the max: advance received logical.
            received.logical + 1
        } else {
            // Physical clock advanced past both: reset logical.
            0
        };

        self.latest = HybridTimestamp::new(max_pt, new_logical, self.node_id);
        self.latest
    }

    /// Peek at the latest timestamp without advancing the clock.
    pub fn latest(&self) -> HybridTimestamp {
        self.latest
    }
}


// ============================================================================
// Gap 13: Follower Reads — serve consistent reads from replicas
// ============================================================================

/// A follower read request: a read that can be served by a replica
/// if the replica's HLC timestamp is >= the read timestamp.
#[derive(Debug, Clone)]
pub struct FollowerReadRequest {
    /// The minimum HLC timestamp required for the read to be consistent.
    pub min_timestamp: HybridTimestamp,
    /// The table/key being read.
    pub key: String,
}

/// Result of checking whether a follower can serve a read.
#[derive(Debug, Clone, PartialEq)]
pub enum FollowerReadResult {
    /// The follower has sufficiently recent data; serve locally.
    ServeLocally,
    /// The follower is behind; redirect to leader.
    RedirectToLeader,
    /// The follower's timestamp is unknown; redirect to leader.
    Unknown,
}

/// Manages follower read eligibility based on HLC timestamps.
pub struct FollowerReadManager {
    /// This node's latest applied HLC timestamp.
    local_timestamp: HybridTimestamp,
    /// The leader's node ID (to redirect if needed).
    pub leader_node: u64,
    /// Maximum allowed staleness before redirecting to leader (in milliseconds).
    pub max_staleness_ms: u64,
}

impl FollowerReadManager {
    pub fn new(leader_node: u64) -> Self {
        FollowerReadManager {
            local_timestamp: HybridTimestamp { physical_ms: 0, logical: 0, node_id: 0 },
            leader_node,
            max_staleness_ms: 5000, // default 5 second staleness
        }
    }

    /// Update the local applied timestamp (called when WAL entries are applied).
    pub fn advance_timestamp(&mut self, ts: HybridTimestamp) {
        if ts > self.local_timestamp {
            self.local_timestamp = ts;
        }
    }

    /// Check if this follower can serve a read at the given timestamp.
    pub fn can_serve(&self, request: &FollowerReadRequest) -> FollowerReadResult {
        if self.local_timestamp.physical_ms == 0 {
            return FollowerReadResult::Unknown;
        }
        if self.local_timestamp >= request.min_timestamp {
            FollowerReadResult::ServeLocally
        } else {
            FollowerReadResult::RedirectToLeader
        }
    }

    /// Check if this follower can serve a "bounded staleness" read.
    /// If the local timestamp is within max_staleness_ms of the request, serve locally.
    pub fn can_serve_bounded(&self, current_time_ms: u64) -> FollowerReadResult {
        if self.local_timestamp.physical_ms == 0 {
            return FollowerReadResult::Unknown;
        }
        let staleness = current_time_ms.saturating_sub(self.local_timestamp.physical_ms);
        if staleness <= self.max_staleness_ms {
            FollowerReadResult::ServeLocally
        } else {
            FollowerReadResult::RedirectToLeader
        }
    }

    pub fn local_timestamp(&self) -> &HybridTimestamp {
        &self.local_timestamp
    }
}

// ============================================================================
// Gap 14: Parallel Commits — pipeline commit with write intents
// ============================================================================

/// A write intent — a tentative write that may be committed or aborted.
#[derive(Debug, Clone)]
pub struct WriteIntent {
    pub txn_id: u64,
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: HybridTimestamp,
    pub status: IntentStatus,
}

/// Status of a write intent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IntentStatus {
    /// The intent is pending — the transaction hasn't committed yet.
    Pending,
    /// The transaction committed; this intent should be resolved to a value.
    Committed,
    /// The transaction aborted; this intent should be cleaned up.
    Aborted,
}

/// The parallel commit coordinator.
///
/// Implements pipelined commits: writes are staged as intents before the
/// transaction commit is confirmed. The commit can proceed asynchronously
/// while intents are resolved in the background.
pub struct ParallelCommitCoordinator {
    /// Active write intents, keyed by (txn_id, key).
    intents: HashMap<(u64, String), WriteIntent>,
    /// Transaction commit status.
    txn_status: HashMap<u64, IntentStatus>,
    next_txn_id: u64,
}

impl ParallelCommitCoordinator {
    pub fn new() -> Self {
        ParallelCommitCoordinator {
            intents: HashMap::new(),
            txn_status: HashMap::new(),
            next_txn_id: 1,
        }
    }

    /// Begin a new parallel commit transaction.
    pub fn begin_txn(&mut self) -> u64 {
        let id = self.next_txn_id;
        self.next_txn_id += 1;
        self.txn_status.insert(id, IntentStatus::Pending);
        id
    }

    /// Stage a write intent for a transaction.
    pub fn stage_intent(
        &mut self,
        txn_id: u64,
        key: &str,
        value: Vec<u8>,
        timestamp: HybridTimestamp,
    ) {
        let intent = WriteIntent {
            txn_id,
            key: key.to_string(),
            value,
            timestamp,
            status: IntentStatus::Pending,
        };
        self.intents.insert((txn_id, key.to_string()), intent);
    }

    /// Commit a transaction: mark all its intents as committed.
    /// In a real system, this would be followed by async intent resolution.
    pub fn commit_txn(&mut self, txn_id: u64) -> usize {
        self.txn_status.insert(txn_id, IntentStatus::Committed);
        let mut resolved = 0;
        for ((tid, _), intent) in &mut self.intents {
            if *tid == txn_id && intent.status == IntentStatus::Pending {
                intent.status = IntentStatus::Committed;
                resolved += 1;
            }
        }
        resolved
    }

    /// Abort a transaction: mark all its intents as aborted.
    pub fn abort_txn(&mut self, txn_id: u64) -> usize {
        self.txn_status.insert(txn_id, IntentStatus::Aborted);
        let mut aborted = 0;
        for ((tid, _), intent) in &mut self.intents {
            if *tid == txn_id && intent.status == IntentStatus::Pending {
                intent.status = IntentStatus::Aborted;
                aborted += 1;
            }
        }
        aborted
    }

    /// Clean up resolved/aborted intents for a transaction.
    pub fn cleanup_intents(&mut self, txn_id: u64) -> usize {
        let before = self.intents.len();
        self.intents.retain(|(tid, _), _| *tid != txn_id);
        before - self.intents.len()
    }

    /// Read a key, resolving any write intents encountered.
    pub fn read_with_intent_resolution(&self, key: &str) -> Option<(&[u8], IntentStatus)> {
        // Check for intents on this key
        for ((_, k), intent) in &self.intents {
            if k == key {
                return Some((&intent.value, intent.status));
            }
        }
        None
    }

    pub fn pending_intent_count(&self) -> usize {
        self.intents.values().filter(|i| i.status == IntentStatus::Pending).count()
    }

    pub fn total_intent_count(&self) -> usize {
        self.intents.len()
    }
}

// ============================================================================
// Gap 15: Multi-Region Awareness — region-pinned data and locality routing
// ============================================================================

/// Geographic region identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RegionId(pub String);

/// Zone configuration for a table: which regions own which key ranges.
#[derive(Debug, Clone)]
pub struct ZoneConfig {
    /// Table this zone config applies to.
    pub table: String,
    /// Primary region where the leaseholder should reside.
    pub primary_region: RegionId,
    /// Regions where replicas should be placed.
    pub replica_regions: Vec<RegionId>,
    /// If true, reads are restricted to the primary region for strong consistency.
    pub strong_consistency: bool,
}

/// A node's region assignment.
#[derive(Debug, Clone)]
pub struct RegionNode {
    pub node_id: u64,
    pub region: RegionId,
    pub is_leaseholder: bool,
}

/// Multi-region routing manager.
pub struct MultiRegionManager {
    /// Zone configurations per table.
    zone_configs: HashMap<String, ZoneConfig>,
    /// Known nodes and their region assignments.
    nodes: Vec<RegionNode>,
    /// This node's region.
    pub local_region: RegionId,
}

impl MultiRegionManager {
    pub fn new(local_region: RegionId) -> Self {
        MultiRegionManager {
            zone_configs: HashMap::new(),
            nodes: Vec::new(),
            local_region,
        }
    }

    /// Set the zone configuration for a table.
    pub fn set_zone_config(&mut self, config: ZoneConfig) {
        self.zone_configs.insert(config.table.clone(), config);
    }

    /// Register a node in a region.
    pub fn register_node(&mut self, node_id: u64, region: RegionId, is_leaseholder: bool) {
        self.nodes.push(RegionNode {
            node_id,
            region,
            is_leaseholder,
        });
    }

    /// Get the preferred node for reading a table, based on locality.
    pub fn route_read(&self, table: &str) -> Option<u64> {
        let config = self.zone_configs.get(table)?;

        if config.strong_consistency {
            // Must read from leaseholder in primary region
            return self.find_leaseholder(&config.primary_region);
        }

        // Prefer a local region node, then primary, then any
        if let Some(node) = self.find_node_in_region(&self.local_region) {
            return Some(node);
        }
        if let Some(node) = self.find_node_in_region(&config.primary_region) {
            return Some(node);
        }
        self.nodes.first().map(|n| n.node_id)
    }

    /// Get the node for writing to a table (always the leaseholder in primary region).
    pub fn route_write(&self, table: &str) -> Option<u64> {
        let config = self.zone_configs.get(table)?;
        self.find_leaseholder(&config.primary_region)
    }

    /// Check if a read is local (can be served without cross-region traffic).
    pub fn is_local_read(&self, table: &str) -> bool {
        if let Some(config) = self.zone_configs.get(table) {
            if config.strong_consistency {
                config.primary_region == self.local_region
            } else {
                self.nodes.iter().any(|n| n.region == self.local_region)
            }
        } else {
            false
        }
    }

    fn find_leaseholder(&self, region: &RegionId) -> Option<u64> {
        self.nodes
            .iter()
            .find(|n| n.region == *region && n.is_leaseholder)
            .map(|n| n.node_id)
    }

    fn find_node_in_region(&self, region: &RegionId) -> Option<u64> {
        self.nodes.iter().find(|n| n.region == *region).map(|n| n.node_id)
    }

    pub fn zone_config_count(&self) -> usize {
        self.zone_configs.len()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_propose_only_as_leader() {
        let mut group = RaftGroup::new(1, vec![1, 2, 3]);
        assert!(group.propose(Operation::Noop).is_err());
        group.start_election(1);
        group.receive_vote(2, true);
        assert!(group.is_leader());
        let idx = group.propose(Operation::Noop).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn raft_election_and_vote_counting() {
        let mut group = RaftGroup::new(1, vec![10, 20, 30, 40, 50]);
        group.start_election(10);
        assert_eq!(group.role, RaftRole::Candidate);
        assert_eq!(group.term, 1);
        // Need majority of 5 = 3 votes. Already have 1 (self).
        assert!(!group.receive_vote(20, true)); // 2 of 5
        assert!(group.receive_vote(30, true));  // 3 of 5 -- majority
        assert_eq!(group.role, RaftRole::Leader);
        assert_eq!(group.leader_id, Some(10));
    }

    #[test]
    fn raft_commit_and_apply() {
        let mut group = RaftGroup::new(1, vec![1, 2, 3]);
        group.start_election(1);
        group.receive_vote(2, true);
        group.propose(Operation::Noop).unwrap();
        group.propose(Operation::Put { key: b"k".to_vec(), value: b"v".to_vec() }).unwrap();
        group.propose(Operation::Noop).unwrap();
        assert_eq!(group.log.len(), 3);

        group.commit_up_to(2);
        assert_eq!(group.commit_index, 2);
        let applied = group.apply_committed();
        assert_eq!(applied.len(), 2);
        assert_eq!(applied[0].index, 1);
        assert_eq!(applied[1].index, 2);
        assert_eq!(group.last_applied, 2);
        assert!(group.apply_committed().is_empty());

        group.commit_up_to(3);
        let applied = group.apply_committed();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].index, 3);
    }

    #[test]
    fn raft_set_leader_resets_state() {
        let mut group = RaftGroup::new(1, vec![1, 2, 3]);
        group.start_election(1);
        assert_eq!(group.role, RaftRole::Candidate);
        group.set_leader(2, 2);
        assert_eq!(group.role, RaftRole::Follower);
        assert_eq!(group.leader_id, Some(2));
        assert_eq!(group.term, 2);
        assert_eq!(group.voted_for, None);
    }

    #[test]
    fn multi_raft_create_remove_groups() {
        let mut mgr = MultiRaftManager::new(1);
        mgr.create_group(10, vec![1, 2, 3]);
        mgr.create_group(20, vec![1, 2, 3]);
        assert_eq!(mgr.group_count(), 2);
        assert!(mgr.get_group(10).is_some());
        mgr.remove_group(10);
        assert_eq!(mgr.group_count(), 1);
        assert!(mgr.get_group(10).is_none());
        assert!(mgr.get_group(20).is_some());
    }

    #[test]
    fn multi_raft_propose_to_shard_routes_correctly() {
        let mut mgr = MultiRaftManager::new(1);
        mgr.create_group(10, vec![1, 2, 3]);
        assert!(mgr.propose_to_shard(10, Operation::Noop).is_err());
        {
            let g = mgr.get_group_mut(10).unwrap();
            g.start_election(1);
            g.receive_vote(2, true);
        }
        assert_eq!(mgr.propose_to_shard(10, Operation::Noop).unwrap(), 1);
        assert!(matches!(mgr.propose_to_shard(99, Operation::Noop), Err(DistributedError::ShardNotFound(99))));
    }

    #[test]
    fn multi_raft_groups_led_by_self() {
        let mut mgr = MultiRaftManager::new(1);
        mgr.create_group(10, vec![1, 2, 3]);
        mgr.create_group(20, vec![1, 2, 3]);
        mgr.create_group(30, vec![1, 2, 3]);
        for sid in [10, 30] {
            let g = mgr.get_group_mut(sid).unwrap();
            g.start_election(1);
            g.receive_vote(2, true);
        }
        mgr.get_group_mut(20).unwrap().set_leader(2, 1);
        let mut led = mgr.groups_led_by_self();
        led.sort();
        assert_eq!(led, vec![10, 30]);
    }

    #[test]
    fn txn_full_2pc_commit_flow() {
        let mut coord = TransactionCoordinator::new(1);
        let txn_id = coord.begin();
        assert_eq!(coord.active_count(), 1);
        coord.add_operation(txn_id, 10, Operation::Noop).unwrap();
        coord.add_operation(txn_id, 20, Operation::Noop).unwrap();
        let shards = coord.prepare(txn_id).unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(coord.vote(txn_id, 10, true).unwrap(), None);
        assert_eq!(coord.vote(txn_id, 20, true).unwrap(), Some(TxnPhase::Committing));
        assert_eq!(coord.complete(txn_id).unwrap(), TxnPhase::Committed);
        assert_eq!(coord.active_count(), 0);
    }

    #[test]
    fn txn_2pc_abort_flow() {
        let mut coord = TransactionCoordinator::new(1);
        let txn_id = coord.begin();
        coord.add_operation(txn_id, 10, Operation::Noop).unwrap();
        coord.add_operation(txn_id, 20, Operation::Noop).unwrap();
        coord.prepare(txn_id).unwrap();
        coord.vote(txn_id, 10, true).unwrap();
        assert_eq!(coord.vote(txn_id, 20, false).unwrap(), Some(TxnPhase::Aborting));
        assert_eq!(coord.complete(txn_id).unwrap(), TxnPhase::Aborted);
    }

    #[test]
    fn txn_timeout_detection() {
        let mut coord = TransactionCoordinator::new(1);
        let txn1 = coord.begin_with_time(1000);
        let txn2 = coord.begin_with_time(5000);
        let _txn3 = coord.begin(); // created_at_ms = 0, never times out
        // timeout = 30_000ms: txn1 expires at 31000, txn2 at 35000
        let timed_out = coord.timed_out_txns(32_000);
        assert_eq!(timed_out.len(), 1);
        assert!(timed_out.contains(&txn1));
        assert!(!timed_out.contains(&txn2));
    }


    #[test]
    fn router_route_by_key_forward() {
        let mut router = QueryRouter::new(1, ClusterMode::MultiRaft);
        router.add_shard(10, 1, 0, 100);
        router.add_shard(20, 2, 100, 200);
        match router.route_by_key(150) {
            RouteDecision::Forward { shard_id, target_node } => {
                assert_eq!(shard_id, 20);
                assert_eq!(target_node, 2);
            }
            other => panic!("expected Forward, got: {other:?}"),
        }
    }

    #[test]
    fn router_route_scan_scatter_gather() {
        let mut router = QueryRouter::new(1, ClusterMode::MultiRaft);
        router.add_shard(10, 1, 0, 100);
        router.add_shard(20, 2, 100, 200);
        router.add_shard(30, 3, 200, 300);
        match router.route_scan(50, 250) {
            RouteDecision::ScatterGather { shards } => {
                assert_eq!(shards.len(), 3);
                let ids: Vec<ShardId> = shards.iter().map(|(s, _)| *s).collect();
                assert!(ids.contains(&10) && ids.contains(&20) && ids.contains(&30));
            }
            other => panic!("expected ScatterGather, got: {other:?}"),
        }
    }

    #[test]
    fn router_route_full_scan() {
        let mut router = QueryRouter::new(1, ClusterMode::MultiRaft);
        router.add_shard(10, 1, 0, 100);
        router.add_shard(20, 2, 100, 200);
        match router.route_full_scan() {
            RouteDecision::ScatterGather { shards } => assert_eq!(shards.len(), 2),
            other => panic!("expected ScatterGather, got: {other:?}"),
        }
        assert_eq!(router.stats().scatter_gather_queries, 1);
    }

    #[test]
    fn cluster_standalone_mode() {
        let coord = ClusterCoordinator::new_standalone(1);
        assert_eq!(coord.mode(), ClusterMode::Standalone);
        assert_eq!(coord.node_count(), 1);
        let status = coord.status();
        assert_eq!(status.mode, ClusterMode::Standalone);
        assert_eq!(status.shard_count, 0);
        assert_eq!(status.shards_led, 0);
    }

    #[test]
    fn cluster_add_remove_nodes_increments_epoch() {
        let mut coord = ClusterCoordinator::new_standalone(1);
        assert_eq!(coord.epoch(), 0);
        coord.add_node(2, "192.168.1.2:5432".to_string());
        assert_eq!(coord.epoch(), 1);
        assert_eq!(coord.node_count(), 2);
        coord.add_node(3, "192.168.1.3:5432".to_string());
        assert_eq!(coord.epoch(), 2);
        coord.remove_node(2);
        assert_eq!(coord.epoch(), 3);
        assert_eq!(coord.node_count(), 2);
    }

    #[test]
    fn cluster_multi_raft_with_shard_groups() {
        let mut coord = ClusterCoordinator::new_multi_raft(1, vec![
            (1, "localhost:5432".to_string()),
            (2, "peer1:5432".to_string()),
            (3, "peer2:5432".to_string()),
        ]);
        assert_eq!(coord.mode(), ClusterMode::MultiRaft);
        assert_eq!(coord.node_count(), 3);
        {
            let mgr = coord.raft_manager_mut().unwrap();
            mgr.create_group(10, vec![1, 2, 3]);
            mgr.create_group(20, vec![1, 2, 3]);
        }
        assert_eq!(coord.status().shard_count, 2);
        {
            let mgr = coord.raft_manager_mut().unwrap();
            let g = mgr.get_group_mut(10).unwrap();
            g.start_election(1);
            g.receive_vote(2, true);
        }
        assert_eq!(coord.propose(10, Operation::Noop).unwrap(), 1);
        assert!(coord.propose(20, Operation::Noop).is_err());
    }

    #[test]
    fn cluster_standalone_propose_returns_error() {
        let mut coord = ClusterCoordinator::new_standalone(1);
        assert!(matches!(coord.propose(10, Operation::Noop), Err(DistributedError::StandaloneMode)));
    }

    #[test]
    fn cluster_begin_txn_and_route() {
        let mut coord = ClusterCoordinator::new_multi_raft(1, vec![
            (1, "localhost".to_string()), (2, "peer".to_string()),
        ]);
        let txn_id = coord.begin_txn();
        assert_eq!(txn_id, 1);
        assert_eq!(coord.status().active_txns, 1);
        coord.router_mut().add_shard(10, 1, 0, 100);
        match coord.route_query(42) {
            RouteDecision::Local { shard_id } => assert_eq!(shard_id, 10),
            other => panic!("expected Local, got: {other:?}"),
        }
    }

    // ── 2PC Recovery Log tests ──────────────────────────────────────

    #[test]
    fn txn_recovery_log_basic() {
        let mut log = TxnRecoveryLog::new();
        assert!(log.is_empty());
        log.log_transition(1, TxnPhase::Active, vec![10, 20], 1000);
        log.log_transition(1, TxnPhase::Preparing, vec![10, 20], 2000);
        assert_eq!(log.len(), 2);
        let entries = log.entries_for_txn(1);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].phase, TxnPhase::Active);
        assert_eq!(entries[1].phase, TxnPhase::Preparing);
    }

    #[test]
    fn txn_recovery_log_in_doubt_txns() {
        let mut log = TxnRecoveryLog::new();
        // Txn 1: fully committed — not in doubt
        log.log_transition(1, TxnPhase::Active, vec![10], 1000);
        log.log_transition(1, TxnPhase::Preparing, vec![10], 2000);
        log.log_transition(1, TxnPhase::Committing, vec![10], 3000);
        log.log_transition(1, TxnPhase::Committed, vec![10], 4000);
        // Txn 2: stuck in Preparing
        log.log_transition(2, TxnPhase::Active, vec![10, 20], 5000);
        log.log_transition(2, TxnPhase::Preparing, vec![10, 20], 6000);
        // Txn 3: stuck in Committing
        log.log_transition(3, TxnPhase::Active, vec![30], 7000);
        log.log_transition(3, TxnPhase::Preparing, vec![30], 8000);
        log.log_transition(3, TxnPhase::Committing, vec![30], 9000);

        let in_doubt = log.in_doubt_txns();
        assert_eq!(in_doubt.len(), 2);
        let ids: Vec<TxnId> = in_doubt.iter().map(|e| e.txn_id).collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    #[test]
    fn txn_recovery_log_resolved_txn_ids() {
        let mut log = TxnRecoveryLog::new();
        log.log_transition(1, TxnPhase::Committed, vec![10], 1000);
        log.log_transition(2, TxnPhase::Aborted, vec![20], 2000);
        log.log_transition(3, TxnPhase::Preparing, vec![30], 3000);
        let resolved = log.resolved_txn_ids();
        assert!(resolved.contains(&1));
        assert!(resolved.contains(&2));
        assert!(!resolved.contains(&3));
    }

    #[test]
    fn txn_recovery_log_truncate_before() {
        let mut log = TxnRecoveryLog::new();
        log.log_transition(1, TxnPhase::Committed, vec![10], 1000);
        log.log_transition(2, TxnPhase::Committed, vec![20], 2000);
        log.log_transition(3, TxnPhase::Preparing, vec![30], 3000);
        assert_eq!(log.len(), 3);
        log.truncate_before(3);
        assert_eq!(log.len(), 1);
        assert!(log.entries_for_txn(1).is_empty());
        assert!(log.entries_for_txn(2).is_empty());
        assert_eq!(log.entries_for_txn(3).len(), 1);
    }

    #[test]
    fn txn_coordinator_recover_in_doubt_preparing() {
        let mut coord = TransactionCoordinator::new(1);
        let mut log = TxnRecoveryLog::new();
        // Simulate crash: txn 1 was in Preparing phase
        log.log_transition(1, TxnPhase::Active, vec![10, 20], 1000);
        log.log_transition(1, TxnPhase::Preparing, vec![10, 20], 2000);
        let resolved = coord.recover_in_doubt(&log);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0], (1, TxnPhase::Aborted));
    }

    #[test]
    fn txn_coordinator_recover_in_doubt_committing() {
        let mut coord = TransactionCoordinator::new(1);
        let mut log = TxnRecoveryLog::new();
        // Simulate crash: txn 2 was in Committing phase
        log.log_transition(2, TxnPhase::Active, vec![10], 1000);
        log.log_transition(2, TxnPhase::Preparing, vec![10], 2000);
        log.log_transition(2, TxnPhase::Committing, vec![10], 3000);
        let resolved = coord.recover_in_doubt(&log);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0], (2, TxnPhase::Committed));
    }

    #[test]
    fn txn_coordinator_recover_mixed() {
        let mut coord = TransactionCoordinator::new(1);
        let mut log = TxnRecoveryLog::new();
        // Txn 1: already committed (not in-doubt)
        log.log_transition(1, TxnPhase::Committed, vec![10], 1000);
        // Txn 2: stuck in Preparing
        log.log_transition(2, TxnPhase::Preparing, vec![10, 20], 2000);
        // Txn 3: stuck in Committing
        log.log_transition(3, TxnPhase::Committing, vec![30], 3000);
        let resolved = coord.recover_in_doubt(&log);
        assert_eq!(resolved.len(), 2);
        let map: HashMap<TxnId, TxnPhase> = resolved.into_iter().collect();
        assert_eq!(map[&2], TxnPhase::Aborted);
        assert_eq!(map[&3], TxnPhase::Committed);
    }

    #[test]
    fn txn_log_phase_transition() {
        let mut coord = TransactionCoordinator::new(1);
        let mut log = TxnRecoveryLog::new();
        let txn_id = coord.begin();
        coord.add_operation(txn_id, 10, Operation::Noop).unwrap();
        coord.log_phase_transition(&mut log, txn_id);
        assert_eq!(log.len(), 1);
        assert_eq!(log.entries_for_txn(txn_id)[0].phase, TxnPhase::Active);

        coord.prepare(txn_id).unwrap();
        coord.log_phase_transition(&mut log, txn_id);
        assert_eq!(log.len(), 2);
        assert_eq!(log.entries_for_txn(txn_id)[1].phase, TxnPhase::Preparing);
    }

    // -- Raft message serialization tests ----

    #[test]
    fn raft_msg_request_vote_roundtrip() {
        let msg = RaftMessage::RequestVote {
            shard_id: 10, term: 5, candidate_id: 3, last_log_index: 42, last_log_term: 4,
        };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
        assert_eq!(decoded.shard_id(), 10);
        assert_eq!(decoded.term(), 5);
    }

    #[test]
    fn raft_msg_vote_response_roundtrip() {
        let msg = RaftMessage::VoteResponse {
            shard_id: 20, term: 3, voter_id: 7, granted: true,
        };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn raft_msg_append_entries_roundtrip() {
        let msg = RaftMessage::AppendEntries {
            shard_id: 5, term: 10, leader_id: 1,
            prev_log_index: 99, prev_log_term: 9,
            entries: vec![
                LogEntry { index: 100, term: 10, operation: Operation::Noop },
                LogEntry { index: 101, term: 10, operation: Operation::Put {
                    key: b"hello".to_vec(), value: b"world".to_vec(),
                }},
                LogEntry { index: 102, term: 10, operation: Operation::Delete {
                    key: b"old_key".to_vec(),
                }},
            ],
            leader_commit: 98,
        };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn raft_msg_append_entries_empty() {
        let msg = RaftMessage::AppendEntries {
            shard_id: 1, term: 1, leader_id: 1,
            prev_log_index: 0, prev_log_term: 0, entries: vec![], leader_commit: 0,
        };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn raft_msg_append_entries_response_roundtrip() {
        let msg = RaftMessage::AppendEntriesResponse {
            shard_id: 10, term: 5, follower_id: 2, success: false, match_index: 50,
        };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn raft_msg_heartbeat_roundtrip() {
        let msg = RaftMessage::Heartbeat { shard_id: 30, term: 7, leader_id: 1 };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn raft_msg_heartbeat_response_roundtrip() {
        let msg = RaftMessage::HeartbeatResponse { shard_id: 30, term: 7, follower_id: 2 };
        let bytes = msg.to_bytes();
        let decoded = RaftMessage::from_bytes(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn raft_msg_invalid_tag() {
        let bytes = vec![255, 0, 0, 0];
        assert!(RaftMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn raft_msg_empty_data() {
        assert!(RaftMessage::from_bytes(&[]).is_err());
    }

    #[test]
    fn raft_msg_too_short() {
        let bytes = vec![1, 0, 0]; // RequestVote tag with insufficient data
        assert!(RaftMessage::from_bytes(&bytes).is_err());
    }

    // -- HLC tests ----

    #[test]
    fn hlc_basic_monotonicity() {
        let mut clock = HybridLogicalClock::new(1);
        let t1 = clock.now();
        let t2 = clock.now();
        let t3 = clock.now();
        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn hlc_clock_skew_handling() {
        let mut clock = HybridLogicalClock::new(1);
        // Advance the clock to a far-future physical time to simulate
        // the wall clock going backwards relative to latest.
        let future_ms = HybridLogicalClock::physical_now() + 100_000;
        clock.latest = HybridTimestamp::new(future_ms, 5, 1);

        let ts = clock.now();
        // Physical time should stay at the future value (not go backwards).
        assert_eq!(ts.physical_ms, future_ms);
        // Logical should increment past 5.
        assert_eq!(ts.logical, 6);
        assert_eq!(ts.node_id, 1);

        // Another call should keep incrementing.
        let ts2 = clock.now();
        assert!(ts2 > ts);
        assert_eq!(ts2.physical_ms, future_ms);
        assert_eq!(ts2.logical, 7);
    }

    #[test]
    fn hlc_update_from_received_future() {
        let mut clock = HybridLogicalClock::new(1);
        let _t1 = clock.now();

        // Simulate receiving a timestamp far in the future.
        let remote_ts = HybridTimestamp::new(
            HybridLogicalClock::physical_now() + 500_000,
            10,
            2,
        );
        let updated = clock.update(&remote_ts);
        // Updated timestamp must be greater than the received one.
        assert!(updated > remote_ts);
        assert_eq!(updated.physical_ms, remote_ts.physical_ms);
        assert_eq!(updated.logical, 11);
        assert_eq!(updated.node_id, 1);
    }

    #[test]
    fn hlc_update_from_received_past() {
        let mut clock = HybridLogicalClock::new(1);
        // Advance local clock.
        let _t1 = clock.now();
        let _t2 = clock.now();
        let local_latest = clock.latest();

        // Receive a timestamp from the distant past.
        let remote_ts = HybridTimestamp::new(1000, 0, 2);
        let updated = clock.update(&remote_ts);

        // Local time should dominate.
        assert!(updated > local_latest);
        assert!(updated > remote_ts);
    }

    #[test]
    fn hlc_serialization_roundtrip() {
        let ts = HybridTimestamp::new(1_700_000_000_000, 42, 7);
        let bytes = ts.to_bytes();
        assert_eq!(bytes.len(), 14);
        let ts2 = HybridTimestamp::from_bytes(bytes);
        assert_eq!(ts, ts2);
        assert_eq!(ts.physical_ms, ts2.physical_ms);
        assert_eq!(ts.logical, ts2.logical);
        assert_eq!(ts.node_id, ts2.node_id);
    }

    #[test]
    fn hlc_serialization_roundtrip_extremes() {
        let ts_zero = HybridTimestamp::new(0, 0, 0);
        assert_eq!(ts_zero, HybridTimestamp::from_bytes(ts_zero.to_bytes()));

        let ts_max = HybridTimestamp::new(u64::MAX, u32::MAX, u16::MAX);
        assert_eq!(ts_max, HybridTimestamp::from_bytes(ts_max.to_bytes()));
    }

    #[test]
    fn hlc_ordering() {
        let a = HybridTimestamp::new(100, 0, 1);
        let b = HybridTimestamp::new(100, 1, 1);
        let c = HybridTimestamp::new(100, 1, 2);
        let d = HybridTimestamp::new(200, 0, 0);

        assert!(a < b);
        assert!(b < c);
        assert!(c < d);

        // Verify Eq
        let a2 = HybridTimestamp::new(100, 0, 1);
        assert_eq!(a, a2);
    }

    #[test]
    fn hlc_concurrent_generation_different_nodes() {
        let mut clock_a = HybridLogicalClock::new(1);
        let mut clock_b = HybridLogicalClock::new(2);

        let ts_a = clock_a.now();
        let ts_b = clock_b.now();

        // Both should be valid timestamps with their respective node IDs.
        assert_eq!(ts_a.node_id, 1);
        assert_eq!(ts_b.node_id, 2);

        // Even if physical times are the same, node_id breaks the tie.
        assert_ne!(ts_a, ts_b);

        // After exchanging, both clocks should advance past both timestamps.
        let ts_a2 = clock_a.update(&ts_b);
        let ts_b2 = clock_b.update(&ts_a);
        assert!(ts_a2 > ts_a);
        assert!(ts_a2 > ts_b);
        assert!(ts_b2 > ts_a);
        assert!(ts_b2 > ts_b);
    }

    #[test]
    fn hlc_display_format() {
        let ts = HybridTimestamp::new(1234567890, 42, 7);
        let s = ts.to_string();
        assert_eq!(s, "1234567890:42:7");
    }

    #[test]
    fn hlc_update_all_three_equal_physical() {
        let mut clock = HybridLogicalClock::new(1);
        // Set latest to a specific physical time.
        let pt = HybridLogicalClock::physical_now() + 200_000;
        clock.latest = HybridTimestamp::new(pt, 10, 1);

        // Receive a timestamp with the same physical time but higher logical.
        let remote = HybridTimestamp::new(pt, 20, 2);
        let updated = clock.update(&remote);

        // Should take max(10, 20) + 1 = 21.
        assert_eq!(updated.physical_ms, pt);
        assert_eq!(updated.logical, 21);
        assert_eq!(updated.node_id, 1);
    }

    // ================================================================
    // Follower Reads tests
    // ================================================================

    #[test]
    fn follower_read_unknown_when_empty() {
        let mgr = FollowerReadManager::new(1);
        let req = FollowerReadRequest {
            min_timestamp: HybridTimestamp::new(100, 0, 0),
            key: "test".into(),
        };
        assert_eq!(mgr.can_serve(&req), FollowerReadResult::Unknown);
    }

    #[test]
    fn follower_read_serve_locally() {
        let mut mgr = FollowerReadManager::new(1);
        mgr.advance_timestamp(HybridTimestamp::new(200, 5, 2));

        let req = FollowerReadRequest {
            min_timestamp: HybridTimestamp::new(100, 0, 0),
            key: "test".into(),
        };
        assert_eq!(mgr.can_serve(&req), FollowerReadResult::ServeLocally);
    }

    #[test]
    fn follower_read_redirect_to_leader() {
        let mut mgr = FollowerReadManager::new(1);
        mgr.advance_timestamp(HybridTimestamp::new(50, 0, 2));

        let req = FollowerReadRequest {
            min_timestamp: HybridTimestamp::new(100, 0, 0),
            key: "test".into(),
        };
        assert_eq!(mgr.can_serve(&req), FollowerReadResult::RedirectToLeader);
    }

    #[test]
    fn follower_read_bounded_staleness() {
        let mut mgr = FollowerReadManager::new(1);
        mgr.max_staleness_ms = 1000;
        mgr.advance_timestamp(HybridTimestamp::new(9500, 0, 2));

        // Current time 10000, local at 9500 → staleness 500ms → ok
        assert_eq!(mgr.can_serve_bounded(10000), FollowerReadResult::ServeLocally);
        // Current time 11000, local at 9500 → staleness 1500ms → too stale
        assert_eq!(mgr.can_serve_bounded(11000), FollowerReadResult::RedirectToLeader);
    }

    #[test]
    fn follower_read_advance_timestamp() {
        let mut mgr = FollowerReadManager::new(1);
        mgr.advance_timestamp(HybridTimestamp::new(100, 0, 0));
        assert_eq!(mgr.local_timestamp().physical_ms, 100);

        // Should not go backwards
        mgr.advance_timestamp(HybridTimestamp::new(50, 0, 0));
        assert_eq!(mgr.local_timestamp().physical_ms, 100);

        mgr.advance_timestamp(HybridTimestamp::new(200, 0, 0));
        assert_eq!(mgr.local_timestamp().physical_ms, 200);
    }

    // ================================================================
    // Parallel Commits tests
    // ================================================================

    #[test]
    fn parallel_commit_basic() {
        let mut coord = ParallelCommitCoordinator::new();
        let txn = coord.begin_txn();

        coord.stage_intent(txn, "key1", b"val1".to_vec(), HybridTimestamp::new(100, 0, 1));
        coord.stage_intent(txn, "key2", b"val2".to_vec(), HybridTimestamp::new(100, 1, 1));

        assert_eq!(coord.pending_intent_count(), 2);
        assert_eq!(coord.total_intent_count(), 2);

        let committed = coord.commit_txn(txn);
        assert_eq!(committed, 2);
        assert_eq!(coord.pending_intent_count(), 0);
    }

    #[test]
    fn parallel_commit_abort() {
        let mut coord = ParallelCommitCoordinator::new();
        let txn = coord.begin_txn();
        coord.stage_intent(txn, "key1", b"val1".to_vec(), HybridTimestamp::new(100, 0, 1));

        let aborted = coord.abort_txn(txn);
        assert_eq!(aborted, 1);
    }

    #[test]
    fn parallel_commit_cleanup() {
        let mut coord = ParallelCommitCoordinator::new();
        let txn = coord.begin_txn();
        coord.stage_intent(txn, "key1", b"val1".to_vec(), HybridTimestamp::new(100, 0, 1));
        coord.commit_txn(txn);

        let cleaned = coord.cleanup_intents(txn);
        assert_eq!(cleaned, 1);
        assert_eq!(coord.total_intent_count(), 0);
    }

    #[test]
    fn parallel_commit_read_intent() {
        let mut coord = ParallelCommitCoordinator::new();
        let txn = coord.begin_txn();
        coord.stage_intent(txn, "key1", b"hello".to_vec(), HybridTimestamp::new(100, 0, 1));

        // Read encounters a pending intent
        let (val, status) = coord.read_with_intent_resolution("key1").unwrap();
        assert_eq!(val, b"hello");
        assert_eq!(status, IntentStatus::Pending);

        coord.commit_txn(txn);
        let (_, status) = coord.read_with_intent_resolution("key1").unwrap();
        assert_eq!(status, IntentStatus::Committed);
    }

    #[test]
    fn parallel_commit_multiple_txns() {
        let mut coord = ParallelCommitCoordinator::new();
        let txn1 = coord.begin_txn();
        let txn2 = coord.begin_txn();

        coord.stage_intent(txn1, "a", b"1".to_vec(), HybridTimestamp::new(100, 0, 1));
        coord.stage_intent(txn2, "b", b"2".to_vec(), HybridTimestamp::new(100, 0, 1));

        assert_eq!(coord.pending_intent_count(), 2);

        coord.commit_txn(txn1);
        assert_eq!(coord.pending_intent_count(), 1);

        coord.abort_txn(txn2);
        assert_eq!(coord.pending_intent_count(), 0);
    }

    // ================================================================
    // Multi-Region Awareness tests
    // ================================================================

    #[test]
    fn multi_region_route_read_local() {
        let mut mgr = MultiRegionManager::new(RegionId("us-east".into()));
        mgr.register_node(1, RegionId("us-east".into()), true);
        mgr.register_node(2, RegionId("eu-west".into()), false);
        mgr.set_zone_config(ZoneConfig {
            table: "users".into(),
            primary_region: RegionId("us-east".into()),
            replica_regions: vec![RegionId("eu-west".into())],
            strong_consistency: false,
        });

        // Should route to local node
        assert_eq!(mgr.route_read("users"), Some(1));
    }

    #[test]
    fn multi_region_route_write() {
        let mut mgr = MultiRegionManager::new(RegionId("eu-west".into()));
        mgr.register_node(1, RegionId("us-east".into()), true);
        mgr.register_node(2, RegionId("eu-west".into()), false);
        mgr.set_zone_config(ZoneConfig {
            table: "users".into(),
            primary_region: RegionId("us-east".into()),
            replica_regions: vec![RegionId("eu-west".into())],
            strong_consistency: false,
        });

        // Writes always go to leaseholder in primary region
        assert_eq!(mgr.route_write("users"), Some(1));
    }

    #[test]
    fn multi_region_strong_consistency() {
        let mut mgr = MultiRegionManager::new(RegionId("eu-west".into()));
        mgr.register_node(1, RegionId("us-east".into()), true);
        mgr.register_node(2, RegionId("eu-west".into()), false);
        mgr.set_zone_config(ZoneConfig {
            table: "orders".into(),
            primary_region: RegionId("us-east".into()),
            replica_regions: vec![RegionId("eu-west".into())],
            strong_consistency: true,
        });

        // Strong consistency: reads must go to primary leaseholder
        assert_eq!(mgr.route_read("orders"), Some(1));
    }

    #[test]
    fn multi_region_is_local() {
        let mut mgr = MultiRegionManager::new(RegionId("us-east".into()));
        mgr.register_node(1, RegionId("us-east".into()), true);
        mgr.set_zone_config(ZoneConfig {
            table: "local_table".into(),
            primary_region: RegionId("us-east".into()),
            replica_regions: vec![],
            strong_consistency: false,
        });

        assert!(mgr.is_local_read("local_table"));
        assert!(!mgr.is_local_read("nonexistent"));
    }

    #[test]
    fn multi_region_zone_config() {
        let mut mgr = MultiRegionManager::new(RegionId("us-east".into()));
        mgr.set_zone_config(ZoneConfig {
            table: "t1".into(),
            primary_region: RegionId("us-east".into()),
            replica_regions: vec![],
            strong_consistency: false,
        });
        mgr.set_zone_config(ZoneConfig {
            table: "t2".into(),
            primary_region: RegionId("eu-west".into()),
            replica_regions: vec![],
            strong_consistency: true,
        });

        assert_eq!(mgr.zone_config_count(), 2);
        assert_eq!(mgr.node_count(), 0);
    }
}