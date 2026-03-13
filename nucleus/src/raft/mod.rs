//! Raft consensus engine for distributed mode.
//!
//! Supports:
//!   - Leader election with randomized timeouts
//!   - Log replication with append entries
//!   - Committed entry application
//!   - Membership changes
//!   - Automatic failover
//!
//! Replaces CockroachDB's consensus layer for Nucleus cluster mode.

use std::collections::HashMap;

// ============================================================================
// Raft types
// ============================================================================

/// Unique node identifier.
pub type NodeId = u64;
/// Log index (1-based).
pub type LogIndex = u64;
/// Election term.
pub type Term = u64;

/// Role of a node in the Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A log entry in the replicated log.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub command: Command,
}

/// Commands that can be replicated.
#[derive(Debug, Clone)]
pub enum Command {
    /// A SQL statement to execute.
    Sql(String),
    /// A no-op entry (used after leader election).
    Noop,
    /// Add a node to the cluster.
    AddNode(NodeId),
    /// Remove a node from the cluster.
    RemoveNode(NodeId),
}

// ============================================================================
// RPC messages
// ============================================================================

/// RequestVote RPC request.
#[derive(Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// AppendEntries RPC request.
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

/// AppendEntries RPC response.
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    /// Hint for next index (optimization for fast backtracking).
    pub match_index: LogIndex,
}

/// InstallSnapshot RPC request (sent by leader to slow followers).
#[derive(Debug, Clone)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    /// Serialized state machine snapshot data.
    pub data: Vec<u8>,
}

/// InstallSnapshot RPC response.
#[derive(Debug, Clone)]
pub struct InstallSnapshotResponse {
    pub term: Term,
}

/// A snapshot of the state machine at a given point in the log.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The last log index included in this snapshot.
    pub last_included_index: LogIndex,
    /// The term of the last log entry included.
    pub last_included_term: Term,
    /// Serialized state machine data.
    pub data: Vec<u8>,
}

// ============================================================================
// Raft node state
// ============================================================================

/// Core Raft state machine for a single node.
pub struct RaftNode {
    /// This node's ID.
    pub id: NodeId,
    /// Current role.
    pub role: Role,
    /// Current term.
    pub current_term: Term,
    /// Who we voted for in current term.
    pub voted_for: Option<NodeId>,
    /// Replicated log (1-indexed; index 0 is a sentinel).
    pub log: Vec<LogEntry>,
    /// Index of highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of highest log entry applied to state machine.
    pub last_applied: LogIndex,

    // Leader state
    /// For each follower: next log index to send.
    pub next_index: HashMap<NodeId, LogIndex>,
    /// For each follower: highest log index known to be replicated.
    pub match_index: HashMap<NodeId, LogIndex>,

    /// Set of known peers.
    pub peers: Vec<NodeId>,
    /// Votes received in current election.
    votes_received: Vec<NodeId>,
    /// Current leader (known by followers).
    pub leader_id: Option<NodeId>,
    /// Applied commands (for state machine output).
    pub applied_commands: Vec<Command>,

    // Snapshot state
    /// The most recent snapshot, if any.
    pub snapshot: Option<Snapshot>,

    // Leadership lease
    /// Number of successful heartbeat responses received in the current round.
    /// The leader tracks this to detect network partitions.
    pub lease_acks: usize,
    /// Whether the leader's lease is currently valid (majority responded recently).
    pub lease_valid: bool,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            // Sentinel entry at index 0
            log: vec![LogEntry {
                index: 0,
                term: 0,
                command: Command::Noop,
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers,
            votes_received: Vec::new(),
            leader_id: None,
            applied_commands: Vec::new(),
            snapshot: None,
            lease_acks: 0,
            lease_valid: false,
        }
    }

    /// Get the last log index.
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    /// Get the last log term.
    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Get the log entry at a specific index.
    pub fn log_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.log.get(index as usize)
    }

    /// Start an election: become candidate, vote for self, increment term.
    pub fn start_election(&mut self) -> Vec<(NodeId, RequestVoteRequest)> {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received = vec![self.id];
        self.leader_id = None;

        let request = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        self.peers
            .iter()
            .map(|&peer| (peer, request.clone()))
            .collect()
    }

    /// Handle a RequestVote RPC.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> RequestVoteResponse {
        // If the request has a higher term, update and become follower
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;
        }

        let vote_granted = if req.term < self.current_term
            || (self.voted_for.is_some() && self.voted_for != Some(req.candidate_id))
        {
            false
        } else {
            // Check log is at least as up-to-date
            let log_ok = req.last_log_term > self.last_log_term()
                || (req.last_log_term == self.last_log_term()
                    && req.last_log_index >= self.last_log_index());
            if log_ok {
                self.voted_for = Some(req.candidate_id);
                true
            } else {
                false
            }
        };

        RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    /// Handle a RequestVote response (as candidate).
    pub fn handle_vote_response(&mut self, from: NodeId, resp: &RequestVoteResponse) -> bool {
        if resp.term > self.current_term {
            self.current_term = resp.term;
            self.role = Role::Follower;
            self.voted_for = None;
            return false;
        }

        if self.role != Role::Candidate || resp.term != self.current_term {
            return false;
        }

        if resp.vote_granted {
            self.votes_received.push(from);
        }

        // Check if we have majority
        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;

        if self.votes_received.len() >= majority {
            self.become_leader();
            true
        } else {
            false
        }
    }

    /// Become leader: initialize leader state.
    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);

        let next = self.last_log_index() + 1;
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }

        // Append a noop entry to establish leadership
        self.append_entry(Command::Noop);
    }

    /// Append an entry to the log (leader only). Returns the log index.
    pub fn append_entry(&mut self, command: Command) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }

        let index = self.last_log_index() + 1;
        self.log.push(LogEntry {
            index,
            term: self.current_term,
            command,
        });

        Some(index)
    }

    /// Build AppendEntries RPCs to send to followers (leader only).
    pub fn build_append_entries(&self) -> Vec<(NodeId, AppendEntriesRequest)> {
        if self.role != Role::Leader {
            return Vec::new();
        }

        self.peers
            .iter()
            .map(|&peer| {
                let next = self.next_index.get(&peer).copied().unwrap_or(1);
                let prev_index = next - 1;
                let prev_term = self.log_at(prev_index).map(|e| e.term).unwrap_or(0);

                let entries: Vec<LogEntry> = self
                    .log
                    .iter()
                    .filter(|e| e.index >= next)
                    .cloned()
                    .collect();

                (
                    peer,
                    AppendEntriesRequest {
                        term: self.current_term,
                        leader_id: self.id,
                        prev_log_index: prev_index,
                        prev_log_term: prev_term,
                        entries,
                        leader_commit: self.commit_index,
                    },
                )
            })
            .collect()
    }

    /// Handle AppendEntries RPC (as follower).
    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        // Stale term
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }

        // Update term if needed
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }

        self.role = Role::Follower;
        self.leader_id = Some(req.leader_id);

        // Check if we have the prev_log entry
        if req.prev_log_index > 0 {
            match self.log_at(req.prev_log_index) {
                None => {
                    return AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: self.last_log_index(),
                    };
                }
                Some(entry) => {
                    if entry.term != req.prev_log_term {
                        // Conflict: truncate log from this point
                        self.log.truncate(req.prev_log_index as usize);
                        return AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            match_index: self.last_log_index(),
                        };
                    }
                }
            }
        }

        // Append new entries (handle conflicts)
        for entry in &req.entries {
            if (entry.index as usize) < self.log.len() {
                // Existing entry — check for conflict
                if let Some(existing) = self.log_at(entry.index)
                    && existing.term != entry.term {
                        self.log.truncate(entry.index as usize);
                        self.log.push(entry.clone());
                    }
            } else {
                self.log.push(entry.clone());
            }
        }

        // Update commit index
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
        }
    }

    /// Handle AppendEntries response (as leader).
    pub fn handle_append_response(&mut self, from: NodeId, resp: &AppendEntriesResponse) {
        if resp.term > self.current_term {
            self.current_term = resp.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;
            return;
        }

        if self.role != Role::Leader {
            return;
        }

        if resp.success {
            self.match_index.insert(from, resp.match_index);
            self.next_index.insert(from, resp.match_index + 1);
        } else {
            // Decrement next_index and retry
            let current = self.next_index.get(&from).copied().unwrap_or(1);
            let new_next = if resp.match_index > 0 {
                resp.match_index + 1
            } else {
                current.saturating_sub(1).max(1)
            };
            self.next_index.insert(from, new_next);
        }

        // Try to advance commit index
        self.try_advance_commit();
    }

    /// Try to advance commit index based on majority replication.
    fn try_advance_commit(&mut self) {
        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;

        for n in (self.commit_index + 1)..=self.last_log_index() {
            // Only commit entries from current term
            if let Some(entry) = self.log_at(n)
                && entry.term != self.current_term {
                    continue;
                }

            // Count replications (leader counts itself)
            let mut count = 1; // self
            for &peer in &self.peers {
                if self.match_index.get(&peer).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }

            if count >= majority {
                self.commit_index = n;
            }
        }
    }

    /// Apply committed entries to the state machine. Returns indices of applied entries.
    pub fn apply_committed(&mut self) -> Vec<LogIndex> {
        let mut applied = Vec::new();

        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied as usize) {
                self.applied_commands.push(entry.command.clone());
            }
            applied.push(self.last_applied);
        }

        applied
    }

    // ========================================================================
    // Snapshot support (Phase 8A)
    // ========================================================================

    /// Take a snapshot at the current `last_applied` index, compacting the log.
    ///
    /// `state_data` is the serialized state machine state provided by the caller.
    /// After snapshotting, all log entries up to `last_applied` are discarded
    /// (replaced by a single sentinel entry preserving the snapshot's index/term).
    ///
    /// Returns the snapshot or None if there's nothing to compact.
    pub fn take_snapshot(&mut self, state_data: Vec<u8>) -> Option<&Snapshot> {
        if self.last_applied == 0 {
            return None;
        }

        let snap_index = self.last_applied;
        let snap_term = self.log_at(snap_index).map(|e| e.term).unwrap_or(0);

        self.snapshot = Some(Snapshot {
            last_included_index: snap_index,
            last_included_term: snap_term,
            data: state_data,
        });

        // Compact the log: keep only entries after the snapshot index.
        // Replace the prefix with a new sentinel at the snapshot point.
        let keep_from = snap_index as usize;
        if keep_from < self.log.len() {
            self.log = std::iter::once(LogEntry {
                index: snap_index,
                term: snap_term,
                command: Command::Noop,
            })
            .chain(self.log.drain((keep_from + 1)..))
            .collect();
        }

        self.snapshot.as_ref()
    }

    /// Build an InstallSnapshot RPC for a follower that is too far behind
    /// to receive log entries (their `next_index` is before our snapshot).
    pub fn build_install_snapshot(&self) -> Option<InstallSnapshotRequest> {
        let snap = self.snapshot.as_ref()?;
        Some(InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id,
            last_included_index: snap.last_included_index,
            last_included_term: snap.last_included_term,
            data: snap.data.clone(),
        })
    }

    /// Handle an InstallSnapshot RPC (as follower).
    pub fn handle_install_snapshot(&mut self, req: &InstallSnapshotRequest) -> InstallSnapshotResponse {
        if req.term < self.current_term {
            return InstallSnapshotResponse { term: self.current_term };
        }

        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader_id = Some(req.leader_id);

        // Install the snapshot: replace log and state
        self.snapshot = Some(Snapshot {
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data: req.data.clone(),
        });

        // Reset log to a single sentinel at the snapshot point
        self.log = vec![LogEntry {
            index: req.last_included_index,
            term: req.last_included_term,
            command: Command::Noop,
        }];

        // Advance applied/committed indices
        if req.last_included_index > self.commit_index {
            self.commit_index = req.last_included_index;
        }
        if req.last_included_index > self.last_applied {
            self.last_applied = req.last_included_index;
        }

        InstallSnapshotResponse { term: self.current_term }
    }

    /// Check if a follower needs a snapshot (their next_index is before our snapshot).
    pub fn needs_snapshot(&self, peer: NodeId) -> bool {
        if let Some(ref snap) = self.snapshot {
            let next = self.next_index.get(&peer).copied().unwrap_or(1);
            next <= snap.last_included_index
        } else {
            false
        }
    }

    // ========================================================================
    // Leadership lease (Phase 8B)
    // ========================================================================

    /// Start a new heartbeat round: reset the ack counter.
    /// Call this before sending heartbeats (AppendEntries) to all followers.
    pub fn start_heartbeat_round(&mut self) {
        if self.role == Role::Leader {
            self.lease_acks = 1; // Count self
        }
    }

    /// Record a successful heartbeat response from a follower.
    /// After processing all responses, call `check_lease()`.
    pub fn record_heartbeat_ack(&mut self) {
        if self.role == Role::Leader {
            self.lease_acks += 1;
        }
    }

    /// Check if the leader has received enough heartbeat acks to maintain its lease.
    /// If not, the leader steps down to prevent serving stale reads during a partition.
    /// Returns true if the lease is valid, false if the leader stepped down.
    pub fn check_lease(&mut self) -> bool {
        if self.role != Role::Leader {
            self.lease_valid = false;
            return false;
        }

        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;

        if self.lease_acks >= majority {
            self.lease_valid = true;
            true
        } else {
            // Can't reach a majority — step down to prevent split-brain
            self.lease_valid = false;
            self.role = Role::Follower;
            self.leader_id = None;
            false
        }
    }

    /// Whether the leader has a valid lease (can serve reads).
    pub fn has_valid_lease(&self) -> bool {
        self.role == Role::Leader && self.lease_valid
    }

    /// Force step-down from leader (e.g., on election timeout without majority).
    pub fn step_down(&mut self) {
        if self.role == Role::Leader {
            self.role = Role::Follower;
            self.leader_id = None;
            self.lease_valid = false;
        }
    }

    /// Get cluster status summary.
    pub fn status(&self) -> ClusterStatus {
        ClusterStatus {
            node_id: self.id,
            role: self.role,
            term: self.current_term,
            leader_id: self.leader_id,
            log_length: self.log.len() - 1, // Exclude sentinel
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            peers: self.peers.clone(),
        }
    }
}

/// Summary of cluster status from a node's perspective.
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader_id: Option<NodeId>,
    pub log_length: usize,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub peers: Vec<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state() {
        let node = RaftNode::new(1, vec![2, 3]);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.last_log_index(), 0);
        assert_eq!(node.peers.len(), 2);
    }

    #[test]
    fn election_and_leader() {
        let mut node1 = RaftNode::new(1, vec![2, 3]);
        let mut node2 = RaftNode::new(2, vec![1, 3]);
        let mut node3 = RaftNode::new(3, vec![1, 2]);

        // Node 1 starts election
        let vote_requests = node1.start_election();
        assert_eq!(node1.role, Role::Candidate);
        assert_eq!(node1.current_term, 1);
        assert_eq!(vote_requests.len(), 2);

        // Node 2 and 3 vote
        let resp2 = node2.handle_request_vote(&vote_requests[0].1);
        let resp3 = node3.handle_request_vote(&vote_requests[1].1);
        assert!(resp2.vote_granted);
        assert!(resp3.vote_granted);

        // Node 1 processes votes
        let became_leader = node1.handle_vote_response(2, &resp2);
        assert!(became_leader); // Majority with 2 votes (self + node2)
        assert_eq!(node1.role, Role::Leader);
    }

    #[test]
    fn log_replication() {
        // Setup: node1 is leader
        let mut node1 = RaftNode::new(1, vec![2, 3]);
        let mut node2 = RaftNode::new(2, vec![1, 3]);
        let mut node3 = RaftNode::new(3, vec![1, 2]);

        // Win election
        let votes = node1.start_election();
        let r2 = node2.handle_request_vote(&votes[0].1);
        node1.handle_vote_response(2, &r2);
        let r3 = node3.handle_request_vote(&votes[1].1);
        node1.handle_vote_response(3, &r3);
        assert_eq!(node1.role, Role::Leader);

        // Leader appends a command
        let idx = node1.append_entry(Command::Sql("INSERT INTO t VALUES (1)".into()));
        assert!(idx.is_some());

        // Send AppendEntries to followers
        let appends = node1.build_append_entries();
        assert_eq!(appends.len(), 2);

        // Followers process
        let resp2 = node2.handle_append_entries(&appends[0].1);
        let resp3 = node3.handle_append_entries(&appends[1].1);
        assert!(resp2.success);
        assert!(resp3.success);

        // Leader processes responses → commit advances
        node1.handle_append_response(2, &resp2);
        node1.handle_append_response(3, &resp3);

        // Commit index should advance (noop + SQL command committed)
        assert!(node1.commit_index >= 1);
    }

    #[test]
    fn reject_stale_term() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.current_term = 5;

        let req = RequestVoteRequest {
            term: 3, // Stale term
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let resp = node.handle_request_vote(&req);
        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn follower_updates_commit() {
        let mut follower = RaftNode::new(2, vec![1, 3]);

        // Simulate receiving entries from leader
        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry { index: 1, term: 1, command: Command::Noop },
                LogEntry { index: 2, term: 1, command: Command::Sql("CREATE TABLE t (id INT)".into()) },
            ],
            leader_commit: 2,
        };

        let resp = follower.handle_append_entries(&req);
        assert!(resp.success);
        assert_eq!(follower.commit_index, 2);
        assert_eq!(follower.last_log_index(), 2);
        assert_eq!(follower.leader_id, Some(1));
    }

    #[test]
    fn only_leader_appends() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        // Follower can't append
        assert!(node.append_entry(Command::Sql("test".into())).is_none());
    }

    #[test]
    fn cluster_status() {
        let node = RaftNode::new(1, vec![2, 3]);
        let status = node.status();
        assert_eq!(status.node_id, 1);
        assert_eq!(status.role, Role::Follower);
        assert_eq!(status.log_length, 0);
    }

    // ================================================================
    // New comprehensive tests
    // ================================================================

    #[test]
    fn leader_election_multiple_candidates() {
        let mut node1 = RaftNode::new(1, vec![2, 3, 4, 5]);
        let mut node2 = RaftNode::new(2, vec![1, 3, 4, 5]);
        let mut node3 = RaftNode::new(3, vec![1, 2, 4, 5]);
        let mut _node4 = RaftNode::new(4, vec![1, 2, 3, 5]);
        let mut node5 = RaftNode::new(5, vec![1, 2, 3, 4]);
        let votes1 = node1.start_election();
        let votes2 = node2.start_election();
        assert_eq!(node1.role, Role::Candidate);
        assert_eq!(node2.role, Role::Candidate);
        let r3_for_1 = node3.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(r3_for_1.vote_granted);
        let r3_for_2 = node3.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(!r3_for_2.vote_granted);
        let r5_for_1 = node5.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 5).unwrap().1);
        assert!(r5_for_1.vote_granted);
        let became_leader_1a = node1.handle_vote_response(3, &r3_for_1);
        assert!(!became_leader_1a);
        let became_leader_1b = node1.handle_vote_response(5, &r5_for_1);
        assert!(became_leader_1b);
        assert_eq!(node1.role, Role::Leader);
    }

    #[test]
    fn log_replication_multiple_entries() {
        let mut leader = RaftNode::new(1, vec![2, 3]);
        let mut f2 = RaftNode::new(2, vec![1, 3]);
        let mut f3 = RaftNode::new(3, vec![1, 2]);
        let votes = leader.start_election();
        let r2 = f2.handle_request_vote(&votes[0].1);
        leader.handle_vote_response(2, &r2);
        assert_eq!(leader.role, Role::Leader);
        let idx1 = leader.append_entry(Command::Sql("INSERT INTO t VALUES (1)".into()));
        let idx2 = leader.append_entry(Command::Sql("INSERT INTO t VALUES (2)".into()));
        let idx3 = leader.append_entry(Command::Sql("INSERT INTO t VALUES (3)".into()));
        assert!(idx1.is_some());
        assert!(idx2.is_some());
        assert!(idx3.is_some());
        let appends = leader.build_append_entries();
        let resp2 = f2.handle_append_entries(&appends.iter().find(|(id, _)| *id == 2).unwrap().1);
        let resp3 = f3.handle_append_entries(&appends.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(resp2.success);
        assert!(resp3.success);
        assert_eq!(f2.last_log_index(), 4);
        assert_eq!(f3.last_log_index(), 4);
        leader.handle_append_response(2, &resp2);
        leader.handle_append_response(3, &resp3);
        assert_eq!(leader.commit_index, 4);
    }

    #[test]
    fn follower_timeout_and_re_election() {
        let mut node1 = RaftNode::new(1, vec![2, 3]);
        let mut node2 = RaftNode::new(2, vec![1, 3]);
        let mut node3 = RaftNode::new(3, vec![1, 2]);
        let votes = node1.start_election();
        let r2 = node2.handle_request_vote(&votes[0].1);
        node1.handle_vote_response(2, &r2);
        assert_eq!(node1.role, Role::Leader);
        assert_eq!(node1.current_term, 1);
        let votes2 = node2.start_election();
        assert_eq!(node2.current_term, 2);
        assert_eq!(node2.role, Role::Candidate);
        let r3 = node3.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(r3.vote_granted);
        let became_leader = node2.handle_vote_response(3, &r3);
        assert!(became_leader);
        assert_eq!(node2.role, Role::Leader);
        assert_eq!(node2.current_term, 2);
        let appends = node2.build_append_entries();
        let resp = node1.handle_append_entries(&appends.iter().find(|(id, _)| *id == 1).unwrap().1);
        assert!(resp.success);
        assert_eq!(node1.role, Role::Follower);
        assert_eq!(node1.current_term, 2);
        assert_eq!(node1.leader_id, Some(2));
    }

    #[test]
    fn commit_index_advancement_requires_majority() {
        let mut leader = RaftNode::new(1, vec![2, 3, 4, 5]);
        let mut f2 = RaftNode::new(2, vec![1, 3, 4, 5]);
        let mut f3 = RaftNode::new(3, vec![1, 2, 4, 5]);
        let votes = leader.start_election();
        let r2 = f2.handle_request_vote(&votes.iter().find(|(id, _)| *id == 2).unwrap().1);
        let r3 = f3.handle_request_vote(&votes.iter().find(|(id, _)| *id == 3).unwrap().1);
        leader.handle_vote_response(2, &r2);
        leader.handle_vote_response(3, &r3);
        assert_eq!(leader.role, Role::Leader);
        leader.append_entry(Command::Sql("SELECT 1".into()));
        let appends = leader.build_append_entries();
        let resp2 = f2.handle_append_entries(&appends.iter().find(|(id, _)| *id == 2).unwrap().1);
        leader.handle_append_response(2, &resp2);
        assert!(leader.commit_index < leader.last_log_index());
        let resp3 = f3.handle_append_entries(&appends.iter().find(|(id, _)| *id == 3).unwrap().1);
        leader.handle_append_response(3, &resp3);
        assert_eq!(leader.commit_index, leader.last_log_index());
    }

    #[test]
    fn split_vote_no_leader() {
        let mut node1 = RaftNode::new(1, vec![2, 3, 4]);
        let mut node2 = RaftNode::new(2, vec![1, 3, 4]);
        let mut node3 = RaftNode::new(3, vec![1, 2, 4]);
        let mut node4 = RaftNode::new(4, vec![1, 2, 3]);
        let votes1 = node1.start_election();
        let votes2 = node2.start_election();
        let r3_for_1 = node3.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(r3_for_1.vote_granted);
        let r3_for_2 = node3.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(!r3_for_2.vote_granted);
        let r4_for_2 = node4.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 4).unwrap().1);
        assert!(r4_for_2.vote_granted);
        let r4_for_1 = node4.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 4).unwrap().1);
        assert!(!r4_for_1.vote_granted);
        let became_leader_1 = node1.handle_vote_response(3, &r3_for_1);
        assert!(!became_leader_1);
        assert_eq!(node1.role, Role::Candidate);
        let became_leader_2 = node2.handle_vote_response(4, &r4_for_2);
        assert!(!became_leader_2);
        assert_eq!(node2.role, Role::Candidate);
    }

    #[test]
    fn append_entries_consistency_check() {
        let mut follower = RaftNode::new(2, vec![1, 3]);
        let req1 = AppendEntriesRequest {
            term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
            entries: vec![LogEntry { index: 1, term: 1, command: Command::Noop }],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(&req1);
        assert!(resp.success);
        assert_eq!(follower.last_log_index(), 1);
        let req_gap = AppendEntriesRequest {
            term: 1, leader_id: 1, prev_log_index: 2, prev_log_term: 1,
            entries: vec![LogEntry { index: 3, term: 1, command: Command::Sql("SELECT 1".into()) }],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(&req_gap);
        assert!(!resp.success);
        let req_bad_term = AppendEntriesRequest {
            term: 2, leader_id: 1, prev_log_index: 1, prev_log_term: 2,
            entries: vec![LogEntry { index: 2, term: 2, command: Command::Sql("SELECT 2".into()) }],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(&req_bad_term);
        assert!(!resp.success);
    }

    #[test]
    fn term_advancement_on_higher_term_vote_request() {
        let mut leader = RaftNode::new(1, vec![2, 3]);
        leader.current_term = 3;
        leader.role = Role::Leader;
        leader.leader_id = Some(1);
        let req = RequestVoteRequest {
            term: 5, candidate_id: 2, last_log_index: 0, last_log_term: 0,
        };
        let resp = leader.handle_request_vote(&req);
        assert!(resp.vote_granted);
        assert_eq!(leader.current_term, 5);
        assert_eq!(leader.role, Role::Follower);
        assert_eq!(leader.voted_for, Some(2));
        assert_eq!(leader.leader_id, None);
    }

    #[test]
    fn term_advancement_on_higher_term_append_response() {
        let mut leader = RaftNode::new(1, vec![2, 3]);
        leader.current_term = 2;
        leader.role = Role::Leader;
        let resp = AppendEntriesResponse { term: 5, success: false, match_index: 0 };
        leader.handle_append_response(2, &resp);
        assert_eq!(leader.role, Role::Follower);
        assert_eq!(leader.current_term, 5);
    }

    #[test]
    fn apply_committed_entries() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.log.push(LogEntry { index: 1, term: 1, command: Command::Noop });
        node.log.push(LogEntry { index: 2, term: 1, command: Command::Sql("INSERT 1".into()) });
        node.log.push(LogEntry { index: 3, term: 1, command: Command::Sql("INSERT 2".into()) });
        node.commit_index = 2;
        let applied = node.apply_committed();
        assert_eq!(applied, vec![1, 2]);
        assert_eq!(node.last_applied, 2);
        let applied2 = node.apply_committed();
        assert!(applied2.is_empty());
        node.commit_index = 3;
        let applied3 = node.apply_committed();
        assert_eq!(applied3, vec![3]);
        assert_eq!(node.last_applied, 3);
    }

}
