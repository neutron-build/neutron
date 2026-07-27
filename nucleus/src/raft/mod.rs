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
//!
//! # Durability
//!
//! Raft's safety proof assumes `current_term`, `voted_for` and the replicated
//! log survive a crash. A node built with [`RaftNode::new`] keeps them in memory
//! only and is therefore **not** restart-safe — it is for tests and simulation.
//! A node built with [`RaftNode::open`] fsyncs those three (plus the commit
//! index and snapshot metadata) through [`storage::RaftStorage`] *before*
//! returning any RPC response that depends on them.
//!
//! When a durable write fails, the node does not pretend: it rolls the in-memory
//! change back, refuses the operation (`vote_granted: false` / `success: false`)
//! and latches [`RaftNode::durability_failed`]. Refusing is always safe in Raft;
//! acknowledging something that is not on disk is not.

use std::collections::HashMap;
use std::path::Path;

pub mod determinism;
pub mod storage;

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

    // Durability
    /// Durable backing store. `None` means volatile mode (tests/simulation).
    storage: Option<storage::RaftStorage>,
    /// Latched once a durable write fails. A node that cannot persist its hard
    /// state must stop participating rather than acknowledge phantom state.
    durability_failed: bool,
}

impl RaftNode {
    /// Create a **volatile** node: nothing is persisted, so it is not
    /// restart-safe. Use [`RaftNode::open`] for any node that can outlive a
    /// process.
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
            storage: None,
            durability_failed: false,
        }
    }

    /// Open a **durable** node, restoring term, vote, log, commit index and
    /// snapshot from `dir` (created if absent).
    ///
    /// The restored term/vote are what make a restart safe: a node that already
    /// voted in term T comes back still knowing it voted, so it cannot be talked
    /// into a second vote in T and cannot help elect a second leader.
    pub fn open(id: NodeId, peers: Vec<NodeId>, dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let (store, persisted) = storage::RaftStorage::open(dir)?;

        // The in-memory log always starts with a sentinel. Without a snapshot it
        // is the index-0 origin; with one it stands in for the compacted prefix
        // and carries the snapshot's index/term so log-matching still works.
        let sentinel = match &persisted.snapshot {
            Some(s) => LogEntry {
                index: s.last_included_index,
                term: s.last_included_term,
                command: Command::Noop,
            },
            None => LogEntry {
                index: 0,
                term: 0,
                command: Command::Noop,
            },
        };
        let mut log = Vec::with_capacity(persisted.entries.len() + 1);
        log.push(sentinel);
        log.extend(persisted.entries);

        let snapshot_index = persisted
            .snapshot
            .as_ref()
            .map(|s| s.last_included_index)
            .unwrap_or(0);
        // Everything the snapshot covers is by definition already applied.
        let last_applied = snapshot_index;
        let commit_index = persisted.commit_index.max(snapshot_index);

        let mut node = Self::new(id, peers);
        node.current_term = persisted.current_term;
        node.voted_for = persisted.voted_for;
        node.commit_index = commit_index;
        node.last_applied = last_applied;
        node.log = log;
        node.snapshot = persisted.snapshot;
        node.storage = Some(store);
        Ok(node)
    }

    /// Whether a durable write has failed. Such a node refuses to vote or to
    /// acknowledge appends, because it can no longer honour Raft's persistence
    /// preconditions.
    pub fn durability_failed(&self) -> bool {
        self.durability_failed
    }

    /// Whether this node persists its state at all.
    pub fn is_durable(&self) -> bool {
        self.storage.is_some()
    }

    // ── Durability helpers ───────────────────────────────────────────────────

    /// fsync term / vote / commit index. Returns `false` on failure, which the
    /// caller must translate into a refusal.
    fn persist_hard_state(&mut self) -> bool {
        let (term, vote, commit) = (self.current_term, self.voted_for, self.commit_index);
        let Some(store) = self.storage.as_mut() else {
            return true;
        };
        match store.save_hard_state(term, vote, commit) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("raft: hard-state fsync failed, refusing operation: {e}");
                self.durability_failed = true;
                false
            }
        }
    }

    /// Record a commit-index advance. Best-effort by design: a persisted commit
    /// index is allowed to lag (it is relearned from the leader) but never to
    /// run ahead, so a failure here is not a safety problem.
    fn note_commit_index(&mut self) {
        let (term, vote, commit) = (self.current_term, self.voted_for, self.commit_index);
        if let Some(store) = self.storage.as_mut()
            && let Err(e) = store.note_commit_index(term, vote, commit)
        {
            tracing::warn!("raft: commit-index checkpoint failed (recoverable): {e}");
        }
    }

    /// fsync freshly appended log entries. Returns `false` on failure.
    fn persist_entries(&mut self, entries: &[LogEntry]) -> bool {
        let Some(store) = self.storage.as_mut() else {
            return true;
        };
        match store.append_entries(entries) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("raft: log append fsync failed, refusing operation: {e}");
                self.durability_failed = true;
                false
            }
        }
    }

    /// Atomically rewrite the durable log to match the in-memory log. Used after
    /// a conflicting suffix is truncated or a snapshot compacts a prefix.
    fn persist_log_rewrite(&mut self) -> bool {
        let entries: Vec<LogEntry> = self.log.iter().skip(1).cloned().collect();
        let Some(store) = self.storage.as_mut() else {
            return true;
        };
        match store.rewrite_log(&entries) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("raft: log rewrite failed, refusing operation: {e}");
                self.durability_failed = true;
                false
            }
        }
    }

    /// fsync snapshot metadata and data.
    fn persist_snapshot(&mut self) -> bool {
        let Some(snap) = self.snapshot.clone() else {
            return true;
        };
        let Some(store) = self.storage.as_mut() else {
            return true;
        };
        match store.save_snapshot(&snap) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("raft: snapshot fsync failed: {e}");
                self.durability_failed = true;
                false
            }
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

    /// The lowest log index still held in memory (the sentinel's index). Zero
    /// on a fresh node; the snapshot's last-included index after compaction.
    fn log_base(&self) -> LogIndex {
        self.log.first().map(|e| e.index).unwrap_or(0)
    }

    /// Position of `index` within `self.log`, accounting for a compacted prefix.
    ///
    /// Before any snapshot the base is 0 and position == index; after compaction
    /// they diverge, so every lookup must go through here rather than indexing
    /// the vector with a log index directly.
    fn log_pos(&self, index: LogIndex) -> Option<usize> {
        let base = self.log_base();
        if index < base {
            return None;
        }
        let pos = (index - base) as usize;
        (pos < self.log.len()).then_some(pos)
    }

    /// Get the log entry at a specific index.
    pub fn log_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.log_pos(index).and_then(|p| self.log.get(p))
    }

    /// Start an election: become candidate, vote for self, increment term.
    ///
    /// The new term and the self-vote are fsync'd before any `RequestVote` is
    /// handed to the caller — otherwise a crash could leave the node able to
    /// vote again in the same term it already campaigned in.
    pub fn start_election(&mut self) -> Vec<(NodeId, RequestVoteRequest)> {
        if self.durability_failed {
            return Vec::new();
        }
        let prev = (self.current_term, self.voted_for, self.role, self.leader_id);

        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received = vec![self.id];
        self.leader_id = None;

        if !self.persist_hard_state() {
            // Could not durably claim the term — unwind and stay put.
            self.current_term = prev.0;
            self.voted_for = prev.1;
            self.role = prev.2;
            self.leader_id = prev.3;
            self.votes_received.clear();
            return Vec::new();
        }

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
    ///
    /// The response is not produced until the resulting term and vote are on
    /// stable storage. That ordering *is* the anti-double-vote guarantee: a
    /// response that outran its own fsync would let a crashed-and-restarted node
    /// grant a second vote in the same term and elect a second leader.
    pub fn handle_request_vote(&mut self, req: &RequestVoteRequest) -> RequestVoteResponse {
        if self.durability_failed {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }
        let prev = (self.current_term, self.voted_for, self.role, self.leader_id);

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

        // Persist BEFORE replying. Both the term bump and the vote are visible
        // to the peer through this response, so both must already be durable.
        if (self.current_term, self.voted_for) != (prev.0, prev.1) && !self.persist_hard_state() {
            self.current_term = prev.0;
            self.voted_for = prev.1;
            self.role = prev.2;
            self.leader_id = prev.3;
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

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
            self.persist_hard_state();
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
    ///
    /// The entry is fsync'd before the index is returned: the leader counts
    /// itself toward the commit quorum, so an entry it has not durably stored
    /// must not be allowed to influence a commit decision.
    pub fn append_entry(&mut self, command: Command) -> Option<LogIndex> {
        if self.role != Role::Leader || self.durability_failed {
            return None;
        }

        let index = self.last_log_index() + 1;
        let entry = LogEntry {
            index,
            term: self.current_term,
            command,
        };
        self.log.push(entry.clone());

        if !self.persist_entries(&[entry]) {
            self.log.pop();
            return None;
        }

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
    ///
    /// `success: true` is a promise the leader is entitled to count toward a
    /// commit quorum — and therefore to report to a client as durable. So the
    /// entries and any term change are fsync'd before this returns; if that
    /// fails the node answers `success: false` instead of lying.
    pub fn handle_append_entries(&mut self, req: &AppendEntriesRequest) -> AppendEntriesResponse {
        // Stale term
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }
        if self.durability_failed {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }

        // Update term if needed
        let term_changed = req.term > self.current_term;
        if term_changed {
            self.current_term = req.term;
            self.voted_for = None;
        }

        self.role = Role::Follower;
        self.leader_id = Some(req.leader_id);

        // A term bump must be durable before it is echoed back in the response.
        if term_changed && !self.persist_hard_state() {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }

        // Check if we have the prev_log entry. After compaction the sentinel
        // carries the snapshot's index/term, so this check still works at the
        // snapshot boundary; anything below it makes us answer false, which is
        // the leader's cue to send a snapshot instead.
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
                        if let Some(pos) = self.log_pos(req.prev_log_index) {
                            self.log.truncate(pos);
                        }
                        self.persist_log_rewrite();
                        return AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            match_index: self.last_log_index(),
                        };
                    }
                }
            }
        }

        // Append new entries (handle conflicts). Track whether a suffix was
        // discarded: an append-only log file cannot express that, so a conflict
        // forces a full atomic rewrite rather than an append.
        let mut appended: Vec<LogEntry> = Vec::new();
        let mut rewrote = false;
        for entry in &req.entries {
            match self.log_at(entry.index) {
                Some(existing) => {
                    if existing.term != entry.term {
                        if let Some(pos) = self.log_pos(entry.index) {
                            self.log.truncate(pos);
                        }
                        self.log.push(entry.clone());
                        rewrote = true;
                    }
                }
                None => {
                    // Only extend contiguously; a gap would corrupt the log.
                    if entry.index == self.last_log_index() + 1 {
                        self.log.push(entry.clone());
                        appended.push(entry.clone());
                    }
                }
            }
        }

        // Persist the log BEFORE acknowledging.
        let persisted = if rewrote {
            self.persist_log_rewrite()
        } else {
            self.persist_entries(&appended)
        };
        if !persisted {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }

        // Update commit index
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
            self.note_commit_index();
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
            self.persist_hard_state();
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
        let mut advanced = false;

        for n in (self.commit_index + 1)..=self.last_log_index() {
            // Only commit entries from current term
            if let Some(entry) = self.log_at(n)
                && entry.term != self.current_term
            {
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
                advanced = true;
            }
        }

        if advanced {
            self.note_commit_index();
        }
    }

    /// Apply committed entries to the state machine. Returns indices of applied entries.
    pub fn apply_committed(&mut self) -> Vec<LogIndex> {
        let mut applied = Vec::new();

        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log_at(self.last_applied) {
                let command = entry.command.clone();
                self.applied_commands.push(command);
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
        if let Some(keep_from) = self.log_pos(snap_index) {
            self.log = std::iter::once(LogEntry {
                index: snap_index,
                term: snap_term,
                command: Command::Noop,
            })
            .chain(self.log.drain((keep_from + 1)..))
            .collect();
        }

        // Snapshot first, then the compacted log: if the crash lands between
        // them we still hold a superset of the state (an uncompacted log plus a
        // valid snapshot), never a hole.
        self.persist_snapshot();
        self.persist_log_rewrite();
        self.persist_hard_state();

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
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        if req.term < self.current_term {
            return InstallSnapshotResponse {
                term: self.current_term,
            };
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

        // All three are durable before the follower acknowledges the install.
        self.persist_snapshot();
        self.persist_log_rewrite();
        self.persist_hard_state();

        InstallSnapshotResponse {
            term: self.current_term,
        }
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
                LogEntry {
                    index: 1,
                    term: 1,
                    command: Command::Noop,
                },
                LogEntry {
                    index: 2,
                    term: 1,
                    command: Command::Sql("CREATE TABLE t (id INT)".into()),
                },
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
        let r3_for_1 =
            node3.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(r3_for_1.vote_granted);
        let r3_for_2 =
            node3.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(!r3_for_2.vote_granted);
        let r5_for_1 =
            node5.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 5).unwrap().1);
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
        let r3_for_1 =
            node3.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(r3_for_1.vote_granted);
        let r3_for_2 =
            node3.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 3).unwrap().1);
        assert!(!r3_for_2.vote_granted);
        let r4_for_2 =
            node4.handle_request_vote(&votes2.iter().find(|(id, _)| *id == 4).unwrap().1);
        assert!(r4_for_2.vote_granted);
        let r4_for_1 =
            node4.handle_request_vote(&votes1.iter().find(|(id, _)| *id == 4).unwrap().1);
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
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                command: Command::Noop,
            }],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(&req1);
        assert!(resp.success);
        assert_eq!(follower.last_log_index(), 1);
        let req_gap = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![LogEntry {
                index: 3,
                term: 1,
                command: Command::Sql("SELECT 1".into()),
            }],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(&req_gap);
        assert!(!resp.success);
        let req_bad_term = AppendEntriesRequest {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![LogEntry {
                index: 2,
                term: 2,
                command: Command::Sql("SELECT 2".into()),
            }],
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
            term: 5,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
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
        let resp = AppendEntriesResponse {
            term: 5,
            success: false,
            match_index: 0,
        };
        leader.handle_append_response(2, &resp);
        assert_eq!(leader.role, Role::Follower);
        assert_eq!(leader.current_term, 5);
    }

    #[test]
    fn apply_committed_entries() {
        let mut node = RaftNode::new(1, vec![2, 3]);
        node.log.push(LogEntry {
            index: 1,
            term: 1,
            command: Command::Noop,
        });
        node.log.push(LogEntry {
            index: 2,
            term: 1,
            command: Command::Sql("INSERT 1".into()),
        });
        node.log.push(LogEntry {
            index: 3,
            term: 1,
            command: Command::Sql("INSERT 2".into()),
        });
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

    // ================================================================
    // Restart safety
    //
    // These exercise the property Raft actually depends on: state that
    // an RPC response already promised must still be there after the
    // process dies. "Crash" here is dropping the node and reopening the
    // same directory — the on-disk bytes are all a restarted process
    // gets, so anything not fsync'd is gone by construction.
    // ================================================================

    fn raft_tmpdir(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "nucleus_raftnode_{name}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// THE safety test. A node votes for candidate 2 in term 5, crashes, and
    /// restarts. Candidate 3 then asks for a vote in the same term 5.
    ///
    /// Granting it would give term 5 two majorities and therefore two leaders,
    /// which lets committed entries be overwritten — unbounded, silent data
    /// loss. The restarted node must refuse.
    ///
    /// Without persistence the restarted node comes back at term 0 with no
    /// recorded vote and cheerfully votes again, which is exactly the bug.
    #[test]
    fn restarted_node_will_not_vote_twice_in_the_same_term() {
        let dir = raft_tmpdir("double_vote");

        let granted_to_2 = {
            let mut node = RaftNode::open(1, vec![2, 3], &dir).unwrap();
            let resp = node.handle_request_vote(&RequestVoteRequest {
                term: 5,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            });
            assert!(resp.vote_granted, "first vote in term 5 should be granted");
            assert_eq!(resp.term, 5);
            resp.vote_granted
            // node dropped here == the process dies
        };
        assert!(granted_to_2);

        // Restart: everything not on disk is gone.
        let mut restarted = RaftNode::open(1, vec![2, 3], &dir).unwrap();
        assert_eq!(
            restarted.current_term, 5,
            "term must survive the crash, else the node moves backwards in time"
        );
        assert_eq!(
            restarted.voted_for,
            Some(2),
            "the recorded vote must survive the crash"
        );

        let second = restarted.handle_request_vote(&RequestVoteRequest {
            term: 5,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(
            !second.vote_granted,
            "restarted node granted a SECOND vote in term 5 — two leaders can now be \
             elected in one term and committed entries can be overwritten"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A candidate that crashes mid-election must not campaign again in the
    /// same term with a fresh vote budget: its own self-vote has to survive.
    #[test]
    fn candidates_own_self_vote_survives_a_crash() {
        let dir = raft_tmpdir("self_vote");
        {
            let mut node = RaftNode::open(7, vec![8, 9], &dir).unwrap();
            let reqs = node.start_election();
            assert_eq!(reqs.len(), 2);
            assert_eq!(node.current_term, 1);
            assert_eq!(node.voted_for, Some(7));
        }
        let mut restarted = RaftNode::open(7, vec![8, 9], &dir).unwrap();
        assert_eq!(restarted.current_term, 1);
        assert_eq!(restarted.voted_for, Some(7));

        // Another candidate in the same term must be refused.
        let resp = restarted.handle_request_vote(&RequestVoteRequest {
            term: 1,
            candidate_id: 8,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(
            !resp.vote_granted,
            "restarted candidate forgot its own self-vote and voted for a rival in term 1"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A follower answering `success: true` is telling the leader "this entry is
    /// mine now". The leader may count that toward a commit quorum and report
    /// the write durable to a client. So those entries must survive a crash.
    #[test]
    fn acknowledged_entries_survive_a_crash() {
        let dir = raft_tmpdir("acked_entries");
        {
            let mut follower = RaftNode::open(2, vec![1, 3], &dir).unwrap();
            let resp = follower.handle_append_entries(&AppendEntriesRequest {
                term: 3,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        index: 1,
                        term: 3,
                        command: Command::Noop,
                    },
                    LogEntry {
                        index: 2,
                        term: 3,
                        command: Command::Sql("INSERT INTO t VALUES (1)".into()),
                    },
                    LogEntry {
                        index: 3,
                        term: 3,
                        command: Command::Sql("INSERT INTO t VALUES (2)".into()),
                    },
                ],
                leader_commit: 3,
            });
            assert!(resp.success, "follower must accept a clean append");
            assert_eq!(resp.match_index, 3);
        }

        let restarted = RaftNode::open(2, vec![1, 3], &dir).unwrap();
        assert_eq!(
            restarted.last_log_index(),
            3,
            "the follower acknowledged 3 entries and then lost them across a restart; \
             the leader may already have told a client those writes were committed"
        );
        assert_eq!(restarted.current_term, 3);
        match &restarted.log_at(2).expect("entry 2 must exist").command {
            Command::Sql(sql) => assert_eq!(sql, "INSERT INTO t VALUES (1)"),
            other => panic!("entry 2 came back as {other:?}"),
        }
        match &restarted.log_at(3).expect("entry 3 must exist").command {
            Command::Sql(sql) => assert_eq!(sql, "INSERT INTO t VALUES (2)"),
            other => panic!("entry 3 came back as {other:?}"),
        }
        // The commit index is allowed to come back LOW (it is relearned from the
        // leader's next AppendEntries). What it must never do is come back HIGH,
        // which would mark uncommitted entries as committed.
        assert!(
            restarted.commit_index <= 3,
            "restart reported commit index {} but only 3 entries were ever committed",
            restarted.commit_index
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Commit-index persistence is checkpointed, not written on every advance.
    /// Pin both halves of that contract: a large advance does get written, and a
    /// restart never reports more than was actually committed.
    #[test]
    fn commit_index_is_checkpointed_and_never_restored_too_high() {
        let dir = raft_tmpdir("commit_checkpoint");
        let committed;
        {
            let mut follower = RaftNode::open(2, vec![1, 3], &dir).unwrap();
            let entries: Vec<LogEntry> = (1..=200)
                .map(|i| LogEntry {
                    index: i,
                    term: 1,
                    command: Command::Sql(format!("INSERT INTO t VALUES ({i})")),
                })
                .collect();
            let resp = follower.handle_append_entries(&AppendEntriesRequest {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 200,
            });
            assert!(resp.success);
            committed = follower.commit_index;
            assert_eq!(committed, 200);
        }
        let restarted = RaftNode::open(2, vec![1, 3], &dir).unwrap();
        assert!(
            restarted.commit_index > 0,
            "a 200-entry commit advance was never checkpointed at all"
        );
        assert!(
            restarted.commit_index <= committed,
            "restart claimed commit index {} but only {} was committed — uncommitted \
             entries would be treated as committed",
            restarted.commit_index,
            committed
        );
        assert_eq!(restarted.last_log_index(), 200);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The leader counts itself toward the commit quorum, so an entry it has
    /// appended must be durable before that self-count can matter.
    #[test]
    fn leader_appended_entries_survive_a_crash() {
        let dir = raft_tmpdir("leader_append");
        {
            let mut leader = RaftNode::open(1, vec![2, 3], &dir).unwrap();
            leader.role = Role::Leader;
            leader.current_term = 2;
            assert_eq!(
                leader.append_entry(Command::Sql("UPDATE t SET x = 1".into())),
                Some(1)
            );
            assert_eq!(leader.append_entry(Command::Noop), Some(2));
        }
        let restarted = RaftNode::open(1, vec![2, 3], &dir).unwrap();
        assert_eq!(restarted.last_log_index(), 2);
        assert!(
            matches!(&restarted.log_at(1).unwrap().command, Command::Sql(s) if s == "UPDATE t SET x = 1")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Durability must happen BEFORE the response is returned, not merely at
    /// some point afterwards. A response that outran its own fsync is exactly as
    /// unsafe as no persistence, because the crash window is the interesting
    /// case. Reading the files through a second, independent handle while the
    /// node is still alive is the observable form of that ordering.
    #[test]
    fn vote_is_on_disk_before_the_response_is_returned() {
        let dir = raft_tmpdir("fsync_order");
        let mut node = RaftNode::open(1, vec![2, 3], &dir).unwrap();

        let resp = node.handle_request_vote(&RequestVoteRequest {
            term: 9,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(resp.vote_granted);

        // Node still alive; read what a restarting process would see right now.
        let (_s, on_disk) = storage::RaftStorage::open(&dir).unwrap();
        assert_eq!(
            (on_disk.current_term, on_disk.voted_for),
            (9, Some(2)),
            "the vote response was returned before the vote reached stable storage"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same ordering requirement for the log: the entries must be readable from
    /// disk at the instant `success: true` is produced.
    #[test]
    fn entries_are_on_disk_before_success_is_returned() {
        let dir = raft_tmpdir("append_fsync_order");
        let mut node = RaftNode::open(2, vec![1, 3], &dir).unwrap();

        let resp = node.handle_append_entries(&AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                command: Command::Sql("INSERT INTO t VALUES (42)".into()),
            }],
            leader_commit: 0,
        });
        assert!(resp.success);

        let (_s, on_disk) = storage::RaftStorage::open(&dir).unwrap();
        assert_eq!(
            on_disk.entries.len(),
            1,
            "the follower acknowledged an entry that was not yet on stable storage"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Raft repairs a divergent follower by truncating the conflicting suffix.
    /// An append-only file cannot express a truncation, so the durable log must
    /// be rewritten — otherwise a restart resurrects entries the leader already
    /// overruled.
    #[test]
    fn truncated_conflicting_suffix_does_not_come_back_after_restart() {
        let dir = raft_tmpdir("truncate");
        {
            let mut follower = RaftNode::open(2, vec![1, 3], &dir).unwrap();
            // Stale leader's entries.
            follower.handle_append_entries(&AppendEntriesRequest {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        index: 1,
                        term: 1,
                        command: Command::Sql("STALE-1".into()),
                    },
                    LogEntry {
                        index: 2,
                        term: 1,
                        command: Command::Sql("STALE-2".into()),
                    },
                ],
                leader_commit: 0,
            });
            assert_eq!(follower.last_log_index(), 2);

            // New leader in term 2 overwrites index 2.
            let resp = follower.handle_append_entries(&AppendEntriesRequest {
                term: 2,
                leader_id: 3,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![LogEntry {
                    index: 2,
                    term: 2,
                    command: Command::Sql("AUTHORITATIVE-2".into()),
                }],
                leader_commit: 0,
            });
            assert!(resp.success);
            assert!(
                matches!(&follower.log_at(2).unwrap().command, Command::Sql(s) if s == "AUTHORITATIVE-2")
            );
        }

        let restarted = RaftNode::open(2, vec![1, 3], &dir).unwrap();
        assert_eq!(restarted.last_log_index(), 2);
        assert_eq!(restarted.current_term, 2);
        match &restarted.log_at(2).unwrap().command {
            Command::Sql(sql) => assert_eq!(
                sql, "AUTHORITATIVE-2",
                "the overruled entry came back after restart; the durable log still \
                 holds a suffix the leader truncated"
            ),
            other => panic!("entry 2 came back as {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Snapshot metadata and the compacted log must both survive, and the
    /// restored node must index its log correctly across the compaction
    /// boundary (positions no longer equal log indices after compaction).
    #[test]
    fn snapshot_and_compacted_log_survive_a_crash() {
        let dir = raft_tmpdir("snapshot");
        {
            let mut node = RaftNode::open(1, vec![2, 3], &dir).unwrap();
            node.handle_append_entries(&AppendEntriesRequest {
                term: 4,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: (1..=5)
                    .map(|i| LogEntry {
                        index: i,
                        term: 4,
                        command: Command::Sql(format!("STMT-{i}")),
                    })
                    .collect(),
                leader_commit: 3,
            });
            node.apply_committed();
            assert_eq!(node.last_applied, 3);
            node.take_snapshot(b"machine-state".to_vec())
                .expect("snapshot should be taken at last_applied");
        }

        let restarted = RaftNode::open(1, vec![2, 3], &dir).unwrap();
        let snap = restarted
            .snapshot
            .as_ref()
            .expect("snapshot metadata must survive restart");
        assert_eq!(snap.last_included_index, 3);
        assert_eq!(snap.last_included_term, 4);
        assert_eq!(snap.data, b"machine-state");

        assert_eq!(
            restarted.last_log_index(),
            5,
            "entries after the snapshot must survive compaction + restart"
        );
        assert!(
            restarted.log_at(3).is_some(),
            "the snapshot boundary must be addressable so log matching still works"
        );
        // Positions and indices diverge after compaction; lookups must respect that.
        match &restarted.log_at(5).expect("entry 5 must exist").command {
            Command::Sql(sql) => assert_eq!(sql, "STMT-5"),
            other => panic!("entry 5 came back as {other:?}"),
        }
        assert!(restarted.last_applied >= 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A node built with `new()` is explicitly volatile. Say so in a test so the
    /// distinction is not folded away by accident: production paths must use
    /// `open()`.
    #[test]
    fn volatile_nodes_advertise_that_they_are_not_durable() {
        let node = RaftNode::new(1, vec![2]);
        assert!(!node.is_durable());
        let dir = raft_tmpdir("durable_flag");
        let durable = RaftNode::open(1, vec![2], &dir).unwrap();
        assert!(durable.is_durable());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
