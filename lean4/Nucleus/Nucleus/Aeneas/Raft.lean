/-
  Raft Consensus — Aeneas-translated model.
  Models the core types from `nucleus/src/raft/mod.rs`.
-/

namespace Nucleus.Aeneas

abbrev NodeId := Nat
abbrev Term := Nat
abbrev LogIndex := Nat

/-- Raft node role. -/
inductive Role where
  | follower
  | candidate
  | leader
  deriving Repr, BEq

/-- A log entry in the Raft log. -/
structure LogEntry where
  term : Term
  index : LogIndex
  command : List Nat  -- simplified command payload
  deriving Repr, BEq

/-- State of a single Raft node. -/
structure RaftNode where
  nodeId : NodeId
  currentTerm : Term
  votedFor : Option NodeId
  log : List LogEntry
  commitIndex : LogIndex
  lastApplied : LogIndex
  role : Role
  deriving Repr

/-- A Raft cluster. -/
structure RaftCluster where
  nodes : List RaftNode
  deriving Repr

/-- Get the last log entry's term and index. -/
def RaftNode.lastLogInfo (node : RaftNode) : Term × LogIndex :=
  match node.log.getLast? with
  | some entry => (entry.term, entry.index)
  | none => (0, 0)

/-- Check if a candidate's log is at least as up-to-date as this node. -/
def RaftNode.isLogUpToDate (node : RaftNode) (candidateTerm : Term)
    (candidateIndex : LogIndex) : Bool :=
  let (lastTerm, lastIndex) := node.lastLogInfo
  candidateTerm > lastTerm || (candidateTerm == lastTerm && candidateIndex ≥ lastIndex)

/-- Start an election: transition to candidate, increment term, vote for self. -/
def RaftNode.startElection (node : RaftNode) : RaftNode :=
  { node with
    currentTerm := node.currentTerm + 1,
    role := .candidate,
    votedFor := some node.nodeId }

/-- Become leader: transition role, initialize follower state. -/
def RaftNode.becomeLeader (node : RaftNode) : RaftNode :=
  { node with role := .leader }

/-- Step down to follower. -/
def RaftNode.stepDown (node : RaftNode) (term : Term) : RaftNode :=
  { node with
    currentTerm := term,
    role := .follower,
    votedFor := none }

/-- Append an entry to the log. -/
def RaftNode.appendEntry (node : RaftNode) (entry : LogEntry) : RaftNode :=
  { node with log := node.log ++ [entry] }

/-- Count leaders in the cluster for a given term. -/
def RaftCluster.leaderCount (cluster : RaftCluster) (term : Term) : Nat :=
  cluster.nodes.filter (fun n => n.role == .leader && n.currentTerm == term) |>.length

end Nucleus.Aeneas
