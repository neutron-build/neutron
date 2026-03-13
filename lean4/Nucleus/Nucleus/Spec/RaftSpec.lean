/-
  Raft Safety Formal Specifications.
-/
import Nucleus.Aeneas.Raft

namespace Nucleus.Spec

open Nucleus.Aeneas

/-- Election Safety: at most one leader per term in a well-behaved cluster. -/
def electionSafety (cluster : RaftCluster) : Prop :=
  ∀ term : Term, cluster.leaderCount term ≤ 1

/-- Leader Append-Only: a leader never removes entries from its log. -/
def leaderAppendOnly (before after : RaftNode) : Prop :=
  before.role = .leader →
  after.role = .leader →
  before.currentTerm = after.currentTerm →
  before.log.length ≤ after.log.length ∧
  ∀ i, i < before.log.length →
    before.log.get? i = after.log.get? i

/-- Log Matching: if two logs have same entry at index, all prior entries match. -/
def logMatching (log1 log2 : List LogEntry) : Prop :=
  ∀ idx, idx < log1.length → idx < log2.length →
    (log1.get? idx).bind (fun e => some e.term) =
    (log2.get? idx).bind (fun e => some e.term) →
    ∀ i, i ≤ idx → log1.get? i = log2.get? i

/-- State Machine Safety: same index → same command applied. -/
def stateMachineSafety (cluster : RaftCluster) : Prop :=
  ∀ n1 n2, n1 ∈ cluster.nodes → n2 ∈ cluster.nodes →
    ∀ idx, idx ≤ n1.commitIndex → idx ≤ n2.commitIndex →
      n1.log.get? idx = n2.log.get? idx

/-- Step down: receiving a higher term causes step down. -/
theorem step_down_updates_term (node : RaftNode) (term : Term)
    (h : term > node.currentTerm) :
    (node.stepDown term).currentTerm = term := by
  simp [RaftNode.stepDown]

/-- Step down: node becomes follower. -/
theorem step_down_becomes_follower (node : RaftNode) (term : Term) :
    (node.stepDown term).role = .follower := by
  simp [RaftNode.stepDown]

/-- Election: starting election increments term. -/
theorem election_increments_term (node : RaftNode) :
    (node.startElection).currentTerm = node.currentTerm + 1 := by
  simp [RaftNode.startElection]

/-- Election: candidate votes for itself. -/
theorem election_self_vote (node : RaftNode) :
    (node.startElection).votedFor = some node.nodeId := by
  simp [RaftNode.startElection]

end Nucleus.Spec
