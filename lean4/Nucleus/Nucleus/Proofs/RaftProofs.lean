/-
  Raft Proofs — machine-checked proofs of consensus safety properties.
-/
import Nucleus.Spec.RaftSpec

namespace Nucleus.Proofs

open Nucleus.Aeneas
open Nucleus.Spec

/-- Starting an election makes the node a candidate. -/
theorem election_role (node : RaftNode) :
    (node.startElection).role = .candidate := by
  simp [RaftNode.startElection]

/-- Stepping down always produces a follower. -/
theorem stepdown_follower (node : RaftNode) (term : Term) :
    (node.stepDown term).role = .follower := by
  exact step_down_becomes_follower node term

/-- Stepping down clears the voted-for field. -/
theorem stepdown_clears_vote (node : RaftNode) (term : Term) :
    (node.stepDown term).votedFor = none := by
  simp [RaftNode.stepDown]

/-- Appending an entry increases log length by 1. -/
theorem append_increases_log (node : RaftNode) (entry : LogEntry) :
    (node.appendEntry entry).log.length = node.log.length + 1 := by
  simp [RaftNode.appendEntry, List.length_append]

/-- An empty cluster trivially satisfies election safety. -/
theorem empty_cluster_safe :
    electionSafety (RaftCluster.mk []) := by
  intro term
  simp [RaftCluster.leaderCount, electionSafety]

end Nucleus.Proofs
