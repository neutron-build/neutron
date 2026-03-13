/-
  Custom Tactics — shared proof automation for Nucleus verification.
-/

namespace Nucleus.Helpers

/-- Tactic to unfold all Nucleus definitions and simplify. -/
macro "nucleus_simp" : tactic =>
  `(tactic| (simp_all [
    Nucleus.Aeneas.Snapshot.isVisible,
    Nucleus.Aeneas.RowVersion.isCommitted,
    Nucleus.Aeneas.RowVersion.isDeleted,
    Nucleus.Aeneas.WAL.append,
    Nucleus.Aeneas.WAL.flush,
    Nucleus.Aeneas.WAL.recoveryRecords,
    Nucleus.Aeneas.RaftNode.startElection,
    Nucleus.Aeneas.RaftNode.stepDown,
    Nucleus.Aeneas.RaftNode.becomeLeader,
    Nucleus.Aeneas.RaftNode.appendEntry
  ]))

/-- Decide boolean propositions about visibility. -/
macro "decide_visibility" : tactic =>
  `(tactic| (
    simp [Nucleus.Aeneas.Snapshot.isVisible]
    split <;> simp_all
    split <;> simp_all
    split <;> simp_all
    split <;> simp_all
  ))

end Nucleus.Helpers
