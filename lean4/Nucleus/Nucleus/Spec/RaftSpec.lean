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
    (_h : term > node.currentTerm) :
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

namespace Nucleus.Spec
open Nucleus.Aeneas

/-- Hand-written pre-leadership transition subset. Quorum election is excluded. -/
def initialNode (id : NodeId) : RaftNode :=
  { nodeId := id, currentTerm := 0, votedFor := none, log := [], commitIndex := 0,
    lastApplied := 0, role := .follower }

def initialCluster : RaftCluster := ⟨[initialNode 1, initialNode 2, initialNode 3]⟩

def noLeaders (cluster : RaftCluster) : Prop := ∀ node ∈ cluster.nodes, node.role ≠ .leader

/-- Election-start and higher-term receipt can occur on every cluster node. -/
inductive PreLeadershipStep : RaftCluster → RaftCluster → Prop where
  | electionStart (cluster : RaftCluster) :
      PreLeadershipStep cluster ⟨cluster.nodes.map RaftNode.startElection⟩
  | higherTerm (cluster : RaftCluster) (term : Term) :
      PreLeadershipStep cluster ⟨cluster.nodes.map (fun node => node.stepDown term)⟩

inductive PreLeadershipReachable : RaftCluster → Prop where
  | initial : PreLeadershipReachable initialCluster
  | next {before after : RaftCluster} : PreLeadershipReachable before →
      PreLeadershipStep before after → PreLeadershipReachable after

theorem initial_no_leaders : noLeaders initialCluster := by
  intro node h
  simp [initialCluster, initialNode] at h
  rcases h with h | h | h <;> subst node <;> simp [initialNode]

theorem preleadership_step_preserves {before after : RaftCluster}
    (_h : noLeaders before) (step : PreLeadershipStep before after) : noLeaders after := by
  cases step with
  | electionStart =>
    intro node h
    obtain ⟨original, _, rfl⟩ := List.mem_map.mp h
    simp [RaftNode.startElection]
  | higherTerm term =>
    intro node h
    obtain ⟨original, _, rfl⟩ := List.mem_map.mp h
    simp [RaftNode.stepDown]

theorem preleadership_reachable_no_leaders {cluster : RaftCluster}
    (h : PreLeadershipReachable cluster) : noLeaders cluster := by
  induction h with
  | initial => exact initial_no_leaders
  | next _ step ih => exact preleadership_step_preserves ih step

theorem preleadership_reachable_nonempty {cluster : RaftCluster}
    (h : PreLeadershipReachable cluster) : cluster.nodes.length = 3 := by
  induction h with
  | initial => rfl
  | next _ step ih => cases step <;> simpa using ih

theorem preleadership_reachable_election_safety {cluster : RaftCluster}
    (h : PreLeadershipReachable cluster) : electionSafety cluster := by
  have safe := preleadership_reachable_no_leaders h
  intro term
  have empty : cluster.nodes.filter (fun node => node.role == .leader && node.currentTerm == term) = [] := by
    apply List.filter_eq_nil.mpr
    intro node mem
    have nr := safe node mem
    cases hr : node.role with
    | follower =>
      have hb : (Role.follower == Role.leader) = false := rfl
      simp [hr, hb]
    | candidate =>
      have hb : (Role.candidate == Role.leader) = false := rfl
      simp [hr, hb]
    | leader => exact False.elim (nr hr)
  simp [RaftCluster.leaderCount, empty]
end Nucleus.Spec

namespace Nucleus.Spec.Quorum
/-- Three fixed voters. Votes are durable and retained separately for every term.
    This election kernel abstracts persistent term-indexed vote history; it does
    not refine the production mutable current-term implementation. -/
structure State where
  votes : Nat → Nat → Option Nat
  elected : Nat → Nat → Prop

def initial : State := ⟨fun _ _ => none, fun _ _ => False⟩
def quorum (s : State) (term candidate : Nat) : Prop :=
  (s.votes term 1 = some candidate ∧ s.votes term 2 = some candidate) ∨
  (s.votes term 1 = some candidate ∧ s.votes term 3 = some candidate) ∨
  (s.votes term 2 = some candidate ∧ s.votes term 3 = some candidate)

def certified (s : State) : Prop := ∀ t c, s.elected t c → quorum s t c

def grant (s : State) (term voter candidate : Nat) : State :=
  { s with votes := fun t v => if t = term ∧ v = voter then some candidate else s.votes t v }
def elect (s : State) (term candidate : Nat) : State :=
  { s with elected := fun t c => (t = term ∧ c = candidate) ∨ s.elected t c }

inductive Step : State → State → Prop where
  | vote (s : State) (t v c : Nat) (hv : v = 1 ∨ v = 2 ∨ v = 3)
      (free : s.votes t v = none) : Step s (grant s t v c)
  | publish (s : State) (t c : Nat) (certificate : quorum s t c) : Step s (elect s t c)
inductive Reachable : State → Prop where
  | start : Reachable initial
  | next {s s' : State} : Reachable s → Step s s' → Reachable s'

private theorem grant_preserves_vote (s : State) (t v c t' v' c' : Nat)
    (free : s.votes t v = none) (owned : s.votes t' v' = some c') :
    (grant s t v c).votes t' v' = some c' := by
  by_cases eq : t' = t ∧ v' = v
  · rcases eq with ⟨rfl, rfl⟩
    rw [free] at owned
    contradiction
  · simp [grant, eq, owned]

theorem step_preserves_certificates {s s' : State} (safe : certified s) (step : Step s s') :
    certified s' := by
  cases step with
  | vote before t v c _ free =>
    intro t' c' elected
    have q := safe t' c' elected
    rcases q with q | q | q
    · exact Or.inl ⟨grant_preserves_vote before t v c t' 1 c' free q.1,
        grant_preserves_vote before t v c t' 2 c' free q.2⟩
    · exact Or.inr (Or.inl ⟨grant_preserves_vote before t v c t' 1 c' free q.1,
        grant_preserves_vote before t v c t' 3 c' free q.2⟩)
    · exact Or.inr (Or.inr ⟨grant_preserves_vote before t v c t' 2 c' free q.1,
        grant_preserves_vote before t v c t' 3 c' free q.2⟩)
  | publish before t c certificate =>
    intro t' c' elected
    rcases elected with eq | old
    · rcases eq with ⟨rfl, rfl⟩; exact certificate
    · exact safe t' c' old

theorem reachable_certified {s : State} (reachable : Reachable s) : certified s := by
  induction reachable with
  | start => intro t c h; exact False.elim h
  | next _ step ih => exact step_preserves_certificates ih step

theorem quorum_unique (s : State) (t a b : Nat) (qa : quorum s t a) (qb : quorum s t b) : a = b := by
  rcases qa with qa | qa | qa <;> rcases qb with qb | qb | qb <;> simp_all

theorem reachable_election_safety {s : State} (h : Reachable s) :
    ∀ t a b, s.elected t a → s.elected t b → a = b := by
  intro t a b ha hb
  exact quorum_unique s t a b (reachable_certified h t a ha) (reachable_certified h t b hb)

def electedOne : State := elect (grant (grant initial 1 1 7) 1 2 7) 1 7

theorem positive_election_reachable : Reachable electedOne := by
  apply Reachable.next (s := grant (grant initial 1 1 7) 1 2 7)
  · apply Reachable.next (s := grant initial 1 1 7)
    · exact Reachable.next Reachable.start (Step.vote initial 1 1 7 (Or.inl rfl) rfl)
    · apply Step.vote _ 1 2 7 (Or.inr (Or.inl rfl)); rfl
  · apply Step.publish; exact Or.inl ⟨rfl, rfl⟩

theorem positive_has_leader : electedOne.elected 1 7 := Or.inl ⟨rfl, rfl⟩

theorem two_leaders_unreachable (s : State) (a b : Nat) (different : a ≠ b)
    (ha : s.elected 1 a) (hb : s.elected 1 b) : ¬ Reachable s := by
  intro h
  exact different (reachable_election_safety h 1 a b ha hb)
end Nucleus.Spec.Quorum
