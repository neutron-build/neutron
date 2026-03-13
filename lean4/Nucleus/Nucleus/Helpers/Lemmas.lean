/-
  Shared Lemmas — reusable across multiple proof modules.
-/

namespace Nucleus.Helpers

/-- A filtered list is a sublist of the original. -/
theorem filter_subset {α : Type} (l : List α) (p : α → Bool) :
    ∀ x, x ∈ l.filter p → x ∈ l := by
  intro x hx
  exact List.mem_of_mem_filter hx

/-- Filtering preserves list membership for elements satisfying the predicate. -/
theorem mem_filter_of_mem {α : Type} (l : List α) (p : α → Bool) (x : α)
    (h_mem : x ∈ l) (h_p : p x = true) :
    x ∈ l.filter p := by
  exact List.mem_filter.mpr ⟨h_mem, h_p⟩

/-- Length of filtered list is at most the length of the original. -/
theorem filter_length_le {α : Type} (l : List α) (p : α → Bool) :
    (l.filter p).length ≤ l.length := by
  exact List.length_filter_le p l

end Nucleus.Helpers
