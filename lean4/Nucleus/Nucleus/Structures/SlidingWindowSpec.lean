/-
  Sliding Window Counter Formal Specifications.
-/
import Nucleus.Structures.SlidingWindow

namespace Nucleus.Structures.Spec

open Nucleus.Structures

/-- Estimated count is non-negative (trivially true for Nat). -/
theorem estimated_non_negative (sw : SlidingWindow) :
    sw.estimatedCount ≥ 0 := by
  omega

/-- Recording a request increases the estimated count. -/
theorem record_increases_estimate (sw : SlidingWindow) :
    sw.recordRequest.estimatedCount ≥ sw.estimatedCount := by
  simp [SlidingWindow.recordRequest, SlidingWindow.estimatedCount]
  omega

/-- A request is rejected when estimated count reaches max. -/
theorem at_max_rejects (sw : SlidingWindow)
    (h : sw.estimatedCount ≥ sw.maxRequests) :
    sw.isAllowed = false := by
  simp [SlidingWindow.isAllowed]
  omega

/-- Fresh window allows requests (if max > 0). -/
theorem fresh_allows (maxReqs windowSz : Nat) (h : maxReqs > 0) :
    (SlidingWindow.new maxReqs windowSz).isAllowed = true := by
  simp [SlidingWindow.new, SlidingWindow.isAllowed, SlidingWindow.estimatedCount]
  omega

/-- Window offset stays within bounds after tick. -/
theorem tick_offset_bounded (sw : SlidingWindow)
    (h : sw.windowOffset < sw.windowSize) :
    sw.tick.windowOffset < sw.windowSize := by
  simp [SlidingWindow.tick]
  split
  · -- Rollover: offset becomes 0
    omega
  · -- Normal: offset increments
    omega

/-- Window rollover preserves request history. -/
theorem rollover_preserves_count (sw : SlidingWindow)
    (h : sw.windowOffset + 1 ≥ sw.windowSize) :
    sw.tick.previousCount = sw.currentCount := by
  simp [SlidingWindow.tick, h]

/-- Estimated count with zero previous is just current count. -/
theorem no_history_estimate (sw : SlidingWindow)
    (h : sw.previousCount = 0) :
    sw.estimatedCount = sw.currentCount := by
  simp [SlidingWindow.estimatedCount, h]

end Nucleus.Structures.Spec
