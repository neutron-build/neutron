/-
  Sliding Window Counter — Aeneas-translated model of the rate limiter.

  Models the sliding window algorithm from `rs/crates/neutron/src/rate_limit.rs`.
  estimated_count = prev_count * (1 - elapsed/window) + current_count
-/

namespace Nucleus.Structures

/-- A sliding window counter for rate limiting. -/
structure SlidingWindow where
  maxRequests : Nat
  windowSize : Nat          -- in ticks
  currentCount : Nat
  previousCount : Nat
  windowOffset : Nat        -- ticks elapsed in current window
  deriving Repr, BEq

/-- Create a new sliding window counter. -/
def SlidingWindow.new (maxReqs windowSz : Nat) : SlidingWindow :=
  { maxRequests := maxReqs, windowSize := windowSz.max 1,
    currentCount := 0, previousCount := 0, windowOffset := 0 }

/-- Estimated request count using weighted interpolation. -/
def SlidingWindow.estimatedCount (sw : SlidingWindow) : Nat :=
  sw.previousCount * (sw.windowSize - sw.windowOffset) / sw.windowSize + sw.currentCount

/-- Check if a request should be allowed. -/
def SlidingWindow.isAllowed (sw : SlidingWindow) : Bool :=
  sw.estimatedCount < sw.maxRequests

/-- Record a request (increment current window count). -/
def SlidingWindow.recordRequest (sw : SlidingWindow) : SlidingWindow :=
  { sw with currentCount := sw.currentCount + 1 }

/-- Advance time by one tick. -/
def SlidingWindow.tick (sw : SlidingWindow) : SlidingWindow :=
  if sw.windowOffset + 1 ≥ sw.windowSize then
    -- Window rolls over
    { sw with previousCount := sw.currentCount,
              currentCount := 0,
              windowOffset := 0 }
  else
    { sw with windowOffset := sw.windowOffset + 1 }

end Nucleus.Structures
