/-
  LRU Cache — Aeneas-translated model of the L1 cache.

  Models the core logic from `rs/crates/neutron-cache/src/l1.rs`.
  Fixed-capacity cache with LRU eviction.
-/

namespace Nucleus.Structures

/-- A cache entry with a key and value. -/
structure CacheEntry (α : Type) where
  key : String
  value : α
  accessTime : Nat   -- logical timestamp of last access
  deriving Repr, BEq

/-- An LRU cache with bounded capacity. -/
structure LruCache (α : Type) where
  entries : List (CacheEntry α)
  capacity : Nat
  clock : Nat         -- logical clock for access ordering
  deriving Repr

/-- Create an empty LRU cache with given capacity. -/
def LruCache.new (cap : Nat) : LruCache α :=
  { entries := [], capacity := cap.max 1, clock := 0 }

/-- Get a value by key, updating access time. -/
def LruCache.get (cache : LruCache α) (k : String) :
    Option α × LruCache α :=
  match cache.entries.find? (fun e => e.key == k) with
  | some entry =>
    let newEntries := cache.entries.map fun e =>
      if e.key == k then { e with accessTime := cache.clock + 1 }
      else e
    (some entry.value, { cache with entries := newEntries, clock := cache.clock + 1 })
  | none => (none, cache)

/-- Find the index of the least recently used entry. -/
def findLru (entries : List (CacheEntry α)) : Option Nat :=
  if entries.isEmpty then none
  else
    let minTime := entries.foldl (fun acc e => min acc e.accessTime) entries.head!.accessTime
    entries.findIdx? (fun e => e.accessTime == minTime)

/-- Insert a key-value pair, evicting LRU entry if at capacity. -/
def LruCache.set (cache : LruCache α) (k : String) (v : α) : LruCache α :=
  let newClock := cache.clock + 1
  let entry := { key := k, value := v, accessTime := newClock : CacheEntry α }
  -- Check if key already exists (overwrite)
  if cache.entries.any (fun e => e.key == k) then
    let newEntries := cache.entries.map fun e =>
      if e.key == k then entry else e
    { cache with entries := newEntries, clock := newClock }
  -- At capacity: evict LRU
  else if cache.entries.length ≥ cache.capacity then
    match findLru cache.entries with
    | some idx =>
      let newEntries := (cache.entries.eraseIdx idx).concat entry
      { entries := newEntries, capacity := cache.capacity, clock := newClock }
    | none =>
      { cache with entries := [entry], clock := newClock }
  -- Under capacity: just append
  else
    { cache with entries := cache.entries.concat entry, clock := newClock }

/-- Delete a key from the cache. -/
def LruCache.del (cache : LruCache α) (k : String) : LruCache α :=
  { cache with entries := cache.entries.filter (fun e => e.key != k) }

/-- Number of entries in the cache. -/
def LruCache.size (cache : LruCache α) : Nat :=
  cache.entries.length

end Nucleus.Structures
