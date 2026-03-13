/-
  B-Tree — Aeneas-translated model of Nucleus's B-tree implementation.
  Models the core types and operations from `nucleus/src/storage/btree.rs`.
-/

namespace Nucleus.Aeneas

/-- B-tree branching factor. -/
def B : Nat := 128

/-- A key-value pair stored in the B-tree. -/
structure Entry (α : Type) where
  key : Nat
  value : α
  deriving Repr, BEq

/-- A B-tree node: either a leaf or an internal node. -/
inductive BTree (α : Type) where
  | leaf (entries : List (Entry α))
  | internal (keys : List Nat) (children : List (BTree α))
  deriving Repr

/-- Get a value by key from the B-tree. -/
def BTree.get (tree : BTree α) (k : Nat) : Option α :=
  match tree with
  | .leaf entries =>
    match entries.find? (fun e => e.key == k) with
    | some e => some e.value
    | none => none
  | .internal keys children =>
    let idx := keys.length -- simplified: find correct child
    -- In production, binary search for the correct child index
    match children.get? (min idx (children.length - 1)) with
    | some child => child.get k
    | none => none

/-- Insert a key-value pair into the B-tree (simplified model). -/
def BTree.insert (tree : BTree α) (k : Nat) (v : α) : BTree α :=
  match tree with
  | .leaf entries =>
    let newEntries := { key := k, value := v } :: entries.filter (fun e => e.key != k)
    .leaf newEntries
  | .internal keys children =>
    -- Simplified: insert into first child
    match children with
    | [] => .leaf [{ key := k, value := v }]
    | c :: cs => .internal keys (c.insert k v :: cs)

/-- Delete a key from the B-tree (simplified model). -/
def BTree.delete (tree : BTree α) (k : Nat) : BTree α :=
  match tree with
  | .leaf entries => .leaf (entries.filter (fun e => e.key != k))
  | .internal keys children =>
    .internal keys (children.map (fun c => c.delete k))

/-- Count all entries in the B-tree. -/
def BTree.size : BTree α → Nat
  | .leaf entries => entries.length
  | .internal _ children => children.foldl (fun acc c => acc + c.size) 0

/-- Get the depth of the B-tree. -/
def BTree.depth : BTree α → Nat
  | .leaf _ => 0
  | .internal _ children =>
    match children.head? with
    | some c => 1 + c.depth
    | none => 1

end Nucleus.Aeneas
