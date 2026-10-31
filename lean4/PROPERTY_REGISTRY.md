# Model property registry

These propositions concern hand-written Lean models, without Rust extraction or
implementation linkage. Build and axiom audit results must come from an actual
pinned Lean invocation. Theorem counts are inventory, not coverage scores.

| Model | Proposition / theorem | Assumptions and exclusions |
|---|---|---|
| First-child unordered BTree abstraction | `(tree.insert k v).get k = some v` / `Nucleus.Spec.insert_get` | Arbitrary abstract tree; excludes ordered routing, splitting and production B-tree well-formedness |
| Functional map | `mapInsert map k v k = some v` / `map_insert_get`; unrelated query preserved / `map_insert_unrelated` | Query differs from inserted key; function map abstraction |
| Tree shape | `LeavesAtDepth` recursively includes every descendant | Definition, not an insertion-preservation theorem |
| Recovered record database | `replay (replay state records) records = replay state records` / `replay_idempotent` | LSN-keyed record deduplication; excludes table materialization and transactional commit filtering; conflicting duplicate LSN uses first matching record |
| Recovery sequence | `wal.recoveryRecords.Sublist wal.records` / `recovery_order_subsequence` | Preserves original sequence order; does not establish sortedness for unsorted input |
| Raft pre-leadership subset | Three-node initializer; reachable states retain three nodes and election safety / `preleadership_reachable_nonempty`, `preleadership_reachable_election_safety` | Election-start and higher-term receipt only; excludes leader creation, quorum election, replication and crash recovery; not complete Raft assurance |

Existing crypto assumptions are audited by `scripts/axioms.sh`. No new axiom or
`sorry` is introduced for these properties. The stronger protocol election model
lives in Quint and does not itself prove a refinement to these Lean definitions.

Source additions awaiting the pinned checker (no checked status yet):

| Model | Proposition / source | Assumptions and execution gate |
|---|---|---|
| Same first-child BTree | `insert_get_unrelated` | Query differs from inserted key; actual `BTree.insert/get`, still unordered. Run build, axiom audit and unrelated-lookup mutation. |
| Durable three-voter election kernel | `Quorum.step_preserves_certificates`, `reachable_election_safety`, `positive_election_reachable`, `two_leaders_unreachable` | Term-indexed durable single votes; fixed three voters, quorum intersection; actual reachable leader creation. Does not refine mutable current-term Raft. |
| Transactional row recovery | `tableMutations`, `rowRecovery`, committed/durable and last-write/delete lemmas | Payload is key plus Nat-list value; strictly ordered unique LSN and unique terminal decisions in `validRecoveryLog`. This is not production page-image materialization. |

The ordered-tree headline is still an explicit proof obligation. The current
`BTree.insert` prepends an entry: a sorted leaf with keys `[1,3]` becomes `[2,1,3]`
after insertion of `2`. A root with separators `[2]` and leaves `[1]`, `[3]` cannot
look up `3`, because `BTree.get` always selects the first child. An insertion into
a leaf already containing `B` distinct entries creates `B+1` entries without any
split. These are concrete model counterexamples to ordering, routing and occupancy
preservation; no sound theorem can establish those properties for these operations.
The required ordered algorithm must introduce separator-guided routing, sorted
leaf replacement, split propagation and recursive occupancy/depth/range invariants.
Required theorems are constructor validity, transition validity, separator/range
routing correctness, split content conservation and all-leaf depth preservation.
The same-operation unordered preservation theorem above does not close them.

For recovery, the next stronger obligation is replay equivalence to an ordered
transactional table transition fold under `validRecoveryLog`, including a unique
terminal decision and no reuse of transaction IDs. Conflicting duplicate LSNs with
different row payloads are a counterexample to accepting arbitrary logs as the same
history. The production trace adapter in Quint tests actual WAL bytes/decisions;
it does not establish this table-model refinement.

The source now also contains a separate `Ordered.Tree` algorithm: sorted leaf
replacement, separator-guided lookup, leaf splits, recursive internal split
propagation and root growth, with a recursive `Ordered.WellFormed` invariant.
`empty_valid` is a candidate constructor proof. `TransitionObligation`,
`LookupObligation` and `PreservationObligation` are explicitly unproved Prop
definitions; they are not audited theorem claims. The bounded independent Python
reference exercises all insertion permutations of seven keys, split propagation
and replacement against a map oracle, but cannot certify the Lean algorithm.
Completing those three inductive proofs and split content conservation remains
mandatory after real Lean elaboration.
