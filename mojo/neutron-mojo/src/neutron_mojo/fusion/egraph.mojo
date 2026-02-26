# ===----------------------------------------------------------------------=== #
# Neutron Mojo — E-Graph Data Structure
# ===----------------------------------------------------------------------=== #

"""E-Graph (Equality Graph) for equality saturation.

An e-graph efficiently represents equivalence classes of expressions. It combines:
- E-nodes: operations with their inputs (canonicalized to e-class IDs)
- E-classes: equivalence classes of nodes that compute the same value
- Hash-consing: deduplication of structurally identical nodes
- Union-find: efficient merging and lookup of equivalence classes

This is the core data structure for the rewrite engine.
"""

from collections import List, Dict, Optional
from .graph import OpKind, ValueId, ENode
from .eclass import ClassId, UnionFind, EClass

# ===----------------------------------------------------------------------=== #
# CanonicalNode — Canonicalized e-node with ClassId inputs
# ===----------------------------------------------------------------------=== #

struct CanonicalNode(Writable, Copyable, Movable):
    """E-node with inputs canonicalized to e-class IDs.

    Before adding a node to the e-graph, we canonicalize its inputs by
    replacing ValueId references with their canonical ClassId representatives.
    This enables hash-consing: structurally identical nodes map to the same
    canonical form.
    """
    var op: OpKind
    var inputs: List[ClassId]

    fn __init__(out self, op: OpKind):
        self.op = op
        self.inputs = List[ClassId]()

    fn __init__(out self, op: OpKind, input0: ClassId):
        self.op = op
        self.inputs = List[ClassId]()
        self.inputs.append(input0)

    fn __init__(out self, op: OpKind, input0: ClassId, input1: ClassId):
        self.op = op
        self.inputs = List[ClassId]()
        self.inputs.append(input0)
        self.inputs.append(input1)

    fn __copyinit__(out self, existing: Self):
        self.op = existing.op
        self.inputs = existing.inputs.copy()

    fn copy(self) -> CanonicalNode:
        """Return a deep copy of this canonical node."""
        var cn = CanonicalNode(self.op)
        cn.inputs = self.inputs.copy()
        return cn^

    fn hash(self) -> Int:
        """Compute hash for hash-consing.

        Simple hash combining op value and input class IDs.
        """
        var h = self.op._value
        for i in range(len(self.inputs)):
            h = h * 31 + self.inputs[i].id()
        return h

    fn __eq__(self, other: CanonicalNode) -> Bool:
        """Check structural equality for hash-consing."""
        if self.op != other.op:
            return False
        if len(self.inputs) != len(other.inputs):
            return False
        for i in range(len(self.inputs)):
            if self.inputs[i] != other.inputs[i]:
                return False
        return True

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(String(self.op))
        writer.write("(")
        for i in range(len(self.inputs)):
            if i > 0:
                writer.write(", ")
            writer.write(String(self.inputs[i]))
        writer.write(")")


# ===----------------------------------------------------------------------=== #
# EGraph — Equality graph with hash-consing
# ===----------------------------------------------------------------------=== #

struct EGraph:
    """E-graph: compact representation of expression equivalence classes.

    Maintains:
    - `nodes`: All canonical nodes in the graph
    - `classes`: Equivalence classes of nodes
    - `unionfind`: Union-find for efficient class merging
    - `hashcons`: Map from canonical node to its e-class (for deduplication)

    Key invariant: All nodes in the same e-class compute the same value.
    """
    var nodes: List[CanonicalNode]
    var classes: List[EClass]
    var unionfind: UnionFind
    var _hash_buckets: List[List[Int]]  # hash % 256 -> list of node indices
    var _num_buckets: Int

    fn __init__(out self):
        self.nodes = List[CanonicalNode]()
        self.classes = List[EClass]()
        self.unionfind = UnionFind()
        self._num_buckets = 256
        self._hash_buckets = List[List[Int]]()
        for _ in range(256):
            self._hash_buckets.append(List[Int]())

    fn add(mut self, var node: CanonicalNode) -> ClassId:
        """Add a canonical node to the e-graph.

        Hash-consing: if a structurally identical node already exists,
        return its e-class instead of creating a new one. Uses bucket-based
        hash lookup for O(1) amortized performance.

        Args:
            node: The canonical node to add.

        Returns:
            The ClassId of the e-class containing this node.
        """
        # O(1) amortized hash-consing via bucket lookup
        var h = node.hash()
        var bucket = (h & 0x7FFFFFFF) & (self._num_buckets - 1)

        # Check bucket for existing match
        for i in range(len(self._hash_buckets[bucket])):
            var idx = self._hash_buckets[bucket][i]
            if self.nodes[idx] == node:
                var existing_class_id = self.classes[idx].id
                return self.unionfind.find(existing_class_id)

        # Node is new, create a fresh e-class
        var class_id = self.unionfind.make_set()
        var node_idx = len(self.nodes)

        self.nodes.append(node^)
        var eclass = EClass(class_id)
        eclass.add_node(node_idx)
        self.classes.append(eclass^)

        # Add to hash bucket
        self._hash_buckets[bucket].append(node_idx)

        return class_id

    fn merge(mut self, id1: ClassId, id2: ClassId) -> ClassId:
        """Merge two e-classes.

        After merging, all nodes in both classes are equivalent.

        Args:
            id1: First e-class.
            id2: Second e-class.

        Returns:
            The canonical ClassId of the merged class.
        """
        return self.unionfind.merge(id1, id2)

    fn find(mut self, id: ClassId) -> ClassId:
        """Find the canonical representative of an e-class.

        Args:
            id: The ClassId to look up.

        Returns:
            The canonical ClassId.
        """
        return self.unionfind.find(id)

    fn canonicalize(mut self, var node: CanonicalNode) -> CanonicalNode:
        """Canonicalize a node by replacing input classes with their canonical representatives.

        This is essential after merging: input references may point to stale
        e-class IDs, so we must canonicalize them.

        Args:
            node: The node to canonicalize.

        Returns:
            A new CanonicalNode with canonicalized inputs.
        """
        var canonical = CanonicalNode(node.op)
        for i in range(len(node.inputs)):
            var canonical_input = self.find(node.inputs[i])
            canonical.inputs.append(canonical_input)
        return canonical^

    fn num_classes(self) -> Int:
        """Return the total number of e-classes (including merged ones)."""
        return self.unionfind.size()

    fn num_nodes(self) -> Int:
        """Return the total number of e-nodes in the graph."""
        return len(self.nodes)
