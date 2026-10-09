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

from std.collections import List, Dict, Optional
from .graph import OpKind, ValueId, ENode
from .eclass import ClassId, UnionFind, EClass
from std.memory import alloc

# ===----------------------------------------------------------------------=== #
# CanonicalNode — Canonicalized e-node with ClassId inputs
# ===----------------------------------------------------------------------=== #

struct CanonicalNode(Writable, Copyable, Movable, ImplicitlyCopyable):
    """E-node with inputs canonicalized to e-class IDs.

    Before adding a node to the e-graph, we canonicalize its inputs by
    replacing ValueId references with their canonical ClassId representatives.
    This enables hash-consing: structurally identical nodes map to the same
    canonical form.
    """
    var op: OpKind
    var symbol: Int
    var has_constant: Bool
    var constant_bits: Int
    var constant_dtype: Int
    var constant_shape: List[Int]
    var inputs: List[ClassId]

    def __init__(out self, op: OpKind):
        self.symbol = -1
        self.has_constant = False
        self.constant_bits = 0
        self.constant_dtype = 32  # scalar F32
        self.constant_shape = [1]
        self.op = op
        self.inputs = List[ClassId]()

    def __init__(out self, op: OpKind, input0: ClassId):
        self.symbol = -1
        self.has_constant = False
        self.constant_bits = 0
        self.constant_dtype = 32  # scalar F32
        self.constant_shape = [1]
        self.op = op
        self.inputs = List[ClassId]()
        self.inputs.append(input0)

    def __init__(out self, op: OpKind, input0: ClassId, input1: ClassId):
        self.symbol = -1
        self.has_constant = False
        self.constant_bits = 0
        self.constant_dtype = 32  # scalar F32
        self.constant_shape = [1]
        self.op = op
        self.inputs = List[ClassId]()
        self.inputs.append(input0)
        self.inputs.append(input1)

    def __init__(out self, *, copy: Self):
        self.symbol = copy.symbol
        self.has_constant = copy.has_constant
        self.constant_bits = copy.constant_bits
        self.constant_dtype = copy.constant_dtype
        self.constant_shape = copy.constant_shape.copy()
        self.op = copy.op
        self.inputs = copy.inputs.copy()

    def copy(self) -> CanonicalNode:
        """Return a deep copy of this canonical node."""
        var cn = CanonicalNode(self.op)
        cn.symbol = self.symbol
        cn.has_constant = self.has_constant
        cn.constant_bits = self.constant_bits
        cn.constant_dtype = self.constant_dtype
        cn.constant_shape = self.constant_shape.copy()
        cn.inputs = self.inputs.copy()
        return cn^

    def hash(self) -> Int:
        """Compute hash for hash-consing.

        Simple hash combining op value and input class IDs.
        """
        var h = self.op._value * 31 + self.symbol
        h = h * 31 + Int(self.has_constant)
        if self.has_constant:
            h = h * 31 + self.constant_bits
            h = h * 31 + self.constant_dtype
            for i in range(len(self.constant_shape)):
                h = h * 31 + self.constant_shape[i]
        for i in range(len(self.inputs)):
            h = h * 31 + self.inputs[i].id()
        return h

    def __eq__(self, other: CanonicalNode) -> Bool:
        """Check structural equality for hash-consing."""
        if self.symbol != other.symbol or self.has_constant != other.has_constant:
            return False
        if self.has_constant and (self.constant_bits != other.constant_bits or self.constant_dtype != other.constant_dtype or self.constant_shape != other.constant_shape):
            return False
        if self.op != other.op:
            return False
        if len(self.inputs) != len(other.inputs):
            return False
        for i in range(len(self.inputs)):
            if self.inputs[i] != other.inputs[i]:
                return False
        return True

    def write_to(self, mut writer: Some[Writer]):
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

    def __init__(out self):
        self.nodes = List[CanonicalNode]()
        self.classes = List[EClass]()
        self.unionfind = UnionFind()
        self._num_buckets = 256
        self._hash_buckets = List[List[Int]]()
        for _ in range(256):
            self._hash_buckets.append(List[Int]())

    def add(mut self, var node: CanonicalNode) -> ClassId:
        """Add a canonical node to the e-graph.

        Hash-consing: if a structurally identical node already exists,
        return its e-class instead of creating a new one. Uses bucket-based
        hash lookup for O(1) amortized performance.

        Args:
            node: The canonical node to add.

        Returns:
            The ClassId of the e-class containing this node.
        """
        # Opaque leaves are distinct symbols, never presumed numeric constants.
        if (node.op == OpKind.Input or node.op == OpKind.Const) and node.symbol < 0 and not node.has_constant:
            node.symbol = len(self.nodes)
        node = self.canonicalize(node^)
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

    def merge(mut self, id1: ClassId, id2: ClassId) -> ClassId:
        """Merge two e-classes.

        After merging, all nodes in both classes are equivalent.

        Args:
            id1: First e-class.
            id2: Second e-class.

        Returns:
            The canonical ClassId of the merged class.
        """
        var root = self.unionfind.merge(id1, id2)
        self.rebuild()
        return self.find(root)

    def find(mut self, id: ClassId) -> ClassId:
        """Find the canonical representative of an e-class.

        Args:
            id: The ClassId to look up.

        Returns:
            The canonical ClassId.
        """
        return self.unionfind.find(id)

    def canonicalize(mut self, var node: CanonicalNode) -> CanonicalNode:
        """Canonicalize a node by replacing input classes with their canonical representatives.

        This is essential after merging: input references may point to stale
        e-class IDs, so we must canonicalize them.

        Args:
            node: The node to canonicalize.

        Returns:
            A new CanonicalNode with canonicalized inputs.
        """
        var canonical = node.copy()
        canonical.inputs = List[ClassId]()
        for i in range(len(node.inputs)):
            var canonical_input = self.find(node.inputs[i])
            canonical.inputs.append(canonical_input)
        return canonical^

    def num_classes(self) -> Int:
        """Return the total number of e-classes (including merged ones)."""
        return self.unionfind.size()

    def num_nodes(self) -> Int:
        """Return the total number of e-nodes in the graph."""
        return len(self.nodes)


    def rebuild(mut self):
        """Rehash canonical children and merge congruent parents to a fixed point.

        Rebuilding buckets on each round is bounded by the existing node count;
        node/class storage remains stable for clients holding old ClassIds.
        """
        var changed = True
        while changed:
            changed = False
            self._hash_buckets = List[List[Int]]()
            for _ in range(self._num_buckets):
                self._hash_buckets.append(List[Int]())
            for idx in range(len(self.nodes)):
                var canonical = self.canonicalize(self.nodes[idx].copy())
                self.nodes[idx] = canonical^
                var bucket = (self.nodes[idx].hash() & 0x7FFFFFFF) & (self._num_buckets - 1)
                for i in range(len(self._hash_buckets[bucket])):
                    var other = self._hash_buckets[bucket][i]
                    if self.nodes[idx] == self.nodes[other]:
                        var a = self.find(self.classes[idx].id)
                        var b = self.find(self.classes[other].id)
                        if a != b:
                            _ = self.unionfind.merge(a,b)
                            changed = True
                self._hash_buckets[bucket].append(idx)

    def add_scalar_f32(mut self, value: Float32) raises -> ClassId:
        var ptr = alloc[Float32](1)
        ptr.unsafe_store(value)
        var bits = Int(ptr.unsafe_bitcast[UInt32]().unsafe_load())
        ptr.free()
        var node = CanonicalNode(OpKind.Const)
        node.has_constant = True
        node.constant_bits = bits
        return self.add(node^)

    def is_scalar_f32(mut self, cid: ClassId, bits: Int) -> Bool:
        var root = self.find(cid)
        for i in range(len(self.nodes)):
            if self.find(self.classes[i].id) == root:
                var node = self.nodes[i].copy()
                if node.op == OpKind.Const and node.has_constant and node.constant_dtype == 32 and node.constant_shape == [1] and node.constant_bits == bits:
                    return True
        return False
