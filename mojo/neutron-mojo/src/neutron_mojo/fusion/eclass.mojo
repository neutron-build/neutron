# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Equivalence Class and Union-Find
# ===----------------------------------------------------------------------=== #

"""Union-find data structure for e-graph equality saturation.

Implements path-compressed union-find to efficiently manage equivalence classes
of graph nodes. This is the core data structure for the e-graph rewrite engine.
"""

from std.collections import List, Optional

# ===----------------------------------------------------------------------=== #
# ClassId — Reference to an equivalence class
# ===----------------------------------------------------------------------=== #

struct ClassId(Writable, TrivialRegisterPassable):
    """Reference to an equivalence class."""
    var _id: Int

    def __init__(out self, id: Int):
        self._id = id

    def __eq__(self, other: ClassId) -> Bool:
        return self._id == other._id

    def __ne__(self, other: ClassId) -> Bool:
        return self._id != other._id

    def id(self) -> Int:
        return self._id

    def write_to(self, mut writer: Some[Writer]):
        writer.write("c")
        writer.write(String(self._id))


# ===----------------------------------------------------------------------=== #
# UnionFind — Path-compressed union-find data structure
# ===----------------------------------------------------------------------=== #

struct UnionFind:
    """Union-find data structure with path compression.

    Each element maintains a parent pointer. Initially, each element is its own
    parent (singleton set). The `find` operation returns the canonical
    representative of the equivalence class, compressing paths as it goes.
    The `merge` operation unions two classes.
    """
    var _parent: List[Int]
    var _rank: List[Int]  # For union by rank optimization

    def __init__(out self):
        self._parent = List[Int]()
        self._rank = List[Int]()

    def make_set(mut self) -> ClassId:
        """Create a new singleton equivalence class.

        Returns the ClassId of the new class.
        """
        var id = len(self._parent)
        self._parent.append(id)  # Parent is itself
        self._rank.append(0)
        return ClassId(id)

    def find(mut self, id: ClassId) -> ClassId:
        """Find the canonical representative of the equivalence class.

        Applies path compression: all nodes on the path to the root are
        updated to point directly to the root.

        Args:
            id: The ClassId to look up.

        Returns:
            The canonical ClassId for this equivalence class.
        """
        var idx = id.id()
        if self._parent[idx] != idx:
            # Path compression: recursively find root and update parent
            self._parent[idx] = self.find(ClassId(self._parent[idx])).id()
        return ClassId(self._parent[idx])

    def merge(mut self, id1: ClassId, id2: ClassId) -> ClassId:
        """Merge two equivalence classes.

        Uses union by rank to keep trees balanced. Returns the canonical
        representative of the merged class.

        Args:
            id1: First ClassId.
            id2: Second ClassId.

        Returns:
            The canonical ClassId of the merged class.
        """
        var root1 = self.find(id1)
        var root2 = self.find(id2)

        # Already in the same class
        if root1 == root2:
            return root1

        var r1 = root1.id()
        var r2 = root2.id()

        # Union by rank: attach smaller tree under larger tree
        if self._rank[r1] < self._rank[r2]:
            self._parent[r1] = r2
            return root2
        elif self._rank[r1] > self._rank[r2]:
            self._parent[r2] = r1
            return root1
        else:
            # Equal rank: attach r2 under r1 and increment r1's rank
            self._parent[r2] = r1
            self._rank[r1] += 1
            return root1

    def in_same_class(mut self, id1: ClassId, id2: ClassId) -> Bool:
        """Check if two ClassIds are in the same equivalence class.

        Args:
            id1: First ClassId.
            id2: Second ClassId.

        Returns:
            True if both are in the same equivalence class.
        """
        return self.find(id1) == self.find(id2)

    def size(self) -> Int:
        """Return the number of equivalence classes (including merged ones).

        Note: This is the total number of make_set calls, not the number of
        distinct equivalence classes after merges.
        """
        return len(self._parent)


# ===----------------------------------------------------------------------=== #
# EClass — Equivalence class of graph nodes
# ===----------------------------------------------------------------------=== #

struct EClass(Copyable, Movable, ImplicitlyCopyable):
    """Equivalence class of graph nodes.

    An e-class represents a set of e-nodes that are known to be equivalent
    (produce the same value). The e-graph maintains the invariant that all
    nodes in the same e-class compute the same result.
    """
    var id: ClassId
    var nodes: List[Int]  # Indices into the global node list

    def __init__(out self, id: ClassId):
        self.id = id
        self.nodes = List[Int]()

    def __init__(out self, *, copy: Self):
        self.id = copy.id
        self.nodes = copy.nodes.copy()

    def copy(self) -> EClass:
        """Return a deep copy of this e-class."""
        var ec = EClass(self.id)
        ec.nodes = self.nodes.copy()
        return ec^

    def add_node(mut self, node_idx: Int):
        """Add a node to this equivalence class.

        Args:
            node_idx: Index of the node in the global node list.
        """
        self.nodes.append(node_idx)

    def size(self) -> Int:
        """Return the number of nodes in this e-class."""
        return len(self.nodes)
