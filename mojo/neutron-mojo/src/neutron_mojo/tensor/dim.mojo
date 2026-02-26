# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Named tensor dimensions
# ===----------------------------------------------------------------------=== #

"""Parametric named dimensions for type-level tensor shape documentation.

`Dim[name]` is a compile-time tag with a runtime `size` field.
"""


# ===----------------------------------------------------------------------=== #
# Dim — parametric named dimension
# ===----------------------------------------------------------------------=== #


struct Dim[name: StringLiteral](Writable, Copyable, Movable):
    """A named tensor dimension with a runtime size.

    The `name` parameter is a compile-time StringLiteral tag for documentation
    and function signature clarity. The `size` field holds the runtime extent.
    """

    var size: Int

    fn __init__(out self, size: Int):
        """Create a dimension with a runtime size."""
        self.size = size

    @staticmethod
    fn static_dim[S: Int]() -> Dim[name]:
        """Create a dimension with a compile-time-known size."""
        constrained[S > 0, "Static dimension size must be positive"]()
        return Dim[name](S)

    fn __eq__(self, other: Dim[name]) -> Bool:
        """Dimensions are equal if their sizes match."""
        return self.size == other.size

    fn __ne__(self, other: Dim[name]) -> Bool:
        """Dimensions are not equal if their sizes differ."""
        return self.size != other.size

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(name, "(", self.size, ")")

    fn __int__(self) -> Int:
        """Convert to Int (returns the size)."""
        return self.size


# ===----------------------------------------------------------------------=== #
# Common dimension aliases
# ===----------------------------------------------------------------------=== #

comptime Batch = Dim["batch"]
comptime Seq = Dim["seq"]
comptime Hidden = Dim["hidden"]
comptime Vocab = Dim["vocab"]
comptime Heads = Dim["heads"]
comptime HeadDim = Dim["head_dim"]
comptime Dynamic = Dim["dynamic"]
