# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd Variable
# ===----------------------------------------------------------------------=== #

"""Variable wrapper for autograd: tracks tensor data, gradients, and tape index.

A Variable is a lightweight handle into the Tape's flat storage. It stores
only metadata (indices, flags) — the actual tensor data lives in the Tape.
"""


struct Variable(Copyable, Movable):
    """A differentiable variable tracked by the autograd tape.

    Fields:
        tape_idx: Index into the tape's variable registry.
        requires_grad: Whether this variable participates in gradient computation.
        shape_dims: Flattened shape dimensions for reconstruction.
        ndim: Number of dimensions.
        numel: Total number of elements.
    """

    var tape_idx: Int
    var requires_grad: Bool
    var shape_dims: List[Int]
    var ndim: Int
    var numel: Int

    fn __init__(out self, tape_idx: Int, requires_grad: Bool, var shape_dims: List[Int], numel: Int):
        self.tape_idx = tape_idx
        self.requires_grad = requires_grad
        self.ndim = len(shape_dims)
        self.numel = numel
        self.shape_dims = shape_dims^

    fn __copyinit__(out self, other: Self):
        self.tape_idx = other.tape_idx
        self.requires_grad = other.requires_grad
        self.ndim = other.ndim
        self.numel = other.numel
        self.shape_dims = List[Int]()
        for i in range(len(other.shape_dims)):
            self.shape_dims.append(other.shape_dims[i])

    fn __moveinit__(out self, deinit other: Self):
        self.tape_idx = other.tape_idx
        self.requires_grad = other.requires_grad
        self.ndim = other.ndim
        self.numel = other.numel
        self.shape_dims = other.shape_dims^

    fn copy(self) -> Variable:
        """Explicit copy."""
        var dims = List[Int]()
        for i in range(len(self.shape_dims)):
            dims.append(self.shape_dims[i])
        return Variable(self.tape_idx, self.requires_grad, dims^, self.numel)
