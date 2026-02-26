# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Autograd Tape (Reverse-mode AD)
# ===----------------------------------------------------------------------=== #

"""Flat-storage autograd tape for reverse-mode automatic differentiation.

Design: Since Tensor is Movable-only (can't use List[Tensor]), the Tape stores
ALL variable data in one flat Tensor with offset indexing. Same pattern as
Model.layer_weights with LayerWeightOffsets.

Each variable gets a contiguous slice: data_flat[offset..offset+size].
Gradients are stored in a parallel grad_flat tensor with the same layout.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from .variable import Variable


# ===----------------------------------------------------------------------=== #
# Op codes for backward dispatch
# ===----------------------------------------------------------------------=== #

# Unary ops
fn OP_ADD() -> Int:
    return 0

fn OP_MUL() -> Int:
    return 1

fn OP_MATMUL() -> Int:
    return 2

fn OP_RELU() -> Int:
    return 3

fn OP_SIGMOID() -> Int:
    return 4

fn OP_TANH() -> Int:
    return 5

fn OP_EXP() -> Int:
    return 6

fn OP_LOG() -> Int:
    return 7

fn OP_SOFTMAX() -> Int:
    return 8

fn OP_SUM() -> Int:
    return 9

fn OP_MEAN() -> Int:
    return 10

fn OP_SUB() -> Int:
    return 11

fn OP_DIV() -> Int:
    return 12

fn OP_POW() -> Int:
    return 13

fn OP_SQRT() -> Int:
    return 14

fn OP_NEG() -> Int:
    return 15

fn OP_CLAMP() -> Int:
    return 16

fn OP_SCALAR_MUL() -> Int:
    return 17

fn OP_RMSNORM() -> Int:
    return 18

fn OP_LAYERNORM() -> Int:
    return 19

fn OP_GELU() -> Int:
    return 20

fn OP_SILU() -> Int:
    return 21

fn OP_SWIGLU() -> Int:
    return 22

fn OP_RESHAPE() -> Int:
    return 23

fn OP_TRANSPOSE() -> Int:
    return 24

fn OP_CONCAT() -> Int:
    return 25

fn OP_SPLIT() -> Int:
    return 26

fn OP_LOG_SOFTMAX() -> Int:
    return 27

fn OP_CROSS_ENTROPY() -> Int:
    return 28

fn OP_MSE() -> Int:
    return 29

fn OP_EMBEDDING() -> Int:
    return 30

fn OP_SCALAR_ADD() -> Int:
    return 31

fn OP_L1() -> Int:
    return 32

fn OP_BCE() -> Int:
    return 33

fn OP_KL_DIV() -> Int:
    return 34


# ===----------------------------------------------------------------------=== #
# TapeEntry — records one operation
# ===----------------------------------------------------------------------=== #


struct TapeEntry(Copyable, Movable):
    """A single recorded operation on the tape.

    Fields:
        op_kind: Operation code (OP_ADD, OP_MUL, etc.)
        input0_idx: First input variable index (-1 if none)
        input1_idx: Second input variable index (-1 if none)
        output_idx: Output variable index
        cached_scalar: Cached scalar value (e.g., exponent for pow)
        cached_scalar2: Second cached scalar (e.g., max_val for clamp)
        cached_int: Cached integer (e.g., axis for sum, dim for matmul)
        cached_int2: Second cached integer (e.g., M for matmul)
        cached_int3: Third cached integer (e.g., K for matmul)
    """

    var op_kind: Int
    var input0_idx: Int
    var input1_idx: Int
    var output_idx: Int
    var cached_scalar: Float64
    var cached_scalar2: Float64
    var cached_int: Int
    var cached_int2: Int
    var cached_int3: Int

    fn __init__(out self,
        op_kind: Int,
        input0_idx: Int,
        input1_idx: Int,
        output_idx: Int,
        cached_scalar: Float64 = 0.0,
        cached_scalar2: Float64 = 0.0,
        cached_int: Int = 0,
        cached_int2: Int = 0,
        cached_int3: Int = 0,
    ):
        self.op_kind = op_kind
        self.input0_idx = input0_idx
        self.input1_idx = input1_idx
        self.output_idx = output_idx
        self.cached_scalar = cached_scalar
        self.cached_scalar2 = cached_scalar2
        self.cached_int = cached_int
        self.cached_int2 = cached_int2
        self.cached_int3 = cached_int3

    fn __copyinit__(out self, other: Self):
        self.op_kind = other.op_kind
        self.input0_idx = other.input0_idx
        self.input1_idx = other.input1_idx
        self.output_idx = other.output_idx
        self.cached_scalar = other.cached_scalar
        self.cached_scalar2 = other.cached_scalar2
        self.cached_int = other.cached_int
        self.cached_int2 = other.cached_int2
        self.cached_int3 = other.cached_int3

    fn __moveinit__(out self, deinit other: Self):
        self.op_kind = other.op_kind
        self.input0_idx = other.input0_idx
        self.input1_idx = other.input1_idx
        self.output_idx = other.output_idx
        self.cached_scalar = other.cached_scalar
        self.cached_scalar2 = other.cached_scalar2
        self.cached_int = other.cached_int
        self.cached_int2 = other.cached_int2
        self.cached_int3 = other.cached_int3

    fn copy(self) -> TapeEntry:
        return TapeEntry(
            self.op_kind, self.input0_idx, self.input1_idx, self.output_idx,
            self.cached_scalar, self.cached_scalar2,
            self.cached_int, self.cached_int2, self.cached_int3,
        )


# ===----------------------------------------------------------------------=== #
# Tape — flat storage for all variables + operation log
# ===----------------------------------------------------------------------=== #


struct Tape(Movable):
    """Autograd tape with flat tensor storage for variables and gradients.

    All variable data is stored in `data_flat` and `grad_flat` tensors.
    Per-variable metadata (offset, size, shape) is in parallel lists.

    Usage:
        var tape = Tape(initial_capacity=1024)
        var x_idx = tape.add_variable(Shape(3, 4), requires_grad=True)
        # ... set data, run tracked ops ...
        backward(tape, loss_idx)
        var grad = tape.get_grad_copy(x_idx)
    """

    var data_flat: Tensor[DType.float32]
    var grad_flat: Tensor[DType.float32]
    var var_offsets: List[Int]
    var var_sizes: List[Int]
    var var_shapes: List[List[Int]]
    var var_requires_grad: List[Bool]
    var entries: List[TapeEntry]
    var total_used: Int
    var capacity: Int

    fn __init__(out self, initial_capacity: Int = 65536):
        """Create a tape with the given initial flat capacity."""
        self.capacity = initial_capacity
        self.data_flat = Tensor[DType.float32](initial_capacity)
        self.grad_flat = Tensor[DType.float32](initial_capacity)
        self.var_offsets = List[Int]()
        self.var_sizes = List[Int]()
        self.var_shapes = List[List[Int]]()
        self.var_requires_grad = List[Bool]()
        self.entries = List[TapeEntry]()
        self.total_used = 0

    fn __moveinit__(out self, deinit other: Self):
        self.data_flat = other.data_flat^
        self.grad_flat = other.grad_flat^
        self.var_offsets = other.var_offsets^
        self.var_sizes = other.var_sizes^
        self.var_shapes = other.var_shapes^
        self.var_requires_grad = other.var_requires_grad^
        self.entries = other.entries^
        self.total_used = other.total_used
        self.capacity = other.capacity

    fn _ensure_capacity(mut self, needed: Int):
        """Grow flat tensors if needed."""
        if self.total_used + needed <= self.capacity:
            return
        var new_cap = self.capacity
        while new_cap < self.total_used + needed:
            new_cap *= 2

        var new_data = Tensor[DType.float32](new_cap)
        var new_grad = Tensor[DType.float32](new_cap)
        # Use .get()/.set() instead of data_ptr() to avoid Mojo 0.26.2
        # aliasing bug where data_ptr() on mut struct field returns
        # pointer to temporary copy.
        for i in range(self.total_used):
            new_data.set(i, self.data_flat.get(i))
            new_grad.set(i, self.grad_flat.get(i))
        self.data_flat = new_data^
        self.grad_flat = new_grad^
        self.capacity = new_cap

    fn add_variable(mut self, shape_dims: List[Int], requires_grad: Bool = True) -> Int:
        """Add a new variable to the tape. Returns its index."""
        var numel = 1
        for i in range(len(shape_dims)):
            numel *= shape_dims[i]

        self._ensure_capacity(numel)

        var idx = len(self.var_offsets)
        self.var_offsets.append(self.total_used)
        self.var_sizes.append(numel)
        var dims_copy = List[Int]()
        for i in range(len(shape_dims)):
            dims_copy.append(shape_dims[i])
        self.var_shapes.append(dims_copy^)
        self.var_requires_grad.append(requires_grad)
        self.total_used += numel
        return idx

    fn add_variable_from_shape(mut self, shape: Shape, requires_grad: Bool = True) -> Int:
        """Add a variable using a Shape object."""
        var dims = List[Int]()
        for i in range(shape.ndim()):
            dims.append(shape[i])
        return self.add_variable(dims^, requires_grad)

    fn num_variables(self) -> Int:
        """Return the number of variables on the tape."""
        return len(self.var_offsets)

    fn var_numel(self, var_idx: Int) -> Int:
        """Return the number of elements for a variable."""
        return self.var_sizes[var_idx]

    fn var_offset(self, var_idx: Int) -> Int:
        """Return the flat offset for a variable."""
        return self.var_offsets[var_idx]

    fn get_data(self, var_idx: Int, elem_idx: Int) -> Float32:
        """Get a single data element from a variable."""
        return self.data_flat.get(self.var_offsets[var_idx] + elem_idx)

    fn set_data(mut self, var_idx: Int, elem_idx: Int, value: Float32):
        """Set a single data element in a variable."""
        self.data_flat.set(self.var_offsets[var_idx] + elem_idx, value)

    fn get_grad(self, var_idx: Int, elem_idx: Int) -> Float32:
        """Get a single gradient element from a variable."""
        return self.grad_flat.get(self.var_offsets[var_idx] + elem_idx)

    fn set_grad(mut self, var_idx: Int, elem_idx: Int, value: Float32):
        """Set a single gradient element in a variable."""
        self.grad_flat.set(self.var_offsets[var_idx] + elem_idx, value)

    fn accumulate_grad(mut self, var_idx: Int, elem_idx: Int, value: Float32):
        """Add to a gradient element (accumulate). Skips if requires_grad is False."""
        if not self.var_requires_grad[var_idx]:
            return
        var offset = self.var_offsets[var_idx] + elem_idx
        var current = self.grad_flat.get(offset)
        self.grad_flat.set(offset, current + value)

    fn get_data_copy(self, var_idx: Int) -> Tensor[DType.float32]:
        """Copy a variable's data into a new tensor."""
        var n = self.var_sizes[var_idx]
        var offset = self.var_offsets[var_idx]
        var result = Tensor[DType.float32](n)
        for i in range(n):
            result.set(i, self.data_flat.get(offset + i))
        return result^

    fn get_grad_copy(self, var_idx: Int) -> Tensor[DType.float32]:
        """Copy a variable's gradient into a new tensor."""
        var n = self.var_sizes[var_idx]
        var offset = self.var_offsets[var_idx]
        var result = Tensor[DType.float32](n)
        for i in range(n):
            result.set(i, self.grad_flat.get(offset + i))
        return result^

    fn set_data_from_tensor(mut self, var_idx: Int, tensor: Tensor[DType.float32]):
        """Copy tensor data into a variable's data slot."""
        var n = self.var_sizes[var_idx]
        var offset = self.var_offsets[var_idx]
        for i in range(n):
            self.data_flat.set(offset + i, tensor.get(i))

    fn record(mut self, entry: TapeEntry):
        """Record an operation on the tape."""
        self.entries.append(entry.copy())

    fn zero_all_grads(mut self):
        """Zero all gradients."""
        for i in range(self.total_used):
            self.grad_flat.set(i, Float32(0.0))

    fn num_entries(self) -> Int:
        """Return the number of recorded operations."""
        return len(self.entries)

    fn get_entry(self, idx: Int) -> TapeEntry:
        """Get a tape entry by index."""
        return self.entries[idx].copy()

    fn make_variable(self, var_idx: Int) -> Variable:
        """Create a Variable handle for the given tape index."""
        var dims = List[Int]()
        var shape = self.var_shapes[var_idx].copy()
        for i in range(len(shape)):
            dims.append(shape[i])
        return Variable(var_idx, self.var_requires_grad[var_idx], dims^, self.var_sizes[var_idx])
