# MAX LayoutTensor: Architecture Study

`LayoutTensor` is MAX's core abstraction for typed, layout-aware memory access on both CPU and GPU. Defined in `max/kernels/src/layout/layout_tensor.mojo`, it combines a pointer, a compile-time layout, and runtime dimension overrides into a single type that the compiler can aggressively optimize.

## Struct Definition and Parameters

```mojo
struct LayoutTensor[
    mut: Bool,                    # Mutability
    dtype: DType,                 # Element type (float32, bfloat16, etc.)
    layout: Layout,               # Compile-time shape and stride
    origin: Origin[mut=mut],      # Lifetime/ownership tracking
    *,
    address_space: AddressSpace = AddressSpace.GENERIC,  # GPU memory type
    element_layout: Layout = Layout(1, 1),  # Sub-element layout for vectorization
    layout_int_type: DType,       # Integer type for layout math
    linear_idx_type: DType,       # Integer type for linear indexing
    masked: Bool = False,         # Whether bounds checking is enabled
    alignment: Int,               # Memory alignment guarantee
]
```

The key insight is that `layout` is a **compile-time parameter**. When the shape and strides are known at compile time (e.g., `Layout.row_major(16, 16)`), all offset calculations constant-fold. When dimensions are unknown (UNKNOWN_VALUE), the struct falls back to `runtime_layout` which stores actual values.

## Core State

```mojo
var ptr: Pointer[...]           # Typed pointer with address space
var runtime_layout: RuntimeLayout  # Actual dimensions when not statically known
var runtime_element_layout       # Sub-element layout info
```

Construction from various sources:
- `LayoutTensor(UnsafePointer)` -- wrap raw memory
- `LayoutTensor(Span)` -- wrap a contiguous slice
- `LayoutTensor(DeviceBuffer)` -- wrap GPU memory
- `LayoutTensor(HostBuffer)` -- wrap CPU-side buffer

## Offset Calculation

The `_offset` method maps N-dimensional coordinates to a linear memory offset:

```mojo
fn _offset(self, coords: IndexList) -> Int:
    # For each dimension, if compile-time stride is known, use it
    # Otherwise fall back to runtime stride
    var offset = 0
    for i in range(rank):
        if layout.stride[i] != UNKNOWN_VALUE:
            offset += coords[i] * layout.stride[i]  # Constant-folded
        else:
            offset += coords[i] * runtime_layout.stride[i]  # Runtime
    return offset
```

This dual-path approach means fully static layouts have zero overhead, while dynamic layouts gracefully degrade.

## The `tile()` Method

`tile()` creates a sub-view into the tensor without copying:

```mojo
# Extract a 16x16 sub-tile at position (row=2, col=3)
var sub = tensor.tile[16, 16](2, 3)
```

Implementation:
1. Compute the byte offset: `offset = sum(coord[i] * tile_size[i] * stride[i])`
2. Create a new LayoutTensor with the same strides but reduced shape
3. Advance the pointer by the offset

The sub-tile shares the original memory -- it is a zero-copy view. The strides are preserved from the parent, so a tile of a row-major tensor is also row-major with the same row stride.

## The `distribute()` Method

`distribute()` maps thread IDs to elements for GPU warp-level operations:

```mojo
# Each of 32 warp threads gets its assigned elements from a 16x8 tile
var my_elements = tile.distribute[Layout.row_major(8, 4)](lane_id())
# Thread 0 -> row 0, col 0
# Thread 1 -> row 0, col 1
# Thread 5 -> row 1, col 1
```

The thread layout parameter defines the mapping from thread ID to tile coordinates. Combined with `vectorize()`, this enables loading MMA fragments:

```mojo
# For bf16 MMA: each thread loads 2 contiguous elements
var frags = tile.vectorize[1, 2]().distribute[Layout.row_major(8, 4)](lane_id())
```

An optional `swizzle` parameter applies a `Swizzle` transformation to the computed offset, enabling bank-conflict-free shared memory access.

## The `vectorize()` Method

`vectorize()` reinterprets the tensor with grouped elements:

```mojo
# Group elements into SIMD-width vectors along columns
var vec_tensor = tensor.vectorize[1, 4]()
# Shape becomes (rows, cols/4) with element_layout of (1, 4)
```

This changes the `element_layout` parameter. When you then load from a vectorized tensor, you get `SIMD[dtype, 4]` instead of scalars. The shape is divided by the vector dimensions, and the strides are multiplied.

## The `copy_from()` Method

`copy_from()` transfers data between LayoutTensors, handling layout differences:

```mojo
# Copy from shared memory to registers
reg_tile.copy_from(shared_tile)
```

It unrolls across the tensor dimensions at compile time (when shapes are static) and uses SIMD stores. This is the primary mechanism for shared-to-register transfers in GPU kernels.

## Shared Memory vs Global Memory

The `address_space` parameter controls memory targeting:

```mojo
# Shared memory tensor (GPU kernel local)
var smem = LayoutTensor[
    DType.bfloat16,
    Layout.row_major(64, 64),
    MutAnyOrigin,
    address_space = AddressSpace.SHARED,
](shared_ptr)

# Global memory tensor
var gmem = LayoutTensor[
    DType.bfloat16,
    Layout.row_major(UNKNOWN_VALUE, UNKNOWN_VALUE),
    MutAnyOrigin,
    address_space = AddressSpace.GENERIC,
](global_ptr, runtime_layout)
```

Key differences:
- Shared memory tensors typically have fully static layouts (all dims known at compile time)
- Global memory tensors often have dynamic dimensions (UNKNOWN_VALUE) with runtime layout
- The `alignment` parameter defaults to `align_of[dtype]()` but can be set higher for vectorized loads (e.g., 128 for TMA)
- AMD GPUs use `make_amd_buffer_resource()` for buffer descriptors

## Stack Allocation Pattern

For register-level tensors (MMA fragment storage), MAX uses stack allocation:

```mojo
var c_reg_tile = Self.c_reg_tile_type.stack_allocation()
```

This allocates on the thread's stack (register file on GPU), creating a LayoutTensor backed by an `InlineArray`. The layout must be fully static. This pattern is used extensively in `TensorCore` for accumulator and fragment storage.

```mojo
# Stack-allocated 2D array for MMA accumulator
comptime layout = Layout.col_major(1, num_matrix_reg[M, N]())
var stack = InlineArray[Scalar[dtype], layout.size()](uninitialized=True)
var reg_tensor = LayoutTensor[dtype, layout](stack)
```

## LayoutTensorIter: Tile Iterators

`LayoutTensorIter` provides circular iteration over tiled shared memory buffers:

```mojo
var a_smem = LayoutTensorIter[
    dtype,
    tile_layout,           # Layout of one tile
    MutAnyOrigin,
    address_space = AddressSpace.SHARED,
    alignment = 128,
](smem_ptr, total_size)    # total_size = num_stages * tile_size

# Access stage N's tile
var tile = a_smem.next(stage_idx)[]
```

The iterator tracks a base pointer and total buffer size, computing `ptr + (stage_idx * tile_size) % total_size` for circular buffering. This is the building block for double/multi-buffered pipeline stages in the matmul kernel.

## SIMD Integration

LayoutTensor load/store operations naturally produce SIMD values:

```mojo
# Load 8 contiguous bf16 values as a SIMD vector
var vec: SIMD[DType.bfloat16, 8] = tensor.load[8](row, col)

# Store a computed SIMD result
tensor.store[8](row, col, result_vec)

# Aligned loads for maximum throughput
var vec = tensor.aligned_load[8](coords)  # Uses alignment parameter
```

The `load_to_simd` utility function flattens an entire LayoutTensor into a single SIMD value, used for passing fragments to MMA instructions.

## Elementwise Operations

LayoutTensor supports in-place arithmetic for common patterns:

```mojo
# Scale all elements
tensor *= scale_factor

# Elementwise add (with broadcast)
tensor += bias_tensor

# Unary ops
var exp_result = tensor.__exp__()
```

These are implemented via `_elementwise_unary` and `_elementwise_binary_with_broadcast`, which iterate over elements using compile-time unrolling when possible.

## Runtime vs Compile-Time Dimensions

The `dim` method provides a unified interface:

```mojo
# Compile-time known dimension (returns Int literal)
comptime N = tensor.dim[1]()  # When layout.shape[1] != UNKNOWN_VALUE

# Runtime dimension (queries runtime_layout)
var N = tensor.dim(1)  # When layout.shape[1] == UNKNOWN_VALUE
```

This duality allows the same kernel code to work with both static and dynamic shapes, with the compiler optimizing the static case.

> **Key Takeaway for Neutron**: LayoutTensor is the single most important abstraction to study. Its power comes from: (1) Compile-time layout parameters that enable zero-overhead offset computation. (2) The `tile()` / `distribute()` / `vectorize()` / `copy_from()` chain that maps naturally to GPU kernel patterns. (3) Stack allocation for register-level tensors that map directly to GPU registers. (4) `LayoutTensorIter` for circular buffer management. For Neutron Mojo, implement LayoutTensor early -- it is the foundation on which all GPU kernels are built. Start with `tile()` and `load`/`store`, then add `distribute()` for warp-level operations. The dual compile-time/runtime dimension handling is essential for real-world use where some shapes are known at compile time (head_dim, block sizes) and others are not (batch size, sequence length).
