# neutron-mojo Core Package API Specification

> **Status:** Pseudocode specification -- NOT compilable Mojo
> **Package:** `neutron-mojo`
> **License:** Apache 2.0
> **Last updated:** 2026-02-16

This document defines every public trait, struct, function, and decorator exposed by the `neutron-mojo` core package. All add-on packages (`neutron-mojo-infer`, `neutron-mojo-python`, etc.) depend on these interfaces.

---

## 1. tensor/ -- Tensor Primitives

### 1.1 DType

Enumeration of all supported data types. Mojo 1.0 lacks enums, so this is implemented as an alias-based type with compile-time constants.

```mojo
# Depends on: nothing (Layer 0)

struct DType:
    """Represents a tensor element data type. Each variant is a compile-time constant."""

    # Floating point
    alias float16: DType      # IEEE 754 half-precision (16-bit)
    alias bfloat16: DType     # Brain floating point (16-bit, 8-bit exponent)
    alias float32: DType      # IEEE 754 single-precision (32-bit)
    alias float64: DType      # IEEE 754 double-precision (64-bit)

    # Float8 variants (Hopper+)
    alias float8_e4m3: DType  # FP8 with 4-bit exponent, 3-bit mantissa (inference)
    alias float8_e5m2: DType  # FP8 with 5-bit exponent, 2-bit mantissa (training)

    # Integer
    alias int8: DType         # Signed 8-bit integer
    alias int16: DType        # Signed 16-bit integer
    alias int32: DType        # Signed 32-bit integer
    alias int64: DType        # Signed 64-bit integer
    alias uint8: DType        # Unsigned 8-bit integer
    alias uint16: DType       # Unsigned 16-bit integer
    alias uint32: DType       # Unsigned 32-bit integer
    alias uint64: DType       # Unsigned 64-bit integer

    # Quantized types (first-class, not wrappers)
    alias nf4: DType          # NF4 normal float (bitsandbytes-style, 4-bit)
    alias int4: DType         # Signed 4-bit integer (GPTQ/AWQ)
    alias q4_k: DType         # GGUF Q4_K_M structured block quantization
    alias q5_k: DType         # GGUF Q5_K_M structured block quantization
    alias q8_0: DType         # GGUF Q8_0 simple 8-bit quantization

    # Boolean
    alias bool: DType         # Boolean (1-bit logical, 8-bit storage)

    # Index type
    alias index: DType        # Platform-native index type (usually int64)

    fn bitwidth(self) -> Int:
        """Returns the number of bits per element."""
        ...

    fn is_floating_point(self) -> Bool:
        """Returns True if this is a float type (including float8)."""
        ...

    fn is_quantized(self) -> Bool:
        """Returns True if this is a quantized type (nf4, int4, q4_k, q5_k, q8_0)."""
        ...

    fn is_integer(self) -> Bool:
        """Returns True if this is a signed or unsigned integer type."""
        ...

    fn can_cast_to(self, target: DType) -> Bool:
        """Returns True if values of this type can be cast to target without error."""
        ...
```

### 1.2 Dim

Parametric type representing a named tensor dimension. Dimensions are types, not just integers -- shape mismatches are compile-time errors.

```mojo
# Depends on: nothing (Layer 0)

struct Dim[name: StringLiteral]:
    """A named tensor dimension used as a type parameter.

    Dimensions can be either static (known at compile time) or dynamic
    (resolved at runtime). Static dimensions enable compile-time shape
    checking; dynamic dimensions use runtime validation.
    """

    var size: Int  # Runtime size of this dimension

    fn __init__(inout self, size: Int):
        """Create a dimension with a runtime size."""
        ...

    @staticmethod
    fn static[S: Int]() -> Dim[name]:
        """Create a dimension with a compile-time-known size."""
        ...

# Sentinel type for dimensions whose size is only known at runtime
alias Dynamic = Dim["dynamic"]

# Common dimension aliases (users define their own)
# alias Batch = Dim["batch"]
# alias Seq = Dim["seq"]
# alias Hidden = Dim["hidden"]
# alias Vocab = Dim["vocab"]
# alias Heads = Dim["heads"]
# alias HeadDim = Dim["head_dim"]
```

### 1.3 Shape

Runtime shape representation. Works alongside typed dimensions for cases where dynamic shape manipulation is needed.

```mojo
# Depends on: DType, Dim (Layer 0)

struct Shape:
    """Runtime tensor shape as a list of dimension sizes.

    Used for dynamic shape operations (reshape, broadcasting) and as the
    runtime counterpart to compile-time Dim parameters.
    """

    var ndim: Int             # Number of dimensions
    var dims: List[Int]       # Size of each dimension

    fn __init__(inout self, *dims: Int):
        """Create a shape from dimension sizes."""
        ...

    fn numel(self) -> Int:
        """Returns the total number of elements (product of all dimensions)."""
        ...

    fn __getitem__(self, idx: Int) -> Int:
        """Returns the size of dimension at index idx."""
        ...

    fn broadcast_with(self, other: Shape) -> Shape:
        """Computes the broadcast-compatible output shape, or raises on mismatch."""
        ...

    fn is_contiguous(self, strides: List[Int]) -> Bool:
        """Returns True if the given strides represent contiguous (dense) memory."""
        ...

    fn __eq__(self, other: Shape) -> Bool:
        """Element-wise shape equality."""
        ...
```

### 1.4 Storage

Device memory management. Handles CPU and GPU buffer allocation.

```mojo
# Depends on: DType (Layer 0)
# Layer 1

struct Storage:
    """Owns a contiguous block of device memory for tensor data.

    Handles allocation, deallocation, and CPU<->GPU transfers. On Apple
    Silicon, unified memory avoids explicit transfers.
    """

    var data_ptr: Pointer[UInt8]   # Raw pointer to device memory
    var size_bytes: Int            # Total allocation size in bytes
    var device: DeviceKind         # CPU, CUDA, ROCm, Metal
    var device_id: Int             # Device ordinal (e.g., GPU 0, GPU 1)

    @staticmethod
    fn allocate(size_bytes: Int, device: DeviceKind, device_id: Int = 0) -> Storage:
        """Allocate device memory. Raises on allocation failure."""
        ...

    fn to_device(self, target_device: DeviceKind, target_id: Int = 0) -> Storage:
        """Copy data to a different device, returning a new Storage. Zero-copy on unified memory."""
        ...

    fn copy_from(inout self, src: Storage):
        """Copy data from src into this storage. Sizes must match."""
        ...

    fn __del__(owned self):
        """Frees the underlying device memory."""
        ...

struct DeviceKind:
    """Enumerates supported device types."""
    alias CPU: DeviceKind
    alias CUDA: DeviceKind
    alias ROCm: DeviceKind
    alias Metal: DeviceKind
```

### 1.5 TensorView

Strided view into tensor data. Enables slicing and broadcasting without copying.

```mojo
# Depends on: DType, Shape, Storage (Layer 0-1)
# Layer 1

struct TensorView:
    """A non-owning strided view into tensor data.

    Supports slicing, transposing, and broadcasting by manipulating
    shape and strides without copying the underlying data.
    """

    var storage: Pointer[Storage]  # Non-owning reference to backing storage
    var shape: Shape               # Logical shape of the view
    var strides: List[Int]         # Strides in elements (not bytes)
    var offset: Int                # Byte offset into storage

    fn slice(self, *slices: Slice) -> TensorView:
        """Returns a view of a sub-region. No data copy."""
        ...

    fn transpose(self, dim0: Int, dim1: Int) -> TensorView:
        """Returns a view with two dimensions swapped. No data copy."""
        ...

    fn reshape(self, new_shape: Shape) -> TensorView:
        """Returns a view with a new shape. Only valid for contiguous data or compatible strides."""
        ...

    fn broadcast_to(self, target_shape: Shape) -> TensorView:
        """Returns a view broadcast to target_shape. Raises on incompatible shapes."""
        ...

    fn is_contiguous(self) -> Bool:
        """Returns True if the view's memory is dense (no gaps between elements)."""
        ...

    fn contiguous(self) -> TensorView:
        """Returns a contiguous copy if not already contiguous, otherwise returns self."""
        ...
```

### 1.6 Tensor

The core tensor type. Parametric over dtype and named dimensions for compile-time shape safety.

```mojo
# Depends on: DType, Dim, Shape, Storage, TensorView (Layer 0-1)
# Layer 2

struct Tensor[dtype: DType, *dims: Dim]:
    """A multi-dimensional array with typed element type and named dimensions.

    Shape mismatches between named dimensions are compile-time type errors.
    Supports eager-mode operations (results computed immediately) and
    DLPack import/export for zero-copy interop with PyTorch/JAX/NumPy.
    """

    var storage: Storage       # Owned device memory
    var view: TensorView       # Current view (shape, strides, offset)

    @staticmethod
    fn zeros(*sizes: Int, device: DeviceKind = DeviceKind.CPU) -> Tensor[dtype, *dims]:
        """Create a tensor filled with zeros."""
        ...

    @staticmethod
    fn ones(*sizes: Int, device: DeviceKind = DeviceKind.CPU) -> Tensor[dtype, *dims]:
        """Create a tensor filled with ones."""
        ...

    @staticmethod
    fn rand(*sizes: Int, device: DeviceKind = DeviceKind.CPU) -> Tensor[dtype, *dims]:
        """Create a tensor filled with uniform random values in [0, 1)."""
        ...

    @staticmethod
    fn randn(*sizes: Int, device: DeviceKind = DeviceKind.CPU) -> Tensor[dtype, *dims]:
        """Create a tensor filled with standard normal random values."""
        ...

    @staticmethod
    fn from_pointer(ptr: Pointer[Scalar[dtype]], shape: Shape, device: DeviceKind = DeviceKind.CPU) -> Tensor[dtype, *dims]:
        """Wrap an existing memory pointer as a tensor. Caller retains ownership of memory."""
        ...

    fn shape(self) -> Shape:
        """Returns the runtime shape."""
        ...

    fn dtype(self) -> DType:
        """Returns the element data type."""
        ...

    fn device(self) -> DeviceKind:
        """Returns the device this tensor resides on."""
        ...

    fn data_ptr(self) -> Pointer[Scalar[dtype]]:
        """Returns a raw pointer to the tensor's data."""
        ...

    fn to(self, device: DeviceKind, device_id: Int = 0) -> Tensor[dtype, *dims]:
        """Move tensor to a different device. Zero-copy on unified memory."""
        ...

    fn cast[target_dtype: DType](self) -> Tensor[target_dtype, *dims]:
        """Cast elements to a different dtype. Returns a new tensor."""
        ...

    fn contiguous(self) -> Tensor[dtype, *dims]:
        """Returns a contiguous copy if not already contiguous."""
        ...

    fn __getitem__(self, *indices: Int) -> Scalar[dtype]:
        """Element access by indices."""
        ...

    fn __setitem__(inout self, *indices: Int, value: Scalar[dtype]):
        """Element assignment by indices."""
        ...

    fn load[width: Int](self, *indices: Int) -> SIMD[dtype, width]:
        """Load a SIMD vector of `width` contiguous elements starting at indices."""
        ...

    fn store[width: Int](inout self, *indices: Int, value: SIMD[dtype, width]):
        """Store a SIMD vector of `width` contiguous elements starting at indices."""
        ...
```

### 1.7 Tensor Ops

Elementwise, reduction, and linear algebra operations dispatched eagerly.

```mojo
# Depends on: Tensor, DType, Dim (Layer 0-2)
# Layer 2

# --- Elementwise Arithmetic ---

fn add[dtype: DType, *dims: Dim](
    a: Tensor[dtype, *dims],
    b: Tensor[dtype, *dims],
) -> Tensor[dtype, *dims]:
    """Elementwise addition. Broadcasting supported via dimension compatibility."""
    ...

fn sub[dtype: DType, *dims: Dim](
    a: Tensor[dtype, *dims],
    b: Tensor[dtype, *dims],
) -> Tensor[dtype, *dims]:
    """Elementwise subtraction."""
    ...

fn mul[dtype: DType, *dims: Dim](
    a: Tensor[dtype, *dims],
    b: Tensor[dtype, *dims],
) -> Tensor[dtype, *dims]:
    """Elementwise multiplication (Hadamard product)."""
    ...

fn div[dtype: DType, *dims: Dim](
    a: Tensor[dtype, *dims],
    b: Tensor[dtype, *dims],
) -> Tensor[dtype, *dims]:
    """Elementwise division."""
    ...

# --- Matrix Multiplication ---

fn matmul[dtype: DType, M: Dim, K: Dim, N: Dim](
    a: Tensor[dtype, M, K],
    b: Tensor[dtype, K, N],
) -> Tensor[dtype, M, N]:
    """Matrix multiplication. Inner dimensions (K) must match at compile time.

    Dispatches to the optimal backend kernel (tensor cores when available).
    Supports batched matmul when leading batch dimensions are present.
    """
    ...

# --- Activation Functions ---

fn relu[dtype: DType, *dims: Dim](x: Tensor[dtype, *dims]) -> Tensor[dtype, *dims]:
    """Rectified linear unit: max(0, x)."""
    ...

fn gelu[dtype: DType, *dims: Dim](x: Tensor[dtype, *dims]) -> Tensor[dtype, *dims]:
    """Gaussian error linear unit (approximate or exact based on compile-time config)."""
    ...

fn silu[dtype: DType, *dims: Dim](x: Tensor[dtype, *dims]) -> Tensor[dtype, *dims]:
    """Sigmoid linear unit (SiLU/Swish): x * sigmoid(x)."""
    ...

fn softmax[dtype: DType, *dims: Dim](
    x: Tensor[dtype, *dims],
    axis: Int = -1,
) -> Tensor[dtype, *dims]:
    """Numerically stable softmax along the specified axis (online algorithm)."""
    ...

# --- Reductions ---

fn reduce_sum[dtype: DType, *dims: Dim](
    x: Tensor[dtype, *dims],
    axis: Int = -1,
    keepdim: Bool = False,
) -> Tensor[dtype, *dims]:
    """Sum reduction along the specified axis."""
    ...

fn reduce_max[dtype: DType, *dims: Dim](
    x: Tensor[dtype, *dims],
    axis: Int = -1,
    keepdim: Bool = False,
) -> Tensor[dtype, *dims]:
    """Max reduction along the specified axis."""
    ...
```

---

## 2. kernel/ -- Kernel Abstraction

### 2.1 @kernel Decorator and Tile Iterator

The `@kernel` decorator marks a function for GPU compilation. The `tiles[]` iterator generates tile descriptors for blocked execution.

```mojo
# Depends on: Tensor, DType, Dim (Layer 0-2)
# Layer 3

# --- @kernel decorator ---

# @kernel
# Decorator that marks a function for GPU kernel compilation via MLIR.
#
# Usage:
#   @kernel
#   fn my_kernel(a: Tensor[F32, M, N], b: Tensor[F32, M, N]) -> Tensor[F32, M, N]:
#       ...
#
# The decorated function can use tiles[], SIMD operations, warp primitives,
# and block primitives. The compiler lowers it through MLIR to the target
# backend (NVIDIA CUBIN, AMD GCN, Apple Metal).
#
# When used with a Schedule (see schedule.mojo), the algorithm and hardware
# mapping are separated -- the @kernel body defines WHAT to compute, the
# Schedule defines HOW to map it to hardware.

# --- Tile Descriptor ---

struct Tile:
    """Describes a tile's position within a tiled iteration space.

    Produced by the tiles[] iterator. Provides row/col offsets and
    tile dimensions for blocked memory access.
    """

    var row: Int         # Starting row offset of this tile
    var col: Int         # Starting column offset of this tile
    var tile_m: Int      # Tile height (may be smaller at boundaries)
    var tile_n: Int      # Tile width (may be smaller at boundaries)
    var tile_id: Int     # Linear tile index (for work scheduling)

    fn in_bounds(self, total_m: Int, total_n: Int) -> Bool:
        """Returns True if the entire tile is within bounds (no boundary masking needed)."""
        ...

# --- tiles[] Iterator ---

fn tiles[BLOCK: Int](*dimensions: Int) -> TileIterator:
    """Creates a tile iterator that partitions the given dimensions into BLOCK-sized tiles.

    Handles boundary conditions automatically. Each iteration yields a Tile
    descriptor with position and size information.

    Example:
        for tile in tiles[64](M, N):
            # tile.row, tile.col give the top-left corner
            # tile.tile_m, tile.tile_n give the (possibly clamped) tile size
    """
    ...

struct TileIterator:
    """Iterator over tiles in a blocked iteration space. Produced by tiles[]."""

    fn __iter__(self) -> Self:
        ...

    fn __next__(inout self) -> Tile:
        ...

    fn __len__(self) -> Int:
        """Returns the total number of tiles."""
        ...
```

### 2.2 Schedule API

Algorithm/schedule separation inspired by Halide. The algorithm (@kernel body) defines the computation; the Schedule defines hardware mapping.

```mojo
# Depends on: @kernel (Layer 3)
# Layer 3

struct Schedule:
    """Hardware mapping directives applied to a @kernel function.

    Separates the WHAT (algorithm) from the HOW (hardware optimization).
    Multiple schedules can be defined for the same kernel, targeting
    different hardware.
    """

    fn tile(self, *dims: Dim, block: Int) -> Schedule:
        """Tile the specified dimensions into blocks of the given size.

        Generates a blocked loop nest. Inner tiles become SIMD-width work items.
        """
        ...

    fn vectorize(self, dim: Dim, width: Int) -> Schedule:
        """SIMD-vectorize the specified dimension with the given vector width.

        The innermost loop over `dim` is replaced with SIMD operations of `width` lanes.
        """
        ...

    fn pipeline(self, stages: Int = 2) -> Schedule:
        """Enable software pipelining with the specified number of stages.

        Overlaps memory loads with compute by issuing loads `stages` iterations
        ahead. Critical for hiding memory latency on Hopper+.
        """
        ...

    fn unroll(self, dim: Dim, factor: Int) -> Schedule:
        """Unroll the loop over `dim` by the given factor.

        Full unroll if factor equals the loop trip count; partial otherwise.
        """
        ...

    fn parallel(self, dim: Dim) -> Schedule:
        """Map the specified dimension to parallel GPU threads/blocks.

        The compiler determines the optimal thread/block mapping based on
        the target hardware.
        """
        ...

    fn target(self, backend: ComputeBackend) -> Schedule:
        """Set the target hardware backend for this schedule."""
        ...

# Apply a schedule to a kernel:
# my_kernel.schedule(
#     Schedule()
#         .tile(M, N, block=64)
#         .tile(K, block=32)
#         .vectorize(N, width=16)
#         .pipeline(stages=3)
#         .target(nvidia_hopper)
# )
```

### 2.3 SIMD Operations

Wrappers around SIMD intrinsics for use within @kernel functions.

```mojo
# Depends on: DType (Layer 0)
# Layer 3

fn fma[dtype: DType, width: Int](
    a: SIMD[dtype, width],
    b: SIMD[dtype, width],
    c: SIMD[dtype, width],
) -> SIMD[dtype, width]:
    """Fused multiply-add: a * b + c in a single instruction. No intermediate rounding."""
    ...

fn simd_add[dtype: DType, width: Int](
    a: SIMD[dtype, width],
    b: SIMD[dtype, width],
) -> SIMD[dtype, width]:
    """SIMD vector addition."""
    ...

fn simd_mul[dtype: DType, width: Int](
    a: SIMD[dtype, width],
    b: SIMD[dtype, width],
) -> SIMD[dtype, width]:
    """SIMD vector multiplication."""
    ...
```

### 2.4 Warp Primitives

Warp-level communication primitives for use within @kernel functions. All operate within a single warp (32 threads on NVIDIA, 64 on AMD).

```mojo
# Depends on: DType (Layer 0)
# Layer 3

struct warp:
    """Warp-level primitives. All operations have implicit warp synchronization."""

    @staticmethod
    fn shuffle[dtype: DType](
        value: Scalar[dtype],
        src_lane: Int,
        width: Int = 32,
    ) -> Scalar[dtype]:
        """Read a value from a specific lane within the warp (shuffle_idx)."""
        ...

    @staticmethod
    fn shuffle_xor[dtype: DType](
        value: Scalar[dtype],
        lane_mask: Int,
    ) -> Scalar[dtype]:
        """Exchange values between lanes whose IDs differ by XOR of lane_mask (butterfly pattern)."""
        ...

    @staticmethod
    fn reduce[dtype: DType, op: ReduceOp](
        value: Scalar[dtype],
    ) -> Scalar[dtype]:
        """Warp-wide reduction (sum, max, min). Result broadcast to all lanes."""
        ...

    @staticmethod
    fn scan[dtype: DType, op: ReduceOp, exclusive: Bool = False](
        value: Scalar[dtype],
    ) -> Scalar[dtype]:
        """Warp-level prefix scan (inclusive or exclusive). Each lane gets the cumulative result of all prior lanes."""
        ...

    @staticmethod
    fn broadcast[dtype: DType](
        value: Scalar[dtype],
        src_lane: Int = 0,
    ) -> Scalar[dtype]:
        """Broadcast a value from src_lane to all lanes in the warp."""
        ...

struct ReduceOp:
    """Reduction operation selector for warp/block reductions."""
    alias SUM: ReduceOp
    alias MAX: ReduceOp
    alias MIN: ReduceOp
```

### 2.5 Block Primitives

Block-level operations that coordinate across all warps in a thread block. Use shared memory internally.

```mojo
# Depends on: DType, ReduceOp (Layer 0, 3)
# Layer 3

struct block:
    """Block-level primitives. All operations synchronize across the entire thread block."""

    @staticmethod
    fn reduce[dtype: DType, op: ReduceOp](
        value: Scalar[dtype],
    ) -> Scalar[dtype]:
        """Block-wide reduction via shared memory. Result broadcast to all threads."""
        ...

    @staticmethod
    fn broadcast[dtype: DType](
        value: Scalar[dtype],
        src_thread: Int = 0,
    ) -> Scalar[dtype]:
        """Broadcast a value from src_thread to all threads in the block."""
        ...

    @staticmethod
    fn prefix_sum[dtype: DType, exclusive: Bool = False](
        value: Scalar[dtype],
    ) -> Scalar[dtype]:
        """Block-level prefix sum (inclusive or exclusive) via shared memory."""
        ...

    @staticmethod
    fn shared_alloc[dtype: DType](size: Int) -> Pointer[Scalar[dtype]]:
        """Allocate shared memory visible to all threads in the block.

        Backed by on-chip SRAM (48-228 KB depending on GPU). Lifetime is
        the kernel invocation.
        """
        ...
```

---

## 3. layout/ -- Memory Layout Algebra

### 3.1 Layout Trait and Standard Layouts

Defines how logical tensor indices map to physical memory offsets. Based on CuTe's layout algebra for bank-conflict-free access.

```mojo
# Depends on: nothing (Layer 0-1)
# Layer 1

trait Layout:
    """Maps logical multi-dimensional indices to a linear memory offset.

    Implementations define the mapping from (i, j, k, ...) to a flat
    byte offset. Used by Tensor, TensorView, and kernel tile operations
    to access memory with optimal access patterns.
    """

    fn offset(self, *indices: Int) -> Int:
        """Compute the linear memory offset for the given logical indices."""
        ...

    fn stride(self, dim: Int) -> Int:
        """Returns the stride (in elements) for the given dimension."""
        ...

    fn shape(self) -> Shape:
        """Returns the logical shape this layout maps."""
        ...

struct RowMajor(Layout):
    """Row-major (C-order) layout. Last dimension varies fastest.

    Memory: [row0_col0, row0_col1, ..., row1_col0, row1_col1, ...]
    Default layout for tensors.
    """

    fn offset(self, *indices: Int) -> Int: ...
    fn stride(self, dim: Int) -> Int: ...
    fn shape(self) -> Shape: ...

struct ColMajor(Layout):
    """Column-major (Fortran-order) layout. First dimension varies fastest.

    Memory: [row0_col0, row1_col0, ..., row0_col1, row1_col1, ...]
    Used when interfacing with BLAS routines.
    """

    fn offset(self, *indices: Int) -> Int: ...
    fn stride(self, dim: Int) -> Int: ...
    fn shape(self) -> Shape: ...

struct Strided(Layout):
    """Arbitrary strided layout. Each dimension has an independent stride.

    Used for views created by slicing, transposing, or broadcasting.
    """

    var strides: List[Int]

    fn __init__(inout self, strides: List[Int]):
        """Create a layout with explicit strides."""
        ...

    fn offset(self, *indices: Int) -> Int: ...
    fn stride(self, dim: Int) -> Int: ...
    fn shape(self) -> Shape: ...
```

### 3.2 Swizzle Functions

Bank-conflict-free memory access patterns. Based on CuTe/ThunderKittens techniques that achieve 85% fewer stalled cycles.

```mojo
# Depends on: Layout (Layer 1)
# Layer 1

fn swizzle[bits: Int, base: Int, shift: Int](index: Int) -> Int:
    """Apply an XOR-based swizzle to a memory index.

    Remaps shared memory accesses to avoid bank conflicts. Parameters:
    - bits: number of bits to swizzle (controls the swizzle pattern width)
    - base: starting bit position for the XOR source
    - shift: bit offset between XOR source and target

    Returns the swizzled index. Used internally by TileLayout and TensorCoreLayout.
    """
    ...

fn swizzle_offset(row: Int, col: Int, num_cols: Int, swizzle_bits: Int = 3) -> Int:
    """Compute a swizzled linear offset for 2D shared memory access.

    Eliminates bank conflicts for common tile sizes (64x64, 128x64, etc.).
    """
    ...
```

### 3.3 TileLayout

Maps tiles to thread-level data ownership. Controls how a tile of data is distributed across threads in a block.

```mojo
# Depends on: Layout, swizzle (Layer 1)
# Layer 1

struct TileLayout(Layout):
    """Layout for a tile that maps thread indices to data elements.

    Combines a logical tile shape with a swizzle pattern to produce
    bank-conflict-free shared memory access. Used by @kernel tile operations.
    """

    var tile_m: Int             # Tile height
    var tile_n: Int             # Tile width
    var swizzle_bits: Int       # Number of swizzle bits (0 = no swizzle)

    fn __init__(inout self, tile_m: Int, tile_n: Int, swizzle_bits: Int = 3):
        """Create a tile layout with automatic bank-conflict avoidance."""
        ...

    fn offset(self, *indices: Int) -> Int:
        """Compute the swizzled memory offset for tile-local (row, col)."""
        ...

    fn stride(self, dim: Int) -> Int: ...
    fn shape(self) -> Shape: ...

    fn thread_to_element(self, thread_id: Int) -> Tuple[Int, Int]:
        """Maps a thread index to the (row, col) of the element it owns."""
        ...
```

### 3.4 TensorCoreLayout

Specialized layout for tensor core MMA (matrix multiply-accumulate) operations. Handles the register-to-shared-memory mapping required by tensor cores.

```mojo
# Depends on: Layout, TileLayout (Layer 1)
# Layer 1

struct TensorCoreLayout(Layout):
    """Layout optimized for tensor core matrix multiply-accumulate operations.

    Maps thread registers to the fragment layout expected by MMA instructions
    (e.g., m16n8k16 on NVIDIA Hopper, m16n16k16 on AMD CDNA3).

    Automatically selects the correct MMA shape based on dtype and target GPU.
    """

    var mma_m: Int       # MMA tile M dimension (e.g., 16)
    var mma_n: Int       # MMA tile N dimension (e.g., 8 or 16)
    var mma_k: Int       # MMA tile K dimension (e.g., 16)
    var dtype: DType     # Element type (determines available MMA shapes)

    fn __init__(inout self, dtype: DType, target: DeviceKind):
        """Auto-select optimal MMA shape for the given dtype and target."""
        ...

    fn offset(self, *indices: Int) -> Int: ...
    fn stride(self, dim: Int) -> Int: ...
    fn shape(self) -> Shape: ...

    fn fragment_size(self) -> Int:
        """Returns the number of elements each thread holds in its register fragment."""
        ...

    fn accumulator_size(self) -> Int:
        """Returns the number of accumulator registers per thread."""
        ...
```

---

## 4. fusion/ -- Graph Optimization

### 4.1 Graph IR

Computation graph intermediate representation. Nodes represent operations on tensors.

```mojo
# Depends on: DType, Shape (Layer 0)
# Layer 4

struct Op:
    """Enumerates all computation graph operations.

    Used as node labels in the computation graph. Each Op maps to a
    kernel implementation in the backend.
    """

    # Elementwise
    alias Add: Op
    alias Sub: Op
    alias Mul: Op
    alias Div: Op
    alias Neg: Op
    alias Exp: Op
    alias Log: Op
    alias Sqrt: Op
    alias Rsqrt: Op
    alias Abs: Op

    # Activations
    alias Relu: Op
    alias Gelu: Op
    alias Silu: Op
    alias Sigmoid: Op
    alias Tanh: Op

    # Reductions
    alias ReduceSum: Op
    alias ReduceMax: Op
    alias ReduceMean: Op
    alias Softmax: Op

    # Linear algebra
    alias MatMul: Op
    alias BatchMatMul: Op
    alias Linear: Op      # matmul + bias

    # Shape operations
    alias Reshape: Op
    alias Transpose: Op
    alias Broadcast: Op
    alias Concat: Op
    alias Split: Op
    alias Slice: Op

    # Normalization
    alias LayerNorm: Op
    alias RMSNorm: Op

    # Type operations
    alias Cast: Op

    # Fused operations (created by fusion engine)
    alias FusedLinearGelu: Op
    alias FusedLinearBias: Op
    alias FusedSoftmaxCrossEntropy: Op
    alias FusedAffine: Op        # matmul + bias in one kernel

struct Node:
    """A node in the computation graph.

    Represents a single operation with typed inputs, output shape, and
    metadata for the fusion engine and scheduler.
    """

    var id: Int                    # Unique node identifier
    var op: Op                     # The operation this node performs
    var inputs: List[Int]          # Node IDs of input operands
    var output_shape: Shape        # Shape of the output tensor
    var output_dtype: DType        # DType of the output tensor
    var metadata: Dict[String, String]  # Backend hints, fusion tags, etc.

    fn __init__(inout self, op: Op, inputs: List[Int], output_shape: Shape, output_dtype: DType):
        """Create a graph node."""
        ...

struct Graph:
    """A computation graph: a DAG of Nodes.

    Built by the @compile decorator via tracing. Consumed by the fusion
    engine and then compiled to target-specific kernels.
    """

    var nodes: List[Node]          # All nodes in topological order
    var inputs: List[Int]          # Node IDs that are graph inputs
    var outputs: List[Int]         # Node IDs that are graph outputs

    fn add_node(inout self, op: Op, inputs: List[Int], output_shape: Shape, output_dtype: DType) -> Int:
        """Add a node to the graph. Returns the new node's ID."""
        ...

    fn get_node(self, id: Int) -> Node:
        """Look up a node by ID."""
        ...

    fn topological_order(self) -> List[Int]:
        """Returns node IDs in valid execution order (all inputs before consumers)."""
        ...

    fn subgraph(self, node_ids: List[Int]) -> Graph:
        """Extract a subgraph containing only the specified nodes."""
        ...
```

### 4.2 Fusion Engine

Fuses adjacent operations that fit in SRAM, enforcing the "no HBM materialization" invariant from XLA.

```mojo
# Depends on: Graph, Node, Op (Layer 4)
# Layer 4

struct FusionEngine:
    """Identifies and fuses adjacent graph operations into single kernels.

    Enforces the "no HBM materialization" invariant: fused ops must fit
    entirely in registers and SRAM. If an intermediate result would need
    to be written to HBM, the ops are NOT fused.
    """

    var sram_budget_bytes: Int     # Available SRAM per SM (e.g., 228KB on H100)

    fn __init__(inout self, sram_budget_bytes: Int):
        """Create a fusion engine with the given SRAM budget."""
        ...

    fn can_fuse(self, graph: Graph, node_a: Int, node_b: Int) -> Bool:
        """Returns True if nodes a and b can be fused without HBM materialization.

        Checks: (1) b consumes a's output, (2) a's output is not used
        elsewhere, (3) combined register/SRAM usage fits the budget.
        """
        ...

    fn fuse(self, graph: Graph) -> Graph:
        """Apply all valid fusions to the graph. Returns a new graph with fused ops.

        Iterates until no more fusions are possible (fixed-point).
        """
        ...
```

### 4.3 E-Graph

Equality graph data structure for algebraic optimization. Explores equivalent computation representations to find optimal rewrites.

```mojo
# Depends on: Graph, Node, Op (Layer 4)
# Layer 4

struct EGraph:
    """Equality graph (e-graph) for algebraic optimization of computation graphs.

    An e-graph compactly represents many equivalent computations. E-classes
    group equivalent e-nodes. Rewrite rules add new equivalences. After
    saturation, the optimal computation is extracted.

    Phase 1 scope: simple algebraic rewrites (30+ rules).
    Phase 2 scope: full equality saturation with cost-based extraction.
    """

    fn __init__(inout self):
        """Create an empty e-graph."""
        ...

    fn add(inout self, node: Node) -> Int:
        """Add an e-node to the e-graph. Returns its e-class ID.

        If an equivalent e-node already exists, returns the existing e-class.
        """
        ...

    fn merge(inout self, class_a: Int, class_b: Int) -> Int:
        """Merge two e-classes (declare them equivalent). Returns the canonical e-class ID.

        Triggers re-canonicalization of all parent e-nodes.
        """
        ...

    fn find(self, class_id: Int) -> Int:
        """Find the canonical e-class ID (union-find with path compression)."""
        ...

    fn extract_best(self, root_class: Int, cost_fn: fn(Node) -> Float64) -> Graph:
        """Extract the lowest-cost computation graph from the e-graph.

        Traverses the e-graph starting from root_class, selecting the
        cheapest e-node from each e-class according to cost_fn.
        """
        ...

struct RewriteRules:
    """Collection of algebraic rewrite rules for e-graph optimization.

    Contains 30+ validated mathematical identities (see specs/egraph_rules.md).
    Rules are applied iteratively until saturation or a budget is exhausted.
    """

    fn __init__(inout self):
        """Create the default rule set (all 30+ algebraic rewrites)."""
        ...

    fn apply_rewrites(self, inout egraph: EGraph, max_iterations: Int = 100) -> Int:
        """Apply all rewrite rules to the e-graph until saturation or max_iterations.

        Returns the number of new e-class merges performed.
        """
        ...

    fn add_rule(inout self, name: String, pattern: Graph, replacement: Graph):
        """Register a custom rewrite rule (pattern -> replacement)."""
        ...
```

---

## 5. backend/ -- Hardware Backends

### 5.1 ComputeBackend Trait

The trait all hardware backends implement. Follows ExecuTorch's delegate/partitioner pattern for automatic subgraph delegation.

```mojo
# Depends on: Graph, DType, Op (Layer 0, 4)
# Layer 5

struct DeviceInfo:
    """Hardware capability descriptor for a compute device."""

    var name: String                # Human-readable device name (e.g., "NVIDIA H100 SXM")
    var compute_capability: Tuple[Int, Int]  # (major, minor) version
    var sram_per_sm_bytes: Int      # Shared memory per SM/CU
    var num_sms: Int                # Number of SMs/CUs
    var hbm_bytes: Int              # Total HBM/VRAM capacity
    var hbm_bandwidth_gbps: Float64 # HBM bandwidth in GB/s
    var supports_tensor_cores: Bool # Tensor core / matrix core support
    var supports_float8: Bool       # FP8 support (Hopper+, MI300+)
    var max_threads_per_block: Int  # Maximum threads per thread block
    var warp_size: Int              # Warp/wavefront width (32 NVIDIA, 64 AMD)
    var unified_memory: Bool        # True for Apple Silicon, false for discrete GPUs

struct CompiledKernel:
    """An opaque handle to a compiled device-specific kernel binary."""

    var binary: Pointer[UInt8]     # Pointer to compiled binary (CUBIN, GCN, AIR, etc.)
    var binary_size: Int           # Size in bytes
    var entry_point: String        # Kernel entry point function name
    var shared_mem_bytes: Int      # Dynamic shared memory requirement
    var registers_per_thread: Int  # Register usage per thread

trait ComputeBackend:
    """Target hardware for kernel compilation and execution.

    Each backend compiles MLIR modules to device-specific binaries and
    manages kernel execution on that device. Backends report their
    capabilities so the partitioner can route subgraphs optimally.
    """

    fn compile_kernel(self, kernel: Graph) -> CompiledKernel:
        """Compile a computation graph (or kernel IR) to a device-specific binary.

        The input is an optimized subgraph from the fusion engine.
        """
        ...

    fn execute(self, kernel: CompiledKernel, inputs: List[Tensor], outputs: List[Tensor]):
        """Launch a compiled kernel with the given input/output tensors.

        Inputs must reside on this backend's device. Outputs are written in-place.
        """
        ...

    fn device_info(self) -> DeviceInfo:
        """Returns capability information for this device."""
        ...

    fn supports_op(self, op: Op, dtype: DType) -> Bool:
        """Returns True if this backend can execute the given op at the given dtype.

        Used by the Partitioner to decide subgraph delegation.
        """
        ...

struct NvidiaBackend(ComputeBackend):
    """NVIDIA GPU backend. Compiles to CUBIN via NVVM dialect.

    Supports Ampere (SM 80), Hopper (SM 90), Blackwell (SM 100+).
    Tensor cores, TMA, warp specialization on Hopper+.
    """

    fn __init__(inout self, device_id: Int = 0):
        ...
    fn compile_kernel(self, kernel: Graph) -> CompiledKernel: ...
    fn execute(self, kernel: CompiledKernel, inputs: List[Tensor], outputs: List[Tensor]): ...
    fn device_info(self) -> DeviceInfo: ...
    fn supports_op(self, op: Op, dtype: DType) -> Bool: ...

struct AmdBackend(ComputeBackend):
    """AMD GPU backend. Compiles to GCN/CDNA via AMDGPU target.

    Supports MI300X (CDNA3). Matrix cores, 64-wide wavefronts.
    """

    fn __init__(inout self, device_id: Int = 0):
        ...
    fn compile_kernel(self, kernel: Graph) -> CompiledKernel: ...
    fn execute(self, kernel: CompiledKernel, inputs: List[Tensor], outputs: List[Tensor]): ...
    fn device_info(self) -> DeviceInfo: ...
    fn supports_op(self, op: Op, dtype: DType) -> Bool: ...

struct AppleBackend(ComputeBackend):
    """Apple Silicon backend. Compiles to Metal/AIR.

    Supports M1-M4 series. Unified memory (zero-copy CPU<->GPU).
    """

    fn __init__(inout self, device_id: Int = 0):
        ...
    fn compile_kernel(self, kernel: Graph) -> CompiledKernel: ...
    fn execute(self, kernel: CompiledKernel, inputs: List[Tensor], outputs: List[Tensor]): ...
    fn device_info(self) -> DeviceInfo: ...
    fn supports_op(self, op: Op, dtype: DType) -> Bool: ...

struct CpuBackend(ComputeBackend):
    """CPU fallback backend. Compiles via LLVM IR.

    Used for operations not supported by the GPU backend, and as the
    reference correctness baseline for testing.
    """

    fn __init__(inout self):
        ...
    fn compile_kernel(self, kernel: Graph) -> CompiledKernel: ...
    fn execute(self, kernel: CompiledKernel, inputs: List[Tensor], outputs: List[Tensor]): ...
    fn device_info(self) -> DeviceInfo: ...
    fn supports_op(self, op: Op, dtype: DType) -> Bool: ...
```

### 5.2 Partitioner

Routes computation subgraphs to the best available backend. Falls back to CPU for unsupported operations.

```mojo
# Depends on: ComputeBackend, Graph, Op, DType (Layer 4-5)
# Layer 5

struct Partitioner:
    """Routes computation subgraphs to the optimal backend.

    Follows ExecuTorch's delegate pattern: partition the graph into
    subgraphs, assign each to the best backend that supports all its
    ops, and fall back to CpuBackend for anything unsupported.
    """

    var backends: List[ComputeBackend]  # Available backends, ordered by preference
    var fallback: CpuBackend            # Always-available CPU fallback

    fn __init__(inout self, backends: List[ComputeBackend]):
        """Create a partitioner with the given backends. CPU fallback is implicit."""
        ...

    fn partition(self, graph: Graph) -> List[Tuple[Graph, ComputeBackend]]:
        """Partition a graph into subgraphs, each assigned to a backend.

        Returns (subgraph, backend) pairs in execution order. Connected
        ops that share a backend are grouped together to minimize
        cross-device data transfers.
        """
        ...

    fn best_backend_for(self, op: Op, dtype: DType) -> ComputeBackend:
        """Returns the highest-priority backend that supports the given op and dtype."""
        ...
```

---

## 6. runtime/ -- Execution Runtime

### 6.1 Eager Dispatch

Default execution mode. Operations execute immediately, like PyTorch.

```mojo
# Depends on: Tensor, ComputeBackend, Partitioner (Layer 0-5)
# Layer 6

fn eager_dispatch[dtype: DType, *dims: Dim](
    op: Op,
    *inputs: Tensor[dtype, *dims],
) -> Tensor[dtype, *dims]:
    """Execute an operation immediately on the best available backend.

    This is the default execution mode. Each tensor operation (add, matmul,
    relu, etc.) calls eager_dispatch to run on the GPU (or CPU fallback)
    without building a computation graph.
    """
    ...
```

### 6.2 @compile Decorator

Traces a function to capture a computation graph, then optimizes and compiles it. Opt-in optimization (eager is the default).

```mojo
# Depends on: Graph, FusionEngine, EGraph, ComputeBackend (Layer 4-5)
# Layer 6

# @compile
# Decorator that captures a computation graph by tracing function execution,
# then optimizes and compiles it for the target backend.
#
# Optimization pipeline:
#   1. Trace: execute the function, recording all operations into a Graph
#   2. E-graph rewrite: apply algebraic simplifications (RewriteRules)
#   3. Fusion: fuse adjacent ops that fit in SRAM (FusionEngine)
#   4. Partition: route subgraphs to backends (Partitioner)
#   5. Compile: lower each subgraph to device-specific binaries
#   6. Cache: store compiled kernels for reuse
#
# Usage:
#   @compile
#   fn my_model(x: Tensor[F32, B, S, H]) -> Tensor[F32, B, S, H]:
#       let y = layernorm(x)
#       let z = linear(y, W) + bias
#       return gelu(z)
#
#   # First call traces and compiles; subsequent calls reuse the compiled version
#   let result = my_model(input_tensor)
#
# Note: @compile requires the function to be pure (no side effects that
# affect tensor values). Control flow that depends on tensor values is
# not supported in Phase 1.
```

### 6.3 DeviceContext

Global device state management.

```mojo
# Depends on: DeviceKind (Layer 1)
# Layer 6

struct DeviceContext:
    """Manages the active device for tensor operations.

    Thread-local: each thread can have its own active device. Operations
    default to the active device unless a tensor is explicitly placed.
    """

    @staticmethod
    fn set_device(device: DeviceKind, device_id: Int = 0):
        """Set the active device for the current thread."""
        ...

    @staticmethod
    fn get_device() -> Tuple[DeviceKind, Int]:
        """Returns the (device_kind, device_id) for the current thread."""
        ...

    @staticmethod
    fn list_devices() -> List[Tuple[DeviceKind, Int, String]]:
        """Returns all available devices as (kind, id, name) tuples."""
        ...
```

### 6.4 MemoryPool

Device memory pool for allocation reuse, reducing malloc/free overhead.

```mojo
# Depends on: DeviceKind (Layer 1)
# Layer 6

struct MemoryPool:
    """Pooled memory allocator for device memory.

    Caches freed allocations for reuse, amortizing the cost of GPU
    malloc/free calls. Tracks peak usage for memory budgeting.
    """

    fn __init__(inout self, device: DeviceKind, device_id: Int = 0):
        """Create a memory pool for the specified device."""
        ...

    fn alloc(inout self, size_bytes: Int) -> Pointer[UInt8]:
        """Allocate memory from the pool. Reuses a cached block if available."""
        ...

    fn free(inout self, ptr: Pointer[UInt8]):
        """Return memory to the pool for reuse (does not free to the OS/driver)."""
        ...

    fn peak(self) -> Int:
        """Returns the peak memory usage (in bytes) since the last reset."""
        ...

    fn reset(inout self):
        """Free all cached memory back to the OS/driver and reset peak tracking."""
        ...
```

### 6.5 Stream

Compute stream for asynchronous kernel execution and synchronization.

```mojo
# Depends on: CompiledKernel, Tensor (Layer 2, 5)
# Layer 6

struct Stream:
    """A FIFO command queue for asynchronous kernel launches.

    Operations launched on a stream execute in order relative to each
    other, but independently of operations on other streams. Use
    sync() to wait for all operations on a stream to complete.
    """

    fn __init__(inout self, device: DeviceKind, device_id: Int = 0):
        """Create a new compute stream on the specified device."""
        ...

    fn launch(inout self, kernel: CompiledKernel, inputs: List[Tensor], outputs: List[Tensor]):
        """Enqueue a kernel launch on this stream. Returns immediately (asynchronous)."""
        ...

    fn sync(self):
        """Block until all operations on this stream have completed."""
        ...

    fn wait(self, other: Stream):
        """Make this stream wait until all prior operations on `other` have completed.

        Creates a cross-stream dependency without blocking the host.
        """
        ...
```

---

## 7. dlpack/ -- DLPack Interop

Zero-copy tensor exchange with PyTorch, JAX, NumPy, and any DLPack-compliant framework.

```mojo
# Depends on: DType, Tensor (Layer 0-2)
# Layer 2

struct DLDataType:
    """DLPack data type descriptor (mirrors dlpack.h DLDataType)."""

    var code: UInt8       # Type code: 0=int, 1=uint, 2=float, 3=bfloat
    var bits: UInt8       # Number of bits per element
    var lanes: UInt16     # Number of SIMD lanes (usually 1)

struct DLDevice:
    """DLPack device descriptor (mirrors dlpack.h DLDevice)."""

    var device_type: Int32   # kDLCPU=1, kDLCUDA=2, kDLROCM=10, kDLMetal=8
    var device_id: Int32     # Device ordinal

struct DLTensor:
    """DLPack tensor descriptor (mirrors dlpack.h DLTensor).

    Describes a tensor's memory layout without owning the data.
    """

    var data: Pointer[UInt8]         # Pointer to the tensor data
    var device: DLDevice             # Device where data resides
    var ndim: Int32                  # Number of dimensions
    var dtype: DLDataType            # Element data type
    var shape: Pointer[Int64]        # Pointer to shape array (length ndim)
    var strides: Pointer[Int64]      # Pointer to strides array (NULL = contiguous)
    var byte_offset: UInt64          # Byte offset from data pointer to first element

struct DLManagedTensor:
    """DLPack managed tensor with ownership (mirrors dlpack.h DLManagedTensor).

    Includes a deleter function for the producing framework to free
    the tensor when the consumer is done with it.
    """

    var dl_tensor: DLTensor                          # The tensor descriptor
    var manager_ctx: Pointer[UInt8]                   # Opaque context for the producer
    var deleter: fn(Pointer[DLManagedTensor]) -> None # Called when consumer releases the tensor

fn to_dlpack[dtype: DType, *dims: Dim](tensor: Tensor[dtype, *dims]) -> DLManagedTensor:
    """Export a Neutron tensor as a DLPack managed tensor for zero-copy sharing.

    The returned DLManagedTensor borrows the tensor's memory. The tensor
    must remain alive until the consumer calls the deleter.
    """
    ...

fn from_dlpack[dtype: DType, *dims: Dim](dl: DLManagedTensor) -> Tensor[dtype, *dims]:
    """Import a DLPack managed tensor as a Neutron tensor (zero-copy).

    Takes ownership of the DLManagedTensor. Calls the deleter when the
    resulting Tensor is destroyed.
    """
    ...
```

---

## 8. ffi/ -- Foreign Function Interface

C ABI exports for calling Mojo compute from Rust, C, or other languages.

```mojo
# Depends on: Tensor, DLPack (Layer 0-2)
# Layer 7

# --- @export decorator ---

# @export
# Marks a function for export via C ABI. The function signature must use
# only C-compatible types (pointers, integers, floats, structs of these).
#
# Usage:
#   @export
#   fn predict(input_ptr: Pointer[Float32], input_len: Int) -> Pointer[Float32]:
#       ...
#
# The compiler generates a C header file with the exported function signatures.

# --- C-Compatible Type Mappings ---

# | Mojo Type               | C Type                    | Notes                        |
# |------------------------|---------------------------|------------------------------|
# | Int                    | int64_t                   | Platform word-size integer   |
# | Int32                  | int32_t                   |                              |
# | Float32                | float                     |                              |
# | Float64                | double                    |                              |
# | Bool                   | _Bool / bool              |                              |
# | Pointer[T]             | T*                        | Raw pointer                  |
# | Pointer[UInt8]         | uint8_t* / void*          | Opaque data pointer          |
# | DLManagedTensor        | DLManagedTensor           | DLPack struct (ABI-compatible)|
# | String (exported as)   | const char* + int64_t len | Pointer + length pair        |

# Typical FFI exports for Rust integration:

@export
fn neutron_tensor_create(
    dtype: Int32,
    shape_ptr: Pointer[Int64],
    ndim: Int32,
    device: Int32,
) -> Pointer[UInt8]:
    """Create a tensor and return an opaque handle. Caller must call neutron_tensor_free."""
    ...

@export
fn neutron_tensor_free(handle: Pointer[UInt8]):
    """Free a tensor created by neutron_tensor_create."""
    ...

@export
fn neutron_tensor_data_ptr(handle: Pointer[UInt8]) -> Pointer[UInt8]:
    """Get the raw data pointer from a tensor handle."""
    ...

@export
fn neutron_matmul(
    a: Pointer[UInt8],   # Tensor handle
    b: Pointer[UInt8],   # Tensor handle
) -> Pointer[UInt8]:
    """Perform matrix multiplication. Returns a new tensor handle."""
    ...
```

---

## 9. profiling/ -- Performance Instrumentation

### 9.1 Timer

GPU-aware kernel timing.

```mojo
# Depends on: Stream (Layer 6)
# Layer 7

struct Timer:
    """GPU event-based timer for measuring kernel execution time.

    Uses GPU events (CUDA events, Metal timestamps) for accurate
    measurement of device-side execution, unaffected by launch latency.
    """

    fn __init__(inout self, stream: Stream):
        """Create a timer on the given stream."""
        ...

    fn start(inout self):
        """Record a start event on the stream."""
        ...

    fn stop(inout self):
        """Record a stop event on the stream."""
        ...

    fn elapsed_ms(self) -> Float64:
        """Returns elapsed time in milliseconds between start and stop.

        Synchronizes the stream if not already complete.
        """
        ...
```

### 9.2 MemoryTracker

Allocation tracking and peak memory reporting.

```mojo
# Depends on: MemoryPool (Layer 6)
# Layer 7

struct MemoryTracker:
    """Tracks memory allocations, deallocations, and peak usage.

    Wraps the MemoryPool to record a trace of all allocation events
    for post-hoc analysis.
    """

    fn __init__(inout self, pool: MemoryPool):
        """Create a tracker wrapping the given memory pool."""
        ...

    fn current_bytes(self) -> Int:
        """Returns the current total allocated bytes."""
        ...

    fn peak_bytes(self) -> Int:
        """Returns the peak allocated bytes since the tracker was created or last reset."""
        ...

    fn num_allocations(self) -> Int:
        """Returns the total number of allocation calls."""
        ...

    fn reset(inout self):
        """Reset peak tracking counters."""
        ...
```

### 9.3 ProfilingReport

Aggregated profiling data.

```mojo
# Depends on: Timer, MemoryTracker (Layer 7)
# Layer 7

struct ProfilingReport:
    """Aggregated profiling report for a computation session.

    Collects kernel timing, memory usage, and operation counts into a
    structured report for analysis.
    """

    var kernel_times: Dict[String, List[Float64]]  # Kernel name -> list of execution times (ms)
    var peak_memory_bytes: Int                      # Peak GPU memory usage
    var total_kernel_time_ms: Float64               # Sum of all kernel execution times
    var num_kernel_launches: Int                     # Total kernel launches

    fn add_kernel_time(inout self, kernel_name: String, time_ms: Float64):
        """Record a kernel execution time."""
        ...

    fn summary(self) -> String:
        """Returns a human-readable summary of the profiling data."""
        ...

    fn to_json(self) -> String:
        """Returns profiling data as a JSON string."""
        ...
```

---

## 10. autotune/ -- Auto-Tuning System

Wraps Mojo's native `@adaptive`/`autotune()`/`search()` with persistent caching and cost-model pruning.

```mojo
# Depends on: ComputeBackend, DeviceInfo (Layer 5)
# Layer 7

fn tune[
    KernelFn: AnyType,
    param_space: VariadicList,
](
    kernel: KernelFn,
    evaluator: fn(Pointer[fn() -> None], Int) -> Int,
    cache_key: String = "",
) -> KernelFn:
    """Auto-tune a kernel by searching the parameter space.

    Wraps Mojo's native `search()` with:
    - Persistent caching (keyed by hardware signature + problem size + cache_key)
    - Cost-model pruning to skip obviously bad configurations
    - Results stored in TuneCache for cross-session reuse

    If a cached result exists for the current hardware, returns immediately.
    """
    ...

struct TuneCache:
    """Persistent cache of auto-tuning results.

    Keyed by (hardware_signature, problem_shape, kernel_name). Stored as
    JSON on disk at ~/.neutron_mojo/tune_cache/.
    """

    fn __init__(inout self, cache_dir: String = "~/.neutron_mojo/tune_cache"):
        """Load or create a tune cache at the specified directory."""
        ...

    fn get(self, key: String) -> Optional[Dict[String, Int]]:
        """Look up a cached tuning result. Returns None if not cached."""
        ...

    fn put(inout self, key: String, config: Dict[String, Int]):
        """Store a tuning result in the cache."""
        ...

    fn invalidate(inout self, hardware_signature: String):
        """Remove all cached results for a specific hardware signature."""
        ...

struct CostModel:
    """Analytical cost model for pruning the auto-tune search space.

    Estimates kernel execution time based on operation count, memory
    bandwidth, and compute throughput. Used to eliminate configurations
    that are provably suboptimal before benchmarking.
    """

    fn __init__(inout self, device: DeviceInfo):
        """Create a cost model calibrated to the given device."""
        ...

    fn estimate_time_ms(self, flops: Int, bytes_accessed: Int, occupancy: Float64) -> Float64:
        """Estimate execution time using a roofline model.

        Returns the estimated time in milliseconds based on whether the
        kernel is compute-bound or memory-bound.
        """
        ...

    fn prune(self, candidates: List[Dict[String, Int]], top_k: Int) -> List[Dict[String, Int]]:
        """Prune the candidate list to the top_k most promising configurations.

        Uses the roofline model to rank candidates and discard the rest.
        """
        ...
```

---

## Cross-Reference: Module Dependencies

```
Layer 0: tensor/dtype, tensor/dim, tensor/shape
         (no internal dependencies)

Layer 1: tensor/storage, tensor/view
         layout/layout, layout/swizzle, layout/tile_layout, layout/tensor_core
         (depends on Layer 0)

Layer 2: tensor/tensor, tensor/ops
         dlpack/dlpack, dlpack/convert
         (depends on Layer 0-1)

Layer 3: kernel/kernel, kernel/tile, kernel/schedule,
         kernel/simd_ops, kernel/warp, kernel/block
         (depends on Layer 0-2)

Layer 4: fusion/graph, fusion/fusion, fusion/egraph, fusion/rewrites
         (depends on Layer 0)

Layer 5: backend/backend, backend/nvidia, backend/amd,
         backend/apple, backend/cpu, backend/partitioner
         (depends on Layer 0, 4)

Layer 6: runtime/eager, runtime/compile, runtime/context,
         runtime/memory, runtime/stream
         (depends on Layer 0-5)

Layer 7: ffi/export, ffi/types
         profiling/timer, profiling/memory_tracker, profiling/report
         autotune/tuner, autotune/cache, autotune/cost_model
         (depends on Layer 0-6)
```
