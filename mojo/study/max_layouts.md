# MAX Layouts: Architecture Study

MAX's layout system in `max/kernels/src/layout/` provides compile-time and runtime tensor memory layout abstractions, with particular focus on swizzle patterns that eliminate shared memory bank conflicts on GPUs.

## Layout Trait Design

The foundation is `LayoutTrait`, which any layout type must implement:

```mojo
trait LayoutTrait:
    comptime has_shape: Bool  # Whether this layout defines a shape
    fn __call__(self, idx: IntTuple) -> Int  # Map coordinates to offset
    fn size(self) -> Int      # Total number of elements
    fn cosize(self) -> Int    # Range of output offsets
```

The core `Layout` struct stores shape and stride as `IntTuple` values that can be hierarchically nested. This means a Layout can represent both simple row-major arrays and complex tiled memory organizations in a single unified type.

## Row-Major vs Column-Major Abstractions

Both are factory methods on `Layout`:

```mojo
# Row-major: last dimension varies fastest (C/Python convention)
Layout.row_major(M, N)    # shape=(M,N), stride=(N,1)
Layout.row_major(M, N, K) # shape=(M,N,K), stride=(N*K, K, 1)

# Column-major: first dimension varies fastest (Fortran convention)
Layout.col_major(M, N)    # shape=(M,N), stride=(1,M)
Layout.col_major(M, N, K) # shape=(M,N,K), stride=(1, M, M*N)
```

Both support compile-time (integer literals) and runtime (UNKNOWN_VALUE) dimensions. When dimensions are known at compile time, stride computation is constant-folded.

## Hierarchical Layout Operations

**`zipped_divide(layout, tiler)`**: Divides a layout into tiles. Creates a hierarchical layout where the outer level indexes tiles and the inner level indexes within a tile:

```
# Divide a 128-element vector into 32-element tiles
zipped_divide(Layout.row_major(128), Layout.row_major(32))
# Result: outer shape (4,), inner shape (32,)
```

**`blocked_product(block, base)`**: Combines block and base layouts hierarchically. Each element of the base layout expands into a block:

```
# 4x4 blocks arranged in a 2x3 grid
blocked_product(Layout.row_major(4, 4), Layout.row_major(2, 3))
# Result: 8x12 layout with block structure
```

**`coalesce(layout)`**: Merges contiguous dimensions to reduce rank. Adjacent dimensions with multiplicative strides are combined.

**`composition(layout_a, layout_b)`**: Chains two layouts -- the output of A becomes the input to B.

## Swizzle Patterns for Bank-Conflict Avoidance

The `Swizzle` struct in `swizzle.mojo` implements XOR-based index transformation:

```mojo
struct Swizzle:
    var bits: Int   # Number of bits in the XOR mask
    var base: Int   # Least-significant bits to keep constant
    var shift: Int  # Distance to shift the mask

    fn __call__(self, offset: Int) -> Int:
        return offset ^ shiftr(offset & self.yyy_mask, self.shift)
```

The operation: given index bits `...YYY...ZZZ...`, the result is `...YYY...AAA...` where `AAA = ZZZ ^ YYY`. This XOR remapping distributes sequential accesses across different memory banks.

### Why Swizzling is Needed

Without swizzling, `ldmatrix` operations cause severe bank conflicts. The file includes an excellent ASCII diagram of the problem:

- 32 shared memory banks, each 4 bytes wide (128 bytes per row)
- A naive thread-to-memory mapping for MMA causes 4-way bank conflicts
- Threads T0, T2, T4, T6 all access the same banks when loading 8x4 submatrices

### Swizzle Example: `Swizzle[2, 0, 3]`

```
lane_id bits: xxxxx
              ^^    (extract 2 bits at shift position 3)

00xxx ^ 00: T0  T1  T2  T3  T4  T5  T6  T7   (no change)
01xxx ^ 01: T9  T8  T11 T10 T13 T12 T15 T14  (pairs swapped)
10xxx ^ 10: T18 T19 T16 T17 T22 T23 T20 T21  (quad swapped)
11xxx ^ 11: T27 T26 T25 T24 T31 T30 T29 T28  (both swapped)
```

Each row's threads access different banks, eliminating conflicts.

## Factory Functions for Common Swizzle Patterns

**`make_ldmatrix_swizzle[dtype, row_size]`**: Computes optimal swizzle for NVIDIA's `ldmatrix` instruction:
```mojo
comptime bytes_32_banks = 128
comptime conflict_ways = min(8 * row_size * type_size // bytes_32_banks, 8)
comptime bits = log2_floor(conflict_ways)
comptime shifts = log2_floor(max(row_size // simd_size, 8))
return Swizzle(bits, log2_vector_width, shifts)
```

**`make_swizzle[num_rows, row_size, access_size]`**: General 2D swizzle:
```mojo
comptime bits = log2_floor(num_rows)
comptime base = log2_floor(access_size)
comptime shifts = log2_floor(row_size) - base
return Swizzle(bits, base, shifts)
```

**`make_swizzle[dtype, TensorMapSwizzle]`**: Matches NVIDIA TMA swizzle modes (32B, 64B, 128B, none).

## ComposedLayout: Layout + Swizzle

The `ComposedLayout` struct chains a base layout with a swizzle transformation:

```mojo
struct ComposedLayout[LayoutA: LayoutTrait, LayoutB: LayoutTrait]:
    var layout_a: LayoutA  # Base layout (coordinate -> linear offset)
    var layout_b: LayoutB  # Swizzle transform (linear offset -> swizzled offset)

    fn __call__(self, idx: IntTuple) -> Int:
        return self.layout_b(self.layout_a(idx))
```

This is used extensively in tensor core fragment loading:
```mojo
comptime ldmatrix_layout = ComposedLayout(
    Layout([16, 2], [num_mat_per_row, 1]),  # Base: 16 rows, 2 columns
    swizzle,                                  # Swizzle for bank conflicts
)
var lane_offset = eval_composed[ldmatrix_layout](lane, offset) * simd_size
```

The `eval_composed` function evaluates this at runtime using compile-time-known shapes/strides for the base layout and the swizzle functor for the transform.

## Tile-to-Thread Mapping

Thread distribution uses the `distribute` method on LayoutTensor:

```mojo
# Map 32 warp threads to a 16x8 MMA tile using row-major ordering
var frags = tile.distribute[Layout.row_major(8, 4)](lane_id())
# Each thread gets elements at positions determined by:
#   thread_row = lane_id // 4
#   thread_col = lane_id % 4
```

Different MMA shapes require different thread maps:
- NVIDIA A matrix: `Layout.row_major(8, 4)` -- 8 rows, 4 columns of threads
- NVIDIA B matrix (non-transposed): `Layout.col_major(4, 8)` -- column-first
- AMD A matrix: `Layout.col_major(mma_m, WARP_SIZE // mma_m)` -- wider tiles

## Shared Memory Layout Patterns

MAX's shared memory layouts combine base layout with swizzle:

1. **Global-to-shared copy**: Data is copied into shared memory using the swizzled layout
2. **Shared-to-register load**: Fragment loading applies the same swizzle in reverse to get correct element ordering
3. **Swizzle consistency**: The same swizzle pattern must be applied during both store and load

For TMA (Tensor Memory Accelerator) on SM90+, the swizzle mode is specified at the TMA descriptor level and handled by hardware.

> **Key Takeaway for Neutron**: The most critical pattern is `ComposedLayout` combining a base layout with a `Swizzle` for bank-conflict-free shared memory access. When implementing GPU kernels, always compute the swizzle parameters based on the data type width, row size, and access pattern (ldmatrix vs. general). The hierarchical `Layout` with `zipped_divide` and `blocked_product` is powerful for expressing tiled algorithms but adds complexity -- start with explicit `tile()` calls and graduate to hierarchical layouts when the abstraction pays off. The `distribute` method for thread-to-element mapping is cleaner than manual index arithmetic and should be adopted early.
