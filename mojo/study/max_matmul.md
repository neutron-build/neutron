# MAX Matmul: Architecture Study

MAX implements GPU matrix multiplication through a multi-stage persistent kernel architecture in `max/kernels/src/linalg/matmul/gpu/`, with separate backends for SM80, SM90, SM100, and AMD, plus a unified tile scheduler and tensor core abstraction layer.

## Tensor Core Integration

The `TensorCore` struct in `max/kernels/src/layout/tensor_core.mojo` abstracts hardware MMA instructions across NVIDIA and AMD:

```mojo
struct TensorCore[
    out_type: DType,     # float32 accumulation
    in_type: DType,      # float16/bf16/fp8 inputs
    shape: IndexList[3], # e.g., [16, 8, 16] for M, N, K
    transpose_b: Bool = False,
]
```

Supported shapes per hardware:
- **NVIDIA SM80+**: 16x8x8 (f32), 16x8x16 (f16/bf16), 16x8x32 (fp8), 8x8x4 (f64)
- **AMD CDNA**: 16x16x4 (f32), 16x16x16 (f16/bf16), 16x16x32 (fp8)
- **AMD RDNA3+**: 16x16x16 (f16/bf16)

The struct provides `load_a()`, `load_b()`, `load_c()`, `mma_op()`, and `store_d()` methods that handle the hardware-specific register layout differences.

## MMA (Matrix Multiply Accumulate) Wrappers

Fragment loading for NVIDIA uses `ldmatrix` instructions through `_load_matrix_frag`:

```mojo
# Thread-to-matrix mapping for ldmatrix
comptime ldmatrix_threadmap = Layout.col_major(16, 2)

# 4 submatrices arranged for conflict-free access
comptime x4_layout = Layout([16, 2], [num_mat_per_row, 1])

# Composed with swizzle for bank-conflict avoidance
comptime ldmatrix_layout = ComposedLayout(x4_layout, swizzle)
```

For AMD, fragment loading uses `distribute` with warp layouts:
```mojo
comptime warp_layout = Layout.col_major(mma_m, WARP_SIZE // mma_m)
var a_reg_frags = a.vectorize[1, simd_width]().distribute[
    warp_layout, swizzle=swizzle
](lane_id())
```

The `TiledTensorCore` struct extends this for multi-step K accumulation, decomposing larger K tiles into multiple MMA calls.

## Persistent Kernel Pattern

The GPU matmul uses persistent thread blocks that process multiple output tiles:

1. **Grid launch**: Fixed number of blocks (often matching SM count * occupancy)
2. **Tile scheduler**: Each block fetches the next tile to process via `TileScheduler.fetch_next_work()`
3. **Loop**: Blocks continue processing tiles until the scheduler reports no more work

This avoids the overhead of launching one block per output tile and enables better load balancing across SMs.

## Tile Scheduling

`TileScheduler` in `tile_scheduler.mojo` supports multiple strategies:

- **TILE1D**: Simple linear mapping of block index to output tile
- **TILE2D**: 2D rasterization with configurable raster order (AlongM or AlongN)
- **DS_SCHEDULER**: DeepSeek-style scheduling for MoE workloads

The scheduler produces `WorkInfo` containing (m, n) output coordinates, K-dimension split info, and a validity flag.

**Swizzled block ordering** improves L2 cache hit rates:
```mojo
fn _get_swizzled_block_idx(self) -> UInt:
    # Maps linear block index to 2D with spatial locality
    # Tiles that share K-dimension data are scheduled on nearby SMs
```

## Register Blocking

The kernel maintains double-buffered register tiles for A and B fragments:

```
a_reg_tiles: 2 * k_group_size * num_m_mmas x a_frag_size
b_reg_tiles: 2 * k_group_size * num_n_mmas x b_frag_size
```

The factor of 2 enables prefetching the next iteration's fragments into one register set while computing with the other. `k_group_size` is the number of MMA K-tiles loaded together (typically 1-2, but can be higher on AMD for efficiency with smaller warp widths).

The accumulator uses `c_reg_tile` sized as `num_m_mmas * num_n_mmas x c_frag_size`, accumulating across all K iterations before writing back.

## Shared Memory Double-Buffering

The kernel allocates multiple pipeline stages in shared memory:

```mojo
comptime a_smem_size = num_pipeline_stages * BM * BK
comptime b_smem_size = num_pipeline_stages * BK * BN
```

Typical `num_pipeline_stages` is 2-4, enabling overlap of:
- **Stage N+2**: Async copy from global to shared memory
- **Stage N+1**: Wait for copy completion
- **Stage N**: Load from shared to registers + compute MMA

The circular buffer is managed by `LayoutTensorIter` which tracks position and wraps around:
```mojo
var a_smem = LayoutTensorIter[...](a_smem_ptr, a_smem_size)
var smem_tile = a_smem.next(stage_idx)[]
```

Async copies use `copy_dram_to_sram_async` on NVIDIA (cp.async) with barrier-based synchronization.

## Layout Requirements for Tensor Cores

Fragment registers follow specific thread-to-element mappings:

**NVIDIA SM80 (16x8x16, bf16):**
- A fragments: 8x4 thread layout (row_major), 2 elements per thread vectorized
- B fragments: 4x8 thread layout (col_major), 2 elements per thread vectorized
- C fragments: 8x4 thread layout (row_major), 2 elements per thread vectorized

**AMD CDNA (16x16x16, bf16):**
- A fragments: col_major(16, WARP_SIZE/16), vectorized in groups of k_group_size
- B fragments: row_major(WARP_SIZE/16, 16) or col_major(16, WARP_SIZE/16) for transpose
- C fragments: row_major(mma_m/reg_per_thread, mma_n), 4-element vectorization

The `TensorCore.load_b` method includes a specialized path for int4 dequantization that unpacks two 4-bit values from each int8, applies scale factors, and produces bf16 fragments -- fusing dequantization directly into the MMA pipeline.

## Split-K Pattern

For tall-skinny matrices (large K, small M*N), the kernel uses Split-K:

1. Partition K dimension across multiple blocks
2. Each block computes partial results into a workspace buffer
3. A reduction kernel (`warp_split_k_reduction`) combines partial results using shared memory tree reduction

The `tile_scheduler_splitk.mojo` variant handles K-partitioned work distribution.

## GPU-Specific Dispatch

The matmul init dispatches to architecture-specific kernels:
- `sm80/`: Baseline CUDA tensor core kernels
- `sm90/`: Hopper-specific with TMA (Tensor Memory Accelerator) and warpgroup MMA
- `sm100/`: Blackwell-specific with UMMA and NVFP4 support
- `amd/`: CDNA/RDNA paths with architecture-aware fragment loading

> **Key Takeaway for Neutron**: The most important patterns to adopt are: (1) The `TensorCore` abstraction that hides hardware MMA differences behind `load_a/load_b/mma_op/store_d` -- this is essential for multi-GPU-vendor support. (2) The persistent kernel + tile scheduler pattern for matmul. (3) Double-buffered shared memory with async copy overlap. (4) The register double-buffering for fragment prefetch. For initial implementation, start with the SM80 path (most portable) and add SM90/SM100 specializations later. The split-K pattern is important for inference where batch sizes are small.
