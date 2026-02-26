# MAX Flash Attention: Architecture Study

MAX implements Flash Attention as a CPU-optimized tiled attention kernel in `max/kernels/src/nn/flash_attention.mojo`, with separate GPU dispatch paths in `max/kernels/src/nn/mha.mojo` that route to SM90/SM100/AMD-specific implementations.

## Overall Architecture

The CPU Flash Attention lives in a single `_FlashAttention` struct that is heavily parameterized with callback functions for Q/K/V access, masking, and output writing. This design allows the same core algorithm to work with both dense tensors and KV caches without branching at runtime.

The struct is parameterized on:
- `dtype`, `rank` -- data type and tensor rank
- `input_q_ptr_fn` -- returns a raw pointer to Q at given coordinates
- `input_k_fn`, `input_v_fn` -- lambda-style accessors for K and V
- `mask_fn` -- score modification callback (additive mask, causal, etc.)
- `output_ptr_fn` -- returns writable pointer for output
- `q_length_fn`, `kv_length_fn`, `kv_cache_length_fn` -- per-batch length functions

This callback-based design means the same kernel handles dense attention, KV cache attention, ragged batches, and split-KV (separate cache + new tokens) without code duplication.

## Tiling Strategy

MAX uses a three-level tiling scheme controlled by `_FlashAttentionConfig`:

1. **Block M** (`block_m`): Number of Q rows per iteration. Constrained to max 64, computed as `output_target_size / o_block_n`, aligned down to multiples of 4.
2. **QK Block N** (`qk_block_n`): KV sequence tile size, fixed at 128. This is how many K/V tokens are processed per inner loop iteration.
3. **O Block N** (`o_block_n`): Output depth tile size. When the head dimension is statically known and <= 256, this matches it exactly. Otherwise defaults to 128.

The target output block size is 8192 elements (block_m * o_block_n), which keeps the working set in L1/L2 cache.

```
# Work decomposition:
num_blocks_m = ceildiv(max_seq_len, block_m)
num_blocks_n = ceildiv(depth_dim, o_block_n)
work_count = num_batches * num_heads * num_blocks_m * num_blocks_n
```

Work is distributed across threads using `sync_parallelize` with `partition_work` for load balancing.

## Online Softmax Implementation

The `_online_softmax` method implements the two-pass online softmax from Milakov & Gimelshein:

**Pass 1 (Scale + Mask + Max):**
- Applies the scale factor and mask function to QK scores
- Finds the running maximum using `map_reduce` with `_simd_max_elementwise`
- Stores scaled+masked scores back to the QK block

**Pass 2 (Exp + Sum):**
- Computes `exp(score - max_val)` for numerical stability
- Accumulates the sum using `map_reduce` with `_simd_sum_elementwise`
- Stores exponentiated values back

**Fixup (Rescaling previous output):**
- Computes `fixup_val = exp(old_max - new_max)` to correct previous iterations
- Rescales the output accumulator: `o_row *= fixup_val`
- Updates running max and sum: `sum = sum * fixup_val + new_sum`

After all KV blocks are processed, the final normalization divides each output row by its accumulated sum.

## Score Modification Callbacks (Masking)

Masking is handled through a generic callback pattern, not hardcoded logic:

```mojo
mask_fn: fn[simd_width: Int, mask_rank: Int](
    idx: IndexList[mask_rank],
    score_vec: SIMD[dtype, simd_width],
    kv_cache_length: Int,
) capturing -> SIMD[dtype, simd_width]
```

The caller provides this function. Common implementations include:
- **Additive mask**: `score_vec + mask_tensor.load(idx)` -- general purpose
- **Causal mask (MHAMask)**: Shifts indices from local to global space using `kv_cache_length`, then delegates to `mask.mask()` which applies `-inf` for invalid positions
- **No mask**: Identity function returning `score_vec` unchanged

The `kv_cache_length` parameter enables correct causal masking when using KV caches -- the mask function shifts the sequence index from local (within current tokens) to global (including cached history).

## Variable Sequence Length Handling

MAX handles variable-length sequences through three mechanisms:

1. **Per-batch length functions**: `q_length_fn(batch)` and `kv_length_fn(batch)` return the actual sequence length for each batch item. Work items with `m >= seq_len` are skipped via `continue`.

2. **Ragged tensor support**: The `flash_attention_kv_cache` overload accepts `q_input_row_offsets` and `kv_input_row_offsets` LayoutTensors. The pointer functions (`input_q_ptr_fn`, `output_ptr_fn`) compute flat indices using these offsets.

3. **Split-KV variant**: `flash_attention_split_kv` handles the case where previous KV cache and current K/V tensors are separate. A `load_from_split_cache` wrapper transparently routes loads: `if seq_idx >= kv_cache_len: load from current tensor; else: load from cache`.

## Block Size Selection

The `_FlashAttentionConfig` struct auto-tunes block sizes at compile time:

```mojo
# Target 8KB output block
output_target_size = 8192

# If depth is statically known and small, match it exactly
if depth_static_dim != UNKNOWN_VALUE:
    o_block_n = align_up(min(depth_dim, 256), simd_width)

# Compute M tile to fill target
block_m = align_down(output_target_size // o_block_n, 4)
block_m = min(max(block_m, 1), 64)
```

For M=1 (single-query decoding), the kernel falls through to a specialized GEMV path that avoids the packed buffer allocation entirely.

## CPU Matmul Integration

QK and OV matmuls reuse the generic `_Matmul` struct which handles:
- **GEMV path**: When M=1, uses `_gemv_transposed` (for QK^T) or `_gemv` (for softmax(QK)*V)
- **Packed matmul path**: For M>1, transposes/packs B into a contiguous buffer, then calls `_matmul_packed` with tiled accumulators
- **Apple Accelerate**: On macOS, optionally delegates to `_cblas_f32`

The K transpose is handled by the `transpose_b=True` parameter, which uses `_pack_buffer_transposed` with an optimized 4x4 transpose kernel.

## Attention Sink Support

MAX includes optional "attention sink" support (for streaming inference). When `sink_weights` is provided:
- A per-head `sink_logit` is added to the max computation
- An extra `exp(sink_logit - max_val)` term is added to the softmax denominator

> **Key Takeaway for Neutron**: The callback-based design for Q/K/V access, masking, and output is the most important pattern to adopt. Rather than building separate kernels for dense attention, KV cache attention, and ragged batches, a single parameterized kernel with lambda accessors handles all cases. The online softmax with fixup-based rescaling is the standard algorithm. For CPU, the automatic GEMV fallback for M=1 (token generation) is critical for inference performance. For GPU, MAX dispatches to completely separate hardware-specific kernels (SM90, SM100, AMD) rather than trying to make one kernel work everywhere.
