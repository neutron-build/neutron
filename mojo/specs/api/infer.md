# neutron-mojo-infer Package API Specification

> **Status:** Pseudocode specification -- NOT compilable Mojo
> **Package:** `neutron-mojo-infer`
> **License:** Apache 2.0
> **Depends on:** `neutron-mojo` (core)
> **Last updated:** 2026-02-16

This document defines every public trait, struct, and function exposed by the `neutron-mojo-infer` package. This package provides LLM inference serving, including FlashAttention variants, KV cache management, quantization, continuous batching, model loading, and a standalone server with OpenAI-compatible API.

---

## 1. attention/ -- FlashAttention Variants

### 1.1 flash_attention

Core FlashAttention-2 implementation with IO-aware tiling.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType, neutron_mojo.tensor.Dim
# Depends on: neutron_mojo.kernel (@kernel, tiles[], fma)
# Depends on: neutron_mojo.layout (TileLayout, swizzle)

fn flash_attention[
    dtype: DType,
    B: Dim,            # Batch
    H: Dim,            # Number of query heads
    S_q: Dim,          # Query sequence length
    S_kv: Dim,         # Key/value sequence length
    D: Dim,            # Head dimension
](
    q: Tensor[dtype, B, H, S_q, D],
    k: Tensor[dtype, B, H, S_kv, D],
    v: Tensor[dtype, B, H, S_kv, D],
    causal: Bool = False,
    scale: Optional[Float32] = None,
    block_q: Int = 128,
    block_kv: Int = 64,
) -> Tensor[dtype, B, H, S_q, D]:
    """FlashAttention-2: IO-aware exact attention with tiled computation.

    Computes softmax(Q @ K^T / scale) @ V without materializing the
    full S_q x S_kv attention matrix in HBM. Uses online softmax for
    numerical stability. Tiled to fit in SRAM with configurable block sizes.

    When causal=True, applies a causal mask (lower-triangular) without
    materializing the mask matrix. Scale defaults to 1/sqrt(D) if not provided.

    Target: 90%+ of hand-tuned CUDA (beating Triton's 78-82% ceiling).
    """
    ...
```

### 1.2 grouped_query_attention

Grouped Query Attention for models like Llama 3 where num_kv_heads < num_q_heads.

```mojo
# Depends on: flash_attention, Tensor, DType, Dim

fn grouped_query_attention[
    dtype: DType,
    B: Dim,
    H_q: Dim,          # Number of query heads
    H_kv: Dim,         # Number of KV heads (H_q must be divisible by H_kv)
    S_q: Dim,
    S_kv: Dim,
    D: Dim,
](
    q: Tensor[dtype, B, H_q, S_q, D],
    k: Tensor[dtype, B, H_kv, S_kv, D],
    v: Tensor[dtype, B, H_kv, S_kv, D],
    causal: Bool = False,
    scale: Optional[Float32] = None,
    block_q: Int = 128,
    block_kv: Int = 64,
    score_mod: Optional[ScoreMod] = None,
) -> Tensor[dtype, B, H_q, S_q, D]:
    """Grouped Query Attention (GQA) with head ratio broadcasting.

    Each KV head is shared across H_q/H_kv query heads. More memory-efficient
    than full multi-head attention with minimal quality loss.

    Optionally applies a ScoreMod function to attention scores before softmax
    (e.g., ALiBi positional bias, relative position encoding).
    """
    ...
```

### 1.3 sliding_window_attention

Sliding window attention for models like Mistral that limit attention span.

```mojo
# Depends on: flash_attention, Tensor, DType, Dim

fn sliding_window_attention[
    dtype: DType,
    B: Dim,
    H: Dim,
    S_q: Dim,
    S_kv: Dim,
    D: Dim,
](
    q: Tensor[dtype, B, H, S_q, D],
    k: Tensor[dtype, B, H, S_kv, D],
    v: Tensor[dtype, B, H, S_kv, D],
    window_size: Int,
    causal: Bool = True,
    scale: Optional[Float32] = None,
    block_q: Int = 128,
    block_kv: Int = 64,
) -> Tensor[dtype, B, H, S_q, D]:
    """Sliding window attention with bounded context.

    Each query token attends only to the `window_size` most recent KV tokens.
    Reduces computation from O(S^2) to O(S * window_size). Uses block-sparse
    tiling to skip tiles that fall entirely outside the window.
    """
    ...
```

### 1.4 ScoreMod Trait

Pluggable attention score modification.

```mojo
# Depends on: Tensor, DType, Dim

trait ScoreMod:
    """Modifier applied to raw attention scores (Q @ K^T) before softmax.

    Enables ALiBi, relative position encoding, or custom attention biases
    without modifying the core attention kernel.
    """

    fn apply[dtype: DType, B: Dim, H: Dim, S_q: Dim, S_kv: Dim](
        self,
        scores: Tensor[dtype, B, H, S_q, S_kv],
        query_pos: Int,
        key_pos: Int,
    ) -> Tensor[dtype, B, H, S_q, S_kv]:
        """Apply the score modification in-place or return modified scores.

        Called per-block during tiled attention computation.
        """
        ...
```

---

## 2. kv_cache/ -- KV Cache Management

### 2.1 KVCache Trait

Common interface for all KV cache implementations.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType

trait KVCache:
    """Interface for key-value cache storage used during autoregressive generation.

    Implementations manage memory allocation, eviction, and retrieval
    of cached KV pairs across generation steps.
    """

    fn append[dtype: DType](
        inout self,
        layer_idx: Int,
        keys: Tensor[dtype],
        values: Tensor[dtype],
        sequence_ids: List[Int],
    ):
        """Append new key-value pairs for the given sequences at the specified layer.

        Called once per generation step per layer. The cache manages
        memory allocation for the new tokens.
        """
        ...

    fn get[dtype: DType](
        self,
        layer_idx: Int,
        sequence_id: Int,
    ) -> Tuple[Tensor[dtype], Tensor[dtype]]:
        """Retrieve all cached keys and values for a sequence at a layer.

        Returns (keys, values) tensors covering all tokens cached so far
        for this sequence.
        """
        ...

    fn evict(inout self, sequence_id: Int):
        """Evict all cached data for a sequence (e.g., when generation completes).

        Frees the physical memory blocks back to the allocator.
        """
        ...

    fn memory_usage(self) -> Int:
        """Returns the current total memory usage of the cache in bytes."""
        ...
```

### 2.2 PagedKVCache

Virtual-memory-paged KV cache inspired by vLLM's PagedAttention.

```mojo
# Depends on: KVCache, Tensor, DType
# Depends on: BlockAllocator

struct PagedKVCache(KVCache):
    """Paged KV cache using virtual memory blocks (vLLM-style PagedAttention).

    Maps logical KV positions to physical memory blocks via a block table.
    Eliminates memory fragmentation and enables fine-grained sharing of
    KV cache between sequences with common prefixes.

    Block size is configurable (default 16 tokens per block). Smaller blocks
    reduce waste but increase block table overhead.
    """

    var block_size: Int                  # Tokens per physical block (default 16)
    var num_layers: Int                  # Number of transformer layers
    var num_heads: Int                   # Number of KV heads
    var head_dim: Int                    # Dimension per head
    var allocator: BlockAllocator        # Physical block pool

    fn __init__(
        inout self,
        num_layers: Int,
        num_heads: Int,
        head_dim: Int,
        max_blocks: Int,
        block_size: Int = 16,
        dtype: DType = DType.float16,
    ):
        """Initialize the paged cache with a fixed physical block pool."""
        ...

    fn append[dtype: DType](inout self, layer_idx: Int, keys: Tensor[dtype], values: Tensor[dtype], sequence_ids: List[Int]): ...
    fn get[dtype: DType](self, layer_idx: Int, sequence_id: Int) -> Tuple[Tensor[dtype], Tensor[dtype]]: ...
    fn evict(inout self, sequence_id: Int): ...
    fn memory_usage(self) -> Int: ...

    fn num_free_blocks(self) -> Int:
        """Returns the number of unallocated physical blocks."""
        ...

    fn can_allocate(self, num_tokens: Int) -> Bool:
        """Returns True if enough free blocks exist for num_tokens."""
        ...
```

### 2.3 RadixKVCache

Prefix-tree-based KV cache inspired by SGLang's RadixAttention.

```mojo
# Depends on: KVCache, Tensor, DType
# Depends on: BlockAllocator

struct RadixKVCache(KVCache):
    """Radix tree KV cache for prefix reuse (SGLang-style RadixAttention).

    Organizes cached KV pairs in a radix tree (prefix tree) keyed by token
    sequences. Sequences with shared prefixes share cached KV blocks,
    avoiding redundant computation. State-of-the-art for multi-turn
    chat and prompt-heavy workloads.

    When a new prompt shares a prefix with an existing cached sequence,
    only the new suffix tokens need attention computation.
    """

    var allocator: BlockAllocator
    var num_layers: Int
    var num_heads: Int
    var head_dim: Int

    fn __init__(
        inout self,
        num_layers: Int,
        num_heads: Int,
        head_dim: Int,
        max_blocks: Int,
        block_size: Int = 16,
        dtype: DType = DType.float16,
    ):
        """Initialize the radix cache with a physical block pool."""
        ...

    fn append[dtype: DType](inout self, layer_idx: Int, keys: Tensor[dtype], values: Tensor[dtype], sequence_ids: List[Int]): ...
    fn get[dtype: DType](self, layer_idx: Int, sequence_id: Int) -> Tuple[Tensor[dtype], Tensor[dtype]]: ...
    fn evict(inout self, sequence_id: Int): ...
    fn memory_usage(self) -> Int: ...

    fn prefix_match(self, token_ids: List[Int]) -> Int:
        """Returns the number of prefix tokens already cached for this token sequence.

        The caller can skip attention computation for these tokens and start
        from the first uncached position.
        """
        ...

    fn insert_prefix(inout self, token_ids: List[Int], sequence_id: Int):
        """Register a token sequence in the radix tree, sharing blocks with matching prefixes."""
        ...
```

### 2.4 CacheManager

High-level cache lifecycle management with eviction policies and memory budgeting.

```mojo
# Depends on: KVCache, PagedKVCache, RadixKVCache

struct CacheManager:
    """Manages KV cache lifecycle: allocation, eviction, and memory budgeting.

    Wraps a KVCache implementation with eviction policies (LRU, priority-based)
    and enforces a memory budget. Decides which sequences to evict when memory
    pressure is high.
    """

    var cache: KVCache                # Underlying cache implementation
    var memory_budget_bytes: Int      # Maximum allowed cache memory
    var eviction_policy: String       # "lru", "priority", or "prefix_aware"

    fn __init__(
        inout self,
        cache: KVCache,
        memory_budget_bytes: Int,
        eviction_policy: String = "lru",
    ):
        """Create a cache manager with the given budget and eviction policy."""
        ...

    fn allocate_for_sequence(inout self, sequence_id: Int, num_tokens: Int) -> Bool:
        """Attempt to allocate cache space for a new sequence.

        Returns True if allocation succeeded. If memory is insufficient,
        evicts sequences according to the eviction policy until space is
        available or no more evictions are possible.
        """
        ...

    fn release_sequence(inout self, sequence_id: Int):
        """Release all cache resources for a completed sequence."""
        ...

    fn active_sequences(self) -> List[Int]:
        """Returns the sequence IDs of all currently cached sequences."""
        ...
```

### 2.5 BlockAllocator

Physical memory block pool underlying all KV cache implementations.

```mojo
# Depends on: neutron_mojo.runtime.MemoryPool

struct BlockAllocator:
    """Pool of fixed-size physical memory blocks for KV cache storage.

    Pre-allocates a fixed pool of blocks at initialization. Allocation
    and deallocation are O(1) via a free list.
    """

    var block_size_bytes: Int     # Size of each block in bytes
    var total_blocks: Int         # Total blocks in the pool
    var free_blocks: Int          # Currently unallocated blocks

    fn __init__(inout self, block_size_bytes: Int, total_blocks: Int, device: DeviceKind):
        """Allocate the block pool on the specified device."""
        ...

    fn alloc(inout self) -> Optional[Int]:
        """Allocate a physical block. Returns block index, or None if pool is exhausted."""
        ...

    fn free(inout self, block_idx: Int):
        """Return a block to the free list."""
        ...

    fn alloc_n(inout self, n: Int) -> Optional[List[Int]]:
        """Allocate n contiguous blocks. Returns block indices, or None if insufficient blocks."""
        ...

    fn free_n(inout self, block_indices: List[Int]):
        """Return multiple blocks to the free list."""
        ...
```

---

## 3. quantization/ -- Quantized Inference

### 3.1 QuantType and QuantConfig

Quantization type enumeration and configuration.

```mojo
# Depends on: neutron_mojo.tensor.DType

struct QuantType:
    """Enumerates supported quantization methods."""

    alias NONE: QuantType              # No quantization (full precision)
    alias Q4_K_M: QuantType            # GGUF Q4_K_M: 256-element super-blocks, 32-element sub-blocks
    alias Q5_K_M: QuantType            # GGUF Q5_K_M: 5-bit with K-quant structure
    alias Q8_0: QuantType              # GGUF Q8_0: simple 8-bit block quantization
    alias NF4: QuantType               # Normal Float 4-bit (bitsandbytes-style)
    alias GPTQ_4BIT: QuantType         # GPTQ 4-bit group quantization
    alias AWQ_4BIT: QuantType          # AWQ 4-bit activation-aware quantization
    alias FP8_E4M3: QuantType          # FP8 with 4-bit exponent, 3-bit mantissa
    alias FP8_E5M2: QuantType          # FP8 with 5-bit exponent, 2-bit mantissa
    alias EXL2: QuantType              # EXL2 mixed-precision per-layer bitrates

struct QuantConfig:
    """Configuration for quantized model loading and inference."""

    var quant_type: QuantType          # Primary quantization method
    var group_size: Int                # Elements per quantization group (default 128)
    var has_zero_point: Bool           # Whether zero-point offsets are stored
    var bits_per_weight: Float32       # Average bits per weight (useful for EXL2 mixed-precision)

    # EXL2 mixed-precision settings
    var attention_bits: Optional[Int]  # Bit budget for attention layers (e.g., 6)
    var ffn_bits: Optional[Int]        # Bit budget for FFN layers (e.g., 3)
    var embedding_bits: Optional[Int]  # Bit budget for embedding layers (e.g., 8)

    fn __init__(
        inout self,
        quant_type: QuantType,
        group_size: Int = 128,
        has_zero_point: Bool = False,
    ):
        """Create a quantization config."""
        ...

    fn estimated_model_size_bytes(self, num_parameters: Int) -> Int:
        """Estimate the model size in bytes given parameter count and quantization settings."""
        ...
```

### 3.2 GGUF Dequantization

Block-structured dequantization for GGUF quantized weights.

```mojo
# Depends on: Tensor, DType, QuantType, @kernel

fn gguf_dequant[
    dtype: DType,
    quant_type: QuantType,
](
    block_data: Tensor[DType.uint8],
    scale: Tensor[DType.float16],
    output_shape: Shape,
) -> Tensor[dtype]:
    """Dequantize GGUF-format quantized blocks to floating point.

    Supports Q4_K_M (256-element super-blocks with 32-element sub-blocks),
    Q5_K_M, and Q8_0 formats. Uses GPU kernels for high-throughput
    dequantization during inference.

    The block_data tensor contains raw quantized bytes in GGUF layout.
    Scale contains per-block scale factors.
    """
    ...
```

### 3.3 NF4 Quantize/Dequantize

Normal Float 4-bit quantization (bitsandbytes-style).

```mojo
# Depends on: Tensor, DType, @kernel

fn nf4_quantize(
    weights: Tensor[DType.float32],
    block_size: Int = 64,
) -> Tuple[Tensor[DType.uint8], Tensor[DType.float32]]:
    """Quantize weights to NF4 (Normal Float 4-bit) format.

    NF4 uses a fixed lookup table of 16 values that are information-theoretically
    optimal for normally-distributed weights. No calibration data required.

    Returns (quantized_data, absmax_scales) where absmax_scales has one
    entry per block_size elements.
    """
    ...

fn nf4_dequantize(
    quantized: Tensor[DType.uint8],
    absmax: Tensor[DType.float32],
    block_size: Int = 64,
    output_dtype: DType = DType.float16,
) -> Tensor:
    """Dequantize NF4 data back to floating point using the lookup table and absmax scales."""
    ...
```

### 3.4 FP8 Quantize/Dequantize

8-bit floating-point quantization for Hopper+ and MI300+ GPUs.

```mojo
# Depends on: Tensor, DType, @kernel

fn fp8_quantize[
    variant: DType,  # DType.float8_e4m3 or DType.float8_e5m2
](
    tensor: Tensor[DType.float16],
    per_tensor_scale: Bool = True,
) -> Tuple[Tensor[variant], Tensor[DType.float32]]:
    """Quantize a FP16 tensor to FP8 format.

    E4M3 (4-bit exponent, 3-bit mantissa) is preferred for inference.
    E5M2 (5-bit exponent, 2-bit mantissa) is preferred for training gradients.

    Returns (fp8_tensor, scale_factor).
    """
    ...

fn fp8_dequantize[
    variant: DType,
](
    fp8_tensor: Tensor[variant],
    scale: Tensor[DType.float32],
    output_dtype: DType = DType.float16,
) -> Tensor:
    """Dequantize FP8 data back to FP16 or FP32."""
    ...
```

### 3.5 EXL2 Mixed Precision

Per-layer bitrate allocation inspired by ExLlamaV2.

```mojo
# Depends on: Tensor, DType, QuantConfig

fn exl2_mixed_precision(
    weights: Dict[String, Tensor[DType.float16]],
    config: QuantConfig,
    calibration_data: Optional[Tensor] = None,
) -> Dict[String, Tuple[Tensor[DType.uint8], QuantConfig]]:
    """Apply EXL2-style mixed-precision quantization across model layers.

    Allocates different bit budgets to different layers based on their
    importance. Attention layers typically get more bits (e.g., 6-bit)
    than FFN layers (e.g., 3-bit). Embedding layers get the most (e.g., 8-bit).

    If calibration_data is provided, uses activation-aware importance
    scoring (AWQ-style) to determine per-layer budgets automatically.

    Returns a dict mapping layer names to (quantized_data, layer_config).
    """
    ...
```

### 3.6 GPU Dequantization Kernel

High-throughput GPU dequantization kernel (Marlin-equivalent performance target).

```mojo
# Depends on: Tensor, DType, QuantType, @kernel, tiles[], SIMD

@kernel
fn dequant_kernel_gpu[
    quant_type: QuantType,
    output_dtype: DType,
](
    quantized: Tensor[DType.uint8],
    scales: Tensor[DType.float16],
    zeros: Optional[Tensor[DType.float16]],
    output: Tensor[output_dtype],
):
    """GPU kernel for high-throughput weight dequantization.

    Performance target: Marlin-equivalent (AWQ without Marlin: 67 tok/s;
    with Marlin: 741 tok/s -- 10x difference). The kernel matters more
    than the quantization algorithm.

    Supports all QuantType variants. Fuses dequantization with the subsequent
    matmul when possible to avoid materializing full-precision weights in HBM.
    """
    ...
```

### 3.7 AWQ Calibration

Activation-aware weight quantization calibration.

```mojo
# Depends on: Tensor, DType, QuantConfig

fn calibrate_awq(
    model_weights: Dict[String, Tensor[DType.float16]],
    calibration_data: Tensor,
    num_samples: Int = 128,
    group_size: Int = 128,
) -> Dict[String, Tensor[DType.float32]]:
    """Compute AWQ importance scores for model weights using calibration data.

    Scores weight channels by their activation magnitude (not weight
    magnitude). Channels with high activation response are protected
    from aggressive quantization. MLSys 2024 Best Paper technique.

    Returns a dict mapping layer names to per-channel importance scores.
    """
    ...
```

---

## 4. batching/ -- Request Batching

### 4.1 Request

Represents a single inference request.

```mojo
# Depends on: nothing (data type only)

struct Request:
    """A single inference request with prompt tokens and generation parameters."""

    var id: Int                        # Unique request ID
    var prompt_tokens: List[Int]       # Tokenized input prompt
    var max_new_tokens: Int            # Maximum tokens to generate
    var temperature: Float32           # Sampling temperature (0.0 = greedy)
    var top_k: Int                     # Top-k sampling parameter (0 = disabled)
    var top_p: Float32                 # Top-p (nucleus) sampling parameter (1.0 = disabled)
    var stop_sequences: List[List[Int]] # Token sequences that stop generation
    var priority: Int                  # Scheduling priority (higher = more urgent)
    var arrival_time_ns: Int           # Timestamp for latency tracking

    fn __init__(
        inout self,
        prompt_tokens: List[Int],
        max_new_tokens: Int = 256,
        temperature: Float32 = 1.0,
        top_k: Int = 0,
        top_p: Float32 = 1.0,
    ):
        """Create a new inference request."""
        ...

    fn is_prefill(self) -> Bool:
        """Returns True if this request hasn't started decoding yet."""
        ...
```

### 4.2 ContinuousBatcher

Iteration-level batching that packs prefill and decode requests together.

```mojo
# Depends on: Request, KVCache
# Depends on: neutron_mojo.runtime.Stream

struct ContinuousBatcher:
    """Continuous batching engine for LLM inference.

    Unlike static batching (which waits for all sequences in a batch to finish),
    continuous batching schedules requests at the iteration level: completed
    sequences are immediately replaced with new ones, maximizing GPU utilization.

    Handles both prefill (processing the full prompt) and decode (generating
    one token at a time) phases within the same batch.
    """

    var max_batch_size: Int           # Maximum sequences in a single batch
    var max_tokens_per_batch: Int     # Token budget per iteration
    var cache: KVCache                # KV cache for active sequences

    fn __init__(
        inout self,
        max_batch_size: Int,
        max_tokens_per_batch: Int,
        cache: KVCache,
    ):
        """Create a continuous batcher with the given capacity."""
        ...

    fn add_request(inout self, request: Request) -> Bool:
        """Enqueue a request for processing. Returns False if the queue is full."""
        ...

    fn step(inout self) -> List[Tuple[Int, List[Int]]]:
        """Execute one iteration: run attention for all active sequences, return newly generated tokens.

        Returns a list of (request_id, new_tokens) pairs. Completed sequences
        are automatically removed and their cache evicted.
        """
        ...

    fn active_requests(self) -> Int:
        """Returns the number of requests currently being processed."""
        ...

    fn pending_requests(self) -> Int:
        """Returns the number of requests waiting in the queue."""
        ...
```

### 4.3 BatchScheduler

Decides which requests to schedule each iteration, handling preemption and priorities.

```mojo
# Depends on: Request, ContinuousBatcher, CacheManager

struct BatchScheduler:
    """Schedules requests for continuous batching with priority and preemption.

    Selects which requests to include in each batch iteration based on
    priority, available KV cache memory, and fairness constraints.
    Supports preemption: low-priority decoding requests can be paused
    to make room for high-priority prefill requests.
    """

    var batcher: ContinuousBatcher
    var cache_manager: CacheManager

    fn __init__(inout self, batcher: ContinuousBatcher, cache_manager: CacheManager):
        """Create a scheduler wrapping the batcher and cache manager."""
        ...

    fn schedule_next_batch(inout self) -> List[Request]:
        """Select requests for the next iteration.

        Considers: (1) priority ordering, (2) available cache memory,
        (3) prefill/decode balance, (4) fairness (starvation prevention).
        May preempt low-priority decode requests.
        """
        ...

    fn preempt(inout self, request_id: Int):
        """Pause a decoding request, saving its state for later resumption."""
        ...

    fn resume(inout self, request_id: Int):
        """Resume a previously preempted request."""
        ...
```

---

## 5. model/ -- Model Loading and Registry

### 5.1 Unified Loader

Single entry point for loading models from any supported format.

```mojo
# Depends on: neutron_mojo.tensor.Tensor, neutron_mojo.tensor.DType
# Depends on: QuantConfig, SafeTensorsLoader, GGUFLoader, NativeLoader

fn load(
    path: String,
    format: String = "auto",
    quant_config: Optional[QuantConfig] = None,
    device: DeviceKind = DeviceKind.CUDA,
    device_id: Int = 0,
) -> Dict[String, Tensor]:
    """Load model weights from disk.

    Supported formats:
    - "safetensors": HuggingFace SafeTensors format (JSON header + binary data)
    - "gguf": llama.cpp GGUF format (structured block quantization)
    - "native": Neutron Mojo native format (mmap, zero-copy)
    - "auto": detect format from file extension / magic bytes

    If quant_config is provided, weights are quantized during loading (no
    separate quantization step). Weights are placed on the specified device.

    Returns a dict mapping parameter names to tensors.
    """
    ...
```

### 5.2 Format-Specific Loaders

```mojo
# Depends on: Tensor, DType, QuantConfig

struct SafeTensorsLoader:
    """Loads models from HuggingFace SafeTensors format.

    SafeTensors: JSON header with tensor metadata (name, dtype, shape,
    data_offsets) followed by contiguous binary data. Supports mmap for
    zero-copy on compatible systems.
    """

    fn __init__(inout self, path: String):
        """Open a SafeTensors file or directory."""
        ...

    fn load(self, device: DeviceKind = DeviceKind.CUDA) -> Dict[String, Tensor]:
        """Load all tensors from the file."""
        ...

    fn tensor_names(self) -> List[String]:
        """List all tensor names without loading data."""
        ...

    fn load_tensor(self, name: String, device: DeviceKind = DeviceKind.CUDA) -> Tensor:
        """Load a single tensor by name."""
        ...

struct GGUFLoader:
    """Loads models from llama.cpp GGUF format.

    GGUF: magic number, version, tensor info blocks with quantization
    metadata. Weights are stored in structured block quantization
    (Q4_K_M super-blocks of 256 with 32-element sub-blocks, etc.).
    """

    fn __init__(inout self, path: String):
        """Open a GGUF file."""
        ...

    fn load(self, device: DeviceKind = DeviceKind.CUDA) -> Dict[String, Tensor]:
        """Load and dequantize all tensors. Quantized weights are dequantized to float16."""
        ...

    fn metadata(self) -> Dict[String, String]:
        """Read GGUF metadata (model architecture, tokenizer info, quantization type)."""
        ...

    fn quant_type(self) -> QuantType:
        """Returns the quantization type used in this GGUF file."""
        ...

struct NativeLoader:
    """Loads models from Neutron Mojo native format.

    Uses mmap for zero-copy loading. The fastest format for production
    deployment -- model files are memory-mapped directly, no parsing or
    conversion overhead.
    """

    fn __init__(inout self, path: String):
        """Open a native format directory."""
        ...

    fn load(self, device: DeviceKind = DeviceKind.CUDA) -> Dict[String, Tensor]:
        """Memory-map all tensors. Effectively zero-cost loading."""
        ...
```

### 5.3 ModelConfig and ModelRegistry

```mojo
# Depends on: QuantConfig

struct ModelConfig:
    """Model architecture and generation configuration.

    Describes the transformer architecture (number of layers, heads,
    hidden size, etc.) and generation parameters. Loaded from model
    metadata (config.json for HuggingFace, GGUF metadata, etc.).
    """

    # Architecture
    var num_layers: Int                # Number of transformer layers
    var num_attention_heads: Int       # Number of query attention heads
    var num_kv_heads: Int              # Number of KV heads (for GQA)
    var hidden_size: Int               # Hidden dimension
    var intermediate_size: Int         # FFN intermediate dimension
    var vocab_size: Int                # Vocabulary size
    var max_position_embeddings: Int   # Maximum sequence length
    var head_dim: Int                  # Per-head dimension (hidden_size / num_attention_heads)

    # Normalization
    var rms_norm_eps: Float32          # RMSNorm epsilon (default 1e-5)
    var norm_type: String              # "rmsnorm" or "layernorm"

    # Activation
    var hidden_act: String             # "silu", "gelu", "relu"

    # Attention
    var rope_theta: Float64            # RoPE base frequency (default 10000.0)
    var sliding_window: Optional[Int]  # Sliding window size (None = full attention)

    fn from_huggingface(path: String) -> ModelConfig:
        """Parse a HuggingFace config.json file."""
        ...

    fn from_gguf_metadata(metadata: Dict[String, String]) -> ModelConfig:
        """Parse GGUF metadata into a ModelConfig."""
        ...

struct ModelRegistry:
    """Registry of known model architectures and their download sources.

    Maps model names to HuggingFace Hub IDs, GGUF download URLs, or
    local paths. Supports model discovery and download.
    """

    fn __init__(inout self):
        """Initialize with built-in model registry."""
        ...

    fn resolve(self, model_name: String) -> String:
        """Resolve a model name to a download path or local directory.

        Handles: "llama3" -> "meta-llama/Llama-3-8B",
        "llama3:q4" -> GGUF Q4_K_M variant, etc.
        """
        ...

    fn download(self, model_name: String, target_dir: String) -> String:
        """Download a model and return the local path."""
        ...

    fn list_available(self) -> List[String]:
        """List all registered model names."""
        ...

    fn register(inout self, name: String, source: String, format: String):
        """Register a custom model name mapping."""
        ...
```

---

## 6. transformer/ -- Transformer Building Blocks

### 6.1 Transformer

Full transformer model for inference.

```mojo
# Depends on: Tensor, DType, Dim, ModelConfig
# Depends on: TransformerLayer, Embedding, LMHead, KVCache
# Depends on: Sampler

struct Transformer:
    """Decoder-only transformer model for autoregressive inference.

    Composes embedding, transformer layers, and language model head.
    Supports both single-token decoding and full-prompt prefill.
    """

    var config: ModelConfig
    var embedding: Embedding
    var layers: List[TransformerLayer]
    var norm: RMSNorm                   # or LayerNorm, based on config
    var lm_head: LMHead
    var cache: KVCache

    fn __init__(
        inout self,
        config: ModelConfig,
        weights: Dict[String, Tensor],
        cache: KVCache,
    ):
        """Initialize the transformer from config and loaded weights."""
        ...

    fn forward[dtype: DType, B: Dim, S: Dim](
        self,
        token_ids: Tensor[DType.int32, B, S],
        start_pos: Int = 0,
    ) -> Tensor[dtype, B, S, Dim["vocab"]]:
        """Forward pass: token IDs -> logits.

        Uses KV cache for efficient autoregressive generation. start_pos
        indicates the position of the first token in the sequence (for
        incremental decoding).
        """
        ...

    fn generate(
        inout self,
        prompt_tokens: List[Int],
        max_new_tokens: Int = 256,
        sampler: Sampler = Sampler.greedy(),
    ) -> List[Int]:
        """Generate tokens autoregressively from a prompt.

        Returns the full generated token sequence (prompt + new tokens).
        Stops at max_new_tokens or when a stop token is generated.
        """
        ...
```

### 6.2 TransformerLayer

Single transformer layer (attention + FFN + norms).

```mojo
# Depends on: Tensor, DType, Dim, ModelConfig
# Depends on: flash_attention or grouped_query_attention
# Depends on: FFN, RMSNorm or LayerNorm

struct TransformerLayer:
    """One transformer layer: pre-norm -> attention -> residual -> pre-norm -> FFN -> residual."""

    var attention_norm: RMSNorm
    var attention_wq: Tensor            # Query projection weights
    var attention_wk: Tensor            # Key projection weights
    var attention_wv: Tensor            # Value projection weights
    var attention_wo: Tensor            # Output projection weights
    var ffn_norm: RMSNorm
    var ffn: FFN

    fn forward[dtype: DType, B: Dim, S: Dim, H: Dim](
        self,
        x: Tensor[dtype, B, S, H],
        start_pos: Int,
        cache: KVCache,
        layer_idx: Int,
    ) -> Tensor[dtype, B, S, H]:
        """Forward pass through one transformer layer with KV caching."""
        ...
```

### 6.3 FFN

Feed-forward network variants.

```mojo
# Depends on: Tensor, DType, Dim

struct FFN:
    """Feed-forward network block. Supports SwiGLU (Llama-style) and standard GeLU.

    SwiGLU: out = (W_gate(x) * silu(W_up(x))) @ W_down
    GeLU:   out = gelu(W_up(x)) @ W_down
    """

    var w_up: Tensor                # Up projection weights
    var w_down: Tensor              # Down projection weights
    var w_gate: Optional[Tensor]    # Gate projection weights (SwiGLU only)
    var activation: String          # "silu" (SwiGLU) or "gelu"

    fn forward[dtype: DType, B: Dim, S: Dim, H: Dim](
        self,
        x: Tensor[dtype, B, S, H],
    ) -> Tensor[dtype, B, S, H]:
        """Feed-forward pass."""
        ...
```

### 6.4 Embedding

Token and position embeddings.

```mojo
# Depends on: Tensor, DType, Dim

struct Embedding:
    """Token embedding lookup table.

    Maps integer token IDs to dense vectors. Position encoding (RoPE) is
    applied inside the attention layer, not here.
    """

    var weight: Tensor              # Embedding weight matrix [vocab_size, hidden_size]

    fn forward[B: Dim, S: Dim](
        self,
        token_ids: Tensor[DType.int32, B, S],
    ) -> Tensor[DType.float16, B, S, Dim["hidden"]]:
        """Look up token embeddings for the given IDs."""
        ...
```

### 6.5 RMSNorm and LayerNorm

```mojo
# Depends on: Tensor, DType, Dim, @kernel

struct RMSNorm:
    """Root Mean Square Layer Normalization.

    RMSNorm(x) = x * rsqrt(mean(x^2) + eps) * weight
    Simpler and faster than LayerNorm (no mean subtraction).
    Used by Llama, Mistral, and most modern LLMs.
    """

    var weight: Tensor              # Learnable scale parameter
    var eps: Float32                # Epsilon for numerical stability (default 1e-5)

    fn forward[dtype: DType, *dims: Dim](
        self,
        x: Tensor[dtype, *dims],
    ) -> Tensor[dtype, *dims]:
        """Apply RMS normalization."""
        ...

struct LayerNorm:
    """Standard Layer Normalization.

    LayerNorm(x) = (x - mean(x)) / sqrt(var(x) + eps) * weight + bias
    """

    var weight: Tensor              # Learnable scale parameter
    var bias: Tensor                # Learnable shift parameter
    var eps: Float32                # Epsilon for numerical stability

    fn forward[dtype: DType, *dims: Dim](
        self,
        x: Tensor[dtype, *dims],
    ) -> Tensor[dtype, *dims]:
        """Apply layer normalization."""
        ...
```

### 6.6 LMHead

Language model head (final logits projection).

```mojo
# Depends on: Tensor, DType, Dim

struct LMHead:
    """Projects hidden states to vocabulary logits.

    Typically shares weights with the token embedding (tied embeddings).
    """

    var weight: Tensor              # Projection weights [hidden_size, vocab_size]
    var tied_embedding: Bool        # True if sharing weights with Embedding

    fn forward[dtype: DType, B: Dim, S: Dim, H: Dim](
        self,
        hidden: Tensor[dtype, B, S, H],
    ) -> Tensor[dtype, B, S, Dim["vocab"]]:
        """Project hidden states to logits over the vocabulary."""
        ...
```

### 6.7 Sampler

Token sampling strategies.

```mojo
# Depends on: Tensor, DType

struct Sampler:
    """Token sampling from logits with various strategies.

    Supports greedy (argmax), top-k, top-p (nucleus), and temperature
    scaling. Strategies can be combined (e.g., temperature + top-p).
    """

    var temperature: Float32         # Sampling temperature (0.0 = greedy)
    var top_k: Int                   # Top-k filter (0 = disabled)
    var top_p: Float32               # Top-p nucleus threshold (1.0 = disabled)
    var repetition_penalty: Float32  # Penalty for repeated tokens (1.0 = no penalty)

    @staticmethod
    fn greedy() -> Sampler:
        """Create a greedy (argmax) sampler."""
        ...

    @staticmethod
    fn top_k(k: Int, temperature: Float32 = 1.0) -> Sampler:
        """Create a top-k sampler."""
        ...

    @staticmethod
    fn top_p(p: Float32, temperature: Float32 = 1.0) -> Sampler:
        """Create a top-p (nucleus) sampler."""
        ...

    @staticmethod
    fn temperature(t: Float32) -> Sampler:
        """Create a temperature-scaled sampler (uses multinomial sampling)."""
        ...

    fn sample(self, logits: Tensor[DType.float32]) -> Int:
        """Sample a single token index from the logits distribution.

        Applies temperature scaling, then top-k/top-p filtering, then
        samples from the filtered distribution.
        """
        ...

    fn sample_batch(self, logits: Tensor[DType.float32]) -> List[Int]:
        """Sample one token per sequence in a batch."""
        ...
```

---

## 7. serve/ -- Serving Infrastructure

### 7.1 InferenceServer

Standalone HTTP server with OpenAI-compatible API.

```mojo
# Depends on: Transformer, ContinuousBatcher, BatchScheduler, Tokenizer, ModelConfig
# NOTE: Requires Mojo async support (post-1.0) or Lightbug HTTP for the HTTP layer.

struct InferenceServer:
    """Standalone LLM inference server with OpenAI-compatible API.

    Wraps the transformer model, continuous batcher, and tokenizer into
    a server that handles concurrent requests. Serves both streaming
    and non-streaming completions.
    """

    var model: Transformer
    var batcher: ContinuousBatcher
    var scheduler: BatchScheduler
    var tokenizer: Tokenizer
    var config: ModelConfig

    fn __init__(
        inout self,
        model: Transformer,
        max_batch_size: Int = 64,
        cache: KVCache = PagedKVCache(...),
    ):
        """Initialize the server with a loaded model."""
        ...

    fn serve(inout self, host: String = "0.0.0.0", port: Int = 8080):
        """Start serving HTTP requests. Blocks until stop() is called.

        Endpoints:
        - POST /v1/chat/completions (OpenAI-compatible chat)
        - POST /v1/completions (OpenAI-compatible text completion)
        - GET /v1/models (list available models)
        - GET /health (health check)
        """
        ...

    fn stop(inout self):
        """Gracefully shut down the server, finishing in-flight requests."""
        ...
```

### 7.2 OpenAIAPI

OpenAI-compatible request/response formatting.

```mojo
# Depends on: Request, Sampler

struct OpenAIAPI:
    """Parses OpenAI-compatible API requests and formats responses.

    Handles both chat completions and text completions formats.
    Supports streaming (SSE) and non-streaming responses.
    """

    fn parse_chat_request(self, body: String) -> Tuple[Request, Sampler]:
        """Parse a /v1/chat/completions JSON request body into a Request and Sampler."""
        ...

    fn format_chat_response(self, request_id: Int, tokens: List[Int], tokenizer: Tokenizer) -> String:
        """Format a completed generation as a /v1/chat/completions JSON response."""
        ...

    fn format_stream_chunk(self, request_id: Int, token: Int, tokenizer: Tokenizer) -> String:
        """Format a single token as an SSE data chunk for streaming responses."""
        ...

    fn format_stream_done(self, request_id: Int) -> String:
        """Format the final SSE [DONE] message."""
        ...
```

### 7.3 Tokenizer

Tokenizer wrapper for BPE and SentencePiece models.

```mojo
# Depends on: nothing (standalone utility)

struct Tokenizer:
    """Wraps BPE or SentencePiece tokenizer models.

    Loads tokenizer files from HuggingFace format (tokenizer.json) or
    SentencePiece format (.model). Handles special tokens (BOS, EOS, PAD).
    """

    var vocab_size: Int
    var bos_token_id: Int              # Beginning of sequence token
    var eos_token_id: Int              # End of sequence token
    var pad_token_id: Int              # Padding token

    fn __init__(inout self, path: String):
        """Load a tokenizer from a file (tokenizer.json or .model)."""
        ...

    fn encode(self, text: String) -> List[Int]:
        """Encode text to token IDs."""
        ...

    fn decode(self, token_ids: List[Int]) -> String:
        """Decode token IDs back to text."""
        ...

    fn decode_single(self, token_id: Int) -> String:
        """Decode a single token ID to its text representation."""
        ...
```

---

## 8. ffi/ -- FFI Exports for Rust Integration

C ABI exports so Neutron Rust can call Mojo inference via FFI.

```mojo
# Depends on: Transformer, Tokenizer, DLPack
# Depends on: neutron_mojo.ffi (@export)

@export
fn load_model(
    model_path_ptr: Pointer[UInt8],
    model_path_len: Int,
    quant_type: Int32,
    device_id: Int32,
) -> Pointer[UInt8]:
    """Load a model from disk and return an opaque model handle.

    Called once at startup by the Rust service. The handle must be
    passed to subsequent predict calls and freed with free_result.
    """
    ...

@export
fn predict(
    model_handle: Pointer[UInt8],
    input_ptr: Pointer[UInt8],
    input_len: Int,
    max_tokens: Int32,
    temperature: Float32,
) -> Pointer[UInt8]:
    """Run inference on a text prompt. Returns a pointer to the output string.

    The returned pointer must be freed with free_result. The output is
    a null-terminated UTF-8 string.
    """
    ...

@export
fn predict_stream(
    model_handle: Pointer[UInt8],
    input_ptr: Pointer[UInt8],
    input_len: Int,
    max_tokens: Int32,
    temperature: Float32,
    callback: fn(Pointer[UInt8], Int) -> Bool,
):
    """Run streaming inference, calling `callback` for each generated token.

    The callback receives (token_text_ptr, token_text_len) and returns
    False to stop generation early. The pointer is only valid during the
    callback invocation.
    """
    ...

@export
fn predict_dlpack(
    model_handle: Pointer[UInt8],
    input_dl_tensor: Pointer[DLManagedTensor],
) -> Pointer[DLManagedTensor]:
    """Run inference with DLPack tensor input/output for zero-copy interop.

    Input is a DLPack tensor of token IDs. Output is a DLPack tensor of
    logits. Caller must call the output's deleter when done.
    """
    ...

@export
fn free_result(ptr: Pointer[UInt8]):
    """Free memory allocated by predict or load_model."""
    ...
```

---

## Cross-Reference: Module Dependencies

```
serve/* ──────> transformer/*, batching/*, model/*
transformer/* ─> attention/*, kv_cache/*, quantization/*, core.tensor/*, core.kernel/*
batching/* ───> kv_cache/*, core.runtime/*
model/* ──────> quantization/types, core.tensor/*
attention/* ──> core.kernel/*, core.layout/*, kv_cache/types
kv_cache/* ───> core.runtime/memory, core.tensor/*
quantization/* > core.kernel/*, core.tensor/*
ffi/exports ──> transformer/*, model/*, serve/*
```
