# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Benchmark and Memory Estimation
# ===----------------------------------------------------------------------=== #

"""Memory estimation, model info, and benchmark harness for inference.

Provides utilities to:
1. Estimate memory usage for models, KV caches, and total inference
2. Display model architecture information
3. Benchmark prefill and decode performance (tokens/sec)

Usage:
    var info = model_info(model)
    print(info.summary())

    var mem = estimate_memory(model.params, batch_size=4, seq_len=512)
    print("Peak memory: " + String(mem.total_mb()) + " MB")

    var result = benchmark_inference(model, tokenizer, "Hello", max_tokens=32)
    print("Decode: " + String(result.decode_tokens_per_sec) + " tok/s")
"""

from time import perf_counter_ns
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    should_stop,
)
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Memory Estimation
# ===----------------------------------------------------------------------=== #

struct MemoryEstimate(Copyable, Movable):
    """Memory usage estimate for model inference."""
    var model_params_bytes: Int    # Total model parameter memory
    var embed_bytes: Int           # Embedding table
    var layer_weights_bytes: Int   # All transformer layer weights
    var lm_head_bytes: Int         # LM head projection
    var kv_cache_bytes: Int        # KV cache for given batch/seq config
    var activation_bytes: Int      # Peak activation memory estimate
    var total_bytes: Int           # Sum of all components

    fn __init__(out self):
        self.model_params_bytes = 0
        self.embed_bytes = 0
        self.layer_weights_bytes = 0
        self.lm_head_bytes = 0
        self.kv_cache_bytes = 0
        self.activation_bytes = 0
        self.total_bytes = 0

    fn __copyinit__(out self, existing: Self):
        self.model_params_bytes = existing.model_params_bytes
        self.embed_bytes = existing.embed_bytes
        self.layer_weights_bytes = existing.layer_weights_bytes
        self.lm_head_bytes = existing.lm_head_bytes
        self.kv_cache_bytes = existing.kv_cache_bytes
        self.activation_bytes = existing.activation_bytes
        self.total_bytes = existing.total_bytes

    fn __moveinit__(out self, deinit other: Self):
        self.model_params_bytes = other.model_params_bytes
        self.embed_bytes = other.embed_bytes
        self.layer_weights_bytes = other.layer_weights_bytes
        self.lm_head_bytes = other.lm_head_bytes
        self.kv_cache_bytes = other.kv_cache_bytes
        self.activation_bytes = other.activation_bytes
        self.total_bytes = other.total_bytes

    fn total_mb(self) -> Float64:
        """Total memory in megabytes."""
        return Float64(self.total_bytes) / (1024.0 * 1024.0)

    fn model_mb(self) -> Float64:
        """Model parameter memory in megabytes."""
        return Float64(self.model_params_bytes) / (1024.0 * 1024.0)

    fn kv_cache_mb(self) -> Float64:
        """KV cache memory in megabytes."""
        return Float64(self.kv_cache_bytes) / (1024.0 * 1024.0)


fn estimate_memory(
    params: ModelParams,
    batch_size: Int = 1,
    seq_len: Int = 512,
    bytes_per_param: Int = 4,
) -> MemoryEstimate:
    """Estimate total memory needed for inference.

    Args:
        params: Model architecture parameters.
        batch_size: Number of concurrent requests.
        seq_len: Maximum sequence length per request.
        bytes_per_param: Bytes per parameter (4 for FP32, 1 for Q8).

    Returns:
        MemoryEstimate with breakdown.
    """
    var est = MemoryEstimate()
    var p = params.copy()

    # Embedding: [vocab_size, hidden_dim]
    est.embed_bytes = p.vocab_size * p.hidden_dim * bytes_per_param

    # Per-layer weights
    est.layer_weights_bytes = p.num_layers * p.layer_weight_count() * bytes_per_param

    # LM head: [vocab_size, hidden_dim]
    est.lm_head_bytes = p.vocab_size * p.hidden_dim * bytes_per_param

    # Total model params
    est.model_params_bytes = est.embed_bytes + est.layer_weights_bytes + est.lm_head_bytes

    # KV cache: per request, per layer: 2 * seq_len * num_kv_heads * head_dim * 4 bytes
    var kv_per_layer = 2 * seq_len * p.num_kv_heads * p.head_dim * 4  # FP32
    est.kv_cache_bytes = batch_size * p.num_layers * kv_per_layer

    # Activation estimate: peak is during FFN (largest intermediate)
    # Per token: max(ffn_dim, q_dim, hidden_dim) * 4 bytes * ~6 intermediates
    var max_intermediate = p.ffn_dim
    if p.q_dim() > max_intermediate:
        max_intermediate = p.q_dim()
    est.activation_bytes = batch_size * max_intermediate * 4 * 6

    est.total_bytes = est.model_params_bytes + est.kv_cache_bytes + est.activation_bytes

    return est^


# ===----------------------------------------------------------------------=== #
# Model Info
# ===----------------------------------------------------------------------=== #

struct ModelInfo(Copyable, Movable):
    """Displayable model architecture information."""
    var num_layers: Int
    var vocab_size: Int
    var hidden_dim: Int
    var num_q_heads: Int
    var num_kv_heads: Int
    var head_dim: Int
    var ffn_dim: Int
    var max_seq_len: Int
    var total_params: Int         # Total parameter count
    var total_params_millions: Float64

    fn __init__(out self):
        self.num_layers = 0
        self.vocab_size = 0
        self.hidden_dim = 0
        self.num_q_heads = 0
        self.num_kv_heads = 0
        self.head_dim = 0
        self.ffn_dim = 0
        self.max_seq_len = 0
        self.total_params = 0
        self.total_params_millions = 0.0

    fn __copyinit__(out self, existing: Self):
        self.num_layers = existing.num_layers
        self.vocab_size = existing.vocab_size
        self.hidden_dim = existing.hidden_dim
        self.num_q_heads = existing.num_q_heads
        self.num_kv_heads = existing.num_kv_heads
        self.head_dim = existing.head_dim
        self.ffn_dim = existing.ffn_dim
        self.max_seq_len = existing.max_seq_len
        self.total_params = existing.total_params
        self.total_params_millions = existing.total_params_millions

    fn __moveinit__(out self, deinit other: Self):
        self.num_layers = other.num_layers
        self.vocab_size = other.vocab_size
        self.hidden_dim = other.hidden_dim
        self.num_q_heads = other.num_q_heads
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.ffn_dim = other.ffn_dim
        self.max_seq_len = other.max_seq_len
        self.total_params = other.total_params
        self.total_params_millions = other.total_params_millions

    fn is_gqa(self) -> Bool:
        """Whether model uses Grouped Query Attention."""
        return self.num_kv_heads < self.num_q_heads

    fn gqa_ratio(self) -> Int:
        """GQA ratio (Q heads per KV head)."""
        if self.num_kv_heads == 0:
            return 0
        return self.num_q_heads // self.num_kv_heads

    fn summary(self) -> String:
        """Format a human-readable summary."""
        var s = String("Model: ")
        s += String(self.total_params_millions) + "M params\n"
        s += "  Layers: " + String(self.num_layers) + "\n"
        s += "  Hidden: " + String(self.hidden_dim) + "\n"
        s += "  Heads: " + String(self.num_q_heads) + "Q / " + String(self.num_kv_heads) + "KV"
        if self.is_gqa():
            s += " (GQA " + String(self.gqa_ratio()) + "x)"
        s += "\n"
        s += "  FFN: " + String(self.ffn_dim) + "\n"
        s += "  Vocab: " + String(self.vocab_size) + "\n"
        s += "  Max Seq: " + String(self.max_seq_len)
        return s^


fn model_info(params: ModelParams) -> ModelInfo:
    """Extract displayable info from model parameters.

    Args:
        params: Model architecture parameters.

    Returns:
        ModelInfo with architecture details and parameter count.
    """
    var p = params.copy()
    var info = ModelInfo()
    info.num_layers = p.num_layers
    info.vocab_size = p.vocab_size
    info.hidden_dim = p.hidden_dim
    info.num_q_heads = p.num_q_heads
    info.num_kv_heads = p.num_kv_heads
    info.head_dim = p.head_dim
    info.ffn_dim = p.ffn_dim
    info.max_seq_len = p.max_seq_len

    # Count parameters
    var embed_params = p.vocab_size * p.hidden_dim
    var layer_params = p.layer_weight_count() * p.num_layers
    var lm_head_params = p.vocab_size * p.hidden_dim
    var norm_params = p.hidden_dim  # final_norm
    info.total_params = embed_params + layer_params + lm_head_params + norm_params
    info.total_params_millions = Float64(info.total_params) / 1_000_000.0

    return info^


# ===----------------------------------------------------------------------=== #
# Benchmark Harness
# ===----------------------------------------------------------------------=== #

struct BenchmarkResult(Copyable, Movable):
    """Results from a benchmark run."""
    var prefill_tokens: Int
    var decode_tokens: Int
    var prefill_ns: Int          # Nanoseconds for prefill
    var decode_ns: Int           # Nanoseconds for decode
    var total_ns: Int
    var prefill_tokens_per_sec: Float64
    var decode_tokens_per_sec: Float64
    var overall_tokens_per_sec: Float64

    fn __init__(out self):
        self.prefill_tokens = 0
        self.decode_tokens = 0
        self.prefill_ns = 0
        self.decode_ns = 0
        self.total_ns = 0
        self.prefill_tokens_per_sec = 0.0
        self.decode_tokens_per_sec = 0.0
        self.overall_tokens_per_sec = 0.0

    fn __copyinit__(out self, existing: Self):
        self.prefill_tokens = existing.prefill_tokens
        self.decode_tokens = existing.decode_tokens
        self.prefill_ns = existing.prefill_ns
        self.decode_ns = existing.decode_ns
        self.total_ns = existing.total_ns
        self.prefill_tokens_per_sec = existing.prefill_tokens_per_sec
        self.decode_tokens_per_sec = existing.decode_tokens_per_sec
        self.overall_tokens_per_sec = existing.overall_tokens_per_sec

    fn __moveinit__(out self, deinit other: Self):
        self.prefill_tokens = other.prefill_tokens
        self.decode_tokens = other.decode_tokens
        self.prefill_ns = other.prefill_ns
        self.decode_ns = other.decode_ns
        self.total_ns = other.total_ns
        self.prefill_tokens_per_sec = other.prefill_tokens_per_sec
        self.decode_tokens_per_sec = other.decode_tokens_per_sec
        self.overall_tokens_per_sec = other.overall_tokens_per_sec

    fn summary(self) -> String:
        """Format benchmark results."""
        var s = String("Benchmark Results:\n")
        s += "  Prefill: " + String(self.prefill_tokens) + " tokens in "
        s += String(Int(Float64(self.prefill_ns) / 1_000_000.0)) + " ms"
        s += " (" + String(Int(self.prefill_tokens_per_sec)) + " tok/s)\n"
        s += "  Decode:  " + String(self.decode_tokens) + " tokens in "
        s += String(Int(Float64(self.decode_ns) / 1_000_000.0)) + " ms"
        s += " (" + String(Int(self.decode_tokens_per_sec)) + " tok/s)\n"
        s += "  Total:   " + String(Int(Float64(self.total_ns) / 1_000_000.0)) + " ms"
        s += " (" + String(Int(self.overall_tokens_per_sec)) + " tok/s overall)"
        return s^


fn benchmark_inference(
    model: Model,
    tokenizer: BPETokenizer,
    prompt: String,
    max_tokens: Int = 32,
    use_batch_prefill: Bool = True,
) raises -> BenchmarkResult:
    """Benchmark model inference with separate prefill and decode timing.

    Args:
        model: Language model to benchmark.
        tokenizer: BPE tokenizer.
        prompt: Input prompt text.
        max_tokens: Number of tokens to generate.
        use_batch_prefill: Use batch prefill (True) or sequential (False).

    Returns:
        BenchmarkResult with timing breakdown.
    """
    var p = model.params.copy()
    var result = BenchmarkResult()

    # Encode prompt
    var input_ids = tokenizer.encode_with_special(prompt, add_bos=True)
    result.prefill_tokens = len(input_ids)

    var total_len = len(input_ids) + max_tokens
    var rope = RoPETable(
        head_dim=p.head_dim,
        max_seq_len=total_len,
        theta=p.rope_theta,
    )
    var cache = MultiLayerKVCache(
        num_layers=p.num_layers, max_seq_len=total_len,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )

    # === Prefill phase ===
    var prefill_start = Int(perf_counter_ns())
    var logits: Tensor[DType.float32]

    if use_batch_prefill:
        logits = model.forward_prefill(input_ids, cache, rope)
    else:
        logits = Tensor[DType.float32](Shape(p.vocab_size))
        for i in range(len(input_ids)):
            logits = model.forward(input_ids[i], cache, rope, pos=i)

    var prefill_end = Int(perf_counter_ns())
    result.prefill_ns = prefill_end - prefill_start

    # === Decode phase ===
    var decode_start = Int(perf_counter_ns())
    var sampler = Sampler(SamplerConfig())
    var generated = List[Int]()
    var stop_tokens = List[Int]()
    if tokenizer.eos_id >= 0:
        stop_tokens.append(tokenizer.eos_id)

    for step in range(max_tokens):
        var next_token = sampler.sample(logits, p.vocab_size)
        if should_stop(next_token, stop_tokens):
            break
        generated.append(next_token)
        var pos = len(input_ids) + step
        logits = model.forward(next_token, cache, rope, pos=pos)

    var decode_end = Int(perf_counter_ns())
    result.decode_ns = decode_end - decode_start
    result.decode_tokens = len(generated)

    # Compute throughput
    result.total_ns = result.prefill_ns + result.decode_ns

    if result.prefill_ns > 0:
        result.prefill_tokens_per_sec = Float64(result.prefill_tokens) / (Float64(result.prefill_ns) / 1_000_000_000.0)
    if result.decode_ns > 0:
        result.decode_tokens_per_sec = Float64(result.decode_tokens) / (Float64(result.decode_ns) / 1_000_000_000.0)
    if result.total_ns > 0:
        var total_tokens = result.prefill_tokens + result.decode_tokens
        result.overall_tokens_per_sec = Float64(total_tokens) / (Float64(result.total_ns) / 1_000_000_000.0)

    return result^


fn benchmark_prefill_comparison(
    model: Model,
    tokenizer: BPETokenizer,
    prompt: String,
) raises -> String:
    """Compare batch prefill vs sequential prefill speed.

    Args:
        model: Language model.
        tokenizer: BPE tokenizer.
        prompt: Input prompt text.

    Returns:
        Formatted comparison string.
    """
    var batch_result = benchmark_inference(model, tokenizer, prompt,
                                           max_tokens=1, use_batch_prefill=True)
    var seq_result = benchmark_inference(model, tokenizer, prompt,
                                         max_tokens=1, use_batch_prefill=False)

    var speedup: Float64 = 0.0
    if batch_result.prefill_ns > 0 and seq_result.prefill_ns > 0:
        speedup = Float64(seq_result.prefill_ns) / Float64(batch_result.prefill_ns)

    var s = String("Prefill Comparison (" + String(batch_result.prefill_tokens) + " tokens):\n")
    s += "  Sequential: " + String(Int(Float64(seq_result.prefill_ns) / 1_000_000.0)) + " ms"
    s += " (" + String(Int(seq_result.prefill_tokens_per_sec)) + " tok/s)\n"
    s += "  Batch:      " + String(Int(Float64(batch_result.prefill_ns) / 1_000_000.0)) + " ms"
    s += " (" + String(Int(batch_result.prefill_tokens_per_sec)) + " tok/s)\n"
    s += "  Speedup:    " + String(speedup) + "x"
    return s^
