# ===----------------------------------------------------------------------=== #
# Neutron Mojo -- Per-Operation Profiler
# ===----------------------------------------------------------------------=== #

"""Per-operation profiling for transformer forward passes.

Instruments each operation (embed, norm, projections, RoPE, attention,
FFN, LM head) with nanosecond-precision timing. Provides per-layer
and aggregate breakdowns.

Usage:
    var prof = ProfileResult()
    var logits = profile_forward(model, token_id, cache, rope, pos, prof)
    print(prof.summary())

    var decode_result = profile_decode(model, tokens, num_steps)
    print(decode_result.summary())
"""

from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    simd_rmsnorm, simd_swiglu, par_simd_matvec,
)
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.attention import gqa_attention_direct
from neutron_mojo.nn.causal_lm import embed_token, argmax


# ===----------------------------------------------------------------------=== #
# Profile Result
# ===----------------------------------------------------------------------=== #

struct ProfileResult(Copyable, Movable):
    """Per-operation timing breakdown for one forward pass (nanoseconds)."""
    var embed_ns: Int
    var attn_norm_ns: Int
    var qkv_proj_ns: Int
    var rope_ns: Int
    var kv_cache_ns: Int
    var attention_ns: Int
    var output_proj_ns: Int
    var ffn_norm_ns: Int
    var ffn_proj_ns: Int     # gate + up + down projections
    var swiglu_ns: Int
    var final_norm_ns: Int
    var lm_head_ns: Int
    var total_ns: Int
    var num_layers: Int

    fn __init__(out self):
        self.embed_ns = 0
        self.attn_norm_ns = 0
        self.qkv_proj_ns = 0
        self.rope_ns = 0
        self.kv_cache_ns = 0
        self.attention_ns = 0
        self.output_proj_ns = 0
        self.ffn_norm_ns = 0
        self.ffn_proj_ns = 0
        self.swiglu_ns = 0
        self.final_norm_ns = 0
        self.lm_head_ns = 0
        self.total_ns = 0
        self.num_layers = 0

    fn __copyinit__(out self, existing: Self):
        self.embed_ns = existing.embed_ns
        self.attn_norm_ns = existing.attn_norm_ns
        self.qkv_proj_ns = existing.qkv_proj_ns
        self.rope_ns = existing.rope_ns
        self.kv_cache_ns = existing.kv_cache_ns
        self.attention_ns = existing.attention_ns
        self.output_proj_ns = existing.output_proj_ns
        self.ffn_norm_ns = existing.ffn_norm_ns
        self.ffn_proj_ns = existing.ffn_proj_ns
        self.swiglu_ns = existing.swiglu_ns
        self.final_norm_ns = existing.final_norm_ns
        self.lm_head_ns = existing.lm_head_ns
        self.total_ns = existing.total_ns
        self.num_layers = existing.num_layers

    fn __moveinit__(out self, deinit other: Self):
        self.embed_ns = other.embed_ns
        self.attn_norm_ns = other.attn_norm_ns
        self.qkv_proj_ns = other.qkv_proj_ns
        self.rope_ns = other.rope_ns
        self.kv_cache_ns = other.kv_cache_ns
        self.attention_ns = other.attention_ns
        self.output_proj_ns = other.output_proj_ns
        self.ffn_norm_ns = other.ffn_norm_ns
        self.ffn_proj_ns = other.ffn_proj_ns
        self.swiglu_ns = other.swiglu_ns
        self.final_norm_ns = other.final_norm_ns
        self.lm_head_ns = other.lm_head_ns
        self.total_ns = other.total_ns
        self.num_layers = other.num_layers

    fn copy(self) -> Self:
        """Create a copy."""
        var r = ProfileResult()
        r.embed_ns = self.embed_ns
        r.attn_norm_ns = self.attn_norm_ns
        r.qkv_proj_ns = self.qkv_proj_ns
        r.rope_ns = self.rope_ns
        r.kv_cache_ns = self.kv_cache_ns
        r.attention_ns = self.attention_ns
        r.output_proj_ns = self.output_proj_ns
        r.ffn_norm_ns = self.ffn_norm_ns
        r.ffn_proj_ns = self.ffn_proj_ns
        r.swiglu_ns = self.swiglu_ns
        r.final_norm_ns = self.final_norm_ns
        r.lm_head_ns = self.lm_head_ns
        r.total_ns = self.total_ns
        r.num_layers = self.num_layers
        return r^

    fn layer_total_ns(self) -> Int:
        """Total time spent in transformer layers (all per-layer ops)."""
        return (self.attn_norm_ns + self.qkv_proj_ns + self.rope_ns +
                self.kv_cache_ns + self.attention_ns + self.output_proj_ns +
                self.ffn_norm_ns + self.ffn_proj_ns + self.swiglu_ns)

    fn overhead_ns(self) -> Int:
        """Time not accounted for by measured operations (residuals, etc)."""
        var measured = (self.embed_ns + self.layer_total_ns() +
                        self.final_norm_ns + self.lm_head_ns)
        if self.total_ns > measured:
            return self.total_ns - measured
        return 0

    fn _pct(self, ns: Int) -> Float64:
        """Compute percentage of total time."""
        if self.total_ns == 0:
            return 0.0
        return Float64(ns) * 100.0 / Float64(self.total_ns)

    fn _us(self, ns: Int) -> Float64:
        """Convert nanoseconds to microseconds."""
        return Float64(ns) / 1000.0

    fn _format_line(self, name: String, ns: Int) -> String:
        """Format one line of the summary table."""
        var us = self._us(ns)
        var pct = self._pct(ns)
        var s = String("  ")
        s += name
        # Pad name to 16 chars
        var pad = 16 - len(name)
        for _ in range(pad):
            s += " "
        s += String(Int(us)) + " us"
        # Pad to align percentage
        var us_str = String(Int(us))
        var pad2 = 10 - len(us_str)
        for _ in range(pad2):
            s += " "
        s += String(Int(pct)) + "%"
        return s^

    fn summary(self) -> String:
        """Format a human-readable profiling summary."""
        var s = String("Profile (")
        s += String(self.num_layers) + " layers, "
        s += String(Int(self._us(self.total_ns))) + " us total):\n"
        s += self._format_line("embed", self.embed_ns) + "\n"
        s += self._format_line("attn_norm", self.attn_norm_ns) + "\n"
        s += self._format_line("qkv_proj", self.qkv_proj_ns) + "\n"
        s += self._format_line("rope", self.rope_ns) + "\n"
        s += self._format_line("kv_cache", self.kv_cache_ns) + "\n"
        s += self._format_line("attention", self.attention_ns) + "\n"
        s += self._format_line("output_proj", self.output_proj_ns) + "\n"
        s += self._format_line("ffn_norm", self.ffn_norm_ns) + "\n"
        s += self._format_line("ffn_proj", self.ffn_proj_ns) + "\n"
        s += self._format_line("swiglu", self.swiglu_ns) + "\n"
        s += self._format_line("final_norm", self.final_norm_ns) + "\n"
        s += self._format_line("lm_head", self.lm_head_ns) + "\n"
        s += self._format_line("overhead", self.overhead_ns())
        return s^

    fn add(mut self, other: Self):
        """Accumulate another ProfileResult into this one."""
        self.embed_ns += other.embed_ns
        self.attn_norm_ns += other.attn_norm_ns
        self.qkv_proj_ns += other.qkv_proj_ns
        self.rope_ns += other.rope_ns
        self.kv_cache_ns += other.kv_cache_ns
        self.attention_ns += other.attention_ns
        self.output_proj_ns += other.output_proj_ns
        self.ffn_norm_ns += other.ffn_norm_ns
        self.ffn_proj_ns += other.ffn_proj_ns
        self.swiglu_ns += other.swiglu_ns
        self.final_norm_ns += other.final_norm_ns
        self.lm_head_ns += other.lm_head_ns
        self.total_ns += other.total_ns


# ===----------------------------------------------------------------------=== #
# Decode Profile Result
# ===----------------------------------------------------------------------=== #

struct DecodeProfileResult(Copyable, Movable):
    """Aggregate profiling over multiple decode steps."""
    var aggregate: ProfileResult  # Summed across all steps
    var num_steps: Int
    var tokens_per_sec: Float64

    fn __init__(out self):
        self.aggregate = ProfileResult()
        self.num_steps = 0
        self.tokens_per_sec = 0.0

    fn __copyinit__(out self, existing: Self):
        self.aggregate = existing.aggregate.copy()
        self.num_steps = existing.num_steps
        self.tokens_per_sec = existing.tokens_per_sec

    fn __moveinit__(out self, deinit other: Self):
        self.aggregate = other.aggregate^
        self.num_steps = other.num_steps
        self.tokens_per_sec = other.tokens_per_sec

    fn avg_step_ns(self) -> Int:
        """Average time per decode step in nanoseconds."""
        if self.num_steps == 0:
            return 0
        return self.aggregate.total_ns // self.num_steps

    fn summary(self) -> String:
        """Format decode profiling summary with per-step averages."""
        var s = String("Decode Profile (")
        s += String(self.num_steps) + " steps, "
        s += String(Int(self.tokens_per_sec)) + " tok/s):\n"
        s += "  Avg step: " + String(Int(Float64(self.avg_step_ns()) / 1000.0)) + " us\n"
        s += self.aggregate.summary()
        return s^


# ===----------------------------------------------------------------------=== #
# Profiled Forward Pass
# ===----------------------------------------------------------------------=== #

fn profile_forward(
    model: Model,
    token_id: Int,
    mut cache: MultiLayerKVCache,
    rope: RoPETable,
    pos: Int,
    mut prof: ProfileResult,
) raises -> Tensor[DType.float32]:
    """Forward pass with per-operation profiling.

    Identical computation to Model.forward() but instruments each
    operation with nanosecond timing. Profile results are written
    to the prof parameter.

    Args:
        model: Language model.
        token_id: Input token ID.
        cache: Multi-layer KV cache.
        rope: RoPE table.
        pos: Current position.
        prof: Output profile result (will be filled in).

    Returns:
        Logits [vocab_size].
    """
    var p = model.params.copy()
    prof.num_layers = p.num_layers

    var total_start = Int(perf_counter_ns())

    # === Embed ===
    var t0 = Int(perf_counter_ns())
    var hidden = embed_token(model.embed, token_id, p.hidden_dim)
    prof.embed_ns = Int(perf_counter_ns()) - t0

    # === Transformer layers ===
    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        var hd = p.hidden_dim

        # Attention norm
        t0 = Int(perf_counter_ns())
        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, hidden, 0, model.layer_weights, off.attn_norm, hd)
        prof.attn_norm_ns += Int(perf_counter_ns()) - t0

        # Q/K/V projections
        t0 = Int(perf_counter_ns())
        var q = model._linear_from_flat(normed, off.wq, p.q_dim(), hd)
        var k = model._linear_from_flat(normed, off.wk, p.kv_dim(), hd)
        var v = model._linear_from_flat(normed, off.wv, p.kv_dim(), hd)
        prof.qkv_proj_ns += Int(perf_counter_ns()) - t0

        # RoPE
        t0 = Int(perf_counter_ns())
        for h in range(p.num_q_heads):
            var q_head = Tensor[DType.float32](Shape(p.head_dim))
            var base = h * p.head_dim
            for d in range(p.head_dim):
                q_head.set(d, q.get(base + d))
            apply_rope_single_head(q_head, rope, pos)
            for d in range(p.head_dim):
                q.set(base + d, q_head.get(d))

        for h in range(p.num_kv_heads):
            var k_head = Tensor[DType.float32](Shape(p.head_dim))
            var base = h * p.head_dim
            for d in range(p.head_dim):
                k_head.set(d, k.get(base + d))
            apply_rope_single_head(k_head, rope, pos)
            for d in range(p.head_dim):
                k.set(base + d, k_head.get(d))
        prof.rope_ns += Int(perf_counter_ns()) - t0

        # KV cache append
        t0 = Int(perf_counter_ns())
        cache.append_kv(layer, k, v, num_new_tokens=1)
        prof.kv_cache_ns += Int(perf_counter_ns()) - t0

        # GQA attention
        t0 = Int(perf_counter_ns())
        var attn_out = gqa_attention_direct(
            q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim
        )
        prof.attention_ns += Int(perf_counter_ns()) - t0

        # Output projection
        t0 = Int(perf_counter_ns())
        var attn_proj = model._linear_from_flat(attn_out, off.wo, hd, p.q_dim())
        prof.output_proj_ns += Int(perf_counter_ns()) - t0

        # Residual (counted in overhead)
        var residual1 = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            residual1.set(i, hidden.get(i) + attn_proj.get(i))

        # FFN norm
        t0 = Int(perf_counter_ns())
        var ffn_normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(ffn_normed, 0, residual1, 0, model.layer_weights, off.ffn_norm, hd)
        prof.ffn_norm_ns += Int(perf_counter_ns()) - t0

        # FFN projections (gate + up)
        t0 = Int(perf_counter_ns())
        var gate = model._linear_from_flat(ffn_normed, off.w_gate, p.ffn_dim, hd)
        var up = model._linear_from_flat(ffn_normed, off.w_up, p.ffn_dim, hd)
        prof.ffn_proj_ns += Int(perf_counter_ns()) - t0

        # SwiGLU
        t0 = Int(perf_counter_ns())
        var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
        simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)
        prof.swiglu_ns += Int(perf_counter_ns()) - t0

        # Down projection (part of ffn_proj)
        t0 = Int(perf_counter_ns())
        var down = model._linear_from_flat(ffn_out, off.w_down, hd, p.ffn_dim)
        prof.ffn_proj_ns += Int(perf_counter_ns()) - t0

        # Residual (counted in overhead)
        hidden = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            hidden.set(i, residual1.get(i) + down.get(i))

    # === Final norm ===
    t0 = Int(perf_counter_ns())
    var final_normed = Tensor[DType.float32](Shape(p.hidden_dim))
    simd_rmsnorm(final_normed, 0, hidden, 0, model.final_norm, 0, p.hidden_dim)
    prof.final_norm_ns = Int(perf_counter_ns()) - t0

    # === LM head ===
    t0 = Int(perf_counter_ns())
    var logits = Tensor[DType.float32](Shape(p.vocab_size))
    par_simd_matvec(logits, 0, model.lm_head, 0, final_normed, 0, p.vocab_size, p.hidden_dim)
    prof.lm_head_ns = Int(perf_counter_ns()) - t0

    prof.total_ns = Int(perf_counter_ns()) - total_start

    return logits^


# ===----------------------------------------------------------------------=== #
# Profiled Decode Loop
# ===----------------------------------------------------------------------=== #

fn profile_decode(
    model: Model,
    prompt_tokens: List[Int],
    num_steps: Int,
) raises -> DecodeProfileResult:
    """Profile a full decode loop (prefill + generation).

    Prefills prompt tokens without profiling, then profiles each
    decode step individually and aggregates results.

    Args:
        model: Language model.
        prompt_tokens: Prompt token IDs.
        num_steps: Number of decode steps to profile.

    Returns:
        DecodeProfileResult with aggregate timing breakdown.
    """
    var p = model.params.copy()
    var total_len = len(prompt_tokens) + num_steps

    var cache = MultiLayerKVCache(
        num_layers=p.num_layers,
        max_seq_len=total_len,
        num_kv_heads=p.num_kv_heads,
        head_dim=p.head_dim,
    )
    var rope = RoPETable(
        head_dim=p.head_dim,
        max_seq_len=total_len,
        theta=p.rope_theta,
    )

    # Prefill (unprofiled)
    var logits = Tensor[DType.float32](Shape(p.vocab_size))
    for i in range(len(prompt_tokens)):
        logits = model.forward(prompt_tokens[i], cache, rope, pos=i)

    # Decode with profiling
    var result = DecodeProfileResult()
    result.num_steps = num_steps

    var decode_start = Int(perf_counter_ns())

    for step in range(num_steps):
        var next_token = argmax(logits, p.vocab_size)
        var step_pos = len(prompt_tokens) + step

        var step_prof = ProfileResult()
        logits = profile_forward(model, next_token, cache, rope, step_pos, step_prof)
        result.aggregate.add(step_prof)

    var decode_elapsed = Int(perf_counter_ns()) - decode_start

    # Compute throughput
    if decode_elapsed > 0:
        result.tokens_per_sec = Float64(num_steps) / (
            Float64(decode_elapsed) / 1_000_000_000.0
        )

    result.aggregate.num_layers = p.num_layers

    return result^
