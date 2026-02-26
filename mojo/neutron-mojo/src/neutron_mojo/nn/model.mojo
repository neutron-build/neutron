# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Multi-Layer Causal Language Model
# ===----------------------------------------------------------------------=== #

"""Full N-layer causal language model with autoregressive generation.

Extends the single-layer causal_lm to support arbitrary layer count,
with shared embedding/LM-head and per-layer transformer weights stored
in a flat indexed structure.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import rmsnorm
from neutron_mojo.tensor.simd_math import (
    simd_matvec, simd_rmsnorm, simd_swiglu, simd_silu, simd_axpy, par_simd_matvec,
    fused_rmsnorm_matvec, fused_matvec_residual_add,
    simd_batch_matvec, simd_batch_rmsnorm, simd_batch_swiglu, simd_batch_add,
)
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head, apply_rope_batch
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.attention import gqa_attention, gqa_attention_direct, gqa_attention_prefill
from neutron_mojo.nn.q_kv_cache import MultiLayerQ8KVCache, q8_gqa_attention
from neutron_mojo.nn.causal_lm import (
    embed_token,
    compute_logits,
    argmax,
    apply_temperature,
)
from neutron_mojo.model.architecture import ArchitectureConfig, ArchitectureKind


# ===----------------------------------------------------------------------=== #
# Per-Layer Weight Offsets (flat storage)
# ===----------------------------------------------------------------------=== #

struct LayerWeightOffsets(Copyable, Movable):
    """Byte/element offsets for one layer's weights in the flat tensor."""
    var attn_norm: Int
    var wq: Int
    var wk: Int
    var wv: Int
    var wo: Int
    var ffn_norm: Int
    var w_gate: Int
    var w_up: Int
    var w_down: Int

    fn __init__(out self):
        self.attn_norm = 0
        self.wq = 0
        self.wk = 0
        self.wv = 0
        self.wo = 0
        self.ffn_norm = 0
        self.w_gate = 0
        self.w_up = 0
        self.w_down = 0

    fn __copyinit__(out self, existing: Self):
        self.attn_norm = existing.attn_norm
        self.wq = existing.wq
        self.wk = existing.wk
        self.wv = existing.wv
        self.wo = existing.wo
        self.ffn_norm = existing.ffn_norm
        self.w_gate = existing.w_gate
        self.w_up = existing.w_up
        self.w_down = existing.w_down

    fn __moveinit__(out self, deinit other: Self):
        self.attn_norm = other.attn_norm
        self.wq = other.wq
        self.wk = other.wk
        self.wv = other.wv
        self.wo = other.wo
        self.ffn_norm = other.ffn_norm
        self.w_gate = other.w_gate
        self.w_up = other.w_up
        self.w_down = other.w_down


# ===----------------------------------------------------------------------=== #
# Model Configuration
# ===----------------------------------------------------------------------=== #

struct ModelParams(Copyable):
    """Model architecture parameters."""
    var num_layers: Int
    var vocab_size: Int
    var hidden_dim: Int
    var num_q_heads: Int
    var num_kv_heads: Int
    var head_dim: Int
    var ffn_dim: Int
    var max_seq_len: Int
    var rope_theta: Float64
    var arch: ArchitectureConfig

    fn __init__(out self):
        self.num_layers = 1
        self.vocab_size = 32000
        self.hidden_dim = 4096
        self.num_q_heads = 32
        self.num_kv_heads = 8
        self.head_dim = 128
        self.ffn_dim = 14336
        self.max_seq_len = 2048
        self.rope_theta = 500000.0
        self.arch = ArchitectureConfig()

    fn __copyinit__(out self, existing: Self):
        self.num_layers = existing.num_layers
        self.vocab_size = existing.vocab_size
        self.hidden_dim = existing.hidden_dim
        self.num_q_heads = existing.num_q_heads
        self.num_kv_heads = existing.num_kv_heads
        self.head_dim = existing.head_dim
        self.ffn_dim = existing.ffn_dim
        self.max_seq_len = existing.max_seq_len
        self.rope_theta = existing.rope_theta
        self.arch = existing.arch.copy()

    fn q_dim(self) -> Int:
        return self.num_q_heads * self.head_dim

    fn kv_dim(self) -> Int:
        return self.num_kv_heads * self.head_dim

    fn layer_weight_count(self) -> Int:
        """Total FP32 elements for one layer's weights."""
        var qd = self.q_dim()
        var kvd = self.kv_dim()
        var hd = self.hidden_dim
        var fd = self.ffn_dim
        return (
            hd +               # attn_norm
            qd * hd +          # wq
            kvd * hd +         # wk
            kvd * hd +         # wv
            hd * qd +          # wo
            hd +               # ffn_norm
            fd * hd +          # w_gate
            fd * hd +          # w_up
            hd * fd            # w_down
        )


fn tiny_test_params() -> ModelParams:
    """Create tiny model params for testing."""
    var p = ModelParams()
    p.num_layers = 2
    p.vocab_size = 8
    p.hidden_dim = 4
    p.num_q_heads = 2
    p.num_kv_heads = 1
    p.head_dim = 2
    p.ffn_dim = 8
    p.max_seq_len = 32
    p.rope_theta = 10000.0
    return p^


# ===----------------------------------------------------------------------=== #
# Multi-Layer Model
# ===----------------------------------------------------------------------=== #

struct Model(Movable):
    """Full N-layer causal language model.

    Stores all weights in flat tensors indexed by layer offset.
    Uses MultiLayerKVCache for efficient generation.
    """
    var params: ModelParams
    var embed: Tensor[DType.float32]          # [vocab_size, hidden_dim]
    var final_norm: Tensor[DType.float32]     # [hidden_dim]
    var lm_head: Tensor[DType.float32]        # [vocab_size, hidden_dim]
    # Per-layer weights stored flat: [num_layers * layer_weight_count]
    var layer_weights: Tensor[DType.float32]
    var layer_size: Int  # elements per layer

    fn __init__(out self, params: ModelParams):
        self.params = params.copy()
        self.layer_size = params.layer_weight_count()

        self.embed = Tensor[DType.float32](Shape(params.vocab_size, params.hidden_dim))
        self.final_norm = Tensor[DType.float32](Shape(params.hidden_dim))
        self.lm_head = Tensor[DType.float32](Shape(params.vocab_size, params.hidden_dim))
        self.layer_weights = Tensor[DType.float32](Shape(params.num_layers * self.layer_size))

        # Initialize norms to 1.0
        for i in range(params.hidden_dim):
            self.final_norm.set(i, 1.0)

        # Initialize per-layer norms to 1.0
        for layer in range(params.num_layers):
            var offsets = self._layer_offsets(layer)
            # attn_norm
            for i in range(params.hidden_dim):
                self.layer_weights.set(offsets.attn_norm + i, 1.0)
            # ffn_norm
            for i in range(params.hidden_dim):
                self.layer_weights.set(offsets.ffn_norm + i, 1.0)

    fn __moveinit__(out self, deinit other: Self):
        self.params = other.params.copy()
        self.embed = other.embed^
        self.final_norm = other.final_norm^
        self.lm_head = other.lm_head^
        self.layer_weights = other.layer_weights^
        self.layer_size = other.layer_size

    fn _layer_offsets(self, layer: Int) -> LayerWeightOffsets:
        """Compute element offsets for a layer's weights."""
        var base = layer * self.layer_size
        var p = self.params.copy()
        var qd = p.q_dim()
        var kvd = p.kv_dim()
        var hd = p.hidden_dim
        var fd = p.ffn_dim

        var off = LayerWeightOffsets()
        var cursor = base
        off.attn_norm = cursor
        cursor += hd
        off.wq = cursor
        cursor += qd * hd
        off.wk = cursor
        cursor += kvd * hd
        off.wv = cursor
        cursor += kvd * hd
        off.wo = cursor
        cursor += hd * qd
        off.ffn_norm = cursor
        cursor += hd
        off.w_gate = cursor
        cursor += fd * hd
        off.w_up = cursor
        cursor += fd * hd
        off.w_down = cursor
        return off^

    fn _get_norm(self, offset: Int, size: Int) -> Tensor[DType.float32]:
        """Extract a norm vector from layer weights."""
        var result = Tensor[DType.float32](Shape(size))
        for i in range(size):
            result.set(i, self.layer_weights.get(offset + i))
        return result^

    fn _linear_from_flat(
        self,
        x: Tensor[DType.float32],
        weight_offset: Int,
        out_dim: Int,
        in_dim: Int,
    ) -> Tensor[DType.float32]:
        """Parallel SIMD matrix-vector multiply using flat weight storage.

        Uses par_simd_matvec which auto-parallelizes for large matrices
        (>64 rows) and falls back to sequential simd_matvec for small ones.
        """
        var result = Tensor[DType.float32](Shape(out_dim))
        par_simd_matvec(result, 0, self.layer_weights, weight_offset, x, 0, out_dim, in_dim)
        return result^

    fn forward_layer(
        self,
        x: Tensor[DType.float32],
        layer: Int,
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Forward pass through a single layer.

        Args:
            x: Input [hidden_dim].
            layer: Layer index.
            cache: Multi-layer KV cache.
            rope: RoPE table.
            pos: Current position.

        Returns:
            Output [hidden_dim].
        """
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = self._layer_offsets(layer)

        # === Attention sublayer ===
        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, x, 0, self.layer_weights, off.attn_norm, hd)

        # Q/K/V projections
        var q = self._linear_from_flat(normed, off.wq, p.q_dim(), hd)
        var k = self._linear_from_flat(normed, off.wk, p.kv_dim(), hd)
        var v = self._linear_from_flat(normed, off.wv, p.kv_dim(), hd)

        # Apply RoPE (with partial rotary support for Phi)
        var rotary_dim = p.head_dim
        if p.arch.partial_rotary_factor < 1.0:
            rotary_dim = Int(Float32(p.head_dim) * p.arch.partial_rotary_factor)
            # Ensure even
            if rotary_dim % 2 != 0:
                rotary_dim -= 1

        for h in range(p.num_q_heads):
            var q_head = Tensor[DType.float32](Shape(rotary_dim))
            var base = h * p.head_dim
            for d in range(rotary_dim):
                q_head.set(d, q.get(base + d))
            apply_rope_single_head(q_head, rope, pos)
            for d in range(rotary_dim):
                q.set(base + d, q_head.get(d))

        for h in range(p.num_kv_heads):
            var k_head = Tensor[DType.float32](Shape(rotary_dim))
            var base = h * p.head_dim
            for d in range(rotary_dim):
                k_head.set(d, k.get(base + d))
            apply_rope_single_head(k_head, rope, pos)
            for d in range(rotary_dim):
                k.set(base + d, k_head.get(d))

        # Update KV cache for this layer
        cache.append_kv(layer, k, v, num_new_tokens=1)

        # GQA attention directly from multi-layer cache (zero-copy)
        var attn_out = gqa_attention_direct(q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim)

        # Output projection
        var attn_proj = self._linear_from_flat(attn_out, off.wo, hd, p.q_dim())

        # Residual: residual1 = x + attn_proj
        var residual1 = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            residual1.set(i, x.get(i) + attn_proj.get(i))

        # === FFN sublayer ===
        var ffn_normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(ffn_normed, 0, residual1, 0, self.layer_weights, off.ffn_norm, hd)

        var gate = self._linear_from_flat(ffn_normed, off.w_gate, p.ffn_dim, hd)
        var up = self._linear_from_flat(ffn_normed, off.w_up, p.ffn_dim, hd)

        # Activation dispatch based on architecture
        var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
        if p.arch.use_gelu:
            # GeLU (Phi-style): gelu(gate) * up
            from math import exp, tanh, sqrt
            for i in range(p.ffn_dim):
                var xi = gate.get(i)
                var x64 = Float64(xi)
                var gelu_val = Float32(0.5 * x64 * (1.0 + tanh(sqrt(2.0 / 3.14159265358979) * (x64 + 0.044715 * x64 * x64 * x64))))
                ffn_out.set(i, gelu_val * up.get(i))
        else:
            # SwiGLU (Llama/Mistral/Gemma default)
            simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

        var down = self._linear_from_flat(ffn_out, off.w_down, hd, p.ffn_dim)

        # Residual: output = residual1 + down
        var output = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            output.set(i, residual1.get(i) + down.get(i))

        return output^

    fn forward(
        self,
        token_id: Int,
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Full forward pass: embed → N layers → norm → logits.

        Args:
            token_id: Input token ID.
            cache: Multi-layer KV cache.
            rope: RoPE table.
            pos: Current position.

        Returns:
            Logits [vocab_size].
        """
        var hidden = embed_token(self.embed, token_id, self.params.hidden_dim)

        for layer in range(self.params.num_layers):
            hidden = self.forward_layer(hidden, layer, cache, rope, pos)

        var normed = Tensor[DType.float32](Shape(self.params.hidden_dim))
        simd_rmsnorm(normed, 0, hidden, 0, self.final_norm, 0, self.params.hidden_dim)
        # LM head is the largest matvec (vocab_size rows) — use parallel version
        var logits = Tensor[DType.float32](Shape(self.params.vocab_size))
        par_simd_matvec(logits, 0, self.lm_head, 0, normed, 0, self.params.vocab_size, self.params.hidden_dim)
        return logits^

    fn forward_layer_q8cache(
        self,
        x: Tensor[DType.float32],
        layer: Int,
        mut cache: MultiLayerQ8KVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Forward pass through a single layer using Q8 KV cache.

        Same as forward_layer but stores K/V quantized and uses Q8 attention.
        """
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = self._layer_offsets(layer)

        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, x, 0, self.layer_weights, off.attn_norm, hd)

        var q = self._linear_from_flat(normed, off.wq, p.q_dim(), hd)
        var k = self._linear_from_flat(normed, off.wk, p.kv_dim(), hd)
        var v = self._linear_from_flat(normed, off.wv, p.kv_dim(), hd)

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

        # Append to Q8 cache (quantizes K/V on append)
        cache.append_kv(layer, k, v, num_new_tokens=1)

        # Q8 attention using extracted layer cache
        var layer_cache = cache.get_layer_cache(layer)
        var attn_out = q8_gqa_attention(
            q, layer_cache, p.num_q_heads, p.num_kv_heads, p.head_dim
        )

        var attn_proj = self._linear_from_flat(attn_out, off.wo, hd, p.q_dim())

        var residual1 = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            residual1.set(i, x.get(i) + attn_proj.get(i))

        var ffn_normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(ffn_normed, 0, residual1, 0, self.layer_weights, off.ffn_norm, hd)

        var gate = self._linear_from_flat(ffn_normed, off.w_gate, p.ffn_dim, hd)
        var up = self._linear_from_flat(ffn_normed, off.w_up, p.ffn_dim, hd)

        var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
        simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

        var down = self._linear_from_flat(ffn_out, off.w_down, hd, p.ffn_dim)

        var output = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            output.set(i, residual1.get(i) + down.get(i))

        return output^

    fn forward_q8cache(
        self,
        token_id: Int,
        mut cache: MultiLayerQ8KVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Full forward pass with Q8 KV cache: embed → N layers → norm → logits."""
        var hidden = embed_token(self.embed, token_id, self.params.hidden_dim)

        for layer in range(self.params.num_layers):
            hidden = self.forward_layer_q8cache(hidden, layer, cache, rope, pos)

        var normed = Tensor[DType.float32](Shape(self.params.hidden_dim))
        simd_rmsnorm(normed, 0, hidden, 0, self.final_norm, 0, self.params.hidden_dim)
        var logits = Tensor[DType.float32](Shape(self.params.vocab_size))
        par_simd_matvec(logits, 0, self.lm_head, 0, normed, 0, self.params.vocab_size, self.params.hidden_dim)
        return logits^

    # === Fused Forward (Sprint 15) ===

    fn forward_layer_fused(
        self,
        x: Tensor[DType.float32],
        layer: Int,
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Forward pass through a single layer with operator fusion.

        Uses fused kernels to reduce memory traffic:
        - fused_rmsnorm_matvec for attention Q/K/V projections
        - fused_matvec_residual_add for output + residual and down + residual

        Args:
            x: Input [hidden_dim].
            layer: Layer index.
            cache: Multi-layer KV cache.
            rope: RoPE table.
            pos: Current position.

        Returns:
            Output [hidden_dim].
        """
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = self._layer_offsets(layer)

        # === Fused RMSNorm + Q/K/V projections ===
        # Instead of: normed = rmsnorm(x); q = W_q @ normed
        # Do: q = W_q @ rmsnorm(x) in one pass
        var q = Tensor[DType.float32](Shape(p.q_dim()))
        fused_rmsnorm_matvec(
            q, 0, x, 0,
            self.layer_weights, off.attn_norm,
            self.layer_weights, off.wq,
            hd, p.q_dim(),
        )

        var k = Tensor[DType.float32](Shape(p.kv_dim()))
        fused_rmsnorm_matvec(
            k, 0, x, 0,
            self.layer_weights, off.attn_norm,
            self.layer_weights, off.wk,
            hd, p.kv_dim(),
        )

        var v = Tensor[DType.float32](Shape(p.kv_dim()))
        fused_rmsnorm_matvec(
            v, 0, x, 0,
            self.layer_weights, off.attn_norm,
            self.layer_weights, off.wv,
            hd, p.kv_dim(),
        )

        # Apply RoPE (same as unfused)
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

        # KV cache + attention (zero-copy direct access)
        cache.append_kv(layer, k, v, num_new_tokens=1)
        var attn_out = gqa_attention_direct(q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim)

        # === Fused output projection + residual add ===
        var residual1 = Tensor[DType.float32](Shape(hd))
        fused_matvec_residual_add(
            residual1, 0, x, 0,
            self.layer_weights, off.wo,
            attn_out, 0, hd, p.q_dim(),
        )

        # === Fused RMSNorm + FFN gate/up projections ===
        var gate = Tensor[DType.float32](Shape(p.ffn_dim))
        fused_rmsnorm_matvec(
            gate, 0, residual1, 0,
            self.layer_weights, off.ffn_norm,
            self.layer_weights, off.w_gate,
            hd, p.ffn_dim,
        )

        var up = Tensor[DType.float32](Shape(p.ffn_dim))
        fused_rmsnorm_matvec(
            up, 0, residual1, 0,
            self.layer_weights, off.ffn_norm,
            self.layer_weights, off.w_up,
            hd, p.ffn_dim,
        )

        # SwiGLU (already fused)
        var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
        simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

        # === Fused down projection + residual add ===
        var output = Tensor[DType.float32](Shape(hd))
        fused_matvec_residual_add(
            output, 0, residual1, 0,
            self.layer_weights, off.w_down,
            ffn_out, 0, hd, p.ffn_dim,
        )

        return output^

    fn forward_fused(
        self,
        token_id: Int,
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Full forward pass with operator fusion: embed → fused layers → norm → logits.

        Uses fused kernels for better cache utilization and fewer memory passes.
        Produces identical results to forward() (within floating-point tolerance).

        Args:
            token_id: Input token ID.
            cache: Multi-layer KV cache.
            rope: RoPE table.
            pos: Current position.

        Returns:
            Logits [vocab_size].
        """
        var hidden = embed_token(self.embed, token_id, self.params.hidden_dim)

        for layer in range(self.params.num_layers):
            hidden = self.forward_layer_fused(hidden, layer, cache, rope, pos)

        var normed = Tensor[DType.float32](Shape(self.params.hidden_dim))
        simd_rmsnorm(normed, 0, hidden, 0, self.final_norm, 0, self.params.hidden_dim)
        var logits = Tensor[DType.float32](Shape(self.params.vocab_size))
        par_simd_matvec(logits, 0, self.lm_head, 0, normed, 0, self.params.vocab_size, self.params.hidden_dim)
        return logits^


    # === Batch Prefill (Sprint 25) ===

    fn _batch_linear_from_flat(
        self,
        x_batch: Tensor[DType.float32],
        num_tokens: Int,
        weight_offset: Int,
        out_dim: Int,
        in_dim: Int,
    ) -> Tensor[DType.float32]:
        """Batch SIMD-vectorized matrix-vector multiply using flat weight storage.

        Processes num_tokens vectors at once with shared weight matrix.
        """
        var result = Tensor[DType.float32](Shape(num_tokens * out_dim))
        simd_batch_matvec(
            result, 0, self.layer_weights, weight_offset,
            x_batch, 0, num_tokens, out_dim, in_dim,
        )
        return result^

    fn forward_layer_prefill(
        self,
        x_batch: Tensor[DType.float32],
        num_tokens: Int,
        layer: Int,
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        start_pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Forward pass through a single layer for batch of tokens (prefill).

        Batches the Q/K/V projections and FFN across all tokens.
        Attention is still per-token (causal mask requires sequential attention).

        Args:
            x_batch: Input [num_tokens * hidden_dim] flattened.
            num_tokens: Number of prompt tokens.
            layer: Layer index.
            cache: Multi-layer KV cache.
            rope: RoPE table.
            start_pos: Starting position in sequence.

        Returns:
            Output [num_tokens * hidden_dim] flattened.
        """
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = self._layer_offsets(layer)

        # === Batch RMSNorm ===
        var normed_batch = Tensor[DType.float32](Shape(num_tokens * hd))
        simd_batch_rmsnorm(
            normed_batch, 0, x_batch, 0,
            self.layer_weights, off.attn_norm,
            num_tokens, hd,
        )

        # === Batch Q/K/V projections ===
        var q_batch = self._batch_linear_from_flat(normed_batch, num_tokens, off.wq, p.q_dim(), hd)
        var k_batch = self._batch_linear_from_flat(normed_batch, num_tokens, off.wk, p.kv_dim(), hd)
        var v_batch = self._batch_linear_from_flat(normed_batch, num_tokens, off.wv, p.kv_dim(), hd)

        # === Batch RoPE (all tokens at once) ===
        apply_rope_batch(
            q_batch, k_batch, rope, start_pos, num_tokens,
            p.num_q_heads, p.num_kv_heads, p.head_dim,
        )

        # === Bulk KV cache insertion (all tokens at once) ===
        cache.append_kv(layer, k_batch, v_batch, num_new_tokens=num_tokens)

        # === Batched causal attention (reads directly from cache) ===
        var attn_out_batch = gqa_attention_prefill(
            q_batch, cache, layer, num_tokens, start_pos,
            p.num_q_heads, p.num_kv_heads, p.head_dim,
        )

        # === Batch output projection ===
        var proj_batch = self._batch_linear_from_flat(attn_out_batch, num_tokens, off.wo, hd, p.q_dim())

        # === Batch residual: residual1 = x_batch + proj_batch ===
        var residual1 = Tensor[DType.float32](Shape(num_tokens * hd))
        simd_batch_add(residual1, 0, x_batch, 0, proj_batch, 0, num_tokens, hd)

        # === Batch FFN RMSNorm ===
        var ffn_normed = Tensor[DType.float32](Shape(num_tokens * hd))
        simd_batch_rmsnorm(
            ffn_normed, 0, residual1, 0,
            self.layer_weights, off.ffn_norm,
            num_tokens, hd,
        )

        # === Batch FFN gate/up projections ===
        var gate_batch = self._batch_linear_from_flat(ffn_normed, num_tokens, off.w_gate, p.ffn_dim, hd)
        var up_batch = self._batch_linear_from_flat(ffn_normed, num_tokens, off.w_up, p.ffn_dim, hd)

        # === Batch SwiGLU ===
        var ffn_out = Tensor[DType.float32](Shape(num_tokens * p.ffn_dim))
        simd_batch_swiglu(ffn_out, 0, gate_batch, 0, up_batch, 0, num_tokens, p.ffn_dim)

        # === Batch down projection ===
        var down_batch = self._batch_linear_from_flat(ffn_out, num_tokens, off.w_down, hd, p.ffn_dim)

        # === Batch residual: output = residual1 + down_batch ===
        var output = Tensor[DType.float32](Shape(num_tokens * hd))
        simd_batch_add(output, 0, residual1, 0, down_batch, 0, num_tokens, hd)

        return output^

    fn forward_prefill(
        self,
        token_ids: List[Int],
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        start_pos: Int = 0,
    ) raises -> Tensor[DType.float32]:
        """Batch prefill: process all prompt tokens through the model at once.

        Instead of N sequential forward() calls, batches the Q/K/V and FFN
        projections across all tokens. Returns logits for the last token only.

        Performance: For a prompt of N tokens, reduces the number of matvec
        operations from 7*N*num_layers individual calls to 7*num_layers batch calls.

        Args:
            token_ids: List of prompt token IDs.
            cache: Multi-layer KV cache (will be filled with N entries).
            rope: RoPE table.
            start_pos: Starting position offset (default 0).

        Returns:
            Logits [vocab_size] for the last token.
        """
        var N = len(token_ids)
        var hd = self.params.hidden_dim

        # Embed all tokens into a batch tensor [N * hidden_dim]
        var hidden_batch = Tensor[DType.float32](Shape(N * hd))
        for i in range(N):
            var emb = embed_token(self.embed, token_ids[i], hd)
            for d in range(hd):
                hidden_batch.set(i * hd + d, emb.get(d))

        # Process through all layers
        for layer in range(self.params.num_layers):
            hidden_batch = self.forward_layer_prefill(
                hidden_batch, N, layer, cache, rope, start_pos,
            )

        # Extract last token's hidden state and compute logits
        var last_hidden = Tensor[DType.float32](Shape(hd))
        var last_off = (N - 1) * hd
        for d in range(hd):
            last_hidden.set(d, hidden_batch.get(last_off + d))

        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, last_hidden, 0, self.final_norm, 0, hd)
        var logits = Tensor[DType.float32](Shape(self.params.vocab_size))
        par_simd_matvec(logits, 0, self.lm_head, 0, normed, 0, self.params.vocab_size, hd)
        return logits^


# ===----------------------------------------------------------------------=== #
# Generation
# ===----------------------------------------------------------------------=== #

fn generate(
    model: Model,
    prompt_tokens: List[Int],
    max_new_tokens: Int,
    temperature: Float32 = 1.0,
) raises -> List[Int]:
    """Autoregressive text generation.

    Args:
        model: The language model.
        prompt_tokens: Input token IDs.
        max_new_tokens: Max tokens to generate.
        temperature: Sampling temperature (1.0 = greedy-ish).

    Returns:
        Generated token IDs (not including prompt).
    """
    var p = model.params.copy()
    var total_len = len(prompt_tokens) + max_new_tokens

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

    var generated = List[Int]()

    # Prefill: process all prompt tokens
    var logits = Tensor[DType.float32](Shape(p.vocab_size))
    for i in range(len(prompt_tokens)):
        logits = model.forward(prompt_tokens[i], cache, rope, pos=i)

    # Decode: generate new tokens one at a time
    for step in range(max_new_tokens):
        if temperature != 1.0 and temperature > 0.0:
            apply_temperature(logits, p.vocab_size, temperature)

        var next_token = argmax(logits, p.vocab_size)
        generated.append(next_token)

        var pos = len(prompt_tokens) + step
        logits = model.forward(next_token, cache, rope, pos=pos)

    return generated^
