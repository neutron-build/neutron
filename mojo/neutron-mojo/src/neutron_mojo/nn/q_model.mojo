# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantized Model (Q8)
# ===----------------------------------------------------------------------=== #

"""Q8-quantized multi-layer causal language model.

Same architecture as Model but projection weights are stored as Q8_0
quantized values with per-block scales. Norm weights, embeddings, and
LM head remain FP32. Dequantization happens on-the-fly during
matrix-vector multiplies via simd_q8_matvec.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    simd_q8_matvec,
    simd_rmsnorm,
    simd_swiglu,
    simd_axpy,
    par_simd_matvec,
)
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.nn.attention import gqa_attention, gqa_attention_direct
from neutron_mojo.nn.q_kv_cache import MultiLayerQ8KVCache, q8_gqa_attention
from neutron_mojo.nn.causal_lm import embed_token, argmax, apply_temperature
from neutron_mojo.nn.model import Model, ModelParams, LayerWeightOffsets


# ===----------------------------------------------------------------------=== #
# Scale Offsets
# ===----------------------------------------------------------------------=== #

struct LayerScaleOffsets(Copyable, Movable):
    """Scale tensor offsets for one layer's quantized projections."""
    var wq: Int
    var wk: Int
    var wv: Int
    var wo: Int
    var w_gate: Int
    var w_up: Int
    var w_down: Int

    fn __init__(out self):
        self.wq = 0
        self.wk = 0
        self.wv = 0
        self.wo = 0
        self.w_gate = 0
        self.w_up = 0
        self.w_down = 0

    fn __copyinit__(out self, existing: Self):
        self.wq = existing.wq
        self.wk = existing.wk
        self.wv = existing.wv
        self.wo = existing.wo
        self.w_gate = existing.w_gate
        self.w_up = existing.w_up
        self.w_down = existing.w_down

    fn __moveinit__(out self, deinit other: Self):
        self.wq = other.wq
        self.wk = other.wk
        self.wv = other.wv
        self.wo = other.wo
        self.w_gate = other.w_gate
        self.w_up = other.w_up
        self.w_down = other.w_down


fn _num_blocks(in_features: Int, block_size: Int) -> Int:
    return (in_features + block_size - 1) // block_size


fn _scales_count(out_features: Int, in_features: Int, block_size: Int) -> Int:
    return out_features * _num_blocks(in_features, block_size)


# ===----------------------------------------------------------------------=== #
# Quantized Model
# ===----------------------------------------------------------------------=== #

struct QuantizedModel(Movable):
    """Q8-quantized N-layer causal language model.

    Mirrors Model structure but projection weights are Q8_0 quantized.
    Norms, embeddings, and LM head remain FP32.
    """
    var params: ModelParams
    var embed: Tensor[DType.float32]
    var final_norm: Tensor[DType.float32]
    var lm_head: Tensor[DType.float32]
    var layer_weights: Tensor[DType.float32]  # Same layout as Model (projections quantized)
    var layer_scales: Tensor[DType.float32]   # Per-block scales for all layers
    var layer_size: Int
    var block_size: Int
    var scales_per_layer: Int

    fn __init__(out self, params: ModelParams, block_size: Int = 32):
        self.params = params.copy()
        self.layer_size = params.layer_weight_count()
        self.block_size = block_size

        self.embed = Tensor[DType.float32](Shape(params.vocab_size, params.hidden_dim))
        self.final_norm = Tensor[DType.float32](Shape(params.hidden_dim))
        self.lm_head = Tensor[DType.float32](Shape(params.vocab_size, params.hidden_dim))
        self.layer_weights = Tensor[DType.float32](Shape(params.num_layers * self.layer_size))

        # Compute scales per layer
        var p = params.copy()
        var qd = p.q_dim()
        var kvd = p.kv_dim()
        var hd = p.hidden_dim
        var fd = p.ffn_dim
        self.scales_per_layer = (
            _scales_count(qd, hd, block_size) +
            _scales_count(kvd, hd, block_size) +
            _scales_count(kvd, hd, block_size) +
            _scales_count(hd, qd, block_size) +
            _scales_count(fd, hd, block_size) +
            _scales_count(fd, hd, block_size) +
            _scales_count(hd, fd, block_size)
        )
        self.layer_scales = Tensor[DType.float32](
            Shape(params.num_layers * self.scales_per_layer)
        )

        # Initialize norms to 1.0
        for i in range(params.hidden_dim):
            self.final_norm.set(i, 1.0)
        for layer in range(params.num_layers):
            var offsets = self._layer_offsets(layer)
            for i in range(params.hidden_dim):
                self.layer_weights.set(offsets.attn_norm + i, 1.0)
                self.layer_weights.set(offsets.ffn_norm + i, 1.0)

    fn __moveinit__(out self, deinit other: Self):
        self.params = other.params.copy()
        self.embed = other.embed^
        self.final_norm = other.final_norm^
        self.lm_head = other.lm_head^
        self.layer_weights = other.layer_weights^
        self.layer_scales = other.layer_scales^
        self.layer_size = other.layer_size
        self.block_size = other.block_size
        self.scales_per_layer = other.scales_per_layer

    fn _layer_offsets(self, layer: Int) -> LayerWeightOffsets:
        """Compute data offsets (same layout as Model)."""
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

    fn _layer_scale_offsets(self, layer: Int) -> LayerScaleOffsets:
        """Compute scale offsets for a layer's projections."""
        var base = layer * self.scales_per_layer
        var p = self.params.copy()
        var qd = p.q_dim()
        var kvd = p.kv_dim()
        var hd = p.hidden_dim
        var fd = p.ffn_dim
        var bs = self.block_size

        var soff = LayerScaleOffsets()
        var cursor = base
        soff.wq = cursor
        cursor += _scales_count(qd, hd, bs)
        soff.wk = cursor
        cursor += _scales_count(kvd, hd, bs)
        soff.wv = cursor
        cursor += _scales_count(kvd, hd, bs)
        soff.wo = cursor
        cursor += _scales_count(hd, qd, bs)
        soff.w_gate = cursor
        cursor += _scales_count(fd, hd, bs)
        soff.w_up = cursor
        cursor += _scales_count(fd, hd, bs)
        soff.w_down = cursor
        return soff^

    fn _q8_linear_from_flat(
        self,
        x: Tensor[DType.float32],
        data_offset: Int,
        scales_offset: Int,
        out_dim: Int,
        in_dim: Int,
    ) -> Tensor[DType.float32]:
        """Q8 dequant-on-the-fly matrix-vector multiply from flat storage."""
        var result = Tensor[DType.float32](Shape(out_dim))
        simd_q8_matvec(
            result, 0, self.layer_weights, data_offset,
            self.layer_scales, scales_offset,
            x, 0, out_dim, in_dim, self.block_size,
        )
        return result^

    fn forward_layer(
        self,
        x: Tensor[DType.float32],
        layer: Int,
        mut cache: MultiLayerKVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Forward pass through a single layer using Q8 weights."""
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = self._layer_offsets(layer)
        var soff = self._layer_scale_offsets(layer)

        # === Attention sublayer ===
        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, x, 0, self.layer_weights, off.attn_norm, hd)

        # Q/K/V projections (quantized)
        var q = self._q8_linear_from_flat(normed, off.wq, soff.wq, p.q_dim(), hd)
        var k = self._q8_linear_from_flat(normed, off.wk, soff.wk, p.kv_dim(), hd)
        var v = self._q8_linear_from_flat(normed, off.wv, soff.wv, p.kv_dim(), hd)

        # Apply RoPE (with partial rotary support)
        var rotary_dim = p.head_dim
        if p.arch.partial_rotary_factor < 1.0:
            rotary_dim = Int(Float32(p.head_dim) * p.arch.partial_rotary_factor)
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

        # Update KV cache
        cache.append_kv(layer, k, v, num_new_tokens=1)

        # GQA attention directly from multi-layer cache (zero-copy)
        var attn_out = gqa_attention_direct(
            q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim
        )

        # Output projection (quantized)
        var attn_proj = self._q8_linear_from_flat(
            attn_out, off.wo, soff.wo, hd, p.q_dim()
        )

        # Residual
        var residual1 = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            residual1.set(i, x.get(i) + attn_proj.get(i))

        # === FFN sublayer ===
        var ffn_normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(
            ffn_normed, 0, residual1, 0, self.layer_weights, off.ffn_norm, hd
        )

        var gate = self._q8_linear_from_flat(
            ffn_normed, off.w_gate, soff.w_gate, p.ffn_dim, hd
        )
        var up = self._q8_linear_from_flat(
            ffn_normed, off.w_up, soff.w_up, p.ffn_dim, hd
        )

        # Activation dispatch based on architecture
        var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
        if p.arch.use_gelu:
            from math import exp, tanh, sqrt
            for i in range(p.ffn_dim):
                var xi = gate.get(i)
                var x64 = Float64(xi)
                var gelu_val = Float32(0.5 * x64 * (1.0 + tanh(sqrt(2.0 / 3.14159265358979) * (x64 + 0.044715 * x64 * x64 * x64))))
                ffn_out.set(i, gelu_val * up.get(i))
        else:
            simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

        var down = self._q8_linear_from_flat(
            ffn_out, off.w_down, soff.w_down, hd, p.ffn_dim
        )

        # Residual
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
        """Full forward pass: embed → N Q8 layers → norm → logits."""
        var hidden = embed_token(self.embed, token_id, self.params.hidden_dim)

        for layer in range(self.params.num_layers):
            hidden = self.forward_layer(hidden, layer, cache, rope, pos)

        var normed = Tensor[DType.float32](Shape(self.params.hidden_dim))
        simd_rmsnorm(
            normed, 0, hidden, 0, self.final_norm, 0, self.params.hidden_dim
        )
        # LM head stays FP32
        var logits = Tensor[DType.float32](Shape(self.params.vocab_size))
        par_simd_matvec(
            logits, 0, self.lm_head, 0, normed, 0,
            self.params.vocab_size, self.params.hidden_dim,
        )
        return logits^

    fn forward_layer_q8cache(
        self,
        x: Tensor[DType.float32],
        layer: Int,
        mut cache: MultiLayerQ8KVCache,
        rope: RoPETable,
        pos: Int,
    ) raises -> Tensor[DType.float32]:
        """Forward pass through a single Q8 layer using Q8 KV cache."""
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = self._layer_offsets(layer)
        var soff = self._layer_scale_offsets(layer)

        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, x, 0, self.layer_weights, off.attn_norm, hd)

        var q = self._q8_linear_from_flat(normed, off.wq, soff.wq, p.q_dim(), hd)
        var k = self._q8_linear_from_flat(normed, off.wk, soff.wk, p.kv_dim(), hd)
        var v = self._q8_linear_from_flat(normed, off.wv, soff.wv, p.kv_dim(), hd)

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

        cache.append_kv(layer, k, v, num_new_tokens=1)

        var layer_cache = cache.get_layer_cache(layer)
        var attn_out = q8_gqa_attention(
            q, layer_cache, p.num_q_heads, p.num_kv_heads, p.head_dim
        )

        var attn_proj = self._q8_linear_from_flat(
            attn_out, off.wo, soff.wo, hd, p.q_dim()
        )

        var residual1 = Tensor[DType.float32](Shape(hd))
        for i in range(hd):
            residual1.set(i, x.get(i) + attn_proj.get(i))

        var ffn_normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(
            ffn_normed, 0, residual1, 0, self.layer_weights, off.ffn_norm, hd
        )

        var gate = self._q8_linear_from_flat(
            ffn_normed, off.w_gate, soff.w_gate, p.ffn_dim, hd
        )
        var up = self._q8_linear_from_flat(
            ffn_normed, off.w_up, soff.w_up, p.ffn_dim, hd
        )

        var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
        simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

        var down = self._q8_linear_from_flat(
            ffn_out, off.w_down, soff.w_down, hd, p.ffn_dim
        )

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
        """Full forward pass with Q8 KV cache."""
        var hidden = embed_token(self.embed, token_id, self.params.hidden_dim)

        for layer in range(self.params.num_layers):
            hidden = self.forward_layer_q8cache(hidden, layer, cache, rope, pos)

        var normed = Tensor[DType.float32](Shape(self.params.hidden_dim))
        simd_rmsnorm(
            normed, 0, hidden, 0, self.final_norm, 0, self.params.hidden_dim
        )
        var logits = Tensor[DType.float32](Shape(self.params.vocab_size))
        par_simd_matvec(
            logits, 0, self.lm_head, 0, normed, 0,
            self.params.vocab_size, self.params.hidden_dim,
        )
        return logits^


# ===----------------------------------------------------------------------=== #
# Quantization
# ===----------------------------------------------------------------------=== #

fn _quantize_projection(
    src: Tensor[DType.float32],
    src_offset: Int,
    mut dst: Tensor[DType.float32],
    dst_offset: Int,
    mut scales: Tensor[DType.float32],
    scales_offset: Int,
    out_features: Int,
    in_features: Int,
    block_size: Int,
):
    """Quantize one projection from src to dst with Q8_0."""
    var num_blocks = _num_blocks(in_features, block_size)

    for row in range(out_features):
        for b in range(num_blocks):
            var start = b * block_size
            var end = start + block_size
            if end > in_features:
                end = in_features

            # Find absmax for this block
            var absmax: Float32 = 0.0
            for j in range(start, end):
                var val = src.get(src_offset + row * in_features + j)
                if val < 0.0:
                    val = -val
                if val > absmax:
                    absmax = val

            if absmax == 0.0:
                absmax = 1.0

            var scale = absmax / 127.0
            scales.set(scales_offset + row * num_blocks + b, scale)

            # Quantize values
            for j in range(start, end):
                var val = src.get(src_offset + row * in_features + j)
                var q = val / scale
                if q > 127.0:
                    q = 127.0
                elif q < -127.0:
                    q = -127.0
                if q >= 0:
                    q = Float32(Int(q + 0.5))
                else:
                    q = Float32(Int(q - 0.5))
                dst.set(dst_offset + row * in_features + j, q)


fn quantize_from_model(model: Model, block_size: Int = 32) -> QuantizedModel:
    """Convert FP32 Model to Q8 QuantizedModel.

    Quantizes all 7 projection weight matrices per layer.
    Norms, embeddings, and LM head remain FP32.

    Args:
        model: Source FP32 model.
        block_size: Quantization block size.

    Returns:
        QuantizedModel with Q8-quantized projections.
    """
    var qm = QuantizedModel(model.params, block_size)
    var p = model.params.copy()

    # Copy FP32 weights: embed, final_norm, lm_head
    # Use 2D get(row, col) for embed/lm_head (data_ptr() aliasing bug in Mojo 0.26.2)
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            qm.embed.set(v * p.hidden_dim + d, model.embed.get(v, d))
            qm.lm_head.set(v * p.hidden_dim + d, model.lm_head.get(v, d))
    for i in range(p.hidden_dim):
        qm.final_norm.set(i, model.final_norm.get(i))

    # Quantize each layer's projections
    for layer in range(p.num_layers):
        var off = qm._layer_offsets(layer)
        var soff = qm._layer_scale_offsets(layer)

        # Copy norm weights as-is (FP32)
        for i in range(p.hidden_dim):
            qm.layer_weights.set(
                off.attn_norm + i, model.layer_weights.get(off.attn_norm + i)
            )
            qm.layer_weights.set(
                off.ffn_norm + i, model.layer_weights.get(off.ffn_norm + i)
            )

        # Quantize 7 projections
        _quantize_projection(
            model.layer_weights, off.wq,
            qm.layer_weights, off.wq,
            qm.layer_scales, soff.wq,
            p.q_dim(), p.hidden_dim, block_size,
        )
        _quantize_projection(
            model.layer_weights, off.wk,
            qm.layer_weights, off.wk,
            qm.layer_scales, soff.wk,
            p.kv_dim(), p.hidden_dim, block_size,
        )
        _quantize_projection(
            model.layer_weights, off.wv,
            qm.layer_weights, off.wv,
            qm.layer_scales, soff.wv,
            p.kv_dim(), p.hidden_dim, block_size,
        )
        _quantize_projection(
            model.layer_weights, off.wo,
            qm.layer_weights, off.wo,
            qm.layer_scales, soff.wo,
            p.hidden_dim, p.q_dim(), block_size,
        )
        _quantize_projection(
            model.layer_weights, off.w_gate,
            qm.layer_weights, off.w_gate,
            qm.layer_scales, soff.w_gate,
            p.ffn_dim, p.hidden_dim, block_size,
        )
        _quantize_projection(
            model.layer_weights, off.w_up,
            qm.layer_weights, off.w_up,
            qm.layer_scales, soff.w_up,
            p.ffn_dim, p.hidden_dim, block_size,
        )
        _quantize_projection(
            model.layer_weights, off.w_down,
            qm.layer_weights, off.w_down,
            qm.layer_scales, soff.w_down,
            p.hidden_dim, p.ffn_dim, block_size,
        )

    return qm^


# ===----------------------------------------------------------------------=== #
# Generation
# ===----------------------------------------------------------------------=== #

fn q_generate(
    model: QuantizedModel,
    prompt_tokens: List[Int],
    max_new_tokens: Int,
    temperature: Float32 = 1.0,
) raises -> List[Int]:
    """Autoregressive generation with quantized model.

    Args:
        model: Quantized language model.
        prompt_tokens: Input token IDs.
        max_new_tokens: Max tokens to generate.
        temperature: Sampling temperature.

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

    # Prefill
    var logits = Tensor[DType.float32](Shape(p.vocab_size))
    for i in range(len(prompt_tokens)):
        logits = model.forward(prompt_tokens[i], cache, rope, pos=i)

    # Decode
    for step in range(max_new_tokens):
        if temperature != 1.0 and temperature > 0.0:
            apply_temperature(logits, p.vocab_size, temperature)

        var next_token = argmax(logits, p.vocab_size)
        generated.append(next_token)

        var pos = len(prompt_tokens) + step
        logits = model.forward(next_token, cache, rope, pos=pos)

    return generated^
