# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Mixed Precision Quantization
# ===----------------------------------------------------------------------=== #

"""Adaptive mixed-precision quantization for per-layer Q8/Q4/FP32 control.

Allows assigning different quantization levels to each transformer layer
based on sensitivity analysis. Sensitive layers (typically early + late)
can use Q8 for quality, while less sensitive layers (middle) use Q4 for
speed. FP32 is available for maximum quality on critical layers.

Usage:
    # Analyze per-layer sensitivity
    var sens = analyze_sensitivity(model)

    # Auto-select modes based on Q4 error threshold
    var modes = auto_calibrate(sens, q4_threshold=0.01)

    # Create mixed-precision model
    var mixed = quantize_mixed(model, modes)

    # Generate
    var tokens = mixed_generate(mixed, prompt, max_tokens)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    simd_q8_matvec,
    simd_rmsnorm,
    simd_swiglu,
    par_simd_matvec,
)
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.attention import gqa_attention_direct
from neutron_mojo.nn.causal_lm import embed_token, argmax, apply_temperature
from neutron_mojo.nn.model import Model, ModelParams, LayerWeightOffsets
from neutron_mojo.nn.q_model import (
    LayerScaleOffsets, _num_blocks, _scales_count, _quantize_projection,
)
from neutron_mojo.nn.q4_model import _quantize_projection_q4


# ===----------------------------------------------------------------------=== #
# Layer Sensitivity
# ===----------------------------------------------------------------------=== #

struct LayerSensitivity(Copyable, Movable):
    """Quantization error measurements for a single layer.

    Fields:
        q8_error: Average MAE across all 7 projections at Q8 precision.
        q4_error: Average MAE across all 7 projections at Q4 precision.
    """
    var q8_error: Float32
    var q4_error: Float32

    fn __init__(out self):
        self.q8_error = 0.0
        self.q4_error = 0.0

    fn __copyinit__(out self, existing: Self):
        self.q8_error = existing.q8_error
        self.q4_error = existing.q4_error

    fn __moveinit__(out self, deinit other: Self):
        self.q8_error = other.q8_error
        self.q4_error = other.q4_error


# ===----------------------------------------------------------------------=== #
# Helpers
# ===----------------------------------------------------------------------=== #

fn _compute_offsets(params: ModelParams, layer: Int) -> LayerWeightOffsets:
    """Compute layer weight offsets (standalone version of Model._layer_offsets)."""
    var base = layer * params.layer_weight_count()
    var p = params.copy()
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


fn _compute_scale_offsets(
    params: ModelParams, layer: Int, scales_per_layer: Int, block_size: Int,
) -> LayerScaleOffsets:
    """Compute scale offsets for a layer's projections."""
    var base = layer * scales_per_layer
    var p = params.copy()
    var qd = p.q_dim()
    var kvd = p.kv_dim()
    var hd = p.hidden_dim
    var fd = p.ffn_dim
    var bs = block_size

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


fn _quant_roundtrip_error(
    weights: Tensor[DType.float32],
    offset: Int,
    out_features: Int,
    in_features: Int,
    block_size: Int,
    q_max: Float32,
    q_min: Float32,
) -> Float32:
    """Compute MAE from quantize-dequantize roundtrip for one projection.

    Args:
        weights: Flat weight tensor.
        offset: Starting offset in the tensor.
        out_features: Output dimension (rows).
        in_features: Input dimension (columns).
        block_size: Quantization block size.
        q_max: Maximum quantized value (127 for Q8, 7 for Q4).
        q_min: Minimum quantized value (-127 for Q8, -8 for Q4).

    Returns:
        Mean absolute error.
    """
    var total_err: Float32 = 0.0
    var count = out_features * in_features
    var num_blocks = (in_features + block_size - 1) // block_size

    for row in range(out_features):
        for b in range(num_blocks):
            var start = b * block_size
            var end = start + block_size
            if end > in_features:
                end = in_features

            # Find absmax for this block
            var absmax: Float32 = 0.0
            for j in range(start, end):
                var val = weights.get(offset + row * in_features + j)
                if val < 0.0:
                    val = -val
                if val > absmax:
                    absmax = val

            if absmax == 0.0:
                absmax = 1.0
            var scale = absmax / q_max

            # Quantize, dequantize, measure error
            for j in range(start, end):
                var orig = weights.get(offset + row * in_features + j)
                var q = orig / scale
                if q > q_max:
                    q = q_max
                elif q < q_min:
                    q = q_min
                if q >= 0:
                    q = Float32(Int(q + 0.5))
                else:
                    q = Float32(Int(q - 0.5))
                var dequant = q * scale
                var err = orig - dequant
                if err < 0.0:
                    err = -err
                total_err += err

    if count == 0:
        return 0.0
    return total_err / Float32(count)


# ===----------------------------------------------------------------------=== #
# Sensitivity Analysis
# ===----------------------------------------------------------------------=== #

fn measure_layer_sensitivity(
    model: Model,
    layer: Int,
    block_size: Int = 32,
) -> LayerSensitivity:
    """Measure quantization sensitivity for a single layer.

    Computes the mean absolute error (MAE) from quantize-dequantize roundtrip
    for all 7 projection weights at both Q8 and Q4 precision.

    Args:
        model: FP32 model.
        layer: Layer index.
        block_size: Quantization block size.

    Returns:
        LayerSensitivity with Q8 and Q4 error measurements.
    """
    var p = model.params.copy()
    var off = _compute_offsets(p, layer)

    var q8_total: Float32 = 0.0
    var q4_total: Float32 = 0.0

    # wq: (q_dim, hidden_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.wq, p.q_dim(), p.hidden_dim, block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.wq, p.q_dim(), p.hidden_dim, block_size, 7.0, -8.0)

    # wk: (kv_dim, hidden_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.wk, p.kv_dim(), p.hidden_dim, block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.wk, p.kv_dim(), p.hidden_dim, block_size, 7.0, -8.0)

    # wv: (kv_dim, hidden_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.wv, p.kv_dim(), p.hidden_dim, block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.wv, p.kv_dim(), p.hidden_dim, block_size, 7.0, -8.0)

    # wo: (hidden_dim, q_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.wo, p.hidden_dim, p.q_dim(), block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.wo, p.hidden_dim, p.q_dim(), block_size, 7.0, -8.0)

    # w_gate: (ffn_dim, hidden_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.w_gate, p.ffn_dim, p.hidden_dim, block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.w_gate, p.ffn_dim, p.hidden_dim, block_size, 7.0, -8.0)

    # w_up: (ffn_dim, hidden_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.w_up, p.ffn_dim, p.hidden_dim, block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.w_up, p.ffn_dim, p.hidden_dim, block_size, 7.0, -8.0)

    # w_down: (hidden_dim, ffn_dim)
    q8_total += _quant_roundtrip_error(model.layer_weights, off.w_down, p.hidden_dim, p.ffn_dim, block_size, 127.0, -127.0)
    q4_total += _quant_roundtrip_error(model.layer_weights, off.w_down, p.hidden_dim, p.ffn_dim, block_size, 7.0, -8.0)

    var result = LayerSensitivity()
    result.q8_error = q8_total / 7.0
    result.q4_error = q4_total / 7.0
    return result^


fn analyze_sensitivity(
    model: Model,
    block_size: Int = 32,
) -> List[LayerSensitivity]:
    """Analyze quantization sensitivity for all layers.

    Args:
        model: FP32 model.
        block_size: Quantization block size.

    Returns:
        List of LayerSensitivity, one per layer.
    """
    var results = List[LayerSensitivity]()
    for layer in range(model.params.num_layers):
        results.append(measure_layer_sensitivity(model, layer, block_size))
    return results^


fn auto_calibrate(
    sensitivities: List[LayerSensitivity],
    q4_threshold: Float32 = 0.01,
) -> List[Int]:
    """Auto-select per-layer quantization modes based on sensitivity.

    Layers with Q4 error below the threshold get Q4 (mode 2),
    otherwise they get Q8 (mode 1). FP32 (mode 0) is not auto-selected
    but can be manually specified.

    Args:
        sensitivities: Per-layer sensitivity measurements.
        q4_threshold: Maximum acceptable Q4 MAE for using Q4.

    Returns:
        List of modes: 0=FP32, 1=Q8, 2=Q4.
    """
    var modes = List[Int]()
    for i in range(len(sensitivities)):
        if sensitivities[i].q4_error <= q4_threshold:
            modes.append(2)  # Q4
        else:
            modes.append(1)  # Q8
    return modes^


# ===----------------------------------------------------------------------=== #
# Mixed Quantization Model
# ===----------------------------------------------------------------------=== #

struct MixedQuantModel(Movable):
    """N-layer model with per-layer quantization mode.

    Each layer can independently use FP32, Q8, or Q4 for its projections.
    Norms, embeddings, and LM head are always FP32. The forward pass
    dispatches each layer's projections to the appropriate linear kernel
    based on the layer's assigned mode.

    Modes:
    - 0: FP32 (par_simd_matvec, no scales)
    - 1: Q8 (simd_q8_matvec with scales)
    - 2: Q4 (simd_q8_matvec with scales, same kernel as Q8)
    """
    var params: ModelParams
    var embed: Tensor[DType.float32]
    var final_norm: Tensor[DType.float32]
    var lm_head: Tensor[DType.float32]
    var layer_weights: Tensor[DType.float32]
    var layer_scales: Tensor[DType.float32]
    var layer_modes: List[Int]
    var layer_size: Int
    var block_size: Int
    var scales_per_layer: Int

    fn __init__(out self, params: ModelParams, layer_modes: List[Int],
                block_size: Int = 32):
        self.params = params.copy()
        self.layer_size = params.layer_weight_count()
        self.block_size = block_size

        self.layer_modes = List[Int]()
        for i in range(len(layer_modes)):
            self.layer_modes.append(layer_modes[i])

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
            var offsets = _compute_offsets(params, layer)
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
        self.layer_modes = other.layer_modes^
        self.layer_size = other.layer_size
        self.block_size = other.block_size
        self.scales_per_layer = other.scales_per_layer

    fn _layer_scale_offsets(self, layer: Int) -> LayerScaleOffsets:
        """Compute scale offsets for a layer's projections."""
        return _compute_scale_offsets(
            self.params, layer, self.scales_per_layer, self.block_size
        )

    fn _mixed_linear(
        self,
        x: Tensor[DType.float32],
        mode: Int,
        data_offset: Int,
        scales_offset: Int,
        out_dim: Int,
        in_dim: Int,
    ) -> Tensor[DType.float32]:
        """Dispatch linear to FP32 or quantized path based on mode."""
        var result = Tensor[DType.float32](Shape(out_dim))
        if mode == 0:
            par_simd_matvec(
                result, 0, self.layer_weights, data_offset,
                x, 0, out_dim, in_dim,
            )
        else:
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
        """Forward pass through a single layer with mixed-precision dispatch."""
        var p = self.params.copy()
        var hd = p.hidden_dim
        var off = _compute_offsets(p, layer)
        var soff = self._layer_scale_offsets(layer)
        var mode = self.layer_modes[layer]

        # === Attention sublayer ===
        var normed = Tensor[DType.float32](Shape(hd))
        simd_rmsnorm(normed, 0, x, 0, self.layer_weights, off.attn_norm, hd)

        var q = self._mixed_linear(normed, mode, off.wq, soff.wq, p.q_dim(), hd)
        var k = self._mixed_linear(normed, mode, off.wk, soff.wk, p.kv_dim(), hd)
        var v = self._mixed_linear(normed, mode, off.wv, soff.wv, p.kv_dim(), hd)

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

        # GQA attention
        var attn_out = gqa_attention_direct(
            q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim
        )

        # Output projection
        var attn_proj = self._mixed_linear(
            attn_out, mode, off.wo, soff.wo, hd, p.q_dim()
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

        var gate = self._mixed_linear(
            ffn_normed, mode, off.w_gate, soff.w_gate, p.ffn_dim, hd
        )
        var up = self._mixed_linear(
            ffn_normed, mode, off.w_up, soff.w_up, p.ffn_dim, hd
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

        var down = self._mixed_linear(
            ffn_out, mode, off.w_down, soff.w_down, hd, p.ffn_dim
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
        """Full forward pass: embed -> N mixed-precision layers -> norm -> logits."""
        var hidden = embed_token(self.embed, token_id, self.params.hidden_dim)

        for layer in range(self.params.num_layers):
            hidden = self.forward_layer(hidden, layer, cache, rope, pos)

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

    fn mode_summary(self) -> String:
        """Return a human-readable summary of per-layer modes."""
        var s = String("[")
        for i in range(len(self.layer_modes)):
            if i > 0:
                s += ", "
            if self.layer_modes[i] == 0:
                s += "FP32"
            elif self.layer_modes[i] == 1:
                s += "Q8"
            else:
                s += "Q4"
        s += "]"
        return s^


# ===----------------------------------------------------------------------=== #
# Quantization
# ===----------------------------------------------------------------------=== #

fn quantize_mixed(
    model: Model,
    layer_modes: List[Int],
    block_size: Int = 32,
) -> MixedQuantModel:
    """Convert FP32 Model to MixedQuantModel with per-layer modes.

    Args:
        model: Source FP32 model.
        layer_modes: Per-layer mode: 0=FP32, 1=Q8, 2=Q4.
        block_size: Quantization block size.

    Returns:
        MixedQuantModel with per-layer quantization.
    """
    var mm = MixedQuantModel(model.params, layer_modes, block_size)
    var p = model.params.copy()

    # Copy FP32 weights: embed, final_norm, lm_head
    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            mm.embed.set(v * p.hidden_dim + d, model.embed.get(v, d))
            mm.lm_head.set(v * p.hidden_dim + d, model.lm_head.get(v, d))
    for i in range(p.hidden_dim):
        mm.final_norm.set(i, model.final_norm.get(i))

    # Process each layer based on its mode
    for layer in range(p.num_layers):
        var off = _compute_offsets(p, layer)
        var soff = mm._layer_scale_offsets(layer)
        var mode = layer_modes[layer]

        # Always copy norm weights as FP32
        for i in range(p.hidden_dim):
            mm.layer_weights.set(
                off.attn_norm + i, model.layer_weights.get(off.attn_norm + i)
            )
            mm.layer_weights.set(
                off.ffn_norm + i, model.layer_weights.get(off.ffn_norm + i)
            )

        if mode == 0:
            # FP32: Copy attention projections (wq, wk, wv, wo — contiguous)
            var attn_start = off.wq
            var attn_end = off.wo + p.hidden_dim * p.q_dim()
            for i in range(attn_start, attn_end):
                mm.layer_weights.set(i, model.layer_weights.get(i))
            # Copy FFN projections (w_gate, w_up, w_down — contiguous)
            var ffn_start = off.w_gate
            var ffn_end = off.w_down + p.hidden_dim * p.ffn_dim
            for i in range(ffn_start, ffn_end):
                mm.layer_weights.set(i, model.layer_weights.get(i))

        elif mode == 1:
            # Q8: Quantize all 7 projections
            _quantize_projection(
                model.layer_weights, off.wq,
                mm.layer_weights, off.wq,
                mm.layer_scales, soff.wq,
                p.q_dim(), p.hidden_dim, block_size,
            )
            _quantize_projection(
                model.layer_weights, off.wk,
                mm.layer_weights, off.wk,
                mm.layer_scales, soff.wk,
                p.kv_dim(), p.hidden_dim, block_size,
            )
            _quantize_projection(
                model.layer_weights, off.wv,
                mm.layer_weights, off.wv,
                mm.layer_scales, soff.wv,
                p.kv_dim(), p.hidden_dim, block_size,
            )
            _quantize_projection(
                model.layer_weights, off.wo,
                mm.layer_weights, off.wo,
                mm.layer_scales, soff.wo,
                p.hidden_dim, p.q_dim(), block_size,
            )
            _quantize_projection(
                model.layer_weights, off.w_gate,
                mm.layer_weights, off.w_gate,
                mm.layer_scales, soff.w_gate,
                p.ffn_dim, p.hidden_dim, block_size,
            )
            _quantize_projection(
                model.layer_weights, off.w_up,
                mm.layer_weights, off.w_up,
                mm.layer_scales, soff.w_up,
                p.ffn_dim, p.hidden_dim, block_size,
            )
            _quantize_projection(
                model.layer_weights, off.w_down,
                mm.layer_weights, off.w_down,
                mm.layer_scales, soff.w_down,
                p.hidden_dim, p.ffn_dim, block_size,
            )

        elif mode == 2:
            # Q4: Quantize all 7 projections with Q4 range
            _quantize_projection_q4(
                model.layer_weights, off.wq,
                mm.layer_weights, off.wq,
                mm.layer_scales, soff.wq,
                p.q_dim(), p.hidden_dim, block_size,
            )
            _quantize_projection_q4(
                model.layer_weights, off.wk,
                mm.layer_weights, off.wk,
                mm.layer_scales, soff.wk,
                p.kv_dim(), p.hidden_dim, block_size,
            )
            _quantize_projection_q4(
                model.layer_weights, off.wv,
                mm.layer_weights, off.wv,
                mm.layer_scales, soff.wv,
                p.kv_dim(), p.hidden_dim, block_size,
            )
            _quantize_projection_q4(
                model.layer_weights, off.wo,
                mm.layer_weights, off.wo,
                mm.layer_scales, soff.wo,
                p.hidden_dim, p.q_dim(), block_size,
            )
            _quantize_projection_q4(
                model.layer_weights, off.w_gate,
                mm.layer_weights, off.w_gate,
                mm.layer_scales, soff.w_gate,
                p.ffn_dim, p.hidden_dim, block_size,
            )
            _quantize_projection_q4(
                model.layer_weights, off.w_up,
                mm.layer_weights, off.w_up,
                mm.layer_scales, soff.w_up,
                p.ffn_dim, p.hidden_dim, block_size,
            )
            _quantize_projection_q4(
                model.layer_weights, off.w_down,
                mm.layer_weights, off.w_down,
                mm.layer_scales, soff.w_down,
                p.hidden_dim, p.ffn_dim, block_size,
            )

    return mm^


# ===----------------------------------------------------------------------=== #
# One-Call Auto-Quantize
# ===----------------------------------------------------------------------=== #

fn auto_quantize(
    model: Model,
    q4_threshold: Float32 = 0.01,
    block_size: Int = 32,
) -> MixedQuantModel:
    """One-call sensitivity analysis + calibration + quantization.

    Chains analyze_sensitivity -> auto_calibrate -> quantize_mixed into
    a single call. Layers with Q4 error below the threshold get Q4,
    the rest get Q8.

    Args:
        model: FP32 model.
        q4_threshold: Maximum acceptable Q4 MAE for using Q4.
        block_size: Quantization block size.

    Returns:
        MixedQuantModel with auto-selected per-layer modes.
    """
    var sens = analyze_sensitivity(model, block_size)
    var modes = auto_calibrate(sens, q4_threshold)
    return quantize_mixed(model, modes, block_size)


# ===----------------------------------------------------------------------=== #
# Generation
# ===----------------------------------------------------------------------=== #

fn mixed_generate(
    model: MixedQuantModel,
    prompt_tokens: List[Int],
    max_new_tokens: Int,
    temperature: Float32 = 1.0,
) raises -> List[Int]:
    """Autoregressive generation with mixed-precision model.

    Args:
        model: Mixed quantization model.
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
