# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Paged KV Cache Forward Pass
# ===----------------------------------------------------------------------=== #

"""Model forward pass using PagedKVCache for memory-efficient inference.

Provides free functions that read weights from a Model (or QuantizedModel/Q4Model)
and write K/V into a PagedKVCache. This enables on-demand page allocation
instead of pre-allocated contiguous memory, allowing 2-4x more concurrent
requests at the same peak memory.

Usage:
    var cache = PagedKVCache(max_pages=256, page_size=16,
                              num_layers=model.params.num_layers,
                              num_kv_heads=model.params.num_kv_heads,
                              head_dim=model.params.head_dim)
    var logits = paged_forward(model, token_id, cache, rope, pos)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import (
    simd_matvec,
    simd_rmsnorm,
    simd_swiglu,
    par_simd_matvec,
    simd_q8_matvec,
)
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.q_model import QuantizedModel
from neutron_mojo.nn.q4_model import Q4Model
from neutron_mojo.nn.paged_kv_cache import PagedKVCache
from neutron_mojo.nn.attention import paged_gqa_attention
from neutron_mojo.nn.causal_lm import embed_token, argmax, apply_temperature


# ===----------------------------------------------------------------------=== #
# FP32 Model + Paged KV Cache
# ===----------------------------------------------------------------------=== #

fn paged_forward_layer(
    model: Model,
    x: Tensor[DType.float32],
    layer: Int,
    mut cache: PagedKVCache,
    rope: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Single-layer forward pass with paged KV cache.

    Same computation as Model.forward_layer but uses PagedKVCache
    for K/V storage instead of MultiLayerKVCache.

    Args:
        model: Model with weights (read-only).
        x: Input hidden state [hidden_dim].
        layer: Layer index.
        cache: Paged KV cache (mutated to append K/V).
        rope: RoPE table.
        pos: Current position.

    Returns:
        Output hidden state [hidden_dim].
    """
    var p = model.params.copy()
    var hd = p.hidden_dim
    var off = model._layer_offsets(layer)

    # === Attention sublayer ===
    var normed = Tensor[DType.float32](Shape(hd))
    simd_rmsnorm(normed, 0, x, 0, model.layer_weights, off.attn_norm, hd)

    # Q/K/V projections
    var q = model._linear_from_flat(normed, off.wq, p.q_dim(), hd)
    var k = model._linear_from_flat(normed, off.wk, p.kv_dim(), hd)
    var v = model._linear_from_flat(normed, off.wv, p.kv_dim(), hd)

    # Apply RoPE
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

    # Append to paged KV cache (allocates new pages on demand)
    cache.append_kv(layer, k, v, num_new_tokens=1)

    # Paged GQA attention
    var attn_out = paged_gqa_attention(
        q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim,
    )

    # Output projection
    var attn_proj = model._linear_from_flat(attn_out, off.wo, hd, p.q_dim())

    # Residual
    var residual1 = Tensor[DType.float32](Shape(hd))
    for i in range(hd):
        residual1.set(i, x.get(i) + attn_proj.get(i))

    # === FFN sublayer ===
    var ffn_normed = Tensor[DType.float32](Shape(hd))
    simd_rmsnorm(ffn_normed, 0, residual1, 0, model.layer_weights, off.ffn_norm, hd)

    var gate = model._linear_from_flat(ffn_normed, off.w_gate, p.ffn_dim, hd)
    var up = model._linear_from_flat(ffn_normed, off.w_up, p.ffn_dim, hd)

    var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
    simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

    var down = model._linear_from_flat(ffn_out, off.w_down, hd, p.ffn_dim)

    # Residual
    var output = Tensor[DType.float32](Shape(hd))
    for i in range(hd):
        output.set(i, residual1.get(i) + down.get(i))

    return output^


fn paged_forward(
    model: Model,
    token_id: Int,
    mut cache: PagedKVCache,
    rope: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Full forward pass with paged KV cache: embed -> N layers -> norm -> logits.

    Args:
        model: Model with weights.
        token_id: Input token ID.
        cache: Paged KV cache.
        rope: RoPE table.
        pos: Current position.

    Returns:
        Logits [vocab_size].
    """
    var hidden = embed_token(model.embed, token_id, model.params.hidden_dim)

    for layer in range(model.params.num_layers):
        hidden = paged_forward_layer(model, hidden, layer, cache, rope, pos)

    var normed = Tensor[DType.float32](Shape(model.params.hidden_dim))
    simd_rmsnorm(
        normed, 0, hidden, 0, model.final_norm, 0, model.params.hidden_dim
    )
    var logits = Tensor[DType.float32](Shape(model.params.vocab_size))
    par_simd_matvec(
        logits, 0, model.lm_head, 0, normed, 0,
        model.params.vocab_size, model.params.hidden_dim,
    )
    return logits^


fn paged_generate(
    model: Model,
    prompt_tokens: List[Int],
    max_new_tokens: Int,
    max_pages: Int,
    page_size: Int = 16,
    temperature: Float32 = 1.0,
) raises -> List[Int]:
    """Autoregressive generation with paged KV cache.

    Args:
        model: FP32 language model.
        prompt_tokens: Input token IDs.
        max_new_tokens: Max tokens to generate.
        max_pages: Total pages in the pool.
        page_size: Tokens per page.
        temperature: Sampling temperature.

    Returns:
        Generated token IDs (not including prompt).
    """
    var p = model.params.copy()
    var total_len = len(prompt_tokens) + max_new_tokens

    var cache = PagedKVCache(
        max_pages=max_pages,
        page_size=page_size,
        num_layers=p.num_layers,
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
        logits = paged_forward(model, prompt_tokens[i], cache, rope, pos=i)

    # Decode
    for step in range(max_new_tokens):
        if temperature != 1.0 and temperature > 0.0:
            apply_temperature(logits, p.vocab_size, temperature)

        var next_token = argmax(logits, p.vocab_size)
        generated.append(next_token)

        var pos = len(prompt_tokens) + step
        logits = paged_forward(model, next_token, cache, rope, pos=pos)

    return generated^


# ===----------------------------------------------------------------------=== #
# Q8 QuantizedModel + Paged KV Cache
# ===----------------------------------------------------------------------=== #

fn paged_q8_forward_layer(
    model: QuantizedModel,
    x: Tensor[DType.float32],
    layer: Int,
    mut cache: PagedKVCache,
    rope: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Single Q8 layer forward with paged KV cache."""
    var p = model.params.copy()
    var hd = p.hidden_dim
    var off = model._layer_offsets(layer)
    var soff = model._layer_scale_offsets(layer)

    var normed = Tensor[DType.float32](Shape(hd))
    simd_rmsnorm(normed, 0, x, 0, model.layer_weights, off.attn_norm, hd)

    var q = model._q8_linear_from_flat(normed, off.wq, soff.wq, p.q_dim(), hd)
    var k = model._q8_linear_from_flat(normed, off.wk, soff.wk, p.kv_dim(), hd)
    var v = model._q8_linear_from_flat(normed, off.wv, soff.wv, p.kv_dim(), hd)

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

    var attn_out = paged_gqa_attention(
        q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim,
    )

    var attn_proj = model._q8_linear_from_flat(attn_out, off.wo, soff.wo, hd, p.q_dim())

    var residual1 = Tensor[DType.float32](Shape(hd))
    for i in range(hd):
        residual1.set(i, x.get(i) + attn_proj.get(i))

    var ffn_normed = Tensor[DType.float32](Shape(hd))
    simd_rmsnorm(ffn_normed, 0, residual1, 0, model.layer_weights, off.ffn_norm, hd)

    var gate = model._q8_linear_from_flat(ffn_normed, off.w_gate, soff.w_gate, p.ffn_dim, hd)
    var up = model._q8_linear_from_flat(ffn_normed, off.w_up, soff.w_up, p.ffn_dim, hd)

    var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
    simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

    var down = model._q8_linear_from_flat(ffn_out, off.w_down, soff.w_down, hd, p.ffn_dim)

    var output = Tensor[DType.float32](Shape(hd))
    for i in range(hd):
        output.set(i, residual1.get(i) + down.get(i))

    return output^


fn paged_q8_forward(
    model: QuantizedModel,
    token_id: Int,
    mut cache: PagedKVCache,
    rope: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Full Q8 forward pass with paged KV cache."""
    var hidden = embed_token(model.embed, token_id, model.params.hidden_dim)

    for layer in range(model.params.num_layers):
        hidden = paged_q8_forward_layer(model, hidden, layer, cache, rope, pos)

    var normed = Tensor[DType.float32](Shape(model.params.hidden_dim))
    simd_rmsnorm(
        normed, 0, hidden, 0, model.final_norm, 0, model.params.hidden_dim
    )
    var logits = Tensor[DType.float32](Shape(model.params.vocab_size))
    par_simd_matvec(
        logits, 0, model.lm_head, 0, normed, 0,
        model.params.vocab_size, model.params.hidden_dim,
    )
    return logits^


fn paged_q8_generate(
    model: QuantizedModel,
    prompt_tokens: List[Int],
    max_new_tokens: Int,
    max_pages: Int,
    page_size: Int = 16,
    temperature: Float32 = 1.0,
) raises -> List[Int]:
    """Autoregressive generation with Q8 model + paged KV cache."""
    var p = model.params.copy()
    var total_len = len(prompt_tokens) + max_new_tokens

    var cache = PagedKVCache(
        max_pages=max_pages, page_size=page_size,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(
        head_dim=p.head_dim, max_seq_len=total_len, theta=p.rope_theta,
    )

    var generated = List[Int]()
    var logits = Tensor[DType.float32](Shape(p.vocab_size))

    for i in range(len(prompt_tokens)):
        logits = paged_q8_forward(model, prompt_tokens[i], cache, rope, pos=i)

    for step in range(max_new_tokens):
        if temperature != 1.0 and temperature > 0.0:
            apply_temperature(logits, p.vocab_size, temperature)
        var next_token = argmax(logits, p.vocab_size)
        generated.append(next_token)
        var pos = len(prompt_tokens) + step
        logits = paged_q8_forward(model, next_token, cache, rope, pos=pos)

    return generated^


# ===----------------------------------------------------------------------=== #
# Q4 Model + Paged KV Cache
# ===----------------------------------------------------------------------=== #

fn paged_q4_forward_layer(
    model: Q4Model,
    x: Tensor[DType.float32],
    layer: Int,
    mut cache: PagedKVCache,
    rope: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Single Q4 layer forward with paged KV cache."""
    var p = model.params.copy()
    var hd = p.hidden_dim
    var off = model._layer_offsets(layer)
    var soff = model._layer_scale_offsets(layer)

    var normed = Tensor[DType.float32](Shape(hd))
    simd_rmsnorm(normed, 0, x, 0, model.layer_weights, off.attn_norm, hd)

    var q = model._q4_linear_from_flat(normed, off.wq, soff.wq, p.q_dim(), hd)
    var k = model._q4_linear_from_flat(normed, off.wk, soff.wk, p.kv_dim(), hd)
    var v = model._q4_linear_from_flat(normed, off.wv, soff.wv, p.kv_dim(), hd)

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

    var attn_out = paged_gqa_attention(
        q, cache, layer, p.num_q_heads, p.num_kv_heads, p.head_dim,
    )

    var attn_proj = model._q4_linear_from_flat(attn_out, off.wo, soff.wo, hd, p.q_dim())

    var residual1 = Tensor[DType.float32](Shape(hd))
    for i in range(hd):
        residual1.set(i, x.get(i) + attn_proj.get(i))

    var ffn_normed = Tensor[DType.float32](Shape(hd))
    simd_rmsnorm(ffn_normed, 0, residual1, 0, model.layer_weights, off.ffn_norm, hd)

    var gate = model._q4_linear_from_flat(ffn_normed, off.w_gate, soff.w_gate, p.ffn_dim, hd)
    var up = model._q4_linear_from_flat(ffn_normed, off.w_up, soff.w_up, p.ffn_dim, hd)

    var ffn_out = Tensor[DType.float32](Shape(p.ffn_dim))
    simd_swiglu(ffn_out, 0, gate, 0, up, 0, p.ffn_dim)

    var down = model._q4_linear_from_flat(ffn_out, off.w_down, soff.w_down, hd, p.ffn_dim)

    var output = Tensor[DType.float32](Shape(hd))
    for i in range(hd):
        output.set(i, residual1.get(i) + down.get(i))

    return output^


fn paged_q4_forward(
    model: Q4Model,
    token_id: Int,
    mut cache: PagedKVCache,
    rope: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Full Q4 forward pass with paged KV cache."""
    var hidden = embed_token(model.embed, token_id, model.params.hidden_dim)

    for layer in range(model.params.num_layers):
        hidden = paged_q4_forward_layer(model, hidden, layer, cache, rope, pos)

    var normed = Tensor[DType.float32](Shape(model.params.hidden_dim))
    simd_rmsnorm(
        normed, 0, hidden, 0, model.final_norm, 0, model.params.hidden_dim
    )
    var logits = Tensor[DType.float32](Shape(model.params.vocab_size))
    par_simd_matvec(
        logits, 0, model.lm_head, 0, normed, 0,
        model.params.vocab_size, model.params.hidden_dim,
    )
    return logits^


fn paged_q4_generate(
    model: Q4Model,
    prompt_tokens: List[Int],
    max_new_tokens: Int,
    max_pages: Int,
    page_size: Int = 16,
    temperature: Float32 = 1.0,
) raises -> List[Int]:
    """Autoregressive generation with Q4 model + paged KV cache."""
    var p = model.params.copy()
    var total_len = len(prompt_tokens) + max_new_tokens

    var cache = PagedKVCache(
        max_pages=max_pages, page_size=page_size,
        num_layers=p.num_layers,
        num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
    )
    var rope = RoPETable(
        head_dim=p.head_dim, max_seq_len=total_len, theta=p.rope_theta,
    )

    var generated = List[Int]()
    var logits = Tensor[DType.float32](Shape(p.vocab_size))

    for i in range(len(prompt_tokens)):
        logits = paged_q4_forward(model, prompt_tokens[i], cache, rope, pos=i)

    for step in range(max_new_tokens):
        if temperature != 1.0 and temperature > 0.0:
            apply_temperature(logits, p.vocab_size, temperature)
        var next_token = argmax(logits, p.vocab_size)
        generated.append(next_token)
        var pos = len(prompt_tokens) + step
        logits = paged_q4_forward(model, next_token, cache, rope, pos=pos)

    return generated^
