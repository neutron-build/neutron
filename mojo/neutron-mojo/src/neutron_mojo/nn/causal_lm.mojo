# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Causal Language Model
# ===----------------------------------------------------------------------=== #

"""Full causal language model with autoregressive generation.

Architecture:
    token_ids → Embedding → [TransformerBlock × N] → RMSNorm → LM Head → logits

Supports greedy, top-k, and temperature-scaled sampling.
"""

from math import exp
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import rmsnorm
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.kv_cache import KVCache
from neutron_mojo.nn.transformer import (
    TransformerWeights,
    transformer_block,
    linear,
)
from neutron_mojo.nn.attention import softmax_inplace


# ===----------------------------------------------------------------------=== #
# Causal LM Weights
# ===----------------------------------------------------------------------=== #

struct CausalLMWeights(Movable):
    """All weights for a causal language model.

    Includes embedding, per-layer transformer weights, final norm,
    and LM head (output projection to vocabulary).
    """
    var embed: Tensor[DType.float32]          # [vocab_size, hidden_dim]
    var final_norm: Tensor[DType.float32]     # [hidden_dim]
    var lm_head: Tensor[DType.float32]        # [vocab_size, hidden_dim]
    var layers: List[Int]                      # layer indices

    # Model config
    var num_layers: Int
    var vocab_size: Int
    var hidden_dim: Int
    var num_q_heads: Int
    var num_kv_heads: Int
    var head_dim: Int
    var ffn_dim: Int

    fn __init__(
        out self,
        num_layers: Int,
        vocab_size: Int,
        hidden_dim: Int,
        num_q_heads: Int,
        num_kv_heads: Int,
        head_dim: Int,
        ffn_dim: Int,
    ):
        """Create model weights (initialized to zeros/ones).

        Args:
            num_layers: Number of transformer layers.
            vocab_size: Vocabulary size.
            hidden_dim: Model hidden dimension.
            num_q_heads: Number of query heads.
            num_kv_heads: Number of KV heads.
            head_dim: Per-head dimension.
            ffn_dim: FFN intermediate dimension.
        """
        self.num_layers = num_layers
        self.vocab_size = vocab_size
        self.hidden_dim = hidden_dim
        self.num_q_heads = num_q_heads
        self.num_kv_heads = num_kv_heads
        self.head_dim = head_dim
        self.ffn_dim = ffn_dim

        self.embed = Tensor[DType.float32](Shape(vocab_size, hidden_dim))
        self.final_norm = Tensor[DType.float32](Shape(hidden_dim))
        self.lm_head = Tensor[DType.float32](Shape(vocab_size, hidden_dim))

        # Initialize final norm to 1.0
        for i in range(hidden_dim):
            self.final_norm.set(i, 1.0)

        # Layer indices for tracking
        self.layers = List[Int]()
        for i in range(num_layers):
            self.layers.append(i)

    fn __moveinit__(out self, deinit other: Self):
        self.embed = other.embed^
        self.final_norm = other.final_norm^
        self.lm_head = other.lm_head^
        self.layers = other.layers^
        self.num_layers = other.num_layers
        self.vocab_size = other.vocab_size
        self.hidden_dim = other.hidden_dim
        self.num_q_heads = other.num_q_heads
        self.num_kv_heads = other.num_kv_heads
        self.head_dim = other.head_dim
        self.ffn_dim = other.ffn_dim


# ===----------------------------------------------------------------------=== #
# Embedding Lookup
# ===----------------------------------------------------------------------=== #

fn embed_token(
    embed_table: Tensor[DType.float32],
    token_id: Int,
    hidden_dim: Int,
) -> Tensor[DType.float32]:
    """Look up a token's embedding vector.

    Args:
        embed_table: Embedding matrix [vocab_size, hidden_dim].
        token_id: Token index.
        hidden_dim: Embedding dimension.

    Returns:
        Embedding vector [hidden_dim].
    """
    var result = Tensor[DType.float32](Shape(hidden_dim))
    for i in range(hidden_dim):
        result.set(i, embed_table.get(token_id, i))
    return result^


# ===----------------------------------------------------------------------=== #
# Logits and Sampling
# ===----------------------------------------------------------------------=== #

fn compute_logits(
    hidden: Tensor[DType.float32],
    lm_head: Tensor[DType.float32],
    vocab_size: Int,
    hidden_dim: Int,
) -> Tensor[DType.float32]:
    """Compute logits from hidden state.

    Args:
        hidden: Hidden state [hidden_dim].
        lm_head: LM head weight [vocab_size, hidden_dim].
        vocab_size: Vocabulary size.
        hidden_dim: Hidden dimension.

    Returns:
        Logits [vocab_size].
    """
    var logits = Tensor[DType.float32](Shape(vocab_size))
    for v in range(vocab_size):
        var sum: Float32 = 0.0
        for d in range(hidden_dim):
            sum += lm_head.get(v, d) * hidden.get(d)
        logits.set(v, sum)
    return logits^


fn argmax(logits: Tensor[DType.float32], size: Int) -> Int:
    """Return index of maximum value.

    Args:
        logits: Values to search.
        size: Number of elements.

    Returns:
        Index of the maximum.
    """
    var best_idx = 0
    var best_val = logits.get(0)
    for i in range(1, size):
        var v = logits.get(i)
        if v > best_val:
            best_val = v
            best_idx = i
    return best_idx


fn apply_temperature(
    mut logits: Tensor[DType.float32], size: Int, temperature: Float32
):
    """Scale logits by temperature.

    Args:
        logits: Logits to scale in-place.
        size: Number of elements.
        temperature: Temperature (>1 = more random, <1 = more greedy).
    """
    if temperature <= 0.0 or temperature == 1.0:
        return
    for i in range(size):
        logits.set(i, logits.get(i) / temperature)


fn top_k_filter(
    mut logits: Tensor[DType.float32], size: Int, k: Int
):
    """Zero out all logits outside the top-k.

    Args:
        logits: Logits to filter in-place.
        size: Number of elements.
        k: Number of top values to keep.
    """
    if k <= 0 or k >= size:
        return

    # Find the k-th largest value (simple selection)
    # For production, use a partial sort. Here we do k passes.
    var used = Tensor[DType.float32](Shape(size))
    for i in range(size):
        used.set(i, 0.0)

    for _ in range(k):
        var best_idx = -1
        var best_val: Float32 = -1e30
        for i in range(size):
            if used.get(i) == 0.0 and logits.get(i) > best_val:
                best_val = logits.get(i)
                best_idx = i
        if best_idx >= 0:
            used.set(best_idx, 1.0)

    # Zero out below threshold
    for i in range(size):
        if used.get(i) == 0.0:
            logits.set(i, -1e30)


# ===----------------------------------------------------------------------=== #
# Forward Pass (single token)
# ===----------------------------------------------------------------------=== #

fn causal_lm_forward(
    token_id: Int,
    model_weights: CausalLMWeights,
    layer_weights: List[Int],                 # indices into external weight store
    transformer_weights_list: Tensor[DType.float32],
    mut caches: List[KVCache],                # one cache per layer
    rope_table: RoPETable,
    pos: Int,
) raises -> Tensor[DType.float32]:
    """Single-token forward path using currently available local weights.

    This legacy signature is retained for compatibility with older call sites.
    Until per-layer external weight dispatch is wired for this API, the function
    computes logits from token embedding + final RMSNorm + LM head projection.
    """
    var hidden = embed_token(model_weights.embed, token_id, model_weights.hidden_dim)
    var normed = rmsnorm[DType.float32](hidden, model_weights.final_norm)
    return compute_logits(
        normed, model_weights.lm_head, model_weights.vocab_size, model_weights.hidden_dim,
    )


# ===----------------------------------------------------------------------=== #
# Simple Generate Loop (1-layer for testing)
# ===----------------------------------------------------------------------=== #

fn generate_greedy_one_layer(
    prompt_tokens: List[Int],
    model_weights: CausalLMWeights,
    layer_weights: TransformerWeights,
    max_new_tokens: Int,
) raises -> List[Int]:
    """Greedy autoregressive generation with a single transformer layer.

    Args:
        prompt_tokens: Input token IDs.
        model_weights: Embedding, final norm, and LM head weights.
        layer_weights: Single transformer layer weights.
        max_new_tokens: Maximum tokens to generate.

    Returns:
        List of generated token IDs (not including prompt).
    """
    var hidden_dim = model_weights.hidden_dim
    var vocab_size = model_weights.vocab_size
    var num_q_heads = model_weights.num_q_heads
    var num_kv_heads = model_weights.num_kv_heads
    var head_dim = model_weights.head_dim

    var cache = KVCache(
        max_seq_len=len(prompt_tokens) + max_new_tokens,
        num_kv_heads=num_kv_heads,
        head_dim=head_dim,
    )
    var rope = RoPETable(
        head_dim=head_dim,
        max_seq_len=len(prompt_tokens) + max_new_tokens,
    )

    var generated = List[Int]()

    # Process prompt (prefill)
    var hidden = Tensor[DType.float32](Shape(hidden_dim))
    for i in range(len(prompt_tokens)):
        var token_id = prompt_tokens[i]
        hidden = embed_token(model_weights.embed, token_id, hidden_dim)
        hidden = transformer_block(
            hidden, layer_weights, cache, rope, pos=i,
            num_q_heads=num_q_heads,
            num_kv_heads=num_kv_heads,
            head_dim=head_dim,
        )

    # Generate new tokens
    var pos = len(prompt_tokens) - 1
    for _ in range(max_new_tokens):
        # Final norm + logits
        var normed = rmsnorm[DType.float32](hidden, model_weights.final_norm)
        var logits = compute_logits(normed, model_weights.lm_head, vocab_size, hidden_dim)

        # Greedy: pick argmax
        var next_token = argmax(logits, vocab_size)
        generated.append(next_token)

        # Prepare next step
        pos += 1
        hidden = embed_token(model_weights.embed, next_token, hidden_dim)
        hidden = transformer_block(
            hidden, layer_weights, cache, rope, pos=pos,
            num_q_heads=num_q_heads,
            num_kv_heads=num_kv_heads,
            head_dim=head_dim,
        )

    return generated^
