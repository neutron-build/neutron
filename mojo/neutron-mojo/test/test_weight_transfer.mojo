# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Weight Transfer Tests
# ===----------------------------------------------------------------------=== #

"""Tests for bidirectional weight transfer between Model and Tape."""

from neutron_mojo.autograd.tape import Tape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.train.trainable import TrainableLM
from neutron_mojo.train.weight_transfer import (
    WeightMapping, build_weight_mapping, model_to_tape, tape_to_model,
)


fn assert_close(a: Float32, b: Float32, atol: Float64 = 1e-5) raises:
    var diff = abs(Float64(a) - Float64(b))
    if diff > atol:
        raise Error("Values not close: " + String(a) + " vs " + String(b) + " diff=" + String(diff))


fn assert_eq(a: Int, b: Int) raises:
    if a != b:
        raise Error("Not equal: " + String(a) + " vs " + String(b))


fn _make_tiny_params() -> ModelParams:
    """Create tiny params matching TrainableLM layout.

    TrainableLM uses single-head attention with q_dim=k_dim=v_dim=hidden_dim,
    so we set num_q_heads=1, num_kv_heads=1, head_dim=hidden_dim.
    """
    var p = ModelParams()
    p.num_layers = 2
    p.vocab_size = 8
    p.hidden_dim = 4
    p.num_q_heads = 1
    p.num_kv_heads = 1
    p.head_dim = 4
    p.ffn_dim = 8
    p.max_seq_len = 32
    return p^


fn test_weight_mapping_struct() raises:
    """WeightMapping stores fields correctly."""
    var wm = WeightMapping(100, 5, 32, 0)
    assert_eq(wm.model_offset, 100)
    assert_eq(wm.tape_var_idx, 5)
    assert_eq(wm.numel, 32)
    assert_eq(wm.source, 0)
    print("  weight_mapping_struct: PASS")


fn test_weight_mapping_copy() raises:
    """WeightMapping is Copyable."""
    var wm1 = WeightMapping(10, 2, 16, 1)
    var wm2 = wm1
    assert_eq(wm2.model_offset, 10)
    assert_eq(wm2.tape_var_idx, 2)
    assert_eq(wm2.numel, 16)
    assert_eq(wm2.source, 1)
    print("  weight_mapping_copy: PASS")


fn test_build_mapping_count() raises:
    """Build mapping produces correct number of entries."""
    var p = _make_tiny_params()
    var model = Model(p)
    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    var mappings = build_weight_mapping(model, trainable)
    # embed(1) + 2 layers * 9 weights + final_norm(1) + lm_head(1) = 21
    assert_eq(len(mappings), 21)
    print("  build_mapping_count: PASS")


fn test_build_mapping_sources() raises:
    """Build mapping has correct source types."""
    var p = _make_tiny_params()
    var model = Model(p)
    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    var mappings = build_weight_mapping(model, trainable)
    # First entry: embed (source=1)
    assert_eq(mappings[0].source, 1)
    # Last entry: lm_head (source=3)
    assert_eq(mappings[len(mappings) - 1].source, 3)
    # Second-to-last: final_norm (source=2)
    assert_eq(mappings[len(mappings) - 2].source, 2)
    print("  build_mapping_sources: PASS")


fn test_model_to_tape_embed() raises:
    """model_to_tape correctly copies embedding weights."""
    var p = _make_tiny_params()
    var model = Model(p)
    # Set known embed values
    for i in range(p.vocab_size * p.hidden_dim):
        model.embed.set(i, Float32(0.01 * (i + 1)))

    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)

    # Verify embed values in tape
    var embed_idx = trainable.embedding.embed_idx
    for i in range(p.vocab_size * p.hidden_dim):
        assert_close(tape.get_data(embed_idx, i), Float32(0.01 * (i + 1)))
    print("  model_to_tape_embed: PASS")


fn test_model_to_tape_final_norm() raises:
    """model_to_tape correctly copies final_norm weights."""
    var p = _make_tiny_params()
    var model = Model(p)
    for i in range(p.hidden_dim):
        model.final_norm.set(i, Float32(0.5 + 0.1 * i))

    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)

    var norm_idx = trainable.final_norm.gamma_idx
    for i in range(p.hidden_dim):
        assert_close(tape.get_data(norm_idx, i), Float32(0.5 + 0.1 * i))
    print("  model_to_tape_final_norm: PASS")


fn test_model_to_tape_lm_head() raises:
    """model_to_tape correctly copies lm_head weights."""
    var p = _make_tiny_params()
    var model = Model(p)
    for i in range(p.vocab_size * p.hidden_dim):
        model.lm_head.set(i, Float32(0.02 * (i + 1)))

    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)

    var head_idx = trainable.lm_head.weight_idx
    for i in range(p.vocab_size * p.hidden_dim):
        assert_close(tape.get_data(head_idx, i), Float32(0.02 * (i + 1)))
    print("  model_to_tape_lm_head: PASS")


fn test_model_to_tape_layer_weights() raises:
    """model_to_tape correctly copies layer weight projections."""
    var p = _make_tiny_params()
    var model = Model(p)
    # Set all layer weights to known pattern
    var total = p.num_layers * p.layer_weight_count()
    for i in range(total):
        model.layer_weights.set(i, Float32(0.001 * (i + 1)))

    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)

    # Check layer 0 attn_norm
    var off = model._layer_offsets(0)
    var norm_idx = trainable.blocks[0].attn_norm.gamma_idx
    for i in range(p.hidden_dim):
        assert_close(tape.get_data(norm_idx, i), model.layer_weights.get(off.attn_norm + i))
    print("  model_to_tape_layer_weights: PASS")


fn test_tape_to_model_creates_model() raises:
    """tape_to_model creates a valid Model."""
    var p = _make_tiny_params()
    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    var model = tape_to_model(tape, trainable, p)
    assert_eq(model.params.num_layers, 2)
    assert_eq(model.params.vocab_size, 8)
    assert_eq(model.params.hidden_dim, 4)
    print("  tape_to_model_creates_model: PASS")


fn test_roundtrip_embed() raises:
    """Roundtrip: Model -> Tape -> Model preserves embed weights."""
    var p = _make_tiny_params()
    var model = Model(p)
    for i in range(p.vocab_size * p.hidden_dim):
        model.embed.set(i, Float32(0.03 * (i + 1)))

    var tape = Tape(262144)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)
    var model2 = tape_to_model(tape, trainable, p)

    for i in range(p.vocab_size * p.hidden_dim):
        assert_close(model2.embed.get(i // p.hidden_dim, i % p.hidden_dim), Float32(0.03 * (i + 1)))
    print("  roundtrip_embed: PASS")


fn test_roundtrip_full() raises:
    """Roundtrip: all weights survive Model -> Tape -> Model."""
    var p = _make_tiny_params()
    var model = Model(p)

    # Fill all weights with distinct values
    for i in range(p.vocab_size * p.hidden_dim):
        model.embed.set(i, Float32(0.01 * (i + 1)))
        model.lm_head.set(i, Float32(0.02 * (i + 1)))
    for i in range(p.hidden_dim):
        model.final_norm.set(i, Float32(1.0 + 0.1 * i))
    var total_lw = p.num_layers * p.layer_weight_count()
    for i in range(total_lw):
        model.layer_weights.set(i, Float32(0.001 * (i + 1)))

    var tape = Tape(524288)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)
    var model2 = tape_to_model(tape, trainable, p)

    # Verify embed
    for i in range(p.vocab_size * p.hidden_dim):
        assert_close(model2.embed.get(i // p.hidden_dim, i % p.hidden_dim), model.embed.get(i // p.hidden_dim, i % p.hidden_dim))
    # Verify final_norm
    for i in range(p.hidden_dim):
        assert_close(model2.final_norm.get(i), model.final_norm.get(i))
    # Verify lm_head
    for i in range(p.vocab_size * p.hidden_dim):
        assert_close(model2.lm_head.get(i // p.hidden_dim, i % p.hidden_dim), model.lm_head.get(i // p.hidden_dim, i % p.hidden_dim))
    # Verify layer_weights
    for i in range(total_lw):
        assert_close(model2.layer_weights.get(i), model.layer_weights.get(i))
    print("  roundtrip_full: PASS")


fn test_roundtrip_layer1() raises:
    """Roundtrip preserves layer 1 (second layer) weights."""
    var p = _make_tiny_params()
    var model = Model(p)

    var total_lw = p.num_layers * p.layer_weight_count()
    for i in range(total_lw):
        model.layer_weights.set(i, Float32(0.005 * (i + 1)))

    var tape = Tape(524288)
    var trainable = TrainableLM(p.vocab_size, p.hidden_dim, p.num_layers, p.ffn_dim)
    trainable.register(tape)

    model_to_tape(model, tape, trainable)
    var model2 = tape_to_model(tape, trainable, p)

    # Check layer 1 specifically
    var off = model._layer_offsets(1)
    for i in range(p.hidden_dim):
        assert_close(
            model2.layer_weights.get(off.attn_norm + i),
            model.layer_weights.get(off.attn_norm + i),
        )
    # Check q_proj of layer 1
    var qd = p.q_dim()
    for i in range(qd * p.hidden_dim):
        assert_close(
            model2.layer_weights.get(off.wq + i),
            model.layer_weights.get(off.wq + i),
        )
    print("  roundtrip_layer1: PASS")


fn main() raises:
    print("test_weight_transfer:")
    test_weight_mapping_struct()
    test_weight_mapping_copy()
    test_build_mapping_count()
    test_build_mapping_sources()
    test_model_to_tape_embed()
    test_model_to_tape_final_norm()
    test_model_to_tape_lm_head()
    test_model_to_tape_layer_weights()
    test_tape_to_model_creates_model()
    test_roundtrip_embed()
    test_roundtrip_full()
    test_roundtrip_layer1()
    print("ALL PASSED (12 tests)")
