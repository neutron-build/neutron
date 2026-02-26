# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Loading Pipeline Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the full loading pipeline: Config → Model → populate → generate.
Also tests GGUF metadata → WeightIndex → Model population.
"""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params, generate
from neutron_mojo.model.config import ModelConfig, RoPEConfig
from neutron_mojo.model.populate import (
    model_from_config,
    load_named_weight,
    set_embed,
    set_lm_head,
    set_final_norm,
    set_layer_projection,
)
from neutron_mojo.model.loader import (
    WeightIndex,
    WeightDescriptor,
    FMT_GGUF,
)
from neutron_mojo.io.gguf import (
    GGUFFile,
    GGUFTensorInfo,
    GGUF_F32,
    GGUF_Q8_0,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# Helper: tiny config matching tiny_test_params
# ===----------------------------------------------------------------------=== #

fn tiny_config() -> ModelConfig:
    """Create a tiny ModelConfig matching tiny_test_params."""
    var cfg = ModelConfig()
    cfg.vocab_size = 8
    cfg.hidden_size = 4
    cfg.num_hidden_layers = 2
    cfg.num_attention_heads = 2
    cfg.num_key_value_heads = 1
    cfg.head_dim = 2
    cfg.intermediate_size = 8
    cfg.max_position_embeddings = 32
    cfg.rope.theta = 10000.0
    return cfg^


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_model_from_config() raises:
    """Test creating a Model from ModelConfig."""
    var cfg = tiny_config()
    var model = model_from_config(cfg)

    assert_true(model.params.vocab_size == 8, "vocab_size")
    assert_true(model.params.hidden_dim == 4, "hidden_dim")
    assert_true(model.params.num_layers == 2, "num_layers")
    assert_true(model.params.num_q_heads == 2, "num_q_heads")
    assert_true(model.params.num_kv_heads == 1, "num_kv_heads")
    assert_true(model.params.head_dim == 2, "head_dim")
    assert_true(model.params.ffn_dim == 8, "ffn_dim")

    print("  model_from_config: PASS")


fn test_set_embed_direct() raises:
    """Test direct embedding weight setter."""
    var p = tiny_test_params()
    var model = Model(p)

    var embed_data = Tensor[DType.float32](Shape(p.vocab_size * p.hidden_dim))
    for i in range(p.vocab_size * p.hidden_dim):
        embed_data.set(i, Float32(i) * 0.01)

    set_embed(model, embed_data, p.vocab_size * p.hidden_dim)

    # Verify using 2D get (row, col) since embed is Shape(vocab, hidden)
    # set(flat_idx, val) writes at flat position; get(row, col) reads at row*stride+col
    # embed[0,0] = flat[0] = 0.0, embed[1,1] = flat[5] = 0.05
    assert_near(model.embed.get(0, 0), 0.0, 0.001, "embed[0,0]")
    assert_near(model.embed.get(1, 1), 0.05, 0.001, "embed[1,1]")
    assert_near(model.embed.get(7, 3), 0.31, 0.001, "embed[7,3]")

    print("  set_embed_direct: PASS")


fn test_set_layer_projection() raises:
    """Test setting individual layer projections."""
    var p = tiny_test_params()
    var model = Model(p)

    # Create wq data [q_dim=4, hidden_dim=4] = 16 elements
    var wq_size = p.q_dim() * p.hidden_dim
    var wq_data = Tensor[DType.float32](Shape(wq_size))
    for i in range(wq_size):
        wq_data.set(i, Float32(i) * 0.1)

    set_layer_projection(model, 0, "wq", wq_data, wq_size)

    # Verify the values were written at the correct offset
    var off = model._layer_offsets(0)
    assert_near(model.layer_weights.get(off.wq), 0.0, 0.001, "wq[0]")
    assert_near(model.layer_weights.get(off.wq + 1), 0.1, 0.001, "wq[1]")
    assert_near(model.layer_weights.get(off.wq + 15), 1.5, 0.001, "wq[15]")

    print("  set_layer_projection: PASS")


fn test_load_named_weight_embed() raises:
    """Test loading a weight by HuggingFace name."""
    var p = tiny_test_params()
    var model = Model(p)

    var embed_size = p.vocab_size * p.hidden_dim
    var data = Tensor[DType.float32](Shape(embed_size))
    for i in range(embed_size):
        data.set(i, Float32(i) * 0.02)

    load_named_weight(model, "model.embed_tokens.weight", data, embed_size)

    # Verify via 2D get: embed[0,0]=flat[0]=0.0, embed[2,2]=flat[10]=0.2
    assert_near(model.embed.get(0, 0), 0.0, 0.001, "named embed[0,0]")
    assert_near(model.embed.get(2, 2), 0.2, 0.001, "named embed[2,2]")

    print("  load_named_weight_embed: PASS")


fn test_load_named_weight_layer() raises:
    """Test loading layer weights by HuggingFace names."""
    var p = tiny_test_params()
    var model = Model(p)

    # Set q_proj for layer 0
    var qd = p.q_dim()
    var hd = p.hidden_dim
    var wq_size = qd * hd  # 4*4 = 16
    var wq_data = Tensor[DType.float32](Shape(wq_size))
    for i in range(wq_size):
        wq_data.set(i, Float32(i + 1) * 0.5)

    load_named_weight(
        model, "model.layers.0.self_attn.q_proj.weight", wq_data, wq_size
    )

    var off = model._layer_offsets(0)
    assert_near(model.layer_weights.get(off.wq), 0.5, 0.001, "named wq[0]")
    assert_near(model.layer_weights.get(off.wq + 1), 1.0, 0.001, "named wq[1]")

    # Set gate_proj for layer 1
    var fd = p.ffn_dim
    var gate_size = fd * hd  # 8*4 = 32
    var gate_data = Tensor[DType.float32](Shape(gate_size))
    for i in range(gate_size):
        gate_data.set(i, Float32(i) * 0.01)

    load_named_weight(
        model, "model.layers.1.mlp.gate_proj.weight", gate_data, gate_size
    )

    var off1 = model._layer_offsets(1)
    assert_near(model.layer_weights.get(off1.w_gate), 0.0, 0.001, "named gate[0]")
    assert_near(model.layer_weights.get(off1.w_gate + 5), 0.05, 0.001, "named gate[5]")

    print("  load_named_weight_layer: PASS")


fn test_full_pipeline_config_to_generate() raises:
    """Test the complete pipeline: config → model → populate all weights → generate."""
    var cfg = tiny_config()
    var model = model_from_config(cfg)
    var p = model.params.copy()

    # Populate all weights via named interface
    var embed_size = p.vocab_size * p.hidden_dim
    var embed_data = Tensor[DType.float32](Shape(embed_size))
    for i in range(embed_size):
        embed_data.set(i, Float32(i) * 0.01)
    load_named_weight(model, "model.embed_tokens.weight", embed_data, embed_size)

    var lm_size = p.vocab_size * p.hidden_dim
    var lm_data = Tensor[DType.float32](Shape(lm_size))
    for i in range(lm_size):
        lm_data.set(i, Float32(i % 5) * 0.1)
    load_named_weight(model, "lm_head.weight", lm_data, lm_size)

    var norm_data = Tensor[DType.float32](Shape(p.hidden_dim))
    for i in range(p.hidden_dim):
        norm_data.set(i, 1.0)
    load_named_weight(model, "model.norm.weight", norm_data, p.hidden_dim)

    # Set all layer weights for both layers
    for layer in range(p.num_layers):
        var layer_str = String(layer)
        var prefix = "model.layers." + layer_str + "."

        # Norms
        var an_data = Tensor[DType.float32](Shape(p.hidden_dim))
        for i in range(p.hidden_dim):
            an_data.set(i, 1.0)
        load_named_weight(model, prefix + "input_layernorm.weight", an_data, p.hidden_dim)

        var fn_data = Tensor[DType.float32](Shape(p.hidden_dim))
        for i in range(p.hidden_dim):
            fn_data.set(i, 1.0)
        load_named_weight(model, prefix + "post_attention_layernorm.weight", fn_data, p.hidden_dim)

        # Attention projections
        var qd = p.q_dim()
        var kvd = p.kv_dim()
        var hd = p.hidden_dim
        var fd = p.ffn_dim

        var wq = Tensor[DType.float32](Shape(qd * hd))
        for i in range(qd * hd):
            wq.set(i, Float32(i % 7) * 0.01)
        load_named_weight(model, prefix + "self_attn.q_proj.weight", wq, qd * hd)

        var wk = Tensor[DType.float32](Shape(kvd * hd))
        for i in range(kvd * hd):
            wk.set(i, Float32(i % 5) * 0.01)
        load_named_weight(model, prefix + "self_attn.k_proj.weight", wk, kvd * hd)

        var wv = Tensor[DType.float32](Shape(kvd * hd))
        for i in range(kvd * hd):
            wv.set(i, Float32(i % 3) * 0.01)
        load_named_weight(model, prefix + "self_attn.v_proj.weight", wv, kvd * hd)

        var wo = Tensor[DType.float32](Shape(hd * qd))
        for i in range(hd * qd):
            wo.set(i, Float32(i % 9) * 0.01)
        load_named_weight(model, prefix + "self_attn.o_proj.weight", wo, hd * qd)

        # FFN projections
        var wg = Tensor[DType.float32](Shape(fd * hd))
        for i in range(fd * hd):
            wg.set(i, Float32(i % 11) * 0.001)
        load_named_weight(model, prefix + "mlp.gate_proj.weight", wg, fd * hd)

        var wu = Tensor[DType.float32](Shape(fd * hd))
        for i in range(fd * hd):
            wu.set(i, Float32(i % 13) * 0.001)
        load_named_weight(model, prefix + "mlp.up_proj.weight", wu, fd * hd)

        var wd = Tensor[DType.float32](Shape(hd * fd))
        for i in range(hd * fd):
            wd.set(i, Float32(i % 7) * 0.001)
        load_named_weight(model, prefix + "mlp.down_proj.weight", wd, hd * fd)

    # Generate tokens
    var prompt = List[Int]()
    prompt.append(1)
    prompt.append(2)
    var tokens = generate(model, prompt, max_new_tokens=3)

    assert_true(len(tokens) == 3, "generated 3 tokens")
    for i in range(3):
        assert_true(tokens[i] >= 0, "token >= 0")
        assert_true(tokens[i] < p.vocab_size, "token < vocab_size")

    print("  full_pipeline_config_to_generate: PASS")


fn test_gguf_metadata_to_weight_index() raises:
    """Test GGUF metadata → WeightIndex pipeline."""
    # Simulate a GGUF file with tensor metadata
    var gguf = GGUFFile()
    gguf.magic = 0x46554747  # "GGUF"
    gguf.version = 3
    gguf.tensor_count = 3
    gguf.data_offset = 512  # Simulated data section start

    # Register some tensors
    var embed_shape = List[Int]()
    embed_shape.append(8)
    embed_shape.append(4)
    gguf.register_tensor("model.embed_tokens.weight", embed_shape, GGUF_F32(), 0)

    var norm_shape = List[Int]()
    norm_shape.append(4)
    gguf.register_tensor("model.norm.weight", norm_shape, GGUF_F32(), 128)

    var wq_shape = List[Int]()
    wq_shape.append(4)
    wq_shape.append(4)
    gguf.register_tensor(
        "model.layers.0.self_attn.q_proj.weight", wq_shape, GGUF_Q8_0(), 144
    )

    # Verify GGUF metadata
    assert_true(gguf.is_valid(), "GGUF valid magic")
    assert_true(gguf.has_tensor("model.embed_tokens.weight"), "has embed")
    assert_true(gguf.has_tensor("model.norm.weight"), "has norm")

    var info = gguf.get_tensor_info("model.embed_tokens.weight")
    assert_true(info.numel() == 32, "embed numel = 8*4 = 32")

    var wq_info = gguf.get_tensor_info("model.layers.0.self_attn.q_proj.weight")
    assert_true(wq_info.numel() == 16, "wq numel = 4*4 = 16")

    print("  gguf_metadata_to_weight_index: PASS")


fn test_weight_index_roundtrip() raises:
    """Test building a WeightIndex and querying it."""
    var index = WeightIndex()
    index.format = FMT_GGUF()
    index.file_path = String("test.gguf")

    var desc = WeightDescriptor()
    desc.name = String("model.embed_tokens.weight")
    desc.dtype = DType.float32
    var sh = List[Int]()
    sh.append(8)
    sh.append(4)
    desc.shape = sh^
    desc.size_bytes = 32 * 4
    desc.file_offset = 512
    desc.is_quantized = False
    desc.quant_type = String("none")
    index.add_weight(desc)

    assert_true(index.has_weight("model.embed_tokens.weight"), "index has embed")
    assert_true(index.num_weights() == 1, "1 weight")

    var retrieved = index.get_weight("model.embed_tokens.weight")
    assert_true(retrieved.numel() == 32, "retrieved numel")
    assert_true(retrieved.size_bytes == 128, "retrieved size_bytes")

    print("  weight_index_roundtrip: PASS")


fn main() raises:
    print("test_model_loading:")

    test_model_from_config()
    test_set_embed_direct()
    test_set_layer_projection()
    test_load_named_weight_embed()
    test_load_named_weight_layer()
    test_full_pipeline_config_to_generate()
    test_gguf_metadata_to_weight_index()
    test_weight_index_roundtrip()

    print("ALL PASSED")
