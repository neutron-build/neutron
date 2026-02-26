# ===----------------------------------------------------------------------=== #
# Neutron Mojo — SafeTensors Sharded Loading Tests (Sprint 14)
# ===----------------------------------------------------------------------=== #

"""Tests for single-file and sharded SafeTensors model loading."""

from neutron_mojo.io.safetensors import (
    TensorInfo,
    SafeTensorsFile,
    SafeTensorsIndex,
    load_safetensors_index_from_string,
    build_safetensors_from_parts,
    dtype_element_size,
)
from neutron_mojo.io.json import (
    parse_weight_map,
    parse_config_json,
)
from neutron_mojo.io.binary_reader import BinaryReader
from neutron_mojo.model.config import ModelConfig
from neutron_mojo.model.populate import model_from_config
from neutron_mojo.model.weight_reader import load_safetensors_from_buffer
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from memory import UnsafePointer, alloc


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    var diff = a - b
    if diff < 0:
        diff = -diff
    if diff > tol:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# Test Helpers
# ===----------------------------------------------------------------------=== #

fn _f32_to_bytes(val: Float32) -> List[UInt8]:
    """Convert a Float32 to 4 little-endian bytes."""
    var p = alloc[Float32](1)
    p.store(val)
    var bp = p.bitcast[UInt8]()
    var result = List[UInt8]()
    result.append(bp.load())
    result.append((bp + 1).load())
    result.append((bp + 2).load())
    result.append((bp + 3).load())
    p.free()
    return result^


fn _tiny_config() -> ModelConfig:
    """Create a tiny model config for testing: vocab=4, hidden=2, 1 layer."""
    var cfg = ModelConfig()
    cfg.vocab_size = 4
    cfg.hidden_size = 2
    cfg.intermediate_size = 4
    cfg.num_hidden_layers = 1
    cfg.num_attention_heads = 1
    cfg.num_key_value_heads = 1
    cfg.head_dim = 2
    cfg.max_position_embeddings = 16
    return cfg^


fn _build_tiny_safetensors() -> List[UInt8]:
    """Build a minimal SafeTensors file with embed + norm + lm_head + 1 layer.

    Tensor layout:
      model.embed_tokens.weight: [4, 2] F32 = 8 floats = 32 bytes
      model.norm.weight: [2] F32 = 2 floats = 8 bytes
      lm_head.weight: [4, 2] F32 = 8 floats = 32 bytes
      model.layers.0.input_layernorm.weight: [2] F32 = 8 bytes
      model.layers.0.self_attn.q_proj.weight: [2, 2] F32 = 4 floats = 16 bytes
      model.layers.0.self_attn.k_proj.weight: [2, 2] F32 = 16 bytes
      model.layers.0.self_attn.v_proj.weight: [2, 2] F32 = 16 bytes
      model.layers.0.self_attn.o_proj.weight: [2, 2] F32 = 16 bytes
      model.layers.0.post_attention_layernorm.weight: [2] F32 = 8 bytes
      model.layers.0.mlp.gate_proj.weight: [4, 2] F32 = 32 bytes
      model.layers.0.mlp.up_proj.weight: [4, 2] F32 = 32 bytes
      model.layers.0.mlp.down_proj.weight: [2, 4] F32 = 32 bytes
    Total: 248 bytes
    """
    # Build data: sequential float values
    var tensor_data = List[UInt8]()
    var float_count = 62  # 8+2+8+2+4+4+4+4+2+8+8+8
    for i in range(float_count):
        var bytes = _f32_to_bytes(Float32(i) * 0.1)
        for j in range(4):
            tensor_data.append(bytes[j])

    # Build JSON header with data_offsets
    var json = String('{"model.embed_tokens.weight":{"dtype":"F32","shape":[4,2],"data_offsets":[0,32]}')
    json += ',"model.norm.weight":{"dtype":"F32","shape":[2],"data_offsets":[32,40]}'
    json += ',"lm_head.weight":{"dtype":"F32","shape":[4,2],"data_offsets":[40,72]}'
    json += ',"model.layers.0.input_layernorm.weight":{"dtype":"F32","shape":[2],"data_offsets":[72,80]}'
    json += ',"model.layers.0.self_attn.q_proj.weight":{"dtype":"F32","shape":[2,2],"data_offsets":[80,96]}'
    json += ',"model.layers.0.self_attn.k_proj.weight":{"dtype":"F32","shape":[2,2],"data_offsets":[96,112]}'
    json += ',"model.layers.0.self_attn.v_proj.weight":{"dtype":"F32","shape":[2,2],"data_offsets":[112,128]}'
    json += ',"model.layers.0.self_attn.o_proj.weight":{"dtype":"F32","shape":[2,2],"data_offsets":[128,144]}'
    json += ',"model.layers.0.post_attention_layernorm.weight":{"dtype":"F32","shape":[2],"data_offsets":[144,152]}'
    json += ',"model.layers.0.mlp.gate_proj.weight":{"dtype":"F32","shape":[4,2],"data_offsets":[152,184]}'
    json += ',"model.layers.0.mlp.up_proj.weight":{"dtype":"F32","shape":[4,2],"data_offsets":[184,216]}'
    json += ',"model.layers.0.mlp.down_proj.weight":{"dtype":"F32","shape":[2,4],"data_offsets":[216,248]}'
    json += "}"

    return build_safetensors_from_parts(json, tensor_data)


# ===----------------------------------------------------------------------=== #
# Tests
# ===----------------------------------------------------------------------=== #

fn test_parse_weight_map() raises:
    """Parse a weight_map JSON."""
    var json = String(
        '{"metadata":{"total_size":12345},'
        + '"weight_map":{'
        + '"model.embed_tokens.weight":"model-00001-of-00002.safetensors",'
        + '"model.layers.0.self_attn.q_proj.weight":"model-00001-of-00002.safetensors",'
        + '"model.layers.0.mlp.gate_proj.weight":"model-00002-of-00002.safetensors"'
        + "}}"
    )

    var wm = parse_weight_map(json)

    assert_true(
        "model.embed_tokens.weight" in wm,
        "embed_tokens in weight_map",
    )
    assert_true(
        wm["model.embed_tokens.weight"] == "model-00001-of-00002.safetensors",
        "embed_tokens maps to shard 1",
    )
    assert_true(
        wm["model.layers.0.mlp.gate_proj.weight"] == "model-00002-of-00002.safetensors",
        "gate_proj maps to shard 2",
    )

    print("  parse_weight_map: PASS")


fn test_parse_config_json() raises:
    """Parse a minimal config.json."""
    var json = String(
        '{"vocab_size":32000,"hidden_size":4096,"num_hidden_layers":32,'
        + '"num_attention_heads":32,"num_key_value_heads":8,'
        + '"intermediate_size":14336,"max_position_embeddings":8192}'
    )

    var cfg = parse_config_json(json)

    assert_eq(cfg["vocab_size"], 32000, "vocab_size")
    assert_eq(cfg["hidden_size"], 4096, "hidden_size")
    assert_eq(cfg["num_hidden_layers"], 32, "num_hidden_layers")
    assert_eq(cfg["num_attention_heads"], 32, "num_attention_heads")
    assert_eq(cfg["num_key_value_heads"], 8, "num_key_value_heads")
    assert_eq(cfg["intermediate_size"], 14336, "intermediate_size")

    print("  parse_config_json: PASS")


fn test_safetensors_index_from_string() raises:
    """Build SafeTensorsIndex from JSON string."""
    var json = String(
        '{"weight_map":{'
        + '"model.embed_tokens.weight":"model-00001-of-00002.safetensors",'
        + '"model.norm.weight":"model-00001-of-00002.safetensors",'
        + '"lm_head.weight":"model-00002-of-00002.safetensors"'
        + "}}"
    )

    var index = load_safetensors_index_from_string(json, "/models/llama")

    assert_true(index.has_tensor("model.embed_tokens.weight"), "has embed")
    assert_true(index.has_tensor("lm_head.weight"), "has lm_head")
    assert_true(not index.has_tensor("nonexistent"), "no nonexistent")

    var shard = index.get_shard("model.embed_tokens.weight")
    assert_true(shard == "model-00001-of-00002.safetensors", "correct shard")

    var path = index.get_shard_path("lm_head.weight")
    assert_true(
        path == "/models/llama/model-00002-of-00002.safetensors",
        "correct shard path",
    )

    print("  safetensors_index_from_string: PASS")


fn test_dtype_element_size() raises:
    """Element sizes for SafeTensors dtypes."""
    assert_eq(dtype_element_size("F32"), 4, "F32 = 4 bytes")
    assert_eq(dtype_element_size("F16"), 2, "F16 = 2 bytes")
    assert_eq(dtype_element_size("BF16"), 2, "BF16 = 2 bytes")
    assert_eq(dtype_element_size("I64"), 8, "I64 = 8 bytes")
    assert_eq(dtype_element_size("U8"), 1, "U8 = 1 byte")
    assert_eq(dtype_element_size("I8"), 1, "I8 = 1 byte")
    assert_eq(dtype_element_size("I32"), 4, "I32 = 4 bytes")

    print("  dtype_element_size: PASS")


fn test_build_safetensors_from_parts() raises:
    """Build SafeTensors binary from JSON header + data."""
    var json = String('{"test":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}')
    var data = List[UInt8]()
    # 2 x F32 = 8 bytes
    var b1 = _f32_to_bytes(Float32(1.0))
    var b2 = _f32_to_bytes(Float32(2.0))
    for i in range(4):
        data.append(b1[i])
    for i in range(4):
        data.append(b2[i])

    var buf = build_safetensors_from_parts(json, data)

    # First 8 bytes = header size (u64 LE)
    var reader = BinaryReader(buf^)
    var header_size = reader.read_u64_le()
    assert_eq(header_size, len(json), "header size matches JSON length")

    # Read back JSON
    var json_back = String("")
    for _ in range(header_size):
        json_back += chr(Int(reader.read_u8()))
    assert_true(json_back == json, "JSON roundtrips")

    # Read first float
    var f1 = reader.read_f32_le()
    assert_near(f1, 1.0, 0.0001, "first float is 1.0")

    var f2 = reader.read_f32_le()
    assert_near(f2, 2.0, 0.0001, "second float is 2.0")

    print("  build_safetensors_from_parts: PASS")


fn test_load_safetensors_from_buffer() raises:
    """Load a tiny SafeTensors model from buffer."""
    var config = _tiny_config()
    var buf = _build_tiny_safetensors()

    var model = load_safetensors_from_buffer(buf^, config)

    # Check embed [4,2]: data values 0*0.1, 1*0.1, ... 7*0.1
    # 2D access: get(row, col) — row=vocab, col=hidden
    assert_near(model.embed.get(0, 0), 0.0, 0.01, "embed(0,0) = 0.0")
    assert_near(model.embed.get(0, 1), 0.1, 0.01, "embed(0,1) = 0.1")
    assert_near(model.embed.get(1, 0), 0.2, 0.01, "embed(1,0) = 0.2")

    # Check norm [2]: data values at positions 8,9 → 0.8, 0.9
    assert_near(model.final_norm.get(0), 0.8, 0.01, "norm[0] = 0.8")
    assert_near(model.final_norm.get(1), 0.9, 0.01, "norm[1] = 0.9")

    # Check lm_head [4,2]: data values at positions 10-17 → 1.0, 1.1, ...
    assert_near(model.lm_head.get(0, 0), 1.0, 0.01, "lm_head(0,0) = 1.0")
    assert_near(model.lm_head.get(0, 1), 1.1, 0.01, "lm_head(0,1) = 1.1")

    print("  load_safetensors_from_buffer: PASS")


fn test_safetensors_model_forward() raises:
    """Loaded SafeTensors model can run forward pass."""
    var config = _tiny_config()
    var buf = _build_tiny_safetensors()

    var model = load_safetensors_from_buffer(buf^, config)

    # Run a forward pass with token 0
    from neutron_mojo.nn.kv_cache import MultiLayerKVCache
    from neutron_mojo.nn.rope import RoPETable

    var cache = MultiLayerKVCache(1, 16, 1, 2)
    var rope = RoPETable(2, 16, 10000.0)

    var logits = model.forward(0, cache, rope, 0)

    # Should produce vocab_size=4 logits
    assert_eq(logits.numel(), 4, "logits has vocab_size elements")

    # Values should be finite (not NaN/Inf)
    var sum = Float32(0.0)
    for i in range(4):
        sum += logits.get(i)
    # Sum should be some finite value (not zero since weights are non-zero)
    assert_true(sum == sum, "logits are finite (not NaN)")

    print("  safetensors_model_forward: PASS")


fn test_safetensors_sharded_index_multi_shard() raises:
    """Index correctly routes tensors to different shards."""
    var json = String(
        '{"weight_map":{'
        + '"model.embed_tokens.weight":"shard-001.safetensors",'
        + '"model.layers.0.self_attn.q_proj.weight":"shard-001.safetensors",'
        + '"model.layers.0.self_attn.k_proj.weight":"shard-001.safetensors",'
        + '"model.layers.0.mlp.gate_proj.weight":"shard-002.safetensors",'
        + '"model.layers.0.mlp.up_proj.weight":"shard-002.safetensors",'
        + '"lm_head.weight":"shard-003.safetensors"'
        + "}}"
    )

    var index = load_safetensors_index_from_string(json, "/data")

    # Verify shard routing
    assert_true(
        index.get_shard("model.embed_tokens.weight") == "shard-001.safetensors",
        "embed in shard 1",
    )
    assert_true(
        index.get_shard("model.layers.0.mlp.gate_proj.weight") == "shard-002.safetensors",
        "gate_proj in shard 2",
    )
    assert_true(
        index.get_shard("lm_head.weight") == "shard-003.safetensors",
        "lm_head in shard 3",
    )

    # Verify full paths
    assert_true(
        index.get_shard_path("lm_head.weight") == "/data/shard-003.safetensors",
        "lm_head full path",
    )

    print("  safetensors_sharded_index_multi_shard: PASS")


fn test_weight_map_empty() raises:
    """Handle empty weight map gracefully."""
    var json = String('{"weight_map":{}}')
    var wm = parse_weight_map(json)
    # Empty dict — no tensors
    # Just verify it doesn't crash
    assert_true("anything" not in wm, "empty weight map has no entries")

    print("  weight_map_empty: PASS")


fn test_config_json_with_strings() raises:
    """Config JSON parser skips string/array/object values correctly."""
    var json = String(
        '{"model_type":"llama","vocab_size":32000,'
        + '"hidden_size":4096,"torch_dtype":"float16",'
        + '"architectures":["LlamaForCausalLM"],'
        + '"rope_scaling":null,"num_hidden_layers":32}'
    )

    var cfg = parse_config_json(json)

    # Should have parsed integer fields
    assert_eq(cfg["vocab_size"], 32000, "vocab_size parsed")
    assert_eq(cfg["hidden_size"], 4096, "hidden_size parsed")
    assert_eq(cfg["num_hidden_layers"], 32, "num_hidden_layers parsed")

    # String/array/null fields should be skipped (not in dict)
    assert_true("model_type" not in cfg, "model_type skipped")

    print("  config_json_with_strings: PASS")


fn main() raises:
    print("test_safetensors_sharded:")

    test_parse_weight_map()
    test_parse_config_json()
    test_safetensors_index_from_string()
    test_dtype_element_size()
    test_build_safetensors_from_parts()
    test_load_safetensors_from_buffer()
    test_safetensors_model_forward()
    test_safetensors_sharded_index_multi_shard()
    test_weight_map_empty()
    test_config_json_with_strings()

    print("ALL PASSED (10 tests)")
