# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Sprint 2c Integration Tests
# ===----------------------------------------------------------------------=== #

"""End-to-end integration tests for quantization + parsing + model config.

Validates that all Sprint 2c components work together:
- Quantization formats (NF4, Q8_0, Q4_K, FP8)
- File parsers (SafeTensors, GGUF)
- Model configs (Llama-3, Mistral)
- Weight loading interface
"""

from math import abs

# Quantization
from neutron_mojo.quant.nf4 import (
    quantize_nf4,
    dequantize_nf4,
    quantize_nf4_block,
    dequantize_nf4_block,
    get_nf4_value,
)
from neutron_mojo.quant.q8_0 import (
    quantize_q8_0,
    dequantize_q8_0,
    quantize_q8_0_block,
    dequantize_q8_0_block,
)
from neutron_mojo.quant.q4_k import (
    quantize_q4_k,
    dequantize_q4_k,
    quantize_q4_k_block,
    dequantize_q4_k_block,
)
from neutron_mojo.quant.fp8 import quantize_fp8_e4m3, dequantize_fp8_e4m3
from neutron_mojo.quant.types import QuantType, QuantConfig, nf4_config, q8_0_config

# I/O
from neutron_mojo.io.safetensors import TensorInfo, SafeTensorsFile, parse_dtype_string
from neutron_mojo.io.gguf import (
    GGUFTensorInfo,
    GGUFFile,
    GGUF_F16,
    GGUF_Q4_0,
    GGUF_Q8_0,
    calculate_tensor_size,
)

# Model
from neutron_mojo.model.config import (
    ModelConfig,
    llama3_8b_config,
    llama3_70b_config,
    mistral_7b_config,
    layer_weight_name,
    embed_weight_name,
)
from neutron_mojo.model.loader import (
    WeightDescriptor,
    WeightIndex,
    FMT_SAFETENSORS,
    FMT_GGUF,
    detect_format,
    register_safetensors_weight,
    register_gguf_weight,
    validate_weights_for_model,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


# ===----------------------------------------------------------------------=== #
# Integration Test 1: Full NF4 quantization pipeline
# ===----------------------------------------------------------------------=== #

fn test_nf4_full_pipeline() raises:
    """Test NF4 quantize → pack → unpack → dequantize pipeline."""
    # Simulate weight block (8 FP32 values)
    var weights = List[Float32]()
    weights.append(Float32(-0.5))
    weights.append(Float32(-0.3))
    weights.append(Float32(-0.1))
    weights.append(Float32(0.0))
    weights.append(Float32(0.1))
    weights.append(Float32(0.3))
    weights.append(Float32(0.5))
    weights.append(Float32(0.8))

    # Quantize block
    var packed = List[UInt8]()
    for _ in range(4):
        packed.append(UInt8(0))

    var scale = quantize_nf4_block(weights.unsafe_ptr(), packed.unsafe_ptr(), 8)

    # Dequantize block
    var restored = List[Float32]()
    for _ in range(8):
        restored.append(Float32(0.0))

    dequantize_nf4_block(packed.unsafe_ptr(), scale, restored.unsafe_ptr(), 8)

    # Verify roundtrip accuracy
    var max_error = Float32(0.0)
    for i in range(8):
        var err = abs(weights[i] - restored[i])
        if err > max_error:
            max_error = err

    assert_true(max_error < 0.15, "NF4 roundtrip max error should be < 0.15")

    # Verify NF4 table is symmetric around zero
    assert_true(get_nf4_value(0) == Float32(-1.0), "NF4 table min should be -1.0")
    assert_true(get_nf4_value(7) == Float32(0.0), "NF4 table mid should be 0.0")
    assert_true(get_nf4_value(15) == Float32(1.0), "NF4 table max should be 1.0")

    print("  nf4_full_pipeline: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 2: Q8_0 quantization with model dimensions
# ===----------------------------------------------------------------------=== #

fn test_q8_0_model_weight_simulation() raises:
    """Simulate quantizing a Llama-3 weight block with Q8_0."""
    var config = llama3_8b_config()

    # Simulate a 32-element block from a weight matrix
    var block = List[Float32]()
    for i in range(32):
        # Gaussian-like distribution around 0
        var val = Float32(i - 16) / 16.0 * 0.1  # Small weights typical in LLMs
        block.append(val)

    # Quantize
    var q_data = List[Int8]()
    for _ in range(32):
        q_data.append(Int8(0))

    var scale = quantize_q8_0_block(block.unsafe_ptr(), q_data.unsafe_ptr(), 32)

    # Dequantize
    var restored = List[Float32]()
    for _ in range(32):
        restored.append(Float32(0.0))

    dequantize_q8_0_block(q_data.unsafe_ptr(), scale, restored.unsafe_ptr(), 32)

    # Verify roundtrip — Q8_0 should have very low error
    var max_error = Float32(0.0)
    for i in range(32):
        var err = abs(block[i] - restored[i])
        if err > max_error:
            max_error = err

    assert_true(max_error < 0.005, "Q8_0 max error should be < 0.005 for small weights")

    # Verify config-based calculations
    assert_true(config.head_dim == 128, "Llama-3 head_dim check")
    assert_true(config.is_gqa(), "Llama-3 should use GQA")

    print("  q8_0_model_weight_simulation: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 3: Q4_K quantization pipeline
# ===----------------------------------------------------------------------=== #

fn test_q4_k_pipeline() raises:
    """Test Q4_K asymmetric quantization pipeline."""
    var data = List[Float32]()
    for i in range(64):
        data.append(Float32(i) / 10.0 - 3.0)  # Range [-3, 3.3]

    var packed = List[UInt8]()
    for _ in range(32):
        packed.append(UInt8(0))

    var params = quantize_q4_k_block(data.unsafe_ptr(), packed.unsafe_ptr(), 64)

    var restored = List[Float32]()
    for _ in range(64):
        restored.append(Float32(0.0))

    dequantize_q4_k_block(
        packed.unsafe_ptr(), params.scale, params.min_val, restored.unsafe_ptr(), 64
    )

    var max_error = Float32(0.0)
    for i in range(64):
        var err = abs(data[i] - restored[i])
        if err > max_error:
            max_error = err

    # 4-bit quantization has more error
    assert_true(max_error < 0.5, "Q4_K max error should be < 0.5")

    print("  q4_k_pipeline: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 4: SafeTensors + model config + weight loading
# ===----------------------------------------------------------------------=== #

fn test_safetensors_model_loading() raises:
    """Simulate loading a SafeTensors model file."""
    var config = llama3_8b_config()

    # Build a weight index simulating SafeTensors file
    var index = WeightIndex()
    index.format = FMT_SAFETENSORS()

    # Register embedding
    var embed_info = TensorInfo()
    embed_info.dtype = String("F16")
    embed_info.shape = List[Int]()
    embed_info.shape.append(config.vocab_size)
    embed_info.shape.append(config.hidden_size)
    embed_info.data_offset_start = 0
    embed_info.data_offset_end = config.vocab_size * config.hidden_size * 2

    register_safetensors_weight(index, embed_weight_name(), embed_info, 256)

    # Register final norm
    var norm_info = TensorInfo()
    norm_info.dtype = String("F16")
    norm_info.shape = List[Int]()
    norm_info.shape.append(config.hidden_size)
    norm_info.data_offset_start = 0
    norm_info.data_offset_end = config.hidden_size * 2

    register_safetensors_weight(index, "model.norm.weight", norm_info, 256)

    # Register first layer Q projection
    var q_info = TensorInfo()
    q_info.dtype = String("F16")
    q_info.shape = List[Int]()
    q_info.shape.append(config.hidden_size)
    q_info.shape.append(config.hidden_size)
    q_info.data_offset_start = 0
    q_info.data_offset_end = config.hidden_size * config.hidden_size * 2

    register_safetensors_weight(
        index, layer_weight_name(0, "self_attn.q_proj.weight"), q_info, 256
    )

    # Validate
    var valid = validate_weights_for_model(index, config)
    assert_true(valid, "Should validate with required weights")
    assert_true(index.num_weights() == 3, "Should have 3 weights")

    # Check embedding dimensions
    var embed_w = index.get_weight(embed_weight_name())
    assert_true(embed_w.shape[0] == 128256, "Embed vocab dim should be 128256")
    assert_true(embed_w.shape[1] == 4096, "Embed hidden dim should be 4096")
    assert_true(embed_w.dtype == DType.float16, "Embed should be F16")

    print("  safetensors_model_loading: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 5: GGUF + quantized weights + model config
# ===----------------------------------------------------------------------=== #

fn test_gguf_quantized_model_loading() raises:
    """Simulate loading a GGUF quantized model."""
    var config = llama3_8b_config()

    var index = WeightIndex()
    index.format = FMT_GGUF()

    # Embedding (F16)
    var embed_info = GGUFTensorInfo()
    embed_info.name = String("model.embed_tokens.weight")
    embed_info.n_dims = 2
    embed_info.shape = List[Int]()
    embed_info.shape.append(config.vocab_size)
    embed_info.shape.append(config.hidden_size)
    embed_info.tensor_type = GGUF_F16()
    embed_info.offset = 0

    register_gguf_weight(index, embed_info.name, embed_info, 5000)

    # Layer 0 Q projection (quantized Q8_0)
    var q_info = GGUFTensorInfo()
    q_info.name = String("model.layers.0.self_attn.q_proj.weight")
    q_info.n_dims = 2
    q_info.shape = List[Int]()
    q_info.shape.append(config.hidden_size)
    q_info.shape.append(config.hidden_size)
    q_info.tensor_type = GGUF_Q8_0()
    q_info.offset = 1000000

    register_gguf_weight(index, q_info.name, q_info, 5000)

    # Final norm (F16)
    var norm_info = GGUFTensorInfo()
    norm_info.name = String("model.norm.weight")
    norm_info.n_dims = 1
    norm_info.shape = List[Int]()
    norm_info.shape.append(config.hidden_size)
    norm_info.tensor_type = GGUF_F16()
    norm_info.offset = 2000000

    register_gguf_weight(index, norm_info.name, norm_info, 5000)

    # Validate
    var valid = validate_weights_for_model(index, config)
    assert_true(valid, "GGUF model should validate")
    assert_true(index.num_weights() == 3, "Should have 3 weights")

    # Check quantization detected
    var q_w = index.get_weight("model.layers.0.self_attn.q_proj.weight")
    assert_true(q_w.is_quantized, "Q projection should be quantized")
    assert_true(q_w.quant_type == "q8_0", "Should be Q8_0")

    var embed_w = index.get_weight("model.embed_tokens.weight")
    assert_true(not embed_w.is_quantized, "Embedding should not be quantized")

    print("  gguf_quantized_model_loading: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 6: Multi-format weight size comparison
# ===----------------------------------------------------------------------=== #

fn test_weight_size_comparison() raises:
    """Compare weight sizes across quantization formats for same tensor."""
    var config = llama3_8b_config()
    var numel = config.hidden_size * config.hidden_size  # 4096 * 4096

    # FP32: 4 bytes per element
    var fp32_bytes = numel * 4

    # FP16: 2 bytes per element
    var fp16_bytes = numel * 2

    # Q8_0: ~34 bytes per 32 elements
    var q8_blocks = (numel + 31) // 32
    var q8_bytes = q8_blocks * 34

    # Q4_0: ~18 bytes per 32 elements
    var q4_blocks = (numel + 31) // 32
    var q4_bytes = q4_blocks * 18

    # Verify sizes make sense
    assert_true(fp32_bytes == 67108864, "FP32 size: 64MB")
    assert_true(fp16_bytes == 33554432, "FP16 size: 32MB")
    assert_true(q8_bytes < fp16_bytes, "Q8_0 should be smaller than FP16")
    assert_true(q4_bytes < q8_bytes, "Q4_0 should be smaller than Q8_0")

    # Compression ratios
    var q8_ratio = Float32(fp32_bytes) / Float32(q8_bytes)
    var q4_ratio = Float32(fp32_bytes) / Float32(q4_bytes)

    assert_true(q8_ratio > 3.5, "Q8_0 should compress ~3.8x vs FP32")
    assert_true(q4_ratio > 7.0, "Q4_0 should compress ~7.1x vs FP32")

    print("  weight_size_comparison: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 7: Model config → weight count estimation
# ===----------------------------------------------------------------------=== #

fn test_model_weight_inventory() raises:
    """Verify expected weight count for a model architecture."""
    var config = llama3_8b_config()

    # Expected weights per layer:
    # - self_attn: q_proj, k_proj, v_proj, o_proj (4 weight matrices)
    # - mlp: gate_proj, up_proj, down_proj (3 weight matrices)
    # - norms: input_layernorm, post_attention_layernorm (2 vectors)
    # Total per layer: 9 tensors
    var per_layer = 9

    # Global weights:
    # - embed_tokens (1)
    # - norm (1)
    # - lm_head (1)
    var global_weights = 3

    var expected_total = global_weights + (per_layer * config.num_hidden_layers)

    # Llama-3 8B: 3 + 9*32 = 291 tensors
    assert_true(expected_total == 291, "Llama-3 8B should have 291 weight tensors")

    # Parameter estimate
    var params = config.total_params_estimate()
    assert_true(params > 7_000_000_000, "Should estimate >7B params")

    print("  model_weight_inventory: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 8: Quantization config integration
# ===----------------------------------------------------------------------=== #

fn test_quant_config_integration() raises:
    """Test QuantConfig works with model dimensions."""
    var nf4_cfg = nf4_config()
    var q8_cfg = q8_0_config()

    # NF4 config properties
    assert_true(nf4_cfg.qtype == QuantType.NF4, "Should be NF4")
    assert_true(nf4_cfg.block_size == 64, "NF4 block size should be 64")

    # Q8_0 config properties
    assert_true(q8_cfg.qtype == QuantType.Q8_0, "Should be Q8_0")
    assert_true(q8_cfg.block_size == 32, "Q8_0 block size should be 32")

    # Compute blocks needed for a 4096-dim vector
    var hidden = 4096
    var nf4_blocks = (hidden + nf4_cfg.block_size - 1) // nf4_cfg.block_size
    var q8_blocks = (hidden + q8_cfg.block_size - 1) // q8_cfg.block_size

    assert_true(nf4_blocks == 64, "Should need 64 NF4 blocks for 4096 elements")
    assert_true(q8_blocks == 128, "Should need 128 Q8_0 blocks for 4096 elements")

    print("  quant_config_integration: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 9: GQA dimensions validation
# ===----------------------------------------------------------------------=== #

fn test_gqa_weight_dimensions() raises:
    """Verify GQA weight dimensions are consistent."""
    var config = llama3_8b_config()

    # Q projection: hidden_size → hidden_size (all heads)
    var q_shape_0 = config.hidden_size  # 4096
    var q_shape_1 = config.num_attention_heads * config.head_dim  # 32 * 128 = 4096

    # K/V projection: hidden_size → kv_heads * head_dim (fewer heads)
    var kv_shape_0 = config.hidden_size  # 4096
    var kv_shape_1 = config.num_key_value_heads * config.head_dim  # 8 * 128 = 1024

    assert_true(q_shape_0 == 4096, "Q input should be 4096")
    assert_true(q_shape_1 == 4096, "Q output should be 4096")
    assert_true(kv_shape_0 == 4096, "KV input should be 4096")
    assert_true(kv_shape_1 == 1024, "KV output should be 1024 (GQA)")

    # KV is 4x smaller than Q due to GQA
    assert_true(q_shape_1 // kv_shape_1 == config.kv_group_size(), "GQA ratio check")

    print("  gqa_weight_dimensions: PASS")


# ===----------------------------------------------------------------------=== #
# Integration Test 10: 70B vs 8B config comparison
# ===----------------------------------------------------------------------=== #

fn test_model_scale_comparison() raises:
    """Compare 8B and 70B model configs."""
    var cfg_8b = llama3_8b_config()
    var cfg_70b = llama3_70b_config()

    # 70B should be larger in every dimension
    assert_true(cfg_70b.hidden_size > cfg_8b.hidden_size, "70B hidden > 8B hidden")
    assert_true(
        cfg_70b.num_hidden_layers > cfg_8b.num_hidden_layers, "70B layers > 8B layers"
    )
    assert_true(
        cfg_70b.num_attention_heads > cfg_8b.num_attention_heads, "70B heads > 8B heads"
    )
    assert_true(
        cfg_70b.intermediate_size > cfg_8b.intermediate_size, "70B FFN > 8B FFN"
    )

    # Both use GQA with 8 KV heads
    assert_true(cfg_8b.num_key_value_heads == 8, "8B: 8 KV heads")
    assert_true(cfg_70b.num_key_value_heads == 8, "70B: 8 KV heads")

    # 70B has higher GQA ratio
    assert_true(cfg_70b.kv_group_size() == 8, "70B GQA ratio should be 8:1")
    assert_true(cfg_8b.kv_group_size() == 4, "8B GQA ratio should be 4:1")

    # Param estimates
    var params_8b = cfg_8b.total_params_estimate()
    var params_70b = cfg_70b.total_params_estimate()
    assert_true(params_70b > params_8b * 5, "70B should have >5x params of 8B")

    print("  model_scale_comparison: PASS")


fn main() raises:
    print("test_quant_integration:")

    test_nf4_full_pipeline()
    test_q8_0_model_weight_simulation()
    test_q4_k_pipeline()
    test_safetensors_model_loading()
    test_gguf_quantized_model_loading()
    test_weight_size_comparison()
    test_model_weight_inventory()
    test_quant_config_integration()
    test_gqa_weight_dimensions()
    test_model_scale_comparison()

    print("ALL PASSED")
