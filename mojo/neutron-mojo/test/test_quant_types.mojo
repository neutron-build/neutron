# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantization Type System Tests
# ===----------------------------------------------------------------------=== #

"""Tests for quantization types and configurations."""

from neutron_mojo.quant.types import (
    QuantType,
    QuantConfig,
    QuantBlock,
    q4_0_config,
    q4_1_config,
    q8_0_config,
    q4_k_m_config,
    nf4_config,
    fp8_e4m3_config,
    fp8_e5m2_config,
    calc_quant_size,
    is_symmetric_quant,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn test_quant_type_bits() raises:
    """Test bits_per_element for different quant types."""
    assert_true(QuantType.Q4_0.bits_per_element() == 4, "Q4_0 should be 4 bits")
    assert_true(QuantType.Q8_0.bits_per_element() == 8, "Q8_0 should be 8 bits")
    assert_true(QuantType.NF4.bits_per_element() == 4, "NF4 should be 4 bits")
    assert_true(QuantType.FP8_E4M3.bits_per_element() == 8, "FP8_E4M3 should be 8 bits")

    print("  quant_type_bits: PASS")


fn test_quant_type_block_size() raises:
    """Test block_size for different quant types."""
    assert_true(QuantType.Q4_0.block_size() == 32, "Q4_0 block size should be 32")
    assert_true(QuantType.Q8_0.block_size() == 32, "Q8_0 block size should be 32")
    assert_true(QuantType.Q4_K_M.block_size() == 256, "Q4_K_M block size should be 256")
    assert_true(QuantType.NF4.block_size() == 64, "NF4 block size should be 64")
    assert_true(QuantType.FP8_E4M3.block_size() == 1, "FP8 should have no blocking")

    print("  quant_type_block_size: PASS")


fn test_quant_config_basic() raises:
    """Test basic QuantConfig creation."""
    var cfg = QuantConfig(QuantType.Q4_0)

    assert_true(cfg.qtype == QuantType.Q4_0, "Config should have Q4_0 type")
    assert_true(cfg.block_size == 32, "Config should have block size 32")
    assert_true(not cfg.has_zero_point, "Q4_0 should not have zero point")

    print("  quant_config_basic: PASS")


fn test_quant_config_with_zero_point() raises:
    """Test QuantConfig with zero point."""
    var cfg = QuantConfig(QuantType.Q8_0).with_zero_point()

    assert_true(cfg.has_zero_point, "Config should have zero point enabled")

    print("  quant_config_with_zero_point: PASS")


fn test_quant_config_with_min_max() raises:
    """Test QuantConfig with min/max."""
    var cfg = QuantConfig(QuantType.Q4_1).with_min_max()

    assert_true(cfg.has_min_max, "Config should have min/max enabled")

    print("  quant_config_with_min_max: PASS")


fn test_q4_0_config() raises:
    """Test Q4_0 config factory."""
    var cfg = q4_0_config()

    assert_true(cfg.qtype == QuantType.Q4_0, "Should be Q4_0")
    assert_true(cfg.block_size == 32, "Block size should be 32")

    print("  q4_0_config: PASS")


fn test_q8_0_config() raises:
    """Test Q8_0 config factory."""
    var cfg = q8_0_config()

    assert_true(cfg.qtype == QuantType.Q8_0, "Should be Q8_0")
    assert_true(cfg.block_size == 32, "Block size should be 32")

    print("  q8_0_config: PASS")


fn test_nf4_config() raises:
    """Test NF4 config factory."""
    var cfg = nf4_config()

    assert_true(cfg.qtype == QuantType.NF4, "Should be NF4")
    assert_true(cfg.block_size == 64, "NF4 block size should be 64")

    print("  nf4_config: PASS")


fn test_fp8_e4m3_config() raises:
    """Test FP8 E4M3 config factory."""
    var cfg = fp8_e4m3_config()

    assert_true(cfg.qtype == QuantType.FP8_E4M3, "Should be FP8_E4M3")
    assert_true(cfg.block_size == 1, "FP8 should have no blocking")

    print("  fp8_e4m3_config: PASS")


fn test_quant_block_creation() raises:
    """Test QuantBlock creation."""
    var block = QuantBlock[QuantType.Q4_0](32)

    assert_true(block.scale == Float16(1.0), "Default scale should be 1.0")
    assert_true(len(block.data) == 16, "Q4_0 with 32 elements should use 16 bytes (32 * 4 / 8)")

    print("  quant_block_creation: PASS")


fn test_quant_block_size_bytes() raises:
    """Test QuantBlock size calculation."""
    var block = QuantBlock[QuantType.Q4_0](32)

    var size = block.size_bytes()
    # Q4_0: 2 bytes (scale) + 16 bytes (data) = 18 bytes
    assert_true(size == 18, "Q4_0 block should be 18 bytes")

    print("  quant_block_size_bytes: PASS")


fn test_calc_quant_size_q4_0() raises:
    """Test calc_quant_size for Q4_0."""
    # 1024 elements, block size 32 -> 32 blocks
    # Each block: 2 bytes (scale) + 16 bytes (data) = 18 bytes
    # Total: 32 * 18 = 576 bytes
    var size = calc_quant_size(1024, QuantType.Q4_0)

    assert_true(size == 576, "1024 elements Q4_0 should be 576 bytes")

    print("  calc_quant_size_q4_0: PASS")


fn test_calc_quant_size_q8_0() raises:
    """Test calc_quant_size for Q8_0."""
    # 1024 elements, block size 32 -> 32 blocks
    # Each block: 2 bytes (scale) + 32 bytes (data) = 34 bytes
    # Total: 32 * 34 = 1088 bytes
    var size = calc_quant_size(1024, QuantType.Q8_0)

    assert_true(size == 1088, "1024 elements Q8_0 should be 1088 bytes")

    print("  calc_quant_size_q8_0: PASS")


fn test_is_symmetric_quant() raises:
    """Test symmetric quantization detection."""
    assert_true(is_symmetric_quant(QuantType.Q4_0), "Q4_0 should be symmetric")
    assert_true(is_symmetric_quant(QuantType.Q8_0), "Q8_0 should be symmetric")
    assert_true(is_symmetric_quant(QuantType.NF4), "NF4 should be symmetric")
    assert_true(not is_symmetric_quant(QuantType.Q4_1), "Q4_1 should be asymmetric")

    print("  is_symmetric_quant: PASS")


fn main() raises:
    print("test_quant_types:")

    test_quant_type_bits()
    test_quant_type_block_size()
    test_quant_config_basic()
    test_quant_config_with_zero_point()
    test_quant_config_with_min_max()
    test_q4_0_config()
    test_q8_0_config()
    test_nf4_config()
    test_fp8_e4m3_config()
    test_quant_block_creation()
    test_quant_block_size_bytes()
    test_calc_quant_size_q4_0()
    test_calc_quant_size_q8_0()
    test_is_symmetric_quant()

    print("ALL PASSED")
