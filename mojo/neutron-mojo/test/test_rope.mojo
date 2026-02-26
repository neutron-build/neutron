# ===----------------------------------------------------------------------=== #
# Neutron Mojo — RoPE Tests
# ===----------------------------------------------------------------------=== #

"""Tests for Rotary Position Embeddings."""

from math import sin, cos, abs
from neutron_mojo.nn.rope import RoPETable, apply_rope, apply_rope_single_head
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg + " got " + String(a) + " vs " + String(b)
        )


fn test_rope_table_creation() raises:
    """Test RoPE table creation with correct dimensions."""
    var table = RoPETable(head_dim=64, max_seq_len=128)

    assert_true(table.head_dim == 64, "head_dim should be 64")
    assert_true(table.max_seq_len == 128, "max_seq_len should be 128")

    # cos/sin tables should have shape [max_seq_len, head_dim/2]
    assert_true(table.cos_table.numel() == 128 * 32, "cos table size")
    assert_true(table.sin_table.numel() == 128 * 32, "sin table size")

    print("  rope_table_creation: PASS")


fn test_rope_table_values_pos0() raises:
    """Test that position 0 has cos=1, sin=0 for all frequencies."""
    var table = RoPETable(head_dim=8, max_seq_len=4)

    var half_dim = 4
    for i in range(half_dim):
        var cos_val = table.cos_table.get(i)  # pos=0, offset = 0*4 + i
        var sin_val = table.sin_table.get(i)
        assert_near(cos_val, 1.0, 1e-5, "cos(0) should be 1")
        assert_near(sin_val, 0.0, 1e-5, "sin(0) should be 0")

    print("  rope_table_values_pos0: PASS")


fn test_rope_table_values_known() raises:
    """Test RoPE table against hand-computed values."""
    var table = RoPETable(head_dim=4, max_seq_len=4, theta=10000.0)

    # For head_dim=4, half_dim=2
    # freq_0 = 1.0 / (10000^(0/4)) = 1.0
    # freq_1 = 1.0 / (10000^(2/4)) = 1.0/100 = 0.01
    #
    # pos=1, i=0: angle = 1 * 1.0 = 1.0
    # cos(1.0) ≈ 0.5403, sin(1.0) ≈ 0.8415
    var cos_1_0 = table.cos_table.get(1 * 2 + 0)  # pos=1, freq=0
    var sin_1_0 = table.sin_table.get(1 * 2 + 0)
    assert_near(cos_1_0, Float32(cos(Float64(1.0))), 1e-4, "cos(1.0)")
    assert_near(sin_1_0, Float32(sin(Float64(1.0))), 1e-4, "sin(1.0)")

    # pos=1, i=1: angle = 1 * 0.01 = 0.01
    var cos_1_1 = table.cos_table.get(1 * 2 + 1)
    var sin_1_1 = table.sin_table.get(1 * 2 + 1)
    assert_near(cos_1_1, Float32(cos(Float64(0.01))), 1e-4, "cos(0.01)")
    assert_near(sin_1_1, Float32(sin(Float64(0.01))), 1e-4, "sin(0.01)")

    print("  rope_table_values_known: PASS")


fn test_rope_table_custom_theta() raises:
    """Test RoPE table with Llama-3 theta=500000."""
    var table = RoPETable(head_dim=4, max_seq_len=2, theta=500000.0)

    # freq_0 = 1.0 / (500000^(0/4)) = 1.0
    # pos=1, i=0: angle = 1.0
    var cos_val = table.cos_table.get(1 * 2 + 0)
    assert_near(cos_val, Float32(cos(Float64(1.0))), 1e-4, "Llama-3 theta cos")

    # freq_1 = 1.0 / (500000^(2/4)) = 1.0 / sqrt(500000) ≈ 0.001414
    var expected_freq = 1.0 / (500000.0 ** 0.5)
    var expected_angle = 1.0 * expected_freq
    var sin_val = table.sin_table.get(1 * 2 + 1)
    assert_near(sin_val, Float32(sin(expected_angle)), 1e-4, "Llama-3 theta sin")

    print("  rope_table_custom_theta: PASS")


fn test_apply_rope_identity_at_pos0() raises:
    """Test that RoPE at position 0 is identity (cos=1, sin=0)."""
    var table = RoPETable(head_dim=4, max_seq_len=8)
    var head_dim = 4
    var num_heads = 2
    var seq_len = 1

    # Create tensor [seq_len=1, num_heads=2, head_dim=4]
    var x = Tensor[DType.float32](Shape(seq_len * num_heads * head_dim))
    # Fill with known values
    for i in range(num_heads * head_dim):
        x.set(i, Float32(i + 1))

    # Save original values
    var orig = List[Float32]()
    for i in range(num_heads * head_dim):
        orig.append(x.get(i))

    # Apply at pos 0 — should be identity
    apply_rope(x, table, start_pos=0, seq_len=1, num_heads=num_heads)

    for i in range(num_heads * head_dim):
        assert_near(x.get(i), orig[i], 1e-4, "pos 0 should be identity")

    print("  apply_rope_identity_at_pos0: PASS")


fn test_apply_rope_rotation() raises:
    """Test that RoPE actually rotates values at non-zero positions."""
    var table = RoPETable(head_dim=4, max_seq_len=8)
    var head_dim = 4

    # Create tensor [1, 1, 4] = just one head
    var x = Tensor[DType.float32](Shape(head_dim))
    x.set(0, 1.0)
    x.set(1, 0.0)
    x.set(2, 1.0)
    x.set(3, 0.0)

    # Apply at position 1
    apply_rope(x, table, start_pos=1, seq_len=1, num_heads=1)

    # Pair (x[0], x[1]) rotated by angle = 1.0 * freq_0 = 1.0
    # x_rot[0] = 1*cos(1) - 0*sin(1) = cos(1) ≈ 0.5403
    # x_rot[1] = 1*sin(1) + 0*cos(1) = sin(1) ≈ 0.8415
    assert_near(x.get(0), Float32(cos(Float64(1.0))), 1e-4, "rotated x[0]")
    assert_near(x.get(1), Float32(sin(Float64(1.0))), 1e-4, "rotated x[1]")

    print("  apply_rope_rotation: PASS")


fn test_apply_rope_rotation_formula() raises:
    """Test the full rotation formula with non-trivial input."""
    var table = RoPETable(head_dim=4, max_seq_len=8)

    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 3.0)
    x.set(1, 4.0)
    x.set(2, 1.0)
    x.set(3, 2.0)

    # Position 2, freq_0 = 1.0, angle_0 = 2.0
    apply_rope(x, table, start_pos=2, seq_len=1, num_heads=1)

    var c0 = Float32(cos(Float64(2.0)))
    var s0 = Float32(sin(Float64(2.0)))
    # x_rot[0] = 3*cos(2) - 4*sin(2)
    # x_rot[1] = 3*sin(2) + 4*cos(2)
    assert_near(x.get(0), 3.0 * c0 - 4.0 * s0, 1e-3, "formula x[0]")
    assert_near(x.get(1), 3.0 * s0 + 4.0 * c0, 1e-3, "formula x[1]")

    print("  apply_rope_rotation_formula: PASS")


fn test_apply_rope_multi_head() raises:
    """Test RoPE with multiple heads."""
    var table = RoPETable(head_dim=4, max_seq_len=8)
    var num_heads = 2

    # [seq_len=1, num_heads=2, head_dim=4]
    var x = Tensor[DType.float32](Shape(num_heads * 4))
    # Head 0: [1, 0, 1, 0]
    x.set(0, 1.0)
    x.set(1, 0.0)
    x.set(2, 1.0)
    x.set(3, 0.0)
    # Head 1: [0, 1, 0, 1]
    x.set(4, 0.0)
    x.set(5, 1.0)
    x.set(6, 0.0)
    x.set(7, 1.0)

    apply_rope(x, table, start_pos=1, seq_len=1, num_heads=2)

    # Head 0, pair 0: x_rot[0] = 1*cos(1) - 0*sin(1) = cos(1)
    assert_near(x.get(0), Float32(cos(Float64(1.0))), 1e-4, "head0 x[0]")

    # Head 1, pair 0: x_rot[4] = 0*cos(1) - 1*sin(1) = -sin(1)
    assert_near(x.get(4), -Float32(sin(Float64(1.0))), 1e-4, "head1 x[0]")

    # Head 1, pair 0: x_rot[5] = 0*sin(1) + 1*cos(1) = cos(1)
    assert_near(x.get(5), Float32(cos(Float64(1.0))), 1e-4, "head1 x[1]")

    print("  apply_rope_multi_head: PASS")


fn test_apply_rope_multi_seq() raises:
    """Test RoPE with multiple sequence positions."""
    var table = RoPETable(head_dim=4, max_seq_len=8)

    # [seq_len=2, num_heads=1, head_dim=4]
    var x = Tensor[DType.float32](Shape(2 * 4))
    # Pos 0: [1, 0, 1, 0]
    x.set(0, 1.0)
    x.set(1, 0.0)
    x.set(2, 1.0)
    x.set(3, 0.0)
    # Pos 1: [1, 0, 1, 0]
    x.set(4, 1.0)
    x.set(5, 0.0)
    x.set(6, 1.0)
    x.set(7, 0.0)

    apply_rope(x, table, start_pos=0, seq_len=2, num_heads=1)

    # Pos 0 should be identity
    assert_near(x.get(0), 1.0, 1e-4, "seq pos 0 identity")
    assert_near(x.get(1), 0.0, 1e-4, "seq pos 0 identity")

    # Pos 1 should be rotated by angle=1.0
    assert_near(x.get(4), Float32(cos(Float64(1.0))), 1e-4, "seq pos 1 rotated")
    assert_near(x.get(5), Float32(sin(Float64(1.0))), 1e-4, "seq pos 1 rotated")

    print("  apply_rope_multi_seq: PASS")


fn test_apply_rope_single_head_fn() raises:
    """Test apply_rope_single_head convenience function."""
    var table = RoPETable(head_dim=4, max_seq_len=8)

    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 1.0)
    x.set(1, 0.0)
    x.set(2, 1.0)
    x.set(3, 0.0)

    apply_rope_single_head(x, table, pos=1)

    assert_near(x.get(0), Float32(cos(Float64(1.0))), 1e-4, "single head cos")
    assert_near(x.get(1), Float32(sin(Float64(1.0))), 1e-4, "single head sin")

    print("  apply_rope_single_head_fn: PASS")


fn test_rope_relative_position() raises:
    """Test that RoPE preserves relative position information.

    The dot product of two RoPE-rotated vectors at positions m and n
    should depend only on (m - n), not on absolute positions.
    """
    var table = RoPETable(head_dim=4, max_seq_len=32)

    # Create two copies of same vector, rotate at pos 2 and 5
    var a1 = Tensor[DType.float32](Shape(4))
    var b1 = Tensor[DType.float32](Shape(4))
    a1.set(0, 1.0)
    a1.set(1, 2.0)
    a1.set(2, 3.0)
    a1.set(3, 4.0)
    b1.set(0, 1.0)
    b1.set(1, 2.0)
    b1.set(2, 3.0)
    b1.set(3, 4.0)
    apply_rope_single_head(a1, table, pos=2)
    apply_rope_single_head(b1, table, pos=5)

    # Dot product at offset 3
    var dot1: Float32 = 0.0
    for i in range(4):
        dot1 += a1.get(i) * b1.get(i)

    # Same vectors at pos 10 and 13 (also offset 3)
    var a2 = Tensor[DType.float32](Shape(4))
    var b2 = Tensor[DType.float32](Shape(4))
    a2.set(0, 1.0)
    a2.set(1, 2.0)
    a2.set(2, 3.0)
    a2.set(3, 4.0)
    b2.set(0, 1.0)
    b2.set(1, 2.0)
    b2.set(2, 3.0)
    b2.set(3, 4.0)
    apply_rope_single_head(a2, table, pos=10)
    apply_rope_single_head(b2, table, pos=13)

    var dot2: Float32 = 0.0
    for i in range(4):
        dot2 += a2.get(i) * b2.get(i)

    # Both dot products should be equal (relative position invariance)
    assert_near(dot1, dot2, 1e-3, "relative position invariance")

    print("  rope_relative_position: PASS")


fn test_rope_start_pos_offset() raises:
    """Test start_pos for KV cache continuation."""
    var table = RoPETable(head_dim=4, max_seq_len=8)

    # Apply at start_pos=5 for a single token
    var x = Tensor[DType.float32](Shape(4))
    x.set(0, 1.0)
    x.set(1, 0.0)
    x.set(2, 1.0)
    x.set(3, 0.0)
    apply_rope(x, table, start_pos=5, seq_len=1, num_heads=1)

    # Should be same as single_head at pos=5
    var y = Tensor[DType.float32](Shape(4))
    y.set(0, 1.0)
    y.set(1, 0.0)
    y.set(2, 1.0)
    y.set(3, 0.0)
    apply_rope_single_head(y, table, pos=5)

    for i in range(4):
        assert_near(x.get(i), y.get(i), 1e-5, "start_pos matches single_head")

    print("  rope_start_pos_offset: PASS")


fn main() raises:
    print("test_rope:")

    test_rope_table_creation()
    test_rope_table_values_pos0()
    test_rope_table_values_known()
    test_rope_table_custom_theta()
    test_apply_rope_identity_at_pos0()
    test_apply_rope_rotation()
    test_apply_rope_rotation_formula()
    test_apply_rope_multi_head()
    test_apply_rope_multi_seq()
    test_apply_rope_single_head_fn()
    test_rope_relative_position()
    test_rope_start_pos_offset()

    print("ALL PASSED")
