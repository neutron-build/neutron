# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Binary Reader Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the binary file reader."""

from math import abs
from neutron_mojo.io.binary_reader import BinaryReader, _fp16_to_fp32
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


# ===----------------------------------------------------------------------=== #
# Helper: build a buffer with known bytes
# ===----------------------------------------------------------------------=== #

fn make_test_buffer() -> List[UInt8]:
    """Create a buffer with known test data.

    Layout:
        [0..1]: u8 = 0xAB
        [1..3]: u16_le = 0x1234 → bytes 0x34, 0x12
        [3..7]: u32_le = 0xDEADBEEF → bytes 0xEF, 0xBE, 0xAD, 0xDE
        [7..15]: u64_le = 42 → bytes 42, 0, 0, 0, 0, 0, 0, 0
        [15..19]: i32_le = -1 → bytes 0xFF, 0xFF, 0xFF, 0xFF
        [19..23]: f32_le = 3.14 (IEEE 754: 0x4048F5C3)
        [23..31]: f64_le = 2.718 (IEEE 754: 0x4005BE76C8B43958)
    """
    var buf = List[UInt8]()
    # [0]: u8 = 0xAB
    buf.append(0xAB)
    # [1..3]: u16_le = 0x1234
    buf.append(0x34)
    buf.append(0x12)
    # [3..7]: u32_le = 0xDEADBEEF
    buf.append(0xEF)
    buf.append(0xBE)
    buf.append(0xAD)
    buf.append(0xDE)
    # [7..15]: u64_le = 42
    buf.append(42)
    for _ in range(7):
        buf.append(0)
    # [15..19]: i32_le = -1
    buf.append(0xFF)
    buf.append(0xFF)
    buf.append(0xFF)
    buf.append(0xFF)
    # [19..23]: f32_le = 3.14 → 0x4048F5C3
    buf.append(0xC3)
    buf.append(0xF5)
    buf.append(0x48)
    buf.append(0x40)
    # [23..31]: f64_le = 2.718 → 0x4005BE76C8B43958
    buf.append(0x58)
    buf.append(0x39)
    buf.append(0xB4)
    buf.append(0xC8)
    buf.append(0x76)
    buf.append(0xBE)
    buf.append(0x05)
    buf.append(0x40)
    return buf^


fn test_read_u8() raises:
    """Test reading a single byte."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    var v = r.read_u8()
    assert_true(Int(v) == 0xAB, "read_u8 should return 0xAB")
    assert_true(r.tell() == 1, "cursor should be at 1")
    print("  read_u8: PASS")


fn test_read_u16_le() raises:
    """Test reading a little-endian u16."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    r.seek(1)
    var v = r.read_u16_le()
    assert_true(v == 0x1234, "read_u16_le should return 0x1234")
    assert_true(r.tell() == 3, "cursor should be at 3")
    print("  read_u16_le: PASS")


fn test_read_u32_le() raises:
    """Test reading a little-endian u32."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    r.seek(3)
    var v = r.read_u32_le()
    assert_true(v == 0xDEADBEEF, "read_u32_le should return 0xDEADBEEF")
    print("  read_u32_le: PASS")


fn test_read_u64_le() raises:
    """Test reading a little-endian u64."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    r.seek(7)
    var v = r.read_u64_le()
    assert_true(v == 42, "read_u64_le should return 42")
    print("  read_u64_le: PASS")


fn test_read_i32_le() raises:
    """Test reading a signed little-endian i32."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    r.seek(15)
    var v = r.read_i32_le()
    assert_true(v == -1, "read_i32_le should return -1")
    print("  read_i32_le: PASS")


fn test_read_f32_le() raises:
    """Test reading a little-endian float32."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    r.seek(19)
    var v = r.read_f32_le()
    assert_near(v, 3.14, 0.001, "read_f32_le should return ~3.14")
    print("  read_f32_le: PASS")


fn test_read_f64_le() raises:
    """Test reading a little-endian float64."""
    var buf = make_test_buffer()
    var r = BinaryReader(buf^)
    r.seek(23)
    var v = r.read_f64_le()
    var diff = v - 2.718
    if diff < 0:
        diff = -diff
    assert_true(diff < 0.001, "read_f64_le should return ~2.718")
    print("  read_f64_le: PASS")


fn test_seek_tell_skip() raises:
    """Test seek, tell, skip, and remaining."""
    var buf = make_test_buffer()
    var total = len(buf)
    var r = BinaryReader(buf^)

    assert_true(r.tell() == 0, "initial cursor should be 0")
    assert_true(r.remaining() == total, "remaining should be buffer size")

    r.seek(10)
    assert_true(r.tell() == 10, "after seek(10) cursor should be 10")

    r.skip(5)
    assert_true(r.tell() == 15, "after skip(5) cursor should be 15")
    assert_true(r.remaining() == total - 15, "remaining should be size - 15")

    print("  seek_tell_skip: PASS")


fn test_read_f32_array() raises:
    """Test reading an array of f32 values."""
    # Build buffer with 3 known floats: 1.0, 2.0, 0.5
    var buf = List[UInt8]()

    # 1.0 = 0x3F800000
    buf.append(0x00)
    buf.append(0x00)
    buf.append(0x80)
    buf.append(0x3F)
    # 2.0 = 0x40000000
    buf.append(0x00)
    buf.append(0x00)
    buf.append(0x00)
    buf.append(0x40)
    # 0.5 = 0x3F000000
    buf.append(0x00)
    buf.append(0x00)
    buf.append(0x00)
    buf.append(0x3F)

    var r = BinaryReader(buf^)
    var t = r.read_f32_array(3)
    assert_near(t.get(0), 1.0, 0.0001, "first float should be 1.0")
    assert_near(t.get(1), 2.0, 0.0001, "second float should be 2.0")
    assert_near(t.get(2), 0.5, 0.0001, "third float should be 0.5")
    assert_true(r.tell() == 12, "cursor should advance by 12")

    print("  read_f32_array: PASS")


fn test_read_f16_to_f32_array() raises:
    """Test reading FP16 values and converting to FP32."""
    var buf = List[UInt8]()

    # FP16 1.0 = 0x3C00 → bytes 0x00, 0x3C
    buf.append(0x00)
    buf.append(0x3C)
    # FP16 0.5 = 0x3800 → bytes 0x00, 0x38
    buf.append(0x00)
    buf.append(0x38)
    # FP16 -1.0 = 0xBC00 → bytes 0x00, 0xBC
    buf.append(0x00)
    buf.append(0xBC)
    # FP16 0.0 = 0x0000
    buf.append(0x00)
    buf.append(0x00)

    var r = BinaryReader(buf^)
    var t = r.read_f16_to_f32_array(4)
    assert_near(t.get(0), 1.0, 0.001, "FP16 1.0 should convert to ~1.0")
    assert_near(t.get(1), 0.5, 0.001, "FP16 0.5 should convert to ~0.5")
    assert_near(t.get(2), -1.0, 0.001, "FP16 -1.0 should convert to ~-1.0")
    assert_near(t.get(3), 0.0, 0.001, "FP16 0.0 should convert to 0.0")
    assert_true(r.tell() == 8, "cursor should advance by 8")

    print("  read_f16_to_f32_array: PASS")


fn test_read_string_gguf() raises:
    """Test reading a GGUF-formatted string (u64 len + bytes)."""
    var buf = List[UInt8]()

    # u64 length = 5
    buf.append(5)
    for _ in range(7):
        buf.append(0)
    # "hello" = 0x68, 0x65, 0x6C, 0x6C, 0x6F
    buf.append(0x68)
    buf.append(0x65)
    buf.append(0x6C)
    buf.append(0x6C)
    buf.append(0x6F)

    var r = BinaryReader(buf^)
    var s = r.read_string_gguf()
    assert_true(s == "hello", "should read 'hello'")
    assert_true(r.tell() == 13, "cursor should be at 13")

    print("  read_string_gguf: PASS")


fn test_out_of_bounds() raises:
    """Test that reading past the end raises an error."""
    var buf = List[UInt8]()
    buf.append(1)
    buf.append(2)
    var r = BinaryReader(buf^)

    # Reading 1 byte: OK
    _ = r.read_u8()
    _ = r.read_u8()

    # Now at end — next read should fail
    var caught = False
    try:
        _ = r.read_u8()
    except:
        caught = True
    assert_true(caught, "should raise on read past end")

    # Seek past end should fail
    var caught2 = False
    try:
        r.seek(100)
    except:
        caught2 = True
    assert_true(caught2, "should raise on seek past end")

    print("  out_of_bounds: PASS")


fn test_fp16_special_values() raises:
    """Test FP16→FP32 conversion for special values."""
    # Positive infinity: 0x7C00
    var inf_val = _fp16_to_fp32(0x7C00)
    # Check it's infinity by verifying it's very large
    assert_true(inf_val > 1.0e30, "FP16 +inf should be large")

    # Negative zero: 0x8000
    var neg_zero = _fp16_to_fp32(0x8000)
    assert_near(neg_zero, 0.0, 0.0001, "FP16 -0 should be ~0.0")

    # Small denormal: 0x0001 = 2^(-24) ≈ 5.96e-8
    var denorm = _fp16_to_fp32(0x0001)
    assert_true(denorm > 0.0, "FP16 denorm should be positive")
    assert_true(denorm < 0.001, "FP16 denorm should be very small")

    print("  fp16_special_values: PASS")


fn test_read_bytes() raises:
    """Test reading raw bytes."""
    var buf = List[UInt8]()
    buf.append(0x10)
    buf.append(0x20)
    buf.append(0x30)
    buf.append(0x40)

    var r = BinaryReader(buf^)
    var bytes = r.read_bytes(3)
    assert_true(len(bytes) == 3, "should read 3 bytes")
    assert_true(Int(bytes[0]) == 0x10, "first byte")
    assert_true(Int(bytes[1]) == 0x20, "second byte")
    assert_true(Int(bytes[2]) == 0x30, "third byte")
    assert_true(r.tell() == 3, "cursor at 3")

    print("  read_bytes: PASS")


fn main() raises:
    print("test_binary_reader:")

    test_read_u8()
    test_read_u16_le()
    test_read_u32_le()
    test_read_u64_le()
    test_read_i32_le()
    test_read_f32_le()
    test_read_f64_le()
    test_seek_tell_skip()
    test_read_f32_array()
    test_read_f16_to_f32_array()
    test_read_string_gguf()
    test_out_of_bounds()
    test_fp16_special_values()
    test_read_bytes()

    print("ALL PASSED (14 tests)")
