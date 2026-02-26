# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Q8_0/Q4_0 Weight Reader Tests
# ===----------------------------------------------------------------------=== #

"""Tests for quantized tensor reading and dequantization."""

from math import abs
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.binary_reader import BinaryReader
from neutron_mojo.io.gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUFFile,
    GGUF_F32,
    GGUF_Q8_0,
    GGUF_Q4_0,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    _align_offset,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)
from neutron_mojo.model.weight_reader import (
    read_tensor_q8_0_as_f32,
    read_tensor_q4_0_as_f32,
    load_gguf_model_from_buffer,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("Assertion failed: " + msg)


fn assert_near(a: Float32, b: Float32, tol: Float32, msg: String) raises:
    if abs(a - b) > tol:
        raise Error(
            "Assertion failed: " + msg
            + " got " + String(a) + " vs " + String(b)
        )


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error(
            "Assertion failed: " + msg
            + " expected " + String(b) + " got " + String(a)
        )


# ===----------------------------------------------------------------------=== #
# Helpers: write FP16 bytes
# ===----------------------------------------------------------------------=== #

fn _write_fp16_le(mut buf: List[UInt8], bits: Int):
    """Write a 16-bit FP16 value as little-endian bytes.

    Args:
        buf: Output buffer.
        bits: Raw 16-bit FP16 representation.
    """
    buf.append(UInt8(bits & 0xFF))
    buf.append(UInt8((bits >> 8) & 0xFF))


# ===----------------------------------------------------------------------=== #
# Tests: Q8_0
# ===----------------------------------------------------------------------=== #

fn test_read_q8_0_single_block() raises:
    """Test reading a single Q8_0 block (32 elements)."""
    var buf = List[UInt8]()

    # Write one Q8_0 block: scale=1.0 (FP16 0x3C00), 32 INT8 values
    _write_fp16_le(buf, 0x3C00)  # scale = 1.0

    # Write signed INT8 values: 0, 1, 2, ..., 31
    for i in range(32):
        buf.append(UInt8(i))

    var reader = BinaryReader(buf^)
    var t = read_tensor_q8_0_as_f32(reader, 0, 32)

    assert_eq(t.numel(), 32, "numel=32")
    # Dequant: val = int8 * scale = i * 1.0
    assert_near(t.get(0), 0.0, 0.001, "q8[0]")
    assert_near(t.get(1), 1.0, 0.001, "q8[1]")
    assert_near(t.get(10), 10.0, 0.001, "q8[10]")
    assert_near(t.get(31), 31.0, 0.001, "q8[31]")

    print("  read_q8_0_single_block: PASS")


fn test_read_q8_0_multi_block() raises:
    """Test reading Q8_0 across 2 blocks (64 elements)."""
    var buf = List[UInt8]()

    # Block 1: scale=0.5 (FP16 0x3800), values 0..31
    _write_fp16_le(buf, 0x3800)  # scale = 0.5
    for i in range(32):
        buf.append(UInt8(i))

    # Block 2: scale=1.0 (FP16 0x3C00), values with negative:
    # 0, 255(-1), 254(-2), 253(-3), ...
    _write_fp16_le(buf, 0x3C00)  # scale = 1.0
    for i in range(32):
        if i < 16:
            buf.append(UInt8(i))
        else:
            # 256 - (i - 16 + 1) gives negative: -1, -2, ..., -16
            buf.append(UInt8(256 - (i - 15)))

    var reader = BinaryReader(buf^)
    var t = read_tensor_q8_0_as_f32(reader, 0, 64)

    assert_eq(t.numel(), 64, "numel=64")
    # Block 1: val = i * 0.5
    assert_near(t.get(0), 0.0, 0.001, "blk1[0]")
    assert_near(t.get(2), 1.0, 0.001, "blk1[2]=2*0.5")
    assert_near(t.get(10), 5.0, 0.001, "blk1[10]=10*0.5")

    # Block 2: positive values
    assert_near(t.get(32), 0.0, 0.001, "blk2[0]")
    assert_near(t.get(33), 1.0, 0.001, "blk2[1]")

    # Block 2: negative values (idx 48 = block2 idx 16 = -1 * 1.0)
    assert_near(t.get(48), -1.0, 0.001, "blk2[16]=-1")
    assert_near(t.get(49), -2.0, 0.001, "blk2[17]=-2")

    print("  read_q8_0_multi_block: PASS")


# ===----------------------------------------------------------------------=== #
# Tests: Q4_0
# ===----------------------------------------------------------------------=== #

fn test_read_q4_0_single_block() raises:
    """Test reading a single Q4_0 block (32 elements)."""
    var buf = List[UInt8]()

    # Write one Q4_0 block: scale=1.0 (FP16 0x3C00), 16 packed nibble bytes
    _write_fp16_le(buf, 0x3C00)  # scale = 1.0

    # 16 bytes of packed nibbles. Each byte: low nibble = first val, high = second.
    # Byte 0: lo=8 (8-8=0), hi=9 (9-8=1) => byte = 0x98
    # Byte 1: lo=10 (10-8=2), hi=11 (11-8=3) => byte = 0xBA
    # etc. For simplicity, write all nibbles as 8 (value=0 after centering)
    for _ in range(16):
        buf.append(UInt8(0x88))  # lo=8, hi=8 -> both dequant to 0.0

    var reader = BinaryReader(buf^)
    var t = read_tensor_q4_0_as_f32(reader, 0, 32)

    assert_eq(t.numel(), 32, "numel=32")
    # All values should be (8-8)*1.0 = 0.0
    for i in range(32):
        assert_near(t.get(i), 0.0, 0.001, "q4[" + String(i) + "]")

    print("  read_q4_0_single_block: PASS")


fn test_read_q4_0_multi_block() raises:
    """Test reading Q4_0 across 2 blocks (64 elements) with non-trivial values."""
    var buf = List[UInt8]()

    # Block 1: scale=0.5, specific nibble values
    _write_fp16_le(buf, 0x3800)  # scale = 0.5
    # Byte 0: lo=0, hi=15 => vals=(0-8)*0.5=-4.0, (15-8)*0.5=3.5
    buf.append(UInt8(0xF0))
    # Byte 1: lo=8, hi=8 => vals=0.0, 0.0
    buf.append(UInt8(0x88))
    # Remaining 14 bytes: all 0x88 = (8-8)=0
    for _ in range(14):
        buf.append(UInt8(0x88))

    # Block 2: scale=1.0, all 0x88
    _write_fp16_le(buf, 0x3C00)  # scale = 1.0
    for _ in range(16):
        buf.append(UInt8(0x88))

    var reader = BinaryReader(buf^)
    var t = read_tensor_q4_0_as_f32(reader, 0, 64)

    assert_eq(t.numel(), 64, "numel=64")

    # Block 1: byte 0 -> [0]=(0-8)*0.5=-4.0, [1]=(15-8)*0.5=3.5
    assert_near(t.get(0), -4.0, 0.001, "blk1[0]")
    assert_near(t.get(1), 3.5, 0.001, "blk1[1]")
    # Block 1: byte 1 -> [2]=0.0, [3]=0.0
    assert_near(t.get(2), 0.0, 0.001, "blk1[2]")
    assert_near(t.get(3), 0.0, 0.001, "blk1[3]")

    # Block 2: all zeros
    assert_near(t.get(32), 0.0, 0.001, "blk2[0]")

    print("  read_q4_0_multi_block: PASS")


# ===----------------------------------------------------------------------=== #
# Tests: _try_load_tensor with Q8_0 / Q4_0
# ===----------------------------------------------------------------------=== #

fn _build_gguf_with_q8_tensor() raises -> List[UInt8]:
    """Build a GGUF with a Q8_0 embedding tensor and F32 norm/lm_head."""
    var buf = List[UInt8]()

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)  # version
    _write_u64_le(buf, 3)  # tensor_count
    _write_u64_le(buf, 7)  # metadata_count

    # Metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 4)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # Tensor info: embed [8,4] Q8_0 = 32 elements = 1 block = 34 bytes
    _write_string_gguf(buf, "model.embed_tokens.weight")
    _write_u32_le(buf, 2)   # n_dims
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 8)   # Q8_0
    _write_u64_le(buf, 0)   # offset

    # norm [4] F32 = 16 bytes
    _write_string_gguf(buf, "model.norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 34)  # after Q8_0 block

    # lm_head [8,4] F32 = 128 bytes
    _write_string_gguf(buf, "lm_head.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 50)  # 34 + 16

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Data: Q8_0 embed block (34 bytes) - scale=0.5, values 0..31
    _write_fp16_le(buf, 0x3800)  # scale=0.5
    for i in range(32):
        buf.append(UInt8(i))

    # F32 norm (16 bytes) - all 1.0
    for _ in range(4):
        _write_f32_le(buf, Float32(1.0))

    # F32 lm_head (128 bytes)
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.01)

    return buf^


fn test_try_load_tensor_q8_0() raises:
    """Test loading a Q8_0 tensor through the model pipeline."""
    var buf = _build_gguf_with_q8_tensor()
    var model = load_gguf_model_from_buffer(buf^)

    # Embed should be dequantized: val = i * 0.5
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.0, 0.001, "q8 embed[0]")
    assert_near(ptr[1], 0.5, 0.001, "q8 embed[1]")
    assert_near(ptr[2], 1.0, 0.001, "q8 embed[2]")
    _ = model.embed.numel()

    # Norm should be 1.0
    assert_near(model.final_norm.get(0), 1.0, 0.001, "norm[0]")

    print("  try_load_tensor_q8_0: PASS")


fn _build_gguf_with_q4_tensor() raises -> List[UInt8]:
    """Build a GGUF with a Q4_0 embedding tensor and F32 norm/lm_head."""
    var buf = List[UInt8]()

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 3)  # tensor_count
    _write_u64_le(buf, 7)  # metadata_count

    # Metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 4)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # Tensor info: embed [8,4] Q4_0 = 32 elements = 1 block = 18 bytes
    _write_string_gguf(buf, "model.embed_tokens.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 2)   # Q4_0
    _write_u64_le(buf, 0)

    # norm [4] F32 = 16 bytes
    _write_string_gguf(buf, "model.norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 18)  # after Q4_0 block

    # lm_head [8,4] F32 = 128 bytes
    _write_string_gguf(buf, "lm_head.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 34)  # 18 + 16

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Data: Q4_0 embed block (18 bytes) - scale=1.0, all nibbles=8 (value=0)
    _write_fp16_le(buf, 0x3C00)  # scale=1.0
    for _ in range(16):
        buf.append(UInt8(0x88))  # lo=8, hi=8 -> (8-8)*1.0 = 0.0

    # F32 norm
    for _ in range(4):
        _write_f32_le(buf, Float32(1.0))

    # F32 lm_head
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.01)

    return buf^


fn test_try_load_tensor_q4_0() raises:
    """Test loading a Q4_0 tensor through the model pipeline."""
    var buf = _build_gguf_with_q4_tensor()
    var model = load_gguf_model_from_buffer(buf^)

    # Embed should be dequantized: all zeros since nibble=8, (8-8)*1.0=0
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.0, 0.001, "q4 embed[0]")
    assert_near(ptr[1], 0.0, 0.001, "q4 embed[1]")
    _ = model.embed.numel()

    # Norm should be 1.0
    assert_near(model.final_norm.get(0), 1.0, 0.001, "q4 norm[0]")

    print("  try_load_tensor_q4_0: PASS")


fn test_mixed_tensor_types() raises:
    """Test GGUF with F32 embed + Q8_0 lm_head in same file."""
    var buf = List[UInt8]()

    # Header
    _write_u32_le(buf, GGUF_MAGIC)
    _write_u32_le(buf, 3)
    _write_u64_le(buf, 3)  # tensor_count
    _write_u64_le(buf, 7)  # metadata_count

    # Metadata
    _write_string_gguf(buf, "general.architecture")
    _write_u32_le(buf, 8)
    _write_string_gguf(buf, "llama")

    _write_string_gguf(buf, "llama.block_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.embedding_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 4)

    _write_string_gguf(buf, "llama.attention.head_count")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 2)

    _write_string_gguf(buf, "llama.attention.head_count_kv")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 1)

    _write_string_gguf(buf, "llama.feed_forward_length")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    _write_string_gguf(buf, "llama.vocab_size")
    _write_u32_le(buf, 4)
    _write_u32_le(buf, 8)

    # Tensor info:
    # embed [8,4] F32 = 128 bytes at offset 0
    _write_string_gguf(buf, "model.embed_tokens.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 0)

    # norm [4] F32 = 16 bytes at offset 128
    _write_string_gguf(buf, "model.norm.weight")
    _write_u32_le(buf, 1)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 0)   # F32
    _write_u64_le(buf, 128)

    # lm_head [8,4] Q8_0 = 32 elements = 1 block = 34 bytes at offset 144
    _write_string_gguf(buf, "lm_head.weight")
    _write_u32_le(buf, 2)
    _write_u64_le(buf, 8)
    _write_u64_le(buf, 4)
    _write_u32_le(buf, 8)   # Q8_0
    _write_u64_le(buf, 144)

    # Align
    var aligned = _align_offset(len(buf), GGUF_DEFAULT_ALIGNMENT)
    while len(buf) < aligned:
        buf.append(0)

    # Data: F32 embed (128 bytes)
    for i in range(32):
        _write_f32_le(buf, Float32(i) * 0.1)

    # F32 norm (16 bytes)
    for _ in range(4):
        _write_f32_le(buf, Float32(1.0))

    # Q8_0 lm_head (34 bytes) - scale=0.5, values 0..31
    _write_fp16_le(buf, 0x3800)  # scale=0.5
    for i in range(32):
        buf.append(UInt8(i))

    var model = load_gguf_model_from_buffer(buf^)

    # Verify F32 embed
    var ptr = model.embed.data_ptr()
    assert_near(ptr[0], 0.0, 0.001, "f32 embed[0]")
    assert_near(ptr[1], 0.1, 0.001, "f32 embed[1]")
    _ = model.embed.numel()

    # Verify Q8_0 lm_head dequantized: val = i * 0.5
    var lm_ptr = model.lm_head.data_ptr()
    assert_near(lm_ptr[0], 0.0, 0.001, "q8 lm_head[0]")
    assert_near(lm_ptr[1], 0.5, 0.001, "q8 lm_head[1]")
    assert_near(lm_ptr[2], 1.0, 0.001, "q8 lm_head[2]")
    _ = model.lm_head.numel()

    print("  mixed_tensor_types: PASS")


fn main() raises:
    print("test_quant_weight_reader:")

    test_read_q8_0_single_block()
    test_read_q8_0_multi_block()
    test_read_q4_0_single_block()
    test_read_q4_0_multi_block()
    test_try_load_tensor_q8_0()
    test_try_load_tensor_q4_0()
    test_mixed_tensor_types()

    print("ALL PASSED (7 tests)")
