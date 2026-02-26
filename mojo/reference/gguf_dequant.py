"""GGUF block dequantization — correctness oracle.

GGUF (GPT-Generated Unified Format) uses structured block quantization
from llama.cpp. Each quantization type uses fixed-size blocks with
scales and optional mins.

Supported types (Phase 1):
  - Q4_K_M: 256-element super-blocks, 32-element sub-blocks, 4-bit
  - Q5_K_M: Same structure, 5-bit values
  - Q8_0: 32-element blocks, 8-bit, simple scale-only

Reference: llama.cpp ggml-quants.c

Tolerance: Exact (integer arithmetic, deterministic)
"""

import numpy as np
import struct


def dequant_q8_0(block_data: np.ndarray, scale: float) -> np.ndarray:
    """Dequantize a Q8_0 block: 32 int8 values with one FP16 scale.

    Q8_0 format per block (34 bytes):
      - scale: float16 (2 bytes)
      - quants: int8[32] (32 bytes)

    Dequant: x_i = quant_i * scale
    """
    assert len(block_data) == 32
    return block_data.astype(np.float32) * np.float32(scale)


def dequant_q4_k_m(
    quants: np.ndarray,
    scales: np.ndarray,
    mins: np.ndarray,
    d: float,
    dmin: float,
) -> np.ndarray:
    """Dequantize a Q4_K_M super-block (256 elements).

    Q4_K_M format per super-block:
      - d: float16 — super-block scale for scales
      - dmin: float16 — super-block scale for mins
      - scales: uint8[12] — packed 6-bit scales and 6-bit mins for 8 sub-blocks
      - quants: uint8[128] — packed 4-bit values (2 per byte), 256 values total

    Each sub-block (32 elements):
      x_i = d * sc * q_i - dmin * m

    where sc is the 6-bit sub-block scale and m is the 6-bit sub-block min.
    """
    assert len(quants) == 128  # 256 values packed as 4-bit pairs
    assert len(scales) == 8    # 6-bit scales, already unpacked
    assert len(mins) == 8      # 6-bit mins, already unpacked

    result = np.zeros(256, dtype=np.float32)

    for sub_block in range(8):
        sc = float(scales[sub_block])
        m = float(mins[sub_block])

        for i in range(32):
            global_idx = sub_block * 32 + i
            byte_idx = global_idx // 2

            if global_idx % 2 == 0:
                q = quants[byte_idx] & 0x0F
            else:
                q = (quants[byte_idx] >> 4) & 0x0F

            result[global_idx] = d * sc * float(q) - dmin * m

    return result


def dequant_q5_k_m(
    quants: np.ndarray,
    high_bits: np.ndarray,
    scales: np.ndarray,
    mins: np.ndarray,
    d: float,
    dmin: float,
) -> np.ndarray:
    """Dequantize a Q5_K_M super-block (256 elements).

    Like Q4_K_M but with an extra high bit per value (5-bit total).
    The high bits are packed separately.
    """
    assert len(quants) == 128   # low 4 bits, packed
    assert len(high_bits) == 32 # high bit, packed as uint8 (256 bits = 32 bytes)
    assert len(scales) == 8
    assert len(mins) == 8

    result = np.zeros(256, dtype=np.float32)

    for sub_block in range(8):
        sc = float(scales[sub_block])
        m = float(mins[sub_block])

        for i in range(32):
            global_idx = sub_block * 32 + i
            byte_idx = global_idx // 2

            # Low 4 bits
            if global_idx % 2 == 0:
                q_low = quants[byte_idx] & 0x0F
            else:
                q_low = (quants[byte_idx] >> 4) & 0x0F

            # High bit
            hb_byte = high_bits[global_idx // 8]
            hb = (hb_byte >> (global_idx % 8)) & 1

            q = q_low | (hb << 4)  # 5-bit value
            result[global_idx] = d * sc * float(q) - dmin * m

    return result


def pack_4bit(values: np.ndarray) -> np.ndarray:
    """Pack an array of 4-bit values (0-15) into bytes (2 per byte)."""
    assert len(values) % 2 == 0
    assert np.all(values >= 0) and np.all(values <= 15)
    low = values[0::2].astype(np.uint8)
    high = values[1::2].astype(np.uint8)
    return low | (high << 4)


def unpack_4bit(packed: np.ndarray) -> np.ndarray:
    """Unpack bytes into 4-bit values."""
    low = packed & 0x0F
    high = (packed >> 4) & 0x0F
    return np.stack([low, high], axis=-1).flatten()


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _test_q8_0_basic():
    """Q8_0: simple scale * int8."""
    data = np.arange(-16, 16, dtype=np.int8)
    scale = 0.5
    result = dequant_q8_0(data, scale)
    expected = np.arange(-16, 16, dtype=np.float32) * 0.5
    assert np.array_equal(result, expected), f"Q8_0 basic failed"
    print("  Q8_0 basic: PASS")


def _test_q8_0_zero_scale():
    data = np.ones(32, dtype=np.int8) * 42
    result = dequant_q8_0(data, scale=0.0)
    assert np.all(result == 0.0), "Q8_0 zero scale failed"
    print("  Q8_0 zero scale: PASS")


def _test_q4_k_m():
    """Q4_K_M: verify dequantization with known values."""
    rng = np.random.default_rng(42)

    # Create fake quantized data
    q_values = rng.integers(0, 16, size=256).astype(np.uint8)
    packed = pack_4bit(q_values)
    scales = rng.integers(1, 64, size=8).astype(np.uint8)
    mins = rng.integers(0, 32, size=8).astype(np.uint8)
    d = 0.01
    dmin = 0.005

    result = dequant_q4_k_m(packed, scales, mins, d, dmin)

    # Verify manually for first sub-block
    for i in range(32):
        q = q_values[i]
        expected = d * float(scales[0]) * float(q) - dmin * float(mins[0])
        assert abs(result[i] - expected) < 1e-6, \
            f"Q4_K_M[{i}]: got {result[i]}, expected {expected}"

    print("  Q4_K_M: PASS")


def _test_pack_unpack():
    """4-bit pack/unpack roundtrip."""
    values = np.array([0, 15, 7, 8, 1, 14, 3, 12], dtype=np.uint8)
    packed = pack_4bit(values)
    unpacked = unpack_4bit(packed)
    assert np.array_equal(values, unpacked), "Pack/unpack roundtrip failed"
    print("  pack/unpack roundtrip: PASS")


def _test_q5_k_m():
    """Q5_K_M: verify 5-bit dequantization."""
    rng = np.random.default_rng(7)

    q_values_4bit = rng.integers(0, 16, size=256).astype(np.uint8)
    packed = pack_4bit(q_values_4bit)
    high_bits_values = rng.integers(0, 2, size=256).astype(np.uint8)

    # Pack high bits into bytes
    high_bits_packed = np.zeros(32, dtype=np.uint8)
    for i in range(256):
        if high_bits_values[i]:
            high_bits_packed[i // 8] |= (1 << (i % 8))

    scales = rng.integers(1, 64, size=8).astype(np.uint8)
    mins = rng.integers(0, 32, size=8).astype(np.uint8)
    d = 0.01
    dmin = 0.005

    result = dequant_q5_k_m(packed, high_bits_packed, scales, mins, d, dmin)

    # Verify first sub-block
    for i in range(32):
        q = q_values_4bit[i] | (high_bits_values[i] << 4)
        expected = d * float(scales[0]) * float(q) - dmin * float(mins[0])
        assert abs(result[i] - expected) < 1e-6, \
            f"Q5_K_M[{i}]: got {result[i]}, expected {expected}"

    print("  Q5_K_M: PASS")


def _test_value_ranges():
    """Dequantized values are in reasonable range."""
    rng = np.random.default_rng(99)

    q_values = rng.integers(0, 16, size=256).astype(np.uint8)
    packed = pack_4bit(q_values)
    scales = np.ones(8, dtype=np.uint8) * 32
    mins = np.ones(8, dtype=np.uint8) * 16
    d = 0.01
    dmin = 0.005

    result = dequant_q4_k_m(packed, scales, mins, d, dmin)
    assert np.all(np.isfinite(result)), "Non-finite values in dequant"
    # Max possible: d * 63 * 15 = 0.01 * 63 * 15 = 9.45
    assert np.max(np.abs(result)) < 10.0, f"Values out of range: max={np.max(np.abs(result))}"
    print("  value ranges: PASS")


if __name__ == "__main__":
    print("gguf_dequant reference tests:")
    _test_q8_0_basic()
    _test_q8_0_zero_scale()
    _test_q4_k_m()
    _test_pack_unpack()
    _test_q5_k_m()
    _test_value_ranges()
    print("ALL PASSED")
