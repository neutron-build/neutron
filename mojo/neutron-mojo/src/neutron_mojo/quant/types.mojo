# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantization Type System
# ===----------------------------------------------------------------------=== #

"""Quantization formats for efficient model storage and inference.

Supports:
- NF4: 4-bit NormalFloat (QLoRA quantization)
- Q4_K: 4-bit K-quantization (GGUF format)
- Q8_0: 8-bit block quantization (GGUF format)
- FP8: 8-bit floating point (E4M3, E5M2)

Reference: GGML quantization schemes, QLoRA paper
"""

from sys import bitwidthof

# ===----------------------------------------------------------------------=== #
# QuantType — Quantization format enumeration
# ===----------------------------------------------------------------------=== #

struct QuantType(Writable, TrivialRegisterPassable):
    """Enum for quantization types."""
    var _value: Int

    # GGUF/GGML formats
    comptime Q4_0 = QuantType(2)    # 4-bit, block size 32
    comptime Q4_1 = QuantType(3)    # 4-bit with min, block size 32
    comptime Q8_0 = QuantType(8)    # 8-bit, block size 32
    comptime Q4_K_S = QuantType(14) # 4-bit K-quant, small
    comptime Q4_K_M = QuantType(15) # 4-bit K-quant, medium

    # QLoRA format
    comptime NF4 = QuantType(20)    # 4-bit NormalFloat

    # FP8 formats
    comptime FP8_E4M3 = QuantType(30) # 8-bit float, 4 exp, 3 mantissa
    comptime FP8_E5M2 = QuantType(31) # 8-bit float, 5 exp, 2 mantissa

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __eq__(self, other: QuantType) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: QuantType) -> Bool:
        return self._value != other._value

    fn bits_per_element(self) -> Int:
        """Return bits per element for this quantization type."""
        if self._value == 2 or self._value == 3:  # Q4_0, Q4_1
            return 4
        elif self._value == 8:  # Q8_0
            return 8
        elif self._value == 14 or self._value == 15:  # Q4_K
            return 4
        elif self._value == 20:  # NF4
            return 4
        elif self._value == 30 or self._value == 31:  # FP8
            return 8
        return 16  # Default to FP16

    fn block_size(self) -> Int:
        """Return block size for block quantization formats."""
        if self._value == 2 or self._value == 3 or self._value == 8:
            return 32  # Q4_0, Q4_1, Q8_0
        elif self._value == 14:  # Q4_K_S
            return 64
        elif self._value == 15:  # Q4_K_M
            return 256
        elif self._value == 20:  # NF4
            return 64
        return 1  # No blocking

    fn write_to[W: Writer](self, mut writer: W):
        if self._value == 2:
            writer.write("Q4_0")
        elif self._value == 3:
            writer.write("Q4_1")
        elif self._value == 8:
            writer.write("Q8_0")
        elif self._value == 14:
            writer.write("Q4_K_S")
        elif self._value == 15:
            writer.write("Q4_K_M")
        elif self._value == 20:
            writer.write("NF4")
        elif self._value == 30:
            writer.write("FP8_E4M3")
        elif self._value == 31:
            writer.write("FP8_E5M2")
        else:
            writer.write("Unknown")


# ===----------------------------------------------------------------------=== #
# QuantConfig — Quantization configuration
# ===----------------------------------------------------------------------=== #

struct QuantConfig(Copyable):
    """Configuration for a quantization scheme.

    Specifies the quantization format, block size, and metadata like
    whether scales/zero-points are per-block or per-tensor.
    """
    var qtype: QuantType
    var block_size: Int
    var has_zero_point: Bool
    var has_min_max: Bool
    var scale_dtype: DType  # DType for scale factors (usually FP16 or FP32)

    fn __init__(out self, qtype: QuantType):
        self.qtype = qtype
        self.block_size = qtype.block_size()
        self.has_zero_point = False
        self.has_min_max = False
        self.scale_dtype = DType.float16

    fn __copyinit__(out self, existing: Self):
        self.qtype = existing.qtype
        self.block_size = existing.block_size
        self.has_zero_point = existing.has_zero_point
        self.has_min_max = existing.has_min_max
        self.scale_dtype = existing.scale_dtype

    fn with_zero_point(var self) -> Self:
        """Enable zero-point offset."""
        self.has_zero_point = True
        return self^

    fn with_min_max(var self) -> Self:
        """Enable min/max metadata (for Q4_1, etc.)."""
        self.has_min_max = True
        return self^

    fn with_scale_dtype(var self, dtype: DType) -> Self:
        """Set the dtype for scale factors."""
        self.scale_dtype = dtype
        return self^


# ===----------------------------------------------------------------------=== #
# Predefined Quantization Configurations
# ===----------------------------------------------------------------------=== #

fn q4_0_config() -> QuantConfig:
    """Q4_0: 4-bit quantization, block size 32, FP16 scale."""
    return QuantConfig(QuantType.Q4_0)

fn q4_1_config() -> QuantConfig:
    """Q4_1: 4-bit quantization with min, block size 32, FP16 scale + min."""
    return QuantConfig(QuantType.Q4_1).with_min_max()

fn q8_0_config() -> QuantConfig:
    """Q8_0: 8-bit quantization, block size 32, FP16 scale."""
    return QuantConfig(QuantType.Q8_0)

fn q4_k_m_config() -> QuantConfig:
    """Q4_K_M: 4-bit K-quantization (medium), block size 256."""
    return QuantConfig(QuantType.Q4_K_M)

fn nf4_config() -> QuantConfig:
    """NF4: 4-bit NormalFloat (QLoRA), block size 64, FP16 scale."""
    return QuantConfig(QuantType.NF4)

fn fp8_e4m3_config() -> QuantConfig:
    """FP8 E4M3: 8-bit float, 4 exp bits, 3 mantissa bits."""
    var cfg = QuantConfig(QuantType.FP8_E4M3)
    cfg.block_size = 1  # No blocking for FP8
    return cfg^

fn fp8_e5m2_config() -> QuantConfig:
    """FP8 E5M2: 8-bit float, 5 exp bits, 2 mantissa bits."""
    var cfg = QuantConfig(QuantType.FP8_E5M2)
    cfg.block_size = 1  # No blocking for FP8
    return cfg^


# ===----------------------------------------------------------------------=== #
# QuantBlock — A quantized block of values
# ===----------------------------------------------------------------------=== #

struct QuantBlock[qtype: QuantType](Copyable, Movable):
    """A block of quantized values.

    For block quantization (Q4_0, Q8_0, etc.), data is divided into blocks,
    each with its own scale factor and optional min/zero-point.
    """
    var scale: Float16
    var min_val: Float16  # For Q4_1, etc.
    var data: List[UInt8]  # Packed quantized values

    fn __init__(out self, block_size: Int):
        self.scale = Float16(1.0)
        self.min_val = Float16(0.0)
        self.data = List[UInt8]()
        # Reserve space for packed data
        # For 4-bit: block_size / 2 bytes
        # For 8-bit: block_size bytes
        var bytes_needed = block_size if Self.qtype.bits_per_element() == 8 else block_size // 2
        for _ in range(bytes_needed):
            self.data.append(0)

    fn __copyinit__(out self, existing: Self):
        self.scale = existing.scale
        self.min_val = existing.min_val
        self.data = existing.data.copy()

    fn size_bytes(self) -> Int:
        """Return size in bytes for this block."""
        # Scale (2 bytes) + optional min (2 bytes) + data
        # Q4_1 (type 3) has min, others don't
        var metadata_bytes = 4 if Self.qtype._value == 3 else 2
        return metadata_bytes + len(self.data)


# ===----------------------------------------------------------------------=== #
# Utility Functions
# ===----------------------------------------------------------------------=== #

fn calc_quant_size(num_elements: Int, qtype: QuantType) -> Int:
    """Calculate total bytes needed for quantized storage.

    Args:
        num_elements: Number of FP32 elements.
        qtype: Quantization type.

    Returns:
        Total bytes needed for quantized representation.
    """
    var block_size = qtype.block_size()
    var num_blocks = (num_elements + block_size - 1) // block_size

    var bits_per_elem = qtype.bits_per_element()
    var data_bytes_per_block = (block_size * bits_per_elem) // 8

    # Metadata: scale (2 bytes FP16) + optional min (2 bytes)
    var metadata_per_block = 2 if qtype._value != 3 else 4  # Q4_1 has min

    return num_blocks * (metadata_per_block + data_bytes_per_block)


fn is_symmetric_quant(qtype: QuantType) -> Bool:
    """Check if quantization is symmetric (zero-centered).

    Symmetric: Q4_0, Q8_0, NF4
    Asymmetric: Q4_1 (has min offset)
    """
    return qtype._value == 2 or qtype._value == 8 or qtype._value == 20
