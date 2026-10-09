# ===----------------------------------------------------------------------=== #
# Neutron Mojo — GGUF File Format Parser
# ===----------------------------------------------------------------------=== #

"""GGUF file format parser for loading llama.cpp model weights.

GGUF (GPT-Generated Unified Format) is used by llama.cpp:
- Magic: "GGUF" (4 bytes)
- Version: u32
- Tensor count: u64
- Metadata count: u64
- Metadata key-value pairs
- Tensor information
- Tensor data (aligned)

Reference: https://github.com/ggerganov/llama.cpp/blob/master/gguf-py/gguf/constants.py
"""

from std.collections import Dict
from neutron_mojo.io.binary_reader import BinaryReader
from neutron_mojo.model.config import ModelConfig, RoPEConfig, ACT_SILU
from neutron_mojo.model.architecture import ArchitectureConfig, detect_architecture
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# GGUF Constants
# ===----------------------------------------------------------------------=== #

comptime GGUF_MAGIC = 0x46554747  # "GGUF" in little-endian
comptime GGUF_VERSION = 3
comptime GGUF_DEFAULT_ALIGNMENT = 32

# GGUF metadata value types
comptime GGUF_TYPE_UINT8 = 0
comptime GGUF_TYPE_INT8 = 1
comptime GGUF_TYPE_UINT16 = 2
comptime GGUF_TYPE_INT16 = 3
comptime GGUF_TYPE_UINT32 = 4
comptime GGUF_TYPE_INT32 = 5
comptime GGUF_TYPE_FLOAT32 = 6
comptime GGUF_TYPE_BOOL = 7
comptime GGUF_TYPE_STRING = 8
comptime GGUF_TYPE_ARRAY = 9
comptime GGUF_TYPE_UINT64 = 10
comptime GGUF_TYPE_INT64 = 11
comptime GGUF_TYPE_FLOAT64 = 12


# ===----------------------------------------------------------------------=== #
# GGUF Tensor Type Enum
# ===----------------------------------------------------------------------=== #

struct GGUFTensorType(Writable, Copyable, Movable, ImplicitlyCopyable):
    """GGUF tensor type enumeration."""
    var _value: Int

    @implicit
    def __init__(out self, value: Int):
        self._value = value

    def __init__(out self, *, copy: Self):
        self._value = copy._value

    def __eq__(self, other: GGUFTensorType) -> Bool:
        return self._value == other._value

    def __ne__(self, other: GGUFTensorType) -> Bool:
        return self._value != other._value

    def write_to(self, mut writer: Some[Writer]):
        if self._value == 0:
            writer.write("F32")
        elif self._value == 1:
            writer.write("F16")
        elif self._value == 2:
            writer.write("Q4_0")
        elif self._value == 3:
            writer.write("Q4_1")
        elif self._value == 8:
            writer.write("Q8_0")
        elif self._value == 12:
            writer.write("Q4_K")
        else:
            writer.write("Unknown")


# Tensor type constants (factory functions)
def GGUF_F32() -> GGUFTensorType:
    return GGUFTensorType(0)

def GGUF_F16() -> GGUFTensorType:
    return GGUFTensorType(1)

def GGUF_Q4_0() -> GGUFTensorType:
    return GGUFTensorType(2)

def GGUF_Q4_1() -> GGUFTensorType:
    return GGUFTensorType(3)

def GGUF_Q8_0() -> GGUFTensorType:
    return GGUFTensorType(8)

def GGUF_Q4_K() -> GGUFTensorType:
    return GGUFTensorType(12)


# ===----------------------------------------------------------------------=== #
# GGUF Tensor Info
# ===----------------------------------------------------------------------=== #

struct GGUFTensorInfo(Copyable, ImplicitlyCopyable):
    """Metadata for a single tensor in GGUF file."""
    var name: String
    var n_dims: Int
    var shape: List[Int]  # GGUF file order (fastest axis first)
    var tensor_type: GGUFTensorType
    var offset: Int  # Offset in data section

    def __init__(out self):
        self.name = String("")
        self.n_dims = 0
        self.shape = List[Int]()
        self.tensor_type = GGUFTensorType(0)  # Default to F32
        self.offset = 0

    def __init__(out self, *, copy: Self):
        self.name = copy.name
        self.n_dims = copy.n_dims
        self.shape = copy.shape.copy()
        self.tensor_type = copy.tensor_type.copy()
        self.offset = copy.offset

    def numel(self) raises -> Int:
        """Calculate total number of elements."""
        return Shape(self.shape.copy()).numel()


# ===----------------------------------------------------------------------=== #
# GGUF File Reader
# ===----------------------------------------------------------------------=== #

struct GGUFFile(Movable):
    """GGUF file reader.

    Parses the GGUF format and provides access to tensor metadata
    and model metadata (string/int/float key-value pairs).
    """
    var magic: Int
    var version: Int
    var tensor_count: Int
    var metadata_count: Int
    var alignment: Int
    var data_offset: Int  # Offset where tensor data begins
    var tensors: Dict[String, GGUFTensorInfo]

    # Metadata dictionaries
    var metadata_str: Dict[String, String]
    var metadata_int: Dict[String, Int]
    var metadata_float: Dict[String, Float64]

    # Tokenizer data (populated during parsing for array metadata)
    var token_vocab: List[String]
    var token_scores: List[Float64]
    var token_merges: List[String]

    def __init__(out self):
        self.magic = 0
        self.version = 0
        self.tensor_count = 0
        self.metadata_count = 0
        self.alignment = GGUF_DEFAULT_ALIGNMENT
        self.data_offset = 0
        self.tensors = Dict[String, GGUFTensorInfo]()
        self.metadata_str = Dict[String, String]()
        self.metadata_int = Dict[String, Int]()
        self.metadata_float = Dict[String, Float64]()
        self.token_vocab = List[String]()
        self.token_scores = List[Float64]()
        self.token_merges = List[String]()

    def __init__(out self, *, deinit move: Self):
        self.magic = move.magic^
        self.version = move.version^
        self.tensor_count = move.tensor_count^
        self.metadata_count = move.metadata_count^
        self.alignment = move.alignment^
        self.data_offset = move.data_offset^
        self.tensors = move.tensors^
        self.metadata_str = move.metadata_str^
        self.metadata_int = move.metadata_int^
        self.metadata_float = move.metadata_float^
        self.token_vocab = move.token_vocab^
        self.token_scores = move.token_scores^
        self.token_merges = move.token_merges^

    def is_valid(self) -> Bool:
        """Check if file has valid GGUF magic number.

        Returns:
            True if magic number matches "GGUF".
        """
        return self.magic == GGUF_MAGIC

    def register_tensor(
        mut self,
        name: String,
        shape: List[Int],
        tensor_type: GGUFTensorType,
        offset: Int,
    ) raises:
        """Manually register a tensor (for testing without file parsing).

        Args:
            name: Tensor name.
            shape: Tensor dimensions.
            tensor_type: GGUF tensor type.
            offset: Offset in data section.
        """
        if self.alignment <= 0 or offset < 0 or offset % self.alignment != 0:
            raise Error("Unaligned GGUF tensor offset")
        var admitted_shape = Shape(shape.copy())
        var info = GGUFTensorInfo()
        info.name = name
        info.n_dims = len(shape)
        info.shape = shape.copy()
        info.tensor_type = tensor_type.copy()
        info.offset = offset
        self.tensors[name] = info^

    def has_tensor(self, name: String) -> Bool:
        """Check if a tensor exists in the file.

        Args:
            name: Tensor name.

        Returns:
            True if tensor exists.
        """
        return name in self.tensors

    def get_tensor_info(self, name: String) raises -> GGUFTensorInfo:
        """Get metadata for a tensor.

        Args:
            name: Tensor name.

        Returns:
            GGUFTensorInfo containing shape, type, and offset.
        """
        if name not in self.tensors:
            raise Error("Tensor not found: " + name)
        return self.tensors[name].copy()

    def get_tensor_offset(self, name: String) raises -> Int:
        """Get absolute file offset for tensor data.

        Args:
            name: Tensor name.

        Returns:
            Absolute offset in file where tensor data begins.
        """
        var info = self.get_tensor_info(name)
        return self.data_offset + info.offset

    def tensor_count_total(self) -> Int:
        """Get total number of tensors in file.

        Returns:
            Number of tensors.
        """
        return self.tensor_count

    def get_str(self, key: String, default: String) -> String:
        """Get a string metadata value."""
        if key in self.metadata_str:
            try:
                return String(self.metadata_str[key])
            except:
                return default
        return default

    def get_int(self, key: String, default: Int) -> Int:
        """Get an integer metadata value."""
        if key in self.metadata_int:
            try:
                return self.metadata_int[key]
            except:
                return default
        return default

    def get_float(self, key: String, default: Float64) -> Float64:
        """Get a float metadata value."""
        if key in self.metadata_float:
            try:
                return self.metadata_float[key]
            except:
                return default
        return default


# ===----------------------------------------------------------------------=== #
# Utility Functions
# ===----------------------------------------------------------------------=== #

def gguf_type_to_dtype(tensor_type: GGUFTensorType) -> DType:
    """Convert GGUF tensor type to Mojo DType.

    Args:
        tensor_type: GGUF tensor type.

    Returns:
        Corresponding Mojo DType (for non-quantized types).
    """
    if tensor_type._value == 0:  # F32
        return DType.float32
    elif tensor_type._value == 1:  # F16
        return DType.float16
    else:
        # Quantized types would need special handling
        return DType.uint8


def dtype_to_gguf_type(dtype: DType) -> GGUFTensorType:
    """Convert Mojo DType to GGUF tensor type.

    Args:
        dtype: Mojo DType.

    Returns:
        Corresponding GGUF tensor type.
    """
    if dtype == DType.float32:
        return GGUFTensorType(0)  # F32
    elif dtype == DType.float16:
        return GGUFTensorType(1)  # F16
    else:
        return GGUFTensorType(0)  # Default to F32


def calculate_tensor_size(shape: List[Int], tensor_type: GGUFTensorType) raises -> Int:
    """Calculate tensor size in bytes based on shape and type.

    Args:
        shape: Tensor dimensions.
        tensor_type: GGUF tensor type.

    Returns:
        Size in bytes.
    """
    var numel = Shape(shape.copy()).numel()
    var width = 0
    var units = numel
    if tensor_type._value == 0:
        width = 4
    elif tensor_type._value == 1:
        width = 2
    elif tensor_type._value == 2 or tensor_type._value == 8:
        if len(shape) == 0 or shape[0] % 32 != 0:
            raise Error("GGUF quantized rows must contain complete 32-element blocks")
        units = numel // 32
        width = 18 if tensor_type._value == 2 else 34
    else:
        raise Error("Unsupported GGUF tensor storage codec")
    if units > 0x7FFFFFFFFFFFFFFF // width:
        raise Error("GGUF tensor byte size overflow")
    return units * width


# ===----------------------------------------------------------------------=== #
# Alignment
# ===----------------------------------------------------------------------=== #

def _align_offset(offset: Int, alignment: Int) raises -> Int:
    """Round offset up to the next alignment boundary.

    Args:
        offset: Current offset.
        alignment: Alignment boundary.

    Returns:
        Aligned offset.
    """
    if offset < 0 or alignment <= 0 or offset > 0x7FFFFFFFFFFFFFFF - (alignment - 1):
        raise Error("Invalid or overflowing alignment")
    var remainder = offset % alignment
    if remainder == 0:
        return offset
    return offset + (alignment - remainder)


# ===----------------------------------------------------------------------=== #
# GGUF Binary Parser
# ===----------------------------------------------------------------------=== #

def _skip_gguf_value(mut reader: BinaryReader, value_type: Int, depth: Int = 0) raises:
    """Skip a metadata value based on its type.

    Args:
        reader: BinaryReader positioned at the value.
        value_type: GGUF value type ID.
    """
    if depth < 0 or depth > 128:
        raise Error("GGUF metadata nesting exceeds supported depth")
    if value_type == GGUF_TYPE_UINT8 or value_type == GGUF_TYPE_INT8 or value_type == GGUF_TYPE_BOOL:
        reader.skip(1)
    elif value_type == GGUF_TYPE_UINT16 or value_type == GGUF_TYPE_INT16:
        reader.skip(2)
    elif value_type == GGUF_TYPE_UINT32 or value_type == GGUF_TYPE_INT32 or value_type == GGUF_TYPE_FLOAT32:
        reader.skip(4)
    elif value_type == GGUF_TYPE_UINT64 or value_type == GGUF_TYPE_INT64 or value_type == GGUF_TYPE_FLOAT64:
        reader.skip(8)
    elif value_type == GGUF_TYPE_STRING:
        var s = reader.read_string_gguf()
        _ = s
    elif value_type == GGUF_TYPE_ARRAY:
        var elem_type = reader.read_u32_le()
        var count = reader.read_u64_le()
        for _ in range(count):
            _skip_gguf_value(reader, elem_type, depth + 1)
    else:
        raise Error("Unknown GGUF value type: " + String(value_type))


def parse_gguf_file(path: String) raises -> GGUFFile:
    """Parse a GGUF file from disk.

    Reads the header, metadata key-value pairs, and tensor info sections.

    Args:
        path: Path to .gguf file.

    Returns:
        Populated GGUFFile with metadata and tensor info.
    """
    var reader = BinaryReader(path)
    return _parse_gguf_from_reader(reader)


def parse_gguf_from_buffer(var buf: List[UInt8]) raises -> GGUFFile:
    """Parse a GGUF from an in-memory buffer (for testing).

    Args:
        buf: Raw GGUF bytes.

    Returns:
        Populated GGUFFile.
    """
    var reader = BinaryReader(buf^)
    return _parse_gguf_from_reader(reader)


def _parse_gguf_from_reader(mut reader: BinaryReader) raises -> GGUFFile:
    """Parse GGUF from a BinaryReader.

    Args:
        reader: BinaryReader positioned at start of GGUF data.

    Returns:
        Populated GGUFFile.
    """
    var gguf = GGUFFile()

    # 1. Read and validate magic
    gguf.magic = reader.read_u32_le()
    if gguf.magic != GGUF_MAGIC:
        raise Error(
            "Invalid GGUF magic: expected 0x46554747, got 0x"
            + String(gguf.magic)
        )

    # 2. Read and validate version
    gguf.version = reader.read_u32_le()
    if gguf.version != 2 and gguf.version != 3:
        raise Error("Unsupported GGUF version: " + String(gguf.version))

    # 3. Read counts
    gguf.tensor_count = reader.read_u64_le()
    gguf.metadata_count = reader.read_u64_le()

    # 4. Read metadata key-value pairs
    for _ in range(gguf.metadata_count):
        var key = reader.read_string_gguf()
        var vtype = reader.read_u32_le()
        if key == "general.alignment" and vtype != GGUF_TYPE_UINT32:
            raise Error("GGUF general.alignment must be UInt32")
        if key in gguf.metadata_int or key in gguf.metadata_float or key in gguf.metadata_str or key + ".count" in gguf.metadata_int:
            raise Error("Duplicate GGUF metadata key")

        if vtype == GGUF_TYPE_UINT32:
            var val = reader.read_u32_le()
            gguf.metadata_int[key] = val
        elif vtype == GGUF_TYPE_INT32:
            var val = reader.read_i32_le()
            gguf.metadata_int[key] = val
        elif vtype == GGUF_TYPE_UINT64:
            var val = reader.read_u64_le()
            gguf.metadata_int[key] = val
        elif vtype == GGUF_TYPE_INT64:
            # Read as u64 then interpret
            var val = reader.read_u64_le()
            gguf.metadata_int[key] = val
        elif vtype == GGUF_TYPE_FLOAT32:
            var val = reader.read_f32_le()
            gguf.metadata_float[key] = Float64(val)
        elif vtype == GGUF_TYPE_FLOAT64:
            var val = reader.read_f64_le()
            gguf.metadata_float[key] = val
        elif vtype == GGUF_TYPE_STRING:
            var val = reader.read_string_gguf()
            gguf.metadata_str[key] = val
        elif vtype == GGUF_TYPE_UINT8:
            var val = reader.read_u8()
            gguf.metadata_int[key] = Int(val)
        elif vtype == GGUF_TYPE_INT8:
            var val = reader.read_u8()
            var ival = Int(val)
            if ival > 127:
                ival -= 256
            gguf.metadata_int[key] = ival
        elif vtype == GGUF_TYPE_UINT16:
            var val = reader.read_u16_le()
            gguf.metadata_int[key] = val
        elif vtype == GGUF_TYPE_INT16:
            var val = reader.read_u16_le()
            if val > 32767:
                val -= 65536
            gguf.metadata_int[key] = val
        elif vtype == GGUF_TYPE_BOOL:
            var val = reader.read_u8()
            gguf.metadata_int[key] = Int(val)
        elif vtype == GGUF_TYPE_ARRAY:
            var elem_type = reader.read_u32_le()
            var count = reader.read_u64_le()

            # Handle special tokenizer arrays
            if key == "tokenizer.ggml.tokens" and elem_type == GGUF_TYPE_STRING:
                for _ in range(count):
                    var tok = reader.read_string_gguf()
                    gguf.token_vocab.append(tok)
                gguf.metadata_int[key + ".count"] = count
            elif key == "tokenizer.ggml.merges" and elem_type == GGUF_TYPE_STRING:
                for _ in range(count):
                    var merge = reader.read_string_gguf()
                    gguf.token_merges.append(merge)
                gguf.metadata_int[key + ".count"] = count
            elif key == "tokenizer.ggml.scores" and elem_type == GGUF_TYPE_FLOAT32:
                for _ in range(count):
                    var score = reader.read_f32_le()
                    gguf.token_scores.append(Float64(score))
                gguf.metadata_int[key + ".count"] = count
            else:
                # Store array count in metadata_int, skip elements
                gguf.metadata_int[key + ".count"] = count
                for _ in range(count):
                    _skip_gguf_value(reader, elem_type)
        else:
            _skip_gguf_value(reader, vtype)

    # GGUF alignment is metadata, and applies to both the data base and
    # tensor offsets. Admit supported power-of-two UInt32 alignments only.
    if "general.alignment" in gguf.metadata_int:
        gguf.alignment = gguf.metadata_int["general.alignment"]
    elif "general.alignment" in gguf.metadata_float or "general.alignment" in gguf.metadata_str:
        raise Error("GGUF alignment must be an integer")
    if gguf.alignment <= 0 or gguf.alignment > 0xFFFFFFFF or (gguf.alignment & (gguf.alignment - 1)) != 0:
        raise Error("Invalid GGUF alignment")

    # 5. Read tensor info entries
    for _ in range(gguf.tensor_count):
        var name = reader.read_string_gguf()
        if name in gguf.tensors:
            raise Error("Duplicate GGUF tensor name")
        var n_dims = reader.read_u32_le()
        if n_dims <= 0 or n_dims > reader.remaining() // 8:
            raise Error("Invalid GGUF tensor rank")
        var shape = List[Int]()
        for _ in range(n_dims):
            var dim = reader.read_u64_le()
            shape.append(dim)
        var admitted_shape = Shape(shape.copy())
        var ttype = reader.read_u32_le()
        var offset = reader.read_u64_le()

        if offset % gguf.alignment != 0:
            raise Error("Unaligned GGUF tensor offset")
        var info = GGUFTensorInfo()
        info.name = name
        info.n_dims = n_dims
        info.shape = shape^
        info.tensor_type = GGUFTensorType(ttype)
        info.offset = offset
        gguf.tensors[name] = info^

    # 6. Compute data offset (align current position)
    if reader.tell() > 0x7FFFFFFFFFFFFFFF - (gguf.alignment - 1):
        raise Error("GGUF alignment overflow")
    gguf.data_offset = _align_offset(reader.tell(), gguf.alignment)
    if gguf.data_offset > reader.size:
        raise Error("Truncated GGUF alignment padding")

    return gguf^


# ===----------------------------------------------------------------------=== #
# Config Extraction
# ===----------------------------------------------------------------------=== #

def gguf_to_model_config(gguf: GGUFFile) -> ModelConfig:
    """Extract model configuration from GGUF metadata.

    Maps GGUF metadata keys to ModelConfig fields:
        general.architecture -> model_type
        {arch}.context_length -> max_position_embeddings
        {arch}.embedding_length -> hidden_size
        {arch}.block_count -> num_hidden_layers
        {arch}.attention.head_count -> num_attention_heads
        {arch}.attention.head_count_kv -> num_key_value_heads
        {arch}.feed_forward_length -> intermediate_size
        {arch}.rope.freq_base -> rope.theta

    Args:
        gguf: Parsed GGUFFile with metadata.

    Returns:
        ModelConfig populated from GGUF metadata.
    """
    var cfg = ModelConfig()

    # Get architecture name (e.g., "llama", "mistral")
    var arch = gguf.get_str("general.architecture", "llama")
    cfg.model_type = arch

    # Core dimensions
    cfg.hidden_size = gguf.get_int(arch + ".embedding_length", cfg.hidden_size)
    cfg.num_hidden_layers = gguf.get_int(arch + ".block_count", cfg.num_hidden_layers)
    cfg.num_attention_heads = gguf.get_int(arch + ".attention.head_count", cfg.num_attention_heads)
    cfg.num_key_value_heads = gguf.get_int(arch + ".attention.head_count_kv", cfg.num_key_value_heads)
    cfg.intermediate_size = gguf.get_int(arch + ".feed_forward_length", cfg.intermediate_size)
    cfg.max_position_embeddings = gguf.get_int(arch + ".context_length", cfg.max_position_embeddings)
    cfg.vocab_size = gguf.get_int(arch + ".vocab_size", cfg.vocab_size)

    # Compute head_dim
    if cfg.num_attention_heads > 0:
        cfg.head_dim = cfg.hidden_size // cfg.num_attention_heads

    # RoPE
    var theta_val = gguf.get_float(arch + ".rope.freq_base", cfg.rope.theta)
    cfg.rope.theta = theta_val
    cfg.rope.max_position = cfg.max_position_embeddings

    # Special tokens
    cfg.bos_token_id = gguf.get_int("tokenizer.ggml.bos_token_id", cfg.bos_token_id)
    cfg.eos_token_id = gguf.get_int("tokenizer.ggml.eos_token_id", cfg.eos_token_id)

    return cfg^


def detect_arch_from_gguf(gguf: GGUFFile) raises -> ArchitectureConfig:
    """Auto-detect architecture from GGUF metadata.

    Reads general.architecture and architecture-specific metadata keys
    (sliding window, partial rotary, etc.) to configure the right dispatch.

    Args:
        gguf: Parsed GGUF file with metadata.

    Returns:
        ArchitectureConfig for the detected architecture.
    """
    var arch_name = gguf.get_str("general.architecture", "llama")

    # Check for sliding window metadata (Mistral-style)
    var has_sw = False
    var sw_size = 0
    var sw_val = gguf.get_int(arch_name + ".attention.sliding_window", 0)
    if sw_val > 0:
        has_sw = True
        sw_size = sw_val

    return detect_architecture(arch_name, has_sw, sw_size)


# ===----------------------------------------------------------------------=== #
# GGUF Writer (for tests)
# ===----------------------------------------------------------------------=== #

def _write_u32_le(mut buf: List[UInt8], val: Int):
    """Append a u32 little-endian to buffer."""
    buf.append(UInt8(val & 0xFF))
    buf.append(UInt8((val >> 8) & 0xFF))
    buf.append(UInt8((val >> 16) & 0xFF))
    buf.append(UInt8((val >> 24) & 0xFF))


def _write_u64_le(mut buf: List[UInt8], val: Int):
    """Append a u64 little-endian to buffer."""
    for i in range(8):
        buf.append(UInt8((val >> (i * 8)) & 0xFF))


def _write_string_gguf(mut buf: List[UInt8], s: String):
    """Write a GGUF string (u64 len + bytes)."""
    var bytes = s.as_bytes()
    _write_u64_le(buf, len(bytes))
    for i in range(len(bytes)):
        buf.append(bytes[i])


def _write_f32_le(mut buf: List[UInt8], val: Float32) raises:
    """Write a float32 as little-endian bytes."""
    from std.memory import alloc
    var p = alloc[Float32](1)
    p.store(val)
    var bp = p.bitcast[UInt8]()
    for i in range(4):
        buf.append(bp.load(i))
    p.free()


def build_test_gguf(
    str_keys: List[String],
    str_vals: List[String],
    int_keys: List[String],
    int_vals: List[Int],
    float_keys: List[String],
    float_vals: List[Float64],
    tensor_names: List[String],
    tensor_shapes: List[List[Int]],
    tensor_types: List[Int],
    tensor_data_sizes: List[Int],
) raises -> List[UInt8]:
    """Build a minimal GGUF binary in memory for testing.

    Uses parallel lists instead of dicts to avoid dict iteration issues.

    Args:
        str_keys: String metadata keys.
        str_vals: String metadata values.
        int_keys: Int metadata keys.
        int_vals: Int metadata values.
        float_keys: Float metadata keys.
        float_vals: Float metadata values.
        tensor_names: Names of tensors.
        tensor_shapes: Shapes of tensors.
        tensor_types: GGUF tensor type IDs.
        tensor_data_sizes: Size in bytes of each tensor's data.

    Returns:
        Complete GGUF binary as bytes.
    """
    if len(str_keys) != len(str_vals) or len(int_keys) != len(int_vals) or len(float_keys) != len(float_vals) or len(tensor_names) != len(tensor_shapes) or len(tensor_names) != len(tensor_types) or len(tensor_names) != len(tensor_data_sizes):
        raise Error("Mismatched GGUF fixture lists")
    var alignment = GGUF_DEFAULT_ALIGNMENT
    for i in range(len(int_keys)):
        if int_keys[i] == "general.alignment":
            alignment = int_vals[i]
    if alignment <= 0 or alignment > 0xFFFFFFFF or (alignment & (alignment - 1)) != 0:
        raise Error("Invalid GGUF fixture alignment")
    var offsets = List[Int]()
    var span = 0
    for i in range(len(tensor_names)):
        if tensor_data_sizes[i] < 0:
            raise Error("Negative GGUF fixture tensor bytes")
        span = _align_offset(span, alignment)
        offsets.append(span)
        if tensor_data_sizes[i] > 0x7FFFFFFFFFFFFFFF - span:
            raise Error("Overflowing GGUF fixture bytes")
        span += tensor_data_sizes[i]
    var buf = List[UInt8]()

    # Magic
    _write_u32_le(buf, GGUF_MAGIC)
    # Version
    _write_u32_le(buf, 3)
    # Tensor count
    _write_u64_le(buf, len(tensor_names))
    # Metadata count
    var meta_count = len(str_keys) + len(int_keys) + len(float_keys)
    _write_u64_le(buf, meta_count)

    # Write string metadata
    for i in range(len(str_keys)):
        _write_string_gguf(buf, str_keys[i])
        _write_u32_le(buf, GGUF_TYPE_STRING)
        _write_string_gguf(buf, str_vals[i])

    # Write int metadata (as u32)
    for i in range(len(int_keys)):
        _write_string_gguf(buf, int_keys[i])
        _write_u32_le(buf, GGUF_TYPE_UINT32)
        _write_u32_le(buf, int_vals[i])

    # Write float metadata (as f32)
    for i in range(len(float_keys)):
        _write_string_gguf(buf, float_keys[i])
        _write_u32_le(buf, GGUF_TYPE_FLOAT32)
        _write_f32_le(buf, Float32(float_vals[i]))

    # Write tensor info
    for i in range(len(tensor_names)):
        _write_string_gguf(buf, tensor_names[i])
        var ndims = len(tensor_shapes[i])
        _write_u32_le(buf, ndims)
        for d in range(ndims):
            _write_u64_le(buf, tensor_shapes[i][d])
        _write_u32_le(buf, tensor_types[i])
        _write_u64_le(buf, offsets[i])

    # Pad to alignment
    var aligned = _align_offset(len(buf), alignment)
    while len(buf) < aligned:
        buf.append(0)

    # Write dummy tensor data
    var data_base = len(buf)
    for i in range(len(tensor_data_sizes)):
        while len(buf) - data_base < offsets[i]:
            buf.append(0)
        for _ in range(tensor_data_sizes[i]):
            buf.append(0)

    return buf^
