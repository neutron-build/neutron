# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Export / Serialization
# ===----------------------------------------------------------------------=== #

"""Save and load models in Neutron Model File (.nmf) format.

Format:
    Header: magic(4 bytes "NMF\0") + version(4 bytes u32) + params_len(4 bytes u32)
    Params: newline-delimited key=value inference configuration (NMF v2)
    Weights: raw float32 data (layer_weights, embed, final_norm, lm_head)

This module serializes FP32 Model storage. Quantized model export requires a
separate format adapter.
"""

from std.memory import Pointer, alloc
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.io.json import json_parse_int
from neutron_mojo.io.binary_reader import _u64_to_f64
from std.math import isfinite
from std.collections import Dict


# ===----------------------------------------------------------------------=== #
# Constants
# ===----------------------------------------------------------------------=== #

def NMF_MAGIC() -> Int:
    return 0x00464D4E  # "NMF\0" little-endian


def NMF_VERSION() -> Int:
    return 2


# ===----------------------------------------------------------------------=== #
# Params serialization (simple key=value text format)
# ===----------------------------------------------------------------------=== #

def _float_bits(value: Float64) -> Int:
    from std.memory import alloc
    var p = alloc[Float64](1)
    p.unsafe_store(value)
    var bits = Int(p.unsafe_bitcast[Int64]().unsafe_load())
    p.free()
    return bits


def serialize_params(p: ModelParams) raises -> String:
    """NMF v2 preserves all inference configuration, using exact float bits."""
    p.validate()
    var s = String("")
    s += "num_layers=" + String(p.num_layers) + "\n"
    s += "vocab_size=" + String(p.vocab_size) + "\n"
    s += "hidden_dim=" + String(p.hidden_dim) + "\n"
    s += "num_q_heads=" + String(p.num_q_heads) + "\n"
    s += "num_kv_heads=" + String(p.num_kv_heads) + "\n"
    s += "head_dim=" + String(p.head_dim) + "\n"
    s += "ffn_dim=" + String(p.ffn_dim) + "\n"
    s += "max_seq_len=" + String(p.max_seq_len) + "\n"
    s += "arch=" + p.arch.kind.name() + "\n"
    s += "rope_theta_bits=" + String(_float_bits(p.rope_theta)) + "\n"
    s += "use_sliding_window=" + String(Int(p.arch.use_sliding_window)) + "\n"
    s += "window_size=" + String(p.arch.window_size) + "\n"
    s += "partial_rotary_factor_bits=" + String(_float_bits(Float64(p.arch.partial_rotary_factor))) + "\n"
    s += "use_gelu=" + String(Int(p.arch.use_gelu)) + "\n"
    s += "use_pre_norm_bias=" + String(Int(p.arch.use_pre_norm_bias)) + "\n"
    s += "rope_scaling_bits=" + String(_float_bits(Float64(p.arch.rope_scaling))) + "\n"
    s += "norm_eps_bits=" + String(_float_bits(Float64(p.arch.norm_eps))) + "\n"
    return s^


def _params_fields(data: String) raises -> Dict[String,String]:
    var fields = Dict[String,String]()
    var start = 0
    for end in range(data.byte_length()):
        if ord(data[byte=end]) != 10:
            continue
        var line = String(data[byte=start:end])
        var eq = -1
        for i in range(line.byte_length()):
            if ord(line[byte=i]) == 61:
                eq = i
                break
        if eq <= 0:
            raise Error("Malformed NMF parameter line")
        var key = String(line[byte=:eq])
        if key in fields:
            raise Error("Duplicate NMF parameter")
        fields[key] = String(line[byte=eq+1:])
        start = end + 1
    if start != data.byte_length():
        raise Error("Unterminated NMF parameter line")
    return fields^


def _required_int(fields: Dict[String,String], key: String) raises -> Int:
    if key not in fields:
        raise Error("Missing NMF parameter: " + key)
    var value = fields[key]
    var parsed = json_parse_int(value, 0)
    if parsed.pos != value.byte_length():
        raise Error("Malformed NMF integer")
    return parsed.value


def _required_bool(fields: Dict[String,String], key: String) raises -> Bool:
    var value = _required_int(fields, key)
    if value != 0 and value != 1:
        raise Error("Invalid NMF boolean")
    return value == 1


def _required_float(fields: Dict[String,String], key: String) raises -> Float64:
    var value = _u64_to_f64(UInt64(_required_int(fields,key)))
    if not isfinite(value):
        raise Error("Nonfinite NMF configuration")
    return value


def deserialize_params(data: String, legacy_defaults: Bool = False) raises -> ModelParams:
    """Read v2 configuration; explicit legacy opt-in defaults omitted v1 fields."""
    var f = _params_fields(data)
    var p = ModelParams()
    p.num_layers = _required_int(f,"num_layers")
    p.vocab_size = _required_int(f,"vocab_size")
    p.hidden_dim = _required_int(f,"hidden_dim")
    p.num_q_heads = _required_int(f,"num_q_heads")
    p.num_kv_heads = _required_int(f,"num_kv_heads")
    p.head_dim = _required_int(f,"head_dim")
    p.ffn_dim = _required_int(f,"ffn_dim")
    p.max_seq_len = _required_int(f,"max_seq_len")
    if "arch" not in f:
        raise Error("Missing NMF architecture")
    var name = f["arch"]
    if name != "Llama" and name != "Mistral" and name != "Phi" and name != "Gemma" and name != "Qwen":
        raise Error("Unsupported NMF architecture")
    from neutron_mojo.model.architecture import arch_from_name
    p.arch = arch_from_name(name)
    if not legacy_defaults:
        p.rope_theta = _required_float(f,"rope_theta_bits")
        p.arch.use_sliding_window = _required_bool(f,"use_sliding_window")
        p.arch.window_size = _required_int(f,"window_size")
        p.arch.partial_rotary_factor = Float32(_required_float(f,"partial_rotary_factor_bits"))
        p.arch.use_gelu = _required_bool(f,"use_gelu")
        p.arch.use_pre_norm_bias = _required_bool(f,"use_pre_norm_bias")
        p.arch.rope_scaling = Float32(_required_float(f,"rope_scaling_bits"))
        p.arch.norm_eps = Float32(_required_float(f,"norm_eps_bits"))
    p.validate()
    if p.rope_theta <= 0 or p.arch.partial_rotary_factor < 0 or p.arch.partial_rotary_factor > 1 or p.arch.rope_scaling <= 0 or p.arch.norm_eps <= 0 or p.arch.window_size < 0:
        raise Error("Invalid NMF inference configuration")
    return p^


# ===----------------------------------------------------------------------=== #
# NMF File Write/Read (in-memory buffer)
# ===----------------------------------------------------------------------=== #

struct NMFBuffer(Movable):
    """In-memory NMF file representation.

    Used for save/load round-trip testing without filesystem access.
    """
    var data: List[UInt8]

    def __init__(out self):
        self.data = List[UInt8]()

    def __init__(out self, *, deinit move: Self):
        self.data = move.data^

    def _write_u32(mut self, val: Int) raises:
        if val < 0 or val > 0xFFFFFFFF:
            raise Error("NMF unsigned field overflow")
        self.data.append(UInt8(val & 0xFF))
        self.data.append(UInt8((val >> 8) & 0xFF))
        self.data.append(UInt8((val >> 16) & 0xFF))
        self.data.append(UInt8((val >> 24) & 0xFF))

    def _write_f32(mut self, val: Float32) raises:
        # Float32 -> UInt32 bits -> 4 LE bytes
        var p = alloc[Float32](1)
        p.store(val)
        var bits = Int(p.bitcast[UInt32]().load())
        p.free()
        self.data.append(UInt8(bits & 0xFF))
        self.data.append(UInt8((bits >> 8) & 0xFF))
        self.data.append(UInt8((bits >> 16) & 0xFF))
        self.data.append(UInt8((bits >> 24) & 0xFF))

    def _read_u32(self, offset: Int) raises -> Int:
        if offset < 0 or offset > len(self.data) or 4 > len(self.data) - offset:
            raise Error("Truncated NMF field")
        var b0 = Int(self.data[offset])
        var b1 = Int(self.data[offset + 1])
        var b2 = Int(self.data[offset + 2])
        var b3 = Int(self.data[offset + 3])
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)

    def _read_f32(self, offset: Int) raises -> Float32:
        if offset < 0 or offset > len(self.data) or 4 > len(self.data) - offset:
            raise Error("Truncated NMF float")
        # 4 LE bytes -> UInt32 bits -> Float32
        var b0 = Int(self.data[offset])
        var b1 = Int(self.data[offset + 1])
        var b2 = Int(self.data[offset + 2])
        var b3 = Int(self.data[offset + 3])
        var bits = UInt32(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))
        var p = alloc[UInt32](1)
        p.store(bits)
        var result = p.bitcast[Float32]().load()
        p.free()
        return result

    def size(self) -> Int:
        return len(self.data)


def save_model_to_buffer(model: Model) raises -> NMFBuffer:
    """Save FP32 model to an NMF buffer.

    Args:
        model: The model to save.

    Returns:
        NMFBuffer containing the serialized model.
    """
    model.validate_storage()
    var buf = NMFBuffer()

    # Header: magic + version
    buf._write_u32(NMF_MAGIC())
    buf._write_u32(NMF_VERSION())

    # Params section
    var params_str = serialize_params(model.params)
    buf._write_u32(params_str.byte_length())
    for i in range(params_str.byte_length()):
        buf.data.append(UInt8(ord(params_str[byte=i])))

    # Weight sections: layer_weights, embed, final_norm, lm_head
    # layer_weights is 1D — get(i) is flat index
    var lw_size = model.layer_weights.numel()
    buf._write_u32(lw_size)
    for i in range(lw_size):
        buf._write_f32(model.layer_weights.get(i))

    # embed is 2D Shape(vocab_size, hidden_dim) — get(i) reads row i, NOT flat i
    var vocab = model.params.vocab_size
    var hidden = model.params.hidden_dim
    var embed_size = model.embed.numel()
    buf._write_u32(embed_size)
    for row in range(vocab):
        for col in range(hidden):
            buf._write_f32(model.embed.get(row, col))

    # final_norm is 1D — get(i) is flat index
    var norm_size = model.final_norm.numel()
    buf._write_u32(norm_size)
    for i in range(norm_size):
        buf._write_f32(model.final_norm.get(i))

    # lm_head is 2D Shape(vocab_size, hidden_dim) — get(i) reads row i, NOT flat i
    var lm_size = model.lm_head.numel()
    buf._write_u32(lm_size)
    for row in range(vocab):
        for col in range(hidden):
            buf._write_f32(model.lm_head.get(row, col))

    return buf^


def load_model_from_buffer(buf: NMFBuffer, allow_legacy_defaults: Bool = False) raises -> Model:
    """Load FP32 model from an NMF buffer.

    Args:
        buf: The NMF buffer to read.

    Returns:
        Loaded Model.
    """
    # Verify header
    var magic = buf._read_u32(0)
    if magic != NMF_MAGIC():
        raise Error("Invalid NMF magic number")

    var version = buf._read_u32(4)
    if version != NMF_VERSION() and not (version == 1 and allow_legacy_defaults):
        raise Error("Unsupported NMF version")

    # Read params
    var params_len = buf._read_u32(8)
    if params_len > len(buf.data) - 12:
        raise Error("Truncated NMF parameters")
    var params_bytes = List[UInt8]()
    for i in range(params_len):
        params_bytes.append(buf.data[12+i])
    var params_str = String(from_utf8=params_bytes)
    var params = deserialize_params(params_str, legacy_defaults=version == 1)

    var offset = 12 + params_len
    var layer_count = Shape(params.num_layers, params.layer_weight_count()).numel()
    var matrix_count = Shape(params.vocab_size, params.hidden_dim).numel()
    var counts = [layer_count, matrix_count, params.hidden_dim, matrix_count]
    var check_offset = offset
    for section in range(4):
        var count = buf._read_u32(check_offset)
        check_offset += 4
        if count != counts[section] or count > (len(buf.data) - check_offset) // 4:
            raise Error("NMF section shape or byte count mismatch")
        check_offset += count * 4
    if check_offset != len(buf.data):
        raise Error("Trailing NMF data")

    # Allocate only after every declared section has passed exact-byte checks.
    var model = Model(params)
    # Read layer_weights
    var lw_size = buf._read_u32(offset)
    offset += 4
    for i in range(lw_size):
        model.layer_weights.set(i, buf._read_f32(offset))
        offset += 4

    # Read embed
    var embed_size = buf._read_u32(offset)
    offset += 4
    for i in range(embed_size):
        model.embed.set(i, buf._read_f32(offset))
        offset += 4

    # Read final_norm
    var norm_size = buf._read_u32(offset)
    offset += 4
    for i in range(norm_size):
        model.final_norm.set(i, buf._read_f32(offset))
        offset += 4

    # Read lm_head
    var lm_size = buf._read_u32(offset)
    offset += 4
    for i in range(lm_size):
        model.lm_head.set(i, buf._read_f32(offset))
        offset += 4

    return model^
