# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Export / Serialization
# ===----------------------------------------------------------------------=== #

"""Save and load models in Neutron Model File (.nmf) format.

Format:
    Header: magic(4 bytes "NMF\0") + version(4 bytes u32) + params_len(4 bytes u32)
    Params: JSON-encoded ModelParams string
    Weights: raw float32 data (layer_weights, embed, final_norm, lm_head)

For quantized models, the format includes quantized int8 data and scale data
in addition to FP32 norm weights.
"""

from memory import UnsafePointer, alloc
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams


# ===----------------------------------------------------------------------=== #
# Constants
# ===----------------------------------------------------------------------=== #

fn NMF_MAGIC() -> Int:
    return 0x00464D4E  # "NMF\0" little-endian


fn NMF_VERSION() -> Int:
    return 1


# ===----------------------------------------------------------------------=== #
# Params serialization (simple key=value text format)
# ===----------------------------------------------------------------------=== #

fn serialize_params(p: ModelParams) -> String:
    """Serialize ModelParams to a simple text format."""
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
    return s^


fn _parse_int_field(data: String, key: String, default: Int) -> Int:
    """Parse an integer field from serialized params."""
    var search = key + "="
    for i in range(len(data) - len(search)):
        var found = True
        for j in range(len(search)):
            if ord(data[byte=i + j]) != ord(search[byte=j]):
                found = False
                break
        if found:
            var start = i + len(search)
            var end_idx = start
            while end_idx < len(data) and ord(data[byte=end_idx]) != ord('\n') and ord(data[byte=end_idx]) != ord('\r'):
                end_idx += 1
            var result = 0
            for k in range(start, end_idx):
                var c = Int(ord(data[byte=k]))
                if c >= Int(ord('0')) and c <= Int(ord('9')):
                    result = result * 10 + c - Int(ord('0'))
            return result
    return default


fn _parse_string_field(data: String, key: String, default: String) -> String:
    """Parse a string field from serialized params."""
    var search = key + "="
    for i in range(len(data) - len(search)):
        var found = True
        for j in range(len(search)):
            if ord(data[byte=i + j]) != ord(search[byte=j]):
                found = False
                break
        if found:
            var start = i + len(search)
            var end_idx = start
            while end_idx < len(data) and ord(data[byte=end_idx]) != ord('\n') and ord(data[byte=end_idx]) != ord('\r'):
                end_idx += 1
            var result = String("")
            for k in range(start, end_idx):
                result += chr(Int(ord(data[byte=k])))
            return result^
    return default


fn deserialize_params(data: String) -> ModelParams:
    """Deserialize ModelParams from text format."""
    var p = ModelParams()
    p.num_layers = _parse_int_field(data, "num_layers", 1)
    p.vocab_size = _parse_int_field(data, "vocab_size", 32000)
    p.hidden_dim = _parse_int_field(data, "hidden_dim", 4096)
    p.num_q_heads = _parse_int_field(data, "num_q_heads", 32)
    p.num_kv_heads = _parse_int_field(data, "num_kv_heads", 8)
    p.head_dim = _parse_int_field(data, "head_dim", 128)
    p.ffn_dim = _parse_int_field(data, "ffn_dim", 14336)
    p.max_seq_len = _parse_int_field(data, "max_seq_len", 2048)

    var arch_name = _parse_string_field(data, "arch", "Llama")
    from neutron_mojo.model.architecture import arch_from_name
    p.arch = arch_from_name(arch_name)

    return p^


# ===----------------------------------------------------------------------=== #
# NMF File Write/Read (in-memory buffer)
# ===----------------------------------------------------------------------=== #

struct NMFBuffer(Movable):
    """In-memory NMF file representation.

    Used for save/load round-trip testing without filesystem access.
    """
    var data: List[UInt8]

    fn __init__(out self):
        self.data = List[UInt8]()

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data^

    fn _write_u32(mut self, val: Int):
        self.data.append(UInt8(val & 0xFF))
        self.data.append(UInt8((val >> 8) & 0xFF))
        self.data.append(UInt8((val >> 16) & 0xFF))
        self.data.append(UInt8((val >> 24) & 0xFF))

    fn _write_f32(mut self, val: Float32):
        # Float32 -> UInt32 bits -> 4 LE bytes
        var p = alloc[Float32](1)
        p.store(val)
        var bits = Int(p.bitcast[UInt32]().load())
        p.free()
        self.data.append(UInt8(bits & 0xFF))
        self.data.append(UInt8((bits >> 8) & 0xFF))
        self.data.append(UInt8((bits >> 16) & 0xFF))
        self.data.append(UInt8((bits >> 24) & 0xFF))

    fn _read_u32(self, offset: Int) -> Int:
        var b0 = Int(self.data[offset])
        var b1 = Int(self.data[offset + 1])
        var b2 = Int(self.data[offset + 2])
        var b3 = Int(self.data[offset + 3])
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)

    fn _read_f32(self, offset: Int) -> Float32:
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

    fn size(self) -> Int:
        return len(self.data)


fn save_model_to_buffer(model: Model) -> NMFBuffer:
    """Save FP32 model to an NMF buffer.

    Args:
        model: The model to save.

    Returns:
        NMFBuffer containing the serialized model.
    """
    var buf = NMFBuffer()

    # Header: magic + version
    buf._write_u32(NMF_MAGIC())
    buf._write_u32(NMF_VERSION())

    # Params section
    var params_str = serialize_params(model.params)
    buf._write_u32(len(params_str))
    for i in range(len(params_str)):
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


fn load_model_from_buffer(buf: NMFBuffer) raises -> Model:
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
    if version != NMF_VERSION():
        raise Error("Unsupported NMF version")

    # Read params
    var params_len = buf._read_u32(8)
    var params_str = String("")
    for i in range(params_len):
        params_str += chr(Int(buf.data[12 + i]))
    var params = deserialize_params(params_str)

    var model = Model(params)
    var offset = 12 + params_len

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
