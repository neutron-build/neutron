# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Weight Reader (GGUF File -> Model)
# ===----------------------------------------------------------------------=== #

"""Functions to read tensor data from GGUF/SafeTensors files and populate Model.

Main entry points:
  GGUF: load_gguf_model(path) -> Model
  SafeTensors: load_safetensors_model(path, config) -> Model
  SafeTensors sharded: load_safetensors_sharded(index_path, config) -> Model

Also: load_gguf_quantized(path, block_size) -> QuantizedModel
Also: load_gguf_quantized_direct(path, block_size) -> QuantizedModel (no Q8->F32->Q8 roundtrip)

Supports F32, F16, Q8_0, and Q4_0 tensor types — quantized tensors are
dequantized to F32 on load. Direct Q8 loading preserves quantized data.
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.io.binary_reader import BinaryReader, _fp16_to_fp32, mmap_reader
from neutron_mojo.io.gguf import (
    GGUFFile,
    GGUFTensorInfo,
    GGUF_F32,
    GGUF_F16,
    GGUF_Q8_0,
    GGUF_Q4_0,
    parse_gguf_file,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    detect_arch_from_gguf,
)
from neutron_mojo.model.config import ModelConfig
from neutron_mojo.model.populate import model_from_config, load_named_weight, normalize_weight_name
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model, _num_blocks


# ===----------------------------------------------------------------------=== #
# Tensor Reading — F32 / F16
# ===----------------------------------------------------------------------=== #

fn read_tensor_f32(
    mut reader: BinaryReader, offset: Int, numel: Int
) raises -> Tensor[DType.float32]:
    """Read F32 tensor data from binary reader.

    Args:
        reader: BinaryReader with the file loaded.
        offset: Absolute byte offset to seek to.
        numel: Number of float32 elements.

    Returns:
        Tensor containing the data.
    """
    reader.seek(offset)
    return reader.read_f32_array(numel)


fn read_tensor_f16_as_f32(
    mut reader: BinaryReader, offset: Int, numel: Int
) raises -> Tensor[DType.float32]:
    """Read FP16 tensor data, converting to F32.

    Args:
        reader: BinaryReader with the file loaded.
        offset: Absolute byte offset to seek to.
        numel: Number of FP16 elements.

    Returns:
        Tensor[float32] with converted values.
    """
    reader.seek(offset)
    return reader.read_f16_to_f32_array(numel)


# ===----------------------------------------------------------------------=== #
# Tensor Reading — Q8_0 / Q4_0 (dequant to F32)
# ===----------------------------------------------------------------------=== #

fn read_tensor_q8_0_as_f32(
    mut reader: BinaryReader, offset: Int, numel: Int
) raises -> Tensor[DType.float32]:
    """Read Q8_0 tensor data and dequantize to F32.

    Q8_0 format: blocks of 32 elements, each block = 34 bytes:
        - 2 bytes: FP16 scale
        - 32 bytes: INT8 quantized values

    Dequant: float_val = int8_val * scale

    Args:
        reader: BinaryReader with the file loaded.
        offset: Absolute byte offset to seek to.
        numel: Total number of elements.

    Returns:
        Tensor[float32] with dequantized values.
    """
    reader.seek(offset)
    var result = Tensor[DType.float32](Shape(numel))

    var num_blocks = (numel + 31) // 32
    var out_idx = 0

    for _ in range(num_blocks):
        # Read 2-byte FP16 scale
        var b0 = Int(reader.read_u8())
        var b1 = Int(reader.read_u8())
        var scale_bits = b0 | (b1 << 8)
        var scale = _fp16_to_fp32(scale_bits)

        # Read 32 INT8 values
        var elems_in_block = 32
        if out_idx + 32 > numel:
            elems_in_block = numel - out_idx

        for _ in range(elems_in_block):
            var raw = Int(reader.read_u8())
            # Convert unsigned byte to signed: if > 127, subtract 256
            var signed_val: Int
            if raw > 127:
                signed_val = raw - 256
            else:
                signed_val = raw
            result.set(out_idx, Float32(signed_val) * scale)
            out_idx += 1

        # Skip remaining bytes in block if we're in a partial block
        for _ in range(32 - elems_in_block):
            _ = reader.read_u8()

    return result^


fn read_tensor_q4_0_as_f32(
    mut reader: BinaryReader, offset: Int, numel: Int
) raises -> Tensor[DType.float32]:
    """Read Q4_0 tensor data and dequantize to F32.

    Q4_0 format: blocks of 32 elements, each block = 18 bytes:
        - 2 bytes: FP16 scale
        - 16 bytes: packed nibbles (2 values per byte)

    Low nibble = first value, high nibble = second value.
    Values are unsigned 0..15, centered: float_val = (nibble - 8) * scale

    Args:
        reader: BinaryReader with the file loaded.
        offset: Absolute byte offset to seek to.
        numel: Total number of elements.

    Returns:
        Tensor[float32] with dequantized values.
    """
    reader.seek(offset)
    var result = Tensor[DType.float32](Shape(numel))

    var num_blocks = (numel + 31) // 32
    var out_idx = 0

    for _ in range(num_blocks):
        # Read 2-byte FP16 scale
        var b0 = Int(reader.read_u8())
        var b1 = Int(reader.read_u8())
        var scale_bits = b0 | (b1 << 8)
        var scale = _fp16_to_fp32(scale_bits)

        # Read 16 packed nibble bytes (each holds 2 values)
        var elems_in_block = 32
        if out_idx + 32 > numel:
            elems_in_block = numel - out_idx

        for i in range(16):
            var byte_val = Int(reader.read_u8())
            var lo = byte_val & 0x0F
            var hi = (byte_val >> 4) & 0x0F

            var idx_lo = i * 2
            var idx_hi = i * 2 + 1

            if idx_lo < elems_in_block:
                result.set(out_idx + idx_lo, Float32(lo - 8) * scale)
            if idx_hi < elems_in_block:
                result.set(out_idx + idx_hi, Float32(hi - 8) * scale)

        out_idx += elems_in_block

    return result^


# ===----------------------------------------------------------------------=== #
# GGUF -> Model
# ===----------------------------------------------------------------------=== #

fn load_gguf_model(path: String) raises -> Model:
    """Load a GGUF model file into a Model struct.

    Steps:
        1. Parse GGUF header/metadata/tensor info
        2. Extract ModelConfig from metadata
        3. Create Model from config
        4. Read each tensor's data and load into model

    Args:
        path: Path to .gguf file.

    Returns:
        Populated Model ready for inference.
    """
    # 1. Parse GGUF
    var gguf = parse_gguf_file(path)

    # 2. Extract config + architecture
    var config = gguf_to_model_config(gguf)
    var arch = detect_arch_from_gguf(gguf)

    # 3. Create model (model_from_config also sets arch from model_type)
    var model = model_from_config(config)
    # Override with GGUF-detected arch (may have sliding window from metadata)
    model.params.arch = arch.copy()

    # 4. Read and load each tensor
    var reader = BinaryReader(path)

    # Load weights by known name conventions
    _load_known_weights(model, gguf, reader)

    return model^


fn _load_known_weights(
    mut model: Model, gguf: GGUFFile, mut reader: BinaryReader
) raises:
    """Load known weight tensors from GGUF into model.

    Tries both GGUF and HuggingFace naming conventions for each weight.

    Args:
        model: Model to populate.
        gguf: Parsed GGUF with tensor info.
        reader: BinaryReader for reading data.
    """
    var p = model.params.copy()

    # Embedding
    _try_load_tensor_multi(model, gguf, reader, "token_embd.weight", "model.embed_tokens.weight")

    # Final norm
    _try_load_tensor_multi(model, gguf, reader, "output_norm.weight", "model.norm.weight")

    # LM head
    _try_load_tensor_multi(model, gguf, reader, "output.weight", "lm_head.weight")

    # Per-layer weights
    for layer in range(p.num_layers):
        var ls = String(layer)
        var gp = "blk." + ls + "."
        var hp = "model.layers." + ls + "."

        _try_load_tensor_multi(model, gguf, reader, gp + "attn_norm.weight", hp + "input_layernorm.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "attn_q.weight", hp + "self_attn.q_proj.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "attn_k.weight", hp + "self_attn.k_proj.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "attn_v.weight", hp + "self_attn.v_proj.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "attn_output.weight", hp + "self_attn.o_proj.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "ffn_norm.weight", hp + "post_attention_layernorm.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "ffn_gate.weight", hp + "mlp.gate_proj.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "ffn_up.weight", hp + "mlp.up_proj.weight")
        _try_load_tensor_multi(model, gguf, reader, gp + "ffn_down.weight", hp + "mlp.down_proj.weight")


fn _try_load_tensor_multi(
    mut model: Model,
    gguf: GGUFFile,
    mut reader: BinaryReader,
    gguf_name: String,
    hf_name: String,
) raises:
    """Try to load a tensor by GGUF name first, then HF name.

    Routes the loaded data through load_named_weight using the HF name.

    Args:
        model: Model to populate.
        gguf: GGUF file with tensor info.
        reader: BinaryReader for data.
        gguf_name: GGUF-convention tensor name (tried first).
        hf_name: HuggingFace-convention tensor name (fallback).
    """
    var found_name: String
    if gguf.has_tensor(gguf_name):
        found_name = gguf_name
    elif gguf.has_tensor(hf_name):
        found_name = hf_name
    else:
        return

    var info = gguf.get_tensor_info(found_name)
    var numel = info.numel()
    var abs_offset = gguf.data_offset + info.offset

    var data: Tensor[DType.float32]
    if info.tensor_type == GGUF_F32():
        data = read_tensor_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_F16():
        data = read_tensor_f16_as_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_Q8_0():
        data = read_tensor_q8_0_as_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_Q4_0():
        data = read_tensor_q4_0_as_f32(reader, abs_offset, numel)
    else:
        raise Error("Unsupported tensor type for " + found_name)

    # Always load using HF name convention
    load_named_weight(model, hf_name, data, numel)


fn load_gguf_model_from_buffer(var buf: List[UInt8]) raises -> Model:
    """Load a GGUF model from an in-memory buffer (for testing).

    Args:
        buf: Complete GGUF binary.

    Returns:
        Populated Model.
    """
    var buf_copy = buf.copy()
    var gguf = parse_gguf_from_buffer(buf^)
    var config = gguf_to_model_config(gguf)
    var arch = detect_arch_from_gguf(gguf)
    var model = model_from_config(config)
    model.params.arch = arch.copy()

    var reader = BinaryReader(buf_copy^)
    _load_known_weights(model, gguf, reader)

    return model^


fn load_gguf_quantized(path: String, block_size: Int = 32) raises -> QuantizedModel:
    """Load GGUF as FP32 model, then quantize to Q8.

    Args:
        path: Path to .gguf file.
        block_size: Quantization block size.

    Returns:
        QuantizedModel with Q8-quantized projections.
    """
    var model = load_gguf_model(path)
    return quantize_from_model(model, block_size)


# ===----------------------------------------------------------------------=== #
# Direct Q8 Loading — QuantizedTensorData
# ===----------------------------------------------------------------------=== #

struct QuantizedTensorData(Movable):
    """Holds Q8 quantized data: INT8 values as Float32 + per-block scales."""
    var data: Tensor[DType.float32]
    var scales: Tensor[DType.float32]

    fn __init__(out self, var data: Tensor[DType.float32], var scales: Tensor[DType.float32]):
        self.data = data^
        self.scales = scales^

    fn __moveinit__(out self, deinit other: Self):
        self.data = other.data^
        self.scales = other.scales^


# ===----------------------------------------------------------------------=== #
# Direct Q8 Reading
# ===----------------------------------------------------------------------=== #

fn read_tensor_q8_0_as_quantized(
    mut reader: BinaryReader, offset: Int, numel: Int, block_size: Int
) raises -> QuantizedTensorData:
    """Read Q8_0 tensor data directly into quantized format.

    Instead of dequantizing to F32, stores INT8 values as Float32
    (matching QuantizedModel.layer_weights format) and FP16 scales
    converted to Float32 (matching QuantizedModel.layer_scales format).

    Q8_0 format: blocks of 32 elements, each block = 34 bytes:
        - 2 bytes: FP16 scale
        - 32 bytes: INT8 quantized values

    Args:
        reader: BinaryReader with the file loaded.
        offset: Absolute byte offset to seek to.
        numel: Total number of elements.
        block_size: Block size for scale indexing (must be 32 for Q8_0).

    Returns:
        QuantizedTensorData with data (INT8 as Float32) and scales (FP16->F32).
    """
    reader.seek(offset)

    var num_blocks = (numel + 31) // 32
    var q_data = Tensor[DType.float32](Shape(numel))
    var q_scales = Tensor[DType.float32](Shape(num_blocks))
    var out_idx = 0

    for blk in range(num_blocks):
        # Read 2-byte FP16 scale
        var b0 = Int(reader.read_u8())
        var b1 = Int(reader.read_u8())
        var scale_bits = b0 | (b1 << 8)
        var scale = _fp16_to_fp32(scale_bits)
        q_scales.set(blk, scale)

        # Read 32 INT8 values (store as Float32 integers)
        var elems_in_block = 32
        if out_idx + 32 > numel:
            elems_in_block = numel - out_idx

        for _ in range(elems_in_block):
            var raw = Int(reader.read_u8())
            var signed_val: Int
            if raw > 127:
                signed_val = raw - 256
            else:
                signed_val = raw
            q_data.set(out_idx, Float32(signed_val))
            out_idx += 1

        # Skip padding in partial blocks
        for _ in range(32 - elems_in_block):
            _ = reader.read_u8()

    return QuantizedTensorData(q_data^, q_scales^)


# ===----------------------------------------------------------------------=== #
# Direct Q8 GGUF Loading
# ===----------------------------------------------------------------------=== #

fn _load_q8_projection(
    mut model: QuantizedModel,
    mut reader: BinaryReader,
    abs_offset: Int,
    numel: Int,
    weight_offset: Int,
    scale_offset: Int,
    out_features: Int,
    in_features: Int,
    block_size: Int,
) raises:
    """Load a Q8_0 projection directly into QuantizedModel storage.

    Reads Q8_0 blocks and writes INT8 values (as Float32) into
    model.layer_weights and scales into model.layer_scales.

    Args:
        model: QuantizedModel to populate.
        reader: BinaryReader for data.
        abs_offset: Absolute byte offset to the tensor data.
        numel: Number of elements in the tensor.
        weight_offset: Offset into model.layer_weights for this projection.
        scale_offset: Offset into model.layer_scales for this projection.
        out_features: Output dimension (rows).
        in_features: Input dimension (cols).
        block_size: Quantization block size.
    """
    var qtd = read_tensor_q8_0_as_quantized(reader, abs_offset, numel, block_size)

    # Copy INT8 values into layer_weights
    for i in range(numel):
        model.layer_weights.set(weight_offset + i, qtd.data.get(i))

    # Copy scales: need to map from flat block index to row-major block layout
    var num_blocks_per_row = _num_blocks(in_features, block_size)
    var total_blocks = out_features * num_blocks_per_row
    for i in range(total_blocks):
        model.layer_scales.set(scale_offset + i, qtd.scales.get(i))


fn _load_q8_known_weights(
    mut model: QuantizedModel, gguf: GGUFFile, mut reader: BinaryReader
) raises:
    """Load known weight tensors from GGUF into QuantizedModel.

    For Q8_0 projection weights, loads directly without dequant/requant.
    For F32/F16 weights (embed, norms, lm_head), loads as F32.

    Args:
        model: QuantizedModel to populate.
        gguf: Parsed GGUF with tensor info.
        reader: BinaryReader for reading data.
    """
    var p = model.params.copy()

    # Embedding (always F32/F16 — non-projection)
    _try_load_q8_tensor_f32(model, gguf, reader, "token_embd.weight", "model.embed_tokens.weight", "embed")

    # Final norm (always F32)
    _try_load_q8_tensor_f32(model, gguf, reader, "output_norm.weight", "model.norm.weight", "final_norm")

    # LM head (always F32/F16 — non-projection)
    _try_load_q8_tensor_f32(model, gguf, reader, "output.weight", "lm_head.weight", "lm_head")

    # Per-layer weights
    for layer in range(p.num_layers):
        var ls = String(layer)
        var gp = "blk." + ls + "."
        var hp = "model.layers." + ls + "."
        var off = model._layer_offsets(layer)
        var soff = model._layer_scale_offsets(layer)

        # Norms (always F32)
        _try_load_q8_tensor_norm(model, gguf, reader, gp + "attn_norm.weight", hp + "input_layernorm.weight", off.attn_norm)
        _try_load_q8_tensor_norm(model, gguf, reader, gp + "ffn_norm.weight", hp + "post_attention_layernorm.weight", off.ffn_norm)

        # Projection weights — direct Q8 if Q8_0, else dequant to F32 then quantize
        _try_load_q8_projection(model, gguf, reader, gp + "attn_q.weight", hp + "self_attn.q_proj.weight", off.wq, soff.wq, p.q_dim(), p.hidden_dim)
        _try_load_q8_projection(model, gguf, reader, gp + "attn_k.weight", hp + "self_attn.k_proj.weight", off.wk, soff.wk, p.kv_dim(), p.hidden_dim)
        _try_load_q8_projection(model, gguf, reader, gp + "attn_v.weight", hp + "self_attn.v_proj.weight", off.wv, soff.wv, p.kv_dim(), p.hidden_dim)
        _try_load_q8_projection(model, gguf, reader, gp + "attn_output.weight", hp + "self_attn.o_proj.weight", off.wo, soff.wo, p.hidden_dim, p.q_dim())
        _try_load_q8_projection(model, gguf, reader, gp + "ffn_gate.weight", hp + "mlp.gate_proj.weight", off.w_gate, soff.w_gate, p.ffn_dim, p.hidden_dim)
        _try_load_q8_projection(model, gguf, reader, gp + "ffn_up.weight", hp + "mlp.up_proj.weight", off.w_up, soff.w_up, p.ffn_dim, p.hidden_dim)
        _try_load_q8_projection(model, gguf, reader, gp + "ffn_down.weight", hp + "mlp.down_proj.weight", off.w_down, soff.w_down, p.hidden_dim, p.ffn_dim)


fn _try_load_q8_tensor_f32(
    mut model: QuantizedModel,
    gguf: GGUFFile,
    mut reader: BinaryReader,
    gguf_name: String,
    hf_name: String,
    target: String,
) raises:
    """Load a non-projection tensor (embed/norm/lm_head) as F32 into QuantizedModel.

    Args:
        model: QuantizedModel to populate.
        gguf: Parsed GGUF.
        reader: BinaryReader.
        gguf_name: GGUF tensor name.
        hf_name: HF tensor name.
        target: "embed", "final_norm", or "lm_head".
    """
    var found_name: String
    if gguf.has_tensor(gguf_name):
        found_name = gguf_name
    elif gguf.has_tensor(hf_name):
        found_name = hf_name
    else:
        return

    var info = gguf.get_tensor_info(found_name)
    var numel = info.numel()
    var abs_offset = gguf.data_offset + info.offset

    var data: Tensor[DType.float32]
    if info.tensor_type == GGUF_F32():
        data = read_tensor_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_F16():
        data = read_tensor_f16_as_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_Q8_0():
        data = read_tensor_q8_0_as_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_Q4_0():
        data = read_tensor_q4_0_as_f32(reader, abs_offset, numel)
    else:
        raise Error("Unsupported tensor type for " + found_name)

    if target == "embed":
        for i in range(numel):
            model.embed.set(i, data.get(i))
    elif target == "final_norm":
        for i in range(numel):
            model.final_norm.set(i, data.get(i))
    elif target == "lm_head":
        for i in range(numel):
            model.lm_head.set(i, data.get(i))


fn _try_load_q8_tensor_norm(
    mut model: QuantizedModel,
    gguf: GGUFFile,
    mut reader: BinaryReader,
    gguf_name: String,
    hf_name: String,
    weight_offset: Int,
) raises:
    """Load a norm tensor as F32 into QuantizedModel.layer_weights.

    Args:
        model: QuantizedModel to populate.
        gguf: Parsed GGUF.
        reader: BinaryReader.
        gguf_name: GGUF tensor name.
        hf_name: HF tensor name.
        weight_offset: Offset into model.layer_weights.
    """
    var found_name: String
    if gguf.has_tensor(gguf_name):
        found_name = gguf_name
    elif gguf.has_tensor(hf_name):
        found_name = hf_name
    else:
        return

    var info = gguf.get_tensor_info(found_name)
    var numel = info.numel()
    var abs_offset = gguf.data_offset + info.offset

    var data: Tensor[DType.float32]
    if info.tensor_type == GGUF_F32():
        data = read_tensor_f32(reader, abs_offset, numel)
    elif info.tensor_type == GGUF_F16():
        data = read_tensor_f16_as_f32(reader, abs_offset, numel)
    else:
        data = read_tensor_f32(reader, abs_offset, numel)

    for i in range(numel):
        model.layer_weights.set(weight_offset + i, data.get(i))


fn _try_load_q8_projection(
    mut model: QuantizedModel,
    gguf: GGUFFile,
    mut reader: BinaryReader,
    gguf_name: String,
    hf_name: String,
    weight_offset: Int,
    scale_offset: Int,
    out_features: Int,
    in_features: Int,
) raises:
    """Load a projection tensor into QuantizedModel.

    If Q8_0: loads directly (no dequant/requant roundtrip).
    If F32/F16: dequants to F32, then quantizes to Q8.

    Args:
        model: QuantizedModel to populate.
        gguf: Parsed GGUF.
        reader: BinaryReader.
        gguf_name: GGUF tensor name.
        hf_name: HF tensor name.
        weight_offset: Offset into model.layer_weights.
        scale_offset: Offset into model.layer_scales.
        out_features: Output dimension.
        in_features: Input dimension.
    """
    var found_name: String
    if gguf.has_tensor(gguf_name):
        found_name = gguf_name
    elif gguf.has_tensor(hf_name):
        found_name = hf_name
    else:
        return

    var info = gguf.get_tensor_info(found_name)
    var numel = info.numel()
    var abs_offset = gguf.data_offset + info.offset

    if info.tensor_type == GGUF_Q8_0():
        # Direct Q8 loading — no roundtrip!
        _load_q8_projection(
            model, reader, abs_offset, numel,
            weight_offset, scale_offset,
            out_features, in_features, model.block_size,
        )
    else:
        # F32/F16/Q4_0 — dequant to F32, then quantize to Q8
        var data: Tensor[DType.float32]
        if info.tensor_type == GGUF_F32():
            data = read_tensor_f32(reader, abs_offset, numel)
        elif info.tensor_type == GGUF_F16():
            data = read_tensor_f16_as_f32(reader, abs_offset, numel)
        elif info.tensor_type == GGUF_Q4_0():
            data = read_tensor_q4_0_as_f32(reader, abs_offset, numel)
        else:
            raise Error("Unsupported tensor type for " + found_name)

        # Quantize the F32 data into the model's Q8 storage
        from neutron_mojo.nn.q_model import _quantize_projection
        _quantize_projection(
            data, 0,
            model.layer_weights, weight_offset,
            model.layer_scales, scale_offset,
            out_features, in_features, model.block_size,
        )


fn load_gguf_quantized_direct(path: String, block_size: Int = 32) raises -> QuantizedModel:
    """Load GGUF directly into QuantizedModel without Q8->F32->Q8 roundtrip.

    For Q8_0 tensors, reads quantized data directly into QuantizedModel storage.
    For F32/F16 tensors (embed, norms, lm_head), loads as F32.

    Args:
        path: Path to .gguf file.
        block_size: Quantization block size.

    Returns:
        QuantizedModel with weights loaded directly.
    """
    var gguf = parse_gguf_file(path)
    var config = gguf_to_model_config(gguf)

    var p = ModelParams()
    p.num_layers = config.num_hidden_layers
    p.vocab_size = config.vocab_size
    p.hidden_dim = config.hidden_size
    p.num_q_heads = config.num_attention_heads
    p.num_kv_heads = config.num_key_value_heads
    p.head_dim = config.head_dim
    p.ffn_dim = config.intermediate_size
    p.max_seq_len = config.max_position_embeddings
    p.rope_theta = config.rope.theta

    var model = QuantizedModel(p, block_size)

    var reader = BinaryReader(path)
    _load_q8_known_weights(model, gguf, reader)

    return model^


fn load_gguf_quantized_direct_from_buffer(var buf: List[UInt8], block_size: Int = 32) raises -> QuantizedModel:
    """Load GGUF directly into QuantizedModel from an in-memory buffer.

    For Q8_0 tensors, reads quantized data directly into QuantizedModel storage.
    For F32/F16 tensors, loads as F32.

    Args:
        buf: Complete GGUF binary.
        block_size: Quantization block size.

    Returns:
        QuantizedModel with weights loaded directly.
    """
    var buf_copy = buf.copy()
    var gguf = parse_gguf_from_buffer(buf^)
    var config = gguf_to_model_config(gguf)

    var p = ModelParams()
    p.num_layers = config.num_hidden_layers
    p.vocab_size = config.vocab_size
    p.hidden_dim = config.hidden_size
    p.num_q_heads = config.num_attention_heads
    p.num_kv_heads = config.num_key_value_heads
    p.head_dim = config.head_dim
    p.ffn_dim = config.intermediate_size
    p.max_seq_len = config.max_position_embeddings
    p.rope_theta = config.rope.theta

    var model = QuantizedModel(p, block_size)

    var reader = BinaryReader(buf_copy^)
    _load_q8_known_weights(model, gguf, reader)

    return model^


# ===----------------------------------------------------------------------=== #
# SafeTensors Loading (Sprint 14)
# ===----------------------------------------------------------------------=== #

fn _read_safetensors_tensor(
    mut reader: BinaryReader,
    data_base_offset: Int,
    info_start: Int,
    info_end: Int,
    dtype: String,
) raises -> Tensor[DType.float32]:
    """Read a tensor from SafeTensors data section.

    Handles F32 and F16 dtypes. F16 is converted to F32 on load.

    Args:
        reader: BinaryReader positioned on the file.
        data_base_offset: Byte offset where tensor data section begins.
        info_start: Tensor data start offset (relative to data section).
        info_end: Tensor data end offset (relative to data section).
        dtype: SafeTensors dtype string ("F32", "F16", etc.).

    Returns:
        Tensor[float32] with the loaded values.
    """
    var abs_offset = data_base_offset + info_start
    var size_bytes = info_end - info_start

    if dtype == "F32":
        var numel = size_bytes // 4
        return read_tensor_f32(reader, abs_offset, numel)
    elif dtype == "F16":
        var numel = size_bytes // 2
        return read_tensor_f16_as_f32(reader, abs_offset, numel)
    elif dtype == "BF16":
        # BF16: upper 16 bits of F32, shift left by 16
        var numel = size_bytes // 2
        reader.seek(abs_offset)
        var result = Tensor[DType.float32](Shape(numel))
        for i in range(numel):
            var b0 = Int(reader.read_u8())
            var b1 = Int(reader.read_u8())
            var f32_bits = UInt32((b0 | (b1 << 8)) << 16)
            from neutron_mojo.io.binary_reader import _u32_to_f32
            result.set(i, _u32_to_f32(f32_bits))
        return result^
    else:
        raise Error("Unsupported SafeTensors dtype: " + dtype)


fn load_safetensors_model(
    path: String, config: ModelConfig
) raises -> Model:
    """Load a single SafeTensors file into a Model.

    Unlike GGUF, SafeTensors files don't contain model architecture info,
    so a ModelConfig must be provided (from config.json or predefined).

    Args:
        path: Path to .safetensors file.
        config: Model configuration (dimensions, layers, etc.).

    Returns:
        Populated Model ready for inference.
    """
    from neutron_mojo.io.safetensors import SafeTensorsFile

    var st = SafeTensorsFile()
    st.load(path)

    var model = model_from_config(config)
    var reader = BinaryReader(path)

    _load_safetensors_weights(model, st, reader)

    return model^


fn _load_safetensors_weights(
    mut model: Model,
    st: SafeTensorsFile,
    mut reader: BinaryReader,
) raises:
    """Load known weight tensors from a SafeTensorsFile into Model.

    Tries all standard HuggingFace weight names for a Llama-style architecture.

    Args:
        model: Model to populate.
        st: Parsed SafeTensors file with tensor metadata.
        reader: BinaryReader for reading tensor data.
    """
    from neutron_mojo.io.safetensors import SafeTensorsFile

    var p = model.params.copy()

    # Global tensors
    _try_load_st_tensor(model, st, reader, "model.embed_tokens.weight")
    _try_load_st_tensor(model, st, reader, "model.norm.weight")
    _try_load_st_tensor(model, st, reader, "lm_head.weight")

    # Per-layer tensors
    for layer in range(p.num_layers):
        var lp = "model.layers." + String(layer) + "."
        _try_load_st_tensor(model, st, reader, lp + "input_layernorm.weight")
        _try_load_st_tensor(model, st, reader, lp + "self_attn.q_proj.weight")
        _try_load_st_tensor(model, st, reader, lp + "self_attn.k_proj.weight")
        _try_load_st_tensor(model, st, reader, lp + "self_attn.v_proj.weight")
        _try_load_st_tensor(model, st, reader, lp + "self_attn.o_proj.weight")
        _try_load_st_tensor(model, st, reader, lp + "post_attention_layernorm.weight")
        _try_load_st_tensor(model, st, reader, lp + "mlp.gate_proj.weight")
        _try_load_st_tensor(model, st, reader, lp + "mlp.up_proj.weight")
        _try_load_st_tensor(model, st, reader, lp + "mlp.down_proj.weight")


fn _try_load_st_tensor(
    mut model: Model,
    st: SafeTensorsFile,
    mut reader: BinaryReader,
    name: String,
) raises:
    """Try to load a single tensor from SafeTensors into Model.

    Silently skips if tensor not found in the file.

    Args:
        model: Model to populate.
        st: Parsed SafeTensors file.
        reader: BinaryReader for data.
        name: HuggingFace tensor name.
    """
    if not st.has_tensor(name):
        return

    var info = st.get_tensor_info(name)
    var data = _read_safetensors_tensor(
        reader, st.data_offset, info.data_offset_start, info.data_offset_end, info.dtype
    )
    var numel = info.numel()
    load_named_weight(model, name, data, numel)


fn load_safetensors_sharded(
    index_path: String, config: ModelConfig
) raises -> Model:
    """Load a sharded SafeTensors model from an index file.

    Reads model.safetensors.index.json to determine which shard each
    tensor lives in, then loads tensors from the appropriate shards.

    Args:
        index_path: Path to model.safetensors.index.json.
        config: Model configuration (dimensions, layers, etc.).

    Returns:
        Populated Model ready for inference.
    """
    from neutron_mojo.io.safetensors import load_safetensors_index, SafeTensorsFile

    var index = load_safetensors_index(index_path)
    var model = model_from_config(config)

    var p = model.params.copy()

    # Build list of all weight names we want to load
    var names = List[String]()
    names.append("model.embed_tokens.weight")
    names.append("model.norm.weight")
    names.append("lm_head.weight")
    for layer in range(p.num_layers):
        var lp = "model.layers." + String(layer) + "."
        names.append(lp + "input_layernorm.weight")
        names.append(lp + "self_attn.q_proj.weight")
        names.append(lp + "self_attn.k_proj.weight")
        names.append(lp + "self_attn.v_proj.weight")
        names.append(lp + "self_attn.o_proj.weight")
        names.append(lp + "post_attention_layernorm.weight")
        names.append(lp + "mlp.gate_proj.weight")
        names.append(lp + "mlp.up_proj.weight")
        names.append(lp + "mlp.down_proj.weight")

    # Load each tensor from its shard
    # Cache: track last opened shard to avoid re-parsing
    var last_shard_name = String("")
    var last_st = SafeTensorsFile()
    var last_reader = BinaryReader(List[UInt8]())

    for i in range(len(names)):
        var name = names[i]
        if not index.has_tensor(name):
            continue

        var shard_name = index.get_shard(name)
        var shard_path = index.get_shard_path(name)

        # Open shard if different from last one
        if shard_name != last_shard_name:
            last_st = SafeTensorsFile()
            last_st.load(shard_path)
            last_reader = BinaryReader(shard_path)
            last_shard_name = shard_name

        _try_load_st_tensor(model, last_st, last_reader, name)

    return model^


fn load_safetensors_from_buffer(
    var buf: List[UInt8], config: ModelConfig
) raises -> Model:
    """Load a SafeTensors model from an in-memory buffer (for testing).

    Args:
        buf: Complete SafeTensors file bytes.
        config: Model configuration.

    Returns:
        Populated Model.
    """
    from neutron_mojo.io.safetensors import SafeTensorsFile
    from neutron_mojo.io.json import parse_safetensors_header

    var buf_copy = buf.copy()

    # Parse header manually from buffer
    var reader = BinaryReader(buf^)
    var header_size = reader.read_u64_le()

    # Read JSON header as string
    var json_str = String("")
    for _ in range(header_size):
        json_str += chr(Int(reader.read_u8()))

    var data_offset = 8 + header_size

    # Parse tensor metadata
    var tensors = parse_safetensors_header(json_str)

    # Create model and load weights
    var model = model_from_config(config)
    var data_reader = BinaryReader(buf_copy^)

    # Load each tensor by trying known weight names
    var p = model.params.copy()
    var all_names = List[String]()
    all_names.append("model.embed_tokens.weight")
    all_names.append("model.norm.weight")
    all_names.append("lm_head.weight")
    for layer in range(p.num_layers):
        var lp = "model.layers." + String(layer) + "."
        all_names.append(lp + "input_layernorm.weight")
        all_names.append(lp + "self_attn.q_proj.weight")
        all_names.append(lp + "self_attn.k_proj.weight")
        all_names.append(lp + "self_attn.v_proj.weight")
        all_names.append(lp + "self_attn.o_proj.weight")
        all_names.append(lp + "post_attention_layernorm.weight")
        all_names.append(lp + "mlp.gate_proj.weight")
        all_names.append(lp + "mlp.up_proj.weight")
        all_names.append(lp + "mlp.down_proj.weight")

    for i in range(len(all_names)):
        var name = all_names[i]
        if name not in tensors:
            continue
        var info = tensors[name].copy()
        var data = _read_safetensors_tensor(
            data_reader, data_offset, info.data_offset_start, info.data_offset_end, info.dtype
        )
        load_named_weight(model, name, data, info.numel())

    return model^


# ===----------------------------------------------------------------------=== #
# Memory-Mapped Loading (Sprint 13)
# ===----------------------------------------------------------------------=== #

fn load_gguf_model_mmap(path: String) raises -> Model:
    """Load a GGUF model file using memory-mapped I/O.

    Same as load_gguf_model() but uses mmap instead of slurping the
    entire file into memory. For large models, this reduces peak memory
    usage since only accessed pages are loaded by the OS.

    Args:
        path: Path to .gguf file.

    Returns:
        Populated Model ready for inference.
    """
    # Parse GGUF header via mmap
    var header_reader = mmap_reader(path)
    from neutron_mojo.io.gguf import _parse_gguf_from_reader
    var gguf = _parse_gguf_from_reader(header_reader)

    # Create model from config with auto-detected architecture
    var config = gguf_to_model_config(gguf)
    var arch = detect_arch_from_gguf(gguf)
    var model = model_from_config(config)
    model.params.arch = arch.copy()

    # Read tensor data via mmap
    var data_reader = mmap_reader(path)
    _load_known_weights(model, gguf, data_reader)

    return model^


fn load_gguf_quantized_mmap(path: String, block_size: Int = 32) raises -> QuantizedModel:
    """Load GGUF as FP32 model via mmap, then quantize to Q8.

    Args:
        path: Path to .gguf file.
        block_size: Quantization block size.

    Returns:
        QuantizedModel with Q8-quantized projections.
    """
    var model = load_gguf_model_mmap(path)
    return quantize_from_model(model, block_size)


fn load_gguf_quantized_direct_mmap(path: String, block_size: Int = 32) raises -> QuantizedModel:
    """Load GGUF directly into QuantizedModel via mmap, no Q8->F32->Q8 roundtrip.

    Same as load_gguf_quantized_direct() but using memory-mapped I/O.

    Args:
        path: Path to .gguf file.
        block_size: Quantization block size.

    Returns:
        QuantizedModel with weights loaded directly.
    """
    # Parse GGUF header via mmap
    var header_reader = mmap_reader(path)
    from neutron_mojo.io.gguf import _parse_gguf_from_reader
    var gguf = _parse_gguf_from_reader(header_reader)

    var config = gguf_to_model_config(gguf)

    var p = ModelParams()
    p.num_layers = config.num_hidden_layers
    p.vocab_size = config.vocab_size
    p.hidden_dim = config.hidden_size
    p.num_q_heads = config.num_attention_heads
    p.num_kv_heads = config.num_key_value_heads
    p.head_dim = config.head_dim
    p.ffn_dim = config.intermediate_size
    p.max_seq_len = config.max_position_embeddings
    p.rope_theta = config.rope.theta

    var model = QuantizedModel(p, block_size)

    var data_reader = mmap_reader(path)
    _load_q8_known_weights(model, gguf, data_reader)

    return model^
