# ===----------------------------------------------------------------------=== #
# Neutron Mojo — I/O Package
# ===----------------------------------------------------------------------=== #

"""I/O utilities for loading model files."""

from .binary_reader import (
    BinaryReader,
    _fp16_to_fp32,
    mmap_reader,
)

from .safetensors import (
    TensorInfo,
    SafeTensorsFile,
    SafeTensorsIndex,
    parse_dtype_string,
    dtype_to_safetensors,
    dtype_element_size,
    load_safetensors_index,
    load_safetensors_index_from_string,
    build_safetensors_from_parts,
    _shard_filename,
)

from .gguf import (
    GGUF_MAGIC,
    GGUF_DEFAULT_ALIGNMENT,
    GGUFTensorType,
    GGUFTensorInfo,
    GGUFFile,
    gguf_type_to_dtype,
    dtype_to_gguf_type,
    calculate_tensor_size,
    _align_offset,
    parse_gguf_file,
    parse_gguf_from_buffer,
    gguf_to_model_config,
    detect_arch_from_gguf,
    build_test_gguf,
    GGUF_F32,
    GGUF_F16,
    GGUF_Q4_0,
    GGUF_Q4_1,
    GGUF_Q8_0,
    GGUF_Q4_K,
    _write_u32_le,
    _write_u64_le,
    _write_string_gguf,
    _write_f32_le,
)

from .json import (
    StringParseResult,
    IntParseResult,
    IntArrayParseResult,
    json_skip_whitespace,
    json_parse_string,
    json_parse_int,
    json_parse_int_array,
    parse_safetensors_header,
    parse_weight_map,
    parse_config_json,
)

from .model_export import (
    NMF_MAGIC,
    NMF_VERSION,
    NMFBuffer,
    serialize_params,
    deserialize_params,
    save_model_to_buffer,
    load_model_from_buffer,
)
