# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Unified Weight Loading Interface
# ===----------------------------------------------------------------------=== #

"""Unified weight loading for SafeTensors and GGUF model files.

Provides a common interface regardless of the underlying file format,
abstracting away format-specific details.
"""

from collections import Dict
from neutron_mojo.io.safetensors import TensorInfo, SafeTensorsFile, parse_dtype_string
from neutron_mojo.io.gguf import (
    GGUFTensorType,
    GGUFTensorInfo,
    GGUFFile,
    GGUF_F32,
    GGUF_F16,
    GGUF_Q4_0,
    GGUF_Q8_0,
    gguf_type_to_dtype,
    calculate_tensor_size,
)
from neutron_mojo.model.config import ModelConfig


# ===----------------------------------------------------------------------=== #
# File Format
# ===----------------------------------------------------------------------=== #

struct FileFormat(Copyable):
    """Model file format."""
    var _value: Int

    @implicit
    fn __init__(out self, value: Int):
        self._value = value

    fn __copyinit__(out self, existing: Self):
        self._value = existing._value

    fn __eq__(self, other: FileFormat) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: FileFormat) -> Bool:
        return self._value != other._value


fn FMT_SAFETENSORS() -> FileFormat:
    return FileFormat(0)

fn FMT_GGUF() -> FileFormat:
    return FileFormat(1)

fn FMT_UNKNOWN() -> FileFormat:
    return FileFormat(99)


fn detect_format(file_path: String) -> FileFormat:
    """Detect file format from extension.

    Args:
        file_path: Path to model file.

    Returns:
        Detected file format.
    """
    if file_path.endswith(".safetensors"):
        return FMT_SAFETENSORS()
    elif file_path.endswith(".gguf"):
        return FMT_GGUF()
    else:
        return FMT_UNKNOWN()


# ===----------------------------------------------------------------------=== #
# Weight Descriptor
# ===----------------------------------------------------------------------=== #

struct WeightDescriptor(Copyable):
    """Unified descriptor for a model weight tensor.

    Abstracts over SafeTensors and GGUF tensor metadata.
    """
    var name: String
    var dtype: DType
    var shape: List[Int]
    var size_bytes: Int
    var file_offset: Int  # Absolute offset in file
    var is_quantized: Bool
    var quant_type: String  # "none", "q4_0", "q8_0", "nf4", etc.

    fn __init__(out self):
        self.name = String("")
        self.dtype = DType.float32
        self.shape = List[Int]()
        self.size_bytes = 0
        self.file_offset = 0
        self.is_quantized = False
        self.quant_type = String("none")

    fn __copyinit__(out self, existing: Self):
        self.name = existing.name
        self.dtype = existing.dtype
        self.shape = existing.shape.copy()
        self.size_bytes = existing.size_bytes
        self.file_offset = existing.file_offset
        self.is_quantized = existing.is_quantized
        self.quant_type = existing.quant_type

    fn numel(self) -> Int:
        """Total number of elements."""
        var total = 1
        for i in range(len(self.shape)):
            total *= self.shape[i]
        return total

    fn ndim(self) -> Int:
        """Number of dimensions."""
        return len(self.shape)


# ===----------------------------------------------------------------------=== #
# Weight Index
# ===----------------------------------------------------------------------=== #

struct WeightIndex(Movable):
    """Index of all weight tensors in a model file.

    Provides a unified view over the file's tensor inventory.
    """
    var format: FileFormat
    var file_path: String
    var weights: Dict[String, WeightDescriptor]
    var weight_names: List[String]  # Ordered list of weight names
    var total_size_bytes: Int

    fn __init__(out self):
        self.format = FMT_UNKNOWN()
        self.file_path = String("")
        self.weights = Dict[String, WeightDescriptor]()
        self.weight_names = List[String]()
        self.total_size_bytes = 0

    fn __moveinit__(out self, deinit other: Self):
        self.format = other.format.copy()
        self.file_path = other.file_path^
        self.weights = other.weights^
        self.weight_names = other.weight_names^
        self.total_size_bytes = other.total_size_bytes

    fn add_weight(mut self, desc: WeightDescriptor):
        """Add a weight descriptor to the index.

        Args:
            desc: Weight descriptor.
        """
        self.weight_names.append(desc.name)
        self.total_size_bytes += desc.size_bytes
        self.weights[desc.name] = desc.copy()

    fn has_weight(self, name: String) -> Bool:
        """Check if a weight exists.

        Args:
            name: Weight name.

        Returns:
            True if weight exists.
        """
        return name in self.weights

    fn get_weight(self, name: String) raises -> WeightDescriptor:
        """Get weight descriptor by name.

        Args:
            name: Weight name.

        Returns:
            Weight descriptor.
        """
        if name not in self.weights:
            raise Error("Weight not found: " + name)
        return self.weights[name].copy()

    fn num_weights(self) -> Int:
        """Get total number of weights."""
        return len(self.weight_names)

    fn total_size_mb(self) -> Float64:
        """Get total size in megabytes."""
        return Float64(self.total_size_bytes) / (1024.0 * 1024.0)


# ===----------------------------------------------------------------------=== #
# Weight Index Builder — from SafeTensors
# ===----------------------------------------------------------------------=== #

fn build_index_from_safetensors(st: SafeTensorsFile) -> WeightIndex:
    """Build a WeightIndex from a SafeTensorsFile.

    Args:
        st: Parsed SafeTensors file.

    Returns:
        WeightIndex with all tensor metadata.
    """
    var index = WeightIndex()
    index.format = FMT_SAFETENSORS()

    # Iterate over registered tensors
    # NOTE: In a full implementation, we'd iterate over all tensors in the file.
    # For now, this works with manually registered tensors.
    return index^


fn register_safetensors_weight(
    mut index: WeightIndex,
    name: String,
    info: TensorInfo,
    data_offset: Int,
):
    """Register a SafeTensors tensor as a weight in the index.

    Args:
        index: Weight index to add to.
        name: Tensor name.
        info: SafeTensors tensor info.
        data_offset: Base data offset in file.
    """
    var desc = WeightDescriptor()
    desc.name = name
    desc.dtype = parse_dtype_string(info.dtype)
    desc.shape = info.shape.copy()
    desc.size_bytes = info.size_bytes()
    desc.file_offset = data_offset + info.data_offset_start
    desc.is_quantized = False
    desc.quant_type = String("none")
    index.add_weight(desc)


# ===----------------------------------------------------------------------=== #
# Weight Index Builder — from GGUF
# ===----------------------------------------------------------------------=== #

fn register_gguf_weight(
    mut index: WeightIndex,
    name: String,
    info: GGUFTensorInfo,
    data_offset: Int,
):
    """Register a GGUF tensor as a weight in the index.

    Args:
        index: Weight index to add to.
        name: Tensor name.
        info: GGUF tensor info.
        data_offset: Base data offset in file.
    """
    var desc = WeightDescriptor()
    desc.name = name
    desc.dtype = gguf_type_to_dtype(info.tensor_type)
    desc.shape = info.shape.copy()
    desc.size_bytes = calculate_tensor_size(info.shape, info.tensor_type)
    desc.file_offset = data_offset + info.offset

    # Detect quantization
    if info.tensor_type._value >= 2:
        desc.is_quantized = True
        if info.tensor_type._value == 2:
            desc.quant_type = String("q4_0")
        elif info.tensor_type._value == 3:
            desc.quant_type = String("q4_1")
        elif info.tensor_type._value == 8:
            desc.quant_type = String("q8_0")
        elif info.tensor_type._value == 12:
            desc.quant_type = String("q4_k")
        else:
            desc.quant_type = String("quantized")
    else:
        desc.is_quantized = False
        desc.quant_type = String("none")

    index.add_weight(desc)


# ===----------------------------------------------------------------------=== #
# Weight Validation
# ===----------------------------------------------------------------------=== #

fn validate_weights_for_model(
    index: WeightIndex, config: ModelConfig
) raises -> Bool:
    """Validate that a weight index has the expected weights for a model config.

    Checks for essential weight names (embedding, layers, norms).

    Args:
        index: Weight index to validate.
        config: Model configuration.

    Returns:
        True if all essential weights are present.
    """
    # Check for embedding
    if not index.has_weight("model.embed_tokens.weight"):
        raise Error("Missing embedding weight")

    # Check for final norm
    if not index.has_weight("model.norm.weight"):
        raise Error("Missing final norm weight")

    # Check first layer weights
    var has_first_layer = (
        index.has_weight("model.layers.0.self_attn.q_proj.weight")
        or index.has_weight("model.layers.0.self_attn.qkv_proj.weight")
    )
    if not has_first_layer:
        raise Error("Missing first layer attention weights")

    return True
