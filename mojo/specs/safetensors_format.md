# SafeTensors Format Specification

Pre-1.0 reference document. Based on the [SafeTensors specification](https://github.com/huggingface/safetensors) by Hugging Face.

SafeTensors is a simple, safe binary format for storing tensors. It is designed to prevent arbitrary code execution (unlike Python pickle), enable zero-copy memory-mapped loading, and provide fast random access to individual tensors.

---

## File Layout

```
+============================+
| header_size (8 bytes)      |   little-endian uint64
+----------------------------+
| JSON header (header_size)  |   UTF-8 encoded JSON object
+----------------------------+
| tensor data (binary)       |   raw tensor bytes, contiguous
+============================+
```

**Total file size** = 8 + `header_size` + total tensor data size

The format has exactly three sections with no padding, no magic bytes, and no alignment requirements beyond the header.

---

## Header Size

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `header_size` | `uint64_t` | 8 bytes | Little-endian byte count of the JSON header |

The header size is capped at a reasonable limit to prevent denial-of-service attacks. Implementations typically enforce a maximum of 100 MB for the header.

---

## JSON Header

The JSON header is a single JSON object mapping tensor names to tensor metadata. It is encoded as UTF-8 and may be padded with trailing spaces (0x20) to achieve alignment if desired.

### Structure

```json
{
  "__metadata__": {
    "format": "pt",
    "description": "Example model"
  },
  "weight_1": {
    "dtype": "F32",
    "shape": [768, 3072],
    "data_offsets": [0, 9437184]
  },
  "weight_2": {
    "dtype": "F16",
    "shape": [3072, 768],
    "data_offsets": [9437184, 14155776]
  }
}
```

### Tensor Entry Fields

| Field | Type | Description |
|-------|------|-------------|
| `dtype` | string | Data type identifier (see dtype table) |
| `shape` | array of int | Dimension sizes in row-major order |
| `data_offsets` | `[start, end]` | Byte range `[start, end)` within the data section |

### data_offsets Semantics

- `data_offsets` is a two-element array `[start, end]` of unsigned integers.
- Offsets are **relative to the start of the binary data section** (i.e., byte position `8 + header_size` in the file).
- The byte range is half-open: bytes `[start, end)` contain the tensor data.
- Tensor byte count: `end - start` must equal `product(shape) * dtype_size_in_bytes`.
- Tensors must not overlap and should cover the data section contiguously.

### Absolute file offset

```
absolute_start = 8 + header_size + data_offsets[0]
absolute_end   = 8 + header_size + data_offsets[1]
```

---

## __metadata__ Key

The JSON header may contain a special `"__metadata__"` key whose value is a flat object of string-to-string key-value pairs. This stores arbitrary metadata (framework version, training config, etc.) without interfering with tensor entries.

```json
{
  "__metadata__": {
    "format": "pt",
    "source": "huggingface/transformers"
  }
}
```

- All values in `__metadata__` must be strings.
- The `__metadata__` key is optional.
- Tensor names must not be `"__metadata__"`.

---

## Supported Data Types

| Dtype String | Description | Size (bytes) | Notes |
|-------------|-------------|--------------|-------|
| `BOOL` | Boolean | 1 | 0 = false, non-zero = true |
| `U8` | Unsigned 8-bit integer | 1 | |
| `I8` | Signed 8-bit integer | 1 | |
| `U16` | Unsigned 16-bit integer | 2 | |
| `I16` | Signed 16-bit integer | 2 | |
| `U32` | Unsigned 32-bit integer | 4 | |
| `I32` | Signed 32-bit integer | 4 | |
| `U64` | Unsigned 64-bit integer | 8 | |
| `I64` | Signed 64-bit integer | 8 | |
| `F16` | IEEE 754 half-precision float | 2 | 1 sign + 5 exp + 10 mantissa |
| `BF16` | Brain floating point 16 | 2 | 1 sign + 8 exp + 7 mantissa |
| `F32` | IEEE 754 single-precision float | 4 | |
| `F64` | IEEE 754 double-precision float | 8 | |
| `F8_E5M2` | 8-bit float (E5M2) | 1 | OFP8 format |
| `F8_E4M3` | 8-bit float (E4M3) | 1 | OFP8 format |

### Dtype to NumPy / PyTorch Mapping

| SafeTensors | NumPy | PyTorch |
|-------------|-------|---------|
| `BOOL` | `np.bool_` | `torch.bool` |
| `U8` | `np.uint8` | `torch.uint8` |
| `I8` | `np.int8` | `torch.int8` |
| `I16` | `np.int16` | `torch.int16` |
| `I32` | `np.int32` | `torch.int32` |
| `I64` | `np.int64` | `torch.int64` |
| `F16` | `np.float16` | `torch.float16` |
| `BF16` | N/A | `torch.bfloat16` |
| `F32` | `np.float32` | `torch.float32` |
| `F64` | `np.float64` | `torch.float64` |

---

## Tensor Storage Order

- Tensors are stored in **C order (row-major)**.
- For a tensor with shape `[d0, d1, ..., dn]`, the last dimension varies fastest.
- The strides are implicit and always contiguous: `stride[i] = product(shape[i+1:])`.
- No stride metadata is stored; all tensors are dense and contiguous.

---

## Validation Rules

Implementations must enforce the following:

1. **No overlapping tensors**: The `[start, end)` ranges must not overlap.
2. **Contiguous coverage**: Tensor data should be packed contiguously (no gaps).
3. **Consistent sizes**: `end - start` must equal `product(shape) * sizeof(dtype)` for each tensor.
4. **Sorted offsets**: Tensors are typically ordered by ascending `data_offsets[0]`.
5. **No duplicate names**: Each tensor name must be unique.
6. **Header size limit**: The JSON header must not exceed the implementation-defined maximum.
7. **No executable code**: The format intentionally contains no mechanism for code execution.

---

## Security Properties

SafeTensors was designed specifically to address the security problems of `pickle`-based formats:

| Property | Pickle / `.pt` | SafeTensors |
|----------|----------------|-------------|
| Arbitrary code execution | Yes | **No** |
| File size verification | No | **Yes** (`header_size + 8 + data_size`) |
| Zero-copy mmap loading | No | **Yes** |
| Random tensor access | No | **Yes** (via `data_offsets`) |
| Cross-framework support | Limited | **Yes** (Rust, Python, JS, C++) |

---

## Memory-Mapped Loading

The format is designed for efficient mmap-based loading:

```
1. Read 8 bytes -> header_size
2. Read header_size bytes -> parse JSON header
3. mmap the remaining file (or specific tensor ranges)
4. Each tensor is a view into the mmap: &data[start..end]
```

No deserialization or copying is required for the tensor data. The JSON header parse is the only allocation needed.

---

## Sharded Files

For models too large for a single file, SafeTensors supports sharding:

- Files are named `model-00001-of-00003.safetensors`, etc.
- An `model.safetensors.index.json` file maps tensor names to shard filenames:

```json
{
  "metadata": {
    "total_size": 14000000000
  },
  "weight_map": {
    "layer.0.weight": "model-00001-of-00003.safetensors",
    "layer.1.weight": "model-00001-of-00003.safetensors",
    "layer.2.weight": "model-00002-of-00003.safetensors"
  }
}
```

---

## Example: Reading a SafeTensors File (Pseudocode)

```python
import struct
import json
import mmap

def read_safetensors(path):
    with open(path, "rb") as f:
        # Read header size
        header_size = struct.unpack("<Q", f.read(8))[0]

        # Parse JSON header
        header_bytes = f.read(header_size)
        header = json.loads(header_bytes)

        # Extract metadata
        metadata = header.pop("__metadata__", {})

        # Data section starts at offset 8 + header_size
        data_offset = 8 + header_size

        # Memory-map the file
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        tensors = {}
        for name, info in header.items():
            start = data_offset + info["data_offsets"][0]
            end   = data_offset + info["data_offsets"][1]
            dtype = info["dtype"]
            shape = info["shape"]
            tensors[name] = {
                "data": mm[start:end],
                "dtype": dtype,
                "shape": shape,
            }

    return tensors, metadata
```

---

## File Size Formula

For a SafeTensors file containing `N` tensors:

```
file_size = 8 + header_size + sum(product(shape_i) * sizeof(dtype_i) for i in 0..N)
```

This relationship can be verified on load as a corruption check.
