# GGUF Binary Format Specification

Pre-1.0 reference document. Based on the [GGUF specification](https://github.com/ggerganov/ggml/blob/master/docs/gguf.md) used by llama.cpp and GGML-based inference engines.

GGUF (GGML Universal File) is a binary format for storing quantized large language model weights and metadata. It supersedes the earlier GGML, GGJT, and GGLA formats.

**Current version**: 3
**Byte order**: Little-endian (default)

---

## File Layout

```
+=====================+
|   File Header       |   magic + version + counts
+---------------------+
|   Metadata KV Pairs |   key-value metadata array
+---------------------+
|   Tensor Info Array  |   name, dims, type, offset per tensor
+---------------------+
|   Padding            |   0x00 bytes to alignment boundary
+=====================+
|   Tensor Data        |   raw weight data (aligned)
+=====================+
```

All tensor data begins at an offset aligned to `general.alignment` (default 32 bytes) from the start of the file.

---

## File Header

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `magic` | `uint8_t[4]` | 4 bytes | ASCII `"GGUF"` = `0x47 0x47 0x55 0x46` |
| `version` | `uint32_t` | 4 bytes | Format version (currently 3) |
| `tensor_count` | `uint64_t` | 8 bytes | Number of tensors in the file |
| `metadata_kv_count` | `uint64_t` | 8 bytes | Number of metadata key-value pairs |

**Total header**: 24 bytes (fixed portion), followed by variable-length metadata and tensor info.

### Version History

| Version | Changes |
|---------|---------|
| 1 | Initial GGUF format |
| 2 | Changed `tensor_count` and `metadata_kv_count` from `uint32_t` to `uint64_t` |
| 3 | Current version; stabilized quantization types |

---

## String Encoding

Strings in GGUF are **not** null-terminated. They are length-prefixed:

```c
struct gguf_string_t {
    uint64_t len;    // length in bytes (not including this field)
    char     data[]; // UTF-8 encoded, no null terminator
};
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `len` | `uint64_t` | 8 bytes | Byte length of the string |
| `data` | `uint8_t[len]` | `len` bytes | Raw UTF-8 string data |

---

## Metadata Key-Value Pairs

Each metadata entry consists of:

```
+------------------+
|  key (string)    |   gguf_string_t
+------------------+
|  value_type      |   uint32_t
+------------------+
|  value (data)    |   variable length, depends on value_type
+------------------+
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `key` | `gguf_string_t` | 8 + len | Metadata key (e.g., `"general.architecture"`) |
| `value_type` | `uint32_t` | 4 bytes | One of `gguf_metadata_value_type` |
| `value` | varies | varies | Value payload (format depends on type) |

---

## Metadata Value Types

```c
enum gguf_metadata_value_type : uint32_t {
    GGUF_METADATA_VALUE_TYPE_UINT8   = 0,
    GGUF_METADATA_VALUE_TYPE_INT8    = 1,
    GGUF_METADATA_VALUE_TYPE_UINT16  = 2,
    GGUF_METADATA_VALUE_TYPE_INT16   = 3,
    GGUF_METADATA_VALUE_TYPE_UINT32  = 4,
    GGUF_METADATA_VALUE_TYPE_INT32   = 5,
    GGUF_METADATA_VALUE_TYPE_FLOAT32 = 6,
    GGUF_METADATA_VALUE_TYPE_BOOL    = 7,
    GGUF_METADATA_VALUE_TYPE_STRING  = 8,
    GGUF_METADATA_VALUE_TYPE_ARRAY   = 9,
    GGUF_METADATA_VALUE_TYPE_UINT64  = 10,
    GGUF_METADATA_VALUE_TYPE_INT64   = 11,
    GGUF_METADATA_VALUE_TYPE_FLOAT64 = 12,
};
```

| Value | Name | Wire Size | Description |
|-------|------|-----------|-------------|
| 0 | `UINT8` | 1 byte | Unsigned 8-bit integer |
| 1 | `INT8` | 1 byte | Signed 8-bit integer |
| 2 | `UINT16` | 2 bytes | Unsigned 16-bit integer |
| 3 | `INT16` | 2 bytes | Signed 16-bit integer |
| 4 | `UINT32` | 4 bytes | Unsigned 32-bit integer |
| 5 | `INT32` | 4 bytes | Signed 32-bit integer |
| 6 | `FLOAT32` | 4 bytes | IEEE 754 32-bit float |
| 7 | `BOOL` | 1 byte | Boolean (0 = false, non-zero = true) |
| 8 | `STRING` | 8 + len | Length-prefixed UTF-8 string |
| 9 | `ARRAY` | varies | Typed array (see below) |
| 10 | `UINT64` | 8 bytes | Unsigned 64-bit integer |
| 11 | `INT64` | 8 bytes | Signed 64-bit integer |
| 12 | `FLOAT64` | 8 bytes | IEEE 754 64-bit float |

### Array Value Format

```
+------------------+
|  element_type    |   uint32_t (gguf_metadata_value_type)
+------------------+
|  count           |   uint64_t
+------------------+
|  elements[count] |   each element encoded per element_type
+------------------+
```

Arrays are homogeneously typed. Nested arrays are permitted (element_type = ARRAY).

---

## Tensor Info Array

After all metadata KV pairs, the file contains `tensor_count` tensor info entries:

```c
struct gguf_tensor_info {
    gguf_string_t name;          // tensor name (e.g., "blk.0.attn_q.weight")
    uint32_t      n_dimensions;  // number of dimensions
    uint64_t      dimensions[];  // array of n_dimensions dimension sizes
    uint32_t      type;          // ggml_type enum value
    uint64_t      offset;        // byte offset into tensor data section
};
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `name` | `gguf_string_t` | 8 + len | Tensor name string |
| `n_dimensions` | `uint32_t` | 4 bytes | Number of dimensions (1-4 typical) |
| `dimensions` | `uint64_t[n]` | 8*n bytes | Size of each dimension |
| `type` | `uint32_t` | 4 bytes | `ggml_type` quantization enum |
| `offset` | `uint64_t` | 8 bytes | Byte offset relative to start of tensor data section |

**Note**: The `offset` is relative to the beginning of the tensor data section, not the beginning of the file. The tensor data section begins at the first alignment boundary after all tensor info entries.

---

## Quantization Types (ggml_type)

| Value | Name | Description | Block Size (elements) | Bytes/Block |
|-------|------|-------------|-----------------------|-------------|
| 0 | `F32` | 32-bit float | 1 | 4 |
| 1 | `F16` | 16-bit float | 1 | 2 |
| 2 | `Q4_0` | 4-bit quantized (symmetric) | 32 | 18 |
| 3 | `Q4_1` | 4-bit quantized (asymmetric) | 32 | 20 |
| 6 | `Q5_0` | 5-bit quantized (symmetric) | 32 | 22 |
| 7 | `Q5_1` | 5-bit quantized (asymmetric) | 32 | 24 |
| 8 | `Q8_0` | 8-bit quantized (symmetric) | 32 | 34 |
| 9 | `Q8_1` | 8-bit quantized (asymmetric) | 32 | 36 |
| 10 | `Q2_K` | 2-bit K-quant | 256 | 84 |
| 11 | `Q3_K` | 3-bit K-quant | 256 | 110 |
| 12 | `Q4_K` | 4-bit K-quant | 256 | 144 |
| 13 | `Q5_K` | 5-bit K-quant | 256 | 176 |
| 14 | `Q6_K` | 6-bit K-quant | 256 | 210 |
| 15 | `Q8_K` | 8-bit K-quant | 256 | 292 |
| 28 | `BF16` | Brain float 16 | 1 | 2 |
| 30 | `F64` | 64-bit float | 1 | 8 |

---

## Quantization Block Structures

### Q8_0 (8-bit Symmetric Quantization)

Block size: 32 elements. Total: **34 bytes per block**.

```c
struct block_q8_0 {
    ggml_half d;       // float16 scale factor (2 bytes)
    int8_t    qs[32];  // quantized values (32 bytes)
};
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `d` | `float16` | 2 bytes | Scale factor: `d = max(abs(x[0..31])) / 127` |
| `qs` | `int8_t[32]` | 32 bytes | Quantized values in [-127, 127] |

**Dequantization**:
```
x[i] = qs[i] * d
```

**Bits per weight**: 8.5 (34 bytes / 32 elements * 8)

---

### Q4_K_M (4-bit K-Quant, Medium)

Super-block size: 256 elements (8 sub-blocks of 32 elements). Total: **144 bytes per super-block**.

```c
struct block_q4_K {
    ggml_half d;            // super-block scale (2 bytes)
    ggml_half dmin;         // super-block minimum (2 bytes)
    uint8_t   scales[12];   // packed 6-bit sub-block scales & mins (12 bytes)
    uint8_t   qs[128];      // packed 4-bit quantized values (128 bytes)
};
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `d` | `float16` | 2 bytes | Super-block scale multiplier |
| `dmin` | `float16` | 2 bytes | Super-block minimum multiplier |
| `scales` | `uint8_t[12]` | 12 bytes | 8 sub-block scales + 8 sub-block mins, 6 bits each, packed |
| `qs` | `uint8_t[128]` | 128 bytes | 256 values packed as 4-bit nibbles (2 per byte) |

**Scale packing** (12 bytes encode 8 scales + 8 minimums, each 6-bit):
- Bytes 0-3: lower 4 bits of scales[0..3] and mins[0..3]
- Bytes 4-7: lower 4 bits of scales[4..7] and mins[4..7]
- Bytes 8-11: upper 2 bits of scales[0..7] and mins[0..7]

**Dequantization** (for sub-block `j`, element `i` within sub-block):
```
sc = scales[j]         // 6-bit scale for sub-block j
m  = mins[j]           // 6-bit minimum for sub-block j
x[j*32 + i] = d * sc * qs[j*32 + i] - dmin * m
```

**Bits per weight**: 4.5 (144 bytes / 256 elements * 8)

---

### Q5_K_M (5-bit K-Quant, Medium)

Super-block size: 256 elements (8 sub-blocks of 32 elements). Total: **176 bytes per super-block**.

```c
struct block_q5_K {
    ggml_half d;            // super-block scale (2 bytes)
    ggml_half dmin;         // super-block minimum (2 bytes)
    uint8_t   scales[12];   // packed 6-bit sub-block scales & mins (12 bytes)
    uint8_t   qh[32];       // high bits of quantized values (32 bytes)
    uint8_t   qs[128];      // low 4 bits of quantized values (128 bytes)
};
```

| Field | Type | Size | Description |
|-------|------|------|-------------|
| `d` | `float16` | 2 bytes | Super-block scale multiplier |
| `dmin` | `float16` | 2 bytes | Super-block minimum multiplier |
| `scales` | `uint8_t[12]` | 12 bytes | Packed 6-bit sub-block scales & mins (same as Q4_K) |
| `qh` | `uint8_t[32]` | 32 bytes | 5th bit for each of the 256 values (bit-packed) |
| `qs` | `uint8_t[128]` | 128 bytes | Lower 4 bits of 256 values (nibble-packed) |

**Dequantization** (for sub-block `j`, element `i` within sub-block):
```
sc = scales[j]
m  = mins[j]
lo = (qs[(j*32 + i) / 2] >> (4 * ((j*32+i) % 2))) & 0xF   // low 4 bits
hi = (qh[(j*32 + i) / 8] >> ((j*32+i) % 8)) & 1            // 5th bit
q  = lo | (hi << 4)                                          // 5-bit value [0..31]
x[j*32 + i] = d * sc * q - dmin * m
```

**Bits per weight**: 5.5 (176 bytes / 256 elements * 8)

---

## Alignment Rules

1. **Default alignment**: 32 bytes (can be overridden by `general.alignment` metadata key).
2. **Tensor data section**: Starts at the first offset that is a multiple of `alignment` after the last tensor info entry.
3. **Individual tensor offsets**: Each tensor's `offset` within the data section must also be a multiple of `alignment`.
4. **Padding bytes**: All padding uses `0x00` bytes.

### Calculating tensor data start

```
tensor_data_start = ALIGN(end_of_tensor_info_section, alignment)

where ALIGN(x, a) = ((x + a - 1) / a) * a
```

### Calculating absolute tensor position

```
absolute_offset = tensor_data_start + tensor_info[i].offset
```

---

## Standard Metadata Keys

| Key | Type | Description |
|-----|------|-------------|
| `general.architecture` | STRING | Model architecture (e.g., `"llama"`, `"gpt2"`) |
| `general.name` | STRING | Human-readable model name |
| `general.file_type` | UINT32 | Predominant quantization type |
| `general.quantization_version` | UINT32 | Quantization format version |
| `general.alignment` | UINT32 | Tensor data alignment in bytes (default 32) |
| `{arch}.context_length` | UINT64 | Maximum context length |
| `{arch}.embedding_length` | UINT64 | Embedding / hidden dimension |
| `{arch}.block_count` | UINT64 | Number of transformer blocks |
| `{arch}.feed_forward_length` | UINT64 | FFN intermediate dimension |
| `{arch}.attention.head_count` | UINT64 | Number of attention heads |
| `{arch}.attention.head_count_kv` | UINT64 | Number of KV heads (for GQA) |
| `tokenizer.ggml.model` | STRING | Tokenizer type (e.g., `"llama"`, `"gpt2"`) |
| `tokenizer.ggml.tokens` | ARRAY[STRING] | Token vocabulary |
| `tokenizer.ggml.scores` | ARRAY[FLOAT32] | Token scores/priorities |
| `tokenizer.ggml.token_type` | ARRAY[INT32] | Token types (normal, control, etc.) |

---

## Example: Reading a GGUF File (Pseudocode)

```python
def read_gguf(path):
    f = open(path, "rb")

    # Header
    magic = f.read(4)                     # b"GGUF"
    version = read_uint32(f)              # 3
    tensor_count = read_uint64(f)         # e.g., 291
    metadata_kv_count = read_uint64(f)    # e.g., 24

    # Metadata
    metadata = {}
    for _ in range(metadata_kv_count):
        key = read_string(f)
        value_type = read_uint32(f)
        value = read_value(f, value_type)
        metadata[key] = value

    alignment = metadata.get("general.alignment", 32)

    # Tensor info
    tensors = []
    for _ in range(tensor_count):
        name = read_string(f)
        n_dims = read_uint32(f)
        dims = [read_uint64(f) for _ in range(n_dims)]
        qtype = read_uint32(f)
        offset = read_uint64(f)
        tensors.append((name, dims, qtype, offset))

    # Align to tensor data section
    data_start = align(f.tell(), alignment)

    # Read tensor data
    for name, dims, qtype, offset in tensors:
        f.seek(data_start + offset)
        data = f.read(tensor_byte_size(dims, qtype))
```
