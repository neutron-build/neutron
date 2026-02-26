# DLPack C Struct Definitions

Pre-1.0 reference document. Transcribed from [`dlpack.h`](https://github.com/dmlc/dlpack/blob/main/include/dlpack/dlpack.h).

DLPack is the open in-memory tensor interchange protocol. It defines a minimal set of C structs that allow frameworks (PyTorch, TensorFlow, JAX, NumPy, etc.) to share tensor data without copying.

**Current version**: 1.0 (ABI-stable since v1.0)

---

## DLDataTypeCode

```c
typedef enum {
  kDLInt      = 0,   // signed integer
  kDLUInt     = 1,   // unsigned integer
  kDLFloat    = 2,   // IEEE 754 floating point
  kDLOpaqueHandle = 3, // opaque handle (not used for computation)
  kDLBfloat   = 4,   // bfloat16 (Brain Floating Point)
  kDLComplex  = 5,   // complex number (real, imag interleaved)
  kDLBool     = 6,   // boolean
} DLDataTypeCode;
```

| Value | Name | Description |
|-------|------|-------------|
| 0 | `kDLInt` | Signed integer (int8, int16, int32, int64) |
| 1 | `kDLUInt` | Unsigned integer (uint8, uint16, uint32, uint64) |
| 2 | `kDLFloat` | IEEE 754 float (float16, float32, float64) |
| 3 | `kDLOpaqueHandle` | Opaque handle type, not used for numeric computation |
| 4 | `kDLBfloat` | Brain floating point (bfloat16) |
| 5 | `kDLComplex` | Complex number; `bits` gives total size (real + imag) |
| 6 | `kDLBool` | Boolean; `bits = 8`, one bool per byte |

---

## DLDeviceType

```c
typedef enum {
  kDLCPU          = 1,
  kDLCUDA         = 2,
  kDLCUDAHost     = 3,
  kDLOpenCL       = 4,
  kDLVulkan       = 7,
  kDLMetal        = 8,
  kDLVPI          = 9,
  kDLROCM         = 10,
  kDLROCMHost     = 11,
  kDLExtDev       = 12,
  kDLCUDAManaged  = 13,
  kDLOneAPI       = 14,
  kDLWebGPU       = 15,
  kDLHexagon      = 16,
  kDLMAIA         = 17,
} DLDeviceType;
```

| Value | Name | Description |
|-------|------|-------------|
| 1 | `kDLCPU` | System main memory (CPU/host) |
| 2 | `kDLCUDA` | NVIDIA CUDA GPU device memory |
| 3 | `kDLCUDAHost` | CUDA pinned host memory (page-locked) |
| 4 | `kDLOpenCL` | OpenCL device memory |
| 7 | `kDLVulkan` | Vulkan device memory |
| 8 | `kDLMetal` | Apple Metal device memory |
| 9 | `kDLVPI` | Verilog simulator memory |
| 10 | `kDLROCM` | AMD ROCm GPU device memory |
| 11 | `kDLROCMHost` | ROCm pinned host memory |
| 12 | `kDLExtDev` | Reserved for extension devices |
| 13 | `kDLCUDAManaged` | CUDA unified/managed memory |
| 14 | `kDLOneAPI` | Intel oneAPI device memory (SYCL) |
| 15 | `kDLWebGPU` | WebGPU device memory |
| 16 | `kDLHexagon` | Qualcomm Hexagon DSP |
| 17 | `kDLMAIA` | Microsoft MAIA accelerator |

---

## DLDataType

```c
typedef struct {
  uint8_t  code;   // DLDataTypeCode enum value
  uint8_t  bits;   // number of bits per element (e.g. 32 for float32)
  uint16_t lanes;  // number of lanes in a SIMD vector (1 for scalar)
} DLDataType;
```

**Size**: 4 bytes total, naturally aligned.

| Field | Type | Offset | Size | Description |
|-------|------|--------|------|-------------|
| `code` | `uint8_t` | 0 | 1 | One of `DLDataTypeCode` |
| `bits` | `uint8_t` | 1 | 1 | Bit-width of a single element |
| `lanes` | `uint16_t` | 2 | 2 | SIMD vector width; 1 = scalar |

**Common configurations**:

| Type | code | bits | lanes |
|------|------|------|-------|
| `float32` | 2 (`kDLFloat`) | 32 | 1 |
| `float16` | 2 (`kDLFloat`) | 16 | 1 |
| `bfloat16` | 4 (`kDLBfloat`) | 16 | 1 |
| `int8` | 0 (`kDLInt`) | 8 | 1 |
| `uint8` | 1 (`kDLUInt`) | 8 | 1 |
| `int32` | 0 (`kDLInt`) | 32 | 1 |
| `int64` | 0 (`kDLInt`) | 64 | 1 |
| `bool` | 6 (`kDLBool`) | 8 | 1 |
| `complex64` | 5 (`kDLComplex`) | 64 | 1 |
| `complex128` | 5 (`kDLComplex`) | 128 | 1 |
| `float32x4` | 2 (`kDLFloat`) | 32 | 4 |

---

## DLDevice

```c
typedef struct {
  DLDeviceType device_type;  // enum (int32_t)
  int32_t      device_id;    // device ordinal (0 for first GPU, etc.)
} DLDevice;
```

**Size**: 8 bytes total (4 + 4), 4-byte aligned.

| Field | Type | Offset | Size | Description |
|-------|------|--------|------|-------------|
| `device_type` | `int32_t` | 0 | 4 | One of `DLDeviceType` |
| `device_id` | `int32_t` | 4 | 4 | Device ordinal (0-indexed) |

---

## DLTensor

```c
typedef struct {
  void*       data;         // pointer to raw data buffer
  DLDevice    device;       // device where data resides
  int32_t     ndim;         // number of dimensions
  DLDataType  dtype;        // element data type
  int64_t*    shape;        // shape array of length ndim
  int64_t*    strides;      // stride array (NULL = compact C-contiguous)
  uint64_t    byte_offset;  // byte offset from data pointer to first element
} DLTensor;
```

**Size (64-bit)**:

| Field | Type | Offset | Size | Description |
|-------|------|--------|------|-------------|
| `data` | `void*` | 0 | 8 | Base pointer to allocated buffer |
| `device` | `DLDevice` | 8 | 8 | Target device (type + id) |
| `ndim` | `int32_t` | 16 | 4 | Number of dimensions |
| `dtype` | `DLDataType` | 20 | 4 | Element type (code, bits, lanes) |
| `shape` | `int64_t*` | 24 | 8 | Pointer to shape array (`int64_t[ndim]`) |
| `strides` | `int64_t*` | 32 | 8 | Pointer to strides array; NULL = C-contiguous |
| `byte_offset` | `uint64_t` | 40 | 8 | Byte offset into `data` for first element |

**Total**: 48 bytes on 64-bit platforms, 8-byte aligned.

**Stride semantics**: Strides are in units of **elements**, not bytes. A NULL strides pointer indicates compact row-major (C-contiguous) layout. The actual byte address of element `[i0, i1, ..., i_{n-1}]` is:

```
data + byte_offset + (i0*strides[0] + i1*strides[1] + ... + i_{n-1}*strides[n-1]) * (dtype.bits * dtype.lanes / 8)
```

---

## DLManagedTensor (Legacy / v0.x)

```c
typedef struct DLManagedTensor {
  DLTensor  dl_tensor;     // the tensor data
  void*     manager_ctx;   // opaque context for the producing framework
  void      (*deleter)(struct DLManagedTensor* self);  // destructor callback
} DLManagedTensor;
```

**Size (64-bit)**: 48 + 8 + 8 = **64 bytes**, 8-byte aligned.

| Field | Type | Offset | Size | Description |
|-------|------|--------|------|-------------|
| `dl_tensor` | `DLTensor` | 0 | 48 | Inline tensor struct |
| `manager_ctx` | `void*` | 48 | 8 | Producer-owned opaque pointer |
| `deleter` | `void(*)(DLManagedTensor*)` | 56 | 8 | Called to release the tensor |

**Lifecycle**: The consumer must call `deleter(self)` exactly once when done. The consumer must not modify `dl_tensor.data` or free it directly.

> **Note**: `DLManagedTensor` is deprecated as of DLPack v1.0. New code should use `DLManagedTensorVersioned`.

---

## DLManagedTensorVersioned (v1.0+)

```c
typedef struct DLManagedTensorVersioned {
  DLPackVersion version;     // protocol version
  void*         manager_ctx; // opaque context for the producing framework
  void          (*deleter)(struct DLManagedTensorVersioned* self);
  uint64_t      flags;       // bitmask flags
  DLTensor      dl_tensor;   // the tensor data
} DLManagedTensorVersioned;
```

**Size (64-bit)**: 8 + 8 + 8 + 8 + 48 = **80 bytes**, 8-byte aligned.

| Field | Type | Offset | Size | Description |
|-------|------|--------|------|-------------|
| `version` | `DLPackVersion` | 0 | 8 | Major and minor version |
| `manager_ctx` | `void*` | 8 | 8 | Producer-owned opaque pointer |
| `deleter` | `void(*)(...)` | 16 | 8 | Destructor callback |
| `flags` | `uint64_t` | 24 | 8 | Bitmask flags (see below) |
| `dl_tensor` | `DLTensor` | 32 | 48 | Inline tensor struct |

**Flags bitmask**:

| Bit | Name | Description |
|-----|------|-------------|
| 0 (`1 << 0`) | `DLPACK_FLAG_BITMASK_READ_ONLY` | Tensor data is read-only |
| 1 (`1 << 1`) | `DLPACK_FLAG_BITMASK_IS_COPIED` | Data was copied by the producer |

---

## DLPackVersion

```c
typedef struct {
  uint32_t major;  // major version (ABI-breaking changes)
  uint32_t minor;  // minor version (backward-compatible additions)
} DLPackVersion;
```

**Size**: 8 bytes total (4 + 4), 4-byte aligned.

| Field | Type | Offset | Size | Description |
|-------|------|--------|------|-------------|
| `major` | `uint32_t` | 0 | 4 | Major version (currently 1) |
| `minor` | `uint32_t` | 4 | 4 | Minor version (currently 0) |

---

## Python DLPack Protocol (v1.0)

The DLPack v1.0 protocol defines two dunder methods on tensor objects:

### `__dlpack__(*, stream=None) -> PyCapsule`

Returns a `PyCapsule` wrapping a `DLManagedTensorVersioned*`. The capsule name must be `"dltensor"` (unconsumed) or `"used_dltensor"` (consumed).

- `stream` (optional): An integer representing the compute stream on which the data is available. Used for cross-device synchronization.
  - `None` = no synchronization needed (CPU, or producer decides)
  - `-1` = the tensor must be made safe to use on any stream
  - `>=0` = specific stream ordinal

### `__dlpack_device__() -> tuple[int, int]`

Returns a tuple `(device_type, device_id)` corresponding to `DLDevice` fields. This allows the consumer to check device compatibility before calling `__dlpack__()`.

### Capsule lifecycle

1. Producer creates `DLManagedTensorVersioned`, wraps it in a `PyCapsule` named `"dltensor"`.
2. Consumer calls `PyCapsule_GetPointer`, reads the tensor, renames capsule to `"used_dltensor"`.
3. When the capsule is garbage-collected, if it is still named `"dltensor"`, the destructor calls `deleter(self)`.
4. If it was renamed to `"used_dltensor"`, the consumer is responsible for calling `deleter(self)`.

---

## Alignment Summary

| Struct | Size (64-bit) | Alignment | Notes |
|--------|---------------|-----------|-------|
| `DLDataType` | 4 bytes | 2-byte | Packed `{u8, u8, u16}` |
| `DLDevice` | 8 bytes | 4-byte | Two `int32_t` fields |
| `DLPackVersion` | 8 bytes | 4-byte | Two `uint32_t` fields |
| `DLTensor` | 48 bytes | 8-byte | Contains pointers |
| `DLManagedTensor` | 64 bytes | 8-byte | Legacy, deprecated |
| `DLManagedTensorVersioned` | 80 bytes | 8-byte | v1.0 preferred struct |

All sizes assume a 64-bit platform (8-byte pointers). On 32-bit platforms, pointer fields are 4 bytes and struct sizes shrink accordingly.
