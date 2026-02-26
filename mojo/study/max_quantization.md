# MAX Quantization: Architecture Study

MAX implements quantization across multiple files: `max/kernels/src/quantization/` for 4-bit symmetric quantization, `max/kernels/src/linalg/fp8_quantization.mojo` for FP8, and `max/kernels/src/linalg/fp4_quantization.mojo` for NVFP4/MXFP8 on SM100 (Blackwell).

## Q4 Symmetric Quantization (Q4sym)

The `Q4sym` struct in `per_channel_grouped_4bit.mojo` implements per-group symmetric 4-bit quantization:

```mojo
struct Q4sym[group_size: Int, float_dtype: DType = DType.float32]:
    var scale: StaticTuple[UInt8, 2]           # FP16 scale stored as 2 bytes
    var bits: StaticTuple[UInt8, group_size // 2]  # Packed uint4 values
```

**Group sizes**: 8, 16, or 32 elements share one scale factor (must be power of 2).

**Encoding scheme**: Two uint4 values packed per byte in a strided layout:
```
Elements: A, B, C, D, E, F, G, H
Storage:  eeeeaaaa | ffffbbbb | ggggcccc | hhhhdddd
```

The low 4 bits hold the first half of elements, high 4 bits hold the second half. This enables efficient SIMD unpacking.

### Quantization Flow

1. **Find scale**: `result_range = max(max_value, -min_value)`, then `scale = result_range / (2^(bits-1) - 1)`
2. **Quantize**: `round(data / scale)` cast to int8, then shifted to unsigned by adding 8 (the zero-point)
3. **Pack**: `lo_half | (hi_half << 4)` packs two values per byte
4. **Store scale**: Cast float scale to FP16, stored as raw bytes to avoid bitcast issues

### Dequantization Flow

1. **Unpack**: Extract low nibble `(byte & 0x0F)` and high nibble `(byte >> 4)`
2. **Recenter**: Subtract implicit zero-point of 8 to get signed values
3. **Scale**: `float_result = signed_value * fp16_scale`

## GGML-Compatible Formats (Q4_K, Q6_K)

MAX also implements GGML's quantization formats for compatibility with llama.cpp model files:

### Q4_K Format

```mojo
struct block_Q4_K:
    var base_scale: Float16    # Superblock scale
    var base_min: Float16      # Superblock minimum
    var q_scales_and_mins: InlineArray[UInt8, 12]  # 6-bit per-group scales/mins
    var q_bits: InlineArray[UInt8, 128]            # 256 uint4 values packed
```

Structure: 256-element superblock with 8 groups of 32. Each group has a 6-bit scale and 6-bit minimum, packed into 12 bytes using a clever bit-packing scheme.

Dequantization: `output = base_scale * q_scale * (nibble_value) - base_min * q_min`

### Q6_K Format

6-bit quantization with 16-element groups. Uses split storage: 4 low bits in `q_bits_lo`, 2 high bits in `q_bits_hi`, with int8 per-group scales and a FP16 superblock scale.

Dequant: `output = base_scale * q_scale * ((lo_4bit | (hi_2bit << 4)) - 32)`

## FP8 Quantization Patterns

The `fp8_quantization.mojo` file implements multiple FP8 strategies:

### Static Scaled FP8
Uses a predetermined scale factor: `fp8_value = input * scale`. Fastest path but requires calibration data.

### Dynamic Scaled FP8
Computes scale per token/group at runtime:

1. Find max absolute value within the group
2. Compute scale: `scale = max_value / fp8_max_representable`
3. Quantize: `fp8_value = input / scale`
4. Store both quantized values and scale factors

Supports granularities: per-tensor, per-token (rowwise), per-column (colwise), and block-wise.

### Block-Scaled FP8 Matmul

`matmul_dynamic_scaled_fp8` performs scaled matmul with separate A and B scales:

```
C = (A * A_scale) @ (B * B_scale)
```

The scale factors are applied during the accumulation, not as a pre-processing step. This avoids materializing the full-precision intermediate.

### AMD Compatibility

`convert_e4m3fn_to_e4m3fnuz` handles bit-pattern differences between NVIDIA's FP8 E4M3 and AMD's E4M3FNUZ format (different zero representation).

## NVFP4 Quantization (SM100/Blackwell)

The `fp4_quantization.mojo` file implements NVIDIA's FP4-E2M1 format, specific to B200 GPUs:

### Format Details
- **NVFP4**: Two FP4-E2M1 values packed per uint8
- **Scale factors**: FP8-E4M3 per group of 16 elements (NVFP4_SF_VECTOR_SIZE=16)
- **Scale factor layout**: Interleaved in a 5D tensor matching TCGEN (Tensor Core Generation) expectations

### Dynamic Quantization Kernel

```mojo
fn quantize_dynamic_scaled_fp4fp8_kernel[...](
    output, scales, input, num_cols, num_cols_padded, tensor_sf
):
    # 8 elements per thread
    var input_vector = input.load[8](global_row_idx, global_col_idx)

    # Warp-level reduction for group max (2 threads share 16 elements)
    var thread_max = abs(input_vector).reduce_max()
    thread_max = max(shuffle_xor(thread_max, 1), thread_max)

    # Compute FP8 scale factor
    var scale_factor = tensor_sf * (group_max * recip(6.0))  # 6.0 = FP4 max
    var fp8_scale_factor = scale_factor.cast[scales_dtype]()

    # Quantize to FP4
    var output_scale = recip(fp8_scale_factor * recip(tensor_sf))
    var output_vector = cast_fp32_to_fp4e2m1(input_f32 * output_scale)
```

### Scale Factor Interleaving

The scale factor layout must match NVIDIA's TCGEN expectations. The `block_scales_interleave_fp4` kernel transforms from a simple 2D layout to the 5D interleaved format:
`[ceildiv(M, SF_MN_GROUP_SIZE), ceildiv(K, SF_K_GROUP_SIZE), SF_ATOM_M[0], SF_ATOM_M[1], SF_ATOM_K]`

### TMA-Based Async Quantization

For large tensors, an optimized path uses TMA (Tensor Memory Accelerator):
- 128 data threads + 32 TMA threads per block
- TMA threads issue async loads from global to shared memory
- Data threads quantize from shared memory and write results via TMA stores
- Uses `named_barrier` for producer-consumer synchronization

## How Dequantization Fuses with Matmul

### CPU Path (qmatmul.mojo)
The int4 dequantization is fused into the matmul inner loop:
1. Load packed int4 weights, unpack to int8
2. Compute int8 x int8 dot products using ISA-specific instructions (VNNI, NEON dotprod)
3. Accumulate in int32
4. Apply scale factors only after completing the full K reduction: `float_result = int32_result * a_scale * b_scale`

### GPU Path (TensorCore.load_b with scales)
The `TensorCore.load_b` overload that accepts `scales` fuses dequantization into fragment loading:

```mojo
fn load_b(self, warp_tile, fragments, scales, mma_tile_coord_k):
    # Load packed int4 data
    var vec = bitcast[DType.int32, 4](mma_tile.vectorize[1, 4]()[0, lane_id])

    # Unpack and dequantize each fragment pair
    for i in range(0, num_frags, 2):
        var q_int = vec[i // 2]
        # Extract and dequantize using scale
        var v1 = int4tobf16(q_int, scales[i, 0])
        q_int >>= 4
        var v2 = int4tobf16(q_int, scales[i, 0])
        fragments[i, 0] = v1.join(v2)
```

The `int4tobf16` function uses a bit-manipulation trick: it constructs BF16 values by placing int4 bits into the BF16 mantissa field, then subtracts a bias, using the `lop` (Logical Operation) intrinsic for efficient bit masking.

### SM100 Block-Scaled Matmul
On Blackwell, block-scaled matmul uses hardware UMMA (Unified MMA) instructions that natively support FP4xFP4 with block scaling. The scale factors are passed alongside the operand tiles in the TCGEN-expected layout.

> **Key Takeaway for Neutron**: For initial quantization support, implement Q4sym with group_size=32 -- it covers the most common LLM quantization format. The key optimization is fusing dequant into the matmul inner loop rather than materializing full-precision intermediates. For GPU, the `TensorCore.load_b(scales)` pattern of dequantizing during fragment loading is the right approach. The GGML-compatible Q4_K/Q6_K formats are essential for loading llama.cpp models. FP8 support should come next, with dynamic per-token scaling as the default. NVFP4 is Blackwell-only and can wait until that hardware is relevant.
