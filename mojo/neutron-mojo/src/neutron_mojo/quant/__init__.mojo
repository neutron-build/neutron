# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Quantization Package
# ===----------------------------------------------------------------------=== #

"""Quantization utilities for efficient model storage and inference."""

from .types import (
    QuantType,
    QuantConfig,
    QuantBlock,
    q4_0_config,
    q4_1_config,
    q8_0_config,
    q4_k_m_config,
    nf4_config,
    fp8_e4m3_config,
    fp8_e5m2_config,
    calc_quant_size,
    is_symmetric_quant,
)

from .nf4 import (
    get_nf4_value,
    quantize_nf4,
    dequantize_nf4,
    quantize_nf4_block,
    dequantize_nf4_block,
    nf4_table_size,
    nf4_bytes_per_block,
)

from .q8_0 import (
    q8_0_block_size,
    q8_0_bytes_per_block,
    quantize_q8_0,
    dequantize_q8_0,
    quantize_q8_0_block,
    dequantize_q8_0_block,
    calc_q8_0_buffer_size,
)

from .q4_k import (
    Q4KParams,
    q4_k_block_size,
    q4_k_subblock_size,
    q4_k_bytes_per_block,
    quantize_q4_k,
    dequantize_q4_k,
    quantize_q4_k_block,
    dequantize_q4_k_block,
    calc_q4_k_buffer_size,
)

from .fp8 import (
    quantize_fp8_e4m3,
    dequantize_fp8_e4m3,
    quantize_fp8_e5m2,
    dequantize_fp8_e5m2,
    convert_fp32_to_fp8_e4m3,
    convert_fp8_e4m3_to_fp32,
    convert_fp32_to_fp8_e5m2,
    convert_fp8_e5m2_to_fp32,
)
