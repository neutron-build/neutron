# ===----------------------------------------------------------------------=== #
# Neutron Mojo — DLPack interop
# ===----------------------------------------------------------------------=== #

"""DLPack interop: struct definitions and tensor conversion."""

from .dlpack import (
    # Constants
    kDLInt,
    kDLUInt,
    kDLFloat,
    kDLOpaqueHandle,
    kDLBfloat,
    kDLComplex,
    kDLBool,
    kDLCPU,
    kDLCUDA,
    kDLCUDAHost,
    kDLMetal,
    kDLROCM,
    DLPACK_FLAG_BITMASK_READ_ONLY,
    DLPACK_FLAG_BITMASK_IS_COPIED,
    DLPACK_VERSION,
    # Structs
    DLDataType,
    DLDevice,
    DLPackVersion,
    DLTensor,
    DLManagedTensorVersioned,
    # Conversion functions
    mojo_dtype_to_dl,
    dl_to_mojo_dtype,
)

from .exchange import (
    tensor_to_dlpack,
    dlpack_to_tensor,
    dlpack_shape,
    dlpack_numel,
    dlpack_free,
)
