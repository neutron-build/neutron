# ===----------------------------------------------------------------------=== #
# Neutron Mojo — DLPack Tensor Exchange
# ===----------------------------------------------------------------------=== #

"""Copy-based DLPack tensor exchange functions.

Fills DLManagedTensorVersioned structs from Mojo tensors and vice versa.
Uses copy semantics (not zero-copy) since Mojo's UnsafePointer exposure
to external runtimes is not yet stable.
"""

from memory import UnsafePointer, alloc
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.dlpack.dlpack import (
    DLTensor, DLManagedTensorVersioned, DLDevice, DLDataType,
    DLPackVersion, DLPACK_VERSION, kDLCPU, kDLFloat,
    DLPACK_FLAG_BITMASK_IS_COPIED,
)


fn tensor_to_dlpack(tensor: Tensor[DType.float32]) -> DLManagedTensorVersioned:
    """Create a DLManagedTensorVersioned from a Mojo tensor (copy-based).

    The data is copied into a freshly allocated buffer owned by the
    managed tensor. The shape array is also freshly allocated.

    Args:
        tensor: Source Mojo float32 tensor.

    Returns:
        DLManagedTensorVersioned with copied data and shape.
    """
    var n = tensor.numel()

    # Allocate and copy data (n float32 = n*4 bytes)
    var data_ptr = alloc[UInt8](n * 4)
    var float_ptr = data_ptr.bitcast[Float32]()
    for i in range(n):
        float_ptr.offset(i).init_pointee_copy(tensor.get(i))

    # Allocate shape (1D)
    var shape_ptr = alloc[Int64](1)
    shape_ptr.init_pointee_copy(Int64(n))

    var strides_ptr = UnsafePointer[Int64, MutExternalOrigin]()

    var dl = DLTensor(
        data=data_ptr,
        device=DLDevice(kDLCPU, 0),
        ndim=1,
        dtype=DLDataType(kDLFloat, 32, 1),
        shape=shape_ptr,
        strides=strides_ptr,
        byte_offset=0,
    )

    var managed = DLManagedTensorVersioned()
    managed.version = DLPACK_VERSION
    managed.flags = DLPACK_FLAG_BITMASK_IS_COPIED
    managed.dl_tensor = dl
    return managed


fn dlpack_to_tensor(managed: DLManagedTensorVersioned) -> Tensor[DType.float32]:
    """Create a Mojo tensor from a DLManagedTensorVersioned (copy-based).

    Copies data from the DLPack buffer into a new Mojo tensor.

    Args:
        managed: Source DLPack managed tensor.

    Returns:
        New Mojo tensor with copied data.
    """
    var ndim = Int(managed.dl_tensor.ndim)
    var total = 1
    for i in range(ndim):
        total *= Int(managed.dl_tensor.shape.offset(i)[])

    var t = Tensor[DType.float32](Shape(total))
    var src = managed.dl_tensor.data.bitcast[Float32]()
    for i in range(total):
        t.set(i, src.offset(i)[])
    return t^


fn dlpack_shape(managed: DLManagedTensorVersioned) -> List[Int]:
    """Extract shape dimensions from a DLPack managed tensor.

    Args:
        managed: Source DLPack managed tensor.

    Returns:
        List of dimension sizes.
    """
    var ndim = Int(managed.dl_tensor.ndim)
    var dims = List[Int]()
    for i in range(ndim):
        dims.append(Int(managed.dl_tensor.shape.offset(i)[]))
    return dims^


fn dlpack_numel(managed: DLManagedTensorVersioned) -> Int:
    """Compute total number of elements from DLPack shape.

    Args:
        managed: Source DLPack managed tensor.

    Returns:
        Total number of elements.
    """
    var ndim = Int(managed.dl_tensor.ndim)
    var total = 1
    for i in range(ndim):
        total *= Int(managed.dl_tensor.shape.offset(i)[])
    return total


fn dlpack_free(mut managed: DLManagedTensorVersioned):
    """Free the data and shape buffers of a DLPack managed tensor.

    Only call this on tensors created by tensor_to_dlpack.

    Args:
        managed: The managed tensor to free.
    """
    if managed.dl_tensor.data:
        managed.dl_tensor.data.free()
    if managed.dl_tensor.shape:
        managed.dl_tensor.shape.free()
