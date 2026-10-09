# ===----------------------------------------------------------------------=== #
# Neutron Mojo — DLPack Tensor Exchange
# ===----------------------------------------------------------------------=== #

"""Copy-based DLPack tensor exchange functions.

Fills DLManagedTensorVersioned structs from Mojo tensors and vice versa.
Uses copy semantics (not zero-copy) since Mojo's UnsafePointer exposure
to external runtimes is not yet stable.
"""

from std.memory import Pointer, alloc
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.dlpack.dlpack import (
    DLTensor, DLManagedTensorVersioned, DLDevice, DLDataType,
    DLPackVersion, DLPACK_VERSION, kDLCPU, kDLFloat,
    DLPACK_FLAG_BITMASK_IS_COPIED,
)


struct OwnedDLPackTensor(Movable):
    """Move-only owner for local copy exchange, with an explicit borrowed view.

    This is not an externally transferable managed capsule: the borrowed ABI
    view has no C deleter and must not outlive this owner. External managed
    consumers must use a separately implemented capsule adapter.
    """
    var _descriptor: DLManagedTensorVersioned

    def __init__(out self, tensor: Tensor[DType.float32]) raises:
        var shape = tensor.shape()
        shape.validate()
        var n = shape.numel()
        if n > 0x7FFFFFFFFFFFFFFF // 4:
            raise Error("DLPack export byte size overflow")
        var data_ptr = alloc[UInt8](n * 4)
        var float_ptr = data_ptr.bitcast[Float32]()
        for i in range(n):
            float_ptr.unsafe_store(i, tensor.data_ptr().unsafe_load(i))
        var shape_ptr = alloc[Int64](max(1, shape.ndim()))
        for i in range(shape.ndim()):
            shape_ptr.unsafe_store(i, Int64(shape[i]))
        self._descriptor = DLManagedTensorVersioned()
        self._descriptor.flags = DLPACK_FLAG_BITMASK_IS_COPIED
        self._descriptor.dl_tensor = DLTensor(
            data=data_ptr, device=DLDevice(kDLCPU, 0), ndim=Int32(shape.ndim()),
            dtype=DLDataType(kDLFloat, 32, 1), shape=shape_ptr,
            strides=None, byte_offset=0,
        )

    def __init__(out self, *, deinit move: Self):
        self._descriptor = move._descriptor
        move._descriptor = DLManagedTensorVersioned()

    def __deinit__(deinit self):
        if self._descriptor.dl_tensor.data:
            self._descriptor.dl_tensor.data.unsafe_value().free()
        if self._descriptor.dl_tensor.shape:
            self._descriptor.dl_tensor.shape.unsafe_value().free()

    def borrow(self) raises -> DLManagedTensorVersioned:
        if not self._descriptor.dl_tensor.shape:
            raise Error("DLPack owner has been released or moved")
        return self._descriptor

    def release(mut self):
        """Idempotent release; descriptor copies never own these allocations."""
        if self._descriptor.dl_tensor.data:
            self._descriptor.dl_tensor.data.unsafe_value().free()
        if self._descriptor.dl_tensor.shape:
            self._descriptor.dl_tensor.shape.unsafe_value().free()
        self._descriptor = DLManagedTensorVersioned()

    def is_copied(self) -> Bool:
        return self._descriptor.is_copied()

    def is_read_only(self) -> Bool:
        return self._descriptor.is_read_only()


def tensor_to_dlpack(tensor: Tensor[DType.float32]) raises -> OwnedDLPackTensor:
    """Copy data and exact shape into a move-only local exchange owner."""
    return OwnedDLPackTensor(tensor)


def _validated_dlpack_shape(managed: DLManagedTensorVersioned) raises -> Shape:
    """Admit borrowed CPU F32 descriptors before touching their data buffer.

    The caller guarantees readable metadata arrays and allocation extent/lifetime.
    This copy importer admits C-contiguous strides only, including a byte offset.
    """
    if managed.version.major != DLPACK_VERSION.major or managed.version.minor > DLPACK_VERSION.minor:
        raise Error("Unsupported DLPack version")
    var dl = managed.dl_tensor
    if dl.device.device_type != kDLCPU or dl.device.device_id != 0:
        raise Error("DLPack importer requires CPU device 0")
    if dl.dtype.code != kDLFloat or dl.dtype.bits != 32 or dl.dtype.lanes != 1:
        raise Error("DLPack importer requires scalar F32")
    var ndim = Int(dl.ndim)
    if ndim < 0 or (ndim > 0 and not dl.shape):
        raise Error("Invalid DLPack rank or missing shape")
    var dims = List[Int]()
    for i in range(ndim):
        var d = Int(dl.shape.unsafe_value().unsafe_load(i))
        if d < 0:
            raise Error("Negative DLPack dimension")
        dims.append(d)
    var shape = Shape(dims^)
    var total = shape.numel()
    if total > 0x7FFFFFFFFFFFFFFF // 4:
        raise Error("DLPack byte size overflow")
    if dl.byte_offset > UInt64(0x7FFFFFFFFFFFFFFF - total * 4) or dl.byte_offset % 4 != 0:
        raise Error("Invalid DLPack byte offset")
    if total > 0 and not dl.data:
        raise Error("Missing nonempty DLPack data")
    if dl.strides:
        var expected = shape.strides()
        for i in range(ndim):
            # Singleton dimensions do not affect addressed elements.
            if shape[i] != 1 and Int(dl.strides.unsafe_value().unsafe_load(i)) != expected[i]:
                raise Error("DLPack importer requires contiguous strides")
    return shape^


def dlpack_to_tensor(managed: DLManagedTensorVersioned) raises -> Tensor[DType.float32]:
    """Copy an admitted borrowed descriptor, preserving rank and shape."""
    var shape = _validated_dlpack_shape(managed)
    var total = shape.numel()
    var t = Tensor[DType.float32](shape)
    if total > 0:
        var src = managed.dl_tensor.data.unsafe_value().unsafe_offset(Int(managed.dl_tensor.byte_offset)).unsafe_bitcast[Float32]()
        for i in range(total):
            t.set(i, src.unsafe_load(i))
    return t^


def dlpack_shape(managed: DLManagedTensorVersioned) raises -> List[Int]:
    var shape = _validated_dlpack_shape(managed)
    var dims = List[Int]()
    for i in range(shape.ndim()):
        dims.append(shape[i])
    return dims^


def dlpack_numel(managed: DLManagedTensorVersioned) raises -> Int:
    return _validated_dlpack_shape(managed).numel()


def dlpack_to_tensor(owner: OwnedDLPackTensor) raises -> Tensor[DType.float32]:
    return dlpack_to_tensor(owner.borrow())


def dlpack_shape(owner: OwnedDLPackTensor) raises -> List[Int]:
    return dlpack_shape(owner.borrow())


def dlpack_numel(owner: OwnedDLPackTensor) raises -> Int:
    return dlpack_numel(owner.borrow())


def dlpack_free(mut owner: OwnedDLPackTensor):
    """Release only the move-only local owner, never a borrowed ABI descriptor."""
    owner.release()
