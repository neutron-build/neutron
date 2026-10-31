"""Independent borrowed DLPack ABI fixtures; runtime pending capacity release."""
from std.memory import alloc
from std.testing import assert_true, assert_equal
from neutron_mojo.dlpack.dlpack import DLManagedTensorVersioned, DLTensor, DLDevice, DLDataType, kDLCPU, kDLFloat
from neutron_mojo.dlpack.exchange import dlpack_to_tensor
from neutron_mojo.tensor.shape import Shape


def must_refuse(descriptor: DLManagedTensorVersioned) raises:
    var refused = False
    try:
        var ignored = dlpack_to_tensor(descriptor)
    except:
        refused = True
    assert_true(refused)


def main() raises:
    var data = alloc[Float32](7)
    var dims = alloc[Int64](2)
    var strides = alloc[Int64](2)
    for i in range(7):
        data.unsafe_store(i,Float32(100+i))
    dims.unsafe_store(0,2)
    dims.unsafe_store(1,3)
    strides.unsafe_store(0,3)
    strides.unsafe_store(1,1)
    var valid = DLManagedTensorVersioned()
    valid.dl_tensor = DLTensor(data=data.bitcast[UInt8](),device=DLDevice(kDLCPU,0),ndim=2,dtype=DLDataType(kDLFloat,32,1),shape=dims,strides=strides,byte_offset=4)
    var copied = dlpack_to_tensor(valid)
    assert_true(copied.shape() == Shape(2, 3), "dlpack copy shape")
    for i in range(6):
        assert_equal(copied.data_ptr()[i],Float32(101+i))
    # Refuse unsupported descriptors before their deliberately absent data is read.
    for case_idx in range(9):
        var bad = valid
        bad.dl_tensor.data = None
        if case_idx == 0:
            bad.dl_tensor.device.device_type = 2
        elif case_idx == 1:
            bad.dl_tensor.device.device_id = 1
        elif case_idx == 2:
            bad.dl_tensor.dtype.code = 0
        elif case_idx == 3:
            bad.dl_tensor.dtype.bits = 16
        elif case_idx == 4:
            bad.dl_tensor.dtype.lanes = 2
        elif case_idx == 5:
            bad.version.major = valid.version.major + 1
        elif case_idx == 6:
            bad.dl_tensor.byte_offset = 1
        elif case_idx == 7:
            bad.dl_tensor.ndim = -1
        else:
            bad.dl_tensor.shape = None
        must_refuse(bad)
    var absent = valid
    absent.dl_tensor.data = None
    must_refuse(absent)
    # Valid data, unsupported transposed stride: rejection cannot be hidden by null.
    strides.unsafe_store(0,1)
    strides.unsafe_store(1,2)
    must_refuse(valid)
    strides.unsafe_store(0,3)
    strides.unsafe_store(1,1)
    dims.unsafe_store(0,-1)
    must_refuse(valid)
    dims.unsafe_store(0,0x7FFFFFFFFFFFFFFF)
    must_refuse(valid)
    dims.unsafe_store(0,2)
    var scalar = valid
    scalar.dl_tensor.ndim = 0
    scalar.dl_tensor.shape = None
    scalar.dl_tensor.strides = None
    scalar.dl_tensor.byte_offset = 8
    var scalar_copy = dlpack_to_tensor(scalar)
    assert_true(scalar_copy.shape() == Shape())
    # Read via .get() (storage load); chained data_ptr()[i] subscripts are
    # unreliable under this toolchain's codegen for adjacent scalar reads.
    assert_equal(scalar_copy.get(),Float32(102))
    dims.unsafe_store(0,0)
    var empty = valid
    empty.dl_tensor.data = None
    empty.dl_tensor.byte_offset = 0
    var empty_copy = dlpack_to_tensor(empty)
    assert_true(empty_copy.shape() == Shape(0, 3), "empty dlpack copy shape")
    assert_equal(empty_copy.numel(), 0)
    data.free()
    dims.free()
    strides.free()
    print("R2 independent DLPack offset/stride/scalar/empty/refusal oracles PASS")
