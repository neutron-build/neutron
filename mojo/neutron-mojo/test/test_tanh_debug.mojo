from math import tanh
from neutron_mojo.tensor import Tensor

fn main() raises:
    print("Testing tanh(0):")
    var t = tanh(0.0)
    print("  tanh(0.0) = " + String(t))

    print("Testing tensor creation:")
    var x = Tensor[DType.float32](1)
    print("  Created tensor(1)")
    print("  x.numel() = " + String(x.numel()))
    print("  x.ndim() = " + String(x.ndim()))
    print("  x.shape()[0] = " + String(x.shape()[0]))

    x.data_ptr().store(0, Float32(0.0))
    var val = x.data_ptr().load(0)
    _ = x.numel()  # keepalive
    print("  Stored 0.0, loaded: " + String(val))

    var result = Tensor[DType.float32](1)
    result.data_ptr().store(0, Float32(42.0))
    var r = result.data_ptr().load(0)
    _ = result.numel()  # keepalive
    print("  Result tensor test: " + String(r))
