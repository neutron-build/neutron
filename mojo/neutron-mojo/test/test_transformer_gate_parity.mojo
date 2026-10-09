"""MJ-K17: asymmetric standalone/Model/SIMD SwiGLU parity."""
from std.testing import assert_true
from std.math import abs, exp
from neutron_mojo.nn.transformer import TransformerWeights, transformer_block
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.kv_cache import KVCache, MultiLayerKVCache
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.simd_math import simd_swiglu


def near(a: Float32, b: Float32) raises:
    assert_true(abs(a - b) < 0.00001, String(a) + " vs " + String(b))


def main() raises:
    var params = ModelParams()
    params.num_layers = 1
    params.hidden_dim = 2
    params.num_q_heads = 1
    params.num_kv_heads = 1
    params.head_dim = 2
    params.ffn_dim = 2
    params.vocab_size = 2
    params.max_seq_len = 4
    # Match the RoPETable default theta so the MJ-K02 descriptor check passes.
    params.rope_theta = 10000.0
    var model = Model(params)
    var weights = TransformerWeights(2, 1, 1, 2, 2)
    var off = model._layer_offsets(0)
    # Attention matrices stay zero; normed input is exactly [1,1] to f32.
    # Distinct negative/positive gates and positive/negative value operands.
    weights.w_gate.set(0, -1)
    weights.w_gate.set(3, 2)
    weights.w_up.set(0, 3)
    weights.w_up.set(3, -4)
    weights.w_down.set(0, 1)
    weights.w_down.set(3, 1)
    for i in range(4):
        model.layer_weights.set(off.w_gate + i, weights.w_gate.data_ptr()[i])
        model.layer_weights.set(off.w_up + i, weights.w_up.data_ptr()[i])
        model.layer_weights.set(off.w_down + i, weights.w_down.data_ptr()[i])
    var x = Tensor[DType.float32].ones(Shape(2))
    var cache = KVCache(4, 1, 2)
    var multi_cache = MultiLayerKVCache(1, 4, 1, 2)
    var rope = RoPETable(2, 4)
    var standalone = transformer_block(x, weights, cache, rope, 0, 1, 1, 2)
    var modeled = model.forward_layer(x, 0, multi_cache, rope, 0)
    var gate = Tensor[DType.float32](Shape(2))
    var up = Tensor[DType.float32](Shape(2))
    gate.set(0, -1)
    gate.set(1, 2)
    up.set(0, 3)
    up.set(1, -4)
    var simd = Tensor[DType.float32](Shape(2))
    simd_swiglu(simd, 0, gate, 0, up, 0, 2)
    for i in range(2):
        var g = gate.get(i)
        var expected = g / (1 + Float32(exp(Float64(-g)))) * up.get(i)
        near(simd.get(i), expected)
        near(standalone.get(i), 1 + expected)
        near(modeled.get(i), standalone.get(i))
    print("MJ-K17: standalone/Model/SIMD asymmetric gate parity PASS")
