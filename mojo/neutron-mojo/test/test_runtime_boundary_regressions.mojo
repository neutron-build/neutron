"""MJ-03/MJ-06/MJ-K02/K19/K20/K22 runtime regressions."""
from std.testing import assert_true, assert_equal
from std.math import abs, cos, sin
from std.random import seed
from neutron_mojo.io.binary_reader import BinaryReader
from neutron_mojo.model.weight_reader import read_tensor_q4_0_as_f32
from neutron_mojo.serve.registry import _copy_tensor, _copy_model
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.rope import RoPETable, apply_rope_single_head
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.streaming import StreamingGenerator
from neutron_mojo.nn.pipeline import PipelineConfig


def main() raises:
    # GGML layout: half scale=1, low nibbles in [0,16), high in [16,32).
    var bytes: List[UInt8] = [0,0x3c]
    for i in range(16):
        bytes.append(UInt8(i | ((15-i)<<4)))
    var reader = BinaryReader(bytes^)
    var decoded = read_tensor_q4_0_as_f32(reader,0,32)
    for i in range(16):
        assert_equal(decoded.get(i),Float32(i-8))
        assert_equal(decoded.get(i+16),Float32(7-i))
    print("MJ-03: independent asymmetric GGML Q4_0 block PASS")
    var tensor = Tensor[DType.float32](Shape(2,3))
    for i in range(6):
        tensor.set(i,Float32(i+1))
    var copied = _copy_tensor(tensor)
    for i in range(6):
        assert_equal(copied.data_ptr().unsafe_load(i),Float32(i+1))
    var params = ModelParams()
    params.num_layers=1
    params.vocab_size=2
    params.hidden_dim=2
    params.num_q_heads=1
    params.num_kv_heads=1
    params.head_dim=2
    params.ffn_dim=2
    params.max_seq_len=4
    var model = Model(params)
    for i in range(4):
        model.embed.set(i,Float32(i+1))
        model.lm_head.set(i,Float32(10+i))
    var model_copy = _copy_model(model)
    for i in range(4):
        assert_equal(model_copy.embed.data_ptr().unsafe_load(i),Float32(i+1))
        assert_equal(model_copy.lm_head.data_ptr().unsafe_load(i),Float32(10+i))
    print("MJ-06: multidimensional registry tensor/Model copy PASS")
    var partial = Tensor[DType.float32](Shape(2))
    partial.set(0,1)
    partial.set(1,2)
    var rope = RoPETable(4,4,rotary_dim=2)
    apply_rope_single_head(partial,rope,1)
    assert_true(abs(partial.get(0)-Float32(cos(1.0)-2*sin(1.0)))<0.00001)
    assert_true(abs(partial.get(1)-Float32(sin(1.0)+2*cos(1.0)))<0.00001)
    print("MJ-K02: explicit partial rotary descriptor PASS")
    var cache = MultiLayerKVCache(1,4,1,2)
    var tokens = List[Int]()
    var refused = False
    try:
        var logits = model.forward_prefill(tokens,cache,rope)
    except:
        refused = True
    assert_true(refused)
    assert_equal(cache.lengths[0],0)
    print("MJ-K22: empty prefill refuses before cache mutation PASS")
    var tokenizer = BPETokenizer()
    _ = tokenizer.add_token("a")
    tokenizer.bos_id=0
    tokenizer.unk_id=0
    var cfg = PipelineConfig()
    cfg.max_new_tokens=0
    var gen = StreamingGenerator(model^,tokenizer^,"a",cfg)
    assert_true(gen.is_finished())
    var event = gen.next_token()
    assert_true(event.is_eos)
    assert_equal(event.token_id,-1)
    assert_equal(gen.tokens_generated(),0)
    print("MJ-K19: zero budget generates no token PASS")
    seed(12345)
    var negative = 0
    var total = Float64(0)
    var square = Float64(0)
    for _ in range(1000):
        var odd = Tensor[DType.float32].randn(Shape(1))
        var value = Float64(odd.get(0))
        if value < 0:
            negative += 1
        total += value
        square += value*value
    assert_true(negative>350 and negative<650)
    assert_true(abs(total/1000)<0.15)
    assert_true(square/1000>0.7 and square/1000<1.3)
    print("MJ-K20: seeded odd-tail sign/mean/variance PASS")
