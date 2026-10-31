"""Per-request RNG and failed queue admission fixtures; unexecuted."""
from std.testing import assert_equal, assert_true
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.paged_kv_cache import PagedKVCache
from neutron_mojo.nn.model import tiny_test_params
from neutron_mojo.nn.tokenizer import BPETokenizer, build_byte_level_vocab
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.serve.scheduler import BatchEntry, BatchScheduler
from neutron_mojo.serve.paged_scheduler import PagedBatchEntry, PagedBatchScheduler
from neutron_mojo.serve.handler import InferenceRequest


def test_interleaving() raises:
    var cfg = PipelineConfig()
    cfg.max_new_tokens = 12
    cfg.sampler_config.temperature = 1
    cfg.sampler_config.seed = 42
    var cfg_b = cfg.copy()
    cfg_b.sampler_config.seed = 7
    var ca = MultiLayerKVCache(1, 16, 1, 2)
    var cb = MultiLayerKVCache(1, 16, 1, 2)
    var a = BatchEntry("a", [0], ca^, cfg, List[Int](), 0)
    var b = BatchEntry("b", [0], cb^, cfg_b, List[Int](), 0)
    var pa = PagedKVCache(4, 4, 1, 1, 2)
    var pb = PagedKVCache(4, 4, 1, 1, 2)
    var paged_a = PagedBatchEntry("pa", [0], pa^, cfg, List[Int](), 0)
    var paged_b = PagedBatchEntry("pb", [0], pb^, cfg_b, List[Int](), 0)
    var logits = Tensor[DType.float32](Shape(4))
    for i in range(4):
        logits.set(i,0)
    # Independent integer LCG oracle with uniform four-way categorical bins.
    # Expected values were computed with modulo 2^31 and integer division.
    var expected_a = [2,0,0,1,3,0,3,0,2,3,2,3]
    var expected_b = [1,3,0,3,0,2,2,0,0,3,3,0]
    for i in range(12):
        # Reverse request ordering on alternating rounds.
        if i % 2 == 0:
            assert_equal(a.sampler.sample(logits,4), expected_a[i])
            assert_equal(b.sampler.sample(logits,4), expected_b[i])
        else:
            assert_equal(b.sampler.sample(logits,4), expected_b[i])
            assert_equal(a.sampler.sample(logits,4), expected_a[i])
        assert_equal(paged_b.sampler.sample(logits,4), expected_b[i])
        assert_equal(paged_a.sampler.sample(logits,4), expected_a[i])
        if i == 3:
            var copy_a = a
            var copy_pa = paged_a
            assert_equal(copy_a.sampler.sample(logits,4), expected_a[i+1])
            assert_equal(copy_pa.sampler.sample(logits,4), expected_a[i+1])
    assert_equal(a.sampler.rng.state, 2146095206)
    assert_equal(b.sampler.rng.state, 134221107)


def test_failed_admission_preserves_queue() raises:
    var params = tiny_test_params()
    params.vocab_size = 256
    var tokenizer = BPETokenizer()
    build_byte_level_vocab(tokenizer)
    var scheduler = BatchScheduler(params,max_seq_len=8)
    var paged = PagedBatchScheduler(params,max_seq_len=8,max_pages_per_request=8,page_size=4)
    var request = InferenceRequest("A")
    request.request_id = "oversized"
    request.max_tokens = 8  # Prompt consumes one more position than capacity.
    assert_true(scheduler.enqueue(request))
    assert_true(paged.enqueue(request))
    for _ in range(2):
        var refused = False
        try:
            scheduler.admit_from_queue(tokenizer)
        except:
            refused = True
        assert_true(refused)
        assert_equal(len(scheduler.active),0)
        assert_equal(len(scheduler.queue.items),1)
        assert_equal(scheduler.queue.peek().request.request_id,"oversized")
        refused = False
        try:
            paged.admit_from_queue(tokenizer)
        except:
            refused = True
        assert_true(refused)
        assert_equal(len(paged.active),0)
        assert_equal(len(paged.queue.items),1)
        assert_equal(paged.queue.peek().request.request_id,"oversized")


def main() raises:
    test_interleaving()
    test_failed_admission_preserves_queue()
    print("R2 request interleaving RNG and failed admission preservation oracles PASS")
