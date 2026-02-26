# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Scheduler Tests
# ===----------------------------------------------------------------------=== #

"""Tests for continuous batching scheduler."""

from neutron_mojo.serve.scheduler import (
    BatchEntry,
    FinishedRequest,
    RequestQueue,
    QueuedRequest,
    SchedulerStats,
    BatchScheduler,
    run_scheduler_to_completion,
)
from neutron_mojo.serve.handler import InferenceRequest
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


# ===----------------------------------------------------------------------=== #
# Helper builders
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny model for testing (1 layer, vocab=32, dim=16)."""
    var params = tiny_test_params()
    var model = Model(params)
    var total = model.layer_weights.numel()
    for i in range(total):
        model.layer_weights.set(i, Float32(0.01) * Float32(i % 7 - 3))
    var embed_total = model.embed.numel()
    for i in range(embed_total):
        model.embed.set(i, Float32(0.01) * Float32(i % 5 - 2))
    for i in range(model.final_norm.numel()):
        model.final_norm.set(i, 1.0)
    var lm_total = model.lm_head.numel()
    for i in range(lm_total):
        model.lm_head.set(i, Float32(0.01) * Float32(i % 11 - 5))
    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    """Build tokenizer for tiny model (vocab=32)."""
    var tok = BPETokenizer()
    _ = tok.add_special_token("<bos>", "bos")
    _ = tok.add_special_token("<eos>", "eos")
    _ = tok.add_special_token("<unk>", "unk")
    for i in range(29):
        _ = tok.add_token(chr(97 + (i % 26)))
    tok.unk_id = 2
    return tok^


fn _make_request(prompt: String, max_tokens: Int) -> InferenceRequest:
    """Create an InferenceRequest."""
    var req = InferenceRequest(prompt)
    req.max_tokens = max_tokens
    return req^


fn _make_request_with_id(prompt: String, max_tokens: Int, request_id: String) -> InferenceRequest:
    """Create an InferenceRequest with a specific ID."""
    var req = InferenceRequest(prompt)
    req.max_tokens = max_tokens
    req.request_id = request_id
    return req^


# ===----------------------------------------------------------------------=== #
# RequestQueue Tests
# ===----------------------------------------------------------------------=== #

fn test_queue_creation() raises:
    """Test RequestQueue initialization."""
    var q = RequestQueue(max_depth=8)
    assert_true(q.is_empty(), "Queue should start empty")
    assert_true(q.depth() == 0, "Depth should be 0")
    assert_true(q.total_enqueued == 0, "Total enqueued should be 0")
    assert_true(q.total_dropped == 0, "Total dropped should be 0")
    print("  queue_creation: PASS")


fn test_queue_enqueue_dequeue() raises:
    """Test basic enqueue and dequeue."""
    var q = RequestQueue(max_depth=8)
    var req = _make_request("hello", 10)

    var ok = q.enqueue(req, 1000)
    assert_true(ok, "Should enqueue successfully")
    assert_true(q.depth() == 1, "Depth should be 1")
    assert_true(q.total_enqueued == 1, "Total enqueued should be 1")

    var dequeued = q.dequeue()
    assert_true(dequeued.request.prompt == "hello", "Should dequeue same request")
    assert_true(dequeued.enqueue_time_ns == 1000, "Should preserve timestamp")
    assert_true(q.is_empty(), "Queue should be empty after dequeue")

    print("  queue_enqueue_dequeue: PASS")


fn test_queue_fifo_order() raises:
    """Test FIFO ordering."""
    var q = RequestQueue(max_depth=8)
    _ = q.enqueue(_make_request_with_id("first", 5, "r1"), 100)
    _ = q.enqueue(_make_request_with_id("second", 5, "r2"), 200)
    _ = q.enqueue(_make_request_with_id("third", 5, "r3"), 300)

    assert_true(q.depth() == 3, "Should have 3 items")

    var d1 = q.dequeue()
    assert_true(d1.request.request_id == "r1", "First dequeue should be r1")
    var d2 = q.dequeue()
    assert_true(d2.request.request_id == "r2", "Second dequeue should be r2")
    var d3 = q.dequeue()
    assert_true(d3.request.request_id == "r3", "Third dequeue should be r3")

    print("  queue_fifo_order: PASS")


fn test_queue_max_depth() raises:
    """Test queue drops requests when full."""
    var q = RequestQueue(max_depth=2)
    _ = q.enqueue(_make_request("a", 5), 100)
    _ = q.enqueue(_make_request("b", 5), 200)

    var ok = q.enqueue(_make_request("c", 5), 300)
    assert_true(not ok, "Should reject when full")
    assert_true(q.total_dropped == 1, "Should track dropped")
    assert_true(q.depth() == 2, "Depth should remain 2")

    print("  queue_max_depth: PASS")


# ===----------------------------------------------------------------------=== #
# SchedulerStats Tests
# ===----------------------------------------------------------------------=== #

fn test_scheduler_stats() raises:
    """Test SchedulerStats initialization and methods."""
    var stats = SchedulerStats()
    assert_true(stats.total_requests_processed == 0, "Should start at 0")
    assert_true(stats.avg_latency_ms() == 0, "Avg latency should be 0 with no data")
    assert_true(stats.tokens_per_step() == 0.0, "Tokens per step should be 0")

    stats.total_requests_processed = 4
    stats.total_latency_ms = 100
    assert_true(stats.avg_latency_ms() == 25, "Avg latency should be 25ms")

    stats.total_tokens_generated = 20
    stats.total_steps = 5
    assert_true(stats.tokens_per_step() == 4.0, "Tokens per step should be 4.0")

    print("  scheduler_stats: PASS")


# ===----------------------------------------------------------------------=== #
# FinishedRequest Tests
# ===----------------------------------------------------------------------=== #

fn test_finished_request() raises:
    """Test FinishedRequest creation and copy."""
    var fr = FinishedRequest("req-1", "Hello world", 10, 5, 42)
    assert_true(fr.request_id == "req-1", "ID should match")
    assert_true(fr.text == "Hello world", "Text should match")
    assert_true(fr.tokens_generated == 10, "Gen tokens should match")
    assert_true(fr.prompt_tokens == 5, "Prompt tokens should match")
    assert_true(fr.latency_ms == 42, "Latency should match")

    var fr2 = fr.copy()
    assert_true(fr2.request_id == "req-1", "Copy should match")

    print("  finished_request: PASS")


# ===----------------------------------------------------------------------=== #
# BatchScheduler Tests
# ===----------------------------------------------------------------------=== #

fn test_scheduler_creation() raises:
    """Test BatchScheduler initialization."""
    var params = tiny_test_params()
    var sched = BatchScheduler(params, max_batch_size=4, max_seq_len=256)

    assert_true(sched.max_batch_size == 4, "Max batch should be 4")
    assert_true(sched.max_seq_len == 256, "Max seq len should be 256")
    assert_true(sched.active_count() == 0, "Should have no active requests")
    assert_true(not sched.has_active(), "Should not have active requests")
    assert_true(not sched.has_pending(), "Should not have pending requests")

    print("  scheduler_creation: PASS")


fn test_scheduler_enqueue() raises:
    """Test enqueueing requests."""
    var params = tiny_test_params()
    var sched = BatchScheduler(params, max_batch_size=2)

    var ok1 = sched.enqueue(_make_request("hello", 5))
    var ok2 = sched.enqueue(_make_request("world", 5))
    assert_true(ok1, "First enqueue should succeed")
    assert_true(ok2, "Second enqueue should succeed")
    assert_true(sched.queue_depth() == 2, "Queue depth should be 2")
    assert_true(sched.has_pending(), "Should have pending requests")

    print("  scheduler_enqueue: PASS")


fn test_scheduler_admit() raises:
    """Test admitting requests from queue to active batch."""
    var params = tiny_test_params()
    var sched = BatchScheduler(params, max_batch_size=2, max_seq_len=64)
    var tok = _build_tiny_tokenizer()

    _ = sched.enqueue(_make_request("ab", 3))
    _ = sched.enqueue(_make_request("cd", 3))
    _ = sched.enqueue(_make_request("ef", 3))  # This one should wait

    sched.admit_from_queue(tok)

    assert_true(sched.active_count() == 2, "Should admit 2 (max batch size)")
    assert_true(sched.queue_depth() == 1, "One should remain in queue")

    print("  scheduler_admit: PASS")


fn test_scheduler_single_request() raises:
    """Test processing a single request to completion."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=64,
        theta=params.rope_theta,
    )

    var sched = BatchScheduler(params, max_batch_size=1, max_seq_len=64)
    _ = sched.enqueue(_make_request_with_id("hi", 3, "req-1"))

    var results = run_scheduler_to_completion(sched, model, tok, rope, max_steps=200)

    assert_true(len(results) == 1, "Should have 1 result")
    assert_true(results[0].request_id == "req-1", "Request ID should match")
    assert_true(results[0].tokens_generated <= 3, "Should generate <= 3 tokens")
    assert_true(results[0].prompt_tokens > 0, "Should have prompt tokens")

    print("  scheduler_single_request: PASS")


fn test_scheduler_multiple_requests() raises:
    """Test processing multiple requests."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=64,
        theta=params.rope_theta,
    )

    var sched = BatchScheduler(params, max_batch_size=4, max_seq_len=64)
    _ = sched.enqueue(_make_request_with_id("ab", 2, "r1"))
    _ = sched.enqueue(_make_request_with_id("cd", 2, "r2"))
    _ = sched.enqueue(_make_request_with_id("ef", 2, "r3"))

    var results = run_scheduler_to_completion(sched, model, tok, rope, max_steps=200)

    assert_true(len(results) == 3, "Should have 3 results, got " + String(len(results)))

    var stats = sched.get_stats()
    assert_true(stats.total_requests_processed == 3, "Should process 3 requests")
    assert_true(stats.total_tokens_generated > 0, "Should generate some tokens")

    print("  scheduler_multiple_requests: PASS")


fn test_scheduler_batch_overflow() raises:
    """Test that requests beyond batch size wait in queue."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=64,
        theta=params.rope_theta,
    )

    var sched = BatchScheduler(params, max_batch_size=1, max_seq_len=64)
    _ = sched.enqueue(_make_request_with_id("a", 2, "first"))
    _ = sched.enqueue(_make_request_with_id("b", 2, "second"))

    # Admit — only 1 should be active
    sched.admit_from_queue(tok)
    assert_true(sched.active_count() == 1, "Only 1 should be active")
    assert_true(sched.queue_depth() == 1, "1 should remain queued")

    # Run to completion — both should finish
    var results = run_scheduler_to_completion(sched, model, tok, rope, max_steps=200)
    assert_true(len(results) == 2, "Both should eventually complete, got " + String(len(results)))

    print("  scheduler_batch_overflow: PASS")


fn test_scheduler_stats_tracking() raises:
    """Test that statistics are tracked correctly."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim,
        max_seq_len=64,
        theta=params.rope_theta,
    )

    var sched = BatchScheduler(params, max_batch_size=2, max_seq_len=64)
    _ = sched.enqueue(_make_request("ab", 2))
    _ = sched.enqueue(_make_request("cd", 2))

    _ = run_scheduler_to_completion(sched, model, tok, rope, max_steps=200)

    var stats = sched.get_stats()
    assert_true(stats.total_requests_processed == 2, "Should process 2")
    assert_true(stats.total_steps > 0, "Should have taken steps")
    assert_true(stats.total_prefill_tokens > 0, "Should have prefill tokens")
    assert_true(stats.peak_batch_size >= 1, "Peak batch should be >= 1")

    print("  scheduler_stats_tracking: PASS")


fn test_scheduler_queue_drop() raises:
    """Test that queue drops are tracked."""
    var params = tiny_test_params()
    var sched = BatchScheduler(params, max_batch_size=2, max_seq_len=64,
                               max_queue_depth=2)

    _ = sched.enqueue(_make_request("a", 5))
    _ = sched.enqueue(_make_request("b", 5))
    var ok = sched.enqueue(_make_request("c", 5))  # Should be dropped

    assert_true(not ok, "Third enqueue should fail")
    var stats = sched.get_stats()
    assert_true(stats.requests_dropped == 1, "Should track 1 drop")

    print("  scheduler_queue_drop: PASS")


fn test_scheduler_has_active() raises:
    """Test has_active reflects both queue and active batch."""
    var params = tiny_test_params()
    var sched = BatchScheduler(params, max_batch_size=2, max_seq_len=64)

    assert_true(not sched.has_active(), "Empty scheduler should not have active")

    _ = sched.enqueue(_make_request("a", 5))
    assert_true(sched.has_active(), "Should have active after enqueue (queue counts)")

    print("  scheduler_has_active: PASS")


fn test_scheduler_peak_batch_tracking() raises:
    """Test peak batch size tracking."""
    var params = tiny_test_params()
    var tok = _build_tiny_tokenizer()
    var sched = BatchScheduler(params, max_batch_size=4, max_seq_len=64)

    _ = sched.enqueue(_make_request("a", 5))
    _ = sched.enqueue(_make_request("b", 5))
    _ = sched.enqueue(_make_request("c", 5))

    sched.admit_from_queue(tok)

    var stats = sched.get_stats()
    assert_true(stats.peak_batch_size == 3, "Peak batch should be 3")

    print("  scheduler_peak_batch_tracking: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_scheduler:")

    # Queue tests
    test_queue_creation()
    test_queue_enqueue_dequeue()
    test_queue_fifo_order()
    test_queue_max_depth()

    # Stats tests
    test_scheduler_stats()
    test_finished_request()

    # Scheduler tests
    test_scheduler_creation()
    test_scheduler_enqueue()
    test_scheduler_admit()
    test_scheduler_single_request()
    test_scheduler_multiple_requests()
    test_scheduler_batch_overflow()
    test_scheduler_stats_tracking()
    test_scheduler_queue_drop()
    test_scheduler_has_active()
    test_scheduler_peak_batch_tracking()

    print("ALL PASSED (16 tests)")
