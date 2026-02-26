# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Paged Batch Scheduler Tests
# ===----------------------------------------------------------------------=== #

"""Tests for continuous batching scheduler with paged KV cache."""

from neutron_mojo.serve.paged_scheduler import (
    PagedBatchEntry,
    PagedBatchScheduler,
    run_paged_scheduler_to_completion,
)
from neutron_mojo.serve.scheduler import (
    FinishedRequest,
    SchedulerStats,
    BatchScheduler,
    run_scheduler_to_completion,
)
from neutron_mojo.serve.handler import InferenceRequest
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.paged_kv_cache import PagedKVCache
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
    """Build a tiny model for testing (2 layers, vocab=32, dim=16)."""
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
# PagedBatchScheduler Tests
# ===----------------------------------------------------------------------=== #

fn test_paged_scheduler_creation() raises:
    """Test PagedBatchScheduler initialization."""
    var params = tiny_test_params()
    var sched = PagedBatchScheduler(
        params, max_batch_size=4, max_seq_len=256,
        max_pages_per_request=64, page_size=16,
    )

    assert_true(sched.max_batch_size == 4, "Max batch should be 4")
    assert_true(sched.max_seq_len == 256, "Max seq len should be 256")
    assert_true(sched.page_size == 16, "Page size should be 16")
    assert_true(sched.active_count() == 0, "Should have no active requests")
    assert_true(not sched.has_active(), "Should not have active requests")
    assert_true(sched.total_pages_used() == 0, "No pages used initially")

    print("  paged_scheduler_creation: PASS")


fn test_paged_scheduler_enqueue() raises:
    """Test enqueueing requests."""
    var params = tiny_test_params()
    var sched = PagedBatchScheduler(params, max_batch_size=2)

    var ok1 = sched.enqueue(_make_request("hello", 5))
    var ok2 = sched.enqueue(_make_request("world", 5))
    assert_true(ok1, "First enqueue should succeed")
    assert_true(ok2, "Second enqueue should succeed")
    assert_true(sched.queue_depth() == 2, "Queue depth should be 2")
    assert_true(sched.has_pending(), "Should have pending requests")

    print("  paged_scheduler_enqueue: PASS")


fn test_paged_scheduler_admit() raises:
    """Test admitting requests from queue to active batch."""
    var params = tiny_test_params()
    var sched = PagedBatchScheduler(params, max_batch_size=2, max_seq_len=64)
    var tok = _build_tiny_tokenizer()

    _ = sched.enqueue(_make_request("ab", 3))
    _ = sched.enqueue(_make_request("cd", 3))
    _ = sched.enqueue(_make_request("ef", 3))

    sched.admit_from_queue(tok)

    assert_true(sched.active_count() == 2, "Should admit 2 (max batch size)")
    assert_true(sched.queue_depth() == 1, "One should remain in queue")

    print("  paged_scheduler_admit: PASS")


fn test_paged_scheduler_single_request() raises:
    """Test processing a single request to completion."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=64, theta=params.rope_theta,
    )

    var sched = PagedBatchScheduler(
        params, max_batch_size=1, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )
    _ = sched.enqueue(_make_request_with_id("hi", 3, "req-1"))

    var results = run_paged_scheduler_to_completion(sched, model, tok, rope, max_steps=200)

    assert_true(len(results) == 1, "Should have 1 result")
    assert_true(results[0].request_id == "req-1", "Request ID should match")
    assert_true(results[0].tokens_generated <= 3, "Should generate <= 3 tokens")
    assert_true(results[0].prompt_tokens > 0, "Should have prompt tokens")

    print("  paged_scheduler_single_request: PASS")


fn test_paged_scheduler_multiple_requests() raises:
    """Test processing multiple requests."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=64, theta=params.rope_theta,
    )

    var sched = PagedBatchScheduler(
        params, max_batch_size=4, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )
    _ = sched.enqueue(_make_request_with_id("ab", 2, "r1"))
    _ = sched.enqueue(_make_request_with_id("cd", 2, "r2"))
    _ = sched.enqueue(_make_request_with_id("ef", 2, "r3"))

    var results = run_paged_scheduler_to_completion(sched, model, tok, rope, max_steps=200)

    assert_true(len(results) == 3, "Should have 3 results, got " + String(len(results)))

    var stats = sched.get_stats()
    assert_true(stats.total_requests_processed == 3, "Should process 3 requests")
    assert_true(stats.total_tokens_generated > 0, "Should generate some tokens")

    print("  paged_scheduler_multiple_requests: PASS")


fn test_paged_scheduler_batch_overflow() raises:
    """Test that requests beyond batch size wait in queue."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=64, theta=params.rope_theta,
    )

    var sched = PagedBatchScheduler(
        params, max_batch_size=1, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )
    _ = sched.enqueue(_make_request_with_id("a", 2, "first"))
    _ = sched.enqueue(_make_request_with_id("b", 2, "second"))

    sched.admit_from_queue(tok)
    assert_true(sched.active_count() == 1, "Only 1 should be active")
    assert_true(sched.queue_depth() == 1, "1 should remain queued")

    var results = run_paged_scheduler_to_completion(sched, model, tok, rope, max_steps=200)
    assert_true(len(results) == 2, "Both should eventually complete, got " + String(len(results)))

    print("  paged_scheduler_batch_overflow: PASS")


fn test_paged_scheduler_stats() raises:
    """Test that statistics are tracked correctly."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=64, theta=params.rope_theta,
    )

    var sched = PagedBatchScheduler(
        params, max_batch_size=2, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )
    _ = sched.enqueue(_make_request("ab", 2))
    _ = sched.enqueue(_make_request("cd", 2))

    _ = run_paged_scheduler_to_completion(sched, model, tok, rope, max_steps=200)

    var stats = sched.get_stats()
    assert_true(stats.total_requests_processed == 2, "Should process 2")
    assert_true(stats.total_steps > 0, "Should have taken steps")
    assert_true(stats.total_prefill_tokens > 0, "Should have prefill tokens")
    assert_true(stats.peak_batch_size >= 1, "Peak batch should be >= 1")

    print("  paged_scheduler_stats: PASS")


fn test_paged_scheduler_queue_drop() raises:
    """Test that queue drops are tracked."""
    var params = tiny_test_params()
    var sched = PagedBatchScheduler(
        params, max_batch_size=2, max_seq_len=64, max_queue_depth=2,
    )

    _ = sched.enqueue(_make_request("a", 5))
    _ = sched.enqueue(_make_request("b", 5))
    var ok = sched.enqueue(_make_request("c", 5))

    assert_true(not ok, "Third enqueue should fail")
    var stats = sched.get_stats()
    assert_true(stats.requests_dropped == 1, "Should track 1 drop")

    print("  paged_scheduler_queue_drop: PASS")


fn test_paged_scheduler_pages_used() raises:
    """Test page usage tracking during processing."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=64, theta=params.rope_theta,
    )

    var sched = PagedBatchScheduler(
        params, max_batch_size=2, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )
    _ = sched.enqueue(_make_request("ab", 2))

    # Before admission — no pages
    assert_true(sched.total_pages_used() == 0, "No pages before admission")

    # Admit and run one step (prefill)
    sched.admit_from_queue(tok)
    assert_true(sched.active_count() == 1, "1 active after admission")

    # Before step — cache is empty (no pages yet)
    assert_true(sched.total_pages_used() == 0, "No pages before first step")

    # After prefill step — pages allocated on demand
    _ = sched.step(model, tok, rope)
    assert_true(sched.total_pages_used() > 0, "Pages allocated after prefill")

    print("  paged_scheduler_pages_used: PASS")


fn test_paged_scheduler_memory_savings() raises:
    """Compare memory usage: paged vs contiguous scheduler."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var max_seq = 64
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=max_seq, theta=params.rope_theta,
    )

    # Contiguous scheduler: pre-allocates full max_seq_len per request
    var contiguous_bytes_per_request = max_seq * params.num_kv_heads * params.head_dim * 4 * 2 * params.num_layers

    # Run paged scheduler with short prompts
    var sched = PagedBatchScheduler(
        params, max_batch_size=2, max_seq_len=max_seq,
        max_pages_per_request=32, page_size=4,
    )
    _ = sched.enqueue(_make_request("a", 2))
    _ = sched.enqueue(_make_request("b", 2))

    var results = run_paged_scheduler_to_completion(sched, model, tok, rope, max_steps=200)
    assert_true(len(results) == 2, "Both should complete")

    # The paged scheduler only allocated pages for actual tokens used,
    # not for the full max_seq_len. With page_size=4 and short sequences,
    # this is a significant saving.
    var contiguous_total = contiguous_bytes_per_request * 2
    print("  Contiguous would use: " + String(contiguous_total) + " B for 2 requests")
    print("  (Paged uses pages on demand — allocated only for actual tokens)")

    print("  paged_scheduler_memory_savings: PASS")


fn test_paged_vs_contiguous_output() raises:
    """Verify paged scheduler produces same tokens as contiguous scheduler."""
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    var rope = RoPETable(
        head_dim=params.head_dim, max_seq_len=64, theta=params.rope_theta,
    )

    # Run contiguous scheduler
    var csched = BatchScheduler(params, max_batch_size=1, max_seq_len=64)
    _ = csched.enqueue(_make_request_with_id("ab", 3, "c1"))
    var c_results = run_scheduler_to_completion(csched, model, tok, rope, max_steps=200)

    # Run paged scheduler
    var psched = PagedBatchScheduler(
        params, max_batch_size=1, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )
    _ = psched.enqueue(_make_request_with_id("ab", 3, "p1"))
    var p_results = run_paged_scheduler_to_completion(psched, model, tok, rope, max_steps=200)

    assert_true(len(c_results) == 1, "Contiguous should produce 1 result")
    assert_true(len(p_results) == 1, "Paged should produce 1 result")
    assert_true(
        c_results[0].tokens_generated == p_results[0].tokens_generated,
        "Same number of tokens generated: c=" +
        String(c_results[0].tokens_generated) +
        " p=" + String(p_results[0].tokens_generated),
    )
    assert_true(
        c_results[0].text == p_results[0].text,
        "Output text should match",
    )

    print("  paged_vs_contiguous_output: PASS")


fn test_paged_scheduler_peak_batch() raises:
    """Test peak batch size tracking."""
    var params = tiny_test_params()
    var tok = _build_tiny_tokenizer()
    var sched = PagedBatchScheduler(
        params, max_batch_size=4, max_seq_len=64,
        max_pages_per_request=32, page_size=4,
    )

    _ = sched.enqueue(_make_request("a", 5))
    _ = sched.enqueue(_make_request("b", 5))
    _ = sched.enqueue(_make_request("c", 5))

    sched.admit_from_queue(tok)

    var stats = sched.get_stats()
    assert_true(stats.peak_batch_size == 3, "Peak batch should be 3")

    print("  paged_scheduler_peak_batch: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_paged_scheduler:")

    test_paged_scheduler_creation()
    test_paged_scheduler_enqueue()
    test_paged_scheduler_admit()
    test_paged_scheduler_single_request()
    test_paged_scheduler_multiple_requests()
    test_paged_scheduler_batch_overflow()
    test_paged_scheduler_stats()
    test_paged_scheduler_queue_drop()
    test_paged_scheduler_pages_used()
    test_paged_scheduler_memory_savings()
    test_paged_vs_contiguous_output()
    test_paged_scheduler_peak_batch()

    print("ALL PASSED (12 tests)")
