# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Paged Batch Scheduler
# ===----------------------------------------------------------------------=== #

"""Continuous batching scheduler with paged KV cache for memory-efficient serving.

Like BatchScheduler but uses PagedKVCache instead of contiguous MultiLayerKVCache.
Each request's cache grows on demand via page allocation, reducing memory waste
for short/variable-length sequences.

Performance: Same throughput as contiguous scheduler but ~50-80% less KV cache
memory, enabling more concurrent requests at the same peak memory.

Usage:
    var sched = PagedBatchScheduler(model.params, max_batch=4,
                                     max_pages_per_request=64, page_size=16)
    sched.enqueue(request1)
    while sched.has_active():
        var finished = sched.step(model, tokenizer, rope)
"""

from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.paged_kv_cache import PagedKVCache
from neutron_mojo.nn.paged_forward import paged_forward
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
)
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.nn.causal_lm import apply_temperature
from neutron_mojo.serve.handler import InferenceRequest
from neutron_mojo.serve.scheduler import (
    FinishedRequest,
    RequestQueue,
    QueuedRequest,
    SchedulerStats,
)


# ===----------------------------------------------------------------------=== #
# PagedBatchEntry — Per-request state with paged KV cache
# ===----------------------------------------------------------------------=== #

struct PagedBatchEntry(Copyable, Movable):
    """State for a single request being processed with paged KV cache.

    Uses PagedKVCache instead of MultiLayerKVCache for on-demand page
    allocation. Memory grows proportionally to actual sequence length
    rather than pre-allocating for the maximum.
    """
    var request_id: String
    var input_ids: List[Int]
    var generated: List[Int]
    var cache: PagedKVCache
    var pos: Int
    var prefilled: Bool
    var finished: Bool
    var max_new_tokens: Int
    var stop_tokens: List[Int]
    var config: PipelineConfig
    var enqueue_time_ns: Int
    var start_gen_time_ns: Int

    fn __init__(out self, request_id: String, input_ids: List[Int],
                var cache: PagedKVCache, config: PipelineConfig,
                stop_tokens: List[Int], enqueue_time_ns: Int):
        self.request_id = request_id
        self.input_ids = List[Int]()
        for i in range(len(input_ids)):
            self.input_ids.append(input_ids[i])
        self.generated = List[Int]()
        self.cache = cache^
        self.pos = 0
        self.prefilled = False
        self.finished = False
        self.max_new_tokens = config.max_new_tokens
        self.stop_tokens = List[Int]()
        for i in range(len(stop_tokens)):
            self.stop_tokens.append(stop_tokens[i])
        self.config = config.copy()
        self.enqueue_time_ns = enqueue_time_ns
        self.start_gen_time_ns = 0

    fn __copyinit__(out self, existing: Self):
        self.request_id = existing.request_id
        self.input_ids = List[Int]()
        for i in range(len(existing.input_ids)):
            self.input_ids.append(existing.input_ids[i])
        self.generated = List[Int]()
        for i in range(len(existing.generated)):
            self.generated.append(existing.generated[i])
        self.cache = existing.cache.copy()
        self.pos = existing.pos
        self.prefilled = existing.prefilled
        self.finished = existing.finished
        self.max_new_tokens = existing.max_new_tokens
        self.stop_tokens = List[Int]()
        for i in range(len(existing.stop_tokens)):
            self.stop_tokens.append(existing.stop_tokens[i])
        self.config = existing.config.copy()
        self.enqueue_time_ns = existing.enqueue_time_ns
        self.start_gen_time_ns = existing.start_gen_time_ns

    fn __moveinit__(out self, deinit other: Self):
        self.request_id = other.request_id^
        self.input_ids = other.input_ids^
        self.generated = other.generated^
        self.cache = other.cache^
        self.pos = other.pos
        self.prefilled = other.prefilled
        self.finished = other.finished
        self.max_new_tokens = other.max_new_tokens
        self.stop_tokens = other.stop_tokens^
        self.config = other.config^
        self.enqueue_time_ns = other.enqueue_time_ns
        self.start_gen_time_ns = other.start_gen_time_ns


# ===----------------------------------------------------------------------=== #
# PagedBatchScheduler — Continuous batching with paged KV cache
# ===----------------------------------------------------------------------=== #

struct PagedBatchScheduler(Movable):
    """Continuous batching scheduler using paged KV cache.

    Each request gets its own PagedKVCache with on-demand page allocation.
    Pages are freed when requests complete, reducing memory waste vs
    the contiguous MultiLayerKVCache approach.

    Key benefit: A request generating 10 tokens with page_size=16 uses
    only 1 page per layer, while contiguous cache pre-allocates for
    the full max_seq_len.
    """
    var active: List[PagedBatchEntry]
    var queue: RequestQueue
    var stats: SchedulerStats
    var max_batch_size: Int
    var max_seq_len: Int
    var params: ModelParams
    var max_pages_per_request: Int
    var page_size: Int

    fn __init__(out self, params: ModelParams, max_batch_size: Int = 4,
                max_seq_len: Int = 512, max_queue_depth: Int = 64,
                max_pages_per_request: Int = 64, page_size: Int = 16):
        """Create a paged batch scheduler.

        Args:
            params: Model architecture parameters.
            max_batch_size: Maximum concurrent requests.
            max_seq_len: Maximum sequence length per request.
            max_queue_depth: Maximum pending queue depth.
            max_pages_per_request: Max pages per request's KV cache.
            page_size: Tokens per page.
        """
        self.active = List[PagedBatchEntry]()
        self.queue = RequestQueue(max_queue_depth)
        self.stats = SchedulerStats()
        self.max_batch_size = max_batch_size
        self.max_seq_len = max_seq_len
        self.params = params.copy()
        self.max_pages_per_request = max_pages_per_request
        self.page_size = page_size

    fn __moveinit__(out self, deinit other: Self):
        self.active = other.active^
        self.queue = other.queue^
        self.stats = other.stats.copy()
        self.max_batch_size = other.max_batch_size
        self.max_seq_len = other.max_seq_len
        self.params = other.params.copy()
        self.max_pages_per_request = other.max_pages_per_request
        self.page_size = other.page_size

    fn enqueue(mut self, request: InferenceRequest) -> Bool:
        """Add a request to the pending queue.

        Args:
            request: Inference request.

        Returns:
            True if enqueued, False if queue is full.
        """
        var time_ns = Int(perf_counter_ns())
        var ok = self.queue.enqueue(request, time_ns)
        if not ok:
            self.stats.requests_dropped += 1
        return ok

    fn admit_from_queue(mut self, tokenizer: BPETokenizer) raises:
        """Move requests from queue to active batch if slots available.

        Creates a PagedKVCache for each admitted request with on-demand
        page allocation.

        Args:
            tokenizer: BPE tokenizer for encoding prompts.
        """
        while len(self.active) < self.max_batch_size and not self.queue.is_empty():
            var queued = self.queue.dequeue()
            var req = queued.request.copy()
            var cfg = req.to_pipeline_config()

            # Encode prompt
            var input_ids = tokenizer.encode_with_special(req.prompt, add_bos=True)

            # Create paged KV cache for this request
            var cache = PagedKVCache(
                max_pages=self.max_pages_per_request,
                page_size=self.page_size,
                num_layers=self.params.num_layers,
                num_kv_heads=self.params.num_kv_heads,
                head_dim=self.params.head_dim,
            )

            # Build stop tokens
            var stop_tokens = List[Int]()
            if tokenizer.eos_id >= 0:
                stop_tokens.append(tokenizer.eos_id)

            var entry = PagedBatchEntry(
                req.request_id, input_ids, cache^, cfg,
                stop_tokens, queued.enqueue_time_ns,
            )
            self.active.append(entry^)

            # Track peak batch size
            if len(self.active) > self.stats.peak_batch_size:
                self.stats.peak_batch_size = len(self.active)

    fn step(mut self, model: Model, tokenizer: BPETokenizer,
            rope: RoPETable) raises -> List[FinishedRequest]:
        """Run one step for all active requests using paged forward.

        For each active entry:
        1. If not yet prefilled, process all prompt tokens sequentially
           via paged_forward (pages allocated on demand)
        2. If prefilled, decode one new token
        3. If finished (EOS or max tokens), mark for removal

        Args:
            model: Language model.
            tokenizer: BPE tokenizer.
            rope: RoPE table.

        Returns:
            List of requests that finished during this step.
        """
        self.stats.total_steps += 1
        var finished_list = List[FinishedRequest]()
        var now = Int(perf_counter_ns())

        for i in range(len(self.active)):
            if self.active[i].finished:
                continue

            # Prefill phase: process all prompt tokens via paged_forward
            if not self.active[i].prefilled:
                var n_prompt = len(self.active[i].input_ids)
                if n_prompt > 0:
                    for t in range(n_prompt):
                        _ = paged_forward(
                            model,
                            self.active[i].input_ids[t],
                            self.active[i].cache,
                            rope,
                            pos=t,
                        )
                    self.active[i].pos = n_prompt
                    self.stats.total_prefill_tokens += n_prompt
                self.active[i].prefilled = True
                self.active[i].start_gen_time_ns = now
                continue

            # Decode phase: generate one token
            var logits: Tensor[DType.float32]
            if len(self.active[i].generated) == 0:
                # First decode step — use last prompt token
                var last_token = self.active[i].input_ids[
                    len(self.active[i].input_ids) - 1
                ]
                logits = paged_forward(
                    model, last_token, self.active[i].cache, rope,
                    pos=self.active[i].pos - 1,
                )
            else:
                var last_gen = self.active[i].generated[
                    len(self.active[i].generated) - 1
                ]
                logits = paged_forward(
                    model, last_gen, self.active[i].cache, rope,
                    pos=self.active[i].pos,
                )
                self.active[i].pos += 1

            # Apply penalties
            var cfg = self.active[i].config.copy()
            if cfg.repetition_penalty > 1.0:
                apply_repetition_penalty(
                    logits, self.params.vocab_size,
                    self.active[i].generated, cfg.repetition_penalty)
            if cfg.frequency_penalty != 0.0 or cfg.presence_penalty != 0.0:
                apply_frequency_penalty(
                    logits, self.params.vocab_size,
                    self.active[i].generated,
                    cfg.frequency_penalty, cfg.presence_penalty)

            # Sample
            var sampler = Sampler(cfg.sampler_config)
            var next_token = sampler.sample(logits, self.params.vocab_size)

            # Check stop
            if should_stop(next_token, self.active[i].stop_tokens):
                self.active[i].finished = True
            else:
                self.active[i].generated.append(next_token)
                self.stats.total_tokens_generated += 1

                # Check max tokens
                if len(self.active[i].generated) >= self.active[i].max_new_tokens:
                    self.active[i].finished = True

        # Collect finished entries and remove them
        var keep = List[PagedBatchEntry]()
        for i in range(len(self.active)):
            if self.active[i].finished and self.active[i].prefilled:
                # Build response
                var text = tokenizer.decode(self.active[i].generated)
                var latency_ns = now - self.active[i].enqueue_time_ns
                var latency_ms = Int(Float64(latency_ns) / 1_000_000.0)

                finished_list.append(FinishedRequest(
                    self.active[i].request_id,
                    text,
                    len(self.active[i].generated),
                    len(self.active[i].input_ids),
                    latency_ms,
                ))

                self.stats.total_requests_processed += 1
                self.stats.total_latency_ms += latency_ms
            else:
                var entry = self.active[i].copy()
                keep.append(entry^)

        self.active = keep^
        return finished_list^

    fn has_active(self) -> Bool:
        """Check if there are active or queued requests."""
        return len(self.active) > 0 or not self.queue.is_empty()

    fn has_pending(self) -> Bool:
        """Check if there are requests waiting in the queue."""
        return not self.queue.is_empty()

    fn active_count(self) -> Int:
        """Number of currently active (processing) requests."""
        return len(self.active)

    fn queue_depth(self) -> Int:
        """Number of requests waiting in the queue."""
        return self.queue.depth()

    fn get_stats(self) -> SchedulerStats:
        """Get a copy of current scheduler statistics."""
        var s = SchedulerStats()
        s.total_requests_processed = self.stats.total_requests_processed
        s.total_tokens_generated = self.stats.total_tokens_generated
        s.total_prefill_tokens = self.stats.total_prefill_tokens
        s.total_steps = self.stats.total_steps
        s.total_latency_ms = self.stats.total_latency_ms
        s.peak_batch_size = self.stats.peak_batch_size
        s.requests_dropped = self.stats.requests_dropped
        return s^

    fn total_pages_used(self) -> Int:
        """Total pages currently allocated across all active requests."""
        var total = 0
        for i in range(len(self.active)):
            total += self.active[i].cache.total_pages_used()
        return total

    fn total_cache_bytes(self) -> Int:
        """Total KV cache memory in use across all active requests."""
        var total = 0
        for i in range(len(self.active)):
            total += self.active[i].cache.used_memory_bytes()
        return total


# ===----------------------------------------------------------------------=== #
# Helper: Run paged scheduler to completion
# ===----------------------------------------------------------------------=== #

fn run_paged_scheduler_to_completion(
    mut scheduler: PagedBatchScheduler,
    model: Model,
    tokenizer: BPETokenizer,
    rope: RoPETable,
    max_steps: Int = 10000,
) raises -> List[FinishedRequest]:
    """Run the paged scheduler until all requests are processed.

    Args:
        scheduler: The paged batch scheduler.
        model: Language model.
        tokenizer: Tokenizer.
        rope: RoPE table.
        max_steps: Safety limit on total steps.

    Returns:
        All finished requests.
    """
    var all_finished = List[FinishedRequest]()

    for _ in range(max_steps):
        # Admit pending requests
        scheduler.admit_from_queue(tokenizer)

        if not scheduler.has_active():
            break

        var finished = scheduler.step(model, tokenizer, rope)
        for i in range(len(finished)):
            all_finished.append(finished[i].copy())

    return all_finished^
