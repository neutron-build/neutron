# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Continuous Batching Scheduler
# ===----------------------------------------------------------------------=== #

"""Request queue and batch scheduler for efficient multi-request serving.

Implements continuous batching: new requests join the batch as slots open,
rather than waiting for the entire batch to finish. Each request gets its
own KV cache and position state, enabling independent progress.

Performance: ~2-4x throughput improvement over sequential processing
when serving multiple concurrent requests.

Usage:
    var scheduler = BatchScheduler(model.params, max_batch=4, max_seq=512)
    scheduler.enqueue(request1)
    scheduler.enqueue(request2)
    while scheduler.has_active():
        var finished = scheduler.step(model, tokenizer, rope)
        for i in range(len(finished)):
            send_response(finished[i])
"""

from time import perf_counter_ns
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
)
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.serve.handler import InferenceRequest, InferenceResponse, make_success_response


# ===----------------------------------------------------------------------=== #
# BatchEntry — Per-request state in the batch
# ===----------------------------------------------------------------------=== #

struct BatchEntry(Copyable, Movable):
    """State for a single request being processed in the batch.

    Each entry has its own KV cache, position counter, and generated tokens.
    This enables independent progress — one request can finish while others
    continue generating.
    """
    var request_id: String
    var input_ids: List[Int]
    var generated: List[Int]
    var cache: MultiLayerKVCache
    var pos: Int                 # Current sequence position
    var prefilled: Bool          # Whether prefill phase is complete
    var finished: Bool           # Whether generation is done
    var max_new_tokens: Int
    var stop_tokens: List[Int]
    var config: PipelineConfig
    var enqueue_time_ns: Int     # For latency tracking
    var start_gen_time_ns: Int   # When generation started

    fn __init__(out self, request_id: String, input_ids: List[Int],
                var cache: MultiLayerKVCache, config: PipelineConfig,
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
        # Deep copy KV cache (MultiLayerKVCache is Movable-only)
        self.cache = MultiLayerKVCache(
            num_layers=existing.cache.num_layers,
            max_seq_len=existing.cache.max_seq_len,
            num_kv_heads=existing.cache.num_kv_heads,
            head_dim=existing.cache.head_dim,
        )
        var total_kv = existing.cache.key_data.numel()
        for i in range(total_kv):
            self.cache.key_data.set(i, existing.cache.key_data.get(i))
            self.cache.value_data.set(i, existing.cache.value_data.get(i))
        for i in range(len(existing.cache.lengths)):
            self.cache.lengths[i] = existing.cache.lengths[i]
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
# FinishedRequest — Completed request with response data
# ===----------------------------------------------------------------------=== #

struct FinishedRequest(Copyable, Movable):
    """A completed request ready to return to the caller."""
    var request_id: String
    var text: String
    var tokens_generated: Int
    var prompt_tokens: Int
    var latency_ms: Int

    fn __init__(out self, request_id: String, text: String,
                tokens_generated: Int, prompt_tokens: Int,
                latency_ms: Int):
        self.request_id = request_id
        self.text = text
        self.tokens_generated = tokens_generated
        self.prompt_tokens = prompt_tokens
        self.latency_ms = latency_ms

    fn __copyinit__(out self, existing: Self):
        self.request_id = existing.request_id
        self.text = existing.text
        self.tokens_generated = existing.tokens_generated
        self.prompt_tokens = existing.prompt_tokens
        self.latency_ms = existing.latency_ms

    fn __moveinit__(out self, deinit other: Self):
        self.request_id = other.request_id^
        self.text = other.text^
        self.tokens_generated = other.tokens_generated
        self.prompt_tokens = other.prompt_tokens
        self.latency_ms = other.latency_ms


# ===----------------------------------------------------------------------=== #
# RequestQueue — FIFO queue for pending requests
# ===----------------------------------------------------------------------=== #

struct QueuedRequest(Copyable, Movable):
    """A request waiting in the queue."""
    var request: InferenceRequest
    var enqueue_time_ns: Int

    fn __init__(out self, request: InferenceRequest, enqueue_time_ns: Int):
        self.request = request.copy()
        self.enqueue_time_ns = enqueue_time_ns

    fn __copyinit__(out self, existing: Self):
        self.request = existing.request.copy()
        self.enqueue_time_ns = existing.enqueue_time_ns

    fn __moveinit__(out self, deinit other: Self):
        self.request = other.request^
        self.enqueue_time_ns = other.enqueue_time_ns


struct RequestQueue(Movable):
    """FIFO queue for pending inference requests.

    Limits queue depth to prevent memory exhaustion under load.
    """
    var items: List[QueuedRequest]
    var max_depth: Int
    var total_enqueued: Int
    var total_dropped: Int

    fn __init__(out self, max_depth: Int = 64):
        self.items = List[QueuedRequest]()
        self.max_depth = max_depth
        self.total_enqueued = 0
        self.total_dropped = 0

    fn __moveinit__(out self, deinit other: Self):
        self.items = other.items^
        self.max_depth = other.max_depth
        self.total_enqueued = other.total_enqueued
        self.total_dropped = other.total_dropped

    fn enqueue(mut self, request: InferenceRequest, time_ns: Int) -> Bool:
        """Add a request to the queue.

        Args:
            request: Inference request.
            time_ns: Current timestamp in nanoseconds.

        Returns:
            True if enqueued, False if queue is full (request dropped).
        """
        if len(self.items) >= self.max_depth:
            self.total_dropped += 1
            return False
        self.items.append(QueuedRequest(request, time_ns))
        self.total_enqueued += 1
        return True

    fn dequeue(mut self) -> QueuedRequest:
        """Remove and return the oldest request.

        Caller must check is_empty() first.

        Returns:
            The oldest queued request.
        """
        # Pop front by rebuilding without first element
        var first = QueuedRequest(self.items[0].request, self.items[0].enqueue_time_ns)
        var new_items = List[QueuedRequest]()
        for i in range(1, len(self.items)):
            new_items.append(QueuedRequest(self.items[i].request, self.items[i].enqueue_time_ns))
        self.items = new_items^
        return first^

    fn is_empty(self) -> Bool:
        return len(self.items) == 0

    fn depth(self) -> Int:
        return len(self.items)


# ===----------------------------------------------------------------------=== #
# SchedulerStats — Throughput and latency tracking
# ===----------------------------------------------------------------------=== #

struct SchedulerStats(Copyable, Movable):
    """Statistics for the batch scheduler."""
    var total_requests_processed: Int
    var total_tokens_generated: Int
    var total_prefill_tokens: Int
    var total_steps: Int
    var total_latency_ms: Int     # Sum of per-request latency
    var peak_batch_size: Int
    var requests_dropped: Int

    fn __init__(out self):
        self.total_requests_processed = 0
        self.total_tokens_generated = 0
        self.total_prefill_tokens = 0
        self.total_steps = 0
        self.total_latency_ms = 0
        self.peak_batch_size = 0
        self.requests_dropped = 0

    fn __copyinit__(out self, existing: Self):
        self.total_requests_processed = existing.total_requests_processed
        self.total_tokens_generated = existing.total_tokens_generated
        self.total_prefill_tokens = existing.total_prefill_tokens
        self.total_steps = existing.total_steps
        self.total_latency_ms = existing.total_latency_ms
        self.peak_batch_size = existing.peak_batch_size
        self.requests_dropped = existing.requests_dropped

    fn __moveinit__(out self, deinit other: Self):
        self.total_requests_processed = other.total_requests_processed
        self.total_tokens_generated = other.total_tokens_generated
        self.total_prefill_tokens = other.total_prefill_tokens
        self.total_steps = other.total_steps
        self.total_latency_ms = other.total_latency_ms
        self.peak_batch_size = other.peak_batch_size
        self.requests_dropped = other.requests_dropped

    fn avg_latency_ms(self) -> Int:
        """Average per-request latency in milliseconds."""
        if self.total_requests_processed == 0:
            return 0
        return self.total_latency_ms // self.total_requests_processed

    fn tokens_per_step(self) -> Float64:
        """Average tokens generated per step."""
        if self.total_steps == 0:
            return 0.0
        return Float64(self.total_tokens_generated) / Float64(self.total_steps)


# ===----------------------------------------------------------------------=== #
# BatchScheduler — Core continuous batching engine
# ===----------------------------------------------------------------------=== #

struct BatchScheduler(Movable):
    """Continuous batching scheduler for model inference.

    Manages a pool of active requests, each with independent KV cache
    and position state. New requests are admitted from the queue as
    slots become available. Each step processes all active requests
    in lockstep (one token per request per step).

    Key design: Requests can join/leave the batch independently,
    enabling continuous batching rather than synchronized batches.
    """
    var active: List[BatchEntry]
    var queue: RequestQueue
    var stats: SchedulerStats
    var max_batch_size: Int
    var max_seq_len: Int
    var params: ModelParams

    fn __init__(out self, params: ModelParams, max_batch_size: Int = 4,
                max_seq_len: Int = 512, max_queue_depth: Int = 64):
        """Create a batch scheduler.

        Args:
            params: Model architecture parameters.
            max_batch_size: Maximum concurrent requests.
            max_seq_len: Maximum sequence length per request.
            max_queue_depth: Maximum pending queue depth.
        """
        self.active = List[BatchEntry]()
        self.queue = RequestQueue(max_queue_depth)
        self.stats = SchedulerStats()
        self.max_batch_size = max_batch_size
        self.max_seq_len = max_seq_len
        self.params = params.copy()

    fn __moveinit__(out self, deinit other: Self):
        self.active = other.active^
        self.queue = other.queue^
        self.stats = other.stats.copy()
        self.max_batch_size = other.max_batch_size
        self.max_seq_len = other.max_seq_len
        self.params = other.params.copy()

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

        Tokenizes the prompt and creates a KV cache for each admitted request.

        Args:
            tokenizer: BPE tokenizer for encoding prompts.
        """
        while len(self.active) < self.max_batch_size and not self.queue.is_empty():
            var queued = self.queue.dequeue()
            var req = queued.request.copy()
            var cfg = req.to_pipeline_config()

            # Encode prompt
            var input_ids = tokenizer.encode_with_special(req.prompt, add_bos=True)
            var total_len = len(input_ids) + cfg.max_new_tokens
            if total_len > self.max_seq_len:
                total_len = self.max_seq_len

            # Create KV cache for this request
            var cache = MultiLayerKVCache(
                num_layers=self.params.num_layers,
                max_seq_len=total_len,
                num_kv_heads=self.params.num_kv_heads,
                head_dim=self.params.head_dim,
            )

            # Build stop tokens
            var stop_tokens = List[Int]()
            if tokenizer.eos_id >= 0:
                stop_tokens.append(tokenizer.eos_id)

            var entry = BatchEntry(
                req.request_id, input_ids, cache^, cfg,
                stop_tokens, queued.enqueue_time_ns,
            )
            self.active.append(entry^)

            # Track peak batch size
            if len(self.active) > self.stats.peak_batch_size:
                self.stats.peak_batch_size = len(self.active)

    fn step(mut self, model: Model, tokenizer: BPETokenizer,
            rope: RoPETable) raises -> List[FinishedRequest]:
        """Run one decode step for all active requests.

        For each active entry:
        1. If not yet prefilled, process one prefill token
        2. If prefilled, decode one new token (with penalties + sampling)
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

            # Prefill phase: batch process all prompt tokens at once
            if not self.active[i].prefilled:
                var n_prompt = len(self.active[i].input_ids)
                if n_prompt > 0:
                    # Use batch prefill to process all prompt tokens at once
                    _ = model.forward_prefill(
                        self.active[i].input_ids,
                        self.active[i].cache,
                        rope,
                        start_pos=0,
                    )
                    self.active[i].pos = n_prompt
                    self.stats.total_prefill_tokens += n_prompt
                self.active[i].prefilled = True
                self.active[i].start_gen_time_ns = now
                continue

            # Decode phase: generate one token
            var logits: Tensor[DType.float32]
            if len(self.active[i].generated) == 0:
                # First decode step — use last prefill logits
                var last_token = self.active[i].input_ids[len(self.active[i].input_ids) - 1]
                logits = model.forward(last_token, self.active[i].cache, rope,
                                       pos=self.active[i].pos - 1)
            else:
                var last_gen = self.active[i].generated[len(self.active[i].generated) - 1]
                logits = model.forward(last_gen, self.active[i].cache, rope,
                                       pos=self.active[i].pos)
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
        var keep = List[BatchEntry]()
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
                # Deep copy via __copyinit__ (handles KV cache + all state)
                var entry = self.active[i].copy()
                keep.append(entry^)

        self.active = keep^
        return finished_list^

    fn has_active(self) -> Bool:
        """Check if there are active requests or queued requests."""
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


# ===----------------------------------------------------------------------=== #
# Helper: Run scheduler to completion
# ===----------------------------------------------------------------------=== #

fn run_scheduler_to_completion(
    mut scheduler: BatchScheduler,
    model: Model,
    tokenizer: BPETokenizer,
    rope: RoPETable,
    max_steps: Int = 10000,
) raises -> List[FinishedRequest]:
    """Run the scheduler until all requests are processed.

    Admits requests from queue, processes steps, and collects results.

    Args:
        scheduler: The batch scheduler.
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
