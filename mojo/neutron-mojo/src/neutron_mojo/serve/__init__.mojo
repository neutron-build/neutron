# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Serving Package
# ===----------------------------------------------------------------------=== #

"""Model serving: request/response types, handler, and protocol.

Provides a serving API for model inference that can be used via:
- Direct function calls (handler.mojo)
- Stdin/stdout text protocol (protocol.mojo)
- Batch file processing (handler.mojo)

When Mojo adds native socket support, an HTTP adapter can wrap
the handler functions without changing the core serving logic.
"""

from .handler import (
    InferenceRequest,
    InferenceResponse,
    make_success_response,
    make_error_response,
    handle_inference_request,
    handle_q8_inference_request,
    handle_batch_requests,
    handle_q8_batch_requests,
    handle_conversation_request,
)
from .protocol import (
    parse_request_line,
    format_response,
    parse_request_block,
)
from .scheduler import (
    BatchEntry,
    FinishedRequest,
    RequestQueue,
    QueuedRequest,
    SchedulerStats,
    BatchScheduler,
    run_scheduler_to_completion,
)
from .registry import (
    ModelEntry,
    RegistryEntryInfo,
    ModelRegistry,
)
from .paged_scheduler import (
    PagedBatchEntry,
    PagedBatchScheduler,
    run_paged_scheduler_to_completion,
)
from .http import (
    ChatMessage as HTTPChatMessage,
    ChatCompletionRequest,
    ChatCompletionResponse,
    format_chat_response,
    format_models_response,
    format_health_response,
    format_error_response,
    format_sse_event,
    format_sse_done,
    parse_chat_request,
)
