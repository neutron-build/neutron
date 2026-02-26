# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Registry
# ===----------------------------------------------------------------------=== #

"""Multi-model registry for serving multiple models simultaneously.

Manages a collection of loaded models, each identified by a unique name.
Supports both FP32 Model and Q8 QuantizedModel instances. Handles request
routing to the correct model by name.

Usage:
    var registry = ModelRegistry()
    registry.register_fp32("llama-7b", model, tokenizer)
    registry.register_q8("llama-7b-q8", q_model, tokenizer)

    var response = registry.infer("llama-7b", request)
"""

from time import perf_counter_ns
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.q_model import QuantizedModel
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig, pipeline_generate
from neutron_mojo.nn.q_pipeline import q_pipeline_generate
from neutron_mojo.nn.bench import MemoryEstimate, estimate_memory, ModelInfo, model_info
from neutron_mojo.serve.handler import (
    InferenceRequest,
    InferenceResponse,
    make_success_response,
    make_error_response,
)
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


# ===----------------------------------------------------------------------=== #
# Deep Copy Helpers (Tensor/Model/QuantizedModel/BPETokenizer are Movable-only)
# ===----------------------------------------------------------------------=== #

fn _copy_tensor(src: Tensor[DType.float32]) -> Tensor[DType.float32]:
    """Deep-copy a tensor by creating a new one and copying all elements."""
    var dst = Tensor[DType.float32](src.shape)
    for i in range(src.numel()):
        dst.set(i, src.get(i))
    return dst^


fn _copy_model(src: Model) -> Model:
    """Deep-copy a Model by copying all tensor data."""
    var m = Model(src.params.copy())
    for i in range(src.embed.numel()):
        m.embed.set(i, src.embed.get(i))
    for i in range(src.final_norm.numel()):
        m.final_norm.set(i, src.final_norm.get(i))
    for i in range(src.lm_head.numel()):
        m.lm_head.set(i, src.lm_head.get(i))
    for i in range(src.layer_weights.numel()):
        m.layer_weights.set(i, src.layer_weights.get(i))
    return m^


fn _copy_q_model(src: QuantizedModel) -> QuantizedModel:
    """Deep-copy a QuantizedModel by copying all tensor data."""
    var m = QuantizedModel(src.params.copy(), src.block_size)
    for i in range(src.embed.numel()):
        m.embed.set(i, src.embed.get(i))
    for i in range(src.final_norm.numel()):
        m.final_norm.set(i, src.final_norm.get(i))
    for i in range(src.lm_head.numel()):
        m.lm_head.set(i, src.lm_head.get(i))
    for i in range(src.layer_weights.numel()):
        m.layer_weights.set(i, src.layer_weights.get(i))
    for i in range(src.layer_scales.numel()):
        m.layer_scales.set(i, src.layer_scales.get(i))
    return m^


fn _copy_tokenizer(src: BPETokenizer) -> BPETokenizer:
    """Deep-copy a BPETokenizer."""
    var tok = BPETokenizer()
    for i in range(src.vocab_size):
        _ = tok.add_token(src.id_to_token[i])
    # Copy merge rules
    for i in range(len(src.merges)):
        tok.add_merge(src.merges[i].left, src.merges[i].right)
    tok.bos_id = src.bos_id
    tok.eos_id = src.eos_id
    tok.unk_id = src.unk_id
    tok.pad_id = src.pad_id
    return tok^


# ===----------------------------------------------------------------------=== #
# Model Entry — Wraps a loaded model with its tokenizer and metadata
# ===----------------------------------------------------------------------=== #

struct ModelEntry(Copyable, Movable):
    """A registered model with its tokenizer and metadata.

    Supports either FP32 or Q8 model (indicated by is_quantized flag).
    Only one of fp32_model/q8_model is valid based on the flag.
    """
    var name: String
    var is_quantized: Bool
    var fp32_model: Model
    var q8_model: QuantizedModel
    var tokenizer: BPETokenizer
    var info: ModelInfo
    var memory: MemoryEstimate
    var total_requests: Int
    var total_tokens_generated: Int

    fn __init__(out self, name: String, var model: Model, var tokenizer: BPETokenizer):
        """Create an FP32 model entry."""
        self.name = name
        self.is_quantized = False
        self.info = model_info(model.params)
        self.memory = estimate_memory(model.params)
        self.fp32_model = model^
        # Create dummy Q8 model (won't be used)
        var dummy_params = ModelParams()
        dummy_params.num_layers = 1
        dummy_params.vocab_size = 2
        dummy_params.hidden_dim = 2
        dummy_params.num_q_heads = 1
        dummy_params.num_kv_heads = 1
        dummy_params.head_dim = 2
        dummy_params.ffn_dim = 2
        self.q8_model = QuantizedModel(dummy_params)
        self.tokenizer = tokenizer^
        self.total_requests = 0
        self.total_tokens_generated = 0

    fn __init__(out self, name: String, var q_model: QuantizedModel,
                var tokenizer: BPETokenizer, params: ModelParams):
        """Create a Q8 model entry."""
        self.name = name
        self.is_quantized = True
        self.info = model_info(params)
        self.memory = estimate_memory(params, bytes_per_param=1)
        # Create dummy FP32 model (won't be used)
        var dummy_params = ModelParams()
        dummy_params.num_layers = 1
        dummy_params.vocab_size = 2
        dummy_params.hidden_dim = 2
        dummy_params.num_q_heads = 1
        dummy_params.num_kv_heads = 1
        dummy_params.head_dim = 2
        dummy_params.ffn_dim = 2
        self.fp32_model = Model(dummy_params)
        self.q8_model = q_model^
        self.tokenizer = tokenizer^
        self.total_requests = 0
        self.total_tokens_generated = 0

    fn __copyinit__(out self, existing: Self):
        self.name = existing.name
        self.is_quantized = existing.is_quantized
        self.fp32_model = _copy_model(existing.fp32_model)
        self.q8_model = _copy_q_model(existing.q8_model)
        self.tokenizer = _copy_tokenizer(existing.tokenizer)
        self.info = existing.info.copy()
        self.memory = existing.memory.copy()
        self.total_requests = existing.total_requests
        self.total_tokens_generated = existing.total_tokens_generated

    fn __moveinit__(out self, deinit other: Self):
        self.name = other.name^
        self.is_quantized = other.is_quantized
        self.fp32_model = other.fp32_model^
        self.q8_model = other.q8_model^
        self.tokenizer = other.tokenizer^
        self.info = other.info.copy()
        self.memory = other.memory.copy()
        self.total_requests = other.total_requests
        self.total_tokens_generated = other.total_tokens_generated


# ===----------------------------------------------------------------------=== #
# Registry Entry Info (lightweight, copyable metadata)
# ===----------------------------------------------------------------------=== #

struct RegistryEntryInfo(Copyable, Movable):
    """Lightweight info about a registered model (for listing)."""
    var name: String
    var is_quantized: Bool
    var total_params_millions: Float64
    var model_memory_mb: Float64
    var total_requests: Int

    fn __init__(out self, name: String, is_quantized: Bool,
                total_params_millions: Float64, model_memory_mb: Float64,
                total_requests: Int):
        self.name = name
        self.is_quantized = is_quantized
        self.total_params_millions = total_params_millions
        self.model_memory_mb = model_memory_mb
        self.total_requests = total_requests

    fn __copyinit__(out self, existing: Self):
        self.name = existing.name
        self.is_quantized = existing.is_quantized
        self.total_params_millions = existing.total_params_millions
        self.model_memory_mb = existing.model_memory_mb
        self.total_requests = existing.total_requests

    fn __moveinit__(out self, deinit other: Self):
        self.name = other.name^
        self.is_quantized = other.is_quantized
        self.total_params_millions = other.total_params_millions
        self.model_memory_mb = other.model_memory_mb
        self.total_requests = other.total_requests

    fn summary(self) -> String:
        var kind = String("FP32")
        if self.is_quantized:
            kind = String("Q8")
        return self.name + " [" + kind + "] " + String(self.total_params_millions) + "M params, " + String(Int(self.model_memory_mb)) + " MB, " + String(self.total_requests) + " requests"


# ===----------------------------------------------------------------------=== #
# Model Registry
# ===----------------------------------------------------------------------=== #

struct ModelRegistry(Movable):
    """Registry for managing multiple loaded models.

    Each model is identified by a unique name string. Supports both
    FP32 and Q8 models. Routes inference requests by model name.
    """
    var entries: List[ModelEntry]
    var default_model: String     # Name of default model for unnamed requests

    fn __init__(out self):
        self.entries = List[ModelEntry]()
        self.default_model = String("")

    fn __moveinit__(out self, deinit other: Self):
        self.entries = other.entries^
        self.default_model = other.default_model^

    fn register_fp32(mut self, name: String, var model: Model,
                     var tokenizer: BPETokenizer):
        """Register an FP32 model.

        Args:
            name: Unique model name.
            model: FP32 language model.
            tokenizer: BPE tokenizer for this model.
        """
        var entry = ModelEntry(name, model^, tokenizer^)
        self.entries.append(entry^)

        # First registered model becomes default
        if len(self.entries) == 1:
            self.default_model = name

    fn register_q8(mut self, name: String, var q_model: QuantizedModel,
                   var tokenizer: BPETokenizer, params: ModelParams):
        """Register a Q8 quantized model.

        Args:
            name: Unique model name.
            q_model: Q8 quantized model.
            tokenizer: BPE tokenizer for this model.
            params: Model architecture parameters.
        """
        var entry = ModelEntry(name, q_model^, tokenizer^, params)
        self.entries.append(entry^)

        if len(self.entries) == 1:
            self.default_model = name

    fn set_default(mut self, name: String):
        """Set the default model for unnamed requests.

        Args:
            name: Model name to use as default.
        """
        self.default_model = name

    fn count(self) -> Int:
        """Number of registered models."""
        return len(self.entries)

    fn has_model(self, name: String) -> Bool:
        """Check if a model with given name is registered."""
        for i in range(len(self.entries)):
            if self.entries[i].name == name:
                return True
        return False

    fn _find_index(self, name: String) -> Int:
        """Find index of model by name. Returns -1 if not found."""
        for i in range(len(self.entries)):
            if self.entries[i].name == name:
                return i
        return -1

    fn list_models(self) -> List[RegistryEntryInfo]:
        """List all registered models with metadata.

        Returns:
            List of RegistryEntryInfo for each model.
        """
        var result = List[RegistryEntryInfo]()
        for i in range(len(self.entries)):
            result.append(RegistryEntryInfo(
                self.entries[i].name,
                self.entries[i].is_quantized,
                self.entries[i].info.total_params_millions,
                self.entries[i].memory.model_mb(),
                self.entries[i].total_requests,
            ))
        return result^

    fn infer(mut self, model_name: String,
             request: InferenceRequest) raises -> InferenceResponse:
        """Route an inference request to the named model.

        Args:
            model_name: Name of the model to use. If empty, uses default.
            request: Inference request.

        Returns:
            InferenceResponse with generated text or error.
        """
        var name = model_name
        if len(name) == 0:
            name = self.default_model

        var idx = self._find_index(name)
        if idx < 0:
            return make_error_response(
                "Model not found: " + name, request.request_id
            )

        var cfg = request.to_pipeline_config()

        var prompt_ids = self.entries[idx].tokenizer.encode_with_special(
            request.prompt, add_bos=True
        )
        var prompt_token_count = len(prompt_ids)

        var start = Int(perf_counter_ns())

        var text: String
        if self.entries[idx].is_quantized:
            text = q_pipeline_generate(
                self.entries[idx].q8_model,
                self.entries[idx].tokenizer,
                request.prompt, cfg,
            )
        else:
            text = pipeline_generate(
                self.entries[idx].fp32_model,
                self.entries[idx].tokenizer,
                request.prompt, cfg,
            )

        var elapsed_ns = Int(perf_counter_ns()) - start
        var elapsed_ms = Int(Float64(elapsed_ns) / 1_000_000.0)
        var gen_tokens = request.max_tokens
        var tps: Float64 = 0.0
        if elapsed_ns > 0:
            tps = Float64(gen_tokens) / (Float64(elapsed_ns) / 1_000_000_000.0)

        # Update stats
        self.entries[idx].total_requests += 1
        self.entries[idx].total_tokens_generated += gen_tokens

        return make_success_response(
            text, request.request_id, gen_tokens,
            prompt_token_count, elapsed_ms, tps,
        )

    fn get_model_info(self, name: String) -> ModelInfo:
        """Get architecture info for a named model.

        Args:
            name: Model name.

        Returns:
            ModelInfo (default if not found).
        """
        var idx = self._find_index(name)
        if idx >= 0:
            return self.entries[idx].info.copy()
        return ModelInfo()

    fn get_memory_estimate(self, name: String) -> MemoryEstimate:
        """Get memory estimate for a named model.

        Args:
            name: Model name.

        Returns:
            MemoryEstimate (default if not found).
        """
        var idx = self._find_index(name)
        if idx >= 0:
            return self.entries[idx].memory.copy()
        return MemoryEstimate()
