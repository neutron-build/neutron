# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Model Registry Tests
# ===----------------------------------------------------------------------=== #

"""Tests for the multi-model registry.

Tests:
1. Empty registry creation
2. Register FP32 model
3. Register Q8 model
4. has_model checks
5. count tracks registered models
6. list_models returns metadata
7. Default model set by first registration
8. set_default changes default
9. Infer routes to FP32 model
10. Infer routes to Q8 model
11. Infer with empty name uses default
12. Infer with unknown model returns error
13. get_model_info returns correct info
14. get_memory_estimate returns correct estimate
15. Multiple models coexist
16. RegistryEntryInfo summary formatting
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.q_model import QuantizedModel, quantize_from_model
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.bench import estimate_memory, model_info
from neutron_mojo.serve.handler import InferenceRequest
from neutron_mojo.serve.registry import (
    ModelEntry,
    RegistryEntryInfo,
    ModelRegistry,
)


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


fn assert_eq(a: Int, b: Int, msg: String) raises:
    if a != b:
        raise Error("FAIL: " + msg + " expected=" + String(b) + " got=" + String(a))


# ===----------------------------------------------------------------------=== #
# Test Helpers
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny FP32 model with non-trivial weights."""
    var p = tiny_test_params()
    var model = Model(p)

    for v in range(p.vocab_size):
        for d in range(p.hidden_dim):
            model.embed.set(v * p.hidden_dim + d, Float32(v * p.hidden_dim + d) * 0.01)
            model.lm_head.set(v * p.hidden_dim + d, Float32(v + d) * 0.1)

    for i in range(p.num_layers * model.layer_size):
        model.layer_weights.set(i, Float32(i % 13) * 0.02 - 0.12)

    for layer in range(p.num_layers):
        var off = model._layer_offsets(layer)
        for i in range(p.hidden_dim):
            model.layer_weights.set(off.attn_norm + i, 1.0)
            model.layer_weights.set(off.ffn_norm + i, 1.0)

    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    """Build a minimal tokenizer for testing."""
    var tok = BPETokenizer()
    _ = tok.add_token("<s>")     # 0
    _ = tok.add_token("</s>")   # 1
    _ = tok.add_token("<unk>")  # 2
    _ = tok.add_token("a")      # 3
    _ = tok.add_token("b")      # 4
    _ = tok.add_token("c")      # 5
    _ = tok.add_token("d")      # 6
    _ = tok.add_token("e")      # 7
    tok.bos_id = 0
    tok.eos_id = 1
    tok.unk_id = 2
    return tok^


# ===----------------------------------------------------------------------=== #
# Registry Tests
# ===----------------------------------------------------------------------=== #

fn test_empty_registry() raises:
    """Empty registry has count 0 and no models."""
    var reg = ModelRegistry()

    assert_eq(reg.count(), 0, "empty registry count")
    assert_true(not reg.has_model("anything"), "empty has no models")
    assert_true(len(reg.default_model) == 0, "empty has no default")

    print("  empty_registry: PASS")


fn test_register_fp32() raises:
    """Register an FP32 model."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    reg.register_fp32("test-fp32", model^, tok^)

    assert_eq(reg.count(), 1, "count after register")
    assert_true(reg.has_model("test-fp32"), "has_model finds it")
    assert_true(not reg.has_model("nonexistent"), "has_model rejects unknown")

    print("  register_fp32: PASS")


fn test_register_q8() raises:
    """Register a Q8 quantized model."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()

    reg.register_q8("test-q8", qm^, tok^, params)

    assert_eq(reg.count(), 1, "count after Q8 register")
    assert_true(reg.has_model("test-q8"), "has_model finds Q8")

    print("  register_q8: PASS")


fn test_has_model() raises:
    """has_model returns correct results."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()

    assert_true(not reg.has_model("m1"), "no model before register")
    reg.register_fp32("m1", model^, tok^)
    assert_true(reg.has_model("m1"), "model found after register")
    assert_true(not reg.has_model("m2"), "other model not found")

    print("  has_model: PASS")


fn test_count() raises:
    """count tracks number of registered models."""
    var reg = ModelRegistry()

    assert_eq(reg.count(), 0, "initially 0")

    var m1 = _build_tiny_model()
    var t1 = _build_tiny_tokenizer()
    reg.register_fp32("m1", m1^, t1^)
    assert_eq(reg.count(), 1, "after first register")

    var m2 = _build_tiny_model()
    var t2 = _build_tiny_tokenizer()
    reg.register_fp32("m2", m2^, t2^)
    assert_eq(reg.count(), 2, "after second register")

    print("  count: PASS")


fn test_list_models() raises:
    """list_models returns metadata for all registered models."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    reg.register_fp32("alpha", model^, tok^)

    var m2 = _build_tiny_model()
    var qm = quantize_from_model(m2, block_size=2)
    var t2 = _build_tiny_tokenizer()
    var params = tiny_test_params()
    reg.register_q8("beta-q8", qm^, t2^, params)

    var infos = reg.list_models()
    assert_eq(len(infos), 2, "list has 2 entries")
    assert_true(infos[0].name == "alpha", "first model name")
    assert_true(not infos[0].is_quantized, "first is FP32")
    assert_true(infos[1].name == "beta-q8", "second model name")
    assert_true(infos[1].is_quantized, "second is Q8")

    print("  list_models: PASS")


fn test_default_model_first_registered() raises:
    """First registered model becomes default."""
    var reg = ModelRegistry()
    var m1 = _build_tiny_model()
    var t1 = _build_tiny_tokenizer()
    reg.register_fp32("first-model", m1^, t1^)

    assert_true(reg.default_model == "first-model", "first model is default")

    var m2 = _build_tiny_model()
    var t2 = _build_tiny_tokenizer()
    reg.register_fp32("second-model", m2^, t2^)

    assert_true(reg.default_model == "first-model", "default unchanged by second")

    print("  default_model_first_registered: PASS")


fn test_set_default() raises:
    """set_default changes the default model."""
    var reg = ModelRegistry()
    var m1 = _build_tiny_model()
    var t1 = _build_tiny_tokenizer()
    reg.register_fp32("m1", m1^, t1^)

    var m2 = _build_tiny_model()
    var t2 = _build_tiny_tokenizer()
    reg.register_fp32("m2", m2^, t2^)

    assert_true(reg.default_model == "m1", "initially first model")
    reg.set_default("m2")
    assert_true(reg.default_model == "m2", "default changed to m2")

    print("  set_default: PASS")


fn test_infer_fp32() raises:
    """Infer routes to FP32 model and returns valid response."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    reg.register_fp32("fp32-model", model^, tok^)

    var req = InferenceRequest("abc")
    req.max_tokens = 3

    var resp = reg.infer("fp32-model", req)
    assert_true(not resp.is_error(), "FP32 infer no error")
    assert_true(resp.elapsed_ms >= 0, "FP32 infer has elapsed time")

    print("  infer_fp32: PASS")


fn test_infer_q8() raises:
    """Infer routes to Q8 model and returns valid response."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var qm = quantize_from_model(model, block_size=2)
    var tok = _build_tiny_tokenizer()
    var params = tiny_test_params()
    reg.register_q8("q8-model", qm^, tok^, params)

    var req = InferenceRequest("abc")
    req.max_tokens = 3

    var resp = reg.infer("q8-model", req)
    assert_true(not resp.is_error(), "Q8 infer no error")
    assert_true(resp.elapsed_ms >= 0, "Q8 infer has elapsed time")

    print("  infer_q8: PASS")


fn test_infer_default() raises:
    """Infer with empty model name uses default."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    reg.register_fp32("my-model", model^, tok^)

    var req = InferenceRequest("abc")
    req.max_tokens = 3

    var resp = reg.infer(String(""), req)
    assert_true(not resp.is_error(), "default infer no error")

    print("  infer_default: PASS")


fn test_infer_unknown_model() raises:
    """Infer with unknown model name returns error."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    reg.register_fp32("known", model^, tok^)

    var req = InferenceRequest("abc")
    req.max_tokens = 3

    var resp = reg.infer("unknown-model", req)
    assert_true(resp.is_error(), "unknown model is error")
    assert_true(resp.error.find("not found") >= 0, "error mentions not found")

    print("  infer_unknown_model: PASS")


fn test_get_model_info() raises:
    """get_model_info returns correct architecture info."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    reg.register_fp32("info-test", model^, tok^)

    var info = reg.get_model_info("info-test")
    var p = tiny_test_params()
    assert_eq(info.num_layers, p.num_layers, "info layers match")
    assert_eq(info.hidden_dim, p.hidden_dim, "info hidden_dim match")
    assert_eq(info.vocab_size, p.vocab_size, "info vocab match")

    # Unknown model returns default
    var unk_info = reg.get_model_info("no-such-model")
    assert_eq(unk_info.num_layers, 0, "unknown model returns default info")

    print("  get_model_info: PASS")


fn test_get_memory_estimate() raises:
    """get_memory_estimate returns correct memory breakdown."""
    var reg = ModelRegistry()
    var model = _build_tiny_model()
    var tok = _build_tiny_tokenizer()
    reg.register_fp32("mem-test", model^, tok^)

    var mem = reg.get_memory_estimate("mem-test")
    assert_true(mem.total_bytes > 0, "memory has total bytes")
    assert_true(mem.model_params_bytes > 0, "memory has model params")

    # Unknown model returns default
    var unk_mem = reg.get_memory_estimate("no-such-model")
    assert_eq(unk_mem.total_bytes, 0, "unknown model returns 0 memory")

    print("  get_memory_estimate: PASS")


fn test_multiple_models() raises:
    """Multiple FP32 and Q8 models coexist and route correctly."""
    var reg = ModelRegistry()

    # Register 2 FP32 + 1 Q8
    var m1 = _build_tiny_model()
    var t1 = _build_tiny_tokenizer()
    reg.register_fp32("fp32-a", m1^, t1^)

    var m2 = _build_tiny_model()
    var t2 = _build_tiny_tokenizer()
    reg.register_fp32("fp32-b", m2^, t2^)

    var m3 = _build_tiny_model()
    var qm = quantize_from_model(m3, block_size=2)
    var t3 = _build_tiny_tokenizer()
    var params = tiny_test_params()
    reg.register_q8("q8-a", qm^, t3^, params)

    assert_eq(reg.count(), 3, "3 models registered")
    assert_true(reg.has_model("fp32-a"), "fp32-a found")
    assert_true(reg.has_model("fp32-b"), "fp32-b found")
    assert_true(reg.has_model("q8-a"), "q8-a found")

    # All can infer
    var req = InferenceRequest("abc")
    req.max_tokens = 2

    var r1 = reg.infer("fp32-a", req)
    assert_true(not r1.is_error(), "fp32-a infers ok")

    var r2 = reg.infer("fp32-b", req)
    assert_true(not r2.is_error(), "fp32-b infers ok")

    var r3 = reg.infer("q8-a", req)
    assert_true(not r3.is_error(), "q8-a infers ok")

    print("  multiple_models: PASS")


fn test_registry_entry_info_summary() raises:
    """RegistryEntryInfo summary formatting works."""
    var info = RegistryEntryInfo("test-model", False, 7.5, 28.6, 42)
    var s = info.summary()

    assert_true(s.find("test-model") >= 0, "summary has name")
    assert_true(s.find("FP32") >= 0, "summary has FP32 type")
    assert_true(s.find("42") >= 0, "summary has request count")

    var q_info = RegistryEntryInfo("q-model", True, 3.2, 12.1, 0)
    var qs = q_info.summary()
    assert_true(qs.find("Q8") >= 0, "Q8 summary has Q8 type")

    print("  registry_entry_info_summary: PASS")


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_registry:")

    test_empty_registry()
    test_register_fp32()
    test_register_q8()
    test_has_model()
    test_count()
    test_list_models()
    test_default_model_first_registered()
    test_set_default()
    test_infer_fp32()
    test_infer_q8()
    test_infer_default()
    test_infer_unknown_model()
    test_get_model_info()
    test_get_memory_estimate()
    test_multiple_models()
    test_registry_entry_info_summary()

    print("ALL PASSED (16 tests)")
