# Neutron Mojo

This repository is the Neutron Mojo implementation within the broader Neutron ecosystem.

A Mojo AI/ML compute library: typed tensors with SIMD kernels, GGUF/SafeTensors
model loading, block quantization, transformer inference, and training utilities.

## Status — preview, on Mojo 1.0

**Mojo 1.0 shipped 2026-08-11 and the SDK was migrated to it** (2026-08-19;
the pinned nightly toolchain no longer compiles). `mojoproject.toml` requires
`mojo >= 1.0`, `max >= 26.5`; the last recorded validation run
(`reports/core-validation-latest.md`, 2026-08-20) is **125 pass / 0 fail on
Mojo 1.0.0**. Treat it as a **preview**, not a production dependency:

- The runtime is **CPU-first SIMD**. There is no dedicated Mojo GPU kernel path
  in-tree yet, despite the ecosystem's eventual GPU ambitions.
- Serving is **helper-level only** — request/response formatting and scheduling
  primitives, not a production HTTP server runtime.
- The package split (`neutron-mojo-infer`, `neutron-mojo-python`) is **deferred**;
  those directories are scaffolding stubs. All working code lives in `neutron-mojo`.
- The 1.0 migration left open gaps with evidence in
  [`MIGRATION_GAPS.md`](MIGRATION_GAPS.md) (12 gaps, 4 residuals, ~6.8k
  deprecation warnings). APIs will keep moving; pin nothing.

Current workspace state: **112 source `.mojo` files, 125 test files.**

## What it is

| Area | What's implemented |
|------|--------------------|
| **Tensor** | `Tensor[dtype: DType]` over flat storage; shapes, dtypes, views, broadcasting |
| **Kernels** | SIMD elementwise (`add`/`sub`/`mul`/`div`), `matmul` (tiled 2D), reductions (`reduce_sum`/`max`/`mean`) |
| **Activations/norms** | `relu`, `gelu`, `silu`, `swiglu`, `sigmoid`, `tanh`, `softmax`, `rmsnorm`, `layernorm` |
| **Quantization** | Q4_0, Q4_1, Q8_0, Q4_K (S/M), NF4, FP8 (E4M3, E5M2) — quantize/dequantize + block codecs |
| **Model IO** | GGUF and SafeTensors parsing/loading (incl. sharded SafeTensors), weight readers |
| **Inference** | `Model`, `QuantizedModel`, `Q4Model`; transformer stack, RoPE, KV cache (paged + Q8), sampler, `pipeline_generate` |
| **Attention** | fused attention, sliding-window, speculative decoding, prefix cache, MoE |
| **Training** | autograd tape + backward, optimizers (`adam`, `sgd`, grad-clip, LR schedulers), losses, LoRA fine-tuning |
| **Fusion** | e-graph based op fusion (pattern/rewrite/rules), graph IR, executor |
| **Interop** | DLPack exchange, Python/PyTorch bridge, HuggingFace pipeline helpers |
| **Serving** | request/response protocol, scheduler, model registry, HTTP formatting helpers |

Only the above are implemented. Anything not listed (a GPU backend, a hardened
server) is not shipped.

## Quick example

Tensor ops mirror the benchmark harness in `benchmarks/neutron/` (verified syntax):

```mojo
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.tensor.ops import matmul, softmax, rmsnorm

fn main() raises:
    var a = Tensor[DType.float32].rand(Shape(512, 512))
    var b = Tensor[DType.float32].rand(Shape(512, 512))

    var c = matmul(a, b)          # tiled, SIMD-backed
    var p = softmax(c)            # last-axis softmax
    print(c.shape())
```

Generation is driven by `pipeline_generate` with a `PipelineConfig`:

```mojo
from neutron_mojo.nn.pipeline import PipelineConfig

var cfg = PipelineConfig()
cfg.max_new_tokens = 128
cfg.chat_template = String("llama")   # "none" | "llama" | "chatml"
cfg.use_q8_cache = True                # Q8-quantized KV cache
```

## Quantization

Quantization formats are selected via `QuantType`, with config helpers in
`quant/types.mojo`:

```mojo
from neutron_mojo.quant.types import (
    q8_0_config, q4_k_m_config, nf4_config, fp8_e4m3_config,
)
from neutron_mojo.quant.q8_0 import quantize_q8_0, dequantize_q8_0
```

- **Q8_0 / Q4_0 / Q4_1** — GGUF block quantization (block size 32)
- **Q4_K (S/M)** — GGUF K-quantization
- **NF4** — 4-bit NormalFloat (QLoRA-style)
- **FP8 (E4M3, E5M2)** — 8-bit float codecs

`QuantizedModel` and `Q4Model` load and run these directly; the pipeline supports
FP32, Q8, Q4, and mixed-precision paths.

## Build and test

This is a [pixi](https://pixi.sh)/`mojoproject.toml` workspace (`mojo >= 1.0`, `max >= 26.5`). A
working Mojo toolchain is required to build or run anything.

```bash
cd neutron-mojo

# Run one test (this is exactly what the validation harness invokes)
pixi run mojo run -I src test/test_tensor.mojo

# Run a benchmark harness
pixi run mojo run -I src ../benchmarks/neutron/bench_matmul.mojo
```

The full suite runs through the validation scripts, which discover every
`test/test_*.mojo`, execute it, and write a report under `reports/`:

```bash
bash scripts/validate-core.sh            # or: pwsh scripts/validate-core.ps1
bash scripts/validate-core.sh --list-only # inventory without running
```

CI runs the same via `.github/workflows/mojo-validation.yml`.

## Benchmarks

Benchmark harnesses live in `benchmarks/neutron/` (`bench_matmul`, `bench_softmax`,
`bench_elementwise`) with competitor references under `benchmarks/competitors/`
(PyTorch, cuDNN, vLLM, SGLang, llama.cpp, Triton).

**No performance numbers are published here.** The harnesses compute GFLOPS/latency
at runtime on your machine; this repository ships no committed benchmark results,
and the CPU-first, pre-1.0 nature of the library means any figure would be
illustrative rather than a claim. Run the harnesses yourself for numbers relevant
to your hardware.

## Architecture

```
neutron-mojo/
  src/neutron_mojo/
    tensor/     # Tensor, Shape, dtype, storage, views, ops, simd_math
    quant/      # QuantType/QuantConfig; q8_0, q4_k, nf4, fp8 codecs
    io/         # gguf, safetensors, binary_reader, json, model_export
    model/      # Model config, architecture, loader, weight_reader
    nn/         # transformer, attention, rope, kv_cache, sampler,
                #   pipeline (pipeline_generate), q_model, moe, lora, ...
    autograd/   # tape, backward, ops, checkpoint, grad_check
    optim/      # adam, sgd, grad_clip, lr_scheduler
    train/      # loop, losses, lora_train, modules, e2e
    fusion/     # egraph, pattern, rewrite, rules, executor (op fusion)
    dlpack/     # DLPack exchange
    python/     # torch_bridge, hf, hf_pipeline (Python interop)
    serve/      # scheduler, protocol, registry, http helpers
    cli/        # inference entrypoint
  test/         # 125 test_*.mojo files
neutron-mojo-infer/   # reserved split package (scaffold only)
neutron-mojo-python/  # reserved split package (scaffold only)
benchmarks/           # neutron harnesses + competitor references
reference/            # Python reference implementations (validation oracles)
```

## License

MIT — see [LICENSE](./LICENSE). Part of the
[Neutron](https://github.com/neutron-build/neutron) ecosystem.
