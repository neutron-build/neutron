# Neutron Mojo — Vision & Roadmap

## The Pitch

Neutron Mojo is the **open-source Mojo framework** — lean at its core, expandable by design. The same philosophy as Neutron TypeScript: minimal core, maximum capability through composable modules.

Mojo is positioned to inherit Python's ecosystem. Neutron Mojo is positioned to be the framework that makes that transition seamless — providing native Mojo performance where it matters, with instant Python fallback for everything else.

**One line:** The Blender to MAX's Maya. The Next.js of Mojo.

---

## What We Already Have (37 sprints)

```
neutron-mojo/
  tensor/     — Custom tensor, shape, SIMD kernels, DType system
  nn/         — Full transformer inference stack (40+ modules)
  quant/      — Q8, Q4, FP8, NF4 quantization formats
  fusion/     — Graph IR, e-graph optimizer, rewrite rules, pattern matching
  io/         — GGUF, SafeTensors, JSON, binary reader
  model/      — Weight loading, config, population
  serve/      — Request handling, scheduling, batching, registry, protocol
  dlpack/     — DLPack C ABI for zero-copy tensor exchange with Python
  cli/        — Command-line inference runner
```

This is already the most comprehensive pure-Mojo ML library that exists. But it's currently ML-only. The vision is broader.

---

## Architecture: Lean Core + Modules

```
                    ┌─────────────────────────────┐
                    │         neutron-mojo         │
                    │         (lean core)          │
                    ├─────────────────────────────┤
                    │  tensor  │  io  │  python    │
                    │  shape   │  fmt │  bridge    │
                    │  simd    │  fs  │  dlpack    │
                    └────┬─────┴──┬───┴─────┬─────┘
                         │        │         │
          ┌──────────────┼────────┼─────────┼──────────────┐
          │              │        │         │              │
     ┌────▼────┐   ┌─────▼───┐ ┌─▼──────┐ ┌▼──────┐  ┌───▼────┐
     │   nn    │   │  serve  │ │ fusion │ │ quant │  │  web   │
     │         │   │         │ │        │ │       │  │        │
     │ models  │   │ http    │ │ graph  │ │ q8/q4 │  │ routes │
     │ attn    │   │ batch   │ │ egraph │ │ fp8   │  │ ssr    │
     │ kv      │   │ sched   │ │ rules  │ │ nf4   │  │ api    │
     │ lora    │   │ stream  │ │ exec   │ │ mixed │  │ static │
     └─────────┘   └─────────┘ └────────┘ └───────┘  └────────┘
```

Each module is optional. Import only what you need. Zero overhead for unused features.

---

## The Python Bridge (Our Killer Feature)

Mojo already has `from python import Python`. But raw Python interop is clunky. We make it seamless:

### What Mojo Gives Us Natively
```mojo
from python import Python

fn use_numpy() raises:
    var np = Python.import_module("numpy")
    var arr = np.array([1, 2, 3])
    print(arr.shape)
```

### What We Build On Top

**1. Tensor Bridge** (partially done via DLPack)
```mojo
from neutron_mojo.python import to_numpy, from_numpy, to_torch, from_torch

# Zero-copy exchange with Python ML ecosystem
var mojo_tensor = Tensor[DType.float32](Shape(3, 4))
var np_array = to_numpy(mojo_tensor)        # zero-copy via DLPack
var pt_tensor = to_torch(mojo_tensor)       # zero-copy via DLPack
var back = from_numpy(np_array)             # zero-copy back
```

**2. Python Package Proxy**
```mojo
from neutron_mojo.python import py

# Use any Python package with Mojo syntax sugar
var plt = py.matplotlib.pyplot
var pd = py.pandas
var requests = py.requests

# Transparent: if Neutron has a native Mojo impl, use it
# Otherwise, delegate to Python automatically
var data = py.json.loads('{"key": "value"}')  # Uses our native JSON parser
var html = py.jinja2.render(template, ctx)    # Falls through to Python
```

**3. HuggingFace Bridge**
```mojo
from neutron_mojo.python.hf import load_model, load_tokenizer

# Download from HF Hub (Python), load into Mojo tensors (native)
var model = load_model("meta-llama/Llama-3-8B", quantize="q8")
var tokenizer = load_tokenizer("meta-llama/Llama-3-8B")
var output = mixed_pipeline_generate(model, tokenizer, "Hello", config)
```

### Borrow From
- **PyO3** (Rust) — ergonomic Python FFI patterns
- **pyo3-numpy** — zero-copy NumPy interop (we do this via DLPack)
- **Mojo's own Python interop** — `PythonObject` wrapping
- **cxx** (Rust-C++ bridge) — type-safe FFI design patterns

---

## Module Roadmap

### Phase 1: Complete the ML Stack (Current → Near Term)

What we have vs. what's missing for a complete inference engine:

| Component | Status | Borrow From |
|-----------|--------|-------------|
| Tensor + SIMD kernels | Done (8 kernels) | — |
| Quantization (Q8/Q4/FP8/NF4/Mixed) | Done | — |
| KV Cache (5 variants) | Done | — |
| Attention (GQA, fused, paged, sliding) | Done | — |
| Serving (scheduler, batching) | Done | — |
| Graph IR + E-Graph optimizer | Done | egg (Willsey 2021) |
| **Graph executor** | Missing | TVM, ONNX Runtime |
| **GPU kernels** | Missing | Triton, FlashAttention |
| **Multi-architecture models** | Partial (Llama only) | llama.cpp model registry |
| **ONNX import** | Missing | onnxruntime |
| **Real model benchmarks** | Missing | llama.cpp benchmarks |

**Borrow from:**
- **llama.cpp** — Model architecture support patterns (Mistral, Phi, Gemma, Qwen). Their `llama_model_loader` handles dozens of architectures with clean dispatch
- **vLLM** — PagedAttention (we have it), continuous batching patterns (we have it), speculative decoding orchestration
- **TVM/Apache TVM** — Graph-level optimization passes, operator fusion strategies, schedule tuning
- **FlashAttention** — Memory-efficient attention algorithm (when we add GPU)
- **Triton** — GPU kernel patterns for matmul, softmax, attention
- **GGML/GGUF** — Quantization format handling (we already support this)
- **ExecuTorch** — Lightweight inference runtime design (lean, embeddable)

### Phase 2: Python Bridge (High Impact, Medium Effort)

Make Mojo feel like "Python but fast" by bridging the gap:

| Component | Effort | Borrow From |
|-----------|--------|-------------|
| `to_numpy` / `from_numpy` (DLPack) | Small (DLPack exists) | pyo3-numpy |
| `to_torch` / `from_torch` | Small | DLPack |
| HuggingFace Hub loader | Medium | huggingface_hub Python SDK |
| Python package proxy | Medium | Mojo's PythonObject |
| Matplotlib plotting bridge | Small | Direct Python import |
| Pandas data bridge | Small | Direct Python import |

**Key insight:** We don't re-implement pandas or matplotlib in Mojo. We make it trivial to pass data between Mojo tensors and Python objects. Mojo handles compute, Python handles ecosystem.

**Borrow from:**
- **PyO3** — Ergonomic Python↔Rust bridge patterns (trait-based conversion, error handling)
- **maturin** — Build system for Python packages with native extensions
- **pybind11** — C++↔Python bridge (class wrapping patterns)

### Phase 3: Web / API Layer (The Next.js Play)

Neutron TypeScript is a web framework. Neutron Mojo can serve the same role:

| Component | Effort | Borrow From |
|-----------|--------|-------------|
| HTTP server (native Mojo) | Medium | Mojo stdlib (when available) |
| JSON API routes | Small (have JSON parser) | FastAPI patterns |
| Request routing | Medium | Express/Hono patterns |
| Middleware chain | Small | Koa/Hono |
| Static file serving | Small | — |
| WebSocket support | Medium | — |
| SSE (Server-Sent Events) | Small | — |
| OpenAPI spec generation | Medium | FastAPI |

**Why this matters:** ML models need serving. Right now we have a text protocol over stdin/stdout. A native HTTP server makes Neutron Mojo a self-contained deployment platform — load model, serve API, no Python/Node intermediary.

**Borrow from:**
- **FastAPI** — Type-safe API design, auto OpenAPI docs, dependency injection
- **Hono** — Minimal core, middleware patterns, multi-runtime (maps to our "lean core" philosophy)
- **Starlette** — ASGI patterns, streaming responses
- **Neutron TypeScript** — SSR patterns, islands architecture
- **Axum** (Rust) — Type-safe extractors, tower middleware

### Phase 4: GPU Acceleration (The Performance Leap)

Mojo's GPU stdlib is landing (25.7+). When it stabilizes:

| Component | Effort | Borrow From |
|-----------|--------|-------------|
| GPU matmul kernel | High | Triton, CUTLASS |
| GPU attention kernel | High | FlashAttention-2/3 |
| GPU RMSNorm/Softmax | Medium | Triton tutorials |
| GPU quantized matmul | High | GPTQ, AWQ kernels |
| Memory manager (GPU) | Medium | vLLM, CUDA malloc |
| CPU↔GPU transfer | Medium | Mojo GPU stdlib |

**Borrow from:**
- **Triton** — Python-like GPU kernel authoring (Mojo's GPU model is similar)
- **FlashAttention** — Tiled attention algorithm, IO-awareness
- **CUTLASS** — High-performance GEMM patterns
- **ThunderKittens** — Simplified GPU kernel patterns
- **Mojo stdlib GPU** — Native GPU programming constructs

### Phase 5: Developer Experience (The Framework Feel)

| Component | Effort | Borrow From |
|-----------|--------|-------------|
| `neutron init` scaffolding | Small | `create-next-app`, `cargo init` |
| `neutron dev` hot reload | Medium | Vite, Neutron TS CLI |
| `neutron serve` model server | Small (have CLI) | Ollama |
| `neutron bench` benchmarking | Small (have bench module) | — |
| `neutron convert` model format | Medium | llama.cpp `convert.py` |
| Package manager integration | Medium | pixi, magic |
| REPL / notebook support | Medium | Mojo notebooks |

**Borrow from:**
- **Ollama** — Dead-simple model serving UX (`ollama run llama3`)
- **Cargo** (Rust) — Project scaffolding, build system, test runner
- **Vite** — Dev server, hot reload, plugin system
- **Neutron TS CLI** — Our own patterns

---

## What We Can Build Fast (Quick Wins)

These leverage existing code and are 1-2 sprint efforts each:

### 1. Python Tensor Bridge (Sprint-sized)
We have DLPack structs. Wire them to Mojo's Python interop:
- `to_numpy()` / `from_numpy()` — NumPy array ↔ Neutron Tensor
- `to_torch()` / `from_torch()` — PyTorch tensor ↔ Neutron Tensor
- Zero-copy where possible, explicit copy where needed

### 2. HuggingFace Loader (Sprint-sized)
Use Python interop to call `huggingface_hub`:
- `hf_download(repo_id, filename)` → local path
- `hf_load_model(repo_id, quantize)` → Model or QuantizedModel
- `hf_load_tokenizer(repo_id)` → BPETokenizer
- Wraps Python's HF Hub SDK, loads into native Mojo structs

### 3. Graph Executor (Sprint-sized)
We have the IR and optimizer. Add execution:
- `GraphExecutor.run(graph, inputs)` → outputs
- Maps OpKind to kernel dispatch (SIMD matmul, attention, etc.)
- Enables: trace model → optimize graph → execute optimized version

### 4. Multi-Architecture Support (2-3 sprints)
Our components already cover most architectures:
- Mistral = Llama + sliding window (have sliding window)
- Phi = Llama + partial rotary (minor RoPE tweak)
- Gemma = Llama + different norms (minor)
- Qwen = Llama + different tokenizer (have tokenizer framework)
- Architecture registry: name → config → forward function

### 5. HTTP API Server (2 sprints)
Replace text protocol with real HTTP:
- Use Python's `http.server` or `uvicorn` via bridge initially
- Native Mojo HTTP when stdlib supports it
- `/v1/chat/completions` — OpenAI-compatible API
- `/v1/models` — model listing
- SSE streaming for token-by-token output

---

## Competitive Positioning

```
                    Ease of Use
                        ▲
                        │
              Neutron   │   Ollama
              Mojo ★    │     ●
                        │
         ───────────────┼──────────────► Performance
                        │
              HF        │   MAX
           Transformers │     ●
                ●       │
                        │   llama.cpp
                        │     ●
```

- **vs. MAX** — We're open, they're closed. We're transparent, they're a black box. We have Python fallback, they have Python API. We compete on openness, not raw speed.
- **vs. llama.cpp** — We're Mojo-native (cleaner than C++), have graph optimization (they don't), have Python bridge (they have Python bindings). They're ahead on GPU and model breadth.
- **vs. HF Transformers** — We're fast by default (Mojo, not Python). They have ecosystem breadth. Our Python bridge lets users access their ecosystem while getting our performance.
- **vs. Ollama** — They're a distribution tool, we're a framework. We could be what Ollama is built on.

---

## Design Principles (from Neutron TypeScript)

1. **Lean core** — Core provides tensor, I/O, and Python bridge. Everything else is a module.
2. **No magic** — Every abstraction is readable. No hidden codegen, no opaque runtime.
3. **Opt-in complexity** — Start with `pipeline_generate()`. Add quantization when you need it. Add batching when you scale. Add GPU when you need speed.
4. **Python is a feature, not a crutch** — Use Python ecosystem freely. Replace with native Mojo when the native version is better.
5. **Progressive disclosure** — Simple things are simple, complex things are possible.

---

## Sprint Priorities (Suggested Order)

| Priority | Module | Impact | Effort |
|----------|--------|--------|--------|
| 1 | Python tensor bridge (DLPack wiring) | Unlocks entire Python ML ecosystem | 1 sprint |
| 2 | HuggingFace model loader | Real model loading without manual download | 1 sprint |
| 3 | Graph executor (wire fusion→execution) | Optimized inference from graph IR | 1-2 sprints |
| 4 | HTTP API server (OpenAI-compatible) | Production serving | 2 sprints |
| 5 | Multi-architecture (Mistral, Phi, Gemma) | Model breadth | 2-3 sprints |
| 6 | `neutron` CLI unification | Developer experience | 1 sprint |
| 7 | GPU matmul + attention | Performance leap | 3-4 sprints |
| 8 | Web framework layer | Full-stack Mojo | 3-4 sprints |

---

## File Count & Scope Today

- **72 source files** across 9 packages
- **90+ test files**, 250+ passing tests
- **37 completed sprints**
- **Packages:** tensor, nn, quant, fusion, io, model, serve, dlpack, cli

The foundation is real. The roadmap is clear. Ship it.
