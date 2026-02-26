# Neutron Mojo Strategy

## Identity

Neutron Mojo is the Mojo implementation focused on AI/ML compute in the Neutron ecosystem.
It is not a web framework and not a general Python replacement.

## Current Reality (February 2026)

- Core package (`neutron-mojo`) is substantial and implemented.
- Split packages (`neutron-mojo-infer`, `neutron-mojo-python`) are still scaffolds.
- Runtime is CPU-first SIMD today; dedicated Mojo GPU kernel path is pending.
- Serving exists at helper/protocol level, not yet as a full production HTTP runtime.

## Source of Truth

When status conflicts across docs, trust the code layout first:

- `src/neutron_mojo/*` for implemented features
- `test/*` for behavior coverage
- `mojoproject.toml` for build/runtime constraints

## What Is Implemented in Core

- Tensor primitives: dtype, shape, storage, ops, SIMD math
- Model IO: GGUF, SafeTensors, binary reader, JSON parser
- Inference stack: attention, KV cache variants, tokenizer, generation pipelines
- Quantization: Q8, Q4, FP8, NF4, mixed paths
- Graph/fusion: graph IR, e-graph, rewrite engine, executor path
- Serve primitives: handlers, schedulers, registry, protocol, HTTP formatting helpers
- Training utilities: autograd, losses, optimizers, modules, LoRA support
- Python interop in core (`neutron_mojo/python/*`)

## Gaps to Close

### Priority 1: Productization

- Implement complete OpenAI-compatible HTTP runtime (not just formatting/parsing helpers)
- Tighten request parsing and error handling behavior in serve layer
- Provide stable CLI workflows for run/serve/bench paths

### Priority 2: Packaging Clarity

- Keep current monolith behavior explicit in docs
- Defer hard split of `-infer` and `-python` until there is a real consumer boundary
- When split starts, move code by module, keep compatibility shims during transition

### Priority 3: Performance Path

- Add real GPU kernel track once Mojo GPU toolchain path is stable
- Add reproducible benchmark baselines on real models and publish methodology
- Expand optimization pass from analysis-oriented flow to extracted optimized execution graphs

## Package Strategy (Near Term)

- Treat `neutron-mojo` as the canonical implementation package.
- Treat `neutron-mojo-infer` and `neutron-mojo-python` as reserved package names.
- Do not duplicate logic into scaffold packages until boundary and ownership are clear.

## Non-Goals (Current Phase)

- Building a web framework in Mojo
- Replacing Python ecosystem packages
- Premature package decomposition that increases maintenance cost without user benefit

## Execution Order

1. Keep docs accurate to code reality.
2. Remove or resolve known placeholders and stale messaging.
3. Harden serve/runtime surfaces.
4. Add GPU track when the toolchain can support maintainable implementations.
5. Split packages only after real API boundaries are exercised in production use.
