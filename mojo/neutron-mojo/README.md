# Neutron Mojo

This repository is the Neutron Mojo implementation within the broader Neutron ecosystem.

AI/ML compute framework in Mojo with tensor primitives, model loading, quantization,
inference, training utilities, and serving helpers.

Part of the [Neutron](https://github.com/neutron-build/neutron) ecosystem.

## Status

**Pre-1.0, active implementation**.

Current code state in this workspace:

- `neutron-mojo` (core): implemented (`112` source `.mojo` files, `125` test files)
- `neutron-mojo-infer`: scaffolding only (package split deferred)
- `neutron-mojo-python`: scaffolding only (package split deferred)
- Runtime is currently CPU-first SIMD; dedicated Mojo GPU kernel path is not implemented yet

## Packages

- `neutron-mojo` (this package) — Core tensor, kernel, layout, fusion, backend, runtime
- `neutron-mojo-infer` — Reserved split package name (currently scaffold-only)
- `neutron-mojo-python` — Reserved split package name (currently scaffold-only)

## What Works Today

- Tensor ops, shape/dtype system, SIMD math kernels
- GGUF and SafeTensors parsing/loading
- Transformer inference pipelines (FP32, Q8, Q4, mixed paths)
- Quantization formats (Q8/Q4/NF4/FP8)
- Serving primitives (request/response, scheduling, text protocol, HTTP formatting helpers)
- Training/autograd utilities and LoRA fine-tuning components

## Known Gaps

- No production-grade HTTP server runtime yet (current HTTP module is helper-level)
- Package split is not complete (`-infer` and `-python` are still stubs)
- No Mojo GPU kernel implementation in-tree yet

## Tooling

- `max >= 25.1` (see `mojoproject.toml`)
- Mojo toolchain availability is required to run tests/benchmarks

## Validation

- PowerShell: `pwsh mojo/scripts/validate-core.ps1`
- Bash: `bash mojo/scripts/validate-core.sh`
- List-only inventory mode:
  - `pwsh mojo/scripts/validate-core.ps1 -ListOnly`
  - `bash mojo/scripts/validate-core.sh --list-only`
- CI workflow: `.github/workflows/mojo-validation.yml`
