# neutron-mojo-infer

LLM inference engine — FlashAttention, KV cache, quantization, serving.

Part of the [tystack](https://github.com/tylerbarron/tystack) ecosystem.

## Status

**Pre-1.0 reserved split package (scaffold only).**

This package name is intentionally reserved for a future code split.
Current inference implementation lives in:

- `../neutron-mojo/src/neutron_mojo/nn/`
- `../neutron-mojo/src/neutron_mojo/serve/`
- `../neutron-mojo/src/neutron_mojo/model/`

Until a real external consumer boundary exists, `neutron-mojo` remains the
canonical package.

## Dependencies

- `neutron-mojo` (core)
