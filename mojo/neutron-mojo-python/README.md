# neutron-mojo-python

Python interop — DLPack bridge, torch.compile backend, weight loading.

Part of the [tystack](https://github.com/tylerbarron/tystack) ecosystem.

## Status

**Pre-1.0 reserved split package (scaffold only).**

This package name is intentionally reserved for a future code split.
Current Python interop implementation lives in:

- `../neutron-mojo/src/neutron_mojo/python/`

Until a real packaging boundary is exercised by downstream consumers,
`neutron-mojo` remains the canonical implementation package.

## Dependencies

- `neutron-mojo` (core)
- PyTorch, JAX, NumPy (runtime-optional)
