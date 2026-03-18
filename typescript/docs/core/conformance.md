# Neutron Conformance

Conformance ensures all Neutron implementations remain compatible at the framework contract level.

## Goals

1. One mental model for developers
2. One AI-readable framework corpus
3. Predictable cross-implementation behavior

## Conformance Areas

1. Router behavior (static, dynamic, catch-all, precedence)
2. Data flow (loader/action timing and payload shape)
3. Middleware composition and ordering
4. Error propagation and status semantics
5. Cache and revalidation guarantees
6. Adapter output and runtime protocol expectations

## Matrix Shape

Track support by implementation:

- Neutron TypeScript
- Neutron Rust
- Neutron Zig
- Neutron Mojo

Each item should be marked:

- `supported`
- `partial`
- `not-supported`

## Release Requirement

Any change to core contract behavior should update:

1. Core contract docs
2. Conformance matrix
3. Migration notes
