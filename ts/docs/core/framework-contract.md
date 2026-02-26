# Neutron Framework Contract

This document is the language-agnostic contract for Neutron.

## Scope

The contract should define consistent semantics for:

1. Routing and route matching
2. Nested layouts and composition
3. Loaders/actions/form flow
4. Middleware lifecycle
5. Error boundaries and HTTP error semantics
6. Caching and invalidation behavior
7. Build output guarantees
8. Adapter/runtime boundary expectations

## Rule

If a behavior is framework-level and expected to be portable across implementations, it belongs in this contract.

If behavior is specific to one implementation toolchain/runtime, it belongs in that implementation's docs.

## Current Status

This file is a scaffold anchor and should be expanded into a versioned contract (`Core Spec v0.x`).
