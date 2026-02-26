# RFC: Naming Convention Across Neutron

- Status: Accepted
- Date: 2026-02-20
- Owners: Neutron maintainers

## Context

The ecosystem has multiple implementations and one shared core subsystem. Naming drift has made artifact ownership and docs boundaries unclear.

## Decision

Adopt the following canonical naming model:

1. `Neutron` is the umbrella platform name.
2. `Neutron TypeScript`, `Neutron Rust`, `Neutron Zig`, and `Neutron Mojo` are implementation names.
3. `Nucleus` is the shared core subsystem name.
4. Artifact names must represent one layer only (platform, implementation, or subsystem).

## Rules

1. Do not combine `neutron` and `nucleus` in one artifact name.
2. Do not combine multiple implementation labels (`typescript`, `rust`, `zig`, `mojo`) in one artifact name.
3. Keep implementation package prefixes as:
   - npm: `neutron`, `neutron-*`, `@neutron/*`
   - Cargo: `neutron` / `neutron-*`
   - Mojo projects: `neutron-mojo-*`
4. Keep subsystem package prefixes as:
   - npm: `@nucleus/*`
   - Cargo/Mojo: `nucleus-*`

## Documentation Model

1. Each implementation has standalone docs and release notes.
2. Shared cross-implementation behavior lives in unified docs under `docs/core/`.
3. Implementation docs link to shared docs for contract-level behavior.

## Enforcement

Naming and workspace graph integrity are CI-gated:

1. `pnpm run ci:naming`
2. `pnpm run ci:workspace`

Snapshot updates are explicit:

1. `pnpm run ci:workspace:snapshot`

## Consequences

Benefits:

1. Clear ownership boundaries for code and docs.
2. Better package discoverability and stable naming over time.
3. Faster onboarding for developers and AI tooling through consistent terminology.

Tradeoff:

1. Teams must follow stricter naming checks when introducing new artifacts.
