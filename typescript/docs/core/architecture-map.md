# Neutron Architecture Map

This map defines how the full ecosystem is partitioned so naming, ownership, and docs stay stable as implementations evolve.

## Layer Model

1. Platform layer: `Neutron`
2. Implementation layer: `Neutron TypeScript`, `Neutron Rust`, `Neutron Zig`, `Neutron Mojo`
3. Shared subsystem layer: `Nucleus`

Rule: one artifact name maps to one layer. Do not merge layer names into a compound artifact.

## Responsibilities

| Layer | Owns | Does Not Own |
|---|---|---|
| Neutron (platform) | Cross-language framework contract, conformance model, shared terminology | Language-specific build/runtime details |
| Neutron TypeScript | TS web framework runtime, CLI, adapters, TS packages | Rust-only runtime internals, Mojo kernels, Nucleus engine internals |
| Neutron Rust | Rust runtime/services/CLI implementation | TS runtime internals, Nucleus ownership |
| Neutron Mojo | Mojo compute/inference/training implementation | TS router/runtime, Rust server framework ownership |
| Nucleus | Shared data/control plane services and database engine | Renaming or owning implementation identities |

## Naming Boundary Rules

1. Product/docs identity:
   - `Neutron` for umbrella references
   - `Neutron <Language>` for implementation references
2. Package/crate/module identity:
   - keep implementation names and subsystem names separate
   - allowed composition is at architecture-doc level, not artifact-name level
3. Disallowed naming:
   - `neutron-typescript-mojo-nucleus`
   - `neutron-rust-mojo-nucleus`

## Mixed-Stack Naming

When one implementation uses another backend, keep names separate:

- Product: `Neutron TypeScript`
- Runtime note: `Uses Mojo acceleration backend`
- Data/control note: `Uses Nucleus services`

## Docs Topology

- Unified contract docs: `docs/core/*`
- Implementation docs:
  - `docs/typescript/*`
  - `docs/rust/*`
  - `docs/zig/*`
  - `docs/mojo/*`

All implementation docs should link back to core contract + naming policy.
