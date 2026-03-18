# Neutron System Naming

This document defines the canonical naming model for the full Neutron system.
Ratified policy source: `docs/rfcs/naming.md`.

## Problem

The current naming can blur these boundaries:

- product umbrella (`Neutron`)
- TypeScript framework implementation (this repo area)
- polyglot engines/runtime backends (Rust, Zig, Mojo)
- shared data/control-plane concepts (`Nucleus`)

## Canonical Model

- `Neutron`:
  umbrella platform and brand. This is the product family name and shared framework contract.
- `Neutron TypeScript`:
  the TypeScript web framework implementation.
- `Neutron Rust`, `Neutron Zig`, `Neutron Mojo`:
  language-specific runtime/compiler/engine implementations.
- `Nucleus`:
  shared core services and state plane (for example, data/control primitives).

## Artifact Naming Matrix

- Product/marketing:
  - `Neutron` (umbrella)
  - `Neutron TypeScript`, `Neutron Rust`, `Neutron Zig`, `Neutron Mojo` (implementations)
- TypeScript packages (npm):
  - framework core: `neutron`
  - implementation extensions: `@neutron/*`
  - subsystem integrations: `@nucleus/*` (when published as separate packages)
- Rust crates (cargo):
  - implementation crates: `neutron-*`
  - subsystem crates: `nucleus-*`
- Docs:
  - implementation docs: `neutron/docs/<language>/`
  - unified contract docs: `neutron/docs/core/`

## Layer Rule (Critical)

Use one name per architectural layer. Do not combine layers into one product/package name.

- Platform layer: `Neutron`
- Implementation layer: `Neutron <Language>`
- Shared subsystem layer: `Nucleus`

Examples:

- Correct: `Neutron TypeScript` + `Nucleus`
- Correct: `Neutron Rust` + `Nucleus`
- Incorrect: `neutron-typescript-mojo-nucleus`
- Incorrect: `neutron-rust-mojo-nucleus`

Reason:

- Combined names hide ownership boundaries.
- They become unstable when internals change (for example replacing Mojo).
- They make docs and package discovery harder for developers and AI tooling.

## Naming Rules

1. Brand layer:
   always user-facing as `Neutron`.
2. Implementation layer:
   always `<Neutron + Language>` in docs/releases.
3. Package/crate/module layer:
   must include ecosystem-native prefixes and be explicit.
4. One artifact name should encode one axis only:
   implementation OR subsystem, not both.

## Recommended Package Naming

TypeScript (npm):

- Keep app-framework package as `neutron`.
- Keep scoped extensions as `@neutron/*` (`@neutron/auth`, `@neutron/security`, etc.).
- Avoid introducing new unscoped packages besides `neutron`, `neutron-cli`, `create-neutron`, `neutron-data`.
- If TypeScript uses Nucleus services, keep package names separate (`@neutron/*` and `@nucleus/*`), not merged.

Rust (cargo):

- Use `neutron` / `neutron-*` crate names for framework/runtime crates.
- Use `nucleus` / `nucleus-*` crate names for shared plane/core services.
- Do not publish crates that mix both prefixes in one crate name.

Zig/Mojo:

- Use `neutron-zig-*` and `neutron-mojo-*` for implementation packages/modules.
- Reserve `nucleus-*` names for shared core/plane artifacts only.
- If Mojo is an accelerator used by another implementation, document it as a dependency, not as a rename.

## How To Name Mixed Stacks

When one implementation uses another runtime/backend, keep the implementation name stable and describe the backend in architecture docs.

- Product name: `Neutron TypeScript`
- Architecture note: `Execution acceleration via Mojo backend`
- Shared plane note: `Uses Nucleus control/data services`

## Repo and Docs Naming

1. Each language implementation should have an explicit identity line in its root README:
   `This is the Neutron <Language> implementation.`
2. Cross-language docs should use:
   - `Neutron (umbrella)`
   - `Neutron TypeScript` / `Neutron Rust` / `Neutron Zig` / `Neutron Mojo`
3. Any `Nucleus` references should explicitly state whether they are:
   - data plane
   - control plane
   - shared runtime substrate

## Docs Model (Required)

- Each implementation must be usable alone and must keep its own docs track.
- The ecosystem must also maintain one unified contract docs track for shared concepts:
  - routing/data contract
  - compatibility/conformance
  - protocol and integration boundaries
- Implementation docs must link back to unified docs instead of duplicating core definitions.

## Immediate Adoption Plan

1. Add explicit implementation identity sections to all language READMEs.
2. Add a single cross-language glossary doc at the org root.
3. Align package/crate names to the rules above before publishing 1.0.
4. Keep aliases only for migration windows, with deprecation dates.

## Governance Automation

Naming and workspace graph gates:

1. `pnpm run ci:naming`
2. `pnpm run ci:workspace`
