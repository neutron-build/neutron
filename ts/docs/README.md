# Neutron Docs

This docs workspace is now organized for a long-term multi-implementation framework model:

- One unified framework contract (`core/`)
- One implementation docs track per language (`typescript/`, `rust/`, `zig/`, `mojo/`)

## Unified Framework Docs

- `core/README.md`
- `core/framework-contract.md`
- `core/glossary.md`
- `core/conformance.md`
- `core/architecture-map.md`
- `core/naming-release-checklist.md`
- `system-naming.md`
- `rfcs/README.md`

## Implementation Docs

- `typescript/README.md`
- `rust/README.md`
- `zig/README.md`
- `mojo/README.md`

## Naming Quick Reference

- Use `Neutron` for the umbrella platform.
- Use `Neutron <Language>` for a standalone implementation.
- Use `Nucleus` for shared cross-implementation core services.
- Do not merge layers into one name (for example `neutron-typescript-mojo-nucleus`).
- Canonical policy is ratified in `rfcs/naming.md`.

## Existing TypeScript Docs (Legacy Flat Layout)

These pages are currently TypeScript-focused and should be migrated into `typescript/` over time:

- `api.md`
- `benchmarks.md`
- `cli.md`
- `content-collections.md`
- `create-neutron.md`
- `deployment.md`
- `enterprise.md`
- `examples.md`
- `migration.md`
- `neutron-data.md`
- `react-compat.md`
- `release.md`
- `view-transitions.md`

## Audit Snapshots

- `framework-audit-2026-02-19.md`
