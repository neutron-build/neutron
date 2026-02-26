# Naming Release Checklist

Use this checklist before shipping any new package/crate/module or major docs update.

## Required Gate

1. Run `pnpm run ci:naming` from `neutron/`.
2. Run `pnpm run ci:workspace` from `neutron/`.
3. Run `pnpm run ci:mirror-sync` from `neutron/` (when `../packages/neutron` mirror exists).
4. Ensure `pnpm run ci:release` passes.

## Artifact Naming Checks

1. The artifact name maps to exactly one layer:
   - platform (`Neutron`) OR
   - implementation (`Neutron <Language>`) OR
   - subsystem (`Nucleus`)
2. No artifact name combines `neutron` and `nucleus`.
3. No artifact name combines multiple implementation labels in one token.
4. Ecosystem prefix rules are followed:
   - npm: `neutron`, `neutron-*`, `@neutron/*`, `@nucleus/*`
   - Cargo: `neutron`/`neutron-*` or `nucleus`/`nucleus-*`
   - Mojo project names: `neutron-mojo-*` or `nucleus-*`

## Docs Naming Checks

1. Public docs use:
   - `Neutron` (umbrella)
   - `Neutron TypeScript` / `Neutron Rust` / `Neutron Zig` / `Neutron Mojo`
2. Implementation docs include an explicit identity line:
   - `This is the Neutron <Language> implementation.`
3. `Nucleus` references indicate role (data plane, control plane, or shared runtime substrate).

## Examples

- Good:
  - `Neutron TypeScript` with note: `uses Nucleus services`
  - package `@neutron/security`
  - crate `neutron-cli`
- Bad:
  - `neutron-typescript-mojo-nucleus`
  - `neutron-rust-mojo-nucleus`
