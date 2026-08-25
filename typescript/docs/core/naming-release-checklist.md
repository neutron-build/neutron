# Naming Release Checklist

Use this checklist before shipping any new package/crate/module or major docs update.

## Required Gate

1. Run `pnpm run ci:naming` from `typescript/`.
2. Run `pnpm run ci:workspace` from `typescript/`.
3. Ensure `pnpm run ci:release` passes.

## Artifact Naming Checks

1. The artifact name maps to exactly one layer:
   - platform (`Neutron`) OR
   - implementation (`Neutron <Language>`) OR
   - subsystem (`Nucleus`)
2. No artifact name combines `neutron` and `nucleus`.
3. No artifact name combines multiple implementation labels in one token.
4. Ecosystem prefix rules are followed:
   - npm: `@neutron-build/*` (plus `create-neutron`, the only unscoped exception — required by the `npm create <name>` convention). See Reality note in `docs/rfcs/naming.md` for why bare `neutron`/`nucleus` and the `@neutron`/`@nucleus` scopes are not used.
   - Cargo: `neutron`/`neutron-*`; Nucleus subsystem artifacts use `nucleusdb` (engine) / `nucleus-*`, and the framework's integration crate is `neutron-nucleusdb` (dependency-named, like `neutron-redis`). Bare `nucleus` is taken on crates.io — see the Reality note in `docs/rfcs/naming.md`.
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
  - package `@neutron-build/security`
  - crate `neutron-cli`
- Bad:
  - `neutron-typescript-mojo-nucleus`
  - `neutron-rust-mojo-nucleus`
