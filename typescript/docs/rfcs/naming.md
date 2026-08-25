# RFC: Naming Convention Across Neutron

- Status: Accepted (amended 2026-05-28 — see "Reality note" below)
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

1. Do not combine the platform name `Neutron` and the subsystem name `Nucleus` in one artifact name. (The org scope `@neutron-build` is a brand/org identifier, not the platform name itself; a package such as `@neutron-build/nucleus` is therefore acceptable — the artifact's package name is `nucleus`, distinct from `neutron` at the platform layer. On Cargo, the framework's integration crate `neutron-nucleusdb` is likewise acceptable — it is named after its dependency artifact `nucleusdb`, exactly as `neutron-redis` is named after `redis`.)
2. Do not combine multiple implementation labels (`typescript`, `rust`, `zig`, `mojo`) in one artifact name.
3. Keep implementation package prefixes as:
   - npm: `@neutron-build/*` (plus `create-neutron` for the project generator) — see Reality note below
   - Cargo: `neutron` / `neutron-*`
   - Mojo projects: `neutron-mojo-*`
4. Keep subsystem package prefixes as:
   - npm: `@neutron-build/nucleus` (the subsystem *client*) — see Reality note below
   - Cargo (engine artifact): `nucleusdb` / `nucleus-*` — see Reality note below
   - Cargo (framework integration crate): `neutron-nucleusdb`, named after its dependency
   - Mojo: `nucleus-*`

## Reality note (npm name availability)

The canonical bare names `neutron` and `nucleus` are both owned on npm by unrelated third-party authors (`neutron` by `leo@zeit.co` since 2.x; `nucleus` by `diffference@gmail.com`) and have been for years. The RFC's literal aspiration to publish under those bare names — or under a free `@neutron` / `@nucleus` scope — is therefore not actionable without acquiring those names from the current owners.

The amended npm rules above use the `@neutron-build/*` org scope, which:

- Preserves the layer separation in spirit by treating `@neutron-build` as an org/brand identifier (not the platform name `Neutron`).
- Keeps the artifact package name within the scope adherent to the layer rule (e.g. `@neutron-build/nucleus` for the Nucleus *client* — a distinct artifact from the subsystem itself).
- Reserves `create-neutron` (unscoped) as the only required unscoped name (npm's `npm create <name>` convention demands a package literally named `create-<name>`).

~~Cargo and Mojo prefixes are unaffected — bare `neutron` / `nucleus` remain available in those ecosystems.~~ **Correction (2026-08-23): that claim was false for Cargo.** Both bare names are taken on crates.io too: `neutron` by a dormant Pulsar client (last publish 2024-04), `nucleus` by an abandoned 2020 Minecraft datapack generator. `nucleus-db` is additionally taken by an active third-party STM32 toolchain (since 2026-06). Decisions, verified free at correction time:

- The engine (`nucleus/`) publishes as **`nucleusdb`** — product "Nucleus", crate `nucleusdb`, the same pattern as `duckdb`/`surrealdb`. The in-repo package name is already `nucleusdb`; the lib name and the server binary remain `nucleus`.
- The framework's Nucleus integration crate is **`neutron-nucleusdb`** (dependency-named, like `neutron-redis`).
- Mojo is unaffected.

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
