# Release Process

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


## Versioning

- Use semver.
- `MAJOR`: breaking API/runtime behavior.
- `MINOR`: backward-compatible features.
- `PATCH`: backward-compatible fixes.

## Pre-Release Checklist

Primary one-command gate:

1. `pnpm run ci:release`

Equivalent expanded checklist:

1. `pnpm run ci:naming`
2. `pnpm -r build`
3. `pnpm --dir ../packages/neutron test` (run framework tests from the Neutron TypeScript package directory)
4. `pnpm run ci:runtime-compat`
5. `pnpm run ci:deploy-presets`
6. `pnpm run ci:bench:smoke`
7. Validate docs touched by the release (`docs/*.md`).
8. Update `CHANGELOG.md`.
9. Confirm security/support policy docs are current (`SECURITY.md`, `SUPPORT.md`).

Naming policy references:

- `docs/system-naming.md`
- `docs/core/architecture-map.md`
- `docs/core/naming-release-checklist.md`

## Changelog Format

Use sections:

- `Added`
- `Changed`
- `Fixed`
- `Performance`
- `Breaking` (only when needed)

Each release entry should include date and version.

## Tagging

1. Bump package versions.
2. Commit version + changelog updates.
3. Create git tag: `vX.Y.Z`.
4. Publish packages.

## Support Policy

- `MAJOR` line receives security fixes for 12 months after first release.
- `MINOR` releases receive bug fixes until the next minor is released.
- Only latest `PATCH` in each supported line is maintained.

## Deprecation Policy

- Mark deprecated APIs in docs + changelog one minor before removal.
- Keep deprecated APIs for at least one minor cycle.
- Breaking removals happen only in the next `MAJOR`.
