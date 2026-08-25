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
3. `pnpm --dir packages/neutron test` (run framework tests from the Neutron TypeScript package directory)
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
3. Create git tag: `ts/vX.Y.Z` (the `ts/` prefix scopes the tag to the TypeScript implementation; the `typescript-publish.yml` workflow fires on `ts/v*`). Other implementations use parallel prefixes: e.g. `rust/v*`, `nucleus/v*`, `cli/v*`.
4. Push the tag to `origin` (Forgejo). The push mirrors to GitHub, which triggers `typescript-publish.yml`. That workflow runs the **build + test gate** (scoped to `./packages/*`, on Node 24) — but see the publishing note below: the `publish` step cannot complete in CI.

## Publishing (must be done locally)

The npm account enforces interactive two-factor auth for publishing, which a CI
token cannot satisfy — the workflow's `publish` step fails with `EOTP`. So the
build/test job is the gate, and the actual publish is run locally:

```bash
npm login --auth-type=web        # approve in browser with your security key
cd typescript
pnpm publish -r --access public --no-git-checks
```

- `pnpm publish -r` skips any version already on the registry, so it is safe to
  re-run if it stops partway (e.g. an OTP prompt lapses mid-run).
- After publishing, the npm registry can lag for several minutes — `npm view`
  may report a just-published package as missing. Confirm against the registry's
  `versions` map (`https://registry.npmjs.org/<pkg>`), not just `npm view`.
- `workspace:*` cross-package deps are rewritten to the concrete version on
  publish (verified: `@neutron-build/data` ships `@neutron-build/nucleus: "0.1.0"`).

## Support Policy

- `MAJOR` line receives security fixes for 12 months after first release.
- `MINOR` releases receive bug fixes until the next minor is released.
- Only latest `PATCH` in each supported line is maintained.

## Deprecation Policy

- Mark deprecated APIs in docs + changelog one minor before removal.
- Keep deprecated APIs for at least one minor cycle.
- Breaking removals happen only in the next `MAJOR`.
