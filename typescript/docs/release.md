# Release Process

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


## Versioning

- Use semver. Before 1.0, a minor version may introduce breaking changes; document
them explicitly and require consumers to migrate before updating.
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
4. Push the tag to `origin` (Forgejo). The push mirrors to GitHub, which triggers `typescript-publish.yml`. That workflow validates the **build + test gate** (scoped to `./packages/*`, on Node 24). It does not publish npm packages; a green validation run is not publication evidence.

## Publishing (must be done locally)

The npm account requires interactive two-factor authentication for publication.
No npm publishing token is configured for this workflow. After release validation
passes, publish locally from an authenticated session:

```bash
npm login --auth-type=web        # approve in browser with your security key
cd typescript
pnpm publish -r --access public --no-git-checks
```

For a release where the CLI depends on a new scaffolder version and the
scaffolder generates new CLI pins, stage all new tarballs with `--tag next`.
Verify every version and dependency in the registry before promoting `latest`;
promote the scaffolder last so newly generated projects can resolve every pin.
Record published package versions and registry integrity values separately from
the validation workflow result.

- `pnpm publish -r` skips any version already on the registry, so it is safe to
  re-run if it stops partway (e.g. an OTP prompt lapses mid-run).
- After publishing, the npm registry can lag for several minutes — `npm view`
  may report a just-published package as missing. Confirm against the registry's
  `versions` map (`https://registry.npmjs.org/<pkg>`), not just `npm view`.
- Workspace dependency protocols are rewritten during pack/publish. Inspect the
  tarball metadata: `workspace:^` becomes a compatible version range, while
  `workspace:*` becomes the concrete version. Publish dependencies before their
  consumers, and publish scaffolders only after every generated pin resolves.

## Support Policy

- `MAJOR` line receives security fixes for 12 months after first release.
- `MINOR` releases receive bug fixes until the next minor is released.
- Only latest `PATCH` in each supported line is maintained.

## Deprecation Policy

- Mark deprecated APIs in docs + changelog one minor before removal.
- Keep deprecated APIs for at least one minor cycle.
- Breaking removals happen only in the next `MAJOR`.
