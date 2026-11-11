# Publishing the TypeScript packages

All packages under `typescript/packages/*` publish together from the workspace
root. `pnpm publish -r` publishes every package whose version is not yet on the
registry, in dependency order, and skips the rest, so a package is released by
bumping its `version`. `workspace:^` dependency ranges are rewritten to the real
version at publish time.

## Source versions

The versions in this source tree are listed below. This table does not establish
that a version has been published; check the registry before releasing.

| Package | Version |
|---|---|
| `@neutron-build/sql` | 0.2.1 |
| `@neutron-build/nucleus` | 0.3.1 |
| `@neutron-build/data` | 0.3.1 |
| `@neutron-build/agents` | 0.3.0 |
| `@neutron-build/core` | 0.5.4 |
| `@neutron-build/cli` | 0.5.4 |
| `@neutron-build/create` | 0.1.14 |
| `@neutron-build/auth` | 0.1.7 |
| `@neutron-build/cache-redis` | 0.2.1 |
| `@neutron-build/security` | 0.1.6 |
| `@neutron-build/ai` | 0.1.3 |
| `@neutron-build/mcp` | 0.1.2 |
| `@neutron-build/workflow` | 0.1.2 |
| `@neutron-build/ops` | 0.1.5 |
| `@neutron-build/otel` | 0.1.5 |
| `@neutron-build/cms` | 0.1.3 |

`@neutron-build/mail` is at 0.2.0.
What changed in each package is in `CHANGELOG.md`. Check the registry for what is
live: `npm view <package> version`.

The former unscoped `create-neutron` package is deprecated. Do not publish new
versions under that name. Version 0.1.11 of `@neutron-build/create` is published
with core 0.5.0 and cli 0.5.0. `create` pins the `core`/`cli` pair (`^0.5.0`), and
the packed `cli` depends on `create`; core 0.5.x patches stay inside that range, so
they publish alone.

`auth`, `cache-redis`, `ops`, `otel` and `security` depend on `core` through
`workspace:^`, which packs as `^<core version>`. When core moves to a new 0.x
minor, republish them (patch bump) so their range admits it; otherwise an app on
the new core installs a second copy of core under them.

## Publish

Publishing is the owner's step. Two routes:

**From CI (preferred):** run the `Validate TypeScript Release` workflow by hand
(`workflow_dispatch`) with `publish` ticked. It builds and tests, then publishes
every package whose version is not on the registry through npm trusted
publishing (OIDC): no npm token is stored anywhere, and each tarball carries a
provenance attestation tied to the workflow run. One-time setup per package on
npmjs.com: Settings → Trusted publishing → GitHub Actions, repository
`neutron-build/neutron`, workflow `typescript-publish.yml`. Once every package
publishes this way, set each package's publishing access to "require two-factor
authentication and disallow tokens". Pushing a `ts/vX.Y.Z` tag only validates; it
does not publish.

**Locally** (fallback; works with 2FA):

```bash
cd typescript
pnpm install --frozen-lockfile
pnpm --filter "./packages/*" run build
npm login --auth-type=web
pnpm publish -r --access public --no-git-checks
```

If a publish stops partway, fix the cause and run it again: versions already on
the registry are skipped.

## Verify

```bash
for p in @neutron-build/create @neutron-build/core @neutron-build/cli @neutron-build/data \
         @neutron-build/nucleus @neutron-build/sql; do
  echo "$p $(npm view $p version)"; done
```

Before publishing, `pnpm publish -r --access public --no-git-checks --dry-run`
lists exactly what would go out and in what order, and
`pnpm pack --pack-destination <dir>` in a package shows its tarball (check the
packed `package.json` has no `workspace:` ranges).

## Rules that keep a release consistent

- npm versions are immutable. A change to a published package needs a new
  version; metadata (description, keywords, engines) cannot be edited afterwards.
- Bump a package whenever its code or dependencies changed, and its dependents
  follow only when a range has to move. Publishing `cli` requires the matching
  `core` and `@neutron-build/create` in the same pass, or the packed `cli` cannot
  resolve them.
- `@neutron-build/create` pins the released `core` and `cli` pair for external
  projects (`src/scaffold.ts`, and the same numbers in `src/index.test.ts`).
  Update both when either version moves.
- The Go CLI has its own tag (`cli/vX.Y.Z`, `.github/workflows/cli.yml`); the
  Go SDK module is tagged `go/vX.Y.Z`. Neither is published by this procedure.
