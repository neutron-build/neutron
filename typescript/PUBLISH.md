# Publishing the TypeScript packages

All packages under `typescript/packages/*` publish together from the workspace
root. `pnpm publish -r` publishes every package whose version is not yet on the
registry, in dependency order, and skips the rest, so a package is released by
bumping its `version`. `workspace:^` dependency ranges are rewritten to the real
version at publish time.

## Current release

The release prepared for the ORM program, with the versions in this tree:

| Package | Version |
|---|---|
| `@neutron-build/sql` | 0.1.0 (first publish, alpha) |
| `@neutron-build/nucleus` | 0.2.0 |
| `@neutron-build/data` | 0.2.0 |
| `@neutron-build/agents` | 0.2.0 |
| `@neutron-build/core` | 0.2.3 |
| `@neutron-build/cli` | 0.2.4 |
| `create-neutron` | 0.1.6 |
| `@neutron-build/auth` | 0.1.4 |
| `@neutron-build/cache-redis` | 0.1.3 |
| `@neutron-build/security` | 0.1.3 |
| `@neutron-build/ai` | 0.1.1 |
| `@neutron-build/mcp` | 0.1.1 |
| `@neutron-build/workflow` | 0.1.1 |

`@neutron-build/ops` (0.1.2), `@neutron-build/otel` (0.1.2) and
`@neutron-build/mail` (0.1.0) are unchanged and stay on the registry as they are.
What changed in each package is in `CHANGELOG.md`. Check the registry for what is
live: `npm view <package> version`.

## Publish

Publishing is the owner's step. Two routes:

**Locally** (works with 2FA; the CI route fails with EOTP unless the token
bypasses 2FA):

```bash
cd typescript
pnpm install --frozen-lockfile
pnpm --filter "./packages/*" run build
npm login --auth-type=web
pnpm publish -r --access public --no-git-checks
```

**From CI:** push a `ts/vX.Y.Z` tag. `.github/workflows/typescript-publish.yml`
runs build and tests, then `pnpm publish -r` with `secrets.NPM_TOKEN`, which must
be an npm token that can publish `@neutron-build/*` and `create-neutron` and
bypasses 2FA.

If a publish stops partway, fix the cause and run it again: versions already on
the registry are skipped.

## Verify

```bash
for p in create-neutron @neutron-build/core @neutron-build/cli @neutron-build/data \
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
  `core` and `create-neutron` in the same pass, or the packed `cli` cannot
  resolve them.
- `create-neutron` pins the released `core` and `cli` pair for external
  projects (`src/scaffold.ts`, and the same numbers in `src/index.test.ts`).
  Update both when either version moves.
- The Go CLI has its own tag (`cli/vX.Y.Z`, `.github/workflows/cli.yml`); the
  Go SDK module is tagged `go/vX.Y.Z`. Neither is published by this procedure.
