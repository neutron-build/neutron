# Render-core package release

## Pending release: core 0.1.6 / cli 0.1.5

Versions already bumped (`package.json`). These ship framework bug fixes that
affect every Neutron site, so downstream sites (teploy.com, DLBS) should update
after publish and can drop any local pnpm patches for these fixes.

What's in it (since core 0.1.5 / cli 0.1.4):
- **Anchor links (core):** the SPA click interceptor + `navigate()` no longer
  strip the URL hash — in-page anchors (`#section`, `/page#section`) scroll
  again. Was broken on every hydrating Neutron site.
- **Heading IDs (core):** markdown AND MDX headings get slugified `id`s, so the
  "on this page" TOC anchors resolve (fixes teploy.com's TOC).
- **Resource routes (core + cli):** dynamic resource routes + catch-all-with-
  literal-suffix routing — enables per-page endpoints like `/docs/<slug>.md`.
- **MCP (cli):** `search_docs` / `get_doc` tools added to `neutron mcp`.

Publish runbook (you run this — the login step needs your security key):

```bash
cd Neutron/typescript
pnpm -r build
npm login --auth-type=web
(cd packages/neutron     && pnpm publish --no-git-checks --access public)   # core 0.1.6 FIRST
(cd packages/neutron-cli && pnpm publish --no-git-checks --access public)   # cli 0.1.5 (workspace:^ rewritten to core 0.1.6)
for p in core cli; do echo -n "@neutron-build/$p: "; npm view @neutron-build/$p version; done
```

Only `core` and `cli` changed this cycle — `ai`/`workflow`/`agents` stay put.

## Release status (completed 2026-07-14)

The render-core packages are published and publicly available:

- `@neutron-build/core@0.1.5`
- `@neutron-build/cli@0.1.4` (published dependency: core `^0.1.5`)
- `@neutron-build/ai@0.1.0`
- `@neutron-build/workflow@0.1.0`
- `@neutron-build/agents@0.1.0`

The commands below are retained as the verified manual release procedure for a
future version. Run pnpm from inside each package directory: using
`pnpm --dir <package> publish` with pnpm 9.15.4 and npm 11.17.0 produced a
malformed npm invocation.

The render-core-unification work added new `@neutron-build/core/runtime-edge`
exports (`renderAppRoute`, `isMutationMethod`, `isJsonRequest`,
`createMemoryLoaderCacheStore`), and the CLI's generated prod entry now imports
them. `core@0.1.4` and `cli@0.1.3` are already published at those exact versions,
so this **requires a version bump** (npm forbids overwriting) and the CLI must
resolve a core that has the new exports.

`cli` depends on core via `workspace:^`, so pnpm rewrites it to the real core
version **at publish time** — bump core first and the pin is automatic.

## Branch state (verified 2026-07-14)

`feat/render-core-unification` is a clean fast-forward from main (main is 0
ahead). It contains the original five render-core commits plus the follow-on
head-resolution/render-guards commit `e3f19e9`.

```bash
# (optional, recommended) merge the finished branch — it's a clean fast-forward
git checkout main && git merge --ff-only feat/render-core-unification
```

Note: another workstream may have uncommitted `nucleus/*.rs` changes in the
working tree — commit/stash those before switching branches.

## Steps

```bash
cd Neutron/typescript
git checkout feat/render-core-unification      # (or main, if you merged above)

# 1. Bump the two already-published packages past the taken versions.
#    (ai/workflow/agents are 0.1.0 and unpublished — leave as-is.)
npm --prefix packages/neutron        version patch --no-git-tag-version   # core 0.1.4 -> 0.1.5
npm --prefix packages/neutron-cli    version patch --no-git-tag-version   # cli  0.1.3 -> 0.1.4

# 2. Build everything.
pnpm -r build

# 3. Log in through the browser/security-key flow.
npm login --auth-type=web

# 4. Publish in dependency order — core FIRST. pnpm rewrites workspace:^.
(cd packages/neutron          && pnpm publish --no-git-checks --access public)
(cd packages/neutron-cli      && pnpm publish --no-git-checks --access public)
(cd packages/neutron-ai       && pnpm publish --no-git-checks --access public)
(cd packages/neutron-workflow && pnpm publish --no-git-checks --access public)
(cd packages/neutron-agents   && pnpm publish --no-git-checks --access public)

# 5. Verify.
for p in core cli ai workflow agents; do echo -n "@neutron-build/$p: "; npm view @neutron-build/$p version; done
```

Sanity check before publishing cli: `npm pack --dry-run packages/neutron-cli` and
confirm the resolved `@neutron-build/core` dependency is `^0.1.5` (not
`workspace:^`).

## Ship de-vendoring (R5)

Ship previously installed the SDKs from packed tarballs because they were ahead
of npm. The `Teploy/teploy-ship` deployment image now uses the published
versions directly.

In `Teploy/teploy-ship`:

1. `deploy/package.ship.json` — replace the `file:./vendor/*.tgz` deps:
   ```json
   "@neutron-build/agents":  "^0.1.0",
   "@neutron-build/ai":      "^0.1.0",
   "@neutron-build/workflow":"^0.1.0",
   ```
2. `deploy/package.web.json` — replace the vendored core/cli:
   ```json
   "@neutron-build/cli":  "^0.1.4",
   "@neutron-build/core": "^0.1.5",
   ```
3. `deploy/build-image.sh` — remove the `pnpm pack … deploy/vendor` step and the
   `COPY deploy/vendor/ vendor/` line in the Dockerfile.
4. Rebuild the Ship image and smoke-test (`docker build` + `web`/`worker` boot).

## Notes
- Monorepo/Ship-from-source are unaffected either way (they use `link:`/`file:`);
  this coupling only bites external npm consumers, so it's not urgent — but don't
  publish cli without publishing the bumped core in the same pass.
- `nucleus@0.1.2` is already on the registry; no action.
