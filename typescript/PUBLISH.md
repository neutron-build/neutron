# Publishing the render-core packages (OTP-gated — run locally)

The render-core-unification work added new `@neutron-build/core/runtime-edge`
exports (`renderAppRoute`, `isMutationMethod`, `isJsonRequest`,
`createMemoryLoaderCacheStore`), and the CLI's generated prod entry now imports
them. `core@0.1.4` and `cli@0.1.3` are already published at those exact versions,
so this **requires a version bump** (npm forbids overwriting) and the CLI must
resolve a core that has the new exports.

`cli` depends on core via `workspace:^`, so pnpm rewrites it to the real core
version **at publish time** — bump core first and the pin is automatic.

## Branch state (verified 2026-07-14)

`feat/render-core-unification` is **complete** (5 commits: d1a066b docker-preset
fix, ecb11f7 dev core, 51f8817 prod codegen, da84835 dead-code, ad1583c
playground regen) and a **clean fast-forward from main** (main is 0 ahead). So
you can merge it to main first, then publish from main:

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

# 3. Log in (your OTP).
npm login

# 4. Publish in dependency order — core FIRST (cli's workspace:^ resolves to it),
#    then cli, then the three new SDKs. --access public for the scoped org.
npm publish packages/neutron          --access public   # @neutron-build/core@0.1.5
npm publish packages/neutron-cli      --access public   # @neutron-build/cli@0.1.4  (deps core ^0.1.5)
npm publish packages/neutron-ai       --access public   # @neutron-build/ai@0.1.0
npm publish packages/neutron-workflow --access public   # @neutron-build/workflow@0.1.0
npm publish packages/neutron-agents   --access public   # @neutron-build/agents@0.1.0

# 5. Verify.
for p in core cli ai workflow agents; do echo -n "@neutron-build/$p: "; npm view @neutron-build/$p version; done
```

Sanity check before publishing cli: `npm pack --dry-run packages/neutron-cli` and
confirm the resolved `@neutron-build/core` dependency is `^0.1.5` (not
`workspace:^`).

## Then: de-vendor Ship (R5)

Ship currently installs the SDKs from packed tarballs because they were ahead of
npm. Once the above lands, switch to the published versions and drop the packing.

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
