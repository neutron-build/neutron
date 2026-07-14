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

- **`feat/render-core-unification`** — the render-core unification, 5 commits,
  a **clean fast-forward from main** (main 0 ahead). This is the minimal,
  isolated publish source.
- **A follow-on commit `d8e7557`** (on `fix/nucleus-value-type-consistency`)
  extracts shared `core/head.ts` + adds the render-guards CI harness + docs
  import fixes. TS-only, 271 tests + guard green. It's an *improvement* on top
  of render-core, not required to publish.

**Two publish choices:**

```bash
# (a) minimal — publish render-core as-is (proven, byte-identical golden output)
git checkout main && git merge --ff-only feat/render-core-unification

# (b) include the head-extraction — cherry-pick the TS-only follow-on first
git checkout feat/render-core-unification
git cherry-pick d8e7557        # clean: touches only typescript/ (no nucleus)
# then merge to main + publish
```

Note: `fix/nucleus-value-type-consistency` also has unrelated in-progress
`nucleus/*.rs` (Rust) work — nothing to do with the npm TS packages; leave it.

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
