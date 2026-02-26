# Framework Audit - 2026-02-19

Audit scope: Neutron TypeScript workspace under `neutron/`.

## Governance Continuation Pass (2026-02-20)

1. Naming policy was formalized as an accepted RFC:
   - `docs/rfcs/naming.md`
2. Workspace graph drift gate was added:
   - `scripts/workspace-graph-check.mjs`
   - root scripts: `ci:workspace`, `ci:workspace:snapshot`
3. CI now enforces naming + workspace graph checks before tests:
   - `.github/workflows/ci.yml`
4. Release check now includes workspace graph validation:
   - `scripts/release-check.mjs`
5. Docs and checklist were aligned to ratified policy:
   - `docs/README.md`
   - `docs/system-naming.md`
   - `docs/core/naming-release-checklist.md`
   - `docs/{typescript,rust,zig,mojo}/README.md`
6. Build pipeline stability update:
   - Disabled Vite websocket startup in middleware-only CLI servers used by build/worker flows.
   - Files: `packages/neutron-cli/src/commands/build.ts`, `packages/neutron-cli/src/commands/worker.ts`
7. Test-depth expansion for previously thin packages:
   - `neutron-data` sliding-window edge cases (malformed prior counter, per-key isolation)
   - `@neutron/security` CSRF middleware behavior (safe issue, unsafe deny, unsafe allow)
   - `@neutron/cache-redis` path-index delete coverage and chunked delete bounds
8. Repository topology guard:
   - Added `scripts/check-mirror-sync.mjs` and `pnpm run ci:mirror-sync` to detect drift between:
     - `neutron/packages/neutron`
     - `../packages/neutron` (when present)
   - Synced drifting mirror manifest (`../packages/neutron/package.json`) in this pass.

## Current State

The framework is functionally broad and largely complete, but had reliability and hardening gaps.  
This pass focused on production-risk items and long-term naming clarity.

## Fixes Applied In This Pass

1. Task reliability and false-green prevention
   - Root scripts now run deterministic workspace tasks with `pnpm -r --if-present` instead of relying on Turbo task discovery alone.
   - Added `scripts/check-workspace-tasks.mjs` to enforce that every framework package defines required `lint`/`test` scripts.
   - Added missing `lint`/`test` scripts across framework packages.
   - Fixed Turbo package omission root cause by replacing `neutron/packages/neutron` junction with a real in-workspace package directory.

2. Security hardening (`@neutron/security`)
   - `resolveClientIp()` now defaults `trustProxy` to `false` (secure by default).
   - In-memory rate limiter now has bounded bucket growth with periodic TTL/size pruning.

3. Redis cache safety (`@neutron/cache-redis`)
   - Replaced `KEYS`-only clear path with `SCAN` iteration when available.
   - Added chunked deletes to avoid oversized delete calls.

4. Rate limit concurrency correctness (`neutron-data`)
   - Reworked `enforceSlidingWindow()` to use atomic `incr` counters and weighted two-window calculation.
   - Removed read-modify-write race behavior.

5. Server islands completeness (`neutron` core)
   - Removed placeholder rendering path in server-islands handler.
   - Added proper server-side HTML rendering for island responses.
   - Wired `/__neutron_island/:id` endpoint into server routing.
   - Added registry TTL/size pruning and one-time island cleanup.

6. Island transform gap reduction
   - JSX prop expression parsing now handles booleans, numbers, null, quoted strings, and JSON literals before fallback.

7. Naming/docs clarity
   - Updated `docs/system-naming.md` with explicit artifact matrix and docs model.
   - Updated root `ARCHITECTURE.md` docs language to reflect unified + per-implementation documentation structure.

8. New executable package tests
   - Added runnable tests for `@neutron/security`:
     - trusted proxy default behavior
     - rate-limit denial behavior
   - Added runnable tests for `neutron-data` sliding-window rate limiting.
   - Added runnable tests for `@neutron/cache-redis` clear behavior:
     - `SCAN` path
     - `KEYS` fallback path

## Remaining Gaps

1. Test depth
   - Executable tests now cover key edge/failure paths for `neutron-data`, `@neutron/security`, and `@neutron/cache-redis`.
   - Broader cross-package integration tests are still needed.

2. Repository topology complexity
   - Framework code appears in both `neutron/packages/neutron` and `packages/neutron`.
   - Drift risk is now CI-gated via `ci:mirror-sync`, but long-term simplification to a single canonical location is still preferred.

3. Naming consistency at package level
   - npm naming still mixes scoped (`@neutron/*`) and unscoped (`neutron-data`, `neutron-cli`) patterns.
   - This is valid today but should be frozen as explicit policy or migrated at a planned major release.

## Validation Notes

- Build checks passed for changed packages:
  - `neutron`
  - `create-neutron`
  - `@neutron/security`
  - `@neutron/cache-redis`
  - `neutron-data`
- New package test suites passed:
  - `pnpm --dir neutron --filter @neutron/security run test`
  - `pnpm --dir neutron --filter neutron-data run test`
  - `pnpm --dir neutron --filter @neutron/cache-redis run test`
- Naming check passed: `pnpm --dir neutron run ci:naming`.
- Workspace graph check passed: `pnpm --dir neutron run ci:workspace`.
- Mirror sync check passed: `pnpm --dir neutron run ci:mirror-sync`.
- Full recursive workspace build/lint/test passed:
  - `pnpm --dir neutron run build`
  - `pnpm --dir neutron run lint`
  - `pnpm --dir neutron run test`
- Release check passed with benchmark gate intentionally skipped:
  - `RELEASE_CHECK_SKIP_BENCH=1 pnpm --dir neutron run ci:release`
- Parallel app/example builds no longer emit websocket port-collision errors after middleware ws disable.
