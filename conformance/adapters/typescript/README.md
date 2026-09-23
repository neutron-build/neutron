# TypeScript SDK conformance adapter

The TypeScript SDK (`typescript/packages/neutron`, published as `@neutron-build/core`)
is a **web/SSR meta-framework** (file-based routing, Preact SSR, islands) built on
Hono. Since S81 it implements all the HTTP conformance dimensions, including the
API surfaces: RFC 7807 errors (§2), typed validation (§2), and OpenAPI 3.1
(§4), and it passes the lifecycle dimensions (§6 env, §8 SIGTERM) through
`neutron-ts start`. The adapter is 14/14 with zero skips.

The app boots headless (no DB) through the SDK's own production entry point,
`neutron-ts start`, run from this directory with `neutron.config.mjs` supplying
the server options. That is deliberate: listen-address resolution
(`NEUTRON_HOST`/`NEUTRON_PORT`, contract §6) and the SIGTERM drain + exit (§8)
live in that command, so an adapter script calling `createServer({ port })`
with its own `PORT` — which is what this used to be — could not see either one
break. Route files (`routes/`) import the SDK through the `@neutron-build/core`
alias set in `vite.config.mts` — the same import a real app uses; the adapter
directory sits outside the pnpm workspace, so the alias points at the built
dist inside it. The runner wires the app in as the `ts` SDK
(`runner/sdks.mjs`); it is reported absent when `@neutron-build/core` or
`@neutron-build/cli` has not been built.

## Boot it

```bash
# 1. Build the packages once (offline tsc build; no network needed if deps installed)
cd typescript
pnpm --filter @neutron-build/core --filter @neutron-build/cli build

# 2. Run the full matrix (the runner builds/boots every available SDK)
cd ../conformance && node runner/run.mjs

# …or just the TS row:
node runner/run.mjs ts

# …or boot the adapter standalone and point the runner at it:
(cd adapters/typescript && NEUTRON_PORT=8084 node ../../../typescript/packages/neutron-cli/bin/neutron-ts.mjs start)
node runner/run.mjs --base=http://127.0.0.1:8084
```

## Dimensions

| Dimension            | Result | How |
|----------------------|--------|-----|
| `health.shape`       | pass   | `GET /health` → exactly `{status, nucleus, version}` |
| `health.types`       | pass   | `nucleus: "unconfigured"` — tri-state string per §7 |
| `feature.detection`  | pass   | detection state exposed via the `nucleus` field |
| `mw.requestid`       | pass   | `x-request-id` response header (§5.1), inbound value reused if present |
| `mw.cors`            | pass   | preflight short-circuit + `Access-Control-Allow-Origin` (§5.4) |
| `mw.compression`     | pass   | gzip over the compressible `/api/items` body |
| `error.*` (×3)       | pass   | `routes/errors/[code].tsx` throws the SDK taxonomy constructors; the render pipeline serves `ProblemError` as `application/problem+json` |
| `validation.format`  | pass   | `routes/api/items.tsx` action validates with `validateJsonBody` (zod) → 422 problem+json with `errors[]` |
| `openapi.present/.31`| pass   | `openapi` server option → `/openapi.json` (3.1.0, generated from the route tree) + `/docs` |
| `config.env`         | pass   | booted with only `NEUTRON_HOST`/`NEUTRON_PORT`; `neutron-ts start` resolves them (flag > env > config > default) |
| `shutdown.sigterm`   | pass   | SIGTERM mid-`/slow`: the request completes 200, new connections are refused, the process exits 0 |

The §2/§4 dimensions are exercised through SDK features (`core/problem.ts`,
`server/openapi.ts`), not hand-built responses in the adapter — before S81 the
features did not exist and the six dimensions were recorded skips in
`conformance/known-skips.json`.

## §7 health: nucleus value

The SSR server holds no Nucleus pool (loaders connect per-request), so `/health`
reports `nucleus: "unconfigured"` — the correct tri-state value when no DB dependency
is wired at the server level. This matches contract §7 (`connected | disconnected |
unconfigured`) and the Rust/Go/Python SDKs booted without a DB.

## Known issue (not a framework drift)

- **KV comma-split** — `packages/neutron-nucleus/src/kv/index.ts` parses
  `KV_SMEMBERS`/`KV_LRANGE`/etc. with `raw.split(',')`, so values containing a literal
  comma are corrupted on read. This is shared across all four SDK KV clients and is
  **coupled to the Nucleus pgwire layer** (the proper fix is the engine emitting
  `jsonb` so clients don't string-split). It is out of scope for this HTTP-level
  contract suite and tracked as engine work.
