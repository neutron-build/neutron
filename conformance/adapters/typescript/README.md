# TypeScript SDK conformance adapter

The TypeScript SDK (`typescript/packages/neutron`, published as `@neutron-build/core`)
is a **web/SSR meta-framework** (file-based routing, Preact SSR, islands) built on
Hono. Since S81 it implements all twelve conformance dimensions, including the
API surfaces: RFC 7807 errors (§2), typed validation (§2), and OpenAPI 3.1
(§4). The adapter is 12/12 with zero skips.

`conformance_app.mjs` boots the server headless (no DB) by importing the built
`dist/server/index.js` directly. Route files (`routes/`) import the SDK through
the `@neutron-build/core` alias set in `vite.config.mts` — the same import a
real app uses; the adapter directory sits outside the pnpm workspace, so the
alias points at the built dist inside it. The runner wires the app in as the
`ts` SDK (`runner/sdks.mjs`); it is auto-skipped (`UNAVAILABLE`) when the
package has not been built.

## Boot it

```bash
# 1. Build the package once (offline tsc build; no network needed if deps installed)
cd typescript
pnpm --filter @neutron-build/core build

# 2. Run the full matrix (the runner builds/boots every available SDK)
cd ../conformance && node runner/run.mjs

# …or just the TS row:
node runner/run.mjs ts

# …or boot the adapter standalone and point the runner at it:
PORT=8084 node adapters/typescript/conformance_app.mjs
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
