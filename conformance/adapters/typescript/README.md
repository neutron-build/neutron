# TypeScript SDK conformance adapter (document-only)

The TypeScript SDK (`typescript/packages/neutron`) is a **web/SSR meta-framework**
(file-based routing, Preact SSR, islands) built on Hono — not a JSON API server
like the Go/Rust/Python SDKs. As a result it only implements a subset of the
FRAMEWORK_CONTRACT.md HTTP surface, and it cannot be booted in this environment
without a network install + build. It is therefore **document-only** here.

## Why it is not auto-booted

1. **No published `dist/`** — `createServer()` is exported from `./server`, which
   resolves to `dist/server/index.js`. The package must be built first
   (`pnpm install && pnpm build` in `typescript/`), which requires network access
   to the npm registry.
2. **Requires a route tree** — `createServer({ rootDir, distDir, routesDir })`
   discovers file-based routes under `src/routes/` and serves a built SSR app.
   There is no standalone "API app" entry point.
3. **API contract surfaces are not implemented at the HTTP layer:**
   - No `/openapi.json` / `/docs` endpoint (the spec lives in the build tooling).
   - No RFC 7807 `application/problem+json` error responses for routes — unknown
     paths return plain `text` 404s (`c.text("Bad Request", 400)` /
     `c.text("Not Found", 404)`), not Problem Details.
   - No typed validation 422 surface.

## What the TS server DOES implement (and how to verify)

`createServer` registers, in FRAMEWORK_CONTRACT.md middleware order
(`typescript/packages/neutron/src/server/index.ts`):

- `GET /health` → contract-shaped body **but with a drift** (see below).
- CORS (preflight + headers) when `cors` is configured.
- gzip compression (`compress()` from Hono) when `compress` is enabled.

### Boot command (after a network-enabled install + build)

```bash
# from repo root
cd typescript
pnpm install
pnpm --filter @neutron-build/neutron build

# then, from this adapter dir, against a built example app that has src/routes/:
#   import { createServer } from "@neutron-build/neutron/server";
#   const { url } = await createServer({ port: Number(process.env.PORT), version: "9.9.9" });
node conformance_app.mjs    # PORT=8084 node conformance_app.mjs
```

Then point the runner at it:

```bash
node ../../runner/run.mjs --base=http://127.0.0.1:8084
```

Only `health.*`, `mw.cors`, and `mw.compression` dimensions are meaningful; the
error/validation/openapi dimensions will report `fail`/`skip` **by design** because
the web framework does not expose those API surfaces.

## Confirmed static drift (code inspection)

- **`/health` nucleus type drift** — `server/index.ts:351` emits
  `nucleus: "unconfigured"` (string), not the boolean the contract §7 mandates.
  Same divergence as the Rust SDK.
- **KV comma-split** — `packages/neutron-nucleus/src/kv/index.ts:274,344,368,375`
  parses `KV_SMEMBERS`/`KV_LRANGE`/etc. with `raw.split(',')`, so values containing
  a literal comma are corrupted on read. Ported across all four SDK clients.
