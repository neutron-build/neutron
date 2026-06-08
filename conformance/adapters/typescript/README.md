# TypeScript SDK conformance adapter

The TypeScript SDK (`typescript/packages/neutron`, published as `@neutron-build/core`)
is a **web/SSR meta-framework** (file-based routing, Preact SSR, islands) built on
Hono — not a JSON API server like the Go/Rust/Python SDKs. It therefore implements
the **cross-cutting** parts of FRAMEWORK_CONTRACT.md (health, request-id, CORS,
compression) and not the API-only parts (forced-error endpoints, typed validation,
OpenAPI). It participates in the conformance matrix on the dimensions it legitimately
exposes; the rest report `skip` **by design**.

`conformance_app.mjs` boots the server headless (no route tree, no DB) by importing
the built `dist/server/index.js` directly. The runner wires it in as the `ts` SDK
(`runner/sdks.mjs`); it is auto-skipped (`UNAVAILABLE`) when the package has not been
built.

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

`createServer` registers middleware in FRAMEWORK_CONTRACT.md order
(`typescript/packages/neutron/src/server/index.ts`):

| Dimension            | Result | Notes |
|----------------------|--------|-------|
| `health.shape`       | pass   | `GET /health` → exactly `{status, nucleus, version}` |
| `health.types`       | pass   | `nucleus: "unconfigured"` — tri-state string per §7 |
| `feature.detection`  | pass   | detection state exposed via the `nucleus` field |
| `mw.requestid`       | pass   | `x-request-id` response header (§5.1), inbound value reused if present |
| `mw.cors`            | pass   | preflight short-circuit + `Access-Control-Allow-Origin` (§5.4) |
| `mw.compression`     | skip   | gzip is wired (`hono/compress`), but the headless app serves no compressible `/api/items` body to probe |
| `error.*` (×3)       | skip   | no forced-error endpoints — SSR errors are HTML pages, not RFC 7807 |
| `validation.format`  | skip   | no JSON API validation surface |
| `openapi.present/.31`| skip   | routes are pages, not API endpoints — no `/openapi.json` |

The `skip`s are honest "surface not exposed" outcomes (the runner defines `skip` as
*documented, not a hard failure*), not drift. `skip` never fails the suite.

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
