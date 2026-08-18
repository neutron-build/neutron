#!/usr/bin/env node
// Canonical Neutron conformance app (TypeScript SDK).
//
// Boots the Neutron TS web/SSR server headless — no DB — so the cross-SDK
// conformance runner can assert the FRAMEWORK_CONTRACT.md surfaces:
//
//   GET  /health            §7 health shape {status, nucleus, version} (nucleus=unconfigured)
//   x-request-id            §5.1 request-id middleware (response header, all routes)
//   CORS                    §5.4 preflight + Access-Control-Allow-Origin
//   compression             §5.5 gzip (over the compressible /api/items body)
//   GET  /errors/{code}     §2 forced standard errors, as RFC 7807 problem+json
//   POST /api/items         §2 typed validation -> 422 problem+json with errors[]
//   GET  /openapi.json      §4 OpenAPI 3.1 document generated from the route tree
//   GET  /docs              §4 Swagger UI over that document
//
// The API dimensions used to be recorded skips: the SDK had no RFC 7807
// support, no typed validation helper and no OpenAPI generation at all
// (S81). They are SDK features now — this app only wires them.
//
// Imports the built dist directly (no package resolution needed); the dist lives
// inside the pnpm workspace so its own hono imports resolve. Route files import
// `@neutron-build/core` via the vite.config.mts alias. Requires the package
// to be built first: `pnpm --filter @neutron-build/core build`.
//
// Listen address: PORT (required by the runner), HOST optional.
//   PORT=8084 node conformance_app.mjs

import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
// conformance/adapters/typescript → repo root is three levels up.
const DIST = path.resolve(
  HERE,
  "../../../typescript/packages/neutron/dist/server/index.js",
);

const { createServer } = await import(DIST);

const port = Number(process.env.PORT || 8084);
const host = process.env.HOST || "127.0.0.1";

await createServer({
  port,
  host,
  version: "9.9.9",
  // Two route modules: `routes/api/items.tsx` (GET list + POST validation —
  // also the compressible body for the gzip probe) and
  // `routes/errors/[code].tsx` (forced §2 errors). It used to point at a
  // path that does not exist — manifest.discoverRoutes tolerates a missing
  // dir and returns [] — which booted the contract middleware and /health
  // but left `mw.compression` probing a 404 and reporting `skip`.
  rootDir: HERE,
  // No build output to serve; point distDir at an existing dir so the static
  // mounts don't log a "root path not found" warning. The runner never probes
  // /assets, so nothing is actually served from here.
  distDir: HERE,
  routesDir: "routes",
  cors: { origin: "*" },
  compress: true,
  // §4: the OpenAPI 3.1 document and /docs, generated from the route tree.
  openapi: { title: "Neutron Conformance API", version: "9.9.9" },
});
