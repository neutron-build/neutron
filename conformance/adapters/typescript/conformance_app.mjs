#!/usr/bin/env node
// Canonical Neutron conformance app (TypeScript SDK).
//
// Boots the Neutron TS web/SSR server headless — no route tree, no DB — so the
// cross-SDK conformance runner can assert the cross-cutting FRAMEWORK_CONTRACT.md
// surfaces an SSR framework legitimately implements:
//
//   GET /health        §7 health shape {status, nucleus, version} (nucleus=unconfigured)
//   x-request-id       §5.1 request-id middleware (response header, all routes)
//   CORS               §5.4 preflight + Access-Control-Allow-Origin
//   compression        §5.5 gzip (only when a compressible body is served)
//
// The API-only dimensions (RFC 7807 forced-error endpoints, typed validation,
// OpenAPI 3.1) are not exposed by a web/SSR framework whose routes are pages, not
// JSON API endpoints — the runner reports those `skip` by design.
//
// Imports the built dist directly (no package resolution needed); the dist lives
// inside the pnpm workspace so its own hono imports resolve. Requires the package
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
  // No route tree: point rootDir at this adapter dir and routesDir at a path that
  // does not exist. manifest.discoverRoutes tolerates a missing dir (returns []),
  // so the server boots with only the contract middleware + /health.
  rootDir: HERE,
  // No build output to serve; point distDir at an existing dir so the static
  // mounts don't log a "root path not found" warning. The runner never probes
  // /assets, so nothing is actually served from here.
  distDir: HERE,
  routesDir: "__no_routes__",
  cors: { origin: "*" },
  compress: true,
});
