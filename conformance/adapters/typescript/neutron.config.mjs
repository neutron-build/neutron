// Server options for the TypeScript conformance app, booted with the SDK's own
// production entry point: `neutron-ts start`, run from this directory.
//
// The app used to be a hand-written script calling `createServer({ port })`
// with a port it read from PORT itself. That bypassed both contract behaviours
// the SDK implements in its CLI — NEUTRON_HOST/NEUTRON_PORT resolution (§6)
// and the SIGTERM drain + exit (§8) — so the conformance run could not see
// either one break. Booting through `neutron-ts start` exercises the path a
// deployed app actually takes; the runner supplies only NEUTRON_HOST and
// NEUTRON_PORT.
//
// Plain object, deliberately no `defineConfig` import: the adapter directory
// sits outside the pnpm workspace, and the loader accepts a bare config.
export default {
  server: {
    version: "9.9.9",
    // Two route modules plus the drain probe: `routes/api/items.tsx` (GET list
    // + POST validation, also the compressible body for the gzip probe),
    // `routes/errors/[code].tsx` (forced §2 errors) and `routes/slow.tsx`.
    routesDir: "routes",
    // No build output to serve; point distDir at an existing dir so the static
    // mounts don't log a "root path not found" warning. The runner never
    // probes /assets, so nothing is actually served from here.
    distDir: ".",
    cors: { origin: "*" },
    compress: true,
    // §4: the OpenAPI 3.1 document and /docs, generated from the route tree.
    openapi: { title: "Neutron Conformance API", version: "9.9.9" },
  },
};
