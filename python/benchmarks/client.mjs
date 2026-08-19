#!/usr/bin/env node
// autocannon driver for the Python benchmark suite.
//
// Uses the SAME client (and, by default, the same install) as the TypeScript
// harness: node_modules/autocannon under typescript/benchmarks. That install
// is read-only for us; if it is absent, point AUTOCANNON_MODULE at another
// autocannon install.
//
// Usage: node client.mjs '<json-config>'
//   { baseUrl, path, method, headers, body, connections, durationSec, warmupSec }
// Prints one JSON result object on stdout.

import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import path from "node:path";
import process from "node:process";

const here = path.dirname(fileURLToPath(import.meta.url));
const defaultInstall = path.resolve(
  here,
  "../../typescript/benchmarks/node_modules/autocannon"
);

const require = createRequire(import.meta.url);
let autocannon;
try {
  autocannon = require(process.env.AUTOCANNON_MODULE || defaultInstall);
} catch (e) {
  console.error(
    `cannot load autocannon (looked at ${defaultInstall}). ` +
      `Set AUTOCANNON_MODULE to an autocannon install: ${e.message}`
  );
  process.exit(2);
}

const cfg = JSON.parse(process.argv[2]);
const url = new URL(cfg.path, cfg.baseUrl);

const result = await autocannon({
  url: url.href,
  connections: cfg.connections,
  duration: cfg.durationSec,
  pipelining: 1,
  method: cfg.method,
  headers: cfg.headers,
  body: cfg.body,
  // Keep non-2xx responses OUT of the latency histogram; the orchestrator
  // asserts on the counts separately. A framework serving 500s fast must
  // not score well on latency.
  excludeErrorStats: true,
});

process.stdout.write(JSON.stringify(result));
