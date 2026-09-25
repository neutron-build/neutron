import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

// F04 / V14: importing the SQL-only root must not load any driver module
// (pg, postgres) or any multi-model code (neutron-nucleus) — the driver is
// imported lazily at createDatabase/loadDriver time. Proven in FRESH child
// processes where a resolve hook makes pg/postgres/@neutron-build/nucleus
// unresolvable: if anything on the root import path needed them, the import
// itself would fail.

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const loaderSource = `
export function resolve(specifier, context, nextResolve) {
  if (specifier === "pg" || specifier === "postgres" || specifier === "@neutron-build/nucleus" || specifier === "@neutron-build/nucleus/sql") {
    throw new Error("simulated-missing-module: " + specifier + " is not installed in this environment");
  }
  return nextResolve(specifier, context);
}
`;

function runChild(script: string): { status: number; stdout: string; stderr: string } {
  const dir = mkdtempSync(path.join(tmpdir(), "neutron-load-trace-"));
  try {
    const loaderPath = path.join(dir, "no-drivers-loader.mjs");
    writeFileSync(loaderPath, loaderSource);
    const prelude = `import { register } from "node:module";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
register(pathToFileURL(${JSON.stringify(loaderPath)}), { data: null });
const require = createRequire(import.meta.url);
`;
    const res = spawnSync(process.execPath, ["--input-type=module", "-e", prelude + script], {
      encoding: "utf8",
      env: process.env,
    });
    return { status: res.status ?? -1, stdout: res.stdout, stderr: res.stderr };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("load-trace: SQL-only root imports with no driver resolvable, loads no driver module", () => {
  const res = runChild(`
const root = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "index.js")).href)});
const driverish = Object.keys(require.cache).filter(
  (k) => k.includes("node_modules/pg/") || k.includes("node_modules/postgres/") || k.includes("neutron-nucleus"),
);
console.log(JSON.stringify({
  imported: true,
  createDatabase: typeof root.createDatabase,
  exportSchemaV2: typeof root.exportSchemaV2,
  pgTable: typeof root.pgTable,
  wrapPgPool: typeof root.wrapPgPool,
  capabilityGate: typeof root.capabilityGate,
  parseVersionString: typeof root.parseVersionString,
  MissingDriverError: typeof root.MissingDriverError,
  ServerSqlError: typeof root.ServerSqlError,
  driverCacheEntries: driverish,
}));
`);
  assert.equal(res.status, 0, `root import must succeed without drivers:\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.imported, true);
  assert.equal(out.createDatabase, "function");
  assert.equal(out.exportSchemaV2, "function");
  assert.equal(out.pgTable, "function");
  assert.equal(out.wrapPgPool, "function");
  assert.equal(out.capabilityGate, "function");
  assert.equal(out.parseVersionString, "function");
  assert.equal(out.MissingDriverError, "function");
  assert.equal(out.ServerSqlError, "function");
  assert.deepEqual(out.driverCacheEntries, [], "no pg/postgres/nucleus module may be loaded by the root import");
});

// X01: the root import must also leave the optional capability modules
// (/pgvector, /fts) unloaded — the SQL-only root does not pull the
// pgvector/FTS surface until a consumer imports the entry point explicitly.
// X05 adds /listen-notify to the same rule.
test("load-trace: SQL-only root loads no /pgvector, /fts or /listen-notify module", () => {
  const res = runChild(`
const root = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "index.js")).href)});
const optionalModules = Object.keys(require.cache).filter(
  (k) => k.includes("/dist/pgvector.js") || k.includes("/dist/fts.js") || k.includes("/dist/listen-notify.js"),
);
console.log(JSON.stringify({
  imported: true,
  vectorAlias: typeof root.vector,
  tsvectorAlias: typeof root.tsvector,
  optionalModulesLoaded: optionalModules,
}));
`);
  assert.equal(res.status, 0, `root import must succeed:\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.imported, true);
  // The deprecated root aliases are plain re-exports living in schema.js —
  // importing the root must still not load the module files themselves.
  assert.deepEqual(out.optionalModulesLoaded, [], "the root import must not load dist/pgvector.js, dist/fts.js or dist/listen-notify.js");
});

// X05: /listen-notify imports standalone with no driver resolvable — pg is
// loaded lazily at listener creation.
test("load-trace: /listen-notify imports standalone without drivers", () => {
  const res = runChild(`
const ln = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "listen-notify.js")).href)});
console.log(JSON.stringify({
  pgListener: typeof ln.pgListener,
  postgresJsListener: typeof ln.postgresJsListener,
  notifyStatement: typeof ln.notifyStatement,
}));
`);
  assert.equal(res.status, 0, `/listen-notify import must succeed without drivers:\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.pgListener, "function");
  assert.equal(out.postgresJsListener, "function");
  assert.equal(out.notifyStatement, "function");
});

// X01: the optional modules import standalone, with no driver resolvable —
// they are capability surfaces over the same lazy-driver substrate.
test("load-trace: /pgvector and /fts import standalone without drivers", () => {
  const res = runChild(`
const pgvector = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "pgvector.js")).href)});
const fts = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "fts.js")).href)});
console.log(JSON.stringify({
  pgVector: typeof pgvector.pgVector,
  l2Distance: typeof pgvector.l2Distance,
  cosineDistance: typeof pgvector.cosineDistance,
  pgvectorExtension: typeof pgvector.pgvectorExtension,
  tsvector: typeof fts.tsvector,
  toTsvector: typeof fts.toTsvector,
  websearchToTsquery: typeof fts.websearchToTsquery,
  tsRank: typeof fts.tsRank,
  matches: typeof fts.matches,
}));
`);
  assert.equal(res.status, 0, `optional-module imports must succeed without drivers:\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.pgVector, "function");
  assert.equal(out.l2Distance, "function");
  assert.equal(out.cosineDistance, "function");
  assert.equal(out.pgvectorExtension, "function");
  assert.equal(out.tsvector, "function");
  assert.equal(out.toTsvector, "function");
  assert.equal(out.websearchToTsquery, "function");
  assert.equal(out.tsRank, "function");
  assert.equal(out.matches, "function");
});

test("load-trace: the unresolvable simulation is real (positive control)", () => {
  const res = runChild(`
try {
  await import("pg");
  console.log(JSON.stringify({ pgImport: "unexpectedly-succeeded" }));
} catch (err) {
  console.log(JSON.stringify({ pgImport: "rejected", msg: String(err).slice(0, 60) }));
}
`);
  assert.equal(res.status, 0);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.pgImport, "rejected");
  assert.match(out.msg, /simulated-missing-module: pg/);
});

test("load-trace: the driver loads lazily — loadDriver fails only when called", () => {
  const res = runChild(`
const { loadDriver } = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "index.js")).href)});
let result;
try {
  await loadDriver("postgres://none@127.0.0.1:1/none", { driver: "postgres" });
  result = { loadDriver: "unexpectedly-succeeded" };
} catch (err) {
  result = { loadDriver: "rejected-at-call-time", msg: String(err).slice(0, 60) };
}
console.log(JSON.stringify(result));
`);
  assert.equal(res.status, 0);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.loadDriver, "rejected-at-call-time");
  assert.match(out.msg, /simulated-missing-module: postgres/);
});
