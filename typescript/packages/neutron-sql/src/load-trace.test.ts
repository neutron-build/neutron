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
  driverCacheEntries: driverish,
}));
`);
  assert.equal(res.status, 0, `root import must succeed without drivers:\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.imported, true);
  assert.equal(out.createDatabase, "function");
  assert.equal(out.exportSchemaV2, "function");
  assert.equal(out.pgTable, "function");
  assert.deepEqual(out.driverCacheEntries, [], "no pg/postgres/nucleus module may be loaded by the root import");
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
