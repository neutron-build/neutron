import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

// I03 runtime-support probe (V14: unsupported edge/browser contexts fail
// clearly — a precise error, not a crash). The probe simulates a no-Node
// environment in fresh child processes with a resolve hook that rejects every
// `node:` builtin specifier (the same pattern neutron-sql's load-trace suite
// uses to prove import purity):
//
// 1. the `@neutron-build/data/drizzle` entry EVALUATES with no node builtin
//    resolvable — module evaluation never needs one (node:path is loaded
//    lazily inside the SQLite branch), so an edge/browser bundling context
//    gets past import;
// 2. with `process.versions.node` removed (the honest stand-in for a runtime
//    without Node compatibility), createDrizzleDatabase rejects with the
//    precise unsupported-runtime error — before any driver import is
//    attempted;
// 3. the root entry declares its Node requirement at import: it re-exports
//    the session/queue drivers which statically import node builtins, so in
//    a no-Node environment the root import fails at module resolution with
//    the runtime's own "node:x" error — the documented contract for the
//    root, asserted here so the behavior is pinned, not folklore.
//
// Detection is positive (process.versions.node), never `typeof window`.

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const loaderSource = `
export function resolve(specifier, context, nextResolve) {
  if (specifier.startsWith("node:") || ["os", "fs", "path", "crypto", "url", "module", "assert", "child_process", "test"].includes(specifier)) {
    throw new Error("simulated-no-node-runtime: " + specifier + " is not available");
  }
  return nextResolve(specifier, context);
}
`;

interface ChildResult {
  status: number;
  stdout: string;
  stderr: string;
}

function runChild(script: string): ChildResult {
  const dir = mkdtempSync(path.join(tmpdir(), "i03-edge-probe-"));
  try {
    const loaderPath = path.join(dir, "no-node-loader.mjs");
    writeFileSync(loaderPath, loaderSource);
    const prelude = `import { register } from "node:module";
import { pathToFileURL } from "node:url";
register(pathToFileURL(${JSON.stringify(loaderPath)}), { data: null });
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

test("edge: /drizzle entry evaluates with no node builtin resolvable", () => {
  const res = runChild(`
    const mod = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "drizzle.js")).href)});
    console.log(JSON.stringify({ imported: true, createDrizzleDatabase: typeof mod.createDrizzleDatabase }));
  `);
  assert.equal(res.status, 0, `/drizzle evaluation must not need node builtins:\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.imported, true);
  assert.equal(out.createDrizzleDatabase, "function");
});

test("edge: no-Node callers get one precise error, not a crash", () => {
  const res = runChild(`
    const mod = await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "drizzle.js")).href)});
    delete process.versions.node;
    let result;
    try {
      await mod.createDrizzleDatabase({ profile: { provider: "sqlite", connectionString: "/tmp/never.db" } });
      result = { failed: false };
    } catch (err) {
      result = { failed: true, name: err?.name, message: String(err?.message).slice(0, 400) };
    }
    console.log(JSON.stringify(result));
  `);
  assert.equal(res.status, 0, `the guard must reject cleanly (process must not crash):\n${res.stderr}`);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.failed, true);
  assert.match(out.message, /requires a Node\.js runtime/);
  assert.match(out.message, /no edge\/browser adapter exists/);
});

test("edge: the no-node simulation is real (positive control)", () => {
  const res = runChild(`
    try {
      await import("node:path");
      console.log(JSON.stringify({ pathImport: "unexpectedly-succeeded" }));
    } catch (err) {
      console.log(JSON.stringify({ pathImport: "rejected", msg: String(err).slice(0, 50) }));
    }
  `);
  assert.equal(res.status, 0);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  assert.equal(out.pathImport, "rejected");
  assert.match(out.msg, /simulated-no-node-runtime/);
});

test("edge: root entry fails at import in no-Node environments (documented contract)", () => {
  const res = runChild(`
    let result;
    try {
      await import(${JSON.stringify(pathToFileURL(path.join(packageRoot, "dist", "index.js")).href)});
      result = { imported: true };
    } catch (err) {
      result = { imported: false, msg: String(err?.message ?? err).slice(0, 120) };
    }
    console.log(JSON.stringify(result));
  `);
  assert.equal(res.status, 0);
  const out = JSON.parse(res.stdout.trim().split("\n").at(-1)!);
  // The root re-exports session/queue/storage surfaces that statically import
  // node builtins — in a no-Node environment the root import itself must
  // fail, naming a node builtin (module resolution), instead of half-loading.
  assert.equal(out.imported, false, "root import cannot succeed without node builtins today");
  assert.match(out.msg, /node:|simulated-no-node-runtime/);
});
