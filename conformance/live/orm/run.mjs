#!/usr/bin/env node
// ORM capability runner (orm-program X00 / V18).
//
// Runs probes.mjs through @neutron-build/sql's own driver adapters — pg and
// postgres.js — against the engine at NEUTRON_TEST_DATABASE_URL and prints a
// supported / unsupported / unknown matrix.
//
//   node conformance/live/orm/run.mjs [--driver pg|postgres]... [--only <id-prefix>]
//        [--out report.json] [--check capabilities.nucleus.json]
//        [--write capabilities.nucleus.json] [--control]
//
//   --check    fail when any status differs from the recorded capability file
//              (a change in either direction means the file is stale: re-run
//              with --write against the new engine and review the diff)
//   --write    record this run as the capability file (reproducer links in the
//              previous file are carried over by probe id)
//   --control  fail unless every probe is `supported` — the PostgreSQL control
//              run proving each probe encodes PostgreSQL behaviour
//
// Required-live: a missing URL, a missing neutron-sql build or zero executed
// probes fails the run. Against PostgreSQL a throwaway database is created and
// dropped; against Nucleus the probes use uniquely named objects and expect a
// disposable engine (conformance/live/scripts/start-engine.sh).

import { createHash, randomBytes } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { AssertionError } from "node:assert";
import { PROBES, Precondition, describeError } from "./probes.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, "..", "..", "..");
const PKG = path.join(REPO, "typescript", "packages", "neutron-sql");
const DIST = path.join(PKG, "dist", "index.js");
const URL_ENV = process.env.NEUTRON_TEST_DATABASE_URL;
const PROBE_TIMEOUT_MS = Number(process.env.ORM_PROBE_TIMEOUT_MS ?? 15000);

function parseArgs(argv) {
  const out = { drivers: [], only: [], out: null, check: null, write: null, control: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) fatal(`${a} needs a value`);
      return argv[++i];
    };
    if (a === "--driver") out.drivers.push(next());
    else if (a === "--only") out.only.push(next());
    else if (a === "--out") out.out = next();
    else if (a === "--check") out.check = next();
    else if (a === "--write") out.write = next();
    else if (a === "--control") out.control = true;
    else fatal(`unknown argument ${a}`);
  }
  if (out.drivers.length === 0) out.drivers = ["pg", "postgres"];
  for (const d of out.drivers) if (d !== "pg" && d !== "postgres") fatal(`--driver must be pg or postgres, got ${d}`);
  return out;
}

function fatal(msg) {
  console.error(`orm-live: ${msg}`);
  process.exit(1);
}

const args = parseArgs(process.argv.slice(2));
if (!URL_ENV) fatal("NEUTRON_TEST_DATABASE_URL is not set — refusing to report a run of zero probes");
if (!existsSync(DIST)) fatal(`no neutron-sql build at ${DIST} — run \`pnpm --filter @neutron-build/sql build\` in typescript/`);

const sql = await import(pathToFileURL(DIST).href);
const pkgVersion = (name) => {
  try {
    return JSON.parse(readFileSync(path.join(PKG, "node_modules", name, "package.json"), "utf8")).version;
  } catch {
    return "missing";
  }
};

function git(...a) {
  try {
    return execFileSync("git", a, { cwd: REPO, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
}

function sourceIdentity() {
  const commit = process.env.NUCLEUS_SOURCE_SHA || git("rev-parse", "HEAD");
  const nucleusTree = git("rev-parse", `${commit || "HEAD"}:nucleus`);
  const dirty = git("status", "--porcelain", "--", "nucleus") !== "";
  let binarySha256 = null;
  if (process.env.NUCLEUS_BIN && existsSync(process.env.NUCLEUS_BIN)) {
    binarySha256 = createHash("sha256").update(readFileSync(process.env.NUCLEUS_BIN)).digest("hex");
  }
  return { commit, nucleusTree, nucleusDirty: dirty, binarySha256 };
}

// ---------------------------------------------------------------------------
// Target preparation
// ---------------------------------------------------------------------------

async function adminDriver(url) {
  return sql.loadDriver(url, { driver: "pg", max: 1, connectTimeout: 5 });
}

const admin = await adminDriver(URL_ENV);
const identity = sql.parseVersionString(String((await admin.query("select version() as v"))[0].v));
let targetUrl = URL_ENV;
let ownedDb = null;
if (identity.product === "postgres") {
  ownedDb = `x00_orm_${process.pid}_${randomBytes(3).toString("hex")}`;
  await admin.execute(`create database "${ownedDb}"`);
  const u = new URL(URL_ENV);
  u.pathname = `/${ownedDb}`;
  targetUrl = u.toString();
}
const createdRoles = new Set();

// ---------------------------------------------------------------------------
// Probe execution
// ---------------------------------------------------------------------------

function withTimeout(promise, ms) {
  let timer;
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(Object.assign(new Error(`no answer within ${ms} ms`), { timeout: true })), ms);
    }),
  ]);
}

async function closeQuietly(drv) {
  try {
    await withTimeout(drv.close(), 3000);
  } catch {
    // A connection stuck on a hung statement is abandoned, not awaited.
  }
}

function classify(err) {
  if (err instanceof Precondition) return { status: "unknown", detail: err.message };
  if (err?.timeout) return { status: "unsupported", detail: `hang: ${err.message} (PostgreSQL answers immediately)` };
  if (err instanceof sql.ConnectionFailedError || sql.isConnectionError?.(err)) {
    return { status: "unknown", detail: `connection lost: ${describeError(err)}` };
  }
  if (err instanceof AssertionError) return { status: "unsupported", detail: `wrong result: ${err.message.split("\n").slice(0, 6).join(" ").slice(0, 600)}` };
  if (err?.sqlstate) return { status: "unsupported", detail: `server error: ${describeError(err)}`, sqlstate: err.sqlstate };
  return { status: "unsupported", detail: `error: ${describeError(err)}` };
}

async function runProbe(probe, driverKind) {
  const sessions = [];
  const tag = randomBytes(3).toString("hex");
  const ctx = {
    sql,
    driverKind,
    name: (p) => `x00${p}_${tag}`,
    role: (name) => {
      createdRoles.add(name);
      return name;
    },
    session: async ({ max = 1 } = {}) => {
      const d = await sql.loadDriver(targetUrl, { driver: driverKind, max, connectTimeout: 5 });
      sessions.push(d);
      return d;
    },
    orm: async (tables, relations) => {
      const d = await sql.loadDriver(targetUrl, { driver: driverKind, max: 2, connectTimeout: 5 });
      sessions.push(d);
      return sql.createDatabase({ driver: d, tables, relations });
    },
    identity: async () => identity,
  };
  const started = Date.now();
  let result;
  try {
    const out = await withTimeout(probe.run(ctx), PROBE_TIMEOUT_MS);
    if (out && typeof out === "object" && "status" in out) result = out;
    else result = { status: "supported", detail: typeof out === "string" ? out : "" };
  } catch (err) {
    result = classify(err);
  }
  await Promise.all(sessions.map(closeQuietly));
  return { ...result, ms: Date.now() - started };
}

const selected = PROBES.filter((p) => args.only.length === 0 || args.only.some((o) => p.id.startsWith(o)));
if (selected.length === 0) fatal("no probes selected");

const results = new Map(selected.map((p) => [p.id, { id: p.id, area: p.area, title: p.title, status: {}, detail: {} }]));
for (const driverKind of args.drivers) {
  process.stderr.write(`[orm-live] ${identity.product} ${identity.version} via ${driverKind}: ${selected.length} probes\n`);
  for (const probe of selected) {
    const r = await runProbe(probe, driverKind);
    const entry = results.get(probe.id);
    entry.status[driverKind] = r.status;
    entry.detail[driverKind] = r.detail ?? "";
    if (r.sqlstate) (entry.sqlstate ??= {})[driverKind] = r.sqlstate;
    if (process.env.ORM_LIVE_VERBOSE) process.stderr.write(`  ${probe.id} ${r.status} (${r.ms} ms) ${r.detail ?? ""}\n`);
  }
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

if (ownedDb) {
  await admin.execute(`drop database if exists "${ownedDb}" with (force)`).catch((e) => console.error(`orm-live: could not drop ${ownedDb}: ${describeError(e)}`));
}
for (const role of createdRoles) {
  if (!/^x00s_[0-9a-f]{6}_app$/.test(role)) continue;
  await admin.execute(`drop role if exists ${role}`).catch((e) => {
    if (identity.product === "postgres") console.error(`orm-live: could not drop role ${role}: ${describeError(e)}`);
  });
}
await admin.close();

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

const list = [...results.values()];
const counts = {};
for (const e of list) for (const d of args.drivers) counts[`${d}:${e.status[d]}`] = (counts[`${d}:${e.status[d]}`] ?? 0) + 1;

const width = Math.max(...list.map((e) => e.id.length));
console.log("probe".padEnd(width) + " | " + args.drivers.map((d) => d.padEnd(11)).join(" | "));
console.log("-".repeat(width + 3 + args.drivers.length * 14));
for (const e of list) console.log(e.id.padEnd(width) + " | " + args.drivers.map((d) => e.status[d].padEnd(11)).join(" | "));
console.log();
console.log(`[orm-live] ${identity.raw}`);
console.log(`[orm-live] ${JSON.stringify(counts)}`);

const report = {
  reportVersion: 1,
  generatedAt: new Date().toISOString().slice(0, 10),
  engine: { product: identity.product, version: identity.version, raw: identity.raw },
  source: identity.product === "nucleus" ? sourceIdentity() : null,
  drivers: Object.fromEntries(args.drivers.map((d) => [d, pkgVersion(d)])),
  runtime: { node: process.version, neutronSql: JSON.parse(readFileSync(path.join(PKG, "package.json"), "utf8")).version },
  summary: counts,
  capabilities: list,
};

if (args.out) writeFileSync(args.out, JSON.stringify(report, null, 2) + "\n");

let failed = false;

if (args.control) {
  for (const e of list) {
    for (const d of args.drivers) {
      if (e.status[d] !== "supported") {
        console.error(`::error::control run: ${e.id} (${d}) is ${e.status[d]} on ${identity.product} ${identity.version} — the probe does not encode PostgreSQL behaviour: ${e.detail[d]}`);
        failed = true;
      }
    }
  }
}

if (args.check) {
  const recorded = JSON.parse(readFileSync(path.resolve(args.check), "utf8"));
  if (recorded.engine?.product !== identity.product) {
    console.error(`::error::capability file records ${recorded.engine?.product}, connected engine is ${identity.product}`);
    failed = true;
  }
  if (identity.product === "nucleus") {
    const tree = sourceIdentity().nucleusTree;
    if (tree && recorded.source?.nucleusTree && tree !== recorded.source.nucleusTree) {
      console.log(`::notice::verdicts measured at nucleus tree ${recorded.source.nucleusTree}, checking ${tree}: statuses are re-verified below; re-record with --write when the engine change is deliberate`);
    }
  }
  const byId = new Map(recorded.capabilities.map((c) => [c.id, c]));
  for (const e of list) {
    const rec = byId.get(e.id);
    if (!rec) {
      console.error(`::error::${e.id} is not recorded in ${args.check} — run with --write and review`);
      failed = true;
      continue;
    }
    for (const d of args.drivers) {
      if (rec.status?.[d] !== e.status[d]) {
        console.error(`::error::${e.id} (${d}): recorded ${rec.status?.[d]}, observed ${e.status[d]} — ${e.detail[d]}. The capability file is stale: re-run with --write against this engine and review the diff.`);
        failed = true;
      }
    }
  }
  if (args.only.length === 0) {
    for (const id of byId.keys()) {
      if (!results.has(id)) {
        console.error(`::error::${id} is recorded in ${args.check} but no probe exists — remove it`);
        failed = true;
      }
    }
  }
}

if (args.write) {
  const target = path.resolve(args.write);
  const previous = existsSync(target) ? JSON.parse(readFileSync(target, "utf8")) : null;
  const carried = new Map((previous?.capabilities ?? []).filter((c) => c.reproducer).map((c) => [c.id, c.reproducer]));
  const doc = {
    $comment:
      "V18 capability report for @neutron-build/sql against a named Nucleus build, generated by conformance/live/orm/run.mjs --write. " +
      "status per driver is supported / unsupported / unknown; detail is the observed evidence. .github/workflows/orm-live.yml re-runs the probes with --check " +
      "and fails on any status change, so this file cannot silently go stale. Consumers (X01-X06, Studio) read it; never hand-edit a status.",
    ...report,
    capabilities: list.map((e) => (carried.has(e.id) ? { ...e, reproducer: carried.get(e.id) } : e)),
  };
  writeFileSync(target, JSON.stringify(doc, null, 2) + "\n");
  console.log(`[orm-live] wrote ${target}`);
}

process.exit(failed ? 1 : 0);
