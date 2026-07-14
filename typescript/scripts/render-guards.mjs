// Render guards — regression gate for the shared render core.
//
// Builds the playground with the docker preset, then enforces two things:
//
//   1. GOLDEN HTML: the rendered static (SSG) pages must match a committed
//      snapshot byte-for-byte, after normalizing build-time-nondeterministic
//      timestamps. This is what catches head/HTML output drifting when the
//      shared render core (core/head.ts, render-static.ts, render-app-route.ts)
//      changes. App-route HTML is covered separately by the vitest server e2e
//      + protocol suites; this guards the on-disk SSG path with zero flake.
//
//   2. BUNDLE BUDGET: the gzipped self-contained server bundle must stay under
//      a ceiling. The dev+prod render-core unification traded a fixed ~30 KB
//      (~7.7 KB gz) of bundle weight for one shared pipeline; this makes that
//      weight visible so it can't creep silently on the cold-start-sensitive
//      edge presets.
//
// Usage:
//   node ./scripts/render-guards.mjs           # check against snapshot (CI)
//   node ./scripts/render-guards.mjs --write    # rebuild + rewrite snapshot
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { readFileSync, writeFileSync, existsSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import process from "node:process";

const PLAYGROUND_FILTER = "@neutron/playground";
const PLAYGROUND_DIR = join(process.cwd(), "apps", "playground");
const DIST_DIR = join(PLAYGROUND_DIR, "dist");
const SNAPSHOT_PATH = join(process.cwd(), "scripts", "render-guards.snapshot.json");

// Gzipped server-bundle ceiling. Measured baseline is ~37 KB gz; the ceiling
// leaves headroom for ordinary growth but trips on a step-change regression.
const BUNDLE_GZ_BUDGET_BYTES = 48 * 1024;

const WRITE = process.argv.includes("--write");

function build() {
  console.log(`[render-guards] building ${PLAYGROUND_FILTER} (docker preset)…`);
  const result = spawnSync(
    "pnpm",
    ["--filter", PLAYGROUND_FILTER, "run", "build:docker"],
    { cwd: process.cwd(), stdio: "inherit", shell: true }
  );
  if (result.status !== 0) {
    throw new Error(`playground docker build failed (exit ${result.status})`);
  }
}

// Strip build-time nondeterminism (ISO-8601 timestamps emitted by route
// loaders/components at build time) so the golden compare is stable.
function normalizeHtml(html) {
  return html.replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/g, "<TS>");
}

function walk(dir, predicate) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) {
      out.push(...walk(full, predicate));
    } else if (predicate(full)) {
      out.push(full);
    }
  }
  return out;
}

function collectHtmlHashes() {
  const files = walk(DIST_DIR, (f) => f.endsWith(".html")).sort();
  const hashes = {};
  for (const file of files) {
    const rel = relative(DIST_DIR, file).split(sep).join("/");
    const normalized = normalizeHtml(readFileSync(file, "utf8"));
    hashes[rel] = createHash("sha256").update(normalized).digest("hex");
  }
  return hashes;
}

function measureServerBundleGz() {
  const serverDir = join(DIST_DIR, "server");
  if (!existsSync(serverDir)) {
    throw new Error(`server bundle dir missing: ${serverDir}`);
  }
  const jsFiles = walk(serverDir, (f) => f.endsWith(".js")).sort();
  const combined = Buffer.concat(jsFiles.map((f) => readFileSync(f)));
  return gzipSync(combined).length;
}

function loadSnapshot() {
  if (!existsSync(SNAPSHOT_PATH)) {
    return null;
  }
  return JSON.parse(readFileSync(SNAPSHOT_PATH, "utf8"));
}

function main() {
  build();

  const htmlHashes = collectHtmlHashes();
  const bundleGz = measureServerBundleGz();
  const pageCount = Object.keys(htmlHashes).length;
  console.log(
    `[render-guards] ${pageCount} rendered pages; server bundle ${bundleGz} B gz`
  );

  if (WRITE) {
    const snapshot = { htmlHashes, serverBundleGzBytes: bundleGz };
    writeFileSync(SNAPSHOT_PATH, JSON.stringify(snapshot, null, 2) + "\n");
    console.log(`[render-guards] wrote snapshot → ${relative(process.cwd(), SNAPSHOT_PATH)}`);
    return;
  }

  const snapshot = loadSnapshot();
  if (!snapshot) {
    throw new Error(
      `no snapshot at ${SNAPSHOT_PATH} — run: node ./scripts/render-guards.mjs --write`
    );
  }

  const failures = [];

  // 1. Golden HTML: every snapshotted page must still match; no page dropped.
  for (const [rel, expected] of Object.entries(snapshot.htmlHashes)) {
    if (!(rel in htmlHashes)) {
      failures.push(`missing rendered page: ${rel}`);
    } else if (htmlHashes[rel] !== expected) {
      failures.push(`HTML changed: ${rel}`);
    }
  }
  for (const rel of Object.keys(htmlHashes)) {
    if (!(rel in snapshot.htmlHashes)) {
      failures.push(`new unsnapshotted page: ${rel} (run --write if intended)`);
    }
  }

  // 2. Bundle budget: hard ceiling.
  if (bundleGz > BUNDLE_GZ_BUDGET_BYTES) {
    failures.push(
      `server bundle ${bundleGz} B gz exceeds budget ${BUNDLE_GZ_BUDGET_BYTES} B`
    );
  }

  if (failures.length > 0) {
    console.error("\n[render-guards] FAILED:");
    for (const f of failures) {
      console.error(`  - ${f}`);
    }
    console.error(
      "\nIf a render/output change is intentional, review the diff and run:\n" +
        "  node ./scripts/render-guards.mjs --write\n"
    );
    process.exit(1);
  }

  console.log("[render-guards] OK — golden HTML matches and bundle within budget.");
}

main();
