#!/usr/bin/env node
// Embedded-Studio gate (orm-program R02, VERIFICATION V17).
//
//   node scripts/embed-gate.mjs --cli <neutron binary> [--dist dir] [--out file.json]
//
// Proves that a CLI binary serves exactly the Studio build of the current
// sources, and that the served UI works in a real browser:
//
//   1. dist/ (default studio/dist) matches its studio-manifest.json and the
//      manifest's source hash is the hash of the current Studio sources
//      (scripts/embed-manifest.mjs) — the build is not stale;
//   2. the binary's Studio serves that manifest byte for byte, and every file
//      it lists with the listed sha256 — the binary embeds exactly this build;
//   3. headless Chrome opens the served UI: the connection screen renders,
//      no uncaught page errors, no failed or >= 400 responses.
//
// A binary built from a stale or partial embed fails step 2 (different
// manifest or files) or refuses to start (cli/internal/studio VerifyEmbed).
// Needs Chrome (or CHROME_PATH); no database.

import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { checkDist, MANIFEST_NAME } from "./embed-manifest.mjs";
import { launchChrome, startStudio } from "./cli-studio.mjs";

const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const CLI = opt("--cli", null);
const OUT = opt("--out", null);
const DIST = path.resolve(opt("--dist", path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "dist")));
if (!CLI) {
  console.error("embed-gate: --cli <neutron binary> is required");
  process.exit(2);
}

const results = [];
function verdict(id, pass, detail = "") {
  results.push({ id, pass, detail });
  console.error(`${pass ? "PASS" : "FAIL"} ${id}${detail ? ` :: ${detail}` : ""}`);
}
const sha256 = (b) => createHash("sha256").update(b).digest("hex");

const cleanups = [];
try {
  const problems = checkDist(DIST);
  verdict("V17.build: dist matches its manifest and the current Studio sources", problems.length === 0, problems.slice(0, 5).join("; ") || DIST);
  const localManifest = readFileSync(path.join(DIST, MANIFEST_NAME));
  const manifest = JSON.parse(localManifest.toString("utf8"));

  const studio = await startStudio(CLI);
  cleanups.push(studio.stop);
  const served = Buffer.from(await (await fetch(`${studio.base}/${MANIFEST_NAME}`)).arrayBuffer());
  verdict("V17.embed: the binary serves this build's manifest byte for byte", served.equals(localManifest), `source ${manifest.source.slice(0, 16)}, served ${sha256(served).slice(0, 16)} vs local ${sha256(localManifest).slice(0, 16)}`);
  const mismatched = [];
  for (const [rel, hash] of Object.entries(manifest.files)) {
    const res = await fetch(`${studio.base}/${rel.split("/").map(encodeURIComponent).join("/")}`);
    const body = Buffer.from(await res.arrayBuffer());
    if (!res.ok || sha256(body) !== hash) mismatched.push(`${rel} (${res.status})`);
  }
  verdict("V17.embed: every manifest file is served with its manifest hash", mismatched.length === 0, mismatched.slice(0, 5).join(", ") || `${Object.keys(manifest.files).length} files`);

  const browser = await launchChrome();
  cleanups.push(() => browser.close());
  const page = await (await browser.newContext({ viewport: { width: 1280, height: 800 } })).newPage();
  const pageErrors = [];
  const badResponses = [];
  page.on("pageerror", (e) => pageErrors.push(String(e)));
  page.on("requestfailed", (r) => badResponses.push(`${r.url()} failed: ${r.failure()?.errorText}`));
  page.on("response", (r) => {
    if (r.status() >= 400) badResponses.push(`${r.url()} ${r.status()}`);
  });
  await page.goto(`${studio.base}/`);
  let rendered = false;
  try {
    await page.getByRole("heading", { name: "Neutron Studio" }).waitFor({ timeout: 30000 });
    await page.getByText("No saved connections. Add one above.").waitFor({ timeout: 30000 });
    rendered = true;
  } catch {}
  verdict("V17.ui: the served Studio renders its connection screen in Chrome", rendered, `${await page.title()} ${page.url()}`);
  await page.waitForLoadState("networkidle").catch(() => {});
  verdict("V17.ui: no uncaught page errors, failed or error responses", pageErrors.length === 0 && badResponses.length === 0, [...pageErrors, ...badResponses].slice(0, 5).join(" | ") || "none");
} catch (err) {
  verdict("harness", false, err instanceof Error ? err.stack : String(err));
} finally {
  for (const c of cleanups.reverse()) {
    try {
      await c();
    } catch (err) {
      verdict("cleanup", false, String(err));
    }
  }
}
const failed = results.filter((r) => !r.pass).length;
if (OUT) writeFileSync(OUT, JSON.stringify({ tool: "studio embed gate", platform: `${process.platform}-${process.arch}`, cli: CLI, results }, null, 2) + "\n");
console.error(`embed-gate: ${results.length - failed}/${results.length} passed`);
process.exit(failed === 0 ? 0 : 1);
