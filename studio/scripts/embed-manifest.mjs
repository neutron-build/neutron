#!/usr/bin/env node
// Studio build manifest for the CLI embed (orm-program R02, VERIFICATION V17).
//
//   node scripts/embed-manifest.mjs            write dist/studio-manifest.json
//   node scripts/embed-manifest.mjs --check [dist]
//                                              verify a dist tree against its
//                                              manifest and the current sources
//
// `npm run build` writes the manifest after `vite build`. The CLI embeds
// dist/ with it (cli/internal/studio/embed.go): the server refuses to start
// a Studio whose embedded files differ from the manifest, and the CLI's Go
// tests fail when the manifest's source hash is not the hash of the Studio
// sources in the checkout — a stale cli/internal/studio/dist is an error,
// not a silently older UI.
//
// Manifest (format neutron-studio-embed/1, keys sorted, two-space JSON):
//   source  sha256 over the build inputs: index.html, package.json,
//           package-lock.json, tsconfig.json, vite.config.ts and every file
//           under src/ and public/, excluding test files (basename contains
//           ".test." or is test-setup.ts) and any path segment starting with
//           ".". Each input contributes `<posix path>\0<sha256 hex of its
//           bytes with CRLF normalized to LF>\n`, in byte order of the path.
//   files   every file in dist/ except the manifest itself: posix path ->
//           sha256 hex of its exact bytes.
// cli/internal/studio/embed.go implements the same two hashes.

import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const MANIFEST_NAME = "studio-manifest.json";
export const MANIFEST_FORMAT = "neutron-studio-embed/1";
const ROOT_INPUTS = ["index.html", "package.json", "package-lock.json", "tsconfig.json", "vite.config.ts"];
const TREE_INPUTS = ["src", "public"];

const studioDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const byteOrder = (a, b) => (Buffer.compare(Buffer.from(a), Buffer.from(b)));

function walk(dir, rel, out) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const childRel = rel ? `${rel}/${entry.name}` : entry.name;
    const child = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(child, childRel, out);
    else if (entry.isFile()) out.push([childRel, child]);
  }
  return out;
}

function isSourceInput(rel) {
  const segments = rel.split("/");
  if (segments.some((s) => s.startsWith("."))) return false;
  const base = segments[segments.length - 1];
  return !base.includes(".test.") && base !== "test-setup.ts";
}

export function sourceHash(dir = studioDir) {
  const inputs = [];
  for (const name of ROOT_INPUTS) {
    const file = path.join(dir, name);
    if (existsSync(file)) inputs.push([name, file]);
  }
  for (const tree of TREE_INPUTS) {
    const root = path.join(dir, tree);
    if (existsSync(root) && statSync(root).isDirectory()) {
      for (const [rel, file] of walk(root, tree, [])) if (isSourceInput(rel)) inputs.push([rel, file]);
    }
  }
  inputs.sort((a, b) => byteOrder(a[0], b[0]));
  const h = createHash("sha256");
  for (const [rel, file] of inputs) {
    const text = readFileSync(file).toString("latin1").replaceAll("\r\n", "\n");
    h.update(`${rel}\0${sha256(Buffer.from(text, "latin1"))}\n`);
  }
  return h.digest("hex");
}

export function distFiles(distDir) {
  const files = {};
  const entries = walk(distDir, "", []).filter(([rel]) => rel !== MANIFEST_NAME);
  entries.sort((a, b) => byteOrder(a[0], b[0]));
  for (const [rel, file] of entries) files[rel] = sha256(readFileSync(file));
  return files;
}

export function renderManifest(source, files) {
  return `${JSON.stringify({ files, format: MANIFEST_FORMAT, source }, null, 2)}\n`;
}

export function writeManifest(distDir = path.join(studioDir, "dist")) {
  const text = renderManifest(sourceHash(), distFiles(distDir));
  writeFileSync(path.join(distDir, MANIFEST_NAME), text);
  return JSON.parse(text);
}

// Returns a list of problems; empty means dist is exactly the build of the
// current sources.
export function checkDist(distDir = path.join(studioDir, "dist"), dir = studioDir) {
  const problems = [];
  const file = path.join(distDir, MANIFEST_NAME);
  if (!existsSync(file)) return [`${file} is missing — rebuild Studio with npm run build`];
  const manifest = JSON.parse(readFileSync(file, "utf8"));
  if (manifest.format !== MANIFEST_FORMAT) problems.push(`manifest format ${JSON.stringify(manifest.format)}, want ${MANIFEST_FORMAT}`);
  const current = sourceHash(dir);
  if (manifest.source !== current) problems.push(`manifest source ${manifest.source} is not the current sources ${current} (stale build)`);
  const actual = distFiles(distDir);
  for (const [rel, hash] of Object.entries(manifest.files ?? {})) {
    if (!(rel in actual)) problems.push(`listed file missing: ${rel}`);
    else if (actual[rel] !== hash) problems.push(`file differs from the manifest: ${rel}`);
  }
  for (const rel of Object.keys(actual)) if (!(rel in (manifest.files ?? {}))) problems.push(`file not in the manifest: ${rel}`);
  return problems;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  if (args[0] === "--check") {
    const distDir = path.resolve(args[1] ?? path.join(studioDir, "dist"));
    const problems = checkDist(distDir);
    for (const p of problems) console.error(`embed-manifest: ${p}`);
    if (problems.length) process.exit(1);
    console.error(`embed-manifest: ${distDir} matches its manifest and the current sources`);
  } else {
    const m = writeManifest();
    console.error(`embed-manifest: wrote ${MANIFEST_NAME} (${Object.keys(m.files).length} files, source ${m.source.slice(0, 16)})`);
  }
}
