#!/usr/bin/env node
// Validates contract-ir.json — its own shape, and its agreement with the prose.
//
// Plan step S39. The contract lived in FRAMEWORK_CONTRACT.md as prose while
// `contract.mjs` carried its own copy of every constant in it: the error
// taxonomy, the health shape, the nucleus tri-state, the OpenAPI path. Two
// sources of truth for one contract, free to drift, and nothing comparing
// them — the checker's list even omitted the 422 `validation` row without
// saying so.
//
// So a well-formed IR is only half of it. The half that matters is re-parsing
// the DOCUMENT and failing when the two disagree, because that is the drift
// nobody would otherwise notice: someone edits the spec table, the checker
// keeps asserting the old taxonomy, and the matrix stays green while testing
// a contract that no longer exists.
//
//   node validate-ir.mjs          # exit 1 on any problem
//
// Deliberately dependency-free and runnable on its own so repo-hygiene can
// call it without booting a single SDK.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CONF = path.resolve(HERE, "..");
const REPO = path.resolve(CONF, "..");
const IR_PATH = path.join(CONF, "contract-ir.json");

const problems = [];
const fail = (m) => problems.push(m);

// ── Load ────────────────────────────────────────────────────────────────────
let ir;
try {
  ir = JSON.parse(fs.readFileSync(IR_PATH, "utf8"));
} catch (e) {
  console.error(`contract-ir.json is unreadable: ${e.message}`);
  process.exit(1);
}

const docPath = path.join(REPO, ir.source || "FRAMEWORK_CONTRACT.md");
if (!fs.existsSync(docPath)) {
  console.error(`IR names source "${ir.source}" and it does not exist at ${docPath}`);
  process.exit(1);
}
const doc = fs.readFileSync(docPath, "utf8");

// ── Shape ───────────────────────────────────────────────────────────────────
for (const key of ["health", "errors", "validation", "openapi", "middleware", "dimensions"]) {
  if (!ir[key]) fail(`IR is missing the "${key}" section`);
}

if (ir.health) {
  const h = ir.health;
  if (!Array.isArray(h.requiredKeys) || h.requiredKeys.length === 0)
    fail("health.requiredKeys must be a non-empty array");
  if (!Array.isArray(h.nucleusStates) || h.nucleusStates.length === 0)
    fail("health.nucleusStates must be a non-empty array");
  for (const k of h.requiredKeys || [])
    if (!h.types?.[k]) fail(`health.types has no entry for required key "${k}"`);
}

if (ir.errors) {
  const seenCode = new Set();
  const seenStatus = new Set();
  for (const e of ir.errors.taxonomy || []) {
    for (const f of ["status", "code", "title"])
      if (e[f] === undefined) fail(`error taxonomy entry ${JSON.stringify(e)} is missing "${f}"`);
    if (seenCode.has(e.code)) fail(`error code "${e.code}" appears twice in the IR`);
    if (seenStatus.has(e.status)) fail(`error status ${e.status} appears twice in the IR`);
    seenCode.add(e.code);
    seenStatus.add(e.status);
  }
  if (!ir.errors.typeBaseUrl?.endsWith("/"))
    fail("errors.typeBaseUrl should end with '/' so suffixes concatenate cleanly");
}

// Dimension ids must be unique, and every one must name a section.
const dimIds = new Set();
for (const d of ir.dimensions || []) {
  if (dimIds.has(d.id)) fail(`dimension "${d.id}" is declared twice`);
  dimIds.add(d.id);
  if (!d.section) fail(`dimension "${d.id}" does not say which contract section it comes from`);
  if (!d.asserts) fail(`dimension "${d.id}" does not say what it asserts`);
}

// ── Agreement with the prose ────────────────────────────────────────────────

// §2 error taxonomy: | 400 | `bad-request` | Bad Request |
const docErrors = [...doc.matchAll(/^\|\s*(\d{3})\s*\|\s*`([a-z-]+)`\s*\|\s*([^|]+?)\s*\|$/gm)].map(
  (m) => ({ status: Number(m[1]), code: m[2], title: m[3].trim() }),
);
if (docErrors.length === 0) {
  fail("could not find the §2 error taxonomy table in the document — the parser or the table changed");
} else {
  const irByCode = new Map((ir.errors?.taxonomy || []).map((e) => [e.code, e]));
  for (const d of docErrors) {
    const got = irByCode.get(d.code);
    if (!got) {
      fail(`document defines error "${d.code}" (${d.status}) and the IR does not`);
      continue;
    }
    if (got.status !== d.status)
      fail(`error "${d.code}": document says ${d.status}, IR says ${got.status}`);
    if (got.title !== d.title)
      fail(`error "${d.code}": document title "${d.title}", IR title "${got.title}"`);
    irByCode.delete(d.code);
  }
  for (const leftover of irByCode.keys())
    fail(`IR defines error "${leftover}" which the document's table does not`);
}

// §5 middleware order: a numbered list.
const mwSection = doc.split("## 5. Middleware Order")[1]?.split("\n## ")[0] ?? "";
const docOrder = [...mwSection.matchAll(/^\s*\d+\.\s+\*{0,2}([^*\n]+?)\*{0,2}\s*$/gm)].map((m) =>
  m[1].trim(),
);
if (docOrder.length === 0) {
  fail("could not find the §5 middleware order list in the document");
} else {
  const irOrder = ir.middleware?.order || [];
  if (irOrder.length !== docOrder.length)
    fail(`middleware order: document has ${docOrder.length} layers, IR has ${irOrder.length}`);
  docOrder.forEach((layer, i) => {
    if (irOrder[i] !== layer)
      fail(`middleware layer ${i + 1}: document "${layer}", IR "${irOrder[i] ?? "(missing)"}"`);
  });
}

// §7 health: the nucleus tri-state is spelled out in the document's example line.
const healthLine = doc.match(/GET \/health → 200 \{[^}]*\}/);
if (!healthLine) {
  fail("could not find the §7 health example in the document");
} else {
  for (const state of ir.health?.nucleusStates || []) {
    if (!healthLine[0].includes(`"${state}"`))
      fail(`health state "${state}" is in the IR but not in the document's §7 example`);
  }
  for (const key of ir.health?.requiredKeys || []) {
    if (!healthLine[0].includes(`"${key}"`))
      fail(`health key "${key}" is in the IR but not in the document's §7 example`);
  }
}

// The OpenAPI spec path must be the one the document names.
if (ir.openapi?.specPath && !doc.includes(ir.openapi.specPath))
  fail(`IR openapi.specPath "${ir.openapi.specPath}" does not appear in the document`);

// ── Report ──────────────────────────────────────────────────────────────────
if (problems.length) {
  console.error(`contract-ir.json does not agree with ${ir.source}:\n`);
  for (const p of problems) console.error(`  FAIL  ${p}`);
  console.error(
    `\n${problems.length} problem(s). The IR and the prose contract must say the same thing —\n` +
      "that is the entire point of the IR. Fix whichever one is wrong.",
  );
  process.exit(1);
}

console.log(
  `contract-ir.json OK — ${ir.dimensions.length} dimensions, ` +
    `${ir.errors.taxonomy.length} error codes, ${ir.middleware.order.length} middleware layers, ` +
    `all matching ${ir.source}.`,
);
