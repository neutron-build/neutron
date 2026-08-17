#!/usr/bin/env node
// Generate an SDK's contract surface from contract-ir.json (plan step S43).
//
// The bet Phase D is built on: if the contract is machine-readable, the part of
// every SDK that implements it is derivable rather than hand-written, and
// adding an Nth SDK stops being an O(N) transcription job with N chances to
// diverge. This is the end-to-end prototype for one language.
//
// What it emits is the CONTRACT SURFACE — the forced-error endpoints, the
// validation endpoint, the health shape — which is exactly what the conformance
// app is. Everything IR-derived: the error taxonomy and its statuses, titles and
// type URLs, the health keys and nucleus states, the validation shape. Nothing
// about the taxonomy is typed twice.
//
//   node generate-app.mjs python            # print to stdout
//   node generate-app.mjs python --write    # write the generated app
//   node generate-app.mjs python --check    # exit 1 if the written app is stale
//
// `--check` is the part that keeps this honest: a generator nobody re-runs is a
// one-off transcription with extra steps. CI runs it so an IR edit that is not
// reflected in the generated app fails.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const IR = JSON.parse(fs.readFileSync(path.join(HERE, "contract-ir.json"), "utf8"));

const GENERATORS = {
  python: {
    out: path.join(HERE, "adapters/python/_generated_contract.py"),
    render: renderPython,
  },
};

// ── Python ──────────────────────────────────────────────────────────────────

function renderPython() {
  const probed = IR.errors.taxonomy.filter((e) => e.probePath);
  const banner = [
    '"""GENERATED FROM conformance/contract-ir.json — DO NOT EDIT BY HAND.',
    "",
    "Regenerate with:  node conformance/generate-app.mjs python --write",
    "",
    "Plan step S43: the contract surface an SDK must expose is derived from the",
    "machine-readable contract rather than transcribed into each language. The",
    "error taxonomy below is not typed here — it comes from the IR, which",
    "validate-ir.mjs keeps in agreement with FRAMEWORK_CONTRACT.md. Editing this",
    "file by hand reintroduces exactly the drift the IR exists to prevent, and CI",
    "runs `--check` to catch it.",
    '"""',
    "",
    "# (status, code, title, probe_path) for every error in FRAMEWORK_CONTRACT §2.",
    "STANDARD_ERRORS = [",
  ];

  for (const e of IR.errors.taxonomy) {
    const probe = e.probePath ? `"${e.probePath}"` : "None";
    banner.push(`    (${e.status}, "${e.code}", "${e.title}", ${probe}),`);
  }

  banner.push(
    "]",
    "",
    `TYPE_BASE_URL = "${IR.errors.typeBaseUrl}"`,
    `PROBLEM_CONTENT_TYPE = "${IR.errors.contentType}"`,
    `ERROR_REQUIRED_FIELDS = ${JSON.stringify(IR.errors.requiredFields)}`,
    "",
    "# §7 health.",
    `HEALTH_PATH = "${IR.health.path}"`,
    `HEALTH_KEYS = ${JSON.stringify(IR.health.requiredKeys)}`,
    `NUCLEUS_STATES = ${JSON.stringify(IR.health.nucleusStates)}`,
    "",
    "# §2 validation.",
    `VALIDATION_ENDPOINT = "${IR.validation.endpoint}"`,
    `VALIDATION_STATUS = ${IR.validation.status}`,
    `VALIDATION_ERRORS_FIELD = "${IR.validation.errorsArrayField}"`,
    "",
    "# §4 OpenAPI.",
    `OPENAPI_SPEC_PATH = "${IR.openapi.specPath}"`,
    `OPENAPI_VERSION_PREFIX = "${IR.openapi.requiredVersionPrefix}"`,
    "",
    "",
    "def error_type_url(code: str) -> str:",
    '    """The RFC 7807 `type` URI for a standard error code."""',
    "    return TYPE_BASE_URL + code",
    "",
    "",
    "def probed_errors():",
    '    """The errors with a forced-error endpoint.',
    "",
    "    `validation` is excluded on purpose: it has no GET probe because it is",
    "    produced by POSTing an invalid body, and is asserted by the",
    "    `validation.format` dimension instead.",
    '    """',
    "    return [e for e in STANDARD_ERRORS if e[3] is not None]",
    "",
  );
  return banner.join("\n");
}

// ── Driver ──────────────────────────────────────────────────────────────────

const [, , lang, ...flags] = process.argv;
if (!lang || !GENERATORS[lang]) {
  console.error(
    `usage: generate-app.mjs <${Object.keys(GENERATORS).join("|")}> [--write|--check]`,
  );
  process.exit(2);
}

const gen = GENERATORS[lang];
const rendered = gen.render();

if (flags.includes("--write")) {
  fs.mkdirSync(path.dirname(gen.out), { recursive: true });
  fs.writeFileSync(gen.out, rendered, "utf8");
  console.log(`wrote ${path.relative(HERE, gen.out)} (${rendered.split("\n").length} lines)`);
} else if (flags.includes("--check")) {
  if (!fs.existsSync(gen.out)) {
    console.error(`${path.relative(HERE, gen.out)} does not exist — run with --write`);
    process.exit(1);
  }
  const onDisk = fs.readFileSync(gen.out, "utf8");
  if (onDisk !== rendered) {
    console.error(
      `${path.relative(HERE, gen.out)} is stale: contract-ir.json has changed since it was\n` +
        "generated. Regenerate with:  node conformance/generate-app.mjs " +
        `${lang} --write\n\n` +
        "A generator nobody re-runs is a one-off transcription with extra steps.",
    );
    process.exit(1);
  }
  console.log(`${path.relative(HERE, gen.out)} is up to date with contract-ir.json`);
} else {
  process.stdout.write(rendered);
}
