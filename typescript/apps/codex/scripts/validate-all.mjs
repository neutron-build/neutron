#!/usr/bin/env node
// Build-time gate: run the codex Python validator against every unit
// before Neutron's content pipeline runs. Fails the build on any unit
// that fails any automated rubric check.

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const codexRoot = resolve(import.meta.dirname, "../../../../../codex");
const validatorPath = resolve(codexRoot, "scripts/validate_all.py");
const venvPython = resolve(codexRoot, ".venv/bin/python");

if (!existsSync(validatorPath)) {
  console.error(`✗ validate-all: cannot find validator at ${validatorPath}`);
  process.exit(1);
}

// Prefer the codex venv (has pyyaml), fall back to system python3.
const python = existsSync(venvPython) ? venvPython : "python3";

const result = spawnSync(python, [validatorPath, "--root", codexRoot], {
  stdio: "inherit",
  cwd: codexRoot,
});

if (result.error) {
  console.error(`✗ validate-all: failed to launch ${python}: ${result.error.message}`);
  process.exit(1);
}

process.exit(result.status ?? 1);
