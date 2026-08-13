#!/usr/bin/env node
// Runs every available SDK executor against one live Nucleus and diffs them.
//
// Each executor prints a JSON document {sdk, specVersion, cases:[{id,status,detail}]}
// on stdout. This orchestrator collects them, prints a matrix, and — the part
// that matters — fails when two SDKs disagree about the same case. A per-SDK
// suite proves each client works. Only the diff proves they work the SAME.
//
//   NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
//       node runner/run.mjs [sdk...]

import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, "..");
const REPO = path.resolve(ROOT, "..", "..");
const URL_ENV = process.env.NEUTRON_TEST_DATABASE_URL;

// Every executor lives at executors/<sdk>/. `available` returns null when the
// executor can run, or a string saying why it cannot. A missing executor is
// reported as missing, never quietly treated as agreement.
const EXECUTORS = [
  {
    sdk: "python",
    entry: "executors/python/run_live.py",
    cmd: () => {
      const venv = path.join(REPO, "python", ".venv", "bin", "python");
      const bin = existsSync(venv) ? venv : "python3";
      return { command: bin, args: [path.join(ROOT, "executors/python/run_live.py")], cwd: path.join(REPO, "python") };
    },
  },
  {
    sdk: "go",
    entry: "executors/go/main.go",
    cmd: () => ({ command: "go", args: ["run", "."], cwd: path.join(ROOT, "executors/go") }),
  },
  {
    sdk: "typescript",
    entry: "executors/typescript/run-live.mjs",
    cmd: () => ({ command: "node", args: [path.join(ROOT, "executors/typescript/run-live.mjs")], cwd: path.join(REPO, "typescript") }),
  },
  {
    sdk: "rust",
    entry: "executors/rust/Cargo.toml",
    cmd: () => ({ command: "cargo", args: ["run", "--release", "--quiet"], cwd: path.join(ROOT, "executors/rust") }),
  },
  {
    sdk: "elixir",
    entry: "executors/elixir/run_live.exs",
    cmd: () => ({ command: "elixir", args: ["run_live.exs"], cwd: path.join(ROOT, "executors/elixir") }),
  },
  {
    sdk: "zig",
    entry: "executors/zig/build.zig",
    cmd: () => ({ command: "zig", args: ["build", "run"], cwd: path.join(ROOT, "executors/zig") }),
  },
  {
    sdk: "julia",
    entry: "executors/julia/run_live.jl",
    cmd: () => ({ command: "julia", args: ["--project=.", "run_live.jl"], cwd: path.join(ROOT, "executors/julia") }),
  },
];

function run({ command, args, cwd }) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd,
      env: { ...process.env, NEUTRON_TEST_DATABASE_URL: URL_ENV },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    // ENOENT here means the language's toolchain is not installed, which is a
    // different fact from "the executor ran and disagreed". Flagged so the
    // caller can report it as unproven rather than as a conformance failure.
    child.on("error", (e) =>
      resolve({ code: 127, out, err: String(e), spawnFailed: e.code === "ENOENT" })
    );
    child.on("close", (code) => resolve({ code, out, err }));
  });
}

const spec = JSON.parse(readFileSync(path.join(ROOT, "spec.json"), "utf8"));
const only = process.argv.slice(2).filter((a) => !a.startsWith("--"));

if (!URL_ENV) {
  console.error(
    "NEUTRON_TEST_DATABASE_URL is not set. This suite is only meaningful\n" +
      "against a live engine; refusing to report a green run for zero cases."
  );
  process.exit(1);
}

const reports = [];
const missing = [];

for (const ex of EXECUTORS) {
  if (only.length && !only.includes(ex.sdk)) continue;
  if (!existsSync(path.join(ROOT, ex.entry))) {
    missing.push(ex.sdk);
    continue;
  }
  process.stderr.write(`[live] ${ex.sdk} …\n`);
  const { out, err, spawnFailed } = await run(ex.cmd());
  if (spawnFailed) {
    // The executor exists; the toolchain to run it does not. Same standing as
    // "no executor at all": UNPROVEN, reported out loud, never counted as
    // agreement — but not a failure, because nothing about the SDK was tested.
    missing.push(`${ex.sdk} (toolchain not installed)`);
    continue;
  }
  try {
    reports.push(JSON.parse(out));
  } catch {
    reports.push({
      sdk: ex.sdk,
      specVersion: spec.specVersion,
      cases: [],
      fatal: (err || out).trim().split("\n").slice(-6).join("\n"),
    });
  }
}

// ── matrix ────────────────────────────────────────────────────────────────
const sdks = reports.map((r) => r.sdk);
const width = Math.max(...spec.cases.map((c) => c.id.length), 4);
const head = "case".padEnd(width) + " | " + sdks.map((s) => s.padEnd(11)).join("| ");
console.log(head);
console.log("-".repeat(head.length));

const byCase = new Map();
for (const r of reports) for (const c of r.cases) byCase.set(`${r.sdk}:${c.id}`, c);

const drift = [];
for (const c of spec.cases) {
  const cells = sdks.map((s) => byCase.get(`${s}:${c.id}`)?.status ?? "absent");
  console.log(c.id.padEnd(width) + " | " + cells.map((x) => x.padEnd(11)).join("| "));
  const distinct = new Set(cells.filter((x) => x !== "absent"));
  if (distinct.size > 1) drift.push({ id: c.id, cells: Object.fromEntries(sdks.map((s, i) => [s, cells[i]])) });
}
console.log();

// The baseline is consulted by BOTH the per-SDK pass below and the drift pass
// at the end. A case listed here is a known, dated, explained gap; reporting it
// as a hard failure in one place and a warning in the other would make the
// suite argue with itself, and a permanently red job is one nobody reads.
const KNOWN = path.join(ROOT, "known-drift.json");
const known = existsSync(KNOWN)
  ? Object.fromEntries((JSON.parse(readFileSync(KNOWN, "utf8")).drift ?? []).map((d) => [d.case, d]))
  : {};
const today = new Date(process.env.LIVE_SUITE_DATE ?? Date.now()).toISOString().slice(0, 10);
const expired = (id) => known[id] && known[id].expires < today;

let failed = false;
for (const r of reports) {
  if (r.fatal) {
    console.error(`::error::${r.sdk} executor did not produce a report:\n${r.fatal}`);
    failed = true;
    continue;
  }
  const counts = {};
  for (const c of r.cases) counts[c.status] = (counts[c.status] ?? 0) + 1;
  console.log(`[${r.sdk}] ${JSON.stringify(counts)}`);
  for (const c of r.cases) {
    // An xpass is ALWAYS an error: the note claiming this is broken is now
    // false, and a stale note is the rot this whole suite exists to prevent.
    // A fail is an error unless it is a recorded, unexpired known gap.
    if (c.status === "xpass") {
      console.error(`::error::${r.sdk} ${c.id}: xpass — ${c.detail ?? ""}`);
      failed = true;
    } else if (c.status === "fail") {
      if (known[c.id] && !expired(c.id)) {
        console.error(`::warning::${r.sdk} ${c.id}: known gap — ${known[c.id].reason}`);
      } else {
        console.error(`::error::${r.sdk} ${c.id}: fail — ${c.detail ?? ""}`);
        failed = true;
      }
    }
  }
  if (r.specVersion !== spec.specVersion) {
    console.error(`::error::${r.sdk} ran spec version ${r.specVersion}, expected ${spec.specVersion}`);
    failed = true;
  }
}

if (missing.length) {
  console.error(`::warning::no executor for: ${missing.join(", ")} — these SDKs are unproven, not passing`);
}

// Drift is the whole point. Two SDKs disagreeing about one case means one of
// them is wrong, and which one is a question the spec exists to force.
//
// But a job that is permanently red teaches everyone to ignore it, which is the
// exact failure this suite was built to end. So drift already recorded in
// known-drift.json with a reason and a hard expiry is a warning; anything new,
// and anything whose expiry has passed, is an error. Same contract as
// .github/workflow-health-exceptions.json: a suppression that cannot expire
// becomes the blind spot it was written to document.

for (const d of drift) {
  const entry = known[d.id];
  if (!entry) {
    console.error(`::error::NEW cross-SDK drift on ${d.id}: ${JSON.stringify(d.cells)}`);
    failed = true;
  } else if (entry.expires < today) {
    console.error(`::error::drift exception for ${d.id} expired on ${entry.expires} — fix it or re-justify it`);
    failed = true;
  } else {
    console.error(`::warning::known drift on ${d.id} (until ${entry.expires}): ${entry.reason}`);
  }
}

// A recorded drift that no longer happens is also stale: it means somebody
// fixed it and the note now describes a state that does not exist.
for (const [id, entry] of Object.entries(known)) {
  if (!drift.some((d) => d.id === id) && reports.length > 1) {
    console.error(`::error::${id} is recorded in known-drift.json but the SDKs now agree — remove the entry (${entry.reason})`);
    failed = true;
  }
}

process.exit(failed ? 1 : 0);
