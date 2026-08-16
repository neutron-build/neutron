#!/usr/bin/env node
// Neutron cross-SDK contract conformance runner.
//
// Boots each available SDK's canonical conformance app on an ephemeral port,
// waits for /health, runs the language-agnostic contract suite, tears the server
// down, and prints a PASS/FAIL matrix (dimension × SDK).
//
// Usage:
//   node run.mjs                 # build + boot + test every available SDK
//   node run.mjs go              # only the named SDK(s)
//   node run.mjs --no-build      # skip build step (use existing binaries)
//   node run.mjs --base=URL      # test an already-running server (no boot)
//
// Exit code is non-zero if any contract dimension FAILS (skips do not fail).

import { spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DIMENSIONS, runContract, waitForHealth } from "./contract.mjs";
import { SDKS } from "./sdks.mjs";

const CONF_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// Contract dimensions an SDK is known not to exercise, each with a reason and a
// hard expiry. Same shape and same rules as `live/known-drift.json`.
//
// A skip used to cost nothing: `run.mjs` said outright "skips do not fail", so
// the matrix reported green while TypeScript left 7 of 12 dimensions untested
// and the adapter self-exempted from a FRAMEWORK_CONTRACT "MUST" in a code
// comment. Recording them turns an invisible gap into a reviewed decision that
// expires.
function loadKnownSkips() {
  const p = path.join(CONF_ROOT, "known-skips.json");
  if (!fs.existsSync(p)) return {};
  try {
    return JSON.parse(fs.readFileSync(p, "utf8")).skips || {};
  } catch (e) {
    console.error(`known-skips.json is unreadable: ${e.message}`);
    process.exit(1);
  }
}

function parseArgs(argv) {
  const opts = { build: true, base: null, only: [], strict: false };
  for (const a of argv.slice(2)) {
    if (a === "--no-build") opts.build = false;
    // --strict: an SDK whose toolchain is missing FAILS instead of warning.
    // For CI, which provisions every toolchain, so "absent" there means the
    // setup silently broke.
    else if (a === "--strict") opts.strict = true;
    else if (a.startsWith("--base=")) opts.base = a.slice("--base=".length);
    else if (!a.startsWith("--")) opts.only.push(a);
  }
  return opts;
}

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

async function bootAndTest(sdk) {
  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  const { command, args } = sdk.cmd();
  const env = { ...process.env, [sdk.portEnv]: String(port) };
  if (sdk.hostEnv) env[sdk.hostEnv] = "127.0.0.1";

  const child = spawn(command, args, { env, stdio: ["ignore", "ignore", "inherit"] });
  let exited = false;
  child.on("exit", () => {
    exited = true;
  });

  try {
    const ready = await waitForHealth(base, 30000);
    if (!ready || exited) {
      return { booted: false, results: [], note: "server did not become healthy" };
    }
    const results = await runContract(base);
    return { booted: true, results, note: "" };
  } finally {
    if (!exited) {
      child.kill("SIGTERM");
      // give graceful shutdown a moment, then SIGKILL.
      await new Promise((r) => setTimeout(r, 1500));
      if (!exited) child.kill("SIGKILL");
    }
  }
}

function glyph(status) {
  return status === "pass" ? "PASS" : status === "fail" ? "FAIL" : "skip";
}

function printMatrix(report) {
  const sdkNames = report.map((r) => r.name);
  const width = Math.max(20, ...DIMENSIONS.map((d) => d.length));
  const col = 8;

  const head = "Dimension".padEnd(width) + " | " + sdkNames.map((n) => n.padEnd(col)).join("| ");
  console.log("\n" + head);
  console.log("-".repeat(head.length));

  for (const dim of DIMENSIONS) {
    let row = dim.padEnd(width) + " | ";
    for (const r of report) {
      const out = r.absent || r.broken;
      const found = out ? null : r.results.find((x) => x.dim === dim);
      const cell = out ? "n/a" : found ? glyph(found.status) : "-";
      row += cell.padEnd(col) + "| ";
    }
    console.log(row);
  }
  console.log("-".repeat(head.length));

  // Per-SDK summary + failing details.
  console.log("");
  for (const r of report) {
    if (r.absent || r.broken) {
      const kind = r.broken ? "BROKEN" : "ABSENT";
      console.log(`[${r.name}] ${kind} — ${r.absent || r.broken}`);
      continue;
    }
    const pass = r.results.filter((x) => x.status === "pass").length;
    const fail = r.results.filter((x) => x.status === "fail").length;
    const skip = r.results.filter((x) => x.status === "skip").length;
    console.log(`[${r.name}] ${pass} pass, ${fail} fail, ${skip} skip${r.note ? " — " + r.note : ""}`);
    for (const x of r.results) {
      if (x.status !== "pass") console.log(`   ${glyph(x.status)} ${x.dim}: ${x.detail}`);
    }
  }
}

async function main() {
  const opts = parseArgs(process.argv);

  // Ad-hoc mode: test an already-running server.
  if (opts.base) {
    const results = await runContract(opts.base);
    printMatrix([{ name: "custom", results, note: "external server " + opts.base }]);
    process.exit(results.some((r) => r.status === "fail") ? 1 : 0);
  }

  let sdks = SDKS;
  if (opts.only.length) sdks = SDKS.filter((s) => opts.only.includes(s.name));

  const report = [];
  for (const sdk of sdks) {
    // Two different things used to collapse into one `unavailable` field, and
    // the difference decides whether the run means anything.
    //
    //   absent — the toolchain is not installed. Legitimate on a dev laptop
    //            that has Go but not Rust; NOT legitimate in CI, which
    //            provisions all of them, so `--strict` fails on it.
    //   broken — the toolchain IS present, we built and booted, and the server
    //            never became healthy (or threw). That is always a defect and
    //            always fails, in CI and locally alike.
    //
    // Collapsing them is how `[python] UNAVAILABLE — server did not become
    // healthy` sat in a GREEN run: the conformance app was dying at import
    // because CI installed three of the SDK's eight dependencies, and an
    // "unavailable" SDK cost nothing. The job is called
    // "contract matrix (go × rust × python × ts)" and was testing three.
    const reason = sdk.available();
    if (reason) {
      report.push({ name: sdk.name, absent: reason, results: [] });
      continue;
    }
    try {
      if (opts.build) {
        console.log(`[${sdk.name}] building conformance app…`);
        sdk.build();
      }
      console.log(`[${sdk.name}] booting…`);
      const r = await bootAndTest(sdk);
      if (!r.booted) {
        report.push({ name: sdk.name, broken: r.note, results: [] });
      } else {
        report.push({ name: sdk.name, results: r.results, note: r.note });
      }
    } catch (e) {
      report.push({ name: sdk.name, broken: String(e.message || e), results: [] });
    }
  }

  printMatrix(report);

  const failed = report.filter((r) => r.results.some((x) => x.status === "fail"));
  const broken = report.filter((r) => r.broken);
  const absent = report.filter((r) => r.absent);

  // Skip accounting, on the same three rules as known-drift.json.
  const known = loadKnownSkips();
  const today = new Date().toISOString().slice(0, 10);
  const unrecordedSkips = [];
  const expiredSkips = [];
  const staleSkips = [];
  for (const r of report) {
    const entries = known[r.name] || {};
    for (const x of r.results) {
      const rec = entries[x.dim];
      if (x.status === "skip") {
        if (!rec) unrecordedSkips.push([r.name, x.dim, x.detail]);
        else if (rec.expires && rec.expires < today) expiredSkips.push([r.name, x.dim, rec.expires]);
      } else if (x.status === "pass" && rec) {
        // The dimension now passes while an entry still says it is skipped.
        staleSkips.push([r.name, x.dim]);
      }
    }
  }
  for (const [sdk, dim, detail] of unrecordedSkips) {
    console.error(
      `\nFAIL [${sdk}] ${dim} was skipped and is not recorded in conformance/known-skips.json` +
        `\n     (${detail}). A skip that costs nothing is invisible; record it with a reason` +
        `\n     and an expiry, or make it pass.`,
    );
  }
  for (const [sdk, dim, exp] of expiredSkips) {
    console.error(`\nFAIL [${sdk}] ${dim} skip expired on ${exp} — close it or re-justify it.`);
  }
  for (const [sdk, dim] of staleSkips) {
    console.error(
      `\nFAIL [${sdk}] ${dim} now PASSES but is still recorded as a known skip.` +
        `\n     Remove the entry — a note describing a state that no longer exists is stale.`,
    );
  }

  for (const r of broken) {
    console.error(
      `\nFAIL [${r.name}] its toolchain is present and its conformance app did not run: ${r.broken}` +
        `\n     An SDK that cannot be booted is UNPROVEN, not passing.`,
    );
  }
  for (const r of absent) {
    const how = opts.strict ? "FAIL" : "warn";
    console.error(`${how} [${r.name}] not exercised — ${r.absent}`);
  }
  if (opts.strict && absent.length) {
    console.error(
      "\n--strict: every SDK must be exercised. CI provisions all of them, so an absent\n" +
        "toolchain there means the setup broke, which must not pass silently.",
    );
  }

  const bad =
    failed.length > 0 ||
    broken.length > 0 ||
    unrecordedSkips.length > 0 ||
    expiredSkips.length > 0 ||
    staleSkips.length > 0 ||
    (opts.strict && absent.length > 0);
  process.exit(bad ? 1 : 0);
}

main();
