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
import net from "node:net";
import { DIMENSIONS, runContract, waitForHealth } from "./contract.mjs";
import { SDKS } from "./sdks.mjs";

function parseArgs(argv) {
  const opts = { build: true, base: null, only: [] };
  for (const a of argv.slice(2)) {
    if (a === "--no-build") opts.build = false;
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
      const found = r.unavailable ? null : r.results.find((x) => x.dim === dim);
      const cell = r.unavailable ? "n/a" : found ? glyph(found.status) : "-";
      row += cell.padEnd(col) + "| ";
    }
    console.log(row);
  }
  console.log("-".repeat(head.length));

  // Per-SDK summary + failing details.
  console.log("");
  for (const r of report) {
    if (r.unavailable) {
      console.log(`[${r.name}] UNAVAILABLE — ${r.unavailable}`);
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
    const reason = sdk.available();
    if (reason) {
      report.push({ name: sdk.name, unavailable: reason, results: [] });
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
        report.push({ name: sdk.name, unavailable: r.note, results: [] });
      } else {
        report.push({ name: sdk.name, results: r.results, note: r.note });
      }
    } catch (e) {
      report.push({ name: sdk.name, unavailable: String(e.message || e), results: [] });
    }
  }

  printMatrix(report);

  const anyFail = report.some((r) => r.results.some((x) => x.status === "fail"));
  process.exit(anyFail ? 1 : 0);
}

main();
