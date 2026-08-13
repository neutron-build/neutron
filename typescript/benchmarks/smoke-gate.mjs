import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();
const baseline = process.env.BENCH_GATE_BASELINE || "results/baseline.json";
const framework = process.env.BENCH_GATE_FRAMEWORK || "neutron";
const failRpsDropPct = process.env.BENCH_GATE_FAIL_RPS_DROP_PCT || "20";
const failP95IncreasePct = process.env.BENCH_GATE_FAIL_P95_INCREASE_PCT || "35";

// Refuse to compare runs taken under different profiles.
//
// This gate ran for the first time on 2026-08-13 (its workflow had been sitting
// in a directory GitHub never reads) and immediately reported Neutron down
// 28.76% on `dynamic`. It was not a regression. The committed baseline was
// recorded on 2026-02-13 at 100 connections / 20s / 5 runs; the CI smoke
// profile is 80 connections / 5s / 1 run, on a shared runner rather than the
// machine the baseline came from. Fewer connections, a quarter of the duration,
// no averaging and different hardware — a throughput delta is the EXPECTED
// output of that comparison, and it carries no information about the code. The
// p95 in the same row moved the other way, which is the tell.
//
// So the mismatch is now named rather than silently rendered as a verdict. A
// gate that can report a regression that did not happen is worse than no gate:
// it trains everyone to ignore the one that eventually matters.
const base = JSON.parse(await readFile(path.resolve(cwd, baseline), "utf8"));
const latest = JSON.parse(await readFile(path.resolve(cwd, "results/latest.json"), "utf8"));
const shape = (r) =>
  [r?.config?.connections, r?.config?.durationSec, r?.config?.runs].join("/");
if (shape(base) !== shape(latest)) {
  console.error(
    `Regression gate cannot run: the baseline was recorded under a different profile.\n` +
      `  baseline (${baseline}): connections/duration/runs = ${shape(base)}` +
      (base.timestamp ? `  recorded ${base.timestamp}` : "") +
      `\n  this run:              connections/duration/runs = ${shape(latest)}\n` +
      `\nComparing them produces a number that reflects the profile, not the code.\n` +
      `Record a baseline with this profile on this runner class and point\n` +
      `BENCH_GATE_BASELINE at it, or run the benchmark with the baseline's profile.`
  );
  process.exit(2);
}

await runNode("./compare-results.mjs", [
  "--baseline",
  baseline,
  "--framework",
  framework,
  "--fail-rps-drop-pct",
  failRpsDropPct,
  "--fail-p95-increase-pct",
  failP95IncreasePct,
]);

console.log(
  `Smoke regression gate passed for ${framework} (RPS drop <= ${failRpsDropPct}%, p95 increase <= ${failP95IncreasePct}%).`
);

function runNode(scriptPath, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.resolve(cwd, scriptPath), ...args], {
      cwd,
      stdio: "inherit",
      env: process.env,
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`Command failed (${code}): node ${scriptPath} ${args.join(" ")}`));
    });
  });
}
