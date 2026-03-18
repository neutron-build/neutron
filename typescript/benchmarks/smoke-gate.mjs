import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();
const baseline = process.env.BENCH_GATE_BASELINE || "results/baseline.json";
const framework = process.env.BENCH_GATE_FRAMEWORK || "neutron";
const failRpsDropPct = process.env.BENCH_GATE_FAIL_RPS_DROP_PCT || "20";
const failP95IncreasePct = process.env.BENCH_GATE_FAIL_P95_INCREASE_PCT || "35";

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
