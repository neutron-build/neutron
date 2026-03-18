import { access } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();
const cli = parseArgs(process.argv.slice(2));
const baselinePath = path.resolve(
  cwd,
  cli.baseline || process.env.BENCH_BASELINE || "results/baseline-dev.json",
);
const failDropPct = cli.failDropPct || process.env.BENCH_FAIL_RPS_DROP_PCT || "";
const skipPrepare = process.env.BENCH_SKIP_PREPARE || "1";
const runs = process.env.BENCH_RUNS || "2";
const duration = process.env.BENCH_DURATION || "6";
const warmup = process.env.BENCH_WARMUP || "2";
const connections = process.env.BENCH_CONNECTIONS || "100";

await ensureBaseline(baselinePath);

await runNode("./run-comparison.mjs", {
  BENCH_TRACK: "node",
  BENCH_ONLY: "neutron",
  BENCH_SCENARIOS: "static,dynamic",
  BENCH_SKIP_PREPARE: skipPrepare,
  BENCH_RUNS: runs,
  BENCH_DURATION: duration,
  BENCH_WARMUP: warmup,
  BENCH_CONNECTIONS: connections,
});

const diffArgs = [
  "--baseline",
  baselinePath,
  "--framework",
  "neutron",
];
if (failDropPct) {
  diffArgs.push("--fail-rps-drop-pct", failDropPct);
}

await runNode("./compare-results.mjs", {}, diffArgs);

console.log("Dev loop benchmark passed.");

async function ensureBaseline(filePath) {
  try {
    await access(filePath);
  } catch {
    console.error(`Missing baseline file: ${filePath}`);
    console.error("Run: pnpm run baseline:pin:dev");
    process.exit(1);
  }
}

function parseArgs(argv) {
  const parsed = {
    baseline: "",
    failDropPct: "",
  };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    const next = argv[i + 1];
    if (token === "--baseline" && next) {
      parsed.baseline = next;
      i += 1;
      continue;
    }
    if (token === "--fail-rps-drop-pct" && next) {
      parsed.failDropPct = next;
      i += 1;
    }
  }
  return parsed;
}

function runNode(scriptPath, envOverrides = {}, args = []) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [path.resolve(cwd, scriptPath), ...args],
      {
        cwd,
        stdio: "inherit",
        env: { ...process.env, ...envOverrides },
      },
    );

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
