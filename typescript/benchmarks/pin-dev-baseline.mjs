import { copyFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();
const skipPrepare = process.env.BENCH_SKIP_PREPARE || "0";
const runs = process.env.BENCH_RUNS || "2";
const duration = process.env.BENCH_DURATION || "6";
const warmup = process.env.BENCH_WARMUP || "2";
const connections = process.env.BENCH_CONNECTIONS || "100";

await runNode("./run-comparison.mjs", {
  BENCH_ONLY: "neutron",
  BENCH_SCENARIOS: "static,dynamic",
  BENCH_SKIP_PREPARE: skipPrepare,
  BENCH_RUNS: runs,
  BENCH_DURATION: duration,
  BENCH_WARMUP: warmup,
  BENCH_CONNECTIONS: connections,
});

const sourcePath = path.resolve(cwd, "results/latest.json");
const targetPath = path.resolve(cwd, "results/baseline-dev.json");
await copyFile(sourcePath, targetPath);
console.log(`Pinned dev baseline: ${targetPath}`);
console.log(`Source: ${sourcePath}`);

function runNode(scriptPath, envOverrides = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.resolve(cwd, scriptPath)], {
      cwd,
      stdio: "inherit",
      env: { ...process.env, ...envOverrides },
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`Command failed (${code}): node ${scriptPath}`));
    });
  });
}
