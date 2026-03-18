import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();

await runNode("./run-comparison.mjs", {
  BENCH_TRACK: process.env.BENCH_TRACK || "both",
  BENCH_PROFILE: process.env.BENCH_PROFILE || "baseline",
  BENCH_RUNS: process.env.BENCH_RUNS || "5",
  BENCH_DURATION: process.env.BENCH_DURATION || "20",
  BENCH_WARMUP: process.env.BENCH_WARMUP || "8",
  BENCH_CONFORMANCE: process.env.BENCH_CONFORMANCE || "1",
  BENCH_PAYLOAD_AUDIT: process.env.BENCH_PAYLOAD_AUDIT || "1",
});

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
