import { spawn } from "node:child_process";
import path from "node:path";
import process from "node:process";

const cwd = process.cwd();

await runNode("./publish-grade.mjs", {
  PUBLISH_REPEATS: process.env.PUBLISH_REPEATS || "1",
  PUBLISH_SETTLE_SEC: process.env.PUBLISH_SETTLE_SEC || "2",
  PUBLISH_IDLE_SAMPLE_SEC: process.env.PUBLISH_IDLE_SAMPLE_SEC || "5",
  BENCH_TRACK: process.env.BENCH_TRACK || "node",
  BENCH_PROFILE: process.env.BENCH_PROFILE || "baseline",
  BENCH_ONLY:
    process.env.BENCH_ONLY || "neutron,neutron-react,next,remix,remix3,astro",
  BENCH_SCENARIOS: process.env.BENCH_SCENARIOS || "static",
  BENCH_RUNS: process.env.BENCH_RUNS || "1",
  BENCH_DURATION: process.env.BENCH_DURATION || "3",
  BENCH_WARMUP: process.env.BENCH_WARMUP || "1",
  BENCH_CONFORMANCE: process.env.BENCH_CONFORMANCE || "0",
  BENCH_PAYLOAD_AUDIT: process.env.BENCH_PAYLOAD_AUDIT || "0",
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
