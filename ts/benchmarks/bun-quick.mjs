import { spawn, spawnSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";
import autocannon from "autocannon";

const BENCH_DIR = process.cwd();
const NEUTRON_ROOT = path.resolve(BENCH_DIR, "..");

const CONFIG = {
  connections: intEnv("BUN_BENCH_CONNECTIONS", 60),
  durationSec: intEnv("BUN_BENCH_DURATION", 4),
  warmupSec: intEnv("BUN_BENCH_WARMUP", 1),
  runs: intEnv("BUN_BENCH_RUNS", 1),
  readyTimeoutMs: intEnv("BUN_BENCH_READY_TIMEOUT_MS", 60000),
  install: process.env.BUN_BENCH_INSTALL === "1",
};

const MUTATION_BODY = JSON.stringify({ seed: 13, repeat: 6000 });
const AUTH_HEADER = "Bearer valid-token";

const SCENARIOS = [
  { id: "static", request: { path: "/", method: "GET" } },
  { id: "dynamic", request: { path: "/users/1", method: "GET" } },
  { id: "compute", request: { path: "/compute", method: "GET" } },
  { id: "big", request: { path: "/big", method: "GET" } },
  {
    id: "mutate",
    request: {
      path: "/api/mutate",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: MUTATION_BODY,
    },
  },
  { id: "login", request: { path: "/login", method: "GET" } },
  {
    id: "protected",
    request: {
      path: "/protected",
      method: "GET",
      headers: {
        Authorization: AUTH_HEADER,
      },
    },
  },
  {
    id: "session-refresh",
    request: {
      path: "/api/session/refresh",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        Authorization: AUTH_HEADER,
      },
      body: "{}",
    },
  },
];

const FRAMEWORKS = [
  {
    id: "neutron",
    label: "Neutron (Bun)",
    cwd: path.join(NEUTRON_ROOT, "apps", "playground"),
    installSteps: [],
    buildSteps: [["pnpm", "run", "build"]],
    startCommand: "bun ../../packages/neutron-cli/dist/index.js start --port 3004 --host 127.0.0.1",
    startEnv: { NODE_ENV: "production", NEUTRON_RUNTIME: "preact" },
    baseUrl: "http://127.0.0.1:3004",
  },
  {
    id: "next",
    label: "Next.js (Bun)",
    cwd: path.join(BENCH_DIR, "next-app"),
    installSteps: [["pnpm", "install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["pnpm", "run", "build"]],
    startCommand: "bun ./node_modules/next/dist/bin/next start -p 3001",
    startEnv: { NODE_ENV: "production" },
    baseUrl: "http://127.0.0.1:3001",
  },
  {
    id: "astro",
    label: "Astro (Bun)",
    cwd: path.join(BENCH_DIR, "astro-app"),
    installSteps: [["pnpm", "install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["pnpm", "run", "build"]],
    startCommand: "bun ./dist/server/entry.mjs",
    startEnv: { NODE_ENV: "production", PORT: "3003", HOST: "127.0.0.1" },
    baseUrl: "http://127.0.0.1:3003",
  },
];

async function main() {
  ensureBunAvailable();

  console.log("Running Bun quick benchmark (isolated output).");
  console.log(
    `Config: connections=${CONFIG.connections}, duration=${CONFIG.durationSec}s, warmup=${CONFIG.warmupSec}s, runs=${CONFIG.runs}`
  );

  for (const framework of FRAMEWORKS) {
    console.log(`\n=== Prepare ${framework.label} ===`);
    if (CONFIG.install) {
      for (const step of framework.installSteps) {
        await runCommand(step[0], step.slice(1), framework.cwd);
      }
    }
    for (const step of framework.buildSteps) {
      await runCommand(step[0], step.slice(1), framework.cwd);
    }
  }

  const rows = [];
  for (const framework of FRAMEWORKS) {
    console.log(`\n=== Start ${framework.label} ===`);
    const child = startCommand(framework.startCommand, framework.cwd, framework.startEnv);
    try {
      await waitForUrl(`${framework.baseUrl}/`);
      for (const scenario of SCENARIOS) {
        const spec = scenario.request;
        const url = `${framework.baseUrl}${spec.path}`;
        await runAutocannon(url, CONFIG.warmupSec, spec);

        const trials = [];
        for (let run = 1; run <= CONFIG.runs; run += 1) {
          const result = await runAutocannon(url, CONFIG.durationSec, spec);
          const metrics = toMetrics(result);
          trials.push(metrics);
        }

        const payloadSample = await sampleResponse(url, spec);
        if (payloadSample.status >= 400) {
          throw new Error(
            `${framework.label} returned status ${payloadSample.status} for ${scenario.id} (${spec.method} ${spec.path})`
          );
        }
        const medianMetrics = summarizeTrials(trials);
        rows.push({
          runtime: "bun",
          frameworkId: framework.id,
          framework: framework.label,
          scenario: scenario.id,
          method: String(spec.method || "GET").toUpperCase(),
          path: spec.path || "/",
          requestsPerSec: medianMetrics.requestsPerSec,
          p50Ms: medianMetrics.p50Ms,
          p95Ms: medianMetrics.p95Ms,
          p99Ms: medianMetrics.p99Ms,
          throughputMBps: medianMetrics.throughputMBps,
          payloadSample,
        });
      }
    } finally {
      stopProcessTree(child);
      await delay(1200);
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const resultsDir = path.join(BENCH_DIR, "results");
  await mkdir(resultsDir, { recursive: true });
  const outPath = path.join(resultsDir, `bun-quick-${stamp}.json`);
  await writeFile(
    outPath,
    JSON.stringify(
      {
        timestamp: new Date().toISOString(),
        config: CONFIG,
        runtime: "bun",
        frameworks: FRAMEWORKS.map((f) => f.id),
        scenarios: SCENARIOS.map((s) => s.id),
        summary: rows,
      },
      null,
      2
    ),
    "utf8"
  );

  console.log("\n=== Bun Quick Summary ===");
  console.table(
    rows.map((row) => ({
      framework: row.framework,
      scenario: row.scenario,
      method: row.method,
      path: row.path,
      requestsPerSec: row.requestsPerSec,
      p50Ms: row.p50Ms,
      p95Ms: row.p95Ms,
      payloadBytes: row.payloadSample.contentLength,
    }))
  );
  console.log(`Saved: ${outPath}`);
}

function ensureBunAvailable() {
  const result = spawnSync("bun", ["--version"], {
    shell: true,
    stdio: "pipe",
    encoding: "utf8",
  });
  if (result.status !== 0) {
    throw new Error("Bun is not available in PATH. Install Bun and retry.");
  }
}

function intEnv(name, fallback) {
  const value = process.env[name];
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

async function runCommand(command, args, cwd, env = {}) {
  await new Promise((resolve, reject) => {
    const child = spawn(shellCommand(command, args), {
      cwd,
      shell: true,
      stdio: "inherit",
      env: { ...process.env, ...env },
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`Command failed (${code}): ${command} ${args.join(" ")}`));
    });
  });
}

function startCommand(command, cwd, env = {}) {
  return spawn(command, {
    cwd,
    shell: true,
    stdio: "ignore",
    env: { ...process.env, ...env },
  });
}

function stopProcessTree(child) {
  if (!child || child.exitCode !== null || child.pid === undefined) {
    return;
  }
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(child.pid), "/t", "/f"], { stdio: "ignore" });
  } else {
    child.kill("SIGTERM");
  }
}

function shellCommand(command, args) {
  return [command, ...args].map(quoteArg).join(" ");
}

function quoteArg(arg) {
  if (/^[a-zA-Z0-9_./:@=+-]+$/.test(arg)) {
    return arg;
  }
  return `"${arg.replaceAll('"', '\\"')}"`;
}

async function waitForUrl(url, timeoutMs = CONFIG.readyTimeoutMs) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const res = await fetch(url, { redirect: "manual" });
      if (res.status < 500) return;
    } catch {}
    await delay(300);
  }
  throw new Error(`Server did not become ready: ${url}`);
}

function runAutocannon(url, durationSec, requestSpec = {}) {
  const method = String(requestSpec.method || "GET").toUpperCase();
  const body = method === "GET" || method === "HEAD" ? undefined : requestSpec.body;
  return new Promise((resolve, reject) => {
    const instance = autocannon({
      url,
      method,
      headers: requestSpec.headers || undefined,
      body,
      connections: CONFIG.connections,
      duration: durationSec,
      pipelining: 1,
    });
    instance.on("done", resolve);
    instance.on("error", reject);
  });
}

function toMetrics(result) {
  const requestsPerSec = metricValue(result.requests, ["average", "mean"]);
  const p50Ms = metricValue(result.latency, ["p50", "median", "average", "mean"]);
  const p95Ms = metricValue(result.latency, ["p95", "p90", "average", "mean"]);
  const p99Ms = metricValue(result.latency, ["p99", "max", "average", "mean"]);
  const throughputBytes = metricValue(result.throughput, ["average", "mean"]);

  return {
    requestsPerSec: round(requestsPerSec),
    p50Ms: round(p50Ms),
    p95Ms: round(p95Ms),
    p99Ms: round(p99Ms),
    throughputMBps: round(throughputBytes / (1024 * 1024)),
  };
}

function metricValue(metric, keys) {
  for (const key of keys) {
    const value = metric?.[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }
  return 0;
}

function round(value) {
  return Number(value.toFixed(2));
}

function summarizeTrials(trials) {
  return {
    requestsPerSec: round(median(trials.map((trial) => trial.requestsPerSec))),
    p50Ms: round(median(trials.map((trial) => trial.p50Ms))),
    p95Ms: round(median(trials.map((trial) => trial.p95Ms))),
    p99Ms: round(median(trials.map((trial) => trial.p99Ms))),
    throughputMBps: round(median(trials.map((trial) => trial.throughputMBps))),
  };
}

function median(values) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

async function sampleResponse(url, requestSpec = {}) {
  const method = String(requestSpec.method || "GET").toUpperCase();
  const requestBody = method === "GET" || method === "HEAD" ? undefined : requestSpec.body;
  const res = await fetch(url, {
    method,
    headers: requestSpec.headers || undefined,
    body: requestBody,
    redirect: "manual",
  });
  const body = await res.arrayBuffer();
  return {
    status: res.status,
    contentType: res.headers.get("content-type") || "",
    contentLength: body.byteLength,
  };
}

main().catch((error) => {
  console.error("\nBun quick benchmark failed.");
  console.error(error);
  process.exit(1);
});
