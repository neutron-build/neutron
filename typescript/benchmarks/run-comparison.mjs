import { spawn, spawnSync } from "node:child_process";
import { access, mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";
import autocannon from "autocannon";

const BENCH_DIR = process.cwd();
const NEUTRON_ROOT = path.resolve(BENCH_DIR, "..");

const PROFILE_PRESETS = {
  baseline: { connections: 100, durationSec: 20, pipelining: 1, warmupSec: 8, runs: 5 },
  stress: { connections: 300, durationSec: 30, pipelining: 1, warmupSec: 10, runs: 5 },
  saturation: { connections: 600, durationSec: 30, pipelining: 1, warmupSec: 10, runs: 3 },
};

const requestedProfile = (process.env.BENCH_PROFILE || "baseline").toLowerCase();
const selectedProfile = PROFILE_PRESETS[requestedProfile] || PROFILE_PRESETS.baseline;

const CONFIG = {
  profile: PROFILE_PRESETS[requestedProfile] ? requestedProfile : "baseline",
  connections: intEnv("BENCH_CONNECTIONS", selectedProfile.connections),
  durationSec: intEnv("BENCH_DURATION", selectedProfile.durationSec),
  pipelining: intEnv("BENCH_PIPELINING", selectedProfile.pipelining),
  warmupSec: intEnv("BENCH_WARMUP", selectedProfile.warmupSec),
  runs: intEnv("BENCH_RUNS", selectedProfile.runs),
  readyTimeoutMs: intEnv("BENCH_READY_TIMEOUT_MS", 60000),
};

const MUTATION_BODY = JSON.stringify({ seed: 13, repeat: 6000 });
const AUTH_HEADER = "Bearer valid-token";
const parsedStaticMemoryMaxKb = Number.parseInt(
  process.env.BENCH_STATIC_MEMORY_MAX_KB || "1024",
  10
);
const STATIC_MEMORY_MAX_KB = Number.isFinite(parsedStaticMemoryMaxKb)
  ? parsedStaticMemoryMaxKb
  : 1024;

const SCENARIO_SETS = {
  node: [
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
  ],
  "optimal-static": [{ id: "static", request: { path: "/", method: "GET" } }],
};

const NODE_FRAMEWORKS = [
  {
    id: "neutron",
    label: "Neutron",
    prepareKey: "neutron-preact",
    cwd: NEUTRON_ROOT,
    installSteps: [],
    buildSteps: [["--filter", "@neutron/playground", "build"]],
    buildEnv: { NEUTRON_RUNTIME: "preact" },
    startArgs: ["--filter", "@neutron/playground", "exec", "neutron", "start", "--port", "3004"],
    startEnv: { NODE_ENV: "production", NEUTRON_RUNTIME: "preact" },
    baseUrl: "http://127.0.0.1:3004",
    supportsSsg: true,
    ssgArtifactPaths: [path.join(NEUTRON_ROOT, "apps", "playground", "dist", "index.html")],
  },
  {
    id: "neutron-react",
    label: "Neutron (React Compat)",
    prepareKey: "neutron-react-compat",
    cwd: NEUTRON_ROOT,
    installSteps: [],
    buildSteps: [["--filter", "@neutron/playground", "build"]],
    buildEnv: { NEUTRON_RUNTIME: "react-compat" },
    startArgs: [
      "--filter",
      "@neutron/playground",
      "exec",
      "neutron",
      "start",
      "--port",
      "3016",
      "--host",
      "127.0.0.1",
    ],
    startEnv: { NODE_ENV: "production", NEUTRON_RUNTIME: "react-compat" },
    baseUrl: "http://127.0.0.1:3016",
    supportsSsg: true,
    ssgArtifactPaths: [path.join(NEUTRON_ROOT, "apps", "playground", "dist", "index.html")],
  },
  {
    id: "next",
    label: "Next.js",
    prepareKey: "next-node",
    cwd: path.join(BENCH_DIR, "next-app"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: ["exec", "next", "start", "-p", "3001"],
    startEnv: { NODE_ENV: "production" },
    baseUrl: "http://127.0.0.1:3001",
    supportsSsg: true,
    ssgArtifactPaths: [path.join(BENCH_DIR, "next-app", ".next", "server", "pages", "index.html")],
  },
  {
    id: "remix",
    label: "Remix 2",
    prepareKey: "remix2-node",
    cwd: path.join(BENCH_DIR, "remix-app"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: ["exec", "remix-serve", "build/index.js"],
    startEnv: { NODE_ENV: "production", PORT: "3002" },
    baseUrl: "http://127.0.0.1:3002",
    supportsSsg: false,
    ssgArtifactPaths: [],
  },
  {
    id: "remix3",
    label: "Remix 3 (RR7)",
    prepareKey: "remix3-node",
    cwd: path.join(BENCH_DIR, "remix3-fw"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: ["exec", "react-router-serve", "./build/server/index.js"],
    startEnv: { NODE_ENV: "production", PORT: "3005" },
    baseUrl: "http://127.0.0.1:3005",
    supportsSsg: false,
    ssgArtifactPaths: [],
  },
  {
    id: "astro",
    label: "Astro",
    prepareKey: "astro-node",
    cwd: path.join(BENCH_DIR, "astro-app"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: ["exec", "node", "./dist/server/entry.mjs"],
    startEnv: { NODE_ENV: "production", PORT: "3003", HOST: "127.0.0.1" },
    baseUrl: "http://127.0.0.1:3003",
    supportsSsg: true,
    ssgArtifactPaths: [],
  },
];

const OPTIMAL_STATIC_FRAMEWORKS = [
  {
    id: "neutron",
    label: "Neutron",
    prepareKey: "neutron-preact",
    cwd: NEUTRON_ROOT,
    installSteps: [],
    buildSteps: [["--filter", "@neutron/playground", "build"]],
    buildEnv: { NEUTRON_RUNTIME: "preact" },
    startArgs: [
      "exec",
      "node",
      "benchmarks/serve-static.mjs",
      "--dir",
      "apps/playground/dist",
      "--port",
      "3104",
      "--host",
      "127.0.0.1",
      "--memory-max-kb",
      String(STATIC_MEMORY_MAX_KB),
    ],
    startEnv: { NODE_ENV: "production" },
    baseUrl: "http://127.0.0.1:3104",
    staticHost: true,
  },
  {
    id: "next",
    label: "Next.js",
    prepareKey: "next-static",
    cwd: path.join(BENCH_DIR, "next-static-app"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: [
      "exec",
      "node",
      "../serve-static.mjs",
      "--dir",
      "out",
      "--port",
      "3101",
      "--host",
      "127.0.0.1",
      "--memory-max-kb",
      String(STATIC_MEMORY_MAX_KB),
    ],
    startEnv: { NODE_ENV: "production" },
    baseUrl: "http://127.0.0.1:3101",
    staticHost: true,
  },
  {
    id: "remix",
    label: "Remix 2",
    prepareKey: "remix2-node",
    cwd: path.join(BENCH_DIR, "remix-app"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: ["exec", "remix-serve", "build/index.js"],
    startEnv: { NODE_ENV: "production", PORT: "3102" },
    baseUrl: "http://127.0.0.1:3102",
    staticHost: false,
  },
  {
    id: "remix3",
    label: "Remix 3 (RR7)",
    prepareKey: "remix3-node",
    cwd: path.join(BENCH_DIR, "remix3-fw"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: ["exec", "react-router-serve", "./build/server/index.js"],
    startEnv: { NODE_ENV: "production", PORT: "3105" },
    baseUrl: "http://127.0.0.1:3105",
    staticHost: false,
  },
  {
    id: "astro",
    label: "Astro",
    prepareKey: "astro-static",
    cwd: path.join(BENCH_DIR, "astro-static-app"),
    installSteps: [["install", "--ignore-workspace", "--frozen-lockfile"]],
    buildSteps: [["run", "build"]],
    startArgs: [
      "exec",
      "node",
      "../serve-static.mjs",
      "--dir",
      "dist",
      "--port",
      "3103",
      "--host",
      "127.0.0.1",
      "--memory-max-kb",
      String(STATIC_MEMORY_MAX_KB),
    ],
    startEnv: { NODE_ENV: "production" },
    baseUrl: "http://127.0.0.1:3103",
    staticHost: true,
  },
];

const TRACKS = {
  node: {
    id: "node",
    label: "Node parity",
    frameworks: NODE_FRAMEWORKS,
    scenarios: SCENARIO_SETS.node,
  },
  "optimal-static": {
    id: "optimal-static",
    label: "Framework-optimal static deployment",
    frameworks: OPTIMAL_STATIC_FRAMEWORKS,
    scenarios: SCENARIO_SETS["optimal-static"],
  },
};

const requestedFrameworkIds = csvEnv("BENCH_ONLY");
const requestedScenarioIds = csvEnv("BENCH_SCENARIOS");
const requestedTrack = (process.env.BENCH_TRACK || "node").toLowerCase();
const verboseServerLogs = process.env.BENCH_VERBOSE_SERVERS === "1";
const debugReadyChecks = process.env.BENCH_DEBUG_READY === "1";
const skipPrepare = process.env.BENCH_SKIP_PREPARE === "1";
const enablePayloadAudit = process.env.BENCH_PAYLOAD_AUDIT !== "0";
const payloadWarnRatio = floatEnv("BENCH_PAYLOAD_WARN_RATIO", 1.3);
const enableConformance = process.env.BENCH_CONFORMANCE !== "0";

const selectedTrackIds =
  requestedTrack === "both"
    ? ["node", "optimal-static"]
    : TRACKS[requestedTrack]
      ? [requestedTrack]
      : ["node"];
const selectedTracks = selectedTrackIds.map((id) => TRACKS[id]);

function intEnv(name, fallback) {
  const value = process.env[name];
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function floatEnv(name, fallback) {
  const value = process.env[name];
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function csvEnv(name) {
  return (process.env[name] || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function runPnpm(args, cwd, env = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(toShellCommand(args), {
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
      reject(new Error(`Command failed (${code}): pnpm ${args.join(" ")}`));
    });
  });
}

function startPnpm(args, cwd, env = {}) {
  const stdio = verboseServerLogs ? ["ignore", "pipe", "pipe"] : "ignore";
  const child = spawn(toShellCommand(args), {
    cwd,
    shell: true,
    stdio,
    env: { ...process.env, ...env },
  });

  if (verboseServerLogs) {
    child.stdout.on("data", (chunk) => {
      process.stdout.write(`[server] ${chunk}`);
    });
    child.stderr.on("data", (chunk) => {
      process.stderr.write(`[server] ${chunk}`);
    });
  }

  return child;
}

function toShellCommand(args) {
  const allArgs = ["pnpm", ...args];
  return allArgs.map(quoteArg).join(" ");
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
      if (debugReadyChecks) {
        console.log(`[ready-check] ${url} -> ${res.status}`);
      }
      if (res.status < 500) {
        return;
      }
      if (debugReadyChecks) {
        const body = await res.text();
        console.log(body.slice(0, 500));
      }
    } catch (error) {
      if (debugReadyChecks) {
        const message = error instanceof Error ? error.message : String(error);
        const causeMessage =
          error instanceof Error && error.cause ? ` | cause: ${String(error.cause)}` : "";
        console.log(`[ready-check] ${url} -> fetch error (${message})`);
        if (causeMessage) {
          console.log(`[ready-check] ${url}${causeMessage}`);
        }
      }
    }
    await delay(300);
  }

  throw new Error(`Server did not become ready: ${url}`);
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
      pipelining: CONFIG.pipelining,
    });

    instance.on("done", resolve);
    instance.on("error", reject);
  });
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

function toMetrics(result) {
  const requestsPerSec = metricValue(result.requests, ["average", "mean"]);
  const p50Ms = metricValue(result.latency, ["p50", "p50_0", "median", "average", "mean"]);
  const p95Ms = metricValue(result.latency, ["p95", "p97_5", "p90", "average", "mean"]);
  const p99Ms = metricValue(result.latency, ["p99", "p99_9", "max", "average", "mean"]);
  const throughputBytes = metricValue(result.throughput, ["average", "mean"]);

  return {
    requestsPerSec: round(requestsPerSec),
    p50Ms: round(p50Ms),
    p95Ms: round(p95Ms),
    p99Ms: round(p99Ms),
    throughputMBps: round(throughputBytes / (1024 * 1024)),
  };
}

function round(value) {
  return Number(value.toFixed(2));
}

function median(values) {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
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

async function installAndBuildTarget(target, preparedKeys) {
  const key = target.prepareKey || `${target.id}:${target.cwd}`;
  if (preparedKeys.has(key)) {
    return;
  }

  for (const step of target.installSteps) {
    await runPnpm(step, target.cwd, { CI: "true" });
  }
  for (const step of target.buildSteps) {
    await runPnpm(step, target.cwd, target.buildEnv || {});
  }

  preparedKeys.add(key);
}

async function benchmarkFrameworkScenario(track, target, scenario) {
  const requestSpec = scenario.request || { path: "/", method: "GET" };
  const requestPath = requestSpec.path || "/";
  const readinessPath = scenario.readinessPath || requestPath;
  const url = `${target.baseUrl}${requestPath}`;
  const child = startPnpm(target.startArgs, target.cwd, target.startEnv || {});
  try {
    await waitForUrl(`${target.baseUrl}${readinessPath}`);
    const payloadSample = enablePayloadAudit ? await sampleResponse(url, requestSpec) : null;
    await runAutocannon(url, CONFIG.warmupSec, requestSpec);

    const trials = [];
    for (let run = 1; run <= CONFIG.runs; run += 1) {
      const result = await runAutocannon(url, CONFIG.durationSec, requestSpec);
      const metrics = toMetrics(result);
      trials.push({ run, ...metrics });
      console.table([
        {
          track: track.id,
          framework: target.label,
          scenario: scenario.id,
          method: String(requestSpec.method || "GET").toUpperCase(),
          path: requestPath,
          run,
          ...metrics,
        },
      ]);
    }

    const medianMetrics = summarizeTrials(trials);
    return {
      track: track.id,
      frameworkId: target.id,
      framework: target.label,
      scenario: scenario.id,
      method: String(requestSpec.method || "GET").toUpperCase(),
      path: requestPath,
      url,
      payloadSample,
      warmupSec: CONFIG.warmupSec,
      runDurationSec: CONFIG.durationSec,
      runs: CONFIG.runs,
      median: medianMetrics,
      trials,
    };
  } finally {
    stopProcessTree(child);
    await delay(1500);
  }
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

function buildSummaryTable(rows) {
  return rows.map((row) => ({
    track: row.track,
    framework: row.framework,
    scenario: row.scenario,
    method: row.method,
    path: row.path,
    requestsPerSec: row.median.requestsPerSec,
    p50Ms: row.median.p50Ms,
    p95Ms: row.median.p95Ms,
    p99Ms: row.median.p99Ms,
    throughputMBps: row.median.throughputMBps,
  }));
}

function buildPayloadAuditRows(rows) {
  const withSamples = rows.filter((row) => row.payloadSample);
  return withSamples.map((row) => ({
    track: row.track,
    framework: row.framework,
    scenario: row.scenario,
    method: row.method,
    path: row.path,
    status: row.payloadSample.status,
    contentType: row.payloadSample.contentType || "(none)",
    contentLength: row.payloadSample.contentLength,
  }));
}

function findPayloadParityWarnings(rows, warnRatio) {
  const grouped = new Map();
  for (const row of rows) {
    if (!row.payloadSample) continue;
    const key = `${row.track}:${row.scenario}`;
    const list = grouped.get(key) || [];
    list.push(row);
    grouped.set(key, list);
  }

  const warnings = [];
  for (const [key, list] of grouped.entries()) {
    const lengths = list
      .map((row) => row.payloadSample.contentLength)
      .filter((value) => Number.isFinite(value) && value > 0);
    if (lengths.length < 2) continue;

    const min = Math.min(...lengths);
    const max = Math.max(...lengths);
    const ratio = max / min;
    if (ratio > warnRatio) {
      const [track, scenario] = key.split(":");
      warnings.push({
        track,
        scenario,
        minBytes: min,
        maxBytes: max,
        ratio: round(ratio),
      });
    }
  }

  return warnings;
}

async function checkSsgSupport(target, staticTrackSuccessByFramework) {
  if (staticTrackSuccessByFramework.has(target.id)) {
    return { pass: true, detail: "served in optimal-static track" };
  }

  if (target.supportsSsg) {
    return { pass: true, detail: "framework supports static generation" };
  }

  const paths = target.ssgArtifactPaths || [];
  for (const filePath of paths) {
    if (await pathExists(filePath)) {
      return { pass: true, detail: path.relative(NEUTRON_ROOT, filePath) };
    }
  }

  return { pass: false, detail: "no static artifact detected" };
}

async function pathExists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function runConformance(nodeFrameworks, staticTrackSuccessByFramework) {
  const rows = [];

  for (const target of nodeFrameworks) {
    const child = startPnpm(target.startArgs, target.cwd, target.startEnv || {});
    try {
      await waitForUrl(`${target.baseUrl}/`);
      const ssg = await checkSsgSupport(target, staticTrackSuccessByFramework);
      const ssr = await checkSSR(target.baseUrl);
      const isrLike = await checkCacheInvalidation(target.baseUrl);
      const streaming = await checkStreaming(target.baseUrl);
      const actions = await checkActions(target.baseUrl);
      const auth = await checkAuth(target.baseUrl);

      rows.push({
        frameworkId: target.id,
        framework: target.label,
        ssg,
        ssr,
        isrLike,
        streaming,
        actions,
        auth,
      });
    } finally {
      stopProcessTree(child);
      await delay(1200);
    }
  }

  const summary = rows.map((row) => ({
    framework: row.framework,
    ssg: row.ssg.pass ? "pass" : "fail",
    ssr: row.ssr.pass ? "pass" : "fail",
    isrLike: row.isrLike.pass ? "pass" : "fail",
    streaming: row.streaming.pass ? "pass" : "fail",
    actions: row.actions.pass ? "pass" : "fail",
    auth: row.auth.pass ? "pass" : "fail",
  }));

  return { rows, summary };
}

async function checkSSR(baseUrl) {
  try {
    const res = await fetch(`${baseUrl}/users/1`, { redirect: "manual" });
    const body = await res.text();
    const contentType = (res.headers.get("content-type") || "").toLowerCase();
    const pass = res.status === 200 && contentType.includes("text/html") && body.length > 0;
    return {
      pass,
      detail: pass
        ? "GET /users/1 returned HTML"
        : `status=${res.status}, contentType=${contentType || "none"}`,
    };
  } catch (error) {
    return { pass: false, detail: formatError(error) };
  }
}

async function checkCacheInvalidation(baseUrl) {
  try {
    const first = await fetchJson(`${baseUrl}/api/cache`, { method: "GET" });
    const second = await fetchJson(`${baseUrl}/api/cache`, { method: "GET" });
    const revalidate = await fetchJson(`${baseUrl}/api/revalidate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const third = await fetchJson(`${baseUrl}/api/cache`, { method: "GET" });

    const firstVersion = Number(first.body?.version ?? 0);
    const secondVersion = Number(second.body?.version ?? 0);
    const thirdVersion = Number(third.body?.version ?? 0);
    const secondHit = String(second.headers.get("x-bench-cache") || "").toUpperCase() === "HIT";
    const revalidated = revalidate.status === 200;
    const invalidated = thirdVersion > secondVersion;
    const pass =
      first.status === 200 &&
      second.status === 200 &&
      third.status === 200 &&
      firstVersion > 0 &&
      secondVersion === firstVersion &&
      secondHit &&
      revalidated &&
      invalidated;

    return {
      pass,
      detail: pass
        ? "cache HIT then version bump after revalidate"
        : `versions=${firstVersion}/${secondVersion}/${thirdVersion}, secondHit=${secondHit}, revalidate=${revalidate.status}`,
    };
  } catch (error) {
    return { pass: false, detail: formatError(error) };
  }
}

async function checkStreaming(baseUrl) {
  try {
    const res = await fetch(`${baseUrl}/api/stream`, { method: "GET", redirect: "manual" });
    const body = await res.text();
    const pass = res.status === 200 && body.includes("chunk-3") && body.includes("stream-end");
    return {
      pass,
      detail: pass ? "stream returned expected chunks" : `status=${res.status}`,
    };
  } catch (error) {
    return { pass: false, detail: formatError(error) };
  }
}

async function checkActions(baseUrl) {
  try {
    const res = await fetch(`${baseUrl}/api/mutate`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: MUTATION_BODY,
      redirect: "manual",
    });
    const payload = await res.json().catch(() => null);
    const pass = res.status === 200 && payload?.ok === true;
    return {
      pass,
      detail: pass ? "POST /api/mutate ok" : `status=${res.status}`,
    };
  } catch (error) {
    return { pass: false, detail: formatError(error) };
  }
}

async function checkAuth(baseUrl) {
  try {
    const login = await fetch(`${baseUrl}/login`, { method: "GET", redirect: "manual" });
    const protectedRes = await fetch(`${baseUrl}/protected`, {
      method: "GET",
      headers: { Authorization: AUTH_HEADER },
      redirect: "manual",
    });
    const refresh = await fetch(`${baseUrl}/api/session/refresh`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: AUTH_HEADER,
        Accept: "application/json",
      },
      body: "{}",
      redirect: "manual",
    });
    const refreshPayload = await refresh.json().catch(() => null);

    const pass =
      login.status === 200 &&
      protectedRes.status === 200 &&
      refresh.status === 200 &&
      refreshPayload?.refreshed === true;

    return {
      pass,
      detail: pass
        ? "login/protected/session-refresh passed"
        : `login=${login.status}, protected=${protectedRes.status}, refresh=${refresh.status}`,
    };
  } catch (error) {
    return { pass: false, detail: formatError(error) };
  }
}

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  const body = await res.json().catch(() => null);
  return { status: res.status, headers: res.headers, body };
}

function formatError(error) {
  return error instanceof Error ? error.message : String(error);
}

async function main() {
  if (selectedTracks.length === 0) {
    throw new Error(`No benchmark track selected. Requested: ${requestedTrack}`);
  }

  const preparedKeys = new Set();
  const rows = [];
  const trackSummaries = [];
  const nodeFrameworksUsed = [];

  if (!skipPrepare) {
    console.log("Installing/building benchmark targets...");
    console.log(
      `Profile: ${CONFIG.profile} (connections=${CONFIG.connections}, duration=${CONFIG.durationSec}s, warmup=${CONFIG.warmupSec}s, runs=${CONFIG.runs})`
    );
  } else {
    console.log("Skipping install/build phase (BENCH_SKIP_PREPARE=1).");
  }

  let hasAnyScenario = false;

  for (const track of selectedTracks) {
    const frameworks =
      requestedFrameworkIds.length === 0
        ? track.frameworks
        : track.frameworks.filter((framework) => requestedFrameworkIds.includes(framework.id));
    const scenarios =
      requestedScenarioIds.length === 0
        ? track.scenarios
        : track.scenarios.filter((scenario) => requestedScenarioIds.includes(scenario.id));

    if (frameworks.length === 0 || scenarios.length === 0) {
      continue;
    }

    hasAnyScenario = true;
    if (track.id === "node") {
      nodeFrameworksUsed.push(...frameworks);
    }

    if (!skipPrepare) {
      for (const target of frameworks) {
        console.log(`\n=== Prepare [${track.id}] ${target.label} ===`);
        await installAndBuildTarget(target, preparedKeys);
      }
    }

    console.log(`\nRunning benchmark track: ${track.label} (${track.id})`);
    for (const scenario of scenarios) {
      for (const target of frameworks) {
        const scenarioPath = scenario.request?.path || "/";
        const scenarioMethod = String(scenario.request?.method || "GET").toUpperCase();
        console.log(
          `\n=== Benchmark [${track.id}] ${target.label} | ${scenario.id} (${scenarioMethod} ${scenarioPath}) ===`
        );
        const row = await benchmarkFrameworkScenario(track, target, scenario);
        rows.push(row);
        console.table(buildSummaryTable([row]));
      }
    }

    trackSummaries.push({
      id: track.id,
      label: track.label,
      frameworks: frameworks.map((framework) => framework.id),
      scenarios: scenarios.map((scenario) => scenario.id),
    });
  }

  if (!hasAnyScenario) {
    throw new Error(
      `No benchmark work selected. Tracks=${selectedTrackIds.join(",")}, frameworks=${requestedFrameworkIds.join(",") || "(all)"}, scenarios=${requestedScenarioIds.join(",") || "(all)"}`
    );
  }

  const summaryTable = buildSummaryTable(rows);

  const trackTargetMap = new Map();
  for (const track of selectedTracks) {
    for (const framework of track.frameworks) {
      trackTargetMap.set(`${track.id}:${framework.id}`, framework);
    }
  }

  const staticTrackSuccessByFramework = new Map();
  for (const row of rows) {
    const trackTarget = trackTargetMap.get(`${row.track}:${row.frameworkId}`);
    if (
      row.track === "optimal-static" &&
      row.scenario === "static" &&
      row.payloadSample?.status === 200 &&
      trackTarget?.staticHost
    ) {
      staticTrackSuccessByFramework.set(row.frameworkId, true);
    }
  }

  const conformance =
    enableConformance && nodeFrameworksUsed.length > 0
      ? await runConformance(uniqueById(nodeFrameworksUsed), staticTrackSuccessByFramework)
      : null;

  const payload = {
    timestamp: new Date().toISOString(),
    config: {
      ...CONFIG,
      requestedTrack,
      tracks: trackSummaries,
      frameworks: [...new Set(rows.map((row) => row.frameworkId))],
      scenarios: [...new Set(rows.map((row) => row.scenario))],
      conformanceEnabled: enableConformance,
    },
    results: rows,
    summary: summaryTable,
    conformance,
  };

  const resultsDir = path.join(BENCH_DIR, "results");
  await mkdir(resultsDir, { recursive: true });
  const stamp = payload.timestamp.replace(/[:.]/g, "-");
  const latestPath = path.join(resultsDir, "latest.json");
  const stampedPath = path.join(resultsDir, `run-${stamp}.json`);

  await writeFile(latestPath, JSON.stringify(payload, null, 2), "utf8");
  await writeFile(stampedPath, JSON.stringify(payload, null, 2), "utf8");

  console.log("\n=== Final Median Summary ===");
  console.table(summaryTable);

  if (enablePayloadAudit) {
    const payloadRows = buildPayloadAuditRows(rows);
    if (payloadRows.length > 0) {
      console.log("\n=== Payload Audit (Single Fetch per Scenario) ===");
      console.table(payloadRows);
      const warnings = findPayloadParityWarnings(rows, payloadWarnRatio);
      if (warnings.length > 0) {
        console.warn(
          `\nPayload parity warnings detected (max/min content-length ratio > ${payloadWarnRatio}).`
        );
        console.table(warnings);
      }
    }
  }

  if (conformance) {
    console.log("\n=== Feature Conformance Matrix ===");
    console.table(conformance.summary);
  }

  console.log(`Saved: ${latestPath}`);
  console.log(`Saved: ${stampedPath}`);
}

function uniqueById(frameworks) {
  const byId = new Map();
  for (const framework of frameworks) {
    if (!byId.has(framework.id)) {
      byId.set(framework.id, framework);
    }
  }
  return [...byId.values()];
}

main().catch((error) => {
  console.error("\nBenchmark run failed.");
  console.error(error);
  process.exit(1);
});
