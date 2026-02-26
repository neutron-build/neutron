import { spawn, spawnSync } from "node:child_process";
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";

const BENCH_DIR = process.cwd();
const RESULTS_DIR = path.join(BENCH_DIR, "results", "publish-grade");

const CONFIG = {
  repeats: Math.max(1, intEnv("PUBLISH_REPEATS", 3)),
  settleSec: Math.max(0, intEnv("PUBLISH_SETTLE_SEC", 10)),
  idleSampleSec: Math.max(1, intEnv("PUBLISH_IDLE_SAMPLE_SEC", 15)),
  idleMaxCpuPct: clamp(floatEnv("PUBLISH_IDLE_MAX_CPU_PCT", 20), 0, 100),
  bootstraps: Math.max(1, intEnv("PUBLISH_BOOTSTRAPS", 2000)),
  allowBusyHost: process.env.PUBLISH_IDLE_ALLOW_BUSY === "1",
  windowsPriority: normalizeWindowsPriority(process.env.PUBLISH_WINDOWS_PRIORITY || "High"),
  windowsAffinity: process.env.PUBLISH_CPU_AFFINITY || "auto",
};

const BENCH_ENV = {
  BENCH_TRACK: process.env.BENCH_TRACK || "both",
  BENCH_PROFILE: process.env.BENCH_PROFILE || "baseline",
  BENCH_RUNS: process.env.BENCH_RUNS || "5",
  BENCH_DURATION: process.env.BENCH_DURATION || "20",
  BENCH_WARMUP: process.env.BENCH_WARMUP || "8",
  BENCH_CONFORMANCE: process.env.BENCH_CONFORMANCE || "1",
  BENCH_PAYLOAD_AUDIT: process.env.BENCH_PAYLOAD_AUDIT || "1",
};

await mkdir(RESULTS_DIR, { recursive: true });
const runId = timestampToken();

console.log("Publish-grade benchmark protocol");
console.log(
  `Config: repeats=${CONFIG.repeats}, settle=${CONFIG.settleSec}s, idle<=${CONFIG.idleMaxCpuPct}% (${CONFIG.idleSampleSec}s sample), bootstraps=${CONFIG.bootstraps}`
);
console.log(
  `Bench: track=${BENCH_ENV.BENCH_TRACK}, profile=${BENCH_ENV.BENCH_PROFILE}, runs=${BENCH_ENV.BENCH_RUNS}, duration=${BENCH_ENV.BENCH_DURATION}s, warmup=${BENCH_ENV.BENCH_WARMUP}s`
);

await maybeApplyWindowsProcessControls();

const repeatArtifacts = [];
for (let repeat = 1; repeat <= CONFIG.repeats; repeat++) {
  console.log(`\n=== Publish Repeat ${repeat}/${CONFIG.repeats} ===`);

  await assertHostIsIdleOrWarn();
  await runComparison(BENCH_ENV);

  const repeatPath = path.join(
    RESULTS_DIR,
    `repeat-${String(repeat).padStart(2, "0")}-${runId}.json`
  );
  await copyFile(path.join(BENCH_DIR, "results", "latest.json"), repeatPath);
  repeatArtifacts.push(repeatPath);
  console.log(`Saved repeat artifact: ${repeatPath}`);

  if (repeat < CONFIG.repeats) {
    console.log(`Cooling down for ${CONFIG.settleSec}s before next repeat...`);
    await delay(CONFIG.settleSec * 1000);
  }
}

const repeats = await Promise.all(repeatArtifacts.map(readJson));
const summary = buildSummary(repeats, {
  runId,
  createdAt: new Date().toISOString(),
  repeats: CONFIG.repeats,
  benchEnv: BENCH_ENV,
  protocol: {
    settleSec: CONFIG.settleSec,
    idleSampleSec: CONFIG.idleSampleSec,
    idleMaxCpuPct: CONFIG.idleMaxCpuPct,
    bootstraps: CONFIG.bootstraps,
  },
});

const summaryPath = path.join(RESULTS_DIR, `summary-${runId}.json`);
await writeFile(summaryPath, JSON.stringify(summary, null, 2), "utf-8");

const markdownPath = path.join(RESULTS_DIR, `summary-${runId}.md`);
await writeFile(markdownPath, toMarkdown(summary), "utf-8");

console.log("\n=== Publish Summary (Median-of-Medians) ===");
console.table(
  summary.summary.map((entry) => ({
    track: entry.track,
    framework: entry.framework,
    scenario: entry.scenario,
    rps: round(entry.rps.medianOfMedians, 1),
    rpsCi95: `[${round(entry.rps.ci95.low, 1)}, ${round(entry.rps.ci95.high, 1)}]`,
    rpsCovPct: round(entry.rps.covPct, 1),
    p95: round(entry.p95Ms.medianOfMedians, 1),
    p95Ci95: `[${round(entry.p95Ms.ci95.low, 1)}, ${round(entry.p95Ms.ci95.high, 1)}]`,
  }))
);

console.log(`\nSaved JSON summary: ${summaryPath}`);
console.log(`Saved Markdown summary: ${markdownPath}`);

function buildSummary(repeats, metadata) {
  const byKey = new Map();

  for (const repeat of repeats) {
    for (const row of repeat.results || []) {
      const key = [
        row.track || "node",
        row.frameworkId || "",
        row.framework || "",
        row.scenario || "",
        row.method || "",
        row.path || "",
      ].join("|");
      const bucket = byKey.get(key) || {
        track: row.track || "node",
        frameworkId: row.frameworkId || "",
        framework: row.framework || row.frameworkId || "",
        scenario: row.scenario || "",
        method: row.method || "",
        path: row.path || "",
        rps: [],
        p95: [],
      };
      bucket.rps.push(Number(row?.median?.requestsPerSec || 0));
      bucket.p95.push(Number(row?.median?.p95Ms || 0));
      byKey.set(key, bucket);
    }
  }

  const summaryRows = [...byKey.values()].map((bucket) => {
    const rpsStats = summarizeSeries(bucket.rps);
    const p95Stats = summarizeSeries(bucket.p95);
    return {
      track: bucket.track,
      frameworkId: bucket.frameworkId,
      framework: bucket.framework,
      scenario: bucket.scenario,
      method: bucket.method,
      path: bucket.path,
      sampleCount: bucket.rps.length,
      rps: rpsStats,
      p95Ms: p95Stats,
    };
  });

  summaryRows.sort((left, right) => {
    const scenarioOrder = left.scenario.localeCompare(right.scenario);
    if (scenarioOrder !== 0) {
      return scenarioOrder;
    }
    const frameworkOrder = left.framework.localeCompare(right.framework);
    if (frameworkOrder !== 0) {
      return frameworkOrder;
    }
    return left.track.localeCompare(right.track);
  });

  return {
    ...metadata,
    sourceArtifacts: repeatArtifactsToRelativePaths(repeatArtifacts),
    summary: summaryRows,
  };
}

function summarizeSeries(values) {
  const cleaned = values.filter((value) => Number.isFinite(value));
  if (cleaned.length === 0) {
    return {
      medianOfMedians: 0,
      mean: 0,
      stddev: 0,
      covPct: 0,
      min: 0,
      max: 0,
      ci95: { low: 0, high: 0 },
      raw: [],
    };
  }

  const sorted = [...cleaned].sort((a, b) => a - b);
  const medianOfMedians = quantileSorted(sorted, 0.5);
  const mean = sorted.reduce((sum, value) => sum + value, 0) / sorted.length;
  const variance =
    sorted.reduce((sum, value) => sum + (value - mean) ** 2, 0) /
    (sorted.length > 1 ? sorted.length - 1 : 1);
  const stddev = Math.sqrt(Math.max(variance, 0));
  const covPct = mean === 0 ? 0 : (stddev / mean) * 100;
  const ci95 = bootstrapMedianCi(sorted, CONFIG.bootstraps);

  return {
    medianOfMedians,
    mean,
    stddev,
    covPct,
    min: sorted[0],
    max: sorted[sorted.length - 1],
    ci95,
    raw: sorted,
  };
}

function bootstrapMedianCi(sortedValues, iterations) {
  if (sortedValues.length <= 1) {
    const value = sortedValues[0] || 0;
    return { low: value, high: value };
  }

  const medians = [];
  for (let i = 0; i < iterations; i++) {
    const sample = [];
    for (let j = 0; j < sortedValues.length; j++) {
      const index = Math.floor(Math.random() * sortedValues.length);
      sample.push(sortedValues[index]);
    }
    sample.sort((a, b) => a - b);
    medians.push(quantileSorted(sample, 0.5));
  }
  medians.sort((a, b) => a - b);

  return {
    low: quantileSorted(medians, 0.025),
    high: quantileSorted(medians, 0.975),
  };
}

function quantileSorted(sortedValues, quantile) {
  if (sortedValues.length === 0) {
    return 0;
  }
  const index = (sortedValues.length - 1) * quantile;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) {
    return sortedValues[lower];
  }
  const weight = index - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function round(value, digits = 2) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function timestampToken() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

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

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

async function runComparison(env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.resolve(BENCH_DIR, "run-comparison.mjs")], {
      cwd: BENCH_DIR,
      stdio: "inherit",
      env: { ...process.env, ...env },
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`run-comparison failed with exit code ${code}`));
    });
  });
}

async function readJson(filePath) {
  const raw = await readFile(filePath, "utf-8");
  return JSON.parse(raw);
}

function toMarkdown(summary) {
  const lines = [];
  lines.push(`# Publish-Grade Benchmark Summary`);
  lines.push("");
  lines.push(`- runId: \`${summary.runId}\``);
  lines.push(`- createdAt: \`${summary.createdAt}\``);
  lines.push(`- repeats: \`${summary.repeats}\``);
  lines.push(
    `- bench config: track=\`${summary.benchEnv.BENCH_TRACK}\`, profile=\`${summary.benchEnv.BENCH_PROFILE}\`, runs=\`${summary.benchEnv.BENCH_RUNS}\`, duration=\`${summary.benchEnv.BENCH_DURATION}\`, warmup=\`${summary.benchEnv.BENCH_WARMUP}\``
  );
  lines.push("");
  lines.push(
    `| Track | Framework | Scenario | Method | Path | RPS median | RPS CI95 | RPS CoV% | p95 median (ms) | p95 CI95 (ms) |`
  );
  lines.push(`| --- | --- | --- | --- | --- | ---: | --- | ---: | ---: | --- |`);

  for (const row of summary.summary) {
    lines.push(
      `| ${row.track} | ${row.framework} | ${row.scenario} | ${row.method} | ${row.path} | ${round(
        row.rps.medianOfMedians,
        1
      )} | [${round(row.rps.ci95.low, 1)}, ${round(row.rps.ci95.high, 1)}] | ${round(
        row.rps.covPct,
        1
      )} | ${round(row.p95Ms.medianOfMedians, 1)} | [${round(row.p95Ms.ci95.low, 1)}, ${round(
        row.p95Ms.ci95.high,
        1
      )}] |`
    );
  }

  lines.push("");
  lines.push(`## Source Artifacts`);
  for (const artifact of summary.sourceArtifacts || []) {
    lines.push(`- \`${artifact}\``);
  }
  lines.push("");

  return lines.join("\n");
}

function repeatArtifactsToRelativePaths(paths) {
  return paths.map((filePath) => path.relative(BENCH_DIR, filePath).split(path.sep).join("/"));
}

async function assertHostIsIdleOrWarn() {
  const busyPct = await sampleCpuBusyPct(CONFIG.idleSampleSec * 1000);
  const rounded = round(busyPct, 2);
  if (busyPct <= CONFIG.idleMaxCpuPct) {
    console.log(`Host idle check OK: CPU busy ${rounded}%`);
    return;
  }

  const message =
    `Host busy check failed: CPU busy ${rounded}% exceeds ${CONFIG.idleMaxCpuPct}% ` +
    `(sample ${CONFIG.idleSampleSec}s).`;
  if (CONFIG.allowBusyHost) {
    console.warn(`${message} Continuing because PUBLISH_IDLE_ALLOW_BUSY=1.`);
    return;
  }

  throw new Error(`${message} Re-run when host is idle or set PUBLISH_IDLE_ALLOW_BUSY=1.`);
}

async function sampleCpuBusyPct(sampleMs) {
  const start = cpuSnapshot();
  await delay(sampleMs);
  const end = cpuSnapshot();

  let idleDelta = 0;
  let totalDelta = 0;
  const count = Math.min(start.length, end.length);
  for (let i = 0; i < count; i++) {
    const a = start[i];
    const b = end[i];
    const idle = Math.max(0, b.idle - a.idle);
    const total = Math.max(0, b.total - a.total);
    idleDelta += idle;
    totalDelta += total;
  }

  if (totalDelta <= 0) {
    return 0;
  }

  const busy = 1 - idleDelta / totalDelta;
  return Math.max(0, Math.min(100, busy * 100));
}

function cpuSnapshot() {
  return os.cpus().map((cpu) => {
    const times = cpu.times;
    const total = times.user + times.nice + times.sys + times.idle + times.irq;
    return { idle: times.idle, total };
  });
}

async function maybeApplyWindowsProcessControls() {
  if (process.platform !== "win32") {
    return;
  }

  const affinityMask = resolveWindowsAffinityMask(CONFIG.windowsAffinity);
  const commands = [];
  commands.push(`$p = Get-Process -Id ${process.pid}`);

  if (CONFIG.windowsPriority) {
    commands.push(`$p.PriorityClass = '${CONFIG.windowsPriority}'`);
  }
  if (affinityMask !== null) {
    commands.push(`$p.ProcessorAffinity = ${affinityMask}`);
  }

  if (commands.length <= 1) {
    return;
  }

  const result = spawnSync(
    "powershell",
    ["-NoProfile", "-Command", commands.join("; ")],
    {
      cwd: BENCH_DIR,
      stdio: "pipe",
      encoding: "utf-8",
    }
  );

  if (result.status !== 0) {
    const err = (result.stderr || "").trim();
    console.warn(`Windows process controls not applied: ${err || "unknown error"}`);
    return;
  }

  const affinityLabel = affinityMask === null ? "default" : String(affinityMask);
  console.log(
    `Windows process controls applied: priority=${CONFIG.windowsPriority || "default"}, affinity=${affinityLabel}`
  );
}

function resolveWindowsAffinityMask(value) {
  const cpuCount = os.cpus().length;
  if (!value || value.toLowerCase() === "none") {
    return null;
  }

  if (value.toLowerCase() === "auto") {
    const coresToUse = Math.max(1, Math.min(cpuCount, 8));
    let mask = 0n;
    for (let i = 0; i < coresToUse; i++) {
      mask |= 1n << BigInt(i);
    }
    return mask.toString(10);
  }

  const trimmed = value.trim().toLowerCase();
  try {
    if (trimmed.startsWith("0x")) {
      return BigInt(trimmed).toString(10);
    }
    const parsed = BigInt(trimmed);
    return parsed > 0n ? parsed.toString(10) : null;
  } catch {
    console.warn(`Invalid PUBLISH_CPU_AFFINITY="${value}", skipping affinity pin.`);
    return null;
  }
}

function normalizeWindowsPriority(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized || normalized === "none") {
    return null;
  }

  const map = {
    idle: "Idle",
    belownormal: "BelowNormal",
    normal: "Normal",
    abovenormal: "AboveNormal",
    high: "High",
    realtime: "RealTime",
  };
  return map[normalized] || "High";
}
