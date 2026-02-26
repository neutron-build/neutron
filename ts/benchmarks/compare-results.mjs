import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const args = parseArgs(process.argv.slice(2));
const cwd = process.cwd();
const baselinePath = path.resolve(cwd, args.baseline ?? "");
const candidatePath = path.resolve(cwd, args.candidate ?? "results/latest.json");

if (!args.baseline) {
  console.error("Missing required --baseline <path> argument.");
  process.exit(1);
}

const baseline = await readJson(baselinePath);
const candidate = await readJson(candidatePath);

const rows = buildDiffRows(baseline, candidate, args.framework);
if (rows.length === 0) {
  console.error("No comparable summary rows were found.");
  process.exit(1);
}

console.log(`Baseline: ${baselinePath}`);
console.log(`Candidate: ${candidatePath}`);
console.log(`Baseline timestamp: ${baseline.timestamp}`);
console.log(`Candidate timestamp: ${candidate.timestamp}`);
console.table(rows);

if (Number.isFinite(args.failRpsDropPct)) {
  const failures = findRpsFailures(rows, args.failRpsDropPct, args.framework);
  if (failures.length > 0) {
    console.error(
      `Regression gate failed: ${failures.length} row(s) dropped more than ${args.failRpsDropPct}% RPS.`,
    );
    console.table(failures);
    process.exit(2);
  }
}

if (Number.isFinite(args.failP95IncreasePct)) {
  const failures = findP95Failures(rows, args.failP95IncreasePct, args.framework);
  if (failures.length > 0) {
    console.error(
      `Regression gate failed: ${failures.length} row(s) exceeded ${args.failP95IncreasePct}% p95 latency increase.`,
    );
    console.table(failures);
    process.exit(3);
  }
}

function parseArgs(argv) {
  const parsed = {
    baseline: "",
    candidate: "results/latest.json",
    framework: "",
    failRpsDropPct: NaN,
    failP95IncreasePct: NaN,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    const next = argv[i + 1];
    if (token === "--baseline" && next) {
      parsed.baseline = next;
      i += 1;
      continue;
    }
    if (token === "--candidate" && next) {
      parsed.candidate = next;
      i += 1;
      continue;
    }
    if (token === "--framework" && next) {
      parsed.framework = next.toLowerCase();
      i += 1;
      continue;
    }
    if (token === "--fail-rps-drop-pct" && next) {
      parsed.failRpsDropPct = Number.parseFloat(next);
      i += 1;
      continue;
    }
    if (token === "--fail-p95-increase-pct" && next) {
      parsed.failP95IncreasePct = Number.parseFloat(next);
      i += 1;
    }
  }

  return parsed;
}

async function readJson(filePath) {
  const raw = await readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function rowKey(row) {
  return `${String(row.track || "node").toLowerCase()}|${String(row.framework || "").toLowerCase()}|${String(row.scenario || "").toLowerCase()}`;
}

function buildDiffRows(baseline, candidate, frameworkFilter) {
  const baselineMap = new Map();
  for (const row of baseline.summary || []) {
    baselineMap.set(rowKey(row), row);
  }

  const rows = [];
  for (const row of candidate.summary || []) {
    const key = rowKey(row);
    const prior = baselineMap.get(key);
    if (!prior) {
      continue;
    }
    const framework = String(row.framework || "");
    if (frameworkFilter && framework.toLowerCase() !== frameworkFilter) {
      continue;
    }

    rows.push({
      track: row.track || "node",
      framework,
      scenario: row.scenario,
      baseRps: round(prior.requestsPerSec),
      newRps: round(row.requestsPerSec),
      rpsDeltaPct: pctDelta(row.requestsPerSec, prior.requestsPerSec),
      baseP95: round(prior.p95Ms),
      newP95: round(row.p95Ms),
      p95DeltaPct: pctDelta(row.p95Ms, prior.p95Ms),
      baseP99: round(prior.p99Ms),
      newP99: round(row.p99Ms),
      p99DeltaPct: pctDelta(row.p99Ms, prior.p99Ms),
    });
  }

  return rows.sort((a, b) => {
    if (a.track !== b.track) {
      return String(a.track).localeCompare(String(b.track));
    }
    if (a.scenario === b.scenario) {
      return a.framework.localeCompare(b.framework);
    }
    return String(a.scenario).localeCompare(String(b.scenario));
  });
}

function round(value) {
  return Number(Number(value).toFixed(2));
}

function pctDelta(next, prev) {
  if (!Number.isFinite(prev) || prev === 0) {
    return NaN;
  }
  return round(((next - prev) / prev) * 100);
}

function findRpsFailures(rows, thresholdPct, frameworkFilter) {
  const threshold = Math.abs(thresholdPct);
  return rows.filter((row) => {
    if (frameworkFilter && String(row.framework).toLowerCase() !== frameworkFilter) {
      return false;
    }
    return Number.isFinite(row.rpsDeltaPct) && row.rpsDeltaPct < -threshold;
  });
}

function findP95Failures(rows, thresholdPct, frameworkFilter) {
  const threshold = Math.abs(thresholdPct);
  return rows.filter((row) => {
    if (frameworkFilter && String(row.framework).toLowerCase() !== frameworkFilter) {
      return false;
    }
    return Number.isFinite(row.p95DeltaPct) && row.p95DeltaPct > threshold;
  });
}
