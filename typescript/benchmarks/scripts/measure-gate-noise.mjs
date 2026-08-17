// Re-derive the CI smoke gate's baseline and thresholds from real CI runs.
//
// The thresholds in `bench-gate-thresholds.json` are not judgement calls, and
// they must not become them. They come from measuring what the runner does to
// an unchanged request path. Re-run this whenever the profile, the runner image
// or the scenario set changes — a threshold inherited from a profile that no
// longer exists is the same defect the gate's own shape check exists to catch.
//
// Usage, from typescript/benchmarks:
//   node scripts/measure-gate-noise.mjs                 # report only
//   node scripts/measure-gate-noise.mjs --write         # also rewrite the baseline
//   node scripts/measure-gate-noise.mjs --runs 12
//
// Needs `gh` authenticated against the repo. Artifacts expire (14 days), so this
// can only see the recent past; if it finds fewer than 5 runs it says so rather
// than deriving a threshold from too little.

import { execFile } from "node:child_process";
import { mkdtemp, readFile, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";
import { promisify } from "node:util";

const run = promisify(execFile);
const REPO = process.env.BENCH_GATE_REPO || "neutron-build/neutron";
const WORKFLOW = "typescript-benchmarks.yml";
const MIN_RUNS = 5;

const args = process.argv.slice(2);
const write = args.includes("--write");
const wanted = Number(args[args.indexOf("--runs") + 1]) || 8;

const list = JSON.parse(
  (
    await run("gh", [
      "run", "list", "--repo", REPO, "--workflow", WORKFLOW,
      "--limit", String(wanted), "--json", "databaseId,conclusion,headSha",
    ])
  ).stdout,
);
const ids = list.filter((r) => r.conclusion === "success").map((r) => r.databaseId);
if (ids.length < MIN_RUNS) {
  console.error(
    `Only ${ids.length} successful runs with artifacts are reachable (need ${MIN_RUNS}).\n` +
      `Artifacts expire after 14 days. Thresholds derived from fewer runs would be a\n` +
      `guess wearing a measurement's clothes — not writing any.`,
  );
  process.exit(1);
}

const dir = await mkdtemp(path.join(tmpdir(), "bench-noise-"));
const runs = [];
for (const id of ids) {
  const dest = path.join(dir, String(id));
  try {
    await run("gh", ["run", "download", String(id), "--repo", REPO, "--dir", dest]);
  } catch {
    console.warn(`  run ${id}: artifacts gone, skipping`);
    continue;
  }
  const found = await find(dest, "latest.json");
  if (found) runs.push(JSON.parse(await readFile(found, "utf8")));
}
if (runs.length < MIN_RUNS) {
  console.error(`Only ${runs.length} runs had downloadable artifacts (need ${MIN_RUNS}).`);
  process.exit(1);
}

const shapes = new Set(runs.map((r) => shape(r)));
if (shapes.size !== 1) {
  console.error(
    `Runs span more than one profile (${[...shapes].join(" and ")}). Mixing them\n` +
      `produces a baseline that is neither. Narrow --runs to a single profile.`,
  );
  process.exit(1);
}

// Baseline = per-row median. A single run is one sample of a shared machine.
const key = (r) => `${r.track || "node"}|${r.framework}|${r.scenario}`;
const buckets = new Map();
for (const r of runs) for (const row of r.summary || []) push(buckets, key(row), row);

const baseline = JSON.parse(JSON.stringify(runs[0]));
delete baseline.results;
delete baseline.conformance;
baseline.summary = (runs[0].summary || []).map((row) => {
  const rows = buckets.get(key(row));
  const out = { ...row, _samples: rows.length };
  for (const f of ["requestsPerSec", "p50Ms", "p95Ms", "p99Ms", "throughputMBps"]) {
    const vals = rows.map((x) => x[f]).filter((v) => Number.isFinite(v));
    if (vals.length) out[f] = round(median(vals));
  }
  return out;
});

// Worst deviation of any single run from that median IS the noise floor.
const bmap = new Map(baseline.summary.map((r) => [key(r), r]));
let worstDrop = 0;
let worstP95 = 0;
const perScenario = new Map();
for (const r of runs) {
  for (const row of r.summary || []) {
    if (String(row.framework).toLowerCase() !== "neutron") continue;
    const b = bmap.get(key(row));
    if (!b) continue;
    const drop = ((b.requestsPerSec - row.requestsPerSec) / b.requestsPerSec) * 100;
    const rise = b.p95Ms ? ((row.p95Ms - b.p95Ms) / b.p95Ms) * 100 : 0;
    worstDrop = Math.max(worstDrop, drop);
    worstP95 = Math.max(worstP95, rise);
    const cur = perScenario.get(row.scenario) || { drop: 0, rise: 0 };
    perScenario.set(row.scenario, {
      drop: Math.max(cur.drop, drop),
      rise: Math.max(cur.rise, rise),
    });
  }
}

console.log(`${runs.length} runs, profile ${shape(runs[0])}\n`);
console.log("Worst single-run deviation from the median, per scenario (neutron):");
console.log(`${"scenario".padEnd(18)}${"rps drop".padStart(10)}${"p95 rise".padStart(10)}   suggested limit`);
for (const [s, v] of [...perScenario].sort((a, b) => a[1].drop - b[1].drop)) {
  const suggested = v.drop < 5 ? 25 : Math.ceil(((v.drop * 1.4) / 5)) * 5;
  console.log(
    `${s.padEnd(18)}${v.drop.toFixed(1).padStart(9)}%${v.rise.toFixed(1).padStart(9)}%   ${String(suggested).padStart(3)}%` +
      (v.drop < 5 ? "   <- stable enough to gate tightly" : ""),
  );
}
console.log(`\nworst overall: ${worstDrop.toFixed(1)}% rps drop, ${worstP95.toFixed(1)}% p95 rise`);
console.log(
  `A 10x regression is a 90% drop. Any global limit between ${Math.ceil(worstDrop * 1.3)}% and 85% ` +
    `catches it without firing on the runner.`,
);

if (write) {
  baseline._provenance = {
    what: `Per-row MEDIAN of ${runs.length} CI runs.`,
    profile: shape(runs[0]),
    runs: ids.map(String),
    recorded: new Date().toISOString().slice(0, 10),
  };
  await writeFile(
    path.resolve(process.cwd(), "results/ci-smoke-baseline.json"),
    `${JSON.stringify(baseline, null, 2)}\n`,
  );
  console.log("\nwrote results/ci-smoke-baseline.json");
  console.log("Update bench-gate-thresholds.json by hand from the table above, and say why.");
}

function shape(r) {
  return [r?.config?.connections, r?.config?.durationSec, r?.config?.runs].join("/");
}
function push(map, k, v) {
  const cur = map.get(k);
  if (cur) cur.push(v);
  else map.set(k, [v]);
}
function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function round(v) {
  return Number(Number(v).toFixed(2));
}
async function find(root, name) {
  for (const e of await readdir(root, { withFileTypes: true })) {
    const p = path.join(root, e.name);
    if (e.isDirectory()) {
      const hit = await find(p, name);
      if (hit) return hit;
    } else if (e.name === name) return p;
  }
  return null;
}
