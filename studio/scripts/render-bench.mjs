#!/usr/bin/env node
// Studio rendering budget gate (orm-program R01, VERIFICATION V16).
//
//   node scripts/render-bench.mjs --cli <neutron binary> [--runs N] [--out file.json] [--gate]
//
// Drives the REAL served Studio — the CLI binary with its embedded build —
// in headless Chrome against a disposable PostgreSQL database and measures
// what a user feels:
//   - table browser: deep link to a 10,000-row table, time to the first
//     painted rows; switch to 1000-row pages, time until they render;
//   - SQL editor: a 10,000-row result, time from Run to painted rows;
//   - for both grids: rows in the DOM (virtualization: visible + overscan,
//     never the result size), reachability of the last row, and frame times
//     while scrolling the whole result top to bottom;
//   - JS heap after the 10,000-row result.
//
// Needs NEUTRON_TEST_DATABASE_URL (a disposable PostgreSQL server; the gate
// creates and drops its own database), psql, and Chrome: the installed
// Google Chrome by default, or CHROME_PATH. The Studio server runs with a
// temporary HOME (its saved-connection store) and no PATH, so it cannot open
// the user's browser or touch their saved connections.
//
// Always enforced: the DOM row bound and last-row reachability. --gate adds
// the time/frame/heap ceilings in scripts/render-budgets.json. Numbers from a
// loaded machine are regression signals, not publishable figures.

import { spawn, execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { createServer } from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const CLI = opt("--cli", null);
const RUNS = Number(opt("--runs", "3"));
const OUT = opt("--out", null);
const GATE = argv.includes("--gate");
const TEST_URL = process.env.NEUTRON_TEST_DATABASE_URL || "";
function fail(msg) {
  console.error(`render-bench: ${msg}`);
  process.exit(2);
}
if (!CLI) fail("--cli <path to a built neutron binary> is required (build Studio, embed it, go build)");
if (!TEST_URL) fail("NEUTRON_TEST_DATABASE_URL is not set");
const budgets = JSON.parse(readFileSync(path.join(HERE, "render-budgets.json"), "utf8"));

const ROWS = 10000;
const DB_NAME = `studio_bench_${process.pid}_${Math.floor(Date.now() / 1000)}`;
const dbUrl = (db) => {
  const u = new URL(TEST_URL);
  u.pathname = `/${db}`;
  return u.toString();
};
// The fixture talks to PostgreSQL through psql: it must not depend on the
// system under test, and no PostgreSQL driver is a Studio dependency.
const psql = (db, sqlText) => execFileSync("psql", [dbUrl(db), "-v", "ON_ERROR_STOP=1", "-qAtc", sqlText], { encoding: "utf8" });

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function stats(samples) {
  const s = [...samples].sort((a, b) => a - b);
  const q = (p) => s[Math.min(s.length - 1, Math.max(0, Math.ceil(p * s.length) - 1))];
  const r = (x) => Math.round(x * 10) / 10;
  return { n: s.length, min: r(s[0]), p50: r(q(0.5)), p95: r(q(0.95)), max: r(s[s.length - 1]) };
}

// Virtualization bound for a grid viewport: the rows that fit plus the
// overscan on both sides (DataGrid's DEFAULT_ROW_HEIGHT and OVERSCAN_ROWS,
// mirrored in render-budgets.json) plus one partially visible row.
function domBound(viewportHeight) {
  return Math.ceil(viewportHeight / budgets.rowHeightPx) + 2 * budgets.overscanRows + 1;
}

// Runs in the page: scroll the grid from top to bottom in viewport-sized
// steps, one step per animation frame, recording frame deltas and the
// largest number of rendered rows.
async function scrollGrid(page) {
  return page.evaluate(async () => {
    const grid = document.querySelector('table[role="grid"]');
    const scroller = grid.parentElement;
    const frames = [];
    let maxRows = 0;
    let last = await new Promise((r) => requestAnimationFrame(r));
    const step = Math.max(1, Math.floor(scroller.clientHeight * 0.9));
    let guard = 0;
    while (scroller.scrollTop + scroller.clientHeight < scroller.scrollHeight - 1 && guard++ < 5000) {
      scroller.scrollTop += step;
      scroller.dispatchEvent(new Event("scroll"));
      const now = await new Promise((r) => requestAnimationFrame(r));
      frames.push(now - last);
      last = now;
      maxRows = Math.max(maxRows, grid.querySelectorAll("tbody tr[data-row-index]").length);
    }
    const lastIndex = Math.max(...[...grid.querySelectorAll("tbody tr[data-row-index]")].map((tr) => Number(tr.dataset.rowIndex)));
    return { frames, maxRows, lastIndex, viewportHeight: scroller.clientHeight };
  });
}

async function waitRows(page, rowCount, timeoutMs = 60000) {
  await page.waitForFunction(
    (n) => {
      const g = document.querySelector('table[role="grid"]');
      return g !== null && g.getAttribute("aria-rowcount") === String(n + 1) && g.querySelector("tbody tr[data-row-index]") !== null;
    },
    rowCount,
    { timeout: timeoutMs, polling: "raf" },
  );
}

const result = { tool: "studio render-bench", startedAt: new Date().toISOString(), runs: RUNS, environment: {}, tableBrowser: {}, sqlEditor: {}, failures: [] };
const cleanups = [];
try {
  psql("postgres", `create database "${DB_NAME}"`);
  cleanups.push(() => psql("postgres", `drop database if exists "${DB_NAME}" with (force)`));
  psql(
    DB_NAME,
    `create table bench_rows (id integer primary key, name text not null, amount numeric(12,2) not null, created_at timestamptz not null, active boolean not null, payload jsonb);
     insert into bench_rows select g, 'row ' || g, (g * 1.25)::numeric(12,2), timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute', g % 2 = 0, jsonb_build_object('g', g)
     from generate_series(1, ${ROWS}) g;
     analyze bench_rows;`,
  );
  result.environment.server = psql(DB_NAME, "select version()").trim();

  const home = mkdtempSync(path.join(os.tmpdir(), "studio-bench-home-"));
  cleanups.push(() => rmSync(home, { recursive: true, force: true }));
  mkdirSync(path.join(home, ".neutron"), { mode: 0o700 });
  writeFileSync(path.join(home, ".neutron", "studio.json"), JSON.stringify([{ id: "r01bench", name: "render bench", url: dbUrl(DB_NAME), isNucleus: false }]), {
    mode: 0o600,
  });

  const port = await freePort();
  const studio = spawn(path.resolve(CLI), ["studio", "--port", String(port)], { env: { HOME: home, PATH: "" }, stdio: ["ignore", "pipe", "pipe"] });
  let studioLog = "";
  studio.stdout.on("data", (d) => (studioLog += d));
  studio.stderr.on("data", (d) => (studioLog += d));
  cleanups.push(() => studio.kill("SIGTERM"));
  const base = `http://127.0.0.1:${port}`;
  for (let i = 0; ; i++) {
    try {
      if ((await fetch(`${base}/`)).ok) break;
    } catch {}
    if (i > 100 || studio.exitCode !== null) throw new Error(`studio did not start:\n${studioLog}`);
    await sleep(100);
  }

  const browser = await chromium.launch(process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : { channel: "chrome" });
  cleanups.push(() => browser.close());
  Object.assign(result.environment, {
    browser: `${browser.browserType().name()} ${browser.version()}`,
    node: process.version,
    platform: `${process.platform}-${process.arch}`,
    cpu: os.cpus()[0]?.model ?? "unknown",
    loadavg: os.loadavg().map((x) => Math.round(x * 100) / 100),
    viewport: "1280x800",
  });

  const tb = { firstRowsMs: [], page1000Ms: [], domRows: [], frames: [], lastIndex: [], bound: 0 };
  const se = { runToRowsMs: [], domRows: [], frames: [], lastIndex: [], heapMiB: [], bound: 0 };
  for (let run = 0; run < RUNS; run++) {
    // A fresh context per run: cold SPA load, no cached state.
    const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
    const page = await context.newPage();
    try {
      const t0 = Date.now();
      await page.goto(`${base}/#/c/r01bench/t/public/bench_rows`);
      await waitRows(page, 200);
      tb.firstRowsMs.push(Date.now() - t0);
      const pageSize = page.locator("select").filter({ has: page.locator('option[value="1000"]') }).first();
      const t1 = Date.now();
      await pageSize.selectOption("1000");
      await waitRows(page, 1000);
      tb.page1000Ms.push(Date.now() - t1);
      const s1 = await scrollGrid(page);
      tb.domRows.push(s1.maxRows);
      tb.frames.push(...s1.frames);
      tb.lastIndex.push(s1.lastIndex);
      tb.bound = domBound(s1.viewportHeight);

      await page.goto(`${base}/#/c/r01bench/sql`);
      const editor = page.locator('.cm-content[aria-label="SQL editor"]');
      await editor.waitFor();
      await editor.click();
      await page.keyboard.press(process.platform === "darwin" ? "Meta+A" : "Control+A");
      await page.keyboard.type(`select * from bench_rows order by id`);
      const t2 = Date.now();
      await page.getByRole("button", { name: /Run$/ }).click();
      await waitRows(page, ROWS);
      se.runToRowsMs.push(Date.now() - t2);
      const cdp = await context.newCDPSession(page);
      await cdp.send("HeapProfiler.collectGarbage");
      await cdp.send("Performance.enable");
      const heap = (await cdp.send("Performance.getMetrics")).metrics.find((m) => m.name === "JSHeapUsedSize");
      if (heap) se.heapMiB.push(heap.value / 2 ** 20);
      const s2 = await scrollGrid(page);
      se.domRows.push(s2.maxRows);
      se.frames.push(...s2.frames);
      se.lastIndex.push(s2.lastIndex);
      se.bound = domBound(s2.viewportHeight);
    } finally {
      await context.close();
    }
  }

  result.tableBrowser = {
    firstRowsMs: stats(tb.firstRowsMs),
    page1000Ms: stats(tb.page1000Ms),
    maxDomRows: Math.max(...tb.domRows),
    domRowBound: tb.bound,
    scrollFrameMs: stats(tb.frames),
    longFrames: tb.frames.filter((f) => f > 50).length,
    reachedLastRow: tb.lastIndex.every((i) => i === 999),
  };
  result.sqlEditor = {
    rows: ROWS,
    runToRowsMs: stats(se.runToRowsMs),
    maxDomRows: Math.max(...se.domRows),
    domRowBound: se.bound,
    scrollFrameMs: stats(se.frames),
    longFrames: se.frames.filter((f) => f > 50).length,
    heapMiB: se.heapMiB.length ? stats(se.heapMiB) : null,
    reachedLastRow: se.lastIndex.every((i) => i === ROWS - 1),
  };
  const T = result.tableBrowser;
  const S = result.sqlEditor;
  const b = budgets;
  const integrity = [
    ["table browser: scrolling reaches row 1000 of the page", T.reachedLastRow],
    [`SQL editor: scrolling reaches row ${ROWS}`, S.reachedLastRow],
    [`table browser: ${T.maxDomRows} DOM rows within the visible+overscan bound ${T.domRowBound}`, T.maxDomRows <= T.domRowBound],
    [`SQL editor: ${S.maxDomRows} DOM rows within the visible+overscan bound ${S.domRowBound}`, S.maxDomRows <= S.domRowBound],
  ];
  const budgetChecks = [
    [`table browser: first rows p95 ${T.firstRowsMs.p95} ms <= ${b.firstRowsP95Ms} ms`, T.firstRowsMs.p95 <= b.firstRowsP95Ms],
    [`table browser: 1000-row page p95 ${T.page1000Ms.p95} ms <= ${b.page1000P95Ms} ms`, T.page1000Ms.p95 <= b.page1000P95Ms],
    [`SQL editor: ${ROWS} rows painted p95 ${S.runToRowsMs.p95} ms <= ${b.sqlRunToRowsP95Ms} ms`, S.runToRowsMs.p95 <= b.sqlRunToRowsP95Ms],
    [`table browser: scroll frame p95 ${T.scrollFrameMs.p95} ms <= ${b.scrollFrameP95Ms} ms`, T.scrollFrameMs.p95 <= b.scrollFrameP95Ms],
    [`SQL editor: scroll frame p95 ${S.scrollFrameMs.p95} ms <= ${b.scrollFrameP95Ms} ms`, S.scrollFrameMs.p95 <= b.scrollFrameP95Ms],
    [`SQL editor: JS heap after ${ROWS} rows ${S.heapMiB?.max} MiB <= ${b.sqlHeapMiB} MiB`, S.heapMiB === null || S.heapMiB.max <= b.sqlHeapMiB],
  ];
  result.checks = [...integrity, ...budgetChecks].map(([label, ok]) => ({ label, ok }));
  for (const [label, ok] of integrity) if (!ok) result.failures.push(label);
  if (GATE) for (const [label, ok] of budgetChecks) if (!ok) result.failures.push(label);
} catch (err) {
  result.failures.push(`harness: ${err instanceof Error ? err.stack : String(err)}`);
} finally {
  for (const c of cleanups.reverse()) {
    try {
      await c();
    } catch (err) {
      result.failures.push(`cleanup: ${String(err)}`);
    }
  }
}

const json = JSON.stringify(result, null, 2);
if (OUT) writeFileSync(OUT, json + "\n");
else process.stdout.write(json + "\n");
for (const f of result.failures) console.error(`render-bench FAIL ${f}`);
console.error(`render-bench: ${result.failures.length === 0 ? "PASS" : `${result.failures.length} failure(s)`}`);
process.exit(result.failures.length === 0 ? 0 : 1);
