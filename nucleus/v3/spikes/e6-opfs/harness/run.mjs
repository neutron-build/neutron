#!/usr/bin/env node
// E6 OPFS spike driver (card X-6; PLAN-v3 §4 E6, §3.2 OpfsEnv).
//
// Serves harness/ on http://127.0.0.1, launches Chromium, Firefox and
// WebKit through Playwright (by absolute path, no npm install), runs
// tests T1–T5 in each engine, and writes results.json next to this file.
//
//   T1  write N pages + WAL + flush, reload, read back — persistence
//   T2  write + flush, then tab kill and full browser kill — durability
//   T3  write WITHOUT flush, then kill — what survives (informational)
//   T4  two tabs coordinate through a Web Lock; second open waits;
//       exclusive-handle probe; final integrity check
//   T5  latency of flush() and of 8 KiB page writes (p50/p99)
//
// Usage:
//   node run.mjs [--quick] [--engines=chromium,firefox,webkit]
//                [--out=results.json] [--step-timeout-ms=N]
//
// --quick shrinks the datasets (used by the card's acceptance line); the
// full run produces the card's reported numbers. Every scenario runs
// under a hard timeout; on timeout the browser process is SIGKILLed and
// the scenario is recorded as 'timeout'. The HTTP server is always
// stopped, including on failure or Ctrl-C.
//
// Exit code: 0 when every requested engine produced a record for every
// test (regardless of pass/fail — a browser failing durability is an
// experiment result, reported under the decision rule, not a harness
// error); 1 when the harness itself failed to complete.

process.env.PLAYWRIGHT_BROWSERS_PATH ||= `${process.env.HOME}/.cache/nv3-target/playwright/browsers`;

import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, statSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const V3_ROOT = path.resolve(HERE, '../../..'); // nucleus/v3 (run cargo from there)
const PLAYWRIGHT_ENTRY = path.join(
  process.env.HOME,
  '.cache/nv3-target/playwright/node_modules/playwright/index.mjs',
);

const argv = process.argv.slice(2);
const QUICK = argv.includes('--quick');
const arg = (name, fallback) => {
  const hit = argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.split('=').slice(1).join('=') : fallback;
};
const ENGINES = arg('engines', 'chromium,firefox,webkit')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const SCENARIO_MS = Number(arg('step-timeout-ms', QUICK ? 120_000 : 300_000));
const OUT_PATH = path.resolve(HERE, arg('out', 'results.json'));

// Dataset sizes: --quick bounds the acceptance run; the defaults are the
// card's numbers (T5: "p50/p99 over 200"; T1: 1,000 pages).
const T1_PAGES = QUICK ? 100 : 1000;
const T2_PAGES = QUICK ? 60 : 300;
const T3_PAGES = QUICK ? 60 : 300;
const T4_PAGES = QUICK ? 60 : 300; // half written by each tab
const SAMPLES = QUICK ? 50 : 200;

// Fixed ASCII records per WAL append, so expected WAL lengths are exact.
const WAL_TEXT = 'wal-record-0123456789';
const T1_WAL_RECORDS = 25;
const WAL_TEXT_A = 't4-wal-from-tab-a';
const WAL_TEXT_B = 't4-wal-from-tab-b';

const STAMP = new Date().toISOString().replace(/[:.]/g, '-');
const PROFILE_BASE = path.join(
  process.env.HOME,
  '.cache/nv3-target/e6-opfs/profiles',
  `run-${STAMP}`,
);

const { chromium, firefox, webkit } = await import(PLAYWRIGHT_ENTRY);
const ENGINE_APIS = { chromium, firefox, webkit };

// ---------------------------------------------------------------- utilities

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const err = new Error(`timeout after ${ms}ms: ${label}`);
      err.isTimeout = true;
      reject(err);
    }, ms);
  });
  // Swallow the loser's eventual rejection so nothing goes unhandled.
  promise.then(
    () => {},
    () => {},
  );
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function pct(samples, p) {
  if (samples.length === 0) return null;
  const sorted = [...samples].sort((a, b) => a - b);
  const rank = Math.max(1, Math.ceil((p / 100) * sorted.length));
  return sorted[rank - 1];
}

function stats(samples) {
  if (!samples || samples.length === 0) return { n: 0 };
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    n: sorted.length,
    min_ms: Number(sorted[0].toFixed(3)),
    p50_ms: Number(pct(sorted, 50).toFixed(3)),
    p99_ms: Number(pct(sorted, 99).toFixed(3)),
    max_ms: Number(sorted[sorted.length - 1].toFixed(3)),
  };
}

function runCmd(cmd, args, opts) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'inherit', 'inherit'], ...opts });
    child.on('error', reject);
    child.on('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} ${args.join(' ')} exited ${code}`)),
    );
  });
}

// Builds the wasm (if the acceptance build step has not run yet) and
// (re)generates the wasm-bindgen web glue under harness/pkg.
async function ensureWasm() {
  const targetDir =
    process.env.CARGO_TARGET_DIR || path.join(process.env.HOME, '.cache/nv3-target/X-6');
  const wasm = path.join(targetDir, 'wasm32-unknown-unknown/release/e6_opfs.wasm');
  if (!existsSync(wasm)) {
    process.stderr.write(`run.mjs: no wasm at ${wasm}; building\n`);
    await runCmd(
      'cargo',
      ['build', '-q', '--release', '--target', 'wasm32-unknown-unknown', '-p', 'e6-opfs'],
      { cwd: V3_ROOT },
    );
  }
  const glue = path.join(HERE, 'pkg/e6_opfs.js');
  if (!existsSync(glue) || statSync(glue).mtimeMs < statSync(wasm).mtimeMs) {
    await runCmd(path.join(process.env.HOME, '.cargo/bin/wasm-bindgen'), [
      '--target',
      'web',
      '--out-dir',
      path.join(HERE, 'pkg'),
      wasm,
    ]);
  }
}

// ------------------------------------------------------------ http server

let server = null; // { proc, port }

async function startServer() {
  return new Promise((resolve, reject) => {
    // -u: unbuffered, or the "Serving HTTP on 127.0.0.1 port N" banner
    // stays in stdout's block buffer and we never learn the port.
    const proc = spawn('python3', ['-u', '-m', 'http.server', '0', '--bind', '127.0.0.1'], {
      cwd: HERE,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const failTimer = setTimeout(() => reject(new Error('http server did not start in 15s')), 15_000);
    proc.stdout.on('data', (chunk) => {
      const match = /port (\d+)/.exec(String(chunk));
      if (match) {
        clearTimeout(failTimer);
        resolve({ proc, port: Number(match[1]) });
      }
    });
    proc.stderr.on('data', () => {}); // drain request-log noise
    proc.on('exit', (code) => {
      clearTimeout(failTimer);
      reject(new Error(`http server exited early with code ${code}`));
    });
  });
}

function stopServer() {
  if (!server) return;
  const proc = server.proc;
  server = null;
  try {
    proc.kill('SIGTERM');
  } catch {
    /* already gone */
  }
  setTimeout(() => {
    try {
      proc.kill('SIGKILL');
    } catch {
      /* already gone */
    }
  }, 3000).unref();
}

// --------------------------------------------------------- browser control

let live = null; // { context, engine }

function browserProcessOf(context) {
  try {
    return context.browser()?.process?.() ?? null;
  } catch {
    return null;
  }
}

async function launch(engineName, scenario) {
  const profile = path.join(PROFILE_BASE, `${engineName}-${scenario}`);
  mkdirSync(profile, { recursive: true });
  const context = await withTimeout(
    ENGINE_APIS[engineName].launchPersistentContext(profile, { headless: true }),
    60_000,
    `${engineName} launch (${scenario})`,
  );
  live = { context, engine: engineName };
  return context;
}

async function closeBrowser() {
  if (!live) return;
  const { context } = live;
  live = null;
  try {
    await Promise.race([context.close(), sleep(10_000)]);
  } catch {
    /* best effort; SIGKILL below is the backstop */
  }
  const proc = browserProcessOf(context);
  if (proc && proc.exitCode === null) {
    try {
      proc.kill('SIGKILL');
    } catch {
      /* already gone */
    }
  }
}

// Hard kill: SIGKILL the browser process (as close to a crash as a
// harness gets), then let the context tear down.
async function sigkillBrowser() {
  if (!live) return false;
  const { context } = live;
  live = null;
  const proc = browserProcessOf(context);
  let killed = false;
  if (proc && proc.exitCode === null) {
    try {
      killed = proc.kill('SIGKILL');
    } catch {
      killed = false;
    }
  }
  try {
    await Promise.race([context.close(), sleep(5_000)]);
  } catch {
    /* expected after SIGKILL */
  }
  return killed;
}

// ------------------------------------------------------------------ pages

async function newPage(context, url) {
  const page = await context.newPage();
  await page.goto(url, { waitUntil: 'load', timeout: 30_000 });
  return page;
}

// Open (with the in-page retry that covers asynchronous OPFS handle
// release after a kill), write pages, append WAL records, flush. Leaves
// the database OPEN unless close=true (T2/T3 kill with handles open).
function writeAndFlush(page, db, pages, walTexts, { close = false } = {}) {
  return page.evaluate(
    async ({ db, pages, walTexts, close }) => {
      await window.e6.open_retry(db);
      const write = await window.e6.write_pages(db, 0, pages);
      for (const text of walTexts) await window.e6.append_wal(db, text);
      await window.e6.flush(db);
      if (close) await window.e6.close(db);
      return { write };
    },
    { db, pages, walTexts, close },
  );
}

function verifyPage(page, db, pages) {
  return page.evaluate(
    async ({ db, pages }) => {
      await window.e6.open_retry(db);
      const check = await window.e6.read_check(db, 0, pages);
      const wal_len = await window.e6.wal_len(db);
      await window.e6.close(db);
      return { check, wal_len };
    },
    { db, pages },
  );
}

const intact = (check) =>
  !!check && check.missing.length === 0 && check.corrupt.length === 0 && check.checked > 0;

// -------------------------------------------------------------- scenarios

async function t1_persistence(engineName, url) {
  const db = `t1-${STAMP}`;
  const context = await launch(engineName, 't1');
  const page = await newPage(context, url);

  await writeAndFlush(page, db, T1_PAGES, Array(T1_WAL_RECORDS).fill(WAL_TEXT));
  const walBeforeReload = await page.evaluate((db) => window.e6.wal_len(db), db);

  // Reload the page (its workers die with it), reopen the database in a
  // fresh worker, read every page and the WAL length back.
  await page.reload({ waitUntil: 'load', timeout: 30_000 });
  const verify = await verifyPage(page, db, T1_PAGES);

  const expectedWal = T1_WAL_RECORDS * WAL_TEXT.length;
  return {
    ok: intact(verify.check) && verify.wal_len === expectedWal,
    detail: {
      db,
      pages: T1_PAGES,
      missing: verify.check.missing,
      corrupt: verify.check.corrupt.slice(0, 20),
      wal_len: verify.wal_len,
      expected_wal_len: expectedWal,
      wal_len_before_reload: walBeforeReload,
    },
  };
}

async function t2_durability(engineName, url) {
  const db = `t2-${STAMP}`;
  const profile = `${engineName}-t2`; // reused across the browser kill
  const expectedWal = WAL_TEXT.length;

  // Phase 1: write + flush, then kill the TAB with handles still open.
  let context = await launch(engineName, profile);
  let page = await newPage(context, url);
  await writeAndFlush(page, db, T2_PAGES, [WAL_TEXT]);
  await page.close({ runBeforeUnload: false }); // tab kill, no unload

  // Phase 2: a new tab in the same context reads it back.
  page = await newPage(context, url);
  const afterTabKill = await verifyPage(page, db, T2_PAGES);

  // Phase 3: reopen mid-session (handles open, nothing new written),
  // SIGKILL the whole browser, relaunch on the same profile, read back.
  await page.evaluate((db) => window.e6.open_retry(db), db);
  await sigkillBrowser();
  context = await launch(engineName, profile);
  page = await newPage(context, url);
  const afterBrowserKill = await verifyPage(page, db, T2_PAGES);

  return {
    ok: intact(afterTabKill.check) && intact(afterBrowserKill.check),
    detail: {
      db,
      pages: T2_PAGES,
      expected_wal_len: expectedWal,
      tab_kill: {
        ok: intact(afterTabKill.check) && afterTabKill.wal_len === expectedWal,
        missing: afterTabKill.check.missing.length,
        corrupt: afterTabKill.check.corrupt.length,
        wal_len: afterTabKill.wal_len,
      },
      browser_kill: {
        ok: intact(afterBrowserKill.check) && afterBrowserKill.wal_len === expectedWal,
        missing: afterBrowserKill.check.missing.length,
        corrupt: afterBrowserKill.check.corrupt.length,
        wal_len: afterBrowserKill.wal_len,
      },
    },
  };
}

async function t3_unflushed_kill(engineName, url) {
  const db = `t3-${STAMP}`;
  const profile = `${engineName}-t3`; // reused across the kill

  let context = await launch(engineName, profile);
  let page = await newPage(context, url);
  await page.evaluate(
    async ({ db, pages }) => {
      await window.e6.open_retry(db);
      await window.e6.write_pages(db, 0, pages);
      await window.e6.append_wal(db, 'unflushed-wal-record');
      // deliberately NO flush and NO close
    },
    { db, pages: T3_PAGES },
  );
  await sigkillBrowser();

  context = await launch(engineName, profile);
  page = await newPage(context, url);
  const verify = await verifyPage(page, db, T3_PAGES);

  const intactPages = T3_PAGES - verify.check.missing.length - verify.check.corrupt.length;
  return {
    ok: null, // informational: the decision rule only covers flushed data
    detail: {
      db,
      pages_written: T3_PAGES,
      intact_pages_after_kill: intactPages,
      missing_pages: verify.check.missing.length,
      corrupt_pages: verify.check.corrupt.length,
      wal_len_after_kill: verify.wal_len,
      wal_len_if_fully_persisted: 'unflushed-wal-record'.length,
      note: 'no flush before the kill; anything surviving is best-effort',
    },
  };
}

async function t4_web_locks(engineName, url) {
  const db = `t4-${STAMP}`;
  const probeDb = `${db}-probe`;
  const half = Math.floor(T4_PAGES / 2);
  const context = await launch(engineName, 't4');

  const pageA = await newPage(context, url);
  const pageB = await newPage(context, url);

  // (a) While tab A holds the sync handles open, tab B's open must fail:
  //     OPFS sync handles are exclusive per file. This is WHY tabs need a
  //     Web Lock to coordinate. It is measured, not assumed.
  await pageA.evaluate((db) => window.e6.open_retry(db), probeDb);
  const exclusiveProbe = await pageB
    .evaluate(async (db) => {
      try {
        await window.e6.open(db);
        await window.e6.close(db);
        return { contested: false, error: null };
      } catch (error) {
        return { contested: true, error: String((error && error.message) || error) };
      }
    }, probeDb)
    .catch((error) => ({ contested: null, error: String(error) }));
  await pageA.evaluate((db) => window.e6.close(db), probeDb);

  // (b) Lock-coordinated handoff. A acquires first and opens; B requests
  //     and must still be waiting 400ms later while A holds; B is granted
  //     only after A flushes, closes its handles and releases the lock.
  const heldByA = await pageA.evaluate((db) => window.e6.hold_lock(db), db); // { held_since }
  await pageA.evaluate((db) => window.e6.open_retry(db), db);

  const bRequestedAt = Date.now();
  const grantPromise = pageB.evaluate((db) => window.e6.hold_lock(db), db); // pending until granted
  const grantSettledWhileHeld = await Promise.race([
    grantPromise.then(() => true),
    sleep(400).then(() => false),
  ]);

  const releasedByA = await pageA.evaluate(
    async ({ db, half, walText }) => {
      await window.e6.write_pages(db, 0, half);
      await window.e6.append_wal(db, walText);
      await window.e6.flush(db);
      await window.e6.close(db); // release the sync handles first...
      return await window.e6.release_lock(db); // ...then the Web Lock
    },
    { db, half, walText: WAL_TEXT_A },
  );

  const grantedToB = await grantPromise; // { held_since } — the grant time
  const handoff = await pageB.evaluate(
    async ({ db, half, walText }) => {
      await window.e6.open_retry(db);
      const first = await window.e6.read_check(db, 0, half); // A's flushed half
      await window.e6.write_pages(db, half, half * 2);
      await window.e6.append_wal(db, walText);
      await window.e6.flush(db);
      await window.e6.close(db);
      await window.e6.release_lock(db);
      return { first };
    },
    { db, half, walText: WAL_TEXT_B },
  );

  // (c) A fresh tab verifies the whole file: no corruption from either tab.
  const pageC = await newPage(context, url);
  const final = await verifyPage(pageC, db, half * 2);

  const expectedWal = WAL_TEXT_A.length + WAL_TEXT_B.length;
  const grantAfterRelease = grantedToB.held_since >= releasedByA.released_at - 5; // 5ms clock tolerance
  return {
    ok:
      grantSettledWhileHeld === false &&
      grantAfterRelease &&
      intact(handoff.first) &&
      intact(final.check) &&
      final.wal_len === expectedWal,
    detail: {
      db,
      pages_per_tab: half,
      second_tab_waited_while_held: grantSettledWhileHeld === false,
      grant_after_release: grantAfterRelease,
      a_held_since: heldByA.held_since,
      a_released_at: releasedByA.released_at,
      b_requested_at: bRequestedAt,
      b_granted_at: grantedToB.held_since,
      b_wait_ms: grantedToB.held_since - bRequestedAt,
      release_to_grant_ms: grantedToB.held_since - releasedByA.released_at,
      first_half_intact: intact(handoff.first),
      first_half: { missing: handoff.first.missing.length, corrupt: handoff.first.corrupt.length },
      final_check: {
        missing: final.check.missing.length,
        corrupt: final.check.corrupt.length,
        wal_len: final.wal_len,
        expected_wal_len: expectedWal,
      },
      exclusive_probe: exclusiveProbe,
    },
  };
}

async function t5_latency(engineName, url) {
  const db = `t5-${STAMP}`;
  const context = await launch(engineName, 't5');
  const page = await newPage(context, url);
  await page.evaluate((db) => window.e6.open_retry(db), db);

  const writeTimes = await page.evaluate(
    ({ db, iters }) => window.e6.bench_write(db, iters),
    { db, iters: SAMPLES },
  );
  const flushTimes = await page.evaluate(
    ({ db, iters }) => window.e6.bench_flush(db, iters),
    { db, iters: SAMPLES },
  );
  await page.evaluate((db) => window.e6.close(db), db);

  return {
    ok: writeTimes.length === SAMPLES && flushTimes.length === SAMPLES,
    detail: {
      page_bytes: 8192,
      write: stats(writeTimes),
      flush: stats(flushTimes),
    },
  };
}

// --------------------------------------------------------------- execution

const SCENARIOS = [
  ['t1', t1_persistence],
  ['t2', t2_durability],
  ['t3', t3_unflushed_kill],
  ['t4', t4_web_locks],
  ['t5', t5_latency],
];

async function runScenario(engineName, [name, fn], url) {
  const started = Date.now();
  try {
    const out = await withTimeout(fn(engineName, url), SCENARIO_MS, `${engineName} ${name}`);
    const status = out.ok === null ? 'info' : out.ok ? 'pass' : 'fail';
    return { status, ms: Date.now() - started, detail: out.detail };
  } catch (error) {
    if (error && error.isTimeout) {
      await sigkillBrowser();
      return { status: 'timeout', ms: Date.now() - started, detail: { error: error.message } };
    }
    return {
      status: 'error',
      ms: Date.now() - started,
      detail: { error: String((error && error.message) || error) },
    };
  } finally {
    await closeBrowser();
  }
}

async function main() {
  await ensureWasm();
  server = await startServer();
  const url = `http://127.0.0.1:${server.port}/`;
  process.stderr.write(`run.mjs: serving ${HERE} at ${url}\n`);

  const results = {
    meta: {
      date: new Date().toISOString(),
      quick: QUICK,
      hardware: `${os.arch()} ${os.platform()}; ${os.cpus().length} cpus`,
      node: process.version,
      playwright: '1.64.1 (by absolute path)',
      scenario_timeout_ms: SCENARIO_MS,
      datasets: {
        t1_pages: T1_PAGES,
        t2_pages: T2_PAGES,
        t3_pages: T3_PAGES,
        t4_pages: T4_PAGES,
        latency_samples: SAMPLES,
        wal_record_bytes: WAL_TEXT.length,
      },
    },
    engines: {},
  };

  for (const engineName of ENGINES) {
    if (!ENGINE_APIS[engineName]) {
      throw new Error(`unknown engine ${engineName} (want chromium, firefox, webkit)`);
    }
    const engine = { engine: engineName, tests: {} };
    results.engines[engineName] = engine;
    for (const scenario of SCENARIOS) {
      const [name] = scenario;
      process.stderr.write(`run.mjs: ${engineName} ${name} ...\n`);
      const record = await runScenario(engineName, scenario, url);
      engine.tests[name] = record;
      process.stderr.write(`run.mjs: ${engineName} ${name} -> ${record.status} (${record.ms}ms)\n`);
    }
  }

  writeFileSync(OUT_PATH, JSON.stringify(results, null, 2) + '\n');

  // Summary. Decision rule (PLAN-v3 §4 E6): committed-and-flushed data
  // survives a tab kill in all engines tested; per-browser durability is
  // documented.
  console.log(`\nE6 results written to ${OUT_PATH}`);
  let complete = true;
  let decision = ENGINES.length > 0;
  for (const engineName of ENGINES) {
    const tests = results.engines[engineName].tests;
    for (const [name] of SCENARIOS) {
      if (!tests[name] || !tests[name].status) complete = false;
    }
    const t1 = tests.t1;
    const t2 = tests.t2;
    const t3 = tests.t3;
    const t4 = tests.t4;
    const t5 = tests.t5;
    const survives =
      t1?.status === 'pass' &&
      t2?.status === 'pass' &&
      t2?.detail?.tab_kill?.ok === true &&
      t2?.detail?.browser_kill?.ok === true;
    decision = decision && survives === true;
    console.log(
      `${engineName}: T1=${t1?.status} T2(tab)=${t2?.detail?.tab_kill?.ok} T2(browser)=${t2?.detail?.browser_kill?.ok}` +
        ` T3 survived=${t3?.detail?.intact_pages_after_kill}/${t3?.detail?.pages_written}` +
        ` T4=${t4?.status} T5 flush p50/p99=${t5?.detail?.flush?.p50_ms}/${t5?.detail?.flush?.p99_ms}ms` +
        ` write p50/p99=${t5?.detail?.write?.p50_ms}/${t5?.detail?.write?.p99_ms}ms`,
    );
  }
  console.log(
    `DECISION committed-and-flushed data survives a tab kill in all engines tested: ${decision ? 'PASS' : 'FAIL'}`,
  );
  process.exitCode = complete ? 0 : 1;
}

process.on('exit', () => {
  if (live) {
    const proc = browserProcessOf(live.context);
    if (proc && proc.exitCode === null) {
      try {
        proc.kill('SIGKILL');
      } catch {
        /* already gone */
      }
    }
  }
  if (server) {
    try {
      server.proc.kill('SIGKILL');
    } catch {
      /* already gone */
    }
  }
});
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    stopServer();
    process.exit(130);
  });
}

try {
  await main();
} catch (err) {
  console.error('run.mjs: harness failure:', err);
  process.exitCode = 1;
} finally {
  await closeBrowser();
  stopServer();
}
