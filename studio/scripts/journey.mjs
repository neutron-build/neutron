#!/usr/bin/env node
// Studio edit journey in a real browser (orm-program R01, VERIFICATION V15).
//
//   node scripts/journey.mjs --cli <neutron binary> [--out file.json]
//
// The CLI binary serves its embedded Studio; headless Chrome drives the UI
// like a user (double-click a cell, type, Enter/Escape, the value-state
// control, the commit bar, Revert commit) against a disposable PostgreSQL
// database, and every outcome is checked in the database with psql — never
// through Studio.
//
//   1. one atomic batch of four staged edits — text, empty string, SQL NULL
//      and an exact 40-digit numeric, two of them on the same row: psql sees
//      'world', '' (not NULL), NULL and the exact numeric; Escape stages
//      nothing;
//   2. Revert commit restores every original value;
//   3. a stale row: an edit staged on a row another session changes before
//      Commit is refused with the commit bar's alert, the draft stays
//      staged, and the concurrent value survives;
//   4. a batch whose second operation fails in the database (CHECK
//      constraint) applies nothing: the first staged edit is not there.
//
// Needs NEUTRON_TEST_DATABASE_URL, psql and Chrome (or CHROME_PATH). The
// Studio runs with a temporary HOME and no PATH (no browser launch, no access
// to the user's saved connections).

import { spawn, execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { createServer } from "node:net";
import os from "node:os";
import path from "node:path";
import { chromium } from "playwright-core";

const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : fallback;
};
const CLI = opt("--cli", null);
const OUT = opt("--out", null);
const TEST_URL = process.env.NEUTRON_TEST_DATABASE_URL || "";
if (!CLI || !TEST_URL) {
  console.error("journey: --cli <neutron binary> and NEUTRON_TEST_DATABASE_URL are required");
  process.exit(2);
}

const DB_NAME = `studio_journey_${process.pid}_${Math.floor(Date.now() / 1000)}`;
const dbUrl = (db) => {
  const u = new URL(TEST_URL);
  u.pathname = `/${db}`;
  return u.toString();
};
const psql = (db, sqlText) => execFileSync("psql", [dbUrl(db), "-v", "ON_ERROR_STOP=1", "-qAtc", sqlText], { encoding: "utf8" }).trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
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

const results = [];
function verdict(id, pass, detail) {
  results.push({ id, pass, detail });
  console.error(`${pass ? "PASS" : "FAIL"} ${id} :: ${detail}`);
}

// Grid helpers: the grid is a real <table role="grid">; columns are found by
// header text (key columns carry a "PK" badge before the name), rows by
// their rendered id cell.
async function cell(page, id, column) {
  const handle = await page.evaluateHandle(
    ([rowId, col]) => {
      const grid = document.querySelector('table[role="grid"]');
      const heads = [...grid.querySelectorAll("thead th")];
      const name = (th) => th.textContent.trim().split(/\s+/).pop();
      const colIdx = heads.findIndex((th) => name(th) === col);
      const idIdx = heads.findIndex((th) => name(th) === "id");
      const row = [...grid.querySelectorAll("tbody tr[data-row-index]")].find((tr) => tr.children[idIdx]?.textContent.trim() === String(rowId));
      return row ? row.children[colIdx] : null;
    },
    [id, column],
  );
  const el = handle.asElement();
  if (!el) throw new Error(`grid cell ${column} of row ${id} not found`);
  return el;
}
async function stageText(page, id, column, value, key = "Enter") {
  await (await cell(page, id, column)).dblclick();
  const input = page.locator('input[aria-label$=" value"]').first();
  await input.waitFor();
  await input.fill(value);
  await input.press(key);
}
async function stageNull(page, id, column) {
  await (await cell(page, id, column)).dblclick();
  const state = page.locator('select[aria-label$=" value state"]').first();
  await state.waitFor();
  await state.focus();
  await state.selectOption("null");
  // Chrome moves focus to the (now disabled) text input and blurs it, so the
  // editor commits NULL on blur right away; press Enter only if it is still
  // open (R01 finding: NULL stages on selection, before Enter).
  await sleep(100);
  if (await state.isVisible().catch(() => false)) await state.press("Enter");
}
async function stagedCount(page) {
  return page.evaluate(() => {
    const b = [...document.querySelectorAll("button")].find((x) => /^Commit \d+ change/.test(x.textContent.trim()));
    return b ? Number(/\d+/.exec(b.textContent)[0]) : 0;
  });
}
async function commit(page) {
  await page.getByRole("button", { name: /^Commit \d+ changes?$/ }).click();
}
async function commitFailedAlert(page) {
  const alert = page.locator('[role="alert"]').filter({ hasText: /commit failed/i }).first();
  try {
    await alert.waitFor({ timeout: 15000 });
    return (await alert.textContent()) ?? "";
  } catch {
    return "";
  }
}

const cleanups = [];
try {
  psql("postgres", `create database "${DB_NAME}"`);
  cleanups.push(() => psql("postgres", `drop database if exists "${DB_NAME}" with (force)`));
  psql(
    DB_NAME,
    `create table journey (id integer primary key, body text, qty integer not null default 7 check (qty < 1000), amount numeric(40,4));
     insert into journey (id, body, amount) values (1, 'hello', 1.5), (2, 'there', 2.5), (3, 'again', 3.5), (4, 'stale', 4.5), (5, 'atomic', 5.5);`,
  );

  const home = mkdtempSync(path.join(os.tmpdir(), "studio-journey-home-"));
  cleanups.push(() => rmSync(home, { recursive: true, force: true }));
  mkdirSync(path.join(home, ".neutron"), { mode: 0o700 });
  writeFileSync(path.join(home, ".neutron", "studio.json"), JSON.stringify([{ id: "r01journey", name: "journey", url: dbUrl(DB_NAME), isNucleus: false }]), {
    mode: 0o600,
  });
  const port = await freePort();
  const studio = spawn(path.resolve(CLI), ["studio", "--port", String(port)], { env: { HOME: home, PATH: "" }, stdio: ["ignore", "pipe", "pipe"] });
  let log = "";
  studio.stdout.on("data", (d) => (log += d));
  studio.stderr.on("data", (d) => (log += d));
  cleanups.push(() => studio.kill("SIGTERM"));
  const base = `http://127.0.0.1:${port}`;
  for (let i = 0; ; i++) {
    try {
      if ((await fetch(`${base}/`)).ok) break;
    } catch {}
    if (i > 100 || studio.exitCode !== null) throw new Error(`studio did not start:\n${log}`);
    await sleep(100);
  }
  const browser = await chromium.launch(process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH } : { channel: "chrome" });
  cleanups.push(() => browser.close());
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  const pageErrors = [];
  page.on("pageerror", (e) => pageErrors.push(String(e)));

  const open = async () => {
    await page.goto(`${base}/#/c/r01journey/t/public/journey`);
    await page.waitForFunction(() => document.querySelectorAll('table[role="grid"] tbody tr[data-row-index]').length === 5, null, { timeout: 30000 });
  };
  await open();

  // 1. Escape stages nothing; then one atomic batch of four edits.
  await stageText(page, 1, "body", "abandoned", "Escape");
  await sleep(200);
  verdict("V15.escape: Escape discards the edit", (await stagedCount(page)) === 0, `staged=${await stagedCount(page)}`);
  const bigNumeric = "123456789012345678901234567890123456.7891";
  await stageText(page, 1, "body", "world");
  await stageText(page, 2, "body", "");
  await stageNull(page, 3, "body");
  await stageText(page, 1, "amount", bigNumeric);
  await sleep(200);
  const staged = await stagedCount(page);
  verdict("V15.stage: four edits stage four operations, once each", staged === 4, `staged=${staged}`);
  await commit(page);
  await page.waitForFunction(() => ![...document.querySelectorAll("button")].some((b) => /^Commit \d+ change/.test(b.textContent.trim())), null, { timeout: 15000 }).catch(() => {});
  const alert1 = await page.locator('[role="alert"]').filter({ hasText: /commit failed/i }).count();
  const rows1 = psql(DB_NAME, `select id || '|' || coalesce(body, '<NULL>') || '|' || (body is null) || '|' || amount::text from journey where id in (1,2,3) order by id`).split("\n");
  verdict(
    "V15.batch: text, empty string and NULL stay distinct; numeric exact; two edits of one row commit",
    alert1 === 0 && rows1[0] === `1|world|false|${bigNumeric}` && rows1[1] === "2||false|2.5000" && rows1[2] === "3|<NULL>|true|3.5000",
    rows1.join(" ; "),
  );

  // 2. Revert commit restores the originals.
  await page.getByRole("button", { name: "Revert commit" }).click();
  let rowsR = "";
  for (let i = 0; i < 50; i++) {
    rowsR = psql(DB_NAME, `select string_agg(id || '|' || coalesce(body, '<NULL>') || '|' || amount::text, ' ; ' order by id) from journey where id in (1,2,3)`);
    if (rowsR === "1|hello|1.5000 ; 2|there|2.5000 ; 3|again|3.5000") break;
    await sleep(100);
  }
  verdict("V15.revert: Revert commit restores every original value", rowsR === "1|hello|1.5000 ; 2|there|2.5000 ; 3|again|3.5000", rowsR);

  // 3. Stale row: staged edit, concurrent change, commit refused, draft kept.
  await open();
  await stageText(page, 4, "body", "from-studio");
  psql(DB_NAME, `update journey set body = 'concurrent' where id = 4`);
  await commit(page);
  const alertText = await commitFailedAlert(page);
  const keptStaged = await stagedCount(page);
  const row4 = psql(DB_NAME, `select body from journey where id = 4`);
  verdict(
    "V15.stale: the concurrent change wins, the commit is refused in the UI, the draft stays staged",
    row4 === "concurrent" && alertText !== "" && keptStaged === 1,
    `db=${row4}; alert=${JSON.stringify(alertText.slice(0, 160))}; staged=${keptStaged}`,
  );
  await page.getByRole("button", { name: "Discard all" }).click();

  // 4. Atomicity: the second operation violates a CHECK constraint in the
  // database; the first must not be applied either.
  await open();
  await stageText(page, 5, "body", "should-not-land");
  await stageText(page, 5, "qty", "5000");
  await commit(page);
  const alert4 = await commitFailedAlert(page);
  const row5 = psql(DB_NAME, `select body || '|' || qty from journey where id = 5`);
  verdict("V15.atomic: a failing operation rolls back the whole batch", row5 === "atomic|7" && alert4 !== "", `db=${row5}; alert=${JSON.stringify(alert4.slice(0, 160))}`);
  verdict("V15.page: no uncaught page errors", pageErrors.length === 0, pageErrors.slice(0, 3).join(" | ") || "none");
} catch (err) {
  verdict("harness", false, err instanceof Error ? err.stack : String(err));
} finally {
  for (const c of cleanups.reverse()) {
    try {
      await c();
    } catch (err) {
      verdict("cleanup", false, String(err));
    }
  }
}
const failed = results.filter((r) => !r.pass).length;
if (OUT) writeFileSync(OUT, JSON.stringify({ tool: "studio journey", results }, null, 2) + "\n");
console.error(`journey: ${results.length - failed}/${results.length} passed`);
process.exit(failed === 0 ? 0 : 1);
