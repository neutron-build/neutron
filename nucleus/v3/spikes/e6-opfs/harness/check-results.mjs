#!/usr/bin/env node
// README↔JSON consistency check for the E6 results table (card X-6-F2-F1).
//
// The E6 README's Results table is rebuilt by hand from a committed raw
// record — results/full-<date>.json, written by harness/run.mjs and never
// hand-edited (card X-6-F2). Until this checker existed, nothing compared
// the two: editing one table cell (say flush p99 0.4 → 9.9 ms) passed
// every automated check (verdicts X-6-F2 of 9fa57ac7 / 45a788f9, both
// judging MERGE-WITH-FOLLOWUPS → this card). The script re-derives every
// fact the Results section states from the JSON that section itself
// cites, and fails with a cell ↔ JSON-path diff on any drift.
//
// Checked — everything inside the `## Results` section that the record
// can derive:
//   * the record citation (exactly one, display text = link target) and
//     that the cited file exists and is tracked by git ("committed",
//     card X-6-F2);
//   * the heading: run date (the cited file name's date), hardware
//     (meta.hardware, `; ` → `, `), Node version (meta.node), and that
//     "full run" is true (meta.quick === false — a quick run cannot
//     back this table; closes X-6-F2's json-quick-flip mutant);
//   * the command's --out record name and the datasets sentence
//     (T1/T2/T3/T4 pages, T5 samples, meta.quick — all meta.datasets);
//   * the `meta.date` line (meta.date truncated to whole seconds);
//   * the table: header column layout (including "T1 persist <N> pages"
//     from meta.datasets.t1_pages and "<K> KiB write" from every
//     engine's t5.detail.page_bytes), one row per `engines` key in the
//     record's order, and all 8 columns of every row re-derived exactly
//     as the README's legend describes — so every JSON path the legend
//     names is resolved here, plus per-engine t1.detail.pages.
//
// Deliberately NOT checked: the intro's engine version labels and
// Playwright version (absent from the record), the prose bullets under
// "Details worth keeping" (they also quote earlier, uncommitted runs),
// the legend and decision prose, and the meta.date line's local-time
// gloss (timezone-dependent). Those are words about the numbers, not
// numbers from the record. Numbers must appear exactly as their JSON
// values render in JS (String(number)): 0.4 stays "0.4", 2 stays "2".
//
// Usage:
//   node check-results.mjs [--readme=<README.md>] [--json=<record.json>]
//
// Both overrides exist for the card's mutation evidence (README-DRIFT,
// JSON-MUTANT run against copies outside the repo) and replace one input:
// --readme swaps the README, --json swaps the record (with --json the
// exists/tracked check is skipped, and the copy's file name must still be
// the cited one, so a copy can only ever stand in for the cited record).
//
// Exit codes: 0 = consistent (prints what was verified); 1 = mismatch
// (one MISMATCH block per drifted cell: README value ↔ JSON path and the
// value it gives); 2 = structural error (unreadable or unparseable
// input, missing record, missing JSON field, reshaped table) — the
// checker fails closed: it refuses to pass on anything it cannot fully
// derive.

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const SPIKE = path.resolve(HERE, '..');

// README display name of each `engines` key in the record. A new engine
// in a future record is a structural error until mapped here.
const DISPLAY = { chromium: 'Chromium', firefox: 'Firefox', webkit: 'WebKit' };

const mismatches = []; // { where, readme, jsonPath, expected }
const errors = []; // strings

function mismatch(where, readme, jsonPath, expected) {
  mismatches.push({ where, readme, jsonPath, expected });
}

function main() {
  const argv = process.argv.slice(2);
  const arg = (name) => {
    const hit = argv.find((a) => a.startsWith(`--${name}=`));
    return hit ? hit.split('=').slice(1).join('=') : undefined;
  };
  const readmeArg = arg('readme');
  const jsonArg = arg('json');
  for (const a of argv) {
    if (!a.startsWith('--readme=') && !a.startsWith('--json='))
      errors.push(`unknown argument '${a}' (only --readme=<path> and --json=<path>)`);
  }
  if (errors.length) return 2;

  // -- README and its Results section ------------------------------------
  const readmePath = path.resolve(readmeArg ?? path.join(SPIKE, 'README.md'));
  let readme;
  try {
    readme = readFileSync(readmePath, 'utf8');
  } catch (e) {
    errors.push(`cannot read the README (${readmePath}): ${e.message}`);
    return 2;
  }
  const headingLine = readme.match(/^## Results.*$/m);
  if (!headingLine) {
    errors.push('the README has no "## Results" section');
    return 2;
  }
  const afterHeading = readme.slice(headingLine.index + headingLine[0].length);
  const nextSection = afterHeading.search(/^## /m);
  const section = nextSection === -1 ? afterHeading : afterHeading.slice(0, nextSection);

  // -- the one record this section cites ---------------------------------
  const citeRe = /\[`(results\/full-\d{4}-\d{2}-\d{2}\.json)`\]\((results\/full-\d{4}-\d{2}-\d{2}\.json)\)/g;
  const citeAll = [...section.matchAll(citeRe)].map((m) => {
    if (m[1] !== m[2]) errors.push(`the record citation's display text (${m[1]}) and link target (${m[2]}) differ`);
    return m[1];
  });
  const cited = [...new Set(citeAll)];
  if (cited.length !== 1) {
    errors.push(
      `the Results section must cite exactly one results/full-<date>.json record with [\\\`path\\\`](path); found ${cited.length ? cited.join(', ') : 'none'}`,
    );
    return 2;
  }
  const citedRel = cited[0]; // e.g. results/full-2026-10-18.json
  const citedBase = path.basename(citedRel);

  let recordPath;
  if (jsonArg) {
    recordPath = path.resolve(jsonArg);
    if (path.basename(recordPath) !== citedBase) {
      errors.push(`--json file '${path.basename(recordPath)}' is not the record the Results section cites ('${citedBase}')`);
      return 2;
    }
  } else {
    recordPath = path.join(SPIKE, citedRel);
    if (!existsSync(recordPath)) {
      errors.push(`the cited record ${recordPath} does not exist`);
      return 2;
    }
    // The record must be committed, not a local leftover of a later run.
    const git = spawnSync('git', ['ls-files', '--', citedRel], { cwd: SPIKE, encoding: 'utf8' });
    if (git.error || git.status !== 0) {
      errors.push(`git ls-files failed for ${citedRel} (${git.error ? git.error.message : `exit ${git.status}`}) — cannot confirm the record is committed`);
    } else if (!git.stdout.trim()) {
      errors.push(`the cited record ${citedRel} is not tracked by git; a Results record must be committed (card X-6-F2)`);
    }
  }

  let record;
  try {
    record = JSON.parse(readFileSync(recordPath, 'utf8'));
  } catch (e) {
    errors.push(`cannot read or parse the record ${recordPath}: ${e.message}`);
    return 2;
  }

  // -- typed field access; every miss is a structural error ---------------
  const need = (root, fieldPath, type, label) => {
    let v = root;
    for (const seg of fieldPath.split('.')) v = v && typeof v === 'object' ? v[seg] : undefined;
    if (v === undefined || v === null) {
      errors.push(`${label}.${fieldPath} is missing from the record`);
      return undefined;
    }
    if (typeof v !== type) {
      errors.push(`${label}.${fieldPath} is a ${typeof v}, expected ${type}`);
      return undefined;
    }
    return v;
  };

  const meta = record.meta;
  if (!meta || typeof meta !== 'object') {
    errors.push('the record has no meta object');
    return 2;
  }
  const datasets = need(meta, 'datasets', 'object', 'meta');
  const t1Pages = need(datasets, 't1_pages', 'number', 'meta.datasets');
  const t2Pages = need(datasets, 't2_pages', 'number', 'meta.datasets');
  const t3Pages = need(datasets, 't3_pages', 'number', 'meta.datasets');
  const t4Pages = need(datasets, 't4_pages', 'number', 'meta.datasets');
  const samples = need(datasets, 'latency_samples', 'number', 'meta.datasets');
  const quick = need(meta, 'quick', 'boolean', 'meta');
  const hardware = need(meta, 'hardware', 'string', 'meta');
  const node = need(meta, 'node', 'string', 'meta');
  const metaDate = need(meta, 'date', 'string', 'meta');
  if (quick !== undefined && quick !== false) {
    errors.push(`the Results heading says "full run" but meta.quick is ${quick} — a quick run cannot back this table`);
  }

  // -- heading: full run, <file date>, <hardware>, Node <version> ---------
  const head = headingLine[0].trimEnd().match(/^## Results \(full run, (\d{4}-\d{2}-\d{2}), (.+), Node (v[^\s)]+)\)$/);
  if (!head) {
    errors.push(`the Results heading changed shape: "${headingLine[0].trim()}" (expected "## Results (full run, <date>, <hardware>, Node <version>)")`);
    return 2;
  }
  const fileDate = citedBase.slice('full-'.length, -'.json'.length);
  if (head[1] !== fileDate) mismatch('Results heading, run date', head[1], '(the cited record file name)', fileDate);
  const hardwareExpected = hardware.replace('; ', ', ');
  if (head[2] !== hardwareExpected) mismatch('Results heading, hardware', head[2], 'meta.hardware', hardwareExpected);
  if (head[3] !== node) mismatch('Results heading, Node version', head[3], 'meta.node', node);

  // -- the command's --out record and the datasets sentence ---------------
  const out = section.match(/--out=\.\.\/(results\/full-\d{4}-\d{2}-\d{2}\.json)/);
  if (!out) {
    errors.push('the Results command no longer names its record with --out=../results/full-<date>.json');
  } else if (out[1] !== citedRel) {
    mismatch('Results command, --out record', out[1], '(the record this section cites)', citedRel);
  }

  const ds = section.match(
    /datasets:\s*T1 (\d+) pages,\s*T2\/T3\/T4 (\d+) pages,\s*T5 (\d+) samples\s*\(`meta\.datasets`,\s*`meta\.quick:\s*(true|false)`\)/,
  );
  if (!ds) {
    errors.push('the datasets sentence (T1/T2/T3/T4 pages, T5 samples, meta.quick) is missing or reshaped');
  } else {
    if (ds[1] !== String(t1Pages)) mismatch('datasets sentence, T1 pages', ds[1], 'meta.datasets.t1_pages', String(t1Pages));
    if (t2Pages === t3Pages && t3Pages === t4Pages) {
      if (ds[2] !== String(t2Pages)) mismatch('datasets sentence, T2/T3/T4 pages', ds[2], 'meta.datasets.t2_pages/t3_pages/t4_pages', String(t2Pages));
    } else {
      mismatch('datasets sentence, T2/T3/T4 pages', ds[2], 'meta.datasets.t2_pages/t3_pages/t4_pages', `${t2Pages}/${t3Pages}/${t4Pages} (they differ; "T2/T3/T4 N" can no longer summarise them)`);
    }
    if (ds[3] !== String(samples)) mismatch('datasets sentence, T5 samples', ds[3], 'meta.datasets.latency_samples', String(samples));
    if (ds[4] !== String(quick)) mismatch('datasets sentence, meta.quick', ds[4], 'meta.quick', String(quick));
  }

  // -- the meta.date line -------------------------------------------------
  const md = section.match(/`meta\.date` is `([^`]+)`/);
  const seconds = metaDate ? metaDate.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})/) : null;
  if (!md) errors.push('the `meta.date` line is missing from the Results section');
  else if (!seconds) errors.push(`meta.date "${metaDate}" is not an ISO timestamp this checker can compare`);
  else if (md[1] !== `${seconds[1]}Z`) mismatch('meta.date line', md[1], 'meta.date (whole seconds)', `${seconds[1]}Z`);

  // -- the table -----------------------------------------------------------
  const engines = record.engines;
  if (!engines || typeof engines !== 'object') {
    errors.push('the record has no engines object');
    return 2;
  }
  const keys = Object.keys(engines);
  for (const k of keys) {
    if (!DISPLAY[k]) errors.push(`engines.${k} has no display name in this checker; extend DISPLAY when a record adds an engine`);
  }
  if (errors.length) return 2;

  const lines = section.split('\n').map((l) => l.trim());
  const blocks = [];
  let current = [];
  for (const l of lines) {
    if (l.startsWith('|')) current.push(l);
    else if (current.length) {
      blocks.push(current);
      current = [];
    }
  }
  if (current.length) blocks.push(current);
  const tables = blocks.filter((b) => b.length >= 3 && /^[-\s|:]+$/.test(b[1]));
  if (tables.length !== 1) {
    errors.push(`expected exactly one table (header, |---| separator, rows) in the Results section, found ${tables.length}`);
    return 2;
  }
  const cellsOf = (line) => line.replace(/^\|/, '').replace(/\|$/, '').split('|').map((c) => c.trim());
  const header = cellsOf(tables[0][0]);
  const rowLines = tables[0].slice(2);

  const rows = [];
  for (const line of rowLines) {
    const cells = cellsOf(line);
    if (cells.length !== header.length) {
      errors.push(`table row "${line.slice(0, 50)}…" has ${cells.length} cells, the header has ${header.length}`);
      continue;
    }
    const label = cells[0].match(/^\*\*([^*]+)\*\*/);
    const key = label ? Object.keys(DISPLAY).find((k) => DISPLAY[k] === label[1]) : undefined;
    if (!key) {
      errors.push(`table row label "${cells[0]}" is not **Chromium**/**Firefox**/**WebKit**`);
      continue;
    }
    rows.push({ key, cells });
  }
  if (rows.length !== keys.length) {
    errors.push(`the table has ${rows.length} engine rows, the record has ${keys.length} engines (${keys.join(', ')})`);
    return 2;
  }
  if (rows.map((r) => r.key).join(',') !== keys.join(',')) {
    errors.push(`table rows [${rows.map((r) => r.key).join(', ')}] do not match the record's engines [${keys.join(', ')}] — one row per engine, same order`);
    return 2;
  }

  // -- every field the table cells are derived from ------------------------
  const data = {};
  for (const k of keys) {
    const tests = engines[k] && engines[k].tests;
    if (!tests || typeof tests !== 'object') {
      errors.push(`engines.${k}.tests is missing`);
      return 2;
    }
    const label = `engines.${k}.tests`;
    const num = (p) => need(tests, p, 'number', label);
    const bool = (p) => need(tests, p, 'boolean', label);
    const arr = (p) => {
      const v = need(tests, p, 'object', label);
      if (v !== undefined && !Array.isArray(v)) {
        errors.push(`${label}.${p} is not an array`);
        return undefined;
      }
      return v;
    };
    data[k] = {
      t1Status: need(tests, 't1.status', 'string', label),
      t1Pages: num('t1.detail.pages'),
      t1WalOk: bool('t1.detail.wal.ok'),
      t1WalLen: num('t1.detail.wal.len'),
      tabOk: bool('t2.detail.tab_kill.ok'),
      tabWalOk: bool('t2.detail.tab_kill.wal.ok'),
      tabWalLen: num('t2.detail.tab_kill.wal.len'),
      brOk: bool('t2.detail.browser_kill.ok'),
      brWalOk: bool('t2.detail.browser_kill.wal.ok'),
      brWalLen: num('t2.detail.browser_kill.wal.len'),
      brKilled: bool('t2.detail.browser_kill.kill.killed'),
      brPids: arr('t2.detail.browser_kill.kill.pids'),
      t3Written: num('t3.detail.pages_written'),
      t3Intact: num('t3.detail.intact_pages_after_kill'),
      t3WalOk: bool('t3.detail.wal_content_after_kill.ok'),
      t3Killed: bool('t3.detail.kill.killed'),
      t3Pids: arr('t3.detail.kill.pids'),
      t4Waited: bool('t4.detail.second_tab_waited_while_held'),
      t4WaitMs: num('t4.detail.b_wait_ms'),
      t4GrantMs: num('t4.detail.release_to_grant_ms'),
      t4WalOk: bool('t4.detail.final_check.wal.ok'),
      t4WalLen: num('t4.detail.final_check.wal.len'),
      flushP50: num('t5.detail.flush.p50_ms'),
      flushP99: num('t5.detail.flush.p99_ms'),
      writeP50: num('t5.detail.write.p50_ms'),
      writeP99: num('t5.detail.write.p99_ms'),
      pageBytes: num('t5.detail.page_bytes'),
    };
  }
  if (errors.length) return 2;

  const kibs = new Set(keys.map((k) => data[k].pageBytes / 1024));
  if (kibs.size !== 1 || !Number.isInteger([...kibs][0])) {
    errors.push(`t5.detail.page_bytes must agree across engines and be a whole number of KiB, got ${keys.map((k) => `${k}=${data[k].pageBytes}`).join(', ')}`);
    return 2;
  }
  const kib = [...kibs][0];

  // -- header column layout, with its two data-derived numbers -------------
  const expectedHeader = [
    '',
    `T1 persist ${t1Pages} pages + reload`,
    'T2 tab kill',
    'T2 browser SIGKILL (real PIDs)',
    'T3 unflushed, then SIGKILL (real PIDs)',
    'T4 second tab waits',
    'WAL content',
    'flush p50/p99',
    `${kib} KiB write p50/p99`,
  ];
  if (header.length !== expectedHeader.length) {
    errors.push(`the table header has ${header.length} columns, expected ${expectedHeader.length} (${expectedHeader.filter(Boolean).join(' | ')})`);
    return 2;
  }
  for (let i = 1; i < header.length; i++) {
    if (header[i] !== expectedHeader[i]) {
      const src = i === 1 ? 'meta.datasets.t1_pages' : i === header.length - 1 ? 'engines.*.tests.t5.detail.page_bytes' : '(column layout)';
      mismatch(`table header, column ${i} ("${expectedHeader[i]}")`, header[i], src, expectedHeader[i]);
    }
  }
  for (const k of keys) {
    if (data[k].t1Pages !== t1Pages) {
      mismatch(`table header "T1 persist ${t1Pages} pages" vs engines.${k}`, String(t1Pages), `engines.${k}.tests.t1.detail.pages`, String(data[k].t1Pages));
    }
  }

  // -- the eight cells of every engine row ---------------------------------
  const killPhrase = (killed, pids, path) =>
    killed ? `killed ${pids.length} pid${pids.length === 1 ? '' : 's'}` : `NOT killed (${path} is false)`;
  const expectedCells = (k) => {
    const d = data[k];
    const p = (sub) => `engines.${k}.tests.${sub}`;
    const t2Wal = d.tabWalLen === d.brWalLen ? `${d.tabWalLen}` : `${d.tabWalLen}/${d.brWalLen}`;
    const walExact = d.t1WalOk && d.tabWalOk && d.brWalOk && d.t4WalOk;
    return {
      1: { exp: d.t1Status, path: p('t1.status') },
      2: { exp: d.tabOk ? 'ok' : 'failed', path: p('t2.detail.tab_kill.ok') },
      3: {
        exp: `${d.brOk ? 'ok' : 'failed'}, ${killPhrase(d.brKilled, d.brPids, p('t2.detail.browser_kill.kill.killed'))}`,
        path: `${p('t2.detail.browser_kill.ok')} + ${p('t2.detail.browser_kill.kill.pids')} + ${p('t2.detail.browser_kill.kill.killed')}`,
      },
      4: {
        exp: `${d.t3Intact}/${d.t3Written} pages + ${d.t3WalOk ? 'WAL record' : 'WAL mismatch'}, ${killPhrase(d.t3Killed, d.t3Pids, p('t3.detail.kill.killed'))}`,
        path: `${p('t3.detail.intact_pages_after_kill')} + ${p('t3.detail.pages_written')} + ${p('t3.detail.wal_content_after_kill.ok')} + ${p('t3.detail.kill.pids')}`,
      },
      5: {
        exp: `${d.t4Waited ? 'yes' : 'no'} (${d.t4WaitMs} ms), granted +${d.t4GrantMs} ms after release`,
        path: `${p('t4.detail.second_tab_waited_while_held')} + ${p('t4.detail.b_wait_ms')} + ${p('t4.detail.release_to_grant_ms')}`,
      },
      6: {
        exp: `${walExact ? 'exact' : 'inexact'} (T1 ${d.t1WalLen} B, T2 ${t2Wal} B, T4 ${d.t4WalLen} B)`,
        path: `${p('t1.detail.wal')} + ${p('t2.detail.tab_kill.wal')} + ${p('t2.detail.browser_kill.wal')} + ${p('t4.detail.final_check.wal')}`,
      },
      7: { exp: `${d.flushP50} / ${d.flushP99} ms`, path: `${p('t5.detail.flush.p50_ms')} + ${p('t5.detail.flush.p99_ms')}` },
      8: { exp: `${d.writeP50} / ${d.writeP99} ms`, path: `${p('t5.detail.write.p50_ms')} + ${p('t5.detail.write.p99_ms')}` },
    };
  };
  for (const row of rows) {
    const exp = expectedCells(row.key);
    for (let i = 1; i < header.length; i++) {
      if (row.cells[i] !== exp[i].exp) mismatch(`table row ${row.key}, column "${header[i]}"`, row.cells[i], exp[i].path, exp[i].exp);
    }
  }
  if (mismatches.length) return 1;

  const cells = rows.length * (header.length - 1);
  console.log(`OK  e6-opfs README Results section matches the committed record ${citedBase}`);
  console.log(`    record : ${recordPath}${jsonArg ? ' (override)' : ' (tracked by git)'}`);
  console.log(`    engines: ${keys.join(', ')} — one row each, table order = record order`);
  console.log(`    checked: table header (incl. T1 pages and the KiB figure) and ${cells} table cells;`);
  console.log(`             heading (date, hardware, Node), --out record, datasets sentence`);
  console.log(`             (T1/T2/T3/T4 pages, T5 samples, meta.quick), meta.date line`);
  return 0;
}

let code = 2;
try {
  code = main();
} catch (e) {
  errors.push(`unexpected failure: ${e && e.stack ? e.stack : e}`);
  code = 2;
}
if (code !== 0) {
  for (const e of errors) console.error(`ERROR ${e}`);
  for (const m of mismatches) {
    console.error(`MISMATCH ${m.where}`);
    console.error(`  README: ${JSON.stringify(m.readme)}`);
    console.error(`  JSON:   ${m.jsonPath} -> ${JSON.stringify(m.expected)}`);
  }
  console.error(`FAIL — the README Results section and ${code === 1 ? `${mismatches.length} cell(s) disagree with the record` : 'the record could not be fully checked'} (see above)`);
  if (!errors.length && !mismatches.length) {
    console.error('ERROR failed without recording a reason');
    code = 2;
  }
}
process.exit(code);
