// X04 MAJOR-2 regression probes — PgTransport.queryCancelable pid-probe window.
// (1) abort fired WHILE the pg_backend_pid probe is still in flight;
// (2) abort fired mid-statement on a connection whose pid probe FAILED
//     (engine without pg_backend_pid — previously a PERMANENT silent no-cancel).
// Both must REJECT with NucleusNotSupportedError (...; the statement completed
// anyway) — resolving normally would silently swallow the caller's abort.
// Deterministic: pg.Client.prototype.query is patched to delay/sabotage the
// probe; everything else runs against the real engine. Targets PostgreSQL
// (case 2 needs pg_sleep; Nucleus has no sleep function on this build —
// there, case 1 applies and case 2 is covered engine-independently by the
// fake-pool unit tests). Usage:
//   node scripts/x04-pid-window-probe.mjs <database-url>
import pg from "pg";
import { PgTransport } from "../dist/transport.js";
import { NucleusNotSupportedError } from "../dist/errors.js";

const url = process.argv[2];
if (!url) {
  console.error("usage: node x04-pid-window-probe.mjs <database-url>");
  process.exit(2);
}

const origQuery = pg.Client.prototype.query;
let exit = 0;
function sqlOf(args) {
  const c = args[0];
  return typeof c === "string" ? c : c?.text ?? "";
}

const t = new PgTransport(url);
// Warm the pool so checkout is instant — the probe window is the thing under test.
await t.fetchval("SELECT 41 AS v");

// --- Case 1: abort DURING the pid-probe window (first probe delayed 300ms) ---
{
  let delayed = false;
  pg.Client.prototype.query = function (...args) {
    if (!delayed && sqlOf(args).includes("pg_backend_pid")) {
      delayed = true;
      return new Promise((resolve) => setTimeout(() => resolve(origQuery.apply(this, args)), 300));
    }
    return origQuery.apply(this, args);
  };
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 100);
  try {
    const v = await t.fetchval("SELECT 42 AS v", [], { signal: ac.signal });
    console.log(`probe-window abort: RESOLVED with ${v} — caller abort SILENTLY IGNORED (FAIL)`);
    exit = 1;
  } catch (err) {
    const ok =
      err instanceof NucleusNotSupportedError &&
      /could not be dispatched/.test(err.message) &&
      /completed anyway/.test(err.message);
    console.log(`probe-window abort: rejected — ${err.constructor.name}: ${String(err.message).slice(0, 120)} ${ok ? "(PASS)" : "(FAIL: unexpected error)"}`);
    if (!ok) exit = 1;
  } finally {
    pg.Client.prototype.query = origQuery;
  }
}

// --- Case 2: pid probe FAILS; abort fires mid-statement (slow statement) ---
{
  let sabotaged = false;
  pg.Client.prototype.query = function (...args) {
    if (!sabotaged && sqlOf(args).includes("pg_backend_pid")) {
      sabotaged = true;
      // Reject asynchronously, the way a real engine error arrives — a
      // synchronous throw would bypass the .catch() under test.
      return Promise.reject(new Error("simulated engine without pg_backend_pid"));
    }
    return origQuery.apply(this, args);
  };
  const ac = new AbortController();
  setTimeout(() => ac.abort(), 100);
  try {
    const v = await t.fetchval("SELECT 42 AS v, pg_sleep(1) AS s", [], { signal: ac.signal });
    console.log(`probe-failure abort: RESOLVED with ${v} — PERMANENT silent no-cancel (FAIL)`);
    exit = 1;
  } catch (err) {
    const ok =
      err instanceof NucleusNotSupportedError &&
      /pg_backend_pid/.test(err.message) &&
      /completed anyway/.test(err.message);
    console.log(`probe-failure abort: rejected — ${err.constructor.name}: ${String(err.message).slice(0, 120)} ${ok ? "(PASS)" : "(FAIL: unexpected error)"}`);
    if (!ok) exit = 1;
  } finally {
    pg.Client.prototype.query = origQuery;
  }
}

const alive = await t.fetchval("SELECT 41 + 1 AS v");
console.log(`pool usable after: ${alive === 42 ? "yes (42)" : `NO (${alive})`}`);
if (alive !== 42) exit = 1;
await t.close();
console.log(exit === 0 ? "PID-WINDOW-PROBES: PASS" : "PID-WINDOW-PROBES: FAIL");
process.exit(exit);
