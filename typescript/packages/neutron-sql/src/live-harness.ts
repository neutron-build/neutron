import test from "node:test";
import type { Driver } from "./index.js";

// Shared plumbing for the live Postgres suites (*.live.*.test.ts).
//
// URL convention (one repo-wide variable):
//   NEUTRON_TEST_DATABASE_URL — Postgres URL the tests own; every suite
//     creates and drops its own uniquely named throwaway database.
//   NEUTRON_SQL_TEST_URL — legacy alias from earlier cards, still honored.
//   No variable set locally: every live test skips with a visible reason.
//
// Required-live mode: NEUTRON_LIVE_REQUIRED=1 (set in CI). A missing URL,
// an unreachable server, or a whole run that executes zero live cases
// fails the run instead of skipping.

export const LIVE_REQUIRED = process.env.NEUTRON_LIVE_REQUIRED === "1";
export const TEST_URL = process.env.NEUTRON_TEST_DATABASE_URL || process.env.NEUTRON_SQL_TEST_URL || "";

let liveRuns = 0;
process.on("exit", () => {
  if (LIVE_REQUIRED && liveRuns === 0) {
    const where = TEST_URL ? ` against ${TEST_URL}` : " (NEUTRON_TEST_DATABASE_URL is not set)";
    console.error(`live harness: NEUTRON_LIVE_REQUIRED=1 but zero live cases executed${where}`);
    process.exitCode = 1;
  }
});

export function noteLiveRun(): void {
  liveRuns++;
}

let probe: Promise<boolean> | null = null;
function reachable(): Promise<boolean> {
  if (!probe) {
    probe = (async () => {
      try {
        const { Pool } = (await import("pg")) as unknown as { Pool: new (o: object) => Driver & { query: (s: string) => Promise<unknown> } };
        const pool = new Pool({ connectionString: TEST_URL, connectionTimeoutMillis: 2000, max: 1 });
        try {
          await pool.query("select 1");
          return true;
        } finally {
          await (pool as unknown as { end: () => Promise<void> }).end();
        }
      } catch {
        return false;
      }
    })();
  }
  return probe;
}

export interface LiveGate {
  ok: boolean;
  reason: string;
}

export async function liveGate(): Promise<LiveGate> {
  if (!TEST_URL) {
    return { ok: false, reason: "NEUTRON_TEST_DATABASE_URL is not set (live suites are opt-in locally; see README)" };
  }
  if (!(await reachable())) {
    return { ok: false, reason: `Postgres not reachable at ${TEST_URL}` };
  }
  return { ok: true, reason: "" };
}

export async function ensureLive(label: string): Promise<boolean> {
  const gate = await liveGate();
  if (gate.ok) {
    noteLiveRun();
    return true;
  }
  if (LIVE_REQUIRED) {
    throw new Error(`${label}: required live mode — ${gate.reason}`);
  }
  test(`${label}: skipped — ${gate.reason}`, { skip: true }, () => {});
  return false;
}

export function uniqueDbName(prefix: string): string {
  return `${prefix}_${process.pid}_${Math.floor(Date.now() / 1000)}`;
}
