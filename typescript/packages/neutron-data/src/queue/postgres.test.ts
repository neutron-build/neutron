import assert from "node:assert/strict";
import test from "node:test";
import { PostgresQueueDriver } from "./postgres.js";

interface RecordedQuery {
  query: string;
  params?: unknown[];
}

type ResultFn = (q: RecordedQuery) => unknown[];

function jobClaimResult(row: Partial<Record<string, unknown>>): ResultFn {
  let claimed = false;
  return ({ query }) => {
    if (
      query.startsWith("UPDATE neutron_jobs") &&
      query.includes("FOR UPDATE SKIP LOCKED") &&
      !claimed
    ) {
      claimed = true;
      return [
        {
          attempts: 1,
          max_attempts: 3,
          created_at: new Date(),
          ...row,
        },
      ];
    }
    return [];
  };
}

function dueScheduleResult(row: Record<string, unknown>): ResultFn {
  let delivered = false;
  return ({ query }) => {
    if (query.startsWith("SELECT id, name, cron, payload FROM neutron_schedules") && !delivered) {
      delivered = true;
      return [row];
    }
    return [];
  };
}

class MockSql {
  readonly queries: RecordedQuery[] = [];
  readonly beginCalls: string[][] = [];
  results: ResultFn | null = null;

  async unsafe<T = Record<string, unknown>>(query: string, params?: unknown[]): Promise<T[]> {
    this.queries.push({ query, params });
    if (this.results) {
      return this.results({ query, params }) as T[];
    }
    return [];
  }

  async begin<T>(fn: (tx: MockSql) => Promise<T>): Promise<T> {
    const executed: string[] = [];
    this.beginCalls.push(executed);
    const tx = {
      unsafe: async (query: string, params?: unknown[]) => {
        executed.push(query);
        this.queries.push({ query, params });
        if (this.results) {
          return this.results({ query, params });
        }
        return [];
      },
    };
    return fn(tx as unknown as MockSql);
  }

  async end(): Promise<void> {}

  matching(fragment: string): RecordedQuery[] {
    return this.queries.filter((q) => q.query.includes(fragment));
  }
}

function makeDriver(sql: MockSql, options: Record<string, unknown> = {}) {
  return new PostgresQueueDriver(sql as any, {
    queueName: "unit",
    pollIntervalMs: 20,
    leaseMs: 300,
    maxAttempts: 3,
    ...options,
  });
}

const DDL_FRAGMENTS = [
  "CREATE TABLE IF NOT EXISTS neutron_jobs",
  "CREATE INDEX IF NOT EXISTS neutron_jobs_ready",
  "CREATE TABLE IF NOT EXISTS neutron_schedules",
];

test("add applies the DDL lazily and idempotently, then inserts the job", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql);

  await driver.add("send-email", { to: "a@b.c" });
  await driver.add("send-email", { to: "d@e.f" });
  await driver.close();

  for (const fragment of DDL_FRAGMENTS) {
    assert.equal(sql.matching(fragment).length, 1, `${fragment} must run exactly once`);
  }
  const inserts = sql.matching("INSERT INTO neutron_jobs");
  assert.equal(inserts.length, 2);
  assert.equal(inserts[0].params?.[1], "unit");
  assert.equal(inserts[0].params?.[2], "send-email");
  assert.deepEqual(JSON.parse(String(inserts[0].params?.[3])), { to: "a@b.c" });
  assert.equal(inserts[0].params?.[4], 3);
});

test("process drives the canonical FOR UPDATE SKIP LOCKED claim query", async () => {
  const sql = new MockSql();
  sql.results = jobClaimResult({
    id: "11111111-1111-1111-1111-111111111111",
    name: "task",
    payload: { n: 1 },
  });
  const driver = makeDriver(sql);
  const handled: unknown[] = [];
  await driver.process("task", (job) => {
    handled.push(job.payload);
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  await driver.close();

  const claim = sql.matching("attempts = attempts + 1");
  assert.ok(claim.length >= 1);
  assert.ok(claim[0].query.includes("FOR UPDATE SKIP LOCKED"));
  assert.ok(claim[0].query.includes("ORDER BY priority, run_at"));
  assert.ok(claim[0].query.includes("status = 'pending'"));
  assert.ok(claim[0].query.includes("run_at <= now()"));
  assert.deepEqual(handled, [{ n: 1 }]);

  const done = sql.matching("SET status = 'done'");
  assert.equal(done.length, 1);
});

test("a failed attempt within budget returns the job to pending with a future run_at", async () => {
  const sql = new MockSql();
  sql.results = jobClaimResult({
    id: "22222222-2222-2222-2222-222222222222",
    name: "flaky",
    payload: null,
  });
  const driver = makeDriver(sql);
  await driver.process("flaky", () => {
    throw new Error("transient");
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  await driver.close();

  const retry = sql.matching("SET status = 'pending', locked_at = NULL, locked_by = NULL,\n             last_error");
  assert.equal(retry.length, 1);
  assert.equal(retry[0].params?.[1], "transient");
  const runAt = retry[0].params?.[2] as Date;
  assert.ok(runAt instanceof Date && runAt.getTime() > Date.now() - 10);
});

test("a failed attempt at max_attempts dead-letters with last_error", async () => {
  const sql = new MockSql();
  sql.results = jobClaimResult({
    id: "33333333-3333-3333-3333-333333333333",
    name: "doomed",
    payload: null,
    attempts: 3,
    max_attempts: 3,
  });
  const driver = makeDriver(sql);
  await driver.process("doomed", () => {
    throw new Error("always");
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  await driver.close();

  const dead = sql.matching("SET status = 'dead'");
  assert.equal(dead.length, 1);
  assert.equal(dead[0].params?.[1], "always");
  assert.equal(sql.matching("SET status = 'pending', locked_at = NULL, locked_by = NULL,\n             last_error").length, 0);
});

test("each tick reaps active rows whose lease expired", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql);
  await driver.process("nothing", () => {});
  await new Promise((resolve) => setTimeout(resolve, 30));
  await driver.close();

  const reap = sql.matching("SET status = 'pending', locked_at = NULL, locked_by = NULL");
  const claimScopedReap = reap.filter((q) => q.query.includes("status = 'active'"));
  assert.ok(claimScopedReap.length >= 1);
  const cutoff = claimScopedReap[0].params?.[1] as Date;
  assert.ok(Math.abs(cutoff.getTime() - (Date.now() - 300)) < 500);
});

test("the retention sweep deletes done/dead rows past the window", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql, { retentionMs: 1000, retentionSweepIntervalMs: 0 });
  await driver.process("nothing", () => {});
  await new Promise((resolve) => setTimeout(resolve, 30));
  await driver.close();

  const sweep = sql.matching("DELETE FROM neutron_jobs");
  assert.ok(sweep.length >= 1);
  assert.ok(sweep[0].query.includes("IN ('done', 'dead')"));
});

test("due schedules are claimed transactionally, materialized as jobs, and advanced past now", async () => {
  const sql = new MockSql();
  sql.results = dueScheduleResult({
    id: "44444444-4444-4444-4444-444444444444",
    name: "ticker",
    cron: "*/5 * * * * *",
    payload: { tick: true },
  });
  const driver = makeDriver(sql);
  await driver.process("ticker", () => {});
  await new Promise((resolve) => setTimeout(resolve, 30));
  await driver.close();

  assert.ok(sql.beginCalls.length >= 1);
  const scheduleClaim = sql.matching("FROM neutron_schedules").find((q) =>
    q.query.includes("FOR UPDATE SKIP LOCKED")
  );
  assert.ok(scheduleClaim, "schedule claim must use SKIP LOCKED");
  assert.ok(scheduleClaim.query.includes("next_run_at <= now()"));

  const jobInsert = sql.matching("INSERT INTO neutron_jobs").find((q) =>
    (q.params as unknown[])[2] === "ticker"
  );
  assert.ok(jobInsert, "a job must be materialized for the due schedule");
  assert.deepEqual(JSON.parse(String(jobInsert.params?.[3])), { tick: true });

  const advance = sql.matching("UPDATE neutron_schedules");
  assert.equal(advance.length, 1);
  const next = advance[0].params?.[1] as Date;
  assert.ok(next.getTime() > Date.now() - 10, "next_run_at must be recomputed from now (drift-tolerant)");
});

test("a running job heartbeats its lease", async () => {
  const sql = new MockSql();
  sql.results = jobClaimResult({
    id: "55555555-5555-5555-5555-555555555555",
    name: "slow",
    payload: null,
  });
  const driver = makeDriver(sql, { leaseMs: 150 });
  await driver.process("slow", () => new Promise((resolve) => setTimeout(resolve, 500)));
  await new Promise((resolve) => setTimeout(resolve, 150));
  await driver.close();

  const heartbeats = sql.matching("SET locked_at = now()");
  assert.ok(heartbeats.length >= 1, "expected at least one heartbeat while the handler ran");
});

test("schedule upserts on (queue, name) with a computed next_run_at", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql);

  await driver.schedule("nightly", "0 3 * * *", { kind: "digest" });

  const upsert = sql.matching("INSERT INTO neutron_schedules");
  assert.equal(upsert.length, 1);
  assert.ok(upsert[0].query.includes("ON CONFLICT (queue, name)"));
  assert.equal(upsert[0].params?.[1], "unit");
  assert.equal(upsert[0].params?.[2], "nightly");
  assert.equal(upsert[0].params?.[3], "0 3 * * *");
  const next = upsert[0].params?.[5] as Date;
  assert.ok(next.getTime() > Date.now(), "next_run_at must be in the future");
  assert.ok(
    next.getTime() < Date.now() + 24.5 * 60 * 60 * 1000,
    "next_run_at must be the next 3am occurrence, not a distant one"
  );
  await driver.close();
});

test("schedule validates the cron pattern before touching the database", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql);

  await assert.rejects(() => driver.schedule("bad", "not-a-cron", null));
  assert.equal(sql.matching("INSERT INTO neutron_schedules").length, 0);
  await driver.close();
});

test("unschedule deletes the row for this queue", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql);

  await driver.schedule("nightly", "0 3 * * *", null);
  await driver.unschedule("nightly");

  const del = sql.matching("DELETE FROM neutron_schedules");
  assert.equal(del.length, 1);
  assert.deepEqual(del[0].params, ["unit", "nightly"]);
  await driver.close();
});

test("schedule honors the opts.queue override and unschedule targets the same queue", async () => {
  const sql = new MockSql();
  const driver = makeDriver(sql);

  await driver.schedule("elsewhere", "*/5 * * * *", null, { queue: "other-queue" });
  await driver.unschedule("elsewhere");

  const upsert = sql.matching("INSERT INTO neutron_schedules");
  assert.equal(upsert[0].params?.[1], "other-queue");
  const del = sql.matching("DELETE FROM neutron_schedules");
  assert.deepEqual(del[0].params, ["other-queue", "elsewhere"]);
  await driver.close();
});
