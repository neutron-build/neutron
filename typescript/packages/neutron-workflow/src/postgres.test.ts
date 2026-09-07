// Live integration against a real PostgreSQL, env-gated: skipped unless a
// server is reachable at NEUTRON_TEST_PG2_URL (default localhost:5435).
// Throwaway container for the run:
//   podman run -d --name neutron-test-pg2 -e POSTGRES_PASSWORD=test -p 5435:5432 postgres:16-alpine
// Conformance ports mirror the Memory/Nucleus suites (run.test.ts,
// replay.test.ts, nucleus.test.ts) plus the Postgres durability specifics:
// reconnect persistence, transactional append atomicity, lease expiry.
import assert from "node:assert/strict";
import { after, test } from "node:test";
import postgres from "postgres";

import { WIRE_FORMAT_VERSION, type WorkflowEvent } from "./events.js";
import { NondeterminismError } from "./errors.js";
import { LeaseManager, executeRunExclusive } from "./lease.js";
import { PostgresEventStore, type PostgresLike } from "./postgres-store.js";
import { cancelRun, completeSleep, deliverEvent, executeRun } from "./run.js";
import { workflow } from "./workflow.js";

const url = process.env.NEUTRON_TEST_PG2_URL ?? "postgres://postgres:test@localhost:5435/postgres";

let client: ReturnType<typeof postgres> | null = null;
let skip: string | boolean = false;
let connected: PostgresLike | undefined;
try {
  const probe = postgres(url, { connect_timeout: 2 });
  await probe`select 1`;
  client = probe;
  connected = probe;
} catch {
  skip = `no postgres reachable at ${url} (set NEUTRON_TEST_PG2_URL or start the throwaway container)`;
}

// Tests below only run when `connected` was set; the cast keeps call sites clean.
const sql = connected as PostgresLike;

const rid = (label: string): string => `pg-${label}-${crypto.randomUUID()}`;

const ev = (seq: number, name: string): WorkflowEvent =>
  JSON.parse(
    JSON.stringify({
      v: WIRE_FORMAT_VERSION,
      seq,
      type: "step-completed",
      at: new Date().toISOString(),
      name,
      data: { result: name },
    }),
  ) as WorkflowEvent;

/** Fast-forward a lease past its expiry without waiting out the TTL. */
const expireLease = (key: string): PromiseLike<unknown> =>
  sql`UPDATE neutron_workflow_leases SET expires_at = now() - interval '1 second' WHERE key = ${key}`;

after(async () => {
  await client?.end();
});

test("workflows run end-to-end on Postgres: suspend, resume, exactly-once steps", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const runId = rid("order");
  const executions: string[] = [];
  const wf = workflow("pg-order", async (ctx, input: { id: string }) => {
    const reserved = await ctx.step("reserve", () => {
      executions.push("reserve");
      return `r-${input.id}`;
    });
    await ctx.sleep("7d");
    return ctx.step("charge", () => {
      executions.push("charge");
      return { charged: reserved };
    });
  });

  const first = await executeRun({ workflow: wf, runId, store, input: { id: "42" } });
  assert.equal(first.status, "sleeping");
  await completeSleep(store, runId);
  const second = await executeRun({ workflow: wf, runId, store });
  assert.equal(second.status, "completed");
  assert.deepEqual(second.output, { charged: "r-42" });
  assert.deepEqual(executions, ["reserve", "charge"]);

  // jsonb round-trips the wire format intact, seq-ascending
  const events = await store.load(runId);
  assert.ok(events.length >= 5);
  for (const [index, event] of events.entries()) {
    assert.equal(event.v, WIRE_FORMAT_VERSION);
    assert.equal(typeof event.seq, "number");
    if (index > 0) assert.ok(event.seq > events[index - 1]!.seq);
  }

  // terminal runs are idempotent and append nothing new
  const logLength = events.length;
  const third = await executeRun({ workflow: wf, runId, store });
  assert.deepEqual(third, second);
  assert.equal((await store.load(runId)).length, logLength);
});

test("load returns seq-ordered events deduped by seq, first writer wins; unknown runs are empty", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const runId = rid("dedupe");
  assert.deepEqual(await store.load(rid("never-existed")), []);

  await store.append(runId, ev(0, "first-writer"));
  await store.append(runId, ev(0, "second-writer")); // same seq — dropped by the PK
  await store.append(runId, ev(1, "next"));

  const events = await store.load(runId);
  assert.equal(events.length, 2);
  assert.equal(events[0]?.name, "first-writer");
  assert.equal(events[1]?.name, "next");
});

test("an event delivered mid-pass does not corrupt the log (seq collision)", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const runId = rid("mid-pass");
  let stepExecutions = 0;

  const wf = workflow("pg-mid-pass", async (ctx) => {
    await ctx.step("a", async () => {
      stepExecutions += 1;
      // lands WHILE the pass is live and allocating dense seqs below 2^40
      await deliverEvent(store, runId, "wake", "wake-payload");
      return "a-result";
    });
    await ctx.sleep("60s");
    return ctx.waitForEvent("wake");
  });

  const parked = await executeRun({ workflow: wf, runId, store, input: null });
  assert.equal(parked.status, "sleeping");

  await completeSleep(store, runId);
  const woken = await executeRun({ workflow: wf, runId, store });
  assert.equal(woken.status, "completed");
  assert.equal(woken.output, "wake-payload");
  assert.equal(stepExecutions, 1, "step side effects must not re-run after a mid-pass delivery");
});

test("waitForEvent suspends and resumes; early deliveries buffer regardless of order", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const runId = rid("signals");
  const wf = workflow("pg-two-signals", async (ctx) => {
    const a = await ctx.waitForEvent<string>("a");
    const b = await ctx.waitForEvent<string>("b");
    return `${a}:${b}`;
  });

  const first = await executeRun({ workflow: wf, runId, store, input: null });
  assert.equal(first.status, "waiting");
  assert.equal(first.eventName, "a");

  // out of order: b arrives before a
  await deliverEvent(store, runId, "b", "beta");
  await deliverEvent(store, runId, "a", "alpha");
  const second = await executeRun({ workflow: wf, runId, store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "alpha:beta");
});

test("cancelRun settles a waiting run; terminal states are idempotent", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const runId = rid("cancel");
  const wf = workflow("pg-waiter", async (ctx) => {
    await ctx.step("a", () => "did-a");
    await ctx.waitForEvent("approval");
    return "done";
  });

  await assert.rejects(() => cancelRun(store, rid("ghost")), /Unknown run/);

  const parked = await executeRun({ workflow: wf, runId, store, input: null });
  assert.equal(parked.status, "waiting");

  await cancelRun(store, runId, "operator stopped it");
  const settled = await executeRun({ workflow: wf, runId, store });
  assert.equal(settled.status, "cancelled");
  assert.equal(settled.error?.detail, "operator stopped it");

  const again = await executeRun({ workflow: wf, runId, store });
  assert.equal(again.status, "cancelled");
  await cancelRun(store, runId); // no-op, no throw
  await assert.rejects(() => deliverEvent(store, runId, "approval", {}), /already finished/);
});

test("a run cannot be executed under a different workflow", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const runId = rid("identity");
  const a = workflow("pg-alpha", async () => "a");
  const b = workflow("pg-beta", async () => "b");
  await executeRun({ workflow: a, runId, store, input: null });
  await assert.rejects(executeRun({ workflow: b, runId, store }), NondeterminismError);
});

test("leases: single holder, conditional renew/release, expiry releases the run", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const leases = new LeaseManager(store, { prefix: "pgtest:lease", ttlSeconds: 30 });
  const runId = rid("lease");
  const key = `pgtest:lease:${runId}`;

  const a = await leases.acquire(runId, "executor-a");
  assert.ok(a);
  assert.equal(await leases.acquire(runId, "executor-b"), null);
  assert.equal(await a!.renew(), true);

  // expiry (a crashed holder): b steals, after which a can neither renew nor release
  await expireLease(key);
  const b = await leases.acquire(runId, "executor-b");
  assert.ok(b);
  assert.equal(await a!.renew(), false);
  assert.equal(await a!.release(), false);
  assert.equal(await b!.release(), true);

  // clean release frees the run immediately
  const c = await leases.acquire(runId, "executor-c");
  assert.ok(c);
  assert.equal(await c!.release(), true);
});

test("executeRunExclusive: one executor wins; a crashed holder is replayed after expiry", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const leases = new LeaseManager(store, { prefix: "pgtest:lease", ttlSeconds: 30 });
  const runId = rid("exclusive");
  const key = `pgtest:lease:${runId}`;
  let executions = 0;
  const wf = workflow("pg-job", async (ctx) => {
    await ctx.step("work", () => {
      executions += 1;
      return "done";
    });
    await ctx.sleep("1h");
    return "finished";
  });

  // a holder that acquired and then died without releasing
  const crashed = await leases.acquire(runId, "dead-executor");
  assert.ok(crashed);
  assert.equal(
    await executeRunExclusive({ workflow: wf, runId, store, input: null, leases, owner: "b" }),
    null,
  );

  // its lease expires; the next executor claims and replays from the log
  await expireLease(key);
  const outcome = await executeRunExclusive({ workflow: wf, runId, store, input: null, leases, owner: "b" });
  assert.equal(outcome?.status, "sleeping");
  assert.equal(executions, 1);

  // lease released after the pass: wake and finish under a new claim
  await completeSleep(store, runId);
  const final = await executeRunExclusive({ workflow: wf, runId, store, leases, owner: "c" });
  assert.equal(final?.status, "completed");
  assert.equal(final?.output, "finished");
  assert.equal(executions, 1); // the step never re-ran
});

test("events survive a client reconnect", { skip }, async () => {
  const first = postgres(url, { connect_timeout: 2 });
  const storeA = new PostgresEventStore(first);
  const runId = rid("reconnect");
  await storeA.append(runId, ev(0, "e0"));
  await storeA.append(runId, ev(1, "e1"));
  await storeA.append(runId, ev(2, "e2"));
  await first.end();

  // a fresh client + fresh store: connect() re-runs the DDL idempotently
  const second = postgres(url, { connect_timeout: 2 });
  const storeB = new PostgresEventStore(second);
  await storeB.connect();
  const events = await storeB.load(runId);
  assert.deepEqual(
    events.map((event) => event.name),
    ["e0", "e1", "e2"],
  );
  await second.end();
});

test("appends composed in one transaction are atomic", { skip }, async () => {
  const store = new PostgresEventStore(sql);
  const rolledBack = rid("tx-rollback");
  const committed = rid("tx-commit");
  const txClient = postgres(url, { connect_timeout: 2 });

  try {
    // tx is callable at runtime, but postgres.js's TransactionSql type
    // assigns to no simpler signature — hence the one cast.
    const asPgLike = (handle: unknown): PostgresLike => handle as PostgresLike;

    // a failure mid-transaction rolls back every append in it
    await assert.rejects(
      txClient.begin(async (tx) => {
        const txStore = new PostgresEventStore(asPgLike(tx));
        await txStore.append(rolledBack, ev(0, "r0"));
        await txStore.append(rolledBack, ev(1, "r1"));
        throw new Error("boom");
      }),
      /boom/,
    );
    assert.equal((await store.load(rolledBack)).length, 0);

    // a clean transaction keeps them all
    await txClient.begin(async (tx) => {
      const txStore = new PostgresEventStore(asPgLike(tx));
      await txStore.append(committed, ev(0, "c0"));
      await txStore.append(committed, ev(1, "c1"));
    });
    assert.deepEqual(
      (await store.load(committed)).map((event) => event.name),
      ["c0", "c1"],
    );
  } finally {
    await txClient.end();
  }
});
