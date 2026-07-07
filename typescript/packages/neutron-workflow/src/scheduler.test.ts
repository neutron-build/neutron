import assert from "node:assert/strict";
import { test } from "node:test";

import { createEventsHandler } from "./events-http.js";
import { LeaseManager, type KVLike } from "./lease.js";
import { deliverEvent, executeRun } from "./run.js";
import { RunIndex, Scheduler, type DocumentLike } from "./scheduler.js";
import { MemoryEventStore } from "./store.js";
import { workflow } from "./workflow.js";

function fakeDocs(): DocumentLike {
  const docs: Array<Record<string, unknown>> = [];
  const matches = (doc: Record<string, unknown>, filter: Record<string, unknown>) =>
    Object.entries(filter).every(([key, value]) => doc[key] === value);
  return {
    async insert(_collection, doc) {
      docs.push({ ...doc });
      return docs.length;
    },
    async find(_collection, filter) {
      return docs.filter((doc) => matches(doc, filter)).map((doc) => ({ ...doc }));
    },
    async update(_collection, filter, update) {
      let count = 0;
      for (const doc of docs) {
        if (matches(doc, filter)) {
          Object.assign(doc, update);
          count += 1;
        }
      }
      return count;
    },
  };
}

function simpleKV(): KVLike {
  const map = new Map<string, string>();
  return {
    async setNX(key, value) {
      if (map.has(key)) return false;
      map.set(key, value);
      return true;
    },
    async cdel(key, expected) {
      if (map.get(key) !== expected) return false;
      map.delete(key);
      return true;
    },
    async cexpire(key, expected) {
      return map.get(key) === expected;
    },
  };
}

function harness() {
  const store = new MemoryEventStore();
  const index = new RunIndex(fakeDocs());
  const leases = new LeaseManager(simpleKV(), { ttlSeconds: 30 });
  return { store, index, leases };
}

test("scheduler wakes due sleepers and completes them", async () => {
  const { store, index, leases } = harness();
  const wf = workflow("nightly", async (ctx) => {
    await ctx.step("work", () => "did work");
    await ctx.sleep("1h");
    return "done";
  });

  const outcome = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await index.record("run-1", wf.name, outcome);
  const scheduler = new Scheduler({ workflows: [wf], store, leases, index, owner: "worker-1" });

  // before the wake time: nothing happens
  await scheduler.tick(new Date(Date.now() + 60_000));
  assert.equal((await store.load("run-1")).some((e) => e.type === "run-completed"), false);

  // past the wake time: the run resumes and completes
  await scheduler.tick(new Date(Date.now() + 2 * 3_600_000));
  const events = await store.load("run-1");
  assert.equal(events.some((e) => e.type === "run-completed"), true);

  // and the index reflects it — a further tick finds nothing due
  await scheduler.tick(new Date(Date.now() + 3 * 3_600_000));
  assert.deepEqual(await index.due(new Date(Date.now() + 4 * 3_600_000)), []);
});

test("the events handler delivers, flags the run, and the next tick resumes it", async () => {
  const { store, index, leases } = harness();
  const wf = workflow("approval", async (ctx) => {
    const decision = await ctx.waitForEvent<{ ok: boolean }>("decision");
    return decision.ok ? "approved" : "rejected";
  });

  const outcome = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await index.record("run-1", wf.name, outcome);
  assert.equal(outcome.status, "waiting");

  const handler = createEventsHandler({ store, index });
  const response = await handler(
    new Request("http://x/events", {
      method: "POST",
      body: JSON.stringify({ runId: "run-1", name: "decision", payload: { ok: true } }),
    }),
  );
  assert.equal(response.status, 202);

  const scheduler = new Scheduler({ workflows: [wf], store, leases, index, owner: "worker-1" });
  await scheduler.tick();
  const events = await store.load("run-1");
  assert.equal(events.some((e) => e.type === "run-completed"), true);
});

test("the events handler maps errors to problem details", async () => {
  const { store, index } = harness();
  const handler = createEventsHandler({ store, index });

  const missing = await handler(
    new Request("http://x/events", { method: "POST", body: JSON.stringify({ runId: "ghost", name: "x" }) }),
  );
  assert.equal(missing.status, 404);
  assert.equal(missing.headers.get("content-type"), "application/problem+json");

  const badBody = await handler(new Request("http://x/events", { method: "POST", body: "not json" }));
  assert.equal(badBody.status, 400);

  const wrongMethod = await handler(new Request("http://x/events", { method: "GET" }));
  assert.equal(wrongMethod.status, 400);
});

/** Docs whose find() throws while state.fail is true; other ops always work. */
function failableDocs(state: { fail: boolean; findCalls: number }): DocumentLike {
  const docs = fakeDocs();
  return {
    insert: (collection, doc) => docs.insert(collection, doc),
    update: (collection, filter, update) => docs.update(collection, filter, update),
    async find(collection, filter) {
      state.findCalls += 1;
      if (state.fail) throw new Error("store unreachable");
      return docs.find(collection, filter);
    },
  };
}

async function until(cond: () => boolean | Promise<boolean>, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("condition not met in time");
}

test("start() routes index.due failures to onTickError and the loop survives", async () => {
  const store = new MemoryEventStore();
  const leases = new LeaseManager(simpleKV(), { ttlSeconds: 30 });
  const state = { fail: true, findCalls: 0 };
  const index = new RunIndex(failableDocs(state));
  const wf = workflow("approval", async (ctx) => {
    await ctx.waitForEvent("decision");
    return "done";
  });

  const outcome = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await index.record("run-1", wf.name, outcome);
  await deliverEvent(store, "run-1", "decision", { ok: true });
  await index.markWake("run-1");

  const tickErrors: unknown[] = [];
  const scheduler = new Scheduler({
    workflows: [wf],
    store,
    leases,
    index,
    owner: "worker-1",
    intervalMs: 5,
    onTickError: (error) => tickErrors.push(error),
  });
  scheduler.start();
  try {
    await until(() => tickErrors.length >= 1);
    assert.equal((tickErrors[0] as Error).message, "store unreachable");

    // store comes back: a subsequent tick still runs and completes the run
    state.fail = false;
    await until(async () => (await store.load("run-1")).some((e) => e.type === "run-completed"));
  } finally {
    scheduler.stop();
  }
});

test("without onTickError, tick failures fall back to onError with run id \"(tick)\"", async () => {
  const store = new MemoryEventStore();
  const leases = new LeaseManager(simpleKV(), { ttlSeconds: 30 });
  const state = { fail: true, findCalls: 0 };
  const index = new RunIndex(failableDocs(state));
  const wf = workflow("noop", async () => "done");

  const errors: Array<{ runId: string; error: unknown }> = [];
  const scheduler = new Scheduler({
    workflows: [wf],
    store,
    leases,
    index,
    owner: "worker-1",
    intervalMs: 5,
    onError: (runId, error) => errors.push({ runId, error }),
  });
  scheduler.start();
  try {
    await until(() => errors.length >= 1);
    assert.equal(errors[0]?.runId, "(tick)");
    assert.equal((errors[0]?.error as Error).message, "store unreachable");
  } finally {
    scheduler.stop();
  }
});

test("with neither handler, tick failures do not crash and the loop keeps polling", async () => {
  const store = new MemoryEventStore();
  const leases = new LeaseManager(simpleKV(), { ttlSeconds: 30 });
  const state = { fail: true, findCalls: 0 };
  const index = new RunIndex(failableDocs(state));
  const wf = workflow("noop", async () => "done");

  const scheduler = new Scheduler({
    workflows: [wf],
    store,
    leases,
    index,
    owner: "worker-1",
    intervalMs: 5,
  });
  scheduler.start();
  try {
    // at least two failing passes prove the interval survived the first error
    await until(() => state.findCalls >= 2);
  } finally {
    scheduler.stop();
  }
});
