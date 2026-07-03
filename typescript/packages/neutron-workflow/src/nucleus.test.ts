import assert from "node:assert/strict";
import { test } from "node:test";

import { WIRE_FORMAT_VERSION } from "./events.js";
import { LeaseManager, executeRunExclusive, type KVLike } from "./lease.js";
import { NucleusEventStore, type StreamsLike } from "./nucleus-store.js";
import { completeSleep, executeRun } from "./run.js";
import { workflow } from "./workflow.js";

/** In-memory StreamsModel double preserving append order like the real stream. */
function fakeStreams(): StreamsLike & { raw: Map<string, Array<{ id: string; fields: Record<string, unknown> }>> } {
  const raw = new Map<string, Array<{ id: string; fields: Record<string, unknown> }>>();
  let counter = 0;
  return {
    raw,
    async xadd(stream, fields) {
      const entries = raw.get(stream) ?? [];
      counter += 1;
      const id = `${Date.now()}-${counter}`;
      entries.push({ id, fields });
      raw.set(stream, entries);
      return id;
    },
    async xrange(stream, _startMs, _endMs, count) {
      return (raw.get(stream) ?? []).slice(0, count);
    },
  };
}

/** In-memory KV double honoring the atomic lease semantics, with a manual clock. */
function fakeKV(): KVLike & { advance(seconds: number): void } {
  const entries = new Map<string, { value: string; expiresAt: number | null }>();
  let now = 0;
  const live = (key: string): { value: string; expiresAt: number | null } | undefined => {
    const entry = entries.get(key);
    if (entry === undefined) return undefined;
    if (entry.expiresAt !== null && entry.expiresAt <= now) {
      entries.delete(key);
      return undefined;
    }
    return entry;
  };
  return {
    advance(seconds) {
      now += seconds;
    },
    async setNX(key, value, opts) {
      if (live(key) !== undefined) return false;
      entries.set(key, { value, expiresAt: opts?.ttl !== undefined ? now + opts.ttl : null });
      return true;
    },
    async cdel(key, expected) {
      const entry = live(key);
      if (entry === undefined || entry.value !== expected) return false;
      entries.delete(key);
      return true;
    },
    async cexpire(key, expected, seconds) {
      const entry = live(key);
      if (entry === undefined || entry.value !== expected) return false;
      entry.expiresAt = now + seconds;
      return true;
    },
  };
}

test("workflows run end-to-end on the Nucleus stream wire format", async () => {
  const streams = fakeStreams();
  const store = new NucleusEventStore(streams);
  const executions: string[] = [];
  const wf = workflow("order", async (ctx, input: { id: string }) => {
    const reserved = await ctx.step("reserve", () => {
      executions.push("reserve");
      return `r-${input.id}`;
    });
    await ctx.sleep("7d");
    return ctx.step("charge", () => {
      executions.push("charge");
      return `charged ${reserved}`;
    });
  });

  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: { id: "42" } });
  assert.equal(first.status, "sleeping");
  await completeSleep(store, "run-1");
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "charged r-42");
  assert.deepEqual(executions, ["reserve", "charge"]);

  // every entry on the wire is a single JSON event field
  const entries = streams.raw.get("wf:run-1")!;
  assert.ok(entries.length >= 5);
  for (const entry of entries) {
    const event = JSON.parse(entry.fields.event as string);
    assert.equal(event.v, WIRE_FORMAT_VERSION);
    assert.equal(typeof event.seq, "number");
  }
});

test("load dedupes racing appends by seq, first writer wins", async () => {
  const streams = fakeStreams();
  const store = new NucleusEventStore(streams);
  const event = (seq: number, name: string) =>
    JSON.stringify({ v: 1, seq, type: "step-completed", at: "t", name, data: { result: name } });
  await streams.xadd("wf:run-1", { event: event(0, "first-writer") });
  await streams.xadd("wf:run-1", { event: event(0, "second-writer") });
  await streams.xadd("wf:run-1", { event: event(1, "next") });

  const events = await store.load("run-1");
  assert.equal(events.length, 2);
  assert.equal(events[0]?.name, "first-writer");
  assert.equal(events[1]?.name, "next");
});

test("leases: single holder, conditional renew/release, expiry steal", async () => {
  const kv = fakeKV();
  const leases = new LeaseManager(kv, { ttlSeconds: 30 });

  const a = await leases.acquire("run-1", "executor-a");
  assert.ok(a);
  assert.equal(await leases.acquire("run-1", "executor-b"), null);
  assert.equal(await a!.renew(), true);

  // expiry: b can steal, after which a can neither renew nor release b's lock
  kv.advance(31);
  const b = await leases.acquire("run-1", "executor-b");
  assert.ok(b);
  assert.equal(await a!.renew(), false);
  assert.equal(await a!.release(), false);
  assert.equal(await b!.release(), true);

  // clean release frees the run immediately
  const c = await leases.acquire("run-1", "executor-c");
  assert.ok(c);
  assert.equal(await c!.release(), true);
});

test("executeRunExclusive: one executor wins, crashed holders are replayed after expiry", async () => {
  const streams = fakeStreams();
  const store = new NucleusEventStore(streams);
  const kv = fakeKV();
  const leases = new LeaseManager(kv, { ttlSeconds: 30 });
  let executions = 0;
  const wf = workflow("job", async (ctx) => {
    await ctx.step("work", () => {
      executions += 1;
      return "done";
    });
    await ctx.sleep("1h");
    return "finished";
  });

  // simulate a holder that acquired and then died without releasing
  const crashed = await leases.acquire("run-1", "dead-executor");
  assert.ok(crashed);
  assert.equal(
    await executeRunExclusive({ workflow: wf, runId: "run-1", store, input: null, leases, owner: "b" }),
    null,
  );

  // its lease expires; the next executor claims and runs from the log
  kv.advance(31);
  const outcome = await executeRunExclusive({ workflow: wf, runId: "run-1", store, input: null, leases, owner: "b" });
  assert.equal(outcome?.status, "sleeping");
  assert.equal(executions, 1);

  // lease released after the pass: wake and finish under a new claim
  await completeSleep(store, "run-1");
  const final = await executeRunExclusive({ workflow: wf, runId: "run-1", store, leases, owner: "c" });
  assert.equal(final?.status, "completed");
  assert.equal(final?.output, "finished");
  assert.equal(executions, 1); // the step never re-ran
});
