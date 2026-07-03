import assert from "node:assert/strict";
import { test } from "node:test";

import { NondeterminismError } from "./errors.js";
import { MemoryEventStore } from "./store.js";
import { StepError } from "./context.js";
import { completeSleep, deliverEvent, executeRun } from "./run.js";
import { workflow } from "./workflow.js";

test("steps execute exactly once across suspensions and resumes", async () => {
  const executions: string[] = [];
  const wf = workflow("order", async (ctx, input: { id: string }) => {
    const reserved = await ctx.step("reserve", () => {
      executions.push("reserve");
      return { reservation: `r-${input.id}` };
    });
    await ctx.sleep("7d");
    const charged = await ctx.step("charge", () => {
      executions.push("charge");
      return { charged: true };
    });
    return { reserved, charged };
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: { id: "42" } });
  assert.equal(first.status, "sleeping");
  assert.ok(first.wakeAt);
  assert.deepEqual(executions, ["reserve"]);

  await completeSleep(store, "run-1");
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.deepEqual(second.output, {
    reserved: { reservation: "r-42" },
    charged: { charged: true },
  });
  // replay did not re-run either step
  assert.deepEqual(executions, ["reserve", "charge"]);
});

test("now() and random() are recorded and replay identically", async () => {
  const observed: Array<{ now: number; random: number }> = [];
  const wf = workflow("timing", async (ctx) => {
    const now = await ctx.now();
    const random = await ctx.random();
    observed.push({ now, random });
    await ctx.sleep("1h");
    return { now, random };
  });

  const store = new MemoryEventStore();
  await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await completeSleep(store, "run-1");
  const outcome = await executeRun({ workflow: wf, runId: "run-1", store });

  assert.equal(outcome.status, "completed");
  assert.equal(observed.length, 2);
  assert.deepEqual(observed[0], observed[1]);
  assert.deepEqual(outcome.output, observed[0]);
});

test("input comes from the log on resume, not from the caller", async () => {
  const wf = workflow("echo", async (ctx, input: string) => {
    await ctx.sleep("1h");
    return `got ${input}`;
  });
  const store = new MemoryEventStore();
  await executeRun({ workflow: wf, runId: "run-1", store, input: "original" });
  await completeSleep(store, "run-1");
  const outcome = await executeRun({ workflow: wf, runId: "run-1", store, input: "different" });
  assert.equal(outcome.output, "got original");
});

test("a throwing step fails the run deterministically and short-circuits thereafter", async () => {
  let attempts = 0;
  const wf = workflow("explode", async (ctx) => {
    await ctx.step("boom", () => {
      attempts += 1;
      throw new Error("payment declined");
    });
    return "unreachable";
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "failed");
  assert.match(first.error?.detail ?? "", /payment declined/);
  assert.equal(attempts, 1);

  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "failed");
  assert.equal(attempts, 1); // terminal short-circuit — nothing re-ran
});

test("workflow code can catch a StepError and take a recovery path", async () => {
  const wf = workflow("recover", async (ctx) => {
    try {
      await ctx.step("primary", () => {
        throw new Error("primary down");
      });
      return "primary";
    } catch (error) {
      if (!(error instanceof StepError)) throw error;
      await ctx.sleep("5m");
      return ctx.step("fallback", () => "fallback ok");
    }
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "sleeping");
  await completeSleep(store, "run-1");
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "fallback ok");
});

test("waitForEvent suspends and resumes with the delivered payload", async () => {
  const wf = workflow("approval", async (ctx) => {
    const decision = await ctx.waitForEvent<{ approved: boolean }>("decision");
    return decision.approved ? "shipped" : "cancelled";
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "waiting");
  assert.equal(first.eventName, "decision");

  await deliverEvent(store, "run-1", "decision", { approved: true });
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "shipped");
});

test("early deliveries buffer until their waitForEvent, regardless of order", async () => {
  const wf = workflow("two-signals", async (ctx) => {
    const a = await ctx.waitForEvent<string>("a");
    const b = await ctx.waitForEvent<string>("b");
    return `${a}:${b}`;
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "waiting");
  assert.equal(first.eventName, "a");

  // deliver out of order: b arrives before a
  await deliverEvent(store, "run-1", "b", "beta");
  await deliverEvent(store, "run-1", "a", "alpha");
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "alpha:beta");
});

test("multiple sequential sleeps consume completions in order", async () => {
  const wf = workflow("two-naps", async (ctx) => {
    await ctx.sleep("1h");
    await ctx.sleep("2h");
    return "rested";
  });

  const store = new MemoryEventStore();
  assert.equal((await executeRun({ workflow: wf, runId: "run-1", store, input: null })).status, "sleeping");
  await completeSleep(store, "run-1");
  assert.equal((await executeRun({ workflow: wf, runId: "run-1", store })).status, "sleeping");
  await completeSleep(store, "run-1");
  assert.equal((await executeRun({ workflow: wf, runId: "run-1", store })).status, "completed");
});

test("a renamed step is nondeterminism, and the run survives to run correctly again", async () => {
  const store = new MemoryEventStore();
  const v1 = workflow("deploy", async (ctx) => {
    await ctx.step("build", () => "built");
    await ctx.sleep("1h");
    await ctx.step("release", () => "released");
    return "done";
  });
  assert.equal((await executeRun({ workflow: v1, runId: "run-1", store, input: null })).status, "sleeping");
  await completeSleep(store, "run-1");

  const v2 = workflow("deploy", async (ctx) => {
    await ctx.step("compile", () => "built"); // renamed!
    await ctx.sleep("1h");
    await ctx.step("release", () => "released");
    return "done";
  });
  await assert.rejects(executeRun({ workflow: v2, runId: "run-1", store }), NondeterminismError);

  // the bad deploy did NOT fail the run; correct code still completes it
  const recovered = await executeRun({ workflow: v1, runId: "run-1", store });
  assert.equal(recovered.status, "completed");
});

test("removing recorded operations is nondeterminism", async () => {
  const store = new MemoryEventStore();
  const v1 = workflow("shrink", async (ctx) => {
    await ctx.step("work", () => 1);
    await ctx.sleep("1h");
    await ctx.step("more", () => 2);
    return "done";
  });
  await executeRun({ workflow: v1, runId: "run-1", store, input: null });
  await completeSleep(store, "run-1");
  await executeRun({ workflow: v1, runId: "run-1", store });

  // now replay the COMPLETED... instead use a fresh run: log has step "work" recorded,
  // v2 returns before consuming it
  const store2 = new MemoryEventStore();
  await executeRun({ workflow: v1, runId: "run-2", store: store2, input: null });
  await completeSleep(store2, "run-2");
  const v2 = workflow("shrink", async (ctx) => {
    await ctx.step("work", () => 1);
    return "done"; // sleep + second step removed
  });
  await assert.rejects(executeRun({ workflow: v2, runId: "run-2", store: store2 }), NondeterminismError);
});

test("swapping a step for now() is nondeterminism", async () => {
  const store = new MemoryEventStore();
  const v1 = workflow("swap", async (ctx) => {
    await ctx.step("fetch", () => 1);
    await ctx.sleep("1h");
    return "done";
  });
  await executeRun({ workflow: v1, runId: "run-1", store, input: null });
  await completeSleep(store, "run-1");
  const v2 = workflow("swap", async (ctx) => {
    await ctx.now(); // was step "fetch"
    await ctx.sleep("1h");
    return "done";
  });
  await assert.rejects(executeRun({ workflow: v2, runId: "run-1", store }), NondeterminismError);
});

test("a run cannot be executed under a different workflow", async () => {
  const store = new MemoryEventStore();
  const a = workflow("alpha", async () => "a");
  const b = workflow("beta", async () => "b");
  await executeRun({ workflow: a, runId: "run-1", store, input: null });
  await assert.rejects(executeRun({ workflow: b, runId: "run-1", store }), NondeterminismError);
});

test("undefined step results normalize to null, live and replayed", async () => {
  const observed: unknown[] = [];
  const wf = workflow("void-step", async (ctx) => {
    const value = await ctx.step("side-effect", () => undefined);
    observed.push(value);
    await ctx.sleep("1h");
    return value;
  });
  const store = new MemoryEventStore();
  await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await completeSleep(store, "run-1");
  const outcome = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.deepEqual(observed, [null, null]);
  assert.equal(outcome.output, null);
});

test("step results are observed post-JSON on first execution too", async () => {
  const observed: unknown[] = [];
  const wf = workflow("json-fidelity", async (ctx) => {
    const value = await ctx.step("dated", () => ({ when: new Date("2026-01-01T00:00:00Z") }));
    observed.push(value);
    await ctx.sleep("1h");
    return value;
  });
  const store = new MemoryEventStore();
  await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await completeSleep(store, "run-1");
  await executeRun({ workflow: wf, runId: "run-1", store });
  // Dates serialize to ISO strings; live execution must see the same shape replay will
  assert.deepEqual(observed[0], { when: "2026-01-01T00:00:00.000Z" });
  assert.deepEqual(observed[0], observed[1]);
});

test("completed runs are idempotent and append nothing new", async () => {
  const wf = workflow("once", async (ctx) => ctx.step("s", () => 41 + 1));
  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  const logAfterFirst = (await store.load("run-1")).length;
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.deepEqual(second, first);
  assert.equal((await store.load("run-1")).length, logAfterFirst);
});

test("deliverEvent guards unknown and finished runs", async () => {
  const store = new MemoryEventStore();
  await assert.rejects(deliverEvent(store, "ghost", "x"), /Unknown run/);
  const wf = workflow("fast", async () => "done");
  await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await assert.rejects(deliverEvent(store, "run-1", "x"), /already finished/);
});

test("completeSleep guards runs with no pending sleep", async () => {
  const store = new MemoryEventStore();
  const wf = workflow("waiter", async (ctx) => ctx.waitForEvent("go"));
  await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await assert.rejects(completeSleep(store, "run-1"), /no pending sleep/);
});
