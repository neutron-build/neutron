import assert from "node:assert/strict";
import { test } from "node:test";

import { cancelRun, completeSleep, deliverEvent, executeRun } from "./run.js";
import { MemoryEventStore } from "./store.js";
import { workflow } from "./workflow.js";

// ── cancellation ──────────────────────────────────────────────────────

test("cancelRun settles a waiting run as cancelled on its next pass", async () => {
  const store = new MemoryEventStore();
  const wf = workflow("w", async (ctx) => {
    await ctx.step("a", () => "did-a");
    await ctx.waitForEvent("approval");
    await ctx.step("b", () => "did-b");
    return "done";
  });

  const parked = await executeRun({ workflow: wf, runId: "r1", store, input: null });
  assert.equal(parked.status, "waiting");

  await cancelRun(store, "r1", "operator stopped it");
  const settled = await executeRun({ workflow: wf, runId: "r1", store });
  assert.equal(settled.status, "cancelled");
  assert.equal(settled.error?.detail, "operator stopped it");

  // terminal + idempotent from here on
  const again = await executeRun({ workflow: wf, runId: "r1", store });
  assert.equal(again.status, "cancelled");
  await cancelRun(store, "r1"); // no-op, no throw
  await assert.rejects(() => deliverEvent(store, "r1", "approval", {}), /already finished/);
});

test("a cancel landing mid-pass stops the run before its next live step", async () => {
  const store = new MemoryEventStore();
  let bRan = false;
  const wf = workflow("w", async (ctx) => {
    await ctx.step("a", async () => {
      // the cancel arrives WHILE step a executes
      await cancelRun(store, "r2", "too expensive");
      return "a-done";
    });
    await ctx.step("b", () => {
      bRan = true;
      return "b-done";
    });
    return "done";
  });

  const outcome = await executeRun({ workflow: wf, runId: "r2", store, input: null });
  assert.equal(outcome.status, "cancelled");
  assert.equal(bRan, false, "the step after the cancellation point must never execute");

  const settled = await executeRun({ workflow: wf, runId: "r2", store });
  assert.equal(settled.status, "cancelled");
});

test("cancelRun rejects unknown and finished runs", async () => {
  const store = new MemoryEventStore();
  await assert.rejects(() => cancelRun(store, "ghost"), /Unknown run/);

  const wf = workflow("w", async () => "ok");
  await executeRun({ workflow: wf, runId: "r3", store, input: null });
  await assert.rejects(() => cancelRun(store, "r3"), /already finished/);
});

// ── external appends racing a live pass (seq collision) ────────────────

test("an event delivered mid-pass does not corrupt the log (seq collision)", async () => {
  // deliverEvent computes its seq from a fresh load while a live pass
  // allocates from its own counter — both used to write the SAME seq, and
  // load()'s first-writer dedupe silently deleted one of them. When the
  // deleted event was a step-completed cursor, the next replay hit
  // NondeterminismError and the run was bricked (deliberately unrecorded).
  const store = new MemoryEventStore();
  const runId = "mid-pass-delivery";
  let stepExecutions = 0;

  const wf = workflow("w", async (ctx) => {
    await ctx.step("a", async () => {
      stepExecutions += 1;
      // The event lands WHILE the pass is live and allocating seqs — the
      // exact interleaving of an HTTP deliverEvent during a step's I/O.
      await deliverEvent(store, runId, "wake", "wake-payload");
      return "a-result";
    });
    await ctx.sleep("60s"); // park so the next pass must replay step "a"
    return await ctx.waitForEvent("wake");
  });

  const parked = await executeRun({ workflow: wf, runId, store, input: null });
  assert.equal(parked.status, "sleeping");

  await completeSleep(store, runId);
  const woken = await executeRun({ workflow: wf, runId, store });
  assert.equal(woken.status, "completed");
  assert.equal(woken.output, "wake-payload");
  assert.equal(stepExecutions, 1, "step side effects must not re-run after a mid-pass delivery");

  // Idempotent from here on.
  const again = await executeRun({ workflow: wf, runId, store });
  assert.equal(again.status, "completed");
  assert.equal(again.output, "wake-payload");
});
